//! Old histories contain non-canonical tree entry modes (`100664`, `100640`,
//! zero-padded `040000`, ...). A crafted SHA-1 repository with such trees must
//! import to exactly the ids git's own SHA-256 conversion gives (which copies
//! the mode text verbatim), and the export must reproduce the same objects.
//!
//! `git` (2.43) cannot convert such a repository itself, and
//! `fast-export | fast-import` normalizes modes, so the oracle is a SHA-256
//! repository built from the same bytes with the ids mapped, which is what the
//! conversion produces.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use ant_core::{EntryMode, RepoPath};
use ant_gitbridge::{ImportOptions, import};
use ant_store::{Backend, ObjectStoreExt, graph::lookup_path};

const ENV: &[(&str, &str)] = &[
    ("GIT_AUTHOR_NAME", "Ada"),
    ("GIT_AUTHOR_EMAIL", "ada@example.com"),
    ("GIT_COMMITTER_NAME", "Bob"),
    ("GIT_COMMITTER_EMAIL", "bob@example.com"),
    ("GIT_AUTHOR_DATE", "1700000000 +0000"),
    ("GIT_COMMITTER_DATE", "1700000100 +0000"),
    ("GIT_CONFIG_NOSYSTEM", "1"),
    ("GIT_CONFIG_GLOBAL", "/dev/null"),
];

fn run(dir: &Path, args: &[&str], stdin: &[u8]) -> Vec<u8> {
    let mut child = Command::new("git")
        .args(args)
        .current_dir(dir)
        .envs(ENV.iter().copied())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    out.stdout
}

fn text(dir: &Path, args: &[&str]) -> String {
    String::from_utf8(run(dir, args, b"")).unwrap().trim().to_string()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

/// What an entry points at in the source repository: a blob, or another tree.
#[derive(Clone)]
enum Child {
    Blob(&'static str),
    Tree(Vec<(&'static str, &'static str, Child)>),
}

/// Write `child` into `repo` with raw mode text, returning the hex id.
fn write_child(repo: &Path, child: &Child) -> String {
    match child {
        Child::Blob(content) => text_stdin(repo, &["hash-object", "-w", "--stdin"], content),
        Child::Tree(entries) => {
            let mut body = Vec::new();
            for (mode, name, c) in entries {
                body.extend_from_slice(format!("{mode} {name}\0").as_bytes());
                body.extend(unhex(&write_child(repo, c)));
            }
            String::from_utf8(run(
                repo,
                &["hash-object", "-t", "tree", "--literally", "-w", "--stdin"],
                &body,
            ))
            .unwrap()
            .trim()
            .to_string()
        }
    }
}

fn text_stdin(dir: &Path, args: &[&str], stdin: &str) -> String {
    String::from_utf8(run(dir, args, stdin.as_bytes())).unwrap().trim().to_string()
}

/// Entries in git order, each with a legacy spelling of its mode.
fn root(version: &'static str) -> Child {
    let sub = Child::Tree(vec![("100664", "f", Child::Blob("in padded dir\n"))]);
    let perms = Child::Tree(vec![("100644", "g", Child::Blob("in 40755 dir\n"))]);
    Child::Tree(vec![
        ("100664", "a", Child::Blob(version)),
        ("100640", "b", Child::Blob("group readable\n")),
        ("100775", "c", Child::Blob("#!/bin/sh\necho hi\n")),
        ("040000", "d", sub),
        ("40755", "e", perms),
        ("120755", "link", Child::Blob("a")),
        ("0100644", "zero", Child::Blob("zero padded file\n")),
    ])
}

/// Two commits whose trees all use legacy modes; returns the tip hex.
fn build(repo: &Path, object_format: &str) -> String {
    run(repo, &["init", "-q", "-b", "main", &format!("--object-format={object_format}")], b"");
    let mut parent: Option<String> = None;
    for version in ["version one\n", "version two\n"] {
        let tree = write_child(repo, &root(version));
        let mut args = vec!["commit-tree", &tree, "-m", version];
        if let Some(p) = &parent {
            args.extend(["-p", p]);
        }
        parent = Some(text(repo, &args));
    }
    let tip = parent.unwrap();
    text(repo, &["update-ref", "refs/heads/main", &tip]);
    tip
}

fn object_ids(repo: &Path) -> BTreeSet<String> {
    text(repo, &["cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype)"])
        .lines()
        .map(str::to_string)
        .collect()
}

async fn serve(b: &Backend) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/repo.git", listener.local_addr().unwrap());
    let app = ant_gitserve::router(b.clone());
    (url, tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }))
}

#[tokio::test]
async fn legacy_mode_import_and_export_match_git_sha256() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, oracle, clone) =
        (tmp.path().join("src"), tmp.path().join("oracle"), tmp.path().join("clone"));
    for d in [&src, &oracle] {
        std::fs::create_dir(d).unwrap();
    }
    build(&src, "sha1");
    let expected_tip = build(&oracle, "sha256");

    // The source really has the legacy spellings; the oracle keeps them.
    let raw = run(&oracle, &["cat-file", "tree", "main"], b"");
    for spelling in ["100664 a\0", "100640 b\0", "040000 d\0", "40755 e\0", "0100644 zero\0"] {
        assert!(raw.windows(spelling.len()).any(|w| w == spelling.as_bytes()), "{spelling:?}");
    }

    let b = Backend::in_memory();
    import(&src, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap();
    let tip = b.refs.get_ref("main").await.unwrap().unwrap();
    assert_eq!(tip.to_hex(), expected_tip, "import must equal git's SHA-256 conversion");

    // Stored trees carry the exact spellings.
    let c = b.objects.read_commit(&tip).await.unwrap();
    let tree = b.objects.read_tree(&c.tree).await.unwrap();
    let modes: Vec<_> =
        tree.entries().iter().map(|e| (e.name.as_str(), e.mode.as_octal())).collect();
    assert_eq!(
        modes,
        [
            ("a", "100664".into()),
            ("b", "100640".into()),
            ("c", "100775".into()),
            ("d", "040000".into()),
            ("e", "40755".into()),
            ("link", "120755".into()),
            ("zero", "0100644".into()),
        ]
    );
    assert!(tree.entries().iter().all(|e| e.mode.is_legacy()));
    // ... but behave like their classes.
    let classes: Vec<_> = tree.entries().iter().map(|e| e.mode.class()).collect();
    assert_eq!(
        classes,
        [
            EntryMode::File,
            EntryMode::File,
            EntryMode::Executable,
            EntryMode::Tree,
            EntryMode::Tree,
            EntryMode::Symlink,
            EntryMode::File,
        ]
    );
    let (mode, _) = lookup_path(b.objects.as_ref(), c.tree, &RepoPath::new("d/f").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mode, EntryMode::File, "lookup reports the semantic class");

    // The recorded changeset of the second commit is a plain modification.
    let changes = b.meta.get_changeset(&tip).await.unwrap().unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].path.as_str(), "a");
    assert_eq!(changes[0].old.unwrap().0, EntryMode::File);
    assert_eq!(changes[0].new.unwrap().0, EntryMode::File);

    // Export: a clone has exactly the oracle's objects, and fsck accepts it.
    let (url, task) = serve(&b).await;
    let out = tokio::process::Command::new("git")
        .args(["clone", "-q", &url])
        .arg(&clone)
        .envs(ENV.iter().copied())
        .output()
        .await
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    task.abort();
    assert_eq!(text(&clone, &["rev-parse", "HEAD"]), expected_tip);
    assert_eq!(object_ids(&clone), object_ids(&oracle));
    text(&clone, &["fsck", "--full"]);
    assert_eq!(
        run(&clone, &["cat-file", "tree", "HEAD"], b""),
        raw,
        "the exported tree has the exact legacy mode text"
    );
    assert_eq!(
        std::fs::read_to_string(clone.join("a")).unwrap(),
        "version two\n",
        "checkout works on a legacy-mode file"
    );
}
