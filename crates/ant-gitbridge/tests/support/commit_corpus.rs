// Crafted commits with unusual-but-valid raw bytes, and the oracle that
// builds them in a SHA-1 and a SHA-256 repository. Git's own SHA-256
// conversion copies commit text verbatim except object ids, and the installed
// git cannot convert such repositories, so the oracle is a SHA-256 repository
// made from the same bytes with ids mapped. Shared (via `#[path]`) by the
// gitbridge import test and the gitserve export test; every item is used by
// both.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

pub const ENV: &[(&str, &str)] = &[
    ("GIT_AUTHOR_NAME", "Ada"),
    ("GIT_AUTHOR_EMAIL", "ada@example.com"),
    ("GIT_COMMITTER_NAME", "Bob"),
    ("GIT_COMMITTER_EMAIL", "bob@example.com"),
    ("GIT_CONFIG_NOSYSTEM", "1"),
    ("GIT_CONFIG_GLOBAL", "/dev/null"),
];

pub fn run(dir: &Path, args: &[&str], stdin: &[u8]) -> Vec<u8> {
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

pub fn text(dir: &Path, args: &[&str]) -> String {
    String::from_utf8(run(dir, args, b"")).unwrap().trim().to_string()
}

const GPGSIG: &[u8] = b"gpgsig -----BEGIN PGP SIGNATURE-----\n \n iQEzBAABCAAdFiEE\n =abcd\n -----END PGP SIGNATURE-----\n";

/// One crafted commit: everything after the `parent` lines. `rest` starts at
/// the `author` line and includes the blank line and message (or not, for the
/// cases without them).
pub struct Case {
    pub name: &'static str,
    pub rest: Vec<u8>,
}

/// `author`/`committer` lines around `sig`, then a plain message.
fn with_sig(name: &'static str, sig: &[u8]) -> Case {
    let mut rest = b"author ".to_vec();
    rest.extend_from_slice(sig);
    rest.extend_from_slice(b"\ncommitter ");
    rest.extend_from_slice(sig);
    rest.extend_from_slice(b"\n\nmsg\n");
    Case { name, rest }
}

pub fn cases() -> Vec<Case> {
    const OK: &[u8] = b"A U Thor <a@x.org> 1700000000 +0000";
    let mut v = vec![with_sig("plain", b"A <a@x.org> 1 +0000")];
    for (name, sig) in [
        // Names: trailing/leading spaces, empty, no separator before `<`.
        ("name-two-trailing-spaces", &b"Name  <e@x> 123 +0000"[..]),
        ("name-many-trailing-spaces", b"Name       <e@x> 123 +0000"),
        ("name-leading-spaces", b"  Name <e@x> 123 +0000"),
        ("name-leading-and-trailing", b"  Name   <e@x> 123 +0000"),
        ("name-empty-with-space", b" <e@x> 123 +0000"),
        ("name-empty-no-space", b"<e@x> 123 +0000"),
        ("name-only-spaces", b"   <e@x> 123 +0000"),
        ("name-no-space-before-lt", b"Name<e@x> 123 +0000"),
        ("name-tab-before-lt", b"Name\t<e@x> 123 +0000"),
        ("name-contains-lt", b"A <B> C <e@x> 123 +0000"),
        ("name-non-utf8", b"J\xf6rg \xff <e@x> 123 +0000"),
        // Emails.
        ("email-empty", b"Name <> 123 +0000"),
        ("email-with-spaces", b"Name <a b  c@x> 123 +0000"),
        ("email-with-lt", b"Name <a<b@x> 123 +0000"),
        ("email-with-gt", b"Name <a>b@x> 123 +0000"),
        ("email-non-utf8", b"Name <\xe9@x> 123 +0000"),
        // Timestamps and zones.
        ("tz-minus-0000", b"Name <e@x> 123 -0000"),
        ("tz-minus-1200", b"Name <e@x> 123 -1200"),
        ("tz-plus-1400", b"Name <e@x> 123 +1400"),
        ("tz-odd-plus-05", b"Name <e@x> 123 +05"),
        ("tz-odd-minutes-75", b"Name <e@x> 123 +0575"),
        ("tz-huge", b"Name <e@x> 123 +9999"),
        ("tz-missing", b"Name <e@x> 123"),
        ("tz-and-time-missing", b"Name <e@x>"),
        ("tz-no-sign", b"Name <e@x> 123 0100"),
        ("tz-garbage", b"Name <e@x> 123 CEST"),
        ("time-huge", b"Name <e@x> 99999999999999 +0000"),
        ("time-overflow", b"Name <e@x> 99999999999999999999999 +0000"),
        ("time-zero-padded", b"Name <e@x> 0001700000000 +0100"),
        ("time-plus-sign", b"Name <e@x> +123 +0000"),
        ("time-negative", b"Name <e@x> -5 +0000"),
        ("time-garbage", b"Name <e@x> yesterday +0000"),
        ("tail-extra-spaces", b"Name <e@x>   123   +0000"),
        ("tail-trailing-space", b"Name <e@x> 123 +0000 "),
        ("tail-trailing-garbage", b"Name <e@x> 123 +0000 extra"),
        ("tail-tab", b"Name <e@x>\t123\t+0000"),
        ("tail-cr", b"Name <e@x> 123 +0000\r"),
        ("no-space-after-gt", b"Name <e@x>123 +0000"),
    ] {
        v.push(with_sig(name, sig));
    }

    // Author and committer differ in shape.
    let mut rest = b"author Name  <e@x> 1 -0000\ncommitter ".to_vec();
    rest.extend_from_slice(OK);
    rest.extend_from_slice(b"\n\nmsg\n");
    v.push(Case { name: "author-odd-committer-ok", rest });

    // Extra headers, then the blank line and message (`tail`).
    for (name, extra, tail) in [
        ("encoding", &b"encoding ISO-8859-1\n"[..], &b"\nm\xe4\n"[..]),
        ("gpgsig-multiline", GPGSIG, b"\nsigned\n"),
        (
            "mergetag-multiline",
            b"mergetag object 1111111111111111111111111111111111111111\n type commit\n tag v1\n tagger T <t@x> 1 +0000\n \n tag message\n -----BEGIN PGP SIGNATURE-----\n  indented\n -----END PGP SIGNATURE-----\n",
            b"\nMerge tag 'v1'\n",
        ),
        ("header-without-value", b"flag\n", b"\nm\n"),
        ("header-empty-value", b"flag \n", b"\nm\n"),
        ("header-trailing-space-value", b"x-note value \n", b"\nm\n"),
        ("header-double-space", b"x-note  two\n", b"\nm\n"),
        ("header-non-utf8-value", b"x-note \xff\xfe\n", b"\nm\n"),
        ("header-continuation-after-keyonly", b"flag\n continued\n", b"\nm\n"),
        ("duplicate-headers", b"x-a 1\nx-a 2\nencoding UTF-8\nencoding UTF-8\n", b"\nm\n"),
        ("header-continuation-blank", b"x-b one\n \n \n two\n", b"\nm\n"),
        ("message-no-trailing-newline", b"", b"\nno newline"),
        ("message-empty", b"", b"\n"),
        ("message-missing-separator", b"", b""),
        ("message-leading-blank-lines", b"", b"\n\n\nbody\n"),
        ("message-contains-blank-lines", b"", b"\nsubject\n\nbody\n\n\nmore\n\n"),
        ("message-non-utf8", b"", b"\nm\xe4\xc0\xff\n"),
        ("message-crlf", b"", b"\r\nsubject\r\n\r\nbody\r\n"),
        (
            "message-looks-like-header",
            b"",
            b"\nparent 1111111111111111111111111111111111111111\ntree x\n",
        ),
        ("message-trailing-spaces", b"", b"\nsubject  \n"),
    ] {
        let mut rest = b"author A <a@x> 1 +0000\ncommitter A <a@x> 1 +0000\n".to_vec();
        rest.extend_from_slice(extra);
        rest.extend_from_slice(tail);
        v.push(Case { name, rest });
    }
    let mut rest = b"author A <a@x> 1 +0000\ncommitter A <a@x> 1 +0000\n".to_vec();
    rest.extend_from_slice(GPGSIG);
    rest.extend_from_slice(b"encoding latin1\n\nm\n");
    v.push(Case { name: "gpgsig-then-encoding", rest });
    v
}

/// The crafted cases plus a final merge commit.
pub fn all_cases() -> Vec<Case> {
    let mut all = cases();
    all.push(Case {
        name: "merge",
        rest: b"author M <m@x> 5 +0000\ncommitter M <m@x> 5 +0000\n\nmerge\n".to_vec(),
    });
    all
}

/// Raw bytes of `case` on top of `tree` and `parents` (hex ids).
pub fn commit_bytes(tree: &str, parents: &[String], case: &Case) -> Vec<u8> {
    let mut b = format!("tree {tree}\n").into_bytes();
    for p in parents {
        b.extend_from_slice(format!("parent {p}\n").as_bytes());
    }
    b.extend_from_slice(&case.rest);
    b
}

/// Builds the crafted chain in `repo` (fresh directory) and points `main` at
/// the tip: one commit per case, each on its predecessor, then a merge of the
/// last case with the first. Returns the commit ids, in `all_cases` order.
pub fn build(repo: &Path, object_format: &str) -> Vec<String> {
    run(repo, &["init", "-q", "-b", "main", &format!("--object-format={object_format}")], b"");
    let tree = text(repo, &["mktree"]);
    let mut ids: Vec<String> = Vec::new();
    let all = all_cases();
    let n = all.len();
    for (i, c) in all.iter().enumerate() {
        let mut parents = Vec::new();
        if i == n - 1 {
            parents.push(ids[i - 1].clone());
            parents.push(ids[0].clone());
        } else if i > 0 {
            parents.push(ids[i - 1].clone());
        }
        let raw = commit_bytes(&tree, &parents, c);
        let out = run(repo, &["hash-object", "-t", "commit", "--literally", "-w", "--stdin"], &raw);
        ids.push(String::from_utf8(out).unwrap().trim().to_string());
    }
    text(repo, &["update-ref", "refs/heads/main", ids.last().unwrap()]);
    ids
}
