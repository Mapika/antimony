//! Import existing git repositories (SHA-1 or SHA-256) into antimony.
//!
//! TASK (gitbridge): implement `import`.
//!
//! - Open the repo with `gix`. For each `(git_ref, ant_ref)` in
//!   `opts.refs`, collect all commits reachable from the git ref that are
//!   not yet imported, and convert them parents-first (topological order).
//! - Conversion rewrites object ids to SHA-256 git objects
//!   (`ant_core::{Tree, Commit}`) while preserving everything else byte for
//!   byte: tree entry names/modes, commit author/committer/time zones,
//!   extra headers (e.g. `encoding`, `mergetag`, `gpgsig`) and messages. The
//!   result MUST equal what `git fast-export | git fast-import` into a
//!   `--object-format=sha256` repo produces for unsigned commits (the test
//!   checks every commit id).
//! - Memoize SHA-1→SHA-256 for trees and blobs within a run (identical
//!   subtrees are converted once). Persist the commit mapping in
//!   `MetaStore` derived data, kind `"git-import/commit/v1"`, key = source
//!   commit id bytes, value = 32-byte antimony id — this makes re-imports
//!   incremental (already-imported commits are skipped without reading
//!   their trees).
//! - Write objects in batches (`opts.batch_objects` per `put_many`); record
//!   each commit with `ant_store::graph::record_commit` after its objects
//!   are stored (parents first).
//! - Finally point each `ant_ref` at the converted tip via `cas_ref`
//!   (expected = current value; it's an import tool, not concurrent).
//! - gix is synchronous: do git reading inside `spawn_blocking`, streaming
//!   converted objects to the async writer through a bounded channel, so
//!   memory stays bounded on big repos.
//! - Non-UTF-8 tree entry names or commit text → error naming the object.
//! - Return stats counting objects actually written (not skipped).

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{anyhow, bail, Context as _};
use ant_core::{Commit, EncodedObject, EntryMode, ObjectId, ObjectKind, Signature, Tree, TreeEntry};
use ant_store::graph;
use ant_store::{Backend, CasOutcome};
use tokio::sync::mpsc;

/// Derived-data kind mapping a source commit id to its antimony commit id.
const COMMIT_KIND: &str = "git-import/commit/v1";

#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// (git ref, antimony ref), e.g. ("refs/heads/main", "main").
    pub refs: Vec<(String, String)>,
    pub batch_objects: usize,
}

impl ImportOptions {
    pub fn single(git_ref: &str, ant_ref: &str) -> Self {
        Self { refs: vec![(git_ref.into(), ant_ref.into())], batch_objects: 1024 }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportStats {
    pub commits: u64,
    pub trees: u64,
    pub blobs: u64,
}

/// Producer → writer messages. Objects always precede the commits that
/// reference them, so the writer can store a batch and then record its
/// commits parents-first.
enum Msg {
    Batch { objects: Vec<EncodedObject>, commits: Vec<(ObjectId, gix::ObjectId)> },
    Tip { ant_ref: String, id: ObjectId },
}

pub async fn import(repo: &Path, backend: &Backend, opts: &ImportOptions) -> anyhow::Result<ImportStats> {
    let batch_objects = opts.batch_objects.max(1);
    let (tx, rx) = mpsc::channel::<Msg>(8);
    let producer = {
        let repo = repo.to_path_buf();
        let backend = backend.clone();
        let refs = opts.refs.clone();
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || produce(&repo, backend, handle, &refs, batch_objects, tx))
    };
    let mut stats = ImportStats::default();
    let written = write(backend, rx, &mut stats).await;
    let produced = producer.await?;
    written?;
    produced?;
    tracing::debug!(?stats, "git import finished");
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Writer (async side)
// ---------------------------------------------------------------------------

async fn write(backend: &Backend, mut rx: mpsc::Receiver<Msg>, stats: &mut ImportStats) -> anyhow::Result<()> {
    while let Some(msg) = rx.recv().await {
        match msg {
            Msg::Batch { objects, commits } => {
                store_objects(backend, objects, stats).await?;
                for (id, git) in commits {
                    graph::record_commit(backend, id).await?;
                    let value = id.as_bytes().to_vec().into();
                    backend.meta.put_derived(COMMIT_KIND, git.as_slice(), value).await?;
                    stats.commits += 1;
                }
            }
            Msg::Tip { ant_ref, id } => {
                let expected = backend.refs.get_ref(&ant_ref).await?;
                match backend.refs.cas_ref(&ant_ref, expected, Some(id)).await? {
                    CasOutcome::Updated => {}
                    CasOutcome::Mismatch { actual } => {
                        bail!("ref {ant_ref} changed during import (now {actual:?})")
                    }
                }
            }
        }
    }
    Ok(())
}

/// Store the objects the target does not already hold; only those are counted.
async fn store_objects(backend: &Backend, objects: Vec<EncodedObject>, stats: &mut ImportStats) -> anyhow::Result<()> {
    if objects.is_empty() {
        return Ok(());
    }
    let ids: Vec<ObjectId> = objects.iter().map(EncodedObject::id).collect();
    let present = backend.objects.has_many(&ids).await?;
    let mut missing = Vec::with_capacity(objects.len());
    for (obj, present) in objects.into_iter().zip(present) {
        if present {
            continue;
        }
        match obj.kind() {
            ObjectKind::Blob => stats.blobs += 1,
            ObjectKind::Tree => stats.trees += 1,
            ObjectKind::Commit => {}
        }
        missing.push(obj);
    }
    if !missing.is_empty() {
        backend.objects.put_many(&missing).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Producer (blocking side)
// ---------------------------------------------------------------------------

fn produce(
    repo_path: &Path,
    backend: Backend,
    handle: tokio::runtime::Handle,
    refs: &[(String, String)],
    batch_objects: usize,
    tx: mpsc::Sender<Msg>,
) -> anyhow::Result<()> {
    let repo = gix::open(repo_path).with_context(|| format!("opening git repo {}", repo_path.display()))?;
    let mut p = Producer {
        repo,
        backend,
        handle,
        tx,
        batch_objects,
        objects: Vec::new(),
        commits: Vec::new(),
        memo: HashMap::new(),
    };
    for (git_ref, ant_ref) in refs {
        let tip = p.resolve(git_ref)?;
        p.walk(tip)?;
        let id = p
            .known_commit(tip)?
            .ok_or_else(|| anyhow!("{git_ref} was not converted"))?;
        p.send_batch()?;
        p.send(Msg::Tip { ant_ref: ant_ref.clone(), id })?;
    }
    p.send_batch()
}

struct Producer {
    repo: gix::Repository,
    backend: Backend,
    handle: tokio::runtime::Handle,
    tx: mpsc::Sender<Msg>,
    batch_objects: usize,
    /// Converted objects not yet sent to the writer.
    objects: Vec<EncodedObject>,
    /// Converted commits not yet sent to the writer (parents-first order).
    commits: Vec<(ObjectId, gix::ObjectId)>,
    /// SHA-1 (source) → SHA-256 (antimony) for trees, blobs and commits
    /// converted during this run.
    memo: HashMap<gix::ObjectId, ObjectId>,
}

/// A commit parsed from git's raw encoding.
struct RawCommit {
    tree: gix::ObjectId,
    parents: Vec<gix::ObjectId>,
    author: Signature,
    committer: Signature,
    extra_headers: Vec<(String, String)>,
    message: String,
}

enum Frame {
    Enter(gix::ObjectId),
    /// Only the id: the commit is re-read when its turn comes, so the DFS
    /// stack never holds the parsed metadata of many commits at once.
    Exit(gix::ObjectId),
}

impl Producer {
    fn resolve(&self, name: &str) -> anyhow::Result<gix::ObjectId> {
        let mut reference = self.repo.find_reference(name).with_context(|| format!("resolving {name}"))?;
        Ok(reference.peel_to_id()?.detach())
    }

    fn read(&self, id: gix::ObjectId) -> anyhow::Result<Vec<u8>> {
        Ok(self.repo.find_object(id).with_context(|| format!("reading object {id}"))?.detach().data)
    }

    /// Antimony id of an already converted commit, from this run or an
    /// earlier import.
    fn known_commit(&self, id: gix::ObjectId) -> anyhow::Result<Option<ObjectId>> {
        if let Some(a) = self.memo.get(&id) {
            return Ok(Some(*a));
        }
        match self.handle.block_on(self.backend.meta.get_derived(COMMIT_KIND, id.as_slice()))? {
            Some(bytes) => Ok(Some(ObjectId::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    fn send(&self, msg: Msg) -> anyhow::Result<()> {
        self.tx.blocking_send(msg).map_err(|_| anyhow!("import writer stopped"))
    }

    fn send_batch(&mut self) -> anyhow::Result<()> {
        if self.objects.is_empty() && self.commits.is_empty() {
            return Ok(());
        }
        let objects = std::mem::take(&mut self.objects);
        let commits = std::mem::take(&mut self.commits);
        self.send(Msg::Batch { objects, commits })
    }

    fn push_object(&mut self, obj: EncodedObject) -> anyhow::Result<()> {
        self.objects.push(obj);
        if self.objects.len() >= self.batch_objects {
            self.send_batch()?;
        }
        Ok(())
    }

    /// Post-order walk from `tip` that converts parents before children and
    /// stops at commits that were already imported.
    fn walk(&mut self, tip: gix::ObjectId) -> anyhow::Result<()> {
        let mut visited = HashSet::new();
        let mut stack = vec![Frame::Enter(tip)];
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter(id) => {
                    if !visited.insert(id) {
                        continue;
                    }
                    // Already imported (earlier run or this one): remember the
                    // answer so children do not query the meta store again.
                    if let Some(a) = self.known_commit(id)? {
                        self.memo.insert(id, a);
                        continue;
                    }
                    let raw = parse_commit(id, &self.read(id)?)?;
                    stack.push(Frame::Exit(id));
                    for p in raw.parents.into_iter().rev() {
                        stack.push(Frame::Enter(p));
                    }
                }
                Frame::Exit(id) => self.convert_commit(id)?,
            }
        }
        Ok(())
    }

    fn convert_commit(&mut self, id: gix::ObjectId) -> anyhow::Result<()> {
        let raw = parse_commit(id, &self.read(id)?)?;
        let tree = self.convert_tree(raw.tree)?;
        let mut parents = Vec::with_capacity(raw.parents.len());
        for p in &raw.parents {
            let a = self
                .known_commit(*p)?
                .ok_or_else(|| anyhow!("parent {p} of {id} was not converted"))?;
            parents.push(a);
        }
        let commit = Commit {
            tree,
            parents,
            author: raw.author,
            committer: raw.committer,
            extra_headers: raw.extra_headers,
            message: raw.message,
        };
        let obj = EncodedObject::new(ObjectKind::Commit, &commit.encode());
        let ant = obj.id();
        self.memo.insert(id, ant);
        self.commits.push((ant, id));
        self.push_object(obj)
    }

    fn convert_tree(&mut self, id: gix::ObjectId) -> anyhow::Result<ObjectId> {
        if let Some(a) = self.memo.get(&id) {
            return Ok(*a);
        }
        let raw = self.read(id)?;
        let hash_len = self.repo.object_hash().len_in_bytes();
        let mut entries = Vec::new();
        let mut rest = &raw[..];
        while !rest.is_empty() {
            let malformed = || anyhow!("malformed tree {id}");
            let sp = rest.iter().position(|&b| b == b' ').ok_or_else(malformed)?;
            let nul = sp + 1 + rest[sp + 1..].iter().position(|&b| b == 0).ok_or_else(malformed)?;
            let oid_end = nul + 1 + hash_len;
            if rest.len() < oid_end {
                bail!("malformed tree {id}");
            }
            let name = std::str::from_utf8(&rest[sp + 1..nul])
                .map_err(|_| anyhow!("non-UTF-8 tree entry name in tree {id}"))?
                .to_string();
            let mode = EntryMode::from_octal(&rest[..sp])?;
            let child = gix::ObjectId::from_bytes_or_panic(&rest[nul + 1..oid_end]);
            let child_id = match mode {
                EntryMode::Tree => self.convert_tree(child)?,
                EntryMode::Gitlink => bail!("submodule entry {name:?} in tree {id} is not supported"),
                _ => self.convert_blob(child)?,
            };
            entries.push(TreeEntry { mode, name, id: child_id });
            rest = &rest[oid_end..];
        }
        let tree = Tree::new(entries)?;
        let obj = EncodedObject::new(ObjectKind::Tree, &tree.encode());
        let ant = obj.id();
        self.memo.insert(id, ant);
        self.push_object(obj)?;
        Ok(ant)
    }

    fn convert_blob(&mut self, id: gix::ObjectId) -> anyhow::Result<ObjectId> {
        if let Some(a) = self.memo.get(&id) {
            return Ok(*a);
        }
        let data = self.read(id)?;
        let obj = EncodedObject::new(ObjectKind::Blob, &data);
        let ant = obj.id();
        self.memo.insert(id, ant);
        self.push_object(obj)?;
        Ok(ant)
    }
}

// ---------------------------------------------------------------------------
// Raw git commit parsing
// ---------------------------------------------------------------------------

fn parse_commit(id: gix::ObjectId, raw: &[u8]) -> anyhow::Result<RawCommit> {
    let bad = |what: &str| anyhow!("commit {id}: {what}");
    let (head, message) = match raw.windows(2).position(|w| w == b"\n\n") {
        Some(i) => (&raw[..i], &raw[i + 2..]),
        None => (raw, &raw[raw.len()..]),
    };

    // Fold continuation lines into the previous header, as ant-core does.
    let mut fields: Vec<(String, String)> = Vec::new();
    for line in head.split(|&b| b == b'\n') {
        if line.first() == Some(&b' ') {
            let last = fields.last_mut().ok_or_else(|| bad("orphan continuation line"))?;
            last.1.push('\n');
            last.1.push_str(std::str::from_utf8(&line[1..]).map_err(|_| bad("non-UTF-8 header"))?);
        } else {
            let (k, v) = match line.iter().position(|&b| b == b' ') {
                Some(i) => (&line[..i], &line[i + 1..]),
                None => (line, &b""[..]),
            };
            let k = std::str::from_utf8(k).map_err(|_| bad("non-UTF-8 header"))?;
            let v = std::str::from_utf8(v).map_err(|_| bad("non-UTF-8 header"))?;
            fields.push((k.to_string(), v.to_string()));
        }
    }

    let mut it = fields.into_iter().peekable();
    let tree = match it.next() {
        Some((k, v)) if k == "tree" => gix::ObjectId::from_hex(v.as_bytes()).map_err(|_| bad("bad tree id"))?,
        _ => return Err(bad("missing tree")),
    };
    let mut parents = Vec::new();
    while matches!(it.peek(), Some((k, _)) if k == "parent") {
        if let Some((_, v)) = it.next() {
            parents.push(gix::ObjectId::from_hex(v.as_bytes()).map_err(|_| bad("bad parent id"))?);
        }
    }
    let author = match it.next() {
        Some((k, v)) if k == "author" => parse_signature(id, &v)?,
        _ => return Err(bad("missing author")),
    };
    let committer = match it.next() {
        Some((k, v)) if k == "committer" => parse_signature(id, &v)?,
        _ => return Err(bad("missing committer")),
    };
    let message = String::from_utf8(message.to_vec()).map_err(|_| bad("non-UTF-8 message"))?;
    Ok(RawCommit { tree, parents, author, committer, extra_headers: it.collect(), message })
}

/// `Name <email> <unix-seconds> <+hhmm>`, same rules as `Signature` decoding in ant-core.
fn parse_signature(id: gix::ObjectId, s: &str) -> anyhow::Result<Signature> {
    let bad = || anyhow!("commit {id}: bad signature {s:?}");
    let lt = s.find('<').ok_or_else(bad)?;
    let gt = s.rfind('>').ok_or_else(bad)?;
    if gt < lt {
        bail!(bad());
    }
    let name = s[..lt].trim_end().to_string();
    let email = s[lt + 1..gt].to_string();
    let mut rest = s[gt + 1..].split_whitespace();
    let time: i64 = rest.next().and_then(|t| t.parse().ok()).ok_or_else(bad)?;
    let tz = rest.next().ok_or_else(bad)?;
    if tz.len() != 5 || !matches!(&tz[..1], "+" | "-") {
        bail!(bad());
    }
    let hh: i32 = tz[1..3].parse().map_err(|_| bad())?;
    let mm: i32 = tz[3..5].parse().map_err(|_| bad())?;
    let off = if &tz[..1] == "-" { -(hh * 60 + mm) } else { hh * 60 + mm };
    Ok(Signature { name, email, time, tz_offset_minutes: off })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;

    use super::*;
    use ant_core::{ObjectId, RepoPath};
    use ant_store::graph::lookup_path;
    use ant_store::ObjectStoreExt;

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "Ada")
            .env("GIT_AUTHOR_EMAIL", "ada@example.com")
            .env("GIT_COMMITTER_NAME", "Bob")
            .env("GIT_COMMITTER_EMAIL", "bob@example.com")
            .env("GIT_AUTHOR_DATE", "1700000000 +0530")
            .env("GIT_COMMITTER_DATE", "1700000100 -0700")
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).unwrap()
    }

    /// History with nested dirs, exec bit, symlink, rename, delete, a merge.
    fn make_repo(root: &Path) -> PathBuf {
        let r = root.join("src");
        std::fs::create_dir(&r).unwrap();
        git(&r, &["init", "-q", "-b", "main"]);
        std::fs::create_dir_all(r.join("a/b")).unwrap();
        std::fs::write(r.join("a/b/c.txt"), "c\n").unwrap();
        std::fs::write(r.join("run.sh"), "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(r.join("run.sh"), std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        std::os::unix::fs::symlink("run.sh", r.join("link")).unwrap();
        git(&r, &["add", "-A"]);
        git(&r, &["commit", "-qm", "first\n\nbody line"]);
        git(&r, &["checkout", "-qb", "side"]);
        std::fs::write(r.join("side.txt"), "s\n").unwrap();
        git(&r, &["add", "-A"]);
        git(&r, &["commit", "-qm", "side work"]);
        git(&r, &["checkout", "-q", "main"]);
        git(&r, &["mv", "a/b/c.txt", "a/c.txt"]);
        git(&r, &["rm", "-q", "link"]);
        git(&r, &["commit", "-qm", "move and delete"]);
        git(&r, &["merge", "-q", "--no-edit", "side"]);
        r
    }

    /// git's own SHA-256 conversion as an oracle: commit subject → id.
    fn oracle(root: &Path, src: &Path) -> Vec<(String, String)> {
        let dst = root.join("oracle");
        std::fs::create_dir(&dst).unwrap();
        git(&dst, &["init", "-q", "-b", "main", "--object-format=sha256"]);
        let export = Command::new("git").args(["fast-export", "--all"]).current_dir(src).output().unwrap();
        let mut child = Command::new("git")
            .args(["fast-import", "--quiet"])
            .current_dir(&dst)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child.stdin.take().unwrap().write_all(&export.stdout).unwrap();
        assert!(child.wait().unwrap().success());
        git(&dst, &["log", "--format=%H %s", "main"])
            .lines()
            .map(|l| {
                let (h, s) = l.split_once(' ').unwrap();
                (h.to_string(), s.to_string())
            })
            .collect()
    }

    #[tokio::test]
    async fn import_matches_git_sha256_conversion() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_repo(tmp.path());
        let expected = oracle(tmp.path(), &src);
        assert_eq!(expected.len(), 4);

        let b = Backend::in_memory();
        let stats = import(&src, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap();
        assert_eq!(stats.commits, 4);

        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        assert_eq!(tip.to_hex(), expected[0].0, "tip id must equal git's sha256 conversion");
        for (hex, subject) in &expected {
            let id: ObjectId = hex.parse().unwrap();
            let c = b.objects.read_commit(&id).await.unwrap_or_else(|e| panic!("{subject}: {e}"));
            assert!(c.message.starts_with(subject.as_str()));
            assert!(b.meta.get_commits(&[id]).await.unwrap()[0].is_some(), "{subject} recorded");
        }

        let c = b.objects.read_commit(&tip).await.unwrap();
        assert_eq!(c.author.tz_offset_minutes, 330);
        assert_eq!(c.committer.tz_offset_minutes, -420);
        let f = lookup_path(b.objects.as_ref(), c.tree, &RepoPath::new("a/c.txt").unwrap()).await.unwrap().unwrap();
        assert_eq!(&b.objects.read_blob(&f.1).await.unwrap()[..], b"c\n");
        let node = b.meta.get_commits(&[tip]).await.unwrap().pop().unwrap().unwrap();
        assert_eq!(node.parents.len(), 2);
        assert_eq!(node.generation, 3);
    }

    #[tokio::test]
    async fn reimport_is_incremental() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_repo(tmp.path());
        let b = Backend::in_memory();
        let opts = ImportOptions::single("refs/heads/main", "main");
        import(&src, &b, &opts).await.unwrap();
        let again = import(&src, &b, &opts).await.unwrap();
        assert_eq!(again, ImportStats::default());

        std::fs::write(src.join("new.txt"), "n\n").unwrap();
        git(&src, &["add", "-A"]);
        git(&src, &["commit", "-qm", "later"]);
        let inc = import(&src, &b, &opts).await.unwrap();
        assert_eq!(inc.commits, 1);
        assert_eq!(inc.blobs, 1);
        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        assert!(b.objects.read_commit(&tip).await.unwrap().message.starts_with("later"));
    }
}
