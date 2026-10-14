//! ls / cat / blame / log / diff via RPCs (no checkout).

use std::fmt::Write as _;

use ant_core::{EntryMode, RepoPath};
use ant_proto::v1 as pb;
use anyhow::{Result, anyhow, bail};

use crate::ctx::{Ctx, Outcome, RpcResult, id_from, kind_name, mode_from_pb};
use crate::rev::parse_rev_path;

pub async fn ls(ctx: &Ctx, target: &str) -> Result<Outcome> {
    let (rev, path) = parse_rev_path(target)?;
    let path = path.unwrap_or("");
    let mut client = ctx.remote().await?.client();
    let r = client
        .read_path(pb::ReadPathRequest {
            rev: rev.into(),
            path: path.into(),
            include_content: false,
        })
        .await
        .rpc()?
        .into_inner();
    if !r.found {
        bail!("{target}: no such path");
    }
    let mut out = String::new();
    let mode = mode_from_pb(r.mode)?;
    if mode == EntryMode::Tree {
        for e in r.entries {
            let mode = mode_from_pb(e.mode)?;
            writeln!(
                out,
                "{} {} {}\t{}",
                mode.as_octal(),
                kind_name(mode),
                id_from(&e.id)?,
                e.name
            )?;
        }
    } else {
        let name = path.rsplit('/').next().unwrap_or(path);
        writeln!(out, "{} {} {}\t{}", mode.as_octal(), kind_name(mode), id_from(&r.id)?, name)?;
    }
    Ok(Outcome::ok(out))
}

pub async fn cat(ctx: &Ctx, target: &str) -> Result<Outcome> {
    let (rev, path) = parse_rev_path(target)?;
    let path = path.filter(|p| !p.is_empty()).ok_or_else(|| anyhow!("cat needs <rev>:<path>"))?;
    let mut client = ctx.remote().await?.client();
    let r = client
        .read_path(pb::ReadPathRequest {
            rev: rev.into(),
            path: path.into(),
            include_content: true,
        })
        .await
        .rpc()?
        .into_inner();
    if !r.found {
        bail!("{target}: no such path");
    }
    if !mode_from_pb(r.mode)?.is_blob() {
        bail!("{target}: not a blob");
    }
    Ok(Outcome::ok(r.content))
}

/// The single path filter of `ant log` (`--path <p>` or `-- <p>`).
pub fn one_path(flag: Option<String>, trailing: Vec<String>) -> Result<Option<String>> {
    match (flag, trailing.len()) {
        (flag, 0) => Ok(flag),
        (None, 1) => Ok(trailing.into_iter().next()),
        _ => bail!("log takes one path (--path <path> or -- <path>)"),
    }
}

/// Commits per `LogRange` page while printing a range.
const RANGE_PAGE: u32 = 1000;

pub async fn log(
    ctx: &Ctx,
    rev: &str,
    limit: Option<u32>,
    path: Option<String>,
) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let path = path.map(|p| p.trim_matches('/').to_string());
    let commits = match rev.split_once("..") {
        Some((from, to)) => {
            if from.is_empty() || to.is_empty() {
                bail!("bad range {rev:?}: use <from>..<to>");
            }
            let mut left = limit.map(|l| l as usize).filter(|&l| l > 0);
            let mut commits = Vec::new();
            let mut token = String::new();
            loop {
                let want = left.map_or(RANGE_PAGE, |l| (l as u32).min(RANGE_PAGE));
                let r = client
                    .log_range(pb::LogRangeRequest {
                        from_exclusive: from.into(),
                        to: to.into(),
                        path: path.clone(),
                        limit: want,
                        page_token: token,
                    })
                    .await
                    .rpc()?
                    .into_inner();
                if let Some(l) = left.as_mut() {
                    *l = l.saturating_sub(r.commits.len());
                }
                commits.extend(r.commits);
                if r.next_page_token.is_empty() || left == Some(0) {
                    break;
                }
                token = r.next_page_token;
            }
            commits
        }
        None => {
            client
                .log(pb::LogRequest { rev: rev.into(), path, limit: limit.unwrap_or(0) })
                .await
                .rpc()?
                .into_inner()
                .commits
        }
    };
    let mut out = String::new();
    for c in commits {
        let subject = String::from_utf8_lossy(&c.message);
        let subject = subject.lines().next().unwrap_or("");
        writeln!(out, "{} {}", id_from(&c.id)?, subject)?;
    }
    Ok(Outcome::ok(out))
}

pub async fn merge_base(ctx: &Ctx, a: &str, b: &str) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let r = client
        .merge_base(pb::MergeBaseRequest { a: a.into(), b: b.into() })
        .await
        .rpc()?
        .into_inner();
    match r.commit {
        Some(id) => Ok(Outcome::ok(format!("{}\n", id_from(&id)?))),
        None => bail!("{a} and {b} have no common ancestor"),
    }
}

pub async fn landed_as(ctx: &Ctx, commit: &str, ref_name: Option<&str>) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let r = client
        .landed_as(pb::LandedAsRequest {
            commit: commit.into(),
            ref_name: ref_name.unwrap_or_default().into(),
        })
        .await
        .rpc()?
        .into_inner();
    if !r.landed {
        match ref_name {
            Some(name) => bail!("{commit} has not landed on {name}"),
            None => bail!("{commit} has not landed on any ref"),
        }
    }
    let mut out = format!("{} {}", id_from(&r.landed_commit)?, r.ref_name);
    if r.rebased {
        out.push_str(" rebased");
    }
    out.push('\n');
    Ok(Outcome::ok(out))
}

pub async fn blame(ctx: &Ctx, target: &str) -> Result<Outcome> {
    let (rev, path) = parse_rev_path(target)?;
    let path = path.filter(|p| !p.is_empty()).ok_or_else(|| anyhow!("blame needs <rev>:<path>"))?;
    let mut client = ctx.remote().await?.client();
    let r = client
        .blame(pb::BlameRequest { rev: rev.into(), path: path.into() })
        .await
        .rpc()?
        .into_inner();
    let mut out = String::new();
    for (i, l) in r.lines.iter().enumerate() {
        writeln!(out, "{} {}\t{}", id_from(&l.commit)?, i + 1, l.line)?;
    }
    Ok(Outcome::ok(out))
}

/// `ant diff` flags beyond the two revs.
pub struct DiffFlags {
    pub patch: bool,
    pub merge_base: bool,
    pub context: Option<u32>,
    pub paths: Vec<String>,
}

pub async fn diff(ctx: &Ctx, from: &str, to: &str, flags: DiffFlags) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let r = client
        .diff(pb::DiffRequest {
            from_rev: from.into(),
            to_rev: to.into(),
            path_prefixes: flags.paths.iter().map(|p| p.trim_matches('/').to_string()).collect(),
            merge_base: flags.merge_base,
            patch: flags.patch,
            context_lines: flags.context,
        })
        .await
        .rpc()?
        .into_inner();
    // Servers older than these request fields ignore them: refuse rather
    // than print a different diff.
    if flags.merge_base && r.from_commit.is_none() {
        bail!("the server does not support merge-base diffs (upgrade the server)");
    }
    let has_content = |m: i32| {
        matches!(
            pb::EntryMode::try_from(m),
            Ok(pb::EntryMode::File | pb::EntryMode::Executable | pb::EntryMode::Symlink)
        )
    };
    if flags.patch
        && r.changes
            .iter()
            .any(|c| c.patch.is_none() && (has_content(c.old_mode) || has_content(c.new_mode)))
    {
        bail!("the server does not support patches (upgrade the server)");
    }
    let prefixes: Vec<RepoPath> =
        flags.paths.iter().map(|p| RepoPath::new(p.trim_matches('/'))).collect::<Result<_, _>>()?;
    let mut out = String::new();
    for c in r.changes {
        if !prefixes.is_empty()
            && !RepoPath::new(c.path.as_str())
                .is_ok_and(|p| prefixes.iter().any(|prefix| p.starts_with(prefix)))
        {
            continue;
        }
        let kind = pb::ChangeKind::try_from(c.kind);
        let letter = match kind {
            Ok(pb::ChangeKind::Added) => 'A',
            Ok(pb::ChangeKind::Modified) => 'M',
            Ok(pb::ChangeKind::Deleted) => 'D',
            _ => bail!("server sent unknown change kind {}", c.kind),
        };
        if flags.patch {
            write_patch(&mut out, &c, letter)?;
        } else {
            writeln!(out, "{letter}\t{}", c.path)?;
        }
    }
    if r.patch_truncated {
        crate::ctx::print_hint(
            "the server's patch size limit was reached; later files show `Large files ... differ`; \
             narrow the diff with -- <path>...",
        );
    }
    Ok(Outcome::ok(out))
}

/// One file of `ant diff --patch`, in the unified format `git apply` reads.
fn write_patch(out: &mut String, c: &pb::PathChange, letter: char) -> Result<()> {
    let p = &c.path;
    writeln!(out, "diff --git a/{p} b/{p}")?;
    let octal = |m: i32| mode_from_pb(m).map(|m| m.as_octal());
    match letter {
        'A' => writeln!(out, "new file mode {}", octal(c.new_mode)?)?,
        'D' => writeln!(out, "deleted file mode {}", octal(c.old_mode)?)?,
        _ if c.old_mode != c.new_mode => {
            writeln!(out, "old mode {}", octal(c.old_mode)?)?;
            writeln!(out, "new mode {}", octal(c.new_mode)?)?;
        }
        _ => {}
    }
    let Some(patch) = &c.patch else { return Ok(()) };
    let old = if letter == 'A' { "/dev/null".to_string() } else { format!("a/{p}") };
    let new = if letter == 'D' { "/dev/null".to_string() } else { format!("b/{p}") };
    if patch.binary {
        writeln!(out, "Binary files {old} and {new} differ")?;
        return Ok(());
    }
    if patch.too_large {
        writeln!(out, "Large files {old} and {new} differ")?;
        return Ok(());
    }
    if patch.hunks.is_empty() {
        return Ok(());
    }
    writeln!(out, "--- {old}")?;
    writeln!(out, "+++ {new}")?;
    let range = |start: u32, lines: u32| {
        if lines == 1 { start.to_string() } else { format!("{start},{lines}") }
    };
    for h in &patch.hunks {
        writeln!(
            out,
            "@@ -{} +{} @@",
            range(h.old_start, h.old_lines),
            range(h.new_start, h.new_lines)
        )?;
        for line in &h.lines {
            writeln!(out, "{line}")?;
        }
    }
    Ok(())
}
