//! The `ant` command: client for an antimony server, plus `ant serve`. This
//! doc comment is the output contract; the end-to-end tests (`tests/*.rs`)
//! exercise it. What may change between releases, and how removals are
//! announced, is in `docs/compatibility.md` (section "CLI output contract").
//!
//! Global flags (each also an env var):
//!   --server <url>     ANT_SERVER   gRPC url, default http://127.0.0.1:7420
//!   --http <url>       ANT_HTTP     optional HTTP object base url (e.g. http://127.0.0.1:7421)
//!   --cache <dir>      ANT_CACHE    default ~/.cache/antimony
//!   --cache-bytes <n>  ANT_CACHE_BYTES  default 20 GiB
//!   --token <t>        ANT_TOKEN    bearer token (see below)
//!   -C <dir>                        start directory for workspace discovery
//! Object reads go through `ReadThrough(DiskObjectStore(cache), RemoteObjectStore)`.
//!
//! Output contract (stdout, one record per line, no decoration). Ids are
//! 64-char lowercase hex. Errors go to stderr as `error: <message>` with exit
//! code 1, except where stated.
//! A failure with an obvious remedy adds a `hint: <text>` line after it.
//!
//!   ant import <git-dir> [--git-ref refs/heads/main] [--ref main] <store flags>
//!       Store flags are exactly ant-server's (`--memory` | `--pg-url` +
//!       `--pg-schema` + optional `--s3-*`, same env vars). Imports straight
//!       into the stores (no server needed).
//!       stdout: `<ref> <tip-id>`
//!   ant ls <rev>[:<path>]
//!       Tree: one line per entry `<mode> <kind> <id>\t<name>` where mode is
//!       git octal (`100644`, `100755`, `120000`, `40000`, `160000`) and kind
//!       is `blob|tree|commit`. If the path is a blob: that single entry.
//!       Missing path: exit 1.
//!   ant cat <rev>:<path>        raw blob bytes. Missing/not a blob: exit 1.
//!   ant blame <rev>:<path>                    `<commit-id> <line-no>\t<line>` per line
//!   ant log [<rev>] [-n N] [--path <path>]   `<id> <subject>` newest first; rev default `main`
//!   ant diff <from-rev> <to-rev>   `A|M|D\t<path>`
//!   ant ref get <name>             `<id>` (missing: exit 1)
//!   ant ref list [<prefix>]        `<id> <name>`
//!   ant ref set <name> <rev> [--expect <id>|--create] [--force]
//!       CAS update; mismatch: exit 1. A move to a commit that does not
//!       descend from the current (or `--expect`ed) target is refused
//!       (exit 1) unless `--force`. stdout: `<id>`
//!   ant ref init [<name>] [-m <msg>]
//!       Creates ref <name> (default `main`) at a new root commit with the empty
//!       tree: the first commit of an empty server. stdout: `<commit-id>`.
//!       Refused (exit 1) if the ref exists.
//!   ant ws new <dir> [--at <rev>] [--sparse <pattern>]... [--lazy]
//!       rev default `main`. stdout: `<commit-id>`. If <rev> is a ref name the
//!       workspace tracks it (`ws update` default).
//!   ant ws remount [<dir>]
//!       Remounts a lazy workspace whose FUSE daemon died (accesses fail with
//!       `Transport endpoint is not connected`; other commands then fail with
//!       `error: lazy workspace mount <dir> is dead ...` and a hint naming this
//!       command) or that was unmounted: detaches the stale mount, restarts the
//!       daemon on the same state (upper layer and journal recovered, edits
//!       kept). <dir> defaults to the dead mount or workspace found from
//!       cwd / `-C`. stdout: `<base-commit-id>`.
//!   ant ws unmount <dir>           unmounts a lazy workspace, keeping its
//!       state (`ws remount` brings it back). stdout: empty.
//!   ant ws update [--to <rev>]
//!       Moves the workspace to <rev> (default: the tracked ref, else
//!       `main`), carrying local edits over: paths changed on one side take
//!       that side; text files changed on both get the server's 3-way land
//!       merge. Overlapping edits are written with `<<<<<<< upstream`,
//!       `||||||| base`, `=======`, `>>>>>>> local` markers; other
//!       both-sides conflicts keep the local version. Conflicts are recorded
//!       in the workspace. stdout: `updated <commit-id>`, then `C\t<path>` per
//!       conflicted path (sorted); with conflicts exit 2 and a `hint:` on
//!       stderr. Refused (exit 1) while conflicts are unresolved. Drops
//!       `.ant/last_submit` when the base changes. Lazy workspaces switch
//!       base and edits atomically (crash-safe); sparse ones only write paths
//!       in their patterns.
//!   ant ws resolve <path>...       marks conflicted paths resolved; refused
//!       (exit 1) for a path that is not conflicted or still has a line
//!       starting with `<<<<<<< ` or `>>>>>>> `. stdout: empty.
//!   ant status                     `A|M|D|C\t<path>` (sorted); `C` = unresolved
//!       conflict from `ws update` (replaces the path's A/M/D line)
//!       Untracked files that match an ignore rule are not listed (nor
//!       submitted). Rules: the `.gitignore` files of the workspace (root and
//!       nested, git semantics: negation, `dir/`, anchoring, `**`) and the
//!       optional workspace-local `.ant/ignore` (root-anchored patterns,
//!       lowest precedence); global git excludes are not read. Tracked files
//!       are always compared, whatever the rules say; an ignored directory is
//!       not entered. `ant status --ignored` also prints `!\t<path>` for each
//!       ignored untracked path (an ignored directory as `<dir>/`, not its
//!       contents), sorted with the other lines. `.gitignore` files are
//!       ordinary files (A/M/D).
//!   ant submit -m <msg>            uploads changed blobs (PutObjects), then
//!       CreateChangeset with base = workspace base commit and blob_id ops.
//!       Records the id in `.ant/last_submit`. stdout: `<commit-id>`.
//!       Nothing to submit, or unresolved conflicts: exit 1. A blob the local
//!       cache evicted meanwhile is re-read from the workspace file (working
//!       tree / lazy upper layer) and uploaded only if it still hashes to the
//!       scanned id; a file edited in that window fails with exit 1 and a
//!       hint to run `ant status` and `ant submit` again.
//!   ant land [--ref main] [<commit>]   default commit = `.ant/last_submit`.
//!       Landed: stdout `landed <new-tip-id>` (+ ` rebased` if rebased), then
//!       resets the workspace to the new tip. Conflict: stdout `conflict`
//!       followed by one path per line, exit code 2.
//!       stderr then carries a `hint: ...` line on how to proceed
//!       (`ant ws update`, resolve, submit and land again).
//!   ant mount <rev> <mountpoint>   read-only FUSE view, foreground until SIGINT/SIGTERM
//!   ant serve <server flags>       the server (`ant_server::serve`), foreground
//!   ant cache stats                the local object cache (`--cache`), by a scan:
//!       `objects <n>`, `bytes <n>` (compressed, as stored), `budget <n>`
//!       (`--cache-bytes`), one per line. The budget is enforced
//!       automatically as objects are cached (least recently used evicted
//!       first, down to 90%); daemons (`mount`, lazy workspaces) also
//!       enforce it periodically.
//!   ant cache gc [--to <bytes>]    evicts least-recently-used objects now: down
//!       to 90% of the budget if the cache is over it, or, with `--to`, down
//!       to at most that many bytes (`--to 0` empties it). Waits for another
//!       running eviction. stdout: `removed <n>`, `freed <bytes>`,
//!       `objects <n>`, `bytes <n>` (the last two: what is left).
//!   ant completions <shell>        shell completion script on stdout
//!   ant admin reindex-acl <rev> --policy <file> [--full] [--history] <store flags>
//!       Zone-tag objects for access control straight in the stores (after
//!       imports / policy path-rule changes). stdout:
//!       `<commit-id> <trees walked> <trees skipped> <tags written>`.
//!   ant admin schema [--migrate] <store flags>
//!       Postgres schema version. With `--migrate`, first applies pending
//!       migrations: `migrated <version> <name>` per migration. Then
//!       `version <n>` (0: none recorded, i.e. a new or pre-versioning
//!       schema), `supported <n>` (this binary's version) and
//!       `pending <version> <name>` per migration still to apply. A schema
//!       newer than supported: exit 1 with a `hint:` to upgrade.
//!   ant admin gc [--dry-run] [--grace 7d] [--quiesce 30m] [--max-batches N] [--abort] <store flags>
//!       Garbage collection (Postgres stores; packs too with `--s3-*`).
//!       Progress on stderr (`gc: ...`). stdout: first line `dry-run`, `done`
//!       or `stopped` (resumable: run again), plus ` resumed` when it
//!       continued an interrupted run; then `cutoff <time>`, `live <n>`,
//!       `condemned <n>`, `objects <count> <bytes>`,
//!       `large-objects <count> <bytes>`, `commits <n>`,
//!       `changesets <count> <bytes>`; except for dry runs `legacy-changes
//!       <n>`, `derived <n>`, `kept <n>`; with S3 `packs <count> <bytes>` and
//!       `packs-kept <count> <bytes> <dead-bytes> <dead-percent>%`. Dry runs
//!       report what a run would delete. `--abort`: `aborted` or
//!       `nothing to abort`. A grace period under 1d: exit 1.
//!
//! Store flags (`ant import`, `ant serve`, `ant admin`) include
//! `--no-migrate` (ANT_NO_MIGRATE): by default the Postgres schema is
//! created or migrated on start; with it, a schema that needs migrations is
//! an error (exit 1, `hint:` naming `ant admin schema --migrate`). Data
//! written by a newer version (schema, disk cache, workspace state) is
//! always refused with a `hint:` to upgrade ant.
//!
//! `--token` / ANT_TOKEN: bearer token sent on every gRPC and HTTP request
//! (servers running with `--policy`). `ant import --policy <file>` (or
//! ANT_POLICY) zone-tags the imported history.
//!
//! Workspace commands (status/submit/land/ws update/ws resolve) find the workspace by walking up
//! from the current directory to the first dir containing `.ant/workspace`;
//! `-C <dir>` overrides the starting directory.
//!
//! Author identity for submit: ANT_AUTHOR_NAME / ANT_AUTHOR_EMAIL, else
//! `git config user.name`/`user.email`, else `$USER` / `$USER@localhost`;
//! time = now, tz offset 0.

mod args;
mod author;
mod commands;
mod ctx;
mod discover;
mod rev;

use std::io::Write as _;
use std::process::ExitCode;

const LIST_REFS_HINT: &str = "list the existing refs with `ant ref list`";

/// A one-line hint for failures with an obvious remedy.
fn hint_for(e: &anyhow::Error) -> Option<String> {
    if let Some(dead) = e.downcast_ref::<discover::DeadMount>() {
        return Some(format!(
            "your edits are safe in the workspace state; run `ant ws remount {}` to remount it",
            dead.0.display()
        ));
    }
    static_hint(e).map(str::to_string)
}

fn static_hint(e: &anyhow::Error) -> Option<&'static str> {
    use tonic::Code;
    // Data from a newer version, or a schema needing migration (before the
    // generic "connecting to" hint: these come wrapped in such a context).
    if let Some(hint) = ant_store::compat_hint(e.chain()) {
        return Some(hint);
    }
    if let Some(rpc) = e.downcast_ref::<ctx::RpcError>() {
        return match rpc.code {
            Code::Unauthenticated => Some(
                "set ANT_TOKEN (or --token) to a token whose sha256 is listed in the server's policy",
            ),
            Code::PermissionDenied => {
                Some("your token's principal lacks access to this path or ref; check the policy")
            }
            Code::NotFound if rpc.message.starts_with("ref ") => Some(LIST_REFS_HINT),
            Code::Unavailable => Some("is `ant serve` running? set ANT_SERVER (or --server)"),
            _ => None,
        };
    }
    let first = e.to_string();
    if first.starts_with("connecting to ") {
        Some("is `ant serve` running? set ANT_SERVER (or --server) to its gRPC url")
    } else if first.contains("HOME is not set") {
        Some("pass --cache <dir> or set ANT_CACHE")
    } else if first.starts_with("ref ") && first.ends_with(" not found") {
        Some(LIST_REFS_HINT)
    } else if first.starts_with("not inside an ant workspace") {
        Some("create one with `ant ws new <dir>`, or point at one with -C <dir>")
    } else if first.starts_with("workspace has unresolved conflicts") {
        Some("fix the files `ant status` marks C, then run `ant ws resolve <path>...`")
    } else if first.ends_with("still contains conflict markers") {
        Some("edit out the <<<<<<< ... >>>>>>> sections, then run `ant ws resolve` again")
    } else if first.ends_with(" is not conflicted") {
        Some("`ant status` lists the conflicted paths as `C<TAB><path>`")
    } else if first.starts_with("non-fast-forward update of ref ") {
        Some("pass --force to move it anyway; commits only the old target reaches leave the ref")
    } else {
        None
    }
}

fn main() -> ExitCode {
    let cli = args::parse();
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: starting runtime: {e}");
            return ExitCode::from(1);
        }
    };
    let code = match rt.block_on(commands::run(cli)) {
        Ok(out) => {
            let mut stdout = std::io::stdout().lock();
            match stdout.write_all(&out.stdout).and_then(|()| stdout.flush()) {
                Ok(()) => out.code,
                Err(e) => {
                    eprintln!("error: writing output: {e}");
                    1
                }
            }
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            if let Some(hint) = hint_for(&e) {
                eprintln!("hint: {hint}");
            }
            1
        }
    };
    // Drop the runtime without waiting on lingering background tasks.
    rt.shutdown_background();
    ExitCode::from(code as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rpc(code: tonic::Code, message: &str) -> anyhow::Error {
        anyhow::Error::new(ctx::RpcError { code, message: message.into() })
    }

    #[test]
    fn hints_by_error_kind() {
        let h = |e: anyhow::Error| hint_for(&e).unwrap_or_default();
        assert!(h(rpc(tonic::Code::Unauthenticated, "missing bearer token")).contains("ANT_TOKEN"));
        assert!(h(rpc(tonic::Code::PermissionDenied, "nope")).contains("policy"));
        assert!(h(rpc(tonic::Code::NotFound, "ref x")).contains("ant ref list"));
        assert!(hint_for(&rpc(tonic::Code::NotFound, "path y")).is_none());
        assert!(h(anyhow::anyhow!("ref x not found")).contains("ant ref list"));
        assert!(h(anyhow::anyhow!("not inside an ant workspace (no .ant)")).contains("ws new"));
        assert!(hint_for(&anyhow::anyhow!("something else")).is_none());
        assert!(h(anyhow::anyhow!("workspace has unresolved conflicts: a")).contains("ws resolve"));
        assert!(h(anyhow::anyhow!("a still contains conflict markers")).contains("ws resolve"));
        assert!(h(anyhow::anyhow!("a is not conflicted")).contains("ant status"));
        assert!(h(anyhow::anyhow!("non-fast-forward update of ref main: x")).contains("--force"));
    }

    #[test]
    fn hints_for_newer_data_and_pending_migrations() {
        use ant_store::StoreError;
        let h = |e: anyhow::Error| hint_for(&e).unwrap_or_default();
        let newer = ant_core::NewerFormat { what: "x".into(), found: 9, supported: 1 };
        // Wrapped in the "connecting to" context, which has a hint of its own.
        let e =
            anyhow::Error::new(StoreError::from(newer.clone())).context("connecting to Postgres");
        assert!(h(e).starts_with("upgrade ant"));
        let e = anyhow::Error::new(std::io::Error::other(newer.clone())).context("opening cache");
        assert!(h(e).starts_with("upgrade ant"));
        assert!(h(anyhow::Error::new(newer)).starts_with("upgrade ant"));
        let e = anyhow::Error::new(StoreError::MigrationRequired {
            schema: "ant".into(),
            found: 1,
            required: 4,
        })
        .context("connecting to Postgres");
        assert!(h(e).contains("ant admin schema --migrate"));
    }

    #[test]
    fn dead_mount_hint_names_the_directory() {
        let e = anyhow::Error::new(discover::DeadMount("/w/ws".into()));
        assert!(e.to_string().starts_with("lazy workspace mount /w/ws is dead"));
        assert!(hint_for(&e).unwrap().contains("ant ws remount /w/ws"));
    }

    #[test]
    fn rpc_error_displays_bare_message() {
        assert_eq!(rpc(tonic::Code::NotFound, "ref x").to_string(), "ref x");
    }
}
