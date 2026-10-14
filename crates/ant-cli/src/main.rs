//! The `ant` command: client for an antimony server, plus `ant serve`. This
//! doc comment is the output contract; `tests/e2e.rs` checks it.
//!
//! Global flags (each also an env var):
//!   --server <url>     ANT_SERVER   gRPC url, default http://127.0.0.1:7420
//!   --http <url>       ANT_HTTP     optional HTTP object base url (e.g. http://127.0.0.1:7421)
//!   --cache <dir>      ANT_CACHE    default ~/.cache/antimony
//!   --cache-bytes <n>  ANT_CACHE_BYTES  default 20 GiB
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
//!   ant ref set <name> <rev> [--expect <id>|--create]   CAS update; mismatch: exit 1
//!   ant ws new <dir> [--at <rev>] [--sparse <pattern>]...
//!       rev default `main`. stdout: `<commit-id>`
//!   ant status                     `A|M|D\t<path>` (sorted)
//!   ant submit -m <msg>            uploads changed blobs (PutObjects), then
//!       CreateChangeset with base = workspace base commit and blob_id ops.
//!       Records the id in `.ant/last_submit`. stdout: `<commit-id>`.
//!       Nothing to submit: exit 1.
//!   ant land [--ref main] [<commit>]   default commit = `.ant/last_submit`.
//!       Landed: stdout `landed <new-tip-id>` (+ ` rebased` if rebased), then
//!       resets the workspace to the new tip. Conflict: stdout `conflict`
//!       followed by one path per line, exit code 2.
//!       stderr then carries a `hint: ...` line on how to proceed.
//!   ant mount <rev> <mountpoint>   read-only FUSE view, foreground until SIGINT/SIGTERM
//!   ant admin reindex-acl <rev> --policy <file> [--full] [--history] <store flags>
//!       Zone-tag objects for access control straight in the stores (after
//!       imports / policy path-rule changes). stdout:
//!       `<commit-id> <trees walked> <trees skipped> <tags written>`.
//!
//! `--token` / ANT_TOKEN: bearer token sent on every gRPC and HTTP request
//! (servers running with `--policy`). `ant import --policy <file>` (or
//! ANT_POLICY) zone-tags the imported history.
//!
//! Workspace commands (status/submit/land) find the workspace by walking up
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

use clap::Parser;

/// A one-line hint for failures with an obvious remedy.
fn hint_for(e: &anyhow::Error) -> Option<&'static str> {
    use tonic::Code;
    if let Some(rpc) = e.downcast_ref::<ctx::RpcError>() {
        return match rpc.code {
            Code::Unauthenticated => Some(
                "set ANT_TOKEN (or --token) to a token whose sha256 is listed in the server's policy",
            ),
            Code::PermissionDenied => {
                Some("your token's principal lacks access to this path or ref; check the policy")
            }
            Code::NotFound if rpc.message.starts_with("ref ") => {
                Some("list the existing refs with `ant ref list`")
            }
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
        Some("list the existing refs with `ant ref list`")
    } else if first.starts_with("not inside an ant workspace") {
        Some("create one with `ant ws new <dir>`, or point at one with -C <dir>")
    } else {
        None
    }
}

fn main() -> ExitCode {
    let cli = args::Cli::parse();
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
        let h = |e: anyhow::Error| hint_for(&e).unwrap_or("");
        assert!(h(rpc(tonic::Code::Unauthenticated, "missing bearer token")).contains("ANT_TOKEN"));
        assert!(h(rpc(tonic::Code::PermissionDenied, "nope")).contains("policy"));
        assert!(h(rpc(tonic::Code::NotFound, "ref x")).contains("ant ref list"));
        assert!(hint_for(&rpc(tonic::Code::NotFound, "path y")).is_none());
        assert!(h(anyhow::anyhow!("ref x not found")).contains("ant ref list"));
        assert!(h(anyhow::anyhow!("not inside an ant workspace (no .ant)")).contains("ws new"));
        assert!(hint_for(&anyhow::anyhow!("something else")).is_none());
    }

    #[test]
    fn rpc_error_displays_bare_message() {
        assert_eq!(rpc(tonic::Code::NotFound, "ref x").to_string(), "ref x");
    }
}
