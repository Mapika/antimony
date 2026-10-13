//! TASK (cli): the `ant` command. This doc comment is the contract shared by
//! the implementation and `tests/e2e.rs`.
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
//!   ant mount <rev> <mountpoint>   read-only FUSE view, foreground until SIGINT/SIGTERM
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
            1
        }
    };
    // Drop the runtime without waiting on lingering background tasks.
    rt.shutdown_background();
    ExitCode::from(code as u8)
}
