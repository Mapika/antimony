//! TASK (cli): the `ant` command.
//!
//! Global flags (also env): `--server <grpc url>` (ANT_SERVER, default
//! http://127.0.0.1:7420), `--http <base url>` (ANT_HTTP, optional),
//! `--cache <dir>` (ANT_CACHE, default ~/.cache/antimony),
//! `--cache-bytes` (default 20 GiB).
//! Object reads go through `ReadThrough(DiskObjectStore(cache), RemoteObjectStore)`.
//!
//! Commands (output is plain text, stable, script-friendly):
//!   ant import <git-dir> [--git-ref refs/heads/main] [--ref main]
//!       [--memory | --pg-url ... --s3-* ...]   imports straight into the
//!       server's stores (same store flags as ant-server); remote import
//!       over gRPC is future work.
//!   ant ls <rev>[:<path>]          tree listing: `<mode> <kind> <id>\t<name>`
//!   ant cat <rev>:<path>           blob content to stdout
//!   ant log [<rev>] [-n N] [-- <path>]   `<id> <subject>` per line
//!   ant diff <from-rev> <to-rev>   `A|M|D\t<path>`
//!   ant ws new <dir> [--at <rev>] [--sparse <pattern>]...
//!   ant status   (in a workspace)  `A|M|D\t<path>`
//!   ant submit -m <msg>            upload changed blobs, CreateChangeset on
//!                                  base; print new commit id
//!   ant land [--ref main] [<commit>]   Land (default: last submitted commit
//!                                  recorded in .ant/); on success reset the
//!                                  workspace to the new tip; on conflict
//!                                  print paths, exit code 2
//!   ant mount <rev> <mountpoint>   read-only FUSE view (foreground until Ctrl-C)
//!   ant ref get|set|list ...
//!
//! Author identity: ANT_AUTHOR_NAME / ANT_AUTHOR_EMAIL, else git config
//! user.name/user.email, else "$USER" / "$USER@localhost".

fn main() {
    todo!("cli")
}
