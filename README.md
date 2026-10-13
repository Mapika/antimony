# antimony

Server-centric source control for **agent fleets on large monorepos**, inspired
by the design described in Anthropic's
[*Redesigning source control*](https://www.anthropic.com/engineering/redesigning-source-control).

Git's object model with SHA-256 ids, but none of git's operating model:
clients are always online, history lives on the server, and nobody pushes.

- **Server-side commits.** An agent sends file edits against a base commit;
  the server builds trees and the commit. Cost scales with the change, not the
  repo.
- **Land instead of push.** `Land` rebases onto the moving tip server-side
  (with 3-way text merges) and advances the ref by compare-and-swap. No merge
  queue, no global lock; conflicts come back as a list of paths. Retries are
  idempotent and stacked commits land together.
- **No packfiles, no repo maintenance.** Objects are content-addressed rows in
  Postgres (small) or S3 (large). Immutable reads are plain HTTP
  (`GET /o/<id>`, `Cache-Control: immutable`), so any CDN or HTTP cache is the
  cache tier.
- **Lazy, sparse clients.** Workspaces materialize only what matches their
  sparse patterns, through a disk cache shared by every agent on the machine;
  a FUSE mount gives an instant read-only view of any commit.
- **API-first.** `ls`, `cat`, `log`, `diff` need no checkout at all.
- **Git interop.** Import any git repo (ids match git's own SHA-256
  conversion exactly); `git clone` (full, `--filter=blob:none`, `--depth`) and
  `git fetch` work against the server for humans and IDEs.

## Layout

| crate | role |
|---|---|
| `ant-core` | object model: SHA-256 ids, blobs/trees/commits (git format), paths |
| `ant-store` | store traits, Postgres / S3 / tiered / in-memory implementations, commit graph |
| `ant-server` | ops (changeset, land, query, history), gRPC, HTTP object endpoint, binary |
| `ant-gitserve` | read-only git smart-HTTP (protocol v2) export |
| `ant-gitbridge` | git → antimony import |
| `antd` | client engine: shared disk cache, read-through, sparse workspaces |
| `ant-fuse` | lazy FUSE view of a tree |
| `ant-auth` | token + path/ref access policy |
| `ant-cli` | the `ant` command |

## Quickstart

```sh
docker compose up -d                      # Postgres + S3-compatible store (SeaweedFS)
cargo build --release

STORE="--pg-url postgres://ant:ant@localhost:54329/ant \
  --s3-endpoint http://localhost:9009 --s3-bucket ant --s3-access-key x --s3-secret-key x"

./target/release/ant import /path/to/git/repo --git-ref refs/heads/main --ref main $STORE
./target/release/ant-server $STORE &      # gRPC :7420, HTTP :7421

ant ls main:src
ant log main -n 5
ant ws new /tmp/ws --at main --sparse src
cd /tmp/ws && $EDITOR src/lib.rs
ant status && ant submit -m "fix" && ant land   # exit code 2 on conflict

git clone http://127.0.0.1:7421/repo.git      # humans and IDEs
```

## Status

Early prototype, built in the open with AI agents. Working end to end against
real Postgres + S3 (see `crates/ant-cli/tests/e2e.rs`), with property-based
tests for codecs, changeset construction and concurrent landing.

In progress: scale benchmarks (fleet load generator, large-repo import),
access control (`ant-auth`), indexed path history and blame, writable lazy
(FUSE copy-on-write) workspaces.

## License

Apache-2.0
