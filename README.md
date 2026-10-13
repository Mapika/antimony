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
indexed path history and blame, writable lazy
(FUSE copy-on-write) workspaces.

## Access control and threat model

`ant-server --policy policy.toml` (or `ANT_POLICY`) turns on access control;
without it the server runs in open mode and logs a warning. The policy
format (principals with SHA-256-hashed bearer tokens, longest-prefix path
rules, ref rules, admins) is documented in `crates/ant-auth`. Clients send
`ANT_TOKEN`: as `authorization: Bearer` on gRPC and HTTP, and as the Basic
auth password (any user name) for `git clone`. Missing or unknown tokens get
`unauthenticated` / 401, denials `permission_denied` / 403.

**What is protected**

- *File content under unreadable paths*: `ReadPath`, `Blame`, `Diff` and
  `Log --path` behave as if those paths did not exist.
- *Objects by id*: every blob and tree written under a restricted path rule
  (a *zone*) is tagged with that zone, so `GetObjects`, `HasObjects` and
  `GET /o/<id>` report it exactly like a missing object (same code, body and
  headers). Content is readable by id if *any* place it is stored is
  readable: the same bytes at a public path are not secret. Objects outside
  the root zone are served with `Cache-Control: private`.
- *Read by copy*: `CreateChangeset` may reference a blob by id only if the
  caller can read it or uploaded it; otherwise it is "not found". Fresh
  uploads stay hidden from everyone until a changeset places them.
- *Writes*: every changed path must be writable — for `CreateChangeset` ops,
  for every commit `Land` replays (or, for a fast-forward, the net tree diff,
  which also stops reverts disguised as merges), and for `UpdateRef` moves
  (tree diff between old and new target). `Land`/`UpdateRef` also need the
  ref rule. Checks run per request before a land joins a group-commit batch,
  so a denied request never affects others. Only admins may upload trees or
  commits.
- *git export*: needs read access to everything (403 otherwise), because the
  export cannot filter trees without changing commit ids.

**Known v1 limitations**

- Structure is not secret: readable trees (e.g. the root) list restricted
  entry *names and ids*; clients need them to sparse-skip. Commit metadata
  (messages, authors, ids, parents) is visible to every authenticated
  principal, including `Log` without a path.
- Objects written before the policy existed, by `ant import`, or by an admin
  through `PutObjects` (trees/commits) are untagged, i.e. public, until
  `ant admin reindex-acl <rev> --history` (`ant import --policy` runs it).
  Path-rule changes need a reindex; read/write list changes do not.
- Creating a ref (land onto a missing ref, `UpdateRef` with no expected
  value) needs only the ref rule: there is no previous content to protect.
- Tagging races can only over-restrict: concurrent first writes of identical
  bytes to a public and a restricted path may leave the object hidden by id
  (path reads still work). Error codes are uniform, but response timing
  still differs slightly between missing and hidden objects.
- Workspaces of principals who cannot read everything must be sparse around
  restricted paths. The client disk cache is shared per machine: principals
  sharing a cache directory share whatever it holds.
- Tokens are bearer secrets: run behind TLS.

## License

Apache-2.0
