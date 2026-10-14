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

## Documentation

- [Architecture](docs/architecture.md): object model, storage, write and read
  paths, clients, git interop, access control.
- [Operations](docs/operations.md): running a server, imports, policies,
  sizing, local services, benchmarks.
- [Operator guide](docs/operator-guide.md): topology, sizing, fleet workflow,
  metrics and alerts, failure runbook, backups.

## Layout

| crate | role |
|---|---|
| `ant-core` | object model: SHA-256 ids, blobs/trees/commits (git format), paths |
| `ant-store` | store traits, Postgres / S3 / tiered / in-memory implementations, commit graph |
| `ant-server` | ops (changeset, land, query, history), gRPC, HTTP object endpoint, binary |
| `ant-gitserve` | read-only git smart-HTTP (protocol v2) export |
| `ant-gitbridge` | git → antimony import |
| `antd` | client engine: shared disk cache, read-through, sparse workspaces |
| `ant-fuse` | lazy FUSE views: read-only and copy-on-write workspaces |
| `ant-auth` | token + path/ref access policy |
| `ant-cli` | the `ant` command |
| `ant-bench` | load and scale harness |

## Quickstart

Needs Rust, `git` and `curl`. It runs against an in-memory throwaway server;
for a durable one add Postgres and an S3-compatible store (`docker compose up
-d`, or `scripts/dev-services.sh start` without Docker) and see "Importing a
large history" below. Run it from the repository root; the demo lives in
`/tmp/ant-demo`.

```sh
cargo build --release                      # one binary: target/release/ant (server + client)
export PATH=$PWD/target/release:$PATH

# 1. Start an empty in-memory server (gRPC :7420, HTTP :7421).
rm -rf /tmp/ant-demo && mkdir -p /tmp/ant-demo
ant serve --memory > /tmp/ant-demo/serve.log 2>&1 &
SERVER_PID=$!
until curl -sf http://127.0.0.1:7421/healthz >/dev/null; do sleep 0.2; done

# 2. Create the first commit (an empty root commit on `main`), then put some files in it.
ant ref init main                          # prints: <root-commit-id>
ant ws new /tmp/ant-demo/seed
mkdir /tmp/ant-demo/seed/src
printf 'pub fn answer() -> u32 {\n    41\n}\n' > /tmp/ant-demo/seed/src/lib.rs
echo '# demo' > /tmp/ant-demo/seed/README.md
(cd /tmp/ant-demo/seed && ant submit -m "initial import" && ant land)    # landed <id>

# 3. Read without checking anything out.
ant ls main                                # <mode> <kind> <id><TAB><name>
ant cat main:src/lib.rs
ant log main -n 5

# 4. Two agents, two sparse workspaces, one file: the first lands, the second conflicts.
ant ws new /tmp/ant-demo/a --sparse src
ant ws new /tmp/ant-demo/b --sparse src
echo '// from a' >> /tmp/ant-demo/a/src/lib.rs
echo '// from b' >> /tmp/ant-demo/b/src/lib.rs
cd /tmp/ant-demo/a && ant status && ant submit -m "a: comment" && ant land   # landed <id>
cd /tmp/ant-demo/b && ant submit -m "b: comment"; ant land; echo "exit $?"    # conflict, src/lib.rs, exit 2

# 5. Carry b's edit onto the new tip: both appended at the same spot, so resolve and retry.
ant ws update; echo "exit $?"              # updated <id>, C<TAB>src/lib.rs, exit 2
ant status                                 # C<TAB>src/lib.rs
cat src/lib.rs                             # <<<<<<< upstream ... >>>>>>> local
printf 'pub fn answer() -> u32 {\n    41\n}\n// from a\n// from b\n' > src/lib.rs
ant ws resolve src/lib.rs && ant submit -m "b: comment" && ant land        # landed <id>

# 6. History.
ant log main --path src/lib.rs
ant diff main~1 main                       # M<TAB>src/lib.rs
ant blame main:src/lib.rs

# 7. Humans and IDEs use plain git; Prometheus scrapes the HTTP port.
git clone -q http://127.0.0.1:7421/repo.git /tmp/ant-demo/clone
curl -s http://127.0.0.1:7421/metrics | grep '^ant_land_outcomes'

# 8. Clean up.
kill $SERVER_PID; cd / && rm -rf /tmp/ant-demo
```

Exit codes: `0` ok, `1` error (`error: ...` on stderr, plus `hint: ...` when
there is an obvious fix), `2` land or update conflict. After a land conflict
(or to pick up others' work) run `ant ws update`: it moves the workspace to
the tip of the ref it was created at (or `--to <rev>`) and carries local
edits over with the server's 3-way merge. Overlapping edits are written with
`<<<<<<< upstream` / `>>>>>>> local` markers and shown as `C` by `ant status`;
fix them, run `ant ws resolve <path>...`, then submit and land again.
`ant ref set` refuses to move a ref backwards or sideways without `--force`.

Optional: `ant ws new /tmp/lazy --lazy` mounts a writable copy-on-write
workspace over FUSE (needs `/dev/fuse` and `fusermount3`; `ant ws unmount
/tmp/lazy` keeps the edits; if its FUSE daemon dies, `ant ws remount /tmp/lazy`
recovers the workspace with its edits), and `ant mount main /tmp/view` gives a read-only
view.

### Access control in two minutes

Restart the server with a policy; `/healthz` and `/metrics` stay
unauthenticated (use `--metrics-listen 127.0.0.1:9100` to move metrics off the
public port).

```sh
TOKEN=$(openssl rand -hex 16)
cat > policy.toml <<EOF
[[principal]]
name = "agent-1"
token_sha256 = "$(printf %s "$TOKEN" | sha256sum | cut -d' ' -f1)"
admin = true

[[path]]
prefix = ""
read = ["*"]
write = ["*"]

[[ref]]
pattern = "main"
update = ["agent-1"]
land = ["*"]
EOF
ant serve --policy policy.toml &
ant log main -n 1                          # error: missing bearer token (+ hint)
ANT_TOKEN=$TOKEN ant log main -n 1
git clone http://x:$TOKEN@127.0.0.1:7421/repo.git   # the token is the Basic-auth password
```

One binary, `ant`, is both the server and the client; the standalone
`ant-server` binary is a thin wrapper around `ant serve`. Clients find the
server via `ANT_SERVER` (default `http://127.0.0.1:7420`).
`ant serve --memory` starts an empty throwaway server. `ant import` writes
straight to the stores, so it cannot feed an in-memory server; instead create
the first commit with `ant ref init [<ref>]` (an empty root commit) and fill it
from a workspace with `ant submit` and `ant land`, as in the Quickstart.
`ant completions <bash|zsh|fish|...>` prints a shell completion script.

### Importing a large history

`ant import` converts in parallel and keeps memory flat (a fixed-size memo, byte-bounded
caches); the Linux kernel's 1.5M commits need about 1.5 GiB of anonymous memory. Large
objects (above `--inline-threshold`) are bundled into packs in S3 with an index in
Postgres, so S3 sees thousands of large requests instead of millions of small ones.
For a one-off bulk load, `ANT_PG_BULK=1` commits object inserts without waiting for the
WAL flush; commit records and refs still commit synchronously, so a crash can only lose
objects that nothing references yet. `ANT_IMPORT_WORKERS` sets the conversion threads.

### Docker

```sh
docker build -t antimony .                       # one image, entrypoint `ant`
docker compose --profile server up -d --build    # postgres + s3 + `ant serve` on :7420/:7421
ANT_SERVER=http://127.0.0.1:7420 ant ref list
```

The image is `debian:bookworm-slim` + `fuse3`. Containers that use FUSE mounts
need `--device /dev/fuse --cap-add SYS_ADMIN`. Any `ant` command works as the
container command, e.g. `docker run antimony import ...`.

## Running several server replicas

Any number of `ant serve` processes can share one Postgres (and S3). A server
keeps no state that matters across requests besides caches of immutable data,
so replicas are interchangeable. Lands on one ref are batched per process;
the batch leaders of different processes take turns through a per-ref
Postgres advisory lock, held across reading the tip, replaying and the CAS.
The ref CAS remains the correctness guarantee; the lock only avoids racing.

Each process opens up to `--pg-max-connections` (default 64) query connections
plus `--pg-lock-connections` (default 8) lock connections. Size them so that
replicas × (both) stays under the database's `max_connections`. Setting
`--pg-lock-connections 0` disables the locks, which is fine for a single
process. `ant-bench soak` exercises this setup end to end.

## Status

Early prototype, built in the open with AI agents. Working end to end against
real Postgres + S3 (see `crates/ant-cli/tests/e2e.rs`), with property-based
tests for codecs, changeset construction and concurrent landing.

Also implemented: indexed path history and blame, and writable lazy
workspaces (`ant ws new --lazy`, FUSE copy-on-write). In progress: scale
benchmarks (`ant-bench`: fleet load generator, large-repo import, checkout).

## Access control and threat model

`ant serve --policy policy.toml` (or `ANT_POLICY`) turns on access control;
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
