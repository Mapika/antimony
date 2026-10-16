# Using antimony

Day-to-day details that go beyond the [README](../README.md) quickstart. The
exact output of every command is the CLI output contract in the module docs
of `crates/ant-cli/src/main.rs`; what it keeps stable between releases is in
[compatibility.md](compatibility.md). Running a server for a fleet is in the
[operator guide](operator-guide.md); flags, imports and upgrades are in
[operations.md](operations.md).

## Exit codes, conflicts and `ws update`

Exit codes: `0` ok, `1` error (`error: ...` on stderr, plus `hint: ...` when
there is an obvious fix), `2` land or update conflict. After a land conflict
(or to pick up others' work) run `ant ws update`: it moves the workspace to
the tip of the ref it was created at (or `--to <rev>`) and carries local
edits over with the server's 3-way merge. Overlapping edits are written with
`<<<<<<< upstream` / `>>>>>>> local` markers and shown as `C` by `ant status`;
fix them, run `ant ws resolve <path>...`, then submit and land again.
`ant ref set` refuses to move a ref backwards or sideways without `--force`.

## Lazy workspaces and read-only views

`ant ws new /tmp/lazy --lazy` mounts a writable copy-on-write workspace over
FUSE (needs `/dev/fuse` and `fusermount3`; `ant ws unmount /tmp/lazy` keeps
the edits; if its FUSE daemon dies, `ant ws remount /tmp/lazy` recovers the
workspace with its edits), and `ant mount main /tmp/view` gives a read-only
view. Lazy views are Linux only. When to choose sparse or lazy:
[operator guide, "Sparse or lazy?"](operator-guide.md#sparse-or-lazy).

## One binary, finding the server, seeding an empty server

One binary, `ant`, is both the server and the client; the standalone
`ant-server` binary is a thin wrapper around `ant serve`. Clients find the
server via `ANT_SERVER` (default `http://127.0.0.1:7420`).
`ant serve --memory` starts an empty throwaway server. `ant import` writes
straight to the stores, so it cannot feed an in-memory server; instead create
the first commit with `ant ref init [<ref>]` (an empty root commit) and fill it
from a workspace with `ant submit` and `ant land`, as in the Quickstart.
`ant completions <bash|zsh|fish|...>` prints a shell completion script.

## Access control in two minutes

Restart the server with a policy; `/healthz`, `/readyz` and `/metrics` stay
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

What a policy protects, and what it does not, is in
[access-control.md](access-control.md).

## Importing a large history

`ant import` converts in parallel and keeps memory flat (a fixed-size memo, byte-bounded
caches); the Linux kernel's 1.5M commits need about 1.5 GiB of anonymous memory. Large
objects (above `--inline-threshold`) are bundled into packs in S3 with an index in
Postgres, so S3 sees thousands of large requests instead of millions of small ones.
For a one-off bulk load, `ANT_PG_BULK=1` commits object inserts without waiting for the
WAL flush; commit records and refs still commit synchronously, so a crash can only lose
objects that nothing references yet. `ANT_IMPORT_WORKERS` sets the conversion threads.
The ref is published with a compare-and-swap against the tip the import started from: a
land that moves the ref meanwhile is never overwritten (the import fails; its commits are
recorded and a re-run publishes them). With `ANT_IMPORT_DEFERRED=1` on Postgres, objects,
commits, changesets and mappings are acknowledged before their WAL flush. Each target ref
gets a private witness lock (`import-witness/<ref>`, taken before any write; lands on the
ref are not blocked), and before the swap the witness session commits a marker ref
synchronously, which proves that no database crash happened and flushes everything before
it. A crash mid-import leaves a consistent prefix and a re-run continues incrementally.
It is off by default: it cuts `put_commits` about 10x but, in measurements on a
disk-bound machine, the wait moved to the object writes without a net gain.

The import command line and capacity planning are in
[operations.md](operations.md#importing-an-existing-repository).

## Docker

```sh
docker build -t antimony .                       # one image, entrypoint `ant`
docker compose --profile server up -d --build    # postgres + s3 + `ant serve` on :7420/:7421
ANT_SERVER=http://127.0.0.1:7420 ant ref list
```

The image is `debian:bookworm-slim` + `fuse3`. Containers that use FUSE mounts
need `--device /dev/fuse --cap-add SYS_ADMIN`. Any `ant` command works as the
container command, e.g. `docker run antimony import ...`. Released images are
published as `ghcr.io/mapika/antimony:<version>`.

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
process. `--pg-lock-timeout` (default `10s`) bounds the wait for a ref lock. `ant-bench soak` exercises this setup end to end.

Topology, sizing and the failure runbook are in the
[operator guide](operator-guide.md).
