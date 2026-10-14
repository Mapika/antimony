# Operator guide: running antimony for a fleet of agents

This guide is for the person who runs the server and owns the fleet. It
complements [operations.md](operations.md) (flags, imports, policy basics, the
upgrade procedure) and [architecture.md](architecture.md) (how it works).

**About the numbers.** Every performance figure here was measured on a single
WSL2 development machine (20 cores, Postgres and SeaweedFS S3 running locally
on the same box and disk, see the 2026-10-14 scorecard in
[scorecard/](scorecard/2026-10-14.md) and the soak/import notes in
[ROADMAP.md](../ROADMAP.md)). They are useful for ratios and orders of
magnitude. Network latency, managed Postgres, real S3 and separate hosts will
all move them; measure your own deployment with `ant-bench` before trusting a
capacity plan.

## 1. Topology

```
 agents (ant CLI / gRPC)          humans, IDEs (git clone)        CDN / HTTP cache (optional)
          |                                 |                              |
          +--------------- TLS-terminating load balancer ------------------+
                                 |  gRPC :7420      HTTP :7421
                +----------------+----------------+
          ant serve #1      ant serve #2      ant serve #N     (stateless, interchangeable)
                +----------------+----------------+
                      |                          |
                 PostgreSQL >= 15           S3-compatible store
          (refs, commit graph, changesets,    (objects > 4 KiB, zstd)
           small objects, history index)
```

| piece | count | notes |
|---|---|---|
| `ant serve` | 1..N | Same binary as the client. Listens on `--grpc-listen` (default `127.0.0.1:7420`) and `--http-listen` (default `127.0.0.1:7421`); set both to a routable address in production. Does not terminate TLS. |
| Load balancer | 1 | Must speak gRPC (HTTP/2) on 7420 and plain HTTP on 7421. Any replica can serve any request; no stickiness needed. |
| PostgreSQL | 1 | The single source of truth for refs and metadata. Run it with whatever HA you normally use; antimony has no opinion beyond "one primary". |
| S3-compatible store | 1 | Bucket is created on first start. Holds only objects larger than `--inline-threshold` (default 4096 bytes). |
| HTTP cache / CDN | optional | Put it in front of `GET /o/<id>` only. Responses are `Cache-Control: public, max-age=31536000, immutable` with an `ETag`. Objects in restricted zones are served `private` (a shared cache must not store them) and unknown ids get `no-store`. Pass the CDN base URL to clients via `ANT_HTTP`. |

**Replicas and coordination.** Replicas share nothing except Postgres and S3.
Lands on one ref are group-committed per process (up to 64 requests per
batch). Between processes, the batch leader takes a per-ref Postgres advisory
lock (a session-level `pg_advisory_lock` on a hash of schema and ref name, on
a dedicated "lock" connection), and keeps it for turns of about 20 ms before
releasing it so other replicas get their turn. The lock is advisory: the ref
compare-and-swap is what guarantees correctness. If the lock cannot be taken
(wait over `--pg-lock-timeout`, default 10 s, or `--pg-lock-connections 0`) the replica logs
`ref lock unavailable; landing with CAS only` and lands without it; a lost
CAS replays the batch from the new tip (counted in
`ant_land_cas_retries_total`) and after 32 attempts the requests are aborted.
Conflicts between replicas therefore cost latency, never correctness.

**What is stateless.** A replica holds only caches of immutable data
(trees, commits, commit-graph nodes, changesets; each cache has a fixed byte
budget, see sizing) and in-flight land queues. You can kill, add or replace
replicas at any time. Nothing on a replica needs backing up. Clients likewise
hold only disposable state, except lazy-workspace edits (see section 6).

**One replica or several?** One replica is enough for a lot (the scorecard's
single process sustained 4,066 lands/s at 64 agents). Add replicas for
availability, for CPU headroom on read-heavy fleets, or to spread connection
load. For a single replica you can set `--pg-lock-connections 0`.

## 2. Sizing (measured)

### Landing and writes

All from `scripts/scorecard.sh` on the dev machine, Postgres + S3, in-process
load generator (`ant-bench fleet`):

| agents | lands/s | land p99 | aborts |
|---|---|---|---|
| 1 | 260.6 | 1.8 ms | 0 |
| 16 | 2,226.7 | 6.8 ms | 0 |
| 64 | 4,066.0 | 29.5 ms | 0 |

ROADMAP records a median of 3,869 lands/s, p50 8.8 ms and p99 37.8 ms (27 ms
best) for 64 agents over several runs, so expect run-to-run spread of about
10 percent. `create_changeset` p50 is 2.0-3.0 ms at 16 agents (the scorecard row
was 3.0 ms against a target of under 3). Commit creation is bounded by one
durable Postgres commit per land batch (about 5-8 ms per synchronous commit on
that disk): your Postgres fsync latency is the first number to check on a new
deployment.

A realistic fleet does not land continuously. The multi-replica soak (below)
drives 1,000 agents who think about 500 ms between operations: 266k lands in
30 minutes is about 148 lands/s across all replicas (plus the reads), far
below the single-process ceiling above. Estimate your own peak as the number
of agents that finish a unit of work in the same second, then compare with the
table.

### Reads

Scorecard (96k-file-class checkouts and a git/git-sized history): `log -n 20`
6 ms, indexed `log --path` 34 ms, `blame` 622 ms first and 10 ms cached,
`status` of a warm full checkout under 0.1 s. Queries need no checkout, so
agents should prefer `ant ls/cat/log/diff/blame` to materializing files.

### Soak: 4 replicas x 1,000 agents

`ant-bench soak` spawns N `ant serve` processes on one Postgres schema (and
optional S3), drives simulated agents (create+land, cat, ls, log, path log,
status) and fails the run on: a non-linear or wrong-length ref history, an
agent file whose content is not its last landed content, a dead replica, an
error rate above `--max-error-rate` (0.001), an RSS growth trend above
`--max-rss-growth-mib-per-hour` (256) after warm-up, or a window p99 more than
`--max-p99-drift` (2x) its first window and above `--p99-floor-ms` (5 ms).
The recorded result in ROADMAP: **4 replicas x 1,000 agents x 30 minutes: 266k
lands, 0 errors, exact history, flat RSS.** Replica RSS in that run was about
610-660 MiB with default caches. (Landing p99 under the soak still spikes
when Postgres as a whole stalls, for example at checkpoints; ROADMAP tracks
it.)

Reproduce a smaller version:

```sh
cargo run --release -p ant-bench -- soak --replicas 2 --agents 300 --duration 600 \
  --pg-url postgres://ant:ant@localhost:54329/ant --s3-endpoint http://localhost:9009
```

### Memory per replica

Budget about 700 MiB per replica to start (soak: 610-660 MiB at 1,000 agents
with default caches). The in-process caches have fixed budgets: trees 64 MiB,
commits 8 MiB, graph nodes 16 MiB, changesets 16 MiB, in two generations (a
cache can briefly hold up to twice its budget). Watch `ant_cache_bytes` and
the process RSS; on glibc you can set `ANT_MALLOC_TRIM_SECS=<n>` to call
`malloc_trim` every n seconds (off by default) if RSS stays high after bursts.
`GET /debug/stats` on the HTTP port prints cache entries and budgets, land
queue counts and running history catch-ups as one JSON line. Without a policy
it is open; with `--policy` it needs an admin token (`Authorization: Bearer
<token>`, or Basic with the token as password): `401` without a valid token,
`403` for a non-admin principal. It is not available on the `--metrics-listen`
port.

### Postgres connection budget

Each replica opens up to `--pg-max-connections` query connections (default 64)
plus `--pg-lock-connections` lock connections (default 8; this bounds the
number of distinct refs being landed on at once per replica). Waiting for a
ref lock is bounded by `--pg-lock-timeout` (`ANT_PG_LOCK_TIMEOUT`, default
`10s`; accepts `500ms`, `10s`, `2m`); on expiry the land proceeds with CAS only.
The rule:

```
replicas x (pg-max-connections + pg-lock-connections)  <  Postgres max_connections - headroom
```

The defaults need 72 connections per replica, so a stock Postgres
(`max_connections = 100`) fits exactly one replica. The dev services run with
`max_connections = 500`. The soak shares a budget (`--pg-budget`, default 400)
across replicas, with 4 lock connections each. Pick a headroom for admin
sessions and your monitoring (10-20 is a reasonable start), then divide. A
smaller query pool costs throughput only when it is saturated: alert on
`ant_pg_pool_in_use` close to `ant_pg_pool_max`. If you front Postgres with a
connection pooler, note that ref locks are session-level advisory locks and
the durable-publish design relies on a dedicated session, so use session
pooling (or connect the lock pool directly); transaction pooling will break
them. This is a constraint of the design, not something measured.

### Storage growth

From the full Linux kernel import (1.49M commits): about 160 GB of objects,
most of it in S3, plus about 17 GB of Postgres tables (`objects` about 9 GB,
`changes` about 7 GB). Per-commit metadata (changesets, log, history index) is
the Postgres-resident part and grows linearly with commits and the paths they
touch; objects up to 4 KiB also live in Postgres. For an agent fleet the
growth driver is lands x average change size, since every land is a commit
with its own tree objects. A land stores new trees along every edited path,
so deep paths cost more. There is no packing or garbage collection of
unreferenced objects (see failure modes: crashed batches leave unreachable
rows, which are harmless). Under sustained bulk loads autovacuum on `objects`
becomes visible; give Postgres I/O headroom.

### Client cache

The shared per-machine disk cache has a 20 GiB budget by default
(`--cache-bytes` / `ANT_CACHE_BYTES`), enforced automatically: when the
cache grows past the budget, the least recently used objects (by file
mtime; reads refresh it, at most once a minute per object) are evicted
down to 90 percent of it. Processes sharing a cache count their writes in
`gc/usage` and take `gc/evict.lock` so only one evicts at a time; long-lived
daemons (`ant mount`, lazy workspaces) also check every five minutes. The
limit is approximate: expect overshoot of about 1/64 of the budget per
concurrently writing process plus the batch in flight. An evicted object
is simply fetched again on its next use. `ant cache stats` prints
`objects`, `bytes` and `budget`; `ant cache gc [--to <bytes>]` evicts now
(for example `--to 0` to empty it, or after lowering the budget). Every
process using one cache should be given the same `--cache-bytes`. Size it to the working set of the paths your agents
touch, multiplied by the number of distinct bases they check out.
Sparse workspaces keep checkouts proportional to what an agent touches. Full
96k-file checkout on the dev machine: 4.2-4.7 s cold, 1.4-1.9 s warm,
`status` 0.09-0.18 s (ROADMAP).

## 3. Starting a replica

```sh
ant serve \
  --pg-url postgres://ant:SECRET@pg.internal:5432/ant --pg-schema ant \
  --pg-max-connections 40 --pg-lock-connections 8 \
  --s3-endpoint https://s3.internal --s3-bucket antimony --s3-region us-east-1 \
  --s3-access-key ... --s3-secret-key ... \
  --grpc-listen 0.0.0.0:7420 --http-listen 0.0.0.0:7421 \
  --metrics-listen 10.0.0.5:9100 \
  --policy /etc/antimony/policy.toml --log-format json
```

Every flag has an `ANT_*` environment variable (`ANT_PG_URL`,
`ANT_PG_MAX_CONNECTIONS`, `ANT_S3_PREFIX`, ...); `ant serve --help` lists them
all. (The help also shows client flags such as `--server` and `--cache`; they
have no effect on the server.) Without `--policy` the server is in open mode
and logs `no --policy given: OPEN mode`; never run that on a network. The
liveness endpoint `GET /healthz` returns `ok` as soon as the HTTP listener
runs and does **not** check Postgres or S3.

Readiness is `GET /readyz` on the HTTP port. It is unauthenticated (also under
a policy) and cheap: a `SELECT 1` on the Postgres query pool and a `HEAD` on
the S3 bucket, run concurrently, each bounded by 2 s, no connection kept.
Healthy (`200`; unconfigured dependencies are omitted, in-memory is just
`{"status":"ok"}`):

```
{"status":"ok","postgres":"ok","s3":"ok"}
```

Otherwise `503` with each failing dependency as `down` or `timeout`, for
example `{"status":"unavailable","postgres":"down","s3":"ok"}`. Details are in
the server log, not the body. A saturated query pool also reads as `timeout`.

Load balancer: use `/readyz` as the health check that removes a replica from
rotation (every 5-10 s, fail after 2-3 misses, so that a Postgres blip does
not flap), and `/healthz` for the process supervisor or Kubernetes
`livenessProbe` (restarting a replica does not fix a down database, so do not
use `/readyz` for liveness). `/readyz` requests are counted under
`route="readyz"`.

`--memory` starts an empty throwaway server. It cannot be seeded by
`ant import`; create its first commit with `ant ref init main` and fill it
from a workspace (`ant ws new`, `ant submit`, `ant land`), or use a scratch
Postgres schema (`--pg-schema scratch`) to import a git repository.

## 4. Agent workflow at fleet scale

### The per-agent loop

```sh
# once per agent (token in ANT_TOKEN, server in ANT_SERVER, shared cache dir in ANT_CACHE)
ant ws new /work/agent-7 --sparse services/billing --sparse libs/common   # prints the base commit id

# edit files, then
cd /work/agent-7
ant status                       # A|M|D|C <TAB> path
ant submit -m "billing: fix rounding"     # prints the commit id, remembers it in .ant/last_submit
ant land                         # "landed <tip>" (+ " rebased"), exit 0
```

On conflict, `ant land` prints `conflict` and the conflicting paths and exits
**2**, with a `hint:` line on stderr. Verified output:

```
$ ant land
hint: main moved under you; run `ant ws update --to main` to merge your edits onto the new tip, resolve any conflicts, then submit and land again
conflict
src/lib.rs
[exit 2]
$ ant ws update
hint: fix the listed files (text conflicts carry <<<<<<< markers; ...), then run `ant ws resolve <path>...` before submitting
updated db0aedef...
C	src/lib.rs
[exit 2]
```

Then the agent edits the file (removing `<<<<<<< upstream`, `||||||| base`,
`=======`, `>>>>>>> local`), runs `ant ws resolve src/lib.rs`, then
`ant submit` and `ant land` again. `ws resolve` refuses a path that still
contains marker lines. `ant ws update` also works without a land conflict, to
pick up other agents' work before continuing; it is refused while conflicts are
unresolved. The server already does a 3-way text merge when landing, so most
overlapping-file cases land as `rebased` with no agent involvement; an agent
sees exit 2 only for true overlaps (same hunk) or non-text conflicts.

Scripting rules for harnesses:

- Exit codes: `0` ok, `1` error (`error: ...` on stderr, optional `hint: ...`),
  `2` land/update conflict. Stdout is machine-readable, one record per line
  (contract in the module docs of `crates/ant-cli/src/main.rs`).
- **Retries are idempotent.** Re-running `ant land` for the same commit
  returns `landed <same tip id>` again (verified: landing the same submit
  twice yields the same id; the second run is served from the `landed-as`
  mapping). So a harness may retry `land` on any transport error or on gRPC
  `aborted` (see below) without fear of duplicates. Metrics count a retried
  land again as `landed`.
- Treat `aborted` as retryable (CAS contention exhausted, a database
  failover mid-batch, a permission check that raced a ref move). Use
  exponential backoff with jitter; do not hammer.
- Exit 1 with `unavailable` or `connecting to ...` means no replica was
  reachable: retry at the harness level.

### Sparse or lazy?

| | sparse (`ws new --sparse`) | lazy (`ws new --lazy`) |
|---|---|---|
| mechanism | real files on disk for matching paths only | FUSE view of the whole tree, content fetched on first read, writes copy-on-write |
| needs | nothing special | `/dev/fuse` and `fusermount3`; in Docker `--device /dev/fuse --cap-add SYS_ADMIN` |
| best for | agents that know their area (services, packages); simplest and most robust; build tools see ordinary files | agents that must browse the whole monorepo, or whose toolchain follows imports anywhere |
| `status` cost | stat comparison, only changed files hashed | proportional to the number of changes |
| failure surface | none beyond the disk | a daemon process per workspace (section 6) |

Default to sparse. Choose lazy when the set of paths is unknowable up front.
Both kinds share one disk cache per machine, so two hundred agents on one host
download each object once. Give each agent its own workspace directory;
workspaces are not shared between agents.

Workspace commands find the workspace by walking up from the current directory;
`-C <dir>` overrides the start. Author identity for `submit` comes from
`ANT_AUTHOR_NAME` / `ANT_AUTHOR_EMAIL` (else `git config`, else `$USER`), which
is how you attribute commits to agents.

### Tokens, principals and ref naming

Server-side identity is a principal with a hashed bearer token; clients send it
as `ANT_TOKEN`. Give **each agent its own principal** if you want per-agent
audit (the principal name appears in slow-request logs) and revocation (remove
the entry, restart replicas; changes to read/write lists need no reindex, path
rule changes need `ant admin reindex-acl <rev> --history`). If that is too many
principals, use one per team or per agent pool. Generate and hash tokens like
this and store only the hash in the policy:

```sh
TOKEN=$(openssl rand -hex 16)
printf %s "$TOKEN" | sha256sum | cut -d' ' -f1      # -> token_sha256
```

A policy that lets any agent land on `main` but lets only an admin move it by
hand, and gives each agent a private namespace for experiments (verified
against a running server):

```toml
[[principal]]
name = "admin"
token_sha256 = "..."
admin = true

[[principal]]
name = "agent-1"
token_sha256 = "..."

[[path]]
prefix = ""
read = ["*"]
write = ["*"]

[[ref]]
pattern = "main"
update = ["admin"]      # ant ref set / UpdateRef
land = ["*"]            # ant land

[[ref]]
pattern = "agents/agent-1/*"
update = ["agent-1"]
land = ["agent-1"]
```

With this policy `ant land --ref agents/agent-1/try1` creates and lands onto
that ref, and `ant ref set main <id> --force` as agent-1 fails with
`error: cannot update ref main` plus the hint to check the policy. For team
isolation add `[[path]]` rules with narrower `prefix`, `read` and `write`
lists, and make the workspaces of principals that cannot read everything
sparse around the restricted paths (see the README threat model). The git
export (`/repo.git`) needs read access to everything.

Ref names are `/`-separated (`main`, `release/1.2`, `agents/agent-7/fix`).
Recommended convention: one long-lived integration ref (`main`), release refs
under `release/`, and per-agent or per-task scratch refs under `agents/<id>/`
or `tasks/<id>/`. Everything lands on `main` through `Land`; the server
queues and merges, so you do not need a merge queue. Refs are cheap, but the
per-ref metric labels cover only the 64 most recently active refs (everything
else reports as `ref="other"`), and ref locks are one per ref being landed
on, so avoid designs that need thousands of hot refs at once. Prefer deleting
finished scratch refs.

## 5. Observability

### What to scrape

`GET /metrics` on the HTTP port, or, preferably, on a private listener with
`--metrics-listen 10.0.0.5:9100` (the endpoint then exists only there). It is
unauthenticated. Scrape every replica separately (do not scrape through the
load balancer). When the metrics listener is separate the HTTP port answers
`/metrics` with `404`, with or without a policy.

Families (all names verified in `crates/ant-server/src/metrics.rs`):

| family | labels | use |
|---|---|---|
| `ant_rpc_requests_total`, `ant_rpc_duration_seconds` | `method`, `code` | gRPC traffic, errors and latency per method; `code` is the snake_case status |
| `ant_http_requests_total`, `ant_http_duration_seconds` | `route`, `status` | `objects`, `git`, `healthz`, `readyz`, `metrics`, `other` |
| `ant_land_outcomes_total` | `outcome` = `landed`, `rebased`, `conflict`, `aborted`, `denied` | land results |
| `ant_land_batch_size` | histogram | requests per group-commit batch |
| `ant_land_cas_retries_total` | | batches replayed after losing the ref CAS |
| `ant_land_queue_depth` | `ref` | requests waiting now |
| `ant_store_op_duration_seconds`, `ant_store_errors_total` | `store` = `objects`/`refs`/`meta`, `op` | every Postgres/S3 call; `refs`/`lock_ref` is the ref-lock wait, `refs`/`cas_ref` the compare-and-swap |
| `ant_cache_hits_total`, `ant_cache_misses_total`, `ant_cache_bytes` | `cache` = `trees`/`commits`/`nodes`/`changesets` | in-process caches (counters since process start) |
| `ant_history_lag_commits` | `ref` | commits the path-history index is behind the ref |
| `ant_history_catch_up_seconds` | histogram | catch-ups that indexed commits |
| `ant_pg_pool_size`, `_idle`, `_in_use`, `_max` | | query pool (the lock pool is not included) |
| `ant_build_info` | `version`, `git` | always 1 |

Notes: `ant_land_outcomes_total` counts one per land request. `rebased` is a
successful land that the server rebased, so total successes are
`landed + rebased`. Errors other than `aborted` and `denied` (for example a
store failure surfacing as `internal`) are not in that family; they show in
`ant_rpc_requests_total{method="Land",code!="ok"}`. `ant_history_lag_commits`
always has a `ref="other"` series, plus one per recently active ref; alert on
`max`. Counters per histogram family also export `_bucket`, `_sum` and
`_count`.

### Suggested alerts (PromQL)

Tune the thresholds to your own baseline; these are starting points.

```promql
# 1. Land conflict rate (agents colliding, or a hot shared file). Page-worthy only if sustained.
sum(rate(ant_land_outcomes_total{outcome="conflict"}[10m]))
  / clamp_min(sum(rate(ant_land_outcomes_total[10m])), 1e-9) > 0.2

# 2. Aborted lands (CAS exhaustion, DB failover, store failure). Should be ~0.
sum(rate(ant_land_outcomes_total{outcome="aborted"}[5m])) > 0

# 3. Land latency p99 (v1 target on the dev machine: < 100 ms at 64 agents)
histogram_quantile(0.99,
  sum by (le) (rate(ant_rpc_duration_seconds_bucket{method="Land"}[5m]))) > 0.25

# 4. Land queue depth growing (replica cannot keep up, or the ref lock/CAS is slow)
max by (instance, ref) (ant_land_queue_depth) > 64     # one full batch waiting, for 5m

# 5. Cross-replica contention: share of batches that lost the CAS
sum(rate(ant_land_cas_retries_total[10m]))
  / clamp_min(sum(rate(ant_land_batch_size_count[10m])), 1e-9) > 0.25

# 6. History index not catching up
max by (instance, ref) (ant_history_lag_commits{ref!="other"}) > 500   # for 10m

# 7. Store errors (Postgres or S3 failing)
sum by (store, op) (rate(ant_store_errors_total[5m])) > 0

# 8. Store latency: durable commit / CAS p99
histogram_quantile(0.99,
  sum by (le) (rate(ant_store_op_duration_seconds_bucket{store="refs",op="cas_ref"}[5m]))) > 0.05

# 9. Ref lock wait (a stuck or slow lock holder shows up here first)
histogram_quantile(0.99,
  sum by (le) (rate(ant_store_op_duration_seconds_bucket{store="refs",op="lock_ref"}[5m]))) > 1

# 10. Postgres pool saturation
max by (instance) (ant_pg_pool_in_use / ant_pg_pool_max) > 0.9      # for 5m

# 11. Cache hit ratio (informational; a low trees ratio with high RSS means the working set exceeds the cache)
sum by (cache) (rate(ant_cache_hits_total[15m]))
  / clamp_min(sum by (cache) (rate(ant_cache_hits_total[15m]) + rate(ant_cache_misses_total[15m])), 1e-9) < 0.5

# 12. Server errors per RPC (anything but ok / expected client errors)
sum by (method, code) (rate(ant_rpc_requests_total{code=~"internal|unavailable|unknown|data_loss"}[5m])) > 0

# 13. Replica down
up{job="antimony"} == 0
```

Do not alert on `denied` unless you expect none (it counts policy refusals on
`Land`). A rising `conflict` rate is an agent-behavior signal, not a server
fault: look at which paths conflict (agents print them) and consider
narrowing sparse patterns or splitting a shared file.

### Logs and request ids

`--log-format json|text` (`ANT_LOG_FORMAT`; default text on a terminal, json
otherwise) writes to **stderr**. The level comes from `RUST_LOG` (default
`info,sqlx=warn`). In JSON each line carries its enclosing spans, so every log
line of a request has the request's `request_id`:

```json
{"timestamp":"2026-10-14T10:39:27.804441Z","level":"INFO","fields":{"message":"access control enabled","policy":"policy.toml"},"target":"ant_server::serve"}
```

Every gRPC and HTTP request has an id: an incoming `x-request-id` (1-128
visible ASCII characters) is kept, otherwise the server generates a ULID. It is
echoed in the `x-request-id` response header, so an agent harness can set its
own and quote it in a bug report, and you can `grep` the logs for it. Verified:
`curl -H 'x-request-id: my-req-1' .../o/<id>` returns `x-request-id: my-req-1`.

Requests that take **1 second or more** log at WARN as `slow request`, with
`what` (method or route), `outcome`, `duration_ms` and, under a policy, the
authenticated `principal`. Other WARN lines worth alerting on:
`ref lock unavailable; landing with CAS only`, `releasing ref lock failed` and
`history catch-up failed`.

### Dashboard

Panel list for a minimal board (one row per concern; use `$instance` as a
template variable on every panel):

1. Lands/s by outcome: `sum by (outcome) (rate(ant_land_outcomes_total[1m]))`
2. Land p50/p99: `histogram_quantile(0.99, sum by (le) (rate(ant_rpc_duration_seconds_bucket{method="Land"}[1m])))`
3. Land queue depth and batch size: `max by (instance) (ant_land_queue_depth)`, `rate(ant_land_batch_size_sum[1m]) / rate(ant_land_batch_size_count[1m])`
4. CAS retries/s: `rate(ant_land_cas_retries_total[1m])`
5. RPC rate and errors by method/code
6. Store p99 by `store,op` and store errors
7. Postgres pool: in use vs max, idle
8. History lag per ref
9. Cache hit ratio and `ant_cache_bytes` by cache
10. HTTP `/o` request rate and p99 (CDN miss traffic)
11. Process RSS and CPU (from node/process exporter)

A compact importable Grafana dashboard (Grafana 10+, Prometheus datasource
chosen at import; panels 1-4 and 6-9 of the list above):

```json
{"title":"antimony","schemaVersion":39,"time":{"from":"now-1h","to":"now"},
 "templating":{"list":[{"name":"DS_PROM","type":"datasource","query":"prometheus"},
  {"name":"instance","type":"query","datasource":"${DS_PROM}","query":"label_values(ant_build_info, instance)","includeAll":true,"multi":true,"current":{"text":"All","value":"$__all"}}]},
 "panels":[
 {"id":1,"type":"timeseries","title":"Lands/s by outcome","gridPos":{"x":0,"y":0,"w":12,"h":8},"datasource":"${DS_PROM}",
  "targets":[{"expr":"sum by (outcome) (rate(ant_land_outcomes_total{instance=~\"$instance\"}[1m]))","legendFormat":"{{outcome}}"}]},
 {"id":2,"type":"timeseries","title":"Land latency p50 / p99","gridPos":{"x":12,"y":0,"w":12,"h":8},"datasource":"${DS_PROM}","fieldConfig":{"defaults":{"unit":"s"}},
  "targets":[{"expr":"histogram_quantile(0.5, sum by (le) (rate(ant_rpc_duration_seconds_bucket{method=\"Land\",instance=~\"$instance\"}[1m])))","legendFormat":"p50"},
             {"expr":"histogram_quantile(0.99, sum by (le) (rate(ant_rpc_duration_seconds_bucket{method=\"Land\",instance=~\"$instance\"}[1m])))","legendFormat":"p99"}]},
 {"id":3,"type":"timeseries","title":"Land queue depth / CAS retries per s","gridPos":{"x":0,"y":8,"w":12,"h":8},"datasource":"${DS_PROM}",
  "targets":[{"expr":"max by (instance) (ant_land_queue_depth{instance=~\"$instance\"})","legendFormat":"queue {{instance}}"},
             {"expr":"sum by (instance) (rate(ant_land_cas_retries_total{instance=~\"$instance\"}[1m]))","legendFormat":"cas retries {{instance}}"}]},
 {"id":4,"type":"timeseries","title":"Store p99 by op","gridPos":{"x":12,"y":8,"w":12,"h":8},"datasource":"${DS_PROM}","fieldConfig":{"defaults":{"unit":"s"}},
  "targets":[{"expr":"histogram_quantile(0.99, sum by (le, store, op) (rate(ant_store_op_duration_seconds_bucket{instance=~\"$instance\"}[1m])))","legendFormat":"{{store}}.{{op}}"}]},
 {"id":5,"type":"timeseries","title":"Store errors/s","gridPos":{"x":0,"y":16,"w":8,"h":8},"datasource":"${DS_PROM}",
  "targets":[{"expr":"sum by (store, op) (rate(ant_store_errors_total{instance=~\"$instance\"}[1m]))","legendFormat":"{{store}}.{{op}}"}]},
 {"id":6,"type":"timeseries","title":"Postgres pool","gridPos":{"x":8,"y":16,"w":8,"h":8},"datasource":"${DS_PROM}",
  "targets":[{"expr":"ant_pg_pool_in_use{instance=~\"$instance\"}","legendFormat":"in use {{instance}}"},
             {"expr":"ant_pg_pool_max{instance=~\"$instance\"}","legendFormat":"max {{instance}}"}]},
 {"id":7,"type":"timeseries","title":"History lag (commits)","gridPos":{"x":16,"y":16,"w":8,"h":8},"datasource":"${DS_PROM}",
  "targets":[{"expr":"max by (instance, ref) (ant_history_lag_commits{instance=~\"$instance\"})","legendFormat":"{{ref}} {{instance}}"}]},
 {"id":8,"type":"timeseries","title":"Cache hit ratio","gridPos":{"x":0,"y":24,"w":12,"h":8},"datasource":"${DS_PROM}","fieldConfig":{"defaults":{"unit":"percentunit","max":1}},
  "targets":[{"expr":"sum by (cache) (rate(ant_cache_hits_total{instance=~\"$instance\"}[5m])) / clamp_min(sum by (cache) (rate(ant_cache_hits_total{instance=~\"$instance\"}[5m]) + rate(ant_cache_misses_total{instance=~\"$instance\"}[5m])), 1e-9)","legendFormat":"{{cache}}"}]},
 {"id":9,"type":"timeseries","title":"Cache bytes","gridPos":{"x":12,"y":24,"w":12,"h":8},"datasource":"${DS_PROM}","fieldConfig":{"defaults":{"unit":"bytes"}},
  "targets":[{"expr":"sum by (cache) (ant_cache_bytes{instance=~\"$instance\"})","legendFormat":"{{cache}}"}]}
 ]}
```

## 6. Failure modes and runbook

Background for the Postgres cases (details in `ant-store` `lib.rs`,
`postgres.rs` and `ops/land/queue.rs`). A land batch writes its objects,
changesets and commit records with *deferred durability*
(`synchronous_commit = off`: acknowledged without waiting for the WAL flush).
It then publishes the ref move plus the "landed-as" mappings in **one
synchronous commit on the same Postgres session that holds the ref's advisory
lock**. That commit flushes everything before it, and it can only succeed if
the session is still the one opened before the writes: a database crash or
restart ends every session, so if anything unflushed was lost, the publish
fails instead. Clients are only told "landed" after the publish. Hence no ref
ever points at lost objects, and a land that was acknowledged is durable.

| situation | symptom | impact | action |
|---|---|---|---|
| **Postgres restart or failover** | burst of `ant_store_errors_total`, `ant_land_outcomes_total{outcome="aborted"}`, RPC `aborted`/`internal`; growing `ant_pg_pool_*` churn; `ant_land_queue_depth` rises then drains | In-flight lands: if the lock session died, the publish fails, the batch is aborted, and agents get `aborted` (not "landed"); because retries are idempotent, they re-run `ant land`. Reads fail until the new primary is reachable. Unpublished objects stay as unreachable rows. No acknowledged land is lost, no ref points at missing data. Replicas reconnect on their own (sqlx pool); if the failover changes the hostname, restart the replicas with the new URL. | Nothing to repair. Make sure the harness retries `aborted`. After a failover, check that the new primary has the same data (a **promoted asynchronous replica can lose acknowledged commits**; that is a Postgres replication property: use synchronous replication if you cannot afford it). |
| **S3 outage or throttling** | `ant_store_errors_total{store="objects"}` (ops `get_many`/`put_many`), `internal` RPC codes, `/o/<id>` 5xx for large objects | Objects up to 4 KiB live in Postgres and keep working, so many small-file operations survive. Anything touching a large object fails: reads of those files, submits and lands that contain them. Metadata queries (`log`, `diff` names) still work. A CDN in front of `/o` keeps serving what it cached. | Restore S3. Agents retry. Nothing to repair: lands that included large objects were aborted before publishing. Verify credentials and bucket policy if errors persist after S3 is healthy. |
| **Replica crash mid-batch** | one replica's `up == 0`, connection errors at clients, `ant_land_queue_depth` for that instance disappears | The crashed replica's lock session is closed by Postgres, which releases the advisory lock; other replicas continue. Requests it was processing get a connection error and were never acknowledged. Objects it wrote are unreachable garbage unless its publish committed (in which case a retry returns the same tip). | Restart or replace the replica; the load balancer drains it. Agents retry (idempotent). No data repair needed. |
| **Postgres disk full** | `ant_store_errors_total` on all stores, Postgres logs `No space left on device`, lands `aborted` | All writes fail; reads of existing data mostly continue. Refs stay consistent (statements are atomic). | Free or grow the volume (consider dropping old `soak_*` or scratch schemas, then `VACUUM`); Postgres recovers by itself. Plan capacity from section 2; alert on volume use at 80 percent. |
| **S3 full or quota exceeded** | as S3 outage, only for `put_many` | Large-object writes fail; reads fine. | Raise the quota. |
| **Replica disk full** | log write errors | A replica keeps no data on disk besides logs; only logging suffers. | Rotate logs. Nothing else. |
| **History index lag** | `ant_history_lag_commits{ref="..."}` does not return to 0; `history catch-up failed` warnings | `log --path` and `blame` still return correct results: a path log scans only the unindexed tail (up to a bounded number of commits) and takes the rest from the index; with a very large lag they get slower. Everything else is unaffected. | The index catches up after every successful land or `UpdateRef`, in the background, coalesced per ref; read the warning text for the store error behind it (usually Postgres trouble). Fixing the cause and landing once more (or waiting for traffic) re-runs it. If the ref was moved non-fast-forward, the index rebuilds from scratch on the next catch-up, which takes time proportional to history. |
| **Stuck ref lock** | `ant_store_op_duration_seconds{store="refs",op="lock_ref"}` p99 near 10 s; WARN `ref lock unavailable; landing with CAS only`; queue depth up on a ref | A hung lock holder (stalled replica, half-open connection) blocks other replicas for up to the 10 s lock timeout; then they fall back to CAS-only landing, which stays correct but gets contended. | Find the holder: `SELECT pid, state, application_name, client_addr, query_start FROM pg_stat_activity WHERE pid IN (SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted)`; if its replica is dead or unresponsive, restart that replica or `SELECT pg_terminate_backend(<pid>)` (the lock is released when the session ends; the holder's batch aborts and is retried). Add `application_name=antimony-<host>` to `--pg-url` to make holders identifiable. Locks are per (schema, ref), so unrelated refs are never affected. |
| **Client cache corruption** | an agent reports decode or hash errors reading objects, or odd missing-file errors | The cache is content-addressed and disposable. A corrupt file (bad compression or framing) is deleted and refetched on its own, and fetched objects are hash-verified. | If problems persist, delete `<cache>/objects` (default `~/.cache/antimony/objects`); it refills on demand. **Do not delete the whole cache directory:** lazy-workspace state (edits) lives under `<cache>/ws/` (see below). Workspace metadata is in each workspace's `.ant/`. |
| **FUSE daemon crash (lazy workspace)** | accessing the mount gives `Transport endpoint is not connected`; `ant -C <dir> status` says `error: lazy workspace mount <dir> is dead` with a `hint:` naming `ant ws remount` | The workspace's edits are **not lost**: they live in the state directory outside the mount (`<cache>/ws/<hash of mountpoint>/`: upper layer, whiteouts, journal). Metadata operations are journaled and recovery rolls an interrupted one forward or drops it; data of files that were still open or unsynced at the moment of the crash may be missing or partial, as with any POSIX crash. | `ant ws remount <dir>`: it detaches the dead mount (`fusermount3 -u -z`), restarts the daemon on the same state (recovery runs) and prints the base commit. Verified by an end-to-end test that SIGKILLs the daemon: uncommitted edits were intact after remounting. Then continue with `status`/`submit`/`land`. A host reboot has the same fix. |

General guidance during an incident: start with `ant_rpc_requests_total` by
`code`, then `ant_store_errors_total` by `store` (which dependency?), then
`ant_pg_pool_in_use`. Use a request id from the failing agent to find the
exact log lines. Replicas are interchangeable, so restarting one is always a
safe first step.

## 7. Backups and upgrades

**Back up Postgres and S3. Nothing on a replica needs a backup.**

- *Postgres* holds refs, the commit graph, changesets, small objects and the
  history index. Use your normal physical backup with point-in-time recovery
  (`pg_basebackup` plus WAL archiving, or the managed equivalent). `pg_dump`
  of the antimony schema also works for smaller deployments; the schema name
  is the `--pg-schema` value.
- *S3* holds the large objects. Objects are immutable and content-addressed,
  never rewritten, so bucket versioning is not required; cross-region
  replication or periodic bucket copies are enough.
- **Consistency.** Take the Postgres backup *before or together with* S3: a
  restored Postgres must not reference objects missing from the restored
  bucket, while extra objects in S3 are harmless. A point-in-time restore of
  Postgres to an earlier time simply rewinds the refs; the S3 bucket may be
  newer. If you restore Postgres to an older state, agents' bases may
  disappear; tell them to start new workspaces.
- The derived history index can be rebuilt from commits (it is rebuilt
  automatically when the ref moves non-fast-forward), so it is the least
  critical table.
- Test a restore: start a replica against the restored database and bucket and
  run `ant ref list` and `ant log main -n 5`.

**Upgrades.** See the "Upgrading" section of [operations.md](operations.md)
for the procedure (schema versioning and rolling restarts). In short, because
replicas are stateless, a rolling restart behind the load balancer is the
normal shape; clients reconnect and retry idempotent lands.

**Disk discipline for big imports and scale runs** (from the contributor
guide): check free space on the filesystem and, on WSL, on the Windows drive
holding the disk image before large imports, and stop if a `DISK_LOW` file
exists in the services directory.

## 8. Checklist for a new fleet deployment

1. Postgres >= 15 with `max_connections` computed from section 2; fsync
   latency measured; backups on.
2. S3 bucket and credentials; lifecycle rules **off** for the bucket.
3. TLS-terminating balancer for 7420 (gRPC) and 7421; metrics on a private
   listener (`--metrics-listen`).
4. `--policy` with a principal per agent or pool; tokens hashed; refs rules as
   in section 4.
5. `--log-format json`, logs shipped, Prometheus scraping each replica, alerts
   from section 5.
6. Harness: retries on `aborted`/`unavailable`, `ws update` + `ws resolve`
   loop on exit code 2, backoff with jitter, sparse workspaces by default, one
   shared `ANT_CACHE` per host.
7. Smoke test: `ant-bench soak` at the intended scale (it drops its own
   `soak_*` schema and S3 prefix afterwards unless `--keep`); `ant-bench fleet`
   for a single-process load check.
