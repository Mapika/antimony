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
| HTTP cache / CDN | optional | Put it in front of `GET /o/<id>` and `GET /t/<tree-id>` only (cache key: the path; forward `Authorization`). Responses are `Cache-Control: public, max-age=31536000, immutable` (`/o/` also with an `ETag`). Objects in restricted zones are served `private`, bundles that depend on the caller `private, no-store` (a shared cache must not store either), and unknown ids get `no-store`. Pass the CDN base URL to clients via `ANT_HTTP`; checkouts then cost about one `/t/` request per 8 MiB of small files instead of one request per file. |

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

**Ref events across replicas.** `WatchRef` (`ant ref watch`) streams a
ref's moves to CI without polling. After a ref move commits, its replica
notifies on the Postgres channel `ant_ref_moves` (a small non-durable
statement from the query pool, batched under load); each replica that has
watchers keeps one extra `LISTEN` connection (application name
`ant-refs/<schema>`, outside both pools) and relays every replica's moves
to its own watchers, so a watcher may connect to any replica.
Notifications lost while that connection is down, or never sent (a replica
crashing right after a move), are found when the connection comes back, at
the ref's next move, or by the 30 s re-read: the ref is re-read and the gap
sent as one coalesced event. See section 5 for the metrics.

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

### On cloud boxes

On Hetzner Cloud (ccx43 server box with Postgres, S3 and `ant serve`; a ccx33
client box driving the fleet over the private network, RTT about 0.35 ms;
[A/B record](scorecard/2026-10-16-hetzner-landing-ab.md)), 0.3.0 measured:

| agents | lands/s | land p99 |
|---|---|---|
| 1 | 397-421 | 1.4-1.5 ms |
| 16 | 2,746-2,803 | 6.0 ms |
| 64 | 3,217-3,245 | 35-39 ms |

The git/git import ran at 8,239-8,379 commits/s. Boxes of the same type vary
by up to 2x, so measure your own. What matters on cloud machines:

- **fsync latency** of the Postgres WAL disk (`pg_test_fsync`). On local NVMe
  (0.1-0.3 ms) it is not the limit. On network block storage (1-5 ms per
  flush) every land batch and every `create_changeset` waits one flush, so
  expect proportionally higher latencies. Batches grow under load, so
  throughput suffers less than latency. Keep `synchronous_commit = on`
  (antimony relaxes it itself only where that is safe, see "Durability"
  below). `commit_delay` made no measurable difference on NVMe.
- **Server cores.** At 64 agents the server box used about 3.5 cores for
  `ant serve` and 5 for Postgres.
- **Turn on `track_io_timing` and `track_wal_io_timing`** so `pg_stat_wal`
  shows flush times. `scripts/landing-bench.sh` samples what Postgres
  backends wait on; mostly `LWLock:WALWrite` means commits queue for WAL
  flushes.

**Durability.** Every land is acknowledged only after one synchronous
Postgres commit that moves the ref and records its landed-as mappings, on the
session that holds the ref's advisory lock. The batch's new objects and
commit records are written before it with `synchronous_commit = off`, on a
*deferred lane* of connections (a quarter of `--pg-max-connections`). The
WAL is sequential, so that synchronous commit flushes them too. If Postgres
crashed in between, the lock's session is gone and the publish fails: a ref
never points at lost data. History-index rows and watermarks are also
written deferred (derived data, rows before the watermark that covers them,
so a crash loses at most a suffix that the next catch-up redoes).
`create_changeset` stays fully synchronous: a returned commit id is durable.

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
with default caches). The in-process caches have fixed budgets: trees 56 MiB,
commits 8 MiB, graph nodes 16 MiB, changesets 16 MiB, small blobs 8 MiB, in two generations (a
cache can briefly hold up to twice its budget). Watch `ant_cache_bytes` and
the process RSS. On glibc the server caps malloc arenas at start
(`--malloc-arenas` / `ANT_MALLOC_ARENAS`, default twice the CPU count up to
16; `MALLOC_ARENA_MAX` in the environment wins; 0 = glibc default): freed
memory otherwise lingers in per-thread arenas and a full git clone's peak RSS
is far above its live heap. On glibc you can also set `ANT_MALLOC_TRIM_SECS=<n>` to call
`malloc_trim` every n seconds (off by default) if RSS stays high after bursts.
`GET /debug/stats` on the HTTP port prints cache entries and budgets, land
queue counts, running history catch-ups and the tree-bundle cache (`bundle_cache`: entries, bytes, budget, hits and misses) as one JSON line. Without a policy
it is open; with `--policy` it needs an admin token (`Authorization: Bearer
<token>`, or Basic with the token as password): `401` without a valid token,
`403` for a non-admin principal. It is not available on the `--metrics-listen`
port.

### Postgres connection budget

Each replica opens up to `--pg-max-connections` query connections (default 64;
a quarter of them, from four connections up, form the deferred-write lane, and
`ant_pg_pool_max` reports the rest) plus `--pg-lock-connections` lock connections (default 8; this bounds the
number of distinct refs being landed on at once per replica). Waiting for a
ref lock is bounded by `--pg-lock-timeout` (`ANT_PG_LOCK_TIMEOUT`, default
`10s`; accepts `500ms`, `10s`, `2m`); on expiry the land proceeds with CAS only.
The rule:

```
replicas x (pg-max-connections + pg-lock-connections)  <  Postgres max_connections - headroom
```

A replica with at least one `WatchRef` stream open adds one `LISTEN`
connection to that sum.

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
so deep paths cost more. Aborted land batches, abandoned changesets,
failed imports and retried uploads leave unreachable data behind (harmless,
see failure modes); `ant admin gc` deletes it (next section). Under
sustained bulk loads autovacuum on `objects` becomes visible; give Postgres
I/O headroom.

### Garbage collection

`ant admin gc <store flags>` deletes what nothing can reach any more:
objects, commit records, changesets and S3 packs. It runs against the stores
directly (like `ant import`), at most one run per schema, while servers keep
serving. What is kept: everything reachable from every ref, from the
history index, from landed-as mappings (retries of a land) and from the
git-import mapping (re-imports), plus everything written within the
**grace period** (`--grace`, default `7d`, at least `1d`). The grace
period is the promise to agents: a changeset can be landed, and an
uploaded blob used, for that long after it was created; an unlanded
changeset older than that may be gone (`land` then reports not found).

A run marks reachable data (in a working table in Postgres, so memory use is
flat), condemns the rest (servers then report it as missing), waits
`--quiesce` (default `30m`; it must exceed the servers' 10 minute cache
lifetime plus your longest request), marks again, then deletes in small
transactions, and finally deletes S3 packs without live objects (a pack
`ant admin repack` moved objects out of is kept for `--quiesce` after the
move). Packs that are only partly dead are kept; the report shows their
dead bytes, which the next repack drops. The module docs of
`ant_store::postgres::gc` give the safety argument. A run can be interrupted
at any time (Ctrl-C, crash, `--max-batches N`); the next run resumes it.
`--abort` drops a pending run
instead (its condemned data becomes visible again). Operations that touch
condemned data during a run fail with not found or `aborted` (retryable),
never with lost data.

```sh
ant admin gc --dry-run --pg-url ... --pg-schema ant --s3-...   # count only
# dry-run
# cutoff 2026-10-07 09:12:44.12+00     rows written before this were candidates
# live 1843202                          ids reachable
# condemned 5310                        ids to delete
# objects 4821 9113204                  small objects: count, stored bytes
# large-objects 12 81733120             packed objects: count, bytes
# commits 402
# changesets 402 380122
# packs 3 70112333                      S3 packs that would be deleted
# packs-kept 812 19400113321 412233 0.0%   kept packs: count, bytes, dead bytes, dead share
ant admin gc --pg-url ... --pg-schema ant --s3-...             # collect
```

Schedule it daily or weekly. A run reads every reachable tree once, so on a
kernel-sized store the mark takes a while and reads from S3; run it off-peak.
Postgres gets the space back through autovacuum (or `VACUUM` after a large
first run).

### Repacking

Writers compress each write on its own (an import batch, a land), so
versions of one file written at different times share nothing.
`ant admin repack <store flags>` (Postgres + S3 stores) is antimony's
`git gc`: it walks the whole history and rewrites *cold* objects, including
small objects stored in Postgres, as delta frames: the versions of each
path, newest first, as git deltas against each other, compressed together.
Objects reachable from the newest `--keep-recent` (default `100`)
first-parent commits of every ref are hot: they stay in Postgres or are
rewritten as plain frames, so checking out a recent revision decodes no
deltas. Rows written within `--min-age` (default `1d`) are left alone.

```sh
ant admin repack --dry-run --pg-url ... --pg-schema ant --s3-...  # estimate only
ant admin repack --pg-url ... --pg-schema ant --s3-...            # repack
# done
# commits 82500                     walked
# reachable 403443                  objects reached (hot ones excluded)
# hot 5788
# selected 266839 2167 0 136604     packed objects: cold, hot, not reached; from Postgres
# old-bytes 687865498 84828053      what they took: S3 (their share of records), Postgres
# new-bytes 88216313                the new packs
# saved 684477238
# packs 2 1803                      packs and records written
# deltas 307844 16                  objects stored as deltas, deepest chain
# moved 405610 136604               objects moved (of them from Postgres)
# skipped 0                         gone, condemned or locked meanwhile: left where they were
```

It is safe while servers serve, land and import and while `ant admin gc`
runs (one repack per schema at a time); reads find every object before,
during and after the move. Old packs are freed by the next `ant admin gc`
(once their quiesce period after the move is over); Postgres reuses the
space of moved rows (autovacuum), or returns it with `VACUUM FULL objects,
object_index` (which locks the tables: off-peak, or use `pg_repack`).
A run can be interrupted (Ctrl-C, `--max-packs N`): what it switched stays
switched, an unswitched pack is garbage for the collector, and the next run
continues with what is left. Later runs only repack what is new since (and
hot objects that have gone cold); `--all` rewrites everything again, which
merges the deltas of incremental runs and drops dead bytes in repacked
packs: run it now and then (monthly), incremental runs after imports or
weekly. Before the first repack, every server must run a release with schema
version 7 or later (they read delta frames).

Measured on git/git (82k commits, 20 cores, local SeaweedFS): 55 s, 127
CPU-seconds, 1.1 GiB peak RSS; stored bytes 901 MiB → 361 MiB (238 MiB after
`VACUUM FULL`; S3 656 → 84 MiB), against 298 MiB for `git gc` and 94 MiB
for `git gc --aggressive` (51 s, 420 CPU-seconds) of the same history.
Reads (fresh server, cold client): checkout of the tip 0.34 → 0.28 s, of a
2015 revision 0.24 → 0.33 s, first blame 258 → 239 ms, blobless clone
6.4 → 5.5 s, full clone 19.4 → 13.2 s. Flags:
`--window` (default 10) and `--depth` (16) as in git; `--record-bytes`
(2 MiB: the most a read of one object decompresses); `--level` (zstd, 19;
9 halves the time for ~5% more bytes); `--jobs` (CPU count, at most 8).
Memory: about 100 bytes per object for the walk, plus per job a read batch
(16 MiB), a delta window (at most 24 MiB) and a record.


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

### Cache tier (`ant cache-serve`)

`ant cache-serve` is a caching proxy for the immutable HTTP surface
(`GET /o/<id>` and the tree bundles `GET /t/<tree>`). It keeps the objects in
a disk cache in the client format and fills misses from an upstream: the
origin's `--http-listen` address or another `cache-serve`, so tiers chain
(`client -> node -> region -> origin`). Clients use it by pointing `--http` /
`ANT_HTTP` at it; their gRPC connection (refs, changesets, land, queries)
still goes to the origin.

```
ant cache-serve --cache /var/cache/ant-tier --cache-bytes 200000000000 \
    --upstream http://origin:7421 --listen 0.0.0.0:7422 --auth forward
```

`--cache` is required: the default client cache holds whatever its user may
read and must not be served. The disk budget, LRU eviction and `ant cache
stats` / `ant cache gc` work on it exactly as for a client cache (pass the
same `--cache`); the bundle index (`<cache>/bundles`, tiny files that say
which objects make up a tree's bundle) is pruned to 2% of the budget (at
least 16 MiB). Every object from the upstream is hash-checked against the
requested id before it is stored, so an upstream cannot poison the cache.
Concurrent misses for one object share one upstream request.

**Access control: the cache never answers on its own authority.**
Which objects a principal may read depends on path rules and zone tags
(`Access policy`), and the origin's answer depends on the caller, so a shared
cache keyed by object id alone would hand an object to someone the origin
would refuse. The two modes:

- `--auth forward` (default; correct for any upstream, including one running
  with a policy). The caller's `Authorization` header is forwarded to the
  origin; the cache never serves bytes on its own authority. For an object
  it holds, it asks the origin with a batched `POST /o/check` (up to 4096
  ids per request, the same authorization code as `GET /o/<id>`, a hidden
  object reported like a missing one): requests of one principal that arrive
  within `--authz-window-ms` (default 2) share one check. Only on a clear
  "yes" are the bytes served from disk; a no, a 401, a 5xx or no answer
  serves nothing (the origin's 404 / refusal is passed on), so it fails
  closed. Misses are forwarded with the caller's credentials and stored (the
  disk is never readable except through the check). A miss by principal B
  while principal A's fetch of the same object is in flight first passes a
  conditional read at the origin, then takes the bytes: waiting never skips
  authorization.
  - **Decision TTL (`--authz-ttl-secs`, default 30, env
    `ANT_CACHE_AUTHZ_TTL`).** An *allowed* answer is reused for the same
    `Authorization` header and object for at most the TTL (counted from when
    the question was sent), so a warm tier costs the origin nothing while
    decisions are fresh. Decisions are per principal (keyed by a SHA-256 of
    the header; the token is not kept, logged or in metrics), never shared;
    denials and errors are never remembered, so a new grant works at once.
    **The TTL is the revocation window:** after you revoke a principal or
    change a policy at the origin, the cache can still serve it objects it
    holds for up to the TTL. Set `--authz-ttl-secs 0` if that is not
    acceptable: every request is then checked (still batched), at one origin
    request per batch.
  - **Tree bundles.** A bundle served `public` by the origin (identical for
    every caller) is indexed; later `/t/<tree>` requests, from any
    principal, are answered by one batched check of the listed ids and a
    bundle rebuilt from disk holding exactly the objects that principal may
    read (`public, immutable` if all, else `private, no-store`, like the
    origin). Bundles the origin marks caller-specific, an unindexed tree, an
    evicted object or any failure are forwarded to the origin as before.
  - **Older origins.** An origin without `POST /o/check` (404/405) is
    detected; the cache then revalidates each hit with a conditional
    `GET /o/<id>` (`304` = readable), keeps no decisions, forwards bundles,
    and probes again after a minute. Chained forward-mode caches therefore
    use conditional reads between them (a cache-serve does not itself offer
    `/o/check`).
- `--auth open`, for an origin started without `--policy` (or a cache-serve in
  open mode in front of one): every object is readable by everyone, so hits
  cost the origin nothing and bundles are rebuilt from disk. The cache checks
  at startup that the upstream really is open (an unauthenticated `GET
  /debug/stats` must be `200`; a server with a policy answers `401`, a
  forward-mode cache `404`) and refuses to start otherwise, so an open cache
  cannot be put in front of a protected origin by accident. It ignores and
  never forwards `Authorization` headers.

The cache's own listener has no authentication: bind it where the clients
that may read the data can reach it. An open cache serves everything to
whoever connects to it.

Operations: `GET /healthz`, `/readyz` (static) and `GET /metrics` on the same
listener; logs on stderr as for `ant serve` (`RUST_LOG`). The cache enforces
its budget and refreshes its gauges every minute. Metrics (names verified in
`crates/ant-cache/src/metrics.rs`):

| family | labels | meaning |
|---|---|---|
| `ant_cacheserve_requests_total` | `route` (`objects`, `bundles`), `result` | `hit` (disk, open mode), `revalidated` (disk, origin confirmed the caller now or within the TTL, forward mode; rebuilt bundles too), `miss` (fetched by this request), `joined` (shared an in-flight fetch), `passthrough` (forward-mode bundle), `refused` (origin said 401/403/404 to a forward request), `not_found`, `error`. Tier hit ratio = (`hit` + `revalidated`) / all |
| `ant_cacheserve_authz_total` | `result` (`cached`, `checked`, `fallback`) | forward-mode permission lookups, one per object per request: decision reused within the TTL, asked of the origin (batched), or per-object fallback for an origin without `/o/check` |
| `ant_cacheserve_response_bytes_total` | `route` | body bytes sent |
| `ant_cacheserve_upstream_requests_total` | `route`, `kind` (`fetch`, `revalidate`, `check`), `status` (`0` = transport error) | requests to the upstream |
| `ant_cacheserve_upstream_bytes_total` | `route` | body bytes received |
| `ant_cacheserve_inflight_fetches` | | upstream fetches in flight |
| `ant_cacheserve_disk_bytes`, `_disk_budget_bytes`, `_index_bytes` | | tracked cache size (shared counter, no scan), budget, bundle index |

Useful alerts: a falling hit ratio, a rising `error` or `status="0"` share
(the upstream is unreachable: forward mode then refuses everything, open mode
still serves what it holds), `disk_bytes` pinned at the budget with a low hit
ratio (undersized).

Measured, forward mode (96k-file synthetic checkout over HTTP with tree
bundles, loopback, open origin, a second client with an empty cache against
the warm tier; `ant-bench checkout --cache-tier --cache-tier-forward`,
`--cache-tier-no-batch` for the previous behaviour): requests reaching the
origin 1,990 (one per bundle and per object, all forwarded) before, 0 to 42
with batched checks (bundles rebuilt from disk; ids checked in batches or
reused from the decision cache). Wall time on a shared, noisy machine was
3.1 to 9.6 s before and 2.6 to 5.0 s after; the gain is origin requests and
bytes (the origin here enforces no policy, so its per-request authorization
work, which grows with policy size, is not in the numbers).

Measured (96k-file synthetic checkout over HTTP with tree bundles, open mode,
loopback, dev machine): a client against a cold tier 4.0 s, origin sees
1,990 requests; further clients with empty caches against the warm tier
2.5 s, 0 origin requests (direct to the origin: 2.2 s). The gain is origin
load and distance, not loopback speed: the tier re-encodes a bundle from its
objects (decompress, recompress) where the origin serves from memory.

**Seeding and sharing on a node.** `ant cache seed <rev>[:<path>]
[--sparse <pattern>]...` walks a tree as `ws new` would (tree bundles when
`--http` is set) and fetches every blob in scope into the cache without
writing files; run it from a snapshot or image build step, or a cron on the
node, so jobs start with hits. It works on a cache-serve's `--cache`
directory too, to warm a tier offline. Processes sharing a cache directory
also fetch each missing object (or tree bundle) once: the first claims it
with an `flock` lock file under `<cache>/claims`, the others wait for it and
read the result from the cache (30 s at most, then they fetch it
themselves; a crashed holder releases its claim with the process).
Claims are only taken while another process uses the cache (each one
registers under `<cache>/live/` while it runs), so a process alone on its
cache pays only one small file per fetch batch (under `<cache>/batches/`,
listing the batch's ids), which a process starting meanwhile waits for
instead of fetching the same objects again. `ANT_CACHE_DEDUP=0` turns it off. 8 concurrent cold checkouts of a 48k-file
tree through one cache (tmpfs): 18,633 HTTP requests and 599k objects
received without it, 2,354 requests and 75k objects (the distinct ones) with
it; wall time 4.8 s to 2.0 s. `ant cache stats --tiers` prints where this
cache's objects have come from over its lifetime (`local_hits`,
`waited_hits`, `joined`, `bundled_objects`, `fetched_objects`, ...).

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
sparse around the restricted paths (see the [threat model](access-control.md)). The git
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

### Rate limits and load shedding

Thousands of agents retry with backoff; one runaway principal, or a thundering
herd after an outage, must not saturate the Postgres pool and starve the rest.
The server therefore applies two independent protections to every gRPC method
and to `/o/`, `/t/`, `/repo.git` and `/debug/stats` (never to `/healthz`,
`/readyz`, `/metrics`):

1. **Per-caller token buckets** (requests/s plus burst) per rate class: a
   principal under a policy, otherwise the client IP (open mode, unknown
   tokens). An empty bucket is answered with gRPC `RESOURCE_EXHAUSTED` /
   HTTP `429`.
2. **Concurrency gates** per class with a bounded queue. A request holds a
   slot until its response ends; with no slot free it waits in the queue for
   at most `--shed-wait`, and is shed with gRPC `UNAVAILABLE` / HTTP `503`
   when the queue is full or the wait runs out.

Both answer *before the handler runs* and carry a retry hint: gRPC trailer
metadata `retry-after-ms`, HTTP `Retry-After` (seconds) plus `retry-after-ms`.
`ant` and `antd` retry exactly the responses that carry the hint, sleeping the
hint (or 50 ms x 2^n, whichever is longer) scaled by a random 1.0-1.5 factor,
at most 8 times and 30 s in total, then surface the error (`error: rate limit
exceeded ...` plus a `hint:`). Because nothing ran, retrying is safe for
`Land`, `CreateChangeset` and `UpdateRef` as well; a bare `UNAVAILABLE` from a
dropped connection has no hint and is not retried by the client (a custom
harness should treat it like `aborted`).

| class | methods / routes | rate bucket | default concurrency (P = `--pg-max-connections`) |
|---|---|---|---|
| `read` | `GetObjects HasObjects GetRef ListRefs ResolveRev ReadPath Log Diff Blame MergeBase IsAncestor LogRange LandedAs WatchRef` (a watch gives its slot back once its stream starts), `/o/`, `/t/`, `/repo.git` | read | 4P |
| `write` | `PutObjects UpdateRef CreateChangeset` | write | 2P |
| `land` | `Land` | write | 16P |
| `admin` | `/debug/stats` | admin | 8 |

The queue of each gate holds as many waiters as it has slots. `land` is wide on
purpose: lands on one ref are queued and group-committed by the server, and a
waiting land holds no Postgres connection, so the gate never limits batching
(the 16-agent in-memory fleet and a 96-agent concurrent-land test batch the
same with and without limits); it only caps how many land requests a replica
accepts at once. Rates are **opt-in** and unlimited by default for everyone:
a token is often shared by a whole team or pool of agents, so a default per
principal would throttle real fleets. Enable them with the flags below (they
apply to every caller, open-mode IPs included) or per principal in the policy,
sized to the agents per token (agents x requests/s each, with headroom):

| flag (env) | default | meaning |
|---|---|---|
| `--rate-read` (`ANT_RATE_READ`) | off | `<rps>[:<burst>]` or `off`; applies to every principal and to open-mode IPs |
| `--rate-write` (`ANT_RATE_WRITE`) | off | changesets, lands, ref updates, uploads |
| `--rate-admin` (`ANT_RATE_ADMIN`) | off | |
| `--max-inflight-{read,write,land,admin}` (`ANT_MAX_INFLIGHT_*`) | 4P, 2P, 16P, 8 | `0` = unlimited; queue length = the same number |
| `--shed-wait` (`ANT_SHED_WAIT`) | `2s` | longest queue wait before shedding |
| `--no-limits` (`ANT_NO_LIMITS`) | off | disable both mechanisms |

Burst defaults to twice the rate. Override a principal in the policy
(additive; `rps = 0` means unlimited, omitted classes keep the default):

```toml
[[principal]]
name = "ci-bridge"
token_sha256 = "..."
[principal.limits]
read  = { rps = 5000, burst = 10000 }
write = { rps = 0 }
```

Sizing: a replica's `read` + `write` slots should not exceed what the pool can
serve (`P` connections x the number of replicas must stay under Postgres'
`max_connections`, section 2); most reads are served from caches, which is why
`read` is 4P. The limits and the token buckets are per replica, so with
several replicas behind one balancer a principal's real ceiling is
`replicas x rps`. A streaming `GetObjects` keeps its slot until the client has
read the whole stream, so a stalled client occupies one `read` slot (not more)
until its connection drops.

Measured (dev machine, 2 replicas x 300 agents, Postgres + S3, 120 s, plus 32
hostile read loops from a second address ignoring every retry hint; agents'
overall p99 in ms, `ant-bench soak --hostile 32`):

| run | cat | create | get_ref | land | hostile served |
|---|---|---|---|---|---|
| no hostile caller | 5.8 | 12.0 | 1.2 | 72 | - |
| hostile, `--no-limits` | 24.6 | 25.6 | 9.0 | 102 | 15,300/s |
| hostile, `--rate-read 1500:3000` | 14.3 | 13.1 | 4.6 | 180 | 3,000/s |

Limits cap the hostile caller at its budget and halve the read-side damage, but
the flood (167,000 rejected requests/s here, on the same machine as the
servers) still costs CPU, which is why the land tail is not better than
without limits in this run: rejecting is cheap, not free. In production put
a network-level limiter in front for floods of that size. The in-memory fleet
(16 agents) lands 322k/243k (before, noisy) vs 328k/331k (after) in 20 s.

Metrics: `ant_ratelimit_rejected_total{class,reason}` (`rate`, `queue_full`,
`queue_timeout`), `ant_ratelimit_inflight{class}`,
`ant_ratelimit_queued{class}`, `ant_ratelimit_inflight_limit{class}`.
Rejections also appear as `resource_exhausted` / `unavailable` in
`ant_rpc_requests_total` and 429/503 in `ant_http_requests_total`.

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
| `ant_ref_watchers` | | open `WatchRef` streams (CI watchers) |
| `ant_ref_watch_events_total`, `ant_ref_watch_coalesced_events_total` | | ref events sent; of those, coalesced ones (catch-ups, gap fills, slow watchers) |
| `ant_pg_pool_size`, `_idle`, `_in_use`, `_max` | | query pool (the lock pool is not included) |
| `ant_s3_retries_total`, `ant_s3_gave_up_total` | `reason` = `throttled`/`server_error`/`network`/`body`/`timeout` | S3 attempts retried; operations that failed after the retry budget |
| `ant_s3_requests_in_flight`, `ant_s3_requests_queued`, `ant_s3_max_concurrency` | | S3 request slots (`--s3-max-concurrency`) in use, waited for, configured |
| `ant_s3_requests_total`, `ant_s3_read_bytes_total` | `op` = `get`/`put`/`head`/`other` | S3 requests sent (retries included); body bytes received |
| `ant_s3_pack_segments_total`, `ant_s3_pack_cache_bytes` | `result` = `hit`/`read` | pack segments found cached (or being read) and read from S3; bytes held (`--s3-pack-cache-mib`) |
| `ant_build_info` | `version`, `git` | always 1 |

`ant cache-serve` has its own `ant_cacheserve_*` families (see Cache tier).

Notes: `ant_land_outcomes_total` counts one per land request. `rebased` is a
successful land that the server rebased, so total successes are
`landed + rebased`. Errors other than `aborted` and `denied` (for example a
store failure surfacing as `internal`) are not in that family; they show in
`ant_rpc_requests_total{method="Land",code!="ok"}`. `ant_history_lag_commits`
always has a `ref="other"` series, plus one per recently active ref; alert on
`max`. Counters per histogram family also export `_bucket`, `_sum` and
`_count`.

### Ref events for CI

`WatchRef` is how post-merge CI learns about lands without polling: `ant ref
watch main` (or the RPC) prints one line per move, `land <old>..<new> <n>
landed` for a group-commit batch of `n` lands, `update` for other
fast-forwards, `force` for rewinds. Run CI on each new tip, or on each
landed commit. A CI runner that restarts passes `--from <last tip it
handled>` and first gets one catch-up event covering what it missed.

- **Cost.** One hub per replica and watched ref builds each event once (one
  first-parent chain read per land publish) and shares it with all its
  watchers. Every ref move costs one small non-durable `pg_notify`
  statement after its commit (batched under load) whether or not anyone
  watches. Measured on the dev machine (64-agent Postgres+S3 fleet, 8
  interleaved 20 s pairs on a busy host): median 2,081 lands/s before,
  2,138 after; with 4 `ant ref watch` streams attached to the server, the
  same as with none (every watcher saw all ~70,000 lands, chained, without
  coalescing).
- **Backpressure.** Each hub keeps the last 256 events. A watcher that reads
  slower than lands publish (a stalled CI runner, a slow network) falls
  behind that window and then gets one coalesced event covering everything
  it missed (`landed` stays complete), so a slow watcher never costs
  unbounded server memory; it just sees fewer, larger events.
- **Access.** Watching needs `read` (new), `update` or `land` on the ref in
  the policy's `[[ref]]` rules (`read = ["group:ci"]` for a read-only CI
  principal); events carry commit ids only.
- **Metrics.** `ant_ref_watchers` (open streams),
  `ant_ref_watch_events_total` and `ant_ref_watch_coalesced_events_total`
  (catch-ups, gap fills after a lost `LISTEN` connection, and slow
  watchers' coalesced backlogs). A steadily rising coalesced share means
  watchers cannot keep up.

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

# 14. A principal (or an unthrottled herd) is being rate limited: sustained 429s
sum by (class) (rate(ant_ratelimit_rejected_total{reason="rate"}[5m])) > 1       # for 10m

# 15. Load shedding: the replica is overloaded (queue full / queue timeouts), or slot holders are stuck
sum by (instance, class) (rate(ant_ratelimit_rejected_total{reason=~"queue_full|queue_timeout"}[5m])) > 0   # for 2m
max by (instance, class) (ant_ratelimit_inflight / clamp_min(ant_ratelimit_inflight_limit, 1)) > 0.9        # for 5m
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
10. HTTP `/o` and `/t` (`route="tree_bundles"`) request rate and p99 (CDN miss traffic)
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
| **S3 outage or throttling** | `ant_s3_retries_total{reason="throttled"}` / `{reason="body"}` rising (absorbed: requests slower, not failing); then `ant_s3_gave_up_total`, `ant_store_errors_total{store="objects"}` (ops `get_many`/`put_many`), `internal` RPC codes, `/o/<id>` 5xx for large objects | Throttling and brief outages are retried for `--s3-retry-budget` (60 s) per operation; only longer ones fail requests. Objects up to 4 KiB live in Postgres and keep working, so many small-file operations survive. Anything touching a large object fails: reads of those files, submits and lands that contain them. Metadata queries (`log`, `diff` names) still work. A CDN in front of `/o` keeps serving what it cached. | Restore S3. Agents retry. Nothing to repair: lands that included large objects were aborted before publishing. Verify credentials and bucket policy if errors persist after S3 is healthy. Under sustained throttling lower `--s3-max-concurrency` (summed over replicas and admin jobs) or raise the store's limits (`docs/operations.md`, S3 timeouts, retries and throttling). |
| **Replica crash mid-batch** | one replica's `up == 0`, connection errors at clients, `ant_land_queue_depth` for that instance disappears | The crashed replica's lock session is closed by Postgres, which releases the advisory lock; other replicas continue. Requests it was processing get a connection error and were never acknowledged. Objects it wrote are unreachable garbage unless its publish committed (in which case a retry returns the same tip). | Restart or replace the replica; the load balancer drains it. Agents retry (idempotent). No data repair needed. |
| **Postgres disk full** | `ant_store_errors_total` on all stores, Postgres logs `No space left on device`, lands `aborted` | All writes fail; reads of existing data mostly continue. Refs stay consistent (statements are atomic). | Free or grow the volume (consider dropping old `soak_*` or scratch schemas, then `VACUUM`); Postgres recovers by itself. Plan capacity from section 2; alert on volume use at 80 percent. |
| **S3 full or quota exceeded** | as S3 outage, only for `put_many` | Large-object writes fail; reads fine. | Raise the quota. |
| **Replica disk full** | log write errors | A replica keeps no data on disk besides logs; only logging suffers. | Rotate logs. Nothing else. |
| **History index lag** | `ant_history_lag_commits{ref="..."}` does not return to 0; `history catch-up failed` warnings | `log --path` and `blame` still return correct results: a path log scans only the unindexed tail (up to a bounded number of commits) and takes the rest from the index; with a very large lag they get slower. Everything else is unaffected. | The index catches up after every successful land or `UpdateRef`, in the background, coalesced per ref; read the warning text for the store error behind it (usually Postgres trouble). Fixing the cause and landing once more (or waiting for traffic) re-runs it. If the ref was moved non-fast-forward, the index rebuilds from scratch on the next catch-up, which takes time proportional to history. |
| **Overload or a runaway principal** | alert 14 or 15; `ant_ratelimit_rejected_total` rising; clients fail with `rate limit exceeded` / `server overloaded` once their retries run out | Rejected requests never ran, so nothing is half done; rate-limited callers slow down, everyone else keeps its latency. Shedding (`queue_full` / `queue_timeout`) means a class is at its concurrency limit: either real overload (also high `ant_pg_pool_in_use / ant_pg_pool_max`, rising store latency) or slots pinned by stalled streaming clients (`ant_ratelimit_inflight` at the limit while RPC throughput is low). | Find the principal: the slow-request log names it, and `reason="rate"` rejections point at a bucket. Raise its `[principal.limits]` if the traffic is legitimate, otherwise fix the harness (`ant` and `antd` already honour `retry-after-ms`; a client that still floods ignores it). For genuine overload add replicas or raise `--pg-max-connections` (watch Postgres `max_connections`). Do not raise `--max-inflight-*` beyond what the pool serves just to hide shedding. `--no-limits` is the emergency switch. |
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
- *S3* holds the large objects. Objects are immutable and never rewritten;
  only garbage collection deletes packs. Cross-region replication or periodic
  bucket copies are enough, but keep deleted packs (bucket versioning with a
  noncurrent-version expiry, or copies) at least as long as your Postgres
  backups: a Postgres backup taken before a collection can refer to packs it
  deleted.
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
normal shape; clients reconnect and retry idempotent lands. From 0.1.x to
0.2 (schema version 7) a rolling upgrade needs `--pack-frames false` on the
new servers until the last 0.1.x server is gone, and no 0.1.x server can be
restarted once the first 0.2 server has started (see "Pack frames" there).

**Disk discipline for big imports and scale runs** (from the contributor
guide): check free space on the filesystem and, on WSL, on the Windows drive
holding the disk image before large imports, and stop if a `DISK_LOW` file
exists in the services directory.

## 8. Checklist for a new fleet deployment

A ready-made single-host implementation of this checklist (compose with TLS,
tuned Postgres, SeaweedFS, backups with a restore procedure, git mirror, alert
rules) is in [deploy/](../deploy/README.md).

1. Postgres >= 15 with `max_connections` computed from section 2; fsync
   latency measured; backups on.
2. S3 bucket and credentials; lifecycle rules **off** for the bucket.
3. TLS-terminating balancer for 7420 (gRPC) and 7421; metrics on a private
   listener (`--metrics-listen`).
4. `--policy` with a principal per agent or pool; tokens hashed; refs rules as
   in section 4.
5. `--log-format json`, logs shipped, Prometheus scraping each replica, alerts
   from section 5.
6. Rate limits: off by default; set per-principal rates sized to the agents
   per token (section 4, "Rate limits and load shedding"), especially for
   bridge/CI principals, and alert on 14 and 15. Concurrency gates are on.
7. Harness: retries on `aborted`/`unavailable` (`ant`/`antd` already honour
   the server's `retry-after-ms`), `ws update` + `ws resolve`
   loop on exit code 2, backoff with jitter, sparse workspaces by default, one
   shared `ANT_CACHE` per host.
8. Smoke test: `ant-bench soak` at the intended scale (it drops its own
   `soak_*` schema and S3 prefix afterwards unless `--keep`); `ant-bench fleet`
   for a single-process load check.
