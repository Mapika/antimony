# Running antimony

For running a fleet in production (sizing, alerts, failure runbook), see the
[operator guide](operator-guide.md).

## Components

| process | role | state |
|---|---|---|
| `ant serve` | gRPC (`:7420`) + HTTP (`:7421`: `/o/<id>` objects, `/t/<tree-id>` tree bundles, `/repo.git` git export, `/healthz`, `/readyz`, `/metrics`) | none; caches only |
| PostgreSQL ≥ 15 | refs, commit graph, changesets, small objects, history index | all metadata |
| S3-compatible store | objects larger than 4 KiB (zstd) | large objects |
| HTTP cache / CDN (optional) | caches `GET /o/<id>` and `GET /t/<tree-id>` (immutable; `private` for restricted zones, `private, no-store` for caller-specific bundles) | disposable |

## Starting a server

```sh
ant serve \
  --pg-url postgres://USER:PASS@HOST:5432/DB --pg-schema ant \
  --s3-endpoint https://s3.example.com --s3-bucket antimony \
  --s3-access-key ... --s3-secret-key ... --s3-region us-east-1 \
  --grpc-listen 0.0.0.0:7420 --http-listen 0.0.0.0:7421 \
  --policy /etc/antimony/policy.toml
```

Every flag has an `ANT_*` environment variable (`ANT_PG_URL`, `ANT_POLICY`, …;
see `ant serve --help`). The schema, tables and S3 bucket are created on first
start. For a copy-paste walk-through of a whole session, see the README
quickstart.
Without `--policy` the server runs in **open mode** (no authentication) and
logs a warning; never expose an open-mode server.

Run it behind TLS. Bearer tokens are secrets, and the server itself does not
terminate TLS. Put a reverse proxy (or a gRPC-aware load balancer) in front
of both ports. Clients (`ant`, `antd`) take an `https://` `ANT_SERVER` and
verify the proxy's certificate against the system's trusted roots (set
`SSL_CERT_FILE` for a private CA). A ready-made Caddy setup is in
[deploy/](../deploy/README.md).


### S3 timeouts, retries and throttling

Every S3 request is bounded, so a stalled or throttling object store fails a
request with an error instead of hanging it (and the read budget it holds).
An attempt covers the request **and the whole response body**; it may take
`--s3-timeout + size / --s3-min-throughput`.

**Retries.** Throttling (429, 503, `SlowDown`), other 5xx, connection errors,
timeouts and bodies that break off or come up short are retried with
exponential backoff and full jitter: the pause after attempt *n* is random
in 0 .. min(5 s, 100 ms x 2^(n-1)); a longer `Retry-After` is honoured (up
to 30 s). An operation keeps retrying for `--s3-retry-budget` (60 s),
counted from its first failure, so a few seconds of throttling cost latency,
not a failed clone or `ant admin gc`. Client errors (403, 404, ...) and
corrupt data fail at once. Only GET, HEAD, PUT and DELETE of
content-addressed or idempotent keys are issued, so retries are safe. When
the budget (or `--s3-max-attempts`, or `--s3-operation-timeout`) runs out
the caller gets a `backend error: s3 <op> <key>: gave up after N attempt(s)
...` (clone and RPC fail cleanly). `/readyz` probes S3 with one attempt.

**Concurrency.** A process keeps at most `--s3-max-concurrency` (32) S3
requests in flight; more wait for a slot (a retry waits again; its pause
holds none). Bursts such as the `ant admin gc` mark, `ant admin repack`,
clone planning and checkout bundles therefore queue in the process instead
of overrunning the store. Object stores throttle beyond some in-flight
volume: SeaweedFS answers 429 once a volume server has more than
`-volume.concurrentDownloadLimitMB` (default 64 MiB) of downloads in flight,
counting whole storage chunks (`-filer.maxMB`, 4 MiB) rather than requested
ranges, so with its defaults about 16 concurrent range reads saturate one
volume server; the filer then breaks the response off mid-body. Raise that
limit on a dedicated SeaweedFS, or lower `--s3-max-concurrency`; AWS S3
sustains thousands of GETs per second per prefix, so a server there may
raise it (64-128). Several processes (replicas, `ant admin gc`, imports)
share the store's limits: budget their sum.

| flag | env | default |
|---|---|---|
| `--s3-connect-timeout` | `ANT_S3_CONNECT_TIMEOUT` | `5s` |
| `--s3-timeout` (fixed part of an attempt) | `ANT_S3_TIMEOUT` | `30s` |
| `--s3-min-throughput` (bytes/s) | `ANT_S3_MIN_THROUGHPUT` | `10000000` |
| `--s3-retry-budget` (retrying after the first failure) | `ANT_S3_RETRY_BUDGET` | `60s` |
| `--s3-max-attempts` (1 = no retries, 0 = no limit) | `ANT_S3_MAX_ATTEMPTS` | `0` |
| `--s3-operation-timeout` (slot wait and retries included) | `ANT_S3_OPERATION_TIMEOUT` | `5m` |
| `--s3-max-concurrency` (requests in flight per process) | `ANT_S3_MAX_CONCURRENCY` | `32` |

Watch `ant_s3_retries_total{reason}` (`throttled`, `server_error`,
`network`, `body`, `timeout`): a steady trickle under load is the backoff
doing its job; `ant_s3_gave_up_total` rising means operations failed after
the budget (S3 down or throttling for longer than the budget). A sustained
`ant_s3_requests_queued` with `ant_s3_requests_in_flight` at
`ant_s3_max_concurrency` means S3 requests are the bottleneck.

Large objects of one write (an import batch, a land) that look alike
(versions of one file or directory) are compressed together in frames of at
most 2 MiB uncompressed; a read decodes the whole frame, and servers keep
the last 64 MiB of decoded frames. `--pack-frames false`
(`ANT_PACK_FRAMES`) writes every object alone, the format servers before
schema version 6 read (see Upgrading). `ant admin repack` rewrites the
history as delta frames afterwards (`docs/operator-guide.md`, Repacking).

## Local mode (one machine, no Postgres or S3)

For one workstation running a few tens of agents, `--local` replaces
Postgres and S3 with one data directory (SQLite metadata and small objects,
pack files for large objects; see `docs/architecture.md`, Local mode):

```sh
ant local start                        # ant serve --local in the background; prints started <pid> <grpc> <http>
ant local status                       # running <pid> <grpc> <http> (exit 0) | busy | stopped (exit 1)
ant local stop                         # SIGTERM, waits until the directory is released
ant serve --local /data/ant            # the same in the foreground, any directory
```

- **Directory.** `--local` without a value (and `ant local --dir`
  omitted) means `$ANT_LOCAL_DIR`, else `$XDG_DATA_HOME/antimony/local`
  (`~/.local/share/antimony/local`). `ant local start` keeps `server.pid`
  and `server.log` there and passes extra `ant serve` flags after `--`
  (`ant local start -- --policy policy.toml`).
- **One process per directory.** The server holds `<dir>/LOCK`; offline
  tools fail at once while it runs (`is in use by another process`). Import
  and collect with the server stopped:
  `ant local stop && ant import <git-dir> --local && ant admin gc --local && ant local start`.
- **Durability.** By default every acknowledged write and land is fsynced
  (group commit shares the fsyncs). `--local-sync normal` skips the fsync at
  commit: a crash of the server loses nothing, an OS crash or power loss can
  lose the last acknowledged lands (the database stays consistent).
- **Resources.** Defaults: async workers min(4, CPUs) (`--worker-threads`),
  server caches 48 MiB (`--cache-mib`), 4 SQLite readers
  (`--local-readers`) with 8 MiB page cache each, decoded-frame cache 16 MiB
  (`--local-frame-cache-mib`), concurrency limits of a 16-connection pool.
  Idle, the server does no work at all.
- **Garbage collection** (`ant admin gc --local`) is offline and immediate
  (no quiesce wait): same roots and grace period as on Postgres, then packs
  without live objects are deleted, packs at least half dead are compacted,
  and the database is vacuumed when a quarter of it is free. `ant admin
  repack` is not available in local mode.
- **Backups.** Stop the server (or use SQLite's `.backup` on `meta.db`) and
  copy the directory; pack files are append-only and never rewritten except
  by `ant admin gc --local`.
- **Moving to Postgres + S3** (or to another directory): stop the server
  and copy the directory into the new stores, then serve those:

  ```sh
  ant local stop
  ant admin copy-local --from ~/.local/share/antimony/local \
      --pg-url postgres://... --pg-schema ant --s3-endpoint ... --s3-bucket ...
  # objects <n> / commits <n> / derived <n> / refs <n>
  ant serve --pg-url postgres://... --pg-schema ant --s3-endpoint ...
  ```

  Every object, commit record, changeset and derived row (landed-as and
  import mappings, blame caches) is copied, then the refs the destination
  does not have yet (others are listed as `skipped <ref> <id>` and left
  alone). Ids do not change, so workspaces keep working against the new
  server once `ANT_SERVER` points at it; the path-history index is rebuilt
  on first use. The copy is idempotent: an interrupted one is run again.
  (A mirror clone of the git export is not a way back in: the export is
  SHA-256 and `ant import` reads SHA-1 repositories only.)

## Metrics and logs

**Metrics.** `GET /metrics` on the HTTP port returns Prometheus (OpenMetrics
text) metrics. It is unauthenticated, so either keep the HTTP port private or
move it with `--metrics-listen 127.0.0.1:9100` (`ANT_METRICS_LISTEN`): the
endpoint then exists only on that listener (the HTTP port answers 404, also
under a policy).

**Cache tier.** `ant cache-serve` (a caching proxy for `/o/` and `/t/`) exports
`ant_cacheserve_requests_total{route,result}`, `_response_bytes_total`,
`_upstream_requests_total{route,kind,status}`, `_upstream_bytes_total`,
`_authz_total{result}`, `_inflight_fetches` and `_disk_bytes` / `_disk_budget_bytes` / `_index_bytes`
at its own `GET /metrics`; see the cache tier section of the operator guide.

**Health.** `GET /healthz` is liveness (static `ok`). `GET /readyz` is
readiness: 200 `{"status":"ok","postgres":"ok","s3":"ok"}` when Postgres
(`SELECT 1`) and S3 (`HEAD` bucket) answer within 2 s, else 503 naming the
failing dependency (`"postgres":"down"`); in-memory is always ready. Both are
unauthenticated. `GET /debug/stats` needs an admin token under a policy. Load
balancer checks should use `/readyz`; see the operator guide.

| family | labels | meaning |
|---|---|---|
| `ant_rpc_requests_total`, `ant_rpc_duration_seconds` | `method`, `code` / `method` | gRPC calls; `code` is the snake_case status (`ok`, `not_found`, `aborted`, ...; `cancelled` if the client left early); duration runs to the end of the response (streams included) |
| `ant_http_requests_total`, `ant_http_duration_seconds` | `route`, `status` / `route` | `objects`, `object_checks`, `tree_bundles`, `git`, `healthz`, `readyz`, `metrics`, `other`; duration runs to the response headers |
| `ant_land_batch_size` | | requests per group-commit batch |
| `ant_land_outcomes_total` | `outcome` | `landed`, `rebased`, `conflict`, `aborted`, `denied` |
| `ant_land_cas_retries_total` | | batches replayed after losing the ref CAS (another replica landed first) |
| `ant_land_queue_depth` | `ref` | land requests waiting right now |
| `ant_store_op_duration_seconds`, `ant_store_errors_total` | `store` (`objects`, `refs`, `meta`), `op` | every store call (Postgres/S3 round trips) |
| `ant_cache_hits_total`, `ant_cache_misses_total`, `ant_cache_bytes` | `cache` (`trees`, `commits`, `nodes`, `changesets`) | in-process caches; hits and misses count since process start |
| `ant_history_lag_commits` | `ref` | commits the history index is behind the ref |
| `ant_history_catch_up_seconds` | | duration of catch-ups that indexed commits |
| `ant_ref_watchers` | | open `WatchRef` streams |
| `ant_ref_watch_events_total`, `ant_ref_watch_coalesced_events_total` | | ref events sent to watchers; of those, events covering an unknown number of moves (catch-ups, gap fills, slow watchers) |
| `ant_build_info` | `version`, `git` | always 1 |
| `ant_pg_pool_size`, `_idle`, `_in_use`, `_max` | | Postgres query pool (0 without Postgres) |
| `ant_s3_retries_total` | `reason` (`throttled`, `server_error`, `network`, `body`, `timeout`) | S3 attempts retried (see S3 timeouts, retries and throttling) |
| `ant_s3_gave_up_total` | | S3 operations that failed after retrying |
| `ant_s3_requests_in_flight`, `ant_s3_requests_queued`, `ant_s3_max_concurrency` | | S3 requests in flight, waiting for a slot, and the slots (0 without S3) |

`ref` labels are capped: the 64 most recently landed-on or indexed refs get
their own value, all others report as `ref="other"`. Useful starting alerts:
sustained `ant_land_queue_depth` growth, a rising share of
`ant_land_cas_retries_total`, `ant_history_lag_commits` that does not return
to 0, `ant_pg_pool_in_use` near `ant_pg_pool_max`, and p99
`ant_store_op_duration_seconds{op="cas_ref"}`.

**Logs.** `--log-format json|text` (`ANT_LOG_FORMAT`; default `text` on a
terminal, `json` otherwise) writes to stderr; the level comes from `RUST_LOG`
(default `info,sqlx=warn`; sqlx's per-start schema notices are muted). In JSON each line carries its enclosing spans, so every log
line of a request has the request's `request_id`.

**Request ids.** Every gRPC and HTTP request has an id: an incoming
`x-request-id` (1-128 visible ASCII characters) is kept, otherwise the server
generates a ULID. It is echoed in the `x-request-id` response header (gRPC
initial metadata or trailers-only headers), so a client can quote it when
reporting a problem and you can `grep` the logs for it.

**Slow requests.** A request taking 1 s or more is logged at WARN as
`slow request` with the method or route, duration and, under a policy, the
authenticated principal.

## Importing an existing repository

```sh
ant import /path/to/repo.git --git-ref refs/heads/main --ref main \
  --pg-url ... --s3-endpoint ...   # same store flags as the server
```

Re-running is incremental. With a policy, add `--policy` (or run
`ant admin reindex-acl main --history` afterwards) so objects written by the
import get their zone tags.

Plan capacity before importing. Full Linux-kernel history (1.49M commits)
writes about 160 GB of objects. Most of it goes to S3, plus about 17 GB of
Postgres tables (the largest are `objects`, ~9 GB, and `changes`, ~7 GB).

## Access policy

See `crates/ant-auth` for the format. Store only the SHA-256 of each token in
the policy:

```sh
printf %s "$TOKEN" | sha256sum
```

Clients send `ANT_TOKEN`. `git clone` uses HTTP Basic auth with the token as
the password; any username works.

After changing **path rules**, run `ant admin reindex-acl <rev> --history` so
zone tags match the new rules. Changes to read/write lists take effect
immediately.

## Sizing notes (measured, single node, WSL2, 20 cores)

- **Landing:** about 4,000 lands/s with 64 concurrent agents on Postgres +
  S3, land p99 ≈ 30 ms, zero aborts (`docs/scorecard/2026-10-14.md`).
  create_changeset p50 ≈ 2 ms: one durable Postgres statement per commit.
- **Replicas:** 4 replicas × 1,000 agents for 30 minutes: 266k lands, zero
  errors, RSS levelling off at about 610–660 MiB per replica with the
  default caches. `ANT_MALLOC_TRIM_SECS=<n>` (off by default) periodically
  returns freed heap to the OS; `MALLOC_ARENA_MAX=2` saves about 50 MiB.
- **Memory during git clones:** a full git/git clone peaked at about 1.0 GiB
  RSS against a live heap of 300-380 MiB: glibc keeps freed memory in
  per-thread arenas. The server therefore caps arenas before its runtime
  starts (`--malloc-arenas` / `ANT_MALLOC_ARENAS`, default twice the CPU
  count within 2..16; an explicit `MALLOC_ARENA_MAX` wins; `0` = glibc
  default; `ant serve`, `ant-server`, `ant-bench serve`). Measured: full
  clone 1005 -> 665 MiB, blobless 867 -> 632 MiB, no slowdown; 8 arenas
  gives about 565 MiB. An in-process 16-agent fleet loses about 2.6% at 16
  arenas (24 arenas: none, but about 700-780 MiB clone peak).
  `MALLOC_TRIM_THRESHOLD_`/`MALLOC_MMAP_THRESHOLD_` of 128 KiB halve the
  peak again but make clones 30-80% slower, so they are not set.
- **Postgres:** `max_connections` must cover every server's pools
  (`--pg-max-connections`, default 64, plus `--pg-lock-connections`,
  default 8, per process; ref-lock waits are bounded by `--pg-lock-timeout`,
  default 10s). Under sustained bulk import, autovacuum on `objects`
  shows up; give it I/O headroom.
- **Clients:** the shared disk cache defaults to 20 GiB
  (`--cache-bytes` / `ANT_CACHE_BYTES`). Sparse workspaces keep checkouts
  proportional to what an agent actually touches.

## Upgrading

**Postgres schema.** Each schema records its layout version in the
`schema_version` table (one row per applied migration: version, name, time,
binary version). `ant serve` and `ant import` (and `ant admin`) migrate the
schema on start: they apply the missing migrations in order, in one
transaction, under a per-schema Postgres advisory lock. Replicas that start
together are safe: one migrates, the others wait for the lock and find
nothing left to do. Migrations are idempotent and additive, so a schema
written by a release from before versioning (version 0: no `schema_version`
table, per-path `changes` rows, large objects one per S3 key) is adopted in
place and stays readable.

```sh
ant admin schema --pg-url ... --pg-schema ant            # inspect
# version 4
# supported 5
# pending 5 gc
ant admin schema --migrate --pg-url ... --pg-schema ant  # apply now
```

| version | migration | adds |
|---|---|---|
| 1 | `baseline` | objects, refs, commit graph, log, per-path changes, derived data, history index |
| 2 | `object_index` | index of large objects bundled into S3 packs |
| 3 | `changesets` | one compressed changeset row per commit |
| 4 | `storage_tuning` | out-of-line storage and insert-only autovacuum settings |
| 5 | `gc` | write times (`written_at`, BRIN-indexed) on objects, packed-object index rows, commits and changesets; garbage-collection state (`gc_condemned`, `gc_live`, `gc_state`) |
| 6 | `pack_frames` | `object_index.share` (a nullable column, no table rewrite); marks that S3 packs may hold multi-object frames, which servers supporting only version 5 cannot read |
| 7 | `repack` | the `packs` table (`ant admin repack` state: packs it wrote, packs it moved rows out of; a new table, no rewrite); marks that S3 packs may hold delta frames, which servers supporting only version 6 cannot read |

To control when migrations run, start servers with `--no-migrate`
(`ANT_NO_MIGRATE=1`) and run `ant admin schema --migrate` yourself; a
server started with `--no-migrate` against an out-of-date schema exits with
`error: Postgres schema "ant" is at version N, ...` and a `hint:`.

**Never run an older binary against a newer store.** A binary refuses a
schema whose version is higher than it supports (`error: Postgres schema
"ant" version 5 is newer than this version of ant supports (up to 4)`,
`hint: upgrade ant ...`) instead of misreading it; for example, servers from
before migration 2 cannot see objects stored in S3 packs. Releases from
before schema versioning do not have this check, so do not start one against
a migrated schema. In a rolling upgrade, start one new server first (it
migrates); servers of the previous release that are already running keep
working while you replace them unless the release notes say a migration
needs all servers stopped first. Take a Postgres backup before upgrading:
there are no down-migrations. Migration 5 gives every existing row the
migration time as its write time, so garbage collection cannot delete
anything written before the upgrade until one grace period after it; run
`ant admin gc` only once every server runs a release with schema version 5
or later (older servers neither hide condemned data nor refresh write
times).

**Pack frames (schema version 6).** From version 6 on, servers write similar
large objects into shared, compressed multi-object frames inside S3 packs
(`ant_store::packed`); servers that support only version 5 cannot decode
them (a read fails with `backend error: ... corrupt pack record` or a zstd
error) and refuse to start against the migrated schema. Old packs stay
readable. Upgrading from 0.1.x (schema version 5) to 0.2 is therefore
either a stop-and-start (stop every 0.1.x server and import job, back up,
start the 0.2 servers) or this rolling upgrade, during which the servers
keep serving:

1. Back up Postgres.
2. Run every 0.2 `ant serve` *and* `ant import` with `--pack-frames false`
   (`ANT_PACK_FRAMES=false`): they read frames but write every object
   alone, in the old format.
3. Start the first 0.2 server. It migrates the schema to version 7 at once
   (migrations 6 and 7), whatever the flag (`ant admin schema` prints
   `version 7`). The 0.1.x
   servers already running keep serving reads and writes, including
   objects the 0.2 servers wrote, but none of them can be started again: a
   0.1.x `ant serve`, `ant import` or `ant admin` now exits 1 with `hint:
   upgrade ant` and changes nothing. Keep supervisors and autoscalers from
   restarting or adding 0.1.x replicas from here on.
4. Replace the remaining 0.1.x servers with 0.2 servers (flag still
   `false`).
5. Once no 0.1.x process is left, restart the 0.2 servers without the flag
   and drop it from import jobs. Frames are written from then on.

After step 3 the only way back to 0.1.x is the backup: nothing a 0.1.x
server cannot read is written before step 5, but the 0.1.x binary refuses
the migrated schema. A 0.1.x server still running after step 5 fails the
reads of framed objects (the client gets `error:` and exit 1; the server
logs `backend error: ... Unknown frame descriptor` or `corrupt pack
record`); it never returns wrong bytes. `ant admin repack` (delta frames,
`docs/operator-guide.md`, Repacking) recompresses objects written while
frames were off; run it only once no 0.1.x process is left. Clients need no particular order: 0.1.x and
0.2 `ant` (and `ant cache-serve`) work against servers of either version,
and keep using caches and workspaces the other one wrote.
`scripts/upgrade-test.sh` checks all of this against a real Postgres and
S3.

**Clients.** The disk cache (`<cache>/format`), sparse workspaces
(`format=` in `.ant/workspace`) and lazy workspace state directories
(`<state>/format`) carry a format version too. Missing markers mean
version 1, the layout from before markers existed, so existing caches and
workspaces keep working; a newer version is refused with `hint: upgrade
ant`, and so is a lazy workspace journal record of an unknown kind. Upgrade
`ant` on a machine before using caches or workspaces that a newer `ant` has
written.

## Local development services

`scripts/dev-services.sh start|stop|status|wipe` runs Postgres and SeaweedFS
as user processes on the ports the tests expect (no Docker needed).
`docker compose up -d` is the equivalent with containers, and is what CI uses.

Tests create scratch Postgres schemas (`t_<label>_<unix secs>_<random>`) and
S3 prefixes (`<label>-t<unix secs>-<random>` in the shared `ant-test` bucket) and remove
them when they finish, also on panic. A killed test run leaves them behind;
every test process sweeps those older than 6 hours when it starts
(`ANT_TEST_SWEEP_HOURS`), and `scripts/clean-test-data.sh` does the same on
demand (`--all` when no test is running, `--drop-bucket` to also delete the
S3 test bucket: SeaweedFS gives disk space back only for deleted buckets,
not for deleted objects).

## Benchmarks

- `scripts/scorecard.sh` runs the full v1 benchmark set on a quiet machine and
  writes a markdown report.
- `scripts/scorecard-local.sh` runs the local-mode set (idle footprint,
  fleets of 16 and 50 agents, git/git import, queries, checkouts, clones),
  each server a separate `ant local start` process; run it under
  `scripts/bench-lock.sh`.
- `ant-bench` (`import`, `fleet`, `checkout`) runs individual measurements.
