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
of both ports.

## Metrics and logs

**Metrics.** `GET /metrics` on the HTTP port returns Prometheus (OpenMetrics
text) metrics. It is unauthenticated, so either keep the HTTP port private or
move it with `--metrics-listen 127.0.0.1:9100` (`ANT_METRICS_LISTEN`): the
endpoint then exists only on that listener (the HTTP port answers 404, also
under a policy).

**Health.** `GET /healthz` is liveness (static `ok`). `GET /readyz` is
readiness: 200 `{"status":"ok","postgres":"ok","s3":"ok"}` when Postgres
(`SELECT 1`) and S3 (`HEAD` bucket) answer within 2 s, else 503 naming the
failing dependency (`"postgres":"down"`); in-memory is always ready. Both are
unauthenticated. `GET /debug/stats` needs an admin token under a policy. Load
balancer checks should use `/readyz`; see the operator guide.

| family | labels | meaning |
|---|---|---|
| `ant_rpc_requests_total`, `ant_rpc_duration_seconds` | `method`, `code` / `method` | gRPC calls; `code` is the snake_case status (`ok`, `not_found`, `aborted`, ...; `cancelled` if the client left early); duration runs to the end of the response (streams included) |
| `ant_http_requests_total`, `ant_http_duration_seconds` | `route`, `status` / `route` | `objects`, `tree_bundles`, `git`, `healthz`, `readyz`, `metrics`, `other`; duration runs to the response headers |
| `ant_land_batch_size` | | requests per group-commit batch |
| `ant_land_outcomes_total` | `outcome` | `landed`, `rebased`, `conflict`, `aborted`, `denied` |
| `ant_land_cas_retries_total` | | batches replayed after losing the ref CAS (another replica landed first) |
| `ant_land_queue_depth` | `ref` | land requests waiting right now |
| `ant_store_op_duration_seconds`, `ant_store_errors_total` | `store` (`objects`, `refs`, `meta`), `op` | every store call (Postgres/S3 round trips) |
| `ant_cache_hits_total`, `ant_cache_misses_total`, `ant_cache_bytes` | `cache` (`trees`, `commits`, `nodes`, `changesets`) | in-process caches; hits and misses count since process start |
| `ant_history_lag_commits` | `ref` | commits the history index is behind the ref |
| `ant_history_catch_up_seconds` | | duration of catch-ups that indexed commits |
| `ant_build_info` | `version`, `git` | always 1 |
| `ant_pg_pool_size`, `_idle`, `_in_use`, `_max` | | Postgres query pool (0 without Postgres) |

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

## Benchmarks

- `scripts/scorecard.sh` runs the full v1 benchmark set on a quiet machine and
  writes a markdown report.
- `ant-bench` (`import`, `fleet`, `checkout`) runs individual measurements.
