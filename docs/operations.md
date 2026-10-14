# Running antimony

## Components

| process | role | state |
|---|---|---|
| `ant serve` | gRPC (`:7420`) + HTTP (`:7421`: `/o/<id>` objects, `/repo.git` git export, `/healthz`, `/metrics`) | none; caches only |
| PostgreSQL ≥ 15 | refs, commit graph, changesets, small objects, history index | all metadata |
| S3-compatible store | objects larger than 4 KiB (zstd) | large objects |
| HTTP cache / CDN (optional) | caches `GET /o/<id>` (immutable; `private` for restricted zones) | disposable |

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
endpoint then exists only on that listener, and the HTTP port answers 404.

| family | labels | meaning |
|---|---|---|
| `ant_rpc_requests_total`, `ant_rpc_duration_seconds` | `method`, `code` / `method` | gRPC calls; `code` is the snake_case status (`ok`, `not_found`, `aborted`, ...; `cancelled` if the client left early); duration runs to the end of the response (streams included) |
| `ant_http_requests_total`, `ant_http_duration_seconds` | `route`, `status` / `route` | `objects`, `git`, `healthz`, `metrics`, `other`; duration runs to the response headers |
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

- **Landing:** about 1,200 lands/s with 64 concurrent agents on Postgres +
  S3, with zero aborts. Commit creation is bounded by one durable Postgres
  commit (≈ 5–8 ms per synchronous commit on that disk).
- **Postgres:** `max_connections` must cover every server's pool (64 per
  process by default). Under sustained bulk import, autovacuum on `objects`
  shows up; give it I/O headroom.
- **Clients:** the shared disk cache defaults to 20 GiB
  (`--cache-bytes` / `ANT_CACHE_BYTES`). Sparse workspaces keep checkouts
  proportional to what an agent actually touches.

## Local development services

`scripts/dev-services.sh start|stop|status|wipe` runs Postgres and SeaweedFS
as user processes on the ports the tests expect (no Docker needed).
`docker compose up -d` is the equivalent with containers, and is what CI uses.

## Benchmarks

- `scripts/scorecard.sh` runs the full v1 benchmark set on a quiet machine and
  writes a markdown report.
- `ant-bench` (`import`, `fleet`, `checkout`) runs individual measurements.
