# Running antimony

## Components

| process | role | state |
|---|---|---|
| `ant serve` | gRPC (`:7420`) + HTTP (`:7421`: `/o/<id>` objects, `/repo.git` git export, `/healthz`) | none; caches only |
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
see `ant serve --help`). The schema and tables are created on first start.
Without `--policy` the server runs in **open mode** (no authentication) and
logs a warning; never expose an open-mode server.

Run it behind TLS. Bearer tokens are secrets, and the server itself does not
terminate TLS. Put a reverse proxy (or a gRPC-aware load balancer) in front
of both ports.

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
