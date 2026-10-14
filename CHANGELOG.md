# Changelog

All notable changes to antimony are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org) as described in
[docs/compatibility.md](docs/compatibility.md), which also says what a minor
release of the 0.x series may change.

## [Unreleased]

### Added

- Overload protection (`ant-server` limits): per-class concurrency gates with
  a bounded queue that shed load (gRPC `UNAVAILABLE` / HTTP 503) are on by
  default, sized from `--pg-max-connections`. Per-caller token-bucket rate
  limits (`RESOURCE_EXHAUSTED` / 429) are opt-in and unlimited by default:
  `--rate-{read,write,admin}` or `[principal.limits]` in the policy (additive
  field). Rejections carry `retry-after-ms` (gRPC metadata) and `Retry-After`;
  `ant`/`antd` retry such hinted rejections with jittered, bounded backoff.
  Metrics `ant_ratelimit_*`; `--no-limits` disables everything. The gRPC
  client buffers each request body once so it can be replayed, a memory cost
  proportional to the request size for large `PutObjects`/`CreateChangeset`.
- History queries for CI, bisect, revert and "did it merge" tooling, without
  a checkout or a client-side log walk: RPCs `MergeBase`, `IsAncestor`
  (optionally first-parent only), `LogRange` (`from..to`, first parents,
  path filter through the history index, paged) and `LandedAs` (follows the
  server-side rebase mapping); `Diff` takes `path_prefixes` (only those
  subtrees are diffed), `merge_base` (`from...to`) and `patch` (unified
  hunks per text file computed on the server, binary files flagged, with
  per-file and per-response size limits). Access control filters them like
  `Log` and `Diff`. CLI: `ant merge-base`, `ant log <a>..<b> [-- <path>]`,
  `ant diff --patch [-U N] [--merge-base] <a> <b> [-- <path>...]`,
  `ant landed-as <commit> [--ref <name>]`.
- Land event stream for post-merge CI: server-streaming RPC `WatchRef`
  (`RefEvent`: old/new tip, every landed commit oldest first, kind
  `land`/`update`/`force`, `coalesced`), with an optional catch-up from a
  commit, gapless across replicas (Postgres `LISTEN/NOTIFY` on ref moves,
  gaps filled from the ref) and bounded per-watcher buffering (slow watchers
  get coalesced events). Needs `read`, `update` or `land` on the ref; ref
  rules accept a new `read` list. CLI: `ant ref watch <ref> [--from <rev>]`.
  Metrics `ant_ref_watchers`, `ant_ref_watch_events_total`,
  `ant_ref_watch_coalesced_events_total`. Postgres replicas with watchers
  hold one extra `LISTEN` connection.

### Changed

- `ant ref set` checks fast-forwards with one `IsAncestor` call instead of
  paging through `Log` (still the fallback against older servers).

### Performance

- Ancestry walks (`graph::merge_base`, `graph::is_ancestor`) read commit nodes a first-parent segment per store call
  (`first_parent_chain`, bounded by the generation gap) instead of one
  round trip per commit: a merge base 10,000 commits back takes 7 store
  calls instead of about 10,000.

- First (uncached) `blame` of a long-lived file is 10-25x faster, with
  byte-identical output: changes between versions are diffed on the changed
  region only (bytewise prefix/suffix, integer line ids, bit-parallel LCS
  with the previous tie-break), version diffs run in parallel on blocking
  threads, and blob reads are prefetched a few batches ahead of the walk.

## [0.1.0] - Unreleased

First release. Working end to end against real Postgres and S3, covered by
unit, integration and property tests; see "Known limitations" before
deploying.

### Server and landing

- Server-side commits: an agent sends file edits against a base commit and
  the server builds the trees and the commit (`CreateChangeset`); cost scales
  with the change, not the repository.
- `Land` rebases onto the moving tip on the server (3-way text merges) and
  advances the ref by compare-and-swap. Conflicts are returned as a list of
  paths. Retries are idempotent and stacked commits land together. Lands on a
  ref are group-committed in pipelined batches with one durable commit per
  batch.
- Any number of `ant serve` replicas can share one Postgres (and S3):
  per-ref Postgres advisory locks avoid racing, the ref CAS stays the
  correctness guarantee (`--pg-max-connections`, `--pg-lock-connections`,
  `--pg-lock-timeout`).
- Queries need no checkout: `ReadPath`, `Log` (with an indexed per-ref path
  history), `Diff`, `Blame`, `ResolveRev`.
- Access control (`--policy policy.toml`): SHA-256-hashed bearer tokens,
  longest-prefix path rules, ref rules, admins. Objects written under a
  restricted path are tagged with a zone and reported as missing by id to
  principals who cannot read them. `ant admin reindex-acl` tags history
  imported or written before a policy existed. The threat model and its
  limits are in the README.
- gRPC on `:7420`; HTTP on `:7421` serving immutable objects at `GET /o/<id>`
  (`Cache-Control: immutable`, so any CDN is the cache tier), `/healthz`,
  `/readyz` (checks Postgres and S3), `/metrics`, `/debug/stats` (admin only)
  and the git export.

### Storage and upgrades

- Git object format with SHA-256 ids, canonical decoding, raw-byte commit
  text; legacy tree entry modes (`100664`, `40755`, ...) are kept byte for
  byte so ids equal git's own SHA-256 conversion.
- Stores: in-memory, Postgres (objects up to 4 KiB, refs, commit graph,
  history index, changesets) and S3 (large objects bundled into packs, with
  an index in Postgres), tiered together, behind shared conformance suites.
  No packfiles to maintain on the server.
- Versioned Postgres schema (`schema_version`, five migrations: `baseline`,
  `object_index`, `changesets`, `storage_tuning`, `gc`). Migrations run on
  start under an advisory lock; `--no-migrate` and `ant admin schema
  [--migrate]` give control. A schema from before versioning is adopted in
  place; a newer schema is refused with a hint. There are no
  down-migrations: back up first.
- Client format markers on the disk cache, sparse workspaces and lazy
  workspace state; newer formats are refused.
- Object garbage collection, `ant admin gc`: mark, condemn, quiesce,
  re-mark, sweep, with a grace period (default 7 days, minimum 1 day),
  resumable and abortable, dry-run mode, and deletion of S3 packs without
  live objects.

### Git interoperability

- `ant import <git-dir>`: parallel, incremental import with exact git SHA-256
  ids, bounded memory, S3 object packs and a compare-and-swap ref publish, so
  an import never overwrites a land that moved the ref. Optional
  `ANT_IMPORT_DEFERRED=1` (Postgres) defers WAL flushes; see limitations.
- Read-only git smart-HTTP export (protocol v2) at `/repo.git`: `git clone`,
  `--filter=blob:none`, `--depth` and `git fetch` work for humans and IDEs,
  with delta packs. Requires read access to everything when a policy is on.

### Clients and workspaces

- Shared per-machine disk cache with read-through and in-flight
  deduplication; the budget (`--cache-bytes`, default 20 GiB) is enforced
  automatically, least recently used first.
- Sparse workspaces (`ant ws new --sparse <pattern>`) materialize only what
  matches; parallel materialization and status.
- Lazy workspaces over FUSE (`ant ws new --lazy`): writable copy-on-write
  with write-behind and a journaled upper layer that survives crashes;
  `ant ws unmount` keeps edits and `ant ws remount` recovers after a dead
  FUSE daemon. `ant mount <rev> <dir>` gives an instant read-only view of any
  commit. Linux only, needs `/dev/fuse` and `fusermount3`.
- `ant ws update` / `ant ws resolve`: carry local edits onto a new base with
  the server's 3-way merge, conflicts marked in the files and shown as `C` by
  `ant status`; crash-safe on lazy workspaces.

### CLI

- One binary, `ant`, is client and server (`ant serve`); `ant-server` is a
  thin wrapper. Commands: `import`, `ls`, `cat`, `blame`, `log`, `diff`,
  `ref get|list|set|init`, `ws new|update|resolve|remount|unmount`, `status`,
  `submit`, `land`, `mount`, `serve`, `cache stats|gc`, `admin
  reindex-acl|schema|gc`, `completions`.
- A documented output contract (module docs of `crates/ant-cli/src/main.rs`):
  one record per line, `error:` / `hint:` on stderr, exit `0` ok, `1` error,
  `2` land or update conflict. `ant ref set` refuses non-fast-forward moves
  without `--force`; `ant ref init` seeds an empty server.
- `ant --version` prints `ant 0.1.0 (<short git sha>[-dirty])`.
- Versioned compatibility rules for the API, schema, on-disk formats and CLI:
  [docs/compatibility.md](docs/compatibility.md).

### Operations and observability

- Prometheus `/metrics` (RPC, HTTP, landing, stores, caches, history lag,
  Postgres pool, `ant_build_info`), request ids, JSON logs
  (`--log-format json`), slow-request warnings; measured overhead under 2%.
  `--metrics-listen` moves metrics off the public port.
- Docker image (`debian:bookworm-slim` + `fuse3`, entrypoint `ant`) and a
  compose file with Postgres, S3 and the server; `scripts/dev-services.sh`
  runs the same services natively.
- Operator guide: topology, sizing, metrics and alerts, failure runbook,
  backups, garbage collection, upgrading ([docs/](docs/)).
- Load and scale harness `ant-bench` (`import`, `fleet`, `checkout`, `soak`)
  and `scripts/scorecard.sh`, which runs the headline benchmarks against the
  v1 targets.

### Performance

From [docs/scorecard/](docs/scorecard/) (WSL2 on one development machine, one
Postgres and SeaweedFS S3 on the same host; numbers are indicative, not
guarantees) and the ROADMAP status:

| measure | result |
|---|---|
| Landing, 64 agents, Postgres + S3 | 2,450 to 4,070 lands/s across scorecard runs (median 3,870/s in the latest bisect work), p99 28 to 84 ms (median 38 ms), 0 aborts, exact history |
| Landing, 16 agents | 2,230 to 2,510 lands/s, p99 about 7 ms |
| `create_changeset` p50, 16 agents | 1.9 to 3.0 ms |
| Import of git/git (82k commits) | 6,400 to 6,800 commits/s, 0.8 to 0.9 GiB peak RSS |
| Linux kernel import (partial runs) | 1.0 to 1.5k commits/s, flat memory, about 2 GiB RSS |
| Soak | 4 replicas x 1,000 agents x 30 min: 266k lands, 0 errors, exact history, flat RSS |
| Checkout, synthetic 96k files (medians) | cold 2.7 s, warm 1.27 s, status 0.09 s |
| Queries on kernel-size history | log 6 to 40 ms, path log 17 to 36 ms, blame 55 ms cached (first blame 0.6 s on git/git) |
| Git export, git/git | `--depth 1` 0.6 to 0.9 s; `blob:none` clone 116 MiB in 14 s (native git: 5.8 s) |
| Lazy workspace writes | 72% of direct write speed, 7,700 small files/s, `cargo build` 1.27x |

The afternoon scorecard of 2026-10-14 scored 25 of 27 targets; its two
failures were the 96k-file checkout rows, whose quiet-machine medians (above)
meet the targets (the run was a first-run outlier). It also showed the
64-agent landing regression listed in the ROADMAP work queue: re-run the
scorecard before relying on those landing numbers.

### Known limitations

- **Restricted structure is visible.** Trees readable by a principal list the
  names and ids of restricted entries, and commit metadata (messages,
  authors, ids, parents) is visible to every authenticated principal.
  Response timing still differs slightly between missing and hidden objects.
  Tokens are bearer secrets: run behind TLS. See the README's threat model.
- **First blame of a long-lived large file takes seconds** (cached
  afterwards).
- **Commit creation latency** is bounded by one durable Postgres commit.
- **Linux only for lazy views.** FUSE workspaces and `ant mount` need Linux;
  there is no macOS client yet (planned, NFS fallback first). Sparse
  workspaces and the CLI client are not FUSE-dependent.
- **Single architecture build.** Release binaries are x86_64 Linux; the
  Docker image is built for the runner's architecture only (amd64).
- **Full Linux kernel import is not re-measured.** Partial runs and the
  earlier full import (1.49M commits, about 160 GB objects plus 17 GB of
  Postgres) work, but the "under 15 minutes" target is unproven; serial
  commit recording and Postgres checkpoint stalls are the bottleneck. The
  kernel `blob:none` clone is likewise unmeasured.
- **Deferred import durability is opt-in and unproven.** `ANT_IMPORT_DEFERRED=1`
  cut `put_commits` about 10x but showed no net gain on the measured
  machine; it may be removed in a later release. Leave it off.
- **Dead S3 pack bytes are not repacked.** GC deletes packs with no live
  objects only; partly dead packs stay and are reported (`packs-kept ...
  <dead-bytes> <dead-percent>`).
- **GC requirements.** `ant admin gc` needs Postgres stores (S3 packs with
  `--s3-*`), one run per schema at a time, every server on schema version 5
  or later (older servers neither hide condemned data nor refresh write
  times), a grace period of at least 1 day, and a quiesce period longer than
  the servers' 10-minute cache lifetime plus your longest request. After
  migrating, nothing written before the upgrade is collectable until one
  grace period has passed. Unlanded changesets older than the grace period
  may be collected.
- **No down-migrations.** Back up Postgres before upgrading; a newer schema
  cannot be used by an older binary.
- **Access-control edge cases.** Objects written before a policy existed, by
  `ant import` or by an admin through `PutObjects` are public until `ant
  admin reindex-acl`; path-rule changes need a reindex. Creating a ref needs
  only the ref rule. Concurrent first writes of identical bytes to a public
  and a restricted path can leave the object hidden by id (path reads still
  work). Workspaces of principals who cannot read everything must be sparse
  around restricted paths, and principals sharing a cache directory share
  what it holds.

[Unreleased]: https://github.com/Mapika/antimony/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Mapika/antimony/releases/tag/v0.1.0
