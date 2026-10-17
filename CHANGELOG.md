# Changelog

All notable changes to antimony are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org) as described in
[docs/compatibility.md](docs/compatibility.md), which also says what a minor
release of the 0.x series may change.

## [Unreleased]

### Added

- Local mode can be maintained while it serves (docs/operations.md, Local
  mode):
  - `ant admin gc --local` runs in the server using the directory (it
    listens on `<dir>/admin.sock`, mode 0600) when one does, and offline
    otherwise. Online runs follow the Postgres collector's protocol
    (condemned data reads as missing, `--quiesce` wait, re-mark, sweep in
    small durable transactions); packs without live objects are deleted
    and mostly-dead ones compacted while readers keep reading; freed
    database pages are returned while serving (`incremental_vacuum`).
  - `ant serve --local --gc-interval <dur>` collects on a schedule
    (`--gc-grace`, default `7d`; `--gc-quiesce`, default `30m`).
  - `ant admin repack --local`: the delta-frame repack of Postgres + S3
    stores for local data directories, online or offline, resumable;
    old packs are deleted when nothing points into them any more (stdout
    adds `packs-deleted <count> <bytes>`).
    git/git imported in local mode: 941 MiB → 223 MiB after an offline
    repack (40-60 s), against 298 MiB for `git gc` and 94 MiB for
    `git gc --aggressive`.

### Changed

- Local data directories: SQLite schema version 2 (migration
  `repacked_packs`, applied on open; 0.3.x binaries then refuse the
  directory). New databases use incremental auto-vacuum. Pack files may
  hold `ADF1` delta frames (written by `ant admin repack --local`). See
  docs/compatibility.md.
- `ant admin gc --local` reports `condemned` (ids condemned) and `kept`
  (condemned ids written or referenced again before the sweep) like
  Postgres runs, and no longer fails while a server uses the directory.

### Performance

- Local mode landing, 2-6x faster and no longer slowing down as the WAL
  fills. Durability is unchanged: a landed, acknowledged commit survives
  power loss with `--local-sync full`.
  - Fixed: checkpoints were scheduled by the WAL file's size, which stays
    at its 64 MiB limit once reached. From then on every commit group ran
    a checkpoint and a WAL restart, which took about two thirds of the
    writer's time. That is why a local server slowed down as it ran.
    Checkpoints are now scheduled by the WAL's length after each commit
    (SQLite's WAL hook).
  - WAL restarts no longer stop the writer for 15-75 ms. The checkpoint
    thread syncs the database after each copy, because SQLite skips that
    sync whenever a writer commits during the checkpoint. The restart
    then copies only the frames committed since and waits for the reads
    in flight instead of polling for a gap in the read stream.
  - A land publish is one writer job, answered at commit and again once
    an fsync covers it, instead of a publish plus an empty durable write.
  - On a ref that keeps moving, history index catch-ups run at most every
    100 ms (all backends), so each one indexes many lands in one write.
  - Fleet benchmark (WSL2, interleaved with the previous build,
    `fdatasync` p50 1.5 ms):
    - 16 agents: 1,320-2,150 lands/s, p99 20-45 ms (was 320-490/s, p99
      60-420 ms).
    - 50 agents: 2,370-2,940 lands/s, p99 32-52 ms (was 710-1,130/s, p99
      54-370 ms).
    - With the disk degraded by back-to-back runs (`fdatasync` p50 6-9 ms
      instead of 1.5 ms) both builds slow down: 16 agents 880/s against
      200-270/s; 50 agents 380-1,760/s (p99 76-280 ms) against
      25-980/s.
  - Idle RSS is unchanged at 16 MiB. A git/git import into a local
    directory runs at 6.9-10.8k commits/s (was 5.3-5.8k/s).

### Fixed

- Garbage collection (Postgres and local) reports as `missing` only ids
  that refs, history or landed-as/import mappings reach. A collection
  running beside `CreateChangeset` or landing counted the objects of
  commits still being written (records and objects are written
  concurrently, and a land batch builds on its predecessor's writes in
  flight) as missing, and so did unreachable leftovers of a writer stopped
  midway. Nothing was deleted wrongly; only the report was off.

## [0.3.2] - 2026-10-17

Patch release: no Postgres schema or protocol changes; 0.3.x servers and
clients interoperate. The server cache (tree bundles and git base packs)
persists across restarts on disk (`--cache-dir`) and optionally in shared S3
(`--shared-cache`); on Hetzner the first checkout after a restart went from
4.92 to 2.71 s and the first blobless clone from 12.1 to 4.39 s (1.46x
native git). New cache formats are additive and versioned.

### Added

- Persistent server cache: tree bundles (`GET /t/`) and the git export's
  base packs survive restarts, so the first checkout and the first clone
  after a restart or deploy no longer rebuild them
  (docs/operations.md, "Persistent server caches").
  - `ant serve --cache-dir DIR` (`ANT_CACHE_DIR`; default `<data dir>/cache`
    in local mode, `--no-cache-dir` to turn that off) with
    `--cache-dir-mib` (default 4096, local 1024; LRU). Entries of different
    repositories are kept apart; one process per directory.
  - `--shared-cache` (`--shared-cache-mib`, default 16384): also keep
    entries in the store's S3 bucket under `<s3 prefix>/cache/`, shared by
    replicas (a new replica reads them instead of building).
  - `--prewarm REF` loads or builds the bundles of a checkout of REF in the
    background after start; `--prewarm-git blobless|full` loads or builds
    the base pack for clones of `main`.
  - Safe by construction: entries are checked (SHA-256) on every read and
    dropped when damaged; a cached bundle is served only while its tree is
    in the store and only to callers the serving process's policy lets
    read all of it; a base pack only when every wanted commit is in the
    store. Format: docs/compatibility.md ("Persistent server cache").
  - `/debug/stats` reports it under `persistent_cache`.
  - Hetzner ccx43 + ccx33, after a server restart onto a filled cache:
    synthetic 96k-file checkout, first one 4.92 → 2.71 s (the warm-server
    median is 2.46 s), `ant serve` CPU 23.2 → 7.7 s, S3 GETs 3.5k → 85;
    git/git blobless clone, first one 12.1 → 4.39 s (repeat 4.30 s, native
    `git daemon` 3.01 s: 1.46×); the 70 MiB base pack loads in 96 ms.
  - Local (WSL2, loopback): git/git blobless clone after a restart 5.1 →
    2.0 s (native 2.3 s); prewarmed 1.95 s.
- `scripts/checkout-bench.sh --server-cache` (rows `<build>-persist`: a
  server restarted onto a filled cache) and `scripts/vs-git.sh
  --server-cache` (the clone phase meets a restarted server whose base
  pack is on disk).

### Fixed

- The git export served a clone from its cached base pack without checking
  that the wanted commits were still in the store (a want equal to a base
  tip was not looked up); after garbage collection of such a commit a
  client knowing its id could still clone it until the server restarted.
  Every want is now checked.

## [0.3.1] - 2026-10-17

Patch release: no storage, schema or protocol changes; 0.3.0 servers and
clients interoperate. Faster warm checkout (Hetzner 96k files 1.72 -> 1.51 s)
and first checkout against a freshly started server (6.1-6.9 -> 4.1-4.5 s,
S3 GETs 35k -> 3k) through a new S3 pack segment cache
(`--s3-pack-cache-mib`, default 512).

### Added

- S3 pack segment cache (`--s3-pack-cache-mib`, `ANT_S3_PACK_CACHE_MIB`,
  default 512, 0 turns it off). Packed records are read as aligned 64 KiB
  segments of their pack: missing segments of one pack at most 1 MiB
  apart are read with one ranged GET of at most 8 MiB, everything read is
  kept (least recently used evicted first), and a segment another caller
  is already reading is waited for, not read again. A checkout or clone
  against a freshly started server no longer issues about one GET per
  object.
  - Synthetic 96k-file checkout, first one against a freshly started
    server, Hetzner ccx43 + ccx33: 6.1-6.9 s → 4.1-4.5 s; S3 GETs
    34.8k → 3.0k, bytes read 1.15 → 0.59 GiB, SeaweedFS CPU 40 → 5 s,
    `ant serve` CPU 26 → 16 s.
  - git/git blobless clone with no cached base: S3 GETs 5.4k → 1.4k, bytes
    272 → 118 MiB; its time is the export's delta search (CPU), not S3.
- Metrics: `ant_s3_requests_total{op}`, `ant_s3_read_bytes_total`,
  `ant_s3_pack_segments_total{result=hit|read}`, `ant_s3_pack_cache_bytes`.
- `scripts/checkout-bench.sh` reports S3 GETs, bytes and server CPU per row.
- `ant-bench checkout --warm-only` (warm checkouts against an existing
  `--cache`, no cold phase) and `--no-status`, to profile the warm path
  alone.

### Changed

- Faster warm checkout (objects already in the client cache): on Hetzner
  (ccx33 client), 96k files over the network 1.72 → 1.51 s (median of 9),
  cold 2.68 → 2.48 s.
  - Files are created in path order. Leaves are sorted before they reach
    the writers, so each writer creates a directory and its contents
    together and writers work in different subtrees. The walk's order made
    ext4 on the cloud disk block in the kernel (about 40 threads waiting on
    the filesystem) rather than use the CPU.
  - Writers remember the directories they created: one `mkdir` per
    directory, without failed retries and `stat`s.
  - Blob reads no longer occupy writer slots.
  - The workspace index is rendered without an allocation per line.
- Client cache packs: up to 256 packs stay open per cache handle, and the
  records a read wants from one pack are read with one `pread` per run of
  nearby records (a warm 96k checkout: 161k → 87k `pread`s). An open pack
  is checked again (still present, mtime to bump) once per touch interval;
  a pack evicted by another process stays readable through the open
  handle until then. No on-disk format change.

### Fixed

- The `repack_races_writers_readers_and_collections` test could run past
  nextest's 300 s timeout on a slower CI runner (it normally takes 66-86 s
  there). It was not a hang: its writers landed at a fixed pace, so a
  slower machine grew more history during each collection round, and each
  round's mark, which costs time in proportion to the history, took longer
  than the one before. Writers now slow to a land a second each after 300
  lands in a phase. They keep racing every phase, and the test's work no
  longer grows from round to round. With marking slowed artificially by
  10/20/40 ms per batch, the test took 408 s/over 600 s/over 600 s before
  this change and 76/125/250 s after it. The pack segment cache made no
  difference.

## [0.3.0] - 2026-10-17

Minor release: no Postgres schema or protocol changes; 0.2.x servers and
clients interoperate. New: local mode (`ant serve --local`, SQLite + local
packs, no Postgres/S3). New on-disk formats are additive (local data
directory, client cache `packs/`). Final scorecard: 27/27; upgrade test
from v0.2.2: 83 passed, 0 failed; cloud numbers from two Hetzner boxes.

### Added

- `ant serve --bundle-cache-mib` (`ANT_BUNDLE_CACHE_MIB`): an in-memory
  cache of served tree bundles.
  - Default 1024, 64 in local mode, 0 turns it off.
  - Entries expire like the other server caches.
  - Under a policy, a cached bundle is served only to callers who may read
    all of it.
  - A fleet checking out one commit has each bundle built once: 6.2 s for
    the first cold checkout above, 2.9 s for the later ones.
  - `/debug/stats` reports it (`bundle_cache`).
- Bundle builds that run at the same time share their store reads, so the
  reads land close together in S3 packs. A cold server is about 20% faster.
- `ant-bench netem` adds round-trip time and an optional bandwidth cap to a
  TCP relay; `ant-bench checkout --rtt-ms/--mbit` uses it. Together they
  emulate a WAN locally, without root.
- `scripts/checkout-bench.sh` measures the network checkout. It runs per
  build and per added RTT, optionally against a baseline build. Use it
  locally or via `scripts/remote-bench.sh --what checkout --extra-bins
  NAME=DIR`.

- **Local mode**: `ant serve --local [<dir>]` keeps refs, metadata and small
  objects in SQLite (WAL, one writer thread with group commit) and large
  objects in local pack files, all in one data directory (default
  `~/.local/share/antimony/local`): no Postgres, S3 or Docker. Low-resource
  defaults (≤ 4 async workers, 48 MiB server caches). `ant local
  start|stop|status` runs it in the background; `ant import`, `ant admin gc`
  and `ant admin schema` take `--local` too (offline: one process uses a
  directory at a time). New on-disk format, versioned
  (`docs/compatibility.md`); flags `--local-readers`,
  `--local-frame-cache-mib`, `--local-sync full|normal`, and for every
  server `--worker-threads` and `--cache-mib`. `/readyz` reports `"local"`.
- `ant admin copy-local --from <dir> <store flags>` copies a local data
  directory into other stores (Postgres + S3): objects, commits,
  changesets, derived data, then the refs the destination lacks.
- `ant serve` exits cleanly (status 0) on SIGTERM and SIGINT.
- `ant-bench fleet --server-pid <pid>` reports the server's RSS and CPU over
  the run; `scripts/scorecard-local.sh` runs the headline benchmarks in local
  mode. Tests that ran in memory take `ANT_TEST_BACKEND=local`.
- `scripts/landing-bench.sh` and `scripts/remote-bench.sh --what landing`
  (`--landing-args`, `--extra-bins`): interleaved A/B runs of the landing
  path and the import for several builds, Postgres options and server
  arguments, with WAL, commit, CPU and Postgres wait-event statistics per
  run. `scripts/dev-services.sh pg-restart` and `ANT_PG_OPTS`.

### Changed

- **Network checkout**: a 96k-file, 1.4 GiB checkout over Hetzner's private
  network now takes 2.9 s cold and 1.8 s warm, down from 14.4 s and 3.5 s.
  The client uses 13 CPU-s instead of 43. Adding 30 ms of RTT costs about
  0.1 s.
  - `Workspace::materialize` overlaps the tree walk, the fetches and the
    file writes. Subtrees are requested as soon as their parent is decoded,
    with no wait between levels. Files come straight from the downloaded
    tree bundles, without reading the cache back.
  - HTTP requests are bounded per client (64), across all of its
    concurrent calls.
- **Client disk cache**: batches of 64 or more objects (tree bundles,
  batched fetches) are stored as one pack plus an index under
  `<cache>/packs/`, instead of one compressed file per object.
  - Pack records are uncompressed: they use more disk and less CPU.
  - Eviction removes whole packs, least recently used first. `ant cache
    stats/gc` count the objects inside them.
  - The change is additive and the cache format stays 1. Older binaries
    ignore `packs/` and fetch what they miss
    (`docs/compatibility.md`).

- **Faster `git clone` from the git export.** A complete clone (no
  `have`s, not shallow) keeps its pack entries as the cached *base pack* of
  its filter (`blob:none` or none) in an unlinked temporary file; later
  clones send the base verbatim followed by exactly the objects their wants
  add beyond it (the pack holds the same objects as without the cache).
  Clones arriving while a base is built wait for it. A new base is built
  once clones reach more than 1/8 of its commits (plus 1,024) beyond it, or
  when a tip of the base is no longer reachable. Bases live in the server
  process only (no store or format change; at most one per filter). The
  export still admits only principals that can read everything, so a base
  never carries objects its requester could not get uncached (new ACL test).
  git/git (Postgres + S3, local, `--single-branch`): blobless clone 6.0-7.8 s
  -> 2.1 s (native `git daemon` 1.6 s), full clone 19-22 s -> 7.1 s (native
  6.3 s); a clone 2,000 first-parent commits past the base: 3.0 s / 10 s.
  Hetzner ccx43 + ccx33:
  blobless 29.5 -> 4.2 s (native 2.9 s), full 77.5 -> 11.4 s (native 8.9 s).
- Smaller and cheaper export packs: trees try 40 delta bases instead of 10
  (git/git blobless `.git` 92.4 -> 81.4 MB, native 61.1 MB; full 160 -> 149
  MB, native 112 MB; most of the rest is SHA-256 ids); entries are zlib-
  compressed through a small scratch buffer with the zlib-rs backend
  (flate2's `compress_vec` zero-filled the multi-MB segment buffer on every
  object: half the export's compression CPU); blobs are read 16 requests of
  64 at a time (was 8 of 16). First (uncached) clone: blobless 6.8 s, full
  12-17 s.
- `scripts/vs-git.sh`: clones use `--single-branch` on both sides (the export
  lists every ref of the store, `synth` and `storm*` included, which made
  antimony's clones 3-7x larger than the daemon's single-branch repository);
  `--phases "clone ..."` runs only the named timed phases and prepares only
  what they need.

### Performance

Landing and import on cloud boxes (Hetzner ccx43 + ccx33, same-box A/B):
- 64 agents: 2,335 → 3,231 lands/s, land p99 46 → 37 ms
- `create_changeset` p50 at 64 agents: 5.5 → 3.2 ms
- git/git import: 4,566 → 8,309 commits/s (peak RSS 0.88 → 1.03 GiB)

- Land worker:
  - It keeps the ref lock past a 20 ms turn while nobody waits for it
    (`RefLock::contended`: `pg_locks` on Postgres, the lock table in
    process), instead of draining its pipeline and re-locking every turn.
    Another replica or an import still gets the lock within a turn.
  - Batches go through three overlapping stages: in-memory replay, writes
    (on their own task), publish. Up to two batches are prepared behind the
    one publishing, each seeded with its predecessor's unwritten state. A
    publish starts as soon as its batch's writes are done, alongside the
    next batch's replay (see Fixed).
  - The batch-wide prefetch runs concurrently with the ref and mapping reads.
- Postgres:
  - A quarter of `--pg-max-connections` (from four up) forms a
    deferred-write lane: sessions with `synchronous_commit = off` that serve
    only the writes that are already deferred (a land batch's objects and
    commit records before its durable publish, import writes under a
    witness, history-index writes). One round trip instead of three, and no
    queueing behind requests that wait for a WAL flush. Land and
    `create_changeset` durability are unchanged.
  - Query-pool connections are pinged before reuse only after a second idle.
    sqlx pinged on every acquire: one extra round trip per statement.
- History-index catch-up walks recently landed history through the node
  and changeset caches instead of a recursive SQL chain query, and writes
  its rows and watermark with deferred durability. The index is derived
  data: rows commit before the watermark that covers them, so a crash loses
  at most a suffix, which the next catch-up redoes.
- A small-blob cache (8 MiB, blobs ≤ 64 KiB, taken from the tree cache's
  budget; `ant_cache_*{cache="blobs"}`): land text merges of files that
  agents keep editing no longer read them from the store.
- Import: the converter releases mapped pack pages and free heap
  (`malloc_trim`) every 250 ms instead of every 64 commits, where it took
  about 40 % of the serial conversion time.
- `deploy/postgres.conf`: `track_io_timing` and `track_wal_io_timing` on,
  notes for cloud disks. The operator guide gains "On cloud boxes".

### Fixed

- **Landing regression on boxes with slow commits** (introduced by the
  land pipeline above, never released). In steady state the land worker
  replayed the next batch *before* publishing the current one, instead of
  during the publish, so every publish waited for a replay, and each
  request waited behind two of those slower cycles. Where the durable
  publish is the bottleneck (a local disk: fdatasync about 0.8 ms on the
  WSL dev box, against 0.12 ms on Hetzner NVMe) batches shrank and lands
  slowed by a quarter or more. The worker now publishes as soon as the
  batch's writes are done, while the next batch is replayed beside it.
  Same-machine A/B (embedded fleet, Postgres + S3, three interleaved
  rounds, `0.2.2` / before the fix / after):
  - 16 agents: 2,296 / 1,739 / **2,472** lands/s, land p50 3.9 / 6.6 /
    3.9 ms, p99 10.3 / 12.3 / 8.0 ms
  - 64 agents: 3,518 / 2,933 / **4,199** lands/s, land p50 11.9 / 16.0 /
    9.3 ms, p99 30.1 / 54.9 / 30.6 ms

  Runs that degraded across rounds (old and new alike) were the dev box's
  disk: after a few minutes of back-to-back runs fdatasync slowed from
  0.8 to 3 ms. Interleaved A/B runs now need idle gaps between them.
- `history_bench_postgres` (benchmark test) no longer races the
  background catch-up its unindexed query starts: it waits for it and
  clears the index before timing a catch-up of the whole history.

## [0.2.2] - 2026-10-16

Patch release: no storage or protocol changes (the shared disk cache gains
an additive `batches/` directory; its format version stays 1).

### Fixed

- Processes sharing a cache could fetch the same objects twice when they
  started at nearly the same moment (`cache_tier
  concurrent_processes_share_fetches` failed about 3 runs in 100): since the
  0.2.0 cold-checkout fix a process alone on its cache fetches without
  per-object claims, and a peer arriving during such a batch did not know
  what it was fetching. A process alone now announces each batch with one
  file under `<cache>/batches/` (its ids, locked while the fetch runs) before
  it checks for peers, so of two simultaneous starters one always sees the
  other; a peer waits for an announced batch instead of fetching its ids
  (same 30 s bound and fallback as a claim). Separately, a tree bundle's own
  tree is now stored after the rest of the bundle: a peer that found it in
  the cache walked on below it and re-fetched subtrees whose files were
  still being written. The lone process keeps its fast path: one small file
  per batch, no per-object claim files.
- `ant admin repack` could hang (until the S3 operation timeout, then
  fail): while it waited for pack uploads it stopped polling the segments
  it was reading, and a paused read keeps the S3 request slots it holds
  (`--s3-max-concurrency`), which the uploads it waited for needed. It now
  keeps every running read going and applies backpressure by not starting
  new segments while uploads are behind.
- Tests: S3 test prefixes get a random suffix
  (`<label>-t<secs>-<random>`); two tests starting in the same second
  shared a prefix and one's cleanup deleted the other's data.

## [0.2.1] - 2026-10-16

Patch release: no storage or protocol changes; servers and clients of 0.2.0 interoperate. Agents need this client to reach a server behind an `https://` proxy.

### Added

- Clients accept `https://` gRPC server URLs (`ANT_SERVER=https://ant.example.com`),
  verifying the TLS proxy in front of `ant serve` against the system roots
  (`SSL_CERT_FILE` for a private CA). Before, an `https://` URL was spoken as
  cleartext HTTP/2 and failed with `operation was canceled`.
- `deploy/`: production single-host kit (compose with Caddy TLS, Postgres 17,
  SeaweedFS), nightly backup and restore procedure, git mirror cron job.

### Changed

- Tests no longer leave Postgres schemas or S3 data behind; killed runs are
  swept (`scripts/clean-test-data.sh`).

### Performance

- Cold checkouts are no longer slowed down by cross-process fetch claims
  when nothing else uses the cache. Since the cache tier (0.1.0) every
  missing object and bundled tree was claimed with its own lock file (create,
  `flock`, stat, unlink: about 217k file operations for the synthetic
  96k-file tree, on the fetch path), which made that cold checkout about 2 s
  slower on ext4. Each `ReadThrough` over a disk cache now registers itself
  under `<cache>/live/` (a file it holds an exclusive `flock` on while it
  lives) and claims objects only while another live handle, or a claim file
  of an older binary, is present: checked once per fetch batch. Dedup between
  concurrent processes is unchanged (8 cold clients, 48k files: 75k objects
  and 2.35k requests reach the server either way, 599k / 18.6k without
  dedup); a process that arrives while a batch is already being fetched
  without claims may fetch some of that batch's objects again, once.
  Synthetic 96k-file cold checkout, same machine and hour, ext4: median
  5.4-6.5 s -> 3.2-3.3 s (tmpfs: 2.3 -> 1.6 s); warm and status unchanged.
  Dead processes' registrations are never counted (nobody holds their lock)
  and are removed by the cache's sweep.

### Fixed

- `ant-bench checkout` flushed dirty pages between phases with a global
  `sync()`, which waits on every mounted filesystem and hung forever on a
  machine with an unresponsive FUSE mount; it now uses `syncfs` on the
  cache's filesystem. `--no-dedup` now applies to the normal phases too, and
  each phase reports its kernel CPU time (`cpu_sys_s`).

## [0.2.0] - 2026-10-16

Storage format change (schema version 7): read docs/operations.md, "Upgrading", before upgrading from 0.1.x. Final scorecard: 27/27; upgrade test from v0.1.0 on git/git: 90/90.

Known issue: the cold checkout of the synthetic 96k-file tree measured 4.7 s (target 5 s; 2.5 s in the 0.1.0 scorecard); to be investigated for 0.2.1 (cause: per-object cross-process claim files, see Unreleased).

### Added

- Batch read-permission check: `POST /o/check` (up to 4096 raw object ids in,
  one flag byte per id out: readable / private) answers exactly what
  `GET /o/<id>` would for the caller, sharing its authorization code, and
  reports a hidden object like a missing one. Additive; metrics route
  `object_checks`. `ant cache-serve --auth forward` uses it: concurrent hits
  of one principal are batched into one request (`--authz-window-ms`, default
  2), allowed decisions are reused for `--authz-ttl-secs` (default 30; 0 =
  always ask; the TTL is the window in which a revoked principal can still
  read objects the cache holds), and tree bundles seen `public` are rebuilt
  from disk after one batched check instead of being forwarded. Against an
  origin without the endpoint (404/405) it falls back to conditional
  `GET /o/<id>` as before. `ant_cacheserve_authz_total{result}` metric;
  `ant-bench checkout --cache-tier-forward [--cache-tier-no-batch]`.
- `scripts/upgrade-test.sh`: the upgrade from a released version (default:
  the previous tag, built in a temporary worktree) to the checkout on real
  Postgres + S3, in place and rolling, with byte-identical reads, `git fsck`
  of the export, client skew both ways, `ant admin gc` and the refused
  downgrade; run before every release. Run from 0.1.0
  with git/git as extra corpus: every check passes.
- `ant admin repack` (`ant_store::postgres::repack`): git-gc-style repacking
  of Postgres + S3 stores. Walks the history from every ref (newest first,
  noting each object's path) and rewrites cold objects, small Postgres
  -resident ones included, as *delta frames* (new S3 pack record kind,
  magic `ADF1`): versions of a path, newest first, stored as git delta
  instructions against the best of the previous `--window` objects (10),
  chains at most `--depth` deep (16), the record (`--record-bytes`, 2 MiB of
  stream) compressed whole at zstd `--level` 19. Delta bases always live in
  the same record, so records still decode on their own and collecting a
  base never breaks the objects built on it. Objects reachable from the
  newest `--keep-recent` (100) first-parent commits of every ref stay one
  decode away (Postgres rows, or plain frames). Index rows switch to the new
  packs in short transactions that keep write times and skip condemned or
  locked rows; old packs are freed by `ant admin gc`, which keeps a pack for
  its quiesce period after a repack moved rows out of it. Safe beside
  serving, landing, imports and collections; resumable (`--max-packs`, or
  any interruption); `--dry-run` reports the savings; `--min-age` (1d),
  `--all` (rewrite earlier repacks too), `--no-small`, `--jobs`. Output
  contract in `ant-cli/src/main.rs`. Every record is decoded and hashed
  before it is uploaded. `ant_core::delta` (git delta instructions) moved
  there from `ant-gitserve`.

### Changed

- **Storage format (schema version 6, migration `pack_frames`).** Large
  objects of one write that look alike (versions of one file or directory)
  are compressed together as multi-object frames in S3 packs (magic `AFR1`,
  at most 2 MiB uncompressed each; `ant_store::packed`). Packs written before
  stay readable, and objects without a look-alike are still written in the
  old single-object format. Servers that support only schema version 5
  cannot read frames and refuse the migrated schema; for a rolling upgrade
  start new servers (and `ant import`) with `--pack-frames false`
  (`ANT_PACK_FRAMES`) until every server runs this release. The first new
  server migrates the schema whatever the flag: 0.1.x servers already
  running keep working, but none can be started again, and going back to
  0.1.x means restoring the backup (`docs/operations.md`, Upgrading). The
  migration adds a nullable column (`object_index.share`, a frame member's
  share of the frame's bytes for `ant admin gc` byte counts) and rewrites
  nothing. It ships in a minor release (0.2.0), as `docs/compatibility.md`
  requires for a migration older servers cannot run alongside.
- **Schema version 7 (migration `repack`).** Adds the `packs` table:
  `ant admin repack` records the packs it wrote (`repacked_at`, `hot`) and
  the packs it moved index rows out of (`moved_at`), which `ant admin gc`
  keeps for its quiesce period after the move. Additive (a new table, no
  rewrite). Packs may now also hold delta frames (`ADF1`), written only by
  `ant admin repack`: run it only once every server reads them (this
  release or later); servers supporting only version 6 refuse the migrated
  schema.

### Performance

- Blame is incremental: its value is keyed by the last commit that changed
  the path (derived kind `blame/v2`, replacing the per-revision `blame/v1`
  cache, whose rows are no longer read and are still collected by GC), so
  every revision since shares it, and a new change is derived from the
  previous version's stored value plus one line diff instead of walking the
  history again; output is byte-identical (property-tested against the
  previous walk on random histories with merges). Postgres + S3, 1,700-line
  file with 1,200 versions: blame after landing 1/10/100 commits elsewhere
  95-168 ms -> 3.3-3.9 ms; after a commit changing the file 93 ms -> 12 ms
  (165 ms -> 8 ms while the history index lags the ref); first blame
  unchanged (~90 ms); repeated blame 2.3 -> 3.3 ms. A typed derived-data
  layer, `ant_store::derive` (`Derivation`: versioned kind, key, codec,
  recursive rule, eager/deferred policy; batched get/put), carries it.
- Path history (`ant log <ref> --path`) is served from the ref's history
  index right after an import: `ant import` and `ant-bench import` build it
  (git/git, 24k first-parent commits: 1.3 s), and a path query or blame on
  a ref the index cannot serve (no index yet, e.g. a ref set straight in
  the store; lag past 4096 commits; a non-fast-forward move) schedules its
  catch-up, so only the first queries scan. Before, an imported ref was
  never indexed until something landed on it, and every path log scanned
  the whole first-parent history. A path scan's chain/changeset reads now
  double from 256 up to 4096 commits (git/git: 95 round-trip pairs -> 10),
  and a Postgres server analyzes the `history` table after writing 50k
  index rows (a freshly built index otherwise looks empty to the planner
  until autovacuum runs, and path queries filter every row of the ref:
  14 ms instead of 1 ms on git/git). git/git on Postgres + S3, 6 paths,
  warm server, CLI wall time: whole history 210 ms -> 6.7 ms (git
  `log --first-parent` with commit-graph Bloom filters: 101 ms), newest 20
  23.4 ms -> 5.3 ms (git 11.7 ms).
- `merge-base` of a branch far off a long first-parent chain is decided
  from the first parents instead of a walk over everything newer than the
  branch point: the skip index finds where the two first-parent chains
  meet (both descend in lockstep, O(log n) entry reads); the branch side's
  ancestry above that commit is read (at most 1024 commits), and its
  commits are shown not to be ancestors of the other side because they
  were recorded after it (`MetaStore::log_seqs`: the commit log is a
  topological order), or both sides' short ancestries are intersected.
  Anything else walks as before; answers are unchanged (oracle property
  tests on random diverged histories, in memory and on Postgres). git/git,
  a commit on `main~12198` against `main`: 193 ms -> 9.3 ms (git 54 ms); a
  3-commit branch off `main~2000` on a 342k-commit merge-heavy synthetic
  history reads 4 store calls / 70 commits.
- `ant import` cuts object batches at 64 MiB instead of 16 MiB (still at
  most 1024 objects), so more versions of a path share a batch and a frame.
- Stored size: a git/git import (82k commits; git pack of what `master`
  reaches 303 MiB) takes 901 MiB (Postgres 246 + S3 655) instead of
  4,094 MiB (243 + 3,851), 3.0x the git pack instead of 13.5x. The import is
  no slower (S3 receives 6x fewer bytes); blobless clone 7.4 -> 5.6 s, full
  clone 22.7 -> 16.8 s (both cold, server CPU 30 -> 21 s and 77 -> 60 s),
  cold checkout unchanged (0.33 -> 0.31 s). Reads decode at most one 2 MiB
  frame per record (no delta chains), with a 64 MiB decoded-frame cache and
  a 64 MiB bound on frames being decoded per server.

### Fixed

- S3 throttling no longer fails `ant admin gc`, clones and reads. Transient
  S3 failures (429, 503 `SlowDown`, 5xx, connection resets, timeouts, bodies
  cut off mid-stream, as SeaweedFS does above its in-flight download limit)
  were retried 5 times within about one second, so a short burst of
  throttling failed a whole `ant admin gc` mark (`gave up after 5
  attempt(s) in 1.05s: reading body: streaming error`). They are now retried
  with exponential backoff and full jitter (pauses up to 5 s, `Retry-After`
  honoured) for a time budget, `--s3-retry-budget` (`ANT_S3_RETRY_BUDGET`,
  default 60 s, counted from the first failure); `--s3-max-attempts` now
  defaults to 0 (no attempt limit). Client errors and corrupt data still fail
  at once. A process keeps at most `--s3-max-concurrency`
  (`ANT_S3_MAX_CONCURRENCY`, default 32) S3 requests in flight, so bursts
  (GC mark, repack, clone planning, checkout bundles) queue instead of
  overrunning the store. New metrics `ant_s3_retries_total{reason}`,
  `ant_s3_gave_up_total`, `ant_s3_requests_in_flight`,
  `ant_s3_requests_queued`, `ant_s3_max_concurrency`. Busy dev SeaweedFS
  (two servers, 16 concurrent git/git clones and two `gc --dry-run`): 429s
  749 -> 157, retried requests 564 -> 84, no failures, same throughput.

## [0.1.0] - 2026-10-16

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
  limits are in
  [docs/access-control.md](docs/access-control.md) (in the README at 0.1.0).
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

### Queries, events, cache tier and overload protection

- Cache tier (crate `ant-cache`): `ant cache-serve --cache <dir> --upstream
  <http-url>` serves the immutable `GET /o/<id>` and `GET /t/<tree>` surface
  from a local disk cache (client cache format, same budget, eviction and
  `ant cache stats/gc`), filling misses from the origin or another
  cache-serve with hash verification and in-flight dedup. `--auth forward`
  (default) never answers on its own authority: every request is authorized
  by the upstream with the caller's own credentials (conditional `GET` ->
  `304`), so it cannot serve what the origin would refuse; `--auth open` is
  for upstreams without a policy and is refused at startup against one.
  `ant_cacheserve_*` Prometheus metrics (hit/revalidated/miss/joined/...
  per route, upstream requests and bytes, disk gauges).
- Processes sharing a client cache directory fetch each missing object or
  tree bundle once (`flock` claim files under `<cache>/claims`, crash-safe,
  additive: the cache format version stays 1); `ANT_CACHE_DEDUP=0` disables
  it. 8 concurrent cold checkouts: 599k objects received -> 75k (the
  distinct ones), 18.6k HTTP requests -> 2.4k.
- `ant cache seed <rev>[:<path>] [--sparse <pattern>]...` pre-fetches a tree
  into the cache through tree bundles, without writing files;
  `ant cache stats --tiers` adds per-tier counters (local, waited-for-peer,
  joined, bundled, fetched) recorded in `<cache>/gc/tiers`.
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

- `ant ref set` checks fast-forwards with one `IsAncestor` call instead of
  paging through `Log` (still the fallback against older servers).

### Performance

From the benchmark scorecards (WSL2 on one development machine, one
Postgres and SeaweedFS S3 on the same host; numbers are indicative, not
guarantees) and the roadmap status:

| measure | result |
|---|---|
| Landing, 64 agents, Postgres + S3 | 4,138 lands/s, p99 26.5 ms, 0 aborts, exact history (final scorecard; earlier runs 2,450 to 4,276/s) |
| Landing, 16 agents | 2,230 to 2,650 lands/s, p99 about 7 ms |
| `create_changeset` p50, 16 agents | 1.9 to 2.0 ms |
| Import of git/git (82k commits) | 6,400 to 6,800 commits/s, 0.8 to 0.9 GiB peak RSS |
| Import of rust-lang/rust (343k commits, 1.1 GB pack) | 110 s (3,124 commits/s), 1.6 GiB peak RSS; log 11 ms, path log 18 ms, blame 363 ms first / 10 ms cached, merge-base 10 ms |
| Linux kernel import (partial runs) | 1.0 to 1.5k commits/s, flat memory, about 2 GiB RSS |
| Soak | 4 replicas x 1,000 agents x 30 min: 266k lands, 0 errors, exact history, flat RSS |
| Checkout, synthetic 96k files (medians) | cold 2.5 s, warm 1.1 s, status 0.1 s |
| Queries on kernel-size history | log 6 to 40 ms, path log 17 to 36 ms, blame 55 ms cached (first blame 0.6 s on git/git) |
| Git export | git/git: `--depth 1` 0.7 s, `blob:none` clone 88 MiB in 7.0 s (1.2x native git); rust-lang/rust `blob:none` 33 s (native 30 s) |
| Lazy workspace writes | 72% of direct write speed, 7,700 small files/s, `cargo build` 1.27x |

The final pre-release scorecard passes all 27 rows.

Also in this release:

- Ancestry walks (`graph::merge_base`, `graph::is_ancestor`) read whole
  windows of ancestry per store call (`MetaStore::ancestors_above`, one
  recursive query on Postgres) and use a first-parent skip index (derived
  kind `graph/skip/v1`, filled lazily and by the importer, collected by GC);
  `rev~N` resolves in O(log N) lookups. On rust-lang/rust (343k commits),
  `merge-base main main~2000` went from 170 s to 10 ms, and `ref set`
  fast-forward checks across 40k merges take about 12 ms.
- Postgres store connections run with `jit = off`. The planner cannot size
  the recursive ancestry walk and estimates it far above the JIT thresholds,
  so servers built with LLVM (such as the official Docker image) compiled
  every walk first: about 80 ms per call instead of 0.5 ms.
- First (uncached) `blame` of a long-lived file is 10-25x faster, with
  byte-identical output: changes between versions are diffed on the changed
  region only (bytewise prefix/suffix, integer line ids, bit-parallel LCS
  with the previous tie-break), version diffs run in parallel on blocking
  threads, and blob reads are prefetched a few batches ahead of the walk.

### Known limitations

- **Restricted structure is visible.** Trees readable by a principal list the
  names and ids of restricted entries, and commit metadata (messages,
  authors, ids, parents) is visible to every authenticated principal.
  Response timing still differs slightly between missing and hidden objects.
  Tokens are bearer secrets: run behind TLS. See the threat model
  ([docs/access-control.md](docs/access-control.md)).
- **Ancestry through merged-in branches is a walk.** Reachability that only
  goes through second parents (a topic commit merged long ago, or a negative
  answer far back) still reads every commit above the target's generation,
  in a few batched calls (0.2 s on rust-lang/rust). The first ancestry query
  on a repo imported before this release fills the skip index once (about
  0.4 s on rust-lang/rust).
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

[Unreleased]: https://github.com/Mapika/antimony/compare/v0.3.2...HEAD
[0.3.2]: https://github.com/Mapika/antimony/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/Mapika/antimony/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/Mapika/antimony/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/Mapika/antimony/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Mapika/antimony/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Mapika/antimony/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Mapika/antimony/releases/tag/v0.1.0
