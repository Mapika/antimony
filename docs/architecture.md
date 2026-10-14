# antimony architecture

antimony keeps git's object model and replaces git's operating model. Clients
are always online, history and writes live on the server, and nothing is
pushed. This document describes how the pieces fit; the code's module docs
hold the detailed contracts.

## Object model (`ant-core`)

Objects are git objects (`blob`, `tree`, `commit`) in git's encoding, hashed
with **SHA-256**, so ids equal those of a `--object-format=sha256` git
repository. Decoding is canonical-only: sorted tree entries, standard modes,
plain decimal lengths. Two different byte strings can therefore never decode
to the same object. Commit text (names, emails, messages, extra headers) is
raw bytes, so real-world histories round-trip byte for byte.

## Storage (`ant-store`)

Three traits, each with in-memory, Postgres and conformance-test
implementations:

| store | contents | Postgres tables |
|---|---|---|
| `ObjectStore` | immutable objects by id | `objects` (small objects, zstd) + S3 for large ones |
| `RefStore` | mutable named pointers, updated only by compare-and-swap | `refs` |
| `MetaStore` | commit graph (generation numbers), per-commit changesets, the commit log, derived data, the per-ref history index | `commits`, `changes`, `log`, `derived`, `history`, `history_watermark` |

`TieredObjectStore` sends objects ≤ 4 KiB (encoded) to Postgres and larger ones
to S3, so the many tiny trees and files avoid S3's per-request latency. Batch
writes are key-ordered so concurrent writers never deadlock. There are no
packfiles and no repository maintenance: objects are written once and never
rewritten.

## Write path (`ant-server::ops`)

**CreateChangeset.** A client sends file edits (write/delete with inline
content or a previously uploaded blob id) against a base commit. The server
loads only the directories on the edited paths, one batched read per tree
level, rebuilds those trees, and records the commit together with its
changeset. The changeset comes directly from the applied edits, with no tree
re-read. Cost scales with the size of the change, not of the repository.

**Land.** Landing advances a ref to include a commit, rebasing server-side
when the ref has moved:

1. Each request is permission-checked first (`land/verify.rs`), then joins a
   per-ref queue (`land/queue.rs`).
2. One leader per ref drains up to 64 queued requests and replays them in
   arrival order against an in-memory evolving tip (`land/replay.rs`).
   Already-landed commits and fast-forwards are recognised, unlanded stacks
   are replayed bottom-up, files changed on both sides get a 3-way text
   merge, and genuine conflicts are reported per request without affecting
   the rest of the batch.
3. Every request gets its own tree and commit, so history stays linear. The
   batch is written with one `put_many` and one `put_commits`, then
   published: one ref compare-and-swap plus the `landed-as` mappings that
   make retries idempotent.
4. Batches are pipelined: while one batch publishes, the next is replayed
   and written against its tentative tip. The speculative batch publishes
   only after its predecessor (its CAS expects the predecessor's tip) and is
   redone from the real tip if the predecessor's publish mismatches or fails.

**Durability of a land (Postgres).** The leader holds the ref's advisory
lock on a dedicated session. The batch's objects, changesets and commit
records are written with `synchronous_commit = off`; the publish (ref CAS
plus mappings, one statement) is a synchronous commit *on the lock's
session*. That gives one WAL flush per batch instead of four, and stays
crash-safe: writes acknowledged before the publish precede it in the WAL,
so its flush makes them durable too; and losing unflushed WAL takes a
database crash or restart, which ends every session, so a publish that
succeeds proves no such loss happened since the lock was taken. If the
session died, the publish fails, the batch is aborted, and no ref ever
points at lost objects. Without the lock (in-memory stores, lock
unavailable) the writes, CAS and mappings are each durable on their own.

`batch_equivalence` property tests prove that a batch gives exactly the same
trees, commits and conflicts as landing the same requests one at a time.
The ref compare-and-swap remains the correctness guarantee across processes.

**Caches** (`ops/cache.rs`) hold only immutable data (trees, commit objects,
recorded commit nodes, changesets) that has been confirmed by the store, so a
cache hit can never hide a missing object.

## Read path

- **Queries without a checkout:** `ResolveRev`, `ReadPath`, `Log`, `Diff`,
  `Blame` over gRPC.
- **Path history:** each ref has an index (`history` table) of which commits
  touched which paths along its first-parent chain. It is maintained in the
  background after lands (`ops/history.rs`, coalesced per ref) and makes
  `log --path` and blame independent of history length. On a busy ref the
  index trails the tip by the commits landed since the last catch-up; a path
  log scans only that lag and takes the rest from the index.
- **Diff:** tree diffs walk both trees breadth first with one batched read
  per depth, through the tree cache, so a `Diff` spanning hundreds of lands
  costs a few round trips, not one per changed directory.
- **Objects:** batched over gRPC (`GetObjects`), or as plain immutable HTTP
  (`GET /o/<id>`, `Cache-Control: immutable`), so any HTTP cache or CDN can
  serve as the regional cache tier.

## Clients (`antd`, `ant-fuse`, `ant-cli`)

- **Shared disk cache:** one content-addressed store per machine
  (`antd::disk`), safe for many concurrent processes (write-temp-then-rename),
  with LRU eviction. A read-through layer fetches misses from the server in
  batches, de-duplicates in-flight fetches and verifies hashes.
- **Sparse workspaces:** real files on disk for the paths matching sparse
  patterns. `status` compares against an index of stat data, so only changed
  files are re-hashed.
- **Lazy workspaces:** a FUSE filesystem shows the whole tree instantly and
  fetches content on first read; writes go to a copy-on-write upper layer. No
  root privileges or kernel overlayfs are needed, and `status` cost is
  proportional to the number of changes.
- **Updating a workspace** (`ant ws update`): moves either kind of workspace
  to a new base and carries local edits over, with the same 3-way rules and
  text merge (`ant_core::merge`) the server uses when landing; overlapping
  edits are written with conflict markers and must be marked resolved
  (`ant ws resolve`) before `submit`. The cost follows the tree diff between
  the two bases plus the local edits. In lazy workspaces the switch is
  crash-atomic: merged files are staged, one journal record commits base,
  whiteouts and edits together, and recovery rolls it forward.
- **CLI:** `ant` covers queries, workspaces, submit/land, import, admin, and
  `ant serve` (the server is the same binary).

## Git interop (`ant-gitbridge`, `ant-gitserve`)

- **Import:** converts any git repository (SHA-1 or SHA-256) to antimony.
  Ids match git's own SHA-256 conversion, re-imports are incremental, and
  throughput is about 1,100 commits/s on git/git.
- **Export:** read-only git smart HTTP (protocol v2) at `/repo.git` on the
  HTTP port. Full, shallow and `--filter=blob:none` clones and incremental
  fetch are supported, with packs delta-compressed against the previous
  version of each path.

## Observability (`ant-server::metrics`, `ant-server::observe`)

One `Metrics` value (a `prometheus-client` registry, no global) is created in
`serve::run` and shared via `Arc`. Transports are observed by wrapping whole
services, so handlers stay untouched: a tower layer around the gRPC service
and an axum middleware around the HTTP router assign the request id (incoming
`x-request-id` or a ULID, a tracing span field, echoed on the response),
record `ant_rpc_*` / `ant_http_*` and log slow requests. Stores are wrapped
once by `Metrics::instrument` (per-op timing and error counts). Landing and
history report through `Option<Arc<Metrics>>` parameters (`land_observed`,
`catch_up_observed`); state that lives in process-wide statics (cache sizes
and lookup counters, land queues) is read at scrape time. See
`operations.md` for the exposed series.

## Access control (`ant-auth`, `ant-server::acl`)

A TOML policy defines principals with hashed bearer tokens, longest-prefix
path rules (read/write), ref rules (update/land) and admins.

- **Objects are protected by id, not only by path:** each object is tagged
  with the *zones* (path rules) it was written under, and an unreadable object
  looks exactly like a missing one.
- **Writes are checked per path:** for changesets, for every commit a land
  replays, and for ref moves.

See the README's threat model for the v1 limitations.
