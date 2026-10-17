# antimony architecture

antimony keeps git's object model and replaces git's operating model. Clients
are always online, history and writes live on the server, and nothing is
pushed. This document describes how the pieces fit; the code's module docs
hold the detailed contracts.

## Crates

| crate | role |
|---|---|
| `ant-core` | object model: SHA-256 ids, blobs/trees/commits in git format, repo paths, 3-way merge rules (`merge`, shared by land and `ws update`) |
| `ant-store` | `ObjectStore`/`RefStore`/`MetaStore` traits; in-memory, Postgres, S3 and tiered implementations; conformance suites; `graph` primitives (tree diff, commit recording, ancestry) |
| `ant-proto` | gRPC definitions (`proto/ant.proto`) and core ↔ protobuf conversions |
| `ant-auth` | access policy: tokens, principals, path and ref rules |
| `ant-server` | `ops` (changeset, land, query, history), `acl`, gRPC (`rpc`), HTTP object endpoint (`http`), `serve` (startup shared with `ant serve`) |
| `ant-gitserve` | read-only git smart-HTTP (protocol v2) export |
| `ant-gitbridge` | git → antimony import |
| `antd` | client engine: shared disk cache, read-through, remote store, sparse workspaces, workspace update planning (`update`) |
| `ant-cache` | cache tier (`ant cache-serve`): `/o/` and `/t/` from a budgeted disk cache, forward-auth or open, bundle index, metrics |
| `ant-fuse` | lazy FUSE views: read-only (`filesystem`) and copy-on-write workspaces (`rw`) |
| `ant-cli` | the `ant` command (client and `ant serve`) |
| `ant-bench` | load and scale harness (`import`, `fleet`, `checkout`, `soak`) |

## Object model (`ant-core`)

Objects are git objects (`blob`, `tree`, `commit`) in git's encoding, hashed
with **SHA-256**, so ids equal those of a `--object-format=sha256` git
repository. Decoding is canonical-only: sorted tree entries, mode text that
maps one-to-one to a mode, plain decimal lengths. Two different byte strings
can therefore never decode to the same object. Commit text (names, emails,
messages, extra headers) is raw bytes, so real-world histories round-trip byte
for byte: the import copies a commit's text verbatim and replaces only the
`tree` and `parent` ids, like git's conversion (odd signature lines such as
`Name  <e> 1 -0000` or `+05` zones, header quirks and messages without a
trailing newline included). Stored commits are therefore read leniently
(`Commit::decode_lenient`); strict `decode` still gates new objects.

Old histories contain non-canonical tree entry modes (`100664`, `100640`,
zero-padded `040000`, `40755`, ...). git's SHA-256 conversion copies mode text
verbatim, so to keep ids equal we do too: `EntryMode::Legacy` stores the exact
spelling (any 1-10 octal digits other than the five standard spellings; one
representation per string, so decoding stays injective). Behaviour always goes
through `EntryMode::class()` (git's `canon_mode`: regular file, executable if
the owner-execute bit is set, symlink, directory, otherwise gitlink); the
protobuf API, recorded changesets, workspaces and the CLI only ever show the
class. Untouched entries keep their spelling when a tree is rewritten; entries
an operation writes get canonical modes, as when git rewrites a tree.
Antimony itself never creates a legacy mode.

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
writes are key-ordered so concurrent writers never deadlock. Writers never
rewrite objects. Within one write, large objects that look alike (versions
of one file or directory, found by a MinHash of entry names or lines) are
compressed together as one zstd frame of at most 2 MiB inside an S3 pack,
which plays the role of git's deltas without base chains: every frame
decodes from its own bytes, so a read is one ranged GET plus at most one
2 MiB decompression, and collecting some members of a frame never breaks
the others.

**Repacking** (`ant admin repack`, `ant_store::postgres::repack`) is the
offline counterpart, like `git gc`: it walks the whole history (newest
first, noting each object's path), and rewrites cold objects, Postgres
-resident ones included, as *delta frames*: versions of one path, ordered
newest first, each stored as git delta instructions against the best of
the previous few (window 10, chains at most 16 deep), the record (at most
2 MiB of stream) compressed whole with zstd 19. Delta bases always live in
the same record, so a delta frame too decodes from its own bytes and a
read stays one ranged GET plus one bounded decompression; readers keep
rebuilt bases while the frame is cached. Objects reachable from the newest
100 first-parent commits of every ref stay one decode away (plain frames,
or Postgres rows). Index rows switch to the new packs in short
transactions that keep each row's write time and skip condemned or locked
rows; old packs are kept for the collector's quiesce period after the move
and then deleted by it. git/git (82k commits): 901 MiB stored → 361 MiB (238 MiB
after `VACUUM FULL`: Postgres 153, S3 84), against a 298 MiB `git gc` pack and 94 MiB
`git gc --aggressive`; tip checkouts, blame and clones from the export are as
fast or faster, a cold checkout of a 2015 revision takes 0.33 s instead of
0.24 s.

**Garbage collection** (`ant admin gc`, `ant_store::postgres::gc`) deletes
data unreachable from refs, the history index, landed-as and import
mappings, and everything written within a grace period (rows carry their
write time). It runs beside live servers: unreachable rows are first
*condemned* (readers report them missing), then, after a wait longer than
any server's cache lifetime and operation, marked again from everything
written meanwhile, and only then deleted. Storing an object that already
exists refreshes an old write time, and the delete re-checks it under the
row lock, so a writer's deduplicated `put_many` never loses its objects.
S3 packs get random ids and are deleted once no index row points into
them.

### Local mode (`ant_store::local`)

`ant serve --local <dir>` keeps every store in one data directory, for a
single machine running tens of agents without Postgres, S3 or Docker:

| store | local-mode home |
|---|---|
| `ObjectStore` | objects ≤ 4 KiB as zstd rows of SQLite `objects`; larger ones in append-only pack files (`packs/<id>.pack`, same `AFR1`/single-object records as S3 packs) indexed by SQLite `object_index` |
| `RefStore` | SQLite `refs`; compare-and-swap inside one write transaction; in-process per-ref locks whose `publish` moves the ref and writes the landed-as rows in one durable transaction |
| `MetaStore` | SQLite `commits` (node + log sequence), `changesets`, `derived`, `history`, `history_watermark` |

- **One writer, many readers.** SQLite runs in WAL mode. Every write is a
  job for one writer thread, which runs whatever is queued (up to 256 jobs)
  as one transaction, each job in its own savepoint (a failing job rolls back
  alone): group commit across all callers, lands included. Reads use a small
  pool of reader connections (default 4) on the blocking pool; WAL readers
  never wait for the writer. Graph walks (`ancestors_above`,
  `first_parent_chain`) are point lookups inside one reader call: without
  network round trips, no recursive SQL is needed.
- **Durability.** A group commits with `synchronous = FULL` (WAL fsynced
  before anyone is answered) unless all its writes were made under
  `deferred_durability`, which commit with `NORMAL`. The WAL is appended in
  commit order, so a land's publish (FULL) makes its deferred objects,
  commits and changesets durable first, the same argument as Postgres's
  `synchronous_commit = off`: unflushed commits are lost only by an OS crash,
  which also ends the process holding the ref lock. The fsync is pipelined:
  commits run with `synchronous = NORMAL` and a second thread fsyncs the
  WAL and only then answers durable writes (and announces ref moves), while
  the writer runs the next group; land publishes are answered after that
  fsync while the land queue already publishes its next batch. Only commit
  records, ref moves, land publishes and history watermarks wait for an
  fsync; objects, derived rows and history rows become durable with the
  next one, which always precedes anything that references them. Pack bytes are fsynced
  (shared `fdatasync`s across concurrent writers) before their index rows
  are written. `--local-sync normal` trades power-loss durability for speed.
- **Ref events** go on the in-process feed after the transaction commits;
  there are no other processes to notify.
- **One process per directory** (a lock on `<dir>/LOCK`): the server, or an
  offline tool (`ant import`, `ant admin gc`). Maintenance of a directory a
  server uses runs inside that server: `ant admin gc|repack --local` ask
  it over a Unix socket in the directory (`admin.sock`), and
  `--gc-interval` schedules collections. Garbage collection follows the
  Postgres collector's protocol (mark, condemn, quiesce, re-mark, sweep)
  with the condemned set in memory: readers hide condemned rows older than
  the cutoff, and every sweep batch runs on the writer thread, re-checking
  write times and ref targets in its own transaction. Packs without live
  records are deleted and mostly-dead ones compacted (live records copied
  to a new pack, index rows moved durably, then the old file deleted; a
  reader that looked up an old location retries). `ant admin repack
  --local` runs the Postgres + S3 repack engine (delta frames across
  history) on SQLite rows and local pack files.
- **Small footprint.** A local server runs min(4, CPUs) async workers, at
  most 64 blocking threads, a 48 MiB server cache budget, 8 MiB SQLite page
  cache per connection and a 16 MiB decoded-frame cache; nothing polls while
  idle.

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

### Ref events (`ops::watch`, `ant_store::feed`)

Stores announce committed ref moves on an in-process feed: the in-memory
store from `cas_ref`, Postgres after the commit, in-process and with a
`pg_notify` sent by a notifier task in a separate non-durable transaction
(a notify inside the move's transaction serializes all ref moves' commits,
WAL flush included), relayed by a per-replica `LISTEN` connection. Notices are hints: per backend and watched ref,
one hub task applies a notice only when its old tip is the hub's tip,
skips duplicates by nonce, and otherwise re-reads the ref and emits one
coalesced event, so `WatchRef` streams stay gapless across replicas and
listener reconnects. Each event is built once (first-parent range walk)
and shared through a bounded ring; a watcher that falls out of the ring
gets one coalesced event instead of unbounded buffering.

## Read path

- **Queries without a checkout:** `ResolveRev`, `ReadPath`, `Log`, `Diff`
  (path prefixes, merge-base diffs, server-side patches), `Blame`,
  `MergeBase`, `IsAncestor`, `LogRange` and `LandedAs` over gRPC
  (`ops/query.rs`, `ops/ancestry.rs`, `ops/patch.rs`). Ancestry walks go in
  generation order (`graph.rs`): walks over the whole DAG (merge base,
  reachability) read the ancestry above a generation floor per store call
  (`MetaStore::ancestors_above`, one recursive query in Postgres; the floor
  drops by a doubling window), first-parent walks a chain segment per call.
  Whether a commit lies on another's first-parent chain is answered from
  the skip index (`graph/skip/v1`: per commit its first-parent depth and
  one jump pointer, O(log n) reads), filled in lazily along a chain and by
  the importer; merge base and reachability try it first when the other
  side is far older.
- **Path history:** each ref has an index (`history` table) of which commits
  touched which paths along its first-parent chain. It is maintained in the
  background after lands and ref updates (`ops/history.rs`, coalesced per
  ref) and makes `log --path` and blame independent of history length. On a busy ref the
  index trails the tip by the commits landed since the last catch-up; a path
  log (and blame) scans only that lag and takes the rest from the index.
- **Derived data** (`ant_store::derive`, the `derived` table): values
  computed from the commit graph and stored by kind and key. A
  `Derivation` names a versioned kind (a format change is a new kind; old
  rows are ignored, then collected), its key and codec, and a recursive
  rule: the value at a node from its predecessor's value plus the node's
  change. Callers evaluate it iteratively, walking predecessors newest first
  in batches with one `get_derived_many` per batch, stopping at the first
  stored value, never one round trip per node. Policy per kind: *eager*
  kinds store every value along a filled chain, *deferred* kinds only the
  queried one. The skip index `graph/skip/v1` is filled the eager way
  (bottom-up along first-parent chains, lazily and after imports; its rule
  reads a second entry, so it keeps its own fill loop rather than the
  trait). Commit-keyed kinds are deleted by GC with their commit.
- **Blame** (`ops/history.rs`, kind `blame/v2`, deferred): keyed by
  `(L, path)`, `L` the last commit on the revision's first-parent chain that
  changed the path, so every revision since `L` shares one value. A new
  change is derived from the previous version's stored value plus one line
  diff; with nothing stored the walk diffs older versions until all lines
  are owned or a stored value covers the rest.
- **Diff:** tree diffs walk both trees breadth first with one batched read
  per depth, through the tree cache, so a `Diff` spanning hundreds of lands
  costs a few round trips, not one per changed directory.
- **Objects:** batched over gRPC (`GetObjects`), or as plain immutable HTTP
  (`GET /o/<id>`, `Cache-Control: immutable`), so any HTTP cache or CDN can
  serve as the regional cache tier. Checkouts over HTTP fetch *tree bundles*
  (`GET /t/<tree-id>`, below) instead of one request per object.

### Tree bundles (`GET /t/<tree-id>`)

A checkout through an HTTP cache used to cost one request per file and
directory (~160k for a 96k-file tree); gRPC `GetObjects` batches, but no
HTTP cache can store it. A tree bundle is a batch keyed by something
immutable, so every tier caches it like an object:

- **Content** (`ant_server::bundle`): the requested tree, its blobs in entry
  order while they fit, then whole subdirectories breadth first until the
  first one that does not fit. Limits: 8 MiB of objects, 8192 objects; blobs
  over 1 MiB are never bundled (they stay single `/o/` requests). A
  subdirectory is included with all its small blobs or not at all. The
  content is a function of the tree id and the limits, so the response is
  `Cache-Control: public, max-age=31536000, immutable`.
- **Format** (`ant_proto::bundle`): one zstd frame of `ANTBNDL1` followed by
  `id (32) | len (u32 BE) | encoded object` frames, the tree first. The
  client hashes every object against its id before caching it (a forged or
  corrupt bundle is discarded and the objects are fetched by id) and stores
  everything in its disk cache.
- **Client** (`Workspace::materialize_with`, `ReadThrough::with_tree_bundles`):
  the breadth-first walk asks for each level's locally missing trees as
  bundles (64 in flight). A directory that arrived in a bundle is local
  when the walk reaches it, so the next request starts where a bundle
  stopped; requests ≈ data / 8 MiB plus one per bundle boundary. Only
  directories wholly inside the sparse scope (no patterns, or under a prefix
  pattern) use bundles; the rest of a sparse walk reads trees by id. Blobs a
  bundle did not carry (large, restricted, evicted) are read per object as
  before. Without `--http` nothing changes (gRPC batches already).
- **Old servers**: their `/t/` 404 lacks the `x-ant-bundle` header, which
  every `/t/` answer of a new server carries (also its 404s), so the client
  turns bundles off after one probe and reads per object.
- **Access control and shared caches.** A shared cache serves a `public`
  response to anyone who reaches it, exactly like `/o/` objects in the root
  zone; so a public bundle must (1) hold only objects that `/o/` would serve
  `public` and (2) be the same for every caller. The server therefore always
  leaves out objects outside the root zone (zone-tagged without the root
  tag), and drops every subdirectory that lists one, with everything below
  it; the client fetches those with their own requests, which are then
  checked for that caller. If the caller cannot read an object the bundle
  would still hold (a policy whose root zone is not readable by everyone),
  that object is left out too and the response becomes `private, no-store`;
  a bundle of a tree outside the root zone is always built per caller and
  `private, no-store`. An unreadable tree is a 404 like a missing one. URLs
  never encode a principal. Like `/o/`, cached public responses outlive a
  later policy change that restricts already-public content: purge the CDN
  when tightening a policy.

## Clients (`antd`, `ant-fuse`, `ant-cli`)

- **Shared disk cache:** one content-addressed store per machine
  (`antd::disk`), safe for many concurrent processes (write-temp-then-rename),
  with a size budget enforced by LRU eviction (by mtime, refreshed on reads;
  a shared byte counter plus a try-lock so one process evicts at a time;
  daemons also check periodically; `ant cache stats|gc`). A read-through layer
  fetches misses (including objects evicted under a reader) from the server in
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
  fetch are supported, with packs delta-compressed (git-style window over
  other versions of each path, sorted by size).

## Observability (`ant-server::metrics`, `ant-server::observe`)

One `Metrics` value (a `prometheus-client` registry, no global) is created in
`serve::run` and shared via `Arc`. Transports are observed by wrapping whole
services, so handlers stay untouched: a tower layer around the gRPC service
and an axum middleware around the HTTP router assign the request id (incoming
`x-request-id` or a ULID, a tracing span field, echoed on the response),
record `ant_rpc_*` / `ant_http_*` and log slow requests. Stores are wrapped
once by `Metrics::instrument` (per-op timing and error counts). Landing and
history report through `Option<&Arc<Metrics>>` parameters (`land_observed`,
`schedule_catch_up`); state that lives in process-wide statics (cache sizes
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

See [access-control.md](access-control.md) for the threat model and its v1
limitations.
