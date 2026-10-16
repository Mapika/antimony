# Compatibility contract (v0.x)

antimony versions follow [Semantic Versioning](https://semver.org) with the
usual 0.x reading: while the major version is 0, the **minor** version
(`0.1` to `0.2`) may contain breaking changes, but only the ones listed
below as allowed, and only with the notice described under "Deprecations".
A **patch** release (`0.1.0` to `0.1.1`) never breaks any surface in this
document. From `1.0` on, anything this document allows in a minor release
moves to major releases.

Every release records changes to these surfaces in `CHANGELOG.md`, under the
headings **Changed**, **Deprecated**, **Removed** or **Security**. A change to
a surface below that is not in the changelog is a bug.

Four surfaces are relied on by deployed clients and stored data. In the
tables, "within 0.x" means within the 0.x series, "patch" means releases that
differ only in the third number.

## 1. gRPC API (`proto/ant.proto`, package `ant.v1`)

| change | patch | minor (0.x) |
|---|---|---|
| New optional fields, new messages, new RPCs | allowed | allowed |
| New enum values | allowed | allowed (clients must tolerate unknown values) |
| Stricter validation of requests that were already invalid | allowed | allowed |
| Changing the meaning, type or number of an existing field | never | never: a new field is added and the old one deprecated |
| Removing or renaming a field, message or RPC | never | only after one minor release of deprecation |
| New or different gRPC status code for an existing failure | never | allowed, listed in the changelog |
| New package version (`ant.v2`) | n/a | only for a redesign; `ant.v1` keeps being served for at least one minor release |

Field numbers and names of removed fields are `reserve`d, never reused. The
HTTP object endpoint (`GET /o/<id>`, `Cache-Control: immutable`, object bytes
in the git encoding) follows the same rules: object ids and encodings never
change; paths of other HTTP endpoints (`/healthz`, `/readyz`, `/metrics`,
`/debug/stats`, the git smart-HTTP export) may change in a minor release with
a deprecation notice. Metric names and labels may be added in any release and
renamed or removed only after one minor release of deprecation.

Client/server skew: a client works against a server of the same minor
version and, for as long as the rules above hold, one minor version older or
newer. Upgrade servers first, then clients.

Late additions in 0.1.0 (all additive; an older server answers a new RPC with
`unimplemented` and ignores new request fields):

| addition | what |
|---|---|
| `MergeBase` | best common ancestor of two revs (`MergeBaseRequest`/`Response`) |
| `IsAncestor` | reachability, optionally first-parent only |
| `LogRange` | first-parent `from..to` history with a path filter, paged (`page_token`) |
| `LandedAs` | whether, and as which commit, a submitted commit landed on a ref |
| `WatchRef` | server stream of a ref's moves (`RefEvent`: old/new tip, landed commits oldest first, kind `land`/`update`/`force`, `coalesced`), optional catch-up `from` a commit |
| `DiffRequest` fields 3-6 | `path_prefixes`, `merge_base`, `patch`, `context_lines` |
| `DiffResponse` fields 2-3, `PathChange.patch` (7), `FilePatch`, `Hunk` | merge base used, per-file unified hunks, truncation flag |
| HTTP `GET /t/<tree-id>` | tree bundle: the tree, its blobs and whole subdirectories in one zstd-framed body (`ant_proto::bundle`, magic `ANTBNDL1`); same caching and access rules as `/o/` (see `docs/architecture.md`) |

Additions after 0.1.0 (additive):

| addition | what |
|---|---|
| HTTP `POST /o/check` | batch read-permission check for cache tiers: body = up to 4096 raw 32-byte object ids, answer = one byte per id (bit 0: `GET /o/<id>` would serve this caller; bit 1: it would be `private`), `Content-Type: application/x-ant-check`, `x-ant-check: 1`, `no-store`; authorization is exactly that of `GET /o/<id>`, and a hidden object is reported like a missing one. Metrics route `object_checks`. Absent on older servers (404/405): `ant cache-serve` detects that and falls back to conditional `GET /o/<id>` |

The bundle format follows the object endpoint's rules: what a URL returns
never changes shape; a new layout gets a new magic *and* a new route (HTTP
caches key on the URL). The bundle size limits are not part of the contract:
a client must accept any subset (objects it still lacks are fetched with
`/o/`).

Clients detect an older server instead of misreading its answer: `ant ref
set` falls back to walking `Log` pages when `IsAncestor` is unimplemented;
`ant diff --merge-base` / `--patch` fail with `error:` when the response
lacks `from_commit` / patches, and `ant diff -- <path>` also filters on the
client. A client with `--http` probes `/t/` once: a 404 or 405 without the
`x-ant-bundle` response header (an older server, or a cache in front of one)
turns bundles off for that process, and it reads per object with `/o/`.

## 2. Postgres schema

The schema is versioned (`schema_version` table, `ant admin schema`). The
binary's supported version is printed by `ant admin schema` as `supported <n>`.

- **Migrations are forward-only, additive and idempotent**. They run on start
  of `ant serve`, `ant import` and `ant admin` (or with `ant admin schema
  --migrate`, with `--no-migrate` to keep control), in one transaction under a
  per-schema advisory lock. There are no down-migrations; **back up before
  upgrading**.
- **A binary refuses a schema newer than it supports** (exit 1, `hint:
  upgrade ant`). It never reads, writes or migrates it. Downgrading across a
  migration is therefore unsupported: restore the backup instead.
- **Patch releases** never add migrations that older servers of the same
  minor cannot run alongside, so a rolling restart is always safe.
- **Minor releases** may add migrations. Running servers of the previous
  minor keep working against the migrated schema unless the changelog says a
  migration needs all servers stopped first (it will then be the only
  allowed exception). A migration that rewrites data or locks tables for long
  is called out in **Upgrading** in `docs/operations.md`.
- Applied migrations are never edited; a fix is a new migration.
- Ref moves are announced with `pg_notify` on the channel `ant_ref_moves`
  (payload `v1 <nonce> <cause> <old> <new> <ref> <schema>`; see
  `ant_store::feed`), sent right after the move commits. A payload version
  a replica does not know is ignored. Replicas that do not announce (older
  versions during a rolling upgrade) only delay `WatchRef` events: watchers
  find their moves with the next announced move of the ref or within the
  hub's 30 s resync, as coalesced events.
- Pre-versioning data (version 0) is adopted in place and stays readable for
  all of 0.x.
- **S3 pack records** are part of stored data. A pack holds single-object
  records (a zstd frame; every pack written before schema version 6),
  from version 6 on multi-object frames (magic `AFR1`), and from version 7
  on delta frames (magic `ADF1`, written only by `ant admin repack`); all
  are specified in the module docs of `ant_store::packed::frame`, and every
  later binary reads all three. A delta frame's bases are always earlier
  members of the same record, so every record decodes from its own bytes;
  a reader refuses chains longer than 1,000 and members larger than 64 MiB.
  Binaries supporting only version 5 cannot read frames: version 6 is the
  first migration after which running servers of the previous release must
  not keep serving once new servers write. A rolling upgrade is still
  possible by starting the new servers with `--pack-frames false` until the
  old ones are gone; old servers already running keep working, but none
  can be started again once the first new server has migrated the schema
  (`docs/operations.md`, Upgrading). Delta frames are written only by an
  operator's `ant admin repack`: run it once no server older than version 7
  is left. `scripts/upgrade-test.sh` checks the upgrade from the previous
  release before each release. Changing a record format again requires a
  new magic and a schema version bump.
- Migration 7 (`repack`) creates the `packs` table (`repacked_at`, `hot`,
  `moved_at`): `ant admin repack` records the packs it wrote and the packs
  it moved index rows out of; `ant admin gc` keeps the latter for its
  quiesce period after the move.
- Table and column names are an implementation detail: use the API, the
  CLI, or `ant admin` rather than reading tables directly. Operational
  surfaces that are documented (`schema_version`, `gc_*` working tables for
  the GC) change only through migrations that are announced in the changelog.

### Local data directory (`ant serve --local`)

| part | marker | now |
|---|---|---|
| directory layout | `<dir>/FORMAT` (`1\n`) | 1 |
| SQLite schema (`meta.db`) | `schema_version` table (`ant admin schema --local`) | 1 |
| pack files (`packs/<32 hex>.pack`) | 8-byte header `ANTPACK1` | 1 |

- The same rules as the Postgres schema apply: migrations are forward-only,
  recorded in `schema_version` and applied on open (or refused with
  `--no-migrate`); a binary refuses a newer `FORMAT` or schema version
  without touching the directory (`hint: upgrade ant`).
- Pack records are the S3 pack records above (single-object records and
  `AFR1` frames; local mode does not write `ADF1`), each self-contained;
  a pack file has no index of its own (the SQLite `object_index` holds
  `id -> (pack, offset, length)`). A new record format needs a new magic, a
  new pack header version and a schema bump.
- Table names are an implementation detail, as for Postgres. Moving a local
  repository to Postgres + S3 goes through `ant admin copy-local` (see
  `docs/operations.md`, Local mode), not through the files.

## 3. On-disk client formats

| format | marker | now |
|---|---|---|
| shared disk cache | `<cache>/format` | 1 |
| sparse workspace | `format=` line in `.ant/workspace` | 1 |
| lazy (FUSE) workspace state | `<state>/format`, journal record kinds | 1 |

- Additions inside a format that older binaries ignore do not change its
  version: `gc/` (size counter, locks, `gc/tiers` counters), `claims/`
  (cross-process fetch claims), `live/` (registrations of the processes
  using the cache) and, for a cache tier, `bundles/` (bundle
  index) sit next to `objects/`, which keeps its layout.
- A **missing marker means version 1**, the layout from before markers
  existed; those caches and workspaces keep working.
- Readers accept every version up to the one they were built for and
  **refuse newer ones** (or journal records of an unknown kind) with `hint:
  upgrade ant`, without modifying anything.
- A newer binary upgrades old state **in place and only when it is safe**,
  or documents the one-time step (for example emptying the cache with `ant
  cache gc --to 0`, which is always possible because the cache is
  rebuildable). Workspaces that hold uncommitted edits are never discarded by
  an upgrade: if they cannot be read in place the release must say how to
  carry the edits over (`ant submit` with the old binary).
- A format version bump is a minor-release change at the earliest, and the
  changelog names it. Patch releases never bump a format.
- Downgrading `ant` on a machine that already wrote a newer format is
  unsupported (and refused, see above).

## 4. CLI output contract

The module documentation of `crates/ant-cli/src/main.rs` is the contract for
commands, flags, environment variables, stdout records, stderr messages and
exit codes (`0` ok, `1` error, `2` land/update conflict). The end-to-end tests
under `crates/ant-cli/tests` exercise it.

- **Stable within 0.x**: the stdout record formats of the listed commands
  (one record per line, tab or space separated as documented), exit codes, the
  `error: ` / `hint: ` stderr prefixes, flag and environment variable names.
  Scripts may parse stdout.
- **Allowed in a minor release**: new commands, flags, environment variables;
  new *trailing* columns or new lines after the documented ones where the
  contract says output is a list (parse by splitting and ignore extra
  fields; never rely on a line count); new `hint:` lines; changed wording of
  `error:` text and hints (match on exit codes, not messages).
- **Allowed with deprecation**: removing or renaming a command, flag,
  environment variable or output field, or changing a record format, only
  after the notice below.
- **Never in a patch release**: anything that makes a script written for the
  same minor stop working.
- stderr progress output (for example `gc: ...` lines) and `--help` text are
  for humans and may change at any time.
- `ant --version` prints `ant <version> (<short git sha>[-dirty])`; the
  parenthesised part is absent for builds outside a git checkout. Parse the
  first two words only.

## Deprecations

1. The release that deprecates something marks it in the changelog under
   **Deprecated**, in the module docs / `--help` of the CLI, in
   `proto/ant.proto` comments (`// Deprecated: ...`, plus
   `[deprecated = true]` for fields) and in the docs.
2. Where there is a way to notice at runtime, the thing emits a one-line
   warning (`warning: ... is deprecated, use ... (removal in 0.N)` on stderr
   for the CLI; a log line, once per process, on the server). Warnings never
   change stdout or exit codes.
3. It keeps working unchanged for **at least one full minor release** after
   the one that deprecated it, and is removed (changelog: **Removed**) in
   that next minor at the earliest.
4. Security fixes may shorten this; the changelog then says so under
   **Security**.

## What is not covered

Rust crate APIs (`ant-core`, `ant-store`, ... are published as workspace
members, not as a library), test helpers, `ant-bench` flags and output,
benchmark and scorecard numbers, log message text, the contents of
`/debug/stats`, and anything marked experimental in its documentation.
