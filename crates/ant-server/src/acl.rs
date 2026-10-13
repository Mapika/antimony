//! Access control enforcement.
//!
//! TASK (acl): implement this module and wire it through `rpc`, `http`,
//! `ant-gitserve`, the `ant-server` binary and the `ant` CLI. Policy semantics
//! live in `ant_auth` (read its module docs first) — this module only applies
//! them.
//!
//! Modes:
//! - No `--policy <file>` → OPEN mode: everything allowed (today's behavior),
//!   log a startup warning.
//! - With a policy, every request must carry a bearer token:
//!   gRPC metadata `authorization: Bearer <token>`; HTTP header
//!   `Authorization: Bearer <token>`; git over HTTP uses Basic auth with the
//!   token as the password (any username). Missing/unknown token →
//!   gRPC `unauthenticated` / HTTP 401 (with `WWW-Authenticate: Basic` on the
//!   git endpoints so `git` prompts/uses credential helpers). Denied →
//!   `permission_denied` / HTTP 403. The CLI sends `ANT_TOKEN` if set.
//!
//! Object zone tags (prevents fetching restricted blobs by id):
//! - Stored as derived data: kind `"acl/zone/v1/<zone>"`, key = object id,
//!   value = b"1" (append-only; a zone is the governing rule prefix, see
//!   `ant_auth::Policy::zone_of`). Look up with
//!   `MetaStore::get_derived_many` (one call per restricted zone per batch;
//!   override it in `PgStore` with a single `= ANY($2)` query).
//! - `tag_written(backend, policy, writes: &[(RepoPath, ObjectId)])` is called
//!   for every object a write path creates or references at a path:
//!   blobs at their file path, trees at their directory path (root tree at
//!   the root). For each write whose zone is NOT the root zone: if the object
//!   has no tags yet and already existed before this write (it was public),
//!   tag it with the root zone too; then tag it with its zone. Writes in the
//!   root zone add the root tag only if the object already carries tags.
//!   Call sites: `ops::changeset::create_changeset` (blobs + rewritten
//!   trees), `ops::land` (rebased/merged objects), and after git import.
//! - `reindex(backend, policy, commit)`: walk the commit's tree and tag every
//!   object under a non-root zone (and objects also present in the root zone
//!   with the root tag). Used after `ant import` and when the policy changes;
//!   expose as `ant admin reindex-acl <rev>` (store flags like `ant import`).
//! - `readable(backend, policy, principal, ids) -> Vec<bool>`: index-aligned;
//!   skip all lookups when `policy.can_read_everything(principal)`.
//!
//! Enforcement points (all must have integration tests in
//! `crates/ant-server/tests/acl.rs`, using a policy with a restricted
//! `secrets/` zone, a reader without access, a writer with access, an admin):
//! - GetObjects / HasObjects / HTTP `GET /o/{id}`: unreadable objects are
//!   reported exactly like missing ones (not found / `found=false` / 404) —
//!   never leak existence. HTTP responses for objects outside the root zone
//!   use `Cache-Control: private, ...` instead of `public`.
//! - ReadPath: denied path → not found. Tree listings still show restricted
//!   entry names and ids (documented v1 limitation: clients need ids to
//!   sparse-skip; it is content access we protect). Log `--path` / Diff: drop
//!   changes under unreadable paths.
//! - CreateChangeset: every op path must be writable, else permission_denied.
//! - Land: principal must `can_land(ref)`, and every path changed by every
//!   replayed commit must be writable by the principal.
//! - UpdateRef: `can_update_ref`.
//! - PutObjects: only blobs allowed unless admin (trees/commits could smuggle
//!   history past path checks) → permission_denied.
//! - git export (`ant-gitserve`): requires `can_read_everything` (it cannot
//!   filter trees without changing ids) else 403.
//!
//! Tests must also cover: fetching a restricted blob by id is denied while
//! the same bytes written to a public path become readable; open mode keeps
//! the existing behavior (all existing tests must still pass untouched).
