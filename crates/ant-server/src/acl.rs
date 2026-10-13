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
//!
//! Implementation notes (beyond the contract above):
//! - Blobs uploaded with `PutObjects` that did not exist before are tagged
//!   with the synthetic zone [`PENDING_ZONE`] (readable by nobody but
//!   admins) plus an uploader record for the uploading principal. Without
//!   this, a secret uploaded first and committed under `secrets/` second
//!   would count as "already existed, untagged" and become public. The
//!   uploader may reference its own uploads by id in `CreateChangeset`;
//!   everyone else must be able to read a blob to reference it by id
//!   (otherwise "read by copy" would leak it). Unreferenceable blobs are
//!   reported as missing (`NotFound`), never as forbidden.
//! - UpdateRef moving an existing ref additionally requires write access to
//!   every path in the tree diff between the old and new target (admins
//!   exempt). Creating a ref has no previous content to protect and only
//!   needs `can_update_ref` (resp. `can_land`).
//! - `reindex` skips subtrees already indexed under the same zone layout,
//!   using a derived marker per (tree id, path) whose kind embeds a
//!   fingerprint of the policy's zone prefixes. Changing read/write lists
//!   needs no reindex; adding/removing path rules does.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use ant_auth::{Policy, Principal, Zone};
use ant_core::{EntryMode, ObjectId, ObjectKind, RepoPath, Tree};
use ant_store::{Backend, ObjectStoreExt, StoreError};
use bytes::Bytes;
use futures::{StreamExt, stream};

use crate::ops::{OpError, OpResult};

/// Derived-data kind prefix of zone tags: `acl/zone/v1/<zone>`.
pub const ZONE_KIND_PREFIX: &str = "acl/zone/v1/";
/// Synthetic zone of freshly uploaded, not yet placed blobs. Policy prefixes
/// are repo paths and can never start with `/`, so no rule can grant it.
pub const PENDING_ZONE: &str = "/pending";
/// Derived-data kind prefix of uploader records: `acl/uploader/v1/<name>`.
const UPLOADER_KIND_PREFIX: &str = "acl/uploader/v1/";
/// Derived-data kind prefix of reindex markers (`<prefix><zone fingerprint>`).
const TREE_MARKER_PREFIX: &str = "acl/reindexed/v1/";
/// Keys per `get_derived_many` / ids per `get_many` call.
const LOOKUP_CHUNK: usize = 8192;
/// Concurrent `put_derived` calls (the trait has no bulk write).
const PUT_CONCURRENCY: usize = 64;
const TAG: &[u8] = b"1";

pub fn zone_kind(zone: &str) -> String {
    format!("{ZONE_KIND_PREFIX}{zone}")
}

fn uploader_kind(principal: &str) -> String {
    format!("{UPLOADER_KIND_PREFIX}{principal}")
}

// ---------------------------------------------------------------------------
// Configuration and authentication
// ---------------------------------------------------------------------------

/// Server-wide access-control configuration: open (no policy) or enforcing.
#[derive(Clone, Default)]
pub struct Acl {
    policy: Option<Arc<Policy>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("missing bearer token")]
    Missing,
    #[error("unknown token")]
    Unknown,
}

impl Acl {
    /// No policy: every request is allowed (pre-ACL behavior).
    pub fn open() -> Self {
        Self { policy: None }
    }

    pub fn new(policy: Policy) -> Self {
        Self { policy: Some(Arc::new(policy)) }
    }

    /// Read and parse a policy file.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading policy {}: {e}", path.display()))?;
        Ok(Self::new(Policy::parse(&text)?))
    }

    pub fn policy(&self) -> Option<&Arc<Policy>> {
        self.policy.as_ref()
    }

    pub fn is_open(&self) -> bool {
        self.policy.is_none()
    }

    /// Resolve a token to a caller. Open mode → `Ok(None)` (no checks).
    pub fn authenticate(&self, token: Option<&str>) -> Result<Option<Access>, AuthError> {
        let Some(policy) = &self.policy else { return Ok(None) };
        let token = token.filter(|t| !t.is_empty()).ok_or(AuthError::Missing)?;
        let principal = policy.authenticate(token).ok_or(AuthError::Unknown)?.clone();
        Ok(Some(Access { policy: policy.clone(), principal }))
    }

    /// Authenticate from an `Authorization` header value (`Bearer <token>`,
    /// or `Basic` with the token as the password).
    pub fn authenticate_header(&self, header: Option<&str>) -> Result<Option<Access>, AuthError> {
        if self.is_open() {
            return Ok(None);
        }
        let token = header.and_then(token_from_authorization);
        self.authenticate(token.as_deref())
    }
}

/// The token carried by an `Authorization` header value: `Bearer <token>`,
/// or `Basic base64(<user>:<token>)` (any user name).
pub fn token_from_authorization(value: &str) -> Option<String> {
    let (scheme, rest) = value.trim().split_once(' ')?;
    let rest = rest.trim();
    if scheme.eq_ignore_ascii_case("bearer") {
        return Some(rest.to_string());
    }
    if scheme.eq_ignore_ascii_case("basic") {
        let text = String::from_utf8(base64_decode(rest)?).ok()?;
        let (_, password) = text.split_once(':')?;
        return Some(password.to_string());
    }
    None
}

/// Standard base64, padding optional.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        Some(u32::from(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        }))
    }
    let s = s.trim_end_matches('=').as_bytes();
    if s.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    for chunk in s.chunks(4) {
        let mut acc = 0u32;
        for (i, c) in chunk.iter().enumerate() {
            acc |= val(*c)? << (18 - 6 * i);
        }
        out.extend_from_slice(&acc.to_be_bytes()[1..chunk.len()]);
    }
    Some(out)
}

/// An authenticated caller under an enforcing policy.
#[derive(Debug, Clone)]
pub struct Access {
    pub policy: Arc<Policy>,
    pub principal: Principal,
}

impl Access {
    pub fn is_admin(&self) -> bool {
        self.principal.admin
    }

    pub fn can_read(&self, path: &RepoPath) -> bool {
        self.policy.can_read(&self.principal, path)
    }

    pub fn can_write(&self, path: &RepoPath) -> bool {
        self.policy.can_write(&self.principal, path)
    }

    pub fn can_read_everything(&self) -> bool {
        self.policy.can_read_everything(&self.principal)
    }

    /// True if `path` and everything underneath it is readable.
    pub fn can_read_all_under(&self, path: &RepoPath) -> bool {
        if self.can_read_everything() {
            return true;
        }
        self.can_read(path)
            && self.policy.zones().iter().all(|z| match RepoPath::new(z.clone()) {
                Ok(zp) if zp.starts_with(path) => self.policy.can_read_zone(&self.principal, z),
                _ => true,
            })
    }

    /// `PermissionDenied` for the first path the caller may not write.
    pub fn check_writable<'a>(
        &self,
        mut paths: impl Iterator<Item = &'a RepoPath>,
    ) -> OpResult<()> {
        if self.is_admin() {
            return Ok(());
        }
        match paths.find(|p| !self.can_write(p)) {
            Some(p) => Err(OpError::PermissionDenied(format!("no write access to {p}"))),
            None => Ok(()),
        }
    }
}

/// Authorize `UpdateRef(name, expected → new)`: `can_update_ref`; and,
/// unless admin, moving an existing ref requires write access to every path
/// in the tree diff between the old and the new target (which must be a
/// recorded commit). Deleting needs only `can_update_ref`; creating has no
/// previous content to protect. The diff is taken against `expected`, which
/// the CAS then guarantees is the ref's actual old value.
pub async fn check_update_ref(
    b: &Backend,
    access: &Access,
    name: &str,
    expected: Option<ObjectId>,
    new: Option<ObjectId>,
) -> OpResult<()> {
    if !access.policy.can_update_ref(&access.principal, name) {
        return Err(OpError::PermissionDenied(format!("cannot update ref {name}")));
    }
    if access.is_admin() {
        return Ok(());
    }
    let Some(new) = new else { return Ok(()) };
    let ids: Vec<ObjectId> = std::iter::once(new).chain(expected).collect();
    let nodes = b.meta.get_commits(&ids).await?;
    let new_tree = nodes[0]
        .as_ref()
        .ok_or_else(|| OpError::NotFound(format!("commit {new}")))?
        .tree;
    if expected.is_none() {
        return Ok(());
    }
    let old_tree = nodes
        .get(1)
        .and_then(Option::as_ref)
        .ok_or_else(|| {
            OpError::PermissionDenied(format!("{name} does not point at a recorded commit"))
        })?
        .tree;
    let changes = ant_store::graph::diff_trees(b.objects.as_ref(), Some(old_tree), Some(new_tree)).await?;
    access.check_writable(changes.iter().map(|c| &c.path))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tag lookups
// ---------------------------------------------------------------------------

/// All zones a tag may name: the policy's zones plus [`PENDING_ZONE`].
fn all_tag_zones(policy: &Policy) -> Vec<Zone> {
    let mut zones = policy.zones();
    zones.push(PENDING_ZONE.to_string());
    zones
}

/// Index-aligned: the subset of `zones` each id is tagged with. One
/// `get_derived_many` per zone per chunk of ids.
async fn tags_in(
    b: &Backend,
    zones: &[Zone],
    ids: &[ObjectId],
) -> Result<Vec<BTreeSet<Zone>>, StoreError> {
    let mut out = vec![BTreeSet::new(); ids.len()];
    for (c, chunk) in ids.chunks(LOOKUP_CHUNK).enumerate() {
        let keys: Vec<&[u8]> = chunk.iter().map(|id| id.as_bytes().as_slice()).collect();
        for zone in zones {
            let found = b.meta.get_derived_many(&zone_kind(zone), &keys).await?;
            for (i, v) in found.into_iter().enumerate() {
                if v.is_some() {
                    out[c * LOOKUP_CHUNK + i].insert(zone.clone());
                }
            }
        }
    }
    Ok(out)
}

/// Index-aligned zone tags of `ids` (the policy's zones and pending).
pub async fn object_tags(
    b: &Backend,
    policy: &Policy,
    ids: &[ObjectId],
) -> Result<Vec<BTreeSet<Zone>>, StoreError> {
    tags_in(b, &all_tag_zones(policy), ids).await
}

/// Index-aligned read access of `principal` to objects by id (see
/// `Policy::can_read_object`). Skips all lookups when the principal can
/// read everything.
pub async fn readable(
    b: &Backend,
    policy: &Policy,
    principal: &Principal,
    ids: &[ObjectId],
) -> Result<Vec<bool>, StoreError> {
    if policy.can_read_everything(principal) || ids.is_empty() {
        return Ok(vec![true; ids.len()]);
    }
    let (can, cannot): (Vec<Zone>, Vec<Zone>) =
        all_tag_zones(policy).into_iter().partition(|z| policy.can_read_zone(principal, z));
    let root_readable = policy.root_zone().is_some_and(|z| policy.can_read_zone(principal, &z));
    if !root_readable {
        // Untagged objects are unreadable: a readable tag is required.
        return Ok(tags_in(b, &can, ids).await?.into_iter().map(|t| !t.is_empty()).collect());
    }
    // Untagged objects are readable; only objects carrying an unreadable tag
    // need a second lookup for a readable one.
    let hidden: Vec<bool> =
        tags_in(b, &cannot, ids).await?.into_iter().map(|t| !t.is_empty()).collect();
    let suspects: Vec<ObjectId> =
        ids.iter().zip(&hidden).filter(|(_, h)| **h).map(|(id, _)| *id).collect();
    let mut rescued = tags_in(b, &can, &suspects).await?.into_iter().map(|t| !t.is_empty());
    Ok(hidden.into_iter().map(|h| !h || rescued.next().unwrap_or(false)).collect())
}

/// Write `(kind, key)` markers with bounded concurrency (the trait has no
/// bulk write). All writes finish before returning; the first error wins.
async fn put_markers(b: &Backend, puts: Vec<(String, Vec<u8>)>) -> Result<(), StoreError> {
    let results: Vec<_> = stream::iter(puts)
        .map(|(kind, key)| async move {
            b.meta.put_derived(&kind, &key, Bytes::from_static(TAG)).await
        })
        .buffer_unordered(PUT_CONCURRENCY)
        .collect()
        .await;
    results.into_iter().collect()
}

// ---------------------------------------------------------------------------
// Tagging
// ---------------------------------------------------------------------------

/// Where one object occurs: its non-root zones, and whether it also occurs
/// in the root zone.
#[derive(Default)]
struct Occurrence {
    zones: BTreeSet<Zone>,
    root: bool,
}

fn add_occurrence(
    occ: &mut HashMap<ObjectId, Occurrence>,
    policy: &Policy,
    root: &Option<Zone>,
    path: &RepoPath,
    id: ObjectId,
) {
    let o = occ.entry(id).or_default();
    match policy.zone_of(path) {
        zone if zone == *root => o.root = true,
        Some(zone) => {
            o.zones.insert(zone);
        }
        // Unreachable: without a root rule every zone is `Some`, and with
        // one every path has a zone.
        None => o.root = true,
    }
}

/// Shared by `tag_written` (`existing_is_public`: an untagged object that
/// already existed was public and keeps that by getting the root tag) and
/// `reindex`. Returns the number of tags written.
async fn apply_tags(
    b: &Backend,
    policy: &Policy,
    occ: HashMap<ObjectId, Occurrence>,
    existing_is_public: bool,
) -> Result<usize, StoreError> {
    if occ.is_empty() {
        return Ok(0);
    }
    let root = policy.root_zone();
    let ids: Vec<ObjectId> = occ.keys().copied().collect();
    let tags = object_tags(b, policy, &ids).await?;
    let mut existed: HashMap<ObjectId, bool> = HashMap::new();
    if existing_is_public && root.is_some() {
        let check: Vec<ObjectId> = ids
            .iter()
            .zip(&tags)
            .filter(|(id, t)| t.is_empty() && !occ[*id].zones.is_empty())
            .map(|(id, _)| *id)
            .collect();
        for chunk in check.chunks(LOOKUP_CHUNK) {
            existed.extend(chunk.iter().copied().zip(b.objects.has_many(chunk).await?));
        }
    }
    let mut puts = Vec::new();
    for (id, tags) in ids.iter().zip(&tags) {
        let o = &occ[id];
        let key = id.as_bytes().to_vec();
        for z in o.zones.iter().filter(|z| !tags.contains(*z)) {
            puts.push((zone_kind(z), key.clone()));
        }
        if let Some(root) = &root
            && !tags.contains(root)
        {
            let carries = !tags.is_empty() || !o.zones.is_empty();
            let was_public = tags.is_empty() && existed.get(id).copied().unwrap_or(false);
            if (o.root && carries) || was_public {
                puts.push((zone_kind(root), key));
            }
        }
    }
    let n = puts.len();
    put_markers(b, puts).await?;
    Ok(n)
}

/// Tag objects a write path creates or references at a path (blobs at their
/// file path, trees at their directory path, the root tree at the root).
/// Must run BEFORE the objects are stored: "already existed" is decided
/// here with `has_many`.
pub async fn tag_written(
    b: &Backend,
    policy: &Policy,
    writes: &[(RepoPath, ObjectId)],
) -> Result<(), StoreError> {
    let root = policy.root_zone();
    let mut occ = HashMap::new();
    for (path, id) in writes {
        add_occurrence(&mut occ, policy, &root, path, *id);
    }
    apply_tags(b, policy, occ, true).await.map(|_| ())
}

/// Record objects uploaded by `access` (`PutObjects`), before storing them:
/// new blobs get the pending tag; new and already-tagged blobs get an
/// uploader record so the uploader can reference them by id (uploading the
/// bytes proves knowledge of them). Untagged existing objects are public.
/// Trees/commits (admin only) stay untagged: reindex after pushing them.
pub async fn record_uploads(
    b: &Backend,
    access: &Access,
    objects: &[(ObjectId, ObjectKind)],
) -> Result<(), StoreError> {
    let ids: Vec<ObjectId> =
        objects.iter().filter(|(_, k)| *k == ObjectKind::Blob).map(|(id, _)| *id).collect();
    if ids.is_empty() {
        return Ok(());
    }
    let mut exists = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(LOOKUP_CHUNK) {
        exists.extend(b.objects.has_many(chunk).await?);
    }
    let tags = object_tags(b, &access.policy, &ids).await?;
    let uploader = uploader_kind(&access.principal.name);
    let pending = zone_kind(PENDING_ZONE);
    let mut puts = Vec::new();
    for ((id, exists), tags) in ids.iter().zip(exists).zip(tags) {
        let key = id.as_bytes().to_vec();
        if !exists {
            puts.push((pending.clone(), key.clone()));
            puts.push((uploader.clone(), key));
        } else if !tags.is_empty() {
            puts.push((uploader.clone(), key));
        }
    }
    put_markers(b, puts).await
}

/// Index-aligned: may `access` reference these blobs by id (readable, or
/// uploaded by this principal)?
pub async fn can_reference(
    b: &Backend,
    access: &Access,
    ids: &[ObjectId],
) -> Result<Vec<bool>, StoreError> {
    let mut ok = readable(b, &access.policy, &access.principal, ids).await?;
    let denied: Vec<usize> = (0..ids.len()).filter(|i| !ok[*i]).collect();
    let kind = uploader_kind(&access.principal.name);
    for chunk in denied.chunks(LOOKUP_CHUNK) {
        let keys: Vec<&[u8]> = chunk.iter().map(|i| ids[*i].as_bytes().as_slice()).collect();
        for (i, v) in chunk.iter().zip(b.meta.get_derived_many(&kind, &keys).await?) {
            if v.is_some() {
                ok[*i] = true;
            }
        }
    }
    Ok(ok)
}

/// True if the object lies outside the root zone (carries tags, but not the
/// root tag): HTTP responses for it must not be stored by shared caches.
pub async fn is_private(b: &Backend, policy: &Policy, id: ObjectId) -> Result<bool, StoreError> {
    let tags = object_tags(b, policy, &[id]).await?.pop().unwrap_or_default();
    Ok(!tags.is_empty() && policy.root_zone().is_none_or(|r| !tags.contains(&r)))
}

// ---------------------------------------------------------------------------
// Reindex
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReindexStats {
    /// Trees read and walked.
    pub trees_walked: usize,
    /// Subtrees skipped because a previous reindex covered them.
    pub trees_skipped: usize,
    /// Tags written.
    pub tags_written: usize,
}

fn marker_kind(policy: &Policy) -> String {
    let mut zones = policy.zones();
    zones.sort();
    let fingerprint = ObjectId(ant_auth::hash_token(&zones.join("\n"))).to_hex();
    format!("{TREE_MARKER_PREFIX}{}", &fingerprint[..16])
}

fn marker_key(path: &RepoPath, tree: &ObjectId) -> Vec<u8> {
    let mut key = tree.as_bytes().to_vec();
    key.extend_from_slice(path.as_str().as_bytes());
    key
}

/// Tag every object of `commit`'s tree by the zones it occurs in: objects
/// under a non-root zone get those zones, plus the root tag if they also
/// occur in the root zone (or already carry tags). Incremental: subtrees
/// (by id and path) indexed before under the same zone layout are skipped.
pub async fn reindex(b: &Backend, policy: &Policy, commit: ObjectId) -> OpResult<ReindexStats> {
    reindex_with(b, policy, commit, false).await
}

/// [`reindex`]; `full` ignores the incremental markers.
pub async fn reindex_with(
    b: &Backend,
    policy: &Policy,
    commit: ObjectId,
    full: bool,
) -> OpResult<ReindexStats> {
    let tree = match b.meta.get_commits(&[commit]).await?.pop().flatten() {
        Some(node) => node.tree,
        None => b.objects.read_commit(&commit).await?.tree,
    };
    reindex_trees(b, policy, vec![tree], full).await
}

/// Commits whose trees are indexed together by [`reindex_history`].
const HISTORY_GROUP: usize = 256;

/// [`reindex_with`] for every recorded commit reachable from `tip` (all
/// parents), so older versions of restricted files are tagged too. Trees
/// unchanged between commits are walked once (markers), so the cost is
/// proportional to the history's distinct (tree, path) pairs.
pub async fn reindex_history(
    b: &Backend,
    policy: &Policy,
    tip: ObjectId,
    full: bool,
) -> OpResult<ReindexStats> {
    let mut seen = std::collections::HashSet::from([tip]);
    let mut frontier = vec![tip];
    let mut trees = Vec::new();
    let mut stats = ReindexStats::default();
    while !frontier.is_empty() {
        let nodes = b.meta.get_commits(&frontier).await?;
        let mut next = Vec::new();
        for node in nodes.into_iter().flatten() {
            trees.push(node.tree);
            next.extend(node.parents.into_iter().filter(|p| seen.insert(*p)));
        }
        frontier = next;
        while trees.len() >= HISTORY_GROUP || (frontier.is_empty() && !trees.is_empty()) {
            let group: Vec<ObjectId> = trees.drain(..trees.len().min(HISTORY_GROUP)).collect();
            let s = reindex_trees(b, policy, group, full).await?;
            stats.trees_walked += s.trees_walked;
            stats.trees_skipped += s.trees_skipped;
            stats.tags_written += s.tags_written;
        }
    }
    Ok(stats)
}

async fn reindex_trees(
    b: &Backend,
    policy: &Policy,
    mut roots: Vec<ObjectId>,
    full: bool,
) -> OpResult<ReindexStats> {
    let root = policy.root_zone();
    let marker = marker_kind(policy);
    let mut stats = ReindexStats::default();
    let mut occ: HashMap<ObjectId, Occurrence> = HashMap::new();
    let mut markers: Vec<Vec<u8>> = Vec::new();
    roots.sort_unstable();
    roots.dedup();
    // Breadth first: one marker lookup and one `get_many` per level chunk.
    let mut level: Vec<(RepoPath, ObjectId)> =
        roots.into_iter().map(|t| (RepoPath::root(), t)).collect();
    let mut queued = std::collections::HashSet::new();
    while !level.is_empty() {
        // The same subtree at the same path is walked once per run.
        level.retain(|(p, id)| queued.insert(marker_key(p, id)));
        let mut todo = Vec::with_capacity(level.len());
        for chunk in level.chunks(LOOKUP_CHUNK) {
            let keys: Vec<Vec<u8>> = chunk.iter().map(|(p, id)| marker_key(p, id)).collect();
            let done = if full {
                vec![None; chunk.len()]
            } else {
                let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
                b.meta.get_derived_many(&marker, &refs).await?
            };
            for ((entry, key), done) in chunk.iter().zip(keys).zip(done) {
                if done.is_some() {
                    stats.trees_skipped += 1;
                } else {
                    todo.push(entry.clone());
                    markers.push(key);
                }
            }
        }
        let mut next = Vec::new();
        for chunk in todo.chunks(LOOKUP_CHUNK) {
            let ids: Vec<ObjectId> = chunk.iter().map(|(_, id)| *id).collect();
            let objects = b.objects.get_many(&ids).await?;
            for ((path, id), object) in chunk.iter().zip(objects) {
                let object = object.ok_or(StoreError::NotFound(*id))?;
                if object.kind() != ObjectKind::Tree {
                    return Err(StoreError::WrongKind {
                        id: *id,
                        expected: ObjectKind::Tree,
                        actual: object.kind(),
                    }
                    .into());
                }
                stats.trees_walked += 1;
                add_occurrence(&mut occ, policy, &root, path, *id);
                for e in Tree::decode(object.body())?.entries() {
                    let child = path.join(&e.name)?;
                    match e.mode {
                        EntryMode::Tree => next.push((child, e.id)),
                        EntryMode::Gitlink => {}
                        _ => add_occurrence(&mut occ, policy, &root, &child, e.id),
                    }
                }
            }
        }
        level = next;
    }
    stats.tags_written = apply_tags(b, policy, occ, false).await?;
    // Markers only once every tag below them is durable.
    put_markers(b, markers.into_iter().map(|k| (marker.clone(), k)).collect()).await?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_headers() {
        assert_eq!(token_from_authorization("Bearer abc").as_deref(), Some("abc"));
        assert_eq!(token_from_authorization("bearer  abc ").as_deref(), Some("abc"));
        // base64("user:tok")
        assert_eq!(token_from_authorization("Basic dXNlcjp0b2s=").as_deref(), Some("tok"));
        assert_eq!(token_from_authorization("Basic dXNlcjp0b2s").as_deref(), Some("tok"));
        assert_eq!(token_from_authorization("Basic !!!!"), None);
        assert_eq!(token_from_authorization("Digest x"), None);
        assert_eq!(token_from_authorization("Bearer"), None);
        // base64(":")
        assert_eq!(token_from_authorization("Basic Og==").as_deref(), Some(""));
    }

    #[test]
    fn base64() {
        assert_eq!(base64_decode("aGVsbG8gd29ybGQ=").unwrap(), b"hello world");
        assert_eq!(base64_decode("YQ==").unwrap(), b"a");
        assert_eq!(base64_decode("YWI=").unwrap(), b"ab");
        assert_eq!(base64_decode("YWJj").unwrap(), b"abc");
        assert!(base64_decode("Y").is_none());
    }

    #[test]
    fn open_mode_never_authenticates() {
        let acl = Acl::open();
        assert!(acl.authenticate(None).unwrap().is_none());
        assert!(acl.authenticate_header(Some("Bearer junk")).unwrap().is_none());
        let acl = Acl::new(Policy::parse("").unwrap());
        assert_eq!(acl.authenticate(None).unwrap_err(), AuthError::Missing);
        assert_eq!(acl.authenticate_header(Some("Bearer x")).unwrap_err(), AuthError::Unknown);
        assert_eq!(acl.authenticate_header(Some("Bearer ")).unwrap_err(), AuthError::Missing);
        assert_eq!(acl.authenticate_header(None).unwrap_err(), AuthError::Missing);
    }
}
