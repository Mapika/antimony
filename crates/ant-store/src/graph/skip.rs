//! First-parent skip index (derived kind [`SKIP_KIND`]): per commit, its
//! first-parent depth and one jump pointer further down its first-parent
//! chain, so "is `a` on `d`'s first-parent chain" costs O(log n) reads
//! however long the chain is (a linear chain read costs O(n) rows).
//!
//! Jump pointers follow Myers' applicative random-access stack
//! ("skew-binary" jumps): a commit `n` with first parent `p` jumps to
//! `jump(jump(p))` when `p`'s jump and its jump's jump span equal depths,
//! else to `p`. Building an entry reads two entries (`p`'s and `jump(p)`'s),
//! entries are fixed-size (no power-of-two pointer lists), and descending to
//! any first-parent ancestor takes O(log depth) hops. A descent towards the
//! lowest chain commit of generation `>= g` is the same search, since
//! generations strictly decrease along a first-parent chain.
//!
//! Entries are derived lazily: a query on a commit without an entry first
//! reads its first-parent chain down to the newest commit that has one (or
//! the root) and writes the missing entries bottom-up, so every commit with
//! an entry has entries along its whole first-parent chain (entries are
//! immutable functions of the chain, written in that order; garbage
//! collection deletes an entry only with its commit, and a commit only with
//! its descendants). Filling in reads the chain in reads growing from
//! [`FIRST_FILL_READ`] to [`MAX_FILL_READ`] commits, each with one
//! `get_derived_many`, and writes [`WRITE_BATCH`] entries per call: the
//! first full fill of an imported history costs a few store calls per
//! [`MAX_FILL_READ`] commits once (the importer does it for imported refs,
//! [`fill`]); later fills cover only the commits recorded since, usually
//! within the first read.

use std::collections::HashMap;

use ant_core::ObjectId;
use bytes::Bytes;

use crate::{Backend, CommitNode, Result};

/// Derived kind of the skip index: key = commit id, value = an [`Entry`].
pub const SKIP_KIND: &str = "graph/skip/v1";

/// First chain read when filling in missing entries (recent commits on an
/// indexed chain); later reads grow by [`FILL_READ_GROWTH`].
const FIRST_FILL_READ: usize = 64;
const FILL_READ_GROWTH: usize = 32;
/// Largest chain read while filling in.
const MAX_FILL_READ: usize = 32768;
/// Entries written per `put_derived_many`.
const WRITE_BATCH: usize = 8192;

/// One commit's skip-index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Entry {
    /// First-parent steps down to the chain's root (the root: 0).
    pub depth: u64,
    pub generation: u64,
    /// A first-parent ancestor (the commit itself for a root).
    pub jump: ObjectId,
    pub jump_depth: u64,
    pub jump_generation: u64,
    /// The first parent (`None`: a root).
    pub parent: Option<ObjectId>,
}

impl Entry {
    fn encode(&self) -> Bytes {
        let mut v = Vec::with_capacity(96);
        v.extend_from_slice(&self.depth.to_le_bytes());
        v.extend_from_slice(&self.generation.to_le_bytes());
        v.extend_from_slice(self.jump.as_bytes());
        v.extend_from_slice(&self.jump_depth.to_le_bytes());
        v.extend_from_slice(&self.jump_generation.to_le_bytes());
        if let Some(p) = self.parent {
            v.extend_from_slice(p.as_bytes());
        }
        Bytes::from(v)
    }

    /// `None` for a malformed value (treated as missing: the caller falls
    /// back to reading the chain).
    fn decode(v: &[u8]) -> Option<Self> {
        if v.len() != 64 && v.len() != 96 {
            return None;
        }
        let u =
            |at: usize| v.get(at..at + 8).and_then(|b| b.try_into().ok()).map(u64::from_le_bytes);
        let id =
            |at: usize| v.get(at..at + 32).and_then(|b| <[u8; 32]>::try_from(b).ok()).map(ObjectId);
        Some(Self {
            depth: u(0)?,
            generation: u(8)?,
            jump: id(16)?,
            jump_depth: u(48)?,
            jump_generation: u(56)?,
            parent: if v.len() == 96 { Some(id(64)?) } else { None },
        })
    }
}

async fn read(b: &Backend, id: ObjectId) -> Result<Option<Entry>> {
    Ok(b.meta.get_derived(SKIP_KIND, id.as_bytes()).await?.and_then(|v| Entry::decode(&v)))
}

/// Fill in the skip index along `tip`'s first-parent chain (e.g. after an
/// import, so that later queries need no fill). False when the chain cannot
/// be indexed (see [`entry`]).
pub async fn fill(b: &Backend, tip: ObjectId) -> Result<bool> {
    Ok(entry(b, tip).await?.is_some())
}

/// `id`'s entry, filling in missing entries along its first-parent chain
/// first. `None` when the chain cannot be indexed (it reaches an unrecorded
/// or hidden commit, or an entry it needs is unreadable).
pub(super) async fn entry(b: &Backend, id: ObjectId) -> Result<Option<Entry>> {
    if let Some(e) = read(b, id).await? {
        return Ok(Some(e));
    }
    // The chain from `id` down to the newest commit with an entry, newest
    // first: (id, generation, first parent).
    let mut missing: Vec<(ObjectId, u64, Option<ObjectId>)> = Vec::new();
    let mut known: HashMap<ObjectId, Entry> = HashMap::new();
    let mut from = id;
    let mut want = FIRST_FILL_READ;
    'read: loop {
        let chain = b.meta.first_parent_chain(from, None, want).await?;
        if chain.first().map(|n| n.id) != Some(from) {
            return Ok(None);
        }
        let keys: Vec<&[u8]> = chain.iter().map(|n| n.id.as_bytes().as_slice()).collect();
        let found = b.meta.get_derived_many(SKIP_KIND, &keys).await?;
        for (n, v) in chain.iter().zip(found) {
            if let Some(e) = v.and_then(|v| Entry::decode(&v)) {
                known.insert(n.id, e);
                break 'read;
            }
            missing.push((n.id, n.generation, n.parents.first().copied()));
        }
        let Some(last) = chain.last() else { return Ok(None) };
        match last.parents.first() {
            None => break,
            // A short read that did not end at a root: the chain reaches a
            // commit that is not readable.
            Some(_) if chain.len() < want => return Ok(None),
            Some(p) => from = *p,
        }
        want = (want * FILL_READ_GROWTH).min(MAX_FILL_READ);
    }
    // Build bottom-up, writing in that order (see the module docs).
    let mut batch: Vec<(Vec<u8>, Bytes)> = Vec::new();
    for &(id, generation, parent) in missing.iter().rev() {
        let e = match parent {
            None => Entry {
                depth: 0,
                generation,
                jump: id,
                jump_depth: 0,
                jump_generation: generation,
                parent: None,
            },
            Some(p) => {
                let Some(pe) = known.get(&p).copied() else { return Ok(None) };
                let je = match known.get(&pe.jump) {
                    Some(e) => *e,
                    None => match read(b, pe.jump).await? {
                        Some(e) => {
                            known.insert(pe.jump, e);
                            e
                        }
                        None => return Ok(None),
                    },
                };
                let (jump, jump_depth, jump_generation) =
                    if pe.depth - je.depth == je.depth - je.jump_depth {
                        (je.jump, je.jump_depth, je.jump_generation)
                    } else {
                        (p, pe.depth, pe.generation)
                    };
                Entry {
                    depth: pe.depth + 1,
                    generation,
                    jump,
                    jump_depth,
                    jump_generation,
                    parent: Some(p),
                }
            }
        };
        known.insert(id, e);
        batch.push((id.as_bytes().to_vec(), e.encode()));
        if batch.len() >= WRITE_BATCH {
            b.meta.put_derived_many(SKIP_KIND, std::mem::take(&mut batch)).await?;
        }
    }
    if !batch.is_empty() {
        b.meta.put_derived_many(SKIP_KIND, batch).await?;
    }
    Ok(known.get(&id).copied())
}

/// Whether `ancestor` lies on `descendant`'s first-parent chain (both
/// recorded), by descending the skip index: O(log depth) entry reads.
/// `None` when the chain cannot be indexed (see [`entry`]).
pub(super) async fn on_first_parent_chain(
    b: &Backend,
    ancestor: &CommitNode,
    descendant: ObjectId,
) -> Result<Option<bool>> {
    let target = ancestor.generation;
    let Some(mut e) = entry(b, descendant).await? else { return Ok(None) };
    let mut at = descendant;
    loop {
        if at == ancestor.id {
            return Ok(Some(true));
        }
        if e.generation <= target {
            return Ok(Some(false));
        }
        // Jump while that stays at or above the lowest chain commit whose
        // generation is >= the target's; else step to the first parent.
        let next = if e.jump != at && e.jump_generation >= target {
            e.jump
        } else {
            match e.parent {
                Some(p) => p,
                None => return Ok(Some(false)),
            }
        };
        let Some(ne) = read(b, next).await? else { return Ok(None) };
        (at, e) = (next, ne);
    }
}

/// `id`'s `n`-th first-parent ancestor (`Some(None)`: the chain is shorter),
/// by descending the skip index: O(log depth) entry reads. `None` when the
/// chain cannot be indexed (see [`entry`]).
pub(super) async fn nth_first_parent(
    b: &Backend,
    id: ObjectId,
    n: u64,
) -> Result<Option<Option<ObjectId>>> {
    let Some(mut e) = entry(b, id).await? else { return Ok(None) };
    let Some(depth) = e.depth.checked_sub(n) else { return Ok(Some(None)) };
    let mut at = id;
    while e.depth > depth {
        let next = if e.jump_depth >= depth && e.jump != at {
            e.jump
        } else {
            match e.parent {
                Some(p) => p,
                None => return Ok(None),
            }
        };
        let Some(ne) = read(b, next).await? else { return Ok(None) };
        (at, e) = (next, ne);
    }
    Ok(Some(Some(at)))
}
