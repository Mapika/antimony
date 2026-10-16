# Git pack export

## Planning and the export walk

Every object body is read from the store once per fetch (pinned by
`every_object_is_read_from_the_store_at_most_once`).

Planning (`select.rs`) uses commit metadata only: it walks the commits from
the wants (shallow boundaries included) and collects the objects the client
already has, reading the client's trees once, ids only. It reads no body of
anything it is going to send.

The export walk (`walk.rs`) then reads the selection once, in pack order:
direct blob wants (largest first), commits, then trees and blobs *path by
path*. All versions of one path form a group; a group is complete as soon as
its parent path's group has been read (a 64-bit path fingerprint hashes the
parent's fingerprint and the entry name), so groups are read breadth-first
with 8 store reads in flight (256 trees/commits or 16 blobs each), each read
parsed on the blocking pool in parallel. Shared subtrees are read and sent
once, at the first path they are reached at. Root trees are walked newest
first by generation number, so consecutive versions are close in history.

Tree versions of a path go out sorted by size, largest first, as git's own
packer orders them: on real, merge-heavy histories the traversal order
interleaves branches, so "consecutive" versions of a directory differ a lot;
size order brings near versions together and lets bases fan out (git/git's
blobless pack was 4x larger without it). Sizes are only known once bodies are
read, and one path can have GiBs of versions (git/git's root directory: 82k
versions, about 2 GB), so a large group is sorted externally: each store read
sorts its versions of the path into a run, kept as a chain of deltas against
the previous entry (consecutive by size, so a few hundred bytes each), and
the complete group is a k-way merge of its runs that rebuilds each body with
one delta application. A group read by one store call is sorted in place.
The runs of one group are capped at 32 MiB (git/git's root: 50 MB in two
parts); past the cap the group is merged in parts, which is correct and
compresses slightly differently. Blob versions keep traversal order (their
sizes are not known either) and are passed on as they arrive. Fingerprint
collisions can reduce compression quality but cannot change reconstructed
bytes.

## Deltas

Clients advertising `ofs-delta` receive tree and blob deltas against an earlier
entry in the same pack. Each blob tries the previous `WINDOW` (10) blobs of its
path, each tree the previous `TREE_WINDOW` (40) trees (small, size-sorted and
the bulk of a blobless clone: git/git's blobless pack 81.7 -> 70.5 MiB for
about 0.3 s more), newest first, and keeps the smallest delta, with git's depth-scaled
size limit (a deeper base must beat the current best proportionally) so chains
fan out instead of running to the cap. Chains are capped at depth 50. A delta
must be below about half the target size (half less 32 bytes, scaled down for
deeper bases). Kinds are checked, including
file/directory transitions. The encoder builds one block index per base (sized
to the base, at most 2^20 slots, high bits of a mixed rolling hash), reuses it
for every target, and extends matches backwards and forwards. No REF_DELTA or
external base is emitted, even when `thin-pack` is offered. Clients without
`ofs-delta` receive full objects. Entries are zlib level 6 (git's default),
compressed by zlib-rs into a small scratch buffer (flate2's `compress_vec`
zero-fills the spare capacity of its output vector on every call, which on a
segment buffer of megabytes once cost more than the compression).

## Streaming and memory

The walk feeds a segmenter that cuts the stream into segments (at a path
change after 1 MiB, or inside a path at 16 MiB). Each segment is
delta-searched and compressed by its own blocking worker (up to 8 at once,
one reused zlib compressor each); one writer lays the segments out in order
and resolves OFS distances. The bounded queues between walk, segmenter and
writer provide backpressure. A cut inside a path hands the last window of
objects to the next segment as delta candidates; the earlier segment keeps
those objects at most 25 deep and the next one assumes that bound, so the
depth cap holds without coordination.

The pack header holds the object count, which only the finished walk knows,
so the writer spills the entries (already compressed: about the pack's size,
in memory up to 4 MiB, then in a file in the temporary directory) and then
streams header, entries and SHA-256 trailer in sidebands (65,515 payload
bytes). While the pack is being built it sends a keepalive (empty data
sideband) every 5 s, which also notices a client that has gone away. A
failed walk (missing object, unknown want) sends the error on sideband 3
(`remote error: ...` in git) and never a pack trailer or flush.

Memory is bounded by these constants (reads in flight, the runs of one
group, about ten segments, windows of at most 16 MiB each), not by history
size; no complete pack or history of object bodies is retained. Walk memory
is O(number of distinct objects): ids of the queued groups and of the objects
seen, plus the client's objects for incremental fetches. Rebuilt tree bodies
share 1 MiB slabs and delta-index tables are reused, which keeps allocator
churn (and so peak RSS) down. Measured on git/git with a counting allocator,
the live heap peaks at 300-380 MiB; peak RSS is 2-3x that with glibc's
default arenas (8 per core), about 1.7x with `MALLOC_ARENA_MAX=16`.

## Cached base packs (`cache.rs`)

A clone (`done`, no `have`s, no shallow boundary, `ofs-delta`) that finds no
usable base is built as above and its entries (the spill file: everything
between pack header and trailer) become the *base* of its filter (`blob:none`
or none), together with the sorted ids of its objects and its *tips* (the
wanted commits no other wanted commit reaches). The base is installed as soon
as the walk is complete, before the pack is streamed. Later clones whose wants
reach every tip are planned like a fetch with the base as `have`s: commits are
walked from the wants down to the first commits in the base, trees and blobs
in the base are excluded by id. The pack is

```text
header(base count + n) | base entries, verbatim | n new entries | trailer
```

OFS_DELTA distances are relative, so the base entries are valid at any
position. The client receives exactly the objects its wants reach, as without
the cache (pinned by `base_pack_reuse_is_exact` and
`clones_reuse_the_cached_base_pack`). A clone whose wants reach more than
`commits / 8 + 1024` new commits, or miss a tip (ref deleted or rewound), is
built from scratch and replaces the base. Clones arriving while a base of
their filter is built wait for it (one build per filter at a time) and then
use it if it fits. Bases live in the server process only, as unlinked
temporary files (about the size of the pack; the git/git bases are 67 and 125
MiB) plus their ids in memory (32 bytes per object); a restarted server
rebuilds them on the first clones. Fetches with `have`s, shallow and
`ofs-delta`-less clones never use or build a base.

git/git on Postgres + SeaweedFS (2026-10-16, loaded 20-core WSL2 machine,
`git clone --single-branch` over HTTP, native = `git daemon` on the
`gc --aggressive` + bitmaps repository):

| clone | before | first clone (builds the base) | later clones | native git |
|---|---:|---:|---:|---:|
| `--filter=blob:none --no-checkout` | 6.0-7.8 s, .git 92.4 MB | 6.8 s | 2.1 s, .git 81.4 MB | 1.6 s, .git 61.1 MB |
| full (with checkout) | 19-22 s, .git 160 MB | 12-17 s | 7.1 s, .git 149 MB | 6.3 s, .git 112 MB |
| base 2,000 first-parent commits behind | | | blobless 3.0 s, full 10 s | |

The remaining 1.33x in bytes is mostly SHA-256: git's own `repack -a -d -f
--window=250` of the same SHA-256 objects (blobless) is 69.7 MB against 53 MB
for its SHA-1 pack.

## Measurements

Unit/integration tests (`cargo nextest run -p ant-gitserve`):

- `merged_history_pack_is_close_to_git_repack`: 1,200 commits plus merged
  topic branches touching a few files in deep trees; the blobless pack must be
  no larger than `git repack -adf` of the same objects and at most 35% of the
  undeltified size (measured: 30%, 0.5x git; the old single-base encoder: 50%).
- `appended_history_stops_at_the_depth_cap`: depth reaches 50 and never passes
  it, also across segment cuts with handed-over context.
- `synthetic_history_pack_size_time_and_peak_rss`,
  `large_blobless_history_is_delta_compressed`: 2,000 x 2,000-entry trees,
  deltas vs undeltified, `git fsck --full`.

git/git (82,443 commits on master) imported into Postgres + SeaweedFS S3,
cloned over HTTP from `ant serve` on 2026-10-14 (release build, 20-core WSL2
machine shared with other work, medians of 3, a fresh server per clone, every
clone `git fsck --full` clean; native = `git clone --single-branch --no-tags
file://` of the SHA-1 source with `uploadpack.allowFilter`, reusing its
on-disk deltas). "two-pass" is the previous export (plan, then read every
tree again for delta search); "single pass" is the export walk above:

| clone | two-pass | single pass | native git |
|---|---:|---:|---:|
| `--filter=blob:none --no-checkout` time | 13.5 s | 6.9 s | 5.9 s |
| ... `.git` size | 116 MiB | 88 MiB | 115 MiB |
| ... server peak RSS | 0.51 GiB | 0.73-0.83 GiB (0.6-0.7 with `MALLOC_ARENA_MAX=16`) | |
| full clone (with checkout) time | 33.7 s | 18.5 s | 14.2 s |
| ... `.git` size | 252 MiB | 153 MiB | 311 MiB |
| ... server peak RSS | 0.73 GiB | 0.86-1.10 GiB (0.6 with `MALLOC_ARENA_MAX=16`) | |
| `--depth 1` | 0.62 s | 0.71 s | 1.24 s |

Earlier (2026-10-14 morning, before planning and delta search were reworked):
blobless 48.3 s and 905 MiB, full clone 99 s. The single-pass pack is
smaller because root trees are walked by generation (consecutive versions
close in history) and the size order is exact per path. Peak RSS is above
the two-pass export's because reading and delta search now overlap.
