# Git pack export

## Planning

The fetch planner keeps object IDs, 64-bit path fingerprints and (for trees,
which it reads anyway) body sizes; it deduplicates before enqueueing trees.
The tree walk keeps several store reads in flight while the previous batch is
parsed on the blocking pool, with a zero-copy entry parser. Shared objects are
sent once. Known historical objects are still subtracted for incremental
fetches, including files restored from earlier revisions.

The plan is sorted by (path, size descending), as git's own packer does. On
real, merge-heavy histories the traversal order interleaves branches, so
"consecutive" versions of a directory differ a lot; size order brings near
versions together and lets bases fan out. Fingerprint collisions can reduce
compression quality but cannot change reconstructed bytes.

## Deltas

Clients advertising `ofs-delta` receive tree and blob deltas against an earlier
entry in the same pack. Each object tries the previous `WINDOW` (10) objects of
its path, newest first, and keeps the smallest delta, with git's depth-scaled
size limit (a deeper base must beat the current best proportionally) so chains
fan out instead of running to the cap. Chains are capped at depth 50. A delta
must be below half the target size. Kinds are checked, including
file/directory transitions. The encoder builds one block index per base (sized
to the base, at most 2^20 slots, high bits of a mixed rolling hash), reuses it
for every target, and extends matches backwards and forwards. No REF_DELTA or
external base is emitted, even when `thin-pack` is offered. Clients without
`ofs-delta` receive full objects. Entries are zlib level 6 (git's default).

## Streaming and memory

Store reads (128 trees/commits or 16 blobs per `get_many`, 8 in flight) feed a
segmenter that cuts the stream into segments (at a path change after 1 MiB,
or inside a path at 16 MiB). Each segment is delta-searched and compressed by
its own blocking worker (up to 8 at once); one writer emits segments in order,
resolves OFS distances, hashes the pack and frames sidebands (65,515 payload
bytes). The bounded queue between segmenter and writer provides backpressure.
A cut inside a path hands the last window of objects to the next segment as
delta candidates; the earlier segment keeps those objects at most 25 deep and
the next one assumes that bound, so the depth cap holds without coordination.
Missing objects and disconnects stop the pipeline; failed reads cannot produce
a successful pack trailer/flush.

Memory is bounded by these constants (reads in flight, about ten segments,
windows of at most 16 MiB each), not by history size; no complete pack or
history of object bodies is retained. Planning memory is O(number of distinct
objects): 48 bytes per planned object plus transient reachability sets.

The S3 packed store coalesces nearby objects of one pack into single ranged
reads and bounds range bytes in flight per store (`ant-store/src/packed.rs`):
SeaweedFS answers 429 above 64 MiB of in-flight chunk reads, and the SDK then
backs off for seconds.

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
machine shared with other work; native = `git clone file://` of the SHA-1
source, master only, reusing its on-disk deltas):

| clone | before | after | native git |
|---|---:|---:|---:|
| `--filter=blob:none --no-checkout` time | 48.3 s | 13.9-15.6 s | 5.8 s |
| ... `.git` size | 905 MiB | 116 MiB | 115 MiB |
| ... trees packed | 899 MB (50k full) | 72.8 MB (6.7k full) | 98.8 MB (SHA-256 `repack -adf`) |
| ... server peak RSS | 177 MiB | 505-534 MiB | |
| `--filter=blob:none` with checkout | 48.7 s, 1 lazy fetch | 14.9-31 s, 1 lazy fetch | 8.8 s |
| `--depth 1` | 0.73 s | 0.54 s | 1.6 s |
| full clone | 99 s, 1.45 GiB | 34-47 s, 252 MiB | 17.8 s, 311 MiB |

Of the blobless time, about 6 s is planning (reading all 164k trees, 3.3 GB
raw, once) and 7 s the pack phase (reading them again for delta search); the
S3 server (SeaweedFS, ~10 cores busy) is the limit in both. The trees cannot
be kept from the first pass without holding the history in memory.
