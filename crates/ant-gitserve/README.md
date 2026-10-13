# Git pack export

The fetch planner keeps object IDs and 64-bit path fingerprints, deduplicates
before enqueueing trees, and stably groups objects by path. Revisions at a path
retain their traversal order (newest first for linear histories). Fingerprint
collisions can reduce compression quality but cannot change reconstructed bytes.
Shared objects are sent once. Known historical objects are still subtracted for
incremental fetches, including files restored from earlier revisions.

Clients advertising `ofs-delta` receive tree and blob deltas against an earlier
entry in the same pack. The encoder indexes 16-byte base blocks in a fixed 256 KiB
hash table and scans the target with a rolling hash. It accepts instructions only
below half the target's uncompressed size, caps chains at 50, and resets to a full
object at the limit. It checks object kinds, including file/directory transitions.
No REF_DELTA or external base is emitted, even when `thin-pack` is offered.
Clients without `ofs-delta` receive full objects.

A blocking worker performs matching, zlib compression, pack hashing, and sideband
framing. A one-batch channel overlaps asynchronous `get_many` reads (16 objects
per batch) with compression and sending. The existing bounded HTTP channel
provides backpressure. Sideband buffering is capped at 65,515 payload bytes;
only one previous base body is retained, capped at 8 MiB. Larger objects are
stream-compressed without delta matching. Missing objects and disconnects stop
the pipeline; failed reads cannot produce a successful pack trailer/flush.

ObjectStore returns whole bodies, so read memory is bounded by three batches
(one being compressed, one queued, one being fetched), not by a strict byte
limit independent of individual object size. Planning reads at most 512 trees
per batch and moves tree parsing and final sorting off async executors. Planning
memory remains O(number of distinct objects): final ID/path arrays use 40 bytes
per object (400 MB for 10 million objects), with transient reachability sets,
queues, and sort/unzip allocations. This is not a constant-memory graph walk.
No complete pack or complete history of object bodies is retained by the export.

## Reproducible measurements

Run offline in the checkout's default `target`:

```
cargo nextest run --offline -p ant-gitserve
cargo clippy --offline -p ant-gitserve --all-targets -- -D warnings
```

`large_blobless_history_is_delta_compressed` clones 2,000 commits with 2,000-entry
trees using real Git over HTTP, measures the received `.pack`, checks it is at
most 30% of the old undeltified encoding, and runs `git fsck --full`.

`synthetic_history_pack_size_time_and_peak_rss` uses the same fixture, planner,
encoder, and real Git `index-pack` over stdin. It runs with deltas disabled and
enabled and checks both packs with `git fsck --full` in a promisor repository.
This also works where listening sockets are prohibited. The undeltified size is
independently calculated using the old encoder's header sizes and zlib settings.
RSS is sampled every 5 ms from `/proc/self/status` on Linux and includes the
in-memory fixture store; the test reports pre-transfer RSS separately.

A local debug-profile run on 2026-10-14 produced:

| Measurement | Deltas disabled | Deltas enabled |
|---|---:|---:|
| Pack bytes | 145,578,540 | 3,379,899 |
| Planning | 2.54 s | 2.58 s |
| Planning + transfer + Git indexing | 4.45 s | 3.83 s |
| Server RSS before transfer | 197,472 KiB | 197,912 KiB |
| Peak server RSS | 199,232 KiB | 198,712 KiB |

The delta pack is 2.32% of the undeltified size. These are controlled comparisons
using the new pipeline in both modes, not timings of the old HTTP server. Git's
child-process memory is not included in the server RSS. This sandbox rejects
localhost socket binding, so HTTP clone timings and the full HTTP suite could
not be validated here. `RUSTC_WRAPPER=` was needed because the sandbox also
rejects sccache; builds still used the local `./target` and `--offline`.
