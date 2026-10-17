# Contributing to antimony

Server-centric source control for agent fleets on large monorepos. Git object
format (SHA-256), but server-side commits, no packfiles, lazy/sparse clients.
See [README.md](README.md) for the user-facing overview and
[docs/architecture.md](docs/architecture.md) for how it works.

Bug reports, questions and pull requests are welcome. For a larger change,
open an issue first so the design can be discussed before the code.

## Layout

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

Docs: `docs/*.md` are the user and operator documentation; the
[deployment kit](deploy/README.md) lives in `deploy/`. Run
`scripts/check-md-links.py` after moving or renaming Markdown: it checks
every relative link and anchor.

## Conventions

- **Tests are the spec.** Behaviour is pinned by unit tests next to the code,
  integration tests under `crates/*/tests`, and property tests
  (`land_props`, `changeset_props`, `batch_equivalence`). Never weaken,
  delete or `#[ignore]` a test to make a change pass; add tests for the edge
  cases you handle.
- **Compatibility surfaces**: `proto/ant.proto`, the Postgres schema, the
  on-disk cache/workspace formats and the CLI output contract (module docs of
  `ant-cli/src/main.rs`) are relied on by deployed clients and data. Change
  them only deliberately, with migrations or versioned kinds; see
  [docs/compatibility.md](docs/compatibility.md).
- **Errors**, one style per layer: `ant-store` returns `StoreError`, server
  operations return `OpError` (mapped to gRPC status codes in `rpc`), binaries
  and tools use `anyhow` with context; the CLI prints `error: ...` plus an
  optional `hint: ...`.
- **Performance**: batch store calls (`get_many`/`put_many`); never one round
  trip per object in a loop; no needless clones of large buffers; no blocking
  I/O on async threads (`spawn_blocking`). The land and changeset hot paths
  are covered by equivalence properties — keep them passing.
- No `unwrap()`/`expect()` on fallible runtime paths outside tests; no
  `todo!()`/`unimplemented!()`.
- Third-party dependency versions live in the root `Cargo.toml`
  (`[workspace.dependencies]`); crates opt in with `{ workspace = true }`.
- Formatting is `cargo fmt --all` (`rustfmt.toml`); CI rejects unformatted
  code and clippy warnings.
- Changes users can see go into the `Unreleased` section of
  [CHANGELOG.md](CHANGELOG.md).

## Development loop

- **Build into the checkout's own `./target`** (the default). Compiles go
  through `scripts/rustc-wrapper.sh`, which uses
  [sccache](https://github.com/mozilla/sccache) when it is installed (plain
  `rustc` otherwise); sccache shares compiled dependencies between several
  checkouts or worktrees on one machine when each builds into its own
  `./target`, so do not point `CARGO_TARGET_DIR` elsewhere or copy target
  directories.
- **Run tests with [nextest](https://nexte.st)** (parallel processes;
  `cargo test` runs test binaries one after another):
  - inner loop: `cargo nextest run -p <crate>` (or `-E 'test(name)'`)
  - fast tier, no services: `cargo nextest run --workspace`
  - full tier (needs the services below) before sending a change:
    `cargo nextest run --workspace --run-ignored all -E 'not test(history_bench_postgres)'`
  - property tests deeper: `PROPTEST_CASES=1000 cargo nextest run -p ant-server --run-ignored all -E 'binary(land_props) | binary(changeset_props) | binary(acl) | test(batch_equivalence)'`
  - the FUSE tests (`ant-fuse`, `cli_lazy_end_to_end`) need `/dev/fuse` and
    `fusermount3` (package `fuse3`).
- `cargo check` / `cargo clippy --workspace --all-targets -- -D warnings`
  while iterating; full builds only to run tests.
- **Benchmarks** run under `scripts/bench-lock.sh`, a machine-wide lock, so
  that concurrent checkouts never measure on top of each other: prefix every
  timed run (fleet, soak, import, checkout, clone timings) with it. Builds
  and tests don't take the lock. Don't wait for anything while holding it.
- Quick load check (in memory):
  `cargo run --release -p ant-bench -- fleet --embedded --memory --agents 16 --duration 10 --hot-rate 0.05`.
- Multi-replica soak (Postgres + S3, spawns N `serve` processes, drops its
  `soak_*` schema and S3 prefix afterwards):
  `cargo run --release -p ant-bench -- soak --replicas 2 --agents 300 --duration 600 --pg-url postgres://ant:ant@localhost:54329/ant --s3-endpoint http://localhost:9009`
- `scripts/upgrade-test.sh` checks the upgrade from the previous release to
  the checkout on real Postgres + S3 (in place and rolling, client skew, the
  refused downgrade); run it when a change touches a compatibility surface.

## Services for tests

The full tier needs Postgres on `localhost:54329` (user, password and
database `ant`) and S3 (SeaweedFS) on `localhost:9009` (any credentials).
Either:

- `scripts/dev-services.sh start` — native user processes, no Docker
  (`install`, `stop`, `status`, `wipe`); data in
  `~/.local/share/antimony-services`; or
- `docker compose up -d` — same ports (used by CI).

**Test data cleanup.** Tests must not leave scratch data behind. Postgres
schemas come from `ant_store::testing::scratch_schema(label)` (named
`t_<label>_<unix secs>_<random>`) and are dropped by `PgStore::connect_scratch`
or a `SchemaGuard`; S3 data lives under a per-run prefix of the shared
`ant-test` bucket (`S3Config::for_tests(label)` -> `<label>-t<unix secs>-<random>`)
and is deleted by `S3ObjectStore::connect_scratch` or an `S3PrefixGuard`.
Drops run on drop, also on panic. A killed run (SIGKILL, OOM, nextest
timeout) cannot clean up, so the first `scratch_schema`/`for_tests` call of
every test process sweeps names older than 6 h (`ANT_TEST_SWEEP_HOURS`), and
`scripts/clean-test-data.sh [--hours N|--all] [--drop-bucket]` does it by
hand. New tests: never invent schema or prefix names yourself.

**Scale runs.** Check free disk space before large imports and clean up
schemas and buckets afterwards. `ant-bench soak` and `scripts/upgrade-test.sh`
refuse to start while a `DISK_LOW` file exists in the services directory, so
a disk watchdog can stop new runs by creating it. Use public repositories
(git/git is a good size) or synthetic histories as corpora. Benchmarks
should write to their own bucket and delete the whole bucket afterwards:
SeaweedFS frees space only for deleted buckets, not for deleted objects
(`printf 's3.bucket.delete -name <bucket>\n' | ~/.local/share/antimony-services/bin/weed shell -master=127.0.0.1:19333`
with the native services).

## License

By contributing you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE), the license of the project.
