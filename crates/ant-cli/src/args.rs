//! Command-line definition (clap derive).

use std::path::PathBuf;
use std::time::Duration;

use ant_server::config::StoreArgs;
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "ant", about = "antimony source control client", version = env!("ANT_VERSION"))]
pub struct Cli {
    /// gRPC url of the antimony server.
    #[arg(long, global = true, env = "ANT_SERVER", default_value = "http://127.0.0.1:7420")]
    pub server: String,

    /// Optional HTTP object base url (e.g. http://127.0.0.1:7421).
    #[arg(long, global = true, env = "ANT_HTTP")]
    pub http: Option<String>,

    /// Local object cache directory (default ~/.cache/antimony).
    #[arg(long, global = true, env = "ANT_CACHE")]
    pub cache: Option<PathBuf>,

    /// Maximum local cache size in bytes.
    #[arg(long, global = true, env = "ANT_CACHE_BYTES", default_value_t = 20 * 1024 * 1024 * 1024)]
    pub cache_bytes: u64,

    /// Bearer token for servers running with an access policy.
    #[arg(long, global = true, env = "ANT_TOKEN", hide_env_values = true)]
    pub token: Option<String>,

    /// Start workspace discovery in this directory instead of the cwd.
    #[arg(short = 'C', global = true, value_name = "DIR")]
    pub directory: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

/// Client-only global flags (they configure the connection to a server).
const CLIENT_GLOBALS: [&str; 6] = ["server", "http", "cache", "cache_bytes", "token", "directory"];

/// Subcommands that run against the stores directly and never talk to a
/// server: they still accept the client globals (existing command lines keep
/// working), but their `--help` does not list them.
const LOCAL_COMMANDS: [&str; 4] = ["serve", "import", "admin", "completions"];

fn hide_client_globals(sub: clap::Command) -> clap::Command {
    // `mut_args` keeps the argument order (re-pushing one breaks `--help`).
    let mut sub = sub
        .mut_args(|a| if CLIENT_GLOBALS.contains(&a.get_id().as_str()) { a.hide(true) } else { a });
    let nested: Vec<String> = sub.get_subcommands().map(|s| s.get_name().to_string()).collect();
    for name in nested {
        sub = sub.mut_subcommand(name, hide_client_globals);
    }
    sub
}

/// The `ant` command line, with client-only globals hidden from the help of
/// server/admin subcommands.
pub fn command() -> clap::Command {
    let mut cmd = Cli::command();
    // Propagate the global args into the subcommands so they can be hidden.
    cmd.build();
    for name in LOCAL_COMMANDS {
        cmd = cmd.mut_subcommand(name, hide_client_globals);
    }
    // `cache-serve` uses the cache flags but is no client of a server.
    cmd = cmd.mut_subcommand("cache-serve", |sub| {
        sub.mut_args(|a| {
            if ["server", "http", "token", "directory"].contains(&a.get_id().as_str()) {
                a.hide(true)
            } else {
                a
            }
        })
    });
    cmd
}

/// Parse the process arguments (exits with usage on error, like clap).
pub fn parse() -> Cli {
    let mut matches = command().get_matches();
    Cli::from_arg_matches_mut(&mut matches).unwrap_or_else(|e| e.exit())
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Import a git repository straight into the stores.
    Import(ImportArgs),
    /// List a tree (or show a single blob entry): <rev>[:<path>].
    Ls {
        /// `<rev>` or `<rev>:<path>`, e.g. `main:src`.
        target: String,
    },
    /// Print a blob: <rev>:<path>.
    Cat {
        /// `<rev>:<path>`, e.g. `main:src/lib.rs`.
        target: String,
    },
    /// First-parent history, newest first: of a rev, or of a range `<a>..<b>`
    /// (commits of `<b>` not reachable from `<a>`).
    Log {
        /// Rev to start from, or `<a>..<b>`.
        #[arg(default_value = "main")]
        rev: String,
        /// Maximum number of commits.
        #[arg(short = 'n', long)]
        limit: Option<u32>,
        /// Only commits touching this path.
        #[arg(long)]
        path: Option<String>,
        /// Only commits touching this path (same as --path).
        #[arg(last = true, value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Per-line attribution of a file: <rev>:<path>.
    Blame {
        /// `<rev>:<path>`.
        target: String,
    },
    /// Changed paths between two revs, or (`--patch`) their line diffs.
    Diff {
        /// Older rev.
        from: String,
        /// Newer rev.
        to: String,
        /// Print a unified diff (computed by the server) instead of paths.
        #[arg(long)]
        patch: bool,
        /// Diff from the merge base of the two revs (like `a...b`).
        #[arg(long)]
        merge_base: bool,
        /// Context lines around changes (with --patch).
        #[arg(short = 'U', long = "unified", value_name = "N", requires = "patch")]
        context: Option<u32>,
        /// Only changes at or under these paths.
        #[arg(last = true, value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Best common ancestor of two revs.
    MergeBase {
        /// A rev.
        a: String,
        /// Another rev.
        b: String,
    },
    /// Whether (and as which commit) a commit was landed onto a ref.
    LandedAs {
        /// The commit that was landed (as submitted).
        commit: String,
        /// Ref to look in (default: every ref, in name order, at most 64).
        #[arg(long = "ref")]
        ref_name: Option<String>,
    },
    /// Read and update refs.
    Ref {
        #[command(subcommand)]
        cmd: RefCmd,
    },
    /// Create and manage workspaces (`ws new`, `ws update`, `ws resolve`, `ws unmount`, `ws remount`).
    Ws {
        #[command(subcommand)]
        cmd: WsCmd,
    },
    /// Show local changes in the workspace.
    Status {
        /// Also list untracked paths hidden by .gitignore / .ant/ignore, marked `!`.
        #[arg(long)]
        ignored: bool,
    },
    /// Upload local changes as a changeset.
    Submit {
        /// Commit message.
        #[arg(short = 'm', long)]
        message: String,
    },
    /// Land a commit onto a ref.
    Land {
        /// Ref to land onto.
        #[arg(long = "ref", default_value = "main")]
        ref_name: String,
        /// Commit to land (default: the last submitted commit).
        commit: Option<String>,
    },
    /// Mount a read-only FUSE view of a rev.
    Mount {
        /// Rev to show.
        rev: String,
        /// Existing empty directory; the view is read-only.
        mountpoint: PathBuf,
    },
    /// Run the antimony server (gRPC + HTTP object endpoint).
    Serve(Box<ant_server::serve::ServeArgs>),
    /// A local-mode server (`ant serve --local`) in the background
    /// (`local start`, `local stop`, `local status`).
    Local {
        #[command(subcommand)]
        cmd: LocalCmd,
    },
    /// Print a shell completion script.
    Completions {
        /// Target shell.
        shell: clap_complete::Shell,
    },
    /// Administrative commands working directly on the stores.
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
    /// Inspect, trim and seed the local object cache (`cache stats`, `cache gc`, `cache seed`).
    Cache {
        #[command(subcommand)]
        cmd: CacheCmd,
    },
    /// Run a cache tier: serve the server's HTTP object surface from a local
    /// disk cache (`--cache`, `--cache-bytes`), filling misses from `--upstream`.
    CacheServe(CacheServeArgs),
}

#[derive(Args, Debug)]
pub struct CacheServeArgs {
    /// Address to serve on.
    #[arg(long, env = "ANT_CACHE_LISTEN", default_value = "127.0.0.1:7422")]
    pub listen: std::net::SocketAddr,
    /// HTTP base url of the upstream: the origin's `--http-listen` address or
    /// another `cache-serve` (e.g. http://origin:7421).
    #[arg(long, env = "ANT_CACHE_UPSTREAM")]
    pub upstream: String,
    /// How reads are authorized. `forward` (default): every request is
    /// checked with the upstream using the caller's own credentials, so the
    /// cache never serves what the origin would refuse. `open`: the upstream
    /// enforces no access policy (checked at startup); serve everyone from
    /// disk.
    #[arg(long, value_enum, env = "ANT_CACHE_AUTH", default_value = "forward")]
    pub auth: CacheAuth,
    /// With `--auth forward`: seconds an allowed read decision for a
    /// principal is reused before the origin is asked again (0: always ask).
    /// Bounds how long a revoked principal can still read objects this cache
    /// holds.
    #[arg(long, env = "ANT_CACHE_AUTHZ_TTL", default_value_t = 30)]
    pub authz_ttl_secs: u64,
    /// With `--auth forward`: milliseconds permission checks wait to be
    /// batched into one request to the origin (`POST /o/check`).
    #[arg(long, env = "ANT_CACHE_AUTHZ_WINDOW_MS", default_value_t = 2)]
    pub authz_window_ms: u64,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheAuth {
    Forward,
    Open,
}

#[derive(Subcommand, Debug)]
pub enum CacheCmd {
    /// Objects and bytes in the cache, and its budget.
    Stats {
        /// Also print the per-tier counters recorded in this cache directory
        /// (where objects came from: disk, other processes, bundles, fetches).
        #[arg(long)]
        tiers: bool,
    },
    /// Fetch a tree (a rev, optionally `<rev>:<path>`) into the cache without
    /// writing files, so later checkouts on this node are cache hits.
    Seed {
        /// `<rev>` or `<rev>:<path>`.
        target: String,
        /// Sparse pattern (repeatable), as for `ws new --sparse`. No patterns
        /// = the whole tree.
        #[arg(long)]
        sparse: Vec<String>,
    },
    /// Evict least-recently-used objects: down to 90% of the budget if over
    /// it, or down to `--to` bytes.
    Gc {
        /// Evict until at most this many bytes remain, whatever the budget.
        #[arg(long, value_name = "BYTES")]
        to: Option<u64>,
    },
}

#[derive(Subcommand, Debug)]
pub enum AdminCmd {
    /// Tag the objects of a rev's tree with their access-control zones (run
    /// after imports and after changing the policy's path rules).
    ReindexAcl(ReindexArgs),
    /// Show the Postgres schema version and pending migrations; `--migrate`
    /// applies them.
    Schema(SchemaArgs),
    /// Delete data nothing can reach any more (garbage collection): objects,
    /// commit records and S3 packs of aborted lands, abandoned changesets
    /// and failed imports. Safe while servers run; interrupted runs resume.
    /// With `--local`: run by the server using the directory, else offline.
    Gc(GcArgs),
    /// Copy a local data directory (`--local`) into the stores given by the
    /// store flags, e.g. Postgres + S3: objects, commits, changesets,
    /// derived data, then the refs the destination lacks. Offline (no
    /// server may use the directory); idempotent.
    CopyLocal(CopyLocalArgs),
    /// Rewrite stored objects with deltas chosen across the whole history
    /// (git-gc-style), keeping recent revisions one decode away. Postgres +
    /// S3 stores, or `--local` (run by the server using the directory, else
    /// offline); safe while servers run; run again to resume.
    Repack(RepackArgs),
}

#[derive(Args, Debug)]
pub struct RepackArgs {
    /// Read and compress everything, write nothing: report the savings.
    #[arg(long)]
    pub dry_run: bool,
    /// Leave rows written within this period as they are (`0` repacks
    /// everything, e.g. right after an import).
    #[arg(long, default_value = "1d", value_parser = parse_duration)]
    pub min_age: Duration,
    /// First-parent commits per ref whose trees stay one decode away (not
    /// delta-compressed).
    #[arg(long, default_value_t = ant_store::postgres::repack::DEFAULT_KEEP_RECENT)]
    pub keep_recent: usize,
    /// Also rewrite objects an earlier repack wrote (a full repack).
    #[arg(long)]
    pub all: bool,
    /// Leave objects stored in Postgres where they are.
    #[arg(long)]
    pub no_small: bool,
    /// Objects tried as delta bases for each object.
    #[arg(long, default_value_t = 10)]
    pub window: usize,
    /// Longest delta chain (0: no deltas).
    #[arg(long, default_value_t = 16)]
    pub depth: u32,
    /// Uncompressed bytes of one record (what reading one object
    /// decompresses at most).
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    pub record_bytes: usize,
    /// zstd level of the new records (1..=22).
    #[arg(long, default_value_t = 19, value_parser = clap::value_parser!(i32).range(1..=22))]
    pub level: i32,
    /// Parallel jobs (default: CPU count, at most 8).
    #[arg(long)]
    pub jobs: Option<usize>,
    /// Stop after this many packs (run again to continue).
    #[arg(long)]
    pub max_packs: Option<usize>,
    #[command(flatten)]
    pub store: StoreArgs,
}

#[derive(Args, Debug)]
pub struct GcArgs {
    /// Count what would be deleted; change nothing.
    #[arg(long)]
    pub dry_run: bool,
    /// Keep everything written within this period, with all it reaches
    /// (unlanded changesets, uploads, imports in progress). `7d`, `36h`, ...;
    /// at least `1d`.
    #[arg(long, default_value = "7d", value_parser = parse_duration)]
    pub grace: Duration,
    /// Wait between condemning data and deleting it. Must exceed the
    /// servers' cache lifetime (10m) plus their longest request (local
    /// mode: skipped when no server uses the directory).
    #[arg(long, default_value = "30m", value_parser = parse_duration)]
    pub quiesce: Duration,
    /// Stop after this many delete batches (run again to resume).
    #[arg(long)]
    pub max_batches: Option<usize>,
    /// Forget an interrupted run (its condemned data becomes visible again)
    /// instead of resuming it.
    #[arg(long, conflicts_with = "dry_run")]
    pub abort: bool,
    #[command(flatten)]
    pub store: StoreArgs,
}

/// `<n>[s|m|h|d]` (no unit: seconds).
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = match s.find(|c: char| !c.is_ascii_digit() && c != '.') {
        Some(i) => s.split_at(i),
        None => (s, "s"),
    };
    let n: f64 = num.parse().map_err(|_| format!("invalid duration {s:?} (e.g. 7d, 36h, 30m)"))?;
    let scale = match unit {
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        "d" => 86400.0,
        _ => return Err(format!("invalid duration unit in {s:?} (use s, m, h or d)")),
    };
    Duration::try_from_secs_f64(n * scale).map_err(|e| format!("invalid duration {s:?}: {e}"))
}

#[derive(Args, Debug)]
pub struct SchemaArgs {
    /// Apply pending migrations (what `ant serve` and `ant import` do on
    /// start unless `--no-migrate`).
    #[arg(long)]
    pub migrate: bool,
    #[command(flatten)]
    pub store: StoreArgs,
}

#[derive(Args, Debug)]
pub struct CopyLocalArgs {
    /// The local data directory to copy (default: `$ANT_LOCAL_DIR`, else
    /// `~/.local/share/antimony/local`).
    #[arg(long)]
    pub from: Option<PathBuf>,
    /// Destination store flags.
    #[command(flatten)]
    pub store: StoreArgs,
}

#[derive(Args, Debug)]
pub struct ReindexArgs {
    /// Revision whose tree to index (ref, id, `~N`).
    pub rev: String,
    /// Access-control policy file.
    #[arg(long, env = "ANT_POLICY")]
    pub policy: PathBuf,
    /// Re-walk every subtree, ignoring markers from earlier runs.
    #[arg(long)]
    pub full: bool,
    /// Also index every ancestor commit (older versions of restricted files).
    #[arg(long)]
    pub history: bool,
    #[command(flatten)]
    pub store: StoreArgs,
}

#[derive(Args, Debug)]
pub struct ImportArgs {
    /// Git directory (or work tree) to import.
    pub git_dir: PathBuf,
    /// Git ref to import.
    #[arg(long, default_value = "refs/heads/main")]
    pub git_ref: String,
    /// antimony ref to create/advance.
    #[arg(long = "ref", default_value = "main")]
    pub ref_name: String,
    /// Access-control policy: zone-tag the imported history
    /// (`admin reindex-acl --history`).
    #[arg(long, env = "ANT_POLICY")]
    pub policy: Option<PathBuf>,
    #[command(flatten)]
    pub store: StoreArgs,
}

#[derive(Subcommand, Debug)]
pub enum RefCmd {
    /// Print the commit id a ref points at.
    Get {
        /// Ref name, e.g. `main`.
        name: String,
    },
    /// List refs as `<id> <name>`, optionally only those under a prefix.
    List {
        /// Only refs whose name starts with this prefix.
        #[arg(default_value = "")]
        prefix: String,
    },
    /// Create a ref at a new empty-tree root commit: the first commit of an
    /// empty server. Refused if the ref exists.
    Init {
        /// Ref to create.
        #[arg(default_value = "main")]
        name: String,
        /// Commit message.
        #[arg(short = 'm', long, default_value = "initial commit")]
        message: String,
    },
    /// Move a ref to a rev (compare-and-swap; `--create` for a new ref).
    /// Moves that are not fast-forwards need `--force`.
    Set {
        /// Ref to update.
        name: String,
        /// New target: a ref, commit id, or `~N` expression.
        rev: String,
        /// Expected current target (CAS).
        #[arg(long, conflicts_with = "create")]
        expect: Option<String>,
        /// Require that the ref does not exist yet.
        #[arg(long)]
        create: bool,
        /// Allow a move to a commit that does not descend from the current
        /// (or expected) target, e.g. a rewind.
        #[arg(long)]
        force: bool,
    },
    /// Follow a ref's moves (lands, updates, force moves) as they happen,
    /// one line per event, until interrupted.
    Watch {
        /// Ref to watch.
        name: String,
        /// First catch up from this rev to the current tip.
        #[arg(long)]
        from: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum WsCmd {
    /// Move the workspace to a new base, carrying local edits over
    /// (3-way merging files changed on both sides).
    Update {
        /// Rev to move to (default: the ref the workspace was created at,
        /// else `main`).
        #[arg(long)]
        to: Option<String>,
    },
    /// Mark conflicted paths (as `ant status` prints them) resolved, once
    /// their conflict markers are gone.
    Resolve {
        /// Conflicted repo paths.
        #[arg(required = true)]
        paths: Vec<String>,
    },
    /// Unmount a lazy workspace, keeping its edits.
    Unmount {
        /// The lazy workspace directory.
        dir: PathBuf,
    },
    /// Remount a lazy workspace whose FUSE daemon died (or that was
    /// unmounted), recovering its edits. Default: the workspace found from
    /// the cwd / `-C`.
    Remount {
        /// The lazy workspace directory.
        dir: Option<PathBuf>,
    },
    #[command(hide = true)]
    Serve { dir: PathBuf, state: PathBuf },
    /// Create a (sparse) workspace.
    New {
        /// Directory to create the workspace in (must be empty or absent).
        dir: PathBuf,
        /// Rev to base the workspace on.
        #[arg(long, default_value = "main")]
        at: String,
        /// Sparse pattern (repeatable). No patterns = everything.
        #[arg(long)]
        sparse: Vec<String>,
        /// Mount a writable lazy workspace in a detached process.
        #[arg(long, conflicts_with = "sparse")]
        lazy: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("7d"), Ok(Duration::from_secs(7 * 86400)));
        assert_eq!(parse_duration("36h"), Ok(Duration::from_secs(36 * 3600)));
        assert_eq!(parse_duration("30m"), Ok(Duration::from_secs(1800)));
        assert_eq!(parse_duration("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse_duration("90"), Ok(Duration::from_secs(90)));
        assert!(parse_duration("7w").is_err());
        assert!(parse_duration("d").is_err());
        assert!(parse_duration("-1d").is_err());
        let cli = Cli::try_parse_from(["ant", "admin", "gc", "--dry-run", "--memory"]).unwrap();
        let Command::Admin { cmd: AdminCmd::Gc(a) } = cli.command else { panic!() };
        assert!(a.dry_run && !a.abort);
        assert_eq!(
            (a.grace, a.quiesce),
            (Duration::from_secs(7 * 86400), Duration::from_secs(1800))
        );
        assert!(Cli::try_parse_from(["ant", "admin", "gc", "--dry-run", "--abort"]).is_err());
    }

    #[test]
    fn repack_flags_and_defaults() {
        let cli = Cli::try_parse_from(["ant", "admin", "repack", "--memory"]).unwrap();
        let Command::Admin { cmd: AdminCmd::Repack(a) } = cli.command else { panic!() };
        assert!(!a.dry_run && !a.all && !a.no_small);
        assert_eq!(a.min_age, Duration::from_secs(86400));
        assert_eq!((a.keep_recent, a.window, a.depth, a.level), (100, 10, 16, 19));
        assert_eq!((a.record_bytes, a.jobs, a.max_packs), (2 * 1024 * 1024, None, None));
        let cli = Cli::try_parse_from([
            "ant",
            "admin",
            "repack",
            "--dry-run",
            "--min-age",
            "0",
            "--all",
            "--depth",
            "0",
            "--max-packs",
            "3",
            "--memory",
        ])
        .unwrap();
        let Command::Admin { cmd: AdminCmd::Repack(a) } = cli.command else { panic!() };
        assert!(a.dry_run && a.all);
        assert_eq!((a.min_age, a.depth, a.max_packs), (Duration::ZERO, 0, Some(3)));
        assert!(Cli::try_parse_from(["ant", "admin", "repack", "--level", "23"]).is_err());
    }

    #[test]
    fn lazy_workspace_commands() {
        let cli = Cli::try_parse_from(["ant", "ws", "new", "--lazy", "/tmp/ws"]).unwrap();
        assert!(matches!(cli.command, Command::Ws { cmd: WsCmd::New { lazy: true, .. } }));
        assert!(
            Cli::try_parse_from(["ant", "ws", "new", "--lazy", "--sparse", "src", "/tmp/ws"])
                .is_err()
        );
        let cli = Cli::try_parse_from(["ant", "ws", "unmount", "/tmp/ws"]).unwrap();
        assert!(matches!(cli.command, Command::Ws { cmd: WsCmd::Unmount { .. } }));
    }

    #[test]
    fn serve_and_import_help_hide_client_globals() {
        for sub in ["serve", "import"] {
            let mut cmd = command();
            let help = cmd.find_subcommand_mut(sub).unwrap().render_long_help().to_string();
            for flag in ["--token", "--server", "--http <", "--cache <", "--cache-bytes", "-C <"] {
                assert!(!help.contains(flag), "ant {sub} --help mentions {flag}:\n{help}");
            }
        }
        let mut cmd = command();
        let top = cmd.render_long_help().to_string();
        assert!(top.contains("--token") && top.contains("--server"));
        // Still accepted where they were before.
        let cli = Cli::try_parse_from(["ant", "serve", "--memory", "--token", "x", "-C", "/"]);
        assert!(cli.is_ok(), "{cli:?}");
    }

    #[test]
    fn cache_tier_commands() {
        let cli =
            Cli::try_parse_from(["ant", "cache-serve", "--upstream", "http://o:7421"]).unwrap();
        let Command::CacheServe(a) = cli.command else { panic!() };
        assert_eq!(a.auth, CacheAuth::Forward);
        assert_eq!(a.listen.port(), 7422);
        assert!(Cli::try_parse_from(["ant", "cache-serve"]).is_err(), "--upstream is required");
        let cli = Cli::try_parse_from(["ant", "cache-serve", "--upstream", "x", "--auth", "open"])
            .unwrap();
        assert!(matches!(cli.command, Command::CacheServe(a) if a.auth == CacheAuth::Open));
        let mut cmd = command();
        let help = cmd.find_subcommand_mut("cache-serve").unwrap().render_long_help().to_string();
        assert!(help.contains("--cache <") && help.contains("--cache-bytes"), "{help}");
        for flag in ["--token", "--server", "--http <", "-C <"] {
            assert!(!help.contains(flag), "cache-serve --help mentions {flag}:\n{help}");
        }
        let cli = Cli::try_parse_from(["ant", "cache", "stats", "--tiers"]).unwrap();
        assert!(matches!(cli.command, Command::Cache { cmd: CacheCmd::Stats { tiers: true } }));
        let cli =
            Cli::try_parse_from(["ant", "cache", "seed", "main:src", "--sparse", "a"]).unwrap();
        assert!(
            matches!(cli.command, Command::Cache { cmd: CacheCmd::Seed { sparse, .. } } if sparse == ["a"])
        );
    }

    #[test]
    fn remount_and_ref_init() {
        let cli = Cli::try_parse_from(["ant", "ws", "remount"]).unwrap();
        assert!(matches!(cli.command, Command::Ws { cmd: WsCmd::Remount { dir: None } }));
        let cli = Cli::try_parse_from(["ant", "ref", "init"]).unwrap();
        assert!(
            matches!(cli.command, Command::Ref { cmd: RefCmd::Init { name, .. } } if name == "main")
        );
    }

    #[test]
    fn update_resolve_and_forced_ref_set() {
        let cli = Cli::try_parse_from(["ant", "ws", "update"]).unwrap();
        assert!(matches!(cli.command, Command::Ws { cmd: WsCmd::Update { to: None } }));
        let cli = Cli::try_parse_from(["ant", "ws", "update", "--to", "main~1"]).unwrap();
        assert!(
            matches!(cli.command, Command::Ws { cmd: WsCmd::Update { to: Some(t) } } if t == "main~1")
        );
        assert!(Cli::try_parse_from(["ant", "ws", "resolve"]).is_err());
        let cli = Cli::try_parse_from(["ant", "ws", "resolve", "a", "b/c"]).unwrap();
        assert!(
            matches!(cli.command, Command::Ws { cmd: WsCmd::Resolve { paths } } if paths.len() == 2)
        );
        let cli = Cli::try_parse_from(["ant", "ref", "set", "main", "x", "--force"]).unwrap();
        assert!(matches!(cli.command, Command::Ref { cmd: RefCmd::Set { force: true, .. } }));
    }
}

#[derive(Subcommand, Debug)]
pub enum LocalCmd {
    /// Start `ant serve --local <dir>` in the background (nothing to do when
    /// it already runs).
    Start(LocalStartArgs),
    /// Stop the background server of a data directory.
    Stop(LocalDirArgs),
    /// Whether a server uses the data directory.
    Status(LocalDirArgs),
}

#[derive(Args, Debug)]
pub struct LocalDirArgs {
    /// Data directory (default: `$ANT_LOCAL_DIR`, else
    /// `~/.local/share/antimony/local`).
    #[arg(long)]
    pub dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct LocalStartArgs {
    #[command(flatten)]
    pub dir: LocalDirArgs,
    /// gRPC listen address.
    #[arg(long, default_value = "127.0.0.1:7420")]
    pub grpc_listen: std::net::SocketAddr,
    /// HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:7421")]
    pub http_listen: std::net::SocketAddr,
    /// More `ant serve` flags, after `--` (e.g. `-- --policy policy.toml`).
    #[arg(last = true)]
    pub serve_args: Vec<String>,
}
