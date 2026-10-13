//! Command-line definition (clap derive).

use std::path::PathBuf;

use ant_server::config::StoreArgs;
use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "ant", about = "antimony source control client", version)]
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

    /// Start workspace discovery in this directory instead of the cwd.
    #[arg(short = 'C', global = true, value_name = "DIR")]
    pub directory: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Import a git repository straight into the stores.
    Import(ImportArgs),
    /// List a tree (or show a single blob entry): <rev>[:<path>].
    Ls { target: String },
    /// Print a blob: <rev>:<path>.
    Cat { target: String },
    /// First-parent history, newest first.
    Log {
        #[arg(default_value = "main")]
        rev: String,
        /// Maximum number of commits.
        #[arg(short = 'n', long)]
        limit: Option<u32>,
        /// Only commits touching this path.
        #[arg(long)]
        path: Option<String>,
    },
    /// Per-line attribution of a file: <rev>:<path>.
    Blame { target: String },
    /// Changed paths between two revs.
    Diff { from: String, to: String },
    /// Read and update refs.
    Ref {
        #[command(subcommand)]
        cmd: RefCmd,
    },
    /// Sparse workspaces.
    Ws {
        #[command(subcommand)]
        cmd: WsCmd,
    },
    /// Show local changes in the workspace.
    Status,
    /// Upload local changes as a changeset.
    Submit {
        #[arg(short = 'm', long)]
        message: String,
    },
    /// Land a commit onto a ref.
    Land {
        #[arg(long = "ref", default_value = "main")]
        ref_name: String,
        /// Commit to land (default: the last submitted commit).
        commit: Option<String>,
    },
    /// Mount a read-only FUSE view of a rev.
    Mount { rev: String, mountpoint: PathBuf },
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
    #[command(flatten)]
    pub store: StoreArgs,
}

#[derive(Subcommand, Debug)]
pub enum RefCmd {
    Get {
        name: String,
    },
    List {
        #[arg(default_value = "")]
        prefix: String,
    },
    Set {
        name: String,
        rev: String,
        /// Expected current target (CAS).
        #[arg(long, conflicts_with = "create")]
        expect: Option<String>,
        /// Require that the ref does not exist yet.
        #[arg(long)]
        create: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum WsCmd {
    /// Create a (sparse) workspace.
    New {
        dir: PathBuf,
        #[arg(long, default_value = "main")]
        at: String,
        /// Sparse pattern (repeatable). No patterns = everything.
        #[arg(long)]
        sparse: Vec<String>,
    },
}
