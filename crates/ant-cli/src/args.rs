//! Command-line definition (clap derive).

use std::path::PathBuf;

use ant_server::config::StoreArgs;
use clap::{Args, Parser, Subcommand};

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
    /// First-parent history, newest first.
    Log {
        /// Rev to start from.
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
    Blame {
        /// `<rev>:<path>`.
        target: String,
    },
    /// Changed paths between two revs.
    Diff {
        /// Older rev.
        from: String,
        /// Newer rev.
        to: String,
    },
    /// Read and update refs.
    Ref {
        #[command(subcommand)]
        cmd: RefCmd,
    },
    /// Create and manage workspaces (`ws new`, `ws update`, `ws resolve`, `ws unmount`).
    Ws {
        #[command(subcommand)]
        cmd: WsCmd,
    },
    /// Show local changes in the workspace.
    Status,
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
    Serve(ant_server::serve::ServeArgs),
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
}

#[derive(Subcommand, Debug)]
pub enum AdminCmd {
    /// Tag the objects of a rev's tree with their access-control zones (run
    /// after imports and after changing the policy's path rules).
    ReindexAcl(ReindexArgs),
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
