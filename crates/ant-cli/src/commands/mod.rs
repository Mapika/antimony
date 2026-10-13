//! Subcommand implementations.

use anyhow::Result;

use crate::args::{AdminCmd, Cli, Command, RefCmd, WsCmd};
use crate::ctx::{Ctx, Outcome};

mod admin;
mod import;
mod mount;
mod query;
mod refs;
mod ws;

pub async fn run(cli: Cli) -> Result<Outcome> {
    let ctx = Ctx::from_cli(&cli)?;
    match cli.command {
        Command::Import(a) => import::run(a).await,
        Command::Ls { target } => query::ls(&ctx, &target).await,
        Command::Cat { target } => query::cat(&ctx, &target).await,
        Command::Blame { target } => query::blame(&ctx, &target).await,
        Command::Log { rev, limit, path } => query::log(&ctx, &rev, limit, path).await,
        Command::Diff { from, to } => query::diff(&ctx, &from, &to).await,
        Command::Ref { cmd } => match cmd {
            RefCmd::Get { name } => refs::get(&ctx, &name).await,
            RefCmd::List { prefix } => refs::list(&ctx, &prefix).await,
            RefCmd::Set { name, rev, expect, create } => refs::set(&ctx, &name, &rev, expect, create).await,
        },
        Command::Ws { cmd: WsCmd::New { dir, at, sparse } } => ws::new(&ctx, &dir, &at, sparse).await,
        Command::Status => ws::status(&ctx).await,
        Command::Submit { message } => ws::submit(&ctx, &message).await,
        Command::Land { ref_name, commit } => ws::land(&ctx, &ref_name, commit).await,
        Command::Mount { rev, mountpoint } => mount::run(&ctx, &rev, &mountpoint).await,
        Command::Admin { cmd: AdminCmd::ReindexAcl(a) } => admin::reindex_acl(a).await,
    }
}
