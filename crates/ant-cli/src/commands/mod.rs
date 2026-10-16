//! Subcommand implementations.

use anyhow::Result;

use crate::args::{AdminCmd, CacheCmd, Cli, Command, RefCmd, WsCmd};
use crate::ctx::{Ctx, Outcome};

mod admin;
mod cache;
mod cache_serve;
mod import;
mod mount;
mod query;
mod refs;
mod ws;

pub async fn run(cli: Cli) -> Result<Outcome> {
    let cli = match cli.command {
        Command::Serve(a) => return ant_server::serve::run(*a).await.map(|()| Outcome::empty()),
        Command::CacheServe(a) => {
            return cache_serve::run(a, cli.cache, cli.cache_bytes)
                .await
                .map(|()| Outcome::empty());
        }
        Command::Completions { shell } => {
            let mut out = Vec::new();
            clap_complete::generate(shell, &mut crate::args::command(), "ant", &mut out);
            return Ok(Outcome::ok(out));
        }
        _ => cli,
    };
    let ctx = Ctx::from_cli(&cli)?;
    match cli.command {
        // Handled above, before the client context is built.
        Command::Serve(_) | Command::CacheServe(_) | Command::Completions { .. } => {
            Ok(Outcome::empty())
        }
        Command::Import(a) => import::run(a).await,
        Command::Ls { target } => query::ls(&ctx, &target).await,
        Command::Cat { target } => query::cat(&ctx, &target).await,
        Command::Blame { target } => query::blame(&ctx, &target).await,
        Command::Log { rev, limit, path, paths } => {
            query::log(&ctx, &rev, limit, query::one_path(path, paths)?).await
        }
        Command::Diff { from, to, patch, merge_base, context, paths } => {
            let flags = query::DiffFlags { patch, merge_base, context, paths };
            query::diff(&ctx, &from, &to, flags).await
        }
        Command::MergeBase { a, b } => query::merge_base(&ctx, &a, &b).await,
        Command::LandedAs { commit, ref_name } => {
            query::landed_as(&ctx, &commit, ref_name.as_deref()).await
        }
        Command::Ref { cmd } => match cmd {
            RefCmd::Get { name } => refs::get(&ctx, &name).await,
            RefCmd::List { prefix } => refs::list(&ctx, &prefix).await,
            RefCmd::Init { name, message } => refs::init(&ctx, &name, &message).await,
            RefCmd::Set { name, rev, expect, create, force } => {
                refs::set(&ctx, &name, &rev, expect, create, force).await
            }
            RefCmd::Watch { name, from } => refs::watch(&ctx, &name, from.as_deref()).await,
        },
        Command::Ws { cmd: WsCmd::New { dir, at, sparse, lazy } } => {
            if lazy {
                ws::new_lazy(&ctx, &dir, &at).await
            } else {
                ws::new(&ctx, &dir, &at, sparse).await
            }
        }
        Command::Ws { cmd: WsCmd::Update { to } } => ws::update(&ctx, to).await,
        Command::Ws { cmd: WsCmd::Resolve { paths } } => ws::resolve(&ctx, paths).await,
        Command::Ws { cmd: WsCmd::Unmount { dir } } => ws::unmount(&dir).await,
        Command::Ws { cmd: WsCmd::Remount { dir } } => ws::remount(&ctx, dir).await,
        Command::Ws { cmd: WsCmd::Serve { dir, state } } => ws::serve(&ctx, &dir, &state).await,
        Command::Status { ignored } => ws::status(&ctx, ignored).await,
        Command::Submit { message } => ws::submit(&ctx, &message).await,
        Command::Land { ref_name, commit } => ws::land(&ctx, &ref_name, commit).await,
        Command::Mount { rev, mountpoint } => mount::run(&ctx, &rev, &mountpoint).await,
        Command::Admin { cmd: AdminCmd::ReindexAcl(a) } => admin::reindex_acl(a).await,
        Command::Admin { cmd: AdminCmd::Schema(a) } => admin::schema(a).await,
        Command::Admin { cmd: AdminCmd::Gc(a) } => admin::gc(a).await,
        Command::Admin { cmd: AdminCmd::Repack(a) } => admin::repack(a).await,
        Command::Cache { cmd: CacheCmd::Stats { tiers } } => cache::stats(&ctx, tiers).await,
        Command::Cache { cmd: CacheCmd::Seed { target, sparse } } => {
            cache::seed(&ctx, &target, sparse).await
        }
        Command::Cache { cmd: CacheCmd::Gc { to } } => cache::gc(&ctx, to).await,
    }
}
