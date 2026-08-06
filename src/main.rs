//! spruce-leaf — a "Codex for sales".
//!
//! Launch it with no arguments for an interactive agent you type into like
//! Codex: describe an expensive workflow and it finds accounts that plausibly
//! have it, the people positioned to see it, writes a hypothesis-led outreach
//! sequence for each, and files everything in a local CRM at
//! http://localhost:<port>.
//!
//! Reasoning runs through the local `claude` CLI (Claude Code), so no API key
//! is needed — it uses your existing Claude authentication. The outreach
//! doctrine lives in editable `playbooks/*.toml`, one per brand.
//!
//! Subcommands:
//!   spruce-leaf            (default) interactive REPL + live CRM
//!   spruce-leaf run "..."  one-shot campaign, filed in the CRM
//!   spruce-leaf crm        just serve the CRM dashboard

mod agent;
mod apollo;
mod cadence;
mod compliance;
mod crm;
mod db;
mod domain;
mod engine;
mod enrich;
mod inbox;
mod knowledge;
mod mailbox;
mod metrics;
mod outreach;
mod pipeline;
mod playbook;
mod prompts;
mod repl;
mod report;
mod send;
mod sourcing;
mod triage;
mod ui;
mod verify;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use engine::Claude;
use playbook::Playbooks;

#[derive(Parser, Debug)]
#[command(
    name = "spruce-leaf",
    about = "Codex for sales: find expensive workflows, the people who see them, and how to reach them."
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Brand playbook to run (gnk | wapahki | outagehub).
    #[arg(long, global = true, default_value = "gnk")]
    brand: String,

    /// Directory of playbook TOML files (shared.toml + one per brand).
    #[arg(long, global = true, default_value = "playbooks")]
    playbooks: String,

    /// Model for the `claude` CLI (e.g. opus, sonnet). Omit to use its default.
    #[arg(long, global = true)]
    model: Option<String>,

    /// Skip the LLM pre-send critique/rewrite pass.
    #[arg(long, global = true)]
    no_critique: bool,

    /// Max concurrent `claude` calls at each fan-out step.
    #[arg(long, global = true, default_value_t = 5)]
    concurrency: usize,

    /// Port for the local CRM web dashboard.
    #[arg(long, global = true, default_value_t = 8787)]
    port: u16,

    /// Path to the CRM JSON store.
    #[arg(long, global = true, default_value = ".spruce/crm.json")]
    store: String,

    /// Path to the book-knowledge library JSON (built by `ingest`).
    #[arg(long, global = true, default_value = ".spruce/knowledge.json")]
    knowledge: String,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// (default) Launch the interactive spruce-leaf REPL with a live CRM.
    Repl,

    /// Run one campaign non-interactively and file it in the CRM.
    Run {
        /// The thesis: the expensive workflow / market to target.
        thesis: String,
        #[arg(long, default_value_t = 5)]
        accounts: usize,
        #[arg(long, default_value_t = 5)]
        contacts: usize,
        #[arg(long, default_value_t = 7)]
        touches: usize,
        /// Optionally also write a standalone Markdown brief to this path.
        #[arg(long)]
        report: Option<String>,
    },

    /// Serve only the CRM dashboard (no agent).
    Crm,

    /// Ingest business/sales books into the knowledge library (.txt/.md/.pdf).
    Ingest {
        /// Files or directories of books to ingest.
        paths: Vec<String>,
        /// Skip Claude principle-distillation; keep only raw passages for retrieval.
        #[arg(long)]
        no_distill: bool,
        /// Max sections per book to distill (evenly sampled if the book has more).
        #[arg(long, default_value_t = 24)]
        max_sections: usize,
    },
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let mut cli = Cli::parse();
    let rt = tokio::runtime::Runtime::new().context("starting Tokio runtime")?;

    let store = crm::open(&cli.store)
        .with_context(|| format!("opening CRM store at {}", cli.store))?;
    let library = knowledge::open(&cli.knowledge)
        .with_context(|| format!("opening knowledge library at {}", cli.knowledge))?;

    let command = cli.command.take().unwrap_or(Command::Repl);
    let critique = !cli.no_critique;

    match command {
        Command::Crm => {
            spawn_server(&rt, &store, cli.port);
            let url = format!("http://localhost:{}", cli.port);
            println!("\u{1F332} CRM dashboard at {url}  (ctrl-c to stop)");
            agent::open_browser(&url);
            rt.block_on(std::future::pending::<()>());
            Ok(())
        }

        Command::Run { thesis, accounts, contacts, touches, report } => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = load_playbooks(&cli)?;
            let pb = playbooks.get(&cli.brand)?;
            eprintln!(
                "\u{2192} [{}] {thesis}\n\u{2192} {accounts}\u{00d7}{contacts}\u{00d7}{touches} \
                 (critique={critique}) via claude CLI",
                pb.name
            );
            let lib = rt.block_on(async { library.read().await.clone() });
            let campaign = rt.block_on(pipeline::run(
                &client,
                pb,
                &playbooks.shared,
                &lib,
                &thesis,
                accounts,
                contacts,
                touches,
                cli.concurrency,
                critique,
                &(),
            ))?;
            if let Some(path) = report {
                std::fs::write(&path, report::render(&campaign))
                    .with_context(|| format!("writing {path}"))?;
                eprintln!("wrote {path}");
            }
            let (ac, ct, to) = rt.block_on(async { store.write().await.ingest(campaign) })?;
            println!(
                "\u{2713} filed {ac} accounts, {ct} contacts, {to} touches into {}.\n  \
                 view: spruce-leaf crm   (http://localhost:{})",
                cli.store, cli.port
            );
            Ok(())
        }

        Command::Ingest { paths, no_distill, max_sections } => {
            if paths.is_empty() {
                eprintln!("usage: spruce-leaf ingest <file-or-dir> [more…]  (.txt/.md/.pdf)");
                return Ok(());
            }
            let client = make_engine(&rt, &cli)?;
            rt.block_on(async {
                let mut lib = library.write().await;
                for p in &paths {
                    eprintln!("\u{2192} ingesting {p} …");
                    match lib
                        .ingest(&client, Path::new(p), !no_distill, max_sections, cli.concurrency)
                        .await
                    {
                        Ok(rep) => println!("{}", rep.summary()),
                        Err(e) => eprintln!("  ! {p}: {e:#}"),
                    }
                }
                println!("\u{2713} library now holds {}.", lib.stats());
            });
            Ok(())
        }

        Command::Repl => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = Arc::new(load_playbooks(&cli)?);
            // Validate the requested brand up front.
            playbooks.get(&cli.brand)?;
            spawn_server(&rt, &store, cli.port);
            let agent = agent::Agent::new(
                client,
                store.clone(),
                library.clone(),
                playbooks,
                cli.brand.clone(),
                critique,
                cli.port,
                cli.concurrency,
            );
            repl::run_repl(&rt, agent)
        }
    }
}

/// Start the CRM web server on the runtime's worker threads.
fn spawn_server(rt: &tokio::runtime::Runtime, store: &crm::SharedStore, port: u16) {
    let store = store.clone();
    rt.spawn(async move {
        if let Err(e) = crm::serve(store, port).await {
            eprintln!("CRM server error: {e:#}");
        }
    });
}

/// Build the Claude engine, preflighting that the `claude` CLI is available.
fn make_engine(rt: &tokio::runtime::Runtime, cli: &Cli) -> Result<Claude> {
    let version = rt.block_on(Claude::check())?;
    eprintln!("\u{2713} using {version} as the reasoning engine");
    Ok(Claude::new(cli.model.clone()))
}

fn load_playbooks(cli: &Cli) -> Result<Playbooks> {
    Playbooks::load(&cli.playbooks)
        .with_context(|| format!("loading playbooks from {}/", cli.playbooks))
}
