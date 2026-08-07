//! spruce-leaf — a "Codex for sales".
//!
//! Launch it with no arguments for an interactive agent you type into like
//! Codex: describe an expensive workflow and it finds accounts that plausibly
//! have it, the people positioned to see it, writes a hypothesis-led outreach
//! sequence for each, and files everything in a local CRM at
//! http://localhost:<port>.
//!
//! Reasoning runs through an authenticated local Codex or Claude CLI, so no
//! model API key is needed. The outreach doctrine lives in editable
//! `playbooks/*.toml`, one per brand.
//!
//! Subcommands:
//!   spruce-leaf            (default) interactive REPL + live CRM
//!   spruce-leaf run "..."  one-shot campaign, filed in the CRM
//!   spruce-leaf crm        just serve the CRM dashboard

mod agent;
mod apollo;
mod business;
mod cadence;
mod calendar;
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
mod opportunity;
mod outreach;
mod pipeline;
mod playbook;
mod prompts;
mod repl;
mod report;
mod research;
mod send;
mod sourcing;
mod triage;
mod ui;
mod verify;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use business::Businesses;
use engine::{Backend, Engine};
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

    /// Directory of business operating profiles (one TOML per business).
    #[arg(long, global = true, default_value = "businesses")]
    businesses: String,

    /// Authenticated local CLI to use for model reasoning.
    #[arg(long, global = true, value_enum, default_value_t = Backend::Claude)]
    backend: Backend,

    /// Model override for the selected backend. Omit to use its default.
    #[arg(long, global = true)]
    model: Option<String>,

    /// Skip the LLM pre-send critique/rewrite pass.
    #[arg(long, global = true)]
    no_critique: bool,

    /// Max concurrent model calls at each fan-out step.
    #[arg(long, global = true, default_value_t = 5, value_parser = positive_usize)]
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

    /// Path to the SQLite execution db (real leads, mailboxes, scheduled sends).
    #[arg(long, global = true, default_value = ".spruce/sales.db")]
    db: String,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// (default) Launch the interactive spruce-leaf REPL with a live CRM.
    Repl,

    /// Run one campaign non-interactively and file it in the CRM.
    Run {
        /// The thesis: the expensive workflow / market to target.
        thesis: String,
        #[arg(long, default_value_t = 5, value_parser = positive_usize)]
        accounts: usize,
        #[arg(long, default_value_t = 5, value_parser = positive_usize)]
        contacts: usize,
        #[arg(long, default_value_t = 7, value_parser = positive_usize)]
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
        /// Skip model principle-distillation; keep only raw passages for retrieval.
        #[arg(long)]
        no_distill: bool,
        /// Max sections per book to distill (evenly sampled if the book has more).
        #[arg(long, default_value_t = 24)]
        max_sections: usize,
    },

    // --- SDR execution pipeline (real leads → verified email → autonomous send) ---
    /// Source REAL leads + people from Apollo and qualify them into the db.
    Source {
        /// The thesis: the expensive workflow / market to target.
        thesis: String,
        #[arg(long, default_value_t = 10, value_parser = positive_usize)]
        accounts: usize,
        #[arg(long, default_value_t = 3, value_parser = positive_usize)]
        contacts: usize,
    },

    /// Reveal + verify emails for sourced people (Apollo enrichment, costs credits).
    Enrich {
        #[arg(long, default_value_t = 50, value_parser = positive_usize)]
        limit: usize,
        /// Request phone enrichment (extra credits; requires APOLLO_WEBHOOK_URL).
        #[arg(long)]
        phone: bool,
    },

    /// Write outreach sequences for verified people and schedule the touches.
    Plan {
        #[arg(long, default_value_t = 7, value_parser = positive_usize)]
        touches: usize,
        /// Schedule email touches immediately instead of leaving them as drafts.
        #[arg(long)]
        auto: bool,
    },

    /// Approve drafted email touches so the cadence engine may send them.
    Approve {
        /// Only approve this person id (default: all email drafts for the brand).
        #[arg(long)]
        person: Option<String>,
    },

    /// Preview one cadence pass, or run the autonomous daemon with --live.
    Daemon {
        /// Actually send email continuously. Default previews once and exits.
        #[arg(long)]
        live: bool,
        /// Seconds between cadence passes.
        #[arg(long, default_value_t = 60, value_parser = positive_u64)]
        interval: u64,
        /// Max sends per pass.
        #[arg(long, default_value_t = 25, value_parser = positive_i64)]
        batch: i64,
    },

    /// Poll inboxes once and triage any replies.
    Inbox,

    /// Show funnel metrics per brand.
    Stats,

    /// Show timing rules, per-business capacity, and observed response timing.
    Calendar,

    /// Configure + health-check per-brand sending mailboxes from env.
    Mailboxes,

    /// Add an email (or @domain) to the active brand's suppression list.
    Suppress { email: String },

    // --- Generic opportunity/funding motion -------------------------------
    /// Discover and conservatively qualify live opportunities from configured official sources.
    DiscoverOpportunities {
        #[arg(long, default_value_t = 20, value_parser = positive_usize)]
        limit: usize,
    },

    /// List persisted opportunities for the active business.
    Opportunities {
        /// Optional pipeline status (shortlisted, watching, applying, ...).
        #[arg(long)]
        status: Option<String>,
        /// Hide closed/ineligible/lost records; includes items needing evidence.
        #[arg(long)]
        actionable: bool,
    },

    /// Resolve the official route and relevant funder people through Apollo.
    /// Domain-first people search is free; organization fallback may consume a search credit.
    OpportunityContacts {
        opportunity: String,
        #[arg(long, default_value_t = 3, value_parser = positive_usize)]
        limit: usize,
        /// Reveal and verify Apollo emails now (consumes Apollo credits).
        #[arg(long)]
        enrich: bool,
    },

    /// Draft a short, grant-appropriate pre-application sequence.
    PlanFundingOutreach {
        opportunity: String,
        #[arg(long, default_value_t = 2, value_parser = positive_usize)]
        touches: usize,
        /// Schedule clean drafts immediately; default leaves them for approval.
        #[arg(long)]
        auto: bool,
    },

    /// Approve drafted opportunity emails for sending.
    ApproveFunding {
        /// Only approve this opportunity contact; default is all for the active business.
        #[arg(long)]
        contact: Option<String>,
    },

    /// Prepare an evidence-gapped go/no-go brief and application work plan.
    PrepareApplication { opportunity: String },
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let mut cli = Cli::parse();
    let rt = tokio::runtime::Runtime::new().context("starting Tokio runtime")?;

    let store =
        crm::open(&cli.store).with_context(|| format!("opening CRM store at {}", cli.store))?;
    let library = knowledge::open(&cli.knowledge)
        .with_context(|| format!("opening knowledge library at {}", cli.knowledge))?;
    let db =
        db::Db::open(&cli.db).with_context(|| format!("opening execution db at {}", cli.db))?;

    let command = cli.command.take().unwrap_or(Command::Repl);
    let critique = !cli.no_critique;

    match command {
        Command::Crm => {
            spawn_server(&rt, &store, &db, cli.port);
            let url = format!("http://localhost:{}", cli.port);
            println!("\u{1F332} CRM dashboard at {url}  (ctrl-c to stop)");
            agent::open_browser(&url);
            rt.block_on(std::future::pending::<()>());
            Ok(())
        }

        Command::Run {
            thesis,
            accounts,
            contacts,
            touches,
            report,
        } => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = load_playbooks(&cli)?;
            let pb = playbooks.get(&cli.brand)?;
            eprintln!(
                "\u{2192} [{}] {thesis}\n\u{2192} {accounts}\u{00d7}{contacts}\u{00d7}{touches} \
                 (critique={critique}) via {} CLI",
                pb.name,
                cli.backend.as_str(),
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

        Command::Ingest {
            paths,
            no_distill,
            max_sections,
        } => {
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
                        .ingest(
                            &client,
                            Path::new(p),
                            !no_distill,
                            max_sections,
                            cli.concurrency,
                        )
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

        Command::Source {
            thesis,
            accounts,
            contacts,
        } => {
            let client = make_engine(&rt, &cli)?;
            let apollo = make_apollo()?;
            let playbooks = load_playbooks(&cli)?;
            let pb = playbooks.get(&cli.brand)?;
            let businesses = load_businesses(&cli)?;
            let business = businesses.get(&cli.brand)?;
            let lib = rt.block_on(async { library.read().await.clone() });
            eprintln!(
                "\u{2192} [{}] sourcing real leads from Apollo: {thesis}",
                pb.name
            );
            let s = rt.block_on(sourcing::source(
                &db,
                &client,
                &apollo,
                pb,
                &playbooks.shared,
                &business.calendar.fallback_recipient_timezone,
                &lib,
                &thesis,
                accounts,
                contacts,
                cli.concurrency,
            ))?;
            println!(
                "\u{2713} {} orgs \u{2192} {} qualified leads, {} people.\n  next: spruce-leaf --brand {} enrich",
                s.orgs_found, s.leads_qualified, s.people_added, cli.brand
            );
            Ok(())
        }

        Command::Enrich { limit, phone } => {
            let apollo = make_apollo()?;
            eprintln!(
                "\u{2192} [{}] enriching + verifying up to {limit} people\u{2026}",
                cli.brand
            );
            let s = rt.block_on(enrich::enrich_pending(
                &db,
                &apollo,
                Some(&cli.brand),
                limit,
                phone,
            ))?;
            println!(
                "\u{2713} attempted {}, emails found {}, verified {} (~{} Apollo credits).{}\n  next: spruce-leaf --brand {} plan",
                s.attempted,
                s.emails_found,
                s.verified,
                s.credits_spent,
                match &s.stopped {
                    Some(r) => format!("\n  \u{26a0} stopped early: {r}"),
                    None => String::new(),
                },
                cli.brand
            );
            Ok(())
        }

        Command::Plan { touches, auto } => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = load_playbooks(&cli)?;
            let pb = playbooks.get(&cli.brand)?;
            let businesses = load_businesses(&cli)?;
            let business = businesses.get(&cli.brand)?;
            let lib = rt.block_on(async { library.read().await.clone() });
            let s = rt.block_on(outreach::plan_pending(
                &db,
                &client,
                pb,
                business,
                &playbooks.shared,
                &lib,
                touches,
                cli.concurrency,
                auto,
            ))?;
            println!(
                "\u{2713} planned {} people: {} touches scheduled, {} drafted.{}",
                s.people_planned,
                s.touches_scheduled,
                s.touches_drafted,
                if auto {
                    ""
                } else {
                    "\n  approve with: spruce-leaf approve"
                }
            );
            Ok(())
        }

        Command::Approve { person } => {
            let n = db.approve_touches(Some(&cli.brand), person.as_deref())?;
            println!("\u{2713} approved {n} touch(es) \u{2192} scheduled.");
            Ok(())
        }

        Command::Daemon {
            live,
            interval,
            batch,
        } => {
            let playbooks = Arc::new(load_playbooks(&cli)?);
            let businesses = Arc::new(load_businesses(&cli)?);
            if live {
                let n_mb = mailbox::load_from_env(&db, &playbooks.keys())?;
                eprintln!("\u{2713} {n_mb} mailbox(es) loaded from env");
            } else {
                let n_mb = db.list_mailboxes(None)?.len();
                eprintln!(
                    "\u{2713} {n_mb} persisted mailbox(es) available for preview; \
                     run `spruce-leaf mailboxes` to sync env configuration"
                );
                eprintln!(
                    "\u{26A0} DRY-RUN: no real mail will be sent. Re-run with --live to send."
                );
            }
            let compliance = compliance::Compliance::from_env();
            if live && compliance.physical_address.trim().is_empty() {
                anyhow::bail!(
                    "refusing live sending: COMPLIANCE_ADDRESS is unset (required for CASL/CAN-SPAM)"
                );
            }
            if live {
                let health = rt.block_on(mailbox::health_check(&db, None))?;
                if !health.iter().any(|(mailbox, _)| mailbox.active) {
                    anyhow::bail!(
                        "refusing live sending: no active mailboxes are configured; run `spruce-leaf mailboxes`"
                    );
                }
                let unhealthy = health
                    .iter()
                    .filter(|(mailbox, auth)| mailbox.active && !auth.is_healthy())
                    .map(|(m, auth)| format!("{} [{}] {}", m.from_email, m.brand, auth.summary()))
                    .collect::<Vec<_>>();
                if !unhealthy.is_empty() {
                    anyhow::bail!(
                        "refusing live sending: mailbox domain checks failed:\n  {}",
                        unhealthy.join("\n  ")
                    );
                }
            }
            let cfg = cadence::CadenceConfig {
                dry_run: !live,
                batch,
                interval_secs: interval,
                ..Default::default()
            };
            // Inbox polling runs alongside live cadence only. A dry preview is
            // intentionally read-only and must not mark inbound mail handled.
            if live {
                let inbox_client = make_engine(&rt, &cli)?;
                let dbi = db.clone();
                rt.spawn(async move {
                    loop {
                        if let Err(e) = inbox::poll_all(&dbi, &inbox_client, None).await {
                            eprintln!("  ! inbox poll: {e:#}");
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(interval.max(30))).await;
                    }
                });
            }
            rt.block_on(cadence::run_daemon(
                db.clone(),
                playbooks,
                businesses,
                compliance,
                cfg,
            ))
        }

        Command::Inbox => {
            let client = make_engine(&rt, &cli)?;
            let n = rt.block_on(inbox::poll_all(&db, &client, None))?;
            println!("\u{2713} handled {n} reply/replies.");
            Ok(())
        }

        Command::Stats => {
            let playbooks = load_playbooks(&cli)?;
            println!("{}", metrics::render(&metrics::funnel(&db, None)?));
            for b in playbooks.keys() {
                let f = metrics::funnel(&db, Some(b))?;
                if f.people > 0 || f.leads > 0 || f.opportunities > 0 {
                    println!("\n{}", metrics::render(&f));
                }
            }
            Ok(())
        }

        Command::Calendar => {
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            println!(
                "{}",
                calendar::render_intelligence(profile, &db, chrono::Utc::now())?
            );
            Ok(())
        }

        Command::Mailboxes => {
            let playbooks = load_playbooks(&cli)?;
            let n = mailbox::load_from_env(&db, &playbooks.keys())?;
            println!("configured {n} mailbox(es) from env:");
            let health = rt.block_on(mailbox::health_check(&db, None))?;
            if health.is_empty() {
                println!("  (none — set <BRAND>_FROM_EMAIL / <BRAND>_SMTP_HOST etc. in .env)");
            }
            for (m, auth) in health {
                println!("  {} [{}]  {}", m.from_email, m.brand, auth.summary());
            }
            Ok(())
        }

        Command::Suppress { email } => {
            db.add_suppression(&cli.brand, &email, "manual")?;
            println!("\u{2713} suppressed {email} for {}.", cli.brand);
            Ok(())
        }

        Command::DiscoverOpportunities { limit } => {
            let client = make_engine(&rt, &cli)?;
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            eprintln!(
                "\u{2192} [{}] discovering opportunities from {} configured source(s)",
                profile.name,
                profile.funding()?.sources.len()
            );
            let summary = rt.block_on(opportunity::discover(&db, &client, profile, limit))?;
            println!(
                "\u{2713} checked {}/{} sources, found {} candidates, verified {}, added {}, updated {}, skipped {}.",
                summary.sources_read,
                summary.sources_attempted,
                summary.candidates_found,
                summary.opportunities_verified,
                summary.opportunities_added,
                summary.opportunities_updated,
                summary.skipped,
            );
            if !summary.errors.is_empty() {
                println!("\nSource warnings:");
                for warning in summary.errors {
                    println!("- {warning}");
                }
            }
            let opportunities = db.list_opportunities(Some(&cli.brand), None)?;
            println!("\n{}", opportunity::render_opportunities(&opportunities));
            Ok(())
        }

        Command::Opportunities { status, actionable } => {
            let mut opportunities = db.list_opportunities(Some(&cli.brand), status.as_deref())?;
            if actionable {
                opportunities.retain(opportunity::is_actionable);
            }
            println!("{}", opportunity::render_opportunities(&opportunities));
            Ok(())
        }

        Command::OpportunityContacts {
            opportunity: opportunity_id,
            limit,
            enrich,
        } => {
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            let apollo = make_apollo()?;
            let summary = rt.block_on(opportunity::resolve_contacts(
                &db,
                &apollo,
                profile,
                &opportunity_id,
                limit,
                enrich,
            ))?;
            println!(
                "\u{2713} {} official route(s), {} Apollo people found, {} contacts stored, {} enriched, {} verified emails.",
                summary.official_contacts,
                summary.apollo_people_found,
                summary.contacts_added,
                summary.contacts_enriched,
                summary.verified_emails,
            );
            for contact in db.list_opportunity_contacts(&opportunity_id)? {
                let route = if !contact.email.is_empty() {
                    format!("{} ({})", contact.email, contact.email_status)
                } else if !contact.phone.is_empty() {
                    format!("phone {}", contact.phone)
                } else {
                    "no direct email or phone".into()
                };
                println!(
                    "- {} — {} [{}] {}\n  id: {}",
                    if contact.name.is_empty() {
                        "Programme contact"
                    } else {
                        &contact.name
                    },
                    contact.title,
                    contact.source,
                    route,
                    contact.id,
                );
            }
            Ok(())
        }

        Command::PlanFundingOutreach {
            opportunity: opportunity_id,
            touches,
            auto,
        } => {
            let client = make_engine(&rt, &cli)?;
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            let playbooks = load_playbooks(&cli)?;
            let playbook = playbooks.get(&cli.brand)?;
            let summary = rt.block_on(opportunity::plan_funding_outreach(
                &db,
                &client,
                profile,
                playbook,
                &playbooks.shared,
                opportunity::FundingOutreachOptions {
                    opportunity_id: &opportunity_id,
                    touches,
                    auto_schedule: auto,
                },
            ))?;
            println!(
                "\u{2713} planned {} contact(s): {} scheduled, {} draft(s).{}",
                summary.contacts_planned,
                summary.touches_scheduled,
                summary.touches_drafted,
                if auto {
                    ""
                } else {
                    " Approve with `spruce-leaf --brand outagehub approve-funding`."
                }
            );
            Ok(())
        }

        Command::ApproveFunding { contact } => {
            let n = db.approve_opportunity_touches(Some(&cli.brand), contact.as_deref())?;
            println!("\u{2713} approved {n} funding touch(es) \u{2192} scheduled.");
            Ok(())
        }

        Command::PrepareApplication {
            opportunity: opportunity_id,
        } => {
            let client = make_engine(&rt, &cli)?;
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            let brief = rt.block_on(opportunity::prepare_application(
                &db,
                &client,
                profile,
                &opportunity_id,
            ))?;
            println!(
                "\u{2713} application brief prepared.\n\nEligibility:\n{}\n\nProject shape:\n{}\n\nEvidence still needed:\n- {}\n\nNext steps:\n- {}",
                brief.eligibility_summary,
                brief.project_shape,
                brief.evidence_needed.join("\n- "),
                brief.next_steps.join("\n- "),
            );
            Ok(())
        }

        Command::Repl => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = Arc::new(load_playbooks(&cli)?);
            let businesses = Arc::new(load_businesses(&cli)?);
            // Validate the requested brand up front.
            playbooks.get(&cli.brand)?;
            businesses.get(&cli.brand)?;
            spawn_server(&rt, &store, &db, cli.port);
            let agent = agent::Agent::new(
                client,
                store.clone(),
                library.clone(),
                db.clone(),
                playbooks,
                businesses,
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
fn spawn_server(
    rt: &tokio::runtime::Runtime,
    store: &crm::SharedStore,
    db: &db::SharedDb,
    port: u16,
) {
    let store = store.clone();
    let db = db.clone();
    rt.spawn(async move {
        if let Err(e) = crm::serve(store, db, port).await {
            eprintln!("CRM server error: {e:#}");
        }
    });
}

/// Build the selected local-CLI engine and preflight that it is available.
fn make_engine(rt: &tokio::runtime::Runtime, cli: &Cli) -> Result<Engine> {
    let engine = Engine::new(cli.backend, cli.model.clone());
    let version = rt.block_on(engine.check())?;
    eprintln!(
        "\u{2713} using {version} ({}) as the reasoning engine",
        engine.backend().as_str()
    );
    Ok(engine)
}

fn load_playbooks(cli: &Cli) -> Result<Playbooks> {
    Playbooks::load(&cli.playbooks)
        .with_context(|| format!("loading playbooks from {}/", cli.playbooks))
}

fn load_businesses(cli: &Cli) -> Result<Businesses> {
    Businesses::load(&cli.businesses)
        .with_context(|| format!("loading business profiles from {}/", cli.businesses))
}

/// Build the Apollo client, surfacing a clear message if the key is missing.
fn make_apollo() -> Result<apollo::Apollo> {
    apollo::Apollo::from_env().context("Apollo is required for sourcing/enrichment")
}

fn positive_usize(raw: &str) -> std::result::Result<usize, String> {
    raw.parse::<usize>()
        .map_err(|_| format!("'{raw}' is not a positive integer"))
        .and_then(|value| {
            if value > 0 {
                Ok(value)
            } else {
                Err("value must be greater than zero".to_string())
            }
        })
}

fn positive_u64(raw: &str) -> std::result::Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|_| format!("'{raw}' is not a positive integer"))
        .and_then(|value| {
            if value > 0 {
                Ok(value)
            } else {
                Err("value must be greater than zero".to_string())
            }
        })
}

fn positive_i64(raw: &str) -> std::result::Result<i64, String> {
    raw.parse::<i64>()
        .map_err(|_| format!("'{raw}' is not a positive integer"))
        .and_then(|value| {
            if value > 0 {
                Ok(value)
            } else {
                Err("value must be greater than zero".to_string())
            }
        })
}
