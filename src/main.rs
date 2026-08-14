//! spruce-leaf — a "Codex for sales".
//!
//! Launch it with no arguments for an interactive agent you type into like
//! Codex: describe an expensive workflow and it finds accounts that plausibly
//! have it, the people positioned to see it, writes a hypothesis-led outreach
//! sequence for each, and files everything in a local CRM at
//! http://localhost:<port>.
//!
//! Reasoning runs through an authenticated local Claude, Codex, or Grok CLI, so
//! no model API key is needed. The outreach doctrine lives in editable
//! `playbooks/*.toml`, one per brand.
//!
//! Subcommands:
//!   spruce-leaf            (default) interactive REPL + live CRM
//!   spruce-leaf simulate "..."  synthetic prompt experiment, never filed or sent
//!   spruce-leaf crm        just serve the CRM dashboard

mod acceptance;
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
mod gmail;
mod google_calendar;
mod google_oauth;
mod gtm;
mod inbox;
mod jobs;
mod knowledge;
mod mailbox;
mod metrics;
mod opportunity;
mod outage_evidence;
#[cfg(test)]
mod outagehub_eval;
mod outreach;
mod outreach_ablation;
mod outreach_eval;
mod pilot;
mod pipeline;
mod playbook;
mod priority;
mod prompts;
mod qualification;
mod repl;
mod reply_agent;
mod report;
mod research;
mod response_design;
mod sales_packet;
mod segments;
mod send;
mod sourcing;
mod storage;
mod synthesis;
mod triage;
mod ui;
mod verify;

use std::ffi::OsString;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use anyhow::{anyhow, Context, Result};
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

    /// Inference provider. OpenAI uses the Responses API; others use local CLIs.
    #[arg(long, global = true, value_enum, default_value_t = Backend::Codex)]
    backend: Backend,

    /// Model override for the selected backend. Omit to use its default.
    #[arg(long, global = true)]
    model: Option<String>,

    /// Use deterministic QA only; skip the semantic copy-edit pass.
    #[arg(long, global = true)]
    no_critique: bool,

    /// Max concurrent model calls at each fan-out step.
    #[arg(long, global = true, default_value_t = 2, value_parser = positive_usize)]
    concurrency: usize,

    /// Preferred port for the local CRM. Reuses a running CRM or finds a free port.
    #[arg(long, global = true)]
    port: Option<u16>,

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

    /// Generate a synthetic prompt experiment. Never writes to the execution db or CRM.
    Simulate {
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

    /// Open the GTM engineering lab: signals, plays, experiments, outcomes, and proofs.
    Gtm,

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
        /// Exact persisted market-segment key. Prevents a mixed ICP query from
        /// being heuristically credited to the wrong coverage ledger.
        #[arg(long)]
        segment: Option<String>,
        /// Research only these employer domains (comma-separated); bypasses broad ICP discovery.
        #[arg(long, value_delimiter = ',')]
        domains: Vec<String>,
        #[arg(long, default_value_t = 10, value_parser = positive_usize)]
        accounts: usize,
        #[arg(long, default_value_t = 3, value_parser = positive_usize)]
        contacts: usize,
    },

    /// Re-read and reassess one existing CRM account against the current GTM play (no Apollo).
    Research {
        /// Exact account name/domain/id, or an unambiguous name fragment.
        account: String,
        /// Optional research focus added to the active brand motion.
        #[arg(long, default_value = "")]
        thesis: String,
    },

    /// Intersect verified Canadian operating locations with historical OutageHub polygons.
    OutageEvidence {
        /// Historical OutageHub JSON archive containing outage polygons.
        #[arg(long)]
        archive: String,
        /// Evidence report consumed by OutageHub account research.
        #[arg(long, default_value = ".spruce/outage-location-matches.json")]
        output: String,
        /// Optional verified-location JSON. Without it, uses Canada's public EV station feed.
        #[arg(long)]
        locations: Option<String>,
    },

    /// Reveal + verify emails for sourced people (Apollo enrichment, costs credits).
    Enrich {
        #[arg(long, default_value_t = 50, value_parser = positive_usize)]
        limit: usize,
        /// Enrich one exact person id, email, or unambiguous name instead of the brand backlog.
        #[arg(long)]
        person: Option<String>,
        /// Request phone enrichment (extra credits; requires APOLLO_WEBHOOK_URL).
        #[arg(long)]
        phone: bool,
    },

    /// Write reviewed outreach drafts for verified contacts.
    Plan {
        /// Requested touch count. Cold planning defaults to one manually reviewed T1.
        #[arg(long, default_value_t = 1, value_parser = positive_usize)]
        touches: usize,
        /// Limit planning to the first N existing companies in CRM order.
        #[arg(long, value_parser = positive_usize)]
        accounts: Option<usize>,
        /// Recipient-sequence cap per company; the requested count is honored.
        #[arg(long, value_parser = positive_usize)]
        contacts: Option<usize>,
        /// Optional total contact cap across the selected companies.
        #[arg(long, value_parser = positive_usize)]
        limit: Option<usize>,
        /// Schedule email touches immediately instead of leaving them as drafts.
        #[arg(long)]
        auto: bool,
        /// Limit planning to one exact person id, email, or name.
        #[arg(long)]
        person: Option<String>,
        /// The exact next response or action wanted from this recipient. The
        /// planner will reduce an over-large request to the nearest earned step.
        #[arg(long)]
        outcome: Option<String>,
        /// Replace an existing active sequence only when it has no sent touches.
        #[arg(long)]
        replace_drafts: bool,
    },

    /// Blind three-candidate inbox evaluation against human unchanged-send labels.
    EvalOutreach {
        #[arg(long, default_value = "evals/outreach-gold.jsonl")]
        corpus: String,
        /// Judge each candidate set in forward and reverse order and require consistency.
        #[arg(long)]
        double_blind: bool,
    },

    /// Remove or expand one copy-prompt layer at a time and blind-compare the result.
    EvalOutreachAblation {
        #[arg(long, default_value = "evals/outreach-gold.jsonl")]
        corpus: String,
        /// One representative case per brand, up to this limit.
        #[arg(long, default_value_t = 3, value_parser = positive_usize)]
        cases: usize,
        /// Independent generations per case and arm.
        #[arg(long, default_value_t = 1, value_parser = positive_usize)]
        repeats: usize,
        /// Print every generated body as well as subjects and blind verdicts.
        #[arg(long)]
        show_drafts: bool,
        /// Run only one named variant against full (for example no-role-contract).
        #[arg(long)]
        only: Option<String>,
    },

    /// Audit one real brand pipeline against its supervised-pilot release threshold.
    /// Read-only: never sources, generates, approves, or sends anything.
    PilotAudit {
        #[arg(long, value_parser = positive_usize)]
        accounts: Option<usize>,
        #[arg(long, value_parser = positive_usize)]
        segments: Option<usize>,
        #[arg(long, value_parser = positive_usize)]
        messages: Option<usize>,
        #[arg(long, value_parser = positive_usize)]
        approvals: Option<usize>,
    },

    /// Export local, read-only sales decision packages. This never researches,
    /// drafts, approves, schedules, or sends.
    SalesPackets {
        /// Exact sales-opportunity id. Omit to export every current opportunity for the brand.
        #[arg(long)]
        opportunity: Option<String>,
        #[arg(long, default_value = ".spruce/opportunity_packets")]
        output: PathBuf,
    },

    /// Export a read-only supervised acceptance sample and its honest failure
    /// report. Never researches, generates, labels, approves, schedules, or sends.
    AcceptanceReport {
        #[arg(long, default_value = ".spruce/acceptance-report")]
        output: PathBuf,
    },

    /// Import a founder/compliance-reviewed prospect sales brief from JSON.
    /// Importing never generates, approves copy, schedules, or sends.
    ImportSalesBrief { file: PathBuf },

    /// Import reviewed source/channel context for email or LinkedIn acquisition.
    /// A conference, webinar, referral, or customer context must be real.
    ImportAcquisitionContext { file: PathBuf },

    /// Import one manually reviewed conditional follow-up plan from JSON.
    ImportConditionalFollowup { file: PathBuf },

    /// Import a SPICED/discovery record; the database requires an in-scope human reply.
    ImportDiscoveryQualification { file: PathBuf },

    /// Import a post-reply application-centric sales brief. This cannot create
    /// cold outreach and requires an in-scope human reply.
    ImportSalesApplication { file: PathBuf },

    /// Import a claim-bound proof asset after a human has completed its real
    /// analysis. Superficial GnK summaries are rejected by the database.
    ImportProofAsset { file: PathBuf },

    /// Prepare one outside-in proof asset solely from a draft brief's cited evidence.
    PrepareProofAsset {
        opportunity: String,
        person: String,
        #[arg(long)]
        asset_type: String,
        #[arg(long, default_value = ".spruce/opportunity_packets")]
        output: PathBuf,
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
        /// Continuously source, enrich, and draft toward funnel targets.
        /// Without --live, outbound email remains disabled.
        #[arg(long)]
        autopilot: bool,
        /// Seconds between cadence passes.
        #[arg(long, default_value_t = 60, value_parser = positive_u64)]
        interval: u64,
        /// Max sends per pass.
        #[arg(long, default_value_t = 90, value_parser = positive_i64)]
        batch: i64,
    },

    /// Seed one verified test person (no Apollo) to exercise the full
    /// send→reply→book loop against a mailbox you control. Pair with
    /// SPRUCE_SEND_ALLOWLIST so a `--live` daemon can only reach that address.
    SeedTestLead {
        /// Recipient address for the seeded person — a mailbox you own.
        #[arg(long)]
        email: String,
        /// Display name for the seeded person.
        #[arg(long, default_value = "Test Prospect")]
        name: String,
        /// Company name for the seeded lead.
        #[arg(long, default_value = "Test Co")]
        company: String,
        /// IANA timezone for scheduling the touch. Pick a zone currently in
        /// business hours for an immediate test send.
        #[arg(long, default_value = "America/Toronto")]
        timezone: String,
    },

    /// Poll inboxes once, resolve threads, and draft conversational replies.
    Inbox {
        /// Book an explicitly accepted, previously-offered Google Calendar slot.
        #[arg(long)]
        book: bool,
    },

    /// Approve reply-agent drafts for delivery by the daemon.
    ApproveReplies {
        /// Only approve this conversation id (default: all drafts for the brand).
        #[arg(long)]
        conversation: Option<String>,
    },

    /// List pending and booked meetings.
    Meetings,

    /// Book pending accepted slots into Google Calendar after rechecking availability.
    BookMeetings {
        /// Only book this pending meeting id (default: all for the brand).
        #[arg(long)]
        id: Option<String>,
    },

    /// Show durable autopilot queue health, including dead-letter work.
    Jobs,

    /// Show funnel metrics per brand.
    Stats,

    /// Cluster discovery across sourced companies + replies into an investor-ready synthesis.
    Synthesize {
        /// Markdown output path (default: .spruce/synthesis-<brand>.md).
        #[arg(long, default_value = "")]
        out: String,
    },

    /// Show timing rules, per-business capacity, and observed response timing.
    Calendar {
        /// Rebuild this brand's approved queue with the portfolio scheduler.
        #[arg(long)]
        rebalance: bool,
    },

    /// Configure + health-check per-brand sending mailboxes from env.
    Mailboxes,

    /// Browser OAuth: link a brand Gmail account (opens Chrome; no App Passwords).
    Login {
        /// Brand key: gnk | wapahki | outagehub
        brand: String,
    },

    /// Show which brand Gmail accounts are linked via OAuth.
    MailStatus,

    /// Pull recent Gmail inbox+sent into conversations and learnings.
    MailSync {
        /// Limit to one brand (default: every linked brand).
        #[arg(long)]
        brand: Option<String>,
        /// Max messages per label (inbox/sent). Default 40.
        #[arg(long, default_value_t = 40)]
        limit: usize,
    },

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

    /// Build sponsorship opportunities from source-backed OutageHub accounts.
    /// All six sponsorship qualification gates must pass before a row is created.
    SeedSponsorships {
        #[arg(long, default_value_t = 30, value_parser = positive_usize)]
        limit: usize,
    },

    /// Import a JSON/JSONL sponsor-research manifest after re-fetching and
    /// independently verifying every cited first-party source and contact.
    ImportSponsorshipResearch { file: PathBuf },

    /// Draft one or two independently reviewed sponsorship emails. Always manual.
    PlanSponsorshipOutreach {
        opportunity: String,
        /// Exact mapped contact to use. Prevents title seniority from displacing the intended sponsor-side owner.
        #[arg(long)]
        contact: Option<String>,
        #[arg(long, default_value_t = 1, value_parser = positive_usize)]
        touches: usize,
        /// Replace existing unsent draft/blocked sponsorship copy after a policy or evidence repair.
        #[arg(long)]
        refresh: bool,
    },

    /// Audit the entire held sponsorship campaign for count, contact routing,
    /// duplicate subjects/rationales, review state, and zero delivery state.
    AuditSponsorshipOutreach {
        #[arg(long, default_value_t = 30, value_parser = positive_usize)]
        target: usize,
    },

    /// Render the governed one-page sponsorship scope/checklist for one target.
    PrepareSponsorshipPack { opportunity: String },
}

fn main() -> Result<()> {
    bootstrap_latest_workspace_binary()?;
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
            let playbooks = Arc::new(load_playbooks(&cli)?);
            let businesses = Arc::new(load_businesses(&cli)?);
            let endpoint = ensure_crm_server(
                &rt,
                &store,
                &db,
                &businesses,
                &playbooks,
                &library,
                CrmStartPolicy::viewer(cli.port),
            )?;
            let url = endpoint.url();
            if endpoint.reused {
                println!("\u{2713} using the CRM already running at {url}");
            } else {
                println!("\u{1F332} CRM dashboard at {url}  (ctrl-c to stop)");
                println!("         Strategy board: {url}/strategy");
                println!("         GTM Lab:        {url}/gtm");
            }
            // Open the strategy board first — the business side of the SDR —
            // so operators see goals and doctrine before the pipeline sheet.
            agent::open_browser(&format!("{url}/strategy"));
            if endpoint.reused {
                return Ok(());
            }
            rt.block_on(std::future::pending::<()>());
            Ok(())
        }

        Command::Gtm => {
            let playbooks = Arc::new(load_playbooks(&cli)?);
            let businesses = Arc::new(load_businesses(&cli)?);
            let endpoint = ensure_crm_server(
                &rt,
                &store,
                &db,
                &businesses,
                &playbooks,
                &library,
                CrmStartPolicy::viewer(cli.port),
            )?;
            let url = format!("{}/gtm/{}", endpoint.url(), cli.brand);
            println!("\u{2713} GTM Lab ready at {url}");
            agent::open_browser(&url);
            if endpoint.reused {
                return Ok(());
            }
            rt.block_on(std::future::pending::<()>());
            Ok(())
        }

        Command::Simulate {
            thesis,
            accounts,
            contacts,
            touches,
            report,
        } => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = load_playbooks(&cli)?;
            let pb = playbooks.get(&cli.brand)?;
            let touches = outreach::supported_touch_count_for_brand(&cli.brand, touches);
            eprintln!(
                "\u{2192} [{}] {thesis}\n\u{2192} {accounts}\u{00d7}{contacts}\u{00d7}{touches} \
                 (critique={critique}) via {} CLI",
                pb.name,
                cli.backend.as_str(),
            );
            let lib = rt.block_on(async { library.read().await.clone() });
            let campaign = rt.block_on(pipeline::simulate(
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
            let rendered = report::render(&campaign);
            if let Some(path) = report {
                std::fs::write(&path, &rendered).with_context(|| format!("writing {path}"))?;
                eprintln!("wrote {path}");
            } else {
                println!("{rendered}");
            }
            eprintln!(
                "\u{2713} synthetic simulation complete; no accounts, contacts, or touches were filed"
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
            segment,
            domains,
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
            let segment_runs = if cli.brand.eq_ignore_ascii_case("wapahki")
                && segment.is_none()
                && domains.is_empty()
            {
                db.list_market_segments(Some("wapahki"))?
                    .into_iter()
                    .filter(|candidate| candidate.status == "active")
                    .map(|candidate| Some(candidate.key))
                    .collect::<Vec<_>>()
            } else {
                vec![segment.clone()]
            };
            eprintln!(
                "\u{2192} [{}] sourcing across {} declared segment(s), then enriching through Apollo: {thesis}",
                pb.name,
                segment_runs.len()
            );
            let business_context = business.operating_context();
            let s = rt.block_on(async {
                let mut total = sourcing::SourceSummary::default();
                let run_count = segment_runs.len().max(1);
                for (index, segment_key) in segment_runs.iter().enumerate() {
                    let run_accounts = if run_count == 1 {
                        accounts
                    } else {
                        accounts / run_count + usize::from(index < accounts % run_count)
                    };
                    if run_accounts == 0 {
                        continue;
                    }
                    let run = sourcing::source(
                        &db,
                        &client,
                        &apollo,
                        pb,
                        &playbooks.shared,
                        &business.calendar.fallback_recipient_timezone,
                        &business_context,
                        &lib,
                        &thesis,
                        run_accounts,
                        contacts,
                        if domains.is_empty() {
                            None
                        } else {
                            Some(domains.as_slice())
                        },
                        segment_key.as_deref(),
                        None,
                        cli.concurrency,
                        None,
                    )
                    .await?;
                    total.orgs_found += run.orgs_found;
                    total.candidates_new += run.candidates_new;
                    total.leads_qualified += run.leads_qualified;
                    total.leads_research_needed += run.leads_research_needed;
                    total.leads_research_required += run.leads_research_required;
                    total.people_added += run.people_added;
                }
                Ok::<_, anyhow::Error>(total)
            })?;
            println!(
                "\u{2713} {} orgs \u{2192} {} action-ready, {} discovery-ready, {} research-required leads; {} people.\n  next: spruce-leaf --brand {} enrich",
                s.orgs_found,
                s.leads_qualified,
                s.leads_research_needed,
                s.leads_research_required,
                s.people_added,
                cli.brand
            );
            Ok(())
        }

        Command::Research { account, thesis } => {
            let mut matches = db
                .list_leads(Some(&cli.brand))?
                .into_iter()
                .filter(|lead| {
                    lead.id.eq_ignore_ascii_case(account.trim())
                        || lead.name.eq_ignore_ascii_case(account.trim())
                        || lead.domain.eq_ignore_ascii_case(account.trim())
                })
                .collect::<Vec<_>>();
            if matches.is_empty() {
                let needle = account.trim().to_ascii_lowercase();
                matches = db
                    .list_leads(Some(&cli.brand))?
                    .into_iter()
                    .filter(|lead| {
                        lead.name.to_ascii_lowercase().contains(&needle)
                            || lead.domain.to_ascii_lowercase().contains(&needle)
                    })
                    .collect();
            }
            if matches.len() != 1 {
                let found = matches
                    .iter()
                    .map(|lead| lead.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(anyhow!(
                    "account query must resolve to exactly one {} lead; found {}{}",
                    cli.brand,
                    matches.len(),
                    if found.is_empty() {
                        String::new()
                    } else {
                        format!(": {found}")
                    }
                ));
            }
            let lead = matches.remove(0);
            let client = make_engine(&rt, &cli)?;
            let playbooks = load_playbooks(&cli)?;
            let pb = playbooks.get(&cli.brand)?;
            let businesses = load_businesses(&cli)?;
            let business = businesses.get(&cli.brand)?;
            let lib = rt.block_on(async { library.read().await.clone() });
            eprintln!(
                "\u{2192} [{}] re-researching {} against the current play (no Apollo)",
                pb.name, lead.name
            );
            let refreshed = rt.block_on(sourcing::refresh_lead_context(
                &db,
                &client,
                pb,
                &business.operating_context(),
                &lib,
                &thesis,
                std::slice::from_ref(&lead.id),
                1,
                true,
            ))?;
            let play = db
                .current_gtm_play(&cli.brand)?
                .ok_or_else(|| anyhow!("no active GTM play for {}", cli.brand))?;
            let assessment = db
                .account_play_assessment(&lead.id, &play.id)?
                .ok_or_else(|| {
                    anyhow!("research produced no current assessment for {}", lead.name)
                })?;
            println!(
                "\u{2713} refreshed {refreshed} account(s): {} is {} for {} v{} (score {}, signals {}).",
                lead.name,
                assessment.status,
                play.name,
                play.version,
                assessment.fit_score,
                assessment.matched_signal_keys.len(),
            );
            if !assessment.evidence_gaps.is_empty() {
                println!("  evidence gaps: {}", assessment.evidence_gaps.join(" | "));
            }
            Ok(())
        }

        Command::OutageEvidence {
            archive,
            output,
            locations,
        } => {
            eprintln!("\u{2192} matching verified Canadian operating locations to historical OutageHub polygons");
            let report = match locations {
                Some(locations) => outage_evidence::build_verified_location_report(
                    Path::new(&archive),
                    Path::new(&locations),
                    Path::new(&output),
                )?,
                None => rt.block_on(outage_evidence::build_report(
                    Path::new(&archive),
                    Path::new(&output),
                ))?,
            };
            let source_groups = report
                .matches
                .iter()
                .map(|matched| {
                    if matched.company.trim().is_empty() {
                        matched.network.as_str()
                    } else {
                        matched.company.as_str()
                    }
                })
                .collect::<std::collections::HashSet<_>>()
                .len();
            println!(
                "\u{2713} {} verified location/polygon matches across {} operator/network groups written to {}.",
                report.matches.len(),
                source_groups,
                output
            );
            if !report.geocoding_attribution.trim().is_empty() {
                println!("  {}", report.geocoding_attribution);
            }
            Ok(())
        }

        Command::Enrich {
            limit,
            person,
            phone,
        } => {
            let apollo = make_apollo()?;
            let only_person_ids = if let Some(filter) = person.as_deref() {
                let filter = filter.trim();
                let matches = db
                    .list_people(Some(&cli.brand), None)?
                    .into_iter()
                    .filter(|candidate| {
                        candidate.id.eq_ignore_ascii_case(filter)
                            || candidate.email.eq_ignore_ascii_case(filter)
                            || candidate.name.eq_ignore_ascii_case(filter)
                    })
                    .collect::<Vec<_>>();
                if matches.len() != 1 {
                    return Err(anyhow!(
                        "--person '{}' matched {} people in {}; use the exact person id",
                        filter,
                        matches.len(),
                        cli.brand
                    ));
                }
                Some(
                    matches
                        .into_iter()
                        .map(|candidate| candidate.id)
                        .collect::<std::collections::HashSet<_>>(),
                )
            } else {
                None
            };
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
                only_person_ids.as_ref(),
                None,
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

        Command::Plan {
            touches,
            accounts,
            contacts,
            limit,
            auto,
            person,
            outcome,
            replace_drafts,
        } => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = load_playbooks(&cli)?;
            let pb = playbooks.get(&cli.brand)?;
            let touches = outreach::supported_touch_count_for_brand(&cli.brand, touches);
            let businesses = load_businesses(&cli)?;
            let business = businesses.get(&cli.brand)?;
            let lib = rt.block_on(async { library.read().await.clone() });
            let scoped = accounts.is_some() || contacts.is_some() || limit.is_some();
            let only_person_ids = if scoped {
                let leads = db.list_leads(Some(&cli.brand))?;
                let people = db.list_people(Some(&cli.brand), Some("verified"))?;
                let account_count = accounts.unwrap_or_else(|| {
                    if contacts.is_some() {
                        1
                    } else {
                        leads.len().max(1)
                    }
                });
                let per_account = contacts.unwrap_or(usize::MAX);
                let total = limit.unwrap_or(usize::MAX);
                let mut eligible_accounts = Vec::new();
                for (account_order, lead) in leads.into_iter().enumerate() {
                    let mut account_people = people
                        .iter()
                        .filter(|person| person.lead_id == lead.id)
                        .filter(|_person| {
                            !pb.max_employees
                                .is_some_and(|max| lead.headcount > 0 && lead.headcount > max)
                        })
                        .filter(|person| {
                            crate::gtm::recipient_sequence_block_reason(
                                &db, &cli.brand, &lead.id, person, touches,
                            )
                            .is_ok_and(|reason| reason.is_none())
                        })
                        .filter_map(|person| {
                            crate::gtm::prepare_action(&db, &cli.brand, &lead.id, person)
                                .ok()
                                .filter(|context| context.sequence_ready_for(touches))
                                .map(|context| {
                                    (
                                        person.clone(),
                                        // Evidence state stays dominant; for
                                        // Wapahki the closing-difficulty band
                                        // breaks ties so near-term accounts
                                        // are drafted before enterprise ones.
                                        crate::gtm::planning_priority(
                                            &context,
                                            &cli.brand,
                                            lead.headcount,
                                        ),
                                    )
                                })
                        })
                        .collect::<Vec<_>>();
                    account_people.sort_by(|left, right| {
                        response_design::contact_priority(
                            &right.0.title,
                            &right.0.vantage,
                            right.0.primary,
                        )
                        .cmp(&response_design::contact_priority(
                            &left.0.title,
                            &left.0.vantage,
                            left.0.primary,
                        ))
                        .then_with(|| left.0.name.cmp(&right.0.name))
                    });
                    if let Some(state_priority) = account_people.iter().map(|(_, rank)| *rank).min()
                    {
                        eligible_accounts.push((state_priority, account_order, account_people));
                    }
                }
                // `--accounts` is a count of eligible sales accounts, not a
                // window over raw insertion order. Hard/rejected rows must not
                // consume the window and hide older action- or discovery-ready
                // opportunities.
                eligible_accounts
                    .sort_by_key(|(state_priority, order, _)| (*state_priority, *order));
                let selected = eligible_accounts
                    .into_iter()
                    .take(account_count)
                    .flat_map(|(_, _, people)| {
                        people
                            .into_iter()
                            .map(|(person, _)| person)
                            .take(per_account)
                    })
                    .collect::<Vec<_>>();
                // Treat --limit as a portfolio target, not merely "the first N
                // rows." Preserve already-approved recipients inside the target,
                // then prefer untouched qualified contacts over repeatedly paying
                // to regenerate a weak recipient who already failed review.
                let mut ranked = selected
                    .into_iter()
                    .enumerate()
                    .map(|(order, person)| {
                        let rank =
                            if db.person_has_current_reviewed_sequence(&person.id, touches)? {
                                0
                            } else if !db.person_has_current_policy_attempt(&person.id)? {
                                1
                            } else {
                                2
                            };
                        Ok((rank, order, person))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                ranked.sort_by_key(|(rank, order, _)| (*rank, *order));
                let selected = ranked
                    .into_iter()
                    .take(total)
                    .map(|(_, _, person)| person)
                    .collect::<Vec<_>>();
                Some(
                    selected
                        .into_iter()
                        .map(|person| person.id)
                        .collect::<std::collections::HashSet<_>>(),
                )
            } else {
                None
            };
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
                critique,
                person.as_deref(),
                replace_drafts,
                if scoped {
                    None
                } else {
                    Some(
                        business
                            .account_limits
                            .max_active_contacts_per_account
                            .clamp(1, 2),
                    )
                },
                only_person_ids.as_ref(),
                outcome.as_deref(),
                None,
            ))?;
            println!(
                "{} planned {} people: {} touches scheduled, {} drafted, {} rejected, {} stopped, {} old draft sequence(s) replaced/pruned.{}{}",
                if s.stopped_reason.is_some() { "!" } else { "\u{2713}" },
                s.people_planned,
                s.touches_scheduled,
                s.touches_drafted,
                s.people_rejected,
                s.people_stopped,
                s.sequences_replaced,
                if auto {
                    ""
                } else {
                    "\n  approve with: spruce-leaf approve"
                },
                s.stopped_reason
                    .as_ref()
                    .map(|reason| format!("\n  stopped early: {reason}"))
                    .unwrap_or_default(),
            );
            Ok(())
        }

        Command::EvalOutreach {
            corpus,
            double_blind,
        } => {
            let client = make_engine(&rt, &cli)?;
            rt.block_on(outreach_eval::run(
                &client,
                Path::new(&corpus),
                double_blind,
            ))?;
            Ok(())
        }

        Command::EvalOutreachAblation {
            corpus,
            cases,
            repeats,
            show_drafts,
            only,
        } => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = load_playbooks(&cli)?;
            rt.block_on(outreach_ablation::run(
                &client,
                &playbooks,
                Path::new(&corpus),
                outreach_ablation::Options {
                    case_limit: cases,
                    repeats,
                    concurrency: cli.concurrency,
                    show_drafts,
                    only: only.as_deref(),
                },
            ))?;
            Ok(())
        }

        Command::PilotAudit {
            accounts,
            segments,
            messages,
            approvals,
        } => {
            let playbooks = load_playbooks(&cli)?;
            let defaults = pilot::PilotThresholds::for_brand(&cli.brand);
            let accounts = accounts.unwrap_or(defaults.accounts);
            let segments = segments.unwrap_or(defaults.segments);
            let messages = messages.unwrap_or(defaults.generated_messages);
            let approvals = approvals.unwrap_or(defaults.exact_approvals);
            let audit = pilot::audit(
                &db, &playbooks, &cli.brand, accounts, segments, messages, approvals,
            )?;
            println!(
                "{} pilot audit: {} real researched account(s) across {} segment(s); {} distinct-account generated/current message(s); {} with full selector provenance; {} exact-copy approvals on distinct accounts; {} distinct approved facilities; {} complete Wapahki Task Briefs; {} allowlisted SMTP delivery/deliveries.",
                cli.brand,
                audit.researched_accounts,
                audit.segments.len(),
                audit.generated_messages,
                audit.selector_provenance_messages,
                audit.approved_distinct_accounts,
                audit.approved_distinct_facilities,
                audit.complete_wapahki_task_briefs,
                audit.allowlisted_smtp_messages,
            );
            if !audit.segments.is_empty() {
                println!("  segments: {}", audit.segments.join(", "));
            }
            for issue in audit.wrong_role_sequences.iter().take(10) {
                println!("  wrong role: {issue}");
            }
            for issue in audit.unsupported_sequences.iter().take(10) {
                println!("  copy/evidence: {issue}");
            }
            if !audit.passed() {
                for blocker in &audit.blockers {
                    println!("  BLOCKED: {blocker}");
                }
                return Err(anyhow!(
                    "{} supervised-pilot threshold is not yet satisfied",
                    cli.brand
                ));
            }
            println!("✓ {} supervised-pilot threshold satisfied.", cli.brand);
            Ok(())
        }

        Command::SalesPackets {
            opportunity,
            output,
        } => {
            let summary = sales_packet::export(&db, &cli.brand, opportunity.as_deref(), &output)?;
            println!(
                "✓ exported {} local sales decision package(s)",
                summary.packets
            );
            for path in summary.paths {
                println!("  {}", path.display());
            }
            Ok(())
        }

        Command::AcceptanceReport { output } => {
            let summary = acceptance::export(&db, &cli.brand, &output)?;
            println!(
                "{} supervised acceptance report: {} companies, {} contacts, {} candidate decisions",
                if summary.passed { "✓" } else { "BLOCKED" },
                summary.companies,
                summary.contacts,
                summary.candidates
            );
            println!("  {}", summary.directory.display());
            Ok(())
        }

        Command::ImportSalesBrief { file } => {
            let id = sales_packet::import_sales_brief(&db, &file)?;
            println!("✓ imported local sales brief {id}; no copy was approved or sent");
            Ok(())
        }

        Command::ImportAcquisitionContext { file } => {
            let ids = sales_packet::import_acquisition_context(&db, &file)?;
            println!(
                "✓ imported {} acquisition context(s); no outreach was generated, approved, or sent",
                ids.len()
            );
            Ok(())
        }

        Command::ImportConditionalFollowup { file } => {
            let id = sales_packet::import_conditional_followup(&db, &file)?;
            println!("✓ imported conditional follow-up {id}; no message was generated or sent");
            Ok(())
        }

        Command::ImportDiscoveryQualification { file } => {
            let id = sales_packet::import_discovery_qualification(&db, &file)?;
            println!("✓ imported reply-backed discovery qualification {id}");
            Ok(())
        }

        Command::ImportSalesApplication { file } => {
            let id = sales_packet::import_sales_application(&db, &file)?;
            println!("✓ imported reply-backed sales application brief {id}");
            Ok(())
        }

        Command::ImportProofAsset { file } => {
            let id = sales_packet::import_proof_asset(&db, &file)?;
            println!("✓ imported validated proof asset {id}; no copy was approved or sent");
            Ok(())
        }

        Command::PrepareProofAsset {
            opportunity,
            person,
            asset_type,
            output,
        } => {
            let asset = sales_packet::prepare_proof_asset(
                &db,
                &opportunity,
                &person,
                &asset_type,
                &output,
            )?;
            println!(
                "✓ prepared {} proof asset {} at {}; no outreach was generated or sent",
                asset.status, asset.id, asset.rendered_path
            );
            Ok(())
        }

        Command::Approve { person } => {
            let playbooks = load_playbooks(&cli)?;
            let pb = playbooks.get(&cli.brand)?;
            let approval = outreach::approve_ready_touches(&db, pb, person.as_deref())?;
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            let plan = calendar::rebalance_approved_sales(&db, profile, chrono::Utc::now())?;
            println!(
                "\u{2713} approved {} touch(es) \u{2192} {} email(s) placed across {} active day(s); {} new conversation(s) admitted; {} recipient(s) held by GTM policy.",
                approval.touches_scheduled,
                plan.emails,
                plan.active_days,
                plan.admitted_people,
                approval.people_held,
            );
            for reason in approval.hold_reasons.iter().take(5) {
                println!("  held: {reason}");
            }
            Ok(())
        }

        Command::Daemon {
            live,
            autopilot,
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
            if live
                && std::env::var("SPRUCE_CASL_PROGRAM_APPROVAL_REF")
                    .unwrap_or_default()
                    .trim()
                    .is_empty()
            {
                anyhow::bail!(
                    "refusing live sending: SPRUCE_CASL_PROGRAM_APPROVAL_REF is unset; record Canadian-counsel program approval before launch"
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
                for brand in businesses.keys() {
                    let profile = businesses.get(brand)?;
                    let plan =
                        calendar::rebalance_approved_sales(&db, profile, chrono::Utc::now())?;
                    if plan.emails > 0 {
                        eprintln!(
                            "\u{2713} [{}] calendar: {} approved email(s), {} new conversation(s), {} active day(s)",
                            brand, plan.emails, plan.admitted_people, plan.active_days
                        );
                    }
                }
            }
            let cfg = cadence::CadenceConfig {
                dry_run: !live,
                batch,
                interval_secs: interval,
                ..Default::default()
            };
            // Fill-only autopilot is deliberately safe to run without mailbox
            // credentials. Preview the cadence once (read-only), then keep the
            // sourcing/enrichment/drafting supervisor in the foreground. The
            // worker's auto_schedule default is false, so new copy remains
            // behind the normal human approval gate as well as the dry-run
            // delivery boundary.
            if autopilot && !live {
                rt.block_on(cadence::run_daemon(
                    db.clone(),
                    playbooks.clone(),
                    businesses.clone(),
                    compliance,
                    cfg,
                ))?;
                let autopilot_client = make_engine(&rt, &cli)?;
                rt.block_on(jobs::run_daemon(
                    db,
                    autopilot_client,
                    playbooks,
                    businesses,
                    library,
                    cli.concurrency,
                    interval,
                ));
                return Ok(());
            }
            // Inbox polling runs alongside live cadence only. A dry preview is
            // intentionally read-only and must not mark inbound mail handled.
            if live {
                let inbox_client = make_engine(&rt, &cli)?;
                let dbi = db.clone();
                let inbox_playbooks = playbooks.clone();
                let allow_booking = autopilot;
                rt.spawn(async move {
                    loop {
                        if let Err(e) = inbox::poll_all(
                            &dbi,
                            &inbox_client,
                            &inbox_playbooks,
                            None,
                            allow_booking,
                        )
                        .await
                        {
                            eprintln!("  ! inbox poll: {e:#}");
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(interval.max(30))).await;
                    }
                });
            }
            if autopilot {
                let autopilot_client = make_engine(&rt, &cli)?;
                let autopilot_db = db.clone();
                let autopilot_playbooks = playbooks.clone();
                let autopilot_businesses = businesses.clone();
                let autopilot_library = library.clone();
                let concurrency = cli.concurrency;
                rt.spawn(jobs::run_daemon(
                    autopilot_db,
                    autopilot_client,
                    autopilot_playbooks,
                    autopilot_businesses,
                    autopilot_library,
                    concurrency,
                    interval,
                ));
            }
            rt.block_on(cadence::run_daemon(
                db.clone(),
                playbooks,
                businesses,
                compliance,
                cfg,
            ))
        }

        Command::SeedTestLead {
            email,
            name,
            company,
            timezone,
        } => {
            let (first, last) = match name.split_once(' ') {
                Some((f, l)) => (f.to_string(), l.to_string()),
                None => (name.clone(), String::new()),
            };
            let domain = email.split('@').nth(1).unwrap_or("example.com").to_string();
            let lead_id = db.upsert_lead(&crate::db::Lead {
                brand: cli.brand.clone(),
                apollo_org_id: format!("seed-test:{email}"),
                name: company,
                domain,
                industry: "test".into(),
                hq: "Toronto, Ontario, Canada".into(),
                timezone: timezone.clone(),
                thesis: "closed-loop send/reply self-test".into(),
                hypothesis: "seeded record for exercising the live daemon end to end".into(),
                status: "qualified".into(),
                ..Default::default()
            })?;
            let person_id = db.upsert_person(&crate::db::Person {
                lead_id,
                brand: cli.brand.clone(),
                apollo_person_id: format!("seed-test:{email}"),
                first_name: first,
                last_name: last,
                name: name.clone(),
                title: "Operations Lead".into(),
                timezone,
                email: email.clone(),
                email_status: "verified".into(),
                status: "verified".into(),
                ..Default::default()
            })?;
            println!(
                "\u{2713} seeded verified test person for {} \u{2192} {person_id} <{email}>",
                cli.brand
            );
            println!(
                "  plan:  spruce-leaf --brand {} plan --auto --person {person_id}",
                cli.brand
            );
            println!(
                "  send:  SPRUCE_SEND_ALLOWLIST={email} spruce-leaf --brand {} daemon --live",
                cli.brand
            );
            Ok(())
        }

        Command::Inbox { book } => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = load_playbooks(&cli)?;
            let n = rt.block_on(inbox::poll_all(&db, &client, &playbooks, None, book))?;
            println!("\u{2713} handled {n} reply/replies.");
            Ok(())
        }

        Command::ApproveReplies { conversation } => {
            let n = db.approve_conversation_messages(Some(&cli.brand), conversation.as_deref())?;
            println!("\u{2713} approved {n} reply draft(s) \u{2192} scheduled.");
            Ok(())
        }

        Command::Meetings => {
            let meetings = db.list_meetings(Some(&cli.brand))?;
            if meetings.is_empty() {
                println!("no meetings for {}", cli.brand);
            }
            for meeting in meetings {
                println!(
                    "{}  {}  {}  {}{}",
                    meeting.starts_at,
                    meeting.status,
                    meeting.attendee_email,
                    meeting.html_link,
                    if meeting.meet_link.is_empty() {
                        String::new()
                    } else {
                        format!("  {}", meeting.meet_link)
                    }
                );
            }
            Ok(())
        }

        Command::BookMeetings { id } => {
            let n = rt.block_on(reply_agent::book_pending(&db, &cli.brand, id.as_deref()))?;
            println!("\u{2713} booked {n} pending meeting(s); Google sent attendee updates.");
            Ok(())
        }

        Command::Jobs => {
            let counts = db.job_status_counts(Some(&cli.brand))?;
            if counts.is_empty() {
                println!("no autopilot jobs for {}", cli.brand);
            } else {
                println!("autopilot jobs [{}]", cli.brand);
                for (status, count) in counts {
                    println!("  {status:<10} {count}");
                }
            }
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

        Command::Synthesize { out } => {
            let client = make_engine(&rt, &cli)?;
            let playbooks = load_playbooks(&cli)?;
            let pb = playbooks.get(&cli.brand)?;
            eprintln!(
                "\u{2192} [{}] synthesizing discovery across sourced companies\u{2026}",
                pb.name
            );
            let s = rt.block_on(synthesis::synthesize(&db, &client, pb))?;
            let path = if out.trim().is_empty() {
                format!(".spruce/synthesis-{}.md", cli.brand)
            } else {
                out
            };
            std::fs::write(&path, synthesis::render_markdown(&s))
                .with_context(|| format!("writing {path}"))?;
            println!("{}", synthesis::render_console(&s));
            println!("\n  wrote {path}");
            Ok(())
        }

        Command::Calendar { rebalance } => {
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            if rebalance {
                let plan = calendar::rebalance_approved_sales(&db, profile, chrono::Utc::now())?;
                println!(
                    "\u{2713} rebalanced {} approved email(s), protected {} active follow-up(s), and admitted {} new conversation(s) across {} active day(s).\n",
                    plan.emails,
                    plan.protected_followups,
                    plan.admitted_people,
                    plan.active_days,
                );
            }
            println!(
                "{}",
                calendar::render_intelligence(profile, &db, chrono::Utc::now())?
            );
            Ok(())
        }

        Command::Login { brand } => {
            let playbooks = load_playbooks(&cli)?;
            if playbooks.get(&brand).is_err() {
                anyhow::bail!(
                    "unknown brand '{brand}'. Available: {}",
                    playbooks.keys().join(", ")
                );
            }
            println!("Opening browser to link Gmail for {brand}…");
            let set = rt.block_on(google_oauth::login_brand(&brand, |url| {
                agent::open_browser(url);
            }))?;
            println!(
                "\u{2713} {brand} linked as {}  (tokens in .spruce/google/{brand}.json)",
                set.email
            );
            println!("Next: spruce-leaf mail-sync --brand {brand}");
            Ok(())
        }

        Command::MailStatus => {
            let playbooks = load_playbooks(&cli)?;
            println!("Gmail OAuth status:");
            println!(
                "{}",
                google_oauth::GoogleTokenSet::status_report(&playbooks.keys())
            );
            Ok(())
        }

        Command::MailSync { brand, limit } => {
            let summaries = rt.block_on(gmail::sync_all(&db, brand.as_deref(), limit))?;
            println!(
                "\u{2713} Gmail sync complete:\n{}",
                gmail::format_sync_report(&summaries)
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

        Command::SeedSponsorships { limit } => {
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            let summary = opportunity::seed_sponsorships(&db, profile, limit)?;
            println!(
                "\u{2713} sponsorship qualification considered {} account(s): {} added, {} updated, {} verified budget-role contact(s) mapped.",
                summary.accounts_considered,
                summary.opportunities_added,
                summary.opportunities_updated,
                summary.contacts_mapped,
            );
            println!(
                "  held: {} without route fit, {} without relevance evidence, {} without source-backed budget/program evidence, {} without a verified route-matched person.",
                summary.skipped_without_target_fit,
                summary.skipped_without_evidence,
                summary.skipped_without_budget_evidence,
                summary.skipped_without_budget_contact,
            );
            Ok(())
        }

        Command::ImportSponsorshipResearch { file } => {
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            let summary = rt.block_on(opportunity::import_sponsorship_research(
                &db, profile, &file,
            ))?;
            println!(
                "\u{2713} sponsorship research re-verified {} row(s): {} imported, {} rejected.",
                summary.rows_read, summary.imported, summary.rejected,
            );
            if !summary.opportunity_ids.is_empty() {
                println!("  opportunity ids: {}", summary.opportunity_ids.join(", "));
            }
            for error in summary.errors {
                println!("  held: {error}");
            }
            Ok(())
        }

        Command::PlanSponsorshipOutreach {
            opportunity: opportunity_id,
            contact,
            touches,
            refresh,
        } => {
            let client = make_engine(&rt, &cli)?;
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            let playbooks = load_playbooks(&cli)?;
            let playbook = playbooks.get(&cli.brand)?;
            let summary = rt.block_on(opportunity::plan_sponsorship_outreach(
                &db,
                &client,
                profile,
                playbook,
                &playbooks.shared,
                opportunity::SponsorshipOutreachOptions {
                    opportunity_id: &opportunity_id,
                    contact_id: contact.as_deref(),
                    touches,
                    refresh,
                },
            ))?;
            println!(
                "\u{2713} sponsorship review processed {} contact(s): {} manual draft(s), {} scheduled.",
                summary.contacts_planned,
                summary.touches_drafted,
                summary.touches_scheduled,
            );
            println!("  Sponsorship planning never auto-schedules; review every draft in the CRM.");
            Ok(())
        }

        Command::AuditSponsorshipOutreach { target } => {
            let audit = opportunity::audit_sponsorship_campaign(&db, &cli.brand, target)?;
            println!(
                "Sponsorship campaign QA: {}\n  organizations: {}/{}\n  ready drafts: {}\n  blocked drafts: {}\n  direct mailboxes: {}\n  routed mailboxes: {}\n  scheduled/sending/sent: {}",
                if audit.passes() { "PASS" } else { "HOLD" },
                audit.organizations,
                audit.target,
                audit.ready,
                audit.blocked,
                audit.direct_mailboxes,
                audit.routed_mailboxes,
                audit.scheduled_or_sent,
            );
            for issue in &audit.issues {
                println!("  issue: {issue}");
            }
            if !audit.passes() {
                anyhow::bail!("sponsorship campaign remains held");
            }
            Ok(())
        }

        Command::PrepareSponsorshipPack {
            opportunity: opportunity_id,
        } => {
            let businesses = load_businesses(&cli)?;
            let profile = businesses.get(&cli.brand)?;
            let sponsorship_opportunity = db
                .get_opportunity(&opportunity_id)?
                .ok_or_else(|| anyhow::anyhow!("opportunity '{opportunity_id}' not found"))?;
            let pack = opportunity::prepare_sponsorship_pack(profile, &sponsorship_opportunity)?;
            println!(
                "{}\nAsk: {}\n\nInfrastructure need:\n{}\n\nProduct truth:\n- {}\n\nPermitted sponsor benefits:\n- {}\n\nIndependence:\n- {}\n\nBuyer checks:\n- {}\n\nAgreement checks:\n- {}",
                pack.title,
                pack.price,
                pack.infrastructure_need,
                pack.product_truth.join("\n- "),
                pack.sponsor_benefits.join("\n- "),
                pack.independence_terms.join("\n- "),
                pack.buyer_checks.join("\n- "),
                pack.agreement_checks.join("\n- "),
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
            let endpoint = ensure_crm_server(
                &rt,
                &store,
                &db,
                &businesses,
                &playbooks,
                &library,
                CrmStartPolicy::interactive(cli.port),
            )?;
            if endpoint.reused {
                eprintln!("\u{2713} reusing CRM at {}", endpoint.url());
            } else {
                eprintln!("\u{2713} CRM ready at {}", endpoint.url());
                eprintln!("         Strategy board: {}/strategy", endpoint.url());
                eprintln!("         GTM Lab:        {}/gtm", endpoint.url());
            }
            let agent = agent::Agent::new(
                client,
                store.clone(),
                library.clone(),
                db.clone(),
                playbooks,
                businesses,
                cli.brand.clone(),
                critique,
                endpoint.port,
                cli.concurrency,
            );
            repl::run_repl(&rt, agent)
        }
    }
}

const DEFAULT_CRM_PORT: u16 = 8787;
const BOOTSTRAPPED_ENV: &str = "SPRUCE_LEAF_BOOTSTRAPPED";

#[derive(Debug, Clone, Copy)]
struct CrmEndpoint {
    port: u16,
    reused: bool,
}

impl CrmEndpoint {
    fn url(self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

#[derive(Debug, Clone, Copy)]
struct CrmStartPolicy {
    preferred_port: Option<u16>,
    allow_reuse: bool,
}

impl CrmStartPolicy {
    fn viewer(preferred_port: Option<u16>) -> Self {
        Self {
            preferred_port,
            allow_reuse: true,
        }
    }

    fn interactive(preferred_port: Option<u16>) -> Self {
        Self {
            preferred_port,
            allow_reuse: false,
        }
    }
}

/// The installed command is a tiny hand-off point to the current workspace
/// build. Cargo is incremental, so unchanged launches are fast; changed source
/// and missing dependencies are rebuilt/downloaded before the real CLI starts.
fn bootstrap_latest_workspace_binary() -> Result<()> {
    if std::env::var_os(BOOTSTRAPPED_ENV).is_some() {
        return Ok(());
    }

    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if !workspace.join("Cargo.toml").is_file() || !workspace.join("src/main.rs").is_file() {
        return Ok(());
    }

    let binary_name = if cfg!(windows) {
        "spruce-leaf.exe"
    } else {
        "spruce-leaf"
    };
    let workspace_binary = workspace.join("target").join("debug").join(binary_name);
    let current_binary = std::env::current_exe().context("finding the spruce-leaf executable")?;
    if !workspace_inputs_are_newer(&workspace, &current_binary) {
        return Ok(());
    }
    let build_is_stale = workspace_inputs_are_newer(&workspace, &workspace_binary);

    if build_is_stale {
        eprintln!("\u{21bb} preparing the latest local Spruce Leaf build\u{2026}");
        let status = ProcessCommand::new("cargo")
            .arg("build")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(workspace.join("Cargo.toml"))
            .current_dir(&workspace)
            .status()
            .context(
                "running Cargo; install Rust from https://rustup.rs if `cargo` is unavailable",
            )?;
        if !status.success() {
            anyhow::bail!("Cargo could not build the latest local Spruce Leaf version");
        }
    }

    let args = std::env::args_os().skip(1).collect::<Vec<OsString>>();
    let mut next = ProcessCommand::new(&workspace_binary);
    next.args(args).env(BOOTSTRAPPED_ENV, "1");

    #[cfg(unix)]
    {
        let error = next.exec();
        Err(error).with_context(|| format!("launching {}", workspace_binary.display()))
    }

    #[cfg(not(unix))]
    {
        let status = next
            .status()
            .with_context(|| format!("launching {}", workspace_binary.display()))?;
        std::process::exit(status.code().unwrap_or(1));
    }
}

fn workspace_inputs_are_newer(workspace: &Path, binary: &Path) -> bool {
    let binary_modified = std::fs::metadata(binary)
        .and_then(|metadata| metadata.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut newest = SystemTime::UNIX_EPOCH;
    for input in [
        workspace.join("Cargo.toml"),
        workspace.join("Cargo.lock"),
        workspace.join("build.rs"),
        workspace.join("src"),
    ] {
        update_newest_mtime(&input, &mut newest);
    }
    newest > binary_modified
}

fn update_newest_mtime(path: &Path, newest: &mut SystemTime) {
    let Ok(metadata) = std::fs::metadata(path) else {
        return;
    };
    if let Ok(modified) = metadata.modified() {
        *newest = (*newest).max(modified);
    }
    if metadata.is_dir() {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            update_newest_mtime(&entry.path(), newest);
        }
    }
}

/// Reuse a running Spruce Leaf CRM when one is already present. Otherwise bind
/// the preferred port or the next free loopback port before spawning, avoiding
/// the check-then-bind race that caused the old silent CRM failures.
fn ensure_crm_server(
    rt: &tokio::runtime::Runtime,
    store: &crm::SharedStore,
    db: &db::SharedDb,
    businesses: &Arc<Businesses>,
    playbooks: &Arc<Playbooks>,
    library: &knowledge::SharedLibrary,
    policy: CrmStartPolicy,
) -> Result<CrmEndpoint> {
    let first = policy.preferred_port.unwrap_or(DEFAULT_CRM_PORT);
    if policy.allow_reuse {
        let existing_ports = if policy.preferred_port.is_some() {
            vec![first]
        } else {
            crm::port_candidates(first)
        };
        if let Some(port) = existing_ports.into_iter().find(|port| is_spruce_crm(*port)) {
            return Ok(CrmEndpoint { port, reused: true });
        }
    }

    let listener = bind_available_crm_listener(first)?;
    let port = listener
        .local_addr()
        .context("reading selected CRM port")?
        .port();
    let store = store.clone();
    let db = db.clone();
    let businesses = businesses.clone();
    let playbooks = playbooks.clone();
    let library = library.clone();
    rt.spawn(async move {
        if let Err(error) =
            crm::serve_on_listener(store, db, businesses, playbooks, library, listener).await
        {
            eprintln!("CRM server error: {error:#}");
        }
    });
    // The spawn above is fire-and-forget: if the server failed to start we would
    // otherwise advertise a dead port and every "in the CRM at …" message would
    // be a lie. Wait for the health endpoint to actually answer before we call it
    // ready — the socket is already listening, so this resolves in a few ms.
    let mut ready = false;
    for _ in 0..40 {
        if is_spruce_crm(port) {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if !ready {
        anyhow::bail!("CRM failed to become healthy at http://127.0.0.1:{port}");
    }
    Ok(CrmEndpoint {
        port,
        reused: false,
    })
}

fn bind_available_crm_listener(first: u16) -> Result<TcpListener> {
    crm::bind_free_listener(first)
}

fn is_spruce_crm(port: u16) -> bool {
    crm::is_live(port)
}

/// Build the selected local-CLI engine and preflight that it is available.
fn make_engine(rt: &tokio::runtime::Runtime, cli: &Cli) -> Result<Engine> {
    let engine = Engine::new(cli.backend, cli.model.clone());
    let version = rt.block_on(engine.check())?;
    // Print as `backend · version` so a version string that already names the
    // product (e.g. `2.1.170 (Claude Code)`) is not double-tagged as `(claude)`.
    eprintln!(
        "\u{2713} reasoning engine: {} · {}",
        engine.backend().as_str(),
        version
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

#[cfg(test)]
mod startup_tests {
    use super::{bind_available_crm_listener, is_spruce_crm, Backend, Cli, CrmEndpoint};
    use crate::crm::CRM_PROTOCOL_REV;
    use clap::Parser;
    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
    use std::thread;

    #[test]
    fn codex_is_the_default_inference_backend() {
        let cli = Cli::try_parse_from(["spruce-leaf"]).expect("parse defaults");
        assert_eq!(cli.backend, Backend::Codex);
    }

    #[test]
    fn free_port_selection_skips_an_occupied_preference() {
        let occupied = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .expect("bind occupied port");
        let preferred = occupied.local_addr().expect("occupied address").port();
        let selected = bind_available_crm_listener(preferred).expect("select another port");
        assert_ne!(
            selected.local_addr().expect("selected address").port(),
            preferred
        );
    }

    #[test]
    fn health_probe_recognizes_an_existing_spruce_crm() {
        let listener =
            TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind test CRM");
        let port = listener.local_addr().expect("test CRM address").port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept health probe");
            let mut request = [0_u8; 512];
            let _ = stream.read(&mut request);
            let response = format!(
                "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n{{\"app\":\"spruce-leaf\",\"protocol\":{CRM_PROTOCOL_REV}}}"
            );
            stream
                .write_all(response.as_bytes())
                .expect("write health response");
        });

        assert!(is_spruce_crm(port));
        server.join().expect("join test CRM");
    }

    #[test]
    fn crm_urls_use_unambiguous_ipv4_loopback() {
        assert_eq!(
            CrmEndpoint {
                port: 8799,
                reused: true,
            }
            .url(),
            "http://127.0.0.1:8799"
        );
    }
}
