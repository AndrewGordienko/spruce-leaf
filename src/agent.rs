//! The agent loop, on a pluggable local-CLI underbase (Claude, Codex, or Grok).
//!
//! The CLI doesn't hand back API-style `tool_use` blocks, so instead of a raw
//! tool loop we use a *structured router*: each user line is sent to the selected model with
//! a schema that makes it choose one action — research a campaign, source real
//! Apollo leads, enrich/plan/approve execution, inspect the CRM, or just reply —
//! which we then execute in Rust. A short rolling transcript is included for
//! conversational continuity.

use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::apollo::Apollo;
use crate::business::Businesses;
use crate::calendar;
use crate::crm::SharedStore;
use crate::db::SharedDb;
use crate::engine::{Backend, Engine, ModelSwitch, StatsSnapshot};
use crate::knowledge::SharedLibrary;
use crate::pipeline;
use crate::playbook::Playbooks;
use crate::ui;
use crate::{enrich, metrics, opportunity, outreach, sourcing};

/// How many past turns to feed back for continuity.
const HISTORY_TURNS: usize = 6;

pub struct Agent {
    client: Engine,
    store: SharedStore,
    library: SharedLibrary,
    db: SharedDb,
    playbooks: Arc<Playbooks>,
    businesses: Arc<Businesses>,
    /// The current working/fallback brand (gnk | wapahki | outagehub). In auto
    /// mode this is just the last brand a request resolved to; in pinned mode the
    /// operator locked it with `/brand <key>`.
    brand: String,
    /// When false (the default), the session is brand-agnostic: the router infers
    /// the brand for each request and bare reads span the whole portfolio. Pinned
    /// via `/brand <key>`; released via `/brand auto`.
    brand_pinned: bool,
    critique: bool,
    port: u16,
    concurrency: usize,
    history: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct Decision {
    /// Conversational answer, used ONLY when there are no steps to run.
    #[serde(default)]
    reply: String,
    /// The actions to run this turn, in order — one (or more) per brand, and they
    /// may be different actions for different brands. Empty for a pure reply.
    #[serde(default)]
    steps: Vec<Step>,
}

/// One action for one brand within a turn.
#[derive(Deserialize)]
struct Step {
    action: String,
    #[serde(default)]
    brand: String,
    #[serde(default)]
    thesis: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    accounts: Option<u64>,
    #[serde(default)]
    contacts: Option<u64>,
    #[serde(default)]
    touches: Option<u64>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    phone: bool,
    #[serde(default)]
    auto: bool,
    /// Force Apollo for *new* accounts even when the CRM already has enough inventory.
    #[serde(default)]
    force_new: bool,
    #[serde(default)]
    enrich: bool,
    #[serde(default)]
    actionable: bool,
    #[serde(default)]
    opportunity_id: String,
}

#[derive(Debug)]
struct PlanScopeAccount {
    name: String,
    requested: usize,
    person_ids: Vec<String>,
}

#[derive(Debug)]
struct PlanScope {
    requested_people: usize,
    selected_ids: std::collections::HashSet<String>,
    pending_enrichment_ids: std::collections::HashSet<String>,
    accounts: Vec<PlanScopeAccount>,
}

fn explicit_total_cap(input: &str) -> bool {
    let normalized = input.to_ascii_lowercase();
    normalized.contains("at most")
        || normalized
            .split(|character: char| !character.is_ascii_alphanumeric())
            .any(|word| {
                matches!(
                    word,
                    "total" | "overall" | "maximum" | "max" | "cap" | "capped" | "limit"
                )
            })
}

fn forbids_contact_enrichment(input: &str) -> bool {
    let normalized = input.to_ascii_lowercase();
    [
        "no apollo",
        "without apollo",
        "don't enrich",
        "do not enrich",
        "without enrichment",
        "no enrichment",
        "verified only",
    ]
    .iter()
    .any(|phrase| normalized.contains(phrase))
}

fn routed_total_limit(
    input: &str,
    accounts: Option<u64>,
    contacts: Option<u64>,
    limit: Option<u64>,
) -> Option<usize> {
    limit
        .filter(|_| accounts.is_none() || contacts.is_none() || explicit_total_cap(input))
        .map(|value| value.max(1) as usize)
}

fn select_plan_scope(
    db: &SharedDb,
    brand: &str,
    account_limit: Option<usize>,
    contacts_per_account: Option<usize>,
    total_limit: Option<usize>,
) -> Result<PlanScope> {
    let leads = db.list_leads(Some(brand))?;
    let people = db.list_people(Some(brand), None)?;
    let account_count = account_limit.unwrap_or_else(|| {
        if contacts_per_account.is_some() {
            1
        } else {
            leads.len().max(1)
        }
    });
    let per_account = contacts_per_account.unwrap_or(usize::MAX);
    let total = total_limit.unwrap_or(usize::MAX);
    let requested_people = match (contacts_per_account, total_limit) {
        (Some(contacts), cap) => account_count
            .saturating_mul(contacts)
            .min(cap.unwrap_or(usize::MAX)),
        (None, Some(cap)) => cap,
        (None, None) => 0,
    };
    let mut selected_ids = std::collections::HashSet::new();
    let mut pending_enrichment_ids = std::collections::HashSet::new();
    let mut accounts = Vec::new();

    for lead in leads.into_iter().take(account_count) {
        let remaining = total.saturating_sub(selected_ids.len());
        if remaining == 0 {
            break;
        }
        let mut roster = people
            .iter()
            .filter(|person| person.lead_id == lead.id)
            .cloned()
            .collect::<Vec<_>>();
        // This is the same order the CRM shows: workflow-primary contacts,
        // verified identities, then the rest in stable database order.
        roster.sort_by_key(|person| {
            (
                !person.primary,
                !person.email_status.eq_ignore_ascii_case("verified"),
                person.status.eq_ignore_ascii_case("suppressed"),
            )
        });
        let take = roster
            .into_iter()
            .take(per_account.min(remaining))
            .collect::<Vec<_>>();
        let requested_for_account = contacts_per_account
            .map(|_| per_account.min(remaining))
            .unwrap_or(take.len());
        let mut person_ids = Vec::new();
        for person in take {
            if person.status.eq_ignore_ascii_case("new")
                && !person.email_status.eq_ignore_ascii_case("verified")
            {
                pending_enrichment_ids.insert(person.id.clone());
            }
            selected_ids.insert(person.id.clone());
            person_ids.push(person.id);
        }
        accounts.push(PlanScopeAccount {
            name: lead.name,
            requested: requested_for_account,
            person_ids,
        });
    }

    Ok(PlanScope {
        requested_people: if requested_people == 0 {
            selected_ids.len()
        } else {
            requested_people
        },
        selected_ids,
        pending_enrichment_ids,
        accounts,
    })
}

impl Agent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Engine,
        store: SharedStore,
        library: SharedLibrary,
        db: SharedDb,
        playbooks: Arc<Playbooks>,
        businesses: Arc<Businesses>,
        brand: String,
        critique: bool,
        port: u16,
        concurrency: usize,
    ) -> Self {
        Self {
            client,
            store,
            library,
            db,
            playbooks,
            businesses,
            brand,
            brand_pinned: false,
            critique,
            port,
            concurrency,
            history: Vec::new(),
        }
    }

    pub fn crm_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Guarantee the CRM the agent advertises is actually answering before we
    /// open or cite it. With several concurrent sessions running, the port we
    /// were handed at startup can belong to a sibling that has since exited — its
    /// listener closes and the link starts refusing connections, which reads as
    /// "the localhost didn't turn on." Probe first; only if the CRM is genuinely
    /// down do we stand a fresh server back up on a free port and repoint at it.
    /// Runs inside the REPL's `block_on`, so `tokio::spawn` has an ambient runtime.
    async fn ensure_crm_live(&mut self) {
        // Two quick probes: a single dropped packet under load shouldn't trigger a
        // spurious restart, but a genuinely dead port should be caught fast.
        for attempt in 0..2 {
            let port = self.port;
            let alive = tokio::task::spawn_blocking(move || crate::crm::is_live(port))
                .await
                .unwrap_or(false);
            if alive {
                return;
            }
            if attempt == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            }
        }

        let listener = match crate::crm::bind_free_listener(self.port) {
            Ok(listener) => listener,
            Err(error) => {
                ui::activity("CRM unavailable", format!("{error:#}"));
                return;
            }
        };
        let port = listener
            .local_addr()
            .map(|addr| addr.port())
            .unwrap_or(self.port);
        let store = self.store.clone();
        let db = self.db.clone();
        let businesses = self.businesses.clone();
        let playbooks = self.playbooks.clone();
        let library = self.library.clone();
        tokio::spawn(async move {
            if let Err(error) =
                crate::crm::serve_on_listener(store, db, businesses, playbooks, library, listener)
                    .await
            {
                ui::activity("CRM server error", format!("{error:#}"));
            }
        });
        self.port = port;

        // Let the fresh server bind its routes before we hand the URL to a
        // browser, so the very next open() doesn't race it to another refusal.
        for _ in 0..20 {
            let port = self.port;
            let ready = tokio::task::spawn_blocking(move || crate::crm::is_live(port))
                .await
                .unwrap_or(false);
            if ready {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        ui::activity("Restarted CRM", self.crm_url());
    }

    pub fn brand(&self) -> &str {
        &self.brand
    }

    pub fn backend(&self) -> &'static str {
        self.client.backend().as_str()
    }

    pub fn model(&self) -> String {
        self.client.model_label()
    }

    pub fn usage_report(&self) -> String {
        self.client.stats().usage_report()
    }

    pub fn usage_snapshot(&self) -> StatsSnapshot {
        self.client.stats().snapshot()
    }

    pub fn usage_since(&self, base: StatsSnapshot) -> String {
        self.client.stats().usage_summary_since(base)
    }

    pub fn select_backend(&self, backend: Backend) {
        self.client.select_backend(backend);
    }

    pub fn select_model(&self, backend: Backend, model: Option<String>) {
        self.client.select_model(backend, model);
    }

    pub fn take_model_switches(&self) -> Vec<ModelSwitch> {
        self.client.take_model_switches()
    }

    pub fn brand_keys(&self) -> Vec<&str> {
        self.playbooks.keys()
    }

    /// Shared execution DB (mail, sequences, learnings). Used by REPL mail sync.
    pub fn db(&self) -> &crate::db::SharedDb {
        &self.db
    }

    /// Switch the working brand if `key` is valid; returns whether it changed.
    /// Used by the router to follow a request to its brand — it does not pin.
    pub fn set_brand(&mut self, key: &str) -> bool {
        if self.playbooks.get(key).is_ok() && self.businesses.get(key).is_ok() {
            self.brand = key.to_string();
            true
        } else {
            false
        }
    }

    pub fn is_brand_pinned(&self) -> bool {
        self.brand_pinned
    }

    /// How the brand shows in the chrome: `auto` when agnostic (the default),
    /// otherwise the pinned brand key — so the session never *looks* locked to one
    /// brand unless the operator asked for it.
    pub fn brand_label(&self) -> String {
        if self.brand_pinned {
            self.brand.clone()
        } else {
            "auto".to_string()
        }
    }

    /// Lock the session to one brand (via `/brand <key>`). Returns whether valid.
    pub fn pin_brand(&mut self, key: &str) -> bool {
        if self.set_brand(key) {
            self.brand_pinned = true;
            true
        } else {
            false
        }
    }

    /// Return to brand-agnostic auto-detection (via `/brand auto`).
    pub fn unpin_brand(&mut self) {
        self.brand_pinned = false;
    }

    pub fn reset(&mut self) {
        self.history.clear();
    }

    /// Handle one line of user input; returns the text to show the user.
    ///
    /// The router call is streamed only into a quiet status indicator. Private
    /// scratch-work is suppressed; after routing we render one deterministic,
    /// truthful action intent before executing it.
    pub async fn handle(&mut self, input: &str) -> Result<String> {
        let prompt = self.build_prompt(input).await;

        let mut turn = ui::TurnView::new();
        let decision: Decision = self
            .client
            .structured_fast_streamed(
                "interactive.router",
                &self.system(),
                &prompt,
                decision_schema(&self.brand_keys()),
                &mut |ev| turn.on_event(ev),
            )
            .await?;
        // Router output is intentionally private, so conversational replies are
        // rendered once from the accepted structured decision.
        let streamed = turn.finish();

        // Pure conversational answer: no actions to run this turn. The model
        // already streamed a visible reply, so don't reprint — but remember it.
        if decision.steps.is_empty() {
            let reply = if streamed {
                String::new()
            } else {
                decision.reply.clone()
            };
            let memo = if reply.is_empty() {
                decision.reply.clone()
            } else {
                reply.clone()
            };
            self.remember(input, &memo);
            return Ok(reply);
        }

        // Guarantee the CRM we're about to open or cite is actually answering
        // before we do so — a sibling session may have owned the port and exited.
        self.ensure_crm_live().await;

        // Run each step in order. One request may mix brands and actions freely:
        // "full motion for gnk and outagehub", or "source gnk and draft wapahki".
        let mut outputs = Vec::new();
        for step in &decision.steps {
            let (title, detail) = self.step_intent(step);
            ui::action_intent(&title, &detail);
            let reply = self.run_step(step, input).await;
            if !reply.trim().is_empty() {
                outputs.push(reply);
            }
        }
        let combined = outputs.join("\n\n");
        self.remember(input, &combined);
        Ok(combined)
    }

    fn step_intent(&self, step: &Step) -> (String, String) {
        let brand_key = if self.brand_pinned || step.brand.trim().is_empty() {
            self.brand.as_str()
        } else {
            step.brand.trim()
        };
        let brand = self
            .playbooks
            .get(brand_key)
            .map(|playbook| playbook.name.as_str())
            .unwrap_or(brand_key);
        match step.action.as_str() {
            "plan_outreach" => {
                let accounts = step.accounts.unwrap_or(0);
                let contacts = step.contacts.unwrap_or(0);
                let touches = step.touches.unwrap_or(7);
                let scope = match (accounts, contacts) {
                    (1, contacts) if contacts > 0 => {
                        format!("{contacts} people from the first account")
                    }
                    (accounts, contacts) if accounts > 0 && contacts > 0 => {
                        format!("{contacts} people from each of {accounts} accounts")
                    }
                    _ => "selected verified people".into(),
                };
                (
                    "Drafting outreach".into(),
                    format!(
                        "{brand} · {scope} · {touches} touches each · {}",
                        if step.auto {
                            "auto-schedule"
                        } else {
                            "drafts only"
                        }
                    ),
                )
            }
            "source_leads" => (
                "Finding qualified companies".into(),
                format!(
                    "{brand} · {} companies · {} people each · active GTM play",
                    step.accounts.unwrap_or(10),
                    step.contacts.unwrap_or(3)
                ),
            ),
            "run_full_motion" => (
                "Running full motion".into(),
                format!(
                    "{brand} · {} accounts · {} people each · {} touches",
                    step.accounts.unwrap_or(5),
                    step.contacts.unwrap_or(5),
                    step.touches.unwrap_or(7)
                ),
            ),
            "enrich_people" => (
                "Enriching contacts".into(),
                format!("{brand} · up to {} people", step.limit.unwrap_or(50)),
            ),
            "approve_outreach" => (
                "Approving reviewed drafts".into(),
                format!("{brand} · eligible email touches only"),
            ),
            "open_crm" => ("Opening CRM".into(), self.crm_url()),
            "open_gtm" => (
                "Opening GTM Lab".into(),
                format!("{brand} · signals and plays"),
            ),
            "search_knowledge" => ("Searching knowledge".into(), step.query.trim().to_string()),
            action => (
                action.replace('_', " "),
                if brand.is_empty() {
                    String::new()
                } else {
                    brand.to_string()
                },
            ),
        }
    }

    /// Execute one routed action for one brand and return the text to show.
    async fn run_step(&mut self, step: &Step, input: &str) -> String {
        // In auto mode follow this step's brand; when pinned, ignore it and stay.
        if !self.brand_pinned && !step.brand.trim().is_empty() {
            self.set_brand(step.brand.trim());
        }
        // Reads scope to the pinned brand; otherwise to this step's brand;
        // otherwise they span the whole portfolio.
        let read_scope: Option<&str> = if self.brand_pinned {
            Some(self.brand.as_str())
        } else {
            let named = step.brand.trim();
            if named.is_empty() {
                None
            } else {
                Some(named)
            }
        };

        match step.action.as_str() {
            "run_campaign" => {
                let thesis = if step.thesis.trim().is_empty() {
                    input.to_string()
                } else {
                    step.thesis.clone()
                };
                let accounts = step.accounts.unwrap_or(5).max(1) as usize;
                let contacts = step.contacts.unwrap_or(5).max(1) as usize;
                let touches = step.touches.unwrap_or(7).max(1) as usize;
                self.run_campaign(&thesis, accounts, contacts, touches)
                    .await
            }
            "source_leads" => {
                let thesis = if step.thesis.trim().is_empty() {
                    input.to_string()
                } else {
                    step.thesis.clone()
                };
                let accounts = step.accounts.unwrap_or(10).max(1) as usize;
                let contacts = step.contacts.unwrap_or(3).max(1) as usize;
                self.source_leads(&thesis, accounts, contacts).await
            }
            "run_full_motion" => {
                let thesis = if step.thesis.trim().is_empty() {
                    input.to_string()
                } else {
                    step.thesis.clone()
                };
                let accounts = step.accounts.unwrap_or(5).max(1) as usize;
                let contacts = step.contacts.unwrap_or(5).max(1) as usize;
                let touches = step.touches.unwrap_or(7).max(1) as usize;
                self.run_full_motion(
                    &thesis,
                    accounts,
                    contacts,
                    touches,
                    step.force_new,
                    // Re-draft when the operator is asking for sequences again
                    // (the common "write the 7-stage sequence" path) or when
                    // they explicitly set replace.
                    true,
                )
                .await
            }
            "enrich_people" => {
                self.enrich_people(step.limit.unwrap_or(50).max(1) as usize, step.phone)
                    .await
            }
            "plan_outreach" => {
                self.plan_outreach(
                    step.touches.unwrap_or(7).max(1) as usize,
                    step.auto,
                    step.accounts.map(|value| value.max(1) as usize),
                    step.contacts.map(|value| value.max(1) as usize),
                    routed_total_limit(input, step.accounts, step.contacts, step.limit),
                    !forbids_contact_enrichment(input),
                )
                .await
            }
            "approve_outreach" => self.approve_outreach(),
            "discover_opportunities" => {
                self.discover_opportunities(step.limit.unwrap_or(20).max(1) as usize)
                    .await
            }
            "list_opportunities" => self.list_opportunities(read_scope, step.actionable),
            "show_learnings" => {
                self.show_learnings(read_scope, step.limit.unwrap_or(30).max(1) as usize)
            }
            "resolve_opportunity_contacts" => {
                self.resolve_opportunity_contacts(
                    step.opportunity_id.trim(),
                    step.contacts.unwrap_or(3).max(1) as usize,
                    step.enrich,
                )
                .await
            }
            "plan_funding_outreach" => {
                self.plan_funding_outreach(
                    step.opportunity_id.trim(),
                    step.touches.unwrap_or(2).clamp(1, 3) as usize,
                    step.auto,
                )
                .await
            }
            "approve_funding_outreach" => self.approve_funding_outreach(step.opportunity_id.trim()),
            "prepare_application" => self.prepare_application(step.opportunity_id.trim()).await,
            "show_funnel" => self.show_funnel(read_scope),
            "show_calendar" => self.show_calendar(),
            "list_accounts" => self.list_accounts(read_scope).await,
            "search_knowledge" => {
                let q = if step.query.trim().is_empty() {
                    input
                } else {
                    step.query.trim()
                };
                self.search_knowledge(q).await
            }
            "open_crm" => {
                open_browser(&self.crm_url());
                ui::activity("Opened CRM dashboard", self.crm_url());
                format!("Opened the CRM dashboard at {}", self.crm_url())
            }
            "open_gtm" => {
                let brand = step.brand.trim();
                let url = if brand.is_empty() {
                    format!("{}/gtm", self.crm_url())
                } else {
                    format!("{}/gtm/{brand}", self.crm_url())
                };
                open_browser(&url);
                ui::activity("Opened GTM engineering lab", &url);
                format!("Opened the GTM engineering lab at {url}")
            }
            _ => String::new(),
        }
    }

    async fn run_campaign(
        &self,
        thesis: &str,
        accounts: usize,
        contacts: usize,
        touches: usize,
    ) -> String {
        let pb = match self.playbooks.get(&self.brand) {
            Ok(p) => p,
            Err(e) => return format!("Can't run: {e:#}"),
        };

        // Live progress tree: header chip + per-account spinners/checkmarks and a
        // running tokens/cost/elapsed footer, painted by its own render thread.
        let header = ui::campaign_header(&pb.name, thesis, accounts, contacts, touches);
        let view = ui::CampaignView::start(header, self.client.stats());

        let lib = self.library.read().await.clone();
        let result = pipeline::run(
            &self.client,
            pb,
            &self.playbooks.shared,
            &lib,
            thesis,
            accounts,
            contacts,
            touches,
            self.concurrency,
            self.critique,
            &view,
        )
        .await;

        // Stop the render thread (leaves the final frame on screen) before we
        // print anything else.
        view.finish(result.is_ok());

        let campaign = match result {
            Ok(c) => c,
            Err(e) => return format!("Campaign failed: {e:#}"),
        };

        let mut store = self.store.write().await;
        match store.ingest(campaign) {
            Ok((ac, ct, to)) => format!(
                "Filed {ac} accounts, {ct} contacts, and {to} touches into the CRM \u{2014} view at \
                 {}. (Only the observed facts are meant to be stated as fact; verify the rest \
                 before any outreach.)",
                self.crm_url()
            ),
            Err(e) => format!("Ran the campaign but failed to file it: {e:#}"),
        }
    }

    /// Source real organizations and people into the durable execution db.
    /// Source real orgs + people into the db, returning the structured summary.
    /// Prints its own progress cell; leaves CRM-opening/formatting to callers so
    /// it can be reused inside the full-motion chain.
    async fn do_source(
        &self,
        thesis: &str,
        accounts: usize,
        contacts: usize,
    ) -> Result<sourcing::SourceSummary, String> {
        let pb = self
            .playbooks
            .get(&self.brand)
            .map_err(|e| format!("Can't source: {e:#}"))?;
        let business = self
            .businesses
            .get(&self.brand)
            .map_err(|e| format!("Can't source: {e:#}"))?;
        let apollo = Apollo::from_env().map_err(|e| format!("Can't source: {e:#}"))?;
        let lib = self.library.read().await.clone();
        let view = ui::SourceView::start(
            format!(
                "{} · {accounts} account target · {contacts} people each · active GTM play",
                pb.name
            ),
            self.client.stats(),
        );
        let progress = view.reporter();
        let result = sourcing::source(
            &self.db,
            &self.client,
            &apollo,
            pb,
            &self.playbooks.shared,
            &business.calendar.fallback_recipient_timezone,
            &business.operating_context(),
            &lib,
            thesis,
            accounts,
            contacts,
            self.concurrency.max(1),
            Some(progress),
        )
        .await;
        view.finish(result.is_ok());
        match result {
            Ok(s) => Ok(s),
            Err(e) => Err(format!("Sourcing failed: {e:#}")),
        }
    }

    async fn source_leads(&self, thesis: &str, accounts: usize, contacts: usize) -> String {
        match self.do_source(thesis, accounts, contacts).await {
            Ok(s) => {
                // Surface the freshly-filed leads/people in the live CRM.
                if s.leads_qualified > 0 || s.people_added > 0 {
                    open_browser(&self.crm_url());
                    ui::activity("Opened CRM dashboard", self.crm_url());
                }
                format!(
                    "Sourced {} real organizations into {} viable lead record(s) and {} people, now filed in the CRM at {}. Fully qualified and research-needed accounts remain visibly distinct. Next, ask me to enrich their emails.",
                    s.orgs_found, s.leads_qualified, s.people_added, self.crm_url()
                )
            }
            Err(e) => e,
        }
    }

    /// Reveal + verify emails for pending people, returning the summary.
    async fn do_enrich(
        &self,
        limit: usize,
        phone: bool,
        only_person_ids: Option<&std::collections::HashSet<String>>,
    ) -> Result<enrich::EnrichSummary, String> {
        let apollo = Apollo::from_env().map_err(|e| format!("Can't enrich: {e:#}"))?;
        let view = ui::SourceView::start_enrichment(
            format!(
                "{} · up to {limit} contacts · email reveal + verification{}",
                self.playbooks
                    .get(&self.brand)
                    .map(|playbook| playbook.name.as_str())
                    .unwrap_or(self.brand.as_str()),
                if phone { " + phone" } else { "" }
            ),
            self.client.stats(),
        );
        let progress = view.enrich_reporter();
        let result = enrich::enrich_pending(
            &self.db,
            &apollo,
            Some(&self.brand),
            limit,
            phone,
            only_person_ids,
            Some(progress),
        )
        .await;
        view.finish(result.is_ok());
        match result {
            Ok(s) => Ok(s),
            Err(e) => Err(format!("Enrichment failed: {e:#}")),
        }
    }

    async fn enrich_people(&self, limit: usize, phone: bool) -> String {
        match self.do_enrich(limit, phone, None).await {
            Ok(s) => format!(
                "Enriched {} people: {} emails found, {} verified. Next, ask me to plan outreach.",
                s.attempted, s.emails_found, s.verified
            ),
            Err(e) => e,
        }
    }

    /// Write sequences for verified people, returning the summary. `replace`
    /// re-drafts existing (unsent) sequences to improve them. `per_account_cap` =
    /// Some(n) fills each company up to n verified contacts (the full motion's
    /// target); None sequences every verified contact found (the explicit sweep).
    /// A scoped request may reveal only its selected, not-yet-verified people so
    /// the requested account × contact cardinality does not silently collapse.
    /// No account or people search occurs here; real send volume remains bounded
    /// by send-time account limits.
    async fn do_plan(
        &self,
        touches: usize,
        auto: bool,
        replace: bool,
        only_person_ids: Option<&std::collections::HashSet<String>>,
        per_account_cap: Option<usize>,
    ) -> Result<outreach::PlanSummary, String> {
        let pb = self
            .playbooks
            .get(&self.brand)
            .map_err(|e| format!("Can't plan outreach: {e:#}"))?;
        let business = self
            .businesses
            .get(&self.brand)
            .map_err(|e| format!("Can't plan outreach: {e:#}"))?;
        let lib = self.library.read().await.clone();
        let view =
            ui::OutreachView::start(
                format!(
                "{} · {touches} touches each · {} · drafts stream into CRM before review finishes",
                pb.name,
                if auto { "auto-schedule eligible" } else { "drafts only" }
            ),
                self.client.stats(),
            );
        let progress = view.reporter();
        let result = outreach::plan_pending(
            &self.db,
            &self.client,
            pb,
            business,
            &self.playbooks.shared,
            &lib,
            touches,
            self.concurrency.max(1),
            auto,
            self.critique,
            None,
            replace,
            per_account_cap,
            only_person_ids,
            Some(progress),
        )
        .await;
        let stopped = result
            .as_ref()
            .map(|summary| summary.stopped_reason.is_some())
            .unwrap_or(true);
        view.finish(if stopped {
            ui::OutreachCompletion::Stopped
        } else {
            ui::OutreachCompletion::Completed
        });
        match result {
            Ok(s) => Ok(s),
            Err(e) => Err(format!("Outreach planning failed: {e:#}")),
        }
    }

    async fn plan_outreach(
        &self,
        touches: usize,
        auto: bool,
        account_limit: Option<usize>,
        contacts_per_account: Option<usize>,
        total_limit: Option<usize>,
        fill_contact_coverage: bool,
    ) -> String {
        let scoped =
            account_limit.is_some() || contacts_per_account.is_some() || total_limit.is_some();
        let mut scope_note = String::new();
        let only_person_ids = if scoped {
            let scope = match select_plan_scope(
                &self.db,
                &self.brand,
                account_limit,
                contacts_per_account,
                total_limit,
            ) {
                Ok(scope) => scope,
                Err(error) => return format!("Can't resolve outreach scope: {error:#}"),
            };
            ui::activity(
                "Resolved outreach scope",
                format!(
                    "{} requested · {} existing people selected across {} account(s)",
                    scope.requested_people,
                    scope.selected_ids.len(),
                    scope.accounts.len()
                ),
            );
            if !scope.pending_enrichment_ids.is_empty() && fill_contact_coverage {
                ui::activity(
                    "Completing contact coverage",
                    format!(
                        "{} selected people need verified email · up to {} Apollo reveal credits",
                        scope.pending_enrichment_ids.len(),
                        scope.pending_enrichment_ids.len()
                    ),
                );
                match self
                    .do_enrich(
                        scope.pending_enrichment_ids.len(),
                        false,
                        Some(&scope.pending_enrichment_ids),
                    )
                    .await
                {
                    Ok(summary) if summary.stopped.is_some() => ui::activity(
                        "Contact enrichment stopped early",
                        format!(
                            "{} attempted · {} verified · {} · continuing only with verified recipients",
                            summary.attempted,
                            summary.verified,
                            summary.stopped.unwrap_or_default()
                        ),
                    ),
                    Ok(_) => {}
                    Err(error) => ui::activity(
                        "Contact enrichment did not complete",
                        format!("{error} · continuing only with verified recipients"),
                    ),
                }
            } else if !scope.pending_enrichment_ids.is_empty() {
                ui::activity(
                    "Contact enrichment skipped",
                    format!(
                        "{} selected identities still need verification · operator requested no Apollo/enrichment",
                        scope.pending_enrichment_ids.len()
                    ),
                );
            }

            let mut verified = std::collections::HashSet::new();
            let mut gaps = Vec::new();
            for account in &scope.accounts {
                let mut ready = 0usize;
                for id in &account.person_ids {
                    if self.db.get_person(id).ok().flatten().is_some_and(|person| {
                        person.status.eq_ignore_ascii_case("verified")
                            && person.email_status.eq_ignore_ascii_case("verified")
                    }) {
                        ready += 1;
                        verified.insert(id.clone());
                    }
                }
                if ready < account.requested {
                    gaps.push(format!("{} {ready}/{}", account.name, account.requested));
                }
            }
            scope_note = format!(
                " Scope: {} requested, {} existing selected, {} verified for drafting across {} account(s).{}",
                scope.requested_people,
                scope.selected_ids.len(),
                verified.len(),
                scope.accounts.len(),
                if gaps.is_empty() {
                    String::new()
                } else {
                    format!(" Coverage still short: {}.", gaps.join("; "))
                }
            );
            Some(verified)
        } else {
            None
        };
        match self
            // Natural-language write/refine requests safely replace only unsent
            // drafts; sent sequences remain protected by the persistence layer.
            .do_plan(touches, auto, true, only_person_ids.as_ref(), None)
            .await
        {
            Ok(s) => {
                let stop = s
                    .stopped_reason
                    .as_ref()
                    .map(|reason| {
                        format!(
                            " Drafting stopped early: {reason}. {} recipient(s) not completed.",
                            s.people_stopped
                        )
                    })
                    .unwrap_or_default();
                let next = if auto {
                    ""
                } else if s.people_planned > 0 {
                    " Review the accepted drafts in CRM; approval remains a separate action."
                } else {
                    " Nothing is approval-eligible; rejection feedback is preserved in CRM."
                };
                format!(
                    "Drafted {} reviewed sequence(s): {} email touches scheduled, {} touches held as drafts, {} recipient(s) rejected.{stop}{scope_note}{next}",
                    s.people_planned,
                    s.touches_scheduled,
                    s.touches_drafted,
                    s.people_rejected,
                )
            }
            Err(e) => e,
        }
    }

    /// Run the whole outbound motion from one request.
    ///
    /// **Reuse-first:** if the brand already has enough accounts/people on file,
    /// Apollo is skipped entirely. We refresh why each company fits (doctrine
    /// framing), enrich only contacts still missing verified email, and
    /// (re)write the sequences for the selected set. Apollo is only called for
    /// the shortfall — or when `force_new` is true.
    async fn run_full_motion(
        &self,
        thesis: &str,
        accounts: usize,
        contacts: usize,
        touches: usize,
        force_new: bool,
        replace_drafts: bool,
    ) -> String {
        // 1. Inventory already on file for this brand.
        let mut reuse = match sourcing::select_reuse(&self.db, &self.brand, accounts, contacts) {
            Ok(selection) => selection,
            Err(e) => return format!("Can't inspect on-file inventory: {e:#}"),
        };

        let mut src = sourcing::SourceSummary::default();
        let need_apollo = force_new || reuse.accounts_shortfall > 0;

        if need_apollo {
            let want = if force_new {
                accounts
            } else {
                reuse.accounts_shortfall.max(1)
            };
            ui::activity(
                if force_new {
                    "Sourcing new accounts"
                } else {
                    "Filling account shortfall"
                },
                format!(
                    "Apollo for {want} account(s) · on-file already has {} with people",
                    reuse.accounts_selected
                ),
            );
            match self.do_source(thesis, want, contacts).await {
                Ok(s) => src = s,
                Err(e) => {
                    // If we already have reusable inventory, keep going; only hard-fail
                    // when there is nothing to work from.
                    if reuse.person_ids.is_empty() {
                        return e;
                    }
                    ui::activity(
                        "Apollo shortfall skipped",
                        format!("{e} — continuing with on-file accounts"),
                    );
                }
            }
            // Re-rank after any new rows land so the working set includes them.
            if let Ok(updated) = sourcing::select_reuse(&self.db, &self.brand, accounts, contacts) {
                reuse = updated;
            }
        } else {
            ui::activity(
                "Reusing on-file inventory",
                format!(
                    "selected {}/{} account(s) · {}/{} people ({} verified of {} on file) · Apollo skipped",
                    reuse.accounts_selected,
                    reuse.accounts_on_file,
                    reuse.people_selected,
                    reuse.people_on_file,
                    reuse.verified_selected,
                    reuse.verified_on_file
                ),
            );
        }

        if reuse.lead_ids.is_empty() || reuse.person_ids.is_empty() {
            open_browser(&self.crm_url());
            return format!(
                "No reusable accounts/people on file for {} and sourcing did not add any. Everything's in the CRM at {}. Try a tighter thesis or say \"new companies\" to force Apollo.",
                self.brand,
                self.crm_url()
            );
        }

        // 2. Refresh commercial framing on the selected accounts (no Apollo) so
        // sequences are written against an up-to-date "why this company".
        let refreshed = self.do_refresh_context(thesis, &reuse.lead_ids).await;

        // 3. Enrich only selected people still missing a verified email.
        let need_enrich = self
            .db
            .list_people(Some(&self.brand), Some("new"))
            .map(|people| {
                people
                    .into_iter()
                    .filter(|p| reuse.person_ids.contains(&p.id))
                    .count()
            })
            .unwrap_or(0);
        let enr = if need_enrich > 0 {
            match self
                .do_enrich(need_enrich, false, Some(&reuse.person_ids))
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    // Verified people can still be sequenced; only hard-stop when
                    // nobody in the set is verified.
                    if reuse.verified_selected == 0 {
                        open_browser(&self.crm_url());
                        return format!(
                            "Refreshed framing for {} account(s) but enrichment stopped before any verified emails: {e}",
                            reuse.accounts_selected
                        );
                    }
                    ui::activity(
                        "Enrichment partial",
                        format!("{e} — drafting verified contacts only"),
                    );
                    enrich::EnrichSummary::default()
                }
            }
        } else {
            ui::activity(
                "Enrichment skipped",
                format!(
                    "{} selected contact(s) already verified · 0 Apollo reveal credits",
                    reuse.verified_selected
                ),
            );
            enrich::EnrichSummary {
                verified: reuse.verified_selected,
                ..Default::default()
            }
        };

        // 4. Draft / re-draft sequences only for the selected working set.
        let pln = match self
            .do_plan(
                touches,
                false,
                replace_drafts,
                Some(&reuse.person_ids),
                Some(contacts),
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                open_browser(&self.crm_url());
                return format!(
                    "Inventory ready in the CRM at {}, but drafting the sequences stopped: {e}",
                    self.crm_url()
                );
            }
        };

        open_browser(&self.crm_url());
        ui::activity("Opened CRM dashboard", self.crm_url());

        if pln.people_planned == 0 {
            if let Some(reason) = &pln.stopped_reason {
                return format!(
                    "Drafting stopped after the working set was prepared: {reason}. {} contact(s) were not completed and {} copy draft(s) were rejected before the stop. Nothing was sent. Existing feedback is in the CRM at {}.",
                    pln.people_stopped,
                    pln.people_rejected,
                    self.crm_url(),
                );
            }
            if pln.people_rejected > 0 {
                return format!(
                    "Drafting reached {} verified contact(s), but every sequence was rejected by copy review. The recipient-specific reasons are saved in the CRM at {}. Nothing was sent.",
                    pln.people_rejected,
                    self.crm_url(),
                );
            }
            return format!(
                "The working set contains {accounts_sel} account(s) / {people_sel} selected people \
                 (Apollo orgs={orgs}, new leads={leads}, refreshed={refreshed}, newly verified={verified}), \
                 but no new sequence was needed. The selected contacts either lack a verified email or already have an active/sent sequence. CRM: {url}.",
                accounts_sel = reuse.accounts_selected,
                people_sel = reuse.people_selected,
                orgs = src.orgs_found,
                leads = src.leads_qualified,
                refreshed = refreshed,
                verified = enr.verified,
                url = self.crm_url(),
            );
        }

        if let Some(reason) = &pln.stopped_reason {
            return format!(
                "Drafting stopped early: {reason}. Saved {} sequence(s) that had already passed; {} contact(s) were not completed and {} draft(s) were rejected. Nothing was sent. CRM: {}.",
                pln.people_planned,
                pln.people_stopped,
                pln.people_rejected,
                self.crm_url(),
            );
        }

        let apollo_note = if src.orgs_found == 0 && src.leads_qualified == 0 {
            "Apollo skipped (reused on-file inventory)".to_string()
        } else {
            format!(
                "Apollo: {} org(s) seen · {} new lead(s) · {} people added",
                src.orgs_found, src.leads_qualified, src.people_added
            )
        };

        format!(
            "Full motion done: {planned} sequence(s) for {accounts_sel} account(s) \
             ({people_sel} contacts, {verified_sel} verified). {apollo_note}. \
             Refreshed framing on {refreshed} account(s). Nothing sent — say \"approve\" to schedule. CRM: {url}.",
            planned = pln.people_planned,
            accounts_sel = reuse.accounts_selected,
            people_sel = reuse.people_selected,
            verified_sel = reuse.verified_selected.max(enr.verified),
            apollo_note = apollo_note,
            refreshed = refreshed,
            url = self.crm_url(),
        )
    }

    /// Refresh doctrine framing for leads already on file (no Apollo).
    async fn do_refresh_context(&self, thesis: &str, lead_ids: &[String]) -> usize {
        if lead_ids.is_empty() {
            return 0;
        }
        let pb = match self.playbooks.get(&self.brand) {
            Ok(p) => p,
            Err(_) => return 0,
        };
        let business = match self.businesses.get(&self.brand) {
            Ok(b) => b,
            Err(_) => return 0,
        };
        let lib = self.library.read().await.clone();
        let work = ui::Spinner::start("Refreshing account framing");
        let result = sourcing::refresh_lead_context(
            &self.db,
            &self.client,
            pb,
            &business.operating_context(),
            &lib,
            thesis,
            lead_ids,
            self.concurrency.max(1),
        )
        .await;
        drop(work);
        match result {
            Ok(n) => {
                ui::activity(
                    "Refreshed account framing",
                    format!("{n} account(s) · why-them updated for copy · no Apollo"),
                );
                n
            }
            Err(e) => {
                ui::activity("Framing refresh skipped", format!("{e:#}"));
                0
            }
        }
    }

    fn approve_outreach(&self) -> String {
        match self.db.approve_touches(Some(&self.brand), None) {
            Ok(n) => {
                ui::activity(
                    "Approved outreach",
                    format!("{n} touch(es) · {}", self.brand),
                );
                format!(
                    "Approved {n} drafted email touch(es) for {} and scheduled them.",
                    self.brand
                )
            }
            Err(e) => format!("Approval failed: {e:#}"),
        }
    }

    async fn discover_opportunities(&self, limit: usize) -> String {
        let profile = match self.businesses.get(&self.brand) {
            Ok(profile) => profile,
            Err(e) => return format!("Can't discover opportunities: {e:#}"),
        };
        if let Err(e) = profile.funding() {
            return format!("Can't discover opportunities: {e:#}");
        }
        let work = ui::Spinner::start("Reading official opportunity sources");
        let result = opportunity::discover(&self.db, &self.client, profile, limit).await;
        drop(work);
        match result {
            Ok(summary) => {
                ui::activity(
                    "Discovered opportunities",
                    format!(
                        "{} candidates · {} verified · {} new · {} updated",
                        summary.candidates_found,
                        summary.opportunities_verified,
                        summary.opportunities_added,
                        summary.opportunities_updated
                    ),
                );
                let opportunities = self
                    .db
                    .list_opportunities(Some(&self.brand), None)
                    .unwrap_or_default();
                let warnings = if summary.errors.is_empty() {
                    String::new()
                } else {
                    format!(
                        "\n\n{} source warning(s) were recorded; successful sources were still kept.",
                        summary.errors.len()
                    )
                };
                format!(
                    "Checked {}/{} sources and verified {} opportunities. Added {}, updated {}, skipped {}.{}\n\n{}",
                    summary.sources_read,
                    summary.sources_attempted,
                    summary.opportunities_verified,
                    summary.opportunities_added,
                    summary.opportunities_updated,
                    summary.skipped,
                    warnings,
                    opportunity::render_opportunities(&opportunities)
                )
            }
            Err(e) => format!("Opportunity discovery failed: {e:#}"),
        }
    }

    fn list_opportunities(&self, scope: Option<&str>, actionable_only: bool) -> String {
        match self.db.list_opportunities(scope, None) {
            Ok(mut opportunities) => {
                if actionable_only {
                    opportunities.retain(opportunity::is_actionable);
                }
                ui::activity(
                    "Read opportunity pipeline",
                    format!(
                        "{} opportunity/opportunities · {}",
                        opportunities.len(),
                        scope.unwrap_or("all brands")
                    ),
                );
                opportunity::render_opportunities(&opportunities)
            }
            Err(e) => format!("Couldn't read opportunities: {e:#}"),
        }
    }

    /// Accumulated business intelligence: what we've learned and keep learning
    /// about each brand's outbound (companies skipped and why, and — over time —
    /// where outreach fails), so the operator can see the funnel isn't starting
    /// from a clean state. Spans all brands unless a brand was named.
    fn show_learnings(&self, scope: Option<&str>, limit: usize) -> String {
        match self.db.recent_learnings(scope, None, limit) {
            Ok(learnings) => {
                ui::activity(
                    "Read business intelligence",
                    format!(
                        "{} learning(s) · {}",
                        learnings.len(),
                        scope.unwrap_or("all brands")
                    ),
                );
                if learnings.is_empty() {
                    return "No learnings recorded yet — they accumulate as runs skip companies \
                            and outreach plays out."
                        .to_string();
                }
                let mut out = format!(
                    "Business intelligence ({}):\n",
                    scope.unwrap_or("all brands")
                );
                for learning in &learnings {
                    let seen = if learning.hits > 1 {
                        format!(" [seen {}×]", learning.hits)
                    } else {
                        String::new()
                    };
                    let last = learning.updated_at.chars().take(10).collect::<String>();
                    out.push_str(&format!(
                        "- [{}] {} — {}{} (last {})\n    {}\n",
                        learning.brand,
                        learning.kind,
                        learning.subject,
                        seen,
                        last,
                        learning.detail
                    ));
                }
                out
            }
            Err(e) => format!("Couldn't read business intelligence: {e:#}"),
        }
    }

    async fn resolve_opportunity_contacts(
        &self,
        opportunity_id: &str,
        limit: usize,
        enrich_now: bool,
    ) -> String {
        if opportunity_id.is_empty() {
            return "Choose an opportunity first; ask me to list opportunities and use its id."
                .into();
        }
        let profile = match self.businesses.get(&self.brand) {
            Ok(profile) => profile,
            Err(e) => return format!("Can't resolve contacts: {e:#}"),
        };
        let apollo = match Apollo::from_env() {
            Ok(apollo) => apollo,
            Err(e) => return format!("Can't resolve contacts: {e:#}"),
        };
        let work = ui::Spinner::start("Resolving official and Apollo contacts");
        let result = opportunity::resolve_contacts(
            &self.db,
            &apollo,
            profile,
            opportunity_id,
            limit,
            enrich_now,
        )
        .await;
        drop(work);
        match result {
            Ok(summary) => {
                ui::activity(
                    "Resolved opportunity contacts",
                    format!(
                        "{} official · {} Apollo · {} verified",
                        summary.official_contacts,
                        summary.apollo_people_found,
                        summary.verified_emails
                    ),
                );
                let contacts = self
                    .db
                    .list_opportunity_contacts(opportunity_id)
                    .unwrap_or_default();
                let listing = contacts
                    .iter()
                    .map(|contact| {
                        let route = if !contact.email.is_empty() {
                            format!("{} ({})", contact.email, contact.email_status)
                        } else if !contact.phone.is_empty() {
                            format!("phone {}", contact.phone)
                        } else {
                            "no direct email or phone".into()
                        };
                        format!(
                            "- {} — {} [{}] {}\n  id: {}",
                            if contact.name.is_empty() {
                                "Programme contact"
                            } else {
                                &contact.name
                            },
                            contact.title,
                            contact.source,
                            route,
                            contact.id
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!(
                    "Found {} Apollo people and stored {} contact(s); {} emails are verified.\n{}",
                    summary.apollo_people_found,
                    contacts.len(),
                    contacts
                        .iter()
                        .filter(|contact| contact.email_status == "verified")
                        .count(),
                    listing
                )
            }
            Err(e) => format!("Contact resolution failed: {e:#}"),
        }
    }

    async fn plan_funding_outreach(
        &self,
        opportunity_id: &str,
        touches: usize,
        auto: bool,
    ) -> String {
        if opportunity_id.is_empty() {
            return "Choose an opportunity first; ask me to list opportunities and use its id."
                .into();
        }
        let profile = match self.businesses.get(&self.brand) {
            Ok(profile) => profile,
            Err(e) => return format!("Can't plan funding outreach: {e:#}"),
        };
        let playbook = match self.playbooks.get(&self.brand) {
            Ok(playbook) => playbook,
            Err(e) => return format!("Can't plan funding outreach: {e:#}"),
        };
        let work = ui::Spinner::start("Drafting pre-application outreach");
        let result = opportunity::plan_funding_outreach(
            &self.db,
            &self.client,
            profile,
            playbook,
            &self.playbooks.shared,
            opportunity::FundingOutreachOptions {
                opportunity_id,
                touches,
                auto_schedule: auto,
            },
        )
        .await;
        drop(work);
        match result {
            Ok(summary) => format!(
                "Planned {} contact(s): {} scheduled and {} left as drafts.{}",
                summary.contacts_planned,
                summary.touches_scheduled,
                summary.touches_drafted,
                if auto {
                    ""
                } else {
                    " Ask me to approve the funding outreach after reviewing it."
                }
            ),
            Err(e) => format!("Funding outreach planning failed: {e:#}"),
        }
    }

    fn approve_funding_outreach(&self, opportunity_id: &str) -> String {
        if opportunity_id.is_empty() {
            return "Name the opportunity you reviewed before approval; I won't approve every funding draft in the business at once.".into();
        }
        let contacts = match self.db.list_opportunity_contacts(opportunity_id) {
            Ok(contacts) => contacts,
            Err(e) => return format!("Couldn't read funding contacts: {e:#}"),
        };
        let mut approved = 0;
        for contact in contacts {
            match self
                .db
                .approve_opportunity_touches(Some(&self.brand), Some(&contact.id))
            {
                Ok(count) => approved += count,
                Err(e) => return format!("Funding approval failed: {e:#}"),
            }
        }
        format!("Approved {approved} review-passing funding touch(es) for {opportunity_id}.")
    }

    async fn prepare_application(&self, opportunity_id: &str) -> String {
        if opportunity_id.is_empty() {
            return "Choose an opportunity first; ask me to list opportunities and use its id."
                .into();
        }
        let profile = match self.businesses.get(&self.brand) {
            Ok(profile) => profile,
            Err(e) => return format!("Can't prepare application: {e:#}"),
        };
        let work = ui::Spinner::start("Preparing evidence-gapped application brief");
        let result =
            opportunity::prepare_application(&self.db, &self.client, profile, opportunity_id).await;
        drop(work);
        match result {
            Ok(brief) => format!(
                "Prepared an application brief.\n\nEligibility:\n{}\n\nProject shape:\n{}\n\nEvidence needed:\n- {}\n\nNext steps:\n- {}",
                brief.eligibility_summary,
                brief.project_shape,
                brief.evidence_needed.join("\n- "),
                brief.next_steps.join("\n- "),
            ),
            Err(e) => format!("Application preparation failed: {e:#}"),
        }
    }

    fn show_funnel(&self, scope: Option<&str>) -> String {
        match metrics::funnel(&self.db, scope) {
            Ok(f) => {
                ui::activity("Read sales funnel", scope.unwrap_or("all brands"));
                metrics::render(&f)
            }
            Err(e) => format!("Couldn't read funnel metrics: {e:#}"),
        }
    }

    async fn list_accounts(&self, scope: Option<&str>) -> String {
        let real = match self.db.list_leads(scope) {
            Ok(leads) => leads,
            Err(e) => return format!("Couldn't read execution leads: {e:#}"),
        };
        let people = match self.db.list_people(scope, None) {
            Ok(people) => people,
            Err(e) => return format!("Couldn't read execution people: {e:#}"),
        };
        let store = self.store.read().await;
        // Research-only accounts carry a brand too; honor the same scope.
        let research: Vec<_> = store
            .data
            .accounts
            .iter()
            .filter(|a| scope.is_none_or(|brand| a.brand == brand))
            .collect();
        if real.is_empty() && research.is_empty() {
            ui::activity("Read CRM", "No accounts found");
            return "The CRM is empty \u{2014} no campaigns run yet.".to_string();
        }
        ui::activity(
            "Read CRM",
            format!(
                "{} real leads · {} research accounts · {} people · {}",
                real.len(),
                research.len(),
                people.len(),
                scope.unwrap_or("all brands")
            ),
        );
        let mut out = String::new();
        if !real.is_empty() {
            out.push_str("Real execution leads:\n");
            for lead in &real {
                let contacts = people.iter().filter(|p| p.lead_id == lead.id).count();
                out.push_str(&format!(
                    "- {} ({}, {}) [{}] \u{2014} {} people\n    hypothesis: {}\n",
                    lead.name, lead.industry, lead.hq, lead.brand, contacts, lead.hypothesis
                ));
            }
        }
        if !research.is_empty() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("Research-only campaign hypotheses:\n");
        }
        for a in research {
            out.push_str(&format!(
                "- {} ({}, {}) [{}] \u{2014} {} contacts\n    hypothesis: {}\n",
                a.name,
                a.industry,
                a.hq,
                a.brand,
                a.contacts.len(),
                a.hypothesis
            ));
        }
        out
    }

    fn show_calendar(&self) -> String {
        let profile = match self.businesses.get(&self.brand) {
            Ok(profile) => profile,
            Err(error) => return format!("Couldn't load calendar policy: {error:#}"),
        };
        match calendar::render_intelligence(profile, &self.db, chrono::Utc::now()) {
            Ok(report) => {
                ui::activity("Read outreach calendar", &self.brand);
                report
            }
            Err(error) => format!("Couldn't read outreach calendar: {error:#}"),
        }
    }

    /// Read-only lookup into the ingested book library.
    async fn search_knowledge(&self, query: &str) -> String {
        let lib = self.library.read().await;
        if lib.is_empty() {
            ui::activity("Searched knowledge library", "No books ingested");
            return "The book library is empty. Ingest books first: \
                    `spruce-leaf ingest <path>` (.txt/.md/.pdf)."
                .to_string();
        }
        let listing = lib.retrieve(query, 8, 3).human_listing();
        ui::activity("Searched knowledge library", query);
        format!("From the book library ({}):\n{listing}", lib.stats())
    }

    /// Build the router prompt: recent turns + current CRM snapshot + the new line.
    async fn build_prompt(&self, input: &str) -> String {
        let mut p = String::new();
        if !self.history.is_empty() {
            p.push_str("Recent conversation:\n");
            for (u, a) in &self.history {
                p.push_str(&format!("User: {u}\nspruce-leaf: {a}\n"));
            }
            p.push('\n');
        }
        let n = self.store.read().await.data.accounts.len();
        let execution = metrics::funnel(&self.db, Some(&self.brand)).unwrap_or_default();
        let business = self
            .businesses
            .get(&self.brand)
            .map(|profile| profile.agent_summary())
            .unwrap_or_default();
        let portfolio = self.businesses.roster();
        let calendar_intelligence = self
            .businesses
            .get(&self.brand)
            .and_then(|profile| calendar::agent_intelligence(profile, &self.db))
            .unwrap_or_default();
        let opportunities = self
            .db
            .list_opportunities(Some(&self.brand), None)
            .unwrap_or_default();
        let brand_mode = if self.brand_pinned {
            format!(
                "Brand is PINNED to '{brand}' — treat every request as concerning {brand} and set brand={brand}.",
                brand = self.brand
            )
        } else {
            format!(
                "No brand is pinned (agnostic): infer the brand for each request from its wording and set it; leave brand empty for portfolio-wide reads. Most recent working brand: {}.",
                self.brand
            )
        };
        p.push_str(&format!(
            "The portfolio (all brands you operate):\n{portfolio}\n\n{brand_mode}\n\nWorking-brand context ({brand}): {business} {calendar_intelligence} The research CRM holds {n} accounts. The real execution db (working brand) holds {leads} qualified leads, {people} people, {verified} verified emails, {contacted} contacted people, and {opps} persisted opportunities.\n\n",
            brand = self.brand,
            business = business,
            calendar_intelligence = calendar_intelligence,
            leads = execution.leads,
            people = execution.people,
            verified = execution.verified,
            contacted = execution.contacted,
            opps = opportunities.len(),
        ));
        p.push_str(&format!("User: {input}"));
        p
    }

    fn system(&self) -> String {
        let brand_mode = if self.brand_pinned {
            format!(
                "The operator has PINNED this session to the '{active}' brand: set `brand`={active} for every action and do not switch brands.",
                active = self.brand
            )
        } else {
            "This is a multi-brand PORTFOLIO, not a single fixed brand. For each request, infer which brand it concerns from its wording and the portfolio list, and set `brand` to that brand — do not stay on the current working brand out of inertia. Leave `brand` empty only for portfolio-wide reads (show_funnel, list_accounts, list_opportunities, show_learnings), which then span every brand.".to_string()
        };
        format!(
            "Turn the request directly into an ordered list of `steps` to run this turn. Each step is exactly ONE action for ONE brand. Do not narrate your analysis or claim an action has completed.\n\n\
MULTIPLE brands or things in one request → emit one step per (brand, action), in the user's order. Examples:\n\
- 'full motion for gnk and outagehub and wapahki' → three run_full_motion steps, brand gnk / outagehub / wapahki.\n\
- 'source 10 for gnk, then draft wapahki' → a source_leads step (brand gnk) and a plan_outreach step (brand wapahki).\n\
- Different actions for different brands in one message are fine; each step carries its own brand and its own fields.\n\
For a pure conversational answer (no action to run), leave `steps` empty and put the answer in `reply`.\n\n\
{brand_mode}\n\n\
Actions (each is a step's `action`):\n\
- run_campaign: hypothetical research-only campaign; no Apollo.\n\
- source_leads: ONLY finds and qualifies Apollo accounts+people, then stops — it writes NO emails. Use only when the request is purely to find companies/people and they want NEW Apollo search. set thesis/accounts/contacts (defaults 10/3).\n\
- run_full_motion: end-to-end motion for a brand (never sends). REUSE-FIRST: if the CRM already has enough accounts/people for that brand, it SKIPS Apollo, refreshes why those companies fit, and rewrites the sequences — cheaper and preferred when the operator says find N companies + write the sequence for a brand that already has inventory. set thesis/accounts/contacts/touches (defaults 5/5/7). set force_new=true ONLY when they explicitly ask for new/fresh/more companies not already on file.\n\
- enrich_people: reveal/verify sourced emails; phone only when explicit.\n\
- plan_outreach: draft or re-draft sequences for contacts ALREADY found (no account/people search). A scoped request may reveal only those selected contacts whose email is still missing, because an email sequence must not silently shrink; skip that reveal when the operator says no Apollo/no enrichment/verified only. For 'first N people in the first company', set accounts=1 and contacts=N. accounts limits companies in current CRM order; contacts limits people per company. OMIT limit unless the user explicitly says total/overall/at most/cap. Existing unsent drafts are safely replaced; auto only when explicit.\n\
- approve_outreach: only after explicit approval — this is what actually schedules drafts to send.\n\
- discover_opportunities, list_opportunities (actionable when requested), resolve_opportunity_contacts (enrich only with explicit credit authorization), plan_funding_outreach (auto only when explicit), approve_funding_outreach (reviewed opportunity_id + explicit approval), prepare_application: the funding/procurement motion; set opportunity_id where needed.\n\
- show_funnel, show_calendar, list_accounts, list_opportunities, show_learnings, open_crm, open_gtm: direct read/open actions (leave a step's brand empty to span all brands). Use open_gtm when the operator asks to inspect signals, root-cause qualification, active plays, experiments, proofs, or attributed outcomes.\n\
- search_knowledge: put the topic in query.\n\
Gmail is linked outside the router via /login <brand> (browser OAuth) and /mail-sync; do not invent credentials. When the user asks to check inbox/sent or what is working, prefer show_learnings after they have synced mail, and remind them to /login + /mail-sync if nothing is linked.\n\n\
Available brands: {brands}. Drafting always HOLDS sequences for review; nothing sends until approve_outreach. Do not ask for confirmation when a step performs the request. Apollo identities are real but commercial conclusions remain hypotheses. Never claim funding eligibility without every mandatory criterion, invent evidence, or treat every instrument as a grant.",
            brands = self.brand_keys().join(" | "),
        )
    }

    fn remember(&mut self, user: &str, assistant: &str) {
        self.history.push((user.to_string(), assistant.to_string()));
        if self.history.len() > HISTORY_TURNS {
            let excess = self.history.len() - HISTORY_TURNS;
            self.history.drain(0..excess);
        }
    }
}

fn decision_schema(brands: &[&str]) -> Value {
    let step = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["action"],
        "properties": {
            "action": {
                "type": "string",
                "enum": ["run_campaign", "source_leads", "run_full_motion", "enrich_people", "plan_outreach", "approve_outreach", "discover_opportunities", "list_opportunities", "resolve_opportunity_contacts", "plan_funding_outreach", "approve_funding_outreach", "prepare_application", "show_funnel", "show_calendar", "list_accounts", "show_learnings", "open_crm", "open_gtm", "search_knowledge"]
            },
            "brand": { "type": "string", "enum": brands, "description": "The brand this step concerns. Leave empty only for portfolio-wide reads (they span all brands)." },
            "thesis": { "type": "string", "description": "The workflow/market to target for sourcing/campaign steps." },
            "query": { "type": "string", "description": "For search_knowledge: the topic to look up in the ingested books." },
            "accounts": { "type": "integer", "description": "For plan_outreach, number of existing companies in current CRM order to scope. Set 1 for 'the first company'." },
            "contacts": { "type": "integer", "description": "For plan_outreach, number of visible-order verified people per selected company. Set 3 for 'first 3 people'." },
            "touches": { "type": "integer" },
            "limit": { "type": "integer", "description": "Optional TOTAL contact cap for plan_outreach. Set only when the user explicitly says total, overall, at most, maximum, limit, or cap; otherwise omit." },
            "phone": { "type": "boolean" },
            "auto": { "type": "boolean" },
            "force_new": { "type": "boolean", "description": "For run_full_motion: force Apollo to find new accounts even when the CRM already has enough. Default false (reuse on-file inventory)." },
            "enrich": { "type": "boolean", "description": "Reveal Apollo opportunity-contact emails now; costs credits." },
            "actionable": { "type": "boolean", "description": "For opportunity lists, exclude closed, ineligible, lost, and expired records." },
            "opportunity_id": { "type": "string", "description": "Persisted opportunity id for contact, outreach, or application actions." }
        }
    });
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["steps", "reply"],
        "properties": {
            "steps": {
                "type": "array",
                "items": step,
                "description": "Ordered actions to run this turn — one (or more) per brand, possibly different actions per brand. Empty for a pure conversational reply."
            },
            "reply": { "type": "string", "description": "Conversational answer, used ONLY when steps is empty: at most two short plain-text sentences, no markdown/lists/emoji." }
        }
    })
}

/// Best-effort open of a URL in the default browser.
pub fn open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let cmd = "open";
    #[cfg(all(unix, not(target_os = "macos")))]
    let cmd = "xdg-open";
    #[cfg(windows)]
    let cmd = "explorer";

    let _ = std::process::Command::new(cmd).arg(url).spawn();
}

#[cfg(test)]
mod tests {
    use super::{
        decision_schema, forbids_contact_enrichment, routed_total_limit, select_plan_scope,
    };
    use crate::db::{Db, Lead, Person};

    #[test]
    fn decision_schema_does_not_request_router_scratch_work() {
        let schema = decision_schema(&["test-brand"]);
        let required = schema["required"].as_array().expect("required fields");

        assert!(required.iter().all(|field| field != "plan"));
        assert!(schema["properties"].get("plan").is_none());
    }

    #[test]
    fn decision_schema_routes_a_list_of_per_brand_steps() {
        let schema = decision_schema(&["gnk", "outagehub", "wapahki"]);
        // A turn is a list of steps, so several brands/actions run in one request.
        assert_eq!(schema["properties"]["steps"]["type"], "array");
        let step = &schema["properties"]["steps"]["items"];
        assert_eq!(step["type"], "object");
        // Each step carries its own action and brand.
        assert_eq!(step["properties"]["action"]["type"], "string");
        assert_eq!(step["properties"]["brand"]["enum"][0], "gnk");
        // `reply` is only for the no-steps conversational path — not a step action.
        let step_actions = step["properties"]["action"]["enum"]
            .as_array()
            .expect("step actions");
        assert!(step_actions.iter().all(|action| action != "reply"));
    }

    #[test]
    fn router_cannot_silently_collapse_account_times_contact_scope() {
        let input = "write the first 2 people for the first 5 companies";
        assert_eq!(routed_total_limit(input, Some(5), Some(2), Some(1)), None);
        assert_eq!(
            routed_total_limit(
                "write 2 people for 5 companies, capped at 6 total",
                Some(5),
                Some(2),
                Some(6)
            ),
            Some(6)
        );
        assert!(forbids_contact_enrichment(
            "refine the same scope without Apollo"
        ));
        assert!(!forbids_contact_enrichment(
            "write the first 2 people for the first 5 companies"
        ));
    }

    #[test]
    fn scoped_plan_selects_visible_people_before_email_enrichment() {
        let db = Db::open(":memory:").expect("open db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "wapahki".into(),
                apollo_org_id: "scope-org".into(),
                name: "Scope Foods".into(),
                ..Default::default()
            })
            .expect("insert lead");
        let primary_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "wapahki".into(),
                apollo_person_id: "scope-primary".into(),
                name: "Primary Operator".into(),
                primary: true,
                status: "new".into(),
                email_status: "unknown".into(),
                ..Default::default()
            })
            .expect("insert primary");
        let verified_id = db
            .upsert_person(&Person {
                lead_id,
                brand: "wapahki".into(),
                apollo_person_id: "scope-verified".into(),
                name: "Verified Operator".into(),
                status: "verified".into(),
                email_status: "verified".into(),
                email: "verified@example.com".into(),
                ..Default::default()
            })
            .expect("insert verified");

        let scope =
            select_plan_scope(&db, "wapahki", Some(1), Some(2), None).expect("select scope");
        assert_eq!(scope.requested_people, 2);
        assert_eq!(scope.selected_ids.len(), 2);
        assert!(scope.selected_ids.contains(&primary_id));
        assert!(scope.selected_ids.contains(&verified_id));
        assert_eq!(scope.pending_enrichment_ids.len(), 1);
        assert!(scope.pending_enrichment_ids.contains(&primary_id));
    }
}
