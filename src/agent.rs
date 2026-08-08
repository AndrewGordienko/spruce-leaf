//! The agent loop, on the `claude` CLI underbase.
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
    action: String,
    /// First-person reasoning the model writes before deciding — streamed live
    /// as the visible "thinking" for the turn.
    #[serde(default)]
    #[allow(dead_code)]
    plan: String,
    #[serde(default)]
    reply: String,
    #[serde(default)]
    thesis: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    brand: String,
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
    #[serde(default)]
    enrich: bool,
    #[serde(default)]
    actionable: bool,
    /// Re-draft existing (unsent) sequences to improve them instead of skipping
    /// contacts who already have a draft. Reuses contacts already found; no
    /// Apollo spend.
    #[serde(default)]
    replace: bool,
    #[serde(default)]
    opportunity_id: String,
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
        tokio::spawn(async move {
            if let Err(error) = crate::crm::serve_on_listener(store, db, listener).await {
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
    /// The router call is *streamed*: a live "thinking…" indicator plays while
    /// the model reasons and picks an action, then its natural-language plan is
    /// streamed token-by-token (Codex-style) before we execute the action.
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
        // Whether the model already streamed a visible answer for this turn.
        let streamed = turn.finish();

        // In auto mode the router follows each request to whichever brand it
        // concerns; when the operator has pinned a brand we ignore the router's
        // pick and stay put.
        if !self.brand_pinned && !decision.brand.trim().is_empty() {
            self.set_brand(decision.brand.trim());
        }

        // Guarantee the CRM we're about to open or cite is actually answering
        // before we do so — a sibling session may have owned the port and exited.
        self.ensure_crm_live().await;

        // Reads scope to the pinned brand; otherwise to a brand named this turn;
        // otherwise they span the whole portfolio.
        let read_scope: Option<&str> = if self.brand_pinned {
            Some(self.brand.as_str())
        } else {
            let named = decision.brand.trim();
            if named.is_empty() {
                None
            } else {
                Some(named)
            }
        };

        let reply = match decision.action.as_str() {
            "run_campaign" => {
                let thesis = if decision.thesis.trim().is_empty() {
                    input.to_string()
                } else {
                    decision.thesis.clone()
                };
                let accounts = decision.accounts.unwrap_or(5).max(1) as usize;
                let contacts = decision.contacts.unwrap_or(5).max(1) as usize;
                let touches = decision.touches.unwrap_or(7).max(1) as usize;
                self.run_campaign(&thesis, accounts, contacts, touches)
                    .await
            }
            "source_leads" => {
                let thesis = if decision.thesis.trim().is_empty() {
                    input.to_string()
                } else {
                    decision.thesis.clone()
                };
                let accounts = decision.accounts.unwrap_or(10).max(1) as usize;
                let contacts = decision.contacts.unwrap_or(3).max(1) as usize;
                self.source_leads(&thesis, accounts, contacts).await
            }
            "run_full_motion" => {
                let thesis = if decision.thesis.trim().is_empty() {
                    input.to_string()
                } else {
                    decision.thesis.clone()
                };
                let accounts = decision.accounts.unwrap_or(5).max(1) as usize;
                let contacts = decision.contacts.unwrap_or(5).max(1) as usize;
                let touches = decision.touches.unwrap_or(7).max(1) as usize;
                self.run_full_motion(&thesis, accounts, contacts, touches)
                    .await
            }
            "enrich_people" => {
                self.enrich_people(decision.limit.unwrap_or(50).max(1) as usize, decision.phone)
                    .await
            }
            "plan_outreach" => {
                self.plan_outreach(
                    decision.touches.unwrap_or(7).max(1) as usize,
                    decision.auto,
                    decision.replace,
                )
                .await
            }
            "approve_outreach" => self.approve_outreach(),
            "discover_opportunities" => {
                self.discover_opportunities(decision.limit.unwrap_or(20).max(1) as usize)
                    .await
            }
            "list_opportunities" => self.list_opportunities(read_scope, decision.actionable),
            "show_learnings" => {
                self.show_learnings(read_scope, decision.limit.unwrap_or(30).max(1) as usize)
            }
            "resolve_opportunity_contacts" => {
                self.resolve_opportunity_contacts(
                    decision.opportunity_id.trim(),
                    decision.contacts.unwrap_or(3).max(1) as usize,
                    decision.enrich,
                )
                .await
            }
            "plan_funding_outreach" => {
                self.plan_funding_outreach(
                    decision.opportunity_id.trim(),
                    decision.touches.unwrap_or(2).clamp(1, 3) as usize,
                    decision.auto,
                )
                .await
            }
            "approve_funding_outreach" => {
                self.approve_funding_outreach(decision.opportunity_id.trim())
            }
            "prepare_application" => {
                self.prepare_application(decision.opportunity_id.trim())
                    .await
            }
            "show_funnel" => self.show_funnel(read_scope),
            "show_calendar" => self.show_calendar(),
            "list_accounts" => self.list_accounts(read_scope).await,
            "search_knowledge" => {
                let q = if decision.query.trim().is_empty() {
                    input
                } else {
                    decision.query.trim()
                };
                self.search_knowledge(q).await
            }
            "open_crm" => {
                open_browser(&self.crm_url());
                ui::activity("Opened CRM dashboard", self.crm_url());
                format!("Opened the CRM dashboard at {}", self.crm_url())
            }
            // Plain conversational reply: the model already streamed it, so
            // don't reprint — but keep the text for conversational memory.
            _ if streamed => String::new(),
            _ => decision.reply.clone(),
        };

        // Remember the substantive text even when we suppressed the reprint.
        let memo = if reply.is_empty() {
            decision.reply.clone()
        } else {
            reply.clone()
        };
        self.remember(input, &memo);
        Ok(reply)
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
        view.finish();

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
        let work = ui::Spinner::start("Searching Apollo");
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
        )
        .await;
        drop(work);
        match result {
            Ok(s) => {
                ui::activity(
                    "Searched Apollo",
                    format!(
                        "{} organizations · {} qualified leads · {} people",
                        s.orgs_found, s.leads_qualified, s.people_added
                    ),
                );
                Ok(s)
            }
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
                    "Sourced {} real organizations into {} qualified leads and {} people, now filed in the CRM at {}. Next, ask me to enrich their emails.",
                    s.orgs_found, s.leads_qualified, s.people_added, self.crm_url()
                )
            }
            Err(e) => e,
        }
    }

    /// Reveal + verify emails for pending people, returning the summary.
    async fn do_enrich(&self, limit: usize, phone: bool) -> Result<enrich::EnrichSummary, String> {
        let apollo = Apollo::from_env().map_err(|e| format!("Can't enrich: {e:#}"))?;
        let work = ui::Spinner::start("Enriching people");
        let result =
            enrich::enrich_pending(&self.db, &apollo, Some(&self.brand), limit, phone).await;
        drop(work);
        match result {
            Ok(s) => {
                ui::activity(
                    "Enriched people",
                    format!(
                        "{} attempted · {} emails found · {} verified",
                        s.attempted, s.emails_found, s.verified
                    ),
                );
                Ok(s)
            }
            Err(e) => Err(format!("Enrichment failed: {e:#}")),
        }
    }

    async fn enrich_people(&self, limit: usize, phone: bool) -> String {
        match self.do_enrich(limit, phone).await {
            Ok(s) => format!(
                "Enriched {} people: {} emails found, {} verified. Next, ask me to plan outreach.",
                s.attempted, s.emails_found, s.verified
            ),
            Err(e) => e,
        }
    }

    /// Write sequences for verified people, returning the summary. `replace`
    /// re-drafts existing (unsent) sequences to improve them; the agent always
    /// sequences every verified contact already found (no Apollo spend), leaving
    /// real send volume to the send-time account limits.
    async fn do_plan(
        &self,
        touches: usize,
        auto: bool,
        replace: bool,
        only_person_ids: Option<&std::collections::HashSet<String>>,
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
        let work = ui::Spinner::start("Drafting outreach");
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
            true,
            only_person_ids,
        )
        .await;
        drop(work);
        match result {
            Ok(s) => {
                ui::activity(
                    "Drafted outreach",
                    format!(
                        "{} people · {} scheduled · {} drafts",
                        s.people_planned, s.touches_scheduled, s.touches_drafted
                    ),
                );
                Ok(s)
            }
            Err(e) => Err(format!("Outreach planning failed: {e:#}")),
        }
    }

    async fn plan_outreach(&self, touches: usize, auto: bool, replace: bool) -> String {
        match self.do_plan(touches, auto, replace, None).await {
            Ok(s) => format!(
                "Planned {} people: {} email touches scheduled and {} touches left as drafts.{}",
                s.people_planned,
                s.touches_scheduled,
                s.touches_drafted,
                if auto {
                    ""
                } else {
                    " Ask me to approve the email drafts when you're ready."
                }
            ),
            Err(e) => e,
        }
    }

    /// Run the whole outbound motion from one request: source real leads →
    /// reveal & verify their emails → draft the sequences. It stops before
    /// anything is sent (sequences are held as drafts for approval). Enrichment
    /// spends Apollo credits, so it's only reached when sourcing returned people.
    async fn run_full_motion(
        &self,
        thesis: &str,
        accounts: usize,
        contacts: usize,
        touches: usize,
    ) -> String {
        let before_ids: std::collections::HashSet<String> = self
            .db
            .list_people(Some(&self.brand), None)
            .map(|people| people.into_iter().map(|person| person.id).collect())
            .unwrap_or_default();

        // 1. Source real accounts + people.
        let src = match self.do_source(thesis, accounts, contacts).await {
            Ok(s) => s,
            Err(e) => return e,
        };
        // Exactly the people this run added — drafting is scoped to these so the
        // full motion never re-drafts the brand's whole accumulated backlog.
        let new_ids: std::collections::HashSet<String> = self
            .db
            .list_people(Some(&self.brand), None)
            .map(|people| {
                people
                    .into_iter()
                    .filter(|person| !before_ids.contains(&person.id))
                    .map(|person| person.id)
                    .collect()
            })
            .unwrap_or_default();
        let new_people = new_ids.len();

        // Nothing to enrich → stop before spending credits, but still show the CRM.
        if new_people == 0 {
            if src.leads_qualified > 0 {
                open_browser(&self.crm_url());
                ui::activity("Opened CRM dashboard", self.crm_url());
            }
            return format!(
                "Sourced {} orgs → {} qualified leads, but 0 contacts came back, so I stopped before spending any enrichment credits. A tighter, single-vertical thesis usually returns real people — want me to re-source narrower?",
                src.orgs_found, src.leads_qualified
            );
        }

        // 2. Reveal + verify emails for the freshly-sourced people (spends credits).
        let enr = match self.do_enrich(new_people, false).await {
            Ok(s) => s,
            Err(e) => {
                open_browser(&self.crm_url());
                return format!(
                    "Sourced {} people into the CRM at {}, but enrichment stopped: {e}",
                    src.people_added,
                    self.crm_url()
                );
            }
        };

        // 3. Draft the sequences (auto=false → held for approval, nothing sends).
        //    Scoped to this run's people so we don't re-draft the whole backlog.
        let pln = match self.do_plan(touches, false, false, Some(&new_ids)).await {
            Ok(s) => s,
            Err(e) => {
                open_browser(&self.crm_url());
                return format!(
                    "Sourced and enriched into the CRM at {}, but drafting the sequences stopped: {e}",
                    self.crm_url()
                );
            }
        };

        open_browser(&self.crm_url());
        ui::activity("Opened CRM dashboard", self.crm_url());

        if pln.people_planned == 0 {
            return format!(
                "Sourced {orgs} orgs → {leads} leads → {people} people, but only {verified} had a verifiable email, so there was no one with a confirmed address to sequence yet. Everything's in the CRM at {url}. Want me to widen sourcing or relax the verified-email gate?",
                orgs = src.orgs_found,
                leads = src.leads_qualified,
                people = src.people_added,
                verified = enr.verified,
                url = self.crm_url(),
            );
        }

        format!(
            "Full motion done: {planned} sequence(s) drafted for {verified} verified contacts across {leads} account(s), all in the CRM at {url}. Nothing has been sent — say \"approve\" to schedule.",
            planned = pln.people_planned,
            verified = enr.verified,
            leads = src.leads_qualified,
            url = self.crm_url(),
        )
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
            .filter(|a| scope.map_or(true, |brand| a.brand == brand))
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
            "Route one terminal request to exactly one structured action. Write `plan` first as one first-person sentence (max 20 words), then fill only fields needed by the action.\n\n\
{brand_mode}\n\n\
Actions:\n\
- run_campaign: hypothetical research-only campaign; no Apollo.\n\
- source_leads: ONLY finds and qualifies Apollo accounts+people, then stops — it writes NO emails. Use only when the request is purely to find companies/people. set thesis/accounts/contacts (defaults 10/3).\n\
- run_full_motion: the end-to-end motion — source, enrich, AND draft the sequences in one request (never sends). Use this whenever ONE request asks to both find companies/people AND write/draft/create outreach or a sequence. set thesis/accounts/contacts/touches (defaults 5/5/7).\n\
- enrich_people: reveal/verify sourced emails; phone only when explicit.\n\
- plan_outreach: draft or re-draft sequences for contacts ALREADY found (no Apollo spend). set replace=true to rewrite/improve existing drafts; auto only when explicit.\n\
- approve_outreach: only after explicit approval — this is what actually schedules drafts to send.\n\
- discover_opportunities, list_opportunities (actionable when requested), resolve_opportunity_contacts (enrich only with explicit credit authorization), plan_funding_outreach (auto only when explicit), approve_funding_outreach (reviewed opportunity_id + explicit approval), prepare_application: the funding/procurement motion; set opportunity_id where needed.\n\
- show_funnel, show_calendar, list_accounts, list_opportunities, show_learnings, open_crm: direct read/open actions (leave brand empty to span all brands).\n\
- search_knowledge: put the topic in query.\n\
- reply: everything else; at most two short plain-text sentences.\n\n\
Available brands: {brands}. Drafting always HOLDS sequences for review; nothing sends until approve_outreach. Do not ask for confirmation when the action performs the request. Apollo identities are real but commercial conclusions remain hypotheses. Never claim funding eligibility without every mandatory criterion, invent evidence, or treat every instrument as a grant.",
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
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["plan", "action", "reply"],
        "properties": {
            "plan": {
                "type": "string",
                "description": "Write this FIRST: ONE short first-person sentence (≤20 words) of what you'll do and why. Shown live as your thinking. Not a multi-step plan, not a list, not user-facing prose, not a summary of the answer."
            },
            "action": {
                "type": "string",
                "enum": ["run_campaign", "source_leads", "run_full_motion", "enrich_people", "plan_outreach", "approve_outreach", "discover_opportunities", "list_opportunities", "resolve_opportunity_contacts", "plan_funding_outreach", "approve_funding_outreach", "prepare_application", "show_funnel", "show_calendar", "list_accounts", "show_learnings", "open_crm", "search_knowledge", "reply"]
            },
            "reply": { "type": "string", "description": "Message to show the user: at most two short plain-text sentences, no markdown/lists/emoji, no re-asking to confirm steps the action already does." },
            "thesis": { "type": "string", "description": "For run_campaign: the workflow/market to target." },
            "query": { "type": "string", "description": "For search_knowledge: the topic to look up in the ingested books." },
            "brand": { "type": "string", "enum": brands, "description": "The brand this request concerns; set it per request. Leave empty for portfolio-wide reads (they span all brands)." },
            "accounts": { "type": "integer" },
            "contacts": { "type": "integer" },
            "touches": { "type": "integer" },
            "limit": { "type": "integer" },
            "phone": { "type": "boolean" },
            "auto": { "type": "boolean" }
            ,"replace": { "type": "boolean", "description": "For plan_outreach: re-draft/improve existing unsent sequences instead of skipping contacts who already have one. Reuses contacts already found; no Apollo spend." }
            ,"enrich": { "type": "boolean", "description": "Reveal Apollo opportunity-contact emails now; costs credits." }
            ,"actionable": { "type": "boolean", "description": "For opportunity lists, exclude closed, ineligible, lost, and expired records." }
            ,"opportunity_id": { "type": "string", "description": "Persisted opportunity id for contact, outreach, or application actions." }
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
    use super::decision_schema;

    #[test]
    fn decision_schema_requires_a_visible_plan() {
        let schema = decision_schema(&["test-brand"]);
        let required = schema["required"].as_array().expect("required fields");

        assert!(required.iter().any(|field| field == "plan"));
        assert_eq!(schema["properties"]["plan"]["type"], "string");
    }
}
