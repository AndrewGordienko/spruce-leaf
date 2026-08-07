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
use crate::engine::Engine;
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
    /// Active brand key (gnk | wapahki | outagehub).
    brand: String,
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
            critique,
            port,
            concurrency,
            history: Vec::new(),
        }
    }

    pub fn crm_url(&self) -> String {
        format!("http://localhost:{}", self.port)
    }

    pub fn brand(&self) -> &str {
        &self.brand
    }

    pub fn backend(&self) -> &str {
        self.client.backend().as_str()
    }

    pub fn model(&self) -> &str {
        self.client.model_label()
    }

    pub fn brand_keys(&self) -> Vec<&str> {
        self.playbooks.keys()
    }

    /// Switch the active brand if `key` is valid; returns whether it changed.
    pub fn set_brand(&mut self, key: &str) -> bool {
        if self.playbooks.get(key).is_ok() && self.businesses.get(key).is_ok() {
            self.brand = key.to_string();
            true
        } else {
            false
        }
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
            .structured_streamed(
                &self.system(),
                &prompt,
                decision_schema(&self.brand_keys()),
                &mut |ev| turn.on_event(ev),
            )
            .await?;
        // Whether the model already streamed a visible answer for this turn.
        let streamed = turn.finish();

        // The router may switch brands as part of a request.
        if !decision.brand.trim().is_empty() {
            self.set_brand(decision.brand.trim());
        }

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
                self.plan_outreach(decision.touches.unwrap_or(7).max(1) as usize, decision.auto)
                    .await
            }
            "approve_outreach" => self.approve_outreach(),
            "discover_opportunities" => {
                self.discover_opportunities(decision.limit.unwrap_or(20).max(1) as usize)
                    .await
            }
            "list_opportunities" => self.list_opportunities(decision.actionable),
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
            "show_funnel" => self.show_funnel(),
            "show_calendar" => self.show_calendar(),
            "list_accounts" => self.list_accounts().await,
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

    /// Write sequences for verified people, returning the summary.
    async fn do_plan(&self, touches: usize, auto: bool) -> Result<outreach::PlanSummary, String> {
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

    async fn plan_outreach(&self, touches: usize, auto: bool) -> String {
        match self.do_plan(touches, auto).await {
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
        let before = self
            .db
            .list_people(Some(&self.brand), None)
            .map(|p| p.len())
            .unwrap_or(0);

        // 1. Source real accounts + people.
        let src = match self.do_source(thesis, accounts, contacts).await {
            Ok(s) => s,
            Err(e) => return e,
        };
        let after = self
            .db
            .list_people(Some(&self.brand), None)
            .map(|p| p.len())
            .unwrap_or(before);
        let new_people = after.saturating_sub(before);

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
        let pln = match self.do_plan(touches, false).await {
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

    fn list_opportunities(&self, actionable_only: bool) -> String {
        match self.db.list_opportunities(Some(&self.brand), None) {
            Ok(mut opportunities) => {
                if actionable_only {
                    opportunities.retain(opportunity::is_actionable);
                }
                ui::activity(
                    "Read opportunity pipeline",
                    format!("{} opportunity/opportunities", opportunities.len()),
                );
                opportunity::render_opportunities(&opportunities)
            }
            Err(e) => format!("Couldn't read opportunities: {e:#}"),
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

    fn show_funnel(&self) -> String {
        match metrics::funnel(&self.db, Some(&self.brand)) {
            Ok(f) => {
                ui::activity("Read sales funnel", &self.brand);
                metrics::render(&f)
            }
            Err(e) => format!("Couldn't read funnel metrics: {e:#}"),
        }
    }

    async fn list_accounts(&self) -> String {
        let real = match self.db.list_leads(Some(&self.brand)) {
            Ok(leads) => leads,
            Err(e) => return format!("Couldn't read execution leads: {e:#}"),
        };
        let people = match self.db.list_people(Some(&self.brand), None) {
            Ok(people) => people,
            Err(e) => return format!("Couldn't read execution people: {e:#}"),
        };
        let store = self.store.read().await;
        if real.is_empty() && store.data.accounts.is_empty() {
            ui::activity("Read CRM", "No accounts found");
            return "The CRM is empty \u{2014} no campaigns run yet.".to_string();
        }
        ui::activity(
            "Read CRM",
            format!(
                "{} real leads · {} research accounts · {} people",
                real.len(),
                store.data.accounts.len(),
                people.len()
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
        if !store.data.accounts.is_empty() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("Research-only campaign hypotheses:\n");
        }
        for a in &store.data.accounts {
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
        let calendar_intelligence = self
            .businesses
            .get(&self.brand)
            .and_then(|profile| calendar::agent_intelligence(profile, &self.db))
            .unwrap_or_default();
        let opportunities = self
            .db
            .list_opportunities(Some(&self.brand), None)
            .unwrap_or_default();
        p.push_str(&format!(
            "Active business: {}. {} {} The research CRM holds {n} accounts. The real execution db holds {} qualified leads, {} people, {} verified emails, {} contacted people, and {} persisted opportunities.\n\n",
            self.brand,
            business,
            calendar_intelligence,
            execution.leads,
            execution.people,
            execution.verified,
            execution.contacted,
            opportunities.len(),
        ));
        p.push_str(&format!("User: {input}"));
        p
    }

    fn system(&self) -> String {
        format!(
            "You are spruce-leaf, an interactive business-development agent running in the user's \
terminal, backed by a local model CLI. You manage distinct sales, funding, and partnership motions \
for three businesses using their configured business profiles and outreach playbooks. For each user message you first \
think out loud in `plan` (ONE short first-person sentence of intent, \u{2264}20 words — streamed as \
your live thinking; not a multi-step plan, not user-facing prose), then choose exactly one action \
and return it as structured JSON:\n\
- run_campaign: research-only hypothesis generation when the user explicitly wants a simulated \
campaign, ideas, or draft strategy without Apollo. Generated companies/people are hypotheses.\n\
- source_leads: the user wants REAL prospects/accounts/people, asks to source/find leads through \
Apollo, or wants to begin executable outreach. Set `thesis`; map companies\u{2192}`accounts` and \
people per company\u{2192}`contacts` (defaults 10/3).\n\
- run_full_motion: the user asks for the WHOLE outbound motion in one go — i.e. find companies \
AND the people AND write/prepare the sequences (e.g. \"find 5 companies, 5 people each, and write \
their sequences\"). This runs source → enrich (reveal/verify emails, spends Apollo credits) → \
draft the sequences in one pass, then stops for approval; nothing is sent. State intent in one \
line and never ask the user whether to proceed \u{2014} running straight through to drafts is the point. \
Set `thesis`, \
`accounts`, `contacts`, and `touches` (defaults 5/5/7). Prefer this over source_leads whenever the \
request already spans sourcing through sequences, so you don't make the user re-ask at each stage.\n\
- enrich_people: reveal and verify emails for already-sourced people. Set `limit` when given and \
`phone=true` only if the user explicitly requests phone reveal (it costs extra credits and \
requires APOLLO_WEBHOOK_URL because Apollo delivers results asynchronously).\n\
- plan_outreach: write sequences for verified people. Map stages/touches\u{2192}`touches` (default 7); \
set `auto=true` only if the user explicitly asks to schedule without approval.\n\
- approve_outreach: the user explicitly approves drafted email touches for the active brand.\n\
- discover_opportunities: find and conservatively qualify live grants, contributions, funded \
pilots, challenges, tax credits, procurement, advisory programmes, or other configured \
opportunities from official sources. Use the active business profile; set `limit` if given.\n\
- list_opportunities: show the active business's persisted opportunity pipeline and ids. Set \
`actionable=true` when the user asks what can be pursued now; this still includes records that \
need mandatory evidence and never means eligibility is proven.\n\
- resolve_opportunity_contacts: map the official programme route and relevant funder people using \
Apollo. Set `opportunity_id`, `contacts`, and `enrich=true` only when the user explicitly authorizes \
credit-consuming email reveal. Apollo finds people; it is not the grant search engine.\n\
- plan_funding_outreach: write 1-3 grant-appropriate pre-application emails for a selected \
opportunity. Set `opportunity_id`, `touches`; `auto=true` only with explicit authorization.\n\
- approve_funding_outreach: the user explicitly approves drafted opportunity emails. Require and \
set the reviewed `opportunity_id`; never approve every funding draft implicitly.\n\
- prepare_application: create an evidence-gapped go/no-go application brief for `opportunity_id`; \
never fabricate eligibility, finances, partners, TRL, metrics, or matching funds.\n\
- show_funnel: the user asks for execution stats, metrics, results, replies, or funnel status.\n\
- show_calendar: the user asks when outreach will happen, daily capacity, timezone handling, weekend rules, or observed timing intelligence.\n\
- list_accounts: the user wants to know which real leads or research accounts are in the CRM.\n\
- open_crm: the user wants to open the CRM dashboard.\n\
- search_knowledge: the user asks what the ingested books say about a topic (cold email, \
pricing, discovery, objections). Put the topic in `query`.\n\
- reply: anything else \u{2014} answer conversationally in `reply`.\n\
Active brand is {active}. If they name a brand ({brands}), set `brand`. Keep `reply` to at most two \
short sentences of plain text \u{2014} no markdown headings, numbered pipelines, bullet lists, or emoji \u{2014} \
and never re-ask the user to confirm a step the chosen action already performs. \
Never claim research-only generated accounts are real; Apollo-sourced identity fields are real, \
while commercial conclusions remain hypotheses to verify. Never claim a business is eligible for \
funding until every mandatory criterion is evidenced, and never call every funding instrument a grant.",
            brands = self.brand_keys().join(" | "),
            active = self.brand,
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
                "enum": ["run_campaign", "source_leads", "run_full_motion", "enrich_people", "plan_outreach", "approve_outreach", "discover_opportunities", "list_opportunities", "resolve_opportunity_contacts", "plan_funding_outreach", "approve_funding_outreach", "prepare_application", "show_funnel", "show_calendar", "list_accounts", "open_crm", "search_knowledge", "reply"]
            },
            "reply": { "type": "string", "description": "Message to show the user: at most two short plain-text sentences, no markdown/lists/emoji, no re-asking to confirm steps the action already does." },
            "thesis": { "type": "string", "description": "For run_campaign: the workflow/market to target." },
            "query": { "type": "string", "description": "For search_knowledge: the topic to look up in the ingested books." },
            "brand": { "type": "string", "enum": brands, "description": "Optional brand to switch to." },
            "accounts": { "type": "integer" },
            "contacts": { "type": "integer" },
            "touches": { "type": "integer" },
            "limit": { "type": "integer" },
            "phone": { "type": "boolean" },
            "auto": { "type": "boolean" }
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
