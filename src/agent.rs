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
#[derive(Debug, Clone, Default, Deserialize)]
struct Step {
    action: String,
    #[serde(default)]
    brand: String,
    #[serde(default)]
    thesis: String,
    #[serde(default)]
    query: String,
    /// Exact next response/action wanted from the named recipient. This is a
    /// private planning input, never buyer-facing copy.
    #[serde(default)]
    outcome: String,
    /// Exact existing person id, email, or name for a targeted planning step.
    #[serde(default)]
    person: String,
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

struct PlanOptions<'a> {
    touches: usize,
    auto: bool,
    replace: bool,
    only_person_ids: Option<&'a std::collections::HashSet<String>>,
    per_account_cap: Option<usize>,
    person_filter: Option<&'a str>,
    desired_outcome: Option<&'a str>,
    /// Standalone planning owns its transcript. Full motion already explains
    /// account selection, so nested readiness checks stay quiet unless copy is
    /// actually drafted or a real error needs to be surfaced.
    show_holds: bool,
}

struct FullMotionOptions<'a> {
    thesis: &'a str,
    accounts: usize,
    contacts: usize,
    touches: usize,
    force_new: bool,
    replace_drafts: bool,
    desired_outcome: Option<&'a str>,
}

fn nonempty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
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

/// Ordinary write/retry requests preserve current-policy drafts that already
/// passed review. Replacing good copy is an explicit operator intent; otherwise
/// a retry after partial rejection needlessly spends most of its model budget
/// regenerating recipients that are already complete.
fn requests_copy_replacement(input: &str) -> bool {
    let normalized = input.to_ascii_lowercase();
    [
        "rewrite",
        "re-write",
        "redraft",
        "re-draft",
        "regenerate",
        "replace the draft",
        "replace drafts",
        "revise",
        "refine",
        "improve the copy",
        "start over",
        "redo all",
    ]
    .iter()
    .any(|phrase| normalized.contains(phrase))
}

fn count_token(token: &str) -> Option<usize> {
    token.parse::<usize>().ok().or_else(|| {
        Some(match token {
            "one" => 1,
            "two" => 2,
            "three" => 3,
            "four" => 4,
            "five" => 5,
            "six" => 6,
            "seven" => 7,
            "eight" => 8,
            "nine" => 9,
            "ten" => 10,
            _ => return None,
        })
    })
}

fn signal_label(key: &str) -> &str {
    match key {
        "account.fit_evidence" => "specific account fit",
        "account.expensive_recurring_workflow" => "a recurring workflow with material consequence",
        "account.cross_system_reconciliation" => "people reconciling records across systems",
        "account.reachable_workflow_owner" => "a reachable workflow owner",
        "account.outage_sensitive_decision" => "an outage-sensitive operating decision",
        "account.distributed_locations" => "distributed locations",
        "account.operated_ev_charging_network" => "an operated Canadian EV charging network",
        "account.historical_location_outage_match" => {
            "a verified charging-location / utility-outage match"
        }
        "account.existing_operational_system" => "an existing operational workflow surface",
        "account.specific_recurring_decision" => "a specific recurring decision",
        "account.believable_operating_consequence" => "a believable operating consequence",
        "account.external_trigger_or_mechanism_evidence" => {
            "an external trigger or direct mechanism evidence"
        }
        "account.bounded_repetitive_task" => "a bounded repetitive task",
        "account.format_variability" => "format or SKU variability",
        "account.exception_heavy_manual_work" => "manual exception handling",
        _ => key,
    }
}

/// A bare "first N people" is a total in CRM order, not N people from each of
/// the router's default five accounts. Resolve this costly ambiguity from the
/// original wording before Apollo enrichment or drafting begins.
fn unqualified_people_total(input: &str) -> Option<usize> {
    let tokens = input
        .to_ascii_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let person_nouns = [
        "person",
        "people",
        "contact",
        "contacts",
        "recipient",
        "recipients",
    ];
    let account_nouns = ["account", "accounts", "company", "companies"];

    if tokens.iter().any(|token| token == "each" || token == "per") {
        return None;
    }
    let has_account_scope = tokens.iter().enumerate().any(|(index, token)| {
        let numbered = count_token(token).is_some()
            && tokens
                .iter()
                .skip(index + 1)
                .take(3)
                .any(|next| account_nouns.contains(&next.as_str()));
        let first_account = token == "first"
            && tokens
                .iter()
                .skip(index + 1)
                .take(4)
                .any(|next| account_nouns.contains(&next.as_str()));
        numbered || first_account
    });
    if has_account_scope {
        return None;
    }

    tokens.iter().enumerate().find_map(|(index, token)| {
        if token != "first" {
            return None;
        }
        let count = tokens.get(index + 1).and_then(|token| count_token(token))?;
        tokens
            .iter()
            .skip(index + 2)
            .take(3)
            .any(|noun| person_nouns.contains(&noun.as_str()))
            .then_some(count.max(1))
    })
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
        // A drafting scope is a deliverable scope, so already verified people
        // come before identities that may consume a reveal credit and still
        // fail. Within each deliverability tier, keep workflow-primary people
        // first. The old primary-first order could select an undeliverable row
        // while leaving a verified colleague outside the requested cap.
        roster.sort_by_key(|person| {
            (
                person.status.eq_ignore_ascii_case("suppressed"),
                !person.email_status.eq_ignore_ascii_case("verified"),
                !person.primary,
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
        self.client.begin_turn_budget();
        let prompt = self.build_prompt(input).await;

        let mut turn = ui::TurnView::new();
        let brand_keys = self.brand_keys();
        let routed = self
            .client
            .structured_fast_streamed(
                "interactive.router",
                &self.system(),
                &prompt,
                decision_schema(&brand_keys),
                &mut |ev| turn.on_event(ev),
            )
            .await;
        // Router output is intentionally private, so conversational replies are
        // rendered once from the accepted structured decision.
        let streamed = turn.finish();
        let mut decision: Decision = match routed {
            Ok(decision) => decision,
            Err(error) => {
                let Some(decision) =
                    deterministic_full_motion_fallback(input, &brand_keys, self.brand.as_str())
                else {
                    return Err(error);
                };
                ui::activity(
                    "Recovered router formatting",
                    "recognized the explicit full-motion command locally",
                );
                decision
            }
        };
        coalesce_full_motion_steps(&mut decision.steps, &self.brand);
        let (outreach_accounts, outreach_recipients) = requested_outreach_scope(&decision.steps);
        self.client
            .scale_turn_budget_for_outreach(outreach_accounts, outreach_recipients);

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
            let (title, detail) = self.step_intent(step, input);
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

    fn step_intent(&self, step: &Step, input: &str) -> (String, String) {
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
                let scope = unqualified_people_total(input)
                    .map(|people| format!("{people} people total in CRM order"))
                    .unwrap_or_else(|| match (accounts, contacts) {
                        (1, contacts) if contacts > 0 => {
                            format!("{contacts} people from the first account")
                        }
                        (accounts, contacts) if accounts > 0 && contacts > 0 => {
                            format!("{contacts} people from each of {accounts} accounts")
                        }
                        _ => "selected verified people".into(),
                    });
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
                    "{brand} · {} accounts × {} recipients × {} touches · drafts only",
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
            "research_account" => {
                let query = if step.query.trim().is_empty() {
                    step.thesis.trim()
                } else {
                    step.query.trim()
                };
                self.research_account(query, step.thesis.trim()).await
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
                self.run_full_motion(FullMotionOptions {
                    thesis: &thesis,
                    accounts,
                    contacts,
                    touches,
                    force_new: step.force_new,
                    replace_drafts: requests_copy_replacement(input),
                    desired_outcome: nonempty(&step.outcome),
                })
                .await
            }
            "enrich_people" => {
                self.enrich_people(step.limit.unwrap_or(50).max(1) as usize, step.phone)
                    .await
            }
            "plan_outreach" => {
                let text_total = unqualified_people_total(input);
                let (account_limit, contacts_per_account, total_limit) =
                    if let Some(total) = text_total {
                        (None, None, Some(total))
                    } else {
                        (
                            step.accounts.map(|value| value.max(1) as usize),
                            step.contacts.map(|value| value.max(1) as usize),
                            routed_total_limit(input, step.accounts, step.contacts, step.limit),
                        )
                    };
                self.plan_outreach(
                    step.touches.unwrap_or(7).max(1) as usize,
                    step.auto,
                    account_limit,
                    contacts_per_account,
                    total_limit,
                    !forbids_contact_enrichment(input),
                    requests_copy_replacement(input),
                    nonempty(&step.person),
                    nonempty(&step.outcome),
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
        let touches = outreach::supported_touch_count_for_brand(&self.brand, touches);

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
        candidate_limit: Option<usize>,
        transient: bool,
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
        let header = format!(
            "{} · {accounts} account target · {contacts} people each · active GTM play",
            pb.name
        );
        let view = if transient {
            ui::SourceView::start_transient(header, self.client.stats())
        } else {
            ui::SourceView::start(header, self.client.stats())
        };
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
            None,
            candidate_limit,
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
        match self
            .do_source(thesis, accounts, contacts, None, false)
            .await
        {
            Ok(s) => {
                // Surface the freshly-filed leads/people in the live CRM.
                if s.leads_qualified + s.leads_research_needed + s.leads_research_required > 0
                    || s.people_added > 0
                {
                    open_browser(&self.crm_url());
                    ui::activity("Opened CRM dashboard", self.crm_url());
                }
                format!(
                    "Sourced {} real organizations into {} easy, {} medium, and {} hard-research lead record(s), plus {} people, now filed in the CRM at {}. Only easy and precise medium accounts can reach copy. Next, ask me to enrich their emails.",
                    s.orgs_found,
                    s.leads_qualified,
                    s.leads_research_needed,
                    s.leads_research_required,
                    s.people_added,
                    self.crm_url()
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
        transient: bool,
    ) -> Result<enrich::EnrichSummary, String> {
        let apollo = Apollo::from_env().map_err(|e| format!("Can't enrich: {e:#}"))?;
        let header = format!(
            "{} · up to {limit} contacts · email reveal + verification{}",
            self.playbooks
                .get(&self.brand)
                .map(|playbook| playbook.name.as_str())
                .unwrap_or(self.brand.as_str()),
            if phone { " + phone" } else { "" }
        );
        let view = if transient {
            ui::SourceView::start_enrichment_transient(header, self.client.stats())
        } else {
            ui::SourceView::start_enrichment(header, self.client.stats())
        };
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
        match self.do_enrich(limit, phone, None, false).await {
            Ok(s) => format!(
                "Enriched {} people: {} emails found, {} verified. Next, ask me to plan outreach.",
                s.attempted, s.emails_found, s.verified
            ),
            Err(e) => e,
        }
    }

    /// Write sequences for verified people, returning the summary. `replace`
    /// re-drafts existing (unsent) sequences to improve them. Bulk motions map
    /// the requested contacts but open cold outreach with only the strongest
    /// person at each account. An explicit single-person request remains exact.
    /// No account or people search occurs here; real send volume remains bounded
    /// by send-time account limits.
    async fn do_plan(&self, options: PlanOptions<'_>) -> Result<outreach::PlanSummary, String> {
        let PlanOptions {
            touches,
            auto,
            replace,
            only_person_ids,
            per_account_cap,
            person_filter,
            desired_outcome,
            show_holds,
        } = options;
        let requested_touches = touches.max(1);
        let touches = outreach::supported_touch_count_for_brand(&self.brand, requested_touches);
        if touches != requested_touches {
            ui::activity(
                "Normalized eager sequence",
                format!(
                    "requested {requested_touches} touches · drafting the supported {touches}-touch shape · later follow-ups should be earned by the live thread"
                ),
            );
        }
        let pb = self
            .playbooks
            .get(&self.brand)
            .map_err(|e| format!("Can't plan outreach: {e:#}"))?;
        let business = self
            .businesses
            .get(&self.brand)
            .map_err(|e| format!("Can't plan outreach: {e:#}"))?;
        let lib = self.library.read().await.clone();
        let mut ready_scope = None;
        let mut people_held = 0usize;
        if let Some(requested) = only_person_ids {
            let mut ready = std::collections::HashSet::new();
            let mut held_accounts =
                std::collections::BTreeMap::<String, (String, Vec<String>)>::new();
            for person_id in requested {
                let Some(person) = self.db.get_person(person_id).ok().flatten() else {
                    continue;
                };
                match crate::gtm::prepare_action(&self.db, &self.brand, &person.lead_id, &person) {
                    Ok(context) if context.sequence_ready_for(touches) => {
                        ready.insert(person_id.clone());
                    }
                    Ok(context) => {
                        people_held += 1;
                        let account = self
                            .db
                            .get_lead(&person.lead_id)
                            .ok()
                            .flatten()
                            .map(|lead| lead.name)
                            .unwrap_or_else(|| "Unknown account".to_string());
                        let (matched, required, missing) = context.play.as_ref().map_or_else(
                            || (0, 1, vec!["an active GTM play".to_string()]),
                            |play| {
                                let matched = play
                                    .required_signal_keys
                                    .iter()
                                    .filter(|key| context.matched_signal_keys.contains(key))
                                    .count();
                                let missing = play
                                    .required_signal_keys
                                    .iter()
                                    .filter(|key| !context.matched_signal_keys.contains(key))
                                    .map(|key| signal_label(key).to_string())
                                    .collect::<Vec<_>>();
                                (
                                    matched,
                                    play.minimum_signal_matches.max(1) as usize,
                                    missing,
                                )
                            },
                        );
                        let reason = format!(
                            "{matched}/{required} required signals supported; missing evidence: {}",
                            missing.join(", ")
                        );
                        held_accounts
                            .entry(account)
                            .and_modify(|(_, names)| names.push(person.name.clone()))
                            .or_insert_with(|| (reason, vec![person.name.clone()]));
                    }
                    Err(error) => {
                        people_held += 1;
                        held_accounts
                            .entry("Readiness check".to_string())
                            .and_modify(|(_, names)| names.push(person.name.clone()))
                            .or_insert_with(|| (error.to_string(), vec![person.name.clone()]));
                    }
                }
            }
            if !held_accounts.is_empty() && show_holds {
                ui::activity(
                    "Held weak account hypotheses",
                    format!(
                        "{} contact(s) not sent to the copywriter · {} · research or re-source before a multi-touch sequence",
                        people_held,
                        held_accounts
                            .into_iter()
                            .map(|(account, (reason, mut names))| {
                                names.sort();
                                names.dedup();
                                format!("{account} [{}]: {reason}", names.join(", "))
                            })
                            .collect::<Vec<_>>()
                            .join("; ")
                    ),
                );
            }
            ready_scope = Some(ready);
        }
        let planning_scope = ready_scope.as_ref().or(only_person_ids);
        if planning_scope.is_some_and(|scope| scope.is_empty()) {
            return Ok(outreach::PlanSummary {
                people_held,
                ..Default::default()
            });
        }
        let view = ui::OutreachView::start(
            format!(
                "{} · {touches} touches each · {} · quality {} · drafts stream into CRM before review finishes",
                pb.name,
                if auto {
                    "auto-schedule eligible"
                } else {
                    "drafts only"
                },
                self.client.outreach_quality_label(),
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
            person_filter,
            replace,
            per_account_cap,
            planning_scope,
            desired_outcome,
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
            Ok(mut s) => {
                s.people_held += people_held;
                Ok(s)
            }
            Err(e) => Err(format!("Outreach planning failed: {e:#}")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn plan_outreach(
        &self,
        touches: usize,
        auto: bool,
        account_limit: Option<usize>,
        contacts_per_account: Option<usize>,
        total_limit: Option<usize>,
        fill_contact_coverage: bool,
        replace_existing: bool,
        person_filter: Option<&str>,
        desired_outcome: Option<&str>,
    ) -> String {
        let scoped =
            account_limit.is_some() || contacts_per_account.is_some() || total_limit.is_some();
        let mut scope_note = String::new();
        let mut only_person_ids = if scoped {
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
                        false,
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
                " Scope: {} requested, {} existing selected, {} verified identities across {} account(s).{}",
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

        // Existing scoped contacts can carry legacy account framing that
        // predates the current versioned play. Reassess only their accounts
        // before the readiness gate. This spends no Apollo credits and keeps a
        // multi-touch request from turning an old speculative hypothesis into
        // polished copy.
        if let Some(person_ids) = only_person_ids.as_ref() {
            let mut lead_ids = person_ids
                .iter()
                .filter_map(|person_id| self.db.get_person(person_id).ok().flatten())
                .map(|person| person.lead_id)
                .collect::<Vec<_>>();
            lead_ids.sort();
            lead_ids.dedup();
            self.do_refresh_context("", &lead_ids, true, false).await;
        }

        // A bare "first N people" means the first N sequenceable people, not
        // "take one speculative account and return zero." Keep the originally
        // requested contacts visible to the readiness gate, but backfill from
        // evidence-ready CRM contacts when some are held. If no ready contacts
        // are already on file, deepen a small bounded set of additional
        // accounts from their official sites. This never spends Apollo credits
        // and never weakens the evidence threshold.
        if let (Some(target), Some(person_ids)) = (total_limit, only_person_ids.as_mut()) {
            let ready_count = |ids: &std::collections::HashSet<String>| {
                ids.iter()
                    .filter(|person_id| {
                        self.db
                            .get_person(person_id)
                            .ok()
                            .flatten()
                            .is_some_and(|person| {
                                crate::gtm::prepare_action(
                                    &self.db,
                                    &self.brand,
                                    &person.lead_id,
                                    &person,
                                )
                                .is_ok_and(|context| context.sequence_ready_for(touches))
                            })
                    })
                    .count()
            };
            let before = ready_count(person_ids);
            if before < target {
                let all_people = self
                    .db
                    .list_people(Some(&self.brand), None)
                    .unwrap_or_default();
                for person in &all_people {
                    if ready_count(person_ids) >= target {
                        break;
                    }
                    if person_ids.contains(&person.id)
                        || !person.status.eq_ignore_ascii_case("verified")
                        || !person.email_status.eq_ignore_ascii_case("verified")
                    {
                        continue;
                    }
                    if crate::gtm::prepare_action(&self.db, &self.brand, &person.lead_id, person)
                        .is_ok_and(|context| context.sequence_ready_for(touches))
                    {
                        person_ids.insert(person.id.clone());
                    }
                }

                if ready_count(person_ids) < target {
                    let selected_leads = person_ids
                        .iter()
                        .filter_map(|person_id| self.db.get_person(person_id).ok().flatten())
                        .map(|person| person.lead_id)
                        .collect::<std::collections::HashSet<_>>();
                    let max_accounts = std::env::var("SPRUCE_SCOPE_BACKFILL_ACCOUNTS")
                        .ok()
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(4)
                        .clamp(1, 10);
                    let candidate_leads = self
                        .db
                        .list_leads(Some(&self.brand))
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|lead| !selected_leads.contains(&lead.id))
                        .filter(|lead| {
                            all_people.iter().any(|person| {
                                person.lead_id == lead.id
                                    && person.status.eq_ignore_ascii_case("verified")
                                    && person.email_status.eq_ignore_ascii_case("verified")
                            })
                        })
                        .take(max_accounts)
                        .map(|lead| lead.id)
                        .collect::<Vec<_>>();
                    if !candidate_leads.is_empty() {
                        ui::activity(
                            "Deepening replacement accounts",
                            format!(
                                "{} existing account(s) · official-site evidence only · no Apollo",
                                candidate_leads.len()
                            ),
                        );
                        self.do_refresh_context("", &candidate_leads, true, false)
                            .await;
                        for person in &all_people {
                            if ready_count(person_ids) >= target {
                                break;
                            }
                            if person_ids.contains(&person.id)
                                || !candidate_leads.contains(&person.lead_id)
                                || !person.status.eq_ignore_ascii_case("verified")
                                || !person.email_status.eq_ignore_ascii_case("verified")
                            {
                                continue;
                            }
                            if crate::gtm::prepare_action(
                                &self.db,
                                &self.brand,
                                &person.lead_id,
                                person,
                            )
                            .is_ok_and(|context| context.sequence_ready_for(touches))
                            {
                                person_ids.insert(person.id.clone());
                            }
                        }
                    }
                }
                let after = ready_count(person_ids);
                if after > before {
                    ui::activity(
                        "Backfilled evidence-ready contacts",
                        format!(
                            "{} replacement(s) added from existing CRM accounts · {after}/{target} ready",
                            after - before
                        ),
                    );
                    scope_note.push_str(&format!(
                        " Readiness backfill added {} existing contact(s).",
                        after - before
                    ));
                } else {
                    ui::activity(
                        "No evidence-ready replacements found",
                        format!(
                            "{before}/{target} ready after bounded CRM research · sourcing new accounts would require a separate Apollo action"
                        ),
                    );
                }
            }
        }
        match self
            // Explicit rewrites safely replace only unsent drafts; sent
            // sequences remain protected by the persistence layer. Ordinary
            // retries preserve accepted current-policy drafts and fill only
            // missing/rejected recipients.
            .do_plan(PlanOptions {
                touches,
                auto,
                replace: replace_existing,
                only_person_ids: only_person_ids.as_ref(),
                per_account_cap: None,
                person_filter,
                desired_outcome,
                show_holds: true,
            })
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
                } else if s.people_rejected > 0 && s.people_held > 0 {
                    " No copy is approval-eligible: recipient-specific rejection feedback is preserved in CRM. The separately held contacts need stronger source-backed workflow evidence before drafting."
                } else if s.people_rejected > 0 {
                    " Nothing is approval-eligible; recipient-specific rejection feedback is preserved in CRM."
                } else if s.people_held > 0 {
                    " These contacts need stronger source-backed workflow evidence; refresh the account or source a better-qualified company before drafting."
                } else {
                    " Nothing new is approval-eligible."
                };
                format!(
                    "Drafted {} reviewed sequence(s): {} email touches scheduled, {} touches held as drafts, {} recipient(s) rejected, {} contact(s) held for research.{stop}{scope_note}{next}",
                    s.people_planned,
                    s.touches_scheduled,
                    s.touches_drafted,
                    s.people_rejected,
                    s.people_held,
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
    async fn run_full_motion(&self, options: FullMotionOptions<'_>) -> String {
        let FullMotionOptions {
            thesis,
            accounts,
            contacts,
            touches,
            force_new,
            replace_drafts,
            desired_outcome,
        } = options;
        let accounts = accounts.max(1);
        let contacts = contacts.max(1);
        let outreach_contacts = 1usize;
        let effective_touches = outreach::supported_touch_count_for_brand(&self.brand, touches);
        let max_source_passes = std::env::var("SPRUCE_FULL_MOTION_SOURCE_PASSES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(8)
            .clamp(1, 20);
        // This is deliberately larger than the account target: failed copy and
        // unverifiable contacts must have room to be replaced. The engine's own
        // model/cost budget remains the ultimate safety boundary.
        let max_motion_rounds = std::env::var("SPRUCE_FULL_MOTION_ROUNDS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or_else(|| accounts.saturating_mul(4).saturating_add(4))
            .clamp(1, 1000);

        let pb = match self.playbooks.get(&self.brand) {
            Ok(playbook) => playbook,
            Err(error) => return format!("Can't load {} playbook: {error:#}", self.brand),
        };
        let mut excluded = std::collections::HashSet::<String>::new();
        if force_new {
            if let Ok(leads) = self.db.list_leads(Some(&self.brand)) {
                excluded.extend(leads.into_iter().map(|lead| lead.id));
            }
        }
        let mut fulfilled = std::collections::HashSet::<String>::new();
        let mut source_total = sourcing::SourceSummary::default();
        let mut source_passes = 0usize;
        let mut consecutive_empty_source_passes = 0usize;
        let mut terminal_reason = None::<String>;
        let mut motion_rounds = 0usize;
        let mut refreshed_total = 0usize;
        let mut verified_total = 0usize;
        let mut people_selected_total = 0usize;
        let mut people_planned_total = 0usize;
        let mut people_rejected_total = 0usize;
        let mut people_held_total = 0usize;
        let mut people_stopped_total = 0usize;
        let mut touches_drafted_total = 0usize;
        let mut touches_scheduled_total = 0usize;

        while fulfilled.len() < accounts && motion_rounds < max_motion_rounds {
            motion_rounds += 1;
            let remaining = accounts.saturating_sub(fulfilled.len());
            let mut blocked = excluded.clone();
            blocked.extend(fulfilled.iter().cloned());
            let mut reuse = match sourcing::select_reuse_excluding(
                &self.db,
                pb,
                &self.brand,
                remaining,
                contacts,
                &blocked,
            ) {
                Ok(selection) => selection,
                Err(error) => {
                    terminal_reason =
                        Some(format!("could not inspect on-file inventory: {error:#}"));
                    break;
                }
            };
            if motion_rounds == 1 {
                ui::activity(
                    "Selecting accounts",
                    format!(
                        "target {accounts} · {} reusable account(s) · {} initial shortfall · {} mapped people ({} verified)",
                        reuse.accounts_on_file,
                        reuse.accounts_shortfall,
                        reuse.people_on_file,
                        reuse.verified_on_file,
                    ),
                );
            }

            // Keep widening and re-deriving the ICP within this same motion.
            // Each failed qualification pass is persisted as a correction and
            // therefore changes the next pass instead of repeating it blindly.
            while reuse.accounts_selected == 0
                && source_passes < max_source_passes
                && terminal_reason.is_none()
            {
                let want = remaining.saturating_sub(reuse.accounts_selected).max(1);
                source_passes += 1;
                if source_passes == 1 {
                    ui::activity(
                        if force_new {
                            "Finding new accounts"
                        } else {
                            "Finding replacements"
                        },
                        format!(
                            "need {want} more · up to {max_source_passes} adaptive searches · prior misses refine later searches"
                        ),
                    );
                }
                // Interleave upstream search with downstream fulfillment. A
                // ten-to-twenty-company deep-research wave can consume the
                // entire turn before writing begins; a bounded wave learns or
                // yields a working account without starving the copy stages.
                let candidate_limit = want.clamp(4, 6);
                match self
                    .do_source(thesis, want, contacts, Some(candidate_limit), true)
                    .await
                {
                    Ok(pass) => {
                        source_total.orgs_found += pass.orgs_found;
                        source_total.candidates_new += pass.candidates_new;
                        source_total.leads_qualified += pass.leads_qualified;
                        source_total.leads_research_needed += pass.leads_research_needed;
                        source_total.leads_research_required += pass.leads_research_required;
                        source_total.people_added += pass.people_added;
                        if pass.candidates_new == 0 {
                            consecutive_empty_source_passes += 1;
                        } else {
                            consecutive_empty_source_passes = 0;
                        }
                    }
                    Err(error) => {
                        terminal_reason = Some(error);
                        break;
                    }
                }

                reuse = match sourcing::select_reuse_excluding(
                    &self.db,
                    pb,
                    &self.brand,
                    remaining,
                    contacts,
                    &blocked,
                ) {
                    Ok(selection) => selection,
                    Err(error) => {
                        terminal_reason = Some(format!(
                            "could not inspect newly sourced inventory: {error:#}"
                        ));
                        break;
                    }
                };
                if consecutive_empty_source_passes >= 2 && reuse.accounts_selected < remaining {
                    terminal_reason = Some(
                        "Apollo returned no previously unseen companies on two successive adaptive searches"
                            .to_string(),
                    );
                    break;
                }
            }

            if reuse.lead_ids.is_empty() || reuse.person_ids.is_empty() {
                if terminal_reason.is_none() && source_passes >= max_source_passes {
                    terminal_reason = Some(format!(
                        "the configured safety ceiling of {max_source_passes} adaptive sourcing passes was reached"
                    ));
                }
                break;
            }

            people_selected_total += reuse.people_selected;

            // Reassess selected inventory against the current play before an
            // old reviewed sequence can satisfy the request. Copy-policy
            // freshness alone is not enough when the ICP/play version changed.
            // This also lets legacy accounts remain candidates for cheap reuse
            // without silently treating their old framing as current truth.
            refreshed_total += self
                .do_refresh_context(thesis, &reuse.lead_ids, false, false)
                .await;

            // Preserve already-reviewed current-policy work on an ordinary run.
            // An account is complete only when the requested number of people
            // each have the requested current-policy touch shape and the account
            // still clears the live play gate.
            let mut plan_leads = reuse.lead_ids.to_vec();
            if !replace_drafts {
                plan_leads.retain(|lead_id| {
                    let ready_people = reuse
                        .person_ids
                        .iter()
                        .filter_map(|person_id| self.db.get_person(person_id).ok().flatten())
                        .filter(|person| person.lead_id == *lead_id)
                        .filter(|person| {
                            crate::gtm::prepare_action(&self.db, &self.brand, lead_id, person)
                                .is_ok_and(|context| context.sequence_ready_for(effective_touches))
                        })
                        .filter(|person| {
                            self.db
                                .person_has_current_reviewed_sequence(&person.id, effective_touches)
                                .unwrap_or(false)
                        })
                        .count();
                    let already_done = ready_people >= outreach_contacts;
                    if already_done {
                        fulfilled.insert(lead_id.clone());
                    }
                    !already_done
                });
            }
            if plan_leads.is_empty() {
                continue;
            }
            let plan_lead_set = plan_leads
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>();
            let plan_person_ids = reuse
                .person_ids
                .iter()
                .filter(|person_id| {
                    self.db
                        .get_person(person_id)
                        .ok()
                        .flatten()
                        .is_some_and(|person| plan_lead_set.contains(&person.lead_id))
                })
                .cloned()
                .collect::<std::collections::HashSet<_>>();

            let need_enrich = self
                .db
                .list_people(Some(&self.brand), Some("new"))
                .map(|people| {
                    people
                        .into_iter()
                        .filter(|person| plan_person_ids.contains(&person.id))
                        .count()
                })
                .unwrap_or(0);
            if need_enrich > 0 {
                match self
                    .do_enrich(need_enrich, false, Some(&plan_person_ids), true)
                    .await
                {
                    Ok(summary) => verified_total += summary.verified,
                    Err(error) => {
                        let verified_now = self
                            .db
                            .list_people(Some(&self.brand), Some("verified"))
                            .map(|people| {
                                people
                                    .into_iter()
                                    .filter(|person| plan_person_ids.contains(&person.id))
                                    .count()
                            })
                            .unwrap_or(0);
                        if verified_now == 0 {
                            terminal_reason = Some(format!(
                                "contact enrichment could not produce a verified address: {error}"
                            ));
                            break;
                        }
                        ui::activity(
                            "Enrichment partial",
                            format!("{error} — drafting the {verified_now} verified contact(s)"),
                        );
                    }
                }
            } else {
                let verified_now = self
                    .db
                    .list_people(Some(&self.brand), Some("verified"))
                    .map(|people| {
                        people
                            .into_iter()
                            .filter(|person| plan_person_ids.contains(&person.id))
                            .count()
                    })
                    .unwrap_or(0);
                verified_total += verified_now;
            }

            let first_plan = self
                .do_plan(PlanOptions {
                    touches,
                    auto: false,
                    replace: replace_drafts,
                    only_person_ids: Some(&plan_person_ids),
                    per_account_cap: Some(outreach_contacts),
                    person_filter: None,
                    desired_outcome,
                    show_holds: false,
                })
                .await;
            let mut pass = match first_plan {
                Ok(summary) => summary,
                Err(error) => {
                    ui::activity(
                        "Drafting pass failed",
                        format!("{error} · replacing this working set and continuing"),
                    );
                    excluded.extend(plan_leads);
                    continue;
                }
            };

            people_planned_total += pass.people_planned;
            people_rejected_total += pass.people_rejected;
            people_held_total += pass.people_held;
            people_stopped_total += pass.people_stopped;
            touches_drafted_total += pass.touches_drafted;
            touches_scheduled_total += pass.touches_scheduled;
            for lead_id in &plan_leads {
                if self
                    .db
                    .lead_current_reviewed_sequence_count(lead_id, effective_touches)
                    .unwrap_or(0)
                    >= outreach_contacts
                {
                    fulfilled.insert(lead_id.clone());
                }
            }
            if let Some(reason) = pass.stopped_reason.take() {
                terminal_reason = Some(reason);
                break;
            }

            // Give rejected copy one fresh, feedback-informed whole-sequence pass
            // before abandoning the account. Successful accounts are excluded
            // from this retry so accepted copy is never churned unnecessarily.
            let retry_leads = plan_leads
                .iter()
                .filter(|lead_id| !fulfilled.contains(*lead_id))
                .cloned()
                .collect::<std::collections::HashSet<_>>();
            if pass.people_rejected > 0 && !retry_leads.is_empty() {
                let retry_people = plan_person_ids
                    .iter()
                    .filter(|person_id| {
                        let on_retry_account = self
                            .db
                            .get_person(person_id)
                            .ok()
                            .flatten()
                            .is_some_and(|person| retry_leads.contains(&person.lead_id));
                        on_retry_account
                            && !self
                                .db
                                .person_has_current_reviewed_sequence(person_id, effective_touches)
                                .unwrap_or(false)
                    })
                    .cloned()
                    .collect::<std::collections::HashSet<_>>();
                ui::activity(
                    "Retrying rejected sequences",
                    format!(
                        "{} account(s) · saved reviewer feedback becomes the rewrite brief",
                        retry_leads.len()
                    ),
                );
                match self
                    .do_plan(PlanOptions {
                        touches,
                        auto: false,
                        replace: true,
                        only_person_ids: Some(&retry_people),
                        per_account_cap: Some(outreach_contacts),
                        person_filter: None,
                        desired_outcome,
                        show_holds: false,
                    })
                    .await
                {
                    Ok(mut retry) => {
                        people_planned_total += retry.people_planned;
                        people_rejected_total += retry.people_rejected;
                        people_held_total += retry.people_held;
                        people_stopped_total += retry.people_stopped;
                        touches_drafted_total += retry.touches_drafted;
                        touches_scheduled_total += retry.touches_scheduled;
                        for lead_id in &plan_leads {
                            if self
                                .db
                                .lead_current_reviewed_sequence_count(lead_id, effective_touches)
                                .unwrap_or(0)
                                >= outreach_contacts
                            {
                                fulfilled.insert(lead_id.clone());
                            }
                        }
                        if let Some(reason) = retry.stopped_reason.take() {
                            terminal_reason = Some(reason);
                            break;
                        }
                    }
                    Err(error) => ui::activity(
                        "Rewrite pass failed",
                        format!("{error} · replacing the affected account(s)"),
                    ),
                }
            }

            // Anything that still has no reviewed sequence is not allowed to
            // occupy a requested output slot. Exclude it for this motion and let
            // the next round select or source a replacement.
            for lead_id in plan_leads {
                if !fulfilled.contains(&lead_id) {
                    excluded.insert(lead_id);
                }
            }
        }

        if fulfilled.len() < accounts && terminal_reason.is_none() {
            terminal_reason = Some(format!(
                "the configured safety ceiling of {max_motion_rounds} replacement rounds was reached"
            ));
        }

        open_browser(&self.crm_url());
        ui::activity("Opened CRM dashboard", self.crm_url());
        let apollo_note = if source_passes == 0 {
            "Apollo skipped; on-file inventory filled the motion".to_string()
        } else {
            format!(
                "Apollo ran {source_passes} adaptive pass(es): {} organizations seen, {} new candidates assessed, {} easy, {} medium, {} hard-research accounts, {} people added",
                source_total.orgs_found,
                source_total.candidates_new,
                source_total.leads_qualified,
                source_total.leads_research_needed,
                source_total.leads_research_required,
                source_total.people_added,
            )
        };

        if fulfilled.len() >= accounts {
            return format!(
                "Full motion filled {filled}/{accounts} account slots with one current reviewed {effective_touches}-touch sequence per account; {contacts} contact(s) per account may still be mapped for later routing. {planned} recipient sequence(s) were newly written; {drafted} touches remain drafts and {scheduled} were scheduled. {apollo_note}. Refreshed {refreshed_total} account(s) and processed {people_selected_total} recipient selection(s), including {verified_total} verified result(s). Nothing was sent. CRM: {url}.",
                filled = fulfilled.len(),
                planned = people_planned_total,
                drafted = touches_drafted_total,
                scheduled = touches_scheduled_total,
                url = self.crm_url(),
            );
        }

        format!(
            "Full motion persisted through {rounds} replacement round(s) and filled {filled}/{accounts} account slots at one cold recipient per account before hitting a real execution boundary: {reason}. Additional mapped contacts remain for later routing. A partially drafted account does not count as filled. {apollo_note}. It rejected {rejected} copy attempt(s), held {held} weak recipient/account attempt(s), and left {stopped} unfinished after the boundary; none of those counted as completed output. Nothing was sent. CRM: {url}.",
            rounds = motion_rounds,
            filled = fulfilled.len(),
            reason = terminal_reason.unwrap_or_else(|| "unknown execution boundary".to_string()),
            rejected = people_rejected_total,
            held = people_held_total,
            stopped = people_stopped_total,
            url = self.crm_url(),
        )
    }

    /// Refresh doctrine framing for leads already on file (no Apollo).
    async fn do_refresh_context(
        &self,
        thesis: &str,
        lead_ids: &[String],
        show_result: bool,
        force_refresh: bool,
    ) -> usize {
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
        // The spinner is transient on a TTY; `show_result` controls only the
        // durable transcript block printed after it finishes.
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
            force_refresh,
        )
        .await;
        drop(work);
        match result {
            Ok(0) => {
                if show_result {
                    ui::activity(
                        "Account framing reused",
                        "recent source-backed framing retained · 0 refresh model calls",
                    );
                }
                0
            }
            Ok(n) => {
                if show_result {
                    ui::activity(
                        "Refreshed account framing",
                        format!(
                            "{n} account(s) · official site re-read + evidence reassessed · no Apollo"
                        ),
                    );
                }
                n
            }
            Err(e) => {
                if show_result {
                    ui::activity("Framing refresh skipped", format!("{e:#}"));
                }
                0
            }
        }
    }

    /// Target one existing account for a fresh official-site read and current
    /// play assessment without starting another Apollo search wave.
    async fn research_account(&self, query: &str, thesis: &str) -> String {
        let query = query.trim();
        if query.is_empty() {
            return "Name the existing account to research.".into();
        }
        let leads = match self.db.list_leads(Some(&self.brand)) {
            Ok(leads) => leads,
            Err(error) => return format!("Could not inspect CRM accounts: {error:#}"),
        };
        let mut matches = leads
            .iter()
            .filter(|lead| {
                lead.id.eq_ignore_ascii_case(query)
                    || lead.name.eq_ignore_ascii_case(query)
                    || lead.domain.eq_ignore_ascii_case(query)
            })
            .cloned()
            .collect::<Vec<_>>();
        if matches.is_empty() {
            let needle = query.to_ascii_lowercase();
            matches = leads
                .into_iter()
                .filter(|lead| {
                    lead.name.to_ascii_lowercase().contains(&needle)
                        || lead.domain.to_ascii_lowercase().contains(&needle)
                })
                .collect();
        }
        if matches.len() != 1 {
            let names = matches
                .iter()
                .map(|lead| lead.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return format!(
                "Account research needs one exact {} CRM match; found {}{}.",
                self.brand,
                matches.len(),
                if names.is_empty() {
                    String::new()
                } else {
                    format!(": {names}")
                }
            );
        }
        let lead = matches.remove(0);
        let refreshed = self
            .do_refresh_context(thesis, std::slice::from_ref(&lead.id), true, true)
            .await;
        let Some(play) = self.db.current_gtm_play(&self.brand).ok().flatten() else {
            return format!(
                "Refreshed {refreshed} account(s), but no active GTM play is configured."
            );
        };
        let Some(assessment) = self
            .db
            .account_play_assessment(&lead.id, &play.id)
            .ok()
            .flatten()
        else {
            return format!(
                "Refreshed {refreshed} account(s), but {} has no current assessment.",
                lead.name
            );
        };
        let gaps = if assessment.evidence_gaps.is_empty() {
            String::new()
        } else {
            format!(" Evidence gaps: {}.", assessment.evidence_gaps.join(" | "))
        };
        format!(
            "Researched {} against {} v{} with no Apollo spend: {} (score {}, {}/{} required signals).{}",
            lead.name,
            play.name,
            play.version,
            assessment.status,
            assessment.fit_score,
            assessment.matched_signal_keys.len(),
            play.minimum_signal_matches,
            gaps,
        )
    }

    fn approve_outreach(&self) -> String {
        let pb = match self.playbooks.get(&self.brand) {
            Ok(pb) => pb,
            Err(error) => return format!("Approval failed: {error:#}"),
        };
        match crate::outreach::approve_ready_touches(&self.db, pb, None) {
            Ok(approval) => {
                let plan = self.businesses.get(&self.brand).and_then(|profile| {
                    crate::calendar::rebalance_approved_sales(&self.db, profile, chrono::Utc::now())
                });
                let schedule = plan
                    .map(|plan| {
                        format!(
                            "{} email(s) across {} active day(s); {} new conversation(s) admitted",
                            plan.emails, plan.active_days, plan.admitted_people
                        )
                    })
                    .unwrap_or_else(|error| format!("calendar refresh failed: {error:#}"));
                ui::activity(
                    "Approved outreach",
                    format!(
                        "{} touch(es) · {} held · {} · {schedule}",
                        approval.touches_scheduled, approval.people_held, self.brand
                    ),
                );
                format!(
                    "Approved {} drafted email touch(es) for {}; held {} recipient(s) at the current GTM gate. {schedule}.",
                    approval.touches_scheduled, self.brand, approval.people_held,
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
- Finding/sourcing companies or people AND writing/drafting their outreach for the SAME brand is ONE run_full_motion step, never separate source_leads + plan_outreach steps. The contacts field is both mapping and drafting cardinality per company: if the operator asks for five people per company, draft up to five qualified, verified recipients per company.\n\
- Different actions for different brands in one message are fine; each step carries its own brand and its own fields.\n\
For a pure conversational answer (no action to run), leave `steps` empty and put the answer in `reply`.\n\n\
{brand_mode}\n\n\
Actions (each is a step's `action`):\n\
- run_campaign: hypothetical research-only campaign; no Apollo.\n\
- source_leads: ONLY finds and qualifies Apollo accounts+people, then stops — it writes NO emails. Use only when the request is purely to find companies/people and contains no request to write, draft, sequence, or perform outreach. set thesis/accounts/contacts (defaults 10/3).\n\
- research_account: re-read and reassess ONE existing CRM account against the current GTM play without Apollo. Put its exact name/domain/id in query and the research focus in thesis. Use this before targeted regeneration when a promising old account has stale, weak, or retired-play evidence.\n\
- run_full_motion: end-to-end motion for a brand (never sends). The requested account count is the outreach FULFILLMENT CONTRACT; contacts-per-account controls contact mapping, while cold copy opens with exactly one carefully ranked person per account. Persist through adaptive sourcing passes, contact shortfalls, weak hypotheses, and rejected copy; save misses as targeting corrections, retry rejected copy with its feedback, and replace an account that still lacks one current reviewed recipient sequence. Stop short only at a real provider/model-budget/search-exhaustion safety boundary and report filled/requested exactly. REUSE-FIRST: if the CRM already has enough accounts/people for that brand, it SKIPS Apollo, refreshes why those companies fit, and writes missing, rejected, or stale sequences. Current-policy reviewed drafts are preserved unless the operator explicitly asks to rewrite/redraft/refine/replace them. set thesis/accounts/contacts/touches (defaults 5/5/7). set force_new=true ONLY when they explicitly ask for new/fresh/more companies not already on file.\n\
- enrich_people: reveal/verify sourced emails; phone only when explicit.\n\
- plan_outreach: draft sequences for contacts ALREADY found (no account/people search). When the operator says 'I need X from person Y,' put Y's exact name/email/id in person and X in outcome. Do not reinterpret X as buyer-facing wording; the response planner will reduce it when it is not yet earned. A scoped request may reveal only those selected contacts whose email is still missing, because an email sequence must not silently shrink; skip that reveal when the operator says no Apollo/no enrichment/verified only. IMPORTANT SCOPE: 'first N people' with NO company/account count means N people TOTAL in CRM order: set limit=N and OMIT accounts/contacts. 'first N people in the first company' means accounts=1, contacts=N. 'N people for each of M companies' means accounts=M, contacts=N. contacts is always PER selected company, never a bare total. Preserve current-policy reviewed drafts on ordinary retries; safely replace unsent drafts only when the operator explicitly says rewrite/redraft/refine/replace. auto only when explicit.\n\
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
                "enum": ["run_campaign", "source_leads", "research_account", "run_full_motion", "enrich_people", "plan_outreach", "approve_outreach", "discover_opportunities", "list_opportunities", "resolve_opportunity_contacts", "plan_funding_outreach", "approve_funding_outreach", "prepare_application", "show_funnel", "show_calendar", "list_accounts", "show_learnings", "open_crm", "open_gtm", "search_knowledge"]
            },
            "brand": { "type": "string", "enum": brands, "description": "The brand this step concerns. Leave empty only for portfolio-wide reads (they span all brands)." },
            "thesis": { "type": "string", "description": "The workflow/market to target for sourcing/campaign steps." },
            "query": { "type": "string", "description": "For search_knowledge: the book topic. For research_account: the exact existing account name/domain/id." },
            "outcome": { "type": "string", "description": "For plan_outreach or run_full_motion: the exact next response or action the operator wants from the named person. Preserve the user's words; leave empty when none is stated." },
            "person": { "type": "string", "description": "For a targeted plan_outreach request: exact existing person id, email, or name stated by the operator." },
            "accounts": { "type": "integer", "description": "For plan_outreach, number of existing companies in current CRM order to scope. Set 1 for 'the first company'." },
            "contacts": { "type": "integer", "description": "For plan_outreach, number of visible-order people PER selected company. Use only when the operator names a company scope, such as '3 people in the first company' or '3 people for each company'. Omit for a bare 'first 3 people'." },
            "touches": { "type": "integer" },
            "limit": { "type": "integer", "description": "TOTAL contact cap for plan_outreach. Also set this for a bare 'first N people' when no company/account count is named. Do not combine with accounts+contacts unless the operator explicitly gives both a per-company scope and a total cap." },
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

/// Full-motion commands already contain all of the costly execution choices.
/// If the model router returns malformed structured output, recover this narrow
/// and explicit intent locally rather than abandoning the entire outbound run.
/// Other commands still surface the router error instead of being guessed.
fn deterministic_full_motion_fallback(
    input: &str,
    brands: &[&str],
    fallback_brand: &str,
) -> Option<Decision> {
    let normalized = input.to_ascii_lowercase();
    if !normalized.contains("full motion") {
        return None;
    }

    let tokens = normalized
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let mentions = tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| count_token(token).map(|count| (index, count)))
        .collect::<Vec<_>>();
    let person_nouns = [
        "person",
        "people",
        "contact",
        "contacts",
        "recipient",
        "recipients",
    ];
    let touch_nouns = ["touch", "touches", "stage", "stages"];
    let account_nouns = ["account", "accounts", "company", "companies"];

    let touches = count_near_noun(&tokens, &mentions, &touch_nouns, &[]);
    let contacts = count_near_noun(&tokens, &mentions, &person_nouns, &[]);
    // "2 people per company" describes contact coverage, not two accounts.
    let mut account_blockers = person_nouns.to_vec();
    account_blockers.extend(["each", "per"]);
    let explicit_accounts = count_near_noun(&tokens, &mentions, &account_nouns, &account_blockers);

    let used = [touches, contacts, explicit_accounts]
        .into_iter()
        .flatten()
        .map(|(index, _)| index)
        .collect::<std::collections::HashSet<_>>();
    let accounts = explicit_accounts.or_else(|| {
        mentions
            .iter()
            .copied()
            .find(|(index, _)| !used.contains(index))
    });
    let brand = brands
        .iter()
        .copied()
        .find(|brand| normalized.contains(&brand.to_ascii_lowercase()))
        .unwrap_or(fallback_brand);
    let force_new = tokens.iter().any(|token| matches!(*token, "new" | "fresh"));

    Some(Decision {
        reply: String::new(),
        steps: vec![Step {
            action: "run_full_motion".into(),
            brand: brand.to_string(),
            thesis: input.to_string(),
            accounts: accounts.map(|(_, count)| count.max(1) as u64),
            contacts: contacts.map(|(_, count)| count.max(1) as u64),
            touches: touches.map(|(_, count)| count.max(1) as u64),
            force_new,
            ..Default::default()
        }],
    })
}

/// Find a number followed within three tokens by one of the requested nouns.
/// Blockers prevent a later noun from stealing a count from an earlier scope,
/// as in "2 mapped people per company".
fn count_near_noun(
    tokens: &[&str],
    mentions: &[(usize, usize)],
    nouns: &[&str],
    blockers: &[&str],
) -> Option<(usize, usize)> {
    mentions.iter().copied().find(|(index, _)| {
        let following = tokens.iter().skip(index + 1).take(3).copied();
        let mut blocked = false;
        for token in following {
            if nouns.contains(&token) {
                return !blocked;
            }
            if blockers.contains(&token) || count_token(token).is_some() {
                blocked = true;
            }
        }
        false
    })
}

/// The model router occasionally represents one end-to-end request as
/// `source_leads` followed by `plan_outreach`. That bypasses the fulfillment
/// loop because both standalone actions are intentionally bounded one-shots.
/// Collapse the pair deterministically whenever it targets the same brand.
fn coalesce_full_motion_steps(steps: &mut Vec<Step>, fallback_brand: &str) {
    let mut consumed = std::collections::HashSet::<usize>::new();
    let mut normalized = Vec::with_capacity(steps.len());

    for index in 0..steps.len() {
        if consumed.contains(&index) {
            continue;
        }
        let action = steps[index].action.as_str();
        if !matches!(action, "source_leads" | "plan_outreach") {
            normalized.push(steps[index].clone());
            continue;
        }
        let counterpart = if action == "source_leads" {
            "plan_outreach"
        } else {
            "source_leads"
        };
        let brand = effective_step_brand(&steps[index], fallback_brand);
        let pair = steps.iter().enumerate().find(|(candidate, step)| {
            *candidate != index
                && !consumed.contains(candidate)
                && step.action == counterpart
                && effective_step_brand(step, fallback_brand) == brand
        });
        let Some((pair_index, pair_step)) = pair else {
            normalized.push(steps[index].clone());
            continue;
        };

        let (source, plan) = if action == "source_leads" {
            (&steps[index], pair_step)
        } else {
            (pair_step, &steps[index])
        };
        let mut full = source.clone();
        full.action = "run_full_motion".into();
        if full.brand.trim().is_empty() {
            full.brand = plan.brand.clone();
        }
        if full.thesis.trim().is_empty() {
            full.thesis = plan.thesis.clone();
        }
        if full.outcome.trim().is_empty() {
            full.outcome = plan.outcome.clone();
        }
        full.accounts = source.accounts.or(plan.accounts);
        // Contacts are requested recipient sequences per company, not merely a
        // hidden mapping pool behind a silently reduced recipient count.
        full.contacts = source.contacts.or(plan.contacts);
        full.touches = plan.touches.or(source.touches);
        full.auto = plan.auto;
        full.force_new = true;
        normalized.push(full);
        consumed.insert(index);
        consumed.insert(pair_index);
    }

    *steps = normalized;
}

fn requested_outreach_scope(steps: &[Step]) -> (usize, usize) {
    let mut accounts = 0usize;
    let mut recipients = 0usize;
    for step in steps {
        match step.action.as_str() {
            "run_full_motion" => {
                let step_accounts = step.accounts.unwrap_or(5).max(1) as usize;
                let contacts = step.contacts.unwrap_or(5).max(1) as usize;
                accounts = accounts.saturating_add(step_accounts);
                recipients = recipients.saturating_add(step_accounts.saturating_mul(contacts));
            }
            "plan_outreach" => {
                let step_accounts = step.accounts.unwrap_or(1).max(1) as usize;
                let step_recipients = if !step.person.trim().is_empty() {
                    1
                } else if let Some(limit) = step.limit {
                    limit.max(1) as usize
                } else {
                    step_accounts.saturating_mul(step.contacts.unwrap_or(1).max(1) as usize)
                };
                accounts = accounts.saturating_add(step_accounts);
                recipients = recipients.saturating_add(step_recipients);
            }
            _ => {}
        }
    }
    (accounts, recipients)
}

fn effective_step_brand<'a>(step: &'a Step, fallback_brand: &'a str) -> &'a str {
    if step.brand.trim().is_empty() {
        fallback_brand
    } else {
        step.brand.trim()
    }
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
        coalesce_full_motion_steps, decision_schema, deterministic_full_motion_fallback,
        forbids_contact_enrichment, requested_outreach_scope, requests_copy_replacement,
        routed_total_limit, select_plan_scope, unqualified_people_total, Step,
    };
    use crate::db::{Db, Lead, Person};

    #[test]
    fn requested_contact_cardinality_sizes_the_execution_envelope() {
        let steps = vec![Step {
            action: "run_full_motion".into(),
            accounts: Some(5),
            contacts: Some(5),
            touches: Some(7),
            ..Default::default()
        }];
        assert_eq!(requested_outreach_scope(&steps), (5, 25));
    }

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
        assert_eq!(step["properties"]["person"]["type"], "string");
        assert_eq!(step["properties"]["outcome"]["type"], "string");
        // `reply` is only for the no-steps conversational path — not a step action.
        let step_actions = step["properties"]["action"]["enum"]
            .as_array()
            .expect("step actions");
        assert!(step_actions.iter().all(|action| action != "reply"));
    }

    #[test]
    fn same_brand_source_then_draft_is_forced_through_full_motion() {
        let mut steps = vec![
            Step {
                action: "source_leads".into(),
                brand: "outagehub".into(),
                thesis: "Canadian distributed operations".into(),
                accounts: Some(3),
                contacts: Some(2),
                ..Default::default()
            },
            Step {
                action: "plan_outreach".into(),
                brand: "outagehub".into(),
                outcome: "get the NOC manager to describe the current triage step".into(),
                accounts: Some(3),
                contacts: Some(1),
                touches: Some(4),
                ..Default::default()
            },
        ];

        coalesce_full_motion_steps(&mut steps, "gnk");

        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].action, "run_full_motion");
        assert_eq!(steps[0].brand, "outagehub");
        assert_eq!(steps[0].accounts, Some(3));
        assert_eq!(steps[0].contacts, Some(2));
        assert_eq!(steps[0].touches, Some(4));
        assert_eq!(
            steps[0].outcome,
            "get the NOC manager to describe the current triage step"
        );
        assert!(steps[0].force_new);
    }

    #[test]
    fn explicit_continue_full_motion_survives_a_broken_model_router() {
        let decision = deterministic_full_motion_fallback(
            "continue the unfinished OutageHub full motion: fill 3 reviewed 4-touch sequences, keep 2 mapped people per company, drafts only",
            &["gnk", "outagehub", "wapahki"],
            "gnk",
        )
        .expect("explicit full motion has a deterministic route");

        assert_eq!(decision.steps.len(), 1);
        let step = &decision.steps[0];
        assert_eq!(step.action, "run_full_motion");
        assert_eq!(step.brand, "outagehub");
        assert_eq!(step.accounts, Some(3));
        assert_eq!(step.contacts, Some(2));
        assert_eq!(step.touches, Some(4));
        assert!(!step.force_new);
    }

    #[test]
    fn deterministic_router_fallback_is_limited_to_explicit_full_motion() {
        assert!(deterministic_full_motion_fallback(
            "draft some OutageHub outreach",
            &["gnk", "outagehub", "wapahki"],
            "gnk",
        )
        .is_none());
    }

    #[test]
    fn different_brand_source_and_draft_remain_independent_steps() {
        let mut steps = vec![
            Step {
                action: "source_leads".into(),
                brand: "gnk".into(),
                ..Default::default()
            },
            Step {
                action: "plan_outreach".into(),
                brand: "outagehub".into(),
                ..Default::default()
            },
        ];

        coalesce_full_motion_steps(&mut steps, "wapahki");

        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].action, "source_leads");
        assert_eq!(steps[1].action, "plan_outreach");
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
        assert!(!requests_copy_replacement(
            "write the first 2 people for the first 5 companies"
        ));
        assert!(!requests_copy_replacement(
            "retry the rejected people and keep the good drafts"
        ));
        assert!(requests_copy_replacement(
            "rewrite all five sequences from scratch"
        ));
        assert!(requests_copy_replacement(
            "refine the existing outreach copy"
        ));
    }

    #[test]
    fn bare_first_people_count_is_total_not_per_default_account() {
        assert_eq!(
            unqualified_people_total("give me the 7 touchpoints for the first 5 people in wahpaki"),
            Some(5)
        );
        assert_eq!(
            unqualified_people_total("write the first five contacts for outagehub"),
            Some(5)
        );
        assert_eq!(
            unqualified_people_total("write the first 2 people for the first 5 companies"),
            None
        );
        assert_eq!(
            unqualified_people_total("write 2 people per company for the first 5 accounts"),
            None
        );
        assert_eq!(
            unqualified_people_total("write the first 3 people in the first company"),
            None
        );
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

    #[test]
    fn scoped_plan_does_not_displace_a_deliverable_person_with_an_unverified_primary() {
        let db = Db::open(":memory:").expect("open db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "deliverable-scope-org".into(),
                name: "Deliverable Scope".into(),
                ..Default::default()
            })
            .expect("insert lead");
        let unverified_primary = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "unverified-primary".into(),
                name: "Unverified Primary".into(),
                primary: true,
                status: "new".into(),
                email_status: "unknown".into(),
                ..Default::default()
            })
            .expect("insert primary");
        let verified = db
            .upsert_person(&Person {
                lead_id,
                brand: "gnk".into(),
                apollo_person_id: "verified-secondary".into(),
                name: "Verified Secondary".into(),
                status: "verified".into(),
                email_status: "verified".into(),
                email: "verified@example.com".into(),
                ..Default::default()
            })
            .expect("insert verified");

        let scope = select_plan_scope(&db, "gnk", Some(1), Some(1), None)
            .expect("select deliverable scope");

        assert_eq!(scope.selected_ids.len(), 1);
        assert!(scope.selected_ids.contains(&verified));
        assert!(!scope.selected_ids.contains(&unverified_primary));
        assert!(scope.pending_enrichment_ids.is_empty());
    }
}
