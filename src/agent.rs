//! The agent loop, on the `claude` CLI underbase.
//!
//! The CLI doesn't hand back API-style `tool_use` blocks, so instead of a raw
//! tool loop we use a *structured router*: each user line is sent to Claude with
//! a schema that makes it choose one action — run a campaign, list the CRM, open
//! the dashboard, switch brand, or just reply — which we then execute in Rust. A
//! short rolling transcript is included for conversational continuity.

use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::crm::SharedStore;
use crate::engine::Claude;
use crate::knowledge::SharedLibrary;
use crate::pipeline;
use crate::playbook::Playbooks;
use crate::ui;

/// How many past turns to feed back for continuity.
const HISTORY_TURNS: usize = 6;

pub struct Agent {
    client: Claude,
    store: SharedStore,
    library: SharedLibrary,
    playbooks: Arc<Playbooks>,
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
}

impl Agent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Claude,
        store: SharedStore,
        library: SharedLibrary,
        playbooks: Arc<Playbooks>,
        brand: String,
        critique: bool,
        port: u16,
        concurrency: usize,
    ) -> Self {
        Self { client, store, library, playbooks, brand, critique, port, concurrency, history: Vec::new() }
    }

    pub fn crm_url(&self) -> String {
        format!("http://localhost:{}", self.port)
    }

    pub fn brand(&self) -> &str {
        &self.brand
    }

    pub fn brand_keys(&self) -> Vec<&str> {
        self.playbooks.keys()
    }

    /// Switch the active brand if `key` is valid; returns whether it changed.
    pub fn set_brand(&mut self, key: &str) -> bool {
        if self.playbooks.get(key).is_ok() {
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
                self.run_campaign(&thesis, accounts, contacts, touches).await
            }
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
                format!("Opened the CRM dashboard at {}", self.crm_url())
            }
            // Plain conversational reply: the model already streamed it, so
            // don't reprint — but keep the text for conversational memory.
            _ if streamed => String::new(),
            _ => decision.reply.clone(),
        };

        // Remember the substantive text even when we suppressed the reprint.
        let memo = if reply.is_empty() { decision.reply.clone() } else { reply.clone() };
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

    async fn list_accounts(&self) -> String {
        let store = self.store.read().await;
        if store.data.accounts.is_empty() {
            return "The CRM is empty \u{2014} no campaigns run yet.".to_string();
        }
        let mut out = String::from("In the CRM:\n");
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

    /// Read-only lookup into the ingested book library.
    async fn search_knowledge(&self, query: &str) -> String {
        let lib = self.library.read().await;
        if lib.is_empty() {
            return "The book library is empty. Ingest books first: \
                    `spruce-leaf ingest <path>` (.txt/.md/.pdf)."
                .to_string();
        }
        let listing = lib.retrieve(query, 8, 3).human_listing();
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
        p.push_str(&format!(
            "Active brand: {}. The CRM currently holds {n} accounts.\n\n",
            self.brand
        ));
        p.push_str(&format!("User: {input}"));
        p
    }

    fn system(&self) -> String {
        format!(
            "You are spruce-leaf, an interactive sales-research agent \u{2014} a \"Codex for \
sales\" running in the user's terminal, backed by Claude. You help the user build \
hypothesis-led outreach campaigns and manage a local CRM. For each user message you choose \
exactly one action and return it as structured JSON:\n\
- run_campaign: the user wants to find accounts with an expensive workflow, the people \
positioned to see it, and/or outreach sequences. Set `thesis` to a crisp distillation of the \
workflow/market to target. Map the user's words to counts: companies\u{2192}`accounts`, \
people\u{2192}`contacts`, stages/touches\u{2192}`touches` (defaults 5/5/7). If they name a \
brand ({brands}), set `brand`. Put a short natural lead-in in `reply`; the system appends the \
real results.\n\
- list_accounts: the user wants to know what's already in the CRM.\n\
- open_crm: the user wants to open the CRM dashboard.\n\
- search_knowledge: the user asks what the ingested books say about a topic (cold email, \
pricing, discovery, objections). Put the topic in `query`.\n\
- reply: anything else \u{2014} answer conversationally in `reply`.\n\
Active brand is {active}. Be concise and concrete. Accounts, people, and every claim the tool \
produces are hypotheses to verify before real outreach.",
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
        "required": ["action", "reply"],
        "properties": {
            "action": {
                "type": "string",
                "enum": ["run_campaign", "list_accounts", "open_crm", "search_knowledge", "reply"]
            },
            "reply": { "type": "string", "description": "Message to show the user." },
            "thesis": { "type": "string", "description": "For run_campaign: the workflow/market to target." },
            "query": { "type": "string", "description": "For search_knowledge: the topic to look up in the ingested books." },
            "brand": { "type": "string", "enum": brands, "description": "Optional brand to switch to." },
            "accounts": { "type": "integer" },
            "contacts": { "type": "integer" },
            "touches": { "type": "integer" }
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
