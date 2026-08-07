//! Per-company research: read a company's own website and distill a brand-aware
//! brief, so qualification and outreach are grounded in what the company actually
//! does — not just Apollo's one-line description.
//!
//! This is the difference between "Acme is a 3PL in Ontario" (Apollo) and "Acme
//! ships to Walmart/Costco and runs three DCs, so retailer deductions plausibly
//! land on a small analyst team" (the hypothesis a first touch can open with).
//!
//! It is deliberately best-effort: no domain, an unreachable site, empty pages,
//! or a model error all fall back to Apollo-only sourcing with no regression.
//! Disable without a recompile via `SPRUCE_RESEARCH=0`.

use serde::Deserialize;
use serde_json::{json, Value};

use crate::apollo::ApolloOrg;
use crate::engine::Engine;
use crate::opportunity::ResearchClient;
use crate::playbook::Playbook;

const PER_PAGE_CHARS: usize = 6_000;
const MAX_CORPUS_CHARS: usize = 9_000;

/// A compact, grounded understanding of one company, distilled from its own site.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CompanyBrief {
    #[serde(default)]
    pub what_they_do: String,
    #[serde(default)]
    pub observed_facts: Vec<String>,
    #[serde(default)]
    pub signals: Vec<String>,
    #[serde(default)]
    pub problem_hypothesis: String,
    #[serde(default)]
    pub why: String,
    #[serde(default)]
    pub sources: Vec<String>,
}

impl CompanyBrief {
    /// True when there's nothing worth injecting into qualification.
    pub fn is_empty(&self) -> bool {
        self.what_they_do.trim().is_empty()
            && self.observed_facts.is_empty()
            && self.signals.is_empty()
            && self.problem_hypothesis.trim().is_empty()
    }

    /// Render as a labeled facts block for the qualify prompt. Website-derived
    /// facts are grounded (the model read the pages), so they may be treated as
    /// observed facts — but are kept clearly separate from Apollo's payload.
    pub fn as_facts_block(&self) -> String {
        if self.is_empty() {
            return String::new();
        }
        let mut s = String::from(
            "WEBSITE RESEARCH (read from the company's own pages — treat as observed facts, \
             traceable to their site):\n",
        );
        if !self.what_they_do.trim().is_empty() {
            s.push_str(&format!("- what they do: {}\n", self.what_they_do.trim()));
        }
        for f in &self.observed_facts {
            s.push_str(&format!("- fact: {f}\n"));
        }
        for sig in &self.signals {
            s.push_str(&format!("- signal: {sig}\n"));
        }
        if !self.problem_hypothesis.trim().is_empty() {
            s.push_str(&format!(
                "- possible fit to our motion: {}\n",
                self.problem_hypothesis.trim()
            ));
        }
        if !self.why.trim().is_empty() {
            s.push_str(&format!("- why (the crux): {}\n", self.why.trim()));
        }
        s
    }
}

/// True unless `SPRUCE_RESEARCH` is explicitly set to a falsey value.
pub fn enabled() -> bool {
    match std::env::var("SPRUCE_RESEARCH") {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            !(v == "0" || v == "false" || v == "off" || v == "no")
        }
        Err(_) => true,
    }
}

/// Research one company from its website, distilled through the brand's motion so
/// the signals surfaced are the ones the funnel actually needs. Returns `None` on
/// any failure (no domain, unreachable site, empty pages, model error) so the
/// caller can proceed on Apollo data alone.
pub async fn research_company(
    client: &Engine,
    research: &ResearchClient,
    pb: &Playbook,
    org: &ApolloOrg,
) -> Option<CompanyBrief> {
    let domain = org.domain();
    if domain.trim().is_empty() {
        return None;
    }

    // Fetch the homepage; if it's thin, try one about/products-style page. Cap at
    // two fetches per company to bound latency and Jina usage across a 50-org run.
    let mut corpus = String::new();
    let mut sources: Vec<String> = Vec::new();
    let homepage = format!("https://{domain}");
    if let Some(text) = read_page(research, &homepage).await {
        corpus.push_str(&text);
        sources.push(homepage);
    }
    if corpus.len() < 1_200 {
        for path in ["about", "products", "solutions", "services"] {
            let url = format!("https://{domain}/{path}");
            if let Some(text) = read_page(research, &url).await {
                corpus.push_str("\n\n");
                corpus.push_str(&text);
                sources.push(url);
                break;
            }
        }
    }
    if corpus.trim().is_empty() {
        return None;
    }
    let corpus = limit_chars(&corpus, MAX_CORPUS_CHARS);

    let system = "You are a research analyst. You read a real company's own website text and \
        extract only what it supports — never invent customers, metrics, dollar figures, or \
        capabilities. You are researching for a specific outreach motion; surface the evidence \
        relevant to THAT motion and say plainly when the pages don't support a claim.";
    let user = format!(
        "COMPANY: {name} ({domain})\n\n\
         THE MOTION we are researching for (find evidence relevant to THIS, and a specific gap \
         this company plausibly has that fits it):\n{motion}\n\n\
         WEBSITE TEXT (the ONLY thing you may treat as fact):\n{corpus}\n\n\
         Return a compact brief: what_they_do (one or two sentences); observed_facts (each \
         traceable to the text — operations, scale, locations/sites, products/formats, systems, \
         manual work, customers named ON their site); signals specifically relevant to the motion; \
         one problem_hypothesis this company plausibly has that fits the motion; and the 'why' — \
         the crux (for a workflow: why it is still manual / the missing layer; for automation: why \
         conventional automation is uneconomic; for outage data: why the decision is hard). If the \
         pages are too thin, return only what is supported and leave the rest empty.",
        name = org.name,
        domain = domain,
        motion = pb.motion,
        corpus = corpus,
    );

    match client
        .structured::<CompanyBrief>(system, &user, brief_schema())
        .await
    {
        Ok(mut brief) => {
            brief.sources = sources;
            if brief.is_empty() {
                None
            } else {
                Some(brief)
            }
        }
        Err(_) => None,
    }
}

async fn read_page(research: &ResearchClient, url: &str) -> Option<String> {
    match research.read(url).await {
        Ok(text) if !text.trim().is_empty() => Some(limit_chars(text.trim(), PER_PAGE_CHARS)),
        _ => None,
    }
}

fn limit_chars(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        value.chars().take(max).collect()
    }
}

fn str_array(desc: &str) -> Value {
    json!({ "type": "array", "items": { "type": "string" }, "description": desc })
}

fn brief_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["what_they_do"],
        "properties": {
            "what_they_do": { "type": "string" },
            "observed_facts": str_array("Facts traceable to the website text ONLY."),
            "signals": str_array("Signals relevant to the outreach motion."),
            "problem_hypothesis": { "type": "string" },
            "why": { "type": "string" }
        }
    })
}
