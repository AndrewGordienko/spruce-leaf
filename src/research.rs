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
const MAX_CORPUS_CHARS: usize = 12_000;

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
    pub hiring_signals: Vec<String>,
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
            && self.hiring_signals.is_empty()
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
        for sig in &self.hiring_signals {
            s.push_str(&format!(
                "- official hiring signal (current when fetched; supports only the stated investment or responsibility, never pain, urgency, budget, or recipient ownership): {sig}\n"
            ));
        }
        for source in &self.sources {
            s.push_str(&format!("- source URL: {source}\n"));
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

    // Fetch the homepage and optional job pages in one concurrent wave. If the
    // homepage is thin, a second wave tries one about/products-style page.
    let mut corpus = String::new();
    let mut sources: Vec<String> = Vec::new();
    let homepage = format!("https://{domain}");
    let careers_url = format!("https://{domain}/careers");
    let jobs_url = format!("https://{domain}/jobs");
    let job_reads = async {
        if job_signals_enabled() {
            futures::future::join(
                read_page(research, &careers_url),
                read_page(research, &jobs_url),
            )
            .await
        } else {
            (None, None)
        }
    };
    let (homepage_text, (careers, jobs)) =
        futures::future::join(read_page(research, &homepage), job_reads).await;
    let relevant_detail_urls = homepage_text
        .as_deref()
        .map(|text| relevant_internal_links(&homepage, text, &pb.key, 2))
        .unwrap_or_default();
    if let Some(text) = homepage_text {
        corpus.push_str(&text);
        sources.push(homepage.clone());
    }
    // A full homepage can still be commercially useless. Follow the two most
    // motion-relevant first-party links it exposes (for example Claims and
    // Technology for GnK) instead of only trying /about when the page is short.
    // This gives account refresh a useful no-search-key deepening path.
    let detail_reads = futures::future::join_all(
        relevant_detail_urls
            .iter()
            .map(|url| read_page(research, url)),
    )
    .await;
    for (url, text) in relevant_detail_urls.into_iter().zip(detail_reads) {
        if let Some(text) = text {
            corpus.push_str("\n\n");
            corpus.push_str(&text);
            sources.push(url);
        }
    }
    if corpus.len() < 1_200 {
        // Fetch a couple of likely detail pages concurrently and keep the first
        // that returns text. Walking four paths one-at-a-time meant a slow or dead
        // site burned one timeout per path in series — the dominant tail latency
        // of a sourcing run. Two concurrent fetches bound that to a single wait.
        let about_url = format!("https://{domain}/about");
        let products_url = format!("https://{domain}/products");
        let (about, products) = futures::future::join(
            read_page(research, &about_url),
            read_page(research, &products_url),
        )
        .await;
        if let Some((url, text)) = [(about_url, about), (products_url, products)]
            .into_iter()
            .find_map(|(url, text)| text.map(|body| (url, body)))
        {
            corpus.push_str("\n\n");
            corpus.push_str(&text);
            sources.push(url);
        }
    }
    // A first-party job page can reveal a capability the company is investing in
    // or a responsibility it explicitly assigns. It is not evidence that a
    // workflow hurts, is urgent, has budget, or belongs to our recipient. Keep
    // this best-effort and concurrent with the homepage, so it does not add a
    // serial website-read wait.
    if let Some((url, text)) = [(careers_url, careers), (jobs_url, jobs)]
        .into_iter()
        .find_map(|(url, text)| text.map(|body| (url, body)))
    {
        corpus.push_str("\n\nFIRST-PARTY CAREERS/JOBS PAGE (hiring evidence only):\n");
        corpus.push_str(&text);
        sources.push(url);
    }
    if corpus.trim().is_empty() {
        return None;
    }
    let corpus = limit_chars(&corpus, MAX_CORPUS_CHARS);

    let system = "You are a research analyst. You read a real company's own website text and \
        extract only what it supports — never invent customers, metrics, dollar figures, or \
        capabilities. You are researching for a specific outreach motion; surface the evidence \
        relevant to THAT motion and say plainly when the pages don't support a claim. Careers or \
        job text may support only the investment, system, workflow, or responsibility it explicitly \
        names. It never proves pain, urgency, budget, buying intent, or that an outreach recipient \
        owns the workflow.";
    let user = format!(
        "COMPANY: {name} ({domain})\n\n\
         THE MOTION we are researching for (find evidence relevant to THIS, and a specific gap \
         this company plausibly has that fits it):\n{motion}\n\n\
         WEBSITE TEXT (the ONLY thing you may treat as fact):\n{corpus}\n\n\
         Return a compact brief: what_they_do (one or two sentences); observed_facts (each \
         traceable to the text — operations, scale, locations/sites, products/formats, systems, \
         manual work, customers named ON their site); signals specifically relevant to the motion; \
         hiring_signals (only current investments, systems, workflows, or responsibilities explicitly \
         named on the supplied first-party careers/jobs page; never inferred pain or buying intent); \
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
        .structured_fast::<CompanyBrief>("source.website_research", system, &user, brief_schema())
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

/// True unless `SPRUCE_JOB_SIGNALS` is explicitly false. Kept separate from the
/// main research switch so operators can trade the extra read for lower latency.
pub fn job_signals_enabled() -> bool {
    match std::env::var("SPRUCE_JOB_SIGNALS") {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            !(v == "0" || v == "false" || v == "off" || v == "no")
        }
        Err(_) => true,
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

fn relevant_internal_links(base: &str, text: &str, brand: &str, limit: usize) -> Vec<String> {
    let Ok(base_url) = reqwest::Url::parse(base) else {
        return Vec::new();
    };
    let Some(base_host) = base_url.host_str() else {
        return Vec::new();
    };
    let brand_terms: &[&str] = match brand {
        "gnk" => &[
            "claims",
            "operations",
            "technology",
            "innovation",
            "underwriting",
            "workflow",
            "portal",
        ],
        "wapahki" => &[
            "manufacturing",
            "capabilities",
            "packaging",
            "automation",
            "production",
            "facility",
        ],
        "outagehub" => &[
            "operations",
            "assets",
            "reliability",
            "outage",
            "emergency",
            "technology",
        ],
        _ => &["operations", "technology", "services", "solutions"],
    };
    let mut candidates = Vec::<(usize, String)>::new();
    let mut rest = text;
    while let Some(close_anchor) = rest.find("](") {
        let before = &rest[..close_anchor];
        let anchor = before
            .rfind('[')
            .map(|start| &before[start + 1..])
            .unwrap_or_default();
        let after = &rest[close_anchor + 2..];
        let Some(close_url) = after.find(')') else {
            break;
        };
        let raw_url = after[..close_url].split_whitespace().next().unwrap_or("");
        rest = &after[close_url + 1..];
        if raw_url.is_empty() || raw_url.starts_with('#') || raw_url.starts_with("mailto:") {
            continue;
        }
        let Ok(url) = base_url.join(raw_url) else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str() != Some(base_host)
            || url.as_str().trim_end_matches('/') == base.trim_end_matches('/')
        {
            continue;
        }
        let haystack = format!("{} {}", anchor, url.path()).to_ascii_lowercase();
        if [
            "privacy", "cookie", "terms", "login", "contact", "careers", "jobs",
        ]
        .iter()
        .any(|term| haystack.contains(term))
        {
            continue;
        }
        let score = brand_terms
            .iter()
            .enumerate()
            .filter(|(_, term)| haystack.contains(**term))
            .map(|(index, _)| brand_terms.len() - index)
            .sum::<usize>();
        if score > 0 {
            let mut canonical = url;
            canonical.set_fragment(None);
            candidates.push((score, canonical.to_string()));
        }
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    candidates.dedup_by(|left, right| left.1 == right.1);
    candidates
        .into_iter()
        .take(limit.max(1))
        .map(|(_, url)| url)
        .collect()
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
            "hiring_signals": str_array("Only investments, systems, workflows, or responsibilities explicitly named on the supplied first-party careers/jobs page. Never inferred pain, urgency, budget, buying intent, or recipient ownership."),
            "problem_hypothesis": { "type": "string" },
            "why": { "type": "string" }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{relevant_internal_links, CompanyBrief};

    #[test]
    fn follows_motion_relevant_first_party_links() {
        let links = relevant_internal_links(
            "https://example.com",
            "[About](/about) [Claims](/claims/) [Technology](https://example.com/technology) [Privacy](/privacy)",
            "gnk",
            2,
        );
        assert_eq!(links.len(), 2);
        assert!(links.iter().any(|link| link.contains("/claims/")));
        assert!(links.iter().any(|link| link.contains("/technology")));
        assert!(links
            .iter()
            .all(|link| link.starts_with("https://example.com")));
    }

    #[test]
    fn hiring_signal_is_labeled_with_its_evidence_boundary() {
        let brief = CompanyBrief {
            hiring_signals: vec![
                "Hiring a controls engineer to deploy machine-vision cells.".into()
            ],
            sources: vec!["https://example.com/careers".into()],
            ..Default::default()
        };

        let facts = brief.as_facts_block();
        assert!(facts.contains("official hiring signal"));
        assert!(facts.contains("never pain, urgency, budget, or recipient ownership"));
        assert!(facts.contains("https://example.com/careers"));
        assert!(!brief.is_empty());
    }
}
