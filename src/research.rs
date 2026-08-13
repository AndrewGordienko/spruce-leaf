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

const PER_PAGE_CHARS: usize = 5_000;
const MAX_CORPUS_CHARS: usize = 18_000;

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
    /// Deterministically extracted from the exact fetched source page. This is
    /// not model output and is used only to bind a physical task to the same
    /// page's Ontario location.
    #[serde(skip)]
    pub source_locations: Vec<SourceLocation>,
}

#[derive(Debug, Clone, Default)]
pub struct SourceLocation {
    pub source_url: String,
    pub city: String,
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
            "WEBSITE/HIRING RESEARCH (read from company pages plus any explicitly labeled \
             company-attributed job mirror — treat only the page's direct statements as observed \
             facts):\n",
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
    research_focus: &str,
) -> Option<CompanyBrief> {
    let domain = org.domain();
    if domain.trim().is_empty() {
        return None;
    }
    let outage_evidence = if pb.key.eq_ignore_ascii_case("outagehub") {
        let report = std::env::var("SPRUCE_OUTAGE_MATCH_REPORT")
            .unwrap_or_else(|_| ".spruce/outage-location-matches.json".into());
        crate::outage_evidence::evidence_for_company(
            std::path::Path::new(&report),
            &org.name,
            &domain,
        )
    } else {
        Vec::new()
    };

    // Fetch the homepage and optional job pages in one concurrent wave. If the
    // homepage is thin, a second wave tries one about/products-style page.
    let mut corpus = String::new();
    let mut career_corpus = String::new();
    let mut job_corpus = String::new();
    let mut sources: Vec<String> = Vec::new();
    // Operator-curated URLs are discovery hints, never evidence by themselves.
    // Every URL is fetched live and must pass the same company/task boundary as
    // a search-discovered page. This keeps useful official pages available when
    // a public search engine is rate-limited or an ATS page is poorly indexed.
    let seeded_urls = research_seed_urls(&domain);
    if !seeded_urls.is_empty() {
        let seeded_reads = futures::future::join_all(
            seeded_urls
                .iter()
                // Curated job URLs can still put several thousand characters of
                // navigation ahead of the actual employer and duties. Read the
                // full discovery window, validate that full text, then reduce it
                // to the evidence-bearing lines below.
                .map(|seeded_url| read_page_for_links(research, seeded_url)),
        )
        .await;
        for (seeded_url, seeded_text) in seeded_urls.into_iter().zip(seeded_reads) {
            if let Some(seeded_text) = seeded_text {
                let host = reqwest::Url::parse(&seeded_url)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_string))
                    .unwrap_or_default();
                let seeded_job_path = reqwest::Url::parse(&seeded_url)
                    .is_ok_and(|url| url.path().contains("/job/") || url.path().contains("/jobs/"));
                let has_task_evidence = job_page_has_task_evidence(&seeded_text, &pb.key);
                let first_party =
                    same_company_host(&host, &domain) && (!seeded_job_path || has_task_evidence);
                let attributed_job = company_mentioned(&seeded_text, &org.name)
                    && has_task_evidence
                    && (trusted_ats_host(&host)
                        || trusted_job_mirror_host(&host)
                        || matches!(host.split('.').next(), Some("jobs" | "careers" | "join")));
                if first_party || attributed_job {
                    let target = if has_task_evidence {
                        &mut job_corpus
                    } else {
                        &mut corpus
                    };
                    let seeded_text = if has_task_evidence {
                        job_evidence_window(&seeded_text, &pb.key, &org.name, PER_PAGE_CHARS)
                    } else {
                        limit_chars(&seeded_text, PER_PAGE_CHARS)
                    };
                    append_source_section(
                        target,
                        "LIVE VERIFIED RESEARCH SEED PAGE",
                        &seeded_url,
                        &seeded_text,
                    );
                    sources.push(seeded_url);
                }
            }
        }
    }
    let homepage = format!("https://{domain}");
    let careers_url = format!("https://{domain}/careers");
    let jobs_url = format!("https://{domain}/jobs");
    let openings_url = format!("https://{domain}/job-openings");
    let job_reads = async {
        if job_signals_enabled() {
            futures::join!(
                read_page(research, &careers_url),
                read_page(research, &jobs_url),
                read_page(research, &openings_url),
            )
        } else {
            (None, None, None)
        }
    };
    let (homepage_text, (careers, jobs, openings)) =
        futures::future::join(read_page(research, &homepage), job_reads).await;
    let linked_career_urls = homepage_text
        .as_deref()
        .map(|text| {
            ranked_internal_links(
                &homepage,
                text,
                &[
                    "careers",
                    "career",
                    "jobs",
                    "job",
                    "openings",
                    "join-us",
                    "join us",
                    "work-with-us",
                ],
                2,
            )
        })
        .unwrap_or_default();
    let relevant_detail_urls = homepage_text
        .as_deref()
        .map(|text| relevant_internal_links(&homepage, text, &pb.key, 5))
        .unwrap_or_default();
    if let Some(text) = homepage_text {
        append_source_section(&mut corpus, "FIRST-PARTY COMPANY PAGE", &homepage, &text);
        sources.push(homepage.clone());
    }
    // A full homepage can still be commercially useless. Follow the most
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
            append_source_section(&mut corpus, "FIRST-PARTY COMPANY PAGE", &url, &text);
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
            append_source_section(&mut corpus, "FIRST-PARTY COMPANY PAGE", &url, &text);
            sources.push(url);
        }
    }
    // A first-party job page can reveal a capability the company is investing in
    // or a responsibility it explicitly assigns. It is not evidence that a
    // workflow hurts, is urgent, has budget, or belongs to our recipient. Keep
    // this best-effort and concurrent with the homepage, so it does not add a
    // serial website-read wait.
    let linked_career_reads = futures::future::join_all(
        linked_career_urls
            .iter()
            .map(|career_url| read_page(research, career_url)),
    )
    .await;
    let mut career_pages = [
        (careers_url, careers),
        (jobs_url, jobs),
        (openings_url, openings),
    ]
    .into_iter()
    .filter_map(|(url, text)| text.map(|body| (url, body)))
    .collect::<Vec<_>>();
    career_pages.extend(
        linked_career_urls
            .into_iter()
            .zip(linked_career_reads)
            .filter_map(|(url, text)| text.map(|body| (url, body))),
    );
    career_pages.sort_by(|left, right| left.0.cmp(&right.0));
    career_pages.dedup_by(|left, right| left.0 == right.0);
    let mut job_detail_urls = Vec::new();
    for (url, text) in career_pages {
        job_detail_urls.extend(relevant_job_links(&url, &text, &pb.key, 3));
        job_detail_urls.extend(ranked_internal_links(
            &url,
            &text,
            &["job", "career", "opening", "position"],
            3,
        ));
        append_source_section(
            &mut career_corpus,
            "FIRST-PARTY CAREERS/JOBS PAGE (hiring evidence only)",
            &url,
            &text,
        );
        sources.push(url);
    }
    let mut seen_job_urls = std::collections::HashSet::new();
    job_detail_urls.retain(|url| seen_job_urls.insert(url.clone()));
    job_detail_urls.retain(|url| {
        !sources
            .iter()
            .any(|source| source.trim_end_matches('/') == url.trim_end_matches('/'))
    });
    job_detail_urls.truncate(3);
    let mut nested_job_urls = Vec::new();
    if !job_detail_urls.is_empty() {
        let job_detail_reads = futures::future::join_all(
            job_detail_urls
                .iter()
                .map(|job_url| read_page(research, job_url)),
        )
        .await;
        for (job_url, job_text) in job_detail_urls.into_iter().zip(job_detail_reads) {
            if let Some(job_text) = job_text.filter(|text| {
                linked_job_page_credible(&job_url, &domain, text, &org.name, &pb.key)
            }) {
                nested_job_urls.extend(relevant_job_links(&job_url, &job_text, &pb.key, 2));
                append_source_section(
                    &mut job_corpus,
                    "FIRST-PARTY JOB DETAIL (hiring evidence only)",
                    &job_url,
                    &job_text,
                );
                sources.push(job_url);
            }
        }
    }
    nested_job_urls.retain(|url| {
        !sources
            .iter()
            .any(|source| source.trim_end_matches('/') == url.trim_end_matches('/'))
    });
    let mut seen_nested = std::collections::HashSet::new();
    nested_job_urls.retain(|url| seen_nested.insert(url.clone()));
    nested_job_urls.truncate(3);
    if !nested_job_urls.is_empty() {
        let nested_reads = futures::future::join_all(
            nested_job_urls
                .iter()
                .map(|job_url| read_page(research, job_url)),
        )
        .await;
        for (job_url, job_text) in nested_job_urls.into_iter().zip(nested_reads) {
            if let Some(job_text) = job_text.filter(|text| {
                linked_job_page_credible(&job_url, &domain, text, &org.name, &pb.key)
            }) {
                append_source_section(
                    &mut job_corpus,
                    "FIRST-PARTY JOB DETAIL (hiring evidence only)",
                    &job_url,
                    &job_text,
                );
                sources.push(job_url);
            }
        }
    }
    // Corporate careers pages often embed a third-party ATS without exposing
    // crawlable role links. Search both the corporate domain and the named
    // employer: the second lane finds official iCIMS/Workday/Greenhouse pages
    // whose hostname has no relationship to the company domain. Search
    // snippets never enter the evidence corpus.
    if job_signals_enabled() && matches!(pb.key.as_str(), "wapahki" | "gnk") {
        let role_queries = role_search_queries(&pb.key, research_focus, &org.name, &domain);
        let search_reads = futures::future::join_all(
            role_queries
                .iter()
                .map(|role_query| research.public_search(role_query)),
        )
        .await;
        let mut discovered = Vec::new();
        for (query_index, (role_query, result)) in role_queries.iter().zip(search_reads).enumerate()
        {
            match result {
                Ok(results) => {
                    let limit = if pb.key == "wapahki" && query_index == 0 {
                        10
                    } else {
                        3
                    };
                    let query_urls = search_result_urls(&results, &domain, limit);
                    if !crate::ui::fancy() && query_urls.is_empty() {
                        eprintln!(
                            "  · job search {} · no candidates from {}; {} chars; organic_marker={}; no_results={}",
                            org.name,
                            role_query,
                            results.len(),
                            results.contains("result__a"),
                            results.contains("No results found")
                        );
                    }
                    discovered.extend(query_urls);
                }
                Err(error) => {
                    if !crate::ui::fancy() {
                        eprintln!(
                            "  · job search {} failed · {}: {}",
                            org.name, role_query, error
                        );
                    }
                }
            }
        }
        let mut seen_discovered = std::collections::HashSet::new();
        discovered.retain(|url| seen_discovered.insert(url.clone()));
        discovered.truncate(14);
        if !crate::ui::fancy() && !discovered.is_empty() {
            eprintln!(
                "  · job search {} · candidates [{}]",
                org.name,
                discovered.join(", ")
            );
        }
        let reads = futures::future::join_all(
            discovered
                .iter()
                .map(|job_url| read_page_for_links(research, job_url)),
        )
        .await;
        let mut job_pages = Vec::new();
        let mut nested_discovered = Vec::new();
        for (job_url, job_text) in discovered.into_iter().zip(reads) {
            if let Some(job_text) = job_text {
                let host = reqwest::Url::parse(&job_url)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_string))
                    .unwrap_or_default();
                let company_named = company_mentioned(&job_text, &org.name);
                let official =
                    same_company_host(&host, &domain) || (trusted_ats_host(&host) && company_named);
                let attributed_mirror = trusted_job_mirror_host(&host)
                    && company_named
                    && job_page_has_task_evidence(&job_text, &pb.key);
                if official || attributed_mirror {
                    if job_location_priority(&pb.key, &job_url, &job_text) < 2 {
                        nested_discovered
                            .extend(relevant_job_links(&job_url, &job_text, &pb.key, 8));
                    }
                    job_pages.push((
                        job_location_priority(&pb.key, &job_url, &job_text),
                        job_url,
                        job_evidence_window(&job_text, &pb.key, &org.name, PER_PAGE_CHARS),
                        official,
                    ));
                }
            }
        }
        let mut seen_nested = std::collections::HashSet::new();
        nested_discovered.retain(|url| seen_nested.insert(url.clone()));
        nested_discovered.truncate(10);
        let nested_reads = futures::future::join_all(
            nested_discovered
                .iter()
                .map(|job_url| read_page(research, job_url)),
        )
        .await;
        for (job_url, job_text) in nested_discovered.into_iter().zip(nested_reads) {
            if let Some(job_text) = job_text.filter(|text| {
                linked_job_page_credible(&job_url, &domain, text, &org.name, &pb.key)
            }) {
                let host = reqwest::Url::parse(&job_url)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_string))
                    .unwrap_or_default();
                job_pages.push((
                    job_location_priority(&pb.key, &job_url, &job_text),
                    job_url,
                    job_text,
                    same_company_host(&host, &domain) || trusted_ats_host(&host),
                ));
            }
        }
        // Wapahki sells locally first. Never let a global employer's U.S. job
        // descriptions stand in for evidence about a Canadian plant. Rank
        // Ontario first, then the rest of Canada, and discard foreign roles.
        if pb.key == "wapahki" {
            job_pages.retain(|(priority, _, _, _)| *priority < 2);
        }
        job_pages.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        job_pages.truncate(5);
        for (_, job_url, job_text, official) in job_pages {
            let label = if official {
                "OFFICIAL SEARCH-DISCOVERED JOB PAGE"
            } else {
                "COMPANY-ATTRIBUTED JOB MIRROR (third-party; duties and posting details only)"
            };
            append_source_section(
                &mut job_corpus,
                &format!("{label} (hiring evidence only)"),
                &job_url,
                &job_text,
            );
            sources.push(job_url);
        }
    }
    // Put concrete role descriptions first so the fixed evidence window cannot
    // be consumed by homepage prose before it reaches task-level facts.
    corpus = if pb.key == "wapahki" {
        format!("{job_corpus}\n\n{career_corpus}\n\n{corpus}")
    } else {
        format!("{corpus}\n\n{job_corpus}\n\n{career_corpus}")
    };
    if corpus.trim().is_empty() {
        if outage_evidence.is_empty() {
            return None;
        }
        return Some(brief_with_outage_evidence(outage_evidence));
    }
    let corpus = limit_chars(&corpus, MAX_CORPUS_CHARS);
    let source_locations = source_locations_from_corpus(&corpus);

    let system = "You are a research analyst. You read a real company's website and hiring text and \
        extract only what it supports — never invent customers, metrics, dollar figures, or \
        capabilities. You are researching for a specific outreach motion; surface the evidence \
        relevant to THAT motion and say plainly when the pages don't support a claim. A current \
        company-attributed job mirror is secondary evidence and supports only the employer, title, \
        date, duties, shift, or requirements it directly states. Careers or job text may support \
        only the investment, system, workflow, or responsibility it explicitly names. It never \
        proves pain, urgency, budget, buying intent, or that an outreach recipient owns the workflow.";
    let user = format!(
        "COMPANY: {name} ({domain})\n\n\
         THE MOTION we are researching for (find evidence relevant to THIS, and a specific gap \
         this company plausibly has that fits it):\n{motion}\n\n\
         CURRENT BOUNDED SEGMENT / RESEARCH FOCUS:\n{research_focus}\n\n\
         WEBSITE TEXT (the ONLY thing you may treat as fact):\n{corpus}\n\n\
         Every item in observed_facts, signals, and hiring_signals MUST begin with the exact \
         source URL in square brackets, for example `[https://example.com/claims] fact`. Do not \
         combine facts from different pages into one item. Return a compact brief: what_they_do \
         (one or two sentences); observed_facts (each traceable to the text — operations, scale, \
         locations/sites, products/formats, systems, \
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
        research_focus = research_focus,
        corpus = corpus,
    );

    // This brief becomes the factual substrate for qualification and buyer-facing
    // personalization. Use the substantive lane: a cheap extraction error here
    // is amplified by every downstream stage.
    match client
        .structured_bulk::<CompanyBrief>("source.website_research", system, &user, brief_schema())
        .await
    {
        Ok(mut brief) => {
            brief.sources = sources;
            brief.source_locations = source_locations;
            add_outage_evidence(&mut brief, outage_evidence);
            if brief.is_empty() {
                None
            } else {
                Some(brief)
            }
        }
        Err(_) => None,
    }
}

fn source_locations_from_corpus(corpus: &str) -> Vec<SourceLocation> {
    let mut locations = Vec::new();
    for source in corpus.split("SOURCE URL: ").skip(1) {
        let Some((url, body)) = source.split_once('\n') else {
            continue;
        };
        let url = url.trim();
        if url.is_empty() {
            continue;
        }
        let body = body.split("SOURCE URL: ").next().unwrap_or(body);
        let Some(city) = crate::db::ontario_site_from_text(body) else {
            continue;
        };
        if locations.iter().any(|location: &SourceLocation| {
            location.source_url.trim_end_matches('/') == url.trim_end_matches('/')
                && location.city == city
        }) {
            continue;
        }
        locations.push(SourceLocation {
            source_url: url.to_string(),
            city: city.to_string(),
        });
    }
    locations
}

fn brief_with_outage_evidence(evidence: Vec<String>) -> CompanyBrief {
    let mut brief = CompanyBrief::default();
    add_outage_evidence(&mut brief, evidence);
    brief
}

fn add_outage_evidence(brief: &mut CompanyBrief, evidence: Vec<String>) {
    for fact in evidence {
        if let Some(source) = bracketed_source(&fact) {
            if !brief.sources.iter().any(|existing| existing == source) {
                brief.sources.push(source.to_string());
            }
        }
        brief.observed_facts.push(fact);
    }
}

fn bracketed_source(value: &str) -> Option<&str> {
    value
        .strip_prefix('[')?
        .split_once(']')
        .map(|(source, _)| source)
}

fn role_search_queries(brand: &str, focus: &str, company: &str, domain: &str) -> Vec<String> {
    let company = company.replace('"', " ");
    if brand == "wapahki" {
        let focus = focus.to_ascii_lowercase();
        let roles = if [
            "machine tend",
            "press tend",
            "press loading",
            "stamping",
            "metal press",
            "fixture loading",
            "cnc",
            "repetitive assembly",
        ]
        .iter()
        .any(|term| focus.contains(term))
        {
            "(\"machine operator\" OR \"press operator\" OR \"cnc operator\" OR \"production assembler\" OR \"fixture loading\" OR \"parts loading\" OR \"manufacturing operator\" OR \"quality inspector\")"
        } else if [
            "warehouse",
            "distribution",
            "3pl",
            "fulfillment",
            "logistics",
        ]
        .iter()
        .any(|term| focus.contains(term))
        {
            "(\"order picker\" OR \"case picker\" OR \"warehouse associate\" OR \"material handler\" OR \"pallet builder\" OR replenishment OR shipper OR receiver OR forklift)"
        } else {
            "(\"packaging operator\" OR \"production operator\" OR palletizing OR \"case packing\" OR \"line operator\" OR \"material handler\" OR assembler OR sanitation)"
        };
        vec![
            format!("site:{domain} (Ontario OR Canada) {roles}"),
            format!("site:{domain} {roles} careers"),
            format!("\"{company}\" {roles} jobs"),
        ]
    } else if brand == "gnk" {
        let focus = focus.to_ascii_lowercase();
        // Segment identity wins over ambiguous problem words. "Delay" and
        // "claims" occur throughout logistics; letting either choose the
        // construction vocabulary sent 3PL research toward RFIs and schedulers.
        let roles = if [
            "3pl",
            "logistics",
            "freight",
            "transport",
            "warehouse",
            "fulfillment",
            "cold chain",
            "cold-chain",
        ]
        .iter()
        .any(|term| focus.contains(term))
        {
            "(exceptions OR accessorial OR detention OR claims OR POD OR \"proof of delivery\" OR billing OR audit)"
        } else if [
            "construction",
            "contractor",
            "change order",
            "project controls",
            "rfi",
            "daily report",
        ]
        .iter()
        .any(|term| focus.contains(term))
        {
            "(\"project controls\" OR \"change order\" OR RFI OR \"daily report\" OR delay OR claims OR scheduler)"
        } else {
            "(claims OR compliance OR recovery OR adjudication OR adjuster OR analyst OR audit)"
        };
        vec![
            format!("site:{domain} {roles} (careers OR jobs)"),
            format!("\"{company}\" {roles} jobs"),
        ]
    } else {
        Vec::new()
    }
}

fn research_seed_urls(domain: &str) -> Vec<String> {
    let path = std::env::var("SPRUCE_RESEARCH_SEEDS")
        .unwrap_or_else(|_| ".spruce/research-seeds.json".into());
    let Ok(raw) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    research_seed_urls_from_json(&raw, domain)
}

fn research_seed_urls_from_json(raw: &str, domain: &str) -> Vec<String> {
    let Ok(seeds) = serde_json::from_str::<std::collections::HashMap<String, Vec<String>>>(raw)
    else {
        return Vec::new();
    };
    let domain = domain
        .trim()
        .trim_start_matches("www.")
        .to_ascii_lowercase();
    let mut urls = seeds
        .into_iter()
        .find(|(key, _)| {
            key.trim()
                .trim_start_matches("www.")
                .eq_ignore_ascii_case(&domain)
        })
        .map(|(_, urls)| urls)
        .unwrap_or_default();
    urls.retain(|url| {
        reqwest::Url::parse(url)
            .is_ok_and(|parsed| parsed.scheme() == "https" && parsed.host_str().is_some())
    });
    urls.sort();
    urls.dedup();
    urls.truncate(12);
    urls
}

fn job_location_priority(brand: &str, url: &str, text: &str) -> u8 {
    if brand != "wapahki" {
        return 0;
    }
    let haystack =
        format!("{url} {}", text.chars().take(3_000).collect::<String>()).to_ascii_lowercase();
    if haystack.contains("ontario") || haystack.contains(", on") || haystack.contains("-on-") {
        0
    } else if haystack.contains("canada") || haystack.contains("/canada-") {
        1
    } else {
        2
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
    read_page_limited(research, url, PER_PAGE_CHARS).await
}

async fn read_page_for_links(research: &ResearchClient, url: &str) -> Option<String> {
    read_page_limited(research, url, 30_000).await
}

async fn read_page_limited(
    research: &ResearchClient,
    url: &str,
    max_chars: usize,
) -> Option<String> {
    match research.read(url).await {
        Ok(text) if research_page_usable(&text) => {
            let compact = compact_page_text(&text);
            (!compact.trim().is_empty()).then(|| limit_chars(&compact, max_chars))
        }
        _ => None,
    }
}

/// Reader output often repeats mobile/desktop navigation, cookie notices, and
/// image-only markdown before the operating facts. Deduplicate that chrome
/// before applying the evidence window so a visually busy site cannot turn a
/// small extraction call into a 30k-character prompt or crowd out the useful
/// text near the bottom of the page.
fn compact_page_text(text: &str) -> String {
    let mut seen = std::collections::HashSet::new();
    let mut kept = Vec::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let lower = line.to_ascii_lowercase();
        let chrome = line.starts_with("![")
            || lower.contains("javascript:;")
            || lower.starts_with("this website stores cookies")
            || lower.starts_with("if you decline, your information")
            || matches!(lower.as_str(), "accept decline" | "accept" | "decline")
            || (line.starts_with("[![") && line.contains("]("));
        if chrome {
            continue;
        }
        let key = lower.split_whitespace().collect::<Vec<_>>().join(" ");
        if seen.insert(key) {
            kept.push(line);
        }
    }
    kept.join("\n")
}

fn append_source_section(target: &mut String, label: &str, url: &str, text: &str) {
    target.push_str("\n\n");
    target.push_str(label);
    target.push_str("\nSOURCE URL: ");
    target.push_str(url);
    target.push('\n');
    target.push_str(text);
}

fn research_page_usable(text: &str) -> bool {
    let text = text.trim();
    if text.is_empty() {
        return false;
    }
    let head = text
        .chars()
        .take(1_000)
        .collect::<String>()
        .to_ascii_lowercase();
    let lower = text.to_ascii_lowercase();
    !head.contains("warning: target url returned error 404")
        && !head.contains("warning: target url returned error 410")
        && !head.contains("# page not found")
        && !head.contains("# 404")
        && !lower.contains("application period for this position has expired")
        && !lower.contains("job opening is not currently accepting applications")
        && !lower.contains("this job is no longer available")
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
            "claim",
            "decision",
            "exceptions",
            "compliance",
            "audit",
            "forms",
            "guide",
            "case-study",
            "case_study",
            "operations",
            "technology",
            "innovation",
            "underwriting",
            "workflow",
            "portal",
        ],
        "wapahki" => &[
            "process",
            "equipment",
            "plant",
            "line",
            "quality",
            "safety",
            "case-study",
            "case_study",
            "manufacturing",
            "capabilities",
            "packaging",
            "automation",
            "production",
            "facility",
        ],
        "outagehub" => &[
            "charging",
            "network",
            "locations",
            "stations",
            "service",
            "support",
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
            || is_image_url(&url)
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

/// Pull concrete role pages from a first-party careers index. Role details often
/// name the line, package, sanitation duty, or recurring operating decision that
/// a corporate homepage omits. Downstream prompts still treat these as hiring
/// evidence only, never proof of pain, urgency, budget, or recipient ownership.
fn relevant_job_links(base: &str, text: &str, brand: &str, limit: usize) -> Vec<String> {
    let role_terms: &[&str] = match brand {
        "wapahki" => &[
            "operator",
            "operation",
            "production",
            "pack",
            "sanitation",
            "maintenance",
            "plant",
            "manufacturing",
            "line",
        ],
        "gnk" => &[
            "claims",
            "adjuster",
            "operations",
            "compliance",
            "recovery",
            "analyst",
            "coordinator",
        ],
        "outagehub" => &[
            "charging",
            "network",
            "operations",
            "reliability",
            "maintenance",
            "service",
        ],
        _ => &["operations", "operator", "analyst", "manager"],
    };
    let mut links = ranked_internal_links(base, text, role_terms, limit);
    links.extend(ranked_company_job_links(base, text, role_terms, limit));
    let mut seen = std::collections::HashSet::new();
    links.retain(|url| seen.insert(url.clone()));
    links.truncate(limit.max(1));
    links
}

/// A link from the employer's own careers page is meaningful provenance even
/// when the company uses a separately branded jobs host. Keep this narrow:
/// only jobs/careers/join subdomains are candidates, and the fetched page must
/// later name the employer and contain motion-specific task evidence.
fn ranked_company_job_links(base: &str, text: &str, terms: &[&str], limit: usize) -> Vec<String> {
    let Ok(base_url) = reqwest::Url::parse(base) else {
        return Vec::new();
    };
    let base_host = base_url.host_str().unwrap_or_default();
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
        let Ok(mut url) = base_url.join(raw_url) else {
            continue;
        };
        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        let external_company_jobs_host = !same_company_host(&host, base_host)
            && !trusted_ats_host(&host)
            && matches!(host.split('.').next(), Some("jobs" | "careers" | "join"));
        if !matches!(url.scheme(), "http" | "https")
            || !external_company_jobs_host
            || is_image_url(&url)
        {
            continue;
        }
        let haystack = format!("{} {}", anchor, url.path()).to_ascii_lowercase();
        let score = terms
            .iter()
            .enumerate()
            .filter(|(_, term)| haystack.contains(**term))
            .map(|(index, _)| terms.len() - index)
            .sum::<usize>();
        if score == 0 {
            continue;
        }
        url.set_fragment(None);
        candidates.push((score, url.to_string()));
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    candidates.dedup_by(|left, right| left.1 == right.1);
    candidates
        .into_iter()
        .take(limit.max(1))
        .map(|(_, url)| url)
        .collect()
}

fn search_result_urls(html: &str, company_domain: &str, limit: usize) -> Vec<String> {
    let mut raw_urls = Vec::new();
    let mut rest = html;
    while let Some(class_at) = rest.find("class=\"result__a\"") {
        rest = &rest[class_at + "class=\"result__a\"".len()..];
        let Some(href_at) = rest.find("href=\"") else {
            break;
        };
        let after = &rest[href_at + "href=\"".len()..];
        let Some(end) = after.find('"') else {
            break;
        };
        let raw = after[..end].replace("&amp;", "&");
        rest = &after[end + 1..];
        raw_urls.push(raw);
    }
    // Brave embeds its organic result state as `url:"https://..."` entries.
    let mut rest = html;
    while let Some(url_at) = rest.find("url:\"https://") {
        let after = &rest[url_at + "url:\"".len()..];
        let Some(end) = after.find('"') else {
            break;
        };
        raw_urls.push(after[..end].replace("\\u0026", "&").replace("&amp;", "&"));
        rest = &after[end + 1..];
    }

    let mut urls = raw_urls
        .into_iter()
        .filter_map(|raw| reqwest::Url::parse(&raw).ok())
        .filter(|url| !is_image_url(url))
        .filter_map(|mut url| {
            let host = url.host_str().unwrap_or_default();
            let priority = if same_company_host(host, company_domain) {
                0
            } else if trusted_ats_host(host) {
                1
            } else if trusted_job_mirror_host(host) {
                2
            } else {
                return None;
            };
            url.set_fragment(None);
            Some((priority, url.to_string()))
        })
        .collect::<Vec<_>>();
    urls.sort_by_key(|(priority, _)| *priority);
    let mut seen = std::collections::HashSet::new();
    urls.retain(|(_, url)| seen.insert(url.clone()));
    urls.truncate(limit.max(1));
    urls.into_iter().map(|(_, url)| url).collect()
}

fn same_company_host(host: &str, company_domain: &str) -> bool {
    let company_domain = company_domain
        .trim()
        .trim_start_matches("www.")
        .to_ascii_lowercase();
    let host = host.trim().trim_start_matches("www.").to_ascii_lowercase();
    host == company_domain || host.ends_with(&format!(".{company_domain}"))
}

fn trusted_ats_host(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    [
        "greenhouse.io",
        "lever.co",
        "myworkdayjobs.com",
        "workdayjobs.com",
        "icims.com",
        "dayforcehcm.com",
        "adp.com",
        "paycomonline.net",
        "ultipro.com",
        "ukg.com",
        "jobvite.com",
        "bamboohr.com",
        "successfactors.com",
        "fitzii.com",
    ]
    .iter()
    .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
}

fn trusted_job_mirror_host(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    [
        "tealhq.com",
        "ziprecruiter.com",
        "indeed.com",
        "simplyhired.ca",
        "simplyhired.com",
        "workopolis.com",
        "linkedin.com",
        "glassdoor.ca",
        "glassdoor.com",
        "careerbeacon.com",
        "jobbank.gc.ca",
        "guichetemplois.gc.ca",
        "talent.com",
        "adzuna.ca",
        "swooped.co",
        "bebee.com",
        "trabajo.org",
        "greaterroccareers.com",
        "jobs.weekday.works",
    ]
    .iter()
    .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
}

fn job_page_has_task_evidence(text: &str, brand: &str) -> bool {
    let text = text.to_ascii_lowercase();
    let terms: &[&str] = if brand == "wapahki" {
        &[
            "pack",
            "pallet",
            "stack",
            "inspect",
            "sanitize",
            "sanitation",
            "production line",
            "machine operator",
            "batch",
            "lift",
            // Canadian Job Bank publishes the same employer-attributed role in
            // English and French. These terms intentionally mirror the narrow
            // physical-task boundary above; they are not a generic French-page
            // acceptance shortcut.
            "emball",
            "palette",
            "empiler",
            "charger manuellement",
            "alimenter et décharger",
            "tâches répétitives",
            "manipuler des charges",
        ]
    } else {
        &[
            "claim",
            "reconcil",
            "exception",
            "compliance",
            "recovery",
            "audit",
            "case management",
        ]
    };
    terms.iter().filter(|term| text.contains(**term)).count() >= 2
}

/// Preserve the employer, location and duty-bearing parts of a job page when
/// navigation would otherwise consume the fixed research window. Search and
/// seed pages are still validated against the full fetched text before this is
/// called; this function only decides which already-read lines reach the model.
fn job_evidence_window(text: &str, brand: &str, company: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }

    let company_terms = company
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(str::trim)
        .filter(|word| word.len() >= 4)
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let brand_terms: &[&str] = if brand == "wapahki" {
        &[
            "task",
            "responsibil",
            "duties",
            "location",
            "published",
            "posted",
            "pack",
            "pallet",
            "stack",
            "conveyor",
            "container",
            "machine",
            "lift",
            "repetitive",
            "manual",
            "physical",
            "fast-paced",
            "shift",
            "tâche",
            "responsabil",
            "emplacement",
            "publi",
            "emball",
            "palette",
            "empiler",
            "convoyeur",
            "contenant",
            "machine",
            "charger",
            "décharger",
            "répétiti",
            "manuel",
            "physique",
            "quart",
        ]
    } else {
        &[
            "task",
            "responsibil",
            "duties",
            "location",
            "published",
            "posted",
            "claim",
            "reconcil",
            "exception",
            "compliance",
            "recovery",
            "audit",
            "case management",
        ]
    };

    let lines = text.lines().collect::<Vec<_>>();
    let mut keep = vec![false; lines.len()];
    for (index, line) in lines.iter().enumerate() {
        let lower = line.to_ascii_lowercase();
        let metadata = index < 6
            || company_terms.iter().any(|term| lower.contains(term))
            || brand_terms.iter().any(|term| lower.contains(term));
        if metadata {
            let start = index.saturating_sub(1);
            let end = (index + 1).min(lines.len().saturating_sub(1));
            for slot in keep.iter_mut().take(end + 1).skip(start) {
                *slot = true;
            }
        }
    }
    let selected = lines
        .into_iter()
        .zip(keep)
        .filter_map(|(line, keep)| keep.then_some(line))
        .collect::<Vec<_>>()
        .join("\n");
    limit_chars(&selected, max_chars)
}

fn is_image_url(url: &reqwest::Url) -> bool {
    [
        ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".mp4", ".webm", ".mov", ".mp3",
        ".wav", ".css", ".js",
    ]
    .iter()
    .any(|extension| url.path().to_ascii_lowercase().ends_with(extension))
}

fn company_mentioned(text: &str, company: &str) -> bool {
    let text = text.to_ascii_lowercase();
    let significant = company
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(str::trim)
        .filter(|word| {
            word.len() >= 4
                || (word.len() >= 2
                    && word
                        .chars()
                        .filter(|ch| ch.is_ascii_alphabetic())
                        .all(|ch| ch.is_ascii_uppercase()))
        })
        .filter(|word| !matches!(*word, "company" | "group" | "foods" | "food" | "inc"))
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let needed = significant.len().min(2).max(1);
    significant
        .iter()
        .filter(|word| text.contains(*word))
        .count()
        >= needed
}

fn linked_job_page_credible(
    url: &str,
    company_domain: &str,
    text: &str,
    company: &str,
    brand: &str,
) -> bool {
    let host = reqwest::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_default();
    same_company_host(&host, company_domain)
        || (company_mentioned(text, company)
            && (trusted_ats_host(&host) || job_page_has_task_evidence(text, brand)))
}

fn ranked_internal_links(base: &str, text: &str, terms: &[&str], limit: usize) -> Vec<String> {
    let Ok(base_url) = reqwest::Url::parse(base) else {
        return Vec::new();
    };
    let Some(base_host) = base_url.host_str() else {
        return Vec::new();
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
        if raw_url.is_empty()
            || raw_url.starts_with('#')
            || raw_url.starts_with("mailto:")
            || raw_url.starts_with("javascript:")
        {
            continue;
        }
        let Ok(mut url) = base_url.join(raw_url) else {
            continue;
        };
        let host = url.host_str().unwrap_or_default();
        if !matches!(url.scheme(), "http" | "https")
            || (!same_company_host(host, base_host) && !trusted_ats_host(host))
            || is_image_url(&url)
        {
            continue;
        }
        let haystack = format!("{} {}", anchor, url.path()).to_ascii_lowercase();
        let score = terms
            .iter()
            .enumerate()
            .filter(|(_, term)| haystack.contains(**term))
            .map(|(index, _)| terms.len() - index)
            .sum::<usize>();
        if score == 0 {
            continue;
        }
        url.set_fragment(None);
        candidates.push((score, url.to_string()));
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
    use super::{
        compact_page_text, company_mentioned, job_evidence_window, job_location_priority,
        job_page_has_task_evidence, linked_job_page_credible, ranked_internal_links,
        relevant_internal_links, relevant_job_links, research_page_usable,
        research_seed_urls_from_json, role_search_queries, search_result_urls,
        source_locations_from_corpus, trusted_ats_host, trusted_job_mirror_host, CompanyBrief,
    };

    #[test]
    fn source_locations_require_ontario_on_the_exact_page() {
        let corpus = "FIRST PAGE\nSOURCE URL: https://jobs.example.com/cambridge\nTitle: Operator - Cambridge, ON\nPackages cases.\n\nSECOND PAGE\nSOURCE URL: https://jobs.example.com/london\nTitle: Operator - London, UK\nPalletizes cases.";
        let locations = source_locations_from_corpus(corpus);
        assert_eq!(locations.len(), 1);
        assert_eq!(
            locations[0].source_url,
            "https://jobs.example.com/cambridge"
        );
        assert_eq!(locations[0].city, "Cambridge");
    }

    #[test]
    fn compacts_repeated_reader_chrome_without_losing_operating_facts() {
        let compact = compact_page_text(
            "This website stores cookies on your computer.\nAccept Decline\n![Logo](logo.svg)\nProducts\nProducts\nMonitor 15,000 chargers in real time.\n24/7 customer support",
        );
        assert!(!compact.contains("cookies"));
        assert!(!compact.contains("logo.svg"));
        assert_eq!(compact.matches("Products").count(), 1);
        assert!(compact.contains("Monitor 15,000 chargers in real time."));
        assert!(compact.contains("24/7 customer support"));
    }

    #[test]
    fn research_seeds_are_domain_scoped_https_urls_only() {
        let seeds = research_seed_urls_from_json(
            r#"{"example.com":["https://careers.example.com/job/1","http://example.com/job/2","not-a-url"],"other.com":["https://other.com/job"]}"#,
            "www.example.com",
        );
        assert_eq!(seeds, vec!["https://careers.example.com/job/1"]);
    }

    #[test]
    fn major_live_job_mirrors_are_valid_seed_hosts() {
        for host in [
            "ca.indeed.com",
            "www.simplyhired.ca",
            "www.workopolis.com",
            "ca.linkedin.com",
            "www.glassdoor.ca",
            "www.careerbeacon.com",
            "www.jobbank.gc.ca",
            "ca.talent.com",
        ] {
            assert!(trusted_job_mirror_host(host), "{host} should be trusted");
        }
        assert!(!trusted_job_mirror_host("random-jobs.example"));
    }

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

    #[test]
    fn follows_motion_relevant_job_detail_links() {
        let links = relevant_job_links(
            "https://example.com/careers",
            "[Financial Controller](/jobs/finance) [Packaging Line Operator](/jobs/packaging-line-operator) [Privacy](/privacy)",
            "wapahki",
            2,
        );
        assert_eq!(
            links,
            vec!["https://example.com/jobs/packaging-line-operator"]
        );
    }

    #[test]
    fn follows_company_linked_job_on_trusted_ats_but_not_arbitrary_hosts() {
        let links = relevant_job_links(
            "https://example.com/careers",
            "[Production Operator](https://jobs.lever.co/example/production-operator) [Production Blog](https://unrelated.example.org/production)",
            "wapahki",
            3,
        );
        assert_eq!(
            links,
            vec!["https://jobs.lever.co/example/production-operator"]
        );

        let branded = relevant_job_links(
            "https://jbsfoodcanada.ca/careers/open-positions/",
            "[Production Operator](https://jobs.jbsfoodsgroup.com/jobs/17253340-production-operator) [Production Blog](https://unrelated.example.org/production)",
            "wapahki",
            3,
        );
        assert_eq!(
            branded,
            vec!["https://jobs.jbsfoodsgroup.com/jobs/17253340-production-operator"]
        );
        assert!(linked_job_page_credible(
            &branded[0],
            "jbsfoodcanada.ca",
            "JBS Canada production operator packs product to pallets and lifts cases.",
            "JBS Food Canada",
            "wapahki"
        ));
    }

    #[test]
    fn extracts_only_company_or_trusted_ats_search_results() {
        let html = r#"
            <a class="result__a" href="https://careers.example.com/jobs/packaging-operator">Own role</a>
            <a class="result__a" href="https://jobs.lever.co/example/production-operator?x=1&amp;y=2">ATS role</a>
            <a class="result__a" href="https://job-scraper.invalid/example">Scrape</a>
        "#;
        assert_eq!(
            search_result_urls(html, "example.com", 5),
            vec![
                "https://careers.example.com/jobs/packaging-operator".to_string(),
                "https://jobs.lever.co/example/production-operator?x=1&y=2".to_string(),
            ]
        );
        assert!(company_mentioned(
            "Baldwin Richardson Foods is hiring a production operator",
            "Baldwin Richardson Foods"
        ));
        assert!(!company_mentioned(
            "Another manufacturer is hiring",
            "Baldwin Richardson Foods"
        ));
        let brave = r#"url:"https://www.example.com/careers" url:"https://www.ziprecruiter.com/c/Example/Job/Machine-Operator" url:"https://untrusted.invalid/job""#;
        assert_eq!(
            search_result_urls(brave, "example.com", 5),
            vec![
                "https://www.example.com/careers".to_string(),
                "https://www.ziprecruiter.com/c/Example/Job/Machine-Operator".to_string(),
            ]
        );
        assert!(job_page_has_task_evidence(
            "Performs packing and stacking on the production line",
            "wapahki"
        ));
        assert!(job_page_has_task_evidence(
            "Tâches répétitives: charger manuellement les contenants et manipuler des charges",
            "wapahki"
        ));
        assert!(trusted_job_mirror_host("www.guichetemplois.gc.ca"));
        let long_job = format!(
            "Title: production labourer\n{}\nEmployer: Original Foods Limited\nLocation: Dunnville, ON\nTasks: Remove filled containers from conveyors.\nManually pack goods into boxes.\nWork conditions: Repetitive tasks and handling heavy loads.",
            "Navigation and generic links\n".repeat(600)
        );
        let evidence = job_evidence_window(&long_job, "wapahki", "Original Foods Limited", 1_000);
        assert!(evidence.contains("Original Foods Limited"));
        assert!(evidence.contains("Dunnville"));
        assert!(evidence.contains("Remove filled containers"));
        assert!(evidence.contains("Repetitive tasks"));
        assert!(evidence.chars().count() <= 1_000);
        let queries = role_search_queries(
            "wapahki",
            "Ontario warehouse case handling",
            "FGF Brands",
            "fgfbrands.com",
        );
        assert_eq!(queries.len(), 3);
        assert!(queries[0].contains("site:fgfbrands.com"));
        assert!(queries[0].contains("Ontario OR Canada"));
        assert!(queries[1].contains("forklift"));
        assert!(queries[1].contains("case picker"));
        assert!(queries[2].contains("\"FGF Brands\""));
        let machine_queries = role_search_queries(
            "wapahki",
            "Ontario machine tending and press loading",
            "Example Parts",
            "exampleparts.ca",
        );
        assert!(machine_queries[0].contains("press operator"));
        assert!(!machine_queries[0].contains("order picker"));
        let food_queries = role_search_queries(
            "wapahki",
            "food packaging line with staffing pressure",
            "Example Foods",
            "examplefoods.ca",
        );
        assert!(food_queries[0].contains("packaging operator"));
        assert!(!food_queries[0].contains("press operator"));
        let logistics_queries = role_search_queries(
            "gnk",
            "Canadian 3PL logistics delay claims and proof-of-delivery decisions",
            "Example Logistics",
            "examplelogistics.ca",
        );
        assert!(logistics_queries[0].contains("proof of delivery"));
        assert!(logistics_queries[0].contains("detention"));
        assert!(!logistics_queries[0].contains("project controls"));
        assert!(trusted_ats_host("pivotalhr.fitzii.com"));
        assert_eq!(
            job_location_priority(
                "wapahki",
                "https://careers.example.com/en/job/london/operator",
                "Location: London, Ontario, Canada"
            ),
            0
        );
        assert_eq!(
            job_location_priority(
                "wapahki",
                "https://example.com/job",
                "Location: Calgary, Canada"
            ),
            1
        );
        assert_eq!(
            job_location_priority(
                "wapahki",
                "https://example.com/job",
                "Location: Memphis, TN"
            ),
            2
        );
    }

    #[test]
    fn rejects_reader_404s_and_follows_generic_ats_openings() {
        assert!(!research_page_usable(
            "Title: Missing\nWarning: Target URL returned error 404: Not Found\nMarkdown Content:"
        ));
        assert!(research_page_usable("Title: Careers\n# Production roles"));
        assert!(!research_page_usable(
            "Title: Operator\nLots of navigation\nThe application period for this position has expired."
        ));
        assert!(!research_page_usable(
            "Title: Fitzii\nSorry\nThis job opening is not currently accepting applications."
        ));
        assert_eq!(
            ranked_internal_links(
                "https://example.com/careers",
                "[Job Openings](https://myjobs.adp.com/example/cx)",
                &["job", "career", "opening"],
                3,
            ),
            vec!["https://myjobs.adp.com/example/cx"]
        );
    }
}
