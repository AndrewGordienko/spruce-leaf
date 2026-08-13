//! Configurable opportunity discovery and pursuit.
//!
//! The engine does not know that "OutageHub means grants". A business profile
//! enables a funding motion and supplies official source pages, search queries,
//! applicant facts, unknowns, themes, and contact roles. This module provides
//! the reusable tools: discover -> read -> extract -> verify -> score -> store,
//! then official/Apollo contact resolution, pre-application outreach, and an
//! application brief.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{Duration, Utc};
use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::apollo::{Apollo, ApolloOrg, OrgFilters, PeopleFilters};
use crate::business::{BusinessProfile, FundingProfile, OpportunitySource, SponsorshipRoute};
use crate::calendar::{self, TimingContext};
use crate::db::{ApplicationBrief, Opportunity, OpportunityContact, OpportunityTouch, SharedDb};
use crate::engine::Engine;
use crate::knowledge::core_strategy_block;
use crate::playbook::{self, Playbook, Shared};
use crate::verify;

const MAX_DOCUMENT_CHARS: usize = 80_000;
const MAX_RAW_SNAPSHOT_CHARS: usize = 20_000;

#[derive(Debug, Default)]
pub struct DiscoverySummary {
    pub sources_attempted: usize,
    pub sources_read: usize,
    pub candidates_found: usize,
    pub opportunities_verified: usize,
    pub opportunities_added: usize,
    pub opportunities_updated: usize,
    pub skipped: usize,
    pub errors: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ContactSummary {
    pub official_contacts: usize,
    pub apollo_people_found: usize,
    pub contacts_added: usize,
    pub contacts_enriched: usize,
    pub verified_emails: usize,
}

#[derive(Debug, Default)]
pub struct FundingPlanSummary {
    pub contacts_planned: usize,
    pub touches_drafted: usize,
    pub touches_scheduled: usize,
}

#[derive(Debug, Default)]
pub struct SponsorshipSeedSummary {
    pub accounts_considered: usize,
    pub opportunities_added: usize,
    pub opportunities_updated: usize,
    pub contacts_mapped: usize,
    pub skipped_without_evidence: usize,
    pub skipped_without_target_fit: usize,
    pub skipped_without_budget_evidence: usize,
    pub skipped_without_budget_contact: usize,
}

#[derive(Debug, Default)]
pub struct SponsorshipPack {
    pub title: String,
    pub price: String,
    pub infrastructure_need: String,
    pub product_truth: Vec<String>,
    pub sponsor_benefits: Vec<String>,
    pub independence_terms: Vec<String>,
    pub buyer_checks: Vec<String>,
    pub agreement_checks: Vec<String>,
}

pub struct FundingOutreachOptions<'a> {
    pub opportunity_id: &'a str,
    pub touches: usize,
    pub auto_schedule: bool,
}

pub struct SponsorshipOutreachOptions<'a> {
    pub opportunity_id: &'a str,
    pub touches: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SponsorshipResearchImport {
    pub organization: String,
    pub domain: String,
    #[serde(default)]
    pub industry: String,
    #[serde(default)]
    pub geography: String,
    pub recipient_kind: String,
    pub relevance_evidence: SponsorshipSourceEvidence,
    pub budget_evidence: SponsorshipSourceEvidence,
    pub contact: SponsorshipResearchContact,
    pub permitted_benefit: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SponsorshipSourceEvidence {
    pub source_url: String,
    pub exact_excerpt: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SponsorshipResearchContact {
    #[serde(default)]
    pub existing_person_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub linkedin_url: String,
    #[serde(default)]
    pub person_source_url: String,
    #[serde(default)]
    pub email_source_url: String,
}

#[derive(Debug, Default)]
pub struct SponsorshipImportSummary {
    pub rows_read: usize,
    pub imported: usize,
    pub rejected: usize,
    pub opportunity_ids: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct Candidate {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    funder: String,
    #[serde(default)]
    reason: String,
}

#[derive(Debug, Deserialize)]
struct CandidateBatch {
    #[serde(default)]
    candidates: Vec<Candidate>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct ExtractedOpportunity {
    #[serde(default)]
    is_opportunity: bool,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    canonical_url: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    funder: String,
    #[serde(default)]
    summary: String,
    #[serde(default)]
    geography: String,
    #[serde(default)]
    opportunity_status: String,
    #[serde(default)]
    opens_at: String,
    #[serde(default)]
    deadline: String,
    #[serde(default)]
    deadline_timezone: String,
    #[serde(default)]
    funding_type: String,
    #[serde(default)]
    amount_min: String,
    #[serde(default)]
    amount_max: String,
    #[serde(default)]
    currency: String,
    #[serde(default)]
    cost_share: String,
    #[serde(default)]
    eligible_applicants: Vec<String>,
    #[serde(default)]
    eligible_activities: Vec<String>,
    #[serde(default)]
    ineligible_activities: Vec<String>,
    #[serde(default)]
    themes: Vec<String>,
    #[serde(default)]
    official_contact_name: String,
    #[serde(default)]
    official_contact_email: String,
    #[serde(default)]
    official_contact_phone: String,
    #[serde(default)]
    evidence: Vec<String>,
    #[serde(default)]
    documents: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct FitAssessment {
    #[serde(default)]
    score: i64,
    #[serde(default)]
    status: String,
    #[serde(default)]
    reasons: Vec<String>,
    #[serde(default)]
    blockers: Vec<String>,
    #[serde(default)]
    unknowns: Vec<String>,
    #[serde(default)]
    next_action: String,
}

#[derive(Debug, Deserialize)]
struct OpportunityEvaluation {
    opportunity: ExtractedOpportunity,
    fit: FitAssessment,
}

#[derive(Debug, Deserialize, Serialize)]
struct FundingSequence {
    #[serde(default)]
    touches: Vec<FundingCopyTouch>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct FundingCopyTouch {
    stage: i64,
    day_offset: i64,
    #[serde(default)]
    subject: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    purpose: String,
    #[serde(default)]
    goal: String,
}

#[derive(Debug, Deserialize)]
struct SponsorshipReview {
    passes: bool,
    score: i64,
    #[serde(default)]
    issues: Vec<String>,
    #[serde(default)]
    revised_subject: String,
    #[serde(default)]
    revised_body: String,
}

#[derive(Debug, Deserialize)]
struct ApplicationDraft {
    #[serde(default)]
    eligibility_summary: String,
    #[serde(default)]
    project_shape: String,
    #[serde(default)]
    narrative: String,
    #[serde(default)]
    workplan: Vec<String>,
    #[serde(default)]
    milestones: Vec<String>,
    #[serde(default)]
    evidence_needed: Vec<String>,
    #[serde(default)]
    required_documents: Vec<String>,
    #[serde(default)]
    budget_questions: Vec<String>,
    #[serde(default)]
    questions_for_funder: Vec<String>,
    #[serde(default)]
    risks: Vec<String>,
    #[serde(default)]
    next_steps: Vec<String>,
}

/// Jina Reader works without a key at a low rate; Jina Search requires one.
/// Direct HTTP is a fallback for pages Reader cannot access.
pub struct ResearchClient {
    http: reqwest::Client,
    jina_key: Option<String>,
}

impl ResearchClient {
    pub fn from_env() -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("spruce-leaf/0.1 opportunity-research")
            // Kept deliberately tight: a hung fetch otherwise stalls a whole
            // sourcing run one request at a time. Real sites answer well inside
            // this; a slow one is not worth the wait when research is best-effort.
            .timeout(std::time::Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if safe_public_http_url(attempt.url()).is_ok() {
                    attempt.follow()
                } else {
                    attempt.stop()
                }
            }))
            .build()
            .context("building opportunity research client")?;
        let jina_key = std::env::var("JINA_API_KEY")
            .ok()
            .filter(|key| !key.trim().is_empty());
        Ok(Self { http, jina_key })
    }

    pub async fn read(&self, url: &str) -> Result<String> {
        let url = validated_research_url(url)?;
        let reader_url = format!("https://r.jina.ai/{}", url.as_str());
        let mut req = self.http.get(reader_url).header(ACCEPT, "text/plain");
        if let Some(key) = &self.jina_key {
            req = req.header(AUTHORIZATION, format!("Bearer {key}"));
        }
        if let Ok(resp) = req.send().await {
            if resp.status().is_success() {
                let text = resp.text().await.unwrap_or_default();
                if !text.trim().is_empty() {
                    return Ok(limit_chars(&text, MAX_DOCUMENT_CHARS));
                }
            }
        }

        let resp = self
            .http
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("fetching {url}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "fetching {url} returned {status}: {}",
                limit_chars(&text, 300)
            );
        }
        Ok(limit_chars(&text, MAX_DOCUMENT_CHARS))
    }

    /// Best-effort public web discovery for account research when no paid Jina
    /// Search key is configured. The caller must still validate every returned
    /// URL and read the underlying page before treating anything as evidence;
    /// search snippets themselves are never evidence.
    pub async fn public_search(&self, query: &str) -> Result<String> {
        static SEARCH_SLOTS: std::sync::OnceLock<tokio::sync::Semaphore> =
            std::sync::OnceLock::new();
        static SEARCH_BLOCKED_UNTIL: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now < SEARCH_BLOCKED_UNTIL.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("public web search is cooling down after a rate-limit response");
        }
        let _permit = SEARCH_SLOTS
            .get_or_init(|| tokio::sync::Semaphore::new(1))
            .acquire()
            .await
            .map_err(|_| anyhow!("public-search concurrency gate closed"))?;
        let after_wait = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if after_wait < SEARCH_BLOCKED_UNTIL.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("public web search is cooling down after a rate-limit response");
        }
        // Keep the no-key endpoint civil and below its burst limit. This search
        // is best-effort, so a small serialized delay is preferable to a 429
        // storm that makes every concurrent account look evidence-free.
        tokio::time::sleep(std::time::Duration::from_millis(1_750)).await;

        // Brave's browser page embeds organic result URLs in rendered state.
        // Callers validate and read every underlying page; this HTML is
        // discovery only and never enters a model prompt.
        let mut brave_url = reqwest::Url::parse("https://search.brave.com/search")?;
        brave_url.query_pairs_mut().append_pair("q", query.trim());
        let brave = self
            .http
            .get(brave_url)
            .send()
            .await
            .context("calling public web search")?;
        let brave_status = brave.status();
        let brave_text = brave.text().await.unwrap_or_default();
        if !brave_status.is_success() {
            if brave_status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                SEARCH_BLOCKED_UNTIL.store(
                    after_wait.saturating_add(300),
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            bail!(
                "public web search returned {brave_status}: {}",
                limit_chars(&brave_text, 300)
            );
        }
        // Do not truncate before URL extraction: Brave places results after a
        // large stylesheet. The caller immediately reduces this to a few URLs.
        Ok(brave_text)
    }

    pub async fn search(&self, query: &str, allowed_domains: &[String]) -> Result<String> {
        let key = self
            .jina_key
            .as_ref()
            .ok_or_else(|| anyhow!("JINA_API_KEY is required for web search sources"))?;
        let scoped = if allowed_domains.is_empty() {
            query.to_string()
        } else {
            format!(
                "{} ({})",
                query,
                allowed_domains
                    .iter()
                    .map(|d| format!("site:{d}"))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            )
        };
        let mut url = reqwest::Url::parse("https://s.jina.ai/")?;
        url.set_path(&scoped);
        let resp = self
            .http
            .get(url)
            .header(ACCEPT, "text/plain")
            .header(AUTHORIZATION, format!("Bearer {key}"))
            .send()
            .await
            .context("calling Jina Search")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("Jina Search returned {status}: {}", limit_chars(&text, 300));
        }
        Ok(limit_chars(&text, MAX_DOCUMENT_CHARS))
    }
}

pub async fn discover(
    db: &SharedDb,
    engine: &Engine,
    profile: &BusinessProfile,
    limit: usize,
) -> Result<DiscoverySummary> {
    let funding = profile.funding()?;
    let research = ResearchClient::from_env()?;
    let limit = limit.clamp(1, 100);
    let mut summary = DiscoverySummary::default();
    let mut batches = Vec::new();
    let source_count = funding.sources.len().max(1);
    let per_source_limit = limit.div_ceil(source_count).max(1);

    for source in &funding.sources {
        summary.sources_attempted += 1;
        match collect_source_candidates(engine, &research, profile, funding, source).await {
            Ok(mut found) => {
                summary.sources_read += 1;
                found.truncate(per_source_limit);
                batches.push(found);
            }
            Err(e) => summary.errors.push(format!("{}: {e:#}", source.name)),
        }
    }

    // Cover every configured source, then interleave the results so a rich
    // catalog cannot crowd all later government/utility sources out of a
    // bounded run.
    let mut candidates = interleave_candidate_batches(batches);
    dedupe_candidates(&mut candidates);
    summary.candidates_found = candidates.len();
    candidates.truncate(limit);

    for candidate in candidates {
        let document = match research.read(&candidate.url).await {
            Ok(doc) => doc,
            Err(e) => {
                summary.errors.push(format!("{}: {e:#}", candidate.url));
                continue;
            }
        };
        let evaluation = match evaluate_opportunity(engine, profile, &candidate, &document).await {
            Ok(evaluation) if evaluation.opportunity.is_opportunity => evaluation,
            Ok(_) => {
                summary.skipped += 1;
                continue;
            }
            Err(e) => {
                summary
                    .errors
                    .push(format!("extract {}: {e:#}", candidate.url));
                continue;
            }
        };
        let extracted = evaluation.opportunity;
        let fit = evaluation.fit;
        summary.opportunities_verified += 1;

        let canonical_url = if extracted.canonical_url.starts_with("http") {
            extracted.canonical_url.clone()
        } else {
            candidate.url.clone()
        };
        let fingerprint = opportunity_fingerprint(
            &profile.key,
            &extracted.funder,
            &extracted.title,
            &canonical_url,
        );
        let existing = db
            .list_opportunities(Some(&profile.key), None)?
            .into_iter()
            .any(|o| o.fingerprint == fingerprint);
        let pipeline_status = initial_pipeline_status(&extracted, &fit);
        let opportunity = Opportunity {
            brand: profile.key.clone(),
            kind: default_string(&extracted.kind, "funding"),
            fingerprint,
            source_name: candidate_source(&candidate),
            source_url: candidate.url.clone(),
            canonical_url: canonical_url.clone(),
            title: extracted.title.clone(),
            funder: extracted.funder.clone(),
            funder_domain: domain_from_url(&canonical_url),
            summary: extracted.summary.clone(),
            geography: extracted.geography.clone(),
            opportunity_status: normalize_opportunity_status(&extracted.opportunity_status),
            opens_at: extracted.opens_at.clone(),
            deadline: extracted.deadline.clone(),
            deadline_timezone: extracted.deadline_timezone.clone(),
            funding_type: extracted.funding_type.clone(),
            amount_min: extracted.amount_min.clone(),
            amount_max: extracted.amount_max.clone(),
            currency: extracted.currency.clone(),
            cost_share: extracted.cost_share.clone(),
            eligible_applicants: extracted.eligible_applicants.clone(),
            eligible_activities: extracted.eligible_activities.clone(),
            ineligible_activities: extracted.ineligible_activities.clone(),
            themes: extracted.themes.clone(),
            official_contact_name: extracted.official_contact_name.clone(),
            official_contact_email: extracted.official_contact_email.clone(),
            official_contact_phone: extracted.official_contact_phone.clone(),
            evidence: extracted.evidence.clone(),
            documents: extracted.documents.clone(),
            fit_score: fit.score.clamp(0, 100),
            fit_status: normalize_fit_status(&fit.status),
            fit_reasons: fit.reasons.clone(),
            blockers: fit.blockers.clone(),
            unknowns: fit.unknowns.clone(),
            next_action: fit.next_action.clone(),
            pipeline_status,
            raw_snapshot: limit_chars(&document, MAX_RAW_SNAPSHOT_CHARS),
            ..Default::default()
        };
        let opportunity_id = db.upsert_opportunity(&opportunity)?;
        if existing {
            summary.opportunities_updated += 1;
        } else {
            summary.opportunities_added += 1;
        }

        if !extracted.official_contact_email.trim().is_empty()
            || !extracted.official_contact_phone.trim().is_empty()
            || !extracted.official_contact_name.trim().is_empty()
        {
            upsert_official_contact(db, &opportunity_id, profile, &extracted).await?;
        }
        db.log_event(
            &profile.key,
            &format!("opportunity:{opportunity_id}"),
            "",
            "opportunity_verified",
            &format!("{} [{}]", extracted.title, fit.status),
        )?;
    }

    Ok(summary)
}

async fn collect_source_candidates(
    engine: &Engine,
    research: &ResearchClient,
    profile: &BusinessProfile,
    funding: &FundingProfile,
    source: &OpportunitySource,
) -> Result<Vec<Candidate>> {
    if source.mode == "program" {
        return Ok(vec![Candidate {
            title: source.name.clone(),
            url: source.url.clone(),
            funder: String::new(),
            reason: format!("Configured official programme source: {}", source.name),
        }]);
    }
    let document = if source.mode == "search" {
        research
            .search(&source.query, &source.allowed_domains)
            .await?
    } else {
        research.read(&source.url).await?
    };
    let today = Utc::now().format("%Y-%m-%d");
    let user = format!(
        "Find funding, funded-pilot, challenge, contribution, tax-credit, procurement, advisory, \
         or innovation-programme pages in this source that could plausibly match {name}. Today is \
         {today}. Return only absolute official URLs present in the document. Include closed but \
         recurring programmes because they belong on a watchlist. Do not invent a programme or URL.\n\n\
         BUSINESS: {summary}\nTHEMES: {themes}\nPROJECT SHAPES: {shapes}\n\nSOURCE: {source_name}\n\n{doc}",
        name = profile.name,
        summary = profile.summary,
        themes = funding.themes.join(" | "),
        shapes = funding.project_shapes.join(" | "),
        source_name = source.name,
        doc = document,
    );
    let mut batch = engine
        .structured_bulk::<CandidateBatch>(
            "opportunity.candidates",
            "You extract candidate opportunity links from official-source text. Evidence only; never invent links.",
            &user,
            candidate_schema(),
        )
        .await?;
    for candidate in &mut batch.candidates {
        if candidate.reason.trim().is_empty() {
            candidate.reason = source.name.clone();
        } else {
            candidate.reason = format!("{} — {}", source.name, candidate.reason);
        }
    }
    batch
        .candidates
        .retain(|c| c.url.starts_with("https://") || c.url.starts_with("http://"));
    if !source.allowed_domains.is_empty() {
        batch.candidates.retain(|candidate| {
            source
                .allowed_domains
                .iter()
                .any(|domain| host_matches_allowed_domain(&candidate.url, domain))
        });
    }
    Ok(batch.candidates)
}

async fn evaluate_opportunity(
    engine: &Engine,
    profile: &BusinessProfile,
    candidate: &Candidate,
    document: &str,
) -> Result<OpportunityEvaluation> {
    let funding = profile.funding()?;
    let today = Utc::now().format("%Y-%m-%d");
    let business = json!({
        "business": profile.name,
        "summary": profile.summary,
        "known_facts": profile.known_facts,
        "known_unknowns": profile.unknowns,
        "constraints": profile.constraints,
        "funding_objective": funding.objective,
        "themes": funding.themes,
        "project_shapes": funding.project_shapes,
    });
    let user = format!(
        "Extract one opportunity from this official page for a structured opportunity database. \
         Today is {today}. Preserve the distinction between grant, non-repayable contribution, \
         repayable contribution, loan, tax credit, procurement, challenge prize, funded pilot, \
         and advisory support. `opportunity_status` must be open, forecast, rolling, closed, or \
         unknown based only on the page. Use ISO dates when the page provides enough information. \
         Every evidence item must identify the field it supports and include source wording or a \
         close paraphrase. If this is only an article or old award announcement with no programme \
         to apply to or monitor, set is_opportunity=false. Then assess fit conservatively using \
         only known_facts as proven. Unknown mandatory criteria mean needs_information; explicit \
         mismatch means ineligible. Separate reasons, blockers, and unknowns. Do not confuse \
         thematic relevance with eligibility.\n\nBUSINESS:\n{business}\n\n\
         CANDIDATE: {hint}\nFUNDER HINT: {funder_hint}\nURL: {url}\n\nDOCUMENT:\n{document}\n\n{doctrine}",
        business = serde_json::to_string_pretty(&business).unwrap_or_default(),
        hint = candidate.title,
        funder_hint = candidate.funder,
        url = candidate.url,
        doctrine = core_strategy_block("funding"),
    );
    engine
        .structured_bulk::<OpportunityEvaluation>(
            "opportunity.evaluate",
            "You are a meticulous funding analyst and conservative eligibility reviewer. Official evidence before claims.",
            &user,
            evaluation_schema(),
        )
        .await
}

async fn upsert_official_contact(
    db: &SharedDb,
    opportunity_id: &str,
    profile: &BusinessProfile,
    extracted: &ExtractedOpportunity,
) -> Result<String> {
    upsert_official_contact_fields(
        db,
        opportunity_id,
        profile,
        &extracted.official_contact_name,
        &extracted.official_contact_email,
        &extracted.official_contact_phone,
    )
    .await
}

async fn upsert_official_contact_fields(
    db: &SharedDb,
    opportunity_id: &str,
    profile: &BusinessProfile,
    name: &str,
    email: &str,
    phone: &str,
) -> Result<String> {
    let geography = db
        .get_opportunity(opportunity_id)?
        .map(|opportunity| opportunity.geography)
        .unwrap_or_default();
    let timezone =
        calendar::timezone_for_location(&geography, &profile.calendar.fallback_recipient_timezone)
            .name()
            .to_string();
    let email = email.trim().to_lowercase();
    let phone = phone.trim().to_string();
    let verdict = if email.is_empty() {
        None
    } else {
        Some(verify::verify_email(&email, "verified").await)
    };
    let route_key = if !email.is_empty() {
        email.clone()
    } else if !phone.is_empty() {
        normalize(&phone)
    } else {
        format!("programme:{opportunity_id}")
    };
    let key = format!("official:{route_key}");
    db.upsert_opportunity_contact(&OpportunityContact {
        opportunity_id: opportunity_id.to_string(),
        brand: profile.key.clone(),
        source: "official".into(),
        contact_key: key,
        name: name.trim().to_string(),
        title: "Published programme contact".into(),
        location: geography,
        timezone,
        role: "official_programme_route".into(),
        why_them:
            "This route is published on the official opportunity page for applicant questions."
                .into(),
        primary: true,
        email,
        email_status: verdict
            .map(|value| value.as_str())
            .unwrap_or("unknown")
            .into(),
        phone,
        status: if verdict == Some(verify::EmailVerdict::Verified) {
            "verified".into()
        } else {
            "new".into()
        },
        ..Default::default()
    })
}

pub async fn resolve_contacts(
    db: &SharedDb,
    apollo: &Apollo,
    profile: &BusinessProfile,
    opportunity_id: &str,
    limit: usize,
    enrich: bool,
) -> Result<ContactSummary> {
    let funding = profile.funding()?;
    let opportunity = db
        .get_opportunity(opportunity_id)?
        .ok_or_else(|| anyhow!("opportunity '{opportunity_id}' not found"))?;
    if !opportunity.official_contact_email.trim().is_empty()
        || !opportunity.official_contact_phone.trim().is_empty()
        || !opportunity.official_contact_name.trim().is_empty()
    {
        upsert_official_contact_fields(
            db,
            opportunity_id,
            profile,
            &opportunity.official_contact_name,
            &opportunity.official_contact_email,
            &opportunity.official_contact_phone,
        )
        .await?;
    }
    let mut summary = ContactSummary {
        official_contacts: db
            .list_opportunity_contacts(opportunity_id)?
            .iter()
            .filter(|c| c.source == "official")
            .count(),
        ..Default::default()
    };

    let source_hint = funding.sources.iter().find(|source| {
        opportunity.source_name.contains(&source.name)
            || (!source.url.is_empty()
                && domain_from_url(&source.url) == domain_from_url(&opportunity.source_url))
    });
    let organization_name = source_hint
        .map(|source| source.apollo_organization_name.trim())
        .filter(|name| !name.is_empty())
        .unwrap_or(opportunity.funder.as_str())
        .to_string();
    let mut organization_domains = source_hint
        .map(|source| source.apollo_domains.clone())
        .unwrap_or_default();
    if organization_domains.is_empty() && !opportunity.funder_domain.trim().is_empty() {
        organization_domains.push(opportunity.funder_domain.clone());
    }
    organization_domains = organization_domains
        .into_iter()
        .map(|domain| normalize_domain(&domain))
        .filter(|domain| !domain.is_empty())
        .collect();

    let people_filter =
        |organization_ids: Vec<String>, organization_domains: Vec<String>| PeopleFilters {
            organization_ids,
            organization_domains,
            titles: funding.preferred_contact_titles.clone(),
            page: 1,
            per_page: (limit.max(1) * 3).clamp(5, 50) as u32,
            ..Default::default()
        };

    // Apollo's People Search accepts employer domains and costs no search
    // credits. Prefer it before the one-credit organization-search fallback.
    let mut people = if organization_domains.is_empty() {
        Vec::new()
    } else {
        apollo
            .search_people(&people_filter(Vec::new(), organization_domains.clone()))
            .await?
    };
    let mut matched_org = None;
    if people.is_empty() {
        let orgs = apollo
            .search_organizations(&OrgFilters {
                name: organization_name.clone(),
                page: 1,
                per_page: 10,
                ..Default::default()
            })
            .await?;
        if let Some(org) = best_org_match_for(&organization_name, &organization_domains, &orgs) {
            people = apollo
                .search_people(&people_filter(vec![org.id.clone()], Vec::new()))
                .await?;
            matched_org = Some(org.clone());
        }
    }
    summary.apollo_people_found = people.len();

    for (index, person) in people.into_iter().take(limit.clamp(1, 10)).enumerate() {
        let key = if person.id.is_empty() {
            format!(
                "apollo:{}:{}",
                normalize(&person.full_name()),
                normalize(&person.title)
            )
        } else {
            format!("apollo:{}", person.id)
        };
        let location = person.location();
        let timezone = calendar::timezone_for_location(
            if location.is_empty() {
                &opportunity.geography
            } else {
                &location
            },
            &profile.calendar.fallback_recipient_timezone,
        )
        .name()
        .to_string();
        let contact = OpportunityContact {
            opportunity_id: opportunity_id.to_string(),
            brand: profile.key.clone(),
            source: "apollo".into(),
            contact_key: key,
            apollo_org_id: if person.organization_id.is_empty() {
                matched_org
                    .as_ref()
                    .map(|org| org.id.clone())
                    .unwrap_or_default()
            } else {
                person.organization_id.clone()
            },
            apollo_person_id: person.id.clone(),
            name: person.full_name(),
            title: person.title.clone(),
            location,
            timezone,
            role: "programme_or_innovation_advisor".into(),
            why_them: format!(
                "Their role at {} may give them a view of programme fit or the right internal route; verify this before relying on it.",
                opportunity.funder
            ),
            primary: summary.official_contacts == 0 && index == 0,
            linkedin_url: person.linkedin_url.clone(),
            email: person.email.clone(),
            email_status: map_apollo_email_status(&person.email_status),
            phone: person.best_phone(),
            status: "new".into(),
            ..Default::default()
        };
        let contact_id = db.upsert_opportunity_contact(&contact)?;
        summary.contacts_added += 1;

        if enrich {
            if db
                .get_opportunity_contact(&contact_id)?
                .is_some_and(|stored| stored.email_status == "verified")
            {
                summary.verified_emails += 1;
                continue;
            }
            let matched = apollo
                .enrich_person(
                    &person.id,
                    &person.first_name,
                    &person.last_name,
                    &matched_org
                        .as_ref()
                        .map(ApolloOrg::domain)
                        .or_else(|| organization_domains.first().cloned())
                        .unwrap_or_default(),
                    false,
                )
                .await;
            if let Ok(matched) = matched {
                summary.contacts_enriched += 1;
                let (email_status, verified) = if matched.email.trim().is_empty() {
                    ("unknown".to_string(), false)
                } else {
                    let verdict = verify::verify_email(&matched.email, &matched.email_status).await;
                    (
                        verdict.as_str().to_string(),
                        verdict == verify::EmailVerdict::Verified,
                    )
                };
                let location = matched.location();
                let timezone = calendar::timezone_for_location(
                    if location.is_empty() {
                        &opportunity.geography
                    } else {
                        &location
                    },
                    &profile.calendar.fallback_recipient_timezone,
                )
                .name()
                .to_string();
                db.set_opportunity_contact_enrichment(
                    &contact_id,
                    &matched.full_name(),
                    &matched.title,
                    &location,
                    &timezone,
                    &matched.linkedin_url,
                    &matched.email,
                    &email_status,
                    &matched.best_phone(),
                )?;
                if verified {
                    summary.verified_emails += 1;
                }
            }
        }
    }
    summary.verified_emails = db
        .list_opportunity_contacts(opportunity_id)?
        .iter()
        .filter(|contact| contact.email_status == "verified")
        .count();
    Ok(summary)
}

/// Build commercial sponsorship opportunities from the already-researched
/// OutageHub market map. Apollo does not define the sponsor universe here:
/// only accounts with an active, URL-backed outage decision/exposure claim are
/// admitted, and only existing verified people near a configured sponsor
/// budget role are mapped. Missing roles remain an explicit research gap.
pub fn seed_sponsorships(
    db: &SharedDb,
    profile: &BusinessProfile,
    limit: usize,
) -> Result<SponsorshipSeedSummary> {
    let sponsorship = profile.sponsorship()?;
    let mut summary = SponsorshipSeedSummary::default();
    let mut seen_leads = HashSet::new();
    let people = db.list_people(None, None)?;
    let lead_domains = db
        .list_leads(None)?
        .into_iter()
        .map(|lead| (lead.id, lead.domain.trim().to_ascii_lowercase()))
        .collect::<std::collections::HashMap<_, _>>();
    let existing_fingerprints = db
        .list_opportunities(Some(&profile.key), None)?
        .into_iter()
        .map(|opportunity| opportunity.fingerprint)
        .collect::<HashSet<_>>();

    for sales_opportunity in db.list_sales_opportunities(Some(&profile.key), None)? {
        if summary.opportunities_added + summary.opportunities_updated >= limit.max(1) {
            break;
        }
        if !matches!(
            sales_opportunity.evidence_tier.as_str(),
            "action_ready" | "discovery_ready"
        ) || !seen_leads.insert(sales_opportunity.lead_id.clone())
        {
            continue;
        }
        summary.accounts_considered += 1;
        let Some(lead) = db.get_lead(&sales_opportunity.lead_id)? else {
            continue;
        };
        let sponsor_haystack = format!(
            "{} {} {} {}",
            lead.name,
            lead.industry,
            lead.observed_facts.join(" "),
            sales_opportunity.task_or_decision
        )
        .to_ascii_lowercase();
        let matched_keywords = sponsorship
            .target_keywords
            .iter()
            .filter(|keyword| sponsor_haystack.contains(&keyword.trim().to_ascii_lowercase()))
            .cloned()
            .collect::<Vec<_>>();
        if matched_keywords.is_empty() {
            summary.skipped_without_target_fit += 1;
            continue;
        }
        let evidence_claims = db
            .list_evidence_claims(Some(&sales_opportunity.id), Some(&profile.key))?
            .into_iter()
            .filter(|claim| {
                matches!(claim.status.as_str(), "observed" | "verified")
                    && !claim.source_url.trim().is_empty()
                    && !claim.source_excerpt.trim().is_empty()
            })
            .collect::<Vec<_>>();
        if evidence_claims.is_empty() {
            summary.skipped_without_evidence += 1;
            continue;
        }
        let recipient_kind = sponsorship_recipient_kind(&lead.name, &lead.industry, &lead.domain);
        let Some(route) = sponsorship
            .routes
            .iter()
            .find(|route| route.recipient_kind == recipient_kind)
        else {
            summary.skipped_without_target_fit += 1;
            continue;
        };
        let budget_evidence = evidence_claims
            .iter()
            .filter(|claim| {
                let excerpt = claim.source_excerpt.to_ascii_lowercase();
                route
                    .budget_evidence_terms
                    .iter()
                    .any(|term| excerpt.contains(&term.trim().to_ascii_lowercase()))
            })
            .collect::<Vec<_>>();
        if budget_evidence.is_empty() {
            summary.skipped_without_budget_evidence += 1;
            continue;
        }
        let source_url = evidence_claims[0].source_url.clone();
        let canonical_url = canonical_company_url(&lead.domain, &source_url);
        let fingerprint = opportunity_fingerprint(
            &profile.key,
            &lead.name,
            &sponsorship.offer_key,
            &canonical_url,
        );
        let was_present = existing_fingerprints.contains(&fingerprint);
        let mut evidence = evidence_claims
            .iter()
            .take(4)
            .map(|claim| {
                format!(
                    "organization_relevance: {} — {}",
                    claim.source_excerpt, claim.source_url
                )
            })
            .collect::<Vec<_>>();
        evidence.extend(budget_evidence.iter().take(2).map(|claim| {
            format!(
                "budget_route_evidence: {} — {}",
                claim.source_excerpt, claim.source_url
            )
        }));
        evidence.push(format!("recipient_kind: {}", route.recipient_kind));
        evidence.push(format!(
            "permitted_benefit: {}",
            sponsorship.permitted_sponsor_benefits[0]
        ));
        let mut sponsor_people = people
            .iter()
            .filter(|person| {
                lead_domains.get(&person.lead_id).is_some_and(|domain| {
                    !domain.is_empty() && domain == &lead.domain.trim().to_ascii_lowercase()
                }) && person.email_status == "verified"
                    && !person.email.trim().is_empty()
            })
            .filter_map(|person| {
                sponsorship_contact_score(&person.title, route).map(|score| (score, person))
            })
            .collect::<Vec<_>>();
        sponsor_people.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.name.cmp(&right.1.name))
        });
        if sponsor_people.is_empty() {
            summary.skipped_without_budget_contact += 1;
            continue;
        }
        let amount = format!("{}.00", sponsorship.ask_amount_cad);
        let opportunity = Opportunity {
            brand: profile.key.clone(),
            kind: "sponsorship".into(),
            fingerprint,
            source_name: "outagehub_market_map".into(),
            source_url,
            canonical_url,
            title: sponsorship.offer_name.clone(),
            funder: lead.name.clone(),
            funder_domain: lead.domain.clone(),
            summary: format!(
                "{} has separate source-backed relevance to OutageHub and evidence of a route matching '{}'. The named contact is plausible but exact budget ownership remains the first question.",
                lead.name, route.recipient_kind
            ),
            geography: lead.hq.clone(),
            opportunity_status: "open".into(),
            funding_type: route.recipient_kind.clone(),
            amount_min: amount.clone(),
            amount_max: amount,
            currency: sponsorship.currency.clone(),
            cost_share: "No cost share; route-specific commercial sponsorship or infrastructure support.".into(),
            eligible_activities: sponsorship.permitted_sponsor_benefits.clone(),
            ineligible_activities: sponsorship.sponsor_independence.clone(),
            themes: matched_keywords,
            evidence,
            fit_score: if sales_opportunity.evidence_tier == "action_ready" {
                80
            } else {
                72
            },
            fit_status: "possible_fit".into(),
            fit_reasons: vec![
                "A named organization has source-backed relevance to OutageHub.".into(),
                "A separate source supports a configured sponsorship/budget route.".into(),
                "A named verified person has a title plausibly connected to that route.".into(),
                format!("The configured action for this recipient kind is: {}", route.action),
                format!(
                    "Andrew is willing to provide: {}",
                    sponsorship.permitted_sponsor_benefits[0]
                ),
            ],
            blockers: vec![
                "The named person's exact ownership, available amount, timing, and procurement path remain unconfirmed; touch one may ask only the route-appropriate question."
                    .into(),
            ],
            unknowns: vec![
                "Whether the named person owns or can route the evidenced budget/program.".into(),
                "Whether CAD $10,000 or the route-specific infrastructure support is available now.".into(),
                "Which permitted benefit matters and which agreement or vendor-onboarding steps apply.".into(),
            ],
            next_action: format!(
                "Draft one manual, source-grounded note to the highest-scoring verified contact. {}",
                route.action
            ),
            pipeline_status: "discovered".into(),
            raw_snapshot: serde_json::to_string(&evidence_claims).unwrap_or_default(),
            ..Default::default()
        };
        let opportunity_id = db.upsert_opportunity(&opportunity)?;
        if was_present {
            summary.opportunities_updated += 1;
        } else {
            summary.opportunities_added += 1;
        }
        for (index, (_, person)) in sponsor_people.into_iter().take(4).enumerate() {
            db.upsert_opportunity_contact(&OpportunityContact {
                opportunity_id: opportunity_id.clone(),
                brand: profile.key.clone(),
                source: "existing_crm".into(),
                contact_key: format!("person:{}", person.id),
                apollo_person_id: person.apollo_person_id.clone(),
                name: person.name.clone(),
                title: person.title.clone(),
                location: person.location.clone(),
                timezone: person.timezone.clone(),
                role: route.recipient_kind.clone(),
                why_them: format!(
                    "The verified title '{}' matches the configured '{}' route; exact budget ownership is still unconfirmed.",
                    person.title, route.recipient_kind
                ),
                primary: index == 0,
                linkedin_url: person.linkedin_url.clone(),
                email: person.email.clone(),
                email_status: person.email_status.clone(),
                phone: person.phone.clone(),
                status: "mapped".into(),
                ..Default::default()
            })?;
            summary.contacts_mapped += 1;
        }
    }
    Ok(summary)
}

/// Import a researched sponsorship manifest and independently re-verify every
/// fact before it can become an outreach opportunity. The manifest is a work
/// queue, never trusted evidence: source excerpts must occur on the fetched
/// pages, route/title rules must match, and a non-CRM email must be published on
/// the organization's own site with a live mail domain.
pub async fn import_sponsorship_research(
    db: &SharedDb,
    profile: &BusinessProfile,
    path: &std::path::Path,
) -> Result<SponsorshipImportSummary> {
    let sponsorship = profile.sponsorship()?;
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading sponsorship research {}", path.display()))?;
    let rows = if raw.trim_start().starts_with('[') {
        serde_json::from_str::<Vec<SponsorshipResearchImport>>(&raw)
            .context("parsing sponsorship research JSON array")?
    } else {
        raw.lines()
            .enumerate()
            .filter(|(_, line)| !line.trim().is_empty())
            .map(|(index, line)| {
                serde_json::from_str::<SponsorshipResearchImport>(line).with_context(|| {
                    format!("parsing sponsorship research JSONL line {}", index + 1)
                })
            })
            .collect::<Result<Vec<_>>>()?
    };
    let research = ResearchClient::from_env()?;
    let mut page_cache = std::collections::HashMap::<String, String>::new();
    let mut summary = SponsorshipImportSummary {
        rows_read: rows.len(),
        ..Default::default()
    };

    for row in rows {
        let result = import_one_sponsorship_candidate(
            db,
            profile,
            sponsorship,
            &research,
            &mut page_cache,
            &row,
        )
        .await;
        match result {
            Ok(id) => {
                summary.imported += 1;
                summary.opportunity_ids.push(id);
            }
            Err(error) => {
                summary.rejected += 1;
                summary
                    .errors
                    .push(format!("{}: {error:#}", row.organization));
            }
        }
    }
    Ok(summary)
}

async fn import_one_sponsorship_candidate(
    db: &SharedDb,
    profile: &BusinessProfile,
    sponsorship: &crate::business::SponsorshipProfile,
    research: &ResearchClient,
    page_cache: &mut std::collections::HashMap<String, String>,
    row: &SponsorshipResearchImport,
) -> Result<String> {
    if row.organization.trim().is_empty() || row.domain.trim().is_empty() {
        bail!("named organization and domain are required");
    }
    let domain = normalize_domain(row.domain.trim());
    if domain.is_empty() || !domain.contains('.') {
        bail!("organization domain is invalid");
    }
    let route = sponsorship
        .routes
        .iter()
        .find(|route| route.recipient_kind == row.recipient_kind)
        .ok_or_else(|| anyhow!("unsupported recipient kind '{}'", row.recipient_kind))?;
    let inferred_kind = sponsorship_recipient_kind(&row.organization, &row.industry, &domain);
    if inferred_kind != row.recipient_kind {
        bail!(
            "recipient kind '{}' contradicts deterministic organization type '{}'",
            row.recipient_kind,
            inferred_kind
        );
    }
    if !sponsorship
        .permitted_sponsor_benefits
        .iter()
        .any(|benefit| benefit == &row.permitted_benefit)
    {
        bail!("permitted_benefit must exactly match configured seller truth");
    }

    for (label, evidence) in [
        ("organization relevance", &row.relevance_evidence),
        ("budget/program", &row.budget_evidence),
    ] {
        if evidence.source_url.trim().is_empty() || evidence.exact_excerpt.trim().len() < 20 {
            bail!("{label} evidence requires URL and a substantive exact excerpt");
        }
        if label == "organization relevance"
            && !host_matches_allowed_domain(&evidence.source_url, &domain)
        {
            bail!("{label} source must be on {domain} or its subdomain");
        }
        let page = read_cached(research, page_cache, &evidence.source_url).await?;
        if !normalized_contains(&page, &evidence.exact_excerpt) {
            bail!("{label} excerpt was not found on {}", evidence.source_url);
        }
        if label == "budget/program"
            && !host_matches_allowed_domain(&evidence.source_url, &domain)
            && !normalized_contains(&page, &row.organization)
        {
            bail!("third-party budget/program evidence must explicitly name the organization");
        }
    }
    let budget_excerpt = row.budget_evidence.exact_excerpt.to_ascii_lowercase();
    if !route
        .budget_evidence_terms
        .iter()
        .any(|term| budget_excerpt.contains(&term.trim().to_ascii_lowercase()))
    {
        bail!(
            "budget/program excerpt contains none of the configured evidence terms for '{}'",
            route.recipient_kind
        );
    }
    let relevance_excerpt = row.relevance_evidence.exact_excerpt.to_ascii_lowercase();
    let relevance_terms = [
        "outage",
        "power",
        "electric",
        "grid",
        "resilien",
        "emergency",
        "critical infrastructure",
        "utility",
        "telecom",
        "cloud",
        "charging",
        "insurance",
        "climate",
        "data",
    ];
    if !relevance_terms
        .iter()
        .any(|term| relevance_excerpt.contains(term))
    {
        bail!("relevance excerpt does not establish why OutageHub matters");
    }

    let (contact, contact_key, contact_source, apollo_person_id) = if !row
        .contact
        .existing_person_id
        .trim()
        .is_empty()
    {
        let person = db
            .get_person(&row.contact.existing_person_id)?
            .ok_or_else(|| anyhow!("existing CRM person was not found"))?;
        if person.email_status != "verified" || person.email.trim().is_empty() {
            bail!("existing CRM person does not have a verified email");
        }
        let person_lead = db
            .get_lead(&person.lead_id)?
            .ok_or_else(|| anyhow!("existing CRM person's account was not found"))?;
        if normalize_domain(&person_lead.domain) != domain {
            bail!("existing CRM person belongs to a different company domain");
        }
        if sponsorship_contact_score(&person.title, route).is_none() {
            bail!("existing CRM person's title does not match the recipient route");
        }
        (
            SponsorshipResearchContact {
                name: person.name.clone(),
                title: person.title.clone(),
                email: person.email.clone(),
                location: person.location.clone(),
                linkedin_url: person.linkedin_url.clone(),
                ..Default::default()
            },
            format!("person:{}", person.id),
            "existing_crm".to_string(),
            person.apollo_person_id,
        )
    } else {
        if row.contact.name.trim().is_empty()
            || row.contact.title.trim().is_empty()
            || row.contact.email.trim().is_empty()
            || row.contact.email_source_url.trim().is_empty()
        {
            bail!("new contact requires name, title, email, and email_source_url");
        }
        if sponsorship_contact_score(&row.contact.title, route).is_none() {
            bail!("new contact title does not match the recipient route");
        }
        let email_source_is_first_party =
            host_matches_allowed_domain(&row.contact.email_source_url, &domain);
        let email_page = read_cached(research, page_cache, &row.contact.email_source_url).await?;
        if !email_page
            .to_ascii_lowercase()
            .contains(&row.contact.email.trim().to_ascii_lowercase())
        {
            bail!("contact source does not publish the exact email");
        }
        let person_source_url = if row.contact.person_source_url.trim().is_empty() {
            row.contact.email_source_url.as_str()
        } else {
            row.contact.person_source_url.as_str()
        };
        let person_page = read_cached(research, page_cache, person_source_url).await?;
        if !normalized_contains(&person_page, &row.contact.name)
            || !normalized_contains(&person_page, &row.contact.title)
        {
            bail!("person source does not publish the exact name and title");
        }
        if !host_matches_allowed_domain(person_source_url, &domain)
            && !normalized_contains(&person_page, &row.organization)
            && !normalized_contains(&person_page, &domain)
        {
            bail!("third-party person source must explicitly identify the organization");
        }
        if !email_source_is_first_party
            && (!normalized_contains(&email_page, &row.organization)
                || !normalized_contains(&email_page, &row.contact.name)
                || !normalized_contains(&email_page, &row.contact.title))
        {
            bail!(
                    "third-party email source must publish the organization, exact person, title, and email together"
                );
        }
        if !crate::verify::syntax_ok(&row.contact.email)
            || !crate::verify::has_mx(row.contact.email.split('@').nth(1).unwrap_or_default()).await
        {
            bail!("published contact email fails syntax or mail-domain verification");
        }
        let contact_email_domain =
            normalize_domain(row.contact.email.split('@').nth(1).unwrap_or_default());
        if !email_source_is_first_party && contact_email_domain != domain {
            bail!(
                "third-party published contact email must use the organization's canonical domain"
            );
        }
        let mailbox = row
            .contact
            .email
            .split('@')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let is_routed = [
            "info",
            "hello",
            "contact",
            "sales",
            "service",
            "support",
            "marketing",
            "partnerships",
            "community",
            "donations",
            "media",
            "office",
            "admin",
            "executiveassistant",
            "ontario",
            "east",
            "west",
            "tantalusinfo",
            "hay",
        ]
        .contains(&mailbox.as_str());
        (
            row.contact.clone(),
            format!(
                "published:{}",
                row.contact.email.trim().to_ascii_lowercase()
            ),
            if is_routed {
                "first_party_published_routed"
            } else if email_source_is_first_party {
                "first_party_published_direct"
            } else {
                "third_party_published_direct"
            }
            .to_string(),
            String::new(),
        )
    };

    let canonical_url = format!("https://{domain}");
    let fingerprint = opportunity_fingerprint(
        &profile.key,
        &row.organization,
        &sponsorship.offer_key,
        &canonical_url,
    );
    let amount = format!("{}.00", sponsorship.ask_amount_cad);
    let evidence = vec![
        format!(
            "organization_relevance: {} — {}",
            row.relevance_evidence.exact_excerpt, row.relevance_evidence.source_url
        ),
        format!(
            "budget_route_evidence: {} — {}",
            row.budget_evidence.exact_excerpt, row.budget_evidence.source_url
        ),
        format!("recipient_kind: {}", route.recipient_kind),
        format!("permitted_benefit: {}", row.permitted_benefit),
    ];
    let opportunity_id = db.upsert_opportunity(&Opportunity {
        brand: profile.key.clone(),
        kind: "sponsorship".into(),
        fingerprint,
        source_name: "verified_sponsorship_research".into(),
        source_url: row.budget_evidence.source_url.clone(),
        canonical_url,
        title: sponsorship.offer_name.clone(),
        funder: row.organization.clone(),
        funder_domain: domain,
        summary: format!(
            "{} has verified first-party relevance and independently published budget/program evidence for the '{}' route.",
            row.organization, route.recipient_kind
        ),
        geography: row.geography.clone(),
        opportunity_status: "open".into(),
        funding_type: route.recipient_kind.clone(),
        amount_min: amount.clone(),
        amount_max: amount,
        currency: sponsorship.currency.clone(),
        cost_share: "No cost share; route-specific sponsorship or infrastructure support.".into(),
        eligible_activities: sponsorship.permitted_sponsor_benefits.clone(),
        ineligible_activities: sponsorship.sponsor_independence.clone(),
        themes: vec![row.industry.clone(), route.recipient_kind.clone()],
        evidence,
        documents: vec![
            row.relevance_evidence.source_url.clone(),
            row.budget_evidence.source_url.clone(),
            if row.contact.email_source_url.is_empty() {
                "existing CRM email verification".into()
            } else {
                row.contact.email_source_url.clone()
            },
            if row.contact.person_source_url.is_empty() {
                row.contact.email_source_url.clone()
            } else {
                row.contact.person_source_url.clone()
            },
        ],
        fit_score: match contact_source.as_str() {
            "first_party_published_routed" => 80,
            "third_party_published_direct" => 88,
            _ => 92,
        },
        fit_status: "strong_fit".into(),
        fit_reasons: vec![
            "Named organization.".into(),
            "Exact independently published budget/program evidence re-fetched and matched.".into(),
            format!("Named verified person with a route-matched title; contact route: {contact_source}."),
            "Exact first-party reason OutageHub matters re-fetched and matched.".into(),
            format!("Configured recipient type: {}.", route.recipient_kind),
            format!("Configured benefit Andrew will provide: {}", row.permitted_benefit),
        ],
        blockers: vec![
            "Exact personal budget ownership, current availability, and procurement remain unconfirmed; the first note may ask only the configured route question."
                .into(),
        ],
        unknowns: vec![
            "Current budget availability and decision timing.".into(),
            "Required agreement, procurement, or infrastructure-credit process.".into(),
        ],
        next_action: format!("Draft one manual email. {}", route.action),
        pipeline_status: "shortlisted".into(),
        raw_snapshot: serde_json::to_string(row).unwrap_or_default(),
        ..Default::default()
    })?;
    db.upsert_opportunity_contact(&OpportunityContact {
        opportunity_id: opportunity_id.clone(),
        brand: profile.key.clone(),
        source: contact_source,
        contact_key,
        apollo_person_id,
        name: contact.name,
        title: contact.title,
        location: contact.location,
        role: route.recipient_kind.clone(),
        why_them: format!(
            "The verified title matches the configured '{}' route; exact budget ownership remains the first question.",
            route.recipient_kind
        ),
        primary: true,
        linkedin_url: contact.linkedin_url,
        email: contact.email,
        email_status: "verified".into(),
        status: "mapped".into(),
        ..Default::default()
    })?;
    Ok(opportunity_id)
}

async fn read_cached(
    research: &ResearchClient,
    cache: &mut std::collections::HashMap<String, String>,
    url: &str,
) -> Result<String> {
    if let Some(page) = cache.get(url) {
        return Ok(page.clone());
    }
    let page = research.read(url).await?;
    cache.insert(url.to_string(), page.clone());
    Ok(page)
}

fn normalized_contains(page: &str, excerpt: &str) -> bool {
    let normalize = |value: &str| {
        let mut normalized = String::with_capacity(value.len());
        let mut pending_space = false;
        for character in value.to_lowercase().chars() {
            if character.is_alphanumeric() {
                if pending_space && !normalized.is_empty() {
                    normalized.push(' ');
                }
                normalized.push(character);
                pending_space = false;
            } else {
                pending_space = true;
            }
        }
        normalized
    };
    normalize(page).contains(&normalize(excerpt))
}

fn canonical_company_url(domain: &str, fallback: &str) -> String {
    let domain = domain.trim().trim_end_matches('/');
    if domain.is_empty() {
        return fallback.trim().trim_end_matches('/').to_string();
    }
    if domain.starts_with("http://") || domain.starts_with("https://") {
        domain.to_string()
    } else {
        format!("https://{domain}")
    }
}

fn sponsorship_recipient_kind(name: &str, industry: &str, domain: &str) -> &'static str {
    let haystack = format!("{name} {industry} {domain}").to_ascii_lowercase();
    if haystack.contains("electricity canada")
        || ["association", "industry council", "trade group"]
            .iter()
            .any(|term| haystack.contains(term))
    {
        "electricity_canada_or_industry_association"
    } else if [
        "government",
        "ministry",
        "municipal",
        "municipality",
        "city of ",
        "province of ",
    ]
    .iter()
    .any(|term| haystack.contains(term))
    {
        "government"
    } else if ["journalism", "newspaper", "news", "media", "broadcast"]
        .iter()
        .any(|term| haystack.contains(term))
    {
        "journalist"
    } else if [
        "cloud",
        "hosting",
        "data center",
        "data centre",
        "infrastructure as a service",
    ]
    .iter()
    .any(|term| haystack.contains(term))
    {
        "cloud_or_infrastructure_vendor"
    } else {
        "commercial_sponsor"
    }
}

fn sponsorship_contact_score(title: &str, route: &SponsorshipRoute) -> Option<i64> {
    let title = title.trim().to_ascii_lowercase();
    let role_match = route
        .target_roles
        .iter()
        .any(|keyword| title.contains(&keyword.trim().to_ascii_lowercase()));
    if !role_match {
        return None;
    }
    let seniority = if title.contains("chief") || title.contains("founder") {
        50
    } else if title.contains("vice president") || title.contains("vp") {
        40
    } else if title.contains("head") || title.contains("director") {
        30
    } else if title.contains("manager") {
        20
    } else {
        10
    };
    let remit = route
        .target_roles
        .iter()
        .filter(|keyword| title.contains(&keyword.trim().to_ascii_lowercase()))
        .count() as i64;
    Some(seniority + remit)
}

pub async fn plan_funding_outreach(
    db: &SharedDb,
    engine: &Engine,
    profile: &BusinessProfile,
    playbook: &Playbook,
    shared: &Shared,
    options: FundingOutreachOptions<'_>,
) -> Result<FundingPlanSummary> {
    let FundingOutreachOptions {
        opportunity_id,
        touches,
        auto_schedule,
    } = options;
    let funding = profile.funding()?;
    let opportunity = db
        .get_opportunity(opportunity_id)?
        .ok_or_else(|| anyhow!("opportunity '{opportunity_id}' not found"))?;
    if opportunity.fit_status == "ineligible" {
        bail!("refusing outreach: opportunity is marked ineligible");
    }
    let n = touches.clamp(1, 3);
    let verified_contacts = db
        .list_opportunity_contacts(opportunity_id)?
        .into_iter()
        .filter(|c| !c.email.trim().is_empty() && c.email_status == "verified")
        .collect::<Vec<_>>();
    if verified_contacts.is_empty() {
        bail!("no verified opportunity contacts; resolve/enrich contacts before drafting outreach");
    }
    // Funding enquiries are routed, not sprayed. Prefer one verified primary
    // route, then an official published address, then the best stored contact.
    let selected = verified_contacts
        .iter()
        .find(|contact| contact.primary)
        .or_else(|| {
            verified_contacts
                .iter()
                .find(|contact| contact.source == "official")
        })
        .unwrap_or(&verified_contacts[0])
        .clone();
    let contacts = vec![selected];
    let mut summary = FundingPlanSummary::default();
    let forbidden = playbook.forbidden(shared);
    let now = Utc::now();

    for contact in contacts {
        if !db.list_opportunity_touches(&contact.id)?.is_empty() {
            continue;
        }
        let sequence = write_funding_sequence(
            engine,
            profile,
            &opportunity,
            &contact,
            &playbook.signature,
            n,
        )
        .await?;
        for copy in sequence.touches.into_iter().take(n) {
            let body = playbook::enforce_signature(&copy.body, &playbook.signature);
            let lint = playbook::lint(&body, &forbidden, funding.min_words, funding.max_words);
            let passes = lint.forbidden_hits.is_empty() && lint.length_ok;
            let status = if auto_schedule && passes {
                "scheduled"
            } else {
                "draft"
            };
            let desired = now + Duration::days(copy.day_offset.max(0));
            let stable_key = format!("funding:{}:{}:{}", opportunity_id, contact.id, copy.stage);
            let timing = TimingContext {
                industry: "public funding",
                title: &contact.title,
                vantage: &contact.role,
                channel: "email",
                location: if contact.location.is_empty() {
                    &opportunity.geography
                } else {
                    &contact.location
                },
                timezone: &contact.timezone,
                stable_key: &stable_key,
            };
            let slot =
                calendar::schedule_with_capacity(profile, &timing, desired, |start, end| {
                    db.planned_touch_count_between(&profile.key, start, end)
                })?;
            db.insert_opportunity_touch(&OpportunityTouch {
                opportunity_id: opportunity_id.to_string(),
                contact_id: contact.id.clone(),
                brand: profile.key.clone(),
                stage: copy.stage,
                day_offset: copy.day_offset,
                subject: copy.subject,
                body,
                purpose: copy.purpose,
                goal: copy.goal,
                status: status.into(),
                due_at: slot.at.to_rfc3339(),
                recipient_timezone: slot.recipient_timezone,
                scheduled_rule: slot.rule,
                schedule_reason: slot.rationale,
                review_passes: Some(passes),
                review_issues: lint.forbidden_hits,
                ..Default::default()
            })?;
            if status == "scheduled" {
                summary.touches_scheduled += 1;
            } else {
                summary.touches_drafted += 1;
            }
        }
        summary.contacts_planned += 1;
    }
    if summary.contacts_planned > 0 {
        db.set_opportunity_pipeline_status(
            opportunity_id,
            if auto_schedule {
                "contacting"
            } else {
                "shortlisted"
            },
        )?;
    }
    Ok(summary)
}

async fn write_funding_sequence(
    engine: &Engine,
    profile: &BusinessProfile,
    opportunity: &Opportunity,
    contact: &OpportunityContact,
    signature: &str,
    n: usize,
) -> Result<FundingSequence> {
    let funding = profile.funding()?;
    let context = json!({
        "business": profile.name,
        "business_summary": profile.summary,
        "known_facts": profile.known_facts,
        "unknowns": profile.unknowns,
        "opportunity": opportunity,
        "contact": contact,
    });
    let user = format!(
        "Write {n} short pre-application email touch(es), no more than three. This is not a sales \
         cadence and not a request to award money by email. Touch 1 must name the programme, state \
         one bounded project shape supported by known facts, acknowledge the exact eligibility or \
         project-fit uncertainty, and ask one answerable question. A later touch must add new \
         decision-useful information, not 'follow up'. Use day offsets such as 0 and 6. Keep every \
         body between {min} and {max} words and end it with exactly {signature}. Do not call a \
         contribution or tax credit a grant. Do not claim eligibility.\n\nBUSINESS-SPECIFIC DOCTRINE:\n{doctrine}\n\nCONTEXT:\n{context}\n\n{core_doctrine}",
        min = funding.min_words,
        max = funding.max_words,
        signature = signature,
        doctrine = funding.doctrine,
        context = serde_json::to_string_pretty(&context).unwrap_or_default(),
        core_doctrine = core_strategy_block("funding"),
    );
    engine
        .structured_bulk::<FundingSequence>(
            "opportunity.outreach",
            "You write precise founder-led pre-application funding enquiries grounded in official criteria.",
            &user,
            funding_sequence_schema(n),
        )
        .await
}

/// Draft a commercial sponsor conversation. This path is intentionally manual:
/// it never schedules, never treats a title as proof of budget ownership, and
/// requires two semantic audits plus deterministic gates before a touch is
/// marked as a reviewable draft.
pub async fn plan_sponsorship_outreach(
    db: &SharedDb,
    engine: &Engine,
    profile: &BusinessProfile,
    playbook: &Playbook,
    shared: &Shared,
    options: SponsorshipOutreachOptions<'_>,
) -> Result<FundingPlanSummary> {
    let SponsorshipOutreachOptions {
        opportunity_id,
        touches,
    } = options;
    let sponsorship = profile.sponsorship()?;
    let opportunity = db
        .get_opportunity(opportunity_id)?
        .ok_or_else(|| anyhow!("opportunity '{opportunity_id}' not found"))?;
    if opportunity.kind != "sponsorship" {
        bail!(
            "opportunity '{}' is kind '{}', not sponsorship",
            opportunity.id,
            opportunity.kind
        );
    }
    if opportunity.fit_status == "ineligible" {
        bail!("refusing sponsorship outreach: opportunity is marked ineligible");
    }
    let n = touches.clamp(1, sponsorship.default_touches.min(2).max(1));
    let verified_contacts = db
        .list_opportunity_contacts(opportunity_id)?
        .into_iter()
        .filter(|contact| !contact.email.trim().is_empty() && contact.email_status == "verified")
        .collect::<Vec<_>>();
    let selected = verified_contacts
        .iter()
        .find(|contact| contact.primary)
        .or_else(|| verified_contacts.first())
        .ok_or_else(|| {
            anyhow!(
                "no verified sponsorship contact; map a role near innovation, partnerships, resilience, research, strategy, data, product, corporate affairs, sustainability, or founder budget ownership"
            )
        })?
        .clone();
    if !db.list_opportunity_touches(&selected.id)?.is_empty() {
        return Ok(FundingPlanSummary::default());
    }

    let mut sequence = write_sponsorship_sequence(
        engine,
        profile,
        &opportunity,
        &selected,
        &playbook.signature,
        n,
    )
    .await?;
    if sequence.touches.len() != n {
        bail!(
            "sponsorship writer returned {} touches; expected {n}",
            sequence.touches.len()
        );
    }
    sequence.touches.sort_by_key(|touch| touch.stage);
    let forbidden = playbook.forbidden(shared);
    let mut semantic_issues = Vec::new();
    let mut semantic_passes = true;
    for audit_round in 1..=2 {
        let review = review_sponsorship_sequence(
            engine,
            profile,
            &opportunity,
            &selected,
            &sequence,
            audit_round == 2,
        )
        .await?;
        if review.passes && review.score >= 85 && review.issues.is_empty() {
            continue;
        }
        semantic_passes = false;
        semantic_issues.extend(
            review
                .issues
                .iter()
                .map(|issue| format!("audit {audit_round} ({}/100): {issue}", review.score)),
        );
        if review.issues.is_empty() {
            semantic_issues.push(format!(
                "audit {audit_round} rejected the sequence at {}/100 without a specific repair",
                review.score
            ));
        }
        if audit_round == 1
            && !review.revised_subject.trim().is_empty()
            && !review.revised_body.trim().is_empty()
            && sequence.touches.len() == 1
        {
            sequence.touches[0].subject = review.revised_subject;
            sequence.touches[0].body = review.revised_body;
            semantic_passes = true;
            semantic_issues.clear();
            continue;
        }
        break;
    }

    let now = Utc::now();
    let mut summary = FundingPlanSummary::default();
    for copy in sequence.touches.into_iter().take(n) {
        let body = playbook::enforce_signature(&copy.body, &playbook.signature);
        let mut issues =
            sponsorship_copy_issues(&copy.subject, &body, copy.stage, &opportunity.funding_type);
        let lint = playbook::lint(
            &body,
            &forbidden,
            sponsorship.min_words,
            sponsorship.max_words,
        );
        issues.extend(lint.forbidden_hits);
        if !lint.length_ok {
            issues.push(format!(
                "body must contain {}–{} words",
                sponsorship.min_words, sponsorship.max_words
            ));
        }
        if !semantic_passes {
            issues.extend(semantic_issues.clone());
        }
        issues.sort();
        issues.dedup();
        let passes = issues.is_empty();
        let desired = now + Duration::days(copy.day_offset.max(0));
        let industry = opportunity
            .themes
            .first()
            .map(String::as_str)
            .unwrap_or("commercial sponsorship");
        let stable_key = format!(
            "sponsorship:{}:{}:{}",
            opportunity_id, selected.id, copy.stage
        );
        let timing = TimingContext {
            industry,
            title: &selected.title,
            vantage: &selected.role,
            channel: "email",
            location: if selected.location.is_empty() {
                &opportunity.geography
            } else {
                &selected.location
            },
            timezone: &selected.timezone,
            stable_key: &stable_key,
        };
        let slot = calendar::schedule_with_capacity(profile, &timing, desired, |start, end| {
            db.planned_touch_count_between(&profile.key, start, end)
        })?;
        db.insert_opportunity_touch(&OpportunityTouch {
            opportunity_id: opportunity_id.to_string(),
            contact_id: selected.id.clone(),
            brand: profile.key.clone(),
            stage: copy.stage,
            day_offset: copy.day_offset,
            subject: copy.subject,
            body,
            purpose: copy.purpose,
            goal: copy.goal,
            status: if passes { "draft" } else { "blocked" }.into(),
            due_at: slot.at.to_rfc3339(),
            recipient_timezone: slot.recipient_timezone,
            scheduled_rule: slot.rule,
            schedule_reason: slot.rationale,
            review_passes: Some(passes),
            review_issues: issues,
            ..Default::default()
        })?;
        if passes {
            summary.touches_drafted += 1;
        }
    }
    summary.contacts_planned = 1;
    db.set_opportunity_pipeline_status(
        opportunity_id,
        if summary.touches_drafted == n {
            "shortlisted"
        } else {
            "discovered"
        },
    )?;
    Ok(summary)
}

async fn write_sponsorship_sequence(
    engine: &Engine,
    profile: &BusinessProfile,
    opportunity: &Opportunity,
    contact: &OpportunityContact,
    signature: &str,
    n: usize,
) -> Result<FundingSequence> {
    let sponsorship = profile.sponsorship()?;
    let route = sponsorship
        .routes
        .iter()
        .find(|route| route.recipient_kind == opportunity.funding_type)
        .ok_or_else(|| {
            anyhow!(
                "sponsorship opportunity has unsupported recipient kind '{}'",
                opportunity.funding_type
            )
        })?;
    let context = json!({
        "business": profile.name,
        "business_summary": profile.summary,
        "verified_seller_facts": profile.known_facts,
        "offer": sponsorship,
        "recipient_route": route,
        "target_company": opportunity.funder,
        "target_company_evidence_is_exhaustive": opportunity.evidence,
        "unverified_company_terms": opportunity.unknowns,
        "recipient": contact,
    });
    let user = format!(
        "Write {n} founder-led founding-sponsorship email touch(es), never more than two. The immediate commercial objective is one paid CAD $10,000 sponsorship. OutageHub is already built and operating; this is not a grant, donation, investment, server-bill appeal, repayment for prior work, or request to finance an unbuilt idea. Touch 1 opens naturally with Andrew as a U of T student who spent the past year building OutageHub with a few friends, says the team funded development themselves so far, and explains in one sentence that OutageHub already collects and normalizes monitored Canadian utilities' public outage reports into a public map and authenticated API. It then gives one concise, exact, supplied company-specific connection to outage resilience; asks for CAD $10,000 toward team time and infrastructure required to keep OutageHub operating and improve coverage; offers 12 months of founding-sponsor recognition, API access, quarterly coverage/data-quality briefings, and inclusion in the eventual annual impact report; and says the sponsorship would not influence the data or conclusions. End with exactly one question asking whether it can fit the recipient's partnerships, community-investment, or innovation budget and, in the same question, asking for routing if someone else owns it. Never claim this person owns the budget. Cloud and infrastructure vendors receive the same paid cash sponsorship ask, not a substitute credits request. Touch 2, if requested, adds one concrete product-truth, public-interest, or independence detail; never write a generic follow-up. Use plain 3–9 word subjects, day offsets 0 and 6, {min}–{max} words per body, and exactly the signature {signature}. Twelve months is the recognition period only; do not invent how long the money funds operations, a cost breakdown, live metric, endorsement, partnership, user, customer, SLA, private site status, or complete Canadian coverage. Use conversation claims only in the exact permitted form. Every target-company claim must appear in the exhaustive evidence. Return only the requested structure.\n\nBUSINESS-SPONSORSHIP DOCTRINE:\n{doctrine}\n\nCONTEXT:\n{context}\n\n{core}",
        min = sponsorship.min_words,
        max = sponsorship.max_words,
        doctrine = sponsorship.doctrine,
        context = serde_json::to_string_pretty(&context).unwrap_or_default(),
        core = core_strategy_block("sequence"),
    );
    engine
        .structured_bulk::<FundingSequence>(
            "opportunity.sponsorship_outreach",
            "You write evidence-bound founder sponsorship outreach. The recipient must see a bounded commercial deliverable and a reason to answer; never invent budget ownership or sell influence over independent findings.",
            &user,
            funding_sequence_schema(n),
        )
        .await
}

async fn review_sponsorship_sequence(
    engine: &Engine,
    profile: &BusinessProfile,
    opportunity: &Opportunity,
    contact: &OpportunityContact,
    sequence: &FundingSequence,
    falsify_clean_result: bool,
) -> Result<SponsorshipReview> {
    let sponsorship = profile.sponsorship()?;
    let route = sponsorship
        .routes
        .iter()
        .find(|route| route.recipient_kind == opportunity.funding_type)
        .ok_or_else(|| anyhow!("unknown sponsorship recipient route"))?;
    let prompt = json!({
        "task": if falsify_clean_result {
            "A prior reviewer found no material defect. Try to falsify that result without inventing a nitpick."
        } else {
            "Judge whether this exact sponsorship outreach is ready unchanged."
        },
        "offer": sponsorship,
        "required_recipient_route": route,
        "verified_target_company_evidence_is_exhaustive": opportunity.evidence,
        "recipient": contact,
        "sequence": sequence,
        "criteria": [
            "The subject and opening give this exact recipient an accurate reason to read.",
            "Every company claim is supported by the supplied evidence and no budget is presumed.",
            "Andrew's U of T student/few-friends/past-year story is human credibility and never implies institutional endorsement.",
            "The copy makes clear OutageHub is already operating, the team funded development themselves, and CAD $10,000 supports continued team time and infrastructure rather than merely a server bill or repayment for old work.",
            "The configured benefits are stated accurately: 12 months of recognition, API access, quarterly coverage/data-quality briefings, and eventual annual impact-report inclusion.",
            "The exact question follows the configured recipient route; immediate commercial and cloud/infrastructure targets are asked directly about the paid cash sponsorship.",
            "The copy explicitly says sponsorship would not influence the data or conclusions.",
            "Touch one asks exactly one low-effort role-matched question.",
            "The note sounds like one founder speaking naturally, not a funding application or mass template."
        ],
        "repair_rule": "If and only if one concise revision can make a single-touch sequence pass without adding unsupported facts, return the complete revised subject and body. Otherwise leave both empty.",
    });
    engine
        .structured_bulk::<SponsorshipReview>(
            "opportunity.sponsorship_review",
            "You are an independent skeptical recipient-side reviewer. Compliance with a writer prompt is not sendability. Reject vague relevance, presumed budgets, seller-only value, hidden grant language, invented metrics or cost periods, implied institutional endorsement, the wrong recipient route, or any sale of influence over independent findings.",
            &serde_json::to_string_pretty(&prompt).unwrap_or_default(),
            sponsorship_review_schema(),
        )
        .await
}

fn sponsorship_copy_issues(
    subject: &str,
    body: &str,
    stage: i64,
    recipient_kind: &str,
) -> Vec<String> {
    let mut issues = Vec::new();
    let subject_words = subject.split_whitespace().count();
    let lower = body.to_ascii_lowercase();
    if !(3..=9).contains(&subject_words) {
        issues.push("subject must contain 3–9 words".into());
    }
    if lower.contains(" grant") || lower.contains("donation") || lower.contains("keep us running") {
        issues.push("sponsorship copy must not read as a grant, donation, or runway appeal".into());
    }
    if lower.contains("control over") && !lower.contains("no control over") {
        issues.push("copy must not offer sponsor control over independent findings".into());
    }
    if stage == 1 {
        if body.matches('?').count() != 1 {
            issues.push("sponsorship touch 1 must ask exactly one question".into());
        }
        for (needle, label) in [
            ("outagehub", "OutageHub"),
            ("10,000", "the CAD $10,000 price"),
            ("few friends", "the few-friends founder story"),
            ("past year", "the past-year build commitment"),
            ("funded", "the team's self-funded development"),
            ("team time", "the continued team-time use of funds"),
            ("infrastructure", "the infrastructure use of funds"),
            ("map", "the existing public map"),
            ("api", "the existing API"),
            ("12 months", "the 12-month recognition period"),
            ("quarterly", "the quarterly briefing benefit"),
            ("annual", "the eventual annual impact-report benefit"),
            ("influence", "the sponsor-independence boundary"),
        ] {
            if !lower.contains(needle) {
                issues.push(format!("sponsorship touch 1 must name {label}"));
            }
        }
        if !(lower.contains("u of t") || lower.contains("university of toronto")) {
            issues.push("sponsorship touch 1 must name Andrew's U of T context".into());
        }
        if !(lower.contains("improve") && lower.contains("coverage")) {
            issues.push("sponsorship touch 1 must connect the ask to improving coverage".into());
        }
        if !(lower.contains("data-quality") || lower.contains("data quality")) {
            issues.push("sponsorship touch 1 must name the data-quality briefing".into());
        }
        if !(lower.contains("impact report") || lower.contains("annual report")) {
            issues.push("sponsorship touch 1 must name eventual impact-report inclusion".into());
        }
        if !(lower.contains("someone else")
            && (lower.contains("point me")
                || lower.contains("route")
                || lower.contains("introduc")))
        {
            issues.push(
                "sponsorship touch 1 must request routing if someone else owns the budget".into(),
            );
        }
        let route_ok = match recipient_kind {
            "commercial_sponsor" => {
                lower.contains("budget")
                    && (lower.contains("fit")
                        || lower.contains("oversee")
                        || lower.contains("owns"))
            }
            "electricity_canada_or_industry_association" => lower.contains("introduc"),
            "government" => {
                lower.contains("route")
                    || lower.contains("introduc")
                    || lower.contains("procurement")
                    || lower.contains("program")
            }
            "journalist" => ["commercial", "licensing", "product", "partnership"]
                .iter()
                .any(|term| lower.contains(term)),
            "cloud_or_infrastructure_vendor" => {
                lower.contains("budget")
                    && (lower.contains("fit")
                        || lower.contains("oversee")
                        || lower.contains("owns"))
            }
            _ => false,
        };
        if !route_ok {
            issues.push(format!(
                "sponsorship touch 1 does not follow recipient route `{recipient_kind}`"
            ));
        }
        if matches!(
            recipient_kind,
            "electricity_canada_or_industry_association" | "government" | "journalist"
        ) && (lower.contains("contribute cad") || lower.contains("contribute $"))
        {
            issues.push(format!(
                "recipient route `{recipient_kind}` must not receive a direct cash ask"
            ));
        }
    } else {
        if body.matches('?').count() > 1 {
            issues.push("sponsorship follow-up may ask at most one question".into());
        }
        if lower.contains("following up") || lower.contains("just checking") {
            issues.push("sponsorship follow-up must add decision-useful information".into());
        }
    }
    for phrase in [
        "complete canadian coverage",
        "canada's official",
        "official outage source",
        "supported by",
        "partnered with",
        "approved by",
        "working with",
        "backed by",
        "endorsed by",
        "tax-deductible",
        "tax deductible",
        "will shut down",
        "revolutionizing critical infrastructure",
    ] {
        if lower.contains(phrase) {
            issues.push(format!(
                "sponsorship copy contains prohibited claim `{phrase}`"
            ));
        }
    }
    issues
}

pub fn prepare_sponsorship_pack(
    profile: &BusinessProfile,
    opportunity: &Opportunity,
) -> Result<SponsorshipPack> {
    let sponsorship = profile.sponsorship()?;
    if opportunity.kind != "sponsorship" {
        bail!("a sponsorship pack requires a sponsorship opportunity");
    }
    Ok(SponsorshipPack {
        title: sponsorship.offer_name.clone(),
        price: format!("{} {}", sponsorship.currency, sponsorship.ask_amount_cad),
        infrastructure_need: sponsorship.sponsorship_need.trim().to_string(),
        product_truth: sponsorship.product_truth.clone(),
        sponsor_benefits: sponsorship.permitted_sponsor_benefits.clone(),
        independence_terms: sponsorship.sponsor_independence.clone(),
        buyer_checks: vec![
            format!(
                "Preserve the source proving {} has the route-specific sponsorship, community-investment, resilience, innovation, data-partnership, procurement, licensing, or infrastructure-support program.",
                opportunity.funder
            ),
            "Confirm that the named person owns or can route the evidenced budget/program and record the required vendor/procurement path.".into(),
            "Confirm the configured recognition, API-access, briefing, and eventual impact-report benefits and who will deliver them.".into(),
        ],
        agreement_checks: vec![
            "Enter the 12-month recognition period, documented team-time/infrastructure scope, and payment/support terms; never infer operating runway.".into(),
            "Name the configured benefits and their delivery limits.".into(),
            "Preserve OutageHub's control over outage records, completeness scores, provider treatment, and published conclusions.".into(),
            "Define sponsor acknowledgement, API allowance if any, confidentiality, termination, and out-of-scope work.".into(),
            "Record invoice or credit agreement issued, support received, and every promised benefit as commercial truth events.".into(),
        ],
    })
}

pub async fn prepare_application(
    db: &SharedDb,
    engine: &Engine,
    profile: &BusinessProfile,
    opportunity_id: &str,
) -> Result<ApplicationBrief> {
    let funding = profile.funding()?;
    let opportunity = db
        .get_opportunity(opportunity_id)?
        .ok_or_else(|| anyhow!("opportunity '{opportunity_id}' not found"))?;
    if opportunity.fit_status == "ineligible" {
        bail!("refusing application brief: opportunity is marked ineligible");
    }
    let context = json!({
        "business": profile.name,
        "summary": profile.summary,
        "known_facts": profile.known_facts,
        "unknowns": profile.unknowns,
        "goals": profile.goals,
        "constraints": profile.constraints,
        "funding_objective": funding.objective,
        "project_shapes": funding.project_shapes,
        "opportunity": opportunity,
    });
    let user = format!(
        "Prepare a go/no-go application brief, not a completed or submission-ready application. \
         Separate known evidence from missing evidence. Select one credible bounded project shape; \
         outline work packages and milestones; identify documents, budget questions, funder \
         questions, risks, and ordered next steps. Do not fabricate metrics, partners, finances, \
         eligibility, TRL, jobs, emissions impact, or matching funds. The narrative must label \
         assumptions and missing proof.\n\n{}\n\n{}",
        serde_json::to_string_pretty(&context).unwrap_or_default(),
        core_strategy_block("funding"),
    );
    let draft = engine
        .structured_bulk::<ApplicationDraft>(
            "opportunity.application",
            "You are a conservative non-dilutive funding application strategist. Evidence before claims.",
            &user,
            application_schema(),
        )
        .await?;
    let brief = ApplicationBrief {
        opportunity_id: opportunity_id.to_string(),
        brand: profile.key.clone(),
        status: "draft".into(),
        eligibility_summary: draft.eligibility_summary,
        project_shape: draft.project_shape,
        narrative: draft.narrative,
        workplan: draft.workplan,
        milestones: draft.milestones,
        evidence_needed: draft.evidence_needed,
        required_documents: draft.required_documents,
        budget_questions: draft.budget_questions,
        questions_for_funder: draft.questions_for_funder,
        risks: draft.risks,
        next_steps: draft.next_steps,
        ..Default::default()
    };
    let id = db.upsert_application_brief(&brief)?;
    // Producing a planning brief is not the same as beginning an application.
    // Human replies or an explicit workflow decision advance to `applying`.
    if !matches!(
        opportunity.pipeline_status.as_str(),
        "applying" | "submitted" | "won" | "lost"
    ) {
        db.set_opportunity_pipeline_status(opportunity_id, "shortlisted")?;
    }
    Ok(ApplicationBrief { id, ..brief })
}

pub fn render_opportunities(opportunities: &[Opportunity]) -> String {
    if opportunities.is_empty() {
        return "No opportunities found.".into();
    }
    let mut out = String::new();
    for (index, opportunity) in opportunities.iter().enumerate() {
        let deadline = if opportunity.deadline.is_empty() {
            "deadline not verified".to_string()
        } else {
            format!("deadline {}", opportunity.deadline)
        };
        out.push_str(&format!(
            "{}. {} — {}\n   {} · fit {}/100 ({}) · {}\n   funder: {}\n   next: {}\n   id: {}\n   source: {}\n",
            index + 1,
            opportunity.title,
            opportunity.opportunity_status,
            opportunity.funding_type,
            opportunity.fit_score,
            opportunity.fit_status,
            deadline,
            opportunity.funder,
            opportunity.next_action,
            opportunity.id,
            opportunity.canonical_url,
        ));
    }
    out
}

pub fn is_actionable(opportunity: &Opportunity) -> bool {
    opportunity.fit_status != "ineligible"
        && opportunity.opportunity_status != "closed"
        && !matches!(opportunity.pipeline_status.as_str(), "lost" | "expired")
}

#[cfg(test)]
fn best_org_match<'a>(opportunity: &Opportunity, orgs: &'a [ApolloOrg]) -> Option<&'a ApolloOrg> {
    best_org_match_for(
        &opportunity.funder,
        std::slice::from_ref(&opportunity.funder_domain),
        orgs,
    )
}

fn best_org_match_for<'a>(
    funder: &str,
    domains: &[String],
    orgs: &'a [ApolloOrg],
) -> Option<&'a ApolloOrg> {
    let funder = normalize(funder);
    let domains = domains
        .iter()
        .map(|domain| normalize_domain(domain))
        .filter(|domain| !domain.is_empty())
        .collect::<Vec<_>>();
    orgs.iter()
        .map(|org| {
            let name = normalize(&org.name);
            let mut score = 0;
            if domains.contains(&normalize_domain(&org.domain())) {
                score += 100;
            }
            if !funder.is_empty() && !name.is_empty() && name == funder {
                score += 80;
            } else if !funder.is_empty()
                && !name.is_empty()
                && (name.contains(&funder) || funder.contains(&name))
            {
                score += 40;
            }
            (org, score)
        })
        .filter(|(_, score)| *score > 0)
        .max_by_key(|(_, score)| *score)
        .map(|(org, _)| org)
}

fn interleave_candidate_batches(mut batches: Vec<Vec<Candidate>>) -> Vec<Candidate> {
    let mut merged = Vec::new();
    let max_len = batches.iter().map(Vec::len).max().unwrap_or(0);
    for index in 0..max_len {
        for batch in &mut batches {
            if index < batch.len() {
                merged.push(batch[index].clone());
            }
        }
    }
    merged
}

fn map_apollo_email_status(status: &str) -> String {
    if status.eq_ignore_ascii_case("verified") {
        "verified".into()
    } else if status.trim().is_empty() {
        "unknown".into()
    } else {
        "unverified".into()
    }
}

fn initial_pipeline_status(extracted: &ExtractedOpportunity, fit: &FitAssessment) -> String {
    let status = normalize_opportunity_status(&extracted.opportunity_status);
    if status == "closed" || status == "forecast" {
        return "watching".into();
    }
    if normalize_fit_status(&fit.status) == "ineligible" {
        return "discovered".into();
    }
    if fit.score >= 60 && matches!(status.as_str(), "open" | "rolling") {
        "shortlisted".into()
    } else {
        "discovered".into()
    }
}

fn normalize_opportunity_status(value: &str) -> String {
    match value.trim().to_lowercase().replace('-', "_").as_str() {
        "open" | "forecast" | "rolling" | "closed" => value.trim().to_lowercase(),
        _ => "unknown".into(),
    }
}

fn normalize_fit_status(value: &str) -> String {
    match value.trim().to_lowercase().replace('-', "_").as_str() {
        "strong_fit" | "possible_fit" | "needs_information" | "ineligible" => {
            value.trim().to_lowercase().replace('-', "_")
        }
        _ => "needs_information".into(),
    }
}

fn candidate_source(candidate: &Candidate) -> String {
    candidate
        .reason
        .split(" — ")
        .next()
        .unwrap_or("official web")
        .to_string()
}

fn dedupe_candidates(candidates: &mut Vec<Candidate>) {
    let mut seen = HashSet::new();
    candidates.retain(|candidate| {
        let key = format!(
            "{}|{}",
            candidate.url.trim_end_matches('/').to_lowercase(),
            normalize(&candidate.title)
        );
        seen.insert(key)
    });
}

fn opportunity_fingerprint(brand: &str, funder: &str, title: &str, url: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    normalize(brand).hash(&mut hasher);
    normalize(funder).hash(&mut hasher);
    normalize(title).hash(&mut hasher);
    url.trim_end_matches('/').to_lowercase().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn normalize_domain(value: &str) -> String {
    value
        .trim()
        .trim_start_matches("www.")
        .trim_end_matches('/')
        .to_lowercase()
}

fn domain_from_url(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(normalize_domain))
        .unwrap_or_default()
}

fn host_matches_allowed_domain(url: &str, allowed_domain: &str) -> bool {
    let host = domain_from_url(url);
    let allowed = normalize_domain(allowed_domain)
        .trim_end_matches('.')
        .to_string();
    !host.is_empty()
        && !allowed.is_empty()
        && (host == allowed || host.ends_with(&format!(".{allowed}")))
}

fn validated_research_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value)
        .with_context(|| format!("invalid opportunity research URL: {value}"))?;
    safe_public_http_url(&url)?;
    Ok(url)
}

fn safe_public_http_url(url: &reqwest::Url) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https") {
        bail!("refusing non-http opportunity URL: {url}");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("refusing opportunity URL containing credentials: {url}");
    }
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("opportunity URL has no host: {url}"))?
        .trim_end_matches('.')
        .to_lowercase();
    if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
        bail!("refusing local opportunity URL: {url}");
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if !is_public_ip(ip) {
            bail!("refusing non-public opportunity URL: {url}");
        }
    } else if !host.contains('.') {
        bail!("refusing single-label opportunity host: {url}");
    }
    Ok(())
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => is_public_ipv6(ip),
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.is_multicast()
        || octets[0] == 0
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 198 && matches!(octets[1], 18 | 19)))
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    let octets = ip.octets();
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (octets[0] & 0xfe) == 0xfc
        || (octets[0] == 0xfe && (octets[1] & 0xc0) == 0x80))
}

fn limit_chars(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

fn default_string(value: &str, default: &str) -> String {
    if value.trim().is_empty() {
        default.into()
    } else {
        value.trim().to_string()
    }
}

fn string_array(description: &str) -> Value {
    json!({"type":"array","items":{"type":"string"},"description":description})
}

fn candidate_schema() -> Value {
    json!({
        "type":"object","additionalProperties":false,"required":["candidates"],
        "properties":{"candidates":{"type":"array","items":{
            "type":"object","additionalProperties":false,
            "required":["title","url","funder","reason"],
            "properties":{
                "title":{"type":"string"},"url":{"type":"string"},
                "funder":{"type":"string"},"reason":{"type":"string"}
            }
        }}}
    })
}

fn opportunity_schema() -> Value {
    json!({
        "type":"object","additionalProperties":false,
        "required":["is_opportunity","kind","canonical_url","title","funder","summary",
            "geography","opportunity_status","opens_at","deadline","deadline_timezone",
            "funding_type","amount_min","amount_max","currency","cost_share",
            "eligible_applicants","eligible_activities","ineligible_activities","themes",
            "official_contact_name","official_contact_email","official_contact_phone",
            "evidence","documents"],
        "properties":{
            "is_opportunity":{"type":"boolean"},
            "kind":{"type":"string","enum":["grant","non_repayable_contribution",
                "repayable_contribution","loan","tax_credit","procurement","challenge_prize",
                "funded_pilot","advisory","accelerator","other"]},
            "canonical_url":{"type":"string"},"title":{"type":"string"},
            "funder":{"type":"string"},"summary":{"type":"string"},
            "geography":{"type":"string"},
            "opportunity_status":{"type":"string","enum":["open","forecast","rolling","closed","unknown"]},
            "opens_at":{"type":"string"},"deadline":{"type":"string"},
            "deadline_timezone":{"type":"string"},"funding_type":{"type":"string"},
            "amount_min":{"type":"string"},"amount_max":{"type":"string"},
            "currency":{"type":"string"},"cost_share":{"type":"string"},
            "eligible_applicants":string_array("Explicit eligible applicant criteria."),
            "eligible_activities":string_array("Explicit eligible activity or cost criteria."),
            "ineligible_activities":string_array("Explicit exclusions."),
            "themes":string_array("Programme themes."),
            "official_contact_name":{"type":"string"},
            "official_contact_email":{"type":"string"},
            "official_contact_phone":{"type":"string"},
            "evidence":string_array("Field-labelled evidence from the official page."),
            "documents":string_array("Official application guide, form, FAQ, or terms URLs.")
        }
    })
}

fn fit_schema() -> Value {
    json!({
        "type":"object","additionalProperties":false,
        "required":["score","status","reasons","blockers","unknowns","next_action"],
        "properties":{
            "score":{"type":"integer","minimum":0,"maximum":100},
            "status":{"type":"string","enum":["strong_fit","possible_fit","needs_information","ineligible"]},
            "reasons":string_array("Supported fit reasons only."),
            "blockers":string_array("Explicit mismatches or closed-status blockers."),
            "unknowns":string_array("Mandatory facts that still require evidence."),
            "next_action":{"type":"string"}
        }
    })
}

fn evaluation_schema() -> Value {
    json!({
        "type":"object",
        "additionalProperties":false,
        "required":["opportunity","fit"],
        "properties":{
            "opportunity": opportunity_schema(),
            "fit": fit_schema()
        }
    })
}

fn funding_sequence_schema(n: usize) -> Value {
    json!({
        "type":"object","additionalProperties":false,"required":["touches"],
        "properties":{"touches":{"type":"array","minItems":n,"maxItems":n,"items":{
            "type":"object","additionalProperties":false,
            "required":["stage","day_offset","subject","body","purpose","goal"],
            "properties":{
                "stage":{"type":"integer"},"day_offset":{"type":"integer"},
                "subject":{"type":"string"},"body":{"type":"string"},
                "purpose":{"type":"string"},"goal":{"type":"string"}
            }
        }}}
    })
}

fn sponsorship_review_schema() -> Value {
    json!({
        "type":"object","additionalProperties":false,
        "required":["passes","score","issues","revised_subject","revised_body"],
        "properties":{
            "passes":{"type":"boolean"},
            "score":{"type":"integer","minimum":0,"maximum":100},
            "issues":string_array("Material sendability defects only."),
            "revised_subject":{"type":"string"},
            "revised_body":{"type":"string"}
        }
    })
}

fn application_schema() -> Value {
    json!({
        "type":"object","additionalProperties":false,
        "required":["eligibility_summary","project_shape","narrative","workplan","milestones",
            "evidence_needed","required_documents","budget_questions","questions_for_funder",
            "risks","next_steps"],
        "properties":{
            "eligibility_summary":{"type":"string"},"project_shape":{"type":"string"},
            "narrative":{"type":"string"},
            "workplan":string_array("Ordered work packages."),
            "milestones":string_array("Evidence-producing milestones."),
            "evidence_needed":string_array("Missing facts and proof."),
            "required_documents":string_array("Documents to obtain or prepare."),
            "budget_questions":string_array("Budget and cost-share questions."),
            "questions_for_funder":string_array("Material pre-application questions."),
            "risks":string_array("Eligibility, timing, evidence, delivery, and stacking risks."),
            "next_steps":string_array("Ordered, governed next steps.")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        best_org_match, dedupe_candidates, domain_from_url, host_matches_allowed_domain,
        interleave_candidate_batches, normalized_contains, opportunity_fingerprint,
        sponsorship_contact_score, sponsorship_copy_issues, sponsorship_recipient_kind,
        validated_research_url, Candidate,
    };
    use crate::apollo::ApolloOrg;
    use crate::business::SponsorshipRoute;
    use crate::db::Opportunity;

    #[test]
    fn fingerprints_are_stable_and_brand_scoped() {
        let a = opportunity_fingerprint("outagehub", "IESO", "Grid Fund", "https://ieso.ca/fund/");
        let b = opportunity_fingerprint("outagehub", "IESO", "Grid Fund", "https://ieso.ca/fund");
        let c = opportunity_fingerprint("wapahki", "IESO", "Grid Fund", "https://ieso.ca/fund");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn dedupes_candidates_but_keeps_distinct_programmes_on_catalog_page() {
        let mut candidates = vec![
            Candidate {
                title: "A".into(),
                url: "https://example.ca/a".into(),
                ..Default::default()
            },
            Candidate {
                title: "A".into(),
                url: "https://example.ca/a/".into(),
                ..Default::default()
            },
            Candidate {
                title: "B".into(),
                url: "https://example.ca/a".into(),
                ..Default::default()
            },
        ];
        dedupe_candidates(&mut candidates);
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn extracts_normalized_domain() {
        assert_eq!(domain_from_url("https://www.ieso.ca/path"), "ieso.ca");
    }

    #[test]
    fn allowed_domains_require_an_exact_host_or_real_subdomain() {
        assert!(host_matches_allowed_domain(
            "https://programs.canada.ca/fund",
            "canada.ca"
        ));
        assert!(host_matches_allowed_domain(
            "https://canada.ca/fund",
            "canada.ca"
        ));
        assert!(!host_matches_allowed_domain(
            "https://evilcanada.ca/fund",
            "canada.ca"
        ));
    }

    #[test]
    fn research_urls_reject_local_and_private_targets() {
        assert!(validated_research_url("https://www.canada.ca/fund").is_ok());
        assert!(validated_research_url("file:///etc/passwd").is_err());
        assert!(validated_research_url("http://localhost:8080").is_err());
        assert!(validated_research_url("http://127.0.0.1/private").is_err());
        assert!(validated_research_url("http://[::1]/private").is_err());
        assert!(validated_research_url("http://10.20.30.40/private").is_err());
    }

    #[test]
    fn interleaves_candidates_across_sources() {
        let candidate = |title: &str| Candidate {
            title: title.into(),
            url: format!("https://example.com/{title}"),
            funder: String::new(),
            reason: String::new(),
        };
        let merged = interleave_candidate_batches(vec![
            vec![candidate("a1"), candidate("a2")],
            vec![candidate("b1"), candidate("b2")],
        ]);
        assert_eq!(
            merged.iter().map(|c| c.title.as_str()).collect::<Vec<_>>(),
            vec!["a1", "b1", "a2", "b2"]
        );
    }

    #[test]
    fn apollo_org_match_rejects_unrelated_results() {
        let opportunity = Opportunity {
            funder: "National Research Council Canada".into(),
            funder_domain: "nrc.canada.ca".into(),
            ..Default::default()
        };
        let unrelated = ApolloOrg {
            name: "Example Widgets".into(),
            primary_domain: "example.com".into(),
            ..Default::default()
        };
        assert!(best_org_match(&opportunity, &[unrelated]).is_none());
    }

    #[test]
    fn sponsorship_routes_are_explicit_and_title_bound() {
        assert_eq!(
            sponsorship_recipient_kind("Electricity Canada", "association", "electricity.ca"),
            "electricity_canada_or_industry_association"
        );
        assert_eq!(
            sponsorship_recipient_kind("Daily News", "media", "daily.example"),
            "journalist"
        );
        assert_eq!(
            sponsorship_recipient_kind("Cloud North", "cloud hosting", "cloud.example"),
            "cloud_or_infrastructure_vendor"
        );
        assert_eq!(
            sponsorship_recipient_kind("Intact", "insurance", "intact.ca"),
            "commercial_sponsor"
        );
        let route = SponsorshipRoute {
            recipient_kind: "commercial_sponsor".into(),
            action: "Ask directly".into(),
            target_roles: vec!["Community Investment".into(), "Innovation".into()],
            budget_evidence_terms: vec!["community investment".into()],
        };
        assert!(sponsorship_contact_score("Director, Community Investment", &route).is_some());
        assert!(sponsorship_contact_score("Network Operations Manager", &route).is_none());
    }

    #[test]
    fn evidence_matching_ignores_markup_and_punctuation_boundaries() {
        let page = "As a**Gold Sponsor**, Get Ready Global is proud to participate.";
        assert!(normalized_contains(
            page,
            "As a Gold Sponsor, Get Ready Global is proud to participate"
        ));
    }

    #[test]
    fn sponsorship_copy_gate_enforces_founder_story_and_recipient_route() {
        let body = "Hi Maya,\n\nI’m a U of T student, and over the past year I built OutageHub with a few friends. It already collects monitored Canadian utilities’ public outage reports into a public map and API, and we funded the development ourselves so far. We are seeking CAD $10,000 toward the team time and infrastructure required to keep it operating and improve coverage. We would provide 12 months of founding-sponsor recognition, API access, quarterly coverage and data-quality briefings, and inclusion in the eventual annual impact report; the sponsorship would not influence the data or our conclusions. Could this fit a community-investment budget, or, if someone else owns that area, would you point me toward them?\n\nAndrew Gordienko";
        assert!(sponsorship_copy_issues(
            "OutageHub infrastructure sponsorship",
            body,
            1,
            "commercial_sponsor"
        )
        .is_empty());
        let wrong_route = sponsorship_copy_issues(
            "OutageHub infrastructure sponsorship",
            body,
            1,
            "electricity_canada_or_industry_association",
        );
        assert!(wrong_route
            .iter()
            .any(|issue| issue.contains("recipient route")));
        let endorsement = body.replace(
            "It already collects",
            "It is endorsed by Electricity Canada and already collects",
        );
        assert!(sponsorship_copy_issues(
            "OutageHub infrastructure sponsorship",
            &endorsement,
            1,
            "commercial_sponsor"
        )
        .iter()
        .any(|issue| issue.contains("endorsed by")));
    }
}
