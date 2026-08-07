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
use crate::business::{BusinessProfile, FundingProfile, OpportunitySource};
use crate::calendar::{self, TimingContext};
use crate::db::{ApplicationBrief, Opportunity, OpportunityContact, OpportunityTouch, SharedDb};
use crate::engine::Engine;
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

pub struct FundingOutreachOptions<'a> {
    pub opportunity_id: &'a str,
    pub touches: usize,
    pub auto_schedule: bool,
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
struct FundingSequence {
    #[serde(default)]
    touches: Vec<FundingCopyTouch>,
}

#[derive(Debug, Deserialize)]
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
            .timeout(std::time::Duration::from_secs(45))
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
        let extracted = match extract_opportunity(engine, profile, &candidate, &document).await {
            Ok(x) if x.is_opportunity => x,
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
        summary.opportunities_verified += 1;
        let fit = assess_fit(engine, profile, &extracted)
            .await
            .unwrap_or_else(|e| {
                summary
                    .errors
                    .push(format!("score {}: {e:#}", extracted.title));
                FitAssessment {
                    status: "needs_information".into(),
                    unknowns: profile.unknowns.clone(),
                    next_action: "Review eligibility manually against the official source.".into(),
                    ..Default::default()
                }
            });

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
        .structured::<CandidateBatch>(
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

async fn extract_opportunity(
    engine: &Engine,
    profile: &BusinessProfile,
    candidate: &Candidate,
    document: &str,
) -> Result<ExtractedOpportunity> {
    let today = Utc::now().format("%Y-%m-%d");
    let user = format!(
        "Extract one opportunity from this official page for a structured opportunity database. \
         Today is {today}. Preserve the distinction between grant, non-repayable contribution, \
         repayable contribution, loan, tax credit, procurement, challenge prize, funded pilot, \
         and advisory support. `opportunity_status` must be open, forecast, rolling, closed, or \
         unknown based only on the page. Use ISO dates when the page provides enough information. \
         Every evidence item must identify the field it supports and include source wording or a \
         close paraphrase. If this is only an article or old award announcement with no programme \
         to apply to or monitor, set is_opportunity=false. Never infer eligibility for {name}.\n\n\
         CANDIDATE: {hint}\nFUNDER HINT: {funder_hint}\nURL: {url}\n\nDOCUMENT:\n{document}",
        name = profile.name,
        hint = candidate.title,
        funder_hint = candidate.funder,
        url = candidate.url,
    );
    engine
        .structured::<ExtractedOpportunity>(
            "You are a meticulous funding-opportunity analyst. Extract facts from the supplied official page only.",
            &user,
            opportunity_schema(),
        )
        .await
}

async fn assess_fit(
    engine: &Engine,
    profile: &BusinessProfile,
    opportunity: &ExtractedOpportunity,
) -> Result<FitAssessment> {
    let funding = profile.funding()?;
    let context = json!({
        "business": profile.name,
        "summary": profile.summary,
        "known_facts": profile.known_facts,
        "known_unknowns": profile.unknowns,
        "constraints": profile.constraints,
        "funding_objective": funding.objective,
        "themes": funding.themes,
        "project_shapes": funding.project_shapes,
        "opportunity": opportunity,
    });
    let user = format!(
        "Assess fit using only known_facts as proven facts. A known_unknown is not evidence. \
         Mandatory criteria that depend on unknown information must produce needs_information, \
         not strong_fit. An explicit mismatch produces ineligible. Score 0-100 for prioritization, \
         list supported reasons separately from blockers and unknowns, and give one concrete next \
         action. Do not confuse thematic relevance with applicant eligibility.\n\n{}",
        serde_json::to_string_pretty(&context).unwrap_or_default()
    );
    engine
        .structured::<FitAssessment>(
            "You are a conservative grant eligibility reviewer. Never claim eligibility without evidence.",
            &user,
            fit_schema(),
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
         contribution or tax credit a grant. Do not claim eligibility.\n\nDOCTRINE:\n{doctrine}\n\nCONTEXT:\n{context}",
        min = funding.min_words,
        max = funding.max_words,
        signature = signature,
        doctrine = funding.doctrine,
        context = serde_json::to_string_pretty(&context).unwrap_or_default(),
    );
    engine
        .structured::<FundingSequence>(
            "You write precise founder-led pre-application funding enquiries grounded in official criteria.",
            &user,
            funding_sequence_schema(n),
        )
        .await
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
         assumptions and missing proof.\n\n{}",
        serde_json::to_string_pretty(&context).unwrap_or_default()
    );
    let draft = engine
        .structured::<ApplicationDraft>(
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
        interleave_candidate_batches, opportunity_fingerprint, validated_research_url, Candidate,
    };
    use crate::apollo::ApolloOrg;
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
}
