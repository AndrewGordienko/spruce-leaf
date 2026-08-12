//! Sourcing: turn a thesis into *real* leads and people from Apollo.
//!
//! The original pipeline asked Claude to invent companies. This asks Claude only
//! to (a) translate the thesis into an Apollo ICP query and (b) *qualify and
//! frame* the real rows Apollo returns — it never invents the company or the
//! person. The doctrine (observed fact vs. inference vs. hypothesis) is enforced
//! by grounding every fact in the Apollo payload we pass in.
//!
//! Flow:
//!   1. derive_icp  — thesis + brand motion → Apollo filters (keywords, sizes,
//!      locations, titles, seniorities)
//!   2. org search  — real companies
//!   3. qualify     — per org, Claude decides fit and writes the doctrine fields
//!      grounded ONLY in the org's real facts
//!   4. people search + vantage — real people at the org, mapped to a vantage
//!   5. upsert into the SQLite spine

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::apollo::{Apollo, ApolloOrg, ApolloPerson, OrgFilters, PeopleFilters};
use crate::calendar;
use crate::db::{AccountPlayAssessment, CoverageRun, Lead, MarketSegment, Person, SharedDb};
use crate::engine::Engine;
use crate::gtm::SignalCandidate;
use crate::knowledge::{core_strategy_block, Library};
use crate::opportunity::ResearchClient;
use crate::playbook::{Playbook, Shared};
use crate::research;

/// How many candidates to source per company relative to the requested contact
/// count, so unverifiable people can be backfilled by a different contact. Costs
/// extra enrichment credits (each filed candidate is a reveal) but keeps
/// companies from ending up short of verified, sequenceable contacts.
const CONTACT_BACKFILL_FACTOR: usize = 2;
const QUALIFICATION_POLICY_TAG: &str = "qv29";
const PORTFOLIO_REUSE_MARKER: &str = "__spruce_portfolio_reuse__";

/// What one `source` run accomplished.
#[derive(Debug, Default)]
pub struct SourceSummary {
    pub orgs_found: usize,
    /// Previously unseen Apollo organizations that actually reached qualification.
    /// Distinguishes a useful miss from an exhausted/repeating search page.
    pub candidates_new: usize,
    pub leads_qualified: usize,
    pub leads_research_needed: usize,
    pub leads_research_required: usize,
    pub people_added: usize,
}

/// One stable row in the interactive sourcing transcript. The sourcing layer
/// reports structured milestones instead of printing ad-hoc lines while a
/// spinner is repainting the terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceProgressUpdate {
    pub key: String,
    pub title: String,
    pub detail: String,
    /// queued | active | complete | warning | failed
    pub status: String,
}

pub type SourceProgressReporter = Arc<dyn Fn(SourceProgressUpdate) + Send + Sync>;

fn report_source(
    reporter: Option<&SourceProgressReporter>,
    key: &str,
    title: impl Into<String>,
    detail: impl Into<String>,
    status: &str,
) {
    if let Some(reporter) = reporter {
        reporter(SourceProgressUpdate {
            key: key.into(),
            title: title.into(),
            detail: detail.into(),
            status: status.into(),
        });
    }
}

fn log_sourcing(message: impl AsRef<str>) {
    if !crate::ui::fancy() {
        eprintln!("  · {}", message.as_ref());
    }
}

/// On-file inventory selected for a reuse-first motion (no Apollo required).
#[derive(Debug, Default, Clone)]
pub struct ReuseSelection {
    pub lead_ids: Vec<String>,
    pub person_ids: std::collections::HashSet<String>,
    pub accounts_on_file: usize,
    pub people_on_file: usize,
    pub verified_on_file: usize,
    pub accounts_selected: usize,
    pub people_selected: usize,
    pub verified_selected: usize,
    /// How many additional accounts still need Apollo if the operator asked for more.
    pub accounts_shortfall: usize,
}

/// Doctrine fields rewritten for an already-qualified lead (no re-qualification).
#[derive(Debug, Default, Deserialize)]
struct LeadRefresh {
    #[serde(default)]
    observed_facts: Vec<String>,
    #[serde(default)]
    inferences: Vec<String>,
    #[serde(default)]
    hypothesis: String,
    #[serde(default)]
    mechanism: String,
    #[serde(default)]
    consequence_metric: String,
    #[serde(default)]
    signals: Vec<String>,
    #[serde(default)]
    structured_signals: Vec<SignalCandidate>,
    #[serde(default)]
    play_fit_score: i64,
    #[serde(default)]
    matched_signal_keys: Vec<String>,
    #[serde(default)]
    symptom: String,
    #[serde(default)]
    root_cause: String,
    #[serde(default)]
    current_workaround: String,
    #[serde(default)]
    why_now: String,
    #[serde(default)]
    proof_fit: String,
    #[serde(default)]
    evidence_gaps: Vec<String>,
    #[serde(default)]
    disqualifiers: Vec<String>,
    #[serde(default)]
    system_concept: String,
    #[serde(default)]
    hard_buyer_question: String,
    #[serde(default)]
    kill_condition: String,
    #[serde(default)]
    magnitude_note: String,
    #[serde(default)]
    applied_principles: Vec<String>,
    /// One sentence: why this specific company is worth a sequence now.
    #[serde(default)]
    why_this_company: String,
}

// --- Structured-output shapes ---------------------------------------------

#[derive(Debug, Deserialize)]
struct Icp {
    #[serde(default)]
    keywords: Vec<String>,
    #[serde(default)]
    employee_ranges: Vec<String>,
    #[serde(default)]
    locations: Vec<String>,
    #[serde(default)]
    titles: Vec<String>,
    #[serde(default)]
    seniorities: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct OrgQual {
    /// True only for fully qualified accounts. Research-needed inventory is
    /// preserved with this false and an explicit routing_status.
    qualified: bool,
    /// qualified (easy) | research_needed (medium) | research_required (hard)
    /// | rejected. Computed locally after the model
    /// returns so missing public evidence is not confused with negative evidence.
    #[serde(skip)]
    routing_status: String,
    #[serde(default)]
    reject_reason: String,
    #[serde(default)]
    observed_facts: Vec<String>,
    #[serde(default)]
    inferences: Vec<String>,
    #[serde(default)]
    hypothesis: String,
    #[serde(default)]
    mechanism: String,
    #[serde(default)]
    consequence_metric: String,
    #[serde(default)]
    signals: Vec<String>,
    #[serde(default)]
    structured_signals: Vec<SignalCandidate>,
    #[serde(default)]
    play_fit_score: i64,
    #[serde(default)]
    matched_signal_keys: Vec<String>,
    #[serde(default)]
    symptom: String,
    #[serde(default)]
    root_cause: String,
    #[serde(default)]
    current_workaround: String,
    #[serde(default)]
    why_now: String,
    #[serde(default)]
    proof_fit: String,
    #[serde(default)]
    evidence_gaps: Vec<String>,
    #[serde(default)]
    disqualifiers: Vec<String>,
    #[serde(default)]
    system_concept: String,
    #[serde(default)]
    hard_buyer_question: String,
    #[serde(default)]
    kill_condition: String,
    #[serde(default)]
    magnitude_note: String,
    #[serde(default)]
    applied_principles: Vec<String>,
    /// Diagnostic-only source lineage captured by this research pass.
    #[serde(skip)]
    research_sources: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct VantageDoc {
    #[serde(default)]
    assignments: Vec<VantageAssignment>,
}

#[derive(Debug, Deserialize)]
struct VantageAssignment {
    index: usize,
    #[serde(default)]
    vantage: String,
    #[serde(default)]
    can_observe: String,
    #[serde(default)]
    why_them: String,
    #[serde(default)]
    primary: bool,
    #[serde(default)]
    route_to: String,
}

/// Run the full sourcing pass and file everything into the db.
#[allow(clippy::too_many_arguments)]
pub async fn source(
    db: &SharedDb,
    client: &Engine,
    apollo: &Apollo,
    pb: &Playbook,
    _shared: &Shared,
    fallback_recipient_timezone: &str,
    business_context: &str,
    library: &Library,
    thesis: &str,
    n_accounts: usize,
    n_contacts: usize,
    exact_domains: Option<&[String]>,
    candidate_limit: Option<usize>,
    concurrency: usize,
    progress: Option<SourceProgressReporter>,
) -> Result<SourceSummary> {
    let explicit_domain_rerun =
        exact_domains.is_some_and(|domains| domains.iter().any(|domain| !domain.trim().is_empty()));
    // Each sourcing stage gets only the rules it needs; buyer-facing copy
    // doctrine is intentionally absent from ICP, qualification, and routing.
    let icp_system = pb.icp_system_prompt();
    let qualification_system = pb.qualification_system_prompt();
    let vantage_system = pb.vantage_system_prompt();
    let active_play = db.current_gtm_play(&pb.key)?;
    let gtm_play_context = crate::gtm::sourcing_play_block(active_play.as_ref());
    let allowed_signal_keys = db
        .list_signal_definitions(Some(&pb.key))?
        .into_iter()
        .filter(|definition| definition.status == "active")
        .map(|definition| definition.key)
        .collect::<HashSet<_>>();

    // Fold what we've already learned about this brand's outbound into the context
    // every sourcing stage sees, so each run builds on prior runs instead of
    // starting from a clean state (the operator's explicit ask).
    // Explicit domains are often rerun because the operator supplied a new,
    // current evidence page. Do not prime that reassessment with the old
    // account-specific rejection; cross-account patterns still apply below.
    let mut skip_learnings = if explicit_domain_rerun {
        Vec::new()
    } else {
        db.recent_learnings(Some(&pb.key), Some("qualification_skip"), 15)
            .unwrap_or_default()
    };
    skip_learnings.retain(|learning| {
        learning
            .detail
            .starts_with(&format!("{QUALIFICATION_POLICY_TAG} "))
    });
    skip_learnings.extend(
        db.recent_learnings(Some(&pb.key), Some("qualification_pattern"), 8)
            .unwrap_or_default(),
    );
    skip_learnings.extend(
        db.recent_learnings(Some(&pb.key), Some("contact_search_pattern"), 4)
            .unwrap_or_default(),
    );
    let augmented_context = augment_context_with_learnings(business_context, &skip_learnings);

    // 1. thesis → Apollo ICP filters, then hard-clamp sizes to the brand's ceiling
    //    so enterprise giants are never even fetched (belt-and-suspenders vs. the prompt).
    report_source(
        progress.as_ref(),
        "icp",
        "Building ICP",
        "Applying the active GTM play, business constraints, and prior qualification learnings",
        "active",
    );
    let mut icp = derive_icp(
        client,
        &icp_system,
        pb,
        &augmented_context,
        &gtm_play_context,
        thesis,
    )
    .await?;
    apply_brand_icp_guard(&pb.key, thesis, &mut icp);
    icp.employee_ranges = clamp_employee_ranges(icp.employee_ranges, pb.max_employees);
    log_sourcing(format!(
        "{} ICP · keywords [{}] · locations [{}] · titles [{}]",
        pb.key,
        compact_list(&icp.keywords, 10),
        compact_list(&icp.locations, 6),
        compact_list(&icp.titles, 6),
    ));
    report_source(
        progress.as_ref(),
        "icp",
        "Built ICP",
        format!(
            "{} keywords · {} buyer titles · {} size bands\nTop titles: {}\nEmployee ranges: {}",
            icp.keywords.len(),
            icp.titles.len(),
            icp.employee_ranges.len(),
            compact_list(&icp.titles, 4),
            compact_list(&icp.employee_ranges, 8),
        ),
        "complete",
    );

    let available_segments = db.list_market_segments(Some(&pb.key)).unwrap_or_default();
    let market_segment = choose_source_segment(&pb.key, thesis, &icp, &available_segments);
    let query_fingerprint = source_query_fingerprint(
        &pb.key,
        market_segment
            .as_ref()
            .map(|segment| segment.key.as_str())
            .unwrap_or("unsegmented"),
        &icp,
    );
    let prior_coverage = market_segment.as_ref().and_then(|segment| {
        db.list_coverage_runs(Some(&pb.key))
            .ok()?
            .into_iter()
            .find(|run| {
                run.segment_id == segment.id
                    && run.source_name == "apollo"
                    && run.query_fingerprint == query_fingerprint
            })
    });

    // 2. Real organizations (overfetch so qualification can be selective). Reuse
    // across runs: never re-qualify a company we've already judged. Rejects live
    // in the learning store; companies we already qualified are Lead rows on file.
    // Combining both lets `source` spend its qualification budget only on genuinely
    // NEW companies — and page deeper when a page is all-known, so a re-run ADDS
    // leads instead of stalling on the same top results.
    const DEFAULT_MAX_SOURCE_PAGES: u32 = 50;
    let exact_domains = exact_domains
        .unwrap_or_default()
        .iter()
        .map(|domain| {
            domain
                .trim()
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_start_matches("www.")
                .trim_end_matches('/')
                .to_ascii_lowercase()
        })
        .filter(|domain| !domain.is_empty())
        .collect::<Vec<_>>();
    // Exact-domain research commonly points at a company that another Spruce
    // Leaf brand already knows. A canonical company may support several
    // independent opportunities, so reuse its firmographics before asking an
    // external provider to rediscover it. Qualification still runs separately
    // for the target brand and active play.
    let portfolio_leads = db.list_leads(None).unwrap_or_default();
    let portfolio_orgs = portfolio_orgs_for_exact_domains(&portfolio_leads, &exact_domains);
    let portfolio_domains = portfolio_orgs
        .iter()
        .map(|org| canonical_company_domain(&org.domain()))
        .collect::<HashSet<_>>();
    let provider_domains = exact_domains
        .iter()
        .filter(|domain| !portfolio_domains.contains(*domain))
        .cloned()
        .collect::<Vec<_>>();
    let portfolio_reused = portfolio_orgs.len();
    let durable_skip_keys = db
        .durable_qualification_skip_keys(&pb.key, QUALIFICATION_POLICY_TAG)
        .unwrap_or_default()
        .into_iter()
        .collect();
    let mut seen = qualification_skip_keys_for_run(&exact_domains, durable_skip_keys);
    let on_file = db.list_leads(Some(&pb.key)).unwrap_or_default();
    for lead in &on_file {
        for key in lead_identity_keys(lead) {
            seen.insert(key);
        }
    }

    let want_candidates = source_candidate_target(n_accounts, candidate_limit);
    let per_page = want_candidates.min(100) as u32;
    report_source(
        progress.as_ref(),
        "apollo",
        if portfolio_reused > 0 {
            "Reusing portfolio accounts"
        } else {
            "Searching Apollo"
        },
        if portfolio_reused > 0 {
            format!(
                "{portfolio_reused} exact-domain accounts already known · {} remaining provider lookups",
                provider_domains.len()
            )
        } else {
            format!(
                "Looking for about {want_candidates} candidates across a resumable market sweep"
            )
        },
        "active",
    );
    let mut fresh: Vec<ApolloOrg> = portfolio_orgs;
    let mut fresh_keys = fresh
        .iter()
        .flat_map(org_identity_keys)
        .filter(|key| key.starts_with("domain:"))
        .collect::<HashSet<_>>();
    let mut orgs_found = portfolio_reused;
    let mut skipped_known = 0usize;
    let max_source_pages = std::env::var("SPRUCE_MAX_SOURCE_PAGES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(DEFAULT_MAX_SOURCE_PAGES)
        .clamp(1, 500);
    let source_pages = if exact_domains.is_empty() {
        max_source_pages
    } else if provider_domains.is_empty() {
        0
    } else {
        1
    };
    let start_page = if exact_domains.is_empty() {
        prior_coverage
            .as_ref()
            .filter(|run| !run.exhausted)
            .and_then(|run| run.cursor.parse::<u32>().ok())
            .unwrap_or(1)
    } else {
        1
    };
    let mut last_page = start_page.saturating_sub(1);
    let mut source_exhausted = (explicit_domain_rerun && provider_domains.is_empty())
        || prior_coverage.as_ref().is_some_and(|run| run.exhausted);
    for page in start_page..start_page.saturating_add(source_pages) {
        if source_exhausted && exact_domains.is_empty() {
            break;
        }
        let page_orgs = match apollo
            .search_organizations(&OrgFilters {
                keywords: if exact_domains.is_empty() {
                    icp.keywords.clone()
                } else {
                    Vec::new()
                },
                domains: provider_domains.clone(),
                employee_ranges: if exact_domains.is_empty() {
                    icp.employee_ranges.clone()
                } else {
                    Vec::new()
                },
                locations: if exact_domains.is_empty() {
                    icp.locations.clone()
                } else {
                    Vec::new()
                },
                page,
                per_page,
                ..Default::default()
            })
            .await
        {
            Ok(orgs) => orgs,
            Err(error) if explicit_domain_rerun && !fresh.is_empty() => {
                report_source(
                    progress.as_ref(),
                    "apollo",
                    "Provider coverage incomplete",
                    format!(
                        "Continuing with {portfolio_reused} portfolio accounts; {} exact domains remain unresolved · {}",
                        provider_domains.len(),
                        first_line(&error.to_string())
                    ),
                    "warning",
                );
                break;
            }
            Err(error) => return Err(error),
        };
        last_page = page;
        if page_orgs.is_empty() {
            source_exhausted = true;
            break;
        }
        if page_orgs.len() < per_page as usize {
            source_exhausted = true;
        }
        orgs_found += page_orgs.len();
        for org in page_orgs {
            let keys = org_identity_keys(&org);
            let learning_key = org_learning_key(&org);
            let revisit_same_brand = explicit_domain_rerun;
            // Skip anything already judged; `insert` returning false also guards
            // the same company reappearing across pages. An explicit same-brand
            // domain rerun is allowed to reassess the account and rebuild its
            // localized contact bench after new evidence arrives.
            let already_seen = (!learning_key.is_empty() && seen.contains(&learning_key))
                || keys.iter().any(|key| seen.contains(key));
            if already_seen && !revisit_same_brand {
                skipped_known += 1;
                continue;
            }
            let fresh_key = keys
                .iter()
                .find(|key| key.starts_with("domain:"))
                .cloned()
                .unwrap_or_else(|| learning_key.clone());
            if !fresh_key.is_empty() && !fresh_keys.insert(fresh_key) {
                skipped_known += 1;
                continue;
            }
            if !learning_key.is_empty() {
                seen.insert(learning_key);
            }
            seen.extend(keys);
            fresh.push(org);
        }
        if fresh.len() >= want_candidates {
            break;
        }
    }
    if let Some(segment) = &market_segment {
        let previous_pages = prior_coverage
            .as_ref()
            .map(|run| run.pages_examined)
            .unwrap_or(0);
        let previous_seen = prior_coverage
            .as_ref()
            .map(|run| run.candidates_seen)
            .unwrap_or(0);
        let next_cursor = if source_exhausted {
            String::new()
        } else {
            last_page.saturating_add(1).to_string()
        };
        let pages_this_run = if last_page >= start_page {
            i64::from(last_page - start_page + 1)
        } else {
            0
        };
        let _ = db.upsert_coverage_run(&CoverageRun {
            segment_id: segment.id.clone(),
            brand: pb.key.clone(),
            source_name: "apollo".into(),
            query_fingerprint: query_fingerprint.clone(),
            cursor: next_cursor,
            pages_examined: previous_pages + pages_this_run,
            candidates_seen: previous_seen + orgs_found as i64,
            accounts_added: prior_coverage
                .as_ref()
                .map(|run| run.accounts_added)
                .unwrap_or(0),
            status: if source_exhausted { "complete" } else { "partial" }.into(),
            exhausted: source_exhausted,
            gap_reason: if source_exhausted {
                String::new()
            } else {
                "Apollo sweep paused after reaching the current research budget; resume from persisted page cursor.".into()
            },
            started_at: prior_coverage
                .as_ref()
                .map(|run| run.started_at.clone())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| Utc::now().to_rfc3339()),
            completed_at: if source_exhausted {
                Utc::now().to_rfc3339()
            } else {
                String::new()
            },
            ..Default::default()
        });
    }
    report_source(
        progress.as_ref(),
        "apollo",
        "Found candidate companies",
        {
            format!(
                "{orgs_found} returned · {} new to qualify · {skipped_known} already judged for this brand · {} on file",
                fresh.len(),
                on_file.len(),
            )
        },
        "complete",
    );
    let mut summary = SourceSummary {
        orgs_found,
        candidates_new: fresh.len(),
        ..Default::default()
    };

    // Per-company website research (best-effort): grounds qualification and the
    // opening touch in what the company actually does, not Apollo's one-liner.
    // Toggle off with SPRUCE_RESEARCH=0.
    let researcher = if research::enabled() {
        match ResearchClient::from_env() {
            Ok(r) => {
                report_source(
                    progress.as_ref(),
                    "research",
                    "Official-site research enabled",
                    "Reading each company's website before judging workflow fit",
                    "complete",
                );
                Some(r)
            }
            Err(_) => {
                report_source(
                    progress.as_ref(),
                    "research",
                    "Official-site research unavailable",
                    "Qualifying from Apollo facts and stored evidence only",
                    "warning",
                );
                None
            }
        }
    } else {
        report_source(
            progress.as_ref(),
            "research",
            "Official-site research disabled",
            "SPRUCE_RESEARCH=0 · qualifying from Apollo facts and stored evidence",
            "warning",
        );
        None
    };
    let researcher_ref = researcher.as_ref();

    // 3. Qualify the bounded overfetch, then rank it. Stopping at the first N
    // model-approved Apollo rows made result order masquerade as strategy; the
    // active versioned play now decides which requested N survive.
    let retrieved = library
        .retrieve_stage(
            &format!("qualifying an expensive workflow: {thesis}; {}", pb.motion),
            "companies",
            3,
            1,
        )
        .playbook_block();
    let knowledge = format!("{}\n\n{}", core_strategy_block("companies"), retrieved);

    let qualification_total = fresh.len();
    report_source(
        progress.as_ref(),
        "qualification",
        "Qualifying root-cause fit",
        format!(
            "0/{qualification_total} reviewed · testing signals, root cause, reachable buyer, and bounded-proof fit"
        ),
        "active",
    );
    let mut quals = Vec::new();
    // Website reads are I/O-bound, but every candidate ends in a substantive
    // structured-model call. Forcing eight concurrent local CLI processes made
    // evidence-rich candidates fail while thin candidates returned quickly.
    let batch_size = concurrency.clamp(1, 4);
    let mut results = stream::iter(fresh.into_iter().map(|org| {
        let system = qualification_system.clone();
        let knowledge = knowledge.clone();
        let business_context = augmented_context.clone();
        let gtm_play_context = gtm_play_context.clone();
        let active_play = active_play.clone();
        let allowed_signal_keys = allowed_signal_keys.clone();
        async move {
            qualify_candidate(
                client,
                apollo,
                researcher_ref,
                pb,
                &system,
                &business_context,
                &gtm_play_context,
                active_play.as_ref(),
                &allowed_signal_keys,
                thesis,
                org,
                &knowledge,
            )
            .await
        }
    }))
    .buffer_unordered(batch_size);
    let mut reviewed = 0usize;
    let mut qualified = 0usize;
    let mut research_needed = 0usize;
    let mut research_required = 0usize;
    let mut skipped = 0usize;
    let mut errors = 0usize;
    while let Some((org, result)) = results.next().await {
        reviewed += 1;
        let latest = match &result {
            Ok(value) if value.routing_status == "qualified" => {
                qualified += 1;
                format!(
                    "Latest: passed {} · fit {}/100",
                    org.name, value.play_fit_score
                )
            }
            Ok(value) if value.routing_status == "research_needed" => {
                research_needed += 1;
                format!(
                    "Latest: research needed {} · fit {}/100",
                    org.name, value.play_fit_score
                )
            }
            Ok(value) if value.routing_status == "research_required" => {
                research_required += 1;
                format!(
                    "Latest: hard-priority research {} · fit {}/100",
                    org.name, value.play_fit_score
                )
            }
            Ok(value) => {
                skipped += 1;
                let classification = if value.disqualifiers.is_empty() {
                    "insufficient_fit"
                } else {
                    "hard_reject"
                };
                let _ = db.record_learning(
                    &pb.key,
                    "qualification_skip",
                    &org.name,
                    &org_learning_key(&org),
                    &qualification_skip_detail(classification, value),
                );
                format!(
                    "Latest: skipped {} · {}",
                    org.name,
                    first_line(&value.reject_reason)
                )
            }
            Err(error) => {
                errors += 1;
                log_sourcing(format!(
                    "qualification error {} · {}",
                    org.name,
                    first_line(&error.to_string())
                ));
                let _ = db.record_learning(
                    &pb.key,
                    "qualification_error",
                    &org.name,
                    &org_learning_key(&org),
                    &format!("{QUALIFICATION_POLICY_TAG}: {error}"),
                );
                format!(
                    "Latest: error for {} · {}",
                    org.name,
                    first_line(&error.to_string())
                )
            }
        };
        report_source(
            progress.as_ref(),
            "qualification",
            "Qualifying root-cause fit",
            format!(
                "{reviewed}/{qualification_total} reviewed · {qualified} easy · {research_needed} medium · {research_required} hard research · {skipped} rejected · {errors} errors\n{latest}"
            ),
            "active",
        );
        quals.push((org, result));
    }

    report_source(
        progress.as_ref(),
        "learning",
        "Learning from qualification misses",
        "Turning repeated rejection causes into tighter filters for the next sourcing run",
        "active",
    );
    let learned_patterns = record_qualification_patterns(db, pb, &quals);
    report_source(
        progress.as_ref(),
        "learning",
        if learned_patterns == 0 {
            "Qualification learning checked"
        } else {
            "Saved targeting corrections"
        },
        if learned_patterns == 0 {
            "No recurring failure pattern was strong enough to persist"
        } else {
            "Stored corrections for missing signals, weak root-cause evidence, and hard disqualifiers; the next ICP build will apply them"
        },
        "complete",
    );

    let mut ranked = quals
        .into_iter()
        .filter_map(|(org, result)| match result {
            Ok(qualification)
                if matches!(
                    qualification.routing_status.as_str(),
                    "qualified" | "research_needed" | "research_required"
                ) =>
            {
                Some((org, qualification))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        routing_priority(&left.1.routing_status)
            .cmp(&routing_priority(&right.1.routing_status))
            .then_with(|| right.1.play_fit_score.cmp(&left.1.play_fit_score))
            .then_with(|| {
                right
                    .1
                    .structured_signals
                    .len()
                    .cmp(&left.1.structured_signals.len())
            })
            .then_with(|| {
                right
                    .1
                    .observed_facts
                    .len()
                    .cmp(&left.1.observed_facts.len())
            })
            .then_with(|| left.0.name.cmp(&right.0.name))
    });
    let selected_count = ranked.len().min(n_accounts);
    let selected_qualified = ranked
        .iter()
        .take(selected_count)
        .filter(|(_, qualification)| qualification.routing_status == "qualified")
        .count();
    let selected_medium = ranked
        .iter()
        .take(selected_count)
        .filter(|(_, qualification)| qualification.routing_status == "research_needed")
        .count();
    let selected_hard = selected_count
        .saturating_sub(selected_qualified)
        .saturating_sub(selected_medium);
    report_source(
        progress.as_ref(),
        "qualification",
        if selected_medium + selected_hard == 0 {
            "Ranked qualified companies"
        } else {
            "Ranked easy, medium, and hard-priority companies"
        },
        format!(
            "{reviewed}/{qualification_total} reviewed · {qualified} easy · {research_needed} medium · {research_required} hard research\nSelected {selected_count}: {selected_qualified} easy · {selected_medium} medium · {selected_hard} hard"
        ),
        if selected_count > 0 { "complete" } else { "warning" },
    );

    // Persist the highest-ranked leads and their play-version assessment, then
    // source people only at those winners.
    let mut winners: Vec<(ApolloOrg, Lead, String)> = Vec::new();
    for (org, q) in ranked.into_iter().take(n_accounts) {
        let routing_status = if q.routing_status.is_empty() {
            "research_needed".to_string()
        } else {
            q.routing_status.clone()
        };
        let structured_signals = q.structured_signals.clone();
        let assessment = active_play.as_ref().map(|play| AccountPlayAssessment {
            brand: pb.key.clone(),
            play_id: play.id.clone(),
            play_version: play.version,
            status: routing_status.clone(),
            fit_score: q.play_fit_score,
            matched_signal_keys: q.matched_signal_keys.clone(),
            symptom: q.symptom.clone(),
            root_cause: q.root_cause.clone(),
            current_workaround: q.current_workaround.clone(),
            why_now: q.why_now.clone(),
            proof_fit: q.proof_fit.clone(),
            evidence_gaps: q.evidence_gaps.clone(),
            disqualifiers: q.disqualifiers.clone(),
            source: "source.qualify".into(),
            ..Default::default()
        });

        let hq = org.hq();
        let lead = Lead {
            brand: pb.key.clone(),
            apollo_org_id: org.id.clone(),
            name: org.name.clone(),
            domain: org.domain(),
            industry: org.industry.clone(),
            timezone: calendar::timezone_for_location(&hq, fallback_recipient_timezone)
                .name()
                .to_string(),
            hq,
            headcount: org.estimated_num_employees,
            revenue: org.annual_revenue_printed.clone(),
            thesis: thesis.to_string(),
            hypothesis: q.hypothesis,
            mechanism: q.mechanism,
            consequence_metric: q.consequence_metric,
            system_concept: q.system_concept,
            hard_buyer_question: q.hard_buyer_question,
            kill_condition: q.kill_condition,
            observed_facts: q.observed_facts,
            inferences: q.inferences,
            signals: q.signals,
            magnitude_note: q.magnitude_note,
            applied_principles: q.applied_principles,
            status: routing_status.clone(),
            ..Default::default()
        };
        let lead_id = db.upsert_lead(&lead)?;
        db.record_signal_candidates(&pb.key, &lead_id, &structured_signals, "source.qualify")?;
        if let Some(mut assessment) = assessment {
            assessment.lead_id = lead_id.clone();
            db.upsert_account_play_assessment(&assessment)?;
        }
        db.log_event(
            &pb.key,
            "",
            "",
            "sourced",
            &format!(
                "{} lead {} (play fit {}/100; ranked by evidence + root cause)",
                routing_status, org.name, q.play_fit_score
            ),
        )?;
        if routing_status == "qualified" {
            summary.leads_qualified += 1;
        } else if routing_status == "research_needed" {
            summary.leads_research_needed += 1;
        } else if routing_status == "research_required" {
            summary.leads_research_required += 1;
        }
        winners.push((org, lead, lead_id));
    }
    if let Some(segment) = &market_segment {
        if let Some(mut coverage) = db
            .list_coverage_runs(Some(&pb.key))
            .unwrap_or_default()
            .into_iter()
            .find(|run| {
                run.segment_id == segment.id
                    && run.source_name == "apollo"
                    && run.query_fingerprint == query_fingerprint
            })
        {
            coverage.accounts_added += winners.len() as i64;
            let _ = db.upsert_coverage_run(&coverage);
        }
    }

    // 4. Real people at each org, mapped to vantage points. Every org is
    // independent (its own Apollo people search + one vantage call), so fan them
    // out concurrently — walking them one-at-a-time was the serialized tail of a
    // sourcing run.
    let vantage_system = &vantage_system;
    let icp = &icp;
    let people_total = winners.len();
    report_source(
        progress.as_ref(),
        "contacts",
        "Mapping buyer contacts",
        format!(
            "0/{people_total} companies mapped · targeting up to {n_contacts} useful vantage points per company"
        ),
        if people_total == 0 { "warning" } else { "active" },
    );
    let mut people_counts =
        stream::iter(winners.into_iter().map(|(org, lead, lead_id)| async move {
            let reused = reuse_portfolio_people(
                db,
                &pb.key,
                &lead_id,
                &lead.domain,
                n_contacts.saturating_mul(CONTACT_BACKFILL_FACTOR).max(8),
            );
            let result = match reused {
                Err(error) => Err(error),
                Ok(reused) if reused >= n_contacts || (explicit_domain_rerun && reused > 0) => {
                    Ok(reused)
                }
                Ok(reused) => source_people(
                    db,
                    client,
                    apollo,
                    pb,
                    vantage_system,
                    &org,
                    &lead,
                    &lead_id,
                    icp,
                    n_contacts.saturating_sub(reused),
                    fallback_recipient_timezone,
                )
                .await
                .map(|added| reused + added),
            };
            (org.name, result)
        }))
        .buffer_unordered(concurrency.max(4));
    let mut companies_mapped = 0usize;
    let mut contact_errors = 0usize;
    let mut contact_shortfalls = 0usize;
    while let Some((name, result)) = people_counts.next().await {
        companies_mapped += 1;
        match result {
            Ok(added) => {
                summary.people_added += added;
                if added < n_contacts {
                    contact_shortfalls += 1;
                }
                report_source(
                    progress.as_ref(),
                    "contacts",
                    "Mapping buyer contacts",
                    format!(
                        "{companies_mapped}/{people_total} companies mapped · {} contacts filed\nLatest: {name} · {added} contacts",
                        summary.people_added,
                    ),
                    "active",
                );
            }
            Err(error) => {
                contact_errors += 1;
                report_source(
                    progress.as_ref(),
                    "contacts",
                    "Mapping buyer contacts",
                    format!(
                        "{companies_mapped}/{people_total} companies mapped · {} contacts filed · {contact_errors} errors\nLatest: {name} · {}",
                        summary.people_added,
                        first_line(&error.to_string()),
                    ),
                    "active",
                );
            }
        }
    }
    if contact_shortfalls > 0 {
        let detail = format!(
            "{contact_shortfalls}/{people_total} qualified companies returned fewer than {n_contacts} useful contacts after title and broad fallback searches; prefer accounts with a visible operations team"
        );
        let _ = db.record_learning(
            &pb.key,
            "contact_search_pattern",
            "Qualified accounts with thin contact coverage",
            "thin_contact_coverage",
            &detail,
        );
        report_source(
            progress.as_ref(),
            "contact-learning",
            "Saved contact-coverage correction",
            detail,
            "warning",
        );
    }
    report_source(
        progress.as_ref(),
        "contacts",
        "Mapped buyer contacts",
        format!(
            "{companies_mapped}/{people_total} companies · {} contacts filed · {contact_errors} errors",
            summary.people_added,
        ),
        if contact_errors == 0 { "complete" } else { "warning" },
    );

    Ok(summary)
}

fn routing_priority(status: &str) -> u8 {
    match status {
        "qualified" => 0,
        "research_needed" => 1,
        "research_required" => 2,
        _ => 3,
    }
}

#[allow(clippy::too_many_arguments)]
async fn qualify_candidate(
    client: &Engine,
    apollo: &Apollo,
    researcher: Option<&ResearchClient>,
    pb: &Playbook,
    system: &str,
    business_context: &str,
    gtm_play_context: &str,
    active_play: Option<&crate::db::GtmPlay>,
    allowed_signal_keys: &HashSet<String>,
    thesis: &str,
    org: ApolloOrg,
    knowledge: &str,
) -> (ApolloOrg, Result<OrgQual>) {
    // Apollo search rows are often sparse; hydrate before judging fit.
    let org = hydrate_org(apollo, org).await;
    let mut result = if let Some(reason) = brand_candidate_precheck(&pb.key, &org) {
        Ok(OrgQual {
            reject_reason: reason.clone(),
            disqualifiers: vec![reason],
            ..Default::default()
        })
    } else if let Some(max) = pb
        .max_employees
        .filter(|max| org.estimated_num_employees > *max)
    {
        Ok(OrgQual {
            reject_reason: format!(
                "{} employees is above {}'s ~{}-employee ceiling for a founder-led motion",
                org.estimated_num_employees, pb.name, max
            ),
            ..Default::default()
        })
    } else {
        let (research_block, research_sources) = match researcher {
            Some(researcher) => {
                match research::research_company(client, researcher, pb, &org, thesis).await {
                    Some(brief) => {
                        log_sourcing(format!(
                            "research {} · sources [{}]",
                            org.name,
                            compact_list(&brief.sources, 8)
                        ));
                        (brief.as_facts_block(), brief.sources)
                    }
                    None => (String::new(), Vec::new()),
                }
            }
            None => (String::new(), Vec::new()),
        };
        let mut qualification = qualify_org(
            client,
            system,
            pb,
            business_context,
            gtm_play_context,
            thesis,
            &org,
            knowledge,
            &research_block,
        )
        .await;
        if let Ok(value) = &mut qualification {
            value.research_sources = research_sources;
        }
        qualification
    };
    if let Ok(qualification) = &mut result {
        enforce_play_qualification(qualification, active_play, allowed_signal_keys);
    }
    (org, result)
}

fn enforce_play_qualification(
    qualification: &mut OrgQual,
    play: Option<&crate::db::GtmPlay>,
    allowed_signal_keys: &HashSet<String>,
) {
    // Models often put "no public evidence of X" in disqualifiers. That is an
    // unknown to investigate, not evidence that X is false. Preserve real hard
    // blockers while moving absence-of-proof language to evidence_gaps.
    let (unknowns, hard_disqualifiers): (Vec<_>, Vec<_>) = qualification
        .disqualifiers
        .drain(..)
        .partition(|item| is_missing_evidence(item));
    qualification.evidence_gaps.extend(unknowns);
    qualification.disqualifiers = hard_disqualifiers;

    // Do not infer canonical evidence by concatenating the whole research
    // corpus. Every structured signal must arrive with the exact passage and
    // URL that supports that individual claim; downstream opportunity lineage
    // groups claims by source domain before any action can become easy-tier.

    qualification.structured_signals.retain(|signal| {
        allowed_signal_keys.contains(signal.definition_key.trim())
            && !signal.evidence.trim().is_empty()
            && !signal.source_url.trim().is_empty()
            && signal.confidence >= 0.60
            && credible_canonical_signal(
                play.map(|play| play.brand.as_str()).unwrap_or_default(),
                &signal.definition_key,
                &signal.evidence,
            )
    });
    let mut matched = qualification
        .structured_signals
        .iter()
        .map(|signal| signal.definition_key.trim().to_string())
        .collect::<Vec<_>>();
    matched.sort();
    matched.dedup();
    qualification.matched_signal_keys = matched.clone();

    let Some(play) = play else {
        qualification.routing_status = if qualification.qualified {
            "qualified".into()
        } else {
            "rejected".into()
        };
        return;
    };
    let required_matches = play
        .required_signal_keys
        .iter()
        .filter(|key| matched.contains(key))
        .count();
    let independent_lineages = independent_source_lineages(
        &qualification.structured_signals,
        &play.required_signal_keys,
    );
    let minimum = play.minimum_signal_matches.max(1) as usize;
    let fully_supported = required_matches >= minimum
        && independent_lineages >= 2
        && qualification_foundations_present(&play.brand, &matched)
        && !qualification.root_cause.trim().is_empty()
        && !qualification.proof_fit.trim().is_empty()
        // A full signal set is necessary but not sufficient when the task and
        // consequence may come from separate facts. Preserve the higher fit
        // floor so weakly connected foundations remain medium-priority.
        && qualification.play_fit_score >= 65
        && qualification.disqualifiers.is_empty();
    let has_account_fit = matched.iter().any(|key| key == "account.fit_evidence");
    let has_source_backed_fit = has_account_fit || !qualification.observed_facts.is_empty();
    let discovery_candidate = has_source_backed_fit
        && qualification_discovery_foundations_present(&play.brand, &matched)
        && qualification.play_fit_score >= 45
        && qualification.disqualifiers.is_empty();
    let hard_research_candidate = has_account_fit
        && qualification.play_fit_score >= 30
        && qualification.disqualifiers.is_empty();

    if fully_supported {
        qualification.qualified = true;
        qualification.routing_status = "qualified".into();
        qualification.reject_reason.clear();
    } else if discovery_candidate {
        // Preserve plausible inventory for additional research, but do not
        // represent it as qualified or let cold copy discover the problem for
        // us.
        qualification.qualified = false;
        qualification.routing_status = "research_needed".into();
        qualification.reject_reason.clear();
        qualification.evidence_gaps.push(format!(
            "Only {required_matches}/{minimum} required play signals and {independent_lineages} independent source lineage(s) are supported; complete research before outreach."
        ));
    } else if hard_research_candidate {
        // A real ICP account with no precise wedge is not a sales rejection.
        // Keep it in the total market for deeper research, but never authorize
        // generic copy from company category alone.
        qualification.qualified = false;
        qualification.routing_status = "research_required".into();
        qualification.reject_reason.clear();
        qualification.evidence_gaps.push(
            "Hard-priority account: identify the exact task or decision and its closest owner before outreach."
                .into(),
        );
    } else {
        qualification.qualified = false;
        qualification.routing_status = "rejected".into();
        let mut reasons = Vec::new();
        if required_matches < minimum {
            reasons.push(format!(
                "only {required_matches} canonical play signal(s) matched; {minimum} required for full qualification"
            ));
        }
        if qualification.play_fit_score < 45 {
            reasons.push(format!(
                "play-fit score {}/100 is below the 45 medium-priority floor",
                qualification.play_fit_score.clamp(0, 100)
            ));
        }
        if qualification.root_cause.trim().is_empty() {
            reasons.push("no defensible root-cause hypothesis".into());
        }
        if qualification.proof_fit.trim().is_empty() {
            reasons.push("no credible fit to the bounded proof".into());
        }
        if !qualification.disqualifiers.is_empty() {
            reasons.push(format!(
                "hard disqualifier(s): {}",
                qualification.disqualifiers.join("; ")
            ));
        }
        if !has_source_backed_fit {
            reasons.push("no source-backed account-fit evidence".into());
        }
        qualification.reject_reason = reasons.join("; ");
    }
}

/// The research model may correctly extract first-party operating facts and
/// completed location/outage results while forgetting to map those same facts
/// to every canonical signal key. Do that mapping deterministically so routing
/// depends on cited evidence, not on whether the model repeated a JSON label.
#[cfg(test)]
fn augment_outage_signals(
    observed_facts: &[String],
    readable_signals: &[String],
    research_sources: &[String],
    structured_signals: &mut Vec<SignalCandidate>,
) {
    let evidence = observed_facts
        .iter()
        .chain(readable_signals)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if evidence.is_empty() {
        return;
    }
    let company_source = research_sources.iter().find(|source| {
        !source.contains("natural-resources.canada.ca") && !source.contains("api.outagehub.ca")
    });
    let station_source = research_sources
        .iter()
        .find(|source| source.contains("natural-resources.canada.ca"));
    let outage_source = research_sources
        .iter()
        .find(|source| source.contains("api.outagehub.ca"));

    for key in [
        "account.distributed_locations",
        "account.outage_sensitive_decision",
        "account.operated_ev_charging_network",
        "account.historical_location_outage_match",
    ] {
        if structured_signals.iter().any(|signal| {
            signal.definition_key == key
                && signal.confidence >= 0.60
                && crate::qualification::credible_outagehub_signal(key, &signal.evidence)
        }) || !crate::qualification::credible_outagehub_signal(key, &evidence)
        {
            continue;
        }
        let source_url = match key {
            "account.distributed_locations" => station_source.or(company_source),
            "account.historical_location_outage_match" => outage_source,
            _ => company_source,
        }
        .cloned()
        .unwrap_or_default();
        structured_signals.push(SignalCandidate {
            definition_key: key.into(),
            evidence: evidence.clone(),
            source_url,
            confidence: 0.9,
        });
    }
}

/// Current, company-attributed operating/job text often states the physical
/// task more clearly than the extraction model labels it. Map only explicit
/// task and burden language to Wapahki's canonical keys so a readable live job
/// posting cannot be lost because one JSON label was omitted. A single page
/// still counts as one lineage, so it can earn a medium first-touch draft but
/// never an easy/action-ready cadence by itself.
#[cfg(test)]
fn augment_wapahki_signals(
    observed_facts: &[String],
    readable_signals: &[String],
    research_sources: &[String],
    structured_signals: &mut Vec<SignalCandidate>,
) {
    let evidence = observed_facts
        .iter()
        .chain(readable_signals)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if evidence.is_empty() {
        return;
    }
    let text = evidence.to_ascii_lowercase();
    let has = |terms: &[&str]| terms.iter().any(|term| text.contains(term));
    let physical_task = has(&[
        "pack",
        "pallet",
        "stack",
        "pick",
        "case",
        "carton",
        "crate",
        "bag",
        "load",
        "unload",
        "lift",
        "transfer",
        "material handling",
    ]);
    let bounded_motion = physical_task
        && has(&[
            "palletize",
            "palletis",
            "pack into",
            "pack products",
            "pick and pack",
            "stack cases",
            "stack them",
            "load cartons",
            "assemble boxes",
            "production line",
            "end of line",
            "end-of-line",
            "finished goods",
            "finished product",
        ]);
    let operating_fit = physical_task
        && has(&[
            "manufactur",
            "production",
            "plant",
            "warehouse",
            "distribution",
            "factory",
            "packaging line",
            "cold storage",
        ]);
    let economic_pressure = bounded_motion
        && has(&[
            "repetitive",
            "repeated",
            "manual",
            "manually",
            "lift",
            " lb",
            "kg",
            "shift",
            "overtime",
            "throughput",
            "target",
            "fast-paced",
            "cold",
            "jam",
            "stoppage",
            "physical",
            "ergonomic",
            "safety",
        ]);
    let source_url = research_sources.first().cloned().unwrap_or_default();
    for (key, supported, confidence) in [
        ("account.fit_evidence", operating_fit, 0.86),
        ("account.bounded_repetitive_task", bounded_motion, 0.9),
        (
            "account.manual_task_economic_pressure",
            economic_pressure,
            0.82,
        ),
    ] {
        if !supported
            || structured_signals
                .iter()
                .any(|signal| signal.definition_key == key && signal.confidence >= 0.60)
        {
            continue;
        }
        structured_signals.push(SignalCandidate {
            definition_key: key.into(),
            evidence: evidence.clone(),
            source_url: source_url.clone(),
            confidence,
        });
    }
}

/// Map explicit recurring decision and artifact language to GnK's canonical
/// keys when the research model extracted the facts but omitted the labels.
/// This never derives a pain claim from company category or generic software
/// use; the relevant decision/mechanism words must appear in the cited text.
#[cfg(test)]
fn augment_gnk_signals(
    observed_facts: &[String],
    readable_signals: &[String],
    research_sources: &[String],
    structured_signals: &mut Vec<SignalCandidate>,
) {
    let evidence = observed_facts
        .iter()
        .chain(readable_signals)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if evidence.is_empty() {
        return;
    }
    let text = evidence.to_ascii_lowercase();
    let has = |terms: &[&str]| terms.iter().any(|term| text.contains(term));
    let workflow = has(&[
        "reconcil",
        "denial",
        "denied",
        "exception",
        "approval",
        "payment posting",
        "accounts receivable",
        "ar follow-up",
        "audit",
        "case management",
        "settlement",
        "dispute",
        "shipment",
        "release",
    ]);
    let specific_decision = workflow
        && has(&[
            "approve",
            "deny",
            "denied",
            "eligibility",
            "resolve",
            "correct",
            "resubmit",
            "review",
            "investigate",
            "compare",
            "match",
            "route",
            "escalat",
            "settle",
            "write-off",
            "write off",
            "release",
            "recover",
        ]);
    let mechanism = workflow
        && has(&[
            "bank statement",
            "supporting document",
            "authorization",
            "supervision",
            "notes",
            "payer portal",
            "eob",
            "remittance",
            "invoice",
            "purchase order",
            "work order",
            "batch",
            "record",
            "system",
            "spreadsheet",
            "email",
            "timeline",
            "documentation",
            "data",
            "three-way match",
            "3-way match",
        ]);
    let consequence = workflow
        && has(&[
            "delay",
            "backlog",
            "rejection",
            "reimbursement",
            "recover revenue",
            "recovery",
            "write-off",
            "write off",
            "late fee",
            "penalty",
            "audit exposure",
            "service level",
            "sla",
            "lost revenue",
            "payment speed",
            "settlement time",
        ]);
    let source_url = research_sources.first().cloned().unwrap_or_default();
    for (key, supported, confidence) in [
        ("account.fit_evidence", workflow, 0.84),
        (
            "account.specific_recurring_decision",
            specific_decision,
            0.88,
        ),
        (
            "account.external_trigger_or_mechanism_evidence",
            mechanism,
            0.86,
        ),
        ("account.believable_operating_consequence", consequence, 0.8),
    ] {
        if !supported
            || structured_signals
                .iter()
                .any(|signal| signal.definition_key == key && signal.confidence >= 0.60)
        {
            continue;
        }
        let canonical_evidence = if key == "account.specific_recurring_decision" {
            format!("Recurring job-duty evidence: {evidence}")
        } else {
            evidence.clone()
        };
        structured_signals.push(SignalCandidate {
            definition_key: key.into(),
            evidence: canonical_evidence,
            source_url: source_url.clone(),
            confidence,
        });
    }
}

fn independent_source_lineages(signals: &[SignalCandidate], required_keys: &[String]) -> usize {
    signals
        .iter()
        .filter(|signal| required_keys.contains(&signal.definition_key))
        .filter_map(|signal| source_independence_group(&signal.source_url))
        .collect::<HashSet<_>>()
        .len()
}

fn source_independence_group(raw: &str) -> Option<String> {
    let host = raw
        .trim()
        .split("//")
        .nth(1)
        .unwrap_or(raw.trim())
        .split(['/', '#', '?'])
        .next()
        .unwrap_or_default()
        .split('@')
        .next_back()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .trim_start_matches("www.")
        .to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }
    let labels = host.split('.').collect::<Vec<_>>();
    let group = if labels.len() >= 3
        && matches!(labels[labels.len() - 2], "co" | "com" | "org" | "gov")
        && labels.last().is_some_and(|suffix| suffix.len() == 2)
    {
        labels[labels.len() - 3..].join(".")
    } else if labels.len() >= 2 {
        labels[labels.len() - 2..].join(".")
    } else {
        host
    };
    Some(group)
}

fn qualification_foundations_present(brand: &str, matched: &[String]) -> bool {
    let required: &[&str] = if brand.eq_ignore_ascii_case("outagehub") {
        &[
            "account.fit_evidence",
            "account.distributed_locations",
            "account.outage_sensitive_decision",
            "account.historical_location_outage_match",
        ]
    } else if brand.eq_ignore_ascii_case("gnk") {
        &[
            "account.fit_evidence",
            "account.specific_recurring_decision",
            "account.believable_operating_consequence",
            "account.external_trigger_or_mechanism_evidence",
        ]
    } else if brand.eq_ignore_ascii_case("wapahki") {
        &[
            "account.fit_evidence",
            "account.bounded_repetitive_task",
            "account.manual_task_economic_pressure",
        ]
    } else {
        return true;
    };
    required
        .iter()
        .all(|required| matched.iter().any(|key| key == required))
}

/// Medium-priority accounts stay in the market when research supports a
/// concrete operating wedge but has not yet proved every economic term. They
/// may receive one hypothesis-led discovery email after a relevant person is
/// verified; multi-touch campaigns still require the full foundation set.
fn qualification_discovery_foundations_present(brand: &str, matched: &[String]) -> bool {
    let has = |key: &str| matched.iter().any(|matched| matched == key);
    if !has("account.fit_evidence") {
        return false;
    }
    if brand.eq_ignore_ascii_case("wapahki") {
        has("account.bounded_repetitive_task")
    } else if brand.eq_ignore_ascii_case("gnk") {
        has("account.specific_recurring_decision")
            || has("account.external_trigger_or_mechanism_evidence")
    } else if brand.eq_ignore_ascii_case("outagehub") {
        has("account.distributed_locations") && has("account.outage_sensitive_decision")
    } else {
        true
    }
}

/// Apply the same evidence and routing policy to a refreshed on-file account as
/// to a newly sourced one. Refresh used to have only qualified/research-needed
/// outcomes, which made hard disqualifiers and very weak fit impossible to evict.
fn enforce_refresh_qualification(
    refresh: &mut LeadRefresh,
    play: Option<&crate::db::GtmPlay>,
    allowed_signal_keys: &HashSet<String>,
) -> String {
    let (unknowns, hard_disqualifiers): (Vec<_>, Vec<_>) = refresh
        .disqualifiers
        .drain(..)
        .partition(|item| is_missing_evidence(item));
    refresh.evidence_gaps.extend(unknowns);
    refresh.disqualifiers = hard_disqualifiers;
    refresh.structured_signals.retain(|signal| {
        allowed_signal_keys.contains(signal.definition_key.trim())
            && !signal.evidence.trim().is_empty()
            && !signal.source_url.trim().is_empty()
            && signal.confidence >= 0.60
            && credible_canonical_signal(
                play.map(|play| play.brand.as_str()).unwrap_or_default(),
                &signal.definition_key,
                &signal.evidence,
            )
    });
    let mut matched = refresh
        .structured_signals
        .iter()
        .map(|signal| signal.definition_key.trim().to_string())
        .collect::<Vec<_>>();
    matched.sort();
    matched.dedup();
    refresh.matched_signal_keys = matched.clone();

    let Some(play) = play else {
        return if refresh.play_fit_score >= 65 && refresh.disqualifiers.is_empty() {
            "qualified"
        } else {
            "rejected"
        }
        .into();
    };
    let required_matches = play
        .required_signal_keys
        .iter()
        .filter(|key| matched.contains(key))
        .count();
    let independent_lineages =
        independent_source_lineages(&refresh.structured_signals, &play.required_signal_keys);
    let minimum = play.minimum_signal_matches.max(1) as usize;
    let fully_supported = required_matches >= minimum
        && independent_lineages >= 2
        && qualification_foundations_present(&play.brand, &matched)
        && !refresh.root_cause.trim().is_empty()
        && !refresh.proof_fit.trim().is_empty()
        && refresh.play_fit_score >= 65
        && refresh.disqualifiers.is_empty();
    let has_source_backed_fit = matched.iter().any(|key| key == "account.fit_evidence")
        || !refresh.observed_facts.is_empty();
    let discovery_candidate = has_source_backed_fit
        && qualification_discovery_foundations_present(&play.brand, &matched)
        && refresh.play_fit_score >= 45
        && refresh.disqualifiers.is_empty();
    let hard_research_candidate = matched.iter().any(|key| key == "account.fit_evidence")
        && refresh.play_fit_score >= 30
        && refresh.disqualifiers.is_empty();

    if fully_supported {
        "qualified".into()
    } else if discovery_candidate {
        refresh.evidence_gaps.push(format!(
            "Only {required_matches}/{minimum} required play signals and {independent_lineages} independent source lineage(s) are supported; complete research before outreach."
        ));
        "research_needed".into()
    } else if hard_research_candidate {
        refresh.evidence_gaps.push(
            "Hard-priority account: identify the exact task or decision and its closest owner before outreach."
                .into(),
        );
        "research_required".into()
    } else {
        "rejected".into()
    }
}

/// A refresh is a reassessment, not permission for the model to erase
/// previously verified facts. The refresh prompt receives the on-file facts,
/// but structured generation can still return only the newest page summary.
/// Merge the evidence record before deterministic qualification so a current
/// job duty or workflow fact cannot disappear merely because the model omitted
/// it from the refreshed JSON. Refreshed wording is kept first; exact duplicate
/// strings are removed case-insensitively.
fn merge_prior_refresh_evidence(lead: &Lead, refresh: &mut LeadRefresh) {
    fn merge(current: &mut Vec<String>, prior: &[String]) {
        let mut seen = current
            .iter()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .collect::<HashSet<_>>();
        for value in prior {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            if seen.insert(trimmed.to_ascii_lowercase()) {
                current.push(trimmed.to_string());
            }
        }
    }

    merge(&mut refresh.observed_facts, &lead.observed_facts);
    merge(&mut refresh.signals, &lead.signals);
}

/// Canonical signals are stronger than topical resemblance. GnK must not turn
/// department or portal presence into a reconciliation claim. OutageHub must
/// not turn a contractor's office list or generic emergency service into a
/// distributed-asset, utility-decision thesis.
fn credible_canonical_signal(brand: &str, key: &str, evidence: &str) -> bool {
    let text = evidence.to_ascii_lowercase();
    let has = |terms: &[&str]| terms.iter().any(|term| text.contains(term));
    if brand.eq_ignore_ascii_case("outagehub") {
        return crate::qualification::credible_outagehub_signal(key, evidence);
    }
    if !brand.eq_ignore_ascii_case("gnk") {
        return true;
    }
    match key.trim() {
        "account.specific_recurring_decision" => {
            has(&[
                "when ",
                "after ",
                "each ",
                "every ",
                "recurring",
                "repeated",
                "per ",
            ]) && has(&[
                "decide",
                "decision",
                "determine",
                "approve",
                "deny",
                "dispute",
                "settle",
                "escalate",
                "write off",
                "release",
                "recover",
                "review",
                "resolve",
                "correct",
                "resubmit",
                "reconcile",
                "compare",
                "match",
                "post payment",
                "post insurer",
            ]) && has(&[
                "rejection",
                "short-pay",
                "deduction",
                "exception",
                "escalation",
                "denial",
                "incident",
                "claim",
                "case",
                "shipment",
                "load",
                "audit",
                "payment",
                "invoice",
                "bank statement",
                "remittance",
                "refund",
            ])
        }
        "account.believable_operating_consequence" => has(&[
            "settlement time",
            "cycle time",
            "leakage",
            "write-off",
            "write off",
            "recovery",
            "recoveries",
            "audit exposure",
            "regulatory",
            "customer sla",
            "service level",
            "escalation",
            "senior reviewer",
            "capacity",
            "backlog",
            "delayed decision",
            "lost revenue",
            "short-paid",
            "short paid",
        ]),
        "account.external_trigger_or_mechanism_evidence" => {
            has(&[
                "rejection",
                "short-pay",
                "deduction notice",
                "denial",
                "escalation",
                "dispute",
                "exception",
                "incident",
                "audit request",
                "receiver",
                "claim",
                "payment",
                "invoice",
                "bank statement",
            ]) && has(&[
                "record",
                "document",
                "report",
                "timeline",
                "temperature",
                "tender",
                "bill of lading",
                "appointment",
                "policy",
                "investigation",
                "prior action",
                "correspondence",
                "system",
                "portal",
                "supporting document",
                "authorization",
                "eob",
                "remittance",
                "invoice",
                "bank statement",
                "batch",
            ])
        }
        "account.expensive_recurring_workflow" => {
            has(&[
                "workflow",
                "process",
                "claim",
                "case",
                "exception",
                "investigation",
                "decision",
                "handoff",
                "reconciliation",
                "review",
            ]) && has(&[
                "manual",
                "repeated",
                "recurring",
                " each ",
                " per ",
                "daily",
                "weekly",
                "monthly",
                "every ",
                "volume",
                "queue",
            ]) && has(&[
                "backlog",
                "delay",
                "rework",
                "hours",
                " time",
                "risk",
                "loss",
                "reserve",
                "settlement",
                "legal cost",
                "severity",
                "capacity",
                "bottleneck",
                "cost",
            ])
        }
        "account.cross_system_reconciliation" => {
            has(&["system", "portal", "record", "document", "source"])
                && has(&[
                    "reconcil",
                    "assembl",
                    "stitch",
                    "manually pull",
                    "manual pull",
                    "re-key",
                    "rekey",
                    "copy between",
                    "compare across",
                    "cross-system",
                    "across systems",
                ])
        }
        // Account qualification happens before contact mapping, so the model
        // cannot prove reachability from company facts. Contact sourcing may
        // record this separately once a real person and vantage exist.
        "account.reachable_workflow_owner" => false,
        _ => true,
    }
}

fn is_missing_evidence(item: &str) -> bool {
    let value = item.to_ascii_lowercase();
    [
        "no public evidence",
        "no evidence provided",
        "not publicly",
        "not disclosed",
        "not found",
        "unknown",
        "unclear",
        "cannot verify",
        "could not verify",
        "insufficient evidence",
        "no source-backed evidence",
        "is not met; only",
    ]
    .iter()
    .any(|needle| value.contains(needle))
}

/// Reuse canonical company identity for an exact-domain run. This is not
/// qualification reuse: the returned organization is still judged against the
/// target brand's current play and receives fresh official-site research.
fn portfolio_orgs_for_exact_domains(leads: &[Lead], exact_domains: &[String]) -> Vec<ApolloOrg> {
    if exact_domains.is_empty() {
        return Vec::new();
    }
    let wanted = exact_domains
        .iter()
        .map(|domain| canonical_company_domain(domain))
        .filter(|domain| !domain.is_empty())
        .collect::<HashSet<_>>();
    let mut best = HashMap::<String, &Lead>::new();
    for lead in leads {
        let domain = canonical_company_domain(&lead.domain);
        if !wanted.contains(&domain) {
            continue;
        }
        let replace = best.get(&domain).is_none_or(|current| {
            portfolio_lead_profile_score(lead) > portfolio_lead_profile_score(current)
        });
        if replace {
            best.insert(domain, lead);
        }
    }
    exact_domains
        .iter()
        .filter_map(|raw_domain| {
            let domain = canonical_company_domain(raw_domain);
            let lead = best.get(&domain)?;
            Some(ApolloOrg {
                id: if lead.apollo_org_id.trim().is_empty() {
                    format!("portfolio-domain:{domain}")
                } else {
                    lead.apollo_org_id.clone()
                },
                name: lead.name.clone(),
                website_url: format!("https://{domain}"),
                primary_domain: domain,
                industry: lead.industry.clone(),
                estimated_num_employees: lead.headcount,
                organization_city: lead.hq.clone(),
                annual_revenue_printed: lead.revenue.clone(),
                // This marker prevents a redundant Apollo hydration call. It
                // is removed before the org reaches qualification; all task
                // evidence must come from the new official-site research.
                technology_names: vec![PORTFOLIO_REUSE_MARKER.into()],
                ..Default::default()
            })
        })
        .collect()
}

fn portfolio_lead_profile_score(lead: &Lead) -> i64 {
    i64::from(!lead.apollo_org_id.trim().is_empty()) * 16
        + i64::from(!lead.industry.trim().is_empty()) * 8
        + i64::from(lead.headcount > 0) * 4
        + i64::from(!lead.hq.trim().is_empty()) * 2
        + i64::from(!lead.observed_facts.is_empty())
}

/// Copy real people already verified for the same canonical company into the
/// target brand's contact map. Their identity and verification are reusable;
/// their brand-specific sales rationale is deliberately cleared. `upsert_person`
/// then maps each person to the current target-brand opportunity committee.
fn reuse_portfolio_people(
    db: &SharedDb,
    brand: &str,
    target_lead_id: &str,
    domain: &str,
    limit: usize,
) -> Result<usize> {
    let domain = canonical_company_domain(domain);
    if domain.is_empty() || limit == 0 {
        return Ok(0);
    }
    let matching_lead_ids = db
        .list_leads(None)?
        .into_iter()
        .filter(|lead| canonical_company_domain(&lead.domain) == domain)
        .map(|lead| lead.id)
        .collect::<HashSet<_>>();
    let mut candidates = db
        .list_people(None, None)?
        .into_iter()
        .filter(|person| matching_lead_ids.contains(&person.lead_id))
        .filter(|person| {
            !matches!(
                person.status.to_ascii_lowercase().as_str(),
                "held" | "suppressed" | "unsubscribed"
            ) && !person.email_status.eq_ignore_ascii_case("invalid")
                && crate::response_design::contact_priority(
                    &person.title,
                    &person.vantage,
                    person.primary,
                ) >= 25
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        reuse_person_score(right)
            .cmp(&reuse_person_score(left))
            .then_with(|| left.name.cmp(&right.name))
    });

    let mut seen = HashSet::new();
    let mut filed = 0usize;
    for source in candidates {
        let identity = reusable_person_identity(&source);
        if identity.is_empty() || !seen.insert(identity) {
            continue;
        }
        let person = Person {
            lead_id: target_lead_id.to_string(),
            brand: brand.to_string(),
            apollo_person_id: if source.apollo_person_id.trim().is_empty() {
                format!("portfolio:{}", source.id)
            } else {
                source.apollo_person_id.clone()
            },
            first_name: source.first_name.clone(),
            last_name: source.last_name.clone(),
            name: source.name.clone(),
            title: source.title.clone(),
            location: source.location.clone(),
            timezone: source.timezone.clone(),
            vantage: crate::response_design::effective_vantage(&source.title, &source.vantage),
            primary: crate::response_design::effective_primary(&source.title, source.primary),
            linkedin_url: source.linkedin_url.clone(),
            linkedin_status: source.linkedin_status.clone(),
            email: source.email.clone(),
            email_status: source.email_status.clone(),
            phone: source.phone.clone(),
            status: source.status.clone(),
            enriched_at: source.enriched_at.clone(),
            ..Default::default()
        };
        db.upsert_person(&person)?;
        filed += 1;
        if filed >= limit {
            break;
        }
    }
    Ok(filed)
}

fn reusable_person_identity(person: &Person) -> String {
    if !person.apollo_person_id.trim().is_empty() {
        return format!("apollo:{}", person.apollo_person_id.trim());
    }
    if !person.email.trim().is_empty() {
        return format!("email:{}", person.email.trim().to_ascii_lowercase());
    }
    if !person.linkedin_url.trim().is_empty() {
        return format!(
            "linkedin:{}",
            person.linkedin_url.trim().to_ascii_lowercase()
        );
    }
    let name = person.name.trim().to_ascii_lowercase();
    let title = person.title.trim().to_ascii_lowercase();
    if name.is_empty() || title.is_empty() {
        String::new()
    } else {
        format!("name:{name}|{title}")
    }
}

/// Fetch real people at an org, assign a vantage to each, and file them.
#[allow(clippy::too_many_arguments)]
async fn source_people(
    db: &SharedDb,
    client: &Engine,
    apollo: &Apollo,
    pb: &Playbook,
    system: &str,
    org: &ApolloOrg,
    lead: &Lead,
    lead_id: &str,
    icp: &Icp,
    n_contacts: usize,
    fallback_recipient_timezone: &str,
) -> Result<usize> {
    // Over-source a bench of candidates, not just n_contacts. Enrichment verifies
    // only a fraction of any org's people (many have no findable email), so if we
    // filed exactly n_contacts we'd routinely end up with fewer than n_contacts
    // VERIFIED contacts and blank rows. Filing extra candidates lets a different
    // contact backfill anyone who doesn't verify; sequencing later caps at
    // n_contacts of the strongest verified people per company.
    let source_pool = n_contacts.saturating_mul(CONTACT_BACKFILL_FACTOR).max(8);
    let preferred_locations = preferred_contact_locations(&pb.key);
    let people = gather_people(apollo, &pb.key, org, icp, source_pool, &preferred_locations).await;
    if people.is_empty() {
        log_sourcing(format!("no people found for {} (all strategies)", org.name));
        return Ok(0);
    }

    let assignments = assign_vantage(client, system, pb, lead, &people)
        .await
        .unwrap_or(VantageDoc {
            assignments: vec![],
        });

    let mut added = 0usize;
    for (i, ap) in people.iter().enumerate().take(source_pool) {
        let va = assignments.assignments.iter().find(|a| a.index == i);
        let location = ap.location();
        let timezone_location = if location.is_empty() {
            lead.hq.as_str()
        } else {
            location.as_str()
        };
        let timezone =
            calendar::timezone_for_location(timezone_location, fallback_recipient_timezone)
                .name()
                .to_string();
        let person = Person {
            lead_id: lead_id.to_string(),
            brand: pb.key.clone(),
            apollo_person_id: ap.id.clone(),
            first_name: ap.first_name.clone(),
            last_name: ap.last_name.clone(),
            name: ap.full_name(),
            title: ap.title.clone(),
            location,
            timezone,
            vantage: crate::response_design::effective_vantage(
                &ap.title,
                &normalize_vantage(va.map(|v| v.vantage.as_str()).unwrap_or("")),
            ),
            can_observe: va.map(|v| v.can_observe.clone()).unwrap_or_default(),
            why_them: va.map(|v| v.why_them.clone()).unwrap_or_default(),
            primary: crate::response_design::effective_primary(
                &ap.title,
                va.map(|v| v.primary).unwrap_or(false),
            ),
            route_to: va.map(|v| v.route_to.clone()).unwrap_or_default(),
            linkedin_url: ap.linkedin_url.clone(),
            // Search results are masked; email is filled later by enrichment.
            email: ap.email.clone(),
            email_status: map_email_status(&ap.email_status),
            phone: ap.best_phone(),
            status: "new".into(),
            ..Default::default()
        };
        let pid = db.upsert_person(&person)?;
        db.log_event(
            &pb.key,
            &pid,
            "",
            "sourced",
            &format!("{} @ {}", person.name, org.name),
        )?;
        added += 1;
    }
    Ok(added)
}

/// Collect up to `n_contacts` real people at an org, deduped, preferring
/// title/seniority-matched people but broadening to any employee and falling
/// back across domain/org-id lookups so a company reliably yields contacts even
/// when Apollo's org-id index is empty or the search record has no domain.
async fn gather_people(
    apollo: &Apollo,
    brand: &str,
    org: &ApolloOrg,
    icp: &Icp,
    n_contacts: usize,
    preferred_locations: &[String],
) -> Vec<ApolloPerson> {
    use std::collections::HashSet;

    let want = n_contacts.max(1);
    let over = (want * 2).clamp(5, 50) as u32;

    // People search by domain is far more reliable than by org id; resolve a
    // domain by name if the search record lacks one.
    let mut domain = org.domain();
    if domain.is_empty() {
        domain = resolve_domain(apollo, org).await;
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<ApolloPerson> = Vec::new();

    // One deliberate search per committee role. Keep at most one strong result
    // from each role before filling the bench, so a page of operations managers
    // cannot masquerade as a mapped buying committee.
    for (role, titles) in committee_title_groups(brand) {
        let mut role_found = false;
        let location_attempts = if preferred_locations.is_empty() {
            vec![Vec::new()]
        } else {
            vec![preferred_locations.to_vec(), Vec::new()]
        };
        for locations in location_attempts {
            if role_found {
                break;
            }
            let filters = if !domain.is_empty() {
                PeopleFilters {
                    organization_domains: vec![domain.clone()],
                    titles: titles.iter().map(|title| (*title).to_string()).collect(),
                    locations,
                    page: 1,
                    per_page: 10,
                    ..Default::default()
                }
            } else {
                PeopleFilters {
                    organization_ids: vec![org.id.clone()],
                    titles: titles.iter().map(|title| (*title).to_string()).collect(),
                    locations,
                    page: 1,
                    per_page: 10,
                    ..Default::default()
                }
            };
            match apollo.search_people(&filters).await {
                Ok(people) => {
                    let preferred = people
                        .iter()
                        .position(|person| committee_role_for_title(&person.title) == role)
                        .or_else(|| (!people.is_empty()).then_some(0));
                    if let Some(index) = preferred {
                        let person = people[index].clone();
                        let key = if !person.id.is_empty() {
                            person.id.clone()
                        } else {
                            format!(
                                "{}|{}",
                                person.full_name().to_lowercase(),
                                person.title.to_lowercase()
                            )
                        };
                        if seen.insert(key) {
                            out.push(person);
                            role_found = true;
                        }
                    }
                }
                Err(error) => log_sourcing(format!(
                    "committee people search ({role}) failed for {}: {error:#}",
                    org.name
                )),
            }
        }
    }

    // Ordered fallback strategies: most targeted first so remaining bench rows
    // progressively broader so we still reach the count.
    let mut attempts: Vec<(&str, PeopleFilters)> = Vec::new();
    if !preferred_locations.is_empty() && !domain.is_empty() {
        attempts.push((
            "domain+titles+location",
            PeopleFilters {
                organization_domains: vec![domain.clone()],
                titles: icp.titles.clone(),
                seniorities: icp.seniorities.clone(),
                locations: preferred_locations.to_vec(),
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }
    if !preferred_locations.is_empty() && !org.id.is_empty() {
        attempts.push((
            "org_id+titles+location",
            PeopleFilters {
                organization_ids: vec![org.id.clone()],
                titles: icp.titles.clone(),
                seniorities: icp.seniorities.clone(),
                locations: preferred_locations.to_vec(),
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }
    if !domain.is_empty() {
        attempts.push((
            "domain+titles",
            PeopleFilters {
                organization_domains: vec![domain.clone()],
                titles: icp.titles.clone(),
                seniorities: icp.seniorities.clone(),
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }
    if !org.id.is_empty() {
        attempts.push((
            "org_id+titles",
            PeopleFilters {
                organization_ids: vec![org.id.clone()],
                titles: icp.titles.clone(),
                seniorities: icp.seniorities.clone(),
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }
    if !domain.is_empty() {
        attempts.push((
            "domain-any",
            PeopleFilters {
                organization_domains: vec![domain.clone()],
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }
    if !org.id.is_empty() {
        attempts.push((
            "org_id-any",
            PeopleFilters {
                organization_ids: vec![org.id.clone()],
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }

    for (label, filters) in attempts {
        if out.len() >= want {
            break;
        }
        match apollo.search_people(&filters).await {
            Ok(people) => {
                for p in people {
                    if p.full_name().trim().is_empty() {
                        continue;
                    }
                    let key = if !p.id.is_empty() {
                        p.id.clone()
                    } else {
                        format!(
                            "{}|{}",
                            p.full_name().to_lowercase(),
                            p.title.to_lowercase()
                        )
                    };
                    if seen.insert(key) {
                        out.push(p);
                    }
                }
            }
            Err(e) => log_sourcing(format!(
                "people search ({label}) failed for {}: {e:#}",
                org.name
            )),
        }
    }
    out.truncate(want);
    out
}

fn preferred_contact_locations(brand: &str) -> Vec<String> {
    if brand.eq_ignore_ascii_case("wapahki") {
        vec!["Ontario, Canada".into()]
    } else if brand.eq_ignore_ascii_case("outagehub") {
        vec!["Canada".into()]
    } else {
        Vec::new()
    }
}

fn committee_title_groups(brand: &str) -> Vec<(&'static str, &'static [&'static str])> {
    let witness: &'static [&'static str] = if brand.eq_ignore_ascii_case("wapahki") {
        &[
            "production supervisor",
            "warehouse supervisor",
            "shift supervisor",
            "operations supervisor",
            "team lead",
        ]
    } else if brand.eq_ignore_ascii_case("outagehub") {
        &[
            "NOC supervisor",
            "field service supervisor",
            "technical support supervisor",
            "service operations supervisor",
        ]
    } else {
        &[
            "operations supervisor",
            "claims supervisor",
            "case management supervisor",
            "revenue cycle supervisor",
            "logistics supervisor",
        ]
    };
    let owner: &'static [&'static str] = if brand.eq_ignore_ascii_case("wapahki") {
        &[
            "plant manager",
            "production manager",
            "warehouse manager",
            "distribution centre manager",
            "operations manager",
            "director operations",
        ]
    } else if brand.eq_ignore_ascii_case("outagehub") {
        &[
            "network operations manager",
            "service operations manager",
            "charging operations manager",
            "field service manager",
            "maintenance manager",
            "director service operations",
        ]
    } else {
        &[
            "operations manager",
            "claims operations manager",
            "revenue cycle manager",
            "project controls manager",
            "logistics operations manager",
            "director operations",
        ]
    };
    vec![
        ("problem_witness", witness),
        ("process_owner", owner),
        (
            "constraint_owner",
            &[
                "quality manager",
                "safety manager",
                "sanitation manager",
                "compliance manager",
            ],
        ),
        (
            "technical_evaluator",
            &[
                "automation manager",
                "controls manager",
                "engineering manager",
                "maintenance manager",
                "IT systems manager",
                "integration manager",
            ],
        ),
        (
            "economic_buyer",
            &[
                "vice president operations",
                "general manager",
                "chief operating officer",
                "president",
            ],
        ),
        (
            "procurement_legal",
            &[
                "procurement manager",
                "purchasing manager",
                "strategic sourcing manager",
            ],
        ),
    ]
}

fn committee_role_for_title(title: &str) -> &'static str {
    let text = format!(" {} ", title.trim().to_ascii_lowercase());
    if [" procurement ", " purchasing ", " strategic sourcing "]
        .iter()
        .any(|term| text.contains(term))
    {
        "procurement_legal"
    } else if [" quality ", " safety ", " sanitation ", " compliance "]
        .iter()
        .any(|term| text.contains(term))
    {
        "constraint_owner"
    } else if [
        " automation ",
        " controls ",
        " engineering ",
        " maintenance ",
        " it systems ",
        " integration ",
    ]
    .iter()
    .any(|term| text.contains(term))
    {
        "technical_evaluator"
    } else if [
        " vice president ",
        " vp ",
        " general manager ",
        " chief operating officer ",
        " president ",
    ]
    .iter()
    .any(|term| text.contains(term))
    {
        "economic_buyer"
    } else if [" supervisor ", " team lead ", " coordinator "]
        .iter()
        .any(|term| text.contains(term))
    {
        "problem_witness"
    } else if text.contains(" manager ") {
        "process_owner"
    } else {
        "router"
    }
}

/// Best-effort domain resolution for an org whose search record has none: search
/// Apollo organizations by name and take the first close match that has a domain.
async fn resolve_domain(apollo: &Apollo, org: &ApolloOrg) -> String {
    if org.name.trim().is_empty() {
        return String::new();
    }
    let found = apollo
        .search_organizations(&OrgFilters {
            name: org.name.clone(),
            page: 1,
            per_page: 5,
            ..Default::default()
        })
        .await
        .unwrap_or_default();
    let want = org.name.to_lowercase();
    found
        .into_iter()
        .filter(|o| {
            let n = o.name.to_lowercase();
            !o.domain().is_empty() && (n == want || n.contains(&want) || want.contains(&n))
        })
        .map(|o| o.domain())
        .next()
        .unwrap_or_default()
}

// --- Claude calls ----------------------------------------------------------

async fn derive_icp(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    business_context: &str,
    gtm_play_context: &str,
    thesis: &str,
) -> Result<Icp> {
    let mut firmographic = String::new();
    if let Some(max) = pb.max_employees {
        firmographic.push_str(&format!(
            " Only target companies with at most {max} employees — this brand is a small, \
             founder-led vendor that cannot realistically land larger enterprises, so pick \
             employee_ranges buckets at or below that ceiling."
        ));
    }
    if !pb.icp_note.trim().is_empty() {
        firmographic.push(' ');
        firmographic.push_str(pb.icp_note.trim());
    }
    let search_discipline = if pb.key.eq_ignore_ascii_case("outagehub") {
        " OUTAGEHUB SEARCH DISCIPLINE: cover Canadian operators whose live decisions could improve from location-matched public utility-outage data: charging networks, telecom and remote infrastructure, cold storage, data centres, multi-site retail, facilities/service operations, backup-power dispatch, and other distributed sites. Prefer an evidenced outage-time decision and a reachable operations owner. Sellers with no operated or monitored footprint are poor targets."
    } else {
        ""
    };
    let context_block = if business_context.trim().is_empty() {
        String::new()
    } else {
        format!("{}\n\n", business_context.trim())
    };
    let user = format!(
        "{context_block}Translate this outreach thesis into an Apollo.io search. The brand's motion is: {motion}.\n\n{gtm_play_context}\n\n\
         THESIS: {thesis}\n\n\
         Return Apollo filters that will surface companies plausibly having this expensive workflow \
         AND that fit what the business (above) is actually trying to accomplish — not merely a loose \
         keyword match. Include the job titles/seniorities of the people who would OWN or OBSERVE the \
         workflow (by vantage, not just seniority). Organization keywords must describe BUYER \
         categories or industries, never the task, equipment, automation method, product, or vendor \
         category being sold; those mechanism words attract suppliers instead of operators. Keep \
         keywords concrete and industry-specific. \
         Employee ranges must use Apollo's bucket format like \"51,200\".{firmographic} If the thesis \
         implies a region, set locations.{search_discipline}\n\n{doctrine}",
        motion = pb.motion,
        doctrine = core_strategy_block("icp"),
    );
    client
        .structured_bulk::<Icp>("source.icp", system, &user, icp_schema())
        .await
}

#[allow(clippy::too_many_arguments)]
async fn qualify_org(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    business_context: &str,
    gtm_play_context: &str,
    thesis: &str,
    org: &ApolloOrg,
    knowledge: &str,
    research: &str,
) -> Result<OrgQual> {
    let brand_guard = brand_qualification_guard(&pb.key);
    let facts = json!({
        "name": org.name,
        "domain": org.domain(),
        "industry": org.industry,
        "headquarters": org.hq(),
        "estimated_employees": org.estimated_num_employees,
        "annual_revenue": org.annual_revenue_printed,
        "description": org.short_description,
        "keywords": org.keywords,
        "technologies": org.technology_names,
    });
    let research_block = if research.trim().is_empty() {
        String::new()
    } else {
        format!("{}\n\n", research.trim())
    };
    let context_block = if business_context.trim().is_empty() {
        String::new()
    } else {
        format!("{}\n\n", business_context.trim())
    };
    let signal_catalog = crate::gtm::signal_catalog_prompt(&pb.key);
    let user = format!(
        "{context_block}{gtm_play_context}\n\nDecide whether this REAL company (from Apollo) fits the thesis AND the \
         business's goals and constraints above, and if so frame the \
         doctrine fields. THESIS: {thesis}\n\nAPOLLO FACTS (the ONLY things you may state as fact):\n{facts}\n\n{research_block}{knowledge}\n\n\
         Rules: observed_facts must each be supported by the Apollo facts OR the website research \
         above — never invent a customer, metric, or dollar figure. Put every reasonable-but-unproven \
         guess in inferences. Keep physical and operational specificity evidence-bound: a hypothesis \
         may name a tray, pouch, case, pallet, conveyor, machine, alarm, dispatch, or internal handoff \
         only when a public source names that object or task. Otherwise use the supported category \
         (for example, physical packing or handling work) and make discovery identify the actual job. \
         An inference is not permission to manufacture a concrete station for outreach. \
         consequence_metric is a measurable consequence, NOT dollars. If at \
         least {min} independent signals don't support the hypothesis, set qualified=false with a \
         one-line reject_reason. Preserve the readable `signals` list, and also map every supported \
         observation you can to `structured_signals` using the canonical catalog. Every canonical \
         signal needs its own direct Apollo or first-party website evidence and source_url. Full \
         qualification requires at least two independent source documents; repeating one page under \
         several signal labels never satisfies that boundary. A technology name, broad \
         product range, company scale, or the existence of several portals does not by itself prove \
         a manual workflow, cross-system reconciliation, pain, material consequence, or reachable \
         workflow owner. Do not relabel one generic fact as several independent required signals, and \
         never invent a signal merely to satisfy the catalog. Missing public evidence is an `evidence_gap`, never \
         a `disqualifier`; reserve disqualifiers for affirmative evidence that the company cannot \
         realistically run, buy, or validate the motion. Account fit plus one workflow signal may \
         remain a research candidate, but missing task or exception evidence must be resolved with \
         further first-party research before outreach. Root-cause analysis must separate the observable \
         symptom/event from the underlying missing information, coordination, capability, or system \
         boundary that plausibly produces it; name the current human workaround and mark anything \
         unproven as hypothesis. Explain why the active play's bounded proof could confirm or kill \
         that cause. Score play fit 0-100: 30 signal/decision evidence, 25 root-cause + workaround \
         clarity, 20 reachable stakeholder vantage, 15 bounded-proof fit, 10 credible timing/why-now. \
         A generic industry, technology, hiring, or scale match cannot score 65. Put real blockers in \
         disqualifiers and unknown evidence in evidence_gaps.\n\n{brand_guard}\n\n{signal_catalog}",
        facts = serde_json::to_string_pretty(&facts).unwrap_or_default(),
        min = pb.min_signals,
    );
    client
        .structured_bulk::<OrgQual>("source.qualify", system, &user, qual_schema())
        .await
}

fn brand_qualification_guard(brand: &str) -> &'static str {
    if brand.eq_ignore_ascii_case("outagehub") {
        "OUTAGEHUB ACCOUNT GUARD: Keep the market broad but the problem narrow. A company can fit when it operates, monitors, supports, or dispatches across Canadian locations and a source-backed outage-time decision could use outside utility status or restoration context. EV charging is one segment, not the whole ICP. Require a distributed footprint and one concrete decision such as diagnosis, dispatch, escalation, continuity, transfer, prioritization, or customer communication. A completed historical location/polygon match makes the account easy-priority; without one, a well-evidenced decision and reachable owner may remain medium-priority for one honest discovery email. Sellers with no operated/monitored footprint are poor fits. Never claim a private site was down."
    } else if brand.eq_ignore_ascii_case("wapahki") {
        "WAPAHKI ACCOUNT GUARD: Cover product manufacturers, factories, warehouses, distribution centres, and fulfillment operations, starting in Ontario and expanding across Canada. Rank one source-supported physical candidate task: a package or product form, station, handoff, line, machine, physical job duty, receiving/picking/packing/palletizing flow, equipment project, facility expansion, or plant document. A current company-attributed job mirror supports only what it directly states. A task plus source-backed staffing, throughput, stoppage, utilization, changeover, sanitation, ergonomics, or safety pressure is easy-priority. A task without proven economics is medium-priority and may receive one honest, task-specific discovery email to the nearest operator; frame the consequence as a question. Company category alone is hard-priority research, not permission for a generic robotics note."
    } else if brand.eq_ignore_ascii_case("gnk") {
        "GNK ACCOUNT GUARD: Cover organizations broadly, but rank a specific software/workflow wedge and the person nearest it. An account is easy-priority when public evidence supports one recurring event and decision, a believable consequence, and the trigger, artifacts, systems, or handoff. It is medium-priority when evidence supports the recurring decision OR the account-specific mechanism but not the full consequence; permit only one honest discovery email that states facts as facts and frames the missing term as a question. Generic company fit with no concrete workflow remains hard-priority research. Never turn software usage or company scale alone into a pain claim."
    } else {
        ""
    }
}

async fn assign_vantage(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    lead: &Lead,
    people: &[ApolloPerson],
) -> Result<VantageDoc> {
    let roster: Vec<Value> = people
        .iter()
        .enumerate()
        .map(|(i, p)| json!({ "index": i, "name": p.full_name(), "title": p.title, "seniority": p.seniority, "departments": p.departments }))
        .collect();
    let user = format!(
        "These are REAL people at {company}. The hypothesis we're testing: {hyp}\n\nROSTER:\n{roster}\n\n\
         Work backward from the one response needed next: a workflow example, confirmation of a recurring problem, a technical boundary, an economic decision, or an internal route. For each person, assign the vantage point that best fits whether they can provide that response by observing, deciding, or routing (not their seniority). Distinguish problem witnesses, process owners, economic buyers, evaluators, and routers. Interns, students, trainees, and apprentices are route-only and never primary. Do not treat a function such as Sales or HR as automatically relevant or irrelevant; match it to the named hypothesis. can_observe must be a cautious 5-15 word note about likely access. \
         why_them must be a plain 6-18 word internal reason to contact them. Do not repeat their title, \
         lecture them about their role, or claim ownership that the title does not prove. Use `likely`, \
         `may`, or `could` where access is inferred. Also say whether they are a primary first contact, \
         and fill route_to only for a router. Vantage notes for this brand:\n{notes}\n\n{doctrine}",
        company = lead.name,
        hyp = lead.hypothesis,
        roster = serde_json::to_string_pretty(&roster).unwrap_or_default(),
        notes = pb.vantage_notes.join("\n"),
        doctrine = core_strategy_block("people"),
    );
    client
        .structured_bulk::<VantageDoc>("source.vantage", system, &user, vantage_schema())
        .await
}

// --- Schemas ---------------------------------------------------------------

fn str_array(desc: &str) -> Value {
    json!({ "type": "array", "items": { "type": "string" }, "description": desc })
}

fn signal_candidates_schema() -> Value {
    json!({
        "type": "array",
        "description": "Source-backed observations mapped to canonical GTM signal keys.",
        "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["definition_key", "evidence", "source_url", "confidence"],
            "properties": {
                "definition_key": { "type": "string" },
                "evidence": { "type": "string" },
                "source_url": { "type": "string" },
                "confidence": { "type": "number", "minimum": 0, "maximum": 1 }
            }
        }
    })
}

fn icp_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["keywords", "titles"],
        "properties": {
            "keywords": str_array("Industry/ICP keyword tags for Apollo org search."),
            "employee_ranges": {
                "type": "array",
                "items": { "type": "string", "enum": ["1,10","11,50","51,200","201,500","501,1000","1001,5000","5001,10000","10001,1000000"] },
                "description": "Apollo headcount buckets."
            },
            "locations": str_array("HQ locations, e.g. 'Canada'."),
            "titles": str_array("Job titles of the people who own/observe the workflow."),
            "seniorities": {
                "type": "array",
                "items": { "type": "string", "enum": ["owner","founder","c_suite","partner","vp","head","director","manager","senior","entry","intern"] }
            }
        }
    })
}

fn qual_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["qualified", "hypothesis", "mechanism", "consequence_metric", "play_fit_score", "symptom", "root_cause", "current_workaround", "why_now", "proof_fit"],
        "properties": {
            "qualified": { "type": "boolean" },
            "reject_reason": { "type": "string" },
            "observed_facts": str_array("Facts supported by the Apollo payload ONLY."),
            "inferences": str_array("Reasonable but unproven guesses."),
            "hypothesis": { "type": "string" },
            "mechanism": { "type": "string" },
            "consequence_metric": { "type": "string", "description": "Measurable consequence, never dollars." },
            "signals": str_array("Independent signals making the hypothesis plausible."),
            "structured_signals": signal_candidates_schema(),
            "play_fit_score": { "type": "integer", "minimum": 0, "maximum": 100 },
            "matched_signal_keys": str_array("Canonical keys supported by the structured observations."),
            "symptom": { "type": "string", "description": "Observable event or workflow symptom, not the root cause." },
            "root_cause": { "type": "string", "description": "Evidence-backed or explicitly hypothetical causal mechanism." },
            "current_workaround": { "type": "string", "description": "How people or systems plausibly compensate today; mark uncertainty." },
            "why_now": { "type": "string", "description": "Observed timing relevance, or empty when none exists." },
            "proof_fit": { "type": "string", "description": "Why the active bounded proof can confirm or kill this hypothesis." },
            "evidence_gaps": str_array("Unknowns that discovery or research must resolve."),
            "disqualifiers": str_array("Observed hard reasons not to pursue this play."),
            "system_concept": { "type": "string" },
            "hard_buyer_question": { "type": "string" },
            "kill_condition": { "type": "string" },
            "magnitude_note": { "type": "string", "description": "Internal-only; never buyer-facing." },
            "applied_principles": str_array("[id]s of book-library principles applied.")
        }
    })
}

fn vantage_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["assignments"],
        "properties": {
            "assignments": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["index", "vantage"],
                    "properties": {
                        "index": { "type": "integer" },
                        "vantage": { "type": "string", "enum": ["process_owner","operator","operational_executive","technical_evaluator","economic_buyer","router"] },
                        "can_observe": { "type": "string" },
                        "why_them": { "type": "string" },
                        "primary": { "type": "boolean" },
                        "route_to": { "type": "string" }
                    }
                }
            }
        }
    })
}

// --- helpers ---------------------------------------------------------------

/// Map Apollo's email_status vocabulary onto ours (verified is the send gate).
fn map_email_status(s: &str) -> String {
    match s.trim().to_lowercase().as_str() {
        "verified" => "verified",
        "likely_to_engage" | "guessed" | "unverified" => "unverified",
        "unavailable" | "" => "unknown",
        _ => "unknown",
    }
    .to_string()
}

fn normalize_vantage(raw: &str) -> String {
    let v = raw.trim().to_lowercase().replace([' ', '-'], "_");
    match v.as_str() {
        "process_owner"
        | "operator"
        | "operational_executive"
        | "technical_evaluator"
        | "economic_buyer"
        | "router" => v,
        "" => "process_owner".to_string(),
        _ => v,
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(120).collect()
}

fn compact_list(values: &[String], limit: usize) -> String {
    if values.is_empty() {
        return "none".into();
    }
    let shown = values.iter().take(limit).cloned().collect::<Vec<_>>();
    if values.len() > shown.len() {
        format!("{} +{}", shown.join(", "), values.len() - shown.len())
    } else {
        shown.join(", ")
    }
}

/// The model chooses the current campaign wedge. These coverage terms keep the
/// long-term market visible without collapsing every sourcing pass into one
/// narrow vertical. Qualification and evidence strength determine easy,
/// medium, and hard priority after discovery.
fn apply_brand_icp_guard(brand: &str, thesis: &str, icp: &mut Icp) {
    let thesis = thesis.to_ascii_lowercase();
    let coverage: &[&str] = if brand.eq_ignore_ascii_case("wapahki") {
        let warehouse_segment = [
            "warehouse",
            "distribution",
            "3pl",
            "fulfillment",
            "logistics",
        ]
        .iter()
        .any(|term| thesis.contains(term));
        let food_segment = !["outside food", "non-food", "excluding food"]
            .iter()
            .any(|term| thesis.contains(term))
            && ["food", "beverage", "bakery", "dairy"]
                .iter()
                .any(|term| thesis.contains(term));
        // Apollo organization keywords are discovery categories, not workflow
        // evidence. Task/equipment phrases ("palletizing", "material
        // handling", "robotics") overwhelmingly return integrators and
        // equipment dealers. The task is established after enumeration from
        // first-party facility/job evidence.
        icp.keywords.retain(|keyword| {
            let keyword = keyword.to_ascii_lowercase();
            ![
                "robot",
                "automation",
                "material handling",
                "pallet",
                "case pack",
                "tray load",
                "machine feed",
                "machine tend",
                "conveyor",
                "order pick",
                "tote",
                "forklift",
                "packaging machine",
            ]
            .iter()
            .any(|term| keyword.contains(term))
        });
        icp.keywords.retain(|keyword| {
            let keyword = keyword.to_ascii_lowercase();
            let irrelevant = if warehouse_segment {
                [
                    "food manufacturing",
                    "food production",
                    "beverage manufacturing",
                    "automotive manufacturing",
                    "medical device manufacturing",
                    "metal fabrication",
                ]
                .as_slice()
            } else if food_segment {
                [
                    "warehousing",
                    "third party logistics",
                    "logistics and supply chain",
                    "automotive manufacturing",
                    "medical device manufacturing",
                    "metal fabrication",
                ]
                .as_slice()
            } else {
                [
                    "food manufacturing",
                    "food production",
                    "beverage manufacturing",
                    "bakery manufacturing",
                    "warehousing",
                    "third party logistics",
                ]
                .as_slice()
            };
            !irrelevant.iter().any(|term| keyword.contains(term))
        });
        if warehouse_segment {
            &[
                "warehousing",
                "third party logistics",
                "logistics and supply chain",
                "distribution center",
                "fulfillment center",
                "contract logistics",
                "cold storage",
                "wholesale distribution",
            ]
        } else if food_segment {
            &[
                "food manufacturing",
                "food production",
                "beverage manufacturing",
                "bakery manufacturing",
                "dairy manufacturing",
                "meat processing",
                "contract food manufacturing",
                "consumer packaged goods",
            ]
        } else {
            &[
                "automotive manufacturing",
                "plastics manufacturing",
                "metal fabrication",
                "medical device manufacturing",
                "consumer goods manufacturing",
                "contract manufacturing",
                "industrial manufacturing",
                "building materials manufacturing",
            ]
        }
    } else if brand.eq_ignore_ascii_case("gnk") {
        &[
            "insurance operations",
            "logistics operations",
            "manufacturing operations",
            "field services",
            "construction operations",
            "healthcare services",
        ]
    } else if brand.eq_ignore_ascii_case("outagehub") {
        &[
            "EV charging network",
            "telecommunications network",
            "cold storage",
            "data center",
            "multi-site retail",
            "facilities management",
            "backup power service",
        ]
    } else {
        return;
    };
    for term in coverage {
        if !icp
            .keywords
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(term))
        {
            icp.keywords.push((*term).to_string());
        }
    }
    icp.keywords.truncate(10);
    icp.locations = if brand.eq_ignore_ascii_case("wapahki") {
        vec!["Ontario, Canada".into()]
    } else {
        vec!["Canada".into()]
    };
}

fn brand_candidate_precheck(brand: &str, org: &ApolloOrg) -> Option<String> {
    let text = format!(
        "{} {} {} {}",
        org.name,
        org.industry,
        org.short_description,
        org.keywords.join(" ")
    )
    .to_ascii_lowercase();
    let has = |terms: &[&str]| terms.iter().any(|term| text.contains(term));

    if brand.eq_ignore_ascii_case("wapahki") {
        if has(&[
            "magazine",
            "media company",
            "news and media",
            "publishing",
            "industry publication",
            "trade publication",
        ]) {
            return Some(
                "Apollo describes a media or publishing company rather than a manufacturer that owns the physical task"
                    .into(),
            );
        }
        let equipment_vendor = has(&[
            "material handling",
            "forklift",
            "packaging machinery",
            "packaging machine",
            "automation equipment",
            "robotic integrator",
            "systems integrator",
            "industrial machinery",
            "machinery manufacturing",
        ]);
        let product_manufacturer = has(&[
            "food production",
            "food manufacturing",
            "food & beverages",
            "food and beverage",
            "beverage manufacturing",
            "consumer goods",
            "consumer products",
            "dairy",
            "bakery",
            "meat processing",
            "seafood processing",
            "co-pack",
            "contract manufacturer",
            "packaging and containers",
            "packaging manufacturer",
        ]);
        if equipment_vendor && !product_manufacturer {
            return Some(
                "Apollo describes an equipment, material-handling, or automation vendor rather than a product manufacturer that owns the candidate physical task".into(),
            );
        }
    } else if brand.eq_ignore_ascii_case("gnk") {
        if has(&[
            "staffing and recruiting",
            "management consulting",
            "marketing and advertising",
            "virtual assistant",
            "outsourcing/offshoring",
            "business process outsourcing",
            "news and media",
            "publishing",
        ]) {
            return Some(
                "Apollo describes a generic staffing, consulting, outsourcing, or media vendor rather than an operator that owns the recurring decision".into(),
            );
        }
    } else if brand.eq_ignore_ascii_case("outagehub") {
        if has(&[
            "electrical contractor",
            "installation services",
            "equipment manufacturer",
            "charging hardware manufacturer",
            "consulting",
            "news and media",
            "publishing",
        ]) && !has(&[
            "network operator",
            "operates",
            "owner and operator",
            "managed services",
            "monitoring",
            "dispatch",
        ]) {
            return Some(
                "Apollo identifies a seller, installer, consultant, or publisher without evidence that it operates, monitors, or dispatches across outage-sensitive sites".into(),
            );
        }
    }
    None
}

fn qualification_skip_detail(classification: &str, value: &OrgQual) -> String {
    let matched = if value.matched_signal_keys.is_empty() {
        "none".to_string()
    } else {
        value.matched_signal_keys.join(",")
    };
    let gaps = value
        .evidence_gaps
        .iter()
        .take(3)
        .map(|gap| first_line(gap))
        .collect::<Vec<_>>()
        .join(" | ");
    format!(
        "{QUALIFICATION_POLICY_TAG} {classification}: {}; fit={}; matched=[{}]; gaps=[{}]; sources=[{}]",
        first_line(&value.reject_reason),
        value.play_fit_score.clamp(0, 100),
        matched,
        gaps,
        compact_list(&value.research_sources, 8),
    )
}

/// Persist repeatable failure *patterns*, not only exact rejected companies.
/// These rows are injected into the next ICP prompt, so a broad Apollo query
/// that surfaced retailers/design publications for an industrial motion gets
/// tighter on the following pass instead of merely avoiding the same domains.
fn record_qualification_patterns(
    db: &SharedDb,
    pb: &Playbook,
    results: &[(ApolloOrg, Result<OrgQual>)],
) -> usize {
    let rejected = results
        .iter()
        .filter_map(|(org, result)| {
            result
                .as_ref()
                .ok()
                .filter(|q| !q.qualified)
                .map(|q| (org, q))
        })
        .collect::<Vec<_>>();
    if rejected.len() < 2 {
        return 0;
    }

    let required = db
        .current_gtm_play(&pb.key)
        .ok()
        .flatten()
        .map(|play| play.minimum_signal_matches.max(1) as usize)
        .unwrap_or(pb.min_signals.max(1));
    let missing_signals = rejected
        .iter()
        .filter(|(_, q)| q.matched_signal_keys.len() < required)
        .count();
    let missing_root_cause = rejected
        .iter()
        .filter(|(_, q)| q.root_cause.trim().is_empty())
        .count();
    let missing_proof = rejected
        .iter()
        .filter(|(_, q)| q.proof_fit.trim().is_empty())
        .count();
    let low_fit = rejected
        .iter()
        .filter(|(_, q)| q.play_fit_score < 65)
        .count();
    let hard_disqualifiers = rejected
        .iter()
        .filter(|(_, q)| !q.disqualifiers.is_empty())
        .count();
    let total = rejected.len();

    let mut industry_counts = HashMap::<String, usize>::new();
    for (org, _) in &rejected {
        let industry = org.industry.trim();
        if !industry.is_empty() {
            *industry_counts.entry(industry.to_string()).or_default() += 1;
        }
    }
    let mut industries = industry_counts.into_iter().collect::<Vec<_>>();
    industries.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let recurring_industries = industries
        .into_iter()
        .filter(|(_, count)| *count >= 2)
        .map(|(industry, count)| format!("{industry} ({count})"))
        .collect::<Vec<_>>();

    let mut saved = 0usize;
    let patterns = [
        (
            "missing_required_signals",
            "Apollo candidates missing required play signals",
            missing_signals,
            format!(
                "{missing_signals}/{total} rejected candidates matched fewer than {required} canonical signals. Tighten keywords and company types toward accounts that visibly exhibit the play's required operational signals."
            ),
        ),
        (
            "weak_root_cause_evidence",
            "Candidates without defensible root-cause evidence",
            missing_root_cause,
            format!(
                "{missing_root_cause}/{total} rejected candidates lacked a defensible root-cause hypothesis. Prefer company evidence that exposes the recurring workflow, current workaround, and decision consequence."
            ),
        ),
        (
            "weak_bounded_proof_fit",
            "Candidates that cannot support the bounded proof",
            missing_proof,
            format!(
                "{missing_proof}/{total} rejected candidates had no credible bounded-proof fit. Tighten toward accounts where the proposed test can be run against a real workflow and measurable stop condition."
            ),
        ),
        (
            "low_play_fit",
            "Apollo candidate pool below the play-fit floor",
            low_fit,
            format!(
                "{low_fit}/{total} rejected candidates scored below 65/100. Do not treat broad industry or size resemblance as qualification; require operational evidence before selecting the account."
            ),
        ),
        (
            "hard_disqualifiers",
            "Hard disqualifiers recurring in Apollo candidates",
            hard_disqualifiers,
            format!(
                "{hard_disqualifiers}/{total} rejected candidates carried hard disqualifiers. Exclude company types that cannot realistically buy, run, or validate this motion."
            ),
        ),
    ];
    for (key, subject, count, detail) in patterns {
        if count >= 2
            && db
                .record_learning(&pb.key, "qualification_pattern", subject, key, &detail)
                .is_ok()
        {
            saved += 1;
        }
    }
    if !recurring_industries.is_empty() {
        let detail = format!(
            "Repeatedly rejected industries in this candidate pool: {}. Treat these as negative evidence unless a company independently shows the required workflow signals.",
            recurring_industries.join(", ")
        );
        if db
            .record_learning(
                &pb.key,
                "qualification_pattern",
                "Industries repeatedly rejected by the active play",
                "rejected_industry_clusters",
                &detail,
            )
            .is_ok()
        {
            saved += 1;
        }
    }
    saved
}

fn canonical_company_domain(raw: &str) -> String {
    raw.trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("www.")
        .trim_end_matches('/')
        .split('/')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
}

/// Durable qualification learnings historically used Apollo id first and
/// domain second. Keep that key stable while the portfolio ownership layer
/// uses both identities at once.
fn org_learning_key(org: &ApolloOrg) -> String {
    if !org.id.trim().is_empty() {
        org.id.clone()
    } else {
        canonical_company_domain(&org.domain())
    }
}

fn company_identity_keys(apollo_id: &str, domain: &str) -> Vec<String> {
    let mut keys = Vec::new();
    if !apollo_id.trim().is_empty() {
        keys.push(format!("apollo:{}", apollo_id.trim()));
    }
    let domain = canonical_company_domain(domain);
    if !domain.is_empty() {
        keys.push(format!("domain:{domain}"));
    }
    keys
}

/// Broad discovery should not spend model budget repeating durable rejects.
/// Explicit operator-curated domains are commonly retried precisely because a
/// current job page or other missing evidence was added. Existing Lead rows are
/// inserted into `seen` separately, so this cannot duplicate CRM accounts.
fn qualification_skip_keys_for_run(
    exact_domains: &[String],
    durable_skip_keys: HashSet<String>,
) -> HashSet<String> {
    if exact_domains.is_empty() {
        durable_skip_keys
    } else {
        HashSet::new()
    }
}

#[cfg(test)]
fn may_revisit_owned_account(
    explicit_domain_rerun: bool,
    portfolio_owner: Option<&str>,
    brand: &str,
) -> bool {
    explicit_domain_rerun && portfolio_owner.is_some_and(|owner| owner.eq_ignore_ascii_case(brand))
}

fn org_identity_keys(org: &ApolloOrg) -> Vec<String> {
    company_identity_keys(&org.id, &org.domain())
}

fn lead_identity_keys(lead: &Lead) -> Vec<String> {
    company_identity_keys(&lead.apollo_org_id, &lead.domain)
}

/// Prepend the brand's accumulated learnings to the operating context so ICP
/// derivation and qualification see them. A no-op when there's nothing learned
/// yet, so a brand's first-ever run behaves exactly as before.
fn augment_context_with_learnings(
    business_context: &str,
    learnings: &[crate::db::Learning],
) -> String {
    if learnings.is_empty() {
        return business_context.to_string();
    }
    let bullets = learnings
        .iter()
        .map(|learning| {
            let seen = if learning.hits > 1 {
                format!("[seen {}×] ", learning.hits)
            } else {
                String::new()
            };
            format!("  - {seen}{}: {}", learning.subject, learning.detail)
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "{}\n\nPRIOR SOURCING LEARNINGS FOR THIS BRAND (exact companies skipped, recurring \
         qualification failures, and contact-coverage problems from earlier runs — refine the \
         Apollo filters accordingly; do NOT re-propose these companies or repeat these patterns):\n{}",
        business_context.trim(),
        bullets,
    )
}

/// Drop Apollo size buckets whose lower bound exceeds the brand's headcount
/// ceiling, so enterprise giants are never fetched. If the model returned only
/// oversized buckets, fall back to every standard bucket within the ceiling —
/// never leaving the size filter empty, which would let giants back in.
fn source_candidate_target(n_accounts: usize, candidate_limit: Option<usize>) -> usize {
    let n_accounts = n_accounts.max(1);
    let ordinary_overfetch = n_accounts.saturating_mul(3).max(25);
    candidate_limit
        .map(|limit| limit.max(n_accounts).min(ordinary_overfetch).max(1))
        .unwrap_or(ordinary_overfetch)
}

fn choose_source_segment<'a>(
    brand: &str,
    thesis: &str,
    icp: &Icp,
    segments: &'a [MarketSegment],
) -> Option<&'a MarketSegment> {
    let context = format!(
        "{} {} {}",
        thesis.to_ascii_lowercase(),
        icp.keywords.join(" ").to_ascii_lowercase(),
        icp.titles.join(" ").to_ascii_lowercase()
    );
    let preferred = if brand.eq_ignore_ascii_case("wapahki") {
        if ["warehouse", "distribution", "3pl", "fulfillment"]
            .iter()
            .any(|term| context.contains(term))
        {
            "ontario_warehouse_case_handling"
        } else if ["food", "beverage", "pack", "pallet"]
            .iter()
            .any(|term| context.contains(term))
        {
            "ontario_food_case_palletizing"
        } else {
            "ontario_manufacturing_machine_tending"
        }
    } else if brand.eq_ignore_ascii_case("gnk") {
        if ["construction", "contractor", "change order"]
            .iter()
            .any(|term| context.contains(term))
        {
            "canada_construction_delay_evidence"
        } else if ["claim", "billing", "eligibility", "filing"]
            .iter()
            .any(|term| context.contains(term))
        {
            "canada_specialty_claims_admin"
        } else {
            "canada_3pl_exception_decisions"
        }
    } else if brand.eq_ignore_ascii_case("outagehub") {
        if ["telecom", "tower", "network operations"]
            .iter()
            .any(|term| context.contains(term))
        {
            "canada_telecom_site_continuity"
        } else if ["generator", "backup power", "refuel"]
            .iter()
            .any(|term| context.contains(term))
        {
            "canada_backup_power_dispatch"
        } else {
            "canada_ev_charging_operations"
        }
    } else {
        ""
    };
    segments
        .iter()
        .find(|segment| segment.key == preferred)
        .or_else(|| segments.first())
}

fn source_query_fingerprint(brand: &str, segment_key: &str, icp: &Icp) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    brand.hash(&mut hasher);
    segment_key.hash(&mut hasher);
    icp.keywords.hash(&mut hasher);
    icp.employee_ranges.hash(&mut hasher);
    icp.locations.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn clamp_employee_ranges(ranges: Vec<String>, max_employees: Option<i64>) -> Vec<String> {
    let Some(max) = max_employees else {
        return ranges;
    };
    let within = |bucket: &str| -> bool {
        bucket
            .split(',')
            .next()
            .and_then(|lo| lo.trim().parse::<i64>().ok())
            .map(|lo| lo <= max)
            .unwrap_or(true)
    };
    let kept: Vec<String> = ranges.into_iter().filter(|b| within(b)).collect();
    if !kept.is_empty() {
        return kept;
    }
    const STD: [&str; 8] = [
        "1,10",
        "11,50",
        "51,200",
        "201,500",
        "501,1000",
        "1001,5000",
        "5001,10000",
        "10001,1000000",
    ];
    STD.iter()
        .filter(|b| within(b))
        .map(|s| s.to_string())
        .collect()
}

/// True when an org's search payload lacks the firmographics qualification needs
/// (industry, headcount, description, keywords) — i.e. it's name+domain only.
fn org_is_thin(o: &ApolloOrg) -> bool {
    o.industry.trim().is_empty()
        && o.short_description.trim().is_empty()
        && o.keywords.is_empty()
        && o.estimated_num_employees == 0
}

/// Fill in a thin org's firmographics via Apollo organization enrichment (by
/// domain). Falls back to the original record if it's already rich, has no
/// domain, or enrichment fails/returns nothing — so sourcing never regresses.
async fn hydrate_org(apollo: &Apollo, mut org: ApolloOrg) -> ApolloOrg {
    if org
        .technology_names
        .iter()
        .any(|name| name == PORTFOLIO_REUSE_MARKER)
    {
        org.technology_names
            .retain(|name| name != PORTFOLIO_REUSE_MARKER);
        return org;
    }
    if !org_is_thin(&org) {
        return org;
    }
    let domain = org.domain();
    if domain.is_empty() {
        return org;
    }
    match apollo.enrich_organization(&domain).await {
        Ok(Some(full)) if !org_is_thin(&full) => full,
        _ => org,
    }
}

// --- Reuse-first inventory -------------------------------------------------

/// Select reusable inventory while omitting accounts that already failed this
/// full-motion attempt. This is how the orchestrator advances to a replacement
/// instead of repeatedly presenting the same held or rejected recipient.
pub fn select_reuse_excluding(
    db: &SharedDb,
    pb: &Playbook,
    brand: &str,
    n_accounts: usize,
    n_contacts: usize,
    excluded_lead_ids: &std::collections::HashSet<String>,
) -> Result<ReuseSelection> {
    let n_accounts = n_accounts.max(1);
    let n_contacts = n_contacts.max(1);
    let leads = db
        .list_leads(Some(brand))?
        .into_iter()
        .filter(|lead| !excluded_lead_ids.contains(&lead.id))
        .collect::<Vec<_>>();
    let people = db.list_people(Some(brand), None)?;
    let current_play = db.current_gtm_play(brand)?;
    let rejected_lead_ids = if let Some(play) = current_play.as_ref() {
        db.list_account_play_assessments(Some(brand))?
            .into_iter()
            .filter(|assessment| assessment.play_id == play.id && assessment.status == "rejected")
            .map(|assessment| assessment.lead_id)
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };

    let mut by_lead: std::collections::HashMap<String, Vec<Person>> =
        std::collections::HashMap::new();
    for person in people {
        by_lead
            .entry(person.lead_id.clone())
            .or_default()
            .push(person);
    }

    let people_on_file = by_lead.values().map(|v| v.len()).sum::<usize>();
    let verified_on_file = by_lead
        .values()
        .flat_map(|v| v.iter())
        .filter(|p| p.email_status.eq_ignore_ascii_case("verified"))
        .count();

    // Only accounts that already have at least one person can be reused without
    // a fresh Apollo people search.
    let mut ranked = leads
        .into_iter()
        .filter_map(|lead| {
            // `leads.status` is legacy/global and may reflect a retired play.
            // A new play must be able to re-research that inventory. Only a
            // rejection attributed to the current play (or a database with no
            // versioned play at all) blocks reuse.
            if rejected_lead_ids.contains(&lead.id)
                || (current_play.is_none() && lead.status.eq_ignore_ascii_case("rejected"))
            {
                return None;
            }
            if pb
                .max_employees
                .is_some_and(|max| lead.headcount > 0 && lead.headcount > max)
            {
                return None;
            }
            let roster = by_lead
                .get(&lead.id)?
                .iter()
                .filter(|person| reusable_workflow_contact(person))
                .cloned()
                .collect::<Vec<_>>();
            if roster.is_empty() {
                return None;
            }
            let ready = roster
                .iter()
                .filter(|person| {
                    crate::gtm::prepare_action(db, brand, &lead.id, person)
                        .is_ok_and(|context| context.sequence_ready_for(2))
                })
                .count();
            let has_contact_coverage = roster.len() >= n_contacts;
            let score = reuse_lead_score(&lead, &roster);
            Some((ready, has_contact_coverage, score, lead, roster))
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| right.2.cmp(&left.2))
            .then_with(|| left.3.name.cmp(&right.3.name))
    });

    let accounts_on_file = ranked.len();
    // A mapped roster is enough to select an account for the refresh step. Its
    // current play assessment may be absent or stale precisely because it has
    // not been refreshed yet; requiring readiness here made the orchestrator
    // skip good existing accounts and spend the turn sourcing weaker ones.
    let reusable_accounts = ranked
        .iter()
        .filter(|(_, covered, _, _, _)| *covered)
        .count();
    let mut selection = ReuseSelection {
        accounts_on_file,
        people_on_file,
        verified_on_file,
        accounts_shortfall: n_accounts.saturating_sub(reusable_accounts),
        ..Default::default()
    };

    for (_ready, _covered, _score, lead, mut roster) in ranked
        .into_iter()
        .filter(|(_, covered, _, _, _)| *covered)
        .take(n_accounts)
    {
        roster.sort_by(|left, right| {
            reuse_person_score(right)
                .cmp(&reuse_person_score(left))
                .then_with(|| left.name.cmp(&right.name))
        });
        let take = roster.into_iter().take(n_contacts).collect::<Vec<_>>();
        selection.accounts_selected += 1;
        selection.lead_ids.push(lead.id);
        for person in take {
            if person.email_status.eq_ignore_ascii_case("verified") {
                selection.verified_selected += 1;
            }
            selection.people_selected += 1;
            selection.person_ids.insert(person.id);
        }
    }
    Ok(selection)
}

fn reusable_workflow_contact(person: &Person) -> bool {
    if !crate::response_design::is_workflow_discovery_contact(&person.title, &person.vantage) {
        return false;
    }
    // Legacy contact maps occasionally promoted finance titles to
    // `process_owner` merely because they could see aggregate cost. That is a
    // later-stage buying vantage, not someone to interview about the workflow.
    let title = person.title.to_ascii_lowercase();
    ![
        "chief financial",
        "cfo",
        "finance",
        "financial controller",
        "accounting",
    ]
    .iter()
    .any(|term| title.contains(term))
}

fn reuse_lead_score(lead: &Lead, people: &[Person]) -> i64 {
    let verified = people
        .iter()
        .filter(|p| p.email_status.eq_ignore_ascii_case("verified"))
        .count() as i64;
    let total = people.len() as i64;
    let doctrine = if lead.hypothesis.trim().is_empty() {
        0
    } else {
        40
    } + if lead.observed_facts.is_empty() {
        0
    } else {
        20
    };
    // Prefer qualified status and denser contact books.
    let status = if lead.status.eq_ignore_ascii_case("qualified") {
        15
    } else {
        0
    };
    verified * 100 + total * 10 + doctrine + status
}

fn reuse_person_score(person: &Person) -> i64 {
    // Role relevance is the primary ordering key. Email availability only
    // breaks ties; it must never drop the best workflow owner before enrichment.
    let mut score =
        crate::response_design::contact_priority(&person.title, &person.vantage, person.primary)
            as i64
            * 10;
    if person.email_status.eq_ignore_ascii_case("verified") {
        score += 2;
    } else if !person.email.trim().is_empty() {
        score += 1;
    }
    score
}

/// Reassess doctrine framing for on-file leads using the current business
/// profile, play, and official-site evidence. No Apollo is spent, but the same
/// qualification policy may now reject a stale or weak account.
#[allow(clippy::too_many_arguments)]
pub async fn refresh_lead_context(
    db: &SharedDb,
    client: &Engine,
    pb: &Playbook,
    business_context: &str,
    library: &Library,
    thesis: &str,
    lead_ids: &[String],
    concurrency: usize,
) -> Result<usize> {
    if lead_ids.is_empty() {
        return Ok(0);
    }
    let active_play = db.current_gtm_play(&pb.key)?;
    let gtm_play_context = crate::gtm::sourcing_play_block(active_play.as_ref());
    let allowed_signal_keys = db
        .list_signal_definitions(Some(&pb.key))?
        .into_iter()
        .filter(|definition| definition.status == "active")
        .map(|definition| definition.key)
        .collect::<HashSet<_>>();
    // A refresh must be capable of learning something new. Reinterpreting the
    // same legacy CRM notes made weak hypotheses look more polished without
    // making them more true. Re-read the official site before reassessment;
    // this is best-effort and never spends Apollo credits.
    let researcher = if research::enabled() {
        ResearchClient::from_env().ok()
    } else {
        None
    };
    let researcher_ref = researcher.as_ref();
    let system = format!(
        "You reassess commercial framing for companies already on file for {name}. Motion: {motion}. \
         Preserve supported account facts, but allow hard disqualifiers or insufficient fit to reject the active play. Separate supported facts, inferences, and a \
         falsifiable workflow hypothesis. Never invent customers, metrics, systems, or dollar impact. \
         Do not invent physical objects, stations, alarms, or private handoffs to make the hypothesis sound concrete. \
         Return only the requested structured data.",
        name = pb.name,
        motion = pb.motion,
    );
    let retrieved = library
        .retrieve_stage(
            &format!("refreshing account hypothesis: {thesis}; {}", pb.motion),
            "companies",
            3,
            1,
        )
        .playbook_block();
    let knowledge = format!("{}\n\n{}", core_strategy_block("companies"), retrieved);
    let concurrency = concurrency.max(1);

    let refresh_ttl_secs = std::env::var("SPRUCE_ACCOUNT_REFRESH_TTL_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(6 * 60 * 60)
        .max(0);
    let refresh_cutoff = Utc::now() - ChronoDuration::seconds(refresh_ttl_secs);
    let mut leads = Vec::new();
    for id in lead_ids {
        // A recent website read is not reusable after the GTM play changes.
        // Otherwise old signals can make an account look action-ready under a
        // new policy without ever receiving the new play's qualification pass.
        let has_current_play_assessment = match active_play.as_ref() {
            Some(play) => db.account_play_assessment(id, &play.id)?.is_some(),
            None => true,
        };
        let recently_refreshed = refresh_ttl_secs > 0
            && has_current_play_assessment
            && db
                .list_active_signal_observations(Some(&pb.key), Some(id), None)?
                .iter()
                .any(|observation| {
                    observation.source_name == "source.refresh"
                        && DateTime::parse_from_rfc3339(&observation.observed_at)
                            .is_ok_and(|observed| observed.with_timezone(&Utc) >= refresh_cutoff)
                });
        if recently_refreshed {
            continue;
        }
        if let Some(lead) = db.get_lead(id)? {
            leads.push(lead);
        }
    }
    if leads.is_empty() {
        return Ok(0);
    }

    let results = stream::iter(leads.into_iter().map(|lead| {
        let system = system.clone();
        let knowledge = knowledge.clone();
        let business_context = business_context.to_string();
        let thesis = thesis.to_string();
        let gtm_play_context = gtm_play_context.clone();
        async move {
            let website_research = match researcher_ref {
                Some(researcher) => {
                    let org = ApolloOrg {
                        id: lead.apollo_org_id.clone(),
                        name: lead.name.clone(),
                        website_url: format!("https://{}", lead.domain),
                        primary_domain: lead.domain.clone(),
                        industry: lead.industry.clone(),
                        estimated_num_employees: lead.headcount,
                        annual_revenue_printed: lead.revenue.clone(),
                        ..Default::default()
                    };
                    research::research_company(client, researcher, pb, &org, &thesis)
                        .await
                        .map(|brief| brief.as_facts_block())
                        .unwrap_or_default()
                }
                None => String::new(),
            };
            let refresh = refresh_one_lead(
                client,
                &system,
                pb,
                &business_context,
                &gtm_play_context,
                &thesis,
                &lead,
                &website_research,
                &knowledge,
            )
            .await;
            (lead, refresh)
        }
    }))
    .buffered(concurrency)
    .collect::<Vec<_>>()
    .await;

    let mut refreshed = 0usize;
    for (mut lead, result) in results {
        match result {
            Ok(mut doc) => {
                merge_prior_refresh_evidence(&lead, &mut doc);
                let routing_status = enforce_refresh_qualification(
                    &mut doc,
                    active_play.as_ref(),
                    &allowed_signal_keys,
                );
                let structured_signals = doc.structured_signals.clone();
                let assessment = active_play.as_ref().map(|play| AccountPlayAssessment {
                    lead_id: lead.id.clone(),
                    brand: pb.key.clone(),
                    play_id: play.id.clone(),
                    play_version: play.version,
                    status: routing_status.clone(),
                    fit_score: doc.play_fit_score,
                    matched_signal_keys: doc.matched_signal_keys.clone(),
                    symptom: doc.symptom.clone(),
                    root_cause: doc.root_cause.clone(),
                    current_workaround: doc.current_workaround.clone(),
                    why_now: doc.why_now.clone(),
                    proof_fit: doc.proof_fit.clone(),
                    evidence_gaps: doc.evidence_gaps.clone(),
                    disqualifiers: doc.disqualifiers.clone(),
                    source: "source.refresh".into(),
                    ..Default::default()
                });
                apply_lead_refresh(&mut lead, doc, thesis);
                lead.status = routing_status.clone();
                let lead_id = db.upsert_lead(&lead)?;
                db.record_signal_candidates(
                    &pb.key,
                    &lead_id,
                    &structured_signals,
                    "source.refresh",
                )?;
                if let Some(assessment) = assessment {
                    db.upsert_account_play_assessment(&assessment)?;
                }
                db.log_event(
                    &pb.key,
                    "",
                    "",
                    if routing_status == "rejected" {
                        "qualification_rejected"
                    } else {
                        "refreshed"
                    },
                    &format!("refreshed framing for {} → {}", lead.name, routing_status),
                )?;
                log_sourcing(format!("✓ refreshed {} → {}", lead.name, routing_status));
                refreshed += 1;
            }
            Err(error) => {
                log_sourcing(format!("! {} refresh error: {error:#}", lead.name));
            }
        }
    }
    Ok(refreshed)
}

#[allow(clippy::too_many_arguments)]
async fn refresh_one_lead(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    business_context: &str,
    gtm_play_context: &str,
    thesis: &str,
    lead: &Lead,
    website_research: &str,
    knowledge: &str,
) -> Result<LeadRefresh> {
    let brand_guard = brand_qualification_guard(&pb.key);
    let facts = json!({
        "name": lead.name,
        "domain": lead.domain,
        "industry": lead.industry,
        "headquarters": lead.hq,
        "estimated_employees": lead.headcount,
        "annual_revenue": lead.revenue,
        "prior_observed_facts": lead.observed_facts,
        "prior_inferences": lead.inferences,
        "prior_hypothesis": lead.hypothesis,
        "prior_mechanism": lead.mechanism,
        "prior_signals": lead.signals,
        "prior_system_concept": lead.system_concept,
    });
    let context_block = if business_context.trim().is_empty() {
        String::new()
    } else {
        format!("{}\n\n", business_context.trim())
    };
    let signal_catalog = crate::gtm::signal_catalog_prompt(&pb.key);
    let website_block = if website_research.trim().is_empty() {
        "OFFICIAL-SITE RESEARCH: unavailable or too thin; do not infer missing facts.\n\n"
            .to_string()
    } else {
        format!("OFFICIAL-SITE RESEARCH:\n{}\n\n", website_research.trim())
    };
    let user = format!(
        "{context_block}{gtm_play_context}\n\nThis company is ALREADY on file. Reassess it against the active play and refresh the commercial \
         framing so outreach copy can explain exactly why THIS company is a fit for the thesis and \
         for the business goals above. Reject it for this play when source evidence is too weak or a hard blocker applies. Prefer keeping prior observed_facts \
         that still hold; tighten or replace weak inferences/hypothesis/mechanism.\n\n\
         THESIS: {thesis}\n\nON-FILE ACCOUNT:\n{facts}\n\n{website_block}{knowledge}\n\n\
         Rules: observed_facts must stay grounded in the on-file fields above, prior facts, or the \
         new official-site research. Inside that research, only `what they do`, `fact`, and a \
         narrowly bounded explicit hiring signal are observations; `signal`, `possible fit`, and \
         `why` remain analyst hypotheses. Put the supporting official URL in source_url for every \
         structured signal derived from the new website evidence. \
         Never invent customers, systems, volumes, or dollar figures. consequence_metric is measurable \
         and non-dollar. A hypothesis or mechanism may name a physical object, station, machine, alarm, \
         dispatch, or private handoff only when the observed facts or official-site research names it. \
         Otherwise stay at the supported task category and make discovery identify the actual work. \
         Do not promote an old inference into concrete copy merely because it sounds plausible. \
         why_this_company: one plain sentence a founder could say out loud. Preserve \
         readable `signals` and map supported evidence to `structured_signals` using only the catalog. \
         Every structured signal's evidence must quote or closely paraphrase a prior_observed_fact; \
         prior_inferences, prior_hypothesis, prior_signals, technology lists, and generic company breadth \
         cannot independently prove a manual workflow, cross-system reconciliation, pain, consequence, \
         or reachable owner. One fact may not be relabeled as several independent required signals. If \
         the observed facts do not support a canonical signal, omit it and name the gap. \
         Separate symptom from root cause, describe the current workaround without asserting guesses \
         as fact, state why the bounded proof fits, and score the account against the same 100-point \
         play-fit rubric used during sourcing. Unknowns go in evidence_gaps; hard blockers go in \
         disqualifiers.\n\n{brand_guard}\n\n{signal_catalog}",
        facts = serde_json::to_string_pretty(&facts).unwrap_or_default(),
    );
    client
        .structured_bulk::<LeadRefresh>("source.refresh", system, &user, refresh_schema())
        .await
}

fn apply_lead_refresh(lead: &mut Lead, doc: LeadRefresh, thesis: &str) {
    if !thesis.trim().is_empty() {
        lead.thesis = thesis.to_string();
    }
    if !doc.observed_facts.is_empty() {
        lead.observed_facts = doc.observed_facts;
    }
    if !doc.inferences.is_empty() {
        lead.inferences = doc.inferences;
    }
    if !doc.hypothesis.trim().is_empty() {
        lead.hypothesis = doc.hypothesis.trim().to_string();
    }
    if !doc.mechanism.trim().is_empty() {
        lead.mechanism = doc.mechanism.trim().to_string();
    }
    if !doc.consequence_metric.trim().is_empty() {
        lead.consequence_metric = doc.consequence_metric.trim().to_string();
    }
    if !doc.signals.is_empty() {
        lead.signals = doc.signals;
    }
    if !doc.system_concept.trim().is_empty() {
        lead.system_concept = doc.system_concept.trim().to_string();
    }
    if !doc.hard_buyer_question.trim().is_empty() {
        lead.hard_buyer_question = doc.hard_buyer_question.trim().to_string();
    }
    if !doc.kill_condition.trim().is_empty() {
        lead.kill_condition = doc.kill_condition.trim().to_string();
    }
    // Fold "why this company" into magnitude_note so the CRM and copy planner
    // both see a crisp internal rationale without a schema migration.
    let why = doc.why_this_company.trim();
    let prior = doc.magnitude_note.trim();
    lead.magnitude_note = match (why.is_empty(), prior.is_empty()) {
        (false, false) => format!("{why} ({prior})"),
        (false, true) => why.to_string(),
        (true, false) => prior.to_string(),
        (true, true) => lead.magnitude_note.clone(),
    };
    if !doc.applied_principles.is_empty() {
        lead.applied_principles = doc.applied_principles;
    }
    if lead.status.trim().is_empty() {
        lead.status = "qualified".into();
    }
}

fn refresh_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["hypothesis", "mechanism", "consequence_metric", "why_this_company", "play_fit_score", "symptom", "root_cause", "current_workaround", "why_now", "proof_fit"],
        "properties": {
            "observed_facts": str_array("Facts still supported by the on-file account payload."),
            "inferences": str_array("Reasonable but unproven guesses."),
            "hypothesis": { "type": "string" },
            "mechanism": { "type": "string" },
            "consequence_metric": { "type": "string", "description": "Measurable consequence, never dollars." },
            "signals": str_array("Independent signals making the hypothesis plausible."),
            "structured_signals": signal_candidates_schema(),
            "play_fit_score": { "type": "integer", "minimum": 0, "maximum": 100 },
            "matched_signal_keys": str_array("Canonical keys supported by the structured observations."),
            "symptom": { "type": "string" },
            "root_cause": { "type": "string" },
            "current_workaround": { "type": "string" },
            "why_now": { "type": "string" },
            "proof_fit": { "type": "string" },
            "evidence_gaps": str_array("Unknowns the next research or discovery step must resolve."),
            "disqualifiers": str_array("Hard reasons this account should not run the active play."),
            "system_concept": { "type": "string" },
            "hard_buyer_question": { "type": "string" },
            "kill_condition": { "type": "string" },
            "magnitude_note": { "type": "string", "description": "Internal-only; never buyer-facing." },
            "applied_principles": str_array("[id]s of book-library principles applied."),
            "why_this_company": { "type": "string", "description": "One plain sentence: why this company specifically." }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        apply_brand_icp_guard, augment_gnk_signals, augment_outage_signals,
        augment_wapahki_signals, brand_candidate_precheck, brand_qualification_guard,
        clamp_employee_ranges, committee_role_for_title, committee_title_groups,
        company_identity_keys, credible_canonical_signal, enforce_play_qualification,
        enforce_refresh_qualification, may_revisit_owned_account, merge_prior_refresh_evidence,
        portfolio_orgs_for_exact_domains, preferred_contact_locations,
        qualification_skip_keys_for_run, reusable_workflow_contact, reuse_lead_score,
        reuse_person_score, reuse_portfolio_people, select_reuse_excluding,
        source_candidate_target, Icp, LeadRefresh, OrgQual,
    };
    use crate::apollo::ApolloOrg;
    use crate::db::{AccountPlayAssessment, Db, Lead, Person};

    #[test]
    fn committee_mapping_has_six_distinct_commercial_roles() {
        let groups = committee_title_groups("wapahki");
        assert_eq!(groups.len(), 6);
        assert_eq!(groups[0].0, "problem_witness");
        assert!(groups.iter().any(|(role, _)| *role == "process_owner"));
        assert!(groups
            .iter()
            .any(|(role, _)| *role == "technical_evaluator"));
        assert!(groups.iter().any(|(role, _)| *role == "economic_buyer"));
        assert_eq!(
            committee_role_for_title("Production Supervisor"),
            "problem_witness"
        );
        assert_eq!(
            committee_role_for_title("Controls Engineering Manager"),
            "technical_evaluator"
        );
        assert_eq!(committee_role_for_title("VP Operations"), "economic_buyer");
    }

    #[test]
    fn outage_facts_are_mapped_to_missing_canonical_labels() {
        let facts = vec![
            "SWTCH provides EV-charging hardware, a cloud platform, load management, billing, monitoring, maintenance, and support for multifamily, workplace, and public/retail properties.".to_string(),
            "SWTCH states that it uses AI-assisted 24/7/365 monitoring, remotely intervenes in charger disruptions, and alerts customers when onsite intervention is required.".to_string(),
            "NRCan lists SWTCH-network public charging stations in Oakville and Guelph.".to_string(),
            "Completed historical analyses found three listed SWTCH-network public charging locations within reported utility-outage areas at the stated utility timestamps.".to_string(),
        ];
        let sources = vec![
            "https://swtchenergy.com/technology/".to_string(),
            "https://natural-resources.canada.ca/stations".to_string(),
            "https://api.outagehub.ca/v1/outages/37535".to_string(),
        ];
        let mut signals = vec![
            SignalCandidate {
                definition_key: "account.fit_evidence".into(),
                evidence: facts[0].clone(),
                source_url: sources[0].clone(),
                confidence: 0.95,
            },
            SignalCandidate {
                definition_key: "account.outage_sensitive_decision".into(),
                evidence: "The company has a customer portal.".into(),
                source_url: sources[0].clone(),
                confidence: 0.9,
            },
        ];
        augment_outage_signals(&facts, &[], &sources, &mut signals);
        for key in [
            "account.distributed_locations",
            "account.outage_sensitive_decision",
            "account.operated_ev_charging_network",
            "account.historical_location_outage_match",
        ] {
            assert!(signals.iter().any(|signal| {
                signal.definition_key == key
                    && crate::qualification::credible_outagehub_signal(key, &signal.evidence)
            }));
        }
    }

    #[test]
    fn wapahki_job_facts_map_to_task_and_pressure_without_faking_lineages() {
        let facts = vec![
            "[https://jobs.example.com/packing] The Ontario production role assembles boxes, hand-packs products, loads cartons into cases, and palletizes finished goods while lifting 50 lb on a night shift."
                .to_string(),
        ];
        let sources = vec!["https://jobs.example.com/packing".to_string()];
        let mut signals = Vec::new();
        augment_wapahki_signals(&facts, &[], &sources, &mut signals);
        for key in [
            "account.fit_evidence",
            "account.bounded_repetitive_task",
            "account.manual_task_economic_pressure",
        ] {
            assert!(signals.iter().any(|signal| signal.definition_key == key));
        }
        assert_eq!(
            signals
                .iter()
                .map(|signal| signal.source_url.as_str())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            1
        );
    }

    #[test]
    fn gnk_job_facts_map_only_explicit_decision_and_artifact_evidence() {
        let facts = vec![
            "[https://jobs.example.com/ar] Each denied claim is reviewed against supporting documentation, corrected, resubmitted, and followed through payment posting to recover reimbursement."
                .to_string(),
        ];
        let sources = vec!["https://jobs.example.com/ar".to_string()];
        let mut signals = Vec::new();
        augment_gnk_signals(&facts, &[], &sources, &mut signals);
        for key in [
            "account.fit_evidence",
            "account.specific_recurring_decision",
            "account.external_trigger_or_mechanism_evidence",
            "account.believable_operating_consequence",
        ] {
            assert!(signals.iter().any(|signal| signal.definition_key == key));
        }

        let mut generic = Vec::new();
        augment_gnk_signals(
            &["The company provides consulting and software services.".into()],
            &[],
            &["https://example.com".into()],
            &mut generic,
        );
        assert!(generic.is_empty());
        assert!(credible_canonical_signal(
            "gnk",
            "account.specific_recurring_decision",
            "Recurring job-duty evidence: each denied claim is reviewed, corrected, and resubmitted for payment."
        ));
        assert!(credible_canonical_signal(
            "gnk",
            "account.external_trigger_or_mechanism_evidence",
            "Each claim payment is reconciled against a bank statement and posting batch."
        ));
    }

    #[test]
    fn refresh_does_not_stretch_legacy_notes_into_atomic_claims() {
        let play = default_plays()
            .into_iter()
            .find(|play| play.brand == "gnk")
            .expect("gnk play");
        let allowed = play
            .required_signal_keys
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let lead = Lead {
            observed_facts: vec![
                "Each denied claim is reviewed against supporting documentation, corrected, resubmitted, and followed through payment recovery."
                    .into(),
            ],
            signals: vec!["The recurring review uses claim records and supporting documents.".into()],
            ..Default::default()
        };
        let mut refresh = LeadRefresh {
            observed_facts: vec!["The company provides revenue-cycle services.".into()],
            play_fit_score: 70,
            root_cause: "The exact exception path is not yet confirmed.".into(),
            proof_fit: "A bounded historical case review can test the path.".into(),
            structured_signals: vec![SignalCandidate {
                definition_key: "account.fit_evidence".into(),
                evidence: "The company provides revenue-cycle services.".into(),
                source_url: "https://example.com/claims".into(),
                confidence: 0.9,
            }],
            ..Default::default()
        };

        merge_prior_refresh_evidence(&lead, &mut refresh);
        let status = enforce_refresh_qualification(&mut refresh, Some(&play), &allowed);

        assert_eq!(status, "research_required");
        assert!(!refresh
            .matched_signal_keys
            .iter()
            .any(|key| key == "account.specific_recurring_decision"));
        assert!(!refresh
            .matched_signal_keys
            .iter()
            .any(|key| key == "account.external_trigger_or_mechanism_evidence"));
    }

    #[test]
    fn wapahki_guard_requires_a_task_bridge_and_an_economic_bridge() {
        let guard = brand_qualification_guard("wapahki");
        assert!(guard.contains("Company category alone"));
        assert!(guard.contains("physical candidate task"));
        assert!(guard.contains("medium-priority"));
        assert!(guard.contains("economic"));
    }

    #[test]
    fn wapahki_icp_uses_buyer_categories_not_equipment_terms() {
        let mut icp = Icp {
            keywords: vec!["material handling".into(), "robotics".into()],
            employee_ranges: vec![],
            locations: vec![],
            titles: vec![],
            seniorities: vec![],
        };
        apply_brand_icp_guard(
            "wapahki",
            "Ontario warehouses and distribution centres",
            &mut icp,
        );
        assert!(!icp.keywords.contains(&"material handling".to_string()));
        assert!(!icp.keywords.contains(&"robotics".to_string()));
        assert!(icp.keywords.contains(&"warehousing".to_string()));
        assert!(icp.keywords.contains(&"third party logistics".to_string()));

        let mut non_food = Icp {
            keywords: vec!["food manufacturing".into(), "automotive suppliers".into()],
            employee_ranges: vec![],
            locations: vec![],
            titles: vec![],
            seniorities: vec![],
        };
        apply_brand_icp_guard(
            "wapahki",
            "Ontario product manufacturing outside food with machine tending",
            &mut non_food,
        );
        assert!(!non_food
            .keywords
            .contains(&"food manufacturing".to_string()));
        assert!(non_food
            .keywords
            .contains(&"automotive manufacturing".to_string()));
    }

    #[test]
    fn wapahki_precheck_rejects_machinery_sellers_but_not_food_plants() {
        let seller = ApolloOrg {
            industry: "Machinery Manufacturing".into(),
            short_description: "Material handling and packaging machinery".into(),
            ..Default::default()
        };
        let plant = ApolloOrg {
            industry: "Food Production".into(),
            short_description: "Manufacturer of frozen baked goods".into(),
            ..Default::default()
        };
        assert!(brand_candidate_precheck("wapahki", &seller).is_some());
        assert!(brand_candidate_precheck("wapahki", &plant).is_none());
    }

    #[test]
    fn portfolio_identity_uses_both_apollo_id_and_canonical_domain() {
        let keys = company_identity_keys("org-123", "https://www.Example.com/path");
        assert_eq!(
            keys,
            vec![
                "apollo:org-123".to_string(),
                "domain:example.com".to_string()
            ]
        );
    }

    #[test]
    fn exact_domain_reuses_the_richest_canonical_portfolio_account() {
        let leads = vec![
            Lead {
                brand: "gnk".into(),
                apollo_org_id: "org-thin".into(),
                name: "Thin profile".into(),
                domain: "https://www.example.com/about".into(),
                ..Default::default()
            },
            Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-rich".into(),
                name: "Example Distribution".into(),
                domain: "example.com".into(),
                industry: "Warehousing".into(),
                hq: "Mississauga, Ontario, Canada".into(),
                headcount: 240,
                ..Default::default()
            },
        ];
        let orgs = portfolio_orgs_for_exact_domains(&leads, &["example.com".into()]);
        assert_eq!(orgs.len(), 1);
        assert_eq!(orgs[0].id, "org-rich");
        assert_eq!(orgs[0].name, "Example Distribution");
        assert_eq!(orgs[0].estimated_num_employees, 240);
    }

    #[test]
    fn verified_people_are_reused_across_brand_opportunities() {
        let path = std::env::temp_dir().join(format!(
            "spruce-portfolio-contact-test-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let db = Db::open(&path).expect("open temp db");
        let source_lead = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-shared".into(),
                name: "Shared Factory".into(),
                domain: "shared.example".into(),
                ..Default::default()
            })
            .expect("source lead");
        let target_lead = db
            .upsert_lead(&Lead {
                brand: "wapahki".into(),
                apollo_org_id: "org-shared".into(),
                name: "Shared Factory".into(),
                domain: "shared.example".into(),
                ..Default::default()
            })
            .expect("target lead");
        db.upsert_person(&Person {
            lead_id: source_lead,
            brand: "outagehub".into(),
            apollo_person_id: "person-1".into(),
            name: "Pat Operator".into(),
            title: "Plant Operations Manager".into(),
            vantage: "process_owner".into(),
            why_them: "Outage-specific rationale that must not cross brands".into(),
            email: "pat@shared.example".into(),
            email_status: "verified".into(),
            status: "verified".into(),
            ..Default::default()
        })
        .expect("source person");

        assert_eq!(
            reuse_portfolio_people(&db, "wapahki", &target_lead, "shared.example", 8)
                .expect("reuse people"),
            1
        );
        let people = db
            .list_people(Some("wapahki"), None)
            .expect("target people");
        assert_eq!(people.len(), 1);
        assert_eq!(people[0].email, "pat@shared.example");
        assert_eq!(people[0].email_status, "verified");
        assert!(people[0].why_them.is_empty());

        drop(db);
        for candidate in [
            path.clone(),
            std::path::PathBuf::from(format!("{}-wal", path.display())),
            std::path::PathBuf::from(format!("{}-shm", path.display())),
        ] {
            let _ = std::fs::remove_file(candidate);
        }
    }

    #[test]
    fn gnk_signals_require_operating_evidence_not_department_or_portal_presence() {
        assert!(!credible_canonical_signal(
            "gnk",
            "account.expensive_recurring_workflow",
            "The website presents Claims as a dedicated operating function."
        ));
        assert!(!credible_canonical_signal(
            "gnk",
            "account.cross_system_reconciliation",
            "The website exposes several separately named portals and systems."
        ));
        assert!(credible_canonical_signal(
            "gnk",
            "account.expensive_recurring_workflow",
            "Adjusters manually review each exception, adding hours of handling time."
        ));
        assert!(credible_canonical_signal(
            "gnk",
            "account.cross_system_reconciliation",
            "Staff assemble the decision record manually across three systems."
        ));
    }

    #[test]
    fn outagehub_signals_reject_office_lists_and_generic_emergency_service() {
        assert!(!credible_canonical_signal(
            "outagehub",
            "account.distributed_locations",
            "The electrical contractor has offices in Vancouver, Burnaby, Kelowna, and Calgary."
        ));
        assert!(!credible_canonical_signal(
            "outagehub",
            "account.outage_sensitive_decision",
            "Technicians provide 24/7 emergency electrical service."
        ));
        assert!(credible_canonical_signal(
            "outagehub",
            "account.distributed_locations",
            "The operator runs automated cold-storage facilities across Ontario, Alberta, and Quebec."
        ));
        assert!(credible_canonical_signal(
            "outagehub",
            "account.outage_sensitive_decision",
            "After a loss-of-power alarm, operators decide whether to dispatch maintenance or hold the response when a utility outage is reported."
        ));
    }
    use crate::gtm::{default_plays, SignalCandidate};
    use crate::playbook::Playbooks;

    #[test]
    fn clamp_drops_buckets_above_the_ceiling() {
        let ranges = vec![
            "201,500".to_string(),
            "501,1000".to_string(),
            "1001,5000".to_string(),
            "10001,1000000".to_string(),
        ];
        let kept = clamp_employee_ranges(ranges, Some(1000));
        assert_eq!(kept, vec!["201,500".to_string(), "501,1000".to_string()]);
    }

    #[test]
    fn clamp_falls_back_to_standard_buckets_when_all_oversized() {
        // Model returned only enterprise buckets → we still emit the in-ceiling set.
        let kept = clamp_employee_ranges(vec!["10001,1000000".to_string()], Some(1000));
        assert_eq!(
            kept,
            vec![
                "1,10".to_string(),
                "11,50".to_string(),
                "51,200".to_string(),
                "201,500".to_string(),
                "501,1000".to_string()
            ]
        );
    }

    #[test]
    fn clamp_is_a_noop_without_a_ceiling() {
        let ranges = vec!["10001,1000000".to_string()];
        assert_eq!(clamp_employee_ranges(ranges.clone(), None), ranges);
    }

    #[test]
    fn full_motion_can_bound_deep_qualification_without_weakening_standalone_source() {
        assert_eq!(source_candidate_target(3, None), 25);
        assert_eq!(source_candidate_target(3, Some(6)), 6);
        assert_eq!(source_candidate_target(1, Some(4)), 4);
        // A caller cannot ask to evaluate fewer candidates than account slots.
        assert_eq!(source_candidate_target(5, Some(2)), 5);
    }

    #[test]
    fn explicit_domains_reopen_prior_qualification_skips() {
        let durable = [
            "domain:qcfoods.ca".to_string(),
            "apollo:quinte-custom-foods".to_string(),
        ]
        .into_iter()
        .collect::<HashSet<_>>();

        assert_eq!(
            qualification_skip_keys_for_run(&[], durable.clone()),
            durable
        );
        assert!(qualification_skip_keys_for_run(&["qcfoods.ca".into()], durable).is_empty());
    }

    #[test]
    fn wapahki_people_search_starts_in_ontario() {
        assert_eq!(
            preferred_contact_locations("wapahki"),
            vec!["Ontario, Canada"]
        );
        assert_eq!(preferred_contact_locations("outagehub"), vec!["Canada"]);
        assert!(preferred_contact_locations("gnk").is_empty());
    }

    #[test]
    fn explicit_same_brand_domain_can_rebuild_its_contact_bench() {
        assert!(may_revisit_owned_account(
            true,
            Some("outagehub"),
            "outagehub"
        ));
        assert!(!may_revisit_owned_account(
            false,
            Some("outagehub"),
            "outagehub"
        ));
        assert!(!may_revisit_owned_account(true, Some("gnk"), "outagehub"));
    }

    #[test]
    fn reuse_scores_prefer_verified_primary_process_owners() {
        let primary = Person {
            primary: true,
            email_status: "verified".into(),
            vantage: "process_owner".into(),
            ..Default::default()
        };
        let router = Person {
            primary: false,
            email_status: "unknown".into(),
            vantage: "router".into(),
            ..Default::default()
        };
        assert!(reuse_person_score(&primary) > reuse_person_score(&router));

        let rich = Lead {
            hypothesis: "they run a NOC".into(),
            observed_facts: vec!["utility ops".into()],
            status: "qualified".into(),
            ..Default::default()
        };
        let thin = Lead::default();
        let people = vec![primary];
        assert!(reuse_lead_score(&rich, &people) > reuse_lead_score(&thin, &people));
    }

    #[test]
    fn verification_cannot_outrank_a_more_relevant_workflow_role() {
        let owner = Person {
            title: "Director of Claims".into(),
            vantage: "process_owner".into(),
            email_status: "unknown".into(),
            ..Default::default()
        };
        let verified_operator = Person {
            title: "Claims Analyst".into(),
            vantage: "operator".into(),
            email: "analyst@example.com".into(),
            email_status: "verified".into(),
            ..Default::default()
        };
        assert!(reuse_person_score(&owner) > reuse_person_score(&verified_operator));
    }

    #[test]
    fn refresh_can_reject_weak_or_disqualified_inventory() {
        let play = default_plays()
            .into_iter()
            .find(|play| play.brand == "gnk")
            .expect("gnk play");
        let allowed = play
            .required_signal_keys
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut refresh = LeadRefresh {
            observed_facts: vec!["The company has a claims department.".into()],
            play_fit_score: 35,
            root_cause: "Unproven".into(),
            proof_fit: "Unproven".into(),
            structured_signals: vec![SignalCandidate {
                definition_key: "account.fit_evidence".into(),
                evidence: "The company has a claims department.".into(),
                confidence: 0.55,
                ..Default::default()
            }],
            disqualifiers: vec!["The active play is outside the account's scope.".into()],
            ..Default::default()
        };

        let status = enforce_refresh_qualification(&mut refresh, Some(&play), &allowed);
        assert_eq!(status, "rejected");
        assert!(refresh.structured_signals.is_empty());
        assert_eq!(refresh.disqualifiers.len(), 1);
    }

    #[test]
    fn reuse_excludes_later_stage_buyers_and_misclassified_finance_titles() {
        let operator = Person {
            title: "Claims Operations Manager".into(),
            vantage: "process_owner".into(),
            ..Default::default()
        };
        let cfo = Person {
            title: "Chief Financial Officer".into(),
            vantage: "economic_buyer".into(),
            ..Default::default()
        };
        let mislabeled_controller = Person {
            title: "Financial Controller".into(),
            vantage: "process_owner".into(),
            ..Default::default()
        };
        assert!(reusable_workflow_contact(&operator));
        assert!(!reusable_workflow_contact(&cfo));
        assert!(!reusable_workflow_contact(&mislabeled_controller));
    }

    #[test]
    fn reuse_selection_does_not_reselect_a_failed_account() {
        let db = std::sync::Arc::new(Db::open(":memory:").expect("open memory db"));
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-failed-motion".into(),
                name: "Failed Motion Account".into(),
                ..Default::default()
            })
            .expect("insert lead");
        db.upsert_person(&Person {
            lead_id: lead_id.clone(),
            brand: "outagehub".into(),
            apollo_person_id: "person-failed-motion".into(),
            name: "Operations Owner".into(),
            title: "Director of Operations".into(),
            vantage: "process_owner".into(),
            ..Default::default()
        })
        .expect("insert person");
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let playbook = playbooks.get("outagehub").expect("outagehub playbook");

        let visible = select_reuse_excluding(&db, playbook, "outagehub", 1, 1, &HashSet::new())
            .expect("select inventory");
        assert_eq!(visible.accounts_on_file, 1);

        let excluded = HashSet::from([lead_id]);
        let replacement = select_reuse_excluding(&db, playbook, "outagehub", 1, 1, &excluded)
            .expect("select replacement inventory");
        assert_eq!(replacement.accounts_on_file, 0);
        assert!(replacement.lead_ids.is_empty());
    }

    #[test]
    fn reuse_selection_refreshes_mapped_accounts_before_requiring_play_readiness() {
        let db = std::sync::Arc::new(Db::open(":memory:").expect("open memory db"));
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-awaiting-refresh".into(),
                name: "Mapped Distributed Operator".into(),
                status: "research_needed".into(),
                ..Default::default()
            })
            .expect("insert lead");
        db.upsert_person(&Person {
            lead_id: lead_id.clone(),
            brand: "outagehub".into(),
            apollo_person_id: "person-awaiting-refresh".into(),
            name: "Network Operations Owner".into(),
            title: "Director of Network Operations".into(),
            vantage: "process_owner".into(),
            email_status: "verified".into(),
            ..Default::default()
        })
        .expect("insert person");
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let playbook = playbooks.get("outagehub").expect("outagehub playbook");

        let selected = select_reuse_excluding(&db, playbook, "outagehub", 1, 1, &HashSet::new())
            .expect("select inventory for refresh");
        assert_eq!(selected.accounts_selected, 1);
        assert_eq!(selected.accounts_shortfall, 0);
        assert_eq!(selected.lead_ids, vec![lead_id]);
    }

    #[test]
    fn retired_play_rejection_can_be_researched_but_current_rejection_cannot() {
        let db = std::sync::Arc::new(Db::open(":memory:").expect("open memory db"));
        let old_reject_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-retired-reject".into(),
                name: "Retired Play Reject".into(),
                status: "rejected".into(),
                ..Default::default()
            })
            .expect("insert old rejected lead");
        db.upsert_person(&Person {
            lead_id: old_reject_id.clone(),
            brand: "outagehub".into(),
            apollo_person_id: "person-retired-reject".into(),
            name: "Operations Owner".into(),
            title: "Director of Network Operations".into(),
            vantage: "process_owner".into(),
            ..Default::default()
        })
        .expect("insert old rejected person");

        let current_reject_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-current-reject".into(),
                name: "Current Play Reject".into(),
                ..Default::default()
            })
            .expect("insert current rejected lead");
        db.upsert_person(&Person {
            lead_id: current_reject_id.clone(),
            brand: "outagehub".into(),
            apollo_person_id: "person-current-reject".into(),
            name: "Service Operations Owner".into(),
            title: "Director of Service Operations".into(),
            vantage: "process_owner".into(),
            ..Default::default()
        })
        .expect("insert current rejected person");
        let play = db
            .current_gtm_play("outagehub")
            .expect("play query")
            .expect("current play");
        db.upsert_account_play_assessment(&AccountPlayAssessment {
            lead_id: current_reject_id,
            brand: "outagehub".into(),
            play_id: play.id,
            play_version: play.version,
            status: "rejected".into(),
            source: "test".into(),
            ..Default::default()
        })
        .expect("current rejection");

        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let playbook = playbooks.get("outagehub").expect("outagehub playbook");
        let selected = select_reuse_excluding(&db, playbook, "outagehub", 2, 1, &HashSet::new())
            .expect("select reusable inventory");
        assert_eq!(selected.lead_ids, vec![old_reject_id]);
    }

    #[test]
    fn active_play_rejects_superficial_account_fit() {
        let play = default_plays()
            .into_iter()
            .find(|play| play.brand == "outagehub")
            .expect("outagehub play");
        let allowed = play
            .required_signal_keys
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut qualification = OrgQual {
            qualified: true,
            play_fit_score: 82,
            structured_signals: vec![SignalCandidate {
                definition_key: "account.distributed_locations".into(),
                evidence: "Operates sites in three provinces.".into(),
                confidence: 0.9,
                ..Default::default()
            }],
            ..Default::default()
        };

        enforce_play_qualification(&mut qualification, Some(&play), &allowed);

        assert!(!qualification.qualified);
        assert!(qualification
            .reject_reason
            .contains("canonical play signal"));
        assert!(qualification.reject_reason.contains("root-cause"));
        assert!(qualification.reject_reason.contains("bounded proof"));
    }

    #[test]
    fn active_play_accepts_evidenced_root_cause_and_proof_fit() {
        let play = default_plays()
            .into_iter()
            .find(|play| play.brand == "outagehub")
            .expect("outagehub play");
        let allowed = play
            .required_signal_keys
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let structured_signals = [
            (
                "account.fit_evidence",
                "Operates a Canadian EV charging network with public site locations.",
            ),
            (
                "account.outage_sensitive_decision",
                "Service Operations checks utility status before dispatch or customer communication when a charging-site availability incident occurs.",
            ),
            (
                "account.distributed_locations",
                "Operates multiple charging sites across Canada.",
            ),
            (
                "account.historical_location_outage_match",
                "On 2026-07-14 at 14:30, the charging site at 123 King Street overlapped a utility outage area in a utility report.",
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (definition_key, evidence))| SignalCandidate {
            definition_key: definition_key.into(),
            evidence: evidence.into(),
            source_url: if index == 3 {
                "https://utility.example/outages/2026-07-14".into()
            } else {
                "https://operator.example/charging-network".into()
            },
            confidence: 0.85,
            ..Default::default()
        })
        .collect();
        let mut qualification = OrgQual {
            qualified: true,
            play_fit_score: 78,
            structured_signals,
            root_cause: "Site telemetry cannot supply external utility context.".into(),
            proof_fit: "Replay three historical alarms against public outage records.".into(),
            ..Default::default()
        };

        enforce_play_qualification(&mut qualification, Some(&play), &allowed);

        assert!(qualification.qualified);
        assert_eq!(qualification.matched_signal_keys.len(), 4);
        assert_eq!(qualification.routing_status, "qualified");
    }

    #[test]
    fn active_play_routes_plausible_two_signal_account_to_discovery() {
        let play = default_plays()
            .into_iter()
            .find(|play| play.brand == "wapahki")
            .expect("wapahki play");
        let allowed = play
            .required_signal_keys
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let structured_signals = [
            (
                "account.fit_evidence",
                "Produces packaged food in an operating plant.",
            ),
            (
                "account.bounded_repetitive_task",
                "Each production run moves sealed cases from the packing conveyor to pallets.",
            ),
        ]
        .into_iter()
        .map(|(definition_key, evidence)| SignalCandidate {
            definition_key: definition_key.into(),
            evidence: evidence.into(),
            source_url: "https://example.com/current-job".into(),
            confidence: 0.8,
        })
        .collect();
        let mut qualification = OrgQual {
            qualified: false,
            play_fit_score: 62,
            structured_signals,
            root_cause: "The exact task and exception rate remain unverified.".into(),
            proof_fit: "A task review could confirm or kill the hypothesis.".into(),
            disqualifiers: vec![
                "No public evidence confirms exception-heavy manual handling.".into(),
            ],
            ..Default::default()
        };

        enforce_play_qualification(&mut qualification, Some(&play), &allowed);

        assert!(!qualification.qualified);
        assert_eq!(qualification.routing_status, "research_needed");
        assert!(qualification.disqualifiers.is_empty());
        assert!(qualification
            .evidence_gaps
            .iter()
            .any(|gap| gap.contains("No public evidence")));
    }

    #[test]
    fn one_source_page_cannot_masquerade_as_independent_qualification_evidence() {
        let play = default_plays()
            .into_iter()
            .find(|play| play.brand == "wapahki")
            .expect("wapahki play");
        let allowed = play
            .required_signal_keys
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let same_page = "https://manufacturer.example/services";
        let mut qualification = OrgQual {
            qualified: true,
            play_fit_score: 82,
            structured_signals: vec![
                SignalCandidate {
                    definition_key: "account.fit_evidence".into(),
                    evidence: "The company operates a packaged-food plant.".into(),
                    source_url: same_page.into(),
                    confidence: 0.9,
                },
                SignalCandidate {
                    definition_key: "account.bounded_repetitive_task".into(),
                    evidence: "The page names a recurring transfer station.".into(),
                    source_url: same_page.into(),
                    confidence: 0.9,
                },
                SignalCandidate {
                    definition_key: "account.manual_task_economic_pressure".into(),
                    evidence: "The same page names throughput pressure at that station.".into(),
                    source_url: same_page.into(),
                    confidence: 0.9,
                },
            ],
            root_cause: "The evidenced station has a bounded automation constraint.".into(),
            proof_fit: "A task review could confirm or kill the constraint.".into(),
            ..Default::default()
        };

        enforce_play_qualification(&mut qualification, Some(&play), &allowed);

        assert!(!qualification.qualified);
        assert_eq!(qualification.routing_status, "research_needed");
        assert!(qualification
            .evidence_gaps
            .iter()
            .any(|gap| gap.contains("1 independent source lineage")));
    }
}
