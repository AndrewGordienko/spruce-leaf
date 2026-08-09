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
use std::sync::Arc;

use crate::apollo::{Apollo, ApolloOrg, ApolloPerson, OrgFilters, PeopleFilters};
use crate::calendar;
use crate::db::{AccountPlayAssessment, Lead, Person, SharedDb};
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

/// What one `source` run accomplished.
#[derive(Debug, Default)]
pub struct SourceSummary {
    pub orgs_found: usize,
    pub leads_qualified: usize,
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
    qualified: bool,
    /// qualified | research_needed | rejected. Computed locally after the model
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
    concurrency: usize,
    progress: Option<SourceProgressReporter>,
) -> Result<SourceSummary> {
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
    let mut skip_learnings = db
        .recent_learnings(Some(&pb.key), Some("qualification_skip"), 15)
        .unwrap_or_default();
    skip_learnings.retain(|learning| !is_legacy_two_signal_reject(&learning.detail));
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
    icp.employee_ranges = clamp_employee_ranges(icp.employee_ranges, pb.max_employees);
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

    // 2. Real organizations (overfetch so qualification can be selective). Reuse
    // across runs: never re-qualify a company we've already judged. Rejects live
    // in the learning store; companies we already qualified are Lead rows on file.
    // Combining both lets `source` spend its qualification budget only on genuinely
    // NEW companies — and page deeper when a page is all-known, so a re-run ADDS
    // leads instead of stalling on the same top results.
    const MAX_SOURCE_PAGES: u32 = 5;
    let mut seen: std::collections::HashSet<String> = db
        .durable_qualification_skip_keys(&pb.key)
        .unwrap_or_default()
        .into_iter()
        .collect();
    let on_file = db.list_leads(Some(&pb.key)).unwrap_or_default();
    for lead in &on_file {
        let key = lead_dedup_key(lead);
        if !key.is_empty() {
            seen.insert(key);
        }
    }

    let want_candidates = (n_accounts * 2).clamp(10, 100);
    let per_page = want_candidates as u32;
    report_source(
        progress.as_ref(),
        "apollo",
        "Searching Apollo",
        format!(
            "Looking for about {want_candidates} candidates across up to {MAX_SOURCE_PAGES} pages"
        ),
        "active",
    );
    let mut fresh: Vec<ApolloOrg> = Vec::new();
    let mut orgs_found = 0usize;
    let mut skipped_known = 0usize;
    for page in 1..=MAX_SOURCE_PAGES {
        let page_orgs = apollo
            .search_organizations(&OrgFilters {
                keywords: icp.keywords.clone(),
                employee_ranges: icp.employee_ranges.clone(),
                locations: icp.locations.clone(),
                page,
                per_page,
                ..Default::default()
            })
            .await?;
        if page_orgs.is_empty() {
            break;
        }
        orgs_found += page_orgs.len();
        for org in page_orgs {
            let key = org_learning_key(&org);
            // Skip anything already judged; `insert` returning false also guards
            // the same company reappearing across pages.
            if !key.is_empty() && !seen.insert(key) {
                skipped_known += 1;
                continue;
            }
            fresh.push(org);
        }
        if fresh.len() >= want_candidates {
            break;
        }
    }
    report_source(
        progress.as_ref(),
        "apollo",
        "Found candidate companies",
        format!(
            "{orgs_found} returned · {} new to qualify · {skipped_known} already judged · {} qualified on file",
            fresh.len(),
            on_file.len(),
        ),
        "complete",
    );
    let mut summary = SourceSummary {
        orgs_found,
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
    // Each candidate is I/O-bound (website reads + an LLM qualification call), so
    // fan out wider than the global concurrency default — a low default (2) makes
    // the qualification stage crawl even though it is almost entirely waiting. With
    // ~10 overfetched orgs this usually clears qualification in a single wave.
    let batch_size = concurrency.max(8);
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
    let mut skipped = 0usize;
    let mut errors = 0usize;
    while let Some((org, result)) = results.next().await {
        reviewed += 1;
        let latest = match &result {
            Ok(value) if value.qualified => {
                if value.routing_status == "research_needed" {
                    research_needed += 1;
                    format!(
                        "Latest: research needed {} · fit {}/100",
                        org.name, value.play_fit_score
                    )
                } else {
                    qualified += 1;
                    format!(
                        "Latest: passed {} · fit {}/100",
                        org.name, value.play_fit_score
                    )
                }
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
                    &format!("{classification}: {}", first_line(&value.reject_reason)),
                );
                format!(
                    "Latest: skipped {} · {}",
                    org.name,
                    first_line(&value.reject_reason)
                )
            }
            Err(error) => {
                errors += 1;
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
                "{reviewed}/{qualification_total} reviewed · {qualified} qualified · {research_needed} research-needed · {skipped} skipped · {errors} errors\n{latest}"
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
            Ok(qualification) if qualification.qualified => Some((org, qualification)),
            _ => None,
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        (right.1.routing_status == "qualified")
            .cmp(&(left.1.routing_status == "qualified"))
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
    let selected_research = selected_count.saturating_sub(selected_qualified);
    report_source(
        progress.as_ref(),
        "qualification",
        if selected_research == 0 {
            "Ranked qualified companies"
        } else {
            "Ranked discovery candidates"
        },
        format!(
            "{reviewed}/{qualification_total} reviewed · {qualified} qualified · {research_needed} research-needed\nSelected {selected_count}: {selected_qualified} qualified · {selected_research} research-needed"
        ),
        if selected_count > 0 { "complete" } else { "warning" },
    );

    // Persist the highest-ranked leads and their play-version assessment, then
    // source people only at those winners.
    let mut winners: Vec<(ApolloOrg, Lead, String)> = Vec::new();
    for (org, q) in ranked.into_iter().take(n_accounts) {
        let structured_signals = q.structured_signals.clone();
        let assessment = active_play.as_ref().map(|play| AccountPlayAssessment {
            brand: pb.key.clone(),
            play_id: play.id.clone(),
            play_version: play.version,
            status: if q.routing_status.is_empty() {
                "qualified".into()
            } else {
                q.routing_status.clone()
            },
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
            status: "qualified".into(),
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
                "qualified lead {} (play fit {}/100; ranked by evidence + root cause)",
                org.name, q.play_fit_score
            ),
        )?;
        summary.leads_qualified += 1;
        winners.push((org, lead, lead_id));
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
            let result = source_people(
                db,
                client,
                apollo,
                pb,
                vantage_system,
                &org,
                &lead,
                &lead_id,
                icp,
                n_contacts,
                fallback_recipient_timezone,
            )
            .await;
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
    let mut result = if let Some(max) = pb
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
        let research_block = match researcher {
            Some(researcher) => research::research_company(client, researcher, pb, &org)
                .await
                .map(|brief| brief.as_facts_block())
                .unwrap_or_default(),
            None => String::new(),
        };
        qualify_org(
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
        .await
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

    qualification.structured_signals.retain(|signal| {
        allowed_signal_keys.contains(signal.definition_key.trim())
            && !signal.evidence.trim().is_empty()
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
    let minimum = play.minimum_signal_matches.max(1) as usize;
    let fully_supported = required_matches >= minimum
        && !qualification.root_cause.trim().is_empty()
        && !qualification.proof_fit.trim().is_empty()
        && qualification.play_fit_score >= 65
        && qualification.disqualifiers.is_empty();
    let has_account_fit = matched.iter().any(|key| key == "account.fit_evidence");
    let has_source_backed_fit = has_account_fit || !qualification.observed_facts.is_empty();
    let discovery_candidate = has_source_backed_fit
        && required_matches >= minimum.saturating_sub(1).max(1)
        && qualification.play_fit_score >= 50
        && qualification.disqualifiers.is_empty();

    if fully_supported {
        qualification.qualified = true;
        qualification.routing_status = "qualified".into();
        qualification.reject_reason.clear();
    } else if discovery_candidate {
        // This is exactly what cold customer discovery is for: the company is a
        // plausible buyer and one workflow signal is still unknown. Keep it in
        // the working set, but label the uncertainty so copy asks for a
        // correction instead of asserting the pain as fact.
        qualification.qualified = true;
        qualification.routing_status = "research_needed".into();
        qualification.reject_reason.clear();
        qualification.evidence_gaps.push(format!(
            "Only {required_matches}/{minimum} required play signals are publicly supported; outreach must test the missing signal rather than claim it."
        ));
    } else {
        qualification.qualified = false;
        qualification.routing_status = "rejected".into();
        let mut reasons = Vec::new();
        if required_matches < minimum {
            reasons.push(format!(
                "only {required_matches} canonical play signal(s) matched; {minimum} required for full qualification"
            ));
        }
        if qualification.play_fit_score < 50 {
            reasons.push(format!(
                "play-fit score {}/100 is below the 50 discovery floor",
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

/// Canonical signals are stronger than topical resemblance. In particular,
/// GnK's old qualifier repeatedly relabeled "has a Claims department" as an
/// expensive recurring workflow and "has several portals" as human
/// reconciliation. Those facts can support account fit, but not the operating
/// condition the play needs before multi-touch outreach.
fn credible_canonical_signal(brand: &str, key: &str, evidence: &str) -> bool {
    if !brand.eq_ignore_ascii_case("gnk") {
        return true;
    }
    let text = evidence.to_ascii_lowercase();
    let has = |terms: &[&str]| terms.iter().any(|term| text.contains(term));
    match key.trim() {
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
    ]
    .iter()
    .any(|needle| value.contains(needle))
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
    let source_pool = n_contacts
        .saturating_mul(CONTACT_BACKFILL_FACTOR)
        .max(n_contacts);
    let people = gather_people(apollo, org, icp, source_pool).await;
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
            vantage: normalize_vantage(va.map(|v| v.vantage.as_str()).unwrap_or("")),
            can_observe: va.map(|v| v.can_observe.clone()).unwrap_or_default(),
            why_them: va.map(|v| v.why_them.clone()).unwrap_or_default(),
            primary: va.map(|v| v.primary).unwrap_or(false),
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
    org: &ApolloOrg,
    icp: &Icp,
    n_contacts: usize,
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

    // Ordered strategies: most targeted first so kept contacts are best-fit, then
    // progressively broader so we still reach the count.
    let mut attempts: Vec<(&str, PeopleFilters)> = Vec::new();
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

    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<ApolloPerson> = Vec::new();
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
         workflow (by vantage, not just seniority). Keep keywords concrete and industry-specific. \
         Employee ranges must use Apollo's bucket format like \"51,200\".{firmographic} If the thesis \
         implies a region, set locations.\n\n{doctrine}",
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
         guess in inferences. consequence_metric is a measurable consequence, NOT dollars. If at \
         least {min} independent signals don't support the hypothesis, set qualified=false with a \
         one-line reject_reason. Preserve the readable `signals` list, and also map every supported \
         observation you can to `structured_signals` using the canonical catalog. Every canonical \
         signal needs its own direct Apollo or first-party website evidence. A technology name, broad \
         product range, company scale, or the existence of several portals does not by itself prove \
         a manual workflow, cross-system reconciliation, pain, material consequence, or reachable \
         workflow owner. Do not relabel one generic fact as several independent required signals, and \
         never invent a signal merely to satisfy the catalog. Missing public evidence is an `evidence_gap`, never \
         a `disqualifier`; reserve disqualifiers for affirmative evidence that the company cannot \
         realistically run, buy, or validate the motion. A plausible manufacturer with account-fit \
         evidence and one supported workflow signal may remain a discovery candidate even when the \
         exact manual task or exception pattern must be tested in outreach. Root-cause analysis must separate the observable \
         symptom/event from the underlying missing information, coordination, capability, or system \
         boundary that plausibly produces it; name the current human workaround and mark anything \
         unproven as hypothesis. Explain why the active play's bounded proof could confirm or kill \
         that cause. Score play fit 0-100: 30 signal/decision evidence, 25 root-cause + workaround \
         clarity, 20 reachable stakeholder vantage, 15 bounded-proof fit, 10 credible timing/why-now. \
         A generic industry, technology, hiring, or scale match cannot score 65. Put real blockers in \
         disqualifiers and unknown evidence in evidence_gaps.\n\n{signal_catalog}",
        facts = serde_json::to_string_pretty(&facts).unwrap_or_default(),
        min = pb.min_signals,
    );
    client
        .structured_bulk::<OrgQual>("source.qualify", system, &user, qual_schema())
        .await
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
         For each person, assign the vantage point that best fits what they may observe, decide, or route \
         (not their seniority). can_observe must be a cautious 5-15 word note about likely access. \
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
            "required": ["definition_key", "evidence", "confidence"],
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

fn is_legacy_two_signal_reject(detail: &str) -> bool {
    !detail.starts_with("hard_reject:")
        && !detail.starts_with("insufficient_fit:")
        && detail.starts_with("only 2 canonical play signal(s) matched")
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

/// Stable dedup handle for a company across runs: prefer Apollo's org id, fall
/// back to its domain. Used to recognize a company we've already judged.
fn org_learning_key(org: &ApolloOrg) -> String {
    if !org.id.trim().is_empty() {
        org.id.clone()
    } else {
        org.domain()
    }
}

/// Dedup handle for a stored Lead, mirroring [`org_learning_key`] so a company
/// already qualified in an earlier run is recognized and never re-qualified.
fn lead_dedup_key(lead: &Lead) -> String {
    if !lead.apollo_org_id.trim().is_empty() {
        lead.apollo_org_id.clone()
    } else {
        lead.domain.clone()
    }
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
async fn hydrate_org(apollo: &Apollo, org: ApolloOrg) -> ApolloOrg {
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

/// Pick the strongest on-file accounts/people for a brand so a full motion can
/// skip Apollo when the CRM already has enough inventory. Preference order:
/// more verified contacts, richer doctrine, more recent update.
pub fn select_reuse(
    db: &SharedDb,
    pb: &Playbook,
    brand: &str,
    n_accounts: usize,
    n_contacts: usize,
) -> Result<ReuseSelection> {
    let n_accounts = n_accounts.max(1);
    let n_contacts = n_contacts.max(1);
    let leads = db.list_leads(Some(brand))?;
    let people = db.list_people(Some(brand), None)?;

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
    // An account with one historical contact, or no evidence-ready contact at
    // all, is inventory but not coverage for a "N people per company" motion.
    // Treat it as a shortfall so the full motion sources replacements instead
    // of selecting weak rows and holding them only after the expensive refresh.
    let reusable_accounts = ranked
        .iter()
        .filter(|(ready, covered, _, _, _)| *ready > 0 && *covered)
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
        .filter(|(ready, covered, _, _, _)| *ready > 0 && *covered)
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
    let vantage = person.vantage.to_ascii_lowercase();
    if !matches!(
        vantage.as_str(),
        "process_owner" | "operator" | "operational_executive"
    ) {
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
    let mut score = if person.primary { 100 } else { 0 };
    if person.email_status.eq_ignore_ascii_case("verified") {
        score += 80;
    } else if !person.email.trim().is_empty() {
        score += 20;
    }
    let vantage = person.vantage.to_ascii_lowercase();
    score += match vantage.as_str() {
        "process_owner" => 70,
        "operator" => 65,
        "operational_executive" => 55,
        "economic_buyer" => 40,
        "technical_evaluator" => 25,
        "router" => 10,
        _ => 0,
    };
    score
}

/// Rewrite doctrine framing for already-qualified leads using the current
/// business profile + thesis. No Apollo, no re-qualification rejects — the
/// company stays; only the commercial "why them" is refreshed for better copy.
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
        "You refresh commercial framing for companies already qualified for {name}. Motion: {motion}. \
         Keep the company; rewrite why it fits now. Separate supported facts, inferences, and a \
         falsifiable workflow hypothesis. Never invent customers, metrics, systems, or dollar impact. \
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
        let recently_refreshed = refresh_ttl_secs > 0
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
                    research::research_company(client, researcher, pb, &org)
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
                doc.structured_signals.retain(|signal| {
                    credible_canonical_signal(&pb.key, &signal.definition_key, &signal.evidence)
                });
                doc.matched_signal_keys = doc
                    .structured_signals
                    .iter()
                    .map(|signal| signal.definition_key.trim().to_string())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                doc.matched_signal_keys.sort();
                let structured_signals = doc.structured_signals.clone();
                let assessment = active_play.as_ref().map(|play| {
                    let matched = structured_signals
                        .iter()
                        .map(|signal| signal.definition_key.trim())
                        .filter(|key| {
                            play.required_signal_keys
                                .iter()
                                .any(|required| required == key)
                        })
                        .collect::<HashSet<_>>()
                        .len();
                    AccountPlayAssessment {
                        lead_id: lead.id.clone(),
                        brand: pb.key.clone(),
                        play_id: play.id.clone(),
                        play_version: play.version,
                        status: if matched >= play.minimum_signal_matches.max(1) as usize
                            && doc.play_fit_score >= 65
                            && !doc.root_cause.trim().is_empty()
                            && !doc.proof_fit.trim().is_empty()
                            && doc.disqualifiers.is_empty()
                        {
                            "qualified".into()
                        } else {
                            "research_needed".into()
                        },
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
                    }
                });
                apply_lead_refresh(&mut lead, doc, thesis);
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
                    "refreshed",
                    &format!("refreshed framing for {}", lead.name),
                )?;
                log_sourcing(format!("✓ refreshed {}", lead.name));
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
         for the business goals above. Do not reject the company. Prefer keeping prior observed_facts \
         that still hold; tighten or replace weak inferences/hypothesis/mechanism.\n\n\
         THESIS: {thesis}\n\nON-FILE ACCOUNT:\n{facts}\n\n{website_block}{knowledge}\n\n\
         Rules: observed_facts must stay grounded in the on-file fields above, prior facts, or the \
         new official-site research. Inside that research, only `what they do`, `fact`, and a \
         narrowly bounded explicit hiring signal are observations; `signal`, `possible fit`, and \
         `why` remain analyst hypotheses. Put the supporting official URL in source_url for every \
         structured signal derived from the new website evidence. \
         Never invent customers, systems, volumes, or dollar figures. consequence_metric is measurable \
         and non-dollar. why_this_company: one plain sentence a founder could say out loud. Preserve \
         readable `signals` and map supported evidence to `structured_signals` using only the catalog. \
         Every structured signal's evidence must quote or closely paraphrase a prior_observed_fact; \
         prior_inferences, prior_hypothesis, prior_signals, technology lists, and generic company breadth \
         cannot independently prove a manual workflow, cross-system reconciliation, pain, consequence, \
         or reachable owner. One fact may not be relabeled as several independent required signals. If \
         the observed facts do not support a canonical signal, omit it and name the gap. \
         Separate symptom from root cause, describe the current workaround without asserting guesses \
         as fact, state why the bounded proof fits, and score the account against the same 100-point \
         play-fit rubric used during sourcing. Unknowns go in evidence_gaps; hard blockers go in \
         disqualifiers.\n\n{signal_catalog}",
        facts = serde_json::to_string_pretty(&facts).unwrap_or_default(),
    );
    let _ = pb; // brand motion already in system prompt
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
        clamp_employee_ranges, credible_canonical_signal, enforce_play_qualification,
        reusable_workflow_contact, reuse_lead_score, reuse_person_score, OrgQual,
    };

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
    use crate::db::{Lead, Person};
    use crate::gtm::{default_plays, SignalCandidate};

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
            ("account.fit_evidence", "Runs a 24/7 operations desk."),
            (
                "account.outage_sensitive_decision",
                "Operators dispatch or hold after a loss-of-power alarm.",
            ),
            (
                "account.distributed_locations",
                "Operates remote locations across utility territories.",
            ),
        ]
        .into_iter()
        .map(|(definition_key, evidence)| SignalCandidate {
            definition_key: definition_key.into(),
            evidence: evidence.into(),
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
        assert_eq!(qualification.matched_signal_keys.len(), 3);
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
                "account.format_variability",
                "The public product catalog spans many case and pack formats.",
            ),
        ]
        .into_iter()
        .map(|(definition_key, evidence)| SignalCandidate {
            definition_key: definition_key.into(),
            evidence: evidence.into(),
            confidence: 0.8,
            ..Default::default()
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

        assert!(qualification.qualified);
        assert_eq!(qualification.routing_status, "research_needed");
        assert!(qualification.disqualifiers.is_empty());
        assert!(qualification
            .evidence_gaps
            .iter()
            .any(|gap| gap.contains("No public evidence")));
    }
}
