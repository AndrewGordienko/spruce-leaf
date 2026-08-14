//! Read-only supervised outreach acceptance exports.
//!
//! This module deliberately does not research, generate, label, approve,
//! schedule, or send. It assembles the current evidence inventory into the
//! samples required by each brand's acceptance doctrine and reports every
//! missing gate without manufacturing quota-fillers.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::db::{
    AccountPlayAssessment, Lead, MessageCandidateAudit, OpportunityStakeholder, Person,
    SalesOpportunity, SharedDb,
};

#[derive(Debug, Serialize)]
struct QualificationRow {
    company_id: String,
    company: String,
    domain: String,
    industry: String,
    headquarters: String,
    sample_segment: String,
    deep_review_selected: bool,
    decision: String,
    difficulty: String,
    assessment_status: String,
    fit_score: i64,
    reason: String,
    why_now: String,
    proof_fit: String,
    exact_facility: Option<serde_json::Value>,
    task_or_decision: String,
    consequence: String,
    known_facts: Vec<String>,
    inferences: Vec<String>,
    evidence_gaps: Vec<String>,
    disqualifiers: Vec<String>,
    source_urls: Vec<String>,
    ready_for_outreach: bool,
    readiness_failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ContactRecord {
    company_id: String,
    company: String,
    sample_segment: String,
    person_id: String,
    name: String,
    title: String,
    email: String,
    email_status: String,
    linkedin_url: String,
    mapped_role: String,
    relationship_to_workflow: String,
    relationship_evidence_claim_ids: Vec<String>,
    employer_verification: String,
    employer_source_url: String,
    current_employment_supported: bool,
    direct_task_ownership_supported: bool,
    primary_recipient: bool,
    contact_failure: Option<String>,
}

#[derive(Debug, Serialize)]
struct CandidateRecord {
    company_id: String,
    sequence_id: String,
    person_id: String,
    candidate: MessageCandidateAudit,
    selector_a_candidate_id: String,
    selector_a_pass: bool,
    selector_a_score: i64,
    selector_a_reason: String,
    selector_b_candidate_id: String,
    selector_b_pass: bool,
    selector_b_score: i64,
    selector_b_reason: String,
    agreed_candidate_id: String,
    selected_content_hash: String,
    exact_copy_human_approved: bool,
    evaluation_partition: String,
}

#[derive(Debug, Serialize)]
struct HumanReviewTemplate {
    company_id: String,
    sequence_id: String,
    candidate_id: String,
    subject: String,
    body: String,
    send_unchanged: Option<bool>,
    factually_supported: Option<bool>,
    correct_recipient: Option<bool>,
    concrete_recipient_value: Option<bool>,
    sounds_human: Option<bool>,
    rejection_reason: Option<String>,
    preferred_candidate: Option<String>,
    reviewer: String,
    reviewed_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct SimilarityFinding {
    left_candidate_id: String,
    right_candidate_id: String,
    left_company_id: String,
    right_company_id: String,
    token_jaccard: f64,
    repeated_opening: bool,
    repeated_question: bool,
    violation: bool,
}

#[derive(Debug, Serialize)]
struct FailureReport {
    company_qualification_failures: Vec<String>,
    contact_selection_failures: Vec<String>,
    evidence_failures: Vec<String>,
    message_quality_failures: Vec<String>,
    selector_failures: Vec<String>,
    template_repetition_failures: Vec<String>,
    missing_data_and_operational_blockers: Vec<String>,
}

#[derive(Debug, Serialize)]
struct AcceptanceManifest {
    brand: String,
    generated_at: String,
    read_only: bool,
    external_send_attempted: bool,
    sample_requested: usize,
    companies_selected: usize,
    qualification_decisions: HashMap<String, usize>,
    contacts_exported: usize,
    contacts_with_current_employment_support: usize,
    missing_buyer_map_roles: usize,
    candidate_mode_decisions: usize,
    available_candidate_messages: usize,
    unavailable_candidate_modes: usize,
    selector_agreements: usize,
    exact_copy_human_approvals: usize,
    selected_messages: usize,
    similarity_violations: usize,
    controlled_allowlist_configured: bool,
    controlled_delivery_recorded: bool,
    pass: bool,
    release_requirements: Vec<String>,
}

#[derive(Clone)]
struct SelectedLead {
    lead: Lead,
    segment: String,
    assessment: Option<AccountPlayAssessment>,
}

pub struct ExportSummary {
    pub directory: PathBuf,
    pub companies: usize,
    pub contacts: usize,
    pub candidates: usize,
    pub passed: bool,
}

pub fn export(db: &SharedDb, brand: &str, output_root: &Path) -> Result<ExportSummary> {
    let brand = brand.to_ascii_lowercase();
    if !matches!(brand.as_str(), "wapahki" | "gnk" | "outagehub") {
        anyhow::bail!("unsupported acceptance brand {brand}");
    }
    let directory = output_root.join(&brand);
    fs::create_dir_all(&directory)
        .with_context(|| format!("creating acceptance export {}", directory.display()))?;

    let leads = db.list_leads(Some(&brand))?;
    let current_play = db.current_gtm_play(&brand)?;
    let assessments = db.list_account_play_assessments(Some(&brand))?;
    let current_assessments = assessments
        .into_iter()
        .filter(|assessment| {
            current_play
                .as_ref()
                .is_some_and(|play| assessment.play_id == play.id)
        })
        .map(|assessment| (assessment.lead_id.clone(), assessment))
        .collect::<HashMap<_, _>>();
    let market_accounts = db.list_market_accounts(Some(&brand))?;
    let market_segments = db.list_market_segments(Some(&brand))?;
    let memberships = db.list_market_segment_memberships(&brand)?;
    let mut account_segments = HashMap::<String, Vec<String>>::new();
    for (market_account_id, segment) in memberships {
        account_segments
            .entry(market_account_id)
            .or_default()
            .push(segment);
    }
    let lead_market_account = leads
        .iter()
        .filter_map(|lead| {
            market_accounts
                .iter()
                .find(|account| {
                    (!lead.apollo_org_id.is_empty() && account.apollo_org_id == lead.apollo_org_id)
                        || (!lead.domain.is_empty()
                            && account.canonical_domain.eq_ignore_ascii_case(&lead.domain))
                })
                .map(|account| (lead.id.clone(), account.id.clone()))
        })
        .collect::<HashMap<_, _>>();
    let opportunity_history = db.list_sales_opportunity_history(&brand)?;
    let segment_keys_by_id = market_segments
        .iter()
        .map(|segment| (segment.id.clone(), canonical_segment_key(&segment.key)))
        .collect::<HashMap<_, _>>();
    let mut lead_segments = HashMap::<String, Vec<String>>::new();
    for lead in &leads {
        if let Some(market_account_id) = lead_market_account.get(&lead.id) {
            if let Some(segments) = account_segments.get(market_account_id) {
                lead_segments
                    .entry(lead.id.clone())
                    .or_default()
                    .extend(segments.iter().map(|key| canonical_segment_key(key)));
            }
        }
    }
    for opportunity in &opportunity_history {
        if let Some(segment) = segment_keys_by_id.get(&opportunity.segment_id) {
            lead_segments
                .entry(opportunity.lead_id.clone())
                .or_default()
                .push(segment.clone());
        }
    }
    if brand == "outagehub" {
        for lead in &leads {
            let assessment = current_assessments.get(&lead.id);
            let evidence = format!(
                "{} {} {} {} {}",
                lead.industry,
                lead.observed_facts.join(" "),
                lead.signals.join(" "),
                assessment
                    .map(|item| item.symptom.as_str())
                    .unwrap_or_default(),
                assessment
                    .map(|item| item.why_now.as_str())
                    .unwrap_or_default(),
            );
            if let Some(segment) = crate::segments::segment_for_evidence(&evidence) {
                if let Some(key) = crate::segments::market_key_for_segment(segment.key) {
                    lead_segments
                        .entry(lead.id.clone())
                        .or_default()
                        .push(key.into());
                }
            }
        }
    }
    for lead in &leads {
        let text = format!("{} {}", lead.name, lead.industry).to_ascii_lowercase();
        let heuristic_keys: Vec<&str> = match brand.as_str() {
            "wapahki" => {
                if contains_any(&text, &["food", "beverage", "grocery", "bakery", "farm"]) {
                    vec!["canada_food_case_palletizing"]
                } else if contains_any(
                    &text,
                    &[
                        "logistics",
                        "warehouse",
                        "distribution",
                        "fulfillment",
                        "retail",
                    ],
                ) {
                    vec!["canada_warehouse_case_handling"]
                } else if contains_any(
                    &text,
                    &[
                        "packaging",
                        "container",
                        "plastic",
                        "automotive",
                        "manufactur",
                        "metal",
                        "chemical",
                        "consumer goods",
                        "building material",
                        "pharma",
                    ],
                ) {
                    vec!["canada_manufacturing_machine_tending"]
                } else {
                    Vec::new()
                }
            }
            "gnk" => {
                if contains_any(
                    &text,
                    &["logistics", "supply chain", "transportation", "warehouse"],
                ) {
                    vec!["canada_3pl_exception_decisions"]
                } else if contains_any(
                    &text,
                    &["construction", "engineering", "architecture", "project"],
                ) {
                    vec!["canada_construction_delay_evidence"]
                } else if contains_any(
                    &text,
                    &["insurance", "health", "medical", "billing", "claims"],
                ) {
                    vec!["canada_specialty_claims_admin"]
                } else {
                    Vec::new()
                }
            }
            _ => {
                if contains_any(&text, &["charging", " ev ", "electric vehicle"]) {
                    vec!["canada_ev_charging_operations"]
                } else if contains_any(&text, &["telecom", "communications", "network", "internet"])
                {
                    vec!["canada_telecom_site_continuity"]
                } else if contains_any(&text, &["insurance", "emergency", "risk", "municipal"]) {
                    vec!["canada_outage_insurance_cat"]
                } else if contains_any(
                    &text,
                    &[
                        "health",
                        "hospital",
                        "laborator",
                        "pharma",
                        "senior",
                        "care",
                    ],
                ) {
                    vec!["canada_outage_labs_healthcare"]
                } else if contains_any(
                    &text,
                    &[
                        "cold",
                        "refriger",
                        "food",
                        "logistics",
                        "warehouse",
                        "produce",
                    ],
                ) {
                    vec!["canada_outage_cold_storage"]
                } else if contains_any(
                    &text,
                    &["generator", "backup power", "energy", "electric", "power"],
                ) {
                    vec!["canada_backup_power_dispatch"]
                } else {
                    Vec::new()
                }
            }
        };
        lead_segments
            .entry(lead.id.clone())
            .or_default()
            .extend(heuristic_keys.into_iter().map(str::to_string));
    }
    for segments in lead_segments.values_mut() {
        segments.sort();
        segments.dedup();
    }
    let selected = select_sample(&brand, leads, &current_assessments, &lead_segments);

    let opportunities = db.list_sales_opportunities(Some(&brand), None)?;
    let people = db.list_people(Some(&brand), None)?;
    let sequences = db.list_sequences(Some(&brand))?;
    let selected_lead_ids = selected
        .iter()
        .map(|item| item.lead.id.as_str())
        .collect::<HashSet<_>>();
    let outage_holdout_sequences = if brand == "outagehub" {
        let mut ids = sequences
            .iter()
            .filter(|sequence| selected_lead_ids.contains(sequence.lead_id.as_str()))
            .map(|sequence| sequence.id.clone())
            .collect::<Vec<_>>();
        ids.sort_by_key(|id| stable_hash(id));
        ids.into_iter().take(10).collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };

    let mut qualification = Vec::new();
    let mut contacts = Vec::new();
    let mut candidate_records = Vec::new();
    let mut human_review = Vec::new();
    let mut company_failures = Vec::new();
    let mut contact_failures = Vec::new();
    let mut evidence_failures = Vec::new();
    let mut message_failures = Vec::new();
    let mut selector_failures = Vec::new();
    let detailed_lead_ids = if brand == "wapahki" {
        let mut ranked = selected.iter().collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            assessment_rank(left.assessment.as_ref())
                .cmp(&assessment_rank(right.assessment.as_ref()))
                .then_with(|| {
                    right
                        .assessment
                        .as_ref()
                        .map_or(0, |item| item.fit_score)
                        .cmp(&left.assessment.as_ref().map_or(0, |item| item.fit_score))
                })
        });
        ranked
            .into_iter()
            .take(10)
            .map(|item| item.lead.id.clone())
            .collect::<HashSet<_>>()
    } else {
        selected
            .iter()
            .map(|item| item.lead.id.clone())
            .collect::<HashSet<_>>()
    };

    for selected_lead in &selected {
        let lead = &selected_lead.lead;
        let opportunity = best_opportunity(&opportunities, &lead.id);
        let stakeholders = if let Some(opportunity) = opportunity {
            db.list_opportunity_stakeholders(Some(&opportunity.id), Some(&brand))?
        } else {
            Vec::new()
        };
        let claims = if let Some(opportunity) = opportunity {
            db.list_evidence_claims(Some(&opportunity.id), Some(&brand))?
        } else {
            Vec::new()
        };
        let source_urls = claims
            .iter()
            .filter_map(|claim| {
                let url = claim.source_url.trim();
                (!url.is_empty()).then(|| url.to_string())
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let facility = opportunity.and_then(|opportunity| {
            db.list_facilities(Some(&opportunity.market_account_id))
                .ok()
                .and_then(|items| {
                    items
                        .into_iter()
                        .find(|facility| facility.id == opportunity.facility_id)
                })
        });
        let mut readiness_failures = Vec::new();
        let brief = stakeholders.iter().find_map(|stakeholder| {
            db.get_sales_brief(
                opportunity.map_or("", |item| item.id.as_str()),
                &stakeholder.person_id,
            )
            .ok()
            .flatten()
        });
        if let Some(brief) = &brief {
            readiness_failures.extend(brief.gate_issues());
            match db.get_acquisition_context(&brief.sales_opportunity_id, &brief.person_id)? {
                Some(context) => readiness_failures.extend(context.gate_issues()),
                None => readiness_failures.push(
                    "no approved acquisition source/channel context for this recipient".into(),
                ),
            }
        } else {
            readiness_failures.push("no founder/compliance-approved sales brief".into());
        }
        if source_urls.is_empty() {
            readiness_failures.push("no current opportunity-bound source URL lineage".into());
        }
        let decision = if selected_lead
            .assessment
            .as_ref()
            .is_some_and(|assessment| assessment.status == "rejected")
        {
            "reject".to_string()
        } else if let Some(brief) = &brief {
            brief.account_decision.clone()
        } else {
            "nurture_until_trigger".to_string()
        };
        let exact_facility = facility.as_ref().map(|facility| {
            serde_json::json!({
                "id": facility.id,
                "name": facility.name,
                "address": facility.address,
                "city": facility.city,
                "region": facility.region,
                "country": facility.country,
                "source_url": facility.source_url,
            })
        });
        let difficulty = difficulty(
            selected_lead.assessment.as_ref(),
            opportunity,
            facility.is_some(),
        );
        let assessment = selected_lead.assessment.as_ref();
        let ready = decision == "send" && readiness_failures.is_empty();
        if !ready {
            company_failures.push(format!(
                "{}: {} ({})",
                lead.name,
                decision,
                readiness_failures.join("; ")
            ));
        }
        if source_urls.is_empty() {
            evidence_failures.push(format!(
                "{}: no current opportunity-bound cited public evidence",
                lead.name
            ));
        }
        qualification.push(QualificationRow {
            company_id: lead.id.clone(),
            company: lead.name.clone(),
            domain: lead.domain.clone(),
            industry: lead.industry.clone(),
            headquarters: lead.hq.clone(),
            sample_segment: selected_lead.segment.clone(),
            deep_review_selected: detailed_lead_ids.contains(&lead.id),
            decision,
            difficulty,
            assessment_status: assessment
                .map_or("unassessed", |item| item.status.as_str())
                .into(),
            fit_score: assessment.map_or(0, |item| item.fit_score),
            reason: assessment
                .map(|item| item.symptom.clone())
                .unwrap_or_else(|| "No current-play assessment exists.".into()),
            why_now: assessment
                .map(|item| item.why_now.clone())
                .unwrap_or_default(),
            proof_fit: assessment
                .map(|item| item.proof_fit.clone())
                .unwrap_or_default(),
            exact_facility,
            task_or_decision: opportunity
                .map(|item| item.task_or_decision.clone())
                .unwrap_or_default(),
            consequence: opportunity
                .map(|item| item.consequence.clone())
                .unwrap_or_default(),
            known_facts: lead.observed_facts.clone(),
            inferences: lead.inferences.clone(),
            evidence_gaps: assessment
                .map(|item| item.evidence_gaps.clone())
                .unwrap_or_else(|| vec!["No current-play assessment exists.".into()]),
            disqualifiers: assessment
                .map(|item| item.disqualifiers.clone())
                .unwrap_or_default(),
            source_urls,
            ready_for_outreach: ready,
            readiness_failures,
        });

        if !detailed_lead_ids.contains(&lead.id) {
            continue;
        }
        let company_people = people
            .iter()
            .filter(|person| person.lead_id == lead.id)
            .cloned()
            .collect::<Vec<_>>();
        let ranked_people = rank_people(company_people, lead, &stakeholders);
        if ranked_people.is_empty() {
            contact_failures.push(format!("{}: no contact records", lead.name));
        }
        for (index, person) in ranked_people.into_iter().take(5).enumerate() {
            let stakeholder = stakeholders
                .iter()
                .find(|stakeholder| stakeholder.person_id == person.id);
            let employment_supported = employment_supported(&person, lead);
            let direct = stakeholder.is_some_and(|item| {
                item.role_fit == "direct"
                    && !item.evidence_claim_ids.is_empty()
                    && !item.relationship_to_task.trim().is_empty()
            });
            let primary = index == 0 && employment_supported && (direct || stakeholder.is_some());
            let failure = if !employment_supported {
                Some("current employment is not independently supported".into())
            } else if stakeholder.is_none() {
                Some(
                    "title/vantage exists but no opportunity-specific relationship evidence".into(),
                )
            } else {
                None
            };
            if let Some(failure) = &failure {
                contact_failures.push(format!("{} / {}: {failure}", lead.name, person.name));
            }
            contacts.push(ContactRecord {
                company_id: lead.id.clone(),
                company: lead.name.clone(),
                sample_segment: selected_lead.segment.clone(),
                person_id: person.id.clone(),
                name: person.name.clone(),
                title: person.title.clone(),
                email: person.email.clone(),
                email_status: person.email_status.clone(),
                linkedin_url: person.linkedin_url.clone(),
                mapped_role: stakeholder
                    .map(|item| item.role.clone())
                    .unwrap_or_else(|| person.vantage.clone()),
                relationship_to_workflow: stakeholder
                    .map(|item| item.relationship_to_task.clone())
                    .unwrap_or_else(|| person.can_observe.clone()),
                relationship_evidence_claim_ids: stakeholder
                    .map(|item| item.evidence_claim_ids.clone())
                    .unwrap_or_default(),
                employer_verification: person.employer_verification.clone(),
                employer_source_url: person.employer_source_url.clone(),
                current_employment_supported: employment_supported,
                direct_task_ownership_supported: direct,
                primary_recipient: primary,
                contact_failure: failure,
            });
        }

        for sequence in sequences
            .iter()
            .filter(|sequence| sequence.lead_id == lead.id)
        {
            let audits = db.list_message_candidate_audit(&sequence.id)?;
            let selection = db.get_message_selection_audit(&sequence.id)?;
            let partition = if outage_holdout_sequences.contains(&sequence.id) {
                "sealed_holdout".to_string()
            } else {
                "development".to_string()
            };
            for candidate in audits {
                let approved = db
                    .list_touches_for_sequence(&sequence.id)?
                    .first()
                    .is_some_and(|touch| {
                        db.touch_has_current_exact_approval(&touch.id)
                            .unwrap_or(false)
                    });
                let record = CandidateRecord {
                    company_id: lead.id.clone(),
                    sequence_id: sequence.id.clone(),
                    person_id: sequence.person_id.clone(),
                    selector_a_candidate_id: selection
                        .as_ref()
                        .map(|item| item.selector_a_candidate_id.clone())
                        .unwrap_or_default(),
                    selector_a_pass: selection
                        .as_ref()
                        .is_some_and(|item| item.selector_a_passed),
                    selector_a_score: selection.as_ref().map_or(0, |item| item.selector_a_score),
                    selector_a_reason: selection
                        .as_ref()
                        .map(|item| item.selector_a_reasons.join(" | "))
                        .unwrap_or_default(),
                    selector_b_candidate_id: selection
                        .as_ref()
                        .map(|item| item.selector_b_candidate_id.clone())
                        .unwrap_or_default(),
                    selector_b_pass: selection
                        .as_ref()
                        .is_some_and(|item| item.selector_b_passed),
                    selector_b_score: selection.as_ref().map_or(0, |item| item.selector_b_score),
                    selector_b_reason: selection
                        .as_ref()
                        .map(|item| item.selector_b_reasons.join(" | "))
                        .unwrap_or_default(),
                    agreed_candidate_id: selection
                        .as_ref()
                        .map(|item| item.agreed_candidate_id.clone())
                        .unwrap_or_default(),
                    selected_content_hash: selection
                        .as_ref()
                        .map(|item| item.selected_content_hash.clone())
                        .unwrap_or_default(),
                    exact_copy_human_approved: approved,
                    evaluation_partition: partition.clone(),
                    candidate: candidate.clone(),
                };
                human_review.push(HumanReviewTemplate {
                    company_id: lead.id.clone(),
                    sequence_id: sequence.id.clone(),
                    candidate_id: candidate.candidate_id.clone(),
                    subject: candidate.subject.clone(),
                    body: candidate.body.clone(),
                    send_unchanged: None,
                    factually_supported: None,
                    correct_recipient: None,
                    concrete_recipient_value: None,
                    sounds_human: None,
                    rejection_reason: None,
                    preferred_candidate: None,
                    reviewer: "Andrew".into(),
                    reviewed_at: None,
                });
                candidate_records.push(record);
            }
            if selection.is_none() {
                selector_failures.push(format!(
                    "{} / {}: no persisted two-selector agreement",
                    lead.name, sequence.id
                ));
            }
        }
    }

    if candidate_records.is_empty() {
        message_failures.push(
            "No candidates were generated because no sampled opportunity passed the governed pre-writing gates."
                .into(),
        );
    }
    let similarity = similarity_report(&candidate_records);
    let repetition_failures = similarity
        .iter()
        .filter(|finding| finding.violation)
        .map(|finding| {
            format!(
                "{} vs {}: cross-account similarity {:.3}",
                finding.left_candidate_id, finding.right_candidate_id, finding.token_jaccard
            )
        })
        .collect::<Vec<_>>();

    let requested = requested_sample(&brand);
    if selected.len() < requested {
        company_failures.push(format!(
            "sample shortfall: {} selected of {} requested without lowering the standard",
            selected.len(),
            requested
        ));
    }
    let controlled_allowlist_configured = std::env::var("SPRUCE_SEND_ALLOWLIST")
        .ok()
        .is_some_and(|value| !value.trim().is_empty());
    let approvals = candidate_records
        .iter()
        .filter(|record| record.exact_copy_human_approved)
        .map(|record| record.sequence_id.as_str())
        .collect::<HashSet<_>>()
        .len();
    let selected_messages = candidate_records
        .iter()
        .filter(|record| {
            record.candidate.available
                && !record.agreed_candidate_id.is_empty()
                && record.candidate.candidate_id == record.agreed_candidate_id
        })
        .count();
    let agreements = candidate_records
        .iter()
        .filter(|record| !record.agreed_candidate_id.is_empty())
        .map(|record| record.sequence_id.as_str())
        .collect::<HashSet<_>>()
        .len();
    let required_final = if brand == "wapahki" {
        10
    } else if brand == "gnk" {
        30
    } else {
        40
    };
    let pass = selected.len() == requested
        && qualification.iter().all(|row| row.source_urls.len() > 0)
        && selected_messages >= required_final
        && approvals >= if brand == "gnk" { 24 } else { required_final }
        && repetition_failures.is_empty();
    let mut decisions = HashMap::new();
    for row in &qualification {
        *decisions.entry(row.decision.clone()).or_insert(0) += 1;
    }
    let manifest = AcceptanceManifest {
        brand: brand.clone(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        read_only: true,
        external_send_attempted: false,
        sample_requested: requested,
        companies_selected: selected.len(),
        qualification_decisions: decisions,
        contacts_exported: contacts.len(),
        contacts_with_current_employment_support: contacts
            .iter()
            .filter(|record| record.current_employment_supported)
            .count(),
        missing_buyer_map_roles: contacts
            .iter()
            .filter(|record| record.contact_failure.is_some())
            .count(),
        candidate_mode_decisions: candidate_records.len(),
        available_candidate_messages: candidate_records
            .iter()
            .filter(|record| record.candidate.available)
            .count(),
        unavailable_candidate_modes: candidate_records
            .iter()
            .filter(|record| !record.candidate.available)
            .count(),
        selector_agreements: agreements,
        exact_copy_human_approvals: approvals,
        selected_messages,
        similarity_violations: repetition_failures.len(),
        controlled_allowlist_configured,
        controlled_delivery_recorded: false,
        pass,
        release_requirements: release_requirements(&brand),
    };
    let failures = FailureReport {
        company_qualification_failures: company_failures,
        contact_selection_failures: contact_failures,
        evidence_failures,
        message_quality_failures: message_failures,
        selector_failures,
        template_repetition_failures: repetition_failures,
        missing_data_and_operational_blockers: vec![
            "Andrew human labels are not present; this exporter never fabricates them.".into(),
            "Controlled-inbox delivery was not attempted by this read-only exporter.".into(),
            if controlled_allowlist_configured {
                "A transport allowlist is configured, but delivery still requires a separately approved exact-copy message and explicit live invocation.".into()
            } else {
                "SPRUCE_SEND_ALLOWLIST is not configured, so controlled delivery is unavailable."
                    .into()
            },
        ],
    };

    write_json(&directory.join("qualification_table.json"), &qualification)?;
    write_json(&directory.join("verified_contact_records.json"), &contacts)?;
    write_json(
        &directory.join("message_candidates.json"),
        &candidate_records,
    )?;
    write_json(&directory.join("human_review_template.json"), &human_review)?;
    write_json(&directory.join("batch_similarity_report.json"), &similarity)?;
    write_json(&directory.join("failure_report.json"), &failures)?;
    write_json(&directory.join("manifest.json"), &manifest)?;

    Ok(ExportSummary {
        directory,
        companies: selected.len(),
        contacts: contacts.len(),
        candidates: candidate_records.len(),
        passed: manifest.pass,
    })
}

fn select_sample(
    brand: &str,
    leads: Vec<Lead>,
    assessments: &HashMap<String, AccountPlayAssessment>,
    lead_segments: &HashMap<String, Vec<String>>,
) -> Vec<SelectedLead> {
    let buckets: Vec<(String, Vec<&str>, usize)> = match brand {
        "wapahki" => vec![
            (
                "food_production_and_copacking".into(),
                vec!["canada_food_case_palletizing"],
                10,
            ),
            (
                "warehousing_and_logistics".into(),
                vec!["canada_warehouse_case_handling"],
                10,
            ),
            (
                "packaging_plastics_and_light_manufacturing".into(),
                vec!["canada_manufacturing_machine_tending"],
                10,
            ),
        ],
        "gnk" => vec![
            (
                "construction_delay_evidence".into(),
                vec!["canada_construction_delay_evidence"],
                20,
            ),
            (
                "3pl_exception_decisions".into(),
                vec!["canada_3pl_exception_decisions"],
                20,
            ),
            (
                "specialty_claims_admin".into(),
                vec!["canada_specialty_claims_admin"],
                20,
            ),
        ],
        _ => vec![
            (
                "generator_and_backup_power".into(),
                vec!["canada_backup_power_dispatch"],
                5,
            ),
            (
                "ev_charging".into(),
                vec!["canada_ev_charging_operations"],
                5,
            ),
            ("cold_storage".into(), vec!["canada_outage_cold_storage"], 5),
            ("telecom".into(), vec!["canada_telecom_site_continuity"], 5),
            (
                "healthcare_labs_senior_care".into(),
                vec![
                    "canada_outage_labs_healthcare",
                    "canada_outage_senior_residences",
                ],
                5,
            ),
            (
                "insurance_emergency_and_risk".into(),
                vec![
                    "canada_outage_insurance_cat",
                    "canada_outage_municipal_emergency",
                ],
                5,
            ),
        ],
    };
    let mut used = HashSet::new();
    let mut selected = Vec::new();
    for (label, keys, quota) in buckets {
        let mut candidates = leads
            .iter()
            .filter(|lead| brand != "wapahki" || lead.hq.to_ascii_lowercase().contains("ontario"))
            .filter(|lead| brand != "outagehub" || lead.hq.to_ascii_lowercase().contains("canada"))
            .filter(|lead| {
                lead_segments.get(&lead.id).is_some_and(|segments| {
                    segments
                        .iter()
                        .any(|segment| keys.iter().any(|key| segment == key))
                })
            })
            .filter(|lead| !used.contains(&lead.id))
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            let left_assessment = assessments.get(&left.id);
            let right_assessment = assessments.get(&right.id);
            assessment_rank(left_assessment)
                .cmp(&assessment_rank(right_assessment))
                .then_with(|| {
                    right_assessment
                        .map_or(0, |item| item.fit_score)
                        .cmp(&left_assessment.map_or(0, |item| item.fit_score))
                })
                .then_with(|| {
                    lead_segments
                        .get(&left.id)
                        .map_or(usize::MAX, Vec::len)
                        .cmp(&lead_segments.get(&right.id).map_or(usize::MAX, Vec::len))
                })
                .then_with(|| left.name.cmp(&right.name))
        });
        for lead in candidates.into_iter().take(quota) {
            used.insert(lead.id.clone());
            selected.push(SelectedLead {
                assessment: assessments.get(&lead.id).cloned(),
                lead,
                segment: label.clone(),
            });
        }
    }
    selected
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

fn canonical_segment_key(key: &str) -> String {
    match key {
        "ontario_food_case_palletizing" => "canada_food_case_palletizing",
        "ontario_warehouse_case_handling" => "canada_warehouse_case_handling",
        "ontario_manufacturing_machine_tending" => "canada_manufacturing_machine_tending",
        _ => key,
    }
    .into()
}

fn requested_sample(brand: &str) -> usize {
    match brand {
        "wapahki" | "outagehub" => 30,
        "gnk" => 60,
        _ => 0,
    }
}

fn assessment_rank(assessment: Option<&AccountPlayAssessment>) -> i64 {
    match assessment.map(|item| item.status.as_str()) {
        Some("qualified") => 0,
        Some("research_needed") => 1,
        Some("research_required") => 2,
        Some("rejected") => 3,
        _ => 4,
    }
}

fn difficulty(
    assessment: Option<&AccountPlayAssessment>,
    opportunity: Option<&SalesOpportunity>,
    has_facility: bool,
) -> String {
    if assessment.is_some_and(|item| item.status == "rejected") {
        return "reject".into();
    }
    if assessment.is_some_and(|item| item.status == "qualified")
        && opportunity.is_some_and(|item| {
            !item.task_claim_id.is_empty() && !item.economic_claim_id.is_empty()
        })
        && has_facility
    {
        "easy".into()
    } else if assessment
        .is_some_and(|item| item.status == "qualified" || item.status == "research_needed")
    {
        "medium".into()
    } else {
        "hard".into()
    }
}

fn best_opportunity<'a>(
    opportunities: &'a [SalesOpportunity],
    lead_id: &str,
) -> Option<&'a SalesOpportunity> {
    opportunities
        .iter()
        .filter(|opportunity| opportunity.lead_id == lead_id)
        .max_by_key(|opportunity| (opportunity.fit_score, opportunity.updated_at.as_str()))
}

fn rank_people(
    mut people: Vec<Person>,
    lead: &Lead,
    stakeholders: &[OpportunityStakeholder],
) -> Vec<Person> {
    people.sort_by(|left, right| {
        let left_stakeholder = stakeholders.iter().find(|item| item.person_id == left.id);
        let right_stakeholder = stakeholders.iter().find(|item| item.person_id == right.id);
        let score = |person: &Person, stakeholder: Option<&OpportunityStakeholder>| {
            (if employment_supported(person, lead) {
                100
            } else {
                0
            }) + stakeholder.map_or(0, |item| if item.role_fit == "direct" { 50 } else { 20 })
                + if person.primary { 10 } else { 0 }
                + if person.email_status == "verified" {
                    5
                } else {
                    0
                }
        };
        score(right, right_stakeholder)
            .cmp(&score(left, left_stakeholder))
            .then_with(|| left.name.cmp(&right.name))
    });
    people
}

fn employment_supported(person: &Person, lead: &Lead) -> bool {
    match person.employer_verification.as_str() {
        "apollo" => {
            !person.apollo_org_id.is_empty()
                && !lead.apollo_org_id.is_empty()
                && person.apollo_org_id == lead.apollo_org_id
        }
        "official" => !person.employer_source_url.trim().is_empty(),
        _ => false,
    }
}

fn stable_hash(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn similarity_report(records: &[CandidateRecord]) -> Vec<SimilarityFinding> {
    let available = records
        .iter()
        .filter(|record| record.candidate.available)
        .collect::<Vec<_>>();
    let mut findings = Vec::new();
    for (index, left) in available.iter().enumerate() {
        for right in available.iter().skip(index + 1) {
            if left.company_id == right.company_id {
                continue;
            }
            let left_tokens = tokens(&left.candidate.body);
            let right_tokens = tokens(&right.candidate.body);
            let union = left_tokens.union(&right_tokens).count();
            let intersection = left_tokens.intersection(&right_tokens).count();
            let jaccard = if union == 0 {
                0.0
            } else {
                intersection as f64 / union as f64
            };
            let left_opening = meaningful_lines(&left.candidate.body)
                .first()
                .cloned()
                .unwrap_or_default();
            let right_opening = meaningful_lines(&right.candidate.body)
                .first()
                .cloned()
                .unwrap_or_default();
            let left_question = question(&left.candidate.body);
            let right_question = question(&right.candidate.body);
            let repeated_opening = !left_opening.is_empty() && left_opening == right_opening;
            let repeated_question = !left_question.is_empty() && left_question == right_question;
            findings.push(SimilarityFinding {
                left_candidate_id: left.candidate.candidate_id.clone(),
                right_candidate_id: right.candidate.candidate_id.clone(),
                left_company_id: left.company_id.clone(),
                right_company_id: right.company_id.clone(),
                token_jaccard: jaccard,
                repeated_opening,
                repeated_question,
                violation: jaccard >= 0.72 || repeated_opening || repeated_question,
            });
        }
    }
    findings
}

fn tokens(text: &str) -> HashSet<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|token| token.len() >= 4)
        .map(str::to_ascii_lowercase)
        .collect()
}

fn meaningful_lines(body: &str) -> Vec<String> {
    body.lines()
        .map(str::trim)
        .filter(|line| {
            !line.is_empty() && !line.to_ascii_lowercase().starts_with("hi ") && *line != "Andrew"
        })
        .map(str::to_ascii_lowercase)
        .collect()
}

fn question(body: &str) -> String {
    body.lines()
        .find(|line| line.contains('?'))
        .unwrap_or_default()
        .trim_end_matches('?')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn release_requirements(brand: &str) -> Vec<String> {
    match brand {
        "wapahki" => vec![
            "30 Ontario facilities across at least three sectors".into(),
            "10 complete Task Briefs and 10 distinct exact-copy human approvals".into(),
            "at least 8 of 10 first-attempt sendable candidates".into(),
            "zero unsupported claims and zero batch-template violations".into(),
            "one controlled allowlisted inbox delivery; no prospect delivery".into(),
        ],
        "gnk" => vec![
            "60 companies across at least three segments".into(),
            "30 final messages, at least 24 approved unchanged by a human".into(),
            "zero wrong recipients, unsupported claims, research requests, or superficial assets"
                .into(),
            "complete tests/CI and one controlled allowlisted inbox delivery".into(),
        ],
        _ => vec![
            "30 companies, five in each of six use cases".into(),
            "40 recipient cases and 120 alternative candidates".into(),
            "36 of 40 cases with an Andrew send-unchanged candidate".into(),
            "selector agreement in 36 of 40 and at least 9 of 10 sealed holdout".into(),
            "zero fabricated facts and no live email during the test".into(),
        ],
    }
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{employment_supported, select_sample};
    use crate::db::{Lead, Person};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn wapahki_acceptance_sample_preserves_three_exact_sector_quotas() {
        let mut leads = Vec::new();
        let mut segments = HashMap::new();
        for (prefix, segment) in [
            ("food", "canada_food_case_palletizing"),
            ("warehouse", "canada_warehouse_case_handling"),
            ("manufacturing", "canada_manufacturing_machine_tending"),
        ] {
            for index in 0..10 {
                let id = format!("{prefix}-{index}");
                leads.push(Lead {
                    id: id.clone(),
                    name: id.clone(),
                    hq: "Ontario, Canada".into(),
                    ..Default::default()
                });
                segments.insert(id, vec![segment.into()]);
            }
        }
        let selected = select_sample("wapahki", leads, &HashMap::new(), &segments);
        assert_eq!(selected.len(), 30);
        assert_eq!(
            selected
                .iter()
                .map(|item| item.lead.id.as_str())
                .collect::<HashSet<_>>()
                .len(),
            30
        );
        for label in [
            "food_production_and_copacking",
            "warehousing_and_logistics",
            "packaging_plastics_and_light_manufacturing",
        ] {
            assert_eq!(
                selected.iter().filter(|item| item.segment == label).count(),
                10
            );
        }
    }

    #[test]
    fn acceptance_does_not_fill_a_missing_sector_from_an_unrelated_company() {
        let lead = Lead {
            id: "food-1".into(),
            name: "Food One".into(),
            hq: "Ontario, Canada".into(),
            ..Default::default()
        };
        let selected = select_sample(
            "wapahki",
            vec![lead],
            &HashMap::new(),
            &HashMap::from([("food-1".into(), vec!["canada_food_case_palletizing".into()])]),
        );
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].segment, "food_production_and_copacking");
    }

    #[test]
    fn title_and_verified_email_do_not_prove_current_employment() {
        let lead = Lead {
            apollo_org_id: "org-1".into(),
            ..Default::default()
        };
        let person = Person {
            title: "Plant Manager".into(),
            email: "plant@example.com".into(),
            email_status: "verified".into(),
            employer_verification: "unverified".into(),
            ..Default::default()
        };
        assert!(!employment_supported(&person, &lead));
    }
}
