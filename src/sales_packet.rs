//! Read-only assembly of the sales decision package expected by the brand
//! manuals. Exporting never researches, drafts, approves, schedules, or sends.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::db::{
    AcquisitionContext, EvidenceClaim, Facility, MessageCandidateAudit, OpportunityStakeholder,
    Person, ProofAsset, SalesBrief, SalesOpportunity, SalesStageTransition, Sequence, SharedDb,
    Touch,
};

#[derive(Debug, Serialize)]
struct AccountDecisionPacket<'a> {
    brand: &'a str,
    sales_opportunity_id: &'a str,
    account_decision: &'a str,
    why_company_could_buy: &'a str,
    commercial_potential: &'a str,
    opportunity_difficulty: &'a str,
    confidence_level: &'a str,
    missing_information: &'a [String],
    sendable_now: bool,
    gate_issues: Vec<String>,
}

#[derive(Debug, Serialize)]
struct FacilityEvidencePacket<'a> {
    facility: Option<&'a Facility>,
    facts: Vec<&'a EvidenceClaim>,
    inferences: &'a [String],
    unknowns: &'a [String],
}

#[derive(Debug, Serialize)]
struct TaskHypothesisPacket<'a> {
    task_or_decision: &'a str,
    physical_or_operational_workflow: &'a str,
    expected_variation: &'a str,
    consequence_hypothesis: &'a str,
    technical_kill_conditions: &'a [String],
    confidence_level: &'a str,
    fact_claim_ids: &'a [String],
}

#[derive(Debug, Serialize)]
struct BuyerMapEntry {
    stakeholder: OpportunityStakeholder,
    person: Option<Person>,
    first_contact: bool,
    objective: String,
}

#[derive(Debug, Serialize)]
struct HumanApprovalEntry {
    sequence: Sequence,
    touches: Vec<ApprovalTouch>,
}

#[derive(Debug, Serialize)]
struct ApprovalTouch {
    touch: Touch,
    exact_current_approval: bool,
}

#[derive(Debug, Serialize)]
struct QualificationPlan<'a> {
    cold_stage: &'a str,
    discovery_starts_only_after_human_reply: bool,
    brand_stage_order: Vec<&'static str>,
    transitions: &'a [SalesStageTransition],
    discovery: Option<crate::db::DiscoveryQualification>,
}

pub struct ExportSummary {
    pub packets: usize,
    pub paths: Vec<PathBuf>,
}

pub fn import_sales_brief(db: &SharedDb, path: &Path) -> Result<String> {
    let raw = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let brief: SalesBrief = serde_json::from_slice(&raw)
        .with_context(|| format!("parsing sales brief JSON {}", path.display()))?;
    db.upsert_sales_brief(&brief)
}

pub fn import_acquisition_context(db: &SharedDb, path: &Path) -> Result<Vec<String>> {
    let raw = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_slice(&raw)
        .with_context(|| format!("parsing acquisition context JSON {}", path.display()))?;
    let contexts: Vec<AcquisitionContext> = if value.is_array() {
        serde_json::from_value(value)
            .with_context(|| format!("parsing acquisition context array {}", path.display()))?
    } else {
        vec![serde_json::from_value(value)
            .with_context(|| format!("parsing acquisition context {}", path.display()))?]
    };
    if contexts.is_empty() {
        anyhow::bail!("acquisition context import is empty");
    }
    contexts
        .iter()
        .map(|context| db.upsert_acquisition_context(context))
        .collect()
}

pub fn import_conditional_followup(db: &SharedDb, path: &Path) -> Result<String> {
    let raw = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let plan: crate::db::ConditionalFollowup = serde_json::from_slice(&raw)
        .with_context(|| format!("parsing conditional follow-up JSON {}", path.display()))?;
    db.upsert_conditional_followup(&plan)
}

pub fn import_discovery_qualification(db: &SharedDb, path: &Path) -> Result<String> {
    let raw = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let qualification: crate::db::DiscoveryQualification = serde_json::from_slice(&raw)
        .with_context(|| format!("parsing discovery JSON {}", path.display()))?;
    db.upsert_discovery_qualification(&qualification)
}

pub fn import_sales_application(db: &SharedDb, path: &Path) -> Result<String> {
    let raw = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let brief: crate::db::SalesApplicationBrief = serde_json::from_slice(&raw)
        .with_context(|| format!("parsing sales application JSON {}", path.display()))?;
    db.upsert_sales_application_brief(&brief)
}

/// Build a bounded artifact only from the operator's draft brief and its
/// active claim ids. It intentionally produces structured outside-in work,
/// never customer results, ROI, private workflows, or invented integrations.
pub fn prepare_proof_asset(
    db: &SharedDb,
    sales_opportunity_id: &str,
    person_id: &str,
    asset_type: &str,
    output_root: &Path,
) -> Result<ProofAsset> {
    let allowed = [
        "coverage_report",
        "historical_outage_replay",
        "sample_api_payload",
        "integration_sketch",
        "triage_checklist",
        "wapahki_task_brief",
        "task_map",
        "workflow_risk_map",
        "failure_surface_teardown",
    ];
    if !allowed.contains(&asset_type) {
        anyhow::bail!("unsupported proof asset type {asset_type}");
    }
    let brief = db
        .get_sales_brief(sales_opportunity_id, person_id)?
        .ok_or_else(|| anyhow::anyhow!("no draft sales brief for this opportunity and person"))?;
    let claims = db.list_evidence_claims(Some(sales_opportunity_id), Some(&brief.brand))?;
    let selected_claims = claims
        .iter()
        .filter(|claim| {
            brief.fact_claim_ids.contains(&claim.id)
                && matches!(claim.status.as_str(), "observed" | "verified")
        })
        .collect::<Vec<_>>();
    if selected_claims.is_empty() {
        anyhow::bail!("proof asset has no active cited evidence");
    }
    let brand_allows = if brief.brand.eq_ignore_ascii_case("outagehub") {
        matches!(
            asset_type,
            "coverage_report"
                | "historical_outage_replay"
                | "sample_api_payload"
                | "integration_sketch"
                | "triage_checklist"
        )
    } else if brief.brand.eq_ignore_ascii_case("wapahki") {
        matches!(asset_type, "wapahki_task_brief" | "task_map")
    } else if brief.brand.eq_ignore_ascii_case("gnk") {
        matches!(asset_type, "workflow_risk_map" | "failure_surface_teardown")
    } else {
        false
    };
    if !brand_allows {
        anyhow::bail!("asset type {asset_type} is not allowed for {}", brief.brand);
    }
    if asset_type == "historical_outage_replay"
        && !selected_claims
            .iter()
            .any(|claim| claim.claim_type == "account.historical_location_outage_match")
    {
        anyhow::bail!("historical replay requires a completed location-outage match claim");
    }
    let content = serde_json::json!({
        "asset_type": asset_type,
        "brand": brief.brand,
        "sales_opportunity_id": sales_opportunity_id,
        "selected_play": brief.selected_play,
        "workflow_or_task": brief.operational_workflow,
        "trigger": brief.trigger,
        "current_workaround": {
            "text": brief.current_workaround,
            "status": brief.current_workaround_status,
        },
        "consequence_hypothesis": brief.consequence_hypothesis,
        "decision_improved": brief.decision_improved,
        "facts": selected_claims.iter().map(|claim| serde_json::json!({
            "claim_id": claim.id,
            "claim_type": claim.claim_type,
            "text": claim.claim_text,
            "source_url": claim.source_url,
            "source_excerpt": claim.source_excerpt,
            "observed_at": claim.observed_at,
        })).collect::<Vec<_>>(),
        "inferences": brief.inferences,
        "unknowns": brief.uncertainties,
        "kill_conditions": brief.technical_kill_conditions,
        "required_customer_input": brief.required_customer_input,
        "evidence_boundary": "Outside-in public evidence only. This artifact does not establish a private workflow, loss, ROI, causation, performance, or customer result.",
    });
    let packet_dir = output_root
        .join(&brief.brand)
        .join(sales_opportunity_id)
        .join("proof_assets");
    fs::create_dir_all(&packet_dir)?;
    let rendered_path = packet_dir.join(format!("{asset_type}.json"));
    fs::write(&rendered_path, serde_json::to_vec_pretty(&content)?)?;
    let existing = db
        .list_proof_assets(sales_opportunity_id)?
        .into_iter()
        .find(|asset| asset.asset_type == asset_type);
    let status = if asset_type == "historical_outage_replay" {
        "completed"
    } else {
        "prepared"
    };
    let asset = ProofAsset {
        id: existing.map(|asset| asset.id).unwrap_or_default(),
        brand: brief.brand.clone(),
        sales_opportunity_id: sales_opportunity_id.into(),
        asset_type: asset_type.into(),
        title: if brief.recommended_proof_asset.trim().is_empty() {
            format!("{} {}", brief.selected_play, asset_type.replace('_', " "))
        } else {
            brief.recommended_proof_asset.clone()
        },
        evidence_claim_ids: selected_claims
            .iter()
            .map(|claim| claim.id.clone())
            .collect(),
        input_ids: vec![brief.id.clone()],
        content_json: serde_json::to_string_pretty(&content)?,
        rendered_path: rendered_path.display().to_string(),
        status: status.into(),
        completed_at: if status == "completed" {
            chrono::Utc::now().to_rfc3339()
        } else {
            String::new()
        },
        ..Default::default()
    };
    let id = db.upsert_proof_asset(&asset)?;
    Ok(ProofAsset { id, ..asset })
}

pub fn export(
    db: &SharedDb,
    brand: &str,
    opportunity_id: Option<&str>,
    output_root: &Path,
) -> Result<ExportSummary> {
    let mut opportunities = db
        .list_sales_opportunities(Some(brand), None)?
        .into_iter()
        .filter(|opportunity| opportunity_id.is_none_or(|id| opportunity.id == id))
        .collect::<Vec<_>>();
    if opportunity_id.is_none() {
        let covered_leads = opportunities
            .iter()
            .map(|opportunity| opportunity.lead_id.clone())
            .collect::<HashSet<String>>();
        for lead in db
            .list_leads(Some(brand))?
            .into_iter()
            .filter(|lead| !covered_leads.contains(&lead.id))
        {
            opportunities.push(SalesOpportunity {
                id: format!("research-hold-{}", lead.id),
                brand: lead.brand,
                lead_id: lead.id,
                title: lead.name,
                task_or_decision:
                    "No verified application or facility-task opportunity exists yet.".into(),
                evidence_status: "research_required".into(),
                evidence_tier: "research_required".into(),
                status: "research".into(),
                evidence_gaps: vec![
                    "No governed sales opportunity has been supported from current evidence."
                        .into(),
                    "No person may receive outreach from this research-only packet.".into(),
                ],
                ..Default::default()
            });
        }
    }
    if opportunities.is_empty() {
        anyhow::bail!("no matching {brand} sales opportunity");
    }
    let mut paths = Vec::new();
    for opportunity in opportunities {
        let path = output_root.join(brand).join(&opportunity.id);
        fs::create_dir_all(&path)
            .with_context(|| format!("creating sales packet directory {}", path.display()))?;
        export_one(db, &opportunity, &path)?;
        paths.push(path);
    }
    Ok(ExportSummary {
        packets: paths.len(),
        paths,
    })
}

fn export_one(db: &SharedDb, opportunity: &SalesOpportunity, path: &Path) -> Result<()> {
    let claims = db.list_evidence_claims(Some(&opportunity.id), Some(&opportunity.brand))?;
    let stakeholders =
        db.list_opportunity_stakeholders(Some(&opportunity.id), Some(&opportunity.brand))?;
    let brief = stakeholders
        .iter()
        .find(|stakeholder| stakeholder.active_thread)
        .and_then(|stakeholder| {
            db.get_sales_brief(&opportunity.id, &stakeholder.person_id)
                .ok()
                .flatten()
        })
        .or_else(|| {
            stakeholders.iter().find_map(|stakeholder| {
                db.get_sales_brief(&opportunity.id, &stakeholder.person_id)
                    .ok()
                    .flatten()
            })
        });
    let fallback = SalesBrief {
        brand: opportunity.brand.clone(),
        sales_opportunity_id: opportunity.id.clone(),
        account_decision: "nurture_until_trigger".into(),
        why_company_could_buy: opportunity.task_or_decision.clone(),
        commercial_potential: "Unassessed; founder evidence required.".into(),
        opportunity_difficulty: "hard".into(),
        confidence_level: "low".into(),
        operational_workflow: opportunity.task_or_decision.clone(),
        expected_variation: "unknown".into(),
        consequence_hypothesis: opportunity.consequence.clone(),
        missing_information: opportunity.evidence_gaps.clone(),
        uncertainties: opportunity.evidence_gaps.clone(),
        technical_kill_conditions: vec!["No approved prospect sales brief exists.".into()],
        status: "draft".into(),
        ..Default::default()
    };
    let brief = brief.as_ref().unwrap_or(&fallback);
    let mut gate_issues = brief.gate_issues();
    let acquisition_context = if brief.person_id.trim().is_empty() {
        None
    } else {
        db.get_acquisition_context(&opportunity.id, &brief.person_id)?
    };
    match &acquisition_context {
        Some(context) => gate_issues.extend(context.gate_issues()),
        None => gate_issues
            .push("no approved acquisition source/channel context for email or LinkedIn".into()),
    }
    if brief.account_decision != "send" {
        gate_issues.push(format!("account decision is {}", brief.account_decision));
    }
    gate_issues.sort();
    gate_issues.dedup();

    write_json(
        &path.join("account_decision.json"),
        &AccountDecisionPacket {
            brand: &opportunity.brand,
            sales_opportunity_id: &opportunity.id,
            account_decision: &brief.account_decision,
            why_company_could_buy: &brief.why_company_could_buy,
            commercial_potential: &brief.commercial_potential,
            opportunity_difficulty: &brief.opportunity_difficulty,
            confidence_level: &brief.confidence_level,
            missing_information: &brief.missing_information,
            sendable_now: gate_issues.is_empty(),
            gate_issues,
        },
    )?;

    let facilities = db.list_facilities(Some(&opportunity.market_account_id))?;
    let facility = facilities
        .iter()
        .find(|facility| facility.id == opportunity.facility_id);
    write_json(
        &path.join("facility_evidence.json"),
        &FacilityEvidencePacket {
            facility,
            facts: claims
                .iter()
                .filter(|claim| brief.fact_claim_ids.contains(&claim.id))
                .collect(),
            inferences: &brief.inferences,
            unknowns: &brief.uncertainties,
        },
    )?;
    write_json(
        &path.join("task_hypothesis.json"),
        &TaskHypothesisPacket {
            task_or_decision: &opportunity.task_or_decision,
            physical_or_operational_workflow: &brief.operational_workflow,
            expected_variation: &brief.expected_variation,
            consequence_hypothesis: &brief.consequence_hypothesis,
            technical_kill_conditions: &brief.technical_kill_conditions,
            confidence_level: &brief.confidence_level,
            fact_claim_ids: &brief.fact_claim_ids,
        },
    )?;

    let buyer_map = if stakeholders.is_empty() {
        db.list_people(Some(&opportunity.brand), None)?
            .into_iter()
            .filter(|person| person.lead_id == opportunity.lead_id)
            .map(|person| {
                let role = if person.vantage.trim().is_empty() {
                    "unverified_contact".to_string()
                } else {
                    person.vantage.clone()
                };
                let stakeholder = OpportunityStakeholder {
                    person_id: person.id.clone(),
                    role: role.clone(),
                    relationship_to_task:
                        "Contact exists, but no application-specific responsibility evidence is attached."
                            .into(),
                    role_fit: "adjacent".into(),
                    active_thread: false,
                    status: "research_only".into(),
                    ..Default::default()
                };
                BuyerMapEntry {
                    objective: buyer_objective(&role, "adjacent"),
                    first_contact: false,
                    stakeholder,
                    person: Some(person),
                }
            })
            .collect::<Vec<_>>()
    } else {
        stakeholders
            .into_iter()
            .map(|stakeholder| {
                let person = db.get_person(&stakeholder.person_id).ok().flatten();
                let objective = buyer_objective(&stakeholder.role, &stakeholder.role_fit);
                BuyerMapEntry {
                    first_contact: stakeholder.active_thread,
                    stakeholder,
                    person,
                    objective,
                }
            })
            .collect::<Vec<_>>()
    };
    write_json(&path.join("buyer_map.json"), &buyer_map)?;
    write_json(&path.join("acquisition_context.json"), &acquisition_context)?;

    write_json(
        &path.join("selected_play.json"),
        &serde_json::json!({
            "play": brief.selected_play,
            "trigger": brief.trigger,
            "workflow": brief.operational_workflow,
            "current_workaround": brief.current_workaround,
            "workaround_status": brief.current_workaround_status,
            "value": brief.recipient_offer,
            "decision_improved": brief.decision_improved,
            "first_proof": brief.recommended_proof_asset,
        }),
    )?;

    let sequences = db
        .list_sequences(Some(&opportunity.brand))?
        .into_iter()
        .filter(|sequence| sequence.sales_opportunity_id == opportunity.id)
        .collect::<Vec<_>>();
    let mut candidates = Vec::<MessageCandidateAudit>::new();
    let mut approvals = Vec::new();
    for sequence in sequences {
        candidates.extend(db.list_message_candidate_audit(&sequence.id)?);
        let touches = db
            .list_touches_for_sequence(&sequence.id)?
            .into_iter()
            .map(|touch| {
                let exact_current_approval = db
                    .touch_has_current_exact_approval(&touch.id)
                    .unwrap_or(false);
                ApprovalTouch {
                    touch,
                    exact_current_approval,
                }
            })
            .collect();
        approvals.push(HumanApprovalEntry { sequence, touches });
    }
    write_json(&path.join("message_candidates.json"), &candidates)?;
    write_json(&path.join("human_approval.json"), &approvals)?;

    let followups = db.list_conditional_followups(&opportunity.id)?;
    write_json(&path.join("followup_plan.json"), &followups)?;
    let proof_assets: Vec<ProofAsset> = db.list_proof_assets(&opportunity.id)?;
    write_json(&path.join("proof_assets.json"), &proof_assets)?;

    let transitions = db.list_sales_stage_transitions(&opportunity.id)?;
    let discovery = db.get_discovery_qualification(&opportunity.id)?;
    write_json(
        &path.join("qualification_plan.json"),
        &QualificationPlan {
            cold_stage: opportunity.evidence_tier.as_str(),
            discovery_starts_only_after_human_reply: true,
            brand_stage_order: brand_stages(&opportunity.brand),
            transitions: &transitions,
            discovery,
        },
    )?;
    write_json(&path.join("sales_stage.json"), &transitions)?;
    let application = db.get_sales_application_brief(&opportunity.id)?;
    write_json(&path.join("application_brief.json"), &application)?;

    let brief_name = if opportunity.brand.eq_ignore_ascii_case("wapahki") {
        "wapahki_task_brief.md"
    } else if opportunity.brand.eq_ignore_ascii_case("outagehub") {
        "outagehub_account_brief.md"
    } else {
        "gnk_sendable_brief.md"
    };
    fs::write(path.join(brief_name), render_brief(opportunity, brief))?;
    Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))
}

fn buyer_objective(role: &str, role_fit: &str) -> String {
    let role = role.to_ascii_lowercase();
    if role.contains("operator") || role.contains("user") {
        "Confirm or correct the observable workflow; never ask for budget authority.".into()
    } else if role.contains("technical") || role.contains("engineering") {
        "Evaluate feasibility, integration, constraints, and proof requirements after the workflow matters.".into()
    } else if role.contains("economic") || role.contains("executive sponsor") {
        "Assess impact, priority, decision path, and commercial evidence after problem confirmation.".into()
    } else if role.contains("procurement") || role.contains("legal") {
        "Engage only after a buyer-sponsored evaluation requires commercial or legal review.".into()
    } else if role.contains("router") || role_fit == "adjacent" {
        "Identify the person connected to the exact workflow; do not describe this person as its owner.".into()
    } else {
        "Test the role's documented relationship to the selected workflow with the smallest useful ask.".into()
    }
}

fn brand_stages(brand: &str) -> Vec<&'static str> {
    if brand.eq_ignore_ascii_case("wapahki") {
        vec![
            "target_facility_verified",
            "task_hypothesis_supported",
            "correct_people_mapped",
            "task_confirmed_through_outreach",
            "technical_and_commercial_feasibility_screened",
            "paid_proof_scoped",
            "deployment_proposed",
        ]
    } else if brand.eq_ignore_ascii_case("gnk") {
        vec![
            "verified_company",
            "credible_opportunity",
            "correct_person",
            "useful_offer",
            "sendable_t1",
            "discovery_and_qualification",
            "paid_sprint",
        ]
    } else {
        vec![
            "account_sales_brief",
            "buying_committee_map",
            "selected_use_case_play",
            "workflow_validated",
            "proof_asset_evaluated",
            "buying_decision_path",
            "close_or_nurture",
        ]
    }
}

fn render_brief(opportunity: &SalesOpportunity, brief: &SalesBrief) -> String {
    format!(
        "# {}\n\n- Account decision: {}\n- Selected play: {}\n- Difficulty: {}\n- Confidence: {}\n\n## Why this company could buy\n\n{}\n\n## Workflow or task\n\n{}\n\n## Trigger / why now\n\n{}\n\n## Consequence hypothesis\n\n{}\n\n## Concrete contribution\n\n{}\n\n## Recipient value\n\n{}\n\n## Required input\n\n{}\n\n## Decision improved\n\n{}\n\n## Facts\n\n{}\n\n## Inferences\n\n{}\n\n## Unknowns\n\n{}\n\n## Kill conditions\n\n{}\n\n## Consent record\n\nBasis: {}  \nEvidence: {}  \nCaptured: {}  \nRole relevance: {}  \nCompliance review: {}\n",
        opportunity.title,
        brief.account_decision,
        brief.selected_play,
        brief.opportunity_difficulty,
        brief.confidence_level,
        brief.why_company_could_buy,
        brief.operational_workflow,
        brief.trigger,
        brief.consequence_hypothesis,
        brief.concrete_contribution,
        brief.recipient_offer,
        brief.required_customer_input,
        brief.decision_improved,
        bullet_list(&brief.fact_claim_ids),
        bullet_list(&brief.inferences),
        bullet_list(&brief.uncertainties),
        bullet_list(&brief.technical_kill_conditions),
        brief.consent_basis,
        brief.consent_evidence_url,
        brief.consent_evidence_captured_at,
        brief.role_relevance,
        brief.compliance_review_status,
    )
}

fn bullet_list(values: &[String]) -> String {
    if values.is_empty() {
        "- None recorded".into()
    } else {
        values
            .iter()
            .map(|value| format!("- {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}
