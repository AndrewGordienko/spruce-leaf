//! GTM engineering: the durable layer between research and buyer-facing action.
//!
//! Agents may interpret evidence and compose a response, but SQLite owns the
//! lineage, play version, experiment assignment, and proof state. This prevents
//! the writing agent (or the sales council) from grading its own commercial
//! hypothesis and turns replies and proof results into attributable learning.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::db::{
    AcquisitionContext, CustomerDevelopmentRecord, EvidenceClaim, GtmExperiment, GtmPlay, Lead,
    MarketSegment, OpportunityStakeholder, Person, ProofAsset, SalesBrief, SalesOpportunity,
    SharedDb, SignalDefinition, SignalObservation,
};
use crate::playbook::Playbook;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CustomerDevelopmentStage {
    pub key: &'static str,
    pub label: &'static str,
    pub proof: &'static str,
    pub next_commitment: &'static str,
}

/// The customer-development ladder. Email activity is intentionally not a
/// stage: each rung represents stronger evidence or a commitment of time,
/// reputation, or money.
pub const CUSTOMER_DEVELOPMENT_STAGES: &[CustomerDevelopmentStage] = &[
    CustomerDevelopmentStage {
        key: "hypothesis",
        label: "Hypothesis",
        proof: "A named account and one falsifiable task hypothesis.",
        next_commitment: "Earn a correction, referral, or first-hand reply about the task.",
    },
    CustomerDevelopmentStage {
        key: "engaged",
        label: "Engaged",
        proof: "A human responded; politeness alone does not validate the problem.",
        next_commitment: "Confirm a real past or current problem and workflow.",
    },
    CustomerDevelopmentStage {
        key: "problem_confirmed",
        label: "Problem confirmed",
        proof: "The prospect described the problem in their own words.",
        next_commitment: "Map one bounded task, why it stays manual, and its variation.",
    },
    CustomerDevelopmentStage {
        key: "task_mapped",
        label: "Task mapped",
        proof: "Object, motion, manual cause, variation, and exceptions are concrete.",
        next_commitment: "Ask for a sketch, video, SKU/changeover set, or site observation.",
    },
    CustomerDevelopmentStage {
        key: "evidence_shared",
        label: "Evidence shared",
        proof: "The prospect spent time or reputation sharing operational evidence.",
        next_commitment: "Agree a bounded feasibility evaluation and its pass/fail criteria.",
    },
    CustomerDevelopmentStage {
        key: "evaluation_agreed",
        label: "Evaluation agreed",
        proof: "Scope, success measure, stop condition, owner, and timing are explicit.",
        next_commitment: "Secure recurring access and sponsorship as a design partner.",
    },
    CustomerDevelopmentStage {
        key: "design_partner",
        label: "Design partner",
        proof: "A sponsor commits access, feedback, and internal coordination.",
        next_commitment: "Document site, quantity, commercial range, timeline, and conditions.",
    },
    CustomerDevelopmentStage {
        key: "loi_candidate",
        label: "LOI candidate",
        proof: "The material deployment and commercial terms are understood.",
        next_commitment: "Ask for conditional written intent tied to pilot criteria.",
    },
    CustomerDevelopmentStage {
        key: "conditional_loi",
        label: "Conditional LOI",
        proof: "Written intent names task/site, quantity, economics, criteria, and timeline.",
        next_commitment: "Convert intent into a paid, bounded pilot.",
    },
    CustomerDevelopmentStage {
        key: "paid_pilot",
        label: "Paid pilot",
        proof: "Money changed hands for a scoped test with pass/fail criteria.",
        next_commitment: "Pass the pilot and contract the first deployment.",
    },
    CustomerDevelopmentStage {
        key: "deployment",
        label: "Deployment",
        proof: "A production deployment is contracted or live.",
        next_commitment: "Document results before expanding scope or repeating the play.",
    },
];

pub fn normalize_commitment_kind(raw: &str) -> &'static str {
    match raw.trim().to_lowercase().replace([' ', '-'], "_").as_str() {
        "evaluation_agreed" => "evaluation_agreed",
        "design_partner" => "design_partner",
        "loi_candidate" => "loi_candidate",
        "conditional_loi" | "loi_signed" => "conditional_loi",
        "paid_pilot" => "paid_pilot",
        "deployment" => "deployment",
        _ => "none",
    }
}

pub fn customer_development_stage(record: &CustomerDevelopmentRecord) -> &'static str {
    let mut stage = 0usize;
    if !record.engaged_at.trim().is_empty() {
        stage = 1;
    }
    if !record.problem.trim().is_empty() {
        stage = stage.max(2);
    }
    if !record.task_scope.trim().is_empty()
        && !record.why_manual.trim().is_empty()
        && (!record.variations.is_empty() || !record.exceptions.is_empty())
    {
        stage = stage.max(3);
    }
    if !record.evidence.is_empty() {
        stage = stage.max(4);
    }
    let commitment = normalize_commitment_kind(&record.commitment_kind);
    let desired = CUSTOMER_DEVELOPMENT_STAGES
        .iter()
        .position(|candidate| candidate.key == commitment)
        .unwrap_or(0);
    if desired >= 5
        && stage >= 4
        && !record.success_criteria.trim().is_empty()
        && !record.stop_condition.trim().is_empty()
        && !record.timeline.trim().is_empty()
        && !record.commitment_detail.trim().is_empty()
    {
        stage = 5;
    }
    if desired >= 6 && stage >= 5 && !record.stakeholders.is_empty() {
        stage = 6;
    }
    if desired >= 7
        && stage >= 6
        && !record.site.trim().is_empty()
        && !record.quantity.trim().is_empty()
        && !record.commercial_case.trim().is_empty()
        && !record.loi_conditions.trim().is_empty()
    {
        stage = 7;
    }
    if desired >= 8 && stage >= 7 {
        stage = 8;
    }
    if desired >= 9 && stage >= 8 {
        stage = 9;
    }
    if desired >= 10 && stage >= 9 {
        stage = 10;
    }
    CUSTOMER_DEVELOPMENT_STAGES[stage].key
}

pub fn customer_development_stage_info(
    record: &CustomerDevelopmentRecord,
) -> &'static CustomerDevelopmentStage {
    let key = customer_development_stage(record);
    CUSTOMER_DEVELOPMENT_STAGES
        .iter()
        .find(|stage| stage.key == key)
        .unwrap_or(&CUSTOMER_DEVELOPMENT_STAGES[0])
}

pub fn customer_development_missing(record: &CustomerDevelopmentRecord) -> Vec<&'static str> {
    match customer_development_stage(record) {
        "hypothesis" => vec!["human reply, correction, or referral"],
        "engaged" => missing_fields(&[(record.problem.as_str(), "prospect-confirmed problem")]),
        "problem_confirmed" => {
            let mut missing = missing_fields(&[
                (record.task_scope.as_str(), "bounded task / motion"),
                (record.current_workflow.as_str(), "current workflow"),
                (record.why_manual.as_str(), "why it remains manual"),
            ]);
            if record.variations.is_empty() && record.exceptions.is_empty() {
                missing.push("variation or exception examples");
            }
            missing
        }
        "task_mapped" => vec!["customer-shared sketch, video, SKU data, or site observation"],
        "evidence_shared" => {
            let mut missing = missing_fields(&[
                (record.success_criteria.as_str(), "success criteria"),
                (record.stop_condition.as_str(), "stop condition"),
                (record.timeline.as_str(), "evaluation timing"),
            ]);
            missing.push("explicit evaluation agreement");
            missing
        }
        "evaluation_agreed" => {
            let mut missing = Vec::new();
            if record.stakeholders.is_empty() {
                missing.push("named sponsor and stakeholder map");
            }
            missing.push("explicit recurring access / feedback commitment");
            missing
        }
        "design_partner" => {
            let mut missing = missing_fields(&[
                (record.site.as_str(), "deployment site"),
                (record.quantity.as_str(), "provisional cell quantity"),
                (
                    record.commercial_case.as_str(),
                    "price range or payback case",
                ),
                (record.timeline.as_str(), "decision / deployment timeline"),
                (record.loi_conditions.as_str(), "conditions to purchase"),
            ]);
            missing.push("explicit agreement to material LOI terms");
            missing
        }
        "loi_candidate" => vec!["conditional written intent"],
        "conditional_loi" => vec!["paid pilot scope and payment"],
        "paid_pilot" => vec!["passed criteria and deployment contract"],
        _ => vec!["measured result and expansion decision"],
    }
}

fn missing_fields(fields: &[(&str, &'static str)]) -> Vec<&'static str> {
    fields
        .iter()
        .filter_map(|(value, label)| value.trim().is_empty().then_some(*label))
        .collect()
}

pub fn sourcing_play_block(play: Option<&GtmPlay>) -> String {
    let Some(play) = play else {
        return "No active versioned GTM play. Treat this run as hypothesis generation and require unusually strong source evidence before qualification.".into();
    };
    format!(
        "ACTIVE VERSIONED GTM PLAY (selection policy, not marketing copy)\n\
         Name: {} v{}\nTarget ICP: {}\nHypothesis: {}\nRequired signal catalog keys: {} (minimum {} distinct catalog keys, not repeated examples)\n\
         A single completed historical location/event result satisfies the historical-match key when its evidence boundary is met; never demand four locations because the play requires four different keys. Missing evidence is a research gap, not a hard disqualifier.\n\
         Action policy: {}\nProof we can actually deliver: {}\nSuccess metric: {}\nKill condition: {}\n\
         Use this play to choose, qualify, and RANK accounts. Reject superficial industry/technology matches and enforce the declared minimum rather than silently making every catalog key mandatory. Exposure evidence may prioritize research, but it never proves an outage-time decision and never authorizes copy by itself. For OutageHub only, a source-backed distributed exposure plus a segment-matched operating recipient may authorize one manually reviewed discovery email whose decision remains an explicit question; multi-touch or action copy still requires source-backed decision evidence and the current mechanism. Always require a credible path to the bounded proof.",
        play.name,
        play.version,
        play.target_icp,
        play.hypothesis,
        play.required_signal_keys.join(", "),
        play.minimum_signal_matches,
        play.action_policy,
        play.proof_description,
        play.success_metric,
        play.kill_condition,
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SignalCandidate {
    /// Must be one of the brand's canonical signal-definition keys.
    pub definition_key: String,
    /// The observed evidence, not an inferred consequence or proposed product.
    pub evidence: String,
    #[serde(default)]
    pub source_url: String,
    /// 0.0–1.0. Confidence is about the observation, not account fit.
    #[serde(default)]
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GtmActionContext {
    pub state: String,
    pub play: Option<GtmPlay>,
    pub opportunity: Option<SalesOpportunity>,
    pub evidence_claims: Vec<EvidenceClaim>,
    pub stakeholders: Vec<OpportunityStakeholder>,
    pub observations: Vec<SignalObservation>,
    pub matched_signal_keys: Vec<String>,
    pub experiment: Option<GtmExperiment>,
    pub experiment_assignment_id: String,
    pub experiment_arm: String,
    /// A prospect reply has passed the explicit-confirmation evidence grade,
    /// unlocking OutageHub's coordinated four-touch motion.
    #[serde(default)]
    pub engaged: bool,
    /// Deterministic commercial lane computed from persisted evidence.
    #[serde(default)]
    pub priority: Option<crate::priority::CommercialPriority>,
    /// Founder/compliance-owned prospect record. The writer receives its
    /// validated projection, never the old free-form account hypothesis.
    #[serde(default)]
    pub sales_brief: Option<SalesBrief>,
    /// Exact prepared/completed artifact bound by the Sales Brief. Writers see
    /// only its validated recipient-facing result, never a status flag alone.
    #[serde(default)]
    pub proof_asset: Option<ProofAsset>,
    /// Reviewed source/channel history. The writer may reference only this
    /// context when the person is not a cold-research acquisition.
    #[serde(default)]
    pub acquisition_context: Option<AcquisitionContext>,
}

impl GtmActionContext {
    pub fn action_ready(&self) -> bool {
        self.state == "action_ready"
    }

    fn is_outagehub(&self) -> bool {
        self.play
            .as_ref()
            .is_some_and(|play| play.brand.eq_ignore_ascii_case("outagehub"))
    }

    fn is_gnk(&self) -> bool {
        self.play
            .as_ref()
            .is_some_and(|play| play.brand.eq_ignore_ascii_case("gnk"))
    }

    fn is_wapahki(&self) -> bool {
        self.play
            .as_ref()
            .is_some_and(|play| play.brand.eq_ignore_ascii_case("wapahki"))
    }

    fn is_supervised_pilot_brand(&self) -> bool {
        self.play.as_ref().is_some_and(|play| {
            matches!(
                play.brand.to_ascii_lowercase().as_str(),
                "gnk" | "wapahki" | "outagehub"
            )
        })
    }

    /// The maximum cold-touch count the current evidence authorizes.
    /// Cold generation remains T1-only. These ceilings govern separately
    /// approved, evidence-dependent next touches after engagement: OutageHub 5,
    /// GnK 4, Wapahki 3. A seven-stage account motion is not seven emails.
    pub fn max_authorized_touches(&self) -> usize {
        match self.state.as_str() {
            "action_ready" if self.is_outagehub() && self.engaged => 5,
            "action_ready" if self.is_outagehub() => 2,
            "action_ready" if self.is_gnk() && self.engaged => 4,
            "action_ready" if self.is_wapahki() && self.engaged => 3,
            "action_ready" if self.is_gnk() || self.is_wapahki() => 1,
            "action_ready" => 7,
            "discovery_ready" => 1,
            _ => 0,
        }
    }

    /// Action-ready accounts may use the cadence their evidence level
    /// authorizes. A discovery-ready account may use one honest,
    /// hypothesis-led first touch; it cannot silently become a follow-up
    /// campaign before the problem is confirmed. Research-required inventory
    /// never reaches copy.
    pub fn sequence_ready_for(&self, touches: usize) -> bool {
        touches >= 1 && touches <= self.max_authorized_touches()
    }

    /// A human may approve an authorized reviewed sequence. Touch count can
    /// shorten a cadence but never widen it past the evidence level.
    pub fn delivery_ready_for(&self, touches: usize) -> bool {
        touches >= 1 && touches <= self.max_authorized_touches()
    }

    /// Automatic scheduling remains action-ready only. Touch count may shorten a
    /// cadence but never weaken evidence or commercial authorization.
    pub fn automatic_delivery_ready_for(&self, touches: usize) -> bool {
        self.action_ready()
            && !self.is_supervised_pilot_brand()
            && touches >= 1
            && touches <= self.max_authorized_touches()
    }

    /// Private context for the planner/writer. This is decision infrastructure,
    /// never prose to paste into a buyer-facing message.
    pub fn prompt_block(&self) -> String {
        let Some(play) = &self.play else {
            return "GTM ACTION STATE: no active play. Draft only a diagnostic question; do not pitch a proof or integration.".into();
        };
        let evidence = self
            .evidence_claims
            .iter()
            .map(|claim| {
                format!(
                    "- [{}; confidence {:.2}; {}; {}] {}",
                    claim.claim_type,
                    claim.confidence,
                    claim.status,
                    claim.source_url,
                    claim.source_excerpt
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let experiment = self.experiment.as_ref().map_or_else(
            || "none — write the current play, not an invented variant".to_string(),
            |experiment| {
                let arm_description = if self.experiment_arm == "variant" {
                    &experiment.variant_description
                } else {
                    &experiment.control_description
                };
                format!(
                    "{} / {}. Assigned arm: {}. Only variable allowed to differ: {}. Arm instruction: {}. Constants: {}",
                    experiment.name,
                    experiment.experiment_type,
                    self.experiment_arm,
                    experiment.variable,
                    arm_description,
                    experiment.constants.join("; ")
                )
            },
        );
        format!(
            "GTM ACTION STATE: {state}\n\
             PLAY: {name} v{version} ({lifecycle})\n\
             HYPOTHESIS: {hypothesis}\n\
             ACTION POLICY: {policy}\n\
             FORWARD-DEPLOYED PROOF (only after the problem is confirmed): {proof}\n\
             SUCCESS METRIC: {metric}\n\
             KILL CONDITION: {kill}\n\
             OBSERVED SIGNALS WITH LINEAGE:\n{evidence}\n\
             EXPERIMENT: {experiment}\n\
             Do not expose internal labels, confidence scores, experiment arms, or strategy language to the buyer. Do not treat a hypothesis as a fact.",
            state = self.state,
            name = play.name,
            version = play.version,
            lifecycle = play.lifecycle,
            hypothesis = play.hypothesis,
            policy = play.action_policy,
            proof = play.proof_description,
            metric = play.success_metric,
            kill = play.kill_condition,
            evidence = if evidence.is_empty() {
                "- none".to_string()
            } else {
                evidence
            },
        )
    }

    /// Small, buyer-safe decision brief for the copywriter.
    ///
    /// The full GTM block contains experiment assignments, proof concepts,
    /// success metrics, and kill conditions. Those are useful to a strategist
    /// but repeatedly pulled cold copy toward internal-memo language and
    /// premature pilots. The writer needs only the evidence strength, the
    /// observations it may rely on, and the smallest outcome the evidence can
    /// support.
    pub fn copy_prompt_block(&self) -> String {
        let mut seen = std::collections::HashSet::new();
        let evidence = self
            .evidence_claims
            .iter()
            .filter(|claim| {
                matches!(claim.status.as_str(), "observed" | "verified")
                    && seen.insert(claim.claim_type.clone())
            })
            .take(6)
            .map(|claim| {
                format!(
                    "- [claim_id={}; opportunity_id={}; task_key={}; type={}; {}] {}",
                    claim.id,
                    claim.sales_opportunity_id,
                    claim.task_key,
                    claim.claim_type,
                    claim.source_url,
                    claim.source_excerpt
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let opportunity = self.opportunity.as_ref().map_or_else(
            || "none — hold for research".to_string(),
            |opportunity| {
                format!(
                    "sales_opportunity_id={} | task_key={} | {} | task/decision: {} | facility_id={} | task_claim_id={} | economic_claim_id={} | tier: {}",
                    opportunity.id,
                    opportunity.task_key,
                    opportunity.title,
                    opportunity.task_or_decision,
                    if opportunity.facility_id.is_empty() {
                        "not linked"
                    } else {
                        &opportunity.facility_id
                    },
                    if opportunity.task_claim_id.is_empty() {
                        "not linked"
                    } else {
                        &opportunity.task_claim_id
                    },
                    if opportunity.economic_claim_id.is_empty() {
                        "not linked"
                    } else {
                        &opportunity.economic_claim_id
                    },
                    opportunity.evidence_tier,
                )
            },
        );
        let action = match self.state.as_str() {
            "action_ready" => "The account has enough sourced evidence for one narrow commercial note. Use only a supplied observation as the company-specific signal. Lead with a role-relevant implication and a credible point of view. A cold outcome may be a short working conversation, interest, correction, or referral; it is not yet a pilot or proof. Never invent collateral or claim an asset exists unless verified seller context explicitly supplies it.",
            "discovery_ready" if self.is_outagehub() => "This opportunity is discovery-ready, not action-ready: research supports a distributed, outage-sensitive operating footprint and this recipient is verified adjacent to that segment, but the account's outage-time decision is unproved. Write one complete first email only. Use a specific location, event, workflow, or public responsibility rather than a category summary; ask one explicit operating question without implying the workflow exists; and offer a historical comparison or sample response OutageHub can actually produce. A direct email answer is the sole next step, but Andrew's desire for research is never the reason to answer. Do not ask for a call, describe a private workflow as fact, or schedule follow-ups before a reply.",
            "discovery_ready" => "This opportunity is discovery-ready, not action-ready: research supports one concrete operating task, decision, or mechanism and this recipient is close to it, but one economic or workflow term remains unproved. Write one complete, useful first email only. State sourced account details as facts, present the exact missing term as one honest question, explain the seller's relevant contribution, and make a direct email answer the sole next step. Do not ask for a call before the missing term is confirmed. Never use a universal diagnostic template or schedule follow-ups before a reply.",
            _ => "The account does not yet have enough sourced evidence for a multi-touch sequence. Hold it for research or use one manual routing note; do not manufacture discovery questions or explain a proof, integration, pilot, or product.",
        };
        format!(
            "COPY DECISION STATE: {state}\nOPPORTUNITY: {opportunity}\nPERMITTED ACTION: {action}\nATOMIC SOURCE CLAIMS:\n{evidence}\nSTRUCTURAL OUTPUT CONTRACT: return the exact sales_opportunity_id and task_key above plus only the exact claim_id values actually used. A mismatch is an automatic rejection before model review.\nSOURCE-FAITHFUL ATTRIBUTION: each bullet is an independent claim from its own URL. Never strengthen one source with a noun or detail found only in another source. If a job posting says `finished products` and a separate company page names the product, keep those as separate sentences or retain the posting's exact generic wording; never say the posting named that product. Treat everything else as a question, not account reality.",
            state = self.state,
            opportunity = opportunity,
            evidence = if evidence.is_empty() {
                "- none".to_string()
            } else {
                evidence
            },
        )
    }
}

/// Whether a prospect has established the operating problem in their own
/// words. This is deliberately stronger than public fit evidence and is the
/// boundary before economic or technical contacts enter the motion.
pub fn problem_confirmed_for_lead(db: &SharedDb, brand: &str, lead_id: &str) -> Result<bool> {
    if db
        .customer_development_for_lead(brand, lead_id)?
        .is_some_and(|record| {
            CUSTOMER_DEVELOPMENT_STAGES
                .iter()
                .position(|stage| stage.key == customer_development_stage(&record))
                .unwrap_or(0)
                >= 2
        })
    {
        return Ok(true);
    }
    Ok(db
        .list_active_signal_observations(Some(brand), Some(lead_id), None)?
        .iter()
        .any(|observation| {
            observation.definition_key == "conversation.problem_confirmed"
                && matches!(observation.status.as_str(), "observed" | "verified")
        }))
}

/// Stronger boundary for widening an OutageHub sequence after a reply. The
/// reply agent must have persisted an exact, body-verified supporting quote and
/// an explicit confirmation grade. Legacy positive replies and manually
/// advanced customer-development rows remain useful discovery evidence, but
/// cannot silently unlock four touches.
pub fn graded_problem_confirmed_for_lead(
    db: &crate::db::Db,
    brand: &str,
    lead_id: &str,
) -> Result<bool> {
    Ok(db
        .list_active_signal_observations(Some(brand), Some(lead_id), None)?
        .iter()
        .filter(|observation| {
            observation.definition_key == "conversation.problem_confirmed"
                && observation.source_name == "prospect_reply"
                && observation.status == "verified"
        })
        .any(|observation| {
            serde_json::from_str::<serde_json::Value>(&observation.value_json)
                .ok()
                .is_some_and(|value| {
                    value.get("grade").and_then(|grade| grade.as_str()) == Some("explicit")
                        && value
                            .get("supporting_quote")
                            .and_then(|quote| quote.as_str())
                            .is_some_and(|quote| !quote.trim().is_empty())
                })
        }))
}

/// Follow-up authorization is scoped to the exact opportunity, person, and
/// conversation thread. One employee's reply must never widen another cold
/// thread at the same account.
pub fn graded_problem_confirmed_for_thread(
    db: &crate::db::Db,
    brand: &str,
    sales_opportunity_id: &str,
    person_id: &str,
) -> Result<bool> {
    for observation in db
        .list_active_signal_observations(Some(brand), None, Some(person_id))?
        .into_iter()
        .filter(|observation| {
            observation.definition_key == "conversation.problem_confirmed"
                && observation.source_name == "prospect_reply"
                && observation.status == "verified"
                && !observation.conversation_id.trim().is_empty()
        })
    {
        let Some(conversation) = db.get_conversation(&observation.conversation_id)? else {
            continue;
        };
        if conversation.person_id != person_id {
            continue;
        }
        let in_scope = db
            .sequence_gtm_attribution(&conversation.sequence_id)?
            .is_some_and(|sequence| {
                sequence.sales_opportunity_id == sales_opportunity_id
                    && sequence.person_id == person_id
                    && sequence.brand.eq_ignore_ascii_case(brand)
            });
        if !in_scope {
            continue;
        }
        if serde_json::from_str::<serde_json::Value>(&observation.value_json)
            .ok()
            .is_some_and(|value| {
                value.get("grade").and_then(|grade| grade.as_str()) == Some("explicit")
                    && value
                        .get("supporting_quote")
                        .and_then(|quote| quote.as_str())
                        .is_some_and(|quote| !quote.trim().is_empty())
            })
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A contact-specific cold-outreach boundary. Routers are held for explicit
/// one-off routing work instead of entering a commercial cadence. Economic,
/// enterprise, and technical contacts wait until a human confirms the problem.
pub fn recipient_sequence_block_reason(
    db: &SharedDb,
    brand: &str,
    lead_id: &str,
    person: &Person,
    touches: usize,
) -> Result<Option<String>> {
    if let Some(reason) = db.person_employment_block_reason(person)? {
        return Ok(Some(reason));
    }
    if brand.eq_ignore_ascii_case("outagehub")
        && db.has_open_sponsorship_outreach_for_lead(brand, lead_id)?
    {
        return Ok(Some(
            "account has an unresolved OutageHub sponsorship thread; resolve that relationship before opening a separate API-customer ask"
                .into(),
        ));
    }
    if brand.eq_ignore_ascii_case("outagehub")
        && !outagehub_workflow_contact(db, lead_id, &person.title, &person.vantage, touches == 1)?
    {
        return Ok(Some(
            "OutageHub requires a role mapped to the evidenced outage-time decision, or for one discovery email only, to the source-backed outage-sensitive segment; title seniority or a generic operations label is insufficient"
                .into(),
        ));
    }
    if crate::response_design::is_route_only_contact(&person.title, &person.vantage) {
        return Ok(Some(
            "recipient is route-only; hold for one bounded manual routing request".into(),
        ));
    }
    if let Some(reason) = opportunity_role_block_reason(db, brand, lead_id, person, touches)? {
        return Ok(Some(reason));
    }
    if crate::response_design::requires_confirmed_problem(&person.title, &person.vantage)
        && !problem_confirmed_for_lead(db, brand, lead_id)?
    {
        return Ok(Some(
            "economic/technical recipient requires a prospect-confirmed problem first".into(),
        ));
    }
    Ok(None)
}

fn opportunity_role_block_reason(
    db: &SharedDb,
    brand: &str,
    lead_id: &str,
    person: &Person,
    touches: usize,
) -> Result<Option<String>> {
    let Some(play) = db.current_gtm_play(brand)? else {
        return Ok(Some("no current GTM play is available".into()));
    };
    let Some(opportunity) = db.best_sales_opportunity(brand, lead_id, &play.id)? else {
        return Ok(Some(
            "no current sales opportunity is mapped for this account".into(),
        ));
    };
    let stakeholders = db.list_opportunity_stakeholders(Some(&opportunity.id), Some(brand))?;
    let Some(stakeholder) = stakeholders
        .iter()
        .find(|stakeholder| stakeholder.person_id == person.id && stakeholder.status != "held")
    else {
        return Ok(Some(
            "recipient is not mapped to the selected sales opportunity".into(),
        ));
    };
    if stakeholder.role_fit != "direct" || stakeholder.evidence_claim_ids.is_empty() {
        // Wapahki and OutageHub have bounded discovery lanes: exactly one
        // evidence-seeking email. Cold provider data can rarely prove
        // person-to-facility employment (it requires the contact's own
        // location record to name the exact facility city), and demanding
        // that proof for every first touch silences the brand entirely while
        // drafting still burns review budget. The lane stays narrow: a real
        // facility, a URL-backed task claim for the same task, and a
        // recipient whose title puts them near the physical workflow.
        // Corporate finance, revenue, and strategy titles wait for evidence.
        if brand.eq_ignore_ascii_case("wapahki") && touches == 1 {
            let claims = db.list_evidence_claims(Some(&opportunity.id), Some(brand))?;
            return Ok(wapahki_discovery_touch_block_reason(
                &opportunity,
                stakeholder,
                person,
                &claims,
            ));
        }
        if brand.eq_ignore_ascii_case("outagehub") && touches == 1 {
            let claims = db.list_evidence_claims(Some(&opportunity.id), Some(brand))?;
            return Ok(outagehub_discovery_touch_block_reason(
                &opportunity,
                stakeholder,
                person,
                &claims,
            ));
        }
        return Ok(Some(
            "recipient role fit is inferred or adjacent; opportunity-specific evidence is required before outreach"
                .into(),
        ));
    }
    let claims = db.list_evidence_claims(Some(&opportunity.id), Some(brand))?;
    if brand.eq_ignore_ascii_case("wapahki") {
        let exact_lineage = !opportunity.facility_id.trim().is_empty()
            && !opportunity.task_claim_id.trim().is_empty()
            && !opportunity.economic_claim_id.trim().is_empty()
            && !stakeholder.contact_facility_evidence_id.trim().is_empty()
            && [
                (
                    &opportunity.task_claim_id,
                    "account.bounded_repetitive_task",
                ),
                (
                    &opportunity.economic_claim_id,
                    "account.manual_task_economic_pressure",
                ),
                (
                    &stakeholder.contact_facility_evidence_id,
                    "contact.facility_employment",
                ),
            ]
            .iter()
            .all(|(id, claim_type)| {
                claims.iter().any(|claim| {
                    claim.id == id.as_str()
                        && claim.claim_type == *claim_type
                        && claim.facility_id == opportunity.facility_id
                        && claim.task_key == opportunity.task_key
                        && matches!(claim.status.as_str(), "observed" | "verified")
                })
            });
        if !exact_lineage {
            return Ok(Some(
                "Wapahki requires an exact facility, task claim, economic claim, and person-to-facility evidence before outreach"
                    .into(),
            ));
        }
    }
    let valid = stakeholder.evidence_claim_ids.iter().all(|claim_id| {
        claims.iter().any(|claim| {
            claim.id == *claim_id
                && claim.task_key == opportunity.task_key
                && matches!(claim.status.as_str(), "observed" | "verified")
        })
    });
    if !valid {
        return Ok(Some(
            "recipient direct-role evidence is stale or belongs to a different task".into(),
        ));
    }
    Ok(None)
}

/// Eligibility for Wapahki's single evidence-seeking discovery email when the
/// contact has no proven facility relationship. The message may attribute the
/// task only to its public source (a posting or page), never to the recipient's
/// own station, and multi-touch cadences remain reserved for proven lineage.
fn wapahki_discovery_touch_block_reason(
    opportunity: &SalesOpportunity,
    stakeholder: &crate::db::OpportunityStakeholder,
    person: &Person,
    claims: &[crate::db::EvidenceClaim],
) -> Option<String> {
    if !matches!(
        opportunity.evidence_status.as_str(),
        "action_ready" | "discovery_ready"
    ) {
        return Some(format!(
            "opportunity evidence state '{}' does not authorize outreach",
            opportunity.evidence_status
        ));
    }
    if opportunity.facility_id.trim().is_empty() {
        return Some(
            "no operating facility is linked to the physical task evidence; the account stays in research"
                .into(),
        );
    }
    if !matches!(stakeholder.role_fit.as_str(), "direct" | "adjacent") {
        return Some(
            "recipient is mapped as a router or unrelated; a discovery email needs someone near the workflow"
                .into(),
        );
    }
    if !crate::db::wapahki_physical_workflow_title(&person.title) {
        return Some(format!(
            "title '{}' is not close to the physical workflow; corporate finance, revenue, and strategy contacts wait for a confirmed task",
            person.title.trim()
        ));
    }
    let task_claim_supported = claims.iter().any(|claim| {
        claim.claim_type == "account.bounded_repetitive_task"
            && claim.task_key == opportunity.task_key
            && matches!(claim.status.as_str(), "observed" | "verified")
            && crate::db::credible_source_url(&claim.source_url)
    });
    if !task_claim_supported {
        return Some(
            "no URL-backed task claim supports this opportunity; a discovery email would have no premise"
                .into(),
        );
    }
    None
}

/// Eligibility for OutageHub's single premise-testing email. The public
/// evidence may prove exposure and footprint, but not the account's private
/// outage workflow. The recipient must be close to the segment and the email
/// must leave that workflow as a question. Multi-touch remains unavailable.
fn outagehub_discovery_touch_block_reason(
    opportunity: &SalesOpportunity,
    stakeholder: &crate::db::OpportunityStakeholder,
    person: &Person,
    claims: &[crate::db::EvidenceClaim],
) -> Option<String> {
    if !matches!(
        opportunity.evidence_status.as_str(),
        "action_ready" | "discovery_ready"
    ) {
        return Some(format!(
            "opportunity evidence state '{}' does not authorize outreach",
            opportunity.evidence_status
        ));
    }
    if !matches!(stakeholder.role_fit.as_str(), "direct" | "adjacent") {
        return Some(
            "recipient is mapped as a router or unrelated; a discovery email needs someone near the outage-sensitive operation"
                .into(),
        );
    }
    let active_claim = |claim_type: &str| {
        claims.iter().find(|claim| {
            claim.claim_type == claim_type
                && claim.task_key == opportunity.task_key
                && matches!(claim.status.as_str(), "observed" | "verified")
                && crate::db::credible_source_url(&claim.source_url)
        })
    };
    let Some(exposure) = active_claim("account.outage_sensitive_exposure") else {
        return Some(
            "no URL-backed outage-sensitive exposure supports this opportunity; the account stays in research"
                .into(),
        );
    };
    if active_claim("account.fit_evidence").is_none()
        || active_claim("account.distributed_locations").is_none()
    {
        return Some(
            "one discovery email requires URL-backed account fit, distributed locations, and outage-sensitive exposure"
                .into(),
        );
    }
    if !crate::qualification::outagehub_role_matches_decision(
        &person.title,
        &person.vantage,
        &exposure.claim_text,
    ) {
        return Some(format!(
            "title '{}' is not mapped to the source-backed outage-sensitive segment",
            person.title.trim()
        ));
    }
    None
}

/// Deterministic closing-difficulty band for one Wapahki account, separate
/// from opportunity value (value stays founder-entered in
/// `CommercialAssessment`; conflating the two is how a pipeline fills with
/// impressive but distant companies).
///
/// 0 = easy: facility-linked URL-backed task, evidenced economic pressure, a
///     workflow-adjacent contact, and a procurement surface small enough for a
///     paid step in roughly 30–60 days.
/// 1 = medium: a real facility-linked task but missing economics or sized so
///     that more stakeholders and validation are likely (two–six months).
/// 2 = hard: weak or missing lineage, no workflow contact, or an
///     enterprise-scale parent whose procurement outlives a pre-seed runway.
pub fn wapahki_commercial_difficulty_band(context: &GtmActionContext, headcount: i64) -> u8 {
    let Some(opportunity) = &context.opportunity else {
        return 2;
    };
    let supported_claim = |claim_type: &str| {
        context.evidence_claims.iter().any(|claim| {
            claim.claim_type == claim_type
                && matches!(claim.status.as_str(), "observed" | "verified")
                && crate::db::credible_source_url(&claim.source_url)
        })
    };
    let facility_linked = !opportunity.facility_id.trim().is_empty();
    let task_supported = supported_claim("account.bounded_repetitive_task");
    let workflow_contact = context
        .stakeholders
        .iter()
        .any(|stakeholder| matches!(stakeholder.role_fit.as_str(), "direct" | "adjacent"));
    let enterprise_procurement = headcount > 2000;
    if !facility_linked || !task_supported || !workflow_contact || enterprise_procurement {
        return 2;
    }
    let economic_pressure = supported_claim("account.manual_task_economic_pressure");
    if economic_pressure && headcount > 0 && headcount <= 600 {
        0
    } else {
        1
    }
}

/// Planning order for one candidate recipient: evidence state stays dominant
/// (an easy-but-unsupported account never outranks a supported one), and for
/// Wapahki the closing-difficulty band breaks ties so near-term-cash accounts
/// are drafted before distant enterprise accounts. Other brands keep their
/// pure state ordering.
pub fn planning_priority(context: &GtmActionContext, brand: &str, headcount: i64) -> u8 {
    let state_rank = match context.state.as_str() {
        "action_ready" => 0u8,
        "discovery_ready" => 1,
        _ => 2,
    };
    if !brand.eq_ignore_ascii_case("wapahki") {
        return state_rank * 3;
    }
    state_rank * 3 + wapahki_commercial_difficulty_band(context, headcount)
}

fn outagehub_workflow_contact(
    db: &SharedDb,
    lead_id: &str,
    title: &str,
    vantage: &str,
    allow_exposure_hypothesis: bool,
) -> Result<bool> {
    let observations = db
        .list_active_signal_observations(Some("outagehub"), Some(lead_id), None)?
        .into_iter()
        .collect::<Vec<_>>();
    let mut decision_evidence = observations
        .iter()
        .filter(|observation| observation.definition_key == "account.outage_sensitive_decision")
        .filter(|observation| {
            crate::qualification::credible_outagehub_signal(
                "account.outage_sensitive_decision",
                &observation.evidence,
            )
        })
        .map(|observation| observation.evidence.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    if decision_evidence.is_empty() && allow_exposure_hypothesis {
        decision_evidence = observations
            .iter()
            .filter(|observation| {
                observation.definition_key == "account.outage_sensitive_exposure"
                    && crate::qualification::credible_outagehub_signal(
                        "account.outage_sensitive_exposure",
                        &observation.evidence,
                    )
            })
            .map(|observation| observation.evidence.as_str())
            .collect::<Vec<_>>()
            .join(" ");
    }
    Ok(crate::qualification::outagehub_role_matches_decision(
        title,
        vantage,
        &decision_evidence,
    ))
}

/// Final cold-email delivery boundary shared by approval and the cadence
/// engine. Re-evaluating immediately before SMTP keeps stale scheduled rows,
/// old play versions, and oversized legacy accounts from bypassing policy.
pub fn delivery_block_reason(
    db: &SharedDb,
    playbook: &Playbook,
    lead: &Lead,
    person: &Person,
) -> Result<Option<String>> {
    delivery_block_reason_inner(db, playbook, lead, person, true, None)
}

/// Eligibility check for a newly reviewed `building` sequence. The candidate
/// has not been promoted yet, so an older active sequence must not prevent its
/// replacement; promotion and every delivery query enforce the current policy.
pub(crate) fn candidate_delivery_block_reason(
    db: &SharedDb,
    playbook: &Playbook,
    lead: &Lead,
    person: &Person,
    touches: usize,
) -> Result<Option<String>> {
    delivery_block_reason_inner(db, playbook, lead, person, false, Some(touches))
}

fn delivery_block_reason_inner(
    db: &SharedDb,
    playbook: &Playbook,
    lead: &Lead,
    person: &Person,
    require_current_active_sequence: bool,
    candidate_touches: Option<usize>,
) -> Result<Option<String>> {
    if playbook
        .max_employees
        .is_some_and(|max| lead.headcount > 0 && lead.headcount > max)
    {
        return Ok(Some(format!(
            "{} employees exceeds the {}-employee account ceiling",
            lead.headcount,
            playbook.max_employees.unwrap_or_default()
        )));
    }
    // A live send must belong to the current policy. During initial
    // finalization the only row is still a `building` checkpoint, so absence of
    // an active sequence is valid here; every scheduling/due-work query also
    // filters by current_copy_policy_version().
    let mut touch_count = candidate_touches.unwrap_or_default();
    if require_current_active_sequence {
        let Some(sequence_id) = db.active_sequence_for_person(&person.id)? else {
            return Ok(Some("no active sequence is available for delivery".into()));
        };
        let sequence = db.sequence_gtm_attribution(&sequence_id)?;
        let current_policy = sequence.as_ref().is_some_and(|sequence| {
            sequence.status == "active"
                && sequence.copy_policy_hash == crate::db::current_copy_policy_hash()
        });
        if !current_policy {
            return Ok(Some(
                "sequence predates the current copy policy and must be regenerated".into(),
            ));
        }
        let current_opportunity_id = context_opportunity_id(db, &playbook.key, &lead.id, person)?;
        if sequence.as_ref().is_none_or(|sequence| {
            sequence.sales_opportunity_id.trim().is_empty()
                || sequence.sales_opportunity_id != current_opportunity_id
        }) {
            return Ok(Some(
                "sequence is not attributed to the current facility/use-case opportunity".into(),
            ));
        }
        if !db.sequence_owns_active_opportunity_thread(&sequence_id)? {
            return Ok(Some(
                "sequence recipient is not the active cold thread for this opportunity".into(),
            ));
        }
        if playbook.key.eq_ignore_ascii_case("wapahki") {
            let context = prepare_action(db, &playbook.key, &lead.id, person)?;
            let lineage_matches = context.opportunity.as_ref().is_some_and(|opportunity| {
                let stakeholder = context
                    .stakeholders
                    .iter()
                    .find(|stakeholder| stakeholder.person_id == person.id);
                sequence.as_ref().is_some_and(|sequence| {
                    sequence.facility_id == opportunity.facility_id
                        && sequence.task_claim_id == opportunity.task_claim_id
                        && sequence.economic_claim_id == opportunity.economic_claim_id
                        && stakeholder.is_some_and(|stakeholder| {
                            sequence.contact_facility_evidence_id
                                == stakeholder.contact_facility_evidence_id
                        })
                })
            });
            if !lineage_matches {
                return Ok(Some(
                    "Wapahki sequence evidence lineage changed; regenerate before delivery".into(),
                ));
            }
        }
        touch_count = db.list_touches_for_sequence(&sequence_id)?.len();
    }
    // The recipient gate runs only after the real touch count is known: a
    // single discovery email and a multi-touch cadence carry different
    // evidence requirements, so evaluating a seven-touch sequence under the
    // one-touch lane would quietly weaken the cadence bar.
    if let Some(reason) =
        recipient_sequence_block_reason(db, &playbook.key, &lead.id, person, touch_count.max(1))?
    {
        return Ok(Some(reason));
    }
    let context = prepare_action(db, &playbook.key, &lead.id, person)?;
    let eligible = if require_current_active_sequence {
        context.delivery_ready_for(touch_count)
    } else {
        context.automatic_delivery_ready_for(touch_count)
    };
    if !eligible {
        return Ok(Some(format!(
            "current GTM state '{}' is not eligible for a {}-touch sequence",
            context.state, touch_count
        )));
    }
    Ok(None)
}

fn context_opportunity_id(
    db: &SharedDb,
    brand: &str,
    lead_id: &str,
    person: &Person,
) -> Result<String> {
    Ok(prepare_action(db, brand, lead_id, person)?
        .opportunity
        .map(|opportunity| opportunity.id)
        .unwrap_or_default())
}

/// Resolve the current versioned play, live evidence, and stable experiment arm
/// before an agent plans copy. An experiment assignment is durable: re-drafting
/// the same person cannot silently move them between control and variant.
pub fn prepare_action(
    db: &SharedDb,
    brand: &str,
    lead_id: &str,
    person: &Person,
) -> Result<GtmActionContext> {
    let play = db.current_gtm_play(brand)?;
    let employment_verified = db.person_employment_block_reason(person)?.is_none();
    let assessment = match &play {
        Some(play) => db.account_play_assessment(lead_id, &play.id)?,
        None => None,
    };
    let mut observations = db.list_active_signal_observations(Some(brand), Some(lead_id), None)?;
    // Preserve old observations for lineage, but do not let them overrule the
    // latest account assessment or restore a signal that the current refresh
    // could not support.
    if let Some(assessment) = &assessment {
        observations.retain(|observation| {
            observation.definition_key == "contact.workflow_vantage"
                || observation.definition_key == "conversation.problem_confirmed"
                || assessment
                    .matched_signal_keys
                    .contains(&observation.definition_key)
        });
    }
    // Validate every canonical observation against its own atomic excerpt.
    // Combining an exposure fact with a separate outage hypothesis previously
    // manufactured a decision that no individual source actually supported.
    observations.retain(|observation| {
        observation.definition_key == "contact.workflow_vantage"
            || credible_action_signal(brand, &observation.definition_key, &observation.evidence)
    });
    let mut matched_signal_keys = observations
        .iter()
        .map(|observation| observation.definition_key.clone())
        .collect::<Vec<_>>();
    // The play asks whether a reachable workflow owner exists at the account,
    // while contact mapping records the stronger, person-specific vantage
    // observation. Treat the selected person's live vantage as satisfying the
    // account requirement. Without this bridge, a company could have five
    // verified process owners and still be permanently held as "no owner".
    let has_reachable_channel = employment_verified
        && (person.email_status.eq_ignore_ascii_case("verified")
            || !person.linkedin_url.trim().is_empty());
    let has_workflow_vantage =
        crate::response_design::is_workflow_discovery_contact(&person.title, &person.vantage);
    if has_reachable_channel
        && has_workflow_vantage
        && observations.iter().any(|observation| {
            observation.definition_key == "contact.workflow_vantage"
                && observation.person_id == person.id
        })
    {
        matched_signal_keys.push("account.reachable_workflow_owner".into());
    }
    matched_signal_keys.sort();
    matched_signal_keys.dedup();

    let has_source_backed_account_fact = db
        .get_lead(lead_id)?
        .is_some_and(|lead| !lead.observed_facts.is_empty());
    let _legacy_state = play.as_ref().map_or("no_play", |play| {
        let matched = play
            .required_signal_keys
            .iter()
            .filter(|key| matched_signal_keys.contains(key))
            .count();
        // OutageHub's earlier play versions let legacy signals make an account
        // look action-ready without ever reassessing it against the current
        // ICP. That kept stale renewable developers and contractors in
        // Pipeline after the play changed. Require a current versioned account
        // assessment for this motion; inventory without one is still reusable,
        // but it must be refreshed before copy can pass the gate.
        // The current GnK and OutageHub experiments require problem proof, not
        // merely company plausibility. Research-needed accounts may be retained
        // for later investigation, but cold copy cannot develop the opportunity
        // for us.
        let assessment_allows_action = assessment
            .as_ref()
            .is_some_and(|assessment| assessment.status == "qualified");
        let assessment_allows_discovery = assessment.as_ref().is_some_and(|assessment| {
            matches!(assessment.status.as_str(), "qualified" | "research_needed")
        });
        if assessment_allows_action
            && matched >= play.minimum_signal_matches.max(1) as usize
            && mandatory_action_signals_present(brand, &matched_signal_keys)
        {
            "action_ready"
        } else if assessment_allows_discovery
            && has_source_backed_account_fact
            && has_reachable_channel
            && has_workflow_vantage
            && mandatory_discovery_signals_present(brand, &matched_signal_keys)
        {
            "discovery_ready"
        } else {
            "research_required"
        }
    });

    let opportunity = match &play {
        Some(play) => db.best_sales_opportunity(brand, lead_id, &play.id)?,
        None => None,
    };
    let evidence_claims = match &opportunity {
        Some(opportunity) => db.list_evidence_claims(Some(&opportunity.id), Some(brand))?,
        None => Vec::new(),
    };
    let stakeholders = match &opportunity {
        Some(opportunity) => {
            db.list_opportunity_stakeholders(Some(&opportunity.id), Some(brand))?
        }
        None => Vec::new(),
    };
    let mut claim_keys = evidence_claims
        .iter()
        .filter(|claim| {
            matches!(claim.status.as_str(), "observed" | "verified")
                && !claim.source_url.trim().is_empty()
                && !claim.source_excerpt.trim().is_empty()
        })
        .map(|claim| claim.claim_type.clone())
        .collect::<Vec<_>>();
    let independent_lineages = evidence_claims
        .iter()
        .filter(|claim| matches!(claim.status.as_str(), "observed" | "verified"))
        .filter(|claim| claim.claim_type.starts_with("account."))
        .map(|claim| claim.independence_group.as_str())
        .filter(|group| !group.trim().is_empty())
        .collect::<std::collections::HashSet<_>>()
        .len();
    let person_stakeholder = stakeholders
        .iter()
        .find(|stakeholder| stakeholder.person_id == person.id && stakeholder.status != "held");
    let person_is_direct = person_stakeholder.is_some_and(|stakeholder| {
        stakeholder.role_fit == "direct"
            && !stakeholder.evidence_claim_ids.is_empty()
            && stakeholder.evidence_claim_ids.iter().all(|claim_id| {
                evidence_claims.iter().any(|claim| {
                    claim.id == *claim_id
                        && opportunity
                            .as_ref()
                            .is_some_and(|opportunity| claim.task_key == opportunity.task_key)
                        && matches!(claim.status.as_str(), "observed" | "verified")
                })
            })
            && (!brand.eq_ignore_ascii_case("wapahki")
                || (!stakeholder.contact_facility_evidence_id.trim().is_empty()
                    && evidence_claims.iter().any(|claim| {
                        claim.id == stakeholder.contact_facility_evidence_id
                            && claim.claim_type == "contact.facility_employment"
                            && claim.source_locator == format!("person:{}", person.id)
                            && opportunity.as_ref().is_some_and(|opportunity| {
                                claim.facility_id == opportunity.facility_id
                                    && claim.task_key == opportunity.task_key
                            })
                            && matches!(claim.status.as_str(), "observed" | "verified")
                    })))
    });
    let outagehub_person_can_discover = brand.eq_ignore_ascii_case("outagehub")
        && opportunity.as_ref().is_some_and(|opportunity| {
            person_stakeholder.is_some_and(|stakeholder| {
                outagehub_discovery_touch_block_reason(
                    opportunity,
                    stakeholder,
                    person,
                    &evidence_claims,
                )
                .is_none()
            })
        });
    let wapahki_person_can_discover = brand.eq_ignore_ascii_case("wapahki")
        && opportunity.as_ref().is_some_and(|opportunity| {
            person_stakeholder.is_some_and(|stakeholder| {
                wapahki_discovery_touch_block_reason(
                    opportunity,
                    stakeholder,
                    person,
                    &evidence_claims,
                )
                .is_none()
            })
        });
    if (person_is_direct || outagehub_person_can_discover || wapahki_person_can_discover)
        && has_reachable_channel
        && has_workflow_vantage
    {
        claim_keys.push("account.reachable_workflow_owner".into());
    }
    claim_keys.sort();
    claim_keys.dedup();
    let opportunity_has_required_site = opportunity.as_ref().is_some_and(|opportunity| {
        !brand.eq_ignore_ascii_case("wapahki")
            || (!opportunity.facility_id.trim().is_empty()
                && !opportunity.task_claim_id.trim().is_empty()
                && !opportunity.economic_claim_id.trim().is_empty()
                && evidence_claims.iter().any(|claim| {
                    claim.id == opportunity.task_claim_id
                        && claim.claim_type == "account.bounded_repetitive_task"
                        && claim.facility_id == opportunity.facility_id
                        && claim.task_key == opportunity.task_key
                        && matches!(claim.status.as_str(), "observed" | "verified")
                })
                && evidence_claims.iter().any(|claim| {
                    claim.id == opportunity.economic_claim_id
                        && claim.claim_type == "account.manual_task_economic_pressure"
                        && claim.facility_id == opportunity.facility_id
                        && claim.task_key == opportunity.task_key
                        && matches!(claim.status.as_str(), "observed" | "verified")
                }))
    });
    let opportunity_state = opportunity
        .as_ref()
        .map_or("research_required", |opportunity| {
            if (!person_is_direct && !outagehub_person_can_discover && !wapahki_person_can_discover)
                || !opportunity_has_required_site
            {
                "research_required"
            } else if person_is_direct
                && opportunity.evidence_status == "action_ready"
                && mandatory_action_signals_present(brand, &claim_keys)
                && independent_lineages >= 2
            {
                "action_ready"
            } else if matches!(
                opportunity.evidence_status.as_str(),
                "action_ready" | "discovery_ready"
            ) && mandatory_discovery_signals_present(brand, &claim_keys)
            {
                "discovery_ready"
            } else {
                "research_required"
            }
        });
    // Opportunity evidence, task identity, and direct-role lineage are the
    // commercial authorization boundary. The legacy lead-level state remains
    // useful for research diagnostics but cannot select or broaden the motion.
    let state = if play.is_none() {
        "no_play"
    } else {
        opportunity_state
    };

    // Deterministic commercial priority: lane + component breakdown derived
    // from the same persisted evidence that authorized the state. Persisted on
    // the opportunity so the CRM can display *why* an account sits in a lane.
    let engaged = if let Some(opportunity) = &opportunity {
        graded_problem_confirmed_for_thread(db, brand, &opportunity.id, &person.id)?
    } else {
        false
    };
    let priority = if brand.eq_ignore_ascii_case("outagehub") {
        if let Some(opportunity) = &opportunity {
            db.recompute_outagehub_opportunity_priority(&opportunity.id)?
        } else {
            Some(crate::priority::outagehub_priority(
                &crate::priority::PriorityInputs {
                    segment: None,
                    active_claims: 0,
                    decision_evidenced: false,
                    historical_match: false,
                    reachable_direct_owner: false,
                    headcount: 0,
                    problem_confirmed: false,
                },
            ))
        }
    } else {
        None
    };

    let sales_brief = match &opportunity {
        Some(opportunity) => db.get_sales_brief(&opportunity.id, &person.id)?,
        None => None,
    };
    let proof_asset = match &sales_brief {
        Some(brief) if !brief.artifact_id.trim().is_empty() => {
            db.get_proof_asset(&brief.artifact_id)?
        }
        _ => None,
    };
    let acquisition_context = match &opportunity {
        Some(opportunity) => db.get_acquisition_context(&opportunity.id, &person.id)?,
        None => None,
    };

    let mut experiment = None;
    let mut experiment_assignment_id = String::new();
    let mut experiment_arm = String::new();
    if state == "action_ready" {
        if let Some(play) = &play {
            if let Some(running) = db.running_experiment_for_play(&play.id)? {
                let assignment =
                    db.ensure_experiment_assignment(&running.id, lead_id, &person.id, "")?;
                experiment_assignment_id = assignment.id;
                experiment_arm = assignment.arm;
                experiment = Some(running);
            }
        }
    }

    Ok(GtmActionContext {
        state: state.into(),
        play,
        opportunity,
        evidence_claims,
        stakeholders,
        observations,
        matched_signal_keys: claim_keys,
        experiment,
        experiment_assignment_id,
        experiment_arm,
        engaged,
        priority,
        sales_brief,
        proof_asset,
        acquisition_context,
    })
}

fn mandatory_action_signals_present(brand: &str, matched: &[String]) -> bool {
    let required: &[&str] = if brand.eq_ignore_ascii_case("outagehub") {
        &[
            "account.fit_evidence",
            "account.distributed_locations",
            "account.outage_sensitive_exposure",
            "account.outage_sensitive_decision",
            "account.historical_location_outage_match",
            "account.reachable_workflow_owner",
        ]
    } else if brand.eq_ignore_ascii_case("gnk") {
        &[
            "account.fit_evidence",
            "account.specific_recurring_decision",
            "account.believable_operating_consequence",
            "account.external_trigger_or_mechanism_evidence",
            "account.reachable_workflow_owner",
        ]
    } else if brand.eq_ignore_ascii_case("wapahki") {
        &[
            "account.fit_evidence",
            "account.bounded_repetitive_task",
            "account.manual_task_economic_pressure",
            "account.reachable_workflow_owner",
        ]
    } else {
        return true;
    };
    required
        .iter()
        .all(|required| matched.iter().any(|key| key == required))
}

fn mandatory_discovery_signals_present(brand: &str, matched: &[String]) -> bool {
    let has = |key: &str| matched.iter().any(|matched| matched == key);
    if !has("account.fit_evidence") || !has("account.reachable_workflow_owner") {
        return false;
    }
    if brand.eq_ignore_ascii_case("wapahki") {
        has("account.bounded_repetitive_task") && has("account.manual_task_economic_pressure")
    } else if brand.eq_ignore_ascii_case("gnk") {
        has("account.specific_recurring_decision")
            || has("account.external_trigger_or_mechanism_evidence")
    } else if brand.eq_ignore_ascii_case("outagehub") {
        has("account.distributed_locations") && has("account.outage_sensitive_exposure")
    } else {
        true
    }
}

/// Last-mile evidence guard. Qualification applies a richer version of this
/// check, but the action boundary must remain safe when old CRM observations
/// were created under an earlier playbook.
fn credible_action_signal(brand: &str, key: &str, evidence: &str) -> bool {
    if !brand.eq_ignore_ascii_case("outagehub") {
        return true;
    }
    crate::qualification::credible_outagehub_signal(key, evidence)
}

pub fn signal_catalog_prompt(brand: &str) -> String {
    let rows = default_signal_definitions()
        .into_iter()
        .filter(|definition| definition.brand == brand)
        .map(|definition| {
            format!(
                "- {}: {} (entity {}; source {}; expires after {} days)",
                definition.key,
                definition.description,
                definition.entity_type,
                definition.source_kind,
                definition.freshness_seconds / 86_400
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "CANONICAL SIGNAL CATALOG\n{rows}\nMap supported evidence to these keys only. Evidence is an observed fact; do not encode a guessed pain, product recommendation, or consequence as a signal."
    )
}

pub fn default_signal_definitions() -> Vec<SignalDefinition> {
    let mut definitions = Vec::new();
    for brand in ["outagehub", "gnk", "wapahki"] {
        definitions.push(SignalDefinition {
            brand: brand.into(),
            key: "account.fit_evidence".into(),
            name: "Account fit evidence".into(),
            description: "A source-backed account fact that materially supports the brand's current workflow hypothesis.".into(),
            topic: "account_fit".into(),
            entity_type: "account".into(),
            value_type: "text".into(),
            source_kind: "research".into(),
            owner: "internal_gtm_engineering".into(),
            refresh_cadence: "on account refresh".into(),
            freshness_seconds: 90 * 86_400,
            evidence_required: true,
            minimum_confidence: 0.60,
            version: 1,
            status: "active".into(),
            ..Default::default()
        });
        definitions.push(SignalDefinition {
            brand: brand.into(),
            key: "contact.workflow_vantage".into(),
            name: "Workflow vantage".into(),
            description: "The contact's role plausibly gives direct observation of, ownership of, or a route to the workflow under test.".into(),
            topic: "stakeholder_fit".into(),
            entity_type: "person".into(),
            value_type: "text".into(),
            source_kind: "research".into(),
            owner: "internal_gtm_engineering".into(),
            refresh_cadence: "on contact refresh".into(),
            freshness_seconds: 90 * 86_400,
            evidence_required: true,
            minimum_confidence: 0.60,
            version: 1,
            status: "active".into(),
            ..Default::default()
        });
        definitions.push(SignalDefinition {
            brand: brand.into(),
            key: "account.job_posting_workflow_evidence".into(),
            name: "Job-posting workflow evidence".into(),
            description: "A current first-party job posting explicitly names a system, workflow, responsibility, or investment relevant to the active motion. It does not prove pain, urgency, budget, buying intent, or recipient ownership.".into(),
            topic: "account_fit".into(),
            entity_type: "account".into(),
            value_type: "text".into(),
            source_kind: "official_job_posting".into(),
            owner: "internal_gtm_engineering".into(),
            refresh_cadence: "monthly while role is live".into(),
            freshness_seconds: 30 * 86_400,
            evidence_required: true,
            minimum_confidence: 0.70,
            version: 1,
            status: "active".into(),
            ..Default::default()
        });
        definitions.push(SignalDefinition {
            brand: brand.into(),
            key: "conversation.problem_confirmed".into(),
            name: "Problem confirmed".into(),
            description: "A human prospect confirmed or corrected the current workflow problem in their own words.".into(),
            topic: "problem_validation".into(),
            entity_type: "conversation".into(),
            value_type: "text".into(),
            source_kind: "reply".into(),
            owner: "forward_deployed_gtm".into(),
            refresh_cadence: "event driven".into(),
            freshness_seconds: 365 * 86_400,
            evidence_required: true,
            minimum_confidence: 0.75,
            version: 1,
            status: "active".into(),
            ..Default::default()
        });
    }

    definitions.extend([
        signal_definition("outagehub", "account.outage_sensitive_exposure", "Outage-sensitive exposure", "First-party evidence shows that the account operates Canadian locations, assets, or services where grid loss could matter. This prioritizes research but does not prove any internal outage workflow or decision."),
        signal_definition("outagehub", "account.outage_sensitive_decision", "Outage-sensitive decision", "Atomic source evidence names an outage or utility-status event and the actual diagnosis, dispatch, escalation, hold, transfer, prioritization, continuity, or communication decision the account makes. Exposure or a proposed use does not count."),
        signal_definition("outagehub", "account.distributed_locations", "Distributed locations", "The account operates multiple or remote locations across utility territories, making location matching operationally relevant."),
        signal_definition("outagehub", "account.operated_ev_charging_network", "Operated EV charging network", "First-party evidence shows the account operates, manages, or monitors a Canadian EV-charging site network rather than merely selling or installing charging equipment."),
        signal_definition("outagehub", "account.historical_location_outage_match", "Historical location-outage match", "A completed account-specific analysis names a verified operated property, laboratory, warehouse, tower, store, residence, plant, charging site, or other location and timestamp that overlapped a reported utility outage area. A proposed or hypothetical match does not count."),
        signal_definition("outagehub", "account.existing_operational_system", "Existing operational system", "The account names or evidences an existing NOC, dispatch, CMMS, SCADA, ServiceNow, Salesforce, or equivalent workflow surface."),
        signal_definition("gnk", "account.expensive_recurring_workflow", "Expensive recurring workflow", "The account evidences a recurring decision, exception, investigation, delay, or handoff with material operational consequences."),
        signal_definition("gnk", "account.cross_system_reconciliation", "Cross-system reconciliation", "People appear to reconcile records, evidence, or decisions across systems that do not supply the required coordination layer."),
        signal_definition("gnk", "account.specific_recurring_decision", "Specific recurring decision", "Public evidence identifies the repeated operating event and the concrete decision or fork that follows; company category or department presence is insufficient."),
        signal_definition("gnk", "account.believable_operating_consequence", "Believable operating consequence", "Public evidence connects the decision to settlement time, leakage, recoveries, audit exposure, customer SLA, write-offs, escalation, or constrained senior capacity."),
        signal_definition("gnk", "account.external_trigger_or_mechanism_evidence", "External trigger or mechanism evidence", "A source names the event, artifacts, records, systems, or handoff that makes the hypothesized reconstruction mechanism account-specific rather than universal."),
        signal_definition("gnk", "account.reachable_workflow_owner", "Reachable workflow owner", "A mid-market workflow owner or close observer is identifiable and reachable for a founder-led diagnostic conversation."),
        signal_definition("wapahki", "account.bounded_repetitive_task", "Bounded repetitive task", "A specific physical motion or handoff repeats enough within a production run to be described and measured."),
        signal_definition("wapahki", "account.format_variability", "Format variability", "SKU, case, orientation, changeover, or presentation variability plausibly defeats conventional fixed automation."),
        signal_definition("wapahki", "account.exception_heavy_manual_work", "Manual exception handling", "Operators remain in the task because misfeeds, damage, sanitation, changeovers, or other exceptions require judgment or dexterity."),
        signal_definition("wapahki", "account.manual_task_economic_pressure", "Manual-task economic pressure", "First-party evidence links the candidate task or station to staffing, overtime, throughput, stoppage, utilization, short-run economics, changeover cost, sanitation, ergonomics, or safety."),
    ]);
    definitions
}

fn signal_definition(brand: &str, key: &str, name: &str, description: &str) -> SignalDefinition {
    SignalDefinition {
        brand: brand.into(),
        key: key.into(),
        name: name.into(),
        description: description.into(),
        topic: "play_eligibility".into(),
        entity_type: "account".into(),
        value_type: "text".into(),
        source_kind: "research".into(),
        owner: "internal_gtm_engineering".into(),
        refresh_cadence: "on account refresh".into(),
        freshness_seconds: 90 * 86_400,
        evidence_required: true,
        minimum_confidence: 0.60,
        version: 1,
        status: "active".into(),
        ..Default::default()
    }
}

pub fn default_plays() -> Vec<GtmPlay> {
    vec![
        GtmPlay {
            brand: "outagehub".into(),
            key: "distributed_site_outage_decision".into(),
            version: 15,
            name: "Distributed-site outage decision".into(),
            lifecycle: "testing".into(),
            motion: "internal_pipeline_to_forward_deployed_proof".into(),
            target_icp: "Canadian operators with distributed, outage-sensitive locations or assets: charging, telecom, cold storage, data centres, multi-site facilities, service dispatch, backup power, and adjacent infrastructure. Prioritize visible decisions and reachable owners; do not require one vertical.".into(),
            target_vantages: vec!["process_owner".into(), "operator".into(), "technical_evaluator".into()],
            required_signal_keys: vec!["account.fit_evidence".into(), "account.distributed_locations".into(), "account.outage_sensitive_exposure".into(), "account.outage_sensitive_decision".into(), "account.historical_location_outage_match".into()],
            minimum_signal_matches: 5,
            hypothesis: "During an ambiguous location or asset incident, location-matched public utility context may improve one evidenced diagnosis, dispatch, escalation, continuity, prioritization, or communication decision.".into(),
            action_policy: "Select one bounded market segment, then assess operator × site-network × outage-time-decision evidence separately from commercial lane. Action-ready opportunities have atomic source claims for the decision, a mapped committee, and completed historical proof; T1 leads with the exact location, utility, and timestamp. Discovery-ready opportunities may receive one independently reviewed, manually approved T1 only when it offers a location-specific comparison or sample response OutageHub can actually produce. Generic footprint summaries and sender-benefit research requests are held. Automation never schedules cold copy and never claims private site status.".into(),
            proof_type: "historical_replay".into(),
            proof_description: "Match supplied or public operating locations to historical utility outage areas, recording the location, utility timestamp, and what an API response or webhook would have returned.".into(),
            success_metric: "Whether outside utility status is checked today and whether it changes the account-specific diagnosis, dispatch, escalation, continuity, prioritization, or communication decision.".into(),
            kill_condition: "Private telemetry already settles the relevant utility question quickly enough, the operation has no location-specific outage decision, or no reachable person can observe it.".into(),
            source_refs: vec!["playbooks/outagehub.toml".into(), "youtube:wNnU2BJILPA@509".into()],
            ..Default::default()
        },
        GtmPlay {
            brand: "gnk".into(),
            key: "closed_case_reconstruction".into(),
            version: 7,
            name: "Workflow-specific decision support".into(),
            lifecycle: "testing".into(),
            motion: "internal_pipeline_to_forward_deployed_proof".into(),
            target_icp: "Organizations with a source-backed recurring decision, exception, coordination burden, or workflow-specific software gap. Cover industries broadly; rank visible consequences and reachable owners first, then investigate discovery-ready and research-required accounts.".into(),
            target_vantages: vec!["process_owner".into(), "operator".into(), "operational_executive".into(), "router".into()],
            required_signal_keys: vec!["account.fit_evidence".into(), "account.specific_recurring_decision".into(), "account.believable_operating_consequence".into(), "account.external_trigger_or_mechanism_evidence".into()],
            minimum_signal_matches: 4,
            hypothesis: "A source-backed trigger creates a recurring exception decision whose inputs, coordination, or existing system boundary produces a meaningful operating consequence.".into(),
            action_policy: "Work one bounded problem segment at a time and qualify company × workflow opportunity, not company category. Cold planning produces T1 only from a complete SendableBrief: atomic account claims, person-specific role evidence or an honest routing sentinel, one expensive moment, consequence, genuinely prepared artifact, required input, improved decision, and expected reply. Titles and account keywords never prove direct ownership. Persist each governed mode, abstain from unsupported modes, validate facts and artifact content, select blindly, and require exact-copy manual approval. If the offer cannot honestly say what GnK examined and returns, hold it. Automation never schedules cold copy.".into(),
            proof_type: "bounded_workflow_replay".into(),
            proof_description: "Apply the proposed workflow to a small historical sample and compare decision time, exception handling, and outcome quality with the current process.".into(),
            success_metric: "The account-specific consequence named in research: resolution time, leakage or recoveries, audit exposure, customer SLA, throughput, escalation, or constrained expert capacity.".into(),
            kill_condition: "The decision is not recurring or consequential, no source-backed trigger or mechanism exists, the position is immediately visible, or the recipient is not close enough to answer.".into(),
            source_refs: vec!["playbooks/gnk.toml".into(), "youtube:wNnU2BJILPA@509".into()],
            ..Default::default()
        },
        GtmPlay {
            brand: "wapahki".into(),
            key: "task_exception_review".into(),
            version: 9,
            name: "Task-and-exception feasibility review".into(),
            lifecycle: "testing".into(),
            motion: "internal_pipeline_to_forward_deployed_proof".into(),
            target_icp: "Factories, product manufacturers, warehouses, distribution centres, and fulfillment operations across Canada. Rank one facility-linked physical task and economic pressure first. Person-specific facility proof permits an owner question; a verified adjacent operations contact may receive one routing question without being described as the owner.".into(),
            target_vantages: vec!["process_owner".into(), "operator".into(), "technical_evaluator".into(), "router".into()],
            required_signal_keys: vec!["account.fit_evidence".into(), "account.bounded_repetitive_task".into(), "account.manual_task_economic_pressure".into()],
            minimum_signal_matches: 3,
            hypothesis: "One recurring physical movement may be automatable enough to investigate, with the exact variation, rate, integration, and economics confirmed by the operator closest to the work.".into(),
            action_policy: "Work company × facility × line/workcell × task, keeping evidence readiness separate from commercial lane. One independently reviewed 45–95 word T1 may be considered only with facility, task, and economic claim lineage. Exact facility-employment proof permits an owner question; the bounded adjacent-contact lane permits only a routing question. Credentials, Wapahki, hypothesis language, and the fit screen are optional. A screen may appear only when a completed account-specific result is attached. Silence creates no cadence and automation never schedules cold copy.".into(),
            proof_type: "task_feasibility_review".into(),
            proof_description: "Review a task sketch, short video, or representative SKU/changeover set; model the normal motion, exceptions, rate, and technical boundaries.".into(),
            success_metric: "Required rate, intervention frequency, changeover burden, task coverage, and a clear technical/economic stop condition.".into(),
            kill_condition: "Variation, sanitation, damage risk, rate, or constant human intervention makes a bounded cell technically or economically unattractive.".into(),
            source_refs: vec!["playbooks/wapahki.toml".into(), "businesses/wapahki.toml".into(), "youtube:wNnU2BJILPA@509".into()],
            ..Default::default()
        },
    ]
}

pub fn default_market_segments() -> Vec<MarketSegment> {
    let mut segments = [
        ("wapahki", "canada_food_case_palletizing", "Canadian food case packing and palletizing", "Canada", "Repeatable case/tray packing, palletizing, depalletizing, or cold-chain handling at food plants and distribution centres", "company × facility × line/workcell × task"),
        ("wapahki", "canada_warehouse_case_handling", "Canadian warehouse case handling", "Canada", "Manual case, tote, pallet, and outbound handling at warehouses, 3PLs, and distribution centres", "company × facility × zone × task"),
        ("wapahki", "canada_manufacturing_machine_tending", "Canadian manufacturing machine tending", "Canada", "Repetitive loading, unloading, transfer, inspection, or kitting around production equipment", "company × facility × line/workcell × task"),
        ("gnk", "canada_3pl_exception_decisions", "3PL exception and reconciliation decisions", "Canada", "Recurring shipment, deduction, claim, document, and SLA exceptions that require cross-system reconstruction", "company × workflow opportunity"),
        ("gnk", "canada_construction_delay_evidence", "Construction delay-evidence reconstruction", "Canada", "Recurring delay, change-order, payment, and project-record decisions with evidence split across tools", "company × project workflow opportunity"),
        ("gnk", "canada_specialty_claims_admin", "Specialty claims and case administration", "Canada", "Recurring eligibility, evidence, escalation, recovery, and filing decisions with narrow software gaps", "company × case workflow opportunity"),
    ]
    .into_iter()
    .map(|(brand, key, name, geography, wedge, unit)| MarketSegment {
        brand: brand.into(),
        key: key.into(),
        version: 1,
        name: name.into(),
        geography: geography.into(),
        wedge: wedge.into(),
        unit_of_analysis: unit.into(),
        enumeration_sources: vec![
            "official registries/directories".into(),
            "company facility pages and job postings".into(),
            "Apollo enrichment after enumeration".into(),
        ],
        status: "active".into(),
        ..Default::default()
    })
    .collect::<Vec<_>>();
    segments.extend(crate::segments::OUTAGE_SEGMENTS.iter().map(|segment| {
        MarketSegment {
            brand: "outagehub".into(),
            key: crate::segments::market_key_for_segment(segment.key)
                .expect("every OutageHub doctrine segment has a market key")
                .into(),
            version: 1,
            name: match segment.key {
                "ev_charging" => "Canadian EV charging operations",
                "telecom" => "Canadian telecom site continuity",
                "generator_services" => "Canadian backup-power dispatch",
                _ => segment.name,
            }
            .into(),
            geography: "Canada".into(),
            wedge: format!("{} {}", segment.operating_event, segment.decision),
            unit_of_analysis: "operator × location portfolio × outage-time decision".into(),
            enumeration_sources: vec![
                "official registries/directories".into(),
                "company operating-location pages and job postings".into(),
                "Apollo enrichment after first-party enumeration".into(),
            ],
            status: "active".into(),
            ..Default::default()
        }
    }));
    segments
}

pub fn seed_defaults(db: &SharedDb) -> Result<()> {
    for definition in default_signal_definitions() {
        db.insert_signal_definition_if_absent(&definition)?;
    }
    for play in default_plays() {
        let play_id = db.insert_gtm_play_if_absent(&play)?;
        db.retire_older_unproven_gtm_play_versions(&play.brand, &play.key, play.version)?;
        db.relabel_unassessed_qualified_leads(&play.brand, &play_id)?;
    }
    for segment in default_market_segments() {
        db.upsert_market_segment(&segment)?;
    }
    db.backfill_legacy_signal_observations()?;
    db.backfill_sequence_gtm_attribution()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        customer_development_missing, customer_development_stage, default_market_segments,
        default_plays, default_signal_definitions, graded_problem_confirmed_for_lead,
        prepare_action, seed_defaults, GtmActionContext, SignalCandidate,
    };
    use crate::db::{
        AccountPlayAssessment, CustomerDevelopmentRecord, Db, EvidenceClaim, Lead, Person, SharedDb,
    };
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn discovery_requires_manual_approval_and_never_auto_schedules() {
        let discovery = GtmActionContext {
            state: "discovery_ready".into(),
            ..Default::default()
        };
        assert!(discovery.sequence_ready_for(1));
        assert!(!discovery.sequence_ready_for(2));
        assert!(discovery.delivery_ready_for(1));
        assert!(!discovery.automatic_delivery_ready_for(1));

        let ready = GtmActionContext {
            state: "action_ready".into(),
            ..Default::default()
        };
        assert!(ready.sequence_ready_for(1));
        assert!(ready.delivery_ready_for(1));
    }

    #[test]
    fn all_twelve_outagehub_doctrine_segments_are_persisted_markets() {
        let markets = default_market_segments()
            .into_iter()
            .filter(|segment| segment.brand == "outagehub")
            .collect::<Vec<_>>();
        assert_eq!(markets.len(), crate::segments::OUTAGE_SEGMENTS.len());
        for doctrine in crate::segments::OUTAGE_SEGMENTS {
            let key =
                crate::segments::market_key_for_segment(doctrine.key).expect("doctrine market key");
            assert!(markets.iter().any(|market| market.key == key), "{key}");
        }
    }

    fn remove_temp_db(path: &std::path::Path) {
        for candidate in [
            path.to_path_buf(),
            std::path::PathBuf::from(format!("{}-wal", path.display())),
            std::path::PathBuf::from(format!("{}-shm", path.display())),
        ] {
            let _ = std::fs::remove_file(candidate);
        }
    }

    #[test]
    fn outagehub_requires_functional_proximity_not_generic_operational_altitude() {
        let charging_decision = "The operator monitors charging stations and decides whether a charger incident needs field-service escalation.";
        assert!(crate::qualification::outagehub_role_matches_decision(
            "Director of Charging Network Operations",
            "operational_executive",
            charging_decision,
        ));
        assert!(crate::qualification::outagehub_role_matches_decision(
            "Maintenance Manager",
            "process_owner",
            charging_decision,
        ));
        assert!(!crate::qualification::outagehub_role_matches_decision(
            "Senior Manager, Customer Operations",
            "process_owner",
            charging_decision,
        ));
        assert!(!crate::qualification::outagehub_role_matches_decision(
            "Director of Customer Success",
            "process_owner",
            charging_decision,
        ));
        assert!(!crate::qualification::outagehub_role_matches_decision(
            "Operational Excellence EPMO",
            "operational_executive",
            charging_decision,
        ));
    }

    #[test]
    fn outagehub_five_email_ceiling_requires_a_graded_reply_observation() {
        let db = Db::open(":memory:").expect("open memory db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-graded-reply".into(),
                name: "Graded Reply Operator".into(),
                ..Default::default()
            })
            .expect("lead");
        db.record_signal_observation(&crate::db::SignalObservation {
            brand: "outagehub".into(),
            definition_key: "conversation.problem_confirmed".into(),
            lead_id: lead_id.clone(),
            conversation_id: "conversation-friendly".into(),
            source_name: "prospect_reply".into(),
            value_json: serde_json::json!({"category":"interested"}).to_string(),
            evidence: "Sounds interesting; happy to talk.".into(),
            confidence: 0.85,
            status: "verified".into(),
            ..Default::default()
        })
        .expect("legacy reply observation");
        assert!(!graded_problem_confirmed_for_lead(&db, "outagehub", &lead_id).unwrap());

        db.record_signal_observation(&crate::db::SignalObservation {
            brand: "outagehub".into(),
            definition_key: "conversation.problem_confirmed".into(),
            lead_id: lead_id.clone(),
            conversation_id: "conversation-explicit".into(),
            source_name: "prospect_reply".into(),
            value_json: serde_json::json!({
                "category":"correction",
                "grade":"explicit",
                "supporting_quote":"We check the utility map before dispatching a crew"
            })
            .to_string(),
            evidence: "Dispatch checks utility context before rolling a crew.".into(),
            confidence: 0.95,
            status: "verified".into(),
            ..Default::default()
        })
        .expect("graded reply observation");
        let engaged = graded_problem_confirmed_for_lead(&db, "outagehub", &lead_id).unwrap();
        assert!(engaged);
        assert_eq!(
            GtmActionContext {
                state: "action_ready".into(),
                play: default_plays()
                    .into_iter()
                    .find(|play| play.brand == "outagehub"),
                engaged,
                ..Default::default()
            }
            .max_authorized_touches(),
            5
        );
    }

    fn qualify_current_play(db: &SharedDb, brand: &str, lead_id: &str, keys: &[&str]) {
        let play = db
            .current_gtm_play(brand)
            .expect("play query")
            .expect("current play");
        db.upsert_account_play_assessment(&AccountPlayAssessment {
            lead_id: lead_id.to_string(),
            brand: brand.to_string(),
            play_id: play.id,
            play_version: play.version,
            status: "qualified".into(),
            fit_score: 85,
            matched_signal_keys: keys.iter().map(|key| (*key).to_string()).collect(),
            root_cause: "A bounded operating decision may benefit from external context.".into(),
            proof_fit: "The hypothesis can be tested with a small historical comparison.".into(),
            source: "test".into(),
            ..Default::default()
        })
        .expect("qualified assessment");
    }

    #[test]
    fn title_and_inferred_vantage_do_not_prove_direct_task_ownership() {
        let path = std::env::temp_dir().join(format!(
            "spruce-vantage-readiness-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Arc::new(Db::open(&path).expect("open temp db"));
        seed_defaults(&db).expect("seed GTM defaults");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "org-vantage-ready".into(),
                name: "Example".into(),
                ..Default::default()
            })
            .expect("lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "person-vantage-ready".into(),
                name: "Pat Operator".into(),
                title: "Claims Operations Manager".into(),
                apollo_org_id: "org-vantage-ready".into(),
                employer_verification: "apollo".into(),
                vantage: "process_owner".into(),
                can_observe: "Owns the recurring claims review workflow".into(),
                email: "pat@example.com".into(),
                email_status: "verified".into(),
                status: "verified".into(),
                ..Default::default()
            })
            .expect("person");
        db.record_signal_candidates(
            "gnk",
            &lead_id,
            &[
                SignalCandidate {
                    definition_key: "account.fit_evidence".into(),
                    evidence: "The company operates specialty claims.".into(),
                    source_url: "https://example.com/claims".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.specific_recurring_decision".into(),
                    evidence: "After every disputed rejection, claims decides whether to pursue recovery or write it off.".into(),
                    source_url: "https://example.org/claims-guide".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.believable_operating_consequence".into(),
                    evidence: "The review affects recoveries, settlement time, and senior reviewer capacity.".into(),
                    source_url: "https://example.net/recovery".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.external_trigger_or_mechanism_evidence".into(),
                    evidence: "A public claims guide requires policy, investigation, and prior-action records before escalation.".into(),
                    source_url: "https://example.ca/escalation".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
            ],
            "test",
        )
        .expect("signals");
        let play = db
            .current_gtm_play("gnk")
            .expect("play query")
            .expect("play");
        db.upsert_account_play_assessment(&AccountPlayAssessment {
            lead_id: lead_id.clone(),
            brand: "gnk".into(),
            play_id: play.id,
            play_version: play.version,
            status: "qualified".into(),
            fit_score: 85,
            matched_signal_keys: vec![
                "account.fit_evidence".into(),
                "account.specific_recurring_decision".into(),
                "account.believable_operating_consequence".into(),
                "account.external_trigger_or_mechanism_evidence".into(),
            ],
            source: "test-before-contact-mapping".into(),
            ..Default::default()
        })
        .expect("research-needed assessment");
        let person = db.get_person(&person_id).unwrap().unwrap();
        let context = prepare_action(&db, "gnk", &lead_id, &person).expect("action context");

        assert!(!context
            .matched_signal_keys
            .contains(&"account.reachable_workflow_owner".to_string()));
        assert_eq!(context.state, "research_required");
        assert!(!context.action_ready());

        let discovery_lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "org-vantage-discovery".into(),
                name: "Discovery Example".into(),
                ..Default::default()
            })
            .expect("discovery lead");
        let discovery_person_id = db
            .upsert_person(&Person {
                lead_id: discovery_lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "person-vantage-discovery".into(),
                name: "Dana Operator".into(),
                title: "Operations Manager".into(),
                vantage: "process_owner".into(),
                email: "dana@example.com".into(),
                email_status: "verified".into(),
                status: "verified".into(),
                ..Default::default()
            })
            .expect("discovery person");
        db.record_signal_candidates(
            "gnk",
            &discovery_lead_id,
            &[SignalCandidate {
                definition_key: "account.fit_evidence".into(),
                evidence: "The company publicly documents a complex operations function.".into(),
                confidence: 0.9,
                ..Default::default()
            }],
            "test",
        )
        .expect("discovery signal");
        let play = db
            .current_gtm_play("gnk")
            .expect("play query")
            .expect("play");
        db.upsert_account_play_assessment(&AccountPlayAssessment {
            lead_id: discovery_lead_id.clone(),
            brand: "gnk".into(),
            play_id: play.id,
            play_version: play.version,
            status: "research_needed".into(),
            fit_score: 60,
            matched_signal_keys: vec!["account.fit_evidence".into()],
            source: "test-current-assessment".into(),
            ..Default::default()
        })
        .expect("discovery assessment");
        let discovery_person = db.get_person(&discovery_person_id).unwrap().unwrap();
        let discovery = prepare_action(&db, "gnk", &discovery_lead_id, &discovery_person)
            .expect("discovery context");
        assert_eq!(discovery.state, "research_required");
        assert!(!discovery.sequence_ready_for(1));
        assert!(!discovery.sequence_ready_for(7));
        assert!(!discovery.action_ready());
        assert!(discovery
            .copy_prompt_block()
            .contains("Hold it for research"));
        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn current_research_needed_assessment_cannot_enter_multitouch_copy() {
        let path = std::env::temp_dir().join(format!(
            "spruce-assessment-readiness-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Arc::new(Db::open(&path).expect("open temp db"));
        seed_defaults(&db).expect("seed GTM defaults");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-assessment-held".into(),
                name: "Held Operator".into(),
                ..Default::default()
            })
            .expect("lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                apollo_person_id: "person-assessment-held".into(),
                name: "Pat Operator".into(),
                vantage: "process_owner".into(),
                email_status: "verified".into(),
                ..Default::default()
            })
            .expect("person");
        let signals = vec![
            SignalCandidate {
                definition_key: "account.fit_evidence".into(),
                evidence: "The company operates a Canadian network operations centre.".into(),
                confidence: 0.9,
                ..Default::default()
            },
            SignalCandidate {
                definition_key: "account.distributed_locations".into(),
                evidence: "The company operates multiple remote sites across Canada.".into(),
                confidence: 0.9,
                ..Default::default()
            },
            SignalCandidate {
                definition_key: "account.outage_sensitive_decision".into(),
                evidence: "Operators check utility outages before dispatching to a site.".into(),
                confidence: 0.9,
                ..Default::default()
            },
        ];
        db.record_signal_candidates("outagehub", &lead_id, &signals, "test")
            .expect("signals");
        let play = db
            .current_gtm_play("outagehub")
            .expect("play query")
            .expect("play");
        db.upsert_account_play_assessment(&AccountPlayAssessment {
            lead_id: lead_id.clone(),
            brand: "outagehub".into(),
            play_id: play.id,
            play_version: play.version,
            status: "research_needed".into(),
            fit_score: 60,
            matched_signal_keys: signals
                .iter()
                .map(|signal| signal.definition_key.clone())
                .collect(),
            ..Default::default()
        })
        .expect("assessment");

        let person = db.get_person(&person_id).unwrap().unwrap();
        let context = prepare_action(&db, "outagehub", &lead_id, &person).expect("context");
        assert!(!context.action_ready());
        assert!(!context.sequence_ready_for(7));
        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn generic_cold_storage_footprint_cannot_enter_the_ev_experiment() {
        let path = std::env::temp_dir().join(format!(
            "spruce-outage-signal-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Arc::new(Db::open(&path).expect("open temp db"));
        seed_defaults(&db).expect("seed GTM defaults");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-generic-cold-storage".into(),
                name: "Generic Cold Storage".into(),
                ..Default::default()
            })
            .expect("lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                apollo_person_id: "person-generic-cold-storage".into(),
                name: "Casey Manager".into(),
                vantage: "process_owner".into(),
                email_status: "verified".into(),
                ..Default::default()
            })
            .expect("person");
        db.record_signal_candidates(
            "outagehub",
            &lead_id,
            &[
                SignalCandidate {
                    definition_key: "account.fit_evidence".into(),
                    evidence: "The company provides refrigerated logistics.".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.distributed_locations".into(),
                    evidence: "The company operates facilities across Canada.".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.outage_sensitive_decision".into(),
                    evidence: "The company provides 24/7 refrigerated logistics and hold control."
                        .into(),
                    confidence: 0.9,
                    ..Default::default()
                },
            ],
            "legacy-test",
        )
        .expect("signals");

        let person = db.get_person(&person_id).unwrap().unwrap();
        let stale = prepare_action(&db, "outagehub", &lead_id, &person).expect("stale context");
        assert!(!stale.action_ready());

        qualify_current_play(
            &db,
            "outagehub",
            &lead_id,
            &["account.fit_evidence", "account.distributed_locations"],
        );
        let context = prepare_action(&db, "outagehub", &lead_id, &person).expect("context");
        assert!(!context
            .matched_signal_keys
            .contains(&"account.outage_sensitive_decision".to_string()));
        assert!(!context.action_ready());
        assert!(!context.sequence_ready_for(2));
        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn historical_match_without_person_responsibility_stays_discovery_only() {
        let path = std::env::temp_dir().join(format!(
            "spruce-outage-combined-signal-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Arc::new(Db::open(&path).expect("open temp db"));
        seed_defaults(&db).expect("seed GTM defaults");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-operated-cold-storage".into(),
                name: "Operated Charging Network".into(),
                hypothesis: "Service Operations checks utility status before dispatch or customer communication when a charging-site availability incident occurs.".into(),
                ..Default::default()
            })
            .expect("lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                apollo_person_id: "person-maintenance-owner".into(),
                name: "Morgan Engineer".into(),
                title: "Director, Service Operations".into(),
                apollo_org_id: "org-operated-cold-storage".into(),
                employer_verification: "apollo".into(),
                vantage: "process_owner".into(),
                email_status: "verified".into(),
                ..Default::default()
            })
            .expect("person");
        db.record_signal_candidates(
            "outagehub",
            &lead_id,
            &[
                SignalCandidate {
                    definition_key: "account.fit_evidence".into(),
                    evidence: "The company operates a Canadian EV charging network with public charging sites.".into(),
                    source_url: "https://operator.example/network".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.distributed_locations".into(),
                    evidence: "The operator runs multiple charging sites across Canada.".into(),
                    source_url: "https://natural-resources.canada.ca/stations".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.outage_sensitive_decision".into(),
                    evidence: "Service Operations checks utility status before dispatch or customer communication when a charging-site availability incident occurs.".into(),
                    source_url: "https://operator.example/service-operations".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.outage_sensitive_exposure".into(),
                    evidence: "The operator monitors its Canadian EV charging network and the charging sites it operates.".into(),
                    source_url: "https://operator.example/network-operations".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.historical_location_outage_match".into(),
                    evidence: "On 2026-07-14 at 14:30, the charging site at 123 King Street overlapped a utility outage area in a utility report.".into(),
                    source_url: "https://api.outagehub.ca/historical-match/123".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
            ],
            "combined-test",
        )
        .expect("signals");

        qualify_current_play(
            &db,
            "outagehub",
            &lead_id,
            &[
                "account.fit_evidence",
                "account.distributed_locations",
                "account.outage_sensitive_exposure",
                "account.outage_sensitive_decision",
                "account.historical_location_outage_match",
            ],
        );
        let person = db.get_person(&person_id).unwrap().unwrap();
        let context = prepare_action(&db, "outagehub", &lead_id, &person).expect("context");
        assert!(context
            .matched_signal_keys
            .contains(&"account.historical_location_outage_match".to_string()));
        assert_eq!(context.state, "discovery_ready", "{context:#?}");
        assert!(context.sequence_ready_for(1));
        assert!(!context.sequence_ready_for(2));
        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn outagehub_exposure_and_segment_owner_allow_one_question_not_a_cadence() {
        let path = std::env::temp_dir().join(format!(
            "spruce-outage-discovery-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Arc::new(Db::open(&path).expect("open temp db"));
        seed_defaults(&db).expect("seed GTM defaults");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-lab-discovery".into(),
                name: "Example Diagnostics".into(),
                observed_facts: vec!["Example Diagnostics operates laboratories and patient service centres across Ontario and Quebec.".into()],
                hypothesis: "When a location reports a power issue, operations may check the local utility separately.".into(),
                ..Default::default()
            })
            .expect("lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                apollo_person_id: "person-lab-operations".into(),
                name: "Karina Operator".into(),
                title: "Director of Operations".into(),
                apollo_org_id: "org-lab-discovery".into(),
                employer_verification: "apollo".into(),
                vantage: "process_owner".into(),
                email_status: "verified".into(),
                ..Default::default()
            })
            .expect("person");
        let signals = vec![
            SignalCandidate {
                definition_key: "account.fit_evidence".into(),
                evidence: "Example Diagnostics operates Canadian laboratories and patient service centres.".into(),
                source_url: "https://example.test/about".into(),
                confidence: 0.9,
            },
            SignalCandidate {
                definition_key: "account.distributed_locations".into(),
                evidence: "Example Diagnostics operates laboratory facilities across Ontario and Quebec.".into(),
                source_url: "https://example.test/locations".into(),
                confidence: 0.9,
            },
            SignalCandidate {
                definition_key: "account.outage_sensitive_exposure".into(),
                evidence: "Example Diagnostics operates laboratories and patient service centres that process clinical specimens across Ontario and Quebec.".into(),
                source_url: "https://example.test/laboratories".into(),
                confidence: 0.9,
            },
        ];
        db.record_signal_candidates("outagehub", &lead_id, &signals, "test")
            .expect("signals");
        let play = db
            .current_gtm_play("outagehub")
            .expect("play query")
            .expect("play");
        db.upsert_account_play_assessment(&AccountPlayAssessment {
            lead_id: lead_id.clone(),
            brand: "outagehub".into(),
            play_id: play.id,
            play_version: play.version,
            status: "research_needed".into(),
            fit_score: 60,
            matched_signal_keys: signals
                .iter()
                .map(|signal| signal.definition_key.clone())
                .collect(),
            symptom: "A location reports a power issue; whether operations separately checks the utility is unknown.".into(),
            source: "test".into(),
            ..Default::default()
        })
        .expect("assessment");

        let person = db.get_person(&person_id).unwrap().unwrap();
        let context = prepare_action(&db, "outagehub", &lead_id, &person).expect("context");
        assert_eq!(context.state, "discovery_ready", "{context:#?}");
        assert!(context.sequence_ready_for(1));
        assert!(!context.sequence_ready_for(2));
        assert!(
            super::recipient_sequence_block_reason(&db, "outagehub", &lead_id, &person, 1,)
                .expect("one-touch gate")
                .is_none()
        );
        assert!(
            super::recipient_sequence_block_reason(&db, "outagehub", &lead_id, &person, 2,)
                .expect("multi-touch gate")
                .is_some()
        );

        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn customer_development_advances_on_evidence_not_activity() {
        let mut record = CustomerDevelopmentRecord::default();
        assert_eq!(customer_development_stage(&record), "hypothesis");

        record.engaged_at = "2026-08-08T10:00:00Z".into();
        assert_eq!(customer_development_stage(&record), "engaged");
        record.problem = "Operators reorient mixed cases by hand.".into();
        assert_eq!(customer_development_stage(&record), "problem_confirmed");

        record.task_scope = "Move sealed cases from conveyor to pallet.".into();
        record.current_workflow = "Two operators handle each case.".into();
        record.why_manual = "Case size and orientation change by SKU.".into();
        assert_eq!(customer_development_stage(&record), "problem_confirmed");
        record.variations = vec!["Five case sizes".into()];
        assert_eq!(customer_development_stage(&record), "task_mapped");

        record.evidence = vec!["Video of the last production run".into()];
        assert_eq!(customer_development_stage(&record), "evidence_shared");
    }

    #[test]
    fn commitments_are_explicit_and_next_gate_names_missing_terms() {
        let mut record = CustomerDevelopmentRecord {
            commitment_kind: "design_partner".into(),
            ..Default::default()
        };
        assert_eq!(customer_development_stage(&record), "hypothesis");

        record.engaged_at = "2026-08-08T10:00:00Z".into();
        record.problem = "Manual case handling".into();
        record.task_scope = "Conveyor to pallet".into();
        record.current_workflow = "Two operators move sealed cases".into();
        record.why_manual = "Formats change".into();
        record.variations = vec!["Five case sizes".into()];
        record.evidence = vec!["Production video".into()];
        record.success_criteria = "Ten cases per minute".into();
        record.stop_condition = "More than one intervention per hundred cases".into();
        record.timeline = "Review in September".into();
        record.commitment_detail =
            "Plant manager agreed to the evaluation and weekly access.".into();
        record.stakeholders = vec!["Plant manager — sponsor".into()];
        assert_eq!(customer_development_stage(&record), "design_partner");
        assert_eq!(
            customer_development_missing(&record),
            vec![
                "deployment site",
                "provisional cell quantity",
                "price range or payback case",
                "conditions to purchase",
                "explicit agreement to material LOI terms",
            ]
        );
    }

    #[test]
    fn job_posting_signal_is_short_lived_and_cannot_claim_pain() {
        let definitions = default_signal_definitions();
        let signal = definitions
            .iter()
            .find(|definition| {
                definition.brand == "wapahki"
                    && definition.key == "account.job_posting_workflow_evidence"
            })
            .expect("job-posting signal");

        assert_eq!(signal.source_kind, "official_job_posting");
        assert_eq!(signal.freshness_seconds, 30 * 86_400);
        assert!(signal.description.contains("does not prove pain"));
    }

    #[test]
    fn wapahki_catalog_includes_manual_task_economics() {
        let definitions = default_signal_definitions();
        let signal = definitions
            .iter()
            .find(|definition| {
                definition.brand == "wapahki"
                    && definition.key == "account.manual_task_economic_pressure"
            })
            .expect("manual-task economic signal");

        assert!(signal.description.contains("staffing"));
        assert!(signal.description.contains("throughput"));
    }

    #[test]
    fn copy_context_exposes_evidence_but_not_internal_proof_machinery() {
        let context = GtmActionContext {
            state: "action_ready".into(),
            evidence_claims: vec![EvidenceClaim {
                claim_type: "account.fit_evidence".into(),
                source_url: "https://example.com/formats".into(),
                source_excerpt: "Official site names five production formats.".into(),
                status: "observed".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let brief = context.copy_prompt_block();
        assert!(brief.contains("Official site names five production formats"));
        assert!(brief.contains("role-relevant implication and a credible point of view"));
        assert!(brief.contains("Never invent collateral"));
        assert!(!brief.contains("FORWARD-DEPLOYED PROOF"));
        assert!(!brief.contains("EXPERIMENT"));
        assert!(!brief.contains("KILL CONDITION"));
    }

    #[test]
    fn fabricated_source_urls_are_rejected_at_the_observation_boundary() {
        let path =
            std::env::temp_dir().join(format!("spruce-source-url-test-{}.sqlite", Uuid::new_v4()));
        let db = Arc::new(Db::open(&path).expect("open temp db"));
        seed_defaults(&db).expect("seed GTM defaults");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "wapahki".into(),
                apollo_org_id: "org-source-url".into(),
                name: "Example Plant".into(),
                ..Default::default()
            })
            .expect("lead");
        let fabricated = db.record_signal_observation(&crate::db::SignalObservation {
            definition_key: "account.bounded_repetitive_task".into(),
            brand: "wapahki".into(),
            lead_id: lead_id.clone(),
            evidence: "Operators palletize cases at the Trenton, Ontario plant.".into(),
            source_url: "not provided in account payload".into(),
            confidence: 0.9,
            ..Default::default()
        });
        let message = fabricated
            .expect_err("prose masquerading as a source URL must be rejected")
            .to_string();
        assert!(
            message.contains("non-URL source"),
            "unexpected error: {message}"
        );
        db.record_signal_observation(&crate::db::SignalObservation {
            definition_key: "account.bounded_repetitive_task".into(),
            brand: "wapahki".into(),
            lead_id,
            evidence: "Operators palletize cases at the Trenton, Ontario plant.".into(),
            source_url: "https://example.com/jobs/packaging-operator".into(),
            confidence: 0.9,
            ..Default::default()
        })
        .expect("a URL-backed observation still persists");
        remove_temp_db(&path);
    }

    #[test]
    fn wapahki_single_discovery_email_is_limited_to_workflow_titles_with_url_backed_tasks() {
        let opportunity = crate::db::SalesOpportunity {
            task_key: "task-1".into(),
            facility_id: "facility-1".into(),
            evidence_status: "discovery_ready".into(),
            ..Default::default()
        };
        let adjacent = crate::db::OpportunityStakeholder {
            role_fit: "adjacent".into(),
            ..Default::default()
        };
        let supervisor = Person {
            title: "Production Supervisor".into(),
            ..Default::default()
        };
        let claims = vec![crate::db::EvidenceClaim {
            claim_type: "account.bounded_repetitive_task".into(),
            task_key: "task-1".into(),
            status: "observed".into(),
            source_url: "https://example.com/jobs/palletizer".into(),
            ..Default::default()
        }];
        assert!(super::wapahki_discovery_touch_block_reason(
            &opportunity,
            &adjacent,
            &supervisor,
            &claims
        )
        .is_none());

        // Corporate revenue/finance titles never receive the cold discovery
        // email; they wait for a confirmed task.
        for title in ["Chief Revenue Officer", "Vice President Finance"] {
            let executive = Person {
                title: title.into(),
                ..Default::default()
            };
            let reason = super::wapahki_discovery_touch_block_reason(
                &opportunity,
                &adjacent,
                &executive,
                &claims,
            )
            .expect("corporate title must be blocked");
            assert!(
                reason.contains("not close to the physical workflow"),
                "unexpected reason for {title}: {reason}"
            );
        }

        let no_facility = crate::db::SalesOpportunity {
            task_key: "task-1".into(),
            evidence_status: "discovery_ready".into(),
            ..Default::default()
        };
        assert!(super::wapahki_discovery_touch_block_reason(
            &no_facility,
            &adjacent,
            &supervisor,
            &claims
        )
        .is_some());

        let research = crate::db::SalesOpportunity {
            task_key: "task-1".into(),
            facility_id: "facility-1".into(),
            evidence_status: "research_required".into(),
            ..Default::default()
        };
        assert!(super::wapahki_discovery_touch_block_reason(
            &research,
            &adjacent,
            &supervisor,
            &claims
        )
        .expect("research-required stays in research")
        .contains("does not authorize outreach"));

        let fabricated_claims = vec![crate::db::EvidenceClaim {
            claim_type: "account.bounded_repetitive_task".into(),
            task_key: "task-1".into(),
            status: "observed".into(),
            source_url: "not provided in account payload".into(),
            ..Default::default()
        }];
        assert!(super::wapahki_discovery_touch_block_reason(
            &opportunity,
            &adjacent,
            &supervisor,
            &fabricated_claims
        )
        .expect("a fabricated source cannot back the discovery premise")
        .contains("URL-backed"));

        let router = crate::db::OpportunityStakeholder {
            role_fit: "router".into(),
            ..Default::default()
        };
        assert!(super::wapahki_discovery_touch_block_reason(
            &opportunity,
            &router,
            &supervisor,
            &claims
        )
        .is_some());
    }

    #[test]
    fn evidence_downgrade_quarantines_open_drafts() {
        let path =
            std::env::temp_dir().join(format!("spruce-quarantine-test-{}.sqlite", Uuid::new_v4()));
        let db = Arc::new(Db::open(&path).expect("open temp db"));
        let sequence_id = db
            .create_sequence(&crate::db::Sequence {
                id: "quarantine-sequence".into(),
                person_id: "person-q".into(),
                lead_id: "lead-q".into(),
                brand: "wapahki".into(),
                sales_opportunity_id: "opp-q".into(),
                status: "active".into(),
                ..Default::default()
            })
            .expect("sequence");
        db.insert_touch(&crate::db::Touch {
            id: "quarantine-touch".into(),
            sequence_id: sequence_id.clone(),
            person_id: "person-q".into(),
            lead_id: "lead-q".into(),
            brand: "wapahki".into(),
            stage: 1,
            status: "draft".into(),
            ..Default::default()
        })
        .expect("draft touch");
        let cancelled = db
            .quarantine_open_outreach_for_opportunity("opp-q", "evidence downgraded to research")
            .expect("quarantine");
        assert_eq!(cancelled, 1);
        let touches = db.list_touches_for_sequence(&sequence_id).expect("touches");
        assert_eq!(touches[0].status, "cancelled");
        assert!(touches[0].error.contains("evidence downgraded"));
        let sequence = db
            .sequence_gtm_attribution(&sequence_id)
            .expect("sequence query")
            .expect("sequence row");
        assert_eq!(sequence.status, "paused");
        remove_temp_db(&path);
    }

    #[test]
    fn wapahki_difficulty_separates_closing_effort_from_evidence_state() {
        let claim = |claim_type: &str, source_url: &str| crate::db::EvidenceClaim {
            claim_type: claim_type.into(),
            status: "observed".into(),
            source_url: source_url.into(),
            ..Default::default()
        };
        let workflow_stakeholder = crate::db::OpportunityStakeholder {
            role_fit: "adjacent".into(),
            ..Default::default()
        };
        let base = GtmActionContext {
            state: "action_ready".into(),
            opportunity: Some(crate::db::SalesOpportunity {
                facility_id: "facility-1".into(),
                ..Default::default()
            }),
            evidence_claims: vec![
                claim(
                    "account.bounded_repetitive_task",
                    "https://example.com/jobs/palletizer",
                ),
                claim(
                    "account.manual_task_economic_pressure",
                    "https://example.com/jobs/palletizer",
                ),
            ],
            stakeholders: vec![workflow_stakeholder.clone()],
            ..Default::default()
        };
        // Small facility, full lineage: easy.
        assert_eq!(super::wapahki_commercial_difficulty_band(&base, 180), 0);
        // Same evidence at enterprise scale: hard, regardless of fit polish.
        assert_eq!(super::wapahki_commercial_difficulty_band(&base, 12_000), 2);
        // Missing economic pressure: medium, not easy.
        let mut no_economics = base.clone();
        no_economics.evidence_claims = vec![claim(
            "account.bounded_repetitive_task",
            "https://example.com/jobs/palletizer",
        )];
        assert_eq!(
            super::wapahki_commercial_difficulty_band(&no_economics, 180),
            1
        );
        // A fabricated task source is no lineage at all: hard.
        let mut fabricated = base.clone();
        fabricated.evidence_claims = vec![
            claim(
                "account.bounded_repetitive_task",
                "not provided in account payload",
            ),
            claim(
                "account.manual_task_economic_pressure",
                "https://example.com/jobs/palletizer",
            ),
        ];
        assert_eq!(
            super::wapahki_commercial_difficulty_band(&fabricated, 180),
            2
        );
        // No facility: hard.
        let mut no_facility = base.clone();
        no_facility.opportunity = Some(crate::db::SalesOpportunity::default());
        assert_eq!(
            super::wapahki_commercial_difficulty_band(&no_facility, 180),
            2
        );
        // Evidence state stays dominant in planning order: a hard action-ready
        // account still outranks an easy discovery-ready one.
        let mut discovery = base.clone();
        discovery.state = "discovery_ready".into();
        let hard_action = super::planning_priority(&base, "wapahki", 12_000);
        let easy_discovery = super::planning_priority(&discovery, "wapahki", 180);
        assert!(hard_action < easy_discovery);
        // Other brands keep pure state ordering.
        assert_eq!(super::planning_priority(&base, "gnk", 12_000), 0);
    }
}
