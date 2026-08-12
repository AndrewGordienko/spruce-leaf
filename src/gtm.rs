//! GTM engineering: the durable layer between research and buyer-facing action.
//!
//! Agents may interpret evidence and compose a response, but SQLite owns the
//! lineage, play version, experiment assignment, and proof state. This prevents
//! the writing agent (or the sales council) from grading its own commercial
//! hypothesis and turns replies and proof results into attributable learning.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::db::{
    CustomerDevelopmentRecord, GtmExperiment, GtmPlay, Lead, Person, SharedDb, SignalDefinition,
    SignalObservation,
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
         Use this play to choose, qualify, and RANK accounts. Reject superficial industry/technology matches and enforce the declared minimum rather than silently making every catalog key mandatory. When the action policy explicitly says the exact decision may be tested, a source-backed operating footprint can earn a cautious discovery note while the decision and root cause remain questions. Otherwise require source-backed evidence of the decision and current mechanism. Always require a credible path to the bounded proof.",
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
    pub observations: Vec<SignalObservation>,
    pub matched_signal_keys: Vec<String>,
    pub experiment: Option<GtmExperiment>,
    pub experiment_assignment_id: String,
    pub experiment_arm: String,
}

impl GtmActionContext {
    pub fn action_ready(&self) -> bool {
        self.state == "action_ready"
    }

    /// Easy-priority accounts may use the full supported cadence. A
    /// medium-priority account may use one honest, hypothesis-led first touch;
    /// it cannot silently become a follow-up campaign before the problem is
    /// confirmed. Hard-priority research never reaches copy.
    pub fn sequence_ready_for(&self, touches: usize) -> bool {
        self.action_ready() || (self.state == "discovery_ready" && touches == 1)
    }

    /// Delivery is deliberately stricter than drafting. Medium-priority
    /// accounts may produce one reviewable hypothesis-led draft, but they can
    /// never become scheduled work until research promotes the account to
    /// action-ready.
    pub fn delivery_ready_for(&self, touches: usize) -> bool {
        self.action_ready() && touches > 0
    }

    /// Private context for the planner/writer. This is decision infrastructure,
    /// never prose to paste into a buyer-facing message.
    pub fn prompt_block(&self) -> String {
        let Some(play) = &self.play else {
            return "GTM ACTION STATE: no active play. Draft only a diagnostic question; do not pitch a proof or integration.".into();
        };
        let evidence = self
            .observations
            .iter()
            .map(|observation| {
                format!(
                    "- [{}; confidence {:.2}; {}] {}",
                    observation.definition_key,
                    observation.confidence,
                    observation.status,
                    observation.evidence
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
            .observations
            .iter()
            .filter(|observation| {
                matches!(observation.status.as_str(), "observed" | "verified")
                    && seen.insert(observation.definition_key.clone())
            })
            .take(4)
            .map(|observation| format!("- {}", observation.evidence))
            .collect::<Vec<_>>()
            .join("\n");
        let action = match self.state.as_str() {
            "action_ready" => "The account has enough sourced evidence for one narrow commercial note. Use only a supplied observation as the company-specific signal. Lead with a role-relevant implication and a credible point of view. A cold outcome may be a short working conversation, interest, correction, or referral; it is not yet a pilot or proof. Never invent collateral or claim an asset exists unless verified seller context explicitly supplies it.",
            "discovery_ready" => "This is a medium-priority account: research supports one concrete operating task, decision, or mechanism and this recipient is close to it, but one economic or workflow term remains unproved. Write one complete, useful first email only. State sourced account details as facts, present the exact missing term as one honest question, explain the seller's relevant contribution, and make a direct email answer the sole next step. Do not ask for a call before the missing term is confirmed. Never use a universal diagnostic template or schedule follow-ups before a reply.",
            _ => "The account does not yet have enough sourced evidence for a multi-touch sequence. Hold it for research or use one manual routing note; do not manufacture discovery questions or explain a proof, integration, pilot, or product.",
        };
        format!(
            "COPY DECISION STATE: {state}\nPERMITTED ACTION: {action}\nSOURCE-BACKED OBSERVATIONS:\n{evidence}\nTreat everything else as a question, not account reality.",
            state = self.state,
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

/// A contact-specific cold-outreach boundary. Routers are held for explicit
/// one-off routing work instead of entering a commercial cadence. Economic,
/// enterprise, and technical contacts wait until a human confirms the problem.
pub fn recipient_sequence_block_reason(
    db: &SharedDb,
    brand: &str,
    lead_id: &str,
    person: &Person,
) -> Result<Option<String>> {
    if brand.eq_ignore_ascii_case("outagehub")
        && !outagehub_workflow_contact(&person.title, &person.vantage)
    {
        return Ok(Some(
            "OutageHub requires a Service/Customer Operations, technical-support, NOC, reliability, maintenance, facilities, field-service, or charging-network operations contact"
                .into(),
        ));
    }
    if crate::response_design::is_route_only_contact(&person.title, &person.vantage) {
        return Ok(Some(
            "recipient is route-only; hold for one bounded manual routing request".into(),
        ));
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

fn outagehub_workflow_contact(title: &str, vantage: &str) -> bool {
    let title = format!(" {} ", title.trim().to_ascii_lowercase());
    let function_is_close = [
        " service operations ",
        " service operation ",
        " network operations ",
        " network operation ",
        " noc ",
        " reliability ",
        " maintenance ",
        " facilities ",
        " facility ",
        " field service ",
        " customer operations ",
        " support operations ",
        " technical support ",
        " charging operations ",
        " charging operation ",
        " charging network ",
        " charger operations ",
        " charger operation ",
    ]
    .iter()
    .any(|term| title.contains(term));
    let vantage_is_close = matches!(
        vantage.trim().to_ascii_lowercase().as_str(),
        "operator" | "process_owner" | "operational_executive"
    );
    function_is_close && vantage_is_close
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
    if let Some(reason) = recipient_sequence_block_reason(db, &playbook.key, &lead.id, person)? {
        return Ok(Some(reason));
    }
    // A live send must belong to the current policy. During initial
    // finalization the only row is still a `building` checkpoint, so absence of
    // an active sequence is valid here; every scheduling/due-work query also
    // filters by CURRENT_COPY_POLICY_VERSION.
    let mut touch_count = candidate_touches.unwrap_or_default();
    if require_current_active_sequence {
        let Some(sequence_id) = db.active_sequence_for_person(&person.id)? else {
            return Ok(Some("no active sequence is available for delivery".into()));
        };
        let current_policy = db
            .sequence_gtm_attribution(&sequence_id)?
            .is_some_and(|sequence| {
                sequence.status == "active"
                    && sequence.copy_policy_version == crate::db::CURRENT_COPY_POLICY_VERSION
            });
        if !current_policy {
            return Ok(Some(
                "sequence predates the current copy policy and must be regenerated".into(),
            ));
        }
        touch_count = db.list_touches_for_sequence(&sequence_id)?.len();
    }
    let context = prepare_action(db, &playbook.key, &lead.id, person)?;
    if !context.delivery_ready_for(touch_count) {
        return Ok(Some(format!(
            "current GTM state '{}' is not eligible for a {}-touch sequence",
            context.state, touch_count
        )));
    }
    Ok(None)
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
    // A source-backed operating context is often split across facts (for
    // example, one observation names six cold-storage facilities while another
    // names the 24/7 refrigerated service). The credibility boundary should
    // reason over that account evidence together, not demand one sentence that
    // unnaturally restates the whole thesis. Contact-vantage text is excluded.
    let account_evidence = observations
        .iter()
        .filter(|observation| observation.definition_key != "contact.workflow_vantage")
        .map(|observation| observation.evidence.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    observations.retain(|observation| {
        observation.definition_key == "contact.workflow_vantage"
            || credible_action_signal(
                brand,
                &observation.definition_key,
                &format!("{} {account_evidence}", observation.evidence),
            )
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
    let has_reachable_channel = person.email_status.eq_ignore_ascii_case("verified")
        || !person.linkedin_url.trim().is_empty();
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
    let state = play.as_ref().map_or("no_play", |play| {
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
        observations,
        matched_signal_keys,
        experiment,
        experiment_assignment_id,
        experiment_arm,
    })
}

fn mandatory_action_signals_present(brand: &str, matched: &[String]) -> bool {
    let required: &[&str] = if brand.eq_ignore_ascii_case("outagehub") {
        &[
            "account.fit_evidence",
            "account.distributed_locations",
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
        signal_definition("outagehub", "account.outage_sensitive_decision", "Outage-sensitive decision", "The account operates a time-sensitive decision in which external grid status or restoration context could change dispatch, escalation, hold, transfer, or communication."),
        signal_definition("outagehub", "account.distributed_locations", "Distributed locations", "The account operates multiple or remote locations across utility territories, making location matching operationally relevant."),
        signal_definition("outagehub", "account.operated_ev_charging_network", "Operated EV charging network", "First-party evidence shows the account operates, manages, or monitors a Canadian EV-charging site network rather than merely selling or installing charging equipment."),
        signal_definition("outagehub", "account.historical_location_outage_match", "Historical location-outage match", "A completed account-specific analysis names a charging location and timestamp that overlapped a reported utility outage area. A proposed or hypothetical match does not count."),
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
            version: 12,
            name: "Distributed-site outage decision".into(),
            lifecycle: "testing".into(),
            motion: "internal_pipeline_to_forward_deployed_proof".into(),
            target_icp: "Canadian operators with distributed, outage-sensitive locations or assets: charging, telecom, cold storage, data centres, multi-site facilities, service dispatch, backup power, and adjacent infrastructure. Prioritize visible decisions and reachable owners; do not require one vertical.".into(),
            target_vantages: vec!["process_owner".into(), "operator".into(), "technical_evaluator".into()],
            required_signal_keys: vec!["account.fit_evidence".into(), "account.distributed_locations".into(), "account.outage_sensitive_decision".into(), "account.historical_location_outage_match".into()],
            minimum_signal_matches: 4,
            hypothesis: "During an ambiguous location or asset incident, location-matched public utility context may improve one evidenced diagnosis, dispatch, escalation, continuity, prioritization, or communication decision.".into(),
            action_policy: "Work easy to hard. Easy accounts have a visible outage-time decision, reachable owner, and completed historical match. Medium accounts have the decision and owner but no completed match and may receive one honest first touch only. Explain OutageHub's API contribution and offer a short conversation or email answer; never claim private site status.".into(),
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
            version: 5,
            name: "Workflow-specific decision support".into(),
            lifecycle: "testing".into(),
            motion: "internal_pipeline_to_forward_deployed_proof".into(),
            target_icp: "Organizations with a source-backed recurring decision, exception, coordination burden, or workflow-specific software gap. Cover industries broadly; rank visible consequences and reachable owners first, then investigate medium and hard accounts.".into(),
            target_vantages: vec!["process_owner".into(), "operator".into(), "operational_executive".into(), "router".into()],
            required_signal_keys: vec!["account.fit_evidence".into(), "account.specific_recurring_decision".into(), "account.believable_operating_consequence".into(), "account.external_trigger_or_mechanism_evidence".into()],
            minimum_signal_matches: 4,
            hypothesis: "A source-backed trigger creates a recurring exception decision whose inputs, coordination, or existing system boundary produces a meaningful operating consequence.".into(),
            action_policy: "Work easy to hard. Easy accounts support the recurring event, decision, consequence, mechanism, and owner. Medium accounts support the decision or mechanism and owner but leave one term open; allow one hypothesis-led first email only. Explain one concrete GnK contribution and offer a short conversation or email response path.".into(),
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
            version: 6,
            name: "Task-and-exception feasibility review".into(),
            lifecycle: "testing".into(),
            motion: "internal_pipeline_to_forward_deployed_proof".into(),
            target_icp: "Factories, product manufacturers, warehouses, distribution centres, and fulfillment operations, starting in Ontario then Canada. Rank one physical task and the nearest operator; prioritize sourced economic pressure but retain task-specific medium accounts.".into(),
            target_vantages: vec!["process_owner".into(), "operator".into(), "technical_evaluator".into(), "router".into()],
            required_signal_keys: vec!["account.fit_evidence".into(), "account.bounded_repetitive_task".into(), "account.manual_task_economic_pressure".into()],
            minimum_signal_matches: 3,
            hypothesis: "One recurring physical movement may be automatable enough to investigate, with the exact variation, rate, integration, and economics confirmed by the operator closest to the work.".into(),
            action_policy: "Work easy to hard. Easy accounts have a source-supported task, economic pressure, and reachable owner. Medium accounts have a task and owner but unproved economics; allow one honest task-specific first email. Briefly give Andrew's University of Toronto and Automata context, explain Wapahki's contribution, and offer a short conversation or email answer. Never ask the recipient to find a use case from scratch.".into(),
            proof_type: "task_feasibility_review".into(),
            proof_description: "Review a task sketch, short video, or representative SKU/changeover set; model the normal motion, exceptions, rate, and technical boundaries.".into(),
            success_metric: "Required rate, intervention frequency, changeover burden, task coverage, and a clear technical/economic stop condition.".into(),
            kill_condition: "Variation, sanitation, damage risk, rate, or constant human intervention makes a bounded cell technically or economically unattractive.".into(),
            source_refs: vec!["playbooks/wapahki.toml".into(), "businesses/wapahki.toml".into(), "youtube:wNnU2BJILPA@509".into()],
            ..Default::default()
        },
    ]
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
    db.backfill_legacy_signal_observations()?;
    db.backfill_sequence_gtm_attribution()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        customer_development_missing, customer_development_stage, default_signal_definitions,
        outagehub_workflow_contact, prepare_action, seed_defaults, GtmActionContext,
        SignalCandidate,
    };
    use crate::db::{
        AccountPlayAssessment, CustomerDevelopmentRecord, Db, Lead, Person, SharedDb,
        SignalObservation,
    };
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn discovery_can_be_drafted_once_but_never_delivered() {
        let discovery = GtmActionContext {
            state: "discovery_ready".into(),
            ..Default::default()
        };
        assert!(discovery.sequence_ready_for(1));
        assert!(!discovery.sequence_ready_for(2));
        assert!(!discovery.delivery_ready_for(1));

        let ready = GtmActionContext {
            state: "action_ready".into(),
            ..Default::default()
        };
        assert!(ready.sequence_ready_for(1));
        assert!(ready.delivery_ready_for(1));
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
        assert!(outagehub_workflow_contact(
            "Director of Charging Network Operations",
            "operational_executive"
        ));
        assert!(outagehub_workflow_contact(
            "Maintenance Manager",
            "process_owner"
        ));
        assert!(outagehub_workflow_contact(
            "Senior Manager, Customer Operations",
            "process_owner"
        ));
        assert!(!outagehub_workflow_contact(
            "Director of Customer Success",
            "process_owner"
        ));
        assert!(!outagehub_workflow_contact(
            "Operational Excellence EPMO",
            "operational_executive"
        ));
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
    fn selected_contact_vantage_satisfies_reachable_owner_requirement() {
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
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.specific_recurring_decision".into(),
                    evidence: "After every disputed rejection, claims decides whether to pursue recovery or write it off.".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.believable_operating_consequence".into(),
                    evidence: "The review affects recoveries, settlement time, and senior reviewer capacity.".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.external_trigger_or_mechanism_evidence".into(),
                    evidence: "A public claims guide requires policy, investigation, and prior-action records before escalation.".into(),
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

        assert!(context
            .matched_signal_keys
            .contains(&"account.reachable_workflow_owner".to_string()));
        assert!(context.action_ready());

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
    fn operated_ev_network_with_historical_match_is_action_ready() {
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
                ..Default::default()
            })
            .expect("lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                apollo_person_id: "person-maintenance-owner".into(),
                name: "Morgan Engineer".into(),
                title: "Director, Network Operations".into(),
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
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.distributed_locations".into(),
                    evidence: "The operator runs multiple charging sites across Canada.".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.outage_sensitive_decision".into(),
                    evidence: "Service Operations checks utility status before dispatch or customer communication when a charging-site availability incident occurs.".into(),
                    confidence: 0.9,
                    ..Default::default()
                },
                SignalCandidate {
                    definition_key: "account.historical_location_outage_match".into(),
                    evidence: "On 2026-07-14 at 14:30, the charging site at 123 King Street overlapped a utility outage area in a utility report.".into(),
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
                "account.outage_sensitive_decision",
                "account.historical_location_outage_match",
            ],
        );
        let person = db.get_person(&person_id).unwrap().unwrap();
        let context = prepare_action(&db, "outagehub", &lead_id, &person).expect("context");
        assert!(context
            .matched_signal_keys
            .contains(&"account.historical_location_outage_match".to_string()));
        assert!(context.action_ready());
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
            observations: vec![SignalObservation {
                evidence: "Official site names five production formats.".into(),
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
}
