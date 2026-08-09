//! GTM engineering: the durable layer between research and buyer-facing action.
//!
//! Agents may interpret evidence and compose a response, but SQLite owns the
//! lineage, play version, experiment assignment, and proof state. This prevents
//! the writing agent (or the sales council) from grading its own commercial
//! hypothesis and turns replies and proof results into attributable learning.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::db::{
    CustomerDevelopmentRecord, GtmExperiment, GtmPlay, Person, SharedDb, SignalDefinition,
    SignalObservation,
};

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
         Name: {} v{}\nTarget ICP: {}\nHypothesis: {}\nRequired signal catalog keys: {} (minimum {} matches)\n\
         Action policy: {}\nProof we can actually deliver: {}\nSuccess metric: {}\nKill condition: {}\n\
         Use this play to choose, qualify, and RANK accounts. Reject superficial industry/technology matches. A high-fit account needs source-backed evidence of the operational decision, a defensible root cause for the current ambiguity or manual workaround, a reachable person who can observe it, and a credible path to this bounded proof.",
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

    /// A multi-touch sequence is a commercial action, not a research artifact.
    /// Only a fully supported action may enter the copywriter. `discovery_ready`
    /// is retained for research/routing UX, but must never be upgraded into a
    /// sequence merely because a plausible title was found.
    /// Discovery-ready context may support one manually reviewed routing note,
    /// but never an automated no-reply sequence.
    pub fn sequence_ready_for(&self, touches: usize) -> bool {
        self.action_ready() || (touches == 1 && self.state == "discovery_ready")
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
            "discovery_ready" => "The company fit and this recipient's operating vantage are sourced, but the internal workflow problem is not public evidence. At most, write one manual hypothesis-safe routing note. State only supplied company facts. Never claim that work is manual, expensive, recurring, fragmented, or cross-system. Never propose a proof, pilot, integration, or product evaluation.",
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

/// Resolve the current versioned play, live evidence, and stable experiment arm
/// before an agent plans copy. An experiment assignment is durable: re-drafting
/// the same person cannot silently move them between control and variant.
pub fn prepare_action(
    db: &SharedDb,
    brand: &str,
    lead_id: &str,
    person: &Person,
) -> Result<GtmActionContext> {
    db.expire_signal_observations()?;
    let play = db.current_gtm_play(brand)?;
    let observations = db.list_active_signal_observations(Some(brand), Some(lead_id), None)?;
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
    let has_workflow_vantage = matches!(
        person.vantage.trim().to_ascii_lowercase().as_str(),
        "process_owner"
            | "operator"
            | "operational_executive"
            | "economic_buyer"
            | "technical_evaluator"
    );
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

    let state = play.as_ref().map_or("no_play", |play| {
        let matched = play
            .required_signal_keys
            .iter()
            .filter(|key| matched_signal_keys.contains(key))
            .count();
        if matched >= play.minimum_signal_matches.max(1) as usize {
            "action_ready"
        } else if brand == "gnk"
            && matched_signal_keys.contains(&"account.fit_evidence".to_string())
            && matched_signal_keys.contains(&"account.reachable_workflow_owner".to_string())
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
        signal_definition("outagehub", "account.existing_operational_system", "Existing operational system", "The account names or evidences an existing NOC, dispatch, CMMS, SCADA, ServiceNow, Salesforce, or equivalent workflow surface."),
        signal_definition("gnk", "account.expensive_recurring_workflow", "Expensive recurring workflow", "The account evidences a recurring decision, exception, investigation, delay, or handoff with material operational consequences."),
        signal_definition("gnk", "account.cross_system_reconciliation", "Cross-system reconciliation", "People appear to reconcile records, evidence, or decisions across systems that do not supply the required coordination layer."),
        signal_definition("gnk", "account.reachable_workflow_owner", "Reachable workflow owner", "A mid-market workflow owner or close observer is identifiable and reachable for a founder-led diagnostic conversation."),
        signal_definition("wapahki", "account.bounded_repetitive_task", "Bounded repetitive task", "A specific physical motion or handoff repeats enough within a production run to be described and measured."),
        signal_definition("wapahki", "account.format_variability", "Format variability", "SKU, case, orientation, changeover, or presentation variability plausibly defeats conventional fixed automation."),
        signal_definition("wapahki", "account.exception_heavy_manual_work", "Manual exception handling", "Operators remain in the task because misfeeds, damage, sanitation, changeovers, or other exceptions require judgment or dexterity."),
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
            key: "historical_outage_replay".into(),
            version: 3,
            name: "Historical location-matched outage replay".into(),
            lifecycle: "testing".into(),
            motion: "internal_pipeline_to_forward_deployed_proof".into(),
            target_icp: "Operators with a source-backed distributed footprint and an outage-sensitive, location-specific operating decision. An internal NOC, dispatch, or software surface strengthens fit but need not be publicly documented before a diagnostic note.".into(),
            target_vantages: vec!["process_owner".into(), "operator".into(), "technical_evaluator".into(), "router".into()],
            required_signal_keys: vec!["account.fit_evidence".into(), "account.outage_sensitive_decision".into(), "account.distributed_locations".into()],
            minimum_signal_matches: 3,
            hypothesis: "Location-matched public utility context changes a live classification, dispatch, escalation, hold, transfer, or communication decision.".into(),
            action_policy: "Earn a correction about the current decision path. If the problem is confirmed, offer a small historical event review; do not lead with an API, webhook, dashboard, or pilot.".into(),
            proof_type: "historical_replay".into(),
            proof_description: "Replay a few known incidents by matching affected locations to public utility events and compare when useful external context became available.".into(),
            success_metric: "Time to correct classification and the number of historical decisions that would have changed before a manual lookup or site call.".into(),
            kill_condition: "Reliable utility context already arrives before the decision, or the matched context would not change any action.".into(),
            source_refs: vec!["playbooks/outagehub.toml".into(), "youtube:wNnU2BJILPA@509".into()],
            ..Default::default()
        },
        GtmPlay {
            brand: "gnk".into(),
            key: "closed_case_reconstruction".into(),
            version: 2,
            name: "Closed-case workflow reconstruction".into(),
            lifecycle: "testing".into(),
            motion: "internal_pipeline_to_forward_deployed_proof".into(),
            target_icp: "Reachable mid-market workflow owners dealing with costly recurring exceptions, investigations, or reconciliation across existing systems.".into(),
            target_vantages: vec!["process_owner".into(), "operator".into(), "operational_executive".into(), "router".into()],
            required_signal_keys: vec!["account.fit_evidence".into(), "account.expensive_recurring_workflow".into(), "account.cross_system_reconciliation".into(), "account.reachable_workflow_owner".into()],
            minimum_signal_matches: 3,
            hypothesis: "A missing decision, coordination, or evidence-reconstruction layer causes repeated handling time, delay, risk, or constrained capacity.".into(),
            action_policy: "Open on one concrete workflow and earn a correction. After confirmation, test a small set of closed cases before proposing a bounded paid build.".into(),
            proof_type: "closed_case_reconstruction".into(),
            proof_description: "Reconstruct a small sample of closed cases using the proposed decision or evidence workflow and compare it with the current process.".into(),
            success_metric: "Handling time, evidence completeness, exceptions resolved, and decisions reached without manual cross-system reconstruction.".into(),
            kill_condition: "The workflow is not frequent or consequential, is already handled well, or no bounded data sample can demonstrate a better result.".into(),
            source_refs: vec!["playbooks/gnk.toml".into(), "youtube:wNnU2BJILPA@509".into()],
            ..Default::default()
        },
        GtmPlay {
            brand: "wapahki".into(),
            key: "task_exception_review".into(),
            version: 3,
            name: "Task-and-exception feasibility review".into(),
            lifecycle: "testing".into(),
            motion: "internal_pipeline_to_forward_deployed_proof".into(),
            target_icp: "Food manufacturers with one bounded repetitive task whose SKU, format, or exception variability keeps it manual.".into(),
            target_vantages: vec!["process_owner".into(), "operator".into(), "technical_evaluator".into(), "router".into()],
            required_signal_keys: vec!["account.fit_evidence".into(), "account.bounded_repetitive_task".into(), "account.format_variability".into(), "account.exception_heavy_manual_work".into()],
            minimum_signal_matches: 3,
            hypothesis: "The normal motion is repeatable enough for a flexible cell while people can retain a bounded set of exceptions that make fixed automation uneconomic.".into(),
            action_policy: "In cold outreach, earn only a correction, referral, or first-hand description of one named task, what changes between runs, and which exceptions interrupt it. After a human reply, read the account's customer-development record and ask for only its next missing commitment. Never infer validation from a send, compliment, or meeting; never ask a non-responsive cold prospect for a pilot or LOI.".into(),
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
        db.insert_gtm_play_if_absent(&play)?;
        db.retire_older_unproven_gtm_play_versions(&play.brand, &play.key, play.version)?;
    }
    db.backfill_legacy_signal_observations()?;
    db.backfill_sequence_gtm_attribution()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        customer_development_missing, customer_development_stage, default_signal_definitions,
        prepare_action, seed_defaults, GtmActionContext, SignalCandidate,
    };
    use crate::db::{CustomerDevelopmentRecord, Db, Lead, Person, SignalObservation};
    use std::sync::Arc;
    use uuid::Uuid;

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
                    definition_key: "account.expensive_recurring_workflow".into(),
                    evidence: "Adjusters manually review every exception, adding handling time."
                        .into(),
                    confidence: 0.9,
                    ..Default::default()
                },
            ],
            "test",
        )
        .expect("signals");
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
        let discovery_person = db.get_person(&discovery_person_id).unwrap().unwrap();
        let discovery = prepare_action(&db, "gnk", &discovery_lead_id, &discovery_person)
            .expect("discovery context");
        assert_eq!(discovery.state, "discovery_ready");
        assert!(discovery.sequence_ready_for(1));
        assert!(!discovery.sequence_ready_for(2));
        assert!(!discovery.action_ready());
        assert!(discovery
            .copy_prompt_block()
            .contains("Never claim that work is manual"));
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
