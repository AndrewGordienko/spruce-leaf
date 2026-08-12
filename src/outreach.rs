//! Outreach planning: write a sequence for each verified person and schedule its
//! touches on the timeline the cadence engine drives.
//!
//! Sourcing + enrichment give us a real, verified person and a doctrine-framed
//! lead. This turns that into an actual multi-touch sequence: the configured AI
//! backend writes the copy (grounded in the lead's real facts + the person's
//! vantage), we run the
//! mechanical forbidden-phrase/length lint over it, then persist a `sequence`
//! plus its `touches` with `due_at` computed from each touch's day offset.
//!
//! Email touches become `scheduled` (in auto mode) or `draft` (approval mode);
//! LinkedIn requests and connected DMs stay manual. Conditional LinkedIn/email
//! touches use the CRM's operator-maintained connection state and fall back to
//! email when the prospect is not marked connected.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::business::BusinessProfile;
use crate::calendar::{self, TimingContext};
use crate::db::{Sequence, SharedDb, Touch, CURRENT_COPY_POLICY_VERSION};
use crate::domain::{
    Account as CopyAccount, Contact as CopyContact, Sequence as CopySequence, Touch as CopyTouch,
    TouchReview,
};
use crate::engine::Engine;
use crate::gtm::GtmActionContext;
use crate::knowledge::Library;
use crate::playbook::{self, Playbook, SalesCriticPersona, Shared};
use crate::response_design;

#[derive(Debug, Default)]
pub struct PlanSummary {
    pub people_planned: usize,
    /// Accounts for which this run produced a current reviewed sequence.
    pub planned_lead_ids: Vec<String>,
    pub touches_scheduled: usize,
    pub touches_drafted: usize,
    pub sequences_replaced: usize,
    pub people_rejected: usize,
    pub people_held: usize,
    pub people_stopped: usize,
    pub stopped_reason: Option<String>,
}

#[derive(Debug, Default)]
pub struct ApprovalSummary {
    pub touches_scheduled: usize,
    pub people_held: usize,
    pub hold_reasons: Vec<String>,
}

fn log_outreach(message: impl AsRef<str>) {
    if !crate::ui::fancy() {
        eprintln!("  · {}", message.as_ref());
    }
}

/// Promote reviewed drafts only after re-evaluating the current account,
/// recipient, and play. Copy approval is never a substitute for GTM readiness.
pub fn approve_ready_touches(
    db: &SharedDb,
    pb: &Playbook,
    person_id: Option<&str>,
) -> Result<ApprovalSummary> {
    let mut summary = ApprovalSummary::default();
    for person in db
        .list_people(Some(&pb.key), None)?
        .into_iter()
        .filter(|person| person_id.is_none_or(|id| person.id == id))
    {
        if db.reviewed_draft_touch_count(Some(&pb.key), Some(&person.id))? == 0 {
            continue;
        }
        let reason = match db.get_lead(&person.lead_id)? {
            Some(lead) => crate::gtm::delivery_block_reason(db, pb, &lead, &person)?,
            None => Some("account record is missing".into()),
        };
        if let Some(reason) = reason {
            summary.people_held += 1;
            summary
                .hold_reasons
                .push(format!("{}: {}", person.name, reason));
            let _ = db.log_event(&pb.key, &person.id, "", "approval_held", &reason);
            continue;
        }
        summary.touches_scheduled +=
            db.schedule_reviewed_touches(Some(&pb.key), Some(&person.id))?;
    }
    Ok(summary)
}

/// One recipient shown in the live outreach progress tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanProgressRecipient {
    pub key: String,
    pub name: String,
    pub account: String,
}

/// Structured live status for the terminal and any future UI consumer. Keeping
/// this typed avoids parsing a long spinner string back into recipient state.
#[derive(Debug, Clone)]
pub struct PlanProgressUpdate {
    pub phase: String,
    pub account: String,
    pub recipient_keys: Vec<String>,
    /// overall | active | accepted | rejected | stopped
    pub state: String,
    pub processed: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub held: usize,
    pub stopped: usize,
    pub total: usize,
    pub roster: Vec<PlanProgressRecipient>,
}

/// Live status updates for the interactive outreach planner. The callback owns
/// its sink so it can safely follow concurrently drafted account batches.
pub type PlanProgressReporter = Arc<dyn Fn(PlanProgressUpdate) + Send + Sync>;

#[derive(Clone)]
struct PlanProgress {
    reporter: Option<PlanProgressReporter>,
    processed: Arc<AtomicUsize>,
    accepted: Arc<AtomicUsize>,
    rejected: Arc<AtomicUsize>,
    held: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
    total: usize,
    roster: Arc<Vec<PlanProgressRecipient>>,
}

impl PlanProgress {
    fn new(reporter: Option<PlanProgressReporter>, roster: Vec<PlanProgressRecipient>) -> Self {
        let progress = Self {
            reporter,
            processed: Arc::new(AtomicUsize::new(0)),
            accepted: Arc::new(AtomicUsize::new(0)),
            rejected: Arc::new(AtomicUsize::new(0)),
            held: Arc::new(AtomicUsize::new(0)),
            stopped: Arc::new(AtomicUsize::new(0)),
            total: roster.len(),
            roster: Arc::new(roster),
        };
        progress.overall("queued for account drafting and copy QA");
        progress
    }

    fn overall(&self, phase: &str) {
        self.emit(phase, "", Vec::new(), "overall");
    }

    fn group(&self, phase: &str, account: &str, people: &[crate::db::Person]) {
        self.emit(
            phase,
            account,
            people.iter().map(|person| person.id.clone()).collect(),
            "active",
        );
    }

    fn person(&self, phase: &str, account: &str, person: &crate::db::Person) {
        self.emit(phase, account, vec![person.id.clone()], "active");
    }

    fn finish_person(&self, account: &str, person: &crate::db::Person, accepted: bool) {
        self.finish_person_as(
            account,
            person,
            if accepted {
                "ready in CRM"
            } else {
                "rejected; feedback saved"
            },
            if accepted { "accepted" } else { "rejected" },
        );
    }

    fn stop_person(&self, account: &str, person: &crate::db::Person, reason: &str) {
        self.finish_person_as(account, person, reason, "stopped");
    }

    fn finish_person_as(
        &self,
        account: &str,
        person: &crate::db::Person,
        phase: &str,
        state: &str,
    ) {
        let processed = self
            .processed
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
            .min(self.total);
        match state {
            "accepted" => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
            }
            "stopped" => {
                self.stopped.fetch_add(1, Ordering::Relaxed);
            }
            "held" => {
                self.held.fetch_add(1, Ordering::Relaxed);
            }
            "rejected" => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        self.emit_at(phase, account, vec![person.id.clone()], state, processed);
    }

    fn emit(&self, phase: &str, account: &str, recipient_keys: Vec<String>, state: &str) {
        let processed = self.processed.load(Ordering::Relaxed).min(self.total);
        self.emit_at(phase, account, recipient_keys, state, processed);
    }

    fn emit_at(
        &self,
        phase: &str,
        account: &str,
        recipient_keys: Vec<String>,
        state: &str,
        processed: usize,
    ) {
        if let Some(reporter) = &self.reporter {
            reporter(PlanProgressUpdate {
                phase: phase.to_string(),
                account: account.to_string(),
                recipient_keys,
                state: state.to_string(),
                processed,
                accepted: self.accepted.load(Ordering::Relaxed).min(self.total),
                rejected: self.rejected.load(Ordering::Relaxed).min(self.total),
                held: self.held.load(Ordering::Relaxed).min(self.total),
                stopped: self.stopped.load(Ordering::Relaxed).min(self.total),
                total: self.total,
                roster: self.roster.as_ref().clone(),
            });
        }
    }
}

#[cfg(test)]
fn format_progress_status(update: &PlanProgressUpdate) -> String {
    let recipients = update
        .roster
        .iter()
        .filter(|recipient| update.recipient_keys.contains(&recipient.key))
        .map(|recipient| recipient.name.as_str())
        .collect::<Vec<_>>()
        .join(" + ");
    let mut parts = vec!["Drafting outreach".to_string(), update.phase.clone()];
    if !update.account.trim().is_empty() {
        parts.push(update.account.clone());
    }
    if !recipients.is_empty() {
        parts.push(recipients);
    }
    parts.push(format!("{}/{} complete", update.processed, update.total));
    parts.join(" · ")
}

fn report_review_progress(progress: Option<&(dyn Fn(&str) + Send + Sync)>, phase: impl AsRef<str>) {
    if let Some(progress) = progress {
        progress(phase.as_ref());
    }
}

struct ReviewedCopy {
    sequence: CopySequence,
    reviews: Vec<TouchReview>,
}

struct FinalizedDraft {
    lead_id: String,
    person_id: String,
    touches_scheduled: usize,
    touches_drafted: usize,
    sequence_replaced: bool,
}

struct CopyFailure {
    reason: String,
    provider_stopped: bool,
    held: bool,
}

struct AccountCopyResult {
    copies: HashMap<String, ReviewedCopy>,
    failures: HashMap<String, CopyFailure>,
    stopped_reason: Option<String>,
}

struct RoleKnowledge {
    block: String,
    /// Every ID this role is allowed to cite.
    allowed_ids: Vec<String>,
}

struct OutreachKnowledge {
    planner: RoleKnowledge,
    writer: RoleKnowledge,
    reviewer: RoleKnowledge,
    council: RoleKnowledge,
}

fn retrieve_outreach_knowledge(
    library: &Library,
    shared: &Shared,
    lead: Option<&crate::db::Lead>,
) -> OutreachKnowledge {
    let account = lead
        .map(|lead| {
            format!(
                "{} {} {} {} {}",
                lead.industry,
                lead.observed_facts
                    .iter()
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" "),
                lead.hypothesis,
                lead.mechanism,
                lead.hard_buyer_question
            )
        })
        .unwrap_or_default();
    let council_personas = shared
        .personas
        .critics
        .iter()
        .map(|critic| critic.prompt.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    OutreachKnowledge {
        planner: retrieve_role_knowledge_with_limits(
            library,
            &shared.personas.planner,
            &account,
            5,
            2,
        ),
        // Retrieval should sharpen judgment, not flood the writer with a book
        // report. Four principles and one short passage are enough to influence
        // the draft without competing with the actual account evidence.
        writer: retrieve_role_knowledge_with_limits(
            library,
            &shared.personas.writer,
            &account,
            4,
            1,
        ),
        reviewer: retrieve_role_knowledge_with_limits(
            library,
            &shared.personas.reviewer,
            &account,
            3,
            0,
        ),
        council: retrieve_role_knowledge_with_limits(library, &council_personas, &account, 6, 2),
    }
}

fn retrieve_role_knowledge_with_limits(
    library: &Library,
    persona: &str,
    account: &str,
    principles: usize,
    passages: usize,
) -> RoleKnowledge {
    let retrieved = library.retrieve_stage(
        &format!("{persona}\n{account}"),
        "sequence",
        principles,
        passages,
    );
    let allowed_ids = retrieved
        .principles
        .iter()
        .map(|principle| principle.id.clone())
        .collect::<Vec<_>>();
    RoleKnowledge {
        // A small relevant retrieval is useful. The entire business-book
        // corpus on every writer and verifier call drowned out the account
        // evidence and rewarded polished sameness.
        block: retrieved.playbook_block(),
        allowed_ids,
    }
}

#[derive(Debug, Deserialize)]
struct BatchCopy {
    #[serde(default)]
    sequences: Vec<PersonSequenceCopy>,
}

#[derive(Debug, Deserialize)]
struct PersonSequenceCopy {
    person_key: String,
    /// send | hold_for_research. Abstention is a successful safety decision,
    /// not malformed model output.
    #[serde(default)]
    send_decision: String,
    #[serde(default)]
    decision_reason: String,
    /// Private pre-copy definition of the exact operating moment and decision
    /// the sequence stays anchored to.
    #[serde(default)]
    operating_decision: String,
    /// Private, explicitly unverified explanation of why that decision could
    /// be difficult. This supplies specificity without turning a guess into an
    /// account claim.
    #[serde(default)]
    mechanism_to_test: String,
    /// The strongest credible reason this recipient would dismiss the premise.
    #[serde(default)]
    hard_buyer_objection: String,
    /// Private pre-copy decision: why replying helps this recipient, not why
    /// Andrew wants research. Requiring the writer to name it prevents a clean
    /// multi-message questionnaire from masquerading as a sales sequence.
    #[serde(default)]
    recipient_reply_reason: String,
    /// The concrete value Andrew gives before asking for meaningful time.
    #[serde(default)]
    value_exchange: String,
    #[serde(default)]
    touches: Vec<CopyTouch>,
    #[serde(default)]
    applied_principles: Vec<String>,
}

/// The strategy the agent reasons out for one recipient BEFORE any copy is
/// written — what each touch should achieve and how, so the writer executes a
/// deliberate plan rather than improvising a batch of messages in one shot.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SequencePlan {
    #[serde(default)]
    overall_strategy: String,
    /// Private response design: what Andrew needs from this particular person
    /// and why that small action could make sense from their side of the inbox.
    #[serde(default)]
    response_strategy: ResponseStrategy,
    #[serde(default)]
    touches: Vec<TouchPlan>,
    #[serde(default)]
    applied_principles: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ResponseStrategy {
    #[serde(default)]
    desired_response: String,
    #[serde(default)]
    role_relevant_motive: String,
    #[serde(default)]
    concrete_scene: String,
    #[serde(default)]
    credibility_basis: String,
    #[serde(default)]
    smallest_commitment: String,
    #[serde(default)]
    reactance_guard: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TouchPlan {
    #[serde(default)]
    stage: usize,
    #[serde(default)]
    channel: String,
    /// What this touch should achieve.
    #[serde(default)]
    objective: String,
    /// How this stage advances the same thread. This may be a clarification,
    /// useful contribution, route, or close; it need not invent a new topic.
    #[serde(default)]
    angle: String,
    /// The single clear ask; empty for the final close.
    #[serde(default)]
    ask: String,
}

#[derive(Debug, Deserialize)]
struct EditDoc {
    /// Independent verdict over the sequence as a whole. Per-touch scores alone
    /// were approving locally grammatical messages that formed one
    /// repetitive, recipient-hostile campaign.
    #[serde(default)]
    sequence_passes: bool,
    #[serde(default)]
    sequence_score: u32,
    #[serde(default)]
    sequence_issues: Vec<String>,
    #[serde(default)]
    reviews: Vec<EditReview>,
}

#[derive(Debug, Deserialize)]
struct EditReview {
    stage: u32,
    passes: bool,
    #[serde(default)]
    score: u32,
    #[serde(default)]
    issues: Vec<String>,
    #[serde(default)]
    revised_subject: String,
    #[serde(default)]
    revised_body: String,
}

#[derive(Debug, Deserialize)]
struct CouncilDoc {
    #[serde(default)]
    critics: Vec<CouncilCriticReview>,
}

#[derive(Debug, Deserialize)]
struct CouncilCriticReview {
    critic_id: String,
    #[serde(default)]
    touches: Vec<CouncilTouchReview>,
}

#[derive(Debug, Deserialize)]
struct CouncilTouchReview {
    stage: u32,
    passes: bool,
    #[serde(default)]
    score: u32,
    #[serde(default)]
    issues: Vec<String>,
    #[serde(default)]
    recommendation: String,
}

fn provisional_channel(stage: usize) -> &'static str {
    const SEVEN: [&str; 7] = [
        "email",
        "email",
        "linkedin_request",
        "email",
        "linkedin_or_email",
        "email",
        "linkedin_or_email",
    ];
    SEVEN
        .get(stage.saturating_sub(1))
        .copied()
        .unwrap_or("email")
}

fn provisional_day_offset(stage: usize, total: usize) -> i64 {
    const SEVEN_DAYS: [i64; 7] = [0, 3, 5, 9, 13, 17, 21];
    const FOUR_DAYS: [i64; 4] = [0, 3, 7, 14];
    const TWO_DAYS: [i64; 2] = [0, 6];
    if total == 7 {
        SEVEN_DAYS
            .get(stage.saturating_sub(1))
            .copied()
            .unwrap_or(21)
    } else if total == 4 {
        FOUR_DAYS
            .get(stage.saturating_sub(1))
            .copied()
            .unwrap_or(14)
    } else if total == 2 {
        TWO_DAYS.get(stage.saturating_sub(1)).copied().unwrap_or(6)
    } else if total <= 1 {
        0
    } else {
        ((stage.saturating_sub(1) * 21) / (total - 1)) as i64
    }
}

fn is_email_capable_channel(channel: &str) -> bool {
    matches!(
        channel.trim().to_ascii_lowercase().as_str(),
        "email" | "linkedin_or_email"
    )
}

#[allow(clippy::too_many_arguments)]
fn create_building_checkpoint(
    db: &SharedDb,
    pb: &Playbook,
    lead: &crate::db::Lead,
    person: &crate::db::Person,
    gtm_context: &GtmActionContext,
    touches: usize,
    generation_backend: &str,
    generation_model: &str,
) -> Result<String> {
    db.interrupt_prior_building_sequences(&person.id)?;
    let sequence_id = db.create_sequence(&Sequence {
        person_id: person.id.clone(),
        lead_id: lead.id.clone(),
        brand: pb.key.clone(),
        thesis: lead.thesis.clone(),
        play_id: gtm_context
            .play
            .as_ref()
            .map(|play| play.id.clone())
            .unwrap_or_default(),
        play_version: gtm_context
            .play
            .as_ref()
            .map(|play| play.version)
            .unwrap_or_default(),
        experiment_id: gtm_context
            .experiment
            .as_ref()
            .map(|experiment| experiment.id.clone())
            .unwrap_or_default(),
        experiment_arm: gtm_context.experiment_arm.clone(),
        experiment_assignment_id: gtm_context.experiment_assignment_id.clone(),
        signal_observation_ids: gtm_context
            .observations
            .iter()
            .map(|observation| observation.id.clone())
            .collect(),
        gtm_state: gtm_context.state.clone(),
        generation_backend: generation_backend.into(),
        generation_model: generation_model.into(),
        status: "building".into(),
        ..Default::default()
    })?;
    for stage in 1..=touches {
        if let Err(error) = db.insert_touch(&Touch {
            sequence_id: sequence_id.clone(),
            person_id: person.id.clone(),
            lead_id: lead.id.clone(),
            brand: pb.key.clone(),
            stage: stage as i64,
            day_offset: provisional_day_offset(stage, touches),
            channel: provisional_channel(stage).into(),
            body: "Writing draft…".into(),
            status: "writing".into(),
            review_issues: vec!["Generation in progress".into()],
            ..Default::default()
        }) {
            let _ = db.reject_building_sequence(&sequence_id, &error.to_string());
            return Err(error);
        }
    }
    Ok(sequence_id)
}

fn close_building_checkpoint(
    db: &SharedDb,
    sequence_id: &str,
    reason: &str,
    provider_stopped: bool,
) {
    if provider_stopped {
        let _ = db.stop_building_sequence(sequence_id, reason);
    } else {
        let _ = db.reject_building_sequence(sequence_id, reason);
    }
}

fn checkpoint_sequence_copy(
    db: &SharedDb,
    sequence_id: &str,
    pb: &Playbook,
    lead: &crate::db::Lead,
    person: &crate::db::Person,
    sequence: &CopySequence,
    reviews: &[TouchReview],
) -> Result<()> {
    for touch in &sequence.touches {
        let review = reviews.iter().find(|review| review.stage == touch.stage);
        let updated = db.update_touch_checkpoint(&Touch {
            sequence_id: sequence_id.to_string(),
            person_id: person.id.clone(),
            lead_id: lead.id.clone(),
            brand: pb.key.clone(),
            stage: touch.stage as i64,
            day_offset: touch.day_offset as i64,
            channel: touch.channel.clone(),
            subject: touch.subject.clone(),
            body: touch.body.clone(),
            purpose: touch.purpose.clone(),
            goal: touch.goal.clone(),
            status: "reviewing".into(),
            review_passes: review.map(|review| review.passes),
            review_issues: review
                .map(|review| {
                    let mut issues =
                        vec![format!("independent review score: {}/100", review.score)];
                    issues.extend(review.issues.clone());
                    issues
                })
                .unwrap_or_else(|| vec!["Copy review in progress".into()]),
            ..Default::default()
        })?;
        if !updated {
            return Err(anyhow!(
                "missing CRM checkpoint for {} stage {}",
                person.name,
                touch.stage
            ));
        }
    }
    Ok(())
}

/// Plan sequences for every verified person in `brand` who doesn't have one yet.
#[allow(clippy::too_many_arguments)]
pub async fn plan_pending(
    db: &SharedDb,
    client: &Engine,
    pb: &Playbook,
    business: &BusinessProfile,
    shared: &Shared,
    library: &Library,
    n_touches: usize,
    concurrency: usize,
    auto_schedule: bool,
    critique: bool,
    person_filter: Option<&str>,
    replace_drafts: bool,
    per_account_cap: Option<usize>,
    only_person_ids: Option<&HashSet<String>>,
    desired_outcome: Option<&str>,
    progress_reporter: Option<PlanProgressReporter>,
) -> Result<PlanSummary> {
    let requested_touches = n_touches.max(1);
    let n_touches = supported_touch_count_for_brand(&pb.key, requested_touches);
    if n_touches != requested_touches {
        log_outreach(format!(
            "normalized eager sequence from {requested_touches} to {n_touches} supported touches"
        ));
    }
    let concurrency = std::env::var("SPRUCE_OUTREACH_CONCURRENCY")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or_else(|| concurrency.min(2))
        .clamp(1, 4);
    let system = pb.copy_system_prompt(shared);

    // Verified people to sequence. An explicit --person request targets that exact
    // row. Bulk cold outreach opens with one best-supported person per account.
    let mut verified = db.list_people(Some(&pb.key), Some("verified"))?;
    // The full motion scopes drafting to the people IT just sourced, so a run
    // doesn't re-draft the brand's entire accumulated backlog every time.
    if let Some(ids) = only_person_ids {
        verified.retain(|person| ids.contains(&person.id));
    }
    let filtered = if person_filter.is_some() || only_person_ids.is_some() {
        verified
            .into_iter()
            .filter(|person| person_matches(person, person_filter))
            .collect::<Vec<_>>()
    } else {
        verified
    };
    // Cold outreach opens one human thread per account. Additional colleagues
    // remain mapped in the CRM and may be contacted only after a reply, route,
    // or explicit single-person request supplies a reason.
    let selected = if person_filter.is_some() {
        filtered
    } else {
        if per_account_cap.unwrap_or(1) > 1 {
            log_outreach("limited cold planning to one primary recipient per account");
        }
        select_people_for_planning(filtered, 1)
    };
    let mut todo = Vec::new();
    let mut matched_people = 0;
    let mut people_held = 0usize;
    for p in selected {
        matched_people += 1;
        if let Some(reason) =
            crate::gtm::recipient_sequence_block_reason(db, &pb.key, &p.lead_id, &p)?
        {
            people_held += 1;
            log_outreach(format!("held {}: {reason}", p.name));
            continue;
        }
        let context = crate::gtm::prepare_action(db, &pb.key, &p.lead_id, &p)?;
        if !context.sequence_ready_for(n_touches) {
            people_held += 1;
            log_outreach(format!(
                "held {}: GTM state '{}' is not ready for {n_touches} touches",
                p.name, context.state
            ));
            continue;
        }
        if let Some(sequence_id) = db.active_sequence_for_person(&p.id)? {
            let stale_copy = db
                .sequence_gtm_attribution(&sequence_id)?
                .is_some_and(|sequence| sequence.copy_policy_version < CURRENT_COPY_POLICY_VERSION);
            if !replace_drafts && !stale_copy {
                continue;
            }
            if db.sequence_sent_count(&sequence_id)? > 0 {
                // An explicit single-person request deserves a clear error; a bulk
                // re-draft just leaves already-in-flight sequences untouched rather
                // than aborting the whole run over one contact who's mid-thread.
                if person_filter.is_some() {
                    return Err(anyhow!(
                        "refusing to replace {}'s sequence because it already has sent touches",
                        p.name
                    ));
                }
                continue;
            }
            todo.push((p, Some(sequence_id), gtm_state_priority(&context.state)));
        } else {
            todo.push((p, None, gtm_state_priority(&context.state)));
        }
    }
    if person_filter.is_some() && matched_people == 0 {
        return Err(anyhow!(
            "no verified person matched '{}'",
            person_filter.unwrap_or_default()
        ));
    }
    if todo.is_empty() {
        return Ok(PlanSummary {
            people_held,
            ..Default::default()
        });
    }

    // When a bounded run or provider budget cannot cover everyone, consume
    // easy/action-ready accounts before medium/discovery-ready accounts.
    todo.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then_with(|| left.0.lead_id.cmp(&right.0.lead_id))
            .then_with(|| left.0.name.cmp(&right.0.name))
    });

    // A full seven-touch sequence is already a large structured result. Keep
    // every writer call to one recipient: three recipients in one response
    // produced 21 touches and exhausted the provider's 12,288-token output
    // boundary before returning valid JSON. Account context is intentionally
    // repeated so one person's failure cannot discard everyone else's copy.
    let max_recipients_per_call = if n_touches == 1 { 4 } else { 1 };
    let leads = db.list_leads(Some(&pb.key))?;
    let roster = todo
        .iter()
        .map(|(person, _, _)| PlanProgressRecipient {
            key: person.id.clone(),
            name: person.name.clone(),
            account: leads
                .iter()
                .find(|lead| lead.id == person.lead_id)
                .map(|lead| lead.name.clone())
                .unwrap_or_else(|| "Unknown account".into()),
        })
        .collect::<Vec<_>>();
    log_outreach(format!(
        "drafting sequences for {} verified people…",
        todo.len()
    ));
    let progress = PlanProgress::new(progress_reporter, roster);
    let mut grouped: HashMap<String, (u8, Vec<(crate::db::Person, Option<String>)>)> =
        HashMap::new();
    for (person, replaced_sequence, priority) in todo {
        grouped
            .entry(person.lead_id.clone())
            .or_insert_with(|| (priority, Vec::new()))
            .1
            .push((person, replaced_sequence));
    }
    let mut groups = grouped.into_iter().collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        left.1
             .0
            .cmp(&right.1 .0)
            .then_with(|| left.0.cmp(&right.0))
    });
    // Fan each account out into bounded recipient chunks.
    type AccountRecipients = Vec<(crate::db::Person, Option<String>)>;
    let units: Vec<(String, AccountRecipients)> = groups
        .into_iter()
        .flat_map(|(lead_id, (_, people))| {
            people
                .chunks(max_recipients_per_call)
                .map(|chunk| (lead_id.clone(), chunk.to_vec()))
                .collect::<Vec<_>>()
        })
        .collect();
    let business_context = business_copy_context(business);
    let performance_context = format!(
        "{}\n\n{}",
        empirical_copy_context(db, &pb.key)?,
        copy_research_rule_context(db, &pb.key)?
    );
    let stopped_reason = Arc::new(Mutex::new(None::<String>));
    let desired_outcome = desired_outcome.map(str::to_string);
    // Capacity checks and touch promotion form one small critical section. The
    // expensive writing and review calls still run concurrently, while every
    // accepted recipient is committed as soon as its batch completes.
    let finalize_lock = Arc::new(Mutex::new(()));
    let drafts = stream::iter(units.into_iter().map(|(lead_id, people)| {
        let db = db.clone();
        let system = system.clone();
        let business_context = business_context.clone();
        let performance_context = performance_context.clone();
        let lead = leads.iter().find(|lead| lead.id == lead_id).cloned();
        let knowledge = retrieve_outreach_knowledge(library, shared, lead.as_ref());
        let progress = progress.clone();
        let stopped_reason = Arc::clone(&stopped_reason);
        let finalize_lock = Arc::clone(&finalize_lock);
        let desired_outcome = desired_outcome.clone();
        async move {
            let Some(lead) = lead else {
                let recipients = people
                    .iter()
                    .map(|(person, _)| person.clone())
                    .collect::<Vec<_>>();
                for person in &recipients {
                    progress.finish_person("Unknown account", person, false);
                }
                return people.into_iter().map(|_| None).collect::<Vec<_>>();
            };
            let recipients = people
                .iter()
                .map(|(person, _)| person.clone())
                .collect::<Vec<_>>();
            let prior_stop = stopped_reason
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .clone();
            if let Some(prior_stop) = prior_stop {
                let phase = model_stop_phase(&prior_stop);
                for person in &recipients {
                    progress.stop_person(&lead.name, person, &format!("not attempted; {phase}"));
                }
                return people.into_iter().map(|_| None).collect::<Vec<_>>();
            }
            progress.group("preparing account context", &lead.name, &recipients);
            let generation_backend = client.backend().as_str().to_string();
            let generation_model = client.model_label();
            let gtm_contexts = people
                .iter()
                .filter_map(|(person, _)| {
                    crate::gtm::prepare_action(&db, &pb.key, &lead.id, person)
                        .ok()
                        .map(|context| (person.id.clone(), context))
                })
                .collect::<HashMap<_, _>>();
            let checkpoints = people
                .iter()
                .filter_map(|(person, _)| {
                    let context = gtm_contexts.get(&person.id)?;
                    match create_building_checkpoint(
                        &db,
                        pb,
                        &lead,
                        person,
                        context,
                        n_touches,
                        &generation_backend,
                        &generation_model,
                    ) {
                        Ok(sequence_id) => Some((person.id.clone(), sequence_id)),
                        Err(error) => {
                            log_outreach(format!(
                                "✗ could not checkpoint {} in CRM — {}",
                                person.name,
                                first_line(&error.to_string())
                            ));
                            None
                        }
                    }
                })
                .collect::<HashMap<_, _>>();
            match write_account_sequences(
                &db,
                client,
                &system,
                pb,
                shared,
                &lead,
                &recipients,
                n_touches,
                &business_context,
                &performance_context,
                &knowledge,
                &gtm_contexts,
                &checkpoints,
                desired_outcome.as_deref(),
                critique,
                &progress,
            )
            .await
            {
                Ok(mut outcome) => {
                    if let Some(reason) = outcome.stopped_reason.clone() {
                        let mut shared = stopped_reason
                            .lock()
                            .unwrap_or_else(|lock| lock.into_inner());
                        if shared.is_none() {
                            *shared = Some(reason);
                        }
                    }
                    people
                        .into_iter()
                        .map(|(person, replaced_sequence)| {
                            let copy = outcome.copies.remove(&person.id);
                            let failure = outcome.failures.remove(&person.id);
                            let gtm_context = gtm_contexts.get(&person.id).cloned();
                            let checkpoint = checkpoints.get(&person.id).cloned();
                            let accepted =
                                copy.is_some() && gtm_context.is_some() && checkpoint.is_some();
                            if !accepted {
                                if let Some(sequence_id) = checkpoint.as_deref() {
                                    if let Some(failure) = failure.as_ref() {
                                        if failure.held {
                                            let _ = db.hold_building_sequence(
                                                sequence_id,
                                                &failure.reason,
                                            );
                                        } else {
                                            close_building_checkpoint(
                                                &db,
                                                sequence_id,
                                                &failure.reason,
                                                failure.provider_stopped,
                                            );
                                        }
                                    } else {
                                        let _ = db.reject_building_sequence(
                                            sequence_id,
                                            "The sequence did not clear writing and review.",
                                        );
                                    }
                                }
                                if failure
                                    .as_ref()
                                    .is_some_and(|failure| failure.provider_stopped)
                                {
                                    progress.stop_person(
                                        &lead.name,
                                        &person,
                                        &failure
                                            .as_ref()
                                            .map(|failure| model_stop_phase(&failure.reason))
                                            .unwrap_or_else(|| {
                                                "stopped; provider unavailable".into()
                                            }),
                                    );
                                } else if failure.as_ref().is_some_and(|failure| failure.held) {
                                    let phase = failure
                                        .as_ref()
                                        .map(|failure| first_line(&failure.reason))
                                        .unwrap_or_else(|| "held for research".to_string());
                                    progress.finish_person_as(&lead.name, &person, &phase, "held");
                                } else {
                                    let phase = failure
                                        .as_ref()
                                        .map(|failure| {
                                            format!("rejected: {}", first_line(&failure.reason))
                                        })
                                        .unwrap_or_else(|| "rejected; feedback saved".into());
                                    progress
                                        .finish_person_as(&lead.name, &person, &phase, "rejected");
                                }
                            } else {
                                progress.person(
                                    "review passed; finalizing CRM",
                                    &lead.name,
                                    &person,
                                );
                            }
                            let copy = copy?;
                            let gtm_context = gtm_context?;
                            let checkpoint = checkpoint?;
                            let _guard = finalize_lock
                                .lock()
                                .unwrap_or_else(|lock| lock.into_inner());
                            match finalize_reviewed_draft(
                                &db,
                                pb,
                                shared,
                                business,
                                auto_schedule,
                                critique,
                                &progress,
                                &person,
                                &lead,
                                &copy,
                                replaced_sequence.as_deref(),
                                &gtm_context,
                                &checkpoint,
                            ) {
                                Ok(finalized) => Some(finalized),
                                Err(error) => {
                                    let reason = format!(
                                        "could not finalize accepted copy: {}",
                                        first_line(&format!("{error:#}"))
                                    );
                                    close_building_checkpoint(&db, &checkpoint, &reason, false);
                                    progress.finish_person_as(
                                        &lead.name,
                                        &person,
                                        &format!("rejected: {reason}"),
                                        "rejected",
                                    );
                                    None
                                }
                            }
                        })
                        .collect::<Vec<_>>()
                }
                Err(e) => {
                    let provider_stopped = crate::engine::is_usage_exhausted(&e)
                        || crate::engine::is_retryable_provider_error(&e)
                        || crate::engine::is_run_budget_exhausted(&e)
                        || crate::engine::is_generation_incomplete(&e);
                    if provider_stopped {
                        let mut shared = stopped_reason
                            .lock()
                            .unwrap_or_else(|lock| lock.into_inner());
                        if shared.is_none() {
                            *shared = Some(model_stop_reason(&e));
                        }
                    }
                    let reason = format!("{e:#}");
                    for (person, _) in &people {
                        if let Some(sequence_id) = checkpoints.get(&person.id) {
                            close_building_checkpoint(&db, sequence_id, &reason, provider_stopped);
                        }
                        if provider_stopped {
                            progress.stop_person(&lead.name, person, &model_stop_phase(&reason));
                        } else {
                            progress.finish_person_as(
                                &lead.name,
                                person,
                                &format!("rejected: {}", first_line(&reason)),
                                "rejected",
                            );
                        }
                        log_outreach(format!(
                            "✗ copy {} for {} — {}",
                            if provider_stopped {
                                "stopped"
                            } else {
                                "rejected"
                            },
                            person.name,
                            first_line(&reason)
                        ));
                    }
                    people.into_iter().map(|_| None).collect::<Vec<_>>()
                }
            }
        }
    }))
    .buffered(concurrency)
    .collect::<Vec<_>>()
    .await;

    let drafts = drafts.into_iter().flatten().flatten().collect::<Vec<_>>();
    let people_rejected = progress.rejected.load(Ordering::Relaxed);
    let people_held = people_held + progress.held.load(Ordering::Relaxed);
    let people_stopped = progress.stopped.load(Ordering::Relaxed);
    let stopped_reason = stopped_reason
        .lock()
        .unwrap_or_else(|lock| lock.into_inner())
        .clone();
    let mut summary = PlanSummary {
        people_rejected,
        people_held,
        people_stopped,
        stopped_reason,
        ..Default::default()
    };
    let mut planned_by_lead: HashMap<String, HashSet<String>> = HashMap::new();
    for draft in drafts {
        planned_by_lead
            .entry(draft.lead_id)
            .or_default()
            .insert(draft.person_id);
        summary.touches_scheduled += draft.touches_scheduled;
        summary.touches_drafted += draft.touches_drafted;
        summary.sequences_replaced += usize::from(draft.sequence_replaced);
        summary.people_planned += 1;
    }
    summary.planned_lead_ids = planned_by_lead.keys().cloned().collect();
    summary.planned_lead_ids.sort();

    // A bulk replacement also retires unsent legacy sequences for contacts that
    // were outside the operator's selected scope at each replanned account.
    // Sent history is never removed.
    if replace_drafts && person_filter.is_none() {
        for person in db.list_people(Some(&pb.key), None)? {
            let Some(kept_people) = planned_by_lead.get(&person.lead_id) else {
                continue;
            };
            if kept_people.contains(&person.id) {
                continue;
            }
            let Some(sequence_id) = db.active_sequence_for_person(&person.id)? else {
                continue;
            };
            if db.sequence_sent_count(&sequence_id)? == 0
                && db.discard_unsent_sequence(&sequence_id)?
            {
                summary.sequences_replaced += 1;
            }
        }
    }

    if summary.stopped_reason.is_some() {
        progress.overall("stopped early; saved every draft that passed before the limit");
    } else {
        progress.overall("finished saving drafts");
    }
    Ok(summary)
}

pub fn supported_touch_count(requested: usize) -> usize {
    match requested.max(1) {
        1 => 1,
        2 => 2,
        7.. => 7,
        _ => 4,
    }
}

pub fn supported_touch_count_for_brand(brand: &str, requested: usize) -> usize {
    if brand.eq_ignore_ascii_case("outagehub") {
        if requested <= 1 {
            1
        } else {
            2
        }
    } else {
        supported_touch_count(requested)
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_reviewed_draft(
    db: &SharedDb,
    pb: &Playbook,
    shared: &Shared,
    business: &BusinessProfile,
    auto_schedule: bool,
    critique: bool,
    progress: &PlanProgress,
    person: &crate::db::Person,
    lead: &crate::db::Lead,
    copy: &ReviewedCopy,
    replaced_sequence: Option<&str>,
    gtm_context: &GtmActionContext,
    seq_id: &str,
) -> Result<FinalizedDraft> {
    let seq = &copy.sequence;
    if let Some(reason) = cross_recipient_similarity_issue(db, pb, person, seq_id, seq)? {
        return Err(anyhow!(reason));
    }
    let now = Utc::now();
    let delivery_ready = auto_schedule
        && gtm_context.delivery_ready_for(seq.touches.len())
        && crate::gtm::candidate_delivery_block_reason(db, pb, lead, person, seq.touches.len())?
            .is_none();
    let mut touches_scheduled = 0usize;
    let mut touches_drafted = 0usize;
    for t in &seq.touches {
        let is_email = is_email_capable_channel(&t.channel);
        let body = if is_email {
            playbook::enforce_signature(&t.body, &pb.signature)
        } else {
            t.body.clone()
        };
        let final_touch = CopyTouch {
            body: body.clone(),
            ..t.clone()
        };
        let lint = lint_copy_touch(pb, shared, &final_touch);
        let review = copy.reviews.iter().find(|review| review.stage == t.stage);
        let passes = lint.forbidden_hits.is_empty()
            && lint.length_ok
            && lint.signature_ok
            && (!critique
                || review.is_some_and(|review| {
                    review.passes && review.score >= 85 && review.issues.is_empty()
                }));
        let review_issues = review
            .map(|review| {
                let mut issues = vec![format!("independent review score: {}/100", review.score)];
                issues.extend(review.issues.clone());
                issues
            })
            .unwrap_or_else(|| lint.forbidden_hits.clone());

        let can_automate = t.channel.eq_ignore_ascii_case("email")
            || (t.channel.eq_ignore_ascii_case("linkedin_or_email")
                && person.linkedin_status != "connected");
        let status = if can_automate && delivery_ready && passes {
            "scheduled"
        } else {
            "draft"
        };
        if status == "scheduled" {
            touches_scheduled += 1;
        } else {
            touches_drafted += 1;
        }

        let desired = now + Duration::days(t.day_offset as i64);
        let stable_key = format!("{}:{}:{}", person.id, seq_id, t.stage);
        let timing = TimingContext {
            industry: &lead.industry,
            title: &person.title,
            vantage: &person.vantage,
            channel: &t.channel,
            location: if person.location.is_empty() {
                &lead.hq
            } else {
                &person.location
            },
            timezone: if person.timezone.is_empty() {
                &lead.timezone
            } else {
                &person.timezone
            },
            stable_key: &stable_key,
        };
        let slot = calendar::schedule_with_capacity(business, &timing, desired, |start, end| {
            db.planned_touch_count_between(&pb.key, start, end)
        })?;
        let updated = db.update_touch_checkpoint(&Touch {
            sequence_id: seq_id.to_string(),
            person_id: person.id.clone(),
            lead_id: lead.id.clone(),
            brand: pb.key.clone(),
            stage: t.stage as i64,
            day_offset: t.day_offset as i64,
            channel: t.channel.clone(),
            subject: t.subject.clone(),
            body,
            purpose: t.purpose.clone(),
            goal: t.goal.clone(),
            status: status.into(),
            due_at: slot.at.to_rfc3339(),
            recipient_timezone: slot.recipient_timezone,
            scheduled_rule: slot.rule,
            schedule_reason: slot.rationale,
            review_passes: Some(passes),
            review_issues,
            ..Default::default()
        })?;
        if !updated {
            return Err(anyhow!(
                "CRM checkpoint disappeared for {} stage {}",
                person.name,
                t.stage
            ));
        }
    }
    db.promote_building_sequence(seq_id, replaced_sequence, &seq.applied_principles)?;
    if touches_scheduled > 0 {
        calendar::rebalance_approved_sales(db, business, chrono::Utc::now())?;
    }
    // The sequence is already durable and visible. Event logging is useful
    // telemetry, but a logging hiccup must not report a successfully promoted
    // sequence as rejected.
    let _ = db.log_event(
        &pb.key,
        &person.id,
        "",
        if touches_scheduled > 0 {
            "scheduled"
        } else {
            "drafted"
        },
        &format!("{}-touch sequence", seq.touches.len()),
    );
    log_outreach(format!(
        "✓ drafted, reviewed, and filed {}-touch sequence for {}",
        seq.touches.len(),
        person.name
    ));
    progress.finish_person(&lead.name, person, true);
    Ok(FinalizedDraft {
        lead_id: lead.id.clone(),
        person_id: person.id.clone(),
        touches_scheduled,
        touches_drafted,
        sequence_replaced: replaced_sequence.is_some(),
    })
}

/// The planning role lives in an editable persona file. Rust only selects the
/// role and supplies typed context.
fn plan_system_prompt(pb: &Playbook, shared: &Shared) -> String {
    pb.planning_system_prompt(shared)
}

/// Plan one recipient's full sequence before a word of copy is written.
struct SequencePlanningInput<'a> {
    account: &'a CopyAccount,
    person: &'a crate::db::Person,
    touches: usize,
    knowledge: &'a RoleKnowledge,
    gtm_context: &'a str,
    desired_outcome: Option<&'a str>,
}

async fn plan_sequence(
    client: &Engine,
    plan_system: &str,
    input: SequencePlanningInput<'_>,
) -> Result<SequencePlan> {
    let SequencePlanningInput {
        account,
        person,
        touches: n,
        knowledge,
        gtm_context,
        desired_outcome,
    } = input;
    let account = planner_account_brief(account);
    let effective_vantage = response_design::effective_vantage(&person.title, &person.vantage);
    let recipient = json!({
        "name": person.name,
        "first_name": person.first_name,
        "title": person.title,
        "vantage": effective_vantage,
        "likely_access_internal_only": person.can_observe.as_str(),
        "role_response_contract_internal_only": response_design::for_person(person).prompt_value(),
        "operator_requested_outcome_internal_only": desired_outcome.unwrap_or("No narrower outcome supplied; choose the smallest useful cold response for this vantage and evidence state."),
    });
    let user = format!(
        "Plan a {n}-touch no-reply sequence for this recipient. Hypotheses define the decision and mechanism but are not facts. Action-ready accounts may use the supported cadence. Discovery-ready accounts may use exactly one complete, hypothesis-led first email; they must not receive follow-ups before a reply. Every other state remains in research. First complete the private response_strategy: the exact response Andrew needs, why it could matter to this role, one concrete operating scene, the credibility basis, the smallest voluntary commitment, and what would trigger reactance. Honor an operator-requested outcome when it is earned by the evidence and this person's vantage; otherwise reduce it only as much as evidence or role fit requires and record that reduction in overall_strategy. When it is supported, preserve its exact operating decision in the T1 ask: do not replace it with a broad workflow-interview question such as `what takes the most time`, `what happens today`, or `how do you handle this`. A short discovery conversation plus an email-reply alternative is appropriate for a direct operator, process owner, or operational executive when research supports a concrete task, decision, or mechanism. Reserve routing-only treatment for routers or misaligned titles. Before selecting the thread, compare three distinct T1 approaches: problem-sniffing from the strongest source, a concise commercial point of view, and an existence-or-routing note. Prefer the one with the clearest recipient reason to answer and lowest evidence risk; do not blend them. State the selected approach and why in overall_strategy.\n\nACCOUNT BRIEF:\n{account}\n\nRECIPIENT:\n{recipient}\n\nPRIVATE GTM ACTION CONTEXT:\n{gtm_context}\n\nRELEVANT PLANNING KNOWLEDGE:\n{knowledge}\n\nT1 connects a verified trigger to one operating decision and one role-matched ask. T2 advances the mechanism. T3 is a human LinkedIn request. T4, when present, adds a sourced fact, useful distinction, objection answer, route, or close. Never invent collateral or a later stage merely to fill the plan. For each touch return stage, channel, objective, angle, and at most one ask. Never make LinkedIn say only that an email was sent. Cite only principle IDs that changed the plan.",
        account = serde_json::to_string_pretty(&account).unwrap_or_default(),
        recipient = serde_json::to_string_pretty(&recipient).unwrap_or_default(),
        knowledge = knowledge.block,
    );
    let mut plan = client
        .structured_bulk::<SequencePlan>("outreach.plan", plan_system, &user, plan_schema(n))
        .await?;
    plan.applied_principles =
        normalize_principle_ids(&plan.applied_principles, &knowledge.allowed_ids);
    Ok(plan)
}

#[allow(clippy::too_many_arguments)]
async fn write_account_sequences(
    db: &SharedDb,
    client: &Engine,
    system: &str,
    pb: &Playbook,
    shared: &Shared,
    lead: &crate::db::Lead,
    people: &[crate::db::Person],
    n: usize,
    business_context: &str,
    performance_context: &str,
    knowledge: &OutreachKnowledge,
    gtm_contexts: &HashMap<String, GtmActionContext>,
    checkpoints: &HashMap<String, String>,
    desired_outcome: Option<&str>,
    critique: bool,
    progress: &PlanProgress,
) -> Result<AccountCopyResult> {
    let account = copy_account(lead);

    let mut plans: HashMap<String, SequencePlan> = HashMap::new();
    let lean = client.prefers_lean_outreach();
    if !lean {
        // Production keeps angle selection independent from realization so the
        // writer is not forced to defend its first idea. An explicit experiment
        // flag can fold the planner into the writer for a lower-cost comparison.
        progress.group(&format!("planning {n}-touch strategy"), &lead.name, people);
        let plan_system = plan_system_prompt(pb, shared);
        let account_ref = &account;
        let planned = futures::future::join_all(people.iter().map(|person| {
            let plan_system = plan_system.clone();
            async move {
                (
                    person.id.clone(),
                    plan_sequence(
                        client,
                        &plan_system,
                        SequencePlanningInput {
                            account: account_ref,
                            person,
                            touches: n,
                            knowledge: &knowledge.planner,
                            gtm_context: &gtm_contexts
                                .get(&person.id)
                                .map(GtmActionContext::prompt_block)
                                .unwrap_or_else(|| "GTM ACTION STATE: unavailable".into()),
                            desired_outcome,
                        },
                    )
                    .await,
                )
            }
        }))
        .await;
        for (person_id, plan) in planned {
            plans.insert(person_id, plan?);
        }
    }

    // Phase 2 — WRITE. Hand each recipient's plan to the writer, which turns the
    // strategy into the actual sendable, greeting-led copy.
    progress.group(
        &if lean {
            format!("designing and writing touches 1–{n}")
        } else {
            format!("writing touches 1–{n}")
        },
        &lead.name,
        people,
    );
    let recipient_payload = |person: &crate::db::Person| {
        json!({
            "person_key": person.id,
            "name": person.name,
            "first_name": person.first_name,
            "title": person.title,
            "vantage": response_design::effective_vantage(&person.title, &person.vantage),
            "likely_access_internal_only": person.can_observe.as_str(),
            "why_this_person_internal_only": person.why_them.as_str(),
            "person_research_status": if person.linkedin_url.trim().is_empty() {
                "No person-level profile source is on file. Do not fake individual personalization; use the account signal and role-appropriate ask."
            } else {
                "A LinkedIn URL is on file, but its profile content has not been retrieved. The title is the only verified person-level insight; do not imply posts, tenure, priorities, or biography."
            },
            "verified_person_insights": Vec::<String>::new(),
            "role_response_contract_internal_only": response_design::for_person(person).prompt_value(),
            "operator_requested_outcome_internal_only": desired_outcome.unwrap_or("No narrower outcome supplied; choose the smallest useful cold response for this vantage and evidence state."),
            "sequence_plan": plans.get(&person.id),
            "previous_rejection_feedback_internal_only": db
                .latest_rejected_sequence_feedback(&person.id)
                .unwrap_or_default(),
            "copy_decision_context": gtm_contexts
                .get(&person.id)
                .map(GtmActionContext::copy_prompt_block)
                .unwrap_or_else(|| "GTM ACTION STATE: unavailable".into()),
        })
    };
    let writer_account = writer_account_brief(&account);
    let planning_contract = if lean {
        "For each recipient, choose one source-backed trigger and one operating decision this title can plausibly answer. Keep the mechanism explicitly unverified. Privately draft three genuinely different T1 candidates: a problem-sniffing note, a concise point of view, and an existence-or-routing note. Pick one; never blend or return the alternatives. T2 must sharpen the mechanism rather than restate T1. T3 is a natural connection request that gives a concrete reason to connect; never fill it with praise for the recipient's remit, background, perspective, or work. If T4 is present, add only a sourced fact, a useful concrete distinction, or an honest answer to the strongest objection; never invent an artifact to fill the slot. The sequence stays on one human thread and must not become an interview or a chain of retreats. Do not expose the private plan or discarded candidates."
    } else {
        "Follow each recipient's supplied private sequence_plan. Treat response_strategy as the governing outcome and recipient-friction brief, not language to paste into the email."
    };
    let writer_knowledge = knowledge.writer.block.clone();
    let brand_trigger_contract = brand_trigger_contract(&pb.key, n);
    let t1_contract = "T1 CONTRACT: Write a complete founder note inside the configured word band. Explain why this person, name one recognizable problem and plausible consequence, state one concrete and evidence-safe seller contribution, and offer one natural response path. For a medium/discovery-ready account, the one direct operating answer is the sole CTA; do not add a call before the missing term is confirmed. A correction may be useful, but Andrew's desire for one is never the recipient's reason to answer. When the operator requested a supported operating decision or response, preserve it in T1; never broaden it into a generic workflow interview such as `what takes the most time`, `what happens today`, or `how do you handle this`. Hold rather than manufacture any missing account foundation.";
    let writer_instructions = format!(
        r#"Write one {n}-touch no-reply sequence for each recipient. {planning_contract}

{brand_trigger_contract}

{t1_contract}

Think through the buyer-safe brief and copy decision context. Return exactly one result for every person_key. First choose send_decision. Use send only when verified facts support the trigger, the title can credibly answer the ask, and one natural first note can test the hypothesis without pretending it is true. Otherwise choose hold_for_research, explain the missing evidence privately in decision_reason, and return no touches. Abstention is better than filler. For a send decision, privately state the operating decision, mechanism to test, strongest objection, recipient's reason to reply, and supported give-back. Never invent collateral, customer proof, or prior analysis.

If previous_rejection_feedback_internal_only is nonempty, this is a whole-sequence rewrite after a failed review. Treat every saved finding as a hard defect to remove, reconsider the structure that produced it, and return genuinely revised copy. Do not quote or mention the feedback to the recipient.

T1 must use the brand-specific word band. Write it as one natural note, not as a checklist. Use one verified fact to explain why this person received it, ask about one recognizable operating moment, state one concrete seller difference in plain language, and give one easy way to answer. Treat the mechanism as uncertainty, but do not narrate the research process or stack caveats. A complete founder note usually moves naturally from why this person, to the operating moment and bounded guess, to what Andrew is exploring, to one response path; use that logic without copying a fixed template. Never dictate answer labels or turn the note into a multiple-choice research form; an easy answer is an ordinary sentence in the recipient's own language. Never manufacture specificity with a task, object, machine, customer, or process that is absent from verified facts. A short discovery conversation is a valid cold ask for an action-ready direct workflow owner when the hypothesis is precise. For a discovery-ready recipient, ask for the one missing operating answer by email and stop; do not stack a call invitation onto it. A router may only be asked to route.

Before returning T1, privately write five possible subjects and discard any that merely label the category, such as `utility status`, `power alarms`, `claim evidence`, `decision trail`, or `automation question`. Choose a 3-9 word subject that names the recognizable event, decision, object, or consequence in the email and gives this recipient an accurate reason to open. It should remain plain and forwardable, never clickbait. Then read the first two body lines aloud. Rewrite compressed phrases such as `make this decision consequential`, `the distinction I have in mind`, and `the practical difference is` into ordinary spoken English.

For one touch use email/0. For two touches use email/0 and email/6. For four touches use email/0, email/3, linkedin_request/7, email/14. For seven touches use email/0, email/3, linkedin_request/5, email/9, linkedin_or_email/13, email/17, linkedin_or_email/21. Every email-capable touch must look like an email: `Hi [First name],` on its own line, a coherent message, and the exact signature on its own line. T1 uses one plain, specific 3-9 word operational subject; sentence case or title case is fine. Later email-capable touches preserve it with one re: prefix. A linkedin_request has no subject, greeting, signature, pitch, meeting ask, or prior-email reference; it must stay under 300 characters. It must name the operating question or shared topic that makes connecting useful. Empty compliments such as `substantial remit`, `valuable perspective`, `impressive background`, or `I respect your work` fail.

Purpose and goal are private CRM notes, never substitutes for buyer-facing prose. Before returning, read the whole sequence as the recipient. Remove generic lessons, fragments, surveys, framework language, and repeated retreat lines. In four touches, at most one touch may mainly say Andrew may be wrong, invite a correction/referral, or close; in seven touches the maximum is three. Rewrite any excess around mechanism, useful contribution, and the hard buyer objection. Never reveal play labels, experiment arms, confidence scores, or internal hypotheses."#,
        n = n,
        planning_contract = planning_contract,
        brand_trigger_contract = brand_trigger_contract,
        t1_contract = t1_contract,
    );
    let writer_user = |recipients: &[Value]| {
        format!(
            "{instructions}\n\nBUYER-SAFE ACCOUNT BRIEF:\n{account}\n\nRECIPIENTS (private context; never quote its labels):\n{recipients}\n\nVERIFIED SELLER CONTEXT:\n{business_context}\n\nEMPIRICAL OUTBOUND FEEDBACK (learn the shape, never copy wording or treat prior reply details as facts about this account):\n{performance_context}\n\nRETRIEVED KNOWLEDGE (apply as judgment; never paste a framework or force a citation):\n{knowledge}",
            instructions = writer_instructions,
            account = serde_json::to_string_pretty(&writer_account).unwrap_or_default(),
            recipients = serde_json::to_string_pretty(recipients).unwrap_or_default(),
            business_context = business_context,
            performance_context = performance_context,
            knowledge = writer_knowledge,
        )
    };
    // The folded-planner experiment isolates recipients because batching made
    // the writer reuse the same polished skeleton. Production normally has one
    // primary recipient here, so its separate plan and copy remain isolated too.
    let requests = if lean {
        people
            .iter()
            .map(|person| {
                let recipients = vec![recipient_payload(person)];
                (vec![person.id.clone()], writer_user(&recipients))
            })
            .collect::<Vec<_>>()
    } else {
        let recipients = people.iter().map(recipient_payload).collect::<Vec<_>>();
        vec![(
            people.iter().map(|person| person.id.clone()).collect(),
            writer_user(&recipients),
        )]
    };
    let written = futures::future::join_all(requests.into_iter().map(|(person_ids, user)| {
        let progress = progress.clone();
        let account_name = lead.name.clone();
        let people_for_progress = people
            .iter()
            .filter(|person| person_ids.contains(&person.id))
            .cloned()
            .collect::<Vec<_>>();
        async move {
            progress.group(
                &format!("writing focused touches 1–{n}"),
                &account_name,
                &people_for_progress,
            );
            let result = client
                .structured_bulk::<BatchCopy>(
                    "outreach.write_account",
                    system,
                    &user,
                    batch_sequence_schema(n, person_ids.len()),
                )
                .await;
            (person_ids, result)
        }
    }))
    .await;
    let expected = people
        .iter()
        .map(|person| person.id.as_str())
        .collect::<HashSet<_>>();
    let mut raw_by_person = HashMap::new();
    let mut write_failures = HashMap::<String, CopyFailure>::new();
    let mut write_stop_reason = None;
    for (requested_people, result) in written {
        match result {
            Ok(batch) => {
                for raw in batch.sequences {
                    if expected.contains(raw.person_key.as_str()) {
                        if raw.send_decision.trim() == "send" && raw.touches.len() == n {
                            raw_by_person.entry(raw.person_key.clone()).or_insert(raw);
                        } else {
                            let reason = if raw.decision_reason.trim().is_empty() {
                                "writer held the recipient because the evidence did not support sendable copy"
                                    .to_string()
                            } else {
                                format!("writer held for research: {}", raw.decision_reason.trim())
                            };
                            write_failures.insert(
                                raw.person_key,
                                CopyFailure {
                                    reason,
                                    provider_stopped: false,
                                    held: true,
                                },
                            );
                        }
                    }
                }
            }
            Err(error) => {
                let reason = format!(
                    "writing focused copy for {}: {error:#}",
                    requested_people.join(", ")
                );
                let provider_stopped = crate::engine::is_usage_exhausted(&error)
                    || crate::engine::is_retryable_provider_error(&error)
                    || crate::engine::is_run_budget_exhausted(&error)
                    || crate::engine::is_generation_incomplete(&error);
                if provider_stopped && write_stop_reason.is_none() {
                    write_stop_reason = Some(model_stop_reason(&error));
                }
                for person_id in requested_people {
                    write_failures.insert(
                        person_id,
                        CopyFailure {
                            reason: reason.clone(),
                            provider_stopped,
                            held: false,
                        },
                    );
                }
            }
        }
    }
    for person in people {
        if !raw_by_person.contains_key(&person.id) && !write_failures.contains_key(&person.id) {
            write_failures.insert(
                person.id.clone(),
                CopyFailure {
                    reason: "writer returned no sequence for this recipient".into(),
                    provider_stopped: false,
                    held: false,
                },
            );
        }
    }

    // Review recipients concurrently. Previously a two-person writer batch then
    // put those people through editor + council one after another, which was the
    // long serialized tail users experienced as an apparently frozen spinner.
    let jobs = people
        .iter()
        .filter_map(|person| {
            raw_by_person.remove(&person.id).map(|raw| {
                (
                    person.clone(),
                    raw,
                    plans.get(&person.id).cloned(),
                    checkpoints.get(&person.id).cloned(),
                    gtm_contexts.get(&person.id).cloned(),
                )
            })
        })
        .collect::<Vec<_>>();
    let reviewed = stream::iter(jobs.into_iter().map(
        |(person, raw, plan, checkpoint, context)| {
            let progress = progress.clone();
            let account = account.clone();
            async move {
                let result = review_person_copy(
                    db,
                    client,
                    pb,
                    shared,
                    lead,
                    &account,
                    &person,
                    raw,
                    plan.as_ref(),
                    context.as_ref(),
                    checkpoint.as_deref(),
                    n,
                    knowledge,
                    business_context,
                    critique,
                    &progress,
                )
                .await;
                (person, checkpoint, result)
            }
        },
    ))
    .buffer_unordered(people.len().max(1))
    .collect::<Vec<_>>()
    .await;
    let mut output = HashMap::new();
    let mut failures = write_failures;
    let mut stopped_reason = write_stop_reason;
    for (person, checkpoint, result) in reviewed {
        match result {
            Ok(copy) => {
                output.insert(person.id.clone(), copy);
            }
            Err(error) => {
                let reason = format!("{error:#}");
                let provider_stopped = crate::engine::is_usage_exhausted(&error)
                    || crate::engine::is_retryable_provider_error(&error)
                    || crate::engine::is_run_budget_exhausted(&error)
                    || crate::engine::is_generation_incomplete(&error);
                if provider_stopped && stopped_reason.is_none() {
                    stopped_reason = Some(model_stop_reason(&error));
                }
                if let Some(sequence_id) = checkpoint.as_deref() {
                    close_building_checkpoint(db, sequence_id, &reason, provider_stopped);
                }
                log_outreach(format!(
                    "✗ copy {} for {} — {}",
                    if provider_stopped {
                        "stopped"
                    } else {
                        "rejected"
                    },
                    person.name,
                    first_line(&reason)
                ));
                failures.insert(
                    person.id.clone(),
                    CopyFailure {
                        reason,
                        provider_stopped,
                        held: false,
                    },
                );
            }
        }
    }
    Ok(AccountCopyResult {
        copies: output,
        failures,
        stopped_reason,
    })
}

pub(crate) fn brand_trigger_contract(brand: &str, touches: usize) -> &'static str {
    if brand == "gnk" && touches == 1 {
        "GNK PRECISE FIRST-TOUCH CONTRACT: Research must support one account-specific recurring decision OR direct mechanism/artifact evidence, plus a recipient close to it. T1 distinguishes sourced fact from hypothesis, names the recognizable workflow in ordinary language, frames the unproved consequence as one sharp question when necessary, explains one concrete way GnK could help, and offers a short conversation with an email answer as the easier path. Never reuse a universal records/reconstruction fork or make correction the only recipient value."
    } else if brand == "gnk" {
        "GNK COMMERCIAL DISCOVERY CONTRACT: Multi-touch copy requires a specific recurring decision, believable consequence, mechanism evidence, and recipient close to the work. T1 names the problem, explains one concrete GnK contribution, and offers a short conversation with an email alternative. Follow-ups add evidence, consequence, a bounded test, or a useful route."
    } else if brand == "wapahki" && touches == 1 {
        "WAPAHKI PRECISE FIRST-TOUCH CONTRACT: Research must identify one physical candidate task or handoff and a recipient close to it. T1 is a natural founder/researcher note: why this operator, the sourced facility/task clue, one honestly framed consequence or economic question, Andrew's University of Toronto and Automata robotics context, one concrete Wapahki contribution, and one low-friction response path. When the verified seller context permits it, give the recipient a practical reason to answer by offering to apply Wapahki's existing one-page automation-fit screen to this exact task and return a first-pass view; never imply that assessment already exists. For a medium account, ask for the missing operating answer by email and stop. Arrive with the candidate task; never ask the recipient to search the operation for a use case."
    } else if brand == "wapahki" {
        "WAPAHKI FOUNDER-RESEARCHER CONTRACT: Multi-touch copy requires one source-supported physical task, credible economic pressure, and a reachable owner. T1 is a natural founder/researcher note with Andrew's University of Toronto and Automata context, one Wapahki contribution, and a short call-or-email response path. Follow-ups advance the same task."
    } else if brand == "outagehub" && touches == 2 {
        "OUTAGEHUB TWO-EMAIL EVIDENCE CONTRACT: This cadence is for distributed Canadian operators with one exact outage-time decision, a nearby operations recipient, and a completed location-specific historical utility match. T1 explains OutageHub's location-matched Canadian utility API, frames one evidence-safe consequence, and offers a natural short conversation or email path. T2 contributes the verified location and timestamp without claiming private site or asset status. Do not prescribe a universal dark-site/equipment-ticket binary."
    } else if brand == "outagehub" {
        "OUTAGEHUB PRECISE FIRST-TOUCH CONTRACT: Target an operator with distributed Canadian locations and one evidenced outage-time diagnosis, dispatch, escalation, continuity, prioritization, or communication decision. T1 names the account-specific decision, explains in plain language that OutageHub supplies location-matched Canadian utility reports through an API, and offers a short conversation with an email answer as the easier path. A completed historical match may be mentioned with its exact boundary; it is not required for this first discovery touch. Never claim private site status or reuse a universal dark-site/ticket binary."
    } else {
        ""
    }
}

#[allow(clippy::too_many_arguments)]
async fn review_person_copy(
    db: &SharedDb,
    client: &Engine,
    pb: &Playbook,
    shared: &Shared,
    lead: &crate::db::Lead,
    account: &CopyAccount,
    person: &crate::db::Person,
    raw: PersonSequenceCopy,
    plan: Option<&SequencePlan>,
    gtm_context: Option<&GtmActionContext>,
    checkpoint: Option<&str>,
    n: usize,
    knowledge: &OutreachKnowledge,
    seller_context: &str,
    critique: bool,
    progress: &PlanProgress,
) -> Result<ReviewedCopy> {
    let person_progress = |phase: &str| progress.person(phase, &lead.name, person);
    person_progress("checkpointing written copy in CRM");
    let sequence_id = checkpoint.ok_or_else(|| anyhow!("CRM checkpoint was not created"))?;
    let lean = client.prefers_lean_outreach();
    let allowed_principles = knowledge.writer.allowed_ids.clone();
    let private_copy_decision = format!(
        "PRIVATE COPY DECISION (judge the messages against this; never paste these labels):\n- desired response: {}\n- role-relevant motive: {}\n- concrete scene: {}\n- credibility basis: {}\n- smallest voluntary commitment: {}\n- reactance guard: {}\n- writer's operating moment and decision: {}\n- writer's mechanism to test, never an account fact: {}\n- canonical account mechanism hypothesis, never an account fact: {}\n- concrete system concept only if the premise is confirmed: {}\n- writer's strongest buyer objection: {}\n- canonical hard buyer question: {}\n- role-relevant reason this topic could matter: {}\n- supported seller give-back, empty when none: {}",
        plan.map(|plan| plan.response_strategy.desired_response.trim()).unwrap_or_default(),
        plan.map(|plan| plan.response_strategy.role_relevant_motive.trim()).unwrap_or_default(),
        plan.map(|plan| plan.response_strategy.concrete_scene.trim()).unwrap_or_default(),
        plan.map(|plan| plan.response_strategy.credibility_basis.trim()).unwrap_or_default(),
        plan.map(|plan| plan.response_strategy.smallest_commitment.trim()).unwrap_or_default(),
        plan.map(|plan| plan.response_strategy.reactance_guard.trim()).unwrap_or_default(),
        raw.operating_decision.trim(),
        raw.mechanism_to_test.trim(),
        account.mechanism.trim(),
        account.system_concept.trim(),
        raw.hard_buyer_objection.trim(),
        account.hard_buyer_question.trim(),
        raw.recipient_reply_reason.trim(),
        raw.value_exchange.trim(),
    );
    let mut sequence = CopySequence {
        touches: raw.touches,
        applied_principles: normalize_principle_ids(&raw.applied_principles, &allowed_principles),
    };
    // Principles are lineage, not a quota. Forcing at least one citation made
    // the model visibly inject frameworks even when plain account evidence was
    // the stronger basis for the email. Keep only IDs it says materially
    // influenced the work; an empty list is valid.
    if let Some(plan) = plan {
        sequence
            .applied_principles
            .extend(plan.applied_principles.iter().cloned());
        sequence.applied_principles.sort();
        sequence.applied_principles.dedup();
    }
    enforce_email_signatures(&mut sequence, &pb.signature);
    normalize_thread_subjects(&mut sequence);
    checkpoint_sequence_copy(db, sequence_id, pb, lead, person, &sequence, &[])?;
    let reviewer_knowledge = format!(
        "{}\n\n{}\n\n{}\n\nVERIFIED SELLER CONTEXT (may validate Andrew's own evidence or give-back; never treat it as a fact about the recipient):\n{}",
        knowledge.reviewer.block,
        private_copy_decision,
        gtm_context
            .map(GtmActionContext::copy_prompt_block)
            .unwrap_or_else(|| "COPY DECISION STATE: unavailable".into()),
        seller_context
    );
    let council_knowledge = format!(
        "{}\n\nVERIFIED SELLER CONTEXT (may validate Andrew's own evidence or give-back; never treat it as a fact about the recipient):\n{}",
        knowledge.council.block, seller_context
    );

    person_progress("checking deterministic copy rules");
    let mut reviews = review_and_edit_sequence_lean(
        client,
        pb,
        shared,
        account,
        &copy_contact(person),
        &mut sequence,
        n,
        critique,
        &reviewer_knowledge,
        Some(&person_progress),
        Some(db),
        &person.id,
    )
    .await?;
    if critique && sales_council_enabled(lean) {
        reviews = satisfy_sales_council(
            client,
            pb,
            shared,
            account,
            &copy_contact(person),
            &mut sequence,
            reviews,
            n,
            &council_knowledge,
            &shared.personas.critics,
            Some(&person_progress),
        )
        .await?;
    }
    person_progress("running final sendability gate");
    scrub_ai_punctuation(&mut sequence);
    let issues =
        account_sequence_quality_issues(pb, shared, account, &sequence, &reviews, n, critique);
    if !issues.is_empty() {
        return Err(anyhow!("sendability gate: {}", issues.join("; ")));
    }
    checkpoint_sequence_copy(db, sequence_id, pb, lead, person, &sequence, &reviews)?;
    person_progress("copy accepted");
    Ok(ReviewedCopy { sequence, reviews })
}

/// The ten-lens council is useful as an explicit audit, not as a production
/// authoring loop. Unanimity across ten simulated viewpoints rewards bland
/// compromise, burns context, and repeatedly strips the sender's voice. The
/// normal path uses one skeptical-recipient verifier; opt in to the council
/// only when deliberately comparing lenses.
fn sales_council_enabled(_lean_api: bool) -> bool {
    match std::env::var("SPRUCE_SALES_COUNCIL") {
        Ok(value) => {
            let value = value.trim().to_ascii_lowercase();
            matches!(value.as_str(), "1" | "true" | "on" | "yes")
        }
        Err(_) => false,
    }
}

/// Autoresearch-style copy loop. The rubric and deterministic envelope stay
/// locked while the sequence is the editable candidate. Every failed audit is
/// appended to the event ledger and compiled into a reusable brand rule; each
/// repair produces a new candidate. Two consecutive clean independent audits
/// are the convergence rule. A hard model-call budget prevents subjective QA
/// from becoming an unbounded token sink.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn review_and_edit_sequence_lean(
    client: &Engine,
    pb: &Playbook,
    shared: &Shared,
    account: &CopyAccount,
    contact: &CopyContact,
    sequence: &mut CopySequence,
    expected_touches: usize,
    critique: bool,
    knowledge: &str,
    progress: Option<&(dyn Fn(&str) + Send + Sync)>,
    research_db: Option<&SharedDb>,
    research_person_id: &str,
) -> Result<Vec<TouchReview>> {
    scrub_ai_punctuation(sequence);
    enforce_email_signatures(sequence, &pb.signature);
    let mut deterministic = account_sequence_quality_issues(
        pb,
        shared,
        account,
        sequence,
        &[],
        expected_touches,
        false,
    );
    if !critique {
        if deterministic.is_empty() {
            return Ok(Vec::new());
        }
        return Err(anyhow!(
            "deterministic QA failed: {}",
            deterministic.join("; ")
        ));
    }

    let review_system = pb.review_system_prompt(shared);
    let max_qa_calls = std::env::var("SPRUCE_COPY_RESEARCH_MAX_QA_CALLS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(8)
        .clamp(4, 16);
    let mut qa_calls = 0usize;
    let mut research_round = 0usize;

    if !deterministic.is_empty() {
        research_round += 1;
        record_copy_research_attempt(
            research_db,
            pb,
            research_person_id,
            sequence,
            research_round,
            "mechanical_findings",
            &deterministic,
        );
        for round in 0..2 {
            if qa_calls >= max_qa_calls {
                return Err(anyhow!(
                    "copy research budget exhausted before mechanical convergence: {}",
                    deterministic.join("; ")
                ));
            }
            report_review_progress(
                progress,
                if round == 0 {
                    "repairing mechanical copy findings".to_string()
                } else {
                    "retrying exact mechanical findings · final recovery".to_string()
                },
            );
            let repair = request_copy_review(
                client,
                &review_system,
                pb,
                account,
                contact,
                sequence,
                &deterministic,
                expected_touches,
                false,
                knowledge,
            )
            .await?;
            qa_calls += 1;
            validate_editor_stages(&repair, sequence)?;
            let apply_error =
                apply_targeted_repairs(sequence, &repair, &deterministic, expected_touches)
                    .err()
                    .map(|error| error.to_string());
            scrub_ai_punctuation(sequence);
            enforce_email_signatures(sequence, &pb.signature);
            deterministic = account_sequence_quality_issues(
                pb,
                shared,
                account,
                sequence,
                &[],
                expected_touches,
                false,
            );
            if let Some(error) = apply_error {
                deterministic.push(error);
            }
            deterministic.sort();
            deterministic.dedup();
            if deterministic.is_empty() {
                break;
            }
            if round == 1 {
                research_round += 1;
                record_copy_research_attempt(
                    research_db,
                    pb,
                    research_person_id,
                    sequence,
                    research_round,
                    "mechanical_repair_failed",
                    &deterministic,
                );
                return Err(anyhow!(
                    "copy could not clear mechanical QA after one recovery: {}",
                    deterministic.join("; ")
                ));
            }
        }
    }

    let mut consecutive_clean_audits = 0usize;
    let mut last_findings = Vec::<String>::new();
    while qa_calls < max_qa_calls {
        let novelty_audit = consecutive_clean_audits == 1;
        report_review_progress(
            progress,
            if novelty_audit {
                "running second independent no-new-problems audit"
            } else {
                "running independent research audit"
            },
        );
        let audit_knowledge = if novelty_audit {
            format!(
                "{knowledge}\n\nSECOND-PASS NOVELTY AUDIT: a prior independent audit found no actionable defect. Try to falsify that clean result by looking for one material issue it may have missed in recognition, consequence, evidence, role fit, reply cost, sequence progression, or natural voice. Do not invent a nitpick. Return clean only if the exact copy is genuinely ready unchanged."
            )
        } else {
            knowledge.to_string()
        };
        let verification = request_copy_review_full(
            client,
            &review_system,
            pb,
            account,
            contact,
            sequence,
            &[],
            expected_touches,
            true,
            &audit_knowledge,
        )
        .await?;
        qa_calls += 1;
        research_round += 1;
        validate_editor_stages(&verification, sequence)?;
        let unresolved = verification_findings(&verification);
        if unresolved.is_empty() {
            consecutive_clean_audits += 1;
            record_copy_research_attempt(
                research_db,
                pb,
                research_person_id,
                sequence,
                research_round,
                if consecutive_clean_audits >= 2 {
                    "converged"
                } else {
                    "clean_audit"
                },
                &[],
            );
            if consecutive_clean_audits >= 2 {
                report_review_progress(progress, "copy research converged · two clean audits");
                return Ok(approved_reviews(verification));
            }
            continue;
        }

        consecutive_clean_audits = 0;
        last_findings = unresolved.clone();
        record_copy_research_attempt(
            research_db,
            pb,
            research_person_id,
            sequence,
            research_round,
            "audit_findings",
            &unresolved,
        );
        if qa_calls >= max_qa_calls {
            break;
        }

        report_review_progress(
            progress,
            format!(
                "research round {research_round} found {} issue(s) · regenerating candidate",
                unresolved.len()
            ),
        );
        let repair = request_copy_review_full(
            client,
            &review_system,
            pb,
            account,
            contact,
            sequence,
            &unresolved,
            expected_touches,
            false,
            knowledge,
        )
        .await?;
        qa_calls += 1;
        validate_editor_stages(&repair, sequence)?;
        apply_targeted_repairs(sequence, &repair, &unresolved, expected_touches)?;
        scrub_ai_punctuation(sequence);
        enforce_email_signatures(sequence, &pb.signature);

        let repair_grade = review_grade_findings(&repair);
        if !repair_grade.is_empty() {
            research_round += 1;
            record_copy_research_attempt(
                research_db,
                pb,
                research_person_id,
                sequence,
                research_round,
                "repair_self_grade_findings",
                &repair_grade,
            );
        }

        let mut mechanical_rounds = 0usize;
        loop {
            deterministic = account_sequence_quality_issues(
                pb,
                shared,
                account,
                sequence,
                &[],
                expected_touches,
                false,
            );
            if deterministic.is_empty() {
                break;
            }
            research_round += 1;
            record_copy_research_attempt(
                research_db,
                pb,
                research_person_id,
                sequence,
                research_round,
                "repair_introduced_mechanical_findings",
                &deterministic,
            );
            if qa_calls >= max_qa_calls || mechanical_rounds >= 2 {
                last_findings = deterministic.clone();
                break;
            }
            mechanical_rounds += 1;
            report_review_progress(
                progress,
                "repair introduced exact findings · regenerating named stages",
            );
            let cleanup = request_copy_review(
                client,
                &review_system,
                pb,
                account,
                contact,
                sequence,
                &deterministic,
                expected_touches,
                false,
                knowledge,
            )
            .await?;
            qa_calls += 1;
            validate_editor_stages(&cleanup, sequence)?;
            apply_targeted_repairs(sequence, &cleanup, &deterministic, expected_touches)?;
            scrub_ai_punctuation(sequence);
            enforce_email_signatures(sequence, &pb.signature);
        }
        if !deterministic.is_empty() {
            break;
        }
    }

    let reason = if last_findings.is_empty() {
        "the fixed copy-research budget ended before two consecutive independent clean audits"
            .to_string()
    } else {
        format!(
            "the fixed copy-research budget ended with unresolved findings: {}",
            last_findings.join(" | ")
        )
    };
    Err(anyhow!("copy research did not converge: {reason}"))
}

fn record_copy_research_attempt(
    db: Option<&SharedDb>,
    pb: &Playbook,
    person_id: &str,
    sequence: &CopySequence,
    round: usize,
    status: &str,
    findings: &[String],
) {
    let Some(db) = db else {
        return;
    };
    let candidate = serde_json::to_string(&sequence.touches).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    candidate.hash(&mut hasher);
    let detail = json!({
        "copy_policy_version": CURRENT_COPY_POLICY_VERSION,
        "round": round,
        "candidate_hash": format!("{:016x}", hasher.finish()),
        "candidate": sequence.touches.iter().map(|touch| json!({
            "stage": touch.stage,
            "subject": touch.subject,
            "body": touch.body,
        })).collect::<Vec<_>>(),
        "status": status,
        "findings": findings,
    });
    let _ = db.log_event(
        &pb.key,
        person_id,
        "",
        "copy_research_attempt",
        &detail.to_string(),
    );
    for finding in findings {
        let (key, subject) = copy_research_rule(finding);
        let compact = finding.chars().take(600).collect::<String>();
        let kind = format!("copy_research_rule_v{CURRENT_COPY_POLICY_VERSION}");
        let _ = db.record_learning(&pb.key, &kind, &subject, &key, &compact);
    }
}

fn copy_research_rule(finding: &str) -> (String, String) {
    let finding = finding.to_ascii_lowercase();
    let category = if [
        "invent",
        "unsupported",
        "unverified",
        "evidence",
        "not a fact",
        "premise",
    ]
    .iter()
    .any(|term| finding.contains(term))
    {
        Some((
            "evidence_before_copy",
            "Require problem evidence, not company fit",
        ))
    } else if [
        "reason to answer",
        "easy to answer",
        "reply",
        "recipient value",
        "recognizable",
        "develop the opportunity",
    ]
    .iter()
    .any(|term| finding.contains(term))
    {
        Some((
            "reply_likelihood",
            "Lower the recipient's work and raise recognition",
        ))
    } else if [
        "consequence",
        "economic",
        "material",
        "why it matters",
        "stakes",
    ]
    .iter()
    .any(|term| finding.contains(term))
    {
        Some((
            "consequence_missing",
            "Name a believable operating consequence",
        ))
    } else if [
        "repeat",
        "restat",
        "follow-up",
        "follow up",
        "sequence progression",
        "same argument",
    ]
    .iter()
    .any(|term| finding.contains(term))
    {
        Some((
            "sequence_progression",
            "Every follow-up must add something new",
        ))
    } else if [
        "internal memo",
        "generated",
        "abstract",
        "jargon",
        "unnatural",
        "framework",
    ]
    .iter()
    .any(|term| finding.contains(term))
    {
        Some((
            "natural_voice",
            "Replace research language with operator language",
        ))
    } else if ["recipient", "role fit", "wrong contact", "vantage", "owner"]
        .iter()
        .any(|term| finding.contains(term))
    {
        Some((
            "role_proximity",
            "Use a recipient close enough to the decision",
        ))
    } else if [
        "historical result",
        "historical match",
        "timestamp",
        "location-specific",
    ]
    .iter()
    .any(|term| finding.contains(term))
    {
        Some((
            "proof_before_followup",
            "Do not promise evidence before it exists",
        ))
    } else if [
        "word",
        "question",
        "subject",
        "signature",
        "day offset",
        "channel",
        "forbidden phrase",
    ]
    .iter()
    .any(|term| finding.contains(term))
    {
        Some((
            "mechanical_sendability",
            "Clear the exact sendability envelope",
        ))
    } else {
        None
    };
    if let Some((key, subject)) = category {
        return (key.to_string(), subject.to_string());
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    finding.hash(&mut hasher);
    (
        format!("provisional_{:016x}", hasher.finish()),
        "Provisional independent-review finding".into(),
    )
}

fn verification_findings(doc: &EditDoc) -> Vec<String> {
    let mut findings = Vec::new();
    if !doc.sequence_passes || doc.sequence_score < 85 || !doc.sequence_issues.is_empty() {
        findings.push(format!(
            "sequence final-verifier feedback (score {}): {}",
            doc.sequence_score,
            if doc.sequence_issues.is_empty() {
                "the campaign is not ready to send as one coherent thread".to_string()
            } else {
                doc.sequence_issues.join("; ")
            }
        ));
    }
    findings.extend(
        doc.reviews
            .iter()
            .filter(|edit| {
                !edit.passes
                    || edit.score < 85
                    || !edit.issues.is_empty()
                    || !edit.revised_subject.trim().is_empty()
                    || !edit.revised_body.trim().is_empty()
            })
            .map(|edit| {
                let detail = if edit.issues.is_empty() {
                    "not ready to send unchanged".to_string()
                } else {
                    edit.issues.join("; ")
                };
                format!(
                    "stage {} final-verifier feedback (score {}): {}",
                    edit.stage, edit.score, detail
                )
            })
            .collect::<Vec<_>>(),
    );
    findings
}

/// Grade the wording returned by an editor. Revised fields are expected here;
/// only its explicit pass/score/issues assessment can keep the repair blocked.
fn review_grade_findings(doc: &EditDoc) -> Vec<String> {
    let mut findings = Vec::new();
    if !doc.sequence_passes || doc.sequence_score < 85 || !doc.sequence_issues.is_empty() {
        findings.push(format!(
            "sequence repair grade (score {}): {}",
            doc.sequence_score,
            if doc.sequence_issues.is_empty() {
                "the repaired sequence is not ready to send".to_string()
            } else {
                doc.sequence_issues.join("; ")
            }
        ));
    }
    findings.extend(
        doc.reviews
            .iter()
            .filter(|edit| !edit.passes || edit.score < 85 || !edit.issues.is_empty())
            .map(|edit| {
                let detail = if edit.issues.is_empty() {
                    "not ready to send unchanged".to_string()
                } else {
                    edit.issues.join("; ")
                };
                format!(
                    "stage {} repair grade (score {}): {}",
                    edit.stage, edit.score, detail
                )
            })
            .collect::<Vec<_>>(),
    );
    findings
}

fn approved_reviews(doc: EditDoc) -> Vec<TouchReview> {
    doc.reviews
        .into_iter()
        .map(|edit| TouchReview {
            stage: edit.stage,
            passes: true,
            score: edit.score,
            issues: Vec::new(),
        })
        .collect()
}

fn apply_targeted_repairs(
    sequence: &mut CopySequence,
    doc: &EditDoc,
    findings: &[String],
    expected_touches: usize,
) -> Result<()> {
    let affected = findings
        .iter()
        .flat_map(|finding| affected_stages(finding, expected_touches))
        .collect::<HashSet<_>>();
    let sequence_level = affected.is_empty();

    // Validate the response before mutating any touch. Previously, an editor
    // could update stage 1 and omit stage 2, leaving a half-applied sequence in
    // memory before the retry/rejection path ran.
    if !sequence_level {
        for stage in &affected {
            let edit = doc
                .reviews
                .iter()
                .find(|review| review.stage == *stage)
                .expect("validated editor stages");
            if edit.revised_body.trim().is_empty() {
                return Err(anyhow!(
                    "stage {} copy editor omitted the required rewrite",
                    stage
                ));
            }
        }
    }

    let mut changed = false;
    for touch in &mut sequence.touches {
        if !sequence_level && !affected.contains(&touch.stage) {
            continue;
        }
        let edit = doc
            .reviews
            .iter()
            .find(|review| review.stage == touch.stage)
            .expect("validated editor stages");
        let body = edit.revised_body.trim();
        let subject = edit.revised_subject.trim();
        if !body.is_empty() {
            touch.body = edit.revised_body.clone();
            changed = true;
        }
        if is_email_capable_channel(&touch.channel) && !subject.is_empty() {
            touch.subject = edit.revised_subject.clone();
            changed = true;
        }
    }
    if !changed {
        return Err(anyhow!("copy editor returned no usable repair"));
    }
    Ok(())
}

fn copy_account(lead: &crate::db::Lead) -> CopyAccount {
    CopyAccount {
        name: lead.name.clone(),
        industry: lead.industry.clone(),
        hq: lead.hq.clone(),
        observed_facts: lead.observed_facts.clone(),
        inferences: lead.inferences.clone(),
        hypothesis: lead.hypothesis.clone(),
        mechanism: lead.mechanism.clone(),
        consequence_metric: lead.consequence_metric.clone(),
        signals: lead.signals.clone(),
        system_concept: lead.system_concept.clone(),
        hard_buyer_question: lead.hard_buyer_question.clone(),
        kill_condition: lead.kill_condition.clone(),
        magnitude_note: lead.magnitude_note.clone(),
        applied_principles: lead.applied_principles.clone(),
    }
}

/// The planner needs the commercial question and the strongest objection, but
/// not the internal magnitude memo or a speculative implementation recipe.
/// Keeping those fields out is more reliable than asking a model not to leak
/// them after they have already been placed beside the copy request.
fn planner_account_brief(account: &CopyAccount) -> Value {
    json!({
        "company": account.name,
        "industry": account.industry,
        "location": account.hq,
        "verified_facts": account.observed_facts.iter().take(3).collect::<Vec<_>>(),
        "internal_hypotheses_not_for_verbatim_copy": {
            "question_to_test": account.hypothesis,
            "why_it_might_be_hard": account.mechanism,
            "measurable_consequence_to_ask_about": account.consequence_metric,
            "strongest_objection": account.hard_buyer_question,
            "reason_to_stop": account.kill_condition,
        }
    })
}

/// The writer gets the evidence-safe parts of the internal thesis. Earlier this
/// brief omitted the mechanism, concrete system concept, and buyer objection;
/// that prevented unsupported claims but also reduced substantive emails to
/// polite correction blurbs. These fields are now supplied under explicit
/// not-fact / not-first-touch labels. Magnitude notes and inferred integrations
/// remain excluded.
fn writer_account_brief(account: &CopyAccount) -> Value {
    json!({
        "company": account.name,
        "industry": account.industry,
        "location": account.hq,
        "verified_facts": account.observed_facts.iter().take(3).collect::<Vec<_>>(),
        "research_notes_not_verified_and_never_declarative": account.signals.iter().take(2).collect::<Vec<_>>(),
        "question_to_test_not_a_fact": account.hypothesis,
        "mechanism_to_test_not_a_fact": account.mechanism,
        "plain_reason_it_might_matter_not_a_fact": account.consequence_metric,
        "concrete_system_if_confirmed_not_an_account_fact": account.system_concept,
        "strongest_buyer_objection_to_answer": account.hard_buyer_question,
    })
}

fn copy_contact(person: &crate::db::Person) -> CopyContact {
    CopyContact {
        name: person.name.clone(),
        title: person.title.clone(),
        vantage: response_design::effective_vantage(&person.title, &person.vantage),
        can_observe: person.can_observe.clone(),
        why_them: person.why_them.clone(),
        primary: person.primary,
        // `route_to` is an internal CRM hypothesis and is frequently inferred
        // rather than source-backed. Never let a guessed coworker name leak
        // into buyer-facing copy or its semantic review context.
        route_to: String::new(),
    }
}

#[allow(dead_code, clippy::too_many_arguments)]
async fn review_and_edit_sequence_legacy(
    client: &Engine,
    pb: &Playbook,
    shared: &Shared,
    account: &CopyAccount,
    contact: &CopyContact,
    sequence: &mut CopySequence,
    expected_touches: usize,
    critique: bool,
    knowledge: &str,
    progress: Option<&(dyn Fn(&str) + Send + Sync)>,
) -> Result<Vec<TouchReview>> {
    // Normalize model-favoured punctuation before either deterministic or
    // semantic review so the reviewer sees the exact copy we may persist.
    report_review_progress(progress, "checking deterministic copy rules");
    scrub_ai_punctuation(sequence);
    enforce_email_signatures(sequence, &pb.signature);

    if !critique {
        let issues = sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
        if !issues.is_empty() {
            return Err(anyhow!("deterministic QA failed: {}", issues.join("; ")));
        }
        return Ok(Vec::new());
    }

    let deterministic = sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
    if let Some(global) = deterministic
        .iter()
        .find(|issue| affected_stages(issue, expected_touches).is_empty())
    {
        return Err(anyhow!("deterministic QA failed: {global}"));
    }
    let review_system = pb.review_system_prompt(shared);
    report_review_progress(
        progress,
        format!("semantic review of all {expected_touches} touches"),
    );
    let semantic = request_copy_review(
        client,
        &review_system,
        pb,
        account,
        contact,
        sequence,
        &deterministic,
        expected_touches,
        false,
        knowledge,
    )
    .await?;
    validate_editor_stages(&semantic, sequence)?;

    const MIN_SENDABILITY_SCORE: u32 = 85;
    let mut repaired = false;
    for touch in &mut sequence.touches {
        let edit = semantic
            .reviews
            .iter()
            .find(|review| review.stage == touch.stage)
            .expect("validated editor stages");
        let deterministic_for_stage = deterministic
            .iter()
            .filter(|issue| affected_stages(issue, expected_touches).contains(&touch.stage))
            .cloned()
            .collect::<Vec<_>>();
        let needs_edit = !edit.passes
            || edit.score < MIN_SENDABILITY_SCORE
            || !edit.issues.is_empty()
            || !deterministic_for_stage.is_empty();
        let offered_edit =
            !edit.revised_body.trim().is_empty() || !edit.revised_subject.trim().is_empty();
        if needs_edit || offered_edit {
            if edit.revised_body.trim().is_empty() {
                return Err(anyhow!(
                    "copy editor rejected stage {} without returning corrected body",
                    touch.stage
                ));
            }
            touch.body = edit.revised_body.clone();
            if is_email_capable_channel(&touch.channel) && !edit.revised_subject.trim().is_empty() {
                touch.subject = edit.revised_subject.clone();
            }
            repaired = true;
        }
    }
    scrub_ai_punctuation(sequence);
    enforce_email_signatures(sequence, &pb.signature);
    let mut after = sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
    for deterministic_round in 0..3 {
        if after.is_empty() {
            break;
        }
        report_review_progress(
            progress,
            format!(
                "repairing deterministic QA · round {}/3",
                deterministic_round + 1
            ),
        );
        let repair = request_copy_review(
            client,
            &review_system,
            pb,
            account,
            contact,
            sequence,
            &after,
            expected_touches,
            false,
            knowledge,
        )
        .await?;
        validate_editor_stages(&repair, sequence)?;
        for touch in &mut sequence.touches {
            if !after
                .iter()
                .any(|issue| affected_stages(issue, expected_touches).contains(&touch.stage))
            {
                continue;
            }
            let edit = repair
                .reviews
                .iter()
                .find(|review| review.stage == touch.stage)
                .expect("validated editor stages");
            if edit.revised_body.trim().is_empty() {
                return Err(anyhow!(
                    "copy editor could not repair deterministic findings at stage {}",
                    touch.stage
                ));
            }
            touch.body = edit.revised_body.clone();
            if is_email_capable_channel(&touch.channel) && !edit.revised_subject.trim().is_empty() {
                touch.subject = edit.revised_subject.clone();
            }
        }
        scrub_ai_punctuation(sequence);
        enforce_email_signatures(sequence, &pb.signature);
        after = sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
        if deterministic_round == 2 && !after.is_empty() {
            return Err(anyhow!(
                "copy editor failed deterministic QA after three repair rounds: {}",
                after.join("; ")
            ));
        }
    }

    // A repair is not approval. The old path force-set every repaired touch to
    // 85/100, which allowed exactly the awkward copy the editor had criticised
    // to appear sendable in the CRM. Verify the final text in a separate gate.
    let mut final_doc = if repaired {
        report_review_progress(progress, "verifying repaired copy");
        request_copy_review(
            client,
            &review_system,
            pb,
            account,
            contact,
            sequence,
            &[],
            expected_touches,
            true,
            knowledge,
        )
        .await?
    } else {
        semantic
    };
    validate_editor_stages(&final_doc, sequence)?;

    // Let the editor respond to the independent gate's concrete objections for
    // two bounded revisions. This is still agent-authored copy: Rust carries
    // feedback between roles and enforces limits, but supplies no fallback
    // wording and never changes a failing score into a pass.
    for final_round in 0..3 {
        let needs_repair = final_doc.reviews.iter().any(|edit| {
            !edit.passes
                || edit.score < MIN_SENDABILITY_SCORE
                || !edit.issues.is_empty()
                || !edit.revised_subject.trim().is_empty()
                || !edit.revised_body.trim().is_empty()
        });
        if !needs_repair || final_round == 2 {
            break;
        }
        let unresolved = final_doc
            .reviews
            .iter()
            .filter(|edit| {
                !edit.passes || edit.score < MIN_SENDABILITY_SCORE || !edit.issues.is_empty()
            })
            .map(|edit| {
                let reason = if edit.issues.is_empty() {
                    "not ready to send unchanged".to_string()
                } else {
                    edit.issues.join("; ")
                };
                format!("stage {} final-gate feedback: {reason}", edit.stage)
            })
            .collect::<Vec<_>>();
        report_review_progress(
            progress,
            format!("revising final-gate feedback · round {}/2", final_round + 1),
        );
        let repair = request_copy_review(
            client,
            &review_system,
            pb,
            account,
            contact,
            sequence,
            &unresolved,
            expected_touches,
            false,
            knowledge,
        )
        .await?;
        validate_editor_stages(&repair, sequence)?;
        for touch in &mut sequence.touches {
            let edit = repair
                .reviews
                .iter()
                .find(|review| review.stage == touch.stage)
                .expect("validated editor stages");
            let needs_edit = !edit.passes
                || edit.score < MIN_SENDABILITY_SCORE
                || !edit.issues.is_empty()
                || unresolved
                    .iter()
                    .any(|issue| affected_stages(issue, expected_touches).contains(&touch.stage));
            let offered_edit =
                !edit.revised_body.trim().is_empty() || !edit.revised_subject.trim().is_empty();
            if needs_edit || offered_edit {
                if edit.revised_body.trim().is_empty() {
                    return Err(anyhow!(
                        "copy editor could not repair stage {} after final-gate feedback",
                        touch.stage
                    ));
                }
                touch.body = edit.revised_body.clone();
                if is_email_capable_channel(&touch.channel)
                    && !edit.revised_subject.trim().is_empty()
                {
                    touch.subject = edit.revised_subject.clone();
                }
            }
        }
        scrub_ai_punctuation(sequence);
        enforce_email_signatures(sequence, &pb.signature);
        let after_retry =
            sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
        if !after_retry.is_empty() {
            return Err(anyhow!(
                "copy editor retry failed deterministic QA: {}",
                after_retry.join("; ")
            ));
        }
        report_review_progress(
            progress,
            format!("rechecking final copy · round {}/2", final_round + 1),
        );
        final_doc = request_copy_review(
            client,
            &review_system,
            pb,
            account,
            contact,
            sequence,
            &[],
            expected_touches,
            true,
            knowledge,
        )
        .await?;
        validate_editor_stages(&final_doc, sequence)?;
    }

    let mut reviews = Vec::with_capacity(sequence.touches.len());
    for edit in final_doc.reviews {
        if !edit.passes
            || edit.score < MIN_SENDABILITY_SCORE
            || !edit.issues.is_empty()
            || !edit.revised_subject.trim().is_empty()
            || !edit.revised_body.trim().is_empty()
        {
            let issues = if edit.issues.is_empty() {
                "did not clear the final human-sendability gate".to_string()
            } else {
                edit.issues.join("; ")
            };
            return Err(anyhow!(
                "stage {} rejected by final copy gate (score {}): {}",
                edit.stage,
                edit.score,
                issues
            ));
        }
        reviews.push(TouchReview {
            stage: edit.stage,
            passes: true,
            score: edit.score,
            issues: Vec::new(),
        });
    }
    report_review_progress(progress, "copy QA passed");
    Ok(reviews)
}

#[allow(clippy::too_many_arguments)]
async fn satisfy_sales_council(
    client: &Engine,
    pb: &Playbook,
    shared: &Shared,
    account: &CopyAccount,
    contact: &CopyContact,
    sequence: &mut CopySequence,
    mut reviews: Vec<TouchReview>,
    expected_touches: usize,
    knowledge: &str,
    critics: &[SalesCriticPersona],
    progress: Option<&(dyn Fn(&str) + Send + Sync)>,
) -> Result<Vec<TouchReview>> {
    const REQUIRED_CRITICS: usize = 10;
    const MIN_SCORE: u32 = 85;
    const MAX_COUNCIL_ROUNDS: usize = 3;

    if critics.len() != REQUIRED_CRITICS {
        return Err(anyhow!(
            "expected {REQUIRED_CRITICS} configured critics, found {}",
            critics.len()
        ));
    }
    let email_stages = sequence
        .touches
        .iter()
        .filter(|touch| is_email_capable_channel(&touch.channel))
        .map(|touch| touch.stage)
        .collect::<HashSet<_>>();
    if email_stages.is_empty() {
        return Ok(reviews);
    }

    for round in 0..MAX_COUNCIL_ROUNDS {
        report_review_progress(
            progress,
            format!(
                "sales council vote ({} critics) · round {}/{}",
                critics.len(),
                round + 1,
                MAX_COUNCIL_ROUNDS
            ),
        );
        let doc = request_sales_council(client, pb, account, contact, sequence, knowledge, critics)
            .await?;
        validate_sales_council(&doc, critics, &email_stages)?;

        let mut feedback = Vec::new();
        let mut failing_stages = HashSet::new();
        for critic in &doc.critics {
            for touch in &critic.touches {
                if !touch.passes || touch.score < MIN_SCORE || !touch.issues.is_empty() {
                    failing_stages.insert(touch.stage);
                    let reason = if touch.issues.is_empty() {
                        "not ready to send unchanged".to_string()
                    } else {
                        touch.issues.join("; ")
                    };
                    let recommendation = touch.recommendation.trim();
                    feedback.push(if recommendation.is_empty() {
                        format!(
                            "{} on stage {} (score {}): {reason}",
                            critic.critic_id, touch.stage, touch.score
                        )
                    } else {
                        format!(
                            "{} on stage {} (score {}): {reason}. Direction: {recommendation}",
                            critic.critic_id, touch.stage, touch.score
                        )
                    });
                }
            }
        }

        if feedback.is_empty() {
            for review in &mut reviews {
                if !email_stages.contains(&review.stage) {
                    continue;
                }
                let council_floor = doc
                    .critics
                    .iter()
                    .filter_map(|critic| {
                        critic
                            .touches
                            .iter()
                            .find(|touch| touch.stage == review.stage)
                            .map(|touch| touch.score)
                    })
                    .min()
                    .unwrap_or(MIN_SCORE);
                review.score = review.score.min(council_floor);
                review.passes = true;
                review.issues.clear();
            }
            report_review_progress(progress, "sales council approved");
            return Ok(reviews);
        }

        if round + 1 == MAX_COUNCIL_ROUNDS {
            return Err(anyhow!(
                "no unanimous approval after {MAX_COUNCIL_ROUNDS} rounds: {}",
                feedback.join(" | ")
            ));
        }

        report_review_progress(
            progress,
            format!("revising sales council feedback · round {}/2", round + 1),
        );
        let repair = request_copy_review_full(
            client,
            &pb.review_system_prompt(shared),
            pb,
            account,
            contact,
            sequence,
            &feedback,
            expected_touches,
            false,
            knowledge,
        )
        .await?;
        validate_editor_stages(&repair, sequence)?;
        for touch in &mut sequence.touches {
            if !failing_stages.contains(&touch.stage) {
                continue;
            }
            let edit = repair
                .reviews
                .iter()
                .find(|review| review.stage == touch.stage)
                .expect("validated editor stages");
            if edit.revised_body.trim().is_empty() {
                return Err(anyhow!(
                    "editor returned no council-driven repair for stage {}",
                    touch.stage
                ));
            }
            touch.body = edit.revised_body.clone();
            if is_email_capable_channel(&touch.channel) && !edit.revised_subject.trim().is_empty() {
                touch.subject = edit.revised_subject.clone();
            }
        }
        scrub_ai_punctuation(sequence);
        enforce_email_signatures(sequence, &pb.signature);
        let mut deterministic =
            sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
        for cleanup_round in 0..2 {
            if deterministic.is_empty() {
                break;
            }
            report_review_progress(
                progress,
                format!("cleaning council rewrite · round {}/2", cleanup_round + 1),
            );
            let cleanup = request_copy_review_full(
                client,
                &pb.review_system_prompt(shared),
                pb,
                account,
                contact,
                sequence,
                &deterministic,
                expected_touches,
                false,
                knowledge,
            )
            .await?;
            validate_editor_stages(&cleanup, sequence)?;
            let affected = deterministic
                .iter()
                .flat_map(|finding| affected_stages(finding, expected_touches))
                .collect::<HashSet<_>>();
            if affected.is_empty() {
                return Err(anyhow!(
                    "council rewrite produced a sequence-level QA failure: {}",
                    deterministic.join("; ")
                ));
            }
            for touch in &mut sequence.touches {
                if !affected.contains(&touch.stage) {
                    continue;
                }
                let edit = cleanup
                    .reviews
                    .iter()
                    .find(|review| review.stage == touch.stage)
                    .expect("validated editor stages");
                if edit.revised_body.trim().is_empty() {
                    return Err(anyhow!(
                        "copy editor omitted council cleanup for stage {}",
                        touch.stage
                    ));
                }
                touch.body = edit.revised_body.clone();
                if is_email_capable_channel(&touch.channel)
                    && !edit.revised_subject.trim().is_empty()
                {
                    touch.subject = edit.revised_subject.clone();
                }
            }
            scrub_ai_punctuation(sequence);
            enforce_email_signatures(sequence, &pb.signature);
            deterministic =
                sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
        }
        if !deterministic.is_empty() {
            return Err(anyhow!(
                "council rewrite still failed deterministic QA after cleanup: {}",
                deterministic.join("; ")
            ));
        }
    }
    Err(anyhow!("sales council ended without a verdict"))
}

async fn request_sales_council(
    client: &Engine,
    pb: &Playbook,
    account: &CopyAccount,
    contact: &CopyContact,
    sequence: &CopySequence,
    knowledge: &str,
    critics: &[SalesCriticPersona],
) -> Result<CouncilDoc> {
    let critic_prompts = critics
        .iter()
        .map(|critic| {
            format!(
                "=== CRITIC ID: {} | {} ===\n{}",
                critic.id,
                critic.name,
                critic.prompt.trim()
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let system = format!(
        "You moderate a pre-send sales council. Apply every configured analytical lens independently; do not average them into one generic opinion and do not imitate or speak as the named real people. Each critic grades the CURRENT wording of every email. Correct, grammatical, and non-offensive is not enough: the first touch needs a plausible self-interested reason to open, keep reading, and answer a stranger. Grade the subject before reading the body. A topical label such as `utility status`, `power alarms`, `claim evidence`, or `decision trail` fails even when accurate; it creates no specific knowledge gap. The subject must plainly name the operating event, decision, object, or consequence that makes this exact note distinct. Calibrate later touches to their job: a reply-thread follow-up may contribute a concrete useful contrast without another CTA; a routing or close touch may be brief. Do not demand a separate offer in every touch. Default to rejection when the sequence feels like automated account research, a multi-part interview, generic theory, or seller curiosity disguised as relevance. Passing means score >= 85, passes=true, and no unresolved issues. Unanimous approval is intentionally difficult. A critic may disagree with another. Be demanding but evidence-bound. Return only the requested structured data.\n\n{critic_prompts}"
    );
    let emails = sequence
        .touches
        .iter()
        .filter(|touch| is_email_capable_channel(&touch.channel))
        .collect::<Vec<_>>();
    let user = format!(
        "Review every current email under every critic lens. This is a vote, not an editing task: recommendations diagnose the smallest needed change but never provide canned replacement copy.\n\nREQUIRED SIGNATURE: {signature}\nVERIFIED ACCOUNT FACTS: {facts}\nQUESTION TO TEST (NOT A FACT): {hypothesis}\nRECIPIENT: {name} ({title}, {vantage})\nROLE RESPONSE CONTRACT (private; shape the judgment, never state inferred motives as facts):\n{role_contract}\n\nCURRENT EMAILS:\n{emails}\n\nRETRIEVED BOOK AND SKILL KNOWLEDGE:\n{knowledge}",
        signature = pb.signature,
        facts = account.observed_facts.join(" | "),
        hypothesis = account.hypothesis,
        name = contact.name,
        title = contact.title,
        vantage = contact.vantage,
        role_contract = response_design::for_title_and_vantage(&contact.title, &contact.vantage)
            .prompt_block(),
        emails = serde_json::to_string_pretty(&emails).unwrap_or_default(),
        knowledge = knowledge,
    );
    client
        .structured_bulk::<CouncilDoc>(
            "outreach.sales_council",
            &system,
            &user,
            sales_council_schema(critics, emails.len()),
        )
        .await
}

fn validate_sales_council(
    doc: &CouncilDoc,
    critics: &[SalesCriticPersona],
    email_stages: &HashSet<u32>,
) -> Result<()> {
    let expected_critics = critics
        .iter()
        .map(|critic| critic.id.as_str())
        .collect::<HashSet<_>>();
    let returned_critics = doc
        .critics
        .iter()
        .map(|critic| critic.critic_id.as_str())
        .collect::<HashSet<_>>();
    if doc.critics.len() != critics.len() || returned_critics != expected_critics {
        return Err(anyhow!(
            "council returned the wrong critic set: expected {expected_critics:?}, got {returned_critics:?}"
        ));
    }
    for critic in &doc.critics {
        let returned_stages = critic
            .touches
            .iter()
            .map(|touch| touch.stage)
            .collect::<HashSet<_>>();
        if critic.touches.len() != email_stages.len() || returned_stages != *email_stages {
            return Err(anyhow!(
                "critic {} returned the wrong email stages: expected {email_stages:?}, got {returned_stages:?}",
                critic.critic_id
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn request_copy_review(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    account: &CopyAccount,
    contact: &CopyContact,
    sequence: &CopySequence,
    deterministic: &[String],
    expected_touches: usize,
    verify_only: bool,
    knowledge: &str,
) -> Result<EditDoc> {
    request_copy_review_with_tier(
        client,
        system,
        pb,
        account,
        contact,
        sequence,
        deterministic,
        expected_touches,
        verify_only,
        knowledge,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn request_copy_review_full(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    account: &CopyAccount,
    contact: &CopyContact,
    sequence: &CopySequence,
    deterministic: &[String],
    expected_touches: usize,
    verify_only: bool,
    knowledge: &str,
) -> Result<EditDoc> {
    request_copy_review_with_tier(
        client,
        system,
        pb,
        account,
        contact,
        sequence,
        deterministic,
        expected_touches,
        verify_only,
        knowledge,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn request_copy_review_with_tier(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    account: &CopyAccount,
    contact: &CopyContact,
    sequence: &CopySequence,
    deterministic: &[String],
    expected_touches: usize,
    verify_only: bool,
    knowledge: &str,
    prefer_economy: bool,
) -> Result<EditDoc> {
    let verified_facts = account
        .observed_facts
        .iter()
        .take(8)
        .cloned()
        .collect::<Vec<_>>()
        .join(" | ");
    let task = if verify_only {
        "This is a final gate over already-repaired copy. Do not edit it. Return empty revised fields. Mark passes=true only when the CURRENT touch is natural, accurate, easy to answer, and ready for Andrew to send unchanged. List only unresolved issues."
    } else if !deterministic.is_empty() {
        "Repair the named findings as hard constraints. Any feedback string containing 'stage N' names stage N. Change only named stages unless a finding applies to the whole sequence. For EVERY named stage you MUST return a nonempty complete corrected body, even if you disagree with the feedback; never mark a named stage passed with an empty revised body. If a finding names an unverified task or object, remove that noun from every named stage and use only a supported category such as product, package, packing, or handling; do not preserve specificity merely because it appears in the hypothesis. Return a complete corrected subject only when the feedback concerns the subject; an empty revised_subject preserves an already-good subject. Count the corrected stage's words and question marks before returning: it must fall inside every stated range. Stage 1 may contain one operating question plus one short CTA question; stages 2 through 6 may contain at most one question mark, and stage 7 must contain none. Preserve verified facts, natural phrasing, and unnamed stages. The passes flag and score must grade the FINAL corrected wording you return. List only issues still present after your correction."
    } else {
        "Review and repair the copy. For every touch that is not ready to send, return a complete corrected body and, for email, a complete corrected subject. Stage 1 may contain one operating question plus one short CTA question; stages 2 through 6 may contain at most one question mark, and the final close contains none. The passes flag and score must grade the FINAL corrected wording you return, not the original. List only issues that remain unresolved after your correction. If it cannot be fixed without inventing facts, mark it failed."
    };
    let stage_contract = format!(
        "SCHEMA CONTRACT: first grade the entire sequence for coherence, relevance, repetition, and whether a sensible recipient has a reason to answer. Then return exactly one review object for every stage 1 through {expected_touches}, even when only a subset needs repair. A sequence passes only at 85+ with no unresolved sequence issues. For an unnamed stage that does not need editing, preserve it with empty revised fields."
    );
    let sendability_contract = "INDEPENDENT SENDABILITY: Judge the words as a skeptical recipient, not as a checker of the writer's requested structure. No generation template, diagnostic-question shape, or factual correction is presumptively sendable. T1 needs a verified trigger, recognizable problem, plausible consequence, concrete seller contribution, and a role-relevant reason to answer through a natural conversation or email path. Compare the actual T1 ask with the private desired response and operating decision in review knowledge. Fail or repair copy that replaces a specific supported decision with a broad workflow-interview question such as `what takes the most time`, `what happens today`, or `how do you handle this`. Curiosity is not recipient value. Never require or invent collateral. When verified seller context explicitly permits Andrew to apply an existing fit screen to this exact sourced task and return a free first-pass blocker view, that is a concrete recipient benefit; preserve it during repair and judge whether the wording makes the no-site-visit, rule-out value clear. Do not demand a second give-back. Later touches must add evidence, a useful distinction, an honest objection answer, route, or close rather than paraphrase.";
    let user = format!(
        "{task}\n\n{stage_contract}\n{sendability_contract}\nCHANNEL: linkedin_request has no subject; linkedin_or_email must work as either a DM or a complete email fallback. A LinkedIn request must give a concrete operating reason to connect, not praise the recipient's remit, background, perspective, or work.\nINBOX TEST: grade T1's subject and first two lines before the rest. A subject that merely labels the category (`utility status`, `power alarms`, `claim evidence`, `decision trail`, `automation question`) fails even if relevant. Require a plain 3-9 word phrase naming the operating event, decision, object, or consequence that makes this exact email worth opening. No clickbait. Reject internal-memo prose such as `make this decision consequential`, `the distinction I have in mind`, and `the practical difference is`; Andrew must be able to say every line naturally aloud.\nEVIDENCE: the verified facts below are exhaustive. The hypothesis is not fact. Never invent an internal event, system, practice, consequence, or ownership claim.\n\nSIGNATURE: {signature}\nVERIFIED FACTS: {facts}\nHYPOTHESIS, NOT FACT: {hypothesis}\nRECIPIENT: {name} ({title}, {vantage})\nLIKELY ACCESS, INTERNAL ONLY: {can_observe}\nROLE RESPONSE CONTRACT (private; test role fit, reply cost, and face risk without inventing personality):\n{role_contract}\nDETERMINISTIC FINDINGS: {deterministic}\n\nCURRENT SEQUENCE:\n{sequence}\n\nRELEVANT REVIEW KNOWLEDGE:\n{knowledge}",
        task = task,
        sendability_contract = sendability_contract,
        signature = pb.signature,
        facts = verified_facts,
        hypothesis = account.hypothesis,
        name = contact.name,
        title = contact.title,
        vantage = contact.vantage,
        can_observe = contact.can_observe,
        role_contract = response_design::for_title_and_vantage(&contact.title, &contact.vantage)
            .prompt_block(),
        deterministic = if deterministic.is_empty() {
            "none".to_string()
        } else {
            deterministic.join(" | ")
        },
        sequence = serde_json::to_string_pretty(&sequence.touches).unwrap_or_default(),
        knowledge = knowledge,
    );
    let stage = if verify_only {
        "outreach.verify_final"
    } else {
        "outreach.review_edit"
    };
    if client.prefers_lean_outreach() && prefer_economy {
        client
            .structured_economy_bulk::<EditDoc>(
                stage,
                system,
                &user,
                review_edit_schema(expected_touches),
            )
            .await
    } else {
        client
            .structured_bulk::<EditDoc>(stage, system, &user, review_edit_schema(expected_touches))
            .await
    }
}

fn validate_editor_stages(doc: &EditDoc, sequence: &CopySequence) -> Result<()> {
    let returned = doc
        .reviews
        .iter()
        .map(|review| review.stage)
        .collect::<HashSet<_>>();
    let expected = sequence
        .touches
        .iter()
        .map(|touch| touch.stage)
        .collect::<HashSet<_>>();
    if returned != expected || doc.reviews.len() != sequence.touches.len() {
        return Err(anyhow!(
            "copy editor returned invalid stages: expected {expected:?}, got {returned:?}"
        ));
    }
    Ok(())
}

fn business_copy_context(business: &BusinessProfile) -> String {
    let discovery = business
        .discovery_evidence
        .iter()
        .map(|call| {
            json!({
                "id": call.id,
                "segment": call.segment,
                "participant_context": call.participant_context,
                "evidence_level": call.evidence_level,
                "participant_reported_workflows": call.reported_workflows.iter().take(3).collect::<Vec<_>>(),
                "permitted_follow_up_angles": call.follow_up_angles.iter().take(3).collect::<Vec<_>>(),
                "evidence_boundaries": call.limits.iter().take(2).collect::<Vec<_>>(),
                "source_url": call.source_url,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&json!({
        "business": business.name,
        "summary": business.summary,
        // Keep the compact verified fact set in full. Wapahki's real one-page
        // fit screen follows the founder/context facts; truncating at five made
        // the reviewer incorrectly treat that give-back as invented.
        "proven_seller_facts": business.known_facts.iter().take(12).collect::<Vec<_>>(),
        "first_party_market_discovery": discovery,
        "hard_constraints": business.constraints.iter().take(5).collect::<Vec<_>>(),
        "instruction": format!(
            "Use this to represent {} accurately. Discovery calls are market-level seller evidence, never proof about the recipient. At most once in a sequence, a later follow-up may explicitly attribute one relevant call observation and ask whether it matches this person's real workflow. Never imply consensus, quote an estimate as fact, or dump goals, constraints, and strategy into the message.",
            business.name
        )
    }))
    .unwrap_or_default()
}

/// Feed real response outcomes back into future copy without turning a prior
/// prospect's words or account facts into a reusable template. Only sequences
/// that passed the current copy policy may teach the writer. The compact shape
/// metrics are enough to reveal which subject/length/ask patterns earned a
/// correction, referral, interest, or objection.
fn empirical_copy_context(db: &SharedDb, brand: &str) -> Result<String> {
    let outcomes = db.list_gtm_outcomes(Some(brand), 40)?;
    if outcomes.is_empty() {
        return Ok("No attributed human reply outcomes yet. Use the evidence and current copy doctrine; do not invent a winning pattern.".into());
    }
    let people = db.list_people(Some(brand), None)?;
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut examples = Vec::new();
    for outcome in outcomes {
        if !matches!(
            outcome.kind.as_str(),
            "positive_reply" | "correction" | "referral" | "not_now" | "objection" | "human_reply"
        ) {
            continue;
        }
        *counts.entry(outcome.kind.clone()).or_default() += 1;
        if examples.len() >= 6 || outcome.sequence_id.trim().is_empty() {
            continue;
        }
        let Some(sequence) = db.sequence_gtm_attribution(&outcome.sequence_id)? else {
            continue;
        };
        if sequence.copy_policy_version < CURRENT_COPY_POLICY_VERSION {
            continue;
        }
        let touches = db.list_touches_for_sequence(&outcome.sequence_id)?;
        let Some(first) = touches.iter().find(|touch| touch.stage == 1) else {
            continue;
        };
        let person = people.iter().find(|person| person.id == outcome.person_id);
        examples.push(json!({
            "outcome": outcome.kind,
            "recipient_vantage": person.map(|person| person.vantage.as_str()).unwrap_or("unknown"),
            "recipient_title": person.map(|person| person.title.as_str()).unwrap_or("unknown"),
            "subject": first.subject,
            "first_email_words": first.body.split_whitespace().count(),
            "first_email_questions": first.body.matches('?').count(),
            "private_email_job": first.purpose,
            "reply_learning": outcome.detail,
        }));
    }
    Ok(serde_json::to_string_pretty(&json!({
        "attributed_reply_counts": counts,
        "current_policy_examples": examples,
        "instruction": "Use aggregate patterns as evidence. Never reuse prior account facts, names, or exact wording. With fewer than 20 comparable outcomes, treat every pattern as provisional."
    }))?)
}

fn copy_research_rule_context(db: &SharedDb, brand: &str) -> Result<String> {
    let kind = format!("copy_research_rule_v{CURRENT_COPY_POLICY_VERSION}");
    let rules = db.recent_learnings(Some(brand), Some(&kind), 20)?;
    if rules.is_empty() {
        return Ok("OUTREACH RESEARCH RULES: no prior independent-review defects have been compiled for this copy policy yet.".into());
    }
    let rows = rules
        .into_iter()
        .map(|rule| {
            let maturity = if rule.hits >= 2 {
                "durable"
            } else {
                "provisional"
            };
            format!(
                "- [{maturity}; {} observations] {}: {}",
                rule.hits, rule.subject, rule.detail
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    Ok(format!(
        "OUTREACH RESEARCH RULES (apply durable rules; treat one-off findings as provisional and never paste their wording):\n{rows}"
    ))
}

fn person_matches(person: &crate::db::Person, filter: Option<&str>) -> bool {
    let Some(filter) = filter.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    person.id.eq_ignore_ascii_case(filter)
        || person.email.eq_ignore_ascii_case(filter)
        || person.name.eq_ignore_ascii_case(filter)
}

fn gtm_state_priority(state: &str) -> u8 {
    match state {
        "action_ready" => 0,
        "discovery_ready" => 1,
        _ => 2,
    }
}

fn select_people_for_planning(
    people: Vec<crate::db::Person>,
    per_account: usize,
) -> Vec<crate::db::Person> {
    let mut by_lead: HashMap<String, Vec<crate::db::Person>> = HashMap::new();
    for person in people {
        by_lead
            .entry(person.lead_id.clone())
            .or_default()
            .push(person);
    }
    let mut lead_ids = by_lead.keys().cloned().collect::<Vec<_>>();
    lead_ids.sort();
    let mut selected = Vec::new();
    for lead_id in lead_ids {
        let Some(mut candidates) = by_lead.remove(&lead_id) else {
            continue;
        };
        candidates.sort_by(|left, right| {
            response_design::contact_priority(&right.title, &right.vantage, right.primary)
                .cmp(&response_design::contact_priority(
                    &left.title,
                    &left.vantage,
                    left.primary,
                ))
                .then_with(|| left.name.cmp(&right.name))
        });
        selected.extend(candidates.into_iter().take(per_account.max(1)));
    }
    selected
}

fn normalize_principle_ids(ids: &[String], allowed: &[String]) -> Vec<String> {
    let allowed = allowed.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    ids.iter()
        .filter_map(|id| {
            let clean = id.trim().trim_start_matches('[').trim_end_matches(']');
            if allowed.contains(clean) && seen.insert(clean.to_string()) {
                Some(clean.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn enforce_email_signatures(sequence: &mut CopySequence, signature: &str) {
    for touch in &mut sequence.touches {
        if is_email_capable_channel(&touch.channel) {
            touch.body = playbook::enforce_signature(&touch.body, signature);
        }
    }
}

/// Strip the AI-tell punctuation the model reaches for by default (em and en
/// dashes) and tidy the spacing left behind, so the copy reads like a person
/// typed it rather than a model. Guaranteed: none survive into the CRM.
fn scrub_ai_punctuation(sequence: &mut CopySequence) {
    for touch in &mut sequence.touches {
        touch.subject = normalize_dashes(&touch.subject);
        touch.body = normalize_dashes(&touch.body);
    }
    normalize_thread_subjects(sequence);
}

/// Keep no-reply follow-ups in one recognizable email thread. Asking the model
/// for a fresh subject on every touch made the CRM look like unrelated
/// content cards and made the actual SMTP thread feel automated. The writer
/// chooses the one real subject; this only applies normal reply mechanics.
fn normalize_thread_subjects(sequence: &mut CopySequence) {
    let Some(root) = sequence
        .touches
        .iter()
        .find(|touch| touch.stage == 1 && touch.channel.eq_ignore_ascii_case("email"))
        .map(|touch| {
            touch
                .subject
                .trim()
                .strip_prefix("re:")
                .unwrap_or(touch.subject.trim())
                .trim()
                .to_string()
        })
        .filter(|subject| !subject.is_empty())
    else {
        return;
    };
    for touch in &mut sequence.touches {
        if touch.channel.eq_ignore_ascii_case("linkedin_request") {
            touch.subject.clear();
        } else if touch.stage > 1 && is_email_capable_channel(&touch.channel) {
            touch.subject = format!("re: {root}");
        }
    }
}

fn normalize_dashes(text: &str) -> String {
    let mut out = text
        .replace(" — ", ", ")
        .replace(" – ", ", ")
        .replace('—', ", ")
        .replace('–', "-");
    while out.contains(", ,") {
        out = out.replace(", ,", ",");
    }
    while out.contains("  ") {
        out = out.replace("  ", " ");
    }
    out.replace(" ,", ",")
}

fn lint_copy_touch(pb: &Playbook, shared: &Shared, touch: &CopyTouch) -> playbook::Lint {
    let forbidden = pb.forbidden(shared);
    let (min, max) = touch_word_band(pb, touch);
    let mut lint = playbook::lint(&touch.body, &forbidden, min, max);
    lint.signature_ok = !is_email_capable_channel(&touch.channel)
        || playbook::has_exact_signature(&touch.body, &pb.signature);
    if !touch.subject.is_empty() {
        let subject_lint = playbook::lint(&touch.subject, &forbidden, 0, 0);
        for hit in subject_lint.forbidden_hits {
            if !lint.forbidden_hits.contains(&hit) {
                lint.forbidden_hits.push(hit);
            }
        }
    }
    lint
}

fn touch_word_band(pb: &Playbook, touch: &CopyTouch) -> (usize, usize) {
    // These bands follow the shape seen in large cold-email datasets: a compact
    // first note, then progressively shorter follow-ups. A high maximum becomes
    // a target for language models, so keep the ceilings honest.
    if touch.channel.eq_ignore_ascii_case("linkedin_request") {
        (8, 45)
    } else if touch.channel.eq_ignore_ascii_case("linkedin_or_email") {
        if touch.stage == 7 {
            (15, 60)
        } else {
            // Dual-use follow-ups still have to be substantive correspondence
            // when LinkedIn is unavailable and they fall back to email.
            (35, 100)
        }
    } else if touch.channel.eq_ignore_ascii_case("email") {
        match touch.stage {
            // T1 uses the configured band exactly. Silently relaxing the floor
            // turned a 75-word commercial standard into a 55-word production
            // standard and made incomplete notes look compliant.
            1 => (pb.min_words, pb.max_words),
            // These are still emails, not caption-sized blurbs. T2 sharpens
            // the diagnostic, T4 contributes value, and T6 routes; each stays
            // shorter than T1 while retaining enough room to do its actual job.
            2 => (35, 145),
            // T4 has to contribute a new distinction, not hit an arbitrary
            // paragraph size. A natural 35–44 word founder follow-up is often
            // complete; forcing filler here made otherwise reviewable
            // sequences fail after the semantic editor had already approved
            // their substance.
            4 => (35, 120),
            6 => (25, 80),
            7 => (15, 60),
            _ => (8, 40),
        }
    } else if touch.channel.eq_ignore_ascii_case("linkedin") {
        (8, 30)
    } else {
        (8, 35)
    }
}

/// Count spoken sentences after removing the email envelope. Greeting and
/// signature lines are not copy sentences, and a final clause without terminal
/// punctuation still counts as one.
fn copy_sentence_count(body: &str, signature: &str) -> usize {
    let mut lines = body
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && *line != signature.trim())
        .collect::<Vec<_>>();
    if lines.first().is_some_and(|line| {
        line.ends_with(',') && line.split_whitespace().count() <= 3 && !line.contains('?')
    }) {
        lines.remove(0);
    }
    let prose = lines.join(" ");
    let mut sentences = 0usize;
    let mut has_text = false;
    for character in prose.chars() {
        if character.is_alphanumeric() {
            has_text = true;
        }
        if matches!(character, '.' | '?' | '!') && has_text {
            sentences += 1;
            has_text = false;
        }
    }
    sentences + usize::from(has_text)
}

/// The first note often has two different jobs that are both naturally phrased
/// as questions: test one operating decision, then ask for a short conversation.
/// Counting punctuation cannot distinguish those jobs, so reserve up to three
/// marks for T1 and keep every follow-up to one. A short diagnostic with two alternatives
/// plus a call-or-email CTA can naturally contain three marks (and appears in
/// the human validation set). Semantic review still rejects a survey or several
/// unrelated operating questions.
fn touch_question_limit(stage: u32) -> usize {
    if stage == 1 {
        3
    } else {
        1
    }
}

/// Count whole epistemic moves, not every cautious modal. Source-safe artifact
/// language can naturally use "may be" once or twice; the failure mode we want
/// to catch is a note that keeps reopening or apologizing for its hypothesis.
fn gnk_hedge_moves(body: &str) -> usize {
    let body = body.to_ascii_lowercase();
    [
        "my guess is",
        "i assume",
        "i wonder",
        "i'm wondering",
        "i am wondering",
        "i do not know",
        "i don't know",
        "if this exists",
        "if the answer",
        "if the latter",
        "there may be nothing",
        "there might be nothing",
        "may already solve",
        "might already solve",
        "could already solve",
        "may already handle",
        "might already handle",
        "could already handle",
        "perhaps",
        "possibly",
    ]
    .iter()
    .filter(|phrase| body.contains(**phrase))
    .count()
}

fn wapahki_names_operating_consequence(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    let named_consequence = [
        "staffing",
        "staffed manually",
        "needs people",
        "need people",
        "needs operators",
        "need operators",
        "vacan",
        "hard-to-fill",
        "hard to fill",
        "overtime",
        "throughput",
        "output",
        "line stop",
        "stoppage",
        "downtime",
        "utilization",
        "short run",
        "short-run",
        "ergonomic",
        "injur",
        "safety",
        "payback",
        "capacity",
        "bottleneck",
        "shift coverage",
        "idle",
        "keep the line moving",
        "cannot keep up",
        "can't keep up",
        "changeover cost",
    ]
    .iter()
    .any(|marker| body.contains(marker));
    let quantified_lifting = body.contains("lift")
        && [" lb", "-lb", " pound"]
            .iter()
            .any(|marker| body.contains(marker));
    let repetitive_lifting = ["regular lifting", "repetitive lifting", "heavy lifting"]
        .iter()
        .any(|marker| body.contains(marker));
    named_consequence || quantified_lifting || repetitive_lifting
}

fn gnk_names_operating_consequence(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    [
        "settlement",
        "delay",
        "recovery",
        "recoveries",
        "write-off",
        "write off",
        "audit",
        "sla",
        "escalation",
        "senior time",
        "reviewer capacity",
        "leakage",
        "burden",
    ]
    .iter()
    .any(|marker| body.contains(marker))
}

fn has_natural_response_path(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    let exact_path = [
        "short conversation",
        "brief conversation",
        "quick conversation",
        "minute call",
        "short call",
        "brief call",
        "email reply",
        "reply by email",
        "replying by email",
        "brief reply",
        "reply is enough",
        "tell me by email",
        "answer by email",
        "by email whether",
        "email would be easier",
        "short email",
        "brief email",
        "reply here",
        "discuss it briefly",
        "discuss briefly",
        "open to a call",
        "open to a conversation",
    ]
    .iter()
    .any(|marker| body.contains(marker));
    let email_path = body.contains("email")
        && ["reply", "answer", "easier", "write back"]
            .iter()
            .any(|marker| body.contains(marker));
    let conversation_path = ["call", "conversation", "chat"]
        .iter()
        .any(|marker| body.contains(marker))
        && ["short", "brief", "quick", "minute", "open to"]
            .iter()
            .any(|marker| body.contains(marker));
    let useful_asset_path = ["fit screen", "one-page"]
        .iter()
        .any(|marker| body.contains(marker))
        && ["send", "share"].iter().any(|marker| body.contains(marker))
        && body.contains('?');
    // A single concrete operating question is already a natural reply path.
    // Requiring an extra `reply by email` or call sentence made the writer add
    // a second CTA, then the independent reviewer correctly rejected the
    // resulting menu. Semantic QA still decides whether the question itself is
    // worth answering.
    let one_direct_question = body.matches('?').count() == 1;
    exact_path || email_path || conversation_path || useful_asset_path || one_direct_question
}

fn mentions_outreach_asset(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    [
        "one-page",
        "outside-view",
        "outside-in",
        "checklist",
        "sketch",
        "historical example",
        "historical comparison",
        "historical review",
        "fit screen",
        "template",
        "worksheet",
        "blank version",
        "formatted page",
        "benchmark",
        "case study",
        "assessment",
    ]
    .iter()
    .any(|term| body.contains(term))
}

fn names_historical_outage_result(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    let result = [
        "i matched",
        "we matched",
        "i found",
        "we found",
        "overlapped",
        "fell inside",
        "reported at",
    ]
    .iter()
    .any(|term| body.contains(term));
    let place = ["location", "charging site", "station", "site in", " at "]
        .iter()
        .any(|term| body.contains(term));
    let outage = ["utility outage", "utility report", "outage area"]
        .iter()
        .any(|term| body.contains(term));
    let time = body.chars().any(|character| character.is_ascii_digit())
        && [
            "timestamp",
            "2024",
            "2025",
            "2026",
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ]
        .iter()
        .any(|term| body.contains(term));
    let hypothetical = [
        "can prepare",
        "could prepare",
        "would prepare",
        "may match",
        "could match",
        "would match",
        "example could",
    ]
    .iter()
    .any(|term| body.contains(term));
    result && place && outage && time && !hypothetical
}

/// Correction and routing are legitimate outcomes, but the failed campaigns
/// showed the same retreat rewritten across five or six stages. Count touches
/// whose primary language is to abandon, be corrected, or find somebody else;
/// the sequence may use those moves deliberately, not as its entire value.
fn is_retreat_or_route_touch(touch: &CopyTouch) -> bool {
    // T1 is separately required to establish evidence, decision, mechanism,
    // and relevance. An executive-friendly "or should I ask someone closer?"
    // alternative is a valid CTA there, not a retreat touch.
    if touch.stage == 1 {
        return false;
    }
    if touch.stage == 7 {
        return true;
    }
    let body = touch.body.to_ascii_lowercase();
    [
        "a correction",
        "correction would",
        "correct me",
        "am i off",
        "i'm off",
        "i am off",
        "off base",
        "wrong premise",
        "wrong concern",
        "wrong thread",
        "not relevant",
        "not material",
        "not applicable",
        "drop this",
        "drop the thread",
        "stop pursuing",
        "set this aside",
        "step back",
        "close this out",
        "closing this out",
        "someone else",
        "better person",
        "right person",
        "right contact",
        "who should i",
        "who would be",
        "point me",
        "a redirect",
    ]
    .iter()
    .any(|marker| body.contains(marker))
}

/// Making the response smaller is useful; dictating the prospect's answer is
/// not. These patterns came from otherwise high-scoring model copy that read
/// like a coded research form rather than a founder email.
fn has_forced_response_menu(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    [
        "yes, no, or",
        "a yes, no",
        "reply with",
        "reply \"",
        "reply ‘",
        "reply “",
        "the easiest reply is",
        "one-line note",
        "one-line reply",
        "one word is enough",
        "is enough to classify",
        "is enough to end",
        "a name is enough",
        "within-run",
        "between-run",
        "fixed or manual",
        "fixed, flexible",
    ]
    .iter()
    .any(|marker| body.contains(marker))
}

fn narrates_internal_copy_logic(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    [
        "the question concerns",
        "the premise was narrow",
        "the key distinction is",
        "the distinction i have in mind",
        "the deciding point is",
        "the practical difference is",
        "repetitive slice",
        "the practical fork is",
        "the practical split is",
        "decision consequential",
        "i respect that work",
    ]
    .iter()
    .any(|marker| body.contains(marker))
}

/// A subject can be mechanically short and still be invisible in an inbox.
/// Reject category labels such as "utility status", "claim evidence", or
/// "decision trail" and make the editor name the operating event, decision,
/// object, or consequence that makes this thread distinct.
pub(crate) fn generic_subject_label(subject: &str) -> bool {
    let generic = [
        "a",
        "about",
        "alarm",
        "alarms",
        "an",
        "at",
        "classification",
        "claim",
        "claims",
        "context",
        "decision",
        "decisions",
        "evidence",
        "for",
        "from",
        "in",
        "issue",
        "issues",
        "location",
        "locations",
        "of",
        "on",
        "operational",
        "operations",
        "outage",
        "outages",
        "power",
        "question",
        "review",
        "reviews",
        "site",
        "sites",
        "status",
        "support",
        "the",
        "to",
        "trail",
        "trails",
        "utility",
        "with",
        "workflow",
        "workflows",
    ];
    let meaningful = subject
        .trim()
        .strip_prefix("re: ")
        .unwrap_or(subject.trim())
        .split_whitespace()
        .map(|word| {
            word.trim_matches(|character: char| !character.is_alphanumeric())
                .to_ascii_lowercase()
        })
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    !meaningful.is_empty()
        && meaningful
            .iter()
            .all(|word| generic.contains(&word.as_str()))
}

fn unsupported_account_task_noun_issues(
    pb: &Playbook,
    account: &CopyAccount,
    sequence: &CopySequence,
) -> Vec<String> {
    if !pb.key.eq_ignore_ascii_case("wapahki") {
        return Vec::new();
    }
    let facts = account.observed_facts.join(" ").to_ascii_lowercase();
    let noun_groups: &[(&[&str], &[&str], &str)] = &[
        (&["tray"], &["tray"], "tray"),
        (&["pouch"], &["pouch"], "pouch"),
        (&["pallet"], &["pallet"], "pallet"),
        (&["conveyor"], &["conveyor"], "conveyor"),
        (&["bottle"], &["bottle"], "bottle"),
        (&["carton"], &["carton"], "carton"),
        (
            &[
                "case-loading",
                "case loading",
                "case-packing",
                "case packing",
                "into cases",
            ],
            &[
                "case loading",
                "case-loading",
                "case packing",
                "case-packing",
                "into cases",
            ],
            "case-loading task",
        ),
        (
            &["pack pattern", "packing pattern"],
            &["pack pattern", "packing pattern"],
            "packing pattern",
        ),
    ];
    let mut issues = Vec::new();
    for touch in &sequence.touches {
        let body = touch.body.to_ascii_lowercase();
        for (copy_terms, fact_terms, label) in noun_groups {
            let appears_in_copy = copy_terms.iter().any(|term| body.contains(term));
            let supported = fact_terms.iter().any(|term| facts.contains(term));
            if appears_in_copy && !supported {
                issues.push(format!(
                    "stage {} names an unverified {label}; keep the email at the supported packing/handling category and let the recipient identify the actual task",
                    touch.stage
                ));
            }
        }
    }
    issues.sort();
    issues.dedup();
    issues
}

fn account_sequence_quality_issues(
    pb: &Playbook,
    shared: &Shared,
    account: &CopyAccount,
    sequence: &CopySequence,
    reviews: &[TouchReview],
    expected_touches: usize,
    critique: bool,
) -> Vec<String> {
    let mut issues =
        sequence_quality_issues(pb, shared, sequence, reviews, expected_touches, critique);
    issues.extend(unsupported_account_task_noun_issues(pb, account, sequence));
    issues.sort();
    issues.dedup();
    issues
}

fn sequence_quality_issues(
    pb: &Playbook,
    shared: &Shared,
    sequence: &CopySequence,
    reviews: &[TouchReview],
    expected_touches: usize,
    critique: bool,
) -> Vec<String> {
    let mut issues = Vec::new();
    if sequence.touches.len() != expected_touches {
        issues.push(format!(
            "expected {expected_touches} touches, got {}",
            sequence.touches.len()
        ));
    }
    let mut stages = HashSet::new();
    let mut last_day = None;
    let expected_channels = [
        "email",
        "email",
        "linkedin_request",
        "email",
        "linkedin_or_email",
        "email",
        "linkedin_or_email",
    ];
    let expected_days = [0, 3, 5, 9, 13, 17, 21];
    let thread_root = sequence
        .touches
        .iter()
        .find(|touch| touch.stage == 1 && touch.channel.eq_ignore_ascii_case("email"))
        .map(|touch| touch.subject.trim().to_string())
        .unwrap_or_default();

    for touch in &sequence.touches {
        if !stages.insert(touch.stage) {
            issues.push(format!("duplicate stage {}", touch.stage));
        }
        if touch.stage == 0 || touch.stage as usize > expected_touches {
            issues.push(format!("invalid stage {}", touch.stage));
        }
        if let Some(previous) = last_day {
            if touch.day_offset <= previous {
                issues.push(format!(
                    "day offsets are not increasing at stage {}",
                    touch.stage
                ));
            }
        }
        last_day = Some(touch.day_offset);

        let channel = touch.channel.to_ascii_lowercase();
        if !matches!(
            channel.as_str(),
            "email" | "linkedin" | "linkedin_request" | "linkedin_or_email"
        ) {
            issues.push(format!("unsupported channel '{}'", touch.channel));
        }
        if is_email_capable_channel(&channel) {
            let subject_words = touch.subject.split_whitespace().count();
            if touch.stage == 1 {
                if !(3..=9).contains(&subject_words) {
                    issues.push(format!(
                        "stage 1 subject has {subject_words} words (needs 3–9)"
                    ));
                }
                if generic_subject_label(&touch.subject) {
                    issues.push(
                        "stage 1 subject is only a generic topic label; name the concrete operating event, decision, object, or consequence that makes the email worth opening"
                            .into(),
                    );
                }
            } else {
                let expected = format!("re: {thread_root}");
                if thread_root.is_empty() || !touch.subject.eq_ignore_ascii_case(&expected) {
                    issues.push(format!(
                        "stage {} must keep the original email subject as 're: {thread_root}'",
                        touch.stage
                    ));
                }
            }
            if touch.subject.contains('?') || touch.subject.chars().any(|c| c.is_ascii_digit()) {
                issues.push(format!(
                    "stage {} subject must not be a question or contain a number",
                    touch.stage
                ));
            }
            if channel == "email" || channel == "linkedin_or_email" {
                let greeting = touch
                    .body
                    .lines()
                    .map(str::trim)
                    .find(|line| !line.is_empty())
                    .unwrap_or("");
                let greeting_lower = greeting.to_ascii_lowercase();
                let greeting_words = greeting.split_whitespace().count();
                if !greeting_lower.starts_with("hi ")
                    || !greeting.ends_with(',')
                    || !(2..=4).contains(&greeting_words)
                    || greeting.contains('?')
                {
                    issues.push(format!(
                        "stage {} must start with 'Hi [First name],' on its own line",
                        touch.stage
                    ));
                }
            }
            let sentences = copy_sentence_count(&touch.body, &pb.signature);
            // Sentence counts are only a coarse corruption guard. Earlier
            // narrow stage-specific maxima rejected perfectly natural short
            // emails after the semantic editor had fixed them, and pushed the
            // writer toward fragments. Word bands and the independent reviewer
            // carry style; this gate catches only empty or sprawling output.
            let sentence_limit_ok = if touch.stage == 1 {
                (2..=12).contains(&sentences)
            } else {
                (1..=8).contains(&sentences)
            };
            if !sentence_limit_ok {
                let expected = if touch.stage == 1 { "2–12" } else { "1–8" };
                issues.push(format!(
                    "stage {} has {sentences} copy sentences (needs {expected})",
                    touch.stage
                ));
            }
            let paragraphs = touch
                .body
                .split("\n\n")
                .filter(|paragraph| {
                    let paragraph = paragraph.trim();
                    !paragraph.is_empty() && paragraph != pb.signature
                })
                .count();
            // T1 may use distinct short paragraphs for role relevance, operating
            // moment, seller mechanism, and CTA. Follow-ups remain more compact.
            let paragraph_limit = if touch.stage == 1 { 7 } else { 6 };
            if paragraphs > paragraph_limit {
                issues.push(format!("stage {} has {paragraphs} paragraphs", touch.stage));
            }
        }
        let question_limit = touch_question_limit(touch.stage);
        let question_count = touch.body.matches('?').count();
        if question_count > question_limit {
            issues.push(if touch.stage == 1 {
                "stage 1 asks more than one central operating question plus one CTA".to_string()
            } else {
                format!("stage {} asks more than one question", touch.stage)
            });
        }
        if pb.key == "gnk" && touch.stage == 1 {
            let body = touch.body.to_ascii_lowercase();
            if !body.contains("gnk")
                || !(body.contains("software") || body.contains("system") || body.contains("tool"))
            {
                issues.push(
                    "GnK stage 1 must name GnK and one concrete software/system contribution"
                        .into(),
                );
            }
            if !gnk_names_operating_consequence(&touch.body) {
                issues.push(
                    "GnK stage 1 must connect the recurring decision to a believable operating or economic consequence"
                        .into(),
                );
            }
            if !has_natural_response_path(&touch.body) {
                issues.push(
                    "GnK stage 1 must offer a natural short-conversation or email response path"
                        .into(),
                );
            }
            let hedge_moves = gnk_hedge_moves(&touch.body);
            if hedge_moves > 1 {
                issues.push(format!(
                    "GnK stage 1 stacks {hedge_moves} hypothesis caveats (maximum 1); state one sharp guess and move to the easy question"
                ));
            }
        }
        if pb.key == "wapahki" && touch.stage == 1 {
            let body = touch.body.to_ascii_lowercase();
            let has_university_context = body.contains("university of toronto")
                || body.contains("u of t")
                || body.contains("uoft");
            if !has_university_context || !body.contains("automata") {
                issues.push(
                    "Wapahki stage 1 must briefly give Andrew's University of Toronto and Automata robotics context"
                        .into(),
                );
            }
            if !(body.contains("wapahki") || body.contains("robotic") || body.contains("robotics"))
            {
                issues.push(
                    "Wapahki stage 1 must state one honest robotics contribution rather than only ask for research"
                        .into(),
                );
            }
            if !wapahki_names_operating_consequence(&touch.body) {
                issues.push(
                    "Wapahki stage 1 names no operating consequence; connect the candidate task to staffing, throughput, stoppage, utilization, economics, safety, or sanitation"
                        .into(),
                );
            }
            if !has_natural_response_path(&touch.body) {
                issues.push(
                    "Wapahki stage 1 must offer a natural short-conversation or email response path"
                        .into(),
                );
            }
        }
        if channel == "linkedin_request" && touch.body.chars().count() > 300 {
            issues.push(format!(
                "stage {} LinkedIn connection request is {} characters (maximum 300)",
                touch.stage,
                touch.body.chars().count()
            ));
        }
        if channel == "linkedin_request" && is_empty_linkedin_praise(&touch.body) {
            issues.push(format!(
                "stage {} is an empty compliment; name the concrete operating question or shared topic that makes connecting useful",
                touch.stage
            ));
        }
        if touch.stage == 7 && touch.body.contains('?') {
            issues.push("stage 7 must close without a question".to_string());
        }
        if has_forced_response_menu(&touch.body) {
            issues.push(format!(
                "stage {} dictates a response menu or internal taxonomy; ask one natural question and let the recipient answer in their own words",
                touch.stage
            ));
        }
        if narrates_internal_copy_logic(&touch.body) {
            issues.push(format!(
                "stage {} narrates the outreach logic instead of speaking naturally to the recipient",
                touch.stage
            ));
        }
        let lint = lint_copy_touch(pb, shared, touch);
        if !lint.forbidden_hits.is_empty() {
            issues.push(format!(
                "stage {} uses forbidden phrase(s): {}",
                touch.stage,
                lint.forbidden_hits.join(", ")
            ));
        }
        if !lint.length_ok {
            let (min, max) = touch_word_band(pb, touch);
            issues.push(format!(
                "stage {} is {} words (needs {min}–{max})",
                touch.stage, lint.word_count
            ));
        }
        if !lint.signature_ok {
            issues.push(format!("stage {} has the wrong signature", touch.stage));
        }
        if critique {
            match reviews.iter().find(|review| review.stage == touch.stage) {
                Some(review) if review.passes && review.score >= 85 && review.issues.is_empty() => {
                }
                Some(review) => issues.push(format!(
                    "stage {} scored {}/100 and was not approved",
                    touch.stage, review.score
                )),
                None => issues.push(format!("stage {} has no semantic review", touch.stage)),
            }
        }
    }

    let question_touches = sequence
        .touches
        .iter()
        .filter(|touch| touch.body.contains('?'))
        .count();
    // A short sequence cannot ask a question in every single touch. T1 and T2
    // may test the premise and the final email may make one ask; the connection
    // note should simply give a human reason to connect. The legacy sequence
    // has room for one additional routing question.
    let question_touch_limit = match expected_touches {
        1 => 1,
        2 => 2,
        4 => 3,
        _ => 4,
    };
    if question_touches > question_touch_limit {
        issues.push(format!(
            "sequence asks questions in {question_touches} touches (maximum {question_touch_limit})"
        ));
    }

    let retreat_touches = sequence
        .touches
        .iter()
        .filter(|touch| is_retreat_or_route_touch(touch))
        .count();
    let retreat_touch_limit = match expected_touches {
        1 | 2 => 0,
        4 => 1,
        _ => 3,
    };
    if retreat_touches > retreat_touch_limit {
        issues.push(format!(
            "sequence relies on retreat, correction, routing, or closure in {retreat_touches} touches (maximum {retreat_touch_limit}; T2 must sharpen the mechanism and later touches must contribute value or answer the buyer objection)"
        ));
    }

    let asset_stages = sequence
        .touches
        .iter()
        .filter(|touch| mentions_outreach_asset(&touch.body))
        .map(|touch| touch.stage)
        .collect::<Vec<_>>();
    // Even a real resource is one move in a conversation, not the premise of
    // the whole sequence. Repeating it in another message is the generic
    // lead-magnet loop that made otherwise tailored sequences robotic.
    let asset_limit = 1;
    for stage in asset_stages.iter().skip(asset_limit) {
        issues.push(format!(
            "stage {stage} repeats collateral across {} touches (maximum {asset_limit}; continue the human conversation instead)",
            asset_stages.len(),
        ));
    }

    let brand_name = pb.name.to_ascii_lowercase();
    let brand_mentions = sequence
        .touches
        .iter()
        .map(|touch| touch.body.to_ascii_lowercase().matches(&brand_name).count())
        .sum::<usize>();
    // Repeating the seller name in most touches is clearly automated, but a
    // second natural reference in a seven-touch thread is not a sendability
    // failure. The semantic reviewer still judges whether either mention is
    // seller-first or unnecessary.
    let brand_mention_limit = if matches!(pb.key.as_str(), "gnk" | "wapahki") {
        1
    } else {
        2
    };
    if brand_mentions > brand_mention_limit {
        issues.push(format!(
            "{} appears {brand_mentions} times (maximum {brand_mention_limit} across the sequence)",
            pb.name,
        ));
    }

    if pb.key == "outagehub" && matches!(expected_touches, 1 | 2) {
        let first = sequence.touches.iter().find(|touch| touch.stage == 1);
        if first.is_some_and(|touch| {
            let body = touch.body.to_ascii_lowercase();
            !body.contains("outagehub") || !body.contains("utility")
        }) {
            issues.push("OutageHub stage 1 must name its utility-location contribution".into());
        }
        if expected_touches == 2 {
            let second = sequence.touches.iter().find(|touch| touch.stage == 2);
            if second.is_some_and(|touch| !names_historical_outage_result(&touch.body)) {
                issues.push(
                    "OutageHub stage 2 must contribute a real location-specific historical result; hold the sequence when no verified example exists"
                        .into(),
                );
            }
        }
    } else if pb.key == "outagehub" {
        issues.push("OutageHub's evidence cadence requires exactly two emails".into());
    }

    if expected_touches == 7 {
        for (index, expected) in expected_channels.iter().enumerate() {
            if sequence
                .touches
                .get(index)
                .is_some_and(|touch| !touch.channel.eq_ignore_ascii_case(expected))
            {
                issues.push(format!("stage {} should use {expected}", index + 1));
            }
            if sequence
                .touches
                .get(index)
                .is_some_and(|touch| touch.day_offset != expected_days[index])
            {
                issues.push(format!(
                    "stage {} should use day offset {}",
                    index + 1,
                    expected_days[index]
                ));
            }
        }
        if sequence
            .touches
            .last()
            .is_some_and(|touch| touch.day_offset > 21)
        {
            issues.push("seven-touch plan extends beyond day 21".to_string());
        }
    } else if expected_touches == 4 {
        let expected_channels = ["email", "email", "linkedin_request", "email"];
        let expected_days = [0, 3, 7, 14];
        for (index, expected) in expected_channels.iter().enumerate() {
            if sequence
                .touches
                .get(index)
                .is_some_and(|touch| !touch.channel.eq_ignore_ascii_case(expected))
            {
                issues.push(format!("stage {} should use {expected}", index + 1));
            }
            if sequence
                .touches
                .get(index)
                .is_some_and(|touch| touch.day_offset != expected_days[index])
            {
                issues.push(format!(
                    "stage {} should use day offset {}",
                    index + 1,
                    expected_days[index]
                ));
            }
        }
    } else if expected_touches == 2 {
        let expected_channels = ["email", "email"];
        let expected_days = [0, 6];
        for (index, expected) in expected_channels.iter().enumerate() {
            if sequence
                .touches
                .get(index)
                .is_some_and(|touch| !touch.channel.eq_ignore_ascii_case(expected))
            {
                issues.push(format!("stage {} should use {expected}", index + 1));
            }
            if sequence
                .touches
                .get(index)
                .is_some_and(|touch| touch.day_offset != expected_days[index])
            {
                issues.push(format!(
                    "stage {} should use day offset {}",
                    index + 1,
                    expected_days[index]
                ));
            }
        }
    }

    let emails = sequence
        .touches
        .iter()
        .filter(|touch| is_email_capable_channel(&touch.channel))
        .collect::<Vec<_>>();
    for left in 0..emails.len() {
        for right in (left + 1)..emails.len() {
            let (similarity, overlap) =
                word_set_similarity(&emails[left].body, &emails[right].body, &pb.signature);
            // On a short thread, two notes inevitably repeat the account's core
            // nouns. Only call them repetitive when at least four substantive
            // words overlap after removing greeting/signature boilerplate.
            if similarity > 0.55 && overlap >= 4 {
                issues.push(format!(
                    "email stages {} and {} are {:.0}% repetitive",
                    emails[left].stage,
                    emails[right].stage,
                    similarity * 100.0
                ));
            }
        }
    }
    issues
}

fn is_empty_linkedin_praise(body: &str) -> bool {
    let normalized = body.to_ascii_lowercase();
    let praise = [
        "substantial remit",
        "valuable perspective",
        "value your perspective",
        "impressive background",
        "impressive experience",
        "respect that work",
        "respect your work",
        "your perspective is directly relevant",
    ];
    praise.iter().any(|phrase| normalized.contains(phrase))
}

fn word_set_similarity(left: &str, right: &str, signature: &str) -> (f64, usize) {
    let signature_words = signature
        .split_whitespace()
        .map(|word| {
            word.trim_matches(|c: char| !c.is_alphanumeric())
                .to_ascii_lowercase()
        })
        .collect::<HashSet<_>>();
    let words = |value: &str| {
        let mut lines = value
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && *line != signature.trim())
            .collect::<Vec<_>>();
        if lines.first().is_some_and(|line| {
            line.ends_with(',') && line.split_whitespace().count() <= 3 && !line.contains('?')
        }) {
            lines.remove(0);
        }
        lines
            .join(" ")
            .split_whitespace()
            .map(|word| {
                word.trim_matches(|c: char| !c.is_alphanumeric())
                    .to_ascii_lowercase()
            })
            .filter(|word| word.len() >= 5 && !signature_words.contains(word))
            .collect::<HashSet<_>>()
    };
    let left = words(left);
    let right = words(right);
    let union = left.union(&right).count();
    let overlap = left.intersection(&right).count();
    if union == 0 {
        (0.0, 0)
    } else {
        (overlap as f64 / union as f64, overlap)
    }
}

fn primary_question(body: &str) -> String {
    let Some(question_end) = body.find('?') else {
        return String::new();
    };
    let through_question = &body[..=question_end];
    let question_start = through_question[..question_end]
        .rfind(|character: char| matches!(character, '.' | '!' | '\n'))
        .map_or(0, |position| position + 1);
    through_question[question_start..].trim().to_string()
}

pub(crate) fn cross_recipient_structural_similarity(
    candidate: &CopySequence,
    other: &CopySequence,
    signature: &str,
) -> Option<(f64, f64)> {
    let candidate = candidate.touches.iter().find(|touch| touch.stage == 1)?;
    let other = other.touches.iter().find(|touch| touch.stage == 1)?;
    let (body_similarity, body_overlap) =
        word_set_similarity(&candidate.body, &other.body, signature);
    let candidate_question = primary_question(&candidate.body);
    let other_question = primary_question(&other.body);
    let (question_similarity, question_overlap) =
        word_set_similarity(&candidate_question, &other_question, signature);
    ((body_similarity >= 0.42 && body_overlap >= 10)
        || (question_similarity >= 0.65 && question_overlap >= 6))
        .then_some((body_similarity, question_similarity))
}

fn cross_recipient_similarity_issue(
    db: &SharedDb,
    pb: &Playbook,
    person: &crate::db::Person,
    current_sequence_id: &str,
    sequence: &CopySequence,
) -> Result<Option<String>> {
    if !sequence.touches.iter().any(|touch| touch.stage == 1) {
        return Ok(None);
    }
    for other in db.list_people(Some(&pb.key), None)? {
        if other.id == person.id {
            continue;
        }
        let Some(other_sequence_id) = db.active_sequence_for_person(&other.id)? else {
            continue;
        };
        if other_sequence_id == current_sequence_id {
            continue;
        }
        let current_policy = db
            .sequence_gtm_attribution(&other_sequence_id)?
            .is_some_and(|attribution| {
                attribution.copy_policy_version == CURRENT_COPY_POLICY_VERSION
            });
        if !current_policy {
            continue;
        }
        let Some(other_first) = db
            .list_touches_for_sequence(&other_sequence_id)?
            .into_iter()
            .find(|touch| touch.stage == 1)
        else {
            continue;
        };
        let other_sequence = CopySequence {
            touches: vec![CopyTouch {
                stage: other_first.stage as u32,
                day_offset: other_first.day_offset as u32,
                channel: other_first.channel,
                subject: other_first.subject,
                body: other_first.body,
                purpose: other_first.purpose,
                goal: other_first.goal,
            }],
            applied_principles: Vec::new(),
        };
        if let Some((body_similarity, question_similarity)) =
            cross_recipient_structural_similarity(sequence, &other_sequence, &pb.signature)
        {
            return Ok(Some(format!(
                "cross-recipient structural duplication with {}: T1 body {:.0}% similar and main question {:.0}% similar; rewrite from this account's evidence",
                other.name,
                body_similarity * 100.0,
                question_similarity * 100.0,
            )));
        }
    }
    Ok(None)
}

fn first_line(s: &str) -> String {
    let line = s.lines().next().unwrap_or("").trim();
    if line.chars().count() <= 500 {
        line.to_string()
    } else {
        format!("{}…", line.chars().take(499).collect::<String>())
    }
}

fn usage_stop_reason(error: &anyhow::Error) -> String {
    let detail = error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    let lower = detail.to_ascii_lowercase();
    let provider = if lower.contains("claude") {
        "Claude"
    } else if lower.contains("codex") || lower.contains("openai") {
        "OpenAI"
    } else if lower.contains("grok") {
        "Grok"
    } else {
        "Reasoning provider"
    };
    let reset = lower.find("resets ").and_then(|index| {
        let tail = &detail[index + "resets ".len()..];
        let value = tail
            .split(['"', '\n', '}', '\\'])
            .next()
            .unwrap_or("")
            .trim()
            .trim_end_matches(['.', ',']);
        (!value.is_empty()).then_some(value)
    });
    match reset {
        Some(reset) => format!("{provider} usage limit reached; resets {reset}"),
        None => format!("{provider} usage limit reached; drafting stopped early"),
    }
}

fn model_stop_reason(error: &anyhow::Error) -> String {
    if crate::engine::is_run_budget_exhausted(error) {
        first_line(&format!("{error:#}"))
    } else if crate::engine::is_generation_incomplete(error) {
        "Model generation reached its output boundary before returning the complete sequence JSON; drafting stopped safely and can be retried".into()
    } else if crate::engine::is_usage_exhausted(error) {
        usage_stop_reason(error)
    } else {
        let detail = format!("{error:#}");
        let lower = detail.to_ascii_lowercase();
        if lower.contains("tls close_notify") || lower.contains("peer closed connection") {
            "OpenAI closed the response connection before completion after bounded retries; drafting stopped safely and the copy was not rejected".into()
        } else if lower.contains("timed out after") {
            format!(
                "{}; drafting stopped safely and the copy was not rejected",
                first_line(&detail)
            )
        } else if lower.contains("http 429") {
            "OpenAI remained rate-limited after bounded retries; drafting stopped safely and the copy was not rejected".into()
        } else {
            format!(
                "OpenAI request failed after bounded retries: {}; the copy was not rejected",
                first_line(&detail)
            )
        }
    }
}

fn model_stop_phase(detail: &str) -> String {
    let lower = detail.to_ascii_lowercase();
    if lower.contains("per-turn") && lower.contains("ceiling") {
        "turn safety ceiling reached".into()
    } else if lower.contains("output boundary") || lower.contains("max_output_tokens") {
        "generation incomplete at output boundary".into()
    } else if lower.contains("usage") && (lower.contains("limit") || lower.contains("exhaust")) {
        "stopped; model usage limit reached".into()
    } else if lower.contains("tls close_notify") || lower.contains("peer closed connection") {
        "stopped; response connection closed after retries".into()
    } else if lower.contains("timed out after") {
        "stopped; model timed out after retries".into()
    } else if lower.contains("http 429") {
        "stopped; provider rate-limited after retries".into()
    } else {
        "stopped; provider unavailable after retries".into()
    }
}

fn affected_stages(issue: &str, expected_touches: usize) -> Vec<u32> {
    (1..=expected_touches as u32)
        .filter(|stage| {
            issue.starts_with(&format!("stage {stage} "))
                || issue.contains(&format!(" stages {stage} and "))
                || issue.contains(&format!(" and {stage} are "))
        })
        .collect()
}

fn plan_schema(n: usize) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["overall_strategy", "response_strategy", "touches", "applied_principles"],
        "properties": {
            "overall_strategy": {
                "type": "string",
                "description": format!("One or two sentences on the arc across all {n} touches.")
            },
            "response_strategy": {
                "type": "object",
                "additionalProperties": false,
                "required": ["desired_response", "role_relevant_motive", "concrete_scene", "credibility_basis", "smallest_commitment", "reactance_guard"],
                "properties": {
                    "desired_response": { "type": "string", "description": "The exact next reply or action Andrew needs from this person." },
                    "role_relevant_motive": { "type": "string", "description": "Why responding could matter to this recipient based only on verified role/account context." },
                    "concrete_scene": { "type": "string", "description": "One recognizable operating moment the recipient can mentally simulate." },
                    "credibility_basis": { "type": "string", "description": "The verified, attributed, or self-testable basis that makes the note worth considering." },
                    "smallest_commitment": { "type": "string", "description": "The lowest-effort voluntary step that still advances the work." },
                    "reactance_guard": { "type": "string", "description": "The claim, pressure, or ask size most likely to feel presumptuous or manipulative and therefore must be avoided." }
                }
            },
            "touches": {
                "type": "array",
                "minItems": n,
                "maxItems": n,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["stage", "channel", "objective", "angle", "ask"],
                    "properties": {
                        "stage": { "type": "integer" },
                        "channel": { "type": "string", "enum": ["email", "linkedin", "linkedin_request", "linkedin_or_email"] },
                        "objective": { "type": "string" },
                        "angle": { "type": "string" },
                        "ask": { "type": "string" }
                    }
                }
            },
            "applied_principles": {
                "type": "array",
                "items": { "type": "string" },
                "description": "IDs of retrieved principles that materially changed this plan."
            }
        }
    })
}

fn touch_schema(n: usize) -> Value {
    json!({
        "type": "array",
        "minItems": 0,
        "maxItems": n,
        "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["stage", "day_offset", "channel", "subject", "body", "purpose", "goal"],
            "properties": {
                "stage": { "type": "integer" },
                "day_offset": { "type": "integer" },
                "channel": { "type": "string", "enum": ["email", "linkedin", "linkedin_request", "linkedin_or_email"] },
                "subject": { "type": "string" },
                "body": { "type": "string" },
                "purpose": { "type": "string" },
                "goal": { "type": "string" }
            },
        }
    })
}

fn batch_sequence_schema(n: usize, people: usize) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["sequences"],
        "properties": {
            "sequences": {
                "type": "array",
                "minItems": people,
                "maxItems": people,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["person_key", "send_decision", "decision_reason", "operating_decision", "mechanism_to_test", "hard_buyer_objection", "recipient_reply_reason", "value_exchange", "touches", "applied_principles"],
                    "properties": {
                        "person_key": { "type": "string" },
                        "send_decision": { "type": "string", "enum": ["send", "hold_for_research"] },
                        "decision_reason": { "type": "string", "description": "Private reason for the send/hold decision. For a hold, name the missing evidence or recipient-fit problem." },
                        "operating_decision": { "type": "string", "description": "Private: the exact recurring operating moment and decision the sequence stays anchored to." },
                        "mechanism_to_test": { "type": "string", "description": "Private: the plausible mechanism that could make the decision difficult. It is a hypothesis, never an account fact." },
                        "hard_buyer_objection": { "type": "string", "description": if n == 4 { "Private: the strongest credible reason this recipient would dismiss the premise; the final follow-up should address it honestly when that creates a useful reply path." } else { "Private: the strongest credible reason this recipient would dismiss the premise; touch 5 should answer it honestly." } },
                        "recipient_reply_reason": { "type": "string", "description": "Private: the recipient's self-interested reason to reply, not Andrew's desire for research." },
                        "value_exchange": { "type": "string", "description": "Private: exact seller give-back explicitly supplied or permitted in verified context, including a tailored item Andrew is allowed to prepare; otherwise empty. Never invent prior analysis or collateral." },
                        "touches": touch_schema(n),
                        "applied_principles": { "type": "array", "items": { "type": "string" } }
                    }
                }
            }
        }
    })
}

fn sales_council_schema(critics: &[SalesCriticPersona], email_count: usize) -> Value {
    let critic_ids = critics
        .iter()
        .map(|critic| critic.id.clone())
        .collect::<Vec<_>>();
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["critics"],
        "properties": {
            "critics": {
                "type": "array",
                "minItems": critics.len(),
                "maxItems": critics.len(),
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["critic_id", "touches"],
                    "properties": {
                        "critic_id": { "type": "string", "enum": critic_ids },
                        "touches": {
                            "type": "array",
                            "minItems": email_count,
                            "maxItems": email_count,
                            "items": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["stage", "passes", "score", "issues", "recommendation"],
                                "properties": {
                                    "stage": { "type": "integer" },
                                    "passes": { "type": "boolean" },
                                    "score": { "type": "integer", "minimum": 0, "maximum": 100 },
                                    "issues": { "type": "array", "items": { "type": "string" } },
                                    "recommendation": { "type": "string" }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

fn review_edit_schema(n: usize) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["sequence_passes", "sequence_score", "sequence_issues", "reviews"],
        "properties": {
            "sequence_passes": { "type": "boolean" },
            "sequence_score": { "type": "integer", "minimum": 0, "maximum": 100 },
            "sequence_issues": { "type": "array", "items": { "type": "string" } },
            "reviews": {
                "type": "array",
                "minItems": n,
                "maxItems": n,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["stage", "passes", "score", "issues", "revised_subject", "revised_body"],
                    "properties": {
                        "stage": { "type": "integer" },
                        "passes": { "type": "boolean" },
                        "score": { "type": "integer" },
                        "issues": { "type": "array", "items": { "type": "string" } },
                        "revised_subject": { "type": "string" },
                        "revised_body": { "type": "string" }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        affected_stages, apply_targeted_repairs, approve_ready_touches, brand_trigger_contract,
        business_copy_context, copy_research_rule, copy_sentence_count,
        cross_recipient_similarity_issue, format_progress_status, generic_subject_label,
        gnk_hedge_moves, has_forced_response_menu, has_natural_response_path,
        is_email_capable_channel, is_empty_linkedin_praise, is_retreat_or_route_touch,
        mentions_outreach_asset, names_historical_outage_result, narrates_internal_copy_logic,
        normalize_dashes, normalize_principle_ids, normalize_thread_subjects, provisional_channel,
        provisional_day_offset, select_people_for_planning, sequence_quality_issues,
        supported_touch_count, supported_touch_count_for_brand, touch_question_limit,
        touch_word_band, unsupported_account_task_noun_issues, wapahki_names_operating_consequence,
        word_set_similarity, CopyAccount, CopySequence, CopyTouch, EditDoc, EditReview,
        PlanProgressRecipient, PlanProgressUpdate, TouchReview,
    };

    #[test]
    fn seven_touch_requests_are_no_longer_collapsed_to_four() {
        assert_eq!(supported_touch_count(7), 7);
        assert_eq!(supported_touch_count(9), 7);
        assert_eq!(supported_touch_count(4), 4);
        assert_eq!(supported_touch_count(2), 2);
        assert_eq!(supported_touch_count(1), 1);
        assert_eq!(supported_touch_count_for_brand("outagehub", 1), 1);
        assert_eq!(supported_touch_count_for_brand("outagehub", 7), 2);
    }
    use crate::business::Businesses;
    use crate::db::{Db, Lead, Person, Sequence, Touch, CURRENT_COPY_POLICY_VERSION};
    use crate::playbook::Playbooks;

    #[test]
    fn outagehub_two_email_contract_requires_completed_historical_evidence() {
        let contract = brand_trigger_contract("outagehub", 2);
        assert!(contract.contains("distributed Canadian operators"));
        assert!(contract.contains("completed location-specific historical utility match"));
        assert!(contract.contains("T2 contributes the verified location and timestamp"));
        assert!(contract.contains("OutageHub's location-matched Canadian utility API"));
        let gnk = brand_trigger_contract("gnk", 4);
        assert!(gnk.contains("specific recurring decision"));
        assert!(gnk.contains("concrete GnK contribution"));
        assert!(gnk.contains("email alternative"));
        let wapahki = brand_trigger_contract("wapahki", 4);
        assert!(wapahki.contains("source-supported physical task"));
        assert!(wapahki.contains("University of Toronto and Automata"));
        assert!(wapahki.contains("Follow-ups advance the same task"));
    }

    #[test]
    fn outagehub_historical_followup_requires_a_completed_timed_result() {
        assert!(names_historical_outage_result(
            "I matched the charging site in Kingston to a utility outage area reported at 14:30 on 2026-07-14."
        ));
        assert!(!names_historical_outage_result(
            "I can prepare a historical comparison for one charging location using a utility report."
        ));
        assert!(!names_historical_outage_result(
            "I found a charging site inside a utility outage area."
        ));
    }

    #[test]
    fn copy_research_findings_compile_into_reusable_rule_categories() {
        assert_eq!(
            copy_research_rule("The premise is unsupported by account evidence").0,
            "evidence_before_copy"
        );
        assert_eq!(
            copy_research_rule("The recipient has no reason to answer").0,
            "reply_likelihood"
        );
        assert_eq!(
            copy_research_rule("The follow-up repeats the same argument").0,
            "sequence_progression"
        );
    }

    #[test]
    fn wapahki_consequence_gate_distinguishes_a_task_from_a_business_wedge() {
        assert!(!wapahki_names_operating_consequence(
            "Operators place finished packs into cases at the end of the line."
        ));
        assert!(wapahki_names_operating_consequence(
            "The station needs extra staffing to keep the line moving."
        ));
        assert!(wapahki_names_operating_consequence(
            "Sanitation changeovers erase the short-run payback."
        ));
        assert!(wapahki_names_operating_consequence(
            "The role description calls for regular 35–40 lb lifting."
        ));
    }

    #[test]
    fn progress_status_exposes_phase_recipient_and_count() {
        assert_eq!(
            format_progress_status(&PlanProgressUpdate {
                phase: "writing touches 1–7".into(),
                account: "Example Co".into(),
                recipient_keys: vec!["maya".into(), "jules".into()],
                state: "active".into(),
                processed: 1,
                accepted: 1,
                rejected: 0,
                held: 0,
                stopped: 0,
                total: 3,
                roster: vec![
                    PlanProgressRecipient {
                        key: "maya".into(),
                        name: "Maya Chen".into(),
                        account: "Example Co".into(),
                    },
                    PlanProgressRecipient {
                        key: "jules".into(),
                        name: "Jules Smith".into(),
                        account: "Example Co".into(),
                    },
                ],
            }),
            "Drafting outreach · writing touches 1–7 · Example Co · Maya Chen + Jules Smith · 1/3 complete"
        );
    }

    #[test]
    fn normalize_dashes_removes_the_ai_tell() {
        // Spaced em dash becomes a comma; nothing dash-like survives.
        let cleaned = normalize_dashes(
            "On a complex claim, an adjuster opens several places — ImageRight, e-Surety — then LexisNexis.",
        );
        assert!(!cleaned.contains('—') && !cleaned.contains('–'));
        assert_eq!(
            cleaned,
            "On a complex claim, an adjuster opens several places, ImageRight, e-Surety, then LexisNexis."
        );
        // En dash between words collapses to a hyphen, not a comma.
        assert_eq!(normalize_dashes("cross–system"), "cross-system");
    }

    #[test]
    fn repetition_check_ignores_greeting_and_signature_boilerplate() {
        let (similarity, overlap) = word_set_similarity(
            "Aldrin,\n\nWashdown can interrupt the line.\n\nAndrew",
            "Aldrin,\n\nA format change may reset the equipment.\n\nAndrew",
            "Andrew",
        );
        assert_eq!(overlap, 0);
        assert_eq!(similarity, 0.0);
    }

    #[test]
    fn cross_recipient_gate_rejects_a_reused_question_skeleton() {
        let db = Db::open(":memory:").expect("open db");
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("gnk").expect("gnk playbook");
        let first_lead = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "cross-sim-org-1".into(),
                name: "First Account".into(),
                ..Default::default()
            })
            .expect("first lead");
        let first_person = db
            .upsert_person(&Person {
                lead_id: first_lead.clone(),
                brand: "gnk".into(),
                apollo_person_id: "cross-sim-person-1".into(),
                name: "Priya".into(),
                ..Default::default()
            })
            .expect("first person");
        let first_sequence = db
            .create_sequence(&Sequence {
                person_id: first_person.clone(),
                lead_id: first_lead.clone(),
                brand: "gnk".into(),
                copy_policy_version: CURRENT_COPY_POLICY_VERSION,
                status: "active".into(),
                ..Default::default()
            })
            .expect("first sequence");
        db.insert_touch(&Touch {
            sequence_id: first_sequence,
            person_id: first_person,
            lead_id: first_lead,
            brand: "gnk".into(),
            stage: 1,
            body: "Hi Priya,\n\nYour operations guide describes disputed cases. When one is escalated, does the reviewer have the supporting evidence together before deciding whether to pursue recovery? GnK builds focused software around that decision. Would you be open to a short conversation, or would an email reply be easier?\n\nAndrew".into(),
            status: "draft".into(),
            ..Default::default()
        })
        .expect("first touch");
        let second_lead = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "cross-sim-org-2".into(),
                name: "Second Account".into(),
                ..Default::default()
            })
            .expect("second lead");
        let second_person_id = db
            .upsert_person(&Person {
                lead_id: second_lead,
                brand: "gnk".into(),
                apollo_person_id: "cross-sim-person-2".into(),
                name: "Morgan".into(),
                ..Default::default()
            })
            .expect("second person");
        let second_person = db
            .get_person(&second_person_id)
            .expect("person query")
            .expect("second person");
        let candidate = CopySequence {
            touches: vec![CopyTouch {
                stage: 1,
                day_offset: 0,
                channel: "email".into(),
                subject: "Escalated case evidence".into(),
                body: "Hi Morgan,\n\nYour services page describes disputed cases. When one is escalated, does the reviewer have the supporting evidence together before deciding whether to pursue recovery? GnK builds focused software around that decision. Would you be open to a short conversation, or would an email reply be easier?\n\nAndrew".into(),
                purpose: String::new(),
                goal: String::new(),
            }],
            applied_principles: Vec::new(),
        };

        let issue = cross_recipient_similarity_issue(
            &db,
            pb,
            &second_person,
            "candidate-sequence",
            &candidate,
        )
        .expect("similarity check");
        assert!(issue.is_some(), "reused structure should be blocked");
    }

    #[test]
    fn sentence_count_ignores_greeting_and_signature() {
        let body = "Maya,\n\nCases change shape between runs. Does that still keep the handoff manual?\n\nAndrew";
        assert_eq!(copy_sentence_count(body, "Andrew"), 2);
    }

    #[test]
    fn first_email_allows_a_bounded_diagnostic_and_cta() {
        assert_eq!(touch_question_limit(1), 3);
        assert!(has_natural_response_path(
            "Could you tell me by email whether that comes up?"
        ));
        assert!(has_natural_response_path(
            "If email is easier, reply with the handoff that still repeats."
        ));
        assert!(has_natural_response_path(
            "Would a 15-minute chat be useful?"
        ));
        assert!(has_natural_response_path(
            "I can send the one-page fit screen if that would be useful?"
        ));
        assert!(has_natural_response_path(
            "You can reply here, or we could discuss it briefly if that is easier."
        ));
        assert!(has_natural_response_path("A brief reply is enough."));
        for stage in 2..=7 {
            assert_eq!(touch_question_limit(stage), 1);
        }
    }

    #[test]
    fn gnk_counts_caveat_moves_without_penalizing_source_safe_artifact_language() {
        assert_eq!(
            gnk_hedge_moves(
                "I assume the temperature record may be in telematics and the setpoint may be in the BOL."
            ),
            1
        );
        assert_eq!(
            gnk_hedge_moves(
                "I wonder whether this exists. It may already solve itself, and perhaps I have this wrong."
            ),
            3
        );
    }

    #[test]
    fn gnk_first_touch_rejects_missing_consequence_and_stacked_caveats() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("gnk").expect("gnk playbook");
        let sequence = CopySequence {
            touches: vec![CopyTouch {
                stage: 1,
                day_offset: 0,
                channel: "email".into(),
                subject: "Refrigerated rejection evidence".into(),
                body: "Hi Kevin,\n\nGnK builds narrow systems for shipment records. I wonder whether a rejected refrigerated load creates a manual review. Perhaps someone still has to pull the temperature history, tender, bill of lading, and receiver paperwork together before deciding whether to dispute the deduction? Would a short call be useful?\n\nAndrew".into(),
                purpose: "test the operating task".into(),
                goal: "earn a reply".into(),
            }],
            applied_principles: Vec::new(),
        };
        let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &[], 1, false);
        assert!(
            issues.iter().any(|issue| issue.contains("consequence")),
            "issues were {issues:?}"
        );
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("hypothesis caveats")),
            "issues were {issues:?}"
        );
    }

    #[test]
    fn targeted_repairs_are_atomic_when_an_editor_omits_a_required_stage() {
        let touch = |stage, body: &str| CopyTouch {
            stage,
            day_offset: stage,
            channel: "email".into(),
            subject: "claim review".into(),
            body: body.into(),
            purpose: String::new(),
            goal: String::new(),
        };
        let mut sequence = CopySequence {
            touches: vec![touch(1, "original one"), touch(2, "original two")],
            applied_principles: Vec::new(),
        };
        let doc = EditDoc {
            sequence_passes: true,
            sequence_score: 90,
            sequence_issues: Vec::new(),
            reviews: vec![
                EditReview {
                    stage: 1,
                    passes: true,
                    score: 90,
                    issues: Vec::new(),
                    revised_subject: String::new(),
                    revised_body: "changed one".into(),
                },
                EditReview {
                    stage: 2,
                    passes: true,
                    score: 90,
                    issues: Vec::new(),
                    revised_subject: String::new(),
                    revised_body: String::new(),
                },
            ],
        };

        let error = apply_targeted_repairs(
            &mut sequence,
            &doc,
            &["stage 1 is too short".into(), "stage 2 is too short".into()],
            2,
        )
        .expect_err("missing stage must reject the edit atomically");

        assert!(error.to_string().starts_with("stage 2 "));
        assert_eq!(sequence.touches[0].body, "original one");
        assert_eq!(sequence.touches[1].body, "original two");
    }

    #[test]
    fn gnk_email_bands_leave_room_for_substance_without_a_dossier() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("gnk").expect("gnk playbook");
        for (stage, expected) in [
            (1, (75, 130)),
            (2, (35, 145)),
            (4, (35, 120)),
            (6, (25, 80)),
        ] {
            let touch = CopyTouch {
                stage,
                day_offset: stage,
                channel: "email".into(),
                subject: "claim review".into(),
                body: String::new(),
                purpose: String::new(),
                goal: String::new(),
            };
            assert_eq!(touch_word_band(pb, &touch), expected);
        }
        let objection_touch = CopyTouch {
            stage: 5,
            day_offset: 13,
            channel: "linkedin_or_email".into(),
            subject: "re: claim review".into(),
            body: String::new(),
            purpose: String::new(),
            goal: String::new(),
        };
        assert_eq!(touch_word_band(pb, &objection_touch), (35, 100));
    }

    #[test]
    fn retreat_detection_distinguishes_a_contribution_from_an_escape_hatch() {
        let touch = |stage, body: &str| CopyTouch {
            stage,
            day_offset: stage,
            channel: "email".into(),
            subject: "claim review".into(),
            body: body.into(),
            purpose: String::new(),
            goal: String::new(),
        };
        assert!(is_retreat_or_route_touch(&touch(
            2,
            "If this is already covered, a correction would settle it."
        )));
        assert!(is_retreat_or_route_touch(&touch(
            6,
            "If someone else handles the review, a redirect is useful."
        )));
        assert!(!is_retreat_or_route_touch(&touch(
            4,
            "I can prepare an outside-in decision-trail sketch with every assumption marked."
        )));
        assert!(!is_retreat_or_route_touch(&touch(
            1,
            "Would a short conversation help, or should I ask someone closer to the work?"
        )));
    }

    #[test]
    fn response_menus_and_experiment_narration_are_not_human_copy() {
        assert!(has_forced_response_menu(
            "Within-run, between-run, or both is enough to classify it."
        ));
        assert!(has_forced_response_menu(
            "A yes, no, or the right person's name is plenty."
        ));
        assert!(has_forced_response_menu(
            "A one-line note on where the work sits would help."
        ));
        assert!(!has_forced_response_menu(
            "Even a short email about where the work stays manual would help."
        ));
        assert!(narrates_internal_copy_logic(
            "The premise was narrow: one handling task changes by format."
        ));
        assert!(narrates_internal_copy_logic(
            "The distinction I have in mind is detection versus response."
        ));
    }

    #[test]
    fn subject_gate_rejects_topical_labels_but_keeps_operating_scenes() {
        for subject in [
            "utility status",
            "power alarms",
            "claim evidence",
            "decision trail",
            "utility context for site alarms",
        ] {
            assert!(generic_subject_label(subject), "should reject {subject}");
        }
        for subject in [
            "Separating grid outages from site faults",
            "Attributing downtime at charging sites",
            "Reconstructing delay evidence after project slips",
            "Bottle packing across format changes",
        ] {
            assert!(!generic_subject_label(subject), "should keep {subject}");
        }
    }

    #[test]
    fn wapahki_specificity_cannot_come_only_from_an_internal_hypothesis() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("wapahki").expect("wapahki playbook");
        let account = CopyAccount {
            name: "Fresh Foods".into(),
            industry: "food production".into(),
            hq: "Ontario".into(),
            observed_facts: vec![
                "Fresh Foods operates six kitchens across five brands.".into(),
                "The company makes prepared foods.".into(),
            ],
            inferences: Vec::new(),
            hypothesis: "A tray-loading task may remain manual.".into(),
            mechanism: String::new(),
            consequence_metric: String::new(),
            signals: Vec::new(),
            system_concept: String::new(),
            hard_buyer_question: String::new(),
            kill_condition: String::new(),
            magnitude_note: String::new(),
            applied_principles: Vec::new(),
        };
        let sequence = CopySequence {
            touches: vec![CopyTouch {
                stage: 1,
                day_offset: 0,
                channel: "email".into(),
                subject: "Packing format changes".into(),
                body: "Hi Maya,\n\nDoes a finished-tray case-loading task remain manual?\n\nAndrew"
                    .into(),
                purpose: "test one task".into(),
                goal: "earn a correction".into(),
            }],
            applied_principles: Vec::new(),
        };
        let issues = unsupported_account_task_noun_issues(pb, &account, &sequence);
        assert!(issues.iter().any(|issue| issue.contains("unverified tray")));

        let mut supported = account;
        supported.observed_facts =
            vec!["The plant describes a tray-filling and case-loading station.".into()];
        assert!(unsupported_account_task_noun_issues(pb, &supported, &sequence).is_empty());
    }

    #[test]
    fn product_language_is_not_mistaken_for_repeated_collateral() {
        assert!(!mentions_outreach_asset(
            "GnK builds a narrow decision-trail view around existing records."
        ));
        assert!(mentions_outreach_asset(
            "I can prepare a one-page outside-in sketch with assumptions marked."
        ));
    }

    #[test]
    fn sequence_gate_rejects_a_polite_seven_touch_retreat_loop() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("gnk").expect("gnk playbook");
        let channels = [
            "email",
            "email",
            "linkedin_request",
            "email",
            "linkedin_or_email",
            "email",
            "linkedin_or_email",
        ];
        let days = [0, 3, 5, 9, 13, 17, 21];
        let bodies = [
            "Maya,\n\nThe public claims page raised a record question. Am I off base?\n\nAndrew",
            "Maya,\n\nIf this is already covered, a correction would settle it.\n\nAndrew",
            "Your claims work is a relevant reason to connect around decision records.",
            "Maya,\n\nIf the distinction is not material, I will set this aside.\n\nAndrew",
            "Maya,\n\nIf someone else handles review, a redirect would help.\n\nAndrew",
            "Maya,\n\nI may have the wrong premise and will stop pursuing it.\n\nAndrew",
            "Maya,\n\nI will close this out here.\n\nAndrew",
        ];
        let touches = channels
            .iter()
            .enumerate()
            .map(|(index, channel)| CopyTouch {
                stage: (index + 1) as u32,
                day_offset: days[index],
                channel: (*channel).into(),
                subject: if *channel == "linkedin_request" {
                    String::new()
                } else if index == 0 {
                    "claim review".into()
                } else {
                    "re: claim review".into()
                },
                body: bodies[index].into(),
                purpose: "continue".into(),
                goal: "reply".into(),
            })
            .collect();
        let issues = sequence_quality_issues(
            pb,
            &playbooks.shared,
            &CopySequence {
                touches,
                applied_principles: Vec::new(),
            },
            &[],
            7,
            false,
        );
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("retreat, correction, routing, or closure")),
            "issues were {issues:?}"
        );
    }

    #[test]
    fn business_context_reaches_the_writer() {
        let businesses = Businesses::load("businesses").expect("load businesses");
        let context = business_copy_context(businesses.get("gnk").expect("gnk business"));
        assert!(context.contains("GnK builds custom software and AI systems"));
        assert!(!context.contains("outside-in workflow or decision-trail sketch"));
        assert!(context.contains("Do not invent savings"));
        assert!(!context.contains("Land a narrow paid pilot"));
        assert!(context.contains("market-level seller evidence, never proof about the recipient"));

        let wapahki = business_copy_context(businesses.get("wapahki").expect("wapahki business"));
        assert!(wapahki.contains("first_party_market_discovery"));
        assert!(wapahki.contains("permitted_follow_up_angles"));
        assert!(wapahki.contains("never proof about the recipient"));
        assert!(wapahki.contains("wapahki-call-factory-packing-01"));
    }

    #[test]
    fn knowledge_citations_must_come_from_the_retrieved_set() {
        let citations = normalize_principle_ids(
            &[
                "[brevity-as-buyer-respect]".into(),
                "made-up".into(),
                "brevity-as-buyer-respect".into(),
            ],
            &[
                "brevity-as-buyer-respect".into(),
                "channel-fit-selection".into(),
            ],
        );
        assert_eq!(citations, vec!["brevity-as-buyer-respect"]);
    }

    #[test]
    fn bulk_planning_honors_the_requested_contacts_per_account() {
        let person =
            |id: &str, lead: &str, name: &str, title: &str, vantage: &str, primary| Person {
                id: id.into(),
                lead_id: lead.into(),
                name: name.into(),
                title: title.into(),
                vantage: vantage.into(),
                primary,
                ..Default::default()
            };
        let selected = select_people_for_planning(
            vec![
                person("r", "a", "Recruiter", "Senior Recruiter", "router", false),
                person("o", "a", "Owner", "Claims Manager", "process_owner", true),
                person(
                    "e",
                    "a",
                    "Executive",
                    "VP Operations",
                    "operational_executive",
                    false,
                ),
                person("f", "a", "Finance", "Controller", "economic_buyer", false),
                person(
                    "t",
                    "a",
                    "Technical",
                    "Systems Engineer",
                    "technical_evaluator",
                    false,
                ),
                person("x", "a", "Coordinator", "Coordinator", "router", false),
                person(
                    "b",
                    "b",
                    "Other Owner",
                    "Operations Manager",
                    "process_owner",
                    true,
                ),
            ],
            5,
        );
        let account_a = selected
            .iter()
            .filter(|person| person.lead_id == "a")
            .map(|person| person.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(account_a, vec!["o", "e", "f", "t", "x"]);
        assert_eq!(
            selected
                .iter()
                .filter(|person| person.lead_id == "b")
                .count(),
            1
        );
    }

    #[test]
    fn manual_approval_cannot_bypass_current_gtm_readiness() {
        let db = Db::open(":memory:").expect("open db");
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("gnk").expect("gnk playbook");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "oversized-org".into(),
                name: "Oversized Account".into(),
                headcount: 35_000,
                status: "qualified".into(),
                ..Default::default()
            })
            .expect("lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "oversized-person".into(),
                name: "Pat Owner".into(),
                title: "Operations Director".into(),
                vantage: "process_owner".into(),
                email: "pat@example.com".into(),
                email_status: "verified".into(),
                status: "verified".into(),
                ..Default::default()
            })
            .expect("person");
        let sequence_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                copy_policy_version: CURRENT_COPY_POLICY_VERSION,
                status: "active".into(),
                ..Default::default()
            })
            .expect("sequence");
        db.insert_touch(&Touch {
            sequence_id,
            person_id: person_id.clone(),
            lead_id,
            brand: "gnk".into(),
            stage: 1,
            channel: "email".into(),
            status: "draft".into(),
            review_passes: Some(true),
            ..Default::default()
        })
        .expect("touch");

        let approval = approve_ready_touches(&db, pb, Some(&person_id)).expect("approval");
        assert_eq!(approval.touches_scheduled, 0);
        assert_eq!(approval.people_held, 1);
        assert!(approval.hold_reasons[0].contains("current GTM state"));
        assert_eq!(
            db.list_touches_for_person(&person_id).unwrap()[0].status,
            "draft"
        );
    }

    #[test]
    fn deterministic_findings_route_only_to_affected_touches() {
        assert_eq!(affected_stages("stage 3 is 80 words", 7), vec![3]);
        assert_eq!(
            affected_stages("email stages 1 and 5 are 70% repetitive", 7),
            vec![1, 5]
        );
        assert!(affected_stages("seven-touch channel order is invalid", 7).is_empty());
    }

    #[test]
    fn follow_up_subjects_stay_in_one_real_email_thread() {
        let mut sequence = CopySequence {
            touches: vec![
                CopyTouch {
                    stage: 1,
                    day_offset: 0,
                    channel: "email".into(),
                    subject: "case handling".into(),
                    body: String::new(),
                    purpose: String::new(),
                    goal: String::new(),
                },
                CopyTouch {
                    stage: 2,
                    day_offset: 3,
                    channel: "email".into(),
                    subject: "another headline".into(),
                    body: String::new(),
                    purpose: String::new(),
                    goal: String::new(),
                },
                CopyTouch {
                    stage: 3,
                    day_offset: 5,
                    channel: "linkedin_request".into(),
                    subject: "should disappear".into(),
                    body: String::new(),
                    purpose: String::new(),
                    goal: String::new(),
                },
                CopyTouch {
                    stage: 5,
                    day_offset: 13,
                    channel: "linkedin_or_email".into(),
                    subject: "fallback headline".into(),
                    body: String::new(),
                    purpose: String::new(),
                    goal: String::new(),
                },
            ],
            applied_principles: Vec::new(),
        };
        normalize_thread_subjects(&mut sequence);
        assert_eq!(sequence.touches[0].subject, "case handling");
        assert_eq!(sequence.touches[1].subject, "re: case handling");
        assert!(sequence.touches[2].subject.is_empty());
        assert_eq!(sequence.touches[3].subject, "re: case handling");
    }

    #[test]
    fn linkedin_connection_requests_enforce_the_real_character_limit() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("wapahki").expect("wapahki playbook");
        let sequence = CopySequence {
            touches: vec![CopyTouch {
                stage: 1,
                day_offset: 0,
                channel: "linkedin_request".into(),
                subject: String::new(),
                body: "a".repeat(301),
                purpose: String::new(),
                goal: String::new(),
            }],
            applied_principles: Vec::new(),
        };
        let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &[], 1, false);
        assert!(issues.iter().any(|issue| issue.contains("maximum 300")));
    }

    #[test]
    fn linkedin_connection_requests_reject_empty_role_praise() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("outagehub").expect("outagehub playbook");
        let sequence = CopySequence {
            touches: vec![CopyTouch {
                stage: 1,
                day_offset: 0,
                channel: "linkedin_request".into(),
                subject: String::new(),
                body: "Reliability engineering across that mix is a substantial remit; I'd be glad to connect."
                    .into(),
                purpose: String::new(),
                goal: String::new(),
            }],
            applied_principles: Vec::new(),
        };
        let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &[], 1, false);
        assert!(issues
            .iter()
            .any(|issue| issue.contains("empty compliment")));
        assert!(!is_empty_linkedin_praise(
            "I'm comparing how multi-site operators separate utility events from equipment alarms. Glad to connect."
        ));
    }

    #[test]
    fn current_email_envelope_rules_apply_to_every_sales_brand() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        for brand in ["wapahki", "gnk", "outagehub"] {
            let pb = playbooks.get(brand).expect("brand playbook");
            let signature = pb.signature.clone();
            let email = |stage, day, middle: &str| CopyTouch {
                stage,
                day_offset: day,
                channel: "email".into(),
                subject: if stage == 1 {
                    "Case handling decisions".into()
                } else {
                    "re: Case handling decisions".into()
                },
                body: format!("Hi Maya,\n\n{middle}\n\n{signature}"),
                purpose: "continue one conversation".into(),
                goal: "earn a correction".into(),
            };
            let mut sequence = CopySequence {
                touches: vec![
                    email(
                        1,
                        0,
                        "Your product range suggests the final handoff may change between formats, but that does not show whether the work is manual. Does one recurring case-handling step still need an operator during a normal run? A simple correction would keep this focused on the right work.",
                    ),
                    email(
                        2,
                        3,
                        "The only point I am trying to place is whether that handoff exists before getting into equipment or economics. Even an already automated answer is useful.",
                    ),
                    CopyTouch {
                        stage: 3,
                        day_offset: 5,
                        channel: "linkedin_request".into(),
                        subject: String::new(),
                        body: "I am comparing where changing formats still leave one steady production handoff. Your operating perspective made this a relevant reason to connect.".into(),
                        purpose: "connect around the operating question".into(),
                        goal: "create a human channel".into(),
                    },
                    email(
                        4,
                        9,
                        "I can share the short screen I use to rule work out early: motion, changes, interventions, physical limits, and required pace. Would that be useful for the person closest to the handoff?",
                    ),
                    CopyTouch {
                        stage: 5,
                        day_offset: 13,
                        channel: "linkedin_or_email".into(),
                        subject: "re: Case handling decisions".into(),
                        body: format!("Hi Maya,\n\nThe screen is practical and takes a minute to scan. Happy to send it without arranging a call.\n\n{signature}"),
                        purpose: "offer one useful resource".into(),
                        goal: "make replying worthwhile".into(),
                    },
                    email(
                        6,
                        17,
                        "If this sits elsewhere, who sees the final production handoff closely enough to confirm whether it is still manual? A name is plenty.",
                    ),
                    CopyTouch {
                        stage: 7,
                        day_offset: 21,
                        channel: "linkedin_or_email".into(),
                        subject: "re: Case handling decisions".into(),
                        body: format!("Hi Maya,\n\nI will close the thread here. Thanks for considering it.\n\n{signature}"),
                        purpose: "close".into(),
                        goal: "stop respectfully".into(),
                    },
                ],
                applied_principles: Vec::new(),
            };
            normalize_thread_subjects(&mut sequence);
            let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &[], 7, false);
            assert!(
                !issues.iter().any(|issue| {
                    issue.contains("must start with 'Hi [First name],'")
                        || issue.contains("must keep the original email subject")
                        || issue.contains("subject has")
                        || issue.contains("maximum 300")
                }),
                "{brand} envelope issues were {issues:?}"
            );
        }
    }

    #[test]
    fn sequence_gate_stops_an_asset_from_becoming_the_whole_campaign() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("gnk").expect("gnk playbook");
        let signature = pb.signature.clone();
        let email = |stage, day, middle: &str| CopyTouch {
            stage,
            day_offset: day,
            channel: "email".into(),
            subject: if stage == 1 {
                "claim review".into()
            } else {
                "re: claim review".into()
            },
            body: format!("Maya,\n\n{middle}\n\n{signature}"),
            purpose: "continue one conversation".into(),
            goal: "earn a reply".into(),
        };
        let sequence = CopySequence {
            touches: vec![
                email(1, 0, "Your public claims material raised a narrow question about the record behind a complex decision. I have a checklist for that review. Would it be relevant to your team?"),
                email(2, 3, "The checklist separates the decision from the records that support it, without assuming anything about your current process."),
                CopyTouch {
                    stage: 3,
                    day_offset: 5,
                    channel: "linkedin_request".into(),
                    subject: String::new(),
                    body: "Your claims operations role made this a relevant reason to connect around complex review records.".into(),
                    purpose: "connect".into(),
                    goal: "connect".into(),
                },
                email(4, 9, "The checklist is intentionally short and shows which source supports each material decision. I can share it if useful."),
                CopyTouch {
                    stage: 5,
                    day_offset: 13,
                    channel: "linkedin_or_email".into(),
                    subject: "re: claim review".into(),
                    body: format!("Maya,\n\nThe record question may sit with another colleague. A role or function is plenty.\n\n{signature}"),
                    purpose: "route".into(),
                    goal: "route".into(),
                },
                email(6, 17, "If complex claim reviews already carry a clear source trail, I have the wrong premise and will stop pursuing it."),
                CopyTouch {
                    stage: 7,
                    day_offset: 21,
                    channel: "linkedin_or_email".into(),
                    subject: "re: claim review".into(),
                    body: format!("Maya,\n\nI will close the thread here.\n\n{signature}"),
                    purpose: "close".into(),
                    goal: "close".into(),
                },
            ],
            applied_principles: Vec::new(),
        };
        let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &[], 7, false);
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("repeats collateral across 3 touches")),
            "issues were {issues:?}"
        );
    }

    #[test]
    fn sendability_gate_rejects_repeated_email_copy() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("gnk").expect("gnk playbook");
        let channels = [
            "email",
            "email",
            "linkedin_request",
            "email",
            "linkedin_or_email",
            "email",
            "linkedin_or_email",
        ];
        let days = [0, 3, 5, 9, 13, 17, 21];
        let repeated = "Rosario, disputed loads can leave the supporting record spread across messages, appointments, and shipment documents. GnK builds narrow tools around work like this. Is assembling that record still a manual step for your operations team?\n\nAndrew";
        let short = "Rosario, I am trying to understand who sees the disputed-load record come together at Fuze. Is that part of your operations remit?";
        let touches = channels
            .iter()
            .enumerate()
            .map(|(index, channel)| CopyTouch {
                stage: (index + 1) as u32,
                day_offset: days[index],
                channel: (*channel).into(),
                subject: if is_email_capable_channel(channel) {
                    "Disputed load records".into()
                } else {
                    String::new()
                },
                body: if is_email_capable_channel(channel) {
                    repeated.into()
                } else {
                    short.into()
                },
                purpose: "new angle".into(),
                goal: "earn a correction".into(),
            })
            .collect();
        let sequence = CopySequence {
            touches,
            applied_principles: vec!["brevity-as-buyer-respect".into()],
        };
        let reviews = (1..=7)
            .map(|stage| TouchReview {
                stage,
                passes: true,
                score: 90,
                issues: Vec::new(),
            })
            .collect::<Vec<_>>();
        let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &reviews, 7, true);
        assert!(
            issues.iter().any(|issue| issue.contains("repetitive")),
            "issues were {issues:?}"
        );
    }

    #[test]
    fn wapahki_gate_limits_questions_and_brand_repetition() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("wapahki").expect("wapahki playbook");
        let channels = [
            "email",
            "email",
            "linkedin_request",
            "email",
            "linkedin_or_email",
            "email",
            "linkedin_or_email",
        ];
        let days = [0, 3, 5, 9, 13, 17, 21];
        let touches = channels
            .iter()
            .enumerate()
            .map(|(index, channel)| CopyTouch {
                stage: (index + 1) as u32,
                day_offset: days[index],
                channel: (*channel).into(),
                subject: if is_email_capable_channel(channel) {
                    "case changes".into()
                } else {
                    String::new()
                },
                body: format!(
                    "Maya, Wapahki is looking at case handling angle {}. Does that vary by run?\n\nAndrew",
                    index + 1
                ),
                purpose: "test one task".into(),
                goal: "earn a correction".into(),
            })
            .collect();
        let sequence = CopySequence {
            touches,
            applied_principles: vec!["brevity-as-buyer-respect".into()],
        };

        let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &[], 7, false);
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("questions in 7 touches")),
            "issues were {issues:?}"
        );
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("Wapahki appears 7 times")),
            "issues were {issues:?}"
        );
    }

    #[test]
    fn shared_gate_limits_questions_and_brand_repetition_for_outagehub() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("outagehub").expect("outagehub playbook");
        let channels = [
            "email",
            "email",
            "linkedin_request",
            "email",
            "linkedin_or_email",
            "email",
            "linkedin_or_email",
        ];
        let days = [0, 3, 5, 9, 13, 17, 21];
        let touches = channels
            .iter()
            .enumerate()
            .map(|(index, channel)| CopyTouch {
                stage: (index + 1) as u32,
                day_offset: days[index],
                channel: (*channel).into(),
                subject: if is_email_capable_channel(channel) {
                    "utility status".into()
                } else {
                    String::new()
                },
                body: if index == 2 {
                    "I sent a note about OutageHub. Glad to connect.".into()
                } else {
                    format!(
                        "OutageHub is one possible input for utility event angle {}. Does that change the operating decision?\n\nAndrew Gordienko",
                        index + 1
                    )
                },
                purpose: "test one decision".into(),
                goal: "earn a correction".into(),
            })
            .collect();
        let sequence = CopySequence {
            touches,
            applied_principles: vec!["brevity-as-buyer-respect".into()],
        };

        let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &[], 7, false);
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("questions in 6 touches")),
            "issues were {issues:?}"
        );
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("OutageHub appears 7 times")),
            "issues were {issues:?}"
        );
        assert!(
            issues.iter().any(|issue| issue.contains("I sent a note")),
            "issues were {issues:?}"
        );
    }

    #[test]
    fn outagehub_gate_rejects_the_repeated_cory_sequence_shape() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("outagehub").expect("outagehub playbook");
        let signature = pb.signature.clone();
        let email = |stage, day, middle: &str| CopyTouch {
            stage,
            day_offset: day,
            channel: "email".into(),
            subject: if stage == 1 {
                "power alert context".into()
            } else {
                "re: power alert context".into()
            },
            body: format!("Cory,\n\n{middle}\n\n{signature}"),
            purpose: "continue the thread".into(),
            goal: "earn a reply".into(),
        };
        let sequence = CopySequence {
            touches: vec![
                email(1, 0, "Across Conestoga's cold-storage warehouses, a possible power alert can create a choice between a facility problem and a wider utility event. Public utility reports would not confirm conditions inside a facility, but tied to a location and time they may show a wider reported event. How is that separated before maintenance is escalated?"),
                email(2, 3, "Public utility updates cannot establish whether refrigeration or generator controls are operating. OutageHub can provide outside context through a reported utility event and independent timeline. I can prepare a historical comparison for Conestoga locations."),
                CopyTouch {
                    stage: 3,
                    day_offset: 7,
                    channel: "linkedin_request".into(),
                    subject: String::new(),
                    body: "Cory, a public utility report cannot establish conditions inside a Conestoga facility. Connecting in case a historical comparison for those locations would be useful.".into(),
                    purpose: "connect".into(),
                    goal: "connect".into(),
                },
                email(4, 14, "A public utility update is useful only when it aligns with a location and time; it cannot establish refrigeration conditions inside a facility. If this comparison is handled elsewhere, would you point me to the right person?"),
            ],
            applied_principles: Vec::new(),
        };

        let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &[], 4, false);
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("exactly two emails")),
            "issues were {issues:?}"
        );
    }

    #[test]
    fn seven_touch_cadence_uses_email_and_linkedin_without_calls() {
        let channels = (1..=7).map(provisional_channel).collect::<Vec<_>>();
        let days = (1..=7)
            .map(|stage| provisional_day_offset(stage, 7))
            .collect::<Vec<_>>();
        assert_eq!(
            channels,
            vec![
                "email",
                "email",
                "linkedin_request",
                "email",
                "linkedin_or_email",
                "email",
                "linkedin_or_email",
            ]
        );
        assert_eq!(days, vec![0, 3, 5, 9, 13, 17, 21]);
        assert!(!channels.contains(&"call"));
    }

    #[test]
    fn two_touch_evidence_experiment_is_email_only_on_days_zero_and_six() {
        let channels = (1..=2).map(provisional_channel).collect::<Vec<_>>();
        let days = (1..=2)
            .map(|stage| provisional_day_offset(stage, 2))
            .collect::<Vec<_>>();
        assert_eq!(channels, vec!["email", "email"]);
        assert_eq!(days, vec![0, 6]);
    }
}
