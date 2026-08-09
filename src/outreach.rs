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

#[derive(Debug, Default)]
pub struct PlanSummary {
    pub people_planned: usize,
    pub touches_scheduled: usize,
    pub touches_drafted: usize,
    pub sequences_replaced: usize,
    pub people_rejected: usize,
    pub people_held: usize,
    pub people_stopped: usize,
    pub stopped_reason: Option<String>,
}

fn log_outreach(message: impl AsRef<str>) {
    if !crate::ui::fancy() {
        eprintln!("  · {}", message.as_ref());
    }
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
    #[serde(default)]
    touches: Vec<TouchPlan>,
    #[serde(default)]
    applied_principles: Vec<String>,
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

fn create_building_checkpoint(
    db: &SharedDb,
    pb: &Playbook,
    lead: &crate::db::Lead,
    person: &crate::db::Person,
    gtm_context: &GtmActionContext,
    touches: usize,
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
                    let mut issues = vec![format!("sendability score: {}/100", review.score)];
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
    progress_reporter: Option<PlanProgressReporter>,
) -> Result<PlanSummary> {
    let requested_touches = n_touches.max(1);
    let eager_full_sequence = std::env::var("SPRUCE_EAGER_FULL_SEQUENCE")
        .ok()
        .is_some_and(|value| matches!(value.trim(), "1" | "true" | "on"));
    let n_touches = if requested_touches == 1 {
        1
    } else if eager_full_sequence && requested_touches >= 7 {
        7
    } else {
        4
    };
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
    // row. Bulk motions activate one primary workflow owner per account; other
    // verified contacts remain fallbacks after a route, bounce, or completed
    // thread. Parallel cold sequences are not account personalization.
    let mut verified = db.list_people(Some(&pb.key), Some("verified"))?;
    // The full motion scopes drafting to the people IT just sourced, so a run
    // doesn't re-draft the brand's entire accumulated backlog every time.
    if let Some(ids) = only_person_ids {
        verified.retain(|person| ids.contains(&person.id));
    }
    let selected = if person_filter.is_some() {
        verified
            .into_iter()
            .filter(|person| person_matches(person, person_filter))
            .collect::<Vec<_>>()
    } else {
        let _requested_cap = per_account_cap;
        select_people_for_planning(verified, 1)
    };
    let mut todo = Vec::new();
    let mut matched_people = 0;
    let mut people_held = 0usize;
    for p in selected {
        matched_people += 1;
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
            todo.push((p, Some(sequence_id)));
        } else {
            todo.push((p, None));
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

    // Group by account so its evidence, business context, and knowledge are sent
    // once, then split each account into small chunks: one writer call produces
    // every recipient's full sequence, so batching several recipients still
    // creates enough copy to blow the model's per-call timeout
    // and get the whole account rejected. Capping recipients per call keeps each
    // call bounded and lets the account's other recipients still succeed.
    let max_recipients_per_call = if client.backend() == crate::engine::Backend::Openai {
        3
    } else {
        // A CLI process cannot share one HTTP connection or a server-side
        // queue; keep each invocation independently bounded.
        1
    };
    let leads = db.list_leads(Some(&pb.key))?;
    let roster = todo
        .iter()
        .map(|(person, _)| PlanProgressRecipient {
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
    let mut grouped: HashMap<String, Vec<(crate::db::Person, Option<String>)>> = HashMap::new();
    for (person, replaced_sequence) in todo {
        grouped
            .entry(person.lead_id.clone())
            .or_default()
            .push((person, replaced_sequence));
    }
    let mut groups = grouped.into_iter().collect::<Vec<_>>();
    groups.sort_by(|left, right| left.0.cmp(&right.0));
    // Fan each account out into bounded recipient chunks.
    type AccountRecipients = Vec<(crate::db::Person, Option<String>)>;
    let units: Vec<(String, AccountRecipients)> = groups
        .into_iter()
        .flat_map(|(lead_id, people)| {
            people
                .chunks(max_recipients_per_call)
                .map(|chunk| (lead_id.clone(), chunk.to_vec()))
                .collect::<Vec<_>>()
        })
        .collect();
    let business_context = business_copy_context(business);
    let performance_context = empirical_copy_context(db, &pb.key)?;
    let stopped_reason = Arc::new(Mutex::new(None::<String>));
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
                    match create_building_checkpoint(&db, pb, &lead, person, context, n_touches) {
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

    // A bulk replacement also retires unsent legacy sequences for lower-priority
    // contacts at each successfully replanned account. Otherwise the CRM would
    // keep displaying the old five-person blast even though the new policy works
    // only the strongest workflow owner. Sent history is never removed.
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
    let now = Utc::now();
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
                let mut issues = vec![format!("sendability score: {}/100", review.score)];
                issues.extend(review.issues.clone());
                issues
            })
            .unwrap_or_else(|| lint.forbidden_hits.clone());

        let can_automate = t.channel.eq_ignore_ascii_case("email")
            || (t.channel.eq_ignore_ascii_case("linkedin_or_email")
                && person.linkedin_status != "connected");
        let status = if can_automate && auto_schedule && passes && gtm_context.action_ready() {
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
async fn plan_sequence(
    client: &Engine,
    plan_system: &str,
    account: &CopyAccount,
    person: &crate::db::Person,
    n: usize,
    knowledge: &RoleKnowledge,
    gtm_context: &str,
) -> Result<SequencePlan> {
    let account = planner_account_brief(account);
    let discovery_only = gtm_context.contains("GTM ACTION STATE: discovery_ready");
    let recipient = json!({
        "name": person.name,
        "first_name": person.first_name,
        "title": person.title,
        "vantage": person.vantage,
        "likely_access_internal_only": if discovery_only { "" } else { person.can_observe.as_str() },
        "ask_scope": recipient_ask_scope(person),
    });
    let user = format!(
        "Plan a {n}-touch no-reply sequence for this recipient. Hypotheses define the decision and mechanism but are not facts. Before selecting the thread, compare three distinct T1 approaches: problem-sniffing from the strongest source, a concise commercial point of view, and an existence-or-routing note. Prefer the one with the clearest recipient reason to answer and lowest evidence risk; do not blend them. State the selected approach and why in overall_strategy.\n\nACCOUNT BRIEF:\n{account}\n\nRECIPIENT:\n{recipient}\n\nPRIVATE GTM ACTION CONTEXT:\n{gtm_context}\n\nRELEVANT PLANNING KNOWLEDGE:\n{knowledge}\n\nT1 connects a verified trigger to one operating decision and one role-matched ask. T2 advances the mechanism. T3 is a human LinkedIn request. T4, when present, adds a sourced fact, useful distinction, objection answer, route, or close. Never invent collateral or a later stage merely to fill the plan. For each touch return stage, channel, objective, angle, and at most one ask. Never make LinkedIn say only that an email was sent. If action state is research_required, do not plan. Cite only principle IDs that changed the plan.",
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
                        account_ref,
                        person,
                        n,
                        &knowledge.planner,
                        &gtm_contexts
                            .get(&person.id)
                            .map(GtmActionContext::prompt_block)
                            .unwrap_or_else(|| "GTM ACTION STATE: unavailable".into()),
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
        let discovery_only = gtm_contexts
            .get(&person.id)
            .is_some_and(|context| context.state == "discovery_ready");
        json!({
            "person_key": person.id,
            "name": person.name,
            "first_name": person.first_name,
            "title": person.title,
            "vantage": person.vantage,
            "likely_access_internal_only": if discovery_only { "" } else { person.can_observe.as_str() },
            "why_this_person_internal_only": if discovery_only { "Selected from verified title and mapped operating vantage only; do not infer access to the private workflow." } else { person.why_them.as_str() },
            "person_research_status": if person.linkedin_url.trim().is_empty() {
                "No person-level profile source is on file. Do not fake individual personalization; use the account signal and role-appropriate ask."
            } else {
                "A LinkedIn URL is on file, but its profile content has not been retrieved. The title is the only verified person-level insight; do not imply posts, tenure, priorities, or biography."
            },
            "verified_person_insights": Vec::<String>::new(),
            "ask_scope": recipient_ask_scope(person),
            "sequence_plan": plans.get(&person.id),
            "copy_decision_context": gtm_contexts
                .get(&person.id)
                .map(GtmActionContext::copy_prompt_block)
                .unwrap_or_else(|| "GTM ACTION STATE: unavailable".into()),
        })
    };
    let writer_account = writer_account_brief(&account);
    let planning_contract = if lean {
        "For each recipient, choose one source-backed trigger and one operating decision this title can plausibly answer. Keep the mechanism explicitly unverified. Privately draft three genuinely different T1 candidates: a problem-sniffing note, a concise point of view, and an existence-or-routing note. Pick one; never blend or return the alternatives. T2 must sharpen the mechanism rather than restate T1. T3 is a natural connection request. If T4 is present, add only a sourced fact, a useful concrete distinction, or an honest answer to the strongest objection; never invent an artifact to fill the slot. The sequence stays on one human thread and must not become an interview or a chain of retreats. Do not expose the private plan or discarded candidates."
    } else {
        "Follow each recipient's supplied private sequence_plan."
    };
    let writer_knowledge = knowledge.writer.block.clone();
    let writer_instructions = format!(
        r#"Write one {n}-touch no-reply sequence for each recipient. {planning_contract}

Think through the buyer-safe brief and copy decision context. Return exactly one result for every person_key. First choose send_decision. Use send only when verified facts support the trigger, the title can credibly answer the ask, and one natural first note can test the hypothesis without pretending it is true. Otherwise choose hold_for_research, explain the missing evidence privately in decision_reason, and return no touches. Abstention is better than filler. For a send decision, privately state the operating decision, mechanism to test, strongest objection, recipient's reason to reply, and supported give-back. Never invent collateral, customer proof, or prior analysis.

T1 must use the brand-specific word band. It cannot be a compressed diagnostic question followed by a vague capability sentence. Connect one verified trigger to a specific operating decision, frame the plausible mechanism as uncertainty, make the consequence or useful distinction concrete, explain one narrow seller capability only when helpful, and end with one role-matched ask. A direct workflow owner may be asked for a short conversation to compare the precise hypothesis with actual work; a router may only be asked to route.

For four touches use email/0, email/3, linkedin_request/7, email/14. For seven touches use email/0, email/3, linkedin_request/5, email/9, linkedin_or_email/13, email/17, linkedin_or_email/21. Every email-capable touch must look like an email: a short first-name greeting on its own line, coherent message, and exact signature on its own line. T1 uses one plain 2-6 word lowercase subject. Later email-capable touches preserve it with one re: prefix. A linkedin_request has no subject, greeting, signature, pitch, meeting ask, or prior-email reference; it must stay under 300 characters.

Purpose and goal are private CRM notes, never substitutes for buyer-facing prose. Before returning, read the whole sequence as the recipient. Remove generic lessons, fragments, surveys, framework language, and repeated retreat lines. In four touches, at most one touch may mainly say Andrew may be wrong, invite a correction/referral, or close; in the legacy seven-touch shape the maximum is three. Rewrite any excess around mechanism, useful contribution, and the hard buyer objection. Never reveal play labels, experiment arms, confidence scores, or internal hypotheses."#,
        n = n,
        planning_contract = planning_contract,
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
        "PRIVATE COPY DECISION (judge the messages against this; never paste these labels):\n- writer's operating moment and decision: {}\n- writer's mechanism to test, never an account fact: {}\n- canonical account mechanism hypothesis, never an account fact: {}\n- concrete system concept only if the premise is confirmed: {}\n- writer's strongest buyer objection: {}\n- canonical hard buyer question: {}\n- role-relevant reason this topic could matter: {}\n- supported seller give-back, empty when none: {}",
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
    let issues = sequence_quality_issues(pb, shared, &sequence, &reviews, n, critique);
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

/// Production QA normally uses at most two model calls per recipient: one
/// repair plus one independent verification, or one verification plus one
/// semantic repair. A third call is reserved for recovery when an editor omits
/// a required rewrite, introduces a deterministic defect, or the independent
/// verifier finds a semantic issue after an initial mechanical repair. Rust
/// reruns its deterministic rules after every edit, so the recovery allowance
/// cannot weaken the sendability envelope.
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
) -> Result<Vec<TouchReview>> {
    scrub_ai_punctuation(sequence);
    enforce_email_signatures(sequence, &pb.signature);
    let mut deterministic =
        sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
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

    const MAX_QA_CALLS: usize = 3;
    let mut qa_calls = 0usize;

    // Mechanical findings are exact and cheap to validate. Most clear in one
    // economical repair. One more economical attempt is allowed only when the
    // first editor violates the repair contract or introduces another exact
    // finding; rejecting the entire recipient for a missing field or a single
    // word-count miss wastes an otherwise valid sequence.
    if !deterministic.is_empty() {
        for round in 0..2 {
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
            deterministic =
                sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
            if let Some(error) = apply_error {
                deterministic.push(error);
            }
            deterministic.sort();
            deterministic.dedup();
            if deterministic.is_empty() {
                break;
            }
            if round == 1 {
                return Err(anyhow!(
                    "copy could not clear mechanical QA after one recovery: {}",
                    deterministic.join("; ")
                ));
            }
        }
    }

    // A single independent recipient-level check is the normal path. It sees
    // the exact wording and cannot edit while judging it.
    report_review_progress(progress, "running independent final verification");
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
        knowledge,
    )
    .await?;
    qa_calls += 1;
    validate_editor_stages(&verification, sequence)?;
    let unresolved = verification_findings(&verification);
    if unresolved.is_empty() {
        report_review_progress(progress, "copy QA passed");
        return Ok(approved_reviews(verification));
    }

    // Preserve an independent verifier. When the normal mechanical repair and
    // verification used two calls, the one recovery call may address its exact
    // semantic findings. If a prior contract failure already consumed that
    // allowance, stop rather than opening an unbounded loop.
    if qa_calls >= MAX_QA_CALLS {
        return Err(anyhow!(
            "copy did not clear independent verification after bounded recovery: {}",
            unresolved.join(" | ")
        ));
    }

    // Mechanically clean copy gets one coherent, targeted repair based on the
    // independent verifier. The repair response grades its final wording; Rust
    // independently reruns every envelope and evidence-safe rule afterward.
    report_review_progress(
        progress,
        "repairing independent-verifier feedback · bounded repair",
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
    let apply_error = apply_targeted_repairs(sequence, &repair, &unresolved, expected_touches)
        .err()
        .map(|error| error.to_string());
    scrub_ai_punctuation(sequence);
    enforce_email_signatures(sequence, &pb.signature);
    let mut after_repair =
        sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
    if let Some(error) = apply_error {
        after_repair.push(error);
    }
    after_repair.sort();
    after_repair.dedup();

    // A semantic edit can accidentally add a fourth question, miss a word
    // band, or omit the requested body. Repair only those exact stages once;
    // this is the third and final QA call on the normal verify→repair path.
    let mut approval_doc = repair;
    if !after_repair.is_empty() {
        if qa_calls >= MAX_QA_CALLS {
            return Err(anyhow!(
                "semantic repair exhausted recovery with mechanical findings: {}",
                after_repair.join("; ")
            ));
        }
        report_review_progress(
            progress,
            "repairing post-review mechanical findings · final recovery",
        );
        let cleanup = request_copy_review(
            client,
            &review_system,
            pb,
            account,
            contact,
            sequence,
            &after_repair,
            expected_touches,
            false,
            knowledge,
        )
        .await?;
        qa_calls += 1;
        validate_editor_stages(&cleanup, sequence)?;
        apply_targeted_repairs(sequence, &cleanup, &after_repair, expected_touches)?;
        scrub_ai_punctuation(sequence);
        enforce_email_signatures(sequence, &pb.signature);
        let after_cleanup =
            sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
        if !after_cleanup.is_empty() {
            return Err(anyhow!(
                "copy could not clear final mechanical recovery: {}",
                after_cleanup.join("; ")
            ));
        }
        approval_doc = cleanup;
    }

    debug_assert!(qa_calls <= MAX_QA_CALLS);
    let repair_findings = review_grade_findings(&approval_doc);
    if !repair_findings.is_empty() {
        return Err(anyhow!(
            "copy did not clear the bounded repair sendability gate: {}",
            repair_findings.join(" | ")
        ));
    }
    report_review_progress(progress, "copy QA passed");
    Ok(approved_reviews(approval_doc))
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

fn recipient_ask_scope(person: &crate::db::Person) -> &'static str {
    ask_scope_for_vantage(&person.vantage)
}

fn ask_scope_for_vantage(vantage: &str) -> &'static str {
    match vantage.to_ascii_lowercase().as_str() {
        "process_owner" | "operator" => {
            "Ask one concrete question about actual work, or ask for a short conversation to compare the precise hypothesis with reality."
        }
        "operational_executive" => {
            "Ask whether this is material across the operation, or who sees it day to day."
        }
        "technical_evaluator" => {
            "Do not ask for a technical evaluation yet; ask who handles the operating decision."
        }
        "economic_buyer" => {
            "Ask whether the issue matters at their level, or who can describe the current process."
        }
        _ => "Ask only who the right person is; do not make them assess the problem or product.",
    }
}

fn copy_contact(person: &crate::db::Person) -> CopyContact {
    CopyContact {
        name: person.name.clone(),
        title: person.title.clone(),
        vantage: person.vantage.clone(),
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
        "You moderate a pre-send sales council. Apply every configured analytical lens independently; do not average them into one generic opinion and do not imitate or speak as the named real people. Each critic grades the CURRENT wording of every email. Correct, grammatical, and non-offensive is not enough: the first touch needs a plausible self-interested reason to stop and answer a stranger. Calibrate later touches to their job: a reply-thread follow-up may contribute a concrete useful contrast without another CTA; a routing or close touch may be brief. Do not demand a separate offer in every touch. Default to rejection when the sequence feels like automated account research, a multi-part interview, generic theory, or seller curiosity disguised as relevance. Passing means score >= 85, passes=true, and no unresolved issues. Unanimous approval is intentionally difficult. A critic may disagree with another. Be demanding but evidence-bound. Return only the requested structured data.\n\n{critic_prompts}"
    );
    let emails = sequence
        .touches
        .iter()
        .filter(|touch| is_email_capable_channel(&touch.channel))
        .collect::<Vec<_>>();
    let user = format!(
        "Review every current email under every critic lens. This is a vote, not an editing task: recommendations diagnose the smallest needed change but never provide canned replacement copy.\n\nREQUIRED SIGNATURE: {signature}\nVERIFIED ACCOUNT FACTS: {facts}\nQUESTION TO TEST (NOT A FACT): {hypothesis}\nRECIPIENT: {name} ({title}, {vantage})\nASK SCOPE: {ask_scope}\n\nCURRENT EMAILS:\n{emails}\n\nRETRIEVED BOOK AND SKILL KNOWLEDGE:\n{knowledge}",
        signature = pb.signature,
        facts = account.observed_facts.join(" | "),
        hypothesis = account.hypothesis,
        name = contact.name,
        title = contact.title,
        vantage = contact.vantage,
        ask_scope = ask_scope_for_vantage(&contact.vantage),
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
        "Repair the named findings as hard constraints. Any feedback string containing 'stage N' names stage N. Change only named stages unless a finding applies to the whole sequence. For EVERY named stage you MUST return a nonempty complete corrected body, even if you disagree with the feedback; never mark a named stage passed with an empty revised body. Return a complete corrected subject only when the feedback concerns the subject; an empty revised_subject preserves an already-good subject. Count the corrected stage's words and question marks before returning: it must fall inside every stated range. Stage 1 may contain one operating question plus one short CTA question; stages 2 through 6 may contain at most one question mark, and stage 7 must contain none. Preserve verified facts, natural phrasing, and unnamed stages. The passes flag and score must grade the FINAL corrected wording you return. List only issues still present after your correction."
    } else {
        "Review and repair the copy. For every touch that is not ready to send, return a complete corrected body and, for email, a complete corrected subject. Stage 1 may contain one operating question plus one short CTA question; stages 2 through 6 may contain at most one question mark, and the final close contains none. The passes flag and score must grade the FINAL corrected wording you return, not the original. List only issues that remain unresolved after your correction. If it cannot be fixed without inventing facts, mark it failed."
    };
    let stage_contract = format!(
        "SCHEMA CONTRACT: first grade the entire sequence for coherence, relevance, repetition, and whether a sensible recipient has a reason to answer. Then return exactly one review object for every stage 1 through {expected_touches}, even when only a subset needs repair. A sequence passes only at 85+ with no unresolved sequence issues. For an unnamed stage that does not need editing, preserve it with empty revised fields."
    );
    let user = format!(
        "{task}\n\n{stage_contract}\nCHANNEL: linkedin_request has no subject; linkedin_or_email must work as either a DM or a complete email fallback.\nSENDABILITY: reject T1 if it is merely a diagnostic question plus a vague capability sentence. Require one verified trigger, one operating decision, uncertainty about the mechanism, and a role-relevant reason to answer. Curiosity is not recipient value. Never require or invent collateral. T2 must advance the mechanism. Any later touch must add a sourced fact, useful distinction, honest objection answer, route, or close rather than paraphrase.\nEVIDENCE: the verified facts below are exhaustive. The hypothesis is not fact. Never invent an internal event, system, practice, consequence, or ownership claim.\n\nSIGNATURE: {signature}\nVERIFIED FACTS: {facts}\nHYPOTHESIS, NOT FACT: {hypothesis}\nRECIPIENT: {name} ({title}, {vantage})\nLIKELY ACCESS, INTERNAL ONLY: {can_observe}\nASK SCOPE: {ask_scope}\nDETERMINISTIC FINDINGS: {deterministic}\n\nCURRENT SEQUENCE:\n{sequence}\n\nRELEVANT REVIEW KNOWLEDGE:\n{knowledge}",
        task = task,
        signature = pb.signature,
        facts = verified_facts,
        hypothesis = account.hypothesis,
        name = contact.name,
        title = contact.title,
        vantage = contact.vantage,
        can_observe = contact.can_observe,
        ask_scope = ask_scope_for_vantage(&contact.vantage),
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
        "proven_seller_facts": business.known_facts.iter().take(5).collect::<Vec<_>>(),
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

fn person_matches(person: &crate::db::Person, filter: Option<&str>) -> bool {
    let Some(filter) = filter.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    person.id.eq_ignore_ascii_case(filter)
        || person.email.eq_ignore_ascii_case(filter)
        || person.name.eq_ignore_ascii_case(filter)
}

fn select_people_for_planning(
    people: Vec<crate::db::Person>,
    _per_account: usize,
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
            planning_priority(right)
                .cmp(&planning_priority(left))
                .then_with(|| left.name.cmp(&right.name))
        });
        selected.extend(candidates.into_iter().take(1));
    }
    selected
}

fn planning_priority(person: &crate::db::Person) -> i32 {
    let vantage = person.vantage.to_ascii_lowercase();
    let mut score = if person.primary { 100 } else { 0 };
    score += match vantage.as_str() {
        "process_owner" => 70,
        "operator" => 65,
        "operational_executive" => 55,
        "economic_buyer" => 40,
        "technical_evaluator" => 25,
        "router" => 10,
        _ => 0,
    };
    let title = person.title.to_ascii_lowercase();
    if [
        "recruit",
        "talent",
        "human resources",
        "business development",
        "sales",
    ]
    .iter()
    .any(|term| title.contains(term))
    {
        score -= 60;
    }
    score
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
            (8, 50)
        } else {
            // Dual-use follow-ups still have to be substantive correspondence
            // when LinkedIn is unavailable and they fall back to email.
            (25, 70)
        }
    } else if touch.channel.eq_ignore_ascii_case("email") {
        match touch.stage {
            // The prompt targets the brand band exactly. QA keeps a small
            // tolerance so a natural 69-word GnK note is not sent through two
            // model repairs merely to add a filler word to a 70-word target.
            1 => (
                pb.min_words.saturating_sub(10),
                pb.max_words.saturating_add(10),
            ),
            // These are still emails, not caption-sized blurbs. T2 sharpens
            // the diagnostic, T4 contributes value, and T6 routes; each stays
            // shorter than T1 while retaining enough room to do its actual job.
            2 => (35, 75),
            4 => (40, 85),
            6 => (18, 60),
            7 => (8, 50),
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
/// Counting punctuation cannot distinguish those jobs, so reserve two marks for
/// T1 and keep every follow-up to one. Semantic review still rejects a survey or
/// several operating questions disguised inside the first mark.
fn touch_question_limit(stage: u32) -> usize {
    if stage == 1 {
        2
    } else {
        1
    }
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
                if !(2..=6).contains(&subject_words) {
                    issues.push(format!(
                        "stage 1 subject has {subject_words} words (needs 2–6)"
                    ));
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
            if touch.subject.chars().any(char::is_uppercase) {
                issues.push(format!("stage {} subject must be lowercase", touch.stage));
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
                let greeting_words = greeting.split_whitespace().count();
                if !greeting.ends_with(',')
                    || greeting_words == 0
                    || greeting_words > 3
                    || greeting.contains('?')
                {
                    issues.push(format!(
                        "stage {} must start with a short greeting on its own line",
                        touch.stage
                    ));
                }
            }
            let sentences = copy_sentence_count(&touch.body, &pb.signature);
            let sentence_limit_ok = match touch.stage {
                1 => (3..=7).contains(&sentences),
                7 => (1..=3).contains(&sentences),
                2 | 4 | 5 => (1..=4).contains(&sentences),
                _ => (1..=3).contains(&sentences),
            };
            if !sentence_limit_ok {
                let expected = match touch.stage {
                    1 => "3–7",
                    7 => "1–3",
                    2 | 4 | 5 => "1–4",
                    _ => "1–3",
                };
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
            // A well-formed email is greeting + up to three body paragraphs + a
            // sign-off line, each separated by a blank line — so the greeting AND
            // the sign-off both count here on top of the body. The old ceiling of 4
            // silently rejected any email-1 with three body paragraphs (the longest
            // touch), a normal sendable shape; six blocks is where it truly runs on.
            if paragraphs > 5 {
                issues.push(format!("stage {} has {paragraphs} paragraphs", touch.stage));
            }
        }
        let question_limit = touch_question_limit(touch.stage);
        if touch.body.matches('?').count() > question_limit {
            issues.push(if touch.stage == 1 {
                "stage 1 asks more than one operating question plus one CTA".to_string()
            } else {
                format!("stage {} asks more than one question", touch.stage)
            });
        }
        if channel == "linkedin_request" && touch.body.chars().count() > 300 {
            issues.push(format!(
                "stage {} LinkedIn connection request is {} characters (maximum 300)",
                touch.stage,
                touch.body.chars().count()
            ));
        }
        if touch.stage == 7 && touch.body.contains('?') {
            issues.push("stage 7 must close without a question".to_string());
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
    let question_touch_limit = if expected_touches == 4 { 3 } else { 4 };
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
    let retreat_touch_limit = if expected_touches == 4 { 1 } else { 3 };
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
    // the whole sequence. Naming collateral in three or more messages is the
    // generic lead-magnet loop that made otherwise tailored sequences robotic.
    for stage in asset_stages.iter().skip(2) {
        issues.push(format!(
            "stage {stage} repeats collateral across {} touches (maximum 2; continue the human conversation instead)",
            asset_stages.len()
        ));
    }

    let brand_name = pb.name.to_ascii_lowercase();
    let brand_mentions = sequence
        .touches
        .iter()
        .map(|touch| touch.body.to_ascii_lowercase().matches(&brand_name).count())
        .sum::<usize>();
    if brand_mentions > 1 {
        issues.push(format!(
            "{} appears {brand_mentions} times (maximum 1 across the sequence)",
            pb.name
        ));
    }

    if pb.key == "wapahki"
        && sequence
            .touches
            .iter()
            .find(|touch| touch.stage == 1)
            .is_some_and(|touch| touch.body.to_ascii_lowercase().contains("wapahki"))
    {
        issues.push("stage 1 must not introduce Wapahki before relevance is established".into());
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
        "OpenAI request path remained unavailable after bounded retries; drafting stopped safely and can be retried without treating the copy as rejected".into()
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
        "required": ["overall_strategy", "touches", "applied_principles"],
        "properties": {
            "overall_strategy": {
                "type": "string",
                "description": format!("One or two sentences on the arc across all {n} touches.")
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
        affected_stages, apply_targeted_repairs, business_copy_context, copy_sentence_count,
        format_progress_status, is_email_capable_channel, is_retreat_or_route_touch,
        mentions_outreach_asset, normalize_dashes, normalize_principle_ids,
        normalize_thread_subjects, provisional_channel, provisional_day_offset,
        select_people_for_planning, sequence_quality_issues, touch_question_limit, touch_word_band,
        word_set_similarity, CopySequence, CopyTouch, EditDoc, EditReview, PlanProgressRecipient,
        PlanProgressUpdate, TouchReview,
    };
    use crate::business::Businesses;
    use crate::db::Person;
    use crate::playbook::Playbooks;

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
    fn sentence_count_ignores_greeting_and_signature() {
        let body = "Maya,\n\nCases change shape between runs. Does that still keep the handoff manual?\n\nAndrew";
        assert_eq!(copy_sentence_count(body, "Andrew"), 2);
    }

    #[test]
    fn first_email_reserves_a_second_question_for_the_cta_only() {
        assert_eq!(touch_question_limit(1), 2);
        for stage in 2..=7 {
            assert_eq!(touch_question_limit(stage), 1);
        }
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
        for (stage, expected) in [(1, (60, 140)), (2, (35, 75)), (4, (40, 85)), (6, (18, 60))] {
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
        assert_eq!(touch_word_band(pb, &objection_touch), (25, 70));
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
    fn bulk_planning_selects_one_primary_workflow_contact_per_account() {
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
                    "b",
                    "b",
                    "Other Owner",
                    "Operations Manager",
                    "process_owner",
                    true,
                ),
            ],
            2,
        );
        let account_a = selected
            .iter()
            .filter(|person| person.lead_id == "a")
            .map(|person| person.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(account_a, vec!["o"]);
        assert_eq!(
            selected
                .iter()
                .filter(|person| person.lead_id == "b")
                .count(),
            1
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
                    "case handling".into()
                } else {
                    "re: case handling".into()
                },
                body: format!("Maya,\n\n{middle}\n\n{signature}"),
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
                        subject: "re: case handling".into(),
                        body: format!("Maya,\n\nThe screen is practical and takes a minute to scan. Happy to send it without arranging a call.\n\n{signature}"),
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
                        subject: "re: case handling".into(),
                        body: format!("Maya,\n\nI will close the thread here. Thanks for considering it.\n\n{signature}"),
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
                    issue.contains("must start with a short greeting")
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
    fn wapahki_gate_limits_questions_and_defers_the_brand_name() {
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
                .any(|issue| issue.contains("stage 1 must not introduce Wapahki")),
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
}
