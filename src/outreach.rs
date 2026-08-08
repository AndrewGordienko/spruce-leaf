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
use crate::db::{Sequence, SharedDb, Touch};
use crate::domain::{
    Account as CopyAccount, Contact as CopyContact, Sequence as CopySequence, Touch as CopyTouch,
    TouchReview,
};
use crate::engine::Engine;
use crate::gtm::GtmActionContext;
use crate::knowledge::{core_principle_ids, core_strategy_block, Library};
use crate::playbook::{self, Playbook, SalesCriticPersona, Shared};

#[derive(Debug, Default)]
pub struct PlanSummary {
    pub people_planned: usize,
    pub touches_scheduled: usize,
    pub touches_drafted: usize,
    pub sequences_replaced: usize,
    pub people_rejected: usize,
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
            _ => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
            }
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

struct CopyFailure {
    reason: String,
    provider_stopped: bool,
}

struct AccountCopyResult {
    copies: HashMap<String, ReviewedCopy>,
    failures: HashMap<String, CopyFailure>,
    stopped_reason: Option<String>,
}

struct RoleKnowledge {
    block: String,
    /// IDs from persisted books/skills, excluding the always-on core cards.
    retrieved_ids: Vec<String>,
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
                "{} {} {} {}",
                lead.industry, lead.hypothesis, lead.mechanism, lead.hard_buyer_question
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
        planner: retrieve_role_knowledge(library, &shared.personas.planner, &account),
        writer: retrieve_role_knowledge(library, &shared.personas.writer, &account),
        reviewer: retrieve_role_knowledge(library, &shared.personas.reviewer, &account),
        council: retrieve_role_knowledge_with_limits(library, &council_personas, &account, 14, 10),
    }
}

fn retrieve_role_knowledge(library: &Library, persona: &str, account: &str) -> RoleKnowledge {
    retrieve_role_knowledge_with_limits(library, persona, account, 6, 4)
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
    let retrieved_ids = retrieved
        .principles
        .iter()
        .map(|principle| principle.id.clone())
        .collect::<Vec<_>>();
    let mut allowed_ids = retrieved_ids.clone();
    allowed_ids.extend(core_principle_ids().iter().map(|id| (*id).to_string()));
    RoleKnowledge {
        block: format!(
            "{}\n\n{}",
            core_strategy_block("sequence"),
            retrieved.playbook_block()
        ),
        retrieved_ids,
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
    #[serde(default)]
    touches: Vec<CopyTouch>,
    #[serde(default)]
    applied_principles: Vec<String>,
}

/// The strategy the agent reasons out for one recipient BEFORE any copy is
/// written — what each touch should achieve and how, so the writer executes a
/// deliberate plan rather than improvising seven messages in one shot.
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
    /// The one new angle it introduces (never a repeat of an earlier touch).
    #[serde(default)]
    angle: String,
    /// The single clear ask; empty for the final close.
    #[serde(default)]
    ask: String,
}

#[derive(Debug, Deserialize)]
struct EditDoc {
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
    if total == 7 {
        SEVEN_DAYS
            .get(stage.saturating_sub(1))
            .copied()
            .unwrap_or(21)
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
    let system = pb.copy_system_prompt(shared);

    // Verified people to sequence. An explicit --person request targets that exact
    // row. `per_account_cap` = Some(n) fills each company up to its n strongest
    // verified contacts (the full motion's target); None sequences every verified
    // person found (the explicit "draft everyone" sweep). Drafting, not sending —
    // send-time account limits still bound real outbound volume.
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
    } else if let Some(cap) = per_account_cap {
        select_people_for_planning(verified, cap.max(1))
    } else {
        verified
    };
    let mut todo = Vec::new();
    let mut matched_people = 0;
    for p in selected {
        matched_people += 1;
        if let Some(sequence_id) = db.active_sequence_for_person(&p.id)? {
            if !replace_drafts {
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
            ..Default::default()
        });
    }

    // Group by account so its evidence, business context, and knowledge are sent
    // once, then split each account into small chunks: one writer call produces
    // every recipient's full sequence, so 5 recipients × 7 touches in a single
    // call is ~35 messages of copy — enough to blow the model's per-call timeout
    // and get the whole account rejected. Capping recipients per call keeps each
    // call bounded and lets the account's other recipients still succeed.
    let max_recipients_per_call = if client.prefers_lean_outreach() { 3 } else { 2 };
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
    let stopped_reason = Arc::new(Mutex::new(None::<String>));
    let drafts = stream::iter(units.into_iter().map(|(lead_id, people)| {
        let db = db.clone();
        let system = system.clone();
        let business_context = business_context.clone();
        let lead = leads.iter().find(|lead| lead.id == lead_id).cloned();
        let knowledge = retrieve_outreach_knowledge(library, shared, lead.as_ref());
        let progress = progress.clone();
        let stopped_reason = Arc::clone(&stopped_reason);
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
            if stopped_reason
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .is_some()
            {
                for person in &recipients {
                    progress.stop_person(
                        &lead.name,
                        person,
                        "not attempted; model usage limit reached",
                    );
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
                                if failure.is_none() {
                                    if let Some(sequence_id) = checkpoint.as_deref() {
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
                                        "stopped; model usage limit reached",
                                    );
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
                            log_outreach(format!(
                                "✓ drafted and reviewed {}-touch sequence for {}",
                                copy.sequence.touches.len(),
                                person.name
                            ));
                            Some((
                                person,
                                lead.clone(),
                                copy,
                                replaced_sequence,
                                gtm_context,
                                checkpoint,
                            ))
                        })
                        .collect::<Vec<_>>()
                }
                Err(e) => {
                    let provider_stopped = crate::engine::is_usage_exhausted(&e);
                    if provider_stopped {
                        let mut shared = stopped_reason
                            .lock()
                            .unwrap_or_else(|lock| lock.into_inner());
                        if shared.is_none() {
                            *shared = Some(usage_stop_reason(&e));
                        }
                    }
                    for (person, _) in &people {
                        if let Some(sequence_id) = checkpoints.get(&person.id) {
                            let _ = db.reject_building_sequence(sequence_id, &e.to_string());
                        }
                        if provider_stopped {
                            progress.stop_person(
                                &lead.name,
                                person,
                                "stopped; model usage limit reached",
                            );
                        } else {
                            progress.finish_person_as(
                                &lead.name,
                                person,
                                &format!("rejected: {}", first_line(&e.to_string())),
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
                            first_line(&e.to_string())
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

    let drafts = drafts.into_iter().flatten().collect::<Vec<_>>();
    let drafts = drafts.into_iter().flatten().collect::<Vec<_>>();
    let people_rejected = progress.rejected.load(Ordering::Relaxed);
    let people_stopped = progress.stopped.load(Ordering::Relaxed);
    let stopped_reason = stopped_reason
        .lock()
        .unwrap_or_else(|lock| lock.into_inner())
        .clone();
    let mut summary = PlanSummary {
        people_rejected,
        people_stopped,
        stopped_reason,
        ..Default::default()
    };
    progress.overall(&format!(
        "saving {} accepted sequences to CRM",
        drafts.len()
    ));
    let now = Utc::now();
    let mut planned_by_lead: HashMap<String, HashSet<String>> = HashMap::new();

    for (person, lead, copy, replaced_sequence, gtm_context, seq_id) in drafts {
        let seq = &copy.sequence;
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
                summary.touches_scheduled += 1;
            } else {
                summary.touches_drafted += 1;
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
            let slot =
                calendar::schedule_with_capacity(business, &timing, desired, |start, end| {
                    db.planned_touch_count_between(&pb.key, start, end)
                })?;
            let updated = db.update_touch_checkpoint(&Touch {
                sequence_id: seq_id.clone(),
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
        db.promote_building_sequence(
            &seq_id,
            replaced_sequence.as_deref(),
            &seq.applied_principles,
        )?;
        if replaced_sequence.is_some() {
            summary.sequences_replaced += 1;
        }
        db.log_event(
            &pb.key,
            &person.id,
            "",
            "scheduled",
            &format!("{}-touch sequence", seq.touches.len()),
        )?;
        planned_by_lead
            .entry(lead.id.clone())
            .or_default()
            .insert(person.id.clone());
        summary.people_planned += 1;
        progress.finish_person(&lead.name, &person, true);
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
    let recipient = json!({
        "name": person.name,
        "first_name": person.first_name,
        "title": person.title,
        "vantage": person.vantage,
        "likely_access_internal_only": person.can_observe,
        "ask_scope": recipient_ask_scope(person),
        "route_to": person.route_to,
    });
    let user = format!(
        "Plan a {n}-touch no-reply sequence for this recipient. The INTERNAL HYPOTHESES help you choose questions; they are not facts and must not become declarative copy. The GTM play governs the commercial action but is not a copy template.\n\nACCOUNT BRIEF:\n{account}\n\nRECIPIENT:\n{recipient}\n\nPRIVATE GTM ACTION CONTEXT:\n{gtm_context}\n\nRETRIEVED KNOWLEDGE FOR THIS PLANNER:\n{knowledge}\n\nFor exactly seven touches use this evidence-informed 21-day order and no calls: T1 day 0 email (grounded diagnostic), T2 day 3 email reply-thread follow-up (short, one new reason to answer), T3 day 5 linkedin_request (personalized connection note, no pitch or meeting ask), T4 day 9 email (new operational angle), T5 day 13 linkedin_or_email (DM if the CRM says connected; otherwise a short email fallback), T6 day 17 email (routing or direct falsification question), T7 day 21 linkedin_or_email (soft close if connected; otherwise a short email close). Scale sensibly if the count differs. For each touch give: stage, channel, objective, one genuinely new angle, and one short role-appropriate ask. The last ask is empty. Never make a LinkedIn touch say merely that an email was sent. overall_strategy should describe how the sequence earns a correction or referral without escalating pressure. If the action state is research_required, stay diagnostic and do not propose the proof. Cite only principle IDs you actually used.",
        account = serde_json::to_string_pretty(&account).unwrap_or_default(),
        recipient = serde_json::to_string_pretty(&recipient).unwrap_or_default(),
        knowledge = knowledge.block,
    );
    let mut plan = client
        .structured_bulk::<SequencePlan>("outreach.plan", plan_system, &user, plan_schema(n))
        .await?;
    plan.applied_principles =
        normalize_principle_ids(&plan.applied_principles, &knowledge.allowed_ids);
    if !knowledge.retrieved_ids.is_empty()
        && !plan
            .applied_principles
            .iter()
            .any(|id| knowledge.retrieved_ids.contains(id))
    {
        return Err(anyhow!(
            "planner did not apply any retrieved book or skill principle"
        ));
    }
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
        // CLI backends retain the explicit planner role. The API path folds the
        // same planning contract into the account writer, avoiding one full
        // model call per recipient.
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
    let recipients = people
        .iter()
        .map(|person| {
            json!({
                "person_key": person.id,
                "name": person.name,
                "first_name": person.first_name,
                "title": person.title,
                "vantage": person.vantage,
                "likely_access_internal_only": person.can_observe,
                "why_this_person_internal_only": person.why_them,
                "ask_scope": recipient_ask_scope(person),
                "route_to": person.route_to,
                "sequence_plan": plans.get(&person.id),
                "private_gtm_action_context": gtm_contexts
                    .get(&person.id)
                    .map(GtmActionContext::prompt_block)
                    .unwrap_or_else(|| "GTM ACTION STATE: unavailable".into()),
            })
        })
        .collect::<Vec<_>>();
    let writer_account = if lean {
        planner_account_brief(&account)
    } else {
        writer_account_brief(&account)
    };
    let planning_contract = if lean {
        "For each recipient, first decide the full sequence arc internally: every touch needs one objective and one genuinely new angle, but an ask appears only when it earns its place and the final ask is empty. A no-reply sequence must not become seven interview questions. Then write the finished copy in the same response. Do not expose the private plan."
    } else {
        "Follow each recipient's supplied private sequence_plan."
    };
    let writer_knowledge = if lean {
        format!(
            "PLANNING KNOWLEDGE:\n{}\n\nWRITING KNOWLEDGE:\n{}",
            knowledge.planner.block, knowledge.writer.block
        )
    } else {
        knowledge.writer.block.clone()
    };
    let user = format!(
        "Write one {n}-touch no-reply sequence for each recipient. {planning_contract} Think through the buyer-safe brief and private GTM action context, then write the messages in Andrew's voice. The play is a policy and hypothesis, never a fixed email template. Return exactly one sequence for every person_key and copy person_key exactly. For seven touches use these exact channels and day offsets: email/0, email/3, linkedin_request/5, email/9, linkedin_or_email/13, email/17, linkedin_or_email/21. A linkedin_request is a short personalized connection note with no pitch, meeting ask, greeting, or signature. A linkedin_or_email touch must work as either a natural LinkedIn DM or a very short email: include a 2-8 word fallback email subject and Andrew's exact email signature, but keep the body concise enough for LinkedIn. Never reveal play labels, experiment arms, confidence scores, internal hypotheses, or strategy language.\n\nBUYER-SAFE ACCOUNT BRIEF:\n{account}\n\nRECIPIENTS (private context; never quote its labels):\n{recipients}\n\nVERIFIED SELLER CONTEXT:\n{business_context}\n\nRETRIEVED KNOWLEDGE:\n{knowledge}",
        account = serde_json::to_string_pretty(&writer_account).unwrap_or_default(),
        recipients = serde_json::to_string_pretty(&recipients).unwrap_or_default(),
        knowledge = writer_knowledge,
    );
    let batch = client
        .structured_bulk::<BatchCopy>(
            "outreach.write_account",
            system,
            &user,
            batch_sequence_schema(n, people.len()),
        )
        .await?;
    let expected = people
        .iter()
        .map(|person| person.id.as_str())
        .collect::<HashSet<_>>();
    let mut raw_by_person = HashMap::new();
    for raw in batch.sequences {
        if expected.contains(raw.person_key.as_str()) {
            raw_by_person.entry(raw.person_key.clone()).or_insert(raw);
        }
    }
    if raw_by_person.len() != people.len() {
        return Err(anyhow!(
            "writer returned {} of {} requested recipient sequences",
            raw_by_person.len(),
            people.len()
        ));
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
                )
            })
        })
        .collect::<Vec<_>>();
    let reviewed = stream::iter(jobs.into_iter().map(|(person, raw, plan, checkpoint)| {
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
                checkpoint.as_deref(),
                n,
                knowledge,
                critique,
                &progress,
            )
            .await;
            (person, checkpoint, result)
        }
    }))
    .buffer_unordered(people.len().max(1))
    .collect::<Vec<_>>()
    .await;
    let mut output = HashMap::new();
    let mut failures = HashMap::new();
    let mut stopped_reason = None;
    for (person, checkpoint, result) in reviewed {
        match result {
            Ok(copy) => {
                output.insert(person.id.clone(), copy);
            }
            Err(error) => {
                let reason = error.to_string();
                let provider_stopped = crate::engine::is_usage_exhausted(&error);
                if provider_stopped && stopped_reason.is_none() {
                    stopped_reason = Some(usage_stop_reason(&error));
                }
                if let Some(sequence_id) = checkpoint.as_deref() {
                    let _ = db.reject_building_sequence(sequence_id, &reason);
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
    checkpoint: Option<&str>,
    n: usize,
    knowledge: &OutreachKnowledge,
    critique: bool,
    progress: &PlanProgress,
) -> Result<ReviewedCopy> {
    let person_progress = |phase: &str| progress.person(phase, &lead.name, person);
    person_progress("checkpointing written copy in CRM");
    let sequence_id = checkpoint.ok_or_else(|| anyhow!("CRM checkpoint was not created"))?;
    let lean = client.prefers_lean_outreach();
    let mut allowed_principles = knowledge.writer.allowed_ids.clone();
    let mut retrieved_principles = knowledge.writer.retrieved_ids.clone();
    if lean {
        allowed_principles.extend(knowledge.planner.allowed_ids.iter().cloned());
        retrieved_principles.extend(knowledge.planner.retrieved_ids.iter().cloned());
        allowed_principles.sort();
        allowed_principles.dedup();
        retrieved_principles.sort();
        retrieved_principles.dedup();
    }
    let mut sequence = CopySequence {
        touches: raw.touches,
        applied_principles: normalize_principle_ids(&raw.applied_principles, &allowed_principles),
    };
    if !retrieved_principles.is_empty()
        && !sequence
            .applied_principles
            .iter()
            .any(|id| retrieved_principles.contains(id))
    {
        return Err(anyhow!(
            "writer applied no retrieved book or skill principle"
        ));
    }
    if let Some(plan) = plan {
        sequence
            .applied_principles
            .extend(plan.applied_principles.iter().cloned());
        sequence.applied_principles.sort();
        sequence.applied_principles.dedup();
    }
    enforce_email_signatures(&mut sequence, &pb.signature);
    checkpoint_sequence_copy(db, sequence_id, pb, lead, person, &sequence, &[])?;

    person_progress("checking deterministic copy rules");
    let mut reviews = if lean {
        review_and_edit_sequence_lean(
            client,
            pb,
            shared,
            account,
            &copy_contact(person),
            &mut sequence,
            n,
            critique,
            &knowledge.reviewer.block,
            Some(&person_progress),
        )
        .await?
    } else {
        review_and_edit_sequence(
            client,
            pb,
            shared,
            account,
            &copy_contact(person),
            &mut sequence,
            n,
            critique,
            &knowledge.reviewer.block,
            Some(&person_progress),
        )
        .await?
    };
    if critique {
        reviews = satisfy_sales_council(
            client,
            pb,
            shared,
            account,
            &copy_contact(person),
            &mut sequence,
            reviews,
            n,
            &knowledge.council.block,
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

/// API outreach gets one editor pass plus at most two targeted repairs per
/// recipient. Later passes can only change stages named by the remaining
/// findings, which prevents a mechanical fix from regressing approved copy.
/// Rust reruns every deterministic rule after each pass.
#[allow(clippy::too_many_arguments)]
async fn review_and_edit_sequence_lean(
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
    let deterministic = sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
    if !critique {
        if deterministic.is_empty() {
            return Ok(Vec::new());
        }
        return Err(anyhow!(
            "deterministic QA failed: {}",
            deterministic.join("; ")
        ));
    }

    let mut findings = deterministic;
    let mut final_review = None;
    for pass in 1..=3 {
        let affected = findings
            .iter()
            .flat_map(|finding| affected_stages(finding, expected_touches))
            .collect::<HashSet<_>>();
        let repair_all = pass == 1 || affected.is_empty();
        report_review_progress(
            progress,
            match pass {
                1 => format!("reviewing and repairing all {expected_touches} touches"),
                2 => "repairing only the remaining copy findings · round 1/2".to_string(),
                _ => "repairing only the remaining copy findings · final round".to_string(),
            },
        );
        let review = request_copy_review(
            client,
            &pb.review_system_prompt(shared),
            pb,
            account,
            contact,
            sequence,
            &findings,
            expected_touches,
            false,
            knowledge,
        )
        .await?;
        validate_editor_stages(&review, sequence)?;

        for touch in &mut sequence.touches {
            if !repair_all && !affected.contains(&touch.stage) {
                continue;
            }
            let edit = review
                .reviews
                .iter()
                .find(|review| review.stage == touch.stage)
                .expect("validated editor stages");
            if !edit.revised_body.trim().is_empty() {
                touch.body = edit.revised_body.clone();
            }
            if is_email_capable_channel(&touch.channel) && !edit.revised_subject.trim().is_empty() {
                touch.subject = edit.revised_subject.clone();
            }
        }
        scrub_ai_punctuation(sequence);
        enforce_email_signatures(sequence, &pb.signature);
        let deterministic_after =
            sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
        let semantic_after = review
            .reviews
            .iter()
            .filter(|review| repair_all || affected.contains(&review.stage))
            .filter(|review| !review.passes || review.score < 85 || !review.issues.is_empty())
            .map(|review| {
                format!(
                    "stage {} scored {}: {}",
                    review.stage,
                    review.score,
                    if review.issues.is_empty() {
                        "not ready to send".to_string()
                    } else {
                        review.issues.join("; ")
                    }
                )
            })
            .collect::<Vec<_>>();
        if deterministic_after.is_empty() && semantic_after.is_empty() {
            final_review = Some(review);
            break;
        }
        if pass == 3 {
            let mut unresolved = deterministic_after;
            unresolved.extend(semantic_after);
            return Err(anyhow!(
                "copy still failed after two targeted repair rounds: {}",
                unresolved.join(" | ")
            ));
        }
        findings = deterministic_after;
        findings.extend(semantic_after);
    }

    let reviews = final_review
        .expect("three-pass editor exits only after accepting or returning an error")
        .reviews
        .into_iter()
        .map(|edit| TouchReview {
            stage: edit.stage,
            passes: edit.passes,
            score: edit.score,
            issues: edit.issues,
        })
        .collect::<Vec<_>>();
    report_review_progress(progress, "copy QA passed");
    Ok(reviews)
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

/// The writer gets an intentionally smaller brief than the planner. In
/// particular it never sees magnitude notes, inferred systems, a proposed
/// integration, or source signals that tempt it to turn research into a pitch.
fn writer_account_brief(account: &CopyAccount) -> Value {
    json!({
        "company": account.name,
        "industry": account.industry,
        "location": account.hq,
        "verified_facts": account.observed_facts.iter().take(3).collect::<Vec<_>>(),
        "question_to_test_not_a_fact": account.hypothesis,
        "plain_reason_it_might_matter_not_a_fact": account.consequence_metric,
    })
}

fn recipient_ask_scope(person: &crate::db::Person) -> &'static str {
    ask_scope_for_vantage(&person.vantage)
}

fn ask_scope_for_vantage(vantage: &str) -> &'static str {
    match vantage.to_ascii_lowercase().as_str() {
        "process_owner" | "operator" => {
            "Ask how the current situation is handled, or invite a correction."
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
        route_to: person.route_to.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn review_and_edit_sequence(
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
            if is_email_capable_channel(&touch.channel) {
                if edit.revised_subject.trim().is_empty() {
                    return Err(anyhow!(
                        "copy editor failed email stage {} without a corrected subject",
                        touch.stage
                    ));
                }
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
            if is_email_capable_channel(&touch.channel) {
                if edit.revised_subject.trim().is_empty() {
                    return Err(anyhow!(
                        "copy editor omitted the corrected subject for stage {}",
                        touch.stage
                    ));
                }
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
                if is_email_capable_channel(&touch.channel) {
                    if edit.revised_subject.trim().is_empty() {
                        return Err(anyhow!(
                            "copy editor omitted the corrected subject for stage {}",
                            touch.stage
                        ));
                    }
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
            if edit.revised_subject.trim().is_empty() {
                return Err(anyhow!(
                    "editor returned no council-driven subject for stage {}",
                    touch.stage
                ));
            }
            touch.subject = edit.revised_subject.clone();
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
            let cleanup = request_copy_review(
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
                if is_email_capable_channel(&touch.channel) {
                    if edit.revised_subject.trim().is_empty() {
                        return Err(anyhow!(
                            "copy editor omitted council cleanup subject for stage {}",
                            touch.stage
                        ));
                    }
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
        "You moderate a pre-send sales council. Apply every configured analytical lens independently; do not average them into one generic opinion and do not imitate or speak as the named real people. Each critic grades the CURRENT wording of every email. Correct, grammatical, and non-offensive is not enough: the recipient needs a plausible self-interested reason to stop and answer a stranger. Default to rejection when the sequence feels like automated account research, a seven-part interview, or seller curiosity disguised as relevance. Passing means score >= 85, passes=true, and no unresolved issues. Unanimous approval is intentionally difficult. A critic may disagree with another. Be demanding but evidence-bound. Return only the requested structured data.\n\n{critic_prompts}"
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
    let task = if verify_only {
        "This is a final gate over already-repaired copy. Do not edit it. Return empty revised fields. Mark passes=true only when the CURRENT touch is natural, accurate, easy to answer, and ready for Andrew to send unchanged. List only unresolved issues."
    } else if !deterministic.is_empty() {
        "Repair the named findings as hard constraints. Change only stages named by those findings unless a finding applies to the whole sequence. For every named stage, return its complete corrected body and, for email, its complete corrected subject. Count the corrected stage's words and question marks before returning: it must fall inside every stated range, contain at most one question mark, and stage 7 must contain none. Preserve verified facts, natural phrasing, and already-good stages. The passes flag and score must grade the FINAL corrected wording you return. List only issues still present after your correction."
    } else {
        "Review and repair the copy. For every touch that is not ready to send, return a complete corrected body and, for email, a complete corrected subject. Every corrected touch may contain at most one question mark; the final close contains none. The passes flag and score must grade the FINAL corrected wording you return, not the original. List only issues that remain unresolved after your correction. If it cannot be fixed without inventing facts, mark it failed."
    };
    let stage_contract = format!(
        "SCHEMA CONTRACT: return exactly one review object for every stage 1 through {expected_touches}, even when only a subset needs repair. For an unnamed stage that does not need editing, preserve it by returning empty revised fields; do not omit its review object."
    );
    let user = format!(
        "{task}\n\n{stage_contract}\n\nREQUIRED EMAIL SIGNATURE: {signature}\nVERIFIED ACCOUNT FACTS: {facts}\nQUESTION TO TEST (NOT A FACT): {hypothesis}\nRECIPIENT: {name} ({title}, {vantage})\nLIKELY ACCESS (INTERNAL, NOT COPY): {can_observe}\nASK SCOPE: {ask_scope}\nDETERMINISTIC FINDINGS: {deterministic}\n\nCURRENT SEQUENCE:\n{sequence}\n\nRETRIEVED KNOWLEDGE FOR THIS REVIEWER:\n{knowledge}",
        task = task,
        signature = pb.signature,
        facts = account.observed_facts.join(" | "),
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
                "participant_reported_workflows": call.reported_workflows,
                "working_interpretations": call.working_interpretations,
                "permitted_follow_up_angles": call.follow_up_angles,
                "evidence_boundaries": call.limits,
                "source_url": call.source_url,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&json!({
        "business": business.name,
        "summary": business.summary,
        "proven_seller_facts": business.known_facts,
        "first_party_market_discovery": discovery,
        "hard_constraints": business.constraints,
        "instruction": format!(
            "Use this to represent {} accurately. Discovery calls are market-level seller evidence, never proof about the recipient. At most once in a sequence, a later follow-up may explicitly attribute one relevant call observation and ask whether it matches this person's real workflow. Never imply consensus, quote an estimate as fact, or dump goals, constraints, and strategy into the message.",
            business.name
        )
    }))
    .unwrap_or_default()
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
            planning_priority(right)
                .cmp(&planning_priority(left))
                .then_with(|| left.name.cmp(&right.name))
        });
        selected.extend(candidates.into_iter().take(per_account.max(1)));
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
    // Cold copy needs enough room for one grounded question, not enough room to
    // turn the account brief into an executive summary.
    if touch.channel.eq_ignore_ascii_case("linkedin_request") {
        (8, 24)
    } else if touch.channel.eq_ignore_ascii_case("linkedin_or_email") {
        if touch.stage == 7 {
            (12, 35)
        } else {
            (12, 45)
        }
    } else if touch.channel.eq_ignore_ascii_case("email") {
        if touch.stage == 1 {
            (pb.min_words, pb.max_words)
        } else if touch.stage == 7 {
            (12, 35)
        } else {
            // A reply-thread follow-up can be a single natural sentence. The
            // old 25-word floor made editors add filler solely for counting.
            (12, 60)
        }
    } else if touch.channel.eq_ignore_ascii_case("linkedin") {
        (12, 32)
    } else {
        (12, 40)
    }
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
            if !(2..=8).contains(&subject_words) {
                issues.push(format!(
                    "stage {} subject has {subject_words} words (needs 2–8)",
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
        if touch.body.matches('?').count() > 1 {
            issues.push(format!("stage {} asks more than one question", touch.stage));
        }
        if touch.stage == 7 && touch.body.contains('?') {
            issues.push("stage 7 must close without a question".to_string());
        }
        if touch.stage > 1 && touch.body.to_ascii_lowercase().contains("gnk") {
            issues.push(format!(
                "stage {} repeats the GnK introduction",
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
    }

    let emails = sequence
        .touches
        .iter()
        .filter(|touch| is_email_capable_channel(&touch.channel))
        .collect::<Vec<_>>();
    for left in 0..emails.len() {
        for right in (left + 1)..emails.len() {
            let similarity = word_set_similarity(&emails[left].body, &emails[right].body);
            if similarity > 0.55 {
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

fn word_set_similarity(left: &str, right: &str) -> f64 {
    let words = |value: &str| {
        value
            .split_whitespace()
            .map(|word| {
                word.trim_matches(|c: char| !c.is_alphanumeric())
                    .to_ascii_lowercase()
            })
            .filter(|word| word.len() >= 5)
            .collect::<HashSet<_>>()
    };
    let left = words(left);
    let right = words(right);
    let union = left.union(&right).count();
    if union == 0 {
        0.0
    } else {
        left.intersection(&right).count() as f64 / union as f64
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(70).collect()
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
        "minItems": n,
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
                    "required": ["person_key", "touches", "applied_principles"],
                    "properties": {
                        "person_key": { "type": "string" },
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
        "required": ["reviews"],
        "properties": {
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
        affected_stages, business_copy_context, format_progress_status, is_email_capable_channel,
        normalize_dashes, normalize_principle_ids, provisional_channel, provisional_day_offset,
        select_people_for_planning, sequence_quality_issues, CopySequence, CopyTouch,
        PlanProgressRecipient, PlanProgressUpdate, TouchReview,
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
    fn business_context_reaches_the_writer() {
        let businesses = Businesses::load("businesses").expect("load businesses");
        let context = business_copy_context(businesses.get("gnk").expect("gnk business"));
        assert!(context.contains("GnK builds custom software and AI systems"));
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
    fn bulk_planning_selects_at_most_two_real_workflow_contacts_per_account() {
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
        assert_eq!(account_a, vec!["o", "e"]);
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
