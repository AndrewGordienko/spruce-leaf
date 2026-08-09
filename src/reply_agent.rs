//! Thread-aware reply handling.
//!
//! The classifier in `triage` stops a cold sequence safely. This module adds
//! the missing conversational layer: it resolves CC/referral replies onto the
//! original thread, records every message, drafts the next bounded response,
//! offers checked calendar slots, and books only a slot that appeared in a
//! previously *sent* message and was explicitly accepted.

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::compliance::Compliance;
use crate::db::{
    Conversation, ConversationMessage, CustomerDevelopmentRecord, GtmOutcome, Meeting, Person,
    ProofBrief, Reply, SharedDb, SignalObservation,
};
use crate::engine::Engine;
use crate::google_calendar::{CalendarSlot, GoogleCalendar};
use crate::knowledge::core_strategy_block;
use crate::playbook::{self, Playbooks};

#[derive(Debug, Clone, Default)]
pub struct InboundMessage {
    pub from_email: String,
    pub from_name: String,
    pub participants: Vec<String>,
    pub subject: String,
    pub body: String,
    pub message_id: String,
    pub in_reply_to: String,
    pub references: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct Decision {
    category: String,
    summary: String,
    next_action: String,
    draft_reply: String,
    accepted_slot: String,
    offered_slots: Vec<String>,
    referred_name: String,
    referred_email: String,
    /// Structured discovery fields are private operating notes. Empty means the
    /// reply did not establish them; the model must never fill gaps by inference.
    validated_problem: String,
    current_workflow: String,
    #[serde(default)]
    evidence_available: Vec<String>,
    #[serde(default)]
    customer_data: Vec<String>,
    proof_scope: String,
    success_metric: String,
    stop_condition: String,
    /// none | discovery_needed | ready
    proof_readiness: String,
    /// Wapahki customer-development evidence. These stay empty unless the
    /// prospect's own message establishes them.
    task_scope: String,
    why_still_manual: String,
    #[serde(default)]
    task_variations: Vec<String>,
    #[serde(default)]
    task_exceptions: Vec<String>,
    task_economics: String,
    /// none | evaluation_agreed | design_partner | loi_candidate |
    /// conditional_loi | paid_pilot | deployment
    commitment_kind: String,
    commitment_detail: String,
    loi_terms: String,
    next_commitment: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplyOutcome {
    pub action: String,
    pub draft_id: Option<String>,
    pub meeting_id: Option<String>,
}

/// Process a sales reply already resolved to `conversation` by RFC headers or
/// known sender identity.
#[allow(clippy::too_many_arguments)]
pub async fn handle_inbound(
    db: &SharedDb,
    client: &Engine,
    playbooks: &Playbooks,
    conversation: &Conversation,
    person: &Person,
    inbound: &InboundMessage,
    allow_booking: bool,
) -> Result<ReplyOutcome> {
    if db.reply_exists(&inbound.message_id)? {
        return Ok(ReplyOutcome {
            action: "duplicate".into(),
            draft_id: None,
            meeting_id: None,
        });
    }

    let inbound_id = db.insert_conversation_message(&ConversationMessage {
        conversation_id: conversation.id.clone(),
        direction: "inbound".into(),
        sender_email: inbound.from_email.clone(),
        participants: inbound.participants.clone(),
        subject: inbound.subject.clone(),
        body: inbound.body.chars().take(8_000).collect(),
        status: "received".into(),
        message_id: inbound.message_id.clone(),
        in_reply_to: inbound.in_reply_to.clone(),
        references: inbound.references.clone(),
        ..Default::default()
    })?;

    // Compliance is deterministic and runs before any model or calendar call.
    if Compliance::is_optout(&inbound.body) || Compliance::is_optout(&inbound.subject) {
        let action = stop_for_optout(db, conversation, person, &inbound.from_email)?;
        db.record_reply(&Reply {
            conversation_id: conversation.id.clone(),
            person_id: person.id.clone(),
            sequence_id: conversation.sequence_id.clone(),
            from_email: inbound.from_email.clone(),
            subject: inbound.subject.clone(),
            body: inbound.body.chars().take(4_000).collect(),
            classification: "unsubscribe".into(),
            action_taken: action.clone(),
            message_id: inbound.message_id.clone(),
            in_reply_to: inbound.in_reply_to.clone(),
            ..Default::default()
        })?;
        return Ok(ReplyOutcome {
            action,
            draft_id: None,
            meeting_id: None,
        });
    }

    let lead = db.get_lead(&conversation.lead_id)?;
    let customer_development =
        db.customer_development_for_lead(&conversation.brand, &conversation.lead_id)?;
    let pb = playbooks.get(&conversation.brand)?;
    let history = db.list_conversation_messages(&conversation.id)?;
    let sent_slots = db.sent_offered_slots(&conversation.id)?;
    let calendar = GoogleCalendar::from_env(&conversation.brand)?;
    let available = match &calendar {
        Some(calendar) => match calendar.available_slots(3).await {
            Ok(slots) => slots,
            Err(error) => {
                db.log_event(
                    &conversation.brand,
                    &person.id,
                    &inbound_id,
                    "calendar_error",
                    &format!("availability: {error:#}"),
                )?;
                Vec::new()
            }
        },
        None => Vec::new(),
    };

    let decision = decide(
        client,
        pb,
        person,
        lead.as_ref(),
        customer_development.as_ref(),
        inbound,
        &history,
        &sent_slots,
        &available,
    )
    .await
    .unwrap_or_else(|error| Decision {
        category: "other".into(),
        summary: format!("reply-agent classification failed: {error}"),
        ..Default::default()
    });
    let category = normalized_category(&decision.category);

    if category == "unsubscribe" {
        let action = stop_for_optout(db, conversation, person, &inbound.from_email)?;
        db.record_reply(&Reply {
            conversation_id: conversation.id.clone(),
            person_id: person.id.clone(),
            sequence_id: conversation.sequence_id.clone(),
            from_email: inbound.from_email.clone(),
            subject: inbound.subject.clone(),
            body: inbound.body.chars().take(4_000).collect(),
            classification: category,
            action_taken: action.clone(),
            message_id: inbound.message_id.clone(),
            in_reply_to: inbound.in_reply_to.clone(),
            ..Default::default()
        })?;
        return Ok(ReplyOutcome {
            action,
            draft_id: None,
            meeting_id: None,
        });
    }

    if category == "auto_reply" {
        let action = "noted (auto-reply, sequence continues)".to_string();
        db.record_reply(&Reply {
            conversation_id: conversation.id.clone(),
            person_id: person.id.clone(),
            sequence_id: conversation.sequence_id.clone(),
            from_email: inbound.from_email.clone(),
            subject: inbound.subject.clone(),
            body: inbound.body.chars().take(4_000).collect(),
            classification: category,
            action_taken: action.clone(),
            message_id: inbound.message_id.clone(),
            in_reply_to: inbound.in_reply_to.clone(),
            ..Default::default()
        })?;
        return Ok(ReplyOutcome {
            action,
            draft_id: None,
            meeting_id: None,
        });
    }

    // Every human reply stops the cold sequence before any conversational work.
    db.set_person_status(&person.id, "replied")?;
    if !conversation.sequence_id.is_empty() {
        db.stop_sequence(&conversation.sequence_id, "completed", "cancelled")?;
    }

    let accepted = accepted_offered_slot(&decision.accepted_slot, &sent_slots);
    let mut meeting_id = None;
    let mut booked = None;
    let mut booking_note = String::new();
    if let Some(start) = accepted {
        if let Some(calendar) = &calendar {
            if allow_booking {
                if calendar.slot_is_free(start).await? {
                    let account = lead
                        .as_ref()
                        .map(|lead| lead.name.as_str())
                        .unwrap_or("prospect");
                    let event = calendar
                        .book(
                            &inbound.from_email,
                            start,
                            &format!("{} discovery — {account}", pb.name),
                            &format!(
                                "Booked from Spruce Leaf conversation {} after the attendee accepted a previously offered slot.",
                                conversation.id
                            ),
                            &conversation.id,
                        )
                        .await?;
                    let id = db.record_meeting(&Meeting {
                        conversation_id: conversation.id.clone(),
                        brand: conversation.brand.clone(),
                        person_id: person.id.clone(),
                        attendee_email: inbound.from_email.clone(),
                        starts_at: event.start.to_rfc3339(),
                        ends_at: event.end.to_rfc3339(),
                        timezone: event.timezone.clone(),
                        status: "booked".into(),
                        google_event_id: event.event_id.clone(),
                        html_link: event.html_link.clone(),
                        meet_link: event.meet_link.clone(),
                        ..Default::default()
                    })?;
                    db.set_person_status(&person.id, "meeting_booked")?;
                    db.log_event(
                        &conversation.brand,
                        &person.id,
                        &inbound_id,
                        "meeting_booked",
                        &format!("{} with {}", event.start, inbound.from_email),
                    )?;
                    meeting_id = Some(id);
                    booked = Some(event);
                } else {
                    booking_note = "accepted slot is no longer free".into();
                }
            } else {
                let end = start + Duration::minutes(calendar.duration_minutes());
                let id = db.record_meeting(&Meeting {
                    conversation_id: conversation.id.clone(),
                    brand: conversation.brand.clone(),
                    person_id: person.id.clone(),
                    attendee_email: inbound.from_email.clone(),
                    starts_at: start.to_rfc3339(),
                    ends_at: end.to_rfc3339(),
                    timezone: calendar.timezone_name().to_string(),
                    status: "pending".into(),
                    ..Default::default()
                })?;
                booking_note = "accepted slot recorded; calendar booking awaits approval".into();
                meeting_id = Some(id);
            }
        } else {
            booking_note = "accepted slot detected, but Google Calendar is not configured".into();
        }
    }

    let validated_offers = validate_offers(&decision.offered_slots, &available);
    let draft_body = if let Some(event) = &booked {
        let local = event
            .start
            .with_timezone(&event.timezone.parse().unwrap_or(chrono_tz::Europe::London));
        let link = if event.meet_link.is_empty() {
            String::new()
        } else {
            format!("\n\nGoogle Meet: {}", event.meet_link)
        };
        format!(
            "Thanks — I’ve sent the calendar invitation for {}.{}\n\n{}",
            local.format("%A %-d %B at %-I:%M %p %Z"),
            link,
            pb.signature
        )
    } else if !booking_note.is_empty() && accepted.is_some() {
        // Never let a model claim a booking that did not happen.
        String::new()
    } else {
        playbook::enforce_signature(decision.draft_reply.trim(), &pb.signature)
    };

    let draft_id = if draft_body.trim().is_empty() {
        None
    } else {
        Some(db.insert_conversation_message(&ConversationMessage {
            conversation_id: conversation.id.clone(),
            direction: "outbound".into(),
            recipient_email: inbound.from_email.clone(),
            participants: inbound.participants.clone(),
            subject: reply_subject(&inbound.subject),
            body: draft_body,
            status: "draft".into(),
            in_reply_to: inbound.message_id.clone(),
            references: merged_references(inbound),
            classification: category.clone(),
            action: decision.next_action.clone(),
            offered_slots: validated_offers,
            ..Default::default()
        })?)
    };

    let mut action = format!("{category}: {}", decision.summary.trim());
    if !decision.referred_email.trim().is_empty() {
        action.push_str(&format!(
            "; referral {} <{}>",
            decision.referred_name.trim(),
            decision.referred_email.trim()
        ));
    }
    if let Some(id) = &draft_id {
        action.push_str(&format!("; reply draft {id}"));
    }
    if !booking_note.is_empty() {
        action.push_str(&format!("; {booking_note}"));
    }
    if booked.is_some() {
        action.push_str("; meeting booked + invite sent");
    }

    db.log_event(
        &conversation.brand,
        &person.id,
        &inbound_id,
        "replied",
        &action,
    )?;
    db.record_reply(&Reply {
        conversation_id: conversation.id.clone(),
        person_id: person.id.clone(),
        sequence_id: conversation.sequence_id.clone(),
        from_email: inbound.from_email.clone(),
        subject: inbound.subject.clone(),
        body: inbound.body.chars().take(4_000).collect(),
        classification: category.clone(),
        action_taken: action.clone(),
        message_id: inbound.message_id.clone(),
        in_reply_to: inbound.in_reply_to.clone(),
        ..Default::default()
    })?;
    record_customer_development_reply(db, conversation, person, &decision)?;
    record_gtm_reply_learning(
        db,
        conversation,
        person,
        &inbound_id,
        &inbound.message_id,
        &category,
        &decision,
        booked.is_some(),
    )?;

    Ok(ReplyOutcome {
        action,
        draft_id,
        meeting_id,
    })
}

fn record_customer_development_reply(
    db: &SharedDb,
    conversation: &Conversation,
    person: &Person,
    decision: &Decision,
) -> Result<()> {
    if conversation.brand != "wapahki" {
        return Ok(());
    }
    let mut record = db
        .customer_development_for_lead(&conversation.brand, &conversation.lead_id)?
        .unwrap_or_else(|| CustomerDevelopmentRecord {
            brand: conversation.brand.clone(),
            lead_id: conversation.lead_id.clone(),
            ..Default::default()
        });
    let prior_stage = crate::gtm::customer_development_stage(&record).to_string();
    record.person_id = person.id.clone();
    record.conversation_id = conversation.id.clone();
    if record.engaged_at.is_empty() {
        record.engaged_at = crate::db::now();
    }
    replace_if_present(&mut record.problem, &decision.validated_problem);
    replace_if_present(&mut record.current_workflow, &decision.current_workflow);
    replace_if_present(&mut record.task_scope, &decision.task_scope);
    replace_if_present(&mut record.why_manual, &decision.why_still_manual);
    replace_if_present(&mut record.economics, &decision.task_economics);
    replace_if_present(&mut record.success_criteria, &decision.success_metric);
    replace_if_present(&mut record.stop_condition, &decision.stop_condition);
    replace_if_present(&mut record.commitment_detail, &decision.commitment_detail);
    replace_if_present(&mut record.loi_conditions, &decision.loi_terms);
    replace_if_present(&mut record.next_action, &decision.next_commitment);
    merge_unique(&mut record.variations, &decision.task_variations);
    merge_unique(&mut record.exceptions, &decision.task_exceptions);
    merge_unique(&mut record.evidence, &decision.evidence_available);
    merge_unique(&mut record.evidence, &decision.customer_data);
    merge_unique(
        &mut record.stakeholders,
        &[format!("{} — {}", person.name, person.title)],
    );
    let commitment = crate::gtm::normalize_commitment_kind(&decision.commitment_kind);
    if commitment != "none" {
        record.commitment_kind = commitment.into();
    } else if record.commitment_kind.is_empty() {
        record.commitment_kind = "none".into();
    }
    record.stage = crate::gtm::customer_development_stage(&record).into();
    record.source = "prospect_reply".into();
    db.upsert_customer_development(&record)?;

    if record.stage != prior_stage {
        db.record_gtm_outcome(&GtmOutcome {
            brand: record.brand.clone(),
            kind: "customer_development_stage".into(),
            lead_id: record.lead_id.clone(),
            person_id: record.person_id.clone(),
            conversation_id: record.conversation_id.clone(),
            value: crate::gtm::CUSTOMER_DEVELOPMENT_STAGES
                .iter()
                .position(|stage| stage.key == record.stage)
                .unwrap_or(0) as f64,
            detail: format!("{prior_stage} → {}", record.stage),
            source: "reply".into(),
            fingerprint: format!(
                "customer-development:{}:{}",
                record.conversation_id, record.stage
            ),
            ..Default::default()
        })?;
    }
    Ok(())
}

fn replace_if_present(target: &mut String, candidate: &str) {
    if !candidate.trim().is_empty() {
        *target = candidate.trim().to_string();
    }
}

fn merge_unique(target: &mut Vec<String>, candidates: &[String]) {
    for candidate in candidates.iter().map(|value| value.trim()) {
        if !candidate.is_empty()
            && !target
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(candidate))
        {
            target.push(candidate.to_string());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn record_gtm_reply_learning(
    db: &SharedDb,
    conversation: &Conversation,
    person: &Person,
    inbound_id: &str,
    message_id: &str,
    category: &str,
    decision: &Decision,
    meeting_booked: bool,
) -> Result<()> {
    let attribution = if conversation.sequence_id.is_empty() {
        None
    } else {
        db.sequence_gtm_attribution(&conversation.sequence_id)?
    };
    let Some(attribution) = attribution else {
        return Ok(());
    };
    let outcome_kind = match category {
        "interested" => "positive_reply",
        "correction" => "correction",
        "referral" => "referral",
        "not_now" => "not_now",
        "objection" => "objection",
        _ => "human_reply",
    };
    let source_key = if message_id.trim().is_empty() {
        inbound_id
    } else {
        message_id
    };
    db.record_gtm_outcome(&GtmOutcome {
        brand: conversation.brand.clone(),
        kind: outcome_kind.into(),
        lead_id: conversation.lead_id.clone(),
        person_id: person.id.clone(),
        sequence_id: conversation.sequence_id.clone(),
        conversation_id: conversation.id.clone(),
        play_id: attribution.play_id.clone(),
        experiment_id: attribution.experiment_id.clone(),
        experiment_assignment_id: attribution.experiment_assignment_id.clone(),
        signal_observation_ids: attribution.signal_observation_ids.clone(),
        value: if category == "interested" { 1.0 } else { 0.0 },
        detail: decision.summary.trim().to_string(),
        source: "reply".into(),
        fingerprint: format!("reply:{source_key}"),
        ..Default::default()
    })?;
    if meeting_booked {
        db.record_gtm_outcome(&GtmOutcome {
            brand: conversation.brand.clone(),
            kind: "meeting_booked".into(),
            lead_id: conversation.lead_id.clone(),
            person_id: person.id.clone(),
            sequence_id: conversation.sequence_id.clone(),
            conversation_id: conversation.id.clone(),
            play_id: attribution.play_id.clone(),
            experiment_id: attribution.experiment_id.clone(),
            experiment_assignment_id: attribution.experiment_assignment_id.clone(),
            signal_observation_ids: attribution.signal_observation_ids.clone(),
            value: 1.0,
            detail: "Prospect accepted a previously offered slot.".into(),
            source: "meeting".into(),
            fingerprint: format!("meeting:{source_key}"),
            ..Default::default()
        })?;
    }

    let validated_problem = decision.validated_problem.trim();
    if validated_problem.is_empty() {
        return Ok(());
    }
    db.record_signal_observation(&SignalObservation {
        brand: conversation.brand.clone(),
        definition_key: "conversation.problem_confirmed".into(),
        lead_id: conversation.lead_id.clone(),
        person_id: person.id.clone(),
        conversation_id: conversation.id.clone(),
        source_name: "prospect_reply".into(),
        provider_key: source_key.to_string(),
        value_json: serde_json::json!({"category": category}).to_string(),
        evidence: validated_problem.to_string(),
        confidence: if category == "correction" { 0.95 } else { 0.85 },
        status: "verified".into(),
        ..Default::default()
    })?;

    // A proof brief is an internal handoff, never approval to build or send.
    // Even a model-rated "ready" proof stays at the human-review gate.
    if matches!(category, "interested" | "correction") && !attribution.play_id.is_empty() {
        let play = db
            .list_gtm_plays(Some(&conversation.brand))?
            .into_iter()
            .find(|play| play.id == attribution.play_id);
        if let Some(play) = play {
            let proof_id = db.upsert_proof_brief(&ProofBrief {
                brand: conversation.brand.clone(),
                lead_id: conversation.lead_id.clone(),
                person_id: person.id.clone(),
                conversation_id: conversation.id.clone(),
                play_id: play.id.clone(),
                status: if decision.proof_readiness == "ready" {
                    "ready".into()
                } else {
                    "draft".into()
                },
                problem: validated_problem.to_string(),
                current_workflow: decision.current_workflow.trim().to_string(),
                evidence_available: decision.evidence_available.clone(),
                scope: if decision.proof_scope.trim().is_empty() {
                    play.proof_description.clone()
                } else {
                    decision.proof_scope.trim().to_string()
                },
                customer_data: decision.customer_data.clone(),
                success_metric: if decision.success_metric.trim().is_empty() {
                    play.success_metric.clone()
                } else {
                    decision.success_metric.trim().to_string()
                },
                stop_condition: if decision.stop_condition.trim().is_empty() {
                    play.kill_condition.clone()
                } else {
                    decision.stop_condition.trim().to_string()
                },
                stakeholders: vec![format!("{} — {}", person.name, person.title)],
                owner: "forward_deployed_gtm".into(),
                expansion_path: "Only define expansion after this proof passes its agreed metric."
                    .into(),
                ..Default::default()
            })?;
            db.record_gtm_outcome(&GtmOutcome {
                brand: conversation.brand.clone(),
                kind: "proof_brief_created".into(),
                lead_id: conversation.lead_id.clone(),
                person_id: person.id.clone(),
                sequence_id: conversation.sequence_id.clone(),
                conversation_id: conversation.id.clone(),
                play_id: play.id,
                experiment_id: attribution.experiment_id,
                experiment_assignment_id: attribution.experiment_assignment_id,
                signal_observation_ids: attribution.signal_observation_ids,
                value: 0.0,
                detail: format!("Internal proof brief {proof_id}; human approval required."),
                source: "reply".into(),
                fingerprint: format!("proof-brief:{source_key}"),
                ..Default::default()
            })?;
        }
    }
    Ok(())
}

/// Explicitly finish pending acceptances created by `inbox` without `--book`.
/// Each slot is rechecked before event insertion, just like the live path.
pub async fn book_pending(db: &SharedDb, brand: &str, meeting_id: Option<&str>) -> Result<usize> {
    let Some(calendar) = GoogleCalendar::from_env(brand)? else {
        anyhow::bail!("Google Calendar is not configured for {brand}");
    };
    let pending = db
        .list_meetings(Some(brand))?
        .into_iter()
        .filter(|meeting| meeting.status == "pending")
        .filter(|meeting| meeting_id.is_none_or(|id| meeting.id == id))
        .collect::<Vec<_>>();
    let mut booked = 0usize;
    for meeting in pending {
        let start = DateTime::parse_from_rfc3339(&meeting.starts_at)?.with_timezone(&Utc);
        if !calendar.slot_is_free(start).await? {
            db.log_event(
                brand,
                &meeting.person_id,
                "",
                "calendar_conflict",
                &format!("pending meeting {} is no longer free", meeting.starts_at),
            )?;
            continue;
        }
        let conversation = db
            .get_conversation(&meeting.conversation_id)?
            .ok_or_else(|| {
                anyhow::anyhow!("conversation {} is missing", meeting.conversation_id)
            })?;
        let account = db
            .get_lead(&conversation.lead_id)?
            .map(|lead| lead.name)
            .unwrap_or_else(|| "prospect".into());
        let event = calendar
            .book(
                &meeting.attendee_email,
                start,
                &format!("{} discovery — {account}", brand),
                &format!(
                    "Booked from approved Spruce Leaf meeting {} after an explicitly accepted slot.",
                    meeting.id
                ),
                &meeting.conversation_id,
            )
            .await?;
        db.update_meeting_booked(
            &meeting.id,
            &event.event_id,
            &event.html_link,
            &event.meet_link,
        )?;
        db.set_person_status(&meeting.person_id, "meeting_booked")?;
        db.log_event(
            brand,
            &meeting.person_id,
            "",
            "meeting_booked",
            &format!("{} with {}", event.start, meeting.attendee_email),
        )?;
        booked += 1;
    }
    Ok(booked)
}

#[allow(clippy::too_many_arguments)]
async fn decide(
    client: &Engine,
    pb: &crate::playbook::Playbook,
    person: &Person,
    lead: Option<&crate::db::Lead>,
    customer_development: Option<&CustomerDevelopmentRecord>,
    inbound: &InboundMessage,
    history: &[ConversationMessage],
    sent_slots: &[String],
    available: &[CalendarSlot],
) -> Result<Decision> {
    let customer_development_rules = if pb.key == "wapahki" {
        " For Wapahki, use customer-development discipline: ask about actual past/current behaviour, not hypothetical enthusiasm. Extract task_scope (one object/motion/handoff), why_still_manual, task_variations, task_exceptions, and task_economics only from explicit first-hand evidence. commitment_kind must be none unless the prospect explicitly commits the corresponding currency: an agreed evaluation, ongoing design-partner access, material LOI terms, written conditional intent, payment for a pilot, or deployment. Move only one rung: reply/correction -> task map -> shared sketch/video/SKU data/site observation -> agreed evaluation -> design partner -> LOI candidate -> conditional LOI -> paid pilot -> deployment. Never turn politeness, a meeting, or model confidence into a higher stage."
    } else {
        ""
    };
    let system = format!(
        "You are Andrew's thread-aware B2B reply and discovery agent for {brand}. Continue a real conversation, not a cold sequence. Be concise, answer direct questions honestly, preserve the sender's exact intent, and move only one commitment rung at a time. Extract validated_problem, current_workflow, evidence_available, customer_data, proof_scope, success_metric, and stop_condition only when the human's own message establishes them; leave unknown fields empty. proof_readiness is none, discovery_needed, or ready. A correction is valuable market evidence and must be categorized correction, not disguised as interest. Never claim a meeting is booked: only the application can book it after your structured decision. If the prospect accepts one of SENT_OFFERED_SLOTS, copy that exact RFC3339 value into accepted_slot. Otherwise accepted_slot must be empty. If offering a meeting, choose at most 2 values exactly from AVAILABLE_SLOTS and include their human display text naturally in draft_reply. Never invent availability, proof readiness, customer data, metrics, or a referral address.{customer_development_rules}",
        brand = pb.name,
    );
    let history = history
        .iter()
        .map(|message| {
            json!({
                "direction": message.direction,
                "from": message.sender_email,
                "to": message.recipient_email,
                "subject": message.subject,
                "body": message.body.chars().take(2_000).collect::<String>(),
                "status": message.status,
            })
        })
        .collect::<Vec<_>>();
    let slots = available
        .iter()
        .map(|slot| json!({"start": slot.start.to_rfc3339(), "display": slot.display}))
        .collect::<Vec<_>>();
    let context = json!({
        "brand_intro": pb.one_liner,
        "original_person": {"name": person.name, "title": person.title, "email": person.email},
        "account": lead.map(|lead| json!({
            "name": lead.name,
            "hypothesis": lead.hypothesis,
            "observed_facts": lead.observed_facts,
            "hard_buyer_question": lead.hard_buyer_question,
        })),
        "customer_development_so_far": customer_development,
        "current_sender": {"name": inbound.from_name, "email": inbound.from_email},
        "thread": history,
        "SENT_OFFERED_SLOTS": sent_slots,
        "AVAILABLE_SLOTS": slots,
    });
    let proof_guidance = customer_development
        .filter(|record| {
            matches!(
                crate::gtm::customer_development_stage(record),
                "evidence_shared"
                    | "evaluation_agreed"
                    | "design_partner"
                    | "loi_candidate"
                    | "conditional_loi"
                    | "paid_pilot"
                    | "deployment"
            )
        })
        .map(|_| core_strategy_block("proof"))
        .unwrap_or_default();
    let user = format!(
        "Analyse the newest inbound message and decide the next bounded action. A correction or referral is useful evidence; do not force a meeting or a proof. A proof is ready only when a specific problem, bounded sample/data, and observable success measure are established. Draft a reply only when a human response is useful.\n\n{}\n\n{}\n\n{}",
        serde_json::to_string_pretty(&context).unwrap_or_default(),
        core_strategy_block("replies"),
        proof_guidance,
    );
    client
        .structured_stage("reply.compose", &system, &user, schema())
        .await
}

fn stop_for_optout(
    db: &SharedDb,
    conversation: &Conversation,
    person: &Person,
    email: &str,
) -> Result<String> {
    db.add_suppression(&conversation.brand, email, "unsubscribed")?;
    db.set_person_status(&person.id, "unsubscribed")?;
    if !conversation.sequence_id.is_empty() {
        db.stop_sequence(&conversation.sequence_id, "stopped", "cancelled")?;
    }
    db.log_event(
        &conversation.brand,
        &person.id,
        "",
        "unsubscribed",
        "opt-out honored",
    )?;
    Ok("suppressed + sequence stopped".into())
}

fn accepted_offered_slot(raw: &str, offered: &[String]) -> Option<DateTime<Utc>> {
    let accepted = DateTime::parse_from_rfc3339(raw.trim())
        .ok()?
        .with_timezone(&Utc);
    offered.iter().find_map(|candidate| {
        let parsed = DateTime::parse_from_rfc3339(candidate)
            .ok()?
            .with_timezone(&Utc);
        (parsed == accepted).then_some(parsed)
    })
}

fn validate_offers(requested: &[String], available: &[CalendarSlot]) -> Vec<String> {
    requested
        .iter()
        .filter_map(|raw| {
            let parsed = DateTime::parse_from_rfc3339(raw).ok()?.with_timezone(&Utc);
            available
                .iter()
                .any(|slot| slot.start == parsed)
                .then(|| parsed.to_rfc3339())
        })
        .take(2)
        .collect()
}

fn normalized_category(raw: &str) -> String {
    match raw.trim().to_lowercase().as_str() {
        "interested" | "correction" | "not_now" | "objection" | "referral" | "unsubscribe"
        | "auto_reply" | "other" => raw.trim().to_lowercase(),
        _ => "other".into(),
    }
}

fn reply_subject(subject: &str) -> String {
    if subject.trim().to_lowercase().starts_with("re:") {
        subject.trim().to_string()
    } else {
        format!("Re: {}", subject.trim())
    }
}

fn merged_references(inbound: &InboundMessage) -> Vec<String> {
    let mut references = inbound.references.clone();
    if !inbound.in_reply_to.trim().is_empty() {
        references.push(inbound.in_reply_to.clone());
    }
    if !inbound.message_id.trim().is_empty() {
        references.push(inbound.message_id.clone());
    }
    let mut deduplicated = Vec::new();
    for reference in references {
        if !reference.trim().is_empty() && !deduplicated.contains(&reference) {
            deduplicated.push(reference);
        }
    }
    deduplicated
}

fn schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["category","summary","next_action","draft_reply","accepted_slot","offered_slots","referred_name","referred_email","validated_problem","current_workflow","evidence_available","customer_data","proof_scope","success_metric","stop_condition","proof_readiness","task_scope","why_still_manual","task_variations","task_exceptions","task_economics","commitment_kind","commitment_detail","loi_terms","next_commitment"],
        "properties": {
            "category": {"type":"string","enum":["interested","correction","not_now","objection","referral","unsubscribe","auto_reply","other"]},
            "summary": {"type":"string"},
            "next_action": {"type":"string","enum":["none","answer","clarify","offer_meeting","accepted_meeting","referral"]},
            "draft_reply": {"type":"string"},
            "accepted_slot": {"type":"string"},
            "offered_slots": {"type":"array","items":{"type":"string"},"maxItems":2},
            "referred_name": {"type":"string"},
            "referred_email": {"type":"string"},
            "validated_problem": {"type":"string"},
            "current_workflow": {"type":"string"},
            "evidence_available": {"type":"array","items":{"type":"string"}},
            "customer_data": {"type":"array","items":{"type":"string"}},
            "proof_scope": {"type":"string"},
            "success_metric": {"type":"string"},
            "stop_condition": {"type":"string"},
            "proof_readiness": {"type":"string","enum":["none","discovery_needed","ready"]}
            ,"task_scope": {"type":"string"}
            ,"why_still_manual": {"type":"string"}
            ,"task_variations": {"type":"array","items":{"type":"string"}}
            ,"task_exceptions": {"type":"array","items":{"type":"string"}}
            ,"task_economics": {"type":"string"}
            ,"commitment_kind": {"type":"string","enum":["none","evaluation_agreed","design_partner","loi_candidate","conditional_loi","paid_pilot","deployment"]}
            ,"commitment_detail": {"type":"string"}
            ,"loi_terms": {"type":"string"}
            ,"next_commitment": {"type":"string"}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_slot_must_equal_a_sent_offer() {
        let offers = vec!["2026-08-10T08:00:00+00:00".to_string()];
        assert!(accepted_offered_slot("2026-08-10T09:00:00+01:00", &offers).is_some());
        assert!(accepted_offered_slot("2026-08-11T08:00:00Z", &offers).is_none());
        assert!(accepted_offered_slot("Monday morning", &offers).is_none());
    }

    #[test]
    fn only_real_available_slots_survive_model_output() {
        let start = DateTime::parse_from_rfc3339("2026-08-10T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let slots = vec![CalendarSlot {
            start,
            end: start + Duration::minutes(30),
            display: "Mon 10 Aug at 9:00 AM BST".into(),
        }];
        let requested = vec![start.to_rfc3339(), "2026-08-12T08:00:00Z".into()];
        assert_eq!(
            validate_offers(&requested, &slots),
            vec![start.to_rfc3339()]
        );
    }
}
