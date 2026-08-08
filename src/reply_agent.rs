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
use crate::db::{Conversation, ConversationMessage, Meeting, Person, Reply, SharedDb};
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
        classification: category,
        action_taken: action.clone(),
        message_id: inbound.message_id.clone(),
        in_reply_to: inbound.in_reply_to.clone(),
        ..Default::default()
    })?;

    Ok(ReplyOutcome {
        action,
        draft_id,
        meeting_id,
    })
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
    inbound: &InboundMessage,
    history: &[ConversationMessage],
    sent_slots: &[String],
    available: &[CalendarSlot],
) -> Result<Decision> {
    let system = format!(
        "You are Andrew's thread-aware B2B reply agent for {brand}. Continue a real conversation, not a cold sequence. Be concise, answer direct questions honestly, preserve the sender's exact intent, and move only one commitment rung at a time. Never claim a meeting is booked: only the application can book it after your structured decision. If the prospect accepts one of SENT_OFFERED_SLOTS, copy that exact RFC3339 value into accepted_slot. Otherwise accepted_slot must be empty. If offering a meeting, choose at most 2 values exactly from AVAILABLE_SLOTS and include their human display text naturally in draft_reply. Never invent availability or a referral address.",
        brand = pb.name
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
        "current_sender": {"name": inbound.from_name, "email": inbound.from_email},
        "thread": history,
        "SENT_OFFERED_SLOTS": sent_slots,
        "AVAILABLE_SLOTS": slots,
    });
    let user = format!(
        "Analyse the newest inbound message and decide the next bounded action. A correction or referral is useful evidence; do not force a meeting. Draft a reply only when a human response is useful.\n\n{}\n\n{}",
        serde_json::to_string_pretty(&context).unwrap_or_default(),
        core_strategy_block("replies"),
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
        "interested" | "not_now" | "objection" | "referral" | "unsubscribe" | "auto_reply"
        | "other" => raw.trim().to_lowercase(),
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
        "required": ["category","summary","next_action","draft_reply","accepted_slot","offered_slots","referred_name","referred_email"],
        "properties": {
            "category": {"type":"string","enum":["interested","not_now","objection","referral","unsubscribe","auto_reply","other"]},
            "summary": {"type":"string"},
            "next_action": {"type":"string","enum":["none","answer","clarify","offer_meeting","accepted_meeting","referral"]},
            "draft_reply": {"type":"string"},
            "accepted_slot": {"type":"string"},
            "offered_slots": {"type":"array","items":{"type":"string"},"maxItems":2},
            "referred_name": {"type":"string"},
            "referred_email": {"type":"string"}
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
