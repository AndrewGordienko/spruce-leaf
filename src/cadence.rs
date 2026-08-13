//! The cadence engine — the clock that makes this an SDR instead of a drafter.
//!
//! A restart-safe loop: wake on an interval, find every touch that is `scheduled`
//! and due, and — for each, only if the recipient is verified, not suppressed,
//! and a mailbox has daily headroom — render the compliant body and send it,
//! advancing the touch and person state and logging an event. All state lives in
//! SQLite, so a crash/restart resumes exactly where it left off.
//!
//! Safety rails baked in:
//!   * recipient-local, industry/title-aware sending windows
//!   * a hard daily cap owned by each business, across all of its mailboxes
//!   * per-mailbox daily caps ([`Db::pick_mailbox_on`])
//!   * suppression checked immediately before every send
//!   * `dry_run` performs one read-only preview pass and exits — this is the
//!     DEFAULT, so testing cannot consume a scheduled touch or mailbox capacity.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use chrono::Utc;

use crate::business::{BusinessProfile, Businesses};
use crate::calendar::{self, TimingContext};
use crate::compliance::Compliance;
use crate::db::{GtmOutcome, Lead, Person, SharedDb, Touch};
use crate::playbook::Playbooks;
use crate::send::{self, Outgoing};

pub struct CadenceConfig {
    pub dry_run: bool,
    pub batch: i64,
    pub send_delay_ms: u64,
    pub interval_secs: u64,
}

impl Default for CadenceConfig {
    fn default() -> Self {
        CadenceConfig {
            dry_run: true,
            // Three independently capped brands can each send 30 emails/day.
            // One pass must therefore be able to drain the full 90-email
            // portfolio without requiring one brand to wait for another tick.
            batch: 90,
            send_delay_ms: std::env::var("CADENCE_SEND_DELAY_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1500),
            interval_secs: 60,
        }
    }
}

/// Run a single cadence pass. Returns how many touches were sent, or would be
/// sent in dry-run mode.
pub async fn tick(
    db: &SharedDb,
    playbooks: &Playbooks,
    businesses: &Businesses,
    compliance: &Compliance,
    cfg: &CadenceConfig,
) -> Result<usize> {
    let mut sent = 0usize;
    let mut dry_run_reservations = HashMap::<String, usize>::new();
    // Stands in for account-opening sends a live pass would have persisted, so a
    // dry-run preview reflects the same per-account throttling instead of
    // "would send" to everyone at a company at once.
    let mut dry_run_account_openers = HashMap::<String, usize>::new();

    // A real human reply outranks new cold work. These messages are separately
    // approval-gated, but once approved they get the first share of this pass.
    sent += tick_conversation_replies(
        db,
        playbooks,
        businesses,
        compliance,
        cfg,
        cfg.batch,
        &mut dry_run_reservations,
    )
    .await?;
    let remaining = cfg.batch.saturating_sub(sent as i64);
    // Scan beyond the send allowance because a large overdue queue for one
    // capped brand or account must not hide eligible work for the other brands.
    let scan_limit = remaining.saturating_mul(10).max(300);
    let due = db.due_touches(None, scan_limit)?;

    for touch in due {
        if sent as i64 >= cfg.batch {
            break;
        }
        let Some(person) = db.get_person(&touch.person_id)? else {
            if !cfg.dry_run {
                db.set_touch_status(&touch.id, "skipped", "", "", "person missing")?;
            }
            continue;
        };
        let Some(lead) = db.get_lead(&touch.lead_id)? else {
            if !cfg.dry_run {
                db.set_touch_status(&touch.id, "skipped", "", "", "lead missing")?;
            }
            continue;
        };
        let playbook = playbooks.get(&touch.brand)?;
        let profile = businesses.get(&touch.brand)?;

        // A scheduled row is not permanent authorization. Recheck current play
        // evidence, account ceiling, and recipient stage immediately before any
        // delivery work so stale/manual approvals cannot bypass GTM policy.
        if let Some(reason) = crate::gtm::delivery_block_reason(db, playbook, &lead, &person)? {
            if !cfg.dry_run {
                db.set_touch_status(&touch.id, "blocked", "", "", &reason)?;
                db.log_event(
                    &touch.brand,
                    &person.id,
                    &touch.id,
                    "delivery_blocked",
                    &reason,
                )?;
            } else {
                eprintln!(
                    "  · would block [{}] stage {} to {} — {}",
                    touch.brand, touch.stage, person.email, reason
                );
            }
            continue;
        }

        // Deterministic chronology guard: whatever the stored due_at says, a
        // later stage never delivers while an earlier stage in its sequence is
        // still pending. Mis-dated tails wait for their opener instead of
        // silently leap-frogging it.
        if touch.stage > 1
            && db.sequence_has_pending_earlier_stage(&touch.sequence_id, touch.stage)?
        {
            let reason = "chronology guard: an earlier stage in this sequence has not been sent";
            if !cfg.dry_run {
                db.set_touch_status(&touch.id, "blocked", "", "", reason)?;
                db.log_event(
                    &touch.brand,
                    &person.id,
                    &touch.id,
                    "delivery_blocked",
                    reason,
                )?;
            } else {
                eprintln!(
                    "  · would block [{}] stage {} to {} — {}",
                    touch.brand, touch.stage, person.email, reason
                );
            }
            continue;
        }

        // Conditional touches become a manual LinkedIn task when the operator
        // has marked the connection accepted. Unknown/requested/not-connected
        // safely fall back to the reviewed email version.
        let conditional = touch.channel.eq_ignore_ascii_case("linkedin_or_email");
        if conditional && person.linkedin_status == "connected" {
            if !cfg.dry_run {
                db.set_touch_status(
                    &touch.id,
                    "draft",
                    "",
                    "",
                    "LinkedIn connected: send this touch manually as a DM",
                )?;
            } else {
                eprintln!(
                    "  · would hold [{}] stage {} for LinkedIn DM to {}",
                    touch.brand, touch.stage, person.name
                );
            }
            continue;
        }
        // Only email-capable touches reach SMTP.
        if !touch.channel.eq_ignore_ascii_case("email") && !conditional {
            continue;
        }
        if person.email.trim().is_empty() || person.email_status != "verified" {
            if !cfg.dry_run {
                db.set_touch_status(&touch.id, "skipped", "", "", "email not verified")?;
            }
            continue;
        }
        if db.is_suppressed(&touch.brand, &person.email)? {
            if !cfg.dry_run {
                db.set_person_status(&person.id, "suppressed")?;
                if let Some(seq) = db.active_sequence_for_person(&person.id)? {
                    db.stop_sequence(&seq, "stopped", "cancelled")?;
                }
                db.log_event(
                    &touch.brand,
                    &person.id,
                    &touch.id,
                    "suppressed",
                    "skipped: suppressed",
                )?;
            }
            continue;
        }

        // Stop a cadence that has gone quiet rather than marching through every
        // remaining stage of a possibly mis-generated sequence.
        let unanswered_cap = profile.account_limits.max_unanswered_touches;
        if unanswered_cap > 0 && db.sequence_sent_count(&touch.sequence_id)? >= unanswered_cap {
            if !cfg.dry_run {
                record_nonresponse_outcome(db, &touch, &lead, &person)?;
                db.stop_sequence(&touch.sequence_id, "completed", "cancelled")?;
                db.log_event(
                    &touch.brand,
                    &person.id,
                    &touch.id,
                    "cadence_stopped",
                    &format!("stopped: {unanswered_cap} touches sent, no reply"),
                )?;
            } else {
                eprintln!(
                    "  · would stop [{}] sequence to {} — {unanswered_cap} sent, no reply",
                    touch.brand, person.email
                );
            }
            continue;
        }

        let timing = TimingContext {
            industry: &lead.industry,
            title: &person.title,
            vantage: &person.vantage,
            channel: "email",
            location: if person.location.is_empty() {
                &lead.hq
            } else {
                &person.location
            },
            timezone: if touch.recipient_timezone.is_empty() {
                if person.timezone.is_empty() {
                    &lead.timezone
                } else {
                    &person.timezone
                }
            } else {
                &touch.recipient_timezone
            },
            stable_key: &touch.id,
        };
        let now = Utc::now();
        if !calendar::can_send_now(profile, &timing, now)? {
            let slot = calendar::next_slot_with_learning(db, profile, &timing, now)?;
            if !cfg.dry_run {
                db.reschedule_touch(
                    &touch.id,
                    &slot.at.to_rfc3339(),
                    &slot.recipient_timezone,
                    &slot.rule,
                    &format!("deferred to recipient-local window: {}", slot.rationale),
                )?;
            }
            continue;
        }
        let (quota_start, quota_end, quota_date) = calendar::quota_day_bounds(profile, now)?;
        if !has_business_capacity(
            db,
            profile,
            quota_start,
            quota_end,
            &quota_date,
            cfg.dry_run,
            &dry_run_reservations,
        )? {
            let slot = calendar::next_slot_with_learning(db, profile, &timing, quota_end)?;
            if !cfg.dry_run {
                db.reschedule_touch(
                    &touch.id,
                    &slot.at.to_rfc3339(),
                    &slot.recipient_timezone,
                    &slot.rule,
                    &format!(
                        "deferred: {} daily touch cap reached; {}",
                        profile.calendar.daily_touch_cap, slot.rationale
                    ),
                )?;
            }
            continue;
        }

        // Per-account fan-out throttle: don't open a fresh cold front at an
        // account that's already being worked, or open more than the daily
        // allowance. Follow-ups to someone already in a thread are exempt — this
        // only gates *new* contacts, which is where a blast comes from.
        let is_new_front = person.status != "contacted";
        if is_new_front {
            if let Some(reason) = account_front_block_reason(
                db,
                profile,
                &touch.lead_id,
                quota_start,
                quota_end,
                &quota_date,
                cfg.dry_run.then_some(&dry_run_account_openers),
            )? {
                let slot = calendar::next_slot_with_learning(db, profile, &timing, quota_end)?;
                if !cfg.dry_run {
                    db.reschedule_touch(
                        &touch.id,
                        &slot.at.to_rfc3339(),
                        &slot.recipient_timezone,
                        &slot.rule,
                        &format!("deferred: account throttle ({reason}); {}", slot.rationale),
                    )?;
                } else {
                    eprintln!(
                        "  · would defer [{}] stage {} to {} — account throttle: {reason}",
                        touch.brand, touch.stage, person.email
                    );
                }
                continue;
            }
        }

        // A configured mailbox with daily headroom, else stop for this brand.
        let mailbox = if cfg.dry_run {
            db.preview_mailbox_on(&touch.brand, &quota_date)?
        } else {
            db.pick_mailbox_on(&touch.brand, &quota_date)?
        };
        let Some(mailbox) = mailbox else {
            if !cfg.dry_run {
                db.log_event(
                    &touch.brand,
                    &person.id,
                    &touch.id,
                    "error",
                    "no mailbox with capacity",
                )?;
            }
            continue;
        };

        let signature = playbooks
            .get(&touch.brand)
            .map(|p| p.signature.clone())
            .unwrap_or_default();
        let body = compliance.render_body(&touch.body, &signature, &mailbox.from_email);
        let in_reply_to = db.previous_message_id(&touch.sequence_id, touch.stage)?;
        let out = Outgoing {
            to: person.email.clone(),
            subject: touch.subject.clone(),
            body,
            list_unsubscribe: compliance.list_unsubscribe(
                &mailbox.from_email,
                &touch.brand,
                &person.id,
            ),
            in_reply_to,
            references: Vec::new(),
        };

        // `due_touches` is a snapshot. Another daemon may have read the same
        // row, so live delivery must win an atomic claim before sending.
        if !cfg.dry_run && !db.claim_touch_for_send(&touch.id)? {
            continue;
        }

        match send::send_email(&mailbox, &out, cfg.dry_run).await {
            Ok(_) if cfg.dry_run => {
                eprintln!(
                    "  · would send [{}] stage {} to {} via {}",
                    touch.brand, touch.stage, person.email, mailbox.from_email
                );
                sent += 1;
                reserve_dry_run(&mut dry_run_reservations, &touch.brand, &quota_date);
                if is_new_front {
                    reserve_account_opener(
                        &mut dry_run_account_openers,
                        &touch.lead_id,
                        &quota_date,
                    );
                }
                continue;
            }
            Ok(message_id) => {
                db.set_touch_status(&touch.id, "sent", &mailbox.id, &message_id, "")?;
                db.bump_mailbox_sent(&mailbox.id)?;
                db.set_person_status(&person.id, "contacted")?;
                db.log_event(
                    &touch.brand,
                    &person.id,
                    &touch.id,
                    "sent",
                    &format!(
                        "stage {} → {}{}",
                        touch.stage,
                        person.email,
                        if cfg.dry_run { " [dry-run]" } else { "" }
                    ),
                )?;
                sent += 1;
            }
            Err(e) => {
                db.set_touch_status(&touch.id, "failed", &mailbox.id, "", &format!("{e:#}"))?;
                db.log_event(
                    &touch.brand,
                    &person.id,
                    &touch.id,
                    "error",
                    &format!("send failed: {e}"),
                )?;
            }
        }

        // Gentle, slightly-varied pacing between sends.
        tokio::time::sleep(StdDuration::from_millis(jitter(
            cfg.send_delay_ms,
            &touch.id,
        )))
        .await;
    }

    let remaining = cfg.batch.saturating_sub(sent as i64);
    if remaining > 0 {
        sent += tick_opportunity_outreach(
            db,
            playbooks,
            businesses,
            compliance,
            cfg,
            remaining,
            &mut dry_run_reservations,
        )
        .await?;
    }

    Ok(sent)
}

fn record_nonresponse_outcome(
    db: &SharedDb,
    due_touch: &Touch,
    lead: &Lead,
    person: &Person,
) -> Result<()> {
    let Some(sequence) = db.sequence_gtm_attribution(&due_touch.sequence_id)? else {
        return Ok(());
    };
    let origin = db
        .list_touches_for_sequence(&due_touch.sequence_id)?
        .into_iter()
        .filter(|touch| touch.status == "sent")
        .max_by_key(|touch| touch.stage);
    db.record_gtm_outcome(&GtmOutcome {
        brand: due_touch.brand.clone(),
        kind: "nonresponse".into(),
        lead_id: due_touch.lead_id.clone(),
        person_id: due_touch.person_id.clone(),
        sequence_id: due_touch.sequence_id.clone(),
        play_id: sequence.play_id,
        experiment_id: sequence.experiment_id,
        experiment_assignment_id: sequence.experiment_assignment_id,
        signal_observation_ids: sequence.signal_observation_ids,
        touch_id: origin
            .as_ref()
            .map(|touch| touch.id.clone())
            .unwrap_or_default(),
        touch_stage: origin.as_ref().map(|touch| touch.stage).unwrap_or(0),
        contact_title: person.title.clone(),
        contact_vantage: person.vantage.clone(),
        account_hypothesis: lead.hypothesis.clone(),
        play_version: sequence.play_version,
        experiment_arm: sequence.experiment_arm,
        copy_policy_version: sequence.copy_policy_version,
        generation_backend: sequence.generation_backend,
        generation_model: sequence.generation_model,
        detail: format!(
            "No reply after {} sent touch(es); cadence stopped at the configured unanswered cap.",
            db.sequence_sent_count(&due_touch.sequence_id)?
        ),
        source: "cadence".into(),
        fingerprint: format!("nonresponse:{}", due_touch.sequence_id),
        ..Default::default()
    })?;
    Ok(())
}

async fn tick_conversation_replies(
    db: &SharedDb,
    playbooks: &Playbooks,
    businesses: &Businesses,
    compliance: &Compliance,
    cfg: &CadenceConfig,
    limit: i64,
    dry_run_reservations: &mut HashMap<String, usize>,
) -> Result<usize> {
    let mut sent = 0usize;
    for message in db.due_conversation_messages(limit)? {
        let Some(conversation) = db.get_conversation(&message.conversation_id)? else {
            if !cfg.dry_run {
                db.set_conversation_message_status(
                    &message.id,
                    "failed",
                    "",
                    "",
                    "conversation missing",
                )?;
            }
            continue;
        };
        let Some(person) = db.get_person(&conversation.person_id)? else {
            if !cfg.dry_run {
                db.set_conversation_message_status(
                    &message.id,
                    "failed",
                    "",
                    "",
                    "person missing",
                )?;
            }
            continue;
        };
        let lead = db.get_lead(&conversation.lead_id)?;
        let profile = businesses.get(&conversation.brand)?;
        let recipient = message.recipient_email.trim();
        if recipient.is_empty() {
            if !cfg.dry_run {
                db.set_conversation_message_status(
                    &message.id,
                    "failed",
                    "",
                    "",
                    "reply recipient missing",
                )?;
            }
            continue;
        }
        if db.is_suppressed(&conversation.brand, recipient)? {
            if !cfg.dry_run {
                db.set_conversation_message_status(
                    &message.id,
                    "cancelled",
                    "",
                    "",
                    "recipient suppressed",
                )?;
            }
            continue;
        }

        let timing = TimingContext {
            industry: lead
                .as_ref()
                .map(|lead| lead.industry.as_str())
                .unwrap_or(""),
            title: &person.title,
            vantage: &person.vantage,
            channel: "email",
            location: if person.location.is_empty() {
                lead.as_ref().map(|lead| lead.hq.as_str()).unwrap_or("")
            } else {
                &person.location
            },
            timezone: if person.timezone.is_empty() {
                lead.as_ref()
                    .map(|lead| lead.timezone.as_str())
                    .unwrap_or("")
            } else {
                &person.timezone
            },
            stable_key: &message.id,
        };
        let now = Utc::now();
        if !calendar::can_send_now(profile, &timing, now)? {
            continue;
        }
        let (quota_start, quota_end, quota_date) = calendar::quota_day_bounds(profile, now)?;
        if !has_business_capacity(
            db,
            profile,
            quota_start,
            quota_end,
            &quota_date,
            cfg.dry_run,
            dry_run_reservations,
        )? {
            continue;
        }
        let mailbox = if cfg.dry_run {
            db.preview_mailbox_on(&conversation.brand, &quota_date)?
        } else {
            db.pick_mailbox_on(&conversation.brand, &quota_date)?
        };
        let Some(mailbox) = mailbox else {
            continue;
        };
        let signature = playbooks
            .get(&conversation.brand)
            .map(|playbook| playbook.signature.clone())
            .unwrap_or_default();
        let body = compliance.render_body(&message.body, &signature, &mailbox.from_email);
        let in_reply_to = if message.in_reply_to.is_empty() {
            db.last_conversation_message_id(&conversation.id)?
        } else {
            message.in_reply_to.clone()
        };
        let outgoing = Outgoing {
            to: recipient.to_string(),
            subject: message.subject.clone(),
            body,
            list_unsubscribe: compliance.list_unsubscribe(
                &mailbox.from_email,
                &conversation.brand,
                &person.id,
            ),
            in_reply_to,
            references: message.references.clone(),
        };
        if !cfg.dry_run && !db.claim_conversation_message_for_send(&message.id)? {
            continue;
        }
        match send::send_email(&mailbox, &outgoing, cfg.dry_run).await {
            Ok(_) if cfg.dry_run => {
                eprintln!(
                    "  · would send [{} reply] to {} via {}",
                    conversation.brand, recipient, mailbox.from_email
                );
                sent += 1;
                reserve_dry_run(dry_run_reservations, &conversation.brand, &quota_date);
            }
            Ok(message_id) => {
                db.set_conversation_message_status(
                    &message.id,
                    "sent",
                    &mailbox.id,
                    &message_id,
                    "reply sent",
                )?;
                db.bump_mailbox_sent(&mailbox.id)?;
                db.log_event(
                    &conversation.brand,
                    &person.id,
                    &message.id,
                    "reply_sent",
                    &format!("conversation reply → {recipient}"),
                )?;
                sent += 1;
            }
            Err(error) => {
                db.set_conversation_message_status(
                    &message.id,
                    "failed",
                    &mailbox.id,
                    "",
                    &format!("send failed: {error:#}"),
                )?;
                db.log_event(
                    &conversation.brand,
                    &person.id,
                    &message.id,
                    "error",
                    &format!("conversation reply send failed: {error:#}"),
                )?;
            }
        }
        tokio::time::sleep(StdDuration::from_millis(jitter(
            cfg.send_delay_ms,
            &message.id,
        )))
        .await;
    }
    Ok(sent)
}

async fn tick_opportunity_outreach(
    db: &SharedDb,
    playbooks: &Playbooks,
    businesses: &Businesses,
    compliance: &Compliance,
    cfg: &CadenceConfig,
    limit: i64,
    dry_run_reservations: &mut HashMap<String, usize>,
) -> Result<usize> {
    let mut sent = 0usize;
    for touch in db.due_opportunity_touches(limit)? {
        let Some(contact) = db.get_opportunity_contact(&touch.contact_id)? else {
            if !cfg.dry_run {
                db.set_opportunity_touch_status(&touch.id, "skipped", "", "", "contact missing")?;
            }
            continue;
        };
        let Some(opportunity) = db.get_opportunity(&touch.opportunity_id)? else {
            if !cfg.dry_run {
                db.set_opportunity_touch_status(
                    &touch.id,
                    "skipped",
                    "",
                    "",
                    "opportunity missing",
                )?;
            }
            continue;
        };
        let profile = businesses.get(&touch.brand)?;
        if contact.email.trim().is_empty() || contact.email_status != "verified" {
            if !cfg.dry_run {
                db.set_opportunity_touch_status(
                    &touch.id,
                    "skipped",
                    "",
                    "",
                    "email not verified",
                )?;
            }
            continue;
        }
        if db.is_suppressed(&touch.brand, &contact.email)? {
            if !cfg.dry_run {
                db.set_opportunity_contact_status(&contact.id, "suppressed")?;
                db.stop_opportunity_outreach(&contact.id, "cancelled")?;
                db.log_event(
                    &touch.brand,
                    &format!("opportunity-contact:{}", contact.id),
                    &touch.id,
                    "suppressed",
                    "funding outreach skipped: suppressed",
                )?;
            }
            continue;
        }

        let timing = TimingContext {
            industry: "public funding",
            title: &contact.title,
            vantage: &contact.role,
            channel: "email",
            location: if contact.location.is_empty() {
                &opportunity.geography
            } else {
                &contact.location
            },
            timezone: if touch.recipient_timezone.is_empty() {
                &contact.timezone
            } else {
                &touch.recipient_timezone
            },
            stable_key: &touch.id,
        };
        let now = Utc::now();
        if !calendar::can_send_now(profile, &timing, now)? {
            let slot = calendar::next_slot_with_learning(db, profile, &timing, now)?;
            if !cfg.dry_run {
                db.reschedule_opportunity_touch(
                    &touch.id,
                    &slot.at.to_rfc3339(),
                    &slot.recipient_timezone,
                    &slot.rule,
                    &format!("deferred to recipient-local window: {}", slot.rationale),
                )?;
            }
            continue;
        }
        let (quota_start, quota_end, quota_date) = calendar::quota_day_bounds(profile, now)?;
        if !has_business_capacity(
            db,
            profile,
            quota_start,
            quota_end,
            &quota_date,
            cfg.dry_run,
            dry_run_reservations,
        )? {
            let slot = calendar::next_slot_with_learning(db, profile, &timing, quota_end)?;
            if !cfg.dry_run {
                db.reschedule_opportunity_touch(
                    &touch.id,
                    &slot.at.to_rfc3339(),
                    &slot.recipient_timezone,
                    &slot.rule,
                    &format!(
                        "deferred: {} daily touch cap reached; {}",
                        profile.calendar.daily_touch_cap, slot.rationale
                    ),
                )?;
            }
            continue;
        }

        let mailbox = if cfg.dry_run {
            db.preview_mailbox_on(&touch.brand, &quota_date)?
        } else {
            db.pick_mailbox_on(&touch.brand, &quota_date)?
        };
        let Some(mailbox) = mailbox else {
            if !cfg.dry_run {
                db.log_event(
                    &touch.brand,
                    &format!("opportunity-contact:{}", contact.id),
                    &touch.id,
                    "error",
                    "no mailbox with capacity for funding outreach",
                )?;
            }
            continue;
        };

        let signature = playbooks
            .get(&touch.brand)
            .map(|playbook| playbook.signature.clone())
            .unwrap_or_default();
        let body = compliance.render_body(&touch.body, &signature, &mailbox.from_email);
        let out = Outgoing {
            to: contact.email.clone(),
            subject: touch.subject.clone(),
            body,
            list_unsubscribe: compliance.list_unsubscribe(
                &mailbox.from_email,
                &touch.brand,
                &contact.id,
            ),
            in_reply_to: db.previous_opportunity_message_id(&contact.id, touch.stage)?,
            references: Vec::new(),
        };

        if !cfg.dry_run && !db.claim_opportunity_touch_for_send(&touch.id)? {
            continue;
        }

        match send::send_email(&mailbox, &out, cfg.dry_run).await {
            Ok(_) if cfg.dry_run => {
                eprintln!(
                    "  · would send [{} funding] stage {} to {} via {} — {}",
                    touch.brand, touch.stage, contact.email, mailbox.from_email, opportunity.title,
                );
                sent += 1;
                reserve_dry_run(dry_run_reservations, &touch.brand, &quota_date);
                continue;
            }
            Ok(message_id) => {
                db.set_opportunity_touch_status(&touch.id, "sent", &mailbox.id, &message_id, "")?;
                db.bump_mailbox_sent(&mailbox.id)?;
                db.set_opportunity_contact_status(&contact.id, "contacted")?;
                db.set_opportunity_pipeline_status(&opportunity.id, "contacting")?;
                db.log_event(
                    &touch.brand,
                    &format!("opportunity-contact:{}", contact.id),
                    &touch.id,
                    "funding_outreach_sent",
                    &format!(
                        "stage {} → {} — {}",
                        touch.stage, contact.email, opportunity.title
                    ),
                )?;
                sent += 1;
            }
            Err(e) => {
                db.set_opportunity_touch_status(
                    &touch.id,
                    "failed",
                    &mailbox.id,
                    "",
                    &format!("{e:#}"),
                )?;
                db.log_event(
                    &touch.brand,
                    &format!("opportunity-contact:{}", contact.id),
                    &touch.id,
                    "error",
                    &format!("funding send failed: {e}"),
                )?;
            }
        }
        tokio::time::sleep(StdDuration::from_millis(jitter(
            cfg.send_delay_ms,
            &touch.id,
        )))
        .await;
    }
    Ok(sent)
}

/// Preview one read-only pass in dry-run mode, or run live forever with one
/// [`tick`] per interval.
pub async fn run_daemon(
    db: SharedDb,
    playbooks: Arc<Playbooks>,
    businesses: Arc<Businesses>,
    compliance: Compliance,
    cfg: CadenceConfig,
) -> Result<()> {
    if cfg.dry_run {
        eprintln!(
            "\u{1F332} cadence preview — one pass, batch {}, DRY-RUN (no real mail)",
            cfg.batch
        );
        let previewed = tick(&db, &playbooks, &businesses, &compliance, &cfg).await?;
        eprintln!(
            "  · preview complete: {previewed} touch(es) would send; execution state unchanged"
        );
        return Ok(());
    }
    eprintln!(
        "\u{1F332} cadence daemon started — every {}s, batch {}, LIVE SENDING",
        cfg.interval_secs, cfg.batch
    );
    loop {
        match tick(&db, &playbooks, &businesses, &compliance, &cfg).await {
            Ok(n) if n > 0 => eprintln!("  · cadence sent {n} touch(es)"),
            Ok(_) => {}
            Err(e) => eprintln!("  ! cadence tick error: {e:#}"),
        }
        tokio::time::sleep(StdDuration::from_secs(cfg.interval_secs)).await;
    }
}

fn has_business_capacity(
    db: &SharedDb,
    profile: &BusinessProfile,
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
    date: &str,
    dry_run: bool,
    dry_run_reservations: &HashMap<String, usize>,
) -> Result<bool> {
    let sent = db.sent_touch_count_between(&profile.key, start, end)?;
    let previewed = if dry_run {
        dry_run_reservations
            .get(&reservation_key(&profile.key, date))
            .copied()
            .unwrap_or_default()
    } else {
        0
    };
    Ok(sent + previewed < profile.calendar.daily_touch_cap)
}

fn reserve_dry_run(reservations: &mut HashMap<String, usize>, brand: &str, date: &str) {
    *reservations
        .entry(reservation_key(brand, date))
        .or_default() += 1;
}

fn reservation_key(brand: &str, date: &str) -> String {
    format!("{brand}:{date}")
}

/// Whether opening a *new* front at this account is allowed right now. Returns
/// `None` if allowed, or `Some(reason)` to defer. In dry-run the reservation map
/// stands in for the opener sends a live pass would have persisted, so a single
/// counter feeds both limits: a freshly-opened front is at once a new opener
/// today and a newly-engaged person.
fn account_front_block_reason(
    db: &SharedDb,
    profile: &BusinessProfile,
    lead_id: &str,
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
    date: &str,
    reservations: Option<&HashMap<String, usize>>,
) -> Result<Option<String>> {
    let limits = &profile.account_limits;
    // `Some(map)` in dry-run, `None` live (where the DB already reflects sends).
    let reserved = reservations
        .and_then(|m| m.get(&account_reservation_key(lead_id, date)).copied())
        .unwrap_or_default();

    if limits.max_active_contacts_per_account > 0 {
        let engaged = db.account_engaged_people(lead_id)? + reserved;
        if engaged >= limits.max_active_contacts_per_account {
            return Ok(Some(format!(
                "{engaged} active contact(s) at account, max {}",
                limits.max_active_contacts_per_account
            )));
        }
    }
    if limits.max_new_contacts_per_account_per_day > 0 {
        let opened = db.account_openers_sent_between(lead_id, start, end)? + reserved;
        if opened >= limits.max_new_contacts_per_account_per_day {
            return Ok(Some(format!(
                "{opened} new contact(s) opened at account today, max {}/day",
                limits.max_new_contacts_per_account_per_day
            )));
        }
    }
    Ok(None)
}

fn reserve_account_opener(reservations: &mut HashMap<String, usize>, lead_id: &str, date: &str) {
    *reservations
        .entry(account_reservation_key(lead_id, date))
        .or_default() += 1;
}

fn account_reservation_key(lead_id: &str, date: &str) -> String {
    format!("{lead_id}:{date}")
}

/// Deterministic per-touch jitter in [base, base*1.6): spaces sends out without
/// needing an RNG dependency (Math.random-free), varying by touch id + clock.
fn jitter(base_ms: u64, seed: &str) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let s: u64 = seed.bytes().map(|b| b as u64).sum();
    base_ms + ((nanos ^ s) % base_ms.max(1) * 6 / 10)
}
