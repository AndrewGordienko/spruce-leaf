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
use crate::db::SharedDb;
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
            batch: 25,
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
    let due = db.due_touches(None, cfg.batch)?;
    let mut sent = 0usize;
    let mut dry_run_reservations = HashMap::<String, usize>::new();

    for touch in due {
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
        let profile = businesses.get(&touch.brand)?;

        // Only email real, verified, still-engageable recipients.
        if !touch.channel.eq_ignore_ascii_case("email") {
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
            let slot = calendar::next_slot(profile, &timing, now)?;
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
            let slot = calendar::next_slot(profile, &timing, quota_end)?;
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
        };

        match send::send_email(&mailbox, &out, cfg.dry_run).await {
            Ok(_) if cfg.dry_run => {
                eprintln!(
                    "  · would send [{}] stage {} to {} via {}",
                    touch.brand, touch.stage, person.email, mailbox.from_email
                );
                sent += 1;
                reserve_dry_run(&mut dry_run_reservations, &touch.brand, &quota_date);
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
            let slot = calendar::next_slot(profile, &timing, now)?;
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
            let slot = calendar::next_slot(profile, &timing, quota_end)?;
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
        };

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
