//! The cadence engine — the clock that makes this an SDR instead of a drafter.
//!
//! A restart-safe loop: wake on an interval, find every touch that is `scheduled`
//! and due, and — for each, only if the recipient is verified, not suppressed,
//! and a mailbox has daily headroom — render the compliant body and send it,
//! advancing the touch and person state and logging an event. All state lives in
//! SQLite, so a crash/restart resumes exactly where it left off.
//!
//! Safety rails baked in:
//!   * sending window (business hours only, via [`Compliance`])
//!   * per-mailbox daily caps ([`Db::pick_mailbox`])
//!   * suppression checked immediately before every send
//!   * `dry_run` sends nothing (returns synthetic Message-IDs) so the full loop
//!     can be exercised safely — this is the DEFAULT.

use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use anyhow::Result;

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
            send_delay_ms: std::env::var("CADENCE_SEND_DELAY_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(1500),
            interval_secs: 60,
        }
    }
}

/// Run a single cadence pass. Returns how many touches were actually sent.
pub async fn tick(
    db: &SharedDb,
    playbooks: &Playbooks,
    compliance: &Compliance,
    cfg: &CadenceConfig,
) -> Result<usize> {
    if !compliance.within_window() {
        return Ok(0);
    }

    let due = db.due_touches(None, cfg.batch)?;
    let mut sent = 0usize;

    for touch in due {
        let Some(person) = db.get_person(&touch.person_id)? else {
            db.set_touch_status(&touch.id, "skipped", "", "", "person missing")?;
            continue;
        };

        // Only email real, verified, still-engageable recipients.
        if !touch.channel.eq_ignore_ascii_case("email") {
            continue;
        }
        if person.email.trim().is_empty() || person.email_status != "verified" {
            db.set_touch_status(&touch.id, "skipped", "", "", "email not verified")?;
            continue;
        }
        if db.is_suppressed(&touch.brand, &person.email)? {
            db.set_person_status(&person.id, "suppressed")?;
            if let Some(seq) = db.active_sequence_for_person(&person.id)? {
                db.stop_sequence(&seq, "stopped", "cancelled")?;
            }
            db.log_event(&touch.brand, &person.id, &touch.id, "suppressed", "skipped: suppressed")?;
            continue;
        }

        // A configured mailbox with daily headroom, else stop for this brand.
        let Some(mailbox) = db.pick_mailbox(&touch.brand)? else {
            db.log_event(&touch.brand, &person.id, &touch.id, "error", "no mailbox with capacity")?;
            continue;
        };

        let signature = playbooks.get(&touch.brand).map(|p| p.signature.clone()).unwrap_or_default();
        let body = compliance.render_body(&touch.body, &signature, &mailbox.from_email);
        let out = Outgoing { to: person.email.clone(), subject: touch.subject.clone(), body };

        match send::send_email(&mailbox, &out, cfg.dry_run).await {
            Ok(message_id) => {
                db.set_touch_status(&touch.id, "sent", &mailbox.id, &message_id, "")?;
                db.bump_mailbox_sent(&mailbox.id)?;
                db.set_person_status(&person.id, "contacted")?;
                db.log_event(
                    &touch.brand,
                    &person.id,
                    &touch.id,
                    "sent",
                    &format!("stage {} → {}{}", touch.stage, person.email, if cfg.dry_run { " [dry-run]" } else { "" }),
                )?;
                sent += 1;
            }
            Err(e) => {
                db.set_touch_status(&touch.id, "failed", &mailbox.id, "", &format!("{e:#}"))?;
                db.log_event(&touch.brand, &person.id, &touch.id, "error", &format!("send failed: {e}"))?;
            }
        }

        // Gentle, slightly-varied pacing between sends.
        tokio::time::sleep(StdDuration::from_millis(jitter(cfg.send_delay_ms, &touch.id))).await;
    }

    Ok(sent)
}

/// Run the cadence loop forever, one [`tick`] per interval.
pub async fn run_daemon(
    db: SharedDb,
    playbooks: Arc<Playbooks>,
    compliance: Compliance,
    cfg: CadenceConfig,
) -> Result<()> {
    eprintln!(
        "\u{1F332} cadence daemon started — every {}s, batch {}, {}",
        cfg.interval_secs,
        cfg.batch,
        if cfg.dry_run { "DRY-RUN (no real mail)" } else { "LIVE SENDING" }
    );
    loop {
        match tick(&db, &playbooks, &compliance, &cfg).await {
            Ok(n) if n > 0 => eprintln!("  · cadence sent {n} touch(es)"),
            Ok(_) => {}
            Err(e) => eprintln!("  ! cadence tick error: {e:#}"),
        }
        tokio::time::sleep(StdDuration::from_secs(cfg.interval_secs)).await;
    }
}

/// Deterministic per-touch jitter in [base, base*1.6): spaces sends out without
/// needing an RNG dependency (Math.random-free), varying by touch id + clock.
fn jitter(base_ms: u64, seed: &str) -> u64 {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.subsec_nanos() as u64).unwrap_or(0);
    let s: u64 = seed.bytes().map(|b| b as u64).sum();
    base_ms + ((nanos ^ s) % base_ms.max(1) * 6 / 10)
}
