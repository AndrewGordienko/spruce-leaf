//! Compliance: the legal + deliverability guardrails on every send.
//!
//! Cold outreach in Canada (CASL) and the US (CAN-SPAM) requires that every
//! message identify the sender, give a real physical address, and offer a clear
//! way to opt out — and that opt-outs are honored. This module renders that
//! footer onto every body, produces the unsubscribe token/List-Unsubscribe
//! value, and enforces the sending window so we only email during business hours.
//!
//! Config comes from env:
//!   COMPLIANCE_ADDRESS   physical mailing address (required for a real send)
//!   BUSINESS_TZ_OFFSET   hours offset from UTC for the sending window (default -5)

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use chrono::{Datelike, Timelike, Utc};

pub struct Compliance {
    pub physical_address: String,
    pub tz_offset_hours: i32,
    pub window_start: u32,
    pub window_end: u32,
}

impl Compliance {
    pub fn from_env() -> Self {
        Compliance {
            physical_address: std::env::var("COMPLIANCE_ADDRESS").unwrap_or_default(),
            tz_offset_hours: std::env::var("BUSINESS_TZ_OFFSET")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(-5),
            window_start: 8,
            window_end: 17,
        }
    }

    /// True when we may send right now: a weekday, within business hours in the
    /// configured timezone. Keeps the daemon from emailing at 3am or on Sunday.
    pub fn within_window(&self) -> bool {
        let local = Utc::now() + chrono::Duration::hours(self.tz_offset_hours as i64);
        let weekday = local.weekday().num_days_from_monday(); // 0=Mon .. 6=Sun
        let hour = local.hour();
        weekday < 5 && hour >= self.window_start && hour < self.window_end
    }

    /// A stable, unguessable-enough opt-out token for a recipient.
    pub fn unsub_token(&self, brand: &str, person_id: &str) -> String {
        let mut h = DefaultHasher::new();
        format!("{brand}:{person_id}:spruce-unsub").hash(&mut h);
        format!("{:016x}", h.finish())
    }

    /// The `List-Unsubscribe` header value (a mailto the recipient can one-click).
    pub fn list_unsubscribe(&self, from_email: &str, brand: &str, person_id: &str) -> String {
        let token = self.unsub_token(brand, person_id);
        format!("<mailto:{from_email}?subject=unsubscribe-{token}>")
    }

    /// Append the compliance footer to a raw body: signature, opt-out line, and
    /// the mandatory physical address. Returns the full sendable body.
    pub fn render_body(&self, raw: &str, signature: &str, from_email: &str) -> String {
        let mut out = raw.trim_end().to_string();
        out.push_str("\n\n");
        out.push_str(signature.trim());
        out.push_str("\n\n—\n");
        out.push_str(&format!(
            "Not the right person, or prefer not to hear from me? Just reply \"unsubscribe\" \
             to {from_email} and I'll remove you immediately."
        ));
        if !self.physical_address.trim().is_empty() {
            out.push('\n');
            out.push_str(self.physical_address.trim());
        }
        out
    }

    /// Does a reply body look like an opt-out request?
    pub fn is_optout(text: &str) -> bool {
        let t = text.to_lowercase();
        ["unsubscribe", "opt out", "opt-out", "remove me", "take me off", "stop emailing", "no longer"]
            .iter()
            .any(|p| t.contains(p))
    }
}
