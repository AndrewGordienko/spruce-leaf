//! Compliance: the legal + deliverability guardrails on every send.
//!
//! Cold outreach in Canada (CASL) and the US (CAN-SPAM) requires that every
//! message identify the sender, give a real physical address, and offer a clear
//! way to opt out — and that opt-outs are honored. This module renders that
//! footer onto every body and produces the unsubscribe token/List-Unsubscribe
//! value. Recipient-local timing lives in `calendar.rs`.
//!
//! Config comes from env:
//!   COMPLIANCE_ADDRESS   physical mailing address (required for a real send)

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::playbook;

pub struct Compliance {
    pub physical_address: String,
}

impl Compliance {
    pub fn from_env() -> Self {
        Compliance {
            physical_address: std::env::var("COMPLIANCE_ADDRESS").unwrap_or_default(),
        }
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
        // Drafts are normalized earlier, but enforce again at the last possible
        // moment so old/imported rows cannot produce a duplicate or stale name.
        let mut out = playbook::enforce_signature(raw, signature);
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
        [
            "unsubscribe",
            "opt out",
            "opt-out",
            "remove me",
            "take me off",
            "stop emailing",
            "no longer",
        ]
        .iter()
        .any(|p| t.contains(p))
    }
}
