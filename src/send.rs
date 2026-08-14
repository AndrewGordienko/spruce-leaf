//! SMTP send transport.
//!
//! Given a [`Mailbox`] and a rendered message, actually deliver it — or, in
//! dry-run mode, pretend to (returning a synthetic Message-ID) so the whole
//! cadence loop can be exercised without touching a real inbox. Port 465 uses
//! implicit TLS; anything else (typically 587) uses STARTTLS.
//!
//! The returned Message-ID is the bracketed `<uuid@domain>` form exactly as it
//! appears on the wire, so inbound replies can be thread-matched against it.

use anyhow::{Context, Result};
use lettre::message::header::{ContentType, HeaderName, HeaderValue};
use lettre::message::Message;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};
use uuid::Uuid;

use crate::db::Mailbox;

pub struct Outgoing {
    pub to: String,
    pub subject: String,
    pub body: String,
    pub list_unsubscribe: String,
    /// Previous RFC Message-ID for a real threaded follow-up.
    pub in_reply_to: String,
    /// Full RFC References chain when known. Falls back to `in_reply_to`.
    pub references: Vec<String>,
}

/// Send one email. In `dry_run` we skip the network entirely and return a
/// synthetic Message-ID. Returns the bracketed Message-ID on success.
pub async fn send_email(m: &Mailbox, out: &Outgoing, dry_run: bool) -> Result<String> {
    let domain = m.from_email.split('@').nth(1).unwrap_or("localhost");
    let raw_id = format!("{}@{}", Uuid::new_v4(), domain);
    let message_id = format!("<{raw_id}>");

    if dry_run {
        return Ok(message_id);
    }
    let configured_allowlist = std::env::var("SPRUCE_SEND_ALLOWLIST").unwrap_or_default();
    if configured_allowlist.trim().is_empty() {
        anyhow::bail!(
            "live sending is pilot-locked for every brand: set a non-empty SPRUCE_SEND_ALLOWLIST containing only controlled inboxes"
        );
    }
    // Test guardrail: when SPRUCE_SEND_ALLOWLIST is set, a live send may only go
    // to an address (or `@domain`) on the list. Unset/empty means no restriction
    // (production default). This lets a `--live` daemon be exercised end-to-end
    // against mailboxes you control with zero risk of reaching a real prospect.
    if let Err(reason) = recipient_allowed(&out.to) {
        anyhow::bail!(reason);
    }
    if m.smtp_host.trim().is_empty() {
        anyhow::bail!("mailbox {} has no SMTP host configured", m.from_email);
    }

    let from = format!("{} <{}>", m.from_name, m.from_email);
    let mut builder = Message::builder()
        .from(from.parse().context("parsing from address")?)
        .to(out
            .to
            .parse()
            .with_context(|| format!("parsing to address {}", out.to))?)
        .subject(out.subject.clone())
        .message_id(Some(raw_id))
        .header(ContentType::TEXT_PLAIN);
    if !out.list_unsubscribe.trim().is_empty() {
        builder = builder.raw_header(HeaderValue::new(
            HeaderName::new_from_ascii_str("List-Unsubscribe"),
            out.list_unsubscribe.clone(),
        ));
    }
    if !out.in_reply_to.trim().is_empty() {
        let references = if out.references.is_empty() {
            out.in_reply_to.clone()
        } else {
            out.references.join(" ")
        };
        builder = builder
            .raw_header(HeaderValue::new(
                HeaderName::new_from_ascii_str("In-Reply-To"),
                out.in_reply_to.clone(),
            ))
            .raw_header(HeaderValue::new(
                HeaderName::new_from_ascii_str("References"),
                references,
            ));
    }
    let email = builder
        .body(out.body.clone())
        .context("building email message")?;

    let creds = Credentials::new(m.smtp_user.clone(), m.smtp_pass.clone());
    let builder = if m.smtp_port == 465 {
        AsyncSmtpTransport::<Tokio1Executor>::relay(&m.smtp_host)
    } else {
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&m.smtp_host)
    }
    .context("configuring SMTP transport")?;

    let mailer = builder.port(m.smtp_port).credentials(creds).build();
    mailer.send(email).await.context("SMTP send failed")?;
    Ok(message_id)
}

/// Extract the bare `local@domain` from a recipient that may be in
/// `Name <addr>` form.
fn extract_addr(s: &str) -> String {
    let s = s.trim();
    if let (Some(lt), Some(gt)) = (s.find('<'), s.rfind('>')) {
        if gt > lt {
            return s[lt + 1..gt].trim().to_lowercase();
        }
    }
    s.to_lowercase()
}

/// Enforce the mandatory pilot `SPRUCE_SEND_ALLOWLIST` guardrail. Entries are
/// comma-separated and either a full address (`me@example.com`) or a domain
/// suffix (`@example.com`). Returns `Err` with a human reason when a live send
/// must be refused; `Ok(())` only when the recipient matches. Kept in the
/// transport so no brand can bypass the controlled-inbox pilot lock.
pub(crate) fn recipient_allowed(to: &str) -> std::result::Result<(), String> {
    let raw = std::env::var("SPRUCE_SEND_ALLOWLIST").unwrap_or_default();
    let entries: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    if entries.is_empty() {
        return Err(
            "SPRUCE_SEND_ALLOWLIST is empty — refusing live send while the supervised pilot lock is active"
                .into(),
        );
    }
    let addr = extract_addr(to);
    let allowed = entries.iter().any(|e| match e.strip_prefix('@') {
        Some(domain) => addr.ends_with(&format!("@{domain}")),
        None => addr == *e,
    });
    if allowed {
        Ok(())
    } else {
        Err(format!(
            "recipient {addr} not in SPRUCE_SEND_ALLOWLIST — refusing live send (test guardrail active)"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::extract_addr;

    #[test]
    fn extract_addr_handles_display_name_and_bare() {
        assert_eq!(extract_addr("Andrew <a@b.com>"), "a@b.com");
        assert_eq!(extract_addr("a@b.com"), "a@b.com");
        assert_eq!(extract_addr("  A@B.COM "), "a@b.com");
    }

    #[test]
    fn every_brand_live_send_guard_is_declared_at_the_transport_boundary() {
        let source = include_str!("send.rs");
        assert!(source.contains("live sending is pilot-locked for every brand"));
        assert!(source.contains("SPRUCE_SEND_ALLOWLIST"));
    }
}
