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
        builder = builder
            .raw_header(HeaderValue::new(
                HeaderName::new_from_ascii_str("In-Reply-To"),
                out.in_reply_to.clone(),
            ))
            .raw_header(HeaderValue::new(
                HeaderName::new_from_ascii_str("References"),
                out.in_reply_to.clone(),
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
