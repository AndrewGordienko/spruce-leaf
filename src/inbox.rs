//! Inbound: poll each brand's IMAP inbox and route replies into triage.
//!
//! The `imap` crate is synchronous, so the network work runs on a blocking task:
//! open the mailbox, `SEARCH UNSEEN`, `FETCH BODY.PEEK[]` (peek so we don't set
//! \Seen until we've actually handled a message), then parse + match + triage on
//! the async side. Matched/handled messages are flagged \Seen at the end.
//!
//! Matching is by From-address against known people for the brand. Messages from
//! a mailer-daemon are treated as bounces: any of our recipients found in the
//! body gets suppressed and its sequence stopped.

use anyhow::Result;
use mail_parser::MessageParser;

use crate::db::{Mailbox, SharedDb};
use crate::engine::Engine;
use crate::triage;

/// Poll every configured mailbox (optionally one brand). Returns replies handled.
pub async fn poll_all(db: &SharedDb, client: &Engine, brand: Option<&str>) -> Result<usize> {
    let mut handled = 0;
    for m in db.list_mailboxes(brand)? {
        if m.imap_host.trim().is_empty() {
            continue;
        }
        match poll_mailbox(db, client, &m).await {
            Ok(n) => handled += n,
            Err(e) => eprintln!("  ! inbox {}: {e:#}", m.from_email),
        }
    }
    Ok(handled)
}

async fn poll_mailbox(db: &SharedDb, client: &Engine, m: &Mailbox) -> Result<usize> {
    let fetched = fetch_unseen(m.clone()).await?;
    let mut handled = 0;
    let mut done_uids: Vec<u32> = Vec::new();

    for (uid, raw) in fetched {
        let Some(parsed) = MessageParser::default().parse(&raw) else {
            continue;
        };
        let from = parsed
            .from()
            .and_then(|a| a.first())
            .and_then(|a| a.address.as_deref())
            .unwrap_or("")
            .to_lowercase();
        let subject = parsed.subject().unwrap_or("").to_string();
        let body = parsed
            .body_text(0)
            .map(|c| c.to_string())
            .unwrap_or_default();
        let message_id = parsed.message_id().unwrap_or("").to_string();
        let in_reply_to = parsed.in_reply_to().as_text().unwrap_or("").to_string();

        if from.is_empty() {
            continue;
        }

        if let Some(person) = db.person_by_email(&m.brand, &from)? {
            let action = triage::handle_reply(
                db,
                client,
                &person,
                &from,
                &subject,
                &body,
                &message_id,
                &in_reply_to,
            )
            .await
            .unwrap_or_else(|e| format!("error: {e}"));
            eprintln!("  · [{}] reply from {from} → {action}", m.brand);
            handled += 1;
            done_uids.push(uid);
        } else if let Some(contact) = db.opportunity_contact_by_email(&m.brand, &from)? {
            let action = triage::handle_opportunity_reply(
                db,
                client,
                &contact,
                &from,
                &subject,
                &body,
                &message_id,
                &in_reply_to,
            )
            .await
            .unwrap_or_else(|e| format!("error: {e}"));
            eprintln!("  · [{}] opportunity reply from {from} → {action}", m.brand);
            handled += 1;
            done_uids.push(uid);
        } else if is_bounce(&from, &subject) {
            let n = handle_bounce(db, &m.brand, &body)?;
            if n > 0 {
                eprintln!("  · [{}] bounce → suppressed {n} address(es)", m.brand);
                handled += n;
            }
            done_uids.push(uid);
        }
    }

    if !done_uids.is_empty() {
        let _ = mark_seen(m.clone(), done_uids).await;
    }
    Ok(handled)
}

/// Suppress any of our recipients that appear in a bounce body.
fn handle_bounce(db: &SharedDb, brand: &str, body: &str) -> Result<usize> {
    let body_lc = body.to_lowercase();
    let mut n = 0;
    for p in db.list_people(Some(brand), None)? {
        if p.email.is_empty() {
            continue;
        }
        if body_lc.contains(&p.email.to_lowercase()) {
            db.add_suppression(brand, &p.email, "bounced")?;
            db.set_person_status(&p.id, "bounced")?;
            if let Some(seq) = db.active_sequence_for_person(&p.id)? {
                db.stop_sequence(&seq, "stopped", "cancelled")?;
            }
            db.log_event(brand, &p.id, "", "bounced", "hard bounce")?;
            n += 1;
        }
    }
    for contact in db.list_opportunity_contacts_for_brand(brand)? {
        if contact.email.is_empty() {
            continue;
        }
        if body_lc.contains(&contact.email.to_lowercase()) {
            db.add_suppression(brand, &contact.email, "bounced")?;
            db.set_opportunity_contact_status(&contact.id, "bounced")?;
            db.stop_opportunity_outreach(&contact.id, "cancelled")?;
            db.log_event(
                brand,
                &format!("opportunity-contact:{}", contact.id),
                "",
                "funding_bounced",
                "hard bounce",
            )?;
            n += 1;
        }
    }
    Ok(n)
}

fn is_bounce(from: &str, subject: &str) -> bool {
    let f = from.to_lowercase();
    let s = subject.to_lowercase();
    f.contains("mailer-daemon")
        || f.contains("postmaster")
        || s.contains("undeliverable")
        || s.contains("delivery status")
        || s.contains("delivery failure")
        || s.contains("returned mail")
}

// --- blocking IMAP work ----------------------------------------------------

async fn fetch_unseen(m: Mailbox) -> Result<Vec<(u32, Vec<u8>)>> {
    tokio::task::spawn_blocking(move || -> Result<Vec<(u32, Vec<u8>)>> {
        let tls = native_tls::TlsConnector::builder().build()?;
        let client = imap::connect(
            (m.imap_host.as_str(), m.imap_port),
            m.imap_host.as_str(),
            &tls,
        )?;
        let mut session = client
            .login(&m.smtp_user, &m.smtp_pass)
            .map_err(|(e, _)| anyhow::anyhow!("IMAP login failed for {}: {e}", m.from_email))?;
        session.select("INBOX")?;
        let uids = session.uid_search("UNSEEN")?;
        let mut out = Vec::new();
        if !uids.is_empty() {
            let set = uids
                .iter()
                .map(|u| u.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let msgs = session.uid_fetch(set, "(UID BODY.PEEK[])")?;
            for msg in msgs.iter() {
                if let (Some(uid), Some(body)) = (msg.uid, msg.body()) {
                    out.push((uid, body.to_vec()));
                }
            }
        }
        let _ = session.logout();
        Ok(out)
    })
    .await?
}

async fn mark_seen(m: Mailbox, uids: Vec<u32>) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
        let tls = native_tls::TlsConnector::builder().build()?;
        let client = imap::connect(
            (m.imap_host.as_str(), m.imap_port),
            m.imap_host.as_str(),
            &tls,
        )?;
        let mut session = client
            .login(&m.smtp_user, &m.smtp_pass)
            .map_err(|(e, _)| anyhow::anyhow!("IMAP login failed: {e}"))?;
        session.select("INBOX")?;
        let set = uids
            .iter()
            .map(|u| u.to_string())
            .collect::<Vec<_>>()
            .join(",");
        session.uid_store(set, "+FLAGS (\\Seen)")?;
        let _ = session.logout();
        Ok(())
    })
    .await?
}
