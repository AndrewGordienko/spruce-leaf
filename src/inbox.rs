//! Inbound: poll each brand's IMAP inbox and route replies into triage.
//!
//! The `imap` crate is synchronous, so the network work runs on a blocking task:
//! open the mailbox, `SEARCH UNSEEN`, `FETCH BODY.PEEK[]` (peek so we don't set
//! \Seen until we've actually handled a message), then parse + match + triage on
//! the async side. Matched/handled messages are flagged \Seen at the end.
//!
//! Matching prefers RFC `In-Reply-To`/`References` identity, then falls back to
//! From-address. That keeps a CC'd referral attached to the original account
//! even when the new sender has never appeared in Apollo. Messages from a
//! mailer-daemon are treated as bounces.

use anyhow::Result;
use mail_parser::MessageParser;

use crate::db::{Mailbox, SharedDb};
use crate::engine::Engine;
use crate::playbook::Playbooks;
use crate::reply_agent::{self, InboundMessage};
use crate::triage;

/// Poll every configured mailbox (optionally one brand). Returns replies handled.
///
/// Prefer Gmail OAuth sync when a brand is linked via `/login` (no IMAP needed).
/// Brands that still have classic IMAP credentials keep the legacy path.
pub async fn poll_all(
    db: &SharedDb,
    client: &Engine,
    playbooks: &Playbooks,
    brand: Option<&str>,
    allow_booking: bool,
) -> Result<usize> {
    let mut handled = 0;

    // 1) Gmail API path for OAuth-linked brands (browser /login).
    let gmail_brands: Vec<String> = match brand {
        Some(b) if crate::google_oauth::GoogleTokenSet::is_logged_in(b) => vec![b.to_string()],
        Some(_) => Vec::new(),
        None => crate::gmail::list_logged_in_brands(),
    };
    if !gmail_brands.is_empty() {
        for b in &gmail_brands {
            match crate::gmail::sync_brand(db, b, 30).await {
                Ok(summary) => {
                    eprintln!(
                        "  · gmail {}: inbox {} · sent {} · matched {}/{}",
                        b,
                        summary.inbox_scanned,
                        summary.sent_scanned,
                        summary.matched_inbound,
                        summary.matched_outbound
                    );
                    handled += summary.matched_inbound;
                }
                Err(e) => eprintln!("  ! gmail {b}: {e:#}"),
            }
        }
    }

    // 2) Legacy IMAP for mailboxes that still have host/password config.
    for m in db.list_mailboxes(brand)? {
        if m.imap_host.trim().is_empty() {
            continue;
        }
        // Skip IMAP when this brand is already covered by Gmail OAuth.
        if crate::google_oauth::GoogleTokenSet::is_logged_in(&m.brand) {
            continue;
        }
        match poll_mailbox(db, client, playbooks, &m, allow_booking).await {
            Ok(n) => handled += n,
            Err(e) => eprintln!("  ! inbox {}: {e:#}", m.from_email),
        }
    }
    Ok(handled)
}

async fn poll_mailbox(
    db: &SharedDb,
    client: &Engine,
    playbooks: &Playbooks,
    m: &Mailbox,
    allow_booking: bool,
) -> Result<usize> {
    let fetched = fetch_unseen(m.clone()).await?;
    let mut handled = 0;
    let mut done_uids: Vec<u32> = Vec::new();

    for (uid, raw) in fetched {
        let Some(parsed) = MessageParser::default().parse(&raw) else {
            continue;
        };
        let from_address = parsed.from().and_then(|a| a.first());
        let from = from_address
            .and_then(|address| address.address.as_deref())
            .unwrap_or("")
            .to_lowercase();
        let from_name = from_address
            .and_then(|address| address.name.as_deref())
            .unwrap_or("")
            .to_string();
        let subject = parsed.subject().unwrap_or("").to_string();
        let body = parsed
            .body_text(0)
            .map(|c| c.to_string())
            .unwrap_or_default();
        let message_id = parsed.message_id().unwrap_or("").to_string();
        let in_reply_to_values = parsed
            .in_reply_to()
            .as_text_list()
            .unwrap_or_default()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let in_reply_to = in_reply_to_values.last().cloned().unwrap_or_default();
        let references = parsed
            .references()
            .as_text_list()
            .unwrap_or_default()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let mut thread_ids = in_reply_to_values;
        thread_ids.extend(references.iter().cloned());
        let mut participants = addresses(parsed.to());
        participants.extend(addresses(parsed.cc()));
        participants.push(from.clone());
        participants.sort();
        participants.dedup();

        if from.is_empty() {
            continue;
        }

        if is_bounce(&from, &subject) {
            let n = handle_bounce(db, &m.brand, &body)?;
            if n > 0 {
                eprintln!("  · [{}] bounce → suppressed {n} address(es)", m.brand);
                handled += n;
            }
            done_uids.push(uid);
        } else if let Some(conversation) =
            db.conversation_for_inbound(&m.brand, &from, &subject, &thread_ids)?
        {
            let Some(person) = db.get_person(&conversation.person_id)? else {
                eprintln!(
                    "  ! [{}] thread {} has no original person {}",
                    m.brand, conversation.id, conversation.person_id
                );
                continue;
            };
            let outcome = reply_agent::handle_inbound(
                db,
                client,
                playbooks,
                &conversation,
                &person,
                &InboundMessage {
                    from_email: from.clone(),
                    from_name,
                    participants,
                    subject: subject.clone(),
                    body: body.clone(),
                    message_id: message_id.clone(),
                    in_reply_to: in_reply_to.clone(),
                    references,
                },
                allow_booking,
            )
            .await;
            match outcome {
                Ok(outcome) => {
                    eprintln!("  · [{}] reply from {from} → {}", m.brand, outcome.action);
                    handled += 1;
                    done_uids.push(uid);
                }
                Err(error) => {
                    // Leave it unseen so a transient model/calendar failure is
                    // retried instead of losing a human reply.
                    eprintln!("  ! [{}] reply from {from}: {error:#}", m.brand);
                }
            }
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
        }
    }

    if !done_uids.is_empty() {
        let _ = mark_seen(m.clone(), done_uids).await;
    }
    Ok(handled)
}

fn addresses(addresses: Option<&mail_parser::Address<'_>>) -> Vec<String> {
    addresses
        .into_iter()
        .flat_map(|addresses| addresses.iter())
        .filter_map(|address| address.address.as_deref())
        .map(|address| address.to_lowercase())
        .collect()
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
