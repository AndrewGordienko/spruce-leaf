//! Gmail API client + inbox/sent sync into the execution spine.
//!
//! Authenticated brands (via [`crate::google_oauth`]) can pull recent SENT and
//! INBOX mail without IMAP/App Passwords. Matched threads land in conversations;
//! unmatched patterns and reply outcomes feed the learnings tree so the SDR
//! knows what is getting traction.

use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use reqwest::Client;
use serde_json::Value;

use crate::db::{ConversationMessage, SharedDb};
use crate::google_oauth::{self, GoogleTokenSet};

#[derive(Debug, Default)]
pub struct SyncSummary {
    pub brand: String,
    pub email: String,
    pub inbox_scanned: usize,
    pub sent_scanned: usize,
    pub matched_inbound: usize,
    pub matched_outbound: usize,
    pub learnings_recorded: usize,
    pub errors: usize,
}

/// Sync recent INBOX + SENT for every logged-in brand (or one brand).
pub async fn sync_all(
    db: &SharedDb,
    brand: Option<&str>,
    max_per_label: usize,
) -> Result<Vec<SyncSummary>> {
    let brands: Vec<String> = match brand {
        Some(b) => vec![b.to_string()],
        None => list_logged_in_brands(),
    };
    if brands.is_empty() {
        bail!("no Gmail accounts linked — run /login gnk (and wapahki, outagehub)");
    }
    let mut out = Vec::new();
    for b in brands {
        match sync_brand(db, &b, max_per_label).await {
            Ok(s) => out.push(s),
            Err(e) => {
                eprintln!("  ! gmail sync {b}: {e:#}");
                out.push(SyncSummary {
                    brand: b,
                    errors: 1,
                    ..Default::default()
                });
            }
        }
    }
    Ok(out)
}

pub fn list_logged_in_brands() -> Vec<String> {
    let dir = std::path::Path::new(".spruce/google");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut brands = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            if GoogleTokenSet::is_logged_in(stem) {
                brands.push(stem.to_string());
            }
        }
    }
    brands.sort();
    brands
}

/// Pull recent mail for one brand and fold it into conversations + learnings.
pub async fn sync_brand(db: &SharedDb, brand: &str, max_per_label: usize) -> Result<SyncSummary> {
    let (token, set) = google_oauth::access_token_for(brand).await?;
    let client = GmailClient {
        http: Client::new(),
        token,
    };
    let mut summary = SyncSummary {
        brand: brand.to_string(),
        email: set.email.clone(),
        ..Default::default()
    };

    // Ensure the brand has a mailbox row so CRM/daemon can see the linked identity.
    ensure_oauth_mailbox(db, brand, &set.email)?;

    let max = max_per_label.clamp(5, 100);
    let inbox = client.list_messages("in:inbox", max).await?;
    summary.inbox_scanned = inbox.len();
    for id in inbox {
        match client.get_message(&id).await {
            Ok(msg) => {
                if ingest_message(db, brand, &set.email, &msg, "inbound").await? {
                    summary.matched_inbound += 1;
                }
            }
            Err(e) => {
                summary.errors += 1;
                eprintln!("  · ! inbox fetch {id}: {e:#}");
            }
        }
    }

    let sent = client.list_messages("in:sent", max).await?;
    summary.sent_scanned = sent.len();
    for id in sent {
        match client.get_message(&id).await {
            Ok(msg) => {
                if ingest_message(db, brand, &set.email, &msg, "outbound").await? {
                    summary.matched_outbound += 1;
                }
            }
            Err(e) => {
                summary.errors += 1;
                eprintln!("  · ! sent fetch {id}: {e:#}");
            }
        }
    }

    summary.learnings_recorded = distill_learnings(db, brand)?;
    db.log_event(
        brand,
        "",
        "",
        "gmail_sync",
        &format!(
            "inbox={} sent={} matched_in={} matched_out={} learnings={}",
            summary.inbox_scanned,
            summary.sent_scanned,
            summary.matched_inbound,
            summary.matched_outbound,
            summary.learnings_recorded
        ),
    )?;
    Ok(summary)
}

struct GmailClient {
    http: Client,
    token: String,
}

#[derive(Debug, Clone)]
struct GmailMessage {
    id: String,
    thread_id: String,
    message_id: String,
    in_reply_to: String,
    references: Vec<String>,
    from: String,
    to: Vec<String>,
    subject: String,
    body: String,
    internal_date_ms: i64,
    label_ids: Vec<String>,
}

impl GmailClient {
    async fn list_messages(&self, query: &str, max: usize) -> Result<Vec<String>> {
        let url = format!(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults={max}&q={}",
            urlencoding_lite(query)
        );
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("listing Gmail messages")?;
        let status = response.status();
        let body: Value = response.json().await.context("decoding Gmail list")?;
        if !status.is_success() {
            bail!("Gmail messages.list failed ({status}): {body}");
        }
        let ids = body
            .get("messages")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|m| m.get("id")?.as_str().map(str::to_string))
            .collect();
        Ok(ids)
    }

    async fn get_message(&self, id: &str) -> Result<GmailMessage> {
        let url =
            format!("https://gmail.googleapis.com/gmail/v1/users/me/messages/{id}?format=full");
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("fetching Gmail message")?;
        let status = response.status();
        let body: Value = response.json().await.context("decoding Gmail message")?;
        if !status.is_success() {
            bail!("Gmail messages.get failed ({status}): {body}");
        }
        parse_gmail_message(&body)
    }
}

fn parse_gmail_message(body: &Value) -> Result<GmailMessage> {
    let id = body
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let thread_id = body
        .get("threadId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let internal_date_ms = body
        .get("internalDate")
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok())
        .or_else(|| body.get("internalDate").and_then(Value::as_i64))
        .unwrap_or(0);
    let label_ids = body
        .get("labelIds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();

    let headers = body
        .pointer("/payload/headers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let header = |name: &str| -> String {
        headers
            .iter()
            .find(|h| {
                h.get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|n| n.eq_ignore_ascii_case(name))
            })
            .and_then(|h| h.get("value"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };

    let message_id = header("Message-ID");
    let in_reply_to = header("In-Reply-To");
    let references = header("References")
        .split_whitespace()
        .map(str::to_string)
        .collect();
    let from = extract_email(&header("From"));
    let to = header("To")
        .split(',')
        .map(extract_email)
        .filter(|e| !e.is_empty())
        .collect();
    let subject = header("Subject");
    let text = extract_body_text(body.pointer("/payload").unwrap_or(body));

    Ok(GmailMessage {
        id,
        thread_id,
        message_id,
        in_reply_to,
        references,
        from,
        to,
        subject,
        body: text,
        internal_date_ms,
        label_ids,
    })
}

fn extract_body_text(payload: &Value) -> String {
    // Prefer text/plain parts; fall back to snippet-like body data.
    if let Some(mime) = payload.get("mimeType").and_then(Value::as_str) {
        if mime.eq_ignore_ascii_case("text/plain") {
            if let Some(data) = payload.pointer("/body/data").and_then(Value::as_str) {
                if let Ok(bytes) = decode_body(data) {
                    return String::from_utf8_lossy(&bytes).into_owned();
                }
            }
        }
    }
    if let Some(parts) = payload.get("parts").and_then(Value::as_array) {
        for part in parts {
            let text = extract_body_text(part);
            if !text.trim().is_empty() {
                return text;
            }
        }
    }
    if let Some(data) = payload.pointer("/body/data").and_then(Value::as_str) {
        if let Ok(bytes) = decode_body(data) {
            return String::from_utf8_lossy(&bytes).into_owned();
        }
    }
    String::new()
}

fn decode_body(data: &str) -> Result<Vec<u8>> {
    // Gmail uses URL-safe base64 without padding.
    let padded = match data.len() % 4 {
        2 => format!("{data}=="),
        3 => format!("{data}="),
        _ => data.to_string(),
    };
    base64::engine::general_purpose::URL_SAFE
        .decode(padded.as_bytes())
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(data.as_bytes()))
        .context("decoding Gmail body")
}

fn extract_email(raw: &str) -> String {
    let raw = raw.trim();
    if let Some(start) = raw.find('<') {
        if let Some(end) = raw.find('>') {
            if end > start {
                return raw[start + 1..end].trim().to_ascii_lowercase();
            }
        }
    }
    raw.split_whitespace()
        .next()
        .unwrap_or(raw)
        .trim()
        .trim_matches(|c| c == '<' || c == '>' || c == '"')
        .to_ascii_lowercase()
}

async fn ingest_message(
    db: &SharedDb,
    brand: &str,
    our_email: &str,
    msg: &GmailMessage,
    direction_hint: &str,
) -> Result<bool> {
    let our = our_email.trim().to_ascii_lowercase();
    let direction = if direction_hint == "outbound" || msg.from == our {
        "outbound"
    } else {
        "inbound"
    };

    let counterparty = if direction == "outbound" {
        msg.to
            .iter()
            .find(|e| e.as_str() != our)
            .cloned()
            .unwrap_or_default()
    } else {
        msg.from.clone()
    };
    if counterparty.is_empty() || counterparty == our {
        return Ok(false);
    }

    // Prefer matching a known person / conversation.
    let mut thread_ids = Vec::new();
    if !msg.in_reply_to.is_empty() {
        thread_ids.push(msg.in_reply_to.clone());
    }
    thread_ids.extend(msg.references.iter().cloned());
    if !msg.message_id.is_empty() {
        thread_ids.push(msg.message_id.clone());
    }
    // Gmail thread id is also a useful handle when we stored prior messages.
    if !msg.thread_id.is_empty() {
        thread_ids.push(format!("gmail:{}", msg.thread_id));
    }

    let conversation =
        db.conversation_for_inbound(brand, &counterparty, &msg.subject, &thread_ids)?;
    let Some(conversation) = conversation else {
        // No CRM person yet — still capture as a learning breadcrumb for patterns.
        if direction == "inbound" {
            let _ = db.record_learning(
                brand,
                "unmatched_inbound",
                &counterparty,
                &counterparty,
                &format!("subject={} · no CRM match yet", truncate(&msg.subject, 80)),
            );
        }
        return Ok(false);
    };

    let message = ConversationMessage {
        conversation_id: conversation.id.clone(),
        direction: direction.to_string(),
        sender_email: if direction == "outbound" {
            our.clone()
        } else {
            counterparty.clone()
        },
        recipient_email: if direction == "outbound" {
            counterparty.clone()
        } else {
            our.clone()
        },
        participants: {
            let mut p = vec![our.clone(), counterparty.clone()];
            p.extend(msg.to.iter().cloned());
            p.sort();
            p.dedup();
            p
        },
        subject: msg.subject.clone(),
        body: msg.body.clone(),
        status: "received".into(),
        message_id: if msg.message_id.is_empty() {
            format!("gmail:{}", msg.id)
        } else {
            msg.message_id.clone()
        },
        in_reply_to: msg.in_reply_to.clone(),
        references: msg.references.clone(),
        classification: if direction == "inbound" {
            "reply".into()
        } else {
            "outbound".into()
        },
        ..Default::default()
    };
    db.insert_conversation_message(&message)?;

    if direction == "inbound" {
        let _ = db.record_learning(
            brand,
            "got_reply",
            &counterparty,
            &counterparty,
            &format!(
                "subject={} · preview={}",
                truncate(&msg.subject, 60),
                truncate(msg.body.trim(), 120)
            ),
        );
        // Mark person as replied when we know them.
        if let Ok(Some(mut person)) = db.get_person(&conversation.person_id) {
            if person.status != "replied" {
                person.status = "replied".into();
                let _ = db.set_person_status(&person.id, "replied");
            }
        }
    }

    let _ = msg.label_ids; // reserved for unread / important later
    let _ = msg.internal_date_ms;
    Ok(true)
}

/// Turn recent conversation outcomes into durable learnings the router can use.
fn distill_learnings(db: &SharedDb, brand: &str) -> Result<usize> {
    let people = db.list_people(Some(brand), None)?;
    let mut recorded = 0usize;
    let mut seen_subjects: HashSet<String> = HashSet::new();

    // People who were contacted (have a sequence) but never replied.
    for person in people {
        if person.email.trim().is_empty() {
            continue;
        }
        let email = person.email.to_ascii_lowercase();
        let seq = db.active_sequence_for_person(&person.id)?;
        let Some(seq_id) = seq else {
            continue;
        };
        let sent = db.sequence_sent_count(&seq_id).unwrap_or(0);
        if sent == 0 {
            continue;
        }
        if person.status.eq_ignore_ascii_case("replied") {
            let key = format!("reply:{email}");
            if seen_subjects.insert(key) {
                db.record_learning(
                    brand,
                    "working_reply",
                    &person.name,
                    &email,
                    &format!(
                        "{} · {} · title={} · vantage={}",
                        person.name, email, person.title, person.vantage
                    ),
                )?;
                recorded += 1;
            }
            continue;
        }
        // No reply after sends.
        let key = format!("noreply:{email}");
        if seen_subjects.insert(key) {
            db.record_learning(
                brand,
                "no_reply_yet",
                &person.name,
                &email,
                &format!(
                    "{} touches sent, no reply yet · title={} · vantage={}",
                    sent, person.title, person.vantage
                ),
            )?;
            recorded += 1;
        }
    }
    Ok(recorded)
}

fn ensure_oauth_mailbox(db: &SharedDb, brand: &str, email: &str) -> Result<()> {
    let existing = db.list_mailboxes(Some(brand))?;
    if existing
        .iter()
        .any(|m| m.from_email.eq_ignore_ascii_case(email))
    {
        return Ok(());
    }
    // Soft presence: Gmail OAuth mailboxes don't need SMTP/IMAP hosts for *read*.
    // Sending can still use SMTP env config or Gmail API later.
    let m = crate::db::Mailbox {
        brand: brand.to_string(),
        from_name: brand.to_string(),
        from_email: email.to_string(),
        smtp_host: String::new(),
        smtp_port: 587,
        smtp_user: email.to_string(),
        smtp_pass: String::new(),
        imap_host: String::new(),
        imap_port: 993,
        daily_cap: 30,
        active: true,
        ..Default::default()
    };
    db.upsert_mailbox(&m)?;
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    let s = s.replace('\n', " ");
    if s.chars().count() <= max {
        return s;
    }
    s.chars().take(max.saturating_sub(1)).collect::<String>() + "…"
}

fn urlencoding_lite(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Human summary for the REPL.
pub fn format_sync_report(summaries: &[SyncSummary]) -> String {
    if summaries.is_empty() {
        return "No mailboxes synced.".into();
    }
    summaries
        .iter()
        .map(|s| {
            format!(
                "{brand} <{email}>: inbox {in_n}, sent {sent}, matched in/out {mi}/{mo}, learnings {learn}{err}",
                brand = s.brand,
                email = if s.email.is_empty() { "?" } else { &s.email },
                in_n = s.inbox_scanned,
                sent = s.sent_scanned,
                mi = s.matched_inbound,
                mo = s.matched_outbound,
                learn = s.learnings_recorded,
                err = if s.errors > 0 {
                    format!(" · {} error(s)", s.errors)
                } else {
                    String::new()
                }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::{extract_email, truncate};

    #[test]
    fn extracts_angle_bracket_emails() {
        assert_eq!(
            extract_email("Jane Doe <Jane.Doe@Example.com>"),
            "jane.doe@example.com"
        );
        assert_eq!(extract_email("plain@example.com"), "plain@example.com");
    }

    #[test]
    fn truncates_with_ellipsis() {
        assert_eq!(truncate("hello", 10), "hello");
        assert!(truncate("abcdefghijklmnopqrstuvwxyz", 8).ends_with('…'));
    }
}
