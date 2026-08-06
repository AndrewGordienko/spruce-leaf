//! The execution spine: a SQLite database of *real* leads, people, mailboxes,
//! and the scheduled outreach the cadence engine drives.
//!
//! The original tool invented companies and people and filed them in a JSON blob
//! you worked by hand. An actual SDR needs durable, queryable, restart-safe state:
//! which real person got which touch, when the next one is due, who replied, who
//! unsubscribed, which mailbox is at its daily cap. That lives here.
//!
//! One process holds one connection (WAL mode, foreign keys on) behind a `Mutex`
//! shared between the web dashboard and the cadence daemon. SQLite writes are
//! sub-millisecond for this scale, so calls run inline on the async runtime.
//!
//! Status vocabularies (stored as TEXT):
//!   * lead.status   — candidate | qualified | rejected | active | done
//!   * person.status — new | enriched | verified | contacted | replied |
//!                     bounced | unsubscribed | suppressed
//!   * sequence.status — active | paused | completed | stopped
//!   * touch.status  — draft | scheduled | sent | skipped | failed | replied |
//!                     cancelled  (only `scheduled` + due fire in the daemon)

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A real company sourced from Apollo and (optionally) qualified against a brand
/// thesis. Everything the model *guesses* stays in the inference/hypothesis
/// fields; the fact fields hold only what Apollo/verification could support.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Lead {
    pub id: String,
    pub brand: String,
    pub apollo_org_id: String,
    pub name: String,
    pub domain: String,
    pub industry: String,
    pub hq: String,
    pub headcount: i64,
    pub revenue: String,
    pub thesis: String,
    pub hypothesis: String,
    pub mechanism: String,
    pub consequence_metric: String,
    pub system_concept: String,
    pub hard_buyer_question: String,
    pub kill_condition: String,
    pub observed_facts: Vec<String>,
    pub inferences: Vec<String>,
    pub signals: Vec<String>,
    pub magnitude_note: String,
    pub applied_principles: Vec<String>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

/// A real person at a lead. `email`/`email_status` are populated by enrichment +
/// verification; sending is gated on `email_status == "verified"`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Person {
    pub id: String,
    pub lead_id: String,
    pub brand: String,
    pub apollo_person_id: String,
    pub first_name: String,
    pub last_name: String,
    pub name: String,
    pub title: String,
    pub vantage: String,
    pub can_observe: String,
    pub why_them: String,
    pub primary: bool,
    pub route_to: String,
    pub linkedin_url: String,
    pub email: String,
    /// verified | unverified | risky | invalid | unknown
    pub email_status: String,
    pub phone: String,
    pub status: String,
    pub enriched_at: String,
    pub created_at: String,
    pub updated_at: String,
}

/// A sending identity for one brand. Deliverability caps and warmup live here so
/// the cadence engine never blows past a mailbox's daily limit.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Mailbox {
    pub id: String,
    pub brand: String,
    pub from_name: String,
    pub from_email: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_user: String,
    pub smtp_pass: String,
    pub imap_host: String,
    pub imap_port: u16,
    pub daily_cap: i64,
    pub sent_today: i64,
    pub warmup_day: i64,
    pub last_reset: String,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Sequence {
    pub id: String,
    pub person_id: String,
    pub lead_id: String,
    pub brand: String,
    pub thesis: String,
    pub status: String,
    pub current_stage: i64,
    pub created_at: String,
}

/// One scheduled touch. `due_at` is when the cadence engine may fire it; a touch
/// is `draft` until approved (or created `scheduled` directly in auto mode).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Touch {
    pub id: String,
    pub sequence_id: String,
    pub person_id: String,
    pub lead_id: String,
    pub brand: String,
    pub stage: i64,
    pub day_offset: i64,
    pub channel: String,
    pub subject: String,
    pub body: String,
    pub purpose: String,
    pub goal: String,
    pub status: String,
    pub due_at: String,
    pub sent_at: String,
    pub mailbox_id: String,
    pub message_id: String,
    pub error: String,
    pub review_passes: Option<bool>,
    pub review_issues: Vec<String>,
    pub created_at: String,
}

/// An append-only activity log — every meaningful thing that happened to a
/// person or touch, so the funnel metrics and audit trail are reconstructable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub ts: String,
    pub brand: String,
    pub person_id: String,
    pub touch_id: String,
    /// sourced | enriched | verified | scheduled | sent | delivered | bounced |
    /// opened | replied | classified | unsubscribed | suppressed | error
    pub kind: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Reply {
    pub id: String,
    pub person_id: String,
    pub sequence_id: String,
    pub ts: String,
    pub from_email: String,
    pub subject: String,
    pub body: String,
    pub classification: String,
    pub action_taken: String,
    pub message_id: String,
    pub in_reply_to: String,
}

pub type SharedDb = Arc<Db>;

pub struct Db {
    conn: Mutex<Connection>,
}

impl Db {
    /// Open (creating if needed) the database at `path` and run migrations.
    pub fn open(path: impl AsRef<Path>) -> Result<SharedDb> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(path)
            .with_context(|| format!("opening SQLite db at {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL").ok();
        conn.pragma_update(None, "foreign_keys", "ON").ok();
        conn.pragma_update(None, "busy_timeout", 5000).ok();
        let db = Db { conn: Mutex::new(conn) };
        db.migrate()?;
        Ok(Arc::new(db))
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(SCHEMA).context("running SQLite migrations")?;
        Ok(())
    }

    // --- Leads -------------------------------------------------------------

    /// Insert or update a lead, keyed on (brand, apollo_org_id). Returns its id.
    pub fn upsert_lead(&self, lead: &Lead) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let now = now();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM leads WHERE brand=?1 AND apollo_org_id=?2",
                params![lead.brand, lead.apollo_org_id],
                |r| r.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| Uuid::new_v4().to_string());
        conn.execute(
            "INSERT INTO leads (id,brand,apollo_org_id,name,domain,industry,hq,headcount,revenue,\
             thesis,hypothesis,mechanism,consequence_metric,system_concept,hard_buyer_question,\
             kill_condition,observed_facts,inferences,signals,magnitude_note,applied_principles,\
             status,created_at,updated_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24) \
             ON CONFLICT(brand,apollo_org_id) DO UPDATE SET \
             name=?4,domain=?5,industry=?6,hq=?7,headcount=?8,revenue=?9,thesis=?10,hypothesis=?11,\
             mechanism=?12,consequence_metric=?13,system_concept=?14,hard_buyer_question=?15,\
             kill_condition=?16,observed_facts=?17,inferences=?18,signals=?19,magnitude_note=?20,\
             applied_principles=?21,status=?22,updated_at=?24",
            params![
                id, lead.brand, lead.apollo_org_id, lead.name, lead.domain, lead.industry, lead.hq,
                lead.headcount, lead.revenue, lead.thesis, lead.hypothesis, lead.mechanism,
                lead.consequence_metric, lead.system_concept, lead.hard_buyer_question,
                lead.kill_condition, js(&lead.observed_facts), js(&lead.inferences), js(&lead.signals),
                lead.magnitude_note, js(&lead.applied_principles), status_or(&lead.status, "candidate"),
                now, now,
            ],
        )?;
        Ok(id)
    }

    pub fn list_leads(&self, brand: Option<&str>) -> Result<Vec<Lead>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM leads WHERE (?1 IS NULL OR brand=?1) ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(params![brand], |r| Ok(row_to_lead(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // --- People ------------------------------------------------------------

    /// Insert or update a person, keyed on (brand, apollo_person_id). Returns id.
    pub fn upsert_person(&self, p: &Person) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let now = now();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM people WHERE brand=?1 AND apollo_person_id=?2",
                params![p.brand, p.apollo_person_id],
                |r| r.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| Uuid::new_v4().to_string());
        conn.execute(
            "INSERT INTO people (id,lead_id,brand,apollo_person_id,first_name,last_name,name,title,\
             vantage,can_observe,why_them,primary_contact,route_to,linkedin_url,email,email_status,\
             phone,status,enriched_at,created_at,updated_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21) \
             ON CONFLICT(brand,apollo_person_id) DO UPDATE SET \
             lead_id=?2,first_name=?5,last_name=?6,name=?7,title=?8,vantage=?9,can_observe=?10,\
             why_them=?11,primary_contact=?12,route_to=?13,linkedin_url=?14,email=?15,email_status=?16,\
             phone=?17,status=?18,enriched_at=?19,updated_at=?21",
            params![
                id, p.lead_id, p.brand, p.apollo_person_id, p.first_name, p.last_name, p.name,
                p.title, p.vantage, p.can_observe, p.why_them, p.primary, p.route_to, p.linkedin_url,
                p.email, status_or(&p.email_status, "unknown"), p.phone, status_or(&p.status, "new"),
                p.enriched_at, now, now,
            ],
        )?;
        Ok(id)
    }

    pub fn get_person(&self, id: &str) -> Result<Option<Person>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row("SELECT * FROM people WHERE id=?1", params![id], |r| Ok(row_to_person(r)))
            .optional()?)
    }

    pub fn list_people(&self, brand: Option<&str>, status: Option<&str>) -> Result<Vec<Person>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM people WHERE (?1 IS NULL OR brand=?1) AND (?2 IS NULL OR status=?2) \
             ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(params![brand, status], |r| Ok(row_to_person(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Update email + verification result after enrichment/verify.
    pub fn set_person_email(&self, id: &str, email: &str, status: &str, phone: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE people SET email=?2,email_status=?3,phone=?4,enriched_at=?5,updated_at=?5, \
             status=CASE WHEN ?3='verified' THEN 'verified' ELSE 'enriched' END WHERE id=?1",
            params![id, email, status, phone, now()],
        )?;
        Ok(())
    }

    pub fn set_person_status(&self, id: &str, status: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE people SET status=?2,updated_at=?3 WHERE id=?1",
            params![id, status, now()],
        )?;
        Ok(())
    }

    // --- Mailboxes ---------------------------------------------------------

    pub fn upsert_mailbox(&self, m: &Mailbox) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM mailboxes WHERE from_email=?1",
                params![m.from_email],
                |r| r.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| Uuid::new_v4().to_string());
        conn.execute(
            "INSERT INTO mailboxes (id,brand,from_name,from_email,smtp_host,smtp_port,smtp_user,\
             smtp_pass,imap_host,imap_port,daily_cap,sent_today,warmup_day,last_reset,active) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15) \
             ON CONFLICT(from_email) DO UPDATE SET brand=?2,from_name=?3,smtp_host=?5,smtp_port=?6,\
             smtp_user=?7,smtp_pass=?8,imap_host=?9,imap_port=?10,daily_cap=?11,active=?15",
            params![
                id, m.brand, m.from_name, m.from_email, m.smtp_host, m.smtp_port, m.smtp_user,
                m.smtp_pass, m.imap_host, m.imap_port, m.daily_cap, m.sent_today, m.warmup_day,
                now_date(), m.active,
            ],
        )?;
        Ok(id)
    }

    pub fn list_mailboxes(&self, brand: Option<&str>) -> Result<Vec<Mailbox>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT * FROM mailboxes WHERE (?1 IS NULL OR brand=?1) ORDER BY from_email")?;
        let rows = stmt.query_map(params![brand], |r| Ok(row_to_mailbox(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Pick the least-loaded active mailbox for a brand that still has headroom
    /// under its daily cap today. Rolls the per-day counter over at midnight UTC.
    pub fn pick_mailbox(&self, brand: &str) -> Result<Option<Mailbox>> {
        let conn = self.conn.lock().unwrap();
        // Roll daily counters that are stale.
        conn.execute(
            "UPDATE mailboxes SET sent_today=0,last_reset=?1 WHERE last_reset<>?1",
            params![now_date()],
        )?;
        Ok(conn
            .query_row(
                "SELECT * FROM mailboxes WHERE brand=?1 AND active=1 AND sent_today<daily_cap \
                 ORDER BY sent_today ASC LIMIT 1",
                params![brand],
                |r| Ok(row_to_mailbox(r)),
            )
            .optional()?)
    }

    pub fn bump_mailbox_sent(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE mailboxes SET sent_today=sent_today+1 WHERE id=?1",
            params![id],
        )?;
        Ok(())
    }

    // --- Sequences + touches ----------------------------------------------

    pub fn create_sequence(&self, s: &Sequence) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if s.id.is_empty() { Uuid::new_v4().to_string() } else { s.id.clone() };
        conn.execute(
            "INSERT INTO sequences (id,person_id,lead_id,brand,thesis,status,current_stage,created_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![id, s.person_id, s.lead_id, s.brand, s.thesis,
                status_or(&s.status, "active"), s.current_stage, now()],
        )?;
        Ok(id)
    }

    pub fn insert_touch(&self, t: &Touch) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if t.id.is_empty() { Uuid::new_v4().to_string() } else { t.id.clone() };
        conn.execute(
            "INSERT INTO touches (id,sequence_id,person_id,lead_id,brand,stage,day_offset,channel,\
             subject,body,purpose,goal,status,due_at,sent_at,mailbox_id,message_id,error,\
             review_passes,review_issues,created_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
            params![
                id, t.sequence_id, t.person_id, t.lead_id, t.brand, t.stage, t.day_offset, t.channel,
                t.subject, t.body, t.purpose, t.goal, status_or(&t.status, "draft"), t.due_at,
                t.sent_at, t.mailbox_id, t.message_id, t.error, t.review_passes, js(&t.review_issues),
                now(),
            ],
        )?;
        Ok(id)
    }

    /// Touches the cadence engine may fire now: scheduled + due, on an active,
    /// unpaused sequence, for an email-channel person who isn't suppressed/replied.
    pub fn due_touches(&self, brand: Option<&str>, limit: i64) -> Result<Vec<Touch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT t.* FROM touches t \
             JOIN sequences s ON s.id=t.sequence_id \
             JOIN people p ON p.id=t.person_id \
             WHERE t.status='scheduled' AND t.due_at<=?1 \
               AND s.status='active' \
               AND p.status NOT IN ('replied','unsubscribed','bounced','suppressed') \
               AND (?2 IS NULL OR t.brand=?2) \
             ORDER BY t.due_at ASC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![now(), brand, limit], |r| Ok(row_to_touch(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_touches_for_person(&self, person_id: &str) -> Result<Vec<Touch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT * FROM touches WHERE person_id=?1 ORDER BY stage ASC")?;
        let rows = stmt.query_map(params![person_id], |r| Ok(row_to_touch(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Mark a touch's outcome and stamp send metadata.
    pub fn set_touch_status(
        &self,
        id: &str,
        status: &str,
        mailbox_id: &str,
        message_id: &str,
        error: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let sent_at = if status == "sent" { now() } else { String::new() };
        conn.execute(
            "UPDATE touches SET status=?2,mailbox_id=?3,message_id=?4,error=?5,\
             sent_at=CASE WHEN ?2='sent' THEN ?6 ELSE sent_at END WHERE id=?1",
            params![id, status, mailbox_id, message_id, error, sent_at],
        )?;
        Ok(())
    }

    /// Flip an entire sequence's remaining touches to a terminal state (used when
    /// a reply lands or the person unsubscribes) and update the sequence status.
    pub fn stop_sequence(&self, sequence_id: &str, seq_status: &str, touch_status: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE touches SET status=?2 WHERE sequence_id=?1 AND status IN ('draft','scheduled')",
            params![sequence_id, touch_status],
        )?;
        conn.execute(
            "UPDATE sequences SET status=?2 WHERE id=?1",
            params![sequence_id, seq_status],
        )?;
        Ok(())
    }

    /// Approve drafted touches (draft → scheduled) for a person or whole brand.
    pub fn approve_touches(&self, brand: Option<&str>, person_id: Option<&str>) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE touches SET status='scheduled' WHERE status='draft' \
             AND (?1 IS NULL OR brand=?1) AND (?2 IS NULL OR person_id=?2)",
            params![brand, person_id],
        )?;
        Ok(n)
    }

    pub fn active_sequence_for_person(&self, person_id: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT id FROM sequences WHERE person_id=?1 AND status='active' LIMIT 1",
                params![person_id],
                |r| r.get(0),
            )
            .optional()?)
    }

    // --- Suppression -------------------------------------------------------

    /// Is this email (or its domain) suppressed for the brand? Suppression is
    /// checked before every send — the last line of compliance defense.
    pub fn is_suppressed(&self, brand: &str, email: &str) -> Result<bool> {
        let email = email.trim().to_lowercase();
        let domain = email.split('@').nth(1).unwrap_or("").to_string();
        let conn = self.conn.lock().unwrap();
        let hit: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM suppression WHERE brand=?1 AND (email=?2 OR (email=?3 AND ?3<>'')) LIMIT 1",
                params![brand, email, format!("@{domain}")],
                |r| r.get(0),
            )
            .optional()?;
        Ok(hit.is_some())
    }

    /// Add an email (or a `@domain` entry) to the suppression list.
    pub fn add_suppression(&self, brand: &str, email: &str, reason: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO suppression (id,brand,email,reason,created_at) \
             VALUES (?1,?2,?3,?4,?5)",
            params![Uuid::new_v4().to_string(), brand, email.trim().to_lowercase(), reason, now()],
        )?;
        Ok(())
    }

    // --- Replies -----------------------------------------------------------

    pub fn record_reply(&self, r: &Reply) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if r.id.is_empty() { Uuid::new_v4().to_string() } else { r.id.clone() };
        conn.execute(
            "INSERT INTO replies (id,person_id,sequence_id,ts,from_email,subject,body,\
             classification,action_taken,message_id,in_reply_to) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![id, r.person_id, r.sequence_id, now(), r.from_email, r.subject, r.body,
                r.classification, r.action_taken, r.message_id, r.in_reply_to],
        )?;
        Ok(id)
    }

    /// Find a person by any of their known emails (for matching inbound replies).
    pub fn person_by_email(&self, brand: &str, email: &str) -> Result<Option<Person>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM people WHERE brand=?1 AND lower(email)=lower(?2) LIMIT 1",
                params![brand, email],
                |r| Ok(row_to_person(r)),
            )
            .optional()?)
    }

    /// Was this inbound Message-ID already recorded? (idempotent reply ingest)
    pub fn reply_exists(&self, message_id: &str) -> Result<bool> {
        if message_id.is_empty() {
            return Ok(false);
        }
        let conn = self.conn.lock().unwrap();
        let hit: Option<i64> = conn
            .query_row("SELECT 1 FROM replies WHERE message_id=?1 LIMIT 1", params![message_id], |r| r.get(0))
            .optional()?;
        Ok(hit.is_some())
    }

    // --- Events + metrics --------------------------------------------------

    pub fn log_event(&self, brand: &str, person_id: &str, touch_id: &str, kind: &str, detail: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO events (id,ts,brand,person_id,touch_id,kind,detail) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![Uuid::new_v4().to_string(), now(), brand, person_id, touch_id, kind, detail],
        )?;
        Ok(())
    }

    /// Count events of each `kind` for a brand (or all) — the funnel raw numbers.
    pub fn event_counts(&self, brand: Option<&str>) -> Result<Vec<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT kind, COUNT(*) FROM events WHERE (?1 IS NULL OR brand=?1) GROUP BY kind",
        )?;
        let rows = stmt.query_map(params![brand], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn recent_events(&self, brand: Option<&str>, limit: i64) -> Result<Vec<Event>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id,ts,brand,person_id,touch_id,kind,detail FROM events \
             WHERE (?1 IS NULL OR brand=?1) ORDER BY ts DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![brand, limit], |r| {
            Ok(Event {
                id: r.get(0)?,
                ts: r.get(1)?,
                brand: r.get(2)?,
                person_id: r.get(3)?,
                touch_id: r.get(4)?,
                kind: r.get(5)?,
                detail: r.get(6)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

// --- Row mappers -----------------------------------------------------------

fn row_to_lead(r: &Row) -> Lead {
    Lead {
        id: g(r, "id"),
        brand: g(r, "brand"),
        apollo_org_id: g(r, "apollo_org_id"),
        name: g(r, "name"),
        domain: g(r, "domain"),
        industry: g(r, "industry"),
        hq: g(r, "hq"),
        headcount: r.get("headcount").unwrap_or(0),
        revenue: g(r, "revenue"),
        thesis: g(r, "thesis"),
        hypothesis: g(r, "hypothesis"),
        mechanism: g(r, "mechanism"),
        consequence_metric: g(r, "consequence_metric"),
        system_concept: g(r, "system_concept"),
        hard_buyer_question: g(r, "hard_buyer_question"),
        kill_condition: g(r, "kill_condition"),
        observed_facts: jd(&g(r, "observed_facts")),
        inferences: jd(&g(r, "inferences")),
        signals: jd(&g(r, "signals")),
        magnitude_note: g(r, "magnitude_note"),
        applied_principles: jd(&g(r, "applied_principles")),
        status: g(r, "status"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_person(r: &Row) -> Person {
    Person {
        id: g(r, "id"),
        lead_id: g(r, "lead_id"),
        brand: g(r, "brand"),
        apollo_person_id: g(r, "apollo_person_id"),
        first_name: g(r, "first_name"),
        last_name: g(r, "last_name"),
        name: g(r, "name"),
        title: g(r, "title"),
        vantage: g(r, "vantage"),
        can_observe: g(r, "can_observe"),
        why_them: g(r, "why_them"),
        primary: r.get::<_, i64>("primary_contact").unwrap_or(0) != 0,
        route_to: g(r, "route_to"),
        linkedin_url: g(r, "linkedin_url"),
        email: g(r, "email"),
        email_status: g(r, "email_status"),
        phone: g(r, "phone"),
        status: g(r, "status"),
        enriched_at: g(r, "enriched_at"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_mailbox(r: &Row) -> Mailbox {
    Mailbox {
        id: g(r, "id"),
        brand: g(r, "brand"),
        from_name: g(r, "from_name"),
        from_email: g(r, "from_email"),
        smtp_host: g(r, "smtp_host"),
        smtp_port: r.get::<_, i64>("smtp_port").unwrap_or(587) as u16,
        smtp_user: g(r, "smtp_user"),
        smtp_pass: g(r, "smtp_pass"),
        imap_host: g(r, "imap_host"),
        imap_port: r.get::<_, i64>("imap_port").unwrap_or(993) as u16,
        daily_cap: r.get("daily_cap").unwrap_or(30),
        sent_today: r.get("sent_today").unwrap_or(0),
        warmup_day: r.get("warmup_day").unwrap_or(0),
        last_reset: g(r, "last_reset"),
        active: r.get::<_, i64>("active").unwrap_or(1) != 0,
    }
}

fn row_to_touch(r: &Row) -> Touch {
    Touch {
        id: g(r, "id"),
        sequence_id: g(r, "sequence_id"),
        person_id: g(r, "person_id"),
        lead_id: g(r, "lead_id"),
        brand: g(r, "brand"),
        stage: r.get("stage").unwrap_or(0),
        day_offset: r.get("day_offset").unwrap_or(0),
        channel: g(r, "channel"),
        subject: g(r, "subject"),
        body: g(r, "body"),
        purpose: g(r, "purpose"),
        goal: g(r, "goal"),
        status: g(r, "status"),
        due_at: g(r, "due_at"),
        sent_at: g(r, "sent_at"),
        mailbox_id: g(r, "mailbox_id"),
        message_id: g(r, "message_id"),
        error: g(r, "error"),
        review_passes: r.get::<_, Option<bool>>("review_passes").unwrap_or(None),
        review_issues: jd(&g(r, "review_issues")),
        created_at: g(r, "created_at"),
    }
}

// --- helpers ---------------------------------------------------------------

/// Column getter that tolerates NULL/missing by returning an empty string.
fn g(r: &Row, col: &str) -> String {
    r.get::<_, Option<String>>(col).unwrap_or(None).unwrap_or_default()
}

fn js<T: Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "[]".into())
}

fn jd(s: &str) -> Vec<String> {
    if s.trim().is_empty() {
        return Vec::new();
    }
    serde_json::from_str(s).unwrap_or_default()
}

fn status_or(s: &str, default: &str) -> String {
    if s.trim().is_empty() { default.to_string() } else { s.to_string() }
}

pub fn now() -> String {
    Utc::now().to_rfc3339()
}

fn now_date() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

/// Parse an rfc3339 timestamp, defaulting to epoch on failure.
pub fn parse_ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS leads (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    apollo_org_id TEXT NOT NULL,
    name TEXT, domain TEXT, industry TEXT, hq TEXT,
    headcount INTEGER DEFAULT 0, revenue TEXT,
    thesis TEXT, hypothesis TEXT, mechanism TEXT, consequence_metric TEXT,
    system_concept TEXT, hard_buyer_question TEXT, kill_condition TEXT,
    observed_facts TEXT, inferences TEXT, signals TEXT,
    magnitude_note TEXT, applied_principles TEXT,
    status TEXT DEFAULT 'candidate',
    created_at TEXT, updated_at TEXT,
    UNIQUE(brand, apollo_org_id)
);
CREATE TABLE IF NOT EXISTS people (
    id TEXT PRIMARY KEY,
    lead_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    apollo_person_id TEXT NOT NULL,
    first_name TEXT, last_name TEXT, name TEXT, title TEXT,
    vantage TEXT, can_observe TEXT, why_them TEXT,
    primary_contact INTEGER DEFAULT 0, route_to TEXT, linkedin_url TEXT,
    email TEXT, email_status TEXT DEFAULT 'unknown', phone TEXT,
    status TEXT DEFAULT 'new', enriched_at TEXT,
    created_at TEXT, updated_at TEXT,
    UNIQUE(brand, apollo_person_id)
);
CREATE TABLE IF NOT EXISTS mailboxes (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    from_name TEXT, from_email TEXT NOT NULL,
    smtp_host TEXT, smtp_port INTEGER DEFAULT 587, smtp_user TEXT, smtp_pass TEXT,
    imap_host TEXT, imap_port INTEGER DEFAULT 993,
    daily_cap INTEGER DEFAULT 30, sent_today INTEGER DEFAULT 0,
    warmup_day INTEGER DEFAULT 0, last_reset TEXT, active INTEGER DEFAULT 1,
    UNIQUE(from_email)
);
CREATE TABLE IF NOT EXISTS sequences (
    id TEXT PRIMARY KEY,
    person_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    thesis TEXT,
    status TEXT DEFAULT 'active',
    current_stage INTEGER DEFAULT 0,
    created_at TEXT
);
CREATE TABLE IF NOT EXISTS touches (
    id TEXT PRIMARY KEY,
    sequence_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    stage INTEGER, day_offset INTEGER,
    channel TEXT, subject TEXT, body TEXT, purpose TEXT, goal TEXT,
    status TEXT DEFAULT 'draft',
    due_at TEXT, sent_at TEXT, mailbox_id TEXT, message_id TEXT, error TEXT,
    review_passes INTEGER, review_issues TEXT,
    created_at TEXT
);
CREATE TABLE IF NOT EXISTS suppression (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    email TEXT NOT NULL,
    reason TEXT,
    created_at TEXT,
    UNIQUE(brand, email)
);
CREATE TABLE IF NOT EXISTS replies (
    id TEXT PRIMARY KEY,
    person_id TEXT, sequence_id TEXT, ts TEXT,
    from_email TEXT, subject TEXT, body TEXT,
    classification TEXT, action_taken TEXT,
    message_id TEXT, in_reply_to TEXT
);
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    ts TEXT, brand TEXT, person_id TEXT, touch_id TEXT,
    kind TEXT, detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_touches_due ON touches(status, due_at);
CREATE INDEX IF NOT EXISTS idx_touches_person ON touches(person_id);
CREATE INDEX IF NOT EXISTS idx_people_brand_email ON people(brand, email);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_replies_msgid ON replies(message_id);
"#;
