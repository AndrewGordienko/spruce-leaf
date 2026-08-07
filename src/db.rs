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
//!     bounced | unsubscribed | suppressed
//!   * sequence.status — active | paused | completed | stopped
//!   * touch.status  — draft | scheduled | sent | skipped | failed | replied |
//!     cancelled  (only `scheduled` + due fire in the daemon)

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
    /// IANA timezone inferred from the Apollo HQ location.
    pub timezone: String,
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
    pub location: String,
    /// IANA timezone from the person's location, falling back to the lead HQ.
    pub timezone: String,
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
    pub recipient_timezone: String,
    pub scheduled_rule: String,
    pub schedule_reason: String,
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

/// One historical send attributed to the last touch before a reply. Calendar
/// analysis treats these as directional observations, not causal proof.
#[derive(Debug, Clone, Default)]
pub struct TimingObservation {
    pub industry: String,
    pub title: String,
    pub vantage: String,
    pub timezone: String,
    pub scheduled_rule: String,
    pub sent_at: String,
    pub replied: bool,
}

#[derive(Debug, Clone, Default)]
pub struct CalendarEntry {
    pub due_at: String,
    pub channel: String,
    pub status: String,
    pub recipient: String,
    pub account: String,
    pub purpose: String,
    pub recipient_timezone: String,
    pub scheduled_rule: String,
    pub motion: String,
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

/// A real funding/tender/pilot/partnership opportunity supported by source
/// evidence. The generic shape is intentional: grant logic lives in a business
/// profile, while the persistence layer can support other opportunity motions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[allow(dead_code)]
pub struct Opportunity {
    pub id: String,
    pub brand: String,
    pub kind: String,
    pub fingerprint: String,
    pub source_name: String,
    pub source_url: String,
    pub canonical_url: String,
    pub title: String,
    pub funder: String,
    pub funder_domain: String,
    pub summary: String,
    pub geography: String,
    /// open | forecast | rolling | closed | unknown
    pub opportunity_status: String,
    pub opens_at: String,
    pub deadline: String,
    pub deadline_timezone: String,
    pub funding_type: String,
    pub amount_min: String,
    pub amount_max: String,
    pub currency: String,
    pub cost_share: String,
    pub eligible_applicants: Vec<String>,
    pub eligible_activities: Vec<String>,
    pub ineligible_activities: Vec<String>,
    pub themes: Vec<String>,
    pub official_contact_name: String,
    pub official_contact_email: String,
    pub official_contact_phone: String,
    pub evidence: Vec<String>,
    pub documents: Vec<String>,
    pub fit_score: i64,
    /// strong_fit | possible_fit | needs_information | ineligible
    pub fit_status: String,
    pub fit_reasons: Vec<String>,
    pub blockers: Vec<String>,
    pub unknowns: Vec<String>,
    pub next_action: String,
    /// discovered | shortlisted | contacting | applying | submitted | won |
    /// lost | watching | expired
    pub pipeline_status: String,
    pub raw_snapshot: String,
    pub first_seen_at: String,
    pub last_verified_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[allow(dead_code)]
pub struct OpportunityContact {
    pub id: String,
    pub opportunity_id: String,
    pub brand: String,
    /// official | apollo
    pub source: String,
    pub contact_key: String,
    pub apollo_org_id: String,
    pub apollo_person_id: String,
    pub name: String,
    pub title: String,
    pub location: String,
    pub timezone: String,
    pub role: String,
    pub why_them: String,
    pub primary: bool,
    pub linkedin_url: String,
    pub email: String,
    pub email_status: String,
    pub phone: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[allow(dead_code)]
pub struct OpportunityTouch {
    pub id: String,
    pub opportunity_id: String,
    pub contact_id: String,
    pub brand: String,
    pub stage: i64,
    pub day_offset: i64,
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
    pub recipient_timezone: String,
    pub scheduled_rule: String,
    pub schedule_reason: String,
    pub review_passes: Option<bool>,
    pub review_issues: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[allow(dead_code)]
pub struct OpportunityReply {
    pub id: String,
    pub opportunity_id: String,
    pub contact_id: String,
    pub ts: String,
    pub from_email: String,
    pub subject: String,
    pub body: String,
    pub classification: String,
    pub action_taken: String,
    pub message_id: String,
    pub in_reply_to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[allow(dead_code)]
pub struct ApplicationBrief {
    pub id: String,
    pub opportunity_id: String,
    pub brand: String,
    pub status: String,
    pub eligibility_summary: String,
    pub project_shape: String,
    pub narrative: String,
    pub workplan: Vec<String>,
    pub milestones: Vec<String>,
    pub evidence_needed: Vec<String>,
    pub required_documents: Vec<String>,
    pub budget_questions: Vec<String>,
    pub questions_for_funder: Vec<String>,
    pub risks: Vec<String>,
    pub next_steps: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
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
        let db = Db {
            conn: Mutex::new(conn),
        };
        db.migrate()?;
        Ok(Arc::new(db))
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(SCHEMA)
            .context("running SQLite migrations")?;
        for (table, column, definition) in [
            ("leads", "timezone", "TEXT DEFAULT ''"),
            ("people", "location", "TEXT DEFAULT ''"),
            ("people", "timezone", "TEXT DEFAULT ''"),
            ("touches", "recipient_timezone", "TEXT DEFAULT ''"),
            ("touches", "scheduled_rule", "TEXT DEFAULT ''"),
            ("touches", "schedule_reason", "TEXT DEFAULT ''"),
            ("opportunity_contacts", "location", "TEXT DEFAULT ''"),
            ("opportunity_contacts", "timezone", "TEXT DEFAULT ''"),
            (
                "opportunity_touches",
                "recipient_timezone",
                "TEXT DEFAULT ''",
            ),
            ("opportunity_touches", "scheduled_rule", "TEXT DEFAULT ''"),
            ("opportunity_touches", "schedule_reason", "TEXT DEFAULT ''"),
        ] {
            ensure_column(&conn, table, column, definition)?;
        }
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
        conn.execute(
            "UPDATE leads SET timezone=?2 WHERE id=?1",
            params![id, lead.timezone],
        )?;
        Ok(id)
    }

    pub fn get_lead(&self, id: &str) -> Result<Option<Lead>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row("SELECT * FROM leads WHERE id=?1", params![id], |r| {
                Ok(row_to_lead(r))
            })
            .optional()?)
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
        conn.execute(
            "UPDATE people SET location=?2,timezone=?3 WHERE id=?1",
            params![id, p.location, p.timezone],
        )?;
        Ok(id)
    }

    pub fn get_person(&self, id: &str) -> Result<Option<Person>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row("SELECT * FROM people WHERE id=?1", params![id], |r| {
                Ok(row_to_person(r))
            })
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
                id,
                m.brand,
                m.from_name,
                m.from_email,
                m.smtp_host,
                m.smtp_port,
                m.smtp_user,
                m.smtp_pass,
                m.imap_host,
                m.imap_port,
                m.daily_cap,
                m.sent_today,
                m.warmup_day,
                now_date(),
                m.active,
            ],
        )?;
        Ok(id)
    }

    pub fn list_mailboxes(&self, brand: Option<&str>) -> Result<Vec<Mailbox>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM mailboxes WHERE (?1 IS NULL OR brand=?1) ORDER BY from_email",
        )?;
        let rows = stmt.query_map(params![brand], |r| Ok(row_to_mailbox(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Select the mailbox [`pick_mailbox_on`](Self::pick_mailbox_on) would use without
    /// rolling counters forward or otherwise changing persistent state. A stale
    /// daily counter is treated as zero only for this query.
    #[cfg(test)]
    pub fn preview_mailbox(&self, brand: &str) -> Result<Option<Mailbox>> {
        self.preview_mailbox_on(brand, &now_date())
    }

    pub fn preview_mailbox_on(&self, brand: &str, counter_date: &str) -> Result<Option<Mailbox>> {
        let conn = self.conn.lock().unwrap();
        // Dry-run: never mutates. Effective headroom is the warmup-limited cap,
        // and a stale counter is treated as fresh (0 sent) for the given date.
        let mut stmt = conn.prepare(
            "SELECT * FROM mailboxes WHERE brand=?1 AND active=1 \
             ORDER BY CASE WHEN last_reset=?2 THEN sent_today ELSE 0 END ASC",
        )?;
        let rows = stmt.query_map(params![brand, counter_date], |r| Ok(row_to_mailbox(r)))?;
        let mut candidates = Vec::new();
        for m in rows {
            candidates.push(m?);
        }
        Ok(candidates.into_iter().find(|m| {
            let sent = if m.last_reset == counter_date {
                m.sent_today
            } else {
                0
            };
            sent < warmup_cap(m.daily_cap, m.warmup_day)
        }))
    }

    /// Pick the least-loaded active mailbox for a brand that still has headroom
    /// for the supplied business calendar date. Headroom respects the per-mailbox
    /// warmup ramp: a cold domain sends only a fraction of its daily cap until it
    /// has aged ~3 weeks, so a large campaign fills in over time instead of
    /// torching a fresh sending reputation on day one.
    pub fn pick_mailbox_on(&self, brand: &str, counter_date: &str) -> Result<Option<Mailbox>> {
        let conn = self.conn.lock().unwrap();
        // Roll stale daily counters, and age the warmup clock by one sending day
        // for mailboxes that have sent before (a brand-new mailbox stays at day 0).
        conn.execute(
            "UPDATE mailboxes SET sent_today=0, \
             warmup_day = warmup_day + (CASE WHEN last_reset<>'' THEN 1 ELSE 0 END), \
             last_reset=?1 WHERE last_reset<>?1",
            params![counter_date],
        )?;
        let mut stmt = conn.prepare(
            "SELECT * FROM mailboxes WHERE brand=?1 AND active=1 ORDER BY sent_today ASC",
        )?;
        let rows = stmt.query_map(params![brand], |r| Ok(row_to_mailbox(r)))?;
        let mut candidates = Vec::new();
        for m in rows {
            candidates.push(m?);
        }
        Ok(candidates
            .into_iter()
            .find(|m| m.sent_today < warmup_cap(m.daily_cap, m.warmup_day)))
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
        let id = if s.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            s.id.clone()
        };
        conn.execute(
            "INSERT INTO sequences (id,person_id,lead_id,brand,thesis,status,current_stage,created_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![id, s.person_id, s.lead_id, s.brand, s.thesis,
                status_or(&s.status, "active"), s.current_stage, now()],
        )?;
        Ok(id)
    }

    pub fn reschedule_touch(
        &self,
        id: &str,
        due_at: &str,
        recipient_timezone: &str,
        rule: &str,
        reason: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE touches SET due_at=?2,recipient_timezone=?3,scheduled_rule=?4,schedule_reason=?5 WHERE id=?1",
            params![id, due_at, recipient_timezone, rule, reason],
        )?;
        Ok(())
    }

    pub fn insert_touch(&self, t: &Touch) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if t.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            t.id.clone()
        };
        conn.execute(
            "INSERT INTO touches (id,sequence_id,person_id,lead_id,brand,stage,day_offset,channel,\
             subject,body,purpose,goal,status,due_at,sent_at,mailbox_id,message_id,error,\
             review_passes,review_issues,created_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
            params![
                id,
                t.sequence_id,
                t.person_id,
                t.lead_id,
                t.brand,
                t.stage,
                t.day_offset,
                t.channel,
                t.subject,
                t.body,
                t.purpose,
                t.goal,
                status_or(&t.status, "draft"),
                t.due_at,
                t.sent_at,
                t.mailbox_id,
                t.message_id,
                t.error,
                t.review_passes,
                js(&t.review_issues),
                now(),
            ],
        )?;
        conn.execute(
            "UPDATE touches SET recipient_timezone=?2,scheduled_rule=?3,schedule_reason=?4 WHERE id=?1",
            params![
                id,
                t.recipient_timezone,
                t.scheduled_rule,
                t.schedule_reason,
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
               AND NOT EXISTS ( \
                   SELECT 1 FROM touches prior \
                   WHERE prior.sequence_id=t.sequence_id AND prior.stage<t.stage \
                     AND lower(prior.channel)='email' \
                     AND prior.status NOT IN ('sent','skipped','cancelled','replied') \
               ) \
             ORDER BY t.due_at ASC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![now(), brand, limit], |r| Ok(row_to_touch(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_touches_for_person(&self, person_id: &str) -> Result<Vec<Touch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT * FROM touches WHERE person_id=?1 ORDER BY stage ASC")?;
        let rows = stmt.query_map(params![person_id], |r| Ok(row_to_touch(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Capacity used or reserved for one business calendar day. Drafts are
    /// included because approval must not turn an apparently safe plan into a
    /// 60-touch day, and sent touches remain part of that day's total. Manual
    /// and automatic channels share the same business cap.
    pub fn planned_touch_count_between(
        &self,
        brand: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let start = start.to_rfc3339();
        let end = end.to_rfc3339();
        let regular: i64 = conn.query_row(
            "SELECT COUNT(*) FROM touches WHERE brand=?1 AND due_at>=?2 AND due_at<?3
             AND status IN ('draft','scheduled','sent')",
            params![brand, start, end],
            |r| r.get(0),
        )?;
        let opportunities: i64 = conn.query_row(
            "SELECT COUNT(*) FROM opportunity_touches WHERE brand=?1 AND due_at>=?2 AND due_at<?3
             AND status IN ('draft','scheduled','sent')",
            params![brand, start, end],
            |r| r.get(0),
        )?;
        Ok((regular + opportunities).max(0) as usize)
    }

    /// Actual automated email sends across every mailbox and motion for a
    /// business. This is the last-second hard cap used by the cadence daemon.
    pub fn sent_touch_count_between(
        &self,
        brand: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let start = start.to_rfc3339();
        let end = end.to_rfc3339();
        let regular: i64 = conn.query_row(
            "SELECT COUNT(*) FROM touches WHERE brand=?1 AND status='sent'
             AND sent_at>=?2 AND sent_at<?3",
            params![brand, start, end],
            |r| r.get(0),
        )?;
        let opportunities: i64 = conn.query_row(
            "SELECT COUNT(*) FROM opportunity_touches WHERE brand=?1 AND status='sent'
             AND sent_at>=?2 AND sent_at<?3",
            params![brand, start, end],
            |r| r.get(0),
        )?;
        Ok((regular + opportunities).max(0) as usize)
    }

    pub fn upcoming_calendar(&self, brand: &str, limit: usize) -> Result<Vec<CalendarEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut entries = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT t.due_at,t.channel,t.status,p.name,l.name,t.purpose,
                        t.recipient_timezone,t.scheduled_rule
                 FROM touches t
                 JOIN people p ON p.id=t.person_id
                 JOIN leads l ON l.id=t.lead_id
                 WHERE t.brand=?1 AND t.status IN ('draft','scheduled')
                 ORDER BY t.due_at ASC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![brand, limit as i64], |row| {
                Ok(CalendarEntry {
                    due_at: row.get(0)?,
                    channel: row.get(1)?,
                    status: row.get(2)?,
                    recipient: row.get(3)?,
                    account: row.get(4)?,
                    purpose: row.get(5)?,
                    recipient_timezone: row.get(6)?,
                    scheduled_rule: row.get(7)?,
                    motion: "sales".into(),
                })
            })?;
            entries.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
        }
        {
            let mut stmt = conn.prepare(
                "SELECT t.due_at,'email',t.status,c.name,o.title,t.purpose,
                        t.recipient_timezone,t.scheduled_rule
                 FROM opportunity_touches t
                 JOIN opportunity_contacts c ON c.id=t.contact_id
                 JOIN opportunities o ON o.id=t.opportunity_id
                 WHERE t.brand=?1 AND t.status IN ('draft','scheduled')
                 ORDER BY t.due_at ASC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![brand, limit as i64], |row| {
                Ok(CalendarEntry {
                    due_at: row.get(0)?,
                    channel: row.get(1)?,
                    status: row.get(2)?,
                    recipient: row.get(3)?,
                    account: row.get(4)?,
                    purpose: row.get(5)?,
                    recipient_timezone: row.get(6)?,
                    scheduled_rule: row.get(7)?,
                    motion: "funding".into(),
                })
            })?;
            entries.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
        }
        entries.sort_by(|left, right| left.due_at.cmp(&right.due_at));
        entries.truncate(limit);
        Ok(entries)
    }

    #[allow(dead_code)]
    pub fn previous_message_id(&self, sequence_id: &str, stage: i64) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT message_id FROM touches WHERE sequence_id=?1 AND stage<?2
                 AND status='sent' AND message_id<>'' ORDER BY stage DESC LIMIT 1",
                params![sequence_id, stage],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or_default())
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
        let sent_at = if status == "sent" {
            now()
        } else {
            String::new()
        };
        conn.execute(
            "UPDATE touches SET status=?2,mailbox_id=?3,message_id=?4,error=?5,\
             sent_at=CASE WHEN ?2='sent' THEN ?6 ELSE sent_at END WHERE id=?1",
            params![id, status, mailbox_id, message_id, error, sent_at],
        )?;
        Ok(())
    }

    /// Flip an entire sequence's remaining touches to a terminal state (used when
    /// a reply lands or the person unsubscribes) and update the sequence status.
    pub fn stop_sequence(
        &self,
        sequence_id: &str,
        seq_status: &str,
        touch_status: &str,
    ) -> Result<()> {
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

    /// Approve drafted email touches (draft → scheduled) for a person or
    /// whole brand. Manual LinkedIn/call tasks deliberately remain drafts: the
    /// cadence daemon cannot execute them and they must not clog its due queue.
    pub fn approve_touches(&self, brand: Option<&str>, person_id: Option<&str>) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE touches SET status='scheduled' WHERE status='draft' \
             AND lower(channel)='email' \
             AND review_passes=1 \
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
}

// --- Opportunities ----------------------------------------------------

#[allow(dead_code)]
impl Db {
    pub fn upsert_opportunity(&self, o: &Opportunity) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let now = now();
        let canonical_url = o.canonical_url.trim_end_matches('/').to_lowercase();
        let existing: Option<(String, String, String)> = conn
            .query_row(
                "SELECT id,first_seen_at,fingerprint FROM opportunities
                 WHERE brand=?1 AND (fingerprint=?2 OR
                       (?3<>'' AND rtrim(lower(canonical_url),'/')=?3))
                 ORDER BY CASE WHEN fingerprint=?2 THEN 0 ELSE 1 END,
                          fit_score DESC,first_seen_at ASC LIMIT 1",
                params![o.brand, o.fingerprint, canonical_url],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let (id, first_seen, fingerprint) = existing.unwrap_or_else(|| {
            (
                Uuid::new_v4().to_string(),
                now.clone(),
                o.fingerprint.clone(),
            )
        });
        conn.execute(
            "INSERT INTO opportunities (id,brand,kind,fingerprint,source_name,source_url,canonical_url,title,
             funder,funder_domain,summary,geography,opportunity_status,opens_at,deadline,deadline_timezone,
             funding_type,amount_min,amount_max,currency,cost_share,eligible_applicants,eligible_activities,
             ineligible_activities,themes,official_contact_name,official_contact_email,official_contact_phone,
             evidence,documents,fit_score,fit_status,fit_reasons,blockers,unknowns,next_action,pipeline_status,
             raw_snapshot,first_seen_at,last_verified_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,
                     ?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38,
                     ?39,?40,?41)
             ON CONFLICT(brand,fingerprint) DO UPDATE SET
             kind=?3,source_name=?5,source_url=?6,canonical_url=?7,title=?8,funder=?9,funder_domain=?10,
             summary=?11,geography=?12,opportunity_status=?13,opens_at=?14,deadline=?15,
             deadline_timezone=?16,funding_type=?17,amount_min=?18,amount_max=?19,currency=?20,
             cost_share=?21,eligible_applicants=?22,eligible_activities=?23,ineligible_activities=?24,
             themes=?25,official_contact_name=?26,official_contact_email=?27,official_contact_phone=?28,
             evidence=?29,documents=?30,fit_score=?31,fit_status=?32,fit_reasons=?33,blockers=?34,
             unknowns=?35,next_action=?36,
             pipeline_status=CASE WHEN opportunities.pipeline_status IN ('applying','submitted','won','lost')
                                  THEN opportunities.pipeline_status ELSE ?37 END,
             raw_snapshot=?38,last_verified_at=?40,updated_at=?41",
            params![
                id,
                o.brand,
                status_or(&o.kind, "funding"),
                fingerprint,
                o.source_name,
                o.source_url,
                o.canonical_url,
                o.title,
                o.funder,
                o.funder_domain,
                o.summary,
                o.geography,
                status_or(&o.opportunity_status, "unknown"),
                o.opens_at,
                o.deadline,
                o.deadline_timezone,
                o.funding_type,
                o.amount_min,
                o.amount_max,
                o.currency,
                o.cost_share,
                js(&o.eligible_applicants),
                js(&o.eligible_activities),
                js(&o.ineligible_activities),
                js(&o.themes),
                o.official_contact_name,
                o.official_contact_email,
                o.official_contact_phone,
                js(&o.evidence),
                js(&o.documents),
                o.fit_score.clamp(0, 100),
                status_or(&o.fit_status, "needs_information"),
                js(&o.fit_reasons),
                js(&o.blockers),
                js(&o.unknowns),
                o.next_action,
                status_or(&o.pipeline_status, "discovered"),
                o.raw_snapshot,
                first_seen,
                now,
                now,
            ],
        )?;
        Ok(id)
    }

    pub fn get_opportunity(&self, id: &str) -> Result<Option<Opportunity>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM opportunities WHERE id=?1",
                params![id],
                |r| Ok(row_to_opportunity(r)),
            )
            .optional()?)
    }

    pub fn list_opportunities(
        &self,
        brand: Option<&str>,
        pipeline_status: Option<&str>,
    ) -> Result<Vec<Opportunity>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM opportunities WHERE (?1 IS NULL OR brand=?1)
             AND (?2 IS NULL OR pipeline_status=?2)
             ORDER BY CASE opportunity_status WHEN 'open' THEN 0 WHEN 'rolling' THEN 1
                       WHEN 'forecast' THEN 2 WHEN 'unknown' THEN 3 ELSE 4 END,
                      fit_score DESC,last_verified_at DESC",
        )?;
        let rows = stmt.query_map(params![brand, pipeline_status], |r| {
            Ok(row_to_opportunity(r))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn set_opportunity_pipeline_status(&self, id: &str, status: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE opportunities SET pipeline_status=?2,updated_at=?3 WHERE id=?1",
            params![id, status, now()],
        )?;
        Ok(())
    }

    // --- Opportunity contacts --------------------------------------------

    pub fn upsert_opportunity_contact(&self, c: &OpportunityContact) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let now = now();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM opportunity_contacts WHERE opportunity_id=?1 AND contact_key=?2",
                params![c.opportunity_id, c.contact_key],
                |r| r.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| Uuid::new_v4().to_string());
        conn.execute(
            "INSERT INTO opportunity_contacts (id,opportunity_id,brand,source,contact_key,apollo_org_id,
             apollo_person_id,name,title,role,why_them,primary_contact,linkedin_url,email,email_status,
             phone,status,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)
             ON CONFLICT(opportunity_id,contact_key) DO UPDATE SET source=?4,apollo_org_id=?6,
             apollo_person_id=?7,
             name=CASE WHEN length(trim(?8))>length(trim(opportunity_contacts.name))
                       THEN ?8 ELSE opportunity_contacts.name END,
             title=CASE WHEN trim(?9)<>'' THEN ?9 ELSE opportunity_contacts.title END,
             role=?10,why_them=?11,primary_contact=?12,
             linkedin_url=CASE WHEN trim(?13)<>'' THEN ?13 ELSE opportunity_contacts.linkedin_url END,
             email=CASE WHEN trim(?14)<>'' THEN ?14 ELSE opportunity_contacts.email END,
             email_status=CASE WHEN trim(?14)<>'' THEN ?15 ELSE opportunity_contacts.email_status END,
             phone=CASE WHEN trim(?16)<>'' THEN ?16 ELSE opportunity_contacts.phone END,
             status=CASE WHEN opportunity_contacts.status IN ('replied','unsubscribed','bounced','suppressed')
                         THEN opportunity_contacts.status
                         WHEN opportunity_contacts.email_status='verified' AND trim(?14)=''
                         THEN opportunity_contacts.status ELSE ?17 END,
             updated_at=?19",
            params![
                id,
                c.opportunity_id,
                c.brand,
                c.source,
                c.contact_key,
                c.apollo_org_id,
                c.apollo_person_id,
                c.name,
                c.title,
                c.role,
                c.why_them,
                c.primary,
                c.linkedin_url,
                c.email,
                status_or(&c.email_status, "unknown"),
                c.phone,
                status_or(&c.status, "new"),
                if c.created_at.is_empty() { now.clone() } else { c.created_at.clone() },
                now,
            ],
        )?;
        conn.execute(
            "UPDATE opportunity_contacts SET location=?2,timezone=?3 WHERE id=?1",
            params![id, c.location, c.timezone],
        )?;
        Ok(id)
    }

    pub fn get_opportunity_contact(&self, id: &str) -> Result<Option<OpportunityContact>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM opportunity_contacts WHERE id=?1",
                params![id],
                |r| Ok(row_to_opportunity_contact(r)),
            )
            .optional()?)
    }

    pub fn list_opportunity_contacts(
        &self,
        opportunity_id: &str,
    ) -> Result<Vec<OpportunityContact>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM opportunity_contacts WHERE opportunity_id=?1
             ORDER BY primary_contact DESC,source='official' DESC,created_at ASC",
        )?;
        let rows = stmt.query_map(params![opportunity_id], |r| {
            Ok(row_to_opportunity_contact(r))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_opportunity_contacts_for_brand(
        &self,
        brand: &str,
    ) -> Result<Vec<OpportunityContact>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM opportunity_contacts WHERE brand=?1
             ORDER BY updated_at DESC,created_at DESC",
        )?;
        let rows = stmt.query_map(params![brand], |r| Ok(row_to_opportunity_contact(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn set_opportunity_contact_email(
        &self,
        id: &str,
        email: &str,
        email_status: &str,
        phone: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE opportunity_contacts SET email=?2,email_status=?3,phone=?4,
             status=CASE WHEN ?3='verified' THEN 'verified' ELSE 'enriched' END,updated_at=?5
             WHERE id=?1",
            params![id, email, email_status, phone, now()],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set_opportunity_contact_enrichment(
        &self,
        id: &str,
        name: &str,
        title: &str,
        location: &str,
        timezone: &str,
        linkedin_url: &str,
        email: &str,
        email_status: &str,
        phone: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE opportunity_contacts SET
             name=CASE WHEN length(trim(?2))>length(trim(name)) THEN ?2 ELSE name END,
             title=CASE WHEN trim(?3)<>'' THEN ?3 ELSE title END,
             location=CASE WHEN trim(?4)<>'' THEN ?4 ELSE location END,
             timezone=CASE WHEN trim(?5)<>'' THEN ?5 ELSE timezone END,
             linkedin_url=CASE WHEN trim(?6)<>'' THEN ?6 ELSE linkedin_url END,
             email=CASE WHEN trim(?7)<>'' THEN lower(?7) ELSE email END,
             email_status=CASE WHEN trim(?7)<>'' THEN ?8 ELSE email_status END,
             phone=CASE WHEN trim(?9)<>'' THEN ?9 ELSE phone END,
             status=CASE WHEN ?8='verified' THEN 'verified' ELSE status END,
             updated_at=?10 WHERE id=?1",
            params![
                id,
                name,
                title,
                location,
                timezone,
                linkedin_url,
                email,
                email_status,
                phone,
                now(),
            ],
        )?;
        Ok(())
    }

    pub fn opportunity_contact_by_email(
        &self,
        brand: &str,
        email: &str,
    ) -> Result<Option<OpportunityContact>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM opportunity_contacts WHERE brand=?1 AND lower(email)=lower(?2)
                 ORDER BY updated_at DESC LIMIT 1",
                params![brand, email],
                |r| Ok(row_to_opportunity_contact(r)),
            )
            .optional()?)
    }

    pub fn set_opportunity_contact_status(&self, id: &str, status: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE opportunity_contacts SET status=?2,updated_at=?3 WHERE id=?1",
            params![id, status, now()],
        )?;
        Ok(())
    }

    // --- Opportunity outreach --------------------------------------------

    pub fn insert_opportunity_touch(&self, t: &OpportunityTouch) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if t.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            t.id.clone()
        };
        conn.execute(
            "INSERT INTO opportunity_touches (id,opportunity_id,contact_id,brand,stage,day_offset,
             subject,body,purpose,goal,status,due_at,sent_at,mailbox_id,message_id,error,
             review_passes,review_issues,created_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
            params![
                id,
                t.opportunity_id,
                t.contact_id,
                t.brand,
                t.stage,
                t.day_offset,
                t.subject,
                t.body,
                t.purpose,
                t.goal,
                status_or(&t.status, "draft"),
                t.due_at,
                t.sent_at,
                t.mailbox_id,
                t.message_id,
                t.error,
                t.review_passes,
                js(&t.review_issues),
                now(),
            ],
        )?;
        conn.execute(
            "UPDATE opportunity_touches SET recipient_timezone=?2,scheduled_rule=?3,schedule_reason=?4 WHERE id=?1",
            params![
                id,
                t.recipient_timezone,
                t.scheduled_rule,
                t.schedule_reason,
            ],
        )?;
        Ok(id)
    }

    pub fn reschedule_opportunity_touch(
        &self,
        id: &str,
        due_at: &str,
        recipient_timezone: &str,
        rule: &str,
        reason: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE opportunity_touches SET due_at=?2,recipient_timezone=?3,scheduled_rule=?4,schedule_reason=?5 WHERE id=?1",
            params![id, due_at, recipient_timezone, rule, reason],
        )?;
        Ok(())
    }

    pub fn list_opportunity_touches(&self, contact_id: &str) -> Result<Vec<OpportunityTouch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT * FROM opportunity_touches WHERE contact_id=?1 ORDER BY stage ASC")?;
        let rows = stmt.query_map(params![contact_id], |r| Ok(row_to_opportunity_touch(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn due_opportunity_touches(&self, limit: i64) -> Result<Vec<OpportunityTouch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT t.* FROM opportunity_touches t
             JOIN opportunities o ON o.id=t.opportunity_id
             JOIN opportunity_contacts c ON c.id=t.contact_id
             WHERE t.status='scheduled' AND t.due_at<=?1
               AND o.pipeline_status NOT IN ('submitted','won','lost','expired')
               AND c.status NOT IN ('replied','unsubscribed','bounced','suppressed')
               AND NOT EXISTS (
                   SELECT 1 FROM opportunity_touches prior
                   WHERE prior.contact_id=t.contact_id AND prior.stage<t.stage
                     AND prior.status NOT IN ('sent','skipped','cancelled','replied')
               )
             ORDER BY t.due_at ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![now(), limit], |r| Ok(row_to_opportunity_touch(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn approve_opportunity_touches(
        &self,
        brand: Option<&str>,
        contact_id: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE opportunity_touches SET status='scheduled' WHERE status='draft'
             AND review_passes=1
             AND (?1 IS NULL OR brand=?1) AND (?2 IS NULL OR contact_id=?2)",
            params![brand, contact_id],
        )?;
        Ok(n)
    }

    pub fn set_opportunity_touch_status(
        &self,
        id: &str,
        status: &str,
        mailbox_id: &str,
        message_id: &str,
        error: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let sent_at = if status == "sent" {
            now()
        } else {
            String::new()
        };
        conn.execute(
            "UPDATE opportunity_touches SET status=?2,mailbox_id=?3,message_id=?4,error=?5,
             sent_at=CASE WHEN ?2='sent' THEN ?6 ELSE sent_at END WHERE id=?1",
            params![id, status, mailbox_id, message_id, error, sent_at],
        )?;
        Ok(())
    }

    pub fn previous_opportunity_message_id(&self, contact_id: &str, stage: i64) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT message_id FROM opportunity_touches WHERE contact_id=?1 AND stage<?2
                 AND status='sent' AND message_id<>'' ORDER BY stage DESC LIMIT 1",
                params![contact_id, stage],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or_default())
    }

    pub fn stop_opportunity_outreach(&self, contact_id: &str, touch_status: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE opportunity_touches SET status=?2 WHERE contact_id=?1
             AND status IN ('draft','scheduled')",
            params![contact_id, touch_status],
        )?;
        Ok(())
    }

    pub fn record_opportunity_reply(&self, r: &OpportunityReply) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if r.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            r.id.clone()
        };
        conn.execute(
            "INSERT INTO opportunity_replies (id,opportunity_id,contact_id,ts,from_email,subject,
             body,classification,action_taken,message_id,in_reply_to)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                id,
                r.opportunity_id,
                r.contact_id,
                now(),
                r.from_email,
                r.subject,
                r.body,
                r.classification,
                r.action_taken,
                r.message_id,
                r.in_reply_to,
            ],
        )?;
        Ok(id)
    }

    pub fn opportunity_reply_exists(&self, message_id: &str) -> Result<bool> {
        if message_id.is_empty() {
            return Ok(false);
        }
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT 1 FROM opportunity_replies WHERE message_id=?1 LIMIT 1",
                params![message_id],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some())
    }

    // --- Application briefs ----------------------------------------------

    pub fn upsert_application_brief(&self, a: &ApplicationBrief) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let now = now();
        let existing: Option<(String, String)> = conn
            .query_row(
                "SELECT id,created_at FROM opportunity_applications WHERE opportunity_id=?1",
                params![a.opportunity_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let (id, created_at) =
            existing.unwrap_or_else(|| (Uuid::new_v4().to_string(), now.clone()));
        conn.execute(
            "INSERT INTO opportunity_applications (id,opportunity_id,brand,status,eligibility_summary,
             project_shape,narrative,workplan,milestones,evidence_needed,required_documents,
             budget_questions,questions_for_funder,risks,next_steps,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)
             ON CONFLICT(opportunity_id) DO UPDATE SET status=?4,eligibility_summary=?5,
             project_shape=?6,narrative=?7,workplan=?8,milestones=?9,evidence_needed=?10,
             required_documents=?11,budget_questions=?12,questions_for_funder=?13,risks=?14,
             next_steps=?15,updated_at=?17",
            params![
                id,
                a.opportunity_id,
                a.brand,
                status_or(&a.status, "draft"),
                a.eligibility_summary,
                a.project_shape,
                a.narrative,
                js(&a.workplan),
                js(&a.milestones),
                js(&a.evidence_needed),
                js(&a.required_documents),
                js(&a.budget_questions),
                js(&a.questions_for_funder),
                js(&a.risks),
                js(&a.next_steps),
                created_at,
                now,
            ],
        )?;
        Ok(id)
    }

    pub fn get_application_brief(&self, opportunity_id: &str) -> Result<Option<ApplicationBrief>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM opportunity_applications WHERE opportunity_id=?1",
                params![opportunity_id],
                |r| Ok(row_to_application_brief(r)),
            )
            .optional()?)
    }
}

// --- Suppression -------------------------------------------------------

impl Db {
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
            params![
                Uuid::new_v4().to_string(),
                brand,
                email.trim().to_lowercase(),
                reason,
                now()
            ],
        )?;
        Ok(())
    }

    // --- Replies -----------------------------------------------------------

    pub fn record_reply(&self, r: &Reply) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if r.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            r.id.clone()
        };
        conn.execute(
            "INSERT INTO replies (id,person_id,sequence_id,ts,from_email,subject,body,\
             classification,action_taken,message_id,in_reply_to) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                id,
                r.person_id,
                r.sequence_id,
                now(),
                r.from_email,
                r.subject,
                r.body,
                r.classification,
                r.action_taken,
                r.message_id,
                r.in_reply_to
            ],
        )?;
        Ok(id)
    }

    /// Recent inbound replies (most recent first) for the CRM review view.
    pub fn list_replies(&self, limit: i64) -> Result<Vec<Reply>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT * FROM replies ORDER BY ts DESC LIMIT ?1")?;
        let rows = stmt.query_map(params![limit], |r| {
            Ok(Reply {
                id: g(r, "id"),
                person_id: g(r, "person_id"),
                sequence_id: g(r, "sequence_id"),
                ts: g(r, "ts"),
                from_email: g(r, "from_email"),
                subject: g(r, "subject"),
                body: g(r, "body"),
                classification: g(r, "classification"),
                action_taken: g(r, "action_taken"),
                message_id: g(r, "message_id"),
                in_reply_to: g(r, "in_reply_to"),
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
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
            .query_row(
                "SELECT 1 FROM replies WHERE message_id=?1 LIMIT 1",
                params![message_id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(hit.is_some())
    }

    // --- Events + metrics --------------------------------------------------

    pub fn log_event(
        &self,
        brand: &str,
        person_id: &str,
        touch_id: &str,
        kind: &str,
        detail: &str,
    ) -> Result<()> {
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
        let rows = stmt.query_map(params![brand], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Count distinct people who generated an event kind. This is used for
    /// funnel stages such as "contacted", whose current person status may later
    /// move on to replied, bounced, or unsubscribed.
    pub fn distinct_event_people(&self, brand: Option<&str>, kind: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count = conn.query_row(
            "SELECT COUNT(DISTINCT person_id) FROM events \
             WHERE kind=?2 AND person_id<>'' AND (?1 IS NULL OR brand=?1)",
            params![brand, kind],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count.max(0) as usize)
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

    /// Historical send/reply observations for calendar intelligence. A reply
    /// is attributed only to the latest sent touch in its sequence at the time
    /// of that reply, avoiding crediting every earlier follow-up.
    pub fn timing_observations(&self, brand: &str) -> Result<Vec<TimingObservation>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT l.industry,p.title,p.vantage,
                    CASE WHEN p.timezone<>'' THEN p.timezone ELSE l.timezone END,
                    t.scheduled_rule,t.sent_at,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM replies r
                        WHERE r.sequence_id=t.sequence_id AND r.ts>=t.sent_at
                          AND NOT EXISTS (
                              SELECT 1 FROM touches later
                              WHERE later.sequence_id=t.sequence_id
                                AND later.status='sent'
                                AND later.sent_at>t.sent_at AND later.sent_at<=r.ts
                          )
                    ) THEN 1 ELSE 0 END
             FROM touches t
             JOIN people p ON p.id=t.person_id
             JOIN leads l ON l.id=t.lead_id
             WHERE t.brand=?1 AND t.status='sent' AND t.sent_at<>''
             ORDER BY t.sent_at DESC",
        )?;
        let rows = stmt.query_map(params![brand], |row| {
            Ok(TimingObservation {
                industry: row.get(0)?,
                title: row.get(1)?,
                vantage: row.get(2)?,
                timezone: row.get(3)?,
                scheduled_rule: row.get(4)?,
                sent_at: row.get(5)?,
                replied: row.get::<_, i64>(6)? != 0,
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
        timezone: g(r, "timezone"),
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
        location: g(r, "location"),
        timezone: g(r, "timezone"),
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

/// The number of sends a mailbox may make on a given warmup day — the smaller of
/// its configured daily cap and a conservative ramp that protects a cold sending
/// domain's reputation. Fully warm (~3 weeks) the daily cap governs entirely.
pub fn warmup_cap(daily_cap: i64, warmup_day: i64) -> i64 {
    let ramp = match warmup_day {
        d if d <= 1 => 5,
        2 => 8,
        3 => 12,
        4 => 16,
        5 => 20,
        6 => 25,
        d if d < 10 => 30,
        d if d < 14 => 40,
        d if d < 21 => 60,
        _ => i64::MAX, // warm: the daily cap alone governs
    };
    daily_cap.min(ramp)
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
        recipient_timezone: g(r, "recipient_timezone"),
        scheduled_rule: g(r, "scheduled_rule"),
        schedule_reason: g(r, "schedule_reason"),
        review_passes: r.get::<_, Option<bool>>("review_passes").unwrap_or(None),
        review_issues: jd(&g(r, "review_issues")),
        created_at: g(r, "created_at"),
    }
}

#[allow(dead_code)]
fn row_to_opportunity(r: &Row) -> Opportunity {
    Opportunity {
        id: g(r, "id"),
        brand: g(r, "brand"),
        kind: g(r, "kind"),
        fingerprint: g(r, "fingerprint"),
        source_name: g(r, "source_name"),
        source_url: g(r, "source_url"),
        canonical_url: g(r, "canonical_url"),
        title: g(r, "title"),
        funder: g(r, "funder"),
        funder_domain: g(r, "funder_domain"),
        summary: g(r, "summary"),
        geography: g(r, "geography"),
        opportunity_status: g(r, "opportunity_status"),
        opens_at: g(r, "opens_at"),
        deadline: g(r, "deadline"),
        deadline_timezone: g(r, "deadline_timezone"),
        funding_type: g(r, "funding_type"),
        amount_min: g(r, "amount_min"),
        amount_max: g(r, "amount_max"),
        currency: g(r, "currency"),
        cost_share: g(r, "cost_share"),
        eligible_applicants: jd(&g(r, "eligible_applicants")),
        eligible_activities: jd(&g(r, "eligible_activities")),
        ineligible_activities: jd(&g(r, "ineligible_activities")),
        themes: jd(&g(r, "themes")),
        official_contact_name: g(r, "official_contact_name"),
        official_contact_email: g(r, "official_contact_email"),
        official_contact_phone: g(r, "official_contact_phone"),
        evidence: jd(&g(r, "evidence")),
        documents: jd(&g(r, "documents")),
        fit_score: r.get("fit_score").unwrap_or(0),
        fit_status: g(r, "fit_status"),
        fit_reasons: jd(&g(r, "fit_reasons")),
        blockers: jd(&g(r, "blockers")),
        unknowns: jd(&g(r, "unknowns")),
        next_action: g(r, "next_action"),
        pipeline_status: g(r, "pipeline_status"),
        raw_snapshot: g(r, "raw_snapshot"),
        first_seen_at: g(r, "first_seen_at"),
        last_verified_at: g(r, "last_verified_at"),
        updated_at: g(r, "updated_at"),
    }
}

#[allow(dead_code)]
fn row_to_opportunity_contact(r: &Row) -> OpportunityContact {
    OpportunityContact {
        id: g(r, "id"),
        opportunity_id: g(r, "opportunity_id"),
        brand: g(r, "brand"),
        source: g(r, "source"),
        contact_key: g(r, "contact_key"),
        apollo_org_id: g(r, "apollo_org_id"),
        apollo_person_id: g(r, "apollo_person_id"),
        name: g(r, "name"),
        title: g(r, "title"),
        location: g(r, "location"),
        timezone: g(r, "timezone"),
        role: g(r, "role"),
        why_them: g(r, "why_them"),
        primary: r.get::<_, i64>("primary_contact").unwrap_or(0) != 0,
        linkedin_url: g(r, "linkedin_url"),
        email: g(r, "email"),
        email_status: g(r, "email_status"),
        phone: g(r, "phone"),
        status: g(r, "status"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

#[allow(dead_code)]
fn row_to_opportunity_touch(r: &Row) -> OpportunityTouch {
    OpportunityTouch {
        id: g(r, "id"),
        opportunity_id: g(r, "opportunity_id"),
        contact_id: g(r, "contact_id"),
        brand: g(r, "brand"),
        stage: r.get("stage").unwrap_or(0),
        day_offset: r.get("day_offset").unwrap_or(0),
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
        recipient_timezone: g(r, "recipient_timezone"),
        scheduled_rule: g(r, "scheduled_rule"),
        schedule_reason: g(r, "schedule_reason"),
        review_passes: r.get::<_, Option<bool>>("review_passes").unwrap_or(None),
        review_issues: jd(&g(r, "review_issues")),
        created_at: g(r, "created_at"),
    }
}

#[allow(dead_code)]
fn row_to_application_brief(r: &Row) -> ApplicationBrief {
    ApplicationBrief {
        id: g(r, "id"),
        opportunity_id: g(r, "opportunity_id"),
        brand: g(r, "brand"),
        status: g(r, "status"),
        eligibility_summary: g(r, "eligibility_summary"),
        project_shape: g(r, "project_shape"),
        narrative: g(r, "narrative"),
        workplan: jd(&g(r, "workplan")),
        milestones: jd(&g(r, "milestones")),
        evidence_needed: jd(&g(r, "evidence_needed")),
        required_documents: jd(&g(r, "required_documents")),
        budget_questions: jd(&g(r, "budget_questions")),
        questions_for_funder: jd(&g(r, "questions_for_funder")),
        risks: jd(&g(r, "risks")),
        next_steps: jd(&g(r, "next_steps")),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

// --- helpers ---------------------------------------------------------------

/// Column getter that tolerates NULL/missing by returning an empty string.
fn g(r: &Row, col: &str) -> String {
    r.get::<_, Option<String>>(col)
        .unwrap_or(None)
        .unwrap_or_default()
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
    if s.trim().is_empty() {
        default.to_string()
    } else {
        s.to_string()
    }
}

fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !names.iter().any(|name| name == column) {
        conn.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition}"
        ))?;
    }
    Ok(())
}

pub fn now() -> String {
    Utc::now().to_rfc3339()
}

fn now_date() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS leads (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    apollo_org_id TEXT NOT NULL,
    name TEXT, domain TEXT, industry TEXT, hq TEXT, timezone TEXT DEFAULT '',
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
    location TEXT DEFAULT '', timezone TEXT DEFAULT '',
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
    recipient_timezone TEXT DEFAULT '', scheduled_rule TEXT DEFAULT '',
    schedule_reason TEXT DEFAULT '',
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
CREATE TABLE IF NOT EXISTS opportunities (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    kind TEXT DEFAULT 'funding',
    fingerprint TEXT NOT NULL,
    source_name TEXT, source_url TEXT, canonical_url TEXT,
    title TEXT, funder TEXT, funder_domain TEXT, summary TEXT, geography TEXT,
    opportunity_status TEXT DEFAULT 'unknown', opens_at TEXT, deadline TEXT,
    deadline_timezone TEXT, funding_type TEXT, amount_min TEXT, amount_max TEXT,
    currency TEXT, cost_share TEXT,
    eligible_applicants TEXT, eligible_activities TEXT, ineligible_activities TEXT,
    themes TEXT, official_contact_name TEXT, official_contact_email TEXT,
    official_contact_phone TEXT, evidence TEXT, documents TEXT,
    fit_score INTEGER DEFAULT 0, fit_status TEXT DEFAULT 'needs_information',
    fit_reasons TEXT, blockers TEXT, unknowns TEXT, next_action TEXT,
    pipeline_status TEXT DEFAULT 'discovered', raw_snapshot TEXT,
    first_seen_at TEXT, last_verified_at TEXT, updated_at TEXT,
    UNIQUE(brand, fingerprint)
);
CREATE TABLE IF NOT EXISTS opportunity_contacts (
    id TEXT PRIMARY KEY,
    opportunity_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    source TEXT, contact_key TEXT NOT NULL,
    apollo_org_id TEXT, apollo_person_id TEXT,
    name TEXT, title TEXT, location TEXT DEFAULT '', timezone TEXT DEFAULT '',
    role TEXT, why_them TEXT,
    primary_contact INTEGER DEFAULT 0, linkedin_url TEXT,
    email TEXT, email_status TEXT DEFAULT 'unknown', phone TEXT,
    status TEXT DEFAULT 'new', created_at TEXT, updated_at TEXT,
    UNIQUE(opportunity_id, contact_key),
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id)
);
CREATE TABLE IF NOT EXISTS opportunity_touches (
    id TEXT PRIMARY KEY,
    opportunity_id TEXT NOT NULL,
    contact_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    stage INTEGER, day_offset INTEGER,
    subject TEXT, body TEXT, purpose TEXT, goal TEXT,
    status TEXT DEFAULT 'draft', due_at TEXT, sent_at TEXT,
    mailbox_id TEXT, message_id TEXT, error TEXT,
    recipient_timezone TEXT DEFAULT '', scheduled_rule TEXT DEFAULT '',
    schedule_reason TEXT DEFAULT '',
    review_passes INTEGER, review_issues TEXT, created_at TEXT,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id),
    FOREIGN KEY(contact_id) REFERENCES opportunity_contacts(id)
);
CREATE TABLE IF NOT EXISTS opportunity_replies (
    id TEXT PRIMARY KEY,
    opportunity_id TEXT NOT NULL, contact_id TEXT NOT NULL, ts TEXT,
    from_email TEXT, subject TEXT, body TEXT,
    classification TEXT, action_taken TEXT, message_id TEXT, in_reply_to TEXT,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id),
    FOREIGN KEY(contact_id) REFERENCES opportunity_contacts(id)
);
CREATE TABLE IF NOT EXISTS opportunity_applications (
    id TEXT PRIMARY KEY,
    opportunity_id TEXT NOT NULL UNIQUE,
    brand TEXT NOT NULL, status TEXT DEFAULT 'draft',
    eligibility_summary TEXT, project_shape TEXT, narrative TEXT,
    workplan TEXT, milestones TEXT, evidence_needed TEXT, required_documents TEXT,
    budget_questions TEXT, questions_for_funder TEXT, risks TEXT, next_steps TEXT,
    created_at TEXT, updated_at TEXT,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id)
);
CREATE INDEX IF NOT EXISTS idx_touches_due ON touches(status, due_at);
CREATE INDEX IF NOT EXISTS idx_touches_person ON touches(person_id);
CREATE INDEX IF NOT EXISTS idx_people_brand_email ON people(brand, email);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_replies_msgid ON replies(message_id);
CREATE INDEX IF NOT EXISTS idx_opportunities_brand_status
    ON opportunities(brand, pipeline_status, opportunity_status, fit_score);
CREATE INDEX IF NOT EXISTS idx_opportunity_contacts_email
    ON opportunity_contacts(brand, email);
CREATE INDEX IF NOT EXISTS idx_opportunity_touches_due
    ON opportunity_touches(status, due_at);
CREATE INDEX IF NOT EXISTS idx_opportunity_replies_msgid
    ON opportunity_replies(message_id);
"#;

#[cfg(test)]
mod tests {
    use super::{
        ApplicationBrief, Db, Lead, Mailbox, Opportunity, OpportunityContact, OpportunityTouch,
        Person, Sequence, Touch,
    };
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn remove_temp_db(path: &std::path::Path) {
        for candidate in [
            path.to_path_buf(),
            std::path::PathBuf::from(format!("{}-wal", path.display())),
            std::path::PathBuf::from(format!("{}-shm", path.display())),
        ] {
            let _ = std::fs::remove_file(candidate);
        }
    }

    #[test]
    fn warmup_ramp_limits_cold_domains_then_yields_to_daily_cap() {
        use super::warmup_cap;
        // Cold: the ramp binds well below the configured cap.
        assert_eq!(warmup_cap(30, 0), 5);
        assert_eq!(warmup_cap(30, 2), 8);
        assert_eq!(warmup_cap(30, 6), 25);
        // Warm enough: the daily cap governs.
        assert_eq!(warmup_cap(30, 7), 30);
        assert_eq!(warmup_cap(30, 100), 30);
        // A low daily cap is never exceeded by the ramp.
        assert_eq!(warmup_cap(15, 5), 15);
        assert_eq!(warmup_cap(15, 100), 15);
    }

    #[test]
    fn mailbox_preview_treats_stale_capacity_as_fresh_without_mutating_it() {
        let path = std::env::temp_dir().join(format!(
            "spruce-mailbox-preview-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Db::open(&path).expect("open temp db");
        db.upsert_mailbox(&Mailbox {
            brand: "gnk".into(),
            from_email: "sender@example.com".into(),
            daily_cap: 30,
            sent_today: 30,
            active: true,
            ..Default::default()
        })
        .expect("insert mailbox");
        {
            let conn = db.conn.lock().expect("lock db");
            conn.execute(
                "UPDATE mailboxes SET sent_today=30,last_reset='2000-01-01'",
                [],
            )
            .expect("make counter stale");
        }

        let selected = db
            .preview_mailbox("gnk")
            .expect("preview mailbox")
            .expect("mailbox has effective capacity");
        assert_eq!(selected.from_email, "sender@example.com");

        let persisted = db.list_mailboxes(Some("gnk")).expect("list mailboxes");
        assert_eq!(persisted[0].sent_today, 30);
        assert_eq!(persisted[0].last_reset, "2000-01-01");

        drop(persisted);
        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn manual_tasks_do_not_block_later_email_touches() {
        let path = std::env::temp_dir().join(format!(
            "spruce-manual-touch-gating-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Db::open(&path).expect("open temp db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "org-gating".into(),
                ..Default::default()
            })
            .expect("insert lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "person-gating".into(),
                email: "person@example.com".into(),
                email_status: "verified".into(),
                status: "verified".into(),
                ..Default::default()
            })
            .expect("insert person");
        let sequence_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                status: "active".into(),
                ..Default::default()
            })
            .expect("insert sequence");
        let mut touch_ids = Vec::new();
        for (stage, channel, status) in [
            (1, "email", "scheduled"),
            (2, "linkedin", "draft"),
            (3, "email", "scheduled"),
        ] {
            touch_ids.push(
                db.insert_touch(&Touch {
                    sequence_id: sequence_id.clone(),
                    person_id: person_id.clone(),
                    lead_id: lead_id.clone(),
                    brand: "gnk".into(),
                    stage,
                    channel: channel.into(),
                    status: status.into(),
                    due_at: "2000-01-01T00:00:00Z".into(),
                    ..Default::default()
                })
                .expect("insert touch"),
            );
        }

        let due = db.due_touches(Some("gnk"), 10).expect("first due query");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].stage, 1);
        db.set_touch_status(&touch_ids[0], "sent", "mailbox", "message", "")
            .expect("complete first email");

        let due = db.due_touches(Some("gnk"), 10).expect("second due query");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].stage, 3);

        drop(due);
        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn opportunity_records_round_trip_through_sqlite() {
        let path = std::env::temp_dir().join(format!(
            "spruce-opportunity-round-trip-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Db::open(&path).expect("open temp db");

        let opportunity_id = db
            .upsert_opportunity(&Opportunity {
                brand: "outagehub".into(),
                kind: "funding".into(),
                fingerprint: "example-fund-2026".into(),
                title: "Example Resilience Fund".into(),
                canonical_url: "https://example.org/resilience-fund".into(),
                opportunity_status: "open".into(),
                eligible_applicants: vec!["UK SMEs".into()],
                themes: vec!["resilience".into()],
                fit_score: 87,
                fit_status: "strong_fit".into(),
                pipeline_status: "shortlisted".into(),
                ..Default::default()
            })
            .expect("upsert opportunity");
        let opportunity = db
            .get_opportunity(&opportunity_id)
            .expect("read opportunity")
            .expect("opportunity exists");
        assert_eq!(opportunity.title, "Example Resilience Fund");
        assert_eq!(opportunity.eligible_applicants, vec!["UK SMEs"]);
        assert_eq!(opportunity.fit_score, 87);

        let same_url_id = db
            .upsert_opportunity(&Opportunity {
                brand: "outagehub".into(),
                fingerprint: "different-model-wording".into(),
                canonical_url: opportunity.canonical_url.clone(),
                title: "A differently worded title".into(),
                ..opportunity.clone()
            })
            .expect("canonical URL upsert");
        assert_eq!(same_url_id, opportunity_id);

        let contact_id = db
            .upsert_opportunity_contact(&OpportunityContact {
                opportunity_id: opportunity_id.clone(),
                brand: "outagehub".into(),
                source: "official".into(),
                contact_key: "grants@example.org".into(),
                name: "Grant Team".into(),
                email: "grants@example.org".into(),
                email_status: "verified".into(),
                primary: true,
                ..Default::default()
            })
            .expect("upsert opportunity contact");
        let contacts = db
            .list_opportunity_contacts(&opportunity_id)
            .expect("list opportunity contacts");
        assert_eq!(contacts.len(), 1);
        assert!(contacts[0].primary);

        db.insert_opportunity_touch(&OpportunityTouch {
            opportunity_id: opportunity_id.clone(),
            contact_id: contact_id.clone(),
            brand: "outagehub".into(),
            stage: 1,
            subject: "Eligibility question".into(),
            body: "Could you clarify the applicant criteria?".into(),
            status: "draft".into(),
            review_passes: Some(true),
            review_issues: vec!["none".into()],
            ..Default::default()
        })
        .expect("insert opportunity touch");
        db.insert_opportunity_touch(&OpportunityTouch {
            opportunity_id: opportunity_id.clone(),
            contact_id: contact_id.clone(),
            brand: "outagehub".into(),
            stage: 2,
            subject: "Unsafe follow-up".into(),
            body: "A draft that failed review.".into(),
            status: "draft".into(),
            review_passes: Some(false),
            review_issues: vec!["forbidden claim".into()],
            ..Default::default()
        })
        .expect("insert failed-review opportunity touch");
        assert_eq!(
            db.approve_opportunity_touches(Some("outagehub"), Some(&contact_id))
                .expect("approve clean funding touch"),
            1
        );
        let touches = db
            .list_opportunity_touches(&contact_id)
            .expect("list opportunity touches");
        assert_eq!(touches.len(), 2);
        assert_eq!(touches[0].status, "scheduled");
        assert_eq!(touches[0].review_issues, vec!["none"]);
        assert_eq!(touches[1].status, "draft");
        assert_eq!(touches[1].review_issues, vec!["forbidden claim"]);

        let due = db
            .due_opportunity_touches(10)
            .expect("query due funding touches");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].stage, 1);

        db.upsert_application_brief(&ApplicationBrief {
            opportunity_id: opportunity_id.clone(),
            brand: "outagehub".into(),
            status: "draft".into(),
            eligibility_summary: "OutageHub appears eligible.".into(),
            workplan: vec!["Confirm eligibility".into()],
            risks: vec!["Deadline".into()],
            ..Default::default()
        })
        .expect("upsert application brief");
        let application = db
            .get_application_brief(&opportunity_id)
            .expect("read application brief")
            .expect("application exists");
        assert_eq!(application.workplan, vec!["Confirm eligibility"]);
        assert_eq!(application.risks, vec!["Deadline"]);

        drop(application);
        drop(touches);
        drop(contacts);
        drop(opportunity);
        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn planned_capacity_is_isolated_per_business() {
        let db = Db::open(":memory:").expect("open memory db");
        let due = Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 8, 11, 0, 0, 0).unwrap();
        for index in 0..30 {
            db.insert_touch(&Touch {
                id: format!("gnk-{index}"),
                brand: "gnk".into(),
                status: if index == 0 { "sent" } else { "draft" }.into(),
                channel: "email".into(),
                due_at: due.to_rfc3339(),
                ..Default::default()
            })
            .expect("insert gnk touch");
        }
        db.insert_touch(&Touch {
            id: "wapahki-1".into(),
            brand: "wapahki".into(),
            status: "draft".into(),
            channel: "call".into(),
            due_at: due.to_rfc3339(),
            ..Default::default()
        })
        .expect("insert wapahki touch");

        assert_eq!(
            db.planned_touch_count_between("gnk", start, end).unwrap(),
            30
        );
        assert_eq!(
            db.planned_touch_count_between("wapahki", start, end)
                .unwrap(),
            1
        );
        assert_eq!(
            db.planned_touch_count_between("outagehub", start, end)
                .unwrap(),
            0
        );
    }
}
