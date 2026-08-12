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

use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Increment when the buyer-facing copy contract changes materially. The CRM
/// only presents sequences approved under the current policy.
// v16 raises the GnK account threshold from plausible workflow fit to a sourced
// recurring decision + consequence + trigger/mechanism, and changes OutageHub
// cold outreach to an evidence-first two-email EV-operator experiment. Older
// copy did not receive those checks and must never remain sendable.
// v22 turns account readiness into an easy/medium/hard queue. Easy accounts may
// use the supported cadence; medium accounts may receive one complete,
// hypothesis-led first email when a concrete task/decision and relevant person
// are supported; hard accounts stay in research. This is not v19's generic
// one-question lane: every T1 must explain seller value, a role-specific reason
// to engage, and an answerable response path, then pass independent QA. The
// mechanical gate checks factual contribution, not subjective CTA wording.
// v23 recognizes quantified or explicitly repetitive lifting as a concrete
// Wapahki operating consequence; v22 could reject an evidence-backed ergonomic
// wedge merely because the writer did not use the literal word "ergonomic".
// v24 recognizes semantically equivalent email-first/call response paths rather
// than requiring one exact CTA phrase. Independent review still decides whether
// the recipient has enough value and context to justify that response.
// v25 also recognizes an answerable offer to send the real one-page fit screen
// as a response path, preventing semantic edits toward recipient value from
// oscillating against the mechanical CTA vocabulary.
// v26 recognizes natural reply-here/discuss-briefly phrasing observed in the
// independent editor's output; the prior literal marker list rejected it.
// v27 removes the contradictory Wapahki call + email + asset CTA stack. An
// unconfirmed workflow now gets one email-first response path; calls are earned.
// v28 keeps the verified fit-screen fact in reviewer context and recognizes
// natural brief-reply wording that v27's literal response detector missed.
// v29 restores the higher easy-tier fit floor after live review showed that a
// task signal plus an unrelated lifting condition still leaves the recipient
// doing seller discovery. Those accounts remain medium until the consequence
// is tied to the candidate task.
// v30 changes the commercial unit from a brand-exclusive company hypothesis to
// an evidence-linked opportunity. A company may belong to several portfolio
// brands; copy is attributable to one facility/use case and one mapped buying
// committee while only one cold thread is active at a time.
pub const CURRENT_COPY_POLICY_VERSION: i64 = 31;

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
    /// unknown | requested | connected | not_connected. This is operator-kept
    /// state; Spruce Leaf has no authority to inspect LinkedIn connections.
    pub linkedin_status: String,
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
    /// Knowledge-library principle ids applied to the buyer-facing copy.
    pub applied_principles: Vec<String>,
    /// Exact GTM play/version and evidence used when this action was composed.
    pub play_id: String,
    pub play_version: i64,
    pub experiment_id: String,
    pub experiment_arm: String,
    pub experiment_assignment_id: String,
    pub signal_observation_ids: Vec<String>,
    /// The facility/use-case opportunity this copy was written for. Empty only
    /// on legacy rows created before copy-policy v30.
    pub sales_opportunity_id: String,
    /// research_required | discovery_ready | action_ready | no_play
    pub gtm_state: String,
    pub copy_policy_version: i64,
    /// Exact generation lane used for this persisted copy. These snapshots make
    /// reply outcomes attributable even after the operator changes backends.
    pub generation_backend: String,
    pub generation_model: String,
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

/// One accumulated lesson about a brand's outbound — a company we skipped and
/// why, an outreach angle that keeps failing — persisted so the funnel improves
/// over time instead of relearning the same thing every run.
#[derive(Debug, Clone, Default)]
pub struct Learning {
    pub brand: String,
    /// qualification_skip | outreach_failure | ...
    pub kind: String,
    pub subject: String,
    pub detail: String,
    /// How many times we've observed this — a high count is a strong pattern.
    pub hits: i64,
    pub updated_at: String,
}

/// Canonical definition for an observable GTM signal. The definition carries
/// ownership, freshness, lineage requirements, and a schema version; individual
/// account observations live separately and may expire without deleting history.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SignalDefinition {
    pub id: String,
    pub brand: String,
    pub key: String,
    pub name: String,
    pub description: String,
    pub topic: String,
    pub entity_type: String,
    pub value_type: String,
    pub source_kind: String,
    pub owner: String,
    pub refresh_cadence: String,
    pub freshness_seconds: i64,
    pub evidence_required: bool,
    pub minimum_confidence: f64,
    pub version: i64,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SignalObservation {
    pub id: String,
    pub definition_id: String,
    pub definition_key: String,
    pub brand: String,
    pub lead_id: String,
    pub person_id: String,
    pub conversation_id: String,
    pub source_name: String,
    pub source_url: String,
    pub provider_key: String,
    pub value_json: String,
    pub evidence: String,
    pub confidence: f64,
    pub observed_at: String,
    pub expires_at: String,
    /// observed | verified | rejected | expired
    pub status: String,
    pub fingerprint: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GtmPlay {
    pub id: String,
    pub brand: String,
    pub key: String,
    pub version: i64,
    pub name: String,
    /// candidate | testing | proven | retired
    pub lifecycle: String,
    pub motion: String,
    pub target_icp: String,
    pub target_vantages: Vec<String>,
    pub required_signal_keys: Vec<String>,
    pub minimum_signal_matches: i64,
    pub hypothesis: String,
    pub action_policy: String,
    pub proof_type: String,
    pub proof_description: String,
    pub success_metric: String,
    pub kill_condition: String,
    pub source_refs: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccountPlayAssessment {
    pub id: String,
    pub lead_id: String,
    pub brand: String,
    pub play_id: String,
    pub play_version: i64,
    /// qualified | research_needed | research_required | rejected
    pub status: String,
    pub fit_score: i64,
    pub matched_signal_keys: Vec<String>,
    pub symptom: String,
    pub root_cause: String,
    pub current_workaround: String,
    pub why_now: String,
    pub proof_fit: String,
    pub evidence_gaps: Vec<String>,
    pub disqualifiers: Vec<String>,
    pub source: String,
    pub created_at: String,
    pub updated_at: String,
}

/// Canonical company identity shared by every Spruce Leaf brand. `Lead` remains
/// a brand-specific working record during the migration, but it no longer owns
/// the company or excludes another brand from pursuing a different use case.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MarketAccount {
    pub id: String,
    pub identity_key: String,
    pub canonical_domain: String,
    pub apollo_org_id: String,
    pub name: String,
    pub industry: String,
    pub hq: String,
    pub created_at: String,
    pub updated_at: String,
}

/// A bounded, enumerable market wedge. Coverage belongs here rather than being
/// inferred from how many rows a single Apollo run happened to return.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MarketSegment {
    pub id: String,
    pub brand: String,
    pub key: String,
    pub version: i64,
    pub name: String,
    pub geography: String,
    pub wedge: String,
    pub unit_of_analysis: String,
    pub enumeration_sources: Vec<String>,
    pub status: String,
    pub estimated_total: i64,
    pub accounts_discovered: i64,
    pub accounts_with_opportunities: i64,
    pub source_exhausted: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// One source cursor within one coverage run. A run is complete only when each
/// declared source is exhausted or carries an explicit gap/reason.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CoverageRun {
    pub id: String,
    pub segment_id: String,
    pub brand: String,
    pub source_name: String,
    pub query_fingerprint: String,
    pub cursor: String,
    pub pages_examined: i64,
    pub candidates_seen: i64,
    pub accounts_added: i64,
    pub status: String,
    pub exhausted: bool,
    pub gap_reason: String,
    pub started_at: String,
    pub completed_at: String,
    pub updated_at: String,
}

/// A physical operating site. A company can own many facilities and one
/// facility can contain many separately qualified tasks.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Facility {
    pub id: String,
    pub market_account_id: String,
    pub name: String,
    pub facility_type: String,
    pub address: String,
    pub city: String,
    pub region: String,
    pub country: String,
    pub source_url: String,
    pub source_excerpt: String,
    pub confidence: f64,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

/// The actual customer-sales object: one problem/task/decision at one account
/// (and, when physical, one facility). Several opportunities may coexist at a
/// company and may belong to different Spruce Leaf brands.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SalesOpportunity {
    pub id: String,
    pub brand: String,
    pub market_account_id: String,
    pub lead_id: String,
    pub segment_id: String,
    pub facility_id: String,
    pub play_id: String,
    pub kind: String,
    pub title: String,
    pub task_or_decision: String,
    pub mechanism: String,
    pub consequence: String,
    pub system_concept: String,
    pub proof_offer: String,
    pub evidence_status: String,
    pub priority_tier: String,
    pub fit_score: i64,
    pub status: String,
    pub evidence_gaps: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// One atomic, auditable claim. `source_excerpt` is the exact supporting
/// passage; `independence_group` prevents two pages on one company site from
/// masquerading as two independent evidence sources.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EvidenceClaim {
    pub id: String,
    pub sales_opportunity_id: String,
    pub brand: String,
    pub lead_id: String,
    pub facility_id: String,
    pub claim_type: String,
    pub claim_text: String,
    pub source_url: String,
    pub source_title: String,
    pub source_excerpt: String,
    pub source_locator: String,
    pub source_domain: String,
    pub lineage_key: String,
    pub independence_group: String,
    pub confidence: f64,
    pub status: String,
    pub observed_at: String,
    pub created_at: String,
    pub updated_at: String,
}

/// A buying-committee role around one opportunity. People are mapped before
/// outreach; `active_thread` is unique per opportunity so committee coverage
/// never becomes simultaneous cold blasting.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpportunityStakeholder {
    pub id: String,
    pub sales_opportunity_id: String,
    pub person_id: String,
    pub role: String,
    pub relationship_to_task: String,
    pub can_observe: String,
    pub can_decide: String,
    pub priority: i64,
    pub active_thread: bool,
    pub status: String,
    pub source: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GtmExperiment {
    pub id: String,
    pub brand: String,
    pub play_id: String,
    pub name: String,
    /// list_only | copy_only | combined
    pub experiment_type: String,
    pub hypothesis: String,
    pub variable: String,
    pub constants: Vec<String>,
    pub control_description: String,
    pub variant_description: String,
    pub minimum_sends_per_arm: i64,
    pub baseline_sends: i64,
    pub baseline_positive_reply_rate: f64,
    pub success_target: f64,
    pub failure_floor: f64,
    pub measurement_days: i64,
    /// draft | running | measuring | complete | inconclusive | cancelled
    pub status: String,
    pub starts_at: String,
    pub ends_at: String,
    pub result_json: String,
    pub confidence: String,
    pub decision: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExperimentAssignment {
    pub id: String,
    pub experiment_id: String,
    pub lead_id: String,
    pub person_id: String,
    pub sequence_id: String,
    pub arm: String,
    pub assigned_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GtmOutcome {
    pub id: String,
    pub brand: String,
    pub kind: String,
    pub lead_id: String,
    pub person_id: String,
    pub sequence_id: String,
    pub conversation_id: String,
    pub play_id: String,
    pub experiment_id: String,
    pub experiment_assignment_id: String,
    pub signal_observation_ids: Vec<String>,
    pub touch_id: String,
    pub touch_stage: i64,
    pub contact_title: String,
    pub contact_vantage: String,
    pub account_hypothesis: String,
    pub play_version: i64,
    pub experiment_arm: String,
    pub copy_policy_version: i64,
    pub generation_backend: String,
    pub generation_model: String,
    pub value: f64,
    pub detail: String,
    pub source: String,
    pub fingerprint: String,
    pub occurred_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProofBrief {
    pub id: String,
    pub brand: String,
    pub lead_id: String,
    pub person_id: String,
    pub conversation_id: String,
    pub play_id: String,
    /// draft | ready | approved | running | passed | failed | withdrawn
    pub status: String,
    pub problem: String,
    pub current_workflow: String,
    pub evidence_available: Vec<String>,
    pub scope: String,
    pub customer_data: Vec<String>,
    pub success_metric: String,
    pub baseline: String,
    pub target: String,
    pub stop_condition: String,
    pub stakeholders: Vec<String>,
    pub owner: String,
    pub expansion_path: String,
    pub result: String,
    pub learnings: Vec<String>,
    pub approved_at: String,
    pub created_at: String,
    pub updated_at: String,
}

/// One account-level customer-development thread. This is deliberately
/// separate from a `ProofBrief`: discovery starts before a proof exists, and a
/// friendly reply is not the same thing as evidence, an evaluation agreement,
/// an LOI, or revenue. `stage` is derived from the recorded evidence and the
/// highest explicit commitment rather than advanced by email activity.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CustomerDevelopmentRecord {
    pub id: String,
    pub brand: String,
    pub lead_id: String,
    pub person_id: String,
    pub conversation_id: String,
    pub stage: String,
    pub problem: String,
    pub task_scope: String,
    pub site: String,
    pub current_workflow: String,
    pub why_manual: String,
    pub variations: Vec<String>,
    pub exceptions: Vec<String>,
    pub evidence: Vec<String>,
    pub economics: String,
    pub success_criteria: String,
    pub stop_condition: String,
    pub stakeholders: Vec<String>,
    /// none | evaluation_agreed | design_partner | loi_candidate |
    /// conditional_loi | paid_pilot | deployment
    pub commitment_kind: String,
    pub commitment_detail: String,
    pub quantity: String,
    pub commercial_case: String,
    pub timeline: String,
    pub loi_conditions: String,
    pub next_action: String,
    pub engaged_at: String,
    pub source: String,
    pub created_at: String,
    pub updated_at: String,
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
    pub brand: String,
    pub due_at: String,
    pub stage: i64,
    pub channel: String,
    pub status: String,
    pub recipient: String,
    pub account: String,
    pub purpose: String,
    pub recipient_timezone: String,
    pub scheduled_rule: String,
    pub motion: String,
}

/// One deterministic calendar assignment produced by the portfolio scheduler.
/// Keeping the write shape in the database layer lets the scheduler calculate
/// every placement first and then commit the complete plan atomically.
#[derive(Debug, Clone, Default)]
pub struct TouchScheduleUpdate {
    pub id: String,
    pub due_at: String,
    pub recipient_timezone: String,
    pub scheduled_rule: String,
    pub schedule_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Reply {
    pub id: String,
    pub conversation_id: String,
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

/// Durable identity for one sales email thread. A conversation remains tied to
/// the originally researched account/person even when a referral replies from a
/// new address on CC, which is why inbound matching cannot rely on `From:` alone.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Conversation {
    pub id: String,
    pub brand: String,
    pub sequence_id: String,
    pub person_id: String,
    pub lead_id: String,
    pub subject: String,
    pub status: String,
    pub last_message_at: String,
    pub created_at: String,
    pub updated_at: String,
}

/// One inbound or outbound message in a conversation. Outbound reply-agent
/// drafts use the same draft → scheduled → sent state machine as cold touches,
/// but stay separate so a human reply never restarts the stopped cold cadence.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConversationMessage {
    pub id: String,
    pub conversation_id: String,
    pub direction: String,
    pub sender_email: String,
    pub recipient_email: String,
    pub participants: Vec<String>,
    pub subject: String,
    pub body: String,
    pub status: String,
    pub message_id: String,
    pub in_reply_to: String,
    pub references: Vec<String>,
    pub classification: String,
    pub action: String,
    /// RFC3339 candidates included in this exact outbound draft. They become
    /// bookable only after this message reaches `sent`.
    pub offered_slots: Vec<String>,
    pub mailbox_id: String,
    pub sent_at: String,
    pub created_at: String,
}

/// A meeting booked from an explicitly accepted, previously-sent slot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Meeting {
    pub id: String,
    pub conversation_id: String,
    pub brand: String,
    pub person_id: String,
    pub attendee_email: String,
    pub starts_at: String,
    pub ends_at: String,
    pub timezone: String,
    pub status: String,
    pub google_event_id: String,
    pub html_link: String,
    pub meet_link: String,
    pub created_at: String,
    pub updated_at: String,
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
        let db = Arc::new(db);
        crate::gtm::seed_defaults(&db)?;
        db.backfill_market_opportunity_model()?;
        Ok(db)
    }

    fn backfill_market_opportunity_model(&self) -> Result<()> {
        // Re-upserting uses the governed domain/Apollo identity hierarchy and
        // creates one brand membership per existing legacy lead.
        for lead in self.list_leads(None)? {
            self.upsert_lead(&lead)?;
        }
        // Signals are already durable; assessments turn them into opportunity
        // claims without upgrading any routing status.
        for assessment in self.list_account_play_assessments(None)? {
            self.materialize_sales_opportunity(&assessment)?;
        }
        // Contact rows then populate the full committee map. They remain
        // inactive until the planner selects one cold thread.
        for person in self.list_people(None, None)? {
            self.upsert_person(&person)?;
        }
        Ok(())
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(SCHEMA)
            .context("running SQLite migrations")?;
        for (table, column, definition) in [
            ("leads", "timezone", "TEXT DEFAULT ''"),
            ("people", "location", "TEXT DEFAULT ''"),
            ("people", "timezone", "TEXT DEFAULT ''"),
            ("people", "linkedin_status", "TEXT DEFAULT 'unknown'"),
            ("sequences", "applied_principles", "TEXT DEFAULT '[]'"),
            ("sequences", "play_id", "TEXT DEFAULT ''"),
            ("sequences", "play_version", "INTEGER DEFAULT 0"),
            ("sequences", "experiment_id", "TEXT DEFAULT ''"),
            ("sequences", "experiment_arm", "TEXT DEFAULT ''"),
            ("sequences", "experiment_assignment_id", "TEXT DEFAULT ''"),
            ("sequences", "signal_observation_ids", "TEXT DEFAULT '[]'"),
            ("sequences", "gtm_state", "TEXT DEFAULT ''"),
            ("sequences", "copy_policy_version", "INTEGER DEFAULT 0"),
            ("sequences", "generation_backend", "TEXT DEFAULT ''"),
            ("sequences", "generation_model", "TEXT DEFAULT ''"),
            ("sequences", "sales_opportunity_id", "TEXT DEFAULT ''"),
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
            ("replies", "conversation_id", "TEXT DEFAULT ''"),
            ("gtm_experiments", "baseline_sends", "INTEGER DEFAULT 0"),
            ("gtm_outcomes", "touch_id", "TEXT DEFAULT ''"),
            ("gtm_outcomes", "touch_stage", "INTEGER DEFAULT 0"),
            ("gtm_outcomes", "contact_title", "TEXT DEFAULT ''"),
            ("gtm_outcomes", "contact_vantage", "TEXT DEFAULT ''"),
            ("gtm_outcomes", "account_hypothesis", "TEXT DEFAULT ''"),
            ("gtm_outcomes", "play_version", "INTEGER DEFAULT 0"),
            ("gtm_outcomes", "experiment_arm", "TEXT DEFAULT ''"),
            ("gtm_outcomes", "copy_policy_version", "INTEGER DEFAULT 0"),
            ("gtm_outcomes", "generation_backend", "TEXT DEFAULT ''"),
            ("gtm_outcomes", "generation_model", "TEXT DEFAULT ''"),
        ] {
            ensure_column(&conn, table, column, definition)?;
        }
        // One-time lineage cleanup for databases created before official-site
        // refreshes became canonical. History stays queryable; only active
        // readiness stops counting older model summaries as current evidence.
        conn.execute(
            "UPDATE signal_observations AS old
             SET status='superseded',updated_at=?1
             WHERE old.person_id=''
               AND old.source_name IN ('source.qualify','account_research','legacy_account_research')
               AND old.status IN ('observed','verified')
               AND EXISTS (
                 SELECT 1 FROM signal_observations fresh
                 WHERE fresh.brand=old.brand AND fresh.lead_id=old.lead_id
                   AND fresh.source_name='source.refresh'
                   AND fresh.observed_at>=old.observed_at
               )",
            params![now()],
        )?;
        // A copy-policy cutover is an execution stop, not merely a dashboard
        // filter. Preserve stale rows for audit while pausing any active
        // sequence that still has unsent scheduled work.
        conn.execute(
            "UPDATE sequences SET status='paused'
             WHERE status='active' AND copy_policy_version<?1
               AND EXISTS (
                 SELECT 1 FROM touches t
                 WHERE t.sequence_id=sequences.id AND t.status='scheduled'
               )",
            params![CURRENT_COPY_POLICY_VERSION],
        )?;
        conn.execute(
            "UPDATE touches SET status='cancelled',
                    error='Cancelled by copy-policy cutover; regenerate from current evidence.'
             WHERE status='scheduled' AND EXISTS (
               SELECT 1 FROM sequences s
               WHERE s.id=touches.sequence_id AND s.status='paused'
                 AND s.copy_policy_version<?1
             )",
            params![CURRENT_COPY_POLICY_VERSION],
        )?;
        Ok(())
    }

    // --- Leads -------------------------------------------------------------

    /// Insert or update a brand-specific lead, keyed on (brand, apollo_org_id).
    /// The canonical company identity is shared through `market_accounts`; a
    /// manufacturer can therefore be pursued by Wapahki and GnK for unrelated
    /// opportunities without duplicating or excluding the company universe.
    pub fn upsert_lead(&self, lead: &Lead) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let now = now();
        let canonical_domain = canonical_domain(&lead.domain);
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
        let identity_key =
            market_identity_key(&canonical_domain, &lead.apollo_org_id, &lead.name, &lead.hq);
        let market_account_id: Option<String> = conn
            .query_row(
                "SELECT id FROM market_accounts WHERE identity_key=?1",
                params![identity_key],
                |row| row.get(0),
            )
            .optional()?;
        let market_account_id = market_account_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        conn.execute(
            "INSERT INTO market_accounts
             (id,identity_key,canonical_domain,apollo_org_id,name,industry,hq,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?8)
             ON CONFLICT(identity_key) DO UPDATE SET
              canonical_domain=CASE WHEN excluded.canonical_domain<>'' THEN excluded.canonical_domain ELSE market_accounts.canonical_domain END,
              apollo_org_id=CASE WHEN excluded.apollo_org_id<>'' THEN excluded.apollo_org_id ELSE market_accounts.apollo_org_id END,
              name=excluded.name,industry=excluded.industry,hq=excluded.hq,updated_at=excluded.updated_at",
            params![
                market_account_id,
                identity_key,
                canonical_domain,
                lead.apollo_org_id,
                lead.name,
                lead.industry,
                lead.hq,
                now,
            ],
        )?;
        let membership_id = Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO brand_account_memberships
             (id,market_account_id,brand,lead_id,status,priority_tier,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,'hard',?6,?6)
             ON CONFLICT(market_account_id,brand) DO UPDATE SET
              lead_id=excluded.lead_id,status=excluded.status,updated_at=excluded.updated_at",
            params![
                membership_id,
                market_account_id,
                lead.brand,
                id,
                status_or(&lead.status, "research"),
                now
            ],
        )?;
        // `lead.signals` are internal research notes, not evidence observations.
        // Only source-qualified SignalCandidate rows may influence GTM readiness.
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

    // --- Market universe + customer opportunities -----------------------

    pub fn market_account_for_lead(&self, lead_id: &str) -> Result<Option<MarketAccount>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT a.* FROM market_accounts a
             JOIN brand_account_memberships m ON m.market_account_id=a.id
             WHERE m.lead_id=?1 LIMIT 1",
            params![lead_id],
            |row| Ok(row_to_market_account(row)),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_market_accounts(&self, brand: Option<&str>) -> Result<Vec<MarketAccount>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT DISTINCT a.* FROM market_accounts a
             LEFT JOIN brand_account_memberships m ON m.market_account_id=a.id
             WHERE (?1 IS NULL OR m.brand=?1)
             ORDER BY a.name",
        )?;
        let rows = stmt.query_map(params![brand], |row| Ok(row_to_market_account(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn upsert_market_segment(&self, segment: &MarketSegment) -> Result<String> {
        if segment.brand.trim().is_empty() || segment.key.trim().is_empty() {
            anyhow::bail!("market segment requires brand and key");
        }
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM market_segments WHERE brand=?1 AND key=?2 AND version=?3",
                params![segment.brand, segment.key, segment.version.max(1)],
                |row| row.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| {
            if segment.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                segment.id.clone()
            }
        });
        let timestamp = now();
        conn.execute(
            "INSERT INTO market_segments
             (id,brand,key,version,name,geography,wedge,unit_of_analysis,enumeration_sources,
              status,estimated_total,accounts_discovered,accounts_with_opportunities,
              source_exhausted,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?15)
             ON CONFLICT(brand,key,version) DO UPDATE SET
              name=excluded.name,geography=excluded.geography,wedge=excluded.wedge,
              unit_of_analysis=excluded.unit_of_analysis,
              enumeration_sources=excluded.enumeration_sources,status=excluded.status,
              estimated_total=excluded.estimated_total,
              accounts_discovered=excluded.accounts_discovered,
              accounts_with_opportunities=excluded.accounts_with_opportunities,
              source_exhausted=excluded.source_exhausted,updated_at=excluded.updated_at",
            params![
                id,
                segment.brand,
                segment.key,
                segment.version.max(1),
                segment.name,
                segment.geography,
                segment.wedge,
                segment.unit_of_analysis,
                js(&segment.enumeration_sources),
                status_or(&segment.status, "active"),
                segment.estimated_total.max(0),
                segment.accounts_discovered.max(0),
                segment.accounts_with_opportunities.max(0),
                segment.source_exhausted,
                timestamp,
            ],
        )?;
        Ok(id)
    }

    /// Convert the legacy company-level assessment into the v30 commercial
    /// object and copy only atomic, URL-backed observations into claim lineage.
    /// This bridge keeps existing research useful while preventing unlinked
    /// account prose from authorizing outreach.
    fn materialize_sales_opportunity(&self, assessment: &AccountPlayAssessment) -> Result<String> {
        let Some(lead) = self.get_lead(&assessment.lead_id)? else {
            anyhow::bail!("assessment lead {} is missing", assessment.lead_id);
        };
        let Some(account) = self.market_account_for_lead(&assessment.lead_id)? else {
            anyhow::bail!(
                "assessment lead {} has no market identity",
                assessment.lead_id
            );
        };
        let observations = self
            .list_active_signal_observations(
                Some(&assessment.brand),
                Some(&assessment.lead_id),
                None,
            )?
            .into_iter()
            .filter(|observation| {
                observation.person_id.is_empty()
                    && !observation.source_url.trim().is_empty()
                    && !observation.evidence.trim().is_empty()
                    && assessment
                        .matched_signal_keys
                        .contains(&observation.definition_key)
            })
            .collect::<Vec<_>>();
        let task_or_decision = if !lead.hypothesis.trim().is_empty() {
            lead.hypothesis.clone()
        } else if !assessment.symptom.trim().is_empty() {
            assessment.symptom.clone()
        } else {
            "Unresolved operating task or decision".into()
        };
        let mut gaps = assessment.evidence_gaps.clone();
        if observations.is_empty() {
            gaps.push(
                "No atomic source-backed evidence claim is linked to this opportunity.".into(),
            );
        }
        let facility_id = if assessment.brand.eq_ignore_ascii_case("wapahki") {
            facility_observation_for_task(&observations)
                .and_then(|observation| {
                    self.upsert_facility(&Facility {
                        market_account_id: account.id.clone(),
                        name: facility_label(&lead, &observation.evidence),
                        facility_type: facility_type_from_evidence(&observation.evidence).into(),
                        region: if observation
                            .evidence
                            .to_ascii_lowercase()
                            .contains("ontario")
                        {
                            "Ontario".into()
                        } else {
                            String::new()
                        },
                        country: "Canada".into(),
                        source_url: observation.source_url.clone(),
                        source_excerpt: observation.evidence.clone(),
                        confidence: observation.confidence,
                        status: "observed".into(),
                        ..Default::default()
                    })
                    .ok()
                })
                .unwrap_or_default()
        } else {
            String::new()
        };
        if assessment.brand.eq_ignore_ascii_case("wapahki") && facility_id.is_empty() {
            gaps.push(
                "No exact operating facility is linked to the physical task evidence.".into(),
            );
        }
        let evidence_status = match assessment.status.as_str() {
            "qualified"
                if !observations.is_empty()
                    && (!assessment.brand.eq_ignore_ascii_case("wapahki")
                        || !facility_id.is_empty()) =>
            {
                "action_ready"
            }
            "qualified" | "research_needed" if !observations.is_empty() => "discovery_ready",
            _ => "research_required",
        };
        let priority_tier = match evidence_status {
            "action_ready" => "easy",
            "discovery_ready" => "medium",
            _ => "hard",
        };
        let segment_id = choose_segment_id(
            &assessment.brand,
            &format!(
                "{} {} {} {}",
                lead.industry, task_or_decision, lead.mechanism, assessment.symptom
            ),
            &self.list_market_segments(Some(&assessment.brand))?,
        );
        let opportunity = SalesOpportunity {
            brand: assessment.brand.clone(),
            market_account_id: account.id,
            lead_id: assessment.lead_id.clone(),
            segment_id,
            facility_id,
            play_id: assessment.play_id.clone(),
            kind: match assessment.brand.as_str() {
                "wapahki" => "physical_task",
                "outagehub" => "outage_decision",
                _ => "software_workflow",
            }
            .into(),
            title: opportunity_title(&task_or_decision),
            task_or_decision,
            mechanism: if !lead.mechanism.trim().is_empty() {
                lead.mechanism.clone()
            } else {
                assessment.root_cause.clone()
            },
            consequence: lead.consequence_metric.clone(),
            system_concept: lead.system_concept.clone(),
            proof_offer: assessment.proof_fit.clone(),
            evidence_status: evidence_status.into(),
            priority_tier: priority_tier.into(),
            fit_score: assessment.fit_score,
            status: if evidence_status == "research_required" {
                "research".into()
            } else {
                "mapped".into()
            },
            evidence_gaps: gaps,
            ..Default::default()
        };
        let opportunity_id = self.upsert_sales_opportunity(&opportunity)?;
        for observation in observations {
            let _ = self.upsert_evidence_claim(&EvidenceClaim {
                sales_opportunity_id: opportunity_id.clone(),
                brand: assessment.brand.clone(),
                lead_id: assessment.lead_id.clone(),
                claim_type: observation.definition_key,
                claim_text: observation.evidence.clone(),
                source_url: observation.source_url,
                source_excerpt: observation.evidence,
                confidence: observation.confidence,
                status: observation.status,
                observed_at: observation.observed_at,
                ..Default::default()
            });
        }
        for person in self
            .list_people(Some(&assessment.brand), None)?
            .into_iter()
            .filter(|person| person.lead_id == assessment.lead_id)
        {
            let role = stakeholder_role(&person.title, &person.vantage);
            self.upsert_opportunity_stakeholder(&OpportunityStakeholder {
                sales_opportunity_id: opportunity_id.clone(),
                person_id: person.id,
                role: role.clone(),
                relationship_to_task: person.why_them,
                can_observe: person.can_observe,
                can_decide: stakeholder_decision_scope(&role).into(),
                priority: stakeholder_priority(&role),
                status: "mapped".into(),
                source: "contact_research".into(),
                ..Default::default()
            })?;
        }
        Ok(opportunity_id)
    }

    pub fn list_market_segments(&self, brand: Option<&str>) -> Result<Vec<MarketSegment>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM market_segments WHERE (?1 IS NULL OR brand=?1)
             ORDER BY brand,status='active' DESC,name,version DESC",
        )?;
        let rows = stmt.query_map(params![brand], |row| Ok(row_to_market_segment(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn upsert_coverage_run(&self, run: &CoverageRun) -> Result<String> {
        if run.segment_id.trim().is_empty()
            || run.source_name.trim().is_empty()
            || run.query_fingerprint.trim().is_empty()
        {
            anyhow::bail!("coverage run requires segment, source, and query fingerprint");
        }
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM coverage_runs
                 WHERE segment_id=?1 AND source_name=?2 AND query_fingerprint=?3",
                params![run.segment_id, run.source_name, run.query_fingerprint],
                |row| row.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| {
            if run.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                run.id.clone()
            }
        });
        let timestamp = now();
        conn.execute(
            "INSERT INTO coverage_runs
             (id,segment_id,brand,source_name,query_fingerprint,cursor,pages_examined,
              candidates_seen,accounts_added,status,exhausted,gap_reason,started_at,
              completed_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)
             ON CONFLICT(segment_id,source_name,query_fingerprint) DO UPDATE SET
              cursor=excluded.cursor,pages_examined=excluded.pages_examined,
              candidates_seen=excluded.candidates_seen,accounts_added=excluded.accounts_added,
              status=excluded.status,exhausted=excluded.exhausted,
              gap_reason=excluded.gap_reason,completed_at=excluded.completed_at,
              updated_at=excluded.updated_at",
            params![
                id,
                run.segment_id,
                run.brand,
                run.source_name,
                run.query_fingerprint,
                run.cursor,
                run.pages_examined.max(0),
                run.candidates_seen.max(0),
                run.accounts_added.max(0),
                status_or(&run.status, "running"),
                run.exhausted,
                run.gap_reason,
                status_or(&run.started_at, &timestamp),
                run.completed_at,
                timestamp,
            ],
        )?;
        conn.execute(
            "UPDATE market_segments SET
              accounts_discovered=COALESCE((SELECT SUM(candidates_seen) FROM coverage_runs WHERE segment_id=?1),0),
              source_exhausted=CASE WHEN EXISTS(SELECT 1 FROM coverage_runs WHERE segment_id=?1)
                 AND NOT EXISTS(SELECT 1 FROM coverage_runs WHERE segment_id=?1 AND exhausted=0)
                 THEN 1 ELSE 0 END,
              updated_at=?2 WHERE id=?1",
            params![run.segment_id, timestamp],
        )?;
        Ok(id)
    }

    pub fn list_coverage_runs(&self, brand: Option<&str>) -> Result<Vec<CoverageRun>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM coverage_runs WHERE (?1 IS NULL OR brand=?1)
             ORDER BY updated_at DESC",
        )?;
        let rows = stmt.query_map(params![brand], |row| Ok(row_to_coverage_run(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn upsert_facility(&self, facility: &Facility) -> Result<String> {
        if facility.market_account_id.trim().is_empty()
            || facility.name.trim().is_empty()
            || facility.source_url.trim().is_empty()
            || facility.source_excerpt.trim().is_empty()
        {
            anyhow::bail!("facility requires account, name, exact source URL, and excerpt");
        }
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM facilities
                 WHERE market_account_id=?1 AND name=?2 AND source_url=?3",
                params![
                    facility.market_account_id,
                    facility.name,
                    facility.source_url
                ],
                |row| row.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| {
            if facility.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                facility.id.clone()
            }
        });
        let timestamp = now();
        conn.execute(
            "INSERT INTO facilities
             (id,market_account_id,name,facility_type,address,city,region,country,
              source_url,source_excerpt,confidence,status,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13)
             ON CONFLICT(market_account_id,name,source_url) DO UPDATE SET
              facility_type=excluded.facility_type,address=excluded.address,city=excluded.city,
              region=excluded.region,country=excluded.country,
              source_excerpt=excluded.source_excerpt,confidence=excluded.confidence,
              status=excluded.status,updated_at=excluded.updated_at",
            params![
                id,
                facility.market_account_id,
                facility.name,
                facility.facility_type,
                facility.address,
                facility.city,
                facility.region,
                facility.country,
                facility.source_url,
                facility.source_excerpt,
                facility.confidence.clamp(0.0, 1.0),
                status_or(&facility.status, "observed"),
                timestamp,
            ],
        )?;
        Ok(id)
    }

    pub fn list_facilities(&self, market_account_id: Option<&str>) -> Result<Vec<Facility>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM facilities WHERE (?1 IS NULL OR market_account_id=?1)
             ORDER BY name",
        )?;
        let rows = stmt.query_map(params![market_account_id], |row| Ok(row_to_facility(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn upsert_sales_opportunity(&self, opportunity: &SalesOpportunity) -> Result<String> {
        if opportunity.brand.trim().is_empty()
            || opportunity.market_account_id.trim().is_empty()
            || opportunity.lead_id.trim().is_empty()
            || opportunity.play_id.trim().is_empty()
            || opportunity.title.trim().is_empty()
            || opportunity.task_or_decision.trim().is_empty()
        {
            anyhow::bail!(
                "sales opportunity requires brand, account, lead, play, title, and task/decision"
            );
        }
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM sales_opportunities
                 WHERE brand=?1 AND lead_id=?2 AND play_id=?3
                 ORDER BY updated_at DESC LIMIT 1",
                params![opportunity.brand, opportunity.lead_id, opportunity.play_id],
                |row| row.get(0),
            )
            .optional()?;
        let existed = existing.is_some();
        let id = existing.unwrap_or_else(|| {
            if opportunity.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                opportunity.id.clone()
            }
        });
        let timestamp = now();
        if !existed {
            conn.execute(
                "INSERT INTO sales_opportunities
             (id,brand,market_account_id,lead_id,segment_id,facility_id,play_id,kind,title,
              task_or_decision,mechanism,consequence,system_concept,proof_offer,evidence_status,
              priority_tier,fit_score,status,evidence_gaps,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?20)
             ON CONFLICT(brand,lead_id,play_id,title) DO UPDATE SET
              market_account_id=excluded.market_account_id,segment_id=excluded.segment_id,
              facility_id=excluded.facility_id,kind=excluded.kind,
              task_or_decision=excluded.task_or_decision,mechanism=excluded.mechanism,
              consequence=excluded.consequence,system_concept=excluded.system_concept,
              proof_offer=excluded.proof_offer,evidence_status=excluded.evidence_status,
              priority_tier=excluded.priority_tier,fit_score=excluded.fit_score,
              status=excluded.status,evidence_gaps=excluded.evidence_gaps,
              updated_at=excluded.updated_at",
                params![
                    id,
                    opportunity.brand,
                    opportunity.market_account_id,
                    opportunity.lead_id,
                    opportunity.segment_id,
                    opportunity.facility_id,
                    opportunity.play_id,
                    opportunity.kind,
                    opportunity.title,
                    opportunity.task_or_decision,
                    opportunity.mechanism,
                    opportunity.consequence,
                    opportunity.system_concept,
                    opportunity.proof_offer,
                    status_or(&opportunity.evidence_status, "research_required"),
                    status_or(&opportunity.priority_tier, "hard"),
                    opportunity.fit_score.clamp(0, 100),
                    status_or(&opportunity.status, "research"),
                    js(&opportunity.evidence_gaps),
                    timestamp,
                ],
            )?;
        }
        // The row may predate a changed title, in which case the SELECT path
        // owns the stable id and must update that exact row.
        conn.execute(
            "UPDATE sales_opportunities SET
              market_account_id=?2,segment_id=?3,facility_id=?4,kind=?5,title=?6,
              task_or_decision=?7,mechanism=?8,consequence=?9,system_concept=?10,
              proof_offer=?11,evidence_status=?12,priority_tier=?13,fit_score=?14,
              status=?15,evidence_gaps=?16,updated_at=?17 WHERE id=?1",
            params![
                id,
                opportunity.market_account_id,
                opportunity.segment_id,
                opportunity.facility_id,
                opportunity.kind,
                opportunity.title,
                opportunity.task_or_decision,
                opportunity.mechanism,
                opportunity.consequence,
                opportunity.system_concept,
                opportunity.proof_offer,
                status_or(&opportunity.evidence_status, "research_required"),
                status_or(&opportunity.priority_tier, "hard"),
                opportunity.fit_score.clamp(0, 100),
                status_or(&opportunity.status, "research"),
                js(&opportunity.evidence_gaps),
                timestamp,
            ],
        )?;
        conn.execute(
            "UPDATE brand_account_memberships SET priority_tier=?3,status=?4,updated_at=?5
             WHERE brand=?1 AND lead_id=?2",
            params![
                opportunity.brand,
                opportunity.lead_id,
                status_or(&opportunity.priority_tier, "hard"),
                status_or(&opportunity.status, "research"),
                timestamp,
            ],
        )?;
        if !opportunity.segment_id.trim().is_empty() {
            conn.execute(
                "UPDATE market_segments SET
                  accounts_with_opportunities=(SELECT COUNT(DISTINCT market_account_id)
                    FROM sales_opportunities WHERE segment_id=?1 AND status<>'rejected'),
                  updated_at=?2 WHERE id=?1",
                params![opportunity.segment_id, timestamp],
            )?;
        }
        Ok(id)
    }

    pub fn list_sales_opportunities(
        &self,
        brand: Option<&str>,
        lead_id: Option<&str>,
    ) -> Result<Vec<SalesOpportunity>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM sales_opportunities
             WHERE (?1 IS NULL OR brand=?1) AND (?2 IS NULL OR lead_id=?2)
             ORDER BY CASE priority_tier WHEN 'easy' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                      fit_score DESC,updated_at DESC",
        )?;
        let rows = stmt.query_map(params![brand, lead_id], |row| {
            Ok(row_to_sales_opportunity(row))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn best_sales_opportunity(
        &self,
        brand: &str,
        lead_id: &str,
        play_id: &str,
    ) -> Result<Option<SalesOpportunity>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT * FROM sales_opportunities
             WHERE brand=?1 AND lead_id=?2 AND play_id=?3 AND status<>'rejected'
             ORDER BY CASE priority_tier WHEN 'easy' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                      fit_score DESC,updated_at DESC LIMIT 1",
            params![brand, lead_id, play_id],
            |row| Ok(row_to_sales_opportunity(row)),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn upsert_evidence_claim(&self, claim: &EvidenceClaim) -> Result<String> {
        if claim.sales_opportunity_id.trim().is_empty()
            || claim.claim_type.trim().is_empty()
            || claim.claim_text.trim().is_empty()
            || claim.source_url.trim().is_empty()
            || claim.source_excerpt.trim().is_empty()
        {
            anyhow::bail!(
                "evidence claim requires opportunity, type, claim, exact source URL, and excerpt"
            );
        }
        let domain = if claim.source_domain.trim().is_empty() {
            source_domain(&claim.source_url)
        } else {
            claim.source_domain.trim().to_ascii_lowercase()
        };
        if domain.is_empty() {
            anyhow::bail!("evidence claim source URL has no canonical domain");
        }
        let lineage_key = if claim.lineage_key.trim().is_empty() {
            format!(
                "{:016x}",
                stable_hash(&format!(
                    "{}|{}|{}|{}",
                    claim.sales_opportunity_id,
                    claim.claim_type,
                    claim.source_url.trim().to_ascii_lowercase(),
                    claim.source_excerpt.trim().to_ascii_lowercase()
                ))
            )
        } else {
            claim.lineage_key.clone()
        };
        let independence_group = if claim.independence_group.trim().is_empty() {
            domain.clone()
        } else {
            claim.independence_group.trim().to_ascii_lowercase()
        };
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM evidence_claims
                 WHERE sales_opportunity_id=?1 AND lineage_key=?2",
                params![claim.sales_opportunity_id, lineage_key],
                |row| row.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| {
            if claim.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                claim.id.clone()
            }
        });
        let timestamp = now();
        conn.execute(
            "INSERT INTO evidence_claims
             (id,sales_opportunity_id,brand,lead_id,facility_id,claim_type,claim_text,
              source_url,source_title,source_excerpt,source_locator,source_domain,lineage_key,
              independence_group,confidence,status,observed_at,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?18)
             ON CONFLICT(sales_opportunity_id,lineage_key) DO UPDATE SET
              claim_text=excluded.claim_text,source_title=excluded.source_title,
              source_excerpt=excluded.source_excerpt,source_locator=excluded.source_locator,
              source_domain=excluded.source_domain,independence_group=excluded.independence_group,
              confidence=excluded.confidence,status=excluded.status,
              observed_at=excluded.observed_at,updated_at=excluded.updated_at",
            params![
                id,
                claim.sales_opportunity_id,
                claim.brand,
                claim.lead_id,
                claim.facility_id,
                claim.claim_type,
                claim.claim_text,
                claim.source_url,
                claim.source_title,
                claim.source_excerpt,
                claim.source_locator,
                domain,
                lineage_key,
                independence_group,
                claim.confidence.clamp(0.0, 1.0),
                status_or(&claim.status, "observed"),
                status_or(&claim.observed_at, &timestamp),
                timestamp,
            ],
        )?;
        Ok(id)
    }

    pub fn list_evidence_claims(
        &self,
        sales_opportunity_id: Option<&str>,
        brand: Option<&str>,
    ) -> Result<Vec<EvidenceClaim>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM evidence_claims
             WHERE (?1 IS NULL OR sales_opportunity_id=?1) AND (?2 IS NULL OR brand=?2)
             ORDER BY observed_at DESC",
        )?;
        let rows = stmt.query_map(params![sales_opportunity_id, brand], |row| {
            Ok(row_to_evidence_claim(row))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    #[allow(dead_code)]
    pub fn upsert_opportunity_stakeholder(
        &self,
        stakeholder: &OpportunityStakeholder,
    ) -> Result<String> {
        if stakeholder.sales_opportunity_id.trim().is_empty()
            || stakeholder.person_id.trim().is_empty()
            || stakeholder.role.trim().is_empty()
        {
            anyhow::bail!("opportunity stakeholder requires opportunity, person, and role");
        }
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM opportunity_stakeholders
                 WHERE sales_opportunity_id=?1 AND person_id=?2",
                params![stakeholder.sales_opportunity_id, stakeholder.person_id],
                |row| row.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| {
            if stakeholder.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                stakeholder.id.clone()
            }
        });
        let timestamp = now();
        conn.execute(
            "INSERT INTO opportunity_stakeholders
             (id,sales_opportunity_id,person_id,role,relationship_to_task,can_observe,
              can_decide,priority,active_thread,status,source,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,0,?9,?10,?11,?11)
             ON CONFLICT(sales_opportunity_id,person_id) DO UPDATE SET
              role=excluded.role,relationship_to_task=excluded.relationship_to_task,
              can_observe=excluded.can_observe,can_decide=excluded.can_decide,
              priority=excluded.priority,status=excluded.status,source=excluded.source,
              updated_at=excluded.updated_at",
            params![
                id,
                stakeholder.sales_opportunity_id,
                stakeholder.person_id,
                stakeholder.role,
                stakeholder.relationship_to_task,
                stakeholder.can_observe,
                stakeholder.can_decide,
                stakeholder.priority.max(0),
                status_or(&stakeholder.status, "mapped"),
                status_or(&stakeholder.source, "contact_research"),
                timestamp,
            ],
        )?;
        Ok(id)
    }

    pub fn activate_opportunity_stakeholder(
        &self,
        sales_opportunity_id: &str,
        person_id: &str,
    ) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE opportunity_stakeholders SET active_thread=0,updated_at=?2
             WHERE sales_opportunity_id=?1",
            params![sales_opportunity_id, now()],
        )?;
        let updated = tx.execute(
            "UPDATE opportunity_stakeholders SET active_thread=1,status='active',updated_at=?3
             WHERE sales_opportunity_id=?1 AND person_id=?2",
            params![sales_opportunity_id, person_id, now()],
        )?;
        if updated == 0 {
            anyhow::bail!("person is not mapped to this sales opportunity");
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_opportunity_stakeholders(
        &self,
        sales_opportunity_id: Option<&str>,
        brand: Option<&str>,
    ) -> Result<Vec<OpportunityStakeholder>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT s.* FROM opportunity_stakeholders s
             JOIN sales_opportunities o ON o.id=s.sales_opportunity_id
             WHERE (?1 IS NULL OR s.sales_opportunity_id=?1) AND (?2 IS NULL OR o.brand=?2)
             ORDER BY s.active_thread DESC,s.priority,s.updated_at DESC",
        )?;
        let rows = stmt.query_map(params![sales_opportunity_id, brand], |row| {
            Ok(row_to_opportunity_stakeholder(row))
        })?;
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
        if !p.linkedin_status.trim().is_empty() {
            conn.execute(
                "UPDATE people SET linkedin_status=?2 WHERE id=?1",
                params![id, normalize_linkedin_status(&p.linkedin_status)],
            )?;
        }
        if !p.vantage.trim().is_empty() || !p.can_observe.trim().is_empty() {
            let evidence = format!(
                "{} — vantage: {}; likely access: {}",
                p.title.trim(),
                p.vantage.trim(),
                p.can_observe.trim()
            );
            let _ = record_signal_observation_conn(
                &conn,
                &SignalObservation {
                    brand: p.brand.clone(),
                    definition_key: "contact.workflow_vantage".into(),
                    lead_id: p.lead_id.clone(),
                    person_id: id.clone(),
                    source_name: "contact_research".into(),
                    evidence,
                    confidence: 0.70,
                    status: "observed".into(),
                    ..Default::default()
                },
            );
        }
        // Map every sourced person onto the current opportunity committee. The
        // outreach planner activates exactly one of these rows later; sourcing
        // the committee is not authorization to contact the committee.
        let opportunity_id: Option<String> = conn
            .query_row(
                "SELECT o.id FROM sales_opportunities o
                 JOIN gtm_plays gp ON gp.id=o.play_id
                 WHERE o.brand=?1 AND o.lead_id=?2 AND o.status<>'rejected'
                   AND gp.lifecycle IN ('proven','testing')
                 ORDER BY CASE gp.lifecycle WHEN 'proven' THEN 0 ELSE 1 END,
                          gp.version DESC,
                          CASE o.priority_tier WHEN 'easy' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                          o.fit_score DESC,o.updated_at DESC LIMIT 1",
                params![p.brand, p.lead_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(opportunity_id) = opportunity_id {
            let role = stakeholder_role(&p.title, &p.vantage);
            conn.execute(
                "INSERT INTO opportunity_stakeholders
                 (id,sales_opportunity_id,person_id,role,relationship_to_task,can_observe,
                  can_decide,priority,active_thread,status,source,created_at,updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,0,'mapped','contact_research',?9,?9)
                 ON CONFLICT(sales_opportunity_id,person_id) DO UPDATE SET
                  role=excluded.role,relationship_to_task=excluded.relationship_to_task,
                  can_observe=excluded.can_observe,can_decide=excluded.can_decide,
                  priority=excluded.priority,updated_at=excluded.updated_at",
                params![
                    Uuid::new_v4().to_string(),
                    opportunity_id,
                    id,
                    role,
                    p.why_them,
                    p.can_observe,
                    stakeholder_decision_scope(&role),
                    stakeholder_priority(&role),
                    now,
                ],
            )?;
        }
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

    pub fn set_person_linkedin_status(&self, id: &str, status: &str) -> Result<()> {
        let status = normalize_linkedin_status(status);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE people SET linkedin_status=?2,updated_at=?3 WHERE id=?1",
            params![id, status, now()],
        )?;
        Ok(())
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
        let copy_policy_version = if s.copy_policy_version <= 0 {
            CURRENT_COPY_POLICY_VERSION
        } else {
            s.copy_policy_version
        };
        conn.execute(
            "INSERT INTO sequences (id,person_id,lead_id,brand,thesis,applied_principles,\
             play_id,play_version,experiment_id,experiment_arm,experiment_assignment_id,\
             signal_observation_ids,gtm_state,copy_policy_version,generation_backend,generation_model,\
             sales_opportunity_id,status,current_stage,created_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
            params![
                id,
                s.person_id,
                s.lead_id,
                s.brand,
                s.thesis,
                js(&s.applied_principles),
                s.play_id,
                s.play_version,
                s.experiment_id,
                s.experiment_arm,
                s.experiment_assignment_id,
                js(&s.signal_observation_ids),
                s.gtm_state,
                copy_policy_version,
                s.generation_backend,
                s.generation_model,
                s.sales_opportunity_id,
                status_or(&s.status, "active"),
                s.current_stage,
                now()
            ],
        )?;
        if !s.experiment_assignment_id.is_empty() {
            conn.execute(
                "UPDATE experiment_assignments SET sequence_id=?2 WHERE id=?1 AND sequence_id=''",
                params![s.experiment_assignment_id, id],
            )?;
        }
        Ok(id)
    }

    pub fn sequence_gtm_attribution(&self, sequence_id: &str) -> Result<Option<Sequence>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM sequences WHERE id=?1",
                params![sequence_id],
                |row| Ok(row_to_sequence(row)),
            )
            .optional()?)
    }

    /// Permanently remove an active sequence only when none of its touches were
    /// sent. Used to replace rejected drafts without rewriting delivery history.
    pub fn discard_unsent_sequence(&self, sequence_id: &str) -> Result<bool> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let sent: i64 = tx.query_row(
            "SELECT COUNT(*) FROM touches WHERE sequence_id=?1 AND status='sent'",
            params![sequence_id],
            |row| row.get(0),
        )?;
        if sent > 0 {
            return Ok(false);
        }
        tx.execute(
            "DELETE FROM touches WHERE sequence_id=?1",
            params![sequence_id],
        )?;
        let removed = tx.execute(
            "DELETE FROM sequences WHERE id=?1 AND status='active'",
            params![sequence_id],
        )?;
        tx.commit()?;
        Ok(removed > 0)
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

    pub fn apply_touch_schedule(&self, updates: &[TouchScheduleUpdate]) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "UPDATE touches SET due_at=?2,recipient_timezone=?3,scheduled_rule=?4,
                 schedule_reason=?5 WHERE id=?1 AND status='scheduled'",
            )?;
            for update in updates {
                stmt.execute(params![
                    update.id,
                    update.due_at,
                    update.recipient_timezone,
                    update.scheduled_rule,
                    update.schedule_reason,
                ])?;
            }
        }
        tx.commit()?;
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

    /// Replace the current checkpoint for one generated touch. Building
    /// sequences are never send-eligible; these rows exist so the CRM can show
    /// writing and review progress before the full sequence finishes.
    pub fn update_touch_checkpoint(&self, t: &Touch) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let updated = conn.execute(
            "UPDATE touches SET day_offset=?3,channel=?4,subject=?5,body=?6,purpose=?7,
             goal=?8,status=?9,due_at=?10,recipient_timezone=?11,scheduled_rule=?12,
             schedule_reason=?13,review_passes=?14,review_issues=?15,error=?16
             WHERE sequence_id=?1 AND stage=?2",
            params![
                t.sequence_id,
                t.stage,
                t.day_offset,
                t.channel,
                t.subject,
                t.body,
                t.purpose,
                t.goal,
                status_or(&t.status, "reviewing"),
                t.due_at,
                t.recipient_timezone,
                t.scheduled_rule,
                t.schedule_reason,
                t.review_passes,
                js(&t.review_issues),
                t.error,
            ],
        )?;
        Ok(updated > 0)
    }

    /// Atomically make a fully reviewed checkpoint sequence active. An unsent
    /// sequence being replaced is removed only at this final promotion point,
    /// so a failed rewrite cannot destroy the operator's prior drafts.
    pub fn promote_building_sequence(
        &self,
        sequence_id: &str,
        replaced_sequence_id: Option<&str>,
        applied_principles: &[String],
    ) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        if let Some(old_id) = replaced_sequence_id.filter(|id| !id.is_empty()) {
            let sent: i64 = tx.query_row(
                "SELECT COUNT(*) FROM touches WHERE sequence_id=?1 AND status='sent'",
                params![old_id],
                |row| row.get(0),
            )?;
            if sent > 0 {
                anyhow::bail!("refusing to replace a sequence with sent touches");
            }
            tx.execute("DELETE FROM touches WHERE sequence_id=?1", params![old_id])?;
            let removed = tx.execute(
                "DELETE FROM sequences WHERE id=?1 AND status='active'",
                params![old_id],
            )?;
            if removed == 0 {
                anyhow::bail!("the prior active sequence changed while drafting");
            }
        }
        let promoted = tx.execute(
            "UPDATE sequences SET status='active',applied_principles=?2,copy_policy_version=?3 WHERE id=?1 AND status='building'",
            params![
                sequence_id,
                js(applied_principles),
                CURRENT_COPY_POLICY_VERSION
            ],
        )?;
        if promoted == 0 {
            anyhow::bail!("the building sequence is no longer promotable");
        }
        tx.commit()?;
        Ok(())
    }

    pub fn reject_building_sequence(&self, sequence_id: &str, reason: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE touches SET status='blocked',review_passes=0,review_issues=?2,error=?3
             WHERE sequence_id=?1 AND status IN ('writing','reviewing')",
            params![sequence_id, js(&vec![reason.to_string()]), reason],
        )?;
        conn.execute(
            "UPDATE sequences SET status='rejected' WHERE id=?1 AND status='building'",
            params![sequence_id],
        )?;
        Ok(())
    }

    /// A valid abstention: evidence or recipient fit was insufficient to write.
    /// Preserve the checkpoint and reason without calling it failed copy.
    pub fn hold_building_sequence(&self, sequence_id: &str, reason: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE touches SET status='blocked',review_passes=0,review_issues=?2,error=?3
             WHERE sequence_id=?1 AND status IN ('writing','reviewing')",
            params![sequence_id, js(&vec![reason.to_string()]), reason],
        )?;
        conn.execute(
            "UPDATE sequences SET status='held' WHERE id=?1 AND status='building'",
            params![sequence_id],
        )?;
        Ok(())
    }

    /// Stop an incomplete generation because the model provider was
    /// unavailable. This is deliberately distinct from copy rejection: no
    /// reviewer saw bad copy, and a later run may safely retry the recipient.
    pub fn stop_building_sequence(&self, sequence_id: &str, reason: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE touches SET status='blocked',review_passes=0,review_issues=?2,error=?3
             WHERE sequence_id=?1 AND status IN ('writing','reviewing')",
            params![sequence_id, js(&vec![reason.to_string()]), reason],
        )?;
        conn.execute(
            "UPDATE sequences SET status='stopped' WHERE id=?1 AND status='building'",
            params![sequence_id],
        )?;
        Ok(())
    }

    pub fn interrupt_prior_building_sequences(&self, person_id: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let reason = "A newer drafting run superseded this incomplete checkpoint.";
        conn.execute(
            "UPDATE touches SET status='blocked',review_passes=0,review_issues=?2,error=?3
             WHERE sequence_id IN (
               SELECT id FROM sequences WHERE person_id=?1 AND status='building'
             ) AND status IN ('writing','reviewing')",
            params![person_id, js(&vec![reason.to_string()]), reason],
        )?;
        Ok(conn.execute(
            "UPDATE sequences SET status='rejected' WHERE person_id=?1 AND status='building'",
            params![person_id],
        )?)
    }

    /// Touches the cadence engine may fire now: scheduled + due, on an active,
    /// unpaused sequence, for an email-capable person who isn't suppressed/replied.
    pub fn due_touches(&self, brand: Option<&str>, limit: i64) -> Result<Vec<Touch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT t.* FROM touches t \
             JOIN sequences s ON s.id=t.sequence_id \
             JOIN people p ON p.id=t.person_id \
             WHERE t.status='scheduled' AND t.due_at<=?1 \
               AND s.status='active' AND s.copy_policy_version=?3 \
               AND p.status NOT IN ('replied','unsubscribed','bounced','suppressed') \
               AND (?2 IS NULL OR t.brand=?2) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM touches prior \
                   WHERE prior.sequence_id=t.sequence_id AND prior.stage<t.stage \
                     AND lower(prior.channel) IN ('email','linkedin_or_email') \
                     AND prior.status NOT IN ('sent','skipped','cancelled','replied') \
               ) \
             ORDER BY CASE WHEN t.stage>1 THEN 0 ELSE 1 END, t.due_at ASC LIMIT ?4",
        )?;
        let rows = stmt.query_map(
            params![now(), brand, CURRENT_COPY_POLICY_VERSION, limit],
            |r| Ok(row_to_touch(r)),
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Every approved, unsent sales email that the portfolio scheduler may
    /// place. Manual LinkedIn work remains outside the email capacity plan.
    pub fn scheduled_email_touches(&self, brand: &str) -> Result<Vec<Touch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT t.* FROM touches t
             JOIN sequences s ON s.id=t.sequence_id
             JOIN people p ON p.id=t.person_id
             WHERE t.brand=?1 AND t.status='scheduled'
               AND lower(t.channel) IN ('email','linkedin_or_email')
               AND s.status='active' AND s.copy_policy_version=?2
               AND p.status NOT IN ('replied','unsubscribed','bounced','suppressed')
             ORDER BY s.created_at ASC,t.stage ASC,t.id ASC",
        )?;
        let rows = stmt.query_map(params![brand, CURRENT_COPY_POLICY_VERSION], |r| {
            Ok(row_to_touch(r))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Atomically reserve one scheduled touch for delivery. Due-work queries are
    /// intentionally read-only, so every live worker must pass this gate just
    /// before crossing the external send boundary. Only one worker can move a
    /// touch from `scheduled` to `sending`; a crashed worker leaves an explicit
    /// reconciliation state instead of making another process send it twice.
    pub fn claim_touch_for_send(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "UPDATE touches SET status='sending',error='' WHERE id=?1 AND status='scheduled'
             AND EXISTS (
               SELECT 1 FROM sequences s WHERE s.id=touches.sequence_id
                 AND s.status='active' AND s.copy_policy_version=?2
             )",
            params![id, CURRENT_COPY_POLICY_VERSION],
        )? == 1)
    }

    #[cfg(test)]
    pub fn list_touches_for_person(&self, person_id: &str) -> Result<Vec<Touch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM touches WHERE sequence_id=(
               SELECT id FROM sequences WHERE person_id=?1
               ORDER BY CASE status WHEN 'building' THEN 0 WHEN 'active' THEN 1 ELSE 2 END,
                        created_at DESC LIMIT 1
             ) ORDER BY stage ASC",
        )?;
        let rows = stmt.query_map(params![person_id], |r| Ok(row_to_touch(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_touches_for_sequence(&self, sequence_id: &str) -> Result<Vec<Touch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT * FROM touches WHERE sequence_id=?1 ORDER BY stage ASC")?;
        let rows = stmt.query_map(params![sequence_id], |r| Ok(row_to_touch(r)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn get_touch(&self, id: &str) -> Result<Option<Touch>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row("SELECT * FROM touches WHERE id=?1", params![id], |row| {
                Ok(row_to_touch(row))
            })
            .optional()?)
    }

    /// Email capacity used or reserved for one business calendar day. Only
    /// approved email-capable work reserves capacity; drafts and manual
    /// LinkedIn tasks do not enter the sending calendar.
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
            "SELECT COUNT(*) FROM touches t
             JOIN sequences s ON s.id=t.sequence_id
             WHERE t.brand=?1 AND t.due_at>=?2 AND t.due_at<?3
             AND lower(t.channel) IN ('email','linkedin_or_email')
             AND (t.status='sent' OR (t.status='scheduled'
                  AND s.status='active' AND s.copy_policy_version=?4))",
            params![brand, start, end, CURRENT_COPY_POLICY_VERSION],
            |r| r.get(0),
        )?;
        let opportunities: i64 = conn.query_row(
            "SELECT COUNT(*) FROM opportunity_touches WHERE brand=?1 AND due_at>=?2 AND due_at<?3
             AND status IN ('scheduled','sent')",
            params![brand, start, end],
            |r| r.get(0),
        )?;
        let conversations: i64 = conn.query_row(
            "SELECT COUNT(*) FROM conversation_messages m
             JOIN conversations c ON c.id=m.conversation_id
             WHERE c.brand=?1 AND m.direction='outbound' AND m.status='sent'
               AND m.sent_at>=?2 AND m.sent_at<?3",
            params![brand, start, end],
            |r| r.get(0),
        )?;
        Ok((regular + opportunities + conversations).max(0) as usize)
    }

    /// Approved funding emails are fixed reservations while the sales
    /// portfolio is rebalanced. Sent funding mail is already included in
    /// `sent_touch_count_between`, so this method intentionally counts only the
    /// unsent scheduled rows.
    pub fn scheduled_opportunity_count_between(
        &self,
        brand: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM opportunity_touches
             WHERE brand=?1 AND status='scheduled' AND due_at>=?2 AND due_at<?3",
            params![brand, start.to_rfc3339(), end.to_rfc3339()],
            |r| r.get(0),
        )?;
        Ok(n.max(0) as usize)
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
        let conversations: i64 = conn.query_row(
            "SELECT COUNT(*) FROM conversation_messages m
             JOIN conversations c ON c.id=m.conversation_id
             WHERE c.brand=?1 AND m.direction='outbound' AND m.status='sent'
               AND m.sent_at>=?2 AND m.sent_at<?3",
            params![brand, start, end],
            |r| r.get(0),
        )?;
        Ok((regular + opportunities + conversations).max(0) as usize)
    }

    /// Distinct people at one account (lead) whose *first* touch (stage 1) was
    /// actually sent within the window — i.e. the number of new conversational
    /// fronts opened at that account today. This is the quantity the
    /// per-account/day throttle bounds so a blast of cold emails can't land on
    /// five people at the same company within hours.
    pub fn account_openers_sent_between(
        &self,
        lead_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let start = start.to_rfc3339();
        let end = end.to_rfc3339();
        let n: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT person_id) FROM touches
             WHERE lead_id=?1 AND stage=1 AND status='sent'
               AND sent_at>=?2 AND sent_at<?3",
            params![lead_id, start, end],
            |r| r.get(0),
        )?;
        Ok(n.max(0) as usize)
    }

    /// People at one account already engaged in outreach — currently contacted
    /// or who have replied. Bounds how many parallel fronts one account carries,
    /// across days rather than just within a single day.
    pub fn account_engaged_people(&self, lead_id: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT p.id) FROM people p
             WHERE p.lead_id=?1 AND (
               p.status IN ('replied','meeting_booked') OR EXISTS (
                 SELECT 1 FROM sequences s
                 WHERE s.person_id=p.id AND s.status IN ('building','active')
                   AND s.copy_policy_version=?2
               )
             )",
            params![lead_id, CURRENT_COPY_POLICY_VERSION],
            |r| r.get(0),
        )?;
        Ok(n.max(0) as usize)
    }

    /// Touches actually sent so far for one sequence. Used to stop a cadence
    /// that has gone quiet rather than marching through every remaining stage.
    pub fn sequence_sent_count(&self, sequence_id: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM touches WHERE sequence_id=?1 AND status='sent'",
            params![sequence_id],
            |r| r.get(0),
        )?;
        Ok(n.max(0) as usize)
    }

    // --- Jobs (durable background work) -----------------------------------
}

impl Db {
    /// Enqueue a job. If `dedup_key` is set and a row already holds it, this is a
    /// no-op returning the existing id — so a supervisor can re-decide every tick
    /// without piling up duplicate work.
    pub fn enqueue_job(&self, job: &Job) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let now = now();
        if !job.dedup_key.is_empty() {
            let existing: Option<String> = conn
                .query_row(
                    "SELECT id FROM jobs WHERE dedup_key=?1",
                    params![job.dedup_key],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(id) = existing {
                return Ok(id);
            }
        }
        let id = if job.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            job.id.clone()
        };
        let next_run_at = if job.next_run_at.is_empty() {
            now.clone()
        } else {
            job.next_run_at.clone()
        };
        let payload = if job.payload.is_empty() {
            "{}".to_string()
        } else {
            job.payload.clone()
        };
        let max_attempts = if job.max_attempts <= 0 {
            5
        } else {
            job.max_attempts
        };
        conn.execute(
            "INSERT INTO jobs (id,brand,kind,payload,status,priority,next_run_at,
                 attempt_count,max_attempts,dedup_key,created_at,updated_at)
             VALUES (?1,?2,?3,?4,'pending',?5,?6,0,?7,?8,?9,?9)",
            params![
                id,
                job.brand,
                job.kind,
                payload,
                job.priority,
                next_run_at,
                max_attempts,
                if job.dedup_key.is_empty() {
                    None
                } else {
                    Some(job.dedup_key.clone())
                },
                now,
            ],
        )?;
        Ok(id)
    }

    /// Atomically lease the next due job to `worker`, reclaiming any whose lease
    /// has expired (a worker that died mid-flight). Bumps the attempt counter as
    /// part of the same statement, so a crash after claim still counts as a try.
    /// Returns None when nothing is due.
    pub fn claim_job(&self, worker: &str, lease_secs: i64) -> Result<Option<Job>> {
        let conn = self.conn.lock().unwrap();
        let now = now();
        let lease_until = (Utc::now() + chrono::Duration::seconds(lease_secs.max(1))).to_rfc3339();
        conn.query_row(
            "UPDATE jobs SET status='leased', lease_owner=?1, lease_expires_at=?2,
                 attempt_count=attempt_count+1, updated_at=?3
             WHERE id = (
                 SELECT id FROM jobs
                 WHERE (status='pending' AND next_run_at<=?3)
                    OR (status='leased' AND lease_expires_at IS NOT NULL
                        AND lease_expires_at<?3)
                 ORDER BY priority DESC, next_run_at ASC
                 LIMIT 1
             )
             RETURNING *",
            params![worker, lease_until, now],
            |r| Ok(row_to_job(r)),
        )
        .optional()
        .map_err(Into::into)
    }

    /// Mark a leased job done and stash whatever the worker returned.
    pub fn complete_job(&self, id: &str, result: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE jobs SET status='done', result=?2, lease_owner=NULL,
                 lease_expires_at=NULL, last_error=NULL, updated_at=?3 WHERE id=?1",
            params![id, result, now()],
        )?;
        Ok(())
    }

    /// Record a failed attempt: retry with linear backoff until `max_attempts`
    /// is reached, then park the job as 'dead' (dead-letter) for a human to see.
    /// Returns the resulting status ('pending' or 'dead').
    pub fn fail_job(&self, id: &str, error: &str) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let now = now();
        let (attempt, max): (i64, i64) = conn.query_row(
            "SELECT attempt_count, max_attempts FROM jobs WHERE id=?1",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if attempt >= max {
            conn.execute(
                "UPDATE jobs SET status='dead', last_error=?2, lease_owner=NULL,
                     lease_expires_at=NULL, updated_at=?3 WHERE id=?1",
                params![id, error, now],
            )?;
            Ok("dead".into())
        } else {
            let next = (Utc::now() + chrono::Duration::seconds(60 * attempt.max(1))).to_rfc3339();
            conn.execute(
                "UPDATE jobs SET status='pending', last_error=?2, next_run_at=?3,
                     lease_owner=NULL, lease_expires_at=NULL, updated_at=?4 WHERE id=?1",
                params![id, error, next, now],
            )?;
            Ok("pending".into())
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn get_job(&self, id: &str) -> Result<Option<Job>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT * FROM jobs WHERE id=?1", params![id], |r| {
            Ok(row_to_job(r))
        })
        .optional()
        .map_err(Into::into)
    }

    /// Job counts by status for one brand (or all) — the queue's health at a
    /// glance, including the dead-letter backlog that must never grow unnoticed.
    pub fn job_status_counts(&self, brand: Option<&str>) -> Result<Vec<(String, usize)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT status, COUNT(*) FROM jobs WHERE (?1 IS NULL OR brand=?1) GROUP BY status",
        )?;
        let rows = stmt.query_map(params![brand], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?.max(0) as usize))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

impl Db {
    pub fn upcoming_calendar(&self, brand: &str, limit: usize) -> Result<Vec<CalendarEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut entries = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT t.due_at,t.stage,t.channel,t.status,p.name,l.name,t.purpose,
                        t.recipient_timezone,t.scheduled_rule
                 FROM touches t
                 JOIN sequences s ON s.id=t.sequence_id
                 JOIN people p ON p.id=t.person_id
                 JOIN leads l ON l.id=t.lead_id
                 WHERE t.brand=?1 AND t.status='scheduled'
                   AND s.status='active' AND s.copy_policy_version=?3
                 ORDER BY t.due_at ASC LIMIT ?2",
            )?;
            let rows = stmt.query_map(
                params![brand, limit as i64, CURRENT_COPY_POLICY_VERSION],
                |row| {
                    Ok(CalendarEntry {
                        brand: brand.to_string(),
                        due_at: row.get(0)?,
                        stage: row.get(1)?,
                        channel: row.get(2)?,
                        status: row.get(3)?,
                        recipient: row.get(4)?,
                        account: row.get(5)?,
                        purpose: row.get(6)?,
                        recipient_timezone: row.get(7)?,
                        scheduled_rule: row.get(8)?,
                        motion: "sales".into(),
                    })
                },
            )?;
            entries.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
        }
        {
            let mut stmt = conn.prepare(
                "SELECT t.due_at,t.stage,'email',t.status,c.name,o.title,t.purpose,
                        t.recipient_timezone,t.scheduled_rule
                 FROM opportunity_touches t
                 JOIN opportunity_contacts c ON c.id=t.contact_id
                 JOIN opportunities o ON o.id=t.opportunity_id
                 WHERE t.brand=?1 AND t.status='scheduled'
                 ORDER BY t.due_at ASC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![brand, limit as i64], |row| {
                Ok(CalendarEntry {
                    brand: brand.to_string(),
                    due_at: row.get(0)?,
                    stage: row.get(1)?,
                    channel: row.get(2)?,
                    status: row.get(3)?,
                    recipient: row.get(4)?,
                    account: row.get(5)?,
                    purpose: row.get(6)?,
                    recipient_timezone: row.get(7)?,
                    scheduled_rule: row.get(8)?,
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

    pub fn touch_by_message_id(&self, brand: &str, message_id: &str) -> Result<Option<Touch>> {
        if message_id.trim().is_empty() {
            return Ok(None);
        }
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM touches WHERE brand=?1 AND message_id=?2 LIMIT 1",
                params![brand, message_id.trim()],
                |row| Ok(row_to_touch(row)),
            )
            .optional()?)
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

    /// Count reviewed drafts that a higher-level policy gate may schedule.
    pub(crate) fn reviewed_draft_touch_count(
        &self,
        brand: Option<&str>,
        person_id: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM touches WHERE status='draft' \
             AND (lower(channel)='email' OR (lower(channel)='linkedin_or_email' AND \
                  COALESCE((SELECT linkedin_status FROM people WHERE people.id=touches.person_id),'unknown')<>'connected')) \
             AND review_passes=1 \
             AND EXISTS ( \
               SELECT 1 FROM sequences s WHERE s.id=touches.sequence_id \
                 AND s.status='active' AND s.copy_policy_version=?3 \
             ) \
             AND (?1 IS NULL OR brand=?1) AND (?2 IS NULL OR person_id=?2)",
            params![brand, person_id, CURRENT_COPY_POLICY_VERSION],
            |row| row.get(0),
        )?;
        Ok(n.max(0) as usize)
    }

    /// Low-level draft → scheduled transition. Callers must pass through the GTM
    /// delivery gate first; keeping this crate-private prevents a UI action from
    /// accidentally treating copy review as account qualification.
    pub(crate) fn schedule_reviewed_touches(
        &self,
        brand: Option<&str>,
        person_id: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE touches SET status='scheduled' WHERE status='draft' \
             AND (lower(channel)='email' OR (lower(channel)='linkedin_or_email' AND \
                  COALESCE((SELECT linkedin_status FROM people WHERE people.id=touches.person_id),'unknown')<>'connected')) \
             AND review_passes=1 \
             AND EXISTS (
               SELECT 1 FROM sequences s WHERE s.id=touches.sequence_id
                 AND s.status='active' AND s.copy_policy_version=?3
             ) \
             AND (?1 IS NULL OR brand=?1) AND (?2 IS NULL OR person_id=?2)",
            params![brand, person_id, CURRENT_COPY_POLICY_VERSION],
        )?;
        Ok(n)
    }

    pub fn active_sequence_for_person(&self, person_id: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT id FROM sequences WHERE person_id=?1 AND status='active'
                 ORDER BY CASE WHEN copy_policy_version=?2 THEN 0 ELSE 1 END,
                          created_at DESC LIMIT 1",
                params![person_id, CURRENT_COPY_POLICY_VERSION],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Whether this specific recipient already has the complete reviewed shape
    /// requested by the current motion. Partial-account retries use this to
    /// preserve good copy for four people while repairing the fifth.
    pub fn person_has_current_reviewed_sequence(
        &self,
        person_id: &str,
        expected_touches: usize,
    ) -> Result<bool> {
        let expected_touches = match expected_touches {
            1 => 1,
            2 => 2,
            7 => 7,
            _ => 4,
        };
        let conn = self.conn.lock().unwrap();
        let exists: i64 = conn.query_row(
            "SELECT EXISTS(
               SELECT 1 FROM sequences s
               WHERE s.person_id=?1 AND s.status='active' AND s.copy_policy_version=?2
                 AND (SELECT COUNT(*) FROM touches t WHERE t.sequence_id=s.id)=?3
                 AND (SELECT MIN(t.stage) FROM touches t WHERE t.sequence_id=s.id)=1
                 AND (SELECT MAX(t.stage) FROM touches t WHERE t.sequence_id=s.id)=
                     (SELECT COUNT(*) FROM touches t WHERE t.sequence_id=s.id)
                 AND (SELECT COUNT(DISTINCT t.stage) FROM touches t WHERE t.sequence_id=s.id)=
                     (SELECT COUNT(*) FROM touches t WHERE t.sequence_id=s.id)
                 AND NOT EXISTS (
                   SELECT 1 FROM touches t WHERE t.sequence_id=s.id
                     AND (COALESCE(t.review_passes,0)<>1 OR trim(t.body)=''
                          OR trim(t.body)='Writing draft…')
                 )
             )",
            params![person_id, CURRENT_COPY_POLICY_VERSION, expected_touches],
            |row| row.get(0),
        )?;
        Ok(exists == 1)
    }

    /// Whether this recipient has already consumed a generation attempt under
    /// the active copy contract. Portfolio filling uses this to try fresh
    /// qualified inventory before spending again on a current-policy reject;
    /// failures from retired policies remain eligible for a clean attempt.
    pub fn person_has_current_policy_attempt(&self, person_id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let exists: i64 = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sequences
                WHERE person_id=?1 AND copy_policy_version=?2
            )",
            params![person_id, CURRENT_COPY_POLICY_VERSION],
            |row| row.get(0),
        )?;
        Ok(exists == 1)
    }

    /// How many people at an account already have a complete, reviewed sequence
    /// under the current copy policy and requested touch shape.
    pub fn lead_current_reviewed_sequence_count(
        &self,
        lead_id: &str,
        expected_touches: usize,
    ) -> Result<usize> {
        let expected_touches = match expected_touches {
            1 => 1,
            2 => 2,
            7 => 7,
            _ => 4,
        };
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT s.person_id)
             FROM sequences s
             WHERE s.lead_id=?1 AND s.status='active' AND s.copy_policy_version=?2
               AND (SELECT COUNT(*) FROM touches t WHERE t.sequence_id=s.id)=?3
               AND (SELECT MIN(t.stage) FROM touches t WHERE t.sequence_id=s.id)=1
               AND (SELECT MAX(t.stage) FROM touches t WHERE t.sequence_id=s.id)=
                   (SELECT COUNT(*) FROM touches t WHERE t.sequence_id=s.id)
               AND (SELECT COUNT(DISTINCT t.stage) FROM touches t WHERE t.sequence_id=s.id)=
                   (SELECT COUNT(*) FROM touches t WHERE t.sequence_id=s.id)
               AND NOT EXISTS (
                 SELECT 1 FROM touches t WHERE t.sequence_id=s.id
                   AND (COALESCE(t.review_passes,0)<>1 OR trim(t.body)=''
                        OR trim(t.body)='Writing draft…')
               )",
            params![lead_id, CURRENT_COPY_POLICY_VERSION, expected_touches],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as usize)
    }

    /// Exact findings from the most recent rejected sequence for this person.
    /// A full-motion rewrite feeds these back to the writer instead of paying
    /// for a statistically independent retry that can repeat the same defects.
    pub fn latest_rejected_sequence_feedback(&self, person_id: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT t.review_issues,t.error FROM touches t
             WHERE t.sequence_id=(
               SELECT s.id FROM sequences s
               WHERE s.person_id=?1 AND s.status='rejected'
               ORDER BY s.created_at DESC, s.rowid DESC LIMIT 1
             )
             ORDER BY t.stage ASC",
        )?;
        let rows = stmt.query_map(params![person_id], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            ))
        })?;
        let mut feedback = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for row in rows {
            let (issues, error) = row?;
            let mut findings = jd(&issues);
            if findings.is_empty() && !error.trim().is_empty() {
                findings.push(error);
            }
            for finding in findings {
                let finding = finding.trim().to_string();
                if !finding.is_empty() && seen.insert(finding.clone()) {
                    feedback.push(finding);
                }
            }
        }
        Ok(feedback)
    }

    // --- Sales conversations ----------------------------------------------

    /// Resolve an inbound message to a durable conversation. RFC thread
    /// headers win over `From:` so a CC'd referral stays attached to the
    /// original account even when their address was never sourced.
    pub fn conversation_for_inbound(
        &self,
        brand: &str,
        from_email: &str,
        subject: &str,
        thread_ids: &[String],
    ) -> Result<Option<Conversation>> {
        let conn = self.conn.lock().unwrap();

        for message_id in thread_ids
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
        {
            let existing = conn
                .query_row(
                    "SELECT c.* FROM conversations c
                     JOIN conversation_messages m ON m.conversation_id=c.id
                     WHERE c.brand=?1 AND m.message_id=?2 LIMIT 1",
                    params![brand, message_id],
                    |row| Ok(row_to_conversation(row)),
                )
                .optional()?;
            if existing.is_some() {
                return Ok(existing);
            }

            let touch: Option<(String, String, String)> = conn
                .query_row(
                    "SELECT sequence_id,person_id,lead_id FROM touches
                     WHERE brand=?1 AND message_id=?2 LIMIT 1",
                    params![brand, message_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            if let Some((sequence_id, person_id, lead_id)) = touch {
                return Ok(Some(ensure_conversation(
                    &conn,
                    brand,
                    &sequence_id,
                    &person_id,
                    &lead_id,
                    subject,
                )?));
            }
        }

        // Headerless replies are a reality (forwarders and some CRM relays
        // strip them). Fall back to a known sender's most recent sequence.
        let identity: Option<(String, String)> = conn
            .query_row(
                "SELECT id,lead_id FROM people
                 WHERE brand=?1 AND lower(email)=lower(?2) LIMIT 1",
                params![brand, from_email],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((person_id, lead_id)) = identity else {
            return Ok(None);
        };
        let sequence_id: Option<String> = conn
            .query_row(
                "SELECT id FROM sequences WHERE person_id=?1
                 ORDER BY CASE WHEN status='active' THEN 0 ELSE 1 END, created_at DESC LIMIT 1",
                params![person_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(Some(ensure_conversation(
            &conn,
            brand,
            sequence_id.as_deref().unwrap_or(""),
            &person_id,
            &lead_id,
            subject,
        )?))
    }

    pub fn get_conversation(&self, id: &str) -> Result<Option<Conversation>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT * FROM conversations WHERE id=?1",
            params![id],
            |row| Ok(row_to_conversation(row)),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn insert_conversation_message(&self, message: &ConversationMessage) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        if !message.message_id.trim().is_empty() {
            let existing: Option<String> = conn
                .query_row(
                    "SELECT id FROM conversation_messages WHERE message_id=?1 LIMIT 1",
                    params![message.message_id],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(id) = existing {
                return Ok(id);
            }
        }
        let id = if message.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            message.id.clone()
        };
        let created_at = if message.created_at.is_empty() {
            now()
        } else {
            message.created_at.clone()
        };
        conn.execute(
            "INSERT INTO conversation_messages
             (id,conversation_id,direction,sender_email,recipient_email,participants,
              subject,body,status,message_id,in_reply_to,references_json,classification,
              action,offered_slots,mailbox_id,sent_at,created_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
            params![
                id,
                message.conversation_id,
                message.direction,
                message.sender_email,
                message.recipient_email,
                js(&message.participants),
                message.subject,
                message.body,
                message.status,
                message.message_id,
                message.in_reply_to,
                js(&message.references),
                message.classification,
                message.action,
                js(&message.offered_slots),
                message.mailbox_id,
                message.sent_at,
                created_at,
            ],
        )?;
        conn.execute(
            "UPDATE conversations SET last_message_at=?2,updated_at=?2,
             subject=CASE WHEN subject='' THEN ?3 ELSE subject END WHERE id=?1",
            params![message.conversation_id, created_at, message.subject],
        )?;
        Ok(id)
    }

    pub fn list_conversation_messages(
        &self,
        conversation_id: &str,
    ) -> Result<Vec<ConversationMessage>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM conversation_messages WHERE conversation_id=?1
             ORDER BY created_at ASC, id ASC",
        )?;
        let rows = stmt.query_map(params![conversation_id], |row| {
            Ok(row_to_conversation_message(row))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn due_conversation_messages(&self, limit: i64) -> Result<Vec<ConversationMessage>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM conversation_messages
             WHERE direction='outbound' AND status='scheduled'
             ORDER BY created_at ASC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| Ok(row_to_conversation_message(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Reserve an approved reply for exactly one live worker.
    pub fn claim_conversation_message_for_send(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "UPDATE conversation_messages SET status='sending' \
             WHERE id=?1 AND direction='outbound' AND status='scheduled'",
            params![id],
        )? == 1)
    }

    pub fn approve_conversation_messages(
        &self,
        brand: Option<&str>,
        conversation_id: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "UPDATE conversation_messages SET status='scheduled'
             WHERE direction='outbound' AND status='draft'
               AND conversation_id IN (
                   SELECT id FROM conversations
                   WHERE (?1 IS NULL OR brand=?1) AND (?2 IS NULL OR id=?2)
               )",
            params![brand, conversation_id],
        )?)
    }

    pub fn set_conversation_message_status(
        &self,
        id: &str,
        status: &str,
        mailbox_id: &str,
        message_id: &str,
        action: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let sent_at = (status == "sent").then(now).unwrap_or_default();
        conn.execute(
            "UPDATE conversation_messages SET status=?2,mailbox_id=?3,
             sender_email=CASE WHEN ?3='' THEN sender_email ELSE
               COALESCE((SELECT from_email FROM mailboxes WHERE id=?3),sender_email) END,
             message_id=CASE WHEN ?4='' THEN message_id ELSE ?4 END,
             action=CASE WHEN ?5='' THEN action ELSE ?5 END,
             sent_at=CASE WHEN ?2='sent' THEN ?6 ELSE sent_at END WHERE id=?1",
            params![id, status, mailbox_id, message_id, action, sent_at],
        )?;
        Ok(())
    }

    pub fn last_conversation_message_id(&self, conversation_id: &str) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT message_id FROM conversation_messages
                 WHERE conversation_id=?1 AND message_id<>''
                   AND status IN ('received','sent')
                 ORDER BY created_at DESC,id DESC LIMIT 1",
                params![conversation_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or_default())
    }

    /// Slots are eligible for booking only if they appeared in a message that
    /// was actually sent, not merely generated in a draft.
    pub fn sent_offered_slots(&self, conversation_id: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT offered_slots FROM conversation_messages
             WHERE conversation_id=?1 AND direction='outbound' AND status='sent'",
        )?;
        let rows = stmt.query_map(params![conversation_id], |row| row.get::<_, String>(0))?;
        let mut slots = Vec::new();
        for row in rows {
            slots.extend(jd(&row?));
        }
        slots.sort();
        slots.dedup();
        Ok(slots)
    }

    pub fn record_meeting(&self, meeting: &Meeting) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if meeting.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            meeting.id.clone()
        };
        let timestamp = now();
        conn.execute(
            "INSERT INTO meetings
             (id,conversation_id,brand,person_id,attendee_email,starts_at,ends_at,
              timezone,status,google_event_id,html_link,meet_link,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13)
             ON CONFLICT(conversation_id,starts_at) DO UPDATE SET
              status=CASE WHEN meetings.status='booked' THEN meetings.status ELSE excluded.status END,
              google_event_id=CASE WHEN meetings.status='booked' THEN meetings.google_event_id ELSE excluded.google_event_id END,
              html_link=CASE WHEN meetings.status='booked' THEN meetings.html_link ELSE excluded.html_link END,
              meet_link=CASE WHEN meetings.status='booked' THEN meetings.meet_link ELSE excluded.meet_link END,
              updated_at=?13",
            params![
                id,
                meeting.conversation_id,
                meeting.brand,
                meeting.person_id,
                meeting.attendee_email,
                meeting.starts_at,
                meeting.ends_at,
                meeting.timezone,
                status_or(&meeting.status, "booked"),
                meeting.google_event_id,
                meeting.html_link,
                meeting.meet_link,
                timestamp,
            ],
        )?;
        let (stored_id, stored_status): (String, String) = conn.query_row(
            "SELECT id,status FROM meetings WHERE conversation_id=?1 AND starts_at=?2",
            params![meeting.conversation_id, meeting.starts_at],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let conversation_status = if stored_status == "pending" {
            "meeting_pending"
        } else {
            "meeting_booked"
        };
        conn.execute(
            "UPDATE conversations SET status=?2,updated_at=?3 WHERE id=?1",
            params![meeting.conversation_id, conversation_status, timestamp],
        )?;
        Ok(stored_id)
    }

    pub fn update_meeting_booked(
        &self,
        id: &str,
        google_event_id: &str,
        html_link: &str,
        meet_link: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let timestamp = now();
        conn.execute(
            "UPDATE meetings SET status='booked',google_event_id=?2,html_link=?3,
             meet_link=?4,updated_at=?5 WHERE id=?1",
            params![id, google_event_id, html_link, meet_link, timestamp],
        )?;
        conn.execute(
            "UPDATE conversations SET status='meeting_booked',updated_at=?2
             WHERE id=(SELECT conversation_id FROM meetings WHERE id=?1)",
            params![id, timestamp],
        )?;
        Ok(())
    }

    pub fn list_meetings(&self, brand: Option<&str>) -> Result<Vec<Meeting>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM meetings WHERE (?1 IS NULL OR brand=?1) ORDER BY starts_at ASC",
        )?;
        let rows = stmt.query_map(params![brand], |row| Ok(row_to_meeting(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
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

    /// Reserve one scheduled opportunity touch for exactly one live worker.
    pub fn claim_opportunity_touch_for_send(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "UPDATE opportunity_touches SET status='sending',error='' \
             WHERE id=?1 AND status='scheduled'",
            params![id],
        )? == 1)
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

    pub fn active_sequence_principles_for_person(&self, person_id: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let value = conn
            .query_row(
                "SELECT applied_principles FROM sequences \
                 WHERE person_id=?1 AND status='active' ORDER BY created_at DESC LIMIT 1",
                params![person_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(value.map(|value| jd(&value)).unwrap_or_default())
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
            "INSERT INTO replies (id,conversation_id,person_id,sequence_id,ts,from_email,subject,body,\
             classification,action_taken,message_id,in_reply_to) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                id,
                r.conversation_id,
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
        let mut stmt = conn.prepare("SELECT * FROM replies ORDER BY ts DESC LIMIT ?1")?;
        let rows = stmt.query_map(params![limit], |r| {
            Ok(Reply {
                id: g(r, "id"),
                conversation_id: g(r, "conversation_id"),
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

    /// Record (or reinforce) a durable lesson for a brand — a qualification skip,
    /// an outreach failure, whatever the funnel learns — so future runs don't
    /// start from a clean state. Repeated observations about the same subject bump
    /// `hits` and refresh the detail instead of piling up duplicate rows. The
    /// subject_key (an Apollo org id, a domain, a persona) is the dedup handle;
    /// when it's blank we fall back to the human subject so a keyless learning
    /// still dedups sensibly.
    pub fn record_learning(
        &self,
        brand: &str,
        kind: &str,
        subject: &str,
        subject_key: &str,
        detail: &str,
    ) -> Result<()> {
        let key = if subject_key.trim().is_empty() {
            subject
        } else {
            subject_key
        };
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO learnings (id,brand,kind,subject,subject_key,detail,hits,created_at,updated_at) \
             VALUES (?1,?2,?3,?4,?5,?6,1,?7,?7) \
             ON CONFLICT(brand,kind,subject_key) DO UPDATE SET \
               hits=hits+1, detail=excluded.detail, subject=excluded.subject, updated_at=excluded.updated_at",
            params![
                Uuid::new_v4().to_string(),
                brand,
                kind,
                subject,
                key,
                detail,
                now()
            ],
        )?;
        Ok(())
    }

    /// Recent learnings for a brand (or all brands when `brand` is None), most
    /// reinforced first — the material fed back into targeting and shown to the
    /// operator as accumulated business intelligence.
    pub fn recent_learnings(
        &self,
        brand: Option<&str>,
        kind: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Learning>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT brand,kind,subject,detail,hits,updated_at FROM learnings \
             WHERE (?1 IS NULL OR brand=?1) AND (?2 IS NULL OR kind=?2) \
             ORDER BY hits DESC, updated_at DESC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![brand, kind, limit as i64], |r| {
            Ok(Learning {
                brand: r.get(0)?,
                kind: r.get(1)?,
                subject: r.get(2)?,
                detail: r.get(3)?,
                hits: r.get(4)?,
                updated_at: r.get(5)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Stable subject keys we've already recorded a learning of `kind` about for a
    /// brand — used to skip re-evaluating (and re-researching) known rejects.
    #[cfg(test)]
    pub fn learning_keys(
        &self,
        brand: &str,
        kind: &str,
    ) -> Result<std::collections::HashSet<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT subject_key FROM learnings WHERE brand=?1 AND kind=?2 AND subject_key<>''",
        )?;
        let rows = stmt.query_map(params![brand, kind], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<std::collections::HashSet<_>>>()?)
    }

    /// Qualification skips that are safe to treat as durable deduplication for
    /// the caller's current research/qualification policy. A crawler, evidence,
    /// or gate change must use a new tag so old misses are reconsidered once.
    pub fn durable_qualification_skip_keys(
        &self,
        brand: &str,
        policy_tag: &str,
    ) -> Result<std::collections::HashSet<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT subject_key,detail FROM learnings \
             WHERE brand=?1 AND kind='qualification_skip' AND subject_key<>''",
        )?;
        let rows = stmt.query_map(params![brand], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut keys = std::collections::HashSet::new();
        let prefix = format!("{} ", policy_tag.trim());
        for row in rows {
            let (key, detail) = row?;
            if detail.starts_with(&prefix) {
                keys.insert(key);
            }
        }
        Ok(keys)
    }

    // --- GTM engineering --------------------------------------------------

    pub fn insert_signal_definition_if_absent(
        &self,
        definition: &SignalDefinition,
    ) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if definition.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            definition.id.clone()
        };
        let timestamp = now();
        conn.execute(
            "INSERT INTO signal_definitions
             (id,brand,key,name,description,topic,entity_type,value_type,source_kind,owner,
              refresh_cadence,freshness_seconds,evidence_required,minimum_confidence,version,
              status,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?17)
             ON CONFLICT(brand,key,version) DO NOTHING",
            params![
                id,
                definition.brand,
                definition.key,
                definition.name,
                definition.description,
                definition.topic,
                definition.entity_type,
                definition.value_type,
                definition.source_kind,
                definition.owner,
                definition.refresh_cadence,
                definition.freshness_seconds.max(0),
                definition.evidence_required,
                definition.minimum_confidence.clamp(0.0, 1.0),
                definition.version.max(1),
                status_or(&definition.status, "active"),
                timestamp,
            ],
        )?;
        Ok(conn.query_row(
            "SELECT id FROM signal_definitions WHERE brand=?1 AND key=?2 AND version=?3",
            params![definition.brand, definition.key, definition.version.max(1)],
            |row| row.get(0),
        )?)
    }

    pub fn list_signal_definitions(&self, brand: Option<&str>) -> Result<Vec<SignalDefinition>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM signal_definitions WHERE (?1 IS NULL OR brand=?1)
             ORDER BY brand,key,version DESC",
        )?;
        let rows = stmt.query_map(params![brand], |row| Ok(row_to_signal_definition(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn record_signal_observation(&self, observation: &SignalObservation) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        record_signal_observation_conn(&conn, observation)
    }

    pub fn record_signal_candidates(
        &self,
        brand: &str,
        lead_id: &str,
        candidates: &[crate::gtm::SignalCandidate],
        source_name: &str,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        if source_name == "source.refresh" {
            // An official-site refresh is the canonical account snapshot. Keep
            // prior rows for audit, but remove stale qualification and legacy
            // research from the active evidence set.
            conn.execute(
                "UPDATE signal_observations
                 SET status='superseded',updated_at=?1
                 WHERE brand=?2 AND lead_id=?3 AND person_id=''
                   AND source_name IN ('source.refresh','source.qualify','account_research','legacy_account_research')
                   AND status IN ('observed','verified')",
                params![now(), brand, lead_id],
            )?;
        }
        let mut recorded = 0usize;
        for candidate in candidates {
            if candidate.definition_key.trim().is_empty()
                || candidate.evidence.trim().is_empty()
                || candidate.source_url.trim().is_empty()
            {
                // Account qualification is allowed to retain an internal note,
                // but it cannot become evidence without a resolvable source.
                continue;
            }
            let observation = SignalObservation {
                brand: brand.to_string(),
                definition_key: candidate.definition_key.trim().to_string(),
                lead_id: lead_id.to_string(),
                source_name: source_name.to_string(),
                source_url: candidate.source_url.trim().to_string(),
                evidence: candidate.evidence.trim().to_string(),
                confidence: candidate.confidence.clamp(0.0, 1.0),
                status: "observed".into(),
                ..Default::default()
            };
            if record_signal_observation_conn(&conn, &observation).is_ok() {
                recorded += 1;
            }
        }
        Ok(recorded)
    }

    pub fn list_active_signal_observations(
        &self,
        brand: Option<&str>,
        lead_id: Option<&str>,
        person_id: Option<&str>,
    ) -> Result<Vec<SignalObservation>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT o.* FROM signal_observations o
             JOIN signal_definitions d ON d.id=o.definition_id
             WHERE (?1 IS NULL OR o.brand=?1)
               AND (?2 IS NULL OR o.lead_id=?2)
               AND (?3 IS NULL OR o.person_id=?3)
               AND o.status IN ('observed','verified')
               AND o.confidence>=d.minimum_confidence
               AND (o.expires_at='' OR o.expires_at>?4)
               AND d.status='active'
             ORDER BY o.observed_at DESC",
        )?;
        let rows = stmt.query_map(params![brand, lead_id, person_id, now()], |row| {
            Ok(row_to_signal_observation(row))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_signal_observations(
        &self,
        brand: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SignalObservation>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM signal_observations WHERE (?1 IS NULL OR brand=?1)
             ORDER BY observed_at DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![brand, limit.max(1) as i64], |row| {
            Ok(row_to_signal_observation(row))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn expire_signal_observations(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "UPDATE signal_observations SET status='expired',updated_at=?1
             WHERE status IN ('observed','verified') AND expires_at<>'' AND expires_at<=?1",
            params![now()],
        )?)
    }

    pub fn backfill_legacy_signal_observations(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id,brand,signals,updated_at FROM leads WHERE signals<>'' AND signals<>'[]'",
        )?;
        let leads = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        let mut inserted = 0usize;
        for (lead_id, brand, raw, observed_at) in leads {
            for evidence in jd(&raw) {
                let observation = SignalObservation {
                    brand: brand.clone(),
                    definition_key: "account.fit_evidence".into(),
                    lead_id: lead_id.clone(),
                    source_name: "legacy_account_research".into(),
                    value_json: serde_json::json!({"legacy": true}).to_string(),
                    evidence,
                    confidence: 0.70,
                    observed_at: observed_at.clone(),
                    status: "observed".into(),
                    ..Default::default()
                };
                if record_signal_observation_conn(&conn, &observation).is_ok() {
                    inserted += 1;
                }
            }
        }
        // Contacts created before the versioned signal system already carry a
        // researched role/vantage, but had no person-level observation. The
        // CRM could display a verified process owner while the readiness gate
        // saw no reachable owner at all. Backfill only identity, title, mapped
        // vantage, and channel verification here; never copy the old
        // `can_observe` hypothesis into evidence.
        let mut stmt = conn.prepare(
            "SELECT id,lead_id,brand,title,vantage,email_status,linkedin_url,updated_at
             FROM people WHERE vantage<>''",
        )?;
        let people = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        for (person_id, lead_id, brand, title, vantage, email_status, linkedin_url, observed_at) in
            people
        {
            let reachable =
                email_status.eq_ignore_ascii_case("verified") || !linkedin_url.trim().is_empty();
            let evidence = format!(
                "{} — CRM contact mapped as {}; {}",
                title.trim(),
                vantage.trim(),
                if reachable {
                    "a verified email or LinkedIn profile is on file"
                } else {
                    "identity is on file but no verified outreach channel is available"
                }
            );
            let observation = SignalObservation {
                brand,
                definition_key: "contact.workflow_vantage".into(),
                lead_id,
                person_id,
                source_name: "legacy_contact_vantage".into(),
                value_json: serde_json::json!({
                    "legacy": true,
                    "vantage": vantage,
                    "reachable": reachable,
                })
                .to_string(),
                evidence,
                confidence: 0.70,
                observed_at,
                status: "observed".into(),
                ..Default::default()
            };
            if record_signal_observation_conn(&conn, &observation).is_ok() {
                inserted += 1;
            }
        }
        Ok(inserted)
    }

    pub fn insert_gtm_play_if_absent(&self, play: &GtmPlay) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if play.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            play.id.clone()
        };
        let timestamp = now();
        conn.execute(
            "INSERT INTO gtm_plays
             (id,brand,key,version,name,lifecycle,motion,target_icp,target_vantages,
              required_signal_keys,minimum_signal_matches,hypothesis,action_policy,proof_type,
              proof_description,success_metric,kill_condition,source_refs,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?19)
             ON CONFLICT(brand,key,version) DO NOTHING",
            params![
                id,
                play.brand,
                play.key,
                play.version.max(1),
                play.name,
                status_or(&play.lifecycle, "candidate"),
                play.motion,
                play.target_icp,
                js(&play.target_vantages),
                js(&play.required_signal_keys),
                play.minimum_signal_matches.max(1),
                play.hypothesis,
                play.action_policy,
                play.proof_type,
                play.proof_description,
                play.success_metric,
                play.kill_condition,
                js(&play.source_refs),
                timestamp,
            ],
        )?;
        Ok(conn.query_row(
            "SELECT id FROM gtm_plays WHERE brand=?1 AND key=?2 AND version=?3",
            params![play.brand, play.key, play.version.max(1)],
            |row| row.get(0),
        )?)
    }

    /// Keep superseded candidate/testing defaults as history without leaving
    /// two action-eligible versions of the same play active. A genuinely proven
    /// older version is preserved until market evidence justifies replacing it.
    pub fn retire_older_unproven_gtm_play_versions(
        &self,
        brand: &str,
        key: &str,
        current_version: i64,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "UPDATE gtm_plays SET lifecycle='retired',updated_at=?4
             WHERE brand=?1 AND key=?2 AND version<?3 AND lifecycle IN ('candidate','testing')",
            params![brand, key, current_version, now()],
        )?)
    }

    pub fn list_gtm_plays(&self, brand: Option<&str>) -> Result<Vec<GtmPlay>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM gtm_plays WHERE (?1 IS NULL OR brand=?1)
             ORDER BY brand,key,version DESC",
        )?;
        let rows = stmt.query_map(params![brand], |row| Ok(row_to_gtm_play(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn upsert_account_play_assessment(
        &self,
        assessment: &AccountPlayAssessment,
    ) -> Result<String> {
        if assessment.play_id.trim().is_empty() {
            anyhow::bail!("account assessment requires a versioned play");
        }
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM account_play_assessments WHERE lead_id=?1 AND play_id=?2",
                params![assessment.lead_id, assessment.play_id],
                |row| row.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| {
            if assessment.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                assessment.id.clone()
            }
        });
        let timestamp = now();
        conn.execute(
            "INSERT INTO account_play_assessments
             (id,lead_id,brand,play_id,play_version,status,fit_score,matched_signal_keys,
              symptom,root_cause,current_workaround,why_now,proof_fit,evidence_gaps,
              disqualifiers,source,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?17)
             ON CONFLICT(lead_id,play_id) DO UPDATE SET
              play_version=excluded.play_version,status=excluded.status,fit_score=excluded.fit_score,
              matched_signal_keys=excluded.matched_signal_keys,symptom=excluded.symptom,
              root_cause=excluded.root_cause,current_workaround=excluded.current_workaround,
              why_now=excluded.why_now,proof_fit=excluded.proof_fit,
              evidence_gaps=excluded.evidence_gaps,disqualifiers=excluded.disqualifiers,
              source=excluded.source,updated_at=excluded.updated_at",
            params![
                id,
                assessment.lead_id,
                assessment.brand,
                assessment.play_id,
                assessment.play_version,
                status_or(&assessment.status, "research_needed"),
                assessment.fit_score.clamp(0, 100),
                js(&assessment.matched_signal_keys),
                assessment.symptom,
                assessment.root_cause,
                assessment.current_workaround,
                assessment.why_now,
                assessment.proof_fit,
                js(&assessment.evidence_gaps),
                js(&assessment.disqualifiers),
                status_or(&assessment.source, "source.qualify"),
                timestamp,
            ],
        )?;
        drop(conn);
        self.materialize_sales_opportunity(assessment)?;
        Ok(id)
    }

    pub fn relabel_unassessed_qualified_leads(
        &self,
        brand: &str,
        current_play_id: &str,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "UPDATE leads SET status='research_needed',updated_at=?3
             WHERE brand=?1 AND status='qualified'
               AND NOT EXISTS (
                 SELECT 1 FROM account_play_assessments a
                 WHERE a.lead_id=leads.id AND a.play_id=?2 AND a.status='qualified'
               )",
            params![brand, current_play_id, now()],
        )?)
    }

    pub fn list_account_play_assessments(
        &self,
        brand: Option<&str>,
    ) -> Result<Vec<AccountPlayAssessment>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM account_play_assessments WHERE (?1 IS NULL OR brand=?1)
             ORDER BY fit_score DESC,updated_at DESC",
        )?;
        let rows = stmt.query_map(params![brand], |row| {
            Ok(row_to_account_play_assessment(row))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn account_play_assessment(
        &self,
        lead_id: &str,
        play_id: &str,
    ) -> Result<Option<AccountPlayAssessment>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM account_play_assessments WHERE lead_id=?1 AND play_id=?2",
                params![lead_id, play_id],
                |row| Ok(row_to_account_play_assessment(row)),
            )
            .optional()?)
    }

    pub fn current_gtm_play(&self, brand: &str) -> Result<Option<GtmPlay>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM gtm_plays WHERE brand=?1 AND lifecycle IN ('proven','testing')
                 ORDER BY CASE lifecycle WHEN 'proven' THEN 0 ELSE 1 END,version DESC LIMIT 1",
                params![brand],
                |row| Ok(row_to_gtm_play(row)),
            )
            .optional()?)
    }

    pub fn set_gtm_play_lifecycle(&self, id: &str, lifecycle: &str) -> Result<()> {
        if !matches!(lifecycle, "candidate" | "testing" | "proven" | "retired") {
            anyhow::bail!("invalid play lifecycle '{lifecycle}'");
        }
        let conn = self.conn.lock().unwrap();
        if lifecycle == "proven" {
            let passed: i64 = conn.query_row(
                "SELECT COUNT(*) FROM proof_briefs WHERE play_id=?1 AND status='passed'",
                params![id],
                |row| row.get(0),
            )?;
            if passed < 2 {
                anyhow::bail!(
                    "a play needs at least two passed customer-data proofs before promotion; it has {passed}"
                );
            }
        }
        conn.execute(
            "UPDATE gtm_plays SET lifecycle=?2,updated_at=?3 WHERE id=?1",
            params![id, lifecycle, now()],
        )?;
        Ok(())
    }

    pub fn create_gtm_experiment(&self, experiment: &GtmExperiment) -> Result<String> {
        if !matches!(
            experiment.experiment_type.as_str(),
            "list_only" | "copy_only" | "combined"
        ) {
            anyhow::bail!("experiment type must be list_only, copy_only, or combined");
        }
        if experiment.variable.trim().is_empty()
            || experiment.control_description.trim().is_empty()
            || experiment.variant_description.trim().is_empty()
        {
            anyhow::bail!(
                "an experiment needs one variable and explicit control/variant descriptions"
            );
        }
        if experiment.constants.is_empty() {
            anyhow::bail!("record the constants so a result remains interpretable");
        }
        let conn = self.conn.lock().unwrap();
        let id = if experiment.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            experiment.id.clone()
        };
        let timestamp = now();
        conn.execute(
            "INSERT INTO gtm_experiments
             (id,brand,play_id,name,experiment_type,hypothesis,variable,constants,
              control_description,variant_description,minimum_sends_per_arm,baseline_sends,
              baseline_positive_reply_rate,success_target,failure_floor,measurement_days,status,
              starts_at,ends_at,result_json,confidence,decision,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?23)",
            params![
                id,
                experiment.brand,
                experiment.play_id,
                experiment.name,
                experiment.experiment_type,
                experiment.hypothesis,
                experiment.variable,
                js(&experiment.constants),
                experiment.control_description,
                experiment.variant_description,
                experiment.minimum_sends_per_arm.max(1),
                experiment.baseline_sends.max(0),
                experiment.baseline_positive_reply_rate.max(0.0),
                if experiment.success_target > 0.0 {
                    experiment.success_target
                } else {
                    experiment.baseline_positive_reply_rate.max(0.0) * 1.2
                },
                if experiment.failure_floor > 0.0 {
                    experiment.failure_floor
                } else {
                    experiment.baseline_positive_reply_rate.max(0.0) * 0.8
                },
                experiment.measurement_days.max(21),
                status_or(&experiment.status, "draft"),
                experiment.starts_at,
                experiment.ends_at,
                experiment.result_json,
                experiment.confidence,
                experiment.decision,
                timestamp,
            ],
        )?;
        Ok(id)
    }

    pub fn list_gtm_experiments(&self, brand: Option<&str>) -> Result<Vec<GtmExperiment>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM gtm_experiments WHERE (?1 IS NULL OR brand=?1)
             ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(params![brand], |row| Ok(row_to_gtm_experiment(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn running_experiment_for_play(&self, play_id: &str) -> Result<Option<GtmExperiment>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT * FROM gtm_experiments WHERE play_id=?1 AND status='running'
                 ORDER BY created_at DESC LIMIT 1",
                params![play_id],
                |row| Ok(row_to_gtm_experiment(row)),
            )
            .optional()?)
    }

    pub fn set_gtm_experiment_status(&self, id: &str, status: &str) -> Result<()> {
        if !matches!(
            status,
            "draft" | "running" | "measuring" | "complete" | "inconclusive" | "cancelled"
        ) {
            anyhow::bail!("invalid experiment status '{status}'");
        }
        let conn = self.conn.lock().unwrap();
        if status == "running" {
            let experiment: (String, i64, i64, f64) = conn.query_row(
                "SELECT play_id,measurement_days,baseline_sends,baseline_positive_reply_rate
                 FROM gtm_experiments WHERE id=?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            if experiment.2 < 200 || experiment.3 < 0.01 {
                anyhow::bail!(
                    "establish a healthy baseline first: at least 200 sends and 1% positive replies"
                );
            }
            let already_running: i64 = conn.query_row(
                "SELECT COUNT(*) FROM gtm_experiments WHERE play_id=?1 AND status='running' AND id<>?2",
                params![experiment.0, id],
                |row| row.get(0),
            )?;
            if already_running > 0 {
                anyhow::bail!("only one experiment may run for a play at a time");
            }
            let start = Utc::now();
            let end = start + Duration::days(experiment.1.max(21));
            conn.execute(
                "UPDATE gtm_experiments SET status='running',starts_at=?2,ends_at=?3,updated_at=?2 WHERE id=?1",
                params![id, start.to_rfc3339(), end.to_rfc3339()],
            )?;
        } else {
            conn.execute(
                "UPDATE gtm_experiments SET status=?2,updated_at=?3 WHERE id=?1",
                params![id, status, now()],
            )?;
        }
        Ok(())
    }

    pub fn evaluate_gtm_experiment(
        &self,
        id: &str,
        control_sent: i64,
        control_positive: i64,
        variant_sent: i64,
        variant_positive: i64,
    ) -> Result<GtmExperiment> {
        if control_sent <= 0
            || variant_sent <= 0
            || control_positive < 0
            || variant_positive < 0
            || control_positive > control_sent
            || variant_positive > variant_sent
        {
            anyhow::bail!("experiment counts are invalid");
        }
        let conn = self.conn.lock().unwrap();
        let mut experiment = conn.query_row(
            "SELECT * FROM gtm_experiments WHERE id=?1",
            params![id],
            |row| Ok(row_to_gtm_experiment(row)),
        )?;
        if !matches!(experiment.status.as_str(), "running" | "measuring") {
            anyhow::bail!("only a running or measuring experiment can be evaluated");
        }
        let ends_at = DateTime::parse_from_rfc3339(&experiment.ends_at)
            .map(|date| date.with_timezone(&Utc))
            .map_err(|_| anyhow::anyhow!("experiment has no valid measurement date"))?;
        if Utc::now() < ends_at {
            anyhow::bail!(
                "do not call the experiment early; measure after {}",
                experiment.ends_at
            );
        }

        let control_rate = control_positive as f64 / control_sent as f64;
        let variant_rate = variant_positive as f64 / variant_sent as f64;
        let relative_lift = if control_rate > 0.0 {
            (variant_rate - control_rate) / control_rate
        } else if variant_rate > 0.0 {
            1.0
        } else {
            0.0
        };
        let sample_met = control_sent >= experiment.minimum_sends_per_arm
            && variant_sent >= experiment.minimum_sends_per_arm;
        let isolated = experiment.experiment_type != "combined";
        let confidence = if sample_met && isolated {
            "high"
        } else if sample_met {
            "medium"
        } else {
            "low"
        };
        let (status, decision) = if !sample_met {
            ("inconclusive", "insufficient_sample_do_not_adopt")
        } else if !isolated {
            ("inconclusive", "combined_test_hypothesis_generation_only")
        } else if variant_rate >= experiment.success_target && relative_lift >= 0.20 {
            ("complete", "adopt_variant_as_new_baseline")
        } else if relative_lift >= 0.10 {
            ("complete", "replicate_variant_on_fresh_leads")
        } else if variant_rate <= experiment.failure_floor || relative_lift < 0.0 {
            ("complete", "keep_control_document_loss")
        } else {
            ("inconclusive", "drop_or_run_larger_test")
        };
        let result_json = serde_json::json!({
            "control": {"sent": control_sent, "positive": control_positive, "positive_reply_rate": control_rate},
            "variant": {"sent": variant_sent, "positive": variant_positive, "positive_reply_rate": variant_rate},
            "relative_lift": relative_lift,
            "sample_requirement_met": sample_met,
            "single_variable_isolated": isolated,
            "measured_after_full_window": true,
        })
        .to_string();
        conn.execute(
            "UPDATE gtm_experiments SET status=?2,result_json=?3,confidence=?4,decision=?5,
             updated_at=?6 WHERE id=?1",
            params![id, status, result_json, confidence, decision, now()],
        )?;
        experiment.status = status.into();
        experiment.result_json = result_json;
        experiment.confidence = confidence.into();
        experiment.decision = decision.into();
        experiment.updated_at = now();
        Ok(experiment)
    }

    pub fn ensure_experiment_assignment(
        &self,
        experiment_id: &str,
        lead_id: &str,
        person_id: &str,
        sequence_id: &str,
    ) -> Result<ExperimentAssignment> {
        let conn = self.conn.lock().unwrap();
        if let Some(existing) = conn
            .query_row(
                "SELECT * FROM experiment_assignments WHERE experiment_id=?1 AND person_id=?2",
                params![experiment_id, person_id],
                |row| Ok(row_to_experiment_assignment(row)),
            )
            .optional()?
        {
            return Ok(existing);
        }
        let arm = if stable_hash(&format!("{experiment_id}:{person_id}")).is_multiple_of(2) {
            "control"
        } else {
            "variant"
        };
        let assignment = ExperimentAssignment {
            id: Uuid::new_v4().to_string(),
            experiment_id: experiment_id.to_string(),
            lead_id: lead_id.to_string(),
            person_id: person_id.to_string(),
            sequence_id: sequence_id.to_string(),
            arm: arm.into(),
            assigned_at: now(),
        };
        conn.execute(
            "INSERT INTO experiment_assignments
             (id,experiment_id,lead_id,person_id,sequence_id,arm,assigned_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                assignment.id,
                assignment.experiment_id,
                assignment.lead_id,
                assignment.person_id,
                assignment.sequence_id,
                assignment.arm,
                assignment.assigned_at
            ],
        )?;
        Ok(assignment)
    }

    pub fn record_gtm_outcome(&self, outcome: &GtmOutcome) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let id = if outcome.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            outcome.id.clone()
        };
        let occurred_at = if outcome.occurred_at.is_empty() {
            now()
        } else {
            outcome.occurred_at.clone()
        };
        let fingerprint = if outcome.fingerprint.is_empty() {
            format!(
                "{:016x}",
                stable_hash(&format!(
                    "{}:{}:{}:{}:{}:{}",
                    outcome.brand,
                    outcome.kind,
                    outcome.sequence_id,
                    outcome.conversation_id,
                    outcome.source,
                    occurred_at
                ))
            )
        } else {
            outcome.fingerprint.clone()
        };
        conn.execute(
            "INSERT INTO gtm_outcomes
             (id,brand,kind,lead_id,person_id,sequence_id,conversation_id,play_id,experiment_id,
              experiment_assignment_id,signal_observation_ids,touch_id,touch_stage,contact_title,
              contact_vantage,account_hypothesis,play_version,experiment_arm,copy_policy_version,
              generation_backend,generation_model,value,detail,source,fingerprint,occurred_at,created_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,
                     ?19,?20,?21,?22,?23,?24,?25,?26,?27)
             ON CONFLICT(brand,fingerprint) DO NOTHING",
            params![
                id,
                outcome.brand,
                outcome.kind,
                outcome.lead_id,
                outcome.person_id,
                outcome.sequence_id,
                outcome.conversation_id,
                outcome.play_id,
                outcome.experiment_id,
                outcome.experiment_assignment_id,
                js(&outcome.signal_observation_ids),
                outcome.touch_id,
                outcome.touch_stage,
                outcome.contact_title,
                outcome.contact_vantage,
                outcome.account_hypothesis,
                outcome.play_version,
                outcome.experiment_arm,
                outcome.copy_policy_version,
                outcome.generation_backend,
                outcome.generation_model,
                outcome.value,
                outcome.detail,
                outcome.source,
                fingerprint,
                occurred_at,
                now(),
            ],
        )?;
        Ok(id)
    }

    pub fn list_gtm_outcomes(&self, brand: Option<&str>, limit: usize) -> Result<Vec<GtmOutcome>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM gtm_outcomes WHERE (?1 IS NULL OR brand=?1)
             ORDER BY occurred_at DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![brand, limit.max(1) as i64], |row| {
            Ok(row_to_gtm_outcome(row))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn upsert_proof_brief(&self, proof: &ProofBrief) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> =
            if proof.conversation_id.is_empty() || proof.play_id.is_empty() {
                None
            } else {
                conn.query_row(
                    "SELECT id FROM proof_briefs WHERE conversation_id=?1 AND play_id=?2",
                    params![proof.conversation_id, proof.play_id],
                    |row| row.get(0),
                )
                .optional()?
            };
        let id = existing.unwrap_or_else(|| {
            if proof.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                proof.id.clone()
            }
        });
        let timestamp = now();
        conn.execute(
            "INSERT INTO proof_briefs
             (id,brand,lead_id,person_id,conversation_id,play_id,status,problem,current_workflow,
              evidence_available,scope,customer_data,success_metric,baseline,target,stop_condition,
              stakeholders,owner,expansion_path,result,learnings,approved_at,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?23)
             ON CONFLICT(conversation_id,play_id) DO UPDATE SET
              status=excluded.status,problem=excluded.problem,current_workflow=excluded.current_workflow,
              evidence_available=excluded.evidence_available,scope=excluded.scope,
              customer_data=excluded.customer_data,success_metric=excluded.success_metric,
              baseline=excluded.baseline,target=excluded.target,stop_condition=excluded.stop_condition,
              stakeholders=excluded.stakeholders,owner=excluded.owner,expansion_path=excluded.expansion_path,
              result=excluded.result,learnings=excluded.learnings,updated_at=excluded.updated_at",
            params![
                id,
                proof.brand,
                proof.lead_id,
                proof.person_id,
                proof.conversation_id,
                proof.play_id,
                status_or(&proof.status, "draft"),
                proof.problem,
                proof.current_workflow,
                js(&proof.evidence_available),
                proof.scope,
                js(&proof.customer_data),
                proof.success_metric,
                proof.baseline,
                proof.target,
                proof.stop_condition,
                js(&proof.stakeholders),
                proof.owner,
                proof.expansion_path,
                proof.result,
                js(&proof.learnings),
                proof.approved_at,
                timestamp,
            ],
        )?;
        Ok(id)
    }

    pub fn list_proof_briefs(&self, brand: Option<&str>) -> Result<Vec<ProofBrief>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM proof_briefs WHERE (?1 IS NULL OR brand=?1)
             ORDER BY updated_at DESC",
        )?;
        let rows = stmt.query_map(params![brand], |row| Ok(row_to_proof_brief(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn set_proof_status(&self, id: &str, status: &str) -> Result<()> {
        if !matches!(
            status,
            "draft" | "ready" | "approved" | "running" | "passed" | "failed" | "withdrawn"
        ) {
            anyhow::bail!("invalid proof status '{status}'");
        }
        let conn = self.conn.lock().unwrap();
        let proof = conn.query_row(
            "SELECT * FROM proof_briefs WHERE id=?1",
            params![id],
            |row| Ok(row_to_proof_brief(row)),
        )?;
        let allowed = matches!(
            (proof.status.as_str(), status),
            ("draft", "ready")
                | ("draft", "withdrawn")
                | ("ready", "draft")
                | ("ready", "approved")
                | ("ready", "withdrawn")
                | ("approved", "running")
                | ("approved", "withdrawn")
                | ("running", "passed")
                | ("running", "failed")
        ) || proof.status == status;
        if !allowed {
            anyhow::bail!(
                "invalid proof transition {} → {}; approve before running and measure before passing",
                proof.status,
                status
            );
        }
        conn.execute(
            "UPDATE proof_briefs SET status=?2,
             approved_at=CASE WHEN ?2='approved' AND approved_at='' THEN ?3 ELSE approved_at END,
             updated_at=?3 WHERE id=?1",
            params![id, status, now()],
        )?;
        if matches!(status, "passed" | "failed") {
            let kind = if status == "passed" {
                "proof_passed"
            } else {
                "proof_failed"
            };
            conn.execute(
                "INSERT INTO gtm_outcomes
                 (id,brand,kind,lead_id,person_id,sequence_id,conversation_id,play_id,
                  experiment_id,experiment_assignment_id,signal_observation_ids,value,detail,
                  source,fingerprint,occurred_at,created_at)
                 VALUES (?1,?2,?3,?4,?5,'',?6,?7,'','','[]',?8,?9,'proof',?10,?11,?11)
                 ON CONFLICT(brand,fingerprint) DO NOTHING",
                params![
                    Uuid::new_v4().to_string(),
                    proof.brand,
                    kind,
                    proof.lead_id,
                    proof.person_id,
                    proof.conversation_id,
                    proof.play_id,
                    if status == "passed" { 1.0 } else { 0.0 },
                    if proof.result.is_empty() {
                        format!(
                            "Proof marked {status}; result narrative still needs documentation."
                        )
                    } else {
                        proof.result
                    },
                    format!("proof:{}:{status}", proof.id),
                    now(),
                ],
            )?;
        }
        Ok(())
    }

    pub fn upsert_customer_development(
        &self,
        record: &CustomerDevelopmentRecord,
    ) -> Result<String> {
        if record.brand.trim().is_empty() || record.lead_id.trim().is_empty() {
            anyhow::bail!("customer development requires brand and lead_id");
        }
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM customer_development WHERE brand=?1 AND lead_id=?2",
                params![record.brand, record.lead_id],
                |row| row.get(0),
            )
            .optional()?;
        let id = existing.unwrap_or_else(|| {
            if record.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                record.id.clone()
            }
        });
        let timestamp = now();
        conn.execute(
            "INSERT INTO customer_development
             (id,brand,lead_id,person_id,conversation_id,stage,problem,task_scope,site,
              current_workflow,why_manual,variations,exceptions,evidence,economics,
              success_criteria,stop_condition,stakeholders,commitment_kind,commitment_detail,
              quantity,commercial_case,timeline,loi_conditions,next_action,engaged_at,source,
              created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,
                     ?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?28)
             ON CONFLICT(brand,lead_id) DO UPDATE SET
              person_id=excluded.person_id,conversation_id=excluded.conversation_id,
              stage=excluded.stage,problem=excluded.problem,task_scope=excluded.task_scope,
              site=excluded.site,current_workflow=excluded.current_workflow,
              why_manual=excluded.why_manual,variations=excluded.variations,
              exceptions=excluded.exceptions,evidence=excluded.evidence,
              economics=excluded.economics,success_criteria=excluded.success_criteria,
              stop_condition=excluded.stop_condition,stakeholders=excluded.stakeholders,
              commitment_kind=excluded.commitment_kind,
              commitment_detail=excluded.commitment_detail,quantity=excluded.quantity,
              commercial_case=excluded.commercial_case,timeline=excluded.timeline,
              loi_conditions=excluded.loi_conditions,next_action=excluded.next_action,
              engaged_at=excluded.engaged_at,source=excluded.source,
              updated_at=excluded.updated_at",
            params![
                id,
                record.brand,
                record.lead_id,
                record.person_id,
                record.conversation_id,
                status_or(&record.stage, "hypothesis"),
                record.problem,
                record.task_scope,
                record.site,
                record.current_workflow,
                record.why_manual,
                js(&record.variations),
                js(&record.exceptions),
                js(&record.evidence),
                record.economics,
                record.success_criteria,
                record.stop_condition,
                js(&record.stakeholders),
                status_or(&record.commitment_kind, "none"),
                record.commitment_detail,
                record.quantity,
                record.commercial_case,
                record.timeline,
                record.loi_conditions,
                record.next_action,
                record.engaged_at,
                status_or(&record.source, "manual_crm"),
                timestamp,
            ],
        )?;
        Ok(id)
    }

    pub fn customer_development_for_lead(
        &self,
        brand: &str,
        lead_id: &str,
    ) -> Result<Option<CustomerDevelopmentRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT * FROM customer_development WHERE brand=?1 AND lead_id=?2",
            params![brand, lead_id],
            |row| Ok(row_to_customer_development(row)),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_customer_development(
        &self,
        brand: Option<&str>,
    ) -> Result<Vec<CustomerDevelopmentRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM customer_development WHERE (?1 IS NULL OR brand=?1)
             ORDER BY updated_at DESC",
        )?;
        let rows = stmt.query_map(params![brand], |row| Ok(row_to_customer_development(row)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn backfill_sequence_gtm_attribution(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "UPDATE sequences SET
               play_id=COALESCE((SELECT p.id FROM gtm_plays p
                 WHERE p.brand=sequences.brand AND p.lifecycle IN ('testing','proven')
                 ORDER BY CASE p.lifecycle WHEN 'proven' THEN 0 ELSE 1 END,p.version DESC LIMIT 1),''),
               play_version=COALESCE((SELECT p.version FROM gtm_plays p
                 WHERE p.brand=sequences.brand AND p.lifecycle IN ('testing','proven')
                 ORDER BY CASE p.lifecycle WHEN 'proven' THEN 0 ELSE 1 END,p.version DESC LIMIT 1),0),
               signal_observation_ids=COALESCE((SELECT json_group_array(o.id)
                 FROM signal_observations o WHERE o.lead_id=sequences.lead_id
                   AND o.status IN ('observed','verified') AND (o.expires_at='' OR o.expires_at>?1)),'[]'),
               gtm_state=CASE WHEN EXISTS(SELECT 1 FROM signal_observations o
                 WHERE o.lead_id=sequences.lead_id AND o.status IN ('observed','verified')
                   AND (o.expires_at='' OR o.expires_at>?1)) THEN 'action_ready' ELSE 'research_required' END
             WHERE play_id=''",
            params![now()],
        )?)
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

fn row_to_market_account(r: &Row) -> MarketAccount {
    MarketAccount {
        id: g(r, "id"),
        identity_key: g(r, "identity_key"),
        canonical_domain: g(r, "canonical_domain"),
        apollo_org_id: g(r, "apollo_org_id"),
        name: g(r, "name"),
        industry: g(r, "industry"),
        hq: g(r, "hq"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_market_segment(r: &Row) -> MarketSegment {
    MarketSegment {
        id: g(r, "id"),
        brand: g(r, "brand"),
        key: g(r, "key"),
        version: r.get("version").unwrap_or(1),
        name: g(r, "name"),
        geography: g(r, "geography"),
        wedge: g(r, "wedge"),
        unit_of_analysis: g(r, "unit_of_analysis"),
        enumeration_sources: jd(&g(r, "enumeration_sources")),
        status: g(r, "status"),
        estimated_total: r.get("estimated_total").unwrap_or(0),
        accounts_discovered: r.get("accounts_discovered").unwrap_or(0),
        accounts_with_opportunities: r.get("accounts_with_opportunities").unwrap_or(0),
        source_exhausted: r.get::<_, i64>("source_exhausted").unwrap_or(0) != 0,
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_coverage_run(r: &Row) -> CoverageRun {
    CoverageRun {
        id: g(r, "id"),
        segment_id: g(r, "segment_id"),
        brand: g(r, "brand"),
        source_name: g(r, "source_name"),
        query_fingerprint: g(r, "query_fingerprint"),
        cursor: g(r, "cursor"),
        pages_examined: r.get("pages_examined").unwrap_or(0),
        candidates_seen: r.get("candidates_seen").unwrap_or(0),
        accounts_added: r.get("accounts_added").unwrap_or(0),
        status: g(r, "status"),
        exhausted: r.get::<_, i64>("exhausted").unwrap_or(0) != 0,
        gap_reason: g(r, "gap_reason"),
        started_at: g(r, "started_at"),
        completed_at: g(r, "completed_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_facility(r: &Row) -> Facility {
    Facility {
        id: g(r, "id"),
        market_account_id: g(r, "market_account_id"),
        name: g(r, "name"),
        facility_type: g(r, "facility_type"),
        address: g(r, "address"),
        city: g(r, "city"),
        region: g(r, "region"),
        country: g(r, "country"),
        source_url: g(r, "source_url"),
        source_excerpt: g(r, "source_excerpt"),
        confidence: r.get("confidence").unwrap_or(0.0),
        status: g(r, "status"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_sales_opportunity(r: &Row) -> SalesOpportunity {
    SalesOpportunity {
        id: g(r, "id"),
        brand: g(r, "brand"),
        market_account_id: g(r, "market_account_id"),
        lead_id: g(r, "lead_id"),
        segment_id: g(r, "segment_id"),
        facility_id: g(r, "facility_id"),
        play_id: g(r, "play_id"),
        kind: g(r, "kind"),
        title: g(r, "title"),
        task_or_decision: g(r, "task_or_decision"),
        mechanism: g(r, "mechanism"),
        consequence: g(r, "consequence"),
        system_concept: g(r, "system_concept"),
        proof_offer: g(r, "proof_offer"),
        evidence_status: g(r, "evidence_status"),
        priority_tier: g(r, "priority_tier"),
        fit_score: r.get("fit_score").unwrap_or(0),
        status: g(r, "status"),
        evidence_gaps: jd(&g(r, "evidence_gaps")),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_evidence_claim(r: &Row) -> EvidenceClaim {
    EvidenceClaim {
        id: g(r, "id"),
        sales_opportunity_id: g(r, "sales_opportunity_id"),
        brand: g(r, "brand"),
        lead_id: g(r, "lead_id"),
        facility_id: g(r, "facility_id"),
        claim_type: g(r, "claim_type"),
        claim_text: g(r, "claim_text"),
        source_url: g(r, "source_url"),
        source_title: g(r, "source_title"),
        source_excerpt: g(r, "source_excerpt"),
        source_locator: g(r, "source_locator"),
        source_domain: g(r, "source_domain"),
        lineage_key: g(r, "lineage_key"),
        independence_group: g(r, "independence_group"),
        confidence: r.get("confidence").unwrap_or(0.0),
        status: g(r, "status"),
        observed_at: g(r, "observed_at"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_opportunity_stakeholder(r: &Row) -> OpportunityStakeholder {
    OpportunityStakeholder {
        id: g(r, "id"),
        sales_opportunity_id: g(r, "sales_opportunity_id"),
        person_id: g(r, "person_id"),
        role: g(r, "role"),
        relationship_to_task: g(r, "relationship_to_task"),
        can_observe: g(r, "can_observe"),
        can_decide: g(r, "can_decide"),
        priority: r.get("priority").unwrap_or(100),
        active_thread: r.get::<_, i64>("active_thread").unwrap_or(0) != 0,
        status: g(r, "status"),
        source: g(r, "source"),
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
        linkedin_status: g(r, "linkedin_status"),
        email: g(r, "email"),
        email_status: g(r, "email_status"),
        phone: g(r, "phone"),
        status: g(r, "status"),
        enriched_at: g(r, "enriched_at"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_sequence(r: &Row) -> Sequence {
    Sequence {
        id: g(r, "id"),
        person_id: g(r, "person_id"),
        lead_id: g(r, "lead_id"),
        brand: g(r, "brand"),
        thesis: g(r, "thesis"),
        applied_principles: jd(&g(r, "applied_principles")),
        play_id: g(r, "play_id"),
        play_version: r.get("play_version").unwrap_or(0),
        experiment_id: g(r, "experiment_id"),
        experiment_arm: g(r, "experiment_arm"),
        experiment_assignment_id: g(r, "experiment_assignment_id"),
        signal_observation_ids: jd(&g(r, "signal_observation_ids")),
        gtm_state: g(r, "gtm_state"),
        copy_policy_version: r.get("copy_policy_version").unwrap_or(0),
        generation_backend: g(r, "generation_backend"),
        generation_model: g(r, "generation_model"),
        sales_opportunity_id: g(r, "sales_opportunity_id"),
        status: g(r, "status"),
        current_stage: r.get("current_stage").unwrap_or(0),
        created_at: g(r, "created_at"),
    }
}

fn row_to_signal_definition(r: &Row) -> SignalDefinition {
    SignalDefinition {
        id: g(r, "id"),
        brand: g(r, "brand"),
        key: g(r, "key"),
        name: g(r, "name"),
        description: g(r, "description"),
        topic: g(r, "topic"),
        entity_type: g(r, "entity_type"),
        value_type: g(r, "value_type"),
        source_kind: g(r, "source_kind"),
        owner: g(r, "owner"),
        refresh_cadence: g(r, "refresh_cadence"),
        freshness_seconds: r.get("freshness_seconds").unwrap_or(0),
        evidence_required: r.get::<_, i64>("evidence_required").unwrap_or(1) != 0,
        minimum_confidence: r.get("minimum_confidence").unwrap_or(0.0),
        version: r.get("version").unwrap_or(1),
        status: g(r, "status"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_signal_observation(r: &Row) -> SignalObservation {
    SignalObservation {
        id: g(r, "id"),
        definition_id: g(r, "definition_id"),
        definition_key: g(r, "definition_key"),
        brand: g(r, "brand"),
        lead_id: g(r, "lead_id"),
        person_id: g(r, "person_id"),
        conversation_id: g(r, "conversation_id"),
        source_name: g(r, "source_name"),
        source_url: g(r, "source_url"),
        provider_key: g(r, "provider_key"),
        value_json: g(r, "value_json"),
        evidence: g(r, "evidence"),
        confidence: r.get("confidence").unwrap_or(0.0),
        observed_at: g(r, "observed_at"),
        expires_at: g(r, "expires_at"),
        status: g(r, "status"),
        fingerprint: g(r, "fingerprint"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_gtm_play(r: &Row) -> GtmPlay {
    GtmPlay {
        id: g(r, "id"),
        brand: g(r, "brand"),
        key: g(r, "key"),
        version: r.get("version").unwrap_or(1),
        name: g(r, "name"),
        lifecycle: g(r, "lifecycle"),
        motion: g(r, "motion"),
        target_icp: g(r, "target_icp"),
        target_vantages: jd(&g(r, "target_vantages")),
        required_signal_keys: jd(&g(r, "required_signal_keys")),
        minimum_signal_matches: r.get("minimum_signal_matches").unwrap_or(1),
        hypothesis: g(r, "hypothesis"),
        action_policy: g(r, "action_policy"),
        proof_type: g(r, "proof_type"),
        proof_description: g(r, "proof_description"),
        success_metric: g(r, "success_metric"),
        kill_condition: g(r, "kill_condition"),
        source_refs: jd(&g(r, "source_refs")),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_account_play_assessment(r: &Row) -> AccountPlayAssessment {
    AccountPlayAssessment {
        id: g(r, "id"),
        lead_id: g(r, "lead_id"),
        brand: g(r, "brand"),
        play_id: g(r, "play_id"),
        play_version: r.get("play_version").unwrap_or(0),
        status: g(r, "status"),
        fit_score: r.get("fit_score").unwrap_or(0),
        matched_signal_keys: jd(&g(r, "matched_signal_keys")),
        symptom: g(r, "symptom"),
        root_cause: g(r, "root_cause"),
        current_workaround: g(r, "current_workaround"),
        why_now: g(r, "why_now"),
        proof_fit: g(r, "proof_fit"),
        evidence_gaps: jd(&g(r, "evidence_gaps")),
        disqualifiers: jd(&g(r, "disqualifiers")),
        source: g(r, "source"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_gtm_experiment(r: &Row) -> GtmExperiment {
    GtmExperiment {
        id: g(r, "id"),
        brand: g(r, "brand"),
        play_id: g(r, "play_id"),
        name: g(r, "name"),
        experiment_type: g(r, "experiment_type"),
        hypothesis: g(r, "hypothesis"),
        variable: g(r, "variable"),
        constants: jd(&g(r, "constants")),
        control_description: g(r, "control_description"),
        variant_description: g(r, "variant_description"),
        minimum_sends_per_arm: r.get("minimum_sends_per_arm").unwrap_or(500),
        baseline_sends: r.get("baseline_sends").unwrap_or(0),
        baseline_positive_reply_rate: r.get("baseline_positive_reply_rate").unwrap_or(0.0),
        success_target: r.get("success_target").unwrap_or(0.0),
        failure_floor: r.get("failure_floor").unwrap_or(0.0),
        measurement_days: r.get("measurement_days").unwrap_or(21),
        status: g(r, "status"),
        starts_at: g(r, "starts_at"),
        ends_at: g(r, "ends_at"),
        result_json: g(r, "result_json"),
        confidence: g(r, "confidence"),
        decision: g(r, "decision"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_experiment_assignment(r: &Row) -> ExperimentAssignment {
    ExperimentAssignment {
        id: g(r, "id"),
        experiment_id: g(r, "experiment_id"),
        lead_id: g(r, "lead_id"),
        person_id: g(r, "person_id"),
        sequence_id: g(r, "sequence_id"),
        arm: g(r, "arm"),
        assigned_at: g(r, "assigned_at"),
    }
}

fn row_to_gtm_outcome(r: &Row) -> GtmOutcome {
    GtmOutcome {
        id: g(r, "id"),
        brand: g(r, "brand"),
        kind: g(r, "kind"),
        lead_id: g(r, "lead_id"),
        person_id: g(r, "person_id"),
        sequence_id: g(r, "sequence_id"),
        conversation_id: g(r, "conversation_id"),
        play_id: g(r, "play_id"),
        experiment_id: g(r, "experiment_id"),
        experiment_assignment_id: g(r, "experiment_assignment_id"),
        signal_observation_ids: jd(&g(r, "signal_observation_ids")),
        touch_id: g(r, "touch_id"),
        touch_stage: r.get("touch_stage").unwrap_or(0),
        contact_title: g(r, "contact_title"),
        contact_vantage: g(r, "contact_vantage"),
        account_hypothesis: g(r, "account_hypothesis"),
        play_version: r.get("play_version").unwrap_or(0),
        experiment_arm: g(r, "experiment_arm"),
        copy_policy_version: r.get("copy_policy_version").unwrap_or(0),
        generation_backend: g(r, "generation_backend"),
        generation_model: g(r, "generation_model"),
        value: r.get("value").unwrap_or(0.0),
        detail: g(r, "detail"),
        source: g(r, "source"),
        fingerprint: g(r, "fingerprint"),
        occurred_at: g(r, "occurred_at"),
        created_at: g(r, "created_at"),
    }
}

fn row_to_proof_brief(r: &Row) -> ProofBrief {
    ProofBrief {
        id: g(r, "id"),
        brand: g(r, "brand"),
        lead_id: g(r, "lead_id"),
        person_id: g(r, "person_id"),
        conversation_id: g(r, "conversation_id"),
        play_id: g(r, "play_id"),
        status: g(r, "status"),
        problem: g(r, "problem"),
        current_workflow: g(r, "current_workflow"),
        evidence_available: jd(&g(r, "evidence_available")),
        scope: g(r, "scope"),
        customer_data: jd(&g(r, "customer_data")),
        success_metric: g(r, "success_metric"),
        baseline: g(r, "baseline"),
        target: g(r, "target"),
        stop_condition: g(r, "stop_condition"),
        stakeholders: jd(&g(r, "stakeholders")),
        owner: g(r, "owner"),
        expansion_path: g(r, "expansion_path"),
        result: g(r, "result"),
        learnings: jd(&g(r, "learnings")),
        approved_at: g(r, "approved_at"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_customer_development(r: &Row) -> CustomerDevelopmentRecord {
    CustomerDevelopmentRecord {
        id: g(r, "id"),
        brand: g(r, "brand"),
        lead_id: g(r, "lead_id"),
        person_id: g(r, "person_id"),
        conversation_id: g(r, "conversation_id"),
        stage: g(r, "stage"),
        problem: g(r, "problem"),
        task_scope: g(r, "task_scope"),
        site: g(r, "site"),
        current_workflow: g(r, "current_workflow"),
        why_manual: g(r, "why_manual"),
        variations: jd(&g(r, "variations")),
        exceptions: jd(&g(r, "exceptions")),
        evidence: jd(&g(r, "evidence")),
        economics: g(r, "economics"),
        success_criteria: g(r, "success_criteria"),
        stop_condition: g(r, "stop_condition"),
        stakeholders: jd(&g(r, "stakeholders")),
        commitment_kind: g(r, "commitment_kind"),
        commitment_detail: g(r, "commitment_detail"),
        quantity: g(r, "quantity"),
        commercial_case: g(r, "commercial_case"),
        timeline: g(r, "timeline"),
        loi_conditions: g(r, "loi_conditions"),
        next_action: g(r, "next_action"),
        engaged_at: g(r, "engaged_at"),
        source: g(r, "source"),
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

fn row_to_conversation(r: &Row) -> Conversation {
    Conversation {
        id: g(r, "id"),
        brand: g(r, "brand"),
        sequence_id: g(r, "sequence_id"),
        person_id: g(r, "person_id"),
        lead_id: g(r, "lead_id"),
        subject: g(r, "subject"),
        status: g(r, "status"),
        last_message_at: g(r, "last_message_at"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn row_to_conversation_message(r: &Row) -> ConversationMessage {
    ConversationMessage {
        id: g(r, "id"),
        conversation_id: g(r, "conversation_id"),
        direction: g(r, "direction"),
        sender_email: g(r, "sender_email"),
        recipient_email: g(r, "recipient_email"),
        participants: jd(&g(r, "participants")),
        subject: g(r, "subject"),
        body: g(r, "body"),
        status: g(r, "status"),
        message_id: g(r, "message_id"),
        in_reply_to: g(r, "in_reply_to"),
        references: jd(&g(r, "references_json")),
        classification: g(r, "classification"),
        action: g(r, "action"),
        offered_slots: jd(&g(r, "offered_slots")),
        mailbox_id: g(r, "mailbox_id"),
        sent_at: g(r, "sent_at"),
        created_at: g(r, "created_at"),
    }
}

fn row_to_meeting(r: &Row) -> Meeting {
    Meeting {
        id: g(r, "id"),
        conversation_id: g(r, "conversation_id"),
        brand: g(r, "brand"),
        person_id: g(r, "person_id"),
        attendee_email: g(r, "attendee_email"),
        starts_at: g(r, "starts_at"),
        ends_at: g(r, "ends_at"),
        timezone: g(r, "timezone"),
        status: g(r, "status"),
        google_event_id: g(r, "google_event_id"),
        html_link: g(r, "html_link"),
        meet_link: g(r, "meet_link"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

fn ensure_conversation(
    conn: &Connection,
    brand: &str,
    sequence_id: &str,
    person_id: &str,
    lead_id: &str,
    subject: &str,
) -> Result<Conversation> {
    let existing = if sequence_id.is_empty() {
        conn.query_row(
            "SELECT * FROM conversations WHERE brand=?1 AND person_id=?2
             ORDER BY updated_at DESC LIMIT 1",
            params![brand, person_id],
            |row| Ok(row_to_conversation(row)),
        )
        .optional()?
    } else {
        conn.query_row(
            "SELECT * FROM conversations WHERE sequence_id=?1 LIMIT 1",
            params![sequence_id],
            |row| Ok(row_to_conversation(row)),
        )
        .optional()?
    };
    if let Some(conversation) = existing {
        return Ok(conversation);
    }

    let id = Uuid::new_v4().to_string();
    let timestamp = now();
    conn.execute(
        "INSERT INTO conversations
         (id,brand,sequence_id,person_id,lead_id,subject,status,last_message_at,created_at,updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,'open',?7,?7,?7)",
        params![
            id,
            brand,
            sequence_id,
            person_id,
            lead_id,
            subject,
            timestamp
        ],
    )?;
    Ok(Conversation {
        id,
        brand: brand.to_string(),
        sequence_id: sequence_id.to_string(),
        person_id: person_id.to_string(),
        lead_id: lead_id.to_string(),
        subject: subject.to_string(),
        status: "open".into(),
        last_message_at: timestamp.clone(),
        created_at: timestamp.clone(),
        updated_at: timestamp,
    })
}

// --- helpers ---------------------------------------------------------------

/// A durable unit of background work. The cadence loop already proved the
/// pattern — claim due rows from SQLite, act, persist state — for one hard-coded
/// kind (send a touch). `Job` generalizes it: any worker can enqueue, lease, and
/// retry work that survives a restart, terminating in a dead-letter state after
/// `max_attempts` rather than silently stranding a prospect mid-pipeline. This
/// is the spine an autonomous supervisor schedules its decisions onto.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Job {
    pub id: String,
    pub brand: String,
    pub kind: String,
    pub payload: String,
    pub status: String,
    pub priority: i64,
    pub next_run_at: String,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub lease_owner: String,
    pub lease_expires_at: String,
    pub last_error: String,
    pub dedup_key: String,
    pub result: String,
    pub created_at: String,
    pub updated_at: String,
}

fn row_to_job(r: &Row) -> Job {
    Job {
        id: g(r, "id"),
        brand: g(r, "brand"),
        kind: g(r, "kind"),
        payload: g(r, "payload"),
        status: g(r, "status"),
        priority: r.get("priority").unwrap_or(0),
        next_run_at: g(r, "next_run_at"),
        attempt_count: r.get("attempt_count").unwrap_or(0),
        max_attempts: r.get("max_attempts").unwrap_or(0),
        lease_owner: g(r, "lease_owner"),
        lease_expires_at: g(r, "lease_expires_at"),
        last_error: g(r, "last_error"),
        dedup_key: g(r, "dedup_key"),
        result: g(r, "result"),
        created_at: g(r, "created_at"),
        updated_at: g(r, "updated_at"),
    }
}

/// Column getter that tolerates NULL/missing by returning an empty string.
fn g(r: &Row, col: &str) -> String {
    r.get::<_, Option<String>>(col)
        .unwrap_or(None)
        .unwrap_or_default()
}

fn js<T: Serialize + ?Sized>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "[]".into())
}

fn jd(s: &str) -> Vec<String> {
    if s.trim().is_empty() {
        return Vec::new();
    }
    serde_json::from_str(s).unwrap_or_default()
}

fn stable_hash(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn record_signal_observation_conn(
    conn: &Connection,
    observation: &SignalObservation,
) -> Result<String> {
    let definition = conn
        .query_row(
            "SELECT * FROM signal_definitions
             WHERE brand=?1 AND key=?2 AND status='active'
             ORDER BY version DESC LIMIT 1",
            params![observation.brand, observation.definition_key],
            |row| Ok(row_to_signal_definition(row)),
        )
        .optional()?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "unknown active signal definition {}:{}",
                observation.brand,
                observation.definition_key
            )
        })?;
    if definition.evidence_required && observation.evidence.trim().is_empty() {
        anyhow::bail!("signal {} requires source-backed evidence", definition.key);
    }
    let has_entity = match definition.entity_type.as_str() {
        "account" => !observation.lead_id.trim().is_empty(),
        "person" => !observation.person_id.trim().is_empty(),
        "conversation" => !observation.conversation_id.trim().is_empty(),
        _ => true,
    };
    if !has_entity {
        anyhow::bail!(
            "signal {} requires a {} entity id",
            definition.key,
            definition.entity_type
        );
    }
    let observed_at = DateTime::parse_from_rfc3339(&observation.observed_at)
        .map(|at| at.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let expires_at = if !observation.expires_at.trim().is_empty() {
        observation.expires_at.clone()
    } else if definition.freshness_seconds > 0 {
        (observed_at + Duration::seconds(definition.freshness_seconds)).to_rfc3339()
    } else {
        String::new()
    };
    let evidence = observation.evidence.trim();
    let fingerprint = if observation.fingerprint.trim().is_empty() {
        format!(
            "{:016x}",
            stable_hash(&format!(
                "{}:{}:{}:{}:{}:{}:{}",
                observation.brand,
                definition.key,
                observation.lead_id,
                observation.person_id,
                observation.conversation_id,
                observation.source_url.trim().to_lowercase(),
                evidence.to_lowercase()
            ))
        )
    } else {
        observation.fingerprint.clone()
    };
    let id = if observation.id.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        observation.id.clone()
    };
    let timestamp = now();
    let status = match observation.status.as_str() {
        "verified" | "rejected" | "expired" | "observed" => observation.status.as_str(),
        _ => "observed",
    };
    conn.execute(
        "INSERT INTO signal_observations
         (id,definition_id,definition_key,brand,lead_id,person_id,conversation_id,source_name,
          source_url,provider_key,value_json,evidence,confidence,observed_at,expires_at,status,
          fingerprint,created_at,updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?18)
         ON CONFLICT(brand,fingerprint) DO UPDATE SET
          definition_id=excluded.definition_id,definition_key=excluded.definition_key,
          source_name=excluded.source_name,source_url=excluded.source_url,
          provider_key=excluded.provider_key,value_json=excluded.value_json,
          evidence=excluded.evidence,confidence=excluded.confidence,observed_at=excluded.observed_at,
          expires_at=excluded.expires_at,status=excluded.status,updated_at=excluded.updated_at",
        params![
            id,
            definition.id,
            definition.key,
            observation.brand,
            observation.lead_id,
            observation.person_id,
            observation.conversation_id,
            status_or(&observation.source_name, &definition.source_kind),
            observation.source_url,
            observation.provider_key,
            observation.value_json,
            evidence,
            observation.confidence.clamp(0.0, 1.0),
            observed_at.to_rfc3339(),
            expires_at,
            status,
            fingerprint,
            timestamp,
        ],
    )?;
    Ok(conn.query_row(
        "SELECT id FROM signal_observations WHERE brand=?1 AND fingerprint=?2",
        params![observation.brand, fingerprint],
        |row| row.get(0),
    )?)
}

fn status_or(s: &str, default: &str) -> String {
    if s.trim().is_empty() {
        default.to_string()
    } else {
        s.to_string()
    }
}

fn canonical_domain(raw: &str) -> String {
    raw.trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("www.")
        .trim_end_matches('/')
        .split('/')
        .next()
        .unwrap_or("")
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn market_identity_key(domain: &str, apollo_org_id: &str, name: &str, hq: &str) -> String {
    if !domain.trim().is_empty() {
        format!("domain:{}", domain.trim().to_ascii_lowercase())
    } else if !apollo_org_id.trim().is_empty() {
        format!("apollo:{}", apollo_org_id.trim().to_ascii_lowercase())
    } else {
        format!(
            "name:{:016x}",
            stable_hash(&format!(
                "{}|{}",
                name.trim().to_ascii_lowercase(),
                hq.trim().to_ascii_lowercase()
            ))
        )
    }
}

fn opportunity_title(task_or_decision: &str) -> String {
    let compact = task_or_decision
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if compact.chars().count() <= 120 {
        compact
    } else {
        let mut title = compact.chars().take(117).collect::<String>();
        title.push_str("...");
        title
    }
}

fn evidence_names_operating_site(evidence: &str) -> bool {
    let text = evidence.to_ascii_lowercase();
    ONTARIO_SITE_NAMES
        .iter()
        .any(|place| text.contains(&place.to_ascii_lowercase()))
        || [
            " at the facility",
            " at its facility",
            " at their facility",
            " facility in ",
            " plant in ",
            " warehouse in ",
            " distribution centre in ",
            " distribution center in ",
            " operating site in ",
        ]
        .iter()
        .any(|term| text.contains(term))
}

/// A Wapahki task belongs to a facility only when the task claim itself names
/// the site, or another atomic claim from the exact same source page names it.
/// A company HQ or locations page cannot silently donate a facility to an
/// unrelated task page.
fn facility_observation_for_task(observations: &[SignalObservation]) -> Option<&SignalObservation> {
    let task = observations
        .iter()
        .find(|observation| observation.definition_key == "account.bounded_repetitive_task")?;
    if evidence_names_operating_site(&task.evidence) {
        return Some(task);
    }
    let task_url = task.source_url.trim().trim_end_matches('/');
    if task_url.is_empty() {
        return None;
    }
    observations.iter().find(|observation| {
        observation.source_url.trim().trim_end_matches('/') == task_url
            && evidence_names_operating_site(&observation.evidence)
    })
}

fn facility_type_from_evidence(evidence: &str) -> &'static str {
    let text = evidence.to_ascii_lowercase();
    if text.contains("warehouse")
        || text.contains("distribution centre")
        || text.contains("distribution center")
        || text.contains("distribution-centre")
        || text.contains("distribution-center")
    {
        "warehouse_or_distribution_centre"
    } else if text.contains("cold storage") || text.contains("freezer") {
        "cold_storage"
    } else {
        "factory_or_production_site"
    }
}

fn facility_label(lead: &Lead, evidence: &str) -> String {
    let lower = evidence.to_ascii_lowercase();
    if let Some(city) = ONTARIO_SITE_NAMES
        .iter()
        .find(|city| lower.contains(&city.to_ascii_lowercase()))
    {
        format!("{} operating site", city)
    } else if !lead.hq.trim().is_empty() && lower.contains(&lead.hq.trim().to_ascii_lowercase()) {
        format!("{} operating site", lead.hq.trim())
    } else {
        format!("{} site documented in task source", lead.name.trim())
    }
}

const ONTARIO_SITE_NAMES: &[&str] = &[
    "Barrie",
    "Belleville",
    "Brampton",
    "Brantford",
    "Burlington",
    "Caledon",
    "Cambridge",
    "Etobicoke",
    "Guelph",
    "Halton Hills",
    "Hamilton",
    "King City",
    "Kingston",
    "Kitchener",
    "Leamington",
    "London",
    "Markham",
    "Mississauga",
    "Newmarket",
    "Oakville",
    "Ottawa",
    "Pickering",
    "Richmond Hill",
    "Toronto",
    "Vars",
    "Vaughan",
    "Waterloo",
    "Windsor",
];

fn choose_segment_id(brand: &str, context: &str, segments: &[MarketSegment]) -> String {
    let text = context.to_ascii_lowercase();
    let preferred_key = if brand.eq_ignore_ascii_case("wapahki") {
        if [
            "warehouse",
            "distribution",
            "3pl",
            "fulfillment",
            "cold storage",
        ]
        .iter()
        .any(|term| text.contains(term))
        {
            "ontario_warehouse_case_handling"
        } else if ["food", "beverage", "pack", "case", "tray", "pallet"]
            .iter()
            .any(|term| text.contains(term))
        {
            "ontario_food_case_palletizing"
        } else {
            "ontario_manufacturing_machine_tending"
        }
    } else if brand.eq_ignore_ascii_case("gnk") {
        if [
            "construction",
            "contractor",
            "change order",
            "project delay",
        ]
        .iter()
        .any(|term| text.contains(term))
        {
            "canada_construction_delay_evidence"
        } else if ["claim", "billing", "eligibility", "filing", "recovery"]
            .iter()
            .any(|term| text.contains(term))
        {
            "canada_specialty_claims_admin"
        } else {
            "canada_3pl_exception_decisions"
        }
    } else if brand.eq_ignore_ascii_case("outagehub") {
        if ["telecom", "tower", "cell site", "network operations"]
            .iter()
            .any(|term| text.contains(term))
        {
            "canada_telecom_site_continuity"
        } else if ["generator", "backup power", "refuelling", "refueling"]
            .iter()
            .any(|term| text.contains(term))
        {
            "canada_backup_power_dispatch"
        } else {
            "canada_ev_charging_operations"
        }
    } else {
        ""
    };
    segments
        .iter()
        .find(|segment| segment.key == preferred_key)
        .or_else(|| segments.first())
        .map(|segment| segment.id.clone())
        .unwrap_or_default()
}

fn stakeholder_role(title: &str, vantage: &str) -> String {
    let text = format!(
        " {} {} ",
        title.trim().to_ascii_lowercase(),
        vantage.trim().to_ascii_lowercase()
    );
    if text.contains(" procurement ") || text.contains(" purchasing ") {
        "procurement_legal"
    } else if text.contains(" safety ")
        || text.contains(" sanitation ")
        || text.contains(" quality ")
    {
        "constraint_owner"
    } else if text.contains(" maintenance ")
        || text.contains(" controls ")
        || text.contains(" automation ")
        || text.contains(" engineering ")
        || text.contains(" technical ")
        || text.contains(" it ")
    {
        "technical_evaluator"
    } else if text.contains(" ceo ")
        || text.contains(" founder ")
        || text.contains(" president ")
        || text.contains(" owner ")
        || text.contains(" economic_buyer ")
    {
        "economic_buyer"
    } else if text.contains(" supervisor ")
        || text.contains(" lead ")
        || text.contains(" operator ")
        || text.contains(" coordinator ")
    {
        "problem_witness"
    } else if text.contains(" manager ")
        || text.contains(" director ")
        || text.contains(" process_owner ")
        || text.contains(" operational_executive ")
    {
        "process_owner"
    } else {
        "router"
    }
    .into()
}

fn stakeholder_priority(role: &str) -> i64 {
    match role {
        "problem_witness" => 10,
        "process_owner" => 20,
        "constraint_owner" => 30,
        "technical_evaluator" => 40,
        "economic_buyer" => 50,
        "procurement_legal" => 60,
        _ => 90,
    }
}

fn stakeholder_decision_scope(role: &str) -> &'static str {
    match role {
        "problem_witness" => "Can confirm the task, current workflow, variation, and exceptions.",
        "process_owner" => "Can sponsor workflow discovery and operational evaluation.",
        "constraint_owner" => {
            "Can validate safety, quality, sanitation, or compliance constraints."
        }
        "technical_evaluator" => {
            "Can evaluate integration, feasibility, data, controls, or security."
        }
        "economic_buyer" => "Can validate economics, priority, budget, and executive sponsorship.",
        "procurement_legal" => {
            "Can route procurement, contracting, and legal review after sponsorship."
        }
        _ => "Can route the opportunity to the closest operating owner.",
    }
}

fn source_domain(raw: &str) -> String {
    canonical_domain(raw)
        .split(':')
        .next()
        .unwrap_or("")
        .to_string()
}

fn normalize_linkedin_status(status: &str) -> &'static str {
    match status.trim().to_ascii_lowercase().as_str() {
        "requested" => "requested",
        "connected" => "connected",
        "not_connected" | "not-connected" | "not connected" => "not_connected",
        _ => "unknown",
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
    linkedin_status TEXT DEFAULT 'unknown',
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
    applied_principles TEXT DEFAULT '[]',
    copy_policy_version INTEGER DEFAULT 0,
    generation_backend TEXT DEFAULT '',
    generation_model TEXT DEFAULT '',
    sales_opportunity_id TEXT DEFAULT '',
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
    id TEXT PRIMARY KEY, conversation_id TEXT DEFAULT '',
    person_id TEXT, sequence_id TEXT, ts TEXT,
    from_email TEXT, subject TEXT, body TEXT,
    classification TEXT, action_taken TEXT,
    message_id TEXT, in_reply_to TEXT
);
CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    sequence_id TEXT,
    person_id TEXT NOT NULL,
    lead_id TEXT,
    subject TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    last_message_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS conversation_messages (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    sender_email TEXT,
    recipient_email TEXT,
    participants TEXT,
    subject TEXT,
    body TEXT,
    status TEXT NOT NULL,
    message_id TEXT,
    in_reply_to TEXT,
    references_json TEXT,
    classification TEXT,
    action TEXT,
    offered_slots TEXT,
    mailbox_id TEXT,
    sent_at TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(conversation_id) REFERENCES conversations(id)
);
CREATE TABLE IF NOT EXISTS meetings (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    person_id TEXT,
    attendee_email TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    timezone TEXT,
    status TEXT NOT NULL DEFAULT 'booked',
    google_event_id TEXT,
    html_link TEXT,
    meet_link TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(conversation_id) REFERENCES conversations(id)
);
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    ts TEXT, brand TEXT, person_id TEXT, touch_id TEXT,
    kind TEXT, detail TEXT
);
CREATE TABLE IF NOT EXISTS learnings (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    kind TEXT NOT NULL,
    subject TEXT DEFAULT '',
    subject_key TEXT DEFAULT '',
    detail TEXT DEFAULT '',
    hits INTEGER DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(brand, kind, subject_key)
);
CREATE TABLE IF NOT EXISTS signal_definitions (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    key TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    topic TEXT DEFAULT '',
    entity_type TEXT NOT NULL,
    value_type TEXT NOT NULL DEFAULT 'text',
    source_kind TEXT NOT NULL,
    owner TEXT NOT NULL,
    refresh_cadence TEXT NOT NULL,
    freshness_seconds INTEGER NOT NULL DEFAULT 7776000,
    evidence_required INTEGER NOT NULL DEFAULT 1,
    minimum_confidence REAL NOT NULL DEFAULT 0.6,
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(brand,key,version)
);
CREATE TABLE IF NOT EXISTS signal_observations (
    id TEXT PRIMARY KEY,
    definition_id TEXT NOT NULL,
    definition_key TEXT NOT NULL,
    brand TEXT NOT NULL,
    lead_id TEXT DEFAULT '',
    person_id TEXT DEFAULT '',
    conversation_id TEXT DEFAULT '',
    source_name TEXT NOT NULL,
    source_url TEXT DEFAULT '',
    provider_key TEXT DEFAULT '',
    value_json TEXT DEFAULT '',
    evidence TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    observed_at TEXT NOT NULL,
    expires_at TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'observed',
    fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(brand,fingerprint),
    FOREIGN KEY(definition_id) REFERENCES signal_definitions(id)
);
CREATE TABLE IF NOT EXISTS gtm_plays (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    key TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    name TEXT NOT NULL,
    lifecycle TEXT NOT NULL DEFAULT 'candidate',
    motion TEXT DEFAULT '',
    target_icp TEXT DEFAULT '',
    target_vantages TEXT DEFAULT '[]',
    required_signal_keys TEXT DEFAULT '[]',
    minimum_signal_matches INTEGER NOT NULL DEFAULT 1,
    hypothesis TEXT NOT NULL,
    action_policy TEXT NOT NULL,
    proof_type TEXT NOT NULL,
    proof_description TEXT NOT NULL,
    success_metric TEXT NOT NULL,
    kill_condition TEXT NOT NULL,
    source_refs TEXT DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(brand,key,version)
);
CREATE TABLE IF NOT EXISTS account_play_assessments (
    id TEXT PRIMARY KEY,
    lead_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    play_id TEXT NOT NULL,
    play_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    fit_score INTEGER NOT NULL DEFAULT 0,
    matched_signal_keys TEXT DEFAULT '[]',
    symptom TEXT DEFAULT '',
    root_cause TEXT DEFAULT '',
    current_workaround TEXT DEFAULT '',
    why_now TEXT DEFAULT '',
    proof_fit TEXT DEFAULT '',
    evidence_gaps TEXT DEFAULT '[]',
    disqualifiers TEXT DEFAULT '[]',
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(lead_id,play_id),
    FOREIGN KEY(lead_id) REFERENCES leads(id),
    FOREIGN KEY(play_id) REFERENCES gtm_plays(id)
);
CREATE TABLE IF NOT EXISTS market_accounts (
    id TEXT PRIMARY KEY,
    identity_key TEXT NOT NULL UNIQUE,
    canonical_domain TEXT DEFAULT '',
    apollo_org_id TEXT DEFAULT '',
    name TEXT NOT NULL,
    industry TEXT DEFAULT '',
    hq TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS brand_account_memberships (
    id TEXT PRIMARY KEY,
    market_account_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'research',
    priority_tier TEXT NOT NULL DEFAULT 'hard',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(market_account_id,brand),
    UNIQUE(brand,lead_id),
    FOREIGN KEY(market_account_id) REFERENCES market_accounts(id),
    FOREIGN KEY(lead_id) REFERENCES leads(id)
);
CREATE TABLE IF NOT EXISTS market_segments (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    key TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    name TEXT NOT NULL,
    geography TEXT NOT NULL,
    wedge TEXT NOT NULL,
    unit_of_analysis TEXT NOT NULL,
    enumeration_sources TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'active',
    estimated_total INTEGER NOT NULL DEFAULT 0,
    accounts_discovered INTEGER NOT NULL DEFAULT 0,
    accounts_with_opportunities INTEGER NOT NULL DEFAULT 0,
    source_exhausted INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(brand,key,version)
);
CREATE TABLE IF NOT EXISTS coverage_runs (
    id TEXT PRIMARY KEY,
    segment_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    source_name TEXT NOT NULL,
    query_fingerprint TEXT NOT NULL,
    cursor TEXT DEFAULT '',
    pages_examined INTEGER NOT NULL DEFAULT 0,
    candidates_seen INTEGER NOT NULL DEFAULT 0,
    accounts_added INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    exhausted INTEGER NOT NULL DEFAULT 0,
    gap_reason TEXT DEFAULT '',
    started_at TEXT NOT NULL,
    completed_at TEXT DEFAULT '',
    updated_at TEXT NOT NULL,
    UNIQUE(segment_id,source_name,query_fingerprint),
    FOREIGN KEY(segment_id) REFERENCES market_segments(id)
);
CREATE TABLE IF NOT EXISTS facilities (
    id TEXT PRIMARY KEY,
    market_account_id TEXT NOT NULL,
    name TEXT NOT NULL,
    facility_type TEXT DEFAULT '',
    address TEXT DEFAULT '',
    city TEXT DEFAULT '',
    region TEXT DEFAULT '',
    country TEXT DEFAULT '',
    source_url TEXT NOT NULL,
    source_excerpt TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'observed',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(market_account_id,name,source_url),
    FOREIGN KEY(market_account_id) REFERENCES market_accounts(id)
);
CREATE TABLE IF NOT EXISTS sales_opportunities (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    market_account_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    segment_id TEXT DEFAULT '',
    facility_id TEXT DEFAULT '',
    play_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    task_or_decision TEXT NOT NULL,
    mechanism TEXT DEFAULT '',
    consequence TEXT DEFAULT '',
    system_concept TEXT DEFAULT '',
    proof_offer TEXT DEFAULT '',
    evidence_status TEXT NOT NULL DEFAULT 'research_required',
    priority_tier TEXT NOT NULL DEFAULT 'hard',
    fit_score INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'research',
    evidence_gaps TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(brand,lead_id,play_id,title),
    FOREIGN KEY(market_account_id) REFERENCES market_accounts(id),
    FOREIGN KEY(lead_id) REFERENCES leads(id)
);
CREATE TABLE IF NOT EXISTS evidence_claims (
    id TEXT PRIMARY KEY,
    sales_opportunity_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    facility_id TEXT DEFAULT '',
    claim_type TEXT NOT NULL,
    claim_text TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_title TEXT DEFAULT '',
    source_excerpt TEXT NOT NULL,
    source_locator TEXT DEFAULT '',
    source_domain TEXT DEFAULT '',
    lineage_key TEXT NOT NULL,
    independence_group TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'observed',
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(sales_opportunity_id,lineage_key),
    FOREIGN KEY(sales_opportunity_id) REFERENCES sales_opportunities(id)
);
CREATE TABLE IF NOT EXISTS opportunity_stakeholders (
    id TEXT PRIMARY KEY,
    sales_opportunity_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    role TEXT NOT NULL,
    relationship_to_task TEXT DEFAULT '',
    can_observe TEXT DEFAULT '',
    can_decide TEXT DEFAULT '',
    priority INTEGER NOT NULL DEFAULT 100,
    active_thread INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'mapped',
    source TEXT NOT NULL DEFAULT 'contact_research',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(sales_opportunity_id,person_id),
    FOREIGN KEY(sales_opportunity_id) REFERENCES sales_opportunities(id),
    FOREIGN KEY(person_id) REFERENCES people(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_cold_thread_per_sales_opportunity
ON opportunity_stakeholders(sales_opportunity_id) WHERE active_thread=1;
CREATE INDEX IF NOT EXISTS idx_sales_opportunities_brand_priority
ON sales_opportunities(brand,priority_tier,fit_score DESC);
CREATE INDEX IF NOT EXISTS idx_evidence_claims_opportunity_type
ON evidence_claims(sales_opportunity_id,claim_type);
CREATE TABLE IF NOT EXISTS gtm_experiments (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    play_id TEXT NOT NULL,
    name TEXT NOT NULL,
    experiment_type TEXT NOT NULL,
    hypothesis TEXT NOT NULL,
    variable TEXT NOT NULL,
    constants TEXT NOT NULL DEFAULT '[]',
    control_description TEXT NOT NULL,
    variant_description TEXT NOT NULL,
    minimum_sends_per_arm INTEGER NOT NULL DEFAULT 500,
    baseline_sends INTEGER NOT NULL DEFAULT 0,
    baseline_positive_reply_rate REAL NOT NULL DEFAULT 0,
    success_target REAL NOT NULL DEFAULT 0,
    failure_floor REAL NOT NULL DEFAULT 0,
    measurement_days INTEGER NOT NULL DEFAULT 21,
    status TEXT NOT NULL DEFAULT 'draft',
    starts_at TEXT DEFAULT '',
    ends_at TEXT DEFAULT '',
    result_json TEXT DEFAULT '',
    confidence TEXT DEFAULT '',
    decision TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(play_id) REFERENCES gtm_plays(id)
);
CREATE TABLE IF NOT EXISTS experiment_assignments (
    id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    sequence_id TEXT DEFAULT '',
    arm TEXT NOT NULL,
    assigned_at TEXT NOT NULL,
    UNIQUE(experiment_id,person_id),
    FOREIGN KEY(experiment_id) REFERENCES gtm_experiments(id)
);
CREATE TABLE IF NOT EXISTS gtm_outcomes (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    kind TEXT NOT NULL,
    lead_id TEXT DEFAULT '',
    person_id TEXT DEFAULT '',
    sequence_id TEXT DEFAULT '',
    conversation_id TEXT DEFAULT '',
    play_id TEXT DEFAULT '',
    experiment_id TEXT DEFAULT '',
    experiment_assignment_id TEXT DEFAULT '',
    signal_observation_ids TEXT DEFAULT '[]',
    touch_id TEXT DEFAULT '',
    touch_stage INTEGER DEFAULT 0,
    contact_title TEXT DEFAULT '',
    contact_vantage TEXT DEFAULT '',
    account_hypothesis TEXT DEFAULT '',
    play_version INTEGER DEFAULT 0,
    experiment_arm TEXT DEFAULT '',
    copy_policy_version INTEGER DEFAULT 0,
    generation_backend TEXT DEFAULT '',
    generation_model TEXT DEFAULT '',
    value REAL NOT NULL DEFAULT 0,
    detail TEXT DEFAULT '',
    source TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(brand,fingerprint)
);
CREATE TABLE IF NOT EXISTS proof_briefs (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    lead_id TEXT DEFAULT '',
    person_id TEXT DEFAULT '',
    conversation_id TEXT DEFAULT '',
    play_id TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'draft',
    problem TEXT NOT NULL,
    current_workflow TEXT DEFAULT '',
    evidence_available TEXT DEFAULT '[]',
    scope TEXT NOT NULL,
    customer_data TEXT DEFAULT '[]',
    success_metric TEXT NOT NULL,
    baseline TEXT DEFAULT '',
    target TEXT DEFAULT '',
    stop_condition TEXT NOT NULL,
    stakeholders TEXT DEFAULT '[]',
    owner TEXT DEFAULT '',
    expansion_path TEXT DEFAULT '',
    result TEXT DEFAULT '',
    learnings TEXT DEFAULT '[]',
    approved_at TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(conversation_id,play_id)
);
CREATE TABLE IF NOT EXISTS customer_development (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    person_id TEXT DEFAULT '',
    conversation_id TEXT DEFAULT '',
    stage TEXT NOT NULL DEFAULT 'hypothesis',
    problem TEXT DEFAULT '',
    task_scope TEXT DEFAULT '',
    site TEXT DEFAULT '',
    current_workflow TEXT DEFAULT '',
    why_manual TEXT DEFAULT '',
    variations TEXT DEFAULT '[]',
    exceptions TEXT DEFAULT '[]',
    evidence TEXT DEFAULT '[]',
    economics TEXT DEFAULT '',
    success_criteria TEXT DEFAULT '',
    stop_condition TEXT DEFAULT '',
    stakeholders TEXT DEFAULT '[]',
    commitment_kind TEXT NOT NULL DEFAULT 'none',
    commitment_detail TEXT DEFAULT '',
    quantity TEXT DEFAULT '',
    commercial_case TEXT DEFAULT '',
    timeline TEXT DEFAULT '',
    loi_conditions TEXT DEFAULT '',
    next_action TEXT DEFAULT '',
    engaged_at TEXT DEFAULT '',
    source TEXT NOT NULL DEFAULT 'manual_crm',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(brand,lead_id),
    FOREIGN KEY(lead_id) REFERENCES leads(id)
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
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    next_run_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_error TEXT,
    dedup_key TEXT,
    result TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_touches_due ON touches(status, due_at);
CREATE INDEX IF NOT EXISTS idx_touches_person ON touches(person_id);
CREATE INDEX IF NOT EXISTS idx_people_brand_email ON people(brand, email);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_signal_definitions_brand
    ON signal_definitions(brand,status,key,version);
CREATE INDEX IF NOT EXISTS idx_signal_observations_entity
    ON signal_observations(brand,lead_id,person_id,status,expires_at);
CREATE INDEX IF NOT EXISTS idx_gtm_plays_brand
    ON gtm_plays(brand,lifecycle,key,version);
CREATE INDEX IF NOT EXISTS idx_account_play_assessments_rank
    ON account_play_assessments(brand,play_id,status,fit_score);
CREATE INDEX IF NOT EXISTS idx_gtm_experiments_play
    ON gtm_experiments(play_id,status,created_at);
CREATE INDEX IF NOT EXISTS idx_gtm_outcomes_attribution
    ON gtm_outcomes(brand,play_id,experiment_id,kind,occurred_at);
CREATE INDEX IF NOT EXISTS idx_proof_briefs_brand
    ON proof_briefs(brand,status,updated_at);
CREATE INDEX IF NOT EXISTS idx_customer_development_stage
    ON customer_development(brand,stage,updated_at);
CREATE INDEX IF NOT EXISTS idx_replies_msgid ON replies(message_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_sequence
    ON conversations(sequence_id) WHERE sequence_id IS NOT NULL AND sequence_id<>'';
CREATE INDEX IF NOT EXISTS idx_conversations_person
    ON conversations(brand, person_id, updated_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_messages_msgid
    ON conversation_messages(message_id) WHERE message_id IS NOT NULL AND message_id<>'';
CREATE INDEX IF NOT EXISTS idx_conversation_messages_due
    ON conversation_messages(status, direction, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_meetings_conversation_start
    ON meetings(conversation_id, starts_at);
CREATE INDEX IF NOT EXISTS idx_opportunities_brand_status
    ON opportunities(brand, pipeline_status, opportunity_status, fit_score);
CREATE INDEX IF NOT EXISTS idx_opportunity_contacts_email
    ON opportunity_contacts(brand, email);
CREATE INDEX IF NOT EXISTS idx_opportunity_touches_due
    ON opportunity_touches(status, due_at);
CREATE INDEX IF NOT EXISTS idx_opportunity_replies_msgid
    ON opportunity_replies(message_id);
-- Idempotency: a supervisor re-deciding every tick must not enqueue the same
-- logical action twice. A partial unique index lets un-keyed jobs coexist while
-- keyed ones (e.g. "gnk:source:2026-08-07") stay singular.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedup ON jobs(dedup_key)
    WHERE dedup_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(status, next_run_at, priority);
"#;

#[cfg(test)]
mod tests {
    use super::{
        evidence_names_operating_site, facility_observation_for_task, AccountPlayAssessment,
        ApplicationBrief, ConversationMessage, CustomerDevelopmentRecord, Db, GtmExperiment,
        GtmOutcome, Job, Lead, Mailbox, Meeting, Opportunity, OpportunityContact, OpportunityTouch,
        Person, Sequence, SignalObservation, Touch, CURRENT_COPY_POLICY_VERSION,
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
    fn facility_link_requires_a_named_site_not_a_generic_warehouse_role() {
        assert!(!evidence_names_operating_site(
            "The Warehouse Selector role loads full cases onto pallets."
        ));
        assert!(evidence_names_operating_site(
            "The Brantford role loads cartons into cases and palletizes finished goods."
        ));
    }

    #[test]
    fn task_can_use_a_site_claim_only_from_the_exact_same_source_page() {
        let task = SignalObservation {
            definition_key: "account.bounded_repetitive_task".into(),
            source_url: "https://example.com/jobs/operator".into(),
            evidence: "The operator palletizes finished cases.".into(),
            ..Default::default()
        };
        let same_page_site = SignalObservation {
            definition_key: "account.job_posting_workflow_evidence".into(),
            source_url: "https://example.com/jobs/operator/".into(),
            evidence: "The Newmarket role is based at the company's production plant.".into(),
            ..Default::default()
        };
        let unrelated_site = SignalObservation {
            definition_key: "account.fit_evidence".into(),
            source_url: "https://example.com/locations".into(),
            evidence: "The company has a facility in Toronto.".into(),
            ..Default::default()
        };
        let linked = [task.clone(), same_page_site, unrelated_site.clone()];
        assert_eq!(
            facility_observation_for_task(&linked)
                .expect("same-page facility")
                .evidence,
            "The Newmarket role is based at the company's production plant."
        );
        assert!(facility_observation_for_task(&[task, unrelated_site]).is_none());
    }

    #[test]
    fn reopening_database_pauses_scheduled_sequences_from_an_old_copy_policy() {
        let path = std::env::temp_dir().join(format!(
            "spruce-copy-cutover-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Db::open(&path).expect("open temp db");
        let sequence_id = db
            .create_sequence(&Sequence {
                id: "stale-policy-sequence".into(),
                person_id: "stale-policy-person".into(),
                lead_id: "stale-policy-lead".into(),
                brand: "gnk".into(),
                copy_policy_version: CURRENT_COPY_POLICY_VERSION - 1,
                status: "active".into(),
                ..Default::default()
            })
            .expect("create stale sequence");
        db.insert_touch(&Touch {
            id: "stale-policy-touch".into(),
            sequence_id: sequence_id.clone(),
            person_id: "stale-policy-person".into(),
            lead_id: "stale-policy-lead".into(),
            brand: "gnk".into(),
            stage: 1,
            status: "scheduled".into(),
            ..Default::default()
        })
        .expect("create scheduled touch");
        drop(db);

        let reopened = Db::open(&path).expect("reopen temp db");
        let sequence = reopened
            .sequence_gtm_attribution(&sequence_id)
            .expect("sequence query")
            .expect("stale sequence");
        assert_eq!(sequence.status, "paused");
        let touches = reopened
            .list_touches_for_sequence(&sequence_id)
            .expect("touch query");
        assert_eq!(touches[0].status, "cancelled");
        drop(reopened);
        remove_temp_db(&path);
    }

    #[test]
    fn one_company_can_hold_distinct_opportunities_for_multiple_portfolio_brands() {
        let db = Db::open(":memory:").expect("open memory db");
        let gnk = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "portfolio-company".into(),
                name: "Shared Company".into(),
                domain: "https://www.shared.example/".into(),
                ..Default::default()
            })
            .expect("claim company for gnk");

        let outagehub = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "portfolio-company".into(),
                name: "Shared Company".into(),
                domain: "shared.example".into(),
                ..Default::default()
            })
            .expect("same company may have a distinct OutageHub opportunity");

        let wapahki = db
            .upsert_lead(&Lead {
                brand: "wapahki".into(),
                apollo_org_id: "different-apollo-id".into(),
                name: "Shared Company Alias".into(),
                domain: "www.shared.example".into(),
                ..Default::default()
            })
            .expect("canonical domain resolves across brands");
        let gnk_account = db.market_account_for_lead(&gnk).unwrap().unwrap();
        let outage_account = db.market_account_for_lead(&outagehub).unwrap().unwrap();
        let wapahki_account = db.market_account_for_lead(&wapahki).unwrap().unwrap();
        assert_eq!(gnk_account.id, outage_account.id);
        assert_eq!(gnk_account.id, wapahki_account.id);
        assert_eq!(db.list_market_accounts(None).unwrap().len(), 1);
    }

    #[test]
    fn legacy_contact_vantage_is_backfilled_for_readiness() {
        let path = std::env::temp_dir().join(format!(
            "spruce-contact-vantage-backfill-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Db::open(&path).expect("open temp db");
        let shared = std::sync::Arc::new(db);
        crate::gtm::seed_defaults(&shared).expect("seed defaults");
        let lead_id = shared
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "org-legacy-vantage".into(),
                ..Default::default()
            })
            .expect("lead");
        let person_id = shared
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "person-legacy-vantage".into(),
                title: "Claims Operations Manager".into(),
                vantage: "process_owner".into(),
                email_status: "verified".into(),
                ..Default::default()
            })
            .expect("person");
        {
            let conn = shared.conn.lock().unwrap();
            conn.execute(
                "DELETE FROM signal_observations WHERE person_id=?1",
                rusqlite::params![person_id],
            )
            .expect("simulate pre-signal contact");
        }
        assert!(shared
            .list_active_signal_observations(Some("gnk"), Some(&lead_id), Some(&person_id))
            .unwrap()
            .is_empty());

        shared
            .backfill_legacy_signal_observations()
            .expect("backfill");
        let observations = shared
            .list_active_signal_observations(Some("gnk"), Some(&lead_id), Some(&person_id))
            .unwrap();
        assert!(observations
            .iter()
            .any(|observation| observation.definition_key == "contact.workflow_vantage"));
        assert!(observations
            .iter()
            .all(|observation| !observation.evidence.contains("can_observe")));
        drop(shared);
        remove_temp_db(&path);
    }

    #[test]
    fn account_throttle_counts_new_fronts_engaged_and_sent() {
        let path = std::env::temp_dir().join(format!(
            "spruce-account-throttle-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Db::open(&path).expect("open temp db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "org-throttle".into(),
                ..Default::default()
            })
            .expect("insert lead");

        // Two people at the same account.
        let mut person_ids = Vec::new();
        for i in 0..2 {
            person_ids.push(
                db.upsert_person(&Person {
                    lead_id: lead_id.clone(),
                    brand: "gnk".into(),
                    apollo_person_id: format!("person-throttle-{i}"),
                    email: format!("p{i}@example.com"),
                    email_status: "verified".into(),
                    status: "verified".into(),
                    ..Default::default()
                })
                .expect("insert person"),
            );
        }

        let day_start = Utc.with_ymd_and_hms(2026, 8, 7, 0, 0, 0).unwrap();
        let day_end = Utc.with_ymd_and_hms(2026, 8, 8, 0, 0, 0).unwrap();

        // Nothing sent yet: no open fronts, nobody engaged.
        assert_eq!(
            db.account_openers_sent_between(&lead_id, day_start, day_end)
                .unwrap(),
            0
        );
        assert_eq!(db.account_engaged_people(&lead_id).unwrap(), 0);

        // Open person 0 with a stage-1 send timestamped inside the target day.
        let seq0 = db
            .create_sequence(&Sequence {
                person_id: person_ids[0].clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                status: "active".into(),
                ..Default::default()
            })
            .expect("seq0");
        let touch0 = db
            .insert_touch(&Touch {
                sequence_id: seq0.clone(),
                person_id: person_ids[0].clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                stage: 1,
                channel: "email".into(),
                status: "sent".into(),
                due_at: "2026-08-07T09:00:00Z".into(),
                ..Default::default()
            })
            .expect("touch0");
        db.set_person_status(&person_ids[0], "contacted")
            .expect("contacted");
        set_sent_at(&db, &touch0, "2026-08-07T09:00:00+00:00");

        // One new front opened today, one engaged person, one send on the seq.
        assert_eq!(
            db.account_openers_sent_between(&lead_id, day_start, day_end)
                .unwrap(),
            1
        );
        assert_eq!(db.account_engaged_people(&lead_id).unwrap(), 1);
        assert_eq!(db.sequence_sent_count(&seq0).unwrap(), 1);

        // Person 1 opened on a *different* day: engaged, but not a front today.
        let seq1 = db
            .create_sequence(&Sequence {
                person_id: person_ids[1].clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                status: "active".into(),
                ..Default::default()
            })
            .expect("seq1");
        let touch1 = db
            .insert_touch(&Touch {
                sequence_id: seq1.clone(),
                person_id: person_ids[1].clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                stage: 1,
                channel: "email".into(),
                status: "sent".into(),
                due_at: "2026-08-01T09:00:00Z".into(),
                ..Default::default()
            })
            .expect("touch1");
        db.set_person_status(&person_ids[1], "contacted")
            .expect("contacted1");
        set_sent_at(&db, &touch1, "2026-08-01T09:00:00+00:00");

        // Still one opener *today*, but two engaged across days.
        assert_eq!(
            db.account_openers_sent_between(&lead_id, day_start, day_end)
                .unwrap(),
            1
        );
        assert_eq!(db.account_engaged_people(&lead_id).unwrap(), 2);

        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn rfc_headers_keep_unknown_referrals_on_the_original_thread() {
        let path = std::env::temp_dir().join(format!(
            "spruce-conversation-test-{}.sqlite",
            Uuid::new_v4()
        ));
        let db = Db::open(&path).expect("open temp db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "thread-org".into(),
                name: "Thread Co".into(),
                ..Default::default()
            })
            .expect("lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "thread-person".into(),
                email: "original@example.com".into(),
                email_status: "verified".into(),
                status: "contacted".into(),
                ..Default::default()
            })
            .expect("person");
        let sequence_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                status: "active".into(),
                ..Default::default()
            })
            .expect("sequence");
        db.insert_touch(&Touch {
            sequence_id: sequence_id.clone(),
            person_id: person_id.clone(),
            lead_id,
            brand: "gnk".into(),
            channel: "email".into(),
            status: "sent".into(),
            message_id: "<cold-1@example.com>".into(),
            ..Default::default()
        })
        .expect("touch");

        // The new sender was never sourced, but References points at our touch.
        let conversation = db
            .conversation_for_inbound(
                "gnk",
                "referral@example.net",
                "Re: workflow",
                &["<cold-1@example.com>".into()],
            )
            .expect("resolve")
            .expect("conversation");
        assert_eq!(conversation.person_id, person_id);
        assert_eq!(conversation.sequence_id, sequence_id);

        db.insert_conversation_message(&ConversationMessage {
            conversation_id: conversation.id.clone(),
            direction: "inbound".into(),
            sender_email: "referral@example.net".into(),
            status: "received".into(),
            message_id: "<inbound-1@example.net>".into(),
            ..Default::default()
        })
        .expect("inbound");
        db.insert_conversation_message(&ConversationMessage {
            conversation_id: conversation.id.clone(),
            direction: "outbound".into(),
            recipient_email: "referral@example.net".into(),
            status: "sent".into(),
            message_id: "<reply-1@example.com>".into(),
            offered_slots: vec!["2026-08-10T08:00:00+00:00".into()],
            ..Default::default()
        })
        .expect("reply");
        db.insert_conversation_message(&ConversationMessage {
            conversation_id: conversation.id.clone(),
            direction: "outbound".into(),
            recipient_email: "referral@example.net".into(),
            status: "draft".into(),
            offered_slots: vec!["2026-08-11T08:00:00+00:00".into()],
            ..Default::default()
        })
        .expect("draft");

        let same = db
            .conversation_for_inbound(
                "gnk",
                "another-cc@example.org",
                "Re: workflow",
                &["<inbound-1@example.net>".into()],
            )
            .expect("resolve chain")
            .expect("same conversation");
        assert_eq!(same.id, conversation.id);
        assert_eq!(
            db.sent_offered_slots(&conversation.id).unwrap(),
            vec!["2026-08-10T08:00:00+00:00"]
        );

        let booked = db
            .record_meeting(&Meeting {
                conversation_id: conversation.id.clone(),
                brand: "gnk".into(),
                person_id: person_id.clone(),
                attendee_email: "referral@example.net".into(),
                starts_at: "2026-08-10T08:00:00+00:00".into(),
                ends_at: "2026-08-10T08:30:00+00:00".into(),
                status: "booked".into(),
                google_event_id: "google-1".into(),
                meet_link: "https://meet.google.com/example".into(),
                ..Default::default()
            })
            .expect("booked meeting");
        let duplicate_pending = db
            .record_meeting(&Meeting {
                conversation_id: conversation.id.clone(),
                brand: "gnk".into(),
                person_id,
                attendee_email: "referral@example.net".into(),
                starts_at: "2026-08-10T08:00:00+00:00".into(),
                ends_at: "2026-08-10T08:30:00+00:00".into(),
                status: "pending".into(),
                ..Default::default()
            })
            .expect("duplicate acceptance");
        assert_eq!(booked, duplicate_pending);
        let meetings = db.list_meetings(Some("gnk")).unwrap();
        assert_eq!(meetings[0].status, "booked");
        assert_eq!(meetings[0].google_event_id, "google-1");

        drop(db);
        remove_temp_db(&path);
    }

    fn set_sent_at(db: &Db, touch_id: &str, sent_at: &str) {
        let conn = db.conn.lock().unwrap();
        conn.execute(
            "UPDATE touches SET sent_at=?2 WHERE id=?1",
            super::params![touch_id, sent_at],
        )
        .expect("stamp sent_at");
    }

    #[test]
    fn job_queue_leases_completes_and_dead_letters() {
        let path =
            std::env::temp_dir().join(format!("spruce-jobs-queue-test-{}.sqlite", Uuid::new_v4()));
        let db = Db::open(&path).expect("open temp db");

        // Low max_attempts so we can drive it into the dead-letter state.
        let id = db
            .enqueue_job(&Job {
                brand: "gnk".into(),
                kind: "source".into(),
                max_attempts: 2,
                ..Default::default()
            })
            .expect("enqueue");

        // Claim it: leased, attempt 1, and nothing else is due.
        let claimed = db
            .claim_job("worker-1", 300)
            .expect("claim")
            .expect("a job");
        assert_eq!(claimed.id, id);
        assert_eq!(claimed.status, "leased");
        assert_eq!(claimed.attempt_count, 1);
        assert!(db.claim_job("worker-1", 300).expect("claim2").is_none());

        // First failure retries (attempt 1 < max 2), but backs off into the
        // future so it isn't immediately re-claimable.
        assert_eq!(db.fail_job(&id, "boom").expect("fail"), "pending");
        assert!(db.claim_job("worker-1", 300).expect("claim3").is_none());

        // Force it due, re-claim (attempt 2), fail again → dead-letter.
        set_next_run_past(&db, &id);
        let again = db.claim_job("worker-1", 300).expect("claim4").expect("due");
        assert_eq!(again.attempt_count, 2);
        assert_eq!(db.fail_job(&id, "boom again").expect("fail2"), "dead");
        assert_eq!(db.get_job(&id).expect("get").expect("row").status, "dead");

        // The dead-letter backlog is visible to observability.
        let counts = db.job_status_counts(Some("gnk")).expect("counts");
        assert_eq!(
            counts.iter().find(|(s, _)| s == "dead").map(|(_, n)| *n),
            Some(1)
        );

        drop(db);
        remove_temp_db(&path);
    }

    #[test]
    fn customer_development_round_trips_and_updates_per_account() {
        let db = Db::open(":memory:").expect("open memory db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "wapahki".into(),
                apollo_org_id: "wapahki-discovery-account".into(),
                name: "Example Foods".into(),
                ..Default::default()
            })
            .expect("insert lead");
        let id = db
            .upsert_customer_development(&CustomerDevelopmentRecord {
                brand: "wapahki".into(),
                lead_id: lead_id.clone(),
                stage: "task_mapped".into(),
                problem: "Manual case handling".into(),
                task_scope: "Conveyor to pallet".into(),
                variations: vec!["Five formats".into()],
                commitment_kind: "none".into(),
                ..Default::default()
            })
            .expect("insert discovery");
        let stored = db
            .customer_development_for_lead("wapahki", &lead_id)
            .expect("read discovery")
            .expect("record exists");
        assert_eq!(stored.id, id);
        assert_eq!(stored.variations, vec!["Five formats"]);

        let updated_id = db
            .upsert_customer_development(&CustomerDevelopmentRecord {
                id: id.clone(),
                brand: "wapahki".into(),
                lead_id: lead_id.clone(),
                stage: "evaluation_agreed".into(),
                problem: stored.problem,
                commitment_kind: "evaluation_agreed".into(),
                commitment_detail: "Plant manager agreed to a video review.".into(),
                ..Default::default()
            })
            .expect("update discovery");
        assert_eq!(updated_id, id);
        let rows = db
            .list_customer_development(Some("wapahki"))
            .expect("list discovery");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].stage, "evaluation_agreed");
        assert_eq!(rows[0].commitment_kind, "evaluation_agreed");
    }

    #[test]
    fn enqueue_job_is_idempotent_on_dedup_key() {
        let path =
            std::env::temp_dir().join(format!("spruce-jobs-dedup-test-{}.sqlite", Uuid::new_v4()));
        let db = Db::open(&path).expect("open temp db");
        let make = || Job {
            brand: "gnk".into(),
            kind: "source".into(),
            dedup_key: "gnk:source:2026-08-07".into(),
            ..Default::default()
        };
        let a = db.enqueue_job(&make()).expect("first");
        let b = db.enqueue_job(&make()).expect("second");
        assert_eq!(a, b, "same dedup key must collapse to one job");
        let pending = db
            .job_status_counts(Some("gnk"))
            .expect("counts")
            .iter()
            .find(|(s, _)| s == "pending")
            .map(|(_, n)| *n);
        assert_eq!(pending, Some(1));

        drop(db);
        remove_temp_db(&path);
    }

    fn set_next_run_past(db: &Db, id: &str) {
        let conn = db.conn.lock().unwrap();
        conn.execute(
            "UPDATE jobs SET next_run_at=?2 WHERE id=?1",
            super::params![id, "2000-01-01T00:00:00+00:00"],
        )
        .expect("force due");
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
    fn conditional_linkedin_touch_uses_connection_state_at_approval() {
        let db = Db::open(":memory:").expect("open memory db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "conditional-org".into(),
                ..Default::default()
            })
            .expect("lead");
        let make_person = |apollo_id: &str| {
            db.upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: apollo_id.into(),
                linkedin_url: format!("https://linkedin.com/in/{apollo_id}"),
                email: format!("{apollo_id}@example.com"),
                email_status: "verified".into(),
                status: "verified".into(),
                ..Default::default()
            })
            .expect("person")
        };
        let unknown = make_person("unknown-linkedin");
        let connected = make_person("connected-linkedin");
        db.set_person_linkedin_status(&connected, "connected")
            .expect("mark connected");

        for person_id in [&unknown, &connected] {
            let sequence_id = db
                .create_sequence(&Sequence {
                    person_id: person_id.clone(),
                    lead_id: lead_id.clone(),
                    brand: "gnk".into(),
                    status: "active".into(),
                    ..Default::default()
                })
                .expect("sequence");
            db.insert_touch(&Touch {
                sequence_id,
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                stage: 5,
                channel: "linkedin_or_email".into(),
                status: "draft".into(),
                review_passes: Some(true),
                due_at: "2030-01-01T00:00:00Z".into(),
                ..Default::default()
            })
            .expect("touch");
        }

        assert_eq!(db.schedule_reviewed_touches(Some("gnk"), None).unwrap(), 1);
        assert_eq!(
            db.get_person(&connected).unwrap().unwrap().linkedin_status,
            "connected"
        );
        assert_eq!(
            db.list_touches_for_person(&unknown).unwrap()[0].status,
            "scheduled"
        );
        assert_eq!(
            db.list_touches_for_person(&connected).unwrap()[0].status,
            "draft"
        );
    }

    #[test]
    fn live_send_claims_are_single_winner() {
        let db = Db::open(":memory:").expect("open memory db");
        {
            let conn = db.conn.lock().expect("lock db");
            conn.execute(
                "INSERT INTO sequences
                   (id,person_id,lead_id,brand,copy_policy_version,status,created_at)
                 VALUES ('sequence-claim','person-claim','lead-claim','gnk',?1,'active','now')",
                rusqlite::params![CURRENT_COPY_POLICY_VERSION],
            )
            .expect("seed current sequence");
            conn.execute(
                "INSERT INTO sequences
                   (id,person_id,lead_id,brand,copy_policy_version,status,created_at)
                 VALUES ('sequence-stale','person-claim','lead-stale','gnk',?1,'active','now')",
                rusqlite::params![CURRENT_COPY_POLICY_VERSION - 1],
            )
            .expect("seed stale sequence");
            conn.execute_batch(
                "INSERT INTO touches
                   (id,sequence_id,person_id,lead_id,brand,status)
                 VALUES ('touch-claim','sequence-claim','person-claim','lead-claim','gnk','scheduled');
                 INSERT INTO touches
                   (id,sequence_id,person_id,lead_id,brand,status)
                 VALUES ('touch-stale','sequence-stale','person-stale','lead-stale','gnk','scheduled');
                 INSERT INTO conversations
                   (id,brand,person_id,status,created_at,updated_at)
                 VALUES ('conversation-claim','gnk','person-claim','open','now','now');
                 INSERT INTO conversation_messages
                   (id,conversation_id,direction,status,created_at)
                 VALUES ('message-claim','conversation-claim','outbound','scheduled','now');
                 INSERT INTO opportunities
                   (id,brand,fingerprint)
                 VALUES ('opportunity-claim','gnk','opportunity-claim');
                 INSERT INTO opportunity_contacts
                   (id,opportunity_id,brand,contact_key)
                 VALUES ('contact-claim','opportunity-claim','gnk','contact-claim');
                 INSERT INTO opportunity_touches
                   (id,opportunity_id,contact_id,brand,status)
                 VALUES ('opportunity-touch-claim','opportunity-claim','contact-claim','gnk','scheduled');",
            )
            .expect("seed claim rows");
        }

        assert_eq!(
            db.active_sequence_for_person("person-claim").unwrap(),
            Some("sequence-claim".to_string())
        );
        assert!(db.claim_touch_for_send("touch-claim").unwrap());
        assert!(!db.claim_touch_for_send("touch-claim").unwrap());
        assert!(!db.claim_touch_for_send("touch-stale").unwrap());
        assert!(db
            .claim_conversation_message_for_send("message-claim")
            .unwrap());
        assert!(!db
            .claim_conversation_message_for_send("message-claim")
            .unwrap());
        assert!(db
            .claim_opportunity_touch_for_send("opportunity-touch-claim")
            .unwrap());
        assert!(!db
            .claim_opportunity_touch_for_send("opportunity-touch-claim")
            .unwrap());
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
    fn approved_email_capacity_is_isolated_per_business() {
        let db = Db::open(":memory:").expect("open memory db");
        let due = Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 8, 11, 0, 0, 0).unwrap();
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "capacity-org".into(),
                name: "Capacity Account".into(),
                ..Default::default()
            })
            .expect("insert capacity lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "capacity-person".into(),
                name: "Capacity Person".into(),
                ..Default::default()
            })
            .expect("insert capacity person");
        let sequence_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                status: "active".into(),
                copy_policy_version: CURRENT_COPY_POLICY_VERSION,
                ..Default::default()
            })
            .expect("insert current sequence");
        for index in 0..30 {
            db.insert_touch(&Touch {
                id: format!("gnk-{index}"),
                sequence_id: sequence_id.clone(),
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                status: match index {
                    0 => "sent",
                    1 => "scheduled",
                    _ => "draft",
                }
                .into(),
                channel: "email".into(),
                due_at: due.to_rfc3339(),
                ..Default::default()
            })
            .expect("insert gnk touch");
        }
        let stale_sequence_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                status: "active".into(),
                copy_policy_version: CURRENT_COPY_POLICY_VERSION - 1,
                ..Default::default()
            })
            .expect("insert stale sequence");
        db.insert_touch(&Touch {
            id: "gnk-stale-scheduled".into(),
            sequence_id: stale_sequence_id,
            person_id,
            lead_id,
            brand: "gnk".into(),
            status: "scheduled".into(),
            channel: "email".into(),
            due_at: due.to_rfc3339(),
            ..Default::default()
        })
        .expect("insert stale scheduled touch");
        db.insert_touch(&Touch {
            id: "wapahki-1".into(),
            brand: "wapahki".into(),
            status: "draft".into(),
            channel: "linkedin_request".into(),
            due_at: due.to_rfc3339(),
            ..Default::default()
        })
        .expect("insert wapahki touch");

        assert_eq!(
            db.planned_touch_count_between("gnk", start, end).unwrap(),
            2
        );
        assert_eq!(
            db.planned_touch_count_between("wapahki", start, end)
                .unwrap(),
            0
        );
        assert_eq!(
            db.planned_touch_count_between("outagehub", start, end)
                .unwrap(),
            0
        );
        assert_eq!(db.upcoming_calendar("gnk", 20).unwrap().len(), 1);
    }

    #[test]
    fn learnings_reinforce_dedup_and_scope_by_brand() {
        let db = Db::open(":memory:").expect("open memory db");

        // First skip of a company records one learning.
        db.record_learning(
            "gnk",
            "qualification_skip",
            "Acme Co",
            "acme-id",
            "thin payload",
        )
        .unwrap();
        // Skipping the same company again reinforces it (hits bump), refreshing
        // the detail rather than creating a duplicate row.
        db.record_learning(
            "gnk",
            "qualification_skip",
            "Acme Co",
            "acme-id",
            "still thin payload",
        )
        .unwrap();
        // A different company for the same brand is its own learning.
        db.record_learning(
            "gnk",
            "qualification_skip",
            "Beta Ltd",
            "beta-id",
            "vendor, not buyer",
        )
        .unwrap();
        // Same subject key but a different brand must not collide.
        db.record_learning(
            "wapahki",
            "qualification_skip",
            "Acme Co",
            "acme-id",
            "wrong motion",
        )
        .unwrap();
        db.record_learning(
            "gnk",
            "qualification_skip",
            "Legacy Borderline Co",
            "legacy-two-signal",
            "only 2 canonical play signal(s) matched; 3 required; play-fit score 62/100",
        )
        .unwrap();
        db.record_learning(
            "gnk",
            "qualification_skip",
            "Current Hard Reject",
            "current-hard-reject",
            "qv2 hard_reject: only 2 canonical play signal(s) matched; affirmative blocker",
        )
        .unwrap();

        let gnk = db
            .recent_learnings(Some("gnk"), Some("qualification_skip"), 10)
            .unwrap();
        assert_eq!(gnk.len(), 4, "four distinct gnk learnings, no duplicate");
        let acme = gnk
            .iter()
            .find(|l| l.subject == "Acme Co")
            .expect("acme learning");
        assert_eq!(acme.hits, 2, "reinforced twice");
        assert_eq!(acme.detail, "still thin payload", "detail refreshed");

        // Known-reject keys are per-brand and drive the re-research skip.
        let keys = db.learning_keys("gnk", "qualification_skip").unwrap();
        assert!(keys.contains("acme-id") && keys.contains("beta-id"));
        let durable = db.durable_qualification_skip_keys("gnk", "qv2").unwrap();
        assert!(!durable.contains("legacy-two-signal"));
        assert!(!durable.contains("acme-id"));
        assert!(durable.contains("current-hard-reject"));
        assert_eq!(
            db.learning_keys("outagehub", "qualification_skip")
                .unwrap()
                .len(),
            0
        );

        // No brand filter spans the whole portfolio.
        let all = db.recent_learnings(None, None, 10).unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn gtm_defaults_drive_evidence_assessment_and_stable_experiment_assignment() {
        let db = Db::open(":memory:").expect("open memory db");
        let play = db
            .current_gtm_play("outagehub")
            .expect("load current play")
            .expect("seeded outagehub play");
        assert_eq!(play.version, 13);
        assert_eq!(play.minimum_signal_matches, 4);
        assert!(play
            .required_signal_keys
            .contains(&"account.historical_location_outage_match".to_string()));
        assert!(!play
            .required_signal_keys
            .contains(&"account.existing_operational_system".to_string()));

        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-gtm-test".into(),
                name: "Distributed Operator".into(),
                signals: vec!["Runs remote facilities across utility territories.".into()],
                ..Default::default()
            })
            .expect("upsert lead");
        let observations = db
            .list_active_signal_observations(Some("outagehub"), Some(&lead_id), None)
            .expect("list observations");
        assert!(
            observations.is_empty(),
            "internal lead.signals must not become active evidence without source qualification"
        );

        db.upsert_account_play_assessment(&AccountPlayAssessment {
            lead_id: lead_id.clone(),
            brand: "outagehub".into(),
            play_id: play.id.clone(),
            play_version: play.version,
            status: "qualified".into(),
            fit_score: 84,
            root_cause: "Internal telemetry cannot establish the external utility cause.".into(),
            proof_fit: "Replay three historical incidents.".into(),
            ..Default::default()
        })
        .expect("upsert play assessment");
        let assessments = db
            .list_account_play_assessments(Some("outagehub"))
            .expect("list assessments");
        assert_eq!(assessments.len(), 1);
        assert_eq!(assessments[0].fit_score, 84);

        let underpowered_id = db
            .create_gtm_experiment(&GtmExperiment {
                brand: "outagehub".into(),
                play_id: play.id.clone(),
                name: "Underpowered test".into(),
                experiment_type: "copy_only".into(),
                hypothesis: "A correction CTA increases positive replies.".into(),
                variable: "CTA".into(),
                constants: vec!["same qualified list".into()],
                control_description: "Question CTA".into(),
                variant_description: "Correction CTA".into(),
                minimum_sends_per_arm: 250,
                baseline_sends: 100,
                baseline_positive_reply_rate: 0.02,
                ..Default::default()
            })
            .expect("create underpowered experiment");
        assert!(db
            .set_gtm_experiment_status(&underpowered_id, "running")
            .is_err());

        let experiment_id = db
            .create_gtm_experiment(&GtmExperiment {
                brand: "outagehub".into(),
                play_id: play.id.clone(),
                name: "Correction CTA".into(),
                experiment_type: "copy_only".into(),
                hypothesis: "A correction CTA increases positive replies.".into(),
                variable: "CTA".into(),
                constants: vec!["same qualified list".into(), "same sender".into()],
                control_description: "Question CTA".into(),
                variant_description: "Correction CTA".into(),
                minimum_sends_per_arm: 250,
                baseline_sends: 300,
                baseline_positive_reply_rate: 0.02,
                ..Default::default()
            })
            .expect("create experiment");
        db.set_gtm_experiment_status(&experiment_id, "running")
            .expect("start experiment");
        let first = db
            .ensure_experiment_assignment(&experiment_id, &lead_id, "person-1", "")
            .expect("assign arm");
        let second = db
            .ensure_experiment_assignment(&experiment_id, &lead_id, "person-1", "")
            .expect("reuse arm");
        assert_eq!(first.id, second.id);
        assert_eq!(first.arm, second.arm);

        let sequence_id = db
            .create_sequence(&Sequence {
                person_id: "person-1".into(),
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                play_id: play.id.clone(),
                play_version: 7,
                experiment_id: experiment_id.clone(),
                experiment_arm: first.arm.clone(),
                experiment_assignment_id: first.id.clone(),
                signal_observation_ids: vec!["signal-1".into()],
                copy_policy_version: CURRENT_COPY_POLICY_VERSION,
                generation_backend: "codex".into(),
                generation_model: "gpt-5.6-terra".into(),
                status: "active".into(),
                ..Default::default()
            })
            .expect("create attributed sequence");
        let touch_id = db
            .insert_touch(&Touch {
                sequence_id: sequence_id.clone(),
                person_id: "person-1".into(),
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                stage: 1,
                channel: "email".into(),
                status: "sent".into(),
                message_id: "<message-1@example.com>".into(),
                ..Default::default()
            })
            .expect("create attributed touch");
        assert_eq!(
            db.touch_by_message_id("outagehub", "<message-1@example.com>")
                .expect("find touch")
                .map(|touch| touch.id),
            Some(touch_id.clone())
        );

        db.record_gtm_outcome(&GtmOutcome {
            brand: "outagehub".into(),
            kind: "positive_reply".into(),
            lead_id,
            person_id: "person-1".into(),
            sequence_id,
            play_id: play.id,
            experiment_id,
            experiment_assignment_id: first.id,
            signal_observation_ids: vec!["signal-1".into()],
            touch_id,
            touch_stage: 1,
            contact_title: "Operations Manager".into(),
            contact_vantage: "process_owner".into(),
            account_hypothesis: "A sourced operating hypothesis".into(),
            play_version: 7,
            experiment_arm: first.arm,
            copy_policy_version: CURRENT_COPY_POLICY_VERSION,
            generation_backend: "codex".into(),
            generation_model: "gpt-5.6-terra".into(),
            fingerprint: "attribution-roundtrip".into(),
            ..Default::default()
        })
        .expect("record attributed outcome");
        let outcome = db
            .list_gtm_outcomes(Some("outagehub"), 1)
            .expect("list outcomes")
            .pop()
            .expect("attributed outcome");
        assert_eq!(outcome.touch_stage, 1);
        assert_eq!(outcome.contact_vantage, "process_owner");
        assert_eq!(outcome.account_hypothesis, "A sourced operating hypothesis");
        assert_eq!(outcome.experiment_arm, second.arm);
        assert_eq!(outcome.generation_backend, "codex");
        assert_eq!(outcome.generation_model, "gpt-5.6-terra");
    }

    #[test]
    fn building_sequences_stream_to_crm_and_promote_without_risking_old_drafts() {
        let db = Db::open(":memory:").expect("open memory db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-checkpoint".into(),
                name: "Checkpoint Operator".into(),
                ..Default::default()
            })
            .expect("insert lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                apollo_person_id: "person-checkpoint".into(),
                name: "Alex Operator".into(),
                email: "alex@example.com".into(),
                email_status: "verified".into(),
                status: "verified".into(),
                ..Default::default()
            })
            .expect("insert person");
        let old_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                status: "active".into(),
                ..Default::default()
            })
            .expect("old active sequence");
        db.insert_touch(&Touch {
            sequence_id: old_id.clone(),
            person_id: person_id.clone(),
            lead_id: lead_id.clone(),
            brand: "outagehub".into(),
            stage: 1,
            channel: "email".into(),
            body: "Old safe draft".into(),
            status: "draft".into(),
            ..Default::default()
        })
        .expect("old touch");

        let building_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                status: "building".into(),
                ..Default::default()
            })
            .expect("building sequence");
        db.insert_touch(&Touch {
            sequence_id: building_id.clone(),
            person_id: person_id.clone(),
            lead_id: lead_id.clone(),
            brand: "outagehub".into(),
            stage: 1,
            channel: "email".into(),
            body: "Writing draft…".into(),
            status: "writing".into(),
            ..Default::default()
        })
        .expect("writing checkpoint");
        let visible = db
            .list_touches_for_person(&person_id)
            .expect("visible checkpoint");
        assert_eq!(visible[0].status, "writing");

        assert!(db
            .update_touch_checkpoint(&Touch {
                sequence_id: building_id.clone(),
                stage: 1,
                channel: "email".into(),
                subject: "Grid context".into(),
                body: "Reviewed new draft".into(),
                status: "draft".into(),
                review_passes: Some(true),
                ..Default::default()
            })
            .expect("update checkpoint"));
        db.promote_building_sequence(&building_id, Some(&old_id), &["principle-1".into()])
            .expect("promote checkpoint");
        assert_eq!(
            db.active_sequence_for_person(&person_id)
                .expect("active sequence"),
            Some(building_id.clone())
        );
        assert!(db
            .sequence_gtm_attribution(&old_id)
            .expect("old lookup")
            .is_none());
        let visible = db
            .list_touches_for_person(&person_id)
            .expect("promoted touch");
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].body, "Reviewed new draft");

        let rejected_person = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                apollo_person_id: "person-rejected-checkpoint".into(),
                name: "Jamie Operator".into(),
                ..Default::default()
            })
            .expect("insert rejected person");
        let rejected_sequence = db
            .create_sequence(&Sequence {
                person_id: rejected_person.clone(),
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                status: "building".into(),
                ..Default::default()
            })
            .expect("rejected building sequence");
        db.insert_touch(&Touch {
            sequence_id: rejected_sequence.clone(),
            person_id: rejected_person.clone(),
            lead_id,
            brand: "outagehub".into(),
            stage: 1,
            status: "writing".into(),
            ..Default::default()
        })
        .expect("rejected writing checkpoint");
        db.reject_building_sequence(&rejected_sequence, "council rejected the copy")
            .expect("reject checkpoint");
        let rejected = db
            .list_touches_for_person(&rejected_person)
            .expect("rejected visible");
        assert_eq!(rejected[0].status, "blocked");
        assert!(rejected[0].error.contains("council rejected"));
        assert_eq!(
            db.latest_rejected_sequence_feedback(&rejected_person)
                .expect("load rewrite feedback"),
            vec!["council rejected the copy".to_string()]
        );
    }

    #[test]
    fn reviewed_sequence_fulfills_only_its_requested_touch_shape() {
        let db = Db::open(":memory:").expect("open memory db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-reviewed-shape".into(),
                name: "Reviewed Shape Account".into(),
                ..Default::default()
            })
            .expect("insert lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                apollo_person_id: "person-reviewed-shape".into(),
                name: "Operations Owner".into(),
                ..Default::default()
            })
            .expect("insert person");
        let sequence_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                status: "active".into(),
                copy_policy_version: CURRENT_COPY_POLICY_VERSION,
                ..Default::default()
            })
            .expect("insert sequence");

        db.insert_touch(&Touch {
            sequence_id: sequence_id.clone(),
            person_id: person_id.clone(),
            lead_id: lead_id.clone(),
            brand: "outagehub".into(),
            stage: 1,
            body: "A reviewed first touch.".into(),
            review_passes: Some(true),
            ..Default::default()
        })
        .expect("insert first touch");
        assert_eq!(
            db.lead_current_reviewed_sequence_count(&lead_id, 1)
                .expect("count one reviewed recipient"),
            1
        );
        assert!(db
            .person_has_current_reviewed_sequence(&person_id, 1)
            .expect("check one reviewed recipient"));
        assert_eq!(
            db.lead_current_reviewed_sequence_count(&lead_id, 4)
                .expect("check incomplete four-touch sequence"),
            0
        );
        assert!(!db
            .person_has_current_reviewed_sequence(&person_id, 4)
            .expect("check incomplete recipient"));

        for stage in 2..=4 {
            db.insert_touch(&Touch {
                sequence_id: sequence_id.clone(),
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "outagehub".into(),
                stage,
                body: format!("Reviewed touch {stage}."),
                review_passes: Some(true),
                ..Default::default()
            })
            .expect("insert reviewed touch");
        }
        assert_eq!(
            db.lead_current_reviewed_sequence_count(&lead_id, 4)
                .expect("check complete four-touch sequence"),
            1
        );
        assert_eq!(
            db.lead_current_reviewed_sequence_count(&lead_id, 7)
                .expect("check incomplete seven-touch sequence"),
            0
        );
    }
}
