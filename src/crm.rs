//! Local CRM: a JSON-backed store plus a small web dashboard.
//!
//! Everything a campaign produces — accounts, the contacts at them (mapped by
//! vantage point), and each contact's outreach sequence with its pre-send
//! critique — is filed here. The store persists to a single JSON file and is
//! shared behind an `Arc<RwLock<_>>` between the web server and the agent.

use std::io::{Read as _, Write as _};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::db::{
    ApplicationBrief, Event, Lead, Mailbox, Opportunity, OpportunityContact, OpportunityTouch,
    Person, SharedDb, Touch,
};
use crate::domain::Campaign;
use crate::metrics::{self, Funnel};

// --- Data model ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Crm {
    pub accounts: Vec<CrmAccount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrmAccount {
    pub id: String,
    pub brand: String,
    pub name: String,
    pub industry: String,
    pub hq: String,
    pub thesis: String,
    pub observed_facts: Vec<String>,
    pub inferences: Vec<String>,
    pub hypothesis: String,
    pub mechanism: String,
    pub consequence_metric: String,
    pub signals: Vec<String>,
    pub system_concept: String,
    pub hard_buyer_question: String,
    pub kill_condition: String,
    #[serde(default)]
    pub magnitude_note: String,
    #[serde(default)]
    pub applied_principles: Vec<String>,
    pub created_at: String,
    pub contacts: Vec<CrmContact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrmContact {
    pub id: String,
    pub name: String,
    pub title: String,
    pub vantage: String,
    pub can_observe: String,
    pub why_them: String,
    #[serde(default)]
    pub primary: bool,
    #[serde(default)]
    pub route_to: String,
    #[serde(default)]
    pub applied_principles: Vec<String>,
    pub touches: Vec<CrmTouch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrmTouch {
    pub stage: u32,
    pub day_offset: u32,
    pub channel: String,
    pub subject: String,
    pub body: String,
    pub purpose: String,
    pub goal: String,
    pub status: StageStatus,
    /// Did the pre-send critic pass the ORIGINAL draft? (None = no critique run.)
    #[serde(default)]
    pub review_passes: Option<bool>,
    #[serde(default)]
    pub review_issues: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StageStatus {
    Pending,
    Sent,
    Skipped,
}

impl StageStatus {
    fn label(self) -> &'static str {
        match self {
            StageStatus::Pending => "pending",
            StageStatus::Sent => "sent",
            StageStatus::Skipped => "skipped",
        }
    }
}

// --- Store -----------------------------------------------------------------

pub struct Store {
    path: PathBuf,
    pub data: Crm,
}

pub type SharedStore = Arc<RwLock<Store>>;

impl Store {
    pub fn load(path: impl Into<PathBuf>) -> Result<Store> {
        let path = path.into();
        let data = if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading CRM store {}", path.display()))?;
            serde_json::from_str(&raw)
                .with_context(|| format!("parsing CRM store {}", path.display()))?
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            Crm::default()
        };
        Ok(Store { path, data })
    }

    pub fn save(&self) -> Result<()> {
        let json = serde_json::to_string_pretty(&self.data)?;
        std::fs::write(&self.path, json)
            .with_context(|| format!("writing CRM store {}", self.path.display()))
    }

    /// Append a finished campaign and persist. Returns (accounts, contacts, touches) added.
    pub fn ingest(&mut self, campaign: Campaign) -> Result<(usize, usize, usize)> {
        let now = Utc::now().to_rfc3339();
        let (mut n_ac, mut n_ct, mut n_to) = (0, 0, 0);

        for plan in campaign.accounts {
            let a = plan.account;
            let contacts: Vec<CrmContact> = plan
                .contacts
                .into_iter()
                .map(|c| {
                    let touches: Vec<CrmTouch> = c
                        .sequence
                        .touches
                        .into_iter()
                        .map(|t| {
                            let review = c.reviews.iter().find(|r| r.stage == t.stage);
                            CrmTouch {
                                stage: t.stage,
                                day_offset: t.day_offset,
                                channel: t.channel,
                                subject: t.subject,
                                body: t.body,
                                purpose: t.purpose,
                                goal: t.goal,
                                status: StageStatus::Pending,
                                review_passes: review.map(|r| r.passes),
                                review_issues: review.map(|r| r.issues.clone()).unwrap_or_default(),
                            }
                        })
                        .collect();
                    n_ct += 1;
                    n_to += touches.len();
                    CrmContact {
                        id: Uuid::new_v4().to_string(),
                        name: c.contact.name,
                        title: c.contact.title,
                        vantage: c.contact.vantage,
                        can_observe: c.contact.can_observe,
                        why_them: c.contact.why_them,
                        primary: c.contact.primary,
                        route_to: c.contact.route_to,
                        applied_principles: c.sequence.applied_principles,
                        touches,
                    }
                })
                .collect();

            n_ac += 1;
            self.data.accounts.push(CrmAccount {
                id: Uuid::new_v4().to_string(),
                brand: campaign.brand.clone(),
                name: a.name,
                industry: a.industry,
                hq: a.hq,
                thesis: campaign.thesis.clone(),
                observed_facts: a.observed_facts,
                inferences: a.inferences,
                hypothesis: a.hypothesis,
                mechanism: a.mechanism,
                consequence_metric: a.consequence_metric,
                signals: a.signals,
                system_concept: a.system_concept,
                hard_buyer_question: a.hard_buyer_question,
                kill_condition: a.kill_condition,
                magnitude_note: a.magnitude_note,
                applied_principles: a.applied_principles,
                created_at: now.clone(),
                contacts,
            });
        }

        self.save()?;
        Ok((n_ac, n_ct, n_to))
    }
}

pub fn open(path: impl AsRef<FsPath>) -> Result<SharedStore> {
    let store = Store::load(path.as_ref().to_path_buf())?;
    Ok(Arc::new(RwLock::new(store)))
}

// --- Web server ------------------------------------------------------------

#[derive(Clone)]
struct WebState {
    store: SharedStore,
    db: SharedDb,
}

#[derive(Debug, Serialize)]
struct ExecutionDashboard {
    funnel: Funnel,
    accounts: Vec<ExecutionAccount>,
    opportunities: Vec<ExecutionOpportunity>,
    mailboxes: Vec<PublicMailbox>,
    replies: Vec<crate::db::Reply>,
    events: Vec<Event>,
}

#[derive(Debug, Serialize)]
struct ExecutionOpportunity {
    opportunity: Opportunity,
    contacts: Vec<ExecutionOpportunityContact>,
    application: Option<ApplicationBrief>,
}

#[derive(Debug, Serialize)]
struct ExecutionOpportunityContact {
    contact: OpportunityContact,
    touches: Vec<OpportunityTouch>,
}

#[derive(Debug, Serialize)]
struct ExecutionAccount {
    lead: Lead,
    people: Vec<ExecutionPerson>,
}

#[derive(Debug, Serialize)]
struct ExecutionPerson {
    person: Person,
    touches: Vec<Touch>,
    applied_principles: Vec<String>,
}

/// Deliberately excludes SMTP/IMAP credentials from the dashboard/API.
#[derive(Debug, Serialize)]
struct PublicMailbox {
    brand: String,
    from_name: String,
    from_email: String,
    daily_cap: i64,
    sent_today: i64,
    active: bool,
}

impl From<Mailbox> for PublicMailbox {
    fn from(m: Mailbox) -> Self {
        Self {
            brand: m.brand,
            from_name: m.from_name,
            from_email: m.from_email,
            daily_cap: m.daily_cap,
            sent_today: m.sent_today,
            active: m.active,
        }
    }
}

fn execution_dashboard(db: &SharedDb) -> Result<ExecutionDashboard> {
    let people = db.list_people(None, None)?;
    let mut accounts = Vec::new();
    for lead in db.list_leads(None)? {
        let mut account_people = Vec::new();
        for person in people.iter().filter(|p| p.lead_id == lead.id) {
            account_people.push(ExecutionPerson {
                person: person.clone(),
                touches: db.list_touches_for_person(&person.id)?,
                applied_principles: db.active_sequence_principles_for_person(&person.id)?,
            });
        }
        accounts.push(ExecutionAccount {
            lead,
            people: account_people,
        });
    }

    let mut opportunities = Vec::new();
    for opportunity in db.list_opportunities(None, None)? {
        let mut contacts = Vec::new();
        for contact in db.list_opportunity_contacts(&opportunity.id)? {
            contacts.push(ExecutionOpportunityContact {
                touches: db.list_opportunity_touches(&contact.id)?,
                contact,
            });
        }
        opportunities.push(ExecutionOpportunity {
            application: db.get_application_brief(&opportunity.id)?,
            opportunity,
            contacts,
        });
    }

    Ok(ExecutionDashboard {
        funnel: metrics::funnel(db, None)?,
        accounts,
        opportunities,
        mailboxes: db
            .list_mailboxes(None)?
            .into_iter()
            .map(Into::into)
            .collect(),
        replies: db.list_replies(100)?,
        events: db.recent_events(None, 40)?,
    })
}

pub fn router(store: SharedStore, db: SharedDb) -> Router {
    let state = WebState { store, db };
    Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/crm", get(api))
        .route("/api/execution", get(execution_api))
        .route("/stage/:contact/:stage/:status", post(set_stage))
        .route("/execution/approve/:person", post(approve_execution))
        .route("/execution/touch/:touch/done", post(mark_touch_done))
        .route(
            "/opportunities/approve/:contact",
            post(approve_opportunity_outreach),
        )
        .with_state(state)
}

pub async fn serve(store: SharedStore, db: SharedDb, port: u16) -> Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = std::net::TcpListener::bind(addr)
        .with_context(|| format!("binding CRM server to http://{addr}"))?;
    serve_on_listener(store, db, listener).await
}

/// How many loopback ports to scan upward from the preferred one before falling
/// back to an OS-assigned port.
const CRM_PORT_SCAN: u16 = 128;

/// Loopback ports to try for the CRM, starting at `first`.
pub fn port_candidates(first: u16) -> Vec<u16> {
    (0..CRM_PORT_SCAN)
        .filter_map(|offset| first.checked_add(offset))
        .collect()
}

/// Bind the first free loopback port at or above `first`, else an OS-assigned
/// one. The returned listener is ready to hand to [`serve_on_listener`]. Binding
/// (rather than check-then-bind) is race-free across concurrent sessions.
pub fn bind_free_listener(first: u16) -> Result<TcpListener> {
    for port in port_candidates(first) {
        let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
        if let Ok(listener) = TcpListener::bind(address) {
            return Ok(listener);
        }
    }
    TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .context("finding a free localhost port for the CRM")
}

/// True when a Spruce Leaf CRM is already answering on this loopback port. A
/// blocking, sub-second probe — safe at startup or from within `spawn_blocking`.
/// This is the single source of truth for "is our CRM actually up?", used both
/// to reuse a sibling session's server and to detect a link that has gone dead.
pub fn is_live(port: u16) -> bool {
    http_probe(port, "/api/health")
        .is_some_and(|response| response.contains("\"app\":\"spruce-leaf\""))
}

fn http_probe(port: u16, path: &str) -> Option<String> {
    let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port).into();
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(50)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(150)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_millis(150)))
        .ok()?;
    write!(
        stream,
        "GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut bytes = Vec::new();
    let _ = stream.take(8192).read_to_end(&mut bytes);
    (!bytes.is_empty()).then(|| String::from_utf8_lossy(&bytes).into_owned())
}

pub async fn serve_on_listener(
    store: SharedStore,
    db: SharedDb,
    listener: std::net::TcpListener,
) -> Result<()> {
    let addr = listener
        .local_addr()
        .context("reading CRM listener address")?;
    listener
        .set_nonblocking(true)
        .context("putting CRM listener in non-blocking mode")?;
    let listener = tokio::net::TcpListener::from_std(listener)
        .with_context(|| format!("starting CRM listener at http://{addr}"))?;
    axum::serve(listener, router(store, db))
        .await
        .context("CRM web server crashed")
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "app": "spruce-leaf",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn index(State(state): State<WebState>) -> Html<String> {
    let crm = state.store.read().await.data.clone();
    let execution = execution_dashboard(&state.db);
    Html(render_html(&crm, execution.as_ref().ok()))
}

async fn api(State(state): State<WebState>) -> Json<Crm> {
    let s = state.store.read().await;
    Json(s.data.clone())
}

async fn execution_api(State(state): State<WebState>) -> impl IntoResponse {
    match execution_dashboard(&state.db) {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read execution db: {e:#}"),
        )
            .into_response(),
    }
}

async fn set_stage(
    State(state): State<WebState>,
    Path((contact, stage, status)): Path<(String, u32, String)>,
) -> impl IntoResponse {
    let new = match status.as_str() {
        "sent" => StageStatus::Sent,
        "skipped" => StageStatus::Skipped,
        _ => StageStatus::Pending,
    };
    {
        let mut s = state.store.write().await;
        for ac in &mut s.data.accounts {
            for c in &mut ac.contacts {
                if c.id == contact {
                    for t in &mut c.touches {
                        if t.stage == stage {
                            t.status = new;
                        }
                    }
                }
            }
        }
        let _ = s.save();
    }
    Redirect::to("/")
}

async fn approve_execution(
    State(state): State<WebState>,
    Path(person_id): Path<String>,
) -> impl IntoResponse {
    if let Ok(Some(person)) = state.db.get_person(&person_id) {
        let _ = state
            .db
            .approve_touches(Some(&person.brand), Some(&person_id));
    }
    Redirect::to("/#execution")
}

async fn mark_touch_done(
    State(state): State<WebState>,
    Path(touch_id): Path<String>,
) -> impl IntoResponse {
    let _ = state
        .db
        .set_touch_status(&touch_id, "sent", "", "", "manually completed");
    Redirect::to("/#execution")
}

async fn approve_opportunity_outreach(
    State(state): State<WebState>,
    Path(contact_id): Path<String>,
) -> impl IntoResponse {
    if let Ok(Some(contact)) = state.db.get_opportunity_contact(&contact_id) {
        let _ = state
            .db
            .approve_opportunity_touches(Some(&contact.brand), Some(&contact_id));
    }
    Redirect::to("/#opportunities")
}

// --- HTML rendering --------------------------------------------------------

fn render_html(crm: &Crm, execution: Option<&ExecutionDashboard>) -> String {
    let mut b = String::new();
    b.push_str("<!doctype html><html><head><meta charset=\"utf-8\">");
    b.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    b.push_str("<title>sales-os CRM</title>");
    b.push_str(SHEET_STYLE);
    b.push_str("</head><body><div class=\"crm-shell\">");

    let live_accounts = execution
        .map(|dashboard| dashboard.accounts.as_slice())
        .unwrap_or_default();
    let use_live = !live_accounts.is_empty();
    let account_count = if use_live {
        live_accounts.len()
    } else {
        crm.accounts.len()
    };
    let contact_count = if use_live {
        live_accounts
            .iter()
            .map(|account| account.people.len().min(5))
            .sum::<usize>()
    } else {
        crm.accounts
            .iter()
            .map(|account| account.contacts.len().min(5))
            .sum::<usize>()
    };
    let scheduled_count = if use_live {
        live_accounts
            .iter()
            .flat_map(|account| &account.people)
            .flat_map(|entry| &entry.touches)
            .filter(|touch| !touch.due_at.is_empty())
            .count()
    } else {
        0
    };

    b.push_str(&format!(
        "<header class=\"topbar\"><div class=\"brand-lockup\"><span class=\"mark\">↗</span>\
         <div><h1>Sales CRM</h1><p>Five people per company · seven scheduled touches per person</p></div></div>\
         <div class=\"top-stats\"><span><strong>{account_count}</strong> companies</span>\
         <span><strong>{contact_count}</strong> people</span><span><strong>{scheduled_count}</strong> scheduled</span></div></header>"
    ));
    b.push_str("<main class=\"sheet-scroll\">");

    if use_live {
        render_people_sheet(&mut b, live_accounts);
    } else if !crm.accounts.is_empty() {
        render_research_sheet(&mut b, &crm.accounts);
    } else {
        render_empty_sheet(&mut b);
    }

    b.push_str("</main></div></body></html>");
    b
}

#[allow(dead_code)]
fn render_execution(b: &mut String, dashboard: &ExecutionDashboard) {
    let f = &dashboard.funnel;
    b.push_str("<section id=\"execution\" class=\"execution\">");
    b.push_str("<div class=\"section-title\"><h2>Real execution</h2><p>Apollo identities, verified delivery state, scheduled touches, and replies.</p></div>");
    b.push_str("<div class=\"funnel\">");
    for (label, value) in [
        ("qualified leads", f.leads.to_string()),
        ("people sourced", f.people.to_string()),
        ("verified", f.verified.to_string()),
        ("contacted", f.contacted.to_string()),
        ("touches sent", f.touches_sent.to_string()),
        ("replied", format!("{} · {:.0}%", f.replied, f.reply_rate())),
        ("unsubscribed", f.unsubscribed.to_string()),
        ("bounced", f.bounced.to_string()),
    ] {
        b.push_str(&format!(
            "<div class=\"metric-card\"><strong>{}</strong><span>{}</span></div>",
            esc(&value),
            label
        ));
    }
    b.push_str("</div>");

    if !dashboard.mailboxes.is_empty() {
        b.push_str("<div class=\"mailboxes\"><h3>Mailboxes</h3>");
        for mailbox in &dashboard.mailboxes {
            b.push_str(&format!(
                "<div class=\"mailbox\"><span class=\"brand {brand}\">{brand}</span>\
                 <strong>{name} &lt;{email}&gt;</strong><span>{sent}/{cap} sent today · {state}</span></div>",
                brand = esc(&mailbox.brand),
                name = esc(&mailbox.from_name),
                email = esc(&mailbox.from_email),
                sent = mailbox.sent_today,
                cap = mailbox.daily_cap,
                state = if mailbox.active { "active" } else { "paused" },
            ));
        }
        b.push_str("</div>");
    }

    render_people_sheet(b, &dashboard.accounts);

    if !dashboard.replies.is_empty() {
        b.push_str("<article class=\"account execution-account\"><h2>Replies</h2>");
        for r in &dashboard.replies {
            b.push_str(&format!(
                "<div class=\"stage execution-touch\"><div class=\"stagehead\">{from} · {cls}\
                 <span class=\"status\">{ts}</span></div>",
                from = esc(&r.from_email),
                cls = esc(&r.classification),
                ts = esc(&r.ts),
            ));
            if !r.subject.is_empty() {
                b.push_str(&format!(
                    "<div class=\"subject\">Subject: {}</div>",
                    esc(&r.subject)
                ));
            }
            b.push_str(&format!(
                "<div class=\"body\">{}</div>",
                esc_multiline(&r.body.chars().take(600).collect::<String>())
            ));
            if !r.action_taken.is_empty() {
                b.push_str(&format!(
                    "<div class=\"goal\">action: {}</div>",
                    esc(&r.action_taken)
                ));
            }
            b.push_str("</div>");
        }
        b.push_str("</article>");
    }

    render_opportunity_pipeline(b, &dashboard.opportunities);

    if dashboard.accounts.is_empty() {
        b.push_str(
            "<div class=\"empty\">No real leads yet. Start with <code>spruce-leaf source \"&lt;thesis&gt;\"</code>.</div>",
        );
    }

    for account in &dashboard.accounts {
        let lead = &account.lead;
        b.push_str("<article class=\"account execution-account\">");
        b.push_str(&format!(
            "<h2><span class=\"brand {brand}\">{brand}</span> {name}</h2>\
             <p class=\"meta\">{industry} · {hq} · {domain} · {headcount} employees</p>\
             <p class=\"hyp\"><strong>Hypothesis:</strong> {hypothesis}</p>\
             <p class=\"metric\"><em>Measure:</em> {measure}</p>",
            brand = esc(&lead.brand),
            name = esc(&lead.name),
            industry = esc(&lead.industry),
            hq = esc(&lead.hq),
            domain = esc(&lead.domain),
            headcount = lead.headcount,
            hypothesis = esc(&lead.hypothesis),
            measure = esc(&lead.consequence_metric),
        ));

        for entry in &account.people {
            let person = &entry.person;
            let sent = entry.touches.iter().filter(|t| t.status == "sent").count();
            let email_drafts = entry
                .touches
                .iter()
                .filter(|t| {
                    t.status == "draft"
                        && t.channel.eq_ignore_ascii_case("email")
                        && t.review_passes == Some(true)
                })
                .count();
            let star = if person.primary { " ★" } else { "" };
            b.push_str("<details class=\"contact execution-contact\">");
            b.push_str(&format!(
                "<summary><span class=\"name\">{}{}</span> — {} \
                 <span class=\"pill\">{}</span><span class=\"state-pill\">{}</span>\
                 <span class=\"prog\">{}/{} sent</span></summary>",
                esc(&person.name),
                star,
                esc(&person.title),
                esc(&vantage_label(&person.vantage)),
                esc(&person.status),
                sent,
                entry.touches.len(),
            ));
            b.push_str(&format!(
                "<p class=\"role\"><em>Contact:</em> {} <span class=\"email-state\">{}</span>{}</p>\
                 <p class=\"role\"><em>Why them:</em> {}</p>\
                 <p class=\"role\"><em>Can observe:</em> {}</p>",
                if person.email.is_empty() {
                    "email not enriched".to_string()
                } else {
                    esc(&person.email)
                },
                esc(&person.email_status),
                if person.phone.is_empty() {
                    String::new()
                } else {
                    format!(" · {}", esc(&person.phone))
                },
                esc(&person.why_them),
                esc(&person.can_observe),
            ));
            if !entry.applied_principles.is_empty() {
                b.push_str(&format!(
                    "<p class=\"role knowledge-cites\"><em>Business knowledge applied:</em> {}</p>",
                    esc(&entry.applied_principles.join(", "))
                ));
            }
            if email_drafts > 0 {
                b.push_str(&format!(
                    "<form class=\"approve-form\" method=\"post\" action=\"/execution/approve/{}\">\
                     <button class=\"btn sent\">Approve {email_drafts} email draft(s)</button></form>",
                    esc(&person.id),
                ));
            }

            for touch in &entry.touches {
                b.push_str(&format!(
                    "<div class=\"stage execution-touch\"><div class=\"stagehead\">Touch {} · day {} · {} · {}\
                     <span class=\"status\">{}</span></div>",
                    touch.stage,
                    touch.day_offset,
                    esc(&touch.channel),
                    esc(&touch.purpose),
                    esc(&touch.status),
                ));
                if !touch.subject.is_empty() {
                    b.push_str(&format!(
                        "<div class=\"subject\">Subject: {}</div>",
                        esc(&touch.subject)
                    ));
                }
                b.push_str(&format!(
                    "<div class=\"body\">{}</div><div class=\"goal\">Due: {}{}{}{}</div>",
                    esc_multiline(&touch.body),
                    esc(&touch.due_at),
                    if touch.recipient_timezone.is_empty() {
                        String::new()
                    } else {
                        format!(" · recipient {}", esc(&touch.recipient_timezone))
                    },
                    if touch.scheduled_rule.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " · rule {} ({})",
                            esc(&touch.scheduled_rule),
                            esc(&touch.schedule_reason)
                        )
                    },
                    if touch.error.is_empty() {
                        String::new()
                    } else {
                        format!(" · error: {}", esc(&touch.error))
                    }
                ));
                if !touch.review_issues.is_empty() {
                    b.push_str(&format!(
                        "<div class=\"review {}\">QA: {}</div>",
                        if touch.review_passes == Some(true) {
                            "ok"
                        } else {
                            "edited"
                        },
                        esc(&touch.review_issues.join(" · ")),
                    ));
                }
                // Manual LinkedIn/call tasks never flow through the send daemon,
                // so give them a one-click "done" that persists to the db.
                if !touch.channel.eq_ignore_ascii_case("email")
                    && touch.status == "draft"
                    && touch.review_passes == Some(true)
                {
                    b.push_str(&format!(
                        "<form class=\"approve-form\" method=\"post\" action=\"/execution/touch/{}/done\">\
                         <button class=\"btn sent\">Mark {} done</button></form>",
                        esc(&touch.id),
                        esc(&touch.channel),
                    ));
                }
                b.push_str("</div>");
            }
            b.push_str("</details>");
        }
        b.push_str("</article>");
    }

    if !dashboard.events.is_empty() {
        b.push_str(
            "<details class=\"activity\"><summary><strong>Recent activity</strong></summary><ol>",
        );
        for event in &dashboard.events {
            b.push_str(&format!(
                "<li><time>{}</time><span class=\"brand {}\">{}</span><strong>{}</strong> {}</li>",
                esc(&event.ts),
                esc(&event.brand),
                esc(&event.brand),
                esc(&event.kind),
                esc(&event.detail),
            ));
        }
        b.push_str("</ol></details>");
    }
    b.push_str("</section>");
}

fn render_people_sheet(b: &mut String, accounts: &[ExecutionAccount]) {
    render_sheet_head(b);

    for (account_index, account) in accounts.iter().enumerate() {
        let mut people = account.people.iter().collect::<Vec<_>>();
        people.sort_by_key(|entry| {
            (
                !entry.person.primary,
                entry.person.email_status != "verified",
                entry.person.status == "suppressed",
            )
        });
        let people = people.into_iter().take(5).collect::<Vec<_>>();

        for slot in 0..5 {
            let entry = people.get(slot).copied();
            let stripe = if account_index % 2 == 1 { " alt" } else { "" };
            let start = if slot == 0 { " account-start" } else { "" };
            b.push_str(&format!("<tr class=\"contact-row{stripe}{start}\">"));

            if slot == 0 {
                render_company_cell(b, &account.lead, stripe);
                render_lead_context_cell(b, &account.lead, stripe);
            }

            if let Some(entry) = entry {
                render_person_cell(b, &entry.person);
                render_why_cell(b, &entry.person.why_them, &entry.person.can_observe);
                for stage in 1..=7 {
                    match entry.touches.iter().find(|touch| touch.stage == stage) {
                        Some(touch) => render_touch_cell(b, touch),
                        None => render_missing_touch(b, stage),
                    }
                }
            } else {
                render_empty_contact_row(b, slot + 1);
            }
            b.push_str("</tr>");
        }
    }

    b.push_str("</tbody></table>");
}

fn render_sheet_head(b: &mut String) {
    b.push_str(
        "<table class=\"crm-sheet\"><colgroup><col class=\"c-company\"><col class=\"c-context\">\
         <col class=\"c-person\"><col class=\"c-why\"><col class=\"c-touch\"><col class=\"c-touch\">\
         <col class=\"c-touch\"><col class=\"c-touch\"><col class=\"c-touch\"><col class=\"c-touch\">\
         <col class=\"c-touch\"></colgroup><thead><tr><th class=\"pin\">Company</th>\
         <th>Company context</th><th>Name</th><th>Why they'd answer</th>\
         <th>T1</th><th>T2</th><th>T3</th><th>T4</th><th>T5</th><th>T6</th><th>T7</th>\
         </tr></thead><tbody>",
    );
}

fn render_company_cell(b: &mut String, lead: &Lead, stripe: &str) {
    let details = [lead.industry.as_str(), lead.hq.as_str()]
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" · ");
    let domain = if lead.domain.trim().is_empty() {
        String::new()
    } else {
        format!(
            "<a href=\"https://{}\" target=\"_blank\" rel=\"noreferrer\">{} ↗</a>",
            esc(&lead.domain),
            esc(&lead.domain)
        )
    };
    b.push_str(&format!(
        "<td class=\"company pin{stripe}\" rowspan=\"5\"><span class=\"brand-tag {brand}\">{brand}</span>\
         <strong>{name}</strong><small>{details}</small>{domain}</td>",
        brand = esc(&lead.brand),
        name = esc(&lead.name),
        details = esc(&details),
    ));
}

fn render_lead_context_cell(b: &mut String, lead: &Lead, stripe: &str) {
    let hypothesis = first_non_empty(&[&lead.hypothesis, &lead.thesis]);
    let observed = lead
        .observed_facts
        .first()
        .map(String::as_str)
        .unwrap_or("");
    let signal = lead.signals.first().map(String::as_str).unwrap_or("");
    b.push_str(&format!(
        "<td class=\"context{stripe}\" rowspan=\"5\">{hypothesis}{observed}{signal}{measure}{mechanism}</td>",
        hypothesis = context_line("Hypothesis", hypothesis),
        observed = context_line("Observed", observed),
        signal = context_line("Signal", signal),
        measure = context_line("Measure", &lead.consequence_metric),
        mechanism = context_line("How", &lead.mechanism),
    ));
}

fn render_person_cell(b: &mut String, person: &Person) {
    let email = if person.email.trim().is_empty() {
        "<span class=\"muted\">Email not found</span>".to_string()
    } else {
        format!(
            "<a class=\"email\" href=\"mailto:{}\">{}</a>",
            esc(&person.email),
            esc(&person.email)
        )
    };
    b.push_str(&format!(
        "<td class=\"person\"><strong>{name}{primary}</strong><small>{title}</small>{email}\
         <span class=\"person-status {status}\">{status}</span></td>",
        name = esc(&person.name),
        primary = if person.primary { " ★" } else { "" },
        title = esc(&person.title),
        status = esc(&person.status),
    ));
}

fn render_why_cell(b: &mut String, why: &str, can_observe: &str) {
    let why = if why.trim().is_empty() {
        "Reply rationale not written yet"
    } else {
        why
    };
    let observation = if can_observe.trim().is_empty() {
        String::new()
    } else {
        format!(
            "<small><b>Can see:</b> {}</small>",
            esc(&preview(can_observe, 120))
        )
    };
    b.push_str(&format!(
        "<td class=\"why\"><p>{}</p>{observation}</td>",
        esc(&preview(why, 190))
    ));
}

fn render_touch_cell(b: &mut String, touch: &Touch) {
    let state = if touch.review_passes == Some(false) {
        "blocked"
    } else {
        touch.status.as_str()
    };
    let copy = if touch.subject.trim().is_empty() {
        preview(&touch.body, 150)
    } else {
        preview(&format!("{} — {}", touch.subject, touch.body), 150)
    };
    let due = display_due(&touch.due_at, &touch.recipient_timezone, touch.day_offset);
    b.push_str(&format!(
        "<td class=\"touch {state}\"><details><summary><span class=\"touch-tag\">{channel} · T{stage}</span>\
         <time>{due}</time><p>{copy}</p></summary><div class=\"touch-full\"><span class=\"touch-state\">{state}</span>\
         {subject}<div class=\"message\">{body}</div>{purpose}{goal}{qa}</div></details></td>",
        state = esc(state),
        channel = esc(&touch.channel),
        stage = touch.stage,
        due = esc(&due),
        copy = esc(&copy),
        subject = if touch.subject.trim().is_empty() {
            String::new()
        } else {
            format!("<strong class=\"subject\">{}</strong>", esc(&touch.subject))
        },
        body = esc_multiline(&touch.body),
        purpose = detail_line("Purpose", &touch.purpose),
        goal = detail_line("Goal", &touch.goal),
        qa = if touch.review_issues.is_empty() {
            String::new()
        } else {
            detail_line("QA", &touch.review_issues.join(" · "))
        },
    ));
}

fn render_missing_touch(b: &mut String, stage: i64) {
    b.push_str(&format!(
        "<td class=\"touch missing\"><span class=\"touch-tag\">T{stage}</span><p>Not written</p></td>"
    ));
}

fn render_empty_contact_row(b: &mut String, slot: usize) {
    b.push_str(&format!(
        "<td class=\"person empty-person\"><strong>Contact {slot}</strong><small>Not sourced yet</small></td>\
         <td class=\"why muted\">Add a person and their reason to reply</td>"
    ));
    for stage in 1..=7 {
        render_missing_touch(b, stage);
    }
}

fn render_research_sheet(b: &mut String, accounts: &[CrmAccount]) {
    render_sheet_head(b);
    for (account_index, account) in accounts.iter().enumerate() {
        let contacts = account.contacts.iter().take(5).collect::<Vec<_>>();
        for slot in 0..5 {
            let stripe = if account_index % 2 == 1 { " alt" } else { "" };
            let start = if slot == 0 { " account-start" } else { "" };
            b.push_str(&format!("<tr class=\"contact-row{stripe}{start}\">"));
            if slot == 0 {
                let details = [account.industry.as_str(), account.hq.as_str()]
                    .into_iter()
                    .filter(|value| !value.trim().is_empty())
                    .collect::<Vec<_>>()
                    .join(" · ");
                b.push_str(&format!(
                    "<td class=\"company pin{stripe}\" rowspan=\"5\"><span class=\"brand-tag {brand}\">{brand}</span>\
                     <strong>{name}</strong><small>{details}</small></td>",
                    brand = esc(&account.brand),
                    name = esc(&account.name),
                    details = esc(&details),
                ));
                let observed = account
                    .observed_facts
                    .first()
                    .map(String::as_str)
                    .unwrap_or("");
                let signal = account.signals.first().map(String::as_str).unwrap_or("");
                b.push_str(&format!(
                    "<td class=\"context{stripe}\" rowspan=\"5\">{hypothesis}{observed}{signal}{measure}{mechanism}</td>",
                    hypothesis = context_line("Hypothesis", &account.hypothesis),
                    observed = context_line("Observed", observed),
                    signal = context_line("Signal", signal),
                    measure = context_line("Measure", &account.consequence_metric),
                    mechanism = context_line("How", &account.mechanism),
                ));
            }
            if let Some(contact) = contacts.get(slot).copied() {
                b.push_str(&format!(
                    "<td class=\"person\"><strong>{name}{primary}</strong><small>{title}</small>\
                     <span class=\"person-status\">research</span></td>",
                    name = esc(&contact.name),
                    primary = if contact.primary { " ★" } else { "" },
                    title = esc(&contact.title),
                ));
                render_why_cell(b, &contact.why_them, &contact.can_observe);
                for stage in 1..=7 {
                    if let Some(touch) = contact.touches.iter().find(|touch| touch.stage == stage) {
                        render_research_touch(b, touch);
                    } else {
                        render_missing_touch(b, stage as i64);
                    }
                }
            } else {
                render_empty_contact_row(b, slot + 1);
            }
            b.push_str("</tr>");
        }
    }
    b.push_str("</tbody></table>");
}

fn render_research_touch(b: &mut String, touch: &CrmTouch) {
    let copy = if touch.subject.trim().is_empty() {
        preview(&touch.body, 150)
    } else {
        preview(&format!("{} — {}", touch.subject, touch.body), 150)
    };
    b.push_str(&format!(
        "<td class=\"touch {state}\"><details><summary><span class=\"touch-tag\">{channel} · T{stage}</span>\
         <time>Day {day} · time not scheduled</time><p>{copy}</p></summary><div class=\"touch-full\">\
         {subject}<div class=\"message\">{body}</div>{purpose}{goal}</div></details></td>",
        state = touch.status.label(),
        channel = esc(&touch.channel),
        stage = touch.stage,
        day = touch.day_offset,
        copy = esc(&copy),
        subject = if touch.subject.trim().is_empty() {
            String::new()
        } else {
            format!("<strong class=\"subject\">{}</strong>", esc(&touch.subject))
        },
        body = esc_multiline(&touch.body),
        purpose = detail_line("Purpose", &touch.purpose),
        goal = detail_line("Goal", &touch.goal),
    ));
}

fn render_empty_sheet(b: &mut String) {
    render_sheet_head(b);
    b.push_str(
        "<tr><td class=\"empty-sheet\" colspan=\"11\"><strong>No companies yet</strong>\
         <span>Source a campaign and the company, five contacts, reply rationale, and T1–T7 schedule will appear here.</span></td></tr>\
         </tbody></table>",
    );
}

fn context_line(label: &str, value: &str) -> String {
    if value.trim().is_empty() {
        String::new()
    } else {
        format!(
            "<div class=\"context-line\"><b>{}</b><p>{}</p></div>",
            esc(label),
            esc(&preview(value, 260))
        )
    }
}

fn detail_line(label: &str, value: &str) -> String {
    if value.trim().is_empty() {
        String::new()
    } else {
        format!("<small><b>{}:</b> {}</small>", esc(label), esc(value))
    }
}

fn first_non_empty<'a>(values: &[&'a str]) -> &'a str {
    values
        .iter()
        .copied()
        .find(|value| !value.trim().is_empty())
        .unwrap_or("")
}

fn preview(value: &str, limit: usize) -> String {
    let clean = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if clean.chars().count() <= limit {
        clean
    } else {
        format!(
            "{}…",
            clean
                .chars()
                .take(limit.saturating_sub(1))
                .collect::<String>()
        )
    }
}

fn display_due(due_at: &str, timezone: &str, day_offset: i64) -> String {
    if due_at.trim().is_empty() {
        return format!("Day {day_offset} · not scheduled");
    }
    let Ok(due) = DateTime::parse_from_rfc3339(due_at) else {
        return due_at.to_string();
    };
    if let Ok(timezone) = timezone.parse::<Tz>() {
        due.with_timezone(&timezone)
            .format("%a %-d %b · %-I:%M %p %Z")
            .to_string()
    } else {
        due.format("%a %-d %b · %-I:%M %p %:z").to_string()
    }
}

#[allow(dead_code)]
fn render_opportunity_pipeline(b: &mut String, opportunities: &[ExecutionOpportunity]) {
    b.push_str(
        "<div class=\"section-title\" id=\"opportunities\"><h2>Opportunity pipeline</h2>\
         <p>Official evidence, conservative fit, funder contacts, outreach, and application work.</p></div>",
    );
    if opportunities.is_empty() {
        b.push_str(
            "<div class=\"empty\">No opportunities yet. Run <code>spruce-leaf --brand outagehub discover-opportunities</code>.</div>",
        );
        return;
    }

    for entry in opportunities {
        let opportunity = &entry.opportunity;
        b.push_str("<article class=\"account opportunity-card\">");
        b.push_str(&format!(
            "<h2><span class=\"brand {brand}\">{brand}</span>{title}</h2>\
             <p class=\"meta\">{kind} · {status} · pipeline {pipeline} · fit {score}/100 ({fit})</p>\
             <p class=\"hyp\"><strong>{funder}</strong> — {summary}</p>\
             <p class=\"metric\"><em>Deadline:</em> {deadline} · <em>Amount:</em> {min}–{max} {currency}</p>\
             <p class=\"metric\"><a href=\"{url}\" rel=\"noreferrer\">Official source</a></p>\
             <p class=\"concept\"><em>Next action:</em> {next}</p>",
            brand = esc(&opportunity.brand),
            title = esc(&opportunity.title),
            kind = esc(&opportunity.funding_type),
            status = esc(&opportunity.opportunity_status),
            pipeline = esc(&opportunity.pipeline_status),
            score = opportunity.fit_score,
            fit = esc(&opportunity.fit_status),
            funder = esc(&opportunity.funder),
            summary = esc(&opportunity.summary),
            deadline = if opportunity.deadline.is_empty() {
                "not verified".to_string()
            } else {
                esc(&opportunity.deadline)
            },
            min = esc(&opportunity.amount_min),
            max = esc(&opportunity.amount_max),
            currency = esc(&opportunity.currency),
            url = esc(&opportunity.canonical_url),
            next = esc(&opportunity.next_action),
        ));
        list_block(b, "Fit evidence", &opportunity.fit_reasons, "facts");
        list_block(b, "Blockers", &opportunity.blockers, "guesses");
        list_block(b, "Unknowns to verify", &opportunity.unknowns, "signals");

        for contact_entry in &entry.contacts {
            let contact = &contact_entry.contact;
            let drafts = contact_entry
                .touches
                .iter()
                .filter(|touch| touch.status == "draft")
                .count();
            b.push_str("<details class=\"contact execution-contact\">");
            b.push_str(&format!(
                "<summary><span class=\"name\">{name}</span> — {title}\
                 <span class=\"pill\">{source}</span><span class=\"state-pill\">{state}</span></summary>\
                 <p class=\"role\"><em>Contact:</em> {email} ({email_status})</p>\
                 <p class=\"role\"><em>Why them:</em> {why}</p>",
                name = if contact.name.is_empty() {
                    "Programme contact".to_string()
                } else {
                    esc(&contact.name)
                },
                title = esc(&contact.title),
                source = esc(&contact.source),
                state = esc(&contact.status),
                email = if contact.email.is_empty() {
                    "email not enriched".to_string()
                } else {
                    esc(&contact.email)
                },
                email_status = esc(&contact.email_status),
                why = esc(&contact.why_them),
            ));
            if drafts > 0 {
                b.push_str(&format!(
                    "<form class=\"approve-form\" method=\"post\" action=\"/opportunities/approve/{id}\">\
                     <button class=\"btn sent\">Approve {drafts} funding draft(s)</button></form>",
                    id = esc(&contact.id),
                ));
            }
            for touch in &contact_entry.touches {
                b.push_str(&format!(
                    "<div class=\"stage execution-touch\"><div class=\"stagehead\">Touch {stage} · day {day} · {purpose}\
                     <span class=\"status\">{status}</span></div>\
                     <div class=\"subject\">Subject: {subject}</div>\
                     <div class=\"body\">{body}</div><div class=\"goal\">Goal: {goal} · Due: {due}{timezone}{rule}</div></div>",
                    stage = touch.stage,
                    day = touch.day_offset,
                    purpose = esc(&touch.purpose),
                    status = esc(&touch.status),
                    subject = esc(&touch.subject),
                    body = esc_multiline(&touch.body),
                    goal = esc(&touch.goal),
                    due = esc(&touch.due_at),
                    timezone = if touch.recipient_timezone.is_empty() {
                        String::new()
                    } else {
                        format!(" · recipient {}", esc(&touch.recipient_timezone))
                    },
                    rule = if touch.scheduled_rule.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " · rule {} ({})",
                            esc(&touch.scheduled_rule),
                            esc(&touch.schedule_reason)
                        )
                    },
                ));
            }
            b.push_str("</details>");
        }
        if let Some(application) = &entry.application {
            b.push_str(
                "<details class=\"contact\"><summary><strong>Application brief</strong></summary>",
            );
            b.push_str(&format!(
                "<p class=\"role\"><em>Eligibility:</em> {}</p><p class=\"role\"><em>Project:</em> {}</p>",
                esc(&application.eligibility_summary),
                esc(&application.project_shape),
            ));
            list_block(
                b,
                "Evidence needed",
                &application.evidence_needed,
                "signals",
            );
            list_block(b, "Next steps", &application.next_steps, "facts");
            b.push_str("</details>");
        }
        b.push_str("</article>");
    }
}

#[allow(dead_code)]
fn list_block(b: &mut String, title: &str, items: &[String], class: &str) {
    if items.is_empty() {
        return;
    }
    b.push_str(&format!(
        "<p class=\"lblhead\">{title}</p><ul class=\"{class}\">"
    ));
    for it in items {
        b.push_str(&format!("<li>{}</li>", esc(it)));
    }
    b.push_str("</ul>");
}

#[allow(dead_code)]
fn vantage_label(v: &str) -> String {
    v.replace('_', " ")
}

#[allow(dead_code)]
fn stage_button(contact: &str, stage: u32, status: &str, label: &str) -> String {
    format!(
        "<form method=\"post\" action=\"/stage/{contact}/{stage}/{status}\">\
         <button class=\"btn {status}\">{label}</button></form>"
    )
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn esc_multiline(s: &str) -> String {
    esc(s).replace('\n', "<br>")
}

#[allow(dead_code)]
const LEGACY_STYLE: &str = "<style>\
:root{--bg:#f4f7fb;--card:#ffffff;--edge:#d8e2f0;--ink:#16233a;--dim:#5f6f86;--leaf:#2f6fed;--warn:#b07d0a;--sky:#2f6fed;--rose:#c0554e;}\
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);\
font:15px/1.55 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}\
.wrap{max-width:920px;margin:0 auto;padding:32px 20px 80px}\
header h1{margin:0 0 4px;font-size:24px}.sub{color:var(--dim);margin:0 0 24px}\
nav{display:flex;gap:8px;margin:-10px 0 28px}nav a{color:var(--ink);text-decoration:none;border:1px solid var(--edge);border-radius:20px;padding:5px 12px;font-size:12px}nav a:hover{border-color:var(--leaf);color:var(--leaf)}\
.section-title{scroll-margin-top:18px;margin:28px 0 12px}.section-title h2{font-size:20px;margin:0}.section-title p{color:var(--dim);font-size:13px;margin:2px 0 0}\
.execution{scroll-margin-top:18px}.funnel{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:0 0 18px}.metric-card{background:var(--card);border:1px solid var(--edge);border-radius:10px;padding:12px}.metric-card strong{display:block;font-size:22px}.metric-card span{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.04em}\
.mailboxes{background:var(--card);border:1px solid var(--edge);border-radius:12px;padding:14px 16px;margin-bottom:18px}.mailboxes h3{margin:0 0 8px;font-size:13px;text-transform:uppercase;color:var(--dim)}.mailbox{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:8px;padding:5px 0}.mailbox>span:last-child{color:var(--dim);font-size:12px}\
.people-title{margin-top:20px}.people-sheet-wrap{overflow:auto;background:var(--card);border:1px solid var(--edge);border-radius:12px;max-height:70vh;margin-bottom:22px}.people-sheet{width:max-content;min-width:100%;border-collapse:separate;border-spacing:0;font-size:12px}.people-sheet th{position:sticky;top:0;z-index:2;background:#edf3fc;color:var(--dim);text-align:left;text-transform:uppercase;letter-spacing:.035em;font-size:10px;padding:9px;border-bottom:1px solid var(--edge);white-space:nowrap}.people-sheet td{max-width:220px;padding:9px;border-bottom:1px solid var(--edge);vertical-align:top}.people-sheet tbody tr:last-child td{border-bottom:0}.people-sheet tbody tr:hover td{background:#f8fbff}.people-sheet td:nth-child(1){min-width:190px}.people-sheet td:nth-child(2){min-width:120px}.people-sheet td:nth-child(3){min-width:190px}.people-sheet td:nth-child(4){min-width:220px}.people-sheet .brand{display:inline-block;margin-bottom:4px}.sheet-email{word-break:break-word}.sheet-email small{display:block;color:var(--dim);margin-top:3px}.touch-slot{min-width:112px}.touch-peek summary{display:grid;gap:2px;padding:5px 6px;border:1px solid var(--edge);border-radius:7px;background:#fbfdff}.touch-peek summary strong{text-transform:uppercase;font-size:10px;color:var(--leaf)}.touch-peek summary small{color:var(--dim)}.touch-peek summary span{font-size:10px;color:var(--dim)}.touch-peek[open]{min-width:300px}.touch-copy{padding:8px 2px 2px}.touch-purpose{color:var(--dim);font-size:11px;text-transform:uppercase;margin:0 0 5px}.touch-missing{color:var(--dim);text-align:center}\
.empty{background:var(--card);border:1px solid var(--edge);border-radius:12px;padding:28px;color:var(--dim)}\
.empty code{color:var(--leaf)}\
.account{background:var(--card);border:1px solid var(--edge);border-radius:14px;padding:20px 22px;margin:0 0 20px}\
.account h2{margin:0 0 2px;font-size:19px}.meta{color:var(--dim);margin:0 0 12px;font-size:13px}\
.brand{font-size:11px;text-transform:uppercase;letter-spacing:.05em;border-radius:6px;padding:2px 7px;margin-right:8px;vertical-align:middle;background:#e6eefc;color:var(--leaf)}\
.brand.wapahki{background:#eaf4df;color:#4a7a2b}.brand.outagehub{background:#e3eefb;color:var(--sky)}\
.hyp{margin:0 0 6px}.mech,.metric,.concept,.hardq,.kill{color:var(--dim);margin:0 0 6px;font-size:14px}\
.lblhead{margin:8px 0 2px;font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:var(--dim)}\
.facts,.guesses,.signals{color:var(--dim);font-size:13px;margin:0 0 6px;padding-left:18px}\
.facts li{color:var(--ink)}\
.contact{border-top:1px solid var(--edge);padding:12px 0 4px;margin-top:10px}\
summary{cursor:pointer;list-style:none}summary::-webkit-details-marker{display:none}\
summary .name{font-weight:600}.pill{background:#e6eefc;color:var(--leaf);border-radius:20px;padding:1px 9px;font-size:11px;margin-left:4px}\
.state-pill,.email-state{color:var(--dim);border:1px solid var(--edge);border-radius:20px;padding:1px 7px;font-size:10px;margin-left:5px}.knowledge-cites{display:block;color:var(--dim);font-size:10px;margin-top:5px;max-width:220px;overflow-wrap:anywhere}.approve-form{margin:8px 0}.execution-touch .status{float:right}.activity{background:var(--card);border:1px solid var(--edge);border-radius:12px;padding:14px 16px;margin-top:18px}.activity ol{list-style:none;padding:0;margin:10px 0 0}.activity li{display:grid;grid-template-columns:190px auto 90px 1fr;gap:8px;border-top:1px solid var(--edge);padding:7px 0;font-size:12px}.activity time{color:var(--dim)}\
.prog{color:var(--dim);font-size:12px;float:right}\
.role{color:var(--dim);font-size:13px;margin:6px 0 2px}\
.stage{border:1px solid var(--edge);border-left:3px solid var(--sky);border-radius:8px;padding:10px 12px;margin:8px 0;background:#fbfdff}\
.stage.sent{border-left-color:var(--leaf)}.stage.skipped{opacity:.55}\
.stagehead{font-size:12px;color:var(--dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px}\
.status{margin-left:6px;padding:0 7px;border-radius:10px;font-size:10px}\
.status.sent{background:#e2f2ea;color:#1f7a4d}.status.pending{background:#fbf1d3;color:#8a6a0b}.status.skipped{background:#f6e3e2;color:var(--rose)}\
.subject{font-weight:600;margin-bottom:4px}.body{white-space:normal;margin-bottom:6px}\
.goal{color:var(--dim);font-size:12px;margin-bottom:4px}\
.review{font-size:11px;border-radius:6px;padding:3px 8px;margin-bottom:8px;display:inline-block}\
.review.ok{background:#e2f2ea;color:#1f7a4d}.review.edited{background:#fbf1d3;color:#8a6a0b}\
.actions{display:flex;gap:6px}.actions form{margin:0}\
.btn{background:#eef4fd;color:var(--ink);border:1px solid var(--edge);border-radius:6px;padding:4px 10px;font-size:12px;cursor:pointer}\
.btn.sent:hover{border-color:var(--leaf);color:var(--leaf)}.btn:hover{border-color:var(--dim)}\
@media(max-width:700px){.funnel{grid-template-columns:repeat(2,1fr)}.mailbox{grid-template-columns:auto 1fr}.mailbox>span:last-child{grid-column:2}.activity li{grid-template-columns:1fr}.prog{float:none;display:block;margin-top:4px}.people-sheet-wrap{max-height:75vh}}\
</style>";

const SHEET_STYLE: &str = r#"<style>
:root {
  --blue: #1a73e8;
  --blue-strong: #1558b8;
  --blue-tint: #e8f0fe;
  --blue-wash: #f5f8ff;
  --ink: #202124;
  --muted: #5f6368;
  --faint: #80868b;
  --line: #e3e7ed;
  --paper: #fff;
  --green: #188038;
  --green-tint: #e6f4ea;
  --amber: #b06000;
  --amber-tint: #fef7e0;
  --red: #c5221f;
  --red-tint: #fce8e6;
  --font: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}
* { box-sizing: border-box; }
html, body { width: 100%; height: 100%; margin: 0; overflow: hidden; }
body { color: var(--ink); background: var(--paper); font: 13px/1.45 var(--font); -webkit-font-smoothing: antialiased; }
.crm-shell { height: 100vh; display: flex; flex-direction: column; }
.topbar {
  min-height: 58px; padding: 0 18px; display: flex; align-items: center; gap: 24px;
  border-bottom: 1px solid var(--line); background: rgba(255,255,255,.96); flex: 0 0 auto;
}
.brand-lockup { display: flex; align-items: center; gap: 10px; min-width: 0; }
.mark {
  width: 30px; height: 30px; border-radius: 8px; display: grid; place-items: center;
  color: #fff; background: linear-gradient(135deg, #4285f4, var(--blue)); font-weight: 700;
  box-shadow: 0 1px 2px rgba(26,115,232,.35);
}
.topbar h1 { font-size: 15px; line-height: 1.2; margin: 0; font-weight: 650; letter-spacing: -.01em; }
.topbar p { margin: 2px 0 0; color: var(--faint); font-size: 11.5px; white-space: nowrap; }
.top-stats { margin-left: auto; display: flex; gap: 22px; color: var(--faint); white-space: nowrap; }
.top-stats strong { color: var(--ink); font-size: 14px; margin-right: 3px; }
.sheet-scroll { flex: 1 1 auto; min-height: 0; overflow: auto; }
.crm-sheet { border-collapse: separate; border-spacing: 0; table-layout: fixed; width: max-content; min-width: 100%; }
.c-company { width: 210px; }.c-context { width: 360px; }.c-person { width: 220px; }.c-why { width: 280px; }.c-touch { width: 230px; }
.crm-sheet th {
  position: sticky; top: 0; z-index: 8; height: 36px; padding: 0 10px; text-align: left;
  color: var(--muted); background: #f2f6fc; border-bottom: 1px solid var(--line); border-right: 1px solid var(--line);
  font-size: 10px; font-weight: 700; letter-spacing: .05em; text-transform: uppercase; white-space: nowrap;
}
.crm-sheet th.pin { left: 0; z-index: 12; }
.crm-sheet td {
  height: 94px; padding: 9px 10px; vertical-align: top; background: var(--paper);
  border-right: 1px solid var(--line); border-bottom: 1px solid var(--line);
}
.crm-sheet tr.alt td { background: #fcfdff; }
.crm-sheet tr:hover td { background: var(--blue-wash); }
.crm-sheet tr.account-start td { border-top: 1px solid #cbd7e8; }
.crm-sheet td.company.pin { position: sticky; left: 0; z-index: 5; background: var(--paper); }
.crm-sheet td.company.pin.alt { background: #f8faff; }
.company strong, .person strong { display: block; font-size: 13px; line-height: 1.35; }
.company small, .person small { display: block; margin-top: 3px; color: var(--faint); font-size: 11px; }
.company a, .email { display: inline-block; margin-top: 6px; color: var(--blue); text-decoration: none; font-size: 11px; overflow-wrap: anywhere; }
.company a:hover, .email:hover { text-decoration: underline; }
.brand-tag {
  display: inline-block; margin-bottom: 7px; padding: 1px 6px; border-radius: 4px;
  color: var(--blue); background: var(--blue-tint); font-size: 9px; font-weight: 750; letter-spacing: .05em; text-transform: uppercase;
}
.brand-tag.wapahki { color: #4f762b; background: #edf5e5; }
.brand-tag.outagehub { color: #0b57d0; background: #e8f0fe; }
.context { background: var(--blue-wash) !important; }
.context.alt { background: #eef4fe !important; }
.context-line + .context-line { margin-top: 9px; }
.context-line b { display: block; margin-bottom: 2px; color: var(--faint); font-size: 9px; letter-spacing: .045em; text-transform: uppercase; }
.context-line p { margin: 0; color: var(--muted); font-size: 11.5px; line-height: 1.45; }
.context-line:first-child p { color: var(--ink); font-weight: 550; }
.person-status {
  display: inline-block; margin-top: 7px; padding: 1px 6px; border: 1px solid var(--line); border-radius: 99px;
  color: var(--faint); background: #fff; font-size: 9.5px; text-transform: lowercase;
}
.person-status.verified, .person-status.contacted, .person-status.replied { color: var(--green); border-color: #c6e4cf; background: var(--green-tint); }
.person-status.suppressed, .person-status.bounced, .person-status.unsubscribed { color: var(--red); border-color: #f3c7c3; background: var(--red-tint); }
.why p { margin: 0; color: var(--muted); font-size: 11.5px; line-height: 1.45; }
.why small { display: block; margin-top: 7px; color: var(--faint); font-size: 10.5px; }
.why small b { color: var(--muted); }
.muted { color: var(--faint); }
.empty-person { background-image: repeating-linear-gradient(135deg, transparent, transparent 6px, rgba(95,99,104,.025) 6px, rgba(95,99,104,.025) 12px) !important; }
.touch { cursor: pointer; }
.touch summary { list-style: none; }.touch summary::-webkit-details-marker { display: none; }
.touch-tag {
  display: inline-block; padding: 1px 6px; border: 1px solid #d2e3fc; border-radius: 4px;
  color: var(--blue); background: var(--blue-tint); font-size: 9.5px; font-weight: 750; letter-spacing: .025em; text-transform: uppercase;
}
.touch.sent .touch-tag { color: var(--green); border-color: #c6e4cf; background: var(--green-tint); }
.touch.blocked .touch-tag, .touch.failed .touch-tag { color: var(--red); border-color: #f3c7c3; background: var(--red-tint); }
.touch time { display: block; margin: 5px 0 4px; color: var(--blue-strong); font-size: 10.5px; font-weight: 650; }
.touch summary p { margin: 0; color: var(--muted); font-size: 11px; line-height: 1.42; }
.touch:hover { background: var(--blue-tint) !important; }
.touch details[open] { min-width: 360px; }
.touch-full { margin-top: 9px; padding-top: 8px; border-top: 1px solid var(--line); }
.touch-full .subject { display: block; margin-bottom: 5px; font-size: 11.5px; }
.touch-full .message { color: var(--ink); white-space: normal; font-size: 11.5px; line-height: 1.5; }
.touch-full small { display: block; margin-top: 7px; color: var(--faint); }
.touch-state { float: right; color: var(--faint); font-size: 9.5px; text-transform: uppercase; }
.touch.missing { cursor: default; background-image: repeating-linear-gradient(135deg, transparent, transparent 6px, rgba(95,99,104,.025) 6px, rgba(95,99,104,.025) 12px); }
.touch.missing .touch-tag { color: var(--faint); border-color: var(--line); background: #f7f8f9; }
.touch.missing p { margin: 7px 0 0; color: #a0a5ac; font-size: 10.5px; }
.empty-sheet { height: calc(100vh - 94px) !important; text-align: center; vertical-align: middle !important; color: var(--faint); }
.empty-sheet strong { display: block; color: var(--ink); font-size: 15px; margin-bottom: 4px; }
.empty-sheet span { font-size: 12px; }
@media (max-width: 760px) {
  .topbar { padding: 0 12px; }.topbar p { display: none; }.top-stats { gap: 10px; font-size: 10px; }
  .c-company { width: 170px; }.c-context { width: 300px; }.c-person { width: 190px; }.c-why { width: 230px; }.c-touch { width: 210px; }
}
</style>"#;

#[cfg(test)]
mod tests {
    use super::{execution_dashboard, render_html, Crm};
    use crate::db::{Db, Lead, Mailbox, Person, Sequence, Touch};
    use uuid::Uuid;

    #[test]
    fn execution_dashboard_renders_real_records_without_credentials() {
        let path =
            std::env::temp_dir().join(format!("spruce-dashboard-test-{}.sqlite", Uuid::new_v4()));
        let db = Db::open(&path).expect("open temp db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "org-1".into(),
                name: "Real Logistics".into(),
                domain: "example.com".into(),
                industry: "logistics".into(),
                hypothesis: "Manual reconciliation is slowing decisions.".into(),
                status: "qualified".into(),
                ..Default::default()
            })
            .expect("insert lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "person-1".into(),
                name: "Alex Rivera".into(),
                title: "Operations Director".into(),
                email: "alex@example.com".into(),
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
        for (channel, stage) in [("email", 1), ("linkedin", 2)] {
            db.insert_touch(&Touch {
                sequence_id: sequence_id.clone(),
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                stage,
                channel: channel.into(),
                body: format!("A {channel} touch"),
                status: "draft".into(),
                due_at: if stage == 1 {
                    "2026-08-07T09:00:00Z".into()
                } else {
                    String::new()
                },
                recipient_timezone: "America/Toronto".into(),
                review_passes: Some(true),
                ..Default::default()
            })
            .expect("insert touch");
        }
        db.upsert_mailbox(&Mailbox {
            brand: "gnk".into(),
            from_name: "Sender".into(),
            from_email: "sender@example.com".into(),
            smtp_host: "smtp.example.com".into(),
            smtp_pass: "TOP_SECRET_PASSWORD".into(),
            active: true,
            ..Default::default()
        })
        .expect("insert mailbox");

        // Approval schedules email only; manual channel tasks remain drafts.
        assert_eq!(
            db.approve_touches(Some("gnk"), Some(&person_id))
                .expect("approve"),
            1
        );
        let touches = db
            .list_touches_for_person(&person_id)
            .expect("list touches");
        assert_eq!(touches[0].status, "scheduled");
        assert_eq!(touches[1].status, "draft");

        let dashboard = execution_dashboard(&db).expect("build dashboard");
        let json = serde_json::to_string(&dashboard).expect("serialize dashboard");
        assert!(!json.contains("TOP_SECRET_PASSWORD"));
        let html = render_html(&Crm::default(), Some(&dashboard));
        assert!(html.contains("Five people per company"));
        assert!(html.contains("Company context"));
        assert!(html.contains("Why they'd answer"));
        assert!(html.contains("Real Logistics"));
        assert!(html.contains("Alex Rivera"));
        assert!(html.contains("alex@example.com"));
        assert!(html.contains("Contact 5"));
        assert!(html.contains("Fri 7 Aug · 5:00 AM EDT"));
        assert!(html.contains("A email touch"));

        drop(dashboard);
        drop(db);
        for candidate in [
            path.clone(),
            std::path::PathBuf::from(format!("{}-wal", path.display())),
            std::path::PathBuf::from(format!("{}-shm", path.display())),
        ] {
            let _ = std::fs::remove_file(candidate);
        }
    }
}
