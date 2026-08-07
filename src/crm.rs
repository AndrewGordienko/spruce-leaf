//! Local CRM: a JSON-backed store plus a small web dashboard.
//!
//! Everything a campaign produces — accounts, the contacts at them (mapped by
//! vantage point), and each contact's outreach sequence with its pre-send
//! critique — is filed here. The store persists to a single JSON file and is
//! shared behind an `Arc<RwLock<_>>` between the web server and the agent.

use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
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
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding CRM server to http://{addr}"))?;
    axum::serve(listener, router(store, db))
        .await
        .context("CRM web server crashed")
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
    b.push_str(STYLE);
    b.push_str("</head><body><div class=\"wrap\">");
    b.push_str("<header><h1>\u{1F332} sales-os CRM</h1>");

    let total_contacts: usize = crm.accounts.iter().map(|a| a.contacts.len()).sum();
    let total_touches: usize = crm
        .accounts
        .iter()
        .flat_map(|a| &a.contacts)
        .map(|c| c.touches.len())
        .sum();
    let sent: usize = crm
        .accounts
        .iter()
        .flat_map(|a| &a.contacts)
        .flat_map(|c| &c.touches)
        .filter(|t| t.status == StageStatus::Sent)
        .count();
    let execution_summary = execution
        .map(|x| {
            format!(
                "{} real leads · {} people · {} verified · {} contacted",
                x.funnel.leads, x.funnel.people, x.funnel.verified, x.funnel.contacted
            )
        })
        .unwrap_or_else(|| "execution data unavailable".to_string());
    b.push_str(&format!(
        "<p class=\"sub\">{execution_summary}<br>{} research accounts · {} contacts · {} / {} manual touches sent</p>\
         <nav><a href=\"#execution\">Execution</a><a href=\"#opportunities\">Opportunities</a><a href=\"#research\">Research CRM</a></nav></header>",
        crm.accounts.len(),
        total_contacts,
        sent,
        total_touches
    ));

    if let Some(execution) = execution {
        render_execution(&mut b, execution);
    } else {
        b.push_str(
            "<section id=\"execution\"><h2>Execution</h2><div class=\"empty\">Could not read the execution database.</div></section>",
        );
    }

    b.push_str(
        "<div class=\"section-title\" id=\"research\"><h2>Research CRM</h2>\
         <p>Hypothesis-only campaigns generated without Apollo.</p></div>",
    );

    if crm.accounts.is_empty() {
        b.push_str(
            "<div class=\"empty\">No campaigns yet. In sales-os, try:<br>\
             <code>gnk: mid-market 3PLs that fight retailer chargebacks</code></div>",
        );
    }

    for ac in &crm.accounts {
        b.push_str("<section class=\"account\">");
        b.push_str(&format!(
            "<h2><span class=\"brand {brand}\">{brand}</span> {name}</h2>\
             <p class=\"meta\">{industry} · {hq}</p>",
            brand = esc(&ac.brand),
            name = esc(&ac.name),
            industry = esc(&ac.industry),
            hq = esc(&ac.hq),
        ));
        b.push_str(&format!(
            "<p class=\"hyp\"><strong>Hypothesis:</strong> {}</p>",
            esc(&ac.hypothesis)
        ));
        b.push_str(&format!(
            "<p class=\"mech\"><em>Mechanism:</em> {}</p>",
            esc(&ac.mechanism)
        ));
        b.push_str(&format!(
            "<p class=\"metric\"><em>Measure (not $):</em> {}</p>",
            esc(&ac.consequence_metric)
        ));

        list_block(&mut b, "Observed facts", &ac.observed_facts, "facts");
        list_block(&mut b, "Inferences (guesses)", &ac.inferences, "guesses");
        list_block(&mut b, "Signals", &ac.signals, "signals");

        b.push_str(&format!(
            "<p class=\"concept\"><em>System concept:</em> {}</p>",
            esc(&ac.system_concept)
        ));
        b.push_str(&format!(
            "<p class=\"hardq\"><em>Hard buyer question:</em> {}</p>",
            esc(&ac.hard_buyer_question)
        ));
        b.push_str(&format!(
            "<p class=\"kill\"><em>Kill condition:</em> {}</p>",
            esc(&ac.kill_condition)
        ));
        if !ac.magnitude_note.trim().is_empty() {
            b.push_str(&format!(
                "<p class=\"kill\"><em>Magnitude (internal only):</em> {}</p>",
                esc(&ac.magnitude_note)
            ));
        }
        if !ac.applied_principles.is_empty() {
            b.push_str(&format!(
                "<p class=\"metric\"><em>Playbook applied:</em> {}</p>",
                esc(&ac.applied_principles.join(", "))
            ));
        }

        for c in &ac.contacts {
            let sent_ct = c
                .touches
                .iter()
                .filter(|t| t.status == StageStatus::Sent)
                .count();
            let star = if c.primary { " ★" } else { "" };
            b.push_str("<details class=\"contact\">");
            b.push_str(&format!(
                "<summary><span class=\"name\">{}{}</span> \u{2014} {} \
                 <span class=\"pill\">{}</span> <span class=\"prog\">{}/{} sent</span></summary>",
                esc(&c.name),
                star,
                esc(&c.title),
                esc(&vantage_label(&c.vantage)),
                sent_ct,
                c.touches.len()
            ));
            b.push_str(&format!(
                "<p class=\"role\"><em>Why them:</em> {}</p>",
                esc(&c.why_them)
            ));
            b.push_str(&format!(
                "<p class=\"role\"><em>Can observe:</em> {}</p>",
                esc(&c.can_observe)
            ));
            if !c.route_to.trim().is_empty() {
                b.push_str(&format!(
                    "<p class=\"role\"><em>Route to:</em> {}</p>",
                    esc(&c.route_to)
                ));
            }
            if !c.applied_principles.is_empty() {
                b.push_str(&format!(
                    "<p class=\"role\"><em>Playbook applied:</em> {}</p>",
                    esc(&c.applied_principles.join(", "))
                ));
            }

            for t in &c.touches {
                let cls = t.status.label();
                b.push_str(&format!("<div class=\"stage {cls}\">"));
                b.push_str(&format!(
                    "<div class=\"stagehead\">Touch {} · Day {} · {} · {} \
                     <span class=\"status {cls}\">{}</span></div>",
                    t.stage,
                    t.day_offset,
                    esc(&t.channel),
                    esc(&t.purpose),
                    cls
                ));
                if !t.subject.is_empty() {
                    b.push_str(&format!(
                        "<div class=\"subject\">Subject: {}</div>",
                        esc(&t.subject)
                    ));
                }
                b.push_str(&format!(
                    "<div class=\"body\">{}</div>",
                    esc_multiline(&t.body)
                ));
                b.push_str(&format!("<div class=\"goal\">Goal: {}</div>", esc(&t.goal)));

                if let Some(passed) = t.review_passes {
                    if passed && t.review_issues.is_empty() {
                        b.push_str("<div class=\"review ok\">pre-send: clean</div>");
                    } else {
                        let issues = if t.review_issues.is_empty() {
                            "revised on pre-send".to_string()
                        } else {
                            t.review_issues
                                .iter()
                                .map(|i| esc(i))
                                .collect::<Vec<_>>()
                                .join("; ")
                        };
                        b.push_str(&format!(
                            "<div class=\"review edited\">pre-send edit: {issues}</div>"
                        ));
                    }
                }

                b.push_str("<div class=\"actions\">");
                b.push_str(&stage_button(&c.id, t.stage, "sent", "Mark sent"));
                b.push_str(&stage_button(&c.id, t.stage, "pending", "Reset"));
                b.push_str(&stage_button(&c.id, t.stage, "skipped", "Skip"));
                b.push_str("</div></div>");
            }
            b.push_str("</details>");
        }
        b.push_str("</section>");
    }

    b.push_str("</div></body></html>");
    b
}

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
                .filter(|t| t.status == "draft" && t.channel.eq_ignore_ascii_case("email"))
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
                // Manual LinkedIn/call tasks never flow through the send daemon,
                // so give them a one-click "done" that persists to the db.
                if !touch.channel.eq_ignore_ascii_case("email") && touch.status == "draft" {
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

fn vantage_label(v: &str) -> String {
    v.replace('_', " ")
}

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

const STYLE: &str = "<style>\
:root{--bg:#f4f7fb;--card:#ffffff;--edge:#d8e2f0;--ink:#16233a;--dim:#5f6f86;--leaf:#2f6fed;--warn:#b07d0a;--sky:#2f6fed;--rose:#c0554e;}\
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);\
font:15px/1.55 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}\
.wrap{max-width:920px;margin:0 auto;padding:32px 20px 80px}\
header h1{margin:0 0 4px;font-size:24px}.sub{color:var(--dim);margin:0 0 24px}\
nav{display:flex;gap:8px;margin:-10px 0 28px}nav a{color:var(--ink);text-decoration:none;border:1px solid var(--edge);border-radius:20px;padding:5px 12px;font-size:12px}nav a:hover{border-color:var(--leaf);color:var(--leaf)}\
.section-title{scroll-margin-top:18px;margin:28px 0 12px}.section-title h2{font-size:20px;margin:0}.section-title p{color:var(--dim);font-size:13px;margin:2px 0 0}\
.execution{scroll-margin-top:18px}.funnel{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:0 0 18px}.metric-card{background:var(--card);border:1px solid var(--edge);border-radius:10px;padding:12px}.metric-card strong{display:block;font-size:22px}.metric-card span{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.04em}\
.mailboxes{background:var(--card);border:1px solid var(--edge);border-radius:12px;padding:14px 16px;margin-bottom:18px}.mailboxes h3{margin:0 0 8px;font-size:13px;text-transform:uppercase;color:var(--dim)}.mailbox{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:8px;padding:5px 0}.mailbox>span:last-child{color:var(--dim);font-size:12px}\
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
.state-pill,.email-state{color:var(--dim);border:1px solid var(--edge);border-radius:20px;padding:1px 7px;font-size:10px;margin-left:5px}.approve-form{margin:8px 0}.execution-touch .status{float:right}.activity{background:var(--card);border:1px solid var(--edge);border-radius:12px;padding:14px 16px;margin-top:18px}.activity ol{list-style:none;padding:0;margin:10px 0 0}.activity li{display:grid;grid-template-columns:190px auto 90px 1fr;gap:8px;border-top:1px solid var(--edge);padding:7px 0;font-size:12px}.activity time{color:var(--dim)}\
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
@media(max-width:700px){.funnel{grid-template-columns:repeat(2,1fr)}.mailbox{grid-template-columns:auto 1fr}.mailbox>span:last-child{grid-column:2}.activity li{grid-template-columns:1fr}.prog{float:none;display:block;margin-top:4px}}\
</style>";

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
        assert!(html.contains("Real execution"));
        assert!(html.contains("Real Logistics"));
        assert!(html.contains("Alex Rivera"));

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
