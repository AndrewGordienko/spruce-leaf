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
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::domain::Campaign;

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

pub fn router(store: SharedStore) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/crm", get(api))
        .route("/stage/:contact/:stage/:status", post(set_stage))
        .with_state(store)
}

pub async fn serve(store: SharedStore, port: u16) -> Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding CRM server to http://{addr}"))?;
    axum::serve(listener, router(store))
        .await
        .context("CRM web server crashed")
}

async fn index(State(store): State<SharedStore>) -> Html<String> {
    let s = store.read().await;
    Html(render_html(&s.data))
}

async fn api(State(store): State<SharedStore>) -> Json<Crm> {
    let s = store.read().await;
    Json(s.data.clone())
}

async fn set_stage(
    State(store): State<SharedStore>,
    Path((contact, stage, status)): Path<(String, u32, String)>,
) -> impl IntoResponse {
    let new = match status.as_str() {
        "sent" => StageStatus::Sent,
        "skipped" => StageStatus::Skipped,
        _ => StageStatus::Pending,
    };
    {
        let mut s = store.write().await;
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

// --- HTML rendering --------------------------------------------------------

fn render_html(crm: &Crm) -> String {
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
    b.push_str(&format!(
        "<p class=\"sub\">{} accounts · {} contacts · {} / {} touches sent</p></header>",
        crm.accounts.len(),
        total_contacts,
        sent,
        total_touches
    ));

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
                b.push_str(&format!("<div class=\"body\">{}</div>", esc_multiline(&t.body)));
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
                        b.push_str(&format!("<div class=\"review edited\">pre-send edit: {issues}</div>"));
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

fn list_block(b: &mut String, title: &str, items: &[String], class: &str) {
    if items.is_empty() {
        return;
    }
    b.push_str(&format!("<p class=\"lblhead\">{title}</p><ul class=\"{class}\">"));
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
:root{--bg:#0f1210;--card:#171b18;--edge:#26302a;--ink:#e6efe9;--dim:#93a49a;--leaf:#4ea36b;--warn:#c9a227;--sky:#5b9bd5;--rose:#c98;}\
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);\
font:15px/1.55 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}\
.wrap{max-width:920px;margin:0 auto;padding:32px 20px 80px}\
header h1{margin:0 0 4px;font-size:24px}.sub{color:var(--dim);margin:0 0 24px}\
.empty{background:var(--card);border:1px solid var(--edge);border-radius:12px;padding:28px;color:var(--dim)}\
.empty code{color:var(--leaf)}\
.account{background:var(--card);border:1px solid var(--edge);border-radius:14px;padding:20px 22px;margin:0 0 20px}\
.account h2{margin:0 0 2px;font-size:19px}.meta{color:var(--dim);margin:0 0 12px;font-size:13px}\
.brand{font-size:11px;text-transform:uppercase;letter-spacing:.05em;border-radius:6px;padding:2px 7px;margin-right:8px;vertical-align:middle;background:#233029;color:var(--leaf)}\
.brand.wapahki{background:#26301f;color:#a8c66c}.brand.outagehub{background:#1f2836;color:var(--sky)}\
.hyp{margin:0 0 6px}.mech,.metric,.concept,.hardq,.kill{color:var(--dim);margin:0 0 6px;font-size:14px}\
.lblhead{margin:8px 0 2px;font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:var(--dim)}\
.facts,.guesses,.signals{color:var(--dim);font-size:13px;margin:0 0 6px;padding-left:18px}\
.facts li{color:var(--ink)}\
.contact{border-top:1px solid var(--edge);padding:12px 0 4px;margin-top:10px}\
summary{cursor:pointer;list-style:none}summary::-webkit-details-marker{display:none}\
summary .name{font-weight:600}.pill{background:#233029;color:var(--leaf);border-radius:20px;padding:1px 9px;font-size:11px;margin-left:4px}\
.prog{color:var(--dim);font-size:12px;float:right}\
.role{color:var(--dim);font-size:13px;margin:6px 0 2px}\
.stage{border:1px solid var(--edge);border-left:3px solid var(--dim);border-radius:8px;padding:10px 12px;margin:8px 0}\
.stage.sent{border-left-color:var(--leaf)}.stage.skipped{opacity:.55}\
.stagehead{font-size:12px;color:var(--dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px}\
.status{margin-left:6px;padding:0 7px;border-radius:10px;font-size:10px}\
.status.sent{background:#1c3326;color:var(--leaf)}.status.pending{background:#2a2a1c;color:var(--warn)}.status.skipped{background:#2a2020;color:var(--rose)}\
.subject{font-weight:600;margin-bottom:4px}.body{white-space:normal;margin-bottom:6px}\
.goal{color:var(--dim);font-size:12px;margin-bottom:4px}\
.review{font-size:11px;border-radius:6px;padding:3px 8px;margin-bottom:8px;display:inline-block}\
.review.ok{background:#1c3326;color:var(--leaf)}.review.edited{background:#2a2a1c;color:var(--warn)}\
.actions{display:flex;gap:6px}.actions form{margin:0}\
.btn{background:#1d241f;color:var(--ink);border:1px solid var(--edge);border-radius:6px;padding:4px 10px;font-size:12px;cursor:pointer}\
.btn.sent:hover{border-color:var(--leaf);color:var(--leaf)}.btn:hover{border-color:var(--dim)}\
</style>";
