//! Local CRM: a JSON-backed store plus a small web dashboard.
//!
//! Everything a campaign produces — accounts, the contacts at them (mapped by
//! vantage point), and each contact's outreach sequence with its pre-send
//! critique — is filed here. The store persists to a single JSON file and is
//! shared behind an `Arc<RwLock<_>>` between the web server and the agent.
//!
//! Alongside the pipeline sheet, the same server exposes a Strategy board
//! (`/strategy`) that surfaces the business operating profiles and outreach
//! playbooks guiding Wapahki, GnK, and OutageHub — the business side of the SDR.

use std::collections::BTreeSet;
use std::io::{Read as _, Write as _};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    extract::{Form, Path, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Datelike, Local, NaiveDate, Utc, Weekday};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::business::{BusinessProfile, Businesses};
use crate::db::{
    AccountPlayAssessment, ApplicationBrief, CommercialAssessment, CommercialEvent,
    CommercialForecast, CommercialOperatingState, CoverageRun, CustomerDevelopmentRecord, Event,
    EvidenceClaim, Facility, GtmExperiment, GtmOutcome, GtmPlay, Lead, Mailbox, MarketAccount,
    MarketSegment, Meeting, Opportunity, OpportunityContact, OpportunityStakeholder,
    OpportunityTouch, Person, ProofBrief, SalesOpportunity, SharedDb, SignalDefinition,
    SignalObservation, Touch,
};
use crate::knowledge::{Library, SharedLibrary};
use crate::metrics::{self, Funnel};
use crate::playbook::{Playbook, Playbooks, Shared as SharedDoctrine};

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
    /// When Spruce Leaf finished writing this draft. Older CRM JSON remains
    /// readable; its legacy rows simply omit the label.
    #[serde(default)]
    pub created_at: String,
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

// --- Brands ----------------------------------------------------------------

/// The portfolio this CRM serves. One clickable tab per brand runs across the
/// top of every page; `key` matches the `brand` column on every record and the
/// CSS accent class. The order here is the order the tabs render in.
struct BrandMeta {
    key: &'static str,
    name: &'static str,
    tagline: &'static str,
}

const BRANDS: &[BrandMeta] = &[
    BrandMeta {
        key: "wapahki",
        name: "Wapahki",
        tagline: "Flexible robotic packing cells for bounded, repetitive food-manufacturing tasks.",
    },
    BrandMeta {
        key: "gnk",
        name: "GnK",
        tagline: "Custom software and AI for expensive, organization-specific workflows.",
    },
    BrandMeta {
        key: "outagehub",
        name: "OutageHub",
        tagline: "The outage-data layer behind an operational decision.",
    },
];

/// Browser icon for the local CRM. Keep this beside the app identity rather
/// than in a separate asset pipeline: the CRM is a single-binary local tool,
/// and the north-east arrow mirrors the mark in the persistent top bar.
const FAVICON_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <defs>
    <linearGradient id="arrow" x1="16" y1="48" x2="48" y2="16" gradientUnits="userSpaceOnUse">
      <stop stop-color="#91f2d5"/>
      <stop offset="1" stop-color="#62bfff"/>
    </linearGradient>
  </defs>
  <rect width="64" height="64" rx="15" fill="#0c1733"/>
  <path d="M18 46 46 18M28 18h18v18" fill="none" stroke="url(#arrow)" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/>
</svg>"##;

fn brand_meta(key: &str) -> Option<&'static BrandMeta> {
    BRANDS.iter().find(|b| b.key == key)
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
        crate::storage::atomic_write(&self.path, json)
            .with_context(|| format!("writing CRM store {}", self.path.display()))
    }
}

pub fn open(path: impl AsRef<FsPath>) -> Result<SharedStore> {
    let store = Store::load(path.as_ref().to_path_buf())?;
    Ok(Arc::new(RwLock::new(store)))
}

// --- Web server ------------------------------------------------------------

/// Which surface the topbar is currently on. Pipeline is the people sheet;
/// Strategy is the business / SDR doctrine board.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Surface {
    Pipeline,
    Sponsorship,
    Strategy,
    Gtm,
}

#[derive(Clone)]
struct WebState {
    store: SharedStore,
    db: SharedDb,
    businesses: Arc<Businesses>,
    playbooks: Arc<Playbooks>,
    library: SharedLibrary,
}

#[derive(Debug, Serialize)]
struct ExecutionDashboard {
    funnel: Funnel,
    accounts: Vec<ExecutionAccount>,
    mapped_contacts: usize,
    leads: Vec<Lead>,
    sales_opportunities: Vec<SalesOpportunity>,
    opportunities: Vec<ExecutionOpportunity>,
    meetings: Vec<Meeting>,
    mailboxes: Vec<PublicMailbox>,
    replies: Vec<crate::db::Reply>,
    events: Vec<Event>,
    customer_development: Vec<CustomerDevelopmentRecord>,
}

#[derive(Debug, Serialize)]
struct GtmSnapshot {
    market_accounts: Vec<MarketAccount>,
    segments: Vec<MarketSegment>,
    coverage_runs: Vec<CoverageRun>,
    facilities: Vec<Facility>,
    sales_opportunities: Vec<SalesOpportunity>,
    evidence_claims: Vec<EvidenceClaim>,
    opportunity_stakeholders: Vec<OpportunityStakeholder>,
    commercial_assessments: Vec<CommercialAssessment>,
    commercial_events: Vec<CommercialEvent>,
    commercial_forecast: CommercialForecast,
    people: Vec<Person>,
    definitions: Vec<SignalDefinition>,
    observations: Vec<SignalObservation>,
    plays: Vec<GtmPlay>,
    assessments: Vec<AccountPlayAssessment>,
    experiments: Vec<GtmExperiment>,
    outcomes: Vec<GtmOutcome>,
    proofs: Vec<ProofBrief>,
    customer_development: Vec<CustomerDevelopmentRecord>,
    leads: Vec<Lead>,
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

fn complete_reviewed_sequence(touches: &[Touch]) -> bool {
    matches!(touches.len(), 1 | 2 | 4 | 7)
        && (1..=touches.len() as i64).all(|stage| {
            touches.iter().any(|touch| {
                touch.stage == stage
                    && touch.review_passes == Some(true)
                    && !touch.body.trim().is_empty()
                    && touch.body.trim() != "Writing draft…"
            })
        })
}

fn ready_execution_person(db: &SharedDb, person: &Person) -> Result<Option<ExecutionPerson>> {
    let Some(sequence_id) = db.active_sequence_for_person(&person.id)? else {
        return Ok(None);
    };
    let current_policy = db
        .sequence_gtm_attribution(&sequence_id)?
        .is_some_and(|sequence| {
            sequence.copy_policy_version == crate::db::CURRENT_COPY_POLICY_VERSION
        });
    if !current_policy {
        return Ok(None);
    }
    let touches = db.list_touches_for_sequence(&sequence_id)?;
    if !complete_reviewed_sequence(&touches) {
        return Ok(None);
    }
    let Some(lead) = db.get_lead(&person.lead_id)? else {
        return Ok(None);
    };
    let playbooks = crate::playbook::Playbooks::load("playbooks")?;
    let playbook = playbooks.get(&person.brand)?;
    if !crate::gtm::prepare_action(db, &person.brand, &person.lead_id, person)?
        .sequence_ready_for(touches.len())
        || crate::gtm::delivery_block_reason(db, playbook, &lead, person)?.is_some()
    {
        // Pipeline is execution state, not an archive. If a newer GTM play or
        // account assessment says the account is no longer ready, retain the
        // sequence in GTM Lab but do not present it as sendable work.
        return Ok(None);
    }
    Ok(Some(ExecutionPerson {
        person: person.clone(),
        touches,
        applied_principles: db.active_sequence_principles_for_person(&person.id)?,
    }))
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

fn execution_dashboard(db: &SharedDb, brand: Option<&str>) -> Result<ExecutionDashboard> {
    let people = db.list_people(brand, None)?;
    let person_ids = people
        .iter()
        .map(|person| person.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let leads = db.list_leads(brand)?;
    let mut accounts = Vec::new();
    for lead in leads.iter().cloned() {
        let mut account_people = Vec::new();
        for person in people.iter().filter(|p| p.lead_id == lead.id) {
            if let Some(entry) = ready_execution_person(db, person)? {
                account_people.push(entry);
            }
        }
        if !account_people.is_empty() {
            accounts.push(ExecutionAccount {
                lead,
                people: account_people,
            });
        }
    }
    let ready_lead_ids = accounts
        .iter()
        .map(|account| account.lead.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let mapped_contacts = people
        .iter()
        .filter(|person| ready_lead_ids.contains(person.lead_id.as_str()))
        .count();

    let mut opportunities = Vec::new();
    for opportunity in db.list_opportunities(brand, None)? {
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
        funnel: metrics::funnel(db, brand)?,
        accounts,
        mapped_contacts,
        leads,
        sales_opportunities: db.list_sales_opportunities(brand, None)?,
        opportunities,
        meetings: db.list_meetings(brand)?,
        mailboxes: db
            .list_mailboxes(brand)?
            .into_iter()
            .map(Into::into)
            .collect(),
        replies: db
            .list_replies(100)?
            .into_iter()
            .filter(|reply| brand.is_none() || person_ids.contains(reply.person_id.as_str()))
            .collect(),
        events: db.recent_events(brand, 40)?,
        customer_development: db.list_customer_development(brand)?,
    })
}

fn gtm_snapshot(db: &SharedDb, brand: Option<&str>) -> Result<GtmSnapshot> {
    db.expire_signal_observations()?;
    let market_accounts = db.list_market_accounts(brand)?;
    let account_ids = market_accounts
        .iter()
        .map(|account| account.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let facilities = db
        .list_facilities(None)?
        .into_iter()
        .filter(|facility| {
            brand.is_none() || account_ids.contains(facility.market_account_id.as_str())
        })
        .collect();
    Ok(GtmSnapshot {
        market_accounts,
        segments: db.list_market_segments(brand)?,
        coverage_runs: db.list_coverage_runs(brand)?,
        facilities,
        sales_opportunities: db.list_sales_opportunities(brand, None)?,
        evidence_claims: db.list_evidence_claims(None, brand)?,
        opportunity_stakeholders: db.list_opportunity_stakeholders(None, brand)?,
        commercial_assessments: db.list_commercial_assessments(brand)?,
        commercial_events: db.list_commercial_events(brand, None)?,
        commercial_forecast: db.commercial_forecast(brand)?,
        people: db.list_people(brand, None)?,
        definitions: db.list_signal_definitions(brand)?,
        observations: db.list_signal_observations(brand, 250)?,
        plays: db.list_gtm_plays(brand)?,
        assessments: db.list_account_play_assessments(brand)?,
        experiments: db.list_gtm_experiments(brand)?,
        outcomes: db.list_gtm_outcomes(brand, 250)?,
        proofs: db.list_proof_briefs(brand)?,
        customer_development: db.list_customer_development(brand)?,
        leads: db.list_leads(brand)?,
    })
}

/// (brand, live contact count) for every tab, so the top bar can show how much
/// is in each book of business without loading a whole dashboard per tab.
fn brand_tab_counts(db: &SharedDb) -> Vec<(&'static BrandMeta, usize)> {
    BRANDS
        .iter()
        .map(|meta| {
            let execution_contacts = db.list_people(Some(meta.key), None).map_or(0, |people| {
                people
                    .iter()
                    .filter(|person| ready_execution_person(db, person).ok().flatten().is_some())
                    .count()
            });
            let sponsorship_contacts = reviewable_sponsorship_contacts(db, meta.key);
            (meta, execution_contacts + sponsorship_contacts)
        })
        .collect()
}

fn reviewable_sponsorship_contacts(db: &SharedDb, brand: &str) -> usize {
    db.list_opportunities(Some(brand), None)
        .map_or(0, |opportunities| {
            opportunities
                .iter()
                .filter(|opportunity| opportunity.kind == "sponsorship")
                .flat_map(|opportunity| {
                    db.list_opportunity_contacts(&opportunity.id)
                        .unwrap_or_default()
                })
                .filter(|contact| {
                    db.list_opportunity_touches(&contact.id)
                        .is_ok_and(|touches| {
                            touches.iter().any(|touch| {
                                touch.status == "draft" && touch.review_passes == Some(true)
                            })
                        })
                })
                .count()
        })
}

pub fn router(
    store: SharedStore,
    db: SharedDb,
    businesses: Arc<Businesses>,
    playbooks: Arc<Playbooks>,
    library: SharedLibrary,
) -> Router {
    let state = WebState {
        store,
        db,
        businesses,
        playbooks,
        library,
    };
    Router::new()
        .route("/", get(hub))
        .route("/favicon.svg", get(favicon))
        .route("/b/:brand", get(brand_index))
        .route("/b/outagehub/sponsorship", get(outagehub_sponsorship_index))
        .route("/strategy", get(strategy_hub))
        .route("/strategy/:brand", get(strategy_brand))
        .route("/gtm", get(gtm_hub))
        .route("/gtm/:brand", get(gtm_brand))
        .route("/api/health", get(health))
        .route("/api/crm", get(api))
        .route("/api/execution", get(execution_api))
        .route("/api/strategy", get(strategy_api))
        .route("/api/gtm", get(gtm_api))
        .route("/gtm/experiment", post(create_gtm_experiment))
        .route("/gtm/play/:id/:status", post(set_gtm_play_status))
        .route(
            "/gtm/experiment/:id/:status",
            post(set_gtm_experiment_status),
        )
        .route(
            "/gtm/experiment/:id/evaluate/results",
            post(evaluate_gtm_experiment),
        )
        .route("/gtm/proof/:id/:status", post(set_gtm_proof_status))
        .route("/commercial-assessment", post(save_commercial_assessment))
        .route("/commercial-event", post(save_commercial_event))
        .route(
            "/commercial-operating-state",
            post(save_commercial_operating_state),
        )
        .route("/customer-development", post(save_customer_development))
        .route("/stage/:contact/:stage/:status", post(set_stage))
        .route("/execution/approve/:person", post(approve_execution))
        .route(
            "/execution/person/:person/linkedin",
            post(set_linkedin_status),
        )
        .route("/execution/touch/:touch/done", post(mark_touch_done))
        .route(
            "/opportunities/approve/:contact",
            post(approve_opportunity_outreach),
        )
        .layer(middleware::from_fn(local_write_guard))
        .with_state(state)
}

async fn favicon() -> impl IntoResponse {
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "image/svg+xml; charset=utf-8",
            ),
            (axum::http::header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        FAVICON_SVG,
    )
}

/// The dashboard is deliberately loopback-only, but a remote web page can still
/// attempt a cross-site form POST to localhost. Refuse browser-originated writes
/// unless the page itself came from loopback. Headerless CLI requests remain
/// available for local diagnostics and automation.
async fn local_write_guard(request: Request, next: Next) -> Response {
    if request.method() != Method::POST || local_write_headers(request.headers()) {
        return next.run(request).await;
    }
    (
        StatusCode::FORBIDDEN,
        "cross-site writes to the local Spruce Leaf CRM are not allowed",
    )
        .into_response()
}

fn local_write_headers(headers: &HeaderMap) -> bool {
    if headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("cross-site"))
    {
        return false;
    }
    let Some(origin) = headers.get("origin").and_then(|value| value.to_str().ok()) else {
        return true;
    };
    ["http://127.0.0.1", "http://localhost", "http://[::1]"]
        .iter()
        .any(|base| {
            origin
                .strip_prefix(base)
                .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with(':'))
        })
}

/// How many loopback ports to scan upward from the preferred one before falling
/// back to an OS-assigned port.
const CRM_PORT_SCAN: u16 = 128;
/// Increment when a dashboard/API change makes reusing an older local server
/// unsafe. Package version stays intentionally stable during local development,
/// so it cannot distinguish a stale process on its own.
pub(crate) const CRM_PROTOCOL_REV: u32 = 3;

/// Loopback ports to try for the CRM, starting at `first`.
pub fn port_candidates(first: u16) -> Vec<u16> {
    (0..CRM_PORT_SCAN)
        .filter_map(|offset| first.checked_add(offset))
        .collect()
}

/// Bind the first free port at or above `first`, else an OS-assigned one. Local
/// development stays loopback-only. A hosted Docker service may explicitly set
/// `SPRUCE_CRM_BIND=0.0.0.0`; the compose file still publishes it only on the
/// VM's loopback interface for an SSH/Tailscale tunnel.
pub fn bind_free_listener(first: u16) -> Result<TcpListener> {
    let ip = std::env::var("SPRUCE_CRM_BIND")
        .ok()
        .and_then(|value| value.parse::<IpAddr>().ok())
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
    for port in port_candidates(first) {
        let address = SocketAddr::new(ip, port);
        if let Ok(listener) = TcpListener::bind(address) {
            return Ok(listener);
        }
    }
    TcpListener::bind(SocketAddr::new(ip, 0)).context("finding a free port for the CRM")
}

/// True when a Spruce Leaf CRM is already answering on this loopback port. A
/// blocking, sub-second probe — safe at startup or from within `spawn_blocking`.
/// This is the single source of truth for "is our CRM actually up?", used both
/// to reuse a sibling session's server and to detect a link that has gone dead.
pub fn is_live(port: u16) -> bool {
    http_probe(port, "/api/health").is_some_and(|response| {
        response.contains("\"app\":\"spruce-leaf\"")
            && response.contains(&format!("\"protocol\":{CRM_PROTOCOL_REV}"))
    })
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
    businesses: Arc<Businesses>,
    playbooks: Arc<Playbooks>,
    library: SharedLibrary,
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
    axum::serve(listener, router(store, db, businesses, playbooks, library))
        .await
        .context("CRM web server crashed")
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "app": "spruce-leaf",
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": CRM_PROTOCOL_REV,
    }))
}

/// Portfolio landing: the persistent brand tabs plus a card per brand, so you
/// can click straight through to any of the three CRMs.
async fn hub(State(state): State<WebState>) -> Html<String> {
    let counts = brand_tab_counts(&state.db);
    Html(render_hub(&counts, &state.businesses, &state.db))
}

/// One brand's CRM: the same dashboard, scoped to a single book of business.
async fn brand_index(
    State(state): State<WebState>,
    Path(brand): Path<String>,
) -> impl IntoResponse {
    let Some(meta) = brand_meta(&brand) else {
        return (StatusCode::NOT_FOUND, format!("unknown brand '{brand}'")).into_response();
    };
    let crm = state.store.read().await.data.clone();
    let execution = execution_dashboard(&state.db, Some(meta.key)).ok();
    let counts = brand_tab_counts(&state.db);
    let profile = state.businesses.get(meta.key).ok();
    Html(render_html(
        &crm,
        execution.as_ref(),
        Some(meta),
        &counts,
        profile,
    ))
    .into_response()
}

async fn outagehub_sponsorship_index(State(state): State<WebState>) -> Html<String> {
    let counts = brand_tab_counts(&state.db);
    let execution = execution_dashboard(&state.db, Some("outagehub")).ok();
    let audit = crate::opportunity::audit_sponsorship_campaign(&state.db, "outagehub", 30).ok();
    Html(render_sponsorship_page(
        execution.as_ref(),
        audit.as_ref(),
        &counts,
    ))
}

/// Portfolio strategy board: what each business is trying to do and the shared
/// SDR doctrine that guides every outbound sequence.
async fn strategy_hub(State(state): State<WebState>) -> Html<String> {
    let counts = brand_tab_counts(&state.db);
    let library = state.library.read().await;
    Html(render_strategy_hub(
        &counts,
        &state.businesses,
        &state.playbooks,
        Some(&library),
    ))
}

/// Per-brand strategy deep dive: operating profile + outreach playbook.
async fn strategy_brand(
    State(state): State<WebState>,
    Path(brand): Path<String>,
) -> impl IntoResponse {
    let Some(meta) = brand_meta(&brand) else {
        return (StatusCode::NOT_FOUND, format!("unknown brand '{brand}'")).into_response();
    };
    let Ok(profile) = state.businesses.get(meta.key) else {
        return (
            StatusCode::NOT_FOUND,
            format!("no business profile for '{brand}'"),
        )
            .into_response();
    };
    let Ok(playbook) = state.playbooks.get(meta.key) else {
        return (
            StatusCode::NOT_FOUND,
            format!("no outreach playbook for '{brand}'"),
        )
            .into_response();
    };
    let counts = brand_tab_counts(&state.db);
    Html(render_strategy_brand(
        meta,
        profile,
        playbook,
        &state.playbooks.shared,
        &counts,
    ))
    .into_response()
}

async fn strategy_api(State(state): State<WebState>) -> impl IntoResponse {
    let library = state.library.read().await;
    let mut brands = Vec::new();
    for key in state.businesses.keys() {
        let Ok(profile) = state.businesses.get(key) else {
            continue;
        };
        let playbook = state.playbooks.get(key).ok();
        brands.push(serde_json::json!({
            "business": profile,
            "playbook": playbook.map(|pb| serde_json::json!({
                "key": pb.key,
                "name": pb.name,
                "signature": pb.signature,
                "one_liner": pb.one_liner,
                "motion": pb.motion,
                "min_words": pb.min_words,
                "max_words": pb.max_words,
                "min_signals": pb.min_signals,
                "max_employees": pb.max_employees,
                "icp_note": pb.icp_note,
                "system_concept_examples": pb.system_concept_examples,
                "subject_examples": pb.subject_examples,
                "vantage_notes": pb.vantage_notes,
                "requirements": pb.requirements,
                "forbidden": pb.forbidden,
                "doctrine": pb.doctrine,
            })),
        }));
    }
    Json(serde_json::json!({
        "shared_doctrine": state.playbooks.shared.doctrine,
        "shared_forbidden": state.playbooks.shared.forbidden,
        "personas": {
            "planner": state.playbooks.shared.personas.planner,
            "writer": state.playbooks.shared.personas.writer,
            "reviewer": state.playbooks.shared.personas.reviewer,
            "psychology": state.playbooks.shared.personas.psychology,
            "sales_council": state.playbooks.shared.personas.critics.iter().map(|critic| serde_json::json!({
                "id": critic.id,
                "name": critic.name,
                "source_path": critic.source_path.display().to_string(),
                "prompt": critic.prompt,
            })).collect::<Vec<_>>(),
        },
        "knowledge": {
            "stats": library.stats(),
            "books": library.books,
        },
        "brands": brands,
    }))
    .into_response()
}

async fn gtm_hub(State(state): State<WebState>) -> impl IntoResponse {
    let counts = brand_tab_counts(&state.db);
    match gtm_snapshot(&state.db, None) {
        Ok(snapshot) => Html(render_gtm_lab(None, &counts, &snapshot, None)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read GTM Lab: {error:#}"),
        )
            .into_response(),
    }
}

async fn gtm_brand(State(state): State<WebState>, Path(brand): Path<String>) -> impl IntoResponse {
    let Some(meta) = brand_meta(&brand) else {
        return (StatusCode::NOT_FOUND, format!("unknown brand '{brand}'")).into_response();
    };
    let counts = brand_tab_counts(&state.db);
    match gtm_snapshot(&state.db, Some(meta.key)) {
        Ok(snapshot) => Html(render_gtm_lab(
            Some(meta),
            &counts,
            &snapshot,
            state.businesses.get(meta.key).ok(),
        ))
        .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read GTM Lab: {error:#}"),
        )
            .into_response(),
    }
}

async fn gtm_api(State(state): State<WebState>) -> impl IntoResponse {
    match gtm_snapshot(&state.db, None) {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read GTM Lab: {error:#}"),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct GtmExperimentForm {
    brand: String,
    play_id: String,
    name: String,
    experiment_type: String,
    hypothesis: String,
    variable: String,
    constants: String,
    control_description: String,
    variant_description: String,
    minimum_sends_per_arm: String,
    baseline_sends: String,
    baseline_positive_reply_rate: String,
    success_target: String,
    failure_floor: String,
    measurement_days: String,
}

async fn create_gtm_experiment(
    State(state): State<WebState>,
    Form(form): Form<GtmExperimentForm>,
) -> impl IntoResponse {
    let play_matches_brand = state
        .db
        .list_gtm_plays(Some(&form.brand))
        .map(|plays| plays.iter().any(|play| play.id == form.play_id))
        .unwrap_or(false);
    if !play_matches_brand {
        return (
            StatusCode::BAD_REQUEST,
            "play does not belong to this brand",
        )
            .into_response();
    }
    let constants = form
        .constants
        .lines()
        .flat_map(|line| line.split(';'))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let experiment = GtmExperiment {
        brand: form.brand.clone(),
        play_id: form.play_id,
        name: form.name,
        experiment_type: form.experiment_type,
        hypothesis: form.hypothesis,
        variable: form.variable,
        constants,
        control_description: form.control_description,
        variant_description: form.variant_description,
        minimum_sends_per_arm: form.minimum_sends_per_arm.parse().unwrap_or(500),
        baseline_sends: form.baseline_sends.parse().unwrap_or(0),
        baseline_positive_reply_rate: form.baseline_positive_reply_rate.parse().unwrap_or(0.0),
        success_target: form.success_target.parse().unwrap_or(0.0),
        failure_floor: form.failure_floor.parse().unwrap_or(0.0),
        measurement_days: form.measurement_days.parse().unwrap_or(21),
        status: "draft".into(),
        ..Default::default()
    };
    match state.db.create_gtm_experiment(&experiment) {
        Ok(_) => Redirect::to(&format!("/gtm/{}", form.brand)).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct GtmExperimentResultForm {
    control_sent: i64,
    control_positive: i64,
    variant_sent: i64,
    variant_positive: i64,
}

async fn evaluate_gtm_experiment(
    State(state): State<WebState>,
    Path(id): Path<String>,
    Form(form): Form<GtmExperimentResultForm>,
) -> impl IntoResponse {
    let brand = state
        .db
        .list_gtm_experiments(None)
        .ok()
        .and_then(|experiments| {
            experiments
                .into_iter()
                .find(|experiment| experiment.id == id)
        })
        .map(|experiment| experiment.brand)
        .unwrap_or_default();
    match state.db.evaluate_gtm_experiment(
        &id,
        form.control_sent,
        form.control_positive,
        form.variant_sent,
        form.variant_positive,
    ) {
        Ok(_) => {
            let destination = if brand.is_empty() {
                "/gtm".to_string()
            } else {
                format!("/gtm/{brand}")
            };
            Redirect::to(&destination).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    }
}

async fn set_gtm_play_status(
    State(state): State<WebState>,
    Path((id, status)): Path<(String, String)>,
) -> impl IntoResponse {
    let brand = state
        .db
        .list_gtm_plays(None)
        .ok()
        .and_then(|plays| plays.into_iter().find(|play| play.id == id))
        .map(|play| play.brand)
        .unwrap_or_default();
    match state.db.set_gtm_play_lifecycle(&id, &status) {
        Ok(()) => {
            let destination = if brand.is_empty() {
                "/gtm".to_string()
            } else {
                format!("/gtm/{brand}")
            };
            Redirect::to(&destination).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    }
}

async fn set_gtm_experiment_status(
    State(state): State<WebState>,
    Path((id, status)): Path<(String, String)>,
) -> impl IntoResponse {
    let brand = state
        .db
        .list_gtm_experiments(None)
        .ok()
        .and_then(|experiments| {
            experiments
                .into_iter()
                .find(|experiment| experiment.id == id)
        })
        .map(|experiment| experiment.brand)
        .unwrap_or_default();
    match state.db.set_gtm_experiment_status(&id, &status) {
        Ok(()) => {
            let destination = if brand.is_empty() {
                "/gtm".to_string()
            } else {
                format!("/gtm/{brand}")
            };
            Redirect::to(&destination).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    }
}

async fn set_gtm_proof_status(
    State(state): State<WebState>,
    Path((id, status)): Path<(String, String)>,
) -> impl IntoResponse {
    let brand = state
        .db
        .list_proof_briefs(None)
        .ok()
        .and_then(|proofs| proofs.into_iter().find(|proof| proof.id == id))
        .map(|proof| proof.brand)
        .unwrap_or_default();
    match state.db.set_proof_status(&id, &status) {
        Ok(()) => {
            let destination = if brand.is_empty() {
                "/gtm".to_string()
            } else {
                format!("/gtm/{brand}")
            };
            Redirect::to(&destination).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct CommercialAssessmentForm {
    brand: String,
    sales_opportunity_id: String,
    commercial_lane: String,
    offer_key: String,
    sales_stage: String,
    expected_contract_value_cad: String,
    expected_upfront_cash_cad: String,
    cash_collectable_within_90d_cad: String,
    expected_arr_cad: String,
    estimated_12m_gross_profit_cad: String,
    days_to_first_cash: String,
    close_probability_percent: String,
    sales_hours_remaining: String,
    estimated_founder_hours: String,
    delivery_hours: String,
    unpaid_delivery_hours: String,
    gross_margin_percent: String,
    procurement_complexity: String,
    integration_complexity: String,
    delivery_risk: String,
    current_trigger: String,
    buyer_access: String,
    budget_path: String,
    budget_owner_status: String,
    champion_status: String,
    champion_strength: String,
    executive_sponsor_status: String,
    compelling_event: String,
    payment_structure: String,
    next_commitment: String,
    next_action: String,
    next_action_due_at: String,
    target_close_date: String,
    stalled_reason: String,
    estimate_basis: String,
    assessment_confidence: String,
}

async fn save_commercial_assessment(
    State(state): State<WebState>,
    Form(form): Form<CommercialAssessmentForm>,
) -> impl IntoResponse {
    let profile = match state.businesses.get(&form.brand) {
        Ok(profile) => profile,
        Err(error) => return (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    };
    if !form.offer_key.trim().is_empty() {
        let Some(offer) = profile.commercial_offer(form.offer_key.trim()) else {
            return (
                StatusCode::BAD_REQUEST,
                "offer is not defined for this business",
            )
                .into_response();
        };
        if offer.lane != form.commercial_lane {
            return (
                StatusCode::BAD_REQUEST,
                "offer lane does not match the selected commercial lane",
            )
                .into_response();
        }
    }
    let parsed = (|| -> Result<CommercialAssessment> {
        Ok(CommercialAssessment {
            sales_opportunity_id: form.sales_opportunity_id.trim().into(),
            brand: form.brand.trim().into(),
            commercial_lane: form.commercial_lane.trim().into(),
            offer_key: form.offer_key.trim().into(),
            sales_stage: form.sales_stage.trim().into(),
            expected_contract_value_cents: parse_optional_cad(&form.expected_contract_value_cad)?,
            expected_upfront_cash_cents: parse_optional_cad(&form.expected_upfront_cash_cad)?,
            cash_collectable_within_90d_cents: parse_optional_cad(
                &form.cash_collectable_within_90d_cad,
            )?,
            expected_arr_cents: parse_optional_cad(&form.expected_arr_cad)?,
            estimated_12m_gross_profit_cents: parse_optional_cad(
                &form.estimated_12m_gross_profit_cad,
            )?,
            days_to_first_cash: parse_optional_i64("days_to_first_cash", &form.days_to_first_cash)?,
            close_probability_bps: parse_optional_percent_bps(
                "close_probability_percent",
                &form.close_probability_percent,
            )?,
            sales_hours_remaining: parse_optional_i64(
                "sales_hours_remaining",
                &form.sales_hours_remaining,
            )?,
            estimated_founder_hours: parse_optional_i64(
                "estimated_founder_hours",
                &form.estimated_founder_hours,
            )?,
            delivery_hours: parse_optional_i64("delivery_hours", &form.delivery_hours)?,
            unpaid_delivery_hours: parse_optional_i64(
                "unpaid_delivery_hours",
                &form.unpaid_delivery_hours,
            )?,
            gross_margin_bps: parse_optional_percent_bps(
                "gross_margin_percent",
                &form.gross_margin_percent,
            )?,
            procurement_complexity: form.procurement_complexity.trim().into(),
            integration_complexity: form.integration_complexity.trim().into(),
            delivery_risk: form.delivery_risk.trim().into(),
            current_trigger: form.current_trigger.trim().into(),
            buyer_access: form.buyer_access.trim().into(),
            budget_path: form.budget_path.trim().into(),
            budget_owner_status: form.budget_owner_status.trim().into(),
            champion_status: form.champion_status.trim().into(),
            champion_strength: form.champion_strength.trim().into(),
            executive_sponsor_status: form.executive_sponsor_status.trim().into(),
            compelling_event: form.compelling_event.trim().into(),
            payment_structure: form.payment_structure.trim().into(),
            next_commitment: form.next_commitment.trim().into(),
            next_action: form.next_action.trim().into(),
            next_action_due_at: form.next_action_due_at.trim().into(),
            target_close_date: form.target_close_date.trim().into(),
            stalled_reason: form.stalled_reason.trim().into(),
            estimate_basis: form_list(&form.estimate_basis),
            assessment_source: "manual_crm".into(),
            assessment_confidence: form.assessment_confidence.trim().into(),
            ..Default::default()
        })
    })();
    match parsed.and_then(|assessment| state.db.upsert_commercial_assessment(&assessment)) {
        Ok(_) => Redirect::to(&format!("/gtm/{}", form.brand)).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct CommercialEventForm {
    brand: String,
    sales_opportunity_id: String,
    kind: String,
    amount_cad: String,
    occurred_at: String,
    external_ref: String,
    detail: String,
}

async fn save_commercial_event(
    State(state): State<WebState>,
    Form(form): Form<CommercialEventForm>,
) -> impl IntoResponse {
    let amount_cents = match parse_optional_cad(&form.amount_cad) {
        Ok(amount) => amount,
        Err(error) => return (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    };
    let event = CommercialEvent {
        sales_opportunity_id: form.sales_opportunity_id.trim().into(),
        brand: form.brand.trim().into(),
        kind: form.kind.trim().into(),
        amount_cents,
        currency: "CAD".into(),
        occurred_at: form.occurred_at.trim().into(),
        source: "manual_crm".into(),
        external_ref: form.external_ref.trim().into(),
        detail: form.detail.trim().into(),
        ..Default::default()
    };
    match state.db.record_commercial_event(&event) {
        Ok(_) => Redirect::to(&format!("/gtm/{}", form.brand)).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct CommercialOperatingStateForm {
    brand: String,
    runway_months: String,
    monthly_cash_need_cad: String,
    as_of: String,
}

async fn save_commercial_operating_state(
    State(state): State<WebState>,
    Form(form): Form<CommercialOperatingStateForm>,
) -> impl IntoResponse {
    let parsed = (|| -> Result<CommercialOperatingState> {
        Ok(CommercialOperatingState {
            brand: form.brand.trim().into(),
            runway_months: parse_optional_f64("runway_months", &form.runway_months)?,
            monthly_cash_need_cents: parse_optional_cad(&form.monthly_cash_need_cad)?,
            source: "manual_crm".into(),
            as_of: form.as_of.trim().into(),
            ..Default::default()
        })
    })();
    match parsed.and_then(|operating| {
        state.db.upsert_commercial_operating_state(&operating)?;
        Ok(())
    }) {
        Ok(()) => Redirect::to(&format!("/gtm/{}", form.brand)).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    }
}

fn parse_optional_i64(label: &str, raw: &str) -> Result<Option<i64>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    raw.parse::<i64>()
        .map(Some)
        .with_context(|| format!("{label} must be a whole number"))
}

fn parse_optional_f64(label: &str, raw: &str) -> Result<Option<f64>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    let value = raw
        .parse::<f64>()
        .with_context(|| format!("{label} must be a number"))?;
    if !value.is_finite() || value < 0.0 {
        anyhow::bail!("{label} must be a non-negative finite number");
    }
    Ok(Some(value))
}

fn parse_optional_percent_bps(label: &str, raw: &str) -> Result<Option<i64>> {
    let raw = raw.trim().trim_end_matches('%').trim();
    if raw.is_empty() {
        return Ok(None);
    }
    let percent = raw
        .parse::<f64>()
        .with_context(|| format!("{label} must be a percentage"))?;
    if !(0.0..=100.0).contains(&percent) {
        anyhow::bail!("{label} must be between 0 and 100");
    }
    Ok(Some((percent * 100.0).round() as i64))
}

fn parse_optional_cad(raw: &str) -> Result<Option<i64>> {
    let normalized = raw
        .trim()
        .trim_start_matches("CAD")
        .trim()
        .trim_start_matches('$')
        .replace(',', "");
    if normalized.is_empty() {
        return Ok(None);
    }
    let negative = normalized.starts_with('-');
    let normalized = normalized.trim_start_matches(['+', '-']);
    let mut parts = normalized.split('.');
    let dollars = parts
        .next()
        .unwrap_or("")
        .parse::<i64>()
        .context("CAD amount must contain whole dollars")?;
    let cents = match parts.next() {
        None | Some("") => 0,
        Some(fraction) if fraction.len() == 1 => fraction.parse::<i64>()? * 10,
        Some(fraction) if fraction.len() == 2 => fraction.parse::<i64>()?,
        Some(_) => anyhow::bail!("CAD amount may have at most two decimal places"),
    };
    if parts.next().is_some() {
        anyhow::bail!("CAD amount has too many decimal points");
    }
    let value = dollars.saturating_mul(100).saturating_add(cents);
    Ok(Some(if negative { -value } else { value }))
}

#[derive(Debug, Deserialize)]
struct CustomerDevelopmentForm {
    brand: String,
    lead_id: String,
    sales_opportunity_id: String,
    #[serde(default)]
    engaged: Option<String>,
    problem: String,
    task_scope: String,
    site: String,
    current_workflow: String,
    why_manual: String,
    variations: String,
    exceptions: String,
    evidence: String,
    economics: String,
    success_criteria: String,
    stop_condition: String,
    stakeholders: String,
    commitment_kind: String,
    commitment_detail: String,
    quantity: String,
    commercial_case: String,
    timeline: String,
    loi_conditions: String,
    next_action: String,
}

async fn save_customer_development(
    State(state): State<WebState>,
    Form(form): Form<CustomerDevelopmentForm>,
) -> impl IntoResponse {
    let lead_matches_brand = state
        .db
        .get_lead(&form.lead_id)
        .ok()
        .flatten()
        .is_some_and(|lead| lead.brand == form.brand);
    let opportunity_matches_scope = state
        .db
        .list_sales_opportunities(Some(&form.brand), Some(&form.lead_id))
        .is_ok_and(|opportunities| {
            opportunities
                .iter()
                .any(|opportunity| opportunity.id == form.sales_opportunity_id)
        });
    if !lead_matches_brand || !opportunity_matches_scope {
        return (
            StatusCode::BAD_REQUEST,
            "account/opportunity does not belong to this brand",
        )
            .into_response();
    }

    let mut record = state
        .db
        .customer_development_for_opportunity(&form.sales_opportunity_id)
        .ok()
        .flatten()
        .unwrap_or_else(|| CustomerDevelopmentRecord {
            brand: form.brand.clone(),
            lead_id: form.lead_id.clone(),
            sales_opportunity_id: form.sales_opportunity_id.clone(),
            ..Default::default()
        });
    let prior_stage = crate::gtm::customer_development_stage(&record).to_string();
    if form.engaged.is_some() && record.engaged_at.is_empty() {
        record.engaged_at = crate::db::now();
    }
    record.problem = form.problem.trim().to_string();
    record.task_scope = form.task_scope.trim().to_string();
    record.site = form.site.trim().to_string();
    record.current_workflow = form.current_workflow.trim().to_string();
    record.why_manual = form.why_manual.trim().to_string();
    record.variations = form_list(&form.variations);
    record.exceptions = form_list(&form.exceptions);
    record.evidence = form_list(&form.evidence);
    record.economics = form.economics.trim().to_string();
    record.success_criteria = form.success_criteria.trim().to_string();
    record.stop_condition = form.stop_condition.trim().to_string();
    record.stakeholders = form_list(&form.stakeholders);
    record.commitment_kind = crate::gtm::normalize_commitment_kind(&form.commitment_kind).into();
    record.commitment_detail = form.commitment_detail.trim().to_string();
    record.quantity = form.quantity.trim().to_string();
    record.commercial_case = form.commercial_case.trim().to_string();
    record.timeline = form.timeline.trim().to_string();
    record.loi_conditions = form.loi_conditions.trim().to_string();
    record.next_action = form.next_action.trim().to_string();
    record.stage = crate::gtm::customer_development_stage(&record).into();
    record.source = "manual_crm".into();

    match state.db.upsert_customer_development(&record) {
        Ok(_) => {
            if record.stage != prior_stage {
                let _ = state.db.record_gtm_outcome(&GtmOutcome {
                    brand: record.brand.clone(),
                    kind: "customer_development_stage".into(),
                    lead_id: record.lead_id.clone(),
                    person_id: record.person_id.clone(),
                    conversation_id: record.conversation_id.clone(),
                    value: crate::gtm::CUSTOMER_DEVELOPMENT_STAGES
                        .iter()
                        .position(|stage| stage.key == record.stage)
                        .unwrap_or(0) as f64,
                    detail: format!("{prior_stage} → {}", record.stage),
                    source: "manual_crm".into(),
                    fingerprint: format!(
                        "customer-development:{}:{}:{}",
                        record.lead_id, record.stage, record.updated_at
                    ),
                    ..Default::default()
                });
            }
            Redirect::to(&format!("/b/{}", form.brand)).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
    }
}

fn form_list(raw: &str) -> Vec<String> {
    raw.lines()
        .flat_map(|line| line.split(';'))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

async fn api(State(state): State<WebState>) -> Json<Crm> {
    let s = state.store.read().await;
    Json(s.data.clone())
}

async fn execution_api(State(state): State<WebState>) -> impl IntoResponse {
    match execution_dashboard(&state.db, None) {
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
    let mut brand = None;
    {
        let mut s = state.store.write().await;
        for ac in &mut s.data.accounts {
            for c in &mut ac.contacts {
                if c.id == contact {
                    brand = Some(ac.brand.clone());
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
    match brand {
        Some(brand) => Redirect::to(&format!("/b/{brand}")),
        None => Redirect::to("/"),
    }
}

async fn approve_execution(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(person_id): Path<String>,
) -> impl IntoResponse {
    let mut brand = None;
    if let Ok(Some(person)) = state.db.get_person(&person_id) {
        if let Ok(pb) = state.playbooks.get(&person.brand) {
            let _ = crate::outreach::approve_ready_touches(&state.db, pb, Some(&person_id));
        }
        if let Ok(profile) = state.businesses.get(&person.brand) {
            let _ = crate::calendar::rebalance_approved_sales(&state.db, profile, Utc::now());
        }
        brand = Some(person.brand);
    }
    redirect_back(&headers, brand.as_deref())
}

#[derive(Debug, Deserialize)]
struct LinkedinStatusForm {
    status: String,
}

async fn set_linkedin_status(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(person_id): Path<String>,
    Form(form): Form<LinkedinStatusForm>,
) -> impl IntoResponse {
    let brand = state
        .db
        .get_person(&person_id)
        .ok()
        .flatten()
        .map(|person| person.brand);
    let _ = state
        .db
        .set_person_linkedin_status(&person_id, &form.status);
    redirect_back(&headers, brand.as_deref())
}

async fn mark_touch_done(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(touch_id): Path<String>,
) -> impl IntoResponse {
    if let Ok(Some(touch)) = state.db.get_touch(&touch_id) {
        if touch.channel.eq_ignore_ascii_case("linkedin_request") {
            let _ = state
                .db
                .set_person_linkedin_status(&touch.person_id, "requested");
        }
    }
    let _ = state
        .db
        .set_touch_status(&touch_id, "sent", "", "", "manually completed");
    redirect_back(&headers, None)
}

async fn approve_opportunity_outreach(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(contact_id): Path<String>,
) -> impl IntoResponse {
    let mut brand = None;
    if let Ok(Some(contact)) = state.db.get_opportunity_contact(&contact_id) {
        let _ = state
            .db
            .approve_opportunity_touches(Some(&contact.brand), Some(&contact_id));
        brand = Some(contact.brand);
    }
    redirect_back(&headers, brand.as_deref())
}

/// Return the user to the brand CRM they acted from. Prefers the entity's own
/// brand; otherwise honours the `Referer` (every action is posted from a
/// `/b/:brand` page), falling back to the portfolio.
fn redirect_back(headers: &HeaderMap, brand: Option<&str>) -> Redirect {
    if let Some(brand) = brand {
        return Redirect::to(&format!("/b/{brand}"));
    }
    let referer = headers
        .get(axum::http::header::REFERER)
        .and_then(|value| value.to_str().ok())
        .filter(|referer| referer.contains("/b/"));
    match referer {
        Some(referer) => Redirect::to(referer),
        None => Redirect::to("/"),
    }
}

// --- HTML rendering --------------------------------------------------------

fn render_html(
    crm: &Crm,
    execution: Option<&ExecutionDashboard>,
    brand: Option<&BrandMeta>,
    tab_counts: &[(&'static BrandMeta, usize)],
    profile: Option<&BusinessProfile>,
) -> String {
    let mut b = String::new();
    let title = match brand {
        Some(meta) => format!("{} · Sales CRM", meta.name),
        None => "Sales CRM".to_string(),
    };
    b.push_str(&page_head(&title));

    render_topbar(
        &mut b,
        brand.map(|meta| meta.key),
        Surface::Pipeline,
        tab_counts,
    );

    // The execution DB is already brand-scoped by `execution_dashboard`, so the
    // only place that still needs filtering is the JSON research store.
    let live_accounts = execution
        .map(|dashboard| dashboard.accounts.as_slice())
        .unwrap_or_default();
    // The pipeline is a deliverable view. When the execution DB is available,
    // never fall back to research-store contacts merely because no full reviewed
    // sequence is ready yet.
    let use_live = execution.is_some();
    let research: Vec<CrmAccount> = crm
        .accounts
        .iter()
        .filter(|account| brand.is_none_or(|meta| account.brand == meta.key))
        .cloned()
        .collect();

    let account_count = if use_live {
        live_accounts.len()
    } else {
        research.len()
    };
    let contact_count = if use_live {
        live_accounts
            .iter()
            .map(|account| account.people.len().min(5))
            .sum::<usize>()
    } else {
        research
            .iter()
            .map(|account| account.contacts.len().min(5))
            .sum::<usize>()
    };
    let scheduled_count = if use_live {
        live_accounts
            .iter()
            .flat_map(|account| &account.people)
            .flat_map(|entry| &entry.touches)
            .filter(|touch| touch.status == "scheduled")
            .count()
    } else {
        0
    };

    let (heading, tagline) = match (brand, profile) {
        (Some(meta), Some(profile)) => (meta.name.to_string(), profile.summary.clone()),
        (Some(meta), None) => (meta.name.to_string(), meta.tagline.to_string()),
        (None, _) => (
            "All businesses".to_string(),
            "Wapahki, GnK and OutageHub — three books of business, one sheet.".to_string(),
        ),
    };
    let stats = if let Some(dashboard) = execution {
        vec![
            (account_count.to_string(), "reviewed + GTM-ready companies"),
            (dashboard.mapped_contacts.to_string(), "mapped contacts"),
            (contact_count.to_string(), "reviewed sequences"),
            (scheduled_count.to_string(), "scheduled touches"),
        ]
    } else {
        vec![
            (account_count.to_string(), "companies"),
            (contact_count.to_string(), "people"),
            (scheduled_count.to_string(), "scheduled"),
        ]
    };
    render_subbar(&mut b, &heading, &tagline, &stats);

    if let (Some(meta), Some(profile)) = (brand, profile) {
        render_strategy_strip(&mut b, meta, profile);
    }
    b.push_str("<main class=\"sheet-scroll\" id=\"pipeline\">");
    if let Some(dashboard) = execution {
        render_outcome_strip(&mut b, dashboard, brand);
    }
    if brand.is_some_and(|meta| meta.key == "wapahki") {
        if let Some(dashboard) = execution {
            render_customer_development(&mut b, dashboard);
        }
    }
    if use_live {
        render_people_sheet(&mut b, live_accounts);
    } else if !research.is_empty() {
        render_research_sheet(&mut b, &research);
    } else {
        render_empty_sheet(&mut b);
    }
    b.push_str(
        "</main><script>
        (() => {
          let refreshing = false;
          async function refreshPipeline() {
            if (refreshing || document.hidden) return;
            refreshing = true;
            try {
              const response = await fetch(location.href, {cache: 'no-store'});
              if (!response.ok) return;
              const incoming = new DOMParser().parseFromString(await response.text(), 'text/html');
              const currentSheet = document.querySelector('.sheet-scroll');
              const nextSheet = incoming.querySelector('.sheet-scroll');
              if (currentSheet && nextSheet && currentSheet.innerHTML !== nextSheet.innerHTML) {
                const left = currentSheet.scrollLeft;
                const top = currentSheet.scrollTop;
                const openItems = [...currentSheet.querySelectorAll('details[data-open-id][open]')]
                  .map(details => details.dataset.openId);
                currentSheet.innerHTML = nextSheet.innerHTML;
                currentSheet.scrollLeft = left;
                currentSheet.scrollTop = top;
                for (const id of openItems) {
                  const details = currentSheet.querySelector('details[data-open-id=' + CSS.escape(id) + ']');
                  if (details) details.open = true;
                }
              }
              const currentStats = document.querySelector('.subbar-stats');
              const nextStats = incoming.querySelector('.subbar-stats');
              if (currentStats && nextStats && currentStats.innerHTML !== nextStats.innerHTML) {
                currentStats.innerHTML = nextStats.innerHTML;
              }
            } catch (_) {
              // The next poll retries; a transient localhost restart is harmless.
            } finally {
              refreshing = false;
            }
          }
          setInterval(refreshPipeline, 3000);
        })();
        </script></div></body></html>",
    );
    b
}

fn render_outcome_strip(b: &mut String, dashboard: &ExecutionDashboard, brand: Option<&BrandMeta>) {
    let mut approvals = 0usize;
    let mut social_tasks = 0usize;
    for entry in dashboard
        .accounts
        .iter()
        .flat_map(|account| &account.people)
    {
        for touch in &entry.touches {
            if touch.status != "draft" || touch.review_passes != Some(true) {
                continue;
            }
            let email = touch.channel.eq_ignore_ascii_case("email")
                || (touch.channel.eq_ignore_ascii_case("linkedin_or_email")
                    && entry.person.linkedin_status != "connected");
            let social = touch.channel.eq_ignore_ascii_case("linkedin")
                || touch.channel.eq_ignore_ascii_case("linkedin_request")
                || (touch.channel.eq_ignore_ascii_case("linkedin_or_email")
                    && entry.person.linkedin_status == "connected");
            approvals += usize::from(email);
            social_tasks += usize::from(social);
        }
    }
    let gtm_href = brand
        .map(|meta| format!("/gtm/{}", meta.key))
        .unwrap_or_else(|| "/gtm".to_string());
    b.push_str(&format!(
        "<section class=\"outcome-strip\" aria-label=\"Next best work\"><div class=\"outcome-intro\">\
         <span class=\"strategy-kicker\">Next best work</span><strong>Start conversations → create meetings → advance proof</strong>\
         <small>{mapped} contacts are mapped across ready accounts; this sheet shows every current reviewed recipient sequence. Research, held accounts, and rejected copy stay in GTM Lab.</small></div>\
         <a class=\"outcome-card\" href=\"#pipeline\"><b>{approvals}</b><span>email drafts</span><small>approve these conversation steps</small></a>\
         <a class=\"outcome-card\" href=\"#pipeline\"><b>{social}</b><span>LinkedIn actions</span><small>complete the manual channel work</small></a>\
         <a class=\"outcome-card\" href=\"#pipeline\"><b>{replies}</b><span>recent replies</span><small>route, answer, or ask for the meeting</small></a>\
         <a class=\"outcome-card meeting\" href=\"#pipeline\"><b>{meetings}</b><span>meetings</span><small>prepare and capture next commitments</small></a>\
         <a class=\"outcome-card\" href=\"{gtm}\"><b>{pursuits}</b><span>active pursuits</span><small>move evidence into a proof or application</small></a>\
         </section>",
        approvals = approvals,
        mapped = dashboard.mapped_contacts,
        social = social_tasks,
        replies = dashboard.replies.len(),
        meetings = dashboard.meetings.len(),
        pursuits = dashboard.funnel.opportunities_active,
        gtm = esc(&gtm_href),
    ));
}

fn reviewable_sponsorship_draft_count(dashboard: &ExecutionDashboard) -> usize {
    dashboard
        .opportunities
        .iter()
        .filter(|entry| entry.opportunity.kind == "sponsorship")
        .flat_map(|entry| &entry.contacts)
        .flat_map(|entry| &entry.touches)
        .filter(|touch| touch.status == "draft" && touch.review_passes == Some(true))
        .count()
}

fn render_sponsorship_page(
    dashboard: Option<&ExecutionDashboard>,
    audit: Option<&crate::opportunity::SponsorshipCampaignAudit>,
    counts: &[(&'static BrandMeta, usize)],
) -> String {
    let mut b = page_head("OutageHub Sponsorship · Sales CRM");
    render_topbar(&mut b, Some("outagehub"), Surface::Sponsorship, counts);
    let ready = dashboard
        .map(reviewable_sponsorship_draft_count)
        .unwrap_or(0);
    let blocked = dashboard.map_or(0, |dashboard| {
        dashboard
            .opportunities
            .iter()
            .filter(|entry| entry.opportunity.kind == "sponsorship")
            .flat_map(|entry| &entry.contacts)
            .flat_map(|entry| &entry.touches)
            .filter(|touch| touch.status == "blocked")
            .count()
    });
    let scheduled = dashboard.map_or(0, |dashboard| {
        dashboard
            .opportunities
            .iter()
            .filter(|entry| entry.opportunity.kind == "sponsorship")
            .flat_map(|entry| &entry.contacts)
            .flat_map(|entry| &entry.touches)
            .filter(|touch| matches!(touch.status.as_str(), "scheduled" | "sending" | "sent"))
            .count()
    });
    render_subbar(
        &mut b,
        "OutageHub Sponsorship",
        "One paid CAD $10,000 founding sponsor. Every email is shown in full for manual review and is excluded from the delivery queue.",
        &[
            ((ready + blocked).to_string(), "researched companies"),
            (ready.to_string(), "ready drafts"),
            (blocked.to_string(), "blocked drafts"),
            (scheduled.to_string(), "scheduled or sent"),
        ],
    );
    b.push_str("<main class=\"sheet-scroll\" id=\"sponsorship-drafts\">");
    if let Some(audit) = audit {
        b.push_str(&format!(
            "<section class=\"campaign-audit {}\"><b>Campaign QA: {}</b><span>{}/{} organizations · {} ready · {} blocked · {} direct mailboxes · {} routed inboxes · {} scheduled/sending/sent</span>{}</section>",
            if audit.passes() { "pass" } else { "hold" },
            if audit.passes() { "PASS" } else { "HOLD" },
            audit.organizations,
            audit.target,
            audit.ready,
            audit.blocked,
            audit.direct_mailboxes,
            audit.routed_mailboxes,
            audit.scheduled_or_sent,
            if audit.issues.is_empty() {
                String::new()
            } else {
                format!(
                    "<ul>{}</ul>",
                    audit
                        .issues
                        .iter()
                        .map(|issue| format!("<li>{}</li>", esc(issue)))
                        .collect::<String>()
                )
            },
        ));
    }
    if let Some(dashboard) = dashboard {
        render_sponsorship_table(&mut b, &dashboard.opportunities);
    } else {
        b.push_str("<div class=\"empty-sheet\"><strong>Sponsorship CRM unavailable</strong><span>Refresh after the local execution database reconnects.</span></div>");
    }
    b.push_str(
        "</main><script>(()=>{let busy=false;setInterval(async()=>{if(busy||document.hidden)return;busy=true;try{const response=await fetch(location.href,{cache:'no-store'});if(!response.ok)return;const incoming=new DOMParser().parseFromString(await response.text(),'text/html');const current=document.querySelector('.sheet-scroll');const next=incoming.querySelector('.sheet-scroll');if(current&&next&&current.innerHTML!==next.innerHTML){const left=current.scrollLeft,top=current.scrollTop;current.innerHTML=next.innerHTML;current.scrollLeft=left;current.scrollTop=top}const stats=document.querySelector('.subbar-stats');const nextStats=incoming.querySelector('.subbar-stats');if(stats&&nextStats&&stats.innerHTML!==nextStats.innerHTML)stats.innerHTML=nextStats.innerHTML}catch(_){ }finally{busy=false}},3000)})();</script></div></body></html>",
    );
    b
}

fn render_sponsorship_table(b: &mut String, opportunities: &[ExecutionOpportunity]) {
    let mut rows = opportunities
        .iter()
        .filter(|entry| entry.opportunity.kind == "sponsorship")
        .flat_map(|entry| {
            entry.contacts.iter().flat_map(move |contact| {
                contact
                    .touches
                    .iter()
                    .map(move |touch| (&entry.opportunity, &contact.contact, touch))
            })
        })
        .collect::<Vec<_>>();
    if rows.is_empty() {
        b.push_str("<div class=\"empty-sheet\"><strong>No sponsorship drafts yet</strong><span>Verified sponsor research will appear here after generation and review.</span></div>");
        return;
    }
    rows.sort_by(|left, right| {
        let status_rank = |status: &str| match status {
            "draft" => 0,
            "blocked" => 1,
            _ => 2,
        };
        status_rank(&left.2.status)
            .cmp(&status_rank(&right.2.status))
            .then_with(|| left.0.funder.cmp(&right.0.funder))
    });
    b.push_str("<table class=\"crm-sheet sponsorship-sheet\"><colgroup><col class=\"c-company\"><col class=\"c-sponsor-evidence\"><col class=\"c-person\"><col class=\"c-sponsor-subject\"><col class=\"c-sponsor-email\"><col class=\"c-sponsor-qa\"></colgroup><thead><tr><th class=\"pin\">Company</th><th>Verified relevance + budget route</th><th>Recipient</th><th>Subject</th><th>Full email</th><th>QA / delivery state</th></tr></thead><tbody>");
    for (opportunity, contact, touch) in rows {
        let ready = touch.status == "draft" && touch.review_passes == Some(true);
        let state = if ready { "ready" } else { "blocked" };
        b.push_str(&format!(
            "<tr class=\"sponsor-row {state}\"><td class=\"company pin\"><span class=\"brand-tag outagehub\">OutageHub</span><strong>{company}</strong><small>CAD $10,000 founding sponsorship</small><a href=\"{url}\" rel=\"noreferrer\">Primary source ↗</a></td><td class=\"sponsor-evidence\"><ul>{evidence}</ul></td><td class=\"person\"><strong>{name}</strong><small>{title}</small><a class=\"email\" href=\"mailto:{email}\">{email}</a><span class=\"person-status verified\">{email_status}</span><span class=\"person-status\">{route}</span><p>{why}</p></td><td class=\"sponsor-subject\"><span class=\"subject\">{subject}</span></td><td class=\"sponsor-message\"><div class=\"message\">{body}</div></td><td class=\"sponsor-state\"><span class=\"touch-tag\">{state}</span>{qa}<small>Delivery: technically blocked from scheduling and sending</small></td></tr>",
            state = state,
            company = esc(&opportunity.funder),
            name = esc(&contact.name),
            title = esc(&contact.title),
            email = esc(&contact.email),
            email_status = esc(&contact.email_status),
            route = if crate::opportunity::sponsorship_contact_is_direct(contact) {
                "direct mailbox"
            } else {
                "shared routing inbox"
            },
            why = esc(&contact.why_them),
            evidence = opportunity
                .evidence
                .iter()
                .map(|item| format!("<li>{}</li>", esc(item)))
                .collect::<String>(),
            url = esc(&opportunity.canonical_url),
            subject = esc(&touch.subject),
            body = esc_multiline(&touch.body),
            qa = if touch.review_issues.is_empty() {
                "<p class=\"sponsor-qa ok\">Passed deterministic QA and both semantic audits. Manual review still required.</p>".to_string()
            } else {
                format!(
                    "<p class=\"sponsor-qa fail\"><b>Held:</b> {}</p>",
                    esc(&touch.review_issues.join(" · "))
                )
            },
        ));
    }
    b.push_str("</tbody></table>");
}

fn render_customer_development(b: &mut String, dashboard: &ExecutionDashboard) {
    let stage_for_opportunity = |sales_opportunity_id: &str| {
        dashboard
            .customer_development
            .iter()
            .find(|record| record.sales_opportunity_id == sales_opportunity_id)
            .map(crate::gtm::customer_development_stage)
            .unwrap_or("hypothesis")
    };
    b.push_str(
        "<section class=\"customer-dev\"><div class=\"customer-dev-head\"><div>\
         <span class=\"strategy-kicker\">Pre-product customer development</span>\
         <h2>From task hypothesis to conditional LOI</h2>\
         <p>Email sends are activity. These stages advance only when a plant supplies evidence or makes an explicit commitment of time, reputation, or money.</p>\
         </div><a href=\"/gtm/wapahki\">Evidence &amp; proof lab →</a></div><div class=\"customer-dev-ladder\">",
    );
    for stage in crate::gtm::CUSTOMER_DEVELOPMENT_STAGES {
        let count = dashboard
            .sales_opportunities
            .iter()
            .filter(|opportunity| stage_for_opportunity(&opportunity.id) == stage.key)
            .count();
        b.push_str(&format!(
            "<div class=\"customer-dev-rung\"><strong>{}</strong><span>{}</span></div>",
            count,
            esc(stage.label),
        ));
    }
    b.push_str("</div><div class=\"customer-dev-accounts\">");

    for opportunity in &dashboard.sales_opportunities {
        let account_name = dashboard
            .leads
            .iter()
            .find(|lead| lead.id == opportunity.lead_id)
            .map(|lead| lead.name.as_str())
            .unwrap_or("Unknown account");
        let fallback = CustomerDevelopmentRecord {
            brand: opportunity.brand.clone(),
            lead_id: opportunity.lead_id.clone(),
            sales_opportunity_id: opportunity.id.clone(),
            commitment_kind: "none".into(),
            ..Default::default()
        };
        let record = dashboard
            .customer_development
            .iter()
            .find(|record| record.sales_opportunity_id == opportunity.id)
            .unwrap_or(&fallback);
        let stage = crate::gtm::customer_development_stage_info(record);
        let missing = crate::gtm::customer_development_missing(record);
        let updated_label = if record.updated_at.is_empty() {
            "No discovery evidence recorded yet".to_string()
        } else {
            format!(
                "Updated {}",
                display_written_at(&record.updated_at).trim_start_matches("Drafted ")
            )
        };
        b.push_str(&format!(
            "<details class=\"customer-dev-account\"><summary><div><span class=\"customer-dev-stage {}\">{}</span>\
             <strong>{}</strong><small>{}</small></div><span class=\"customer-dev-next\">Next: {}</span></summary>\
             <div class=\"customer-dev-body\"><div class=\"customer-dev-gate\"><div><b>Evidence for this stage</b><p>{}</p></div>\
             <div><b>Next commitment</b><p>{}</p></div><div><b>Still missing</b><p>{}</p></div></div>",
            esc(stage.key),
            esc(stage.label),
            esc(account_name),
            esc(&updated_label),
            esc(if record.next_action.trim().is_empty() {
                stage.next_commitment
            } else {
                &record.next_action
            }),
            esc(stage.proof),
            esc(stage.next_commitment),
            esc(&missing.join(" · ")),
        ));
        b.push_str(&format!(
            "<form method=\"post\" action=\"/customer-development\" class=\"customer-dev-form\">\
             <input type=\"hidden\" name=\"brand\" value=\"{}\"><input type=\"hidden\" name=\"lead_id\" value=\"{}\"><input type=\"hidden\" name=\"sales_opportunity_id\" value=\"{}\">\
             <label class=\"customer-dev-check\"><input type=\"checkbox\" name=\"engaged\" value=\"yes\" {}> Human reply, correction, referral, or discovery conversation recorded</label>\
             <label>Prospect-confirmed problem<textarea name=\"problem\" placeholder=\"Their words, not our inference\">{}</textarea></label>\
             <label>Bounded task / motion<textarea name=\"task_scope\" placeholder=\"Object, from where, to where\">{}</textarea></label>\
             <label>Current workflow<textarea name=\"current_workflow\">{}</textarea></label>\
             <label>Why it remains manual<textarea name=\"why_manual\" placeholder=\"What was tried; what breaks or changes\">{}</textarea></label>\
             <label>Variation between runs<textarea name=\"variations\" placeholder=\"One item per line\">{}</textarea></label>\
             <label>Exceptions during a run<textarea name=\"exceptions\" placeholder=\"One item per line\">{}</textarea></label>\
             <label class=\"wide\">Evidence the customer shared<textarea name=\"evidence\" placeholder=\"Video, task sketch, SKU/changeover data, rates, site observation — one per line\">{}</textarea></label>\
             <label class=\"wide\">Task economics<textarea name=\"economics\" placeholder=\"Operators, shifts, loaded wage, rate, changeover, intervention frequency, payback constraint\">{}</textarea></label>\
             <label>Evaluation success criteria<textarea name=\"success_criteria\" placeholder=\"Example: 10 cases/min across 5 SKUs with &lt;1% intervention\">{}</textarea></label>\
             <label>Technical/economic stop condition<textarea name=\"stop_condition\">{}</textarea></label>\
             <label>Stakeholders<textarea name=\"stakeholders\" placeholder=\"Champion, operator, engineering, quality, economic buyer — one per line\">{}</textarea></label>\
             <label>Next concrete action<textarea name=\"next_action\" placeholder=\"Owner + action + date\">{}</textarea></label>",
            esc(&record.brand),
            esc(&record.lead_id),
            esc(&record.sales_opportunity_id),
            if record.engaged_at.is_empty() { "" } else { "checked" },
            esc(&record.problem),
            esc(&record.task_scope),
            esc(&record.current_workflow),
            esc(&record.why_manual),
            esc(&record.variations.join("\n")),
            esc(&record.exceptions.join("\n")),
            esc(&record.evidence.join("\n")),
            esc(&record.economics),
            esc(&record.success_criteria),
            esc(&record.stop_condition),
            esc(&record.stakeholders.join("\n")),
            esc(&record.next_action),
        ));
        b.push_str(&format!(
            "<details class=\"customer-dev-commercial wide\"><summary>Commercial commitment / LOI fields</summary><div>\
             <label>Plant / site<input name=\"site\" value=\"{}\"></label>\
             <label>Provisional quantity<input name=\"quantity\" value=\"{}\" placeholder=\"Cells / lines / sites\"></label>\
             <label>Price range / payback case<textarea name=\"commercial_case\">{}</textarea></label>\
             <label>Decision / deployment timeline<textarea name=\"timeline\">{}</textarea></label>\
             <label class=\"wide\">Conditions to purchase / LOI terms<textarea name=\"loi_conditions\" placeholder=\"Pilot criteria, task/site, quantity, commercial assumption, sponsor, timeline\">{}</textarea></label>\
             <label>Highest explicit commitment<select name=\"commitment_kind\">{}</select></label>\
             <label>Evidence of that commitment<textarea name=\"commitment_detail\" placeholder=\"What they agreed, who agreed, and where it is documented\">{}</textarea></label>\
             </div></details><button type=\"submit\">Save discovery evidence</button></form></div></details>",
            esc(&record.site),
            esc(&record.quantity),
            esc(&record.commercial_case),
            esc(&record.timeline),
            esc(&record.loi_conditions),
            commitment_options(&record.commitment_kind),
            esc(&record.commitment_detail),
        ));
    }
    if dashboard.sales_opportunities.is_empty() {
        b.push_str(
            "<div class=\"customer-dev-empty\"><strong>No Wapahki accounts yet</strong><span>Source a small set of plants around one concrete task hypothesis. Each account will start at Hypothesis; replies and saved evidence move it forward.</span></div>",
        );
    }
    b.push_str("</div></section>");
}

fn commitment_options(current: &str) -> String {
    [
        ("none", "None — discovery only"),
        ("evaluation_agreed", "Evaluation agreed"),
        ("design_partner", "Design partner"),
        ("loi_candidate", "LOI candidate"),
        ("conditional_loi", "Conditional LOI signed"),
        ("paid_pilot", "Paid pilot"),
        ("deployment", "Deployment"),
    ]
    .into_iter()
    .map(|(value, label)| {
        format!(
            "<option value=\"{}\" {}>{}</option>",
            value,
            if crate::gtm::normalize_commitment_kind(current) == value {
                "selected"
            } else {
                ""
            },
            esc(label)
        )
    })
    .collect()
}

/// Shared document head + open the CRM shell. Every page (portfolio and each
/// brand) uses the same chrome so switching brands feels like one app.
fn page_head(title: &str) -> String {
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\">\
         <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
         <title>{}</title>{}</head><body><div class=\"crm-shell\">",
        esc(title),
        SHEET_STYLE
    )
}

/// The persistent top bar: the SalesOS lockup, surface switch (Pipeline /
/// Strategy), then one clickable tab per brand. `active` is the brand currently
/// shown, or `None` on the portfolio hub, which lights the "All" tab instead.
fn render_topbar(
    b: &mut String,
    active: Option<&str>,
    surface: Surface,
    counts: &[(&'static BrandMeta, usize)],
) {
    b.push_str(
        "<header class=\"topbar\"><a class=\"brand-lockup\" href=\"/\">\
         <span class=\"mark\">↗</span>\
         <span class=\"wordmark\">Sales<span class=\"wordmark-dim\">OS</span></span></a>\
         <nav class=\"surface-tabs\">",
    );
    let pipeline_href = match active {
        Some(brand) => format!("/b/{brand}"),
        None => "/".to_string(),
    };
    let strategy_href = match active {
        Some(brand) => format!("/strategy/{brand}"),
        None => "/strategy".to_string(),
    };
    let gtm_href = match active {
        Some(brand) => format!("/gtm/{brand}"),
        None => "/gtm".to_string(),
    };
    b.push_str(&format!(
        "<a class=\"surface-tab{}\" href=\"{}\">Pipeline</a>\
         <a class=\"surface-tab{}\" href=\"{}\">Strategy</a>\
         <a class=\"surface-tab{}\" href=\"{}\">GTM Lab</a>",
        if surface == Surface::Pipeline {
            " active"
        } else {
            ""
        },
        esc(&pipeline_href),
        if surface == Surface::Strategy {
            " active"
        } else {
            ""
        },
        esc(&strategy_href),
        if surface == Surface::Gtm {
            " active"
        } else {
            ""
        },
        esc(&gtm_href),
    ));
    if active == Some("outagehub") || surface == Surface::Sponsorship {
        b.push_str(&format!(
            "<a class=\"surface-tab{}\" href=\"/b/outagehub/sponsorship\">OutageHub Sponsorship</a>",
            if surface == Surface::Sponsorship {
                " active"
            } else {
                ""
            },
        ));
    }
    b.push_str("</nav><nav class=\"biz-tabs\">");
    let all_total: usize = counts.iter().map(|(_, contacts)| contacts).sum();
    let all_href = match surface {
        Surface::Strategy => "/strategy",
        Surface::Gtm => "/gtm",
        Surface::Sponsorship => "/",
        Surface::Pipeline => "/",
    };
    b.push_str(&format!(
        "<a class=\"biz-tab{}\" href=\"{}\">All<span class=\"count\">{}</span></a>",
        if active.is_none() { " active" } else { "" },
        all_href,
        all_total,
    ));
    for (meta, contacts) in counts {
        let href = match surface {
            Surface::Strategy => format!("/strategy/{}", meta.key),
            Surface::Gtm => format!("/gtm/{}", meta.key),
            Surface::Sponsorship => format!("/b/{}", meta.key),
            Surface::Pipeline => format!("/b/{}", meta.key),
        };
        b.push_str(&format!(
            "<a class=\"biz-tab {brand}{active}\" href=\"{href}\">{name}<span class=\"count\">{contacts}</span></a>",
            brand = meta.key,
            active = if active == Some(meta.key) { " active" } else { "" },
            name = esc(meta.name),
            href = href,
        ));
    }
    b.push_str("</nav></header>");
}

/// The sub bar under the tabs: the active view's title, one-line description,
/// and a few headline stats on the right.
fn render_subbar(b: &mut String, title: &str, tagline: &str, stats: &[(String, &str)]) {
    b.push_str(&format!(
        "<section class=\"subbar\"><div class=\"subbar-left\"><h1>{}</h1><p class=\"tagline\">{}</p></div>\
         <div class=\"subbar-stats\">",
        esc(title),
        esc(tagline),
    ));
    for (value, label) in stats {
        b.push_str(&format!(
            "<div class=\"stat\"><div class=\"n\">{}</div><div class=\"l\">{}</div></div>",
            esc(value),
            esc(label),
        ));
    }
    b.push_str("</div></section>");
}

/// The portfolio landing page: the brand tabs plus one card per brand, each a
/// direct link into that brand's CRM.
fn render_hub(
    counts: &[(&'static BrandMeta, usize)],
    businesses: &Businesses,
    db: &SharedDb,
) -> String {
    let mut b = page_head("Sales CRM · Outreach calendar");
    render_topbar(&mut b, None, Surface::Pipeline, counts);
    let mut all_entries = Vec::new();
    for (meta, _) in counts {
        if let Ok(mut entries) = db.upcoming_calendar(meta.key, 600) {
            all_entries.append(&mut entries);
        }
    }
    all_entries.sort_by(|left, right| left.due_at.cmp(&right.due_at));
    let now = Utc::now();
    let overdue = all_entries
        .iter()
        .filter(|entry| parse_calendar_due(&entry.due_at).is_some_and(|due| due < now))
        .count();
    let scheduled_people = all_entries
        .iter()
        .map(|entry| format!("{}:{}:{}", entry.brand, entry.account, entry.recipient))
        .collect::<BTreeSet<_>>()
        .len();
    let total_cap: usize = counts
        .iter()
        .filter_map(|(meta, _)| businesses.get(meta.key).ok())
        .map(|profile| profile.calendar.daily_touch_cap)
        .sum();
    render_subbar(
        &mut b,
        "Outreach calendar",
        "Approved emails across all three businesses. Follow-ups are protected; remaining capacity opens new conversations across accounts.",
        &[
            (format!("{total_cap}/day"), "portfolio ceiling"),
            (scheduled_people.to_string(), "people scheduled"),
            (overdue.to_string(), "overdue"),
        ],
    );
    b.push_str("<main class=\"sheet-scroll calendar-scroll\">");
    b.push_str(
        "<section class=\"calendar-policy\"><div><b>Portfolio rule</b><span>30 emails per business per quota day. Replies first, then due follow-ups, then new people breadth-first across accounts. A new person enters only after the rest of their cadence has calendar room.</span></div><span class=\"calendar-policy-total\">90 max/day</span></section>",
    );
    if overdue > 0 {
        b.push_str(&format!(
            "<section class=\"calendar-alert\"><b>{overdue} approved email{} overdue.</b> The daemon will take replies and follow-ups first, then move any overflow into the next valid recipient window.</section>",
            if overdue == 1 { " is" } else { "s are" }
        ));
    }

    b.push_str("<section class=\"calendar-brand-strip\">");
    for (meta, contacts) in counts {
        b.push_str(&format!(
            "<a class=\"calendar-brand-summary {brand}\" href=\"/b/{brand}\"><span class=\"brand-chip {brand}\">{name}</span><span><b>30/day</b><small>{contacts} reviewable contact{suffix}</small></span><i>Open pipeline →</i></a>",
            brand = meta.key,
            name = esc(meta.name),
            suffix = if *contacts == 1 { "" } else { "s" },
        ));
    }
    b.push_str("</section>");

    let dates = portfolio_calendar_dates(businesses, &all_entries, now, 10);
    b.push_str("<section class=\"calendar-grid\">");
    for date in dates {
        render_calendar_day(&mut b, date, counts, businesses, db, &all_entries, now);
    }
    b.push_str("</section></main></div></body></html>");
    b
}

fn portfolio_calendar_dates(
    businesses: &Businesses,
    entries: &[crate::db::CalendarEntry],
    now: DateTime<Utc>,
    count: usize,
) -> Vec<NaiveDate> {
    let tz = businesses
        .get("gnk")
        .ok()
        .and_then(|profile| profile.calendar.quota_timezone.parse::<Tz>().ok())
        .unwrap_or(chrono_tz::Europe::London);
    let mut date = now.with_timezone(&tz).date_naive();
    let entry_dates = entries
        .iter()
        .filter_map(|entry| {
            let profile = businesses.get(&entry.brand).ok()?;
            let tz = profile.calendar.quota_timezone.parse::<Tz>().ok()?;
            Some(
                parse_calendar_due(&entry.due_at)?
                    .with_timezone(&tz)
                    .date_naive(),
            )
        })
        .collect::<BTreeSet<_>>();
    let mut dates = Vec::new();
    while dates.len() < count {
        let weekday = date.weekday();
        if !matches!(weekday, Weekday::Sat | Weekday::Sun) || entry_dates.contains(&date) {
            dates.push(date);
        }
        date = match date.succ_opt() {
            Some(next) => next,
            None => break,
        };
    }
    dates
}

fn render_calendar_day(
    b: &mut String,
    date: NaiveDate,
    counts: &[(&'static BrandMeta, usize)],
    businesses: &Businesses,
    db: &SharedDb,
    entries: &[crate::db::CalendarEntry],
    now: DateTime<Utc>,
) {
    let today = businesses
        .get("gnk")
        .ok()
        .and_then(|profile| profile.calendar.quota_timezone.parse::<Tz>().ok())
        .map(|tz| now.with_timezone(&tz).date_naive() == date)
        .unwrap_or(false);
    let mut total = 0usize;
    let mut brand_rows = Vec::new();
    for (meta, _) in counts {
        let Some(profile) = businesses.get(meta.key).ok() else {
            continue;
        };
        let (used, sent) = crate::calendar::quota_date_bounds(profile, date)
            .ok()
            .and_then(|(start, end)| {
                Some((
                    db.planned_touch_count_between(meta.key, start, end).ok()?,
                    db.sent_touch_count_between(meta.key, start, end).ok()?,
                ))
            })
            .unwrap_or_default();
        total += used;
        let mut day_entries = entries
            .iter()
            .filter(|entry| entry.brand == meta.key)
            .filter(|entry| {
                let tz = profile
                    .calendar
                    .quota_timezone
                    .parse::<Tz>()
                    .unwrap_or(chrono_tz::Europe::London);
                parse_calendar_due(&entry.due_at)
                    .is_some_and(|due| due.with_timezone(&tz).date_naive() == date)
            })
            .collect::<Vec<_>>();
        day_entries.sort_by(|left, right| left.due_at.cmp(&right.due_at));
        brand_rows.push((meta, profile, used, sent, day_entries));
    }
    b.push_str(&format!(
        "<article class=\"calendar-day{}\"><header><div><span>{}</span><b>{}</b></div><strong>{}/90</strong></header>",
        if today { " today" } else { "" },
        esc(&date.format("%a").to_string()),
        esc(&date.format("%-d %b").to_string()),
        total.min(90),
    ));
    for (meta, profile, used, sent, day_entries) in brand_rows {
        let cap = profile.calendar.daily_touch_cap.max(1);
        let width = ((used.min(cap) * 100) / cap).max(usize::from(used > 0) * 3);
        b.push_str(&format!(
            "<div class=\"calendar-lane {brand}\"><div class=\"calendar-lane-head\"><a href=\"/b/{brand}\">{name}</a><span>{used}/{cap}<small>{sent} sent</small></span></div><div class=\"calendar-meter\"><i style=\"width:{width}%\"></i></div>",
            brand = meta.key,
            name = esc(meta.name),
        ));
        if day_entries.is_empty() {
            b.push_str("<p class=\"calendar-open\">capacity available</p>");
        } else {
            b.push_str("<div class=\"calendar-events\">");
            for entry in day_entries.iter().take(3) {
                b.push_str(&format!(
                    "<div class=\"calendar-event\"><time>{}</time><span><b>{}</b> · {} · T{}</span></div>",
                    esc(&calendar_entry_time(entry)),
                    esc(&entry.account),
                    esc(&entry.recipient),
                    entry.stage,
                ));
            }
            if day_entries.len() > 3 {
                b.push_str(&format!(
                    "<p class=\"calendar-more\">+{} more approved email{}</p>",
                    day_entries.len() - 3,
                    if day_entries.len() == 4 { "" } else { "s" }
                ));
            }
            b.push_str("</div>");
        }
        b.push_str("</div>");
    }
    b.push_str("</article>");
}

fn parse_calendar_due(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|due| due.with_timezone(&Utc))
}

fn calendar_entry_time(entry: &crate::db::CalendarEntry) -> String {
    let Some(due) = parse_calendar_due(&entry.due_at) else {
        return "time pending".into();
    };
    let tz = entry
        .recipient_timezone
        .parse::<Tz>()
        .unwrap_or(chrono_tz::UTC);
    due.with_timezone(&tz).format("%-I:%M %p %Z").to_string()
}

/// Compact business-context strip on each brand pipeline page so the sheet never
/// feels like a pure contact list disconnected from the commercial goal.
fn render_strategy_strip(b: &mut String, meta: &BrandMeta, profile: &BusinessProfile) {
    let goal = profile
        .goals
        .first()
        .map(String::as_str)
        .unwrap_or(meta.tagline);
    let motions = profile
        .motions
        .iter()
        .filter(|m| m.enabled)
        .map(|m| format!("{} · {}", m.key.replace('_', " "), m.kind))
        .collect::<Vec<_>>()
        .join(" · ");
    b.push_str(&format!(
        "<section class=\"strategy-strip\">\
         <div class=\"strategy-strip-main\">\
         <span class=\"strategy-kicker\">Business context</span>\
         <p class=\"strategy-strip-goal\">{goal}</p>\
         <p class=\"strategy-strip-motions\">{motions}</p></div>\
         <a class=\"strategy-strip-link\" href=\"/strategy/{brand}\">Full strategy →</a>\
         </section>",
        brand = meta.key,
        goal = esc(&preview(goal, 220)),
        motions = if motions.is_empty() {
            "No enabled motions".to_string()
        } else {
            esc(&motions)
        },
    ));
}

fn render_strategy_hub(
    counts: &[(&'static BrandMeta, usize)],
    businesses: &Businesses,
    playbooks: &Playbooks,
    knowledge: Option<&Library>,
) -> String {
    let mut b = page_head("Strategy · Portfolio");
    render_topbar(&mut b, None, Surface::Strategy, counts);
    render_subbar(
        &mut b,
        "SDR strategy",
        "What each business is trying to do, and the doctrine that guides every outreach sequence.",
        &[
            (counts.len().to_string(), "brands"),
            (
                knowledge
                    .map_or(0, |library| library.books.len())
                    .to_string(),
                "knowledge sources",
            ),
            (
                knowledge
                    .map_or(0, |library| library.principles.len())
                    .to_string(),
                "principles",
            ),
        ],
    );

    b.push_str("<main class=\"strategy-scroll\">");
    b.push_str(
        "<section class=\"strategy-panel doctrine-panel\">\
         <div class=\"strategy-panel-head\">\
         <h2>What guides every email</h2>\
         <p>Shared founder-led outreach doctrine. Brand pages add the product-specific motion.</p>\
         </div><div class=\"doctrine-grid\">",
    );
    for (title, body) in SHARED_STRATEGY_PILLARS {
        b.push_str(&format!(
            "<article class=\"doctrine-card\"><h3>{}</h3><p>{}</p></article>",
            esc(title),
            esc(body),
        ));
    }
    b.push_str("</div>");
    b.push_str(&format!(
        "<details class=\"doctrine-full\"><summary>Full shared doctrine (from playbooks/shared.toml)</summary>\
         <div class=\"prose\">{}</div></details></section>",
        esc_multiline(playbooks.shared.doctrine.trim()),
    ));

    b.push_str(
        "<section class=\"strategy-panel\"><div class=\"strategy-panel-head\">\
         <h2>Agent personas &amp; retrieved knowledge</h2>\
         <p>The writer receives a compact evidence brief. A separate reviewer grades each touch and the campaign as a whole; deterministic checks enforce channels, evidence boundaries, and delivery safety. The optional ten-lens council is an audit mode, not the production default.</p></div>\
         <div class=\"doctrine-grid\">",
    );
    for (name, source, persona) in [
        (
            "Planner",
            "playbooks/personas/planner.md",
            &playbooks.shared.personas.planner,
        ),
        (
            "Writer",
            "playbooks/personas/writer.md",
            &playbooks.shared.personas.writer,
        ),
        (
            "Reviewer",
            "playbooks/personas/reviewer.md",
            &playbooks.shared.personas.reviewer,
        ),
        (
            "Response design",
            "playbooks/personas/psychology.md",
            &playbooks.shared.personas.psychology,
        ),
    ] {
        b.push_str(&format!(
            "<article class=\"doctrine-card\"><h3>{name}</h3><p>{summary}</p>\
             <details><summary>{source}</summary><div class=\"prose\">{full}</div></details></article>",
            name = esc(name),
            summary = esc(&preview(persona, 190)),
            source = esc(source),
            full = esc_multiline(persona.trim()),
        ));
    }
    b.push_str("</div>");
    b.push_str(
        "<div class=\"strategy-panel-head council-head\"><h3>Optional ten-lens audit</h3>\
         <p>Available for deliberate comparison runs. Production uses one independent sequence-level verifier so simulated unanimity does not flatten the copy.</p></div>\
         <div class=\"doctrine-grid\">",
    );
    for critic in &playbooks.shared.personas.critics {
        b.push_str(&format!(
            "<article class=\"doctrine-card\"><h3>{name}</h3><p>{summary}</p>\
             <details><summary>{source}</summary><div class=\"prose\">{full}</div></details></article>",
            name = esc(&critic.name),
            summary = esc(&preview(&critic.prompt, 210)),
            source = esc(&critic.source_path.display().to_string()),
            full = esc_multiline(critic.prompt.trim()),
        ));
    }
    b.push_str("</div>");
    if let Some(library) = knowledge {
        let distilled_sources = library
            .books
            .iter()
            .filter(|book| book.n_principles > 0)
            .count();
        let raw_only_sources = library.books.len().saturating_sub(distilled_sources);
        let mut titles = library
            .books
            .iter()
            .map(|book| book.title.as_str())
            .collect::<Vec<_>>();
        titles.sort_unstable();
        titles.dedup();
        b.push_str(&format!(
            "<p class=\"strategy-meta\"><b>{}</b> sources (<b>{}</b> distilled, <b>{}</b> raw-only) · <b>{}</b> distilled principles · <b>{}</b> passages. Outreach roles receive small stage-specific retrievals; the writer gets at most four principles and one passage, while the verifier gets at most three principles and no passages. Raw-only sources are searchable, but should be distilled before their guidance can be cited as reusable guidance.</p>\
             <details class=\"doctrine-full\"><summary>Knowledge sources currently loaded</summary><div class=\"prose\">{}</div></details>",
            library.books.len(),
            distilled_sources,
            raw_only_sources,
            library.principles.len(),
            library.chunks.len(),
            esc_multiline(&titles.join("\n")),
        ));
    }
    b.push_str("</section>");

    b.push_str(
        "<section class=\"strategy-panel\"><div class=\"strategy-panel-head\">\
         <h2>Three businesses</h2>\
         <p>Operating goals and the outreach motion each brand is testing.</p></div>\
         <div class=\"strategy-brand-grid\">",
    );
    for (meta, _) in counts {
        let Ok(profile) = businesses.get(meta.key) else {
            continue;
        };
        let Ok(playbook) = playbooks.get(meta.key) else {
            continue;
        };
        render_strategy_brand_card(&mut b, meta, profile, playbook);
    }
    b.push_str("</div></section></main></div></body></html>");
    b
}

fn render_strategy_brand_card(
    b: &mut String,
    meta: &BrandMeta,
    profile: &BusinessProfile,
    playbook: &Playbook,
) {
    let motions = profile
        .motions
        .iter()
        .filter(|m| m.enabled)
        .map(|m| {
            format!(
                "<li><b>{}</b> <span class=\"motion-kind\">{}</span><br>{}</li>",
                esc(&m.key.replace('_', " ")),
                esc(&m.kind),
                esc(&m.objective)
            )
        })
        .collect::<String>();
    let goals = profile
        .goals
        .iter()
        .take(2)
        .map(|g| format!("<li>{}</li>", esc(g)))
        .collect::<String>();
    b.push_str(&format!(
        "<article class=\"strategy-brand-card {brand}\">\
         <div class=\"strategy-brand-top\">\
         <span class=\"brand-chip {brand}\">{name}</span>\
         <a href=\"/strategy/{brand}\">Open →</a></div>\
         <p class=\"strategy-summary\">{summary}</p>\
         <h3>Trying to accomplish</h3><ul class=\"strategy-list\">{goals}</ul>\
         <h3>Enabled motions</h3><ul class=\"strategy-list motions\">{motions}</ul>\
         <h3>Outreach tests for</h3>\
         <p class=\"strategy-motion\">{motion}</p>\
         <p class=\"strategy-intro\"><b>How we introduce:</b> {one_liner}</p>\
         <div class=\"strategy-card-links\">\
         <a href=\"/strategy/{brand}\">Full strategy</a>\
         <a href=\"/b/{brand}\">Pipeline</a></div></article>",
        brand = meta.key,
        name = esc(meta.name),
        summary = esc(&profile.summary),
        goals = if goals.is_empty() {
            "<li>No goals configured.</li>".to_string()
        } else {
            goals
        },
        motions = if motions.is_empty() {
            "<li>No motions enabled.</li>".to_string()
        } else {
            motions
        },
        motion = esc(&playbook.motion),
        one_liner = esc(&playbook.one_liner),
    ));
}

fn render_strategy_brand(
    meta: &BrandMeta,
    profile: &BusinessProfile,
    playbook: &Playbook,
    shared: &SharedDoctrine,
    counts: &[(&'static BrandMeta, usize)],
) -> String {
    let mut b = page_head(&format!("{} · Strategy", meta.name));
    render_topbar(&mut b, Some(meta.key), Surface::Strategy, counts);

    let motion_count = profile.motions.iter().filter(|m| m.enabled).count();
    render_subbar(
        &mut b,
        &format!("{} strategy", meta.name),
        &profile.summary,
        &[
            (motion_count.to_string(), "motions"),
            (
                format!("{}–{}", playbook.min_words, playbook.max_words),
                "word band",
            ),
            (profile.calendar.daily_touch_cap.to_string(), "daily cap"),
        ],
    );

    b.push_str("<main class=\"strategy-scroll\">");

    // Commercial intent
    b.push_str(
        "<section class=\"strategy-panel\"><div class=\"strategy-panel-head\">\
         <h2>What we are trying to do</h2>\
         <p>Durable operating goals from the business profile — not pipeline counts.</p></div>",
    );
    render_bullet_block(&mut b, "Goals", &profile.goals, "goals");
    render_bullet_block(&mut b, "Known facts", &profile.known_facts, "facts");
    render_bullet_block(&mut b, "Unknowns", &profile.unknowns, "unknowns");
    render_bullet_block(
        &mut b,
        "Hard constraints",
        &profile.constraints,
        "constraints",
    );
    b.push_str("</section>");

    if !profile.discovery_evidence.is_empty() {
        b.push_str(
            "<section class=\"strategy-panel\"><div class=\"strategy-panel-head\">\
             <h2>Founder discovery evidence</h2>\
             <p>First-party call learning that guides targeting and questions. It is not proof about a prospect.</p></div>\
             <div class=\"motion-grid\">",
        );
        for call in &profile.discovery_evidence {
            let source = if call.source_url.trim().is_empty() {
                String::new()
            } else {
                format!(
                    "<a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">Open source notes ↗</a>",
                    esc(&call.source_url)
                )
            };
            let reported = call
                .reported_workflows
                .iter()
                .map(|item| format!("<li>{}</li>", esc(item)))
                .collect::<String>();
            let implications = call
                .sourcing_implications
                .iter()
                .map(|item| format!("<li>{}</li>", esc(item)))
                .collect::<String>();
            let follow_up = call
                .follow_up_angles
                .iter()
                .map(|item| format!("<li>{}</li>", esc(item)))
                .collect::<String>();
            let limits = call
                .limits
                .iter()
                .map(|item| format!("<li>{}</li>", esc(item)))
                .collect::<String>();
            b.push_str(&format!(
                "<article class=\"motion-card discovery-card\">\
                 <div class=\"motion-card-top\"><strong>{}</strong><span class=\"motion-kind\">{}</span></div>\
                 <p>{}</p><p class=\"strategy-meta\">{}</p>{source}\
                 <details><summary>Reported workflow</summary><ul class=\"strategy-list facts\">{reported}</ul></details>\
                 <details><summary>Sourcing implications</summary><ul class=\"strategy-list goals\">{implications}</ul></details>\
                 <details><summary>Permitted follow-up angles</summary><ul class=\"strategy-list goals\">{follow_up}</ul></details>\
                 <details><summary>Evidence boundaries</summary><ul class=\"strategy-list constraints\">{limits}</ul></details>\
                 </article>",
                esc(&call.segment),
                esc(&call.source_kind),
                esc(&call.participant_context),
                esc(&call.evidence_level),
            ));
        }
        b.push_str("</div></section>");
    }

    // Motions
    b.push_str(
        "<section class=\"strategy-panel\"><div class=\"strategy-panel-head\">\
         <h2>Operating motions</h2>\
         <p>Enabled commercial paths this brand can pursue.</p></div>\
         <div class=\"motion-grid\">",
    );
    for motion in profile.motions.iter().filter(|m| m.enabled) {
        b.push_str(&format!(
            "<article class=\"motion-card\">\
             <div class=\"motion-card-top\"><strong>{}</strong>\
             <span class=\"motion-kind\">{}</span></div>\
             <p>{}</p></article>",
            esc(&motion.key.replace('_', " ")),
            esc(&motion.kind),
            esc(&motion.objective),
        ));
    }
    if motion_count == 0 {
        b.push_str("<p class=\"empty-inline\">No enabled motions.</p>");
    }
    b.push_str("</div></section>");

    // Funding (OutageHub)
    if let Some(funding) = &profile.funding {
        b.push_str(
            "<section class=\"strategy-panel\"><div class=\"strategy-panel-head\">\
             <h2>Funding motion</h2>\
             <p>Pre-application enquiry doctrine — not a sales pitch to public servants.</p></div>",
        );
        b.push_str(&format!(
            "<p class=\"strategy-lead\">{}</p>",
            esc(&funding.objective)
        ));
        render_bullet_block(&mut b, "Themes", &funding.themes, "facts");
        render_bullet_block(&mut b, "Project shapes", &funding.project_shapes, "facts");
        render_bullet_block(
            &mut b,
            "Preferred contact titles",
            &funding.preferred_contact_titles,
            "facts",
        );
        if !funding.doctrine.trim().is_empty() {
            b.push_str(&format!(
                "<details class=\"doctrine-full openable\" open>\
                 <summary>Funding email doctrine</summary>\
                 <div class=\"prose\">{}</div></details>",
                esc_multiline(funding.doctrine.trim()),
            ));
        }
        if !funding.sources.is_empty() {
            b.push_str(
                "<h3 class=\"strategy-subhead\">Official sources</h3><ul class=\"source-list\">",
            );
            for source in &funding.sources {
                let label = if source.url.is_empty() {
                    esc(&source.name)
                } else {
                    format!(
                        "<a href=\"{}\" rel=\"noreferrer\">{}</a>",
                        esc(&source.url),
                        esc(&source.name)
                    )
                };
                b.push_str(&format!(
                    "<li><span class=\"motion-kind\">{}</span> {}</li>",
                    esc(&source.mode),
                    label
                ));
            }
            b.push_str("</ul>");
        }
        b.push_str("</section>");
    }

    if let Some(sponsorship) = &profile.sponsorship {
        b.push_str(
            "<section class=\"strategy-panel\"><div class=\"strategy-panel-head\">\
             <h2>Infrastructure sponsorship</h2>\
             <p>A commercial sponsor motion for an already-operating data resource — not a grant application.</p></div>",
        );
        b.push_str(&format!(
            "<p class=\"strategy-lead\">{}</p><p class=\"metric\"><strong>Ask:</strong> {} ${}</p>",
            esc(&sponsorship.objective),
            esc(&sponsorship.currency),
            sponsorship.ask_amount_cad,
        ));
        render_bullet_block(&mut b, "Product truth", &sponsorship.product_truth, "facts");
        render_bullet_block(
            &mut b,
            "Permitted sponsor benefits",
            &sponsorship.permitted_sponsor_benefits,
            "goals",
        );
        render_bullet_block(
            &mut b,
            "Sponsor independence",
            &sponsorship.sponsor_independence,
            "constraints",
        );
        b.push_str(
            "<h3 class=\"strategy-subhead\">Recipient routes</h3><div class=\"motion-grid\">",
        );
        for route in &sponsorship.routes {
            b.push_str(&format!(
                "<article class=\"motion-card\"><div class=\"motion-card-top\"><strong>{}</strong></div><p>{}</p><details><summary>Eligible titles</summary><ul class=\"strategy-list facts\">{}</ul></details><details><summary>Required budget/program evidence</summary><ul class=\"strategy-list constraints\">{}</ul></details></article>",
                esc(&route.recipient_kind.replace('_', " ")),
                esc(&route.action),
                route
                    .target_roles
                    .iter()
                    .map(|item| format!("<li>{}</li>", esc(item)))
                    .collect::<String>(),
                route
                    .budget_evidence_terms
                    .iter()
                    .map(|item| format!("<li>{}</li>", esc(item)))
                    .collect::<String>(),
            ));
        }
        b.push_str("</div>");
        if !sponsorship.doctrine.trim().is_empty() {
            b.push_str(&format!(
                "<details class=\"doctrine-full openable\" open><summary>Sponsorship email doctrine</summary><div class=\"prose\">{}</div></details>",
                esc_multiline(sponsorship.doctrine.trim()),
            ));
        }
        b.push_str("</section>");
    }

    // Outreach playbook
    b.push_str(
        "<section class=\"strategy-panel\"><div class=\"strategy-panel-head\">\
         <h2>Outreach playbook</h2>\
         <p>How the SDR introduces the brand, who it contacts, and the rules it will not break.</p></div>",
    );
    b.push_str(&format!(
        "<div class=\"playbook-hero\">\
         <div><span class=\"strategy-kicker\">One-liner</span><p>{}</p></div>\
         <div><span class=\"strategy-kicker\">Motion under test</span><p>{}</p></div>\
         <div><span class=\"strategy-kicker\">Signature</span><p>{}</p></div>\
         </div>",
        esc(&playbook.one_liner),
        esc(&playbook.motion),
        esc(&playbook.signature),
    ));
    if !playbook.icp_note.is_empty() {
        b.push_str(&format!(
            "<p class=\"strategy-lead\"><b>ICP note:</b> {}</p>",
            esc(&playbook.icp_note)
        ));
    }
    if let Some(max) = playbook.max_employees {
        b.push_str(&format!(
            "<p class=\"strategy-meta\">Max target headcount: <b>{max}</b> · Min independent signals: <b>{}</b> · Body length: <b>{}–{} words</b></p>",
            playbook.min_signals,
            playbook.min_words,
            playbook.max_words,
        ));
    } else {
        b.push_str(&format!(
            "<p class=\"strategy-meta\">Min independent signals: <b>{}</b> · Body length: <b>{}–{} words</b></p>",
            playbook.min_signals,
            playbook.min_words,
            playbook.max_words,
        ));
    }
    render_bullet_block(
        &mut b,
        "Who we contact (vantage)",
        &playbook.vantage_notes,
        "vantage",
    );
    render_bullet_block(
        &mut b,
        "Rules of engagement",
        &playbook.requirements,
        "requirements",
    );
    render_bullet_block(
        &mut b,
        "System concepts we may propose",
        &playbook.system_concept_examples,
        "facts",
    );
    render_bullet_block(
        &mut b,
        "Subject-line examples",
        &playbook.subject_examples,
        "facts",
    );
    if !playbook.doctrine.trim().is_empty() {
        b.push_str(&format!(
            "<details class=\"doctrine-full openable\" open>\
             <summary>Brand doctrine</summary>\
             <div class=\"prose\">{}</div></details>",
            esc_multiline(playbook.doctrine.trim()),
        ));
    }
    b.push_str(&format!(
        "<details class=\"doctrine-full\"><summary>Shared doctrine spine</summary>\
         <div class=\"prose\">{}</div></details></section>",
        esc_multiline(shared.doctrine.trim()),
    ));

    // Capacity / timing
    let cal = &profile.calendar;
    let limits = &profile.account_limits;
    b.push_str(
        "<section class=\"strategy-panel\"><div class=\"strategy-panel-head\">\
         <h2>Capacity &amp; timing</h2>\
         <p>Business-owned calendar and account throttles the cadence engine enforces.</p></div>\
         <div class=\"capacity-grid\">",
    );
    for (label, value) in [
        ("Daily touch cap", cal.daily_touch_cap.to_string()),
        ("Quota timezone", cal.quota_timezone.clone()),
        (
            "Recipient fallback TZ",
            cal.fallback_recipient_timezone.clone(),
        ),
        (
            "Window",
            format!(
                "{}–{} local · {}",
                cal.window_start,
                cal.window_end,
                cal.weekdays.join(", ")
            ),
        ),
        (
            "Preferred hours",
            cal.preferred_hours
                .iter()
                .map(|h| h.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        ),
        (
            "New contacts / account / day",
            limits.max_new_contacts_per_account_per_day.to_string(),
        ),
        (
            "Active contacts / account",
            limits.max_active_contacts_per_account.to_string(),
        ),
        (
            "Max unanswered touches",
            limits.max_unanswered_touches.to_string(),
        ),
    ] {
        b.push_str(&format!(
            "<div class=\"capacity-card\"><span>{}</span><strong>{}</strong></div>",
            esc(label),
            esc(&value),
        ));
    }
    b.push_str("</div>");
    if !cal.rules.is_empty() {
        b.push_str("<h3 class=\"strategy-subhead\">Named timing hypotheses</h3><div class=\"timing-rules\">");
        for rule in &cal.rules {
            b.push_str(&format!(
                "<article class=\"timing-rule\"><strong>{}</strong>\
                 <p>{}</p>\
                 <small>{}{}{}</small></article>",
                esc(&rule.key.replace('_', " ")),
                esc(&rule.rationale),
                if rule.weekdays.is_empty() {
                    String::new()
                } else {
                    format!("Days: {} · ", esc(&rule.weekdays.join(", ")))
                },
                if rule.preferred_hours.is_empty() {
                    String::new()
                } else {
                    format!(
                        "Hours: {} · ",
                        rule.preferred_hours
                            .iter()
                            .map(|h| h.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                },
                if rule.industries.is_empty() && rule.title_keywords.is_empty() {
                    String::new()
                } else {
                    format!(
                        "Match: {} {}",
                        esc(&rule.industries.join(", ")),
                        esc(&rule.title_keywords.join(", "))
                    )
                },
            ));
        }
        b.push_str("</div>");
    }
    b.push_str(&format!(
        "<p class=\"strategy-foot\"><a href=\"/b/{}\">← Back to {} pipeline</a></p>\
         </section></main></div></body></html>",
        meta.key,
        esc(meta.name),
    ));
    b
}

fn render_gtm_lab(
    active: Option<&BrandMeta>,
    counts: &[(&'static BrandMeta, usize)],
    snapshot: &GtmSnapshot,
    profile: Option<&BusinessProfile>,
) -> String {
    let brand_name = active.map(|meta| meta.name).unwrap_or("Portfolio");
    let mut b = page_head(&format!("{brand_name} · GTM Lab"));
    render_topbar(&mut b, active.map(|meta| meta.key), Surface::Gtm, counts);
    let live_signals = snapshot
        .observations
        .iter()
        .filter(|observation| matches!(observation.status.as_str(), "observed" | "verified"))
        .count();
    let mapped_opportunities = snapshot
        .sales_opportunities
        .iter()
        .filter(|opportunity| opportunity.status != "rejected")
        .count();
    render_subbar(
        &mut b,
        &format!("{brand_name} GTM Lab"),
        "Evidence → versioned play → controlled action → attributable outcome → bounded proof.",
        &[
            (
                snapshot.market_accounts.len().to_string(),
                "universe accounts",
            ),
            (snapshot.segments.len().to_string(), "bounded segments"),
            (mapped_opportunities.to_string(), "mapped opportunities"),
            (live_signals.to_string(), "live signals"),
        ],
    );
    b.push_str(
        "<main class=\"gtm-scroll\"><section class=\"gtm-doctrine\">\
         <div><span class=\"strategy-kicker\">Internal GTM engineering</span>\
         <h2>Build the decision system before automating the action</h2>\
         <p>Signal lineage, eligibility, play selection, and experiment assignment are deterministic. Agents interpret evidence and write original copy inside those boundaries.</p></div>\
         <div><span class=\"strategy-kicker\">Forward-deployed GTM</span>\
         <h2>Prove one acknowledged problem on real data</h2>\
         <p>A reply can create a proof brief, never approve it. Proofs remain bounded by a metric and stop condition, then feed their result back into the play.</p></div>\
         <div><span class=\"strategy-kicker\">Market truth</span>\
         <h2>The council does not grade commercial success</h2>\
         <p>Persona reviews guard sendability. Only prospect outcomes and proof results count as evidence for promoting or retiring a play.</p></div>\
         </section>",
    );

    let lead_names = snapshot
        .leads
        .iter()
        .map(|lead| (lead.id.as_str(), lead.name.as_str()))
        .collect::<std::collections::HashMap<_, _>>();

    let market_names = snapshot
        .market_accounts
        .iter()
        .map(|account| (account.id.as_str(), account.name.as_str()))
        .collect::<std::collections::HashMap<_, _>>();
    let facility_names = snapshot
        .facilities
        .iter()
        .map(|facility| (facility.id.as_str(), facility.name.as_str()))
        .collect::<std::collections::HashMap<_, _>>();
    let person_names = snapshot
        .people
        .iter()
        .map(|person| (person.id.as_str(), person.name.as_str()))
        .collect::<std::collections::HashMap<_, _>>();
    let opportunity_names = snapshot
        .sales_opportunities
        .iter()
        .map(|opportunity| (opportunity.id.as_str(), opportunity.title.as_str()))
        .collect::<std::collections::HashMap<_, _>>();

    b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Market coverage ledger</h2><p>Coverage is measured per bounded segment and source cursor. Apollo enriches an enumerated market; it no longer defines the denominator.</p></div><div class=\"gtm-table-wrap\"><table class=\"gtm-table\"><thead><tr><th>Segment</th><th>Geography / unit</th><th>Coverage</th><th>Latest source cursor</th></tr></thead><tbody>");
    for segment in &snapshot.segments {
        let runs = snapshot
            .coverage_runs
            .iter()
            .filter(|run| run.segment_id == segment.id)
            .collect::<Vec<_>>();
        let latest = runs.first().copied();
        b.push_str(&format!(
            "<tr><td><b>{}</b><small>{}</small></td><td>{}<small>{}</small></td><td>{} discovered / {} estimated<small>{} opportunity accounts · {}</small></td><td>{}<small>{} pages · {} candidates · {}</small></td></tr>",
            esc(&segment.name),
            esc(&segment.wedge),
            esc(&segment.geography),
            esc(&segment.unit_of_analysis),
            segment.accounts_discovered,
            if segment.estimated_total > 0 { segment.estimated_total.to_string() } else { "denominator pending".into() },
            segment.accounts_with_opportunities,
            if segment.source_exhausted { "sources exhausted" } else { "coverage open" },
            latest.map(|run| esc(&run.source_name)).unwrap_or_else(|| "not started".into()),
            latest.map(|run| run.pages_examined).unwrap_or(0),
            latest.map(|run| run.candidates_seen).unwrap_or(0),
            latest.map(|run| if run.exhausted { "exhausted".to_string() } else { format!("resume {}", run.cursor) }).unwrap_or_else(|| "no cursor".into()),
        ));
    }
    if snapshot.segments.is_empty() {
        b.push_str("<tr><td colspan=\"4\" class=\"empty\">No bounded market segments are configured.</td></tr>");
    }
    b.push_str("</tbody></table></div></section>");

    let forecast = &snapshot.commercial_forecast;
    let cash_need = forecast
        .monthly_cash_need_cents
        .map(format_cad)
        .unwrap_or_else(|| "unknown".into());
    let coverage = forecast
        .cash_now_pipeline_coverage
        .map(|coverage| format!("{coverage:.2}×"))
        .unwrap_or_else(|| "unknown".into());
    b.push_str(&format!(
        "<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><div><h2>Commercial operating queue</h2><p>Evidence readiness answers whether outreach is defensible. This separate lane-first queue protects cash and founder capacity; unknown estimates remain unknown.</p></div></div><div class=\"outcome-strip\"><div class=\"outcome-card\"><b>{}</b><span>cash collected this month</span></div><div class=\"outcome-card\"><b>{}</b><span>expected 30-day cash</span></div><div class=\"outcome-card\"><b>{}</b><span>expected 90-day cash</span></div><div class=\"outcome-card\"><b>{}</b><span>expected 180-day cash</span></div><div class=\"outcome-card\"><b>{}</b><span>monthly cash need</span></div><div class=\"outcome-card\"><b>{}</b><span>cash-now 90d coverage</span></div><div class=\"outcome-card\"><b>{}</b><span>stalled / no dated action</span></div></div><div class=\"gtm-card-grid\">",
        format_cad(forecast.cash_collected_month_cents),
        format_cad(forecast.expected_30d_cash_cents),
        format_cad(forecast.expected_90d_cash_cents),
        format_cad(forecast.expected_180d_cash_cents),
        esc(&cash_need),
        esc(&coverage),
        forecast.stalled_without_dated_next_action,
    ));
    for assessment in &snapshot.commercial_assessments {
        let opportunity = snapshot
            .sales_opportunities
            .iter()
            .find(|opportunity| opportunity.id == assessment.sales_opportunity_id);
        let account = opportunity
            .and_then(|opportunity| lead_names.get(opportunity.lead_id.as_str()).copied())
            .unwrap_or("Unknown account");
        let opportunity_title = opportunity
            .map(|opportunity| opportunity.title.as_str())
            .unwrap_or("Unknown opportunity");
        let evidence_tier = opportunity
            .map(|opportunity| opportunity.evidence_tier.as_str())
            .unwrap_or("unknown");
        let expected_cash = assessment
            .expected_upfront_cash_cents
            .map(format_cad)
            .unwrap_or_else(|| "unknown".into());
        let close_probability = assessment
            .close_probability_bps
            .map(|bps| format!("{:.1}%", bps as f64 / 100.0))
            .unwrap_or_else(|| "unknown".into());
        let days_to_cash = assessment
            .days_to_first_cash
            .map(|days| format!("{days} days"))
            .unwrap_or_else(|| "unknown".into());
        let event_count = snapshot
            .commercial_events
            .iter()
            .filter(|event| event.sales_opportunity_id == assessment.sales_opportunity_id)
            .count();
        b.push_str(&format!(
            "<article class=\"gtm-card\"><div class=\"gtm-card-head\"><div><span class=\"strategy-kicker\">{} · evidence {}</span><h3>{}</h3></div><span class=\"gtm-status\">{}</span></div><p><b>Account:</b> {}</p><p><b>Offer:</b> {}</p><p><b>Upfront cash / probability / timing:</b> {} · {} · {}</p><p><b>Next action:</b> {} {}</p><p class=\"gtm-meta\">{} truth events · assessment v{} · source: {} · confidence: {}</p></article>",
            esc(&assessment.commercial_lane),
            esc(evidence_tier),
            esc(opportunity_title),
            esc(&assessment.sales_stage),
            esc(account),
            esc(if assessment.offer_key.is_empty() { "unselected" } else { &assessment.offer_key }),
            esc(&expected_cash),
            esc(&close_probability),
            esc(&days_to_cash),
            esc(if assessment.next_action.is_empty() { "not recorded" } else { &assessment.next_action }),
            esc(&assessment.next_action_due_at),
            event_count,
            assessment.version,
            esc(&assessment.assessment_source),
            esc(&assessment.assessment_confidence),
        ));
    }
    if snapshot.commercial_assessments.is_empty() {
        b.push_str("<div class=\"empty\">No opportunity has a founder-reviewed commercial assessment yet. No cash forecast is being inferred from fit or email readiness.</div>");
    }
    if let Some(profile) = profile {
        let allocation = profile.commercial_allocation(forecast.runway_months);
        b.push_str(&format!(
            "<div class=\"gtm-proof-shape\"><b>Cash-constrained founder allocation</b><p>{}% cash-now · {}% core · {}% strategic. Unknown runway stays cash-constrained. Strategic pursuits capped at {} and require a champion, compelling event, procurement path, and paid first phase.</p><form method=\"post\" action=\"/commercial-operating-state\" class=\"gtm-form\"><input type=\"hidden\" name=\"brand\" value=\"{}\"><label>Runway months<input name=\"runway_months\" value=\"{}\" placeholder=\"blank = unknown\"></label><label>Monthly cash need (CAD)<input name=\"monthly_cash_need_cad\" value=\"{}\" placeholder=\"blank = unknown\"></label><label>As of<input type=\"date\" name=\"as_of\"></label><button type=\"submit\">Update founder constraints</button></form></div>",
            allocation.cash_now_share_bps / 100,
            allocation.core_share_bps / 100,
            allocation.strategic_share_bps / 100,
            profile.commercial.max_active_strategic,
            esc(&profile.key),
            forecast.runway_months.map(|value| value.to_string()).unwrap_or_default(),
            optional_cad_input(forecast.monthly_cash_need_cents),
        ));
        for opportunity in &snapshot.sales_opportunities {
            let current = snapshot
                .commercial_assessments
                .iter()
                .find(|assessment| assessment.sales_opportunity_id == opportunity.id)
                .cloned()
                .unwrap_or_else(|| CommercialAssessment {
                    sales_opportunity_id: opportunity.id.clone(),
                    brand: opportunity.brand.clone(),
                    commercial_lane: "unassessed".into(),
                    sales_stage: "mapped".into(),
                    procurement_complexity: "unknown".into(),
                    delivery_risk: "unknown".into(),
                    buyer_access: "unknown".into(),
                    budget_owner_status: "unknown".into(),
                    champion_status: "unknown".into(),
                    assessment_confidence: "unknown".into(),
                    ..Default::default()
                });
            b.push_str(&format!(
                "<details class=\"gtm-card\"><summary><b>Assess {}</b> · {}</summary><form method=\"post\" action=\"/commercial-assessment\" class=\"gtm-form\"><input type=\"hidden\" name=\"brand\" value=\"{}\"><input type=\"hidden\" name=\"sales_opportunity_id\" value=\"{}\"><label>Commercial lane<select name=\"commercial_lane\">{}</select></label><label>Offer<select name=\"offer_key\">{}</select></label><label>Sales stage<select name=\"sales_stage\">{}</select></label><label>Expected contract value (CAD)<input name=\"expected_contract_value_cad\" value=\"{}\" placeholder=\"blank = unknown\"></label><label>Expected upfront cash (CAD)<input name=\"expected_upfront_cash_cad\" value=\"{}\" placeholder=\"blank = unknown\"></label><label>Cash collectable within 90d (CAD)<input name=\"cash_collectable_within_90d_cad\" value=\"{}\" placeholder=\"blank = unknown\"></label><label>Expected ARR (CAD)<input name=\"expected_arr_cad\" value=\"{}\" placeholder=\"blank = unknown\"></label><label>Estimated 12m gross profit (CAD)<input name=\"estimated_12m_gross_profit_cad\" value=\"{}\" placeholder=\"blank = unknown\"></label><label>Days to first cash<input type=\"number\" min=\"0\" name=\"days_to_first_cash\" value=\"{}\" placeholder=\"unknown\"></label><label>Close probability (%)<input name=\"close_probability_percent\" value=\"{}\" placeholder=\"unknown\"></label><label>Sales hours remaining<input type=\"number\" min=\"0\" name=\"sales_hours_remaining\" value=\"{}\"></label><label>Founder hours total<input type=\"number\" min=\"0\" name=\"estimated_founder_hours\" value=\"{}\"></label><label>Delivery hours<input type=\"number\" min=\"0\" name=\"delivery_hours\" value=\"{}\"></label><label>Unpaid delivery hours<input type=\"number\" min=\"0\" name=\"unpaid_delivery_hours\" value=\"{}\"></label><label>Gross margin (%)<input name=\"gross_margin_percent\" value=\"{}\"></label><label>Procurement complexity<input name=\"procurement_complexity\" value=\"{}\" placeholder=\"unknown / low / medium / high\"></label><label>Integration complexity<input name=\"integration_complexity\" value=\"{}\" placeholder=\"unknown / low / medium / high\"></label><label>Delivery risk<input name=\"delivery_risk\" value=\"{}\" placeholder=\"unknown / low / medium / high\"></label><label class=\"wide\">Current trigger<textarea name=\"current_trigger\">{}</textarea></label><label>Buyer access<input name=\"buyer_access\" value=\"{}\"></label><label class=\"wide\">Budget / procurement path<textarea name=\"budget_path\">{}</textarea></label><label>Budget owner status<input name=\"budget_owner_status\" value=\"{}\"></label><label>Champion status<input name=\"champion_status\" value=\"{}\"></label><label>Champion strength<input name=\"champion_strength\" value=\"{}\"></label><label>Executive sponsor status<input name=\"executive_sponsor_status\" value=\"{}\"></label><label class=\"wide\">Compelling event<textarea name=\"compelling_event\">{}</textarea></label><label class=\"wide\">Payment structure<textarea name=\"payment_structure\">{}</textarea></label><label class=\"wide\">Next buyer commitment<textarea name=\"next_commitment\">{}</textarea></label><label class=\"wide\">Internal next action<textarea name=\"next_action\">{}</textarea></label><label>Next commitment due<input type=\"date\" name=\"next_action_due_at\" value=\"{}\"></label><label>Target close date<input type=\"date\" name=\"target_close_date\" value=\"{}\"></label><label class=\"wide\">Stalled reason<textarea name=\"stalled_reason\" placeholder=\"Required when an active deal has no dated buyer commitment\">{}</textarea></label><label>Estimate confidence<select name=\"assessment_confidence\">{}</select></label><label class=\"wide\">Estimate basis<textarea name=\"estimate_basis\" placeholder=\"One human/offer/buyer basis per line; required for every numeric estimate\">{}</textarea></label><button type=\"submit\">Save versioned assessment</button></form><form method=\"post\" action=\"/commercial-event\" class=\"gtm-form\"><input type=\"hidden\" name=\"brand\" value=\"{}\"><input type=\"hidden\" name=\"sales_opportunity_id\" value=\"{}\"><label>Truth event<select name=\"kind\">{}</select></label><label>Amount (CAD)<input name=\"amount_cad\" placeholder=\"required for payment/cash events\"></label><label>Occurred at<input type=\"datetime-local\" name=\"occurred_at\"></label><label>Invoice/payment/external ref<input name=\"external_ref\"></label><label class=\"wide\">Evidence detail<textarea name=\"detail\" placeholder=\"Record only a real proposal, invoice, payment, win, loss, or refund\"></textarea></label><button type=\"submit\">Append truth event</button></form></details>",
                esc(&opportunity.title),
                esc(&opportunity.evidence_tier),
                esc(&opportunity.brand),
                esc(&opportunity.id),
                select_options(&["unassessed", "cash_now", "core", "strategic", "parked"], &current.commercial_lane),
                commercial_offer_options(profile, &current.offer_key),
                select_options(&["mapped", "contacted", "discovery", "scoped", "proposal", "procurement", "paid_pilot", "won", "lost"], &current.sales_stage),
                optional_cad_input(current.expected_contract_value_cents),
                optional_cad_input(current.expected_upfront_cash_cents),
                optional_cad_input(current.cash_collectable_within_90d_cents),
                optional_cad_input(current.expected_arr_cents),
                optional_cad_input(current.estimated_12m_gross_profit_cents),
                optional_i64_input(current.days_to_first_cash),
                optional_bps_input(current.close_probability_bps),
                optional_i64_input(current.sales_hours_remaining),
                optional_i64_input(current.estimated_founder_hours),
                optional_i64_input(current.delivery_hours),
                optional_i64_input(current.unpaid_delivery_hours),
                optional_bps_input(current.gross_margin_bps),
                esc(&current.procurement_complexity),
                esc(&current.integration_complexity),
                esc(&current.delivery_risk),
                esc(&current.current_trigger),
                esc(&current.buyer_access),
                esc(&current.budget_path),
                esc(&current.budget_owner_status),
                esc(&current.champion_status),
                esc(&current.champion_strength),
                esc(&current.executive_sponsor_status),
                esc(&current.compelling_event),
                esc(&current.payment_structure),
                esc(&current.next_commitment),
                esc(&current.next_action),
                esc(&current.next_action_due_at),
                esc(&current.target_close_date),
                esc(&current.stalled_reason),
                select_options(&["unknown", "hypothesis", "buyer_validated", "transaction_validated"], &current.assessment_confidence),
                esc(&current.estimate_basis.join("\n")),
                esc(&opportunity.brand),
                esc(&opportunity.id),
                select_options(&["proposal_issued", "deposit_requested", "deposit_paid", "pilot_paid", "contract_won", "cash_collected", "contract_lost", "refund_issued"], "proposal_issued"),
            ));
        }
    }
    b.push_str("</div></section>");

    b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Facility and workflow opportunities</h2><p>The sales object is a facility/task or workflow/decision with atomic evidence and a mapped committee—not a generic company hypothesis.</p></div><div class=\"gtm-card-grid\">");
    for opportunity in &snapshot.sales_opportunities {
        let account = market_names
            .get(opportunity.market_account_id.as_str())
            .copied()
            .unwrap_or("Unknown account");
        let facility = if opportunity.facility_id.is_empty() {
            "No facility required or linked"
        } else {
            facility_names
                .get(opportunity.facility_id.as_str())
                .copied()
                .unwrap_or("Unknown facility")
        };
        let claims = snapshot
            .evidence_claims
            .iter()
            .filter(|claim| claim.sales_opportunity_id == opportunity.id)
            .count();
        let committee = snapshot
            .opportunity_stakeholders
            .iter()
            .filter(|stakeholder| stakeholder.sales_opportunity_id == opportunity.id)
            .collect::<Vec<_>>();
        let active_person = committee
            .iter()
            .find(|stakeholder| stakeholder.active_thread)
            .and_then(|stakeholder| person_names.get(stakeholder.person_id.as_str()).copied())
            .unwrap_or("none");
        b.push_str(&format!(
            "<article class=\"gtm-card\"><div class=\"gtm-card-head\"><div><span class=\"strategy-kicker\">{} · {}</span><h3>{}</h3></div><span class=\"gtm-status {}\">{}</span></div><p><b>Account / facility:</b> {} · {}</p><p><b>Task or decision:</b> {}</p><p><b>Mechanism:</b> {}</p><p><b>Consequence:</b> {}</p><div class=\"gtm-proof-shape\"><b>Proof contribution</b><p>{}</p></div><p class=\"gtm-meta\">{} atomic claims · {} committee roles · active cold thread: {}<br>Gaps: {}</p></article>",
            esc(&opportunity.brand),
            esc(&opportunity.evidence_tier),
            esc(&opportunity.title),
            esc(&opportunity.evidence_status),
            esc(&opportunity.evidence_status),
            esc(account),
            esc(facility),
            esc(&opportunity.task_or_decision),
            esc(&opportunity.mechanism),
            esc(&opportunity.consequence),
            esc(&opportunity.proof_offer),
            claims,
            committee.len(),
            esc(active_person),
            esc(&opportunity.evidence_gaps.join("; ")),
        ));
    }
    if snapshot.sales_opportunities.is_empty() {
        b.push_str("<p class=\"empty\">No evidence-linked opportunities yet. Existing assessments backfill as research until exact claims and committee roles are present.</p>");
    }
    b.push_str("</div></section>");

    b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Buying committees</h2><p>Map witnesses, process owners, constraint owners, technical evaluators, economic buyers, and procurement; activate one cold thread at a time.</p></div><div class=\"gtm-table-wrap\"><table class=\"gtm-table\"><thead><tr><th>Opportunity</th><th>Person</th><th>Role</th><th>Thread</th></tr></thead><tbody>");
    for stakeholder in &snapshot.opportunity_stakeholders {
        b.push_str(&format!(
            "<tr><td><b>{}</b></td><td>{}</td><td>{}<small>{}</small></td><td>{}</td></tr>",
            esc(opportunity_names
                .get(stakeholder.sales_opportunity_id.as_str())
                .copied()
                .unwrap_or("Unknown opportunity")),
            esc(person_names
                .get(stakeholder.person_id.as_str())
                .copied()
                .unwrap_or("Unknown person")),
            esc(&stakeholder.role),
            esc(&stakeholder.relationship_to_task),
            if stakeholder.active_thread {
                "active cold thread"
            } else {
                "mapped / held"
            },
        ));
    }
    if snapshot.opportunity_stakeholders.is_empty() {
        b.push_str(
            "<tr><td colspan=\"4\" class=\"empty\">No committee roles mapped yet.</td></tr>",
        );
    }
    b.push_str("</tbody></table></div></section>");

    if active.is_some_and(|meta| meta.key == "wapahki") {
        b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Customer-development stage gates</h2><p>The operating path is discovery → evidence → evaluation → design partnership → conditional intent → paid pilot → deployment. No rung advances merely because an email was sent or a meeting was booked.</p></div><div class=\"customer-dev-stage-grid\">");
        for (index, stage) in crate::gtm::CUSTOMER_DEVELOPMENT_STAGES.iter().enumerate() {
            let count = snapshot
                .customer_development
                .iter()
                .filter(|record| crate::gtm::customer_development_stage(record) == stage.key)
                .count()
                + if stage.key == "hypothesis" {
                    snapshot
                        .leads
                        .iter()
                        .filter(|lead| {
                            !snapshot
                                .customer_development
                                .iter()
                                .any(|record| record.lead_id == lead.id)
                        })
                        .count()
                } else {
                    0
                };
            b.push_str(&format!(
                "<article><span>{:02}</span><strong>{}</strong><em>{} account(s)</em><p>{}</p><small>Next: {}</small></article>",
                index + 1,
                esc(stage.label),
                count,
                esc(stage.proof),
                esc(stage.next_commitment),
            ));
        }
        b.push_str("</div></section>");
    }

    b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Versioned plays</h2><p>The commercial policy agents reason from. These are hypotheses and proof patterns, not email templates.</p></div><div class=\"gtm-card-grid\">");
    for play in &snapshot.plays {
        b.push_str(&format!(
            "<article class=\"gtm-card\"><div class=\"gtm-card-head\"><div><span class=\"strategy-kicker\">{} · v{}</span><h3>{}</h3></div><span class=\"gtm-status {}\">{}</span></div>\
             <p><b>Hypothesis:</b> {}</p><p><b>Action policy:</b> {}</p>\
             <div class=\"gtm-proof-shape\"><b>Forward-deployed proof</b><p>{}</p><small>Measure: {}<br>Stop: {}</small></div>\
             <p class=\"gtm-meta\">Eligible after {} of: {}</p>\
             <div class=\"gtm-actions\">\
             <form method=\"post\" action=\"/gtm/play/{}/testing\"><button>Testing</button></form>\
             <form method=\"post\" action=\"/gtm/play/{}/proven\"><button>Proven</button></form>\
             <form method=\"post\" action=\"/gtm/play/{}/retired\"><button class=\"quiet\">Retire</button></form>\
             </div></article>",
            esc(&play.brand),
            play.version,
            esc(&play.name),
            esc(&play.lifecycle),
            esc(&play.lifecycle),
            esc(&play.hypothesis),
            esc(&play.action_policy),
            esc(&play.proof_description),
            esc(&play.success_metric),
            esc(&play.kill_condition),
            play.minimum_signal_matches,
            esc(&play.required_signal_keys.join(", ")),
            esc(&play.id),
            esc(&play.id),
            esc(&play.id),
        ));
    }
    b.push_str("</div></section>");

    b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Account root-cause ranking</h2><p>The active play shapes sourcing. Accounts are ranked by evidence, root-cause clarity, stakeholder vantage, and fit to the bounded proof—not Apollo result order.</p></div><div class=\"gtm-card-grid\">");
    for assessment in &snapshot.assessments {
        let account = lead_names
            .get(assessment.lead_id.as_str())
            .copied()
            .unwrap_or("Unknown account");
        b.push_str(&format!(
            "<article class=\"gtm-card\"><div class=\"gtm-card-head\"><div><span class=\"strategy-kicker\">{} · play v{}</span><h3>{}</h3></div><span class=\"gtm-score\">{}/100</span></div>\
             <p><b>Observed symptom:</b> {}</p><p><b>Root-cause hypothesis:</b> {}</p>\
             <p><b>Current workaround:</b> {}</p><p><b>Why now:</b> {}</p>\
             <div class=\"gtm-proof-shape\"><b>Proof fit</b><p>{}</p></div>\
             <p class=\"gtm-meta\">Matched: {}<br>Evidence gaps: {}<br>Disqualifiers: {}</p></article>",
            esc(&assessment.brand),
            assessment.play_version,
            esc(account),
            assessment.fit_score,
            esc(&assessment.symptom),
            esc(&assessment.root_cause),
            esc(&assessment.current_workaround),
            esc(&assessment.why_now),
            esc(&assessment.proof_fit),
            esc(&assessment.matched_signal_keys.join(", ")),
            esc(&assessment.evidence_gaps.join("; ")),
            esc(&assessment.disqualifiers.join("; ")),
        ));
    }
    if snapshot.assessments.is_empty() {
        b.push_str("<p class=\"empty\">No versioned play assessments yet. The next source or refresh run will create them and rank the requested accounts.</p>");
    }
    b.push_str("</div></section>");

    b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Signal registry</h2><p>Canonical definitions carry an owner, refresh cadence, confidence floor, version, and expiry. Observations retain their evidence and source.</p></div><div class=\"gtm-split\"><div><h3>Definitions</h3><div class=\"gtm-table-wrap\"><table class=\"gtm-table\"><thead><tr><th>Signal</th><th>Entity</th><th>Owner / refresh</th><th>Floor / TTL</th></tr></thead><tbody>");
    for definition in &snapshot.definitions {
        b.push_str(&format!(
            "<tr><td><b>{}</b><small>{}<br>{}</small></td><td>{}</td><td>{}<small>{}</small></td><td>{:.0}%<small>{} days · v{}</small></td></tr>",
            esc(&definition.key),
            esc(&definition.name),
            esc(&definition.description),
            esc(&definition.entity_type),
            esc(&definition.owner),
            esc(&definition.refresh_cadence),
            definition.minimum_confidence * 100.0,
            definition.freshness_seconds / 86_400,
            definition.version,
        ));
    }
    b.push_str("</tbody></table></div></div><div><h3>Latest observations</h3><div class=\"gtm-observations\">");
    for observation in snapshot.observations.iter().take(80) {
        let account = lead_names
            .get(observation.lead_id.as_str())
            .copied()
            .unwrap_or("Conversation/contact signal");
        let source = if observation.source_url.starts_with("http://")
            || observation.source_url.starts_with("https://")
        {
            format!(
                "<a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">source ↗</a>",
                esc(&observation.source_url)
            )
        } else {
            esc(&observation.source_name)
        };
        b.push_str(&format!(
            "<article class=\"gtm-observation\"><div><b>{}</b><span class=\"gtm-status {}\">{}</span></div>\
             <small>{} · {:.0}% confidence · {}</small><p>{}</p><small>Observed {} · expires {}</small></article>",
            esc(account),
            esc(&observation.status),
            esc(&observation.definition_key),
            esc(&observation.brand),
            observation.confidence * 100.0,
            source,
            esc(&observation.evidence),
            esc(&observation.observed_at),
            if observation.expires_at.is_empty() {
                "never".to_string()
            } else {
                esc(&observation.expires_at)
            },
        ));
    }
    if snapshot.observations.is_empty() {
        b.push_str("<p class=\"empty\">No observations yet. Existing account signals are converted automatically when the database opens.</p>");
    }
    b.push_str("</div></div></div></section>");

    b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Experiments</h2><p>One variable, stable control/variant assignment, simultaneous launch, and a 21-day minimum measurement window.</p></div>");
    if let (Some(meta), Some(play)) = (active, snapshot.plays.first()) {
        b.push_str(&format!(
            "<details class=\"gtm-create\"><summary>Plan an experiment</summary>\
             <form method=\"post\" action=\"/gtm/experiment\" class=\"gtm-form\">\
             <input type=\"hidden\" name=\"brand\" value=\"{}\"><input type=\"hidden\" name=\"play_id\" value=\"{}\">\
             <label>Name<input name=\"name\" required placeholder=\"Operations-manager list test\"></label>\
             <label>Type<select name=\"experiment_type\"><option value=\"list_only\">List only</option><option value=\"copy_only\">Copy only</option><option value=\"combined\">Combined (hypothesis generation)</option></select></label>\
             <label class=\"wide\">One-sentence hypothesis<textarea name=\"hypothesis\" required></textarea></label>\
             <label>Single variable<input name=\"variable\" required></label>\
             <label class=\"wide\">Constants (one per line)<textarea name=\"constants\" required placeholder=\"Offer\nInfrastructure\nTiming\nAll other targeting fields\"></textarea></label>\
             <label>Control<textarea name=\"control_description\" required></textarea></label>\
             <label>Variant<textarea name=\"variant_description\" required></textarea></label>\
             <label>Minimum sends / arm<input type=\"number\" name=\"minimum_sends_per_arm\" value=\"2000\" min=\"1\"></label>\
             <label>Baseline sends<input type=\"number\" name=\"baseline_sends\" value=\"0\" min=\"0\"></label>\
             <label>Baseline positive reply rate<input type=\"number\" step=\"0.001\" name=\"baseline_positive_reply_rate\" value=\"0\"></label>\
             <label>Success target<input type=\"number\" step=\"0.001\" name=\"success_target\" value=\"0\"></label>\
             <label>Failure floor<input type=\"number\" step=\"0.001\" name=\"failure_floor\" value=\"0\"></label>\
             <label>Measurement days<input type=\"number\" name=\"measurement_days\" value=\"21\" min=\"21\"></label>\
             <button type=\"submit\">Create draft experiment</button></form></details>",
            esc(meta.key),
            esc(&play.id),
        ));
    }
    b.push_str("<div class=\"gtm-card-grid\">");
    for experiment in &snapshot.experiments {
        b.push_str(&format!(
            "<article class=\"gtm-card\"><div class=\"gtm-card-head\"><div><span class=\"strategy-kicker\">{} · {}</span><h3>{}</h3></div><span class=\"gtm-status {}\">{}</span></div>\
             <p><b>Hypothesis:</b> {}</p><p><b>Only variable:</b> {}</p>\
             <div class=\"experiment-arms\"><div><b>Control</b><p>{}</p></div><div><b>Variant</b><p>{}</p></div></div>\
             <p class=\"gtm-meta\">Constants: {}<br>Baseline: {} sends at {:.2}% positive · minimum: {} sends/arm · measure after {} days. Low-volume results remain directional.<br>Decision: {} · confidence: {}</p>\
             <div class=\"gtm-actions\"><form method=\"post\" action=\"/gtm/experiment/{}/running\"><button>Start</button></form>\
             <form method=\"post\" action=\"/gtm/experiment/{}/measuring\"><button>Measure</button></form>\
             <form method=\"post\" action=\"/gtm/experiment/{}/cancelled\"><button class=\"quiet\">Cancel</button></form></div>\
             <details class=\"gtm-results\"><summary>Record completed-arm results</summary>\
             <form method=\"post\" action=\"/gtm/experiment/{}/evaluate/results\">\
             <label>Control sent<input type=\"number\" name=\"control_sent\" min=\"1\" required></label>\
             <label>Control positive<input type=\"number\" name=\"control_positive\" min=\"0\" required></label>\
             <label>Variant sent<input type=\"number\" name=\"variant_sent\" min=\"1\" required></label>\
             <label>Variant positive<input type=\"number\" name=\"variant_positive\" min=\"0\" required></label>\
             <button>Evaluate after full window</button></form></details></article>",
            esc(&experiment.brand),
            esc(&experiment.experiment_type),
            esc(&experiment.name),
            esc(&experiment.status),
            esc(&experiment.status),
            esc(&experiment.hypothesis),
            esc(&experiment.variable),
            esc(&experiment.control_description),
            esc(&experiment.variant_description),
            esc(&experiment.constants.join("; ")),
            experiment.baseline_sends,
            experiment.baseline_positive_reply_rate * 100.0,
            experiment.minimum_sends_per_arm,
            experiment.measurement_days,
            esc(if experiment.decision.is_empty() { "not measured" } else { &experiment.decision }),
            esc(if experiment.confidence.is_empty() { "—" } else { &experiment.confidence }),
            esc(&experiment.id),
            esc(&experiment.id),
            esc(&experiment.id),
            esc(&experiment.id),
        ));
    }
    if snapshot.experiments.is_empty() {
        b.push_str("<p class=\"empty\">No experiments yet. Establish a baseline before starting one; three personalized contacts are discovery, not an A/B test.</p>");
    }
    b.push_str("</div></section>");

    b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Forward-deployed proof briefs</h2><p>Created from real replies. A model may draft or mark one ready; Andrew still approves the build.</p></div><div class=\"gtm-card-grid\">");
    for proof in &snapshot.proofs {
        let account = lead_names
            .get(proof.lead_id.as_str())
            .copied()
            .unwrap_or("Unknown account");
        b.push_str(&format!(
            "<article class=\"gtm-card\"><div class=\"gtm-card-head\"><div><span class=\"strategy-kicker\">{}</span><h3>{}</h3></div><span class=\"gtm-status {}\">{}</span></div>\
             <p><b>Confirmed problem:</b> {}</p><p><b>Current workflow:</b> {}</p>\
             <div class=\"gtm-proof-shape\"><b>Bounded scope</b><p>{}</p><small>Measure: {}<br>Stop: {}</small></div>\
             <p class=\"gtm-meta\">Evidence/data: {} {}</p>\
             <div class=\"gtm-actions\"><form method=\"post\" action=\"/gtm/proof/{}/approved\"><button>Approve</button></form>\
             <form method=\"post\" action=\"/gtm/proof/{}/running\"><button>Running</button></form>\
             <form method=\"post\" action=\"/gtm/proof/{}/passed\"><button>Passed</button></form>\
             <form method=\"post\" action=\"/gtm/proof/{}/failed\"><button class=\"quiet\">Failed</button></form></div></article>",
            esc(&proof.brand),
            esc(account),
            esc(&proof.status),
            esc(&proof.status),
            esc(&proof.problem),
            esc(&proof.current_workflow),
            esc(&proof.scope),
            esc(&proof.success_metric),
            esc(&proof.stop_condition),
            esc(&proof.evidence_available.join("; ")),
            esc(&proof.customer_data.join("; ")),
            esc(&proof.id),
            esc(&proof.id),
            esc(&proof.id),
            esc(&proof.id),
        ));
    }
    if snapshot.proofs.is_empty() {
        b.push_str("<p class=\"empty\">No validated problem has produced a proof brief yet.</p>");
    }
    b.push_str("</div></section>");

    b.push_str("<section class=\"gtm-panel\"><div class=\"strategy-panel-head\"><h2>Attributable market outcomes</h2><p>Replies, corrections, referrals, meetings, and proof events linked to the exact play, evidence, and experiment arm.</p></div><div class=\"gtm-table-wrap\"><table class=\"gtm-table\"><thead><tr><th>When</th><th>Brand</th><th>Outcome</th><th>Account</th><th>Evidence</th></tr></thead><tbody>");
    for outcome in snapshot.outcomes.iter().take(100) {
        let account = lead_names
            .get(outcome.lead_id.as_str())
            .copied()
            .unwrap_or("—");
        b.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td><b>{}</b><small>{}</small></td><td>{}</td><td>{} signal(s)<small>{}</small></td></tr>",
            esc(&outcome.occurred_at),
            esc(&outcome.brand),
            esc(&outcome.kind),
            esc(&outcome.source),
            esc(account),
            outcome.signal_observation_ids.len(),
            esc(&outcome.detail),
        ));
    }
    b.push_str("</tbody></table></div></section></main></div></body></html>");
    b
}

/// Compact, operator-facing pillars distilled from the shared playbook. The
/// full doctrine remains one click away on the strategy hub.
const SHARED_STRATEGY_PILLARS: &[(&str, &str)] = &[
    (
        "Hypothesis-led discovery",
        "Every email tests whether one expensive workflow exists. A correction or referral is a win — success is learning, not a forced meeting.",
    ),
    (
        "Fact vs guess vs question",
        "Only publicly supportable facts are stated as fact. Inferences are framed as guesses. The commercial claim is asked as a question.",
    ),
    (
        "Vantage over seniority",
        "Contacts are chosen by what they can observe, decide, or route — not title rank. Routers get one easy routing question, never the whole case.",
    ),
    (
        "Mechanism, not pitch",
        "Explain why the burden might occur, propose one narrow system concept, and keep human judgment in the loop. No invented ROI or agency capacity language.",
    ),
    (
        "One ask per touch",
        "The sequence is one unfolding investigation across mixed channels, not a stack of paraphrases. Each touch adds a new reason to reply.",
    ),
    (
        "Pre-send discipline",
        "Evidence, role fit, one concrete problem, measurable consequence, and Andrew's voice must all clear before anything leaves the queue.",
    ),
];

fn render_bullet_block(b: &mut String, title: &str, items: &[String], class: &str) {
    if items.is_empty() {
        return;
    }
    b.push_str(&format!(
        "<h3 class=\"strategy-subhead\">{}</h3><ul class=\"strategy-list {}\">",
        esc(title),
        class
    ));
    for item in items {
        b.push_str(&format!("<li>{}</li>", esc(item)));
    }
    b.push_str("</ul>");
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
                        && (t.channel.eq_ignore_ascii_case("email")
                            || (t.channel.eq_ignore_ascii_case("linkedin_or_email")
                                && person.linkedin_status != "connected"))
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
                 <p class=\"role\"><em>Can observe:</em> {}</p>{}",
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
                linkedin_status_control(person),
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
                    esc(channel_label(&touch.channel)),
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
                // Manual social tasks and legacy calls never flow through the
                // send daemon, so give them a one-click "done" in the CRM.
                let manual_linkedin = touch.channel.eq_ignore_ascii_case("linkedin")
                    || touch.channel.eq_ignore_ascii_case("linkedin_request")
                    || touch.channel.eq_ignore_ascii_case("call")
                    || (touch.channel.eq_ignore_ascii_case("linkedin_or_email")
                        && person.linkedin_status == "connected");
                if manual_linkedin && touch.status == "draft" && touch.review_passes == Some(true) {
                    b.push_str(&format!(
                        "<form class=\"approve-form\" method=\"post\" action=\"/execution/touch/{}/done\">\
                         <button class=\"btn sent\">Mark {} done</button></form>",
                        esc(&touch.id),
                        esc(channel_label(&touch.channel)),
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
    let max_stage = accounts
        .iter()
        .flat_map(|account| &account.people)
        .flat_map(|entry| &entry.touches)
        .map(|touch| touch.stage)
        .max()
        .unwrap_or(4)
        .clamp(4, 7);
    b.push_str("<div class=\"desktop-pipeline\">");
    render_sheet_head(b, max_stage);

    let ready_people = accounts
        .iter()
        .map(|account| account.people.len())
        .sum::<usize>();
    if ready_people == 0 {
        b.push_str(
            &format!(
                "<tr><td class=\"empty-sheet\" colspan=\"{}\"><strong>No reviewed sequences ready yet</strong>\
             <span>Partial, rejected, and older drafts stay hidden until every touch in the current sequence passes the copy gate.</span></td></tr>",
                4 + max_stage
            ),
        );
    }

    for (account_index, account) in accounts.iter().enumerate() {
        let mut people = account.people.iter().collect::<Vec<_>>();
        people.sort_by_key(|entry| {
            (
                !entry.person.primary,
                entry.person.email_status != "verified",
                entry.person.status == "suppressed",
            )
        });
        let people = people.into_iter().collect::<Vec<_>>();

        for (slot, entry) in people.iter().copied().enumerate() {
            let stripe = if account_index % 2 == 1 { " alt" } else { "" };
            let start = if slot == 0 { " account-start" } else { "" };
            b.push_str(&format!("<tr class=\"contact-row{stripe}{start}\">"));

            if slot == 0 {
                render_company_cell(b, &account.lead, stripe, people.len());
                render_lead_context_cell(b, &account.lead, stripe, people.len());
            }

            render_person_cell(b, &entry.person);
            render_why_cell(b, &entry.person.why_them, &entry.person.can_observe);
            for stage in 1..=max_stage {
                match entry.touches.iter().find(|touch| touch.stage == stage) {
                    Some(touch) => render_touch_cell(b, touch),
                    None => render_missing_touch(b, stage),
                }
            }
            b.push_str("</tr>");
        }
    }

    b.push_str("</tbody></table></div>");
    render_mobile_people_cards(b, accounts, max_stage);
}

fn render_mobile_people_cards(b: &mut String, accounts: &[ExecutionAccount], max_stage: i64) {
    b.push_str("<section class=\"mobile-pipeline\" aria-label=\"Account pipeline\">");
    if accounts.iter().all(|account| account.people.is_empty()) {
        b.push_str(
            "<div class=\"mobile-empty\"><strong>No reviewed sequences ready yet</strong>\
             <span>Partial, rejected, and older drafts stay hidden until every touch in the current sequence passes the copy gate.</span></div>",
        );
        b.push_str("</section>");
        return;
    }
    for account in accounts {
        let people = account.people.iter().collect::<Vec<_>>();
        let touch_count = people
            .iter()
            .map(|entry| entry.touches.len())
            .sum::<usize>();
        b.push_str(&format!(
            "<details class=\"mobile-account\" data-open-id=\"account-{id}\"><summary>\
             <span><span class=\"brand-tag {brand}\">{brand}</span><strong>{name}</strong>\
             <small>{industry}</small></span><span class=\"mobile-count\">{people} people · {touches} touches</span>\
             </summary><div class=\"mobile-account-body\"><div class=\"mobile-context\">\
             {hypothesis}{signal}{measure}{mechanism}</div>",
            id = esc(&account.lead.id),
            brand = esc(&account.lead.brand),
            name = esc(&account.lead.name),
            industry = esc(&account.lead.industry),
            people = people.len(),
            touches = touch_count,
            hypothesis = context_line("Hypothesis", &account.lead.hypothesis),
            signal = context_line(
                "Signal",
                account
                    .lead
                    .signals
                    .first()
                    .map(String::as_str)
                    .unwrap_or("")
            ),
            measure = context_line("Measure", &account.lead.consequence_metric),
            mechanism = context_line("How", &account.lead.mechanism),
        ));

        for entry in people {
            render_mobile_person(b, entry, max_stage);
        }
        b.push_str("</div></details>");
    }
    b.push_str("</section>");
}

fn render_mobile_person(b: &mut String, entry: &ExecutionPerson, max_stage: i64) {
    let person = &entry.person;
    let sent = entry
        .touches
        .iter()
        .filter(|touch| touch.status == "sent")
        .count();
    let ready = entry
        .touches
        .iter()
        .filter(|touch| touch.status == "draft" && touch.review_passes == Some(true))
        .count();
    let email_drafts = entry
        .touches
        .iter()
        .filter(|touch| {
            touch.status == "draft"
                && (touch.channel.eq_ignore_ascii_case("email")
                    || (touch.channel.eq_ignore_ascii_case("linkedin_or_email")
                        && person.linkedin_status != "connected"))
                && touch.review_passes == Some(true)
        })
        .count();
    b.push_str(&format!(
        "<details class=\"mobile-contact\" data-open-id=\"person-{id}\"><summary><span>\
         <strong>{name}{primary}</strong><small>{title}</small></span>\
         <span class=\"mobile-count\">{sent} sent · {ready} ready</span></summary><div class=\"mobile-contact-body\">\
         <p class=\"mobile-why\"><b>Why this person</b>{why}</p>\
         <p class=\"mobile-why\"><b>Likely access (unverified)</b>{observe}</p>{email}{linkedin}",
        id = esc(&person.id),
        name = esc(&person.name),
        primary = if person.primary { " ★" } else { "" },
        title = esc(&person.title),
        sent = sent,
        ready = ready,
        why = esc(&person.why_them),
        observe = esc(&person.can_observe),
        email = if person.email.is_empty() {
            "<span class=\"muted\">Email not found</span>".to_string()
        } else {
            format!(
                "<a class=\"email\" href=\"mailto:{}\">{}</a>",
                esc(&person.email),
                esc(&person.email)
            )
        },
        linkedin = linkedin_status_control(person),
    ));
    if email_drafts > 0 {
        b.push_str(&format!(
            "<form class=\"approve-form\" method=\"post\" action=\"/execution/approve/{}\">\
             <button class=\"btn sent\">Approve {email_drafts} email draft(s)</button></form>",
            esc(&person.id),
        ));
    }
    b.push_str("<div class=\"mobile-touches\">");
    for stage in 1..=max_stage {
        if let Some(touch) = entry.touches.iter().find(|touch| touch.stage == stage) {
            render_mobile_touch(b, touch, person);
        } else {
            b.push_str(&format!(
                "<div class=\"mobile-touch missing\"><span class=\"touch-tag\">T{stage}</span><span>Not written</span></div>"
            ));
        }
    }
    b.push_str("</div></div></details>");
}

fn render_mobile_touch(b: &mut String, touch: &Touch, person: &Person) {
    let state = if touch.review_passes == Some(false) {
        "blocked"
    } else {
        touch.status.as_str()
    };
    b.push_str(&format!(
        "<article class=\"mobile-touch {state} touch-inline\" data-touch-id=\"{id}\">\
         <div class=\"touch-head\"><span class=\"touch-tag\">{channel} · T{stage}</span>\
         <span class=\"touch-state\">{state}</span><time>{due}</time></div>{subject}\
         <div class=\"message\">{body}</div><details class=\"touch-meta\"><summary>Review details</summary>{purpose}{goal}{qa}</details>",
        state = esc(state),
        id = esc(&touch.id),
        channel = esc(channel_label(&touch.channel)),
        stage = touch.stage,
        due = esc(&display_due(
            &touch.due_at,
            &touch.recipient_timezone,
            touch.day_offset
        )),
        subject = if touch.subject.trim().is_empty() {
            String::new()
        } else {
            format!("<strong class=\"subject\">{}</strong>", esc(&touch.subject))
        },
        body = esc_multiline(&touch.body),
        purpose = detail_line("Internal purpose (not sent)", &touch.purpose),
        goal = detail_line("Internal outcome (not sent)", &touch.goal),
        qa = if touch.review_issues.is_empty() {
            String::new()
        } else {
            detail_line("QA", &touch.review_issues.join(" · "))
        },
    ));
    let manual_linkedin = touch.channel.eq_ignore_ascii_case("linkedin")
        || touch.channel.eq_ignore_ascii_case("linkedin_request")
        || (touch.channel.eq_ignore_ascii_case("linkedin_or_email")
            && person.linkedin_status == "connected");
    if manual_linkedin && touch.status == "draft" && touch.review_passes == Some(true) {
        b.push_str(&format!(
            "<form class=\"approve-form\" method=\"post\" action=\"/execution/touch/{}/done\">\
             <button class=\"btn sent\">Mark {} done</button></form>",
            esc(&touch.id),
            esc(channel_label(&touch.channel)),
        ));
    }
    b.push_str("</article>");
}

fn render_sheet_head(b: &mut String, max_stage: i64) {
    b.push_str(
        "<table class=\"crm-sheet\"><colgroup><col class=\"c-company\"><col class=\"c-context\">\
         <col class=\"c-person\"><col class=\"c-why\">",
    );
    for _ in 1..=max_stage {
        b.push_str("<col class=\"c-touch\">");
    }
    b.push_str(
        "</colgroup><thead><tr><th class=\"pin\">Company</th><th>Company context</th>\
         <th>Name</th><th>Internal role fit (not sent)</th>",
    );
    for stage in 1..=max_stage {
        b.push_str(&format!("<th>T{stage}</th>"));
    }
    b.push_str("</tr></thead><tbody>");
}

fn render_company_cell(b: &mut String, lead: &Lead, stripe: &str, rows: usize) {
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
        "<td class=\"company pin{stripe}\" rowspan=\"{rows}\"><span class=\"brand-tag {brand}\">{brand}</span>\
         <strong>{name}</strong><small>{details}</small>{domain}</td>",
        brand = esc(&lead.brand),
        name = esc(&lead.name),
        details = esc(&details),
        rows = rows,
    ));
}

fn render_lead_context_cell(b: &mut String, lead: &Lead, stripe: &str, rows: usize) {
    let hypothesis = first_non_empty(&[&lead.hypothesis, &lead.thesis]);
    let observed = lead
        .observed_facts
        .first()
        .map(String::as_str)
        .unwrap_or("");
    let signal = lead.signals.first().map(String::as_str).unwrap_or("");
    b.push_str(&format!(
        "<td class=\"context{stripe}\" rowspan=\"{rows}\">{hypothesis}{observed}{signal}{measure}{mechanism}</td>",
        hypothesis = context_line("Hypothesis", hypothesis),
        observed = context_line("Observed", observed),
        signal = context_line("Signal", signal),
        measure = context_line("Measure", &lead.consequence_metric),
        mechanism = context_line("How", &lead.mechanism),
        rows = rows,
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
         <span class=\"person-status {status}\">{status}</span>{linkedin}</td>",
        name = esc(&person.name),
        primary = if person.primary { " ★" } else { "" },
        title = esc(&person.title),
        status = esc(&person.status),
        linkedin = linkedin_status_control(person),
    ));
}

fn linkedin_status_control(person: &Person) -> String {
    if person.linkedin_url.trim().is_empty() {
        return String::new();
    }
    let current = match person.linkedin_status.as_str() {
        "requested" | "connected" | "not_connected" => person.linkedin_status.as_str(),
        _ => "unknown",
    };
    let option = |value: &str, label: &str| {
        format!(
            "<option value=\"{}\"{}>{}</option>",
            value,
            if current == value { " selected" } else { "" },
            label
        )
    };
    format!(
        "<form class=\"linkedin-state\" method=\"post\" action=\"/execution/person/{}/linkedin\">\
         <a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">LinkedIn</a>\
         <select name=\"status\" aria-label=\"LinkedIn connection status\" onchange=\"this.form.submit()\">{}{}{}{}</select></form>",
        esc(&person.id),
        esc(&person.linkedin_url),
        option("unknown", "Unknown"),
        option("requested", "Requested"),
        option("connected", "Connected"),
        option("not_connected", "Not connected"),
    )
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
            "<small><b>Likely access (unverified):</b> {}</small>",
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
    let due = display_due(&touch.due_at, &touch.recipient_timezone, touch.day_offset);
    b.push_str(&format!(
        "<td class=\"touch {state}\" data-touch-id=\"{id}\"><article class=\"touch-inline\">\
         <div class=\"touch-head\"><span class=\"touch-tag\">{channel} · T{stage}</span><span class=\"touch-state\">{state}</span>\
         {written}<time>{due}</time></div>{subject}<div class=\"message\">{body}</div>\
         <details class=\"touch-meta\"><summary>Review details</summary>{purpose}{goal}{qa}</details></article></td>",
        state = esc(state),
        id = esc(&touch.id),
        channel = esc(channel_label(&touch.channel)),
        stage = touch.stage,
        written = written_at_html(&touch.created_at),
        due = esc(&due),
        subject = if touch.subject.trim().is_empty() {
            String::new()
        } else {
            format!("<strong class=\"subject\">{}</strong>", esc(&touch.subject))
        },
        body = esc_multiline(&touch.body),
        purpose = detail_line("Internal purpose (not sent)", &touch.purpose),
        goal = detail_line("Internal outcome (not sent)", &touch.goal),
        qa = if touch.review_issues.is_empty() {
            String::new()
        } else {
            detail_line("QA", &touch.review_issues.join(" · "))
        },
    ));
}

fn channel_label(channel: &str) -> &str {
    match channel.to_ascii_lowercase().as_str() {
        "linkedin_request" => "LinkedIn request",
        "linkedin_or_email" => "LinkedIn / email",
        "linkedin" => "LinkedIn",
        "email" => "Email",
        "" => "Touch",
        _ => channel,
    }
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
    b.push_str("<div class=\"desktop-pipeline\">");
    render_sheet_head(b, 7);
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
    b.push_str("</tbody></table></div>");
    render_mobile_research_cards(b, accounts);
}

fn render_mobile_research_cards(b: &mut String, accounts: &[CrmAccount]) {
    b.push_str("<section class=\"mobile-pipeline\" aria-label=\"Research pipeline\">");
    for account in accounts {
        let contacts = account.contacts.iter().take(5).collect::<Vec<_>>();
        b.push_str(&format!(
            "<details class=\"mobile-account\" data-open-id=\"research-account-{id}\"><summary>\
             <span><span class=\"brand-tag {brand}\">{brand}</span><strong>{name}</strong>\
             <small>{industry}</small></span><span class=\"mobile-count\">{people} people</span>\
             </summary><div class=\"mobile-account-body\"><div class=\"mobile-context\">\
             {hypothesis}{signal}{measure}{mechanism}</div>",
            id = esc(&account.id),
            brand = esc(&account.brand),
            name = esc(&account.name),
            industry = esc(&account.industry),
            people = contacts.len(),
            hypothesis = context_line("Hypothesis", &account.hypothesis),
            signal = context_line(
                "Signal",
                account.signals.first().map(String::as_str).unwrap_or("")
            ),
            measure = context_line("Measure", &account.consequence_metric),
            mechanism = context_line("How", &account.mechanism),
        ));
        for contact in contacts {
            b.push_str(&format!(
                "<details class=\"mobile-contact\" data-open-id=\"research-person-{id}\"><summary><span>\
                 <strong>{name}{primary}</strong><small>{title}</small></span>\
                 <span class=\"mobile-count\">research</span></summary><div class=\"mobile-contact-body\">\
                 <p class=\"mobile-why\"><b>Why this person</b>{why}</p>\
                 <p class=\"mobile-why\"><b>Likely access (unverified)</b>{observe}</p><div class=\"mobile-touches\">",
                id = esc(&contact.id),
                name = esc(&contact.name),
                primary = if contact.primary { " ★" } else { "" },
                title = esc(&contact.title),
                why = esc(&contact.why_them),
                observe = esc(&contact.can_observe),
            ));
            for stage in 1..=7 {
                if let Some(touch) = contact.touches.iter().find(|touch| touch.stage == stage) {
                    b.push_str(&format!(
                        "<article class=\"mobile-touch {state} touch-inline\" data-touch-id=\"research-touch-{contact}-{stage}\">\
                         <div class=\"touch-head\"><span class=\"touch-tag\">{channel} · T{stage}</span>\
                         <time>Day {day} · not scheduled</time></div>{subject}<div class=\"message\">{body}</div>\
                         <details class=\"touch-meta\"><summary>Plan details</summary>{purpose}{goal}</details></article>",
                        state = touch.status.label(),
                        contact = esc(&contact.id),
                        stage = touch.stage,
                        channel = esc(channel_label(&touch.channel)),
                        day = touch.day_offset,
                        subject = if touch.subject.trim().is_empty() {
                            String::new()
                        } else {
                            format!(
                                "<strong class=\"subject\">{}</strong>",
                                esc(&touch.subject)
                            )
                        },
                        body = esc_multiline(&touch.body),
                        purpose = detail_line("Purpose", &touch.purpose),
                        goal = detail_line("Goal", &touch.goal),
                    ));
                } else {
                    b.push_str(&format!(
                        "<div class=\"mobile-touch missing\"><span class=\"touch-tag\">T{stage}</span><span>Not written</span></div>"
                    ));
                }
            }
            b.push_str("</div></div></details>");
        }
        b.push_str("</div></details>");
    }
    b.push_str("</section>");
}

fn render_research_touch(b: &mut String, touch: &CrmTouch) {
    b.push_str(&format!(
        "<td class=\"touch {state}\"><article class=\"touch-inline\"><div class=\"touch-head\">\
         <span class=\"touch-tag\">{channel} · T{stage}</span>{written}<time>Day {day} · time not scheduled</time></div>\
         {subject}<div class=\"message\">{body}</div><details class=\"touch-meta\"><summary>Plan details</summary>{purpose}{goal}</details></article></td>",
        state = touch.status.label(),
        channel = esc(channel_label(&touch.channel)),
        stage = touch.stage,
        written = written_at_html(&touch.created_at),
        day = touch.day_offset,
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
    b.push_str("<div class=\"desktop-pipeline\">");
    render_sheet_head(b, 4);
    b.push_str(
        "<tr><td class=\"empty-sheet\" colspan=\"8\"><strong>No companies yet</strong>\
         <span>Source a campaign and the company, primary contact, reply rationale, and current touch schedule will appear here.</span></td></tr>\
         </tbody></table></div><section class=\"mobile-pipeline mobile-empty\"><strong>No companies yet</strong>\
         <span>Ask Spruce Leaf to source accounts and they will appear here.</span></section>",
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

/// Human-readable authored time for CRM copy. This is deliberately separate
/// from `due_at`: rescheduling a touch must never make an old draft look new.
fn display_written_at(created_at: &str) -> String {
    if created_at.trim().is_empty() {
        return String::new();
    }
    let Ok(created) = DateTime::parse_from_rfc3339(created_at) else {
        return format!("Drafted {created_at}");
    };
    format!(
        "Drafted {}",
        created
            .with_timezone(&Local)
            .format("%a %-d %b %Y · %-I:%M %p %Z")
    )
}

fn written_at_html(created_at: &str) -> String {
    let label = display_written_at(created_at);
    if label.is_empty() {
        String::new()
    } else {
        format!(
            "<time class=\"written-at\" datetime=\"{}\">{}</time>",
            esc(created_at),
            esc(&label)
        )
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

fn format_cad(cents: i64) -> String {
    let sign = if cents < 0 { "-" } else { "" };
    let cents = cents.saturating_abs();
    format!("{sign}CAD ${}.{:02}", cents / 100, cents % 100)
}

fn select_options(values: &[&str], current: &str) -> String {
    values
        .iter()
        .map(|value| {
            format!(
                "<option value=\"{}\" {}>{}</option>",
                esc(value),
                if *value == current { "selected" } else { "" },
                esc(&value.replace('_', " ")),
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn commercial_offer_options(profile: &BusinessProfile, current: &str) -> String {
    let mut options = format!(
        "<option value=\"\" {}>unselected</option>",
        if current.is_empty() { "selected" } else { "" }
    );
    for offer in &profile.commercial.offers {
        options.push_str(&format!(
            "<option value=\"{}\" {}>{} · {}</option>",
            esc(&offer.key),
            if offer.key == current { "selected" } else { "" },
            esc(&offer.name),
            esc(&offer.lane.replace('_', " ")),
        ));
    }
    options
}

fn optional_cad_input(value: Option<i64>) -> String {
    value
        .map(|cents| format!("{}.{:02}", cents / 100, cents.saturating_abs() % 100))
        .unwrap_or_default()
}

fn optional_i64_input(value: Option<i64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn optional_bps_input(value: Option<i64>) -> String {
    value
        .map(|bps| format!("{:.2}", bps as f64 / 100.0))
        .unwrap_or_default()
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
.state-pill,.email-state{color:var(--dim);border:1px solid var(--edge);border-radius:20px;padding:1px 7px;font-size:10px;margin-left:5px}.knowledge-cites{display:block;color:var(--dim);font-size:10px;margin-top:5px;max-width:220px;overflow-wrap:anywhere}.approve-form{margin:8px 0}.linkedin-state{display:flex;align-items:center;gap:6px;margin:7px 0}.linkedin-state a{color:var(--sky);font-size:11px;text-decoration:none}.linkedin-state select{padding:2px 5px;border:1px solid var(--edge);border-radius:5px;background:#fff;color:var(--dim);font:inherit;font-size:10px}.execution-touch .status{float:right}.activity{background:var(--card);border:1px solid var(--edge);border-radius:12px;padding:14px 16px;margin-top:18px}.activity ol{list-style:none;padding:0;margin:10px 0 0}.activity li{display:grid;grid-template-columns:190px auto 90px 1fr;gap:8px;border-top:1px solid var(--edge);padding:7px 0;font-size:12px}.activity time{color:var(--dim)}\
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
.crm-shell { height: 100vh; height: 100dvh; display: flex; flex-direction: column; }
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
.campaign-audit { position: sticky; left: 0; z-index: 7; display: flex; align-items: baseline; gap: 12px; padding: 9px 12px; border-bottom: 1px solid var(--line); font-size: 11px; }
.campaign-audit.pass { color: var(--green); background: var(--green-tint); }
.campaign-audit.hold { color: var(--red); background: var(--red-tint); }
.campaign-audit span { color: var(--muted); }
.campaign-audit ul { margin: 0 0 0 auto; padding-left: 18px; max-width: 700px; }
.crm-sheet { border-collapse: separate; border-spacing: 0; table-layout: fixed; width: max-content; min-width: 100%; }
.c-company { width: 210px; }.c-context { width: 360px; }.c-person { width: 220px; }.c-why { width: 280px; }.c-touch { width: 300px; }
.c-sponsor-evidence { width: 360px; }.c-sponsor-subject { width: 230px; }.c-sponsor-email { width: 560px; }.c-sponsor-qa { width: 300px; }
.sponsorship-sheet td { height: auto; min-height: 120px; }
.sponsorship-sheet tr.blocked td { background: #fffafa; }
.sponsorship-sheet tr.blocked td.company.pin { border-left: 4px solid var(--red); }
.sponsorship-sheet tr.ready td.company.pin { border-left: 4px solid var(--green); }
.sponsor-evidence ul { margin: 0; padding-left: 16px; color: var(--muted); font-size: 10.5px; line-height: 1.45; }
.sponsor-evidence li + li { margin-top: 5px; }
.sponsor-subject .subject { display: block; font-size: 12px; line-height: 1.45; }
.sponsor-message .message { color: var(--ink); white-space: normal; font-size: 11.5px; line-height: 1.55; overflow-wrap: anywhere; }
.sponsor-state .touch-tag { margin-bottom: 7px; }
.sponsor-state .sponsor-qa { margin: 0 0 8px; padding: 7px; border-radius: 6px; font-size: 10.5px; line-height: 1.4; }
.sponsor-state .sponsor-qa.ok { color: var(--green); background: var(--green-tint); }
.sponsor-state .sponsor-qa.fail { color: var(--red); background: var(--red-tint); }
.sponsor-state > small { display: block; color: var(--faint); font-size: 10px; line-height: 1.4; }
.sponsor-row.blocked .touch-tag { color: var(--red); border-color: #f3c7c3; background: var(--red-tint); }
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
.linkedin-state { display: flex; align-items: center; gap: 5px; margin-top: 6px; }
.linkedin-state a { color: var(--blue); font-size: 10px; font-weight: 650; text-decoration: none; }
.linkedin-state select { max-width: 112px; padding: 2px 4px; border: 1px solid var(--line); border-radius: 5px; background: #fff; color: var(--muted); font: inherit; font-size: 9.5px; }
.why p { margin: 0; color: var(--muted); font-size: 11.5px; line-height: 1.45; }
.why small { display: block; margin-top: 7px; color: var(--faint); font-size: 10.5px; }
.why small b { color: var(--muted); }
.muted { color: var(--faint); }
.empty-person { background-image: repeating-linear-gradient(135deg, transparent, transparent 6px, rgba(95,99,104,.025) 6px, rgba(95,99,104,.025) 12px) !important; }
.touch { cursor: default; }
.touch-tag {
  display: inline-block; padding: 1px 6px; border: 1px solid #d2e3fc; border-radius: 4px;
  color: var(--blue); background: var(--blue-tint); font-size: 9.5px; font-weight: 750; letter-spacing: .025em; text-transform: uppercase;
}
.touch.sent .touch-tag { color: var(--green); border-color: #c6e4cf; background: var(--green-tint); }
.touch.blocked .touch-tag, .touch.failed .touch-tag { color: var(--red); border-color: #f3c7c3; background: var(--red-tint); }
.touch.writing { background: linear-gradient(110deg, #f7faff 30%, #eaf2ff 50%, #f7faff 70%) !important; background-size: 220% 100% !important; animation: draft-shimmer 1.8s linear infinite; }
.touch.writing .touch-tag { color: #3c6fd1; border-color: #c9dafb; background: #edf3ff; }
.touch.reviewing { background: #fbf8ff !important; }
.touch.reviewing .touch-tag { color: #7651b6; border-color: #ddcff4; background: #f3edff; }
@keyframes draft-shimmer { from { background-position: 180% 0; } to { background-position: -40% 0; } }
.touch time { display: block; margin: 5px 0 4px; color: var(--blue-strong); font-size: 10.5px; font-weight: 650; }
.touch time.written-at { margin: 6px 0 0; color: var(--faint); font-size: 9.5px; font-weight: 550; }
.touch time.written-at + time { margin-top: 2px; }
.touch:hover { background: var(--blue-tint) !important; }
.touch-head { min-height: 24px; }
.touch-inline > .subject { display: block; margin: 8px 0 6px; color: var(--ink); font-size: 11.5px; }
.touch-inline > .message { color: var(--ink); white-space: normal; font-size: 11.5px; line-height: 1.52; overflow-wrap: anywhere; }
.touch-meta { margin-top: 10px; padding-top: 7px; border-top: 1px solid var(--line); }
.touch-meta > summary { cursor: pointer; list-style: none; color: var(--faint); font-size: 9.5px; font-weight: 650; text-transform: uppercase; }
.touch-meta > summary::-webkit-details-marker { display: none; }
.touch-meta > summary::before { content: '+ '; }
.touch-meta[open] > summary::before { content: '− '; }
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
  .c-company { width: 170px; }.c-context { width: 300px; }.c-person { width: 190px; }.c-why { width: 230px; }.c-touch { width: 280px; }
}

/* ---- Portfolio: brand tabs + sub bar + hub cards --------------------- */
a.brand-lockup { text-decoration: none; color: var(--ink); flex: 0 0 auto; }
.wordmark { font-weight: 650; font-size: 15px; letter-spacing: -.01em; }
.wordmark-dim { color: var(--faint); font-weight: 500; margin-left: 2px; }
.biz-tabs { display: flex; align-items: stretch; gap: 2px; height: 100%; min-width: 0; overflow-x: auto; scrollbar-width: none; }
.biz-tabs::-webkit-scrollbar { display: none; }
.biz-tab {
  position: relative; display: flex; align-items: center; gap: 8px; height: 100%;
  padding: 0 14px; font-size: 13.5px; font-weight: 550; color: var(--muted);
  text-decoration: none; white-space: nowrap;
}
.biz-tab:hover { color: var(--ink); }
.biz-tab.active { color: var(--blue); }
.biz-tab.active::after {
  content: ''; position: absolute; left: 12px; right: 12px; bottom: -1px;
  height: 2px; border-radius: 2px 2px 0 0; background: var(--blue);
}
.biz-tab .count {
  font-size: 11px; font-weight: 600; color: var(--faint);
  background: #f1f3f4; border-radius: 999px; padding: 1px 7px;
}
.biz-tab.active .count { color: var(--blue); background: var(--blue-tint); }
.subbar {
  flex: 0 0 auto; display: flex; align-items: flex-end; justify-content: space-between;
  gap: 24px; padding: 16px 20px 13px; background: #fff; border-bottom: 1px solid var(--line);
}
.subbar-left { min-width: 0; }
.subbar h1 { margin: 0; font-size: 20px; line-height: 1.15; font-weight: 650; letter-spacing: -.015em; }
.subbar .tagline { margin: 4px 0 0; color: var(--muted); font-size: 13px; max-width: 640px; }
.subbar-stats { display: flex; gap: 26px; flex-shrink: 0; }
.subbar-stats .stat { text-align: right; }
.subbar-stats .n { font-size: 19px; font-weight: 650; letter-spacing: -.02em; color: var(--ink); }
.subbar-stats .l { margin-top: 1px; font-size: 10.5px; color: var(--faint); text-transform: uppercase; letter-spacing: .04em; }
.portfolio {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(290px, 1fr));
  gap: 18px; padding: 24px; max-width: 1180px;
}
.brand-card {
  display: flex; flex-direction: column; gap: 12px; min-height: 156px; padding: 20px;
  border: 1px solid var(--line); border-radius: 14px; background: #fff; color: var(--ink);
  text-decoration: none; box-shadow: 0 1px 2px rgba(60,64,67,.06);
  transition: border-color .12s, box-shadow .12s, transform .12s;
}
.brand-card:hover { border-color: #d2e3fc; box-shadow: 0 6px 18px rgba(26,115,232,.12); transform: translateY(-1px); }
.brand-card-top { display: flex; align-items: center; justify-content: space-between; }
.brand-chip { font-size: 13px; font-weight: 700; padding: 3px 10px; border-radius: 7px; color: var(--blue); background: var(--blue-tint); }
.brand-chip.wapahki { color: #4f762b; background: #edf5e5; }
.brand-chip.outagehub { color: #0b57d0; background: #e8f0fe; }
.brand-card-count { font-size: 12px; color: var(--faint); }
.brand-card-tagline { flex: 1 1 auto; margin: 0; color: var(--muted); font-size: 13px; line-height: 1.5; }
.brand-card-open { font-size: 12.5px; font-weight: 600; color: var(--blue); }
.brand-card-open.secondary { color: var(--muted); }
.brand-card-goal { margin: 0; color: var(--ink); font-size: 12.5px; line-height: 1.45; }
.brand-card-goal b { color: var(--faint); font-weight: 650; text-transform: uppercase; letter-spacing: .03em; font-size: 10.5px; display: block; margin-bottom: 3px; }
.brand-card-actions { display: flex; gap: 14px; margin-top: auto; }
.portfolio-lead { padding: 18px 24px 0; max-width: 1180px; color: var(--muted); font-size: 13px; }
.portfolio-lead a { color: var(--blue); font-weight: 600; text-decoration: none; }
.calendar-scroll { padding: 16px 18px 28px; background: #f7f9fc; }
.calendar-policy {
  display: flex; align-items: center; justify-content: space-between; gap: 20px;
  max-width: 1500px; margin: 0 auto 12px; padding: 12px 14px;
  border: 1px solid #d2e3fc; border-radius: 10px; background: #edf4ff; color: var(--muted);
}
.calendar-policy > div { display: flex; align-items: baseline; gap: 10px; min-width: 0; }
.calendar-policy b { color: var(--blue-strong); font-size: 12px; white-space: nowrap; }
.calendar-policy span { font-size: 12px; line-height: 1.4; }
.calendar-policy .calendar-policy-total { color: var(--blue); font-weight: 750; white-space: nowrap; }
.calendar-alert {
  max-width: 1500px; margin: 0 auto 12px; padding: 10px 13px; border: 1px solid #f4c7c3;
  border-radius: 9px; background: #fce8e6; color: #7c2d26; font-size: 12px;
}
.calendar-brand-strip {
  display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px;
  max-width: 1500px; margin: 0 auto 14px;
}
.calendar-brand-summary {
  display: flex; align-items: center; gap: 12px; min-width: 0; padding: 11px 13px;
  border: 1px solid var(--line); border-radius: 10px; background: #fff; color: var(--ink); text-decoration: none;
}
.calendar-brand-summary:hover { border-color: #bdd2f5; box-shadow: 0 2px 8px rgba(26,115,232,.09); }
.calendar-brand-summary > span:nth-child(2) { min-width: 0; }
.calendar-brand-summary > span b { display: block; font-size: 12px; }
.calendar-brand-summary small { display: block; color: var(--faint); font-size: 10px; margin-top: 1px; }
.calendar-brand-summary i { margin-left: auto; color: var(--blue); font-size: 10.5px; font-style: normal; white-space: nowrap; }
.calendar-grid {
  display: grid; grid-template-columns: repeat(5, minmax(225px, 1fr)); gap: 10px;
  max-width: 1500px; margin: 0 auto;
}
.calendar-day {
  min-width: 0; overflow: hidden; border: 1px solid var(--line); border-radius: 11px;
  background: #fff; box-shadow: 0 1px 2px rgba(60,64,67,.04);
}
.calendar-day.today { border-color: #9fc0f5; box-shadow: 0 0 0 1px #d2e3fc; }
.calendar-day > header {
  display: flex; align-items: center; justify-content: space-between; padding: 10px 11px;
  border-bottom: 1px solid var(--line); background: #f8faff;
}
.calendar-day > header div { display: flex; align-items: baseline; gap: 6px; }
.calendar-day > header span { color: var(--blue); font-size: 10px; font-weight: 750; text-transform: uppercase; }
.calendar-day > header b { font-size: 13px; }
.calendar-day > header strong { color: var(--faint); font-size: 10.5px; font-weight: 650; }
.calendar-lane { padding: 9px 10px 10px; border-bottom: 1px solid #edf0f4; }
.calendar-lane:last-child { border-bottom: 0; }
.calendar-lane-head { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }
.calendar-lane-head a { color: var(--ink); font-size: 10.5px; font-weight: 700; text-decoration: none; }
.calendar-lane-head > span { color: var(--muted); font-size: 10px; font-weight: 650; white-space: nowrap; }
.calendar-lane-head small { margin-left: 5px; color: var(--faint); font-size: 8.5px; font-weight: 500; }
.calendar-meter { height: 3px; margin: 5px 0 7px; overflow: hidden; border-radius: 99px; background: #edf0f4; }
.calendar-meter i { display: block; height: 100%; border-radius: inherit; background: #6d8fbf; }
.calendar-lane.gnk .calendar-meter i { background: #7c6bb0; }
.calendar-lane.wapahki .calendar-meter i { background: #6d914a; }
.calendar-lane.outagehub .calendar-meter i { background: #4285f4; }
.calendar-open { margin: 0; color: #a1a7af; font-size: 9.5px; }
.calendar-events { display: grid; gap: 4px; }
.calendar-event { display: grid; grid-template-columns: auto minmax(0,1fr); gap: 5px; align-items: baseline; }
.calendar-event time { color: var(--faint); font-size: 8.5px; white-space: nowrap; }
.calendar-event span { min-width: 0; overflow: hidden; color: var(--muted); font-size: 9px; text-overflow: ellipsis; white-space: nowrap; }
.calendar-event span b { color: var(--ink); font-weight: 600; }
.calendar-more { margin: 2px 0 0; color: var(--blue); font-size: 9px; font-weight: 600; }
.surface-tabs {
  display: flex; align-items: center; gap: 2px; height: 28px; margin-right: 10px;
  padding: 2px; border: 1px solid var(--line); border-radius: 999px; background: #f8f9fb;
  flex: 0 0 auto;
}
.surface-tab {
  display: inline-flex; align-items: center; height: 100%; padding: 0 12px;
  border-radius: 999px; color: var(--muted); font-size: 12px; font-weight: 600;
  text-decoration: none; white-space: nowrap;
}
.surface-tab:hover { color: var(--ink); }
.surface-tab.active { color: #fff; background: var(--blue); }
.strategy-strip {
  display: flex; align-items: flex-start; justify-content: space-between; gap: 18px;
  padding: 12px 20px; background: linear-gradient(180deg, #f7faff 0%, #fff 100%);
  border-bottom: 1px solid var(--line);
}
.strategy-strip-main { min-width: 0; }
.strategy-kicker {
  display: inline-block; margin-bottom: 4px; color: var(--blue); font-size: 10.5px;
  font-weight: 700; letter-spacing: .05em; text-transform: uppercase;
}
.strategy-strip-goal { margin: 0; color: var(--ink); font-size: 13.5px; line-height: 1.45; max-width: 920px; }
.strategy-strip-motions { margin: 4px 0 0; color: var(--faint); font-size: 12px; }
.strategy-strip-link {
  flex: 0 0 auto; margin-top: 2px; color: var(--blue); font-size: 12.5px;
  font-weight: 650; text-decoration: none; white-space: nowrap;
}
.strategy-scroll {
  flex: 1 1 auto; overflow: auto; padding: 20px 24px 48px; background: #f6f8fb;
}
.strategy-panel {
  max-width: 1100px; margin: 0 auto 18px; padding: 20px 22px;
  background: #fff; border: 1px solid var(--line); border-radius: 14px;
  box-shadow: 0 1px 2px rgba(60,64,67,.05);
}
.strategy-panel-head { margin-bottom: 14px; }
.strategy-panel-head h2 { margin: 0; font-size: 17px; letter-spacing: -.01em; }
.strategy-panel-head p { margin: 4px 0 0; color: var(--muted); font-size: 13px; max-width: 720px; }
.doctrine-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px;
}
.doctrine-card {
  padding: 14px; border: 1px solid var(--line); border-radius: 12px; background: var(--blue-wash);
}
.doctrine-card h3 { margin: 0 0 6px; font-size: 13px; color: var(--blue-strong); }
.doctrine-card p { margin: 0; color: var(--ink); font-size: 12.5px; line-height: 1.45; }
.doctrine-full { margin-top: 14px; border-top: 1px solid var(--line); padding-top: 10px; }
.doctrine-full summary {
  cursor: pointer; color: var(--blue); font-size: 12.5px; font-weight: 650; list-style: none;
}
.doctrine-full .prose {
  margin-top: 10px; padding: 12px 14px; border-radius: 10px; background: #f8fafc;
  color: var(--ink); font-size: 12.5px; line-height: 1.55; white-space: pre-wrap;
}
.strategy-brand-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 14px;
}
.strategy-brand-card {
  display: flex; flex-direction: column; gap: 8px; padding: 16px;
  border: 1px solid var(--line); border-radius: 12px; background: #fff;
}
.strategy-brand-top { display: flex; align-items: center; justify-content: space-between; }
.strategy-brand-top a { color: var(--blue); font-size: 12.5px; font-weight: 650; text-decoration: none; }
.strategy-summary { margin: 0; color: var(--muted); font-size: 13px; line-height: 1.45; }
.strategy-brand-card h3 {
  margin: 8px 0 0; color: var(--faint); font-size: 10.5px; font-weight: 700;
  letter-spacing: .04em; text-transform: uppercase;
}
.strategy-list { margin: 4px 0 0; padding-left: 18px; color: var(--ink); font-size: 12.5px; line-height: 1.45; }
.strategy-list li { margin: 0 0 5px; }
.strategy-list.motions li { margin-bottom: 8px; }
.motion-kind {
  display: inline-block; margin-left: 4px; padding: 1px 7px; border-radius: 999px;
  background: var(--blue-tint); color: var(--blue); font-size: 10px; font-weight: 700;
  letter-spacing: .03em; text-transform: uppercase;
}
.strategy-motion, .strategy-intro { margin: 0; font-size: 12.5px; line-height: 1.45; color: var(--ink); }
.strategy-card-links { display: flex; gap: 14px; margin-top: auto; padding-top: 8px; }
.strategy-card-links a { color: var(--blue); font-size: 12.5px; font-weight: 650; text-decoration: none; }
.strategy-lead { margin: 0 0 10px; font-size: 14px; line-height: 1.5; color: var(--ink); }
.strategy-meta { margin: 0 0 12px; color: var(--muted); font-size: 12.5px; }
.strategy-subhead {
  margin: 16px 0 6px; color: var(--faint); font-size: 11px; font-weight: 700;
  letter-spacing: .04em; text-transform: uppercase;
}
.playbook-hero {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 12px; margin-bottom: 8px;
}
.playbook-hero > div {
  padding: 12px 14px; border: 1px solid var(--line); border-radius: 12px; background: var(--blue-wash);
}
.playbook-hero p { margin: 4px 0 0; font-size: 13px; line-height: 1.45; }
.motion-grid, .capacity-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px;
}
.motion-card, .capacity-card, .timing-rule {
  padding: 12px 14px; border: 1px solid var(--line); border-radius: 12px; background: #fbfcfe;
}
.motion-card-top { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 6px; }
.motion-card p, .timing-rule p { margin: 0; font-size: 12.5px; line-height: 1.45; color: var(--ink); }
.capacity-card span { display: block; color: var(--faint); font-size: 10.5px; font-weight: 650; letter-spacing: .03em; text-transform: uppercase; }
.capacity-card strong { display: block; margin-top: 4px; font-size: 14px; font-weight: 650; color: var(--ink); }
.timing-rules { display: grid; gap: 10px; }
.timing-rule small { display: block; margin-top: 6px; color: var(--faint); font-size: 11px; }
.source-list { margin: 0; padding-left: 18px; font-size: 12.5px; line-height: 1.55; }
.source-list a { color: var(--blue); text-decoration: none; font-weight: 600; }
.strategy-foot { margin: 18px 0 0; }
.strategy-foot a { color: var(--blue); font-weight: 650; text-decoration: none; }
.empty-inline { color: var(--faint); font-size: 13px; }
.gtm-scroll { flex: 1 1 auto; overflow: auto; padding: 20px 24px 48px; background: #f6f8fb; }
.gtm-doctrine {
  max-width: 1180px; margin: 0 auto 18px; display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px;
}
.gtm-doctrine > div { padding: 16px; border: 1px solid #cfdcf4; border-radius: 14px; background: #f5f8ff; }
.gtm-doctrine h2 { margin: 2px 0 6px; font-size: 14px; line-height: 1.35; }
.gtm-doctrine p { margin: 0; color: var(--muted); font-size: 12.5px; line-height: 1.5; }
.gtm-panel {
  max-width: 1180px; margin: 0 auto 18px; padding: 20px 22px;
  background: #fff; border: 1px solid var(--line); border-radius: 14px;
  box-shadow: 0 1px 2px rgba(60,64,67,.05);
}
.gtm-card-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(330px, 1fr)); gap: 12px; }
.gtm-card { padding: 15px; border: 1px solid var(--line); border-radius: 12px; background: #fff; }
.gtm-card-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
.gtm-card-head h3 { margin: 0; font-size: 14px; }
.gtm-card p { margin: 9px 0; color: var(--ink); font-size: 12.5px; line-height: 1.5; }
.gtm-meta { color: var(--faint) !important; font-size: 11.5px !important; }
.gtm-status { display: inline-flex; padding: 2px 7px; border-radius: 999px; background: #eef2f7; color: var(--muted); font-size: 10px; font-weight: 700; white-space: nowrap; }
.gtm-status.proven, .gtm-status.passed, .gtm-status.verified, .gtm-status.complete { background: #e4f4e8; color: #23733a; }
.gtm-status.testing, .gtm-status.running, .gtm-status.ready { background: var(--blue-tint); color: var(--blue); }
.gtm-status.failed, .gtm-status.rejected, .gtm-status.retired, .gtm-status.cancelled { background: #fce8e6; color: #a50e0e; }
.gtm-score { display: inline-flex; padding: 4px 8px; border-radius: 8px; background: var(--blue-tint); color: var(--blue); font-size: 12px; font-weight: 750; white-space: nowrap; }
.gtm-proof-shape { margin: 10px 0; padding: 11px 12px; border-left: 3px solid var(--blue); background: var(--blue-wash); border-radius: 0 9px 9px 0; }
.gtm-proof-shape p { margin: 4px 0; }
.gtm-proof-shape small { color: var(--muted); line-height: 1.45; }
.gtm-actions { display: flex; flex-wrap: wrap; gap: 7px; margin-top: 12px; }
.gtm-actions form { margin: 0; }
.gtm-actions button, .gtm-form button { border: 0; border-radius: 7px; padding: 6px 10px; background: var(--blue); color: #fff; font: inherit; font-size: 11.5px; font-weight: 650; cursor: pointer; }
.gtm-actions button.quiet { background: #eef1f5; color: var(--muted); }
.gtm-split { display: grid; grid-template-columns: minmax(500px, 1.15fr) minmax(330px, .85fr); gap: 18px; }
.gtm-split h3 { margin: 0 0 8px; font-size: 13px; }
.gtm-table-wrap { overflow-x: auto; }
.gtm-table { width: 100%; border-collapse: collapse; font-size: 11.5px; }
.gtm-table th { text-align: left; padding: 7px 8px; color: var(--faint); border-bottom: 1px solid var(--line); font-size: 10px; text-transform: uppercase; letter-spacing: .04em; }
.gtm-table td { vertical-align: top; padding: 9px 8px; border-bottom: 1px solid #edf0f3; line-height: 1.4; }
.gtm-table td small { display: block; margin-top: 3px; color: var(--faint); font-size: 10.5px; }
.gtm-observations { display: grid; gap: 8px; max-height: 520px; overflow: auto; }
.gtm-observation { padding: 10px 11px; border: 1px solid var(--line); border-radius: 9px; background: #fbfcfe; }
.gtm-observation > div { display: flex; justify-content: space-between; gap: 8px; }
.gtm-observation p { margin: 5px 0; font-size: 12px; line-height: 1.42; }
.gtm-observation small { color: var(--faint); font-size: 10.5px; }
.gtm-observation a { color: var(--blue); text-decoration: none; }
.experiment-arms { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.experiment-arms > div { padding: 9px 10px; border: 1px solid var(--line); border-radius: 9px; background: #fbfcfe; }
.experiment-arms p { margin-bottom: 0; }
.gtm-create { margin: 0 0 14px; padding: 12px 14px; border: 1px solid #cfdcf4; border-radius: 10px; background: var(--blue-wash); }
.gtm-create summary { cursor: pointer; color: var(--blue); font-size: 12.5px; font-weight: 700; }
.gtm-form { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin-top: 12px; }
.gtm-form label { display: grid; gap: 4px; color: var(--muted); font-size: 11px; font-weight: 600; }
.gtm-form label.wide { grid-column: 1 / -1; }
.gtm-form input, .gtm-form select, .gtm-form textarea { width: 100%; box-sizing: border-box; padding: 7px 8px; border: 1px solid var(--line); border-radius: 7px; background: #fff; color: var(--ink); font: inherit; font-size: 12px; }
.gtm-form textarea { min-height: 58px; resize: vertical; }
.gtm-form button { justify-self: start; }
.gtm-results { margin-top: 10px; padding-top: 9px; border-top: 1px solid var(--line); }
.gtm-results summary { cursor: pointer; color: var(--blue); font-size: 11.5px; font-weight: 650; }
.gtm-results form { display: grid; grid-template-columns: repeat(2, 1fr); gap: 7px; margin-top: 8px; }
.gtm-results label { display: grid; gap: 3px; color: var(--faint); font-size: 10.5px; }
.gtm-results input { width: 100%; box-sizing: border-box; padding: 5px 6px; border: 1px solid var(--line); border-radius: 6px; }
.gtm-results button { grid-column: 1 / -1; justify-self: start; border: 0; border-radius: 7px; padding: 6px 9px; background: var(--blue); color: #fff; font-size: 11px; font-weight: 650; cursor: pointer; }
.customer-dev { min-width: 1050px; padding: 18px 20px 20px; border-bottom: 1px solid var(--line); background: #f7f9f4; }
.customer-dev-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 20px; }
.customer-dev-head h2 { margin: 0; font-size: 17px; }
.customer-dev-head p { margin: 4px 0 0; max-width: 820px; color: var(--muted); font-size: 12.5px; }
.customer-dev-head a { color: var(--blue); font-size: 12px; font-weight: 650; text-decoration: none; white-space: nowrap; }
.customer-dev-ladder { display: grid; grid-template-columns: repeat(11, minmax(88px, 1fr)); gap: 5px; margin: 14px 0 12px; }
.customer-dev-rung { position: relative; min-height: 48px; padding: 7px 8px; border: 1px solid #dfe6d8; border-radius: 8px; background: #fff; }
.customer-dev-rung:not(:last-child)::after { content: '›'; position: absolute; right: -6px; top: 13px; z-index: 2; color: #9aa690; font-weight: 800; }
.customer-dev-rung strong { display: block; color: #4f762b; font-size: 14px; }
.customer-dev-rung span { display: block; color: var(--faint); font-size: 9.5px; line-height: 1.25; }
.customer-dev-accounts { display: grid; gap: 8px; }
.customer-dev-account { border: 1px solid #dfe6d8; border-radius: 10px; background: #fff; }
.customer-dev-account > summary { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 11px 13px; cursor: pointer; list-style: none; }
.customer-dev-account > summary::-webkit-details-marker, .customer-dev-commercial > summary::-webkit-details-marker { display: none; }
.customer-dev-account > summary > div { display: flex; align-items: center; gap: 9px; min-width: 0; }
.customer-dev-account > summary strong { font-size: 13px; }
.customer-dev-account > summary small { color: var(--faint); font-size: 10.5px; }
.customer-dev-stage { padding: 2px 7px; border-radius: 999px; background: #edf5e5; color: #4f762b; font-size: 10px; font-weight: 750; white-space: nowrap; }
.customer-dev-stage.conditional_loi, .customer-dev-stage.paid_pilot, .customer-dev-stage.deployment { background: var(--green-tint); color: var(--green); }
.customer-dev-next { max-width: 480px; color: var(--blue-strong); font-size: 11px; text-align: right; }
.customer-dev-body { padding: 0 13px 14px; border-top: 1px solid #edf0e9; }
.customer-dev-gate { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin: 12px 0; }
.customer-dev-gate > div { padding: 9px 10px; border-radius: 8px; background: #f7f9f4; }
.customer-dev-gate b { color: #4f762b; font-size: 10px; text-transform: uppercase; letter-spacing: .035em; }
.customer-dev-gate p { margin: 3px 0 0; color: var(--muted); font-size: 11.5px; line-height: 1.4; }
.customer-dev-form { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px; }
.customer-dev-form label, .customer-dev-commercial label { display: grid; gap: 4px; color: var(--muted); font-size: 10.5px; font-weight: 650; }
.customer-dev-form label.wide, .customer-dev-commercial.wide { grid-column: 1 / -1; }
.customer-dev-form textarea, .customer-dev-form input, .customer-dev-form select { width: 100%; padding: 7px 8px; border: 1px solid var(--line); border-radius: 7px; background: #fff; color: var(--ink); font: inherit; font-size: 11.5px; }
.customer-dev-form textarea { min-height: 54px; resize: vertical; }
.customer-dev-form .customer-dev-check { display: flex; grid-column: 1 / -1; align-items: center; gap: 7px; padding: 7px 9px; border-radius: 7px; background: var(--blue-wash); color: var(--ink); }
.customer-dev-form .customer-dev-check input { width: auto; }
.customer-dev-commercial { padding: 9px 10px; border: 1px solid var(--line); border-radius: 8px; background: #fbfcfe; }
.customer-dev-commercial > summary { cursor: pointer; color: var(--blue); font-size: 11.5px; font-weight: 700; }
.customer-dev-commercial > div { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px; margin-top: 9px; }
.customer-dev-commercial label.wide { grid-column: 1 / -1; }
.customer-dev-form > button { grid-column: 1 / -1; justify-self: start; border: 0; border-radius: 7px; padding: 7px 11px; background: #4f762b; color: #fff; font: inherit; font-size: 11.5px; font-weight: 700; cursor: pointer; }
.customer-dev-empty { display: grid; justify-items: center; padding: 22px; border: 1px dashed #ccd7c2; border-radius: 10px; color: var(--faint); }
.customer-dev-empty strong { color: var(--ink); }
.customer-dev-stage-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(185px, 1fr)); gap: 8px; }
.customer-dev-stage-grid article { padding: 11px 12px; border: 1px solid #dfe6d8; border-radius: 9px; background: #fbfdf9; }
.customer-dev-stage-grid article > span { color: #7d9270; font-size: 10px; font-weight: 750; }
.customer-dev-stage-grid article > strong { display: block; margin-top: 2px; font-size: 12.5px; }
.customer-dev-stage-grid article > em { display: block; color: #4f762b; font-size: 10.5px; font-style: normal; font-weight: 650; }
.customer-dev-stage-grid article p { margin: 6px 0; color: var(--muted); font-size: 11.5px; line-height: 1.4; }
.customer-dev-stage-grid article small { color: var(--faint); font-size: 10.5px; line-height: 1.35; }
.outcome-strip {
  display: grid; grid-template-columns: minmax(235px, 1.6fr) repeat(5, minmax(118px, 1fr));
  gap: 8px; min-width: 920px; padding: 12px 14px; border-bottom: 1px solid var(--line);
  background: #f7f9fc;
}
.outcome-intro, .outcome-card {
  min-width: 0; padding: 10px 11px; border: 1px solid var(--line); border-radius: 10px; background: #fff;
}
.outcome-intro strong { display: block; font-size: 13px; line-height: 1.35; }
.outcome-intro small, .outcome-card small {
  display: block; margin-top: 3px; color: var(--faint); font-size: 9.5px; line-height: 1.3;
}
.outcome-card { color: var(--ink); text-decoration: none; }
.outcome-card:hover { border-color: #c7d8f2; background: var(--blue-wash); }
.outcome-card b { display: block; color: var(--blue); font-size: 19px; line-height: 1.1; }
.outcome-card span { display: block; margin-top: 3px; color: var(--ink); font-size: 10.5px; font-weight: 650; }
.outcome-card.meeting b { color: var(--green); }
.mobile-pipeline { display: none; }
.mobile-account, .mobile-contact, .mobile-touch {
  overflow: hidden; border: 1px solid var(--line); border-radius: 12px; background: #fff;
}
.mobile-account > summary, .mobile-contact > summary {
  min-height: 58px; display: flex; align-items: center; justify-content: space-between; gap: 12px;
  padding: 12px 14px; cursor: pointer; list-style: none;
}
.mobile-account > summary::-webkit-details-marker,
.mobile-contact > summary::-webkit-details-marker,
.mobile-touch > summary::-webkit-details-marker { display: none; }
.mobile-account > summary > span:first-child, .mobile-contact > summary > span:first-child { min-width: 0; }
.mobile-account > summary strong, .mobile-contact > summary strong {
  display: block; font-size: 13.5px; line-height: 1.35;
}
.mobile-account > summary small, .mobile-contact > summary small {
  display: block; margin-top: 2px; color: var(--faint); font-size: 11px;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.mobile-account > summary::after, .mobile-contact > summary::after {
  content: '›'; flex: 0 0 auto; color: var(--faint); font-size: 20px; line-height: 1;
  transform: rotate(90deg); transition: transform .12s ease;
}
.mobile-account[open] > summary::after, .mobile-contact[open] > summary::after { transform: rotate(-90deg); }
.mobile-count {
  margin-left: auto; color: var(--faint); font-size: 10.5px; white-space: nowrap;
}
.mobile-account-body { padding: 0 10px 10px; border-top: 1px solid var(--line); background: #f8fafc; }
.mobile-context {
  margin: 10px 0; padding: 12px; border: 1px solid #d8e4f5; border-radius: 10px;
  background: var(--blue-wash);
}
.mobile-context .context-line + .context-line { margin-top: 8px; }
.mobile-contact { margin-top: 8px; }
.mobile-contact-body { padding: 0 12px 12px; border-top: 1px solid var(--line); }
.mobile-why { margin: 10px 0 0; color: var(--muted); font-size: 11.5px; line-height: 1.45; }
.mobile-why b {
  display: block; margin-bottom: 2px; color: var(--faint); font-size: 9.5px;
  letter-spacing: .04em; text-transform: uppercase;
}
.mobile-contact-body > .email { margin-top: 10px; }
.mobile-touches { display: grid; gap: 8px; margin-top: 12px; }
.mobile-touch { border-radius: 9px; }
.mobile-touch.touch-inline { padding: 11px; }
.mobile-touch.touch-inline .touch-head { min-height: 22px; }
.mobile-touch.touch-inline .touch-head time { float: right; max-width: 58%; margin: 1px 0 0; }
.mobile-touch.touch-inline > .subject { margin-top: 9px; font-size: 12px; }
.mobile-touch.touch-inline > .message { font-size: 12px; line-height: 1.55; }
.mobile-touch > summary { min-height: 56px; padding: 10px 11px; cursor: pointer; list-style: none; }
.mobile-touch > summary time {
  float: right; max-width: 58%; margin: 1px 0 0; color: var(--blue-strong);
  font-size: 10px; font-weight: 650; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.mobile-touch > summary p { clear: both; margin: 7px 0 0; color: var(--muted); font-size: 11px; line-height: 1.4; }
.mobile-touch .touch-full { margin: 0 11px 11px; }
.mobile-touch.sent { border-color: #c6e4cf; }
.mobile-touch.blocked, .mobile-touch.failed { border-color: #f3c7c3; }
.mobile-touch.writing {
  background: linear-gradient(110deg, #f7faff 30%, #eaf2ff 50%, #f7faff 70%);
  background-size: 220% 100%; animation: draft-shimmer 1.8s linear infinite;
}
.mobile-touch.reviewing { background: #fbf8ff; }
.mobile-touch.missing {
  min-height: 44px; display: flex; align-items: center; justify-content: space-between;
  padding: 9px 11px; color: var(--faint); background: #f8f9fa; border-style: dashed;
}
.mobile-empty {
  min-height: 180px; place-content: center; justify-items: center; gap: 4px;
  color: var(--faint); text-align: center;
}
.mobile-empty strong { color: var(--ink); font-size: 15px; }
@media (max-width: 760px) {
  .topbar {
    min-height: 0; padding: max(8px, env(safe-area-inset-top)) 10px 8px;
    flex-wrap: wrap; align-content: center; gap: 4px 8px;
  }
  .brand-lockup { height: 38px; }
  .mark { width: 28px; height: 28px; }
  .wordmark { font-size: 14px; }
  .biz-tabs { order: 2; flex: 1 1 180px; height: 38px; }
  .biz-tab { min-height: 38px; padding: 0 9px; font-size: 12px; }
  .biz-tab .count { display: none; }
  .surface-tabs {
    order: 3; display: flex; width: 100%; height: 36px; margin: 3px 0 0;
    border-radius: 10px; overflow-x: auto;
  }
  .surface-tab { flex: 1 0 auto; justify-content: center; min-width: 86px; padding: 0 10px; border-radius: 8px; }
  .subbar { align-items: stretch; padding: 12px; gap: 10px; }
  .subbar h1 { font-size: 17px; }
  .subbar .tagline { font-size: 11.5px; line-height: 1.4; }
  .subbar-stats { display: grid; grid-template-columns: repeat(3, minmax(45px, 1fr)); gap: 6px; }
  .subbar-stats .stat { min-width: 48px; text-align: center; }
  .subbar-stats .n { font-size: 15px; }
  .subbar-stats .l { font-size: 8.5px; }
  .sheet-scroll {
    overflow-x: hidden; padding: 10px 10px max(22px, env(safe-area-inset-bottom)); background: #f3f6fa;
  }
  .outcome-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr)); min-width: 0;
    margin: -10px -10px 10px; padding: 10px; border-bottom: 1px solid var(--line);
  }
  .outcome-intro { grid-column: 1 / -1; }
  .outcome-card:last-child { grid-column: 1 / -1; }
  .desktop-pipeline { display: none; }
  .mobile-pipeline { display: grid; gap: 10px; }
  .strategy-strip { flex-direction: column; gap: 8px; }
  .strategy-scroll, .gtm-scroll {
    padding: 12px 10px max(22px, env(safe-area-inset-bottom));
  }
  .strategy-panel, .gtm-panel { padding: 15px 13px; border-radius: 11px; }
  .portfolio { grid-template-columns: 1fr; gap: 10px; padding: 12px 10px; }
  .portfolio-lead { padding: 12px 12px 0; }
  .calendar-scroll { padding: 10px; }
  .calendar-policy { align-items: flex-start; }
  .calendar-policy > div { display: block; }
  .calendar-policy b { display: block; margin-bottom: 3px; }
  .calendar-brand-strip { grid-template-columns: 1fr; }
  .calendar-grid { display: flex; overflow-x: auto; scroll-snap-type: x mandatory; padding-bottom: 8px; }
  .calendar-day { flex: 0 0 82vw; scroll-snap-align: start; }
  .strategy-brand-grid, .gtm-card-grid { grid-template-columns: minmax(0, 1fr); }
  .gtm-doctrine, .gtm-split { grid-template-columns: 1fr; }
  .gtm-form { grid-template-columns: 1fr; }
  .experiment-arms { grid-template-columns: 1fr; }
  .customer-dev {
    min-width: 0; margin: -10px -10px 10px; padding: 14px 10px; overflow: hidden;
  }
  .customer-dev-head { display: grid; gap: 7px; }
  .customer-dev-head a { white-space: normal; }
  .customer-dev-ladder {
    display: flex; overflow-x: auto; gap: 6px; padding-bottom: 4px; scroll-snap-type: x proximity;
  }
  .customer-dev-rung { min-width: 94px; scroll-snap-align: start; }
  .customer-dev-account > summary { align-items: flex-start; gap: 8px; }
  .customer-dev-account > summary > div { display: grid; gap: 3px; }
  .customer-dev-next { max-width: 42%; font-size: 10px; }
  .customer-dev-form, .customer-dev-gate, .customer-dev-commercial > div { grid-template-columns: 1fr; }
  button, select, .btn, .gtm-actions button, .gtm-form button { min-height: 44px; }
  input, select, textarea, .gtm-form input, .gtm-form select, .gtm-form textarea,
  .customer-dev-form textarea, .customer-dev-form input, .customer-dev-form select { font-size: 16px; }
}
</style>"#;

#[cfg(test)]
mod tests {
    use super::{
        brand_meta, brand_tab_counts, display_written_at, execution_dashboard, favicon,
        gtm_snapshot, local_write_headers, page_head, render_gtm_lab, render_html, render_hub,
        render_sponsorship_table, render_strategy_brand, render_strategy_hub, Crm,
        ExecutionOpportunity, ExecutionOpportunityContact, BRANDS, FAVICON_SVG,
    };
    use crate::business::Businesses;
    use crate::db::{
        AccountPlayAssessment, CustomerDevelopmentRecord, Db, Lead, Mailbox, Opportunity,
        OpportunityContact, OpportunityStakeholder, OpportunityTouch, Person, Reply,
        SalesOpportunity, Sequence, SignalObservation, Touch,
    };
    use crate::playbook::Playbooks;
    use axum::http::{HeaderMap, HeaderValue};
    use axum::response::IntoResponse;
    use uuid::Uuid;

    #[test]
    fn every_crm_page_links_the_sales_os_favicon() {
        let head = page_head("Sales CRM");
        assert!(head.contains("<link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">"));
        assert!(FAVICON_SVG.contains("viewBox=\"0 0 64 64\""));
        assert!(FAVICON_SVG.contains("#0c1733"));
        assert!(FAVICON_SVG.contains("linearGradient"));
    }

    #[tokio::test]
    async fn favicon_is_served_as_cacheable_svg() {
        let response = favicon().await.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers().get(axum::http::header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("image/svg+xml; charset=utf-8"))
        );
        assert_eq!(
            response.headers().get(axum::http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("public, max-age=86400"))
        );
    }

    #[test]
    fn sponsorship_table_surfaces_full_manual_drafts_without_clicks_or_approval() {
        let entry = ExecutionOpportunity {
            opportunity: Opportunity {
                id: "sponsor-opportunity".into(),
                brand: "outagehub".into(),
                kind: "sponsorship".into(),
                funder: "Canadian Resilience Co.".into(),
                canonical_url: "https://example.org/sponsor-evidence".into(),
                evidence: vec!["organization_relevance: outage resilience".into()],
                ..Default::default()
            },
            contacts: vec![ExecutionOpportunityContact {
                contact: OpportunityContact {
                    name: "Maya Chen".into(),
                    title: "CEO".into(),
                    email: "maya@example.org".into(),
                    ..Default::default()
                },
                touches: vec![OpportunityTouch {
                    id: "sponsor-touch".into(),
                    status: "draft".into(),
                    review_passes: Some(true),
                    subject: "OutageHub founding sponsorship".into(),
                    body: "A complete manual-review sponsorship email.".into(),
                    ..Default::default()
                }],
            }],
            application: None,
        };
        let mut html = String::new();
        render_sponsorship_table(&mut html, &[entry]);

        assert!(html.contains("<table class=\"crm-sheet sponsorship-sheet\""));
        assert!(html.contains("Canadian Resilience Co."));
        assert!(html.contains("maya@example.org"));
        assert!(html.contains("A complete manual-review sponsorship email."));
        assert!(html.contains("technically blocked from scheduling and sending"));
        assert!(!html.contains("<details"));
        assert!(!html.contains("/opportunities/approve/"));
    }

    #[test]
    fn local_dashboard_rejects_cross_site_browser_writes() {
        let mut local = HeaderMap::new();
        local.insert("origin", HeaderValue::from_static("http://127.0.0.1:8788"));
        assert!(local_write_headers(&local));

        let mut remote = HeaderMap::new();
        remote.insert(
            "origin",
            HeaderValue::from_static("https://attacker.example"),
        );
        assert!(!local_write_headers(&remote));

        let mut cross_site = HeaderMap::new();
        cross_site.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
        assert!(!local_write_headers(&cross_site));

        assert!(local_write_headers(&HeaderMap::new()));
    }

    #[test]
    fn written_at_is_distinct_from_the_delivery_schedule() {
        let label = display_written_at("2026-08-08T14:37:49+00:00");
        assert!(label.starts_with("Drafted Sat 8 Aug 2026 · "));
        assert!(!label.contains("Due"));
    }

    #[test]
    fn execution_dashboard_keeps_replies_in_the_selected_brand() {
        let db = Db::open(":memory:").expect("open memory db");
        for brand in ["gnk", "outagehub"] {
            let lead_id = db
                .upsert_lead(&Lead {
                    brand: brand.into(),
                    apollo_org_id: format!("{brand}-reply-org"),
                    name: format!("{brand} account"),
                    ..Default::default()
                })
                .expect("insert lead");
            let person_id = db
                .upsert_person(&Person {
                    lead_id,
                    brand: brand.into(),
                    apollo_person_id: format!("{brand}-reply-person"),
                    name: format!("{brand} person"),
                    email: format!("{brand}@example.com"),
                    ..Default::default()
                })
                .expect("insert person");
            db.record_reply(&Reply {
                person_id,
                from_email: format!("{brand}@example.com"),
                classification: "positive".into(),
                ..Default::default()
            })
            .expect("record reply");
        }

        let gnk = execution_dashboard(&db, Some("gnk")).expect("gnk dashboard");
        assert_eq!(gnk.replies.len(), 1);
        assert_eq!(gnk.replies[0].from_email, "gnk@example.com");
        let all = execution_dashboard(&db, None).expect("all dashboard");
        assert_eq!(all.replies.len(), 2);
    }

    #[test]
    fn strategy_board_surfaces_business_goals_and_playbook_doctrine() {
        let businesses = Businesses::load("businesses").expect("load businesses");
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let counts: Vec<_> = BRANDS.iter().map(|meta| (meta, 0usize)).collect();

        let hub = render_strategy_hub(&counts, &businesses, &playbooks, None);
        assert!(hub.contains("SDR strategy"));
        assert!(hub.contains("What guides every email"));
        assert!(hub.contains("Hypothesis-led discovery"));
        assert!(hub.contains("Agent personas &amp; retrieved knowledge"));
        assert!(hub.contains("playbooks/personas/writer.md"));
        assert!(hub.contains("Optional ten-lens audit"));
        assert!(hub.contains("Alex Hormozi value-and-offer lens"));
        assert!(hub.contains("Wapahki"));
        assert!(hub.contains("GnK"));
        assert!(hub.contains("OutageHub"));
        assert!(hub.contains("href=\"/strategy/wapahki\""));
        assert!(hub.contains("class=\"surface-tab active\""));

        let gnk = businesses.get("gnk").expect("gnk profile");
        let pb = playbooks.get("gnk").expect("gnk playbook");
        let page = render_strategy_brand(
            brand_meta("gnk").expect("meta"),
            gnk,
            pb,
            &playbooks.shared,
            &counts,
        );
        assert!(page.contains("GnK strategy"));
        assert!(page.contains("What we are trying to do"));
        assert!(page.contains("Outreach playbook"));
        assert!(page.contains(&pb.one_liner));
        assert!(page.contains("Operating motions"));
        assert!(page.contains("Capacity &amp; timing"));
        assert!(page.contains("href=\"/b/gnk\""));

        let wapahki = businesses.get("wapahki").expect("wapahki profile");
        let wapahki_pb = playbooks.get("wapahki").expect("wapahki playbook");
        let wapahki_page = render_strategy_brand(
            brand_meta("wapahki").expect("meta"),
            wapahki,
            wapahki_pb,
            &playbooks.shared,
            &counts,
        );
        assert!(wapahki_page.contains("Founder discovery evidence"));
        assert!(wapahki_page.contains("high-mix manufacturing"));
        assert!(wapahki_page.contains("Open source notes"));
        assert!(wapahki_page.contains("not proof about a prospect"));

        let ohub = businesses.get("outagehub").expect("outagehub profile");
        let ohub_pb = playbooks.get("outagehub").expect("outagehub playbook");
        let sponsorship_page = render_strategy_brand(
            brand_meta("outagehub").expect("meta"),
            ohub,
            ohub_pb,
            &playbooks.shared,
            &counts,
        );
        assert!(sponsorship_page.contains("Infrastructure sponsorship"));
        assert!(sponsorship_page.contains("Recipient routes"));
        assert!(sponsorship_page.contains("commercial sponsor"));
        assert!(!sponsorship_page.contains("Funding motion"));
    }

    #[test]
    fn pipeline_all_is_the_cross_brand_capacity_calendar() {
        let db = Db::open(":memory:").expect("open memory db");
        let businesses = Businesses::load("businesses").expect("load businesses");
        let counts = brand_tab_counts(&db);
        let html = render_hub(&counts, &businesses, &db);

        assert!(html.contains("Outreach calendar"));
        assert!(html.contains("90 max/day"));
        assert!(html.contains("Replies first, then due follow-ups"));
        assert!(html.contains("class=\"calendar-grid\""));
        assert!(html.contains("Wapahki"));
        assert!(html.contains("GnK"));
        assert!(html.contains("OutageHub"));
        assert!(html.contains("href=\"/b/outagehub\""));
    }

    #[test]
    fn gtm_lab_surfaces_sourcing_policy_and_root_cause_rankings() {
        let db = Db::open(":memory:").expect("open memory db");
        let play = db
            .current_gtm_play("outagehub")
            .expect("load play")
            .expect("seeded play");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "outagehub".into(),
                apollo_org_id: "org-gtm-ui".into(),
                name: "Northern Cold Chain".into(),
                ..Default::default()
            })
            .expect("insert lead");
        db.upsert_account_play_assessment(&AccountPlayAssessment {
            lead_id,
            brand: "outagehub".into(),
            play_id: play.id,
            play_version: play.version,
            status: "qualified".into(),
            fit_score: 88,
            symptom: "An alarm does not identify whether the grid is involved.".into(),
            root_cause: "Internal alarms cannot supply location-matched utility context.".into(),
            current_workaround: "The shift calls or checks the local utility.".into(),
            proof_fit: "Replay a few historical outage alarms.".into(),
            ..Default::default()
        })
        .expect("insert assessment");

        let snapshot = gtm_snapshot(&db, Some("outagehub")).expect("snapshot");
        let counts = brand_tab_counts(&db);
        let html = render_gtm_lab(brand_meta("outagehub"), &counts, &snapshot, None);

        assert!(html.contains("Account root-cause ranking"));
        assert!(html.contains("Market coverage ledger"));
        assert!(html.contains("Facility and workflow opportunities"));
        assert!(html.contains("Buying committees"));
        assert!(html.contains("Canadian EV charging operations"));
        assert!(html.contains("Northern Cold Chain"));
        assert!(html.contains("Internal alarms cannot supply"));
        assert!(html.contains("88/100"));
        assert!(html.contains("Match supplied or public operating locations"));
        assert!(html.contains("Forward-deployed proof briefs"));
    }

    #[test]
    fn wapahki_pipeline_surfaces_stage_gates_and_editable_evidence() {
        let db = Db::open(":memory:").expect("open memory db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "wapahki".into(),
                apollo_org_id: "wapahki-ui-account".into(),
                name: "Ontario Prepared Foods".into(),
                ..Default::default()
            })
            .expect("insert lead");
        let market_account_id = db
            .market_account_for_lead(&lead_id)
            .expect("account query")
            .expect("market account")
            .id;
        let play_id = db
            .current_gtm_play("wapahki")
            .expect("play query")
            .expect("current play")
            .id;
        let sales_opportunity_id = db
            .upsert_sales_opportunity(&SalesOpportunity {
                brand: "wapahki".into(),
                market_account_id,
                lead_id: lead_id.clone(),
                play_id,
                kind: "physical_task".into(),
                title: "Prepared-food repacking".into(),
                task_or_decision: "Repack changing prepared-food formats".into(),
                ..Default::default()
            })
            .expect("insert sales opportunity");
        db.upsert_customer_development(&CustomerDevelopmentRecord {
            brand: "wapahki".into(),
            lead_id,
            sales_opportunity_id,
            problem: "Operators manually repack changing formats.".into(),
            stage: "problem_confirmed".into(),
            engaged_at: "2026-08-08T10:00:00Z".into(),
            next_action: "Ask for a video of the last changeover.".into(),
            ..Default::default()
        })
        .expect("insert discovery");
        let dashboard = execution_dashboard(&db, Some("wapahki")).expect("dashboard");
        let counts = brand_tab_counts(&db);
        let html = render_html(
            &Crm::default(),
            Some(&dashboard),
            brand_meta("wapahki"),
            &counts,
            None,
        );
        assert!(html.contains("From task hypothesis to conditional LOI"));
        assert!(html.contains("Problem confirmed"));
        assert!(html.contains("Ontario Prepared Foods"));
        assert!(html.contains("Ask for a video of the last changeover."));
        assert!(html.contains("action=\"/customer-development\""));
        assert!(html.contains("Highest explicit commitment"));

        let snapshot = gtm_snapshot(&db, Some("wapahki")).expect("snapshot");
        let gtm = render_gtm_lab(brand_meta("wapahki"), &counts, &snapshot, None);
        assert!(gtm.contains("Customer-development stage gates"));
        assert!(gtm.contains("conditional intent"));
    }

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
                vantage: "process_owner".into(),
                linkedin_url: "https://www.linkedin.com/in/alex-rivera".into(),
                linkedin_status: "requested".into(),
                email: "alex@example.com".into(),
                email_status: "verified".into(),
                status: "verified".into(),
                ..Default::default()
            })
            .expect("insert person");
        let play = db
            .current_gtm_play("gnk")
            .expect("current play query")
            .expect("current GnK play");
        let signal_keys = [
            "account.fit_evidence",
            "account.specific_recurring_decision",
            "account.believable_operating_consequence",
            "account.external_trigger_or_mechanism_evidence",
        ];
        db.record_signal_candidates(
            "gnk",
            &lead_id,
            &signal_keys
                .iter()
                .enumerate()
                .map(|(index, key)| crate::gtm::SignalCandidate {
                    definition_key: (*key).into(),
                    evidence: format!("Source-backed workflow evidence {index}"),
                    source_url: format!("https://evidence-{index}.example/evidence/{index}"),
                    confidence: 0.9,
                })
                .collect::<Vec<_>>(),
            "test",
        )
        .expect("record account signals");
        db.record_signal_observation(&SignalObservation {
            brand: "gnk".into(),
            definition_key: "contact.workflow_vantage".into(),
            lead_id: lead_id.clone(),
            person_id: person_id.clone(),
            evidence: "Operations Director is close to the recurring decision.".into(),
            confidence: 0.9,
            status: "observed".into(),
            ..Default::default()
        })
        .expect("record contact vantage");
        db.upsert_account_play_assessment(&AccountPlayAssessment {
            lead_id: lead_id.clone(),
            brand: "gnk".into(),
            play_id: play.id.clone(),
            play_version: play.version,
            status: "qualified".into(),
            fit_score: 85,
            matched_signal_keys: signal_keys.iter().map(|key| (*key).into()).collect(),
            root_cause: "A current workflow boundary creates a recurring decision burden.".into(),
            proof_fit: "A bounded historical workflow replay can test it.".into(),
            source: "test".into(),
            ..Default::default()
        })
        .expect("record current assessment");
        let sales_opportunity_id = db
            .best_sales_opportunity("gnk", &lead_id, &play.id)
            .expect("opportunity query")
            .expect("materialized sales opportunity")
            .id;
        let sequence_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                play_id: play.id,
                play_version: play.version,
                sales_opportunity_id: sales_opportunity_id.clone(),
                gtm_state: "action_ready".into(),
                copy_policy_version: crate::db::CURRENT_COPY_POLICY_VERSION,
                status: "active".into(),
                ..Default::default()
            })
            .expect("insert sequence");
        db.upsert_opportunity_stakeholder(&OpportunityStakeholder {
            sales_opportunity_id: sales_opportunity_id.clone(),
            person_id: person_id.clone(),
            role: "process_owner".into(),
            status: "mapped".into(),
            ..Default::default()
        })
        .expect("map stakeholder");
        db.activate_opportunity_stakeholder(&sales_opportunity_id, &person_id)
            .expect("activate stakeholder thread");
        for (index, channel) in ["email", "email", "linkedin_request", "email"]
            .iter()
            .enumerate()
        {
            let stage = index as i64 + 1;
            db.insert_touch(&Touch {
                sequence_id: sequence_id.clone(),
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                stage,
                channel: (*channel).into(),
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
            db.schedule_reviewed_touches(Some("gnk"), Some(&person_id))
                .expect("approve"),
            3
        );
        let touches = db
            .list_touches_for_person(&person_id)
            .expect("list touches");
        assert_eq!(touches[0].status, "scheduled");
        assert_eq!(touches[2].status, "draft");

        let dashboard = execution_dashboard(&db, Some("gnk")).expect("build dashboard");
        let json = serde_json::to_string(&dashboard).expect("serialize dashboard");
        assert!(!json.contains("TOP_SECRET_PASSWORD"));
        let counts = brand_tab_counts(&db);
        let html = render_html(
            &Crm::default(),
            Some(&dashboard),
            brand_meta("gnk"),
            &counts,
            None,
        );
        // Persistent brand tabs let you jump between the three CRMs from any page.
        assert!(html.contains("class=\"biz-tabs\""));
        assert!(html.contains("LinkedIn request"));
        assert!(html.contains("LinkedIn connection status"));
        assert!(html.contains("value=\"requested\" selected"));
        assert!(html.contains(&format!(
            "action=\"/execution/person/{person_id}/linkedin\""
        )));
        assert!(html.contains("href=\"/b/wapahki\""));
        assert!(html.contains("href=\"/b/gnk\""));
        assert!(html.contains("href=\"/b/outagehub\""));
        assert!(html.contains("class=\"biz-tab gnk active\""));
        assert!(html.contains("surface-tab"));
        assert!(html.contains("href=\"/strategy/gnk\""));
        assert!(html.contains("Company context"));
        assert!(html.contains("Internal role fit (not sent)"));
        assert!(html.contains("<th>T4</th>"));
        assert!(!html.contains("<th>T5</th>"));
        assert!(html.contains("Real Logistics"));
        assert!(html.contains("Alex Rivera"));
        assert!(html.contains("alex@example.com"));
        assert!(!html.contains("Contact 5"));
        assert!(html.contains("Fri 7 Aug · 5:00 AM EDT"));
        assert!(html.contains("class=\"written-at\""));
        assert!(html.contains("Drafted "));
        assert!(html.contains("A email touch"));
        assert!(html.contains("setInterval(refreshPipeline, 3000)"));
        assert!(html.contains("data-touch-id="));
        assert!(html.contains("class=\"touch-inline\""));
        assert!(html.contains("<div class=\"message\">A email touch"));
        assert!(!html.contains("data-open-id=\"touch-"));
        // Desktop keeps the dense sheet; phone widths receive an outcome-oriented
        // account/contact card view with the same controls and copy.
        assert!(html.contains("class=\"desktop-pipeline\""));
        assert!(html.contains("class=\"mobile-pipeline\""));
        assert!(html.contains(&format!("data-open-id=\"person-{person_id}\"")));
        assert!(html.contains("0 sent · 1 ready"));
        assert!(html.contains("Next best work"));
        assert!(html.contains("ready companies"));
        assert!(html.contains("reviewed sequences"));
        assert!(html.contains("email drafts"));
        assert!(!html.contains("reviewed drafts"));
        assert!(html.contains("<b>1</b><span>LinkedIn actions</span>"));
        assert!(html.contains("viewport-fit=cover"));
        assert!(html.contains(".surface-tabs {\n    order: 3; display: flex"));

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

    #[test]
    fn pipeline_hides_people_until_the_current_sequence_is_complete() {
        let db = Db::open(":memory:").expect("open memory db");
        let lead_id = db
            .upsert_lead(&Lead {
                brand: "gnk".into(),
                apollo_org_id: "hidden-org".into(),
                name: "Hidden Until Ready Ltd".into(),
                ..Default::default()
            })
            .expect("insert lead");
        let person_id = db
            .upsert_person(&Person {
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                apollo_person_id: "hidden-person".into(),
                name: "Partial Draft Person".into(),
                ..Default::default()
            })
            .expect("insert person");
        let sequence_id = db
            .create_sequence(&Sequence {
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                copy_policy_version: crate::db::CURRENT_COPY_POLICY_VERSION,
                status: "active".into(),
                ..Default::default()
            })
            .expect("insert sequence");
        for stage in 1..=6 {
            db.insert_touch(&Touch {
                sequence_id: sequence_id.clone(),
                person_id: person_id.clone(),
                lead_id: lead_id.clone(),
                brand: "gnk".into(),
                stage,
                channel: "email".into(),
                body: format!("Reviewed stage {stage}"),
                status: "draft".into(),
                review_passes: Some(true),
                ..Default::default()
            })
            .expect("insert partial touch");
        }

        let dashboard = execution_dashboard(&db, Some("gnk")).expect("dashboard");
        assert!(dashboard.accounts.is_empty());
        assert_eq!(brand_tab_counts(&db)[1].1, 0);
        let html = render_html(
            &Crm::default(),
            Some(&dashboard),
            brand_meta("gnk"),
            &brand_tab_counts(&db),
            None,
        );
        assert!(!html.contains("Partial Draft Person"));
        assert!(!html.contains("Not written"));
        assert!(html.contains("No reviewed sequences ready yet"));
    }
}
