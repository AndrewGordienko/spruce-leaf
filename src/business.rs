//! Business profiles: what each company is, what it is trying to accomplish,
//! and which operating motions are enabled.
//!
//! Outreach playbooks deliberately remain about buyer-facing copy. These
//! profiles are the durable operating context used by opportunity discovery,
//! eligibility assessment, and the agent router. Keeping this in TOML means a
//! new grant, tender, pilot, or partnership motion does not require Rust code.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BusinessProfile {
    pub key: String,
    pub name: String,
    pub summary: String,
    #[serde(default)]
    pub known_facts: Vec<String>,
    #[serde(default)]
    pub unknowns: Vec<String>,
    #[serde(default)]
    pub goals: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    /// Source-linked founder discovery. Unlike `known_facts`, these are
    /// participant reports about their own work or market; they guide ICP and
    /// questions but never become facts about a target account.
    #[serde(default)]
    pub discovery_evidence: Vec<DiscoveryEvidence>,
    #[serde(default)]
    pub motions: Vec<Motion>,
    #[serde(default)]
    pub funding: Option<FundingProfile>,
    #[serde(default)]
    pub sponsorship: Option<SponsorshipProfile>,
    #[serde(default)]
    pub calendar: OutreachCalendar,
    #[serde(default)]
    pub account_limits: AccountLimits,
    /// Founder-capacity and cash-allocation policy. Evidence readiness remains
    /// an independent GTM concern; this section says which paid offer and
    /// commercial lane deserves attention once an opportunity is understood.
    #[serde(default)]
    pub commercial: CommercialPolicy,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CommercialPolicy {
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default = "default_cash_constrained_runway_months")]
    pub cash_constrained_below_runway_months: u32,
    #[serde(default)]
    pub cash_constrained_allocation: CommercialAllocation,
    #[serde(default = "default_stable_allocation")]
    pub stable_allocation: CommercialAllocation,
    #[serde(default = "default_max_active_strategic")]
    pub max_active_strategic: usize,
    #[serde(default = "default_pipeline_coverage")]
    pub minimum_cash_now_pipeline_coverage: f64,
    #[serde(default)]
    pub offers: Vec<CommercialOffer>,
}

impl Default for CommercialPolicy {
    fn default() -> Self {
        Self {
            currency: default_currency(),
            cash_constrained_below_runway_months: default_cash_constrained_runway_months(),
            cash_constrained_allocation: CommercialAllocation::default(),
            stable_allocation: default_stable_allocation(),
            max_active_strategic: default_max_active_strategic(),
            minimum_cash_now_pipeline_coverage: default_pipeline_coverage(),
            offers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CommercialAllocation {
    #[serde(default = "default_cash_now_share_bps")]
    pub cash_now_share_bps: u32,
    #[serde(default = "default_core_share_bps")]
    pub core_share_bps: u32,
    #[serde(default = "default_strategic_share_bps")]
    pub strategic_share_bps: u32,
}

impl Default for CommercialAllocation {
    fn default() -> Self {
        Self {
            cash_now_share_bps: default_cash_now_share_bps(),
            core_share_bps: default_core_share_bps(),
            strategic_share_bps: default_strategic_share_bps(),
        }
    }
}

/// A governed offer definition, not a forecast. Optional money/timing fields
/// remain absent until the founder has an explicit pricing hypothesis or buyer
/// evidence; zero is never used as shorthand for unknown.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct CommercialOffer {
    pub key: String,
    pub name: String,
    /// cash_now | core | strategic
    pub lane: String,
    pub description: String,
    #[serde(default)]
    pub price_min_cents: Option<i64>,
    #[serde(default)]
    pub price_max_cents: Option<i64>,
    #[serde(default)]
    pub delivery_days_min: Option<u32>,
    #[serde(default)]
    pub delivery_days_max: Option<u32>,
    #[serde(default)]
    pub payment_structure: String,
    /// hypothesis | buyer_validated | transaction_validated
    #[serde(default)]
    pub estimate_confidence: String,
    #[serde(default)]
    pub estimate_basis: Vec<String>,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub success_criteria: Vec<String>,
    #[serde(default)]
    pub exclusions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiscoveryEvidence {
    pub id: String,
    #[serde(default)]
    pub recorded_at: String,
    #[serde(default)]
    pub source_kind: String,
    #[serde(default)]
    pub source_url: String,
    #[serde(default)]
    pub segment: String,
    #[serde(default)]
    pub participant_context: String,
    #[serde(default)]
    pub evidence_level: String,
    #[serde(default)]
    pub reported_workflows: Vec<String>,
    #[serde(default)]
    pub reported_estimates: Vec<String>,
    #[serde(default)]
    pub working_interpretations: Vec<String>,
    #[serde(default)]
    pub sourcing_implications: Vec<String>,
    #[serde(default)]
    pub follow_up_angles: Vec<String>,
    #[serde(default)]
    pub next_questions: Vec<String>,
    #[serde(default)]
    pub limits: Vec<String>,
}

/// Per-account send throttles. The business `daily_touch_cap` bounds approved
/// email volume, but says nothing about how that volume is spread. Without these, the
/// cadence engine will happily open a cold email to five people at the same
/// plant within the same hour — which reads as a blast, burns the account, and
/// is exactly the failure mode an autopilot amplifies. These are enforced at
/// send time, so they hold no matter what queued the work (human approval or an
/// autonomous supervisor). A value of `0` disables that particular limit.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountLimits {
    /// How many *new* conversational fronts (a first touch to a not-yet-contacted
    /// person) may open at one account per business day. Follow-ups to people
    /// already in a thread are never blocked by this.
    #[serde(default = "default_max_new_contacts_per_account_per_day")]
    pub max_new_contacts_per_account_per_day: usize,
    /// How many people at one account may be actively worked (contacted or
    /// replied) at once. Bounds parallel fronts across days, not just per day.
    /// Zero leaves the requested recipient count uncapped; the daily opener
    /// limit can still stagger first touches without discarding sequences.
    #[serde(default = "default_max_active_contacts_per_account")]
    pub max_active_contacts_per_account: usize,
    /// Stop a person's sequence once this many touches have been sent with no
    /// reply, rather than marching through every stage of a mis-generated cadence.
    #[serde(default = "default_max_unanswered_touches")]
    pub max_unanswered_touches: usize,
}

impl Default for AccountLimits {
    fn default() -> Self {
        Self {
            max_new_contacts_per_account_per_day: default_max_new_contacts_per_account_per_day(),
            max_active_contacts_per_account: default_max_active_contacts_per_account(),
            max_unanswered_touches: default_max_unanswered_touches(),
        }
    }
}

/// The business-owned outreach calendar. This is deliberately separate from
/// mailbox warm-up limits: adding a second mailbox must never double a
/// business's total daily activity.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutreachCalendar {
    #[serde(default = "default_daily_touch_cap")]
    pub daily_touch_cap: usize,
    #[serde(default = "default_calendar_timezone")]
    pub quota_timezone: String,
    #[serde(default = "default_calendar_timezone")]
    pub fallback_recipient_timezone: String,
    #[serde(default = "default_weekdays")]
    pub weekdays: Vec<String>,
    #[serde(default = "default_window_start")]
    pub window_start: u32,
    #[serde(default = "default_window_end")]
    pub window_end: u32,
    #[serde(default = "default_preferred_hours")]
    pub preferred_hours: Vec<u32>,
    #[serde(default = "default_learning_min_samples")]
    pub learning_min_samples: usize,
    #[serde(default)]
    pub rules: Vec<TimingRule>,
}

impl Default for OutreachCalendar {
    fn default() -> Self {
        Self {
            daily_touch_cap: default_daily_touch_cap(),
            quota_timezone: default_calendar_timezone(),
            fallback_recipient_timezone: default_calendar_timezone(),
            weekdays: default_weekdays(),
            window_start: default_window_start(),
            window_end: default_window_end(),
            preferred_hours: default_preferred_hours(),
            learning_min_samples: default_learning_min_samples(),
            rules: Vec::new(),
        }
    }
}

/// A named timing hypothesis. Match fields are ANDed across groups and ORed
/// within a group. This makes weekend activity explicit and auditable instead
/// of allowing a model to infer that every person in an industry works Sunday.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TimingRule {
    pub key: String,
    #[serde(default)]
    pub industries: Vec<String>,
    #[serde(default)]
    pub title_keywords: Vec<String>,
    #[serde(default)]
    pub vantages: Vec<String>,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub weekdays: Vec<String>,
    #[serde(default)]
    pub preferred_hours: Vec<u32>,
    #[serde(default)]
    pub window_start: Option<u32>,
    #[serde(default)]
    pub window_end: Option<u32>,
    #[serde(default)]
    pub rationale: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Motion {
    pub key: String,
    pub kind: String,
    pub objective: String,
    #[serde(default = "yes")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FundingProfile {
    pub objective: String,
    #[serde(default)]
    pub themes: Vec<String>,
    #[serde(default)]
    pub project_shapes: Vec<String>,
    #[serde(default)]
    pub preferred_contact_titles: Vec<String>,
    #[serde(default)]
    pub sources: Vec<OpportunitySource>,
    #[serde(default = "default_funding_min_words")]
    pub min_words: usize,
    #[serde(default = "default_funding_max_words")]
    pub max_words: usize,
    #[serde(default = "default_funding_touches")]
    pub default_touches: usize,
    #[serde(default)]
    pub doctrine: String,
}

/// A bounded commercial sponsorship motion. Unlike a grant, the counterparty
/// buys named infrastructure support and benefits under an invoice/agreement
/// and receives no control over independent data treatment. This stays separate from `FundingProfile` so a
/// business cannot accidentally send grant-eligibility copy to a commercial
/// sponsor or build an application where a sales process is required.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SponsorshipProfile {
    pub objective: String,
    pub offer_key: String,
    pub offer_name: String,
    pub ask_amount_cad: i64,
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default)]
    pub primary_goal: String,
    #[serde(default)]
    pub target_industries: Vec<String>,
    #[serde(default)]
    pub target_keywords: Vec<String>,
    #[serde(default)]
    pub preferred_contact_titles: Vec<String>,
    #[serde(default)]
    pub founder_story: String,
    #[serde(default)]
    pub product_truth: Vec<String>,
    #[serde(default)]
    pub dynamic_metrics: Vec<String>,
    #[serde(default)]
    pub founder_reported_conversations: Vec<String>,
    #[serde(default)]
    pub conversation_claim_rules: Vec<String>,
    #[serde(default)]
    pub sponsorship_need: String,
    #[serde(default)]
    pub public_interest_case: Vec<String>,
    #[serde(default)]
    pub permitted_sponsor_benefits: Vec<String>,
    #[serde(default)]
    pub sponsor_independence: Vec<String>,
    #[serde(default)]
    pub prohibited_claims: Vec<String>,
    #[serde(default)]
    pub email_structure: Vec<String>,
    #[serde(default)]
    pub voice: Vec<String>,
    #[serde(default)]
    pub routes: Vec<SponsorshipRoute>,
    #[serde(default = "default_funding_min_words")]
    pub min_words: usize,
    #[serde(default = "default_funding_max_words")]
    pub max_words: usize,
    #[serde(default = "default_funding_touches")]
    pub default_touches: usize,
    #[serde(default)]
    pub doctrine: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SponsorshipRoute {
    pub recipient_kind: String,
    pub action: String,
    #[serde(default)]
    pub target_roles: Vec<String>,
    #[serde(default)]
    pub budget_evidence_terms: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpportunitySource {
    pub name: String,
    /// `catalog` extracts programme links; `program` treats the URL as one
    /// opportunity; `search` uses Jina Search and requires JINA_API_KEY.
    pub mode: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    /// Optional Apollo employer hints for programme pages whose public-web
    /// domain or programme name differs from the administering organization.
    #[serde(default)]
    pub apollo_organization_name: String,
    #[serde(default)]
    pub apollo_domains: Vec<String>,
}

fn yes() -> bool {
    true
}

fn default_funding_min_words() -> usize {
    80
}

fn default_funding_max_words() -> usize {
    170
}

fn default_funding_touches() -> usize {
    2
}

fn default_daily_touch_cap() -> usize {
    30
}

fn default_max_new_contacts_per_account_per_day() -> usize {
    1
}

fn default_max_active_contacts_per_account() -> usize {
    0
}

fn default_max_unanswered_touches() -> usize {
    5
}

fn default_calendar_timezone() -> String {
    "Europe/London".into()
}

fn default_weekdays() -> Vec<String> {
    ["mon", "tue", "wed", "thu", "fri"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn default_window_start() -> u32 {
    8
}

fn default_window_end() -> u32 {
    17
}

fn default_preferred_hours() -> Vec<u32> {
    vec![9, 11, 14]
}

fn default_learning_min_samples() -> usize {
    20
}

fn default_currency() -> String {
    "CAD".into()
}

fn default_cash_constrained_runway_months() -> u32 {
    6
}

fn default_cash_now_share_bps() -> u32 {
    7000
}

fn default_core_share_bps() -> u32 {
    2000
}

fn default_strategic_share_bps() -> u32 {
    1000
}

fn default_stable_allocation() -> CommercialAllocation {
    CommercialAllocation {
        cash_now_share_bps: 5000,
        core_share_bps: 3500,
        strategic_share_bps: 1500,
    }
}

fn default_max_active_strategic() -> usize {
    3
}

fn default_pipeline_coverage() -> f64 {
    3.0
}

pub struct Businesses {
    profiles: BTreeMap<String, BusinessProfile>,
}

impl Businesses {
    pub fn load(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let mut profiles = BTreeMap::new();
        for key in ["gnk", "wapahki", "outagehub"] {
            let path = dir.join(format!("{key}.toml"));
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading business profile {}", path.display()))?;
            let profile: BusinessProfile = toml::from_str(&raw)
                .with_context(|| format!("parsing business profile {}", path.display()))?;
            if profile.key != key {
                return Err(anyhow!(
                    "{}: key = '{}' does not match filename '{key}.toml'",
                    path.display(),
                    profile.key
                ));
            }
            validate(&profile).with_context(|| format!("validating {key} business profile"))?;
            profiles.insert(key.to_string(), profile);
        }
        Ok(Self { profiles })
    }

    pub fn get(&self, key: &str) -> Result<&BusinessProfile> {
        self.profiles.get(key).ok_or_else(|| {
            anyhow!(
                "unknown business '{key}'. Available: {}",
                self.keys().join(", ")
            )
        })
    }

    pub fn keys(&self) -> Vec<&str> {
        self.profiles.keys().map(String::as_str).collect()
    }

    /// One compact line per business, so the router keeps a sense of the whole
    /// portfolio (what each brand is) even while it acts on the active one.
    pub fn roster(&self) -> String {
        self.profiles
            .values()
            .map(|p| format!("- {} ({}): {}", p.name, p.key, p.summary))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl BusinessProfile {
    pub fn has_motion(&self, kind: &str) -> bool {
        self.motions
            .iter()
            .any(|m| m.enabled && m.kind.eq_ignore_ascii_case(kind))
    }

    pub fn funding(&self) -> Result<&FundingProfile> {
        if !self.has_motion("funding") {
            return Err(anyhow!(
                "{} does not have an enabled funding motion",
                self.name
            ));
        }
        self.funding
            .as_ref()
            .ok_or_else(|| anyhow!("{} has no funding profile", self.name))
    }

    pub fn sponsorship(&self) -> Result<&SponsorshipProfile> {
        if !self.has_motion("sponsorship") {
            return Err(anyhow!(
                "{} does not have an enabled sponsorship motion",
                self.name
            ));
        }
        self.sponsorship
            .as_ref()
            .ok_or_else(|| anyhow!("{} has no sponsorship profile", self.name))
    }

    pub fn commercial_offer(&self, key: &str) -> Option<&CommercialOffer> {
        self.commercial.offers.iter().find(|offer| offer.key == key)
    }

    /// Unknown runway takes the conservative cash-constrained allocation. A
    /// founder must explicitly record sufficient runway before strategic work
    /// receives the stable-business share.
    pub fn commercial_allocation(&self, runway_months: Option<f64>) -> &CommercialAllocation {
        if runway_months.is_some_and(|months| {
            months >= self.commercial.cash_constrained_below_runway_months as f64
        }) {
            &self.commercial.stable_allocation
        } else {
            &self.commercial.cash_constrained_allocation
        }
    }

    pub fn agent_summary(&self) -> String {
        let motions = self
            .motions
            .iter()
            .filter(|m| m.enabled)
            .map(|m| format!("{} ({})", m.key, m.kind))
            .collect::<Vec<_>>()
            .join(", ");
        let mut summary = format!(
            "{}: {} Enabled motions: {}. Outreach calendar: at most {} approved emails per {} day; recipient-local timing with named industry/title exceptions.",
            self.name,
            self.summary,
            motions,
            self.calendar.daily_touch_cap,
            self.calendar.quota_timezone,
        );
        if !self.goals.is_empty() {
            summary.push_str(&format!(
                " What {} is trying to accomplish: {}",
                self.name,
                self.goals.join(" ")
            ));
        }
        if !self.constraints.is_empty() {
            summary.push_str(&format!(
                " Hard constraints: {}",
                self.constraints.join(" ")
            ));
        }
        if !self.commercial.offers.is_empty() {
            summary.push_str(&format!(
                " Commercial offers: {}. Unknown runway uses the cash-constrained {}% / {}% / {}% cash-now/core/strategic allocation.",
                self.commercial
                    .offers
                    .iter()
                    .map(|offer| format!("{} ({})", offer.key, offer.lane))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.commercial.cash_constrained_allocation.cash_now_share_bps / 100,
                self.commercial.cash_constrained_allocation.core_share_bps / 100,
                self.commercial.cash_constrained_allocation.strategic_share_bps / 100,
            ));
        }
        summary
    }

    /// The durable operating context handed to the sourcing pipeline: what the
    /// business is, what it is actually trying to accomplish with this outreach,
    /// and the constraints it must respect. Threading this into ICP derivation
    /// and qualification is what makes those stages judge a candidate against the
    /// *business's* real goals instead of a single one-line motion.
    pub fn operating_context(&self) -> String {
        let mut s = format!("ABOUT {} — {}\n", self.name, self.summary);
        if !self.known_facts.is_empty() {
            s.push_str("What is true about the business (may be stated plainly):\n");
            for fact in &self.known_facts {
                s.push_str(&format!("  - {fact}\n"));
            }
        }
        if !self.goals.is_empty() {
            s.push_str("What the business is trying to accomplish with this outreach:\n");
            for goal in &self.goals {
                s.push_str(&format!("  - {goal}\n"));
            }
        }
        if !self.constraints.is_empty() {
            s.push_str("Hard constraints (never violate):\n");
            for constraint in &self.constraints {
                s.push_str(&format!("  - {constraint}\n"));
            }
        }
        if !self.unknowns.is_empty() {
            s.push_str("Open unknowns (do not assume you know the answer to these):\n");
            for unknown in &self.unknowns {
                s.push_str(&format!("  - {unknown}\n"));
            }
        }
        if !self.discovery_evidence.is_empty() {
            s.push_str(
                "Founder discovery evidence (market-level context, NOT proof about a candidate account):\n",
            );
            for call in &self.discovery_evidence {
                s.push_str(&format!(
                    "  CALL {} — {} | {} | {}\n",
                    call.id, call.segment, call.participant_context, call.evidence_level
                ));
                for item in &call.reported_workflows {
                    s.push_str(&format!("    - Participant reported: {item}\n"));
                }
                for item in &call.reported_estimates {
                    s.push_str(&format!("    - Unverified estimate/example: {item}\n"));
                }
                for item in &call.working_interpretations {
                    s.push_str(&format!("    - Working interpretation: {item}\n"));
                }
                for item in &call.sourcing_implications {
                    s.push_str(&format!("    - Sourcing implication: {item}\n"));
                }
                for item in &call.follow_up_angles {
                    s.push_str(&format!(
                        "    - Permitted call-grounded follow-up angle: {item}\n"
                    ));
                }
                for item in &call.next_questions {
                    s.push_str(&format!("    - Still ask: {item}\n"));
                }
                for item in &call.limits {
                    s.push_str(&format!("    - Evidence boundary: {item}\n"));
                }
                if !call.source_url.is_empty() {
                    s.push_str(&format!("    - Source record: {}\n", call.source_url));
                }
            }
            s.push_str(
                "Use these calls to choose segments and ask sharper questions. They do not establish any target company's workflow. In copy, attribute a call insight explicitly (for example, 'In a recent conversation with a technical manager...') and ask whether it matches this recipient's experience. Never imply multiple buyers, consensus, ROI, or adoption.\n",
            );
        }
        s.trim_end().to_string()
    }
}

fn validate(profile: &BusinessProfile) -> Result<()> {
    if profile.name.trim().is_empty() || profile.summary.trim().is_empty() {
        return Err(anyhow!("name and summary are required"));
    }
    let mut discovery_ids = std::collections::HashSet::new();
    for evidence in &profile.discovery_evidence {
        if evidence.id.trim().is_empty()
            || evidence.segment.trim().is_empty()
            || evidence.participant_context.trim().is_empty()
        {
            return Err(anyhow!(
                "discovery evidence requires id, segment, and participant_context"
            ));
        }
        if !discovery_ids.insert(evidence.id.trim()) {
            return Err(anyhow!("duplicate discovery evidence id '{}'", evidence.id));
        }
    }
    validate_commercial_policy(profile)?;
    if profile.has_motion("funding") {
        let funding = profile
            .funding
            .as_ref()
            .ok_or_else(|| anyhow!("funding motion requires a [funding] section"))?;
        if funding.sources.is_empty() {
            return Err(anyhow!(
                "funding motion requires at least one [[funding.sources]]"
            ));
        }
        for source in &funding.sources {
            match source.mode.as_str() {
                "catalog" | "program" if source.url.trim().is_empty() => {
                    return Err(anyhow!("source '{}' requires url", source.name));
                }
                "search" if source.query.trim().is_empty() => {
                    return Err(anyhow!("search source '{}' requires query", source.name));
                }
                "catalog" | "program" | "search" => {}
                other => {
                    return Err(anyhow!(
                        "source '{}' has unsupported mode '{other}'",
                        source.name
                    ));
                }
            }
        }
    }
    if profile.has_motion("sponsorship") {
        let sponsorship = profile
            .sponsorship
            .as_ref()
            .ok_or_else(|| anyhow!("sponsorship motion requires a [sponsorship] section"))?;
        if sponsorship.offer_key.trim().is_empty()
            || sponsorship.offer_name.trim().is_empty()
            || sponsorship.objective.trim().is_empty()
        {
            return Err(anyhow!(
                "sponsorship requires objective, offer_key, and offer_name"
            ));
        }
        if sponsorship.ask_amount_cad <= 0 {
            return Err(anyhow!("sponsorship requires a positive ask_amount_cad"));
        }
        if sponsorship.default_touches == 0
            || sponsorship.default_touches > 2
            || sponsorship.min_words == 0
            || sponsorship.max_words < sponsorship.min_words
        {
            return Err(anyhow!(
                "sponsorship permits one or two touches and requires a valid word band"
            ));
        }
        if sponsorship.product_truth.is_empty()
            || sponsorship.permitted_sponsor_benefits.is_empty()
            || sponsorship.sponsor_independence.is_empty()
            || sponsorship.target_keywords.is_empty()
            || sponsorship.routes.is_empty()
            || sponsorship.sponsorship_need.trim().is_empty()
        {
            return Err(anyhow!(
                "sponsorship requires target keywords, product truth, sponsor benefits, independence terms, routes, and sponsorship need"
            ));
        }
        let mut route_kinds = std::collections::HashSet::new();
        for route in &sponsorship.routes {
            if route.recipient_kind.trim().is_empty()
                || route.action.trim().is_empty()
                || route.target_roles.is_empty()
                || route.budget_evidence_terms.is_empty()
            {
                return Err(anyhow!(
                    "sponsorship routes require recipient_kind, action, target_roles, and budget_evidence_terms"
                ));
            }
            if !route_kinds.insert(route.recipient_kind.trim()) {
                return Err(anyhow!(
                    "duplicate sponsorship recipient kind '{}'",
                    route.recipient_kind
                ));
            }
        }
        let price_cents = sponsorship.ask_amount_cad.saturating_mul(100);
        let offer = profile
            .commercial_offer(&sponsorship.offer_key)
            .ok_or_else(|| anyhow!("sponsorship offer_key must name a commercial offer"))?;
        if offer.price_min_cents != Some(price_cents) || offer.price_max_cents != Some(price_cents)
        {
            return Err(anyhow!(
                "sponsorship price must match the exact configured commercial offer price"
            ));
        }
    }
    validate_calendar(&profile.calendar)?;
    Ok(())
}

fn validate_commercial_policy(profile: &BusinessProfile) -> Result<()> {
    let commercial = &profile.commercial;
    if commercial.currency.trim().is_empty() {
        return Err(anyhow!("commercial.currency is required"));
    }
    for (label, allocation) in [
        (
            "cash_constrained_allocation",
            &commercial.cash_constrained_allocation,
        ),
        ("stable_allocation", &commercial.stable_allocation),
    ] {
        let total = allocation.cash_now_share_bps
            + allocation.core_share_bps
            + allocation.strategic_share_bps;
        if total != 10_000 {
            return Err(anyhow!(
                "commercial.{label} must total 10000 basis points, got {total}"
            ));
        }
    }
    if commercial.minimum_cash_now_pipeline_coverage < 0.0 {
        return Err(anyhow!(
            "commercial.minimum_cash_now_pipeline_coverage cannot be negative"
        ));
    }
    let mut offer_keys = std::collections::HashSet::new();
    for offer in &commercial.offers {
        if offer.key.trim().is_empty()
            || offer.name.trim().is_empty()
            || offer.description.trim().is_empty()
        {
            return Err(anyhow!(
                "commercial offers require key, name, and description"
            ));
        }
        if !offer_keys.insert(offer.key.trim()) {
            return Err(anyhow!("duplicate commercial offer key '{}'", offer.key));
        }
        if !matches!(offer.lane.as_str(), "cash_now" | "core" | "strategic") {
            return Err(anyhow!(
                "commercial offer '{}' has unsupported lane '{}'",
                offer.key,
                offer.lane
            ));
        }
        if let (Some(min), Some(max)) = (offer.price_min_cents, offer.price_max_cents) {
            if min < 0 || max < min {
                return Err(anyhow!(
                    "commercial offer '{}' has an invalid price range",
                    offer.key
                ));
            }
        }
        if offer.price_min_cents.is_some() || offer.price_max_cents.is_some() {
            if offer.estimate_confidence.trim().is_empty() || offer.estimate_basis.is_empty() {
                return Err(anyhow!(
                    "priced commercial offer '{}' requires confidence and estimate_basis",
                    offer.key
                ));
            }
        }
        if let (Some(min), Some(max)) = (offer.delivery_days_min, offer.delivery_days_max) {
            if min == 0 || max < min {
                return Err(anyhow!(
                    "commercial offer '{}' has an invalid delivery window",
                    offer.key
                ));
            }
        }
    }
    if profile.has_motion("sales") && commercial.offers.is_empty() {
        return Err(anyhow!(
            "sales motion requires at least one [[commercial.offers]]"
        ));
    }
    Ok(())
}

fn validate_calendar(calendar: &OutreachCalendar) -> Result<()> {
    if calendar.daily_touch_cap == 0 {
        return Err(anyhow!(
            "calendar.daily_touch_cap must be greater than zero"
        ));
    }
    for (label, value) in [
        ("quota_timezone", calendar.quota_timezone.as_str()),
        (
            "fallback_recipient_timezone",
            calendar.fallback_recipient_timezone.as_str(),
        ),
    ] {
        value
            .parse::<chrono_tz::Tz>()
            .map_err(|_| anyhow!("calendar.{label} is not a valid IANA timezone: '{value}'"))?;
    }
    validate_window(
        calendar.window_start,
        calendar.window_end,
        &calendar.preferred_hours,
        "calendar",
    )?;
    validate_weekdays(&calendar.weekdays, "calendar.weekdays")?;
    for rule in &calendar.rules {
        if rule.key.trim().is_empty() {
            return Err(anyhow!("calendar rule key is required"));
        }
        let start = rule.window_start.unwrap_or(calendar.window_start);
        let end = rule.window_end.unwrap_or(calendar.window_end);
        let hours = if rule.preferred_hours.is_empty() {
            &calendar.preferred_hours
        } else {
            &rule.preferred_hours
        };
        validate_window(start, end, hours, &format!("calendar rule '{}'", rule.key))?;
        if !rule.weekdays.is_empty() {
            validate_weekdays(
                &rule.weekdays,
                &format!("calendar rule '{}'.weekdays", rule.key),
            )?;
        }
    }
    Ok(())
}

fn validate_window(start: u32, end: u32, hours: &[u32], label: &str) -> Result<()> {
    if start >= end || end > 24 {
        return Err(anyhow!(
            "{label} window must satisfy 0 <= start < end <= 24"
        ));
    }
    if hours.is_empty() || hours.iter().any(|hour| *hour < start || *hour >= end) {
        return Err(anyhow!(
            "{label} preferred_hours must be non-empty and inside [{start}, {end})"
        ));
    }
    Ok(())
}

fn validate_weekdays(days: &[String], label: &str) -> Result<()> {
    if days.is_empty() {
        return Err(anyhow!("{label} must not be empty"));
    }
    for day in days {
        if !matches!(
            day.trim().to_ascii_lowercase().as_str(),
            "mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun"
        ) {
            return Err(anyhow!("{label} contains unknown weekday '{day}'"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Businesses;

    #[test]
    fn loads_three_distinct_businesses_and_outagehub_sponsorship() {
        let businesses = Businesses::load("businesses").expect("business profiles");
        assert_eq!(businesses.keys(), vec!["gnk", "outagehub", "wapahki"]);
        assert!(businesses.keys().into_iter().all(|key| businesses
            .get(key)
            .unwrap()
            .account_limits
            .max_active_contacts_per_account
            == 0));
        assert!(!businesses.get("gnk").unwrap().has_motion("funding"));
        let outagehub = businesses.get("outagehub").unwrap();
        assert!(!outagehub.has_motion("funding"));
        assert!(outagehub.has_motion("sponsorship"));
        let sponsorship = outagehub.sponsorship().unwrap();
        assert_eq!(sponsorship.ask_amount_cad, 10_000);
        assert_eq!(sponsorship.routes.len(), 5);
        assert_eq!(
            outagehub
                .commercial_offer("founding_infrastructure_sponsorship")
                .unwrap()
                .price_min_cents,
            Some(1_000_000)
        );
        let wapahki = businesses.get("wapahki").unwrap();
        assert_eq!(wapahki.discovery_evidence.len(), 2);
        assert_eq!(wapahki.commercial.offers.len(), 3);
        assert_eq!(
            wapahki
                .commercial_offer("paid_task_feasibility_sprint")
                .unwrap()
                .lane,
            "cash_now"
        );
        assert_eq!(
            wapahki.commercial_allocation(Some(2.0)).cash_now_share_bps,
            7_000
        );
        assert_eq!(
            wapahki.commercial_allocation(Some(8.0)).cash_now_share_bps,
            6_000
        );
        for key in ["gnk", "wapahki"] {
            let profile = businesses.get(key).unwrap();
            assert_eq!(profile.commercial.offers.len(), 3);
            assert!(profile
                .commercial_offer(&profile.commercial.offers[0].key)
                .is_some());
        }
        assert_eq!(outagehub.commercial.offers.len(), 4);
        let context = wapahki.operating_context();
        assert!(context.contains("Founder discovery evidence"));
        assert!(context.contains("NOT proof about a candidate account"));
        assert!(context.contains("recent conversation with a technical manager"));
    }
}
