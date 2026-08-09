//! Core domain model, rebuilt around the outreach doctrine.
//!
//! The old model forced a fabricated `estimated_annual_cost_usd` onto every
//! company — exactly the invented-ROI move the doctrine forbids. The model now
//! separates what we can state as fact (observed_facts) from what we may only
//! guess (inferences) and the commercial question we are actually testing
//! (hypothesis), and it carries the mechanism, a *measurable* consequence (never
//! dollars), the one narrow system concept, the hardest buyer question, and the
//! kill condition.
//!
//! Types that come back from the model derive `Deserialize` so structured-output
//! JSON maps straight on. The assembled `Campaign` tree also derives `Serialize`.

use serde::{Deserialize, Serialize};

/// An account we believe may have an expensive, specific, addressable workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub name: String,
    pub industry: String,
    pub hq: String,

    /// Publicly supportable facts. ONLY these may be stated as fact in copy.
    pub observed_facts: Vec<String>,
    /// Reasonable but unproven guesses — framed as guesses in copy.
    pub inferences: Vec<String>,
    /// The commercial question being tested: the workflow that may be expensive.
    pub hypothesis: String,
    /// Why the burden/time/risk occurs (records spread across systems, etc.).
    pub mechanism: String,
    /// A metric that could be measured if the hypothesis holds — NOT a dollar figure.
    pub consequence_metric: String,
    /// Independent signals that make the hypothesis plausible.
    pub signals: Vec<String>,
    /// The one concrete, narrow system/offer this brand could build for them.
    pub system_concept: String,
    /// The strongest skeptical objection the buyer could raise.
    pub hard_buyer_question: String,
    /// What would prove there is no meaningful problem here.
    pub kill_condition: String,
    /// Optional internal-only magnitude guess. Never a buyer-facing claim.
    #[serde(default)]
    pub magnitude_note: String,
    /// `[id]`s of the book-library principles the model applied when selecting
    /// and framing this account (see `crate::knowledge`). Empty when no library
    /// is loaded.
    #[serde(default)]
    pub applied_principles: Vec<String>,
}

/// Wrapper matching the `accounts` array in the structured-output schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Accounts {
    pub accounts: Vec<Account>,
}

/// A person chosen by vantage point (what they can observe/decide/route), not
/// by seniority.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contact {
    pub name: String,
    pub title: String,
    /// One of: process_owner | operator | operational_executive |
    /// technical_evaluator | economic_buyer | router.
    pub vantage: String,
    /// What this person can credibly observe, explain, decide, or route.
    pub can_observe: String,
    /// Why we chose them — expressed through their vantage, not their title.
    pub why_them: String,
    /// Whether this is the primary contact to activate first.
    #[serde(default)]
    pub primary: bool,
    /// If a router, who they might point us to.
    #[serde(default)]
    pub route_to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contacts {
    pub contacts: Vec<Contact>,
}

/// One touch in the unfolding investigation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Touch {
    pub stage: u32,
    /// Days after the start of the sequence that this touch fires.
    pub day_offset: u32,
    /// "email" | "linkedin" | "linkedin_request" | "linkedin_or_email".
    pub channel: String,
    /// Empty for channels without a subject.
    pub subject: String,
    pub body: String,
    /// Which move in the sequence this is (observation, diagnostic, useful
    /// contribution, hard question, routing, or close).
    pub purpose: String,
    /// What this specific touch is trying to achieve.
    pub goal: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sequence {
    pub touches: Vec<Touch>,
    /// `[id]`s of the book-library principles the model applied when writing this
    /// sequence's copy. Empty when no library is loaded.
    #[serde(default)]
    pub applied_principles: Vec<String>,
}

// --- Critique pass ---------------------------------------------------------

/// The compact reviewer verdict on one touch. Rewrites are applied directly to
/// the sequence and are not duplicated in the persisted review metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TouchReview {
    pub stage: u32,
    /// Does the REVISED touch pass the pre-send test and score at least 80/100?
    pub passes: bool,
    /// Sendability score for the revised touch.
    #[serde(default)]
    pub score: u32,
    /// Specific problems found (voice, leaked fact, weak question, etc.).
    #[serde(default)]
    pub issues: Vec<String>,
}

// --- Assembled output tree -------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct ContactPlan {
    pub contact: Contact,
    pub sequence: Sequence,
    /// Per-touch critique notes, parallel to `sequence.touches`.
    pub reviews: Vec<TouchReview>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountPlan {
    pub account: Account,
    pub contacts: Vec<ContactPlan>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Campaign {
    /// Brand key: gnk | wapahki | outagehub.
    pub brand: String,
    pub thesis: String,
    pub accounts: Vec<AccountPlan>,
}
