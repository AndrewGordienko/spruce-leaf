//! Prompts + JSON schemas for each pipeline stage.
//!
//! The heavy doctrine lives in the brand's system prompt (built in `playbook.rs`
//! from the editable `playbooks/*.toml`). Here we build the per-stage USER
//! prompt and its structured-output schema. Counts are enforced via prompt text;
//! schemas avoid `minItems`/`maxItems` since the raw API rejects most array/
//! numeric constraints.

use serde_json::{json, Value};

use crate::domain::{Account, Contact, Sequence};
use crate::playbook::{Lint, Playbook};

/// Append a retrieved book-knowledge playbook block (if any) to a stage's user
/// prompt. `cite` toggles the citation instruction for stages whose schema has
/// an `applied_principles` field. A no-op when `knowledge` is empty, so the
/// pipeline behaves identically before any books are ingested.
fn with_knowledge(base: String, knowledge: &str, cite: bool) -> String {
    if knowledge.trim().is_empty() {
        return base;
    }
    let tail = if cite {
        "\n\nGround your choices in the playbook above where a principle genuinely applies, and \
put the [id]s of the principles you actually used in `applied_principles` (leave it an empty \
array if none truly applied — do not cite for the sake of citing)."
    } else {
        "\n\nGround your choices in the playbook above where a principle genuinely applies."
    };
    format!("{base}\n\n--- BOOK PLAYBOOK ---\n{knowledge}{tail}")
}

// --- Stage 1: accounts -----------------------------------------------------

pub fn accounts_user(pb: &Playbook, thesis: &str, n: usize, knowledge: &str) -> String {
    let base = format!(
        "Find {n} distinct accounts (companies/organizations) that plausibly have this workflow, \
in the context of: {thesis}.\n\n\
For {name}, the workflow we test for is: {motion}.\n\n\
For each account, separate what you can support from what you are guessing:\n\
  - observed_facts: 2-4 publicly supportable facts (only these may later be stated as fact).\n\
  - inferences: 1-3 reasonable but unproven guesses the facts make plausible.\n\
  - hypothesis: the ONE commercial question — the specific recurring workflow/decision that may \
be expensive, framed as something to test.\n\
  - mechanism: WHY the burden, time, or risk occurs (e.g. the record is in one system, the \
evidence needed to act on it is spread across others).\n\
  - consequence_metric: a metric that could be MEASURED if the hypothesis holds (hours, decisions \
changed, handling time, false escalations, time-to-answer). NEVER a dollar figure.\n\
  - signals: {min_signals}+ independent signals that make the hypothesis plausible.\n\
  - system_concept: the ONE concrete, narrow system/offer {name} could build for this account.\n\
  - hard_buyer_question: the strongest skeptical objection this buyer could raise.\n\
  - kill_condition: what would prove there is no meaningful problem here.\n\
  - magnitude_note: OPTIONAL, internal-only rough magnitude guess — never a buyer-facing claim; \
leave empty unless useful.\n\n\
Pick accounts where the workflow is genuinely plausible, not generic. A single weak signal \
(e.g. 'they are hiring') is not a thesis.",
        name = pb.name,
        motion = pb.motion,
        min_signals = pb.min_signals,
    );
    with_knowledge(base, knowledge, true)
}

pub fn accounts_schema() -> Value {
    let str_arr = json!({ "type": "array", "items": { "type": "string" } });
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["accounts"],
        "properties": {
            "accounts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "name", "industry", "hq", "observed_facts", "inferences", "hypothesis",
                        "mechanism", "consequence_metric", "signals", "system_concept",
                        "hard_buyer_question", "kill_condition", "magnitude_note",
                        "applied_principles"
                    ],
                    "properties": {
                        "name": { "type": "string" },
                        "industry": { "type": "string" },
                        "hq": { "type": "string" },
                        "observed_facts": str_arr,
                        "inferences": str_arr,
                        "hypothesis": { "type": "string" },
                        "mechanism": { "type": "string" },
                        "consequence_metric": { "type": "string" },
                        "signals": str_arr,
                        "system_concept": { "type": "string" },
                        "hard_buyer_question": { "type": "string" },
                        "kill_condition": { "type": "string" },
                        "magnitude_note": { "type": "string" },
                        "applied_principles": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "[id]s of any BOOK PLAYBOOK principles applied; empty array if none/no playbook."
                        }
                    }
                }
            }
        }
    })
}

// --- Stage 2: contacts -----------------------------------------------------

pub fn contacts_user(pb: &Playbook, account: &Account, n: usize, knowledge: &str) -> String {
    let base = format!(
        "At {acct} ({industry}, HQ {hq}), map {n} people to reach for {brand} about this workflow:\n\n\
\"{hypothesis}\"\n\n\
Choose people by VANTAGE POINT, not seniority. For each, set:\n\
  - vantage: exactly one of process_owner, operator, operational_executive, technical_evaluator, \
economic_buyer, router.\n\
  - can_observe: what this person can credibly observe, explain, decide, or route.\n\
  - why_them: why we chose them, expressed through their vantage — NOT by repeating their title.\n\
  - primary: set true for exactly ONE person (the one to activate first — usually the process \
owner/operator closest to the work, or a credible router if ownership is unclear).\n\
  - route_to: if a router, who they might point us to; otherwise empty.\n\n\
If you don't know a real named executive, give the most likely title and a clearly-illustrative \
placeholder name — these are hypotheses to verify against real data. Prefer people who can \
actually answer the question over people who merely hold a senior title.",
        acct = account.name,
        industry = account.industry,
        hq = account.hq,
        hypothesis = account.hypothesis,
        brand = pb.name,
    );
    with_knowledge(base, knowledge, false)
}

pub fn contacts_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["contacts"],
        "properties": {
            "contacts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["name", "title", "vantage", "can_observe", "why_them", "primary", "route_to"],
                    "properties": {
                        "name": { "type": "string" },
                        "title": { "type": "string" },
                        "vantage": { "type": "string" },
                        "can_observe": { "type": "string" },
                        "why_them": { "type": "string" },
                        "primary": { "type": "boolean" },
                        "route_to": { "type": "string" }
                    }
                }
            }
        }
    })
}

// --- Stage 3: sequence -----------------------------------------------------

pub fn sequence_user(pb: &Playbook, account: &Account, contact: &Contact, n: usize, knowledge: &str) -> String {
    let base = format!(
        "Write a {n}-touch outreach sequence to {name} ({title}, vantage: {vantage}) at {acct}.\n\n\
ACCOUNT CONTEXT (your research — most of this stays OUT of the email):\n\
  Hypothesis to test: {hypothesis}\n\
  Mechanism: {mechanism}\n\
  Measurable consequence (a metric, never dollars): {consequence}\n\
  What {name} can observe: {can_observe}\n\
  One system concept to propose: {system_concept}\n\
  Hardest buyer question: {hard_q}\n\
  Observed facts (only THESE may be stated as fact): {facts}\n\n\
Number the touches 1..{n}. The sequence is ONE unfolding investigation, each touch adding a new \
angle — not restatements. Match the channel and ask to {name}'s vantage. Use email/linkedin/call. \
For each touch set: day_offset from the start; channel; subject (empty string for non-email); the \
body copy; purpose (which move this is — observation / diagnostic / consequence / artifact / \
hard-question / routing / close); and goal.\n\n\
Follow the length band, the voice, the forbidden list, and the pre-send test from the doctrine. \
Center the operating decision, not {brand}. Keep the whole sequence on ONE problem thread. A \
correction or a referral is a successful outcome.",
        name = contact.name,
        title = contact.title,
        vantage = contact.vantage,
        acct = account.name,
        hypothesis = account.hypothesis,
        mechanism = account.mechanism,
        consequence = account.consequence_metric,
        can_observe = contact.can_observe,
        system_concept = account.system_concept,
        hard_q = account.hard_buyer_question,
        facts = account.observed_facts.join(" | "),
        brand = pb.name,
    );
    with_knowledge(base, knowledge, true)
}

pub fn sequence_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["touches", "applied_principles"],
        "properties": {
            "applied_principles": {
                "type": "array",
                "items": { "type": "string" },
                "description": "[id]s of any BOOK PLAYBOOK principles applied to this sequence's copy; empty array if none/no playbook."
            },
            "touches": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["stage", "day_offset", "channel", "subject", "body", "purpose", "goal"],
                    "properties": {
                        "stage": { "type": "integer" },
                        "day_offset": { "type": "integer" },
                        "channel": { "type": "string" },
                        "subject": { "type": "string" },
                        "body": { "type": "string" },
                        "purpose": { "type": "string" },
                        "goal": { "type": "string" }
                    }
                }
            }
        }
    })
}

// --- Stage 4: critique + revise --------------------------------------------

/// Build the critic prompt, feeding in the mechanical lint findings so the model
/// must fix forbidden phrases and length as well as doctrine issues.
pub fn critique_user(
    pb: &Playbook,
    account: &Account,
    contact: &Contact,
    sequence: &Sequence,
    lints: &[Lint],
) -> String {
    let mut touches = String::new();
    for (t, lint) in sequence.touches.iter().zip(lints) {
        touches.push_str(&format!(
            "\n--- Touch {} ({}, day {}, purpose: {}) ---\n",
            t.stage, t.channel, t.day_offset, t.purpose
        ));
        if !t.subject.is_empty() {
            touches.push_str(&format!("Subject: {}\n", t.subject));
        }
        touches.push_str(&format!("Body:\n{}\n", t.body));
        let mut flags = Vec::new();
        if !lint.forbidden_hits.is_empty() {
            flags.push(format!("FORBIDDEN PHRASES PRESENT: {}", lint.forbidden_hits.join(", ")));
        }
        if !lint.length_ok {
            flags.push(format!(
                "LENGTH OFF: {} words (band is {}–{})",
                lint.word_count, pb.min_words, pb.max_words
            ));
        }
        if !flags.is_empty() {
            touches.push_str(&format!("MECHANICAL FLAGS: {}\n", flags.join("; ")));
        }
    }

    format!(
        "You are the final pre-send editor for {brand}. Run each touch below through the pre-send \
test and rewrite it to pass. This is Andrew's last native-language edit: strip model-like \
phrasing, corporate mush, leaked internal labels, fabricated numbers, unsupported claims, weak \
interchangeable CTAs, and anything that only exists to show off research.\n\n\
Context you may rely on:\n\
  Hypothesis: {hypothesis}\n\
  Observed facts (the ONLY things that may be stated as fact): {facts}\n\
  Recipient vantage: {vantage} — {can_observe}\n\n\
For each touch return: stage; passes (did the ORIGINAL pass the pre-send test?); issues (specific \
problems); revised_subject; revised_body. Fix EVERY mechanical flag listed. Keep the body inside \
the {min}–{max} word band. Preserve the sequence as one unfolding investigation. If a touch is \
already clean, return it unchanged with passes=true and empty issues.\n\
{touches}",
        brand = pb.name,
        hypothesis = account.hypothesis,
        facts = account.observed_facts.join(" | "),
        vantage = contact.vantage,
        can_observe = contact.can_observe,
        min = pb.min_words,
        max = pb.max_words,
    )
}

pub fn critique_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["reviews"],
        "properties": {
            "reviews": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["stage", "passes", "issues", "revised_subject", "revised_body"],
                    "properties": {
                        "stage": { "type": "integer" },
                        "passes": { "type": "boolean" },
                        "issues": { "type": "array", "items": { "type": "string" } },
                        "revised_subject": { "type": "string" },
                        "revised_body": { "type": "string" }
                    }
                }
            }
        }
    })
}
