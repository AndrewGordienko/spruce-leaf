//! Prompts + JSON schemas for each pipeline stage.
//!
//! The heavy doctrine lives in the brand's system prompt (built in `playbook.rs`
//! from the editable `playbooks/*.toml`). Here we build the per-stage USER
//! prompt and its structured-output schema. Counts are enforced via prompt text;
//! schemas avoid `minItems`/`maxItems` since the raw API rejects most array/
//! numeric constraints.

use serde_json::{json, Value};

use crate::domain::{Account, Contact};
use crate::playbook::Playbook;

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
be expensive, framed as something to test. Name a physical object, station, machine, alarm, or \
private handoff only when an observed fact names it; otherwise use the supported task category and \
make discovery identify the actual work.\n\
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
First identify the ONE response needed next: a concrete workflow example, confirmation that the problem recurs, a technical boundary, an economic decision, or an internal route. Then choose people by their ability to provide that response, not seniority or generic title prestige. A problem witness, process owner, economic buyer, evaluator, and router are different jobs in the outreach sequence. Do not pitch a witness as though they own budget, and do not ask a buyer to reconstruct an operator's daily work. Interns, students, trainees, and apprentices are route-only and can never be primary. A department such as Sales is neither automatically relevant nor irrelevant; it must match the named problem. For each person, set:\n\
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

pub fn sequence_user(
    pb: &Playbook,
    account: &Account,
    contact: &Contact,
    n: usize,
    knowledge: &str,
) -> String {
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
angle — not restatements. Match the channel and ask to {name}'s vantage. Use email and LinkedIn only. \
For each touch set: day_offset from the start; channel; subject (empty string for a connection request); the \
body copy; purpose (which move this is — observation / diagnostic / consequence / artifact / \
hard-question / routing / close); and goal.\n\n\
If writing four touches, use these exact channel/day pairs: email/0, email/3, \
linkedin_request/7, email/14. If writing seven touches, use: email/0, email/3, \
linkedin_request/5, email/9, linkedin_or_email/13, email/17, linkedin_or_email/21. \
The connection request is 8–24 words with no pitch or meeting ask. Conditional touches are short \
LinkedIn DMs when connected and email fallbacks otherwise. Finish by day 21. Touch 1 email is \
{min}–{max} words including the greeting and signature; later emails are shorter but substantive; touch 7 has no question. \
Mention {brand} only in touch 1. Use at most one central operating question, one CTA, one verified account fact, and one short explanation of {brand} per touch. \
Touch one may use a related diagnostic question plus a short CTA. Privately consider five subject lines, then choose 3–9 plain operational words that name the recognizable event, decision, object, or consequence. Reject generic category labels such as `utility status`, `power alarms`, `claim evidence`, `decision trail`, or `automation question`; accurate but invisible is not good enough. Use no numbers or question marks; sentence or title case is fine. Do not use fake familiarity, explain your strategy, dump the \
research, or assert limitations of the recipient's tools.\n\n\
Follow the length band, the voice, the forbidden list, and the pre-send test from the doctrine. \
Center the operating decision, not {brand}. In four touches, T2 sharpens the mechanism, T3 is a \
natural connection request, and T4 contributes a useful distinction or answers the hard buyer \
question before closing. In the seven-touch sequence, T4 contributes value, T5 answers the hard buyer \
question, T6 routes once, and T7 closes. Keep the whole sequence on ONE problem thread. No more \
than three touches may primarily retreat, request correction/referral, or close.",
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
        min = pb.min_words,
        max = pb.max_words,
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
