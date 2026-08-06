//! Outreach planning: write a sequence for each verified person and schedule its
//! touches on the timeline the cadence engine drives.
//!
//! Sourcing + enrichment give us a real, verified person and a doctrine-framed
//! lead. This turns that into an actual multi-touch sequence: Claude writes the
//! copy (grounded in the lead's real facts + the person's vantage), we run the
//! mechanical forbidden-phrase/length lint over it, then persist a `sequence`
//! plus its `touches` with `due_at` computed from each touch's day offset.
//!
//! Email touches become `scheduled` (in auto mode) or `draft` (approval mode);
//! LinkedIn/call touches are always `draft` — surfaced as manual tasks, since the
//! autonomous channel is email.

use anyhow::Result;
use chrono::{Duration, Utc};
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};

use crate::db::{Sequence, SharedDb, Touch};
use crate::domain::Sequence as CopySequence;
use crate::engine::Claude;
use crate::knowledge::Library;
use crate::playbook::{self, Playbook, Shared};

#[derive(Debug, Default)]
pub struct PlanSummary {
    pub people_planned: usize,
    pub touches_scheduled: usize,
    pub touches_drafted: usize,
}

/// Plan sequences for every verified person in `brand` who doesn't have one yet.
#[allow(clippy::too_many_arguments)]
pub async fn plan_pending(
    db: &SharedDb,
    client: &Claude,
    pb: &Playbook,
    shared: &Shared,
    library: &Library,
    n_touches: usize,
    concurrency: usize,
    auto_schedule: bool,
) -> Result<PlanSummary> {
    let system = pb.system_prompt(shared);
    let forbidden = pb.forbidden(shared);

    // Verified people with no active sequence.
    let mut todo = Vec::new();
    for p in db.list_people(Some(&pb.key), Some("verified"))? {
        if db.active_sequence_for_person(&p.id)?.is_none() {
            todo.push(p);
        }
    }
    if todo.is_empty() {
        return Ok(PlanSummary::default());
    }

    // Generate copy concurrently.
    let leads = db.list_leads(Some(&pb.key))?;
    let drafts = stream::iter(todo.into_iter().map(|person| {
        let system = system.clone();
        let lead = leads.iter().find(|l| l.id == person.lead_id).cloned();
        let knowledge = library
            .retrieve_stage(
                &format!("cold outreach copy earning a reply: {}", lead.as_ref().map(|l| l.hypothesis.clone()).unwrap_or_default()),
                "sequence",
                6,
                2,
            )
            .playbook_block();
        async move {
            let Some(lead) = lead else { return None };
            match write_sequence(client, &system, pb, &lead, &person, n_touches, &knowledge).await {
                Ok(seq) => Some((person, lead, seq)),
                Err(e) => {
                    eprintln!("  · copy failed for {}: {e:#}", person.name);
                    None
                }
            }
        }
    }))
    .buffered(concurrency)
    .filter_map(|x| async move { x })
    .collect::<Vec<_>>()
    .await;

    let mut summary = PlanSummary::default();
    let now = Utc::now();

    for (person, lead, seq) in drafts {
        let seq_id = db.create_sequence(&Sequence {
            person_id: person.id.clone(),
            lead_id: lead.id.clone(),
            brand: pb.key.clone(),
            thesis: lead.thesis.clone(),
            status: "active".into(),
            ..Default::default()
        })?;

        for t in &seq.touches {
            let is_email = t.channel.eq_ignore_ascii_case("email");
            let (min, max) = if is_email { (pb.min_words, pb.max_words) } else { (0, 0) };
            let lint = playbook::lint(&t.body, &forbidden, min, max);
            let passes = lint.forbidden_hits.is_empty() && (min == 0 || lint.length_ok);

            let status = if is_email && auto_schedule { "scheduled" } else { "draft" };
            if status == "scheduled" {
                summary.touches_scheduled += 1;
            } else {
                summary.touches_drafted += 1;
            }

            let due = now + Duration::days(t.day_offset.max(0) as i64);
            db.insert_touch(&Touch {
                sequence_id: seq_id.clone(),
                person_id: person.id.clone(),
                lead_id: lead.id.clone(),
                brand: pb.key.clone(),
                stage: t.stage as i64,
                day_offset: t.day_offset as i64,
                channel: t.channel.clone(),
                subject: t.subject.clone(),
                body: t.body.clone(),
                purpose: t.purpose.clone(),
                goal: t.goal.clone(),
                status: status.into(),
                due_at: due.to_rfc3339(),
                review_passes: Some(passes),
                review_issues: lint.forbidden_hits.clone(),
                ..Default::default()
            })?;
        }
        db.log_event(&pb.key, &person.id, "", "scheduled", &format!("{}-touch sequence", seq.touches.len()))?;
        summary.people_planned += 1;
    }

    Ok(summary)
}

async fn write_sequence(
    client: &Claude,
    system: &str,
    pb: &Playbook,
    lead: &crate::db::Lead,
    person: &crate::db::Person,
    n: usize,
    knowledge: &str,
) -> Result<CopySequence> {
    let ctx = json!({
        "company": lead.name,
        "industry": lead.industry,
        "observed_facts": lead.observed_facts,
        "hypothesis": lead.hypothesis,
        "mechanism": lead.mechanism,
        "consequence_metric": lead.consequence_metric,
        "system_concept": lead.system_concept,
        "hard_buyer_question": lead.hard_buyer_question,
        "person": {
            "name": person.name,
            "title": person.title,
            "vantage": person.vantage,
            "can_observe": person.can_observe,
            "why_them": person.why_them,
        }
    });
    let user = format!(
        "Write a {n}-touch outreach sequence to this REAL person, as an unfolding investigation that \
         earns a reply/correction/referral — never a pitch. Ground every factual claim in \
         observed_facts; frame guesses as questions. Body length {min}–{max} words for email. Vary \
         day_offset so touches space out (e.g. 0, 3, 7, 12, …). Use the person's vantage to decide \
         what to ask.\n\nCONTEXT:\n{ctx}\n\n{knowledge}\n\nSign as {sig}.",
        min = pb.min_words,
        max = pb.max_words,
        ctx = serde_json::to_string_pretty(&ctx).unwrap_or_default(),
        sig = pb.signature,
    );
    client.structured::<CopySequence>(system, &user, sequence_schema(n)).await
}

fn sequence_schema(n: usize) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["touches"],
        "properties": {
            "touches": {
                "type": "array",
                "minItems": n,
                "maxItems": n,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["stage", "day_offset", "channel", "subject", "body", "purpose", "goal"],
                    "properties": {
                        "stage": { "type": "integer" },
                        "day_offset": { "type": "integer" },
                        "channel": { "type": "string", "enum": ["email", "linkedin", "call"] },
                        "subject": { "type": "string" },
                        "body": { "type": "string" },
                        "purpose": { "type": "string" },
                        "goal": { "type": "string" }
                    }
                }
            },
            "applied_principles": { "type": "array", "items": { "type": "string" } }
        }
    })
}
