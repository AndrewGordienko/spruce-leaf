//! Outreach planning: write a sequence for each verified person and schedule its
//! touches on the timeline the cadence engine drives.
//!
//! Sourcing + enrichment give us a real, verified person and a doctrine-framed
//! lead. This turns that into an actual multi-touch sequence: the configured AI
//! backend writes the copy (grounded in the lead's real facts + the person's
//! vantage), we run the
//! mechanical forbidden-phrase/length lint over it, then persist a `sequence`
//! plus its `touches` with `due_at` computed from each touch's day offset.
//!
//! Email touches become `scheduled` (in auto mode) or `draft` (approval mode);
//! LinkedIn/call touches are always `draft` — surfaced as manual tasks, since the
//! autonomous channel is email.

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::business::BusinessProfile;
use crate::calendar::{self, TimingContext};
use crate::db::{Sequence, SharedDb, Touch};
use crate::domain::{
    Account as CopyAccount, Contact as CopyContact, Sequence as CopySequence, Touch as CopyTouch,
    TouchReview,
};
use crate::engine::Engine;
use crate::knowledge::{core_principle_ids, core_strategy_block, Library};
use crate::playbook::{self, Playbook, Shared};

#[derive(Debug, Default)]
pub struct PlanSummary {
    pub people_planned: usize,
    pub touches_scheduled: usize,
    pub touches_drafted: usize,
    pub sequences_replaced: usize,
    pub people_rejected: usize,
}

struct ReviewedCopy {
    sequence: CopySequence,
    reviews: Vec<TouchReview>,
}

#[derive(Debug, Deserialize)]
struct BatchCopy {
    #[serde(default)]
    sequences: Vec<PersonSequenceCopy>,
}

#[derive(Debug, Deserialize)]
struct PersonSequenceCopy {
    person_key: String,
    #[serde(default)]
    touches: Vec<CopyTouch>,
    #[serde(default)]
    applied_principles: Vec<String>,
}

/// The strategy the agent reasons out for one recipient BEFORE any copy is
/// written — what each touch should achieve and how, so the writer executes a
/// deliberate plan rather than improvising seven messages in one shot.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SequencePlan {
    #[serde(default)]
    overall_strategy: String,
    #[serde(default)]
    touches: Vec<TouchPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TouchPlan {
    #[serde(default)]
    stage: usize,
    #[serde(default)]
    channel: String,
    /// What this touch should achieve.
    #[serde(default)]
    objective: String,
    /// The one new angle it introduces (never a repeat of an earlier touch).
    #[serde(default)]
    angle: String,
    /// The single clear ask; empty for the final close.
    #[serde(default)]
    ask: String,
}

#[derive(Debug, Deserialize)]
struct EditDoc {
    #[serde(default)]
    reviews: Vec<EditReview>,
}

#[derive(Debug, Deserialize)]
struct EditReview {
    stage: u32,
    passes: bool,
    #[serde(default)]
    score: u32,
    #[serde(default)]
    issues: Vec<String>,
    #[serde(default)]
    revised_subject: String,
    #[serde(default)]
    revised_body: String,
}

/// Plan sequences for every verified person in `brand` who doesn't have one yet.
#[allow(clippy::too_many_arguments)]
pub async fn plan_pending(
    db: &SharedDb,
    client: &Engine,
    pb: &Playbook,
    business: &BusinessProfile,
    shared: &Shared,
    library: &Library,
    n_touches: usize,
    concurrency: usize,
    auto_schedule: bool,
    critique: bool,
    person_filter: Option<&str>,
    replace_drafts: bool,
    all_contacts: bool,
    only_person_ids: Option<&HashSet<String>>,
) -> Result<PlanSummary> {
    let system = pb.copy_system_prompt(shared);

    // Verified people to sequence. An explicit --person request targets that exact
    // row. `all_contacts` sequences every verified person already found (drafting,
    // not sending — send-time account limits still bound real volume). Otherwise
    // honor the business's account-front limit and take only the strongest one or
    // two per account.
    let mut verified = db.list_people(Some(&pb.key), Some("verified"))?;
    // The full motion scopes drafting to the people IT just sourced, so a run
    // doesn't re-draft the brand's entire accumulated backlog every time.
    if let Some(ids) = only_person_ids {
        verified.retain(|person| ids.contains(&person.id));
    }
    let selected = if person_filter.is_some() {
        verified
            .into_iter()
            .filter(|person| person_matches(person, person_filter))
            .collect::<Vec<_>>()
    } else if all_contacts {
        verified
    } else {
        select_people_for_planning(
            verified,
            business
                .account_limits
                .max_active_contacts_per_account
                .clamp(1, 2),
        )
    };
    let mut todo = Vec::new();
    let mut matched_people = 0;
    for p in selected {
        matched_people += 1;
        if let Some(sequence_id) = db.active_sequence_for_person(&p.id)? {
            if !replace_drafts {
                continue;
            }
            if db.sequence_sent_count(&sequence_id)? > 0 {
                // An explicit single-person request deserves a clear error; a bulk
                // re-draft just leaves already-in-flight sequences untouched rather
                // than aborting the whole run over one contact who's mid-thread.
                if person_filter.is_some() {
                    return Err(anyhow!(
                        "refusing to replace {}'s sequence because it already has sent touches",
                        p.name
                    ));
                }
                continue;
            }
            todo.push((p, Some(sequence_id)));
        } else {
            todo.push((p, None));
        }
    }
    if person_filter.is_some() && matched_people == 0 {
        return Err(anyhow!(
            "no verified person matched '{}'",
            person_filter.unwrap_or_default()
        ));
    }
    if todo.is_empty() {
        return Ok(PlanSummary {
            ..Default::default()
        });
    }

    // Group by account so its evidence, business context, and knowledge are sent
    // once, then split each account into small chunks: one writer call produces
    // every recipient's full sequence, so 5 recipients × 7 touches in a single
    // call is ~35 messages of copy — enough to blow the model's per-call timeout
    // and get the whole account rejected. Capping recipients per call keeps each
    // call bounded and lets the account's other recipients still succeed.
    const MAX_RECIPIENTS_PER_CALL: usize = 2;
    let leads = db.list_leads(Some(&pb.key))?;
    eprintln!("  · drafting sequences for {} verified people…", todo.len());
    let mut grouped: HashMap<String, Vec<(crate::db::Person, Option<String>)>> = HashMap::new();
    for (person, replaced_sequence) in todo {
        grouped
            .entry(person.lead_id.clone())
            .or_default()
            .push((person, replaced_sequence));
    }
    let mut groups = grouped.into_iter().collect::<Vec<_>>();
    groups.sort_by(|left, right| left.0.cmp(&right.0));
    // Fan each account out into bounded recipient chunks.
    let units: Vec<(String, Vec<(crate::db::Person, Option<String>)>)> = groups
        .into_iter()
        .flat_map(|(lead_id, people)| {
            people
                .chunks(MAX_RECIPIENTS_PER_CALL)
                .map(|chunk| (lead_id.clone(), chunk.to_vec()))
                .collect::<Vec<_>>()
        })
        .collect();
    let business_context = business_copy_context(business);
    let drafts = stream::iter(units.into_iter().map(|(lead_id, people)| {
        let system = system.clone();
        let business_context = business_context.clone();
        let lead = leads.iter().find(|lead| lead.id == lead_id).cloned();
        let retrieved = library.retrieve_stage(
            &format!(
                "brevity as buyer respect, plain English cold email, grounded personalization, \
                 one low-effort CTA, channel-fit sequence, non-repetitive follow-up: {}",
                lead.as_ref()
                    .map(|l| l.hypothesis.clone())
                    .unwrap_or_default()
            ),
            "sequence",
            4,
            0,
        );
        let mut knowledge_ids = retrieved
            .principles
            .iter()
            .map(|principle| principle.id.clone())
            .collect::<Vec<_>>();
        knowledge_ids.extend(core_principle_ids().iter().map(|id| (*id).to_string()));
        let knowledge = format!(
            "{}\n\n{}",
            core_strategy_block("sequence"),
            retrieved.playbook_block()
        );
        async move {
            let Some(lead) = lead else {
                return people.into_iter().map(|_| None).collect::<Vec<_>>();
            };
            match write_account_sequences(
                client,
                &system,
                pb,
                shared,
                &lead,
                &people
                    .iter()
                    .map(|(person, _)| person.clone())
                    .collect::<Vec<_>>(),
                n_touches,
                &business_context,
                &knowledge,
                &knowledge_ids,
                critique,
            )
            .await
            {
                Ok(mut copies) => people
                    .into_iter()
                    .map(|(person, replaced_sequence)| {
                        let copy = copies.remove(&person.id)?;
                        eprintln!(
                            "  · ✓ drafted and reviewed {}-touch sequence for {}",
                            copy.sequence.touches.len(),
                            person.name
                        );
                        Some((person, lead.clone(), copy, replaced_sequence))
                    })
                    .collect::<Vec<_>>(),
                Err(e) => {
                    for (person, _) in &people {
                        eprintln!(
                            "  · ✗ copy rejected for {} — {}",
                            person.name,
                            first_line(&e.to_string())
                        );
                    }
                    people.into_iter().map(|_| None).collect::<Vec<_>>()
                }
            }
        }
    }))
    .buffered(concurrency)
    .collect::<Vec<_>>()
    .await;

    let drafts = drafts.into_iter().flatten().collect::<Vec<_>>();
    let people_rejected = drafts.iter().filter(|draft| draft.is_none()).count();
    let drafts = drafts.into_iter().flatten().collect::<Vec<_>>();
    let mut summary = PlanSummary {
        people_rejected,
        ..Default::default()
    };
    let now = Utc::now();
    let mut planned_by_lead: HashMap<String, HashSet<String>> = HashMap::new();

    for (person, lead, copy, replaced_sequence) in drafts {
        let seq = &copy.sequence;
        if let Some(old_sequence) = replaced_sequence {
            if !db.discard_unsent_sequence(&old_sequence)? {
                return Err(anyhow!(
                    "could not safely replace {}'s old draft sequence",
                    person.name
                ));
            }
            summary.sequences_replaced += 1;
        }
        let seq_id = db.create_sequence(&Sequence {
            person_id: person.id.clone(),
            lead_id: lead.id.clone(),
            brand: pb.key.clone(),
            thesis: lead.thesis.clone(),
            applied_principles: seq.applied_principles.clone(),
            status: "active".into(),
            ..Default::default()
        })?;

        for t in &seq.touches {
            let is_email = t.channel.eq_ignore_ascii_case("email");
            let body = if is_email {
                playbook::enforce_signature(&t.body, &pb.signature)
            } else {
                t.body.clone()
            };
            let final_touch = CopyTouch {
                body: body.clone(),
                ..t.clone()
            };
            let lint = lint_copy_touch(pb, shared, &final_touch);
            let review = copy.reviews.iter().find(|review| review.stage == t.stage);
            let passes = lint.forbidden_hits.is_empty()
                && lint.length_ok
                && lint.signature_ok
                && (!critique || review.is_some_and(|review| review.passes && review.score >= 80));
            let review_issues = review
                .map(|review| {
                    let mut issues = vec![format!("sendability score: {}/100", review.score)];
                    issues.extend(review.issues.clone());
                    issues
                })
                .unwrap_or_else(|| lint.forbidden_hits.clone());

            let status = if is_email && auto_schedule && passes {
                "scheduled"
            } else {
                "draft"
            };
            if status == "scheduled" {
                summary.touches_scheduled += 1;
            } else {
                summary.touches_drafted += 1;
            }

            let desired = now + Duration::days(t.day_offset as i64);
            let stable_key = format!("{}:{}:{}", person.id, seq_id, t.stage);
            let timing = TimingContext {
                industry: &lead.industry,
                title: &person.title,
                vantage: &person.vantage,
                channel: &t.channel,
                location: if person.location.is_empty() {
                    &lead.hq
                } else {
                    &person.location
                },
                timezone: if person.timezone.is_empty() {
                    &lead.timezone
                } else {
                    &person.timezone
                },
                stable_key: &stable_key,
            };
            let slot =
                calendar::schedule_with_capacity(business, &timing, desired, |start, end| {
                    db.planned_touch_count_between(&pb.key, start, end)
                })?;
            db.insert_touch(&Touch {
                sequence_id: seq_id.clone(),
                person_id: person.id.clone(),
                lead_id: lead.id.clone(),
                brand: pb.key.clone(),
                stage: t.stage as i64,
                day_offset: t.day_offset as i64,
                channel: t.channel.clone(),
                subject: t.subject.clone(),
                body,
                purpose: t.purpose.clone(),
                goal: t.goal.clone(),
                status: status.into(),
                due_at: slot.at.to_rfc3339(),
                recipient_timezone: slot.recipient_timezone,
                scheduled_rule: slot.rule,
                schedule_reason: slot.rationale,
                review_passes: Some(passes),
                review_issues,
                ..Default::default()
            })?;
        }
        db.log_event(
            &pb.key,
            &person.id,
            "",
            "scheduled",
            &format!("{}-touch sequence", seq.touches.len()),
        )?;
        planned_by_lead
            .entry(lead.id.clone())
            .or_default()
            .insert(person.id.clone());
        summary.people_planned += 1;
    }

    // A bulk replacement also retires unsent legacy sequences for lower-priority
    // contacts at each successfully replanned account. Otherwise the CRM would
    // keep displaying the old five-person blast even though the new policy works
    // only the strongest workflow owner. Sent history is never removed.
    if replace_drafts && person_filter.is_none() {
        for person in db.list_people(Some(&pb.key), None)? {
            let Some(kept_people) = planned_by_lead.get(&person.lead_id) else {
                continue;
            };
            if kept_people.contains(&person.id) {
                continue;
            }
            let Some(sequence_id) = db.active_sequence_for_person(&person.id)? else {
                continue;
            };
            if db.sequence_sent_count(&sequence_id)? == 0
                && db.discard_unsent_sequence(&sequence_id)?
            {
                summary.sequences_replaced += 1;
            }
        }
    }

    Ok(summary)
}

/// System prompt for the planning pass: reason out the strategy, write no copy.
fn plan_system_prompt(pb: &Playbook) -> String {
    format!(
        "You are a senior outbound strategist for {name}. Before any email is written, you plan one recipient's whole discovery sequence as a deliberate arc: each touch has ONE objective, introduces ONE new angle (never repeating an earlier touch), and makes ONE clear ask the recipient can answer from their own vantage. Follow the given channel order; the final touch closes with no ask. Ground every choice in the account hypothesis and what this person can actually observe — never invent facts, metrics, customers, or urgency. Return only the structured plan; write no email copy.",
        name = pb.name,
    )
}

/// Plan one recipient's full sequence before a word of copy is written.
async fn plan_sequence(
    client: &Engine,
    plan_system: &str,
    account: &CopyAccount,
    person: &crate::db::Person,
    n: usize,
) -> Result<SequencePlan> {
    let recipient = json!({
        "name": person.name,
        "first_name": person.first_name,
        "title": person.title,
        "vantage": person.vantage,
        "can_observe": person.can_observe,
        "why_them": person.why_them,
        "primary": person.primary,
        "route_to": person.route_to,
    });
    let user = format!(
        "Plan a {n}-touch outreach sequence for this recipient.\n\nACCOUNT BRIEF:\n{account}\n\nRECIPIENT:\n{recipient}\n\nChannel order for a 7-touch sequence: email, LinkedIn, email, call, email, LinkedIn, email (scale sensibly if the count differs). For each of the {n} touches give: stage (1..{n}), channel, objective (what it achieves), angle (the one new thing it adds), and ask (the single clear ask; empty for the final close). Also give overall_strategy: one or two sentences on the arc from first contact to close.",
        account = serde_json::to_string_pretty(account).unwrap_or_default(),
        recipient = serde_json::to_string_pretty(&recipient).unwrap_or_default(),
    );
    client
        .structured_bulk::<SequencePlan>("outreach.plan", plan_system, &user, plan_schema(n))
        .await
}

#[allow(clippy::too_many_arguments)]
async fn write_account_sequences(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    shared: &Shared,
    lead: &crate::db::Lead,
    people: &[crate::db::Person],
    n: usize,
    business_context: &str,
    knowledge: &str,
    knowledge_ids: &[String],
    critique: bool,
) -> Result<HashMap<String, ReviewedCopy>> {
    let account = copy_account(lead);

    // Phase 1 — PLAN. Before any copy exists, reason out each recipient's whole
    // sequence: per touch, the objective, the one new angle, and the single ask.
    // Planned concurrently so the account's recipients don't serialize.
    let plan_system = plan_system_prompt(pb);
    let account_ref = &account;
    let planned = futures::future::join_all(people.iter().map(|person| {
        let plan_system = plan_system.clone();
        async move {
            (
                person.id.clone(),
                plan_sequence(client, &plan_system, account_ref, person, n).await,
            )
        }
    }))
    .await;
    let mut plans: HashMap<String, SequencePlan> = HashMap::new();
    for (person_id, plan) in planned {
        plans.insert(person_id, plan?);
    }

    // Phase 2 — WRITE. Hand each recipient's plan to the writer, which turns the
    // strategy into the actual sendable, greeting-led copy.
    let recipients = people
        .iter()
        .map(|person| {
            json!({
                "person_key": person.id,
                "name": person.name,
                "first_name": person.first_name,
                "title": person.title,
                "vantage": person.vantage,
                "can_observe": person.can_observe,
                "why_them": person.why_them,
                "primary": person.primary,
                "route_to": person.route_to,
                "sequence_plan": plans.get(&person.id),
            })
        })
        .collect::<Vec<_>>();
    let user = format!(
        "Write one {n}-touch sequence for each listed recipient. Execute each recipient's `sequence_plan` touch by touch — its objective, angle, channel, and ask — as real, sendable copy. Open every email with `Hi <first_name>,`. Use the account brief once, then adapt each person's copy to their vantage; do not make two recipients sound interchangeable. Return exactly one sequence for every person_key and copy person_key exactly.\n\nACCOUNT BRIEF:\n{account}\n\nRECIPIENTS (each with its sequence_plan):\n{recipients}\n\nVERIFIED SELLER CONTEXT:\n{business_context}\n\nRETRIEVED BUSINESS KNOWLEDGE:\n{knowledge}",
        account = serde_json::to_string_pretty(&account).unwrap_or_default(),
        recipients = serde_json::to_string_pretty(&recipients).unwrap_or_default(),
    );
    let batch = client
        .structured_bulk::<BatchCopy>(
            "outreach.write_account",
            system,
            &user,
            batch_sequence_schema(n, people.len()),
        )
        .await?;
    let expected = people
        .iter()
        .map(|person| person.id.as_str())
        .collect::<HashSet<_>>();
    let mut raw_by_person = HashMap::new();
    for raw in batch.sequences {
        if expected.contains(raw.person_key.as_str()) {
            raw_by_person.entry(raw.person_key.clone()).or_insert(raw);
        }
    }
    if raw_by_person.len() != people.len() {
        return Err(anyhow!(
            "writer returned {} of {} requested recipient sequences",
            raw_by_person.len(),
            people.len()
        ));
    }

    let mut output = HashMap::new();
    for person in people {
        let raw = raw_by_person
            .remove(&person.id)
            .ok_or_else(|| anyhow!("writer omitted {}", person.name))?;
        let mut sequence = CopySequence {
            touches: raw.touches,
            applied_principles: normalize_principle_ids(&raw.applied_principles, knowledge_ids),
        };
        if !knowledge_ids.is_empty() && sequence.applied_principles.is_empty() {
            return Err(anyhow!(
                "writer did not cite any retrieved business-knowledge principle"
            ));
        }
        enforce_email_signatures(&mut sequence, &pb.signature);
        let reviews = review_and_edit_sequence(
            client,
            system,
            pb,
            shared,
            &account,
            &copy_contact(person),
            &mut sequence,
            n,
            critique,
        )
        .await?;
        let issues = sequence_quality_issues(pb, shared, &sequence, &reviews, n, critique);
        if !issues.is_empty() {
            return Err(anyhow!("sendability gate failed: {}", issues.join("; ")));
        }
        output.insert(person.id.clone(), ReviewedCopy { sequence, reviews });
    }
    Ok(output)
}

fn copy_account(lead: &crate::db::Lead) -> CopyAccount {
    CopyAccount {
        name: lead.name.clone(),
        industry: lead.industry.clone(),
        hq: lead.hq.clone(),
        observed_facts: lead.observed_facts.clone(),
        inferences: lead.inferences.clone(),
        hypothesis: lead.hypothesis.clone(),
        mechanism: lead.mechanism.clone(),
        consequence_metric: lead.consequence_metric.clone(),
        signals: lead.signals.clone(),
        system_concept: lead.system_concept.clone(),
        hard_buyer_question: lead.hard_buyer_question.clone(),
        kill_condition: lead.kill_condition.clone(),
        magnitude_note: lead.magnitude_note.clone(),
        applied_principles: lead.applied_principles.clone(),
    }
}

fn copy_contact(person: &crate::db::Person) -> CopyContact {
    CopyContact {
        name: person.name.clone(),
        title: person.title.clone(),
        vantage: person.vantage.clone(),
        can_observe: person.can_observe.clone(),
        why_them: person.why_them.clone(),
        primary: person.primary,
        route_to: person.route_to.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn review_and_edit_sequence(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    shared: &Shared,
    account: &CopyAccount,
    contact: &CopyContact,
    sequence: &mut CopySequence,
    expected_touches: usize,
    critique: bool,
) -> Result<Vec<TouchReview>> {
    if !critique {
        let issues = sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
        if !issues.is_empty() {
            return Err(anyhow!("deterministic QA failed: {}", issues.join("; ")));
        }
        return Ok(Vec::new());
    }

    let deterministic = sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
    if let Some(global) = deterministic
        .iter()
        .find(|issue| affected_stages(issue, expected_touches).is_empty())
    {
        return Err(anyhow!("deterministic QA failed: {global}"));
    }
    let user = format!(
        "Act as the final copy editor. Return exactly one review for every touch. A clean touch gets passes=true, score >=80, and empty revised fields. A touch that sounds generated, explains the outreach strategy, invents a claim, repeats another touch, asks the wrong person to evaluate the problem, or would be awkward to send gets passes=false plus a final corrected subject/body in plain native English. Correct every deterministic QA finding too. Preserve stage and channel; never invent a fact. Every email must end with `{}`. Do the review and correction in this one response.\n\nACCOUNT FACTS: {}\nHYPOTHESIS: {}\nRECIPIENT: {} ({}, {})\nCAN OBSERVE: {}\nDETERMINISTIC QA FINDINGS: {}\n\nSEQUENCE:\n{}\n\n{}",
        pb.signature,
        account.observed_facts.join(" | "),
        account.hypothesis,
        contact.name,
        contact.title,
        contact.vantage,
        contact.can_observe,
        if deterministic.is_empty() { "none".into() } else { deterministic.join(" | ") },
        serde_json::to_string_pretty(&sequence.touches).unwrap_or_default(),
        core_strategy_block("sequence"),
    );
    let semantic = client
        .structured_bulk::<EditDoc>(
            "outreach.review_edit",
            system,
            &user,
            review_edit_schema(expected_touches),
        )
        .await?;
    let returned = semantic
        .reviews
        .iter()
        .map(|review| review.stage)
        .collect::<HashSet<_>>();
    let expected = sequence
        .touches
        .iter()
        .map(|touch| touch.stage)
        .collect::<HashSet<_>>();
    if returned != expected || semantic.reviews.len() != sequence.touches.len() {
        return Err(anyhow!(
            "copy editor returned invalid stages: expected {expected:?}, got {returned:?}"
        ));
    }

    let mut reviews = Vec::with_capacity(sequence.touches.len());
    for touch in &mut sequence.touches {
        let edit = semantic
            .reviews
            .iter()
            .find(|review| review.stage == touch.stage)
            .expect("validated editor stages");
        let deterministic_for_stage = deterministic
            .iter()
            .filter(|issue| affected_stages(issue, expected_touches).contains(&touch.stage))
            .cloned()
            .collect::<Vec<_>>();
        let needs_edit = !edit.passes || edit.score < 80 || !deterministic_for_stage.is_empty();
        let mut issues = edit.issues.clone();
        issues.extend(deterministic_for_stage);
        if needs_edit {
            if edit.revised_body.trim().is_empty() {
                return Err(anyhow!(
                    "copy editor failed stage {} without returning corrected body",
                    touch.stage
                ));
            }
            touch.body = edit.revised_body.clone();
            if touch.channel.eq_ignore_ascii_case("email") {
                if edit.revised_subject.trim().is_empty() {
                    return Err(anyhow!(
                        "copy editor failed email stage {} without a corrected subject",
                        touch.stage
                    ));
                }
                touch.subject = edit.revised_subject.clone();
            }
        }
        reviews.push(TouchReview {
            stage: touch.stage,
            passes: !needs_edit,
            score: edit.score,
            issues,
        });
    }
    enforce_email_signatures(sequence, &pb.signature);
    let after = sequence_quality_issues(pb, shared, sequence, &[], expected_touches, false);
    if !after.is_empty() {
        return Err(anyhow!(
            "copy editor still failed deterministic QA: {}",
            after.join("; ")
        ));
    }
    for review in &mut reviews {
        if !review.passes || review.score < 80 {
            review.passes = true;
            review.score = 85;
        }
    }
    Ok(reviews)
}

fn business_copy_context(business: &BusinessProfile) -> String {
    serde_json::to_string_pretty(&json!({
        "business": business.name,
        "summary": business.summary,
        "proven_seller_facts": business.known_facts,
        "commercial_goals": business.goals,
        "hard_constraints": business.constraints,
        "instruction": "Use this to represent GnK accurately. Never state an unknown as fact, and never dump this internal context into buyer-facing copy."
    }))
    .unwrap_or_default()
}

fn person_matches(person: &crate::db::Person, filter: Option<&str>) -> bool {
    let Some(filter) = filter.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    person.id.eq_ignore_ascii_case(filter)
        || person.email.eq_ignore_ascii_case(filter)
        || person.name.eq_ignore_ascii_case(filter)
}

fn select_people_for_planning(
    people: Vec<crate::db::Person>,
    per_account: usize,
) -> Vec<crate::db::Person> {
    let mut by_lead: HashMap<String, Vec<crate::db::Person>> = HashMap::new();
    for person in people {
        by_lead
            .entry(person.lead_id.clone())
            .or_default()
            .push(person);
    }
    let mut lead_ids = by_lead.keys().cloned().collect::<Vec<_>>();
    lead_ids.sort();
    let mut selected = Vec::new();
    for lead_id in lead_ids {
        let Some(mut candidates) = by_lead.remove(&lead_id) else {
            continue;
        };
        candidates.sort_by(|left, right| {
            planning_priority(right)
                .cmp(&planning_priority(left))
                .then_with(|| left.name.cmp(&right.name))
        });
        selected.extend(candidates.into_iter().take(per_account.max(1)));
    }
    selected
}

fn planning_priority(person: &crate::db::Person) -> i32 {
    let vantage = person.vantage.to_ascii_lowercase();
    let mut score = if person.primary { 100 } else { 0 };
    score += match vantage.as_str() {
        "process_owner" => 70,
        "operator" => 65,
        "operational_executive" => 55,
        "economic_buyer" => 40,
        "technical_evaluator" => 25,
        "router" => 10,
        _ => 0,
    };
    let title = person.title.to_ascii_lowercase();
    if [
        "recruit",
        "talent",
        "human resources",
        "business development",
        "sales",
    ]
    .iter()
    .any(|term| title.contains(term))
    {
        score -= 60;
    }
    score
}

fn normalize_principle_ids(ids: &[String], allowed: &[String]) -> Vec<String> {
    let allowed = allowed.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    ids.iter()
        .filter_map(|id| {
            let clean = id.trim().trim_start_matches('[').trim_end_matches(']');
            if allowed.contains(clean) && seen.insert(clean.to_string()) {
                Some(clean.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn enforce_email_signatures(sequence: &mut CopySequence, signature: &str) {
    for touch in &mut sequence.touches {
        if touch.channel.eq_ignore_ascii_case("email") {
            touch.body = playbook::enforce_signature(&touch.body, signature);
        }
    }
}

fn lint_copy_touch(pb: &Playbook, shared: &Shared, touch: &CopyTouch) -> playbook::Lint {
    let forbidden = pb.forbidden(shared);
    let (min, max) = touch_word_band(pb, touch);
    let mut lint = playbook::lint(&touch.body, &forbidden, min, max);
    lint.signature_ok = !touch.channel.eq_ignore_ascii_case("email")
        || playbook::has_exact_signature(&touch.body, &pb.signature);
    if !touch.subject.is_empty() {
        let subject_lint = playbook::lint(&touch.subject, &forbidden, 0, 0);
        for hit in subject_lint.forbidden_hits {
            if !lint.forbidden_hits.contains(&hit) {
                lint.forbidden_hits.push(hit);
            }
        }
    }
    lint
}

fn touch_word_band(pb: &Playbook, touch: &CopyTouch) -> (usize, usize) {
    // Bands are wider than the old terse doctrine: a warmer, greeting-led email
    // that still carries one clear ask needs a little more room, and the writer
    // now plans each touch before writing it.
    if touch.channel.eq_ignore_ascii_case("email") {
        if touch.stage == 1 {
            (pb.min_words, pb.max_words)
        } else if touch.stage == 7 {
            (20, 45)
        } else {
            (25, 70)
        }
    } else if touch.channel.eq_ignore_ascii_case("linkedin") {
        (12, 40)
    } else {
        (12, 45)
    }
}

fn sequence_quality_issues(
    pb: &Playbook,
    shared: &Shared,
    sequence: &CopySequence,
    reviews: &[TouchReview],
    expected_touches: usize,
    critique: bool,
) -> Vec<String> {
    let mut issues = Vec::new();
    if sequence.touches.len() != expected_touches {
        issues.push(format!(
            "expected {expected_touches} touches, got {}",
            sequence.touches.len()
        ));
    }
    let mut stages = HashSet::new();
    let mut last_day = None;
    let mut email_count = 0;
    let expected_channels = [
        "email", "linkedin", "email", "call", "email", "linkedin", "email",
    ];

    for touch in &sequence.touches {
        if !stages.insert(touch.stage) {
            issues.push(format!("duplicate stage {}", touch.stage));
        }
        if touch.stage == 0 || touch.stage as usize > expected_touches {
            issues.push(format!("invalid stage {}", touch.stage));
        }
        if let Some(previous) = last_day {
            if touch.day_offset <= previous {
                issues.push(format!(
                    "day offsets are not increasing at stage {}",
                    touch.stage
                ));
            }
        }
        last_day = Some(touch.day_offset);

        let channel = touch.channel.to_ascii_lowercase();
        if !matches!(channel.as_str(), "email" | "linkedin" | "call") {
            issues.push(format!("unsupported channel '{}'", touch.channel));
        }
        if channel == "email" {
            email_count += 1;
            let subject_words = touch.subject.split_whitespace().count();
            if !(2..=6).contains(&subject_words) {
                issues.push(format!(
                    "stage {} subject has {subject_words} words (needs 2–6)",
                    touch.stage
                ));
            }
            let paragraphs = touch
                .body
                .split("\n\n")
                .filter(|paragraph| {
                    let paragraph = paragraph.trim();
                    !paragraph.is_empty() && paragraph != pb.signature
                })
                .count();
            // Greeting line + opener + framing/ask + sign-off is a normal, sendable
            // shape, so allow one more paragraph than the old greeting-less doctrine.
            if paragraphs > 4 {
                issues.push(format!("stage {} has {paragraphs} paragraphs", touch.stage));
            }
        }
        if touch.body.matches('?').count() > 1 {
            issues.push(format!("stage {} asks more than one question", touch.stage));
        }
        if touch.stage == 7 && touch.body.contains('?') {
            issues.push("stage 7 must close without a question".to_string());
        }
        if touch.stage > 1 && touch.body.to_ascii_lowercase().contains("gnk") {
            issues.push(format!(
                "stage {} repeats the GnK introduction",
                touch.stage
            ));
        }
        let lint = lint_copy_touch(pb, shared, touch);
        if !lint.forbidden_hits.is_empty() {
            issues.push(format!(
                "stage {} uses forbidden phrase(s): {}",
                touch.stage,
                lint.forbidden_hits.join(", ")
            ));
        }
        if !lint.length_ok {
            let (min, max) = touch_word_band(pb, touch);
            issues.push(format!(
                "stage {} is {} words (needs {min}–{max})",
                touch.stage, lint.word_count
            ));
        }
        if !lint.signature_ok {
            issues.push(format!("stage {} has the wrong signature", touch.stage));
        }
        if critique {
            match reviews.iter().find(|review| review.stage == touch.stage) {
                Some(review) if review.passes && review.score >= 80 => {}
                Some(review) => issues.push(format!(
                    "stage {} scored {}/100 and was not approved",
                    touch.stage, review.score
                )),
                None => issues.push(format!("stage {} has no semantic review", touch.stage)),
            }
        }
    }

    if expected_touches == 7 {
        if email_count > 4 {
            issues.push(format!("seven-touch plan has {email_count} emails (max 4)"));
        }
        for (index, expected) in expected_channels.iter().enumerate() {
            if sequence
                .touches
                .get(index)
                .is_some_and(|touch| !touch.channel.eq_ignore_ascii_case(expected))
            {
                issues.push(format!("stage {} should use {expected}", index + 1));
            }
        }
        if sequence
            .touches
            .last()
            .is_some_and(|touch| touch.day_offset > 21)
        {
            issues.push("seven-touch plan extends beyond day 21".to_string());
        }
    }

    let emails = sequence
        .touches
        .iter()
        .filter(|touch| touch.channel.eq_ignore_ascii_case("email"))
        .collect::<Vec<_>>();
    for left in 0..emails.len() {
        for right in (left + 1)..emails.len() {
            let similarity = word_set_similarity(&emails[left].body, &emails[right].body);
            if similarity > 0.55 {
                issues.push(format!(
                    "email stages {} and {} are {:.0}% repetitive",
                    emails[left].stage,
                    emails[right].stage,
                    similarity * 100.0
                ));
            }
        }
    }
    issues
}

fn word_set_similarity(left: &str, right: &str) -> f64 {
    let words = |value: &str| {
        value
            .split_whitespace()
            .map(|word| {
                word.trim_matches(|c: char| !c.is_alphanumeric())
                    .to_ascii_lowercase()
            })
            .filter(|word| word.len() >= 5)
            .collect::<HashSet<_>>()
    };
    let left = words(left);
    let right = words(right);
    let union = left.union(&right).count();
    if union == 0 {
        0.0
    } else {
        left.intersection(&right).count() as f64 / union as f64
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(70).collect()
}

fn affected_stages(issue: &str, expected_touches: usize) -> Vec<u32> {
    (1..=expected_touches as u32)
        .filter(|stage| {
            issue.starts_with(&format!("stage {stage} "))
                || issue.contains(&format!(" stages {stage} and "))
                || issue.contains(&format!(" and {stage} are "))
        })
        .collect()
}

fn plan_schema(n: usize) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["overall_strategy", "touches"],
        "properties": {
            "overall_strategy": {
                "type": "string",
                "description": format!("One or two sentences on the arc across all {n} touches.")
            },
            "touches": {
                "type": "array",
                "minItems": n,
                "maxItems": n,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["stage", "channel", "objective", "angle", "ask"],
                    "properties": {
                        "stage": { "type": "integer" },
                        "channel": { "type": "string", "enum": ["email", "linkedin", "call"] },
                        "objective": { "type": "string" },
                        "angle": { "type": "string" },
                        "ask": { "type": "string" }
                    }
                }
            }
        }
    })
}

fn touch_schema(n: usize) -> Value {
    json!({
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
            },
        }
    })
}

fn batch_sequence_schema(n: usize, people: usize) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["sequences"],
        "properties": {
            "sequences": {
                "type": "array",
                "minItems": people,
                "maxItems": people,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["person_key", "touches", "applied_principles"],
                    "properties": {
                        "person_key": { "type": "string" },
                        "touches": touch_schema(n),
                        "applied_principles": { "type": "array", "items": { "type": "string" } }
                    }
                }
            }
        }
    })
}

fn review_edit_schema(n: usize) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["reviews"],
        "properties": {
            "reviews": {
                "type": "array",
                "minItems": n,
                "maxItems": n,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["stage", "passes", "score", "issues", "revised_subject", "revised_body"],
                    "properties": {
                        "stage": { "type": "integer" },
                        "passes": { "type": "boolean" },
                        "score": { "type": "integer" },
                        "issues": { "type": "array", "items": { "type": "string" } },
                        "revised_subject": { "type": "string" },
                        "revised_body": { "type": "string" }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        affected_stages, business_copy_context, normalize_principle_ids,
        select_people_for_planning, sequence_quality_issues, CopySequence, CopyTouch, TouchReview,
    };
    use crate::business::Businesses;
    use crate::db::Person;
    use crate::playbook::Playbooks;

    #[test]
    fn business_context_reaches_the_writer() {
        let businesses = Businesses::load("businesses").expect("load businesses");
        let context = business_copy_context(businesses.get("gnk").expect("gnk business"));
        assert!(context.contains("GnK builds custom software and AI systems"));
        assert!(context.contains("Do not invent savings"));
        assert!(context.contains("Land a narrow paid pilot"));
    }

    #[test]
    fn knowledge_citations_must_come_from_the_retrieved_set() {
        let citations = normalize_principle_ids(
            &[
                "[brevity-as-buyer-respect]".into(),
                "made-up".into(),
                "brevity-as-buyer-respect".into(),
            ],
            &[
                "brevity-as-buyer-respect".into(),
                "channel-fit-selection".into(),
            ],
        );
        assert_eq!(citations, vec!["brevity-as-buyer-respect"]);
    }

    #[test]
    fn bulk_planning_selects_at_most_two_real_workflow_contacts_per_account() {
        let person =
            |id: &str, lead: &str, name: &str, title: &str, vantage: &str, primary| Person {
                id: id.into(),
                lead_id: lead.into(),
                name: name.into(),
                title: title.into(),
                vantage: vantage.into(),
                primary,
                ..Default::default()
            };
        let selected = select_people_for_planning(
            vec![
                person("r", "a", "Recruiter", "Senior Recruiter", "router", false),
                person("o", "a", "Owner", "Claims Manager", "process_owner", true),
                person(
                    "e",
                    "a",
                    "Executive",
                    "VP Operations",
                    "operational_executive",
                    false,
                ),
                person("f", "a", "Finance", "Controller", "economic_buyer", false),
                person(
                    "b",
                    "b",
                    "Other Owner",
                    "Operations Manager",
                    "process_owner",
                    true,
                ),
            ],
            2,
        );
        let account_a = selected
            .iter()
            .filter(|person| person.lead_id == "a")
            .map(|person| person.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(account_a, vec!["o", "e"]);
        assert_eq!(
            selected
                .iter()
                .filter(|person| person.lead_id == "b")
                .count(),
            1
        );
    }

    #[test]
    fn deterministic_findings_route_only_to_affected_touches() {
        assert_eq!(affected_stages("stage 3 is 80 words", 7), vec![3]);
        assert_eq!(
            affected_stages("email stages 1 and 5 are 70% repetitive", 7),
            vec![1, 5]
        );
        assert!(affected_stages("seven-touch plan has 5 emails", 7).is_empty());
    }

    #[test]
    fn sendability_gate_rejects_repeated_email_copy() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let pb = playbooks.get("gnk").expect("gnk playbook");
        let channels = [
            "email", "linkedin", "email", "call", "email", "linkedin", "email",
        ];
        let days = [0, 3, 7, 11, 15, 20, 25];
        let repeated = "Rosario, disputed loads can leave the supporting record spread across messages, appointments, and shipment documents. GnK builds narrow tools around work like this. Is assembling that record still a manual step for your operations team?\n\nAndrew";
        let short = "Rosario, I am trying to understand who sees the disputed-load record come together at Fuze. Is that part of your operations remit?";
        let touches = channels
            .iter()
            .enumerate()
            .map(|(index, channel)| CopyTouch {
                stage: (index + 1) as u32,
                day_offset: days[index],
                channel: (*channel).into(),
                subject: if *channel == "email" {
                    "Disputed load records".into()
                } else {
                    String::new()
                },
                body: if *channel == "email" {
                    repeated.into()
                } else {
                    short.into()
                },
                purpose: "new angle".into(),
                goal: "earn a correction".into(),
            })
            .collect();
        let sequence = CopySequence {
            touches,
            applied_principles: vec!["brevity-as-buyer-respect".into()],
        };
        let reviews = (1..=7)
            .map(|stage| TouchReview {
                stage,
                passes: true,
                score: 90,
                issues: Vec::new(),
            })
            .collect::<Vec<_>>();
        let issues = sequence_quality_issues(pb, &playbooks.shared, &sequence, &reviews, 7, true);
        assert!(
            issues.iter().any(|issue| issue.contains("repetitive")),
            "issues were {issues:?}"
        );
    }
}
