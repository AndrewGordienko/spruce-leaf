//! Read-only supervised-pilot release audit.
//!
//! This checks durable production artifacts rather than synthetic fixtures:
//! source-backed real accounts, materialized opportunities, selected contacts,
//! model-generated copy, current deterministic sendability, explicit manual
//! approval, and an allowlisted SMTP delivery to a controlled inbox.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::db::{current_copy_policy_hash, SharedDb};
use crate::playbook::Playbooks;

#[derive(Debug, Default)]
pub struct PilotAudit {
    pub researched_accounts: usize,
    pub segments: Vec<String>,
    pub generated_messages: usize,
    pub manually_approved_messages: usize,
    pub allowlisted_smtp_messages: usize,
    pub wrong_role_sequences: Vec<String>,
    pub unsupported_sequences: Vec<String>,
    pub blockers: Vec<String>,
}

impl PilotAudit {
    pub fn passed(&self) -> bool {
        self.blockers.is_empty()
    }
}

fn real_account(lead: &crate::db::Lead) -> bool {
    let domain = lead
        .domain
        .trim()
        .trim_start_matches("www.")
        .to_ascii_lowercase();
    !domain.is_empty()
        && !lead.apollo_org_id.starts_with("seed-test:")
        && !domain.ends_with(".example")
        && !domain.ends_with(".test")
        && domain != "example.com"
        && !lead.name.to_ascii_lowercase().contains("test prospect")
}

pub fn audit(
    db: &SharedDb,
    playbooks: &Playbooks,
    minimum_accounts: usize,
    minimum_segments: usize,
    minimum_messages: usize,
) -> Result<PilotAudit> {
    let pb = playbooks.get("outagehub")?;
    let leads = db.list_leads(Some("outagehub"))?;
    let lead_by_id = leads
        .iter()
        .map(|lead| (lead.id.clone(), lead))
        .collect::<HashMap<_, _>>();
    let people = db.list_people(Some("outagehub"), None)?;
    let person_by_id = people
        .iter()
        .map(|person| (person.id.clone(), person))
        .collect::<HashMap<_, _>>();
    let market_key_by_id = db
        .list_market_segments(Some("outagehub"))?
        .into_iter()
        .map(|segment| (segment.id, segment.key))
        .collect::<HashMap<_, _>>();

    let mut researched_accounts = HashSet::new();
    let mut account_segment = HashMap::<String, String>::new();
    for lead in leads.iter().filter(|lead| real_account(lead)) {
        for opportunity in db.list_sales_opportunities(Some("outagehub"), Some(&lead.id))? {
            let claims = db.list_evidence_claims(Some(&opportunity.id), Some("outagehub"))?;
            let has_source_backed_research = claims.iter().any(|claim| {
                matches!(claim.status.as_str(), "observed" | "verified")
                    && crate::db::credible_source_url(&claim.source_url)
            });
            let decision = claims
                .iter()
                .filter(|claim| {
                    claim.claim_type == "account.outage_sensitive_decision"
                        && matches!(claim.status.as_str(), "observed" | "verified")
                })
                .map(|claim| claim.source_excerpt.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            let segment_key = market_key_by_id
                .get(&opportunity.segment_id)
                .and_then(|key| crate::segments::segment_for_market_key(key))
                .map(|segment| segment.key)
                .or_else(|| {
                    crate::segments::segment_for_evidence(&decision).map(|segment| segment.key)
                });
            if has_source_backed_research {
                if let Some(segment_key) = segment_key {
                    researched_accounts.insert(lead.id.clone());
                    account_segment.insert(lead.id.clone(), segment_key.into());
                    break;
                }
            }
        }
    }

    let events = db.recent_events(Some("outagehub"), 100_000)?;
    let manually_approved_people = events
        .iter()
        .filter(|event| event.kind == "manual_approved")
        .map(|event| event.person_id.as_str())
        .collect::<HashSet<_>>();
    let allowlist_configured = std::env::var("SPRUCE_SEND_ALLOWLIST")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);

    let mut generated_messages = 0usize;
    let mut manually_approved_messages = 0usize;
    let mut allowlisted_smtp_messages = 0usize;
    let mut wrong_role_sequences = Vec::new();
    let mut unsupported_sequences = Vec::new();

    for sequence in db
        .list_sequences(Some("outagehub"))?
        .into_iter()
        .filter(|sequence| {
            sequence.status == "active"
                && sequence.copy_policy_hash == current_copy_policy_hash()
                && !sequence.generation_backend.trim().is_empty()
                && !matches!(
                    sequence
                        .generation_backend
                        .trim()
                        .to_ascii_lowercase()
                        .as_str(),
                    "manual" | "fixture" | "test"
                )
        })
    {
        let Some(lead) = lead_by_id.get(&sequence.lead_id).copied() else {
            unsupported_sequences.push(format!("{}: missing account", sequence.id));
            continue;
        };
        let Some(person) = person_by_id.get(&sequence.person_id).copied() else {
            unsupported_sequences.push(format!("{}: missing recipient", sequence.id));
            continue;
        };
        if !researched_accounts.contains(&lead.id) {
            unsupported_sequences.push(format!(
                "{} / {}: no source-backed real-account decision segment",
                lead.name, person.name
            ));
            continue;
        }
        let context = crate::gtm::prepare_action(db, "outagehub", &lead.id, person)?;
        let decision = context
            .evidence_claims
            .iter()
            .filter(|claim| {
                claim.claim_type == "account.outage_sensitive_decision"
                    && matches!(claim.status.as_str(), "observed" | "verified")
            })
            .map(|claim| claim.source_excerpt.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        if !crate::qualification::outagehub_role_matches_decision(
            &person.title,
            &person.vantage,
            &decision,
        ) {
            wrong_role_sequences.push(format!(
                "{} / {} ({})",
                lead.name, person.name, person.title
            ));
            continue;
        }
        if context
            .opportunity
            .as_ref()
            .is_none_or(|opportunity| opportunity.id != sequence.sales_opportunity_id)
        {
            unsupported_sequences.push(format!(
                "{} / {}: generated sequence is not bound to the current opportunity",
                lead.name, person.name
            ));
            continue;
        }
        let touches = db.list_touches_for_sequence(&sequence.id)?;
        let issues = crate::outreach::audit_persisted_outagehub_sequence(
            pb,
            &playbooks.shared,
            lead,
            &context,
            &touches,
        );
        if !issues.is_empty() {
            unsupported_sequences.push(format!(
                "{} / {}: {}",
                lead.name,
                person.name,
                issues.join("; ")
            ));
            continue;
        }
        generated_messages += 1;
        if manually_approved_people.contains(person.id.as_str()) {
            manually_approved_messages += 1;
        }
        if manually_approved_people.contains(person.id.as_str())
            && allowlist_configured
            && crate::send::recipient_allowed(&person.email).is_ok()
            && touches.iter().any(|touch| {
                touch.status == "sent"
                    && !touch.sent_at.trim().is_empty()
                    && !touch.message_id.trim().is_empty()
            })
        {
            allowlisted_smtp_messages += 1;
        }
    }

    let mut segments = account_segment
        .values()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    segments.sort();
    wrong_role_sequences.sort();
    wrong_role_sequences.dedup();
    unsupported_sequences.sort();
    unsupported_sequences.dedup();

    let mut audit = PilotAudit {
        researched_accounts: researched_accounts.len(),
        segments,
        generated_messages,
        manually_approved_messages,
        allowlisted_smtp_messages,
        wrong_role_sequences,
        unsupported_sequences,
        blockers: Vec::new(),
    };
    if audit.researched_accounts < minimum_accounts {
        audit.blockers.push(format!(
            "need {minimum_accounts} source-backed real accounts; found {}",
            audit.researched_accounts
        ));
    }
    if audit.segments.len() < minimum_segments {
        audit.blockers.push(format!(
            "need {minimum_segments} evidenced segments; found {} ({})",
            audit.segments.len(),
            audit.segments.join(", ")
        ));
    }
    if audit.generated_messages < minimum_messages {
        audit.blockers.push(format!(
            "need {minimum_messages} current model-generated sendable messages; found {}",
            audit.generated_messages
        ));
    }
    if !audit.wrong_role_sequences.is_empty() {
        audit.blockers.push(format!(
            "{} generated sequence(s) target the wrong role",
            audit.wrong_role_sequences.len()
        ));
    }
    if !audit.unsupported_sequences.is_empty() {
        audit.blockers.push(format!(
            "{} generated sequence(s) fail current evidence/copy policy",
            audit.unsupported_sequences.len()
        ));
    }
    if audit.manually_approved_messages < minimum_messages {
        audit.blockers.push(format!(
            "need {minimum_messages} explicitly manually approved messages; found {}",
            audit.manually_approved_messages
        ));
    }
    if audit.allowlisted_smtp_messages == 0 {
        audit.blockers.push(
            "need at least one manually approved SMTP delivery to a currently allowlisted controlled inbox"
                .into(),
        );
    }
    Ok(audit)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::audit;
    use crate::db::Db;
    use crate::playbook::Playbooks;

    #[test]
    fn empty_database_fails_every_external_pilot_threshold() {
        let db = Arc::new(Db::open(":memory:").expect("open db"));
        crate::gtm::seed_defaults(&db).expect("seed defaults");
        let playbooks = Playbooks::load("playbooks").expect("playbooks");
        let result = audit(&db, &playbooks, 20, 5, 10).expect("audit");
        assert!(!result.passed());
        assert!(result.blockers.iter().any(|blocker| blocker.contains("20")));
        assert!(result
            .blockers
            .iter()
            .any(|blocker| blocker.contains("allowlisted controlled inbox")));
    }
}
