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

#[derive(Debug, Clone, Copy)]
pub struct PilotThresholds {
    pub accounts: usize,
    pub segments: usize,
    pub generated_messages: usize,
    pub exact_approvals: usize,
}

impl PilotThresholds {
    pub fn for_brand(brand: &str) -> Self {
        if brand.eq_ignore_ascii_case("gnk") {
            Self {
                accounts: 30,
                segments: 3,
                generated_messages: 30,
                exact_approvals: 24,
            }
        } else if brand.eq_ignore_ascii_case("wapahki") {
            Self {
                accounts: 10,
                segments: 3,
                generated_messages: 10,
                exact_approvals: 10,
            }
        } else {
            Self {
                accounts: 20,
                segments: 5,
                generated_messages: 20,
                exact_approvals: 20,
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct PilotAudit {
    pub researched_accounts: usize,
    pub segments: Vec<String>,
    pub generated_messages: usize,
    pub manually_approved_messages: usize,
    pub allowlisted_smtp_messages: usize,
    pub casl_program_approval_recorded: bool,
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
    brand: &str,
    minimum_accounts: usize,
    minimum_segments: usize,
    minimum_messages: usize,
    minimum_approvals: usize,
) -> Result<PilotAudit> {
    let brand = brand.trim().to_ascii_lowercase();
    let pb = playbooks.get(&brand)?;
    let leads = db.list_leads(Some(&brand))?;
    let lead_by_id = leads
        .iter()
        .map(|lead| (lead.id.clone(), lead))
        .collect::<HashMap<_, _>>();
    let people = db.list_people(Some(&brand), None)?;
    let person_by_id = people
        .iter()
        .map(|person| (person.id.clone(), person))
        .collect::<HashMap<_, _>>();
    let market_key_by_id = db
        .list_market_segments(Some(&brand))?
        .into_iter()
        .map(|segment| (segment.id, segment.key))
        .collect::<HashMap<_, _>>();

    let mut researched_accounts = HashSet::new();
    let mut account_segment = HashMap::<String, String>::new();
    for lead in leads.iter().filter(|lead| real_account(lead)) {
        for opportunity in db.list_sales_opportunities(Some(&brand), Some(&lead.id))? {
            let claims = db.list_evidence_claims(Some(&opportunity.id), Some(&brand))?;
            let has_source_backed_research = claims.iter().any(|claim| {
                matches!(claim.status.as_str(), "observed" | "verified")
                    && crate::db::credible_source_url(&claim.source_url)
            });
            let decision = claims
                .iter()
                .filter(|claim| matches!(claim.status.as_str(), "observed" | "verified"))
                .map(|claim| claim.source_excerpt.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            let segment_key = if brand == "outagehub" {
                market_key_by_id
                    .get(&opportunity.segment_id)
                    .and_then(|key| crate::segments::segment_for_market_key(key))
                    .map(|segment| segment.key.to_string())
                    .or_else(|| {
                        crate::segments::segment_for_evidence(&decision)
                            .map(|segment| segment.key.to_string())
                    })
            } else {
                market_key_by_id
                    .get(&opportunity.segment_id)
                    .cloned()
                    .or_else(|| {
                        (!opportunity.priority_lane.trim().is_empty())
                            .then(|| opportunity.priority_lane.clone())
                    })
                    .or_else(|| {
                        (!opportunity.task_key.trim().is_empty())
                            .then(|| opportunity.task_key.clone())
                    })
            };
            if has_source_backed_research {
                researched_accounts.insert(lead.id.clone());
                if let Some(segment_key) = segment_key {
                    account_segment.insert(lead.id.clone(), segment_key);
                }
                break;
            }
        }
    }
    let allowlist_configured = std::env::var("SPRUCE_SEND_ALLOWLIST")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    let casl_program_approval_recorded = std::env::var("SPRUCE_CASL_PROGRAM_APPROVAL_REF")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);

    let mut generated_messages = 0usize;
    let mut manually_approved_messages = 0usize;
    let mut allowlisted_smtp_messages = 0usize;
    let mut wrong_role_sequences = Vec::new();
    let mut unsupported_sequences = Vec::new();

    for sequence in db
        .list_sequences(Some(&brand))?
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
        let context = match crate::gtm::prepare_action(db, &brand, &lead.id, person) {
            Ok(context) => context,
            Err(error) => {
                unsupported_sequences.push(format!(
                    "{} / {}: current action context failed: {error:#}",
                    lead.name, person.name
                ));
                continue;
            }
        };
        let stakeholder = context
            .stakeholders
            .iter()
            .find(|stakeholder| stakeholder.person_id == person.id && stakeholder.status != "held");
        let adjacent_discovery =
            matches!(brand.as_str(), "wapahki" | "outagehub") && context.state == "discovery_ready";
        let role_supported = stakeholder.is_some_and(|stakeholder| {
            (stakeholder.role_fit == "direct" && !stakeholder.evidence_claim_ids.is_empty())
                || adjacent_discovery
        });
        let outage_role_supported = if brand == "outagehub" {
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
            crate::qualification::outagehub_role_matches_decision(
                &person.title,
                &person.vantage,
                &decision,
            )
        } else {
            true
        };
        if !role_supported || !outage_role_supported {
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
        let issues = crate::outreach::audit_persisted_sequence(
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
        let first_touch = touches.iter().find(|touch| touch.stage == 1);
        let exact_approval = first_touch
            .map(|touch| db.touch_has_current_exact_approval(&touch.id))
            .transpose()?
            .unwrap_or(false);
        if exact_approval {
            manually_approved_messages += 1;
        }
        if exact_approval
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
        casl_program_approval_recorded,
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
    if audit.manually_approved_messages < minimum_approvals {
        audit.blockers.push(format!(
            "need {minimum_approvals} exact-copy manual approvals bound to sequence, touch, policy, and content hash; found {}",
            audit.manually_approved_messages
        ));
    }
    if audit.allowlisted_smtp_messages == 0 {
        audit.blockers.push(
            "need at least one manually approved SMTP delivery to a currently allowlisted controlled inbox"
                .into(),
        );
    }
    if !audit.casl_program_approval_recorded {
        audit.blockers.push(
            "need a Canadian-counsel program approval reference in SPRUCE_CASL_PROGRAM_APPROVAL_REF before prospect launch"
                .into(),
        );
    }
    Ok(audit)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{audit, PilotThresholds};
    use crate::db::Db;
    use crate::playbook::Playbooks;

    #[test]
    fn empty_database_fails_every_external_pilot_threshold() {
        let db = Arc::new(Db::open(":memory:").expect("open db"));
        crate::gtm::seed_defaults(&db).expect("seed defaults");
        let playbooks = Playbooks::load("playbooks").expect("playbooks");
        let thresholds = PilotThresholds::for_brand("outagehub");
        let result = audit(
            &db,
            &playbooks,
            "outagehub",
            thresholds.accounts,
            thresholds.segments,
            thresholds.generated_messages,
            thresholds.exact_approvals,
        )
        .expect("audit");
        assert!(!result.passed());
        assert!(result.blockers.iter().any(|blocker| blocker.contains("20")));
        assert!(result
            .blockers
            .iter()
            .any(|blocker| blocker.contains("allowlisted controlled inbox")));
    }
}
