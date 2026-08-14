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
    pub selector_provenance_messages: usize,
    pub manually_approved_messages: usize,
    pub approved_distinct_accounts: usize,
    pub approved_distinct_facilities: usize,
    pub complete_wapahki_task_briefs: usize,
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

    let mut generated_accounts = HashSet::<String>::new();
    let mut selector_provenance_accounts = HashSet::<String>::new();
    let mut approved_accounts = HashSet::<String>::new();
    let mut approved_facilities = HashSet::<String>::new();
    let mut complete_wapahki_task_briefs = HashSet::<String>::new();
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
        let first_touch = touches.iter().find(|touch| touch.stage == 1);
        let Some(first_touch) = first_touch else {
            unsupported_sequences.push(format!(
                "{} / {}: generated sequence has no T1",
                lead.name, person.name
            ));
            continue;
        };
        let candidate_modes = db.list_message_candidate_audit(&sequence.id)?;
        let mode_set = candidate_modes
            .iter()
            .map(|candidate| candidate.mode.as_str())
            .collect::<HashSet<_>>();
        let selection = db.get_message_selection_audit(&sequence.id)?;
        let selected = candidate_modes.iter().find(|candidate| candidate.selected);
        let selected_hash = crate::db::touch_content_hash(&first_touch.subject, &first_touch.body);
        let provenance_valid = candidate_modes.len() == 3
            && mode_set
                == HashSet::from([
                    "operating_question",
                    "evidence_contribution",
                    "routing_question",
                ])
            && candidate_modes
                .iter()
                .filter(|candidate| candidate.selected)
                .count()
                == 1
            && selected.is_some_and(|candidate| {
                candidate.available
                    && candidate.content_hash == selected_hash
                    && candidate.subject == first_touch.subject
                    && candidate.body == first_touch.body
            })
            && selection.as_ref().is_some_and(|selection| {
                selection.selector_a_passed
                    && selection.selector_b_passed
                    && selection.selector_a_candidate_id == selection.selector_b_candidate_id
                    && selection.agreed_candidate_id == selection.selector_a_candidate_id
                    && selection.copy_policy_hash == current_copy_policy_hash()
                    && selection.selected_content_hash == selected_hash
                    && selected.is_some_and(|candidate| {
                        candidate.candidate_id == selection.agreed_candidate_id
                    })
            });
        if !provenance_valid {
            unsupported_sequences.push(format!(
                "{} / {}: missing full three-mode abstention/candidate and two-selector exact-copy provenance",
                lead.name, person.name
            ));
            continue;
        }
        selector_provenance_accounts.insert(lead.id.clone());

        if let Some(asset) = context.proof_asset.as_ref() {
            let artifact_issues = crate::db::proof_asset_gate_issues(asset);
            if !artifact_issues.is_empty() {
                unsupported_sequences.push(format!(
                    "{} / {}: superficial artifact: {}",
                    lead.name,
                    person.name,
                    artifact_issues.join("; ")
                ));
                continue;
            }
            if brand == "wapahki"
                && asset.asset_type == "wapahki_task_brief"
                && matches!(asset.status.as_str(), "prepared" | "completed")
            {
                if let Some(opportunity) = context.opportunity.as_ref() {
                    if !opportunity.facility_id.trim().is_empty() {
                        complete_wapahki_task_briefs.insert(opportunity.facility_id.clone());
                    }
                }
            }
        }
        generated_accounts.insert(lead.id.clone());
        let exact_approval = db.touch_has_current_exact_approval(&first_touch.id)?;
        if exact_approval {
            approved_accounts.insert(lead.id.clone());
            if let Some(opportunity) = context.opportunity.as_ref() {
                if !opportunity.facility_id.trim().is_empty() {
                    approved_facilities.insert(opportunity.facility_id.clone());
                }
            }
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
        generated_messages: generated_accounts.len(),
        selector_provenance_messages: selector_provenance_accounts.len(),
        manually_approved_messages: approved_accounts.len(),
        approved_distinct_accounts: approved_accounts.len(),
        approved_distinct_facilities: approved_facilities.len(),
        complete_wapahki_task_briefs: complete_wapahki_task_briefs.len(),
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
    if audit.selector_provenance_messages < minimum_messages {
        audit.blockers.push(format!(
            "need {minimum_messages} distinct-account messages with three persisted mode decisions and two agreeing selector records; found {}",
            audit.selector_provenance_messages
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
    if audit.approved_distinct_accounts < minimum_approvals {
        audit.blockers.push(format!(
            "need {minimum_approvals} exact approvals on distinct accounts; found {}",
            audit.approved_distinct_accounts
        ));
    }
    if brand == "wapahki" {
        if audit.complete_wapahki_task_briefs < minimum_accounts {
            audit.blockers.push(format!(
                "need {minimum_accounts} distinct facilities with complete Wapahki Task Briefs; found {}",
                audit.complete_wapahki_task_briefs
            ));
        }
        if audit.approved_distinct_facilities < minimum_approvals {
            audit.blockers.push(format!(
                "need {minimum_approvals} exact approvals across distinct Wapahki facilities; found {}",
                audit.approved_distinct_facilities
            ));
        }
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
