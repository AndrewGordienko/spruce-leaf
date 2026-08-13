//! OutageHub end-to-end evaluation corpus (test-only module).
//!
//! Every case runs the REAL production predicates — qualification signals,
//! segment classification, role gating, touch authorization, commercial lanes,
//! and the deterministic copy linter — against a fixture that encodes one of
//! the old system's observed failure modes. A regression here means outreach
//! quality regressed, not that a string changed.

use serde::Deserialize;

use crate::domain::{Sequence as CopySequence, Touch as CopyTouch};
use crate::gtm::GtmActionContext;
use crate::playbook::Playbooks;
use crate::priority::{outagehub_priority, PriorityInputs};
use crate::qualification::{credible_outagehub_signal, outagehub_role_matches_decision};
use crate::segments::segment_for_evidence;

#[derive(Debug, Deserialize)]
struct Corpus {
    account_cases: Vec<AccountCase>,
    copy_cases: Vec<CopyCase>,
}

#[derive(Debug, Deserialize)]
struct AccountCase {
    name: String,
    exposure_evidence: String,
    decision_evidence: String,
    historical_match_evidence: String,
    contact_title: String,
    contact_vantage: String,
    headcount: i64,
    expected_segment: Option<String>,
    expected_role_direct: bool,
    expected_route_only: bool,
    expected_requires_confirmed_problem: bool,
    expected_state: String,
    expected_touches: usize,
    expected_lane: String,
}

#[derive(Debug, Deserialize)]
struct CopyCase {
    name: String,
    expected_touches: usize,
    subject: String,
    body: String,
    #[serde(default)]
    second_body: String,
    sendable: bool,
    expect_issue_containing: String,
}

fn corpus() -> Corpus {
    serde_json::from_str(include_str!(
        "../tests/fixtures/outagehub_eval_2026-08-13.json"
    ))
    .expect("parse outagehub eval corpus")
}

fn outagehub_context(state: &str) -> GtmActionContext {
    let play = crate::gtm::default_plays()
        .into_iter()
        .find(|play| play.brand == "outagehub")
        .expect("outagehub play");
    GtmActionContext {
        state: state.into(),
        play: Some(play),
        ..Default::default()
    }
}

#[test]
fn account_corpus_states_roles_touches_and_lanes() {
    for case in corpus().account_cases {
        let exposure =
            credible_outagehub_signal("account.outage_sensitive_exposure", &case.exposure_evidence);
        let decision =
            credible_outagehub_signal("account.outage_sensitive_decision", &case.decision_evidence);
        let matched = !case.historical_match_evidence.is_empty()
            && credible_outagehub_signal(
                "account.historical_location_outage_match",
                &case.historical_match_evidence,
            );
        // Production only feeds credible decision observations into segment
        // classification and role matching; mirror that boundary exactly.
        let effective_decision = if decision {
            case.decision_evidence.as_str()
        } else {
            ""
        };
        let segment = segment_for_evidence(effective_decision);
        assert_eq!(
            segment.map(|segment| segment.key),
            case.expected_segment.as_deref(),
            "{}: segment",
            case.name
        );

        let role_direct = outagehub_role_matches_decision(
            &case.contact_title,
            &case.contact_vantage,
            effective_decision,
        );
        assert_eq!(
            role_direct, case.expected_role_direct,
            "{}: role",
            case.name
        );
        assert_eq!(
            crate::response_design::is_route_only_contact(
                &case.contact_title,
                &case.contact_vantage
            ),
            case.expected_route_only,
            "{}: route-only",
            case.name
        );
        assert_eq!(
            crate::response_design::requires_confirmed_problem(
                &case.contact_title,
                &case.contact_vantage
            ),
            case.expected_requires_confirmed_problem,
            "{}: requires confirmed problem",
            case.name
        );

        let state = if !(exposure && decision && role_direct) {
            "research_required"
        } else if matched {
            "action_ready"
        } else {
            "discovery_ready"
        };
        assert_eq!(state, case.expected_state, "{}: state", case.name);

        let context = outagehub_context(state);
        assert_eq!(
            context.max_authorized_touches(),
            case.expected_touches,
            "{}: authorized touches",
            case.name
        );

        let priority = outagehub_priority(&PriorityInputs {
            segment,
            active_claims: 3,
            decision_evidenced: decision,
            historical_match: matched,
            reachable_direct_owner: role_direct,
            headcount: case.headcount,
            problem_confirmed: false,
        });
        assert_eq!(priority.lane, case.expected_lane, "{}: lane", case.name);
        if priority.lane != "research" {
            assert_eq!(
                priority.components.len(),
                10,
                "{}: every priority component must be persisted and displayable",
                case.name
            );
            assert!(
                !priority.next_missing_fact.is_empty(),
                "{}: next missing fact",
                case.name
            );
        }
    }
}

#[test]
fn engaged_accounts_unlock_four_touches_and_seven_stays_retired() {
    let mut context = outagehub_context("action_ready");
    assert_eq!(context.max_authorized_touches(), 2);
    context.engaged = true;
    assert_eq!(context.max_authorized_touches(), 4);
    assert!(!context.sequence_ready_for(7));
    // A CLI request for the legacy full cadence collapses to one discovery email.
    assert_eq!(
        crate::outreach::supported_touch_count_for_brand("outagehub", 7),
        1
    );
    assert_eq!(
        crate::outreach::supported_touch_count_for_brand("outagehub", 4),
        4
    );
}

#[test]
fn copy_corpus_runs_the_real_deterministic_linter() {
    let playbooks = Playbooks::load("playbooks").expect("load playbooks");
    let pb = playbooks.get("outagehub").expect("outagehub playbook");
    for case in corpus().copy_cases {
        let mut touches = vec![CopyTouch {
            stage: 1,
            day_offset: 0,
            channel: "email".into(),
            subject: case.subject.clone(),
            body: case.body.clone(),
            purpose: String::new(),
            goal: String::new(),
        }];
        if !case.second_body.is_empty() {
            touches.push(CopyTouch {
                stage: 2,
                day_offset: 6,
                channel: "email".into(),
                subject: format!("re: {}", case.subject),
                body: case.second_body.clone(),
                purpose: String::new(),
                goal: String::new(),
            });
        }
        let sequence = CopySequence {
            touches,
            applied_principles: Vec::new(),
        };
        let issues = crate::outreach::sequence_quality_issues(
            pb,
            &playbooks.shared,
            &sequence,
            &[],
            case.expected_touches,
            false,
        );
        if case.sendable {
            assert!(
                issues.is_empty(),
                "{}: expected sendable, got issues {issues:?}",
                case.name
            );
        } else {
            assert!(
                issues
                    .iter()
                    .any(|issue| issue.contains(&case.expect_issue_containing)),
                "{}: expected an issue containing '{}', got {issues:?}",
                case.name,
                case.expect_issue_containing
            );
        }
    }
}
