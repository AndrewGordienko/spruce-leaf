//! Easy/medium/hard commercial prioritization for OutageHub accounts.
//!
//! Market coverage and selling priority are different questions: an account can
//! be worth researching while being a poor place to spend writing capacity this
//! month. The lane is computed deterministically from persisted evidence, every
//! component is stored and displayed, and the score is only used to order work
//! inside a lane — it never overrides an evidence gate.

use serde::{Deserialize, Serialize};

use crate::segments::OutageSegment;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityComponent {
    pub name: String,
    /// 0–100. A component score is an ordering aid, never an authorization.
    pub score: i64,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CommercialPriority {
    /// easy | medium | hard | research
    pub lane: String,
    pub score: i64,
    pub segment_key: String,
    pub offer: String,
    pub first_contract_range: String,
    pub expected_cycle: String,
    pub procurement_complexity: String,
    pub strategic_value: String,
    pub next_missing_fact: String,
    pub components: Vec<PriorityComponent>,
}

/// Deterministic inputs assembled from the database; nothing here is a model
/// judgment. `reachable_direct_owner` means a stakeholder with direct role fit,
/// opportunity-linked evidence, and a verified channel.
pub struct PriorityInputs<'a> {
    pub segment: Option<&'a OutageSegment>,
    pub active_claims: usize,
    pub decision_evidenced: bool,
    pub historical_match: bool,
    pub reachable_direct_owner: bool,
    pub headcount: i64,
    pub problem_confirmed: bool,
}

pub fn outagehub_priority(inputs: &PriorityInputs) -> CommercialPriority {
    let Some(segment) = inputs.segment else {
        return research_priority("No decision evidence maps this account to a segment; exposure alone is research inventory.");
    };
    if !inputs.decision_evidenced {
        let mut priority = research_priority(
            "The segment is recognizable but no source names the outage-time decision yet.",
        );
        priority.segment_key = segment.key.into();
        return priority;
    }

    // Lane: start from segment doctrine, then adjust for the account.
    let enterprise = inputs.headcount > 2_000;
    let small = inputs.headcount > 0 && inputs.headcount <= 500;
    let lane = if segment.deprioritized || enterprise {
        "hard"
    } else if segment.default_lane == "easy" && !small && inputs.headcount > 0 {
        "medium"
    } else if segment.default_lane == "medium"
        && small
        && inputs.historical_match
        && inputs.reachable_direct_owner
    {
        // A bounded replay an owner can approve inside ~30 days.
        "easy"
    } else {
        segment.default_lane
    };

    let (range, cycle, procurement, strategic, offer_name) = match lane {
        "easy" => (
            "CAD $2,000–$15,000",
            "≈30 days",
            "low — owner or operations leader can approve",
            "reference account and reusable replay template",
            "Historical Location Replay",
        ),
        "medium" => (
            "CAD $7,500–$30,000 (subject to discovery)",
            "30–120 days",
            "vendor onboarding plus a technical evaluation",
            "recurring API revenue after the paid evaluation",
            "API Evaluation",
        ),
        _ => (
            "CAD $50,000+ first phase",
            "120+ days",
            "security review, procurement, and multiple stakeholders",
            "large embedded or enterprise expansion value",
            "Enterprise / Embedded Outage Data",
        ),
    };

    let next_missing_fact = if segment.deprioritized {
        format!("segment is deprioritized: {}", segment.kill_condition)
    } else if !inputs.reachable_direct_owner {
        "a named owner with title evidence near the decision and a verified channel".to_string()
    } else if !inputs.historical_match {
        "a completed historical location/outage match for a verified address".to_string()
    } else if !inputs.problem_confirmed {
        "a reply confirming the workflow exists as evidenced".to_string()
    } else {
        "none — negotiate the bounded first project".to_string()
    };
    let missing = [
        !inputs.reachable_direct_owner,
        !inputs.historical_match,
        !inputs.problem_confirmed,
    ]
    .iter()
    .filter(|gap| **gap)
    .count() as i64;

    let components = vec![
        PriorityComponent {
            name: "evidence_strength".into(),
            score: (inputs.active_claims as i64 * 20).min(100),
            note: format!("{} active atomic claims", inputs.active_claims),
        },
        PriorityComponent {
            name: "decision_specificity".into(),
            score: 100,
            note: format!("source-named decision in segment {}", segment.key),
        },
        PriorityComponent {
            name: "historical_match_availability".into(),
            score: if inputs.historical_match { 100 } else { 40 },
            note: if inputs.historical_match {
                "completed verified-address match exists".into()
            } else {
                "replay can run from verified addresses without integration".into()
            },
        },
        PriorityComponent {
            name: "recipient_reachability".into(),
            score: if inputs.reachable_direct_owner {
                100
            } else {
                0
            },
            note: if inputs.reachable_direct_owner {
                "direct workflow owner with a verified channel".into()
            } else {
                "no direct owner mapped yet; account stays research/routing".into()
            },
        },
        PriorityComponent {
            name: "first_offer_fit".into(),
            score: if segment.deprioritized {
                0
            } else {
                match lane {
                    "easy" => 100,
                    "medium" => 70,
                    _ => 40,
                }
            },
            note: segment.bounded_first_offer.into(),
        },
        PriorityComponent {
            name: "expected_sales_cycle".into(),
            score: match lane {
                "easy" => 100,
                "medium" => 60,
                _ => 20,
            },
            note: cycle.into(),
        },
        PriorityComponent {
            name: "procurement_complexity".into(),
            score: match lane {
                "easy" => 100,
                "medium" => 60,
                _ => 15,
            },
            note: procurement.into(),
        },
        PriorityComponent {
            name: "estimated_first_contract".into(),
            score: match lane {
                "easy" => 40,
                "medium" => 70,
                _ => 100,
            },
            note: range.into(),
        },
        PriorityComponent {
            name: "strategic_expansion_value".into(),
            score: match lane {
                "easy" => 30,
                "medium" => 60,
                _ => 100,
            },
            note: strategic.into(),
        },
        PriorityComponent {
            name: "next_missing_fact".into(),
            score: 100 - missing * 25,
            note: next_missing_fact.clone(),
        },
    ];

    // Cash-weighted ordering: closable evidence and reachability dominate;
    // strategic size matters least while cash-constrained.
    let score = components
        .iter()
        .map(|component| {
            let weight = match component.name.as_str() {
                "evidence_strength" | "decision_specificity" => 15,
                "historical_match_availability" | "recipient_reachability" => 15,
                "first_offer_fit" | "expected_sales_cycle" | "procurement_complexity" => 10,
                _ => 5,
            };
            component.score * weight
        })
        .sum::<i64>()
        / 100;

    CommercialPriority {
        lane: lane.into(),
        score,
        segment_key: segment.key.into(),
        offer: format!("{offer_name}: {}", segment.bounded_first_offer),
        first_contract_range: range.into(),
        expected_cycle: cycle.into(),
        procurement_complexity: procurement.into(),
        strategic_value: strategic.into(),
        next_missing_fact,
        components,
    }
}

fn research_priority(reason: &str) -> CommercialPriority {
    CommercialPriority {
        lane: "research".into(),
        score: 0,
        next_missing_fact:
            "a source naming a concrete outage-time decision (exposure is not a decision)".into(),
        components: vec![PriorityComponent {
            name: "decision_specificity".into(),
            score: 0,
            note: reason.into(),
        }],
        ..Default::default()
    }
}

/// Sort key for allocating research/writing capacity: easy first, then medium,
/// then hard, then research inventory; higher score first inside a lane.
pub fn lane_rank(lane: &str) -> u8 {
    match lane {
        "easy" => 0,
        "medium" => 1,
        "hard" => 2,
        _ => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::{lane_rank, outagehub_priority, PriorityInputs};
    use crate::segments::segment_by_key;

    #[test]
    fn small_evidenced_replayable_account_lands_in_the_easy_lane() {
        let priority = outagehub_priority(&PriorityInputs {
            segment: segment_by_key("labs_healthcare"),
            active_claims: 4,
            decision_evidenced: true,
            historical_match: true,
            reachable_direct_owner: true,
            headcount: 180,
            problem_confirmed: false,
        });
        assert_eq!(priority.lane, "easy");
        assert!(priority.offer.contains("Historical Location Replay"));
        assert_eq!(priority.components.len(), 10);
        assert!(priority.next_missing_fact.contains("reply confirming"));
    }

    #[test]
    fn enterprise_scale_forces_the_hard_lane_regardless_of_segment() {
        let priority = outagehub_priority(&PriorityInputs {
            segment: segment_by_key("cold_storage"),
            active_claims: 5,
            decision_evidenced: true,
            historical_match: true,
            reachable_direct_owner: true,
            headcount: 12_000,
            problem_confirmed: true,
        });
        assert_eq!(priority.lane, "hard");
        assert!(priority.offer.contains("Enterprise"));
    }

    #[test]
    fn exposure_without_a_decision_is_research_not_a_lane() {
        let priority = outagehub_priority(&PriorityInputs {
            segment: None,
            active_claims: 3,
            decision_evidenced: false,
            historical_match: false,
            reachable_direct_owner: true,
            headcount: 90,
            problem_confirmed: false,
        });
        assert_eq!(priority.lane, "research");
        assert_eq!(priority.score, 0);
        assert!(lane_rank(&priority.lane) > lane_rank("hard"));
    }

    #[test]
    fn deprioritized_segments_never_earn_a_cash_lane() {
        let priority = outagehub_priority(&PriorityInputs {
            segment: segment_by_key("municipal_emergency"),
            active_claims: 5,
            decision_evidenced: true,
            historical_match: true,
            reachable_direct_owner: true,
            headcount: 400,
            problem_confirmed: true,
        });
        assert_eq!(priority.lane, "hard");
        assert!(priority.next_missing_fact.contains("deprioritized"));
    }
}
