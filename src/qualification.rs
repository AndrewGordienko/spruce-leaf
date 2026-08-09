//! Deterministic qualification boundaries shared by sourcing and activation.
//!
//! Model judgment decides whether an account is interesting. These checks only
//! prevent a previously saved or loosely worded signal from crossing the final
//! action boundary. Keeping them here avoids sourcing and outreach drifting
//! into two different definitions of a credible account.

pub(crate) fn credible_outagehub_signal(key: &str, evidence: &str) -> bool {
    let text = evidence.to_ascii_lowercase();
    match key.trim() {
        "account.distributed_locations" => distributed_operating_footprint(&text),
        "account.outage_sensitive_decision" => outage_decision_or_exposure(&text),
        _ => true,
    }
}

fn has(text: &str, terms: &[&str]) -> bool {
    terms.iter().any(|term| text.contains(term))
}

fn distributed_operating_footprint(text: &str) -> bool {
    let operating_site = has(
        text,
        &[
            "site",
            "facility",
            "facilities",
            "warehouse",
            "residence",
            "charger",
            "station",
            "network",
            "asset",
            "portfolio",
            "store",
            "branch",
            "terminal",
            "plant",
            "campus",
            "tower",
            "location",
        ],
    );
    let distributed = has(
        text,
        &[
            "multiple",
            "several",
            "across",
            "province",
            "region",
            "territor",
            "nationwide",
            "canada",
            "remote",
            "distributed",
            "locations",
            "facilities",
            "sites",
        ],
    );
    let office_only = has(text, &["office", "headquarters", " hq"])
        && !has(
            text,
            &[
                "site",
                "facility",
                "facilities",
                "warehouse",
                "residence",
                "charger",
                "station",
                "network",
                "asset",
                "store",
                "terminal",
                "plant",
                "tower",
            ],
        );
    operating_site && distributed && !office_only
}

fn outage_decision_or_exposure(text: &str) -> bool {
    let explicit_outage = has(
        text,
        &[
            "outage",
            "utility",
            "grid",
            "loss of power",
            "loss-of-power",
            "power loss",
            "power failure",
            "power interruption",
            "blackout",
            "restoration",
        ],
    );
    let explicit_action = has(
        text,
        &[
            "dispatch",
            "escalat",
            "hold",
            "transfer",
            "communicat",
            "prioriti",
            "classif",
            "check",
            "respond",
            "response",
            "route",
            "investigat",
            "redirect",
            "backup",
            "continuity",
        ],
    );
    if explicit_outage && explicit_action {
        return true;
    }

    // Public sites rarely describe the private alarm or triage workflow that a
    // cold email is meant to test. A first-party operating footprint can still
    // support a cautious hypothesis when loss of grid power has an obvious,
    // time-sensitive bearing on the service. This is exposure evidence, not
    // permission to state the internal workflow as fact.
    let owns_or_runs = has(
        text,
        &[
            "operates",
            "operating",
            "runs",
            "owns",
            "manages",
            "maintains",
            "monitors",
            "oversees",
        ],
    );
    let charging_network = has(text, &["charger", "charging station", "charging network"])
        && has(
            text,
            &["network", "site", "station", "operations", "monitor"],
        );
    let laboratory_service = has(text, &["laboratory", "laboratories", "diagnostic lab"])
        && has(
            text,
            &[
                "specimen",
                "testing",
                "diagnostic",
                "clinical",
                "service centre",
            ],
        );
    let cold_chain = has(
        text,
        &[
            "cold-storage",
            "cold storage",
            "refrigerated warehouse",
            "refrigerated facilities",
        ],
    );
    let care_portfolio = has(
        text,
        &[
            "senior living",
            "long-term care",
            "residence",
            "healthcare facility",
        ],
    ) && has(
        text,
        &["resident", "patient", "care", "continuity", "portfolio"],
    );
    let communications_network = has(
        text,
        &[
            "network operations centre",
            "network operations center",
            " noc ",
            "cell site",
            "communications site",
            "telecom network",
            "tower network",
        ],
    );
    let managed_power_response = has(text, &["generator", "temporary power", "backup power"])
        && has(
            text,
            &[
                "rental fleet",
                "fuel delivery",
                "remote monitoring",
                "dispatch",
                "field technicians",
                "customer sites",
                "emergency response",
            ],
        );

    (owns_or_runs
        && (charging_network
            || laboratory_service
            || cold_chain
            || care_portfolio
            || communications_network))
        || managed_power_response
}

#[cfg(test)]
mod tests {
    use super::credible_outagehub_signal;

    #[test]
    fn stable_operating_context_can_support_a_hypothesis_without_a_public_incident() {
        for evidence in [
            "The company operates and monitors a charging network across Canadian sites.",
            "The company operates laboratories and service centres that process clinical specimens across Ontario.",
            "The operator runs automated cold-storage facilities across Ontario, Alberta, and Quebec.",
            "The organization operates a telecom network through a network operations centre and remote sites.",
            "The provider dispatches field technicians and a rental generator fleet to customer sites during emergency response.",
        ] {
            assert!(credible_outagehub_signal(
                "account.outage_sensitive_decision",
                evidence
            ));
        }
    }

    #[test]
    fn generic_contractors_and_electricity_users_do_not_pass() {
        for evidence in [
            "The electrical contractor provides 24/7 emergency electrical service.",
            "The installer sells generators and solar equipment.",
            "The company has offices across Canada and every office uses electricity.",
            "The company provides refrigerated logistics and hold control.",
        ] {
            assert!(!credible_outagehub_signal(
                "account.outage_sensitive_decision",
                evidence
            ));
        }
    }
}
