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
        "account.outage_sensitive_exposure" => outage_sensitive_exposure(&text),
        "account.outage_sensitive_decision" => outage_sensitive_decision(&text),
        "account.operated_ev_charging_network" => operated_ev_charging_network(&text),
        "account.historical_location_outage_match" => historical_location_outage_match(&text),
        _ => true,
    }
}

fn has(text: &str, terms: &[&str]) -> bool {
    terms.iter().any(|term| text.contains(term))
}

fn distributed_operating_footprint(text: &str) -> bool {
    let concrete_site = has(
        text,
        &[
            "site",
            "facility",
            "facilities",
            "warehouse",
            "residence",
            "charger",
            "station",
            "asset",
            "store",
            "branch",
            "terminal",
            "plant",
            "campus",
            "tower",
        ],
    );
    let operated_network = has(text, &["network"])
        && has(
            text,
            &[
                "charging",
                "charger",
                "telecom",
                "communications",
                "cell site",
                "tower",
                "remote site",
                "asset network",
            ],
        );
    let operating_site = concrete_site || operated_network;
    let distributed = has(
        text,
        &[
            "multiple",
            "two",
            "three",
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
    // A supplier's customer addresses are not its own distributed operating
    // footprint. This distinction matters for OutageHub: matching 175,000
    // delivery destinations is not evidence that the supplier monitors or
    // makes outage-time decisions for those locations.
    let customer_delivery_only = has(
        text,
        &[
            "customer delivery location",
            "customer locations",
            "delivery destinations",
            "delivery addresses",
        ],
    ) && !has(
        text,
        &[
            "operates the sites",
            "operates sites",
            "operates facilities",
            "owns the sites",
            "owns facilities",
            "manages the sites",
            "monitors the sites",
        ],
    );
    operating_site && distributed && !office_only && !customer_delivery_only
}

fn operated_ev_charging_network(text: &str) -> bool {
    let operator = has(
        text,
        &[
            "operates",
            "operating",
            "owns",
            "manages",
            "monitors",
            "monitoring",
            "charging network",
            "charging sites",
        ],
    );
    let charging = has(
        text,
        &[
            "ev charging",
            "electric vehicle charging",
            "charging station",
            "charging site",
            "charger network",
        ],
    );
    let footprint = has(
        text,
        &[
            "canada",
            "canadian",
            "ontario",
            "quebec",
            "alberta",
            "british columbia",
            "locations",
            "sites",
            "stations",
            "network",
        ],
    );
    operator && charging && footprint
}

fn historical_location_outage_match(text: &str) -> bool {
    let completed_match = has(
        text,
        &[
            "matched",
            "overlapped",
            "fell inside",
            "was inside",
            "inside reported",
            "within reported",
            "historical result",
            "historical match",
        ],
    );
    let utility_event = has(
        text,
        &[
            "utility outage",
            "utility report",
            "outage area",
            "reported outage",
        ],
    );
    let account_location = has(
        text,
        &[
            "charging site",
            "charging station",
            "charging location",
            "charger location",
            "site at",
            "site in",
            "station at",
            "station in",
            "facility at",
            "facility in",
            "laboratory at",
            "laboratory in",
            "lab at",
            "lab in",
            "warehouse at",
            "warehouse in",
            "tower at",
            "tower in",
            "location at",
            "location in",
            "store at",
            "store in",
            "residence at",
            "residence in",
            "plant at",
            "plant in",
            "property at",
            "property in",
            "asset at",
        ],
    );
    let time = text.chars().any(|character| character.is_ascii_digit())
        && has(
            text,
            &[
                "timestamp",
                "date",
                "time",
                "2024",
                "2025",
                "2026",
                "january",
                "february",
                "march",
                "april",
                "may",
                "june",
                "july",
                "august",
                "september",
                "october",
                "november",
                "december",
            ],
        );
    let hypothetical = has(
        text,
        &[
            "could match",
            "can match",
            "would match",
            "may match",
            "example could",
            "proposed",
        ],
    );
    completed_match && utility_event && account_location && time && !hypothetical
}

fn outage_sensitive_decision(text: &str) -> bool {
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
    let speculative = has(
        text,
        &[
            "could use",
            "could improve",
            "could change",
            "may use",
            "may improve",
            "might use",
            "might improve",
            "would use",
            "would improve",
            "can use",
            "can improve",
            "plausibly",
            "hypothesis",
            "proposed workflow",
            "potential workflow",
            "trying to learn whether",
        ],
    );

    explicit_outage && explicit_action && !speculative
}

/// First-party evidence that grid loss could matter to the operated footprint.
/// This is deliberately separate from evidence that the account actually makes
/// a named outage-time decision. Exposure may prioritize research; it can never
/// satisfy `account.outage_sensitive_decision` or authorize copy by itself.
fn outage_sensitive_exposure(text: &str) -> bool {
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
            "monitoring",
            "oversees",
        ],
    );
    let charging_network = has(
        text,
        &[
            "charger",
            "charging station",
            "charging site",
            "charging network",
        ],
    ) && has(
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
    let power_service = has(text, &["generator", "temporary power", "backup power"]);
    let response_operation = has(
        text,
        &[
            "rental fleet",
            "dispatches technicians",
            "dispatching technicians",
            "dispatches crews",
            "field technicians",
            "emergency response fleet",
            "temporary-power response",
        ],
    ) || (has(text, &["remote monitoring", "monitors"])
        && has(text, &["generator", "customer sites"]))
        || (has(text, &["emergency response"]) && has(text, &["generator", "temporary power"]));
    let managed_power_response = power_service && response_operation;

    (owns_or_runs
        && (charging_network
            || laboratory_service
            || cold_chain
            || care_portfolio
            || communications_network))
        || managed_power_response
}

/// Whether a title is close enough to the exact evidenced outage-time decision
/// to receive customer outreach. Broad operating titles are useful only when
/// the decision evidence names an operating segment; high-risk corporate
/// functions never become workflow owners through a model-supplied vantage.
pub(crate) fn outagehub_role_matches_decision(
    title: &str,
    vantage: &str,
    decision_evidence: &str,
) -> bool {
    let title = format!(
        " {} ",
        title
            .trim()
            .to_ascii_lowercase()
            .replace(['-', '/', ',', '&'], " ")
    );
    let has_title = |terms: &[&str]| terms.iter().any(|term| title.contains(term));

    if has_title(&[
        " human resources ",
        " hr ",
        " people and culture ",
        " people operations ",
        " talent ",
        " recruiter ",
        " marketing ",
        " sales ",
        " business development ",
        " account manager ",
        " finance ",
        " financial ",
        " accounting ",
        " controller ",
        " legal ",
        " counsel ",
        " procurement ",
        " purchasing ",
        " sourcing ",
        " underwriter ",
        " underwriting ",
    ]) {
        return false;
    }

    if !matches!(
        vantage.trim().to_ascii_lowercase().as_str(),
        "operator" | "process_owner" | "operational_executive"
    ) {
        return false;
    }

    // The segment catalog owns the evidence→owner-title mapping. No matched
    // segment means the evidence never named a concrete decision, so no title
    // is close enough; deprioritized segments hold cold outreach entirely.
    let Some(segment) = crate::segments::segment_for_evidence(decision_evidence) else {
        return false;
    };
    if segment.deprioritized {
        return false;
    }

    // Disruption-coordination functions own outage response across segments
    // and are valid direct contacts whenever a concrete decision is evidenced.
    if has_title(crate::segments::CONTINUITY_TITLE_TERMS) {
        return true;
    }

    has_title(segment.owner_title_terms)
}

#[cfg(test)]
mod tests {
    use super::credible_outagehub_signal;

    #[test]
    fn exposure_never_masquerades_as_an_evidenced_decision() {
        for evidence in [
            "The company operates and monitors a charging network across Canadian sites.",
            "The company operates laboratories and service centres that process clinical specimens across Ontario.",
            "The operator runs automated cold-storage facilities across Ontario, Alberta, and Quebec.",
            "The organization operates a telecom network through a network operations centre and remote sites.",
            "The provider dispatches field technicians and a rental generator fleet to customer sites during emergency response.",
        ] {
            assert!(credible_outagehub_signal(
                "account.outage_sensitive_exposure",
                evidence
            ));
            assert!(!credible_outagehub_signal(
                "account.outage_sensitive_decision",
                evidence
            ));
        }
    }

    #[test]
    fn decision_requires_an_actual_outage_event_and_action_not_a_possibility() {
        assert!(credible_outagehub_signal(
            "account.outage_sensitive_decision",
            "Service Operations checks utility outage status before dispatching a field crew."
        ));
        assert!(!credible_outagehub_signal(
            "account.outage_sensitive_decision",
            "Utility outage context could improve field dispatch."
        ));
    }

    #[test]
    fn generic_contractors_and_electricity_users_do_not_pass() {
        for evidence in [
            "The electrical contractor provides 24/7 emergency electrical service.",
            "The installer sells generators and solar equipment.",
            "The company has offices across Canada and every office uses electricity.",
            "The company provides refrigerated logistics and hold control.",
            "The propane supplier provides fuel delivery to customer backup-power locations across Canada.",
            "The supplier serves 175,000 customer delivery locations across Canada.",
        ] {
            assert!(!credible_outagehub_signal(
                "account.outage_sensitive_exposure",
                evidence
            ));
        }
    }

    #[test]
    fn customer_addresses_are_not_the_operators_distributed_sites() {
        assert!(!credible_outagehub_signal(
            "account.distributed_locations",
            "The supplier serves 175,000 customer delivery locations across Canada."
        ));
        assert!(credible_outagehub_signal(
            "account.distributed_locations",
            "The operator runs automated cold-storage facilities across Ontario, Alberta, and Quebec."
        ));
    }

    #[test]
    fn vertical_roles_follow_the_evidenced_decision_and_block_corporate_functions() {
        use super::outagehub_role_matches_decision;

        let lab =
            "Laboratory operations checks utility status during a facility power interruption.";
        assert!(outagehub_role_matches_decision(
            "Director of Operations",
            "process_owner",
            lab
        ));
        assert!(outagehub_role_matches_decision(
            "Business Continuity Manager",
            "process_owner",
            lab
        ));
        assert!(!outagehub_role_matches_decision(
            "VP Finance",
            "process_owner",
            lab
        ));
        assert!(!outagehub_role_matches_decision(
            "Senior Account Manager",
            "process_owner",
            lab
        ));

        let claims =
            "CAT claims operations prioritizes catastrophe response using utility outage reports.";
        assert!(outagehub_role_matches_decision(
            "Director, CAT Claims Operations",
            "process_owner",
            claims
        ));
        assert!(!outagehub_role_matches_decision(
            "Property Underwriting Director",
            "process_owner",
            claims
        ));
    }

    #[test]
    fn reviewed_export_people_and_corporate_functions_cannot_receive_outagehub_copy() {
        use super::outagehub_role_matches_decision;

        let decision =
            "Facility operations checks utility status during a site power interruption.";
        for (name, title) in [
            ("Stuart", "Senior Account Manager"),
            ("Philippe", "Human Resources Director"),
            (
                "Lee Hodgkinson",
                "Commercial Sales & Business Development Manager",
            ),
            ("Finance fixture", "VP Finance"),
            ("Marketing fixture", "Director of Marketing"),
        ] {
            assert!(
                !outagehub_role_matches_decision(title, "process_owner", decision),
                "{name} with title {title} must remain blocked"
            );
        }
    }
}
