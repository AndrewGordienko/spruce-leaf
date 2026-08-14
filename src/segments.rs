//! OutageHub's segment-specific market model. One universal site-prioritization
//! story produced fluent but commercially weak outreach; each segment here
//! names its own operating event, decision, evidence bar, buyer map, bounded
//! first offer, and kill condition. Classification is deterministic and driven
//! by the *decision evidence text*, never by industry labels alone, so an
//! exposure-only account can never borrow another segment's workflow story.

/// One OutageHub market segment. Every field is buyer-market doctrine, not
/// copy: the writer still only sees atomic evidence claims. `evidence_terms`
/// classify decision evidence into the segment; `owner_title_terms` are the
/// only title fragments that can make a person a direct workflow contact for
/// that segment's decision.
#[derive(Debug)]
pub struct OutageSegment {
    pub key: &'static str,
    pub name: &'static str,
    /// The exact operating event OutageHub could matter to.
    pub operating_event: &'static str,
    /// The decision OutageHub might improve.
    pub decision: &'static str,
    /// Evidence required before copy may mention that decision.
    pub evidence_required: &'static str,
    /// What the operator most likely uses today.
    pub current_alternatives: &'static str,
    /// Why public utility information could add something.
    pub utility_context_value: &'static str,
    /// Honest reasons it may add nothing.
    pub may_add_nothing: &'static str,
    /// Apollo search titles for likely workflow witnesses.
    pub witness_titles: &'static [&'static str],
    /// Apollo search titles for likely process owners.
    pub owner_titles: &'static [&'static str],
    /// Later-stage technical evaluators (never discovery contacts).
    pub technical_evaluators: &'static [&'static str],
    /// Later-stage economic buyers (never discovery contacts).
    pub economic_buyers: &'static [&'static str],
    /// Roles that may only be asked to route.
    pub routers: &'static [&'static str],
    /// Roles that must never receive this segment's operational campaign.
    pub unsuitable_roles: &'static [&'static str],
    /// The bounded first offer matched to available evidence.
    pub bounded_first_offer: &'static str,
    /// When to stop pursuing an account in this segment.
    pub kill_condition: &'static str,
    /// Default commercial lane before account-level adjustment.
    pub default_lane: &'static str,
    /// True when the segment should not receive proactive cold outreach until
    /// better evidence or a better product exists. Research may continue.
    pub deprioritized: bool,
    /// Lowercase fragments that classify decision evidence into this segment.
    pub evidence_terms: &'static [&'static str],
    /// Lowercase, space-padded title fragments accepted as direct owners.
    pub owner_title_terms: &'static [&'static str],
}

/// Disruption-coordination titles that own outage response across segments.
/// They are valid direct contacts only when a concrete decision is evidenced.
pub const CONTINUITY_TITLE_TERMS: &[&str] = &[
    " business continuity ",
    " emergency management ",
    " emergency operations ",
    " emergency preparedness ",
    " incident management ",
    " incident response ",
    " resilience operations ",
];

pub const CONTINUITY_SEARCH_TITLES: &[&str] = &[
    "business continuity manager",
    "emergency management director",
];

/// Ordered most-specific first: classification takes the first segment whose
/// evidence terms appear, so `cold storage` wins over the generic facilities
/// fallback and `catastrophe claims` never lands in property management.
pub static OUTAGE_SEGMENTS: &[OutageSegment] = &[
    OutageSegment {
        key: "insurance_cat",
        name: "Insurance catastrophe claims",
        operating_event: "A storm or grid event produces a surge of property or business-interruption claims across a region.",
        decision: "Whether a claimed loss window overlaps a utility-reported outage at the insured location, affecting triage, reserving, and fraud screening.",
        evidence_required: "Source names CAT/claims operations handling outage-related claims or using outage data; generic property underwriting exposure never counts.",
        current_alternatives: "Adjuster judgment, policyholder statements, weather data vendors, utility call-ins.",
        utility_context_value: "Independent, timestamped utility reports at the insured location can corroborate or contradict a claimed outage window without site visits.",
        may_add_nothing: "Carriers with established weather/CAT data stacks may already license outage feeds; small books may not justify integration.",
        witness_titles: &["CAT claims operations manager", "catastrophe response manager"],
        owner_titles: &["director CAT claims operations", "director claims operations"],
        technical_evaluators: &["claims data analytics lead", "geospatial analytics manager"],
        economic_buyers: &["VP claims", "chief claims officer"],
        routers: &["claims team lead"],
        unsuitable_roles: &["underwriter", "actuary", "broker relations", "sales", "marketing", "HR"],
        bounded_first_offer: "Historical replay of a named CAT event: utility-reported outage windows for a sample of claimed locations.",
        kill_condition: "Claims operations confirms outage verification is already sourced or not part of triage.",
        default_lane: "medium",
        deprioritized: false,
        evidence_terms: &["claim", "catastrophe", " cat ", "policyholder", "insured location", "business interruption"],
        owner_title_terms: &[
            " cat claims ",
            " catastrophe claims ",
            " claims operations ",
            " claims response ",
            " catastrophe response ",
            " claims data ",
            " geospatial ",
        ],
    },
    OutageSegment {
        key: "telecom",
        name: "Telecom and remote infrastructure",
        operating_event: "A tower, cell site, or remote node goes unreachable and the NOC must decide whether it is a power event before dispatching.",
        decision: "Dispatch and escalation: separate utility-grid loss from equipment failure before sending a field crew or opening a carrier ticket.",
        evidence_required: "Source names NOC/field operations handling site outages, power alarms, or dispatch decisions for distributed sites.",
        current_alternatives: "Site telemetry and battery alarms, utility web pages checked by hand, carrier NOC feeds.",
        utility_context_value: "A location-matched utility report can rule grid loss in or out at sites without power telemetry, before a truck rolls.",
        may_add_nothing: "Sites with full DC-plant monitoring already distinguish grid loss; large carriers may have direct utility relationships.",
        witness_titles: &["NOC supervisor", "network operations lead"],
        owner_titles: &["director network operations", "NOC manager"],
        technical_evaluators: &["network tools engineer", "OSS integration lead"],
        economic_buyers: &["VP network operations"],
        routers: &["service assurance manager"],
        unsuitable_roles: &["carrier sales", "marketing", "HR", "finance", "procurement"],
        bounded_first_offer: "Historical replay: utility events intersected with a sample of tower/site coordinates over the last storm season.",
        kill_condition: "NOC confirms power state is already deterministic from telemetry at every site class.",
        default_lane: "medium",
        deprioritized: false,
        evidence_terms: &["telecom", "cell site", "tower", "network operations centre", "network operations center", " noc "],
        owner_title_terms: &[
            " network operations ",
            " noc ",
            " field operations ",
            " field service ",
            " service assurance ",
            " reliability ",
            " site operations ",
            " infrastructure operations ",
        ],
    },
    OutageSegment {
        key: "ev_charging",
        name: "EV charging networks",
        operating_event: "Chargers at a site stop reporting or fail sessions and support must decide whether it is a site power event.",
        decision: "Diagnosis and field-service escalation: exclude utility loss before dispatching a technician or refunding sessions.",
        evidence_required: "First-party evidence the account operates/monitors Canadian charging sites AND a source naming the incident/dispatch decision.",
        current_alternatives: "Charger telemetry (OCPP status), CPO network dashboards, manual utility-page checks.",
        utility_context_value: "Charger-offline telemetry cannot distinguish grid loss from hardware failure; a location/time-matched utility report can.",
        may_add_nothing: "Networks whose site hosts own the electrical service may not act on utility data; some CPOs already poll utility feeds.",
        witness_titles: &["charging operations supervisor", "technical support supervisor"],
        owner_titles: &["director charging operations", "charging operations manager"],
        technical_evaluators: &["platform engineering lead", "integrations engineer"],
        economic_buyers: &["VP operations", "head of charging"],
        routers: &["customer success manager"],
        unsuitable_roles: &["site-host sales", "marketing", "HR", "finance", "procurement"],
        bounded_first_offer: "Historical replay: public charging locations intersected with utility outage polygons, with timestamps.",
        kill_condition: "Operator confirms utility status is already integrated into charger incident triage.",
        default_lane: "medium",
        deprioritized: false,
        evidence_terms: &["charger", "charging station", "charging site", "charging network", "ev charging"],
        owner_title_terms: &[
            " charging operations ",
            " charger operations ",
            " charging network ",
            " network operations ",
            " service operations ",
            " field operations ",
            " field service ",
            " product operations ",
            " support engineering ",
            " reliability ",
            " maintenance ",
        ],
    },
    OutageSegment {
        key: "generator_services",
        name: "Generator rental and emergency power",
        operating_event: "Outage-driven demand spikes: customers call for temporary power and dispatch must stage crews and fleet toward affected areas.",
        decision: "Dispatch staging and fleet positioning: which region's calls are grid-driven and how long the utility expects restoration to take.",
        evidence_required: "Source names dispatch/service operations responding to outages or emergency power calls.",
        current_alternatives: "Inbound call volume as the signal, utility public maps checked manually, local news.",
        utility_context_value: "Aggregated live and historical outage reports show where demand is forming and whether restoration estimates justify a rental.",
        may_add_nothing: "Operators serving planned construction power may see little outage-driven demand; tiny fleets may not reposition anyway.",
        witness_titles: &["dispatch supervisor", "field service supervisor"],
        owner_titles: &["director field operations", "service operations manager"],
        technical_evaluators: &["fleet systems administrator"],
        economic_buyers: &["general manager", "owner"],
        routers: &["rental coordinator"],
        unsuitable_roles: &["equipment sales", "marketing", "HR", "finance"],
        bounded_first_offer: "Historical replay: last season's utility outages mapped against the operator's service territory and depot locations.",
        kill_condition: "Dispatch confirms demand is fully planned/contracted and outage response is not a line of business.",
        default_lane: "easy",
        deprioritized: false,
        evidence_terms: &["generator fleet", "generator crew", "generator rental", "rental generator", "rental fleet", "temporary power", "emergency power", "standby power"],
        owner_title_terms: &[
            " field operations ",
            " field service ",
            " dispatch ",
            " service operations ",
            " rental operations ",
            " emergency operations ",
        ],
    },
    OutageSegment {
        key: "cold_storage",
        name: "Cold storage and refrigerated logistics",
        operating_event: "A site loses grid power and refrigeration rides on backup while maintenance decides how to verify and how long the window is.",
        decision: "Continuity and hold decisions: confirm a suspected utility event and use restoration context for generator runtime and inventory-risk calls.",
        evidence_required: "Source names facility/maintenance/refrigeration operations handling power interruptions or temperature-excursion response.",
        current_alternatives: "Temperature monitoring and alarms, generator autostart, calling the utility.",
        utility_context_value: "A matched utility report distinguishes site electrical faults from area outages and adds a restoration outlook alarms cannot give.",
        may_add_nothing: "Fully instrumented cold chains with 24/7 monitoring vendors may already receive utility context.",
        witness_titles: &["site operations manager", "facilities manager"],
        owner_titles: &["director of operations", "maintenance manager"],
        technical_evaluators: &["refrigeration engineer", "controls technician"],
        economic_buyers: &["VP operations", "general manager"],
        routers: &["quality assurance manager"],
        unsuitable_roles: &["logistics sales", "marketing", "HR", "finance", "procurement"],
        bounded_first_offer: "Historical replay: verified facility addresses intersected with utility outage polygons over the past year.",
        kill_condition: "Maintenance confirms every site has monitored generators and utility state is already visible in their alarm chain.",
        default_lane: "easy",
        deprioritized: false,
        evidence_terms: &["cold storage", "cold-storage", "refrigerat", "freezer", "cold chain", "reefer"],
        owner_title_terms: &[
            " director of operations ",
            " operations director ",
            " site operations ",
            " facilities ",
            " facility ",
            " warehouse operations ",
            " plant operations ",
            " maintenance ",
            " refrigeration ",
            " engineering services ",
        ],
    },
    OutageSegment {
        key: "data_centres",
        name: "Data centres and critical facilities",
        operating_event: "A utility disturbance forces a transfer to generator/UPS and facilities must log cause and coordinate with the utility on restoration.",
        decision: "Incident-record completeness and utility coordination during transfer events.",
        evidence_required: "Source names critical-facilities operations handling utility events; SLA-driven uptime marketing alone never counts.",
        current_alternatives: "Building management systems, EPMS, utility account managers, NOC procedures.",
        utility_context_value: "External confirmation and area context for the incident record; useful for post-incident review and customer communication.",
        may_add_nothing: "Tier III/IV operators already have utility relationships and full electrical telemetry; the marginal value is small.",
        witness_titles: &["critical facilities manager", "data centre operations manager"],
        owner_titles: &["director critical facilities", "director data centre operations"],
        technical_evaluators: &["electrical engineering manager"],
        economic_buyers: &["VP infrastructure"],
        routers: &["service delivery manager"],
        unsuitable_roles: &["colocation sales", "marketing", "HR", "finance", "procurement"],
        bounded_first_offer: "API evaluation scoped to event-record enrichment for a subset of sites.",
        kill_condition: "Facilities confirms utility liaison and telemetry already cover every transfer event.",
        default_lane: "hard",
        deprioritized: false,
        evidence_terms: &["data centre", "data center", "colocation", "critical facilit", "uptime institute"],
        owner_title_terms: &[
            " critical facilities ",
            " data centre operations ",
            " data center operations ",
            " facilities ",
            " facility ",
            " site operations ",
            " electrical operations ",
        ],
    },
    OutageSegment {
        key: "labs_healthcare",
        name: "Laboratories, pharmacies, and healthcare networks",
        operating_event: "A site power interruption threatens specimen/vaccine storage and testing throughput; operations must verify scope and duration.",
        decision: "Continuity response: confirm a utility event at the affected site and use restoration context for transfer or courier decisions.",
        evidence_required: "Source names lab/pharmacy/facility operations handling power interruptions, cold-chain response, or site continuity.",
        current_alternatives: "Temperature alarms, site staff phone reports, calling the utility, generator vendors.",
        utility_context_value: "Multi-site networks lack power telemetry at retail-style sites; a matched utility report scopes the event and its area.",
        may_add_nothing: "Hospital-grade facilities with plant operations already know; small single-site labs have no coordination problem.",
        witness_titles: &["laboratory operations manager", "site operations manager"],
        owner_titles: &["director of operations", "director facilities"],
        technical_evaluators: &["clinical engineering lead"],
        economic_buyers: &["VP operations"],
        routers: &["quality and compliance manager"],
        unsuitable_roles: &["physician liaison", "marketing", "HR", "finance", "procurement", "sales"],
        bounded_first_offer: "Historical replay: verified site addresses intersected with utility outage polygons, with timestamps.",
        kill_condition: "Operations confirms site power events are already centrally visible with restoration context.",
        default_lane: "medium",
        deprioritized: false,
        evidence_terms: &["laborator", " lab ", "pharmac", "specimen", "vaccine", "clinic", "diagnostic"],
        owner_title_terms: &[
            " director of operations ",
            " operations director ",
            " laboratory operations ",
            " lab operations ",
            " pharmacy operations ",
            " clinical operations ",
            " site operations ",
            " facilities ",
            " facility ",
            " maintenance ",
        ],
    },
    OutageSegment {
        key: "senior_residences",
        name: "Senior residences and care operators",
        operating_event: "A residence loses grid power and staff must run the emergency plan: verify scope, start generator checks, decide on family/authority notice.",
        decision: "Emergency-plan activation and notification: confirm the utility event, its area, and the restoration outlook.",
        evidence_required: "Source names facility/emergency-preparedness operations for power events; a generic emergency-plan webpage alone is exposure, not a decision.",
        current_alternatives: "Site staff observation, utility phone lines, regional managers calling around.",
        utility_context_value: "Portfolio operators get one consistent, timestamped view across residences instead of per-site phone checks.",
        may_add_nothing: "Single-residence operators resolve this by looking out the window; regulation may already mandate richer monitoring.",
        witness_titles: &["facilities manager", "environmental services manager"],
        owner_titles: &["director property operations", "regional director of operations"],
        technical_evaluators: &["building systems manager"],
        economic_buyers: &["VP operations"],
        routers: &["executive director"],
        unsuitable_roles: &["sales and marketing director", "move-in coordinator", "HR", "finance"],
        bounded_first_offer: "Historical replay: residence addresses intersected with utility outage polygons over the past year.",
        kill_condition: "Operations confirms per-site staffing makes central outage awareness redundant.",
        default_lane: "medium",
        deprioritized: false,
        evidence_terms: &["residence", "retirement", "long-term care", "long term care", "care home", "assisted living", "senior living"],
        owner_title_terms: &[
            " property operations ",
            " regional operations ",
            " director of operations ",
            " operations director ",
            " facilities ",
            " facility ",
            " environmental services ",
            " building operations ",
            " maintenance ",
        ],
    },
    OutageSegment {
        key: "retail_fuel",
        name: "Multi-site retail and fuel networks",
        operating_event: "Stores or stations go dark in an area event; central operations must scope which sites are affected and for how long.",
        decision: "Area scoping and store-communication decisions during a regional outage.",
        evidence_required: "Source names central/store operations or loss prevention handling outage response; POS downtime marketing never counts.",
        current_alternatives: "POS/network monitoring, store managers phoning in, utility public maps.",
        utility_context_value: "One matched feed scopes affected stores and restoration outlook without collecting phone reports.",
        may_add_nothing: "POS connectivity monitoring already approximates store power state; many chains accept the phone-tree cost.",
        witness_titles: &["store operations manager", "facilities manager"],
        owner_titles: &["director store operations", "director of operations"],
        technical_evaluators: &["retail systems manager"],
        economic_buyers: &["VP store operations"],
        routers: &["loss prevention manager"],
        unsuitable_roles: &["merchandising", "marketing", "HR", "finance", "procurement", "real estate"],
        bounded_first_offer: "Historical replay: store addresses intersected with utility outage polygons for the last storm season.",
        kill_condition: "Central operations confirms POS-derived power state is sufficient and no restoration context is wanted.",
        default_lane: "medium",
        deprioritized: false,
        evidence_terms: &["store", "retail", "fuel station", "gas station", "convenience", "supermarket", "grocery"],
        owner_title_terms: &[
            " store operations ",
            " retail operations ",
            " central operations ",
            " director of operations ",
            " operations director ",
            " facilities ",
            " facility ",
            " loss prevention ",
            " maintenance ",
        ],
    },
    OutageSegment {
        key: "property_facilities",
        name: "Multi-site property and facilities management",
        operating_event: "Buildings across a portfolio report power loss; operations must scope the event, brief tenants, and stage vendors.",
        decision: "Tenant communication and vendor staging: confirm the utility event and restoration outlook per building.",
        evidence_required: "Source names property/facility operations handling outage or emergency response across multiple buildings.",
        current_alternatives: "Building automation alarms, tenant calls, per-utility web pages.",
        utility_context_value: "A portfolio-wide matched feed replaces checking several utilities' pages during a regional event.",
        may_add_nothing: "Class-A portfolios with staffed buildings and BAS coverage may see no gap.",
        witness_titles: &["property manager", "building operations manager"],
        owner_titles: &["director property management", "director facilities"],
        technical_evaluators: &["building systems lead"],
        economic_buyers: &["VP property operations"],
        routers: &["tenant services coordinator"],
        unsuitable_roles: &["leasing", "marketing", "HR", "finance", "procurement"],
        bounded_first_offer: "Historical replay: portfolio addresses intersected with utility outage polygons over the past year.",
        kill_condition: "Operations confirms BAS and staffing already give real-time, portfolio-wide power state.",
        default_lane: "medium",
        deprioritized: false,
        evidence_terms: &["propert", "facilit", "building", "portfolio", "campus", "site operations", "warehouse", "plant"],
        owner_title_terms: &[
            " property operations ",
            " property management ",
            " building operations ",
            " director of operations ",
            " operations director ",
            " site operations ",
            " facilities ",
            " facility ",
            " warehouse operations ",
            " plant operations ",
            " maintenance ",
        ],
    },
    OutageSegment {
        key: "municipal_emergency",
        name: "Municipal emergency management and alerting",
        operating_event: "A regional outage triggers EOC activation and public-communication decisions.",
        decision: "Situational awareness feeds for EOC dashboards and public alerting.",
        evidence_required: "Source names an EOC/emergency-management function consuming outage data operationally.",
        current_alternatives: "Direct utility liaison, provincial feeds, 911 call patterns.",
        utility_context_value: "Aggregation across utilities could simplify multi-provider municipalities.",
        may_add_nothing: "Municipalities typically have direct utility relationships and formal mutual-aid channels; procurement is slow and political.",
        witness_titles: &["emergency management coordinator"],
        owner_titles: &["emergency management director"],
        technical_evaluators: &["GIS analyst"],
        economic_buyers: &["city manager"],
        routers: &["communications officer"],
        unsuitable_roles: &["council members", "procurement", "HR", "finance"],
        bounded_first_offer: "None yet; requires an RFP-shaped product. Research inventory only.",
        kill_condition: "Deprioritized until a public-sector-ready offer exists; revisit on inbound interest only.",
        default_lane: "hard",
        deprioritized: true,
        evidence_terms: &["municipal", "emergency operations centre", "emergency operations center", " eoc ", "public alert", "911"],
        owner_title_terms: &[" emergency management ", " emergency operations "],
    },
    OutageSegment {
        key: "embedded_partners",
        name: "Embedded data, weather, risk, and incident-software partners",
        operating_event: "A platform's customers ask for outage context inside an existing product (weather, risk, FSM, monitoring).",
        decision: "Whether to embed a Canadian outage layer rather than build per-utility scrapers.",
        evidence_required: "Source shows the platform surfaces or plans outage/disruption data for customers.",
        current_alternatives: "In-house scraping, US-centric outage vendors, ignoring Canada.",
        utility_context_value: "One normalized Canadian feed replaces dozens of per-utility integrations.",
        may_add_nothing: "Platforms without Canadian traction have no reason to buy; build-vs-buy may favour build at scale.",
        witness_titles: &["product manager data"],
        owner_titles: &["head of data partnerships", "VP product"],
        technical_evaluators: &["data engineering lead"],
        economic_buyers: &["VP product"],
        routers: &["partnerships associate"],
        unsuitable_roles: &["field sales", "marketing", "HR", "finance"],
        bounded_first_offer: "Evaluation API key with historical archive access; partner motion, not cold operational outreach.",
        kill_condition: "Deprioritized for cold outbound: pursue via partnership conversations, not the operational-outreach machine.",
        default_lane: "hard",
        deprioritized: true,
        evidence_terms: &["api partner", "data partner", "weather platform", "risk platform", "incident software", "field service software", "embed outage"],
        owner_title_terms: &[" data partnerships ", " product "],
    },
];

/// Classify decision evidence into the first matching segment. Returns `None`
/// for empty or exposure-only text, which downstream code must treat as
/// research-required: no committee search, no direct-role grants, no copy.
pub fn segment_for_evidence(decision_evidence: &str) -> Option<&'static OutageSegment> {
    let text = format!(" {} ", decision_evidence.trim().to_ascii_lowercase());
    if text.trim().is_empty() {
        return None;
    }
    OUTAGE_SEGMENTS.iter().find(|segment| {
        segment
            .evidence_terms
            .iter()
            .any(|term| text.contains(term))
    })
}

pub fn segment_by_key(key: &str) -> Option<&'static OutageSegment> {
    OUTAGE_SEGMENTS
        .iter()
        .find(|segment| segment.key.eq_ignore_ascii_case(key.trim()))
}

/// Stable database/CLI market key for each doctrine segment. The first three
/// retain their shipped keys so existing coverage ledgers remain valid.
pub fn market_key_for_segment(key: &str) -> Option<&'static str> {
    match key.trim() {
        "insurance_cat" => Some("canada_outage_insurance_cat"),
        "telecom" => Some("canada_telecom_site_continuity"),
        "ev_charging" => Some("canada_ev_charging_operations"),
        "generator_services" => Some("canada_backup_power_dispatch"),
        "cold_storage" => Some("canada_outage_cold_storage"),
        "data_centres" => Some("canada_outage_data_centres"),
        "labs_healthcare" => Some("canada_outage_labs_healthcare"),
        "senior_residences" => Some("canada_outage_senior_residences"),
        "retail_fuel" => Some("canada_outage_retail_fuel"),
        "property_facilities" => Some("canada_outage_property_facilities"),
        "municipal_emergency" => Some("canada_outage_municipal_emergency"),
        "embedded_partners" => Some("canada_outage_embedded_partners"),
        _ => None,
    }
}

pub fn segment_for_market_key(key: &str) -> Option<&'static OutageSegment> {
    OUTAGE_SEGMENTS.iter().find(|segment| {
        market_key_for_segment(segment.key)
            .is_some_and(|market_key| market_key.eq_ignore_ascii_case(key.trim()))
    })
}

#[cfg(test)]
mod tests {
    use super::{segment_for_evidence, OUTAGE_SEGMENTS};

    #[test]
    fn twelve_segments_each_carry_complete_doctrine() {
        assert_eq!(OUTAGE_SEGMENTS.len(), 12);
        for segment in OUTAGE_SEGMENTS {
            assert!(!segment.operating_event.is_empty(), "{}", segment.key);
            assert!(!segment.decision.is_empty(), "{}", segment.key);
            assert!(!segment.evidence_required.is_empty(), "{}", segment.key);
            assert!(!segment.bounded_first_offer.is_empty(), "{}", segment.key);
            assert!(!segment.kill_condition.is_empty(), "{}", segment.key);
            assert!(!segment.unsuitable_roles.is_empty(), "{}", segment.key);
            assert!(
                matches!(segment.default_lane, "easy" | "medium" | "hard"),
                "{}",
                segment.key
            );
        }
    }

    #[test]
    fn classification_requires_decision_language_not_industry_labels() {
        assert!(segment_for_evidence("").is_none());
        assert_eq!(
            segment_for_evidence(
                "CAT claims operations prioritizes catastrophe response using utility outage reports."
            )
            .map(|segment| segment.key),
            Some("insurance_cat")
        );
        assert_eq!(
            segment_for_evidence(
                "The NOC decides whether a tower alarm is a grid event before dispatching a crew."
            )
            .map(|segment| segment.key),
            Some("telecom")
        );
        assert_eq!(
            segment_for_evidence(
                "Maintenance verifies utility status when a cold storage site loses power."
            )
            .map(|segment| segment.key),
            Some("cold_storage")
        );
    }

    #[test]
    fn weak_segments_are_explicitly_deprioritized_not_silently_equal() {
        let deprioritized = OUTAGE_SEGMENTS
            .iter()
            .filter(|segment| segment.deprioritized)
            .map(|segment| segment.key)
            .collect::<Vec<_>>();
        assert_eq!(
            deprioritized,
            vec!["municipal_emergency", "embedded_partners"]
        );
    }
}
