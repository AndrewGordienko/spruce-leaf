//! Role-aware response design for cold outreach.
//!
//! This deliberately models the recipient's work context, not their private
//! personality. A title and mapped vantage can support cautious claims about
//! what someone may know, what level of detail is useful, and what size of ask
//! is appropriate. They cannot support psychographic guesses about ambition,
//! fear, temperament, or personal "triggers."

use serde_json::{json, Value};

use crate::db::Person;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoleClass {
    Operator,
    FrontlineLeader,
    ProcessOwner,
    OperationalExecutive,
    EconomicLeader,
    EnterpriseExecutive,
    TechnicalEvaluator,
    CommercialRouter,
    Router,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResponseContract {
    pub role_class: RoleClass,
    pub label: &'static str,
    pub question_shape: &'static str,
    pub ask_shape: &'static str,
    pub reactance_guard: &'static str,
}

impl ResponseContract {
    pub(crate) fn prompt_value(&self) -> Value {
        json!({
            "role_class": self.label,
            "ask": self.question_shape,
            "next_step": self.ask_shape,
            "avoid": self.reactance_guard,
            "boundary": "Role-level work context only. Translate this into normal language; never mention the contract or infer private personality."
        })
    }

    pub(crate) fn prompt_block(&self) -> String {
        serde_json::to_string_pretty(&self.prompt_value()).unwrap_or_default()
    }
}

pub(crate) fn for_person(person: &Person) -> ResponseContract {
    for_title_and_vantage(&person.title, &person.vantage)
}

/// Return the outreach vantage we can safely use after checking the title.
///
/// The model maps a person against the account hypothesis, but a bad mapping
/// must not turn an explicitly temporary learning role into a workflow owner.
/// Interns and trainees may still be useful human routers; they are never the
/// recipient for an owner-level pitch, discovery request, or budget question.
pub(crate) fn effective_vantage(title: &str, vantage: &str) -> String {
    if is_learning_role(title) || is_people_function(title) {
        return "router".to_string();
    }
    let vantage = normalize_vantage(vantage);
    if vantage.is_empty() {
        "router".to_string()
    } else {
        vantage
    }
}

pub(crate) fn effective_primary(title: &str, primary: bool) -> bool {
    primary && !is_learning_role(title) && !is_people_function(title)
}

/// One shared ordering policy for campaign generation, persisted-contact
/// planning, and lead reuse. Keeping this here prevents the same person from
/// being treated as a buyer in one path and a router in another.
pub(crate) fn contact_priority(title: &str, vantage: &str, primary: bool) -> i32 {
    let class = classify(title, vantage);
    let route_only = matches!(class, RoleClass::CommercialRouter | RoleClass::Router);
    let mut score = match class {
        RoleClass::ProcessOwner => 70,
        RoleClass::FrontlineLeader => 65,
        RoleClass::Operator => 60,
        RoleClass::OperationalExecutive => 55,
        RoleClass::EnterpriseExecutive => 45,
        RoleClass::EconomicLeader => 40,
        RoleClass::TechnicalEvaluator => 25,
        RoleClass::CommercialRouter => 10,
        RoleClass::Router => 5,
    };

    // `primary` is a model judgment, not authority evidence. Keep it strictly a
    // tie-breaker inside a role class: the former +100 bonus let a model-marked
    // lower-relevance contact outrank a title-supported workflow owner.
    score += if primary && !route_only { 2 } else { 0 };
    score
}

pub(crate) fn is_workflow_discovery_contact(title: &str, vantage: &str) -> bool {
    matches!(
        classify(title, vantage),
        RoleClass::Operator
            | RoleClass::FrontlineLeader
            | RoleClass::ProcessOwner
            | RoleClass::OperationalExecutive
    )
}

pub(crate) fn is_route_only_contact(title: &str, vantage: &str) -> bool {
    matches!(
        classify(title, vantage),
        RoleClass::CommercialRouter | RoleClass::Router
    )
}

/// Contacts whose useful contribution starts only after an operating problem
/// is confirmed by a human. Public account evidence alone is not enough to ask
/// them for an economic or technical decision.
pub(crate) fn requires_confirmed_problem(title: &str, vantage: &str) -> bool {
    matches!(
        classify(title, vantage),
        RoleClass::EnterpriseExecutive | RoleClass::EconomicLeader | RoleClass::TechnicalEvaluator
    )
}

pub(crate) fn for_title_and_vantage(title: &str, vantage: &str) -> ResponseContract {
    contract(classify(title, vantage))
}

fn classify(title: &str, vantage: &str) -> RoleClass {
    let title = normalized(title);
    let vantage = normalize_vantage(vantage);

    // A learning-placement title is direct evidence that owner-level authority
    // is not established. This override is intentionally narrow: generic terms
    // such as `sales`, `business development`, or `coordinator` may be exactly
    // the right function for a sales-workflow problem and are not penalized.
    if is_learning_role_normalized(&title) || is_people_function_normalized(&title) {
        return RoleClass::Router;
    }

    // These titles carry enough explicit altitude/function evidence to prevent
    // a contradictory model label from turning them into floor-level owners.
    if has_any(
        &title,
        &[" chief executive ", " ceo ", " president ", " founder "],
    ) || is_company_owner_title(&title)
    {
        return RoleClass::EnterpriseExecutive;
    }
    if has_any(
        &title,
        &[
            " chief financial ",
            " cfo ",
            " finance ",
            " financial ",
            " controller ",
            " accounting ",
        ],
    ) {
        return RoleClass::EconomicLeader;
    }

    // A mapped vantage is the primary signal. Title terms refine its operating
    // altitude and prevent, for example, treating a CFO as a floor operator or
    // a business-development contact as the workflow owner.
    match vantage.as_str() {
        "operator" => {
            if has_any(
                &title,
                &["manager", "supervisor", "foreman", "lead", "chief engineer"],
            ) {
                RoleClass::FrontlineLeader
            } else {
                RoleClass::Operator
            }
        }
        "process_owner" => {
            if has_any(
                &title,
                &["manager", "supervisor", "foreman", "lead", "chief engineer"],
            ) && !has_any(&title, &["director", "vice president", "vp", "head of"])
            {
                RoleClass::FrontlineLeader
            } else {
                RoleClass::ProcessOwner
            }
        }
        "operational_executive" => RoleClass::OperationalExecutive,
        "technical_evaluator" => RoleClass::TechnicalEvaluator,
        "economic_buyer" => {
            if has_any(
                &title,
                &["chief executive", "ceo", "president", "founder", "owner"],
            ) {
                RoleClass::EnterpriseExecutive
            } else {
                RoleClass::EconomicLeader
            }
        }
        "router" => {
            if has_any(
                &title,
                &[
                    "business development",
                    "partnership",
                    "sales",
                    "client service",
                    "account executive",
                    "commercial",
                ],
            ) {
                RoleClass::CommercialRouter
            } else {
                RoleClass::Router
            }
        }
        _ => classify_from_title(&title),
    }
}

fn classify_from_title(title: &str) -> RoleClass {
    if has_any(title, &["chief executive", "ceo", "president", "founder"])
        || is_company_owner_title(title)
    {
        RoleClass::EnterpriseExecutive
    } else if has_any(
        title,
        &[
            "chief financial",
            "cfo",
            "finance",
            "financial",
            "controller",
        ],
    ) {
        RoleClass::EconomicLeader
    } else if has_any(
        title,
        &[
            "chief operating",
            "coo",
            "vice president operations",
            "vp operations",
        ],
    ) {
        RoleClass::OperationalExecutive
    } else if has_any(
        title,
        &[
            "sales operations",
            "revenue operations",
            "business operations",
            "commercial operations",
            "client service operations",
        ],
    ) && has_any(title, &["manager", "supervisor", "lead"])
    {
        RoleClass::FrontlineLeader
    } else if has_any(
        title,
        &[
            "sales operations",
            "revenue operations",
            "business operations",
            "commercial operations",
            "client service operations",
        ],
    ) && has_any(title, &["director", "head of", "vice president", " vp "])
    {
        RoleClass::ProcessOwner
    } else if has_any(
        title,
        &[
            "engineer",
            "technology",
            "technical",
            "automation",
            "controls",
            "information technology",
            " it ",
        ],
    ) {
        RoleClass::TechnicalEvaluator
    } else if has_any(
        title,
        &[
            "business development",
            "partnership",
            "sales",
            "client service",
            "account executive",
            "commercial",
        ],
    ) {
        RoleClass::CommercialRouter
    } else if has_any(title, &["operator", "technician", "adjuster", "analyst"]) {
        RoleClass::Operator
    } else if has_any(title, &["manager", "supervisor", "foreman", "lead"]) {
        RoleClass::FrontlineLeader
    } else if has_any(title, &["director", "head of", "vice president", " vp "]) {
        RoleClass::ProcessOwner
    } else {
        RoleClass::Router
    }
}

fn contract(class: RoleClass) -> ResponseContract {
    match class {
        RoleClass::Operator => ResponseContract {
            role_class: class,
            label: "operator",
            question_shape: "Ask whether someone still performs one concrete task manually at one recognizable event; if they engage, ask for one rough example.",
            ask_shape: "One five-second operating answer first; a brief workflow conversation only after the answer earns it.",
            reactance_guard: "Do not imply poor performance, ask for budget or policy judgments, or make them expose a mistake to a stranger.",
        },
        RoleClass::FrontlineLeader => ResponseContract {
            role_class: class,
            label: "frontline_leader",
            question_shape: "Ask whether the task recurs and which concrete step consumes the time; do not make them decode an abstract framework.",
            ask_shape: "One quick operating answer first; a short conversation only after relevance is established.",
            reactance_guard: "Do not audit the team, imply incompetence, or demand internal metrics before the problem is confirmed.",
        },
        RoleClass::ProcessOwner => ResponseContract {
            role_class: class,
            label: "process_owner",
            question_shape: "Ask whether someone still has to pull or reconcile the named records manually when the event occurs.",
            ask_shape: "One direct operating answer first; a 15-minute conversation only after the answer confirms relevance.",
            reactance_guard: "Do not present the workflow gap as fact, prescribe a system before confirmation, or turn the note into a discovery questionnaire.",
        },
        RoleClass::OperationalExecutive => ResponseContract {
            role_class: class,
            label: "operational_executive",
            question_shape: "Ask whether teams still handle the named step manually, or which group handles it; make the question easy to answer or forward.",
            ask_shape: "One answer or internal route first; a short conversation only after interest is clear.",
            reactance_guard: "Do not ask them to reconstruct a detailed incident, pretend they run the daily workflow, or bury the decision in product features.",
        },
        RoleClass::EconomicLeader => ResponseContract {
            role_class: class,
            label: "economic_leader",
            question_shape: "Ask whether the named event still triggers manual reconstruction before a write-off, recovery, or other financial decision, or who handles it.",
            ask_shape: "One answer or internal route first; discuss economics only after the operating task is confirmed.",
            reactance_guard: "Do not claim losses, savings, budget, authority, or workflow ownership from a finance title alone.",
        },
        RoleClass::EnterpriseExecutive => ResponseContract {
            role_class: class,
            label: "enterprise_executive",
            question_shape: "Ask one sharp operating question the executive can answer or forward without decoding; name the event and manual task.",
            ask_shape: "One answer or internal direction first; do not lead with a meeting request.",
            reactance_guard: "Do not ask for a process tour, flatter seniority, overstate urgency, or make the executive do routing research.",
        },
        RoleClass::TechnicalEvaluator => ResponseContract {
            role_class: class,
            label: "technical_evaluator",
            question_shape: "Before problem confirmation, ask whether the named cross-system step is still manual and where the operating decision sits; after confirmation, ask one feasibility question.",
            ask_shape: "One small technical clarification or route first; evaluation only after the operating task is confirmed.",
            reactance_guard: "Do not imply their stack is inadequate, conflate public and private data, or ask them to evaluate a solution to an unconfirmed problem.",
        },
        RoleClass::CommercialRouter => ResponseContract {
            role_class: class,
            label: "commercial_router",
            question_shape: "Ask one routing question: which internal role handles the named event and manual task.",
            ask_shape: "One internal name or role. Never request a meeting or access to a customer.",
            reactance_guard: "Do not ask them to validate private mechanics, expose a customer contact, or own an operational gap.",
        },
        RoleClass::Router => ResponseContract {
            role_class: class,
            label: "router",
            question_shape: "Ask only which role or person owns the named decision.",
            ask_shape: "One routing reply.",
            reactance_guard: "Do not ask for a meeting, a workflow assessment, internal data, or multiple possible actions.",
        },
    }
}

fn normalized(value: &str) -> String {
    format!(
        " {} ",
        value
            .trim()
            .to_ascii_lowercase()
            .replace(['-', '/', ','], " ")
    )
}

fn normalize_vantage(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace([' ', '-'], "_")
}

fn is_learning_role(value: &str) -> bool {
    is_learning_role_normalized(&normalized(value))
}

fn is_learning_role_normalized(value: &str) -> bool {
    has_any(
        value,
        &[
            " intern ",
            " internship ",
            " student ",
            " trainee ",
            " apprentice ",
            " co op ",
            " placement year ",
        ],
    )
}

fn is_people_function(value: &str) -> bool {
    is_people_function_normalized(&normalized(value))
}

fn is_people_function_normalized(value: &str) -> bool {
    has_any(
        value,
        &[
            " human resources ",
            " hr ",
            " people operations ",
            " people and culture ",
            " people & culture ",
            " chief people ",
            " talent acquisition ",
            " recruiter ",
            " recruitment ",
        ],
    )
}

fn is_company_owner_title(value: &str) -> bool {
    matches!(
        value.trim(),
        "owner" | "business owner" | "company owner" | "co owner"
    )
}

fn has_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::{
        contact_priority, effective_primary, effective_vantage, for_title_and_vantage,
        is_route_only_contact, requires_confirmed_problem, RoleClass,
    };

    #[test]
    fn operating_roles_get_different_response_contracts() {
        let operator = for_title_and_vantage("Refrigeration Technician", "operator");
        let supervisor = for_title_and_vantage("Maintenance Manager", "operator");
        let executive = for_title_and_vantage("Chief Operating Officer", "operational_executive");

        assert_eq!(operator.role_class, RoleClass::Operator);
        assert_eq!(supervisor.role_class, RoleClass::FrontlineLeader);
        assert_eq!(executive.role_class, RoleClass::OperationalExecutive);
        assert_ne!(operator.question_shape, executive.question_shape);
        assert_ne!(supervisor.ask_shape, executive.ask_shape);
    }

    #[test]
    fn title_refines_economic_and_routing_roles() {
        assert_eq!(
            for_title_and_vantage("President and CEO", "economic_buyer").role_class,
            RoleClass::EnterpriseExecutive
        );
        assert_eq!(
            for_title_and_vantage("VP Finance", "economic_buyer").role_class,
            RoleClass::EconomicLeader
        );
        assert_eq!(
            for_title_and_vantage("Director of Business Development", "router").role_class,
            RoleClass::CommercialRouter
        );
    }

    #[test]
    fn contract_explicitly_forbids_personality_inference() {
        let block = for_title_and_vantage("Plant Manager", "process_owner").prompt_block();
        assert!(block.contains("never mention the contract or infer private personality"));
        assert!(block.contains("\"ask\""));
        assert!(block.contains("\"avoid\""));
    }

    #[test]
    fn an_intern_cannot_be_promoted_to_process_owner_by_model_output() {
        let contract = for_title_and_vantage("Sales Operations Intern", "process_owner");
        assert_eq!(contract.role_class, RoleClass::Router);
        assert_eq!(
            effective_vantage("Sales Operations Intern", "process_owner"),
            "router"
        );
        assert!(!effective_primary("Sales Operations Intern", true));
        assert!(contract.ask_shape.contains("routing"));
    }

    #[test]
    fn the_real_sales_owner_is_not_penalized_for_working_in_sales() {
        let owner = contact_priority("Director of Sales Operations", "process_owner", false);
        let intern = contact_priority("Sales Intern", "process_owner", true);
        assert!(owner > intern);
        assert_eq!(
            for_title_and_vantage("Director of Sales Operations", "process_owner").role_class,
            RoleClass::ProcessOwner
        );
    }

    #[test]
    fn contradictory_model_output_cannot_turn_a_cfo_into_a_process_owner() {
        assert_eq!(
            for_title_and_vantage("Chief Financial Officer", "process_owner").role_class,
            RoleClass::EconomicLeader
        );
    }

    #[test]
    fn people_function_title_cannot_be_promoted_to_an_operating_owner() {
        let title = "National Health & Safety Manager / HR Liaison";
        assert!(is_route_only_contact(title, "operational_executive"));
        assert_eq!(effective_vantage(title, "operational_executive"), "router");
        assert!(!effective_primary(title, true));
    }

    #[test]
    fn product_owner_is_not_mistaken_for_company_owner() {
        assert_eq!(
            for_title_and_vantage("Product Owner", "process_owner").role_class,
            RoleClass::ProcessOwner
        );
    }

    #[test]
    fn model_primary_is_only_a_tie_breaker_inside_role_relevance() {
        let owner = contact_priority("Director of Claims", "process_owner", false);
        let primary_operator = contact_priority("Claims Analyst", "operator", true);
        assert!(owner > primary_operator);
    }

    #[test]
    fn sales_operations_is_not_demoted_by_a_static_sales_keyword() {
        assert_eq!(
            for_title_and_vantage("Director of Sales Operations", "unknown").role_class,
            RoleClass::ProcessOwner
        );
        assert!(!is_route_only_contact(
            "Director of Sales Operations",
            "unknown"
        ));
    }

    #[test]
    fn economic_and_technical_contacts_wait_for_problem_confirmation() {
        assert!(requires_confirmed_problem(
            "Chief Financial Officer",
            "economic_buyer"
        ));
        assert!(requires_confirmed_problem(
            "Systems Engineer",
            "technical_evaluator"
        ));
        assert!(!requires_confirmed_problem(
            "Claims Operations Manager",
            "process_owner"
        ));
    }
}
