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

pub(crate) fn for_title_and_vantage(title: &str, vantage: &str) -> ResponseContract {
    contract(classify(title, vantage))
}

fn classify(title: &str, vantage: &str) -> RoleClass {
    let title = normalized(title);
    let vantage = normalized(vantage);

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
    if has_any(
        title,
        &["chief executive", "ceo", "president", "founder", "owner"],
    ) {
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
            "engineer",
            "technology",
            "technical",
            "automation",
            "controls",
            "data",
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
            question_shape: "Ask what happens at one recognizable moment or for one rough example; do not ask them to diagnose the whole company.",
            ask_shape: "A short email example or a brief workflow conversation.",
            reactance_guard: "Do not imply poor performance, ask for budget or policy judgments, or make them expose a mistake to a stranger.",
        },
        RoleClass::FrontlineLeader => ResponseContract {
            role_class: class,
            label: "frontline_leader",
            question_shape: "Ask whether the pattern recurs and where the decision becomes difficult; invite one example.",
            ask_shape: "A short conversation with an email explanation as the easier path.",
            reactance_guard: "Do not audit the team, imply incompetence, or demand internal metrics before the problem is confirmed.",
        },
        RoleClass::ProcessOwner => ResponseContract {
            role_class: class,
            label: "process_owner",
            question_shape: "Ask how one decision is made today or whether the hypothesized mechanism is material enough to examine.",
            ask_shape: "A 15–20 minute comparison, with a short email correction as an easier alternative.",
            reactance_guard: "Do not present the workflow gap as fact, prescribe a system before confirmation, or turn the note into a discovery questionnaire.",
        },
        RoleClass::OperationalExecutive => ResponseContract {
            role_class: class,
            label: "operational_executive",
            question_shape: "Ask whether the issue is material across the operation or where ownership sits.",
            ask_shape: "A short strategic conversation or a route to the process owner.",
            reactance_guard: "Do not ask them to reconstruct a detailed incident, pretend they run the daily workflow, or bury the decision in product features.",
        },
        RoleClass::EconomicLeader => ResponseContract {
            role_class: class,
            label: "economic_leader",
            question_shape: "Ask how that event is attributed or reviewed today, or who owns the operating evidence.",
            ask_shape: "A short discussion, with a brief email answer or internal route as easier alternatives.",
            reactance_guard: "Do not claim losses, savings, budget, authority, or workflow ownership from a finance title alone.",
        },
        RoleClass::EnterpriseExecutive => ResponseContract {
            role_class: class,
            label: "enterprise_executive",
            question_shape: "Ask whether the issue merits attention at their level or who should test it operationally.",
            ask_shape: "A brief executive conversation or one internal direction.",
            reactance_guard: "Do not ask for a process tour, flatter seniority, overstate urgency, or make the executive do routing research.",
        },
        RoleClass::TechnicalEvaluator => ResponseContract {
            role_class: class,
            label: "technical_evaluator",
            question_shape: "Before problem confirmation, ask where the operating decision sits; after confirmation, ask one feasibility or data-boundary question.",
            ask_shape: "A small technical clarification or a route to the operating owner; an evaluation only after business relevance is established.",
            reactance_guard: "Do not imply their stack is inadequate, conflate public and private data, or ask them to evaluate a solution to an unconfirmed problem.",
        },
        RoleClass::CommercialRouter => ResponseContract {
            role_class: class,
            label: "commercial_router",
            question_shape: "Ask whether their team becomes involved; if not, ask which internal role owns it.",
            ask_shape: "A short conversation if their team is involved, otherwise one internal name or role. Never request access to a customer.",
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

fn has_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::{for_title_and_vantage, RoleClass};

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
}
