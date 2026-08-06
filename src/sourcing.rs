//! Sourcing: turn a thesis into *real* leads and people from Apollo.
//!
//! The original pipeline asked Claude to invent companies. This asks Claude only
//! to (a) translate the thesis into an Apollo ICP query and (b) *qualify and
//! frame* the real rows Apollo returns — it never invents the company or the
//! person. The doctrine (observed fact vs. inference vs. hypothesis) is enforced
//! by grounding every fact in the Apollo payload we pass in.
//!
//! Flow:
//!   1. derive_icp  — thesis + brand motion → Apollo filters (keywords, sizes,
//!      locations, titles, seniorities)
//!   2. org search  — real companies
//!   3. qualify     — per org, Claude decides fit and writes the doctrine fields
//!      grounded ONLY in the org's real facts
//!   4. people search + vantage — real people at the org, mapped to a vantage
//!   5. upsert into the SQLite spine

use anyhow::Result;
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::apollo::{Apollo, ApolloOrg, ApolloPerson, OrgFilters, PeopleFilters};
use crate::db::{Lead, Person, SharedDb};
use crate::engine::Claude;
use crate::knowledge::Library;
use crate::playbook::{Playbook, Shared};

/// What one `source` run accomplished.
#[derive(Debug, Default)]
pub struct SourceSummary {
    pub orgs_found: usize,
    pub leads_qualified: usize,
    pub people_added: usize,
}

// --- Structured-output shapes ---------------------------------------------

#[derive(Debug, Deserialize)]
struct Icp {
    #[serde(default)]
    keywords: Vec<String>,
    #[serde(default)]
    employee_ranges: Vec<String>,
    #[serde(default)]
    locations: Vec<String>,
    #[serde(default)]
    titles: Vec<String>,
    #[serde(default)]
    seniorities: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct OrgQual {
    qualified: bool,
    #[serde(default)]
    reject_reason: String,
    #[serde(default)]
    observed_facts: Vec<String>,
    #[serde(default)]
    inferences: Vec<String>,
    #[serde(default)]
    hypothesis: String,
    #[serde(default)]
    mechanism: String,
    #[serde(default)]
    consequence_metric: String,
    #[serde(default)]
    signals: Vec<String>,
    #[serde(default)]
    system_concept: String,
    #[serde(default)]
    hard_buyer_question: String,
    #[serde(default)]
    kill_condition: String,
    #[serde(default)]
    magnitude_note: String,
    #[serde(default)]
    applied_principles: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct VantageDoc {
    #[serde(default)]
    assignments: Vec<VantageAssignment>,
}

#[derive(Debug, Deserialize)]
struct VantageAssignment {
    index: usize,
    #[serde(default)]
    vantage: String,
    #[serde(default)]
    can_observe: String,
    #[serde(default)]
    why_them: String,
    #[serde(default)]
    primary: bool,
    #[serde(default)]
    route_to: String,
}

/// Run the full sourcing pass and file everything into the db.
#[allow(clippy::too_many_arguments)]
pub async fn source(
    db: &SharedDb,
    client: &Claude,
    apollo: &Apollo,
    pb: &Playbook,
    shared: &Shared,
    library: &Library,
    thesis: &str,
    n_accounts: usize,
    n_contacts: usize,
    concurrency: usize,
) -> Result<SourceSummary> {
    let system = pb.system_prompt(shared);

    // 1. thesis → Apollo ICP filters.
    let icp = derive_icp(client, &system, pb, thesis).await?;
    eprintln!(
        "  · ICP: {} keyword(s), titles [{}], sizes [{}]",
        icp.keywords.len(),
        icp.titles.join(", "),
        icp.employee_ranges.join(", "),
    );

    // 2. real organizations (overfetch so qualification can be selective).
    let orgs = apollo
        .search_organizations(&OrgFilters {
            keywords: icp.keywords.clone(),
            employee_ranges: icp.employee_ranges.clone(),
            locations: icp.locations.clone(),
            page: 1,
            per_page: (n_accounts * 2).clamp(10, 100) as u32,
        })
        .await?;
    eprintln!("  · Apollo returned {} organizations", orgs.len());
    let mut summary = SourceSummary { orgs_found: orgs.len(), ..Default::default() };

    // 3. qualify concurrently; keep the ones that fit, up to n_accounts.
    let quals = stream::iter(orgs.into_iter().map(|org| {
        let system = system.clone();
        let knowledge = library
            .retrieve_stage(&format!("qualifying an expensive workflow: {thesis}; {}", pb.motion), "companies", 5, 2)
            .playbook_block();
        async move {
            let q = qualify_org(client, &system, pb, thesis, &org, &knowledge).await;
            (org, q)
        }
    }))
    .buffered(concurrency)
    .collect::<Vec<_>>()
    .await;

    let mut kept = 0usize;
    for (org, q) in quals {
        if kept >= n_accounts {
            break;
        }
        let q = match q {
            Ok(q) if q.qualified => q,
            Ok(q) => {
                eprintln!("  · skip {} — {}", org.name, first_line(&q.reject_reason));
                continue;
            }
            Err(e) => {
                eprintln!("  · qualify failed for {}: {e:#}", org.name);
                continue;
            }
        };

        let lead = Lead {
            brand: pb.key.clone(),
            apollo_org_id: org.id.clone(),
            name: org.name.clone(),
            domain: org.domain(),
            industry: org.industry.clone(),
            hq: org.hq(),
            headcount: org.estimated_num_employees,
            revenue: org.annual_revenue_printed.clone(),
            thesis: thesis.to_string(),
            hypothesis: q.hypothesis,
            mechanism: q.mechanism,
            consequence_metric: q.consequence_metric,
            system_concept: q.system_concept,
            hard_buyer_question: q.hard_buyer_question,
            kill_condition: q.kill_condition,
            observed_facts: q.observed_facts,
            inferences: q.inferences,
            signals: q.signals,
            magnitude_note: q.magnitude_note,
            applied_principles: q.applied_principles,
            status: "qualified".into(),
            ..Default::default()
        };
        let lead_id = db.upsert_lead(&lead)?;
        db.log_event(&pb.key, "", "", "sourced", &format!("qualified lead {}", org.name))?;
        kept += 1;
        summary.leads_qualified += 1;

        // 4. real people at this org, mapped to vantage points.
        let added = source_people(db, client, apollo, pb, &system, &org, &lead, &lead_id, &icp, n_contacts).await?;
        summary.people_added += added;
        eprintln!("  · {} — {added} contact(s)", org.name);
    }

    Ok(summary)
}

/// Fetch real people at an org, assign a vantage to each, and file them.
#[allow(clippy::too_many_arguments)]
async fn source_people(
    db: &SharedDb,
    client: &Claude,
    apollo: &Apollo,
    pb: &Playbook,
    system: &str,
    org: &ApolloOrg,
    lead: &Lead,
    lead_id: &str,
    icp: &Icp,
    n_contacts: usize,
) -> Result<usize> {
    let people = apollo
        .search_people(&PeopleFilters {
            organization_ids: vec![org.id.clone()],
            titles: icp.titles.clone(),
            seniorities: icp.seniorities.clone(),
            locations: vec![],
            page: 1,
            per_page: (n_contacts * 2).clamp(5, 50) as u32,
        })
        .await
        .unwrap_or_default();

    if people.is_empty() {
        return Ok(0);
    }

    let assignments = assign_vantage(client, system, pb, lead, &people)
        .await
        .unwrap_or(VantageDoc { assignments: vec![] });

    let mut added = 0usize;
    for (i, ap) in people.iter().enumerate().take(n_contacts) {
        let va = assignments.assignments.iter().find(|a| a.index == i);
        let person = Person {
            lead_id: lead_id.to_string(),
            brand: pb.key.clone(),
            apollo_person_id: ap.id.clone(),
            first_name: ap.first_name.clone(),
            last_name: ap.last_name.clone(),
            name: ap.full_name(),
            title: ap.title.clone(),
            vantage: normalize_vantage(va.map(|v| v.vantage.as_str()).unwrap_or("")),
            can_observe: va.map(|v| v.can_observe.clone()).unwrap_or_default(),
            why_them: va.map(|v| v.why_them.clone()).unwrap_or_default(),
            primary: va.map(|v| v.primary).unwrap_or(false),
            route_to: va.map(|v| v.route_to.clone()).unwrap_or_default(),
            linkedin_url: ap.linkedin_url.clone(),
            // Search results are masked; email is filled later by enrichment.
            email: ap.email.clone(),
            email_status: map_email_status(&ap.email_status),
            phone: ap.best_phone(),
            status: "new".into(),
            ..Default::default()
        };
        let pid = db.upsert_person(&person)?;
        db.log_event(&pb.key, &pid, "", "sourced", &format!("{} @ {}", person.name, org.name))?;
        added += 1;
    }
    Ok(added)
}

// --- Claude calls ----------------------------------------------------------

async fn derive_icp(client: &Claude, system: &str, pb: &Playbook, thesis: &str) -> Result<Icp> {
    let user = format!(
        "Translate this outreach thesis into an Apollo.io search. The brand's motion is: {motion}.\n\n\
         THESIS: {thesis}\n\n\
         Return Apollo filters that will surface companies plausibly having this expensive workflow, \
         and the job titles/seniorities of the people who would OWN or OBSERVE it (by vantage, not \
         just seniority). Keep keywords concrete and industry-specific. Employee ranges must use \
         Apollo's bucket format like \"51,200\". If the thesis implies a region, set locations.",
        motion = pb.motion,
    );
    client.structured::<Icp>(system, &user, icp_schema()).await
}

async fn qualify_org(
    client: &Claude,
    system: &str,
    pb: &Playbook,
    thesis: &str,
    org: &ApolloOrg,
    knowledge: &str,
) -> Result<OrgQual> {
    let facts = json!({
        "name": org.name,
        "domain": org.domain(),
        "industry": org.industry,
        "headquarters": org.hq(),
        "estimated_employees": org.estimated_num_employees,
        "annual_revenue": org.annual_revenue_printed,
        "description": org.short_description,
        "keywords": org.keywords,
        "technologies": org.technology_names,
    });
    let user = format!(
        "Decide whether this REAL company (from Apollo) fits the thesis, and if so frame the \
         doctrine fields. THESIS: {thesis}\n\nAPOLLO FACTS (the ONLY things you may state as fact):\n{facts}\n\n{knowledge}\n\n\
         Rules: observed_facts must each be supported by the Apollo facts above — never invent a \
         customer, metric, or dollar figure. Put every reasonable-but-unproven guess in inferences. \
         consequence_metric is a measurable consequence, NOT dollars. If at least {min} independent \
         signals don't support the hypothesis, set qualified=false with a one-line reject_reason.",
        facts = serde_json::to_string_pretty(&facts).unwrap_or_default(),
        min = pb.min_signals,
    );
    client.structured::<OrgQual>(system, &user, qual_schema()).await
}

async fn assign_vantage(
    client: &Claude,
    system: &str,
    pb: &Playbook,
    lead: &Lead,
    people: &[ApolloPerson],
) -> Result<VantageDoc> {
    let roster: Vec<Value> = people
        .iter()
        .enumerate()
        .map(|(i, p)| json!({ "index": i, "name": p.full_name(), "title": p.title, "seniority": p.seniority, "departments": p.departments }))
        .collect();
    let user = format!(
        "These are REAL people at {company}. The hypothesis we're testing: {hyp}\n\nROSTER:\n{roster}\n\n\
         For each person, assign the vantage point that best fits what they can observe/decide/route \
         (not their seniority), one narrow sentence of can_observe, one sentence why_them, whether \
         they are the primary first contact, and route_to if they're a router. Vantage notes for this \
         brand:\n{notes}",
        company = lead.name,
        hyp = lead.hypothesis,
        roster = serde_json::to_string_pretty(&roster).unwrap_or_default(),
        notes = pb.vantage_notes.join("\n"),
    );
    client.structured::<VantageDoc>(system, &user, vantage_schema()).await
}

// --- Schemas ---------------------------------------------------------------

fn str_array(desc: &str) -> Value {
    json!({ "type": "array", "items": { "type": "string" }, "description": desc })
}

fn icp_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["keywords", "titles"],
        "properties": {
            "keywords": str_array("Industry/ICP keyword tags for Apollo org search."),
            "employee_ranges": {
                "type": "array",
                "items": { "type": "string", "enum": ["1,10","11,50","51,200","201,500","501,1000","1001,5000","5001,10000","10001,1000000"] },
                "description": "Apollo headcount buckets."
            },
            "locations": str_array("HQ locations, e.g. 'Canada'."),
            "titles": str_array("Job titles of the people who own/observe the workflow."),
            "seniorities": {
                "type": "array",
                "items": { "type": "string", "enum": ["owner","founder","c_suite","partner","vp","head","director","manager","senior","entry","intern"] }
            }
        }
    })
}

fn qual_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["qualified", "hypothesis", "mechanism", "consequence_metric"],
        "properties": {
            "qualified": { "type": "boolean" },
            "reject_reason": { "type": "string" },
            "observed_facts": str_array("Facts supported by the Apollo payload ONLY."),
            "inferences": str_array("Reasonable but unproven guesses."),
            "hypothesis": { "type": "string" },
            "mechanism": { "type": "string" },
            "consequence_metric": { "type": "string", "description": "Measurable consequence, never dollars." },
            "signals": str_array("Independent signals making the hypothesis plausible."),
            "system_concept": { "type": "string" },
            "hard_buyer_question": { "type": "string" },
            "kill_condition": { "type": "string" },
            "magnitude_note": { "type": "string", "description": "Internal-only; never buyer-facing." },
            "applied_principles": str_array("[id]s of book-library principles applied.")
        }
    })
}

fn vantage_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["assignments"],
        "properties": {
            "assignments": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["index", "vantage"],
                    "properties": {
                        "index": { "type": "integer" },
                        "vantage": { "type": "string", "enum": ["process_owner","operator","operational_executive","technical_evaluator","economic_buyer","router"] },
                        "can_observe": { "type": "string" },
                        "why_them": { "type": "string" },
                        "primary": { "type": "boolean" },
                        "route_to": { "type": "string" }
                    }
                }
            }
        }
    })
}

// --- helpers ---------------------------------------------------------------

/// Map Apollo's email_status vocabulary onto ours (verified is the send gate).
fn map_email_status(s: &str) -> String {
    match s.trim().to_lowercase().as_str() {
        "verified" => "verified",
        "likely_to_engage" | "guessed" | "unverified" => "unverified",
        "unavailable" | "" => "unknown",
        _ => "unknown",
    }
    .to_string()
}

fn normalize_vantage(raw: &str) -> String {
    let v = raw.trim().to_lowercase().replace([' ', '-'], "_");
    match v.as_str() {
        "process_owner" | "operator" | "operational_executive" | "technical_evaluator"
        | "economic_buyer" | "router" => v,
        "" => "process_owner".to_string(),
        _ => v,
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(80).collect()
}
