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
use crate::calendar;
use crate::db::{Lead, Person, SharedDb};
use crate::engine::Engine;
use crate::knowledge::Library;
use crate::opportunity::ResearchClient;
use crate::playbook::{Playbook, Shared};
use crate::research;

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

#[derive(Debug, Default, Deserialize)]
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
    client: &Engine,
    apollo: &Apollo,
    pb: &Playbook,
    shared: &Shared,
    fallback_recipient_timezone: &str,
    library: &Library,
    thesis: &str,
    n_accounts: usize,
    n_contacts: usize,
    concurrency: usize,
) -> Result<SourceSummary> {
    let system = pb.system_prompt(shared);

    // 1. thesis → Apollo ICP filters, then hard-clamp sizes to the brand's ceiling
    //    so enterprise giants are never even fetched (belt-and-suspenders vs. the prompt).
    let mut icp = derive_icp(client, &system, pb, thesis).await?;
    icp.employee_ranges = clamp_employee_ranges(icp.employee_ranges, pb.max_employees);
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
            ..Default::default()
        })
        .await?;
    eprintln!("  · Apollo returned {} organizations", orgs.len());
    let mut summary = SourceSummary {
        orgs_found: orgs.len(),
        ..Default::default()
    };

    // Per-company website research (best-effort): grounds qualification and the
    // opening touch in what the company actually does, not Apollo's one-liner.
    // Toggle off with SPRUCE_RESEARCH=0.
    let researcher = if research::enabled() {
        match ResearchClient::from_env() {
            Ok(r) => {
                eprintln!("  · per-company website research: on");
                Some(r)
            }
            Err(_) => None,
        }
    } else {
        None
    };
    let researcher_ref = researcher.as_ref();

    // 3. qualify concurrently; keep the ones that fit, up to n_accounts.
    let quals = stream::iter(orgs.into_iter().map(|org| {
        let system = system.clone();
        let knowledge = library
            .retrieve_stage(
                &format!("qualifying an expensive workflow: {thesis}; {}", pb.motion),
                "companies",
                5,
                2,
            )
            .playbook_block();
        async move {
            // Apollo's company search often returns a sparse payload; enrich thin
            // orgs by domain first so qualification judges real fit, not missing data.
            let org = hydrate_org(apollo, org).await;
            // Winnability gate: a small/founder-led vendor can't realistically land
            // companies far above its size ceiling. Reject those up front (once we
            // actually know the headcount) rather than spending a qualify call.
            let q = if let Some(max) = pb.max_employees.filter(|m| org.estimated_num_employees > *m) {
                Ok(OrgQual {
                    reject_reason: format!(
                        "{} employees is above {}'s ~{}-employee ceiling for a founder-led motion",
                        org.estimated_num_employees, pb.name, max
                    ),
                    ..Default::default()
                })
            } else {
                // Only research companies we're actually going to qualify.
                let research_block = match researcher_ref {
                    Some(r) => research::research_company(client, r, pb, &org)
                        .await
                        .map(|b| b.as_facts_block())
                        .unwrap_or_default(),
                    None => String::new(),
                };
                qualify_org(client, &system, pb, thesis, &org, &knowledge, &research_block).await
            };
            // Report each verdict the moment it lands so this concurrent stage
            // isn't a silent multi-minute spinner.
            match &q {
                Ok(v) if v.qualified => eprintln!("  · ✓ qualified {}", org.name),
                Ok(v) => eprintln!("  · ✗ skip {} — {}", org.name, first_line(&v.reject_reason)),
                Err(e) => eprintln!("  · ! {} qualify error: {e:#}", org.name),
            }
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
        // Verdicts were already logged as they streamed in above.
        let q = match q {
            Ok(q) if q.qualified => q,
            _ => continue,
        };

        let hq = org.hq();
        let lead = Lead {
            brand: pb.key.clone(),
            apollo_org_id: org.id.clone(),
            name: org.name.clone(),
            domain: org.domain(),
            industry: org.industry.clone(),
            timezone: calendar::timezone_for_location(&hq, fallback_recipient_timezone)
                .name()
                .to_string(),
            hq,
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
        db.log_event(
            &pb.key,
            "",
            "",
            "sourced",
            &format!("qualified lead {}", org.name),
        )?;
        kept += 1;
        summary.leads_qualified += 1;

        // 4. real people at this org, mapped to vantage points.
        let added = source_people(
            db,
            client,
            apollo,
            pb,
            &system,
            &org,
            &lead,
            &lead_id,
            &icp,
            n_contacts,
            fallback_recipient_timezone,
        )
        .await?;
        summary.people_added += added;
        eprintln!("  · {} — {added} contact(s)", org.name);
    }

    Ok(summary)
}

/// Fetch real people at an org, assign a vantage to each, and file them.
#[allow(clippy::too_many_arguments)]
async fn source_people(
    db: &SharedDb,
    client: &Engine,
    apollo: &Apollo,
    pb: &Playbook,
    system: &str,
    org: &ApolloOrg,
    lead: &Lead,
    lead_id: &str,
    icp: &Icp,
    n_contacts: usize,
    fallback_recipient_timezone: &str,
) -> Result<usize> {
    // People-search by DOMAIN is far more reliable than by org id (the id from
    // company search is a different record people are frequently NOT indexed
    // under, so `organization_ids` silently returns 0 for many real companies).
    // Run a waterfall — domain+titles → org-id+titles → broaden to any employee
    // → resolve a missing domain by name — topping up until we reach n_contacts,
    // so "5 people per company" holds even for stubborn orgs.
    let people = gather_people(apollo, org, icp, n_contacts).await;
    if people.is_empty() {
        eprintln!("  · no people found for {} (all strategies)", org.name);
        return Ok(0);
    }

    let assignments = assign_vantage(client, system, pb, lead, &people)
        .await
        .unwrap_or(VantageDoc {
            assignments: vec![],
        });

    let mut added = 0usize;
    for (i, ap) in people.iter().enumerate().take(n_contacts) {
        let va = assignments.assignments.iter().find(|a| a.index == i);
        let location = ap.location();
        let timezone_location = if location.is_empty() {
            lead.hq.as_str()
        } else {
            location.as_str()
        };
        let timezone =
            calendar::timezone_for_location(timezone_location, fallback_recipient_timezone)
                .name()
                .to_string();
        let person = Person {
            lead_id: lead_id.to_string(),
            brand: pb.key.clone(),
            apollo_person_id: ap.id.clone(),
            first_name: ap.first_name.clone(),
            last_name: ap.last_name.clone(),
            name: ap.full_name(),
            title: ap.title.clone(),
            location,
            timezone,
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
        db.log_event(
            &pb.key,
            &pid,
            "",
            "sourced",
            &format!("{} @ {}", person.name, org.name),
        )?;
        added += 1;
    }
    Ok(added)
}

/// Collect up to `n_contacts` real people at an org, deduped, preferring
/// title/seniority-matched people but broadening to any employee and falling
/// back across domain/org-id lookups so a company reliably yields contacts even
/// when Apollo's org-id index is empty or the search record has no domain.
async fn gather_people(
    apollo: &Apollo,
    org: &ApolloOrg,
    icp: &Icp,
    n_contacts: usize,
) -> Vec<ApolloPerson> {
    use std::collections::HashSet;

    let want = n_contacts.max(1);
    let over = (want * 2).clamp(5, 50) as u32;

    // People search by domain is far more reliable than by org id; resolve a
    // domain by name if the search record lacks one.
    let mut domain = org.domain();
    if domain.is_empty() {
        domain = resolve_domain(apollo, org).await;
    }

    // Ordered strategies: most targeted first so kept contacts are best-fit, then
    // progressively broader so we still reach the count.
    let mut attempts: Vec<(&str, PeopleFilters)> = Vec::new();
    if !domain.is_empty() {
        attempts.push((
            "domain+titles",
            PeopleFilters {
                organization_domains: vec![domain.clone()],
                titles: icp.titles.clone(),
                seniorities: icp.seniorities.clone(),
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }
    if !org.id.is_empty() {
        attempts.push((
            "org_id+titles",
            PeopleFilters {
                organization_ids: vec![org.id.clone()],
                titles: icp.titles.clone(),
                seniorities: icp.seniorities.clone(),
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }
    if !domain.is_empty() {
        attempts.push((
            "domain-any",
            PeopleFilters {
                organization_domains: vec![domain.clone()],
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }
    if !org.id.is_empty() {
        attempts.push((
            "org_id-any",
            PeopleFilters {
                organization_ids: vec![org.id.clone()],
                page: 1,
                per_page: over,
                ..Default::default()
            },
        ));
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<ApolloPerson> = Vec::new();
    for (label, filters) in attempts {
        if out.len() >= want {
            break;
        }
        match apollo.search_people(&filters).await {
            Ok(people) => {
                for p in people {
                    if p.full_name().trim().is_empty() {
                        continue;
                    }
                    let key = if !p.id.is_empty() {
                        p.id.clone()
                    } else {
                        format!("{}|{}", p.full_name().to_lowercase(), p.title.to_lowercase())
                    };
                    if seen.insert(key) {
                        out.push(p);
                    }
                }
            }
            Err(e) => eprintln!("  · people search ({label}) failed for {}: {e:#}", org.name),
        }
    }
    out.truncate(want);
    out
}

/// Best-effort domain resolution for an org whose search record has none: search
/// Apollo organizations by name and take the first close match that has a domain.
async fn resolve_domain(apollo: &Apollo, org: &ApolloOrg) -> String {
    if org.name.trim().is_empty() {
        return String::new();
    }
    let found = apollo
        .search_organizations(&OrgFilters {
            name: org.name.clone(),
            page: 1,
            per_page: 5,
            ..Default::default()
        })
        .await
        .unwrap_or_default();
    let want = org.name.to_lowercase();
    found
        .into_iter()
        .filter(|o| {
            let n = o.name.to_lowercase();
            !o.domain().is_empty() && (n == want || n.contains(&want) || want.contains(&n))
        })
        .map(|o| o.domain())
        .next()
        .unwrap_or_default()
}

// --- Claude calls ----------------------------------------------------------

async fn derive_icp(client: &Engine, system: &str, pb: &Playbook, thesis: &str) -> Result<Icp> {
    let mut firmographic = String::new();
    if let Some(max) = pb.max_employees {
        firmographic.push_str(&format!(
            " Only target companies with at most {max} employees — this brand is a small, \
             founder-led vendor that cannot realistically land larger enterprises, so pick \
             employee_ranges buckets at or below that ceiling."
        ));
    }
    if !pb.icp_note.trim().is_empty() {
        firmographic.push(' ');
        firmographic.push_str(pb.icp_note.trim());
    }
    let user = format!(
        "Translate this outreach thesis into an Apollo.io search. The brand's motion is: {motion}.\n\n\
         THESIS: {thesis}\n\n\
         Return Apollo filters that will surface companies plausibly having this expensive workflow, \
         and the job titles/seniorities of the people who would OWN or OBSERVE it (by vantage, not \
         just seniority). Keep keywords concrete and industry-specific. Employee ranges must use \
         Apollo's bucket format like \"51,200\".{firmographic} If the thesis implies a region, set locations.",
        motion = pb.motion,
    );
    client.structured::<Icp>(system, &user, icp_schema()).await
}

async fn qualify_org(
    client: &Engine,
    system: &str,
    pb: &Playbook,
    thesis: &str,
    org: &ApolloOrg,
    knowledge: &str,
    research: &str,
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
    let research_block = if research.trim().is_empty() {
        String::new()
    } else {
        format!("{}\n\n", research.trim())
    };
    let user = format!(
        "Decide whether this REAL company (from Apollo) fits the thesis, and if so frame the \
         doctrine fields. THESIS: {thesis}\n\nAPOLLO FACTS (the ONLY things you may state as fact):\n{facts}\n\n{research_block}{knowledge}\n\n\
         Rules: observed_facts must each be supported by the Apollo facts OR the website research \
         above — never invent a customer, metric, or dollar figure. Put every reasonable-but-unproven \
         guess in inferences. consequence_metric is a measurable consequence, NOT dollars. If at \
         least {min} independent signals don't support the hypothesis, set qualified=false with a \
         one-line reject_reason.",
        facts = serde_json::to_string_pretty(&facts).unwrap_or_default(),
        min = pb.min_signals,
    );
    client
        .structured::<OrgQual>(system, &user, qual_schema())
        .await
}

async fn assign_vantage(
    client: &Engine,
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
    client
        .structured::<VantageDoc>(system, &user, vantage_schema())
        .await
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
        "process_owner"
        | "operator"
        | "operational_executive"
        | "technical_evaluator"
        | "economic_buyer"
        | "router" => v,
        "" => "process_owner".to_string(),
        _ => v,
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(80).collect()
}

/// Drop Apollo size buckets whose lower bound exceeds the brand's headcount
/// ceiling, so enterprise giants are never fetched. If the model returned only
/// oversized buckets, fall back to every standard bucket within the ceiling —
/// never leaving the size filter empty, which would let giants back in.
fn clamp_employee_ranges(ranges: Vec<String>, max_employees: Option<i64>) -> Vec<String> {
    let Some(max) = max_employees else {
        return ranges;
    };
    let within = |bucket: &str| -> bool {
        bucket
            .split(',')
            .next()
            .and_then(|lo| lo.trim().parse::<i64>().ok())
            .map(|lo| lo <= max)
            .unwrap_or(true)
    };
    let kept: Vec<String> = ranges.into_iter().filter(|b| within(b)).collect();
    if !kept.is_empty() {
        return kept;
    }
    const STD: [&str; 8] = [
        "1,10", "11,50", "51,200", "201,500", "501,1000", "1001,5000", "5001,10000",
        "10001,1000000",
    ];
    STD.iter()
        .filter(|b| within(b))
        .map(|s| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::clamp_employee_ranges;

    #[test]
    fn clamp_drops_buckets_above_the_ceiling() {
        let ranges = vec![
            "201,500".to_string(),
            "501,1000".to_string(),
            "1001,5000".to_string(),
            "10001,1000000".to_string(),
        ];
        let kept = clamp_employee_ranges(ranges, Some(1000));
        assert_eq!(kept, vec!["201,500".to_string(), "501,1000".to_string()]);
    }

    #[test]
    fn clamp_falls_back_to_standard_buckets_when_all_oversized() {
        // Model returned only enterprise buckets → we still emit the in-ceiling set.
        let kept = clamp_employee_ranges(vec!["10001,1000000".to_string()], Some(1000));
        assert_eq!(
            kept,
            vec![
                "1,10".to_string(),
                "11,50".to_string(),
                "51,200".to_string(),
                "201,500".to_string(),
                "501,1000".to_string()
            ]
        );
    }

    #[test]
    fn clamp_is_a_noop_without_a_ceiling() {
        let ranges = vec!["10001,1000000".to_string()];
        assert_eq!(clamp_employee_ranges(ranges.clone(), None), ranges);
    }
}

/// True when an org's search payload lacks the firmographics qualification needs
/// (industry, headcount, description, keywords) — i.e. it's name+domain only.
fn org_is_thin(o: &ApolloOrg) -> bool {
    o.industry.trim().is_empty()
        && o.short_description.trim().is_empty()
        && o.keywords.is_empty()
        && o.estimated_num_employees == 0
}

/// Fill in a thin org's firmographics via Apollo organization enrichment (by
/// domain). Falls back to the original record if it's already rich, has no
/// domain, or enrichment fails/returns nothing — so sourcing never regresses.
async fn hydrate_org(apollo: &Apollo, org: ApolloOrg) -> ApolloOrg {
    if !org_is_thin(&org) {
        return org;
    }
    let domain = org.domain();
    if domain.is_empty() {
        return org;
    }
    match apollo.enrich_organization(&domain).await {
        Ok(Some(full)) if !org_is_thin(&full) => full,
        _ => org,
    }
}
