//! Enrichment: reveal real, verified contact details for sourced people.
//!
//! Sourcing files people with masked/empty emails (that's all Apollo search
//! returns). This step calls Apollo's People Enrichment (`people/match`) to
//! reveal the actual email + phone, then runs [`crate::verify`] over it so only
//! genuinely deliverable addresses become send-eligible. Costs Apollo credits,
//! so it's a separate, explicit pass.

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::apollo::{Apollo, ApolloPerson};
use crate::db::{Lead, Person, SharedDb};
use crate::verify;

#[derive(Debug, Default)]
pub struct EnrichSummary {
    pub attempted: usize,
    pub emails_found: usize,
    pub verified: usize,
    /// Apollo credits consumed this pass (one per returned match).
    pub credits_spent: usize,
    /// Set when the pass halted early (e.g. Apollo out of credits).
    pub stopped: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EnrichProgressUpdate {
    pub key: String,
    pub title: String,
    pub detail: String,
    /// active | complete | warning | failed
    pub status: String,
}

pub type EnrichProgressReporter = Arc<dyn Fn(EnrichProgressUpdate) + Send + Sync>;

fn report_enrich(
    reporter: Option<&EnrichProgressReporter>,
    title: impl Into<String>,
    detail: impl Into<String>,
    status: &str,
) {
    if let Some(reporter) = reporter {
        reporter(EnrichProgressUpdate {
            key: "contacts".into(),
            title: title.into(),
            detail: detail.into(),
            status: status.into(),
        });
    }
}

/// Enrich up to `limit` not-yet-enriched people for `brand` (or all brands).
pub async fn enrich_pending(
    db: &SharedDb,
    apollo: &Apollo,
    brand: Option<&str>,
    limit: usize,
    reveal_phone: bool,
    only_person_ids: Option<&HashSet<String>>,
    progress: Option<EnrichProgressReporter>,
) -> Result<EnrichSummary> {
    let people = db.list_people(brand, Some("new"))?;
    let mut summary = EnrichSummary::default();

    // Cache lead domains so we can match by name+domain when Apollo has no id.
    let leads = db.list_leads(brand)?;
    let domain_for = |lead_id: &str| -> String {
        leads
            .iter()
            .find(|l: &&Lead| l.id == lead_id)
            .map(|l| l.domain.clone())
            .unwrap_or_default()
    };

    // Paid reveal credits should follow the active opportunity map, not the
    // insertion order of every historical contact in the CRM. Build a
    // round-robin order so each current easy/medium opportunity gets its best
    // workflow-side committee member before we deepen any one account. Hard
    // research opportunities and unmapped legacy people remain available as a
    // fallback, but cannot consume the front of a normal enrichment batch.
    let priority_order = current_opportunity_enrichment_order(db, brand)?;
    let batch = select_enrichment_batch(people, limit, only_person_ids, &priority_order);
    let total = batch.len();
    report_enrich(
        progress.as_ref(),
        "Revealing and verifying contacts",
        format!("0/{total} processed · up to {total} Apollo reveal credits"),
        "active",
    );
    if total > 0 && progress.is_none() {
        eprintln!("  · enriching {total} people (~{total} Apollo credits, 1 per reveal)…");
    }
    let mut no_email = 0usize;
    let mut failed = 0usize;
    for (i, mut p) in batch.into_iter().enumerate() {
        summary.attempted += 1;
        let who = if p.name.trim().is_empty() {
            format!("person {}", i + 1)
        } else {
            p.name.clone()
        };
        let domain = domain_for(&p.lead_id);
        let matched = apollo
            .enrich_person(
                &p.apollo_person_id,
                &p.first_name,
                &p.last_name,
                &domain,
                reveal_phone,
            )
            .await;

        let matched = match matched {
            Ok(m) => m,
            Err(e) => {
                let msg = e.to_string();
                if is_quota_error(&msg) {
                    report_enrich(
                        progress.as_ref(),
                        "Enrichment stopped by Apollo quota",
                        format!(
                            "{}/{total} processed · {} verified · {} no email · {} errors\n{}",
                            i + 1,
                            summary.verified,
                            no_email,
                            failed,
                            first_line(&msg)
                        ),
                        "warning",
                    );
                    if progress.is_none() {
                        eprintln!(
                            "  · [{}/{total}] stopping early — Apollo out of credits / quota: {}",
                            i + 1,
                            first_line(&msg)
                        );
                    }
                    db.log_event(
                        &p.brand,
                        &p.id,
                        "",
                        "error",
                        "apollo out of credits — enrich stopped",
                    )?;
                    summary.stopped = Some("Apollo out of credits / rate quota".into());
                    break;
                }
                failed += 1;
                report_enrich(
                    progress.as_ref(),
                    "Revealing and verifying contacts",
                    format!(
                        "{}/{total} processed · {} verified · {no_email} no email · {failed} errors\nLatest: {who} · {}",
                        i + 1,
                        summary.verified,
                        first_line(&msg)
                    ),
                    "active",
                );
                if progress.is_none() {
                    eprintln!(
                        "  · [{}/{total}] {who} — enrich failed: {}",
                        i + 1,
                        first_line(&msg)
                    );
                }
                db.log_event(&p.brand, &p.id, "", "error", &format!("enrich failed: {e}"))?;
                continue;
            }
        };
        // A returned match consumes one Apollo credit.
        summary.credits_spent += 1;

        let email = matched.email.trim().to_string();
        let phone = if matched.best_phone().is_empty() {
            p.phone.clone()
        } else {
            matched.best_phone()
        };
        let lead = leads.iter().find(|lead| lead.id == p.lead_id);
        if lead.is_none_or(|lead| !enrichment_matches_account(lead, &matched)) {
            failed += 1;
            db.set_person_email(&p.id, &email, "invalid", &phone)?;
            db.set_person_status(&p.id, "held")?;
            let matched_employer = matched
                .organization
                .as_ref()
                .map(|organization| format!("{} ({})", organization.name, organization.domain()))
                .unwrap_or_else(|| "unknown employer".into());
            let expected = lead
                .map(|lead| format!("{} ({})", lead.name, lead.domain))
                .unwrap_or_else(|| "missing CRM account".into());
            let detail = format!(
                "identity mismatch: Apollo returned {matched_employer}; expected {expected}"
            );
            report_enrich(
                progress.as_ref(),
                "Revealing and verifying contacts",
                format!(
                    "{}/{total} processed · {} verified · {no_email} no email · {failed} errors\nLatest: {who} · {detail}",
                    i + 1,
                    summary.verified,
                ),
                "active",
            );
            if progress.is_none() {
                eprintln!("  · [{}/{total}] {who} — held: {detail}", i + 1);
            }
            db.log_event(&p.brand, &p.id, "", "identity_mismatch", &detail)?;
            continue;
        }

        merge_enriched_identity(&mut p, &matched);
        db.upsert_person(&p)?;

        if email.is_empty() {
            no_email += 1;
            report_enrich(
                progress.as_ref(),
                "Revealing and verifying contacts",
                format!(
                    "{}/{total} processed · {} verified · {no_email} no email · {failed} errors\nLatest: {who} · no email revealed",
                    i + 1,
                    summary.verified,
                ),
                "active",
            );
            if progress.is_none() {
                eprintln!("  · [{}/{total}] {who} — no email revealed", i + 1);
            }
            db.log_event(&p.brand, &p.id, "", "enriched", "no email revealed")?;
            db.set_person_status(&p.id, "enriched")?;
            continue;
        }
        summary.emails_found += 1;

        let verdict = verify::verify_email(&email, &matched.email_status).await;
        db.set_person_email(&p.id, &email, verdict.as_str(), &phone)?;
        db.log_event(
            &p.brand,
            &p.id,
            "",
            "verified",
            &format!("{email} → {}", verdict.as_str()),
        )?;
        let mark = if verdict == verify::EmailVerdict::Verified {
            summary.verified += 1;
            "✓"
        } else {
            "·"
        };
        report_enrich(
            progress.as_ref(),
            "Revealing and verifying contacts",
            format!(
                "{}/{total} processed · {} verified · {no_email} no email · {failed} errors\nLatest: {mark} {who} · {}",
                i + 1,
                summary.verified,
                verdict.as_str(),
            ),
            "active",
        );
        if progress.is_none() {
            eprintln!(
                "  · [{}/{total}] {mark} {who} — {email} ({})",
                i + 1,
                verdict.as_str()
            );
        }

        // Respect suppression immediately: if this address is on the list, park it.
        if db.is_suppressed(&p.brand, &email)? {
            db.set_person_status(&p.id, "suppressed")?;
            db.log_event(&p.brand, &p.id, "", "suppressed", "on suppression list")?;
        }
    }

    report_enrich(
        progress.as_ref(),
        "Enriched contacts",
        format!(
            "{} attempted · {} emails found · {} verified · {} no email · {} errors",
            summary.attempted, summary.emails_found, summary.verified, no_email, failed
        ),
        if summary.stopped.is_some() {
            "warning"
        } else {
            "complete"
        },
    );

    Ok(summary)
}

/// Search results are intentionally masked, while the paid enrichment response
/// can contain the full surname, profile URL, and location. Preserve the mapped
/// workflow vantage but hydrate those identity fields so later selection can
/// choose the local owner and the CRM does not display a half-name.
fn merge_enriched_identity(person: &mut Person, matched: &ApolloPerson) {
    if !matched.first_name.trim().is_empty() {
        person.first_name = matched.first_name.trim().to_string();
    }
    if !matched.last_name.trim().is_empty() {
        person.last_name = matched.last_name.trim().to_string();
    }
    let name = matched.full_name();
    if !name.trim().is_empty() {
        person.name = name.trim().to_string();
    }
    if !matched.title.trim().is_empty() {
        person.title = matched.title.trim().to_string();
    }
    if !matched.linkedin_url.trim().is_empty() {
        person.linkedin_url = matched.linkedin_url.trim().to_string();
    }
    let location = matched.location();
    if !location.trim().is_empty() {
        person.location = location;
    }
}

fn enrichment_matches_account(lead: &Lead, matched: &ApolloPerson) -> bool {
    let expected_org_id = lead.apollo_org_id.trim();
    let matched_org_id = if matched.organization_id.trim().is_empty() {
        matched
            .organization
            .as_ref()
            .map(|organization| organization.id.trim())
            .unwrap_or("")
    } else {
        matched.organization_id.trim()
    };
    if !expected_org_id.is_empty()
        && !matched_org_id.is_empty()
        && expected_org_id == matched_org_id
    {
        return true;
    }

    let expected_domain = normalized_domain(&lead.domain);
    let employer_domain = matched
        .organization
        .as_ref()
        .map(|organization| normalized_domain(&organization.domain()))
        .unwrap_or_default();
    if domains_related(&expected_domain, &employer_domain) {
        return true;
    }

    let email_domain = matched
        .email
        .rsplit_once('@')
        .map(|(_, domain)| normalized_domain(domain))
        .unwrap_or_default();
    // Apollo occasionally omits organization data from a valid match. In that
    // case a company-domain email is acceptable; a different employer/domain
    // is held for explicit identity resolution.
    matched_org_id.is_empty()
        && employer_domain.is_empty()
        && domains_related(&expected_domain, &email_domain)
}

fn normalized_domain(value: &str) -> String {
    value
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("www.")
        .split('/')
        .next()
        .unwrap_or("")
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn domains_related(left: &str, right: &str) -> bool {
    !left.is_empty()
        && !right.is_empty()
        && (left == right
            || left.ends_with(&format!(".{right}"))
            || right.ends_with(&format!(".{left}")))
}

fn select_enrichment_batch(
    people: Vec<Person>,
    limit: usize,
    only_person_ids: Option<&HashSet<String>>,
    priority_order: &[String],
) -> Vec<Person> {
    let mut eligible = people
        .into_iter()
        .enumerate()
        .filter(|(_, person)| {
            only_person_ids.is_none_or(|person_ids| person_ids.contains(&person.id))
        })
        .map(|(index, person)| (person.id.clone(), (index, person)))
        .collect::<HashMap<_, _>>();
    let mut selected = Vec::with_capacity(limit.min(eligible.len()));
    for person_id in priority_order {
        if selected.len() >= limit {
            break;
        }
        if let Some((_, person)) = eligible.remove(person_id) {
            selected.push(person);
        }
    }
    if selected.len() < limit {
        let mut fallback = eligible.into_values().collect::<Vec<_>>();
        fallback.sort_by(|left, right| {
            crate::response_design::contact_priority(
                &right.1.title,
                &right.1.vantage,
                right.1.primary,
            )
            .cmp(&crate::response_design::contact_priority(
                &left.1.title,
                &left.1.vantage,
                left.1.primary,
            ))
            .then_with(|| left.0.cmp(&right.0))
        });
        selected.extend(
            fallback
                .into_iter()
                .map(|(_, person)| person)
                .take(limit - selected.len()),
        );
    }
    selected
}

fn current_opportunity_enrichment_order(db: &SharedDb, brand: Option<&str>) -> Result<Vec<String>> {
    let brands = match brand {
        Some(brand) => vec![brand.to_string()],
        None => db
            .list_leads(None)?
            .into_iter()
            .map(|lead| lead.brand)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect(),
    };
    let people = db
        .list_people(brand, Some("new"))?
        .into_iter()
        .map(|person| (person.id.clone(), person))
        .collect::<HashMap<_, _>>();
    let mut opportunity_rosters = Vec::new();
    for brand in brands {
        let Some(play) = db.current_gtm_play(&brand)? else {
            continue;
        };
        for opportunity in db
            .list_sales_opportunities(Some(&brand), None)?
            .into_iter()
            .filter(|opportunity| opportunity.play_id == play.id)
            .filter(|opportunity| opportunity.status != "rejected")
        {
            let tier = match opportunity.evidence_tier.as_str() {
                "action_ready" => 0,
                "discovery_ready" => 1,
                _ => 2,
            };
            let mut roster = db
                .list_opportunity_stakeholders(Some(&opportunity.id), Some(&brand))?
                .into_iter()
                .filter_map(|stakeholder| {
                    let person = people.get(&stakeholder.person_id)?;
                    if stakeholder.status == "held" {
                        return None;
                    }
                    Some((
                        stakeholder.priority,
                        crate::response_design::contact_priority(
                            &person.title,
                            &person.vantage,
                            person.primary,
                        ),
                        person.name.clone(),
                        person.id.clone(),
                    ))
                })
                .collect::<Vec<_>>();
            roster.sort_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| right.1.cmp(&left.1))
                    .then_with(|| left.2.cmp(&right.2))
            });
            if !roster.is_empty() {
                opportunity_rosters.push((tier, opportunity.fit_score, opportunity.title, roster));
            }
        }
    }
    opportunity_rosters.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.2.cmp(&right.2))
    });

    let mut ordered = Vec::new();
    let mut seen = HashSet::new();
    let max_depth = opportunity_rosters
        .iter()
        .map(|(_, _, _, roster)| roster.len())
        .max()
        .unwrap_or(0);
    // Finish easy and medium coverage before revealing contacts for hard
    // opportunities that are not allowed to draft yet.
    for allowed_tier in [1, 2] {
        for depth in 0..max_depth {
            for (tier, _, _, roster) in &opportunity_rosters {
                if *tier > allowed_tier || (*tier <= 1 && allowed_tier == 2) {
                    continue;
                }
                if let Some((_, _, _, person_id)) = roster.get(depth) {
                    if seen.insert(person_id.clone()) {
                        ordered.push(person_id.clone());
                    }
                }
            }
        }
    }
    Ok(ordered)
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(70).collect()
}

/// Heuristic for Apollo signalling the account is out of credits or over a
/// rate/spend quota — the point at which continuing just wastes calls.
fn is_quota_error(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("insufficient credit")
        || m.contains("out of credit")
        || m.contains("not enough credit")
        || m.contains("credit limit")
        || m.contains("payment required")
        || m.contains(" 402")
        || m.contains("quota")
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{enrichment_matches_account, merge_enriched_identity, select_enrichment_batch};
    use crate::apollo::{ApolloOrg, ApolloPerson};
    use crate::db::{Lead, Person};

    #[test]
    fn enrichment_respects_the_full_motion_working_set() {
        let people = [
            ("a-1", "account-a"),
            ("a-2", "account-a"),
            ("b-1", "account-b"),
            ("b-2", "account-b"),
            ("c-1", "account-c"),
        ]
        .into_iter()
        .map(|(id, lead_id)| Person {
            id: id.into(),
            lead_id: lead_id.into(),
            ..Default::default()
        })
        .collect::<Vec<_>>();
        let selected = ["a-1", "b-1", "c-1"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>();

        let batch = select_enrichment_batch(people, 3, Some(&selected), &[]);
        assert_eq!(
            batch
                .iter()
                .map(|person| person.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a-1", "b-1", "c-1"]
        );
    }

    #[test]
    fn enrichment_follows_current_opportunity_order_before_fifo() {
        let people = ["legacy", "second-current", "first-current"]
            .into_iter()
            .map(|id| Person {
                id: id.into(),
                name: id.into(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let priority = vec!["first-current".into(), "second-current".into()];

        let batch = select_enrichment_batch(people, 2, None, &priority);

        assert_eq!(
            batch
                .iter()
                .map(|person| person.id.as_str())
                .collect::<Vec<_>>(),
            vec!["first-current", "second-current"]
        );
    }

    #[test]
    fn enrichment_hydrates_identity_without_erasing_vantage() {
        let mut person = Person {
            first_name: "Sam".into(),
            name: "Sam".into(),
            title: "Production Manager".into(),
            vantage: "process_owner".into(),
            ..Default::default()
        };
        let matched = ApolloPerson {
            first_name: "Sam".into(),
            last_name: "Rivera".into(),
            name: "Sam Rivera".into(),
            title: "Production Manager".into(),
            linkedin_url: "https://linkedin.com/in/sam-rivera".into(),
            city: "Brantford".into(),
            state: "Ontario".into(),
            country: "Canada".into(),
            ..Default::default()
        };

        merge_enriched_identity(&mut person, &matched);

        assert_eq!(person.name, "Sam Rivera");
        assert_eq!(person.location, "Brantford, Ontario, Canada");
        assert_eq!(person.vantage, "process_owner");
    }

    #[test]
    fn enrichment_holds_a_person_who_moved_to_another_employer() {
        let lead = Lead {
            apollo_org_id: "redpath-org".into(),
            name: "Redpath Sugar".into(),
            domain: "redpathsugar.com".into(),
            ..Default::default()
        };
        let matched = ApolloPerson {
            organization_id: "disney-org".into(),
            email: "david@disney.com".into(),
            organization: Some(ApolloOrg {
                id: "disney-org".into(),
                name: "Disney".into(),
                primary_domain: "disney.com".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(!enrichment_matches_account(&lead, &matched));
    }

    #[test]
    fn enrichment_accepts_an_explicit_apollo_employer_identity_with_alias_email() {
        let lead = Lead {
            apollo_org_id: "company-org".into(),
            name: "Company Canada".into(),
            domain: "company.ca".into(),
            ..Default::default()
        };
        let matched = ApolloPerson {
            organization_id: "company-org".into(),
            email: "owner@parent-company.com".into(),
            ..Default::default()
        };

        assert!(enrichment_matches_account(&lead, &matched));
    }
}
