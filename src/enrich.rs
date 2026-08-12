//! Enrichment: reveal real, verified contact details for sourced people.
//!
//! Sourcing files people with masked/empty emails (that's all Apollo search
//! returns). This step calls Apollo's People Enrichment (`people/match`) to
//! reveal the actual email + phone, then runs [`crate::verify`] over it so only
//! genuinely deliverable addresses become send-eligible. Costs Apollo credits,
//! so it's a separate, explicit pass.

use anyhow::Result;
use std::collections::HashSet;
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

    let batch = select_enrichment_batch(people, limit, only_person_ids);
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

        merge_enriched_identity(&mut p, &matched);
        db.upsert_person(&p)?;

        let email = matched.email.trim().to_string();
        let phone = if matched.best_phone().is_empty() {
            p.phone.clone()
        } else {
            matched.best_phone()
        };

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

fn select_enrichment_batch(
    people: Vec<Person>,
    limit: usize,
    only_person_ids: Option<&HashSet<String>>,
) -> Vec<Person> {
    people
        .into_iter()
        .filter(|person| only_person_ids.is_none_or(|person_ids| person_ids.contains(&person.id)))
        .take(limit)
        .collect()
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

    use super::{merge_enriched_identity, select_enrichment_batch};
    use crate::apollo::ApolloPerson;
    use crate::db::Person;

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

        let batch = select_enrichment_batch(people, 3, Some(&selected));
        assert_eq!(
            batch
                .iter()
                .map(|person| person.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a-1", "b-1", "c-1"]
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
}
