//! Enrichment: reveal real, verified contact details for sourced people.
//!
//! Sourcing files people with masked/empty emails (that's all Apollo search
//! returns). This step calls Apollo's People Enrichment (`people/match`) to
//! reveal the actual email + phone, then runs [`crate::verify`] over it so only
//! genuinely deliverable addresses become send-eligible. Costs Apollo credits,
//! so it's a separate, explicit pass.

use anyhow::Result;

use crate::apollo::Apollo;
use crate::db::{Lead, SharedDb};
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

/// Enrich up to `limit` not-yet-enriched people for `brand` (or all brands).
pub async fn enrich_pending(
    db: &SharedDb,
    apollo: &Apollo,
    brand: Option<&str>,
    limit: usize,
    reveal_phone: bool,
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

    let batch: Vec<_> = people.into_iter().take(limit).collect();
    let total = batch.len();
    if total > 0 {
        eprintln!("  · enriching {total} people (~{total} Apollo credits, 1 per reveal)…");
    }
    for (i, p) in batch.into_iter().enumerate() {
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
                    eprintln!(
                        "  · [{}/{total}] stopping early — Apollo out of credits / quota: {}",
                        i + 1,
                        first_line(&msg)
                    );
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
                eprintln!(
                    "  · [{}/{total}] {who} — enrich failed: {}",
                    i + 1,
                    first_line(&msg)
                );
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

        if email.is_empty() {
            eprintln!("  · [{}/{total}] {who} — no email revealed", i + 1);
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
        eprintln!(
            "  · [{}/{total}] {mark} {who} — {email} ({})",
            i + 1,
            verdict.as_str()
        );

        // Respect suppression immediately: if this address is on the list, park it.
        if db.is_suppressed(&p.brand, &email)? {
            db.set_person_status(&p.id, "suppressed")?;
            db.log_event(&p.brand, &p.id, "", "suppressed", "on suppression list")?;
        }
    }

    Ok(summary)
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
