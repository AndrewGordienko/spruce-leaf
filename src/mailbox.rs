//! Sending identities: load per-brand mailboxes from env and health-check them.
//!
//! Each brand sends from its own domain/mailbox so one brand's reputation can't
//! sink another's. Config is env-driven, prefixed by the brand key (uppercased):
//!
//!   GNK_FROM_EMAIL, GNK_FROM_NAME, GNK_SMTP_HOST, GNK_SMTP_PORT,
//!   GNK_SMTP_USER, GNK_SMTP_PASS, GNK_IMAP_HOST, GNK_IMAP_PORT, GNK_DAILY_CAP
//!
//! (…and WAPAHKI_*, OUTAGEHUB_*). A brand with no FROM_EMAIL/SMTP_HOST is simply
//! skipped — you can run sourcing/enrichment long before any mailbox exists.

use anyhow::Result;

use crate::db::{Mailbox, SharedDb};
use crate::verify;

fn env_for(brand: &str, field: &str) -> Option<String> {
    let key = format!("{}_{field}", brand.to_uppercase());
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

/// Upsert a mailbox for every brand that has at least FROM_EMAIL + SMTP_HOST set
/// in the environment. Returns how many were configured.
pub fn load_from_env(db: &SharedDb, brand_keys: &[&str]) -> Result<usize> {
    let mut n = 0;
    for &brand in brand_keys {
        let (Some(from_email), Some(smtp_host)) =
            (env_for(brand, "FROM_EMAIL"), env_for(brand, "SMTP_HOST"))
        else {
            continue;
        };
        let m = Mailbox {
            brand: brand.to_string(),
            from_name: env_for(brand, "FROM_NAME").unwrap_or_else(|| brand.to_string()),
            from_email: from_email.clone(),
            smtp_host,
            smtp_port: env_for(brand, "SMTP_PORT").and_then(|s| s.parse().ok()).unwrap_or(587),
            smtp_user: env_for(brand, "SMTP_USER").unwrap_or(from_email),
            smtp_pass: env_for(brand, "SMTP_PASS").unwrap_or_default(),
            imap_host: env_for(brand, "IMAP_HOST").unwrap_or_default(),
            imap_port: env_for(brand, "IMAP_PORT").and_then(|s| s.parse().ok()).unwrap_or(993),
            daily_cap: env_for(brand, "DAILY_CAP").and_then(|s| s.parse().ok()).unwrap_or(30),
            active: true,
            ..Default::default()
        };
        db.upsert_mailbox(&m)?;
        n += 1;
    }
    Ok(n)
}

/// The domain of a mailbox's from-address.
pub fn mailbox_domain(m: &Mailbox) -> String {
    m.from_email.split('@').nth(1).unwrap_or("").to_string()
}

/// Health-check every mailbox's sending domain (SPF/DMARC/MX).
pub async fn health_check(db: &SharedDb, brand: Option<&str>) -> Result<Vec<(Mailbox, verify::DomainAuth)>> {
    let mut out = Vec::new();
    for m in db.list_mailboxes(brand)? {
        let auth = verify::check_sending_domain(&mailbox_domain(&m)).await.unwrap_or_default();
        out.push((m, auth));
    }
    Ok(out)
}
