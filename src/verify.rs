//! Email + sending-domain verification.
//!
//! Two jobs, both DNS-backed:
//!   * [`verify_email`] — is a prospect's address plausibly deliverable? Syntax +
//!     MX lookup, combined with Apollo's own status. This gates sending: we only
//!     send to `verified`. Sending to unverified/scraped addresses from a cold
//!     domain is the fastest way to wreck deliverability.
//!   * [`check_sending_domain`] — does OUR sending domain have SPF and DMARC set
//!     up? A pre-flight so a misconfigured mailbox is caught before it sends.

use anyhow::Result;
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::TokioAsyncResolver;

/// Verdict for a prospect email. `verified` is the only send-eligible state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmailVerdict {
    Verified,
    Risky,
    Invalid,
}

impl EmailVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            EmailVerdict::Verified => "verified",
            EmailVerdict::Risky => "risky",
            EmailVerdict::Invalid => "invalid",
        }
    }
}

fn resolver() -> TokioAsyncResolver {
    TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default())
}

/// Basic RFC-ish syntax check: one `@`, non-empty local + domain, a dot in the
/// domain, no spaces.
pub fn syntax_ok(email: &str) -> bool {
    let e = email.trim();
    if e.contains(char::is_whitespace) {
        return false;
    }
    let mut parts = e.split('@');
    let (Some(local), Some(domain), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    !local.is_empty() && domain.contains('.') && !domain.starts_with('.') && !domain.ends_with('.')
}

/// Does the domain publish MX (or at least A) records — i.e. can it receive mail?
pub async fn has_mx(domain: &str) -> bool {
    let r = resolver();
    if let Ok(mx) = r.mx_lookup(domain).await {
        if mx.iter().next().is_some() {
            return true;
        }
    }
    // Some domains accept mail on the A record with no explicit MX.
    r.lookup_ip(domain)
        .await
        .map(|l| l.iter().next().is_some())
        .unwrap_or(false)
}

/// Combine syntax + MX with Apollo's status into a single verdict.
///   * bad syntax / no MX            → Invalid
///   * MX ok + Apollo "verified"     → Verified
///   * MX ok + anything else         → Risky
pub async fn verify_email(email: &str, apollo_status: &str) -> EmailVerdict {
    if email.trim().is_empty() || !syntax_ok(email) {
        return EmailVerdict::Invalid;
    }
    let domain = email.split('@').nth(1).unwrap_or("");
    if domain.is_empty() {
        return EmailVerdict::Invalid;
    }
    if !has_mx(domain).await {
        return EmailVerdict::Invalid;
    }
    if apollo_status.eq_ignore_ascii_case("verified") {
        EmailVerdict::Verified
    } else {
        EmailVerdict::Risky
    }
}

/// Result of checking that a sending domain is set up for deliverability.
#[derive(Debug, Clone, Default)]
pub struct DomainAuth {
    pub has_spf: bool,
    pub has_dmarc: bool,
    pub mx_ok: bool,
}

impl DomainAuth {
    pub fn is_healthy(&self) -> bool {
        self.has_spf && self.has_dmarc && self.mx_ok
    }

    pub fn summary(&self) -> String {
        format!(
            "SPF {} · DMARC {} · MX {}",
            yn(self.has_spf),
            yn(self.has_dmarc),
            yn(self.mx_ok),
        )
    }
}

fn yn(b: bool) -> &'static str {
    if b {
        "ok"
    } else {
        "MISSING"
    }
}

/// Check SPF + DMARC + MX for one of our sending domains.
pub async fn check_sending_domain(domain: &str) -> Result<DomainAuth> {
    let r = resolver();
    let mut auth = DomainAuth::default();

    if let Ok(txt) = r.txt_lookup(domain).await {
        auth.has_spf = txt.iter().any(|rec| {
            rec.txt_data().iter().any(|d| {
                String::from_utf8_lossy(d)
                    .to_lowercase()
                    .starts_with("v=spf1")
            })
        });
    }
    if let Ok(txt) = r.txt_lookup(format!("_dmarc.{domain}")).await {
        auth.has_dmarc = txt.iter().any(|rec| {
            rec.txt_data().iter().any(|d| {
                String::from_utf8_lossy(d)
                    .to_lowercase()
                    .contains("v=dmarc1")
            })
        });
    }
    auth.mx_ok = has_mx(domain).await;
    Ok(auth)
}
