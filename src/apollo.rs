//! Apollo.io API client — the source of *real* companies and people.
//!
//! This is what stops the tool from inventing accounts. The waterfall Apollo
//! actually supports:
//!
//!   1. **Organization search** (`/mixed_companies/search`) — companies matching
//!      an ICP (headcount, location, industry, keywords, technologies).
//!   2. **People search** (`/mixed_people/api_search`) — people at those orgs by
//!      title/seniority. NOTE: on most plans this returns *masked* data (no
//!      email, obfuscated last name); it's for discovery only.
//!   3. **People enrichment / match** (`/people/match`) — reveals a single
//!      person's verified email and optionally requests phone delivery to a
//!      configured webhook (consumes credits).
//!
//! Auth is header-only (`x-api-key`) as of Apollo's Sept-2024 change. The key
//! comes from `APOLLO_API_KEY`. All response structs use `#[serde(default)]` and
//! ignore unknown fields, so Apollo's large, drifting payloads deserialize safely.

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::Duration;

const BASE: &str = "https://api.apollo.io/api/v1";

/// Apollo sends explicit `null`s for absent fields (e.g. `organization_state: null`).
/// Our response structs use `#[serde(default)]`, which only fills *missing* keys — a
/// `null` where a `String`/`Vec` is expected still fails with "invalid type: null,
/// expected a string". Recursively dropping null-valued object keys lets those
/// defaults apply instead, keeping the drifting payloads safe to deserialize.
fn strip_nulls(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                strip_nulls(v);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(strip_nulls),
        _ => {}
    }
}

pub struct Apollo {
    key: String,
    phone_webhook_url: Option<String>,
    http: Client,
}

/// ICP filters for organization search. Empty vecs are omitted from the request.
#[derive(Debug, Clone, Default)]
pub struct OrgFilters {
    /// Free-text ICP keywords (e.g. "third party logistics", "clinical trials").
    pub keywords: Vec<String>,
    /// Specific organization name; Apollo accepts partial company-name matches.
    pub name: String,
    /// Employer domains without scheme, www, or @.
    pub domains: Vec<String>,
    /// Apollo headcount buckets, e.g. "51,200" or "201,500".
    pub employee_ranges: Vec<String>,
    /// HQ locations, e.g. "Canada", "Ontario, Canada".
    pub locations: Vec<String>,
    pub page: u32,
    pub per_page: u32,
}

/// Filters for people search within a set of orgs / by title.
#[derive(Debug, Clone, Default)]
pub struct PeopleFilters {
    pub organization_ids: Vec<String>,
    pub organization_domains: Vec<String>,
    pub titles: Vec<String>,
    pub seniorities: Vec<String>,
    pub locations: Vec<String>,
    pub page: u32,
    pub per_page: u32,
}

// --- Response shapes -------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ApolloOrg {
    pub id: String,
    pub name: String,
    pub website_url: String,
    pub primary_domain: String,
    pub industry: String,
    pub estimated_num_employees: i64,
    pub organization_city: String,
    pub organization_state: String,
    pub organization_country: String,
    pub city: String,
    pub state: String,
    pub country: String,
    pub annual_revenue_printed: String,
    pub short_description: String,
    pub keywords: Vec<String>,
    #[serde(rename = "technology_names")]
    pub technology_names: Vec<String>,
}

impl ApolloOrg {
    pub fn domain(&self) -> String {
        if !self.primary_domain.is_empty() {
            return self.primary_domain.clone();
        }
        self.website_url
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_start_matches("www.")
            .split('/')
            .next()
            .unwrap_or("")
            .to_string()
    }

    pub fn hq(&self) -> String {
        let city = if !self.organization_city.is_empty() {
            &self.organization_city
        } else {
            &self.city
        };
        let state = if !self.organization_state.is_empty() {
            &self.organization_state
        } else {
            &self.state
        };
        let country = if !self.organization_country.is_empty() {
            &self.organization_country
        } else {
            &self.country
        };
        [city.as_str(), state.as_str(), country.as_str()]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ApolloPhone {
    pub raw_number: String,
    pub sanitized_number: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ApolloPerson {
    pub id: String,
    pub first_name: String,
    pub last_name: String,
    pub name: String,
    pub title: String,
    pub linkedin_url: String,
    pub email: String,
    /// verified | unverified | likely_to_engage | unavailable | ...
    pub email_status: String,
    pub seniority: String,
    pub departments: Vec<String>,
    pub city: String,
    pub state: String,
    pub country: String,
    pub organization: Option<ApolloOrg>,
    pub organization_id: String,
    pub phone_numbers: Vec<ApolloPhone>,
}

impl ApolloPerson {
    pub fn best_phone(&self) -> String {
        self.phone_numbers
            .iter()
            .find(|p| !p.sanitized_number.is_empty())
            .map(|p| p.sanitized_number.clone())
            .unwrap_or_default()
    }

    pub fn full_name(&self) -> String {
        if !self.name.is_empty() {
            self.name.clone()
        } else {
            format!("{} {}", self.first_name, self.last_name)
                .trim()
                .to_string()
        }
    }

    pub fn location(&self) -> String {
        [
            self.city.as_str(),
            self.state.as_str(),
            self.country.as_str(),
        ]
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join(", ")
    }
}

#[derive(Debug, Deserialize)]
struct OrgSearchResp {
    #[serde(default)]
    organizations: Vec<ApolloOrg>,
    #[serde(default)]
    accounts: Vec<ApolloOrg>,
}

#[derive(Debug, Deserialize)]
struct OrgEnrichResp {
    #[serde(default)]
    organization: Option<ApolloOrg>,
}

#[derive(Debug, Deserialize)]
struct PeopleSearchResp {
    #[serde(default)]
    people: Vec<ApolloPerson>,
    #[serde(default)]
    contacts: Vec<ApolloPerson>,
}

#[derive(Debug, Deserialize)]
struct MatchResp {
    #[serde(default)]
    person: Option<ApolloPerson>,
}

impl Apollo {
    /// Build a client from `APOLLO_API_KEY`; errors if it's unset so callers can
    /// print a clear "set your Apollo key" message.
    pub fn from_env() -> Result<Self> {
        let key = std::env::var("APOLLO_API_KEY")
            .map_err(|_| anyhow!("APOLLO_API_KEY is not set — add it to .env"))?;
        if key.trim().is_empty() {
            bail!("APOLLO_API_KEY is empty");
        }
        Ok(Self {
            key,
            phone_webhook_url: std::env::var("APOLLO_WEBHOOK_URL")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            http: Client::new(),
        })
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value> {
        if let Some(cached) = read_cached_response(path, &body) {
            return Ok(cached);
        }
        // Retry transient rate-limit (429) and 5xx responses with exponential
        // backoff so a bulk pass (hundreds of reveals) rides out Apollo's rate
        // limits instead of failing mid-run. Honors Retry-After when present.
        const MAX_ATTEMPTS: u32 = 4;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let resp = self
                .http
                .post(format!("{BASE}{path}"))
                .header("x-api-key", &self.key)
                .header("Content-Type", "application/json")
                .header("Cache-Control", "no-cache")
                .json(&body)
                .send()
                .await
                .with_context(|| format!("calling Apollo {path}"))?;
            let status = resp.status();
            if (status.as_u16() == 429 || status.is_server_error()) && attempt < MAX_ATTEMPTS {
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok());
                let delay = retry_after.unwrap_or(2u64.pow(attempt)).clamp(1, 60);
                eprintln!(
                    "  · Apollo {path} {} — backing off {delay}s (attempt {attempt}/{MAX_ATTEMPTS})",
                    status.as_u16()
                );
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                continue;
            }
            let text = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                bail!(
                    "Apollo {path} returned {status}: {}",
                    text.chars().take(400).collect::<String>()
                );
            }
            let mut value: Value = serde_json::from_str(&text)
                .with_context(|| format!("parsing Apollo {path} response"))?;
            strip_nulls(&mut value);
            write_cached_response(path, &body, &value);
            return Ok(value);
        }
    }

    /// Search for organizations matching the ICP filters.
    pub async fn search_organizations(&self, f: &OrgFilters) -> Result<Vec<ApolloOrg>> {
        let mut body = json!({
            "page": f.page.max(1),
            "per_page": if f.per_page == 0 { 25 } else { f.per_page },
        });
        if !f.keywords.is_empty() {
            body["q_organization_keyword_tags"] = json!(f.keywords);
        }
        if !f.name.trim().is_empty() {
            body["q_organization_name"] = json!(f.name.trim());
        }
        if !f.domains.is_empty() {
            body["q_organization_domains_list"] = json!(f.domains);
        }
        if !f.employee_ranges.is_empty() {
            body["organization_num_employees_ranges"] = json!(f.employee_ranges);
        }
        if !f.locations.is_empty() {
            body["organization_locations"] = json!(f.locations);
        }
        let resp: OrgSearchResp =
            serde_json::from_value(self.post("/mixed_companies/search", body).await?)?;
        let mut orgs = resp.organizations;
        orgs.extend(resp.accounts);
        Ok(orgs)
    }

    /// Enrich one organization by domain, returning full firmographics.
    ///
    /// Apollo's company *search* frequently returns sparse records (little more
    /// than name + domain), which makes fit-qualification reject real companies
    /// for *missing* data rather than bad fit. This fills in industry, headcount,
    /// revenue, description, keywords, and technologies. Organization enrichment
    /// does not consume the people/email-reveal credits.
    pub async fn enrich_organization(&self, domain: &str) -> Result<Option<ApolloOrg>> {
        if domain.trim().is_empty() {
            return Ok(None);
        }
        let body = json!({ "domain": domain.trim() });
        let resp: OrgEnrichResp =
            serde_json::from_value(self.post("/organizations/enrich", body).await?)?;
        Ok(resp.organization)
    }

    /// Search for people (masked) within orgs / by title.
    pub async fn search_people(&self, f: &PeopleFilters) -> Result<Vec<ApolloPerson>> {
        let mut body = json!({
            "page": f.page.max(1),
            "per_page": if f.per_page == 0 { 10 } else { f.per_page },
        });
        if !f.organization_ids.is_empty() {
            body["organization_ids"] = json!(f.organization_ids);
        }
        if !f.organization_domains.is_empty() {
            body["q_organization_domains_list"] = json!(f.organization_domains);
        }
        if !f.titles.is_empty() {
            body["person_titles"] = json!(f.titles);
            body["include_similar_titles"] = json!(true);
        }
        if !f.seniorities.is_empty() {
            body["person_seniorities"] = json!(f.seniorities);
        }
        if !f.locations.is_empty() {
            body["person_locations"] = json!(f.locations);
        }
        let resp: PeopleSearchResp =
            serde_json::from_value(self.post("/mixed_people/api_search", body).await?)?;
        let mut people = resp.people;
        people.extend(resp.contacts);
        Ok(people)
    }

    /// Reveal a person's verified email and optionally request phone enrichment.
    /// Apollo delivers revealed phones asynchronously, so phone requests require
    /// `APOLLO_WEBHOOK_URL`; any phone already present in the synchronous person
    /// response is still persisted by the caller.
    pub async fn enrich_person(
        &self,
        apollo_id: &str,
        first_name: &str,
        last_name: &str,
        domain: &str,
        reveal_phone: bool,
    ) -> Result<ApolloPerson> {
        let mut body = json!({ "reveal_personal_emails": true });
        if !apollo_id.is_empty() {
            body["id"] = json!(apollo_id);
        }
        if !first_name.is_empty() {
            body["first_name"] = json!(first_name);
        }
        if !last_name.is_empty() {
            body["last_name"] = json!(last_name);
        }
        if !domain.is_empty() {
            body["domain"] = json!(domain);
        }
        if reveal_phone {
            let webhook_url = self.phone_webhook_url.as_deref().ok_or_else(|| {
                anyhow!(
                    "phone reveal requires APOLLO_WEBHOOK_URL (Apollo delivers phone results asynchronously)"
                )
            })?;
            body["reveal_phone_number"] = json!(true);
            body["webhook_url"] = json!(webhook_url);
        }
        let resp: MatchResp = serde_json::from_value(self.post("/people/match", body).await?)?;
        resp.person
            .ok_or_else(|| anyhow!("Apollo returned no match for {first_name} {last_name}"))
    }
}

fn cache_ttl(path: &str) -> Duration {
    let days = match path {
        // Email reveals consume credits. Preserve a successful identity result
        // for a year; callers still verify the address before it becomes usable.
        "/people/match" => 365,
        "/mixed_people/api_search" | "/organizations/enrich" => 30,
        _ => 7,
    };
    Duration::from_secs(days * 24 * 60 * 60)
}

fn apollo_cache_path(path: &str, body: &Value) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    "apollo-cache-v1".hash(&mut hasher);
    path.hash(&mut hasher);
    serde_json::to_string(body)
        .unwrap_or_default()
        .hash(&mut hasher);
    let directory = std::env::var("APOLLO_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(".spruce/apollo-cache"));
    directory.join(format!("{:016x}.json", hasher.finish()))
}

fn apollo_cache_enabled() -> bool {
    !std::env::var("APOLLO_CACHE_BYPASS")
        .ok()
        .is_some_and(|value| matches!(value.trim(), "1" | "true" | "yes"))
}

fn read_cached_response(path: &str, body: &Value) -> Option<Value> {
    if !apollo_cache_enabled() {
        return None;
    }
    let cache_path = apollo_cache_path(path, body);
    let metadata = std::fs::metadata(&cache_path).ok()?;
    if metadata.modified().ok()?.elapsed().ok()? > cache_ttl(path) {
        return None;
    }
    let mut value = serde_json::from_slice::<Value>(&std::fs::read(cache_path).ok()?).ok()?;
    strip_nulls(&mut value);
    Some(value)
}

fn write_cached_response(path: &str, body: &Value, response: &Value) {
    if !apollo_cache_enabled() {
        return;
    }
    let cache_path = apollo_cache_path(path, body);
    let Some(parent) = cache_path.parent() else {
        return;
    };
    if std::fs::create_dir_all(parent).is_err() {
        return;
    }
    let temporary = cache_path.with_extension(format!("{}.tmp", std::process::id()));
    let Ok(bytes) = serde_json::to_vec(response) else {
        return;
    };
    if std::fs::write(&temporary, bytes).is_ok() {
        let _ = std::fs::rename(temporary, cache_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apollo_cache_keys_are_stable_and_request_specific() {
        let first = apollo_cache_path("/people/match", &serde_json::json!({"id":"person-1"}));
        let again = apollo_cache_path("/people/match", &serde_json::json!({"id":"person-1"}));
        let second = apollo_cache_path("/people/match", &serde_json::json!({"id":"person-2"}));
        assert_eq!(first, again);
        assert_ne!(first, second);
        assert_eq!(
            first.extension().and_then(|value| value.to_str()),
            Some("json")
        );
    }

    #[test]
    fn null_string_and_vec_fields_deserialize_to_defaults() {
        // A realistic Apollo org payload where several fields come back as `null`.
        // Without strip_nulls this fails with "invalid type: null, expected a string".
        let mut raw = serde_json::json!({
            "id": "abc",
            "name": "Acme Logistics",
            "organization_state": null,
            "annual_revenue_printed": null,
            "keywords": null,
            "estimated_num_employees": null,
        });
        strip_nulls(&mut raw);
        let org: ApolloOrg = serde_json::from_value(raw).expect("should deserialize");
        assert_eq!(org.name, "Acme Logistics");
        assert_eq!(org.organization_state, "");
        assert_eq!(org.estimated_num_employees, 0);
        assert!(org.keywords.is_empty());
    }

    #[test]
    fn strip_nulls_recurses_into_nested_person_org() {
        let mut raw = serde_json::json!({
            "person": {
                "id": "p1",
                "name": "Dana Ops",
                "title": null,
                "organization": { "id": "o1", "name": "Acme", "industry": null }
            }
        });
        strip_nulls(&mut raw);
        let resp: MatchResp = serde_json::from_value(raw).expect("should deserialize");
        let person = resp.person.expect("person present");
        assert_eq!(person.title, "");
        assert_eq!(person.organization.unwrap().industry, "");
    }
}
