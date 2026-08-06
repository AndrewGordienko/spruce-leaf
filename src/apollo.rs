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
//!      person's verified email and phone (consumes credits).
//!
//! Auth is header-only (`x-api-key`) as of Apollo's Sept-2024 change. The key
//! comes from `APOLLO_API_KEY`. All response structs use `#[serde(default)]` and
//! ignore unknown fields, so Apollo's large, drifting payloads deserialize safely.

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const BASE: &str = "https://api.apollo.io/api/v1";

pub struct Apollo {
    key: String,
    http: Client,
}

/// ICP filters for organization search. Empty vecs are omitted from the request.
#[derive(Debug, Clone, Default)]
pub struct OrgFilters {
    /// Free-text ICP keywords (e.g. "third party logistics", "clinical trials").
    pub keywords: Vec<String>,
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
        let city = if !self.organization_city.is_empty() { &self.organization_city } else { &self.city };
        let state = if !self.organization_state.is_empty() { &self.organization_state } else { &self.state };
        let country = if !self.organization_country.is_empty() { &self.organization_country } else { &self.country };
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
            format!("{} {}", self.first_name, self.last_name).trim().to_string()
        }
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
        Ok(Self { key, http: Client::new() })
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value> {
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
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("Apollo {path} returned {status}: {}", text.chars().take(400).collect::<String>());
        }
        serde_json::from_str(&text).with_context(|| format!("parsing Apollo {path} response"))
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
        if !f.employee_ranges.is_empty() {
            body["organization_num_employees_ranges"] = json!(f.employee_ranges);
        }
        if !f.locations.is_empty() {
            body["organization_locations"] = json!(f.locations);
        }
        let resp: OrgSearchResp = serde_json::from_value(self.post("/mixed_companies/search", body).await?)?;
        let mut orgs = resp.organizations;
        orgs.extend(resp.accounts);
        Ok(orgs)
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

    /// Reveal a person's verified email + phone. Match by Apollo id when known,
    /// else by name + org domain. `reveal_personal_emails` costs credits.
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
            body["reveal_phone_number"] = json!(true);
        }
        let resp: MatchResp = serde_json::from_value(self.post("/people/match", body).await?)?;
        resp.person.ok_or_else(|| anyhow!("Apollo returned no match for {first_name} {last_name}"))
    }
}
