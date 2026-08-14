//! Build account-safe OutageHub evidence by intersecting verified Canadian
//! operating locations with historical OutageHub utility polygons.
//!
//! Locations may come from the public Canadian EV station feed or a verified
//! address inventory for properties, laboratories, warehouses, towers, stores,
//! residences, plants, and other operated sites. The result is outside utility
//! context, never proof of private site or asset status.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

const STATION_LOCATOR_URL: &str = "https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/electric-charging-alternative-fuelling-stationslocator-map";
const STATION_API_URL: &str = "https://developer.nlr.gov/api/alt-fuel-stations/v1.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocationOutageMatch {
    #[serde(default)]
    pub location_id: String,
    #[serde(default)]
    pub company: String,
    #[serde(default)]
    pub domain: String,
    #[serde(default)]
    pub location_kind: String,
    #[serde(default)]
    pub operating_relationship: String,
    pub station_id: i64,
    pub station_name: String,
    pub network: String,
    pub address: String,
    pub city: String,
    pub province: String,
    pub latitude: f64,
    pub longitude: f64,
    pub outage_id: i64,
    pub utility_provider: String,
    pub outage_start_ts: i64,
    pub outage_end_ts: i64,
    pub outage_start_utc: String,
    pub station_source_url: String,
    #[serde(default)]
    pub geocode_source_url: String,
    #[serde(default)]
    pub geocoding_attribution: String,
    pub outage_source_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchReport {
    pub generated_at: String,
    pub station_source_url: String,
    pub outage_archive: String,
    pub interpretation_boundary: String,
    #[serde(default)]
    pub geocoding_attribution: String,
    pub matches: Vec<LocationOutageMatch>,
}

/// A first-party, exact-page-validated operating address nominated by company
/// research. Coordinates are deliberately absent: the evidence extractor does
/// not get to invent them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VerifiedLocationCandidate {
    /// owned | operated | managed | monitored
    pub relationship: String,
    pub location_kind: String,
    pub name: String,
    pub street_address: String,
    pub city: String,
    pub province: String,
    pub postal_code: String,
    pub source_url: String,
    pub source_excerpt: String,
}

#[derive(Debug, Deserialize)]
struct StationPayload {
    #[serde(default)]
    fuel_stations: Vec<Station>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Station {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    location_id: String,
    #[serde(default)]
    company: String,
    #[serde(default)]
    domain: String,
    #[serde(default = "default_location_kind")]
    location_kind: String,
    #[serde(default)]
    operating_relationship: String,
    #[serde(default)]
    station_name: String,
    #[serde(default)]
    ev_network: String,
    #[serde(default)]
    street_address: String,
    #[serde(default)]
    city: String,
    #[serde(default)]
    state: String,
    latitude: f64,
    longitude: f64,
    #[serde(default)]
    source_url: String,
    #[serde(default)]
    source_excerpt: String,
    #[serde(default)]
    postal_code: String,
    #[serde(default)]
    geocode_source_url: String,
    #[serde(default)]
    geocoding_attribution: String,
}

#[derive(Debug, Deserialize)]
struct GeocodeResult {
    lat: String,
    lon: String,
    #[serde(default)]
    licence: String,
    #[serde(default)]
    address: GeocodeAddress,
}

#[derive(Debug, Default, Deserialize)]
struct GeocodeAddress {
    #[serde(default)]
    country_code: String,
}

static LOCATION_INVENTORY_WRITE: OnceLock<Mutex<()>> = OnceLock::new();
static GEOCODER_RATE_LIMIT: OnceLock<tokio::sync::Mutex<Option<tokio::time::Instant>>> =
    OnceLock::new();

fn default_location_kind() -> String {
    "operating location".into()
}

#[derive(Debug, Deserialize)]
struct OutageRow {
    id: i64,
    #[serde(default)]
    provider: String,
    #[serde(default)]
    polygon: Option<String>,
    #[serde(rename = "startTs", default)]
    start_ts: i64,
    #[serde(rename = "endTs", default)]
    end_ts: i64,
}

struct StationIndex {
    stations: Vec<Station>,
    grid: HashMap<(i16, i16), Vec<usize>>,
}

impl StationIndex {
    fn new(stations: Vec<Station>) -> Self {
        let mut grid = HashMap::<(i16, i16), Vec<usize>>::new();
        for (index, station) in stations.iter().enumerate() {
            grid.entry(grid_cell(station.latitude, station.longitude))
                .or_default()
                .push(index);
        }
        Self { stations, grid }
    }

    fn candidates(&self, bounds: Bounds) -> Vec<usize> {
        let mut candidates = Vec::new();
        for lat in (bounds.min_lat.floor() as i16)..=(bounds.max_lat.floor() as i16) {
            for lon in (bounds.min_lon.floor() as i16)..=(bounds.max_lon.floor() as i16) {
                if let Some(indices) = self.grid.get(&(lat, lon)) {
                    candidates.extend(indices.iter().copied());
                }
            }
        }
        candidates
    }
}

#[derive(Debug, Clone, Copy)]
struct Bounds {
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
}

type Point = (f64, f64); // longitude, latitude
type Polygon = Vec<Vec<Point>>; // first ring is the exterior; the rest are holes

pub async fn build_report(archive: &Path, output: &Path) -> Result<MatchReport> {
    let stations = fetch_canadian_stations().await?;
    build_report_from_locations(archive, output, stations, STATION_LOCATOR_URL)
}

/// Match an operator-supplied JSON array of verified Canadian locations. Every
/// row must include `company`, `domain`, `location_kind`,
/// `operating_relationship`, `station_name`, `street_address`, `city`, `state`,
/// `latitude`, `longitude`, and a public `source_url`. Coordinates make the
/// polygon result reproducible; the address and source establish that it
/// belongs to the operator rather than a customer.
pub fn build_verified_location_report(
    archive: &Path,
    locations: &Path,
    output: &Path,
) -> Result<MatchReport> {
    let file = File::open(locations)
        .with_context(|| format!("open verified locations {}", locations.display()))?;
    let mut rows = serde_json::from_reader::<_, Vec<Station>>(BufReader::new(file))
        .context("parse verified location JSON")?;
    for (index, row) in rows.iter_mut().enumerate() {
        if row.location_id.trim().is_empty() {
            row.location_id = format!("verified-location-{}", index + 1);
        }
        if row.company.trim().is_empty()
            || row.domain.trim().is_empty()
            || row.location_kind.trim().is_empty()
            || !matches!(
                row.operating_relationship.trim(),
                "owned" | "operated" | "managed" | "monitored"
            )
            || row.station_name.trim().is_empty()
            || row.street_address.trim().is_empty()
            || row.city.trim().is_empty()
            || row.state.trim().is_empty()
            || row.source_url.trim().is_empty()
        {
            anyhow::bail!(
                "verified location row {} must name company, domain, location_kind, an owned/operated/managed/monitored relationship, station_name, street_address, city, state, and source_url",
                index + 1
            );
        }
        if !(41.0..=84.0).contains(&row.latitude) || !(-142.0..=-52.0).contains(&row.longitude) {
            anyhow::bail!(
                "verified location row {} has coordinates outside Canada",
                index + 1
            );
        }
    }
    build_report_from_locations(archive, output, rows, &locations.display().to_string())
}

/// Geocode, cache, and match exact-page-validated operating addresses found by
/// account research. The public default is intentionally conservative: one
/// globally serialized request every 15 seconds, cached permanently, with a
/// configurable endpoint. This is account evidence intake, not a general
/// geocoding API.
pub async fn ingest_researched_locations(
    company: &str,
    domain: &str,
    candidates: &[VerifiedLocationCandidate],
    archive: &Path,
    inventory: &Path,
    output: &Path,
) -> Result<Vec<String>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let existing = read_station_inventory(inventory);
    let mut additions = Vec::new();
    // One verified address is sufficient for the bounded historical replay.
    // Trying at most three prevents a malformed first address from blocking an
    // account while keeping public-geocoder use deliberately small.
    for candidate in candidates.iter().take(3) {
        if let Some(cached) = existing.iter().find(|station| {
            station.domain.eq_ignore_ascii_case(domain)
                && normalize(&station.street_address) == normalize(&candidate.street_address)
        }) {
            // Coordinates and geocoder provenance are immutable cache data;
            // operating metadata comes from the newly validated first-party
            // page so an older cache row cannot erase its relationship lineage.
            let mut refreshed = cached.clone();
            refreshed.company = company.trim().to_string();
            refreshed.domain = domain
                .trim()
                .trim_start_matches("www.")
                .to_ascii_lowercase();
            refreshed.location_kind = candidate.location_kind.trim().to_string();
            refreshed.operating_relationship = candidate.relationship.trim().to_string();
            refreshed.station_name = candidate.name.trim().to_string();
            refreshed.street_address = candidate.street_address.trim().to_string();
            refreshed.city = candidate.city.trim().to_string();
            refreshed.state = candidate.province.trim().to_string();
            refreshed.postal_code = candidate.postal_code.trim().to_string();
            refreshed.source_url = candidate.source_url.trim().to_string();
            refreshed.source_excerpt = candidate.source_excerpt.trim().to_string();
            additions.push(refreshed);
            break;
        }
        if let Some(geocoded) = geocode_verified_location(company, domain, candidate).await? {
            additions.push(geocoded);
            break;
        }
    }
    if additions.is_empty() {
        return Ok(Vec::new());
    }

    let _guard = LOCATION_INVENTORY_WRITE
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| anyhow::anyhow!("verified-location inventory lock was poisoned"))?;
    let mut inventory_rows = read_station_inventory(inventory);
    for addition in additions {
        if let Some(existing) = inventory_rows.iter_mut().find(|station| {
            station.domain.eq_ignore_ascii_case(&addition.domain)
                && normalize(&station.street_address) == normalize(&addition.street_address)
        }) {
            *existing = addition;
        } else {
            inventory_rows.push(addition);
        }
    }
    inventory_rows.sort_by(|left, right| {
        left.domain
            .cmp(&right.domain)
            .then_with(|| left.street_address.cmp(&right.street_address))
    });
    crate::storage::atomic_write(inventory, serde_json::to_vec_pretty(&inventory_rows)?)?;
    if !archive.exists() {
        return Ok(Vec::new());
    }
    build_report_from_locations(
        archive,
        output,
        inventory_rows,
        &inventory.display().to_string(),
    )?;
    Ok(evidence_for_company(output, company, domain))
}

fn read_station_inventory(path: &Path) -> Vec<Station> {
    File::open(path)
        .ok()
        .and_then(|file| serde_json::from_reader(BufReader::new(file)).ok())
        .unwrap_or_default()
}

async fn geocode_verified_location(
    company: &str,
    domain: &str,
    candidate: &VerifiedLocationCandidate,
) -> Result<Option<Station>> {
    let endpoint = std::env::var("SPRUCE_GEOCODER_URL")
        .unwrap_or_else(|_| "https://nominatim.openstreetmap.org/search".into());
    let mut url = reqwest::Url::parse(&endpoint).context("parse SPRUCE_GEOCODER_URL")?;
    url.query_pairs_mut()
        .append_pair(
            "q",
            &format!(
                "{}, {}, {}, {}, Canada",
                candidate.street_address, candidate.city, candidate.province, candidate.postal_code
            ),
        )
        .append_pair("format", "jsonv2")
        .append_pair("addressdetails", "1")
        .append_pair("countrycodes", "ca")
        .append_pair("limit", "1");

    let public_nominatim = url.host_str() == Some("nominatim.openstreetmap.org");
    let minimum_interval = if public_nominatim {
        15
    } else {
        std::env::var("SPRUCE_GEOCODER_INTERVAL_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1)
            .max(1)
    };
    let rate_limit = GEOCODER_RATE_LIMIT.get_or_init(|| tokio::sync::Mutex::new(None));
    let mut previous = rate_limit.lock().await;
    if let Some(previous) = *previous {
        let elapsed = previous.elapsed();
        let interval = Duration::from_secs(minimum_interval);
        if elapsed < interval {
            tokio::time::sleep(interval - elapsed).await;
        }
    }
    *previous = Some(tokio::time::Instant::now());
    drop(previous);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent("spruce-leaf/0.1 verified-location-research")
        .build()?;
    let response = client
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<GeocodeResult>>()
        .await?;
    let Some(result) = response.into_iter().next() else {
        return Ok(None);
    };
    if !result.address.country_code.eq_ignore_ascii_case("ca") {
        return Ok(None);
    }
    let Ok(latitude) = result.lat.parse::<f64>() else {
        return Ok(None);
    };
    let Ok(longitude) = result.lon.parse::<f64>() else {
        return Ok(None);
    };
    if !(41.0..=84.0).contains(&latitude) || !(-142.0..=-52.0).contains(&longitude) {
        return Ok(None);
    }
    let identity = format!("{}|{}", domain, normalize(&candidate.street_address));
    Ok(Some(Station {
        location_id: format!("verified-{:016x}", stable_location_hash(&identity)),
        company: company.trim().to_string(),
        domain: domain
            .trim()
            .trim_start_matches("www.")
            .to_ascii_lowercase(),
        location_kind: candidate.location_kind.trim().to_string(),
        operating_relationship: candidate.relationship.trim().to_string(),
        station_name: candidate.name.trim().to_string(),
        street_address: candidate.street_address.trim().to_string(),
        city: candidate.city.trim().to_string(),
        state: candidate.province.trim().to_string(),
        postal_code: candidate.postal_code.trim().to_string(),
        latitude,
        longitude,
        source_url: candidate.source_url.trim().to_string(),
        source_excerpt: candidate.source_excerpt.trim().to_string(),
        geocode_source_url: url.to_string(),
        geocoding_attribution: if result.licence.trim().is_empty() {
            "Geocoding © OpenStreetMap contributors, ODbL 1.0".into()
        } else {
            result.licence
        },
        ..Default::default()
    }))
}

fn stable_location_hash(value: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn build_report_from_locations(
    archive: &Path,
    output: &Path,
    stations: Vec<Station>,
    location_source: &str,
) -> Result<MatchReport> {
    let index = StationIndex::new(stations);
    let mut matches = Vec::new();
    let mut matched_station_ids = HashSet::new();

    let file = File::open(archive)
        .with_context(|| format!("open OutageHub archive {}", archive.display()))?;
    let mut deserializer = serde_json::Deserializer::from_reader(BufReader::new(file));
    ArchiveSeed {
        index: &index,
        matches: &mut matches,
        matched_station_ids: &mut matched_station_ids,
    }
    .deserialize(&mut deserializer)
    .context("stream historical OutageHub archive")?;

    matches.sort_by(|left, right| {
        left.network
            .cmp(&right.network)
            .then_with(|| left.station_name.cmp(&right.station_name))
            .then_with(|| left.outage_start_ts.cmp(&right.outage_start_ts))
    });
    let report = MatchReport {
        generated_at: Utc::now().to_rfc3339(),
        station_source_url: location_source.into(),
        outage_archive: archive.display().to_string(),
        interpretation_boundary: "A match means a source-verified operating-location coordinate fell inside a utility-reported outage polygon at the recorded time. It does not establish private site or asset status, telemetry, incident cause, or the operator's internal workflow.".into(),
        geocoding_attribution: index
            .stations
            .iter()
            .find_map(|station| {
                (!station.geocoding_attribution.trim().is_empty())
                    .then(|| station.geocoding_attribution.clone())
            })
            .unwrap_or_default(),
        matches,
    };
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create evidence directory {}", parent.display()))?;
    }
    serde_json::to_writer_pretty(
        BufWriter::new(
            File::create(output)
                .with_context(|| format!("create evidence report {}", output.display()))?,
        ),
        &report,
    )?;
    Ok(report)
}

pub fn evidence_for_company(report_path: &Path, company: &str, domain: &str) -> Vec<String> {
    let Ok(file) = File::open(report_path) else {
        return Vec::new();
    };
    let Ok(report) = serde_json::from_reader::<_, MatchReport>(BufReader::new(file)) else {
        return Vec::new();
    };
    let company_key = normalize(company);
    let domain_key = domain
        .trim()
        .trim_start_matches("www.")
        .to_ascii_lowercase();
    report
        .matches
        .iter()
        .filter(|matched| company_matches_network(&company_key, &domain_key, matched))
        .take(3)
        .flat_map(|matched| {
            let location = format_location(matched);
            [
                format!(
                    "[{}] A verified source lists {} {} {} at {}{}.",
                    matched.station_source_url,
                    if matched.company.trim().is_empty() { company } else { &matched.company },
                    if matched.location_kind.trim().is_empty() { "operating location" } else { &matched.location_kind },
                    matched.station_name,
                    location,
                    if matched.network.trim().is_empty() { String::new() } else { format!(", on the {} network", matched.network) }
                ),
                format!(
                    "[{}] Completed historical geospatial result: the verified {} at {} fell inside {}'s reported utility outage area beginning {}. This is outside utility context only, not evidence of private site or asset status or cause.",
                    matched.outage_source_url,
                    if matched.location_kind.trim().is_empty() { "operating location" } else { &matched.location_kind },
                    location,
                    matched.utility_provider,
                    matched.outage_start_utc
                ),
            ]
        })
        .collect()
}

fn company_matches_network(company_key: &str, domain: &str, matched: &LocationOutageMatch) -> bool {
    let matched_company = normalize(&matched.company);
    let matched_domain = matched
        .domain
        .trim()
        .trim_start_matches("www.")
        .to_ascii_lowercase();
    if (!matched_company.is_empty()
        && (company_key.contains(&matched_company) || matched_company.contains(company_key)))
        || (!matched_domain.is_empty()
            && (domain == matched_domain || domain.ends_with(&format!(".{matched_domain}"))))
    {
        return true;
    }
    let network = normalize(&matched.network);
    if !network.is_empty()
        && network != "nonnetworked"
        && (company_key.contains(&network)
            || network.contains(company_key)
            || network_domain_alias(&network)
                .iter()
                .any(|alias| domain == *alias || domain.ends_with(&format!(".{alias}"))))
    {
        return true;
    }
    let station = normalize(&matched.station_name);
    company_key.len() >= 5
        && station.len() >= 5
        && (station.contains(company_key) || company_key.contains(&station))
}

fn network_domain_alias(network: &str) -> &'static [&'static str] {
    match network {
        "chargepointnetwork" => &["chargepoint.com"],
        "flo" => &["flo.com"],
        "swtch" => &["swtchenergy.com"],
        "tesla" | "tesladestination" => &["tesla.com"],
        "chargelab" => &["chargelab.co"],
        "ivy" => &["ivycharge.com"],
        "opconnect" => &["opconnect.com"],
        "evconnect" => &["evconnect.com"],
        "jule" => &["julepower.com"],
        "circuitelectrique" => &["lecircuitelectrique.com"],
        "shellrecharge" => &["shell.ca", "shell.com"],
        "electrifycanada" => &["electrify-canada.ca"],
        "ontherunev" => &["ontherunstores.com", "parkland.ca"],
        "couchetard" => &["circlek.com", "couche-tard.com"],
        "evgateway" => &["evgateway.com"],
        "loop" => &["loopglobal.com"],
        "honeybadger" => &["honeybadgercharging.com"],
        "chargeup" => &["chargeupcanada.ca"],
        _ => &[],
    }
}

async fn fetch_canadian_stations() -> Result<Vec<Station>> {
    let http = reqwest::Client::builder()
        .user_agent("spruce-leaf/0.1 evidence research")
        .build()?;
    let locator = http
        .get(STATION_LOCATOR_URL)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let api_key = extract_public_api_key(&locator)
        .context("NRCan station locator did not expose its public client API key")?;
    let payload = http
        .get(STATION_API_URL)
        .header(reqwest::header::REFERER, STATION_LOCATOR_URL)
        .header(
            reqwest::header::ORIGIN,
            "https://natural-resources.canada.ca",
        )
        .query(&[
            ("api_key", api_key.as_str()),
            ("country", "CA"),
            ("fuel_type", "ELEC"),
            ("access", "public"),
            ("status", "E"),
            ("limit", "all"),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<StationPayload>()
        .await?;
    Ok(payload
        .fuel_stations
        .into_iter()
        .map(|mut station| {
            station.location_id = format!("nrcan-ev-{}", station.id);
            station.location_kind = "public charging location".into();
            station.source_url = STATION_LOCATOR_URL.into();
            station
        })
        .collect())
}

fn extract_public_api_key(page: &str) -> Option<String> {
    let after = page.split_once("apiKey:")?.1.trim_start();
    let quote = after.chars().next()?;
    if !matches!(quote, '\'' | '"') {
        return None;
    }
    let value = &after[quote.len_utf8()..];
    let end = value.find(quote)?;
    Some(value[..end].to_string())
}

struct ArchiveSeed<'a> {
    index: &'a StationIndex,
    matches: &'a mut Vec<LocationOutageMatch>,
    matched_station_ids: &'a mut HashSet<String>,
}

impl<'de> DeserializeSeed<'de> for ArchiveSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ArchiveVisitor {
            index: self.index,
            matches: self.matches,
            matched_station_ids: self.matched_station_ids,
        })
    }
}

struct ArchiveVisitor<'a> {
    index: &'a StationIndex,
    matches: &'a mut Vec<LocationOutageMatch>,
    matched_station_ids: &'a mut HashSet<String>,
}

impl<'de> Visitor<'de> for ArchiveVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an OutageHub archive object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut found = false;
        while let Some(key) = map.next_key::<String>()? {
            if key == "outages" {
                found = true;
                map.next_value_seed(OutagesSeed {
                    index: self.index,
                    matches: &mut *self.matches,
                    matched_station_ids: &mut *self.matched_station_ids,
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        if !found {
            return Err(serde::de::Error::missing_field("outages"));
        }
        Ok(())
    }
}

struct OutagesSeed<'a> {
    index: &'a StationIndex,
    matches: &'a mut Vec<LocationOutageMatch>,
    matched_station_ids: &'a mut HashSet<String>,
}

impl<'de> DeserializeSeed<'de> for OutagesSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(OutagesVisitor {
            index: self.index,
            matches: self.matches,
            matched_station_ids: self.matched_station_ids,
        })
    }
}

struct OutagesVisitor<'a> {
    index: &'a StationIndex,
    matches: &'a mut Vec<LocationOutageMatch>,
    matched_station_ids: &'a mut HashSet<String>,
}

impl<'de> Visitor<'de> for OutagesVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of OutageHub observations")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(outage) = sequence.next_element::<OutageRow>()? {
            let Some(wkt) = outage.polygon.as_deref() else {
                continue;
            };
            let polygons = parse_wkt(wkt);
            let Some(bounds) = polygon_bounds(&polygons) else {
                continue;
            };
            for station_index in self.index.candidates(bounds) {
                let station = &self.index.stations[station_index];
                let location_identity = if !station.location_id.trim().is_empty() {
                    station.location_id.clone()
                } else if station.id != 0 {
                    format!("station-id:{}", station.id)
                } else {
                    format!(
                        "{}|{}|{:.6}|{:.6}",
                        station.company,
                        station.street_address,
                        station.latitude,
                        station.longitude
                    )
                };
                if self.matched_station_ids.contains(&location_identity)
                    || !point_in_polygons((station.longitude, station.latitude), &polygons)
                {
                    continue;
                }
                self.matched_station_ids.insert(location_identity);
                self.matches.push(LocationOutageMatch {
                    location_id: if station.location_id.trim().is_empty() {
                        station.id.to_string()
                    } else {
                        station.location_id.clone()
                    },
                    company: station.company.clone(),
                    domain: station.domain.clone(),
                    location_kind: station.location_kind.clone(),
                    operating_relationship: station.operating_relationship.clone(),
                    station_id: station.id,
                    station_name: station.station_name.clone(),
                    network: station.ev_network.clone(),
                    address: station.street_address.clone(),
                    city: station.city.clone(),
                    province: station.state.clone(),
                    latitude: station.latitude,
                    longitude: station.longitude,
                    outage_id: outage.id,
                    utility_provider: outage.provider.clone(),
                    outage_start_ts: outage.start_ts,
                    outage_end_ts: outage.end_ts,
                    outage_start_utc: DateTime::<Utc>::from_timestamp(outage.start_ts, 0)
                        .map(|value| value.to_rfc3339())
                        .unwrap_or_else(|| outage.start_ts.to_string()),
                    station_source_url: if station.source_url.trim().is_empty() {
                        STATION_LOCATOR_URL.into()
                    } else {
                        station.source_url.clone()
                    },
                    geocode_source_url: station.geocode_source_url.clone(),
                    geocoding_attribution: station.geocoding_attribution.clone(),
                    outage_source_url: format!("https://api.outagehub.ca/v1/outages/{}", outage.id),
                });
            }
        }
        Ok(())
    }
}

fn parse_wkt(wkt: &str) -> Vec<Polygon> {
    let value = wkt.trim();
    if let Some(body) = value.strip_prefix("POLYGON") {
        let Some(body) = strip_outer_parentheses(body.trim()) else {
            return Vec::new();
        };
        return vec![parse_polygon(body)];
    }
    if let Some(body) = value.strip_prefix("MULTIPOLYGON") {
        let Some(body) = strip_outer_parentheses(body.trim()) else {
            return Vec::new();
        };
        return split_top_level(body)
            .into_iter()
            .filter_map(|polygon| strip_outer_parentheses(polygon.trim()))
            .map(parse_polygon)
            .filter(|polygon| !polygon.is_empty())
            .collect();
    }
    Vec::new()
}

fn parse_polygon(body: &str) -> Polygon {
    split_top_level(body)
        .into_iter()
        .filter_map(|ring| strip_outer_parentheses(ring.trim()))
        .map(parse_ring)
        .filter(|ring| ring.len() >= 3)
        .collect()
}

fn parse_ring(body: &str) -> Vec<Point> {
    body.split(',')
        .filter_map(|pair| {
            let mut numbers = pair.split_whitespace();
            Some((numbers.next()?.parse().ok()?, numbers.next()?.parse().ok()?))
        })
        .collect()
}

fn strip_outer_parentheses(value: &str) -> Option<&str> {
    let value = value.trim();
    value.strip_prefix('(')?.strip_suffix(')')
}

fn split_top_level(value: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (index, character) in value.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&value[start..index]);
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&value[start..]);
    parts
}

fn polygon_bounds(polygons: &[Polygon]) -> Option<Bounds> {
    let mut points = polygons.iter().flat_map(|polygon| polygon.iter().flatten());
    let first = *points.next()?;
    let mut bounds = Bounds {
        min_lon: first.0,
        max_lon: first.0,
        min_lat: first.1,
        max_lat: first.1,
    };
    for point in points {
        bounds.min_lon = bounds.min_lon.min(point.0);
        bounds.max_lon = bounds.max_lon.max(point.0);
        bounds.min_lat = bounds.min_lat.min(point.1);
        bounds.max_lat = bounds.max_lat.max(point.1);
    }
    Some(bounds)
}

fn point_in_polygons(point: Point, polygons: &[Polygon]) -> bool {
    polygons.iter().any(|polygon| {
        let Some(exterior) = polygon.first() else {
            return false;
        };
        point_in_ring(point, exterior)
            && !polygon
                .iter()
                .skip(1)
                .any(|hole| point_in_ring(point, hole))
    })
}

fn point_in_ring(point: Point, ring: &[Point]) -> bool {
    let mut inside = false;
    let mut previous = ring.len().saturating_sub(1);
    for current in 0..ring.len() {
        let (x1, y1) = ring[current];
        let (x2, y2) = ring[previous];
        let crosses = (y1 > point.1) != (y2 > point.1)
            && point.0 < (x2 - x1) * (point.1 - y1) / (y2 - y1) + x1;
        if crosses {
            inside = !inside;
        }
        previous = current;
    }
    inside
}

fn grid_cell(latitude: f64, longitude: f64) -> (i16, i16) {
    (latitude.floor() as i16, longitude.floor() as i16)
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn format_location(matched: &LocationOutageMatch) -> String {
    [
        matched.address.trim(),
        matched.city.trim(),
        matched.province.trim(),
    ]
    .into_iter()
    .filter(|part| !part.is_empty())
    .collect::<Vec<_>>()
    .join(", ")
}

#[cfg(test)]
mod tests {
    use super::{
        build_report_from_locations, company_matches_network, extract_public_api_key,
        ingest_researched_locations, parse_wkt, point_in_polygons, LocationOutageMatch,
        MatchReport, Station, VerifiedLocationCandidate,
    };

    #[test]
    fn extracts_the_public_widget_key_without_baking_it_into_source() {
        assert_eq!(
            extract_public_api_key("window.options = { apiKey: 'public-key' };").as_deref(),
            Some("public-key")
        );
    }

    #[test]
    fn handles_polygon_holes_and_multipolygons() {
        let polygon = parse_wkt("POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,6 4,6 6,4 6,4 4))");
        assert!(point_in_polygons((2.0, 2.0), &polygon));
        assert!(!point_in_polygons((5.0, 5.0), &polygon));
        let multi =
            parse_wkt("MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((10 10,11 10,11 11,10 11,10 10)))");
        assert!(point_in_polygons((10.5, 10.5), &multi));
    }

    #[test]
    fn maps_known_networks_to_their_company_domains() {
        let matched = LocationOutageMatch {
            location_id: "nrcan-ev-1".into(),
            company: String::new(),
            domain: String::new(),
            location_kind: "public charging location".into(),
            station_id: 1,
            network: "SWTCH".into(),
            station_name: "Apartment charger".into(),
            address: String::new(),
            city: String::new(),
            province: String::new(),
            latitude: 0.0,
            longitude: 0.0,
            outage_id: 1,
            utility_provider: "Hydro One".into(),
            outage_start_ts: 1,
            outage_end_ts: 2,
            outage_start_utc: String::new(),
            station_source_url: String::new(),
            outage_source_url: String::new(),
            ..Default::default()
        };
        assert!(company_matches_network(
            "swtchenergyinc",
            "swtchenergy.com",
            &matched
        ));
        assert!(!company_matches_network(
            "unrelated",
            "unrelated.com",
            &matched
        ));
    }

    #[test]
    fn maps_generic_verified_properties_to_the_operator() {
        let matched = LocationOutageMatch {
            location_id: "dynacare-lab-1".into(),
            company: "Dynacare".into(),
            domain: "dynacare.ca".into(),
            location_kind: "laboratory".into(),
            station_name: "Toronto laboratory".into(),
            ..Default::default()
        };
        assert!(company_matches_network("dynacare", "dynacare.ca", &matched));
        assert!(!company_matches_network(
            "anotheroperator",
            "example.ca",
            &matched
        ));
    }

    #[test]
    fn generic_locations_with_provider_default_ids_are_matched_independently() {
        let run = uuid::Uuid::new_v4();
        let archive = std::env::temp_dir().join(format!("outage-archive-{run}.json"));
        let output = std::env::temp_dir().join(format!("outage-report-{run}.json"));
        serde_json::to_writer(
            std::fs::File::create(&archive).unwrap(),
            &serde_json::json!({
                "outages": [{
                    "id": 1,
                    "provider": "Test Utility",
                    "polygon": "POLYGON((-80 43,-78 43,-78 45,-80 45,-80 43))",
                    "startTs": 1786636800,
                    "endTs": 1786640400
                }]
            }),
        )
        .unwrap();
        let station = |location_id: &str, name: &str, latitude: f64, longitude: f64| Station {
            location_id: location_id.into(),
            company: "Dynacare".into(),
            domain: "dynacare.ca".into(),
            location_kind: "laboratory".into(),
            station_name: name.into(),
            street_address: format!("{name} address"),
            city: "Toronto".into(),
            state: "Ontario".into(),
            latitude,
            longitude,
            source_url: "https://dynacare.ca/locations".into(),
            ..Default::default()
        };
        let report = build_report_from_locations(
            &archive,
            &output,
            vec![
                station("dynacare-lab-1", "Lab one", 43.7, -79.4),
                station("dynacare-lab-2", "Lab two", 44.0, -79.0),
            ],
            "https://dynacare.ca/locations",
        )
        .unwrap();
        assert_eq!(report.matches.len(), 2);
        let _ = std::fs::remove_file(archive);
        let _ = std::fs::remove_file(output);
    }

    #[tokio::test]
    async fn researched_location_intake_reuses_cache_and_produces_company_evidence() {
        let run = uuid::Uuid::new_v4();
        let root = std::env::temp_dir().join(format!("outage-intake-{run}"));
        let archive = root.join("archive.json");
        let inventory = root.join("verified-locations.json");
        let output = root.join("matches.json");
        std::fs::create_dir_all(&root).unwrap();
        serde_json::to_writer(
            std::fs::File::create(&archive).unwrap(),
            &serde_json::json!({
                "outages": [{
                    "id": 42,
                    "provider": "Test Utility",
                    "polygon": "POLYGON((-80 43,-78 43,-78 45,-80 45,-80 43))",
                    "startTs": 1786636800,
                    "endTs": 1786640400
                }]
            }),
        )
        .unwrap();
        serde_json::to_writer(
            std::fs::File::create(&inventory).unwrap(),
            &vec![Station {
                location_id: "verified-lab-one".into(),
                company: "Dynacare".into(),
                domain: "dynacare.ca".into(),
                location_kind: "laboratory".into(),
                station_name: "Lab one".into(),
                street_address: "123 King Street West".into(),
                city: "Toronto".into(),
                state: "Ontario".into(),
                latitude: 43.7,
                longitude: -79.4,
                source_url: "https://dynacare.ca/locations".into(),
                geocode_source_url: "https://nominatim.openstreetmap.org/search?q=cached".into(),
                geocoding_attribution: "Data © OpenStreetMap contributors, ODbL 1.0".into(),
                ..Default::default()
            }],
        )
        .unwrap();
        let evidence = ingest_researched_locations(
            "Dynacare",
            "dynacare.ca",
            &[VerifiedLocationCandidate {
                relationship: "operated".into(),
                location_kind: "laboratory".into(),
                name: "Lab one".into(),
                street_address: "123 King Street West".into(),
                city: "Toronto".into(),
                province: "Ontario".into(),
                postal_code: "M5V 1A1".into(),
                source_url: "https://dynacare.ca/locations".into(),
                source_excerpt: "Lab one — 123 King Street West, Toronto, Ontario".into(),
            }],
            &archive,
            &inventory,
            &output,
        )
        .await
        .unwrap();
        assert!(evidence
            .iter()
            .any(|fact| fact.contains("Completed historical geospatial result")));
        let report: MatchReport =
            serde_json::from_reader(std::fs::File::open(&output).unwrap()).unwrap();
        assert_eq!(report.matches.len(), 1);
        assert_eq!(report.matches[0].operating_relationship, "operated");
        assert!(report.geocoding_attribution.contains("OpenStreetMap"));
        let _ = std::fs::remove_dir_all(root);
    }
}
