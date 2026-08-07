//! Business-owned, recipient-local outreach scheduling.
//!
//! The model may suggest sequence spacing, but it does not get to invent send
//! times. This module turns a desired `day_offset` into an auditable calendar
//! slot using the business profile, the recipient's timezone, named
//! industry/title hypotheses, and a hard per-business daily capacity.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::Tz;

use crate::business::{BusinessProfile, TimingRule};
use crate::db::{SharedDb, TimingObservation};

#[derive(Debug, Clone, Default)]
pub struct TimingContext<'a> {
    pub industry: &'a str,
    pub title: &'a str,
    pub vantage: &'a str,
    pub channel: &'a str,
    pub location: &'a str,
    /// An already-resolved IANA timezone. When empty, `location` is mapped and
    /// then the business fallback is used.
    pub timezone: &'a str,
    /// Person/touch identity used only to spread activity across preferred
    /// hours and minutes deterministically.
    pub stable_key: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalendarSlot {
    pub at: DateTime<Utc>,
    pub recipient_timezone: String,
    pub rule: String,
    pub rationale: String,
}

#[derive(Debug, Clone)]
struct EffectivePolicy {
    weekdays: Vec<Weekday>,
    preferred_hours: Vec<u32>,
    window_start: u32,
    window_end: u32,
    rule: String,
    rationale: String,
}

/// Allocate the first policy-valid slot whose business-local date still has
/// capacity. `planned_count` lets callers count both automatic email and manual
/// tasks without coupling this pure calendar module to SQLite.
pub fn schedule_with_capacity<F>(
    profile: &BusinessProfile,
    context: &TimingContext<'_>,
    not_before: DateTime<Utc>,
    mut planned_count: F,
) -> Result<CalendarSlot>
where
    F: FnMut(DateTime<Utc>, DateTime<Utc>) -> Result<usize>,
{
    let mut cursor = not_before;
    for _ in 0..370 {
        let slot = next_slot(profile, context, cursor)?;
        let (start, end, _) = quota_day_bounds(profile, slot.at)?;
        if planned_count(start, end)? < profile.calendar.daily_touch_cap {
            return Ok(slot);
        }
        cursor = end;
    }
    Err(anyhow!(
        "no outreach capacity found for {} in the next year",
        profile.name
    ))
}

/// Find the next recipient-local slot allowed by the matching timing rule.
pub fn next_slot(
    profile: &BusinessProfile,
    context: &TimingContext<'_>,
    not_before: DateTime<Utc>,
) -> Result<CalendarSlot> {
    let tz = recipient_timezone(profile, context);
    let policy = effective_policy(profile, context)?;
    let local_start = not_before.with_timezone(&tz);
    let mut date = local_start.date_naive();

    for _ in 0..32 {
        if policy.weekdays.contains(&date.weekday()) {
            let mut hours = policy.preferred_hours.clone();
            let hours_len = hours.len();
            rotate(
                &mut hours,
                stable_index(context.stable_key, &date.to_string(), hours_len),
            );
            for hour in hours {
                let minute = stable_minute(context.stable_key, &date.to_string(), hour);
                let Some(local) = tz
                    .with_ymd_and_hms(date.year(), date.month(), date.day(), hour, minute, 0)
                    .single()
                else {
                    continue;
                };
                let candidate = local.with_timezone(&Utc);
                if candidate >= not_before {
                    return Ok(CalendarSlot {
                        at: candidate,
                        recipient_timezone: tz.name().to_string(),
                        rule: policy.rule,
                        rationale: policy.rationale,
                    });
                }
            }
        }
        date = date
            .succ_opt()
            .ok_or_else(|| anyhow!("calendar date overflow"))?;
    }

    Err(anyhow!(
        "no recipient-local outreach slot found for {} within 32 days",
        profile.name
    ))
}

/// Whether `now` is inside the effective recipient-local window. The daemon
/// uses this as a final guard for old/imported/late-approved touches.
pub fn can_send_now(
    profile: &BusinessProfile,
    context: &TimingContext<'_>,
    now: DateTime<Utc>,
) -> Result<bool> {
    let tz = recipient_timezone(profile, context);
    let local = now.with_timezone(&tz);
    let policy = effective_policy(profile, context)?;
    Ok(policy.weekdays.contains(&local.weekday())
        && local.hour() >= policy.window_start
        && local.hour() < policy.window_end)
}

/// UTC bounds for the quota calendar date containing `at`. Constructing both
/// local midnights independently keeps the boundary correct across DST changes.
pub fn quota_day_bounds(
    profile: &BusinessProfile,
    at: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>, String)> {
    let tz = profile.calendar.quota_timezone.parse::<Tz>().map_err(|_| {
        anyhow!(
            "invalid quota timezone '{}'",
            profile.calendar.quota_timezone
        )
    })?;
    let date = at.with_timezone(&tz).date_naive();
    let next = date
        .succ_opt()
        .ok_or_else(|| anyhow!("calendar date overflow"))?;
    let start = local_midnight(tz, date)?;
    let end = local_midnight(tz, next)?;
    Ok((
        start.with_timezone(&Utc),
        end.with_timezone(&Utc),
        date.to_string(),
    ))
}

pub fn recipient_timezone(profile: &BusinessProfile, context: &TimingContext<'_>) -> Tz {
    context.timezone.parse::<Tz>().ok().unwrap_or_else(|| {
        timezone_for_location(
            context.location,
            &profile.calendar.fallback_recipient_timezone,
        )
    })
}

/// Resolve the common Apollo location shapes without an external geocoding
/// dependency. Ambiguous or unknown locations intentionally fall back to the
/// business profile so they remain visible as lower-confidence scheduling data.
pub fn timezone_for_location(location: &str, fallback: &str) -> Tz {
    let fallback = fallback.parse::<Tz>().unwrap_or(chrono_tz::UTC);
    let parts = location
        .split(',')
        .map(|part| part.trim().to_ascii_lowercase())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let has = |values: &[&str]| parts.iter().any(|part| values.contains(&part.as_str()));

    // Resolve country-qualified Australian abbreviations before North American
    // state/province abbreviations such as WA.
    if has(&["australia"]) {
        if has(&["western australia", "wa"]) {
            return chrono_tz::Australia::Perth;
        }
        if has(&["queensland", "qld"]) {
            return chrono_tz::Australia::Brisbane;
        }
        return chrono_tz::Australia::Sydney;
    }

    // Canada (Apollo usually supplies province names).
    if has(&["british columbia", "bc"]) {
        return chrono_tz::America::Vancouver;
    }
    if has(&["alberta", "ab"]) {
        return chrono_tz::America::Edmonton;
    }
    if has(&["saskatchewan", "sk"]) {
        return chrono_tz::America::Regina;
    }
    if has(&["manitoba", "mb"]) {
        return chrono_tz::America::Winnipeg;
    }
    if has(&["ontario", "on"]) {
        return chrono_tz::America::Toronto;
    }
    if has(&["quebec", "québec", "qc"]) {
        return chrono_tz::America::Montreal;
    }
    if has(&[
        "nova scotia",
        "new brunswick",
        "prince edward island",
        "ns",
        "nb",
        "pe",
    ]) {
        return chrono_tz::America::Halifax;
    }
    if has(&["newfoundland and labrador", "newfoundland", "nl"]) {
        return chrono_tz::America::St_Johns;
    }
    if has(&["yukon", "yt"]) {
        return chrono_tz::America::Whitehorse;
    }

    // United States. State groupings are intentionally explicit so a city name
    // cannot accidentally match a two-letter abbreviation.
    if has(&[
        "california",
        "washington",
        "oregon",
        "nevada",
        "ca",
        "wa",
        "or",
        "nv",
    ]) {
        return chrono_tz::America::Los_Angeles;
    }
    if has(&["arizona", "az"]) {
        return chrono_tz::America::Phoenix;
    }
    if has(&[
        "colorado",
        "utah",
        "montana",
        "wyoming",
        "new mexico",
        "idaho",
        "co",
        "ut",
        "mt",
        "wy",
        "nm",
        "id",
    ]) {
        return chrono_tz::America::Denver;
    }
    if has(&[
        "texas",
        "illinois",
        "wisconsin",
        "minnesota",
        "iowa",
        "missouri",
        "arkansas",
        "louisiana",
        "oklahoma",
        "kansas",
        "nebraska",
        "north dakota",
        "south dakota",
        "alabama",
        "mississippi",
        "tennessee",
        "tx",
        "il",
        "wi",
        "mn",
        "ia",
        "mo",
        "ar",
        "la",
        "ok",
        "ks",
        "ne",
        "nd",
        "sd",
        "al",
        "ms",
        "tn",
    ]) {
        return chrono_tz::America::Chicago;
    }
    if has(&["alaska", "ak"]) {
        return chrono_tz::America::Anchorage;
    }
    if has(&["hawaii", "hi"]) {
        return chrono_tz::Pacific::Honolulu;
    }
    if has(&[
        "united states",
        "usa",
        "us",
        "new york",
        "massachusetts",
        "florida",
        "georgia",
        "pennsylvania",
        "ohio",
        "michigan",
        "new jersey",
        "virginia",
        "north carolina",
        "south carolina",
        "maryland",
        "connecticut",
        "maine",
        "vermont",
        "new hampshire",
        "rhode island",
        "delaware",
        "district of columbia",
        "west virginia",
        "indiana",
        "kentucky",
        "ny",
        "ma",
        "fl",
        "ga",
        "pa",
        "oh",
        "mi",
        "nj",
        "va",
        "nc",
        "sc",
        "md",
        "ct",
        "me",
        "vt",
        "nh",
        "ri",
        "de",
        "dc",
        "wv",
        "in",
        "ky",
    ]) {
        return chrono_tz::America::New_York;
    }

    if has(&[
        "united kingdom",
        "uk",
        "england",
        "scotland",
        "wales",
        "northern ireland",
    ]) {
        return chrono_tz::Europe::London;
    }
    if has(&["ireland"]) {
        return chrono_tz::Europe::Dublin;
    }
    if has(&["france"]) {
        return chrono_tz::Europe::Paris;
    }
    if has(&["germany"]) {
        return chrono_tz::Europe::Berlin;
    }
    if has(&["netherlands"]) {
        return chrono_tz::Europe::Amsterdam;
    }
    if has(&["spain"]) {
        return chrono_tz::Europe::Madrid;
    }
    if has(&[
        "new south wales",
        "victoria",
        "tasmania",
        "act",
        "nsw",
        "vic",
    ]) {
        return chrono_tz::Australia::Sydney;
    }
    if has(&["queensland", "qld"]) {
        return chrono_tz::Australia::Brisbane;
    }
    if has(&["western australia", "wa australia"]) {
        return chrono_tz::Australia::Perth;
    }
    if has(&["new zealand"]) {
        return chrono_tz::Pacific::Auckland;
    }

    fallback
}

pub fn policy_summary(profile: &BusinessProfile) -> String {
    let weekend_rules = profile
        .calendar
        .rules
        .iter()
        .filter(|rule| {
            rule.weekdays
                .iter()
                .any(|day| matches!(day.to_ascii_lowercase().as_str(), "sat" | "sun"))
        })
        .map(|rule| rule.key.as_str())
        .collect::<Vec<_>>();
    format!(
        "{}: max {} touchpoints/day in {}; default {} at {:?}:00 recipient-local; weekend only via [{}]",
        profile.name,
        profile.calendar.daily_touch_cap,
        profile.calendar.quota_timezone,
        profile.calendar.weekdays.join("/"),
        profile.calendar.preferred_hours,
        weekend_rules.join(", "),
    )
}

/// Compact context injected into the router agent on every turn. Configured
/// timing rules remain hypotheses; observed performance is surfaced only after
/// the profile's minimum sample threshold.
pub fn agent_intelligence(profile: &BusinessProfile, db: &SharedDb) -> Result<String> {
    let observations = db.timing_observations(&profile.key)?;
    if observations.len() < profile.calendar.learning_min_samples {
        return Ok(format!(
            "{} Calendar evidence: {}/{} sends observed, so use configured timing hypotheses and do not claim a learned best time yet.",
            policy_summary(profile),
            observations.len(),
            profile.calendar.learning_min_samples,
        ));
    }
    let (day, sends, replies) = best_weekday(&observations).unwrap_or(("unknown".into(), 0, 0));
    Ok(format!(
        "{} Directional history: {}/{} replies after sends; strongest observed recipient-local weekday is {} ({}/{}). Treat this as correlation and inspect cohorts before changing policy.",
        policy_summary(profile),
        observations.iter().filter(|observation| observation.replied).count(),
        observations.len(),
        day,
        replies,
        sends,
    ))
}

pub fn render_intelligence(
    profile: &BusinessProfile,
    db: &SharedDb,
    now: DateTime<Utc>,
) -> Result<String> {
    let mut out = String::new();
    out.push_str(&format!("outreach calendar [{}]\n", profile.key));
    out.push_str(&format!("• {}\n", policy_summary(profile)));
    out.push_str(&format!(
        "• local window {}:00–{}:00; learning threshold {} sends\n",
        profile.calendar.window_start,
        profile.calendar.window_end,
        profile.calendar.learning_min_samples,
    ));
    if profile.calendar.rules.is_empty() {
        out.push_str("• no cohort overrides configured\n");
    } else {
        out.push_str("\nconfigured timing hypotheses\n");
        for rule in &profile.calendar.rules {
            out.push_str(&format!(
                "• {} — {} at {:?}:00\n  {}\n",
                rule.key,
                if rule.weekdays.is_empty() {
                    profile.calendar.weekdays.join("/")
                } else {
                    rule.weekdays.join("/")
                },
                if rule.preferred_hours.is_empty() {
                    &profile.calendar.preferred_hours
                } else {
                    &rule.preferred_hours
                },
                rule.rationale,
            ));
        }
    }

    out.push_str("\nused/reserved capacity (draft + scheduled + sent, all channels)\n");
    let mut seen_dates = Vec::<String>::new();
    let mut day_offset = 0i64;
    while seen_dates.len() < 7 && day_offset < 10 {
        let (start, end, date) =
            quota_day_bounds(profile, now + chrono::Duration::days(day_offset))?;
        if !seen_dates.contains(&date) {
            let planned = db.planned_touch_count_between(&profile.key, start, end)?;
            out.push_str(&format!(
                "• {date}: {planned}/{} used or reserved\n",
                profile.calendar.daily_touch_cap
            ));
            seen_dates.push(date);
        }
        day_offset += 1;
    }

    let upcoming = db.upcoming_calendar(&profile.key, 20)?;
    out.push_str("\nupcoming touchpoints\n");
    if upcoming.is_empty() {
        out.push_str("• none planned\n");
    } else {
        for entry in upcoming {
            let local_due = display_local_due(&entry.due_at, &entry.recipient_timezone);
            out.push_str(&format!(
                "• {} [{} / {} / {}] {} @ {} — {}{}\n",
                local_due,
                entry.motion,
                entry.channel,
                entry.status,
                entry.recipient,
                entry.account,
                entry.purpose,
                if entry.scheduled_rule.is_empty() {
                    String::new()
                } else {
                    format!(" · rule {}", entry.scheduled_rule)
                },
            ));
        }
    }

    let observations = db.timing_observations(&profile.key)?;
    let replies = observations
        .iter()
        .filter(|observation| observation.replied)
        .count();
    out.push_str(&format!(
        "\nobserved response timing\n• {}/{} sends were the last touch before a reply\n",
        replies,
        observations.len()
    ));
    if observations.len() < profile.calendar.learning_min_samples {
        out.push_str(&format!(
            "• insufficient sample for a learned recommendation ({}/{}); configured rules remain explicit test hypotheses\n",
            observations.len(), profile.calendar.learning_min_samples
        ));
    } else {
        for (label, sends, replies) in top_weekdays(&observations, 4) {
            out.push_str(&format!(
                "• {label}: {replies}/{sends} ({:.0}%)\n",
                replies as f64 / sends.max(1) as f64 * 100.0
            ));
        }
        out.push_str("• rates are directional correlations, not causal lift; inspect industry/title cohorts before editing a rule\n");
        let cohorts = top_cohorts(&observations, 5);
        if !cohorts.is_empty() {
            out.push_str("\nindustry / title-vantage cohorts (3+ sends)\n");
            for (cohort, sends, replies) in cohorts {
                out.push_str(&format!("• {cohort}: {replies}/{sends}\n"));
            }
        }
    }

    let mut rule_stats = BTreeMap::<String, (usize, usize)>::new();
    for observation in &observations {
        let key = if observation.scheduled_rule.trim().is_empty() {
            "legacy/unlabelled".to_string()
        } else {
            observation.scheduled_rule.clone()
        };
        let entry = rule_stats.entry(key).or_default();
        entry.0 += 1;
        entry.1 += usize::from(observation.replied);
    }
    if !rule_stats.is_empty() {
        out.push_str("\nrule observations\n");
        for (rule, (sends, replies)) in rule_stats {
            out.push_str(&format!("• {rule}: {replies}/{sends}\n"));
        }
    }
    Ok(out.trim_end().to_string())
}

fn display_local_due(due_at: &str, timezone: &str) -> String {
    let Ok(due) = DateTime::parse_from_rfc3339(due_at) else {
        return due_at.to_string();
    };
    let Ok(tz) = timezone.parse::<Tz>() else {
        return format!("{due_at} (timezone fallback)");
    };
    format!(
        "{} ({})",
        due.with_timezone(&tz).format("%Y-%m-%d %a %H:%M"),
        tz.name()
    )
}

fn best_weekday(observations: &[TimingObservation]) -> Option<(String, usize, usize)> {
    top_weekdays(observations, 1).into_iter().next()
}

fn top_weekdays(observations: &[TimingObservation], limit: usize) -> Vec<(String, usize, usize)> {
    let mut groups = BTreeMap::<String, (usize, usize)>::new();
    for observation in observations {
        let Ok(sent) = DateTime::parse_from_rfc3339(&observation.sent_at) else {
            continue;
        };
        let tz = observation.timezone.parse::<Tz>().unwrap_or(chrono_tz::UTC);
        let day = sent
            .with_timezone(&tz)
            .format("%a")
            .to_string()
            .to_ascii_lowercase();
        let entry = groups.entry(day).or_default();
        entry.0 += 1;
        entry.1 += usize::from(observation.replied);
    }
    let mut groups = groups
        .into_iter()
        .map(|(day, (sends, replies))| (day, sends, replies))
        .collect::<Vec<_>>();
    // A small beta prior avoids declaring a one-for-one bucket the winner.
    groups.sort_by(|left, right| {
        let left_score = (left.2 as f64 + 1.0) / (left.1 as f64 + 4.0);
        let right_score = (right.2 as f64 + 1.0) / (right.1 as f64 + 4.0);
        right_score
            .partial_cmp(&left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.1.cmp(&left.1))
    });
    groups.truncate(limit);
    groups
}

fn top_cohorts(observations: &[TimingObservation], limit: usize) -> Vec<(String, usize, usize)> {
    let mut groups = BTreeMap::<String, (usize, usize)>::new();
    for observation in observations {
        let industry = if observation.industry.trim().is_empty() {
            "unknown industry"
        } else {
            observation.industry.trim()
        };
        let role = if !observation.vantage.trim().is_empty() {
            observation.vantage.trim().to_string()
        } else {
            title_family(&observation.title).to_string()
        };
        let entry = groups.entry(format!("{industry} / {role}")).or_default();
        entry.0 += 1;
        entry.1 += usize::from(observation.replied);
    }
    let mut groups = groups
        .into_iter()
        .filter(|(_, (sends, _))| *sends >= 3)
        .map(|(cohort, (sends, replies))| (cohort, sends, replies))
        .collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        let left_score = (left.2 as f64 + 1.0) / (left.1 as f64 + 4.0);
        let right_score = (right.2 as f64 + 1.0) / (right.1 as f64 + 4.0);
        right_score
            .partial_cmp(&left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.1.cmp(&left.1))
    });
    groups.truncate(limit);
    groups
}

fn title_family(title: &str) -> &'static str {
    let title = title.to_ascii_lowercase();
    if [
        "plant",
        "production",
        "packaging",
        "maintenance",
        "warehouse",
        "shift",
    ]
    .iter()
    .any(|term| title.contains(term))
    {
        "plant_operations"
    } else if ["program", "programme", "funding", "grant", "advisor"]
        .iter()
        .any(|term| title.contains(term))
    {
        "funding_programme"
    } else if ["engineering", "technology", "data", "systems", "technical"]
        .iter()
        .any(|term| title.contains(term))
    {
        "technical"
    } else if ["chief", "president", "owner", "vice president", "vp"]
        .iter()
        .any(|term| title.contains(term))
    {
        "executive"
    } else if [
        "operations",
        "incident",
        "dispatch",
        "continuity",
        "reliability",
    ]
    .iter()
    .any(|term| title.contains(term))
    {
        "operations"
    } else {
        "other"
    }
}

fn effective_policy(
    profile: &BusinessProfile,
    context: &TimingContext<'_>,
) -> Result<EffectivePolicy> {
    let mut best: Option<(&TimingRule, usize)> = None;
    for rule in &profile.calendar.rules {
        if !rule_matches(rule, context) {
            continue;
        }
        let specificity = [
            !rule.industries.is_empty(),
            !rule.title_keywords.is_empty(),
            !rule.vantages.is_empty(),
            !rule.channels.is_empty(),
        ]
        .into_iter()
        .filter(|matched| *matched)
        .count();
        if best.is_none() || specificity > best.map(|(_, score)| score).unwrap_or_default() {
            best = Some((rule, specificity));
        }
    }

    let (weekdays, preferred_hours, window_start, window_end, rule, rationale) = match best {
        Some((rule, _)) => (
            if rule.weekdays.is_empty() {
                &profile.calendar.weekdays
            } else {
                &rule.weekdays
            },
            if rule.preferred_hours.is_empty() {
                &profile.calendar.preferred_hours
            } else {
                &rule.preferred_hours
            },
            rule.window_start.unwrap_or(profile.calendar.window_start),
            rule.window_end.unwrap_or(profile.calendar.window_end),
            rule.key.clone(),
            rule.rationale.clone(),
        ),
        None => (
            &profile.calendar.weekdays,
            &profile.calendar.preferred_hours,
            profile.calendar.window_start,
            profile.calendar.window_end,
            "default".into(),
            "business default; treat timing as a hypothesis until response history is sufficient"
                .into(),
        ),
    };

    Ok(EffectivePolicy {
        weekdays: weekdays
            .iter()
            .map(|day| parse_weekday(day))
            .collect::<Result<Vec<_>>>()?,
        preferred_hours: preferred_hours.clone(),
        window_start,
        window_end,
        rule,
        rationale,
    })
}

fn rule_matches(rule: &TimingRule, context: &TimingContext<'_>) -> bool {
    contains_any(context.industry, &rule.industries)
        && contains_any(context.title, &rule.title_keywords)
        && exact_any(context.vantage, &rule.vantages)
        && exact_any(context.channel, &rule.channels)
}

fn contains_any(value: &str, needles: &[String]) -> bool {
    needles.is_empty()
        || needles.iter().any(|needle| {
            value
                .to_ascii_lowercase()
                .contains(&needle.to_ascii_lowercase())
        })
}

fn exact_any(value: &str, candidates: &[String]) -> bool {
    candidates.is_empty()
        || candidates
            .iter()
            .any(|candidate| value.trim().eq_ignore_ascii_case(candidate.trim()))
}

fn parse_weekday(value: &str) -> Result<Weekday> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mon" => Ok(Weekday::Mon),
        "tue" => Ok(Weekday::Tue),
        "wed" => Ok(Weekday::Wed),
        "thu" => Ok(Weekday::Thu),
        "fri" => Ok(Weekday::Fri),
        "sat" => Ok(Weekday::Sat),
        "sun" => Ok(Weekday::Sun),
        other => Err(anyhow!("unknown weekday '{other}'")),
    }
}

fn local_midnight(tz: Tz, date: NaiveDate) -> Result<DateTime<Tz>> {
    tz.with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
        .single()
        .ok_or_else(|| anyhow!("could not resolve midnight for {date} in {}", tz.name()))
}

fn stable_index(key: &str, salt: &str, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    salt.hash(&mut hasher);
    hasher.finish() as usize % len
}

fn stable_minute(key: &str, date: &str, hour: u32) -> u32 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    date.hash(&mut hasher);
    hour.hash(&mut hasher);
    (hasher.finish() % 45) as u32
}

fn rotate<T>(values: &mut [T], start: usize) {
    if !values.is_empty() {
        values.rotate_left(start % values.len());
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, TimeZone, Timelike, Utc, Weekday};

    use crate::business::Businesses;

    use super::{
        can_send_now, next_slot, quota_day_bounds, schedule_with_capacity, timezone_for_location,
        TimingContext,
    };

    #[test]
    fn resolves_common_apollo_locations_with_dst_aware_zones() {
        assert_eq!(
            timezone_for_location("Toronto, Ontario, Canada", "Europe/London").name(),
            "America/Toronto"
        );
        assert_eq!(
            timezone_for_location("Los Angeles, California, United States", "Europe/London").name(),
            "America/Los_Angeles"
        );
        assert_eq!(
            timezone_for_location("London, United Kingdom", "America/Toronto").name(),
            "Europe/London"
        );
        assert_eq!(
            timezone_for_location("Perth, WA, Australia", "Europe/London").name(),
            "Australia/Perth"
        );
    }

    #[test]
    fn weekend_is_enabled_only_for_a_matching_named_rule() {
        let businesses = Businesses::load("businesses").expect("business profiles");
        let profile = businesses.get("wapahki").unwrap();
        let saturday = Utc.with_ymd_and_hms(2026, 8, 8, 13, 0, 0).unwrap(); // 09:00 Toronto
        let plant = TimingContext {
            industry: "food manufacturing",
            title: "Plant Operations Manager",
            channel: "email",
            location: "Toronto, Ontario, Canada",
            stable_key: "plant-person",
            ..Default::default()
        };
        let finance = TimingContext {
            title: "Chief Financial Officer",
            stable_key: "finance-person",
            ..plant.clone()
        };

        assert!(can_send_now(profile, &plant, saturday).unwrap());
        assert!(!can_send_now(profile, &finance, saturday).unwrap());
    }

    #[test]
    fn next_slot_uses_recipient_local_time_and_skips_default_weekends() {
        let businesses = Businesses::load("businesses").expect("business profiles");
        let profile = businesses.get("gnk").unwrap();
        let friday_evening = Utc.with_ymd_and_hms(2026, 8, 7, 23, 0, 0).unwrap();
        let context = TimingContext {
            industry: "professional services",
            title: "COO",
            channel: "email",
            location: "Toronto, Ontario, Canada",
            stable_key: "coo-1",
            ..Default::default()
        };
        let slot = next_slot(profile, &context, friday_evening).unwrap();
        let local = slot.at.with_timezone(&chrono_tz::America::Toronto);

        assert_eq!(local.weekday(), Weekday::Mon);
        assert!((8..17).contains(&local.hour()));
    }

    #[test]
    fn a_full_business_day_spills_to_the_next_allowed_day() {
        let businesses = Businesses::load("businesses").expect("business profiles");
        let profile = businesses.get("gnk").unwrap();
        let monday = Utc.with_ymd_and_hms(2026, 8, 3, 10, 0, 0).unwrap();
        let context = TimingContext {
            title: "COO",
            channel: "email",
            location: "London, United Kingdom",
            stable_key: "capacity-test",
            ..Default::default()
        };
        let first = next_slot(profile, &context, monday).unwrap();
        let (blocked_start, _, _) = quota_day_bounds(profile, first.at).unwrap();
        let slot = schedule_with_capacity(profile, &context, monday, |start, _| {
            Ok(if start == blocked_start { 30 } else { 0 })
        })
        .unwrap();

        assert_ne!(
            quota_day_bounds(profile, slot.at).unwrap().2,
            quota_day_bounds(profile, first.at).unwrap().2
        );
    }

    #[test]
    fn quota_day_bounds_follow_dst_instead_of_a_fixed_offset() {
        let businesses = Businesses::load("businesses").expect("business profiles");
        let profile = businesses.get("gnk").unwrap();
        let autumn_change = Utc.with_ymd_and_hms(2026, 10, 25, 12, 0, 0).unwrap();
        let (start, end, _) = quota_day_bounds(profile, autumn_change).unwrap();

        assert_eq!((end - start).num_hours(), 25);
    }
}
