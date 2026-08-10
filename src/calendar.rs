//! Business-owned, recipient-local outreach scheduling.
//!
//! The model may suggest sequence spacing, but it does not get to invent send
//! times. This module turns a desired `day_offset` into an auditable calendar
//! slot using the business profile, the recipient's timezone, named
//! industry/title hypotheses, and a hard per-business daily capacity.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::Tz;

use crate::business::{BusinessProfile, TimingRule};
use crate::db::{Lead, Person, SharedDb, TimingObservation, Touch, TouchScheduleUpdate};

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PortfolioScheduleSummary {
    pub emails: usize,
    pub protected_followups: usize,
    pub admitted_people: usize,
    pub active_days: usize,
}

#[derive(Debug, Clone)]
struct SequencePlan {
    sequence_id: String,
    lead: Lead,
    person: Person,
    touches: Vec<Touch>,
    anchor: Option<DateTime<Utc>>,
    active: bool,
    created_at: String,
}

#[derive(Debug, Default)]
struct CapacityLedger {
    by_day: HashMap<String, usize>,
    openers_by_account_day: HashMap<String, usize>,
}

/// Rebuild the approved sales calendar for one business as a portfolio rather
/// than preserving generation order. Existing conversations get their
/// follow-ups first. New sequences are then admitted breadth-first across
/// accounts, and every email in a person's cadence is reserved relative to the
/// actual/planned opener before the next person is admitted.
pub fn rebalance_approved_sales(
    db: &SharedDb,
    profile: &BusinessProfile,
    now: DateTime<Utc>,
) -> Result<PortfolioScheduleSummary> {
    let scheduled = db.scheduled_email_touches(&profile.key)?;
    if scheduled.is_empty() {
        return Ok(PortfolioScheduleSummary::default());
    }

    let mut grouped = BTreeMap::<String, Vec<Touch>>::new();
    for touch in scheduled {
        grouped
            .entry(touch.sequence_id.clone())
            .or_default()
            .push(touch);
    }

    let mut active = Vec::<SequencePlan>::new();
    let mut unopened = BTreeMap::<String, Vec<SequencePlan>>::new();
    for (sequence_id, mut touches) in grouped {
        touches.sort_by_key(|touch| touch.stage);
        let Some(person) = db.get_person(&touches[0].person_id)? else {
            continue;
        };
        let Some(lead) = db.get_lead(&touches[0].lead_id)? else {
            continue;
        };
        let history = db.list_touches_for_sequence(&sequence_id)?;
        let sent = history
            .iter()
            .filter(|touch| touch.status == "sent" && !touch.sent_at.is_empty())
            .collect::<Vec<_>>();
        let anchor = sent
            .iter()
            .find(|touch| touch.stage == 1)
            .and_then(|touch| parse_utc(&touch.sent_at))
            .or_else(|| {
                sent.iter().find_map(|touch| {
                    parse_utc(&touch.sent_at)
                        .map(|sent_at| sent_at - chrono::Duration::days(touch.day_offset.max(0)))
                })
            });
        let plan = SequencePlan {
            sequence_id,
            lead: lead.clone(),
            person,
            created_at: touches[0].created_at.clone(),
            touches,
            anchor,
            active: !sent.is_empty(),
        };
        if plan.active {
            active.push(plan);
        } else {
            unopened.entry(lead.id).or_default().push(plan);
        }
    }

    active.sort_by(|left, right| {
        plan_eligible(left, now)
            .cmp(&plan_eligible(right, now))
            .then_with(|| left.created_at.cmp(&right.created_at))
            .then_with(|| left.sequence_id.cmp(&right.sequence_id))
    });
    for plans in unopened.values_mut() {
        plans.sort_by(|left, right| {
            right
                .person
                .primary
                .cmp(&left.person.primary)
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.person.id.cmp(&right.person.id))
        });
    }

    // Breadth first: contact 1 at every account, then contact 2, etc. Explicitly
    // requested contacts are never dropped; later contacts simply enter on a
    // later capacity-safe day.
    let mut new_plans = Vec::<SequencePlan>::new();
    let max_depth = unopened.values().map(Vec::len).max().unwrap_or_default();
    for depth in 0..max_depth {
        for plans in unopened.values() {
            if let Some(plan) = plans.get(depth) {
                new_plans.push(plan.clone());
            }
        }
    }

    let mut ledger = CapacityLedger::default();
    let mut updates = Vec::<TouchScheduleUpdate>::new();
    let mut days = HashSet::<String>::new();
    let mut protected_followups = 0usize;
    let mut admitted_people = 0usize;

    for plan in active.iter().chain(new_plans.iter()) {
        let priority = if plan.active {
            "protected active follow-up"
        } else {
            "admitted new conversation with full cadence reserved"
        };
        let mut anchor = plan.anchor;
        let mut plan_updates = Vec::<TouchScheduleUpdate>::new();
        for touch in &plan.touches {
            let is_opener = touch.stage == 1 && !plan.active;
            let eligible = if is_opener {
                now
            } else {
                anchor.unwrap_or(now) + chrono::Duration::days(touch.day_offset.max(0))
            };
            let context = TimingContext {
                industry: &plan.lead.industry,
                title: &plan.person.title,
                vantage: &plan.person.vantage,
                channel: "email",
                location: if plan.person.location.is_empty() {
                    &plan.lead.hq
                } else {
                    &plan.person.location
                },
                timezone: if plan.person.timezone.is_empty() {
                    &plan.lead.timezone
                } else {
                    &plan.person.timezone
                },
                stable_key: &touch.id,
            };
            let slot = allocate_portfolio_slot(
                db,
                profile,
                &context,
                eligible.max(now),
                is_opener.then_some(plan.lead.id.as_str()),
                &mut ledger,
            )?;
            if is_opener {
                anchor = Some(slot.at);
                admitted_people += 1;
            }
            if plan.active && touch.stage > 1 {
                protected_followups += 1;
            }
            let (_, _, date) = quota_day_bounds(profile, slot.at)?;
            days.insert(date);
            plan_updates.push(TouchScheduleUpdate {
                id: touch.id.clone(),
                due_at: slot.at.to_rfc3339(),
                recipient_timezone: slot.recipient_timezone,
                scheduled_rule: slot.rule,
                schedule_reason: format!("portfolio scheduler: {priority}; {}", slot.rationale),
            });
        }
        updates.extend(plan_updates);
    }

    db.apply_touch_schedule(&updates)?;
    Ok(PortfolioScheduleSummary {
        emails: updates.len(),
        protected_followups,
        admitted_people,
        active_days: days.len(),
    })
}

fn plan_eligible(plan: &SequencePlan, now: DateTime<Utc>) -> DateTime<Utc> {
    plan.touches
        .first()
        .map(|touch| plan.anchor.unwrap_or(now) + chrono::Duration::days(touch.day_offset.max(0)))
        .unwrap_or(now)
}

fn parse_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn allocate_portfolio_slot(
    db: &SharedDb,
    profile: &BusinessProfile,
    context: &TimingContext<'_>,
    not_before: DateTime<Utc>,
    opener_account: Option<&str>,
    ledger: &mut CapacityLedger,
) -> Result<CalendarSlot> {
    let mut cursor = not_before;
    for _ in 0..370 {
        let slot = next_slot_with_learning(db, profile, context, cursor)?;
        let (start, end, date) = quota_day_bounds(profile, slot.at)?;
        let used = match ledger.by_day.get(&date) {
            Some(used) => *used,
            None => {
                let fixed = db.sent_touch_count_between(&profile.key, start, end)?
                    + db.scheduled_opportunity_count_between(&profile.key, start, end)?;
                ledger.by_day.insert(date.clone(), fixed);
                fixed
            }
        };
        let opener_ok = match opener_account {
            Some(account) if profile.account_limits.max_new_contacts_per_account_per_day > 0 => {
                let key = format!("{account}:{date}");
                let opened = match ledger.openers_by_account_day.get(&key) {
                    Some(opened) => *opened,
                    None => {
                        let sent = db.account_openers_sent_between(account, start, end)?;
                        ledger.openers_by_account_day.insert(key.clone(), sent);
                        sent
                    }
                };
                opened < profile.account_limits.max_new_contacts_per_account_per_day
            }
            _ => true,
        };
        if used < profile.calendar.daily_touch_cap && opener_ok {
            *ledger.by_day.entry(date.clone()).or_default() += 1;
            if let Some(account) = opener_account {
                *ledger
                    .openers_by_account_day
                    .entry(format!("{account}:{date}"))
                    .or_default() += 1;
            }
            return Ok(slot);
        }
        cursor = end;
    }
    Err(anyhow!(
        "no portfolio email capacity found for {} in the next year",
        profile.name
    ))
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
    next_slot_for_policy(profile, context, not_before, tz, policy)
}

/// Apply response-history learning only after the configured sample threshold
/// is met for a comparable role/industry cohort. Learning may rank times that a
/// business rule already allows; it can never invent a weekend or expand the
/// recipient-local window.
pub fn next_slot_with_learning(
    db: &SharedDb,
    profile: &BusinessProfile,
    context: &TimingContext<'_>,
    not_before: DateTime<Utc>,
) -> Result<CalendarSlot> {
    let tz = recipient_timezone(profile, context);
    let mut policy = effective_policy(profile, context)?;
    let observations = db
        .timing_observations(&profile.key)?
        .into_iter()
        .filter(|observation| comparable_cohort(observation, context))
        .collect::<Vec<_>>();
    if observations.len() >= profile.calendar.learning_min_samples {
        let mut hours = policy
            .preferred_hours
            .iter()
            .map(|hour| (*hour, 0usize, 0usize))
            .collect::<Vec<_>>();
        let mut weekdays = policy
            .weekdays
            .iter()
            .map(|day| (*day, 0usize, 0usize))
            .collect::<Vec<_>>();
        for observation in &observations {
            let Some(sent) = parse_utc(&observation.sent_at) else {
                continue;
            };
            let local_tz = observation.timezone.parse::<Tz>().unwrap_or(tz);
            let local = sent.with_timezone(&local_tz);
            if let Some(bucket) = hours
                .iter_mut()
                .min_by_key(|(hour, _, _)| (i64::from(*hour) - i64::from(local.hour())).abs())
            {
                bucket.1 += 1;
                bucket.2 += usize::from(observation.replied);
            }
            if let Some(bucket) = weekdays
                .iter_mut()
                .find(|(weekday, _, _)| *weekday == local.weekday())
            {
                bucket.1 += 1;
                bucket.2 += usize::from(observation.replied);
            }
        }
        hours.sort_by(|left, right| response_bucket_order(*left, *right));
        weekdays.sort_by(|left, right| response_bucket_order(*left, *right));
        // Retain two hours and three weekdays so learning improves priority
        // without collapsing all sends into one fragile historical bucket.
        let learned_hours = hours
            .iter()
            .filter(|(_, sends, _)| *sends >= 3)
            .take(2)
            .map(|(hour, _, _)| *hour)
            .collect::<Vec<_>>();
        let learned_weekdays = weekdays
            .iter()
            .filter(|(_, sends, _)| *sends >= 3)
            .take(3)
            .map(|(day, _, _)| *day)
            .collect::<Vec<_>>();
        if !learned_hours.is_empty() {
            policy.preferred_hours = learned_hours;
        }
        if !learned_weekdays.is_empty() {
            policy.weekdays = learned_weekdays;
        }
        policy.rule = format!("{}+learned_cohort", policy.rule);
        policy.rationale = format!(
            "{}; ranked within the allowed window from {} comparable sends (directional, not causal)",
            policy.rationale,
            observations.len()
        );
    }
    next_slot_for_policy(profile, context, not_before, tz, policy)
}

fn next_slot_for_policy(
    profile: &BusinessProfile,
    context: &TimingContext<'_>,
    not_before: DateTime<Utc>,
    tz: Tz,
    policy: EffectivePolicy,
) -> Result<CalendarSlot> {
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

fn comparable_cohort(observation: &TimingObservation, context: &TimingContext<'_>) -> bool {
    let role_matches = (!context.vantage.trim().is_empty()
        && observation
            .vantage
            .trim()
            .eq_ignore_ascii_case(context.vantage.trim()))
        || title_family(&observation.title) == title_family(context.title);
    let industry_matches = context.industry.trim().is_empty()
        || observation.industry.trim().is_empty()
        || observation
            .industry
            .trim()
            .eq_ignore_ascii_case(context.industry.trim());
    role_matches && industry_matches
}

fn response_bucket_order<T: Copy>(
    left: (T, usize, usize),
    right: (T, usize, usize),
) -> std::cmp::Ordering {
    let left_score = (left.2 as f64 + 1.0) / (left.1 as f64 + 4.0);
    let right_score = (right.2 as f64 + 1.0) / (right.1 as f64 + 4.0);
    right_score
        .partial_cmp(&left_score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| right.1.cmp(&left.1))
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

/// UTC bounds for an explicit business-local quota date. Calendar UIs use this
/// to show the same capacity bucket the daemon enforces.
pub fn quota_date_bounds(
    profile: &BusinessProfile,
    date: NaiveDate,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let tz = profile.calendar.quota_timezone.parse::<Tz>().map_err(|_| {
        anyhow!(
            "invalid quota timezone '{}'",
            profile.calendar.quota_timezone
        )
    })?;
    let next = date
        .succ_opt()
        .ok_or_else(|| anyhow!("calendar date overflow"))?;
    Ok((
        local_midnight(tz, date)?.with_timezone(&Utc),
        local_midnight(tz, next)?.with_timezone(&Utc),
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
        "{}: max {} approved emails/day in {}; default {} at {:?}:00 recipient-local; weekend only via [{}]",
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

    out.push_str("\nused/reserved email capacity (approved + sent)\n");
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
    out.push_str("\nupcoming approved emails\n");
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
    // Human-looking, auditable offsets: never :00/:15/:30/:45 or another
    // five-minute boundary, and the date salt changes the choice day to day.
    const MINUTES: [u32; 12] = [3, 7, 11, 16, 22, 26, 32, 37, 41, 47, 53, 57];
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    date.hash(&mut hasher);
    hour.hash(&mut hasher);
    MINUTES[hasher.finish() as usize % MINUTES.len()]
}

fn rotate<T>(values: &mut [T], start: usize) {
    if !values.is_empty() {
        values.rotate_left(start % values.len());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc, Weekday};

    use crate::business::Businesses;
    use crate::db::{Db, Lead, Person, Sequence, Touch, CURRENT_COPY_POLICY_VERSION};

    use super::{
        can_send_now, next_slot, quota_day_bounds, rebalance_approved_sales,
        schedule_with_capacity, timezone_for_location, TimingContext,
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
        assert_ne!(local.minute() % 5, 0);
    }

    #[test]
    fn portfolio_scheduler_spreads_accounts_and_reserves_full_cadences() {
        let businesses = Businesses::load("businesses").expect("business profiles");
        let mut profile = businesses.get("gnk").unwrap().clone();
        profile.calendar.daily_touch_cap = 2;
        profile.account_limits.max_new_contacts_per_account_per_day = 1;
        let db = Db::open(":memory:").expect("open memory db");
        let mut people_by_lead = BTreeMap::<String, Vec<String>>::new();

        for account in 0..2 {
            let lead_id = db
                .upsert_lead(&Lead {
                    brand: "gnk".into(),
                    apollo_org_id: format!("calendar-org-{account}"),
                    name: format!("Calendar Account {account}"),
                    industry: "logistics".into(),
                    hq: "London, United Kingdom".into(),
                    ..Default::default()
                })
                .unwrap();
            for contact in 0..2 {
                let person_id = db
                    .upsert_person(&Person {
                        lead_id: lead_id.clone(),
                        brand: "gnk".into(),
                        apollo_person_id: format!("calendar-person-{account}-{contact}"),
                        name: format!("Person {account}-{contact}"),
                        title: "Operations Director".into(),
                        location: "London, United Kingdom".into(),
                        timezone: "Europe/London".into(),
                        email: format!("person-{account}-{contact}@example.com"),
                        email_status: "verified".into(),
                        status: "verified".into(),
                        primary: contact == 0,
                        ..Default::default()
                    })
                    .unwrap();
                people_by_lead
                    .entry(lead_id.clone())
                    .or_default()
                    .push(person_id.clone());
                let sequence_id = db
                    .create_sequence(&Sequence {
                        person_id: person_id.clone(),
                        lead_id: lead_id.clone(),
                        brand: "gnk".into(),
                        status: "active".into(),
                        copy_policy_version: CURRENT_COPY_POLICY_VERSION,
                        ..Default::default()
                    })
                    .unwrap();
                for (stage, day_offset) in [(1, 0), (2, 3)] {
                    db.insert_touch(&Touch {
                        sequence_id: sequence_id.clone(),
                        person_id: person_id.clone(),
                        lead_id: lead_id.clone(),
                        brand: "gnk".into(),
                        stage,
                        day_offset,
                        channel: "email".into(),
                        status: "scheduled".into(),
                        due_at: "2026-08-10T08:00:00Z".into(),
                        ..Default::default()
                    })
                    .unwrap();
                }
            }
        }

        let now = Utc.with_ymd_and_hms(2026, 8, 10, 7, 0, 0).unwrap();
        let summary = rebalance_approved_sales(&db, &profile, now).unwrap();
        assert_eq!(summary.emails, 8);
        assert_eq!(summary.admitted_people, 4);

        for people in people_by_lead.values() {
            let opener_dates = people
                .iter()
                .map(|person_id| {
                    let touches = db.list_touches_for_person(person_id).unwrap();
                    let due = DateTime::parse_from_rfc3339(&touches[0].due_at).unwrap();
                    due.with_timezone(&chrono_tz::Europe::London).date_naive()
                })
                .collect::<Vec<_>>();
            assert_ne!(opener_dates[0], opener_dates[1]);
        }

        for offset in 0..10 {
            let at = now + chrono::Duration::days(offset);
            let (start, end, _) = quota_day_bounds(&profile, at).unwrap();
            assert!(db.planned_touch_count_between("gnk", start, end).unwrap() <= 2);
        }
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
