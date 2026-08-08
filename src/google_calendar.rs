//! Google Calendar availability and guarded event creation.
//!
//! Credentials stay in environment variables. The client accepts either a
//! short-lived `GOOGLE_CALENDAR_ACCESS_TOKEN` (handy for local testing) or the
//! offline OAuth trio `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, and
//! `GOOGLE_REFRESH_TOKEN`. Each brand opts in with
//! `<BRAND>_GOOGLE_CALENDAR_ID`; no calendar id means meeting suggestions fall
//! back to asking the prospect for availability and no API call is attempted.

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc, Weekday};
use chrono_tz::Tz;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CalendarSlot {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub display: String,
}

#[derive(Debug, Clone)]
pub struct BookedEvent {
    pub event_id: String,
    pub html_link: String,
    pub meet_link: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub timezone: String,
}

#[derive(Clone)]
pub struct GoogleCalendar {
    http: Client,
    calendar_id: String,
    timezone: Tz,
    duration_minutes: i64,
    preferred_hours: Vec<u32>,
    access_token: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    refresh_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
}

impl GoogleCalendar {
    /// `Ok(None)` is the normal unconfigured state. A configured calendar with
    /// incomplete credentials is an error because silently pretending to offer
    /// checked availability would be misleading.
    pub fn from_env(brand: &str) -> Result<Option<Self>> {
        let prefix = brand.to_uppercase();
        let Some(calendar_id) = env_nonempty(&format!("{prefix}_GOOGLE_CALENDAR_ID")) else {
            return Ok(None);
        };
        let timezone_name = env_nonempty(&format!("{prefix}_MEETING_TIMEZONE"))
            .unwrap_or_else(|| "Europe/London".to_string());
        let timezone: Tz = timezone_name
            .parse()
            .with_context(|| format!("invalid {prefix}_MEETING_TIMEZONE '{timezone_name}'"))?;
        let duration_minutes = env_nonempty(&format!("{prefix}_MEETING_DURATION_MINUTES"))
            .and_then(|raw| raw.parse::<i64>().ok())
            .filter(|minutes| (15..=120).contains(minutes))
            .unwrap_or(30);
        let preferred_hours = env_nonempty(&format!("{prefix}_MEETING_HOURS"))
            .map(|raw| parse_hours(&raw))
            .filter(|hours| !hours.is_empty())
            .unwrap_or_else(|| vec![9, 11, 14]);

        let access_token = env_nonempty("GOOGLE_CALENDAR_ACCESS_TOKEN");
        let client_id = env_nonempty("GOOGLE_CLIENT_ID");
        let client_secret = env_nonempty("GOOGLE_CLIENT_SECRET");
        let refresh_token = env_nonempty("GOOGLE_REFRESH_TOKEN");
        let can_refresh = client_id.is_some() && client_secret.is_some() && refresh_token.is_some();
        if access_token.is_none() && !can_refresh {
            anyhow::bail!(
                "{prefix}_GOOGLE_CALENDAR_ID is set, but Google OAuth credentials are incomplete"
            );
        }

        Ok(Some(Self {
            http: Client::new(),
            calendar_id,
            timezone,
            duration_minutes,
            preferred_hours,
            access_token,
            client_id,
            client_secret,
            refresh_token,
        }))
    }

    pub fn timezone_name(&self) -> &str {
        self.timezone.name()
    }

    pub fn duration_minutes(&self) -> i64 {
        self.duration_minutes
    }

    /// Return the next checked free slots. Candidate generation is
    /// deterministic; Google FreeBusy is the only external source of truth.
    pub async fn available_slots(&self, limit: usize) -> Result<Vec<CalendarSlot>> {
        let candidates = candidate_slots(
            Utc::now(),
            self.timezone,
            self.duration_minutes,
            &self.preferred_hours,
            14,
        );
        if candidates.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let token = self.access_token().await?;
        let window_start = candidates.first().expect("nonempty").start;
        let window_end = candidates.last().expect("nonempty").end + Duration::days(1);
        let response = self
            .http
            .post("https://www.googleapis.com/calendar/v3/freeBusy")
            .bearer_auth(token)
            .json(&json!({
                "timeMin": window_start.to_rfc3339(),
                "timeMax": window_end.to_rfc3339(),
                "timeZone": self.timezone.name(),
                "items": [{"id": self.calendar_id}],
            }))
            .send()
            .await
            .context("querying Google Calendar FreeBusy")?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .context("decoding FreeBusy response")?;
        if !status.is_success() {
            anyhow::bail!("Google Calendar FreeBusy returned {status}: {body}");
        }
        let busy = body
            .get("calendars")
            .and_then(|calendars| calendars.get(&self.calendar_id))
            .and_then(|calendar| calendar.get("busy"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|period| {
                let start = period.get("start")?.as_str()?.parse().ok()?;
                let end = period.get("end")?.as_str()?.parse().ok()?;
                Some((start, end))
            })
            .collect::<Vec<(DateTime<Utc>, DateTime<Utc>)>>();

        Ok(candidates
            .into_iter()
            .filter(|slot| {
                !busy
                    .iter()
                    .any(|(start, end)| slot.start < *end && slot.end > *start)
            })
            .take(limit)
            .collect())
    }

    /// Recheck one previously offered slot immediately before creating an
    /// event. Availability can change between the offer and the prospect's
    /// reply, so booking never trusts the older snapshot.
    pub async fn slot_is_free(&self, start: DateTime<Utc>) -> Result<bool> {
        let end = start + Duration::minutes(self.duration_minutes);
        let token = self.access_token().await?;
        let response = self
            .http
            .post("https://www.googleapis.com/calendar/v3/freeBusy")
            .bearer_auth(token)
            .json(&json!({
                "timeMin": start.to_rfc3339(),
                "timeMax": end.to_rfc3339(),
                "timeZone": self.timezone.name(),
                "items": [{"id": self.calendar_id}],
            }))
            .send()
            .await
            .context("rechecking Google Calendar availability")?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .context("decoding FreeBusy response")?;
        if !status.is_success() {
            anyhow::bail!("Google Calendar FreeBusy returned {status}: {body}");
        }
        let busy = body
            .get("calendars")
            .and_then(|calendars| calendars.get(&self.calendar_id))
            .and_then(|calendar| calendar.get("busy"))
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        Ok(busy == 0)
    }

    /// Insert an attendee-visible event and request a Google Meet conference.
    pub async fn book(
        &self,
        attendee_email: &str,
        start: DateTime<Utc>,
        summary: &str,
        description: &str,
        booking_key: &str,
    ) -> Result<BookedEvent> {
        if start <= Utc::now() {
            anyhow::bail!("refusing to book a meeting in the past");
        }
        let end = start + Duration::minutes(self.duration_minutes);
        let token = self.access_token().await?;
        let mut url = Url::parse("https://www.googleapis.com/calendar/v3/")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid Google Calendar base URL"))?
            .extend(["calendars", self.calendar_id.as_str(), "events"]);
        url.query_pairs_mut()
            .append_pair("conferenceDataVersion", "1")
            .append_pair("sendUpdates", "all");
        let event_id = stable_event_id(booking_key, start);
        let response = self
            .http
            .post(url.clone())
            .bearer_auth(&token)
            .json(&json!({
                "id": event_id,
                "summary": summary,
                "description": description,
                "start": {"dateTime": start.to_rfc3339(), "timeZone": self.timezone.name()},
                "end": {"dateTime": end.to_rfc3339(), "timeZone": self.timezone.name()},
                "attendees": [{"email": attendee_email}],
                "conferenceData": {
                    "createRequest": {
                        "requestId": Uuid::new_v4().to_string(),
                        "conferenceSolutionKey": {"type": "hangoutsMeet"}
                    }
                }
            }))
            .send()
            .await
            .context("creating Google Calendar event")?;
        let status = response.status();
        // A stable client-generated event id makes a retry idempotent. If the
        // first insert succeeded but the process lost its response before
        // persisting SQLite, recover the existing event instead of inviting the
        // attendee twice.
        let body: Value = if status.as_u16() == 409 {
            url.query_pairs_mut().clear();
            url.path_segments_mut()
                .map_err(|_| anyhow::anyhow!("invalid Google Calendar event URL"))?
                .push(&event_id);
            let existing = self
                .http
                .get(url)
                .bearer_auth(&token)
                .send()
                .await
                .context("recovering existing Google Calendar event")?;
            let existing_status = existing.status();
            let body: Value = existing
                .json()
                .await
                .context("decoding existing event response")?;
            if !existing_status.is_success() {
                anyhow::bail!("Google Calendar event recovery returned {existing_status}: {body}");
            }
            body
        } else {
            let body: Value = response.json().await.context("decoding event response")?;
            if !status.is_success() {
                anyhow::bail!("Google Calendar event insert returned {status}: {body}");
            }
            body
        };
        let meet_link = body
            .get("hangoutLink")
            .and_then(Value::as_str)
            .or_else(|| {
                body.pointer("/conferenceData/entryPoints")?
                    .as_array()?
                    .iter()
                    .find(|entry| {
                        entry.get("entryPointType").and_then(Value::as_str) == Some("video")
                    })?
                    .get("uri")?
                    .as_str()
            })
            .unwrap_or_default()
            .to_string();
        Ok(BookedEvent {
            event_id: text_field(&body, "id"),
            html_link: text_field(&body, "htmlLink"),
            meet_link,
            start,
            end,
            timezone: self.timezone.name().to_string(),
        })
    }

    async fn access_token(&self) -> Result<String> {
        if let Some(token) = &self.access_token {
            return Ok(token.clone());
        }
        let response = self
            .http
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("client_id", self.client_id.as_deref().unwrap_or("")),
                ("client_secret", self.client_secret.as_deref().unwrap_or("")),
                ("refresh_token", self.refresh_token.as_deref().unwrap_or("")),
                ("grant_type", "refresh_token"),
            ])
            .send()
            .await
            .context("refreshing Google OAuth access token")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Google OAuth refresh returned {status}: {body}");
        }
        Ok(response
            .json::<TokenResponse>()
            .await
            .context("decoding Google OAuth token")?
            .access_token)
    }
}

fn candidate_slots(
    now: DateTime<Utc>,
    timezone: Tz,
    duration_minutes: i64,
    preferred_hours: &[u32],
    days: i64,
) -> Vec<CalendarSlot> {
    let local_now = now.with_timezone(&timezone);
    let mut slots = Vec::new();
    // Start tomorrow: a slot proposed in email should not become stale while
    // waiting for approval and delivery.
    for offset in 1..=days {
        let date = (local_now + Duration::days(offset)).date_naive();
        if matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
            continue;
        }
        for &hour in preferred_hours {
            let Some(local) = timezone
                .with_ymd_and_hms(date.year(), date.month(), date.day(), hour, 0, 0)
                .single()
            else {
                continue;
            };
            let start = local.with_timezone(&Utc);
            let end = start + Duration::minutes(duration_minutes);
            slots.push(CalendarSlot {
                start,
                end,
                display: local.format("%a %-d %b at %-I:%M %p %Z").to_string(),
            });
        }
    }
    slots
}

fn parse_hours(raw: &str) -> Vec<u32> {
    let mut hours = raw
        .split(',')
        .filter_map(|part| part.trim().parse::<u32>().ok())
        .filter(|hour| *hour <= 23)
        .collect::<Vec<_>>();
    hours.sort_unstable();
    hours.dedup();
    hours
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn text_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Google event IDs accept base32hex characters (0-9, a-v). Conversation ids
/// are UUID hex; combining them with the UTC timestamp gives one stable id per
/// thread/slot and remains valid after stripping punctuation.
fn stable_event_id(booking_key: &str, start: DateTime<Utc>) -> String {
    let key = booking_key
        .to_lowercase()
        .chars()
        .filter(|character| character.is_ascii_digit() || matches!(character, 'a'..='v'))
        .collect::<String>();
    format!("spruce{}{:x}", key, start.timestamp())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidates_start_tomorrow_and_skip_weekends() {
        let now = DateTime::parse_from_rfc3339("2026-08-07T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc); // Friday
        let slots = candidate_slots(now, chrono_tz::Europe::London, 30, &[9, 14], 5);
        assert!(!slots.is_empty());
        assert!(slots.iter().all(|slot| !matches!(
            slot.start
                .with_timezone(&chrono_tz::Europe::London)
                .weekday(),
            Weekday::Sat | Weekday::Sun
        )));
        assert_eq!(
            slots[0]
                .start
                .with_timezone(&chrono_tz::Europe::London)
                .weekday(),
            Weekday::Mon
        );
    }

    #[test]
    fn meeting_hours_are_sanitized() {
        assert_eq!(parse_hours("14, 9,14,99,nope"), vec![9, 14]);
    }

    #[test]
    fn event_ids_are_stable_and_google_safe() {
        let start = DateTime::parse_from_rfc3339("2026-08-10T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let a = stable_event_id("8f67d9f8-28d5-4a33-a966-6ebc9563e555", start);
        let b = stable_event_id("8f67d9f8-28d5-4a33-a966-6ebc9563e555", start);
        assert_eq!(a, b);
        assert!(a.len() >= 5);
        assert!(a
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, 'a'..='v')));
    }
}
