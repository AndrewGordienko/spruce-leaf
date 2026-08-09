//! The autopilot's scheduling spine.
//!
//! Two layers. The durable work queue itself — the [`Job`] type plus the
//! claim/lease/retry/dead-letter methods — lives in [`crate::db`], generalizing
//! the pattern the cadence loop already proved: claim due rows from SQLite, act,
//! persist, survive a restart. On top of that sits a *supervisor*: given a
//! brand's funnel it decides the single most useful next action and enqueues it
//! idempotently. Deliberately a deterministic state machine — a worker may let
//! the model make a bounded decision *inside* a job, but what runs next, and
//! whether it retries, is plain code that a crash can't corrupt.
//!
//! Staged objective (discovery → meetings): in the discovery phase the
//! supervisor keeps the funnel full from the top — source accounts, reveal
//! emails, draft cadences — so convergence evidence accumulates across many
//! companies. The meeting phase layers onto the same spine later without
//! rewriting it.
//!
//! `run_daemon` closes the loop: each pass re-evaluates every brand, schedules
//! at most one useful action per brand, then leases and executes one job. The
//! lease/retry state stays in SQLite, so killing the process during a model or
//! Apollo call is recoverable rather than silently losing the work.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::apollo::Apollo;
use crate::business::Businesses;
use crate::db::{Job, SharedDb};
use crate::engine::Engine;
use crate::enrich;
use crate::knowledge::SharedLibrary;
use crate::metrics::{self, Funnel};
use crate::outreach;
use crate::playbook::Playbooks;
use crate::sourcing;

/// Job kinds the supervisor schedules. Stored as plain strings in the DB; these
/// are the canonical spellings so producers and workers agree.
pub mod kind {
    pub const SOURCE: &str = "source";
    pub const ENRICH: &str = "enrich";
    pub const PLAN: &str = "plan";
}

/// How full each stage of the funnel should be kept. These will eventually be
/// read from the `[autopilot]` block of a brand's playbook; for now they're
/// explicit inputs so the decision stays pure and testable.
#[derive(Debug, Clone)]
pub struct SupervisorTargets {
    pub min_leads: usize,
    pub min_verified: usize,
    pub min_contacted: usize,
}

impl Default for SupervisorTargets {
    fn default() -> Self {
        Self {
            min_leads: 25,
            min_verified: 40,
            min_contacted: 25,
        }
    }
}

impl SupervisorTargets {
    /// Optional process-level tuning without making the safe defaults depend on
    /// a particular local business profile revision.
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            min_leads: env_usize("AUTOPILOT_MIN_LEADS").unwrap_or(defaults.min_leads),
            min_verified: env_usize("AUTOPILOT_MIN_VERIFIED").unwrap_or(defaults.min_verified),
            min_contacted: env_usize("AUTOPILOT_MIN_CONTACTED").unwrap_or(defaults.min_contacted),
        }
    }
}

/// The next action to take for a brand, or `Idle` when every stage is at target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Source,
    Enrich,
    Plan,
    Idle,
}

impl Decision {
    pub fn kind(self) -> Option<&'static str> {
        match self {
            Decision::Source => Some(kind::SOURCE),
            Decision::Enrich => Some(kind::ENRICH),
            Decision::Plan => Some(kind::PLAN),
            Decision::Idle => None,
        }
    }
}

/// Decide the single most useful next action from a funnel snapshot. The funnel
/// fills from the top: there's no point revealing emails with no accounts, nor
/// drafting cadences with nobody verified.
pub fn decide(f: &Funnel, t: &SupervisorTargets) -> Decision {
    if f.leads < t.min_leads {
        return Decision::Source;
    }
    // People are sourced but their emails aren't revealed yet, and we're short
    // of the verified target. (If everyone sourced is already verified there's
    // nothing to enrich, so fall through even when below target.)
    if f.verified < t.min_verified && f.verified < f.people {
        return Decision::Enrich;
    }
    // Verified prospects are sitting un-contacted; draft cadences for them.
    if f.contacted < t.min_contacted && f.verified > f.contacted {
        return Decision::Plan;
    }
    Decision::Idle
}

/// Look at a brand's live funnel, decide the next action, and enqueue it. Returns
/// the job id if one was scheduled, or `None` when the brand is at target.
pub fn schedule_next(
    db: &SharedDb,
    brand: &str,
    targets: &SupervisorTargets,
) -> Result<Option<String>> {
    let funnel = metrics::funnel(db, Some(brand))?;
    let decision = decide(&funnel, targets);
    let Some(kind) = decision.kind() else {
        return Ok(None);
    };
    // Include the stage's current progress in the key. If a job makes progress,
    // another pass may enqueue the next batch on the same day; if it makes no
    // progress, the completed row remains the circuit breaker against a tight,
    // credit-burning loop.
    let progress = match decision {
        Decision::Source => funnel.leads,
        Decision::Enrich => funnel.verified,
        Decision::Plan => funnel.contacted,
        Decision::Idle => unreachable!(),
    };
    let job = Job {
        brand: brand.to_string(),
        kind: kind.to_string(),
        dedup_key: format!("{brand}:{kind}:{progress}"),
        ..Default::default()
    };
    Ok(Some(db.enqueue_job(&job)?))
}

/// Optional per-job overrides. Supervisor-created jobs use these defaults;
/// keeping the payload explicit also makes manually-enqueued work auditable.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct JobPayload {
    pub thesis: String,
    pub accounts: usize,
    pub contacts: usize,
    pub limit: usize,
    pub phone: bool,
    pub touches: usize,
    /// Deliberately false by default: the discovery autopilot may draft copy,
    /// but cold email still crosses the existing approval gate.
    pub auto_schedule: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkResult {
    pub job_id: String,
    pub brand: String,
    pub kind: String,
    pub summary: String,
}

/// Lease and execute one due job. Execution errors are persisted through the
/// retry/dead-letter policy and returned as a normal outcome so one bad brand or
/// expired credential cannot take down the cadence daemon.
#[allow(clippy::too_many_arguments)]
pub async fn work_one(
    db: &SharedDb,
    client: &Engine,
    playbooks: &Playbooks,
    businesses: &Businesses,
    library: &SharedLibrary,
    concurrency: usize,
    worker_id: &str,
) -> Result<Option<WorkResult>> {
    // Sourcing can fan out into many model calls; use a lease comfortably above
    // the per-call timeout so a second process does not reclaim legitimate work.
    let Some(job) = db.claim_job(worker_id, 2 * 60 * 60)? else {
        return Ok(None);
    };
    let execution = async {
        let payload: JobPayload = serde_json::from_str(&job.payload)
            .with_context(|| format!("parsing payload for job {}", job.id))?;
        execute(
            db,
            client,
            playbooks,
            businesses,
            library,
            concurrency,
            &job,
            &payload,
        )
        .await
    }
    .await;

    match execution {
        Ok(summary) => {
            let result = WorkResult {
                job_id: job.id.clone(),
                brand: job.brand.clone(),
                kind: job.kind.clone(),
                summary,
            };
            db.complete_job(&job.id, &serde_json::to_string(&result)?)?;
            db.log_event(
                &job.brand,
                "",
                "",
                "job_completed",
                &format!("{}: {}", job.kind, result.summary),
            )?;
            Ok(Some(result))
        }
        Err(error) => {
            let message = format!("{error:#}");
            let status = db.fail_job(&job.id, &message)?;
            db.log_event(
                &job.brand,
                "",
                "",
                "job_failed",
                &format!("{} → {status}: {message}", job.kind),
            )?;
            eprintln!(
                "  ! autopilot job {} [{}:{}] failed → {status}: {message}",
                job.id, job.brand, job.kind
            );
            Ok(None)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    db: &SharedDb,
    client: &Engine,
    playbooks: &Playbooks,
    businesses: &Businesses,
    library: &SharedLibrary,
    concurrency: usize,
    job: &Job,
    payload: &JobPayload,
) -> Result<String> {
    let pb = playbooks.get(&job.brand)?;
    let business = businesses.get(&job.brand)?;
    match job.kind.as_str() {
        kind::SOURCE => {
            let apollo = Apollo::from_env()?;
            let thesis = if payload.thesis.trim().is_empty() {
                pb.motion.as_str()
            } else {
                payload.thesis.as_str()
            };
            let accounts = if payload.accounts == 0 {
                10
            } else {
                payload.accounts.clamp(1, 25)
            };
            let contacts = if payload.contacts == 0 {
                3
            } else {
                payload.contacts.clamp(1, 10)
            };
            let lib = library.read().await.clone();
            let s = sourcing::source(
                db,
                client,
                &apollo,
                pb,
                &playbooks.shared,
                &business.calendar.fallback_recipient_timezone,
                &business.operating_context(),
                &lib,
                thesis,
                accounts,
                contacts,
                None,
                concurrency,
                None,
            )
            .await?;
            Ok(format!(
                "{} Apollo orgs, {} qualified leads, {} people",
                s.orgs_found, s.leads_qualified, s.people_added
            ))
        }
        kind::ENRICH => {
            let apollo = Apollo::from_env()?;
            let limit = if payload.limit == 0 {
                50
            } else {
                payload.limit.min(200)
            };
            let s = enrich::enrich_pending(
                db,
                &apollo,
                Some(&job.brand),
                limit,
                payload.phone,
                None,
                None,
            )
            .await?;
            Ok(format!(
                "{} attempted, {} emails, {} verified, {} credits",
                s.attempted, s.emails_found, s.verified, s.credits_spent
            ))
        }
        kind::PLAN => {
            let touches = if payload.touches == 0 {
                business.account_limits.max_unanswered_touches.max(1)
            } else {
                payload.touches.clamp(1, 12)
            };
            let lib = library.read().await.clone();
            let s = outreach::plan_pending(
                db,
                client,
                pb,
                business,
                &playbooks.shared,
                &lib,
                touches,
                concurrency,
                payload.auto_schedule,
                true,
                None,
                false,
                Some(
                    business
                        .account_limits
                        .max_active_contacts_per_account
                        .clamp(1, 2),
                ),
                None,
                None,
            )
            .await?;
            Ok(format!(
                "{} people planned, {} scheduled, {} drafts",
                s.people_planned, s.touches_scheduled, s.touches_drafted
            ))
        }
        other => anyhow::bail!("unsupported job kind '{other}'"),
    }
}

/// Continuously schedule and drain the discovery funnel for every configured
/// brand. This is only started by the explicit `daemon --autopilot` flag.
#[allow(clippy::too_many_arguments)]
pub async fn run_daemon(
    db: SharedDb,
    client: Engine,
    playbooks: Arc<Playbooks>,
    businesses: Arc<Businesses>,
    library: SharedLibrary,
    concurrency: usize,
    interval_secs: u64,
) {
    let worker_id = format!("spruce-{}", std::process::id());
    let targets = SupervisorTargets::from_env();
    eprintln!(
        "\u{1F9ED} discovery autopilot started — targets {} leads / {} verified / {} contacted",
        targets.min_leads, targets.min_verified, targets.min_contacted
    );
    loop {
        for brand in playbooks.keys() {
            match schedule_next(&db, brand, &targets) {
                Ok(Some(id)) => eprintln!("  · autopilot [{brand}] scheduled {id}"),
                Ok(None) => {}
                Err(error) => eprintln!("  ! autopilot supervisor [{brand}]: {error:#}"),
            }
        }
        if let Err(error) = work_one(
            &db,
            &client,
            &playbooks,
            &businesses,
            &library,
            concurrency,
            &worker_id,
        )
        .await
        {
            eprintln!("  ! autopilot worker: {error:#}");
        }
        tokio::time::sleep(Duration::from_secs(interval_secs.max(15))).await;
    }
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn funnel(leads: usize, people: usize, verified: usize, contacted: usize) -> Funnel {
        Funnel {
            leads,
            people,
            verified,
            contacted,
            ..Default::default()
        }
    }

    #[test]
    fn fills_the_funnel_from_the_top() {
        let t = SupervisorTargets {
            min_leads: 10,
            min_verified: 10,
            min_contacted: 10,
        };
        // No accounts → source.
        assert_eq!(decide(&funnel(0, 0, 0, 0), &t), Decision::Source);
        // Accounts + people but unrevealed emails → enrich.
        assert_eq!(decide(&funnel(10, 20, 3, 0), &t), Decision::Enrich);
        // Verified but nobody contacted → plan.
        assert_eq!(decide(&funnel(10, 20, 12, 0), &t), Decision::Plan);
        // Every stage at target → idle.
        assert_eq!(decide(&funnel(10, 20, 12, 10), &t), Decision::Idle);
    }

    #[test]
    fn enrich_yields_to_plan_when_nothing_is_left_to_reveal() {
        let t = SupervisorTargets {
            min_leads: 10,
            min_verified: 50,
            min_contacted: 10,
        };
        // verified == people: below the verified target, but there are no
        // unrevealed people to enrich, so the ladder moves to planning.
        assert_eq!(decide(&funnel(10, 8, 8, 0), &t), Decision::Plan);
    }

    #[test]
    fn schedule_next_enqueues_source_and_is_idempotent() {
        let path =
            std::env::temp_dir().join(format!("spruce-jobs-sched-{}.sqlite", uuid::Uuid::new_v4()));
        let db = crate::db::Db::open(&path).expect("open db");
        let targets = SupervisorTargets::default();

        // Empty funnel → the top of the ladder, sourcing.
        let first = schedule_next(&db, "gnk", &targets)
            .expect("schedule")
            .expect("a job");
        let second = schedule_next(&db, "gnk", &targets)
            .expect("schedule again")
            .expect("same job");
        assert_eq!(
            first, second,
            "re-deciding the same day must not duplicate work"
        );

        let job = db.get_job(&first).expect("get").expect("exists");
        assert_eq!(job.kind, kind::SOURCE);
        assert_eq!(job.status, "pending");

        drop(db);
        for p in [
            path.clone(),
            std::path::PathBuf::from(format!("{}-wal", path.display())),
            std::path::PathBuf::from(format!("{}-shm", path.display())),
        ] {
            let _ = std::fs::remove_file(p);
        }
    }
}
