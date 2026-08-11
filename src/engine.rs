//! Provider-neutral reasoning through the OpenAI Responses API or a local CLI.
//!
//! The direct API path avoids starting a full coding-agent process for every
//! inference call. Local authenticated CLIs remain available as fallbacks:
//!
//!   * OpenAI: `POST /v1/responses` with strict structured outputs.
//!   * Codex: `codex exec --json --output-schema`, read-only and ephemeral.
//!   * Claude: `claude -p --output-format ... --json-schema`.
//!   * Grok:   `grok -p ... --output-format json|streaming-messages-json --json-schema`.
//!
//! Every call also accrues token/cost/latency into a shared [`Stats`] so the UI
//! can print a footer.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

/// OS process arguments cannot contain NUL bytes. Website text is untrusted
/// input and occasionally includes one, so normalize only that impossible argv
/// character before handing prompts to a local model CLI.
fn cli_safe_arg(input: &str) -> Cow<'_, str> {
    if input.contains('\0') {
        Cow::Owned(input.replace('\0', "\u{FFFD}"))
    } else {
        Cow::Borrowed(input)
    }
}

#[derive(Debug)]
struct OpenAiBackgroundPollError(String);

impl std::fmt::Display for OpenAiBackgroundPollError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for OpenAiBackgroundPollError {}

/// Which provider supplies model inference.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, clap::ValueEnum)]
pub enum Backend {
    Openai,
    Codex,
    Claude,
    Grok,
}

impl Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            Backend::Openai => "openai",
            Backend::Codex => "codex",
            Backend::Claude => "claude",
            Backend::Grok => "grok",
        }
    }

    /// Preferred cross-provider fallback when the active provider hits a usage cap.
    pub fn other(self) -> Self {
        match self {
            Backend::Openai => Backend::Claude,
            Backend::Codex => Backend::Claude,
            Backend::Claude => Backend::Codex,
            Backend::Grok => Backend::Claude,
        }
    }
}

impl std::fmt::Display for Backend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Running totals across every model-CLI call this process has made.
#[derive(Default)]
pub struct Stats {
    pub attempts: AtomicU64,
    pub calls: AtomicU64,
    pub failures: AtomicU64,
    pub fallback_attempts: AtomicU64,
    pub input_tokens: AtomicU64,
    pub cached_input_tokens: AtomicU64,
    pub cache_write_input_tokens: AtomicU64,
    pub output_tokens: AtomicU64,
    pub prompt_chars: AtomicU64,
    /// Cost accumulated in micro-dollars (1e-6 USD) to keep it integer/atomic.
    pub cost_micro_usd: AtomicU64,
    stages: Mutex<BTreeMap<String, StageStats>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StageStats {
    pub attempts: u64,
    pub calls: u64,
    pub failures: u64,
    pub fallback_attempts: u64,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    pub output_tokens: u64,
    pub prompt_chars: u64,
    pub cost_micro_usd: u64,
}

/// An immutable point-in-time reading of [`Stats`], for diffing across a span.
#[derive(Clone, Copy, Default)]
pub struct StatsSnapshot {
    pub attempts: u64,
    pub calls: u64,
    pub failures: u64,
    pub fallback_attempts: u64,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    pub output_tokens: u64,
    pub prompt_chars: u64,
    pub cost_usd: f64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            calls: self.calls.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            fallback_attempts: self.fallback_attempts.load(Ordering::Relaxed),
            input_tokens: self.input_tokens.load(Ordering::Relaxed),
            cached_input_tokens: self.cached_input_tokens.load(Ordering::Relaxed),
            cache_write_input_tokens: self.cache_write_input_tokens.load(Ordering::Relaxed),
            output_tokens: self.output_tokens.load(Ordering::Relaxed),
            prompt_chars: self.prompt_chars.load(Ordering::Relaxed),
            cost_usd: self.cost_micro_usd.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        }
    }

    fn record_attempt(&self, stage: &str, prompt_chars: u64, fallback: bool) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.prompt_chars.fetch_add(prompt_chars, Ordering::Relaxed);
        if fallback {
            self.fallback_attempts.fetch_add(1, Ordering::Relaxed);
        }
        let mut stages = self.stages.lock().unwrap_or_else(|lock| lock.into_inner());
        let stats = stages.entry(stage.to_string()).or_default();
        stats.attempts += 1;
        stats.prompt_chars += prompt_chars;
        if fallback {
            stats.fallback_attempts += 1;
        }
    }

    fn record_success(&self, stage: &str, outcome: &CallOutcome) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.input_tokens
            .fetch_add(outcome.input_tokens, Ordering::Relaxed);
        self.cached_input_tokens
            .fetch_add(outcome.cached_input_tokens, Ordering::Relaxed);
        self.cache_write_input_tokens
            .fetch_add(outcome.cache_write_input_tokens, Ordering::Relaxed);
        self.output_tokens
            .fetch_add(outcome.output_tokens, Ordering::Relaxed);
        self.cost_micro_usd
            .fetch_add((outcome.cost_usd * 1_000_000.0) as u64, Ordering::Relaxed);
        let mut stages = self.stages.lock().unwrap_or_else(|lock| lock.into_inner());
        let stats = stages.entry(stage.to_string()).or_default();
        stats.calls += 1;
        stats.input_tokens += outcome.input_tokens;
        stats.cached_input_tokens += outcome.cached_input_tokens;
        stats.cache_write_input_tokens += outcome.cache_write_input_tokens;
        stats.output_tokens += outcome.output_tokens;
        stats.cost_micro_usd += (outcome.cost_usd * 1_000_000.0) as u64;
    }

    fn record_failure(&self, stage: &str) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        let mut stages = self.stages.lock().unwrap_or_else(|lock| lock.into_inner());
        stages.entry(stage.to_string()).or_default().failures += 1;
    }

    fn record_billed_failure(&self, stage: &str, outcome: &CallOutcome) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        self.input_tokens
            .fetch_add(outcome.input_tokens, Ordering::Relaxed);
        self.cached_input_tokens
            .fetch_add(outcome.cached_input_tokens, Ordering::Relaxed);
        self.cache_write_input_tokens
            .fetch_add(outcome.cache_write_input_tokens, Ordering::Relaxed);
        self.output_tokens
            .fetch_add(outcome.output_tokens, Ordering::Relaxed);
        self.cost_micro_usd
            .fetch_add((outcome.cost_usd * 1_000_000.0) as u64, Ordering::Relaxed);
        let mut stages = self.stages.lock().unwrap_or_else(|lock| lock.into_inner());
        let stats = stages.entry(stage.to_string()).or_default();
        stats.failures += 1;
        stats.input_tokens += outcome.input_tokens;
        stats.cached_input_tokens += outcome.cached_input_tokens;
        stats.cache_write_input_tokens += outcome.cache_write_input_tokens;
        stats.output_tokens += outcome.output_tokens;
        stats.cost_micro_usd += (outcome.cost_usd * 1_000_000.0) as u64;
    }

    /// Cumulative per-stage usage for the current process, sorted by stage name.
    pub fn stage_snapshot(&self) -> BTreeMap<String, StageStats> {
        self.stages
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .clone()
    }

    /// Human-readable audit that exposes input/cache/output and failed attempts.
    pub fn usage_report(&self) -> String {
        let total = self.snapshot();
        let mut out = format!(
            "{} attempts · {} completed · {} failed · {} fallback · {} input · {} cached · {} cache writes · {} output · ${:.4}",
            total.attempts,
            total.calls,
            total.failures,
            total.fallback_attempts,
            total.input_tokens,
            total.cached_input_tokens,
            total.cache_write_input_tokens,
            total.output_tokens,
            total.cost_usd,
        );
        let mut stages = self.stage_snapshot().into_iter().collect::<Vec<_>>();
        stages.sort_by(|left, right| {
            let left_total =
                left.1.input_tokens + left.1.cached_input_tokens + left.1.output_tokens;
            let right_total =
                right.1.input_tokens + right.1.cached_input_tokens + right.1.output_tokens;
            right_total
                .cmp(&left_total)
                .then_with(|| left.0.cmp(&right.0))
        });
        for (stage, stats) in stages {
            out.push_str(&format!(
                "\n{stage}: {} attempts, {} completed, {} failed, {} input, {} cached, {} cache writes, {} output, {} prompt chars",
                stats.attempts,
                stats.calls,
                stats.failures,
                stats.input_tokens,
                stats.cached_input_tokens,
                stats.cache_write_input_tokens,
                stats.output_tokens,
                stats.prompt_chars,
            ));
        }
        out
    }

    pub fn usage_summary_since(&self, base: StatsSnapshot) -> String {
        let usage = self.snapshot().since(base);
        format!(
            "{} attempts · {} completed · {} failed · {} fallback · {} input · {} cached · {} cache writes · {} output · ${:.4}",
            usage.attempts,
            usage.calls,
            usage.failures,
            usage.fallback_attempts,
            usage.input_tokens,
            usage.cached_input_tokens,
            usage.cache_write_input_tokens,
            usage.output_tokens,
            usage.cost_usd,
        )
    }
}

impl StatsSnapshot {
    /// The delta from an earlier snapshot to this one.
    pub fn since(&self, base: StatsSnapshot) -> StatsSnapshot {
        StatsSnapshot {
            attempts: self.attempts.saturating_sub(base.attempts),
            calls: self.calls.saturating_sub(base.calls),
            failures: self.failures.saturating_sub(base.failures),
            fallback_attempts: self
                .fallback_attempts
                .saturating_sub(base.fallback_attempts),
            input_tokens: self.input_tokens.saturating_sub(base.input_tokens),
            cached_input_tokens: self
                .cached_input_tokens
                .saturating_sub(base.cached_input_tokens),
            cache_write_input_tokens: self
                .cache_write_input_tokens
                .saturating_sub(base.cache_write_input_tokens),
            output_tokens: self.output_tokens.saturating_sub(base.output_tokens),
            prompt_chars: self.prompt_chars.saturating_sub(base.prompt_chars),
            cost_usd: (self.cost_usd - base.cost_usd).max(0.0),
        }
    }
}

/// One semantic event from a streaming model-CLI call. Borrows from the JSON
/// line that produced it, so it is only valid for the callback's duration.
///
/// Some fields (redacted thinking text, tool name) are carried for API
/// completeness even though the current UI doesn't render them.
#[allow(dead_code)]
pub enum StreamEvent<'a> {
    /// A content block opened; `kind` is "thinking" | "text" | "tool_use".
    BlockStart(&'a str),
    /// Reasoning text exposed by the backend (empty/redacted in some Claude
    /// streams; summarized by Codex).
    ThinkingDelta(&'a str),
    /// A chunk of the model's visible answer.
    TextDelta(&'a str),
    /// A chunk of the streaming tool-call arguments (partial JSON). For a
    /// structured call this is the decision object being assembled — the UI can
    /// pull fields (e.g. a `plan`) out of it as they stream.
    ToolInputDelta(&'a str),
    /// The model invoked a tool (for structured calls this is `StructuredOutput`).
    ToolUse { name: &'a str },
    /// The current content block closed.
    BlockStop,
}

/// What a completed call yielded, regardless of output format.
#[derive(Debug, Default)]
pub struct CallOutcome {
    pub result_text: String,
    pub structured: Option<Value>,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd: f64,
    pub service_tier: String,
    pub duration_ms: u64,
    pub is_error: bool,
}

#[derive(Debug)]
struct BilledCallError {
    message: String,
    usage: CallOutcome,
}

impl std::fmt::Display for BilledCallError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BilledCallError {}

#[derive(Clone, Debug)]
struct ModelSelection {
    backend: Backend,
    model: Option<String>,
    generation: u64,
    fast: bool,
}

#[derive(Debug)]
struct ModelState {
    backend: Backend,
    openai_model: Option<String>,
    codex_model: Option<String>,
    claude_model: Option<String>,
    grok_model: Option<String>,
    generation: u64,
}

impl ModelState {
    fn active(&self) -> ModelSelection {
        self.selection(self.backend)
    }

    fn selection(&self, backend: Backend) -> ModelSelection {
        ModelSelection {
            backend,
            model: match backend {
                Backend::Openai => self.openai_model.clone(),
                Backend::Codex => self.codex_model.clone(),
                Backend::Claude => self.claude_model.clone(),
                Backend::Grok => self.grok_model.clone(),
            },
            generation: self.generation,
            fast: false,
        }
    }

    fn model_mut(&mut self, backend: Backend) -> &mut Option<String> {
        match backend {
            Backend::Openai => &mut self.openai_model,
            Backend::Codex => &mut self.codex_model,
            Backend::Claude => &mut self.claude_model,
            Backend::Grok => &mut self.grok_model,
        }
    }
}

#[derive(Clone, Copy)]
struct TurnBudget {
    base: StatsSnapshot,
    max_attempts: u64,
    max_output_tokens: u64,
    max_cost_usd: f64,
}

/// An automatic provider change caused by the active CLI exhausting its usage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelSwitch {
    pub from: Backend,
    pub to: Backend,
    pub model: String,
}

pub struct Engine {
    models: RwLock<ModelState>,
    switch_notices: Mutex<Vec<ModelSwitch>>,
    stats: Arc<Stats>,
    /// Wall-clock cap per inference call, so a hung/rate-limited provider surfaces as
    /// an error instead of spinning forever. Override via SPRUCE_MODEL_TIMEOUT_SECS.
    call_timeout: Duration,
    http: reqwest::Client,
    openai_api_key: Option<String>,
    openai_base_url: String,
    /// Sol/high and Sol/xhigh calls are expensive, long-lived requests. Letting
    /// every account and recipient fan-out hit the API at once produced a
    /// thundering herd of connection failures. Queue frontier work globally;
    /// fast Luna routing remains unconstrained.
    frontier_limiter: Arc<tokio::sync::Semaphore>,
    /// A real quota response opens a session-local circuit. Queued work for
    /// that provider then fails locally instead of spending dozens more HTTP
    /// attempts to rediscover the same zero-credit state.
    exhausted_backends: Mutex<HashSet<Backend>>,
    /// One natural-language turn has a bounded inference envelope. This is a
    /// final safety rail, not a target: normal outreach should finish far below
    /// it, while runaway retries/planners stop before consuming an open-ended
    /// amount of paid inference.
    turn_budget: Mutex<Option<TurnBudget>>,
    /// Metadata-only audit trail. Prompts and model output are never logged.
    usage_log: Mutex<Option<PathBuf>>,
}

impl Engine {
    pub fn new(backend: Backend, model: Option<String>) -> Self {
        let secs = std::env::var("SPRUCE_MODEL_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|s| *s > 0)
            .unwrap_or(240);
        let openai_default = default_openai_model();
        let codex_default = default_codex_model();
        let frontier_concurrency = std::env::var("SPRUCE_OPENAI_FRONTIER_CONCURRENCY")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(2)
            .clamp(1, 8);
        Self {
            models: RwLock::new(ModelState {
                backend,
                openai_model: Some(if backend == Backend::Openai {
                    model.clone().unwrap_or(openai_default)
                } else {
                    openai_default
                }),
                codex_model: Some(if backend == Backend::Codex {
                    model.clone().unwrap_or(codex_default)
                } else {
                    codex_default
                }),
                claude_model: if backend == Backend::Claude {
                    model.clone()
                } else {
                    None
                },
                grok_model: if backend == Backend::Grok {
                    model
                } else {
                    None
                },
                generation: 0,
            }),
            switch_notices: Mutex::new(Vec::new()),
            stats: Arc::new(Stats::default()),
            call_timeout: Duration::from_secs(secs),
            http: reqwest::Client::new(),
            openai_api_key: std::env::var("OPENAI_API_KEY")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            openai_base_url: std::env::var("SPRUCE_OPENAI_BASE_URL")
                .ok()
                .map(|value| value.trim_end_matches('/').to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "https://api.openai.com/v1".to_string()),
            frontier_limiter: Arc::new(tokio::sync::Semaphore::new(frontier_concurrency)),
            exhausted_backends: Mutex::new(HashSet::new()),
            turn_budget: Mutex::new(None),
            usage_log: Mutex::new(Some(
                std::env::var("SPRUCE_USAGE_LOG")
                    .ok()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from(".spruce/model-usage.jsonl")),
            )),
        }
    }

    /// A handle onto the cumulative token/cost/call counters.
    pub fn stats(&self) -> Arc<Stats> {
        self.stats.clone()
    }

    /// Start a fresh cost/token/attempt envelope for one user turn.
    pub fn begin_turn_budget(&self) {
        let max_attempts = std::env::var("SPRUCE_TURN_MAX_MODEL_ATTEMPTS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(100)
            .max(1);
        let max_output_tokens = std::env::var("SPRUCE_TURN_MAX_OUTPUT_TOKENS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(120_000)
            .max(1_000);
        let max_cost_usd = std::env::var("SPRUCE_TURN_MAX_COST_USD")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(2.0);
        *self
            .turn_budget
            .lock()
            .unwrap_or_else(|lock| lock.into_inner()) = Some(TurnBudget {
            base: self.stats.snapshot(),
            max_attempts,
            max_output_tokens,
            max_cost_usd,
        });
    }

    /// Size the default safety envelope to an explicitly requested outreach
    /// scope. These are ceilings, not spend targets. Explicit environment
    /// limits remain hard operator overrides.
    pub fn scale_turn_budget_for_outreach(&self, accounts: usize, recipients: usize) {
        if recipients == 0 {
            return;
        }
        let (attempts, output_tokens, cost_usd) = outreach_budget_floor(accounts, recipients);
        let mut budget = self
            .turn_budget
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let Some(budget) = budget.as_mut() else {
            return;
        };
        if std::env::var_os("SPRUCE_TURN_MAX_MODEL_ATTEMPTS").is_none() {
            budget.max_attempts = budget.max_attempts.max(attempts);
        }
        if std::env::var_os("SPRUCE_TURN_MAX_OUTPUT_TOKENS").is_none() {
            budget.max_output_tokens = budget.max_output_tokens.max(output_tokens);
        }
        if std::env::var_os("SPRUCE_TURN_MAX_COST_USD").is_none() {
            budget.max_cost_usd = budget.max_cost_usd.max(cost_usd);
        }
    }

    fn check_turn_budget(&self) -> Result<()> {
        let budget = *self
            .turn_budget
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let Some(budget) = budget else {
            return Ok(());
        };
        let used = self.stats.snapshot().since(budget.base);
        if used.attempts >= budget.max_attempts {
            bail!(
                "Spruce per-turn model-attempt ceiling reached ({} attempts; limit {})",
                used.attempts,
                budget.max_attempts
            );
        }
        if used.output_tokens >= budget.max_output_tokens {
            bail!(
                "Spruce per-turn output-token ceiling reached ({} tokens; limit {})",
                used.output_tokens,
                budget.max_output_tokens
            );
        }
        if used.cost_usd >= budget.max_cost_usd {
            bail!(
                "Spruce per-turn model-cost ceiling reached (${:.2}; limit ${:.2})",
                used.cost_usd,
                budget.max_cost_usd
            );
        }
        Ok(())
    }

    pub fn backend(&self) -> Backend {
        self.selection().backend
    }

    pub fn model_label(&self) -> String {
        self.selection()
            .model
            .unwrap_or_else(|| "default".to_string())
    }

    /// Production separates angle selection from realization. Folding both into
    /// one call is cheaper, but it made the writer defend its first idea instead
    /// of choosing among distinct angles.
    pub fn prefers_lean_outreach(&self) -> bool {
        if std::env::var("SPRUCE_FOLD_OUTREACH_PLANNER")
            .ok()
            .is_some_and(|value| matches!(value.trim(), "1" | "true" | "on"))
        {
            return true;
        }
        if let Ok(value) = std::env::var("SPRUCE_SEPARATE_OUTREACH_PLANNER") {
            return value.trim() != "1";
        }
        self.backend() == Backend::Codex
    }

    /// Select a provider while preserving its last model override.
    pub fn select_backend(&self, backend: Backend) {
        let mut state = self.models.write().unwrap_or_else(|lock| lock.into_inner());
        if state.backend != backend {
            state.backend = backend;
            state.generation = state.generation.wrapping_add(1);
        }
        self.exhausted_backends
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .remove(&backend);
    }

    /// Select a provider and set (or clear) its model override.
    pub fn select_model(&self, backend: Backend, model: Option<String>) {
        let model = match backend {
            Backend::Openai => Some(model.unwrap_or_else(default_openai_model)),
            Backend::Codex => Some(model.unwrap_or_else(default_codex_model)),
            _ => model,
        };
        let mut state = self.models.write().unwrap_or_else(|lock| lock.into_inner());
        let changed = state.backend != backend || *state.model_mut(backend) != model;
        *state.model_mut(backend) = model;
        state.backend = backend;
        if changed {
            state.generation = state.generation.wrapping_add(1);
        }
        self.exhausted_backends
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .remove(&backend);
    }

    /// Drain provider-switch notices for a UI to render after the active turn.
    pub fn take_model_switches(&self) -> Vec<ModelSwitch> {
        let mut notices = self
            .switch_notices
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        std::mem::take(&mut *notices)
    }

    fn selection(&self) -> ModelSelection {
        self.models
            .read()
            .unwrap_or_else(|lock| lock.into_inner())
            .active()
    }

    /// Lightweight stages can use a cheaper backend-specific model without
    /// changing the user's selected model for substantive work.
    fn selection_for(&self, fast: bool) -> ModelSelection {
        let mut selection = self.selection();
        self.apply_fast_model(&mut selection, fast);
        selection
    }

    fn selection_for_backend(&self, backend: Backend, fast: bool) -> ModelSelection {
        let mut selection = self
            .models
            .read()
            .unwrap_or_else(|lock| lock.into_inner())
            .selection(backend);
        self.apply_fast_model(&mut selection, fast);
        selection
    }

    fn apply_fast_model(&self, selection: &mut ModelSelection, fast: bool) {
        if !fast {
            return;
        }
        selection.fast = true;
        let key = match selection.backend {
            Backend::Openai => "SPRUCE_OPENAI_FAST_MODEL",
            Backend::Codex => "SPRUCE_CODEX_FAST_MODEL",
            Backend::Claude => "SPRUCE_CLAUDE_FAST_MODEL",
            Backend::Grok => "SPRUCE_GROK_FAST_MODEL",
        };
        selection.model = std::env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| (selection.backend == Backend::Openai).then(|| "gpt-5.6-luna".to_string()))
            .or_else(|| (selection.backend == Backend::Codex).then(|| "gpt-5.6-luna".to_string()))
            .or_else(|| (selection.backend == Backend::Claude).then(|| "haiku".to_string()))
            .or_else(|| selection.model.clone());
    }

    /// A cheap router may borrow the alternate provider for this one call, but
    /// it must not mutate the provider selected for the expensive action the
    /// router is about to launch.
    fn temporary_fallback(&self, failed: &ModelSelection, fast: bool) -> ModelSelection {
        self.selection_for_backend(failed.backend.other(), fast)
    }

    /// Use the frontier model only for the few outreach stages where taste,
    /// synthesis, and skeptical-recipient judgment materially change the
    /// result. Routing, extraction, and mechanical cleanup stay on the normal
    /// or fast lane. This keeps Sol spend focused instead of multiplying it
    /// across every CRM operation.
    fn selection_for_stage(&self, stage: &str, fast: bool) -> ModelSelection {
        let mut selection = self.selection_for(fast);
        if !fast
            && selection.backend == Backend::Openai
            && (is_outreach_quality_stage(stage) || is_outreach_strategy_stage(stage))
        {
            let model_keys: &[&str] = match stage {
                "outreach.write_account" => {
                    &["SPRUCE_OPENAI_WRITER_MODEL", "SPRUCE_OPENAI_COPY_MODEL"]
                }
                "outreach.review_edit" => {
                    &["SPRUCE_OPENAI_EDITOR_MODEL", "SPRUCE_OPENAI_COPY_MODEL"]
                }
                "outreach.verify_final" | "outreach.eval_pairwise" => {
                    &["SPRUCE_OPENAI_VERIFIER_MODEL", "SPRUCE_OPENAI_COPY_MODEL"]
                }
                _ if is_outreach_quality_stage(stage) => &["SPRUCE_OPENAI_COPY_MODEL"],
                _ => &["SPRUCE_OPENAI_STRATEGY_MODEL"],
            };
            selection.model = model_keys
                .iter()
                .find_map(|key| std::env::var(key).ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .or_else(|| (stage == "outreach.write_account").then(|| "gpt-5.6-sol".to_string()))
                .or(selection.model);
        }
        selection
    }

    /// If the dedicated frontier writer lane is temporarily unavailable, keep
    /// the job moving on the user's normal OpenAI model. This is a stage-local
    /// fallback: it does not change the selected model for the session and it
    /// never crosses providers for bulk work.
    fn transient_stage_fallback(
        &self,
        stage: &str,
        failed: &ModelSelection,
    ) -> Option<ModelSelection> {
        if stage != "outreach.write_account"
            || failed.backend != Backend::Openai
            || !failed
                .model
                .as_deref()
                .is_some_and(|model| model.starts_with("gpt-5.6-sol"))
        {
            return None;
        }
        let mut fallback = self.selection_for(false);
        fallback.model = std::env::var("SPRUCE_OPENAI_WRITER_FALLBACK_MODEL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                fallback
                    .model
                    .clone()
                    .filter(|model| !model.starts_with("gpt-5.6-sol"))
            })
            .or_else(|| Some("gpt-5.6-terra".to_string()));
        (fallback.backend == Backend::Openai && fallback.model != failed.model).then_some(fallback)
    }

    /// Human-readable model lane shown before costly outreach drafting begins.
    pub fn outreach_quality_label(&self) -> String {
        let selection = self.selection_for_stage("outreach.write_account", false);
        let model = selection.model.unwrap_or_else(|| "default".to_string());
        match selection.backend {
            Backend::Openai => format!(
                "{model} · {}",
                openai_reasoning_effort("outreach.write_account", false)
            ),
            Backend::Codex => format!(
                "{model} · {}",
                codex_reasoning_effort("outreach.write_account", false)
            ),
            _ => model,
        }
    }

    /// Switch only if the failed call still represents the active selection.
    /// This prevents several concurrent calls from toggling the provider back
    /// and forth after they all observe the same exhausted quota.
    fn fallback_after(&self, failed: &ModelSelection) -> ModelSelection {
        let mut state = self.models.write().unwrap_or_else(|lock| lock.into_inner());
        if state.backend == failed.backend && state.generation == failed.generation {
            let from = state.backend;
            state.backend = from.other();
            state.generation = state.generation.wrapping_add(1);
            let active = state.active();
            self.switch_notices
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .push(ModelSwitch {
                    from,
                    to: active.backend,
                    model: active
                        .model
                        .clone()
                        .unwrap_or_else(|| "default".to_string()),
                });
            active
        } else {
            state.active()
        }
    }

    fn log_usage(
        &self,
        stage: &str,
        selection: &ModelSelection,
        prompt_chars: u64,
        fallback: bool,
        result: &Result<CallOutcome>,
    ) {
        let guard = self
            .usage_log
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let Some(path) = guard.as_ref() else {
            return;
        };
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            let _ = std::fs::create_dir_all(parent);
        }
        let (status, input, cached, cache_write, output, cost, service_tier, error_kind, error) =
            match result {
                Ok(outcome) => (
                    "completed",
                    outcome.input_tokens,
                    outcome.cached_input_tokens,
                    outcome.cache_write_input_tokens,
                    outcome.output_tokens,
                    outcome.cost_usd,
                    outcome.service_tier.as_str(),
                    "",
                    String::new(),
                ),
                Err(error) => {
                    let error_kind = if is_usage_exhausted(error) {
                        "usage_exhausted"
                    } else if is_generation_incomplete(error) {
                        "generation_incomplete"
                    } else if is_retryable_provider_error(error) {
                        "transient_provider_error"
                    } else {
                        "provider_error"
                    };
                    let billed = error.downcast_ref::<BilledCallError>();
                    (
                        "failed",
                        billed.map_or(0, |failure| failure.usage.input_tokens),
                        billed.map_or(0, |failure| failure.usage.cached_input_tokens),
                        billed.map_or(0, |failure| failure.usage.cache_write_input_tokens),
                        billed.map_or(0, |failure| failure.usage.output_tokens),
                        billed.map_or(0.0, |failure| failure.usage.cost_usd),
                        billed.map_or("", |failure| failure.usage.service_tier.as_str()),
                        error_kind,
                        format!("{error:#}")
                            .lines()
                            .next()
                            .unwrap_or_default()
                            .to_string(),
                    )
                }
            };
        let event = serde_json::json!({
            "ts": chrono::Utc::now().to_rfc3339(),
            "stage": stage,
            "backend": selection.backend.as_str(),
            "model": selection.model.as_deref().unwrap_or("default"),
            "fast": selection.fast,
            "fallback": fallback,
            "status": status,
            "prompt_chars": prompt_chars,
            "input_tokens": input,
            "cached_input_tokens": cached,
            "cache_write_input_tokens": cache_write,
            "output_tokens": output,
            "cost_usd": cost,
            "service_tier": service_tier,
            "error_kind": error_kind,
            "error": error,
        });
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(file, "{event}");
        }
    }

    /// Preflight the selected inference provider without spending model tokens.
    pub async fn check(&self) -> Result<String> {
        if self.backend() == Backend::Openai {
            if self.openai_api_key.is_none() {
                bail!("OPENAI_API_KEY is not set — add it to .env before using --backend openai");
            }
            return Ok(format!("Responses API · {}", self.model_label()));
        }
        let executable = self.backend().as_str();
        let out = Command::new(executable)
            .arg("--version")
            .kill_on_drop(true)
            .output()
            .await
            .with_context(|| {
                format!("couldn't run `{executable}` — install its CLI and make sure it is on PATH")
            })?;
        if !out.status.success() {
            bail!(
                "`{executable} --version` failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Shared Claude argv for both the blocking and streaming paths.
    fn claude_command(&self, system: &str, schema: Option<&str>, model: Option<&str>) -> Command {
        let mut cmd = Command::new("claude");
        cmd.arg("-p")
            // spruce-leaf needs inference, not a second coding agent. Removing
            // tools, skills, plugins and project discovery cuts a large hidden
            // system prompt from every subprocess call.
            .arg("--safe-mode")
            .arg("--tools")
            .arg("")
            .arg("--disable-slash-commands")
            .arg("--no-chrome")
            .arg("--no-session-persistence")
            .arg("--strict-mcp-config")
            .arg("--system-prompt")
            .arg(cli_safe_arg(system).as_ref());
        if let Some(m) = model {
            cmd.arg("--model").arg(m);
        }
        if let Some(s) = schema {
            cmd.arg("--json-schema").arg(s);
        }
        // So a timed-out call's child is killed rather than left running.
        cmd.kill_on_drop(true);
        cmd
    }

    /// Codex runs as an inference-only subprocess. A compact per-call
    /// instruction file replaces the coding agent's built-in instructions;
    /// the task itself remains the user message.
    fn codex_command(
        &self,
        instructions_path: &PathBuf,
        model: Option<&str>,
        effort: &str,
    ) -> Command {
        let instructions_path =
            toml::Value::String(instructions_path.to_string_lossy().to_string()).to_string();
        let mut cmd = Command::new("codex");
        cmd.arg("exec")
            .arg("--ephemeral")
            .arg("--sandbox")
            .arg("read-only")
            .arg("--skip-git-repo-check")
            .arg("--ignore-user-config")
            .arg("--ignore-rules")
            .arg("--strict-config")
            // Codex CLI is an agent by default. This process is only a typed
            // inference adapter, so omit agent tools and their prompt payloads.
            .arg("--disable")
            .arg("shell_tool")
            .arg("--disable")
            .arg("unified_exec")
            .arg("--disable")
            .arg("apps")
            .arg("--disable")
            .arg("plugins")
            .arg("--disable")
            .arg("multi_agent")
            .arg("--disable")
            .arg("multi_agent_v2")
            .arg("--disable")
            .arg("computer_use")
            .arg("--disable")
            .arg("in_app_browser")
            .arg("--disable")
            .arg("browser_use")
            .arg("--disable")
            .arg("image_generation")
            .arg("--disable")
            .arg("goals")
            .arg("--disable")
            .arg("tool_suggest")
            .arg("--color")
            .arg("never")
            .arg("--json")
            .arg("--cd")
            .arg(std::env::temp_dir())
            .arg("--config")
            .arg(format!("model_instructions_file={instructions_path}"))
            .arg("--config")
            .arg("include_permissions_instructions=false")
            .arg("--config")
            .arg("include_apps_instructions=false")
            .arg("--config")
            .arg("include_collaboration_mode_instructions=false")
            .arg("--config")
            .arg("include_environment_context=false")
            // Pin effort on every invocation. Spruce ignores the user's global
            // Codex config, so its cost posture remains stable across upgrades.
            .arg("--config")
            .arg(format!("model_reasoning_effort=\"{effort}\""));
        if let Some(model) = model {
            cmd.arg("--model").arg(model);
        }
        cmd.kill_on_drop(true);
        cmd
    }

    /// Shared Grok argv for both the blocking and streaming paths.
    ///
    /// Grok is an agent CLI by default. spruce-leaf only needs inference, so
    /// tools/subagents/web are disabled and the system prompt is overridden
    /// with application instructions.
    fn grok_command(
        &self,
        system: &str,
        schema: Option<&str>,
        model: Option<&str>,
        fast: bool,
    ) -> Command {
        let mut cmd = Command::new("grok");
        cmd.arg("--system-prompt-override")
            .arg(cli_safe_arg(system).as_ref())
            .arg("--disallowed-tools")
            .arg("all")
            .arg("--disable-web-search")
            .arg("--no-subagents")
            .arg("--no-plan")
            .arg("--max-turns")
            .arg("1")
            .arg("--permission-mode")
            .arg("dontAsk");
        if fast {
            cmd.arg("--reasoning-effort").arg("low");
        }
        if let Some(m) = model {
            cmd.arg("--model").arg(m);
        }
        if let Some(s) = schema {
            cmd.arg("--json-schema").arg(s);
        }
        cmd.kill_on_drop(true);
        cmd
    }

    /// One blocking Claude invocation; returns a provider-neutral outcome.
    async fn call_claude(
        &self,
        selection: &ModelSelection,
        system: &str,
        user: &str,
        schema: Option<&Value>,
    ) -> Result<CallOutcome> {
        // Hold the serialized schema alive until after the command runs.
        let schema_str = match schema {
            Some(s) => Some(serde_json::to_string(s).context("serializing JSON schema")?),
            None => None,
        };
        let mut cmd =
            self.claude_command(system, schema_str.as_deref(), selection.model.as_deref());
        cmd.arg("--output-format").arg("json");
        cmd.arg(cli_safe_arg(user).as_ref())
            .stdin(std::process::Stdio::null());

        let out = cmd
            .output()
            .await
            .context("failed to run `claude` (is Claude Code installed and on PATH?)")?;

        let stdout = String::from_utf8_lossy(&out.stdout);
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            bail!(
                "claude CLI exited with {}:\n{}\n{}",
                out.status,
                stderr.trim(),
                stdout.trim()
            );
        }

        let payload: Value = serde_json::from_str(stdout.trim())
            .with_context(|| format!("parsing claude CLI output as JSON:\n{stdout}"))?;

        if payload["is_error"].as_bool().unwrap_or(false) {
            bail!(
                "claude CLI reported an error: {}",
                payload["result"].as_str().unwrap_or("<no message>")
            );
        }

        let outcome = CallOutcome {
            result_text: payload["result"].as_str().unwrap_or_default().to_string(),
            structured: payload
                .get("structured_output")
                .filter(|value| !value.is_null())
                .cloned(),
            input_tokens: payload["usage"]["input_tokens"].as_u64().unwrap_or(0),
            cached_input_tokens: payload["usage"]["cache_read_input_tokens"]
                .as_u64()
                .or_else(|| payload["usage"]["cached_input_tokens"].as_u64())
                .unwrap_or(0),
            cache_write_input_tokens: 0,
            output_tokens: payload["usage"]["output_tokens"].as_u64().unwrap_or(0),
            cost_usd: payload["total_cost_usd"].as_f64().unwrap_or(0.0),
            service_tier: "cli".to_string(),
            duration_ms: payload["duration_ms"].as_u64().unwrap_or(0),
            is_error: false,
        };
        Ok(outcome)
    }

    /// One blocking Grok invocation; returns a provider-neutral outcome.
    async fn call_grok(
        &self,
        selection: &ModelSelection,
        system: &str,
        user: &str,
        schema: Option<&Value>,
    ) -> Result<CallOutcome> {
        let schema_str = match schema {
            Some(s) => Some(serde_json::to_string(s).context("serializing JSON schema")?),
            None => None,
        };
        let mut cmd = self.grok_command(
            system,
            schema_str.as_deref(),
            selection.model.as_deref(),
            selection.fast,
        );
        // Grok takes the single-turn prompt as the value of `-p`/`--single`.
        cmd.arg("--output-format")
            .arg("json")
            .arg("-p")
            .arg(cli_safe_arg(user).as_ref())
            .stdin(std::process::Stdio::null());

        let out = cmd
            .output()
            .await
            .context("failed to run `grok` (is the Grok CLI installed and on PATH?)")?;

        let stdout = String::from_utf8_lossy(&out.stdout);
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            bail!(
                "grok CLI exited with {}:\n{}\n{}",
                out.status,
                stderr.trim(),
                stdout.trim()
            );
        }

        let payload: Value = serde_json::from_str(stdout.trim())
            .with_context(|| format!("parsing grok CLI output as JSON:\n{stdout}"))?;

        if payload["is_error"].as_bool().unwrap_or(false) {
            bail!(
                "grok CLI reported an error: {}",
                payload["text"]
                    .as_str()
                    .or_else(|| payload["result"].as_str())
                    .unwrap_or("<no message>")
            );
        }

        let result_text = payload["text"]
            .as_str()
            .or_else(|| payload["result"].as_str())
            .unwrap_or_default()
            .to_string();
        // Non-stream JSON uses camelCase `structuredOutput`; stream result uses
        // snake_case. Fall back to parsing the text body when a schema was set.
        let structured = payload
            .get("structuredOutput")
            .or_else(|| payload.get("structured_output"))
            .filter(|value| !value.is_null())
            .cloned()
            .or_else(|| {
                schema
                    .is_some()
                    .then(|| serde_json::from_str(result_text.trim()).ok())
                    .flatten()
            });

        let outcome = CallOutcome {
            result_text,
            structured,
            input_tokens: payload["usage"]["input_tokens"].as_u64().unwrap_or(0),
            cached_input_tokens: payload["usage"]["cache_read_input_tokens"]
                .as_u64()
                .or_else(|| payload["usage"]["cached_input_tokens"].as_u64())
                .unwrap_or(0),
            cache_write_input_tokens: 0,
            output_tokens: payload["usage"]["output_tokens"].as_u64().unwrap_or(0),
            cost_usd: payload["total_cost_usd"].as_f64().unwrap_or(0.0),
            service_tier: "cli".to_string(),
            duration_ms: payload["duration_ms"].as_u64().unwrap_or(0),
            is_error: false,
        };
        Ok(outcome)
    }

    /// One direct OpenAI Responses API call. This is intentionally a plain
    /// inference request: no agent tools, project discovery, or coding CLI
    /// system prompt is involved.
    async fn call_openai(
        &self,
        stage: &str,
        selection: &ModelSelection,
        system: &str,
        user: &str,
        schema: Option<&Value>,
    ) -> Result<CallOutcome> {
        let api_key = self.openai_api_key.as_deref().ok_or_else(|| {
            anyhow!("OPENAI_API_KEY is not set — add it to .env before using OpenAI")
        })?;
        let request = openai_request(stage, selection, system, user, schema);
        let started = Instant::now();
        let response = self
            .http
            .post(format!("{}/responses", self.openai_base_url))
            .bearer_auth(api_key)
            .json(&request)
            .send()
            .await
            .context("sending OpenAI Responses API request")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("reading OpenAI Responses API response")?;
        let payload: Value = serde_json::from_str(&body).with_context(|| {
            format!(
                "OpenAI API returned non-JSON HTTP {status}: {}",
                body.chars().take(500).collect::<String>()
            )
        })?;
        if !status.is_success() {
            bail!(
                "OpenAI API HTTP {status}: {} (type: {}, code: {})",
                payload["error"]["message"]
                    .as_str()
                    .unwrap_or("unknown OpenAI API error"),
                payload["error"]["type"].as_str().unwrap_or("unknown"),
                payload["error"]["code"].as_str().unwrap_or("unknown"),
            );
        }
        let payload = if openai_uses_background(stage, selection) {
            self.poll_openai_response(api_key, payload).await?
        } else {
            payload
        };
        parse_openai_response(&payload, schema.is_some(), started.elapsed())
    }

    /// Long-running reasoning requests are created once and then retrieved by
    /// response id. A failed poll must never restart the generation: doing so
    /// both wastes spend and can create several independent copies of the same
    /// answer. Short GET failures therefore retry here, against the same id.
    async fn poll_openai_response(&self, api_key: &str, mut payload: Value) -> Result<Value> {
        self.poll_openai_response_inner(api_key, &mut payload)
            .await
            .map_err(|error| {
                anyhow::Error::new(OpenAiBackgroundPollError(format!(
                    "OpenAI background response could not be retrieved safely: {error:#}"
                )))
            })?;
        Ok(payload)
    }

    async fn poll_openai_response_inner(&self, api_key: &str, payload: &mut Value) -> Result<()> {
        let initial_status = payload["status"].as_str().unwrap_or("unknown");
        if !matches!(initial_status, "queued" | "in_progress") {
            return Ok(());
        }
        let response_id = payload["id"]
            .as_str()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| anyhow!("OpenAI background response did not include an id"))?
            .to_string();
        let url = format!("{}/responses/{response_id}", self.openai_base_url);
        let mut consecutive_failures = 0u32;

        loop {
            let status = payload["status"].as_str().unwrap_or("unknown");
            if !matches!(status, "queued" | "in_progress") {
                return Ok(());
            }

            tokio::time::sleep(openai_background_poll_interval()).await;
            let response = match self.http.get(&url).bearer_auth(api_key).send().await {
                Ok(response) => response,
                Err(error) if consecutive_failures < MAX_BACKGROUND_POLL_RETRIES => {
                    consecutive_failures += 1;
                    tokio::time::sleep(transient_backoff(consecutive_failures)).await;
                    let _ = error;
                    continue;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "retrieving OpenAI background response {response_id} after {} retries",
                            MAX_BACKGROUND_POLL_RETRIES
                        )
                    });
                }
            };
            let http_status = response.status();
            let body = response
                .text()
                .await
                .with_context(|| format!("reading OpenAI background response {response_id}"))?;

            if (http_status.as_u16() == 429 || http_status.is_server_error())
                && consecutive_failures < MAX_BACKGROUND_POLL_RETRIES
            {
                consecutive_failures += 1;
                tokio::time::sleep(transient_backoff(consecutive_failures)).await;
                continue;
            }

            *payload = serde_json::from_str(&body).with_context(|| {
                format!(
                    "OpenAI background response {response_id} returned non-JSON HTTP {http_status}: {}",
                    body.chars().take(500).collect::<String>()
                )
            })?;
            if !http_status.is_success() {
                bail!(
                    "OpenAI background response {response_id} returned HTTP {http_status}: {} (type: {}, code: {})",
                    payload["error"]["message"]
                        .as_str()
                        .unwrap_or("unknown OpenAI API error"),
                    payload["error"]["type"].as_str().unwrap_or("unknown"),
                    payload["error"]["code"].as_str().unwrap_or("unknown"),
                );
            }
            consecutive_failures = 0;
        }
    }

    /// Run provider work under the configured wall-clock cap. CLI children are
    /// killed on drop; HTTP request futures are cancelled.
    async fn with_timeout<T>(
        &self,
        backend: Backend,
        timeout: Duration,
        background_response: bool,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        match tokio::time::timeout(timeout, fut).await {
            Ok(res) => res,
            Err(_) if background_response => Err(anyhow::Error::new(
                OpenAiBackgroundPollError(format!(
                    "OpenAI background response timed out after {}s; the existing generation was not restarted (raise SPRUCE_OPENAI_BACKGROUND_TIMEOUT_SECS to allow longer)",
                    timeout.as_secs()
                )),
            )),
            Err(_) => bail!(
                "{} call timed out after {}s — the provider did not complete \
                 (raise SPRUCE_MODEL_TIMEOUT_SECS to allow longer)",
                backend.as_str(),
                timeout.as_secs()
            ),
        }
    }

    fn timeout_for(&self, stage: &str, selection: &ModelSelection) -> Duration {
        if openai_uses_background(stage, selection) {
            let seconds = std::env::var("SPRUCE_OPENAI_BACKGROUND_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(900);
            return self.call_timeout.max(Duration::from_secs(seconds));
        }
        self.call_timeout
    }

    async fn call_once(
        &self,
        stage: &str,
        selection: &ModelSelection,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        fallback: bool,
    ) -> Result<CallOutcome> {
        self.check_turn_budget()?;
        if self
            .exhausted_backends
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .contains(&selection.backend)
        {
            bail!(
                "{} usage exhausted earlier in this session; queued paid work was stopped locally",
                selection.backend
            );
        }
        let _frontier_permit = if selection.backend == Backend::Openai
            && (is_outreach_quality_stage(stage) || is_outreach_strategy_stage(stage))
        {
            Some(
                self.frontier_limiter
                    .acquire()
                    .await
                    .map_err(|_| anyhow!("OpenAI frontier request queue closed"))?,
            )
        } else {
            None
        };
        // Quota can be exhausted while this request waits for a frontier
        // permit. Recheck after the queue, before recording or sending it.
        if self
            .exhausted_backends
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .contains(&selection.backend)
        {
            bail!(
                "{} usage exhausted earlier in this session; queued paid work was stopped locally",
                selection.backend
            );
        }
        self.check_turn_budget()?;
        let prompt_chars = request_chars(system, user, schema);
        self.stats.record_attempt(stage, prompt_chars, fallback);
        let work = async {
            match selection.backend {
                Backend::Openai => {
                    self.call_openai(stage, selection, system, user, schema)
                        .await
                }
                Backend::Claude => self.call_claude(selection, system, user, schema).await,
                Backend::Grok => self.call_grok(selection, system, user, schema).await,
                Backend::Codex => {
                    fn ignore(_: StreamEvent<'_>) {}
                    self.stream_codex(stage, selection, system, user, schema, &mut ignore)
                        .await
                }
            }
        };
        let result = self
            .with_timeout(
                selection.backend,
                self.timeout_for(stage, selection),
                openai_uses_background(stage, selection),
                work,
            )
            .await;
        if result.as_ref().is_err_and(is_usage_exhausted) {
            self.exhausted_backends
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .insert(selection.backend);
        }
        match &result {
            Ok(outcome) => self.stats.record_success(stage, outcome),
            Err(error) => {
                if let Some(failure) = error.downcast_ref::<BilledCallError>() {
                    self.stats.record_billed_failure(stage, &failure.usage);
                } else {
                    self.stats.record_failure(stage);
                }
            }
        }
        self.log_usage(stage, selection, prompt_chars, fallback, &result);
        result
    }

    async fn call(
        &self,
        stage: &str,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        allow_fallback: bool,
        fast: bool,
    ) -> Result<CallOutcome> {
        let selection = self.selection_for_stage(stage, fast);
        // Transient CLI failures (a bare non-zero exit or a timeout — usually the
        // local CLI choking under concurrent load, not a bad prompt) are retried a
        // few times with backoff before we give up on the call. Deterministic
        // failures (schema/parse) and genuine usage exhaustion are handled below,
        // not retried here — they would fail identically.
        let max_retries = if selection.backend != Backend::Openai && is_long_bulk_stage(stage) {
            0
        } else {
            MAX_TRANSIENT_RETRIES
        };
        let mut attempt = 0u32;
        let first = loop {
            let outcome = self
                .call_once(stage, &selection, system, user, schema, false)
                .await;
            match &outcome {
                Err(error) if attempt < max_retries && is_safe_to_retry_provider_request(error) => {
                    attempt += 1;
                    tokio::time::sleep(transient_backoff(attempt)).await;
                    continue;
                }
                _ => break outcome,
            }
        };
        let first = match first {
            Err(error)
                if is_safe_to_retry_provider_request(&error)
                    || is_generation_incomplete(&error) =>
            {
                if let Some(fallback) = self.transient_stage_fallback(stage, &selection) {
                    let first_failure = if is_generation_incomplete(&error) {
                        "did not finish within its output allowance"
                    } else {
                        "remained unavailable after bounded retries"
                    };
                    self.call_once(stage, &fallback, system, user, schema, true)
                        .await
                        .with_context(|| {
                            format!(
                                "{} ({}) {first_failure}; writer fallback {} also failed",
                                selection.backend,
                                selection.model.as_deref().unwrap_or("default"),
                                fallback.model.as_deref().unwrap_or("default")
                            )
                        })
                } else {
                    Err(error)
                }
            }
            result => result,
        };
        match first {
            Err(error) if allow_fallback && is_usage_exhausted(&error) => {
                let fallback = if fast {
                    self.temporary_fallback(&selection, true)
                } else {
                    self.fallback_after(&selection)
                };
                self.call_once(stage, &fallback, system, user, schema, true)
                    .await
                    .with_context(|| {
                        format!(
                            "{} usage exhausted; switched to {} ({}) but the retry failed",
                            selection.backend,
                            fallback.backend,
                            fallback.model.as_deref().unwrap_or("default")
                        )
                    })
            }
            Err(error) if is_usage_exhausted(&error) => Err(error).with_context(|| {
                format!(
                    "{stage}: usage exhausted; automatic cross-provider fallback is disabled for bulk work"
                )
            }),
            result => result,
        }
    }

    /// Constrain the response to `schema` and deserialize it into `T`.
    #[allow(dead_code)]
    pub async fn structured<T: DeserializeOwned>(
        &self,
        system: &str,
        user: &str,
        schema: Value,
    ) -> Result<T> {
        self.structured_stage("unattributed", system, user, schema)
            .await
    }

    /// Structured call with auditable stage attribution and normal fallback.
    pub async fn structured_stage<T: DeserializeOwned>(
        &self,
        stage: &str,
        system: &str,
        user: &str,
        schema: Value,
    ) -> Result<T> {
        self.structured_with(stage, system, user, schema, true, false)
            .await
    }

    /// Bulk work is fail-fast: never duplicate a fan-out across providers.
    pub async fn structured_bulk<T: DeserializeOwned>(
        &self,
        stage: &str,
        system: &str,
        user: &str,
        schema: Value,
    ) -> Result<T> {
        self.structured_with(stage, system, user, schema, false, false)
            .await
    }

    /// Lightweight routing/classification on a backend-specific fast model.
    pub async fn structured_fast<T: DeserializeOwned>(
        &self,
        stage: &str,
        system: &str,
        user: &str,
        schema: Value,
    ) -> Result<T> {
        let allow_fallback = matches!(stage, "interactive.router" | "reply.triage");
        self.structured_with(stage, system, user, schema, allow_fallback, true)
            .await
    }

    /// High-volume structured work that must not duplicate itself across a
    /// fallback provider. OpenAI routes this to Luna; substantive generation
    /// remains on the selected Terra/Sol model.
    pub async fn structured_economy_bulk<T: DeserializeOwned>(
        &self,
        stage: &str,
        system: &str,
        user: &str,
        schema: Value,
    ) -> Result<T> {
        self.structured_with(stage, system, user, schema, false, true)
            .await
    }

    async fn structured_with<T: DeserializeOwned>(
        &self,
        stage: &str,
        system: &str,
        user: &str,
        schema: Value,
        allow_fallback: bool,
        fast: bool,
    ) -> Result<T> {
        let outcome = self
            .call(stage, system, user, Some(&schema), allow_fallback, fast)
            .await?;
        let structured = outcome.structured.ok_or_else(|| {
            anyhow!(
                "{} returned no structured output; text was: {}",
                self.backend(),
                outcome.result_text
            )
        })?;
        serde_json::from_value::<T>(structured)
            .context("deserializing structured_output into the expected type")
    }

    /// A plain-text completion (the `result` field of the envelope).
    #[allow(dead_code)]
    pub async fn text(&self, system: &str, user: &str) -> Result<String> {
        Ok(self
            .call("text", system, user, None, true, false)
            .await?
            .result_text)
    }

    /// A streaming `claude` call. Every semantic event is handed to `on_event`
    /// as it arrives; the assembled [`CallOutcome`] (final text, structured
    /// output, usage) is returned when the stream ends.
    #[allow(dead_code)]
    pub async fn stream(
        &self,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<CallOutcome> {
        self.stream_with(
            "interactive.router",
            system,
            user,
            schema,
            on_event,
            true,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn stream_with(
        &self,
        stage: &str,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
        allow_fallback: bool,
        fast: bool,
    ) -> Result<CallOutcome> {
        let selection = self.selection_for_stage(stage, fast);
        self.check_turn_budget()?;
        let first = if self
            .exhausted_backends
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .contains(&selection.backend)
        {
            Err(anyhow!(
                "{} usage exhausted earlier in this session; queued paid work was stopped locally",
                selection.backend
            ))
        } else {
            self.stream_once(stage, &selection, system, user, schema, on_event, false)
                .await
        };
        if first.as_ref().is_err_and(is_usage_exhausted) {
            self.exhausted_backends
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .insert(selection.backend);
        }
        match first {
            Err(error) if allow_fallback && is_usage_exhausted(&error) => {
                let fallback = if fast {
                    self.temporary_fallback(&selection, true)
                } else {
                    self.fallback_after(&selection)
                };
                self.check_turn_budget()?;
                self.stream_once(stage, &fallback, system, user, schema, on_event, true)
                    .await
                    .with_context(|| {
                        format!(
                            "{} usage exhausted; switched to {} ({}) but the retry failed",
                            selection.backend,
                            fallback.backend,
                            fallback.model.as_deref().unwrap_or("default")
                        )
                    })
            }
            Err(error) if is_usage_exhausted(&error) => Err(error).with_context(|| {
                format!(
                    "{stage}: usage exhausted; automatic cross-provider fallback is disabled for bulk work"
                )
            }),
            result => result,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn stream_once(
        &self,
        stage: &str,
        selection: &ModelSelection,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
        fallback: bool,
    ) -> Result<CallOutcome> {
        let prompt_chars = request_chars(system, user, schema);
        self.stats.record_attempt(stage, prompt_chars, fallback);
        let work = async {
            match selection.backend {
                Backend::Openai => {
                    let outcome = self
                        .call_openai(stage, selection, system, user, schema)
                        .await?;
                    if schema.is_some() {
                        on_event(StreamEvent::BlockStart("tool_use"));
                        on_event(StreamEvent::ToolUse {
                            name: "StructuredOutput",
                        });
                        on_event(StreamEvent::ToolInputDelta(&outcome.result_text));
                    } else {
                        on_event(StreamEvent::BlockStart("text"));
                        on_event(StreamEvent::TextDelta(&outcome.result_text));
                    }
                    on_event(StreamEvent::BlockStop);
                    Ok(outcome)
                }
                Backend::Claude => {
                    self.stream_claude(selection, system, user, schema, on_event)
                        .await
                }
                Backend::Grok => {
                    self.stream_grok(selection, system, user, schema, on_event)
                        .await
                }
                Backend::Codex => {
                    self.stream_codex(stage, selection, system, user, schema, on_event)
                        .await
                }
            }
        };
        let result = self
            .with_timeout(
                selection.backend,
                self.timeout_for(stage, selection),
                openai_uses_background(stage, selection),
                work,
            )
            .await;
        match &result {
            Ok(outcome) => self.stats.record_success(stage, outcome),
            Err(error) => {
                if let Some(failure) = error.downcast_ref::<BilledCallError>() {
                    self.stats.record_billed_failure(stage, &failure.usage);
                } else {
                    self.stats.record_failure(stage);
                }
            }
        }
        self.log_usage(stage, selection, prompt_chars, fallback, &result);
        result
    }

    async fn stream_claude(
        &self,
        selection: &ModelSelection,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<CallOutcome> {
        let schema_str = match schema {
            Some(s) => Some(serde_json::to_string(s).context("serializing JSON schema")?),
            None => None,
        };
        let mut cmd =
            self.claude_command(system, schema_str.as_deref(), selection.model.as_deref());
        cmd.arg("--output-format")
            .arg("stream-json")
            .arg("--verbose")
            .arg("--include-partial-messages");
        cmd.arg(cli_safe_arg(user).as_ref())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd
            .spawn()
            .context("failed to run `claude` (is Claude Code installed and on PATH?)")?;
        let stdout = child.stdout.take().expect("piped stdout");
        // Drain stderr concurrently so a chatty error can't deadlock the pipe.
        let stderr = child.stderr.take().expect("piped stderr");
        let stderr_task = tokio::spawn(async move {
            let mut buf = String::new();
            let _ = BufReader::new(stderr).read_to_string(&mut buf).await;
            buf
        });

        let mut lines = BufReader::new(stdout).lines();
        let mut outcome = CallOutcome::default();
        while let Some(line) = lines.next_line().await.context("reading claude stream")? {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(v) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            dispatch(&v, on_event, &mut outcome);
        }

        let status = child.wait().await.context("waiting on `claude`")?;
        let stderr_out = stderr_task.await.unwrap_or_default();
        if !status.success() {
            bail!(
                "claude CLI exited with {status}:\n{}\n{}",
                stderr_out.trim(),
                outcome.result_text.trim()
            );
        }
        if outcome.is_error {
            bail!("claude CLI reported an error: {}", outcome.result_text);
        }

        Ok(outcome)
    }

    /// Stream Grok via Anthropic-compatible `streaming-messages-json` lines.
    /// Semantic events share Claude's shape, so [`dispatch`] handles them.
    async fn stream_grok(
        &self,
        selection: &ModelSelection,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<CallOutcome> {
        let schema_str = match schema {
            Some(s) => Some(serde_json::to_string(s).context("serializing JSON schema")?),
            None => None,
        };
        let mut cmd = self.grok_command(
            system,
            schema_str.as_deref(),
            selection.model.as_deref(),
            selection.fast,
        );
        cmd.arg("--output-format")
            .arg("streaming-messages-json")
            .arg("--include-partial-messages")
            .arg("-p")
            .arg(cli_safe_arg(user).as_ref())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd
            .spawn()
            .context("failed to run `grok` (is the Grok CLI installed and on PATH?)")?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let stderr_task = tokio::spawn(async move {
            let mut buf = String::new();
            let _ = BufReader::new(stderr).read_to_string(&mut buf).await;
            buf
        });

        let structured_call = schema.is_some();
        let mut lines = BufReader::new(stdout).lines();
        let mut outcome = CallOutcome::default();
        while let Some(line) = lines.next_line().await.context("reading grok stream")? {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(v) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            dispatch(&v, on_event, &mut outcome);
        }

        let status = child.wait().await.context("waiting on `grok`")?;
        let stderr_out = stderr_task.await.unwrap_or_default();
        if !status.success() {
            bail!(
                "grok CLI exited with {status}:\n{}\n{}",
                stderr_out.trim(),
                outcome.result_text.trim()
            );
        }
        if outcome.is_error {
            bail!("grok CLI reported an error: {}", outcome.result_text);
        }
        if structured_call && outcome.structured.is_none() {
            outcome.structured = serde_json::from_str(outcome.result_text.trim()).ok();
        }

        Ok(outcome)
    }

    async fn stream_codex(
        &self,
        stage: &str,
        selection: &ModelSelection,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<CallOutcome> {
        let schema_file = schema.map(TempSchema::new).transpose()?;
        let instructions_file = TempInstructions::new(system)?;
        let effort = codex_reasoning_effort(stage, selection.fast);
        let mut cmd =
            self.codex_command(&instructions_file.path, selection.model.as_deref(), &effort);
        if let Some(file) = &schema_file {
            cmd.arg("--output-schema").arg(&file.path);
        }
        cmd.arg(cli_safe_arg(user).as_ref())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let started = Instant::now();
        let mut child = cmd
            .spawn()
            .context("failed to run `codex` (is Codex CLI installed and on PATH?)")?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let stderr_task = tokio::spawn(async move {
            let mut buf = String::new();
            let _ = BufReader::new(stderr).read_to_string(&mut buf).await;
            buf
        });

        let structured_call = schema.is_some();
        let mut lines = BufReader::new(stdout).lines();
        let mut outcome = CallOutcome::default();
        while let Some(line) = lines.next_line().await.context("reading codex stream")? {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            dispatch_codex(&value, structured_call, on_event, &mut outcome);
        }

        let status = child.wait().await.context("waiting on `codex`")?;
        let stderr_out = stderr_task.await.unwrap_or_default();
        outcome.duration_ms = started.elapsed().as_millis() as u64;
        if !status.success() {
            bail!(
                "codex CLI exited with {status}:\n{}\n{}",
                stderr_out.trim(),
                outcome.result_text.trim()
            );
        }
        if outcome.is_error {
            bail!("codex CLI reported an error: {}", outcome.result_text);
        }
        if structured_call && outcome.structured.is_none() {
            outcome.structured = serde_json::from_str(outcome.result_text.trim()).ok();
        }

        Ok(outcome)
    }

    /// Streaming structured call: render events live, return the typed result.
    #[allow(dead_code)]
    pub async fn structured_streamed<T: DeserializeOwned>(
        &self,
        system: &str,
        user: &str,
        schema: Value,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<T> {
        let outcome = self.stream(system, user, Some(&schema), on_event).await?;
        deserialize_structured(self.backend(), outcome)
    }

    /// Fast-model streaming variant used by the interactive action router.
    pub async fn structured_fast_streamed<T: DeserializeOwned>(
        &self,
        stage: &str,
        system: &str,
        user: &str,
        schema: Value,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<T> {
        let outcome = self
            .stream_with(stage, system, user, Some(&schema), on_event, true, true)
            .await?;
        deserialize_structured(self.backend(), outcome)
    }
}

fn default_openai_model() -> String {
    std::env::var("SPRUCE_OPENAI_MODEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "gpt-5.6-terra".to_string())
}

fn default_codex_model() -> String {
    std::env::var("SPRUCE_CODEX_MODEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "gpt-5.6-terra".to_string())
}

fn outreach_budget_floor(accounts: usize, recipients: usize) -> (u64, u64, f64) {
    let accounts = accounts.max(1) as u64;
    let recipients = recipients.max(1) as u64;
    (
        60 + accounts.saturating_mul(8) + recipients.saturating_mul(12),
        80_000 + accounts.saturating_mul(8_000) + recipients.saturating_mul(20_000),
        1.0 + accounts as f64 * 0.25 + recipients as f64 * 1.25,
    )
}

fn is_outreach_quality_stage(stage: &str) -> bool {
    matches!(
        stage,
        "outreach.write_account"
            | "outreach.review_edit"
            | "outreach.verify_final"
            | "outreach.eval_pairwise"
    )
}

fn is_outreach_strategy_stage(stage: &str) -> bool {
    matches!(
        stage,
        "source.qualify" | "source.refresh" | "source.vantage" | "outreach.plan"
    )
}

fn is_long_bulk_stage(stage: &str) -> bool {
    is_outreach_quality_stage(stage) || is_outreach_strategy_stage(stage)
}

fn openai_reasoning_effort(stage: &str, fast: bool) -> String {
    let (keys, default_effort): (&[&str], &str) = if fast {
        (&["SPRUCE_OPENAI_FAST_REASONING_EFFORT"], "none")
    } else if stage == "outreach.write_account" {
        (
            &[
                "SPRUCE_OPENAI_WRITER_REASONING_EFFORT",
                "SPRUCE_OPENAI_COPY_REASONING_EFFORT",
            ],
            "high",
        )
    } else if stage == "outreach.review_edit" {
        (
            &[
                "SPRUCE_OPENAI_EDITOR_REASONING_EFFORT",
                "SPRUCE_OPENAI_COPY_REASONING_EFFORT",
            ],
            "medium",
        )
    } else if matches!(stage, "outreach.verify_final" | "outreach.eval_pairwise") {
        (
            &[
                "SPRUCE_OPENAI_VERIFIER_REASONING_EFFORT",
                "SPRUCE_OPENAI_COPY_REASONING_EFFORT",
            ],
            "high",
        )
    } else if is_outreach_strategy_stage(stage) {
        (&["SPRUCE_OPENAI_STRATEGY_REASONING_EFFORT"], "medium")
    } else {
        (&["SPRUCE_OPENAI_REASONING_EFFORT"], "low")
    };
    keys.iter()
        .find_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| {
            matches!(
                value.as_str(),
                "none" | "low" | "medium" | "high" | "xhigh" | "max"
            )
        })
        .unwrap_or_else(|| default_effort.to_string())
}

fn codex_reasoning_effort(stage: &str, fast: bool) -> String {
    let (keys, default_effort): (&[&str], &str) = if fast {
        (&["SPRUCE_CODEX_FAST_REASONING_EFFORT"], "low")
    } else if stage == "outreach.write_account" {
        (
            &[
                "SPRUCE_CODEX_WRITER_REASONING_EFFORT",
                "SPRUCE_CODEX_COPY_REASONING_EFFORT",
            ],
            "medium",
        )
    } else if stage == "outreach.review_edit" {
        (
            &[
                "SPRUCE_CODEX_EDITOR_REASONING_EFFORT",
                "SPRUCE_CODEX_COPY_REASONING_EFFORT",
            ],
            "low",
        )
    } else if matches!(stage, "outreach.verify_final" | "outreach.eval_pairwise") {
        (
            &[
                "SPRUCE_CODEX_VERIFIER_REASONING_EFFORT",
                "SPRUCE_CODEX_COPY_REASONING_EFFORT",
            ],
            "medium",
        )
    } else if is_outreach_strategy_stage(stage) {
        (&["SPRUCE_CODEX_STRATEGY_REASONING_EFFORT"], "medium")
    } else {
        (&["SPRUCE_CODEX_REASONING_EFFORT"], "low")
    };
    keys.iter()
        .find_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| {
            matches!(
                value.as_str(),
                "minimal" | "low" | "medium" | "high" | "xhigh"
            )
        })
        .unwrap_or_else(|| default_effort.to_string())
}

/// Synchronous HTTP connections are a poor fit for long reasoning runs: an
/// intermediary can close an otherwise healthy request before the model
/// finishes. OpenAI background responses return an id immediately and let us
/// poll that same generation to completion. Operators can explicitly disable
/// this for a compatible proxy with SPRUCE_OPENAI_BACKGROUND_MODE=off.
fn openai_uses_background(stage: &str, selection: &ModelSelection) -> bool {
    if selection.backend != Backend::Openai || selection.fast {
        return false;
    }
    if let Ok(value) = std::env::var("SPRUCE_OPENAI_BACKGROUND_MODE") {
        return matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "on" | "yes"
        );
    }
    selection
        .model
        .as_deref()
        .is_some_and(|model| model.starts_with("gpt-5.6-sol"))
        || openai_reasoning_effort(stage, selection.fast) == "xhigh"
}

#[cfg(not(test))]
fn openai_background_poll_interval() -> Duration {
    Duration::from_secs(2)
}

#[cfg(test)]
fn openai_background_poll_interval() -> Duration {
    Duration::from_millis(1)
}

fn openai_request(
    stage: &str,
    selection: &ModelSelection,
    system: &str,
    user: &str,
    schema: Option<&Value>,
) -> Value {
    let model = selection
        .model
        .as_deref()
        .filter(|model| !model.trim().is_empty())
        .unwrap_or("gpt-5.6-terra");
    let effort = openai_reasoning_effort(stage, selection.fast);
    let max_output_tokens = std::env::var("SPRUCE_OPENAI_MAX_OUTPUT_TOKENS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value >= 256)
        .unwrap_or_else(|| openai_output_cap(stage));
    let service_tier = std::env::var("SPRUCE_OPENAI_SERVICE_TIER")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| matches!(value.as_str(), "default" | "flex" | "fast"))
        .unwrap_or_else(|| "default".to_string());

    let mut text = serde_json::json!({ "verbosity": "low" });
    if let Some(schema) = schema {
        let mut strict_schema = schema.clone();
        make_codex_schema_strict(&mut strict_schema);
        text["format"] = serde_json::json!({
            "type": "json_schema",
            "name": "spruce_output",
            "strict": true,
            "schema": strict_schema,
        });
    }

    let mut request = serde_json::json!({
        "model": model,
        "instructions": system,
        "input": user,
        "reasoning": { "effort": effort },
        "service_tier": service_tier,
        "max_output_tokens": max_output_tokens,
        "text": text,
        "store": false,
        // GPT-5.6 cache writes cost 1.25x. These prompts contain dynamic account
        // and recipient text, so disable the implicit latest-message breakpoint
        // until a measured reusable prefix justifies an explicit breakpoint.
        "prompt_cache_options": { "mode": "explicit" },
        "metadata": { "application": "spruce-leaf", "stage": stage },
    });
    if openai_uses_background(stage, selection) {
        request["background"] = Value::Bool(true);
    }
    request
}

fn openai_output_cap(stage: &str) -> u64 {
    match stage {
        "interactive.router" | "reply.triage" => 2_048,
        "source.icp"
        | "source.qualify"
        | "source.vantage"
        | "source.website_research"
        | "source.refresh"
        | "outreach.eval_pairwise" => 4_096,
        // Frontier reasoning tokens share the output allowance with strict
        // JSON. Planning and review regularly need more than 4k at xhigh even
        // though the visible document is small; truncating them throws away a
        // completed generation instead of improving copy quality.
        "outreach.plan" | "outreach.review_edit" | "outreach.verify_final" => 8_192,
        // Reasoning tokens count against this allowance. Seven complete touches
        // plus strict JSON need headroom after the model has reasoned; 6,144
        // repeatedly terminated valid Sol runs before the closing JSON.
        "outreach.write_account" => 12_288,
        _ => 16_384,
    }
}

fn parse_openai_response(
    payload: &Value,
    structured_call: bool,
    elapsed: Duration,
) -> Result<CallOutcome> {
    if let Some(error) = payload.get("error").filter(|error| !error.is_null()) {
        bail!(
            "OpenAI API error: {} (type: {}, code: {})",
            error["message"].as_str().unwrap_or("unknown error"),
            error["type"].as_str().unwrap_or("unknown"),
            error["code"].as_str().unwrap_or("unknown"),
        );
    }
    let input_tokens = payload["usage"]["input_tokens"].as_u64().unwrap_or(0);
    let cached_input_tokens = payload["usage"]["input_tokens_details"]["cached_tokens"]
        .as_u64()
        .unwrap_or(0);
    let cache_write_input_tokens = payload["usage"]["input_tokens_details"]["cache_write_tokens"]
        .as_u64()
        .unwrap_or(0);
    let output_tokens = payload["usage"]["output_tokens"].as_u64().unwrap_or(0);
    let model = payload["model"].as_str().unwrap_or_default();
    let service_tier = payload["service_tier"].as_str().unwrap_or("default");
    let usage = || CallOutcome {
        input_tokens,
        cached_input_tokens,
        cache_write_input_tokens,
        output_tokens,
        cost_usd: openai_cost(
            model,
            service_tier,
            input_tokens,
            cached_input_tokens,
            cache_write_input_tokens,
            output_tokens,
        ),
        service_tier: service_tier.to_string(),
        duration_ms: elapsed.as_millis() as u64,
        ..Default::default()
    };
    let status = payload["status"].as_str().unwrap_or("completed");
    if status != "completed" {
        let reason = payload["incomplete_details"]["reason"]
            .as_str()
            .unwrap_or("response did not complete");
        return Err(BilledCallError {
            message: format!("OpenAI response status was {status}: {reason}"),
            usage: usage(),
        }
        .into());
    }

    let mut text_parts = Vec::new();
    let mut refusals = Vec::new();
    for item in payload["output"].as_array().into_iter().flatten() {
        for content in item["content"].as_array().into_iter().flatten() {
            match content["type"].as_str() {
                Some("output_text") => {
                    if let Some(text) = content["text"].as_str() {
                        text_parts.push(text);
                    }
                }
                Some("refusal") => {
                    refusals.push(content["refusal"].as_str().unwrap_or("request refused"));
                }
                _ => {}
            }
        }
    }
    if !refusals.is_empty() {
        bail!("OpenAI refused the request: {}", refusals.join("; "));
    }
    let result_text = text_parts.join("\n");
    if result_text.trim().is_empty() {
        bail!("OpenAI response contained no output text");
    }
    let structured = if structured_call {
        let parsed = parse_first_json_value(&result_text)
            .context("parsing OpenAI structured output as JSON");
        match parsed {
            Ok(value) => Some(value),
            Err(error) => {
                return Err(BilledCallError {
                    message: format!("{error:#}"),
                    usage: usage(),
                }
                .into())
            }
        }
    } else {
        None
    };
    Ok(CallOutcome {
        result_text,
        structured,
        ..usage()
    })
}

/// Strict-schema providers should return one JSON document, but Responses can
/// occasionally emit the same completed object in two output-text parts. The
/// ordinary parser reports that as trailing characters. Accept the first full
/// object/array while still rejecting prose or a truncated first document.
fn parse_first_json_value(text: &str) -> Result<Value> {
    let trimmed = text.trim();
    match serde_json::from_str::<Value>(trimmed) {
        Ok(value) => Ok(value),
        Err(full_error) => {
            let mut documents = serde_json::Deserializer::from_str(trimmed).into_iter::<Value>();
            if let Some(Ok(value)) = documents.next() {
                return Ok(value);
            }
            Err(full_error.into())
        }
    }
}

/// Current GPT-5.6 token price calculation for direct Responses processing.
/// Unknown models intentionally report zero rather than fabricating a rate.
fn openai_cost(
    model: &str,
    service_tier: &str,
    input_tokens: u64,
    cached_tokens: u64,
    cache_write_tokens: u64,
    output_tokens: u64,
) -> f64 {
    let long = input_tokens > 272_000;
    let rates = if model == "gpt-5.6" || model.starts_with("gpt-5.6-sol") {
        if long {
            (10.0, 1.0, 12.5, 45.0)
        } else {
            (5.0, 0.5, 6.25, 30.0)
        }
    } else if model.starts_with("gpt-5.6-terra") {
        if long {
            (4.0, 0.4, 5.0, 18.0)
        } else {
            (2.0, 0.2, 2.5, 12.0)
        }
    } else if model.starts_with("gpt-5.6-luna") {
        if long {
            (0.4, 0.04, 0.5, 1.8)
        } else {
            (0.2, 0.02, 0.25, 1.2)
        }
    } else {
        return 0.0;
    };
    let uncached = input_tokens.saturating_sub(cached_tokens + cache_write_tokens);
    let standard = (uncached as f64 * rates.0
        + cached_tokens as f64 * rates.1
        + cache_write_tokens as f64 * rates.2
        + output_tokens as f64 * rates.3)
        / 1_000_000.0;
    match service_tier {
        "flex" => standard * 0.5,
        "fast" | "priority" => standard * 2.0,
        _ => standard,
    }
}

fn deserialize_structured<T: DeserializeOwned>(
    backend: Backend,
    outcome: CallOutcome,
) -> Result<T> {
    let structured = outcome.structured.ok_or_else(|| {
        anyhow!(
            "{} returned no structured output; text was: {}",
            backend,
            outcome.result_text
        )
    })?;
    serde_json::from_value::<T>(structured)
        .context("deserializing structured_output into the expected type")
}

/// Providers use human-readable quota errors, sometimes wrapped in JSON and
/// sometimes written to stderr. Keep this deliberately narrower than a
/// generic transient rate-limit check: only exhausted usage should change the
/// user's selected provider.
pub(crate) fn is_usage_exhausted(error: &anyhow::Error) -> bool {
    let message = error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        .to_ascii_lowercase();
    usage_exhausted_message(&message)
}

pub(crate) fn is_run_budget_exhausted(error: &anyhow::Error) -> bool {
    error
        .chain()
        .map(ToString::to_string)
        .any(|message| message.contains("Spruce per-turn"))
}

pub(crate) fn is_generation_incomplete(error: &anyhow::Error) -> bool {
    error.chain().map(ToString::to_string).any(|message| {
        let message = message.to_ascii_lowercase();
        message.contains("response status was incomplete") || message.contains("max_output_tokens")
    })
}

/// How many times to retry a transient CLI failure before giving up on a call.
const MAX_TRANSIENT_RETRIES: u32 = 2;

/// Poll failures target an already-created response id, so they can retry more
/// generously without creating duplicate generations or duplicate spend.
const MAX_BACKGROUND_POLL_RETRIES: u32 = 5;

/// A transient provider failure worth retrying: process/network overload, a
/// timeout, or a retryable API status — not a deterministic prompt/schema error
/// and not genuine usage exhaustion (which has cross-provider fallback).
pub(crate) fn is_retryable_provider_error(error: &anyhow::Error) -> bool {
    if is_usage_exhausted(error) {
        return false;
    }
    if error.downcast_ref::<OpenAiBackgroundPollError>().is_some() {
        return true;
    }
    let message = error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        .to_ascii_lowercase();
    [
        "exited with",
        "timed out after",
        "is hung or rate-limited",
        "provider did not complete",
        "failed to run",
        "sending openai responses api request",
        "openai api http 429",
        "openai api http 500",
        "openai api http 502",
        "openai api http 503",
        "openai api http 504",
        "connection reset",
        "broken pipe",
        "resource temporarily unavailable",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

/// Once OpenAI has returned a background response id, retrying the POST would
/// create a second paid generation. Such failures still count as provider
/// stops to outreach, but only pre-id transport errors may restart a request.
fn is_safe_to_retry_provider_request(error: &anyhow::Error) -> bool {
    is_retryable_provider_error(error)
        && error.downcast_ref::<OpenAiBackgroundPollError>().is_none()
}

fn transient_backoff(attempt: u32) -> std::time::Duration {
    // 2s then 5s — enough for the local CLI / rate window to recover between
    // tries without stalling the whole run.
    let secs = if attempt <= 1 { 2 } else { 5 };
    std::time::Duration::from_secs(secs)
}

fn request_chars(system: &str, user: &str, schema: Option<&Value>) -> u64 {
    let schema_chars = schema
        .and_then(|value| serde_json::to_string(value).ok())
        .map(|value| value.chars().count())
        .unwrap_or(0);
    (system.chars().count() + user.chars().count() + schema_chars) as u64
}

fn usage_exhausted_message(message: &str) -> bool {
    [
        "usage limit",
        "usage cap",
        "hit your limit",
        "hit your session limit",
        "reached your limit",
        "limit has been reached",
        "quota exceeded",
        "quota has been exceeded",
        "exceeded your current quota",
        "insufficient_quota",
        "insufficient quota",
        "out of extra usage",
        "out of credits",
        "credit balance is too low",
        "credits exhausted",
        "no credits remaining",
        "0 weighted tokens left",
        "zero weighted tokens left",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

/// A per-call schema file for `codex exec --output-schema`.
struct TempSchema {
    path: PathBuf,
}

/// Compact replacement instructions for one inference-only Codex subprocess.
struct TempInstructions {
    path: PathBuf,
}

impl TempInstructions {
    fn new(system: &str) -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "spruce-leaf-instructions-{}-{}.md",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let content = format!(
            "You are a pure inference backend embedded in spruce-leaf. Do not inspect files, run \
             commands, browse, call tools, or modify anything. Return only the requested answer.\n\n\
             APPLICATION INSTRUCTIONS:\n{}",
            cli_safe_arg(system)
        );
        std::fs::write(&path, content)
            .with_context(|| format!("writing temporary Codex instructions {}", path.display()))?;
        Ok(Self { path })
    }
}

impl TempSchema {
    fn new(schema: &Value) -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "spruce-leaf-schema-{}-{}.json",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let mut schema = schema.clone();
        make_codex_schema_strict(&mut schema);
        let bytes = serde_json::to_vec(&schema).context("serializing Codex output schema")?;
        std::fs::write(&path, bytes)
            .with_context(|| format!("writing temporary schema {}", path.display()))?;
        Ok(Self { path })
    }
}

/// Codex structured output follows the strict Responses API schema contract:
/// every declared object property must also appear in that object's `required`
/// array. Claude accepts optional properties, so normalize only the temporary
/// Codex copy and leave the application's provider-neutral schemas unchanged.
fn make_codex_schema_strict(schema: &mut Value) {
    match schema {
        Value::Object(object) => {
            if let Some(Value::Object(properties)) = object.get_mut("properties") {
                let required = properties.keys().cloned().map(Value::String).collect();
                for property in properties.values_mut() {
                    make_codex_schema_strict(property);
                }
                object.insert("required".to_string(), Value::Array(required));
                object.insert("additionalProperties".to_string(), Value::Bool(false));
            }
            for key in ["items", "additionalProperties"] {
                if let Some(child) = object.get_mut(key) {
                    make_codex_schema_strict(child);
                }
            }
            for key in ["anyOf", "allOf", "oneOf"] {
                if let Some(Value::Array(children)) = object.get_mut(key) {
                    for child in children {
                        make_codex_schema_strict(child);
                    }
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                make_codex_schema_strict(value);
            }
        }
        _ => {}
    }
}

impl Drop for TempSchema {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Drop for TempInstructions {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Translate one Codex `exec --json` event into the common stream vocabulary.
fn dispatch_codex(
    value: &Value,
    structured_call: bool,
    on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    outcome: &mut CallOutcome,
) {
    match value["type"].as_str() {
        Some("turn.started") => on_event(StreamEvent::BlockStart("thinking")),
        Some("item.completed") => {
            let item = &value["item"];
            let text = item["text"].as_str().unwrap_or_default();
            match item["type"].as_str() {
                Some("reasoning") if !text.is_empty() => {
                    on_event(StreamEvent::BlockStart("thinking"));
                    on_event(StreamEvent::ThinkingDelta(text));
                    on_event(StreamEvent::BlockStop);
                }
                Some("agent_message") => {
                    outcome.result_text = text.to_string();
                    if structured_call {
                        outcome.structured = serde_json::from_str(text.trim()).ok();
                        on_event(StreamEvent::BlockStart("tool_use"));
                        on_event(StreamEvent::ToolUse {
                            name: "StructuredOutput",
                        });
                        on_event(StreamEvent::ToolInputDelta(text));
                        on_event(StreamEvent::BlockStop);
                    } else {
                        on_event(StreamEvent::BlockStart("text"));
                        on_event(StreamEvent::TextDelta(text));
                        on_event(StreamEvent::BlockStop);
                    }
                }
                _ => {}
            }
        }
        Some("turn.completed") => {
            outcome.input_tokens = value["usage"]["input_tokens"].as_u64().unwrap_or(0);
            outcome.cached_input_tokens = value["usage"]["cached_input_tokens"]
                .as_u64()
                .or_else(|| value["usage"]["cache_read_input_tokens"].as_u64())
                .unwrap_or(0);
            outcome.output_tokens = value["usage"]["output_tokens"].as_u64().unwrap_or(0);
        }
        Some("turn.failed") | Some("error") => {
            outcome.is_error = true;
            let message = value["message"]
                .as_str()
                .or_else(|| value["error"]["message"].as_str())
                .unwrap_or("unknown Codex error");
            outcome.result_text = message.to_string();
        }
        _ => {}
    }
}

/// Translate one raw NDJSON line into [`StreamEvent`]s and/or outcome updates.
fn dispatch(
    v: &Value,
    on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    outcome: &mut CallOutcome,
) {
    match v["type"].as_str() {
        Some("stream_event") => {
            let event = &v["event"];
            match event["type"].as_str() {
                Some("message_start") => {
                    let usage = &event["message"]["usage"];
                    if let Some(n) = usage["input_tokens"].as_u64() {
                        outcome.input_tokens = n;
                    }
                    if let Some(n) = usage["cache_read_input_tokens"]
                        .as_u64()
                        .or_else(|| usage["cached_input_tokens"].as_u64())
                    {
                        outcome.cached_input_tokens = n;
                    }
                }
                Some("content_block_start") => {
                    let block = &event["content_block"];
                    if let Some(kind) = block["type"].as_str() {
                        on_event(StreamEvent::BlockStart(kind));
                        if kind == "tool_use" {
                            on_event(StreamEvent::ToolUse {
                                name: block["name"].as_str().unwrap_or("tool"),
                            });
                        }
                    }
                }
                Some("content_block_delta") => {
                    let delta = &event["delta"];
                    match delta["type"].as_str() {
                        Some("thinking_delta") => {
                            if let Some(t) = delta["thinking"].as_str() {
                                on_event(StreamEvent::ThinkingDelta(t));
                            }
                        }
                        Some("text_delta") => {
                            if let Some(t) = delta["text"].as_str() {
                                on_event(StreamEvent::TextDelta(t));
                            }
                        }
                        Some("input_json_delta") => {
                            if let Some(t) = delta["partial_json"].as_str() {
                                on_event(StreamEvent::ToolInputDelta(t));
                            }
                        }
                        _ => {}
                    }
                }
                Some("content_block_stop") => on_event(StreamEvent::BlockStop),
                Some("message_delta") => {
                    if let Some(n) = event["usage"]["input_tokens"].as_u64() {
                        outcome.input_tokens = n;
                    }
                    if let Some(n) = event["usage"]["cache_read_input_tokens"]
                        .as_u64()
                        .or_else(|| event["usage"]["cached_input_tokens"].as_u64())
                    {
                        outcome.cached_input_tokens = n;
                    }
                    if let Some(n) = event["usage"]["output_tokens"].as_u64() {
                        outcome.output_tokens = n;
                    }
                }
                _ => {}
            }
        }
        Some("result") => {
            outcome.is_error = v["is_error"].as_bool().unwrap_or(false);
            outcome.result_text = v["result"]
                .as_str()
                .or_else(|| v["text"].as_str())
                .unwrap_or_default()
                .to_string();
            // Claude + Grok stream use snake_case; Grok non-stream uses camelCase.
            outcome.structured = v
                .get("structured_output")
                .or_else(|| v.get("structuredOutput"))
                .filter(|x| !x.is_null())
                .cloned();
            outcome.cost_usd = v["total_cost_usd"].as_f64().unwrap_or(0.0);
            outcome.duration_ms = v["duration_ms"].as_u64().unwrap_or(0);
            if let Some(n) = v["usage"]["input_tokens"].as_u64() {
                outcome.input_tokens = n;
            }
            if let Some(n) = v["usage"]["cache_read_input_tokens"]
                .as_u64()
                .or_else(|| v["usage"]["cached_input_tokens"].as_u64())
            {
                outcome.cached_input_tokens = n;
            }
            if let Some(n) = v["usage"]["output_tokens"].as_u64() {
                outcome.output_tokens = n;
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cli_safe_arg, codex_reasoning_effort, dispatch, dispatch_codex, is_generation_incomplete,
        is_retryable_provider_error, is_run_budget_exhausted, is_safe_to_retry_provider_request,
        make_codex_schema_strict, openai_cost, openai_request, parse_openai_response,
        usage_exhausted_message, Backend, CallOutcome, Engine, ModelSelection,
        OpenAiBackgroundPollError, Stats, StreamEvent,
    };
    use serde_json::json;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn local_cli_arguments_normalize_nul_bytes_from_untrusted_page_text() {
        assert_eq!(cli_safe_arg("ordinary prompt"), "ordinary prompt");
        assert_eq!(cli_safe_arg("before\0after"), "before\u{FFFD}after");
    }

    #[test]
    fn transient_provider_errors_retry_but_deterministic_and_quota_do_not() {
        // A bare non-zero CLI exit, timeout, or pre-response API transport
        // failure is transient and should be retried.
        assert!(is_retryable_provider_error(&anyhow::anyhow!(
            "claude CLI exited with exit status: 1:"
        )));
        assert!(is_retryable_provider_error(&anyhow::anyhow!(
            "interactive.router call timed out after 240s — the CLI is hung or rate-limited"
        )));
        assert!(is_retryable_provider_error(&anyhow::anyhow!(
            "writing focused copy: sending OpenAI Responses API request"
        )));
        // Genuine usage exhaustion has its own cross-provider fallback, not this retry.
        assert!(!is_retryable_provider_error(&anyhow::anyhow!(
            "claude CLI exited with 1: usage limit reached"
        )));
        // Deterministic failures would fail identically, so they must not retry.
        assert!(!is_retryable_provider_error(&anyhow::anyhow!(
            "writer did not cite any retrieved business-knowledge principle"
        )));
        assert!(!is_retryable_provider_error(&anyhow::anyhow!(
            "parsing claude CLI output as JSON"
        )));

        let background_poll: anyhow::Error =
            OpenAiBackgroundPollError("poll connection reset".into()).into();
        assert!(is_retryable_provider_error(&background_poll));
        assert!(!is_safe_to_retry_provider_request(&background_poll));
    }

    #[test]
    fn dispatches_partial_structured_input() {
        let event = json!({
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": "{\"plan\":\"I will inspect"
                }
            }
        });
        let mut chunks = Vec::new();
        let mut outcome = CallOutcome::default();

        dispatch(
            &event,
            &mut |event| {
                if let StreamEvent::ToolInputDelta(chunk) = event {
                    chunks.push(chunk.to_string());
                }
            },
            &mut outcome,
        );

        assert_eq!(chunks, vec![r#"{"plan":"I will inspect"#]);
    }

    #[test]
    fn openai_request_uses_responses_structured_output_and_explicit_effort() {
        let selection = ModelSelection {
            backend: Backend::Openai,
            model: Some("gpt-5.6-terra".into()),
            generation: 0,
            fast: false,
        };
        let request = openai_request(
            "outreach.write_account",
            &selection,
            "system rules",
            "write copy",
            Some(&json!({
                "type": "object",
                "properties": { "answer": { "type": "string" } }
            })),
        );

        assert_eq!(request["model"], "gpt-5.6-terra");
        assert_eq!(request["reasoning"]["effort"], "high");
        assert_eq!(request["service_tier"], "default");
        assert_eq!(request["max_output_tokens"], 12_288);
        assert_eq!(request["prompt_cache_options"]["mode"], "explicit");
        assert!(request.get("prompt_cache_key").is_none());
        assert_eq!(request["store"], false);
        assert!(request.get("background").is_none());
        assert_eq!(request["text"]["format"]["type"], "json_schema");
        assert_eq!(request["text"]["format"]["strict"], true);
        assert_eq!(
            request["text"]["format"]["schema"]["required"],
            json!(["answer"])
        );
        assert_eq!(
            request["text"]["format"]["schema"]["additionalProperties"],
            false
        );
    }

    #[test]
    fn openai_response_parses_structured_text_usage_and_current_cost() {
        let payload = json!({
            "status": "completed",
            "model": "gpt-5.6-terra",
            "service_tier": "default",
            "output": [{
                "type": "message",
                "content": [{"type": "output_text", "text": "{\"answer\":\"yes\"}"}]
            }],
            "usage": {
                "input_tokens": 1000,
                "input_tokens_details": {"cached_tokens": 300, "cache_write_tokens": 100},
                "output_tokens": 200
            }
        });
        let outcome = parse_openai_response(&payload, true, Duration::from_millis(42))
            .expect("parse response");

        assert_eq!(outcome.structured, Some(json!({"answer": "yes"})));
        assert_eq!(outcome.cached_input_tokens, 300);
        assert_eq!(outcome.cache_write_input_tokens, 100);
        assert_eq!(outcome.duration_ms, 42);
        assert!((outcome.cost_usd - 0.00391).abs() < 0.0000001);
    }

    #[test]
    fn openai_structured_response_recovers_a_valid_object_before_trailing_output() {
        let payload = json!({
            "status": "completed",
            "model": "gpt-5.6-terra",
            "service_tier": "default",
            "output": [{
                "type": "message",
                "content": [{
                    "type": "output_text",
                    "text": "{\"answer\":\"yes\"}\n{\"answer\":\"duplicate\"}"
                }]
            }],
            "usage": {"input_tokens": 100, "output_tokens": 20}
        });

        let outcome = parse_openai_response(&payload, true, Duration::from_millis(1))
            .expect("recover first complete structured document");

        assert_eq!(outcome.structured, Some(json!({"answer": "yes"})));
    }

    #[test]
    fn malformed_openai_structured_output_retains_billed_usage() {
        let payload = json!({
            "status": "completed",
            "model": "gpt-5.6-terra",
            "service_tier": "default",
            "output": [{
                "type": "message",
                "content": [{"type": "output_text", "text": "not json"}]
            }],
            "usage": {"input_tokens": 321, "output_tokens": 12}
        });

        let error = parse_openai_response(&payload, true, Duration::from_millis(1))
            .expect_err("invalid structured output must remain an error");
        let billed = error
            .downcast_ref::<super::BilledCallError>()
            .expect("parse error retains provider usage");
        assert_eq!(billed.usage.input_tokens, 321);
        assert_eq!(billed.usage.output_tokens, 12);
    }

    #[test]
    fn incomplete_openai_response_preserves_billed_usage() {
        let payload = json!({
            "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "model": "gpt-5.6-terra",
            "service_tier": "default",
            "usage": {
                "input_tokens": 5000,
                "input_tokens_details": {"cached_tokens": 1000, "cache_write_tokens": 0},
                "output_tokens": 4096
            }
        });
        let error = parse_openai_response(&payload, true, Duration::from_millis(42))
            .expect_err("incomplete response must not become copy");
        let billed = error
            .downcast_ref::<super::BilledCallError>()
            .expect("billed error metadata");

        assert!(is_generation_incomplete(&error));
        assert_eq!(billed.usage.input_tokens, 5000);
        assert_eq!(billed.usage.output_tokens, 4096);
        assert!(billed.usage.cost_usd > 0.0);
    }

    #[tokio::test]
    async fn openai_backend_posts_a_responses_request_and_parses_the_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock API");
        let address = listener.local_addr().expect("mock address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let mut request = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let read = stream.read(&mut chunk).await.expect("read request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .starts_with("content-length:")
                            .then(|| line.split_once(':')?.1.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
            let request_text = String::from_utf8_lossy(&request);
            assert!(request_text.starts_with("POST /v1/responses HTTP/1.1"));
            assert!(request_text
                .to_ascii_lowercase()
                .contains("authorization: bearer test-key"));
            assert!(request_text.contains("gpt-5.6-terra"));

            let body = json!({
                "status": "completed",
                "model": "gpt-5.6-terra",
                "output": [{
                    "type": "message",
                    "content": [{"type": "output_text", "text": "{\"ok\":true}"}]
                }],
                "usage": {
                    "input_tokens": 20,
                    "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                    "output_tokens": 5
                }
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });

        let mut engine = Engine::new(Backend::Openai, Some("gpt-5.6-terra".into()));
        engine.openai_api_key = Some("test-key".into());
        engine.openai_base_url = format!("http://{address}/v1");
        let outcome = engine
            .call_openai(
                "test.responses",
                &engine.selection(),
                "Return the schema.",
                "Go",
                Some(&json!({
                    "type": "object",
                    "properties": {"ok": {"type": "boolean"}}
                })),
            )
            .await
            .expect("OpenAI request succeeds");
        server.await.expect("mock server task");
        assert_eq!(outcome.structured, Some(json!({"ok": true})));
    }

    #[tokio::test]
    async fn openai_frontier_writer_creates_once_then_polls_the_same_background_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock API");
        let address = listener.local_addr().expect("mock address");
        let server = tokio::spawn(async move {
            for request_number in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("accept request");
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let read = stream.read(&mut chunk).await.expect("read request");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n")
                    else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .starts_with("content-length:")
                                .then(|| line.split_once(':')?.1.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if request.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
                let request_text = String::from_utf8_lossy(&request);
                let body =
                    if request_number == 0 {
                        assert!(request_text.starts_with("POST /v1/responses HTTP/1.1"));
                        assert!(request_text.contains("\"background\":true"));
                        json!({
                            "id": "resp_spruce_test",
                            "status": "queued",
                            "model": "gpt-5.6-sol"
                        })
                    } else {
                        assert!(
                            request_text.starts_with("GET /v1/responses/resp_spruce_test HTTP/1.1")
                        );
                        json!({
                            "id": "resp_spruce_test",
                            "status": "completed",
                            "model": "gpt-5.6-sol",
                            "service_tier": "default",
                            "output": [{
                                "type": "message",
                                "content": [{"type": "output_text", "text": "{\"ok\":true}"}]
                            }],
                            "usage": {"input_tokens": 20, "output_tokens": 5}
                        })
                    }
                    .to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
            }
        });

        let mut engine = Engine::new(Backend::Openai, Some("gpt-5.6-terra".into()));
        engine.openai_api_key = Some("test-key".into());
        engine.openai_base_url = format!("http://{address}/v1");
        let selection = ModelSelection {
            backend: Backend::Openai,
            model: Some("gpt-5.6-sol".into()),
            generation: 0,
            fast: false,
        };
        let outcome = engine
            .call_openai(
                "outreach.write_account",
                &selection,
                "Return the schema.",
                "Go",
                Some(&json!({
                    "type": "object",
                    "properties": {"ok": {"type": "boolean"}}
                })),
            )
            .await
            .expect("background OpenAI request succeeds");
        server.await.expect("mock server task");
        assert_eq!(outcome.structured, Some(json!({"ok": true})));
    }

    #[tokio::test]
    async fn background_timeout_is_a_provider_stop_but_never_restarts_the_post() {
        let engine = Engine::new(Backend::Openai, Some("gpt-5.6-terra".into()));
        let error = engine
            .with_timeout(Backend::Openai, Duration::from_millis(1), true, async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok::<(), anyhow::Error>(())
            })
            .await
            .expect_err("background call should hit the test timeout");

        assert!(is_retryable_provider_error(&error));
        assert!(!is_safe_to_retry_provider_request(&error));
        assert!(format!("{error:#}").contains("existing generation was not restarted"));
    }

    #[test]
    fn gpt_56_price_roles_are_not_accidentally_flattened() {
        let terra = openai_cost("gpt-5.6-terra", "default", 100_000, 0, 0, 100_000);
        let luna = openai_cost("gpt-5.6-luna", "default", 100_000, 0, 0, 100_000);
        let sol = openai_cost("gpt-5.6-sol", "default", 100_000, 0, 0, 100_000);
        assert_eq!(terra, 1.4);
        assert_eq!(luna, 0.14);
        assert_eq!(sol, 3.5);
        assert_eq!(
            openai_cost("gpt-5.6-terra", "flex", 100_000, 0, 0, 100_000),
            terra * 0.5
        );
        assert_eq!(
            openai_cost("gpt-5.6-terra", "priority", 100_000, 0, 0, 100_000),
            terra * 2.0
        );
    }

    #[test]
    fn dispatches_codex_structured_message_and_usage() {
        let started = json!({ "type": "turn.started" });
        let message = json!({
            "type": "item.completed",
            "item": {
                "type": "agent_message",
                "text": "{\"plan\":\"Check the CRM\",\"action\":\"list_accounts\",\"reply\":\"\"}"
            }
        });
        let usage = json!({
            "type": "turn.completed",
            "usage": { "input_tokens": 120, "cached_input_tokens": 80, "output_tokens": 42 }
        });
        let mut chunks = Vec::new();
        let mut thinking_started = false;
        let mut outcome = CallOutcome::default();

        dispatch_codex(
            &started,
            true,
            &mut |event| {
                thinking_started = matches!(event, StreamEvent::BlockStart("thinking"));
            },
            &mut outcome,
        );
        dispatch_codex(
            &message,
            true,
            &mut |event| {
                if let StreamEvent::ToolInputDelta(chunk) = event {
                    chunks.push(chunk.to_string());
                }
            },
            &mut outcome,
        );
        dispatch_codex(&usage, true, &mut |_| {}, &mut outcome);

        assert_eq!(
            outcome.structured.as_ref().unwrap()["action"],
            "list_accounts"
        );
        assert_eq!(outcome.output_tokens, 42);
        assert_eq!(outcome.input_tokens, 120);
        assert_eq!(outcome.cached_input_tokens, 80);
        assert_eq!(chunks.len(), 1);
        assert!(thinking_started);
    }

    #[test]
    fn codex_schema_normalization_requires_every_nested_property() {
        let mut schema = json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": { "type": "string" },
                "accounts": { "type": "integer" },
                "nested": {
                    "type": "object",
                    "properties": {
                        "optional": { "type": "boolean" }
                    }
                }
            }
        });

        make_codex_schema_strict(&mut schema);

        assert_eq!(schema["required"], json!(["accounts", "action", "nested"]));
        assert_eq!(
            schema["properties"]["nested"]["required"],
            json!(["optional"])
        );
    }

    #[test]
    fn recognizes_usage_exhaustion_but_not_transient_errors() {
        assert!(usage_exhausted_message(
            "You've hit your usage limit. Try again after the reset."
        ));
        assert!(usage_exhausted_message("Error code: insufficient_quota"));
        assert!(usage_exhausted_message("You're out of extra usage"));
        assert!(usage_exhausted_message(
            "You've hit your session limit · resets 11:30pm"
        ));
        assert!(!usage_exhausted_message("HTTP 429: too many requests"));
        assert!(!usage_exhausted_message("connection reset by peer"));
    }

    #[test]
    fn usage_ledger_counts_attempts_failures_and_every_token_class_by_stage() {
        let stats = Stats::default();
        stats.record_attempt("outreach.write", 4_000, false);
        stats.record_success(
            "outreach.write",
            &CallOutcome {
                input_tokens: 1_000,
                cached_input_tokens: 600,
                output_tokens: 250,
                cost_usd: 0.01,
                ..Default::default()
            },
        );
        stats.record_attempt("outreach.write", 4_000, true);
        stats.record_failure("outreach.write");

        let total = stats.snapshot();
        assert_eq!(total.attempts, 2);
        assert_eq!(total.calls, 1);
        assert_eq!(total.failures, 1);
        assert_eq!(total.fallback_attempts, 1);
        assert_eq!(total.input_tokens, 1_000);
        assert_eq!(total.cached_input_tokens, 600);
        assert_eq!(total.output_tokens, 250);
        let stage = stats.stage_snapshot()["outreach.write"];
        assert_eq!(stage.prompt_chars, 8_000);
        assert_eq!(stage.failures, 1);
    }

    #[test]
    fn fallback_switches_once_for_concurrent_failures() {
        let engine = Engine::new(Backend::Claude, Some("sonnet".to_string()));
        let failed = engine.selection();

        let first = engine.fallback_after(&failed);
        let second = engine.fallback_after(&failed);

        assert_eq!(first.backend, Backend::Codex);
        assert_eq!(second.backend, Backend::Codex);
        assert_eq!(engine.backend(), Backend::Codex);
        assert_eq!(engine.take_model_switches().len(), 1);
    }

    #[test]
    fn fallback_is_bidirectional() {
        let engine = Engine::new(Backend::Codex, None);
        let failed = engine.selection();

        let fallback = engine.fallback_after(&failed);

        assert_eq!(fallback.backend, Backend::Claude);
        assert_eq!(engine.backend(), Backend::Claude);
    }

    #[test]
    fn grok_falls_back_to_claude() {
        let engine = Engine::new(Backend::Grok, Some("grok-4.5".to_string()));
        let failed = engine.selection();
        let fallback = engine.fallback_after(&failed);
        assert_eq!(fallback.backend, Backend::Claude);
        assert_eq!(engine.backend(), Backend::Claude);
    }

    #[test]
    fn openai_falls_back_to_claude_on_exhausted_usage() {
        let engine = Engine::new(Backend::Openai, Some("gpt-5.6-terra".to_string()));
        let failed = engine.selection();
        let fallback = engine.fallback_after(&failed);
        assert_eq!(fallback.backend, Backend::Claude);
        assert_eq!(engine.backend(), Backend::Claude);
    }

    #[test]
    fn openai_economy_selection_uses_luna_without_changing_terra_default() {
        let engine = Engine::new(Backend::Openai, Some("gpt-5.6-terra".to_string()));
        let economy = engine.selection_for(true);
        assert_eq!(economy.model.as_deref(), Some("gpt-5.6-luna"));
        assert!(economy.fast);
        assert_eq!(engine.model_label(), "gpt-5.6-terra");
    }

    #[test]
    fn codex_economy_selection_pins_models_and_stage_effort() {
        let engine = Engine::new(Backend::Codex, None);
        assert_eq!(engine.model_label(), "gpt-5.6-terra");

        let economy = engine.selection_for(true);
        assert_eq!(economy.model.as_deref(), Some("gpt-5.6-luna"));
        assert!(economy.fast);

        assert_eq!(
            codex_reasoning_effort("source.website_research", true),
            "low"
        );
        assert_eq!(
            codex_reasoning_effort("source.website_research", false),
            "low"
        );
        assert_eq!(codex_reasoning_effort("source.qualify", false), "medium");
        assert_eq!(
            codex_reasoning_effort("outreach.write_account", false),
            "medium"
        );
        assert_eq!(codex_reasoning_effort("outreach.review_edit", false), "low");
        assert_eq!(
            codex_reasoning_effort("outreach.verify_final", false),
            "medium"
        );
    }

    #[test]
    fn outreach_writer_uses_the_frontier_lane_without_changing_other_stages() {
        let engine = Engine::new(Backend::Openai, Some("gpt-5.6-terra".to_string()));
        let quality = engine.selection_for_stage("outreach.write_account", false);
        assert_eq!(quality.model.as_deref(), Some("gpt-5.6-sol"));
        assert!(!quality.fast);

        let ordinary = engine.selection_for_stage("source.website_research", false);
        assert_eq!(ordinary.model.as_deref(), Some("gpt-5.6-terra"));

        let strategy = engine.selection_for_stage("source.refresh", false);
        assert_eq!(strategy.model.as_deref(), Some("gpt-5.6-terra"));

        let fallback = engine
            .transient_stage_fallback("outreach.write_account", &quality)
            .expect("frontier writer has a bounded normal-model fallback");
        assert_eq!(fallback.model.as_deref(), Some("gpt-5.6-terra"));
        assert_eq!(engine.model_label(), "gpt-5.6-terra");

        let sol_selected = Engine::new(Backend::Openai, Some("gpt-5.6-sol".to_string()));
        let failed = sol_selected.selection_for_stage("outreach.write_account", false);
        let fallback = sol_selected
            .transient_stage_fallback("outreach.write_account", &failed)
            .expect("an explicitly selected Sol writer still has a Terra fallback");
        assert_eq!(fallback.model.as_deref(), Some("gpt-5.6-terra"));
        assert_eq!(sol_selected.model_label(), "gpt-5.6-sol");
    }

    #[test]
    fn fast_fallback_does_not_mutate_the_bulk_provider() {
        let engine = Engine::new(Backend::Openai, Some("gpt-5.6-terra".to_string()));
        let failed = engine.selection_for(true);
        let fallback = engine.temporary_fallback(&failed, true);

        assert_eq!(fallback.backend, Backend::Claude);
        assert_eq!(fallback.model.as_deref(), Some("haiku"));
        assert_eq!(engine.backend(), Backend::Openai);
        assert!(engine.take_model_switches().is_empty());
    }

    #[test]
    fn turn_budget_stops_new_calls_after_the_cost_ceiling() {
        let engine = Engine::new(Backend::Openai, Some("gpt-5.6-terra".to_string()));
        let base = engine.stats.snapshot();
        *engine
            .turn_budget
            .lock()
            .unwrap_or_else(|lock| lock.into_inner()) = Some(super::TurnBudget {
            base,
            max_attempts: 80,
            max_output_tokens: 80_000,
            max_cost_usd: 0.01,
        });
        engine.stats.record_success(
            "test",
            &CallOutcome {
                cost_usd: 0.02,
                ..Default::default()
            },
        );

        let error = engine.check_turn_budget().expect_err("budget should stop");
        assert!(is_run_budget_exhausted(&error));
    }

    #[test]
    fn outreach_budget_floor_scales_with_requested_recipient_count() {
        let one = super::outreach_budget_floor(1, 1);
        let twenty_five = super::outreach_budget_floor(5, 25);
        assert!(twenty_five.0 > one.0);
        assert!(twenty_five.1 > one.1);
        assert!(twenty_five.2 > one.2);
        assert!(twenty_five.0 >= 400);
        assert!(twenty_five.2 >= 30.0);
    }

    #[test]
    fn manual_switch_preserves_each_provider_model() {
        let engine = Engine::new(Backend::Claude, Some("sonnet".to_string()));
        engine.select_model(Backend::Codex, Some("gpt-5.4".to_string()));
        assert_eq!(engine.model_label(), "gpt-5.4");

        engine.select_backend(Backend::Claude);
        assert_eq!(engine.model_label(), "sonnet");

        engine.select_model(Backend::Grok, Some("grok-4.5".to_string()));
        assert_eq!(engine.model_label(), "grok-4.5");

        engine.select_backend(Backend::Claude);
        assert_eq!(engine.model_label(), "sonnet");

        engine.select_model(Backend::Claude, None);
        assert_eq!(engine.model_label(), "default");

        engine.select_model(Backend::Openai, Some("gpt-5.6-sol".to_string()));
        assert_eq!(engine.model_label(), "gpt-5.6-sol");
        engine.select_model(Backend::Openai, None);
        assert_eq!(engine.model_label(), "gpt-5.6-terra");
    }
}
