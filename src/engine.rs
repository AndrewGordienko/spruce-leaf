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

use std::collections::BTreeMap;
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

/// Which provider supplies model inference.
#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
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
#[derive(Default)]
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
        ModelSelection {
            backend: self.backend,
            model: match self.backend {
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
        Self {
            models: RwLock::new(ModelState {
                backend,
                openai_model: Some(if backend == Backend::Openai {
                    model.clone().unwrap_or(openai_default)
                } else {
                    openai_default
                }),
                codex_model: if backend == Backend::Codex {
                    model.clone()
                } else {
                    None
                },
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

    pub fn backend(&self) -> Backend {
        self.selection().backend
    }

    pub fn model_label(&self) -> String {
        self.selection()
            .model
            .unwrap_or_else(|| "default".to_string())
    }

    /// Direct API inference uses a deliberately lean outreach topology: one
    /// account writer plus one bounded review per recipient.
    pub fn prefers_lean_outreach(&self) -> bool {
        self.backend() == Backend::Openai
    }

    /// Select a provider while preserving its last model override.
    pub fn select_backend(&self, backend: Backend) {
        let mut state = self.models.write().unwrap_or_else(|lock| lock.into_inner());
        if state.backend != backend {
            state.backend = backend;
            state.generation = state.generation.wrapping_add(1);
        }
    }

    /// Select a provider and set (or clear) its model override.
    pub fn select_model(&self, backend: Backend, model: Option<String>) {
        let model = if backend == Backend::Openai {
            Some(model.unwrap_or_else(default_openai_model))
        } else {
            model
        };
        let mut state = self.models.write().unwrap_or_else(|lock| lock.into_inner());
        let changed = state.backend != backend || *state.model_mut(backend) != model;
        *state.model_mut(backend) = model;
        state.backend = backend;
        if changed {
            state.generation = state.generation.wrapping_add(1);
        }
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
        if !fast {
            return selection;
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
            .or_else(|| (selection.backend == Backend::Claude).then(|| "haiku".to_string()))
            .or(selection.model);
        selection
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
        let (status, input, cached, cache_write, output, cost, error_kind, error) = match result {
            Ok(outcome) => (
                "completed",
                outcome.input_tokens,
                outcome.cached_input_tokens,
                outcome.cache_write_input_tokens,
                outcome.output_tokens,
                outcome.cost_usd,
                "",
                String::new(),
            ),
            Err(error) => {
                let error_kind = if is_usage_exhausted(error) {
                    "usage_exhausted"
                } else {
                    "provider_error"
                };
                (
                    "failed",
                    0,
                    0,
                    0,
                    0,
                    0.0,
                    error_kind,
                    error
                        .to_string()
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
            "service_tier": result.as_ref().ok().map(|outcome| outcome.service_tier.as_str()).unwrap_or(""),
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
            .arg(system);
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

    /// Codex runs as an inference-only subprocess. App instructions are passed
    /// at developer priority; the task itself remains the user message.
    fn codex_command(&self, system: &str, model: Option<&str>, fast: bool) -> Command {
        let developer = format!(
            "You are a pure inference backend embedded in spruce-leaf. Do not inspect files, run \
             commands, browse, call tools, or modify anything. Return only the requested answer.\n\n\
             APPLICATION INSTRUCTIONS:\n{system}"
        );
        let developer_toml = toml::Value::String(developer).to_string();

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
            .arg(format!("developer_instructions={developer_toml}"))
            .arg("--config")
            .arg("include_permissions_instructions=false")
            .arg("--config")
            .arg("include_apps_instructions=false")
            .arg("--config")
            .arg("include_collaboration_mode_instructions=false")
            .arg("--config")
            .arg("include_environment_context=false");
        if fast {
            cmd.arg("--config").arg("model_reasoning_effort=\"low\"");
        }
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
            .arg(system)
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
        cmd.arg(user).stdin(std::process::Stdio::null());

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
            .arg(user)
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
        parse_openai_response(&payload, schema.is_some(), started.elapsed())
    }

    /// Run provider work under the configured wall-clock cap. CLI children are
    /// killed on drop; HTTP request futures are cancelled.
    async fn with_timeout<T>(
        &self,
        backend: Backend,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        match tokio::time::timeout(self.call_timeout, fut).await {
            Ok(res) => res,
            Err(_) => bail!(
                "{} call timed out after {}s — the provider did not complete \
                 (raise SPRUCE_MODEL_TIMEOUT_SECS to allow longer)",
                backend.as_str(),
                self.call_timeout.as_secs()
            ),
        }
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
                    self.stream_codex(selection, system, user, schema, &mut ignore)
                        .await
                }
            }
        };
        let result = self.with_timeout(selection.backend, work).await;
        match &result {
            Ok(outcome) => self.stats.record_success(stage, outcome),
            Err(_) => self.stats.record_failure(stage),
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
        let selection = self.selection_for(fast);
        // Transient CLI failures (a bare non-zero exit or a timeout — usually the
        // local CLI choking under concurrent load, not a bad prompt) are retried a
        // few times with backoff before we give up on the call. Deterministic
        // failures (schema/parse) and genuine usage exhaustion are handled below,
        // not retried here — they would fail identically.
        let mut attempt = 0u32;
        let first = loop {
            let outcome = self
                .call_once(stage, &selection, system, user, schema, false)
                .await;
            match &outcome {
                Err(error) if attempt < MAX_TRANSIENT_RETRIES && is_transient_cli_error(error) => {
                    attempt += 1;
                    tokio::time::sleep(transient_backoff(attempt)).await;
                    continue;
                }
                _ => break outcome,
            }
        };
        match first {
            Err(error) if allow_fallback && is_usage_exhausted(&error) => {
                let mut fallback = self.fallback_after(&selection);
                if fast {
                    fallback = self.selection_for(true);
                }
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
        self.structured_with(stage, system, user, schema, true, true)
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
        let selection = self.selection_for(fast);
        match self
            .stream_once(stage, &selection, system, user, schema, on_event, false)
            .await
        {
            Err(error) if allow_fallback && is_usage_exhausted(&error) => {
                let mut fallback = self.fallback_after(&selection);
                if fast {
                    fallback = self.selection_for(true);
                }
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
                    self.stream_codex(selection, system, user, schema, on_event)
                        .await
                }
            }
        };
        let result = self.with_timeout(selection.backend, work).await;
        match &result {
            Ok(outcome) => self.stats.record_success(stage, outcome),
            Err(_) => self.stats.record_failure(stage),
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
        cmd.arg(user)
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
            .arg(user)
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
        selection: &ModelSelection,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<CallOutcome> {
        let schema_file = schema.map(TempSchema::new).transpose()?;
        let mut cmd = self.codex_command(system, selection.model.as_deref(), selection.fast);
        if let Some(file) = &schema_file {
            cmd.arg("--output-schema").arg(&file.path);
        }
        cmd.arg(user)
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
    let effort_key = if selection.fast {
        "SPRUCE_OPENAI_FAST_REASONING_EFFORT"
    } else {
        "SPRUCE_OPENAI_REASONING_EFFORT"
    };
    let default_effort = if selection.fast { "none" } else { "low" };
    let effort = std::env::var(effort_key)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| {
            matches!(
                value.as_str(),
                "none" | "low" | "medium" | "high" | "xhigh" | "max"
            )
        })
        .unwrap_or_else(|| default_effort.to_string());
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

    serde_json::json!({
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
    })
}

fn openai_output_cap(stage: &str) -> u64 {
    match stage {
        "interactive.router" | "reply.triage" => 2_048,
        "source.icp"
        | "source.qualify"
        | "source.vantage"
        | "source.website_research"
        | "source.refresh"
        | "outreach.plan"
        | "outreach.review_edit"
        | "outreach.verify_final" => 4_096,
        "outreach.write_account" => 8_192,
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
    let status = payload["status"].as_str().unwrap_or("completed");
    if status != "completed" {
        let reason = payload["incomplete_details"]["reason"]
            .as_str()
            .unwrap_or("response did not complete");
        bail!("OpenAI response status was {status}: {reason}");
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
        Some(
            serde_json::from_str(result_text.trim())
                .context("parsing OpenAI structured output as JSON")?,
        )
    } else {
        None
    };
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

    Ok(CallOutcome {
        result_text,
        structured,
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
        is_error: false,
    })
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

/// How many times to retry a transient CLI failure before giving up on a call.
const MAX_TRANSIENT_RETRIES: u32 = 2;

/// A transient provider failure worth retrying: process/network overload, a
/// timeout, or a retryable API status — not a deterministic prompt/schema error
/// and not genuine usage exhaustion (which has cross-provider fallback).
fn is_transient_cli_error(error: &anyhow::Error) -> bool {
    if is_usage_exhausted(error) {
        return false;
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
        dispatch, dispatch_codex, is_transient_cli_error, make_codex_schema_strict, openai_cost,
        openai_request, parse_openai_response, usage_exhausted_message, Backend, CallOutcome,
        Engine, ModelSelection, Stats, StreamEvent,
    };
    use serde_json::json;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn transient_cli_errors_retry_but_deterministic_and_quota_do_not() {
        // A bare non-zero CLI exit or a timeout is transient → retry.
        assert!(is_transient_cli_error(&anyhow::anyhow!(
            "claude CLI exited with exit status: 1:"
        )));
        assert!(is_transient_cli_error(&anyhow::anyhow!(
            "interactive.router call timed out after 240s — the CLI is hung or rate-limited"
        )));
        // Genuine usage exhaustion has its own cross-provider fallback, not this retry.
        assert!(!is_transient_cli_error(&anyhow::anyhow!(
            "claude CLI exited with 1: usage limit reached"
        )));
        // Deterministic failures would fail identically, so they must not retry.
        assert!(!is_transient_cli_error(&anyhow::anyhow!(
            "writer did not cite any retrieved business-knowledge principle"
        )));
        assert!(!is_transient_cli_error(&anyhow::anyhow!(
            "parsing claude CLI output as JSON"
        )));
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
        assert_eq!(request["reasoning"]["effort"], "low");
        assert_eq!(request["service_tier"], "default");
        assert_eq!(request["max_output_tokens"], 8_192);
        assert_eq!(request["prompt_cache_options"]["mode"], "explicit");
        assert!(request.get("prompt_cache_key").is_none());
        assert_eq!(request["store"], false);
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
