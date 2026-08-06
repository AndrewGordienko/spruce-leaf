//! The "Claude underbase" — reasoning via the local `claude` CLI (Claude Code).
//!
//! spruce-leaf has no API key of its own; it borrows the user's existing Claude
//! authentication by shelling out to the `claude` CLI. Two shapes are used:
//!
//!   * [`Claude::structured`] / [`Claude::text`] — a blocking `--output-format
//!     json` call, used for the concurrent pipeline fan-out where we only want
//!     the final typed result, not a play-by-play.
//!   * [`Claude::stream`] — a live `--output-format stream-json` call that emits
//!     [`StreamEvent`]s (thinking / text deltas, tool use) as they arrive, so the
//!     REPL can render the model reasoning in real time like Claude/Codex CLI.
//!
//! Every call also accrues token/cost/latency into a shared [`Stats`] so the UI
//! can print a footer.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

/// Running totals across every `claude` call this process has made.
#[derive(Default)]
pub struct Stats {
    pub calls: AtomicU64,
    pub output_tokens: AtomicU64,
    /// Cost accumulated in micro-dollars (1e-6 USD) to keep it integer/atomic.
    pub cost_micro_usd: AtomicU64,
}

/// An immutable point-in-time reading of [`Stats`], for diffing across a span.
#[derive(Clone, Copy, Default)]
pub struct StatsSnapshot {
    pub calls: u64,
    pub output_tokens: u64,
    pub cost_usd: f64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            output_tokens: self.output_tokens.load(Ordering::Relaxed),
            cost_usd: self.cost_micro_usd.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        }
    }

    fn record(&self, output_tokens: u64, cost_usd: f64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.output_tokens.fetch_add(output_tokens, Ordering::Relaxed);
        self.cost_micro_usd
            .fetch_add((cost_usd * 1_000_000.0) as u64, Ordering::Relaxed);
    }
}

impl StatsSnapshot {
    /// The delta from an earlier snapshot to this one.
    pub fn since(&self, base: StatsSnapshot) -> StatsSnapshot {
        StatsSnapshot {
            calls: self.calls.saturating_sub(base.calls),
            output_tokens: self.output_tokens.saturating_sub(base.output_tokens),
            cost_usd: (self.cost_usd - base.cost_usd).max(0.0),
        }
    }
}

/// One semantic event from a streaming `claude` call. Borrows from the JSON line
/// that produced it, so it is only valid for the duration of the callback.
///
/// Some fields (redacted thinking text, tool name) are carried for API
/// completeness even though the current UI doesn't render them.
#[allow(dead_code)]
pub enum StreamEvent<'a> {
    /// A content block opened; `kind` is "thinking" | "text" | "tool_use".
    BlockStart(&'a str),
    /// Extended-thinking text (often empty/redacted via the CLI, but signals the
    /// model is reasoning).
    ThinkingDelta(&'a str),
    /// A chunk of the model's visible answer.
    TextDelta(&'a str),
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
    pub output_tokens: u64,
    pub cost_usd: f64,
    pub duration_ms: u64,
    pub is_error: bool,
}

pub struct Claude {
    /// Optional model override; `None` uses the `claude` CLI's default model.
    model: Option<String>,
    stats: Arc<Stats>,
}

impl Claude {
    pub fn new(model: Option<String>) -> Self {
        Self { model, stats: Arc::new(Stats::default()) }
    }

    /// A handle onto the cumulative token/cost/call counters.
    pub fn stats(&self) -> Arc<Stats> {
        self.stats.clone()
    }

    /// Preflight: confirm the `claude` CLI is reachable, returning its version.
    pub async fn check() -> Result<String> {
        let out = Command::new("claude")
            .arg("--version")
            .output()
            .await
            .context(
                "couldn't run `claude` — install Claude Code and make sure it's on your PATH",
            )?;
        if !out.status.success() {
            bail!("`claude --version` failed: {}", String::from_utf8_lossy(&out.stderr).trim());
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Shared argv for both the blocking and streaming paths.
    fn base_command(&self, system: &str, schema: Option<&str>) -> Command {
        let mut cmd = Command::new("claude");
        cmd.arg("-p").arg("--system-prompt").arg(system);
        if let Some(m) = &self.model {
            cmd.arg("--model").arg(m);
        }
        if let Some(s) = schema {
            cmd.arg("--json-schema").arg(s);
        }
        cmd
    }

    /// One blocking `claude` invocation; returns the parsed JSON envelope.
    async fn call(&self, system: &str, user: &str, schema: Option<&Value>) -> Result<Value> {
        // Hold the serialized schema alive until after the command runs.
        let schema_str = match schema {
            Some(s) => Some(serde_json::to_string(s).context("serializing JSON schema")?),
            None => None,
        };
        let mut cmd = self.base_command(system, schema_str.as_deref());
        cmd.arg("--output-format").arg("json");
        cmd.arg(user).stdin(std::process::Stdio::null());

        let out = cmd
            .output()
            .await
            .context("failed to run `claude` (is Claude Code installed and on PATH?)")?;

        let stdout = String::from_utf8_lossy(&out.stdout);
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            bail!("claude CLI exited with {}:\n{}\n{}", out.status, stderr.trim(), stdout.trim());
        }

        let payload: Value = serde_json::from_str(stdout.trim())
            .with_context(|| format!("parsing claude CLI output as JSON:\n{stdout}"))?;

        if payload["is_error"].as_bool().unwrap_or(false) {
            bail!(
                "claude CLI reported an error: {}",
                payload["result"].as_str().unwrap_or("<no message>")
            );
        }

        self.stats.record(
            payload["usage"]["output_tokens"].as_u64().unwrap_or(0),
            payload["total_cost_usd"].as_f64().unwrap_or(0.0),
        );
        Ok(payload)
    }

    /// Constrain the response to `schema` and deserialize it into `T`.
    pub async fn structured<T: DeserializeOwned>(
        &self,
        system: &str,
        user: &str,
        schema: Value,
    ) -> Result<T> {
        let payload = self.call(system, user, Some(&schema)).await?;
        let structured = payload
            .get("structured_output")
            .filter(|v| !v.is_null())
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "claude CLI returned no structured_output; text was: {}",
                    payload["result"].as_str().unwrap_or("")
                )
            })?;
        serde_json::from_value::<T>(structured)
            .context("deserializing structured_output into the expected type")
    }

    /// A plain-text completion (the `result` field of the envelope).
    #[allow(dead_code)]
    pub async fn text(&self, system: &str, user: &str) -> Result<String> {
        let payload = self.call(system, user, None).await?;
        Ok(payload["result"].as_str().unwrap_or_default().to_string())
    }

    /// A streaming `claude` call. Every semantic event is handed to `on_event`
    /// as it arrives; the assembled [`CallOutcome`] (final text, structured
    /// output, usage) is returned when the stream ends.
    pub async fn stream(
        &self,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut dyn FnMut(StreamEvent<'_>),
    ) -> Result<CallOutcome> {
        let schema_str = match schema {
            Some(s) => Some(serde_json::to_string(s).context("serializing JSON schema")?),
            None => None,
        };
        let mut cmd = self.base_command(system, schema_str.as_deref());
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
            let Ok(v) = serde_json::from_str::<Value>(line) else { continue };
            dispatch(&v, on_event, &mut outcome);
        }

        let status = child.wait().await.context("waiting on `claude`")?;
        let stderr_out = stderr_task.await.unwrap_or_default();
        if !status.success() {
            bail!("claude CLI exited with {status}:\n{}", stderr_out.trim());
        }
        if outcome.is_error {
            bail!("claude CLI reported an error: {}", outcome.result_text);
        }

        self.stats.record(outcome.output_tokens, outcome.cost_usd);
        Ok(outcome)
    }

    /// Streaming structured call: render events live, return the typed result.
    pub async fn structured_streamed<T: DeserializeOwned>(
        &self,
        system: &str,
        user: &str,
        schema: Value,
        on_event: &mut dyn FnMut(StreamEvent<'_>),
    ) -> Result<T> {
        let outcome = self.stream(system, user, Some(&schema), on_event).await?;
        let structured = outcome.structured.ok_or_else(|| {
            anyhow!("claude CLI returned no structured_output; text was: {}", outcome.result_text)
        })?;
        serde_json::from_value::<T>(structured)
            .context("deserializing structured_output into the expected type")
    }
}

/// Translate one raw NDJSON line into [`StreamEvent`]s and/or outcome updates.
fn dispatch(v: &Value, on_event: &mut dyn FnMut(StreamEvent<'_>), outcome: &mut CallOutcome) {
    match v["type"].as_str() {
        Some("stream_event") => {
            let event = &v["event"];
            match event["type"].as_str() {
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
                        _ => {}
                    }
                }
                Some("content_block_stop") => on_event(StreamEvent::BlockStop),
                Some("message_delta") => {
                    if let Some(n) = event["usage"]["output_tokens"].as_u64() {
                        outcome.output_tokens = n;
                    }
                }
                _ => {}
            }
        }
        Some("result") => {
            outcome.is_error = v["is_error"].as_bool().unwrap_or(false);
            outcome.result_text = v["result"].as_str().unwrap_or_default().to_string();
            outcome.structured =
                v.get("structured_output").filter(|x| !x.is_null()).cloned();
            outcome.cost_usd = v["total_cost_usd"].as_f64().unwrap_or(0.0);
            outcome.duration_ms = v["duration_ms"].as_u64().unwrap_or(0);
            if let Some(n) = v["usage"]["output_tokens"].as_u64() {
                outcome.output_tokens = n;
            }
        }
        _ => {}
    }
}
