//! Pluggable local-CLI reasoning via Codex or Claude Code.
//!
//! spruce-leaf has no API key of its own. It borrows the user's existing CLI
//! authentication and supports two providers:
//!
//!   * Codex: `codex exec --json --output-schema`, read-only and ephemeral.
//!   * Claude: `claude -p --output-format ... --json-schema`.
//!
//! Every call also accrues token/cost/latency into a shared [`Stats`] so the UI
//! can print a footer.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

/// Which authenticated local CLI supplies model inference.
#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
pub enum Backend {
    Codex,
    Claude,
}

impl Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            Backend::Codex => "codex",
            Backend::Claude => "claude",
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
        self.output_tokens
            .fetch_add(output_tokens, Ordering::Relaxed);
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
    pub output_tokens: u64,
    pub cost_usd: f64,
    pub duration_ms: u64,
    pub is_error: bool,
}

pub struct Engine {
    backend: Backend,
    /// Optional model override; `None` uses the selected CLI's default model.
    model: Option<String>,
    stats: Arc<Stats>,
    /// Wall-clock cap per model-CLI call, so a hung/rate-limited CLI surfaces as
    /// an error instead of spinning forever. Override via SPRUCE_MODEL_TIMEOUT_SECS.
    call_timeout: Duration,
}

impl Engine {
    pub fn new(backend: Backend, model: Option<String>) -> Self {
        let secs = std::env::var("SPRUCE_MODEL_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|s| *s > 0)
            .unwrap_or(240);
        Self {
            backend,
            model,
            stats: Arc::new(Stats::default()),
            call_timeout: Duration::from_secs(secs),
        }
    }

    /// A handle onto the cumulative token/cost/call counters.
    pub fn stats(&self) -> Arc<Stats> {
        self.stats.clone()
    }

    pub fn backend(&self) -> Backend {
        self.backend
    }

    pub fn model_label(&self) -> &str {
        self.model.as_deref().unwrap_or("default")
    }

    /// Preflight: confirm the selected CLI is reachable, returning its version.
    pub async fn check(&self) -> Result<String> {
        let executable = self.backend.as_str();
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
    fn claude_command(&self, system: &str, schema: Option<&str>) -> Command {
        let mut cmd = Command::new("claude");
        cmd.arg("-p").arg("--system-prompt").arg(system);
        if let Some(m) = &self.model {
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
    fn codex_command(&self, system: &str) -> Command {
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
            .arg("--color")
            .arg("never")
            .arg("--json")
            .arg("--cd")
            .arg(std::env::temp_dir())
            .arg("--config")
            .arg(format!("developer_instructions={developer_toml}"));
        if let Some(model) = &self.model {
            cmd.arg("--model").arg(model);
        }
        cmd.kill_on_drop(true);
        cmd
    }

    /// One blocking Claude invocation; returns a provider-neutral outcome.
    async fn call_claude(
        &self,
        system: &str,
        user: &str,
        schema: Option<&Value>,
    ) -> Result<CallOutcome> {
        // Hold the serialized schema alive until after the command runs.
        let schema_str = match schema {
            Some(s) => Some(serde_json::to_string(s).context("serializing JSON schema")?),
            None => None,
        };
        let mut cmd = self.claude_command(system, schema_str.as_deref());
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
            output_tokens: payload["usage"]["output_tokens"].as_u64().unwrap_or(0),
            cost_usd: payload["total_cost_usd"].as_f64().unwrap_or(0.0),
            duration_ms: payload["duration_ms"].as_u64().unwrap_or(0),
            is_error: false,
        };
        self.stats.record(outcome.output_tokens, outcome.cost_usd);
        Ok(outcome)
    }

    /// Run a model-CLI future under the configured wall-clock cap. The child is
    /// killed on drop (see the command builders), so a timeout frees it rather
    /// than leaving a hung `claude`/`codex` process behind.
    async fn with_timeout<T>(
        &self,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        match tokio::time::timeout(self.call_timeout, fut).await {
            Ok(res) => res,
            Err(_) => bail!(
                "{} call timed out after {}s — the CLI is hung or rate-limited \
                 (raise SPRUCE_MODEL_TIMEOUT_SECS to allow longer)",
                self.backend.as_str(),
                self.call_timeout.as_secs()
            ),
        }
    }

    async fn call(&self, system: &str, user: &str, schema: Option<&Value>) -> Result<CallOutcome> {
        let work = async {
            match self.backend {
                Backend::Claude => self.call_claude(system, user, schema).await,
                Backend::Codex => {
                    fn ignore(_: StreamEvent<'_>) {}
                    self.stream_codex(system, user, schema, &mut ignore).await
                }
            }
        };
        self.with_timeout(work).await
    }

    /// Constrain the response to `schema` and deserialize it into `T`.
    pub async fn structured<T: DeserializeOwned>(
        &self,
        system: &str,
        user: &str,
        schema: Value,
    ) -> Result<T> {
        let outcome = self.call(system, user, Some(&schema)).await?;
        let structured = outcome.structured.ok_or_else(|| {
            anyhow!(
                "{} CLI returned no structured output; text was: {}",
                self.backend.as_str(),
                outcome.result_text
            )
        })?;
        serde_json::from_value::<T>(structured)
            .context("deserializing structured_output into the expected type")
    }

    /// A plain-text completion (the `result` field of the envelope).
    #[allow(dead_code)]
    pub async fn text(&self, system: &str, user: &str) -> Result<String> {
        Ok(self.call(system, user, None).await?.result_text)
    }

    /// A streaming `claude` call. Every semantic event is handed to `on_event`
    /// as it arrives; the assembled [`CallOutcome`] (final text, structured
    /// output, usage) is returned when the stream ends.
    pub async fn stream(
        &self,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<CallOutcome> {
        let work = async {
            match self.backend {
                Backend::Claude => self.stream_claude(system, user, schema, on_event).await,
                Backend::Codex => self.stream_codex(system, user, schema, on_event).await,
            }
        };
        self.with_timeout(work).await
    }

    async fn stream_claude(
        &self,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<CallOutcome> {
        let schema_str = match schema {
            Some(s) => Some(serde_json::to_string(s).context("serializing JSON schema")?),
            None => None,
        };
        let mut cmd = self.claude_command(system, schema_str.as_deref());
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
            bail!("claude CLI exited with {status}:\n{}", stderr_out.trim());
        }
        if outcome.is_error {
            bail!("claude CLI reported an error: {}", outcome.result_text);
        }

        self.stats.record(outcome.output_tokens, outcome.cost_usd);
        Ok(outcome)
    }

    async fn stream_codex(
        &self,
        system: &str,
        user: &str,
        schema: Option<&Value>,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<CallOutcome> {
        let schema_file = schema.map(TempSchema::new).transpose()?;
        let mut cmd = self.codex_command(system);
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

        self.stats.record(outcome.output_tokens, 0.0);
        Ok(outcome)
    }

    /// Streaming structured call: render events live, return the typed result.
    pub async fn structured_streamed<T: DeserializeOwned>(
        &self,
        system: &str,
        user: &str,
        schema: Value,
        on_event: &mut (dyn FnMut(StreamEvent<'_>) + Send),
    ) -> Result<T> {
        let outcome = self.stream(system, user, Some(&schema), on_event).await?;
        let structured = outcome.structured.ok_or_else(|| {
            anyhow!(
                "{} CLI returned no structured output; text was: {}",
                self.backend.as_str(),
                outcome.result_text
            )
        })?;
        serde_json::from_value::<T>(structured)
            .context("deserializing structured_output into the expected type")
    }
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
            outcome.structured = v.get("structured_output").filter(|x| !x.is_null()).cloned();
            outcome.cost_usd = v["total_cost_usd"].as_f64().unwrap_or(0.0);
            outcome.duration_ms = v["duration_ms"].as_u64().unwrap_or(0);
            if let Some(n) = v["usage"]["output_tokens"].as_u64() {
                outcome.output_tokens = n;
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{dispatch, dispatch_codex, make_codex_schema_strict, CallOutcome, StreamEvent};
    use serde_json::json;

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
            "usage": { "output_tokens": 42 }
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
}
