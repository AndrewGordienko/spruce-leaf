//! The polished terminal layer — the part that makes spruce-leaf feel like the
//! Claude/Codex CLI rather than a script printing lines.
//!
//! Four pieces:
//!   * session/composer helpers that give the REPL the same quiet hierarchy as
//!     Codex: a compact header, `›` input, bullet-prefixed transcript cells,
//!     and a low-contrast context line.
//!   * [`TurnView`] — the sink for a streaming router turn. It shows a live
//!     "thinking…" spinner while the model reasons, then streams the model's
//!     natural-language plan token-by-token.
//!   * [`CampaignView`] — a live, self-redrawing progress tree for the concurrent
//!     campaign pipeline (accounts → contacts → sequences), with a running
//!     tokens/cost/elapsed footer.
//!   * a small [`Spinner`] and a set of ANSI style helpers underneath both.
//!
//! Everything degrades gracefully: when stdout isn't a TTY (or `NO_COLOR` is
//! set) the animations and colors switch off and we fall back to plain lines.

use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::engine::{Stats, StatsSnapshot, StreamEvent};

const FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const TICK_MS: u64 = 90;

// --- styling ---------------------------------------------------------------

/// Whether to emit ANSI color/animation at all. Honors the `NO_COLOR` and
/// `CLICOLOR_FORCE` conventions, else auto-detects a TTY on stdout.
pub fn fancy() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        if std::env::var_os("NO_COLOR").is_some() {
            false
        } else if std::env::var_os("CLICOLOR_FORCE").is_some() {
            true
        } else {
            std::io::stdout().is_terminal()
        }
    })
}

fn paint(s: &str, code: &str) -> String {
    if fancy() {
        format!("\x1b[{code}m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

pub fn dim(s: &str) -> String {
    paint(s, "2")
}
pub fn bold(s: &str) -> String {
    paint(s, "1")
}
pub fn leaf(s: &str) -> String {
    paint(s, "38;5;71")
}
pub fn cyan(s: &str) -> String {
    paint(s, "38;5;80")
}
#[allow(dead_code)]
pub fn yellow(s: &str) -> String {
    paint(s, "38;5;179")
}
pub fn gray(s: &str) -> String {
    paint(s, "38;5;245")
}
pub fn italic(s: &str) -> String {
    paint(s, "3")
}

// --- lightweight markdown -------------------------------------------------

/// Replace paired `delim…delim` spans in `s`, passing the inner text through
/// `apply`. Unmatched or empty delimiters are left verbatim, so stray `*` or a
/// lone backtick never mangle the line.
fn wrap_spans(s: &str, delim: &str, apply: &dyn Fn(&str) -> String) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some(open) = rest.find(delim) {
        let after = &rest[open + delim.len()..];
        match after.find(delim) {
            // Non-empty span with a closing delimiter → styled.
            Some(close) if close > 0 => {
                out.push_str(&rest[..open]);
                out.push_str(&apply(&after[..close]));
                rest = &after[close + delim.len()..];
            }
            // No close (or empty ``): keep the opener literal and move past it.
            _ => {
                out.push_str(&rest[..open + delim.len()]);
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Render one line of lightweight Markdown for the terminal.
///
/// With `emphasis` on, `**bold**`, `*italic*`, and `` `code` `` become real ANSI
/// styles; with it off (e.g. already-dimmed reasoning) the markers are simply
/// stripped so no literal `**`/`*`/`` ` `` leak onto the screen. Headings collapse
/// to their text (bold when emphasized) and `-`/`*`/`+` bullets become `•`.
/// Underscores are left alone so `snake_case` identifiers survive intact.
pub fn markdown_line(line: &str, emphasis: bool) -> String {
    let indent_len = line.len() - line.trim_start().len();
    let indent = &line[..indent_len];
    let mut body = line[indent_len..].to_string();

    // Heading: strip a leading run of '#'s followed by a space.
    let after_hashes = body.trim_start_matches('#');
    let heading = after_hashes.len() < body.len() && after_hashes.starts_with(' ');
    if heading {
        body = after_hashes.trim_start().to_string();
    }

    // Bullet markers become a real glyph (skip `**bold**` at line start).
    for marker in ["- ", "+ "] {
        if let Some(text) = body.strip_prefix(marker) {
            body = format!("• {text}");
            break;
        }
    }
    if let Some(text) = body.strip_prefix("* ") {
        if !text.starts_with('*') {
            body = format!("• {text}");
        }
    }

    // Inline spans — double marker (bold) before single (italic).
    body = wrap_spans(&body, "**", &|inner| {
        if emphasis {
            bold(inner)
        } else {
            inner.to_string()
        }
    });
    body = wrap_spans(&body, "`", &|inner| {
        if emphasis {
            cyan(inner)
        } else {
            inner.to_string()
        }
    });
    body = wrap_spans(&body, "*", &|inner| {
        if emphasis {
            italic(inner)
        } else {
            inner.to_string()
        }
    });

    if heading && emphasis {
        body = bold(&body);
    }
    format!("{indent}{body}")
}

// --- session + transcript -------------------------------------------------

/// Print the compact session card used when the interactive terminal opens.
pub fn session_header(backend: &str, model: &str, brand: &str, directory: &str, crm: &str) {
    let lines = session_header_lines(backend, model, brand, directory, crm);
    for (index, line) in lines.iter().enumerate() {
        let rendered = if index == 1 { bold(line) } else { dim(line) };
        println!("{rendered}");
    }
}

fn session_header_lines(
    backend: &str,
    model: &str,
    brand: &str,
    directory: &str,
    crm: &str,
) -> Vec<String> {
    let version = env!("CARGO_PKG_VERSION");
    let rows = [
        format!(">_ Spruce Leaf (v{version})"),
        String::new(),
        format!("model:     {backend} · {model}"),
        format!("brand:     {brand}   /brand to change"),
        format!("directory: {directory}"),
        format!("crm:       {crm}   /crm to open"),
    ];
    let inner = rows
        .iter()
        .map(|row| row.chars().count())
        .max()
        .unwrap_or(40)
        .clamp(40, 68);
    let border = format!("╭{}╮", "─".repeat(inner + 2));
    let bottom = format!("╰{}╯", "─".repeat(inner + 2));
    let mut lines = vec![border];
    for row in rows {
        let row = truncate(&row, inner);
        let padding = " ".repeat(inner.saturating_sub(row.chars().count()));
        lines.push(format!("│ {row}{padding} │"));
    }
    lines.push(bottom);
    lines
}

/// Render a finalized assistant response as a Codex-style transcript cell.
pub fn assistant_message(message: &str) {
    for (index, raw) in message.trim().lines().enumerate() {
        if raw.trim().is_empty() {
            println!();
            continue;
        }
        let line = markdown_line(raw, true);
        if index == 0 {
            println!("{}{}", dim("• "), line);
        } else {
            println!("  {line}");
        }
    }
}

/// Render a compact tool/action cell with optional nested detail.
pub fn activity(title: &str, detail: impl AsRef<str>) {
    println!("{}{}", dim("• "), bold(title));
    for (index, line) in detail.as_ref().lines().enumerate() {
        let branch = if index == 0 { "  └ " } else { "    " };
        println!("{}{}", dim(branch), dim(line));
    }
}

/// Low-contrast session context shown next to the next composer.
pub fn context_line(backend: &str, model: &str, brand: &str, directory: &str) {
    println!(
        "{}",
        dim(&format!("  {backend} {model} · {brand} · {directory}"))
    );
}

/// Format a stats delta as a compact footer, e.g. "31 calls · 2.0k tok · $0.11 · 48s".
fn footer(snap: StatsSnapshot, elapsed: Duration) -> String {
    format!(
        "{} calls · {} tok · ${:.2} · {}s",
        snap.calls,
        human_tokens(snap.output_tokens),
        snap.cost_usd,
        elapsed.as_secs()
    )
}

fn human_tokens(n: u64) -> String {
    if n >= 1000 {
        format!("{:.1}k", n as f64 / 1000.0)
    } else {
        n.to_string()
    }
}

// --- spinner ---------------------------------------------------------------

/// A single-line braille spinner driven by its own thread, so it keeps ticking
/// while the main thread is blocked awaiting the model.
pub struct Spinner {
    msg: Arc<Mutex<String>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Spinner {
    pub fn start(initial: &str) -> Self {
        let msg = Arc::new(Mutex::new(initial.to_string()));
        let stop = Arc::new(AtomicBool::new(false));

        let handle = if fancy() {
            let (msg_t, stop_t) = (msg.clone(), stop.clone());
            let started = Instant::now();
            Some(thread::spawn(move || {
                let mut i = 0usize;
                while !stop_t.load(Ordering::Relaxed) {
                    let text = msg_t.lock().unwrap().clone();
                    let secs = started.elapsed().as_secs_f32();
                    print!(
                        "\r\x1b[K{} {} {}",
                        cyan(FRAMES[i % FRAMES.len()]),
                        text,
                        dim(&format!("({secs:.0}s)"))
                    );
                    let _ = std::io::stdout().flush();
                    i += 1;
                    thread::sleep(Duration::from_millis(TICK_MS));
                }
            }))
        } else {
            println!("• {initial}");
            None
        };

        Spinner { msg, stop, handle }
    }

    pub fn set(&self, msg: &str) {
        if let Ok(mut m) = self.msg.lock() {
            *m = msg.to_string();
        }
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        // Idempotent: only the first stop joins + clears the line.
        if self.stop.swap(true, Ordering::Relaxed) {
            return;
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        if fancy() {
            print!("\r\x1b[K");
            let _ = std::io::stdout().flush();
        }
    }
}

// --- turn view (streaming router reasoning) --------------------------------

enum TurnPhase {
    Idle,
    Thinking,
    Reasoning,
    Text,
}

/// Renders one streaming agent turn, Codex-style: a "thinking…" spinner while
/// the model reasons, then its `plan` streamed live as dim reasoning, then its
/// answer streamed in normal weight.
pub struct TurnView {
    spinner: Option<Spinner>,
    phase: TurnPhase,
    thought_since: Option<Instant>,
    any_text: bool,
    /// Accumulated tool-call JSON, scanned for the `plan` field as it streams.
    json_buf: String,
    /// Byte length of the decoded plan already printed.
    plan_shown: usize,
    /// A completed reasoning block needs a newline before the next one.
    reasoning_needs_separator: bool,
    /// Codex can provide its own reasoning summary before the structured plan.
    /// When it does, avoid echoing the plan field as a second copy.
    reasoning_from_backend: bool,
    /// Whether the next streamed character begins a new visual line.
    at_line_start: bool,
    /// The not-yet-newline-terminated tail of the current line. Held back so the
    /// completed line can be Markdown-rendered as a whole (no split `**` markers).
    pending: String,
    /// Whether `pending` is dimmed reasoning (vs. normal answer text).
    pending_dim: bool,
}

impl Default for TurnView {
    fn default() -> Self {
        Self::new()
    }
}

impl TurnView {
    pub fn new() -> Self {
        TurnView {
            spinner: None,
            phase: TurnPhase::Idle,
            thought_since: None,
            any_text: false,
            json_buf: String::new(),
            plan_shown: 0,
            reasoning_needs_separator: false,
            reasoning_from_backend: false,
            at_line_start: true,
            pending: String::new(),
            pending_dim: false,
        }
    }

    /// Feed one streaming event. Wire this straight into `Engine::stream`.
    pub fn on_event(&mut self, ev: StreamEvent) {
        match ev {
            StreamEvent::BlockStart(kind) => match kind {
                "thinking" => {
                    self.thought_since.get_or_insert_with(Instant::now);
                    if !matches!(self.phase, TurnPhase::Reasoning) {
                        self.set_spinner("Working");
                        self.phase = TurnPhase::Thinking;
                    }
                }
                "tool_use" if !matches!(self.phase, TurnPhase::Reasoning) => {
                    self.set_spinner("Working");
                }
                "text" => self.begin_text(),
                _ => {}
            },
            // The decision object (incl. the `plan`) streams as partial JSON.
            StreamEvent::ToolInputDelta(chunk) => {
                self.json_buf.push_str(chunk);
                self.stream_plan();
            }
            StreamEvent::TextDelta(s) => {
                if !matches!(self.phase, TurnPhase::Text) {
                    self.begin_text();
                }
                self.any_text = true;
                self.write_stream(s, false);
            }
            // Claude currently redacts these; Codex emits completed reasoning
            // summaries. Render whichever non-empty text the backend exposes.
            StreamEvent::ThinkingDelta(s) => self.stream_reasoning(s),
            StreamEvent::ToolUse { .. } => {}
            StreamEvent::BlockStop => {
                if matches!(self.phase, TurnPhase::Reasoning) {
                    self.flush_pending();
                    self.reasoning_needs_separator = true;
                }
            }
        }
    }

    fn set_spinner(&mut self, msg: &str) {
        match &self.spinner {
            Some(sp) => sp.set(msg),
            None => self.spinner = Some(Spinner::start(msg)),
        }
    }

    /// Print any newly-arrived characters of the streaming `plan` field, as dim
    /// "reasoning" text under a header.
    fn stream_plan(&mut self) {
        if self.reasoning_from_backend {
            return;
        }
        let Some((plan, _complete)) = json_string_prefix(&self.json_buf, "plan") else {
            return;
        };
        // Ignore leading whitespace so reasoning never opens with a blank "• " line.
        let plan = plan.trim_start();
        if plan.len() <= self.plan_shown {
            return;
        }
        if self.plan_shown == 0 {
            if plan.is_empty() {
                return; // hold off on the header until real content arrives
            }
            self.begin_reasoning();
        }
        let fresh = &plan[self.plan_shown..];
        self.write_stream(fresh, true);
        self.plan_shown = plan.len();
    }

    fn stream_reasoning(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        self.reasoning_from_backend = true;
        self.begin_reasoning();
        self.write_stream(text, true);
    }

    fn begin_reasoning(&mut self) {
        self.spinner.take();
        self.thought_since.take(); // visible reasoning replaces the timing-only summary
        if matches!(self.phase, TurnPhase::Reasoning) {
            if self.reasoning_needs_separator {
                if !self.at_line_start {
                    println!();
                }
                print!("{}", dim("• "));
                self.at_line_start = false;
                self.reasoning_needs_separator = false;
            }
        } else {
            print!("{}", dim("• "));
            self.at_line_start = false;
            self.phase = TurnPhase::Reasoning;
        }
    }

    /// Buffer streamed text, emitting each completed (newline-terminated) line
    /// through the Markdown renderer. The trailing partial line is held in
    /// `pending` until its newline arrives (or the turn/block ends), so inline
    /// markers like `**bold**` are never split mid-render.
    fn write_stream(&mut self, text: &str, dimmed: bool) {
        self.pending_dim = dimmed;
        self.pending.push_str(text);
        while let Some(nl) = self.pending.find('\n') {
            let mut line: String = self.pending.drain(..=nl).collect();
            line.pop(); // drop the trailing '\n'
            if !line.is_empty() {
                self.emit_line(&line, dimmed);
            }
            println!();
            self.at_line_start = true;
        }
        let _ = std::io::stdout().flush();
    }

    /// Render and print one complete line (with its leading indent), no newline.
    fn emit_line(&mut self, line: &str, dimmed: bool) {
        if self.at_line_start {
            print!("  ");
        }
        let rendered = markdown_line(line, !dimmed);
        if dimmed {
            print!("{}", dim(&rendered));
        } else {
            print!("{rendered}");
        }
        self.at_line_start = false;
    }

    /// Emit any buffered partial line (used at block/turn boundaries).
    fn flush_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let line = std::mem::take(&mut self.pending);
        let dimmed = self.pending_dim;
        self.emit_line(&line, dimmed);
        let _ = std::io::stdout().flush();
    }

    fn begin_text(&mut self) {
        self.spinner.take(); // stop + clear the spinner line
        if matches!(self.phase, TurnPhase::Reasoning) {
            self.flush_pending();
            if !self.at_line_start {
                println!();
            }
            println!();
        } else if let Some(t) = self.thought_since.take() {
            println!(
                "{}",
                dim(&format!("• Thought for {:.1}s", t.elapsed().as_secs_f32()))
            );
            println!();
        }
        print!("{}", dim("• "));
        self.at_line_start = false;
        self.phase = TurnPhase::Text;
    }

    /// Finish the turn; returns whether any visible answer text was streamed.
    pub fn finish(mut self) -> bool {
        self.spinner.take();
        self.flush_pending();
        if matches!(self.phase, TurnPhase::Reasoning | TurnPhase::Text) && !self.at_line_start {
            println!();
        }
        self.any_text
    }
}

/// Extract the (possibly partial) value of a top-level JSON string field from a
/// streaming buffer. Returns the decoded value so far and whether it's complete
/// (closing quote seen). Handles the common escapes and tolerates a truncated
/// tail (mid-escape / mid-value while still streaming).
fn json_string_prefix(buf: &str, key: &str) -> Option<(String, bool)> {
    let pat = format!("\"{key}\"");
    let after_key = buf.find(&pat)? + pat.len();
    let rest = &buf[after_key..];

    // Find the opening quote of the value (skip `:` and whitespace).
    let mut seen_colon = false;
    let mut value_start = None;
    for (i, c) in rest.char_indices() {
        match c {
            ':' => seen_colon = true,
            '"' if seen_colon => {
                value_start = Some(i + c.len_utf8());
                break;
            }
            c if c.is_whitespace() => {}
            _ if seen_colon => return None, // a non-string value — not what we want
            _ => {}
        }
    }
    let value_start = value_start?;

    let mut out = String::new();
    let mut chars = rest[value_start..].chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some((out, true)),
            '\\' => match chars.next() {
                None => return Some((out, false)),
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some('/') => out.push('/'),
                Some('u') => {
                    let mut hex = String::new();
                    for _ in 0..4 {
                        match chars.next() {
                            Some(h) => hex.push(h),
                            None => return Some((out, false)),
                        }
                    }
                    if let Ok(n) = u32::from_str_radix(&hex, 16) {
                        // JSON represents non-BMP characters as UTF-16
                        // surrogate pairs. If the low surrogate has not
                        // arrived yet, leave the pair out of this prefix and
                        // decode it on the next streaming delta.
                        if (0xD800..=0xDBFF).contains(&n) {
                            let mut tail = chars.clone();
                            if tail.next() != Some('\\') || tail.next() != Some('u') {
                                if tail.as_str().is_empty() {
                                    return Some((out, false));
                                }
                                out.push('\u{FFFD}');
                                continue;
                            }

                            let mut low_hex = String::new();
                            for _ in 0..4 {
                                match tail.next() {
                                    Some(h) => low_hex.push(h),
                                    None => return Some((out, false)),
                                }
                            }
                            let Ok(low) = u32::from_str_radix(&low_hex, 16) else {
                                out.push('\u{FFFD}');
                                continue;
                            };
                            if (0xDC00..=0xDFFF).contains(&low) {
                                let scalar = 0x10000 + ((n - 0xD800) << 10) + (low - 0xDC00);
                                if let Some(ch) = char::from_u32(scalar) {
                                    out.push(ch);
                                }
                                chars = tail;
                            } else {
                                out.push('\u{FFFD}');
                            }
                        } else if let Some(ch) = char::from_u32(n) {
                            out.push(ch);
                        } else {
                            out.push('\u{FFFD}');
                        }
                    }
                }
                Some(other) => out.push(other),
            },
            c => out.push(c),
        }
    }
    Some((out, false))
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod turn_view_tests {
    use super::{json_string_prefix, markdown_line, session_header_lines};

    #[test]
    fn markdown_strips_markers_when_emphasis_off() {
        // Dimmed reasoning: markers must vanish, never leak as literal characters.
        assert_eq!(
            markdown_line("**Running now:** Apollo `search`", false),
            "Running now: Apollo search"
        );
        assert_eq!(markdown_line("- reveal & verify", false), "• reveal & verify");
        assert_eq!(markdown_line("### Then the pipeline", false), "Then the pipeline");
    }

    #[test]
    fn markdown_consumes_markers_when_emphasis_on() {
        // ANSI styling is TTY-gated, but the markers must be consumed regardless
        // so nothing leaks as literal `**`/`` ` ``.
        let out = markdown_line("**Running now:** Apollo `search`", true);
        assert!(!out.contains("**"), "bold markers should be consumed: {out:?}");
        assert!(!out.contains('`'), "code markers should be consumed: {out:?}");
        assert!(out.contains("Running now:") && out.contains("search"));
    }

    #[test]
    fn markdown_leaves_snake_case_and_stray_markers_intact() {
        // No paired delimiters → nothing to style; identifiers survive.
        assert_eq!(
            markdown_line("call enrich_people then plan_outreach", true),
            "call enrich_people then plan_outreach"
        );
        assert_eq!(markdown_line("2 * 3 = 6", true), "2 * 3 = 6");
        assert_eq!(markdown_line("a lone * marker", false), "a lone * marker");
    }

    #[test]
    fn markdown_preserves_indentation() {
        assert_eq!(markdown_line("    nested text", true), "    nested text");
    }

    #[test]
    fn session_header_has_stable_box_width_and_context() {
        let lines = session_header_lines(
            "codex",
            "default",
            "gnk",
            "/tmp/sales-os2",
            "http://localhost:8787",
        );
        let width = lines[0].chars().count();

        assert!(lines.iter().all(|line| line.chars().count() == width));
        assert!(lines.iter().any(|line| line.contains("codex · default")));
        assert!(lines.iter().any(|line| line.contains("/brand to change")));
    }

    #[test]
    fn extracts_partial_plan_as_it_streams() {
        assert_eq!(json_string_prefix(r#"{"plan"#, "plan"), None);
        assert_eq!(
            json_string_prefix(r#"{"plan":"I will inspect"#, "plan"),
            Some(("I will inspect".to_string(), false))
        );
        assert_eq!(
            json_string_prefix(r#"{"plan":"I will inspect."}"#, "plan"),
            Some(("I will inspect.".to_string(), true))
        );
    }

    #[test]
    fn decodes_json_escapes_and_unicode() {
        assert_eq!(
            json_string_prefix(
                r#"{"plan":"Use \"evidence\"\nthen \u03bb and \ud83d\ude80."}"#,
                "plan"
            ),
            Some(("Use \"evidence\"\nthen λ and 🚀.".to_string(), true))
        );
    }

    #[test]
    fn waits_for_a_complete_escape_sequence() {
        assert_eq!(
            json_string_prefix(r#"{"plan":"go \uD83D"#, "plan"),
            Some(("go ".to_string(), false))
        );
        assert_eq!(
            json_string_prefix(r#"{"plan":"go \uD83D\uDE80"#, "plan"),
            Some(("go 🚀".to_string(), false))
        );
    }
}

// --- campaign view (live pipeline progress tree) ---------------------------

enum Stage {
    Researching,
    Building,
    Done,
}

struct AcctProg {
    name: String,
    /// Number of contacts mapped (0 until the contact stage returns).
    contacts: usize,
    /// Sequences finished for this account.
    seqs_done: usize,
}

struct CampaignState {
    header: String,
    stage: Stage,
    accounts: Vec<AcctProg>,
    started: Instant,
}

/// A self-redrawing progress tree for a running campaign. Implements
/// [`crate::pipeline::Progress`] so the pipeline can report into it from its
/// concurrent stages while the render thread paints the tree.
pub struct CampaignView {
    state: Arc<Mutex<CampaignState>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    stats: Arc<Stats>,
    base: StatsSnapshot,
}

impl CampaignView {
    /// `header` is the one-line campaign descriptor (already styled-free), e.g.
    /// `GnK · logistics chargebacks · 5×5×7`.
    pub fn start(header: String, stats: Arc<Stats>) -> Self {
        let base = stats.snapshot();
        let state = Arc::new(Mutex::new(CampaignState {
            header,
            stage: Stage::Researching,
            accounts: Vec::new(),
            started: Instant::now(),
        }));
        let stop = Arc::new(AtomicBool::new(false));

        let handle = if fancy() {
            let (state_t, stop_t, stats_t) = (state.clone(), stop.clone(), stats.clone());
            Some(thread::spawn(move || {
                ticker(state_t, stop_t, stats_t, base)
            }))
        } else {
            // Non-TTY: print a single static "started" cell.
            activity("Running campaign", &state.lock().unwrap().header);
            None
        };

        CampaignView {
            state,
            stop,
            handle,
            stats,
            base,
        }
    }

    /// Stop the render loop (leaving the final frame on screen) and return the
    /// token/cost/time totals for this campaign.
    pub fn finish(mut self) -> StatsSnapshot {
        self.state.lock().unwrap().stage = Stage::Done;
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        self.stats.snapshot().since(self.base)
    }
}

impl crate::pipeline::Progress for CampaignView {
    fn accounts_found(&self, names: &[String]) {
        let mut s = self.state.lock().unwrap();
        s.stage = Stage::Building;
        s.accounts = names
            .iter()
            .map(|n| AcctProg {
                name: n.clone(),
                contacts: 0,
                seqs_done: 0,
            })
            .collect();
    }

    fn account_contacts(&self, account: &str, contacts: usize) {
        let mut s = self.state.lock().unwrap();
        if let Some(a) = s.accounts.iter_mut().find(|a| a.name == account) {
            a.contacts = contacts;
        }
    }

    fn sequence_done(&self, account: &str) {
        let mut s = self.state.lock().unwrap();
        if let Some(a) = s.accounts.iter_mut().find(|a| a.name == account) {
            a.seqs_done += 1;
        }
    }
}

/// The render thread: repaint the tree in place every tick until stopped, then
/// paint one final frame.
fn ticker(
    state: Arc<Mutex<CampaignState>>,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
    base: StatsSnapshot,
) {
    let mut prev_lines = 0usize;
    let mut frame_i = 0usize;
    loop {
        let done = stop.load(Ordering::Relaxed);
        let lines = render(&state.lock().unwrap(), &stats, base, frame_i);

        let out = std::io::stdout();
        let mut lock = out.lock();
        if prev_lines > 0 {
            let _ = write!(lock, "\x1b[{prev_lines}A");
        }
        let _ = write!(lock, "\r\x1b[J");
        for l in &lines {
            let _ = writeln!(lock, "{l}");
        }
        let _ = lock.flush();
        prev_lines = lines.len();

        if done {
            break;
        }
        frame_i += 1;
        thread::sleep(Duration::from_millis(TICK_MS));
    }
}

/// Build the tree as a vector of lines (no trailing newlines).
fn render(
    state: &CampaignState,
    stats: &Stats,
    base: StatsSnapshot,
    frame_i: usize,
) -> Vec<String> {
    let sp = FRAMES[frame_i % FRAMES.len()];
    let title = if matches!(state.stage, Stage::Done) {
        "Ran campaign"
    } else {
        "Running campaign"
    };
    let mut lines = vec![
        format!("{}{}", dim("• "), bold(title)),
        format!("{}{}", dim("  └ "), dim(&state.header)),
    ];

    match state.stage {
        Stage::Researching => {
            lines.push(format!("    {} {}", cyan(sp), dim("Researching accounts…")));
        }
        Stage::Building | Stage::Done => {
            for a in &state.accounts {
                let name = pad(&a.name, 30);
                if a.contacts == 0 {
                    lines.push(format!(
                        "    {} {} {}",
                        cyan(sp),
                        name,
                        dim("Mapping contacts…")
                    ));
                } else if a.seqs_done >= a.contacts {
                    lines.push(format!(
                        "    {} {} {}",
                        leaf("✓"),
                        name,
                        dim(&format!("{} contacts", a.contacts))
                    ));
                } else {
                    lines.push(format!(
                        "    {} {} {}",
                        cyan(sp),
                        name,
                        dim(&format!("{}/{} sequences", a.seqs_done, a.contacts))
                    ));
                }
            }
        }
    }

    let snap = stats.snapshot().since(base);
    let glyph = if matches!(state.stage, Stage::Done) {
        leaf("✓")
    } else {
        gray(sp)
    };
    lines.push(format!(
        "    {} {}",
        glyph,
        dim(&footer(snap, state.started.elapsed()))
    ));
    lines
}

/// Left-pad a plain (uncolored) string to `width` display columns.
fn pad(s: &str, width: usize) -> String {
    let n = s.chars().count();
    if n >= width {
        s.to_string()
    } else {
        format!("{s}{}", " ".repeat(width - n))
    }
}

/// The one-line campaign descriptor shown as the tree's header chip, e.g.
/// `GnK · logistics chargebacks · 5×5×7`.
pub fn campaign_header(
    brand: &str,
    thesis: &str,
    accounts: usize,
    contacts: usize,
    touches: usize,
) -> String {
    format!(
        "{brand} · {} · {accounts}×{contacts}×{touches}",
        truncate(thesis, 46)
    )
}

/// Trim a string to `max` display characters, adding an ellipsis if cut.
fn truncate(s: &str, max: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= max {
        return s.to_string();
    }
    let cut: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{}…", cut.trim_end())
}
