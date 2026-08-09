//! The polished terminal layer — the part that makes spruce-leaf feel like the
//! Claude/Codex CLI rather than a script printing lines.
//!
//! Four pieces:
//!   * session/composer helpers that give the REPL the same quiet hierarchy as
//!     Codex: a compact header, `›` input, bullet-prefixed transcript cells,
//!     and a low-contrast context line.
//!   * [`TurnView`] — a quiet router-status sink. Private model scratch-work is
//!     never printed; the agent renders one truthful action intent after routing.
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

/// Start an interactive session on a blank terminal, including scrollback where
/// the terminal supports the xterm erase-scrollback sequence. Non-TTY commands
/// keep their output intact for scripts and logs.
pub fn clear_terminal() {
    if std::io::stdout().is_terminal() {
        print!("\x1b[2J\x1b[3J\x1b[H");
        let _ = std::io::stdout().flush();
    }
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
pub fn blue(s: &str) -> String {
    paint(s, "38;5;75")
}
pub fn dark_blue(s: &str) -> String {
    paint(s, "38;5;68")
}
pub fn red(s: &str) -> String {
    paint(s, "38;5;203")
}
pub fn orange(s: &str) -> String {
    paint(s, "38;5;214")
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
        let rendered = if index == 1 {
            blue(&bold(line))
        } else {
            dim(line)
        };
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
        format!("gtm:       {crm}/gtm   /gtm to open"),
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
            println!("{}{}", blue("• "), line);
        } else {
            println!("  {line}");
        }
    }
}

/// Render a compact tool/action cell with optional nested detail.
pub fn activity(title: &str, detail: impl AsRef<str>) {
    println!("{}{}", blue("• "), bold(title));
    for (index, line) in detail.as_ref().lines().enumerate() {
        let branch = if index == 0 { "  └ " } else { "    " };
        println!("{}{}", dim(branch), dim(line));
    }
}

/// One concise, truthful description of the accepted structured action. This
/// replaces streamed router scratch-work and never claims completion early.
pub fn action_intent(title: &str, detail: impl AsRef<str>) {
    println!("{} {}", blue("›"), bold(title));
    if !detail.as_ref().trim().is_empty() {
        println!("  {} {}", gray("└"), dim(detail.as_ref()));
    }
    println!();
}

/// Low-contrast session context shown next to the next composer.
pub fn context_line(backend: &str, model: &str, brand: &str, directory: &str) {
    println!(
        "  {} {} {} {} {} {}",
        dark_blue(backend),
        blue(model),
        dim("·"),
        blue(brand),
        dim("·"),
        dim(directory)
    );
}

/// Format a transparent stats delta: attempts/failures and all token classes.
fn footer(snap: StatsSnapshot, elapsed: Duration) -> String {
    format!(
        "{}/{} calls · {} in ({} cached) · {} out · {} failed · ${:.2} · {}s",
        snap.calls,
        snap.attempts,
        human_tokens(snap.input_tokens),
        human_tokens(snap.cached_input_tokens),
        human_tokens(snap.output_tokens),
        snap.failures,
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
                        dark_blue(FRAMES[i % FRAMES.len()]),
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
            if *m == msg {
                return;
            }
            *m = msg.to_string();
            // Pipes, CI logs, and redirected sessions do not have the render
            // thread. Preserve meaningful phase changes there as plain lines.
            if !fancy() {
                println!("• {msg}");
            }
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

// --- sourcing view --------------------------------------------------------

#[derive(Clone)]
struct SourceRow {
    key: String,
    title: String,
    detail: String,
    status: String,
}

struct SourceState {
    header: String,
    active_title: String,
    success_title: String,
    failure_title: String,
    rows: Vec<SourceRow>,
    started: Instant,
    done: bool,
    succeeded: bool,
    transient: bool,
}

/// A stable, Codex-like activity transcript for account sourcing. Milestones
/// remain visible while their detail is updated in place, avoiding the long
/// flattened spinner line that previously mixed ICP filters, Apollo results,
/// and individual rejection messages together.
pub struct SourceView {
    state: Arc<Mutex<SourceState>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    stats: Arc<Stats>,
    base: StatsSnapshot,
}

impl SourceView {
    pub fn start(header: String, stats: Arc<Stats>) -> Self {
        Self::start_with_titles(
            header,
            stats,
            "Sourcing companies",
            "Sourced companies",
            "Sourcing stopped",
            false,
        )
    }

    /// Full motion already owns the durable pass-by-pass transcript. Keep the
    /// detailed sourcing view live while a pass runs, then clear it so eight
    /// adaptive attempts do not bury the actual fulfillment result.
    pub fn start_transient(header: String, stats: Arc<Stats>) -> Self {
        Self::start_with_titles(
            header,
            stats,
            "Sourcing companies",
            "Sourced companies",
            "Sourcing stopped",
            true,
        )
    }

    pub fn start_enrichment(header: String, stats: Arc<Stats>) -> Self {
        Self::start_with_titles(
            header,
            stats,
            "Enriching contacts",
            "Enriched contacts",
            "Enrichment stopped",
            false,
        )
    }

    /// Full motion reports contact coverage as part of its own final result.
    /// Keep Apollo reveal progress visible only while it is running so a
    /// replacement loop does not leave one durable enrichment block per pass.
    pub fn start_enrichment_transient(header: String, stats: Arc<Stats>) -> Self {
        Self::start_with_titles(
            header,
            stats,
            "Enriching contacts",
            "Enriched contacts",
            "Enrichment stopped",
            true,
        )
    }

    fn start_with_titles(
        header: String,
        stats: Arc<Stats>,
        active_title: &str,
        success_title: &str,
        failure_title: &str,
        transient: bool,
    ) -> Self {
        let base = stats.snapshot();
        let state = Arc::new(Mutex::new(SourceState {
            header,
            active_title: active_title.into(),
            success_title: success_title.into(),
            failure_title: failure_title.into(),
            rows: Vec::new(),
            started: Instant::now(),
            done: false,
            succeeded: false,
            transient,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = if fancy() {
            println!();
            let state_t = Arc::clone(&state);
            let stop_t = Arc::clone(&stop);
            let stats_t = Arc::clone(&stats);
            Some(thread::spawn(move || {
                source_ticker(state_t, stop_t, stats_t, base)
            }))
        } else if !transient {
            activity(active_title, &state.lock().unwrap().header);
            None
        } else {
            None
        };
        Self {
            state,
            stop,
            handle,
            stats,
            base,
        }
    }

    pub fn reporter(&self) -> crate::sourcing::SourceProgressReporter {
        let state = Arc::clone(&self.state);
        Arc::new(move |update| {
            update_source_state(
                &state,
                update.key,
                update.title,
                update.detail,
                update.status,
            );
        })
    }

    pub fn enrich_reporter(&self) -> crate::enrich::EnrichProgressReporter {
        let state = Arc::clone(&self.state);
        Arc::new(move |update| {
            update_source_state(
                &state,
                update.key,
                update.title,
                update.detail,
                update.status,
            );
        })
    }

    pub fn finish(mut self, succeeded: bool) -> StatsSnapshot {
        {
            let mut state = self.state.lock().unwrap();
            state.done = true;
            state.succeeded = succeeded;
        }
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        let transient = self.state.lock().unwrap().transient;
        if fancy() && !transient {
            println!();
        }
        self.stats.snapshot().since(self.base)
    }
}

fn update_source_state(
    state: &Arc<Mutex<SourceState>>,
    key: String,
    title: String,
    detail: String,
    status: String,
) {
    let mut view = state.lock().unwrap();
    if let Some(row) = view.rows.iter_mut().find(|row| row.key == key) {
        row.title.clone_from(&title);
        row.detail.clone_from(&detail);
        row.status.clone_from(&status);
    } else {
        view.rows.push(SourceRow {
            key,
            title: title.clone(),
            detail: detail.clone(),
            status,
        });
    }
    if !fancy() && !view.transient {
        println!("• {title}");
        for line in detail.lines() {
            println!("  └ {line}");
        }
    }
}

fn source_ticker(
    state: Arc<Mutex<SourceState>>,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
    base: StatsSnapshot,
) {
    let mut previous_lines = 0usize;
    let mut frame = 0usize;
    loop {
        let done = stop.load(Ordering::Relaxed);
        let transient = state.lock().unwrap().transient;
        if done && transient {
            let out = std::io::stdout();
            let mut lock = out.lock();
            if previous_lines > 0 {
                let _ = write!(lock, "\x1b[{previous_lines}A");
            }
            let _ = write!(lock, "\r\x1b[J");
            let _ = lock.flush();
            break;
        }
        let lines = render_source(&state.lock().unwrap(), &stats, base, frame);
        let out = std::io::stdout();
        let mut lock = out.lock();
        if previous_lines > 0 {
            let _ = write!(lock, "\x1b[{previous_lines}A");
        }
        let _ = write!(lock, "\r\x1b[J");
        for line in &lines {
            let _ = writeln!(lock, "{line}");
        }
        let _ = lock.flush();
        previous_lines = lines.len();
        if done {
            break;
        }
        frame += 1;
        thread::sleep(Duration::from_millis(TICK_MS));
    }
}

fn render_source(
    state: &SourceState,
    stats: &Stats,
    base: StatsSnapshot,
    frame: usize,
) -> Vec<String> {
    let spinner = FRAMES[frame % FRAMES.len()];
    let (root_glyph, root_title) = if state.done && state.succeeded {
        (leaf("✓"), state.success_title.as_str())
    } else if state.done {
        (red("×"), state.failure_title.as_str())
    } else {
        (blue("•"), state.active_title.as_str())
    };
    let mut lines = vec![
        format!("{root_glyph} {}", bold(root_title)),
        format!("  {} {}", gray("└"), dim(&state.header)),
        String::new(),
    ];

    for (row_index, row) in state.rows.iter().enumerate() {
        if row_index > 0 {
            lines.push(String::new());
        }
        let (glyph, title) = match row.status.as_str() {
            "complete" => (leaf("✓"), row.title.clone()),
            "warning" => (orange("!"), orange(&row.title)),
            "failed" => (red("×"), red(&row.title)),
            "active" => (dark_blue(spinner), blue(&row.title)),
            _ => (gray("○"), dim(&row.title)),
        };
        lines.push(format!("  {glyph} {title}"));
        let details = row
            .detail
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>();
        for (index, detail) in details.iter().enumerate() {
            let branch = if index + 1 == details.len() {
                "└"
            } else {
                "├"
            };
            lines.push(format!(
                "    {} {}",
                gray(branch),
                dim(&truncate(detail, 108))
            ));
        }
    }

    let snap = stats.snapshot().since(base);
    let footer_glyph = if state.done && state.succeeded {
        leaf("✓")
    } else if state.done {
        red("×")
    } else {
        dark_blue(spinner)
    };
    lines.push(String::new());
    lines.push(format!(
        "  {} {footer_glyph} {}",
        gray("└"),
        dim(&footer(snap, state.started.elapsed()))
    ));
    lines
}

// --- outreach view --------------------------------------------------------

#[derive(Clone)]
struct OutreachRow {
    key: String,
    name: String,
    account: String,
    phase: String,
    state: String,
}

struct OutreachState {
    header: String,
    overall: String,
    rows: Vec<OutreachRow>,
    processed: usize,
    accepted: usize,
    rejected: usize,
    held: usize,
    stopped: usize,
    total: usize,
    started: Instant,
    done: bool,
    succeeded: bool,
}

/// Codex-like multi-line progress for the expensive outreach pipeline. Every
/// recipient owns a row, so concurrent model calls no longer overwrite one
/// opaque spinner message or sit at `0/N` for the entire review cycle.
pub struct OutreachView {
    state: Arc<Mutex<OutreachState>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    stats: Arc<Stats>,
    base: StatsSnapshot,
}

#[derive(Clone, Copy)]
pub enum OutreachCompletion {
    Completed,
    Stopped,
}

impl OutreachView {
    pub fn start(header: String, stats: Arc<Stats>) -> Self {
        let base = stats.snapshot();
        let state = Arc::new(Mutex::new(OutreachState {
            header,
            overall: "Selecting recipients".into(),
            rows: Vec::new(),
            processed: 0,
            accepted: 0,
            rejected: 0,
            held: 0,
            stopped: 0,
            total: 0,
            started: Instant::now(),
            done: false,
            succeeded: false,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = if fancy() {
            println!();
            let state_t = Arc::clone(&state);
            let stop_t = Arc::clone(&stop);
            let stats_t = Arc::clone(&stats);
            Some(thread::spawn(move || {
                outreach_ticker(state_t, stop_t, stats_t, base)
            }))
        } else {
            activity("Drafting outreach", &state.lock().unwrap().header);
            None
        };
        Self {
            state,
            stop,
            handle,
            stats,
            base,
        }
    }

    pub fn reporter(&self) -> crate::outreach::PlanProgressReporter {
        let state = Arc::clone(&self.state);
        Arc::new(move |update| {
            let mut view = state.lock().unwrap();
            view.overall = update.phase.clone();
            view.processed = update.processed;
            view.accepted = update.accepted;
            view.rejected = update.rejected;
            view.held = update.held;
            view.stopped = update.stopped;
            view.total = update.total;
            for recipient in &update.roster {
                if view.rows.iter().all(|row| row.key != recipient.key) {
                    view.rows.push(OutreachRow {
                        key: recipient.key.clone(),
                        name: recipient.name.clone(),
                        account: recipient.account.clone(),
                        phase: "Queued".into(),
                        state: "queued".into(),
                    });
                }
            }
            for key in &update.recipient_keys {
                if let Some(row) = view.rows.iter_mut().find(|row| row.key == *key) {
                    row.phase = update.phase.clone();
                    row.state = update.state.clone();
                    if !update.account.trim().is_empty() {
                        row.account = update.account.clone();
                    }
                }
            }
            if !fancy() {
                println!("• {}", outreach_update_line(&view, &update.recipient_keys));
            }
        })
    }

    pub fn finish(mut self, completion: OutreachCompletion) -> StatsSnapshot {
        {
            let mut state = self.state.lock().unwrap();
            state.done = true;
            let stopped_early = matches!(completion, OutreachCompletion::Stopped);
            state.succeeded = !stopped_early && state.rejected == 0 && state.stopped == 0;
            state.overall = if stopped_early || state.stopped > 0 {
                "Outreach stopped before every recipient completed".into()
            } else if state.accepted == 0 && state.rejected > 0 {
                "Every outreach draft was rejected".into()
            } else if state.rejected > 0 {
                "Outreach finished with rejected drafts".into()
            } else if state.accepted > 0 {
                "Outreach drafts ready".into()
            } else {
                "No outreach drafts needed".into()
            };
        }
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        if fancy() {
            println!();
        }
        self.stats.snapshot().since(self.base)
    }
}

fn outreach_update_line(state: &OutreachState, keys: &[String]) -> String {
    let names = state
        .rows
        .iter()
        .filter(|row| keys.contains(&row.key))
        .map(|row| row.name.as_str())
        .collect::<Vec<_>>()
        .join(" + ");
    if names.is_empty() {
        format!(
            "{} · {}/{} complete",
            state.overall, state.processed, state.total
        )
    } else {
        format!(
            "{names} · {} · {}/{} complete",
            state.overall, state.processed, state.total
        )
    }
}

fn outreach_ticker(
    state: Arc<Mutex<OutreachState>>,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
    base: StatsSnapshot,
) {
    let mut previous_lines = 0usize;
    let mut frame = 0usize;
    loop {
        let done = stop.load(Ordering::Relaxed);
        let lines = render_outreach(&state.lock().unwrap(), &stats, base, frame);
        let out = std::io::stdout();
        let mut lock = out.lock();
        if previous_lines > 0 {
            let _ = write!(lock, "\x1b[{previous_lines}A");
        }
        let _ = write!(lock, "\r\x1b[J");
        for line in &lines {
            let _ = writeln!(lock, "{line}");
        }
        let _ = lock.flush();
        previous_lines = lines.len();
        if done {
            break;
        }
        frame += 1;
        thread::sleep(Duration::from_millis(TICK_MS));
    }
}

fn render_outreach(
    state: &OutreachState,
    stats: &Stats,
    base: StatsSnapshot,
    frame: usize,
) -> Vec<String> {
    let spinner = FRAMES[frame % FRAMES.len()];
    let title = if state.done {
        state.overall.as_str()
    } else {
        "Drafting outreach"
    };
    let title_glyph = if state.done && state.succeeded {
        leaf("✓")
    } else if state.done && (state.stopped > 0 || state.accepted > 0) {
        orange("!")
    } else if state.done {
        red("×")
    } else {
        blue("•")
    };
    let mut lines = vec![
        format!("{title_glyph} {}", bold(title)),
        format!("  {} {}", gray("└"), dim(&state.header)),
        String::new(),
    ];
    // Repainting more rows than the terminal viewport can hold makes ANSI
    // cursor-up drift after the terminal scrolls: every spinner frame then
    // becomes permanent transcript output. Keep the live block compact and
    // show the complete roster once in the final frame.
    let visible_rows = visible_outreach_rows(state, 6);
    let hidden_rows = state.rows.len().saturating_sub(visible_rows.len());
    let mut previous_account: Option<&str> = None;
    for row in visible_rows {
        if previous_account != Some(row.account.as_str()) {
            if previous_account.is_some() {
                lines.push(String::new());
            }
            lines.push(format!(
                "  {} {}",
                gray("•"),
                bold(&truncate(&row.account, 72))
            ));
        }
        previous_account = Some(&row.account);
        let (glyph, phase, detail) = match row.state.as_str() {
            "accepted" => (leaf("✓"), leaf(&row.phase), None),
            "rejected" => (
                red("×"),
                red("rejected; feedback saved"),
                failure_detail(&row.phase),
            ),
            "held" => (orange("○"), orange(&row.phase), None),
            "stopped" => (
                orange("!"),
                orange("stopped; details below"),
                failure_detail(&row.phase),
            ),
            "active" => {
                let phase = row.phase.to_ascii_lowercase();
                let color = if ["council", "review", "qa", "gate"]
                    .iter()
                    .any(|needle| phase.contains(needle))
                {
                    orange(&row.phase)
                } else {
                    blue(&row.phase)
                };
                (dark_blue(spinner), color, None)
            }
            _ => (gray("○"), dim(&row.phase), None),
        };
        lines.push(format!(
            "    {} {}  {}",
            glyph,
            pad(&truncate(&row.name, 22), 22),
            phase
        ));
        if let Some(detail) = detail {
            for (index, part) in wrap_words(&detail, 86).into_iter().enumerate() {
                lines.push(format!(
                    "      {} {}",
                    gray(if index == 0 { "└" } else { " " }),
                    red(&part)
                ));
            }
        }
    }
    if hidden_rows > 0 {
        lines.push(format!(
            "  {} {}",
            gray("└"),
            dim(&format!(
                "{hidden_rows} queued or completed recipient(s) hidden while live"
            ))
        ));
    }
    let snapshot = stats.snapshot().since(base);
    lines.push(String::new());
    lines.push(format!(
        "  {} {}  {}",
        if state.done && state.succeeded {
            leaf("✓")
        } else if state.done && (state.stopped > 0 || state.accepted > 0) {
            orange("!")
        } else if state.done {
            red("×")
        } else {
            gray(spinner)
        },
        dim(&format!(
            "{}/{} ready · {} held · {} rejected · {} stopped",
            state.accepted, state.total, state.held, state.rejected, state.stopped
        )),
        dim(&footer(snapshot, state.started.elapsed()))
    ));
    lines
}

fn visible_outreach_rows(state: &OutreachState, limit: usize) -> Vec<&OutreachRow> {
    if state.done || state.rows.len() <= limit {
        return state.rows.iter().collect();
    }

    let mut keys = Vec::new();
    let mut add = |row: &OutreachRow| {
        if keys.len() < limit && !keys.iter().any(|key| key == &row.key) {
            keys.push(row.key.clone());
        }
    };

    // Active work is always visible. Then retain the newest decisions so the
    // user can see useful progress, and use any remaining room for what is next.
    for row in state.rows.iter().filter(|row| row.state == "active") {
        add(row);
    }
    for row in state.rows.iter().rev().filter(|row| {
        matches!(
            row.state.as_str(),
            "accepted" | "held" | "rejected" | "stopped"
        )
    }) {
        add(row);
    }
    for row in state.rows.iter().filter(|row| row.state == "queued") {
        add(row);
    }

    state
        .rows
        .iter()
        .filter(|row| keys.contains(&row.key))
        .collect()
}

// --- turn view (quiet structured router) ----------------------------------

/// Keep routing responsive without exposing the model's chain-of-thought or an
/// unreliable pre-action success claim. Once routing returns, `Agent` prints a
/// deterministic intent derived from the accepted structured action.
pub struct TurnView {
    spinner: Option<Spinner>,
}

impl Default for TurnView {
    fn default() -> Self {
        Self::new()
    }
}

impl TurnView {
    pub fn new() -> Self {
        TurnView {
            spinner: Some(Spinner::start("Understanding request")),
        }
    }

    /// Feed one streaming event. Content is deliberately ignored: the router's
    /// private reasoning is not part of the user transcript.
    pub fn on_event(&mut self, ev: StreamEvent) {
        match ev {
            StreamEvent::BlockStart("tool_use") => self.set_spinner("Choosing action"),
            StreamEvent::BlockStart(_) => self.set_spinner("Understanding request"),
            StreamEvent::ToolInputDelta(_)
            | StreamEvent::TextDelta(_)
            | StreamEvent::ThinkingDelta(_)
            | StreamEvent::ToolUse { .. }
            | StreamEvent::BlockStop => {}
        }
    }

    fn set_spinner(&mut self, msg: &str) {
        match &self.spinner {
            Some(sp) => sp.set(msg),
            None => self.spinner = Some(Spinner::start(msg)),
        }
    }

    /// Finish routing. No model text was exposed, so the caller should render
    /// either the structured reply or a concise deterministic action intent.
    pub fn finish(mut self) -> bool {
        self.spinner.take();
        false
    }
}

/// Extract the (possibly partial) value of a top-level JSON string field from a
/// streaming buffer. Returns the decoded value so far and whether it's complete
/// (closing quote seen). Handles the common escapes and tolerates a truncated
/// tail (mid-escape / mid-value while still streaming).
#[cfg(test)]
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
    use std::time::Instant;

    use super::{
        json_string_prefix, markdown_line, render_outreach, render_source, session_header_lines,
        visible_outreach_rows, OutreachRow, OutreachState, SourceRow, SourceState,
    };
    use crate::engine::{Stats, StatsSnapshot};

    #[test]
    fn markdown_strips_markers_when_emphasis_off() {
        // Dimmed reasoning: markers must vanish, never leak as literal characters.
        assert_eq!(
            markdown_line("**Running now:** Apollo `search`", false),
            "Running now: Apollo search"
        );
        assert_eq!(
            markdown_line("- reveal & verify", false),
            "• reveal & verify"
        );
        assert_eq!(
            markdown_line("### Then the pipeline", false),
            "Then the pipeline"
        );
    }

    #[test]
    fn markdown_consumes_markers_when_emphasis_on() {
        // ANSI styling is TTY-gated, but the markers must be consumed regardless
        // so nothing leaks as literal `**`/`` ` ``.
        let out = markdown_line("**Running now:** Apollo `search`", true);
        assert!(
            !out.contains("**"),
            "bold markers should be consumed: {out:?}"
        );
        assert!(
            !out.contains('`'),
            "code markers should be consumed: {out:?}"
        );
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

    #[test]
    fn outreach_progress_keeps_each_recipient_and_truthful_counts_visible() {
        let state = OutreachState {
            header: "OutageHub · 4 touches each · drafts only".into(),
            overall: "sales council vote".into(),
            rows: vec![
                OutreachRow {
                    key: "cory".into(),
                    name: "Cory".into(),
                    account: "Conestoga Cold Storage".into(),
                    phase: "sales council vote · round 1/3".into(),
                    state: "active".into(),
                },
                OutreachRow {
                    key: "derrick".into(),
                    name: "Derrick".into(),
                    account: "Conestoga Cold Storage".into(),
                    phase: "ready in CRM".into(),
                    state: "accepted".into(),
                },
            ],
            processed: 1,
            accepted: 1,
            rejected: 0,
            held: 0,
            stopped: 0,
            total: 2,
            started: Instant::now(),
            done: false,
            succeeded: false,
        };
        let lines = render_outreach(&state, &Stats::default(), StatsSnapshot::default(), 0);
        let output = lines.join("\n");
        assert!(output.contains("Cory"));
        assert!(output.contains("Derrick"));
        assert!(output.contains("sales council vote"));
        assert!(output.contains("1/2 ready · 0 held · 0 rejected"));
    }

    #[test]
    fn live_outreach_view_is_bounded_but_final_view_is_complete() {
        let rows = (0..25)
            .map(|index| OutreachRow {
                key: format!("person-{index}"),
                name: format!("Person {index}"),
                account: format!("Account {}", index / 5),
                phase: if index < 3 {
                    "writing".into()
                } else {
                    "Queued".into()
                },
                state: if index < 3 {
                    "active".into()
                } else {
                    "queued".into()
                },
            })
            .collect::<Vec<_>>();
        let mut state = OutreachState {
            header: "GnK · 5×5×7".into(),
            overall: "writing".into(),
            rows,
            processed: 0,
            accepted: 0,
            rejected: 0,
            held: 0,
            stopped: 0,
            total: 25,
            started: Instant::now(),
            done: false,
            succeeded: false,
        };
        assert_eq!(visible_outreach_rows(&state, 6).len(), 6);
        let live =
            render_outreach(&state, &Stats::default(), StatsSnapshot::default(), 0).join("\n");
        assert!(live.contains("19 queued or completed recipient(s) hidden while live"));

        state.done = true;
        assert_eq!(visible_outreach_rows(&state, 6).len(), 25);
    }

    #[test]
    fn outreach_completion_does_not_call_all_rejected_copy_finished() {
        let state = OutreachState {
            header: "Wapahki · drafts only".into(),
            overall: "Every outreach draft was rejected".into(),
            rows: vec![OutreachRow {
                key: "one".into(),
                name: "Aldrin".into(),
                account: "Delmar Foods".into(),
                phase: "rejected: deterministic QA failed".into(),
                state: "rejected".into(),
            }],
            processed: 1,
            accepted: 0,
            rejected: 1,
            held: 0,
            stopped: 0,
            total: 1,
            started: Instant::now(),
            done: true,
            succeeded: false,
        };
        let output =
            render_outreach(&state, &Stats::default(), StatsSnapshot::default(), 0).join("\n");
        assert!(output.contains("Every outreach draft was rejected"));
        assert!(output.contains("0/1 ready · 0 held · 1 rejected · 0 stopped"));
        assert!(!output.contains("Outreach drafts finished"));
    }

    #[test]
    fn outreach_rejection_keeps_the_full_reason_visible_under_the_recipient() {
        let reason = "rejected: copy still failed after two targeted repair rounds: stage 1 asks more than one question | stage 5 is 46 words (needs 18–45) | the recipient has no self-interested reason to answer a seven-part research questionnaire";
        let state = OutreachState {
            header: "Wapahki · drafts only".into(),
            overall: "Outreach finished with rejected drafts".into(),
            rows: vec![OutreachRow {
                key: "one".into(),
                name: "Safiullah".into(),
                account: "Give and Go Prepared Foods".into(),
                phase: reason.into(),
                state: "rejected".into(),
            }],
            processed: 1,
            accepted: 0,
            rejected: 1,
            held: 0,
            stopped: 0,
            total: 1,
            started: Instant::now(),
            done: true,
            succeeded: false,
        };
        let lines = render_outreach(&state, &Stats::default(), StatsSnapshot::default(), 0);
        let output = lines.join("\n");
        assert!(output.contains("Give and Go Prepared Foods"));
        assert!(output.contains("rejected; feedback saved"));
        assert!(output.contains("copy still failed after two targeted repair rounds"));
        assert!(output.contains("seven-part research questionnaire"));
        assert!(lines.iter().any(|line| line.trim_start().starts_with("└ ")));
    }

    #[test]
    fn provider_limits_are_distinct_from_copy_rejections() {
        let state = OutreachState {
            header: "Wapahki · drafts only".into(),
            overall: "Outreach stopped before every recipient completed".into(),
            rows: vec![OutreachRow {
                key: "one".into(),
                name: "Arezou".into(),
                account: "Freshstone".into(),
                phase: "stopped; model usage limit reached".into(),
                state: "stopped".into(),
            }],
            processed: 1,
            accepted: 0,
            rejected: 0,
            held: 0,
            stopped: 1,
            total: 1,
            started: Instant::now(),
            done: true,
            succeeded: false,
        };
        let output =
            render_outreach(&state, &Stats::default(), StatsSnapshot::default(), 0).join("\n");
        assert!(output.contains("stopped; model usage limit reached"));
        assert!(output.contains("0 rejected · 1 stopped"));
    }

    #[test]
    fn sourcing_progress_uses_stable_nested_milestones() {
        let state = SourceState {
            header: "Wapahki · 5 account target · 5 people each · active GTM play".into(),
            active_title: "Sourcing companies".into(),
            success_title: "Sourced companies".into(),
            failure_title: "Sourcing stopped".into(),
            rows: vec![
                SourceRow {
                    key: "icp".into(),
                    title: "Built ICP".into(),
                    detail: "12 keywords · 13 buyer titles\nEmployee ranges: 51,200, 201,500"
                        .into(),
                    status: "complete".into(),
                },
                SourceRow {
                    key: "qualification".into(),
                    title: "Qualifying root-cause fit".into(),
                    detail: "3/10 reviewed · 1 qualified · 1 research-needed · 1 skipped".into(),
                    status: "active".into(),
                },
            ],
            started: Instant::now(),
            done: false,
            succeeded: false,
            transient: false,
        };
        let output =
            render_source(&state, &Stats::default(), StatsSnapshot::default(), 0).join("\n");
        assert!(output.contains("Built ICP"));
        assert!(output.contains("Employee ranges"));
        assert!(output.contains("Qualifying root-cause fit"));
        assert!(output.contains("research-needed"));
        assert!(output.contains("├") && output.contains("└"));
        assert!(output.contains("\n\n"));
    }

    #[test]
    fn enrichment_progress_is_one_aggregate_milestone() {
        let state = SourceState {
            header: "Wapahki · 25 contacts · Apollo reveal and verification".into(),
            active_title: "Enriching contacts".into(),
            success_title: "Enriched contacts".into(),
            failure_title: "Enrichment stopped".into(),
            rows: vec![SourceRow {
                key: "enrichment".into(),
                title: "Revealing and verifying contacts".into(),
                detail: "14/25 processed · 12 verified · 2 no email · 0 errors\nLatest: Arezou · verified".into(),
                status: "active".into(),
            }],
            started: Instant::now(),
            done: false,
            succeeded: false,
            transient: false,
        };
        let output =
            render_source(&state, &Stats::default(), StatsSnapshot::default(), 0).join("\n");
        assert_eq!(
            output.matches("Revealing and verifying contacts").count(),
            1
        );
        assert!(output.contains("14/25 processed · 12 verified"));
        assert!(output.contains("Latest: Arezou · verified"));
    }
}

// --- campaign view (live pipeline progress tree) ---------------------------

enum Stage {
    Researching,
    Building,
    Done,
    Failed,
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
            println!();
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
    pub fn finish(mut self, succeeded: bool) -> StatsSnapshot {
        self.state.lock().unwrap().stage = if succeeded {
            Stage::Done
        } else {
            Stage::Failed
        };
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        if fancy() {
            println!();
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
    let title = match state.stage {
        Stage::Done => "Ran campaign",
        Stage::Failed => "Campaign failed",
        Stage::Researching | Stage::Building => "Running campaign",
    };
    let mut lines = vec![
        format!("{}{}", blue("• "), bold(title)),
        format!("{}{}", dim("  └ "), dim(&state.header)),
    ];

    match state.stage {
        Stage::Researching => {
            lines.push(format!(
                "    {} {}",
                dark_blue(sp),
                blue("Researching accounts…")
            ));
        }
        Stage::Building | Stage::Done | Stage::Failed => {
            for a in &state.accounts {
                let name = pad(&a.name, 30);
                if a.contacts == 0 {
                    lines.push(format!(
                        "    {} {} {}",
                        dark_blue(sp),
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
                        dark_blue(sp),
                        name,
                        dim(&format!("{}/{} sequences", a.seqs_done, a.contacts))
                    ));
                }
            }
        }
    }

    let snap = stats.snapshot().since(base);
    let glyph = match state.stage {
        Stage::Done => leaf("✓"),
        Stage::Failed => red("×"),
        Stage::Researching | Stage::Building => gray(sp),
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

fn failure_detail(phase: &str) -> Option<String> {
    let detail = phase
        .trim()
        .strip_prefix("rejected:")
        .or_else(|| phase.trim().strip_prefix("stopped:"))
        .unwrap_or_else(|| phase.trim())
        .trim();
    if detail.is_empty()
        || matches!(
            detail,
            "feedback saved" | "rejected; feedback saved" | "stopped; details below"
        )
    {
        None
    } else {
        Some(detail.to_string())
    }
}

fn wrap_words(text: &str, width: usize) -> Vec<String> {
    let width = width.max(16);
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        let extra = usize::from(!current.is_empty());
        if !current.is_empty() && current.chars().count() + extra + word.chars().count() > width {
            lines.push(current);
            current = String::new();
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}
