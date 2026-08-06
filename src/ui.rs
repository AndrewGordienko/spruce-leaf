//! The polished terminal layer — the part that makes spruce-leaf feel like the
//! Claude/Codex CLI rather than a script printing lines.
//!
//! Three pieces:
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
                        "\r\x1b[K{} {}  {}",
                        leaf(FRAMES[i % FRAMES.len()]),
                        text,
                        dim(&format!("{secs:.1}s"))
                    );
                    let _ = std::io::stdout().flush();
                    i += 1;
                    thread::sleep(Duration::from_millis(TICK_MS));
                }
            }))
        } else {
            println!("… {initial}");
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
    Text,
}

/// Renders one streaming agent turn: a "thinking…" spinner while the model
/// reasons and picks an action, then its narration streamed live.
pub struct TurnView {
    spinner: Option<Spinner>,
    phase: TurnPhase,
    thought_since: Option<Instant>,
    any_text: bool,
}

impl Default for TurnView {
    fn default() -> Self {
        Self::new()
    }
}

impl TurnView {
    pub fn new() -> Self {
        TurnView { spinner: None, phase: TurnPhase::Idle, thought_since: None, any_text: false }
    }

    /// Feed one streaming event. Wire this straight into `Claude::stream`.
    pub fn on_event(&mut self, ev: StreamEvent) {
        match ev {
            StreamEvent::BlockStart(kind) => match kind {
                "thinking" => {
                    self.thought_since.get_or_insert_with(Instant::now);
                    self.set_spinner("thinking");
                    self.phase = TurnPhase::Thinking;
                }
                "tool_use" => self.set_spinner("deciding"),
                "text" => self.begin_text(),
                _ => {}
            },
            StreamEvent::TextDelta(s) => {
                if !matches!(self.phase, TurnPhase::Text) {
                    self.begin_text();
                }
                self.any_text = true;
                print!("{s}");
                let _ = std::io::stdout().flush();
            }
            // Thinking deltas come through redacted (empty) via the CLI; the
            // spinner is what conveys "the model is reasoning".
            StreamEvent::ThinkingDelta(_)
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

    fn begin_text(&mut self) {
        self.spinner.take(); // stop + clear the spinner line
        if let Some(t) = self.thought_since.take() {
            println!("{}", dim(&format!("✳ thought for {:.1}s", t.elapsed().as_secs_f32())));
        }
        self.phase = TurnPhase::Text;
    }

    /// Finish the turn; returns whether any visible answer text was streamed.
    pub fn finish(mut self) -> bool {
        self.spinner.take();
        if self.any_text {
            println!();
        }
        self.any_text
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
    /// `run_campaign  gnk · logistics chargebacks · 5×5×7`.
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
            Some(thread::spawn(move || ticker(state_t, stop_t, stats_t, base)))
        } else {
            // Non-TTY: print a single static "started" line.
            println!("● {}", state.lock().unwrap().header);
            None
        };

        CampaignView { state, stop, handle, stats, base }
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
            .map(|n| AcctProg { name: n.clone(), contacts: 0, seqs_done: 0 })
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
    let mut lines = vec![format!("{} {}", leaf("●"), bold(&state.header))];

    match state.stage {
        Stage::Researching => {
            lines.push(format!("  {} {}", cyan(sp), dim("researching accounts…")));
        }
        Stage::Building | Stage::Done => {
            for a in &state.accounts {
                let name = pad(&a.name, 30);
                if a.contacts == 0 {
                    lines.push(format!("  {} {} {}", cyan(sp), name, dim("mapping contacts…")));
                } else if a.seqs_done >= a.contacts {
                    lines.push(format!(
                        "  {} {} {}",
                        leaf("✓"),
                        name,
                        dim(&format!("{} contacts", a.contacts))
                    ));
                } else {
                    lines.push(format!(
                        "  {} {} {}",
                        cyan(sp),
                        name,
                        dim(&format!("{}/{} sequences", a.seqs_done, a.contacts))
                    ));
                }
            }
        }
    }

    let snap = stats.snapshot().since(base);
    let glyph = if matches!(state.stage, Stage::Done) { leaf("✓") } else { gray(sp) };
    lines.push(format!("  {} {}", glyph, dim(&footer(snap, state.started.elapsed()))));
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
/// `run_campaign  gnk · logistics chargebacks · 5×5×7`.
pub fn campaign_header(brand: &str, thesis: &str, accounts: usize, contacts: usize, touches: usize) -> String {
    format!(
        "run_campaign  {brand} · {} · {accounts}×{contacts}×{touches}",
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
