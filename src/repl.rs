//! The interactive `spruce-leaf` composer.
//!
//! Rustyline keeps editing, history, multiline paste, and terminal restoration
//! dependable. This layer adds the Codex-like surface: a `›` composer, ghost
//! placeholder, slash completion, and compact transcript cells.

use std::borrow::Cow;

use anyhow::Result;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::{Hint, Hinter};
use rustyline::history::DefaultHistory;
use rustyline::validate::Validator;
use rustyline::{Context, Editor, Helper};
use tokio::runtime::Runtime;

use crate::agent::{open_browser, Agent};
use crate::ui;

const COMMANDS: &[(&str, &str)] = &[
    ("/crm", "/crm"),
    ("/brand ", "/brand [key]"),
    ("/clear", "/clear"),
    ("/help", "/help"),
    ("/quit", "/quit"),
];

struct PlaceholderHint(&'static str);

impl Hint for PlaceholderHint {
    fn display(&self) -> &str {
        self.0
    }

    fn completion(&self) -> Option<&str> {
        None
    }
}

struct ReplHelper;

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let typed = &line[..pos];
        if !typed.starts_with('/') {
            return Ok((0, Vec::new()));
        }
        let matches = COMMANDS
            .iter()
            .filter(|(replacement, _)| replacement.starts_with(typed))
            .map(|(replacement, display)| Pair {
                display: (*display).to_string(),
                replacement: (*replacement).to_string(),
            })
            .collect();
        Ok((0, matches))
    }
}

impl Hinter for ReplHelper {
    type Hint = PlaceholderHint;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        (line.is_empty() && pos == 0).then_some(PlaceholderHint("Ask Spruce Leaf to do anything"))
    }
}

impl Highlighter for ReplHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default && ui::fancy() {
            Cow::Owned(ui::bold(prompt))
        } else {
            Cow::Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        if ui::fancy() {
            Cow::Owned(ui::dim(hint))
        } else {
            Cow::Borrowed(hint)
        }
    }
}

impl Validator for ReplHelper {}
impl Helper for ReplHelper {}

pub fn run_repl(rt: &Runtime, mut agent: Agent) -> Result<()> {
    let mut editor = Editor::<ReplHelper, DefaultHistory>::new()?;
    editor.set_helper(Some(ReplHelper));
    banner(&agent);

    loop {
        ui::context_line(
            agent.backend(),
            agent.model(),
            agent.brand(),
            &directory_label(),
        );
        match editor.readline("› ") {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(line);

                if let Some(cmd) = line.strip_prefix('/') {
                    if handle_command(&mut agent, cmd) {
                        break;
                    }
                    continue;
                }

                match rt.block_on(agent.handle(line)) {
                    Ok(reply) => {
                        let reply = reply.trim();
                        if !reply.is_empty() {
                            ui::assistant_message(reply);
                        }
                        println!();
                    }
                    Err(error) => {
                        ui::assistant_message(&format!("Error: {error:#}"));
                        println!();
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                ui::assistant_message("Press Ctrl-D or type /quit to exit.");
                println!();
            }
            Err(ReadlineError::Eof) => {
                ui::assistant_message("See you soon 🌲");
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Returns `true` if the REPL should exit.
fn handle_command(agent: &mut Agent, cmd: &str) -> bool {
    match cmd.split_whitespace().next().unwrap_or("") {
        "quit" | "exit" | "q" => {
            ui::assistant_message("See you soon 🌲");
            return true;
        }
        "help" | "h" | "?" => help(),
        "crm" => {
            let url = agent.crm_url();
            open_browser(&url);
            ui::activity("Opened CRM dashboard", &url);
            println!();
        }
        "clear" => {
            agent.reset();
            if ui::fancy() {
                print!("\x1b[2J\x1b[H");
            }
            banner(agent);
        }
        "brand" | "brands" => {
            let arg = cmd.split_whitespace().nth(1).unwrap_or("");
            if arg.is_empty() {
                ui::activity(
                    &format!("Active brand: {}", agent.brand()),
                    format!(
                        "Available: {}\nUse /brand <key> to switch",
                        agent.brand_keys().join(", ")
                    ),
                );
                println!();
            } else if agent.set_brand(arg) {
                ui::activity("Switched brand", agent.brand());
                println!();
            } else {
                ui::assistant_message(&format!(
                    "Unknown brand '{arg}'. Available: {}",
                    agent.brand_keys().join(", ")
                ));
                println!();
            }
        }
        other => {
            ui::assistant_message(&format!("Unknown command: /{other}. Try /help."));
            println!();
        }
    }
    false
}

fn banner(agent: &Agent) {
    let directory = std::env::current_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| ".".to_string());
    ui::session_header(
        agent.backend(),
        agent.model(),
        agent.brand(),
        &directory,
        &agent.crm_url(),
    );
    println!();
}

fn help() {
    ui::activity(
        "Shortcuts",
        "/crm          Open the CRM dashboard\n\
         /brand [key]  Show or switch the active brand\n\
         /clear        Clear the conversation; keep CRM data\n\
         /help         Show this list\n\
         /quit         Exit (or Ctrl-D)\n\
         Tab           Complete a slash command",
    );
    println!();
}

fn directory_label() -> String {
    std::env::current_dir()
        .ok()
        .and_then(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| ".".to_string())
}
