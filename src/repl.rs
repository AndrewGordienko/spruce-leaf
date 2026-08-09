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
use crate::engine::Backend;
use crate::ui;

const COMMANDS: &[(&str, &str)] = &[
    ("/crm", "/crm"),
    ("/gtm", "/gtm"),
    ("/brand ", "/brand [key]"),
    ("/model ", "/model [codex|claude|grok] [id|default]"),
    ("/login ", "/login [gnk|wapahki|outagehub]"),
    ("/logout ", "/logout [brand]"),
    ("/mail", "/mail"),
    ("/mail-sync", "/mail-sync [brand]"),
    ("/usage", "/usage"),
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
    ui::clear_terminal();
    banner(&agent);

    loop {
        ui::context_line(
            agent.backend(),
            &agent.model(),
            &agent.brand_label(),
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
                    if handle_command(rt, &mut agent, cmd) {
                        break;
                    }
                    continue;
                }

                let usage_base = agent.usage_snapshot();
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
                ui::activity("Model usage", agent.usage_since(usage_base));
                show_model_switches(&agent);
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
fn handle_command(rt: &Runtime, agent: &mut Agent, cmd: &str) -> bool {
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
        "gtm" => {
            let url = format!("{}/gtm/{}", agent.crm_url(), agent.brand());
            open_browser(&url);
            ui::activity("Opened GTM engineering lab", &url);
            println!();
        }
        "clear" => {
            agent.reset();
            ui::clear_terminal();
            banner(agent);
        }
        "brand" | "brands" => {
            let arg = cmd.split_whitespace().nth(1).unwrap_or("");
            if arg.is_empty() {
                let status = if agent.is_brand_pinned() {
                    format!("Pinned to {}", agent.brand())
                } else {
                    "Auto-detect (brand-agnostic)".to_string()
                };
                ui::activity(
                    &format!("Brand mode: {status}"),
                    format!(
                        "Available: {}\nUse /brand <key> to pin one brand, /brand auto to go agnostic",
                        agent.brand_keys().join(", ")
                    ),
                );
                println!();
            } else if matches!(arg, "auto" | "any" | "off" | "portfolio" | "all") {
                agent.unpin_brand();
                ui::activity(
                    "Brand mode",
                    "auto-detect (agnostic) — inferred per request",
                );
                println!();
            } else if agent.pin_brand(arg) {
                ui::activity("Pinned brand", agent.brand());
                println!();
            } else {
                ui::assistant_message(&format!(
                    "Unknown brand '{arg}'. Available: {} (or 'auto' to go agnostic)",
                    agent.brand_keys().join(", ")
                ));
                println!();
            }
        }
        "model" | "models" => model_command(agent, cmd),
        "login" => login_command(rt, agent, cmd),
        "logout" => logout_command(agent, cmd),
        "mail" | "gmail" => mail_status_command(agent),
        "mail-sync" | "sync-mail" | "syncmail" => mail_sync_command(rt, agent, cmd),
        "usage" | "tokens" => {
            ui::activity("Model usage", agent.usage_report());
            println!();
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
        &agent.model(),
        &agent.brand_label(),
        &directory,
        &agent.crm_url(),
    );
    println!();
}

fn help() {
    ui::activity(
        "Shortcuts",
        "/crm          Open the CRM (Pipeline + Strategy + GTM Lab)\n\
         /gtm          Open signals, plays, root-cause ranking, experiments, and proofs\n\
         /brand [key]  Pin a brand, or /brand auto for agnostic (default)\n\
         /model [provider] [id|default]  Switch Claude/Codex/Grok\n\
         /login [brand]  Open browser to connect that brand's Gmail\n\
         /logout [brand] Disconnect a linked Gmail\n\
         /mail           Show which brand Gmail accounts are linked\n\
         /mail-sync [brand]  Pull inbox+sent into CRM learnings\n\
         /usage        Show input/cache/output usage by stage\n\
         /clear        Clear the conversation; keep CRM data\n\
         /help         Show this list\n\
         /quit         Exit (or Ctrl-D)\n\
         Tab           Complete a slash command",
    );
    println!();
}

fn login_command(rt: &Runtime, agent: &Agent, cmd: &str) {
    let brand = cmd
        .split_whitespace()
        .nth(1)
        .map(str::to_ascii_lowercase)
        .filter(|b| !b.is_empty());
    let Some(brand) = brand else {
        ui::activity(
            "Gmail login",
            format!(
                "Link each brand mailbox via browser OAuth (no Gmail settings changes):\n\
                 /login gnk\n/login wapahki\n/login outagehub\n\n{}",
                crate::google_oauth::GoogleTokenSet::status_report(&agent.brand_keys())
            ),
        );
        println!();
        return;
    };
    if !agent.brand_keys().iter().any(|k| *k == brand) {
        ui::assistant_message(&format!(
            "Unknown brand '{brand}'. Available: {}",
            agent.brand_keys().join(", ")
        ));
        println!();
        return;
    }

    ui::activity(
        "Gmail login",
        format!("Opening browser to link {brand}… sign in as that brand's mailbox"),
    );
    println!();

    match rt.block_on(crate::google_oauth::login_brand(&brand, |url| {
        open_browser(url);
    })) {
        Ok(set) => {
            ui::activity(
                "Gmail linked",
                format!(
                    "{brand} → {}\nTokens saved under .spruce/google/{brand}.json\n\
                     Next: /mail-sync {brand}  (or /mail-sync for all)",
                    set.email
                ),
            );
            println!();
        }
        Err(e) => {
            ui::assistant_message(&format!("Login failed: {e:#}"));
            println!();
        }
    }
}

fn logout_command(agent: &Agent, cmd: &str) {
    let brand = cmd
        .split_whitespace()
        .nth(1)
        .map(str::to_ascii_lowercase)
        .filter(|b| !b.is_empty());
    let Some(brand) = brand else {
        ui::activity(
            "Gmail logout",
            format!(
                "Usage: /logout <brand>\n{}",
                crate::google_oauth::GoogleTokenSet::status_report(&agent.brand_keys())
            ),
        );
        println!();
        return;
    };
    match crate::google_oauth::GoogleTokenSet::delete(&brand) {
        Ok(true) => {
            ui::activity("Gmail disconnected", format!("{brand} tokens removed"));
            println!();
        }
        Ok(false) => {
            ui::activity("Gmail logout", format!("{brand} was not linked"));
            println!();
        }
        Err(e) => {
            ui::assistant_message(&format!("Logout failed: {e:#}"));
            println!();
        }
    }
}

fn mail_status_command(agent: &Agent) {
    ui::activity(
        "Gmail accounts",
        crate::google_oauth::GoogleTokenSet::status_report(&agent.brand_keys()),
    );
    println!();
}

fn mail_sync_command(rt: &Runtime, agent: &Agent, cmd: &str) {
    let brand = cmd
        .split_whitespace()
        .nth(1)
        .map(str::to_ascii_lowercase)
        .filter(|b| !b.is_empty());
    if let Some(ref b) = brand {
        if !agent.brand_keys().iter().any(|k| *k == *b) {
            ui::assistant_message(&format!(
                "Unknown brand '{b}'. Available: {}",
                agent.brand_keys().join(", ")
            ));
            println!();
            return;
        }
    }

    let work = ui::Spinner::start("Syncing Gmail");
    let result = rt.block_on(crate::gmail::sync_all(agent.db(), brand.as_deref(), 40));
    drop(work);
    match result {
        Ok(summaries) => {
            ui::activity("Gmail sync", crate::gmail::format_sync_report(&summaries));
            println!();
        }
        Err(e) => {
            ui::assistant_message(&format!("Gmail sync failed: {e:#}"));
            println!();
        }
    }
}

fn model_command(agent: &Agent, cmd: &str) {
    let mut args = cmd.split_whitespace().skip(1);
    let Some(provider) = args.next() else {
        ui::activity(
            &format!("Active model: {} · {}", agent.backend(), agent.model()),
            "Use /model openai, /model codex, /model claude, or /model grok to switch\n\
             Optional: /model <provider> <model-id|default>",
        );
        println!();
        return;
    };

    let backend = match provider.to_ascii_lowercase().as_str() {
        "openai" | "gpt" => Backend::Openai,
        "codex" => Backend::Codex,
        "claude" => Backend::Claude,
        "grok" => Backend::Grok,
        "toggle" | "switch" => match agent.backend() {
            "openai" => Backend::Claude,
            "claude" => Backend::Codex,
            "codex" => Backend::Grok,
            _ => Backend::Openai,
        },
        _ => {
            ui::assistant_message(&format!(
                "Unknown model provider '{provider}'. Use /model openai, /model codex, /model claude, or /model grok."
            ));
            println!();
            return;
        }
    };

    if let Some(model) = args.next() {
        let model = (!model.eq_ignore_ascii_case("default")).then(|| model.to_string());
        agent.select_model(backend, model);
    } else {
        agent.select_backend(backend);
    }

    ui::activity(
        "Switched model",
        format!(
            "{} · {}\nOne-call fallback is available for routing; it never changes the bulk-work provider",
            agent.backend(),
            agent.model()
        ),
    );
    println!();
}

fn show_model_switches(agent: &Agent) {
    for switch in agent.take_model_switches() {
        ui::activity(
            "Model usage fallback",
            format!(
                "{} usage exhausted; switched to {} · {}",
                switch.from, switch.to, switch.model
            ),
        );
        println!();
    }
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
