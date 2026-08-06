//! The interactive `spruce-leaf` prompt.
//!
//! A blocking readline loop on the main thread; each non-command line is sent
//! to the agent via the shared Tokio runtime. Slash commands are handled
//! locally.

use anyhow::Result;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use tokio::runtime::Runtime;

use crate::agent::{open_browser, Agent};

pub fn run_repl(rt: &Runtime, mut agent: Agent) -> Result<()> {
    let mut ed = DefaultEditor::new()?;
    banner(&agent);

    loop {
        match ed.readline("spruce-leaf \u{203a} ") {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = ed.add_history_entry(line);

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
                            println!("{reply}\n");
                        }
                    }
                    Err(e) => println!("error: {e:#}\n"),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("(ctrl-c \u{2014} type /quit or ctrl-d to exit)");
            }
            Err(ReadlineError::Eof) => {
                println!("bye \u{1F332}");
                break;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Returns `true` if the REPL should exit.
fn handle_command(agent: &mut Agent, cmd: &str) -> bool {
    match cmd.split_whitespace().next().unwrap_or("") {
        "quit" | "exit" | "q" => {
            println!("bye \u{1F332}");
            return true;
        }
        "help" | "h" | "?" => help(),
        "crm" => {
            let url = agent.crm_url();
            open_browser(&url);
            println!("opening {url}\n");
        }
        "clear" => {
            agent.reset();
            println!("(conversation cleared \u{2014} CRM kept)\n");
        }
        "brand" | "brands" => {
            let arg = cmd.split_whitespace().nth(1).unwrap_or("");
            if arg.is_empty() {
                println!(
                    "active brand: {}   (available: {})\n  use: /brand <key>\n",
                    agent.brand(),
                    agent.brand_keys().join(", ")
                );
            } else if agent.set_brand(arg) {
                println!("switched brand to {}\n", agent.brand());
            } else {
                println!(
                    "unknown brand '{arg}'  \u{2014}  available: {}\n",
                    agent.brand_keys().join(", ")
                );
            }
        }
        other => println!("unknown command: /{other}  \u{2014}  try /help\n"),
    }
    false
}

fn banner(agent: &Agent) {
    println!("\u{1F332} spruce-leaf \u{2014} Codex for sales");
    println!("   CRM dashboard: {}", agent.crm_url());
    println!(
        "   brand: {}   (switch with /brand <{}> )",
        agent.brand(),
        agent.brand_keys().join(" | ")
    );
    println!("   Ask me to find accounts with an expensive workflow, the people who see it,");
    println!("   and I'll write hypothesis-led sequences and file them in the CRM.");
    println!("   Try: find 5 companies with a $1M reconciliation problem in mid-market");
    println!("        logistics, 5 people each, 7 touches.");
    println!("   /help for commands, /quit to exit.\n");
}

fn help() {
    println!("commands:");
    println!("  /crm            open the CRM dashboard in your browser");
    println!("  /brand [key]    show or switch the active brand");
    println!("  /clear          clear the conversation (keeps the CRM)");
    println!("  /help           show this");
    println!("  /quit           exit (or ctrl-d)");
    println!("anything else is sent to the agent \u{2014} e.g.");
    println!("  \"find 3 accounts drowning in manual QA, 4 people each, 5 touches\"\n");
}
