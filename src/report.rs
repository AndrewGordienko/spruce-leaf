//! Render a `Campaign` as a human-readable Markdown brief.

use std::fmt::Write;

use crate::domain::Campaign;

pub fn render(c: &Campaign) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# {} outreach campaign\n", c.brand);
    let _ = writeln!(s, "**Thesis:** {}\n", c.thesis);
    let _ = writeln!(
        s,
        "> ⚠️ Model-generated hypotheses. Only the *observed facts* are meant to be \
stated as fact; verify accounts, people, and every claim against real data before \
any outreach. Consequences are metrics to measure, not proven numbers.\n"
    );

    for ap in &c.accounts {
        let a = &ap.account;
        let _ = writeln!(s, "---\n");
        let _ = writeln!(s, "## {} — {} ({})\n", a.name, a.industry, a.hq);
        let _ = writeln!(s, "**Hypothesis:** {}\n", a.hypothesis);
        let _ = writeln!(s, "**Mechanism:** {}\n", a.mechanism);
        let _ = writeln!(s, "**Measure (not $):** {}\n", a.consequence_metric);

        bullets(&mut s, "Observed facts", &a.observed_facts);
        bullets(&mut s, "Inferences (guesses)", &a.inferences);
        bullets(&mut s, "Signals", &a.signals);

        let _ = writeln!(s, "**System concept:** {}\n", a.system_concept);
        let _ = writeln!(s, "**Hard buyer question:** {}\n", a.hard_buyer_question);
        let _ = writeln!(s, "**Kill condition:** {}\n", a.kill_condition);
        if !a.magnitude_note.trim().is_empty() {
            let _ = writeln!(s, "**Magnitude (internal only):** {}\n", a.magnitude_note);
        }
        if !a.applied_principles.is_empty() {
            let _ = writeln!(s, "**Playbook applied:** {}\n", a.applied_principles.join(", "));
        }

        for cp in &ap.contacts {
            let p = &cp.contact;
            let star = if p.primary { " ★ primary" } else { "" };
            let _ = writeln!(
                s,
                "### {} — {} [{}]{}\n",
                p.name,
                p.title,
                p.vantage.replace('_', " "),
                star
            );
            let _ = writeln!(s, "_Why them:_ {}\n", p.why_them);
            let _ = writeln!(s, "_Can observe:_ {}\n", p.can_observe);
            if !p.route_to.trim().is_empty() {
                let _ = writeln!(s, "_Route to:_ {}\n", p.route_to);
            }
            let _ = writeln!(s, "**{}-touch sequence:**\n", cp.sequence.touches.len());
            if !cp.sequence.applied_principles.is_empty() {
                let _ = writeln!(
                    s,
                    "_Playbook applied:_ {}\n",
                    cp.sequence.applied_principles.join(", ")
                );
            }

            for t in &cp.sequence.touches {
                let _ = writeln!(
                    s,
                    "**Touch {} · Day {} · {} · {}**\n",
                    t.stage, t.day_offset, t.channel, t.purpose
                );
                if !t.subject.is_empty() {
                    let _ = writeln!(s, "*Subject:* {}\n", t.subject);
                }
                let _ = writeln!(s, "{}\n", t.body);
                let _ = writeln!(s, "_Goal:_ {}\n", t.goal);
            }
        }
    }

    s
}

fn bullets(s: &mut String, title: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }
    let _ = writeln!(s, "**{title}:**");
    for it in items {
        let _ = writeln!(s, "- {it}");
    }
    let _ = writeln!(s);
}
