//! Copy-prompt ablations against fixed, human-anchored outreach cases.
//!
//! This is an engineering diagnostic, not a substitute for a live reply-rate
//! experiment. It keeps the account, recipient, evidence, hypothesis, model,
//! output contract, and blind evaluator fixed while removing or expanding one
//! prompt layer at a time.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use anyhow::{bail, Result};
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use serde_json::json;

use crate::engine::Engine;
use crate::outreach::generic_subject_label;
use crate::outreach_eval::{judge_candidates, load, normalize_label, swap_label, EvalCase};
use crate::playbook::{Playbook, Playbooks, Shared};
use crate::response_design;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Arm {
    Full,
    NoRoleContract,
    NoPsychology,
    NoWriterPersona,
    NoBrandDoctrine,
    CompactWriter,
    ExpandedPsychology,
}

impl Arm {
    const ALL: [Self; 7] = [
        Self::Full,
        Self::NoRoleContract,
        Self::NoPsychology,
        Self::NoWriterPersona,
        Self::NoBrandDoctrine,
        Self::CompactWriter,
        Self::ExpandedPsychology,
    ];

    fn key(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::NoRoleContract => "no_role_contract",
            Self::NoPsychology => "no_psychology",
            Self::NoWriterPersona => "no_writer_persona",
            Self::NoBrandDoctrine => "no_brand_doctrine",
            Self::CompactWriter => "compact_writer",
            Self::ExpandedPsychology => "expanded_psychology",
        }
    }

    fn interpretation(self) -> &'static str {
        match self {
            Self::Full => "production-sized prompt baseline",
            Self::NoRoleContract => {
                "removes only the recipient's title-and-vantage response contract"
            }
            Self::NoPsychology => "removes only private response-design doctrine",
            Self::NoWriterPersona => "removes only the editable writer persona excerpt",
            Self::NoBrandDoctrine => "removes only brand constraints and examples",
            Self::CompactWriter => "shrinks only the writer excerpt from 360 to 120 words",
            Self::ExpandedPsychology => "expands only the psychology excerpt from 130 to 300 words",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        let key = value.trim().to_ascii_lowercase().replace('-', "_");
        Self::ALL.into_iter().find(|arm| arm.key() == key)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct GeneratedEmail {
    subject: String,
    body: String,
}

impl GeneratedEmail {
    fn as_candidate(&self) -> String {
        format!("Subject: {}\n\n{}", self.subject.trim(), self.body.trim())
    }
}

#[derive(Default)]
struct ArmResult {
    generations: usize,
    absolute_passes: usize,
    comparisons: usize,
    arm_wins: usize,
    full_wins: usize,
    ties: usize,
    inconsistent: usize,
    prompt_words: usize,
}

pub(crate) struct Options<'a> {
    pub case_limit: usize,
    pub repeats: usize,
    pub concurrency: usize,
    pub show_drafts: bool,
    pub only: Option<&'a str>,
}

pub async fn run(
    engine: &Engine,
    playbooks: &Playbooks,
    path: &Path,
    options: Options<'_>,
) -> Result<()> {
    let all_cases = load(path)?;
    let cases = representative_cases(all_cases, options.case_limit);
    if cases.is_empty() {
        bail!("outreach ablation corpus is empty: {}", path.display());
    }

    println!(
        "Copy-prompt ablation · {} fixed case(s) · {} repeat(s) · model held constant",
        cases.len(),
        options.repeats
    );
    let arms = match options.only {
        Some(value) => {
            let arm = Arm::parse(value)
                .filter(|arm| *arm != Arm::Full)
                .ok_or_else(|| anyhow::anyhow!("unknown ablation arm '{value}'"))?;
            vec![Arm::Full, arm]
        }
        None => Arm::ALL.to_vec(),
    };
    println!(
        "Variable: one prompt layer{}. Constants: recipient, account, facts, hypothesis, requested cold outcome, model, schema, and blind evaluator.",
        options
            .only
            .map_or_else(String::new, |value| format!(" ({value})"))
    );

    let jobs = (0..options.repeats)
        .flat_map(|repeat| {
            let arms = arms.clone();
            cases.iter().cloned().flat_map(move |case| {
                arms.clone()
                    .into_iter()
                    .map(move |arm| (repeat, case.clone(), arm))
            })
        })
        .collect::<Vec<_>>();

    let generated = stream::iter(jobs)
        .map(|(repeat, case, arm)| async move {
            let playbook = playbooks.get(&case.brand)?;
            let system = system_prompt(playbook, &playbooks.shared, arm);
            let role_words = if arm == Arm::NoRoleContract {
                0
            } else {
                response_design::for_title_and_vantage(&case.title, "")
                    .prompt_block()
                    .split_whitespace()
                    .count()
            };
            let prompt_words = system.split_whitespace().count() + role_words;
            let draft = generate(engine, &case, playbook, arm, &system).await?;
            Ok::<_, anyhow::Error>(((repeat, case.id.clone(), arm), draft, prompt_words))
        })
        .buffer_unordered(options.concurrency.max(1))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    let mut results = BTreeMap::<Arm, ArmResult>::new();
    let mut drafts = HashMap::new();
    let mut prompt_words = BTreeMap::new();
    for (key, draft, words) in generated {
        prompt_words.insert(key.2, words);
        let case = cases
            .iter()
            .find(|case| case.id == key.1)
            .expect("generated case");
        let playbook = playbooks.get(&case.brand)?;
        let result = results.entry(key.2).or_default();
        result.generations += 1;
        result.absolute_passes += usize::from(absolute_issues(case, playbook, &draft).is_empty());
        drafts.insert(key, draft);
    }

    for &arm in &arms {
        results.entry(arm).or_default().prompt_words = *prompt_words.get(&arm).unwrap_or(&0);
    }

    for repeat in 0..options.repeats {
        for case in &cases {
            let full = drafts
                .get(&(repeat, case.id.clone(), Arm::Full))
                .expect("full draft");
            println!("\n{} · {} ({})", case.id, case.recipient, case.title);
            println!("  full subject: {}", full.subject.trim());
            print_absolute_issues(case, playbooks.get(&case.brand)?, full);
            if options.show_drafts {
                println!("\n{}\n", full.body.trim());
            }
            for arm in arms.iter().copied().filter(|arm| *arm != Arm::Full) {
                let variant = drafts
                    .get(&(repeat, case.id.clone(), arm))
                    .expect("variant draft");
                let comparison_case = evaluation_case(case, playbooks.get(&case.brand)?);
                let forward = judge_candidates(
                    engine,
                    &comparison_case,
                    &full.as_candidate(),
                    &variant.as_candidate(),
                    false,
                )
                .await?;
                let reverse = judge_candidates(
                    engine,
                    &comparison_case,
                    &full.as_candidate(),
                    &variant.as_candidate(),
                    true,
                )
                .await?;
                let forward_label = normalize_label(&forward.preferred);
                let reverse_label = swap_label(&reverse.preferred);
                let result = results.entry(arm).or_default();
                result.comparisons += 1;
                let verdict = if forward_label != reverse_label {
                    result.inconsistent += 1;
                    "inconsistent"
                } else {
                    match forward_label {
                        "a" => {
                            result.full_wins += 1;
                            "full"
                        }
                        "b" => {
                            result.arm_wins += 1;
                            "variant"
                        }
                        _ => {
                            result.ties += 1;
                            "tie"
                        }
                    }
                };
                println!(
                    "  {:<20} {:<12} subject: {}",
                    arm.key(),
                    verdict,
                    variant.subject.trim()
                );
                print_absolute_issues(case, playbooks.get(&case.brand)?, variant);
                println!("    {}", forward.rationale.trim());
                if options.show_drafts {
                    println!("\n{}\n", variant.body.trim());
                }
            }
        }
    }

    let full_words = results
        .get(&Arm::Full)
        .map_or(0, |result| result.prompt_words);
    println!("\nAblation summary (directional model-quality evidence, not reply-rate evidence)");
    println!("arm                  words   delta   abs pass   variant wins   ties   full wins   inconsistent");
    for arm in arms.iter().copied().filter(|arm| *arm != Arm::Full) {
        let result = results.get(&arm).expect("arm result");
        println!(
            "{:<20} {:>5} {:+7} {:>4}/{:<4} {:>14} {:>6} {:>11} {:>14}",
            arm.key(),
            result.prompt_words,
            result.prompt_words as isize - full_words as isize,
            result.absolute_passes,
            result.generations,
            result.arm_wins,
            result.ties,
            result.full_wins,
            result.inconsistent,
        );
        println!("  {}", arm.interpretation());
    }
    println!(
        "\nDecision rule: remove a layer only after the no-layer arm ties or wins repeatedly across brands; expand a layer only if its single-variable expanded arm wins consistently enough to justify the added tokens. Confirm adopted changes later with a single-variable live reply-rate test."
    );
    Ok(())
}

fn representative_cases(cases: Vec<EvalCase>, limit: usize) -> Vec<EvalCase> {
    const PRIORITY: [&str; 3] = [
        "gnk-3pl-nancy-human-t1",
        "outagehub-ivy-wendy-guide-02",
        "wapahki-amcor-rhoneil-human-t1",
    ];
    let mut seen = HashSet::new();
    let mut selected = Vec::new();
    for id in PRIORITY {
        if let Some(case) = cases.iter().find(|case| case.id == id) {
            if seen.insert(case.brand.clone()) {
                selected.push(case.clone());
            }
        }
        if selected.len() >= limit {
            return selected;
        }
    }
    for case in cases {
        if seen.insert(case.brand.clone()) {
            selected.push(case);
            if selected.len() >= limit {
                break;
            }
        }
    }
    selected
}

async fn generate(
    engine: &Engine,
    case: &EvalCase,
    playbook: &Playbook,
    arm: Arm,
    system: &str,
) -> Result<GeneratedEmail> {
    let seller_facts = seller_facts(case, playbook);
    let mut payload = json!({
        "brand": case.brand,
        "account": case.account,
        "recipient": case.recipient,
        "title": case.title,
        "verified_account_facts": case.verified_facts,
        "verified_seller_facts": seller_facts,
        "hypothesis_not_fact": case.hypothesis_not_fact,
        "requested_outcome": "A short discovery conversation, with a short email answer as the easier alternative when the recipient is a credible workflow owner; otherwise an appropriate correction or route.",
        "required_signature": playbook.signature,
        "email_word_band_including_signature": [playbook.min_words, playbook.max_words],
    });
    if arm != Arm::NoRoleContract {
        payload.as_object_mut().expect("ablation payload").insert(
            "role_response_contract_internal_only".into(),
            response_design::for_title_and_vantage(&case.title, "").prompt_value(),
        );
    }
    let user = serde_json::to_string_pretty(&payload)?;
    engine
        .structured_bulk::<GeneratedEmail>(
            &format!("outreach.ablation.generate.{}", arm.key()),
            system,
            &user,
            email_schema(),
        )
        .await
}

fn evaluation_case(case: &EvalCase, playbook: &Playbook) -> EvalCase {
    let mut comparison = case.clone();
    comparison.verified_seller_facts = seller_facts(case, playbook);
    comparison
}

fn seller_facts(case: &EvalCase, playbook: &Playbook) -> Vec<String> {
    let mut facts = case.verified_seller_facts.clone();
    facts.push(playbook.one_liner.clone());
    facts.extend(playbook.verified_seller_facts.iter().cloned());
    facts.sort();
    facts.dedup();
    facts
}

fn system_prompt(playbook: &Playbook, shared: &Shared, arm: Arm) -> String {
    let core = "Write one cold first email as Andrew to the supplied recipient. Return a subject and body only. The subject must be a plain 3-9 word operating phrase that creates an honest, specific reason to open; privately consider several subjects before choosing. The body must begin `Hi [recipient first name],` on its own line, end with the exact required signature on its own line, and form a complete founder note in ordinary spoken English: why this person, one recognizable operating moment, a bounded guess about the difficulty, the seller's relevant difference, and one role-appropriate response path. Use the verified account and seller facts as separate exhaustive evidence boundaries. The hypothesis is a question, never a fact. Do not invent private workflows, systems, objects, incidents, ownership, impact, seller capabilities, or collateral. Make replying worthwhile and easy without a scripted answer menu, pressure, hype, or internal strategy language. Read the subject and first two lines as an inbox recipient before returning the structured result.";
    let mut production = format!("{core}\n\n{}", playbook.copy_system_prompt(shared));
    match arm {
        Arm::Full | Arm::NoRoleContract => production,
        Arm::NoPsychology => {
            remove_section(
                &mut production,
                "=== PRIVATE RESPONSE-DESIGN DOCTRINE ===",
                "=== ",
            );
            production
        }
        Arm::NoWriterPersona => {
            remove_section(&mut production, "=== EDITABLE PERSONA EXCERPT ===", "=== ");
            production
        }
        Arm::NoBrandDoctrine => {
            let header = format!("=== {} BUYER-FACING CONSTRAINTS ===", playbook.name);
            remove_section(&mut production, &header, "Do not expose");
            production
        }
        Arm::CompactWriter => {
            replace_section(
                &mut production,
                "=== EDITABLE PERSONA EXCERPT ===",
                "=== PRIVATE RESPONSE-DESIGN DOCTRINE ===",
                &persona_excerpt(&shared.personas.writer, 120),
            );
            production
        }
        Arm::ExpandedPsychology => {
            replace_section(
                &mut production,
                "=== PRIVATE RESPONSE-DESIGN DOCTRINE ===",
                &format!("=== {} BUYER-FACING CONSTRAINTS ===", playbook.name),
                &persona_excerpt(&shared.personas.psychology, 300),
            );
            production
        }
    }
}

fn persona_excerpt(persona: &str, max_words: usize) -> String {
    let mut excerpt = String::new();
    let mut words = 0usize;
    for paragraph in persona.split("\n\n") {
        let paragraph = paragraph.trim();
        if paragraph.is_empty() || paragraph.starts_with('#') {
            continue;
        }
        let paragraph_words = paragraph.split_whitespace().count();
        if !excerpt.is_empty() && words + paragraph_words > max_words {
            break;
        }
        if !excerpt.is_empty() {
            excerpt.push_str("\n\n");
        }
        excerpt.push_str(paragraph);
        words += paragraph_words;
    }
    excerpt
}

fn absolute_issues(case: &EvalCase, playbook: &Playbook, draft: &GeneratedEmail) -> Vec<String> {
    let mut issues = Vec::new();
    let subject_words = draft.subject.split_whitespace().count();
    if !(3..=9).contains(&subject_words) {
        issues.push(format!("subject has {subject_words} words"));
    }
    if generic_subject_label(&draft.subject) {
        issues.push("subject is a generic topic label".into());
    }
    let expected_greeting = format!("hi {},", case.recipient.to_ascii_lowercase());
    if !draft
        .body
        .trim_start()
        .to_ascii_lowercase()
        .starts_with(&expected_greeting)
    {
        issues.push("missing exact greeting".into());
    }
    if !draft.body.trim_end().ends_with(&playbook.signature) {
        issues.push("missing exact signature".into());
    }
    let body_words = draft.body.split_whitespace().count();
    if body_words < playbook.min_words || body_words > playbook.max_words {
        issues.push(format!(
            "body has {body_words} words (needs {}-{})",
            playbook.min_words, playbook.max_words
        ));
    }
    issues
}

fn print_absolute_issues(case: &EvalCase, playbook: &Playbook, draft: &GeneratedEmail) {
    let issues = absolute_issues(case, playbook, draft);
    if !issues.is_empty() {
        println!("    absolute QA: FAIL · {}", issues.join("; "));
    }
}

fn remove_section(text: &mut String, start: &str, next: &str) {
    let Some(start_index) = text.find(start) else {
        return;
    };
    let search_from = start_index + start.len();
    let Some(relative_end) = text[search_from..].find(next) else {
        return;
    };
    text.replace_range(start_index..search_from + relative_end, "");
}

fn replace_section(text: &mut String, start: &str, next: &str, replacement: &str) {
    let Some(start_index) = text.find(start) else {
        return;
    };
    let content_start = start_index + start.len();
    let Some(relative_end) = text[content_start..].find(next) else {
        return;
    };
    text.replace_range(
        content_start..content_start + relative_end,
        &format!("\n{replacement}\n\n"),
    );
}

fn email_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["subject", "body"],
        "properties": {
            "subject": { "type": "string" },
            "body": { "type": "string" }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{representative_cases, system_prompt, Arm};
    use crate::outreach_eval::load;
    use crate::playbook::Playbooks;
    use std::path::Path;

    #[test]
    fn selects_one_case_per_brand() {
        let cases = load(Path::new("evals/outreach-gold.jsonl")).expect("cases");
        let selected = representative_cases(cases, 3);
        let brands = selected
            .iter()
            .map(|case| case.brand.as_str())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(selected.len(), 3);
        assert_eq!(brands.len(), 3);
    }

    #[test]
    fn ablation_arms_change_only_the_named_prompt_sections() {
        let playbooks = Playbooks::load("playbooks").expect("playbooks");
        let playbook = playbooks.get("outagehub").expect("outagehub");
        let full = system_prompt(playbook, &playbooks.shared, Arm::Full);
        let no_psych = system_prompt(playbook, &playbooks.shared, Arm::NoPsychology);
        let no_role = system_prompt(playbook, &playbooks.shared, Arm::NoRoleContract);
        let no_persona = system_prompt(playbook, &playbooks.shared, Arm::NoWriterPersona);
        let no_brand = system_prompt(playbook, &playbooks.shared, Arm::NoBrandDoctrine);
        let compact_writer = system_prompt(playbook, &playbooks.shared, Arm::CompactWriter);
        let expanded_psychology =
            system_prompt(playbook, &playbooks.shared, Arm::ExpandedPsychology);

        assert!(full.contains("PRIVATE RESPONSE-DESIGN DOCTRINE"));
        assert_eq!(full, no_role);
        assert!(!no_psych.contains("PRIVATE RESPONSE-DESIGN DOCTRINE"));
        assert!(!no_persona.contains("EDITABLE PERSONA EXCERPT"));
        assert!(!no_brand.contains("OutageHub BUYER-FACING CONSTRAINTS"));
        assert!(compact_writer.split_whitespace().count() < full.split_whitespace().count());
        assert!(expanded_psychology.split_whitespace().count() > full.split_whitespace().count());
    }
}
