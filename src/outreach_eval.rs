//! Human-anchored pairwise outreach evaluation.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::json;

use crate::engine::Engine;

const HUMAN_STYLE_RUBRIC: &str = include_str!("../evals/style-guide-rubric.md");

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct EvalCase {
    pub(crate) id: String,
    pub(crate) brand: String,
    pub(crate) account: String,
    pub(crate) recipient: String,
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) verified_facts: Vec<String>,
    #[serde(default)]
    pub(crate) verified_seller_facts: Vec<String>,
    #[serde(default)]
    pub(crate) hypothesis_not_fact: String,
    pub(crate) candidate_a: String,
    pub(crate) candidate_b: String,
    /// Human gold label: a, b, or tie.
    pub(crate) expected: String,
    /// Absolute human sendability labels. Pairwise preference alone can choose
    /// the less-bad draft even when neither message should be sent.
    #[serde(default)]
    pub(crate) expected_sendable_a: Option<bool>,
    #[serde(default)]
    pub(crate) expected_sendable_b: Option<bool>,
    #[serde(default)]
    pub(crate) editor_note: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PairwiseVerdict {
    pub(crate) preferred: String,
    pub(crate) sendable_a: bool,
    pub(crate) sendable_b: bool,
    pub(crate) rationale: String,
    #[serde(default)]
    pub(crate) unsupported_claims_a: Vec<String>,
    #[serde(default)]
    pub(crate) unsupported_claims_b: Vec<String>,
}

pub async fn run(engine: &Engine, path: &Path, double_blind: bool) -> Result<()> {
    let cases = load(path)?;
    if cases.len() < 30 {
        bail!(
            "outreach promotion requires at least 30 cases; {} contains {}",
            path.display(),
            cases.len()
        );
    }
    if !double_blind {
        bail!("outreach promotion requires --double-blind order checking");
    }

    let mut correct = 0usize;
    let mut consistent = 0usize;
    let mut absolute_correct = 0usize;
    let mut absolute_labels = 0usize;
    let mut unsupported_sendable = 0usize;
    for case in &cases {
        let forward = judge(engine, case, false).await?;
        let reverse = judge(engine, case, true).await?;
        let preferred = if double_blind {
            let reverse_normalized = swap_label(&reverse.preferred);
            let absolute_consistent = forward.sendable_a == reverse.sendable_b
                && forward.sendable_b == reverse.sendable_a;
            if normalize_label(&forward.preferred) == reverse_normalized && absolute_consistent {
                consistent += 1;
                normalize_label(&forward.preferred)
            } else {
                "inconsistent"
            }
        } else {
            normalize_label(&forward.preferred)
        };
        let expected = normalize_label(&case.expected);
        let passed = preferred == expected;
        correct += usize::from(passed);
        for (expected, judged, unsupported) in [
            (
                case.expected_sendable_a,
                forward.sendable_a,
                &forward.unsupported_claims_a,
            ),
            (
                case.expected_sendable_b,
                forward.sendable_b,
                &forward.unsupported_claims_b,
            ),
        ] {
            if let Some(expected) = expected {
                absolute_labels += 1;
                absolute_correct += usize::from(expected == judged);
                unsupported_sendable += usize::from(expected && !unsupported.is_empty());
            }
        }
        println!(
            "{} {} expected={} judged={} - {}",
            if passed { "PASS" } else { "FAIL" },
            case.id,
            expected,
            preferred,
            forward.rationale.trim()
        );
        if !passed && !case.editor_note.trim().is_empty() {
            println!("  human note: {}", case.editor_note.trim());
        }
        if !forward.unsupported_claims_a.is_empty() || !forward.unsupported_claims_b.is_empty() {
            println!(
                "  unsupported: A [{}] / B [{}]",
                forward.unsupported_claims_a.join("; "),
                forward.unsupported_claims_b.join("; ")
            );
        }
    }

    let accuracy = correct as f64 / cases.len() as f64;
    if absolute_labels < 20 {
        bail!(
            "outreach promotion requires at least 20 absolute sendability labels; found {absolute_labels}"
        );
    }
    let absolute_accuracy = absolute_correct as f64 / absolute_labels as f64;
    println!(
        "\nPairwise accuracy: {correct}/{} ({:.1}%){}",
        cases.len(),
        accuracy * 100.0,
        if double_blind {
            format!(" / order-consistent {consistent}/{}", cases.len())
        } else {
            String::new()
        }
    );
    println!(
        "Absolute sendability accuracy: {absolute_correct}/{absolute_labels} ({:.1}%) · expected-sendable drafts with unsupported claims: {unsupported_sendable}",
        absolute_accuracy * 100.0
    );
    if accuracy < 0.90 || absolute_accuracy < 0.90 || unsupported_sendable > 0 {
        bail!(
            "outreach evaluation failed: pairwise {:.1}%, absolute {:.1}%, unsupported expected-sendable {} (requires >=90%, >=90%, and zero)",
            accuracy * 100.0,
            absolute_accuracy * 100.0,
            unsupported_sendable
        );
    }
    Ok(())
}

pub(crate) fn load(path: &Path) -> Result<Vec<EvalCase>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(|(index, line)| match line {
            Ok(line) if line.trim().is_empty() || line.trim_start().starts_with('#') => None,
            Ok(line) => Some(
                serde_json::from_str::<EvalCase>(&line)
                    .with_context(|| format!("parsing {} line {}", path.display(), index + 1)),
            ),
            Err(error) => Some(Err(error).context("reading outreach eval corpus")),
        })
        .collect()
}

async fn judge(engine: &Engine, case: &EvalCase, swap: bool) -> Result<PairwiseVerdict> {
    let (candidate_a, candidate_b) = if swap {
        (&case.candidate_b, &case.candidate_a)
    } else {
        (&case.candidate_a, &case.candidate_b)
    };
    let user = serde_json::to_string_pretty(&json!({
        "brand": case.brand,
        "account": case.account,
        "recipient": case.recipient,
        "title": case.title,
        "verified_facts": case.verified_facts,
        "verified_seller_facts": case.verified_seller_facts,
        "hypothesis_not_fact": case.hypothesis_not_fact,
        "candidate_a": candidate_a,
        "candidate_b": candidate_b,
    }))?;
    engine
        .structured_bulk::<PairwiseVerdict>(
            "outreach.eval_pairwise",
            &eval_system_prompt(),
            &user,
            schema(),
        )
        .await
}

pub(crate) async fn judge_candidates(
    engine: &Engine,
    case: &EvalCase,
    candidate_a: &str,
    candidate_b: &str,
    swap: bool,
) -> Result<PairwiseVerdict> {
    let mut comparison = case.clone();
    comparison.candidate_a = candidate_a.to_string();
    comparison.candidate_b = candidate_b.to_string();
    judge(engine, &comparison, swap).await
}

fn eval_system_prompt() -> String {
    format!(
        "You are a blind, skeptical cold-outreach evaluator. First judge each candidate independently as sendable or not sendable; then choose the stronger message. A winner may still be unsendable. Judge evidence safety, recipient value, specificity, natural spoken language, cognitive load, and whether replying is easy. Minimum length is not the goal. A scripted yes/no/category/referral menu is not low friction. Penalize invented task nouns and internal labels. Do not reward polish or visible framework compliance.\n\nFor Wapahki first touches, sendable means 75–110 words, one source-supported facility/task/consequence, one hypothesis, one concrete contribution, exactly one question, and no call or meeting request. The recipient must not be asked to find the use case.\n\nFor OutageHub discovery first touches, sendable means 60–95 words, an operations recipient whose title matches the evidenced segment, one sourced distributed/exposure fact, one honest outage-time operating question, and a plain explanation that OutageHub matches Canadian utility reports to locations through an API. The sole next step is a direct email answer: any call, meeting, demo, chat, calendar, or synchronous-conversation request is unsendable. Never assert a private site outage or internal workflow. A historical result is sendable only when its exact verified address, utility, full timestamp, and outside-utility-context boundary are all present.\n\nThe verified account facts and verified seller facts are separate exhaustive evidence boundaries; a supplied hypothesis is not a fact. Flag materially new declarative details or capabilities. Return tie only when neither is materially better. Never infer the preferred answer from candidate order.\n\nHUMAN STYLE RUBRIC:\n{HUMAN_STYLE_RUBRIC}"
    )
}

pub(crate) fn normalize_label(value: &str) -> &str {
    match value.trim().to_ascii_lowercase().as_str() {
        "a" | "candidate_a" => "a",
        "b" | "candidate_b" => "b",
        "tie" => "tie",
        _ => "invalid",
    }
}

pub(crate) fn swap_label(value: &str) -> &str {
    match normalize_label(value) {
        "a" => "b",
        "b" => "a",
        other => other,
    }
}

fn schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["preferred", "sendable_a", "sendable_b", "rationale", "unsupported_claims_a", "unsupported_claims_b"],
        "properties": {
            "preferred": { "type": "string", "enum": ["a", "b", "tie"] },
            "sendable_a": { "type": "boolean" },
            "sendable_b": { "type": "boolean" },
            "rationale": { "type": "string" },
            "unsupported_claims_a": { "type": "array", "items": { "type": "string" } },
            "unsupported_claims_b": { "type": "array", "items": { "type": "string" } }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{eval_system_prompt, load, normalize_label, swap_label};

    #[test]
    fn normalizes_and_swaps_pairwise_labels() {
        assert_eq!(normalize_label("candidate_A"), "a");
        assert_eq!(swap_label("a"), "b");
        assert_eq!(swap_label("tie"), "tie");
    }

    #[test]
    fn bundled_corpus_is_parseable_legacy_input() {
        let cases = load(Path::new("evals/outreach-gold.jsonl")).expect("load eval corpus");
        // The checked-in corpus remains readable while `run` deliberately
        // refuses to promote a policy until it is expanded to 30 cases with
        // absolute sendability labels.
        assert!(cases.len() >= 18);
    }

    #[test]
    fn blind_judge_uses_human_style_without_gold_labels() {
        let prompt = eval_system_prompt();
        assert!(prompt.contains("scripted yes/no/category/referral menu"));
        assert!(prompt.contains("First judge each candidate independently"));
        assert!(prompt.contains("For Wapahki first touches"));
        assert!(prompt.contains("For OutageHub discovery first touches"));
        assert!(prompt.contains("sole next step is a direct email answer"));
        assert!(!prompt.contains("expected"));
        assert!(!prompt.contains("editor_note"));
    }

    #[test]
    fn outagehub_gold_cases_match_the_current_discovery_contract() {
        let cases = load(Path::new("evals/outreach-gold.jsonl")).expect("load eval corpus");
        let outagehub = cases
            .iter()
            .filter(|case| case.brand == "outagehub")
            .collect::<Vec<_>>();
        assert_eq!(outagehub.len(), 7);
        for case in outagehub {
            assert_eq!(case.expected_sendable_a, Some(true), "{}", case.id);
            assert_eq!(case.expected_sendable_b, Some(false), "{}", case.id);
            let body = case.candidate_a.to_ascii_lowercase();
            assert!(body.contains("through an api"), "{}", case.id);
            assert_eq!(body.matches('?').count(), 1, "{}", case.id);
            assert!(
                ![
                    "minute call",
                    "minutes next",
                    "conversation next",
                    "meeting",
                    "demo",
                    "calendar",
                ]
                .iter()
                .any(|marker| body.contains(marker)),
                "{}",
                case.id
            );
            let words = body.split_whitespace().count();
            assert!((60..=95).contains(&words), "{}: {words}", case.id);
        }
    }
}
