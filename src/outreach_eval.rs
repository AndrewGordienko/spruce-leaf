//! Human-anchored three-candidate inbox evaluation.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::json;

use crate::engine::Engine;

const HUMAN_STYLE_RUBRIC: &str = include_str!("../evals/style-guide-rubric.md");
const OUTREACH_QUALITY_RUBRIC: &str = include_str!("../evals/outreach-quality-rubric.md");

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
    #[serde(default)]
    pub(crate) candidate_c: String,
    /// Human gold label: a, b, c, or none.
    pub(crate) expected: String,
    /// Absolute human sendability labels. Pairwise preference alone can choose
    /// the less-bad draft even when neither message should be sent.
    #[serde(default)]
    pub(crate) expected_sendable_a: Option<bool>,
    #[serde(default)]
    pub(crate) expected_sendable_b: Option<bool>,
    #[serde(default)]
    pub(crate) expected_sendable_c: Option<bool>,
    /// Promotion corpora must record Andrew's unchanged-send decision rather
    /// than a label inferred by the same prompt that produced the messages.
    #[serde(default)]
    pub(crate) label_source: String,
    /// train | holdout. Labels are never included in a judge request.
    #[serde(default)]
    pub(crate) partition: String,
    #[serde(default)]
    pub(crate) editor_note: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
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

#[derive(Clone, Debug, Deserialize)]
struct InboxScore {
    recipient_value: u8,
    specificity: u8,
    credibility: u8,
    reply_ease: u8,
    naturalness: u8,
}

impl InboxScore {
    fn total(&self) -> u16 {
        u16::from(self.recipient_value)
            + u16::from(self.specificity)
            + u16::from(self.credibility)
            + u16::from(self.reply_ease)
            + u16::from(self.naturalness)
    }

    fn is_sendable(&self) -> bool {
        self.recipient_value >= 24
            && self.specificity >= 15
            && self.credibility >= 16
            && self.reply_ease >= 11
            && self.naturalness >= 11
            && self.total() >= 80
    }
}

#[derive(Debug, Deserialize)]
struct ThreeCandidateVerdict {
    preferred: String,
    sendable_a: bool,
    sendable_b: bool,
    sendable_c: bool,
    score_a: InboxScore,
    score_b: InboxScore,
    score_c: InboxScore,
    rationale: String,
}

pub async fn run(engine: &Engine, path: &Path, double_blind: bool) -> Result<()> {
    let cases = load(path)?;
    validate_promotion_corpus(&cases, path)?;
    if !double_blind {
        bail!("outreach promotion requires --double-blind order checking");
    }

    let mut correct = 0usize;
    let mut consistent = 0usize;
    let mut absolute_correct = 0usize;
    let mut absolute_labels = 0usize;
    let mut brand_scores = HashMap::<String, (usize, usize, usize, usize, usize, usize)>::new();
    for case in &cases {
        let forward = judge_three(engine, case, false).await?;
        let reverse = judge_three(engine, case, true).await?;
        let preferred = if double_blind {
            let reverse_normalized = reverse_three_label(&reverse.preferred);
            let absolute_consistent = forward.sendable_a == reverse.sendable_c
                && forward.sendable_b == reverse.sendable_b
                && forward.sendable_c == reverse.sendable_a;
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
        let brand_score = brand_scores
            .entry(case.brand.trim().to_ascii_lowercase())
            .or_default();
        brand_score.0 += usize::from(passed);
        brand_score.1 += 1;
        if case.partition == "holdout" {
            brand_score.4 += usize::from(passed);
            brand_score.5 += 1;
        }
        for (expected, judged) in [
            (case.expected_sendable_a, forward.sendable_a),
            (case.expected_sendable_b, forward.sendable_b),
            (case.expected_sendable_c, forward.sendable_c),
        ] {
            if let Some(expected) = expected {
                absolute_labels += 1;
                absolute_correct += usize::from(expected == judged);
                brand_score.3 += 1;
                brand_score.2 += usize::from(expected == judged);
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
    }

    let accuracy = correct as f64 / cases.len() as f64;
    let absolute_accuracy = absolute_correct as f64 / absolute_labels as f64;
    println!(
        "\nThree-candidate selection accuracy: {correct}/{} ({:.1}%){}",
        cases.len(),
        accuracy * 100.0,
        if double_blind {
            format!(" / order-consistent {consistent}/{}", cases.len())
        } else {
            String::new()
        }
    );
    println!(
        "Absolute sendability accuracy: {absolute_correct}/{absolute_labels} ({:.1}%)",
        absolute_accuracy * 100.0
    );
    let mut brand_failed = false;
    for (brand, (selected, cases, absolute, labels, holdout, holdout_cases)) in &brand_scores {
        let selection_accuracy = *selected as f64 / *cases as f64;
        let absolute_brand_accuracy = *absolute as f64 / *labels as f64;
        let holdout_accuracy = *holdout as f64 / *holdout_cases as f64;
        println!(
            "{brand}: selection {selected}/{cases} ({:.1}%), absolute {absolute}/{labels} ({:.1}%), sealed holdout {holdout}/{holdout_cases} ({:.1}%)",
            selection_accuracy * 100.0,
            absolute_brand_accuracy * 100.0,
            holdout_accuracy * 100.0,
        );
        brand_failed |=
            selection_accuracy < 0.90 || absolute_brand_accuracy < 0.90 || holdout_accuracy < 0.90;
    }
    if accuracy < 0.90 || absolute_accuracy < 0.90 || brand_failed {
        bail!(
            "outreach evaluation failed: selection {:.1}%, absolute {:.1}% (requires >=90% for both)",
            accuracy * 100.0,
            absolute_accuracy * 100.0
        );
    }
    Ok(())
}

fn validate_promotion_corpus(cases: &[EvalCase], path: &Path) -> Result<()> {
    for (brand, minimum, minimum_holdout) in [
        ("gnk", 30usize, 6usize),
        ("wapahki", 10, 2),
        ("outagehub", 40, 10),
    ] {
        let brand_cases = cases
            .iter()
            .filter(|case| case.brand.eq_ignore_ascii_case(brand))
            .collect::<Vec<_>>();
        if brand_cases.len() < minimum {
            bail!(
                "outreach promotion requires at least {minimum} {brand} cases; {} contains {}",
                path.display(),
                brand_cases.len()
            );
        }
        if brand_cases.iter().any(|case| {
            case.candidate_a.trim().is_empty()
                || case.candidate_b.trim().is_empty()
                || case.candidate_c.trim().is_empty()
        }) {
            bail!("{brand} promotion cases require three visible candidates");
        }
        if brand_cases.iter().any(|case| {
            case.expected_sendable_a.is_none()
                || case.expected_sendable_b.is_none()
                || case.expected_sendable_c.is_none()
        }) {
            bail!("{brand} promotion cases require absolute sendability labels for A, B, and C");
        }
        if brand_cases.iter().any(|case| {
            case.label_source.trim() != "andrew_unchanged_send_decision"
                || !matches!(case.partition.trim(), "train" | "holdout")
        }) {
            bail!(
                "{brand} promotion labels must be Andrew's unchanged-send decision and declare train/holdout"
            );
        }
        let holdout = brand_cases
            .iter()
            .filter(|case| case.partition == "holdout")
            .count();
        if holdout < minimum_holdout || holdout * 5 < brand_cases.len() {
            bail!(
                "{brand} promotion requires at least {minimum_holdout} sealed holdout cases and at least 20% of the brand corpus"
            );
        }
        let labels = brand_cases
            .iter()
            .map(|case| normalize_label(&case.expected))
            .collect::<Vec<_>>();
        if labels.iter().any(|label| *label == "invalid") {
            bail!("{brand} promotion labels must be a, b, c, or none");
        }
        for required in ["a", "b", "c", "none"] {
            if labels.iter().filter(|label| **label == required).count() < 2 {
                bail!(
                    "{brand} promotion corpus must include at least two human selections of {required}"
                );
            }
        }
        for case in brand_cases {
            let sendable = [
                case.expected_sendable_a.unwrap_or(false),
                case.expected_sendable_b.unwrap_or(false),
                case.expected_sendable_c.unwrap_or(false),
            ];
            let selected = match normalize_label(&case.expected) {
                "a" => sendable[0],
                "b" => sendable[1],
                "c" => sendable[2],
                "none" => !sendable.iter().any(|value| *value),
                _ => false,
            };
            if !selected {
                bail!(
                    "{} has a selection inconsistent with its sendability labels",
                    case.id
                );
            }
        }
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

async fn judge_three(
    engine: &Engine,
    case: &EvalCase,
    reverse: bool,
) -> Result<ThreeCandidateVerdict> {
    let candidates = if reverse {
        [&case.candidate_c, &case.candidate_b, &case.candidate_a]
    } else {
        [&case.candidate_a, &case.candidate_b, &case.candidate_c]
    };
    // This is deliberately an inbox-only view. Account hypotheses, evaluator
    // labels, editor notes, and internal briefs are not available to the final
    // buyer-perspective judge. Production factual lineage is enforced before
    // this stage by claim-id and deterministic-copy validation.
    let user = serde_json::to_string_pretty(&json!({
        "recipient_title": case.title,
        "public_seller_description": case.verified_seller_facts,
        "candidate_a": candidates[0],
        "candidate_b": candidates[1],
        "candidate_c": candidates[2],
    }))?;
    let mut verdict = engine
        .structured_bulk::<ThreeCandidateVerdict>(
            "outreach.eval_inbox_three",
            &three_candidate_prompt(),
            &user,
            three_candidate_schema(),
        )
        .await?;
    // A model cannot declare a low-scoring candidate sendable. This keeps the
    // weighted buyer standard deterministic at the evaluation boundary.
    verdict.sendable_a &= verdict.score_a.is_sendable();
    verdict.sendable_b &= verdict.score_b.is_sendable();
    verdict.sendable_c &= verdict.score_c.is_sendable();
    let selected_is_sendable = match normalize_label(&verdict.preferred) {
        "a" => verdict.sendable_a,
        "b" => verdict.sendable_b,
        "c" => verdict.sendable_c,
        "none" => !verdict.sendable_a && !verdict.sendable_b && !verdict.sendable_c,
        _ => false,
    };
    if !selected_is_sendable {
        verdict.preferred = "none".into();
    }
    Ok(verdict)
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
        "You are a skeptical factual validator for cold outreach. Judge each candidate independently. A winner may still be unsendable. Reject invented facts, wrong-role assumptions, vague research interviews, sender-benefit language, and a promised artifact presented as completed. Minimum length and template conformity are not quality. For Wapahki use 45–95 words, one evidenced physical task and one easy question; credentials and a fit screen are optional, and a screen may appear only if completed. For OutageHub require a location-specific result, a comparison the product can actually produce, a relevant sample response, or a one-line correction; generic company-category summaries and 'an answer would help' are unsendable. For GnK use 60–110 words and require a bounded deliverable that says what GnK examines, returns, and improves; do not infer ownership from a title. Calls are not an earned first step for discovery contacts. The verified account and seller facts are exhaustive boundaries. Flag every materially new declarative detail or capability. Never infer the preferred answer from candidate order.\n\nCURRENT SENDABILITY RUBRIC:\n{OUTREACH_QUALITY_RUBRIC}\n\nHUMAN STYLE RUBRIC:\n{HUMAN_STYLE_RUBRIC}"
    )
}

fn three_candidate_prompt() -> String {
    "You are one independent blind inbox reviewer. You see only the recipient title, a public seller description, and three unchanged emails. You do not see an internal account brief or hypothesis and must not imagine one. Score each candidate independently: recipient_value /30, specificity /20, credibility /20, reply_ease /15, naturalness /15. Hard reject any candidate that mainly asks the recipient to educate the sender, contains no concrete contribution, requires hidden context to matter, sounds accurate but uninteresting, or provides no reason to reply now. Select a, b, c, or none. Use none whenever no email should be sent unchanged. Do not rewrite, merge, or repair candidates, and do not infer the answer from order."
        .into()
}

pub(crate) fn normalize_label(value: &str) -> &str {
    match value.trim().to_ascii_lowercase().as_str() {
        "a" | "candidate_a" => "a",
        "b" | "candidate_b" => "b",
        "c" | "candidate_c" => "c",
        "none" | "none_sendable" | "tie" => "none",
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

fn reverse_three_label(value: &str) -> &str {
    match normalize_label(value) {
        "a" => "c",
        "c" => "a",
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

fn three_candidate_schema() -> serde_json::Value {
    let score = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["recipient_value", "specificity", "credibility", "reply_ease", "naturalness"],
        "properties": {
            "recipient_value": { "type": "integer", "minimum": 0, "maximum": 30 },
            "specificity": { "type": "integer", "minimum": 0, "maximum": 20 },
            "credibility": { "type": "integer", "minimum": 0, "maximum": 20 },
            "reply_ease": { "type": "integer", "minimum": 0, "maximum": 15 },
            "naturalness": { "type": "integer", "minimum": 0, "maximum": 15 }
        }
    });
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["preferred", "sendable_a", "sendable_b", "sendable_c", "score_a", "score_b", "score_c", "rationale"],
        "properties": {
            "preferred": { "type": "string", "enum": ["a", "b", "c", "none"] },
            "sendable_a": { "type": "boolean" },
            "sendable_b": { "type": "boolean" },
            "sendable_c": { "type": "boolean" },
            "score_a": score.clone(),
            "score_b": score.clone(),
            "score_c": score,
            "rationale": { "type": "string" }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        eval_system_prompt, load, normalize_label, swap_label, three_candidate_prompt,
        validate_promotion_corpus,
    };

    #[test]
    fn normalizes_and_swaps_pairwise_labels() {
        assert_eq!(normalize_label("candidate_A"), "a");
        assert_eq!(swap_label("a"), "b");
        assert_eq!(normalize_label("candidate_c"), "c");
        assert_eq!(normalize_label("none_sendable"), "none");
        assert_eq!(swap_label("none"), "none");
    }

    #[test]
    fn bundled_corpus_is_parseable_legacy_input() {
        let cases = load(Path::new("evals/outreach-gold.jsonl")).expect("load eval corpus");
        // The checked-in corpus remains readable as historical evidence while
        // promotion deliberately requires the new three-candidate standard.
        assert!(cases.len() >= 18);
        let error = validate_promotion_corpus(&cases, Path::new("evals/outreach-gold.jsonl"))
            .expect_err("legacy A-always-wins corpus must not promote copy");
        assert!(error.to_string().contains("at least 30 gnk cases"));
    }

    #[test]
    fn blind_judge_uses_human_style_without_gold_labels() {
        let prompt = three_candidate_prompt();
        assert!(prompt.contains("one independent blind inbox reviewer"));
        assert!(prompt.contains("three unchanged emails"));
        assert!(prompt.contains("recipient_value /30"));
        assert!(prompt.contains("Select a, b, c, or none"));
        assert!(prompt.contains("do not see an internal account brief"));
        assert!(!prompt.contains("expected"));
        assert!(!prompt.contains("editor_note"));
        let safety = eval_system_prompt();
        assert!(safety.contains("45–95 words"));
        assert!(safety.contains("bounded deliverable"));
        assert!(safety.contains("an answer would help"));
    }
}
