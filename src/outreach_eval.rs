//! Human-anchored pairwise outreach evaluation.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::json;

use crate::engine::Engine;

#[derive(Debug, Deserialize)]
struct EvalCase {
    id: String,
    brand: String,
    account: String,
    recipient: String,
    title: String,
    #[serde(default)]
    verified_facts: Vec<String>,
    #[serde(default)]
    verified_seller_facts: Vec<String>,
    #[serde(default)]
    hypothesis_not_fact: String,
    candidate_a: String,
    candidate_b: String,
    /// Human gold label: a, b, or tie.
    expected: String,
    #[serde(default)]
    editor_note: String,
}

#[derive(Debug, Deserialize)]
struct PairwiseVerdict {
    preferred: String,
    rationale: String,
    #[serde(default)]
    unsupported_claims_a: Vec<String>,
    #[serde(default)]
    unsupported_claims_b: Vec<String>,
}

pub async fn run(engine: &Engine, path: &Path, double_blind: bool) -> Result<()> {
    let cases = load(path)?;
    if cases.is_empty() {
        bail!("outreach eval corpus is empty: {}", path.display());
    }

    let mut correct = 0usize;
    let mut consistent = 0usize;
    for case in &cases {
        let forward = judge(engine, case, false).await?;
        let preferred = if double_blind {
            let reverse = judge(engine, case, true).await?;
            let reverse_normalized = swap_label(&reverse.preferred);
            if normalize_label(&forward.preferred) == reverse_normalized {
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
    if accuracy < 0.80 {
        bail!(
            "outreach evaluation failed: {:.1}% is below the 80% promotion threshold",
            accuracy * 100.0
        );
    }
    Ok(())
}

fn load(path: &Path) -> Result<Vec<EvalCase>> {
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
            "You are a blind, skeptical cold-outreach evaluator. Choose the message a sensible recipient is more likely to answer. Judge evidence safety, recipient relevance, specificity, natural spoken language, cognitive load, and whether replying is easy. Do not reward length, polish, or framework compliance. The verified account facts and verified seller facts are separate exhaustive evidence boundaries; a supplied hypothesis is not a fact. Allow faithful paraphrases and conservative subset claims (for example, custom systems described as small internal tools). Flag only materially new declarative account details, outcomes, or seller capabilities that cross either boundary. Return tie only when neither is materially better.",
            &user,
            schema(),
        )
        .await
}

fn normalize_label(value: &str) -> &str {
    match value.trim().to_ascii_lowercase().as_str() {
        "a" | "candidate_a" => "a",
        "b" | "candidate_b" => "b",
        "tie" => "tie",
        _ => "invalid",
    }
}

fn swap_label(value: &str) -> &str {
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
        "required": ["preferred", "rationale", "unsupported_claims_a", "unsupported_claims_b"],
        "properties": {
            "preferred": { "type": "string", "enum": ["a", "b", "tie"] },
            "rationale": { "type": "string" },
            "unsupported_claims_a": { "type": "array", "items": { "type": "string" } },
            "unsupported_claims_b": { "type": "array", "items": { "type": "string" } }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{load, normalize_label, swap_label};

    #[test]
    fn normalizes_and_swaps_pairwise_labels() {
        assert_eq!(normalize_label("candidate_A"), "a");
        assert_eq!(swap_label("a"), "b");
        assert_eq!(swap_label("tie"), "tie");
    }

    #[test]
    fn bundled_corpus_is_valid_jsonl() {
        let cases = load(Path::new("evals/outreach-gold.jsonl")).expect("load eval corpus");
        assert!(cases.len() >= 2);
    }
}
