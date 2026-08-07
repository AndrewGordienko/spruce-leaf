//! Brand playbooks: the editable doctrine that drives every prompt.
//!
//! The outreach doctrine lives in `playbooks/*.toml`, NOT in this binary, so it
//! can be tuned without a recompile. `shared.toml` holds the spine common to
//! every brand; each brand file (`gnk.toml`, `wapahki.toml`, `outagehub.toml`)
//! adds the deltas — product one-liner, the "motion" it tests, length band,
//! subject style, vantage notes, and extra forbidden phrases.
//!
//! At startup we load them all into a [`Playbooks`] registry and hand the right
//! [`Playbook`] to each pipeline stage, which folds it into the system prompt
//! and into the mechanical lint (forbidden phrases + word band).

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

/// The shared doctrine spine (`shared.toml`), applied to every brand.
#[derive(Debug, Clone, Deserialize)]
pub struct Shared {
    /// Mechanical forbidden phrases matched (case-insensitively) in every brand.
    #[serde(default)]
    pub forbidden: Vec<String>,
    /// Prose doctrine injected verbatim into every system prompt.
    pub doctrine: String,
}

/// One brand's playbook (`<brand>.toml`).
#[derive(Debug, Clone, Deserialize)]
pub struct Playbook {
    /// Short key used on the CLI and in the CRM (e.g. "gnk").
    pub key: String,
    /// Display name (e.g. "GnK").
    pub name: String,
    /// Sign-off name for emails.
    pub signature: String,
    /// One-sentence, concrete brand introduction.
    pub one_liner: String,
    /// The kind of workflow / task / decision this brand tests for.
    pub motion: String,

    #[serde(default = "default_min_words")]
    pub min_words: usize,
    #[serde(default = "default_max_words")]
    pub max_words: usize,
    /// Independent signals a high-value candidate should show.
    #[serde(default = "default_min_signals")]
    pub min_signals: usize,
    /// Realistic upper bound on target company headcount. Keeps ICP derivation and
    /// qualification off enterprise giants a small/founder-led vendor can't land.
    /// `None` (unset) means no ceiling. Set per business in its private config.
    #[serde(default)]
    pub max_employees: Option<i64>,
    /// Free-text firmographic guidance woven into ICP derivation (e.g. "mid-market,
    /// founder-reachable; avoid Fortune 500 with entrenched internal IT").
    #[serde(default)]
    pub icp_note: String,

    #[serde(default)]
    pub system_concept_examples: Vec<String>,
    #[serde(default)]
    pub subject_examples: Vec<String>,
    /// How each vantage point shows up for this brand.
    #[serde(default)]
    pub vantage_notes: Vec<String>,
    /// Extra brand-specific rules the model must honor.
    #[serde(default)]
    pub requirements: Vec<String>,
    /// Brand-specific forbidden phrases (added to the shared list).
    #[serde(default)]
    pub forbidden: Vec<String>,
    /// Prose doctrine for this brand.
    pub doctrine: String,
}

fn default_min_words() -> usize {
    80
}
fn default_max_words() -> usize {
    150
}
fn default_min_signals() -> usize {
    1
}

/// All brands + the shared spine, loaded from a playbook directory.
pub struct Playbooks {
    pub shared: Shared,
    brands: BTreeMap<String, Playbook>,
}

impl Playbooks {
    /// Load `shared.toml` and every brand file from `dir`.
    pub fn load(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let shared: Shared = read_toml(&dir.join("shared.toml"))
            .context("loading shared.toml (the doctrine spine)")?;

        let mut brands = BTreeMap::new();
        for key in ["gnk", "wapahki", "outagehub"] {
            let path = dir.join(format!("{key}.toml"));
            let pb: Playbook =
                read_toml(&path).with_context(|| format!("loading brand playbook {key}"))?;
            if pb.key != key {
                return Err(anyhow!(
                    "{}: key = \"{}\" does not match filename \"{key}.toml\"",
                    path.display(),
                    pb.key
                ));
            }
            brands.insert(pb.key.clone(), pb);
        }

        Ok(Self { shared, brands })
    }

    /// Look up a brand by key, with a helpful error listing what's available.
    pub fn get(&self, key: &str) -> Result<&Playbook> {
        self.brands.get(key).ok_or_else(|| {
            anyhow!(
                "unknown brand '{key}'. Available: {}",
                self.keys().join(", ")
            )
        })
    }

    pub fn keys(&self) -> Vec<&str> {
        self.brands.keys().map(String::as_str).collect()
    }
}

fn read_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
}

impl Playbook {
    /// The full forbidden-phrase list for this brand (shared + brand-specific).
    pub fn forbidden<'a>(&'a self, shared: &'a Shared) -> Vec<&'a str> {
        shared
            .forbidden
            .iter()
            .chain(self.forbidden.iter())
            .map(String::as_str)
            .collect()
    }

    /// The system-prompt preamble every stage shares: who we are, the shared
    /// doctrine, this brand's doctrine, and the brand's structured knobs.
    pub fn system_prompt(&self, shared: &Shared) -> String {
        let mut s = String::new();
        s.push_str(&format!(
            "You are Andrew's outreach strategist and writer for {name}. Your job is to run \
             hypothesis-led discovery outreach that earns a reply, a correction, or a referral \
             — never a pitch.\n\n",
            name = self.name
        ));

        s.push_str("=== SHARED DOCTRINE (applies to every brand) ===\n");
        s.push_str(shared.doctrine.trim());
        s.push_str("\n\n");

        s.push_str(&format!("=== {} DOCTRINE ===\n", self.name.to_uppercase()));
        s.push_str(&format!("Brand: {}\n", self.name));
        s.push_str(&format!(
            "Brand intro (use close to verbatim): {}\n",
            self.one_liner
        ));
        s.push_str(&format!(
            "The motion this brand tests for: {}\n",
            self.motion
        ));
        s.push_str(&format!(
            "Email body length band: {}–{} words. A high-value candidate should show at \
             least {} independent signal(s).\n\n",
            self.min_words, self.max_words, self.min_signals
        ));
        s.push_str(self.doctrine.trim());
        s.push('\n');

        if !self.system_concept_examples.is_empty() {
            s.push_str("\nConcrete system-concept vocabulary (adapt, don't recite):\n");
            for c in &self.system_concept_examples {
                s.push_str(&format!("  - {c}\n"));
            }
        }
        if !self.subject_examples.is_empty() {
            s.push_str("\nSubject-line style (plain, forwardable):\n");
            for c in &self.subject_examples {
                s.push_str(&format!("  - {c}\n"));
            }
        }
        if !self.vantage_notes.is_empty() {
            s.push_str("\nHow vantage points show up for this brand:\n");
            for c in &self.vantage_notes {
                s.push_str(&format!("  - {c}\n"));
            }
        }
        if !self.requirements.is_empty() {
            s.push_str("\nHard requirements for this brand:\n");
            for c in &self.requirements {
                s.push_str(&format!("  - {c}\n"));
            }
        }

        let all_forbidden = self.forbidden(shared);
        s.push_str(
            "\nNEVER use these phrases in a subject or body (they read as generic sales copy):\n  ",
        );
        s.push_str(&all_forbidden.join(", "));
        s.push_str(&format!("\n\nSign emails as: {}\n", self.signature));
        s
    }
}

/// Result of the mechanical (non-LLM) lint of one piece of copy.
#[derive(Debug, Clone)]
pub struct Lint {
    /// Forbidden phrases found (as they appear in the doctrine list).
    pub forbidden_hits: Vec<String>,
    pub word_count: usize,
    pub length_ok: bool,
    /// Whether an email ends with the exact configured playbook signature.
    /// Generic lint calls leave this true; the pipeline sets it for email bodies.
    pub signature_ok: bool,
}

impl Default for Lint {
    fn default() -> Self {
        Self {
            forbidden_hits: Vec::new(),
            word_count: 0,
            length_ok: false,
            signature_ok: true,
        }
    }
}

/// Lint a body against the forbidden list and the word band. `min`/`max` of 0
/// disables the length check (used for subjects and non-email channels).
pub fn lint(text: &str, forbidden: &[&str], min: usize, max: usize) -> Lint {
    let lower = text.to_lowercase();
    let forbidden_hits = forbidden
        .iter()
        .filter(|p| lower.contains(&p.to_lowercase()))
        .map(|p| p.to_string())
        .collect();

    let word_count = text.split_whitespace().count();
    let length_ok = (min == 0 && max == 0) || (word_count >= min && word_count <= max);

    Lint {
        forbidden_hits,
        word_count,
        length_ok,
        signature_ok: true,
    }
}

/// True when the last non-empty line is exactly the configured signature.
pub fn has_exact_signature(body: &str, signature: &str) -> bool {
    let expected = signature.trim();
    !expected.is_empty()
        && body
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .is_some_and(|line| line.trim() == expected)
}

/// Make an email body end in exactly one configured signature.
///
/// The model occasionally abbreviates a configured full name (for example,
/// `Andrew Gordienko` to `Andrew`). Remove an exact signature or a leading-name
/// abbreviation from the end, drop a conventional closing immediately before
/// it, then append the canonical playbook value. Calling this repeatedly is
/// idempotent.
pub fn enforce_signature(body: &str, signature: &str) -> String {
    let expected = signature.trim();
    if expected.is_empty() {
        return body.trim_end().to_string();
    }

    let mut lines: Vec<&str> = body.trim_end().lines().collect();
    loop {
        while lines.last().is_some_and(|line| line.trim().is_empty()) {
            lines.pop();
        }
        let Some(last) = lines.last() else { break };
        if !is_signature_variant(last.trim(), expected) {
            break;
        }
        lines.pop();
        while lines.last().is_some_and(|line| line.trim().is_empty()) {
            lines.pop();
        }
        if lines
            .last()
            .is_some_and(|line| is_conventional_closing(line.trim()))
        {
            lines.pop();
        }
    }

    let mut out = lines.join("\n").trim_end().to_string();
    if !out.is_empty() {
        out.push_str("\n\n");
    }
    out.push_str(expected);
    out
}

fn is_signature_variant(candidate: &str, expected: &str) -> bool {
    let normalize = |value: &str| {
        value
            .split_whitespace()
            .map(|part| part.trim_matches([',', '.']).to_lowercase())
            .collect::<Vec<_>>()
    };
    let candidate = normalize(candidate);
    let expected = normalize(expected);
    !candidate.is_empty()
        && candidate.len() <= expected.len()
        && candidate
            .iter()
            .zip(&expected)
            .all(|(left, right)| left == right)
}

fn is_conventional_closing(line: &str) -> bool {
    matches!(
        line.trim_end_matches(',').to_ascii_lowercase().as_str(),
        "best" | "best regards" | "regards" | "thanks" | "thank you" | "cheers" | "sincerely"
    )
}

#[cfg(test)]
mod tests {
    use super::{enforce_signature, has_exact_signature};

    #[test]
    fn appends_the_configured_signature_when_missing() {
        let body = enforce_signature("One answer would help.", "Andrew Gordienko");
        assert_eq!(body, "One answer would help.\n\nAndrew Gordienko");
        assert!(has_exact_signature(&body, "Andrew Gordienko"));
    }

    #[test]
    fn replaces_an_abbreviated_signature_and_closing() {
        let body = enforce_signature(
            "One answer would help.\n\nBest,\nAndrew",
            "Andrew Gordienko",
        );
        assert_eq!(body, "One answer would help.\n\nAndrew Gordienko");
    }

    #[test]
    fn enforcement_is_idempotent_and_removes_duplicates() {
        let once = enforce_signature(
            "One answer would help.\n\nAndrew\n\nAndrew Gordienko",
            "Andrew Gordienko",
        );
        let twice = enforce_signature(&once, "Andrew Gordienko");
        assert_eq!(once, "One answer would help.\n\nAndrew Gordienko");
        assert_eq!(twice, once);
    }
}
