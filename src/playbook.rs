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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

    /// Compact sourcing prompt: firmographic translation does not need several
    /// thousand words of copywriting doctrine.
    pub fn icp_system_prompt(&self) -> String {
        format!(
            "You translate a concrete commercial thesis into conservative Apollo search filters for {name}. Target organizations where this motion is plausible: {motion}. Prefer reachable workflow owners over prestige titles. Never invent companies, people, facts, urgency, or financial impact. Return only the requested structured data.",
            name = self.name,
            motion = self.motion,
        )
    }

    /// Compact company qualification prompt, focused on evidence boundaries.
    pub fn qualification_system_prompt(&self) -> String {
        format!(
            "You qualify real companies for {name}. Motion: {motion}. Separate supported facts, reasonable inferences, and a falsifiable workflow hypothesis. A company qualifies only when at least {signals} independent signals support one specific recurring workflow and the company is realistically winnable. Never invent systems, customers, volumes, savings, urgency, or dollar impact. Name the mechanism, a measurable non-dollar consequence, the strongest objection, and what would falsify the thesis. Return only the requested structured data.",
            name = self.name,
            motion = self.motion,
            signals = self.min_signals,
        )
    }

    /// Compact people-mapping prompt. Copy rules are irrelevant at this stage.
    pub fn vantage_system_prompt(&self) -> String {
        format!(
            "You map real people to the workflow vantage they can credibly observe for {name}. Choose by access to the work, not seniority. Prefer a process owner or operator; then an operational executive. Use a router only when ownership is unclear. Mark at most two primary contacts per account. Never infer responsibilities beyond the supplied title and account hypothesis. Return only the requested structured data.",
            name = self.name,
        )
    }

    /// Compact copy prompt used only for buyer-facing writing. It carries the
    /// enforceable rules and forbidden list, not the long-form internal essay.
    pub fn copy_system_prompt(&self, shared: &Shared) -> String {
        let requirements = self
            .requirements
            .iter()
            .map(|rule| format!("- {rule}"))
            .collect::<Vec<_>>()
            .join("\n");
        let subjects = self.subject_examples.join(" | ");
        let forbidden = self.forbidden(shared).join(", ");
        format!(
            "You write founder-led cold outreach for Andrew and {name}. Write warm, natural, plain English that a busy operator reads once and understands. The goal is a useful reply, correction, or referral — earned by being specific and human, not by pitching.\n\nSELLER: {intro}\nMOTION: {motion}\n\nEach recipient comes with a per-touch `sequence_plan` (objective, angle, channel, ask). Follow it: write the actual sendable copy that executes that plan, touch by touch.\n\nCore rules:\n- Open every EMAIL with a short greeting on its own line: `Hi <first name>,`. LinkedIn notes and calls take no greeting.\n- After the greeting, lead with one recognizable operating moment in their world — not a compliment, and nothing about Andrew or software.\n- State only supplied observed facts as facts. Frame everything else as a question or modest uncertainty. Never fabricate numbers, savings, systems, customers, or urgency, and never claim what their current tools can't do.\n- Give one crisp framing, then exactly one clear ask the recipient can answer from their vantage (one question mark per email). Never ask a router to evaluate the business case.\n- Mention {name} only once, in touch 1. Keep paragraphs short and never explain the outreach strategy.\n- Each later touch adds one new diagnostic, consequence, objection, artifact, or routing angle — never repeat the opening premise.\n- Keep human judgment central and describe at most one narrow system.\n- Email 1 is {min}–{max} words including the greeting and `{signature}`; later emails 25–70; LinkedIn 12–40; calls 12–45; the final close 20–45 with no question.\n- Seven-touch channel order: email, LinkedIn, email, call, email, LinkedIn, email; finish by day 21.\n- Every email ends with a brief warm sign-off line, then exactly `{signature}` on its own line.\n\nBrand-specific rules:\n{requirements}\n\nPlain subject examples: {subjects}\nForbidden phrases: {forbidden}\nReturn only the requested structured data.",
            name = self.name,
            intro = self.one_liner,
            motion = self.motion,
            min = self.min_words,
            max = self.max_words,
            signature = self.signature,
        )
    }

    /// The system-prompt preamble every stage shares: who we are, the shared
    /// doctrine, this brand's doctrine, and the brand's structured knobs.
    #[allow(dead_code)]
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
    use super::{enforce_signature, has_exact_signature, Playbooks};

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

    #[test]
    fn stage_prompts_are_materially_smaller_than_the_legacy_full_doctrine() {
        let playbooks = Playbooks::load("playbooks").expect("load playbooks");
        let gnk = playbooks.get("gnk").expect("gnk");
        let full = gnk.system_prompt(&playbooks.shared);
        let copy = gnk.copy_system_prompt(&playbooks.shared);
        assert!(copy.split_whitespace().count() * 2 < full.split_whitespace().count());
        assert!(gnk.icp_system_prompt().split_whitespace().count() < 100);
        assert!(gnk.qualification_system_prompt().split_whitespace().count() < 150);
        assert!(copy.contains("natural, plain English"));
    }
}
