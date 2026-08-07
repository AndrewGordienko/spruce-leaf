//! Book knowledge — spruce-leaf's "SecondBrain".
//!
//! This is how we do what Genspark's SecondBrain does: you ingest business /
//! sales books once, we parse them into a persistent on-disk library, distill
//! each into compact *principle cards* (with the selected model), then retrieve the
//! handful of relevant principles for every pipeline stage so the model's
//! output is grounded in — and cites — real playbooks instead of its unaided
//! priors.
//!
//! The four levers that make the knowledge get *used consistently*:
//!   1. Persistence — the library is a standing layer re-queried on every run.
//!   2. Per-step retrieval — each stage gets the ~5 principles relevant to it.
//!   3. Distillation — books become reusable "when-to-use + the rule" cards.
//!   4. A forcing function — schemas require an `applied_principles` citation.
//!
//! Retrieval is lexical BM25 (zero extra API keys, deterministic). The
//! `retrieve`/`playbook_block` surface is deliberately backend-agnostic so a
//! semantic (e.g. Voyage embeddings) retriever can be fused in later.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;

use crate::engine::Engine;

// --- Data model ------------------------------------------------------------

/// The whole persisted library. The BM25 indexes and backing path are rebuilt
/// on load, not serialized.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Library {
    pub books: Vec<Book>,
    pub principles: Vec<Principle>,
    pub chunks: Vec<Chunk>,
    #[serde(skip)]
    path: PathBuf,
    #[serde(skip)]
    p_index: Bm25,
    #[serde(skip)]
    c_index: Bm25,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Book {
    pub id: String,
    pub title: String,
    pub author: String,
    pub source: String,
    /// Non-cryptographic content hash, used to skip re-ingesting the same file.
    pub hash: u64,
    pub n_chunks: usize,
    pub n_principles: usize,
}

/// A raw passage kept for grounding/citation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub id: String,
    pub book_id: String,
    pub book_title: String,
    pub ordinal: usize,
    pub heading: String,
    pub text: String,
}

/// A distilled, reusable principle — the high-signal unit we inject.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principle {
    pub id: String,
    pub book_id: String,
    pub book_title: String,
    pub name: String,
    /// The rule, in 1-2 sentences.
    pub summary: String,
    pub when_to_use: String,
    /// Keywords, for retrieval.
    pub tags: Vec<String>,
    /// Which pipeline stages this applies to: "companies" | "people" | "sequence".
    pub stages: Vec<String>,
}

/// The result of a retrieval, ready to format into a prompt.
pub struct Retrieved {
    pub principles: Vec<Principle>,
    pub chunks: Vec<Chunk>,
}

/// What one ingest run did.
pub struct IngestReport {
    pub books_added: usize,
    pub books_skipped: usize,
    pub chunks: usize,
    pub principles: usize,
    pub notes: Vec<String>,
}

pub type SharedLibrary = Arc<RwLock<Library>>;

// --- Load / save -----------------------------------------------------------

impl Library {
    pub fn load(path: impl Into<PathBuf>) -> Result<Library> {
        let path = path.into();
        let mut lib: Library = if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading knowledge library {}", path.display()))?;
            serde_json::from_str(&raw)
                .with_context(|| format!("parsing knowledge library {}", path.display()))?
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            Library::default()
        };
        lib.path = path;
        lib.build_index();
        Ok(lib)
    }

    pub fn save(&self) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&self.path, json)
            .with_context(|| format!("writing knowledge library {}", self.path.display()))
    }

    pub fn is_empty(&self) -> bool {
        self.principles.is_empty() && self.chunks.is_empty()
    }

    /// One-line status for banners / tool output.
    pub fn stats(&self) -> String {
        format!(
            "{} book(s), {} principle(s), {} passage(s)",
            self.books.len(),
            self.principles.len(),
            self.chunks.len()
        )
    }

    fn build_index(&mut self) {
        let p_docs: Vec<String> = self
            .principles
            .iter()
            .map(|p| {
                // Boost name + tags by repeating them.
                format!(
                    "{name} {name} {summary} {when} {tags} {tags}",
                    name = p.name,
                    summary = p.summary,
                    when = p.when_to_use,
                    tags = p.tags.join(" ")
                )
            })
            .collect();
        let c_docs: Vec<String> = self
            .chunks
            .iter()
            .map(|c| format!("{} {}", c.heading, c.text))
            .collect();
        self.p_index = Bm25::build(&p_docs);
        self.c_index = Bm25::build(&c_docs);
    }

    // --- Retrieval ---------------------------------------------------------

    /// Retrieve the most relevant principles (primary) and passages (grounding)
    /// for `query`.
    pub fn retrieve(&self, query: &str, n_principles: usize, n_chunks: usize) -> Retrieved {
        let principles = self
            .p_index
            .search(query, n_principles)
            .into_iter()
            .filter_map(|(i, _)| self.principles.get(i).cloned())
            .collect();
        let chunks = self
            .c_index
            .search(query, n_chunks)
            .into_iter()
            .filter_map(|(i, _)| self.chunks.get(i).cloned())
            .collect();
        Retrieved { principles, chunks }
    }

    /// Like [`retrieve`], but bias toward principles tagged for a given pipeline
    /// `stage` ("companies" | "people" | "sequence"). Principles with no stage
    /// tag are treated as applying everywhere. Falls back to unfiltered results
    /// if the stage filter would starve the list.
    pub fn retrieve_stage(
        &self,
        query: &str,
        stage: &str,
        n_principles: usize,
        n_chunks: usize,
    ) -> Retrieved {
        // Over-fetch, then filter by stage tag and take the top `n_principles`.
        let ranked = self.p_index.search(query, n_principles * 3);
        let mut principles: Vec<Principle> = ranked
            .iter()
            .filter_map(|&(i, _)| self.principles.get(i))
            .filter(|p| p.stages.is_empty() || p.stages.iter().any(|s| s == stage))
            .take(n_principles)
            .cloned()
            .collect();
        // If stage filtering left us short, backfill with the best unfiltered hits.
        if principles.len() < n_principles {
            for &(i, _) in &ranked {
                if principles.len() >= n_principles {
                    break;
                }
                if let Some(p) = self.principles.get(i) {
                    if !principles.iter().any(|x| x.id == p.id) {
                        principles.push(p.clone());
                    }
                }
            }
        }
        let chunks = self
            .c_index
            .search(query, n_chunks)
            .into_iter()
            .filter_map(|(i, _)| self.chunks.get(i).cloned())
            .collect();
        Retrieved { principles, chunks }
    }

    // --- Ingestion ---------------------------------------------------------

    /// Ingest a file or a directory (recursively) of books. `.txt`/`.md` are
    /// read directly; `.pdf` is text-extracted. Unsupported files are skipped.
    pub async fn ingest(
        &mut self,
        client: &Engine,
        path: &Path,
        distill: bool,
        max_sections: usize,
        concurrency: usize,
    ) -> Result<IngestReport> {
        let mut files = Vec::new();
        collect_files(path, &mut files);
        files.sort();

        let mut report = IngestReport {
            books_added: 0,
            books_skipped: 0,
            chunks: 0,
            principles: 0,
            notes: Vec::new(),
        };

        if files.is_empty() {
            report.notes.push(format!(
                "no supported files (.txt/.md/.pdf) under {}",
                path.display()
            ));
            return Ok(report);
        }

        let mut existing: HashSet<u64> = self.books.iter().map(|b| b.hash).collect();

        for file in files {
            let text = match extract_text(&file) {
                Ok(t) if t.trim().len() > 200 => t,
                Ok(_) => {
                    report.notes.push(format!(
                        "skipped {} (too little extractable text)",
                        file.display()
                    ));
                    continue;
                }
                Err(e) => {
                    report
                        .notes
                        .push(format!("skipped {}: {e:#}", file.display()));
                    continue;
                }
            };

            let hash = hash_text(&text);
            if existing.contains(&hash) {
                report.books_skipped += 1;
                report
                    .notes
                    .push(format!("skipped {} (already ingested)", file.display()));
                continue;
            }

            let title = title_from_path(&file);
            let book_id = format!("{}-{:06x}", slug(&title), hash & 0xff_ffff);

            // Chunk.
            let raw_chunks = chunk_text(&text);
            let chunks: Vec<Chunk> = raw_chunks
                .into_iter()
                .enumerate()
                .map(|(i, (heading, body))| Chunk {
                    id: format!("{book_id}#{i}"),
                    book_id: book_id.clone(),
                    book_title: title.clone(),
                    ordinal: i,
                    heading,
                    text: body,
                })
                .collect();

            // Distill principle cards.
            let mut principles = Vec::new();
            if distill {
                let sections = section_batches(&chunks, 12_000, max_sections);
                if sections.sampled {
                    report.notes.push(format!(
                        "{}: sampled {} of {} sections for principle distillation \
                         (raise --max-sections for fuller coverage)",
                        title, sections.used, sections.total
                    ));
                }
                let results: Vec<Result<Vec<RawPrinciple>>> =
                    stream::iter(sections.batches.into_iter().map(|batch| {
                        let title = title.clone();
                        async move { distill_batch(client, &title, &batch).await }
                    }))
                    .buffered(concurrency.max(1))
                    .collect()
                    .await;

                let mut raws = Vec::new();
                let mut failed_batches = 0usize;
                for (index, result) in results.into_iter().enumerate() {
                    match result {
                        Ok(batch) => raws.extend(batch),
                        Err(error) => {
                            failed_batches += 1;
                            report.notes.push(format!(
                                "{title}: principle distillation failed for section {}: {error:#}",
                                index + 1
                            ));
                        }
                    }
                }
                if failed_batches > 0 && raws.is_empty() {
                    report.books_skipped += 1;
                    report.notes.push(format!(
                        "{title}: not ingested because every principle-distillation call failed; retry when the selected model CLI is available"
                    ));
                    continue;
                }

                let mut used_ids: HashSet<String> =
                    self.principles.iter().map(|p| p.id.clone()).collect();
                let mut used_names: HashSet<String> = HashSet::new();
                for raw in raws {
                    let key = raw.name.to_lowercase();
                    if raw.name.trim().is_empty() || !used_names.insert(key) {
                        continue; // drop blank / duplicate names within this book
                    }
                    let id = unique_id(&slug(&raw.name), &mut used_ids);
                    principles.push(Principle {
                        id,
                        book_id: book_id.clone(),
                        book_title: title.clone(),
                        name: raw.name,
                        summary: raw.summary,
                        when_to_use: raw.when_to_use,
                        tags: raw.tags,
                        stages: normalize_stages(raw.stages),
                    });
                }
            }

            report.books_added += 1;
            report.chunks += chunks.len();
            report.principles += principles.len();
            self.books.push(Book {
                id: book_id,
                title,
                author: "unknown".to_string(),
                source: file.display().to_string(),
                hash,
                n_chunks: chunks.len(),
                n_principles: principles.len(),
            });
            self.chunks.extend(chunks);
            self.principles.extend(principles);
            existing.insert(hash);
        }

        self.build_index();
        self.save()?;
        Ok(report)
    }
}

impl Retrieved {
    pub fn is_empty(&self) -> bool {
        self.principles.is_empty() && self.chunks.is_empty()
    }

    /// Format the retrieval as a playbook block to splice into a stage prompt.
    pub fn playbook_block(&self) -> String {
        if self.is_empty() {
            return String::new();
        }
        let mut s = String::from(
            "AUTHORITATIVE PLAYBOOK (distilled from the sales/business library). Apply the \
             relevant principles below to your reasoning, and cite the [id]s you actually use \
             in `applied_principles`:",
        );
        for p in &self.principles {
            s.push_str(&format!(
                "\n\n[{}] {} — {} (from {}).",
                p.id, p.name, p.summary, p.book_title
            ));
            if !p.when_to_use.trim().is_empty() {
                s.push_str(&format!(" When to use: {}", p.when_to_use.trim()));
            }
        }
        if !self.chunks.is_empty() {
            s.push_str("\n\nSupporting passages (grounding only — cite a principle, not these):");
            for c in &self.chunks {
                s.push_str(&format!(
                    "\n- [{}] \"{}\" — {}",
                    c.id,
                    snippet(&c.text, 320),
                    c.book_title
                ));
            }
        }
        s
    }

    /// A human-readable listing for the `search_knowledge` tool.
    pub fn human_listing(&self) -> String {
        if self.is_empty() {
            return "No matching principles in the library yet. Ingest books with `ingest_book` \
                    or `spruce-leaf ingest <path>`."
                .to_string();
        }
        let mut s = String::new();
        for p in &self.principles {
            s.push_str(&format!(
                "• [{}] {} ({}) — {}",
                p.id, p.name, p.book_title, p.summary
            ));
            if !p.when_to_use.trim().is_empty() {
                s.push_str(&format!(" [when: {}]", p.when_to_use.trim()));
            }
            s.push('\n');
        }
        s
    }
}

impl IngestReport {
    pub fn summary(&self) -> String {
        let mut s = format!(
            "Ingested {} book(s) ({} skipped): {} passages, {} principle cards.",
            self.books_added, self.books_skipped, self.chunks, self.principles
        );
        for n in &self.notes {
            s.push_str(&format!("\n  · {n}"));
        }
        s
    }
}

/// Convenience: load a shared library at the given path.
pub fn open(path: impl AsRef<Path>) -> Result<SharedLibrary> {
    let lib = Library::load(path.as_ref().to_path_buf())?;
    Ok(Arc::new(RwLock::new(lib)))
}

// --- Distillation (model) --------------------------------------------------

#[derive(Deserialize, Default)]
struct RawPrincipleSet {
    #[serde(default)]
    principles: Vec<RawPrinciple>,
}

#[derive(Deserialize, Default)]
struct RawPrinciple {
    name: String,
    #[serde(default)]
    summary: String,
    #[serde(default)]
    when_to_use: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    stages: Vec<String>,
}

const DISTILL_SYSTEM: &str = "\
You distill business and sales books into reusable, actionable principles a B2B revenue \
strategist can apply. Extract durable ideas — frameworks, heuristics, rules of thumb — not \
chapter summaries or anecdotes. Each principle must be specific enough to change a concrete \
decision (who to target, who buys, how to frame a message, how to price). Prefer fewer, \
sharper principles over many vague ones.";

async fn distill_batch(
    client: &Engine,
    book_title: &str,
    excerpt: &str,
) -> Result<Vec<RawPrinciple>> {
    let user = format!(
        "From this excerpt of \"{book_title}\", extract up to 6 concrete, reusable principles \
a B2B sales strategist could apply.\n\n\
For each: `name` (a short handle), `summary` (the rule in 1-2 sentences), `when_to_use` (the \
situation it applies to), `tags` (5-10 lowercase keywords for retrieval), and `stages` (which \
of these it informs: \"companies\" = which accounts to target, \"people\" = who in the org \
buys/owns the problem, \"sequence\" = outreach messaging). Only include principles actually \
supported by the text.\n\nExcerpt:\n{excerpt}"
    );
    let set: RawPrincipleSet = client
        .structured(DISTILL_SYSTEM, &user, principles_schema())
        .await?;
    Ok(set.principles)
}

fn principles_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["principles"],
        "properties": {
            "principles": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["name", "summary", "when_to_use", "tags", "stages"],
                    "properties": {
                        "name": { "type": "string" },
                        "summary": { "type": "string" },
                        "when_to_use": { "type": "string" },
                        "tags": { "type": "array", "items": { "type": "string" } },
                        "stages": { "type": "array", "items": { "type": "string" } }
                    }
                }
            }
        }
    })
}

fn normalize_stages(stages: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = stages
        .into_iter()
        .map(|s| s.to_lowercase())
        .filter(|s| s == "companies" || s == "people" || s == "sequence")
        .collect();
    out.sort();
    out.dedup();
    if out.is_empty() {
        out = vec!["companies".into(), "people".into(), "sequence".into()];
    }
    out
}

// --- Text extraction & chunking -------------------------------------------

fn collect_files(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_dir() {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                collect_files(&entry.path(), out);
            }
        }
    } else if is_supported(path) {
        out.push(path.to_path_buf());
    }
}

fn is_supported(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .as_deref(),
        Some("txt") | Some("md") | Some("markdown") | Some("text") | Some("pdf")
    )
}

fn extract_text(path: &Path) -> Result<String> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase())
        .unwrap_or_default();
    let text = if ext == "pdf" {
        pdf_extract::extract_text(path)
            .with_context(|| format!("extracting text from PDF {}", path.display()))
    } else {
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))
    }?;

    // Gutenberg plain-text files commonly use CRLF. The chunker splits on
    // blank LF-delimited paragraphs, so normalize all input sources first;
    // otherwise an entire CRLF book becomes one enormous passage.
    Ok(text.replace("\r\n", "\n").replace('\r', "\n"))
}

/// Split text into overlapping ~800-token chunks, tracking the nearest heading.
fn chunk_text(text: &str) -> Vec<(String, String)> {
    const TARGET: usize = 3200; // ~800 tokens at ~4 chars/token
    const OVERLAP: usize = 600;

    let paras: Vec<&str> = text
        .split("\n\n")
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect();

    let mut chunks = Vec::new();
    let mut heading = String::new();
    let mut cur = String::new();

    for p in paras {
        if let Some(h) = detect_heading(p) {
            heading = h;
        }
        if cur.len() + p.len() + 2 > TARGET && !cur.is_empty() {
            chunks.push((heading.clone(), cur.clone()));
            cur = tail_chars(&cur, OVERLAP);
        }
        if !cur.is_empty() {
            cur.push_str("\n\n");
        }
        cur.push_str(p);
    }
    if !cur.trim().is_empty() {
        chunks.push((heading, cur));
    }
    chunks
}

fn detect_heading(para: &str) -> Option<String> {
    let line = para.lines().next().unwrap_or("").trim();
    if line.is_empty() {
        return None;
    }
    if let Some(rest) = line.strip_prefix('#') {
        return Some(rest.trim_start_matches('#').trim().to_string());
    }
    let lower = line.to_lowercase();
    if line.len() < 70 && (lower.starts_with("chapter") || lower.starts_with("part ")) {
        return Some(line.to_string());
    }
    None
}

/// Group consecutive chunks into ~`target`-char sections for distillation,
/// evenly sampling down to `max` sections when there are more.
struct Sections {
    batches: Vec<String>,
    total: usize,
    used: usize,
    sampled: bool,
}

fn section_batches(chunks: &[Chunk], target: usize, max: usize) -> Sections {
    let mut groups: Vec<String> = Vec::new();
    let mut cur = String::new();
    for ch in chunks {
        if cur.len() + ch.text.len() > target && !cur.is_empty() {
            groups.push(std::mem::take(&mut cur));
        }
        cur.push_str(&ch.text);
        cur.push_str("\n\n");
    }
    if !cur.trim().is_empty() {
        groups.push(cur);
    }

    let total = groups.len();
    if max > 0 && total > max {
        let step = total as f32 / max as f32;
        let sampled: Vec<String> = (0..max)
            .map(|i| groups[((i as f32 * step) as usize).min(total - 1)].clone())
            .collect();
        return Sections {
            used: sampled.len(),
            batches: sampled,
            total,
            sampled: true,
        };
    }
    Sections {
        used: total,
        batches: groups,
        total,
        sampled: false,
    }
}

// --- BM25 ------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct Bm25 {
    /// term -> list of (doc index, term frequency)
    postings: HashMap<String, Vec<(usize, u32)>>,
    doc_len: Vec<u32>,
    avgdl: f32,
    n: usize,
}

impl Bm25 {
    fn build(docs: &[String]) -> Bm25 {
        let mut postings: HashMap<String, Vec<(usize, u32)>> = HashMap::new();
        let mut doc_len = Vec::with_capacity(docs.len());
        for (i, d) in docs.iter().enumerate() {
            let toks = tokenize(d);
            doc_len.push(toks.len() as u32);
            let mut tf: HashMap<String, u32> = HashMap::new();
            for t in toks {
                *tf.entry(t).or_default() += 1;
            }
            for (t, c) in tf {
                postings.entry(t).or_default().push((i, c));
            }
        }
        let total: u64 = doc_len.iter().map(|&x| x as u64).sum();
        let n = docs.len();
        let avgdl = if n > 0 { total as f32 / n as f32 } else { 0.0 };
        Bm25 {
            postings,
            doc_len,
            avgdl,
            n,
        }
    }

    fn search(&self, query: &str, k: usize) -> Vec<(usize, f32)> {
        if self.n == 0 || k == 0 {
            return Vec::new();
        }
        const K1: f32 = 1.5;
        const B: f32 = 0.75;
        let avgdl = self.avgdl.max(1.0);
        let mut scores: HashMap<usize, f32> = HashMap::new();
        for t in tokenize(query) {
            let Some(post) = self.postings.get(&t) else {
                continue;
            };
            let df = post.len() as f32;
            let idf = (((self.n as f32 - df + 0.5) / (df + 0.5)) + 1.0).ln();
            for &(doc, tf) in post {
                let tf = tf as f32;
                let dl = self.doc_len[doc] as f32;
                let denom = tf + K1 * (1.0 - B + B * dl / avgdl);
                *scores.entry(doc).or_default() += idf * (tf * (K1 + 1.0)) / denom;
            }
        }
        let mut ranked: Vec<(usize, f32)> = scores.into_iter().filter(|&(_, s)| s > 0.0).collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(k);
        ranked
    }
}

fn tokenize(s: &str) -> Vec<String> {
    s.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 2 && !is_stopword(w))
        .map(|w| w.to_string())
        .collect()
}

fn is_stopword(w: &str) -> bool {
    matches!(
        w,
        "the"
            | "and"
            | "for"
            | "are"
            | "but"
            | "not"
            | "you"
            | "your"
            | "with"
            | "this"
            | "that"
            | "from"
            | "have"
            | "has"
            | "was"
            | "were"
            | "will"
            | "would"
            | "their"
            | "they"
            | "them"
            | "its"
            | "into"
            | "than"
            | "then"
            | "when"
            | "what"
            | "who"
            | "how"
            | "why"
            | "can"
            | "could"
            | "should"
            | "about"
            | "which"
            | "these"
            | "those"
            | "such"
            | "each"
            | "any"
            | "all"
            | "one"
            | "two"
    )
}

// --- Small helpers ---------------------------------------------------------

fn hash_text(t: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

fn slug(s: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for c in s.chars() {
        if c.is_alphanumeric() {
            out.extend(c.to_lowercase());
            prev_dash = false;
        } else if !prev_dash && !out.is_empty() {
            out.push('-');
            prev_dash = true;
        }
    }
    let out = out.trim_matches('-').to_string();
    if out.is_empty() {
        "x".to_string()
    } else {
        out
    }
}

fn unique_id(base: &str, used: &mut HashSet<String>) -> String {
    if used.insert(base.to_string()) {
        return base.to_string();
    }
    let mut i = 2;
    loop {
        let candidate = format!("{base}-{i}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        i += 1;
    }
}

fn title_from_path(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("Untitled");
    let cleaned = stem.replace(['_', '-'], " ");
    cleaned
        .split_whitespace()
        .map(|w| {
            let mut cs = w.chars();
            match cs.next() {
                Some(f) => f.to_uppercase().collect::<String>() + cs.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn tail_chars(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    // Walk back to a char boundary at least `n` bytes from the end.
    let mut start = s.len() - n;
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    s[start..].to_string()
}

fn snippet(s: &str, max: usize) -> String {
    let collapsed = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max {
        return collapsed;
    }
    let truncated: String = collapsed.chars().take(max).collect();
    format!("{truncated}…")
}

#[cfg(test)]
mod tests {
    use super::Library;
    use crate::engine::{Backend, Engine};

    #[tokio::test]
    async fn skips_duplicate_content_within_one_ingest_run() {
        let root = std::env::temp_dir().join(format!(
            "spruce-leaf-knowledge-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let text = "A durable sales principle needs enough repeated source text to be ingested. "
            .repeat(8);
        std::fs::write(root.join("first.txt"), &text).unwrap();
        std::fs::write(root.join("duplicate.txt"), &text).unwrap();

        let mut library = Library::load(root.join("knowledge.json")).unwrap();
        let engine = Engine::new(Backend::Codex, None);
        let report = library.ingest(&engine, &root, false, 1, 1).await.unwrap();

        assert_eq!(report.books_added, 1);
        assert_eq!(report.books_skipped, 1);
        assert_eq!(library.books.len(), 1);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
