//! Discovery synthesis: the investor-facing artifact the whole engine exists to
//! produce. Outreach isn't the goal — CONVERGENCE is: many companies
//! independently describing the SAME recurring problem, with a few advancing up
//! the commitment ladder. This clusters every sourced company (its hypothesis)
//! plus any real replies into recurring themes, a commitment funnel, and an
//! honest assessment of how strong the evidence is.
//!
//! For Wapahki that reads "9 plants describe variable-format case packing; 3
//! would evaluate; 2 near a conditional LOI" — the pre-seed story. For GnK it's
//! the recurring $1M+ workflow clusters; for OutageHub the recurring
//! outage-sensitive decisions ready to consume an API.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::db::SharedDb;
use crate::engine::Engine;
use crate::knowledge::core_strategy_block;
use crate::metrics::{self, Funnel};
use crate::playbook::Playbook;

/// The finished synthesis for one brand.
#[derive(Debug)]
pub struct Synthesis {
    pub brand: String,
    pub headline: String,
    pub funnel: SynthFunnel,
    pub clusters: Vec<Cluster>,
    pub recommended_next: Vec<String>,
    pub missing_evidence: Vec<String>,
    pub company_count: usize,
}

/// The deterministic commitment funnel (counted from the db, not the model).
#[derive(Debug)]
pub struct SynthFunnel {
    pub companies: usize,
    pub people: usize,
    pub contacted: usize,
    pub replied: usize,
    pub positive_replies: usize,
    pub reply_rate: f64,
}

impl SynthFunnel {
    fn from_funnel(f: &Funnel, positive_replies: usize) -> Self {
        Self {
            companies: f.leads,
            people: f.people,
            contacted: f.contacted,
            replied: f.replied,
            positive_replies,
            reply_rate: f.reply_rate(),
        }
    }
}

/// One recurring problem shared across companies.
#[derive(Debug)]
pub struct Cluster {
    pub theme: String,
    pub why: String,
    pub companies: Vec<String>,
    pub evidence: String,
    pub furthest_commitment: String,
    pub strength: String,
}

// --- model output ----------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SynthesisDoc {
    #[serde(default)]
    headline: String,
    #[serde(default)]
    clusters: Vec<ClusterDoc>,
    #[serde(default)]
    recommended_next: Vec<String>,
    #[serde(default)]
    missing_evidence: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ClusterDoc {
    #[serde(default)]
    theme: String,
    #[serde(default)]
    why: String,
    #[serde(default)]
    companies: Vec<String>,
    #[serde(default)]
    evidence: String,
    #[serde(default)]
    furthest_commitment: String,
    #[serde(default)]
    strength: String,
}

impl From<ClusterDoc> for Cluster {
    fn from(c: ClusterDoc) -> Self {
        Self {
            theme: c.theme,
            why: c.why,
            companies: c.companies,
            evidence: c.evidence,
            furthest_commitment: c.furthest_commitment,
            strength: c.strength,
        }
    }
}

/// Build the discovery synthesis for one brand from its sourced companies + replies.
pub async fn synthesize(db: &SharedDb, client: &Engine, pb: &Playbook) -> Result<Synthesis> {
    let brand = pb.key.clone();
    let leads = db.list_leads(Some(&brand))?;
    let people = db.list_people(Some(&brand), None)?;
    let funnel = metrics::funnel(db, Some(&brand))?;

    let person_lead: HashMap<String, String> = people
        .iter()
        .map(|p| (p.id.clone(), p.lead_id.clone()))
        .collect();
    let brand_person_ids: HashSet<String> = people.iter().map(|p| p.id.clone()).collect();
    let replies: Vec<_> = db
        .list_replies(500)?
        .into_iter()
        .filter(|r| brand_person_ids.contains(&r.person_id))
        .collect();
    let positive_replies = replies
        .iter()
        .filter(|r| {
            matches!(
                r.classification.to_lowercase().as_str(),
                "interested" | "referral"
            )
        })
        .count();

    if leads.is_empty() {
        return Ok(Synthesis {
            brand,
            headline: "No sourced companies yet — run `source` first.".to_string(),
            funnel: SynthFunnel::from_funnel(&funnel, positive_replies),
            clusters: Vec::new(),
            recommended_next: Vec::new(),
            missing_evidence: Vec::new(),
            company_count: 0,
        });
    }

    // Compact per-company payload: the hypothesis being tested + any real reply
    // evidence. Capped so the prompt stays bounded on large books of business.
    let companies: Vec<Value> = leads
        .iter()
        .take(120)
        .map(|lead| {
            let contacted_people = people
                .iter()
                .filter(|p| {
                    p.lead_id == lead.id
                        && matches!(
                            p.status.as_str(),
                            "contacted" | "replied" | "bounced" | "unsubscribed"
                        )
                })
                .count();
            let lead_replies: Vec<Value> = replies
                .iter()
                .filter(|r| {
                    person_lead
                        .get(&r.person_id)
                        .is_some_and(|lid| lid == &lead.id)
                })
                .map(|r| {
                    json!({
                        "classification": r.classification,
                        "from": r.from_email,
                        "snippet": r.body.chars().take(400).collect::<String>(),
                    })
                })
                .collect();
            json!({
                "company": lead.name,
                "industry": lead.industry,
                "status": lead.status,
                "hypothesis": lead.hypothesis,
                "mechanism": lead.mechanism,
                "consequence_metric": lead.consequence_metric,
                "signals": lead.signals,
                "people_contacted": contacted_people,
                "replies": lead_replies,
            })
        })
        .collect();
    let company_count = companies.len();

    let system = "You are a market-research analyst producing an investor-grade discovery \
        synthesis. You cluster companies by the SAME underlying recurring problem, and you never \
        overstate commitment: base every cluster and every claimed commitment strictly on the \
        hypotheses and reply evidence provided. If replies are sparse, say so and mark strength \
        honestly (hypothesis-only clusters are 'weak').";
    let user = format!(
        "BRAND: {name}\nMOTION we are validating: {motion}\n\n\
         COMMITMENT LADDER (furthest rung reached, in order): sourced -> contacted -> replied -> \
         corrected/interested -> discovery call -> shared data or agreed to evaluate -> paid pilot \
         -> conditional LOI / contract.\n\n\
         COMPANIES (hypotheses + any reply evidence):\n{data}\n\n\
         Cluster the companies by the SAME recurring problem/task/decision (NOT by industry). For \
         each cluster return: a concrete theme; the 'why' (the crux that makes it a real market \
         gap); the member companies; the evidence we actually have (cite replies where present); \
         the furthest commitment any member reached; and strength (strong = multiple companies with \
         reply evidence; emerging = a few with thin evidence; weak = hypothesis-only). Then a \
         one-line investor HEADLINE in this brand's terms (e.g. 'N companies independently describe \
         X; M would evaluate; K near a pilot/LOI'), recommended_next actions, and missing_evidence \
         we should collect to strengthen the story.\n\n{doctrine}",
        name = pb.name,
        motion = pb.motion,
        data = serde_json::to_string_pretty(&companies).unwrap_or_default(),
        doctrine = core_strategy_block("synthesis"),
    );

    let doc: SynthesisDoc = client
        .structured_bulk("synthesis", system, &user, synth_schema())
        .await?;
    Ok(Synthesis {
        brand,
        headline: doc.headline,
        funnel: SynthFunnel::from_funnel(&funnel, positive_replies),
        clusters: doc.clusters.into_iter().map(Cluster::from).collect(),
        recommended_next: doc.recommended_next,
        missing_evidence: doc.missing_evidence,
        company_count,
    })
}

fn strength_rank(s: &str) -> u8 {
    match s.trim().to_lowercase().as_str() {
        "strong" => 0,
        "emerging" => 1,
        "weak" => 2,
        _ => 3,
    }
}

/// Clusters ordered strongest-evidence first.
fn sorted(clusters: &[Cluster]) -> Vec<&Cluster> {
    let mut refs: Vec<&Cluster> = clusters.iter().collect();
    refs.sort_by_key(|c| strength_rank(&c.strength));
    refs
}

/// The full markdown report (written to disk + shareable).
pub fn render_markdown(s: &Synthesis) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(out, "# {} — discovery synthesis\n", s.brand);
    let _ = writeln!(
        out,
        "> Model-generated synthesis of sourced hypotheses + real replies. Commitments are only \
         as strong as the reply evidence; verify before quoting to investors.\n"
    );
    if !s.headline.trim().is_empty() {
        let _ = writeln!(out, "**{}**\n", s.headline.trim());
    }

    let f = &s.funnel;
    let _ = writeln!(out, "## Funnel\n");
    let _ = writeln!(out, "- companies sourced: {}", f.companies);
    let _ = writeln!(out, "- people sourced: {}", f.people);
    let _ = writeln!(out, "- people contacted: {}", f.contacted);
    let _ = writeln!(
        out,
        "- replied: {} ({:.0}% of contacted)",
        f.replied, f.reply_rate
    );
    let _ = writeln!(
        out,
        "- positive replies (interested/referral): {}\n",
        f.positive_replies
    );

    let _ = writeln!(
        out,
        "## Clusters — recurring problems ({} companies analysed)\n",
        s.company_count
    );
    if s.clusters.is_empty() {
        let _ = writeln!(out, "_No clusters yet._\n");
    }
    for c in sorted(&s.clusters) {
        let _ = writeln!(
            out,
            "### [{}] {} ({} companies)\n",
            c.strength.trim(),
            c.theme.trim(),
            c.companies.len()
        );
        if !c.why.trim().is_empty() {
            let _ = writeln!(out, "**Why it's a gap:** {}\n", c.why.trim());
        }
        if !c.companies.is_empty() {
            let _ = writeln!(out, "**Companies:** {}\n", c.companies.join(", "));
        }
        if !c.evidence.trim().is_empty() {
            let _ = writeln!(out, "**Evidence:** {}\n", c.evidence.trim());
        }
        if !c.furthest_commitment.trim().is_empty() {
            let _ = writeln!(
                out,
                "**Furthest commitment:** {}\n",
                c.furthest_commitment.trim()
            );
        }
    }

    if !s.recommended_next.is_empty() {
        let _ = writeln!(out, "## Recommended next\n");
        for n in &s.recommended_next {
            let _ = writeln!(out, "- {n}");
        }
        let _ = writeln!(out);
    }
    if !s.missing_evidence.is_empty() {
        let _ = writeln!(out, "## Missing evidence to strengthen the story\n");
        for n in &s.missing_evidence {
            let _ = writeln!(out, "- {n}");
        }
        let _ = writeln!(out);
    }
    out
}

/// A compact console summary.
pub fn render_console(s: &Synthesis) -> String {
    let mut out = format!("discovery synthesis [{}]\n", s.brand);
    if !s.headline.trim().is_empty() {
        out.push_str(&format!("  {}\n", s.headline.trim()));
    }
    let f = &s.funnel;
    out.push_str(&format!(
        "  funnel: {} companies \u{b7} {} contacted \u{b7} {} replied ({:.0}%) \u{b7} {} positive\n",
        f.companies, f.contacted, f.replied, f.reply_rate, f.positive_replies
    ));
    if s.clusters.is_empty() {
        out.push_str("  (no clusters yet)\n");
    }
    for c in sorted(&s.clusters) {
        out.push_str(&format!(
            "  \u{2022} [{}] {} \u{2014} {} companies\n",
            c.strength.trim(),
            c.theme.trim(),
            c.companies.len()
        ));
    }
    out
}

fn str_array(desc: &str) -> Value {
    json!({ "type": "array", "items": { "type": "string" }, "description": desc })
}

fn synth_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["headline", "clusters"],
        "properties": {
            "headline": { "type": "string" },
            "clusters": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["theme", "companies", "strength"],
                    "properties": {
                        "theme": { "type": "string" },
                        "why": { "type": "string" },
                        "companies": str_array("Member company names (from the input)."),
                        "evidence": { "type": "string" },
                        "furthest_commitment": { "type": "string" },
                        "strength": { "type": "string", "enum": ["strong", "emerging", "weak"] }
                    }
                }
            },
            "recommended_next": str_array("Concrete next actions."),
            "missing_evidence": str_array("Evidence to collect to strengthen the story.")
        }
    })
}
