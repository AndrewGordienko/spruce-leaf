//! The doctrine-driven pipeline:
//!   1. research accounts (fact / inference / hypothesis / mechanism / metric)
//!   2. map contacts by vantage point (per account, concurrent)
//!   3. write an N-touch unfolding investigation (per contact, concurrent)
//!   4. mechanical lint (forbidden phrases + length) + LLM pre-send critique that
//!      rewrites each touch to pass — then apply the revisions.

use anyhow::Result;
use futures::stream::{self, StreamExt, TryStreamExt};

use crate::domain::*;
use crate::engine::Claude;
use crate::knowledge::Library;
use crate::playbook::{self, Lint, Playbook, Shared};
use crate::prompts;

/// A live progress sink for the pipeline. The interactive UI implements this to
/// paint a progress tree; the non-interactive path uses the no-op `()` impl.
pub trait Progress {
    /// The account-research stage returned these accounts (in discovery order).
    fn accounts_found(&self, names: &[String]);
    /// `account` had `contacts` people mapped to it.
    fn account_contacts(&self, account: &str, contacts: usize);
    /// One more contact's sequence finished (written + critiqued) for `account`.
    fn sequence_done(&self, account: &str);
}

impl Progress for () {
    fn accounts_found(&self, _: &[String]) {}
    fn account_contacts(&self, _: &str, _: usize) {}
    fn sequence_done(&self, _: &str) {}
}

/// Everything a run needs that doesn't change between stages.
pub struct Run<'a> {
    pub client: &'a Claude,
    pub pb: &'a Playbook,
    pub shared: &'a Shared,
    /// The book-knowledge library, retrieved from per stage. Empty is fine —
    /// retrieval then returns nothing and the prompts are unchanged.
    pub library: &'a Library,
    /// Prebuilt brand system prompt (shared + brand doctrine + knobs).
    pub system: String,
    pub concurrency: usize,
    pub critique: bool,
    /// Live progress sink (the UI tree, or `&()` for none).
    pub reporter: &'a dyn Progress,
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    client: &Claude,
    pb: &Playbook,
    shared: &Shared,
    library: &Library,
    thesis: &str,
    n_accounts: usize,
    n_contacts: usize,
    n_touches: usize,
    concurrency: usize,
    critique: bool,
    reporter: &dyn Progress,
) -> Result<Campaign> {
    let run = Run {
        client,
        pb,
        shared,
        library,
        system: pb.system_prompt(shared),
        concurrency,
        critique,
        reporter,
    };

    let accounts = find_accounts(&run, thesis, n_accounts).await?;
    let names: Vec<String> = accounts.accounts.iter().map(|a| a.name.clone()).collect();
    run.reporter.accounts_found(&names);

    let plans = stream::iter(
        accounts
            .accounts
            .into_iter()
            .map(|a| plan_account(&run, a, n_contacts, n_touches)),
    )
    .buffered(concurrency)
    .try_collect::<Vec<AccountPlan>>()
    .await?;

    Ok(Campaign {
        brand: pb.key.clone(),
        thesis: thesis.to_string(),
        accounts: plans,
    })
}

async fn plan_account(
    run: &Run<'_>,
    account: Account,
    n_contacts: usize,
    n_touches: usize,
) -> Result<AccountPlan> {
    let contacts = find_contacts(run, &account, n_contacts).await?;
    run.reporter.account_contacts(&account.name, contacts.contacts.len());

    let plans = stream::iter(contacts.contacts.into_iter().map(|mut contact| {
        contact.vantage = normalize_vantage(&contact.vantage);
        let account = account.clone();
        async move {
            let mut sequence = write_sequence(run, &account, &contact, n_touches).await?;
            let reviews = if run.critique {
                let review = critique(run, &account, &contact, &sequence).await?;
                apply_revisions(&mut sequence, &review);
                review.reviews
            } else {
                Vec::new()
            };
            run.reporter.sequence_done(&account.name);
            Ok::<ContactPlan, anyhow::Error>(ContactPlan { contact, sequence, reviews })
        }
    }))
    .buffered(run.concurrency)
    .try_collect::<Vec<ContactPlan>>()
    .await?;

    Ok(AccountPlan { account, contacts: plans })
}

async fn find_accounts(run: &Run<'_>, thesis: &str, n: usize) -> Result<Accounts> {
    let query = format!(
        "which accounts to target; ideal customer profile; qualifying an expensive workflow: \
         {thesis}; {motion}",
        motion = run.pb.motion,
    );
    let knowledge = run.library.retrieve_stage(&query, "companies", 5, 2).playbook_block();
    let user = prompts::accounts_user(run.pb, thesis, n, &knowledge);
    run.client
        .structured::<Accounts>(&run.system, &user, prompts::accounts_schema())
        .await
}

async fn find_contacts(run: &Run<'_>, account: &Account, n: usize) -> Result<Contacts> {
    let query = format!(
        "who to reach inside the account; economic buyer, champion, decision maker, gatekeeper, \
         the person who owns the problem: {}",
        account.hypothesis,
    );
    let knowledge = run.library.retrieve_stage(&query, "people", 4, 1).playbook_block();
    let user = prompts::contacts_user(run.pb, account, n, &knowledge);
    run.client
        .structured::<Contacts>(&run.system, &user, prompts::contacts_schema())
        .await
}

async fn write_sequence(
    run: &Run<'_>,
    account: &Account,
    contact: &Contact,
    n: usize,
) -> Result<Sequence> {
    let query = format!(
        "cold outreach messaging; email copywriting; opening lines, value framing, objections, \
         the ask; earning a reply about: {}",
        account.hypothesis,
    );
    let knowledge = run.library.retrieve_stage(&query, "sequence", 6, 2).playbook_block();
    let user = prompts::sequence_user(run.pb, account, contact, n, &knowledge);
    run.client
        .structured::<Sequence>(&run.system, &user, prompts::sequence_schema())
        .await
}

async fn critique(
    run: &Run<'_>,
    account: &Account,
    contact: &Contact,
    sequence: &Sequence,
) -> Result<SequenceReview> {
    let forbidden = run.pb.forbidden(run.shared);
    let lints: Vec<Lint> = sequence
        .touches
        .iter()
        .map(|t| lint_touch(run.pb, t, &forbidden))
        .collect();

    let user = prompts::critique_user(run.pb, account, contact, sequence, &lints);
    run.client
        .structured::<SequenceReview>(&run.system, &user, prompts::critique_schema())
        .await
}

/// Lint one touch: forbidden phrases across subject+body, length band on the
/// body (skipped for call scripts, which aren't word-bounded).
fn lint_touch(pb: &Playbook, t: &Touch, forbidden: &[&str]) -> Lint {
    let (min, max) = if t.channel.eq_ignore_ascii_case("call") {
        (0, 0)
    } else {
        (pb.min_words, pb.max_words)
    };
    let mut lint = playbook::lint(&t.body, forbidden, min, max);
    if !t.subject.is_empty() {
        let subj = playbook::lint(&t.subject, forbidden, 0, 0);
        for hit in subj.forbidden_hits {
            if !lint.forbidden_hits.contains(&hit) {
                lint.forbidden_hits.push(hit);
            }
        }
    }
    lint
}

/// Replace each touch's subject/body with the critic's revised copy.
fn apply_revisions(sequence: &mut Sequence, review: &SequenceReview) {
    for r in &review.reviews {
        if let Some(t) = sequence.touches.iter_mut().find(|t| t.stage == r.stage) {
            if !r.revised_body.trim().is_empty() {
                t.body = r.revised_body.clone();
            }
            // Only overwrite the subject on channels that have one.
            if !t.subject.is_empty() && !r.revised_subject.trim().is_empty() {
                t.subject = r.revised_subject.clone();
            }
        }
    }
}

/// Normalize a model-supplied vantage to the canonical set for consistent badges.
fn normalize_vantage(raw: &str) -> String {
    let v = raw.trim().to_lowercase().replace([' ', '-'], "_");
    match v.as_str() {
        "process_owner" | "operator" | "operational_executive" | "technical_evaluator"
        | "economic_buyer" | "router" => v,
        _ => raw.trim().to_lowercase(),
    }
}
