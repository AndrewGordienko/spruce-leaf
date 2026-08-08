//! The doctrine-driven pipeline:
//!   1. research accounts (fact / inference / hypothesis / mechanism / metric)
//!   2. map contacts by vantage point (per account, concurrent)
//!   3. write an N-touch unfolding investigation (per contact, concurrent)
//!   4. mechanical lint (forbidden phrases + length) + LLM pre-send critique that
//!      rewrites each touch to pass — then apply the revisions.

use anyhow::Result;
use futures::stream::{self, StreamExt, TryStreamExt};

use crate::domain::*;
use crate::engine::Engine;
use crate::knowledge::{core_strategy_block, Library};
use crate::outreach;
use crate::playbook::{self, Playbook, Shared};
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
    pub client: &'a Engine,
    pub pb: &'a Playbook,
    pub shared: &'a Shared,
    /// The book-knowledge library, retrieved from per stage. Empty is fine —
    /// retrieval then returns nothing and the prompts are unchanged.
    pub library: &'a Library,
    /// Compact stage prompts are built per task; no full doctrine is repeated.
    pub concurrency: usize,
    pub critique: bool,
    /// Live progress sink (the UI tree, or `&()` for none).
    pub reporter: &'a dyn Progress,
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    client: &Engine,
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
    let mut contacts = find_contacts(run, &account, n_contacts).await?.contacts;
    contacts.sort_by(|left, right| {
        contact_priority(right)
            .cmp(&contact_priority(left))
            .then_with(|| left.name.cmp(&right.name))
    });
    contacts.truncate(2);
    run.reporter.account_contacts(&account.name, contacts.len());

    let plans = stream::iter(contacts.into_iter().map(|mut contact| {
        contact.vantage = normalize_vantage(&contact.vantage);
        let account = account.clone();
        async move {
            let mut sequence = write_sequence(run, &account, &contact, n_touches).await?;
            enforce_email_signatures(&mut sequence, &run.pb.signature);
            let reviews = outreach::review_and_edit_sequence(
                run.client,
                &run.pb.copy_system_prompt(run.shared),
                run.pb,
                run.shared,
                &account,
                &contact,
                &mut sequence,
                n_touches,
                run.critique,
            )
            .await?;
            run.reporter.sequence_done(&account.name);
            Ok::<ContactPlan, anyhow::Error>(ContactPlan {
                contact,
                sequence,
                reviews,
            })
        }
    }))
    .buffered(run.concurrency)
    .try_collect::<Vec<ContactPlan>>()
    .await?;

    Ok(AccountPlan {
        account,
        contacts: plans,
    })
}

async fn find_accounts(run: &Run<'_>, thesis: &str, n: usize) -> Result<Accounts> {
    let query = format!(
        "which accounts to target; ideal customer profile; qualifying an expensive workflow: \
         {thesis}; {motion}",
        motion = run.pb.motion,
    );
    let retrieved = run
        .library
        .retrieve_stage(&query, "companies", 3, 1)
        .playbook_block();
    let knowledge = format!("{}\n\n{}", core_strategy_block("companies"), retrieved);
    let user = prompts::accounts_user(run.pb, thesis, n, &knowledge);
    run.client
        .structured_bulk::<Accounts>(
            "campaign.accounts",
            &run.pb.qualification_system_prompt(),
            &user,
            prompts::accounts_schema(),
        )
        .await
}

async fn find_contacts(run: &Run<'_>, account: &Account, n: usize) -> Result<Contacts> {
    let query = format!(
        "who to reach inside the account; economic buyer, champion, decision maker, gatekeeper, \
         the person who owns the problem: {}",
        account.hypothesis,
    );
    let retrieved = run
        .library
        .retrieve_stage(&query, "people", 2, 0)
        .playbook_block();
    let knowledge = format!("{}\n\n{}", core_strategy_block("people"), retrieved);
    let user = prompts::contacts_user(run.pb, account, n, &knowledge);
    run.client
        .structured_bulk::<Contacts>(
            "campaign.contacts",
            &run.pb.vantage_system_prompt(),
            &user,
            prompts::contacts_schema(),
        )
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
    let retrieved = run
        .library
        .retrieve_stage(&query, "sequence", 4, 0)
        .playbook_block();
    let knowledge = format!("{}\n\n{}", core_strategy_block("sequence"), retrieved);
    let user = prompts::sequence_user(run.pb, account, contact, n, &knowledge);
    run.client
        .structured_bulk::<Sequence>(
            "campaign.sequence",
            &run.pb.copy_system_prompt(run.shared),
            &user,
            prompts::sequence_schema(),
        )
        .await
}

/// Apply the playbook signature after the critic so its rewrite cannot drift.
fn enforce_email_signatures(sequence: &mut Sequence, signature: &str) {
    for touch in &mut sequence.touches {
        if touch.channel.eq_ignore_ascii_case("email") {
            touch.body = playbook::enforce_signature(&touch.body, signature);
        }
    }
}

fn contact_priority(contact: &Contact) -> i32 {
    let mut score = if contact.primary { 100 } else { 0 };
    score += match normalize_vantage(&contact.vantage).as_str() {
        "process_owner" => 70,
        "operator" => 65,
        "operational_executive" => 55,
        "economic_buyer" => 40,
        "technical_evaluator" => 25,
        "router" => 10,
        _ => 0,
    };
    score
}

/// Normalize a model-supplied vantage to the canonical set for consistent badges.
fn normalize_vantage(raw: &str) -> String {
    let v = raw.trim().to_lowercase().replace([' ', '-'], "_");
    match v.as_str() {
        "process_owner"
        | "operator"
        | "operational_executive"
        | "technical_evaluator"
        | "economic_buyer"
        | "router" => v,
        _ => raw.trim().to_lowercase(),
    }
}
