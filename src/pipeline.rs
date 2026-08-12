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
use crate::response_design;

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
    let n_touches = outreach::supported_touch_count_for_brand(&pb.key, n_touches);
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

    for left in 0..plans.len() {
        for right in (left + 1)..plans.len() {
            let Some(left_contact) = plans[left].contacts.first() else {
                continue;
            };
            let Some(right_contact) = plans[right].contacts.first() else {
                continue;
            };
            if let Some((body_similarity, question_similarity)) =
                outreach::cross_recipient_structural_similarity(
                    &left_contact.sequence,
                    &right_contact.sequence,
                    &pb.signature,
                )
            {
                return Err(anyhow::anyhow!(
                    "cross-recipient structural duplication between {} and {}: T1 body {:.0}% similar and main question {:.0}% similar",
                    left_contact.contact.name,
                    right_contact.contact.name,
                    body_similarity * 100.0,
                    question_similarity * 100.0,
                ));
            }
        }
    }

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
        response_design::contact_priority(&right.title, &right.vantage, right.primary)
            .cmp(&response_design::contact_priority(
                &left.title,
                &left.vantage,
                left.primary,
            ))
            .then_with(|| left.name.cmp(&right.name))
    });
    // Map broadly, then contact only the best-supported workflow owner first.
    // Reaching additional people at the account requires an explicit later run.
    contacts.truncate(1);
    run.reporter.account_contacts(&account.name, contacts.len());

    let plans = stream::iter(contacts.into_iter().map(|mut contact| {
        contact.vantage = response_design::effective_vantage(&contact.title, &contact.vantage);
        contact.primary = response_design::effective_primary(&contact.title, contact.primary);
        let account = account.clone();
        async move {
            let mut sequence = write_sequence(run, &account, &contact, n_touches).await?;
            enforce_email_signatures(&mut sequence, &run.pb.signature);
            let reviewer_knowledge =
                role_knowledge(run, &run.shared.personas.reviewer, &account.hypothesis);
            let reviews = outreach::review_and_edit_sequence_lean(
                run.client,
                run.pb,
                run.shared,
                &account,
                &contact,
                &mut sequence,
                n_touches,
                run.critique,
                &reviewer_knowledge,
                None,
                None,
                "",
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
    let knowledge = role_knowledge(run, &run.shared.personas.writer, &account.hypothesis);
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

fn role_knowledge(run: &Run<'_>, persona: &str, account_question: &str) -> String {
    let retrieved =
        run.library
            .retrieve_stage(&format!("{persona}\n{account_question}"), "sequence", 6, 2);
    format!(
        "{}\n\n{}",
        core_strategy_block("sequence"),
        retrieved.playbook_block()
    )
}

/// Apply the playbook signature after the critic so its rewrite cannot drift.
fn enforce_email_signatures(sequence: &mut Sequence, signature: &str) {
    for touch in &mut sequence.touches {
        if touch.channel.eq_ignore_ascii_case("email") {
            touch.body = playbook::enforce_signature(&touch.body, signature);
        }
    }
}
