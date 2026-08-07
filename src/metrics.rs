//! Funnel metrics: the numbers an SDR manager actually looks at.
//!
//! Reconstructed from the durable people table + the append-only event log, so
//! it's always consistent with what really happened. One [`Funnel`] per brand
//! (or across all), rendered as a compact text block for the CLI/REPL.

use anyhow::Result;
use serde::Serialize;

use crate::db::SharedDb;

#[derive(Debug, Default, Serialize)]
pub struct Funnel {
    pub brand: String,
    pub leads: usize,
    pub people: usize,
    pub verified: usize,
    pub contacted: usize,
    pub touches_sent: i64,
    pub replied: usize,
    pub unsubscribed: usize,
    pub bounced: usize,
    pub opportunities: usize,
    pub opportunities_shortlisted: usize,
    pub opportunities_active: usize,
    pub opportunity_contacts: usize,
    pub opportunity_contacts_verified: usize,
    pub funding_touches_sent: i64,
}

impl Funnel {
    pub fn reply_rate(&self) -> f64 {
        if self.contacted == 0 {
            0.0
        } else {
            self.replied as f64 / self.contacted as f64 * 100.0
        }
    }
}

pub fn funnel(db: &SharedDb, brand: Option<&str>) -> Result<Funnel> {
    let people = db.list_people(brand, None)?;
    let opportunities = db.list_opportunities(brand, None)?;
    let opportunity_contacts = match brand {
        Some(brand) => db.list_opportunity_contacts_for_brand(brand)?,
        None => opportunities
            .iter()
            .map(|opportunity| db.list_opportunity_contacts(&opportunity.id))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect(),
    };
    let counts = db.event_counts(brand)?;
    let sent = counts
        .iter()
        .find(|(k, _)| k == "sent")
        .map(|(_, v)| *v)
        .unwrap_or(0);
    let funding_sent = counts
        .iter()
        .find(|(kind, _)| kind == "funding_outreach_sent")
        .map(|(_, count)| *count)
        .unwrap_or(0);

    let count =
        |pred: &dyn Fn(&crate::db::Person) -> bool| people.iter().filter(|p| pred(p)).count();

    Ok(Funnel {
        brand: brand.unwrap_or("all").to_string(),
        leads: db.list_leads(brand)?.len(),
        people: people.len(),
        verified: count(&|p| p.email_status == "verified"),
        // Current status is not a history: a contacted person later becomes
        // replied/bounced/unsubscribed. The append-only send events preserve the
        // correct distinct denominator for the reply rate.
        contacted: db.distinct_event_people(brand, "sent")?,
        touches_sent: sent,
        replied: count(&|p| p.status == "replied"),
        unsubscribed: count(&|p| p.status == "unsubscribed"),
        bounced: count(&|p| p.status == "bounced"),
        opportunities: opportunities.len(),
        opportunities_shortlisted: opportunities
            .iter()
            .filter(|opportunity| opportunity.pipeline_status == "shortlisted")
            .count(),
        opportunities_active: opportunities
            .iter()
            .filter(|opportunity| {
                matches!(
                    opportunity.pipeline_status.as_str(),
                    "contacting" | "applying" | "submitted"
                )
            })
            .count(),
        opportunity_contacts: opportunity_contacts.len(),
        opportunity_contacts_verified: opportunity_contacts
            .iter()
            .filter(|contact| contact.email_status == "verified")
            .count(),
        funding_touches_sent: funding_sent,
    })
}

pub fn render(f: &Funnel) -> String {
    format!(
        "funnel [{brand}]\n\
         \u{2022} leads qualified   {leads}\n\
         \u{2022} people sourced    {people}\n\
         \u{2022} verified emails   {verified}\n\
         \u{2022} contacted         {contacted}\n\
         \u{2022} touches sent      {sent}\n\
         \u{2022} replied           {replied}  ({rate:.0}% of contacted)\n\
         \u{2022} unsubscribed      {unsub}\n\
         \u{2022} bounced           {bounced}\n\
         opportunity pipeline\n\
         \u{2022} opportunities     {opportunities}\n\
         \u{2022} shortlisted       {shortlisted}\n\
         \u{2022} active pursuits   {active}\n\
         \u{2022} funder contacts   {funding_contacts} ({funding_verified} verified)\n\
         \u{2022} funding emails    {funding_sent}",
        brand = f.brand,
        leads = f.leads,
        people = f.people,
        verified = f.verified,
        contacted = f.contacted,
        sent = f.touches_sent,
        replied = f.replied,
        rate = f.reply_rate(),
        unsub = f.unsubscribed,
        bounced = f.bounced,
        opportunities = f.opportunities,
        shortlisted = f.opportunities_shortlisted,
        active = f.opportunities_active,
        funding_contacts = f.opportunity_contacts,
        funding_verified = f.opportunity_contacts_verified,
        funding_sent = f.funding_touches_sent,
    )
}
