//! Funnel metrics: the numbers an SDR manager actually looks at.
//!
//! Reconstructed from the durable people table + the append-only event log, so
//! it's always consistent with what really happened. One [`Funnel`] per brand
//! (or across all), rendered as a compact text block for the CLI/REPL.

use anyhow::Result;

use crate::db::SharedDb;

#[derive(Debug, Default)]
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
}

impl Funnel {
    pub fn reply_rate(&self) -> f64 {
        if self.contacted == 0 { 0.0 } else { self.replied as f64 / self.contacted as f64 * 100.0 }
    }
}

pub fn funnel(db: &SharedDb, brand: Option<&str>) -> Result<Funnel> {
    let people = db.list_people(brand, None)?;
    let counts = db.event_counts(brand)?;
    let sent = counts.iter().find(|(k, _)| k == "sent").map(|(_, v)| *v).unwrap_or(0);

    let count = |pred: &dyn Fn(&crate::db::Person) -> bool| people.iter().filter(|p| pred(p)).count();

    Ok(Funnel {
        brand: brand.unwrap_or("all").to_string(),
        leads: db.list_leads(brand)?.len(),
        people: people.len(),
        verified: count(&|p| p.email_status == "verified"),
        contacted: count(&|p| matches!(p.status.as_str(), "contacted" | "replied")),
        touches_sent: sent,
        replied: count(&|p| p.status == "replied"),
        unsubscribed: count(&|p| p.status == "unsubscribed"),
        bounced: count(&|p| p.status == "bounced"),
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
         \u{2022} bounced           {bounced}",
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
    )
}
