//! Reply triage: classify an inbound reply and take the right action.
//!
//! The moment a real human replies, the sequence must stop — nothing reads worse
//! than touch 4 landing after someone already answered. This module classifies
//! each reply (interested / objection / not-now / referral / unsubscribe /
//! auto-reply) and acts:
//!   * unsubscribe / opt-out  → suppress + stop the sequence (compliance)
//!   * auto-reply (OOO)       → note it, leave the sequence running
//!   * anything else human     → mark replied, stop the sequence, flag for you
//!
//! Opt-out detection never depends on the model: [`Compliance::is_optout`] runs
//! first and forces the unsubscribe path.

use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::compliance::Compliance;
use crate::db::{OpportunityContact, OpportunityReply, SharedDb};
use crate::engine::Engine;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Classification {
    /// interested | not_now | objection | referral | unsubscribe | auto_reply | other
    pub category: String,
    #[serde(default)]
    pub summary: String,
}

pub async fn classify(
    client: &Engine,
    person_name: &str,
    subject: &str,
    body: &str,
) -> Result<Classification> {
    let system = "You triage inbound replies to professional B2B sales or funding-programme \
                  outreach for the sender. Read the reply and classify the sender's intent. Be \
                  conservative: an out-of-office or automated bounce is auto_reply, not interested.";
    let user = format!(
        "Reply from {person_name}.\nSubject: {subject}\n\n{}\n\nClassify it and summarize in one line.",
        body.chars().take(4000).collect::<String>()
    );
    client
        .structured_fast::<Classification>("reply.triage", system, &user, schema())
        .await
}

/// Funding/opportunity replies use the same conservative intent classifier but
/// stop only the relevant opportunity contact's sequence and advance the
/// opportunity workflow instead of the sales funnel.
#[allow(clippy::too_many_arguments)]
pub async fn handle_opportunity_reply(
    db: &SharedDb,
    client: &Engine,
    contact: &OpportunityContact,
    from_email: &str,
    subject: &str,
    body: &str,
    message_id: &str,
    in_reply_to: &str,
) -> Result<String> {
    if db.opportunity_reply_exists(message_id)? {
        return Ok("duplicate".into());
    }
    let class = classify(client, &contact.name, subject, body)
        .await
        .unwrap_or_default();
    let optout = Compliance::is_optout(body) || Compliance::is_optout(subject);
    let category = if optout {
        "unsubscribe".to_string()
    } else if class.category.is_empty() {
        "other".into()
    } else {
        class.category.clone()
    };

    let action = match category.as_str() {
        "unsubscribe" => {
            db.add_suppression(&contact.brand, from_email, "unsubscribed")?;
            db.set_opportunity_contact_status(&contact.id, "unsubscribed")?;
            db.stop_opportunity_outreach(&contact.id, "cancelled")?;
            "suppressed + opportunity outreach stopped".to_string()
        }
        "auto_reply" => "noted (auto-reply, opportunity outreach continues)".to_string(),
        other => {
            db.set_opportunity_contact_status(&contact.id, "replied")?;
            db.stop_opportunity_outreach(&contact.id, "cancelled")?;
            if matches!(other, "interested" | "referral") {
                db.set_opportunity_pipeline_status(&contact.opportunity_id, "applying")?;
            }
            format!("marked opportunity contact replied ({other})")
        }
    };
    db.log_event(
        &contact.brand,
        &format!("opportunity-contact:{}", contact.id),
        "",
        "funding_reply",
        &format!("{}: {}", category, class.summary),
    )?;
    db.record_opportunity_reply(&OpportunityReply {
        opportunity_id: contact.opportunity_id.clone(),
        contact_id: contact.id.clone(),
        from_email: from_email.to_string(),
        subject: subject.to_string(),
        body: body.chars().take(4000).collect(),
        classification: category,
        action_taken: action.clone(),
        message_id: message_id.to_string(),
        in_reply_to: in_reply_to.to_string(),
        ..Default::default()
    })?;
    Ok(action)
}

fn schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["category"],
        "properties": {
            "category": { "type": "string", "enum": ["interested","not_now","objection","referral","unsubscribe","auto_reply","other"] },
            "summary": { "type": "string" }
        }
    })
}
