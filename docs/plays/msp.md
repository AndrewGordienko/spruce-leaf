# OutageHub play: managed service providers

## Job and decision

Confirm whether a customer-site connectivity incident may be utility related before escalating, opening a carrier case, or dispatching service.

## Likely people

Service operations, managed-services leadership, support engineering, NOC leadership, and product owners for ticket automation.

## Strong signals

- support for multiple Canadian customer locations;
- public evidence of 24/7 monitoring, on-site service, ticket triage, or dispatch;
- a named integration surface or current service-operations initiative.

## Useful first contributions

- a sample enriched ticket using a public location;
- a historical comparison with utility, timestamp, and returned status;
- a minimal API or webhook integration sketch;
- a one-page triage decision map.

## Easy questions

- Does service operations check local utility status before an on-site escalation?
- Would that context belong in the ticket, alert, or technician workflow?

## Evaluation and disqualification

Test whether the MSP has usable location data, whether utility context changes a ticket decision, and whether false positives are manageable. Disqualify when the customer owns triage entirely, addresses are unavailable, or the signal would add noise without changing action.
