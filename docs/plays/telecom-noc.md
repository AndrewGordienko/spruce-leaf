# OutageHub play: telecom and NOC

## Job and decision

Distinguish a network fault from utility-power loss at a specific site before escalating, dispatching, or changing customer communications.

## Likely people

NOC leadership, network assurance, service assurance, field operations, and incident management. A title is not ownership evidence; map what the person can observe or route.

## Strong signals

- a public Canadian site or service footprint;
- explicit NOC, alarm-triage, incident, or field-dispatch responsibility;
- a current expansion, reliability program, public incident, or location-specific historical utility match.

## Present state to test

Utility status may be checked manually, through a vendor, after other diagnostics, or not at all. Never claim unnecessary dispatches or slow triage without customer evidence.

## Useful first contributions

- replay public utility events against several public sites;
- show the exact sample payload for one location and timestamp;
- sketch ticket enrichment using the recipient's publicly documented tooling;
- provide an outage-verification checklist.

## Easy questions

- When a site-power alarm lands, is utility status checked before or after remote diagnostics?
- Is that check owned by the NOC, service assurance, or field operations?

## Evaluation and disqualification

Evaluate location coverage, timeliness, false associations, ticket fit, and whether the result changes a decision. Disqualify when sites are not addressable, utility context arrives too late, the workflow is already solved, or the recipient is unrelated.
