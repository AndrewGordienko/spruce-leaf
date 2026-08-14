# OutageHub supervised-pilot gate

OutageHub live SMTP is pilot-locked. A live send requires a non-empty
`SPRUCE_SEND_ALLOWLIST`; an unset allowlist cannot be used as a production
default for this brand.

The release check is read-only:

```sh
spruce-leaf --brand outagehub pilot-audit
```

It passes only when the execution database proves all of the following:

- at least 20 real, source-backed accounts assigned across at least five of the
  twelve persisted OutageHub market segments;
- at least ten current model-generated messages whose opportunity/evidence
  lineage and deterministic copy checks still pass;
- zero recipients outside the evidenced segment's operating-role map;
- zero unsupported or stale claims, including exact T2
  address/utility/timestamp binding;
- at least ten explicit operator approvals recorded through
  `spruce-leaf --brand outagehub approve`;
- at least one of those messages delivered by SMTP to a controlled address
  currently covered by `SPRUCE_SEND_ALLOWLIST`.

The audit never sources, generates, approves, schedules, or sends. A failed
gate prints the missing evidence. Fix the input or infrastructure and rerun it;
do not replace the result with fixture counts.
