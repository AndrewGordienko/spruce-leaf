# OutageHub verified-location evidence

OutageHub action-ready evidence is not limited to EV charging. The matcher can
intersect any source-verified Canadian operating address with historical
utility outage polygons, including properties, laboratories, warehouses,
towers, stores, residences, plants, and charging sites.

## Automatic account-research intake

For OutageHub research, the website extractor now returns candidate Canadian
operating addresses alongside ordinary facts. A candidate enters the inventory
only when all of these hold:

- its HTTPS source is on the company's own domain;
- its source excerpt, street, city, province, and postal code all occur on that
  exact fetched page; and
- it is presented as an owned, operated, managed, or monitored company
  location—not a customer, delivery, claim, service-area, or hypothetical
  address.

The runtime geocodes the first usable address, caches it in
`.spruce/verified-locations.json`, rebuilds
`.spruce/outage-location-matches.json` when
`.spruce/outage-archive.json` exists, and immediately supplies a completed match
to the same qualification pass. This path is segment-agnostic: laboratories,
warehouses, towers, stores, residences, plants, properties, and charging sites
use the same evidence contract.

The default public geocoder is Nominatim. Spruce Leaf serializes requests,
limits uncached use to four requests per minute, stores the returned licence and
query URL, and permanently reuses cached coordinates. Review the
[Nominatim usage policy](https://operations.osmfoundation.org/policies/nominatim/);
for regular or larger workloads, configure a self-hosted or commercial
Nominatim-compatible endpoint with `SPRUCE_GEOCODER_URL`.

Set these paths when they differ from the defaults:

```sh
SPRUCE_OUTAGE_ARCHIVE=/data/outage-archive.json
SPRUCE_VERIFIED_LOCATIONS=/data/verified-locations.json
SPRUCE_OUTAGE_MATCH_REPORT=/data/outage-location-matches.json
```

Set `SPRUCE_OUTAGE_LOCATION_DISCOVERY=0` to disable automatic intake.

## Operator-supplied inventory

Provide a JSON array with one object per operated location:

```json
[
  {
    "location_id": "dynacare-toronto-lab-1",
    "company": "Dynacare",
    "domain": "dynacare.ca",
    "location_kind": "laboratory",
    "operating_relationship": "operated",
    "station_name": "Toronto laboratory",
    "street_address": "123 Example Street",
    "city": "Toronto",
    "state": "Ontario",
    "latitude": 43.6532,
    "longitude": -79.3832,
    "source_url": "https://dynacare.ca/locations"
  }
]
```

Run the matcher against a historical OutageHub archive:

```sh
cargo run --locked -- --brand outagehub outage-evidence \
  --archive .spruce/outage-archive.json \
  --locations .spruce/verified-locations.json \
  --output .spruce/outage-location-matches.json
```

Rows without a company, domain, location kind, owned/operated/managed/monitored
relationship, public source, name, street address, city, or province are
rejected. Coordinates outside Canadian bounds are rejected. A polygon match is
outside utility context only: it does not prove private site status, equipment
status, incident cause, or an internal workflow.
