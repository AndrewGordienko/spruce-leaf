# OutageHub verified-location evidence

OutageHub action-ready evidence is not limited to EV charging. The matcher can
intersect any source-verified Canadian operating address with historical
utility outage polygons, including properties, laboratories, warehouses,
towers, stores, residences, plants, and charging sites.

Provide a JSON array with one object per operated location:

```json
[
  {
    "location_id": "dynacare-toronto-lab-1",
    "company": "Dynacare",
    "domain": "dynacare.ca",
    "location_kind": "laboratory",
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

Rows without a company, domain, public source, name, or street address are
rejected. Coordinates outside Canadian bounds are rejected. A polygon match is
outside utility context only: it does not prove private site status, equipment
status, incident cause, or an internal workflow.
