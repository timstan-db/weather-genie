# NOAA Weather Explorer — Genie Space on Databricks

Natural-language exploration of the NOAA Global Historical Climatology Network
daily (GHCNd) dataset on Databricks. ~120,000 weather stations across 200+
countries, records back to the 1700s — all queryable in plain English through
an AI/BI Genie Space.

The repo is a single
[Databricks Asset Bundle (DAB)](https://docs.databricks.com/aws/en/dev-tools/bundles/)
that ingests the raw NOAA data and creates the Genie Space over it.

## Prerequisites

- A Databricks workspace with Unity Catalog and serverless compute enabled.
- The [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install)
  v0.228+ installed and [configured](https://docs.databricks.com/aws/en/dev-tools/cli/authentication)
  for your workspace (e.g. `databricks configure`).
- A UC catalog you can write to (defaults to `main`).
- A serverless SQL warehouse in the workspace (any will do — the demo
  picks one automatically).

## Deploy

From the repo root:

```bash
databricks bundle deploy

# 1. Land the NOAA data (SDP pipeline, ~10–20 min on first backfill)
databricks bundle run ingest

# 2. Create the Genie Space
databricks bundle run setup
```

The demo writes to `main.raw_weather_noaa.*` and uses the first
serverless SQL warehouse it finds. To override either, see below.

When `setup` finishes, the job output prints the Genie Space URL. Open it and
try:

> How many weather stations are there by country?
>
> How has the Colorado snowpack in January above 10,000 feet changed over the years?
>
> Where should I go on vacation in February if I'm looking for daily highs between 65–80°F?

### Overriding catalog or warehouse

```bash
export BUNDLE_VAR_catalog=YOUR_CATALOG
export BUNDLE_VAR_default_warehouse_id=YOUR_WAREHOUSE_ID
```

Or pass `--var=catalog=...` / `--var=default_warehouse_id=...` on each
`databricks bundle` invocation.

## What the bundle does

1. Creates the target schema (`<catalog>.raw_weather_noaa`).
2. Creates an SDP pipeline that reads directly from the public
   `s3://noaa-ghcn-pds/` bucket (`src/raw_weather_data.py`).
3. Applies table and column comments plus UC tags to all four tables
   (`src/03_table_comments.py`).
4. Creates or updates the Genie Space *NOAA Weather Explorer* over the raw
   tables, with sample questions and modeling instructions
   (`src/01_create_genie_space.py`).

The Genie instructions handle common gotchas: temperature-trend questions
default to TMAX, data-coverage gaps are accounted for, US-states questions
filter out Canadian provinces, and travel-recommendation questions get real
station-based answers instead of out-of-scope refusals.

## What you get

**Unity Catalog schema** `<your_catalog>.raw_weather_noaa`:

| Table | Rows | Description |
|---|---|---|
| `countries` | ~220 | FIPS country code → country name |
| `states` | ~74 | US states + Canadian provinces |
| `stations` | ~120k | Station dimension with lat/lon/elevation/country |
| `weather` | ~3B | Daily observations (TMAX, TMIN, PRCP, SNOW, SNWD…) |

**A Genie Space** named *NOAA Weather Explorer* over those four tables.

## Data source

[NOAA Global Historical Climatology Network daily (GHCNd)](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily),
served publicly via the [Registry of Open Data on AWS](https://registry.opendata.aws/noaa-ghcn/).
No NOAA credentials required.

## Notes

- **Serverless only.** Both the pipeline and the Genie Space's warehouse use
  Databricks serverless compute.
- Genie Spaces aren't yet a native Databricks Asset Bundle resource (see
  [databricks/cli#4191](https://github.com/databricks/cli/pull/4191)), so
  creation happens via the REST API from a notebook task in the bundle.

## Contributing

This repo is published from an upstream source, so pull requests here will be
overwritten on the next release. Please **file an
[issue](https://github.com/timstan-db/weather-genie/issues)** for bugs,
questions, or improvement ideas — fixes will be made upstream and reflected
here on the next push.

## License

MIT — see [LICENSE](LICENSE).
