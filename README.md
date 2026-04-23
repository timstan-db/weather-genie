# NOAA Weather Explorer — Genie Space on Databricks

Natural-language exploration of the NOAA Global Historical Climatology Network
daily (GHCNd) dataset on Databricks. ~120,000 weather stations across 200+
countries, records back to the 1700s — all queryable in plain English through
an AI/BI Genie Space.

This repo is a **generated artifact** of a private monorepo. It contains two
self-contained [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/):

- `sources/weather_noaa/` — SDP pipeline that ingests GHCNd directly from the
  public `s3://noaa-ghcn-pds/` bucket into four Delta tables (`countries`,
  `states`, `stations`, `weather`), with column comments and UC tags.
- `demos/weather_noaa_genie/` — creates (or updates) a Genie Space over those
  tables with curated sample questions and modeling instructions.

## Prerequisites

- A Databricks workspace on AWS, Azure, or GCP.
- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) v0.220+ configured (`databricks auth login`).
- A Unity Catalog catalog you can write to.
- A running SQL warehouse (serverless or classic).

## Deploy

Deploy the source bundle first (it creates the schema and lands the data), then
the demo bundle (it creates the Genie Space).

```bash
# 1. Land the NOAA data
cd sources/weather_noaa
databricks bundle deploy \
  --var="catalog=YOUR_CATALOG"
databricks bundle run ingest

# 2. Create the Genie Space over the raw tables
cd ../../demos/weather_noaa_genie
databricks bundle deploy \
  --var="catalog=YOUR_CATALOG" \
  --var="default_warehouse_id=YOUR_WAREHOUSE_ID"
databricks bundle run setup
```

The source ingest takes ~10–20 minutes on first run (it backfills the GHCNd
daily file — ~500M rows). Subsequent runs are incremental via Auto Loader.

### Setting defaults once

If you don't want to pass `--var` every time, export them in your shell:

```bash
export BUNDLE_VAR_catalog=YOUR_CATALOG
export BUNDLE_VAR_default_warehouse_id=YOUR_WAREHOUSE_ID
```

## What you get

**Unity Catalog schema** `YOUR_CATALOG.raw_weather_noaa`:

| Table | Rows | Description |
|---|---|---|
| `countries` | ~220 | FIPS country code → country name |
| `states` | ~74 | US states + Canadian provinces |
| `stations` | ~120k | Station dimension with lat/lon/elevation/country |
| `weather` | ~3B | Daily observations (TMAX, TMIN, PRCP, SNOW, SNWD…) |

**A Genie Space** named *NOAA Weather Explorer* with sample questions like:

- *How many weather stations are there by country?*
- *How has the Colorado snowpack in January above 10,000 feet changed over the years?*
- *Where should I go on vacation in February if I'm looking for daily highs between 65–80°F?*

The Genie instructions handle common gotchas: temperature-trend questions
default to TMAX, data-coverage gaps are accounted for, US-states questions
filter out Canadian provinces, and travel-recommendation questions get real
station-based answers instead of out-of-scope refusals.

## Data source

[NOAA Global Historical Climatology Network daily (GHCNd)](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily),
served publicly via the [Registry of Open Data on AWS](https://registry.opendata.aws/noaa-ghcn/).
No NOAA credentials required.

## Notes

- **Serverless only.** Both bundles use Databricks serverless compute.
- **Don't edit this repo directly** — it's generated from a private monorepo.
- Genie Spaces aren't yet a native Databricks Asset Bundle resource (see
  [databricks/cli#4191](https://github.com/databricks/cli/pull/4191)), so the
  demo bundle manages the Space via the REST API from a notebook task.

## License

MIT — see [LICENSE](LICENSE).
