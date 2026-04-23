# NOAA Weather Explorer — Genie Space on Databricks

Natural-language exploration of the NOAA Global Historical Climatology Network
daily (GHCNd) dataset on Databricks. ~120,000 weather stations across 200+
countries, records back to the 1700s — all queryable in plain English through
an AI/BI Genie Space.

This repo is a **generated artifact** of a private monorepo. It contains:

- `demos/weather_noaa_genie/src/deploy.py` — an **orchestrator notebook** that
  deploys everything end-to-end from a single Run All. Works on Databricks
  Free Edition.
- `sources/weather_noaa/` — an optional [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/)
  for the ingestion side (SDP pipeline + schema + UC tags), for folks who
  prefer Infrastructure-as-Code over notebook-driven deploys.
- `demos/weather_noaa_genie/` — a matching bundle for the Genie Space side.

## Quick start (recommended — works on Free Edition)

You need a Databricks workspace with a Unity Catalog catalog you can write to.
No SQL warehouse required up front — the notebook will pick one if you have it,
or provision a small serverless warehouse if you don't.

1. **Get the repo into your workspace.**
   In Databricks: **Workspace** → **Create** → **Git folder**, paste
   `https://github.com/timstan-db/weather-genie`, and import.
2. **Open `demos/weather_noaa_genie/src/deploy.py`** in the workspace.
3. **Set the widgets at the top of the notebook (defaults are fine for Free Edition):**
   - `catalog` — the UC catalog to write into (default: `workspace`).
   - `schema` — the raw schema (default: `raw_weather_noaa`).
   - `warehouse_id` — **leave blank** to auto-resolve, or paste a specific
     warehouse ID to pin one.
4. **Run All.**

On first run, the SDP pipeline backfills the full GHCNd daily dataset — expect
~10–20 minutes. Subsequent runs are incremental.

When it finishes, the notebook prints the Genie Space URL. Open it and try:

> How many weather stations are there by country?
>
> How has the Colorado snowpack in January above 10,000 feet changed over the years?
>
> Where should I go on vacation in February if I'm looking for daily highs between 65–80°F?

## What the notebook does

1. **Create the target schema** (`<catalog>.raw_weather_noaa`).
2. **Create or update the SDP pipeline** that reads directly from the public
   `s3://noaa-ghcn-pds/` bucket. Source: `sources/weather_noaa/src/raw_weather_data.py`.
3. **Run the pipeline and wait for completion.**
4. **Apply table and column comments** plus UC tags (`managed_by`, `area`,
   `dir_name`) to all four tables.
5. **Resolve a SQL warehouse** — reuses the best existing warehouse if any,
   otherwise provisions `weather-genie-warehouse` (2X-Small serverless PRO,
   10-min auto-stop — sized to fit Free Edition).
6. **Create or update the Genie Space** *NOAA Weather Explorer* over the
   raw tables, with sample questions and modeling instructions.

The Genie instructions handle common gotchas: temperature-trend questions
default to TMAX, data-coverage gaps are accounted for, US-states questions
filter out Canadian provinces, and travel-recommendation questions get real
station-based answers instead of out-of-scope refusals.

## Alternative — deploy via Databricks Asset Bundles

For paid workspaces where the Jobs API is available, you can deploy the two
bundles directly:

```bash
# 1. Land the NOAA data
cd sources/weather_noaa
databricks bundle deploy --var="catalog=YOUR_CATALOG"
databricks bundle run ingest

# 2. Create the Genie Space
cd ../../demos/weather_noaa_genie
databricks bundle deploy \
  --var="catalog=YOUR_CATALOG" \
  --var="default_warehouse_id=YOUR_WAREHOUSE_ID"
databricks bundle run setup
```

**Not supported on Databricks Free Edition** — Free Edition's Jobs API is
gated, which blocks bundle deploys that include a `jobs:` resource. Use the
orchestrator notebook above instead.

### Setting variable defaults

Instead of passing `--var` every time, export shell vars:

```bash
export BUNDLE_VAR_catalog=YOUR_CATALOG
export BUNDLE_VAR_default_warehouse_id=YOUR_WAREHOUSE_ID
```

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

- **Serverless only.** Both the pipeline and the Genie warehouse use
  Databricks serverless compute.
- **Don't edit this repo directly** — it's generated from a private monorepo.
- Genie Spaces aren't yet a native Databricks Asset Bundle resource (see
  [databricks/cli#4191](https://github.com/databricks/cli/pull/4191)), so
  creation happens via the REST API either from the orchestrator notebook or
  a notebook task inside the demo bundle.

## License

MIT — see [LICENSE](LICENSE).
