# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy — NOAA Weather Explorer
# MAGIC
# MAGIC One-notebook deploy of the NOAA Weather Explorer demo. Runs everything
# MAGIC inline, no Jobs API required (works on Databricks Free Edition).
# MAGIC
# MAGIC **What this does:**
# MAGIC 1. Create the target schema
# MAGIC 2. Create or update the SDP pipeline (reads from `s3://noaa-ghcn-pds`)
# MAGIC 3. Run the pipeline and wait for completion (~10–20 min on first run)
# MAGIC 4. Apply table and column comments plus UC tags
# MAGIC 5. Resolve or provision a SQL warehouse for the Genie Space
# MAGIC 6. Create or update the Genie Space
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - The full `weather-genie` repo imported into your workspace (the
# MAGIC   pipeline's source file must be at
# MAGIC   `<repo_root>/sources/weather_noaa/src/raw_weather_data.py`).
# MAGIC - Optionally: a specific SQL warehouse ID via the `warehouse_id`
# MAGIC   widget. If left blank, the notebook auto-resolves one — picks the
# MAGIC   best running warehouse, or provisions a small serverless warehouse
# MAGIC   sized for Free Edition.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "raw_weather_noaa")
dbutils.widgets.text("warehouse_id", "")

import re

_SAFE_ID = re.compile(r"^[a-zA-Z0-9_-]+$")


def _check_id(value: str, label: str) -> str:
    if not _SAFE_ID.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


catalog = _check_id(dbutils.widgets.get("catalog").strip(), "catalog")
schema = _check_id(dbutils.widgets.get("schema").strip(), "schema")
warehouse_override = dbutils.widgets.get("warehouse_id").strip()

PIPELINE_NAME = "weather_noaa_raw"
GENIE_DISPLAY_NAME = "NOAA Weather Explorer"
GENIE_WAREHOUSE_NAME = "weather-genie-warehouse"
AREA = "sources"
DIR_NAME = "weather_noaa"
TABLES = ["countries", "states", "stations", "weather"]

print(f"Catalog:      {catalog}")
print(f"Schema:       {schema}")
print(f"Warehouse ID: {warehouse_override or '(auto-resolve)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the target schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
print(f"Schema ready: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create or update the SDP pipeline

# COMMAND ----------

import json
import pathlib
import time

import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
notebook_path = ctx.notebookPath().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# This notebook lives at <repo>/demos/weather_noaa_genie/src/deploy.py.
# The SDP source file lives at <repo>/sources/weather_noaa/src/raw_weather_data.py.
_notebook_parent = pathlib.PurePosixPath(notebook_path).parent
_repo_root = _notebook_parent.parent.parent.parent
sdp_source_path = str(_repo_root / "sources" / "weather_noaa" / "src" / "raw_weather_data.py")

print(f"Notebook path:   {notebook_path}")
print(f"Computed repo:   {_repo_root}")
print(f"SDP source file: {sdp_source_path}")

status_resp = requests.get(
    f"{host}/api/2.0/workspace/get-status",
    headers=headers,
    params={"path": sdp_source_path},
    timeout=30,
)
if not status_resp.ok:
    raise RuntimeError(
        f"SDP source file not found at {sdp_source_path}.\n"
        f"Import the full weather-genie repo into your workspace "
        f"(Workspace > Create > Git folder), then re-run this notebook."
    )

# COMMAND ----------

# Find existing pipeline by name
pipeline_id = None
list_resp = requests.get(
    f"{host}/api/2.0/pipelines",
    headers=headers,
    params={"max_results": 100},
    timeout=30,
)
list_resp.raise_for_status()
for p in list_resp.json().get("statuses", []) or []:
    if p.get("name") == PIPELINE_NAME:
        pipeline_id = p.get("pipeline_id")
        break

pipeline_spec = {
    "name": PIPELINE_NAME,
    "catalog": catalog,
    "target": schema,
    "serverless": True,
    "channel": "CURRENT",
    "libraries": [{"file": {"path": sdp_source_path}}],
}

if pipeline_id:
    edit_payload = {**pipeline_spec, "id": pipeline_id}
    resp = requests.put(
        f"{host}/api/2.0/pipelines/{pipeline_id}",
        headers=headers,
        json=edit_payload,
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"Pipeline update failed ({resp.status_code}): {resp.text[:500]}")
    print(f"Updated existing pipeline: {pipeline_id}")
else:
    resp = requests.post(
        f"{host}/api/2.0/pipelines",
        headers=headers,
        json=pipeline_spec,
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"Pipeline create failed ({resp.status_code}): {resp.text[:500]}")
    pipeline_id = resp.json()["pipeline_id"]
    print(f"Created new pipeline: {pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run the pipeline and wait for completion

# COMMAND ----------

start_resp = requests.post(
    f"{host}/api/2.0/pipelines/{pipeline_id}/updates",
    headers=headers,
    json={},
    timeout=30,
)
start_resp.raise_for_status()
update_id = start_resp.json()["update_id"]
print(f"Started pipeline update: {update_id}")
print("Polling for completion (timeout 30 min)...")

deadline = time.time() + 30 * 60
last_state = None
while time.time() < deadline:
    poll = requests.get(
        f"{host}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}",
        headers=headers,
        timeout=30,
    )
    poll.raise_for_status()
    update = poll.json().get("update", {})
    state = update.get("state")
    if state != last_state:
        print(f"  State: {state}")
        last_state = state
    if state == "COMPLETED":
        break
    if state in ("FAILED", "CANCELED"):
        raise RuntimeError(f"Pipeline update ended with state {state}")
    time.sleep(20)
else:
    raise TimeoutError(f"Pipeline update {update_id} did not complete within 30 minutes")

print(f"Pipeline update COMPLETED: {update_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Apply table comments and UC tags

# COMMAND ----------

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

TABLE_COMMENTS: dict[str, tuple[str, dict[str, str]]] = {
    "countries": (
        (
            "NOAA GHCNd country dimension. "
            "Maps two-character FIPS country codes to full country names. "
            "Source: noaa-ghcn-pds/ghcnd-countries.txt."
        ),
        {
            "country_code": "FIPS country code (two characters). Primary key.",
            "country": "Full name of the country.",
        },
    ),
    "states": (
        (
            "NOAA GHCNd state/province dimension. "
            "Maps two-character postal codes to full state or province names. "
            "Only applies to U.S. states/territories and Canadian provinces. "
            "Source: noaa-ghcn-pds/ghcnd-states.txt."
        ),
        {
            "state_code": "U.S. postal code or Canadian province code (two characters). Primary key.",
            "state": "Full name of the state or province.",
        },
    ),
    "stations": (
        (
            "NOAA GHCNd weather station dimension. "
            "Contains location, elevation, and network flags for every station in the dataset. "
            "Source: noaa-ghcn-pds/ghcnd-stations.txt."
        ),
        {
            "station_id": "11-character station identification code. First two characters are the FIPS country code; third character is the network code. Primary key.",
            "country_code": "FIPS country code. FK to countries.country_code.",
            "latitude": "Latitude of the station. Unit: decimal degrees.",
            "longitude": "Longitude of the station. Unit: decimal degrees.",
            "elevation": "Elevation of the station. Unit: meters.",
            "state_code": "U.S. postal code for the state (U.S. and Canadian stations only). FK to states.state_code.",
            "station_name": "Name of the station.",
            "gsn_flag": "GCOS Surface Network flag. Blank = non-GSN or WMO number unavailable, GSN = GSN station.",
            "hcn_crn_flag": "Historical Climatology / Climate Reference Network flag. Blank = neither, HCN = U.S. HCN station, CRN = U.S. CRN or Regional CRN station.",
            "wmo_id": "World Meteorological Organization number for the station. NULL if unavailable.",
        },
    ),
    "weather": (
        (
            "NOAA GHCNd daily weather observations. "
            "One record per station, date, and element. "
            "Incrementally ingested via Auto Loader from s3a://noaa-ghcn-pds/csv/by_year/. "
            "Source: NOAA Global Historical Climatology Network daily (GHCNd)."
        ),
        {
            "station_id": "11-character station identification code. FK to stations.station_id.",
            "date": "Date of the weather observation.",
            "data_value": "Observed value for the element. Units vary: PRCP in tenths of mm, TMAX/TMIN in tenths of degrees C, SNOW/SNWD in mm.",
            "m_flag": "Measurement flag. Blank = no info, B = two 12-hour totals, D = four 6-hour totals, H = hourly extreme/average, K = from knots, L = lagged, O = from oktas, P = presumed zero, T = trace, W = from WBAN code.",
            "q_flag": "Quality flag. Blank = passed all checks. Letters indicate specific failed checks (D=duplicate, G=gap, I=consistency, etc.).",
            "s_flag": "Source flag indicating original data provider (0=US COOP, A/B=ASOS, C=Canada, E=ECA&D, G=GCOS, etc.).",
            "obs_time": "Time of observation in HHMM format (e.g. 0700 = 7:00 am).",
            "element": "Element type: PRCP=Precipitation, SNOW=Snowfall, SNWD=Snow depth, TMAX=Max temperature, TMIN=Min temperature.",
            "source_file_path": "Auto Loader metadata: path of the source file ingested.",
            "source_file_modification_time": "Auto Loader metadata: last modification time of the source file.",
            "ingested_at": "UTC timestamp when the record was ingested by the pipeline.",
        },
    ),
}


def apply_table_comments(
    table_name: str, table_comment: str, column_comments: dict[str, str]
) -> dict[str, str]:
    results: dict[str, str] = {}
    escaped = table_comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE `{table_name}` IS '{escaped}'")
    results["__table__"] = "ok"
    existing = {c.name for c in spark.table(table_name).schema.fields}
    for col_name, col_comment in column_comments.items():
        if col_name not in existing:
            results[col_name] = "skipped (column not found)"
            continue
        try:
            ec = col_comment.replace("'", "''")
            spark.sql(f"COMMENT ON COLUMN `{table_name}`.`{col_name}` IS '{ec}'")
            results[col_name] = "ok"
        except Exception as e:
            results[col_name] = str(e)
    return results


def apply_uc_tags(table_name: str) -> str:
    try:
        spark.sql(
            f"ALTER TABLE `{table_name}` SET TAGS ("
            f"'managed_by' = 'databricks-demos', "
            f"'area' = '{AREA}', "
            f"'dir_name' = '{DIR_NAME}')"
        )
        return "ok"
    except Exception as e:
        return str(e)


for table_name, (table_comment, column_comments) in TABLE_COMMENTS.items():
    print(f"Applying comments to {table_name}...")
    result = apply_table_comments(table_name, table_comment, column_comments)
    ok = sum(1 for v in result.values() if v == "ok")
    skip = sum(1 for v in result.values() if "skipped" in v)
    fail = len(result) - ok - skip
    print(f"  Applied: {ok}  Skipped: {skip}  Failed: {fail}")

print()
print(f"Applying UC tags (managed_by, area={AREA}, dir_name={DIR_NAME})")
tag_ok = 0
for table_name in TABLE_COMMENTS:
    r = apply_uc_tags(table_name)
    if r == "ok":
        tag_ok += 1
    else:
        print(f"  FAILED: {table_name}: {r}")
print(f"Tags applied: {tag_ok}/{len(TABLE_COMMENTS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resolve a SQL warehouse for the Genie Space
# MAGIC
# MAGIC If `warehouse_id` was set, use it. Otherwise pick the best existing
# MAGIC warehouse (running > starting > stopped, smaller size preferred), or
# MAGIC provision a 2X-Small serverless PRO warehouse (Free Edition-sized).

# COMMAND ----------


def _resolve_warehouse() -> str:
    if warehouse_override:
        _check_id(warehouse_override, "warehouse_id")
        print(f"Using warehouse override: {warehouse_override}")
        return warehouse_override

    resp = requests.get(f"{host}/api/2.0/sql/warehouses", headers=headers, timeout=30)
    resp.raise_for_status()
    warehouses = resp.json().get("warehouses", []) or []

    _state_rank = {"RUNNING": 0, "STARTING": 1}
    _size_rank = {"2X-Small": 0, "X-Small": 1, "Small": 2, "Medium": 3, "Large": 4}

    def _rank(wh: dict) -> tuple[int, int]:
        return (
            _state_rank.get(wh.get("state"), 9),
            _size_rank.get(wh.get("cluster_size"), 9),
        )

    if warehouses:
        best = sorted(warehouses, key=_rank)[0]
        wh_id = best["id"]
        print(
            f"Using existing warehouse: {best.get('name')} "
            f"(id={wh_id}, size={best.get('cluster_size')}, state={best.get('state')})"
        )
        return wh_id

    # None available — provision one sized for Free Edition
    payload = {
        "name": GENIE_WAREHOUSE_NAME,
        "cluster_size": "2X-Small",
        "warehouse_type": "PRO",
        "enable_serverless_compute": True,
        "auto_stop_mins": 10,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
    }
    create_resp = requests.post(
        f"{host}/api/2.0/sql/warehouses", headers=headers, json=payload, timeout=60
    )
    if not create_resp.ok:
        raise RuntimeError(
            f"Warehouse create failed ({create_resp.status_code}): {create_resp.text[:500]}"
        )
    wh_id = create_resp.json()["id"]
    print(f"Provisioned new warehouse: {GENIE_WAREHOUSE_NAME} (id={wh_id}, 2X-Small serverless)")
    return wh_id


warehouse_id = _resolve_warehouse()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create or update the Genie Space

# COMMAND ----------

import hashlib

GENIE_DESCRIPTION = (
    "Explore NOAA Global Historical Climatology Network daily weather data "
    "using natural language. Covers ~120,000 weather stations across 200+ "
    "countries with records dating back to the 1700s."
)

SAMPLE_QUESTIONS = [
    "How many weather stations are there by country?",
    "How has the Colorado snowpack in January above 10000 feet elevation changed over the years?",
    "Where should I go on vacation in February if I am looking for daily highs between 65-80 degrees Fahrenheit?",
]


def _stable_hex_id(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


table_identifiers = [f"{catalog}.{schema}.{t}" for t in TABLES]
serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": [{"id": _stable_hex_id(q), "question": [q]} for q in SAMPLE_QUESTIONS],
    },
    "data_sources": {"tables": [{"identifier": tid} for tid in table_identifiers]},
}

# Find existing space by name
existing_space_id = None
try:
    resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers, timeout=30)
    if resp.ok:
        for s in resp.json().get("spaces", []):
            if s.get("title") == GENIE_DISPLAY_NAME:
                existing_space_id = s.get("space_id") or s.get("id")
                break
except Exception as e:
    print(f"Warning: could not list Genie spaces: {e}")

payload = {
    "title": GENIE_DISPLAY_NAME,
    "description": GENIE_DESCRIPTION,
    "warehouse_id": warehouse_id,
    "serialized_space": json.dumps(serialized_space),
}

if existing_space_id:
    resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{existing_space_id}",
        headers=headers,
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    space_id = existing_space_id
    action = "Updated"
else:
    payload["table_identifiers"] = table_identifiers
    resp = requests.post(
        f"{host}/api/2.0/genie/spaces",
        headers=headers,
        json=payload,
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"Genie create failed ({resp.status_code}): {resp.text[:500]}")
    space_id = resp.json().get("space_id") or resp.json()["id"]
    action = "Created"

genie_url = f"{host}/genie/rooms/{space_id}"
print(f"{action} Genie space: {space_id}")
print(f"URL: {genie_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done

# COMMAND ----------

print("NOAA Weather Explorer is ready.")
print()
print(f"Pipeline:     {pipeline_id}")
print(f"Schema:       {catalog}.{schema}")
print(f"Tables:       {', '.join(TABLES)}")
print(f"Genie Space:  {genie_url}")
print()
print("Try asking:")
for q in SAMPLE_QUESTIONS:
    print(f"  • {q}")
