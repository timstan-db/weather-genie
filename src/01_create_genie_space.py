# Databricks notebook source
# MAGIC %md
# MAGIC # Create / Update Genie Space -- NOAA Weather Explorer
# MAGIC
# MAGIC Creates (or updates) an AI/BI Genie space over the
# MAGIC `raw_weather_noaa` schema. Includes all 4 raw tables
# MAGIC with sample questions and general instructions.
# MAGIC
# MAGIC Genie spaces are not yet supported as a native DAB resource
# MAGIC (pending [databricks/cli#4191](https://github.com/databricks/cli/pull/4191)),
# MAGIC so this notebook manages the space via the REST API.

# COMMAND ----------

dbutils.widgets.text("catalog", "timstanton_stable")
dbutils.widgets.text("schema", "raw_weather_noaa")
dbutils.widgets.text("warehouse_id", "8c35ef80cbacd670")
dbutils.widgets.text("workspace_folder", "")

import json
import re

import requests

_SAFE_ID = re.compile(r"^[a-zA-Z0-9_-]+$")


def _check_id(value: str, label: str) -> str:
    if not _SAFE_ID.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


catalog = _check_id(dbutils.widgets.get("catalog").strip(), "catalog")
schema = _check_id(dbutils.widgets.get("schema").strip(), "schema")
warehouse_id_input = dbutils.widgets.get("warehouse_id").strip()

print(f"Catalog:      {catalog}")
print(f"Schema:       {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

DISPLAY_NAME = "NOAA Weather Explorer"

DESCRIPTION = (
    "Explore NOAA Global Historical Climatology Network daily weather data "
    "using natural language. Covers ~120,000 weather stations across 200+ "
    "countries with records dating back to the 1700s."
)

TABLES = [
    "countries",
    "states",
    "stations",
    "weather",
]

SAMPLE_QUESTIONS = [
    "How many weather stations are there by country?",
    "How has the Colorado snowpack in January above 10000 feet elevation changed over the years?",
    "Where should I go on vacation in February if I am looking for daily highs between 65-80 degrees Fahrenheit?",
]

INSTRUCTIONS = """\
- When asked about temperature trends generally (without the user specifying \
highs, lows, averages, etc.), assume the user wants to explore the element TMAX.
- The station data is not always complete in terms of elements missing and \
gaps in dates. Pay extra attention to data coverage and remove nulls or \
downselect to stations that have the most complete data for the particular \
slices (time, geographies, elements) in question.
- Remove Canadian provinces and other U.S. territories (see state field) when \
the user is asking about U.S. states (there should only be 50 states).
- If the user is asking for recommendations on vacations or travel advice, \
don't tell them it's out of scope; instead, provide location recommendations \
(based on weather station locations) that satisfy the user's weather-related \
questions.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update the Genie space

# COMMAND ----------

table_identifiers = [f"{catalog}.{schema}.{t}" for t in TABLES]

import hashlib


def _stable_hex_id(value: str) -> str:
    """Generate a lowercase 32-char hex ID from a string."""
    return hashlib.md5(value.encode("utf-8")).hexdigest()


serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": [{"id": _stable_hex_id(q), "question": [q]} for q in SAMPLE_QUESTIONS],
    },
    "data_sources": {
        "tables": [{"identifier": tid} for tid in table_identifiers],
    },
    "instructions": {
        "text_instructions": [
            {
                "id": _stable_hex_id("general"),
                # Genie splits content on newlines and concatenates list elements
                # without separators, so pass the whole block as a single string.
                "content": [INSTRUCTIONS],
            }
        ],
    },
}

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# Resolve warehouse: explicit value wins; otherwise pick the first serverless
# SQL warehouse the workspace exposes (preferring one that's already RUNNING).
if warehouse_id_input:
    warehouse_id = _check_id(warehouse_id_input, "warehouse_id")
else:
    resp = requests.get(f"{host}/api/2.0/sql/warehouses", headers=headers, timeout=30)
    resp.raise_for_status()
    warehouses = resp.json().get("warehouses", [])
    serverless = [w for w in warehouses if w.get("enable_serverless_compute")]
    if not serverless:
        raise RuntimeError(
            "No serverless SQL warehouse found in this workspace. "
            "Pass --var=default_warehouse_id=<id> with a valid warehouse."
        )
    running = [w for w in serverless if w.get("state") == "RUNNING"]
    chosen = (running or serverless)[0]
    warehouse_id = _check_id(chosen["id"], "warehouse_id")
    print(f"Auto-detected serverless warehouse: {chosen.get('name')} ({warehouse_id})")
print(f"Warehouse ID: {warehouse_id}")

# Search for existing space by name
existing_space_id = None
try:
    resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers, timeout=30)
    print(f"List spaces: status={resp.status_code}, count={len(resp.json().get('spaces', []))}")
    if resp.ok:
        for s in resp.json().get("spaces", []):
            if s.get("title") == DISPLAY_NAME:
                existing_space_id = s.get("space_id") or s.get("id")
                print(f"Found existing Genie space: {existing_space_id}")
                break
    if not existing_space_id:
        print(f"No space found with title '{DISPLAY_NAME}'")
        titles = [s.get("title") for s in resp.json().get("spaces", [])]
        print(f"Available titles: {titles}")
except Exception as e:
    print(f"Warning: could not list spaces: {e}")

payload = {
    "title": DISPLAY_NAME,
    "description": DESCRIPTION,
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
    if not resp.ok:
        print(f"Update failed ({resp.status_code}): {resp.text[:500]}")
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
        print(f"Create failed ({resp.status_code}): {resp.text[:500]}")
    resp.raise_for_status()
    result = resp.json()
    space_id = result.get("space_id") or result["id"]
    action = "Created"

    # Move Genie space into the bundle's workspace folder
    workspace_folder = dbutils.widgets.get("workspace_folder").strip()
    if workspace_folder:
        user_name = ctx.userName().get()
        source_path = f"/Users/{user_name}/{DISPLAY_NAME}"
        dest_path = f"{workspace_folder}/{DISPLAY_NAME}"
        requests.post(
            f"{host}/api/2.0/workspace/mkdirs",
            headers=headers,
            json={"path": workspace_folder},
            timeout=30,
        )
        move_resp = requests.post(
            f"{host}/api/2.0/workspace/move",
            headers=headers,
            json={"source_path": source_path, "destination_path": dest_path},
            timeout=30,
        )
        if move_resp.ok:
            print(f"Moved Genie space to {dest_path}")
        else:
            print(f"Note: could not move space: {move_resp.status_code}")

genie_url = f"{host}/genie/rooms/{space_id}"
print(f"{action} Genie space: {space_id}")
print(f"URL: {genie_url}")
print(f"Tables: {len(TABLES)}")
print(f"Sample questions: {len(SAMPLE_QUESTIONS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print(f"Genie space '{DISPLAY_NAME}' is ready.")
print(f"  Space ID:   {space_id}")
print(f"  Warehouse:  {warehouse_id}")
print(f"  Tables:     {len(TABLES)}")
print()
print("Tables included:")
for t in TABLES:
    print(f"  - {catalog}.{schema}.{t}")
print()
print("Sample questions:")
for q in SAMPLE_QUESTIONS:
    print(f"  - {q}")
