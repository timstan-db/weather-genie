# Databricks notebook source
# MAGIC %md
# MAGIC # Table & Column Comments
# MAGIC
# MAGIC Adds descriptive comments and UC tags to the raw_weather_noaa tables.
# MAGIC Column descriptions sourced from the NOAA GHCNd documentation:
# MAGIC https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ghcn

# COMMAND ----------

dbutils.widgets.text("catalog", "timstanton_stable")
dbutils.widgets.text("schema", "raw_weather_noaa")
dbutils.widgets.text("area", "sources")
dbutils.widgets.text("dir_name", "weather_noaa")

import re

_SAFE_ID = re.compile(r"^[a-zA-Z0-9_-]+$")


def _check_id(value: str, label: str) -> str:
    if not _SAFE_ID.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


catalog = _check_id(dbutils.widgets.get("catalog").strip(), "catalog")
schema = _check_id(dbutils.widgets.get("schema").strip(), "schema")
area = _check_id(dbutils.widgets.get("area").strip(), "area")
dir_name = _check_id(dbutils.widgets.get("dir_name").strip(), "dir_name")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

print(f"Catalog:  {catalog}")
print(f"Schema:   {schema}")
print(f"Area:     {area}")
print(f"Dir name: {dir_name}")

# COMMAND ----------

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

# COMMAND ----------


def apply_table_comments(
    table_name: str,
    table_comment: str,
    column_comments: dict[str, str],
) -> dict[str, str]:
    """Apply COMMENT ON TABLE and COMMENT ON COLUMN statements."""
    results: dict[str, str] = {}

    escaped_comment = table_comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE `{table_name}` IS '{escaped_comment}'")
    results["__table__"] = "ok"

    existing_columns = {c.name for c in spark.table(table_name).schema.fields}
    for col_name, col_comment in column_comments.items():
        if col_name not in existing_columns:
            results[col_name] = "skipped (column not found)"
            continue
        try:
            escaped = col_comment.replace("'", "''")
            spark.sql(f"COMMENT ON COLUMN `{table_name}`.`{col_name}` IS '{escaped}'")
            results[col_name] = "ok"
        except Exception as e:
            results[col_name] = str(e)

    return results


def apply_uc_tags(table_name: str, area: str, dir_name: str) -> str:
    """Apply standard UC tags (managed_by, area, dir_name) to a table."""
    try:
        spark.sql(
            f"ALTER TABLE `{table_name}` SET TAGS ("
            f"'managed_by' = 'databricks-demos', "
            f"'area' = '{area}', "
            f"'dir_name' = '{dir_name}')"
        )
        return "ok"
    except Exception as e:
        return str(e)


# COMMAND ----------

for table_name, (table_comment, column_comments) in TABLE_COMMENTS.items():
    print(f"Applying comments to {table_name}...")
    result = apply_table_comments(table_name, table_comment, column_comments)
    ok = sum(1 for v in result.values() if v == "ok")
    skip = sum(1 for v in result.values() if "skipped" in v)
    fail = len(result) - ok - skip
    print(f"  Applied: {ok}  Skipped: {skip}  Failed: {fail}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply UC tags

# COMMAND ----------

print(f"Applying UC tags: managed_by=databricks-demos, area={area}, dir_name={dir_name}")

tag_ok = 0
tag_fail = 0
for table_name in TABLE_COMMENTS:
    result = apply_uc_tags(table_name, area, dir_name)
    if result == "ok":
        tag_ok += 1
    else:
        tag_fail += 1
        print(f"  FAILED: {table_name}: {result}")

print(f"\nTags: {tag_ok} applied, {tag_fail} failed")

# COMMAND ----------

# Verify
for table_name in TABLE_COMMENTS:
    print(f"{'=' * 60}")
    print(f"Table: {table_name}")
    print(f"{'=' * 60}")
    cols_df = spark.sql(f"DESCRIBE TABLE `{table_name}`").collect()
    for row in cols_df:
        col_name = row["col_name"]
        if col_name.startswith("#") or not col_name.strip():
            continue
        comment = row["comment"] or ""
        status = "OK" if comment else "MISSING"
        print(f"  {col_name:<40} [{status}] {comment}")
    print()
