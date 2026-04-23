# NOAA Weather Data Pipeline
#
# Spark Declarative Pipeline that ingests climate data from the
# Global Historical Climatology Network daily (GHCNd) dataset
# provided by the National Oceanic and Atmospheric Administration (NOAA).
#
# The pipeline reads directly from the public `noaa-ghcn-pds` S3 bucket and produces:
# - Dimension tables (countries, states, stations): materialized views refreshed on each pipeline run
# - Fact table (weather): streaming table incrementally ingested via Auto Loader
#
# Links:
# - AWS Marketplace Listing: https://aws.amazon.com/marketplace/pp/prodview-dzppucmwfpuk4#overview
# - Documentation: https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ghcn
# - S3 Bucket: https://noaa-ghcn-pds.s3.amazonaws.com/index.html

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, substring, to_date, trim

# -- Dimension Tables ----------------------------------------------------------


@dp.materialized_view(
    name="countries",
    comment="NOAA GHCNd country dimension. Maps two-character FIPS country codes to full country names.",
    schema="""
        country_code STRING NOT NULL
            COMMENT 'The FIPS country code of the country where the station is located'
            PRIMARY KEY RELY,
        country STRING
            COMMENT 'The full name of the country'
    """,
)
def countries():
    return spark.read.text("s3a://noaa-ghcn-pds/ghcnd-countries.txt").select(
        trim(substring(col("value"), 1, 2)).alias("country_code"),
        trim(substring(col("value"), 3, 63)).alias("country"),
    )


@dp.materialized_view(
    name="states",
    comment="NOAA GHCNd state/province dimension. Maps two-character postal codes to full names. Only applies to USA and Canada.",
    schema="""
        state_code STRING NOT NULL
            COMMENT 'The POSTAL code of the U.S. state/territory or Canadian province where the station is located'
            PRIMARY KEY RELY,
        state STRING
            COMMENT 'The full name of the state in USA or Canada'
    """,
)
def states():
    return spark.read.text("s3a://noaa-ghcn-pds/ghcnd-states.txt").select(
        trim(substring(col("value"), 1, 2)).alias("state_code"),
        trim(substring(col("value"), 3, 63)).alias("state"),
    )


@dp.materialized_view(
    name="stations",
    comment="NOAA GHCNd weather station dimension. Contains location, elevation, and network flags for every station in the dataset.",
    schema="""
        station_id STRING NOT NULL
            COMMENT 'Weather station identification code. The first two characters denote the FIPS country code. The third character is a network code that identifies the station numbering system used: 0 = unspecified (station identified by up to eight alphanumeric characters), 1 = Community Collaborative Rain, Hail, and Snow (CoCoRaHS) based identification number. To ensure consistency with GHCN Daily, all numbers in the original CoCoRaHS IDs have been left-filled to make them all four digits long. In addition, the characters "-" and "_" have been removed to ensure that the IDs do not exceed 11 characters when preceded by "US1". For example, the CoCoRaHS ID "AZ-MR-156" becomes "US1AZMR0156" in GHCN-Daily. C = U.S. Cooperative Network identification number (last six characters of the GHCN-Daily ID), E = Identification number used in the ECA&D non-blended dataset, M = World Meteorological Organization ID (last five characters of the GHCN-Daily ID), N = Identification number used in data supplied by a National Meteorological or Hydrological Center, R = U.S. Interagency Remote Automatic Weather Station (RAWS) identifier, S = U.S. Natural Resources Conservation Service SNOwpack TELemtry (SNOTEL) station identifier, W = WBAN identification number (last five characters of the GHCN-Daily ID). The remaining eight characters contain the actual station ID.'
            PRIMARY KEY RELY,
        country_code STRING
            COMMENT 'FIPS country code',
        latitude DOUBLE
            COMMENT 'Latitude of the station in decimal degrees',
        longitude DOUBLE
            COMMENT 'Longitude of the station in decimal degrees',
        elevation DOUBLE
            COMMENT 'Elevation of the station in meters',
        state_code STRING
            COMMENT 'U.S. postal code for the state (for U.S. and Canadian stations only)',
        station_name STRING
            COMMENT 'Name of the station',
        gsn_flag STRING
            COMMENT 'Flag that indicates whether the station is part of the GCOS Surface Network (GSN). The flag is assigned by cross-referencing the number in the WMOID field with the official list of GSN stations. There are two possible values: Blank = non-GSN station or WMO Station number not available, GSN = GSN station',
        hcn_crn_flag STRING
            COMMENT 'Flag that indicates whether the station is part of the U.S. Historical Climatology Network (HCN). There are three possible values: Blank = Not a member of the U.S. Historical Climatology or U.S. Climate Reference Networks, HCN = U.S. Historical Climatology Network station, CRN = U.S. Climate Reference Network or U.S. Regional Climate Network Station',
        wmo_id INT
            COMMENT 'The World Meteorological Organization (WMO) number for the station. If the station has no WMO number (or one has not yet been matched to this station), then the field is blank.',
        CONSTRAINT fk_stations_country FOREIGN KEY (country_code) REFERENCES countries(country_code),
        CONSTRAINT fk_stations_state FOREIGN KEY (state_code) REFERENCES states(state_code)
    """,
)
def stations():
    return spark.read.text("s3a://noaa-ghcn-pds/ghcnd-stations.txt").select(
        trim(substring(col("value"), 1, 11)).alias("station_id"),
        trim(substring(col("value"), 1, 2)).alias("country_code"),
        substring(col("value"), 13, 8).cast("double").alias("latitude"),
        substring(col("value"), 22, 9).cast("double").alias("longitude"),
        substring(col("value"), 32, 6).cast("double").alias("elevation"),
        trim(substring(col("value"), 39, 2)).alias("state_code"),
        trim(substring(col("value"), 42, 30)).alias("station_name"),
        trim(substring(col("value"), 73, 3)).alias("gsn_flag"),
        trim(substring(col("value"), 77, 3)).alias("hcn_crn_flag"),
        substring(col("value"), 81, 5).try_cast("integer").alias("wmo_id"),
    )


# -- Fact Table ----------------------------------------------------------------


@dp.table(
    name="weather",
    comment="NOAA GHCNd daily weather observations. One record per station, date, and element. Incrementally ingested via Auto Loader from s3a://noaa-ghcn-pds/csv/by_year/.",
    schema="""
        station_id STRING
            COMMENT '11 character station identification code. See stations dimension table for details.',
        date DATE
            COMMENT 'Date of the weather observation',
        data_value BIGINT
            COMMENT 'Data value for the element. Units vary by element type (tenths of mm for PRCP, tenths of degrees C for TMAX/TMIN, mm for SNOW/SNWD).',
        m_flag STRING
            COMMENT 'Measurement flag. Blank = no measurement info applicable, B = precipitation total formed from two 12-hour totals, D = from four six-hour totals, H = highest/lowest hourly temperature or average of hourly values, K = converted from knots, L = temperature appears lagged, O = converted from oktas, P = missing presumed zero, T = trace, W = converted from 16-point WBAN code',
        q_flag STRING
            COMMENT 'Quality flag. Blank = passed all checks, D = duplicate, G = gap, I = internal consistency, K = streak/frequent-value, L = multiday length, M = mega consistency, N = naught, O = climatological outlier, R = lagged range, S = spatial consistency, T = temporal consistency, W = too warm for snow, X = bounds, Z = Datzilla investigation',
        s_flag STRING
            COMMENT 'Source flag indicating the original data source. 0 = US COOP, A/B = ASOS, C = Environment Canada, E = ECA&D, G = GCOS, M = METAR, N = CoCoRaHS, R = NCEI Reference Network, S = Global Summary of Day, and others',
        obs_time STRING
            COMMENT '4-character time of observation in hour-minute format (e.g. 0700 = 7:00 am)',
        element STRING
            COMMENT 'Element type: PRCP = Precipitation (tenths of mm), SNOW = Snowfall (mm), SNWD = Snow depth (mm), TMAX = Maximum temperature (tenths of degrees C), TMIN = Minimum temperature (tenths of degrees C)',
        source_file_path STRING
            COMMENT 'Auto Loader metadata: path of the source file ingested',
        source_file_modification_time TIMESTAMP
            COMMENT 'Auto Loader metadata: last modification time of the source file',
        ingested_at TIMESTAMP
            COMMENT 'Timestamp when the record was ingested by the pipeline',
        CONSTRAINT fk_weather_station FOREIGN KEY (station_id) REFERENCES stations(station_id)
    """,
    cluster_by=["station_id", "date"],
)
def weather():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("s3a://noaa-ghcn-pds/csv/by_year/")
        .select(
            col("ID").alias("station_id"),
            to_date(col("DATE"), "yyyyMMdd").alias("date"),
            col("DATA_VALUE").cast("bigint").alias("data_value"),
            col("M_FLAG").alias("m_flag"),
            col("Q_FLAG").alias("q_flag"),
            col("S_FLAG").alias("s_flag"),
            col("OBS_TIME").alias("obs_time"),
            col("ELEMENT").alias("element"),
            col("_metadata.file_path").alias("source_file_path"),
            col("_metadata.file_modification_time")
            .cast("timestamp")
            .alias("source_file_modification_time"),
            current_timestamp().alias("ingested_at"),
        )
    )
