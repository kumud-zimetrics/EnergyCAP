# schemas.py
# Schema design for the utility-bill Bronze ingestion pipeline.
#
# S3 layout this pipeline reads from:
#
#   raw/utility-bills/
#       bill_type=electricity/
#           vendor_id=CPC001/  CPC001_STR-001_electricity_2025-01_BILL-1001.pdf
#           vendor_id=REC002/  REC002_STR-002_electricity_2025-01_BILL-1037.pdf
#       bill_type=water/
#           vendor_id=AMW001/  AMW001_STR-001_water_2025-01_BILL-1025.pdf
#       bill_type=gas/
#           vendor_id=MGU001/  MGU001_STR-002_gas_2025-01_BILL-1049.pdf
#           vendor_id=NGS002/  NGS002_STR-001_gas_2025-01_BILL-1013.pdf
#
# bill_type and vendor_id are Hive-style partition columns baked into the path,
# so Auto Loader/Spark infers them for free -- no need to parse them out of the
# PDF or filename.

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, TimestampType
)

# ---------------------------------------------------------------------------
# 1. RAW LANDING SCHEMA  (what Auto Loader gives you reading binaryFile)
# ---------------------------------------------------------------------------
# This is not something you define yourself -- Spark's binaryFile format
# always returns this shape. Documented here so the next layer's contract
# is clear.
RAW_BINARY_SCHEMA = StructType([
    StructField("path", StringType(), False),          # full S3 path, incl. partitions
    StructField("modificationTime", TimestampType(), False),
    StructField("length", DoubleType(), False),         # file size in bytes
    StructField("content", StringType(), False),        # actually BinaryType; PDF bytes
])

# ---------------------------------------------------------------------------
# 2. BRONZE EXTRACTED SCHEMA  (common schema, one row per parsed PDF)
# ---------------------------------------------------------------------------
# One unified schema for electricity, water, and gas. A single schema (rather
# than three separate ones) is deliberate:
#   - FR-5/FR-7/FR-8 all operate "per store+commodity" -- a single table with
#     a `commodity` column makes those group-bys trivial.
#   - `unit` already carries the commodity-specific measurement (kWh / gallons
#     / therms), so we don't need different numeric columns per type.
#   - bill_type / vendor_id come from the S3 partition path, not the PDF body
#     -- cheap, reliable, and free of parsing errors.
BRONZE_BILLS_SCHEMA = StructType([
    # --- identifiers ---
    StructField("bill_id",          StringType(), True),   # invoice/statement no. from PDF body
    StructField("building_id",      StringType(), True),   # matched from customer name/address -> buildings.csv
    StructField("commodity",        StringType(), False),  # electricity | water | gas  (= bill_type partition)
    StructField("vendor",           StringType(), False),  # human-readable vendor name
    StructField("vendor_id",        StringType(), False),  # e.g. CPC001         (= vendor_id partition)

    # --- billing period ---
    StructField("bill_start_date",  DateType(), True),
    StructField("bill_end_date",    DateType(), True),

    # --- amounts ---
    StructField("consumption",      DoubleType(), True),   # may be negative (credit/correction)
    StructField("unit",             StringType(), True),   # kWh | therms | gallons
    StructField("cost_usd",         DoubleType(), True),

    # --- lineage / traceability (never dropped, always populated) ---
    StructField("source_file",      StringType(), False),  # full S3 path of the source PDF
    StructField("bill_type_partition", StringType(), False),  # raw partition value, for cross-check
    StructField("ingestion_ts",     TimestampType(), False),  # when this row was written to Bronze
    StructField("parser_version",   StringType(), False),  # e.g. "cpc_v1" -- which parser produced this row

    # --- parse-quality flags (FR-1: never silently skip a failure) ---
    StructField("parse_status",     StringType(), False),  # SUCCESS | PARTIAL | FAILED
    StructField("parse_error",      StringType(), True),   # error message / missing-field note if not SUCCESS
])

# ---------------------------------------------------------------------------
# 3. Commodity -> expected unit, used for validation right after extraction
# ---------------------------------------------------------------------------
EXPECTED_UNIT_BY_COMMODITY = {
    "electricity": "kWh",
    "gas": "therms",
    "water": "gallons",
}

# ---------------------------------------------------------------------------
# 4. vendor_id -> parser function name, used to dispatch the right extractor
# ---------------------------------------------------------------------------
VENDOR_PARSER_MAP = {
    "CPC001": "parse_citypower",       # table-style corporate invoice
    "REC002": "parse_regional_electric",  # two-column meter-read summary
    "MGU001": "parse_metrogas",        # narrative / paragraph statement
    "NGS002": "parse_national_gas",    # bordered form layout
    "AMW001": "parse_aqua_municipal",  # municipal utility layout
}

# ---------------------------------------------------------------------------
# 5. SILVER BILLS SCHEMA  (FR-4: cleansed, deduped, flagged bills)
# ---------------------------------------------------------------------------
# Built from BRONZE_BILLS_SCHEMA. Differences from Bronze, and why:
#   - Types are enforced (bill_start_date/bill_end_date become real DateType,
#     not text) -- Bronze may have these as strings straight out of regex.
#   - Exact-duplicate ROWS are dropped here (same PDF parsed into Bronze
#     twice). This is NOT the same as FR-5's "duplicate bill" detection,
#     which flags two DIFFERENT bill_ids covering the same building +
#     commodity + period -- that's a business-level issue and stays in
#     Silver, to be caught by the FR-5 exceptions table instead.
#   - Negative/zero consumption and missing required fields are FLAGGED,
#     never deleted -- a negative value is often a legitimate billing
#     credit, so Silver's job is to surface it, not decide it's wrong.
SILVER_BILLS_SCHEMA = StructType([
    StructField("bill_id",          StringType(), True),
    StructField("building_id",      StringType(), True),
    StructField("commodity",        StringType(), False),  # standardized lowercase
    StructField("vendor",           StringType(), False),
    StructField("vendor_id",        StringType(), False),

    StructField("bill_start_date",  DateType(), True),
    StructField("bill_end_date",    DateType(), True),

    StructField("consumption",      DoubleType(), True),
    StructField("unit",             StringType(), True),   # standardized casing: kWh/therms/gallons
    StructField("cost_usd",         DoubleType(), True),

    StructField("source_file",      StringType(), False),  # lineage, always retained

    # --- FR-4 quality flags (never used to drop a row) ---
    StructField("is_negative_consumption",   StringType(), False),  # "true"/"false"
    StructField("is_zero_consumption",       StringType(), False),
    StructField("has_missing_required_field", StringType(), False),
    StructField("missing_fields",            StringType(), True),   # comma-list, e.g. "cost_usd,bill_id"

    StructField("silver_ingestion_ts", TimestampType(), False),
])

# ---------------------------------------------------------------------------
# 6. SILVER METER INTERVALS SCHEMA  (FR-4: cleansed, deduped meter reads)
# ---------------------------------------------------------------------------
# Dedup key is (timestamp, meter_id) -- not (timestamp, building_id) --
# because a building could in principle have more than one meter; a true
# duplicate is the SAME meter reporting the SAME instant twice, not two
# different meters legitimately reporting at the same instant.
SILVER_METER_SCHEMA = StructType([
    StructField("timestamp",   TimestampType(), False),
    StructField("building_id", StringType(), False),
    StructField("meter_id",    StringType(), False),
    StructField("kwh",         DoubleType(), True),
    StructField("source_file", StringType(), False),

    StructField("is_missing_kwh", StringType(), False),  # "true"/"false"
    StructField("silver_ingestion_ts", TimestampType(), False),
])
