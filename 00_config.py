# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Shared configuration
# MAGIC
# MAGIC Names, control totals and small helpers used by every notebook (`%run ./00_config`).
# MAGIC One place to change a catalog, a table name or a convention, so layers stay consistent.

# COMMAND ----------

import datetime
import uuid
from decimal import Decimal

import pandas as pd
from pyspark.sql import functions as F

# --- Where things live ---------------------------------------------------------
CATALOG = "lindedata"
RAW_SCHEMA = "raw"                 # landing zone: source files exactly as delivered
LANDING_VOLUME = "lindechallenge"  # volume inside the raw schema
SOURCE_FILE = ""                   # leave empty to auto-detect the single .xlsx/.csv in the volume

LANDING_DIR = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{LANDING_VOLUME}"

SCHEMAS = {
    "raw": "Landing zone. Source files exactly as delivered; nothing is modified here.",
    "bronze": "Raw copy of source data in Delta. Values untouched, audit columns added. Append only.",
    "silver": "Typed, validated and flagged data. Data quality rules run here.",
    "gold": "Star schema and marts. The only layer that reporting reads.",
}

T_BRONZE_ORDERS = f"{CATALOG}.bronze.bronze_orders"
T_SILVER_ORDERS = f"{CATALOG}.silver.silver_orders"
T_SILVER_QUARANTINE = f"{CATALOG}.silver.silver_orders_quarantine"
T_DQ_RESULTS = f"{CATALOG}.silver.dq_rule_results"

RUN_ID = uuid.uuid4().hex[:12]

# --- Source contract -----------------------------------------------------------
# Control totals taken from the source file with an independent tool (pandas),
# before any pipeline ran. Bronze must match them exactly.
MANIFEST = {
    "rows": 224_670,
    "orders": 165_547,
    "cylinders": 615_067,
    "total_value": 28_304_592.15,
    "min_date": 20170102,
    "max_date": 20221121,
}

SOURCE_COLUMNS = [
    "COUNTRY", "CUSTOMER_ID", "CUSTOMER_INDUSTRY_SEGMENT", "PRODUCT_ID", "PRODUCT_DESCRIPTION",
    "PRODUCT_SUPERGROUP", "ORDER_CREATION_DATE", "ORDER_CHANNEL", "ORDER_NO", "ORDER_ITEM_NO",
    "QUANTITY_UNIT", "QUANTITY", "ORDER_VALUE_UNIT", "ORDER_VALUE",
]
# Types bronze keeps: same values as the sheet, typed only so every read path gives an identical table.
NUMERIC_TYPES = {
    "CUSTOMER_ID": "bigint", "ORDER_CREATION_DATE": "bigint", "ORDER_NO": "bigint",
    "ORDER_ITEM_NO": "bigint", "QUANTITY": "bigint", "ORDER_VALUE": "double",
}


# --- Helpers -------------------------------------------------------------------
def sql_str(text):
    """Escape a string for use inside single quotes in SQL."""
    return text.replace("'", "''")


def apply_governance(table, table_comment, column_comments, tags):
    """Table comment, column comments and tags. The data dictionary is generated from these.
    Tags are best effort: tag support on Free Edition is still to be validated."""
    spark.sql(f"COMMENT ON TABLE {table} IS '{sql_str(table_comment)}'")
    existing = {f.name for f in spark.table(table).schema.fields}
    for col, text in column_comments.items():
        if col in existing:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{sql_str(text)}'")
    tag_sql = ", ".join(f"'{sql_str(k)}' = '{sql_str(v)}'" for k, v in tags.items())
    try:
        spark.sql(f"ALTER TABLE {table} SET TAGS ({tag_sql})")
    except Exception as exc:
        print(f"Tags not applied to {table} (to be validated on Free Edition): {str(exc)[:160]}")


def resolve_source_file():
    """Return (file name, full path) of the file to load from the landing volume."""
    names = [f.name for f in dbutils.fs.ls(LANDING_DIR)]
    if SOURCE_FILE:
        if SOURCE_FILE not in names:
            raise FileNotFoundError(f"{SOURCE_FILE} not found in {LANDING_DIR}. Found: {names}")
        return SOURCE_FILE, f"{LANDING_DIR}/{SOURCE_FILE}"
    candidates = [n for n in names if n.lower().endswith((".xlsx", ".csv"))]
    if len(candidates) != 1:
        raise FileNotFoundError(
            f"Expected exactly one .xlsx or .csv in {LANDING_DIR}, found {candidates or 'none'} "
            f"(all files: {names}). Set SOURCE_FILE in 00_config to pick one."
        )
    return candidates[0], f"{LANDING_DIR}/{candidates[0]}"


print(f"Config loaded: catalog={CATALOG}, landing={LANDING_DIR}, run_id={RUN_ID}")
