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
EXCEL_READER = "auto"              # "auto": native reader, pandas if it is not usable · "native" · "pandas"

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


def with_comments(df, column_comments):
    """Attach column comments to the DataFrame schema.

    A Delta table written from this DataFrame is created with the comments already in place. The alternative,
    one ALTER TABLE ... ALTER COLUMN per column after the write, is a Delta commit plus a catalog round trip
    each (about a second on serverless), and the old version of the pipeline issued around 75 of them per run."""
    return df.select([
        F.col(c).alias(c, metadata={"comment": column_comments[c]}) if c in column_comments else F.col(c)
        for c in df.columns
    ])


def describe_governance(table):
    """Table comment and column comments as the catalog holds them now, from one DESCRIBE call."""
    columns, table_comment, in_columns = {}, "", True
    for r in spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect():
        name = (r["col_name"] or "").strip()
        if in_columns:
            if not name or name.startswith("#"):
                in_columns = False  # end of the column section
            else:
                columns[name] = r["comment"] or ""
        elif name == "Comment":
            table_comment = r["data_type"] or ""
    return table_comment, columns


def current_tags(table):
    """Tags the catalog holds for a table, from one information_schema query."""
    catalog, schema, name = table.split(".")
    rows = spark.sql(
        f"SELECT tag_name, tag_value FROM {catalog}.information_schema.table_tags "
        f"WHERE schema_name = '{sql_str(schema)}' AND table_name = '{sql_str(name)}'"
    ).collect()
    return {r["tag_name"]: r["tag_value"] for r in rows}


def apply_governance(table, table_comment, column_comments, tags):
    """Table comment, column comments and tags. The data dictionary is generated from these.

    Only what differs from the catalog is changed. A table written through with_comments() normally needs
    nothing: the cost is two reads, not one DDL statement per column.
    Tags are best effort: tag support on Free Edition is still to be validated."""
    current_comment, current_columns = describe_governance(table)
    changed = []
    if current_comment != table_comment:
        spark.sql(f"COMMENT ON TABLE {table} IS '{sql_str(table_comment)}'")
        changed.append("table comment")
    for col, text in column_comments.items():
        if col in current_columns and current_columns[col] != text:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{sql_str(text)}'")
            changed.append(col)
    try:
        if any(current_tags(table).get(k) != v for k, v in tags.items()):
            tag_sql = ", ".join(f"'{sql_str(k)}' = '{sql_str(v)}'" for k, v in tags.items())
            spark.sql(f"ALTER TABLE {table} SET TAGS ({tag_sql})")
            changed.append("tags")
    except Exception as exc:
        print(f"Tags not applied to {table} (to be validated on Free Edition): {str(exc)[:160]}")
    print(f"Governance on {table}: " + (", ".join(changed) if changed else "up to date, no DDL issued"))


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
