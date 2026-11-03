# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Setup and bronze layer
# MAGIC
# MAGIC **Linde EMEA data challenge · mini-lakehouse on Databricks Free Edition**
# MAGIC
# MAGIC 1. Creates the `raw`, `bronze`, `silver` and `gold` schemas (the landing volume already exists).
# MAGIC 2. Reads the source file from the landing volume (native Excel reader first, pandas as fallback; CSV also accepted).
# MAGIC 3. Writes `bronze_orders` as a Delta table: values untouched, plus audit columns. Append only.
# MAGIC 4. Reconciles the table against control totals from the source file and **fails** if anything differs.
# MAGIC 5. Adds table and column comments and tags (governance).
# MAGIC
# MAGIC Compute: serverless. Every step is safe to repeat, and a repeat does only the work it needs:
# MAGIC a file that is already in bronze is not opened again, the workbook is parsed once (not before and again
# MAGIC during the write), and governance touches only what differs from the catalog. `openpyxl` is needed by the
# MAGIC pandas fallback only, so it is installed on demand instead of a `%pip` restart on every run.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas
# MAGIC One `SHOW SCHEMAS` per run; `CREATE SCHEMA` only for the ones that are missing.

# COMMAND ----------

try:
    existing_schemas = {r[0] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()}
except Exception as exc:
    raise RuntimeError(
        f"Catalog '{CATALOG}' not found or not readable. Edit CATALOG in 00_config. ({str(exc)[:200]})"
    ) from exc

for schema, description in SCHEMAS.items():
    if schema not in existing_schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{sql_str(description)}'")
        print(f"Created schema {CATALOG}.{schema}")
print(f"Schemas in {CATALOG}: {', '.join(sorted(existing_schemas | set(SCHEMAS)))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the landing file

# COMMAND ----------

source_name, SRC_PATH = resolve_source_file()
print(f"Source file: {SRC_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Is the file already in bronze?
# MAGIC Checked before the workbook is opened. Parsing it is the slowest step of this notebook, and a repeat run used to
# MAGIC parse it anyway, only to find there was nothing to load.

# COMMAND ----------

bronze_exists = spark.catalog.tableExists(T_BRONZE_ORDERS)
already_loaded = 0
if bronze_exists:
    already_loaded = spark.table(T_BRONZE_ORDERS).where(F.col("_source_file") == SRC_PATH).count()
print(f"{source_name}: {already_loaded:,} rows already in {T_BRONZE_ORDERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the file
# MAGIC The native Excel reader (`spark.read.format("excel")`) needs Databricks Runtime 17.1 or above. Its columns are checked
# MAGIC against the source contract (schema only, no scan); if the read fails or the columns are wrong, the notebook falls
# MAGIC back to pandas + openpyxl and says so. Row count and totals are checked after the write, in the reconciliation step,
# MAGIC so the workbook is parsed for the write only, not once more just to count it.

# COMMAND ----------

def ensure_openpyxl():
    """openpyxl is not in the serverless base environment. Install it only when the pandas fallback runs
    (a `%pip install` cell would restart Python on every run, including the ones that never need it)."""
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        import importlib
        import subprocess
        import sys
        print("Installing openpyxl for the pandas fallback (add it to the job environment to skip this step).")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "openpyxl"])
        importlib.invalidate_caches()


def read_native():
    return spark.read.format("excel").option("headerRows", 1).load(SRC_PATH)


def read_pandas():
    ensure_openpyxl()
    pdf = pd.read_excel(SRC_PATH, engine="openpyxl")
    return spark.createDataFrame(pdf)


def read_csv():
    return spark.read.option("header", True).option("inferSchema", False).csv(SRC_PATH)


def read_source():
    """Return (DataFrame, read method). The columns must match the source contract."""
    if SRC_PATH.lower().endswith(".csv"):
        df, method = read_csv(), "csv"
    elif EXCEL_READER == "pandas":
        df, method = read_pandas(), "pandas + openpyxl"
    else:
        try:
            df, method = read_native(), "spark.read excel (native)"
            if list(df.columns) != SOURCE_COLUMNS:
                raise ValueError(f"native reader returned unexpected columns: {list(df.columns)}")
        except Exception as exc:
            if EXCEL_READER == "native":
                raise
            print(f"Native Excel reader not usable ({type(exc).__name__}: {str(exc)[:200]}). Falling back to pandas.")
            df, method = read_pandas(), "pandas + openpyxl"
    if list(df.columns) != SOURCE_COLUMNS:
        raise ValueError(f"{method} returned unexpected columns {list(df.columns)}; expected {SOURCE_COLUMNS}")
    return df, method

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write bronze (Delta, append only)
# MAGIC Audit columns: `_source_file`, `_loaded_at`, `_batch_id`. A file that is already in the table is not loaded twice.
# MAGIC The first load creates the table with its column comments in the schema (see `with_comments` in `00_config`).

# COMMAND ----------

BRONZE_TABLE_COMMENT = (
    "Raw copy of the INTERVIEW_ORDERS source file (one row per order line). Values untouched; "
    "audit columns added. Append only. Source of silver_orders."
)
BRONZE_COLUMN_COMMENTS = {
    "COUNTRY": "Country of the order (single value in this extract).",
    "CUSTOMER_ID": "Customer identifier as delivered by the source.",
    "CUSTOMER_INDUSTRY_SEGMENT": "Industry segment label of the customer at order time.",
    "PRODUCT_ID": "Product (SKU) identifier.",
    "PRODUCT_DESCRIPTION": "Free-text product description, not documented in the brief.",
    "PRODUCT_SUPERGROUP": "Product family, for example Packaged Industrial Argon.",
    "ORDER_CREATION_DATE": "Order creation date as an integer in YYYYMMDD format; typed to DATE in silver.",
    "ORDER_CHANNEL": "Order channel label (labels changed in Nov 2020).",
    "ORDER_NO": "Order number.",
    "ORDER_ITEM_NO": "Order line number within the order.",
    "QUANTITY_UNIT": "Unit of QUANTITY (CYLINDERS in this extract).",
    "QUANTITY": "Quantity ordered, in QUANTITY_UNIT.",
    "ORDER_VALUE_UNIT": "Currency of ORDER_VALUE (EUR in this extract).",
    "ORDER_VALUE": "Order line value in ORDER_VALUE_UNIT.",
    "_source_file": "Path of the source file this row was loaded from.",
    "_loaded_at": "Timestamp of the load.",
    "_batch_id": "Identifier of the load run.",
}
BRONZE_TAGS = {"layer": "bronze", "domain": "sales", "source": "interview_orders"}

wrote_this_run = False
if already_loaded > 0:
    print(f"{source_name} already loaded ({already_loaded:,} rows). Nothing to load. "
          f"To reload on purpose: DELETE FROM {T_BRONZE_ORDERS} WHERE _source_file = '{SRC_PATH}'")
else:
    raw, read_method = read_source()
    typed = raw.select([F.col(c).cast(NUMERIC_TYPES.get(c, "string")).alias(c) for c in SOURCE_COLUMNS])
    print(f"Read method: {read_method}")
    typed.printSchema()

    bronze_df = (
        typed
        .withColumn("_source_file", F.lit(SRC_PATH))
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(RUN_ID))
    )
    if not bronze_exists:
        bronze_df = with_comments(bronze_df, BRONZE_COLUMN_COMMENTS)
    bronze_df.write.format("delta").mode("append").saveAsTable(T_BRONZE_ORDERS)
    wrote_this_run = True
    print(f"Appended to {T_BRONZE_ORDERS} (batch {RUN_ID}, read with {read_method})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconcile against the source control totals
# MAGIC The notebook stops here if any number differs. A batch written by this run that fails the check is removed
# MAGIC again, so bronze never keeps rows that do not match the file.

# COMMAND ----------

res = (
    spark.table(T_BRONZE_ORDERS)
    .where(F.col("_source_file") == SRC_PATH)
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("ORDER_NO").alias("orders"),
        F.sum("QUANTITY").alias("cylinders"),
        F.round(F.sum(F.col("ORDER_VALUE").cast("decimal(18,2)")), 2).alias("total_value"),
        F.min("ORDER_CREATION_DATE").alias("min_date"),
        F.max("ORDER_CREATION_DATE").alias("max_date"),
    )
    .first()
    .asDict()
)

rows = []
for key, expected in MANIFEST.items():
    actual = res[key]
    actual = float(actual) if isinstance(actual, Decimal) else actual
    ok = actual is not None and abs(actual - expected) < 0.005
    rows.append({"check": key, "expected": expected, "bronze": actual, "ok": ok})

report = pd.DataFrame(rows)
display(report)
if not report["ok"].all():
    if wrote_this_run:
        spark.sql(f"DELETE FROM {T_BRONZE_ORDERS} WHERE _batch_id = '{RUN_ID}'")
        print(f"Batch {RUN_ID} removed from bronze: it did not match the source control totals.")
    raise AssertionError("Reconciliation failed: bronze does not match the source control totals.")
print("Reconciliation passed: bronze matches the source file.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance: comments and tags
# MAGIC Compared with the catalog first; only differences are applied (normally nothing, since the comments were written
# MAGIC with the schema).

# COMMAND ----------

apply_governance(T_BRONZE_ORDERS, BRONZE_TABLE_COMMENT, BRONZE_COLUMN_COMMENTS, BRONZE_TAGS)
display(spark.sql(f"DESCRIBE TABLE EXTENDED {T_BRONZE_ORDERS}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC `bronze_orders` is loaded and reconciled. Next: **02 · Silver**.