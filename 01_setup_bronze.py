# Databricks notebook source
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
# MAGIC Compute: serverless. Every step is safe to repeat.

# COMMAND ----------

# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas

# COMMAND ----------

catalogs = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
assert CATALOG in catalogs, f"Catalog '{CATALOG}' not found. Available: {catalogs}. Edit CATALOG in 00_config."

for schema, description in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{sql_str(description)}'")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the landing file

# COMMAND ----------

source_name, SRC_PATH = resolve_source_file()
print(f"Source file: {SRC_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the file
# MAGIC The native Excel reader (`spark.read.format("excel")`) needs Databricks Runtime 17.1 or above. Its result is validated
# MAGIC (columns and row count); if the read fails or looks wrong, the notebook falls back to pandas + openpyxl and says so.

# COMMAND ----------

def read_native():
    return spark.read.format("excel").option("headerRows", 1).load(SRC_PATH)


def read_pandas():
    pdf = pd.read_excel(SRC_PATH, engine="openpyxl")
    return spark.createDataFrame(pdf)


def read_csv():
    return spark.read.option("header", True).option("inferSchema", False).csv(SRC_PATH)


def looks_right(df):
    return list(df.columns) == SOURCE_COLUMNS and df.count() == MANIFEST["rows"]


if SRC_PATH.lower().endswith(".csv"):
    raw, read_method = read_csv(), "csv"
else:
    try:
        raw = read_native()
        if not looks_right(raw):
            raise ValueError("native reader returned unexpected columns or row count")
        read_method = "spark.read excel (native)"
    except Exception as exc:
        print(f"Native Excel reader not usable ({type(exc).__name__}: {str(exc)[:200]}). Falling back to pandas.")
        raw, read_method = read_pandas(), "pandas + openpyxl"

typed = raw.select([F.col(c).cast(NUMERIC_TYPES.get(c, "string")).alias(c) for c in SOURCE_COLUMNS])
print(f"Read method: {read_method}")
typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write bronze (Delta, append only)
# MAGIC Audit columns: `_source_file`, `_loaded_at`, `_batch_id`. A file that is already in the table is not loaded twice.

# COMMAND ----------

already_loaded = 0
if spark.catalog.tableExists(T_BRONZE_ORDERS):
    already_loaded = spark.table(T_BRONZE_ORDERS).where(F.col("_source_file") == SRC_PATH).count()

if already_loaded > 0:
    print(f"{source_name} already loaded ({already_loaded:,} rows). Nothing to do. "
          f"To reload on purpose: DELETE FROM {T_BRONZE_ORDERS} WHERE _source_file = '{SRC_PATH}'")
else:
    (
        typed
        .withColumn("_source_file", F.lit(SRC_PATH))
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(RUN_ID))
        .write.format("delta").mode("append").saveAsTable(T_BRONZE_ORDERS)
    )
    print(f"Appended to {T_BRONZE_ORDERS} (batch {RUN_ID}, read with {read_method})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconcile against the source control totals
# MAGIC The notebook stops here if any number differs.

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
    rows.append({"check": key, "expected": expected, "bronze": actual, "ok": abs(actual - expected) < 0.005})

report = pd.DataFrame(rows)
display(report)
assert report["ok"].all(), "Reconciliation failed: bronze does not match the source control totals."
print("Reconciliation passed: bronze matches the source file.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance: comments and tags

# COMMAND ----------

apply_governance(
    T_BRONZE_ORDERS,
    "Raw copy of the INTERVIEW_ORDERS source file (one row per order line). Values untouched; "
    "audit columns added. Append only. Source of silver_orders.",
    {
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
    },
    {"layer": "bronze", "domain": "sales", "source": "interview_orders"},
)
display(spark.sql(f"DESCRIBE TABLE EXTENDED {T_BRONZE_ORDERS}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC `bronze_orders` is loaded and reconciled. Next: **02 · Silver**.
