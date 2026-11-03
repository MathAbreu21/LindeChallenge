# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver layer: typing, flags and data quality rules
# MAGIC
# MAGIC Reads `bronze_orders` and produces:
# MAGIC - `silver_orders`: typed, validated, flagged order lines (the clean layer everything else builds on);
# MAGIC - `silver_orders_quarantine`: rows that broke a quarantine rule, with the reason (nothing is silently dropped);
# MAGIC - `dq_rule_results`: one row per rule per run (the audit trail of the checks).
# MAGIC
# MAGIC Rules are code, each with an explicit consequence:
# MAGIC
# MAGIC | Action | What happens |
# MAGIC | --- | --- |
# MAGIC | fail | the run stops before anything is written to silver |
# MAGIC | quarantine | the row goes to the quarantine table with the rule id |
# MAGIC | flag | the row stays in silver with a boolean column |
# MAGIC | alert | the row stays; the result is logged and shown for a person to look at |
# MAGIC
# MAGIC Silver is rebuilt from bronze on every run, so it is safe to repeat.
# MAGIC
# MAGIC Shape of the run: bronze is scanned three times (one aggregation gives every rule result, the bronze totals, the
# MAGIC duplicate keys and the comparable-window cut; then the two writes), the rule results are written once, and the
# MAGIC column comments travel with the schema, so governance issues no DDL unless something changed.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import operator
from functools import reduce

# Window stated in the brief. Rows outside it are quarantined; change it when a new delivery extends the range.
VALID_FROM = "2017-01-02"
VALID_TO = "2022-11-21"

EXPECTED_UNIT = "CYLINDERS"
EXPECTED_CURRENCY = "EUR"
DEFAULT_SEGMENT = "Default value for new customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Type and derive

# COMMAND ----------

bronze = spark.table(T_BRONZE_ORDERS)

cid = F.col("CUSTOMER_ID").cast("string")
stg = bronze.select(
    F.col("COUNTRY").alias("country"),
    cid.alias("customer_id"),
    # Customer ids end in a 6-digit family (090900, 090200, ...); what is before it is the radical.
    F.expr("substring(cast(CUSTOMER_ID as string), 1, length(cast(CUSTOMER_ID as string)) - 6)").alias("customer_radical"),
    F.expr("right(cast(CUSTOMER_ID as string), 6)").alias("customer_id_family"),
    F.col("CUSTOMER_INDUSTRY_SEGMENT").alias("customer_segment"),
    F.col("PRODUCT_ID").alias("product_id"),
    F.col("PRODUCT_DESCRIPTION").alias("product_description"),
    F.col("PRODUCT_SUPERGROUP").alias("product_supergroup"),
    # try_to_timestamp returns NULL for an invalid date instead of failing the whole job (ANSI mode is on in serverless)
    F.expr("cast(try_to_timestamp(cast(ORDER_CREATION_DATE as string), 'yyyyMMdd') as date)").alias("order_date"),
    F.col("ORDER_CREATION_DATE").alias("_order_date_raw"),
    F.col("ORDER_CHANNEL").alias("order_channel"),
    F.col("ORDER_NO").alias("order_no"),
    F.col("ORDER_ITEM_NO").cast("int").alias("order_item_no"),
    F.col("QUANTITY").cast("int").alias("quantity"),
    F.col("QUANTITY_UNIT").alias("quantity_unit"),
    F.col("ORDER_VALUE").cast("decimal(18,2)").alias("order_value"),
    F.col("ORDER_VALUE_UNIT").alias("order_value_unit"),
    F.col("_source_file"),
    F.col("_batch_id").alias("_bronze_batch_id"),
)

# Flags that describe a row without removing it
stg = (
    stg
    .withColumn("is_zero_value", F.col("order_value") == 0)
    .withColumn("is_negative_value", F.col("order_value") < 0)
    # Legacy order-number series: 9-digit numbers starting with 3 (the rest are 10-digit, starting with 1)
    .withColumn("is_order_series_3", F.substring(F.col("order_no").cast("string"), 1, 1) == "3")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data quality rules (as code)

# COMMAND ----------

def violated(expr):
    """A NULL test result counts as a violation."""
    return F.coalesce(expr, F.lit(True))


ROW_RULES = [
    {"id": "R02_date_in_range", "action": "quarantine",
     "description": f"Order date is a real date between {VALID_FROM} and {VALID_TO}",
     "violation": violated(
         F.col("order_date").isNull()
         | (F.col("order_date") < F.lit(VALID_FROM).cast("date"))
         | (F.col("order_date") > F.lit(VALID_TO).cast("date")))},
    {"id": "R03_quantity_positive", "action": "quarantine",
     "description": "Quantity is greater than zero",
     "violation": violated(F.col("quantity") <= 0)},
    {"id": "R04_value_positive", "action": "flag",
     "description": "Order value is greater than zero (zero or negative values are flagged, not removed)",
     "violation": violated(F.col("order_value") <= 0)},
    {"id": "R05_unit_and_currency", "action": "fail",
     "description": f"Quantity unit is {EXPECTED_UNIT} and currency is {EXPECTED_CURRENCY}; other units are never converted silently",
     "violation": violated((F.col("quantity_unit") != EXPECTED_UNIT) | (F.col("order_value_unit") != EXPECTED_CURRENCY))},
    {"id": "R06_channel_known", "action": "alert",
     "description": f"Order channel is not {UNKNOWN_CHANNEL}",
     "violation": violated(F.col("order_channel") == UNKNOWN_CHANNEL)},
    {"id": "R07_segment_filled", "action": "alert",
     "description": "Customer segment is a real label, not the default placeholder",
     "violation": violated((F.col("customer_segment") == DEFAULT_SEGMENT) | F.col("customer_segment").isNull())},
]
quarantine_rules = [r for r in ROW_RULES if r["action"] == "quarantine"]
any_quarantine = reduce(operator.or_, [r["violation"] for r in quarantine_rules])

# One pass over bronze for every number needed before writing: bronze totals, rows and value broken by each
# rule, duplicate keys (R01) and the latest clean date (comparable window). Each of these used to be its own query.
aggs = [
    F.count("*").alias("total_rows"),
    F.sum("order_value").alias("total_value"),
    F.countDistinct(F.struct("order_no", "order_item_no")).alias("distinct_keys"),
    F.max(F.when(~any_quarantine, F.col("order_date"))).alias("max_clean_date"),
]
for r in ROW_RULES:
    aggs.append(F.sum(F.when(r["violation"], 1).otherwise(0)).alias(f"{r['id']}__rows"))
    aggs.append(F.sum(F.when(r["violation"], F.coalesce(F.col("order_value"), F.lit(0).cast("decimal(18,2)")))).alias(f"{r['id']}__value"))
agg = stg.agg(*aggs).first().asDict()

bronze_rows = int(agg["total_rows"])
bronze_value = agg["total_value"] if agg["total_value"] is not None else Decimal("0.00")
duplicate_rows = bronze_rows - int(agg["distinct_keys"] or 0)  # rows that share a key with an earlier row
max_date = agg["max_clean_date"]
print(f"Bronze: {bronze_rows:,} rows, value {bronze_value:,}")

now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
results = [(
    RUN_ID, now, "silver", "R01_unique_order_line",
    "ORDER_NO + ORDER_ITEM_NO identifies one line (violations: rows that repeat a key)", "fail",
    int(duplicate_rows), Decimal("0.00"), bronze_rows,
    "FAIL" if duplicate_rows > 0 else "PASS",
)]
for r in ROW_RULES:
    n = int(agg[f"{r['id']}__rows"] or 0)
    v = agg[f"{r['id']}__value"] or Decimal("0.00")
    results.append((
        RUN_ID, now, "silver", r["id"], r["description"], r["action"],
        n, Decimal(v).quantize(Decimal("0.01")), bronze_rows,
        STATUS_IF_VIOLATED[r["action"]] if n > 0 else "PASS",
    ))

DQ_TABLE_COMMENT = (
    "One row per data quality rule per run: what was checked, the action on violation, "
    "how many rows and how much value were affected."
)
DQ_COLUMN_COMMENTS = {
    "run_id": "Identifier of the pipeline run.",
    "run_ts": "UTC timestamp of the run.",
    "layer": "Layer the rule belongs to.",
    "rule_id": "Rule identifier.",
    "rule_description": "What the rule checks.",
    "action": "Consequence of a violation: fail, quarantine, flag or alert.",
    "violations": "Rows that broke the rule.",
    "violation_value": "Order value on the rows that broke the rule.",
    "total_rows": "Rows checked.",
    "status": "PASS, FAIL, QUARANTINED, FLAGGED or ALERT.",
}
DQ_TAGS = {"layer": "silver", "domain": "data_quality"}


def write_dq_results(rows):
    """Append this run's rule results in one commit. The first run creates the table with its column comments."""
    df = spark.createDataFrame(rows, RESULT_SCHEMA)
    if not spark.catalog.tableExists(T_DQ_RESULTS):
        df = with_comments(df, DQ_COLUMN_COMMENTS)
    df.write.format("delta").mode("append").saveAsTable(T_DQ_RESULTS)
    return df


display(spark.createDataFrame(results, RESULT_SCHEMA).drop("run_id", "run_ts", "layer"))

failed = [r for r in results if r[9] == "FAIL"]
if failed:
    write_dq_results(results)  # the audit trail is kept even when the run stops here
    raise RuntimeError(
        "Data quality FAIL rules triggered, nothing was written to silver: "
        + ", ".join(f"{r[3]} ({r[6]:,} rows)" for r in failed)
    )
print("No fail-level rule triggered. Results are written together with the reconciliation rule after the load.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Split clean rows from quarantine

# COMMAND ----------

stg = stg.withColumn(
    "_quarantine_reason",
    F.concat_ws(",", *[F.when(r["violation"], F.lit(r["id"])) for r in quarantine_rules]),
)

clean = stg.where(F.col("_quarantine_reason") == "")
quarantined = stg.where(F.col("_quarantine_reason") != "")

# Comparable window: the same month-day span in every year, anchored on the latest date in the clean data
# (a partial last year must be compared with the same part of the other years). The date came from the single pass above.
if max_date is None:
    raise RuntimeError("Every bronze row broke a quarantine rule: there is nothing to write to silver.")
print(f"Latest order date in the data: {max_date}; comparable window = Jan 1 to {max_date:%b} {max_date.day} of each year")
cut_month, cut_day = F.lit(max_date.month), F.lit(max_date.day)
clean = clean.withColumn(
    "is_in_comparable_window",
    (F.month("order_date") < cut_month) | ((F.month("order_date") == cut_month) & (F.dayofmonth("order_date") <= cut_day)),
)

lineage = [
    F.col("_source_file"),
    F.col("_bronze_batch_id"),
    F.lit(RUN_ID).alias("_silver_run_id"),
    F.current_timestamp().alias("_silver_loaded_at"),
]
BASE_COLUMNS = [
    "country", "customer_id", "customer_radical", "customer_id_family", "customer_segment",
    "product_id", "product_description", "product_supergroup", "order_date", "order_channel",
    "order_no", "order_item_no", "quantity", "quantity_unit", "order_value", "order_value_unit",
    "is_zero_value", "is_negative_value", "is_order_series_3",
]

silver_df = clean.select(*BASE_COLUMNS, "is_in_comparable_window", *lineage)
quarantine_df = quarantined.select(*BASE_COLUMNS, "_order_date_raw", "_quarantine_reason", *lineage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write silver and quarantine (Delta, rebuilt from bronze)
# MAGIC The column comments are part of the written schema, so the rebuilt tables come out documented.

# COMMAND ----------

SILVER_COLUMN_COMMENTS = {
    "country": "Country of the order.",
    "customer_id": "Customer identifier as a string (an identifier, not a number).",
    "customer_radical": "Customer id without its 6-digit family suffix. Customers can appear under more than one family, so this is the key to check double counting.",
    "customer_id_family": "Last 6 digits of the customer id (090900 and 090200 are the main families).",
    "customer_segment": "Industry segment label at order time. A customer can carry more than one label over time.",
    "product_id": "Product (SKU) identifier.",
    "product_description": "Free-text product description; gas and size are parsed from it in gold.",
    "product_supergroup": "Product family.",
    "order_date": "Order creation date.",
    "order_channel": "Order channel label. Labels changed in Nov 2020, so channel mix across that date is not comparable.",
    "order_no": "Order number.",
    "order_item_no": "Order line number within the order.",
    "quantity": "Quantity ordered, in quantity_unit.",
    "quantity_unit": "Unit of quantity (CYLINDERS).",
    "order_value": "Order line value in order_value_unit.",
    "order_value_unit": "Currency of order_value (EUR).",
    "is_zero_value": "True when the line has value zero (no charge). Excluded from price and the price-volume-mix bridge.",
    "is_negative_value": "True when the line has a negative value.",
    "is_order_series_3": "True when the order number belongs to the legacy 9-digit series starting with 3.",
    "is_in_comparable_window": "True when the date falls in the same month-day span, Jan 1 to the latest date in the data, in any year.",
    "_order_date_raw": "Original YYYYMMDD integer from the source (kept for quarantined rows).",
    "_quarantine_reason": "Comma-separated ids of the quarantine rules the row broke.",
    "_source_file": "Source file the row came from.",
    "_bronze_batch_id": "Bronze load run that brought the row in.",
    "_silver_run_id": "Silver run that produced the row.",
    "_silver_loaded_at": "Timestamp of the silver run.",
}

for df, table in [(silver_df, T_SILVER_ORDERS), (quarantine_df, T_SILVER_QUARANTINE)]:
    (
        with_comments(df, SILVER_COLUMN_COMMENTS)
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    )
    print(f"Wrote {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reconcile layers
# MAGIC Every bronze row must be in silver or in quarantine, and the value must add up. Stops the run if not.
# MAGIC One query over both tables; its result is written with the other rule results in a single commit.

# COMMAND ----------

by_layer = {
    r["layer"]: r
    for r in (
        spark.table(T_SILVER_ORDERS).select(F.lit("silver").alias("layer"), "order_value")
        .unionByName(spark.table(T_SILVER_QUARANTINE).select(F.lit("quarantine").alias("layer"), "order_value"))
        .groupBy("layer").agg(F.count("*").alias("n"), F.sum("order_value").alias("v"))
        .collect()
    )
}


def layer_totals(layer):
    r = by_layer.get(layer)
    return (int(r["n"]), r["v"] or Decimal("0.00")) if r is not None else (0, Decimal("0.00"))


silver_rows, silver_value = layer_totals("silver")
quarantine_rows, quarantine_value = layer_totals("quarantine")

rows_ok = silver_rows + quarantine_rows == bronze_rows
value_ok = silver_value + quarantine_value == bronze_value
results.append((
    RUN_ID, now, "silver", "R08_layers_reconcile",
    "Bronze rows and value equal silver plus quarantine", "fail",
    0 if (rows_ok and value_ok) else int(abs(bronze_rows - silver_rows - quarantine_rows)) or 1,
    Decimal("0.00") if value_ok else Decimal(bronze_value - silver_value - quarantine_value).quantize(Decimal("0.01")),
    bronze_rows, "PASS" if (rows_ok and value_ok) else "FAIL",
))
results_df = write_dq_results(results)

display(pd.DataFrame([
    {"layer": "bronze", "rows": bronze_rows, "value": float(bronze_value)},
    {"layer": "silver", "rows": silver_rows, "value": float(silver_value)},
    {"layer": "quarantine", "rows": quarantine_rows, "value": float(quarantine_value)},
]))
assert rows_ok and value_ok, "Reconciliation failed: bronze is not equal to silver plus quarantine."
print("Reconciliation passed: bronze = silver + quarantine, rows and value.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Governance: comments and tags
# MAGIC Compared with the catalog first; only differences are applied. Column comments were written with the schema,
# MAGIC so a normal run issues no DDL here.

# COMMAND ----------

apply_governance(
    T_SILVER_ORDERS,
    "Typed, validated and flagged order lines. One row per order line. Rebuilt from bronze_orders on every run; "
    "rows that break a quarantine rule are in silver_orders_quarantine.",
    SILVER_COLUMN_COMMENTS, {"layer": "silver", "domain": "sales"},
)
apply_governance(
    T_SILVER_QUARANTINE,
    "Order lines that broke a quarantine rule, with the rule id in _quarantine_reason. Review before reprocessing.",
    SILVER_COLUMN_COMMENTS, {"layer": "silver", "domain": "sales", "purpose": "quarantine"},
)
apply_governance(T_DQ_RESULTS, DQ_TABLE_COMMENT, DQ_COLUMN_COMMENTS, DQ_TAGS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of this run

# COMMAND ----------

display(results_df.select("rule_id", "action", "violations", "violation_value", "status"))
print(f"Silver ready: {silver_rows:,} rows in {T_SILVER_ORDERS}; {quarantine_rows:,} in quarantine. Next: 03 · Gold.")
