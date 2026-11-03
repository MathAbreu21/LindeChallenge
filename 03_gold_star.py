# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold layer: star schema
# MAGIC
# MAGIC Builds one fact table and four conformed dimensions from `silver_orders`.
# MAGIC
# MAGIC | Table | Grain | Key |
# MAGIC | --- | --- | --- |
# MAGIC | `fct_order_line` | one order line | `order_no` + `order_item_no` |
# MAGIC | `dim_date` | one calendar day (whole years covering the data) | `date_key` (yyyymmdd) |
# MAGIC | `dim_product` | one SKU | `product_key` |
# MAGIC | `dim_customer` | one customer id | `customer_key` |
# MAGIC | `dim_channel` | one order channel label | `channel_key` |
# MAGIC
# MAGIC Design choices:
# MAGIC - **Deterministic surrogate keys** (`xxhash64` of the natural key): the same customer or SKU always gets the same key, so reruns and
# MAGIC   incremental loads never renumber anything. Collisions are checked below.
# MAGIC - **Unit, currency and country live on the fact as attributes**, not as assumptions, so a second country or unit is data, not a rebuild.
# MAGIC - **The comparable-window flag is defined once, in `dim_date`.**
# MAGIC - **`customer_segment_at_order`** keeps the label as it was on the order (it changes over time); `dim_customer.dominant_segment`
# MAGIC   is the most frequent label. A slowly changing dimension (SCD type 2) is the next step if segment history becomes a requirement.
# MAGIC - **Nothing is published unless the integrity rules pass** (grain, row and value reconciliation, no orphan keys, no key collisions).
# MAGIC
# MAGIC Shape of the run: two passes over silver for all the checks (silver totals; the fact joined to the four dimensions, which also
# MAGIC counts surrogate-key collisions), the rule results written once, the five tables written at the same time with their column
# MAGIC comments in the schema, and governance that reads the catalog once and issues DDL only for what differs.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver totals
# MAGIC One aggregation gives the control totals the fact must reproduce, the date range that sizes `dim_date`, and the product
# MAGIC conformity counts (G06): if a product id has one description and one supergroup, counting distinct pairs equals counting distinct ids.

# COMMAND ----------

from pyspark.sql import Window

silver = spark.table(T_SILVER_ORDERS)
silver_stats = silver.agg(
    F.count("*").alias("rows"),
    F.sum("order_value").alias("value"),
    F.sum("quantity").alias("quantity"),
    F.min("order_date").alias("min_date"),
    F.max("order_date").alias("max_date"),
    F.countDistinct("product_id").alias("products"),
    F.countDistinct(F.struct("product_id", "product_description")).alias("product_description_pairs"),
    F.countDistinct(F.struct("product_id", "product_supergroup")).alias("product_supergroup_pairs"),
).first()
silver_rows, silver_value, silver_qty = silver_stats["rows"], silver_stats["value"], silver_stats["quantity"]
min_date, max_date = silver_stats["min_date"], silver_stats["max_date"]
product_conflicts = (
    (silver_stats["product_description_pairs"] - silver_stats["products"])
    + (silver_stats["product_supergroup_pairs"] - silver_stats["products"])
)
print(f"Silver: {silver_rows:,} rows, {min_date} to {max_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date
# MAGIC Whole calendar years covering the data. `is_in_comparable_window` marks the same month-day span (Jan 1 to the latest date in the data)
# MAGIC in every year, so a partial last year is compared with the same part of the other years.

# COMMAND ----------

cut_month, cut_day = max_date.month, max_date.day
cal_start = datetime.date(min_date.year, 1, 1)
cal_end = datetime.date(max_date.year, 12, 31)

dim_date = (
    spark.range(1)
    .select(F.explode(F.sequence(F.lit(cal_start), F.lit(cal_end), F.expr("interval 1 day"))).alias("date"))
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.expr("elt(month(date), 'Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofmonth("date").alias("day_of_month"),
        (F.expr("weekday(date)") + 1).alias("iso_day_of_week"),
    )
    .withColumn("is_weekend", F.col("iso_day_of_week") >= 6)
    .withColumn(
        "is_in_comparable_window",
        (F.col("month") < cut_month) | ((F.col("month") == cut_month) & (F.col("day_of_month") <= cut_day)),
    )
    .withColumn("is_partial_year", (F.col("year") == max_date.year) & F.lit(max_date < datetime.date(max_date.year, 12, 31)))
)

DIM_DATE_COMMENTS = {
    "date_key": "Surrogate key, the date as an integer yyyymmdd.",
    "date": "Calendar date.",
    "year": "Calendar year.",
    "quarter": "Calendar quarter (1 to 4).",
    "month": "Month number (1 to 12).",
    "month_name": "Three-letter month name in English.",
    "year_month": "Year and month as yyyy-MM.",
    "day_of_month": "Day of the month.",
    "iso_day_of_week": "ISO day of week, 1 = Monday to 7 = Sunday.",
    "is_weekend": "True for Saturday and Sunday.",
    "is_in_comparable_window": "True when the month-day falls between Jan 1 and the latest date in the data. Defined once here.",
    "is_partial_year": "True for the last year of the data when it ends before Dec 31.",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_product
# MAGIC Gas (or mixture brand) and cylinder size are parsed from the description; `is_helium` isolates the highest-value product family.

# COMMAND ----------

size_text = F.regexp_extract("product_description", r"(\d+)\s*L$", 1)

dim_product = (
    silver.groupBy("product_id")
    .agg(
        F.min("product_description").alias("product_description"),
        F.min("product_supergroup").alias("product_supergroup"),
    )
    .withColumn("product_key", F.xxhash64("product_id"))
    .withColumn("gas", F.regexp_extract("product_description", r"^(\w+)", 1))
    .withColumn("size_liters", F.when(size_text != "", size_text.cast("int")))
    .withColumn("is_helium", F.col("product_supergroup") == "Cylinder Helium")
    .select("product_key", "product_id", "product_description", "product_supergroup", "gas", "size_liters", "is_helium")
)

DIM_PRODUCT_COMMENTS = {
    "product_key": "Deterministic surrogate key (xxhash64 of product_id).",
    "product_id": "Product (SKU) identifier, the natural key.",
    "product_description": "Free-text description from the source.",
    "product_supergroup": "Product family.",
    "gas": "First word of the description: the pure gas or the mixture brand (for example Protegon).",
    "size_liters": "Cylinder size in liters, parsed from the end of the description.",
    "is_helium": "True for the Cylinder Helium family, the highest-value product family.",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customer
# MAGIC `dominant_segment` is the most frequent label (ties: most recent, then alphabetical). `radical_in_multiple_families` flags radicals that
# MAGIC appear under more than one id family (090900 and 090200), the source of possible double counting.

# COMMAND ----------

label_counts = silver.groupBy("customer_id", "customer_segment").agg(
    F.count("*").alias("lines"), F.max("order_date").alias("last_seen")
)
dominant = (
    label_counts
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.desc("lines"), F.desc("last_seen"), F.asc("customer_segment"))))
    .where("rn = 1")
    .select("customer_id", F.col("customer_segment").alias("dominant_segment"))
)

per_customer = silver.groupBy("customer_id").agg(
    F.min("customer_radical").alias("customer_radical"),
    F.min("customer_id_family").alias("customer_id_family"),
    F.countDistinct("customer_segment").alias("segment_label_count"),
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date"),
)
per_radical = per_customer.groupBy("customer_radical").agg(
    F.count("*").alias("radical_id_count"),
    F.countDistinct("customer_id_family").alias("radical_family_count"),
)

dim_customer = (
    per_customer.join(dominant, "customer_id").join(per_radical, "customer_radical")
    .withColumn("customer_key", F.xxhash64("customer_id"))
    .withColumn("radical_in_multiple_families", F.col("radical_family_count") > 1)
    .select(
        "customer_key", "customer_id", "customer_radical", "customer_id_family",
        "radical_id_count", "radical_family_count", "radical_in_multiple_families",
        "dominant_segment", "segment_label_count", "first_order_date", "last_order_date",
    )
)

DIM_CUSTOMER_COMMENTS = {
    "customer_key": "Deterministic surrogate key (xxhash64 of customer_id).",
    "customer_id": "Customer identifier, the natural key.",
    "customer_radical": "Customer id without its 6-digit family suffix.",
    "customer_id_family": "Last 6 digits of the customer id (090900 and 090200 are the main families).",
    "radical_id_count": "Number of customer ids that share this radical.",
    "radical_family_count": "Number of id families that share this radical.",
    "radical_in_multiple_families": "True when the radical appears under more than one id family (possible double counting).",
    "dominant_segment": "Most frequent segment label of the customer (ties: most recent, then alphabetical).",
    "segment_label_count": "Number of distinct segment labels the customer carried.",
    "first_order_date": "Date of the customer's first order in the data.",
    "last_order_date": "Date of the customer's last order in the data.",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_channel
# MAGIC `is_attributed` is false for `UNKNOWN SOURCE`, so any channel analysis can state how much revenue it cannot attribute.

# COMMAND ----------

dim_channel = (
    silver.groupBy("order_channel")
    .agg(F.min("order_date").alias("first_order_date"), F.max("order_date").alias("last_order_date"))
    .withColumnRenamed("order_channel", "channel_name")
    .withColumn("channel_key", F.xxhash64("channel_name"))
    .withColumn("is_attributed", F.col("channel_name") != UNKNOWN_CHANNEL)
    .select("channel_key", "channel_name", "is_attributed", "first_order_date", "last_order_date")
)

DIM_CHANNEL_COMMENTS = {
    "channel_key": "Deterministic surrogate key (xxhash64 of channel_name).",
    "channel_name": "Channel label as delivered by the source.",
    "is_attributed": "False for UNKNOWN SOURCE: revenue that cannot be assigned to a channel.",
    "first_order_date": "Date of the first order with this label.",
    "last_order_date": "Date of the last order with this label.",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## fct_order_line
# MAGIC Measures: `quantity` (cylinders) and `order_value` (EUR). `is_price_basis` marks lines that carry a price (value above zero); price and the
# MAGIC price-volume-mix bridge use only those, while cylinder and order counts keep every line.

# COMMAND ----------

fact_columns = [
    F.col("order_no"),
    F.col("order_item_no"),
    F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
    F.xxhash64("customer_id").alias("customer_key"),
    F.xxhash64("product_id").alias("product_key"),
    F.xxhash64("order_channel").alias("channel_key"),
    F.col("country"),
    F.col("customer_segment").alias("customer_segment_at_order"),
    F.col("quantity"),
    F.col("quantity_unit"),
    F.col("order_value"),
    F.col("order_value_unit").alias("currency"),
    (F.col("order_value") > 0).alias("is_price_basis"),
    F.when(F.col("order_value") > 0, (F.col("order_value") / F.col("quantity")).cast("decimal(18,4)")).alias("price_per_cylinder"),
    F.col("is_zero_value"),
    F.col("is_negative_value"),
    F.col("is_order_series_3"),
    F.col("_silver_run_id"),
]
fct_order_line = silver.select(*fact_columns)

FCT_ORDER_LINE_COMMENTS = {
    "order_no": "Order number (degenerate dimension).",
    "order_item_no": "Order line number within the order (degenerate dimension).",
    "date_key": "Foreign key to dim_date.",
    "customer_key": "Foreign key to dim_customer.",
    "product_key": "Foreign key to dim_product.",
    "channel_key": "Foreign key to dim_channel.",
    "country": "Country of the order.",
    "customer_segment_at_order": "Segment label as it appeared on the order (changes over time; see dim_customer.dominant_segment).",
    "quantity": "Quantity ordered, in quantity_unit. Measure.",
    "quantity_unit": "Unit of quantity (CYLINDERS).",
    "order_value": "Line value in currency. Measure.",
    "currency": "Currency of order_value (EUR).",
    "is_price_basis": "True when order_value > 0. Price and the price-volume-mix bridge use only these lines.",
    "price_per_cylinder": "order_value / quantity for price-basis lines, otherwise null.",
    "is_zero_value": "True when the line has value zero (no charge).",
    "is_negative_value": "True when the line has a negative value.",
    "is_order_series_3": "True for the legacy 9-digit order number series starting with 3.",
    "_silver_run_id": "Silver run that produced the source row.",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integrity rules (all fail-level)
# MAGIC Checked on the DataFrames **before** anything is published. Results go to `dq_rule_results` with `layer = 'gold'`.
# MAGIC
# MAGIC - **One pass over the fact.** The fact is joined (broadcast) to the four dimensions and aggregated once. That single result
# MAGIC   gives the grain (G01), rows and totals against silver (G02, G03), the missing keys per dimension (G04) and the window flag (G07).
# MAGIC   Violations of G04 count fact lines whose key is not in the dimension.
# MAGIC - **Key collisions in the same pass (G05).** Distinct natural keys and distinct surrogate keys are counted on the joined fact; a
# MAGIC   collision would make them differ. A duplicate key inside a dimension would duplicate fact lines in that join, so G01 and G02
# MAGIC   cover dimension uniqueness, and `date_key` (yyyymmdd) is a bijection by construction.

# COMMAND ----------

checked = (
    silver.select(*fact_columns, F.col("is_in_comparable_window").alias("_silver_window_flag"),
                  "customer_id", "product_id", "order_channel")  # natural keys, for the collision count (G05)
    .join(F.broadcast(dim_date.select("date_key", F.col("is_in_comparable_window").alias("_dim_window_flag"), F.lit(1).alias("_in_date"))), "date_key", "left")
    .join(F.broadcast(dim_customer.select("customer_key", F.lit(1).alias("_in_customer"))), "customer_key", "left")
    .join(F.broadcast(dim_product.select("product_key", F.lit(1).alias("_in_product"))), "product_key", "left")
    .join(F.broadcast(dim_channel.select("channel_key", F.lit(1).alias("_in_channel"))), "channel_key", "left")
)


def missing(flag):
    return F.sum(F.when(F.col(flag).isNull(), 1).otherwise(0))


fact_check = checked.agg(
    F.count("*").alias("rows"),
    F.countDistinct(F.struct("order_no", "order_item_no")).alias("distinct_lines"),
    F.sum("order_value").alias("value"),
    F.sum("quantity").alias("quantity"),
    missing("_in_date").alias("orphan_date"),
    missing("_in_customer").alias("orphan_customer"),
    missing("_in_product").alias("orphan_product"),
    missing("_in_channel").alias("orphan_channel"),
    F.sum(F.when(F.col("_silver_window_flag") != F.col("_dim_window_flag"), 1).otherwise(0)).alias("window_mismatch"),
    (F.countDistinct("customer_id") - F.countDistinct("customer_key")).alias("collisions_customer"),
    (F.countDistinct("product_id") - F.countDistinct("product_key")).alias("collisions_product"),
    (F.countDistinct("order_channel") - F.countDistinct("channel_key")).alias("collisions_channel"),
).first().asDict()


# G05: a surrogate-key collision would make two natural keys share one key, so distinct natural keys and distinct surrogate
# keys must agree. Counted in the pass above, instead of a separate query over the four dimensions.
key_problems = sum(abs(fact_check[c]) for c in ("collisions_customer", "collisions_product", "collisions_channel"))

rules = [
    dq_result("gold", "G01_fact_grain", "fct_order_line is unique on order_no + order_item_no (violations: rows that repeat a key)", "fail",
              fact_check["rows"] - fact_check["distinct_lines"], silver_rows),
    dq_result("gold", "G02_fact_rows", "Fact rows equal silver rows", "fail", abs(fact_check["rows"] - silver_rows), silver_rows),
    dq_result("gold", "G03_fact_totals", "Fact value and quantity equal silver value and quantity", "fail",
              int(fact_check["value"] != silver_value) + int(fact_check["quantity"] != silver_qty), silver_rows),
    dq_result("gold", "G04_no_orphan_date", "Every fact line has a date_key that exists in dim_date", "fail", fact_check["orphan_date"], silver_rows),
    dq_result("gold", "G04_no_orphan_customer", "Every fact line has a customer_key that exists in dim_customer", "fail", fact_check["orphan_customer"], silver_rows),
    dq_result("gold", "G04_no_orphan_product", "Every fact line has a product_key that exists in dim_product", "fail", fact_check["orphan_product"], silver_rows),
    dq_result("gold", "G04_no_orphan_channel", "Every fact line has a channel_key that exists in dim_channel", "fail", fact_check["orphan_channel"], silver_rows),
    dq_result("gold", "G05_keys_unique", "Surrogate keys map one to one to natural keys in every dimension (no hash collisions)", "fail",
              key_problems, silver_rows),
    dq_result("gold", "G06_product_conformed", "Each product_id has one description and one supergroup", "fail", product_conflicts, silver_rows),
    dq_result("gold", "G07_window_flag", "Comparable-window flag in silver equals dim_date for every line", "fail", fact_check["window_mismatch"], silver_rows),
]
log_and_enforce(rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish
# MAGIC Each table is rebuilt whole, with its column comments written together with the schema (`write_delta` in `00_config`).
# MAGIC The five writes run at the same time.

# COMMAND ----------

# The five tables are independent, so they are written at the same time (PARALLEL_WRITES in 00_config).
write_delta_many([
    (dim_date, T_DIM_DATE, DIM_DATE_COMMENTS),
    (dim_product, T_DIM_PRODUCT, DIM_PRODUCT_COMMENTS),
    (dim_customer, T_DIM_CUSTOMER, DIM_CUSTOMER_COMMENTS),
    (dim_channel, T_DIM_CHANNEL, DIM_CHANNEL_COMMENTS),
    (fct_order_line, T_FCT_ORDER_LINE, FCT_ORDER_LINE_COMMENTS),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance: comments and tags
# MAGIC One catalog read for the five tables; only differences are applied. Column comments were written with the schema,
# MAGIC so a normal run issues at most a table comment.

# COMMAND ----------

GOLD_TAGS = {"layer": "gold", "domain": "sales", "model": "star_schema"}

apply_governance_batch([
    (T_DIM_DATE,
     "Calendar dimension: one row per day, whole years covering the data. Holds the comparable-window flag used by every like-for-like metric.",
     DIM_DATE_COMMENTS,
     {**GOLD_TAGS, "table_type": "dimension"}),
    (T_DIM_PRODUCT,
     "Product dimension: one row per SKU, with gas and cylinder size parsed from the description.",
     DIM_PRODUCT_COMMENTS,
     {**GOLD_TAGS, "table_type": "dimension"}),
    (T_DIM_CUSTOMER,
     "Customer dimension: one row per customer id. Customers can appear under more than one id family, so check radical_in_multiple_families before counting customers.",
     DIM_CUSTOMER_COMMENTS,
     {**GOLD_TAGS, "table_type": "dimension"}),
    (T_DIM_CHANNEL,
     "Order channel dimension: one row per channel label. Labels changed in Nov 2020, so channel mix across that date is not comparable.",
     DIM_CHANNEL_COMMENTS,
     {**GOLD_TAGS, "table_type": "dimension"}),
    (T_FCT_ORDER_LINE,
     "Fact table: one row per order line. Quantity in cylinders and value in EUR (unit and currency kept as attributes). Join to the dim_* tables on the *_key columns.",
     FCT_ORDER_LINE_COMMENTS,
     {**GOLD_TAGS, "table_type": "fact"}),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access model (design for a multi-user workspace)
# MAGIC Not executed here: Free Edition has a single user. In a shared workspace, reporting and data science read gold only:
# MAGIC ```sql
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA lindedata.gold   TO `analysts`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA lindedata.silver TO `data_engineers`;
# MAGIC GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA lindedata.bronze TO `data_engineers`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Row counts of the published tables, in one query.

# COMMAND ----------

display(table_row_counts([T_DIM_DATE, T_DIM_PRODUCT, T_DIM_CUSTOMER, T_DIM_CHANNEL, T_FCT_ORDER_LINE]))
print("Star schema ready. Next: 04 · Marts.")
