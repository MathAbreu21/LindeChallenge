# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Shared configuration
# MAGIC
# MAGIC Names, control totals and small helpers used by every notebook (`%run ./00_config`).
# MAGIC One place to change a catalog, a table name or a convention, so layers stay consistent.

# COMMAND ----------

import datetime
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pandas as pd
from pyspark.sql import functions as F

# --- Where things live ---------------------------------------------------------
CATALOG = "lindedata"
RAW_SCHEMA = "raw"                 # landing zone: source files exactly as delivered
LANDING_VOLUME = "lindechallenge"  # volume inside the raw schema
SOURCE_FILE = ""                   # leave empty to auto-detect the single .xlsx/.csv in the volume
EXCEL_READER = "auto"              # "auto": native reader, pandas if it is not usable · "native" · "pandas"
PARALLEL_WRITES = 4                # independent tables written at the same time in one wave (1 = one after the other)

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

# Gold: star schema
T_DIM_DATE = f"{CATALOG}.gold.dim_date"
T_DIM_PRODUCT = f"{CATALOG}.gold.dim_product"
T_DIM_CUSTOMER = f"{CATALOG}.gold.dim_customer"
T_DIM_CHANNEL = f"{CATALOG}.gold.dim_channel"
T_FCT_ORDER_LINE = f"{CATALOG}.gold.fct_order_line"

# Gold: marts (one per report screen) and reference tables
T_MART_KPI_YTD = f"{CATALOG}.gold.mart_kpi_ytd"
T_MART_PVM_SKU = f"{CATALOG}.gold.mart_pvm_sku_year"
T_MART_PVM_BRIDGE = f"{CATALOG}.gold.mart_pvm_bridge"
T_MART_GROWTH_ANATOMY = f"{CATALOG}.gold.mart_growth_anatomy"
T_MART_CUSTOMER_YEAR = f"{CATALOG}.gold.mart_customer_year"
T_MART_CUSTOMER_BRIDGE = f"{CATALOG}.gold.mart_customer_bridge"
T_MART_CUSTOMER_CADENCE = f"{CATALOG}.gold.mart_customer_cadence"
T_MART_CHANNEL_MONTH = f"{CATALOG}.gold.mart_channel_month"
T_REF_CHANNEL_EVENTS = f"{CATALOG}.gold.ref_channel_events"
T_METRIC_CATALOG = f"{CATALOG}.gold.metric_catalog"
V_CADENCE_BACKTEST = f"{CATALOG}.gold.v_cadence_backtest"

RUN_ID = uuid.uuid4().hex[:12]

UNKNOWN_CHANNEL = "UNKNOWN SOURCE"  # channel label meaning "no channel captured"

# One audit trail for every layer: one row per rule per run.
RESULT_SCHEMA = (
    "run_id string, run_ts timestamp, layer string, rule_id string, rule_description string, action string, "
    "violations bigint, violation_value decimal(18,2), total_rows bigint, status string"
)
STATUS_IF_VIOLATED = {"fail": "FAIL", "quarantine": "QUARANTINED", "flag": "FLAGGED", "alert": "ALERT"}

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


def catalog_governance(tables):
    """Table comment, column comments and tags of several tables, with one information_schema query per schema.
    Returns {table: {"comment": str, "columns": {column: comment}, "tags": {key: value}}}. A table the catalog does
    not list is absent, and the caller falls back to DESCRIBE for it."""
    by_schema = {}
    for table in tables:
        catalog, schema, name = table.split(".")
        by_schema.setdefault((catalog, schema), []).append(name)
    state = {}
    for (catalog, schema), names in by_schema.items():
        name_list = ", ".join(f"'{sql_str(n)}'" for n in names)
        rows = spark.sql(f"""
            SELECT table_name, 'table' AS kind, CAST(NULL AS STRING) AS name, comment AS value
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{sql_str(schema)}' AND table_name IN ({name_list})
            UNION ALL
            SELECT table_name, 'column' AS kind, column_name AS name, comment AS value
            FROM {catalog}.information_schema.columns
            WHERE table_schema = '{sql_str(schema)}' AND table_name IN ({name_list})
            UNION ALL
            SELECT table_name, 'tag' AS kind, tag_name AS name, tag_value AS value
            FROM {catalog}.information_schema.table_tags
            WHERE schema_name = '{sql_str(schema)}' AND table_name IN ({name_list})
        """).collect()
        for r in rows:
            entry = state.setdefault(f"{catalog}.{schema}.{r['table_name']}", {"comment": "", "columns": {}, "tags": {}})
            if r["kind"] == "table":
                entry["comment"] = r["value"] or ""
            elif r["kind"] == "column":
                entry["columns"][r["name"]] = r["value"] or ""
            else:
                entry["tags"][r["name"]] = r["value"] or ""
    return state


def apply_governance_batch(specs):
    """Table comments, column comments and tags for several tables: one catalog read for all of them, then DDL only
    for what differs. Tables written through with_comments() normally need nothing, so a notebook with ten gold tables
    costs one query here instead of twenty reads (and instead of one ALTER TABLE per column before that).
    specs: list of (table, table_comment, column_comments, tags). Tags are best effort on Free Edition."""
    try:
        current = catalog_governance([spec[0] for spec in specs])
    except Exception as exc:
        print(f"information_schema not usable ({str(exc)[:120]}); reading one table at a time.")
        current = {}
    for table, table_comment, column_comments, tags in specs:
        state = current.get(table)
        if state is not None:
            cur_comment, cur_columns, cur_tags = state["comment"], state["columns"], state["tags"]
        else:
            cur_comment, cur_columns = describe_governance(table)
            try:
                cur_tags = current_tags(table)
            except Exception:
                cur_tags = None  # unknown: set them below
        changed = []
        if cur_comment != table_comment:
            spark.sql(f"COMMENT ON TABLE {table} IS '{sql_str(table_comment)}'")
            changed.append("table comment")
        for col, text in column_comments.items():
            if col in cur_columns and cur_columns[col] != text:
                spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{col}` COMMENT '{sql_str(text)}'")
                changed.append(col)
        try:
            if cur_tags is None or any(cur_tags.get(k) != v for k, v in tags.items()):
                tag_sql = ", ".join(f"'{sql_str(k)}' = '{sql_str(v)}'" for k, v in tags.items())
                spark.sql(f"ALTER TABLE {table} SET TAGS ({tag_sql})")
                changed.append("tags")
        except Exception as exc:
            print(f"Tags not applied to {table} (to be validated on Free Edition): {str(exc)[:160]}")
        print(f"Governance on {table}: " + (", ".join(changed) if changed else "up to date, no DDL issued"))


def apply_governance(table, table_comment, column_comments, tags):
    """Governance for one table. Several tables in one notebook: apply_governance_batch, one catalog read for all."""
    apply_governance_batch([(table, table_comment, column_comments, tags)])


def dq_result(layer, rule_id, description, action, violations, total_rows, value=0):
    """One row for the data quality audit table."""
    value = Decimal(str(value or 0)).quantize(Decimal("0.01"))
    status = STATUS_IF_VIOLATED[action] if violations > 0 else "PASS"
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    return (RUN_ID, now, layer, rule_id, description, action, int(violations), value, int(total_rows), status)


def log_and_enforce(rows):
    """Append rule results to the audit table in one commit, then stop the run if any fail-level rule was violated.
    The audit table is created (with its column comments) by 02_silver."""
    df = spark.createDataFrame(rows, RESULT_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(T_DQ_RESULTS)
    display(df.select("rule_id", "action", "violations", "total_rows", "status"))
    failed = [r for r in rows if r[9] == "FAIL"]
    if failed:
        raise RuntimeError("Fail-level rules triggered, nothing was published: "
                           + ", ".join(f"{r[3]} ({r[6]:,})" for r in failed))
    print("All fail-level rules passed.")


def write_delta(df, table, column_comments=None):
    """Rebuild a derived table from a DataFrame (silver and gold are derived, so they are rewritten whole).
    Column comments travel with the schema (see with_comments); columns without a comment are reported."""
    if column_comments is not None:
        missing = [c for c in df.columns if c not in column_comments]
        if missing:
            print(f"  columns without a comment in {table}: {', '.join(missing)}")
        df = with_comments(df, column_comments)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"Wrote {table}")


def run_parallel(tasks, workers=None):
    """Run independent tasks (zero-argument callables) at the same time, up to PARALLEL_WRITES at once.
    Spark Connect accepts queries from several threads and the tasks touch different tables, so a wave of writes
    takes about as long as its slowest write instead of the sum. Waits for every task, then re-raises the first
    error, so a failed wave never leaves a thread behind."""
    tasks = list(tasks)
    workers = min(workers or PARALLEL_WRITES, len(tasks))
    if workers <= 1:
        for task in tasks:
            task()
        return
    def attached(task):
        # Classic PySpark maps a Python thread to a JVM thread with no active SparkSession, and Delta then drops
        # table-level metadata such as the comment on a replace. Attach the session first. Spark Connect (serverless)
        # has no JVM side in the client, so this is a no-op there.
        def run():
            jvm = getattr(spark, "_jvm", None)
            if jvm is not None:
                jvm.SparkSession.setActiveSession(spark._jsparkSession)
            return task()
        return run

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(attached(task)) for task in tasks]
        errors = [f.exception() for f in futures if f.exception() is not None]
    if errors:
        for extra in errors[1:]:
            print(f"Also failed: {type(extra).__name__}: {str(extra)[:200]}")
        raise errors[0]


def write_delta_many(jobs):
    """One wave of independent table writes. jobs: list of (DataFrame or SQL text, table, column_comments)."""
    def task(source, table, comments):
        return lambda: write_delta(spark.sql(source) if isinstance(source, str) else source, table, comments)
    run_parallel([task(*job) for job in jobs])


def table_row_counts(tables):
    """Row count of several tables in one query (one job instead of one per table)."""
    sql = " UNION ALL ".join(f"SELECT '{t.split('.')[-1]}' AS table_name, COUNT(*) AS `rows` FROM {t}" for t in tables)
    return pd.DataFrame([r.asDict() for r in spark.sql(sql).collect()])


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
