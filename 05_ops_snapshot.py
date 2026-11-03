# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Ops snapshot: how the pipeline ran
# MAGIC
# MAGIC The data quality rules say whether the **data** is right. This notebook records how the **pipeline** behaved, so a dashboard can show both.
# MAGIC It writes four small tables to `lindedata.ops`:
# MAGIC
# MAGIC | Table | Grain | Source |
# MAGIC | --- | --- | --- |
# MAGIC | `job_runs` | one row per run of the job | Jobs API (`PIPELINE_JOB_ID` in `00_config`) |
# MAGIC | `job_task_runs` | one row per task of each run | Jobs API |
# MAGIC | `table_history` | one row per Delta commit of every table the pipeline writes | `DESCRIBE HISTORY`, each commit attributed to the task that was running when it happened |
# MAGIC | `table_inventory` | one row per table, as of now | `DESCRIBE DETAIL` plus a row count |
# MAGIC
# MAGIC **Safe to repeat.** Runs are upserted by id (so history survives after the API stops returning old runs); history and inventory are rebuilt from the tables themselves.
# MAGIC
# MAGIC **When to run it.** After the job has finished: run it by hand, or as its own small job. As a task of the job it would see its own run still in progress; the next snapshot corrects that row.
# MAGIC
# MAGIC Times are stored as epoch milliseconds (`start_ms`, `end_ms`) and as timestamps derived from them, so no time zone is ever guessed.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import types as T

OPS_TAGS = {"layer": "ops", "domain": "pipeline_observability"}

# Same rule as 01: one SHOW SCHEMAS, and CREATE SCHEMA only when the schema is missing.
if "ops" not in {r[0] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()}:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops COMMENT '{sql_str(SCHEMAS['ops'])}'")
    print(f"Created schema {CATALOG}.ops")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Job runs and task runs
# MAGIC The Jobs API keeps about 60 days of runs. `expand_tasks=True` returns the tasks of each run in the same call.
# MAGIC Duration is end minus start, which is what the Jobs page shows. The state comes from `state` (or from `status` when the API only fills that one).

# COMMAND ----------


def fetch_runs(job_id):
    try:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
        return [r.as_dict() for r in client.jobs.list_runs(job_id=job_id, expand_tasks=True)]
    except Exception as exc:
        raise RuntimeError(
            f"Could not read the runs of job {job_id} through the Jobs API: {str(exc)[:300]}. "
            "Check PIPELINE_JOB_ID in 00_config and that this user can view the job."
        ) from exc


def state_of(d):
    """(life cycle state, result state, termination code, message) from the old `state` or the new `status` block."""
    state, status = d.get("state") or {}, d.get("status") or {}
    term = status.get("termination_details") or {}
    return (
        state.get("life_cycle_state") or status.get("state"),
        state.get("result_state") or term.get("code"),
        term.get("code"),
        state.get("state_message") or term.get("message"),
    )


raw_runs = fetch_runs(PIPELINE_JOB_ID)
if not raw_runs:
    raise RuntimeError(f"The Jobs API returned no runs for job {PIPELINE_JOB_ID}. Run the job once, or fix PIPELINE_JOB_ID.")
print(f"{len(raw_runs)} runs returned for job {PIPELINE_JOB_ID}")
print("Fields seen on a run :", sorted(raw_runs[0].keys()))
first_tasks = next((r.get("tasks") for r in raw_runs if r.get("tasks")), [])
print("Fields seen on a task:", sorted(first_tasks[0].keys()) if first_tasks else "no tasks returned")

RUN_SCHEMA = T.StructType([
    T.StructField("job_id", T.LongType()), T.StructField("run_id", T.LongType()),
    T.StructField("run_page_url", T.StringType()), T.StructField("trigger", T.StringType()),
    T.StructField("run_type", T.StringType()), T.StructField("creator_user_name", T.StringType()),
    T.StructField("start_ms", T.LongType()), T.StructField("end_ms", T.LongType()),
    T.StructField("run_duration_ms", T.LongType()), T.StructField("queue_duration_ms", T.LongType()),
    T.StructField("life_cycle_state", T.StringType()), T.StructField("result_state", T.StringType()),
    T.StructField("termination_code", T.StringType()), T.StructField("state_message", T.StringType()),
    T.StructField("task_count", T.IntegerType()),
])
TASK_SCHEMA = T.StructType([
    T.StructField("job_id", T.LongType()), T.StructField("run_id", T.LongType()),
    T.StructField("task_run_id", T.LongType()), T.StructField("task_key", T.StringType()),
    T.StructField("notebook_path", T.StringType()),
    T.StructField("start_ms", T.LongType()), T.StructField("end_ms", T.LongType()),
    T.StructField("setup_ms", T.LongType()), T.StructField("execution_ms", T.LongType()),
    T.StructField("cleanup_ms", T.LongType()), T.StructField("queue_ms", T.LongType()),
    T.StructField("attempt_number", T.IntegerType()),
    T.StructField("life_cycle_state", T.StringType()), T.StructField("result_state", T.StringType()),
    T.StructField("termination_code", T.StringType()), T.StructField("state_message", T.StringType()),
])


def as_long(value):
    return int(value) if value is not None else None


run_rows, task_rows = [], []
for r in raw_runs:
    life, result, code, message = state_of(r)
    tasks = r.get("tasks") or []
    run_rows.append((
        as_long(r.get("job_id", PIPELINE_JOB_ID)), as_long(r["run_id"]), r.get("run_page_url"), r.get("trigger"),
        r.get("run_type"), r.get("creator_user_name"),
        as_long(r.get("start_time")), as_long(r.get("end_time")), as_long(r.get("run_duration")), as_long(r.get("queue_duration")),
        life, result, code, message, len(tasks),
    ))
    for t in tasks:
        life, result, code, message = state_of(t)
        task_rows.append((
            as_long(r.get("job_id", PIPELINE_JOB_ID)), as_long(r["run_id"]), as_long(t.get("run_id")), t.get("task_key"),
            (t.get("notebook_task") or {}).get("notebook_path"),
            as_long(t.get("start_time")), as_long(t.get("end_time")),
            as_long(t.get("setup_duration")), as_long(t.get("execution_duration")),
            as_long(t.get("cleanup_duration")), as_long(t.get("queue_duration")),
            t.get("attempt_number"), life, result, code, message,
        ))


def with_times(df):
    """Timestamps derived from the epoch milliseconds; duration only once the run has ended."""
    return (
        df.withColumn("start_ts", F.expr("timestamp_millis(start_ms)"))
        .withColumn("end_ts", F.when(F.col("end_ms") > 0, F.expr("timestamp_millis(end_ms)")))
        .withColumn("duration_s", F.when(F.col("end_ms") > 0, (F.col("end_ms") - F.col("start_ms")) / 1000.0))
        .withColumn("snapshot_ts", F.current_timestamp())
    )


runs_df = with_times(spark.createDataFrame(run_rows, RUN_SCHEMA))
tasks_df = with_times(spark.createDataFrame(task_rows, TASK_SCHEMA))

# COMMAND ----------

JOB_RUNS_COMMENTS = {
    "job_id": "Databricks job id (PIPELINE_JOB_ID in 00_config).",
    "run_id": "Run id of the job run, as shown on the Jobs page.",
    "run_page_url": "Link to the run page.",
    "trigger": "What started the run: ONE_TIME for Run now, PERIODIC for a schedule, TABLE for a table trigger.",
    "run_type": "JOB_RUN, WORKFLOW_RUN or SUBMIT_RUN.",
    "creator_user_name": "User who started the run, when the API returns it.",
    "start_ms": "Run start, epoch milliseconds (UTC).",
    "end_ms": "Run end, epoch milliseconds (UTC). 0 while the run is still going.",
    "run_duration_ms": "Duration as reported by the API, in milliseconds. duration_s is the one to use.",
    "queue_duration_ms": "Time spent queued, in milliseconds, when the API returns it.",
    "life_cycle_state": "Where the run is: PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED or INTERNAL_ERROR.",
    "result_state": "Outcome once finished: SUCCESS, FAILED, TIMEDOUT or CANCELED. Null while running.",
    "termination_code": "Why the run ended, from the API status block.",
    "state_message": "Message attached to the final state, usually the error text for a failed run.",
    "task_count": "Number of tasks in the run.",
    "start_ts": "Run start as a timestamp.",
    "end_ts": "Run end as a timestamp. Null while running.",
    "duration_s": "End minus start, in seconds: the duration the Jobs page shows. Null while running.",
    "snapshot_ts": "When this row was last refreshed by 05_ops_snapshot.",
}
TASK_RUNS_COMMENTS = {
    "job_id": "Databricks job id.",
    "run_id": "Run id of the job run this task belongs to (joins to job_runs.run_id).",
    "task_run_id": "Run id of the task itself.",
    "task_key": "Task name in the job, for example Linde_Silver.",
    "notebook_path": "Notebook the task runs.",
    "start_ms": "Task start, epoch milliseconds (UTC).",
    "end_ms": "Task end, epoch milliseconds (UTC). 0 while the task is still going.",
    "setup_ms": "Compute setup time in milliseconds, when the API returns it (zero on serverless multi-task runs).",
    "execution_ms": "Execution time in milliseconds, when the API returns it.",
    "cleanup_ms": "Cleanup time in milliseconds, when the API returns it.",
    "queue_ms": "Time queued, in milliseconds, when the API returns it.",
    "attempt_number": "0 for the first attempt, higher for retries.",
    "life_cycle_state": "Where the task is: PENDING, RUNNING, TERMINATED, SKIPPED or INTERNAL_ERROR.",
    "result_state": "Outcome once finished: SUCCESS, FAILED, TIMEDOUT or CANCELED. Null while running.",
    "termination_code": "Why the task ended, from the API status block.",
    "state_message": "Message attached to the final state, usually the error text for a failed task.",
    "start_ts": "Task start as a timestamp.",
    "end_ts": "Task end as a timestamp. Null while running.",
    "duration_s": "End minus start, in seconds. Null while running.",
    "snapshot_ts": "When this row was last refreshed by 05_ops_snapshot.",
}


def upsert(df, table, key, column_comments):
    """Create the table on the first run, then MERGE by key: refresh what the API returns, keep what it no longer returns."""
    if spark.catalog.tableExists(table):
        df.createOrReplaceTempView("_ops_source")
        spark.sql(
            f"MERGE INTO {table} AS t USING _ops_source AS s ON t.{key} = s.{key} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
        print(f"Merged {df.count()} rows into {table}")
    else:
        write_delta(df, table, column_comments)


upsert(runs_df, T_OPS_JOB_RUNS, "run_id", JOB_RUNS_COMMENTS)
upsert(tasks_df, T_OPS_TASK_RUNS, "task_run_id", TASK_RUNS_COMMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Table history: what every run wrote
# MAGIC Every write to a Delta table is a commit with its row count. Each commit is attributed to the task that was running when it happened
# MAGIC (commit time inside the task's start and end, with 2 seconds of tolerance). Commits outside any task, such as a notebook run by hand, stay unattributed.
# MAGIC The sum of `rows_written` of a run is the "Rows written" figure on the run page.

# COMMAND ----------

frames = []
for table in TRACKED_TABLES:
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table}")
    except Exception as exc:
        print(f"No history for {table}: {str(exc)[:140]}")
        continue
    frames.append(history.select(
        F.lit(table.split(".")[-1]).alias("table_name"),
        F.lit(table.split(".")[1]).alias("layer"),
        F.col("version").cast("bigint").alias("version"),
        F.col("timestamp").alias("commit_ts"),
        F.col("operation"),
        F.col("operationMetrics")["numOutputRows"].cast("bigint").alias("rows_written"),
        F.col("operationMetrics")["numFiles"].cast("bigint").alias("files_written"),
        F.col("operationMetrics")["numOutputBytes"].cast("bigint").alias("bytes_written"),
        F.col("isBlindAppend").alias("is_blind_append"),
        F.col("userName").alias("user_name"),
        F.to_json(F.col("job")).alias("history_job_json"),
    ))
if not frames:
    raise RuntimeError("No table history could be read. Run 01 to 04 first.")

reduce(DataFrame.unionByName, frames).createOrReplaceTempView("_ops_history")
spark.table(T_OPS_TASK_RUNS).createOrReplaceTempView("_ops_tasks")
history_df = spark.sql("""
    SELECT * FROM (
        SELECT h.*, t.run_id AS job_run_id, t.task_key,
               ROW_NUMBER() OVER (PARTITION BY h.table_name, h.version ORDER BY t.start_ms DESC) AS rn
        FROM _ops_history h
        LEFT JOIN _ops_tasks t
          ON h.commit_ts >= timestamp_millis(t.start_ms)
         AND h.commit_ts <= timestamp_millis(COALESCE(NULLIF(t.end_ms, 0), unix_millis(current_timestamp())) + 2000)
    ) WHERE rn = 1
""").drop("rn")

TABLE_HISTORY_COMMENTS = {
    "table_name": "Table the commit belongs to (without catalog and schema).",
    "layer": "Schema of the table: bronze, silver or gold.",
    "version": "Delta table version created by the commit.",
    "commit_ts": "When the commit happened.",
    "operation": "Delta operation: WRITE, CREATE OR REPLACE TABLE AS SELECT, MERGE, SET TBLPROPERTIES and so on.",
    "rows_written": "Rows written by the commit (numOutputRows). Null for operations that write no rows, such as comment and tag changes.",
    "files_written": "Files written by the commit (numFiles).",
    "bytes_written": "Bytes written by the commit (numOutputBytes).",
    "is_blind_append": "True when the commit only appended rows without reading the table.",
    "user_name": "User that ran the commit.",
    "history_job_json": "The job block of the Delta history, as Databricks recorded it. Null for commits not made by a job.",
    "job_run_id": "Job run the commit is attributed to (joins to job_runs.run_id). Null when no task was running at that time.",
    "task_key": "Task the commit is attributed to. Null when no task was running at that time.",
}
write_delta(history_df, T_OPS_TABLE_HISTORY, TABLE_HISTORY_COMMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Table inventory: what exists now
# MAGIC Size and files from `DESCRIBE DETAIL`, row counts in one query.

# COMMAND ----------

details = []
for table in TRACKED_TABLES:
    try:
        d = spark.sql(f"DESCRIBE DETAIL {table}")
    except Exception as exc:
        print(f"No detail for {table}: {str(exc)[:140]}")
        continue
    details.append(d.select(
        F.lit(table.split(".")[-1]).alias("table_name"),
        F.lit(table.split(".")[1]).alias("layer"),
        F.col("numFiles").cast("bigint").alias("num_files"),
        F.col("sizeInBytes").cast("bigint").alias("size_bytes"),
        F.col("createdAt").alias("created_at"),
        F.col("lastModified").alias("last_modified"),
    ))
counts = spark.createDataFrame(table_row_counts(TRACKED_TABLES)).withColumnRenamed("rows", "row_count")
name = F.col("table_name")
inventory_df = (
    reduce(DataFrame.unionByName, details)
    .join(counts, "table_name", "left")
    .withColumn("role", (
        F.when(name.startswith("dim_"), "dimension").when(name.startswith("fct_"), "fact")
        .when(name.startswith("mart_"), "mart").when(name.startswith("ref_") | name.startswith("metric_"), "reference")
        .when(name == "bronze_orders", "raw copy").when(name == "silver_orders", "clean")
        .when(name == "silver_orders_quarantine", "quarantine").when(name == "dq_rule_results", "audit")
        .otherwise("other")))
    .withColumn("snapshot_ts", F.current_timestamp())
    .select("table_name", "layer", "role", "row_count", "num_files", "size_bytes", "created_at", "last_modified", "snapshot_ts")
)
TABLE_INVENTORY_COMMENTS = {
    "table_name": "Table name without catalog and schema.",
    "layer": "Schema of the table: bronze, silver or gold.",
    "role": "What the table is for: raw copy, clean, quarantine, audit, dimension, fact, mart or reference.",
    "row_count": "Rows in the table at snapshot time.",
    "num_files": "Data files in the current version of the table.",
    "size_bytes": "Size of the current version of the table, in bytes.",
    "created_at": "When the table was created.",
    "last_modified": "When the table was last changed.",
    "snapshot_ts": "When this row was captured.",
}
write_delta(inventory_df, T_OPS_TABLE_INVENTORY, TABLE_INVENTORY_COMMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Governance

# COMMAND ----------

apply_governance(T_OPS_JOB_RUNS, "One row per run of the pipeline job, from the Jobs API. Refreshed by 05_ops_snapshot; older runs are kept.", JOB_RUNS_COMMENTS, OPS_TAGS)
apply_governance(T_OPS_TASK_RUNS, "One row per task of each pipeline job run, from the Jobs API. Refreshed by 05_ops_snapshot; older runs are kept.", TASK_RUNS_COMMENTS, OPS_TAGS)
apply_governance(T_OPS_TABLE_HISTORY, "Every Delta commit of the tables the pipeline writes, attributed to the job task that was running. Rebuilt by 05_ops_snapshot.", TABLE_HISTORY_COMMENTS, OPS_TAGS)
apply_governance(T_OPS_TABLE_INVENTORY, "Rows, files and size of every table the pipeline writes, as of the last snapshot. Rebuilt by 05_ops_snapshot.", TABLE_INVENTORY_COMMENTS, OPS_TAGS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Compare these tables with the Jobs page: run duration, task durations and "Rows written" should agree.

# COMMAND ----------

display(spark.sql(f"""
    SELECT run_id, result_state, ROUND(duration_s, 1) AS duration_s, task_count, start_ts
    FROM {T_OPS_JOB_RUNS} WHERE job_id = {PIPELINE_JOB_ID} ORDER BY start_ts
"""))
display(spark.sql(f"""
    SELECT run_id, task_key, result_state, ROUND(duration_s, 1) AS duration_s
    FROM {T_OPS_TASK_RUNS} WHERE job_id = {PIPELINE_JOB_ID} ORDER BY run_id, start_ts
"""))
display(spark.sql(f"""
    SELECT job_run_id, SUM(rows_written) AS rows_written, COUNT(*) AS commits
    FROM {T_OPS_TABLE_HISTORY} WHERE job_run_id IS NOT NULL GROUP BY job_run_id ORDER BY job_run_id
"""))
unattributed = spark.sql(f"SELECT COUNT(*) AS n FROM {T_OPS_TABLE_HISTORY} WHERE job_run_id IS NULL").first()["n"]
print(f"Commits outside any job task (notebooks run by hand, or runs older than the API window): {unattributed}")
print(f"Ops snapshot ready in {CATALOG}.ops. Next: the data quality and pipeline dashboard.")
