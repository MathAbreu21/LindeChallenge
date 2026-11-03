# LindeChallenge

Mini-lakehouse for the Linde EMEA Data Engineer exercise: 224,670 order lines (one country, 2017-01-02 to 2022-11-21),
built on Databricks Free Edition (serverless, Delta tables, Unity Catalog). The sales report reads the gold layer only.

## Layers

| Layer | Schema | What it holds | Rule |
| --- | --- | --- | --- |
| Landing | `lindedata.raw` | The source file in the volume `lindechallenge`, as delivered | Never modified |
| Bronze | `lindedata.bronze` | `bronze_orders`: raw copy in Delta plus audit columns | Append only; reconciled to control totals taken from the file |
| Silver | `lindedata.silver` | `silver_orders`, `silver_orders_quarantine`, `dq_rule_results` | Typed, flagged, validated; rebuilt from bronze on every run |
| Gold | `lindedata.gold` | Star schema (`dim_*`, `fct_order_line`) and one mart per report screen | The only layer the sales report reads; the star schema is published only if its integrity rules pass |
| Ops | `lindedata.ops` | `job_runs`, `job_task_runs`, `table_history`, `table_inventory`: how the pipeline ran | Filled by `05_ops_snapshot`, read only by the data quality and pipeline dashboard |

## Notebooks

Run in order. Every notebook is safe to repeat.

| Notebook | Purpose |
| --- | --- |
| `00_config` | Names, control totals and helpers shared by all notebooks (`%run ./00_config`) |
| `01_setup_bronze` | Creates the schemas, loads the file into `bronze_orders`, reconciles it, adds comments and tags |
| `02_silver` | Types the data, applies the data quality rules, splits clean rows from quarantine, reconciles layers |
| `03_gold_star` | Builds the star schema, checks its integrity (G01 to G07) and publishes only if every check passes |
| `04_gold_marts` | Builds the marts and the metric catalog from the star schema, checks them against each other (M01 to M06) and against the figures in the report |
| `05_ops_snapshot` | Records how the pipeline ran, for the dashboard: job runs and task runs from the Jobs API, every Delta commit of the pipeline tables attributed to the task that made it, and a table inventory. Run it after the job; repeat runs update in place |

## Star schema

Grain of the fact: **one order line** (`order_no` + `order_item_no`).

| Table | Key | Notes |
| --- | --- | --- |
| `fct_order_line` | `order_no`, `order_item_no` | Keys to the four dimensions; `quantity`, `order_value`, `price_per_cylinder`; country, unit and currency kept as attributes; `customer_segment_at_order`; flags `is_price_basis`, `is_zero_value`, `is_negative_value`, `is_order_series_3` |
| `dim_date` | `date_key` | Whole calendar years; year, quarter, month, ISO weekday; `is_in_comparable_window` is defined here, once |
| `dim_product` | `product_key` | Gas, cylinder size and `is_helium` derived from the description |
| `dim_customer` | `customer_key` | Dominant segment, radical and id family, first and last order date; Type 2 history is a future step |
| `dim_channel` | `channel_key` | `is_attributed` is false for `UNKNOWN SOURCE` |

Surrogate keys are deterministic (`xxhash64` of the business key), so a rebuild never changes a key and no lookup table is needed.

## Marts

| Mart | Grain | Feeds |
| --- | --- | --- |
| `mart_kpi_ytd` | year | Opening and trend: cylinders, revenue, orders, customers, price, index (first year = 100) |
| `mart_pvm_sku_year` | base year, current year, SKU | Price, volume and mix effects by SKU |
| `mart_pvm_bridge` | base year, current year | The waterfall; closes exactly on the revenue change |
| `mart_growth_anatomy` | base year, current year | Top-gaining customer, helium to others, rest of the portfolio |
| `mart_customer_year` | year, customer | Status against the previous year, 24-month new-customer flag |
| `mart_customer_bridge` | base year, current year | Cylinder bridge (lost, new, win-back, retained up and down) and retention |
| `mart_customer_cadence` | as-of date, customer | Overdue customers, with a 12-month backtest |
| `mart_channel_month` | month, channel | Channel mix, zeros included |
| `ref_channel_events` | event | Channel events to mark on the chart (observed or hypothesis) |
| `metric_catalog` | metric | Definition, source, scope and caveat of every metric |

**Comparable window.** Every year is cut at the same month and day as the latest date in the data (Jan 1 to Nov 21), so a partial 2022 is compared with the same part of the other years. Price and the price-volume-mix bridge use only lines with value above zero.

## Data quality rules (silver)

Each rule is code with an explicit consequence, and every run logs one row per rule in `dq_rule_results`.

| Rule | Action | Checks |
| --- | --- | --- |
| R01 | fail | `ORDER_NO` + `ORDER_ITEM_NO` identifies one line |
| R02 | quarantine | Order date is a real date inside the delivery window |
| R03 | quarantine | Quantity is greater than zero |
| R04 | flag | Order value is greater than zero (zero and negative values are flagged, not removed) |
| R05 | fail | Unit is CYLINDERS and currency is EUR; other units are never converted silently |
| R06 | alert | Order channel is known (`UNKNOWN SOURCE` is reported) |
| R07 | alert | Customer segment is a real label, not the default placeholder |
| R08 | fail | Bronze rows and value equal silver plus quarantine |

## Data quality rules (gold)

Logged in the same table with `layer = 'gold'`. All are fail-level.

| Rule | Checks |
| --- | --- |
| G01 to G03 | Fact grain is one order line; fact rows, value and quantity equal silver |
| G04 | Every fact line has a key that exists in each of the four dimensions |
| G05 | Surrogate keys map one to one to natural keys (no hash collisions) |
| G06 | Each product id has one description and one supergroup |
| G07 | The comparable-window flag in silver equals `dim_date` for every line |
| M01 to M06 | PVM and cylinder bridges close; the three growth bars add up; marts agree with the fact and with each other; channel shares add up to 100 |

The last cell of `04_gold_marts` is an **acceptance test**: the gold layer must reproduce the figures quoted in the report, which were computed independently with pandas. Set `ENFORCE_REPORT_FIGURES = False` when the source data changes on purpose.

## Design choices

- **Control totals before the pipeline.** Row count, order count, cylinders, value and date range come from the source file
  with an independent tool, and bronze must match them exactly.
- **Nothing disappears.** Rows that break a quarantine rule go to a table with the rule id; layers reconcile in rows and value.
- **Governance in the catalog.** Every table and column has a comment (the data dictionary is generated from them) and tags.
- **Scale by design.** Country, unit and currency are attributes, not assumptions, so a second country is data, not a rebuild.
- **One definition per metric.** The comparable window lives in `dim_date`, every metric is documented in `metric_catalog`, and the reporting tool only sums and divides.
- **Publish only if it is right.** The star schema checks run on the DataFrames before any table is written, so a failed run leaves the previous version in place. Mart checks and the acceptance test run right after the marts are built and stop the notebook, so a bad number never reaches a dashboard unnoticed.
- **Repeat runs are cheap.** A file already in bronze is not opened again, silver needs one aggregation pass plus the two
  writes, rule results go in one commit, and column comments are written with the schema, so governance issues no DDL
  unless something changed in the code or the catalog. Gold follows the same rules: the star schema checks need three passes
  over silver (totals, the fact joined to the four dimensions, the dimension keys), the mart checks and the acceptance test read
  the marts in a handful of queries, and divisions are guarded with `NULLIF` because serverless runs in ANSI mode.

## Running as a job

The job **Linde ETL** (ID 231937700218123) has four serverless tasks in a line: `01_setup_bronze`, `02_silver`, `03_gold_star`, `04_gold_marts`. `05_ops_snapshot` runs on demand, after the job.
Nothing is installed at run time on the normal path: the native
Excel reader (Databricks Runtime 17.1 or above) needs no extra package. `openpyxl` is needed only by the pandas fallback in 01,
and is installed on demand when that path runs; to avoid even that, add `openpyxl` to the job's environment
dependencies. `EXCEL_READER` in `00_config` forces one reader when the automatic choice is not wanted.

What the job has today, read from the Jobs page on 2 Nov 2026: started by hand (no schedule or trigger), no notifications, no duration warning, no parameters, notebooks in a workspace folder with Git not configured. Fail-level data quality rules stop a run: silver and star schema rules before their tables are written, mart rules and the acceptance test right after the marts are built. Next steps, in the order I would take them: a trigger, a failure notification and duration warning, a Git folder with the job as code, parameters instead of constants in `00_config`, and a second job that runs `05_ops_snapshot` when this one finishes.

## Pipeline observability

`05_ops_snapshot` writes four tables to `lindedata.ops` (every table and column has a comment):

| Table | Grain | Source |
| --- | --- | --- |
| `job_runs` | one row per run of the job | Jobs API, `jobs.list_runs(job_id, expand_tasks=True)` |
| `job_task_runs` | one row per task of each run | Jobs API |
| `table_history` | one row per Delta commit of every table the pipeline writes | `DESCRIBE HISTORY`; a commit is attributed to the task whose time window contains it (end + 2 s) |
| `table_inventory` | one row per table, as of now | `DESCRIBE DETAIL` plus a row count |

Things to know. Runs are upserted by id, so they stay after the Jobs API stops returning them (it keeps about 60 days); the history and the inventory are rebuilt from the Delta tables, which keep their log for the retention period of each table. A run still in progress is stored without an end time and corrected by the next snapshot, which is why the notebook is not a task of the job. The attribution is by time window, so a manual run of a notebook that happens during a job run would be attributed to that run. The "rows written" of a run is the sum of `numOutputRows` over its commits; it should match the figure on the Jobs page (224,678 for a run of Bronze and Silver, 487,486 for a run of all four tasks: both numbers were reproduced from the table row counts). The tests ran on a local Spark with the Jobs API and the Delta history simulated from the real runs, so the field names of the real API and history are the one thing to confirm on the first real run (the notebook prints the fields it received).

## Reporting

Looker Studio reads the gold tables through its Databricks connector (server hostname, a SQL warehouse, and a personal access token entered as the password with user `token`). The connector takes **one SQL statement per data source**, so `reporting/looker/` holds the seven the report uses, one file each, with nothing in the file but the statement to paste (see `reporting/README.md`). They only select, reshape and rename: the marts are already at report grain and no business logic lives in the BI tool. The reshaping is the stacking of waterfall steps into rows, a product lookup, and customer ids cast to text.

The native Databricks AI/BI dashboard reads the same marts through the datasets in `reporting/databricks/`; the Genie prompt that builds its pages is in `reporting/databricks/genie_prompt.md`.

A second dashboard, **Linde ETL - Data quality and pipeline health**, reads `dq_rule_results`, the layers (row and value reconciliation, flagged lines by year, freshness), `information_schema` (comment coverage) and `lindedata.ops`. Its 13 datasets are in `reporting/databricks_dq/` and the Genie prompt that builds its six pages is `reporting/databricks_dq/genie_prompt_dq.md`.

## Not in this repository

The source file (`INTERVIEW_ORDERS.xlsx`) is not committed. It stays in the landing volume.
