# LindeChallenge

Mini-lakehouse for the Linde EMEA Data Engineer exercise: 224,670 order lines (one country, 2017-01-02 to 2022-11-21),
built on Databricks Free Edition (serverless, Delta tables, Unity Catalog). Reporting reads the gold layer only.

## Layers

| Layer | Schema | What it holds | Rule |
| --- | --- | --- | --- |
| Landing | `lindedata.raw` | The source file in the volume `lindechallenge`, as delivered | Never modified |
| Bronze | `lindedata.bronze` | `bronze_orders`: raw copy in Delta plus audit columns | Append only; reconciled to control totals taken from the file |
| Silver | `lindedata.silver` | `silver_orders`, `silver_orders_quarantine`, `dq_rule_results` | Typed, flagged, validated; rebuilt from bronze on every run |
| Gold | `lindedata.gold` | Star schema (`dim_*`, `fct_order_line`) and one mart per report screen | The only layer reporting reads; the star schema is published only if its integrity rules pass |

## Notebooks

Run in order. Every notebook is safe to repeat.

| Notebook | Purpose |
| --- | --- |
| `00_config` | Names, control totals and helpers shared by all notebooks (`%run ./00_config`) |
| `01_setup_bronze` | Creates the schemas, loads the file into `bronze_orders`, reconciles it, adds comments and tags |
| `02_silver` | Types the data, applies the data quality rules, splits clean rows from quarantine, reconciles layers |
| `03_gold_star` | Builds the star schema, checks its integrity (G01 to G07) and publishes only if every check passes |
| `04_gold_marts` | Builds the marts and the metric catalog from the star schema, checks them against each other (M01 to M06) and against the figures in the report |

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
- **Publish only if it is right.** The star schema checks run on the DataFrames before any table is written, so a failed run leaves the previous version in place. Mart checks and the acceptance test run right after the marts are built and stop the notebook, so a bad number never reaches the export step unnoticed.
- **Repeat runs are cheap.** A file already in bronze is not opened again, silver needs one aggregation pass plus the two
  writes, rule results go in one commit, and column comments are written with the schema, so governance issues no DDL
  unless something changed in the code or the catalog. Gold follows the same rules: the star schema checks need three passes
  over silver (totals, the fact joined to the four dimensions, the dimension keys), the mart checks and the acceptance test read
  the marts in a handful of queries, and divisions are guarded with `NULLIF` because serverless runs in ANSI mode.

## Running as a job

Four serverless tasks in order: `01_setup_bronze`, `02_silver`, `03_gold_star`, `04_gold_marts`. Nothing is installed at run time on the normal path: the native
Excel reader (Databricks Runtime 17.1 or above) needs no extra package. `openpyxl` is only needed by the pandas fallback
and is installed on demand when that path runs; to avoid even that, add `openpyxl` to the job's environment
dependencies. `EXCEL_READER` in `00_config` forces one reader when the automatic choice is not wanted.

## Not in this repository

The source file (`INTERVIEW_ORDERS.xlsx`) is not committed. It stays in the landing volume.
