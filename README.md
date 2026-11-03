# LindeChallenge

Mini-lakehouse for the Linde EMEA Data Engineer exercise: 224,670 order lines (one country, 2017-01-02 to 2022-11-21),
built on Databricks Free Edition (serverless, Delta tables, Unity Catalog). Reporting reads the gold layer only.

## Layers

| Layer | Schema | What it holds | Rule |
| --- | --- | --- | --- |
| Landing | `lindedata.raw` | The source file in the volume `lindechallenge`, as delivered | Never modified |
| Bronze | `lindedata.bronze` | `bronze_orders`: raw copy in Delta plus audit columns | Append only; reconciled to control totals taken from the file |
| Silver | `lindedata.silver` | `silver_orders`, `silver_orders_quarantine`, `dq_rule_results` | Typed, flagged, validated; rebuilt from bronze on every run |
| Gold | `lindedata.gold` | Star schema and marts (next) | The only layer reporting reads |

## Notebooks

Run in order. Every notebook is safe to repeat.

| Notebook | Purpose |
| --- | --- |
| `00_config` | Names, control totals and helpers shared by all notebooks (`%run ./00_config`) |
| `01_setup_bronze` | Creates the schemas, loads the file into `bronze_orders`, reconciles it, adds comments and tags |
| `02_silver` | Types the data, applies the data quality rules, splits clean rows from quarantine, reconciles layers |

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

## Design choices

- **Control totals before the pipeline.** Row count, order count, cylinders, value and date range come from the source file
  with an independent tool, and bronze must match them exactly.
- **Nothing disappears.** Rows that break a quarantine rule go to a table with the rule id; layers reconcile in rows and value.
- **Governance in the catalog.** Every table and column has a comment (the data dictionary is generated from them) and tags.
- **Scale by design.** Country, unit and currency are attributes, not assumptions, so a second country is data, not a rebuild.

## Not in this repository

The source file (`INTERVIEW_ORDERS.xlsx`) is not committed. It stays in the landing volume.
