# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold marts and metric catalog
# MAGIC
# MAGIC One mart per report screen, all built from the star schema (`fct_order_line` plus the `dim_*` tables) and nothing else.
# MAGIC Every metric is defined once here and documented in `metric_catalog`; the reporting tool only sums and divides.
# MAGIC
# MAGIC | Mart | Grain | Feeds screen |
# MAGIC | --- | --- | --- |
# MAGIC | `mart_kpi_ytd` | year (comparable window) | Opening, Trend |
# MAGIC | `mart_pvm_sku_year` | base year, current year, SKU | Decomposition |
# MAGIC | `mart_pvm_bridge` | base year, current year | Decomposition (the waterfall) |
# MAGIC | `mart_growth_anatomy` | base year, current year | Anatomy of 2022 |
# MAGIC | `mart_customer_year` | year, customer | Customers |
# MAGIC | `mart_customer_bridge` | base year, current year | Customers (cylinder bridge, retention) |
# MAGIC | `mart_customer_cadence` | as-of date, customer | Action |
# MAGIC | `mart_channel_month` | month, channel | Channels |
# MAGIC
# MAGIC The comparable window (Jan 1 to the latest date in the data, in every year) comes from `dim_date.is_in_comparable_window`.
# MAGIC
# MAGIC Shape of the run: one write per table with its column comments in the schema, no read-back after a write, the cross-mart checks
# MAGIC in one query, the acceptance test in three, and governance that issues DDL only for what differs from the catalog.
# MAGIC Divisions are guarded with `NULLIF`, because serverless runs in ANSI mode, where a division by zero is an error.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC The churn-risk rule is explicit and explainable: a customer is **overdue** when it ordered on at least `MIN_ORDER_DAYS` different days in the last
# MAGIC `WINDOW_MONTHS` months and has been silent for longer than `SILENCE_MULTIPLIER` times its own median gap between orders (and at least `MIN_SILENCE_DAYS`).

# COMMAND ----------

WINDOW_MONTHS = 24
MIN_ORDER_DAYS = 4
SILENCE_MULTIPLIER = 2
MIN_SILENCE_DAYS = 45

# Acceptance test: the gold layer must reproduce the figures quoted in the report. Turn off after the data changes.
ENFORCE_REPORT_FIGURES = True

MART_TAGS = {"layer": "gold", "domain": "sales", "table_type": "mart"}
REF_TAGS = {"layer": "gold", "domain": "sales", "table_type": "reference"}
PAIR_COMMENTS = {"base_year": "Year the comparison starts from.", "current_year": "Year being compared with the base year."}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis views
# MAGIC `all_lines` is the fact joined to its dimensions. `lfl_lines` keeps only the comparable window. Both are lazy views: they cost nothing
# MAGIC until a mart reads them, and Delta reads only the columns each mart uses.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW all_lines AS
SELECT d.date, d.year, d.year_month, d.is_in_comparable_window,
       f.order_no, f.quantity, f.order_value, f.is_price_basis, f.customer_segment_at_order,
       c.customer_id, c.customer_radical,
       p.product_id, p.product_supergroup, p.is_helium,
       ch.channel_name, ch.is_attributed
FROM {T_FCT_ORDER_LINE} f
JOIN {T_DIM_DATE} d ON d.date_key = f.date_key
JOIN {T_DIM_CUSTOMER} c ON c.customer_key = f.customer_key
JOIN {T_DIM_PRODUCT} p ON p.product_key = f.product_key
JOIN {T_DIM_CHANNEL} ch ON ch.channel_key = f.channel_key
""")
spark.sql("CREATE OR REPLACE TEMP VIEW lfl_lines AS SELECT * FROM all_lines WHERE is_in_comparable_window")

bounds = spark.sql("SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM all_lines").first()
MIN_DATE, MAX_DATE = bounds["min_date"], bounds["max_date"]
print(f"Fact covers {MIN_DATE} to {MAX_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mart_kpi_ytd: the headline numbers and the 2017 = 100 index

# COMMAND ----------

KPI_COMMENTS = {
    "year": "Calendar year.",
    "window_start": "First day of the comparable window in this year.",
    "window_end": "Last day of the comparable window in this year (the same month-day in every year).",
    "cylinders": "Cylinders ordered in the window (all lines).",
    "revenue": "Revenue in EUR in the window (all lines).",
    "orders": "Distinct orders in the window.",
    "customers": "Distinct customer ids active in the window.",
    "price_per_cylinder": "Revenue divided by cylinders (realized price).",
    "revenue_full_year": "Revenue for the whole calendar year (partial for the last year).",
    "idx_cylinders": "Cylinders as a percentage of the first year.",
    "idx_revenue": "Revenue as a percentage of the first year.",
    "idx_price": "Price per cylinder as a percentage of the first year.",
    "idx_customers": "Active customers as a percentage of the first year.",
    "revenue_yoy_pct": "Revenue change against the previous year, same window, in percent.",
}

write_delta(spark.sql(f"""
WITH y AS (
  SELECT year, SUM(quantity) AS cylinders, SUM(order_value) AS revenue,
         COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT customer_id) AS customers
  FROM lfl_lines GROUP BY year),
fy AS (SELECT year, SUM(order_value) AS revenue_full_year FROM all_lines GROUP BY year),
w AS (SELECT year, MIN(date) AS window_start, MAX(date) AS window_end
      FROM {T_DIM_DATE} WHERE is_in_comparable_window GROUP BY year),
k AS (
  SELECT y.year, w.window_start, w.window_end, y.cylinders, y.revenue, y.orders, y.customers,
         y.revenue / NULLIF(y.cylinders, 0) AS price_per_cylinder, fy.revenue_full_year
  FROM y JOIN fy ON fy.year = y.year JOIN w ON w.year = y.year)
SELECT year, window_start, window_end, cylinders,
       CAST(revenue AS DECIMAL(18,2)) AS revenue, orders, customers,
       CAST(ROUND(price_per_cylinder, 4) AS DECIMAL(18,4)) AS price_per_cylinder,
       CAST(revenue_full_year AS DECIMAL(18,2)) AS revenue_full_year,
       ROUND(100.0 * cylinders / NULLIF(FIRST_VALUE(cylinders) OVER (ORDER BY year), 0), 1) AS idx_cylinders,
       ROUND(100.0 * revenue / NULLIF(FIRST_VALUE(revenue) OVER (ORDER BY year), 0), 1) AS idx_revenue,
       ROUND(100.0 * price_per_cylinder / NULLIF(FIRST_VALUE(price_per_cylinder) OVER (ORDER BY year), 0), 1) AS idx_price,
       ROUND(100.0 * customers / NULLIF(FIRST_VALUE(customers) OVER (ORDER BY year), 0), 1) AS idx_customers,
       ROUND(100.0 * (revenue / NULLIF(LAG(revenue) OVER (ORDER BY year), 0) - 1), 1) AS revenue_yoy_pct
FROM k ORDER BY year
"""), T_MART_KPI_YTD, KPI_COMMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## mart_pvm_sku_year and mart_pvm_bridge: price, volume and mix
# MAGIC Uses price-basis lines only (value above zero), by SKU, for every pair of years in the comparable window.
# MAGIC - **Volume**: the change in total cylinders, valued at the base-year average price, spread over SKUs by their base-year share.
# MAGIC - **Mix**: the shift of cylinders between SKUs, valued at base-year SKU prices.
# MAGIC - **Price**: the change in SKU price, applied to current-year cylinders.
# MAGIC
# MAGIC The three effects add up exactly to the change in revenue. A SKU sold in only one of the two years counts as mix, never as price.

# COMMAND ----------

PVM_SKU_COMMENTS = {
    **PAIR_COMMENTS,
    "product_id": "SKU.",
    "cylinders_base": "Cylinders in the base year.",
    "cylinders_current": "Cylinders in the current year.",
    "revenue_base": "Revenue in the base year.",
    "revenue_current": "Revenue in the current year.",
    "price_base": "Realized price per cylinder in the base year.",
    "price_current": "Realized price per cylinder in the current year.",
    "volume_effect": "Revenue effect of the change in total cylinders, at the base average price, spread over SKUs by base share.",
    "mix_effect": "Revenue effect of the shift of cylinders between SKUs, at base SKU prices.",
    "price_effect": "Revenue effect of the change in SKU price, on current cylinders.",
}
PVM_BRIDGE_COMMENTS = {
    **PAIR_COMMENTS,
    "revenue_base": "Revenue of the base year (price-basis lines).",
    "volume_effect": "Total volume effect.",
    "mix_effect": "Total mix effect.",
    "price_effect": "Total price effect.",
    "revenue_current": "Revenue of the current year (price-basis lines).",
    "closing_difference": "Start plus the three effects minus end. Must be zero.",
    "price_offsets_volume_loss_pct": "Price effect as a percentage of the volume loss (null when volume did not fall).",
}

write_delta(spark.sql("""
WITH s AS (
  SELECT year, product_id, SUM(quantity) AS q, CAST(SUM(order_value) AS DOUBLE) AS v
  FROM lfl_lines WHERE is_price_basis GROUP BY year, product_id),
yrs AS (SELECT DISTINCT year FROM s),
pairs AS (SELECT a.year AS base_year, b.year AS current_year FROM yrs a JOIN yrs b ON a.year < b.year),
grid AS (SELECT p.base_year, p.current_year, k.product_id FROM pairs p CROSS JOIN (SELECT DISTINCT product_id FROM s) k),
j AS (
  SELECT g.base_year, g.current_year, g.product_id,
         CAST(COALESCE(a.q, 0) AS DOUBLE) AS q_base, COALESCE(a.v, 0.0) AS rev_base,
         CAST(COALESCE(b.q, 0) AS DOUBLE) AS q_cur,  COALESCE(b.v, 0.0) AS rev_cur
  FROM grid g
  LEFT JOIN s a ON a.year = g.base_year AND a.product_id = g.product_id
  LEFT JOIN s b ON b.year = g.current_year AND b.product_id = g.product_id),
t AS (
  SELECT *,
         SUM(q_base) OVER (PARTITION BY base_year, current_year) AS q0_total,
         SUM(q_cur)  OVER (PARTITION BY base_year, current_year) AS q1_total,
         CASE WHEN q_base > 0 THEN rev_base / q_base END AS p_base_raw,
         CASE WHEN q_cur  > 0 THEN rev_cur  / q_cur  END AS p_cur_raw
  FROM j WHERE q_base > 0 OR q_cur > 0),
u AS (
  SELECT *, COALESCE(p_base_raw, p_cur_raw) AS p0, COALESCE(p_cur_raw, p_base_raw) AS p1 FROM t)
SELECT base_year, current_year, product_id,
       CAST(q_base AS BIGINT) AS cylinders_base, CAST(q_cur AS BIGINT) AS cylinders_current,
       CAST(rev_base AS DECIMAL(18,2)) AS revenue_base, CAST(rev_cur AS DECIMAL(18,2)) AS revenue_current,
       CAST(p0 AS DECIMAL(18,4)) AS price_base, CAST(p1 AS DECIMAL(18,4)) AS price_current,
       CAST((q1_total * q_base / NULLIF(q0_total, 0) - q_base) * p0 AS DECIMAL(18,4)) AS volume_effect,
       CAST((q_cur - q1_total * q_base / NULLIF(q0_total, 0)) * p0 AS DECIMAL(18,4)) AS mix_effect,
       CAST(q_cur * (p1 - p0) AS DECIMAL(18,4)) AS price_effect
FROM u
"""), T_MART_PVM_SKU, PVM_SKU_COMMENTS)

write_delta(spark.sql(f"""
SELECT base_year, current_year,
       CAST(SUM(revenue_base) AS DECIMAL(18,2)) AS revenue_base,
       CAST(SUM(volume_effect) AS DECIMAL(18,2)) AS volume_effect,
       CAST(SUM(mix_effect) AS DECIMAL(18,2)) AS mix_effect,
       CAST(SUM(price_effect) AS DECIMAL(18,2)) AS price_effect,
       CAST(SUM(revenue_current) AS DECIMAL(18,2)) AS revenue_current,
       CAST(SUM(revenue_base) + SUM(volume_effect) + SUM(mix_effect) + SUM(price_effect) - SUM(revenue_current) AS DECIMAL(18,4)) AS closing_difference,
       CASE WHEN SUM(volume_effect) < 0 THEN ROUND(100.0 * SUM(price_effect) / -SUM(volume_effect), 1) END AS price_offsets_volume_loss_pct
FROM {T_MART_PVM_SKU}
GROUP BY base_year, current_year
ORDER BY base_year, current_year
"""), T_MART_PVM_BRIDGE, PVM_BRIDGE_COMMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## mart_growth_anatomy: what is behind the growth
# MAGIC For every pair of years, the change in revenue is split into three parts that add up to it: the **top-gaining customer**, **helium sold to
# MAGIC everyone else**, and the **rest of the portfolio**. It also gives the growth rate without the top customer and without helium.

# COMMAND ----------

ANATOMY_COMMENTS = {
    **PAIR_COMMENTS,
    "revenue_base": "Revenue in the base year (comparable window).",
    "revenue_current": "Revenue in the current year.",
    "revenue_delta": "Change in revenue.",
    "growth_pct": "Revenue growth in percent.",
    "top_customer_id": "Customer with the largest revenue gain between the two years.",
    "top_customer_share_of_growth_pct": "Share of the total growth that comes from the top customer (null when revenue did not grow).",
    "growth_ex_top_customer_pct": "Revenue growth without the top customer in both years.",
    "growth_ex_helium_pct": "Revenue growth without the Cylinder Helium family in both years.",
    "bar_top_customer": "Revenue change of the top customer (all products).",
    "bar_helium_excl_top_customer": "Revenue change of helium sold to all other customers.",
    "bar_rest_of_portfolio": "Revenue change of everything else.",
}

write_delta(spark.sql("""
WITH cy AS (
  SELECT year, customer_id, SUM(order_value) AS rev,
         SUM(CASE WHEN is_helium THEN order_value ELSE 0 END) AS rev_he
  FROM lfl_lines GROUP BY year, customer_id),
yrs AS (SELECT DISTINCT year FROM cy),
pairs AS (SELECT a.year AS base_year, b.year AS current_year FROM yrs a JOIN yrs b ON a.year < b.year),
grid AS (SELECT p.base_year, p.current_year, c.customer_id FROM pairs p CROSS JOIN (SELECT DISTINCT customer_id FROM cy) c),
d AS (
  SELECT g.base_year, g.current_year, g.customer_id,
         COALESCE(a.rev, 0) AS rev_base, COALESCE(b.rev, 0) AS rev_cur,
         COALESCE(a.rev_he, 0) AS he_base, COALESCE(b.rev_he, 0) AS he_cur
  FROM grid g
  LEFT JOIN cy a ON a.year = g.base_year AND a.customer_id = g.customer_id
  LEFT JOIN cy b ON b.year = g.current_year AND b.customer_id = g.customer_id),
r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY base_year, current_year
                               ORDER BY (rev_cur - rev_base) DESC, customer_id) AS rk
  FROM d),
agg AS (
  SELECT base_year, current_year,
         SUM(rev_base) AS tot_base, SUM(rev_cur) AS tot_cur, SUM(he_base) AS he_base, SUM(he_cur) AS he_cur,
         MAX(CASE WHEN rk = 1 THEN customer_id END) AS top_customer_id,
         MAX(CASE WHEN rk = 1 THEN rev_base END) AS top_base, MAX(CASE WHEN rk = 1 THEN rev_cur END) AS top_cur,
         MAX(CASE WHEN rk = 1 THEN he_base END) AS top_he_base, MAX(CASE WHEN rk = 1 THEN he_cur END) AS top_he_cur
  FROM r GROUP BY base_year, current_year)
SELECT base_year, current_year,
       CAST(tot_base AS DECIMAL(18,2)) AS revenue_base,
       CAST(tot_cur AS DECIMAL(18,2)) AS revenue_current,
       CAST(tot_cur - tot_base AS DECIMAL(18,2)) AS revenue_delta,
       ROUND(100.0 * (tot_cur / NULLIF(tot_base, 0) - 1), 1) AS growth_pct,
       top_customer_id,
       CASE WHEN tot_cur > tot_base THEN ROUND(100.0 * (top_cur - top_base) / (tot_cur - tot_base), 1) END AS top_customer_share_of_growth_pct,
       ROUND(100.0 * ((tot_cur - top_cur) / NULLIF(tot_base - top_base, 0) - 1), 1) AS growth_ex_top_customer_pct,
       ROUND(100.0 * ((tot_cur - he_cur) / NULLIF(tot_base - he_base, 0) - 1), 1) AS growth_ex_helium_pct,
       CAST(top_cur - top_base AS DECIMAL(18,2)) AS bar_top_customer,
       CAST((he_cur - he_base) - (top_he_cur - top_he_base) AS DECIMAL(18,2)) AS bar_helium_excl_top_customer,
       CAST((tot_cur - tot_base) - (top_cur - top_base) - ((he_cur - he_base) - (top_he_cur - top_he_base)) AS DECIMAL(18,2)) AS bar_rest_of_portfolio
FROM agg ORDER BY base_year, current_year
"""), T_MART_GROWTH_ANATOMY, ANATOMY_COMMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## mart_customer_year: customers by year
# MAGIC Status against the previous year (`retained`, `new`, `winback`), the dominant segment of the year, and `is_new_24m`: a customer is new only if it
# MAGIC did not buy in the 24 months before Jan 1 of the year (null while the data does not cover a full 24-month lookback).

# COMMAND ----------

CUSTOMER_YEAR_COMMENTS = {
    "year": "Calendar year.",
    "customer_id": "Customer id.",
    "customer_radical": "Customer id without its family suffix.",
    "segment": "Dominant segment label of the customer in the year (most orders; ties alphabetical).",
    "cylinders": "Cylinders in the window.",
    "revenue": "Revenue in the window.",
    "orders": "Distinct orders in the window.",
    "cylinders_prev_year": "Cylinders in the previous year's window (null if not active).",
    "revenue_prev_year": "Revenue in the previous year's window (null if not active).",
    "status_vs_prev_year": "retained (active last year), winback (inactive last year but seen earlier), new (never seen before). Null in the first year.",
    "is_new_24m": "True when the customer had no order in the 24 months before Jan 1 of the year; null while the data has no full lookback.",
}

write_delta(spark.sql(f"""
WITH cy AS (
  SELECT year, customer_id, SUM(quantity) AS cylinders, SUM(order_value) AS revenue, COUNT(DISTINCT order_no) AS orders
  FROM lfl_lines GROUP BY year, customer_id),
seg AS (
  SELECT year, customer_id, customer_segment_at_order AS segment, COUNT(*) AS n
  FROM lfl_lines GROUP BY year, customer_id, customer_segment_at_order),
seg1 AS (
  SELECT year, customer_id, segment
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY year, customer_id ORDER BY n DESC, segment) AS rn FROM seg) WHERE rn = 1),
fa AS (SELECT customer_id, MIN(year) AS first_active_year FROM cy GROUP BY customer_id),
b AS (SELECT MIN(year) AS min_year FROM all_lines),
prior24 AS (
  SELECT DISTINCT c.year, c.customer_id
  FROM cy c JOIN all_lines a
    ON a.customer_id = c.customer_id
   AND a.date >= add_months(make_date(c.year, 1, 1), -{WINDOW_MONTHS})
   AND a.date <  make_date(c.year, 1, 1))
SELECT c.year, c.customer_id, cu.customer_radical, s.segment,
       c.cylinders, CAST(c.revenue AS DECIMAL(18,2)) AS revenue, c.orders,
       p.cylinders AS cylinders_prev_year, CAST(p.revenue AS DECIMAL(18,2)) AS revenue_prev_year,
       CASE WHEN c.year = b.min_year THEN NULL
            WHEN p.customer_id IS NOT NULL THEN 'retained'
            WHEN fa.first_active_year < c.year THEN 'winback'
            ELSE 'new' END AS status_vs_prev_year,
       CASE WHEN add_months(make_date(c.year, 1, 1), -{WINDOW_MONTHS}) >= make_date(b.min_year, 1, 1)
            THEN pr.customer_id IS NULL END AS is_new_24m
FROM cy c
JOIN seg1 s ON s.year = c.year AND s.customer_id = c.customer_id
JOIN fa ON fa.customer_id = c.customer_id
JOIN {T_DIM_CUSTOMER} cu ON cu.customer_id = c.customer_id
CROSS JOIN b
LEFT JOIN cy p ON p.customer_id = c.customer_id AND p.year = c.year - 1
LEFT JOIN prior24 pr ON pr.year = c.year AND pr.customer_id = c.customer_id
"""), T_MART_CUSTOMER_YEAR, CUSTOMER_YEAR_COMMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## mart_customer_bridge: the cylinder bridge and retention
# MAGIC For every pair of years: cylinders at the start, lost customers, new customers, win-backs (customers seen before the base year), retained customers
# MAGIC buying more and buying less, cylinders at the end. The bridge closes exactly.

# COMMAND ----------

CUSTOMER_BRIDGE_COMMENTS = {
    **PAIR_COMMENTS,
    "cylinders_start": "Cylinders in the base year.",
    "cylinders_lost": "Cylinders of customers active in the base year and not in the current year (negative).",
    "cylinders_new": "Cylinders of customers never seen before the base year.",
    "cylinders_winback": "Cylinders of customers inactive in the base year but seen before it.",
    "cylinders_retained_up": "Net increase from retained customers who bought more.",
    "cylinders_retained_down": "Net decrease from retained customers who bought less (negative).",
    "cylinders_end": "Cylinders in the current year.",
    "closing_difference": "Start plus movements minus end. Must be zero.",
    "customers_start": "Customers active in the base year.",
    "customers_retained": "Customers active in both years.",
    "customers_lost": "Customers active in the base year only.",
    "customers_new": "New customers in the current year.",
    "customers_winback": "Customers returning after being inactive in the base year.",
    "customers_end": "Customers active in the current year.",
    "retention_pct": "Retained customers as a percentage of base-year customers.",
}

write_delta(spark.sql("""
WITH cy AS (SELECT year, customer_id, SUM(quantity) AS q FROM lfl_lines GROUP BY year, customer_id),
fa AS (SELECT customer_id, MIN(year) AS first_year FROM cy GROUP BY customer_id),
yrs AS (SELECT DISTINCT year FROM cy),
pairs AS (SELECT a.year AS base_year, b.year AS current_year FROM yrs a JOIN yrs b ON a.year < b.year),
grid AS (SELECT p.base_year, p.current_year, c.customer_id FROM pairs p CROSS JOIN (SELECT DISTINCT customer_id FROM cy) c),
d AS (
  SELECT g.base_year, g.current_year, g.customer_id, fa.first_year,
         COALESCE(a.q, 0) AS qa, COALESCE(b.q, 0) AS qb
  FROM grid g
  JOIN fa ON fa.customer_id = g.customer_id
  LEFT JOIN cy a ON a.year = g.base_year AND a.customer_id = g.customer_id
  LEFT JOIN cy b ON b.year = g.current_year AND b.customer_id = g.customer_id),
c AS (
  SELECT *,
         CASE WHEN qa > 0 AND qb = 0 THEN 'lost'
              WHEN qa = 0 AND qb > 0 AND first_year < base_year THEN 'winback'
              WHEN qa = 0 AND qb > 0 THEN 'new'
              WHEN qa > 0 AND qb >= qa THEN 'retained_up'
              WHEN qa > 0 AND qb > 0 THEN 'retained_down' END AS cat
  FROM d)
SELECT base_year, current_year,
       SUM(qa) AS cylinders_start,
       -SUM(CASE WHEN cat = 'lost' THEN qa ELSE 0 END) AS cylinders_lost,
       SUM(CASE WHEN cat = 'new' THEN qb ELSE 0 END) AS cylinders_new,
       SUM(CASE WHEN cat = 'winback' THEN qb ELSE 0 END) AS cylinders_winback,
       SUM(CASE WHEN cat = 'retained_up' THEN qb - qa ELSE 0 END) AS cylinders_retained_up,
       SUM(CASE WHEN cat = 'retained_down' THEN qb - qa ELSE 0 END) AS cylinders_retained_down,
       SUM(qb) AS cylinders_end,
       SUM(qa) - SUM(CASE WHEN cat = 'lost' THEN qa ELSE 0 END) + SUM(CASE WHEN cat = 'new' THEN qb ELSE 0 END)
         + SUM(CASE WHEN cat = 'winback' THEN qb ELSE 0 END) + SUM(CASE WHEN cat IN ('retained_up', 'retained_down') THEN qb - qa ELSE 0 END)
         - SUM(qb) AS closing_difference,
       COUNT(CASE WHEN qa > 0 THEN 1 END) AS customers_start,
       COUNT(CASE WHEN cat IN ('retained_up', 'retained_down') THEN 1 END) AS customers_retained,
       COUNT(CASE WHEN cat = 'lost' THEN 1 END) AS customers_lost,
       COUNT(CASE WHEN cat = 'new' THEN 1 END) AS customers_new,
       COUNT(CASE WHEN cat = 'winback' THEN 1 END) AS customers_winback,
       COUNT(CASE WHEN qb > 0 THEN 1 END) AS customers_end,
       ROUND(100.0 * COUNT(CASE WHEN cat IN ('retained_up', 'retained_down') THEN 1 END)
             / NULLIF(COUNT(CASE WHEN qa > 0 THEN 1 END), 0), 1) AS retention_pct
FROM c GROUP BY base_year, current_year ORDER BY base_year, current_year
"""), T_MART_CUSTOMER_BRIDGE, CUSTOMER_BRIDGE_COMMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## mart_customer_cadence: who has gone quiet
# MAGIC Two as-of dates: `current` (the latest date in the data) and `backtest` (12 months earlier). For the backtest, `returned_next_12m` says whether the
# MAGIC customer ordered again in the following 12 months, which is how the rule is validated: flagged customers should come back much less often.

# COMMAND ----------

CADENCE_COMMENTS = {
    "scope": "current (latest date in the data) or backtest (12 months earlier).",
    "as_of_date": "Date the rule is evaluated at.",
    "customer_id": "Customer id.",
    "order_days": "Distinct days with an order in the 24-month window.",
    "last_order_date": "Last order date up to the as-of date.",
    "median_gap_days": "Median days between consecutive order days.",
    "silent_days": "Days between the last order and the as-of date.",
    "revenue_24m": "Revenue in the 24-month window (revenue exposed if the customer is lost).",
    "is_overdue": "True when silent for more than twice the median gap (and at least 45 days).",
    "returned_next_12m": "Backtest only: whether the customer ordered again in the following 12 months.",
}

write_delta(spark.sql(f"""
WITH cut AS (
  SELECT DATE'{MAX_DATE}' AS cutoff, 'current' AS scope
  UNION ALL SELECT add_months(DATE'{MAX_DATE}', -12), 'backtest'),
od AS (SELECT customer_id, date, SUM(order_value) AS value FROM all_lines GROUP BY customer_id, date),
w AS (
  SELECT c.cutoff, c.scope, o.customer_id, o.date, o.value
  FROM cut c JOIN od o ON o.date <= c.cutoff AND o.date > add_months(c.cutoff, -{WINDOW_MONTHS})),
g AS (
  SELECT *, DATEDIFF(date, LAG(date) OVER (PARTITION BY scope, customer_id ORDER BY date)) AS gap FROM w),
a AS (
  SELECT scope, cutoff, customer_id, COUNT(*) AS order_days, MAX(date) AS last_order_date,
         percentile(gap, 0.5) AS median_gap_days, SUM(value) AS revenue_24m
  FROM g GROUP BY scope, cutoff, customer_id HAVING COUNT(*) >= {MIN_ORDER_DAYS}),
fut AS (
  SELECT DISTINCT customer_id FROM od
  WHERE date > add_months(DATE'{MAX_DATE}', -12) AND date <= DATE'{MAX_DATE}')
SELECT a.scope, a.cutoff AS as_of_date, a.customer_id, a.order_days, a.last_order_date,
       a.median_gap_days, DATEDIFF(a.cutoff, a.last_order_date) AS silent_days,
       CAST(a.revenue_24m AS DECIMAL(18,2)) AS revenue_24m,
       DATEDIFF(a.cutoff, a.last_order_date) > GREATEST({SILENCE_MULTIPLIER} * a.median_gap_days, {MIN_SILENCE_DAYS}) AS is_overdue,
       CASE WHEN a.scope = 'backtest' THEN f.customer_id IS NOT NULL END AS returned_next_12m
FROM a LEFT JOIN fut f ON f.customer_id = a.customer_id
"""), T_MART_CUSTOMER_CADENCE, CADENCE_COMMENTS)

spark.sql(f"""
CREATE OR REPLACE VIEW {V_CADENCE_BACKTEST} (
  is_overdue COMMENT 'True when silent for more than twice the median gap (and at least 45 days) at the backtest date.',
  customers COMMENT 'Customers in the group.',
  returned_next_12m_pct COMMENT 'Share of the group that ordered again within 12 months, in percent.',
  revenue_24m COMMENT 'Revenue in the 24-month window before the backtest date, in EUR.'
) COMMENT 'Backtest of the overdue rule: did flagged customers come back within 12 months, compared with the others.' AS
SELECT is_overdue, COUNT(*) AS customers,
       ROUND(100.0 * AVG(CAST(returned_next_12m AS INT)), 1) AS returned_next_12m_pct,
       SUM(revenue_24m) AS revenue_24m
FROM {T_MART_CUSTOMER_CADENCE} WHERE scope = 'backtest' GROUP BY is_overdue
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mart_channel_month and ref_channel_events
# MAGIC Dense by month and channel (zeros included) so a stacked area chart is correct. An order always belongs to one channel in this data, so
# MAGIC `share_of_month_orders_pct` adds up to 100 within a month. Channel labels changed in Nov 2020, so channel mix across that date is not comparable.

# COMMAND ----------

CHANNEL_MONTH_COMMENTS = {
    "year_month": "Month as yyyy-MM.",
    "month_start": "First day of the month.",
    "channel_name": "Channel label.",
    "is_attributed": "False for UNKNOWN SOURCE.",
    "orders": "Distinct orders.",
    "customers": "Distinct customer ids.",
    "cylinders": "Cylinders.",
    "revenue": "Revenue in EUR.",
    "share_of_month_orders_pct": "Channel share of the month's orders, in percent.",
}

write_delta(spark.sql(f"""
WITH months AS (
  SELECT DISTINCT year_month FROM {T_DIM_DATE} WHERE date BETWEEN DATE'{MIN_DATE}' AND DATE'{MAX_DATE}'),
ch AS (SELECT channel_name, is_attributed FROM {T_DIM_CHANNEL}),
m AS (
  SELECT year_month, channel_name,
         COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT customer_id) AS customers,
         SUM(quantity) AS cylinders, SUM(order_value) AS revenue
  FROM all_lines GROUP BY year_month, channel_name),
f AS (
  SELECT mo.year_month, ch.channel_name, ch.is_attributed,
         COALESCE(m.orders, 0) AS orders, COALESCE(m.customers, 0) AS customers,
         COALESCE(m.cylinders, 0) AS cylinders, COALESCE(m.revenue, 0) AS revenue
  FROM months mo CROSS JOIN ch
  LEFT JOIN m ON m.year_month = mo.year_month AND m.channel_name = ch.channel_name)
SELECT year_month, to_date(concat(year_month, '-01')) AS month_start, channel_name, is_attributed,
       orders, customers, cylinders, CAST(revenue AS DECIMAL(18,2)) AS revenue,
       ROUND(100.0 * orders / NULLIF(SUM(orders) OVER (PARTITION BY year_month), 0), 1) AS share_of_month_orders_pct
FROM f ORDER BY year_month, channel_name
"""), T_MART_CHANNEL_MONTH, CHANNEL_MONTH_COMMENTS)

# What to mark on the channel chart. The first event is read from the data (one query); the second is a hypothesis to confirm.
unknown = spark.sql(f"""
SELECT MIN(CASE WHEN channel_name = '{UNKNOWN_CHANNEL}' AND orders > 0 THEN year_month END) AS first_month,
       ROUND(100.0 * SUM(CASE WHEN channel_name = '{UNKNOWN_CHANNEL}' AND year_month >= '{MAX_DATE.year}-01' THEN orders ELSE 0 END)
             / NULLIF(SUM(CASE WHEN year_month >= '{MAX_DATE.year}-01' THEN orders ELSE 0 END), 0), 0) AS share_last_year
FROM {T_MART_CHANNEL_MONTH}
""").first()

events = []
if unknown["first_month"] is not None:
    share_text = f" It grows from zero to about {int(unknown['share_last_year'])}% of orders in {MAX_DATE.year}." if unknown["share_last_year"] is not None else ""
    events.append((unknown["first_month"], UNKNOWN_CHANNEL, "UNKNOWN SOURCE appears",
                   f"First order with this label in the data.{share_text}", "observed"))
events.append((
    "2020-11", "PHONECALL / RETAIL SHOP", "Orders shift from PHONECALL to RETAIL SHOP",
    "PHONECALL falls and RETAIL SHOP rises sharply from Nov 2020, and the shift partly reverses in Feb 2021. "
    "It looks like a label or process change rather than customer behavior; confirm with the business before using channel in a decision.",
    "hypothesis",
))
events.sort()

CHANNEL_EVENTS_COMMENTS = {
    "event_month": "Month of the event (yyyy-MM).",
    "channel": "Channel(s) involved.",
    "event": "Short name.",
    "evidence": "What the data shows.",
    "status": "observed or hypothesis.",
}
write_delta(
    spark.createDataFrame(events, "event_month string, channel string, event string, evidence string, status string"),
    T_REF_CHANNEL_EVENTS, CHANNEL_EVENTS_COMMENTS,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric catalog
# MAGIC Every metric used by the report: what it means, where it lives and what to watch for. This table is the data dictionary for numbers.

# COMMAND ----------

metrics = [
    ("cylinders", "Cylinders", "Cylinders ordered, all lines, in the comparable window.", "cylinders", "year", "mart_kpi_ytd", "cylinders", "Comparable window", "Includes zero-value lines."),
    ("revenue", "Revenue", "Order value in EUR, all lines, in the comparable window.", "EUR", "year", "mart_kpi_ytd", "revenue", "Comparable window", "A partial last year is compared with the same part of other years; see revenue_full_year for the full-year figure."),
    ("orders", "Orders", "Distinct order numbers in the comparable window.", "orders", "year", "mart_kpi_ytd", "orders", "Comparable window", "An order belongs to one customer, one date and one channel in this data."),
    ("active_customers", "Active customers", "Distinct customer ids with at least one order in the comparable window.", "customer ids", "year", "mart_kpi_ytd", "customers", "Comparable window", "Counts ids, not legal customers: a radical can appear under two id families (dim_customer.radical_in_multiple_families)."),
    ("price_per_cylinder", "Price per cylinder", "Revenue divided by cylinders.", "EUR per cylinder", "year", "mart_kpi_ytd", "price_per_cylinder", "Comparable window", "Realized price, not list price; mixes SKUs, so it moves with product mix."),
    ("idx_revenue", "Revenue index", "Revenue as a percentage of the first year (first year = 100). Same for idx_cylinders, idx_price and idx_customers.", "index", "year", "mart_kpi_ytd", "idx_revenue", "Comparable window", "Index is the only way to compare series with different units on one axis."),
    ("pvm_volume", "Volume effect", "Change in revenue caused by the change in total cylinders, at the base-year average price.", "EUR", "base year, current year", "mart_pvm_bridge", "volume_effect", "Comparable window, price-basis lines", "Lines with value zero or below are excluded."),
    ("pvm_mix", "Mix effect", "Change in revenue caused by the shift of cylinders between SKUs, at base-year SKU prices.", "EUR", "base year, current year", "mart_pvm_bridge", "mix_effect", "Comparable window, price-basis lines", "A SKU sold in only one of the two years counts as mix."),
    ("pvm_price", "Price effect", "Change in revenue caused by the change in SKU price, on current-year cylinders.", "EUR", "base year, current year", "mart_pvm_bridge", "price_effect", "Comparable window, price-basis lines", "Realized price per SKU; the base has no cost or inflation index, so it does not say why price rose."),
    ("top_customer_share_of_growth", "Top customer share of growth", "Revenue gained by the top-gaining customer divided by total revenue growth.", "percent", "base year, current year", "mart_growth_anatomy", "top_customer_share_of_growth_pct", "Comparable window", "Null when revenue did not grow."),
    ("growth_ex_top_customer", "Growth without the top customer", "Revenue growth after removing the top-gaining customer from both years.", "percent", "base year, current year", "mart_growth_anatomy", "growth_ex_top_customer_pct", "Comparable window", None),
    ("growth_ex_helium", "Growth without helium", "Revenue growth after removing the Cylinder Helium family from both years.", "percent", "base year, current year", "mart_growth_anatomy", "growth_ex_helium_pct", "Comparable window", None),
    ("customer_retention", "Customer retention", "Customers active in both years divided by customers active in the base year.", "percent", "base year, current year", "mart_customer_bridge", "retention_pct", "Comparable window", "Activity means at least one order in the window, not a contract."),
    ("cylinders_retained_down", "Cylinders: retained customers buying less", "Change in cylinders from customers active in both years who bought less in the current year.", "cylinders", "base year, current year", "mart_customer_bridge", "cylinders_retained_down", "Comparable window", "Retention can look stable while this leak grows."),
    ("new_customers_24m", "New customers (24-month lookback)", "Customers with no order in the 24 months before Jan 1 of the year.", "customer ids", "year, customer", "mart_customer_year", "is_new_24m", "Comparable window, all earlier history", "Null before the data covers a full 24-month lookback; the naive definition (first order in the data) overstates new customers."),
    ("overdue_customers", "Overdue customers", f"At least {MIN_ORDER_DAYS} order days in the last {WINDOW_MONTHS} months and silent for more than {SILENCE_MULTIPLIER} times the median gap between orders (minimum {MIN_SILENCE_DAYS} days).", "customer ids", "as-of date, customer", "mart_customer_cadence", "is_overdue", "Last 24 months before the as-of date", "Revenue exposed, not expected loss: in the backtest a share of flagged customers still returned."),
    ("unattributed_order_share", "Orders without a channel", "Share of orders labelled UNKNOWN SOURCE.", "percent", "month, channel", "mart_channel_month", "share_of_month_orders_pct", "All dates", "Channel labels changed in Nov 2020; do not read channel behavior across that date."),
]
METRIC_CATALOG_COMMENTS = {
    "metric_id": "Metric identifier.",
    "metric_name": "Display name.",
    "definition": "What the metric means.",
    "unit": "Unit of the metric.",
    "grain": "Level of detail of the source table.",
    "source_table": "Gold table that holds the metric.",
    "source_column": "Column that holds the metric.",
    "scope": "Rows the metric covers.",
    "caveat": "What to keep in mind when reading it.",
}
write_delta(
    spark.createDataFrame(
        metrics,
        "metric_id string, metric_name string, definition string, unit string, grain string, source_table string, source_column string, scope string, caveat string",
    ),
    T_METRIC_CATALOG, METRIC_CATALOG_COMMENTS,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks across the marts (fail-level)
# MAGIC The marts must agree with each other and with the fact. One query gives every number; results are logged in `dq_rule_results` (`layer = 'gold'`).

# COMMAND ----------

m = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {T_MART_PVM_BRIDGE}) AS pairs,
  (SELECT COUNT(*) FROM {T_MART_PVM_BRIDGE} WHERE ABS(closing_difference) > 0.01) AS m01,
  (SELECT COUNT(*) FROM {T_MART_CUSTOMER_BRIDGE} WHERE closing_difference <> 0) AS m02,
  (SELECT COUNT(*) FROM {T_MART_GROWTH_ANATOMY}
     WHERE ABS(bar_top_customer + bar_helium_excl_top_customer + bar_rest_of_portfolio - revenue_delta) > 0.01) AS m03,
  (SELECT SUM(revenue_full_year) FROM {T_MART_KPI_YTD}) AS kpi_full_year,
  (SELECT SUM(order_value) FROM {T_FCT_ORDER_LINE}) AS fact_value,
  (SELECT COUNT(*) FROM {T_MART_KPI_YTD}) AS years,
  (SELECT COUNT(*) FROM {T_MART_KPI_YTD} k
     JOIN (SELECT year, SUM(revenue) AS r FROM {T_MART_CUSTOMER_YEAR} GROUP BY year) c ON c.year = k.year
     WHERE ABS(k.revenue - c.r) > 0.01) AS m05,
  (SELECT COUNT(*) FROM (SELECT year_month FROM {T_MART_CHANNEL_MONTH}
     GROUP BY year_month HAVING ABS(SUM(share_of_month_orders_pct) - 100) > 0.5)) AS m06,
  (SELECT COUNT(DISTINCT year_month) FROM {T_MART_CHANNEL_MONTH}) AS months
""").first().asDict()

rules = [
    dq_result("gold", "M01_pvm_closes", "Volume + mix + price add up to the revenue change for every pair of years", "fail", m["m01"], m["pairs"]),
    dq_result("gold", "M02_customer_bridge_closes", "The cylinder bridge closes for every pair of years", "fail", m["m02"], m["pairs"]),
    dq_result("gold", "M03_anatomy_adds_up", "Top customer + other helium + rest equals the revenue change", "fail", m["m03"], m["pairs"]),
    dq_result("gold", "M04_kpi_vs_fact", "Full-year revenue in mart_kpi_ytd adds up to the fact total", "fail",
              int(abs(m["kpi_full_year"] - m["fact_value"]) > Decimal("0.01")), 1),
    dq_result("gold", "M05_customer_year_vs_kpi", "Revenue in mart_customer_year equals revenue in mart_kpi_ytd for every year", "fail", m["m05"], m["years"]),
    dq_result("gold", "M06_channel_share", "Channel shares add up to 100 in every month", "fail", m["m06"], m["months"]),
]
log_and_enforce(rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acceptance test: gold reproduces the figures in the report
# MAGIC The figures quoted in the report were computed independently with pandas. The gold layer has to give the same numbers.
# MAGIC Three reads: the KPI mart (6 rows), the 2017 to 2022 bridge and the 2021 to 2022 anatomy (one row each), and one query for the customer counts.

# COMMAND ----------

def need(row, what):
    if row is None:
        raise AssertionError(f"Acceptance test cannot find {what}. If the data changed on purpose, set ENFORCE_REPORT_FIGURES = False.")
    return row


kpi_by_year = {r["year"]: r for r in spark.table(T_MART_KPI_YTD).collect()}
bridge = need(spark.table(T_MART_PVM_BRIDGE).where("base_year = 2017 AND current_year = 2022").first(), "the 2017 to 2022 bridge")
anatomy = need(spark.table(T_MART_GROWTH_ANATOMY).where("base_year = 2021 AND current_year = 2022").first(), "the 2021 to 2022 anatomy")
counts = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {T_MART_CUSTOMER_YEAR} WHERE year = 2019 AND is_new_24m) AS new_2019,
  (SELECT COUNT(*) FROM {T_MART_CUSTOMER_YEAR} WHERE year = 2022 AND is_new_24m) AS new_2022,
  (SELECT COUNT(*) FROM {T_MART_CUSTOMER_CADENCE} WHERE scope = 'current' AND is_overdue) AS overdue_now,
  (SELECT returned_next_12m_pct FROM {V_CADENCE_BACKTEST} WHERE is_overdue) AS backtest_overdue_returned,
  (SELECT returned_next_12m_pct FROM {V_CADENCE_BACKTEST} WHERE NOT is_overdue) AS backtest_other_returned
""").first().asDict()

# Comparable-window KPIs by year, as computed independently with pandas: (cylinders, revenue, orders, customers)
REPORT_KPIS = {
    2017: (111_235, 4_410_209.97, 29_371, 4_269),
    2018: (111_674, 4_651_553.32, 28_922, 4_176),
    2019: (104_020, 4_611_941.25, 27_268, 4_117),
    2020: (88_477, 4_217_163.86, 24_154, 3_801),
    2021: (77_894, 3_959_955.75, 22_110, 3_644),
    2022: (71_647, 4_081_036.86, 20_118, 3_377),
}
kpi_figures = []
for yr, (cyl, rev, orders_, cust) in REPORT_KPIS.items():
    k = need(kpi_by_year.get(yr), f"year {yr} in mart_kpi_ytd")
    kpi_figures += [
        (f"{yr}: cylinders (comparable window)", float(k["cylinders"]), cyl, 0),
        (f"{yr}: revenue (EUR)", float(k["revenue"]), rev, 0.01),
        (f"{yr}: orders", float(k["orders"]), orders_, 0),
        (f"{yr}: active customers", float(k["customers"]), cust, 0),
    ]

# (name, actual, expected, tolerance)
figures = kpi_figures + [
    ("Price per cylinder 2017", float(kpi_by_year[2017]["price_per_cylinder"]), 39.65, 0.005),
    ("Price per cylinder 2022", float(kpi_by_year[2022]["price_per_cylinder"]), 56.96, 0.005),
    ("Bridge 2017 to 2022: start (EUR k)", float(bridge["revenue_base"]) / 1000, 4_410, 0.5),
    ("Bridge 2017 to 2022: volume (EUR k)", float(bridge["volume_effect"]) / 1000, -1_568, 0.5),
    ("Bridge 2017 to 2022: mix (EUR k)", float(bridge["mix_effect"]) / 1000, 99, 0.5),
    ("Bridge 2017 to 2022: price (EUR k)", float(bridge["price_effect"]) / 1000, 1_140, 0.5),
    ("Bridge 2017 to 2022: end (EUR k)", float(bridge["revenue_current"]) / 1000, 4_081, 0.5),
    ("Top-gaining customer 2021 to 2022", int(anatomy["top_customer_id"]), 4_420_090_200, 0),
    ("Top customer share of growth (%)", float(anatomy["top_customer_share_of_growth_pct"]), 81, 1.0),
    ("Growth without the top customer (%)", float(anatomy["growth_ex_top_customer_pct"]), 0.6, 0.05),
    ("Growth without helium (%)", float(anatomy["growth_ex_helium_pct"]), -4.2, 0.05),
    ("New customers 2019 (24-month lookback)", counts["new_2019"], 618, 0),
    ("New customers 2022 (24-month lookback)", counts["new_2022"], 456, 0),
    ("Overdue customers now", counts["overdue_now"], 480, 0),
    ("Backtest: overdue customers that returned (%)", float(counts["backtest_overdue_returned"]), 63, 0.5),
    ("Backtest: other customers that returned (%)", float(counts["backtest_other_returned"]), 93, 0.5),
]
acceptance = pd.DataFrame(
    [{"figure": n, "gold": a, "report": e, "ok": abs(a - e) <= t} for n, a, e, t in figures]
)
display(acceptance)
if ENFORCE_REPORT_FIGURES:
    assert acceptance["ok"].all(), "Gold does not reproduce the figures in the report. See the table above."
    print("Acceptance test passed: gold reproduces every figure quoted in the report.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance: comments and tags
# MAGIC Compared with the catalog first; only differences are applied. Column comments were written with the schema,
# MAGIC so a normal run issues at most the table comment.

# COMMAND ----------

apply_governance(
    T_MART_KPI_YTD,
    "Headline numbers by year in the comparable window, plus the index with the first year = 100. Feeds the Opening and Trend screens.",
    KPI_COMMENTS, MART_TAGS,
)
apply_governance(
    T_MART_PVM_SKU,
    "Price, volume and mix effects by SKU for every pair of years (comparable window, price-basis lines). The three effects add up to the revenue change.",
    PVM_SKU_COMMENTS, MART_TAGS,
)
apply_governance(
    T_MART_PVM_BRIDGE,
    "The price-volume-mix bridge by pair of years: the table behind the waterfall chart.",
    PVM_BRIDGE_COMMENTS, MART_TAGS,
)
apply_governance(
    T_MART_GROWTH_ANATOMY,
    "What is behind revenue growth, by pair of years: the top-gaining customer, helium sold to everyone else, and the rest. The three bars add up to the revenue change.",
    ANATOMY_COMMENTS, MART_TAGS,
)
apply_governance(
    T_MART_CUSTOMER_YEAR,
    "One row per customer id and year (comparable window): activity, status against the previous year, dominant segment and the 24-month new-customer flag.",
    CUSTOMER_YEAR_COMMENTS, MART_TAGS,
)
apply_governance(
    T_MART_CUSTOMER_BRIDGE,
    "The cylinder bridge by pair of years: lost, new, win-back and retained customers (buying more or less), plus retention. Closes exactly.",
    CUSTOMER_BRIDGE_COMMENTS, MART_TAGS,
)
apply_governance(
    T_MART_CUSTOMER_CADENCE,
    "Customers who ordered on at least 4 days in the last 24 months, with their own ordering rhythm and whether they are overdue. Two as-of dates: current and a 12-month backtest.",
    CADENCE_COMMENTS, MART_TAGS,
)
apply_governance(
    T_MART_CHANNEL_MONTH,
    "Orders, customers, cylinders and revenue by month and channel, with zeros included. Channel labels changed in Nov 2020; see ref_channel_events.",
    CHANNEL_MONTH_COMMENTS, MART_TAGS,
)
apply_governance(
    T_REF_CHANNEL_EVENTS,
    "Events worth marking on the channel chart. Status says whether an event is observed in the data or is a hypothesis to confirm.",
    CHANNEL_EVENTS_COMMENTS, REF_TAGS,
)
apply_governance(
    T_METRIC_CATALOG,
    "Data dictionary for numbers: every metric used by the report, its definition, where it lives and what to watch for.",
    METRIC_CATALOG_COMMENTS, REF_TAGS,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Row counts of the published tables, in one query.

# COMMAND ----------

display(table_row_counts([
    T_MART_KPI_YTD, T_MART_PVM_SKU, T_MART_PVM_BRIDGE, T_MART_GROWTH_ANATOMY, T_MART_CUSTOMER_YEAR,
    T_MART_CUSTOMER_BRIDGE, T_MART_CUSTOMER_CADENCE, T_MART_CHANNEL_MONTH, T_REF_CHANNEL_EVENTS, T_METRIC_CATALOG,
]))
print("Marts ready. Next: connect the reporting tool to lindedata.gold.")
