import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

const goldOperations = [
  // ─── 1–10: Business Modeling ───
  {
    id: 1,
    group: 'Business Modeling',
    title: 'KPI Calculation',
    desc: 'Compute core business KPIs from Silver data into Gold KPI table',
    code: `-- KPI calculation: revenue, AOV, conversion rate
CREATE OR REPLACE TABLE gold.kpis.daily_kpis AS
SELECT
  order_date,
  SUM(revenue)                          AS total_revenue,
  COUNT(DISTINCT order_id)              AS total_orders,
  SUM(revenue) / COUNT(DISTINCT order_id) AS avg_order_value,
  COUNT(DISTINCT customer_id)           AS unique_customers,
  SUM(units_sold)                       AS total_units
FROM silver.sales.orders_clean
GROUP BY order_date
ORDER BY order_date;`,
  },
  {
    id: 2,
    group: 'Business Modeling',
    title: 'Business Metric Definition',
    desc: 'Define reusable business metrics using Delta Live Tables expectations',
    code: `-- Business metric: Monthly Recurring Revenue (MRR)
CREATE OR REPLACE TABLE gold.finance.mrr AS
SELECT
  DATE_TRUNC('month', subscription_start) AS month,
  SUM(monthly_value)                       AS mrr,
  COUNT(DISTINCT customer_id)              AS paying_customers,
  SUM(monthly_value) / COUNT(DISTINCT customer_id) AS arpu
FROM silver.subscriptions.active_subs
WHERE status = 'active'
GROUP BY DATE_TRUNC('month', subscription_start);`,
  },
  {
    id: 3,
    group: 'Business Modeling',
    title: 'Fact Table Creation',
    desc: 'Build central fact table for transactional data at grain level',
    code: `-- Fact table: fact_sales at order-line grain
CREATE OR REPLACE TABLE gold.marts.fact_sales AS
SELECT
  s.order_id, s.order_date, s.customer_key,
  s.product_key, s.store_key, s.date_key,
  s.quantity, s.unit_price,
  s.quantity * s.unit_price AS gross_revenue,
  s.discount_amount,
  (s.quantity * s.unit_price) - s.discount_amount AS net_revenue
FROM silver.sales.orders_clean s
WHERE s.status NOT IN ('cancelled', 'returned');`,
  },
  {
    id: 4,
    group: 'Business Modeling',
    title: 'Dimension Table Creation',
    desc: 'Build slowly changing dimension (SCD Type 2) for customers',
    code: `-- SCD Type 2 dimension: dim_customer
MERGE INTO gold.marts.dim_customer AS target
USING silver.customers.customers_clean AS source
ON target.customer_id = source.customer_id
  AND target.is_current = TRUE
WHEN MATCHED AND (target.email <> source.email OR target.segment <> source.segment) THEN
  UPDATE SET target.is_current = FALSE, target.end_date = current_date()
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, segment, start_date, end_date, is_current)
  VALUES (source.customer_id, source.email, source.segment, current_date(), NULL, TRUE);`,
  },
  {
    id: 5,
    group: 'Business Modeling',
    title: 'Aggregation',
    desc: 'Roll up transactional data into daily, weekly, monthly aggregates',
    code: `-- Multi-granularity aggregation using GROUPING SETS
CREATE OR REPLACE TABLE gold.finance.revenue_agg AS
SELECT
  COALESCE(CAST(order_date AS STRING), 'ALL')   AS date_val,
  COALESCE(region, 'ALL')                        AS region,
  COALESCE(product_category, 'ALL')              AS category,
  SUM(net_revenue)                               AS revenue,
  COUNT(DISTINCT order_id)                       AS orders
FROM silver.sales.orders_clean
GROUP BY GROUPING SETS (
  (order_date, region, product_category),
  (order_date, region),
  (order_date),
  ()
);`,
  },
  {
    id: 6,
    group: 'Business Modeling',
    title: 'Data Mart Creation',
    desc: 'Build a subject-area data mart scoped to finance domain',
    code: `-- Finance data mart: combine facts + dims
CREATE OR REPLACE TABLE gold.finance_mart.revenue_by_customer AS
SELECT
  dc.customer_name, dc.segment, dc.region,
  dp.product_name, dp.category,
  fs.order_date,
  SUM(fs.net_revenue) AS revenue,
  COUNT(fs.order_id)  AS orders
FROM gold.marts.fact_sales fs
JOIN gold.marts.dim_customer dc ON fs.customer_key = dc.customer_key AND dc.is_current
JOIN gold.marts.dim_product  dp ON fs.product_key  = dp.product_key
GROUP BY 1, 2, 3, 4, 5, 6;`,
  },
  {
    id: 7,
    group: 'Business Modeling',
    title: 'Semantic Modeling',
    desc: 'Define Databricks semantic layer with metrics and dimensions',
    code: `-- Semantic model via Unity Catalog metric views
CREATE METRIC VIEW gold.semantic.sales_metrics AS
SELECT
  order_date         AS dim_date,
  region             AS dim_region,
  product_category   AS dim_category,
  SUM(net_revenue)   AS metric_revenue,
  COUNT(order_id)    AS metric_orders,
  AVG(net_revenue)   AS metric_aov
FROM gold.marts.fact_sales
GROUP BY ALL;`,
  },
  {
    id: 8,
    group: 'Business Modeling',
    title: 'Star Schema',
    desc: 'Build classic star schema with one fact and multiple dimension tables',
    code: `-- Star schema setup
CREATE SCHEMA IF NOT EXISTS gold.star;
-- Fact
CREATE OR REPLACE TABLE gold.star.fact_orders   AS SELECT * FROM gold.marts.fact_sales;
-- Dimensions
CREATE OR REPLACE TABLE gold.star.dim_date      AS SELECT * FROM gold.marts.dim_date;
CREATE OR REPLACE TABLE gold.star.dim_customer  AS SELECT * FROM gold.marts.dim_customer;
CREATE OR REPLACE TABLE gold.star.dim_product   AS SELECT * FROM gold.marts.dim_product;
CREATE OR REPLACE TABLE gold.star.dim_geography AS SELECT * FROM gold.marts.dim_geography;
-- Verify joins
SELECT COUNT(*) FROM gold.star.fact_orders f
JOIN gold.star.dim_customer c ON f.customer_key = c.customer_key;`,
  },
  {
    id: 9,
    group: 'Business Modeling',
    title: 'Snowflake Schema',
    desc: 'Normalise product dimension into category and subcategory sub-tables',
    code: `-- Snowflake schema: split dim_product into sub-dims
CREATE OR REPLACE TABLE gold.snowflake.dim_category AS
SELECT DISTINCT category_id, category_name, dept_id FROM silver.products.product_clean;

CREATE OR REPLACE TABLE gold.snowflake.dim_subcategory AS
SELECT DISTINCT subcategory_id, subcategory_name, category_id FROM silver.products.product_clean;

CREATE OR REPLACE TABLE gold.snowflake.dim_product AS
SELECT product_id, product_name, sku, subcategory_id, unit_cost, list_price
FROM silver.products.product_clean;`,
  },
  {
    id: 10,
    group: 'Business Modeling',
    title: 'Business Rule Enforcement',
    desc: 'Apply business rules as constraints on Gold layer tables',
    code: `-- Business rule: revenue cannot be negative; orders need valid customer
ALTER TABLE gold.marts.fact_sales
  ADD CONSTRAINT chk_revenue_positive CHECK (net_revenue >= 0);

ALTER TABLE gold.marts.fact_sales
  ADD CONSTRAINT fk_customer FOREIGN KEY (customer_key)
  REFERENCES gold.marts.dim_customer(customer_key);

-- Validation query
SELECT COUNT(*) AS violations
FROM gold.marts.fact_sales
WHERE net_revenue < 0 OR customer_key IS NULL;`,
  },

  // ─── 11–20: Data Marts ───
  {
    id: 11,
    group: 'Data Marts',
    title: 'Finance Mart',
    desc: 'P&L, balance sheet and cash flow summary tables for finance teams',
    code: `-- Finance mart: P&L summary
CREATE OR REPLACE TABLE gold.finance_mart.pnl_summary AS
SELECT
  fiscal_year, fiscal_quarter, cost_center,
  SUM(CASE WHEN type = 'revenue' THEN amount ELSE 0 END) AS revenue,
  SUM(CASE WHEN type = 'cogs'    THEN amount ELSE 0 END) AS cogs,
  SUM(CASE WHEN type = 'opex'    THEN amount ELSE 0 END) AS opex,
  SUM(CASE WHEN type = 'revenue' THEN amount ELSE 0 END)
    - SUM(CASE WHEN type IN ('cogs','opex') THEN amount ELSE 0 END) AS ebitda
FROM silver.finance.gl_entries_clean
GROUP BY 1, 2, 3;`,
  },
  {
    id: 12,
    group: 'Data Marts',
    title: 'Customer 360 Mart',
    desc: 'Unified customer view across CRM, transactions, support and web',
    code: `-- Customer 360: unified profile
CREATE OR REPLACE TABLE gold.cx_mart.customer_360 AS
SELECT
  c.customer_id, c.email, c.segment, c.lifetime_value,
  t.total_orders, t.total_revenue, t.last_order_date,
  s.open_tickets, s.csat_score,
  w.last_visit_date, w.sessions_last_30d
FROM silver.customers.profiles      c
LEFT JOIN silver.sales.cust_agg     t USING (customer_id)
LEFT JOIN silver.support.cust_agg   s USING (customer_id)
LEFT JOIN silver.web.cust_web_agg   w USING (customer_id);`,
  },
  {
    id: 13,
    group: 'Data Marts',
    title: 'Sales Mart',
    desc: 'Sales performance mart: reps, quotas, pipeline and attainment',
    code: `-- Sales mart: rep attainment
CREATE OR REPLACE TABLE gold.sales_mart.rep_attainment AS
SELECT
  r.rep_id, r.rep_name, r.region, r.manager,
  q.quota_amount,
  a.actual_revenue,
  ROUND(a.actual_revenue / NULLIF(q.quota_amount, 0) * 100, 1) AS attainment_pct,
  a.deals_won, a.pipeline_value
FROM silver.sales.reps           r
JOIN silver.sales.quotas         q USING (rep_id)
JOIN silver.sales.actuals_agg    a USING (rep_id);`,
  },
  {
    id: 14,
    group: 'Data Marts',
    title: 'Marketing Mart',
    desc: 'Campaign ROI, attribution and channel performance mart',
    code: `-- Marketing mart: campaign performance
CREATE OR REPLACE TABLE gold.mktg_mart.campaign_roi AS
SELECT
  c.campaign_id, c.campaign_name, c.channel, c.start_date,
  c.spend,
  a.attributed_revenue,
  a.attributed_orders,
  a.attributed_revenue / NULLIF(c.spend, 0) AS roas,
  (a.attributed_revenue - c.spend) / NULLIF(c.spend, 0) AS roi
FROM silver.marketing.campaigns   c
LEFT JOIN silver.marketing.attribution_agg a USING (campaign_id);`,
  },
  {
    id: 15,
    group: 'Data Marts',
    title: 'Supply Chain Mart',
    desc: 'Inventory turns, fill rate and supplier OTD for supply chain ops',
    code: `-- Supply chain mart: inventory & supplier OTD
CREATE OR REPLACE TABLE gold.scm_mart.inventory_health AS
SELECT
  product_id, warehouse_id, sku,
  on_hand_units,
  avg_daily_demand,
  on_hand_units / NULLIF(avg_daily_demand, 0) AS days_of_supply,
  reorder_point,
  CASE WHEN on_hand_units < reorder_point THEN 'REORDER' ELSE 'OK' END AS status
FROM silver.scm.inventory_clean
JOIN silver.scm.demand_agg USING (product_id);`,
  },
  {
    id: 16,
    group: 'Data Marts',
    title: 'Risk Mart',
    desc: 'Credit risk scores, exposure and VaR aggregated for risk reporting',
    code: `-- Risk mart: credit exposure summary
CREATE OR REPLACE TABLE gold.risk_mart.credit_exposure AS
SELECT
  counterparty_id, counterparty_name, rating,
  SUM(notional_value)                   AS total_exposure,
  SUM(expected_loss)                    AS expected_loss,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY simulated_loss) AS var_99,
  MAX(days_past_due)                    AS max_dpd
FROM silver.risk.positions_clean
GROUP BY counterparty_id, counterparty_name, rating;`,
  },
  {
    id: 17,
    group: 'Data Marts',
    title: 'Product Mart',
    desc: 'Product adoption, engagement and retention metrics mart',
    code: `-- Product mart: feature adoption
CREATE OR REPLACE TABLE gold.product_mart.feature_adoption AS
SELECT
  feature_id, feature_name, release_date,
  COUNT(DISTINCT user_id)                              AS adopted_users,
  COUNT(DISTINCT user_id) / MAX(t.total_active_users) AS adoption_rate,
  AVG(events_per_user)                                 AS avg_engagement,
  COUNT(DISTINCT CASE WHEN days_since_first_use <= 7 THEN user_id END) AS d7_retained
FROM silver.product.feature_events_agg
CROSS JOIN (SELECT COUNT(DISTINCT user_id) AS total_active_users FROM silver.product.sessions) t
GROUP BY 1, 2, 3;`,
  },
  {
    id: 18,
    group: 'Data Marts',
    title: 'HR Mart',
    desc: 'Headcount, attrition, hiring funnel and span of control mart',
    code: `-- HR mart: headcount and attrition
CREATE OR REPLACE TABLE gold.hr_mart.workforce_summary AS
SELECT
  department, location, job_level,
  COUNT(DISTINCT employee_id)                  AS headcount,
  SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_hc,
  SUM(CASE WHEN termination_date IS NOT NULL
        AND YEAR(termination_date) = YEAR(current_date()) THEN 1 ELSE 0 END) AS ytd_attritions,
  AVG(tenure_years)                            AS avg_tenure
FROM silver.hr.employees_clean
GROUP BY 1, 2, 3;`,
  },
  {
    id: 19,
    group: 'Data Marts',
    title: 'Operations Mart',
    desc: 'Operational efficiency metrics: cycle times, utilisation and OEE',
    code: `-- Operations mart: OEE (Overall Equipment Effectiveness)
CREATE OR REPLACE TABLE gold.ops_mart.oee_summary AS
SELECT
  machine_id, shift_date, shift,
  planned_minutes,
  actual_run_minutes,
  good_units, total_units,
  actual_run_minutes / NULLIF(planned_minutes, 0)          AS availability,
  good_units          / NULLIF(total_units, 0)             AS quality,
  total_units         / NULLIF(ideal_run_units, 0)         AS performance
FROM silver.operations.machine_logs_agg;`,
  },
  {
    id: 20,
    group: 'Data Marts',
    title: 'Executive Dashboard Mart',
    desc: 'Consolidated C-suite KPIs across all domains in a single table',
    code: `-- Executive dashboard mart: cross-domain KPIs
CREATE OR REPLACE TABLE gold.exec_mart.cxo_kpis AS
SELECT current_date() AS snapshot_date,
  f.total_revenue, f.ebitda_margin,
  s.total_orders, s.avg_attainment_pct,
  c.avg_csat, c.churn_rate,
  o.avg_oee, o.on_time_delivery_rate,
  h.headcount, h.attrition_rate,
  r.total_exposure, r.avg_pd
FROM gold.finance_mart.exec_summary  f
CROSS JOIN gold.sales_mart.exec_summary    s
CROSS JOIN gold.cx_mart.exec_summary       c
CROSS JOIN gold.ops_mart.exec_summary      o
CROSS JOIN gold.hr_mart.exec_summary       h
CROSS JOIN gold.risk_mart.exec_summary     r;`,
  },

  // ─── 21–30: Aggregation & Perf ───
  {
    id: 21,
    group: 'Aggregation & Perf',
    title: 'Pre-aggregation',
    desc: 'Compute expensive aggregations once and store for fast BI reads',
    code: `-- Pre-aggregate sales at daily × region × category
CREATE OR REPLACE TABLE gold.agg.daily_sales_agg
USING DELTA
PARTITIONED BY (order_date)
AS
SELECT
  order_date, region, product_category,
  SUM(net_revenue) AS revenue,
  COUNT(order_id)  AS orders,
  COUNT(DISTINCT customer_id) AS customers
FROM silver.sales.orders_clean
GROUP BY order_date, region, product_category;`,
  },
  {
    id: 22,
    group: 'Aggregation & Perf',
    title: 'Rollups',
    desc: 'Build multi-level rollup hierarchy from daily to yearly totals',
    code: `-- Rollup: daily → weekly → monthly → quarterly → yearly
CREATE OR REPLACE TABLE gold.agg.revenue_rollup AS
SELECT
  COALESCE(CAST(order_date AS STRING), 'ALL')                AS day_val,
  COALESCE(CAST(DATE_TRUNC('week',  order_date) AS STRING), 'ALL') AS week_val,
  COALESCE(CAST(DATE_TRUNC('month', order_date) AS STRING), 'ALL') AS month_val,
  SUM(net_revenue) AS revenue,
  GROUPING_ID(order_date) AS grouping_level
FROM silver.sales.orders_clean
GROUP BY ROLLUP(order_date);`,
  },
  {
    id: 23,
    group: 'Aggregation & Perf',
    title: 'Cubes OLAP',
    desc: 'OLAP cube over region, category and date for slice-and-dice queries',
    code: `-- OLAP cube: all dimension combinations
CREATE OR REPLACE TABLE gold.agg.sales_cube AS
SELECT
  COALESCE(region, '__ALL__')           AS region,
  COALESCE(product_category, '__ALL__') AS category,
  COALESCE(CAST(order_date AS STRING), '__ALL__') AS order_date,
  SUM(net_revenue) AS revenue,
  COUNT(order_id)  AS orders
FROM silver.sales.orders_clean
GROUP BY CUBE(region, product_category, order_date);`,
  },
  {
    id: 24,
    group: 'Aggregation & Perf',
    title: 'Materialized Views',
    desc: 'Create materialized view that auto-refreshes on upstream table changes',
    code: `-- Materialized view (Databricks SQL)
CREATE MATERIALIZED VIEW gold.mv.daily_revenue_mv AS
SELECT
  order_date,
  region,
  SUM(net_revenue)        AS revenue,
  COUNT(DISTINCT order_id) AS orders
FROM silver.sales.orders_clean
GROUP BY order_date, region;
-- Auto-refreshes on pipeline trigger or schedule
REFRESH MATERIALIZED VIEW gold.mv.daily_revenue_mv;`,
  },
  {
    id: 25,
    group: 'Aggregation & Perf',
    title: 'Summary Tables',
    desc: 'Persist summary tables for monthly and YTD financial metrics',
    code: `-- Summary table: monthly financial summary
CREATE OR REPLACE TABLE gold.agg.monthly_finance_summary AS
SELECT
  DATE_TRUNC('month', order_date) AS month,
  region,
  SUM(net_revenue)                AS mtd_revenue,
  SUM(SUM(net_revenue)) OVER (PARTITION BY region ORDER BY DATE_TRUNC('month', order_date)) AS ytd_revenue,
  COUNT(DISTINCT customer_id)     AS active_customers
FROM silver.sales.orders_clean
GROUP BY 1, 2;`,
  },
  {
    id: 26,
    group: 'Aggregation & Perf',
    title: 'Data Denormalization',
    desc: 'Flatten star schema into wide denormalized table for BI tools',
    code: `-- Denormalized wide table for Power BI / Tableau
CREATE OR REPLACE TABLE gold.wide.sales_wide AS
SELECT
  f.order_id, f.order_date, f.net_revenue,
  c.customer_name, c.segment, c.region, c.country,
  p.product_name, p.category, p.subcategory,
  s.store_name, s.city,
  d.fiscal_year, d.fiscal_quarter, d.week_of_year
FROM gold.marts.fact_sales        f
JOIN gold.marts.dim_customer      c ON f.customer_key = c.customer_key AND c.is_current
JOIN gold.marts.dim_product       p ON f.product_key  = p.product_key
JOIN gold.marts.dim_store         s ON f.store_key    = s.store_key
JOIN gold.marts.dim_date          d ON f.date_key     = d.date_key;`,
  },
  {
    id: 27,
    group: 'Aggregation & Perf',
    title: 'Query Optimization',
    desc: 'Apply Delta optimization techniques to speed up Gold table reads',
    code: `-- Optimize Gold tables for query performance
OPTIMIZE gold.marts.fact_sales ZORDER BY (order_date, customer_key);
OPTIMIZE gold.marts.dim_customer ZORDER BY (customer_key);

-- Collect statistics for query optimizer
ANALYZE TABLE gold.marts.fact_sales COMPUTE STATISTICS FOR ALL COLUMNS;

-- Check table statistics
DESCRIBE DETAIL gold.marts.fact_sales;`,
  },
  {
    id: 28,
    group: 'Aggregation & Perf',
    title: 'Partition Tuning',
    desc: 'Re-partition Gold tables by date to minimise files-per-partition',
    code: `-- Re-partition fact table by month for efficient range scans
CREATE OR REPLACE TABLE gold.marts.fact_sales_partitioned
USING DELTA
PARTITIONED BY (order_year, order_month)
AS
SELECT *, YEAR(order_date) AS order_year, MONTH(order_date) AS order_month
FROM gold.marts.fact_sales;

-- Verify partition stats
SELECT order_year, order_month, COUNT(*) AS rows
FROM gold.marts.fact_sales_partitioned
GROUP BY 1, 2 ORDER BY 1, 2;`,
  },
  {
    id: 29,
    group: 'Aggregation & Perf',
    title: 'Indexing Strategies',
    desc: 'Configure Bloom filters and Z-order indexes on high-cardinality columns',
    code: `-- Bloom filter index on customer_id for point-lookup queries
CREATE BLOOMFILTER INDEX ON TABLE gold.marts.fact_sales
  FOR COLUMNS(customer_id OPTIONS (fpp=0.01));

-- Z-order on composite filter columns
OPTIMIZE gold.cx_mart.customer_360
  ZORDER BY (segment, region, churn_risk_score);

-- Liquid clustering (Databricks Runtime 13+)
ALTER TABLE gold.agg.daily_sales_agg
  CLUSTER BY (order_date, region);`,
  },
  {
    id: 30,
    group: 'Aggregation & Perf',
    title: 'Cache Optimization',
    desc: 'Cache hot Gold tables in Databricks SQL warehouse result cache',
    code: `-- Cache hot aggregation table in memory
CACHE SELECT * FROM gold.agg.daily_sales_agg
WHERE order_date >= CURRENT_DATE - INTERVAL 90 DAYS;

-- Enable Delta caching on cluster
-- (Set spark.databricks.io.cache.enabled=true in cluster config)

-- Monitor cache hit rate
SELECT metric_name, metric_value
FROM system.compute.query_metrics
WHERE metric_name IN ('diskReadBytes', 'cacheReadBytes')
ORDER BY query_start_time DESC LIMIT 20;`,
  },

  // ─── 31–40: BI/Reporting ───
  {
    id: 31,
    group: 'BI/Reporting',
    title: 'Dashboard Dataset Creation',
    desc: 'Build a purpose-built dataset optimized for executive dashboard reads',
    code: `-- Dashboard dataset: revenue snapshot for C-suite dashboard
CREATE OR REPLACE TABLE gold.bi.exec_dashboard_ds AS
SELECT
  order_date, region, product_category,
  SUM(net_revenue)         AS revenue,
  COUNT(DISTINCT order_id) AS orders,
  AVG(net_revenue)         AS aov,
  SUM(net_revenue) - LAG(SUM(net_revenue)) OVER (PARTITION BY region ORDER BY order_date) AS rev_delta
FROM silver.sales.orders_clean
GROUP BY 1, 2, 3;`,
  },
  {
    id: 32,
    group: 'BI/Reporting',
    title: 'Report Data Preparation',
    desc: 'Prepare clean, formatted dataset for monthly business review report',
    code: `-- Monthly business review report dataset
CREATE OR REPLACE TABLE gold.bi.mbr_dataset AS
SELECT
  DATE_FORMAT(order_date, 'yyyy-MM') AS report_month,
  region, product_category,
  ROUND(SUM(net_revenue), 2)         AS revenue,
  COUNT(DISTINCT order_id)           AS orders,
  COUNT(DISTINCT customer_id)        AS customers,
  ROUND(SUM(net_revenue) / COUNT(DISTINCT order_id), 2) AS aov,
  ROUND(SUM(net_revenue) / SUM(SUM(net_revenue)) OVER (PARTITION BY DATE_FORMAT(order_date,'yyyy-MM')) * 100, 1) AS revenue_share_pct
FROM silver.sales.orders_clean
GROUP BY 1, 2, 3;`,
  },
  {
    id: 33,
    group: 'BI/Reporting',
    title: 'KPI Dashboard Feeds',
    desc: 'Low-latency KPI feed table refreshed every 15 minutes for live tiles',
    code: `-- Near-real-time KPI feed (15-min micro-batch)
CREATE OR REPLACE TABLE gold.bi.kpi_live_feed AS
SELECT
  current_timestamp() AS refreshed_at,
  SUM(net_revenue)    AS revenue_today,
  COUNT(order_id)     AS orders_today,
  COUNT(DISTINCT customer_id) AS customers_today,
  SUM(net_revenue) / COUNT(order_id) AS aov_today
FROM silver.sales.orders_clean
WHERE order_date = current_date();`,
  },
  {
    id: 34,
    group: 'BI/Reporting',
    title: 'Drill-Down Capability',
    desc: 'Hierarchical dataset enabling country → region → city drill-down',
    code: `-- Drill-down dataset: country > region > city
CREATE OR REPLACE TABLE gold.bi.geo_drilldown AS
SELECT
  country, region, city,
  SUM(net_revenue)         AS revenue,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(DISTINCT customer_id) AS customers
FROM silver.sales.orders_clean o
JOIN silver.customers.profiles c USING (customer_id)
GROUP BY GROUPING SETS (
  (country, region, city),
  (country, region),
  (country),
  ()
);`,
  },
  {
    id: 35,
    group: 'BI/Reporting',
    title: 'Drill-Through Datasets',
    desc: 'Detail-level dataset backing drill-through from summary to line items',
    code: `-- Drill-through: from summary tile to order detail
CREATE OR REPLACE TABLE gold.bi.order_drillthrough AS
SELECT
  o.order_id, o.order_date, o.customer_id,
  c.customer_name, c.segment,
  o.product_id, p.product_name, p.category,
  o.quantity, o.unit_price, o.net_revenue,
  o.store_id, s.store_name, s.region
FROM silver.sales.orders_clean o
JOIN silver.customers.profiles  c USING (customer_id)
JOIN silver.products.product_clean p USING (product_id)
JOIN silver.stores.store_clean     s USING (store_id)
WHERE o.order_date >= CURRENT_DATE - INTERVAL 365 DAYS;`,
  },
  {
    id: 36,
    group: 'BI/Reporting',
    title: 'Self-Service BI',
    desc: 'Expose curated wide tables with row-level security for self-service',
    code: `-- Self-service BI: wide table with RLS
CREATE OR REPLACE TABLE gold.bi.self_service_sales AS
SELECT * FROM gold.wide.sales_wide;

-- Row-level security: users see their own region only
ALTER TABLE gold.bi.self_service_sales
  ADD ROW FILTER catalog.filters.region_filter
  ON (region);

-- Grant read to BI users group
GRANT SELECT ON TABLE gold.bi.self_service_sales TO \`bi_users\`;`,
  },
  {
    id: 37,
    group: 'BI/Reporting',
    title: 'Data API Exposure',
    desc: 'Expose Gold table through Databricks SQL REST API for downstream apps',
    code: `-- Data API: use Databricks SQL Statement API
-- POST /api/2.0/sql/statements
-- {
--   "warehouse_id": "<sql_warehouse_id>",
--   "statement": "SELECT * FROM gold.bi.exec_dashboard_ds WHERE order_date = :dt",
--   "parameters": [{"name": "dt", "value": "2024-01-15", "type": "DATE"}]
-- }

-- Verify table is accessible via API
SELECT COUNT(*) FROM gold.bi.exec_dashboard_ds;
DESCRIBE gold.bi.exec_dashboard_ds;`,
  },
  {
    id: 38,
    group: 'BI/Reporting',
    title: 'Scheduled Refresh',
    desc: 'Configure Delta Live Table pipeline or workflow to refresh Gold tables',
    code: `-- Schedule Gold table refresh via Databricks Workflow
-- (Triggered by Silver pipeline completion event)

-- Refresh materialized view
REFRESH MATERIALIZED VIEW gold.mv.daily_revenue_mv;

-- Incremental merge for BI dataset
MERGE INTO gold.bi.exec_dashboard_ds AS target
USING (SELECT * FROM silver.sales.orders_clean
       WHERE order_date >= current_date() - INTERVAL 2 DAYS) AS src
ON target.order_date = src.order_date AND target.region = src.region
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;`,
  },
  {
    id: 39,
    group: 'BI/Reporting',
    title: 'Data Extracts',
    desc: 'Export Gold table to Parquet/CSV for offline BI tools and stakeholders',
    code: `-- Export Gold data to ADLS for Power BI dataflow ingestion
COPY INTO 'abfss://exports@datalake.dfs.core.windows.net/gold/monthly_finance/'
FROM (
  SELECT * FROM gold.agg.monthly_finance_summary
  WHERE month >= ADD_MONTHS(CURRENT_DATE(), -3)
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('compression' = 'snappy')
COPY_OPTIONS ('mergeSchema' = 'true');`,
  },
  {
    id: 40,
    group: 'BI/Reporting',
    title: 'Visualization Optimization',
    desc: 'Optimise Gold table column ordering and types for fast BI rendering',
    code: `-- Optimize column types for BI tool compatibility
CREATE OR REPLACE TABLE gold.bi.pbi_optimized AS
SELECT
  CAST(order_date AS DATE)           AS order_date,
  CAST(region AS STRING)             AS region,
  CAST(product_category AS STRING)   AS category,
  CAST(SUM(net_revenue) AS DECIMAL(18,2)) AS revenue,
  CAST(COUNT(order_id) AS BIGINT)    AS orders
FROM silver.sales.orders_clean
GROUP BY 1, 2, 3
ORDER BY order_date DESC, region;

OPTIMIZE gold.bi.pbi_optimized ZORDER BY (order_date);`,
  },

  // ─── 41–50: Analytics & AI ───
  {
    id: 41,
    group: 'Analytics & AI',
    title: 'ML Feature Serving',
    desc: 'Publish computed ML features from Feature Store for online serving',
    code: `# Publish features to Databricks Feature Store
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

fe.create_table(
    name='gold.feature_store.customer_features',
    primary_keys=['customer_id'],
    df=spark.table('silver.ml.customer_features_clean'),
    description='Customer features for churn and LTV models'
)

# Materialize online features to DynamoDB/CosmosDB
fe.publish_table(name='gold.feature_store.customer_features', online_store=online_store_spec)`,
  },
  {
    id: 42,
    group: 'Analytics & AI',
    title: 'Prediction Output Tables',
    desc: 'Store batch ML model predictions with model version and timestamp',
    code: `-- Prediction output: churn probability scores
CREATE OR REPLACE TABLE gold.ml.churn_predictions AS
SELECT
  customer_id,
  prediction_date,
  churn_probability,
  CASE WHEN churn_probability >= 0.7 THEN 'HIGH'
       WHEN churn_probability >= 0.4 THEN 'MEDIUM'
       ELSE 'LOW' END AS churn_segment,
  model_version,
  current_timestamp() AS scored_at
FROM (
  SELECT customer_id, current_date() AS prediction_date,
    ai_query('churn_model_endpoint', to_json(struct(*))) AS churn_probability,
    '2.1.0' AS model_version
  FROM gold.feature_store.customer_features
);`,
  },
  {
    id: 43,
    group: 'Analytics & AI',
    title: 'Forecast Datasets',
    desc: 'Time-series forecast output table for demand planning teams',
    code: `-- Demand forecast output table
CREATE OR REPLACE TABLE gold.ml.demand_forecast AS
SELECT
  product_id, warehouse_id,
  forecast_date,
  predicted_units,
  lower_bound_95,
  upper_bound_95,
  model_name,
  mape_score,
  run_date
FROM silver.ml.forecast_raw
WHERE run_date = (SELECT MAX(run_date) FROM silver.ml.forecast_raw)
  AND forecast_horizon_days <= 90;`,
  },
  {
    id: 44,
    group: 'Analytics & AI',
    title: 'Recommendation Datasets',
    desc: 'Product recommendation scores per customer for personalisation engine',
    code: `-- Recommendation table: top-10 products per customer
CREATE OR REPLACE TABLE gold.ml.product_recommendations AS
SELECT customer_id, product_id, rank, score, model_version, scored_at
FROM (
  SELECT
    customer_id, product_id,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY score DESC) AS rank,
    score, model_version, current_timestamp() AS scored_at
  FROM silver.ml.collab_filter_scores
)
WHERE rank <= 10;`,
  },
  {
    id: 45,
    group: 'Analytics & AI',
    title: 'Churn Analytics',
    desc: 'Churn cohort analysis combining predictions with actuals for model validation',
    code: `-- Churn analytics: predicted vs actual
CREATE OR REPLACE TABLE gold.analytics.churn_validation AS
SELECT
  p.customer_id, p.churn_segment, p.churn_probability,
  c.churned, c.churn_date,
  CASE WHEN p.churn_segment = 'HIGH' AND c.churned = TRUE  THEN 'TP'
       WHEN p.churn_segment = 'HIGH' AND c.churned = FALSE THEN 'FP'
       WHEN p.churn_segment = 'LOW'  AND c.churned = TRUE  THEN 'FN'
       ELSE 'TN' END AS confusion_label
FROM gold.ml.churn_predictions p
LEFT JOIN silver.customers.churn_actuals c USING (customer_id);`,
  },
  {
    id: 46,
    group: 'Analytics & AI',
    title: 'Fraud Analytics',
    desc: 'Fraud detection model scores with review queue prioritisation',
    code: `-- Fraud scoring output and review queue
CREATE OR REPLACE TABLE gold.ml.fraud_scores AS
SELECT
  transaction_id, account_id, transaction_date,
  fraud_probability,
  fraud_reason_codes,
  CASE WHEN fraud_probability >= 0.9 THEN 'AUTO_BLOCK'
       WHEN fraud_probability >= 0.6 THEN 'MANUAL_REVIEW'
       ELSE 'PASS' END AS action,
  model_version, scored_at
FROM silver.ml.fraud_raw_scores
WHERE scored_at >= current_timestamp() - INTERVAL 24 HOURS;`,
  },
  {
    id: 47,
    group: 'Analytics & AI',
    title: 'Risk Scoring Tables',
    desc: 'Credit and operational risk scores consolidated for risk dashboard',
    code: `-- Risk scoring: combined risk index
CREATE OR REPLACE TABLE gold.risk.risk_scores AS
SELECT
  entity_id, entity_type,
  credit_score, fraud_score, operational_score,
  (credit_score * 0.4 + fraud_score * 0.4 + operational_score * 0.2) AS composite_risk,
  CASE WHEN composite_risk >= 0.8 THEN 'CRITICAL'
       WHEN composite_risk >= 0.6 THEN 'HIGH'
       WHEN composite_risk >= 0.4 THEN 'MEDIUM'
       ELSE 'LOW' END AS risk_tier,
  score_date
FROM silver.risk.scores_combined;`,
  },
  {
    id: 48,
    group: 'Analytics & AI',
    title: 'Customer Segmentation',
    desc: 'K-means cluster output table for marketing segment activation',
    code: `-- Customer segmentation output
CREATE OR REPLACE TABLE gold.ml.customer_segments AS
SELECT
  customer_id,
  cluster_id,
  CASE cluster_id
    WHEN 0 THEN 'Champions'
    WHEN 1 THEN 'Loyal'
    WHEN 2 THEN 'At-Risk'
    WHEN 3 THEN 'Lost'
    ELSE 'New' END AS segment_label,
  rfm_score, cluster_distance, model_version, run_date
FROM silver.ml.kmeans_output
WHERE run_date = (SELECT MAX(run_date) FROM silver.ml.kmeans_output);`,
  },
  {
    id: 49,
    group: 'Analytics & AI',
    title: 'A/B Testing Datasets',
    desc: 'Experiment analysis dataset with treatment assignments and outcomes',
    code: `-- A/B test results: checkout flow experiment
CREATE OR REPLACE TABLE gold.analytics.ab_test_results AS
SELECT
  e.experiment_id, e.variant, u.user_id,
  u.conversion_flag, u.revenue, u.session_count,
  AVG(u.conversion_flag) OVER (PARTITION BY e.experiment_id, e.variant) AS variant_cvr,
  AVG(u.revenue)         OVER (PARTITION BY e.experiment_id, e.variant) AS variant_arpu
FROM silver.experiments.assignments e
JOIN silver.experiments.outcomes     u USING (user_id, experiment_id)
WHERE e.experiment_status = 'concluded';`,
  },
  {
    id: 50,
    group: 'Analytics & AI',
    title: 'AI Feedback Loop',
    desc: 'Capture user feedback on AI outputs and feed back into retraining pipeline',
    code: `-- AI feedback loop: store thumbs up/down on AI responses
CREATE OR REPLACE TABLE gold.ml.ai_feedback AS
SELECT
  request_id, user_id, model_endpoint,
  input_hash, response_hash,
  feedback_label,  -- 'positive', 'negative', 'neutral'
  feedback_reason,
  feedback_timestamp,
  DATE_TRUNC('week', feedback_timestamp) AS feedback_week
FROM silver.ai.user_feedback_clean
WHERE feedback_timestamp >= current_timestamp() - INTERVAL 90 DAYS;`,
  },

  // ─── 51–60: RAG/GenAI Gold ───
  {
    id: 51,
    group: 'RAG/GenAI Gold',
    title: 'Curated Knowledge Datasets',
    desc: 'Curated and deduplicated document corpus for RAG knowledge base',
    code: `-- Curated knowledge base: deduplicated, quality-filtered docs
CREATE OR REPLACE TABLE gold.rag.knowledge_base AS
SELECT
  doc_id, source_system, doc_type,
  title, clean_content AS content,
  quality_score, word_count,
  last_updated, author
FROM silver.docs.documents_clean
WHERE quality_score >= 0.7
  AND word_count BETWEEN 50 AND 10000
  AND is_duplicate = FALSE
  AND status = 'published';`,
  },
  {
    id: 52,
    group: 'RAG/GenAI Gold',
    title: 'Ranked Document Sets',
    desc: 'BM25 + semantic relevance ranked document sets for retrieval tuning',
    code: `-- Ranked documents: combine BM25 + semantic scores
CREATE OR REPLACE TABLE gold.rag.ranked_docs AS
SELECT
  query_id, doc_id,
  bm25_score, semantic_score,
  (bm25_score * 0.3 + semantic_score * 0.7) AS combined_score,
  ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY (bm25_score * 0.3 + semantic_score * 0.7) DESC) AS rank,
  retrieved_at
FROM silver.rag.retrieval_scores
WHERE retrieved_at >= current_date() - INTERVAL 30 DAYS;`,
  },
  {
    id: 53,
    group: 'RAG/GenAI Gold',
    title: 'Context-Optimized Datasets',
    desc: 'Window-optimised chunk sets fitting LLM context window constraints',
    code: `-- Context-optimised chunks: max 4096 tokens per context window
CREATE OR REPLACE TABLE gold.rag.context_chunks AS
SELECT
  query_id,
  COLLECT_LIST(STRUCT(doc_id, chunk_text, chunk_tokens)) AS context_docs,
  SUM(chunk_tokens)     AS total_tokens,
  COUNT(doc_id)         AS chunk_count
FROM silver.rag.chunks_with_scores
WHERE chunk_tokens <= 512
GROUP BY query_id
HAVING SUM(chunk_tokens) <= 4096;`,
  },
  {
    id: 54,
    group: 'RAG/GenAI Gold',
    title: 'Response Validation',
    desc: 'Store LLM response quality scores and hallucination detection flags',
    code: `-- Response validation: faithfulness and relevance scores
CREATE OR REPLACE TABLE gold.rag.response_quality AS
SELECT
  request_id, query_text,
  response_text,
  faithfulness_score,      -- 0-1: is answer supported by context?
  answer_relevance_score,  -- 0-1: does it answer the question?
  context_recall_score,    -- 0-1: did retrieval get the right docs?
  hallucination_flag,
  latency_ms,
  model_id, run_date
FROM silver.rag.eval_results_clean
WHERE run_date >= current_date() - INTERVAL 7 DAYS;`,
  },
  {
    id: 55,
    group: 'RAG/GenAI Gold',
    title: 'Prompt Evaluation Datasets',
    desc: 'Prompt variant performance comparison for prompt engineering teams',
    code: `-- Prompt evaluation: A/B comparison of prompt templates
CREATE OR REPLACE TABLE gold.rag.prompt_eval AS
SELECT
  prompt_variant_id, prompt_template,
  AVG(faithfulness_score)      AS avg_faithfulness,
  AVG(answer_relevance_score)  AS avg_relevance,
  AVG(latency_ms)              AS avg_latency_ms,
  COUNT(request_id)            AS request_count,
  SUM(hallucination_flag)      AS hallucination_count
FROM gold.rag.response_quality
GROUP BY prompt_variant_id, prompt_template;`,
  },
  {
    id: 56,
    group: 'RAG/GenAI Gold',
    title: 'Ground Truth Datasets',
    desc: 'Human-labelled question-answer pairs for RAG evaluation benchmarking',
    code: `-- Ground truth QA pairs for offline evaluation
CREATE OR REPLACE TABLE gold.rag.ground_truth AS
SELECT
  gt.question_id,
  gt.question_text,
  gt.expected_answer,
  gt.source_doc_ids,
  gt.difficulty_level,  -- 'easy', 'medium', 'hard'
  gt.domain,
  gt.created_by, gt.validated_by,
  gt.created_date
FROM silver.rag.human_labels gt
WHERE gt.validation_status = 'approved'
  AND gt.is_ambiguous = FALSE;`,
  },
  {
    id: 57,
    group: 'RAG/GenAI Gold',
    title: 'Answer Caching Tables',
    desc: 'Cache frequent query-answer pairs to reduce LLM API costs',
    code: `-- Answer cache: store high-confidence responses for reuse
CREATE OR REPLACE TABLE gold.rag.answer_cache AS
SELECT
  query_hash,
  query_text,
  cached_response,
  context_doc_ids,
  avg_confidence_score,
  cache_hit_count,
  first_cached_at,
  last_used_at
FROM silver.rag.cached_responses
WHERE avg_confidence_score >= 0.9
  AND cache_hit_count >= 3
  AND last_used_at >= current_date() - INTERVAL 14 DAYS;`,
  },
  {
    id: 58,
    group: 'RAG/GenAI Gold',
    title: 'Retrieval Optimization',
    desc: 'Analyse retrieval recall and precision to tune vector search thresholds',
    code: `-- Retrieval optimisation: recall@K and precision@K
CREATE OR REPLACE TABLE gold.rag.retrieval_metrics AS
SELECT
  model_id, index_version,
  AVG(recall_at_1)    AS recall_at_1,
  AVG(recall_at_5)    AS recall_at_5,
  AVG(recall_at_10)   AS recall_at_10,
  AVG(precision_at_5) AS precision_at_5,
  AVG(mrr)            AS mean_reciprocal_rank,
  COUNT(query_id)     AS query_count,
  eval_date
FROM silver.rag.retrieval_eval_results
GROUP BY model_id, index_version, eval_date;`,
  },
  {
    id: 59,
    group: 'RAG/GenAI Gold',
    title: 'Vector Metadata Enrichment',
    desc: 'Enrich vector store metadata with business tags for filtered retrieval',
    code: `-- Enrich vector metadata with business taxonomy
CREATE OR REPLACE TABLE gold.rag.vector_metadata AS
SELECT
  v.chunk_id, v.doc_id,
  v.embedding_model, v.vector_dim,
  k.knowledge_domain, k.doc_type, k.product_line,
  k.region, k.language, k.publish_year,
  k.access_tier,  -- 'public', 'internal', 'confidential'
  k.expiry_date
FROM silver.rag.vector_index_meta   v
JOIN gold.rag.knowledge_base        k USING (doc_id);`,
  },
  {
    id: 60,
    group: 'RAG/GenAI Gold',
    title: 'Explainability Datasets',
    desc: 'Store attribution scores linking LLM answers to source chunks',
    code: `-- Explainability: which chunks drove each answer
CREATE OR REPLACE TABLE gold.rag.answer_attribution AS
SELECT
  request_id,
  answer_sentence_id,
  doc_id, chunk_id,
  attribution_score,     -- 0-1: how much did this chunk contribute?
  highlight_span,        -- character offsets in source text
  attribution_method,    -- 'attention', 'gradient', 'lime'
  model_version
FROM silver.rag.attribution_results
WHERE attribution_score >= 0.1;`,
  },

  // ─── 61–70: Security & Gov ───
  {
    id: 61,
    group: 'Security & Gov',
    title: 'Final Access Control',
    desc: 'Apply least-privilege access grants on all Gold tables',
    code: `-- Final access control: Gold layer permissions
REVOKE ALL PRIVILEGES ON SCHEMA gold.marts FROM \`data_engineers\`;
GRANT SELECT ON SCHEMA gold.marts TO \`analysts\`;
GRANT SELECT ON SCHEMA gold.bi    TO \`bi_users\`;
GRANT SELECT ON SCHEMA gold.ml    TO \`data_scientists\`;

-- Service accounts: narrow access
GRANT SELECT ON TABLE gold.bi.exec_dashboard_ds TO \`powerbi_sp\`;
GRANT SELECT ON TABLE gold.ml.churn_predictions TO \`crm_integration_sp\`;`,
  },
  {
    id: 62,
    group: 'Security & Gov',
    title: 'Row-Level Security',
    desc: 'Enforce row-level security so managers see only their region data',
    code: `-- Row-level security: region-based filter
CREATE FUNCTION catalog.filters.region_rls(region STRING)
RETURNS BOOLEAN
RETURN region = current_user_region()
  OR IS_MEMBER('data_admins');

ALTER TABLE gold.bi.self_service_sales
  ADD ROW FILTER catalog.filters.region_rls ON (region);

-- Test: user in APAC sees only APAC rows
SELECT DISTINCT region FROM gold.bi.self_service_sales;`,
  },
  {
    id: 63,
    group: 'Security & Gov',
    title: 'Column-Level Security',
    desc: 'Restrict PII columns to authorised users only via column masking',
    code: `-- Column-level security: mask email for non-PII roles
CREATE FUNCTION catalog.masks.email_mask(email STRING)
RETURNS STRING
RETURN CASE WHEN IS_MEMBER('pii_access') THEN email
            ELSE REGEXP_REPLACE(email, '(.).+(@.+)', '$1***$2') END;

ALTER TABLE gold.cx_mart.customer_360
  ALTER COLUMN email SET MASK catalog.masks.email_mask;

-- Test: non-PII user sees masked emails
SELECT customer_id, email FROM gold.cx_mart.customer_360 LIMIT 5;`,
  },
  {
    id: 64,
    group: 'Security & Gov',
    title: 'Data Masking Final',
    desc: 'Apply production data masking policies on all sensitive Gold columns',
    code: `-- Apply masking to multiple sensitive columns
ALTER TABLE gold.hr_mart.workforce_summary
  ALTER COLUMN salary      SET MASK catalog.masks.salary_mask;
ALTER TABLE gold.risk_mart.credit_exposure
  ALTER COLUMN ssn         SET MASK catalog.masks.ssn_mask;
ALTER TABLE gold.cx_mart.customer_360
  ALTER COLUMN phone_number SET MASK catalog.masks.phone_mask;

-- Verify masking policies applied
SELECT column_name, mask_name
FROM information_schema.column_masks
WHERE table_schema = 'gold';`,
  },
  {
    id: 65,
    group: 'Security & Gov',
    title: 'Data Certification',
    desc: 'Tag Gold tables as certified to signal data quality to consumers',
    code: `-- Certify Gold tables via tags
ALTER TABLE gold.marts.fact_sales
  SET TAGS ('certified' = 'true', 'certified_by' = 'data_governance',
            'certified_date' = '2024-01-15', 'sla_tier' = 'gold');

ALTER TABLE gold.bi.exec_dashboard_ds
  SET TAGS ('certified' = 'true', 'owner' = 'finance_team',
            'refresh_cadence' = 'daily', 'pii' = 'false');

-- View certified tables
SELECT table_name, tag_name, tag_value
FROM information_schema.table_tags
WHERE tag_name = 'certified' AND tag_value = 'true';`,
  },
  {
    id: 66,
    group: 'Security & Gov',
    title: 'Data Ownership Enforcement',
    desc: 'Assign data stewards and owners to all Gold tables',
    code: `-- Data ownership via tags and comments
COMMENT ON TABLE gold.marts.fact_sales IS 'Owner: data_engineering. Steward: john.smith@company.com. Domain: Sales';
COMMENT ON TABLE gold.finance_mart.pnl_summary IS 'Owner: finance_team. Steward: jane.doe@company.com. Domain: Finance';

-- Tag all gold tables with owner
ALTER TABLE gold.cx_mart.customer_360
  SET TAGS ('data_owner' = 'cx_team', 'data_steward' = 'cx-data@company.com');

SELECT table_name, comment FROM information_schema.tables
WHERE table_schema LIKE 'gold%' AND comment IS NOT NULL;`,
  },
  {
    id: 67,
    group: 'Security & Gov',
    title: 'Audit Reporting',
    desc: 'Query Unity Catalog audit logs to generate data access reports',
    code: `-- Audit report: who accessed Gold tables in last 30 days
SELECT
  user_identity.email   AS user_email,
  request_params.table  AS table_name,
  action_name,
  COUNT(*)              AS access_count,
  MIN(event_time)       AS first_access,
  MAX(event_time)       AS last_access
FROM system.access.audit
WHERE event_time >= current_date() - INTERVAL 30 DAYS
  AND request_params.table LIKE 'gold%'
  AND action_name IN ('SELECT', 'DESCRIBE')
GROUP BY 1, 2, 3
ORDER BY access_count DESC;`,
  },
  {
    id: 68,
    group: 'Security & Gov',
    title: 'Data Lineage Exposure',
    desc: 'Expose column-level lineage from Bronze through to Gold for governance',
    code: `-- Query Unity Catalog lineage for Gold table
SELECT
  source_table_full_name,
  target_table_full_name,
  source_column_name,
  target_column_name,
  transformation_type
FROM system.access.column_lineage
WHERE target_table_full_name = 'main.gold.marts.fact_sales'
ORDER BY target_column_name, source_table_full_name;`,
  },
  {
    id: 69,
    group: 'Security & Gov',
    title: 'Compliance Reporting',
    desc: 'GDPR data subject report: all PII held for a given customer',
    code: `-- GDPR Subject Access Request (SAR) report
CREATE OR REPLACE TEMP VIEW gdpr_subject_data AS
SELECT 'customer_360'  AS source_table, customer_id, email, phone_number, NULL AS card_last4 FROM gold.cx_mart.customer_360   WHERE customer_id = :subject_id
UNION ALL
SELECT 'order_detail', customer_id, NULL, NULL, NULL FROM gold.bi.order_drillthrough WHERE customer_id = :subject_id
UNION ALL
SELECT 'churn_score',  customer_id, NULL, NULL, NULL FROM gold.ml.churn_predictions WHERE customer_id = :subject_id;
SELECT * FROM gdpr_subject_data;`,
  },
  {
    id: 70,
    group: 'Security & Gov',
    title: 'Secure Data Sharing',
    desc: 'Delta Sharing: share curated Gold datasets with external partners',
    code: `-- Secure Delta Sharing of Gold datasets
CREATE SHARE IF NOT EXISTS partner_analytics_share;
ALTER SHARE partner_analytics_share
  ADD TABLE gold.bi.exec_dashboard_ds PARTITION (region = 'EMEA');
ALTER SHARE partner_analytics_share
  ADD TABLE gold.agg.monthly_finance_summary;

CREATE RECIPIENT emea_partner COMMENT 'EMEA analytics partner';
GRANT SELECT ON SHARE partner_analytics_share TO RECIPIENT emea_partner;

-- Monitor share usage
SELECT * FROM system.access.audit WHERE action_name = 'deltaSharing.QueryTable';`,
  },

  // ─── 71–80: Data Consumption ───
  {
    id: 71,
    group: 'Data Consumption',
    title: 'API Data Serving',
    desc: 'Serve Gold table data via Databricks SQL Statement API to web apps',
    code: `-- API serving pattern: parameterised SQL via REST
-- Endpoint: POST /api/2.0/sql/statements
-- Payload:
-- {
--   "warehouse_id": "<wh_id>",
--   "statement": "SELECT * FROM gold.bi.kpi_live_feed",
--   "wait_timeout": "10s"
-- }
-- Poll: GET /api/2.0/sql/statements/{statement_id}

SELECT * FROM gold.bi.kpi_live_feed
WHERE refreshed_at >= current_timestamp() - INTERVAL 15 MINUTES;`,
  },
  {
    id: 72,
    group: 'Data Consumption',
    title: 'BI Tool Integration',
    desc: 'Configure Power BI DirectQuery over Gold tables via JDBC connector',
    code: `-- Power BI DirectQuery optimisation
-- 1. Use SQL Warehouse (serverless) as endpoint
-- 2. Create optimised view for PBI
CREATE OR REPLACE VIEW gold.bi.pbi_sales_view AS
SELECT
  CAST(order_date AS DATE)         AS Date,
  region                           AS Region,
  product_category                 AS Category,
  CAST(SUM(net_revenue) AS DOUBLE) AS Revenue,
  COUNT(order_id)                  AS Orders
FROM silver.sales.orders_clean
GROUP BY 1, 2, 3;
-- 3. Grant to PowerBI service principal
GRANT SELECT ON VIEW gold.bi.pbi_sales_view TO \`powerbi_sp\`;`,
  },
  {
    id: 73,
    group: 'Data Consumption',
    title: 'Delta Sharing',
    desc: 'Share live Gold Delta tables with downstream consumers without data copy',
    code: `-- Delta Sharing: live data sharing
CREATE SHARE IF NOT EXISTS internal_analytics;
ALTER SHARE internal_analytics ADD TABLE gold.marts.fact_sales;
ALTER SHARE internal_analytics ADD TABLE gold.agg.daily_sales_agg;

-- Internal recipient: data science workspace
CREATE RECIPIENT ds_workspace COMMENT 'Internal data science team';
GRANT SELECT ON SHARE internal_analytics TO RECIPIENT ds_workspace;

-- Recipient fetches activation link and uses it in their workspace
SHOW RECIPIENTS;`,
  },
  {
    id: 74,
    group: 'Data Consumption',
    title: 'Microservices Integration',
    desc: 'Expose Gold lookup tables to microservices via online feature store',
    code: `# Publish Gold table to online feature store for microservice reads
from databricks.feature_engineering import FeatureEngineeringClient, OnlineStoreSpec
fe = FeatureEngineeringClient()

online_store = OnlineStoreSpec(
    online_store_type='cosmosdb',
    host='<cosmos_account>.documents.azure.com',
    port=443, container_name='customer_features'
)
fe.publish_table(
    name='gold.feature_store.customer_features',
    online_store=online_store,
    streaming=True
)`,
  },
  {
    id: 75,
    group: 'Data Consumption',
    title: 'Real-Time Serving',
    desc: 'Stream Gold table changes via Delta Change Data Feed to Kafka topic',
    code: `# Real-time serving: stream Gold CDC to Kafka
from pyspark.sql.functions import col
df = spark.readStream.format('delta') \
    .option('readChangeFeed', 'true') \
    .option('startingVersion', 'latest') \
    .table('gold.ml.churn_predictions')

df.filter(col('_change_type').isin(['insert', 'update_postimage'])) \
  .writeStream.format('kafka') \
  .option('kafka.bootstrap.servers', 'broker:9092') \
  .option('topic', 'gold.churn_predictions') \
  .option('checkpointLocation', '/checkpoints/gold_churn') \
  .start()`,
  },
  {
    id: 76,
    group: 'Data Consumption',
    title: 'Data Export',
    desc: 'Scheduled export of Gold summary tables to Azure Blob for external use',
    code: `-- Scheduled Gold export to Azure Blob Storage
COPY INTO 'abfss://exports@company.dfs.core.windows.net/gold/finance/'
FROM (SELECT * FROM gold.finance_mart.pnl_summary
      WHERE fiscal_year = YEAR(current_date()))
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('compression' = 'snappy', 'mergeSchema' = 'true')
COPY_OPTIONS ('overwrite' = 'true');

-- Verify export
LIST 'abfss://exports@company.dfs.core.windows.net/gold/finance/';`,
  },
  {
    id: 77,
    group: 'Data Consumption',
    title: 'Embedded Analytics',
    desc: 'Embed Databricks SQL dashboard into internal portal via iframe + token',
    code: `-- Embedded analytics setup
-- 1. Create SQL dashboard on Gold tables
-- 2. Generate embed token via REST API
-- POST /api/2.0/token/create
-- {"comment": "embed-token-portal", "lifetime_seconds": 86400}

-- 3. Embed SQL widget query
SELECT order_date, SUM(net_revenue) AS revenue
FROM gold.bi.exec_dashboard_ds
WHERE order_date >= current_date() - INTERVAL 30 DAYS
GROUP BY order_date ORDER BY order_date;
-- 4. Use Personal Access Token in Authorization header`,
  },
  {
    id: 78,
    group: 'Data Consumption',
    title: 'External Reporting Feeds',
    desc: 'Push daily Gold summary data to external CRM and ERP systems via REST',
    code: `# Push Gold data to Salesforce CRM via REST API
import requests, json
from pyspark.sql.functions import col

summary = spark.table('gold.sales_mart.rep_attainment') \
    .filter(col('attainment_pct').isNotNull()) \
    .toPandas().to_dict('records')

for record in summary:
    requests.patch(
        f"https://company.salesforce.com/services/data/v58.0/sobjects/Rep_KPI__c/{record['rep_id']}",
        headers={'Authorization': f"Bearer {sf_token}", 'Content-Type': 'application/json'},
        data=json.dumps({'Attainment__c': record['attainment_pct']}),
        timeout=10
    )`,
  },
  {
    id: 79,
    group: 'Data Consumption',
    title: 'Data Virtualization',
    desc: 'Create federated virtual views across Gold tables and external systems',
    code: `-- Data virtualisation: federated view joining Gold + external Postgres
CREATE OR REPLACE VIEW gold.virtual.sales_enriched AS
SELECT
  g.order_date, g.customer_id, g.net_revenue,
  e.crm_segment, e.lifetime_value_crm, e.account_manager
FROM gold.marts.fact_sales                          g
JOIN postgres_external.crm.customer_profiles_ext    e
  ON g.customer_id = e.customer_id
WHERE g.order_date >= current_date() - INTERVAL 90 DAYS;`,
  },
  {
    id: 80,
    group: 'Data Consumption',
    title: 'Cross-Domain Integration',
    desc: 'Unified cross-domain view joining finance, sales and HR Gold marts',
    code: `-- Cross-domain integration: finance + sales + HR unified view
CREATE OR REPLACE VIEW gold.integrated.business_overview AS
SELECT
  f.fiscal_quarter,
  f.ebitda                          AS finance_ebitda,
  s.total_revenue                   AS sales_revenue,
  s.avg_attainment_pct              AS sales_attainment,
  h.headcount                       AS hc_total,
  ROUND(f.ebitda / h.headcount, 0)  AS ebitda_per_head
FROM gold.finance_mart.exec_summary  f
JOIN gold.sales_mart.exec_summary    s ON f.fiscal_quarter = s.fiscal_quarter
JOIN gold.hr_mart.exec_summary       h ON f.fiscal_quarter = h.fiscal_quarter;`,
  },

  // ─── 81–90: Monitoring ───
  {
    id: 81,
    group: 'Monitoring',
    title: 'KPI Monitoring',
    desc: 'Automated alert when daily revenue KPI deviates from expected range',
    code: `-- KPI monitoring: revenue anomaly detection
SELECT
  order_date,
  revenue,
  AVG(revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7,
  STDDEV(revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS std7,
  CASE WHEN ABS(revenue - AVG(revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))
            > 2 * STDDEV(revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
       THEN TRUE ELSE FALSE END AS is_anomaly
FROM gold.kpis.daily_kpis
ORDER BY order_date DESC;`,
  },
  {
    id: 82,
    group: 'Monitoring',
    title: 'SLA Tracking',
    desc: 'Track Gold pipeline SLA: table must be available by 06:00 UTC daily',
    code: `-- SLA tracking: check Gold tables updated within SLA window
SELECT
  table_name,
  MAX(last_modified) AS last_refresh,
  CASE WHEN MAX(last_modified) < current_date() + INTERVAL '6 hours'
       THEN 'SLA_BREACH' ELSE 'OK' END AS sla_status
FROM information_schema.tables t
WHERE table_schema LIKE 'gold%'
GROUP BY table_name
HAVING MAX(last_modified) < current_date() + INTERVAL '6 hours';`,
  },
  {
    id: 83,
    group: 'Monitoring',
    title: 'Usage Analytics',
    desc: 'Analyse which Gold tables are most queried and by whom',
    code: `-- Usage analytics: top accessed Gold tables
SELECT
  request_params.table          AS table_name,
  user_identity.email           AS user_email,
  COUNT(*)                      AS query_count,
  AVG(response_time_ms)         AS avg_latency_ms,
  SUM(bytes_scanned)            AS total_bytes_scanned
FROM system.access.audit
WHERE event_time >= current_date() - INTERVAL 30 DAYS
  AND request_params.table LIKE 'gold%'
  AND action_name = 'SELECT'
GROUP BY 1, 2
ORDER BY query_count DESC
LIMIT 50;`,
  },
  {
    id: 84,
    group: 'Monitoring',
    title: 'Performance Monitoring',
    desc: 'Monitor Gold query performance and surface slow queries in SQL warehouse',
    code: `-- Performance monitoring: slow queries on Gold tables
SELECT
  query_id, statement_text,
  user_name, warehouse_name,
  ROUND(duration / 1000, 1) AS duration_sec,
  rows_produced, bytes_read
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 7 DAYS
  AND statement_text LIKE '%gold%'
  AND duration > 30000  -- queries > 30 seconds
ORDER BY duration DESC
LIMIT 20;`,
  },
  {
    id: 85,
    group: 'Monitoring',
    title: 'Data Drift Monitoring',
    desc: 'Detect statistical drift in Gold KPI distributions over time',
    code: `-- Data drift: PSI on revenue distribution (current vs baseline)
SELECT
  decile,
  baseline_pct,
  current_pct,
  ABS(current_pct - baseline_pct) AS psi_term,
  SUM(ABS(current_pct - baseline_pct)) OVER () AS total_psi
FROM (
  SELECT
    NTILE(10) OVER (ORDER BY net_revenue) AS decile,
    COUNT(*) / SUM(COUNT(*)) OVER ()      AS current_pct
  FROM gold.marts.fact_sales WHERE order_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY decile
) HAVING total_psi > 0.2;  -- PSI > 0.2 = significant drift`,
  },
  {
    id: 86,
    group: 'Monitoring',
    title: 'Alerting',
    desc: 'Configure Databricks SQL alerts on Gold KPI thresholds',
    code: `-- Alerting: configure via Databricks SQL Alerts
-- Alert 1: Revenue drops below threshold
SELECT
  CASE WHEN revenue_today < revenue_yesterday * 0.8
       THEN 'ALERT: Revenue down >20%'
       ELSE 'OK' END AS alert_status,
  revenue_today, revenue_yesterday
FROM (
  SELECT SUM(CASE WHEN order_date = current_date() THEN net_revenue END) AS revenue_today,
         SUM(CASE WHEN order_date = current_date() - 1 THEN net_revenue END) AS revenue_yesterday
  FROM gold.kpis.daily_kpis
);`,
  },
  {
    id: 87,
    group: 'Monitoring',
    title: 'Cost Monitoring',
    desc: 'Track DBU consumption by Gold pipeline jobs to manage compute costs',
    code: `-- Cost monitoring: DBU usage by Gold pipeline
SELECT
  cluster_id, job_id, job_name,
  SUM(dbu_used)                   AS total_dbu,
  SUM(dbu_used) * 0.40            AS estimated_cost_usd,
  MIN(start_time)                 AS first_run,
  MAX(end_time)                   AS last_run,
  COUNT(*)                        AS run_count
FROM system.compute.clusters
WHERE cluster_tags.layer = 'gold'
  AND start_time >= current_date() - INTERVAL 30 DAYS
GROUP BY 1, 2, 3
ORDER BY total_dbu DESC;`,
  },
  {
    id: 88,
    group: 'Monitoring',
    title: 'Data Freshness Checks',
    desc: 'Assert Gold tables are updated within expected freshness windows',
    code: `-- Data freshness check: Gold tables must refresh within 4 hours
SELECT
  'gold.marts.fact_sales'      AS table_name,
  MAX(_commit_timestamp)       AS last_updated,
  current_timestamp()          AS checked_at,
  DATEDIFF(HOUR, MAX(_commit_timestamp), current_timestamp()) AS hours_since_update,
  CASE WHEN DATEDIFF(HOUR, MAX(_commit_timestamp), current_timestamp()) > 4
       THEN 'STALE' ELSE 'FRESH' END AS freshness_status
FROM gold.marts.fact_sales;`,
  },
  {
    id: 89,
    group: 'Monitoring',
    title: 'Report Validation',
    desc: 'Cross-validate Gold report totals against Silver source to detect discrepancies',
    code: `-- Report validation: Gold vs Silver reconciliation
SELECT
  'Gold Total'   AS source,
  SUM(net_revenue) AS revenue,
  COUNT(order_id)  AS orders
FROM gold.marts.fact_sales
WHERE order_date = current_date() - 1
UNION ALL
SELECT
  'Silver Total' AS source,
  SUM(net_revenue),
  COUNT(order_id)
FROM silver.sales.orders_clean
WHERE order_date = current_date() - 1
  AND status NOT IN ('cancelled', 'returned');`,
  },
  {
    id: 90,
    group: 'Monitoring',
    title: 'Dashboard Health Tracking',
    desc: 'Monitor BI dashboard query success rates and refresh failures',
    code: `-- Dashboard health: query failure rate per dashboard
SELECT
  dashboard_id, dashboard_name,
  COUNT(*)                                  AS total_refreshes,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)  AS failed_refreshes,
  ROUND(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS failure_rate_pct,
  AVG(duration_ms)                          AS avg_refresh_ms
FROM system.query.history
WHERE statement_text LIKE '%dashboard%'
  AND start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY 1, 2
ORDER BY failure_rate_pct DESC;`,
  },

  // ─── 91–100: Error/Validation ───
  {
    id: 91,
    group: 'Error/Validation',
    title: 'Business Reconciliation',
    desc: 'Reconcile Gold financial totals against source ERP system figures',
    code: `-- Business reconciliation: Gold vs ERP
SELECT
  month,
  gold_revenue, erp_revenue,
  ABS(gold_revenue - erp_revenue)                           AS variance,
  ABS(gold_revenue - erp_revenue) / NULLIF(erp_revenue, 0) AS variance_pct,
  CASE WHEN ABS(gold_revenue - erp_revenue) / NULLIF(erp_revenue, 0) > 0.005
       THEN 'INVESTIGATE' ELSE 'RECONCILED' END             AS status
FROM gold.finance_mart.pnl_summary g
JOIN external.erp.monthly_actuals  e USING (month)
ORDER BY month DESC;`,
  },
  {
    id: 92,
    group: 'Error/Validation',
    title: 'Cross-System Validation',
    desc: 'Validate customer counts and order totals between Gold and CRM',
    code: `-- Cross-system validation: Gold vs CRM customer counts
SELECT
  'Gold'   AS system, COUNT(DISTINCT customer_id) AS customer_count FROM gold.cx_mart.customer_360
UNION ALL
SELECT 'CRM', COUNT(DISTINCT crm_id) FROM external.salesforce.accounts_ext
UNION ALL
SELECT 'Delta', ABS(
  (SELECT COUNT(DISTINCT customer_id) FROM gold.cx_mart.customer_360) -
  (SELECT COUNT(DISTINCT crm_id)      FROM external.salesforce.accounts_ext)
), NULL;`,
  },
  {
    id: 93,
    group: 'Error/Validation',
    title: 'Data Anomaly Detection',
    desc: 'Flag statistical anomalies in Gold KPI table using Z-score method',
    code: `-- Anomaly detection: Z-score on daily revenue
WITH stats AS (
  SELECT AVG(revenue) AS mu, STDDEV(revenue) AS sigma FROM gold.kpis.daily_kpis
  WHERE order_date BETWEEN current_date() - 90 AND current_date() - 1
)
SELECT
  k.order_date, k.revenue,
  ROUND((k.revenue - s.mu) / NULLIF(s.sigma, 0), 2) AS z_score,
  CASE WHEN ABS((k.revenue - s.mu) / NULLIF(s.sigma, 0)) > 3
       THEN 'ANOMALY' ELSE 'NORMAL' END AS status
FROM gold.kpis.daily_kpis k CROSS JOIN stats s
ORDER BY ABS((k.revenue - s.mu) / NULLIF(s.sigma, 0)) DESC;`,
  },
  {
    id: 94,
    group: 'Error/Validation',
    title: 'KPI Validation',
    desc: 'Assert all KPI values are within expected business bounds',
    code: `-- KPI validation: business rule assertions
SELECT
  order_date,
  CASE WHEN total_revenue < 0            THEN 'FAIL: negative revenue'
       WHEN total_orders = 0             THEN 'FAIL: zero orders'
       WHEN avg_order_value > 100000     THEN 'FAIL: AOV too high'
       WHEN unique_customers > total_orders THEN 'FAIL: customers > orders'
       ELSE 'PASS' END AS validation_status,
  total_revenue, total_orders, avg_order_value
FROM gold.kpis.daily_kpis
WHERE order_date >= current_date() - INTERVAL 7 DAYS
ORDER BY order_date DESC;`,
  },
  {
    id: 95,
    group: 'Error/Validation',
    title: 'Backfill Correction',
    desc: 'Re-process and backfill Gold tables for a specific date range',
    code: `-- Backfill correction: re-process Gold for affected date range
-- Step 1: Delete affected records
DELETE FROM gold.kpis.daily_kpis
WHERE order_date BETWEEN '2024-01-10' AND '2024-01-15';

-- Step 2: Re-insert corrected data
INSERT INTO gold.kpis.daily_kpis
SELECT
  order_date, SUM(revenue) AS total_revenue,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(revenue) / COUNT(DISTINCT order_id) AS avg_order_value,
  COUNT(DISTINCT customer_id) AS unique_customers, SUM(units_sold) AS total_units
FROM silver.sales.orders_clean
WHERE order_date BETWEEN '2024-01-10' AND '2024-01-15'
GROUP BY order_date;`,
  },
  {
    id: 96,
    group: 'Error/Validation',
    title: 'Version Control',
    desc: 'Use Delta time travel to track and compare Gold table versions',
    code: `-- Delta time travel: compare Gold versions
-- Current vs 7 days ago revenue
SELECT 'current' AS snapshot, SUM(net_revenue) AS revenue FROM gold.marts.fact_sales
UNION ALL
SELECT '7d_ago', SUM(net_revenue) FROM gold.marts.fact_sales TIMESTAMP AS OF CURRENT_TIMESTAMP() - INTERVAL 7 DAYS;

-- Show version history
DESCRIBE HISTORY gold.marts.fact_sales;

-- Restore to last known good version
RESTORE TABLE gold.kpis.daily_kpis TO VERSION AS OF 42;`,
  },
  {
    id: 97,
    group: 'Error/Validation',
    title: 'Rollback Capability',
    desc: 'Roll back Gold table to previous version using Delta time travel',
    code: `-- Rollback Gold table to last good version
-- Step 1: Identify last good version
DESCRIBE HISTORY gold.marts.fact_sales LIMIT 10;

-- Step 2: Validate data at previous version
SELECT COUNT(*), SUM(net_revenue)
FROM gold.marts.fact_sales VERSION AS OF 55;

-- Step 3: Restore
RESTORE TABLE gold.marts.fact_sales TO VERSION AS OF 55;

-- Step 4: Confirm restore
SELECT COUNT(*), MAX(_commit_timestamp) FROM gold.marts.fact_sales;`,
  },
  {
    id: 98,
    group: 'Error/Validation',
    title: 'Audit Validation',
    desc: 'Verify every write to Gold tables is captured in audit log',
    code: `-- Audit validation: confirm all Gold writes are logged
SELECT
  action_name,
  user_identity.email AS user_email,
  request_params.table AS table_name,
  COUNT(*) AS operation_count,
  MIN(event_time) AS first_op, MAX(event_time) AS last_op
FROM system.access.audit
WHERE event_time >= current_date() - INTERVAL 1 DAYS
  AND request_params.table LIKE 'gold%'
  AND action_name IN ('CREATE', 'REPLACE', 'INSERT', 'MERGE', 'DELETE', 'RESTORE')
GROUP BY 1, 2, 3
ORDER BY operation_count DESC;`,
  },
  {
    id: 99,
    group: 'Error/Validation',
    title: 'Exception Reporting',
    desc: 'Capture and report pipeline exceptions and data quality failures',
    code: `-- Exception reporting: consolidate all Gold pipeline errors
CREATE OR REPLACE TABLE gold.monitoring.pipeline_exceptions AS
SELECT
  run_id, job_id, task_key,
  error_type, error_message,
  affected_table, affected_records,
  severity,  -- 'WARNING', 'ERROR', 'CRITICAL'
  occurred_at, resolved_at,
  is_resolved
FROM silver.monitoring.pipeline_errors
WHERE occurred_at >= current_date() - INTERVAL 30 DAYS
  AND layer = 'gold'
ORDER BY occurred_at DESC;`,
  },
  {
    id: 100,
    group: 'Error/Validation',
    title: 'Data Correction Pipelines',
    desc: 'Automated correction pipeline triggered when validation checks fail',
    code: `# Data correction pipeline: auto-fix known Gold issues
from pyspark.sql.functions import col, when

# Load Gold table and apply business corrections
df = spark.table('gold.kpis.daily_kpis')

corrected = df.withColumn(
    'avg_order_value',
    when(col('avg_order_value') > 100000, col('total_revenue') / col('total_orders'))
    .otherwise(col('avg_order_value'))
).withColumn(
    'total_revenue',
    when(col('total_revenue') < 0, 0).otherwise(col('total_revenue'))
)

corrected.write.format('delta').mode('overwrite').option('overwriteSchema', 'false') \
    .saveAsTable('gold.kpis.daily_kpis')`,
  },
];

const groups = [...new Set(goldOperations.map((op) => op.group))];

function GoldOperations() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const filtered = goldOperations.filter((op) => {
    const matchGroup = selectedGroup === 'All' || op.group === selectedGroup;
    const matchSearch =
      op.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      op.desc.toLowerCase().includes(searchTerm.toLowerCase());
    return matchGroup && matchSearch;
  });

  const downloadCSV = () => {
    exportToCSV(
      goldOperations.map((op) => ({
        id: op.id,
        group: op.group,
        title: op.title,
        desc: op.desc,
      })),
      'gold-operations.csv'
    );
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Gold Layer Operations</h1>
          <p>
            100 operations — Business modeling, Data marts, BI, AI/ML, RAG, Security, Monitoring
          </p>
        </div>
      </div>

      {/* Stat Cards */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon yellow">{'\u{1F947}'}</div>
          <div className="stat-info">
            <h4>100</h4>
            <p>Operations</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon blue">{'\u{1F4CA}'}</div>
          <div className="stat-info">
            <h4>10</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">{'\u{1F4CB}'}</div>
          <div className="stat-info">
            <h4>{filtered.length}</h4>
            <p>Showing</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">{'\u{1F916}'}</div>
          <div className="stat-info">
            <h4>AI/RAG</h4>
            <p>Ready</p>
          </div>
        </div>
      </div>

      {/* Flow diagram */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ fontWeight: 700, marginBottom: '0.5rem', fontSize: '0.85rem' }}>
          Gold Layer Flow
        </div>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.4rem',
            flexWrap: 'wrap',
            fontSize: '0.8rem',
          }}
        >
          {['Silver', 'Business Logic', 'Aggregation', 'Data Mart', 'KPI', 'BI / AI / API'].map(
            (step, i, arr) => (
              <React.Fragment key={step}>
                <span
                  style={{
                    padding: '3px 10px',
                    borderRadius: '12px',
                    background: i === 0 ? '#dbeafe' : i === arr.length - 1 ? '#fef9c3' : '#f3f4f6',
                    color: i === 0 ? '#1d4ed8' : i === arr.length - 1 ? '#92400e' : '#374151',
                    fontWeight: 600,
                  }}
                >
                  {step}
                </span>
                {i < arr.length - 1 && (
                  <span style={{ color: 'var(--text-secondary)' }}>{'\u2192'}</span>
                )}
              </React.Fragment>
            )
          )}
        </div>
      </div>

      {/* Anti-patterns card */}
      <div className="card" style={{ marginBottom: '1rem', borderLeft: '4px solid var(--error)' }}>
        <div style={{ fontWeight: 700, color: 'var(--error)', marginBottom: '0.4rem' }}>
          Gold Layer Anti-Patterns to Avoid
        </div>
        <ul
          style={{
            margin: 0,
            paddingLeft: '1.2rem',
            fontSize: '0.85rem',
            color: 'var(--text-secondary)',
          }}
        >
          <li>Storing raw or lightly-transformed data in the Gold layer</li>
          <li>Applying business logic in the BI tool instead of Gold tables</li>
          <li>Creating overly wide denormalized tables with 100+ columns</li>
          <li>Skipping row/column-level security on consumer-facing tables</li>
          <li>Running heavy aggregations at query time instead of pre-aggregating</li>
          <li>No data freshness SLA or monitoring on Gold pipeline jobs</li>
          <li>Mixing ML feature engineering directly into BI-serving tables</li>
        </ul>
      </div>

      {/* Controls */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search operations..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '280px' }}
          />
          <select
            className="form-input"
            value={selectedGroup}
            onChange={(e) => setSelectedGroup(e.target.value)}
            style={{ maxWidth: '260px' }}
          >
            <option value="All">All Categories (100)</option>
            {groups.map((g) => (
              <option key={g} value={g}>
                {g} ({goldOperations.filter((op) => op.group === g).length})
              </option>
            ))}
          </select>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadCSV}
            style={{ marginLeft: 'auto' }}
          >
            Download CSV
          </button>
        </div>
      </div>

      {/* Operation list */}
      {filtered.map((op) => {
        const isExpanded = expandedId === op.id;
        return (
          <div key={op.id} className="card" style={{ marginBottom: '0.75rem' }}>
            <div
              onClick={() => setExpandedId(isExpanded ? null : op.id)}
              style={{
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <div style={{ flex: 1 }}>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    flexWrap: 'wrap',
                    marginBottom: '0.2rem',
                  }}
                >
                  <span className="badge running">{op.group}</span>
                  <strong>
                    #{op.id} &mdash; {op.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{op.desc}</p>
              </div>
              <span style={{ color: 'var(--text-secondary)', marginLeft: '0.75rem' }}>
                {isExpanded ? '\u25BC' : '\u25B6'}
              </span>
            </div>

            {isExpanded && (
              <div style={{ marginTop: '1rem' }}>
                <div className="code-block" style={{ maxHeight: '350px', overflowY: 'auto' }}>
                  {op.code}
                </div>
              </div>
            )}
          </div>
        );
      })}

      {filtered.length === 0 && (
        <div className="card" style={{ textAlign: 'center', color: 'var(--text-secondary)' }}>
          No operations match your search.
        </div>
      )}
    </div>
  );
}

export default GoldOperations;
