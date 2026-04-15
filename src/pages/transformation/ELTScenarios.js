import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

const eltScenarios = [
  // ─── 1–10: Basic ELT (Load First → Transform Later) ───
  {
    id: 1,
    group: 'Basic ELT',
    title: 'Raw CSV ELT',
    flow: 'Extract \u2192 Load Raw \u2192 SQL Transform',
    complexity: 2,
    volume: 3,
    sla: 2,
    reliability: 3,
    governance: 2,
    score: 2.4,
    code: `-- ELT: Load first, transform with SQL after\n-- Step 1: LOAD raw (minimal logic)\nCOPY INTO catalog.bronze.csv_raw\nFROM 's3://landing/csv/'\nFILEFORMAT = CSV\nFORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')\nCOPY_OPTIONS ('mergeSchema' = 'true');\n\n-- Step 2: TRANSFORM in-place with SQL\nCREATE OR REPLACE TABLE catalog.silver.csv_clean AS\nSELECT\n    CAST(id AS BIGINT) AS id,\n    TRIM(LOWER(name)) AS name,\n    LOWER(email) AS email,\n    CAST(amount AS DECIMAL(18,2)) AS amount,\n    TO_DATE(date_str, 'yyyy-MM-dd') AS event_date,\n    current_timestamp() AS _etl_ts\nFROM catalog.bronze.csv_raw\nWHERE id IS NOT NULL\nQUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) = 1;`,
  },
  {
    id: 2,
    group: 'Basic ELT',
    title: 'JSON ELT Pipeline',
    flow: 'Extract \u2192 Load JSON \u2192 Parse \u2192 Model',
    complexity: 3,
    volume: 4,
    sla: 2,
    reliability: 3,
    governance: 2,
    score: 2.8,
    code: `-- LOAD raw JSON\nCOPY INTO catalog.bronze.json_raw\nFROM 's3://landing/json/'\nFILEFORMAT = JSON\nFORMAT_OPTIONS ('multiLine' = 'true');\n\n-- TRANSFORM: flatten nested JSON\nCREATE OR REPLACE TABLE catalog.silver.json_flat AS\nSELECT\n    id, name,\n    address:city::STRING AS city,\n    address:zip::STRING AS zip,\n    EXPLODE(tags) AS tag,\n    current_timestamp() AS _etl_ts\nFROM catalog.bronze.json_raw\nWHERE id IS NOT NULL;`,
  },
  {
    id: 3,
    group: 'Basic ELT',
    title: 'Parquet ELT',
    flow: 'Extract \u2192 Load Parquet \u2192 Transform',
    complexity: 1,
    volume: 5,
    sla: 2,
    reliability: 3,
    governance: 2,
    score: 2.6,
    code: `-- LOAD: Parquet → Delta (near zero transform)\nCREATE TABLE catalog.bronze.parquet_raw\nUSING DELTA AS\nSELECT *, current_timestamp() AS _load_ts\nFROM parquet.\`s3://data-lake/parquet/\`;\n\n-- TRANSFORM: partition + optimize\nCREATE OR REPLACE TABLE catalog.silver.parquet_data\nPARTITIONED BY (year, month) AS\nSELECT *, YEAR(event_date) AS year, MONTH(event_date) AS month\nFROM catalog.bronze.parquet_raw;\n\nOPTIMIZE catalog.silver.parquet_data ZORDER BY (user_id);`,
  },
  {
    id: 4,
    group: 'Basic ELT',
    title: 'Multi-File ELT Pipeline',
    flow: 'Load All Files \u2192 Unify \u2192 Transform',
    complexity: 3,
    volume: 4,
    sla: 2,
    reliability: 4,
    governance: 2,
    score: 3.0,
    code: `-- LOAD: Auto Loader ingests all files\ndf = spark.readStream.format("cloudFiles") \\\n    .option("cloudFiles.format", "csv") \\\n    .option("header", "true") \\\n    .option("cloudFiles.schemaLocation", "/mnt/schema/multi") \\\n    .load("s3://landing/daily_drops/")\n\ndf.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/multi_elt") \\\n    .trigger(availableNow=True) \\\n    .toTable("catalog.bronze.daily_raw")\n\n-- TRANSFORM: SQL after load\nCREATE OR REPLACE TABLE catalog.silver.daily_unified AS\nSELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) AS rn\nFROM catalog.bronze.daily_raw\nQUALIFY rn = 1;`,
  },
  {
    id: 5,
    group: 'Basic ELT',
    title: 'Archive ELT Pipeline',
    flow: 'Historical Load \u2192 Batch Transform',
    complexity: 3,
    volume: 5,
    sla: 1,
    reliability: 4,
    governance: 2,
    score: 3.0,
    code: `-- LOAD: bulk historical data\nfor year in range(2020, 2025):\n    spark.read.parquet(f"s3://archive/{year}/") \\\n        .withColumn("_load_year", lit(year)) \\\n        .write.format("delta").mode("append") \\\n        .saveAsTable("catalog.bronze.historical_raw")\n\n-- TRANSFORM: SQL\nCREATE OR REPLACE TABLE catalog.silver.historical AS\nSELECT *, DATE_TRUNC('month', event_date) AS event_month\nFROM catalog.bronze.historical_raw\nWHERE status != 'CANCELLED';`,
  },
  {
    id: 6,
    group: 'Basic ELT',
    title: 'Schema-Evolution ELT',
    flow: 'Load Evolving Schema \u2192 Adaptive Transform',
    complexity: 5,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `-- LOAD: Auto Loader handles schema changes\ndf = spark.readStream.format("cloudFiles") \\\n    .option("cloudFiles.format", "json") \\\n    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\\n    .option("cloudFiles.schemaLocation", "/mnt/schema/evolving") \\\n    .load("/mnt/landing/evolving/")\n\ndf.writeStream.format("delta") \\\n    .option("mergeSchema", "true") \\\n    .option("checkpointLocation", "/mnt/cp/evolving") \\\n    .trigger(availableNow=True) \\\n    .toTable("catalog.bronze.evolving_raw")\n\n-- TRANSFORM: handle null new columns gracefully\nCREATE OR REPLACE TABLE catalog.silver.evolving AS\nSELECT *, COALESCE(new_col_v3, 'N/A') AS new_col_v3_safe\nFROM catalog.bronze.evolving_raw;`,
  },
  {
    id: 7,
    group: 'Basic ELT',
    title: 'Incremental File ELT',
    flow: 'Load Daily Files \u2192 Transform Partitions',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `-- LOAD: daily incremental via Auto Loader\ndf = spark.readStream.format("cloudFiles") \\\n    .option("cloudFiles.format", "csv") \\\n    .option("header", "true") \\\n    .load("s3://landing/daily/")\n\ndf.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/daily_incr") \\\n    .trigger(availableNow=True) \\\n    .toTable("catalog.bronze.daily_incremental")\n\n-- TRANSFORM: only new partitions\nMERGE INTO catalog.silver.orders AS t\nUSING (\n    SELECT * FROM catalog.bronze.daily_incremental\n    WHERE _ingest_date = current_date()\n) AS s ON t.order_id = s.order_id\nWHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *;`,
  },
  {
    id: 8,
    group: 'Basic ELT',
    title: 'File Merge ELT',
    flow: 'Load Files \u2192 Dedupe/Merge Transform',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 3.8,
    code: `-- LOAD: all files into bronze\nCOPY INTO catalog.bronze.file_merge_raw\nFROM 's3://landing/batch/'\nFILEFORMAT = CSV FORMAT_OPTIONS ('header'='true');\n\n-- TRANSFORM: merge/dedupe\nMERGE INTO catalog.silver.master_records AS t\nUSING (\n    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn\n    FROM catalog.bronze.file_merge_raw\n    QUALIFY rn = 1\n) AS s ON t.id = s.id\nWHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *;`,
  },
  {
    id: 9,
    group: 'Basic ELT',
    title: 'Snapshot ELT',
    flow: 'Load Snapshot \u2192 Compare \u2192 Transform',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 3.8,
    code: `-- LOAD: daily snapshot\ndf = spark.read.format("jdbc").option("dbtable","inventory").load()\ndf.withColumn("snapshot_date", current_date()) \\\n    .write.format("delta").mode("append") \\\n    .saveAsTable("catalog.bronze.inventory_snapshots")\n\n-- TRANSFORM: detect changes between snapshots\nCREATE OR REPLACE TABLE catalog.silver.inventory_changes AS\nSELECT\n    today.sku, today.quantity AS current_qty,\n    yesterday.quantity AS prev_qty,\n    today.quantity - yesterday.quantity AS change,\n    CASE WHEN today.quantity - yesterday.quantity > 0 THEN 'RESTOCK'\n         WHEN today.quantity - yesterday.quantity < 0 THEN 'SOLD'\n         ELSE 'NO_CHANGE' END AS change_type\nFROM catalog.bronze.inventory_snapshots today\nJOIN catalog.bronze.inventory_snapshots yesterday\n    ON today.sku = yesterday.sku\n    AND today.snapshot_date = current_date()\n    AND yesterday.snapshot_date = current_date() - 1;`,
  },
  {
    id: 10,
    group: 'Basic ELT',
    title: 'Bulk Load ELT',
    flow: 'Mass Load \u2192 Transform in Cluster',
    complexity: 2,
    volume: 5,
    sla: 2,
    reliability: 4,
    governance: 2,
    score: 3.0,
    code: `-- LOAD: massive bulk (minimal transform)\nCOPY INTO catalog.bronze.bulk_data\nFROM 's3://data-dump/2024/'\nFILEFORMAT = PARQUET;\n\n-- TRANSFORM: leverage cluster compute\nCREATE OR REPLACE TABLE catalog.silver.bulk_processed AS\nSELECT *,\n    HASH(id, name, email) AS row_hash,\n    current_timestamp() AS _processed_ts\nFROM catalog.bronze.bulk_data\nWHERE id IS NOT NULL;`,
  },

  // ─── 11–20: Database ELT ───
  {
    id: 11,
    group: 'Database ELT',
    title: 'Full Table ELT',
    flow: 'Extract DB \u2192 Load Raw \u2192 Transform',
    complexity: 2,
    volume: 4,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.0,
    code: `-- LOAD: raw DB extract\ndf = spark.read.format("jdbc").option("dbtable","orders").option("numPartitions",8).load()\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.db_orders")\n\n-- TRANSFORM: SQL in lakehouse\nCREATE OR REPLACE TABLE catalog.silver.orders AS\nSELECT\n    order_id, customer_id, product_id,\n    CAST(amount AS DECIMAL(18,2)) AS amount,\n    TO_DATE(order_date) AS order_date,\n    CASE WHEN status = 'S' THEN 'SHIPPED' WHEN status = 'P' THEN 'PENDING' ELSE status END AS status\nFROM catalog.bronze.db_orders\nWHERE order_id IS NOT NULL;`,
  },
  {
    id: 12,
    group: 'Database ELT',
    title: 'Incremental ELT',
    flow: 'Extract Delta \u2192 Load \u2192 Transform',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 3.6,
    code: `-- LOAD incremental\nwatermark = spark.sql("SELECT MAX(modified_at) FROM catalog.bronze.orders").collect()[0][0]\ndf = spark.read.format("jdbc") \\\n    .option("dbtable", f"(SELECT * FROM orders WHERE modified_at > '{watermark}') t").load()\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.orders")\n\n-- TRANSFORM via MERGE\nMERGE INTO catalog.silver.orders AS t\nUSING catalog.bronze.orders AS s ON t.order_id = s.order_id\nWHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *;`,
  },
  {
    id: 13,
    group: 'Database ELT',
    title: 'CDC ELT Pipeline',
    flow: 'CDC Logs \u2192 Load \u2192 Merge Transform',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `-- LOAD CDC logs raw\ndf = spark.read.format("jdbc").option("dbtable","cdc_log").load()\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.cdc_raw")\n\n-- TRANSFORM: apply CDC operations\nMERGE INTO catalog.silver.customers AS t\nUSING (\n    SELECT * FROM catalog.bronze.cdc_raw\n    WHERE batch_id = (SELECT MAX(batch_id) FROM catalog.bronze.cdc_raw)\n) AS s ON t.id = s.id\nWHEN MATCHED AND s.operation = 'UPDATE' THEN UPDATE SET *\nWHEN MATCHED AND s.operation = 'DELETE' THEN DELETE\nWHEN NOT MATCHED AND s.operation = 'INSERT' THEN INSERT *;`,
  },
  {
    id: 14,
    group: 'Database ELT',
    title: 'Multi-Table ELT',
    flow: 'Many Tables \u2192 Load \u2192 Relational Transform',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `-- LOAD: extract multiple tables raw\ntables = ["orders", "customers", "products", "categories"]\nfor t in tables:\n    df = spark.read.format("jdbc").option("dbtable", t).load()\n    df.write.format("delta").mode("overwrite").saveAsTable(f"catalog.bronze.{t}")\n\n-- TRANSFORM: join in lakehouse\nCREATE OR REPLACE TABLE catalog.silver.order_details AS\nSELECT o.*, c.name AS customer_name, c.segment,\n    p.name AS product_name, cat.category_name\nFROM catalog.bronze.orders o\nJOIN catalog.bronze.customers c ON o.customer_id = c.id\nJOIN catalog.bronze.products p ON o.product_id = p.id\nJOIN catalog.bronze.categories cat ON p.category_id = cat.id;`,
  },
  {
    id: 15,
    group: 'Database ELT',
    title: 'ERP ELT Pipeline',
    flow: 'ERP \u2192 Raw \u2192 Transform to Finance Model',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `-- LOAD: raw ERP data\ndf = spark.read.format("jdbc").option("url","jdbc:sap://hana:30015") \\\n    .option("dbtable","GL_ENTRIES").load()\ndf.write.format("delta").saveAsTable("catalog.bronze.erp_gl")\n\ncoa = spark.read.format("jdbc").option("dbtable","CHART_OF_ACCOUNTS").load()\ncoa.write.format("delta").saveAsTable("catalog.bronze.erp_coa")\n\n-- TRANSFORM: finance model in SQL\nCREATE OR REPLACE TABLE catalog.gold.finance_model AS\nSELECT\n    gl.fiscal_period, coa.account_type, gl.cost_center,\n    SUM(CASE WHEN gl.entry_type='DR' THEN gl.amount ELSE 0 END) AS debit,\n    SUM(CASE WHEN gl.entry_type='CR' THEN gl.amount ELSE 0 END) AS credit,\n    SUM(CASE WHEN gl.entry_type='DR' THEN gl.amount ELSE -gl.amount END) AS net\nFROM catalog.bronze.erp_gl gl\nJOIN catalog.bronze.erp_coa coa ON gl.account_code = coa.code\nGROUP BY gl.fiscal_period, coa.account_type, gl.cost_center;`,
  },
  {
    id: 16,
    group: 'Database ELT',
    title: 'CRM ELT Pipeline',
    flow: 'CRM \u2192 Raw \u2192 Customer Model',
    complexity: 3,
    volume: 3,
    sla: 3,
    reliability: 4,
    governance: 4,
    score: 3.4,
    code: `-- LOAD CRM raw\nfrom simple_salesforce import Salesforce\nsf = Salesforce(username=dbutils.secrets.get("sf","user"), password=dbutils.secrets.get("sf","pass"), security_token=dbutils.secrets.get("sf","token"))\ndf = spark.createDataFrame(sf.bulk.Account.query("SELECT Id,Name,Industry,AnnualRevenue FROM Account"))\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.crm_accounts")\n\n-- TRANSFORM in SQL\nCREATE OR REPLACE TABLE catalog.silver.customers AS\nSELECT Id AS customer_id, Name AS name, Industry AS industry,\n    AnnualRevenue AS revenue,\n    CASE WHEN AnnualRevenue > 1000000 THEN 'Enterprise'\n         WHEN AnnualRevenue > 100000 THEN 'Mid-Market' ELSE 'SMB' END AS segment\nFROM catalog.bronze.crm_accounts;`,
  },
  {
    id: 17,
    group: 'Database ELT',
    title: 'HR ELT Pipeline',
    flow: 'HR \u2192 Raw \u2192 Secure Transform',
    complexity: 3,
    volume: 2,
    sla: 2,
    reliability: 4,
    governance: 5,
    score: 3.2,
    code: `-- LOAD raw HR data\ndf = spark.read.format("jdbc").option("dbtable","employees").load()\ndf.write.format("delta").saveAsTable("catalog.bronze.hr_raw")\n\n-- TRANSFORM: mask PII in SQL\nCREATE OR REPLACE TABLE catalog.secure.hr_employees AS\nSELECT\n    employee_id, department, job_title, hire_date, location,\n    SHA2(ssn, 256) AS ssn_hash,\n    CONCAT(LEFT(name, 1), '***') AS name_masked,\n    CASE WHEN salary < 50000 THEN 'Band1'\n         WHEN salary < 100000 THEN 'Band2' ELSE 'Band3' END AS salary_band\nFROM catalog.bronze.hr_raw;`,
  },
  {
    id: 18,
    group: 'Database ELT',
    title: 'Banking ELT Pipeline',
    flow: 'Core Banking \u2192 Raw \u2192 Model',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `-- LOAD raw banking transactions\ndf = spark.read.format("jdbc").option("url","jdbc:db2://core:50000/BANK") \\\n    .option("dbtable","DAILY_TRANSACTIONS").option("numPartitions",16).load()\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.bank_raw")\n\n-- TRANSFORM: secure model in SQL\nCREATE OR REPLACE TABLE catalog.secure.bank_transactions AS\nSELECT\n    txn_id, txn_type, amount,\n    SHA2(CONCAT(account_no, 'SALT'), 256) AS account_hash,\n    CONCAT('***-**-', RIGHT(ssn, 4)) AS ssn_masked,\n    CASE WHEN ABS(amount) > 10000 THEN TRUE ELSE FALSE END AS high_value_flag,\n    txn_date, branch_code\nFROM catalog.bronze.bank_raw;`,
  },
  {
    id: 19,
    group: 'Database ELT',
    title: 'POS ELT Pipeline',
    flow: 'POS \u2192 Raw \u2192 Sales Model',
    complexity: 3,
    volume: 5,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.6,
    code: `-- LOAD from multiple stores\nstores = ["nyc","la","chi","sf"]\nfor store in stores:\n    df = spark.read.format("jdbc").option("url",f"jdbc:mysql://{store}-db/pos") \\\n        .option("dbtable","transactions").load()\n    df.withColumn("store_id", lit(store)) \\\n        .write.format("delta").mode("append").saveAsTable("catalog.bronze.pos_raw")\n\n-- TRANSFORM in SQL\nCREATE OR REPLACE TABLE catalog.silver.pos_sales AS\nSELECT *, quantity * unit_price AS total_amount,\n    DATE(transaction_timestamp) AS sale_date\nFROM catalog.bronze.pos_raw\nWHERE quantity > 0;`,
  },
  {
    id: 20,
    group: 'Database ELT',
    title: 'Inventory ELT Pipeline',
    flow: 'Inventory \u2192 Raw \u2192 Stock Model',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `-- LOAD\ndf = spark.read.format("jdbc").option("dbtable","inventory_levels").load()\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.inventory_raw")\n\n-- TRANSFORM\nCREATE OR REPLACE TABLE catalog.silver.inventory AS\nSELECT sku, warehouse, quantity,\n    quantity / NULLIF(avg_daily_demand, 0) AS days_of_supply,\n    CASE WHEN quantity < reorder_point THEN 'LOW'\n         WHEN quantity = 0 THEN 'OUT_OF_STOCK' ELSE 'OK' END AS stock_status,\n    current_timestamp() AS _snapshot_ts\nFROM catalog.bronze.inventory_raw;`,
  },

  // ─── 21–30: Bronze → Silver ELT ───
  {
    id: 21,
    group: 'Bronze-to-Silver ELT',
    title: 'Cleansing ELT',
    flow: 'Bronze \u2192 Clean \u2192 Silver',
    complexity: 2,
    volume: 5,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.2,
    code: `CREATE OR REPLACE TABLE catalog.silver.clean_data AS\nSELECT\n    id, TRIM(INITCAP(name)) AS name, LOWER(TRIM(email)) AS email,\n    CAST(amount AS DECIMAL(18,2)) AS amount,\n    COALESCE(phone, 'N/A') AS phone\nFROM catalog.bronze.raw_data\nWHERE id IS NOT NULL\nQUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) = 1;`,
  },
  {
    id: 22,
    group: 'Bronze-to-Silver ELT',
    title: 'Deduplication ELT',
    flow: 'Bronze \u2192 Dedupe \u2192 Silver',
    complexity: 3,
    volume: 5,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 3.8,
    code: `CREATE OR REPLACE TABLE catalog.silver.customers AS\nSELECT * FROM (\n    SELECT *,\n        ROW_NUMBER() OVER (PARTITION BY email ORDER BY updated_at DESC) AS rn\n    FROM catalog.bronze.customers\n) WHERE rn = 1;`,
  },
  {
    id: 23,
    group: 'Bronze-to-Silver ELT',
    title: 'Conformance ELT',
    flow: 'Bronze \u2192 Standard Schema',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `-- Conform multiple source schemas to canonical\nCREATE OR REPLACE TABLE catalog.silver.orders_canonical AS\nSELECT\n    COALESCE(order_id, ord_id, id) AS order_id,\n    COALESCE(customer_name, cust_name, name) AS customer_name,\n    CAST(COALESCE(order_amount, amount, total) AS DECIMAL(18,2)) AS amount,\n    COALESCE(\n        TO_DATE(order_date, 'yyyy-MM-dd'),\n        TO_DATE(ord_dt, 'yyyyMMdd'),\n        TO_DATE(date_field, 'MM/dd/yyyy')\n    ) AS order_date\nFROM catalog.bronze.multi_source_raw;`,
  },
  {
    id: 24,
    group: 'Bronze-to-Silver ELT',
    title: 'PII Masking ELT',
    flow: 'Bronze \u2192 Mask \u2192 Silver',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `CREATE OR REPLACE TABLE catalog.silver.customers_safe AS\nSELECT\n    customer_id,\n    CONCAT(LEFT(name, 1), '***') AS name_masked,\n    SHA2(email, 256) AS email_hash,\n    CONCAT('***-***-', RIGHT(phone, 4)) AS phone_masked,\n    segment, region, created_date\nFROM catalog.bronze.customers;`,
  },
  {
    id: 25,
    group: 'Bronze-to-Silver ELT',
    title: 'Data Quality ELT',
    flow: 'Bronze \u2192 Validate \u2192 Silver',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `-- Good records → Silver\nCREATE OR REPLACE TABLE catalog.silver.orders AS\nSELECT * FROM catalog.bronze.orders\nWHERE order_id IS NOT NULL\n    AND amount > 0 AND amount < 1000000\n    AND order_date >= '2020-01-01'\n    AND customer_id IS NOT NULL;\n\n-- Bad records → Quarantine\nCREATE OR REPLACE TABLE catalog.quarantine.orders AS\nSELECT *, 'FAILED_VALIDATION' AS quarantine_reason\nFROM catalog.bronze.orders\nWHERE order_id IS NULL OR amount <= 0 OR amount >= 1000000\n    OR order_date < '2020-01-01' OR customer_id IS NULL;`,
  },
  {
    id: 26,
    group: 'Bronze-to-Silver ELT',
    title: 'Enrichment ELT',
    flow: 'Bronze + Lookup \u2192 Silver',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `CREATE OR REPLACE TABLE catalog.silver.orders_enriched AS\nSELECT o.*,\n    c.name AS customer_name, c.segment,\n    g.region, g.country\nFROM catalog.bronze.orders o\nLEFT JOIN catalog.silver.customer_dim c ON o.customer_id = c.customer_id\nLEFT JOIN catalog.reference.geo_lookup g ON o.zip_code = g.zip;`,
  },
  {
    id: 27,
    group: 'Bronze-to-Silver ELT',
    title: 'Join-Heavy ELT',
    flow: 'Multi-Table Join \u2192 Silver',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `CREATE OR REPLACE TABLE catalog.silver.full_order_view AS\nSELECT\n    ol.line_id, o.order_id, o.order_date,\n    c.name AS customer, c.segment,\n    p.name AS product, p.category,\n    s.name AS store, s.region,\n    COALESCE(pr.discount_pct, 0) AS discount,\n    ol.quantity, ol.unit_price,\n    ol.quantity * ol.unit_price * (1 - COALESCE(pr.discount_pct,0)/100) AS net_amount\nFROM catalog.bronze.order_lines ol\nJOIN catalog.bronze.orders o ON ol.order_id = o.order_id\nJOIN catalog.bronze.customers c ON o.customer_id = c.id\nJOIN catalog.bronze.products p ON ol.product_id = p.id\nJOIN catalog.bronze.stores s ON o.store_id = s.id\nLEFT JOIN catalog.bronze.promotions pr ON ol.promo_code = pr.code;`,
  },
  {
    id: 28,
    group: 'Bronze-to-Silver ELT',
    title: 'SCD ELT',
    flow: 'Bronze \u2192 SCD1/2 Transform',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `-- SCD Type 2 via MERGE\nMERGE INTO catalog.silver.customers_scd2 AS t\nUSING catalog.bronze.customers AS s\nON t.customer_id = s.customer_id AND t.is_current = TRUE\n\nWHEN MATCHED AND (\n    t.email != s.email OR t.city != s.city OR t.tier != s.tier\n) THEN UPDATE SET\n    is_current = FALSE, valid_to = current_date()\n\nWHEN NOT MATCHED THEN INSERT (\n    customer_id, name, email, city, tier,\n    is_current, valid_from, valid_to\n) VALUES (\n    s.customer_id, s.name, s.email, s.city, s.tier,\n    TRUE, current_date(), '9999-12-31'\n);`,
  },
  {
    id: 29,
    group: 'Bronze-to-Silver ELT',
    title: 'Late Data ELT',
    flow: 'Bronze \u2192 Reconcile \u2192 Silver',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `-- Handle late-arriving records\nMERGE INTO catalog.silver.events AS t\nUSING (\n    SELECT *, TRUE AS is_late_arrival\n    FROM catalog.bronze.late_events\n    WHERE _ingest_date = current_date()\n) AS s ON t.event_id = s.event_id\nWHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *;\n\n-- Log late arrivals for monitoring\nINSERT INTO catalog.audit.late_data_log\nSELECT event_id, event_date, _ingest_date,\n    DATEDIFF(_ingest_date, event_date) AS days_late\nFROM catalog.bronze.late_events\nWHERE _ingest_date = current_date();`,
  },
  {
    id: 30,
    group: 'Bronze-to-Silver ELT',
    title: 'Multi-Source ELT',
    flow: 'Many Sources \u2192 Unified Silver',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.6,
    code: `-- Unify 3 customer sources in SQL\nCREATE OR REPLACE TABLE catalog.silver.customer_master AS\nWITH all_customers AS (\n    SELECT sf_id AS source_id, 'CRM' AS source, Name AS name, Email AS email\n    FROM catalog.bronze.crm_accounts\n    UNION ALL\n    SELECT CUST_NO, 'ERP', FULL_NAME, EMAIL_ADDR\n    FROM catalog.bronze.erp_customers\n    UNION ALL\n    SELECT user_id, 'WEB', display_name, email\n    FROM catalog.bronze.web_users\n)\nSELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(email)) ORDER BY source) AS rn\nFROM all_customers\nQUALIFY rn = 1;`,
  },

  // ─── 31–40: Silver → Gold ELT ───
  {
    id: 31,
    group: 'Silver-to-Gold ELT',
    title: 'Fact Table ELT',
    flow: 'Silver \u2192 Fact Tables',
    complexity: 3,
    volume: 5,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `CREATE OR REPLACE TABLE catalog.gold.fact_sales AS\nSELECT o.order_id, o.order_date, o.customer_id, c.segment,\n    o.product_id, p.category, o.quantity, o.unit_price,\n    o.quantity * o.unit_price AS total_amount\nFROM catalog.silver.orders o\nJOIN catalog.silver.customers c ON o.customer_id = c.customer_id\nJOIN catalog.silver.products p ON o.product_id = p.product_id\nWHERE o.status = 'COMPLETED';`,
  },
  {
    id: 32,
    group: 'Silver-to-Gold ELT',
    title: 'Dimension ELT',
    flow: 'Silver \u2192 Dimension Tables',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 3.8,
    code: `CREATE OR REPLACE TABLE catalog.gold.dim_customer AS\nSELECT customer_id, name, segment, region,\n    MIN(first_order_date) AS customer_since,\n    COUNT(DISTINCT order_id) AS lifetime_orders,\n    SUM(amount) AS lifetime_value,\n    CASE WHEN SUM(amount)>10000 THEN 'VIP' WHEN SUM(amount)>1000 THEN 'Regular' ELSE 'New' END AS tier\nFROM catalog.silver.orders o JOIN catalog.silver.customers c USING(customer_id)\nGROUP BY ALL;`,
  },
  {
    id: 33,
    group: 'Silver-to-Gold ELT',
    title: 'KPI Aggregation ELT',
    flow: 'Silver \u2192 KPI Metrics',
    complexity: 3,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 3,
    score: 3.8,
    code: `CREATE OR REPLACE TABLE catalog.gold.kpi_daily AS\nSELECT order_date,\n    COUNT(DISTINCT order_id) AS orders,\n    COUNT(DISTINCT customer_id) AS unique_customers,\n    SUM(amount) AS revenue, AVG(amount) AS avg_order_value\nFROM catalog.silver.orders WHERE status = 'COMPLETED'\nGROUP BY order_date;`,
  },
  {
    id: 34,
    group: 'Silver-to-Gold ELT',
    title: 'Customer 360 ELT',
    flow: 'Multi-Domain \u2192 Customer Mart',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.6,
    code: `CREATE OR REPLACE TABLE catalog.gold.customer_360 AS\nSELECT c.*, o.total_orders, o.ltv, o.avg_order,\n    s.total_tickets, s.avg_resolution_hrs,\n    w.sessions, w.avg_pages_viewed\nFROM catalog.silver.customers c\nLEFT JOIN (SELECT customer_id, COUNT(*) AS total_orders, SUM(amount) AS ltv, AVG(amount) AS avg_order FROM catalog.silver.orders GROUP BY 1) o USING(customer_id)\nLEFT JOIN (SELECT customer_id, COUNT(*) AS total_tickets, AVG(resolution_hours) AS avg_resolution_hrs FROM catalog.silver.support GROUP BY 1) s USING(customer_id)\nLEFT JOIN (SELECT customer_id, COUNT(*) AS sessions, AVG(pages) AS avg_pages_viewed FROM catalog.silver.web GROUP BY 1) w USING(customer_id);`,
  },
  {
    id: 35,
    group: 'Silver-to-Gold ELT',
    title: 'Finance Reporting ELT',
    flow: 'Silver \u2192 Finance Mart',
    complexity: 5,
    volume: 4,
    sla: 5,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `CREATE OR REPLACE TABLE catalog.gold.finance_report AS\nSELECT fiscal_period, account_type, cost_center,\n    SUM(CASE WHEN entry_type='DEBIT' THEN amount ELSE 0 END) AS total_debit,\n    SUM(CASE WHEN entry_type='CREDIT' THEN amount ELSE 0 END) AS total_credit,\n    SUM(CASE WHEN entry_type='DEBIT' THEN amount ELSE -amount END) AS net\nFROM catalog.silver.gl_entries\nGROUP BY fiscal_period, account_type, cost_center;`,
  },
  {
    id: 36,
    group: 'Silver-to-Gold ELT',
    title: 'Supply Chain ELT',
    flow: 'Silver \u2192 Supply Analytics',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `CREATE OR REPLACE TABLE catalog.gold.supply_chain AS\nSELECT po.po_number, po.supplier, po.order_date,\n    s.actual_delivery, DATEDIFF(s.actual_delivery, po.order_date) AS lead_days,\n    s.actual_delivery <= po.promised_date AS on_time,\n    i.quantity AS current_stock, i.reorder_point\nFROM catalog.silver.purchase_orders po\nLEFT JOIN catalog.silver.shipments s ON po.po_number = s.po_number\nLEFT JOIN catalog.silver.inventory i ON po.product_id = i.product_id;`,
  },
  {
    id: 37,
    group: 'Silver-to-Gold ELT',
    title: 'Product Performance ELT',
    flow: 'Product + Sales \u2192 Mart',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.6,
    code: `CREATE OR REPLACE TABLE catalog.gold.product_performance AS\nSELECT p.product_id, p.name, p.category,\n    COUNT(DISTINCT o.order_id) AS orders, SUM(o.quantity) AS units_sold,\n    SUM(o.amount) AS revenue, AVG(r.rating) AS avg_rating, COUNT(r.review_id) AS reviews\nFROM catalog.silver.products p\nLEFT JOIN catalog.silver.orders o ON p.product_id = o.product_id\nLEFT JOIN catalog.silver.reviews r ON p.product_id = r.product_id\nGROUP BY p.product_id, p.name, p.category;`,
  },
  {
    id: 38,
    group: 'Silver-to-Gold ELT',
    title: 'Trend Analysis ELT',
    flow: 'History \u2192 Trend Tables',
    complexity: 4,
    volume: 5,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 4.0,
    code: `CREATE OR REPLACE TABLE catalog.gold.revenue_trend AS\nSELECT order_date, daily_revenue,\n    AVG(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7d,\n    AVG(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30d,\n    CASE WHEN AVG(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) >\n         AVG(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)\n    THEN 'UP' ELSE 'DOWN' END AS trend\nFROM (SELECT order_date, SUM(amount) AS daily_revenue FROM catalog.silver.orders GROUP BY 1);`,
  },
  {
    id: 39,
    group: 'Silver-to-Gold ELT',
    title: 'Reconciliation ELT',
    flow: 'Source vs Target Checks',
    complexity: 4,
    volume: 3,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `-- Automated reconciliation\nINSERT INTO catalog.audit.reconciliation\nSELECT\n    'orders' AS table_name,\n    (SELECT COUNT(*) FROM catalog.bronze.orders) AS source_count,\n    (SELECT COUNT(*) FROM catalog.silver.orders) AS target_count,\n    (SELECT COUNT(*) FROM catalog.bronze.orders) = (SELECT COUNT(*) FROM catalog.silver.orders) AS counts_match,\n    current_timestamp() AS check_ts;`,
  },
  {
    id: 40,
    group: 'Silver-to-Gold ELT',
    title: 'Semantic Layer ELT',
    flow: 'Gold \u2192 BI-Ready Model',
    complexity: 4,
    volume: 3,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `CREATE OR REPLACE TABLE catalog.gold.bi_model AS\nSELECT f.order_date, f.order_id,\n    dc.name AS customer, dc.segment, dc.region,\n    dp.name AS product, dp.category,\n    f.quantity, f.total_amount,\n    YEAR(f.order_date) AS year, QUARTER(f.order_date) AS quarter, MONTH(f.order_date) AS month\nFROM catalog.gold.fact_sales f\nJOIN catalog.gold.dim_customer dc ON f.customer_id = dc.customer_id\nJOIN catalog.gold.dim_product dp ON f.product_id = dp.product_id;`,
  },

  // ─── 41–50: Advanced ELT ───
  {
    id: 41,
    group: 'Advanced ELT',
    title: 'Feature Engineering ELT',
    flow: 'Silver \u2192 ML Features',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.6,
    code: `CREATE OR REPLACE TABLE catalog.ml.customer_features AS\nSELECT customer_id,\n    COUNT(*) AS total_orders, SUM(amount) AS ltv, AVG(amount) AS avg_order,\n    DATEDIFF(current_date(), MAX(order_date)) AS recency,\n    COUNT(DISTINCT product_id) AS unique_products,\n    SUM(CASE WHEN amount > 100 THEN 1 ELSE 0 END) AS high_value_count\nFROM catalog.silver.orders\nGROUP BY customer_id;`,
  },
  {
    id: 42,
    group: 'Advanced ELT',
    title: 'Model Training Dataset ELT',
    flow: 'Raw \u2192 Curated ML Dataset',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.6,
    code: `# Load features + labels, split\nfeatures = spark.table("catalog.ml.customer_features")\nlabels = spark.table("catalog.silver.churn_labels")\ndataset = features.join(labels, "customer_id")\ntrain, test = dataset.randomSplit([0.8, 0.2], seed=42)\ntrain.write.format("delta").saveAsTable("catalog.ml.train")\ntest.write.format("delta").saveAsTable("catalog.ml.test")`,
  },
  {
    id: 43,
    group: 'Advanced ELT',
    title: 'RAG Chunking ELT',
    flow: 'Docs \u2192 Chunks',
    complexity: 5,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.4,
    code: `-- LOAD docs first, then chunk in SQL\nCREATE OR REPLACE TABLE catalog.ml.doc_chunks AS\nSELECT doc_id, title,\n    EXPLODE(TRANSFORM(\n        SEQUENCE(0, LENGTH(content) - 1, 450),\n        x -> SUBSTRING(content, x + 1, 500)\n    )) AS chunk_text,\n    MONOTONICALLY_INCREASING_ID() AS chunk_id\nFROM catalog.bronze.documents;`,
  },
  {
    id: 44,
    group: 'Advanced ELT',
    title: 'Embedding ELT Pipeline',
    flow: 'Text \u2192 Embeddings',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.6,
    code: `from sentence_transformers import SentenceTransformer\nimport pandas as pd\nmodel = SentenceTransformer("all-MiniLM-L6-v2")\n\n@pandas_udf("array<float>")\ndef embed(texts: pd.Series) -> pd.Series:\n    return pd.Series(model.encode(texts.tolist()).tolist())\n\nchunks = spark.table("catalog.ml.doc_chunks")\nchunks.withColumn("embedding", embed("chunk_text")) \\\n    .write.format("delta").saveAsTable("catalog.ml.embeddings")`,
  },
  {
    id: 45,
    group: 'Advanced ELT',
    title: 'Data Masking ELT',
    flow: 'Raw \u2192 Secure Model',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `CREATE OR REPLACE TABLE catalog.secure.masked_data AS\nSELECT\n    id, CONCAT(LEFT(name,1),'***') AS name,\n    SHA2(email, 256) AS email_hash,\n    CONCAT('***-***-', RIGHT(phone, 4)) AS phone,\n    YEAR(dob) AS birth_year,\n    department, region\nFROM catalog.bronze.sensitive_data;`,
  },
  {
    id: 46,
    group: 'Advanced ELT',
    title: 'Audit ELT Pipeline',
    flow: 'Logs \u2192 Audit Tables',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `-- LOAD audit logs\ndf = spark.read.json("/mnt/audit/")\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.audit_raw")\n\n-- TRANSFORM: categorize\nCREATE OR REPLACE VIEW catalog.audit.events_categorized AS\nSELECT *,\n    CASE WHEN action LIKE '%DELETE%' THEN 'DESTRUCTIVE'\n         WHEN action LIKE '%CREATE%' THEN 'CREATIVE' ELSE 'READ' END AS category,\n    CASE WHEN action LIKE '%DELETE%' THEN 'HIGH' ELSE 'NORMAL' END AS risk\nFROM catalog.bronze.audit_raw;`,
  },
  {
    id: 47,
    group: 'Advanced ELT',
    title: 'Lineage ELT Pipeline',
    flow: 'Metadata \u2192 Lineage Model',
    complexity: 4,
    volume: 3,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `-- Build lineage from system tables\nCREATE OR REPLACE TABLE catalog.audit.lineage_graph AS\nSELECT DISTINCT\n    source_table_full_name AS source_table,\n    target_table_full_name AS target_table,\n    source_type, target_type,\n    MAX(event_time) AS last_seen\nFROM system.access.table_lineage\nGROUP BY source_table_full_name, target_table_full_name, source_type, target_type;`,
  },
  {
    id: 48,
    group: 'Advanced ELT',
    title: 'Cost Analytics ELT',
    flow: 'Usage \u2192 Cost Model',
    complexity: 3,
    volume: 3,
    sla: 3,
    reliability: 4,
    governance: 4,
    score: 3.4,
    code: `-- LOAD cloud cost data\nimport requests\ncosts = requests.get("https://management.azure.com/.../costManagement", headers=headers, timeout=30).json()\ndf = spark.createDataFrame(costs["properties"]["rows"])\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.cloud_costs")\n\n-- TRANSFORM\nCREATE OR REPLACE TABLE catalog.gold.cost_summary AS\nSELECT DATE_TRUNC('month', date) AS month, service,\n    SUM(cost) AS total_cost, AVG(cost) AS avg_daily_cost\nFROM catalog.bronze.cloud_costs\nGROUP BY DATE_TRUNC('month', date), service;`,
  },
  {
    id: 49,
    group: 'Advanced ELT',
    title: 'DR Replication ELT',
    flow: 'Region \u2192 Region Sync',
    complexity: 5,
    volume: 5,
    sla: 5,
    reliability: 5,
    governance: 5,
    score: 5.0,
    code: `-- Cross-region replication via DEEP CLONE\ntables = spark.sql("SHOW TABLES IN catalog.gold").collect()\nfor row in tables:\n    src = f"catalog.gold.{row.tableName}"\n    dr = f"catalog_dr.gold.{row.tableName}"\n    spark.sql(f"CREATE OR REPLACE TABLE {dr} DEEP CLONE {src}")\n    # Verify\n    assert spark.table(src).count() == spark.table(dr).count()`,
  },
  {
    id: 50,
    group: 'Advanced ELT',
    title: 'Regulatory ELT Pipeline',
    flow: 'Multi-Source \u2192 Regulatory Output',
    complexity: 5,
    volume: 4,
    sla: 5,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `-- Regulatory compliance report (SOX/GDPR/HIPAA)\nCREATE OR REPLACE TABLE catalog.regulatory.compliance_report AS\nSELECT 'SOX' AS regulation,\n    COUNT(*) AS controls, SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) AS passed,\n    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS failed,\n    current_timestamp() AS report_ts\nFROM catalog.audit.compliance_checks\nWHERE check_date >= DATE_TRUNC('quarter', current_date())\nGROUP BY regulation;`,
  },
];

const groups = [...new Set(eltScenarios.map((s) => s.group))];

function ScoreBar({ label, value, max }) {
  const pct = (value / max) * 100;
  const color = value >= 4 ? '#22c55e' : value >= 3 ? '#f59e0b' : '#ef4444';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '0.7rem' }}>
      <span style={{ width: '65px', color: 'var(--text-secondary)' }}>{label}</span>
      <div style={{ flex: 1, height: '8px', background: '#f3f4f6', borderRadius: '4px' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: '4px' }} />
      </div>
      <span style={{ width: '18px', textAlign: 'right', fontWeight: 600 }}>{value}</span>
    </div>
  );
}

function ELTScenarios() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('id');

  const filtered = eltScenarios
    .filter((s) => {
      const matchGroup = selectedGroup === 'All' || s.group === selectedGroup;
      const matchSearch =
        s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
        s.flow.toLowerCase().includes(searchTerm.toLowerCase());
      return matchGroup && matchSearch;
    })
    .sort((a, b) => (sortBy === 'score' ? b.score - a.score : a.id - b.id));

  const downloadScorecard = () => {
    exportToCSV(
      eltScenarios.map((s) => ({
        id: s.id,
        group: s.group,
        title: s.title,
        flow: s.flow,
        complexity: s.complexity,
        volume: s.volume,
        sla: s.sla,
        reliability: s.reliability,
        governance: s.governance,
        score: s.score,
      })),
      'elt-scenarios-scorecard.csv'
    );
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>ELT Pipeline Scenarios</h1>
          <p>
            50 Extract \u2192 Load \u2192 Transform patterns \u2014 push compute to the lakehouse
          </p>
        </div>
      </div>

      <div
        className="card"
        style={{
          marginBottom: '1rem',
          padding: '0.75rem 1rem',
          background: '#eff6ff',
          border: '1px solid #bfdbfe',
        }}
      >
        <div style={{ fontSize: '0.8rem', color: '#1e40af' }}>
          <strong>ELT vs ETL:</strong> In ELT, data is loaded <em>first</em> (raw/bronze), then
          transformed <em>inside</em> the lakehouse using SQL/Spark. Heavy compute stays in the
          platform \u2014 no external transform tools needed.
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">{'\ud83d\udd04'}</div>
          <div className="stat-info">
            <h4>50</h4>
            <p>ELT Pipelines</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">{'\ud83c\udfc2'}</div>
          <div className="stat-info">
            <h4>{groups.length}</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">{'\u2b50'}</div>
          <div className="stat-info">
            <h4>{(eltScenarios.reduce((s, x) => s + x.score, 0) / 50).toFixed(1)}</h4>
            <p>Avg Score</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">{'\ud83c\udfc6'}</div>
          <div className="stat-info">
            <h4>{eltScenarios.filter((s) => s.score >= 4.0).length}</h4>
            <p>Advanced (4.0+)</p>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search ELT scenarios..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '250px' }}
          />
          <select
            className="form-input"
            value={selectedGroup}
            onChange={(e) => setSelectedGroup(e.target.value)}
            style={{ maxWidth: '220px' }}
          >
            <option value="All">All Groups (50)</option>
            {groups.map((g) => (
              <option key={g} value={g}>
                {g} ({eltScenarios.filter((s) => s.group === g).length})
              </option>
            ))}
          </select>
          <select
            className="form-input"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            style={{ maxWidth: '150px' }}
          >
            <option value="id">Sort by #</option>
            <option value="score">Sort by Score</option>
          </select>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadScorecard}
            style={{ marginLeft: 'auto' }}
          >
            Download Scorecard (CSV)
          </button>
        </div>
      </div>

      {filtered.map((s) => {
        const isExpanded = expandedId === s.id;
        return (
          <div key={s.id} className="card" style={{ marginBottom: '0.75rem' }}>
            <div
              onClick={() => setExpandedId(isExpanded ? null : s.id)}
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
                  <span className="badge running">{s.group}</span>
                  <strong>
                    #{s.id} \u2014 {s.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{s.flow}</p>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <span
                  style={{
                    fontSize: '1.1rem',
                    fontWeight: 700,
                    color:
                      s.score >= 4
                        ? 'var(--success)'
                        : s.score >= 3
                          ? 'var(--warning)'
                          : 'var(--error)',
                  }}
                >
                  {s.score}
                </span>
                <span style={{ color: 'var(--text-secondary)' }}>
                  {isExpanded ? '\u25BC' : '\u25B6'}
                </span>
              </div>
            </div>

            {isExpanded && (
              <div
                style={{
                  marginTop: '1rem',
                  display: 'grid',
                  gridTemplateColumns: '250px 1fr',
                  gap: '1rem',
                }}
              >
                <div style={{ padding: '0.6rem', background: '#f8f9fa', borderRadius: '6px' }}>
                  <div style={{ fontSize: '0.72rem', fontWeight: 700, marginBottom: '0.4rem' }}>
                    ELT KPI
                  </div>
                  <ScoreBar label="Complexity" value={s.complexity} max={5} />
                  <ScoreBar label="Volume" value={s.volume} max={5} />
                  <ScoreBar label="SLA" value={s.sla} max={5} />
                  <ScoreBar label="Reliability" value={s.reliability} max={5} />
                  <ScoreBar label="Governance" value={s.governance} max={5} />
                  <div style={{ marginTop: '0.4rem', fontSize: '0.8rem', fontWeight: 700 }}>
                    Overall:{' '}
                    <span
                      style={{
                        color: s.score >= 4 ? '#22c55e' : s.score >= 3 ? '#f59e0b' : '#ef4444',
                      }}
                    >
                      {s.score}/5.0
                    </span>
                  </div>
                </div>
                <div>
                  <div className="code-block" style={{ maxHeight: '350px', overflowY: 'auto' }}>
                    {s.code}
                  </div>
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default ELTScenarios;
