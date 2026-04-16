import React, { useState } from 'react';
import ScenarioCard from '../../components/common/ScenarioCard';
import { exportToCSV } from '../../utils/fileExport';

const etlScenarios = [
  // ─── 1–10: Basic ETL ───
  {
    id: 1,
    group: 'Basic ETL',
    title: 'CSV ETL Pipeline',
    flow: 'Extract CSV \u2192 Transform \u2192 Load Table',
    complexity: 2,
    volume: 3,
    sla: 2,
    reliability: 3,
    governance: 2,
    score: 2.4,
    code: `# CSV ETL: Extract → Transform → Load\nfrom pyspark.sql.functions import trim, lower, current_timestamp, col\n\n# EXTRACT\nraw = spark.read.csv("s3://landing/csv/", header=True, inferSchema=True)\n\n# TRANSFORM (before load)\ncleaned = raw \\\n    .withColumn("name", trim(col("name"))) \\\n    .withColumn("email", lower(col("email"))) \\\n    .filter(col("id").isNotNull()) \\\n    .dropDuplicates(["id"]) \\\n    .withColumn("_etl_ts", current_timestamp())\n\n# LOAD\ncleaned.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.csv_data")\nprint(f"ETL: {raw.count()} raw → {cleaned.count()} loaded")`,
  },
  {
    id: 2,
    group: 'Basic ETL',
    title: 'JSON ETL Pipeline',
    flow: 'Extract JSON \u2192 Parse \u2192 Transform \u2192 Load',
    complexity: 3,
    volume: 4,
    sla: 2,
    reliability: 3,
    governance: 2,
    score: 2.8,
    code: `# JSON ETL: Extract → Parse nested → Transform → Load\nfrom pyspark.sql.functions import col, explode, current_timestamp\n\n# EXTRACT\nraw = spark.read.json("s3://landing/json/")\n\n# TRANSFORM: flatten nested structures\nflat = raw.select(\n    col("id"), col("name"),\n    col("address.city").alias("city"),\n    col("address.zip").alias("zip"),\n    explode("tags").alias("tag")\n).withColumn("_etl_ts", current_timestamp())\n\n# LOAD\nflat.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.json_data")`,
  },
  {
    id: 3,
    group: 'Basic ETL',
    title: 'XML ETL Pipeline',
    flow: 'Extract XML \u2192 Parse \u2192 Transform \u2192 Load',
    complexity: 4,
    volume: 3,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.2,
    code: `# XML ETL\nraw = spark.read.format("com.databricks.spark.xml") \\\n    .option("rowTag", "order").load("/mnt/xml/")\n\n# TRANSFORM\nfrom pyspark.sql.functions import col, to_date\ntransformed = raw.select(\n    col("@id").alias("order_id"),\n    col("customer.name").alias("customer_name"),\n    col("amount").cast("double"),\n    to_date(col("date"), "yyyy-MM-dd").alias("order_date")\n).filter("amount > 0")\n\n# LOAD\ntransformed.write.format("delta").saveAsTable("catalog.silver.xml_orders")`,
  },
  {
    id: 4,
    group: 'Basic ETL',
    title: 'Excel ETL Pipeline',
    flow: 'Extract Excel \u2192 Clean \u2192 Load',
    complexity: 2,
    volume: 2,
    sla: 1,
    reliability: 3,
    governance: 2,
    score: 2.0,
    code: `import pandas as pd\n\n# EXTRACT\npdf = pd.read_excel("/dbfs/mnt/reports/quarterly.xlsx", sheet_name="Sales")\n\n# TRANSFORM (pandas)\npdf.columns = [c.lower().replace(" ", "_") for c in pdf.columns]\npdf = pdf.dropna(subset=["order_id"])\npdf["amount"] = pd.to_numeric(pdf["amount"], errors="coerce").fillna(0)\n\n# LOAD\ndf = spark.createDataFrame(pdf)\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.excel_sales")`,
  },
  {
    id: 5,
    group: 'Basic ETL',
    title: 'Multi-File ETL',
    flow: 'Extract Many Files \u2192 Unify \u2192 Load',
    complexity: 3,
    volume: 4,
    sla: 2,
    reliability: 4,
    governance: 2,
    score: 3.0,
    code: `from functools import reduce\nfrom pyspark.sql.functions import input_file_name, current_timestamp\n\n# EXTRACT: read all file types\ncsv_df = spark.read.csv("/mnt/landing/csv/", header=True)\njson_df = spark.read.json("/mnt/landing/json/")\n\n# TRANSFORM: unify schema\ncsv_norm = csv_df.select("id", "name", "amount").withColumn("source", lit("csv"))\njson_norm = json_df.select("id", "name", "amount").withColumn("source", lit("json"))\nunified = csv_norm.unionByName(json_norm)\n\n# LOAD\nunified.withColumn("_etl_ts", current_timestamp()) \\\n    .write.format("delta").mode("append").saveAsTable("catalog.silver.unified_data")`,
  },
  {
    id: 6,
    group: 'Basic ETL',
    title: 'Archive ETL',
    flow: 'Extract Archive \u2192 Batch Transform \u2192 Load',
    complexity: 3,
    volume: 5,
    sla: 1,
    reliability: 4,
    governance: 2,
    score: 3.0,
    code: `# EXTRACT: historical archive\nfor year in range(2020, 2025):\n    df = spark.read.parquet(f"s3://archive/{year}/")\n    \n    # TRANSFORM per year\n    transformed = df.withColumn("fiscal_year", lit(year)) \\\n        .filter(col("status") != "CANCELLED")\n    \n    # LOAD\n    transformed.write.format("delta").mode("append") \\\n        .partitionBy("fiscal_year").saveAsTable("catalog.silver.historical")`,
  },
  {
    id: 7,
    group: 'Basic ETL',
    title: 'Schema-Mapping ETL',
    flow: 'Extract \u2192 Map Schema \u2192 Load',
    complexity: 3,
    volume: 4,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.2,
    code: `# Schema mapping: source columns → canonical target\ncolumn_map = {\n    "CUST_ID": "customer_id", "CUST_NM": "name",\n    "CUST_EMAIL": "email", "ORD_AMT": "amount",\n    "ORD_DT": "order_date"\n}\n\nraw = spark.read.format("jdbc").option("dbtable", "LEGACY_ORDERS").load()\n\n# TRANSFORM: apply mapping\nfor old, new in column_map.items():\n    raw = raw.withColumnRenamed(old, new)\n\n# Type casting\ntransformed = raw.withColumn("amount", col("amount").cast("decimal(18,2)")) \\\n    .withColumn("order_date", to_date(col("order_date"), "yyyyMMdd"))\n\ntransformed.write.format("delta").saveAsTable("catalog.silver.orders_canonical")`,
  },
  {
    id: 8,
    group: 'Basic ETL',
    title: 'Data Type Transformation ETL',
    flow: 'Extract \u2192 Cast Types \u2192 Load',
    complexity: 2,
    volume: 4,
    sla: 2,
    reliability: 3,
    governance: 2,
    score: 2.6,
    code: `raw = spark.read.csv("/mnt/data/mixed_types.csv", header=True)\n\n# TRANSFORM: enforce types\ntransformed = raw.select(\n    col("id").cast("long"),\n    col("name").cast("string"),\n    col("amount").cast("decimal(18,2)"),\n    to_date(col("date_str"), "MM/dd/yyyy").alias("event_date"),\n    col("is_active").cast("boolean")\n)\n\ntransformed.write.format("delta").saveAsTable("catalog.silver.typed_data")`,
  },
  {
    id: 9,
    group: 'Basic ETL',
    title: 'Aggregation ETL',
    flow: 'Extract \u2192 Aggregate \u2192 Load',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 2,
    score: 3.2,
    code: `from pyspark.sql.functions import sum as spark_sum, count, avg, max as spark_max\n\nraw = spark.table("catalog.bronze.transactions")\n\n# TRANSFORM: aggregate\nagg = raw.groupBy("customer_id", "product_category").agg(\n    count("*").alias("transaction_count"),\n    spark_sum("amount").alias("total_amount"),\n    avg("amount").alias("avg_amount"),\n    spark_max("transaction_date").alias("last_transaction")\n)\n\nagg.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.customer_product_summary")`,
  },
  {
    id: 10,
    group: 'Basic ETL',
    title: 'Snapshot ETL',
    flow: 'Extract \u2192 Snapshot \u2192 Load History',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `from pyspark.sql.functions import current_date, lit\n\n# EXTRACT current state\ncurrent = spark.read.format("jdbc").option("dbtable", "inventory").load()\n\n# TRANSFORM: add snapshot metadata\nsnapshot = current.withColumn("snapshot_date", current_date()) \\\n    .withColumn("snapshot_type", lit("DAILY"))\n\n# LOAD: append to history (preserves all snapshots)\nsnapshot.write.format("delta").mode("append") \\\n    .partitionBy("snapshot_date") \\\n    .saveAsTable("catalog.silver.inventory_history")`,
  },

  // ─── 11–20: Database ETL ───
  {
    id: 11,
    group: 'Database ETL',
    title: 'Full DB ETL',
    flow: 'Extract DB \u2192 Transform \u2192 Load',
    complexity: 2,
    volume: 4,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.0,
    code: `raw = spark.read.format("jdbc").option("url", jdbc_url) \\\n    .option("dbtable", "orders").option("numPartitions", 8).load()\n\n# TRANSFORM\ntransformed = raw.filter("status != 'CANCELLED'") \\\n    .withColumn("amount_usd", col("amount") * col("exchange_rate")) \\\n    .drop("exchange_rate")\n\ntransformed.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.orders")`,
  },
  {
    id: 12,
    group: 'Database ETL',
    title: 'Incremental ETL',
    flow: 'Extract Delta \u2192 Transform \u2192 Load',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 3.6,
    code: `# Watermark-based incremental\nwatermark = spark.sql("SELECT MAX(modified_at) FROM catalog.silver.orders").collect()[0][0]\n\nincremental = spark.read.format("jdbc") \\\n    .option("dbtable", f"(SELECT * FROM orders WHERE modified_at > '{watermark}') t").load()\n\n# TRANSFORM\ntransformed = incremental.withColumn("amount_usd", col("amount") * 1.0) \\\n    .withColumn("_etl_ts", current_timestamp())\n\n# MERGE into target\nfrom delta.tables import DeltaTable\ntarget = DeltaTable.forName(spark, "catalog.silver.orders")\ntarget.alias("t").merge(transformed.alias("s"), "t.order_id = s.order_id") \\\n    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`,
  },
  {
    id: 13,
    group: 'Database ETL',
    title: 'CDC ETL Pipeline',
    flow: 'CDC Logs \u2192 Transform \u2192 Merge Load',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `from delta.tables import DeltaTable\n\n# EXTRACT CDC log\ncdc = spark.read.format("jdbc").option("dbtable", "cdc_change_log").load()\n\n# TRANSFORM: apply business rules\ntransformed = cdc.filter("operation IN ('INSERT', 'UPDATE')") \\\n    .withColumn("processed_at", current_timestamp())\n\ndeletes = cdc.filter("operation = 'DELETE'")\n\n# LOAD: merge\ntarget = DeltaTable.forName(spark, "catalog.silver.customers")\ntarget.alias("t").merge(transformed.alias("s"), "t.id = s.id") \\\n    .whenMatchedUpdateAll() \\\n    .whenNotMatchedInsertAll() \\\n    .execute()\n\n# Apply deletes\nfor row in deletes.collect():\n    spark.sql(f"DELETE FROM catalog.silver.customers WHERE id = {row.id}")`,
  },
  {
    id: 14,
    group: 'Database ETL',
    title: 'Multi-Table ETL',
    flow: 'Extract Multiple Tables \u2192 Join \u2192 Load',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `# EXTRACT multiple tables\norders = spark.read.format("jdbc").option("dbtable", "orders").load()\ncustomers = spark.read.format("jdbc").option("dbtable", "customers").load()\nproducts = spark.read.format("jdbc").option("dbtable", "products").load()\n\n# TRANSFORM: join and enrich\nresult = orders \\\n    .join(customers, "customer_id") \\\n    .join(products, "product_id") \\\n    .select("order_id", "customer_name", "product_name", "quantity", "amount", "order_date") \\\n    .withColumn("total", col("quantity") * col("amount"))\n\n# LOAD\nresult.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.order_details")`,
  },
  {
    id: 15,
    group: 'Database ETL',
    title: 'ERP ETL Pipeline',
    flow: 'ERP \u2192 Transform \u2192 Finance Model',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `# ERP Finance ETL\ngl = spark.read.format("jdbc").option("dbtable", "GL_ENTRIES").load()\ncoa = spark.read.format("jdbc").option("dbtable", "CHART_OF_ACCOUNTS").load()\n\n# TRANSFORM: apply finance rules\nresult = gl.join(coa, "account_code") \\\n    .withColumn("amount_usd", when(col("currency")=="EUR", col("amount")*1.08).otherwise(col("amount"))) \\\n    .withColumn("fiscal_period", concat(col("fiscal_year"), lit("-"), lpad(col("fiscal_month"), 2, "0"))) \\\n    .groupBy("fiscal_period", "account_type", "cost_center") \\\n    .agg(spark_sum("amount_usd").alias("total_amount"))\n\nresult.write.format("delta").saveAsTable("catalog.gold.finance_summary")`,
  },
  {
    id: 16,
    group: 'Database ETL',
    title: 'CRM ETL Pipeline',
    flow: 'CRM \u2192 Transform \u2192 Customer Model',
    complexity: 3,
    volume: 3,
    sla: 3,
    reliability: 4,
    governance: 4,
    score: 3.4,
    code: `from simple_salesforce import Salesforce\nsf = Salesforce(username=dbutils.secrets.get("sf","user"), password=dbutils.secrets.get("sf","pass"), security_token=dbutils.secrets.get("sf","token"))\n\n# EXTRACT\naccounts = spark.createDataFrame(sf.bulk.Account.query("SELECT Id, Name, Industry, AnnualRevenue FROM Account"))\n\n# TRANSFORM\ntransformed = accounts \\\n    .withColumn("segment", when(col("AnnualRevenue")>1000000, "Enterprise").when(col("AnnualRevenue")>100000, "Mid-Market").otherwise("SMB")) \\\n    .withColumnRenamed("Id", "sf_id")\n\n# LOAD\ntransformed.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.crm_accounts")`,
  },
  {
    id: 17,
    group: 'Database ETL',
    title: 'HR ETL Pipeline',
    flow: 'HR \u2192 Secure Transform \u2192 Load',
    complexity: 3,
    volume: 2,
    sla: 2,
    reliability: 4,
    governance: 5,
    score: 3.2,
    code: `# HR ETL with PII masking\nraw = spark.read.format("jdbc").option("dbtable", "employees").load()\n\n# TRANSFORM: mask before load\nfrom pyspark.sql.functions import sha2, regexp_replace\ntransformed = raw \\\n    .withColumn("ssn_hash", sha2("ssn", 256)) \\\n    .withColumn("salary_band", when(col("salary")<50000,"Band1").when(col("salary")<100000,"Band2").otherwise("Band3")) \\\n    .drop("ssn", "salary", "bank_account")\n\n# LOAD to secure zone\ntransformed.write.format("delta").saveAsTable("catalog.secure.hr_employees")`,
  },
  {
    id: 18,
    group: 'Database ETL',
    title: 'Banking ETL Pipeline',
    flow: 'Banking \u2192 Transform \u2192 Load',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `# Banking ETL: heavy transform + PII masking\nraw = spark.read.format("jdbc").option("url","jdbc:db2://core:50000/BANK") \\\n    .option("dbtable","DAILY_TRANSACTIONS").option("numPartitions",16).load()\n\n# TRANSFORM\nfrom pyspark.sql.functions import sha2, concat, lit, abs as spark_abs\ntransformed = raw \\\n    .withColumn("acct_hash", sha2(concat("account_no", lit("SALT")), 256)) \\\n    .withColumn("amount_abs", spark_abs("amount")) \\\n    .withColumn("txn_type", when(col("amount")>0, "CREDIT").otherwise("DEBIT")) \\\n    .withColumn("risk_flag", when(spark_abs(col("amount"))>10000, True).otherwise(False)) \\\n    .drop("account_no", "ssn")\n\n# LOAD\ntransformed.write.format("delta").mode("append").saveAsTable("catalog.secure.bank_transactions")`,
  },
  {
    id: 19,
    group: 'Database ETL',
    title: 'POS ETL Pipeline',
    flow: 'POS \u2192 Transform \u2192 Load Sales',
    complexity: 3,
    volume: 5,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.6,
    code: `# POS multi-store ETL\nstores = ["nyc","la","chi","sf"]\nall_dfs = []\nfor store in stores:\n    df = spark.read.format("jdbc").option("url", f"jdbc:mysql://{store}-db/pos").option("dbtable", "transactions").load()\n    df = df.withColumn("store_id", lit(store)) \\\n        .withColumn("txn_date", to_date("transaction_timestamp")) \\\n        .withColumn("total", col("quantity") * col("unit_price"))\n    all_dfs.append(df)\n\nfrom functools import reduce\nmerged = reduce(lambda a,b: a.unionByName(b), all_dfs)\nmerged.write.format("delta").mode("append").saveAsTable("catalog.silver.pos_sales")`,
  },
  {
    id: 20,
    group: 'Database ETL',
    title: 'Inventory ETL Pipeline',
    flow: 'Inventory \u2192 Transform \u2192 Stock Model',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `raw = spark.read.format("jdbc").option("dbtable", "inventory_levels").load()\n\n# TRANSFORM: calculate metrics\ntransformed = raw \\\n    .withColumn("days_of_supply", col("quantity") / col("avg_daily_demand")) \\\n    .withColumn("reorder_flag", when(col("quantity") < col("reorder_point"), True).otherwise(False)) \\\n    .withColumn("stock_status", when(col("quantity")==0, "OUT_OF_STOCK").when(col("reorder_flag"), "LOW").otherwise("OK"))\n\ntransformed.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.inventory_status")`,
  },

  // ─── 21–30: Transformation-Heavy ETL ───
  {
    id: 21,
    group: 'Transform-Heavy',
    title: 'Cleansing ETL',
    flow: 'Extract \u2192 Clean \u2192 Load',
    complexity: 3,
    volume: 5,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `raw = spark.table("catalog.bronze.raw_data")\n\n# TRANSFORM: comprehensive cleaning\ncleaned = raw \\\n    .dropDuplicates(["id"]) \\\n    .filter(col("id").isNotNull()) \\\n    .withColumn("name", trim(initcap(col("name")))) \\\n    .withColumn("email", lower(trim(col("email")))) \\\n    .withColumn("phone", regexp_replace("phone", "[^0-9]", "")) \\\n    .withColumn("amount", col("amount").cast("decimal(18,2)")) \\\n    .na.fill({"name": "Unknown", "amount": 0})\n\ncleaned.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.clean_data")`,
  },
  {
    id: 22,
    group: 'Transform-Heavy',
    title: 'Deduplication ETL',
    flow: 'Extract \u2192 Dedupe \u2192 Load',
    complexity: 4,
    volume: 5,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 4.0,
    code: `from pyspark.sql.window import Window\nfrom pyspark.sql.functions import row_number\n\nraw = spark.table("catalog.bronze.customers")\n\n# TRANSFORM: keep latest record per email\nwindow = Window.partitionBy("email").orderBy(col("updated_at").desc())\ndeduped = raw.withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn")\n\n# Also fuzzy dedupe on name similarity\n# (simplified: exact match on normalized name)\ndeduped = deduped.withColumn("name_norm", lower(trim(regexp_replace("name", "[^a-zA-Z ]", "")))) \\\n    .dropDuplicates(["name_norm", "phone"])\n\ndeduped.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.customers_deduped")`,
  },
  {
    id: 23,
    group: 'Transform-Heavy',
    title: 'Conformance ETL',
    flow: 'Extract \u2192 Standardize \u2192 Load',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `# Conform multiple source schemas to canonical model\nraw = spark.table("catalog.bronze.multi_source")\n\n# TRANSFORM: canonical schema\ncanonical = raw.select(\n    coalesce(col("customer_id"), col("cust_id"), col("id")).alias("customer_id"),\n    coalesce(col("full_name"), col("name"), concat("first_name", lit(" "), "last_name")).alias("name"),\n    lower(coalesce(col("email_address"), col("email"), col("e_mail"))).alias("email"),\n    coalesce(col("order_amount"), col("amount"), col("total")).cast("decimal(18,2)").alias("amount"),\n    coalesce(to_date("order_date","yyyy-MM-dd"), to_date("ord_dt","yyyyMMdd"), to_date("date","MM/dd/yyyy")).alias("order_date")\n)\n\ncanonical.write.format("delta").saveAsTable("catalog.silver.orders_canonical")`,
  },
  {
    id: 24,
    group: 'Transform-Heavy',
    title: 'PII Masking ETL',
    flow: 'Extract \u2192 Mask \u2192 Load',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `from pyspark.sql.functions import sha2, regexp_replace, lit, concat\n\nraw = spark.table("catalog.bronze.customers")\n\n# TRANSFORM: mask all PII before loading\nmasked = raw \\\n    .withColumn("email_hash", sha2("email", 256)) \\\n    .withColumn("phone_masked", concat(lit("***-***-"), col("phone").substr(-4, 4))) \\\n    .withColumn("ssn_masked", concat(lit("***-**-"), col("ssn").substr(-4, 4))) \\\n    .withColumn("name_masked", concat(col("name").substr(1,1), lit("***"))) \\\n    .drop("email", "phone", "ssn", "name") \\\n    .withColumnRenamed("name_masked", "name")\n\nmasked.write.format("delta").saveAsTable("catalog.secure.customers_masked")`,
  },
  {
    id: 25,
    group: 'Transform-Heavy',
    title: 'Data Quality ETL',
    flow: 'Extract \u2192 Validate \u2192 Load',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `raw = spark.table("catalog.bronze.orders")\n\n# TRANSFORM: validate with quality rules\nvalid = raw.filter(\n    (col("order_id").isNotNull()) &\n    (col("amount") > 0) &\n    (col("amount") < 1000000) &\n    (col("order_date") >= "2020-01-01") &\n    (col("customer_id").isNotNull())\n)\n\nquarantine = raw.subtract(valid)\n\n# LOAD: good data → silver, bad data → quarantine\nvalid.write.format("delta").saveAsTable("catalog.silver.orders")\nquarantine.write.format("delta").saveAsTable("catalog.quarantine.orders")\n\nprint(f"Valid: {valid.count()}, Quarantined: {quarantine.count()}")`,
  },
  {
    id: 26,
    group: 'Transform-Heavy',
    title: 'Enrichment ETL',
    flow: 'Extract + Lookup \u2192 Transform \u2192 Load',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.6,
    code: `orders = spark.table("catalog.bronze.orders")\ncustomers = spark.table("catalog.silver.customer_dim")\ngeo = spark.table("catalog.reference.geo_lookup")\n\n# TRANSFORM: enrich with lookups\nenriched = orders \\\n    .join(customers, "customer_id", "left") \\\n    .join(geo, orders.zip_code == geo.zip, "left") \\\n    .withColumn("customer_segment", coalesce("segment", lit("Unknown"))) \\\n    .withColumn("region", coalesce("geo_region", lit("Unknown"))) \\\n    .withColumn("is_new_customer", col("first_order_date") == col("order_date"))\n\nenriched.write.format("delta").saveAsTable("catalog.silver.orders_enriched")`,
  },
  {
    id: 27,
    group: 'Transform-Heavy',
    title: 'Join-Heavy ETL',
    flow: 'Extract Many Tables \u2192 Join \u2192 Load',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `# 6-table join ETL\norders = spark.table("catalog.bronze.orders")\nlines = spark.table("catalog.bronze.order_lines")\ncustomers = spark.table("catalog.bronze.customers")\nproducts = spark.table("catalog.bronze.products")\nstores = spark.table("catalog.bronze.stores")\npromotions = spark.table("catalog.bronze.promotions")\n\n# TRANSFORM: star schema join\nresult = lines \\\n    .join(orders, "order_id") \\\n    .join(customers, "customer_id") \\\n    .join(products, "product_id") \\\n    .join(stores, "store_id") \\\n    .join(promotions, lines.promo_code == promotions.code, "left") \\\n    .select("order_id","customer_name","product_name","store_name", \\\n        "quantity","unit_price","discount_pct","order_date")\n\nresult.write.format("delta").saveAsTable("catalog.silver.order_details_full")`,
  },
  {
    id: 28,
    group: 'Transform-Heavy',
    title: 'SCD ETL',
    flow: 'Extract \u2192 SCD Logic \u2192 Load Dimension',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `from delta.tables import DeltaTable\n\nsource = spark.read.format("jdbc").option("dbtable", "customers").load()\ntarget = DeltaTable.forName(spark, "catalog.silver.customers_scd2")\n\n# TRANSFORM + LOAD: SCD Type 2 merge\ntarget.alias("t").merge(source.alias("s"),\n    "t.customer_id = s.customer_id AND t.is_current = true"\n).whenMatchedUpdate(\n    condition="t.email != s.email OR t.city != s.city OR t.tier != s.tier",\n    set={"is_current": "false", "valid_to": "current_date()"}\n).whenNotMatchedInsert(values={\n    "customer_id": "s.customer_id", "name": "s.name", "email": "s.email",\n    "city": "s.city", "tier": "s.tier",\n    "is_current": "true", "valid_from": "current_date()", "valid_to": "lit('9999-12-31')"\n}).execute()`,
  },
  {
    id: 29,
    group: 'Transform-Heavy',
    title: 'Late Data ETL',
    flow: 'Extract \u2192 Reconcile \u2192 Load',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `from delta.tables import DeltaTable\n\n# EXTRACT late-arriving data\nlate = spark.read.format("jdbc").option("dbtable", "late_arrivals").load()\n\n# TRANSFORM: flag as late\ntransformed = late.withColumn("is_late", lit(True)) \\\n    .withColumn("original_date", col("event_date")) \\\n    .withColumn("processed_date", current_date())\n\n# LOAD: merge into existing\ntarget = DeltaTable.forName(spark, "catalog.silver.events")\ntarget.alias("t").merge(transformed.alias("s"), "t.event_id = s.event_id") \\\n    .whenMatchedUpdateAll() \\\n    .whenNotMatchedInsertAll() \\\n    .execute()`,
  },
  {
    id: 30,
    group: 'Transform-Heavy',
    title: 'Multi-Source ETL',
    flow: 'Extract Many Sources \u2192 Unify \u2192 Load',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.6,
    code: `# Unify 3 customer sources with different schemas\ncrm = spark.table("catalog.bronze.crm").select(\n    col("sf_id").alias("source_id"), lit("CRM").alias("source"),\n    col("Name").alias("name"), col("Email").alias("email"))\n\nerp = spark.table("catalog.bronze.erp").select(\n    col("CUST_NO").alias("source_id"), lit("ERP").alias("source"),\n    col("FULL_NAME").alias("name"), col("EMAIL_ADDR").alias("email"))\n\nweb = spark.table("catalog.bronze.web_users").select(\n    col("user_id").alias("source_id"), lit("WEB").alias("source"),\n    col("display_name").alias("name"), col("email"))\n\n# TRANSFORM: unify + dedupe across sources\nunified = crm.unionByName(erp).unionByName(web)\nmaster = unified.withColumn("email_norm", lower(trim("email"))) \\\n    .dropDuplicates(["email_norm"])\n\nmaster.write.format("delta").saveAsTable("catalog.silver.customer_master")`,
  },

  // ─── 31–40: Analytics / DW ETL ───
  {
    id: 31,
    group: 'Analytics/DW ETL',
    title: 'Fact Table ETL',
    flow: 'Extract \u2192 Transform \u2192 Load Fact',
    complexity: 3,
    volume: 5,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `# Build fact_sales from silver tables\nresult = spark.sql("""\nSELECT o.order_id, o.order_date, o.customer_id, c.segment,\n    o.product_id, p.category, o.quantity, o.unit_price,\n    o.quantity * o.unit_price AS total_amount,\n    o.discount_pct, o.quantity * o.unit_price * (1 - o.discount_pct/100) AS net_amount\nFROM catalog.silver.orders o\nJOIN catalog.silver.customers c ON o.customer_id = c.customer_id\nJOIN catalog.silver.products p ON o.product_id = p.product_id\nWHERE o.status = 'COMPLETED'\n""")\nresult.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.fact_sales")`,
  },
  {
    id: 32,
    group: 'Analytics/DW ETL',
    title: 'Dimension ETL',
    flow: 'Extract \u2192 Transform \u2192 Load Dimension',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 3.8,
    code: `result = spark.sql("""\nSELECT customer_id, name, email, segment, region, city,\n    MIN(first_order_date) AS customer_since,\n    COUNT(DISTINCT order_id) AS lifetime_orders,\n    SUM(amount) AS lifetime_value,\n    CASE WHEN SUM(amount) > 10000 THEN 'VIP'\n         WHEN SUM(amount) > 1000 THEN 'Regular' ELSE 'New' END AS tier\nFROM catalog.silver.orders o JOIN catalog.silver.customers c USING(customer_id)\nGROUP BY customer_id, name, email, segment, region, city, first_order_date\n""")\nresult.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.dim_customer")`,
  },
  {
    id: 33,
    group: 'Analytics/DW ETL',
    title: 'KPI ETL',
    flow: 'Extract \u2192 Calculate \u2192 Load Metrics',
    complexity: 3,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 3,
    score: 3.8,
    code: `result = spark.sql("""\nSELECT order_date,\n    COUNT(DISTINCT order_id) AS orders,\n    COUNT(DISTINCT customer_id) AS unique_customers,\n    SUM(amount) AS revenue,\n    AVG(amount) AS avg_order_value,\n    SUM(amount) / COUNT(DISTINCT customer_id) AS revenue_per_customer\nFROM catalog.silver.orders\nWHERE status = 'COMPLETED'\nGROUP BY order_date\n""")\nresult.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.kpi_daily")`,
  },
  {
    id: 34,
    group: 'Analytics/DW ETL',
    title: 'Customer 360 ETL',
    flow: 'Multi-Source \u2192 Transform \u2192 Unified Model',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.6,
    code: `# Customer 360: merge CRM + orders + support + web behavior\ncrm = spark.table("catalog.silver.crm_accounts")\norders = spark.table("catalog.silver.orders").groupBy("customer_id").agg(\n    count("*").alias("total_orders"), spark_sum("amount").alias("ltv"))\nsupport = spark.table("catalog.silver.support_tickets").groupBy("customer_id").agg(\n    count("*").alias("total_tickets"), avg("resolution_hours").alias("avg_resolution"))\nweb = spark.table("catalog.silver.web_sessions").groupBy("customer_id").agg(\n    count("*").alias("sessions"), avg("pages_viewed").alias("avg_pages"))\n\nc360 = crm.join(orders, "customer_id", "left") \\\n    .join(support, "customer_id", "left") \\\n    .join(web, "customer_id", "left")\n\nc360.write.format("delta").saveAsTable("catalog.gold.customer_360")`,
  },
  {
    id: 35,
    group: 'Analytics/DW ETL',
    title: 'Finance Reporting ETL',
    flow: 'Extract \u2192 Finance Logic \u2192 Load Mart',
    complexity: 5,
    volume: 4,
    sla: 5,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `# Finance reporting mart\nresult = spark.sql("""\nSELECT fiscal_period, account_type, cost_center,\n    SUM(CASE WHEN entry_type='DEBIT' THEN amount ELSE 0 END) AS total_debit,\n    SUM(CASE WHEN entry_type='CREDIT' THEN amount ELSE 0 END) AS total_credit,\n    SUM(CASE WHEN entry_type='DEBIT' THEN amount ELSE -amount END) AS net_amount,\n    COUNT(*) AS entry_count\nFROM catalog.silver.gl_entries\nGROUP BY fiscal_period, account_type, cost_center\n""")\nresult.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.finance_report")`,
  },
  {
    id: 36,
    group: 'Analytics/DW ETL',
    title: 'Supply Chain ETL',
    flow: 'Extract \u2192 Logistics Transform \u2192 Load',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `orders = spark.table("catalog.silver.purchase_orders")\nshipments = spark.table("catalog.silver.shipments")\ninventory = spark.table("catalog.silver.inventory")\n\nresult = orders.join(shipments, "po_number", "left") \\\n    .join(inventory, "product_id", "left") \\\n    .withColumn("delivery_days", datediff("actual_delivery", "order_date")) \\\n    .withColumn("on_time", col("delivery_days") <= col("promised_days")) \\\n    .withColumn("stock_status", when(col("quantity_available")>col("reorder_point"),"OK").otherwise("LOW"))\n\nresult.write.format("delta").saveAsTable("catalog.gold.supply_chain_metrics")`,
  },
  {
    id: 37,
    group: 'Analytics/DW ETL',
    title: 'Product Analytics ETL',
    flow: 'Extract \u2192 Product Transform \u2192 Load',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.6,
    code: `result = spark.sql("""\nSELECT p.product_id, p.name, p.category,\n    COUNT(DISTINCT o.order_id) AS orders,\n    SUM(o.quantity) AS units_sold,\n    SUM(o.amount) AS revenue,\n    AVG(r.rating) AS avg_rating,\n    COUNT(r.review_id) AS review_count\nFROM catalog.silver.products p\nLEFT JOIN catalog.silver.orders o ON p.product_id = o.product_id\nLEFT JOIN catalog.silver.reviews r ON p.product_id = r.product_id\nGROUP BY p.product_id, p.name, p.category\n""")\nresult.write.format("delta").saveAsTable("catalog.gold.product_analytics")`,
  },
  {
    id: 38,
    group: 'Analytics/DW ETL',
    title: 'Trend ETL',
    flow: 'Extract History \u2192 Transform \u2192 Load Trend',
    complexity: 4,
    volume: 5,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 4.0,
    code: `from pyspark.sql.window import Window\n\nresult = spark.sql("SELECT order_date, SUM(amount) AS daily_revenue FROM catalog.silver.orders GROUP BY order_date ORDER BY order_date")\n\nwindow_7d = Window.orderBy("order_date").rowsBetween(-6, 0)\nwindow_30d = Window.orderBy("order_date").rowsBetween(-29, 0)\n\ntrend = result \\\n    .withColumn("revenue_7d_avg", avg("daily_revenue").over(window_7d)) \\\n    .withColumn("revenue_30d_avg", avg("daily_revenue").over(window_30d)) \\\n    .withColumn("trend", when(col("revenue_7d_avg")>col("revenue_30d_avg"),"UP").otherwise("DOWN"))\n\ntrend.write.format("delta").saveAsTable("catalog.gold.revenue_trend")`,
  },
  {
    id: 39,
    group: 'Analytics/DW ETL',
    title: 'Reconciliation ETL',
    flow: 'Extract \u2192 Compare \u2192 Load Audit',
    complexity: 4,
    volume: 3,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `from pyspark.sql.functions import md5, concat_ws\n\nsource = spark.read.format("jdbc").option("dbtable", "orders").load()\ntarget = spark.table("catalog.silver.orders")\n\n# TRANSFORM: compare\nsource_agg = source.agg(count("*").alias("src_count"), spark_sum("amount").alias("src_sum")).collect()[0]\ntarget_agg = target.agg(count("*").alias("tgt_count"), spark_sum("amount").alias("tgt_sum")).collect()[0]\n\nrecon = spark.createDataFrame([{\n    "check_ts": str(current_timestamp()),\n    "source_count": source_agg.src_count, "target_count": target_agg.tgt_count,\n    "count_match": source_agg.src_count == target_agg.tgt_count,\n    "source_sum": float(source_agg.src_sum), "target_sum": float(target_agg.tgt_sum),\n    "sum_match": abs(float(source_agg.src_sum) - float(target_agg.tgt_sum)) < 0.01\n}])\nrecon.write.format("delta").mode("append").saveAsTable("catalog.audit.reconciliation")`,
  },
  {
    id: 40,
    group: 'Analytics/DW ETL',
    title: 'Semantic Layer ETL',
    flow: 'Extract \u2192 Transform \u2192 BI-Ready Model',
    complexity: 4,
    volume: 3,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `# Build BI-ready semantic layer\nspark.sql("""\nCREATE OR REPLACE TABLE catalog.gold.bi_sales_model AS\nSELECT\n    f.order_date, f.order_id,\n    c.name AS customer_name, c.segment, c.region,\n    p.name AS product_name, p.category, p.subcategory,\n    f.quantity, f.unit_price, f.total_amount, f.net_amount,\n    YEAR(f.order_date) AS year, MONTH(f.order_date) AS month,\n    QUARTER(f.order_date) AS quarter,\n    DAYOFWEEK(f.order_date) AS day_of_week\nFROM catalog.gold.fact_sales f\nJOIN catalog.gold.dim_customer c ON f.customer_id = c.customer_id\nJOIN catalog.gold.dim_product p ON f.product_id = p.product_id\n""")`,
  },

  // ─── 41–50: Advanced ETL ───
  {
    id: 41,
    group: 'Advanced/AI ETL',
    title: 'Feature Engineering ETL',
    flow: 'Extract \u2192 Transform \u2192 ML Features',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.6,
    code: `# ML feature engineering\norders = spark.table("catalog.silver.orders")\n\nfeatures = orders.groupBy("customer_id").agg(\n    count("*").alias("total_orders"),\n    spark_sum("amount").alias("lifetime_value"),\n    avg("amount").alias("avg_order_value"),\n    datediff(current_date(), spark_max("order_date")).alias("days_since_last"),\n    countDistinct("product_id").alias("unique_products"),\n    spark_sum(when(col("amount")>100,1).otherwise(0)).alias("high_value_orders")\n)\n\nfrom databricks.feature_engineering import FeatureEngineeringClient\nfe = FeatureEngineeringClient()\nfe.create_table(name="catalog.ml.customer_features", primary_keys=["customer_id"], df=features)`,
  },
  {
    id: 42,
    group: 'Advanced/AI ETL',
    title: 'ML Dataset ETL',
    flow: 'Extract \u2192 Curate \u2192 Load ML Dataset',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.6,
    code: `features = spark.table("catalog.ml.customer_features")\nlabels = spark.table("catalog.silver.churn_labels")\n\n# TRANSFORM: join features + labels, split\ndataset = features.join(labels, "customer_id")\ntrain, test = dataset.randomSplit([0.8, 0.2], seed=42)\n\ntrain.write.format("delta").saveAsTable("catalog.ml.train_data")\ntest.write.format("delta").saveAsTable("catalog.ml.test_data")\nprint(f"Train: {train.count()}, Test: {test.count()}")`,
  },
  {
    id: 43,
    group: 'Advanced/AI ETL',
    title: 'RAG ETL',
    flow: 'Extract Docs \u2192 Chunk \u2192 Load',
    complexity: 5,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.4,
    code: `from pyspark.sql.functions import udf, explode\nfrom pyspark.sql.types import ArrayType, StringType\n\n@udf(ArrayType(StringType()))\ndef chunk_text(text, size=500, overlap=50):\n    if not text: return []\n    return [text[i:i+size] for i in range(0, len(text), size-overlap)]\n\ndocs = spark.table("catalog.bronze.documents")\nchunked = docs.withColumn("chunks", chunk_text("content")) \\\n    .select("doc_id", "title", explode("chunks").alias("chunk_text")) \\\n    .withColumn("chunk_id", monotonically_increasing_id())\n\nchunked.write.format("delta").saveAsTable("catalog.ml.doc_chunks")`,
  },
  {
    id: 44,
    group: 'Advanced/AI ETL',
    title: 'Embedding ETL',
    flow: 'Extract Text \u2192 Embed \u2192 Load Vector',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.6,
    code: `from sentence_transformers import SentenceTransformer\nimport pandas as pd\n\nmodel = SentenceTransformer("all-MiniLM-L6-v2")\n\n@pandas_udf("array<float>")\ndef generate_embedding(texts: pd.Series) -> pd.Series:\n    embeddings = model.encode(texts.tolist())\n    return pd.Series(embeddings.tolist())\n\nchunks = spark.table("catalog.ml.doc_chunks")\nwith_embeddings = chunks.withColumn("embedding", generate_embedding("chunk_text"))\nwith_embeddings.write.format("delta").saveAsTable("catalog.ml.embeddings")`,
  },
  {
    id: 45,
    group: 'Advanced/AI ETL',
    title: 'Masking ETL',
    flow: 'Extract \u2192 Secure Transform \u2192 Load',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `raw = spark.table("catalog.bronze.sensitive_data")\n\nmasked = raw \\\n    .withColumn("ssn", concat(lit("***-**-"), col("ssn").substr(-4,4))) \\\n    .withColumn("email", sha2("email", 256)) \\\n    .withColumn("phone", concat(lit("***-***-"), col("phone").substr(-4,4))) \\\n    .withColumn("dob", date_trunc("year", "date_of_birth")) \\\n    .drop("credit_card", "bank_account")\n\nmasked.write.format("delta").saveAsTable("catalog.secure.masked_data")`,
  },
  {
    id: 46,
    group: 'Advanced/AI ETL',
    title: 'Audit ETL',
    flow: 'Extract Logs \u2192 Transform \u2192 Load Audit',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `logs = spark.read.json("/mnt/audit/system_logs/")\n\n# TRANSFORM: parse and categorize\naudit = logs \\\n    .withColumn("event_category", when(col("action").contains("DELETE"), "DESTRUCTIVE") \\\n        .when(col("action").contains("CREATE"), "CREATIVE") \\\n        .otherwise("READ")) \\\n    .withColumn("risk_level", when(col("event_category")=="DESTRUCTIVE", "HIGH").otherwise("NORMAL"))\n\naudit.write.format("delta").mode("append").saveAsTable("catalog.audit.system_events")`,
  },
  {
    id: 47,
    group: 'Advanced/AI ETL',
    title: 'Lineage ETL',
    flow: 'Extract Metadata \u2192 Transform \u2192 Lineage Model',
    complexity: 4,
    volume: 3,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `# Build lineage graph from job metadata\njob_runs = spark.table("catalog.audit.job_metrics")\n\nlineage = job_runs.select(\n    col("input_tables").alias("source"),\n    col("output_table").alias("target"),\n    col("job_name").alias("pipeline"),\n    col("run_time"),\n    col("rows_processed")\n).withColumn("lineage_edge", concat("source", lit(" -> "), "target"))\n\nlineage.write.format("delta").mode("append").saveAsTable("catalog.audit.data_lineage")`,
  },
  {
    id: 48,
    group: 'Advanced/AI ETL',
    title: 'Cost ETL',
    flow: 'Extract Usage \u2192 Transform \u2192 Cost Model',
    complexity: 3,
    volume: 3,
    sla: 3,
    reliability: 4,
    governance: 4,
    score: 3.4,
    code: `import requests\nheaders = {"Authorization": f"Bearer {dbutils.secrets.get('cloud','token')}"}\n\n# EXTRACT cloud costs\ncost_data = requests.get("https://management.azure.com/.../costManagement/query", headers=headers, timeout=30).json()\n\ndf = spark.createDataFrame(cost_data["properties"]["rows"], schema=["date","service","cost","currency"])\n\n# TRANSFORM\ndf = df.withColumn("cost_usd", col("cost").cast("decimal(10,2)")) \\\n    .withColumn("month", date_trunc("month", "date"))\n\ndf.write.format("delta").mode("append").saveAsTable("catalog.gold.cloud_costs")`,
  },
  {
    id: 49,
    group: 'Advanced/AI ETL',
    title: 'DR ETL',
    flow: 'Extract \u2192 Transform \u2192 Replicate',
    complexity: 5,
    volume: 5,
    sla: 5,
    reliability: 5,
    governance: 5,
    score: 5.0,
    code: `# Disaster Recovery: full ETL replication\ntables = spark.sql("SHOW TABLES IN catalog.gold").collect()\n\nfor row in tables:\n    source = f"catalog.gold.{row.tableName}"\n    dr = f"catalog_dr.gold.{row.tableName}"\n    \n    # Deep clone preserves history + metadata\n    spark.sql(f"CREATE OR REPLACE TABLE {dr} DEEP CLONE {source}")\n    \n    # Verify\n    src_count = spark.table(source).count()\n    dr_count = spark.table(dr).count()\n    assert src_count == dr_count, f"DR mismatch for {source}"\n    print(f"Replicated {source} → {dr} ({src_count} rows)")`,
  },
  {
    id: 50,
    group: 'Advanced/AI ETL',
    title: 'Regulatory ETL',
    flow: 'Multi-Source \u2192 Transform \u2192 Compliance Load',
    complexity: 5,
    volume: 4,
    sla: 5,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `# Regulatory reporting: SOX / GDPR / HIPAA\nfinance = spark.table("catalog.silver.gl_entries")\ncustomers = spark.table("catalog.silver.customers")\naudit = spark.table("catalog.audit.access_logs")\n\n# TRANSFORM: build compliance report\nreport = spark.sql("""\nSELECT 'SOX' AS regulation, COUNT(*) AS controls_checked,\n    SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) AS passed,\n    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS failed\nFROM catalog.audit.compliance_checks\nWHERE check_date >= date_trunc('quarter', current_date())\nGROUP BY regulation\n""")\n\nreport.write.format("delta").mode("overwrite").saveAsTable("catalog.regulatory.compliance_report")`,
  },
];

const groups = [...new Set(etlScenarios.map((s) => s.group))];

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

function ETLScenarios() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('id');

  const filtered = etlScenarios
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
      etlScenarios.map((s) => ({
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
      'etl-scenarios-scorecard.csv'
    );
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>ETL Pipeline Scenarios</h1>
          <p>50 classic Extract \u2192 Transform \u2192 Load pipelines with KPI scoring</p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">{'\u2699\ufe0f'}</div>
          <div className="stat-info">
            <h4>50</h4>
            <p>ETL Pipelines</p>
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
            <h4>{(etlScenarios.reduce((s, x) => s + x.score, 0) / 50).toFixed(1)}</h4>
            <p>Avg Score</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">{'\ud83c\udfc6'}</div>
          <div className="stat-info">
            <h4>{etlScenarios.filter((s) => s.score >= 4.0).length}</h4>
            <p>Advanced (4.0+)</p>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search ETL scenarios..."
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
                {g} ({etlScenarios.filter((s) => s.group === g).length})
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
                    ETL KPI Scoring
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
                  <ScenarioCard scenario={s} />
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default ETLScenarios;
