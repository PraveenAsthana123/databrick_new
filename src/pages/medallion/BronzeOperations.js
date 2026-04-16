import React, { useState } from 'react';
import ScenarioCard from '../../components/common/ScenarioCard';
import { exportToCSV } from '../../utils/fileExport';

const bronzeOperations = [
  // ─── 1–10: Core Ingestion ───
  {
    id: 1,
    group: 'Core Ingestion',
    title: 'File Ingestion',
    desc: 'Reads raw files (CSV, JSON, Parquet) from a landing zone into a Bronze Delta table.',
    code: `from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("bronze_file_ingest").getOrCreate()
df = spark.read.option("header", True).option("inferSchema", True).csv("abfss://landing@storage.dfs.core.windows.net/orders/")
df = df.withColumn("_ingest_ts", current_timestamp()).withColumn("_source_file", input_file_name())
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")`,
  },
  {
    id: 2,
    group: 'Core Ingestion',
    title: 'Streaming Ingestion',
    desc: 'Continuously reads from a streaming source (Event Hub / Kafka) into Bronze Delta using Structured Streaming.',
    code: `df_stream = spark.readStream.format("eventhubs").options(**ehConf).load()
from pyspark.sql.functions import current_timestamp
df_stream = df_stream.withColumn("_ingest_ts", current_timestamp())
query = (df_stream.writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/checkpoints/bronze/events")
  .start("abfss://bronze@storage.dfs.core.windows.net/events/"))
query.awaitTermination()`,
  },
  {
    id: 3,
    group: 'Core Ingestion',
    title: 'API Ingestion',
    desc: 'Fetches data from a REST API endpoint, converts the JSON payload to a DataFrame, and writes to Bronze.',
    code: `import requests, json
from pyspark.sql.types import StringType
response = requests.get("https://api.example.com/v1/orders", headers={"Authorization": "Bearer TOKEN"}, timeout=30)
data = response.json()["data"]
df = spark.createDataFrame(data)
df = df.withColumn("_ingest_ts", current_timestamp()).withColumn("_source", lit("api.example.com"))
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/api_orders/")`,
  },
  {
    id: 4,
    group: 'Core Ingestion',
    title: 'JDBC Ingestion',
    desc: 'Pulls data from a relational database (SQL Server, Postgres) via JDBC and writes raw rows to Bronze.',
    code: `jdbc_url = "jdbc:sqlserver://server.database.windows.net:1433;database=SourceDB"
df = (spark.read.format("jdbc")
  .option("url", jdbc_url)
  .option("dbtable", "dbo.orders")
  .option("user", dbutils.secrets.get("kv", "jdbc-user"))
  .option("password", dbutils.secrets.get("kv", "jdbc-password"))
  .option("numPartitions", 8).option("partitionColumn", "order_id")
  .option("lowerBound", 1).option("upperBound", 10000000).load())
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/jdbc_orders/")`,
  },
  {
    id: 5,
    group: 'Core Ingestion',
    title: 'Auto Loader',
    desc: 'Uses Databricks Auto Loader to incrementally and efficiently ingest new files as they arrive in cloud storage.',
    code: `df = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/schema/bronze/events")
  .option("cloudFiles.inferColumnTypes", True)
  .load("abfss://landing@storage.dfs.core.windows.net/events/"))
df = df.withColumn("_ingest_ts", current_timestamp())
(df.writeStream.format("delta")
  .option("checkpointLocation", "/checkpoints/bronze/autoloader_events")
  .option("mergeSchema", "true")
  .start("abfss://bronze@storage.dfs.core.windows.net/events/"))`,
  },
  {
    id: 6,
    group: 'Core Ingestion',
    title: 'Batch Ingestion',
    desc: 'Scheduled batch job that reads all files in a landing folder and appends them to a Bronze Delta table.',
    code: `from pyspark.sql.functions import current_timestamp, lit, input_file_name
batch_date = dbutils.widgets.get("batch_date")
df = spark.read.format("parquet").load(f"abfss://landing@storage.dfs.core.windows.net/sales/{batch_date}/")
df = (df.withColumn("_ingest_ts", current_timestamp())
        .withColumn("_batch_date", lit(batch_date))
        .withColumn("_source_file", input_file_name()))
df.write.format("delta").mode("append").partitionBy("_batch_date").save("abfss://bronze@storage.dfs.core.windows.net/sales/")
print(f"Ingested {df.count()} records for batch {batch_date}")`,
  },
  {
    id: 7,
    group: 'Core Ingestion',
    title: 'CDC Ingestion',
    desc: 'Captures Change Data Capture (CDC) events from a source and writes insert/update/delete records to Bronze.',
    code: `df_cdc = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .load("abfss://landing@storage.dfs.core.windows.net/cdc/customers/"))
from pyspark.sql.functions import current_timestamp
df_cdc = df_cdc.withColumn("_ingest_ts", current_timestamp())
# Preserve op type: I=Insert, U=Update, D=Delete
(df_cdc.writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/checkpoints/bronze/cdc_customers")
  .start("abfss://bronze@storage.dfs.core.windows.net/cdc_customers/"))`,
  },
  {
    id: 8,
    group: 'Core Ingestion',
    title: 'Multi-Source Ingestion',
    desc: 'Merges data from multiple heterogeneous sources (files, JDBC, API) into a unified Bronze table.',
    code: `from pyspark.sql.functions import current_timestamp, lit
df_file = spark.read.json("abfss://landing@storage.dfs.core.windows.net/src1/")
df_jdbc = spark.read.format("jdbc").option("dbtable", "orders").load()
df_api  = spark.createDataFrame(api_data)
df_file = df_file.withColumn("_source", lit("file")).withColumn("_ingest_ts", current_timestamp())
df_jdbc = df_jdbc.withColumn("_source", lit("jdbc")).withColumn("_ingest_ts", current_timestamp())
df_api  = df_api.withColumn("_source", lit("api")).withColumn("_ingest_ts", current_timestamp())
df_all  = df_file.unionByName(df_jdbc, allowMissingColumns=True).unionByName(df_api, allowMissingColumns=True)
df_all.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/unified_orders/")`,
  },
  {
    id: 9,
    group: 'Core Ingestion',
    title: 'Incremental Ingestion',
    desc: 'Reads only new records since the last successful run using a high-watermark timestamp or sequence number.',
    code: `last_watermark = spark.sql("SELECT MAX(_ingest_ts) FROM bronze.orders").collect()[0][0]
df_new = (spark.read.format("jdbc")
  .option("dbtable", f"(SELECT * FROM orders WHERE updated_at > '{last_watermark}') t")
  .option("url", jdbc_url).load())
from pyspark.sql.functions import current_timestamp
df_new = df_new.withColumn("_ingest_ts", current_timestamp())
df_new.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
print(f"Incremental load: {df_new.count()} new records")`,
  },
  {
    id: 10,
    group: 'Core Ingestion',
    title: 'Full Load Ingestion',
    desc: 'Replaces the entire Bronze table with a full extract from the source system on a scheduled basis.',
    code: `from pyspark.sql.functions import current_timestamp, lit
df_full = spark.read.format("jdbc").option("dbtable", "dbo.reference_data").option("url", jdbc_url).load()
df_full = df_full.withColumn("_ingest_ts", current_timestamp()).withColumn("_load_type", lit("FULL"))
# Overwrite mode replaces all data
df_full.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
  "abfss://bronze@storage.dfs.core.windows.net/reference_data/")
print(f"Full load complete: {df_full.count()} rows")`,
  },

  // ─── 11–20: Raw Data Handling ───
  {
    id: 11,
    group: 'Raw Data Handling',
    title: 'Raw Storage Immutable',
    desc: 'Marks raw Bronze tables as append-only and enables Delta table properties to prevent updates/deletes.',
    code: `-- Make Bronze table effectively immutable (append-only policy)
ALTER TABLE bronze.raw_events SET TBLPROPERTIES (
  'delta.appendOnly' = 'true',
  'comment' = 'Immutable raw layer — no updates or deletes allowed'
);
-- Verify
SHOW TBLPROPERTIES bronze.raw_events;
-- Attempt UPDATE will raise error: DeltaAnalysisException`,
  },
  {
    id: 12,
    group: 'Raw Data Handling',
    title: 'Append-Only Writes',
    desc: 'Enforces append-only write mode on all Bronze ingestion jobs to preserve raw data history.',
    code: `df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/events/")
# Confirm append-only via table property
spark.sql("""
  ALTER TABLE bronze.events
  SET TBLPROPERTIES ('delta.appendOnly' = 'true')
""")
# Any overwrite attempt now raises:
# DeltaAnalysisException: This table is configured to only allow appends`,
  },
  {
    id: 13,
    group: 'Raw Data Handling',
    title: 'Partitioning Strategy',
    desc: 'Partitions Bronze tables by ingestion date to enable efficient partition pruning and data management.',
    code: `from pyspark.sql.functions import current_timestamp, to_date
df = df.withColumn("_ingest_date", to_date(current_timestamp()))
df.write.format("delta").mode("append").partitionBy("_ingest_date").save(
  "abfss://bronze@storage.dfs.core.windows.net/orders/")
# Query with partition pruning
spark.sql("SELECT * FROM bronze.orders WHERE _ingest_date = '2024-01-15'")`,
  },
  {
    id: 14,
    group: 'Raw Data Handling',
    title: 'File Compaction',
    desc: 'Compacts many small files in Bronze Delta tables into fewer larger files to improve read performance.',
    code: `-- Compact small files in Bronze table (OPTIMIZE)
OPTIMIZE bronze.raw_events;
-- With Z-ordering on high-cardinality column for better skipping
OPTIMIZE bronze.raw_events ZORDER BY (event_type, source_system);
-- Schedule vacuum to remove old file versions (7-day default)
VACUUM bronze.raw_events RETAIN 168 HOURS;
SELECT * FROM (DESCRIBE DETAIL bronze.raw_events) LIMIT 1;`,
  },
  {
    id: 15,
    group: 'Raw Data Handling',
    title: 'Schema Inference',
    desc: 'Automatically infers schema from raw files during initial ingestion using Auto Loader schema inference.',
    code: `df = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/schema/bronze/orders")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
  .load("abfss://landing@storage.dfs.core.windows.net/orders/"))
# Inferred schema is persisted at schemaLocation
df.printSchema()`,
  },
  {
    id: 16,
    group: 'Raw Data Handling',
    title: 'Schema Evolution',
    desc: 'Handles source schema changes by automatically merging new columns into the existing Bronze Delta schema.',
    code: `df_new = spark.read.json("abfss://landing@storage.dfs.core.windows.net/orders_v2/")
# mergeSchema allows adding new columns without breaking existing data
df_new.write.format("delta").mode("append").option("mergeSchema", "true").save(
  "abfss://bronze@storage.dfs.core.windows.net/orders/")
-- SQL equivalent
ALTER TABLE bronze.orders SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');`,
  },
  {
    id: 17,
    group: 'Raw Data Handling',
    title: 'Metadata Capture',
    desc: 'Captures file-level metadata (file path, size, modification time) alongside raw records for traceability.',
    code: `from pyspark.sql.functions import input_file_name, current_timestamp, lit
df = spark.read.format("json").load("abfss://landing@storage.dfs.core.windows.net/events/")
df = (df
  .withColumn("_source_file", input_file_name())
  .withColumn("_ingest_ts", current_timestamp())
  .withColumn("_pipeline_version", lit("v2.1.0"))
  .withColumn("_env", lit("prod")))
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/events/")`,
  },
  {
    id: 18,
    group: 'Raw Data Handling',
    title: 'Source System Tagging',
    desc: 'Tags each Bronze record with the originating source system name for multi-source lineage tracking.',
    code: `from pyspark.sql.functions import lit, current_timestamp
SOURCE_SYSTEM = "SAP_ECC"
df = df.withColumn("_source_system", lit(SOURCE_SYSTEM))
df = df.withColumn("_source_env", lit("PROD"))
df = df.withColumn("_ingest_ts", current_timestamp())
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/sap_orders/")
print(f"Source tagged: {SOURCE_SYSTEM} | Records: {df.count()}")`,
  },
  {
    id: 19,
    group: 'Raw Data Handling',
    title: 'Load Timestamp Capture',
    desc: 'Adds a precise UTC load timestamp column to every Bronze record for freshness tracking and auditing.',
    code: `from pyspark.sql.functions import current_timestamp
from datetime import datetime, timezone
load_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
df = df.withColumn("_load_ts", current_timestamp())
df = df.withColumn("_load_date", to_date(current_timestamp()))
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
spark.sql(f"SELECT MIN(_load_ts), MAX(_load_ts) FROM bronze.orders WHERE _load_date = current_date()")`,
  },
  {
    id: 20,
    group: 'Raw Data Handling',
    title: 'Batch ID Tagging',
    desc: 'Assigns a unique batch/run ID to each ingestion run for traceability, reprocessing, and audit.',
    code: `import uuid
from pyspark.sql.functions import lit, current_timestamp
BATCH_ID = str(uuid.uuid4())
RUN_DATE = dbutils.widgets.get("run_date")
df = (df
  .withColumn("_batch_id", lit(BATCH_ID))
  .withColumn("_run_date", lit(RUN_DATE))
  .withColumn("_ingest_ts", current_timestamp()))
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
print(f"Batch ID: {BATCH_ID} | Run Date: {RUN_DATE} | Records: {df.count()}")`,
  },

  // ─── 21–30: Data Quality (Light) ───
  {
    id: 21,
    group: 'Data Quality (Light)',
    title: 'Schema Validation',
    desc: 'Validates incoming raw data against an expected schema and rejects records with unexpected structure.',
    code: `from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
expected_schema = StructType([
  StructField("order_id", LongType(), False),
  StructField("customer_id", LongType(), True),
  StructField("order_date", TimestampType(), True),
  StructField("amount", StringType(), True),
])
df = spark.read.schema(expected_schema).json("abfss://landing@storage.dfs.core.windows.net/orders/")
# Records not matching schema are null-filled; log any schema mismatch
bad = df.filter(df.order_id.isNull()); print(f"Schema invalid rows: {bad.count()}")`,
  },
  {
    id: 22,
    group: 'Data Quality (Light)',
    title: 'Null Check Basic',
    desc: 'Flags records with null values in mandatory key columns during Bronze ingestion.',
    code: `from pyspark.sql.functions import col, when, lit
mandatory_cols = ["order_id", "customer_id", "order_date"]
df = df.withColumn("_has_nulls", lit(False))
for c in mandatory_cols:
    df = df.withColumn("_has_nulls", when(col(c).isNull(), lit(True)).otherwise(col("_has_nulls")))
null_count = df.filter(col("_has_nulls") == True).count()
print(f"Records with null mandatory fields: {null_count}")
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")`,
  },
  {
    id: 23,
    group: 'Data Quality (Light)',
    title: 'File Completeness Check',
    desc: 'Compares expected file count and record count against actual delivered files before ingestion proceeds.',
    code: `import os
EXPECTED_FILES = int(dbutils.widgets.get("expected_files"))
files = dbutils.fs.ls("abfss://landing@storage.dfs.core.windows.net/orders/2024-01-15/")
actual_files = len([f for f in files if f.name.endswith(".parquet")])
if actual_files < EXPECTED_FILES:
    raise ValueError(f"Incomplete delivery: expected {EXPECTED_FILES} files, got {actual_files}")
df = spark.read.parquet(f"abfss://landing@storage.dfs.core.windows.net/orders/2024-01-15/")
print(f"File check passed: {actual_files}/{EXPECTED_FILES} files | {df.count()} records")`,
  },
  {
    id: 24,
    group: 'Data Quality (Light)',
    title: 'Record Count Logging',
    desc: 'Logs the input and output record count for each Bronze ingestion run to a monitoring table.',
    code: `from pyspark.sql.functions import lit, current_timestamp
input_count = df.count()
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
output_count = spark.sql("SELECT COUNT(*) FROM bronze.orders WHERE _batch_id = '{BATCH_ID}'").collect()[0][0]
log_df = spark.createDataFrame([{
  "pipeline": "bronze_orders", "input_count": input_count,
  "output_count": output_count, "logged_at": str(current_timestamp())}])
log_df.write.format("delta").mode("append").save("abfss://audit@storage.dfs.core.windows.net/ingestion_log/")`,
  },
  {
    id: 25,
    group: 'Data Quality (Light)',
    title: 'Duplicate Detection Flag',
    desc: 'Flags potential duplicate records in Bronze using a composite key, without removing them (raw layer preserves all).',
    code: `from pyspark.sql.functions import count, col, lit
from pyspark.sql import Window
import pyspark.sql.functions as F
key_cols = ["order_id", "order_date", "source_system"]
w = Window.partitionBy(key_cols)
df = df.withColumn("_dup_count", count("*").over(w))
df = df.withColumn("_is_duplicate", (col("_dup_count") > 1))
dup_count = df.filter(col("_is_duplicate")).count()
print(f"Potential duplicates flagged (not removed): {dup_count}")
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")`,
  },
  {
    id: 26,
    group: 'Data Quality (Light)',
    title: 'Format Validation',
    desc: 'Validates that key fields conform to expected formats (dates, emails, phone numbers) and flags violations.',
    code: `from pyspark.sql.functions import col, regexp_extract, when, lit
# Validate date format YYYY-MM-DD
df = df.withColumn("_date_valid", regexp_extract(col("order_date").cast("string"), r"^\d{4}-\d{2}-\d{2}", 0) != "")
# Validate email format
df = df.withColumn("_email_valid", col("email").rlike(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"))
invalid = df.filter(~col("_date_valid") | ~col("_email_valid")).count()
print(f"Format validation failures: {invalid}")`,
  },
  {
    id: 27,
    group: 'Data Quality (Light)',
    title: 'Corrupt Record Capture',
    desc: 'Captures malformed or unparseable records in a separate corrupt-record column instead of dropping them.',
    code: `from pyspark.sql.functions import col
# PERMISSIVE mode keeps bad records in _corrupt_record column
df = (spark.read.format("json")
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .load("abfss://landing@storage.dfs.core.windows.net/events/"))
good = df.filter(col("_corrupt_record").isNull())
bad  = df.filter(col("_corrupt_record").isNotNull())
print(f"Good: {good.count()} | Corrupt: {bad.count()}")
bad.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/corrupt_records/")`,
  },
  {
    id: 28,
    group: 'Data Quality (Light)',
    title: 'Quarantine Raw',
    desc: 'Routes records failing basic quality checks to a quarantine Bronze table for investigation without blocking the pipeline.',
    code: `from pyspark.sql.functions import col, lit
QUARANTINE_PATH = "abfss://bronze@storage.dfs.core.windows.net/quarantine/orders/"
GOOD_PATH       = "abfss://bronze@storage.dfs.core.windows.net/orders/"
df_good = df.filter(col("order_id").isNotNull() & col("_corrupt_record").isNull())
df_bad  = df.filter(col("order_id").isNull()  | col("_corrupt_record").isNotNull())
df_bad  = df_bad.withColumn("_quarantine_reason", lit("null_key_or_corrupt"))
df_good.write.format("delta").mode("append").save(GOOD_PATH)
df_bad.write.format("delta").mode("append").save(QUARANTINE_PATH)`,
  },
  {
    id: 29,
    group: 'Data Quality (Light)',
    title: 'Data Freshness Check',
    desc: 'Checks that the latest ingested record timestamp meets the SLA freshness window before downstream processing.',
    code: `from datetime import datetime, timezone, timedelta
SLA_HOURS = 6
last_ingest = spark.sql("SELECT MAX(_ingest_ts) AS latest FROM bronze.orders").collect()[0]["latest"]
now = datetime.now(timezone.utc)
age_hours = (now - last_ingest.replace(tzinfo=timezone.utc)).total_seconds() / 3600
if age_hours > SLA_HOURS:
    raise RuntimeError(f"FRESHNESS SLA BREACH: Bronze data is {age_hours:.1f}h old (SLA={SLA_HOURS}h)")
print(f"Freshness OK: {age_hours:.1f}h since last ingest")`,
  },
  {
    id: 30,
    group: 'Data Quality (Light)',
    title: 'Source vs Load Count',
    desc: 'Compares the record count reported by the source system against the count actually loaded into Bronze.',
    code: `SOURCE_COUNT = int(dbutils.widgets.get("source_record_count"))
bronze_count = spark.sql(f"SELECT COUNT(*) FROM bronze.orders WHERE _batch_id = '{BATCH_ID}'").collect()[0][0]
variance_pct = abs(SOURCE_COUNT - bronze_count) / max(SOURCE_COUNT, 1) * 100
print(f"Source: {SOURCE_COUNT} | Bronze: {bronze_count} | Variance: {variance_pct:.2f}%")
if variance_pct > 1.0:
    raise ValueError(f"Record count mismatch exceeds 1% threshold: {variance_pct:.2f}%")
print("Count reconciliation PASSED")`,
  },

  // ─── 31–40: Metadata & Lineage ───
  {
    id: 31,
    group: 'Metadata & Lineage',
    title: 'File Metadata Tracking',
    desc: 'Records file-level metadata (name, size, modified time) in a Bronze metadata table for every ingested file.',
    code: `from pyspark.sql.functions import lit, current_timestamp
files = dbutils.fs.ls("abfss://landing@storage.dfs.core.windows.net/orders/2024-01-15/")
meta_rows = [{"file_name": f.name, "file_size": f.size, "modification_time": f.modificationTime,
              "table": "bronze.orders", "ingested_at": str(current_timestamp())} for f in files]
meta_df = spark.createDataFrame(meta_rows)
meta_df.write.format("delta").mode("append").save("abfss://audit@storage.dfs.core.windows.net/file_metadata/")`,
  },
  {
    id: 32,
    group: 'Metadata & Lineage',
    title: 'Ingestion Timestamp',
    desc: 'Standardizes the ingestion timestamp column (_ingest_ts) across all Bronze tables for uniform lineage tracking.',
    code: `from pyspark.sql.functions import current_timestamp
# All Bronze ingestion pipelines MUST add _ingest_ts
df = df.withColumn("_ingest_ts", current_timestamp())
# Verify the column exists before write
assert "_ingest_ts" in df.columns, "Missing _ingest_ts — lineage column required"
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
spark.sql("SELECT _ingest_ts, COUNT(*) FROM bronze.orders GROUP BY 1 ORDER BY 1 DESC LIMIT 5")`,
  },
  {
    id: 33,
    group: 'Metadata & Lineage',
    title: 'Source System Tagging',
    desc: 'Tags each Bronze record with a standardized source system identifier for cross-system lineage queries.',
    code: `from pyspark.sql.functions import lit
# Standardized source system codes
SOURCE_MAP = {"sap": "SAP_ECC", "salesforce": "SFDC", "postgres": "PG_PROD"}
source_key = dbutils.widgets.get("source_key")
df = df.withColumn("_source_system", lit(SOURCE_MAP[source_key]))
df = df.withColumn("_source_schema", lit("dbo.orders"))
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
print(f"Source tagged: {SOURCE_MAP[source_key]}")`,
  },
  {
    id: 34,
    group: 'Metadata & Lineage',
    title: 'Lineage Registration',
    desc: 'Registers Bronze table lineage in Unity Catalog so upstream source and downstream Silver tables are tracked.',
    code: `-- Unity Catalog automatically captures lineage when using SQL/DataFrame APIs
-- Verify lineage is tracked:
SELECT * FROM system.access.audit
WHERE action_name IN ('createTable', 'commandSubmit')
  AND request_params.full_name_arg LIKE '%bronze%'
ORDER BY event_time DESC LIMIT 20;
-- View table lineage in Catalog Explorer UI or:
-- DESCRIBE EXTENDED bronze.orders;`,
  },
  {
    id: 35,
    group: 'Metadata & Lineage',
    title: 'Job Run Logging',
    desc: 'Logs pipeline job run details (job ID, start time, end time, status) to a Bronze audit log table.',
    code: `import time
from pyspark.sql.functions import lit
JOB_ID = dbutils.widgets.get("job_id")
RUN_ID = dbutils.widgets.get("run_id")
start_ts = time.time()
# ... ingestion logic ...
end_ts = time.time()
log = spark.createDataFrame([{"job_id": JOB_ID, "run_id": RUN_ID,
  "pipeline": "bronze_orders", "status": "SUCCESS",
  "start_ts": start_ts, "end_ts": end_ts, "duration_s": end_ts - start_ts}])
log.write.format("delta").mode("append").save("abfss://audit@storage.dfs.core.windows.net/job_runs/")`,
  },
  {
    id: 36,
    group: 'Metadata & Lineage',
    title: 'Batch/Run ID Tracking',
    desc: 'Tracks each ingestion run with a unique batch ID so any batch can be identified, audited, or replayed.',
    code: `import uuid
BATCH_ID = str(uuid.uuid4())
print(f"Starting batch: {BATCH_ID}")
df = df.withColumn("_batch_id", lit(BATCH_ID))
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
# Query specific batch
spark.sql(f"SELECT COUNT(*), MIN(_ingest_ts), MAX(_ingest_ts) FROM bronze.orders WHERE _batch_id = '{BATCH_ID}'")`,
  },
  {
    id: 37,
    group: 'Metadata & Lineage',
    title: 'Data Versioning',
    desc: 'Uses Delta Lake time travel to maintain historical versions of Bronze data for audit and reprocessing.',
    code: `-- View Delta version history
DESCRIBE HISTORY bronze.orders;
-- Read specific version (time travel)
SELECT * FROM bronze.orders VERSION AS OF 5 LIMIT 100;
-- Read as of timestamp
SELECT * FROM bronze.orders TIMESTAMP AS OF '2024-01-15 08:00:00' LIMIT 100;
-- Restore to previous version if needed
RESTORE TABLE bronze.orders TO VERSION AS OF 3;`,
  },
  {
    id: 38,
    group: 'Metadata & Lineage',
    title: 'Schema Version Tracking',
    desc: 'Tracks schema versions for Bronze tables so changes in source schema are logged and auditable.',
    code: `import hashlib, json
schema_json = df.schema.json()
schema_hash = hashlib.md5(schema_json.encode()).hexdigest()
schema_log = spark.createDataFrame([{
  "table": "bronze.orders",
  "schema_hash": schema_hash,
  "schema_json": schema_json,
  "recorded_at": str(current_timestamp()),
  "batch_id": BATCH_ID
}])
schema_log.write.format("delta").mode("append").save("abfss://audit@storage.dfs.core.windows.net/schema_versions/")`,
  },
  {
    id: 39,
    group: 'Metadata & Lineage',
    title: 'Ownership Tagging',
    desc: 'Tags Bronze tables with team ownership and steward information using Unity Catalog table tags.',
    code: `-- Tag Bronze tables with ownership metadata
ALTER TABLE bronze.orders SET TAGS (
  'owner_team'  = 'data-engineering',
  'data_steward'= 'alice@company.com',
  'domain'      = 'commerce',
  'tier'        = 'bronze',
  'criticality' = 'high'
);
-- View tags
SELECT * FROM system.information_schema.table_tags WHERE table_name = 'orders';`,
  },
  {
    id: 40,
    group: 'Metadata & Lineage',
    title: 'Data Classification Tagging',
    desc: 'Applies sensitivity classification tags (PII, confidential, public) to Bronze columns via Unity Catalog.',
    code: `-- Classify columns in Bronze table
ALTER TABLE bronze.customers ALTER COLUMN email       SET TAGS ('pii' = 'true', 'classification' = 'confidential');
ALTER TABLE bronze.customers ALTER COLUMN phone       SET TAGS ('pii' = 'true', 'classification' = 'confidential');
ALTER TABLE bronze.customers ALTER COLUMN customer_id SET TAGS ('pii' = 'false', 'classification' = 'internal');
ALTER TABLE bronze.customers ALTER COLUMN country     SET TAGS ('pii' = 'false', 'classification' = 'public');
-- Audit: list all PII columns in bronze
SELECT table_name, column_name, tag_value FROM system.information_schema.column_tags
WHERE tag_name = 'pii' AND tag_value = 'true' AND table_schema = 'bronze';`,
  },

  // ─── 41–50: Security & Governance ───
  {
    id: 41,
    group: 'Security & Governance',
    title: 'Encryption at Rest',
    desc: 'Ensures Bronze data is encrypted at rest using cloud-managed or customer-managed keys (Azure CMK / AWS KMS).',
    code: `-- Verify Delta table encryption at rest (Azure: ADLS Gen2 with CMK)
-- Storage Account > Security: Encryption with Customer-Managed Key enabled
-- Unity Catalog storage credential uses managed identity
CREATE STORAGE CREDENTIAL bronze_cred
  WITH AZURE_MANAGED_IDENTITY = (CREDENTIAL 'bronze-managed-identity');
-- Verify encryption:
DESCRIBE DETAIL bronze.orders;
-- Confirm: encryption_key = 'CustomerManagedKey'`,
  },
  {
    id: 42,
    group: 'Security & Governance',
    title: 'Encryption in Transit',
    desc: 'Enforces TLS 1.2+ for all data movement into and out of the Bronze layer.',
    code: `# Enforce TLS via Databricks cluster Spark config
# Set in cluster or job config:
# spark.ssl.enabled true
# spark.ssl.protocol TLSv1.2
# JDBC connections — always use encrypt=true
jdbc_url = (
  "jdbc:sqlserver://server.database.windows.net:1433;"
  "database=SourceDB;encrypt=true;trustServerCertificate=false;"
  "hostNameInCertificate=*.database.windows.net;loginTimeout=30"
)
print("TLS enforced for JDBC ingestion")`,
  },
  {
    id: 43,
    group: 'Security & Governance',
    title: 'Access Control RBAC',
    desc: 'Implements role-based access control on Bronze tables — engineers can write, analysts can only read Silver+.',
    code: `-- Bronze: writable only by data engineers / service principals
GRANT USE SCHEMA ON SCHEMA bronze TO \`data_engineers\`;
GRANT SELECT, MODIFY ON SCHEMA bronze TO \`data_engineers\`;
GRANT USE SCHEMA ON SCHEMA bronze TO \`bronze_svc_principal\`;
GRANT SELECT, MODIFY ON SCHEMA bronze TO \`bronze_svc_principal\`;
-- Analysts: no access to bronze
REVOKE ALL PRIVILEGES ON SCHEMA bronze FROM \`analysts\`;
-- Verify
SHOW GRANTS ON SCHEMA bronze;`,
  },
  {
    id: 44,
    group: 'Security & Governance',
    title: 'Column Tagging PII',
    desc: 'Tags all PII columns in Bronze tables with Unity Catalog column-level tags before downstream use.',
    code: `-- Apply PII tags to Bronze customer table columns
ALTER TABLE bronze.customers ALTER COLUMN email       SET TAGS ('pii' = 'true');
ALTER TABLE bronze.customers ALTER COLUMN phone       SET TAGS ('pii' = 'true');
ALTER TABLE bronze.customers ALTER COLUMN ssn         SET TAGS ('pii' = 'true', 'sensitivity' = 'high');
ALTER TABLE bronze.customers ALTER COLUMN first_name  SET TAGS ('pii' = 'true');
ALTER TABLE bronze.customers ALTER COLUMN ip_address  SET TAGS ('pii' = 'true', 'gdpr' = 'true');
SELECT * FROM system.information_schema.column_tags WHERE table_name = 'customers' AND tag_name = 'pii';`,
  },
  {
    id: 45,
    group: 'Security & Governance',
    title: 'Masking Optional Minimal',
    desc: 'Applies minimal, optional column masking at Bronze only where legal mandates require it (e.g., SSN partial mask).',
    code: `-- Create mask function for SSN (Bronze: partial mask for compliance)
CREATE OR REPLACE FUNCTION bronze.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE WHEN is_member('data_engineers') THEN ssn
            ELSE CONCAT('***-**-', RIGHT(ssn, 4)) END;
-- Apply to Bronze table (only where legally required)
ALTER TABLE bronze.customers ALTER COLUMN ssn
  SET MASK bronze.mask_ssn;
SELECT customer_id, ssn FROM bronze.customers LIMIT 5;`,
  },
  {
    id: 46,
    group: 'Security & Governance',
    title: 'Audit Logging',
    desc: 'Enables and queries Unity Catalog audit logs to track all access to Bronze tables.',
    code: `-- Query audit logs for Bronze table access
SELECT event_time, user_identity.email AS user,
       action_name, request_params.full_name_arg AS table_accessed
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '%bronze%'
  AND event_date >= current_date() - 7
ORDER BY event_time DESC
LIMIT 100;`,
  },
  {
    id: 47,
    group: 'Security & Governance',
    title: 'Service Account Control',
    desc: 'Restricts Bronze write access to dedicated service principals only — no human users can write to Bronze directly.',
    code: `-- Only service principals can write to Bronze
GRANT USE CATALOG ON CATALOG main TO \`bronze-ingest-sp\`;
GRANT USE SCHEMA ON SCHEMA main.bronze TO \`bronze-ingest-sp\`;
GRANT SELECT, MODIFY ON SCHEMA main.bronze TO \`bronze-ingest-sp\`;
-- Deny write to all human users
REVOKE MODIFY ON SCHEMA main.bronze FROM \`data_engineers\`;
-- Engineers can read Bronze but not write
GRANT SELECT ON SCHEMA main.bronze TO \`data_engineers\`;
SHOW GRANTS ON SCHEMA main.bronze;`,
  },
  {
    id: 48,
    group: 'Security & Governance',
    title: 'Secret Management',
    desc: 'Stores all credentials (JDBC passwords, API keys, storage keys) in Azure Key Vault via Databricks secret scope.',
    code: `# Never hard-code credentials — use secret scopes
jdbc_password = dbutils.secrets.get(scope="kv-bronze", key="jdbc-password")
api_key       = dbutils.secrets.get(scope="kv-bronze", key="source-api-key")
storage_key   = dbutils.secrets.get(scope="kv-bronze", key="adls-access-key")
# Confirm scopes exist
scopes = [s.name for s in dbutils.secrets.listScopes()]
print(f"Available secret scopes: {scopes}")
assert "kv-bronze" in scopes, "kv-bronze secret scope not configured"`,
  },
  {
    id: 49,
    group: 'Security & Governance',
    title: 'Network Isolation',
    desc: 'Ensures Bronze storage is accessible only from the Databricks VNet via private endpoints — no public internet access.',
    code: `# Network isolation is enforced at infrastructure level:
# 1. ADLS Gen2 Bronze container: disable public network access
# 2. Private endpoint on ADLS from Databricks VNet
# 3. Databricks workspace: VNet injection (no-public-IP clusters)
# 4. Network Security Group: allow only Databricks subnet CIDR
# Verify connectivity from Databricks:
dbutils.fs.ls("abfss://bronze@storage.dfs.core.windows.net/")
print("Private endpoint connectivity confirmed")`,
  },
  {
    id: 50,
    group: 'Security & Governance',
    title: 'Data Access Segregation',
    desc: 'Separates Bronze storage accounts by data domain so finance, HR, and operations raw data are physically isolated.',
    code: `-- Separate catalogs per domain (each pointing to isolated storage)
CREATE CATALOG IF NOT EXISTS bronze_finance  MANAGED LOCATION 'abfss://finance@bronzestorage.dfs.core.windows.net/';
CREATE CATALOG IF NOT EXISTS bronze_hr       MANAGED LOCATION 'abfss://hr@bronzestorage.dfs.core.windows.net/';
CREATE CATALOG IF NOT EXISTS bronze_ops      MANAGED LOCATION 'abfss://ops@bronzestorage.dfs.core.windows.net/';
-- Grant domain-specific access only
GRANT ALL PRIVILEGES ON CATALOG bronze_finance TO \`finance_data_engineers\`;
GRANT ALL PRIVILEGES ON CATALOG bronze_hr      TO \`hr_data_engineers\`;
SHOW CATALOGS;`,
  },

  // ─── 51–60: Error Handling ───
  {
    id: 51,
    group: 'Error Handling',
    title: 'Retry Mechanism',
    desc: 'Implements exponential backoff retry logic for transient failures during Bronze ingestion from external sources.',
    code: `import time
def ingest_with_retry(ingest_fn, max_retries=3, base_delay=2):
    for attempt in range(max_retries):
        try:
            return ingest_fn()
        except Exception as e:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Ingestion failed after {max_retries} retries") from e
            wait = base_delay * (2 ** attempt)
            print(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
result = ingest_with_retry(lambda: spark.read.json("abfss://landing@storage.dfs.core.windows.net/orders/"))`,
  },
  {
    id: 52,
    group: 'Error Handling',
    title: 'Idempotent Ingestion',
    desc: 'Designs Bronze ingestion to be safely re-runnable — re-running the same batch produces the same result.',
    code: `from delta.tables import DeltaTable
BATCH_ID = dbutils.widgets.get("batch_id")
# Check if batch was already ingested
existing = spark.sql(f"SELECT COUNT(*) FROM bronze.orders WHERE _batch_id = '{BATCH_ID}'").collect()[0][0]
if existing > 0:
    print(f"Batch {BATCH_ID} already ingested ({existing} records). Skipping.")
    dbutils.notebook.exit("ALREADY_INGESTED")
df = df.withColumn("_batch_id", lit(BATCH_ID))
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")`,
  },
  {
    id: 53,
    group: 'Error Handling',
    title: 'Checkpointing',
    desc: 'Uses Structured Streaming checkpoint locations to ensure exactly-once or at-least-once delivery to Bronze.',
    code: `CHECKPOINT = "abfss://checkpoints@storage.dfs.core.windows.net/bronze/orders/"
df_stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(LANDING_PATH)
df_stream = df_stream.withColumn("_ingest_ts", current_timestamp())
query = (df_stream.writeStream
  .format("delta")
  .option("checkpointLocation", CHECKPOINT)
  .outputMode("append")
  .trigger(availableNow=True)
  .start("abfss://bronze@storage.dfs.core.windows.net/orders/"))
query.awaitTermination()`,
  },
  {
    id: 54,
    group: 'Error Handling',
    title: 'Dead Letter Queue',
    desc: 'Routes unprocessable records (parse errors, schema mismatches) to a dead letter Bronze table for investigation.',
    code: `from pyspark.sql.functions import col, lit, current_timestamp
df_all = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json(LANDING_PATH)
df_good = df_all.filter(col("_corrupt_record").isNull())
df_dlq  = df_all.filter(col("_corrupt_record").isNotNull()).withColumn("_dlq_ts", current_timestamp()).withColumn("_dlq_reason", lit("parse_error"))
df_good.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
df_dlq.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders_dlq/")
print(f"DLQ: {df_dlq.count()} records quarantined")`,
  },
  {
    id: 55,
    group: 'Error Handling',
    title: 'Reprocessing Pipeline',
    desc: 'Re-ingests a specific failed batch from the landing zone into Bronze after fixing the root cause.',
    code: `BATCH_DATE = dbutils.widgets.get("reprocess_date")
BATCH_ID   = dbutils.widgets.get("reprocess_batch_id")
# Delete failed batch records
spark.sql(f"DELETE FROM bronze.orders WHERE _batch_id = '{BATCH_ID}'")
# Re-ingest from landing zone
df = spark.read.parquet(f"abfss://landing@storage.dfs.core.windows.net/orders/{BATCH_DATE}/")
df = df.withColumn("_batch_id", lit(BATCH_ID)).withColumn("_ingest_ts", current_timestamp()).withColumn("_reprocessed", lit(True))
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
print(f"Reprocessed batch {BATCH_ID}: {df.count()} records")`,
  },
  {
    id: 56,
    group: 'Error Handling',
    title: 'Partial Load Handling',
    desc: 'Detects and handles partial file deliveries — waits for complete file sets before committing to Bronze.',
    code: `EXPECTED_COUNT = int(dbutils.widgets.get("expected_record_count"))
df = spark.read.parquet(f"abfss://landing@storage.dfs.core.windows.net/orders/{BATCH_DATE}/")
actual_count = df.count()
tolerance = 0.01  # 1% tolerance
if abs(actual_count - EXPECTED_COUNT) / EXPECTED_COUNT > tolerance:
    raise ValueError(f"Partial load detected: expected {EXPECTED_COUNT}, got {actual_count}")
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
print(f"Partial load check passed: {actual_count} records")`,
  },
  {
    id: 57,
    group: 'Error Handling',
    title: 'Late Arrival Handling',
    desc: 'Handles late-arriving records by writing them to Bronze with original event time, not dropping them.',
    code: `from pyspark.sql.functions import col, current_timestamp, lit, datediff
df = df.withColumn("_ingest_ts", current_timestamp())
df = df.withColumn("_late_arrival", datediff(col("_ingest_ts"), col("event_time")) > 1)
late_count = df.filter(col("_late_arrival")).count()
print(f"Late arrivals detected: {late_count} records (still ingested)")
# Late records are written normally — Silver/Gold handle watermarking
df.write.format("delta").mode("append").partitionBy("event_date").save("abfss://bronze@storage.dfs.core.windows.net/events/")`,
  },
  {
    id: 58,
    group: 'Error Handling',
    title: 'Backfill Processing',
    desc: 'Runs a historical backfill to ingest missed or corrected data for a date range into Bronze.',
    code: `from datetime import datetime, timedelta
START_DATE = dbutils.widgets.get("start_date")
END_DATE   = dbutils.widgets.get("end_date")
start = datetime.strptime(START_DATE, "%Y-%m-%d")
end   = datetime.strptime(END_DATE, "%Y-%m-%d")
current = start
while current <= end:
    date_str = current.strftime("%Y-%m-%d")
    df = spark.read.parquet(f"abfss://landing@storage.dfs.core.windows.net/orders/{date_str}/")
    df = df.withColumn("_batch_id", lit(f"backfill_{date_str}")).withColumn("_ingest_ts", current_timestamp())
    df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
    current += timedelta(days=1)`,
  },
  {
    id: 59,
    group: 'Error Handling',
    title: 'Failure Logging',
    desc: 'Logs all ingestion failures with error details, stack trace, and context to a Bronze error log table.',
    code: `import traceback
from pyspark.sql.functions import lit, current_timestamp
try:
    df = spark.read.json("abfss://landing@storage.dfs.core.windows.net/orders/")
    df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
except Exception as e:
    error_df = spark.createDataFrame([{"pipeline": "bronze_orders", "error_message": str(e),
      "stack_trace": traceback.format_exc(), "batch_id": BATCH_ID, "failed_at": str(current_timestamp())}])
    error_df.write.format("delta").mode("append").save("abfss://audit@storage.dfs.core.windows.net/error_log/")
    raise`,
  },
  {
    id: 60,
    group: 'Error Handling',
    title: 'Alerting',
    desc: 'Sends alerts via email or Teams/Slack when Bronze ingestion fails or SLA thresholds are breached.',
    code: `import requests
def send_alert(message: str, severity: str = "HIGH"):
    webhook_url = dbutils.secrets.get("kv-bronze", "teams-webhook-url")
    payload = {"text": f"[{severity}] Bronze Ingestion Alert\\n{message}"}
    resp = requests.post(webhook_url, json=payload, timeout=10)
    resp.raise_for_status()

try:
    # ... ingestion logic ...
    pass
except Exception as e:
    send_alert(f"Pipeline: bronze_orders\\nError: {str(e)}\\nBatch: {BATCH_ID}", severity="CRITICAL")
    raise`,
  },

  // ─── 61–70: Performance ───
  {
    id: 61,
    group: 'Performance',
    title: 'Partition Pruning',
    desc: 'Designs Bronze partitioning by date so queries and downstream Silver jobs scan only relevant partitions.',
    code: `from pyspark.sql.functions import to_date, current_timestamp
# Partition by ingestion date for efficient downstream access
df = df.withColumn("_ingest_date", to_date(current_timestamp()))
df.write.format("delta").mode("append").partitionBy("_ingest_date").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
-- Silver reads only needed partition
spark.sql("SELECT * FROM bronze.orders WHERE _ingest_date = '2024-01-15'")
-- Check partition stats
spark.sql("DESCRIBE DETAIL bronze.orders")`,
  },
  {
    id: 62,
    group: 'Performance',
    title: 'Small File Handling',
    desc: 'Prevents small file proliferation in Bronze by auto-compacting with Delta OPTIMIZE and configuring target file size.',
    code: `-- Set auto-compaction and optimize write on Bronze tables
ALTER TABLE bronze.orders SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.targetFileSize'             = '134217728'  -- 128 MB target
);
-- Manual OPTIMIZE if needed
OPTIMIZE bronze.orders WHERE _ingest_date >= current_date() - 7;
SELECT COUNT(*) AS file_count FROM (DESCRIBE DETAIL bronze.orders);`,
  },
  {
    id: 63,
    group: 'Performance',
    title: 'Delta Optimization Light',
    desc: 'Runs a lightweight OPTIMIZE on recent Bronze partitions after each daily ingestion to maintain read performance.',
    code: `from datetime import datetime, timedelta
OPTIMIZE_DAYS = 3
for i in range(OPTIMIZE_DAYS):
    date_str = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
    spark.sql(f"OPTIMIZE bronze.orders WHERE _ingest_date = '{date_str}'")
    print(f"Optimized partition: {date_str}")
# Vacuum old versions (keep 7 days for recovery)
spark.sql("VACUUM bronze.orders RETAIN 168 HOURS")`,
  },
  {
    id: 64,
    group: 'Performance',
    title: 'Compression',
    desc: 'Configures Snappy or ZSTD compression on Bronze Delta tables to reduce storage cost and improve read speed.',
    code: `-- Set compression codec on Bronze table
ALTER TABLE bronze.orders SET TBLPROPERTIES (
  'delta.parquet.compression.codec' = 'snappy'
);
-- For new tables: configure in Spark session
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
-- Verify: DESCRIBE DETAIL shows compression`,
  },
  {
    id: 65,
    group: 'Performance',
    title: 'Storage Tiering',
    desc: 'Moves older Bronze partitions to cool/archive storage tiers to reduce cost while keeping data accessible.',
    code: `# Azure Storage lifecycle management policy (set via Azure Portal / ARM):
# Bronze data > 30 days: move to Cool tier
# Bronze data > 90 days: move to Archive tier (or delete if past retention)
# Lifecycle rule JSON:
LIFECYCLE_RULE = {
  "rules": [{"name": "bronze-cool", "type": "Lifecycle",
    "definition": {"filters": {"blobTypes": ["blockBlob"], "prefixMatch": ["bronze/"]},
    "actions": {"baseBlob": {"tierToCool": {"daysAfterModificationGreaterThan": 30},
                             "tierToArchive": {"daysAfterModificationGreaterThan": 90}}}}}]}
print("Lifecycle policy applied via Azure Portal / az CLI")`,
  },
  {
    id: 66,
    group: 'Performance',
    title: 'Parallel Ingestion',
    desc: 'Parallelizes ingestion of multiple source files or partitions across Databricks cluster workers.',
    code: `from concurrent.futures import ThreadPoolExecutor
def ingest_partition(date_str):
    df = spark.read.parquet(f"abfss://landing@storage.dfs.core.windows.net/orders/{date_str}/")
    df = df.withColumn("_batch_date", lit(date_str)).withColumn("_ingest_ts", current_timestamp())
    df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
    return date_str

dates = ["2024-01-13", "2024-01-14", "2024-01-15"]
with ThreadPoolExecutor(max_workers=3) as ex:
    results = list(ex.map(ingest_partition, dates))
print(f"Parallel ingestion complete: {results}")`,
  },
  {
    id: 67,
    group: 'Performance',
    title: 'Cluster Autoscaling',
    desc: 'Configures Databricks cluster autoscaling so Bronze ingestion jobs right-size compute based on data volume.',
    code: `# Cluster config JSON for Bronze ingestion jobs:
CLUSTER_CONFIG = {
  "autoscale": {"min_workers": 2, "max_workers": 20},
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "spark_conf": {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
  },
  "enable_elastic_disk": True
}
print("Autoscaling cluster config applied to Bronze ingestion job")`,
  },
  {
    id: 68,
    group: 'Performance',
    title: 'File Size Standardization',
    desc: 'Configures Delta writer to target a standard file size (128 MB) to avoid both tiny and oversized files.',
    code: `# Set target file size before writing to Bronze
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", "128")  # MB
# For existing table:
spark.sql("""
  ALTER TABLE bronze.orders SET TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
  )
""")
print("Target file size: 128 MB per Parquet file in Bronze")`,
  },
  {
    id: 69,
    group: 'Performance',
    title: 'Incremental Load Strategy',
    desc: 'Implements a high-watermark strategy to load only new/changed records, reducing Bronze ingestion time significantly.',
    code: `-- Store watermark in a control table
CREATE TABLE IF NOT EXISTS bronze.ingestion_control (
  pipeline STRING, last_watermark TIMESTAMP, updated_at TIMESTAMP);
-- Read watermark
last_wm = spark.sql("SELECT last_watermark FROM bronze.ingestion_control WHERE pipeline='orders'").collect()[0][0]
-- Incremental load
df_new = spark.read.format("jdbc").option("dbtable", f"(SELECT * FROM orders WHERE updated_at > '{last_wm}') t").load()
df_new.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/orders/")
-- Update watermark
spark.sql(f"UPDATE bronze.ingestion_control SET last_watermark = current_timestamp() WHERE pipeline = 'orders'")`,
  },
  {
    id: 70,
    group: 'Performance',
    title: 'Data Skipping Delta',
    desc: 'Leverages Delta Lake data skipping via Z-ordering to speed up selective queries on Bronze tables.',
    code: `-- Z-order on columns commonly used in Silver/Gold filters
OPTIMIZE bronze.orders ZORDER BY (order_id, customer_id, order_date);
-- Delta min/max statistics enable data skipping automatically
-- Verify statistics are collected:
SELECT * FROM (DESCRIBE DETAIL bronze.orders) LIMIT 1;
-- Test skipping effectiveness:
EXPLAIN SELECT * FROM bronze.orders WHERE order_id = 12345 AND order_date > '2024-01-01';`,
  },

  // ─── 71–80: Streaming Bronze ───
  {
    id: 71,
    group: 'Streaming Bronze',
    title: 'Kafka Offset Tracking',
    desc: 'Reads from Kafka with explicit offset management so Bronze streaming is resumable and auditable.',
    code: `df_kafka = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", dbutils.secrets.get("kv-bronze", "kafka-brokers"))
  .option("subscribe", "orders-raw")
  .option("startingOffsets", "latest")
  .option("kafka.security.protocol", "SASL_SSL")
  .load())
from pyspark.sql.functions import col, current_timestamp
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) AS raw_json", "topic", "partition", "offset", "timestamp AS event_ts")
df_parsed = df_parsed.withColumn("_ingest_ts", current_timestamp())
df_parsed.writeStream.format("delta").option("checkpointLocation", "/checkpoints/bronze/kafka_orders").start("abfss://bronze@storage.dfs.core.windows.net/kafka_orders/")`,
  },
  {
    id: 72,
    group: 'Streaming Bronze',
    title: 'Checkpoint Location',
    desc: 'Configures persistent checkpoint locations for all Bronze streaming jobs to guarantee resume after failure.',
    code: `CHECKPOINT_BASE = "abfss://checkpoints@storage.dfs.core.windows.net/bronze/"
PIPELINE_NAME   = "events_stream"
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}{PIPELINE_NAME}/"
df_stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(LANDING_PATH)
(df_stream.writeStream
  .format("delta")
  .option("checkpointLocation", CHECKPOINT_PATH)
  .option("mergeSchema", "true")
  .outputMode("append")
  .start("abfss://bronze@storage.dfs.core.windows.net/events/"))
print(f"Checkpoint: {CHECKPOINT_PATH}")`,
  },
  {
    id: 73,
    group: 'Streaming Bronze',
    title: 'Watermarking Minimal',
    desc: 'Applies a minimal watermark at Bronze to handle late-arriving events in the stream without dropping data.',
    code: `from pyspark.sql.functions import col, current_timestamp
df_stream = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", KAFKA_BROKERS).option("subscribe", "events-raw").load())
df_parsed = df_stream.selectExpr("CAST(value AS STRING) AS raw", "timestamp AS event_ts")
# Minimal 1-hour watermark — Bronze keeps late data, Silver enforces stricter
df_watermarked = df_parsed.withWatermark("event_ts", "1 hour")
(df_watermarked.writeStream.format("delta")
  .option("checkpointLocation", "/checkpoints/bronze/events")
  .start("abfss://bronze@storage.dfs.core.windows.net/events/"))`,
  },
  {
    id: 74,
    group: 'Streaming Bronze',
    title: 'Micro-Batch Processing',
    desc: 'Uses Structured Streaming with trigger intervals to process Bronze micro-batches on a defined schedule.',
    code: `from pyspark.sql.functions import current_timestamp
df_stream = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/schema/bronze/events")
  .load(LANDING_PATH))
df_stream = df_stream.withColumn("_ingest_ts", current_timestamp())
(df_stream.writeStream
  .format("delta")
  .trigger(processingTime="5 minutes")  # 5-minute micro-batches
  .option("checkpointLocation", "/checkpoints/bronze/events")
  .start("abfss://bronze@storage.dfs.core.windows.net/events/"))`,
  },
  {
    id: 75,
    group: 'Streaming Bronze',
    title: 'Stream-to-Delta Write',
    desc: 'Writes Structured Streaming output directly to a Delta table in Bronze with exactly-once semantics.',
    code: `from pyspark.sql.functions import current_timestamp, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
schema = StructType([StructField("order_id", LongType()), StructField("customer_id", LongType()), StructField("amount", StringType())])
df_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKERS).option("subscribe", "orders").load()
df_parsed = df_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
df_parsed = df_parsed.withColumn("_ingest_ts", current_timestamp())
(df_parsed.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/checkpoints/bronze/orders").start("abfss://bronze@storage.dfs.core.windows.net/orders/"))`,
  },
  {
    id: 76,
    group: 'Streaming Bronze',
    title: 'Event Time Capture',
    desc: 'Preserves the original event timestamp from the source alongside the ingestion timestamp in Bronze.',
    code: `from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp
df_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKERS).option("subscribe", "events").load()
df_parsed = df_stream.selectExpr("CAST(value AS STRING) AS raw_json", "timestamp AS kafka_ts")
df_parsed = df_parsed.withColumn("_event_ts", col("kafka_ts")).withColumn("_ingest_ts", current_timestamp())
# Both event time and ingest time preserved at Bronze
(df_parsed.writeStream.format("delta").option("checkpointLocation", "/checkpoints/bronze/events").start("abfss://bronze@storage.dfs.core.windows.net/events/"))`,
  },
  {
    id: 77,
    group: 'Streaming Bronze',
    title: 'Duplicate Event Handling Flag',
    desc: 'Flags potential duplicate events in Bronze streaming using a dedup key — does not remove them at this layer.',
    code: `from pyspark.sql.functions import col, count
from pyspark.sql import Window
# After micro-batch lands, flag duplicates for Silver to deduplicate
df_batch = spark.read.format("delta").load("abfss://bronze@storage.dfs.core.windows.net/events/")
w = Window.partitionBy("event_id", "event_type")
df_flagged = df_batch.withColumn("_dup_flag", count("*").over(w) > 1)
dup_count = df_flagged.filter(col("_dup_flag")).count()
print(f"Duplicate events flagged in Bronze (not removed): {dup_count}")`,
  },
  {
    id: 78,
    group: 'Streaming Bronze',
    title: 'Backpressure Handling',
    desc: 'Configures Structured Streaming maxOffsetsPerTrigger to prevent Bronze cluster overload during Kafka bursts.',
    code: `df_kafka = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", KAFKA_BROKERS)
  .option("subscribe", "events-raw")
  .option("maxOffsetsPerTrigger", 100000)  # Cap per micro-batch
  .option("kafka.fetch.max.bytes", "52428800")  # 50MB max fetch
  .load())
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) AS raw_json", "timestamp AS event_ts")
df_parsed = df_parsed.withColumn("_ingest_ts", current_timestamp())
(df_parsed.writeStream.format("delta").option("checkpointLocation", "/checkpoints/bronze/events_bp").start("abfss://bronze@storage.dfs.core.windows.net/events/"))`,
  },
  {
    id: 79,
    group: 'Streaming Bronze',
    title: 'Replay from Offset',
    desc: 'Replays Bronze streaming from a specific Kafka offset or timestamp for reprocessing or recovery.',
    code: `import json
# Replay from specific Kafka offsets
starting_offsets = json.dumps({"events-raw": {"0": 500000, "1": 500000, "2": 500000}})
df_replay = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", KAFKA_BROKERS)
  .option("subscribe", "events-raw")
  .option("startingOffsets", starting_offsets)
  .option("endingOffsets", "latest")
  .load())
df_replay = df_replay.selectExpr("CAST(value AS STRING) AS raw_json").withColumn("_ingest_ts", current_timestamp()).withColumn("_is_replay", lit(True))
df_replay.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/events/")`,
  },
  {
    id: 80,
    group: 'Streaming Bronze',
    title: 'Stream Monitoring',
    desc: 'Monitors Bronze streaming query progress metrics (throughput, lag, batch duration) using StreamingQueryListener.',
    code: `from pyspark.sql.streaming import StreamingQueryListener
class BronzeStreamMonitor(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Stream started: {event.name}")
    def onQueryProgress(self, event):
        progress = event.progress
        print(f"Batch {progress.batchId}: input={progress.numInputRows} rows | rate={progress.processedRowsPerSecond:.1f} rows/s")
    def onQueryTerminated(self, event):
        print(f"Stream terminated: {event.id} | Exception: {event.exception}")
spark.streams.addListener(BronzeStreamMonitor())`,
  },

  // ─── 81–90: AI/RAG Bronze ───
  {
    id: 81,
    group: 'AI/RAG Bronze',
    title: 'Document Ingestion',
    desc: 'Ingests raw documents (PDF, DOCX, TXT) from a landing zone into a Bronze document store table.',
    code: `import base64
from pyspark.sql.functions import lit, current_timestamp, input_file_name
docs = dbutils.fs.ls("abfss://landing@storage.dfs.core.windows.net/rag_docs/")
rows = []
for doc in docs:
    content = dbutils.fs.head(doc.path, 1024*1024)  # Read up to 1MB
    rows.append({"file_name": doc.name, "file_path": doc.path,
                 "file_size": doc.size, "raw_content": content,
                 "ingested_at": str(current_timestamp())})
df_docs = spark.createDataFrame(rows)
df_docs.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/rag_documents/")`,
  },
  {
    id: 82,
    group: 'AI/RAG Bronze',
    title: 'Raw Text Extraction',
    desc: 'Extracts plain text from raw documents (PDF/DOCX) at Bronze ingestion time using Apache Tika via UDF.',
    code: `from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import subprocess
@udf(StringType())
def extract_text(file_path: str) -> str:
    try:
        result = subprocess.run(["java", "-jar", "/opt/tika-app.jar", "--text", file_path],
          capture_output=True, text=True, timeout=60)
        return result.stdout[:500000]  # Cap at 500K chars
    except Exception as e:
        return f"EXTRACT_ERROR: {str(e)}"
df_docs = df_docs.withColumn("extracted_text", extract_text("file_path"))
df_docs.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/rag_documents/")`,
  },
  {
    id: 83,
    group: 'AI/RAG Bronze',
    title: 'Metadata Capture Docs',
    desc: 'Captures document-level metadata (author, created date, page count, language) alongside raw content in Bronze.',
    code: `from pyspark.sql.functions import lit, current_timestamp
def extract_metadata(file_path: str) -> dict:
    # Placeholder: use python-docx / PyMuPDF / pdfplumber in production
    return {"author": "unknown", "page_count": -1, "language": "en", "doc_type": file_path.split(".")[-1]}
df_docs = df_docs.withColumn("_doc_author",     lit("extracted_from_source"))
df_docs = df_docs.withColumn("_doc_type",       lit("pdf"))
df_docs = df_docs.withColumn("_ingest_ts",      current_timestamp())
df_docs = df_docs.withColumn("_source_system",  lit("sharepoint"))
df_docs.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/rag_documents/")`,
  },
  {
    id: 84,
    group: 'AI/RAG Bronze',
    title: 'Document Versioning',
    desc: 'Tracks document versions in Bronze so updated source documents create new version records without overwriting.',
    code: `import hashlib
from pyspark.sql.functions import lit, current_timestamp
def compute_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()
df_docs = df_docs.withColumn("_content_hash", lit("computed_per_doc"))
# Check for existing version
existing = spark.sql("SELECT content_hash FROM bronze.rag_documents WHERE file_name = 'policy.pdf'")
# Only ingest if content changed
df_new = df_docs  # Filter to changed docs in production
df_new = df_new.withColumn("_version", lit(2)).withColumn("_ingest_ts", current_timestamp())
df_new.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/rag_documents/")`,
  },
  {
    id: 85,
    group: 'AI/RAG Bronze',
    title: 'Raw Storage for RAG',
    desc: 'Stores unmodified raw document content in Bronze as the source of truth before chunking and embedding.',
    code: `from pyspark.sql.functions import current_timestamp, lit, input_file_name
# Raw content stored as-is — no modification at Bronze
df_raw = spark.read.format("binaryFile").load("abfss://landing@storage.dfs.core.windows.net/rag_docs/")
df_raw = (df_raw
  .withColumnRenamed("content", "raw_bytes")
  .withColumn("_ingest_ts", current_timestamp())
  .withColumn("_layer", lit("bronze"))
  .withColumn("_modified", lit(False)))  # Immutable raw store
df_raw.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/rag_documents_raw/")`,
  },
  {
    id: 86,
    group: 'AI/RAG Bronze',
    title: 'PII Tagging in Docs',
    desc: 'Flags documents containing PII at Bronze ingestion using regex scanning and Unity Catalog column tags.',
    code: `from pyspark.sql.functions import col, regexp_extract, when, lit
# Basic regex PII detection on extracted text (Bronze: flag only, not mask)
df_docs = df_docs.withColumn("_has_email",
    col("extracted_text").rlike(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"))
df_docs = df_docs.withColumn("_has_ssn",
    col("extracted_text").rlike(r"\d{3}-\d{2}-\d{4}"))
df_docs = df_docs.withColumn("_pii_flag", col("_has_email") | col("_has_ssn"))
pii_count = df_docs.filter(col("_pii_flag")).count()
print(f"Documents with PII detected: {pii_count}")`,
  },
  {
    id: 87,
    group: 'AI/RAG Bronze',
    title: 'OCR Ingestion',
    desc: 'Runs OCR on scanned image-based PDFs at Bronze ingestion to extract machine-readable text.',
    code: `from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
@udf(StringType())
def run_ocr(file_path: str) -> str:
    try:
        import pytesseract
        from PIL import Image
        import fitz  # PyMuPDF
        doc = fitz.open(file_path)
        text = ""
        for page in doc:
            pix = page.get_pixmap(dpi=300)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            text += pytesseract.image_to_string(img)
        return text[:500000]
    except Exception as e:
        return f"OCR_ERROR: {str(e)}"
df_docs = df_docs.withColumn("ocr_text", run_ocr("file_path"))`,
  },
  {
    id: 88,
    group: 'AI/RAG Bronze',
    title: 'Multi-Format Ingestion',
    desc: 'Handles multiple document formats (PDF, DOCX, HTML, TXT, CSV) in a single Bronze RAG ingestion pipeline.',
    code: `import os
from pyspark.sql.functions import lit, current_timestamp, input_file_name
SUPPORTED_FORMATS = {".pdf", ".docx", ".txt", ".html", ".csv"}
files = dbutils.fs.ls("abfss://landing@storage.dfs.core.windows.net/rag_docs/")
valid_files = [f for f in files if any(f.name.endswith(ext) for ext in SUPPORTED_FORMATS)]
unsupported  = [f for f in files if not any(f.name.endswith(ext) for ext in SUPPORTED_FORMATS)]
print(f"Valid: {len(valid_files)} | Unsupported (skipped): {len(unsupported)}")
for f in valid_files:
    ext = os.path.splitext(f.name)[1]
    df = spark.read.text(f.path).withColumn("_format", lit(ext)).withColumn("_ingest_ts", current_timestamp())
    df.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/rag_documents/")`,
  },
  {
    id: 89,
    group: 'AI/RAG Bronze',
    title: 'Source Ranking Tagging',
    desc: 'Tags each ingested RAG document with a source reliability score to guide retrieval ranking in Silver.',
    code: `from pyspark.sql.functions import lit, current_timestamp
SOURCE_RANKS = {
  "official_policy": 5, "engineering_docs": 4,
  "wiki": 3, "email_archive": 2, "external_web": 1
}
SOURCE = dbutils.widgets.get("doc_source")
RANK   = SOURCE_RANKS.get(SOURCE, 1)
df_docs = (df_docs
  .withColumn("_source_category", lit(SOURCE))
  .withColumn("_source_rank",     lit(RANK))
  .withColumn("_ingest_ts",       current_timestamp()))
df_docs.write.format("delta").mode("append").save("abfss://bronze@storage.dfs.core.windows.net/rag_documents/")
print(f"Source: {SOURCE} | Rank: {RANK}/5")`,
  },
  {
    id: 90,
    group: 'AI/RAG Bronze',
    title: 'Raw Embedding Staging',
    desc: 'Stages raw document records in Bronze that are ready for embedding generation in the Silver/ML layer.',
    code: `from pyspark.sql.functions import col, lit, current_timestamp
# Bronze: flag records ready for embedding (text extracted, not yet chunked)
df_ready = spark.read.format("delta").load("abfss://bronze@storage.dfs.core.windows.net/rag_documents/").filter(
  col("extracted_text").isNotNull() & (col("extracted_text") != "") & col("_pii_flag") == False)
df_ready = df_ready.withColumn("_embedding_status", lit("PENDING"))
df_ready = df_ready.withColumn("_staged_at", current_timestamp())
# Write to staging table (Silver pipeline will pick these up)
df_ready.write.format("delta").mode("overwrite").save("abfss://bronze@storage.dfs.core.windows.net/rag_embedding_staging/")
print(f"Records staged for embedding: {df_ready.count()}")`,
  },

  // ─── 91–100: Observability ───
  {
    id: 91,
    group: 'Observability',
    title: 'Pipeline Monitoring',
    desc: 'Monitors Bronze ingestion pipeline health by tracking job status, duration, and record counts in a dashboard table.',
    code: `-- Bronze pipeline health dashboard query
SELECT
  pipeline_name,
  run_date,
  status,
  input_record_count,
  output_record_count,
  duration_seconds,
  CASE WHEN status = 'FAILED' THEN 'RED'
       WHEN duration_seconds > 3600 THEN 'YELLOW'
       ELSE 'GREEN' END AS health_status
FROM bronze_audit.pipeline_runs
WHERE run_date >= current_date() - 7
ORDER BY run_date DESC, pipeline_name;`,
  },
  {
    id: 92,
    group: 'Observability',
    title: 'SLA Monitoring',
    desc: 'Checks Bronze table freshness against defined SLAs and raises alerts when data is stale.',
    code: `-- Bronze SLA monitoring: data must be loaded within N hours of source update
SELECT
  table_name,
  MAX(_ingest_ts) AS last_ingested,
  TIMESTAMPDIFF(HOUR, MAX(_ingest_ts), current_timestamp()) AS hours_stale,
  CASE WHEN TIMESTAMPDIFF(HOUR, MAX(_ingest_ts), current_timestamp()) > 6 THEN 'SLA_BREACH'
       WHEN TIMESTAMPDIFF(HOUR, MAX(_ingest_ts), current_timestamp()) > 4 THEN 'SLA_WARNING'
       ELSE 'OK' END AS sla_status
FROM (
  SELECT 'bronze.orders' AS table_name, _ingest_ts FROM bronze.orders
  UNION ALL
  SELECT 'bronze.events', _ingest_ts FROM bronze.events
) GROUP BY table_name ORDER BY hours_stale DESC;`,
  },
  {
    id: 93,
    group: 'Observability',
    title: 'Data Volume Monitoring',
    desc: 'Tracks daily Bronze ingestion volumes to detect anomalies (unexpected drops or spikes) in record counts.',
    code: `-- Daily Bronze volume trend
SELECT
  _ingest_date,
  COUNT(*) AS daily_records,
  SUM(COUNT(*)) OVER (ORDER BY _ingest_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / 7 AS rolling_7d_avg,
  COUNT(*) / (SUM(COUNT(*)) OVER (ORDER BY _ingest_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / 7) AS ratio_vs_avg
FROM bronze.orders
GROUP BY _ingest_date
ORDER BY _ingest_date DESC
LIMIT 30;`,
  },
  {
    id: 94,
    group: 'Observability',
    title: 'Error Rate Tracking',
    desc: 'Monitors the ratio of failed/quarantined records to total records per Bronze ingestion run.',
    code: `-- Bronze error rate by pipeline
SELECT
  pipeline_name,
  run_date,
  total_records,
  failed_records,
  quarantine_records,
  ROUND(failed_records * 100.0 / NULLIF(total_records, 0), 2) AS error_rate_pct,
  CASE WHEN failed_records * 100.0 / NULLIF(total_records, 0) > 5 THEN 'HIGH_ERROR'
       WHEN failed_records * 100.0 / NULLIF(total_records, 0) > 1 THEN 'ELEVATED'
       ELSE 'NORMAL' END AS error_status
FROM bronze_audit.ingestion_summary
WHERE run_date >= current_date() - 30
ORDER BY run_date DESC;`,
  },
  {
    id: 95,
    group: 'Observability',
    title: 'Throughput Monitoring',
    desc: 'Measures Bronze ingestion throughput (records/second and MB/second) per job run for capacity planning.',
    code: `-- Bronze throughput metrics
SELECT
  pipeline_name,
  run_date,
  input_record_count,
  duration_seconds,
  ROUND(input_record_count / NULLIF(duration_seconds, 0), 0) AS records_per_sec,
  ROUND(bytes_ingested / 1024 / 1024 / NULLIF(duration_seconds, 0), 2) AS mb_per_sec,
  CASE WHEN input_record_count / NULLIF(duration_seconds, 0) < 1000 THEN 'SLOW'
       WHEN input_record_count / NULLIF(duration_seconds, 0) > 50000 THEN 'FAST'
       ELSE 'NORMAL' END AS throughput_status
FROM bronze_audit.pipeline_runs
WHERE run_date >= current_date() - 14
ORDER BY run_date DESC;`,
  },
  {
    id: 96,
    group: 'Observability',
    title: 'Latency Tracking',
    desc: 'Tracks end-to-end latency from source event time to Bronze landing time to measure ingestion lag.',
    code: `-- Bronze ingestion latency: event_ts to ingest_ts
SELECT
  _ingest_date,
  AVG(TIMESTAMPDIFF(SECOND, event_ts, _ingest_ts)) AS avg_latency_sec,
  MAX(TIMESTAMPDIFF(SECOND, event_ts, _ingest_ts)) AS max_latency_sec,
  PERCENTILE(TIMESTAMPDIFF(SECOND, event_ts, _ingest_ts), 0.95) AS p95_latency_sec,
  PERCENTILE(TIMESTAMPDIFF(SECOND, event_ts, _ingest_ts), 0.99) AS p99_latency_sec
FROM bronze.events
WHERE _ingest_date >= current_date() - 7
GROUP BY _ingest_date
ORDER BY _ingest_date DESC;`,
  },
  {
    id: 97,
    group: 'Observability',
    title: 'Cost Monitoring',
    desc: 'Tracks Databricks DBU consumption and storage costs per Bronze ingestion pipeline for FinOps visibility.',
    code: `-- Bronze cost monitoring via system tables
SELECT
  usage_date,
  usage_metadata.job_name AS pipeline,
  SUM(usage_quantity) AS total_dbus,
  SUM(usage_quantity) * 0.22 AS estimated_cost_usd  -- $0.22/DBU Jobs compute
FROM system.billing.usage
WHERE usage_metadata.job_name LIKE '%bronze%'
  AND usage_date >= current_date() - 30
GROUP BY usage_date, usage_metadata.job_name
ORDER BY usage_date DESC, total_dbus DESC;`,
  },
  {
    id: 98,
    group: 'Observability',
    title: 'Log Aggregation',
    desc: 'Aggregates Bronze pipeline logs from all jobs into a central audit log table for unified monitoring.',
    code: `-- Unified Bronze log aggregation query
SELECT
  logged_at,
  pipeline_name,
  log_level,
  message,
  batch_id,
  error_code,
  source_system
FROM (
  SELECT * FROM bronze_audit.ingestion_log
  UNION ALL
  SELECT * FROM bronze_audit.error_log
  UNION ALL
  SELECT * FROM bronze_audit.schema_change_log
)
WHERE logged_at >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY logged_at DESC
LIMIT 500;`,
  },
  {
    id: 99,
    group: 'Observability',
    title: 'Alerting System',
    desc: 'Configures Databricks SQL Alerts to notify on-call teams when Bronze SLA, error rate, or volume anomalies are detected.',
    code: `-- Create Databricks SQL Alert (via UI or REST API)
-- Alert query: detect Bronze SLA breach
SELECT COUNT(*) AS sla_breach_count
FROM (
  SELECT table_name, MAX(_ingest_ts) AS last_ingest,
    TIMESTAMPDIFF(HOUR, MAX(_ingest_ts), current_timestamp()) AS hours_stale
  FROM bronze.orders
  GROUP BY table_name
)
WHERE hours_stale > 6;
-- Alert condition: value > 0
-- Notification: email/Teams/PagerDuty
-- Schedule: every 30 minutes`,
  },
  {
    id: 100,
    group: 'Observability',
    title: 'Dashboard Reporting',
    desc: 'Provides a Databricks SQL dashboard with key Bronze KPIs: freshness, volume, error rate, throughput, and cost.',
    code: `-- Bronze layer health summary for dashboard
SELECT
  'Total Records (7d)'   AS metric, FORMAT_NUMBER(SUM(input_record_count), 0) AS value
  FROM bronze_audit.pipeline_runs WHERE run_date >= current_date() - 7
UNION ALL
SELECT 'Avg Latency (sec)', ROUND(AVG(avg_latency_sec), 1)
  FROM bronze_audit.latency_stats WHERE stat_date >= current_date() - 7
UNION ALL
SELECT 'Error Rate (%)', ROUND(AVG(error_rate_pct), 2)
  FROM bronze_audit.ingestion_summary WHERE run_date >= current_date() - 7
UNION ALL
SELECT 'Cost (7d USD)', ROUND(SUM(estimated_cost_usd), 2)
  FROM bronze_audit.cost_summary WHERE usage_date >= current_date() - 7;`,
  },
];

const groups = [...new Set(bronzeOperations.map((op) => op.group))];

const ANTI_PATTERNS = [
  'Applying business logic (filtering, joins, aggregations) at the Bronze layer — Bronze is raw only.',
  'Deleting or updating records in Bronze — it must be append-only and immutable.',
  'Storing only transformed data — always preserve the original raw record in Bronze.',
  'Skipping metadata columns (_ingest_ts, _source_system, _batch_id) — they are mandatory for lineage.',
  'Mixing Bronze and Silver responsibilities in the same pipeline — keep layers strictly separated.',
  'Using overwrite mode on Bronze tables — this destroys raw history and breaks replay capability.',
  'Ingesting without checkpointing in streaming pipelines — this causes data loss on restart.',
  'Hard-coding credentials (passwords, API keys) — always use Databricks secret scopes.',
  'Not partitioning Bronze tables — full scans on multi-TB raw tables cause severe performance issues.',
  'Skipping quarantine for corrupt/bad records — dropping them silently hides data quality issues.',
  'Allowing analysts direct write access to Bronze — only service principals should write.',
  'Ingesting without schema validation or evolution handling — schema drift will break pipelines silently.',
];

function BronzeOperations() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const filtered = bronzeOperations.filter((op) => {
    const matchGroup = selectedGroup === 'All' || op.group === selectedGroup;
    const matchSearch =
      op.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      op.desc.toLowerCase().includes(searchTerm.toLowerCase()) ||
      op.group.toLowerCase().includes(searchTerm.toLowerCase());
    return matchGroup && matchSearch;
  });

  const downloadCSV = () => {
    exportToCSV(
      bronzeOperations.map((op) => ({
        id: op.id,
        group: op.group,
        title: op.title,
        desc: op.desc,
      })),
      'bronze-operations.csv'
    );
  };

  return (
    <div>
      {/* Page Header */}
      <div className="page-header">
        <div>
          <h1>Bronze Layer Operations</h1>
          <p>
            100 operations across 10 categories &mdash; Raw ingestion, metadata, quality, security,
            streaming, AI/RAG
          </p>
        </div>
      </div>

      {/* Stat Cards */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#128190;</div>
          <div className="stat-info">
            <h4>100</h4>
            <p>Total Operations</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#128193;</div>
          <div className="stat-info">
            <h4>{groups.length}</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#9889;</div>
          <div className="stat-info">
            <h4>10</h4>
            <p>Ops per Category</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#128274;</div>
          <div className="stat-info">
            <h4>Raw</h4>
            <p>Layer</p>
          </div>
        </div>
      </div>

      {/* Flow Diagram */}
      <div
        className="card"
        style={{ marginBottom: '1rem', background: '#f0f9ff', border: '1px solid #bae6fd' }}
      >
        <div style={{ fontWeight: 700, marginBottom: '0.5rem', color: '#0369a1' }}>
          Bronze Layer Flow
        </div>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem',
            flexWrap: 'wrap',
            fontSize: '0.9rem',
            fontFamily: 'monospace',
          }}
        >
          {[
            'Source Systems',
            'Ingest',
            'Raw Store (Bronze)',
            'Metadata + Audit',
            'Pass to Silver',
          ].map((step, i, arr) => (
            <React.Fragment key={step}>
              <span
                style={{
                  padding: '0.3rem 0.75rem',
                  background: '#0ea5e9',
                  color: '#fff',
                  borderRadius: '6px',
                  fontWeight: 600,
                  fontSize: '0.8rem',
                }}
              >
                {step}
              </span>
              {i < arr.length - 1 && (
                <span style={{ color: '#0369a1', fontWeight: 700, fontSize: '1.1rem' }}>
                  &rarr;
                </span>
              )}
            </React.Fragment>
          ))}
        </div>
      </div>

      {/* Filters + Search + Download */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search Bronze operations..."
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
                {g} ({bronzeOperations.filter((op) => op.group === g).length})
              </option>
            ))}
          </select>
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Showing {filtered.length} of 100
          </span>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadCSV}
            style={{ marginLeft: 'auto' }}
          >
            Download CSV
          </button>
        </div>
      </div>

      {/* Operation List */}
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
                <ScenarioCard scenario={op} />
              </div>
            )}
          </div>
        );
      })}

      {filtered.length === 0 && (
        <div
          className="card"
          style={{ textAlign: 'center', color: 'var(--text-secondary)', padding: '2rem' }}
        >
          No operations match your search. Try a different keyword or category.
        </div>
      )}

      {/* Anti-Patterns Section */}
      <div
        className="card"
        style={{
          marginTop: '2rem',
          background: '#fef2f2',
          border: '1px solid #fca5a5',
        }}
      >
        <div
          style={{
            fontWeight: 700,
            fontSize: '1rem',
            color: '#dc2626',
            marginBottom: '0.75rem',
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem',
          }}
        >
          <span>&#9888;</span> Bronze Layer Anti-Patterns &mdash; What NOT To Do
        </div>
        <ul style={{ margin: 0, paddingLeft: '1.25rem', color: '#7f1d1d' }}>
          {ANTI_PATTERNS.map((ap, i) => (
            <li key={i} style={{ marginBottom: '0.4rem', fontSize: '0.875rem', lineHeight: '1.5' }}>
              {ap}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default BronzeOperations;
