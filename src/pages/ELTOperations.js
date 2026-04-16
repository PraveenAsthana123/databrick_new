import React, { useState } from 'react';
import ScenarioCard from '../components/common/ScenarioCard';

// ─── Tab 1: ELT Transactions (10 scenarios) ────────────────────────────────────
const eltTransactions = [
  {
    id: 1,
    title: 'Extract Patterns',
    desc: 'Extract data from multiple sources using Auto Loader and JDBC in parallel',
    code: `# Auto Loader extract from cloud storage
df_files = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "json") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/orders") \\
    .option("cloudFiles.inferColumnTypes", "true") \\
    .load("s3://raw-bucket/orders/")

# JDBC extract from source database
df_jdbc = spark.read.format("jdbc") \\
    .option("url", "jdbc:postgresql://source-db:5432/prod") \\
    .option("dbtable", "(SELECT * FROM orders WHERE updated_at > '2024-01-01') sub") \\
    .option("user", dbutils.secrets.get("scope", "pg_user")) \\
    .option("password", dbutils.secrets.get("scope", "pg_pass")) \\
    .option("fetchsize", "50000") \\
    .option("numPartitions", "8") \\
    .option("partitionColumn", "order_id") \\
    .option("lowerBound", "1") \\
    .option("upperBound", "10000000") \\
    .load()

# Write extracted data to bronze layer
df_files.writeStream \\
    .format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/orders_bronze") \\
    .trigger(availableNow=True) \\
    .toTable("bronze.orders_stream")

df_jdbc.write.format("delta").mode("overwrite").saveAsTable("bronze.orders_jdbc")`,
  },
  {
    id: 2,
    title: 'Load Strategies',
    desc: 'Overwrite, append, merge, and partition-overwrite loading patterns',
    code: `# Strategy 1: Full overwrite
df.write.format("delta").mode("overwrite").saveAsTable("silver.customers")

# Strategy 2: Append only (for immutable event logs)
df.write.format("delta").mode("append").saveAsTable("bronze.event_log")

# Strategy 3: Partition overwrite (replace only affected partitions)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.format("delta") \\
    .mode("overwrite") \\
    .partitionBy("load_date") \\
    .saveAsTable("silver.transactions")

# Strategy 4: Merge / Upsert
from delta.tables import DeltaTable
target = DeltaTable.forName(spark, "silver.products")
target.alias("t").merge(
    df.alias("s"), "t.product_id = s.product_id"
).whenMatchedUpdateAll() \\
 .whenNotMatchedInsertAll() \\
 .execute()

# Strategy 5: Insert overwrite with replaceWhere
df.write.format("delta") \\
    .mode("overwrite") \\
    .option("replaceWhere", "region = 'US' AND load_date = '2024-03-15'") \\
    .saveAsTable("silver.sales")`,
  },
  {
    id: 3,
    title: 'Transform Approaches',
    desc: 'SQL-based, DataFrame-based, and DLT transformation patterns',
    code: `# Approach 1: DataFrame API transformations
from pyspark.sql.functions import col, when, lit, upper, trim, to_date, current_timestamp

df_silver = (
    df_bronze
    .filter(col("status").isNotNull())
    .withColumn("customer_name", upper(trim(col("customer_name"))))
    .withColumn("order_date", to_date(col("order_date_str"), "yyyy-MM-dd"))
    .withColumn("order_category", when(col("amount") > 1000, "high")
                                  .when(col("amount") > 100, "medium")
                                  .otherwise("low"))
    .withColumn("_loaded_at", current_timestamp())
    .dropDuplicates(["order_id"])
    .drop("order_date_str", "_rescued_data")
)

# Approach 2: SQL-based transformation
spark.sql("""
    CREATE OR REPLACE TABLE silver.orders AS
    SELECT
        order_id,
        UPPER(TRIM(customer_name)) AS customer_name,
        TO_DATE(order_date_str, 'yyyy-MM-dd') AS order_date,
        amount,
        CASE
            WHEN amount > 1000 THEN 'high'
            WHEN amount > 100 THEN 'medium'
            ELSE 'low'
        END AS order_category,
        current_timestamp() AS _loaded_at
    FROM bronze.orders
    WHERE status IS NOT NULL
""")

# Approach 3: DLT expectations for quality enforcement
import dlt
@dlt.table(comment="Cleansed orders")
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_date", "order_date IS NOT NULL")
def silver_orders():
    return dlt.read("bronze_orders").select("order_id", "customer_name", "amount", "order_date")`,
  },
  {
    id: 4,
    title: 'Incremental ELT',
    desc: 'Watermark-based and checkpoint-driven incremental processing',
    code: `# Pattern 1: Auto Loader incremental with schema evolution
df_incremental = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "parquet") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/events") \\
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\
    .load("s3://data-lake/events/")

df_incremental.writeStream \\
    .format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/events") \\
    .option("mergeSchema", "true") \\
    .trigger(availableNow=True) \\
    .toTable("bronze.events")

# Pattern 2: Watermark-based incremental
last_watermark = spark.sql(
    "SELECT COALESCE(MAX(updated_at), '1900-01-01') AS wm FROM silver.customers"
).collect()[0]["wm"]

df_new = spark.read.format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", f"(SELECT * FROM customers WHERE updated_at > '{last_watermark}') sub") \\
    .load()

# Merge incremental into target
from delta.tables import DeltaTable
target = DeltaTable.forName(spark, "silver.customers")
target.alias("t").merge(
    df_new.alias("s"), "t.customer_id = s.customer_id"
).whenMatchedUpdateAll() \\
 .whenNotMatchedInsertAll() \\
 .execute()

# Pattern 3: MERGE INTO with foreachBatch for streaming
def upsert_micro_batch(batch_df, batch_id):
    target = DeltaTable.forName(spark, "silver.orders")
    target.alias("t").merge(
        batch_df.alias("s"), "t.order_id = s.order_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

df_stream.writeStream \\
    .foreachBatch(upsert_micro_batch) \\
    .option("checkpointLocation", "/mnt/checkpoints/orders_upsert") \\
    .trigger(availableNow=True) \\
    .start()`,
  },
  {
    id: 5,
    title: 'Full Refresh ELT',
    desc: 'Complete table rebuild with validation and swap pattern',
    code: `from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

# Step 1: Extract all data from source
df_source = spark.read.format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", "public.dim_product") \\
    .load()

# Step 2: Transform
df_transformed = (
    df_source
    .withColumn("_etl_loaded_at", current_timestamp())
    .withColumn("_etl_source", lit("postgresql_prod"))
)

# Step 3: Write to staging table
df_transformed.write.format("delta") \\
    .mode("overwrite") \\
    .saveAsTable("staging.dim_product_new")

# Step 4: Validate row counts
source_count = df_source.count()
target_count = spark.table("staging.dim_product_new").count()
assert target_count == source_count, \\
    f"Row count mismatch: source={source_count}, target={target_count}"

# Step 5: Atomic swap using RENAME
spark.sql("ALTER TABLE silver.dim_product RENAME TO silver.dim_product_backup")
spark.sql("ALTER TABLE staging.dim_product_new RENAME TO silver.dim_product")

# Step 6: Cleanup old backup after verification
spark.sql("DROP TABLE IF EXISTS silver.dim_product_backup")`,
  },
  {
    id: 6,
    title: 'Merge Patterns',
    desc: 'Complex MERGE operations with conditional updates and deletes',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, col, lit

target = DeltaTable.forName(spark, "silver.customers")

# Pattern 1: Upsert with conditional update (only update if source is newer)
target.alias("t").merge(
    df_source.alias("s"), "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    condition="s.updated_at > t.updated_at",
    set={
        "customer_name": "s.customer_name",
        "email": "s.email",
        "phone": "s.phone",
        "updated_at": "s.updated_at",
        "_etl_updated_at": "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
        "customer_id": "s.customer_id",
        "customer_name": "s.customer_name",
        "email": "s.email",
        "phone": "s.phone",
        "updated_at": "s.updated_at",
        "_etl_loaded_at": "current_timestamp()",
        "_etl_updated_at": "current_timestamp()"
    }
).execute()

# Pattern 2: Merge with delete (soft delete from source)
target.alias("t").merge(
    df_source.alias("s"), "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    condition="s.is_deleted = true",
    set={"is_active": lit(False), "_etl_deleted_at": current_timestamp()}
).whenMatchedUpdate(
    condition="s.is_deleted = false AND s.updated_at > t.updated_at",
    set={"customer_name": "s.customer_name", "email": "s.email", "is_active": lit(True)}
).whenNotMatchedInsert(
    condition="s.is_deleted = false",
    values={"customer_id": "s.customer_id", "customer_name": "s.customer_name",
            "email": "s.email", "is_active": lit(True), "_etl_loaded_at": current_timestamp()}
).execute()

# Pattern 3: SQL MERGE with multiple WHEN clauses
spark.sql("""
    MERGE INTO silver.inventory t
    USING bronze.inventory_updates s
    ON t.sku = s.sku AND t.warehouse_id = s.warehouse_id
    WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
    WHEN MATCHED AND s.operation = 'UPDATE' THEN UPDATE SET
        t.quantity = s.quantity, t.updated_at = current_timestamp()
    WHEN NOT MATCHED AND s.operation != 'DELETE' THEN INSERT *
""")`,
  },
  {
    id: 7,
    title: 'ELT Error Handling',
    desc: 'Try-catch patterns, quarantine tables, and dead letter queues',
    code: `from pyspark.sql.functions import col, current_timestamp, lit, to_date
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("elt_pipeline")

# Pattern 1: Quarantine bad records
df_raw = spark.read.format("cloudFiles") \\
    .option("cloudFiles.format", "json") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/orders") \\
    .option("rescuedDataColumn", "_rescued_data") \\
    .load("s3://raw/orders/")

# Good records -> silver, bad records -> quarantine
df_good = df_raw.filter(col("_rescued_data").isNull() & col("order_id").isNotNull())
df_bad = df_raw.filter(col("_rescued_data").isNotNull() | col("order_id").isNull())

df_good.write.format("delta").mode("append").saveAsTable("silver.orders")
df_bad.withColumn("_quarantine_reason", lit("schema_mismatch")) \\
    .withColumn("_quarantined_at", current_timestamp()) \\
    .write.format("delta").mode("append").saveAsTable("quarantine.orders")

logger.info(f"Processed: {df_good.count()} good, {df_bad.count()} quarantined")

# Pattern 2: DLT expectations for quality gates
import dlt

@dlt.table(comment="Quality-checked orders")
@dlt.expect("valid_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0 AND amount < 1000000")
@dlt.expect_or_fail("valid_date", "order_date >= '2020-01-01'")
def silver_orders_validated():
    return dlt.read("bronze_orders")

# Pattern 3: Pipeline-level try/catch with status tracking
def run_elt_step(step_name, func):
    try:
        logger.info(f"Starting ELT step: {step_name}")
        result = func()
        spark.sql(f"""
            INSERT INTO audit.elt_log VALUES (
                '{step_name}', 'SUCCESS', current_timestamp(), NULL
            )
        """)
        return result
    except Exception as e:
        logger.error(f"ELT step failed: {step_name} - {str(e)}")
        spark.sql(f"""
            INSERT INTO audit.elt_log VALUES (
                '{step_name}', 'FAILED', current_timestamp(), '{str(e)[:500]}'
            )
        """)
        raise`,
  },
  {
    id: 8,
    title: 'ELT Audit Logging',
    desc: 'Track pipeline runs, row counts, data quality metrics, and lineage',
    code: `from pyspark.sql.functions import current_timestamp, lit, count, sum as _sum
from datetime import datetime
import uuid

# Create audit log table
spark.sql("""
    CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
        run_id STRING,
        pipeline_name STRING,
        step_name STRING,
        status STRING,
        source_count LONG,
        target_count LONG,
        rejected_count LONG,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        error_message STRING,
        parameters MAP<STRING, STRING>
    ) USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# Audit wrapper for ELT steps
class ELTAuditLogger:
    def __init__(self, pipeline_name):
        self.run_id = str(uuid.uuid4())
        self.pipeline_name = pipeline_name

    def log_step(self, step_name, source_df, target_table, transform_func):
        started = datetime.utcnow()
        source_count = source_df.count()
        try:
            result_df = transform_func(source_df)
            result_df.write.format("delta").mode("append").saveAsTable(target_table)
            target_count = spark.table(target_table).count()
            self._write_log(step_name, "SUCCESS", source_count,
                           target_count, 0, started, None)
            return result_df
        except Exception as e:
            self._write_log(step_name, "FAILED", source_count,
                           0, 0, started, str(e)[:500])
            raise

    def _write_log(self, step, status, src, tgt, rej, started, error):
        from pyspark.sql import Row
        log = Row(run_id=self.run_id, pipeline_name=self.pipeline_name,
                  step_name=step, status=status, source_count=src,
                  target_count=tgt, rejected_count=rej,
                  started_at=started, completed_at=datetime.utcnow(),
                  error_message=error, parameters={})
        spark.createDataFrame([log]).write.format("delta") \\
            .mode("append").saveAsTable("audit.pipeline_runs")

# Usage
audit = ELTAuditLogger("daily_orders")
audit.log_step("extract", df_raw, "bronze.orders", lambda df: df)
audit.log_step("transform", df_bronze, "silver.orders", transform_orders)`,
  },
  {
    id: 9,
    title: 'Parallel ELT',
    desc: 'Run multiple ELT pipelines concurrently using ThreadPoolExecutor and Databricks Jobs',
    code: `from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger("parallel_elt")

# Define independent ELT tasks
def elt_customers():
    df = spark.read.format("jdbc").option("dbtable", "customers").load()
    df.write.format("delta").mode("overwrite").saveAsTable("bronze.customers")
    return "customers", df.count()

def elt_orders():
    df = spark.read.format("jdbc").option("dbtable", "orders").load()
    df.write.format("delta").mode("overwrite").saveAsTable("bronze.orders")
    return "orders", df.count()

def elt_products():
    df = spark.read.format("jdbc").option("dbtable", "products").load()
    df.write.format("delta").mode("overwrite").saveAsTable("bronze.products")
    return "products", df.count()

def elt_inventory():
    df = spark.read.format("jdbc").option("dbtable", "inventory").load()
    df.write.format("delta").mode("overwrite").saveAsTable("bronze.inventory")
    return "inventory", df.count()

# Run extractions in parallel
tasks = [elt_customers, elt_orders, elt_products, elt_inventory]
results = {}

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(task): task.__name__ for task in tasks}
    for future in as_completed(futures):
        task_name = futures[future]
        try:
            table, count = future.result(timeout=600)
            results[table] = {"status": "SUCCESS", "rows": count}
            logger.info(f"Completed {table}: {count} rows")
        except Exception as e:
            results[task_name] = {"status": "FAILED", "error": str(e)}
            logger.error(f"Failed {task_name}: {e}")

# Summary
for table, info in results.items():
    print(f"  {table}: {info}")`,
  },
  {
    id: 10,
    title: 'ELT Orchestration',
    desc: 'Multi-step pipeline orchestration with dependencies using Databricks Workflows',
    code: `# Databricks Workflow JSON definition (Jobs API 2.1)
workflow_config = {
    "name": "daily_elt_pipeline",
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "UTC"
    },
    "tasks": [
        {
            "task_key": "extract_customers",
            "notebook_task": {"notebook_path": "/Repos/etl/extract_customers"},
            "cluster_id": "existing-cluster-id"
        },
        {
            "task_key": "extract_orders",
            "notebook_task": {"notebook_path": "/Repos/etl/extract_orders"},
            "cluster_id": "existing-cluster-id"
        },
        {
            "task_key": "transform_silver",
            "depends_on": [
                {"task_key": "extract_customers"},
                {"task_key": "extract_orders"}
            ],
            "notebook_task": {"notebook_path": "/Repos/etl/transform_silver"}
        },
        {
            "task_key": "build_gold",
            "depends_on": [{"task_key": "transform_silver"}],
            "notebook_task": {"notebook_path": "/Repos/etl/build_gold"}
        },
        {
            "task_key": "validate_output",
            "depends_on": [{"task_key": "build_gold"}],
            "notebook_task": {"notebook_path": "/Repos/etl/validate_output"}
        }
    ],
    "email_notifications": {
        "on_failure": ["data-team@company.com"]
    },
    "max_concurrent_runs": 1
}

# DLT Pipeline orchestration alternative
# dbutils.notebook.run() for chained notebook execution
def run_pipeline():
    steps = [
        ("/Repos/etl/01_extract", 600),
        ("/Repos/etl/02_transform", 1200),
        ("/Repos/etl/03_validate", 300),
        ("/Repos/etl/04_publish", 300),
    ]
    for notebook, timeout in steps:
        result = dbutils.notebook.run(notebook, timeout)
        if result != "SUCCESS":
            raise Exception(f"Pipeline failed at {notebook}: {result}")

run_pipeline()`,
  },
];

// ─── Tab 2: SCD (Slowly Changing Dimensions) (8 scenarios) ─────────────────────
const scdScenarios = [
  {
    id: 11,
    title: 'SCD Type 0 - Fixed Dimension',
    desc: 'Attributes that never change after initial load (e.g., original credit score)',
    code: `from delta.tables import DeltaTable

# Type 0: Insert only, never update existing records
# Use case: regulatory snapshots, immutable reference data

target = DeltaTable.forName(spark, "silver.dim_customer_original")

target.alias("t").merge(
    df_source.alias("s"),
    "t.customer_id = s.customer_id"
).whenNotMatchedInsertAll() \\
 .execute()  # No whenMatchedUpdate - existing records are never modified

# SQL equivalent
spark.sql("""
    MERGE INTO silver.dim_customer_original t
    USING staging.customer_updates s
    ON t.customer_id = s.customer_id
    WHEN NOT MATCHED THEN INSERT *
    -- No WHEN MATCHED clause: existing rows remain unchanged
""")`,
  },
  {
    id: 12,
    title: 'SCD Type 1 - Overwrite',
    desc: 'Current value only, no history preserved. Update in place.',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Type 1: Overwrite old value with new value
# Use case: correcting errors, non-historical attributes (email, phone)

target = DeltaTable.forName(spark, "silver.dim_customer")

target.alias("t").merge(
    df_updates.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(set={
    "customer_name": "s.customer_name",
    "email": "s.email",
    "phone": "s.phone",
    "city": "s.city",
    "updated_at": "current_timestamp()"
}).whenNotMatchedInsert(values={
    "customer_id": "s.customer_id",
    "customer_name": "s.customer_name",
    "email": "s.email",
    "phone": "s.phone",
    "city": "s.city",
    "created_at": "current_timestamp()",
    "updated_at": "current_timestamp()"
}).execute()

# Verify
spark.sql("SELECT * FROM silver.dim_customer WHERE customer_id = 'C001'").show()`,
  },
  {
    id: 13,
    title: 'SCD Type 2 - Historical Tracking',
    desc: 'Full history with effective dates and current flag. New row per change.',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col, sha2, concat_ws

# Type 2: Add new row for each change, track history with effective dates
# Step 1: Generate hash of trackable columns for change detection
df_source = df_updates.withColumn(
    "row_hash", sha2(concat_ws("||", "customer_name", "email", "city"), 256)
)

# Step 2: Identify changes by comparing hashes
df_existing = spark.table("silver.dim_customer_scd2") \\
    .filter(col("is_current") == True)

df_changes = df_source.alias("s").join(
    df_existing.alias("t"),
    col("s.customer_id") == col("t.customer_id"),
    "left"
).filter(
    col("t.customer_id").isNull() |  # New records
    (col("s.row_hash") != col("t.row_hash"))  # Changed records
).select("s.*")

# Step 3: Close existing records (expire them)
target = DeltaTable.forName(spark, "silver.dim_customer_scd2")
target.alias("t").merge(
    df_changes.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "effective_end_date": "current_timestamp()"
}).execute()

# Step 4: Insert new current versions
df_new_rows = df_changes.select(
    "customer_id", "customer_name", "email", "city", "row_hash"
).withColumn("effective_start_date", current_timestamp()) \\
 .withColumn("effective_end_date", lit("9999-12-31").cast("timestamp")) \\
 .withColumn("is_current", lit(True))

df_new_rows.write.format("delta").mode("append") \\
    .saveAsTable("silver.dim_customer_scd2")`,
  },
  {
    id: 14,
    title: 'SCD Type 3 - Previous Value Column',
    desc: 'Keep current and previous value in separate columns, limited history',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Type 3: Store current + previous value only (one level of history)
# Use case: track last known city before move

# Table schema:
# customer_id, city_current, city_previous, city_changed_at, ...

target = DeltaTable.forName(spark, "silver.dim_customer_type3")

target.alias("t").merge(
    df_updates.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    condition="t.city_current != s.city",  # Only update if city actually changed
    set={
        "city_previous": "t.city_current",
        "city_current": "s.city",
        "city_changed_at": "current_timestamp()",
        "customer_name": "s.customer_name",
        "email": "s.email"
    }
).whenMatchedUpdate(
    condition="t.city_current = s.city",  # Non-tracked changes
    set={
        "customer_name": "s.customer_name",
        "email": "s.email"
    }
).whenNotMatchedInsert(values={
    "customer_id": "s.customer_id",
    "customer_name": "s.customer_name",
    "email": "s.email",
    "city_current": "s.city",
    "city_previous": "NULL",
    "city_changed_at": "current_timestamp()"
}).execute()`,
  },
  {
    id: 15,
    title: 'SCD Type 4 - Mini-Dimension',
    desc: 'Separate history table for rapidly changing attributes',
    code: `from pyspark.sql.functions import current_timestamp, monotonically_increasing_id, lit

# Type 4: Mini-dimension pattern
# Main dimension (slow-changing): customer core attributes
# Mini-dimension (fast-changing): customer behavior/scores

# Step 1: Update main dimension (SCD Type 1)
spark.sql("""
    MERGE INTO silver.dim_customer_main t
    USING staging.customer_updates s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET
        t.customer_name = s.customer_name,
        t.date_of_birth = s.date_of_birth,
        t.gender = s.gender,
        t.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT *
""")

# Step 2: Always insert into mini-dimension (append-only history)
df_mini = df_updates.select(
    "customer_id",
    "credit_score",
    "income_band",
    "loyalty_tier",
    "spending_segment"
).withColumn("mini_dim_key", monotonically_increasing_id()) \\
 .withColumn("effective_date", current_timestamp()) \\
 .withColumn("is_current", lit(True))

# Expire previous current records
spark.sql("""
    UPDATE silver.dim_customer_mini
    SET is_current = false
    WHERE customer_id IN (SELECT DISTINCT customer_id FROM staging.customer_updates)
    AND is_current = true
""")

# Insert new mini-dimension records
df_mini.write.format("delta").mode("append").saveAsTable("silver.dim_customer_mini")

# Fact table joins both: fact.customer_key -> dim_main, fact.mini_dim_key -> dim_mini`,
  },
  {
    id: 16,
    title: 'SCD Type 6 - Hybrid (1+2+3)',
    desc: 'Combines Type 1, 2, and 3: historical rows, current flag, and previous value columns',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col

# Type 6 = Type 1 + Type 2 + Type 3 combined
# Each row has: effective dates (Type 2), previous value (Type 3),
# and a "current_*" column updated across all rows (Type 1 behavior)

# Table: customer_id, name, city_current (T1), city_historical (T2),
#         city_previous (T3), effective_start, effective_end, is_current

# Step 1: Close old current record (Type 2 behavior)
target = DeltaTable.forName(spark, "silver.dim_customer_type6")
target.alias("t").merge(
    df_updates.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(
    condition="t.city_historical != s.city",
    set={
        "is_current": lit(False),
        "effective_end_date": "current_timestamp()",
        "city_current": "s.city"  # Type 1: update current even on old row
    }
).execute()

# Step 2: Insert new current row
df_changed = df_updates.alias("s").join(
    spark.table("silver.dim_customer_type6")
        .filter(col("is_current") == False)
        .groupBy("customer_id").agg({"effective_end_date": "max"})
        .alias("t"),
    "customer_id", "inner"
)

df_new = df_updates.select(
    "customer_id", "customer_name",
    col("city").alias("city_current"),       # Type 1: always current
    col("city").alias("city_historical"),     # Type 2: this version
).withColumn("city_previous", lit(None))  \\
 .withColumn("effective_start_date", current_timestamp()) \\
 .withColumn("effective_end_date", lit("9999-12-31").cast("timestamp")) \\
 .withColumn("is_current", lit(True))

df_new.write.format("delta").mode("append") \\
    .saveAsTable("silver.dim_customer_type6")

# Step 3: Type 1 update - update city_current on ALL historical rows
spark.sql("""
    MERGE INTO silver.dim_customer_type6 t
    USING staging.customer_updates s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET t.city_current = s.city
""")`,
  },
  {
    id: 17,
    title: 'SCD with Delta MERGE',
    desc: 'Efficient SCD Type 2 implementation using Delta Lake MERGE with multiple WHEN clauses',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import *

# Efficient SCD2 with single MERGE statement using staged updates
# This avoids multiple passes over the data

# Step 1: Prepare staged updates (union of closes + inserts)
df_updates_with_hash = df_source.withColumn(
    "hash", sha2(concat_ws("||", *["name", "email", "city", "phone"]), 256)
)

# Identify which records actually changed
df_current = spark.table("silver.dim_customer_scd2").filter("is_current = true")
df_joined = df_updates_with_hash.alias("s").join(
    df_current.alias("t"), "customer_id", "left"
)

df_changed = df_joined.filter(
    col("t.customer_id").isNull() | (col("s.hash") != col("t.hash"))
).select("s.*")

# Build staged updates: close rows + new rows in one DataFrame
df_close_rows = df_changed.select(
    col("customer_id"),
    lit(None).alias("name"), lit(None).alias("email"),
    lit(None).alias("city"), lit(None).alias("phone"),
    lit(None).alias("hash"),
    lit("close").alias("_merge_action")
)

df_insert_rows = df_changed.withColumn("_merge_action", lit("insert"))
df_staged = df_close_rows.unionByName(df_insert_rows)

# Step 2: Single MERGE with multiple WHEN clauses
target = DeltaTable.forName(spark, "silver.dim_customer_scd2")
target.alias("t").merge(
    df_staged.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true AND s._merge_action = 'close'"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "effective_end_date": current_timestamp()
}).whenNotMatchedInsert(
    condition="s._merge_action = 'insert'",
    values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "email": "s.email",
        "city": "s.city",
        "phone": "s.phone",
        "hash": "s.hash",
        "is_current": lit(True),
        "effective_start_date": current_timestamp(),
        "effective_end_date": lit("9999-12-31").cast("timestamp")
    }
).execute()`,
  },
  {
    id: 18,
    title: 'SCD with DLT (Delta Live Tables)',
    desc: 'Declarative SCD Type 1 and Type 2 using DLT apply_changes',
    code: `import dlt
from pyspark.sql.functions import col

# DLT makes SCD implementation declarative and simple

# Source: streaming table from CDC feed
@dlt.table(comment="Raw CDC events from source system")
def bronze_customer_cdc():
    return spark.readStream.format("cloudFiles") \\
        .option("cloudFiles.format", "json") \\
        .load("/mnt/cdc/customers/")

# SCD Type 1 with DLT apply_changes (overwrite, no history)
dlt.create_streaming_table("silver_customer_scd1")
dlt.apply_changes(
    target="silver_customer_scd1",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1  # Overwrite (Type 1)
)

# SCD Type 2 with DLT apply_changes (full history)
dlt.create_streaming_table("silver_customer_scd2")
dlt.apply_changes(
    target="silver_customer_scd2",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2,  # Historical tracking (Type 2)
    # DLT automatically manages:
    # __START_AT, __END_AT columns (effective dates)
    # __IS_CURRENT flag
)

# SCD Type 2 with specific tracked columns
dlt.create_streaming_table("silver_customer_scd2_tracked")
dlt.apply_changes(
    target="silver_customer_scd2_tracked",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2,
    track_history_column_list=["city", "income_band", "loyalty_tier"],
    # Only create new history row when tracked columns change
    # Other column changes update in place (Type 1 behavior)
)`,
  },
];

// ─── Tab 3: CDC (Change Data Capture) (8 scenarios) ────────────────────────────
const cdcScenarios = [
  {
    id: 19,
    title: 'Debezium CDC',
    desc: 'Process Debezium CDC events from Kafka with schema registry',
    code: `from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp

# Read Debezium CDC events from Kafka
df_cdc = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \\
    .option("subscribe", "dbserver1.public.customers") \\
    .option("startingOffsets", "earliest") \\
    .load()

# Parse Debezium envelope format
cdc_schema = spark.read.json(
    spark.sparkContext.parallelize([sample_json])
).schema

df_parsed = df_cdc.select(
    from_json(col("value").cast("string"), cdc_schema).alias("data")
).select(
    col("data.payload.op").alias("operation"),       # c=create, u=update, d=delete
    col("data.payload.after.*"),                      # New values
    col("data.payload.ts_ms").alias("event_timestamp"),
    col("data.payload.source.lsn").alias("lsn")     # Log sequence number
)

# Route by operation type
df_upserts = df_parsed.filter(col("operation").isin("c", "u", "r"))
df_deletes = df_parsed.filter(col("operation") == "d")

# Apply changes to target
def apply_cdc_batch(batch_df, batch_id):
    from delta.tables import DeltaTable
    if batch_df.isEmpty():
        return
    target = DeltaTable.forName(spark, "silver.customers")
    target.alias("t").merge(
        batch_df.alias("s"), "t.customer_id = s.customer_id"
    ).whenMatchedUpdate(
        condition="s.operation != 'd'",
        set={"customer_name": "s.customer_name", "email": "s.email",
             "updated_at": "current_timestamp()"}
    ).whenMatchedDelete(condition="s.operation = 'd'") \\
     .whenNotMatchedInsertAll() \\
     .execute()

df_parsed.writeStream.foreachBatch(apply_cdc_batch) \\
    .option("checkpointLocation", "/mnt/checkpoints/debezium_customers") \\
    .trigger(processingTime="30 seconds") \\
    .start()`,
  },
  {
    id: 20,
    title: 'Delta Change Data Feed (CDF)',
    desc: 'Read row-level changes from Delta tables using Change Data Feed',
    code: `# Enable CDF on source table
spark.sql("""
    ALTER TABLE silver.orders
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Or create table with CDF enabled
spark.sql("""
    CREATE TABLE silver.orders_cdf (
        order_id BIGINT,
        customer_id STRING,
        amount DECIMAL(10,2),
        status STRING
    ) USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Read changes since a specific version
df_changes = spark.read.format("delta") \\
    .option("readChangeFeed", "true") \\
    .option("startingVersion", 5) \\
    .table("silver.orders_cdf")

# Read changes in a time range
df_changes_time = spark.read.format("delta") \\
    .option("readChangeFeed", "true") \\
    .option("startingTimestamp", "2024-03-01T00:00:00Z") \\
    .option("endingTimestamp", "2024-03-15T23:59:59Z") \\
    .table("silver.orders_cdf")

# CDF columns: _change_type (insert, update_preimage, update_postimage, delete),
#               _commit_version, _commit_timestamp
df_changes.select(
    "_change_type", "_commit_version", "_commit_timestamp",
    "order_id", "customer_id", "amount", "status"
).show()

# Streaming CDF for real-time propagation
df_stream = spark.readStream.format("delta") \\
    .option("readChangeFeed", "true") \\
    .option("startingVersion", "latest") \\
    .table("silver.orders_cdf")

# Propagate changes downstream
df_stream.filter(col("_change_type") != "update_preimage") \\
    .writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/orders_cdf_gold") \\
    .trigger(availableNow=True) \\
    .toTable("gold.orders_latest")`,
  },
  {
    id: 21,
    title: 'DLT apply_changes',
    desc: 'Declarative CDC processing with Delta Live Tables apply_changes API',
    code: `import dlt
from pyspark.sql.functions import col, expr

# Source: raw CDC events
@dlt.table(comment="Raw CDC events from source database")
def bronze_orders_cdc():
    return spark.readStream.format("cloudFiles") \\
        .option("cloudFiles.format", "json") \\
        .option("cloudFiles.schemaLocation", "/mnt/schema/cdc_orders") \\
        .load("/mnt/cdc/orders/")

# Apply CDC changes - DLT handles insert/update/delete automatically
dlt.create_streaming_table("silver_orders")

dlt.apply_changes(
    target="silver_orders",
    source="bronze_orders_cdc",
    keys=["order_id"],
    sequence_by=col("event_timestamp"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    apply_as_truncates=expr("operation = 'TRUNCATE'"),
    except_column_list=["operation", "event_timestamp", "_rescued_data"],
    stored_as_scd_type=1
)

# CDC with SCD Type 2 history
dlt.create_streaming_table("silver_orders_history")

dlt.apply_changes(
    target="silver_orders_history",
    source="bronze_orders_cdc",
    keys=["order_id"],
    sequence_by=col("event_timestamp"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "event_timestamp"],
    stored_as_scd_type=2,
    track_history_column_list=["status", "amount", "shipping_address"]
)

# Multiple CDC targets from same source
@dlt.table
def bronze_multi_cdc():
    return spark.readStream.format("kafka") \\
        .option("subscribe", "cdc.public.*") \\
        .load() \\
        .select(from_json(col("value").cast("string"), cdc_schema).alias("data")) \\
        .select("data.*")`,
  },
  {
    id: 22,
    title: 'Kafka CDC Pipeline',
    desc: 'End-to-end CDC from Kafka with Avro deserialization and exactly-once processing',
    code: `from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient

# Schema Registry client for Avro deserialization
schema_registry_conf = {"url": "http://schema-registry:8081"}
schema_client = SchemaRegistryClient(schema_registry_conf)
schema = schema_client.get_latest_version("customers-value").schema.schema_str

# Read from Kafka with exactly-once semantics
df_kafka = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \\
    .option("subscribe", "cdc.public.customers") \\
    .option("startingOffsets", "earliest") \\
    .option("kafka.group.id", "databricks-cdc-consumer") \\
    .option("failOnDataLoss", "false") \\
    .option("maxOffsetsPerTrigger", "100000") \\
    .load()

# Deserialize Avro payload
df_parsed = df_kafka.select(
    col("key").cast("string").alias("message_key"),
    from_avro(col("value"), schema).alias("payload"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
).select("message_key", "payload.*", "kafka_timestamp", "offset")

# Apply CDC with idempotency (using Kafka offset as sequence)
def apply_kafka_cdc(batch_df, batch_id):
    from delta.tables import DeltaTable
    if batch_df.isEmpty():
        return

    # Deduplicate within micro-batch
    deduped = batch_df.orderBy(col("offset").desc()) \\
        .dropDuplicates(["customer_id"])

    target = DeltaTable.forName(spark, "silver.customers")
    target.alias("t").merge(
        deduped.alias("s"), "t.customer_id = s.customer_id"
    ).whenMatchedUpdateAll() \\
     .whenNotMatchedInsertAll() \\
     .execute()

df_parsed.writeStream \\
    .foreachBatch(apply_kafka_cdc) \\
    .option("checkpointLocation", "/mnt/checkpoints/kafka_cdc_customers") \\
    .trigger(processingTime="1 minute") \\
    .start()`,
  },
  {
    id: 23,
    title: 'Database Trigger CDC',
    desc: 'Capture changes using database audit triggers with shadow tables',
    code: `# Step 1: Source DB setup (PostgreSQL example - run on source DB)
# CREATE TABLE audit.customer_changes (
#     change_id SERIAL PRIMARY KEY,
#     operation CHAR(1),  -- I=Insert, U=Update, D=Delete
#     changed_at TIMESTAMP DEFAULT NOW(),
#     customer_id INT,
#     old_data JSONB,
#     new_data JSONB
# );
#
# CREATE FUNCTION audit.capture_customer_changes() RETURNS TRIGGER AS $$
# BEGIN
#     IF TG_OP = 'INSERT' THEN
#         INSERT INTO audit.customer_changes (operation, customer_id, new_data)
#         VALUES ('I', NEW.customer_id, row_to_json(NEW));
#     ELSIF TG_OP = 'UPDATE' THEN
#         INSERT INTO audit.customer_changes (operation, customer_id, old_data, new_data)
#         VALUES ('U', NEW.customer_id, row_to_json(OLD), row_to_json(NEW));
#     ELSIF TG_OP = 'DELETE' THEN
#         INSERT INTO audit.customer_changes (operation, customer_id, old_data)
#         VALUES ('D', OLD.customer_id, row_to_json(OLD));
#     END IF;
#     RETURN NULL;
# END;
# $$ LANGUAGE plpgsql;

# Step 2: PySpark - Extract trigger-based changes
last_change_id = spark.sql(
    "SELECT COALESCE(MAX(source_change_id), 0) AS max_id FROM bronze.customer_cdc_log"
).collect()[0]["max_id"]

df_changes = spark.read.format("jdbc") \\
    .option("url", "jdbc:postgresql://source:5432/prod") \\
    .option("dbtable", f"(SELECT * FROM audit.customer_changes WHERE change_id > {last_change_id}) sub") \\
    .option("user", dbutils.secrets.get("scope", "pg_user")) \\
    .option("password", dbutils.secrets.get("scope", "pg_pass")) \\
    .load()

# Step 3: Parse and apply
from pyspark.sql.functions import col, from_json, when
from delta.tables import DeltaTable

df_parsed = df_changes.select(
    col("change_id").alias("source_change_id"),
    col("operation"),
    col("customer_id"),
    from_json(col("new_data"), customer_schema).alias("new_data"),
    col("changed_at")
)

# Log changes to bronze
df_parsed.write.format("delta").mode("append").saveAsTable("bronze.customer_cdc_log")

# Apply to target
target = DeltaTable.forName(spark, "silver.customers")
df_upserts = df_parsed.filter(col("operation").isin("I", "U")).select("new_data.*")
target.alias("t").merge(
    df_upserts.alias("s"), "t.customer_id = s.customer_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`,
  },
  {
    id: 24,
    title: 'Log-Based CDC',
    desc: 'Process database transaction logs (WAL/binlog) for real-time replication',
    code: `# Log-based CDC reads database WAL (Write-Ahead Log) or binlog
# Typically delivered via Debezium, AWS DMS, or Fivetran

from pyspark.sql.functions import col, from_json, struct, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Read WAL events (delivered to S3/ADLS by DMS or Debezium)
wal_schema = StructType([
    StructField("table", StringType()),
    StructField("op", StringType()),      # I, U, D
    StructField("lsn", LongType()),       # Log Sequence Number
    StructField("ts_ms", LongType()),     # Event timestamp
    StructField("before", StringType()),  # JSON of old row
    StructField("after", StringType())    # JSON of new row
])

df_wal = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "json") \\
    .schema(wal_schema) \\
    .load("s3://cdc-bucket/wal-events/")

# Route events by table
def process_table_cdc(batch_df, batch_id):
    from delta.tables import DeltaTable

    tables = [row["table"] for row in batch_df.select("table").distinct().collect()]

    for table_name in tables:
        table_events = batch_df.filter(col("table") == table_name) \\
            .orderBy("lsn")  # Apply in order

        # Parse 'after' JSON based on target schema
        target_schema = spark.table(f"silver.{table_name}").schema
        df_parsed = table_events.select(
            col("op"),
            from_json(col("after"), target_schema).alias("data"),
            col("lsn")
        ).select("op", "data.*", "lsn")

        target = DeltaTable.forName(spark, f"silver.{table_name}")
        pk_col = f"{table_name}_id"

        # Upserts
        df_upserts = df_parsed.filter(col("op").isin("I", "U"))
        if df_upserts.count() > 0:
            target.alias("t").merge(
                df_upserts.alias("s"), f"t.{pk_col} = s.{pk_col}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        # Deletes
        df_deletes = df_parsed.filter(col("op") == "D")
        if df_deletes.count() > 0:
            target.alias("t").merge(
                df_deletes.alias("s"), f"t.{pk_col} = s.{pk_col}"
            ).whenMatchedDelete().execute()

df_wal.writeStream.foreachBatch(process_table_cdc) \\
    .option("checkpointLocation", "/mnt/checkpoints/wal_cdc") \\
    .trigger(processingTime="1 minute") \\
    .start()`,
  },
  {
    id: 25,
    title: 'Timestamp-Based CDC',
    desc: 'Incremental extraction using updated_at timestamp watermarks',
    code: `from pyspark.sql.functions import col, max as _max, current_timestamp, lit

# Simple timestamp-based CDC: extract rows modified since last run
# Requires: source table has reliable updated_at / modified_at column

# Step 1: Get high watermark from last successful run
watermark_df = spark.sql("""
    SELECT COALESCE(MAX(watermark_value), '1900-01-01T00:00:00Z') AS last_watermark
    FROM audit.cdc_watermarks
    WHERE table_name = 'customers' AND status = 'SUCCESS'
""")
last_watermark = watermark_df.collect()[0]["last_watermark"]

# Step 2: Extract changed records from source
df_incremental = spark.read.format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", f"""(
        SELECT *, updated_at AS _source_updated_at
        FROM public.customers
        WHERE updated_at > '{last_watermark}'
        ORDER BY updated_at
    ) sub""") \\
    .option("fetchsize", "10000") \\
    .load()

record_count = df_incremental.count()
new_watermark = df_incremental.agg(_max("_source_updated_at")).collect()[0][0]

if record_count > 0:
    # Step 3: Apply changes to target
    from delta.tables import DeltaTable
    target = DeltaTable.forName(spark, "silver.customers")
    target.alias("t").merge(
        df_incremental.alias("s"), "t.customer_id = s.customer_id"
    ).whenMatchedUpdate(
        condition="s._source_updated_at > t._source_updated_at",
        set={"customer_name": "s.customer_name", "email": "s.email",
             "phone": "s.phone", "_source_updated_at": "s._source_updated_at",
             "_etl_updated_at": "current_timestamp()"}
    ).whenNotMatchedInsertAll().execute()

    # Step 4: Update watermark on success
    spark.sql(f"""
        INSERT INTO audit.cdc_watermarks
        VALUES ('customers', '{new_watermark}', {record_count}, 'SUCCESS', current_timestamp())
    """)
else:
    spark.sql(f"""
        INSERT INTO audit.cdc_watermarks
        VALUES ('customers', '{last_watermark}', 0, 'NO_CHANGES', current_timestamp())
    """)`,
  },
  {
    id: 26,
    title: 'Full Diff CDC',
    desc: 'Detect changes by comparing full snapshots when no CDC mechanism exists',
    code: `from pyspark.sql.functions import col, sha2, concat_ws, lit, current_timestamp, coalesce

# Full Diff CDC: compare entire source snapshot to existing target
# Use when source has no updated_at column and no WAL/trigger access

# Step 1: Load current source snapshot
df_source = spark.read.format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", "public.products") \\
    .load() \\
    .withColumn("_row_hash", sha2(concat_ws("||",
        *["product_id", "name", "category", "price", "status"]), 256))

# Step 2: Load current target
df_target = spark.table("silver.products") \\
    .withColumn("_row_hash", sha2(concat_ws("||",
        *["product_id", "name", "category", "price", "status"]), 256))

# Step 3: Detect changes using hash comparison
# New records (in source but not in target)
df_inserts = df_source.alias("s").join(
    df_target.alias("t"), col("s.product_id") == col("t.product_id"), "left_anti"
).withColumn("_change_type", lit("INSERT"))

# Changed records (hash mismatch)
df_updates = df_source.alias("s").join(
    df_target.alias("t"), col("s.product_id") == col("t.product_id"), "inner"
).filter(col("s._row_hash") != col("t._row_hash")) \\
 .select("s.*") \\
 .withColumn("_change_type", lit("UPDATE"))

# Deleted records (in target but not in source)
df_deletes = df_target.alias("t").join(
    df_source.alias("s"), col("t.product_id") == col("s.product_id"), "left_anti"
).select("t.product_id") \\
 .withColumn("_change_type", lit("DELETE"))

print(f"Inserts: {df_inserts.count()}, Updates: {df_updates.count()}, Deletes: {df_deletes.count()}")

# Step 4: Apply all changes
from delta.tables import DeltaTable
df_upserts = df_inserts.drop("_change_type").unionByName(df_updates.drop("_change_type"))

target = DeltaTable.forName(spark, "silver.products")
target.alias("t").merge(
    df_upserts.alias("s"), "t.product_id = s.product_id"
).whenMatchedUpdateAll() \\
 .whenNotMatchedInsertAll() \\
 .execute()

# Soft-delete removed records
if df_deletes.count() > 0:
    target.alias("t").merge(
        df_deletes.alias("s"), "t.product_id = s.product_id"
    ).whenMatchedUpdate(set={
        "is_active": lit(False),
        "_deleted_at": current_timestamp()
    }).execute()`,
  },
];

// ─── Tab 4: DataFrame Operations (12 scenarios) ────────────────────────────────
const dataFrameScenarios = [
  {
    id: 27,
    title: 'Select and Projection',
    desc: 'Column selection, aliasing, computed columns, and nested field access',
    code: `from pyspark.sql.functions import col, lit, concat, upper, round as _round, when, struct

df = spark.table("silver.orders")

# Basic select
df.select("order_id", "customer_name", "amount").show()

# With aliases and computed columns
df.select(
    col("order_id"),
    upper(col("customer_name")).alias("name_upper"),
    _round(col("amount") * 1.1, 2).alias("amount_with_tax"),
    concat(col("city"), lit(", "), col("state")).alias("location"),
    when(col("amount") > 1000, "premium")
        .when(col("amount") > 100, "standard")
        .otherwise("basic").alias("tier")
).show()

# Nested struct access
df.select(
    col("order_id"),
    col("address.street").alias("street"),
    col("address.city").alias("city"),
    col("address.zip").alias("zip_code")
).show()

# Select with expression
df.selectExpr(
    "order_id",
    "amount * 1.1 AS amount_with_tax",
    "CASE WHEN status = 'shipped' THEN true ELSE false END AS is_shipped",
    "DATE_FORMAT(order_date, 'yyyy-MM') AS order_month"
).show()

# Drop columns
df.drop("_rescued_data", "_etl_timestamp", "raw_payload").show()`,
  },
  {
    id: 28,
    title: 'Filter and Where',
    desc: 'Row filtering with conditions, null handling, regex, and IN clauses',
    code: `from pyspark.sql.functions import col, array_contains, year, length

df = spark.table("silver.orders")

# Basic filter
df.filter(col("amount") > 1000).show()
df.where("status = 'active' AND amount BETWEEN 100 AND 5000").show()

# Multiple conditions
df.filter(
    (col("status") == "active") &
    (col("amount") > 100) &
    (col("region").isin("US", "EU", "APAC")) &
    (col("order_date") >= "2024-01-01")
).show()

# NULL handling
df.filter(col("email").isNotNull()).show()
df.filter(col("phone").isNull() | (col("phone") == "")).show()

# Pattern matching
df.filter(col("email").rlike(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z]{2,}$")).show()
df.filter(col("customer_name").like("%Smith%")).show()
df.filter(col("product_name").contains("Pro")).show()
df.filter(col("sku").startswith("SKU-")).show()

# Negative filter
df.filter(~col("status").isin("cancelled", "returned", "failed")).show()

# Complex nested filter
df.filter(
    (year(col("order_date")) == 2024) &
    (length(col("customer_name")) > 3) &
    (col("amount").between(50, 10000))
).show()

# Array column filter
df.filter(array_contains(col("tags"), "urgent")).show()`,
  },
  {
    id: 29,
    title: 'GroupBy Aggregations',
    desc: 'Grouping with multiple aggregation functions, rollup, and cube',
    code: `from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max,
    countDistinct, collect_list, collect_set, first, stddev,
    percentile_approx, round as _round
)

df = spark.table("silver.orders")

# Basic aggregation
df.groupBy("region").agg(
    count("*").alias("order_count"),
    _sum("amount").alias("total_amount"),
    _round(avg("amount"), 2).alias("avg_amount"),
    _min("amount").alias("min_amount"),
    _max("amount").alias("max_amount"),
    countDistinct("customer_id").alias("unique_customers"),
    _round(stddev("amount"), 2).alias("amount_stddev")
).orderBy(col("total_amount").desc()).show()

# Multiple groupBy columns
df.groupBy("region", "product_category").agg(
    count("*").alias("orders"),
    _sum("amount").alias("revenue"),
    _round(avg("amount"), 2).alias("avg_order_value")
).show()

# Collect aggregations
df.groupBy("customer_id").agg(
    first("customer_name").alias("name"),
    count("*").alias("total_orders"),
    collect_list("product_name").alias("products_ordered"),
    collect_set("region").alias("regions")
).show(truncate=False)

# Percentile aggregation
df.groupBy("region").agg(
    percentile_approx("amount", 0.5).alias("median_amount"),
    percentile_approx("amount", [0.25, 0.5, 0.75]).alias("quartiles")
).show()

# ROLLUP (subtotals)
df.rollup("region", "product_category").agg(
    _sum("amount").alias("total")
).orderBy("region", "product_category").show()

# CUBE (all dimension combinations)
df.cube("region", "product_category").agg(
    _sum("amount").alias("total"),
    count("*").alias("count")
).show()`,
  },
  {
    id: 30,
    title: 'Join Operations',
    desc: 'All join types: inner, left, right, full, cross, semi, anti with examples',
    code: `from pyspark.sql.functions import col, coalesce

orders = spark.table("silver.orders")
customers = spark.table("silver.customers")
products = spark.table("silver.products")

# INNER JOIN - only matching rows
df_inner = orders.join(customers, orders.customer_id == customers.customer_id, "inner") \\
    .select(orders["*"], customers["customer_name"], customers["email"])

# LEFT JOIN - all from left, matching from right
df_left = orders.join(customers, "customer_id", "left") \\
    .withColumn("customer_name", coalesce(col("customer_name"), col("unknown")))

# RIGHT JOIN
df_right = customers.join(orders, "customer_id", "right")

# FULL OUTER JOIN
df_full = orders.join(customers, "customer_id", "full")

# CROSS JOIN (cartesian product - use carefully!)
regions = spark.createDataFrame([("US",), ("EU",), ("APAC",)], ["region"])
quarters = spark.createDataFrame([("Q1",), ("Q2",), ("Q3",), ("Q4",)], ["quarter"])
df_cross = regions.crossJoin(quarters)  # 12 rows

# LEFT SEMI JOIN - rows from left that have match in right (like IN subquery)
df_semi = orders.join(
    customers.filter(col("loyalty_tier") == "gold"),
    "customer_id", "left_semi"
)
# Equivalent SQL: SELECT * FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE loyalty_tier = 'gold')

# LEFT ANTI JOIN - rows from left with NO match in right (like NOT IN)
df_anti = orders.join(customers, "customer_id", "left_anti")
# Orders from customers not in the customers table

# Multi-table join
df_enriched = (
    orders.alias("o")
    .join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left")
    .join(products.alias("p"), col("o.product_id") == col("p.product_id"), "left")
    .select(
        col("o.order_id"), col("o.amount"),
        col("c.customer_name"), col("c.email"),
        col("p.product_name"), col("p.category")
    )
)

# Join with broadcast hint for small tables
from pyspark.sql.functions import broadcast
df_broadcast = orders.join(broadcast(products), "product_id", "inner")`,
  },
  {
    id: 31,
    title: 'Window Functions',
    desc: 'Ranking, running totals, lag/lead, and moving averages with window specs',
    code: `from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, ntile,
    lag, lead, sum as _sum, avg, count,
    first, last, round as _round
)
from pyspark.sql.window import Window

df = spark.table("silver.orders")

# Ranking functions
window_by_region = Window.partitionBy("region").orderBy(col("amount").desc())

df_ranked = df.select(
    "*",
    row_number().over(window_by_region).alias("row_num"),
    rank().over(window_by_region).alias("rank"),
    dense_rank().over(window_by_region).alias("dense_rank"),
    ntile(4).over(window_by_region).alias("quartile")
)

# Top N per group
df_top3 = df_ranked.filter(col("row_num") <= 3)

# Running totals
window_running = Window.partitionBy("customer_id") \\
    .orderBy("order_date") \\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.select(
    "*",
    _sum("amount").over(window_running).alias("cumulative_amount"),
    count("*").over(window_running).alias("order_sequence"),
    avg("amount").over(window_running).alias("running_avg")
).show()

# Lag / Lead (previous/next row values)
window_ordered = Window.partitionBy("customer_id").orderBy("order_date")
df.select(
    "*",
    lag("amount", 1).over(window_ordered).alias("prev_order_amount"),
    lead("amount", 1).over(window_ordered).alias("next_order_amount"),
    (col("amount") - lag("amount", 1).over(window_ordered)).alias("amount_change")
).show()

# Moving average (last 7 days)
window_7day = Window.partitionBy("product_id") \\
    .orderBy(col("order_date").cast("long")) \\
    .rangeBetween(-7 * 86400, 0)

df.select(
    "*",
    _round(avg("amount").over(window_7day), 2).alias("7day_moving_avg")
).show()`,
  },
  {
    id: 32,
    title: 'Pivot and Unpivot',
    desc: 'Reshape data: rows to columns (pivot) and columns to rows (unpivot)',
    code: `from pyspark.sql.functions import sum as _sum, col, expr, round as _round

df = spark.table("silver.orders")

# PIVOT: rows to columns
# Revenue by region per quarter
df_pivot = df.groupBy("region").pivot("quarter", ["Q1", "Q2", "Q3", "Q4"]).agg(
    _round(_sum("amount"), 2)
)
df_pivot.show()
# Result: region | Q1 | Q2 | Q3 | Q4

# Pivot with multiple aggregations
df_pivot_multi = df.groupBy("region").pivot("quarter").agg(
    _round(_sum("amount"), 2).alias("revenue"),
    count("*").alias("orders")
)
# Result: region | Q1_revenue | Q1_orders | Q2_revenue | Q2_orders | ...

# UNPIVOT: columns to rows (Spark 3.4+)
df_metrics = spark.createDataFrame([
    ("product_a", 100, 85, 92),
    ("product_b", 200, 150, 180),
], ["product", "jan_sales", "feb_sales", "mar_sales"])

# Using stack() for unpivot
df_unpivot = df_metrics.select(
    "product",
    expr("""
        stack(3,
            'jan', jan_sales,
            'feb', feb_sales,
            'mar', mar_sales
        ) AS (month, sales)
    """)
)
df_unpivot.show()
# Result: product | month | sales

# Spark 3.4+ native UNPIVOT
df_unpivot_native = df_metrics.unpivot(
    ids=["product"],
    values=["jan_sales", "feb_sales", "mar_sales"],
    variableColumnName="month",
    valueColumnName="sales"
)
df_unpivot_native.show()`,
  },
  {
    id: 33,
    title: 'User Defined Functions (UDF)',
    desc: 'Python UDFs, Pandas UDFs, and SQL UDFs for custom transformations',
    code: `from pyspark.sql.functions import udf, pandas_udf, col
from pyspark.sql.types import StringType, DoubleType, ArrayType
import pandas as pd

# Standard Python UDF (row-at-a-time, slower)
@udf(returnType=StringType())
def normalize_phone(phone):
    if phone is None:
        return None
    digits = ''.join(c for c in phone if c.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return phone

df = df.withColumn("phone_formatted", normalize_phone(col("phone")))

# Pandas UDF (vectorized, much faster)
@pandas_udf(DoubleType())
def haversine_distance(lat1: pd.Series, lon1: pd.Series,
                        lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    import numpy as np
    R = 6371  # Earth radius in km
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * \\
        np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
    return pd.Series(R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a)))

df = df.withColumn("distance_km",
    haversine_distance("store_lat", "store_lon", "customer_lat", "customer_lon"))

# Pandas UDF for grouped operations
@pandas_udf("customer_id string, total double, avg_amount double, count long")
def customer_summary(pdf: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id": [pdf["customer_id"].iloc[0]],
        "total": [pdf["amount"].sum()],
        "avg_amount": [pdf["amount"].mean()],
        "count": [len(pdf)]
    })

df.groupBy("customer_id").applyInPandas(customer_summary, schema=result_schema).show()

# SQL UDF registration
spark.udf.register("normalize_phone_sql", normalize_phone)
spark.sql("SELECT normalize_phone_sql(phone) FROM silver.customers").show()`,
  },
  {
    id: 34,
    title: 'Complex Types',
    desc: 'Work with arrays, maps, structs, and nested data structures',
    code: `from pyspark.sql.functions import (
    col, array, struct, map_from_arrays, create_map,
    explode, explode_outer, posexplode, flatten,
    array_contains, array_distinct, array_sort, array_union,
    element_at, size, map_keys, map_values, transform,
    from_json, to_json, schema_of_json
)

# ARRAYS
df = spark.createDataFrame([
    (1, ["apple", "banana", "cherry"]),
    (2, ["banana", "date"]),
], ["id", "fruits"])

df.select(
    "id",
    size("fruits").alias("count"),
    element_at("fruits", 1).alias("first_fruit"),
    array_contains("fruits", "banana").alias("has_banana"),
    array_sort("fruits").alias("sorted"),
    array_distinct("fruits").alias("unique")
).show()

# Explode array to rows
df.select("id", explode("fruits").alias("fruit")).show()

# Transform array elements
df.select("id", transform("fruits", lambda x: upper(x)).alias("upper_fruits")).show()

# MAPS
df_map = spark.createDataFrame([
    (1, {"color": "red", "size": "L", "brand": "Nike"}),
    (2, {"color": "blue", "size": "M"}),
], ["id", "attributes"])

df_map.select(
    "id",
    col("attributes")["color"].alias("color"),
    map_keys("attributes").alias("keys"),
    map_values("attributes").alias("values"),
    size("attributes").alias("attr_count")
).show()

# STRUCTS
df_nested = spark.createDataFrame([
    (1, ("John", "Doe", ("NYC", "NY", "10001"))),
], ["id", "name"])  # name is struct<first, last, address<city, state, zip>>

df_nested.select(
    "id",
    col("name.first").alias("first_name"),
    col("name.address.city").alias("city")
).show()

# Parse JSON strings to structs
df_json = spark.createDataFrame([
    (1, '{"name": "John", "scores": [85, 92, 78]}'),
], ["id", "json_data"])

json_schema = schema_of_json('{"name": "str", "scores": [0]}')
df_json.select("id", from_json("json_data", json_schema).alias("parsed")).show()`,
  },
  {
    id: 35,
    title: 'Caching and Persistence',
    desc: 'Cache DataFrames in memory, disk, or off-heap for repeated access',
    code: `from pyspark.storagelevel import StorageLevel

df = spark.table("silver.large_dataset")

# Cache in memory (deserialized)
df.cache()  # Same as df.persist(StorageLevel.MEMORY_ONLY)

# Force materialization (cache is lazy)
df.count()

# Check if cached
print(f"Is cached: {df.is_cached}")
print(f"Storage level: {df.storageLevel}")

# Different persistence levels
df.persist(StorageLevel.MEMORY_ONLY)           # Memory only, recompute if evicted
df.persist(StorageLevel.MEMORY_AND_DISK)       # Spill to disk if memory full
df.persist(StorageLevel.DISK_ONLY)             # Disk only (for very large datasets)
df.persist(StorageLevel.MEMORY_ONLY_SER)       # Serialized (less memory, more CPU)
df.persist(StorageLevel.OFF_HEAP)              # Off-heap memory (Tungsten)

# Use cached DataFrame multiple times
df_filtered = df.filter(col("region") == "US")
agg1 = df_filtered.groupBy("category").count()
agg2 = df_filtered.groupBy("month").agg(sum("amount"))

# Unpersist when done
df.unpersist()

# Delta Cache (Databricks-specific, automatic)
# Automatically caches remote Parquet files on local SSD
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")

# Temporary view for SQL access to cached data
df.createOrReplaceTempView("cached_orders")
spark.sql("CACHE TABLE cached_orders")
spark.sql("SELECT region, COUNT(*) FROM cached_orders GROUP BY region").show()
spark.sql("UNCACHE TABLE cached_orders")`,
  },
  {
    id: 36,
    title: 'Repartition and Coalesce',
    desc: 'Control data distribution, partition count, and shuffle behavior',
    code: `from pyspark.sql.functions import col, spark_partition_id

df = spark.table("silver.orders")

# Check current partitions
print(f"Current partitions: {df.rdd.getNumPartitions()}")
df.groupBy(spark_partition_id().alias("partition")).count().show()

# REPARTITION: increase or decrease partitions (full shuffle)
df_repartitioned = df.repartition(200)  # Set exact partition count

# Repartition by column (hash partitioning - good for joins/aggregations)
df_by_region = df.repartition("region")
df_by_date = df.repartition(100, "order_date")
df_by_multi = df.repartition("region", "product_category")

# Range partitioning (good for sorted output)
df_range = df.repartitionByRange(50, col("order_date"))

# COALESCE: reduce partitions WITHOUT full shuffle (more efficient)
df_coalesced = df.coalesce(10)  # Reduce to 10 partitions

# Best practices for writing
# Write with optimized partition count
df.repartition(1).write.format("delta").mode("overwrite") \\
    .saveAsTable("gold.small_report")  # Single file output

# Partition by column for large tables
df.repartition("year", "month") \\
    .write.format("delta") \\
    .partitionBy("year", "month") \\
    .mode("overwrite") \\
    .saveAsTable("silver.orders_partitioned")

# Auto-optimize (Databricks)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# AQE (Adaptive Query Execution) - auto-coalesce shuffle partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")`,
  },
  {
    id: 37,
    title: 'Broadcast Join',
    desc: 'Optimize joins by broadcasting small tables to all executors',
    code: `from pyspark.sql.functions import broadcast, col

# Large fact table
orders = spark.table("silver.orders")       # 100M+ rows
# Small dimension tables
products = spark.table("silver.products")   # 10K rows
regions = spark.table("silver.regions")     # 50 rows
currencies = spark.table("silver.currencies")  # 200 rows

# Explicit broadcast hint (recommended for clarity)
df_enriched = orders \\
    .join(broadcast(products), "product_id", "inner") \\
    .join(broadcast(regions), "region_code", "inner") \\
    .join(broadcast(currencies), "currency_code", "inner")

# SQL broadcast hint
spark.sql("""
    SELECT /*+ BROADCAST(p), BROADCAST(r) */
        o.order_id, o.amount, p.product_name, r.region_name
    FROM silver.orders o
    JOIN silver.products p ON o.product_id = p.product_id
    JOIN silver.regions r ON o.region_code = r.region_code
""")

# Configure auto-broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")  # Default 10MB

# Disable auto-broadcast (useful when stats are incorrect)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Check execution plan for broadcast
df_enriched.explain(True)
# Look for "BroadcastHashJoin" in the plan

# Broadcast variable for lookup (non-join use case)
lookup_dict = {row["code"]: row["name"]
               for row in regions.collect()}
broadcast_lookup = spark.sparkContext.broadcast(lookup_dict)

@udf(returnType=StringType())
def resolve_region(code):
    return broadcast_lookup.value.get(code, "Unknown")

orders.withColumn("region_name", resolve_region("region_code")).show()`,
  },
  {
    id: 38,
    title: 'DataFrame API Advanced',
    desc: 'withColumn, drop, distinct, sample, limit, union, intersect, describe, and schema ops',
    code: `from pyspark.sql.functions import (
    col, lit, when, coalesce, monotonically_increasing_id,
    current_timestamp, date_format, regexp_replace, trim
)

df = spark.table("silver.orders")

# Add / modify columns
df2 = (df
    .withColumn("surrogate_key", monotonically_increasing_id())
    .withColumn("amount_usd", col("amount") * col("exchange_rate"))
    .withColumn("status_clean", trim(upper(col("status"))))
    .withColumn("email_domain", regexp_replace(col("email"), ".*@", ""))
    .withColumn("processed_at", current_timestamp())
    .withColumn("fiscal_year",
        when(col("order_month") >= 4, col("order_year"))
        .otherwise(col("order_year") - 1))
    .withColumnRenamed("customer_id", "cust_id")
)

# Drop columns
df_clean = df.drop("_rescued_data", "_etl_temp", "raw_json")

# Distinct / deduplicate
df.distinct().show()
df.dropDuplicates(["customer_id", "order_date"]).show()

# Sampling
df_sample = df.sample(fraction=0.1, seed=42)       # 10% sample
df_stratified = df.sampleBy("region", {"US": 0.1, "EU": 0.2}, seed=42)

# Limit
df.limit(1000).show()
df.tail(5)

# Set operations
df1 = spark.table("silver.orders_2023")
df2 = spark.table("silver.orders_2024")

df_union = df1.unionByName(df2, allowMissingColumns=True)
df_intersect = df1.intersect(df2)          # Common rows
df_except = df1.exceptAll(df2)             # In df1 but not df2

# Schema operations
df.printSchema()
df.dtypes                    # List of (column, type) tuples
df.columns                   # List of column names
df.schema.json()             # JSON schema string
df.describe("amount", "quantity").show()  # Summary statistics
df.summary("count", "min", "25%", "50%", "75%", "max").show()`,
  },
];

// ─── Tab 5: Delta Table Operations (10 scenarios) ──────────────────────────────
const deltaTableScenarios = [
  {
    id: 39,
    title: 'Create Delta Tables',
    desc: 'Create managed and external Delta tables with properties, constraints, and partitioning',
    code: `# Managed table (Databricks manages storage)
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.customers (
        customer_id STRING NOT NULL,
        customer_name STRING,
        email STRING,
        phone STRING,
        city STRING,
        state STRING,
        created_at TIMESTAMP DEFAULT current_timestamp(),
        updated_at TIMESTAMP,
        CONSTRAINT pk_customer PRIMARY KEY (customer_id)
    ) USING DELTA
    COMMENT 'Silver layer customer dimension'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.enableChangeDataFeed' = 'true',
        'delta.logRetentionDuration' = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days',
        'delta.columnMapping.mode' = 'name'
    )
""")

# External table (you manage storage location)
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.raw_events (
        event_id STRING,
        event_type STRING,
        payload STRING,
        event_timestamp TIMESTAMP
    ) USING DELTA
    LOCATION 's3://data-lake/bronze/raw_events/'
    PARTITIONED BY (event_date DATE)
""")

# Create table from DataFrame
df.write.format("delta") \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .partitionBy("year", "month") \\
    .saveAsTable("silver.orders")

# Create table using DeltaTableBuilder
from delta.tables import DeltaTable
DeltaTable.createIfNotExists(spark) \\
    .tableName("silver.products") \\
    .addColumn("product_id", "STRING", comment="Primary key") \\
    .addColumn("name", "STRING") \\
    .addColumn("price", "DECIMAL(10,2)") \\
    .addColumn("category", "STRING") \\
    .addColumn("_loaded_at", "TIMESTAMP") \\
    .property("delta.enableChangeDataFeed", "true") \\
    .execute()

# Add CHECK constraints
spark.sql("""
    ALTER TABLE silver.orders ADD CONSTRAINT valid_amount CHECK (amount > 0);
    ALTER TABLE silver.orders ADD CONSTRAINT valid_status
        CHECK (status IN ('pending', 'active', 'shipped', 'delivered', 'cancelled'));
""")`,
  },
  {
    id: 40,
    title: 'MERGE (Upsert)',
    desc: 'Delta MERGE for insert, update, delete in a single atomic operation',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col

# Basic upsert
target = DeltaTable.forName(spark, "silver.customers")
target.alias("t").merge(
    df_updates.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdateAll() \\
 .whenNotMatchedInsertAll() \\
 .execute()

# MERGE with specific column mappings and conditions
target.alias("t").merge(
    df_source.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    condition="s.updated_at > t.updated_at",
    set={
        "customer_name": "s.customer_name",
        "email": "s.email",
        "phone": "s.phone",
        "updated_at": "s.updated_at",
        "_etl_updated_at": "current_timestamp()"
    }
).whenNotMatchedInsert(
    condition="s.is_active = true",
    values={
        "customer_id": "s.customer_id",
        "customer_name": "s.customer_name",
        "email": "s.email",
        "phone": "s.phone",
        "updated_at": "s.updated_at",
        "_etl_loaded_at": "current_timestamp()",
        "_etl_updated_at": "current_timestamp()"
    }
).whenNotMatchedBySourceDelete(
    condition="t.updated_at < current_timestamp() - INTERVAL 90 DAYS"
).execute()

# SQL MERGE
spark.sql("""
    MERGE INTO silver.inventory t
    USING staging.inventory_updates s
    ON t.sku = s.sku AND t.warehouse_id = s.warehouse_id
    WHEN MATCHED AND s.quantity = 0 THEN DELETE
    WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity, t.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT *
""")`,
  },
  {
    id: 41,
    title: 'Update and Delete',
    desc: 'Conditional row updates and deletes with Delta table operations',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, when, expr

# UPDATE with Python API
dt = DeltaTable.forName(spark, "silver.customers")

# Simple update
dt.update(
    condition=col("city") == "New York",
    set={"state": lit("NY"), "updated_at": current_timestamp()}
)

# Complex conditional update
dt.update(
    condition=(col("loyalty_points") > 10000) & (col("tier") != "platinum"),
    set={
        "tier": lit("platinum"),
        "tier_changed_at": current_timestamp(),
        "discount_pct": lit(0.15)
    }
)

# SQL UPDATE
spark.sql("""
    UPDATE silver.orders
    SET status = 'cancelled',
        cancelled_at = current_timestamp(),
        cancel_reason = 'customer_request'
    WHERE status = 'pending'
    AND order_date < current_date() - INTERVAL 30 DAYS
""")

# DELETE with Python API
dt.delete(condition=col("is_test_account") == True)

# DELETE with complex condition
dt.delete(
    condition=(col("status") == "inactive") &
              (col("last_login_date") < "2023-01-01") &
              (col("total_orders") == 0)
)

# SQL DELETE
spark.sql("""
    DELETE FROM silver.audit_log
    WHERE created_at < current_timestamp() - INTERVAL 90 DAYS
""")

# Soft delete pattern (preferred over hard delete)
dt.update(
    condition=col("customer_id").isin(["C001", "C002", "C003"]),
    set={"is_deleted": lit(True), "deleted_at": current_timestamp()}
)`,
  },
  {
    id: 42,
    title: 'Time Travel',
    desc: 'Query historical versions, restore to previous state, and audit changes',
    code: `# Query by version number
df_v0 = spark.read.format("delta") \\
    .option("versionAsOf", 0) \\
    .table("silver.customers")

df_v5 = spark.read.format("delta") \\
    .option("versionAsOf", 5) \\
    .table("silver.customers")

# Query by timestamp
df_yesterday = spark.read.format("delta") \\
    .option("timestampAsOf", "2024-03-14T10:00:00Z") \\
    .table("silver.customers")

# SQL time travel
spark.sql("SELECT * FROM silver.customers VERSION AS OF 5")
spark.sql("SELECT * FROM silver.customers TIMESTAMP AS OF '2024-03-14'")

# View table history
spark.sql("DESCRIBE HISTORY silver.customers").show(truncate=False)
# Shows: version, timestamp, operation, operationParameters, userMetrics

from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "silver.customers")
dt.history().show(truncate=False)
dt.history(10).select("version", "timestamp", "operation", "operationMetrics").show()

# Compare versions (diff between two points in time)
df_old = spark.read.option("versionAsOf", 3).table("silver.customers")
df_new = spark.read.option("versionAsOf", 7).table("silver.customers")

df_added = df_new.exceptAll(df_old)    # New/modified rows
df_removed = df_old.exceptAll(df_new)  # Deleted/changed rows

print(f"Rows added/changed: {df_added.count()}")
print(f"Rows removed/changed: {df_removed.count()}")

# RESTORE table to previous version
spark.sql("RESTORE TABLE silver.customers TO VERSION AS OF 5")
spark.sql("RESTORE TABLE silver.customers TO TIMESTAMP AS OF '2024-03-14'")`,
  },
  {
    id: 43,
    title: 'Vacuum and Optimize',
    desc: 'Clean up old files, compact small files, and optimize table layout',
    code: `from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "silver.orders")

# VACUUM: remove old files no longer referenced by Delta log
# Default retention: 7 days (168 hours)
dt.vacuum()             # Uses default retention
dt.vacuum(168)          # Explicit: 168 hours = 7 days

# SQL VACUUM
spark.sql("VACUUM silver.orders RETAIN 168 HOURS")

# DRY RUN - see which files would be deleted
spark.sql("VACUUM silver.orders RETAIN 168 HOURS DRY RUN")

# WARNING: Reducing retention below 7 days requires this setting:
# spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
# Only do this if you're sure no long-running queries need old files

# OPTIMIZE: compact small files into larger ones
spark.sql("OPTIMIZE silver.orders")

# OPTIMIZE with predicate (only compact specific partitions)
spark.sql("OPTIMIZE silver.orders WHERE order_date >= '2024-03-01'")

# Python API
dt.optimize().executeCompaction()
dt.optimize().where("order_date >= '2024-03-01'").executeCompaction()

# Z-ORDER: co-locate related data for faster point lookups
spark.sql("OPTIMIZE silver.orders ZORDER BY (customer_id, order_date)")

# Z-ORDER with partition predicate
spark.sql("""
    OPTIMIZE silver.orders
    WHERE year = 2024 AND month = 3
    ZORDER BY (customer_id)
""")

dt.optimize().where("year = 2024").executeZOrderBy("customer_id", "order_date")

# Auto-optimize settings (Databricks)
spark.sql("""
    ALTER TABLE silver.orders SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")`,
  },
  {
    id: 44,
    title: 'Z-Order and Liquid Clustering',
    desc: 'Advanced data layout optimization with Z-Order indexing and Liquid Clustering',
    code: `# Z-ORDER: Multi-dimensional data skipping
# Best for: columns frequently used in WHERE filters
# Limit: works well with up to 4 columns

spark.sql("""
    OPTIMIZE silver.orders
    ZORDER BY (customer_id, order_date, product_id)
""")

# Z-ORDER optimization tips:
# 1. Choose high-cardinality columns used in filters
# 2. Put most-filtered column first
# 3. Run after bulk loads, not after every micro-batch
# 4. Combine with partitioning (partition on low-cardinality, z-order on high)

# ─── LIQUID CLUSTERING (Databricks, Delta 3.0+) ─────────
# Replaces partitioning + Z-ORDER with automatic, incremental clustering

# Create table with liquid clustering
spark.sql("""
    CREATE TABLE silver.orders_clustered (
        order_id BIGINT,
        customer_id STRING,
        order_date DATE,
        product_id STRING,
        amount DECIMAL(10,2),
        region STRING
    ) USING DELTA
    CLUSTER BY (customer_id, order_date)
""")

# Enable on existing table
spark.sql("""
    ALTER TABLE silver.orders
    CLUSTER BY (customer_id, order_date)
""")

# Trigger clustering (runs incrementally - only new/unclustered files)
spark.sql("OPTIMIZE silver.orders_clustered")

# Change clustering columns without rewriting data
spark.sql("ALTER TABLE silver.orders_clustered CLUSTER BY (region, order_date)")

# Remove clustering
spark.sql("ALTER TABLE silver.orders_clustered CLUSTER BY NONE")

# Liquid clustering advantages over Z-ORDER:
# - Incremental: only processes new data
# - No need to specify partition columns
# - Can change clustering columns without rewriting
# - Works with column mapping and schema evolution
# - Supports row-level concurrency`,
  },
  {
    id: 45,
    title: 'Clone and Restore',
    desc: 'Deep and shallow clones for backups, testing, and disaster recovery',
    code: `# DEEP CLONE: full copy of data + metadata (independent copy)
spark.sql("""
    CREATE TABLE silver.customers_backup
    DEEP CLONE silver.customers
""")

# Deep clone at specific version
spark.sql("""
    CREATE TABLE silver.customers_backup_v5
    DEEP CLONE silver.customers VERSION AS OF 5
""")

# Deep clone at specific timestamp
spark.sql("""
    CREATE OR REPLACE TABLE silver.customers_backup
    DEEP CLONE silver.customers TIMESTAMP AS OF '2024-03-14'
""")

# SHALLOW CLONE: metadata copy, references source data files (fast, space-efficient)
# Great for testing/development environments
spark.sql("""
    CREATE TABLE dev.customers_test
    SHALLOW CLONE silver.customers
""")

# Shallow clone notes:
# - Source files are referenced, not copied
# - Writes to clone create new files
# - If source is vacuumed, shallow clone may break
# - Use deep clone for production backups

# Python API
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "silver.customers")

# Clone to new location
dt.clone("s3://backup-bucket/customers_clone/", isShallow=False)

# RESTORE: revert table to previous version
spark.sql("RESTORE TABLE silver.customers TO VERSION AS OF 10")
spark.sql("RESTORE TABLE silver.customers TO TIMESTAMP AS OF '2024-03-14T08:00:00Z'")

# Restore workflow: backup before risky operation
spark.sql("CREATE OR REPLACE TABLE silver.customers_pre_migration DEEP CLONE silver.customers")
try:
    run_migration()
except Exception as e:
    # Rollback
    spark.sql("""
        CREATE OR REPLACE TABLE silver.customers
        DEEP CLONE silver.customers_pre_migration
    """)
    raise`,
  },
  {
    id: 46,
    title: 'Change Data Feed (CDF)',
    desc: 'Enable and consume row-level change tracking on Delta tables',
    code: `# Enable CDF on table creation
spark.sql("""
    CREATE TABLE silver.products (
        product_id STRING,
        name STRING,
        price DECIMAL(10,2),
        category STRING
    ) USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Enable CDF on existing table
spark.sql("ALTER TABLE silver.products SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Make some changes
spark.sql("INSERT INTO silver.products VALUES ('P001', 'Widget', 9.99, 'gadgets')")
spark.sql("UPDATE silver.products SET price = 12.99 WHERE product_id = 'P001'")
spark.sql("DELETE FROM silver.products WHERE product_id = 'P001'")

# Read change feed (batch)
df_changes = spark.read.format("delta") \\
    .option("readChangeFeed", "true") \\
    .option("startingVersion", 1) \\
    .table("silver.products")

df_changes.show()
# Columns: product_id, name, price, category,
#           _change_type, _commit_version, _commit_timestamp
# _change_type values: insert, update_preimage, update_postimage, delete

# Read changes in a version range
df_range = spark.read.format("delta") \\
    .option("readChangeFeed", "true") \\
    .option("startingVersion", 2) \\
    .option("endingVersion", 5) \\
    .table("silver.products")

# Streaming CDF (real-time change propagation)
df_cdf_stream = spark.readStream.format("delta") \\
    .option("readChangeFeed", "true") \\
    .option("startingVersion", "latest") \\
    .table("silver.products")

# Propagate to downstream gold table
def apply_product_changes(batch_df, batch_id):
    # Filter out preimages (only care about final state)
    df_latest = batch_df.filter(col("_change_type") != "update_preimage")
    from delta.tables import DeltaTable
    target = DeltaTable.forName(spark, "gold.product_catalog")
    target.alias("t").merge(
        df_latest.filter(col("_change_type") != "delete").alias("s"),
        "t.product_id = s.product_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

df_cdf_stream.writeStream.foreachBatch(apply_product_changes) \\
    .option("checkpointLocation", "/mnt/checkpoints/product_cdf") \\
    .trigger(availableNow=True) \\
    .start()`,
  },
  {
    id: 47,
    title: 'Convert to Delta',
    desc: 'Convert Parquet/Iceberg tables to Delta format and migrate existing data',
    code: `from delta.tables import DeltaTable

# Convert Parquet table to Delta (in-place, no data copy)
DeltaTable.convertToDelta(spark, "parquet.\`s3://data-lake/warehouse/orders\`")

# Convert partitioned Parquet table
DeltaTable.convertToDelta(
    spark,
    "parquet.\`s3://data-lake/warehouse/orders_partitioned\`",
    "year INT, month INT"  # Partition schema
)

# SQL conversion
spark.sql("""
    CONVERT TO DELTA parquet.\`s3://data-lake/warehouse/events\`
    PARTITIONED BY (event_date DATE)
""")

# Convert Iceberg table to Delta (Databricks)
spark.sql("""
    CONVERT TO DELTA iceberg.\`s3://iceberg-warehouse/db/orders\`
""")

# Convert CSV/JSON to Delta (requires reading and writing)
df = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .load("s3://legacy-data/customers.csv")

df.write.format("delta") \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .saveAsTable("bronze.customers_migrated")

# Migrate Hive managed table to Delta
spark.sql("CONVERT TO DELTA default.legacy_hive_table")

# Verify conversion
dt = DeltaTable.forName(spark, "bronze.customers_migrated")
print(f"Format: {dt.detail().select('format').collect()[0][0]}")
print(f"Files: {dt.detail().select('numFiles').collect()[0][0]}")
dt.history().show(truncate=False)`,
  },
  {
    id: 48,
    title: 'Table Properties and Configuration',
    desc: 'Set Delta table properties for auto-optimize, retention, column mapping, and more',
    code: `# View current table properties
spark.sql("SHOW TBLPROPERTIES silver.orders").show(truncate=False)
spark.sql("DESCRIBE DETAIL silver.orders").show(truncate=False)
spark.sql("DESCRIBE EXTENDED silver.orders").show(truncate=False)

# Auto-optimization
spark.sql("""
    ALTER TABLE silver.orders SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# Log and file retention
spark.sql("""
    ALTER TABLE silver.orders SET TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")

# Column mapping (enables column rename/drop)
spark.sql("""
    ALTER TABLE silver.orders SET TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
""")

# After enabling column mapping, you can:
spark.sql("ALTER TABLE silver.orders RENAME COLUMN customer_id TO cust_id")
spark.sql("ALTER TABLE silver.orders DROP COLUMN temp_column")

# Enable Change Data Feed
spark.sql("""
    ALTER TABLE silver.orders SET TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
""")

# Row-level concurrency (Databricks, for merge conflicts)
spark.sql("""
    ALTER TABLE silver.orders SET TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.enableDeletionVectors' = 'true'
    )
""")

# Target file size
spark.sql("""
    ALTER TABLE silver.orders SET TBLPROPERTIES (
        'delta.targetFileSize' = '128mb',
        'delta.tuneFileSizesForRewrites' = 'true'
    )
""")

# Add table comment and tags
spark.sql("COMMENT ON TABLE silver.orders IS 'Silver layer order fact table'")
spark.sql("ALTER TABLE silver.orders SET TAGS ('domain' = 'sales', 'pii' = 'false')")`,
  },
];

// ─── Tab configuration ─────────────────────────────────────────────────────────
const tabs = [
  { key: 'elt', label: 'ELT Transactions', data: eltTransactions, badge: eltTransactions.length },
  { key: 'scd', label: 'SCD', data: scdScenarios, badge: scdScenarios.length },
  { key: 'cdc', label: 'CDC', data: cdcScenarios, badge: cdcScenarios.length },
  {
    key: 'dataframe',
    label: 'DataFrame Ops',
    data: dataFrameScenarios,
    badge: dataFrameScenarios.length,
  },
  {
    key: 'delta',
    label: 'Delta Table Ops',
    data: deltaTableScenarios,
    badge: deltaTableScenarios.length,
  },
];

const ELT_TABS = ['elt', 'scd', 'cdc'];
const ETL_TABS = ['dataframe', 'delta'];

function ELTOperations({ filter = 'elt' }) {
  const filteredTabs =
    filter === 'etl'
      ? tabs.filter((t) => ETL_TABS.includes(t.key))
      : filter === 'elt'
        ? tabs.filter((t) => ELT_TABS.includes(t.key))
        : tabs;

  const [activeTab, setActiveTab] = useState(filteredTabs[0]?.key || 'elt');
  const [expandedId, setExpandedId] = useState(null);

  const currentTab = filteredTabs.find((t) => t.key === activeTab) || filteredTabs[0];
  const scenarios = currentTab ? currentTab.data : [];

  const totalScenarios = filteredTabs.reduce((sum, t) => sum + t.data.length, 0);
  const pageTitle = filter === 'etl' ? 'ETL Transformations' : 'ELT Operations';
  const pageDesc =
    filter === 'etl'
      ? `${totalScenarios} scenarios — DataFrame operations, Delta Table operations`
      : `${totalScenarios} scenarios — ELT transactions, SCD types, CDC methods`;

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>{pageTitle}</h1>
          <p>{pageDesc}</p>
        </div>
      </div>

      <div className="tabs" style={{ marginBottom: '1.5rem' }}>
        {filteredTabs.map((tab) => (
          <button
            key={tab.key}
            className={`tab ${activeTab === tab.key ? 'active' : ''}`}
            onClick={() => {
              setActiveTab(tab.key);
              setExpandedId(null);
            }}
          >
            {tab.label} <span className="badge">{tab.badge}</span>
          </button>
        ))}
      </div>

      <div className="scenarios-list">
        {scenarios.map((scenario) => (
          <div key={scenario.id} className="card scenario-card" style={{ marginBottom: '0.75rem' }}>
            <div
              className="scenario-header"
              onClick={() => setExpandedId(expandedId === scenario.id ? null : scenario.id)}
              style={{
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <div>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    marginBottom: '0.25rem',
                  }}
                >
                  <span className="badge running">{currentTab.label}</span>
                  <strong>
                    #{scenario.id} — {scenario.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                  {scenario.desc}
                </p>
              </div>
              <span style={{ fontSize: '1.2rem', color: 'var(--text-secondary)' }}>
                {expandedId === scenario.id ? '▼' : '▶'}
              </span>
            </div>
            {expandedId === scenario.id && <ScenarioCard scenario={scenario} />}
          </div>
        ))}
      </div>
    </div>
  );
}

export default ELTOperations;
