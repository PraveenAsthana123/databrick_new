import React, { useState } from 'react';
import ScenarioCard from '../../components/common/ScenarioCard';
import { exportToCSV } from '../../utils/fileExport';

const mergePatterns = [
  // ─── 1–5: Basic MERGE (UPSERT) ───
  {
    id: 1,
    group: 'Basic MERGE (UPSERT)',
    title: 'Simple Upsert (Insert or Update)',
    flow: 'MERGE matchedUpdate + notMatchedInsert',
    complexity: 2,
    code: `-- Simple UPSERT: update existing rows, insert new rows
MERGE INTO catalog.silver.customers AS target
USING catalog.bronze.customers_staging AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    target.name        = source.name,
    target.email       = source.email,
    target.updated_at  = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, created_at, updated_at)
  VALUES (source.customer_id, source.name, source.email,
          current_timestamp(), current_timestamp());`,
  },
  {
    id: 2,
    group: 'Basic MERGE (UPSERT)',
    title: 'Insert-Only Merge',
    flow: 'MERGE notMatchedInsert only',
    complexity: 1,
    code: `-- Insert-only merge: add new rows, skip existing
MERGE INTO catalog.silver.products AS target
USING catalog.bronze.products_new AS source
ON target.product_id = source.product_id
WHEN NOT MATCHED THEN
  INSERT (product_id, name, category, price, created_at)
  VALUES (source.product_id, source.name, source.category,
          source.price, current_timestamp());
-- Rows that already exist are silently skipped (no update).`,
  },
  {
    id: 3,
    group: 'Basic MERGE (UPSERT)',
    title: 'Update-Only Merge',
    flow: 'MERGE matchedUpdate only',
    complexity: 2,
    code: `-- Update-only merge: refresh attributes for existing records
MERGE INTO catalog.silver.inventory AS target
USING catalog.bronze.inventory_snapshot AS source
ON target.sku = source.sku
WHEN MATCHED THEN
  UPDATE SET
    target.quantity_on_hand = source.quantity_on_hand,
    target.warehouse         = source.warehouse,
    target.last_counted_at   = source.snapshot_ts;
-- New SKUs in source are ignored (no insert clause).`,
  },
  {
    id: 4,
    group: 'Basic MERGE (UPSERT)',
    title: 'Delete on Match',
    flow: 'MERGE matchedDelete',
    complexity: 2,
    code: `-- Soft-delete merge: remove matched records from target
MERGE INTO catalog.silver.orders AS target
USING catalog.bronze.cancelled_orders AS source
ON target.order_id = source.order_id
   AND target.status != 'CANCELLED'
WHEN MATCHED THEN
  DELETE;
-- Only orders found in the cancellation feed are removed.
-- Rows not in source remain untouched.`,
  },
  {
    id: 5,
    group: 'Basic MERGE (UPSERT)',
    title: 'Full CRUD Merge (Insert/Update/Delete)',
    flow: 'MERGE all 3 clauses',
    complexity: 3,
    code: `-- Full CRUD merge: insert new, update changed, delete removed
MERGE INTO catalog.silver.employees AS target
USING catalog.bronze.hr_feed AS source
ON target.emp_id = source.emp_id
WHEN MATCHED AND source.action = 'DELETE' THEN
  DELETE
WHEN MATCHED AND source.action != 'DELETE' THEN
  UPDATE SET
    target.name       = source.name,
    target.department = source.department,
    target.salary     = source.salary,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED AND source.action != 'DELETE' THEN
  INSERT (emp_id, name, department, salary, created_at)
  VALUES (source.emp_id, source.name, source.department,
          source.salary, current_timestamp());`,
  },

  // ─── 6–10: SCD Merge Patterns ───
  {
    id: 6,
    group: 'SCD Merge Patterns',
    title: 'SCD Type 1 (Overwrite)',
    flow: 'MERGE matchedUpdateAll',
    complexity: 2,
    code: `-- SCD Type 1: overwrite in place — no history kept
MERGE INTO catalog.silver.dim_customer AS target
USING catalog.bronze.crm_updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET *        -- updateAll: overwrite every column
WHEN NOT MATCHED THEN
  INSERT *;           -- insertAll: insert all source columns
-- Simple but destroys historical values.`,
  },
  {
    id: 7,
    group: 'SCD Merge Patterns',
    title: 'SCD Type 2 (History)',
    flow: 'MERGE close old + insert new',
    complexity: 4,
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit

target = DeltaTable.forName(spark, "catalog.silver.dim_customer_scd2")
updates = spark.table("catalog.bronze.crm_updates").withColumn("is_current", lit(True))

# Step 1 — expire existing current rows that have changed
target.alias("t").merge(
    updates.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(set={
    "is_current": "false",
    "valid_to":   "current_date()"
}).execute()

# Step 2 — insert new versions
updates.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("catalog.silver.dim_customer_scd2")`,
  },
  {
    id: 8,
    group: 'SCD Merge Patterns',
    title: 'SCD Type 2 with Effective Dates',
    flow: 'MERGE with valid_from/valid_to',
    complexity: 5,
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit, col

target = DeltaTable.forName(spark, "catalog.silver.dim_product_scd2")
src    = spark.table("catalog.bronze.product_updates") \
              .withColumn("valid_from", current_date()) \
              .withColumn("valid_to",   lit("9999-12-31").cast("date")) \
              .withColumn("is_current", lit(True))

# Close superseded rows
target.alias("t").merge(src.alias("s"),
    "t.product_id = s.product_id AND t.is_current = true AND ("
    "t.name != s.name OR t.price != s.price OR t.category != s.category)"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "valid_to":   col("s.valid_from")
}).execute()

# Insert new effective rows
src.write.format("delta").mode("append").saveAsTable("catalog.silver.dim_product_scd2")`,
  },
  {
    id: 9,
    group: 'SCD Merge Patterns',
    title: 'SCD Type 3 (Previous Value)',
    flow: 'MERGE keep previous column',
    complexity: 3,
    code: `-- SCD Type 3: keep current + one previous value per attribute
MERGE INTO catalog.silver.dim_employee_scd3 AS target
USING catalog.bronze.hr_updates AS source
ON target.emp_id = source.emp_id
WHEN MATCHED AND target.department != source.department THEN
  UPDATE SET
    target.prev_department = target.department,   -- archive old value
    target.department      = source.department,   -- set new value
    target.dept_changed_at = current_timestamp()
WHEN MATCHED THEN
  UPDATE SET
    target.name       = source.name,
    target.salary     = source.salary,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (emp_id, name, department, prev_department, salary, dept_changed_at)
  VALUES (source.emp_id, source.name, source.department, NULL, source.salary, NULL);`,
  },
  {
    id: 10,
    group: 'SCD Merge Patterns',
    title: 'SCD Type 6 (Hybrid 1+2+3)',
    flow: 'Combined SCD patterns',
    complexity: 5,
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit, col, md5, concat_ws

target = DeltaTable.forName(spark, "catalog.silver.dim_customer_scd6")
src = spark.table("catalog.bronze.crm_updates") \
    .withColumn("valid_from",   current_date()) \
    .withColumn("valid_to",     lit("9999-12-31").cast("date")) \
    .withColumn("is_current",   lit(True)) \
    .withColumn("row_hash",     md5(concat_ws("|", "name", "email", "segment", "region")))

# Type 2: expire old rows
target.alias("t").merge(src.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true AND t.row_hash != s.row_hash"
).whenMatchedUpdate(set={
    "is_current":       lit(False),
    "valid_to":         col("s.valid_from"),
    "prev_segment":     col("t.segment"),   # Type 3: save last segment
    "prev_region":      col("t.region"),    # Type 3: save last region
}).execute()

# Insert new version (Type 1 current cols + Type 2 history + Type 3 prev cols)
src.write.format("delta").mode("append").saveAsTable("catalog.silver.dim_customer_scd6")`,
  },

  // ─── 11–15: CDC Merge Patterns ───
  {
    id: 11,
    group: 'CDC Merge Patterns',
    title: 'CDC Insert/Update Merge',
    flow: 'Apply CDC inserts and updates',
    complexity: 3,
    code: `from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "catalog.silver.orders")

# CDC feed contains op_type: 'I' (insert), 'U' (update)
cdc = spark.table("catalog.bronze.orders_cdc") \
    .filter("op_type IN ('I', 'U')")

target.alias("t").merge(cdc.alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdate(set={
    "t.status":     "s.status",
    "t.amount":     "s.amount",
    "t.updated_at": "s.cdc_ts"
}).whenNotMatchedInsert(values={
    "order_id":   "s.order_id",
    "customer_id":"s.customer_id",
    "amount":     "s.amount",
    "status":     "s.status",
    "created_at": "s.cdc_ts",
    "updated_at": "s.cdc_ts"
}).execute()`,
  },
  {
    id: 12,
    group: 'CDC Merge Patterns',
    title: 'CDC with Delete Operations',
    flow: 'Apply CDC deletes via merge',
    complexity: 4,
    code: `from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "catalog.silver.products")

# CDC feed: op_type 'I', 'U', 'D'
cdc = spark.table("catalog.bronze.products_cdc") \
    .orderBy("cdc_seq").dropDuplicates(["product_id"])

target.alias("t").merge(cdc.alias("s"),
    "t.product_id = s.product_id"
).whenMatchedDelete(
    condition="s.op_type = 'D'"
).whenMatchedUpdate(
    condition="s.op_type IN ('U')",
    set={"t.name": "s.name", "t.price": "s.price",
         "t.category": "s.category", "t.updated_at": "s.cdc_ts"}
).whenNotMatchedInsert(
    condition="s.op_type IN ('I', 'U')",
    values={"product_id": "s.product_id", "name": "s.name",
            "price": "s.price", "category": "s.category",
            "created_at": "s.cdc_ts", "updated_at": "s.cdc_ts"}
).execute()`,
  },
  {
    id: 13,
    group: 'CDC Merge Patterns',
    title: 'Debezium CDC Merge',
    flow: 'Parse Debezium format + merge',
    complexity: 4,
    code: `from pyspark.sql.functions import col, from_json, schema_of_json
from delta.tables import DeltaTable

# Debezium payload: {"before":{...}, "after":{...}, "op": "c/u/d/r"}
raw = spark.table("catalog.bronze.debezium_orders_raw")
cdc = raw.select(
    col("payload.op").alias("op"),
    col("payload.after.order_id").alias("order_id"),
    col("payload.after.amount").alias("amount"),
    col("payload.after.status").alias("status"),
    col("payload.ts_ms").alias("event_ms")
).dropDuplicates(["order_id", "event_ms"])

target = DeltaTable.forName(spark, "catalog.silver.orders")
target.alias("t").merge(cdc.alias("s"), "t.order_id = s.order_id") \
    .whenMatchedDelete(condition="s.op = 'd'") \
    .whenMatchedUpdate(condition="s.op IN ('u', 'r')",
        set={"t.amount": "s.amount", "t.status": "s.status"}) \
    .whenNotMatchedInsert(condition="s.op IN ('c', 'r')",
        values={"order_id": "s.order_id", "amount": "s.amount", "status": "s.status"}) \
    .execute()`,
  },
  {
    id: 14,
    group: 'CDC Merge Patterns',
    title: 'Delta CDF (Change Data Feed)',
    flow: 'Read CDF + apply changes',
    complexity: 3,
    code: `from delta.tables import DeltaTable

# Enable CDF on source table (one-time)
spark.sql("ALTER TABLE catalog.silver.orders SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")

# Read changes since last processed version
cdf = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", dbutils.widgets.get("last_processed_version")) \
    .table("catalog.silver.orders") \
    .filter("_change_type IN ('insert', 'update_postimage', 'delete')")

target = DeltaTable.forName(spark, "catalog.gold.orders_replica")
target.alias("t").merge(cdf.alias("s"), "t.order_id = s.order_id") \
    .whenMatchedDelete(condition="s._change_type = 'delete'") \
    .whenMatchedUpdateAll(condition="s._change_type = 'update_postimage'") \
    .whenNotMatchedInsertAll(condition="s._change_type = 'insert'") \
    .execute()`,
  },
  {
    id: 15,
    group: 'CDC Merge Patterns',
    title: 'DLT Apply Changes',
    flow: 'Delta Live Tables CDC',
    complexity: 3,
    code: `import dlt
from pyspark.sql.functions import col

# Delta Live Tables — declarative CDC pipeline
@dlt.table(comment="Bronze raw CDC feed from Kafka")
def orders_cdc_raw():
    return spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", "orders.cdc") \
        .load()

dlt.create_streaming_table("orders_silver")

dlt.apply_changes(
    target        = "orders_silver",
    source        = "orders_cdc_raw",
    keys          = ["order_id"],
    sequence_by   = col("cdc_seq"),
    apply_as_deletes = col("op_type") == "DELETE",
    except_column_list = ["op_type", "cdc_seq", "_kafka_timestamp"],
    stored_as_scd_type = 1
)`,
  },

  // ─── 16–20: Dedup & Idempotent Merge ───
  {
    id: 16,
    group: 'Dedup & Idempotent Merge',
    title: 'Dedup Merge (Keep Latest)',
    flow: 'MERGE with ROW_NUMBER window',
    complexity: 3,
    code: `from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, desc
from delta.tables import DeltaTable

# Deduplicate source: keep latest record per key
w = Window.partitionBy("order_id").orderBy(desc("event_ts"))
deduped = spark.table("catalog.bronze.orders_raw") \
    .withColumn("rn", row_number().over(w)) \
    .filter("rn = 1").drop("rn")

target = DeltaTable.forName(spark, "catalog.silver.orders")
target.alias("t").merge(deduped.alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdateAll(
    condition="s.event_ts > t.event_ts"   # only overwrite if source is newer
).whenNotMatchedInsertAll() \
 .execute()`,
  },
  {
    id: 17,
    group: 'Dedup & Idempotent Merge',
    title: 'Idempotent Upsert',
    flow: 'Safe re-run with batch tracking',
    complexity: 3,
    code: `from delta.tables import DeltaTable

batch_id = dbutils.widgets.get("batch_id")  # e.g. "2024-01-15_01"

# Check if batch already applied — idempotent guard
already_done = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM catalog.audit.merge_batches
    WHERE batch_id = '{batch_id}' AND status = 'SUCCESS'
""").collect()[0].cnt

if already_done > 0:
    print(f"Batch {batch_id} already applied — skipping.")
else:
    src = spark.table("catalog.bronze.orders_stage").filter(f"batch_id = '{batch_id}'")
    target = DeltaTable.forName(spark, "catalog.silver.orders")
    target.alias("t").merge(src.alias("s"), "t.order_id = s.order_id") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    spark.sql(f"INSERT INTO catalog.audit.merge_batches VALUES ('{batch_id}', 'SUCCESS', current_timestamp())")`,
  },
  {
    id: 18,
    group: 'Dedup & Idempotent Merge',
    title: 'Watermark-Based Dedup',
    flow: 'Streaming dedup via watermark',
    complexity: 4,
    code: `from pyspark.sql.functions import col
from delta.tables import DeltaTable

def upsert_with_watermark(micro_batch_df, batch_id):
    # Deduplicate within micro-batch using watermark
    deduped = micro_batch_df.dropDuplicates(["event_id"]) \
        .filter(col("event_ts") > col("watermark_ts"))

    target = DeltaTable.forName(spark, "catalog.silver.events")
    target.alias("t").merge(deduped.alias("s"), "t.event_id = s.event_id") \
        .whenMatchedUpdate(condition="s.event_ts > t.event_ts",
            set={"t.payload": "s.payload", "t.event_ts": "s.event_ts"}) \
        .whenNotMatchedInsertAll() \
        .execute()

stream = spark.readStream.format("delta").table("catalog.bronze.events") \
    .withWatermark("event_ts", "10 minutes") \
    .writeStream.foreachBatch(upsert_with_watermark) \
    .option("checkpointLocation", "/mnt/checkpoints/events") \
    .start()`,
  },
  {
    id: 19,
    group: 'Dedup & Idempotent Merge',
    title: 'Composite Key Merge',
    flow: 'Multi-column join key',
    complexity: 3,
    code: `from delta.tables import DeltaTable

# Composite key: (tenant_id, order_id, line_item_id)
src = spark.table("catalog.bronze.order_line_items")

target = DeltaTable.forName(spark, "catalog.silver.order_line_items")
target.alias("t").merge(
    src.alias("s"),
    """
    t.tenant_id    = s.tenant_id    AND
    t.order_id     = s.order_id     AND
    t.line_item_id = s.line_item_id
    """
).whenMatchedUpdate(set={
    "t.quantity":   "s.quantity",
    "t.unit_price": "s.unit_price",
    "t.amount":     "s.quantity * s.unit_price",
    "t.updated_at": "current_timestamp()"
}).whenNotMatchedInsertAll() \
 .execute()`,
  },
  {
    id: 20,
    group: 'Dedup & Idempotent Merge',
    title: 'Hash-Based Dedup Merge',
    flow: 'Hash comparison for changes',
    complexity: 4,
    code: `from pyspark.sql.functions import md5, concat_ws, col
from delta.tables import DeltaTable

# Compute row hash on source to detect actual changes
src = spark.table("catalog.bronze.products") \
    .withColumn("row_hash", md5(concat_ws("|",
        col("name"), col("price"), col("category"), col("description"))))

target = DeltaTable.forName(spark, "catalog.silver.products")
target.alias("t").merge(
    src.alias("s"), "t.product_id = s.product_id"
).whenMatchedUpdate(
    condition="t.row_hash != s.row_hash",     # skip if nothing changed
    set={"t.name": "s.name", "t.price": "s.price",
         "t.category": "s.category", "t.description": "s.description",
         "t.row_hash": "s.row_hash", "t.updated_at": "current_timestamp()"}
).whenNotMatchedInsertAll() \
 .execute()`,
  },

  // ─── 21–25: Advanced Merge Patterns ───
  {
    id: 21,
    group: 'Advanced Merge Patterns',
    title: 'Conditional Update Merge',
    flow: 'Update only changed columns',
    complexity: 4,
    code: `-- Conditional column-level updates (only modify what actually changed)
MERGE INTO catalog.silver.accounts AS target
USING catalog.bronze.account_updates AS source
ON target.account_id = source.account_id
WHEN MATCHED AND (
    target.email    != source.email   OR
    target.phone    != source.phone   OR
    target.address  != source.address OR
    target.tier     != source.tier
) THEN
  UPDATE SET
    target.email      = CASE WHEN target.email   != source.email   THEN source.email   ELSE target.email   END,
    target.phone      = CASE WHEN target.phone   != source.phone   THEN source.phone   ELSE target.phone   END,
    target.address    = CASE WHEN target.address != source.address THEN source.address ELSE target.address END,
    target.tier       = CASE WHEN target.tier    != source.tier    THEN source.tier    ELSE target.tier    END,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT * ;`,
  },
  {
    id: 22,
    group: 'Advanced Merge Patterns',
    title: 'Multi-Table Merge',
    flow: 'Merge into multiple targets',
    complexity: 5,
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Single source feed → fan-out into multiple targets
src = spark.table("catalog.bronze.unified_events")

# Target 1: orders table
DeltaTable.forName(spark, "catalog.silver.orders").alias("t") \
    .merge(src.filter("event_type = 'ORDER'").alias("s"), "t.order_id = s.ref_id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Target 2: payments table
DeltaTable.forName(spark, "catalog.silver.payments").alias("t") \
    .merge(src.filter("event_type = 'PAYMENT'").alias("s"), "t.payment_id = s.ref_id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Target 3: refunds table
DeltaTable.forName(spark, "catalog.silver.refunds").alias("t") \
    .merge(src.filter("event_type = 'REFUND'").alias("s"), "t.refund_id = s.ref_id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`,
  },
  {
    id: 23,
    group: 'Advanced Merge Patterns',
    title: 'Partitioned Merge',
    flow: 'Merge with partition pruning',
    complexity: 4,
    code: `from delta.tables import DeltaTable

# Restrict merge to a specific date partition — avoids full table scan
merge_date = dbutils.widgets.get("merge_date")   # e.g. "2024-01-15"

src = spark.table("catalog.bronze.events") \
    .filter(f"event_date = '{merge_date}'")

target = DeltaTable.forName(spark, "catalog.silver.events")

# Partition filter in join condition enables partition pruning
target.alias("t").merge(
    src.alias("s"),
    f"t.event_date = '{merge_date}' AND t.event_id = s.event_id"
).whenMatchedUpdate(set={
    "t.status":     "s.status",
    "t.updated_at": "current_timestamp()"
}).whenNotMatchedInsertAll() \
 .execute()

print(f"Merge complete for partition: {merge_date}")`,
  },
  {
    id: 24,
    group: 'Advanced Merge Patterns',
    title: 'Streaming Merge (foreachBatch)',
    flow: 'Merge inside streaming micro-batch',
    complexity: 5,
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import col, window

def merge_micro_batch(batch_df, batch_id):
    # Deduplicate within batch before merge
    batch_deduped = batch_df.dropDuplicates(["event_id"]) \
        .filter(col("event_ts").isNotNull())

    target = DeltaTable.forName(spark, "catalog.silver.clickstream")
    target.alias("t").merge(
        batch_deduped.alias("s"), "t.event_id = s.event_id"
    ).whenMatchedUpdate(
        condition="s.event_ts > t.event_ts",
        set={"t.page": "s.page", "t.session_id": "s.session_id",
             "t.event_ts": "s.event_ts"}
    ).whenNotMatchedInsertAll().execute()

spark.readStream \
    .format("kafka").option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "clickstream").load() \
    .writeStream.foreachBatch(merge_micro_batch) \
    .option("checkpointLocation", "/mnt/chk/clickstream") \
    .trigger(processingTime="30 seconds").start()`,
  },
  {
    id: 25,
    group: 'Advanced Merge Patterns',
    title: 'Schema Evolution Merge',
    flow: 'Merge with mergeSchema option',
    complexity: 4,
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

# Source has new columns not in target — use mergeSchema to auto-evolve
src = spark.table("catalog.bronze.products_v2")   # has new cols: tags, rating

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

target = DeltaTable.forName(spark, "catalog.silver.products")
target.alias("t").merge(
    src.alias("s"), "t.product_id = s.product_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# New columns (tags, rating) are automatically added to the target schema.
print("Schema after merge:")
spark.table("catalog.silver.products").printSchema()`,
  },

  // ─── 26–30: Enterprise Merge Patterns ───
  {
    id: 26,
    group: 'Enterprise Merge Patterns',
    title: 'Audit Trail Merge',
    flow: 'Merge + log all changes',
    complexity: 4,
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col

target_tbl = "catalog.silver.accounts"
src = spark.table("catalog.bronze.account_updates")
target = DeltaTable.forName(spark, target_tbl)

# Capture before-state for audit
before = spark.table(target_tbl).join(src.select("account_id"), "account_id")

target.alias("t").merge(src.alias("s"), "t.account_id = s.account_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Capture after-state and write diff to audit log
after = spark.table(target_tbl).join(src.select("account_id"), "account_id")
audit = before.alias("b").join(after.alias("a"), "account_id") \
    .select("account_id",
            col("b.tier").alias("tier_before"), col("a.tier").alias("tier_after"),
            col("b.email").alias("email_before"), col("a.email").alias("email_after"),
            current_timestamp().alias("changed_at"), lit("MERGE").alias("operation"))
audit.write.format("delta").mode("append").saveAsTable("catalog.audit.account_changes")`,
  },
  {
    id: 27,
    group: 'Enterprise Merge Patterns',
    title: 'PII-Safe Merge',
    flow: 'Merge with masking',
    complexity: 5,
    code: `from pyspark.sql.functions import sha2, concat, lit, col, substring
from delta.tables import DeltaTable

# Mask PII before merging into analytics layer
src_raw = spark.table("catalog.bronze.customers_raw")
src_masked = src_raw.withColumn(
    "email_hash",   sha2(col("email"), 256)
).withColumn(
    "phone_masked", concat(lit("***-***-"), substring("phone", -4, 4))
).withColumn(
    "ssn_masked",   concat(lit("***-**-"), substring("ssn", -4, 4))
).withColumn(
    "name_token",   sha2(concat("first_name", lit("|"), "last_name"), 256)
).drop("email", "phone", "ssn", "first_name", "last_name",
       "date_of_birth", "credit_card_number")

target = DeltaTable.forName(spark, "catalog.silver.customers_analytics")
target.alias("t").merge(src_masked.alias("s"), "t.customer_id = s.customer_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()`,
  },
  {
    id: 28,
    group: 'Enterprise Merge Patterns',
    title: 'Reconciliation Merge',
    flow: 'Source vs target reconcile',
    complexity: 4,
    code: `from pyspark.sql.functions import col, count, sum as spark_sum, abs as spark_abs
from delta.tables import DeltaTable

src    = spark.table("catalog.bronze.orders_source")
target = DeltaTable.forName(spark, "catalog.silver.orders")

# Step 1 — merge changes
target.alias("t").merge(src.alias("s"), "t.order_id = s.order_id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Step 2 — reconcile counts and sums
src_agg = src.agg(count("*").alias("cnt"), spark_sum("amount").alias("total")).first()
tgt_agg = spark.table("catalog.silver.orders") \
    .agg(count("*").alias("cnt"), spark_sum("amount").alias("total")).first()

recon = spark.createDataFrame([{
    "run_ts":        str(__import__("datetime").datetime.utcnow()),
    "src_count":     src_agg.cnt,  "tgt_count":  tgt_agg.cnt,
    "count_ok":      src_agg.cnt == tgt_agg.cnt,
    "src_total":     float(src_agg.total), "tgt_total": float(tgt_agg.total),
    "amount_ok":     abs(float(src_agg.total) - float(tgt_agg.total)) < 0.01
}])
recon.write.format("delta").mode("append").saveAsTable("catalog.audit.merge_reconciliation")`,
  },
  {
    id: 29,
    group: 'Enterprise Merge Patterns',
    title: 'Cross-Region Merge',
    flow: 'DEEP CLONE + MERGE for DR',
    complexity: 5,
    code: `# Cross-region DR: replicate via DEEP CLONE then MERGE incremental changes
tables = ["orders", "customers", "products", "payments"]
dr_catalog = "catalog_us_east"
primary_catalog = "catalog_us_west"

for tbl in tables:
    src_full = f"{primary_catalog}.silver.{tbl}"
    dr_full  = f"{dr_catalog}.silver.{tbl}"

    # Initial clone (or if DR is very stale, re-clone)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {dr_full} DEEP CLONE {src_full} LOCATION 'abfss://dr@eastus.dfs.core.windows.net/{tbl}'")

    # Incremental catch-up: read CDF and merge into DR
    cdf = spark.read.format("delta").option("readChangeFeed","true") \
        .option("startingVersion", dbutils.widgets.get(f"last_version_{tbl}")) \
        .table(src_full)

    from delta.tables import DeltaTable
    DeltaTable.forName(spark, dr_full).alias("t") \
        .merge(cdf.filter("_change_type != 'delete'").alias("s"), f"t.{tbl[:-1]}_id = s.{tbl[:-1]}_id") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"DR sync complete: {tbl}")`,
  },
  {
    id: 30,
    group: 'Enterprise Merge Patterns',
    title: 'High-Volume Merge (Optimized)',
    flow: 'Partitioned + Z-ordered merge at scale',
    complexity: 5,
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Pre-optimize target before large merge (Z-Order on join key + filter cols)
spark.sql("OPTIMIZE catalog.silver.transactions ZORDER BY (account_id, txn_date)")

# Repartition source to match target partition layout
src = spark.table("catalog.bronze.transactions_batch") \
    .repartition(200, col("account_id"))  # match target shuffle partitions

target = DeltaTable.forName(spark, "catalog.silver.transactions")

# Restrict merge to affected partitions only (partition pruning)
max_date = src.agg({"txn_date": "max"}).collect()[0][0]
min_date = src.agg({"txn_date": "min"}).collect()[0][0]

target.alias("t").merge(
    src.alias("s"),
    f"t.txn_date BETWEEN '{min_date}' AND '{max_date}' AND t.txn_id = s.txn_id"
).whenMatchedUpdate(set={
    "t.status": "s.status", "t.settled_at": "s.settled_at"
}).whenNotMatchedInsertAll().execute()

spark.sql("OPTIMIZE catalog.silver.transactions ZORDER BY (account_id, txn_date)")`,
  },
];

const groups = [...new Set(mergePatterns.map((s) => s.group))];

const antiPatterns = [
  {
    title: 'MERGE without partition filter on large tables',
    description:
      'Running MERGE without a partition filter forces a full table scan and shuffle on every run. Always add `target.partition_col BETWEEN min AND max` in the ON condition to enable partition pruning.',
  },
  {
    title: 'Non-deterministic source (duplicates on the join key)',
    description:
      'Delta MERGE throws a runtime error if the source has duplicate rows on the join key. Always deduplicate the source with dropDuplicates() or ROW_NUMBER() before merging.',
  },
  {
    title: 'SCD Type 2 in a single MERGE pass',
    description:
      'Attempting to both expire old rows and insert new versions in a single MERGE statement causes conflicts. Always use two separate steps: expire first, then append new rows.',
  },
  {
    title: 'Merging unbounded streaming data without foreachBatch',
    description:
      'Delta MERGE is a batch operation. Calling it directly inside readStream will fail. Always wrap the merge inside a foreachBatch function and pass it to writeStream.',
  },
  {
    title: 'Skipping OPTIMIZE + ZORDER after high-volume merges',
    description:
      'Frequent merges produce many small files, degrading read performance. Always run OPTIMIZE ZORDER BY (join_key) after large merges to compact files and restore Z-ordering.',
  },
];

const decisionGuide = [
  {
    question: 'New data may contain duplicates and some records should overwrite others',
    answer: 'Use Dedup Merge (Keep Latest) — ROW_NUMBER window on source before merging',
  },
  {
    question: 'Need to track full history of every attribute change',
    answer: 'Use SCD Type 2 with Effective Dates — valid_from / valid_to + is_current flag',
  },
  {
    question: 'Source system sends CDC events (insert/update/delete)',
    answer: 'Use CDC with Delete Operations or Debezium CDC Merge depending on format',
  },
  {
    question: 'Pipeline re-runs must be safe (idempotent)',
    answer: 'Use Idempotent Upsert — track applied batch_ids in an audit table',
  },
  {
    question: 'Real-time streaming source, sub-minute latency',
    answer: 'Use Streaming Merge (foreachBatch) with checkpoint and dedup inside micro-batch',
  },
  {
    question: 'Target table has billions of rows and merge is slow',
    answer:
      'Use High-Volume Merge (Optimized) — OPTIMIZE ZORDER before merge + partition filter in ON clause',
  },
  {
    question: 'Need to audit every change for compliance',
    answer: 'Use Audit Trail Merge — capture before/after state and write diff to audit log',
  },
  {
    question: 'Simple refresh: only the latest value per key matters',
    answer: 'Use SCD Type 1 (Overwrite) or Simple Upsert — no history needed',
  },
];

function ComplexityBar({ value }) {
  const colors = { 1: '#22c55e', 2: '#84cc16', 3: '#f59e0b', 4: '#f97316', 5: '#ef4444' };
  const labels = { 1: 'Beginner', 2: 'Easy', 3: 'Moderate', 4: 'Advanced', 5: 'Expert' };
  const color = colors[value] || '#6b7280';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
      <div style={{ display: 'flex', gap: '3px' }}>
        {[1, 2, 3, 4, 5].map((i) => (
          <div
            key={i}
            style={{
              width: '18px',
              height: '10px',
              borderRadius: '3px',
              background: i <= value ? color : '#e5e7eb',
            }}
          />
        ))}
      </div>
      <span style={{ fontSize: '0.75rem', color, fontWeight: 600 }}>{labels[value]}</span>
    </div>
  );
}

function MergePatterns() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('id');

  const filtered = mergePatterns
    .filter((s) => {
      const matchGroup = selectedGroup === 'All' || s.group === selectedGroup;
      const matchSearch =
        s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
        s.flow.toLowerCase().includes(searchTerm.toLowerCase());
      return matchGroup && matchSearch;
    })
    .sort((a, b) => (sortBy === 'complexity' ? b.complexity - a.complexity : a.id - b.id));

  const downloadCSV = () => {
    exportToCSV(
      mergePatterns.map((s) => ({
        id: s.id,
        group: s.group,
        title: s.title,
        flow: s.flow,
        complexity: s.complexity,
      })),
      'delta-merge-patterns.csv'
    );
  };

  return (
    <div>
      {/* ── Header ── */}
      <div className="page-header">
        <div>
          <h1>Delta MERGE Patterns</h1>
          <p>30 merge patterns — UPSERT, SCD, CDC, Dedup, Advanced, Enterprise</p>
        </div>
      </div>

      {/* ── Stats ── */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">⚡</div>
          <div className="stat-info">
            <h4>30</h4>
            <p>Merge Patterns</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">🗂</div>
          <div className="stat-info">
            <h4>6</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">🔥</div>
          <div className="stat-info">
            <h4>{mergePatterns.filter((s) => s.complexity >= 4).length}</h4>
            <p>Advanced (4–5)</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">🏢</div>
          <div className="stat-info">
            <h4>{mergePatterns.filter((s) => s.group === 'Enterprise Merge Patterns').length}</h4>
            <p>Enterprise</p>
          </div>
        </div>
      </div>

      {/* ── Filters ── */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search merge patterns..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '260px' }}
          />
          <select
            className="form-input"
            value={selectedGroup}
            onChange={(e) => setSelectedGroup(e.target.value)}
            style={{ maxWidth: '240px' }}
          >
            <option value="All">All Groups (30)</option>
            {groups.map((g) => (
              <option key={g} value={g}>
                {g} ({mergePatterns.filter((s) => s.group === g).length})
              </option>
            ))}
          </select>
          <select
            className="form-input"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            style={{ maxWidth: '180px' }}
          >
            <option value="id">Sort by #</option>
            <option value="complexity">Sort by Complexity</option>
          </select>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadCSV}
            style={{ marginLeft: 'auto' }}
          >
            Download Patterns (CSV)
          </button>
        </div>
      </div>

      {/* ── Pattern Cards ── */}
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
                    #{s.id} &mdash; {s.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{s.flow}</p>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                <ComplexityBar value={s.complexity} />
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
                  gridTemplateColumns: '200px 1fr',
                  gap: '1rem',
                }}
              >
                <div style={{ padding: '0.75rem', background: '#f8f9fa', borderRadius: '6px' }}>
                  <div
                    style={{
                      fontSize: '0.72rem',
                      fontWeight: 700,
                      marginBottom: '0.6rem',
                      color: 'var(--text-secondary)',
                    }}
                  >
                    COMPLEXITY
                  </div>
                  <ComplexityBar value={s.complexity} />
                  <div
                    style={{
                      marginTop: '0.75rem',
                      fontSize: '0.72rem',
                      color: 'var(--text-secondary)',
                      fontWeight: 700,
                    }}
                  >
                    CATEGORY
                  </div>
                  <div style={{ marginTop: '0.25rem', fontSize: '0.8rem' }}>{s.group}</div>
                  <div
                    style={{
                      marginTop: '0.75rem',
                      fontSize: '0.72rem',
                      color: 'var(--text-secondary)',
                      fontWeight: 700,
                    }}
                  >
                    PATTERN #
                  </div>
                  <div style={{ marginTop: '0.25rem', fontSize: '0.8rem', fontWeight: 600 }}>
                    {s.id} / 30
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

      {/* ── Merge Anti-Patterns ── */}
      <div
        className="card"
        style={{
          marginTop: '2rem',
          border: '1.5px solid #fca5a5',
          background: '#fff5f5',
        }}
      >
        <h3 style={{ color: '#dc2626', marginBottom: '0.25rem' }}>Merge Anti-Patterns</h3>
        <p style={{ color: '#6b7280', fontSize: '0.85rem', marginBottom: '1rem' }}>
          5 common mistakes that cause incorrect results, poor performance, or runtime failures.
        </p>
        <div style={{ display: 'grid', gap: '0.75rem' }}>
          {antiPatterns.map((ap, i) => (
            <div
              key={i}
              style={{
                padding: '0.75rem 1rem',
                borderRadius: '6px',
                background: '#fff',
                borderLeft: '4px solid #ef4444',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  alignItems: 'flex-start',
                  gap: '0.5rem',
                }}
              >
                <span style={{ fontWeight: 700, color: '#ef4444', minWidth: '22px' }}>
                  {i + 1}.
                </span>
                <div>
                  <div style={{ fontWeight: 700, marginBottom: '0.2rem' }}>{ap.title}</div>
                  <div style={{ fontSize: '0.84rem', color: '#6b7280' }}>{ap.description}</div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ── Decision Guide ── */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h3 style={{ marginBottom: '0.25rem' }}>When to Use Which Pattern</h3>
        <p style={{ color: '#6b7280', fontSize: '0.85rem', marginBottom: '1rem' }}>
          Use this decision guide to pick the right merge pattern for your scenario.
        </p>
        <div style={{ display: 'grid', gap: '0.6rem' }}>
          {decisionGuide.map((item, i) => (
            <div
              key={i}
              style={{
                display: 'grid',
                gridTemplateColumns: '1fr 1fr',
                gap: '0.75rem',
                padding: '0.7rem 1rem',
                borderRadius: '6px',
                background: i % 2 === 0 ? '#f8fafc' : '#fff',
                borderLeft: '4px solid #6366f1',
              }}
            >
              <div style={{ fontSize: '0.85rem', color: '#374151' }}>
                <span style={{ fontWeight: 600, color: '#6366f1', marginRight: '0.4rem' }}>
                  If:
                </span>
                {item.question}
              </div>
              <div style={{ fontSize: '0.85rem', color: '#374151' }}>
                <span style={{ fontWeight: 600, color: '#059669', marginRight: '0.4rem' }}>
                  Use:
                </span>
                {item.answer}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default MergePatterns;
