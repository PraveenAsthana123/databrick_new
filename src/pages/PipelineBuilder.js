import React, { useState } from 'react';

const pipelines = [
  {
    id: 1,
    category: 'Ingestion',
    title: 'CSV → Bronze → Silver',
    desc: 'File ingestion pipeline',
    stages: [
      'Read CSV from landing',
      'Add metadata columns',
      'Write to Bronze Delta',
      'Validate schema',
      'Clean & deduplicate',
      'Write to Silver Delta',
    ],
    code: `# Pipeline: CSV → Bronze → Silver
from pyspark.sql.functions import current_timestamp, input_file_name, col, trim, lower

# Stage 1: Ingest from landing
raw = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "csv") \\
    .option("header", "true") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/csv_pipeline") \\
    .load("/mnt/landing/csv/")

# Stage 2: Enrich & write Bronze
bronze = raw.withColumn("_ingest_ts", current_timestamp()) \\
    .withColumn("_source_file", input_file_name())
bronze.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/csv_bronze") \\
    .trigger(availableNow=True) \\
    .toTable("catalog.bronze.csv_data")

# Stage 3: Clean & write Silver
bronze_df = spark.table("catalog.bronze.csv_data")
silver = bronze_df.dropDuplicates(["id"]) \\
    .filter(col("id").isNotNull()) \\
    .withColumn("name", trim(col("name"))) \\
    .withColumn("email", lower(col("email")))
silver.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.clean_csv")`,
  },
  {
    id: 2,
    category: 'Ingestion',
    title: 'API → Bronze → Silver → Gold',
    desc: 'API data pipeline end-to-end',
    stages: [
      'Fetch API data',
      'Write to Bronze',
      'Validate & clean',
      'Write to Silver',
      'Aggregate',
      'Write to Gold',
    ],
    code: `import requests
from pyspark.sql.functions import current_timestamp, col

# Stage 1: Extract from API
resp = requests.get("https://api.example.com/v1/orders",
    headers={"Authorization": f"Bearer {dbutils.secrets.get('s','token')}"}, timeout=30)
data = resp.json()["results"]

# Stage 2: Bronze
df = spark.createDataFrame(data).withColumn("_ingest_ts", current_timestamp())
df.write.format("delta").mode("append").saveAsTable("catalog.bronze.api_orders")

# Stage 3: Silver
silver = spark.table("catalog.bronze.api_orders") \\
    .dropDuplicates(["order_id"]) \\
    .filter(col("amount") > 0)
silver.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.orders")

# Stage 4: Gold
spark.sql("""
CREATE OR REPLACE TABLE catalog.gold.daily_orders AS
SELECT order_date, COUNT(*) AS order_count, SUM(amount) AS revenue
FROM catalog.silver.orders GROUP BY order_date
""")`,
  },
  {
    id: 3,
    category: 'Ingestion',
    title: 'Kafka → Bronze → Silver (Streaming)',
    desc: 'Real-time streaming pipeline',
    stages: [
      'Read Kafka stream',
      'Parse JSON payload',
      'Write Bronze',
      'Apply quality rules',
      'Merge to Silver',
    ],
    code: `from pyspark.sql.functions import from_json, col, current_timestamp

# Streaming pipeline
schema = "event_id STRING, user_id STRING, action STRING, timestamp LONG, properties MAP<STRING,STRING>"

df = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "broker:9092") \\
    .option("subscribe", "user_events") \\
    .load()

# Parse and enrich
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_ts")
).select("data.*", "kafka_ts") \\
 .withColumn("_ingest_ts", current_timestamp())

# Write to Bronze
parsed.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/events_bronze") \\
    .toTable("catalog.bronze.user_events")

# Merge to Silver (foreachBatch)
def merge_to_silver(batch, batch_id):
    from delta.tables import DeltaTable
    target = DeltaTable.forName(spark, "catalog.silver.user_events")
    target.alias("t").merge(batch.alias("s"), "t.event_id = s.event_id") \\
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

parsed.writeStream.foreachBatch(merge_to_silver) \\
    .option("checkpointLocation", "/mnt/cp/events_silver") \\
    .start()`,
  },
  {
    id: 4,
    category: 'Ingestion',
    title: 'Database CDC → Bronze → Silver',
    desc: 'Change data capture pipeline',
    stages: [
      'Read CDC stream',
      'Parse Debezium events',
      'Write Bronze',
      'Apply changes to Silver',
      'Track history',
    ],
    code: `from pyspark.sql.functions import from_json, col

# Read CDC from Kafka
cdc = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "broker:9092") \\
    .option("subscribe", "dbserver.public.customers") \\
    .load()

cdc_schema = "before STRUCT<id:INT,name:STRING,email:STRING>, after STRUCT<id:INT,name:STRING,email:STRING>, op STRING, ts_ms LONG"
parsed = cdc.select(from_json(col("value").cast("string"), cdc_schema).alias("cdc")).select("cdc.*")

# Bronze: Raw CDC events
parsed.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/cdc_bronze") \\
    .toTable("catalog.bronze.cdc_customers")

# Silver: Apply changes using DLT
import dlt
dlt.create_streaming_table("customers")
dlt.apply_changes(
    target="customers", source="cdc_customers",
    keys=["after.id"], sequence_by="ts_ms",
    apply_as_deletes=col("op") == "d", stored_as_scd_type=2
)`,
  },
  {
    id: 5,
    category: 'Ingestion',
    title: 'Multi-Source Join Pipeline',
    desc: 'Combine data from 3+ sources',
    stages: [
      'Read from PostgreSQL',
      'Read from S3 CSV',
      'Read from API',
      'Join all sources',
      'Write to Silver',
    ],
    code: `# Source 1: PostgreSQL
pg_df = spark.read.format("jdbc") \\
    .option("url", "jdbc:postgresql://host:5432/db") \\
    .option("dbtable", "customers").load()

# Source 2: S3 CSV
s3_df = spark.read.csv("s3://bucket/orders/*.csv", header=True, inferSchema=True)

# Source 3: API
import requests
api_data = requests.get("https://api.example.com/products", timeout=30).json()
api_df = spark.createDataFrame(api_data)

# Join all
enriched = s3_df.join(pg_df, "customer_id", "left") \\
    .join(api_df, "product_id", "left")
enriched.write.format("delta").saveAsTable("catalog.silver.enriched_orders")`,
  },
  {
    id: 6,
    category: 'ELT',
    title: 'Full Refresh ELT',
    desc: 'Complete overwrite each run',
    stages: [
      'Extract from source',
      'Load raw to staging',
      'Transform in SQL',
      'Write to target',
      'Update metadata',
    ],
    code: `# Full refresh pattern
source = spark.read.format("jdbc") \\
    .option("url", jdbc_url).option("dbtable", "orders").load()

# Load to staging
source.write.format("delta").mode("overwrite").saveAsTable("catalog.staging.orders_raw")

# Transform in SQL
spark.sql("""
CREATE OR REPLACE TABLE catalog.silver.orders AS
SELECT order_id, customer_id, product_id,
    CAST(amount AS DOUBLE) AS amount,
    CAST(order_date AS DATE) AS order_date,
    current_timestamp() AS _processed_at
FROM catalog.staging.orders_raw
WHERE amount > 0 AND order_date IS NOT NULL
""")

# Update metadata
spark.sql("INSERT INTO catalog.audit.elt_log VALUES (current_timestamp(), 'orders', 'FULL_REFRESH', 'SUCCESS')")`,
  },
  {
    id: 7,
    category: 'ELT',
    title: 'Incremental ELT with Watermark',
    desc: 'Process only new records',
    stages: [
      'Get last watermark',
      'Extract new records',
      'Validate',
      'Merge to target',
      'Update watermark',
    ],
    code: `# Get last processed watermark
last_ts = spark.sql("SELECT MAX(watermark_value) FROM catalog.audit.watermarks WHERE table_name = 'orders'").collect()[0][0]

# Extract only new records
new_data = spark.read.format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", f"(SELECT * FROM orders WHERE updated_at > '{last_ts}') AS subq") \\
    .load()

if new_data.count() > 0:
    # Merge to silver
    from delta.tables import DeltaTable
    target = DeltaTable.forName(spark, "catalog.silver.orders")
    target.alias("t").merge(new_data.alias("s"), "t.order_id = s.order_id") \\
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # Update watermark
    max_ts = new_data.agg({"updated_at": "max"}).collect()[0][0]
    spark.sql(f"UPDATE catalog.audit.watermarks SET watermark_value = '{max_ts}' WHERE table_name = 'orders'")`,
  },
  {
    id: 8,
    category: 'ELT',
    title: 'Parallel Multi-Table ELT',
    desc: 'Process multiple tables concurrently',
    stages: [
      'Define table configs',
      'Extract in parallel',
      'Transform each',
      'Load to silver',
      'Validate all',
    ],
    code: `from concurrent.futures import ThreadPoolExecutor

tables = [
    {"name": "customers", "key": "customer_id", "source": "public.customers"},
    {"name": "orders", "key": "order_id", "source": "public.orders"},
    {"name": "products", "key": "product_id", "source": "public.products"},
    {"name": "inventory", "key": "sku_id", "source": "public.inventory"},
]

def process_table(config):
    df = spark.read.format("jdbc") \\
        .option("url", jdbc_url).option("dbtable", config["source"]).load()

    from delta.tables import DeltaTable
    target = DeltaTable.forName(spark, f"catalog.silver.{config['name']}")
    target.alias("t").merge(df.alias("s"), f"t.{config['key']} = s.{config['key']}") \\
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    return f"{config['name']}: OK"

with ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(process_table, tables))
print(results)`,
  },
  {
    id: 9,
    category: 'ML',
    title: 'Feature Engineering Pipeline',
    desc: 'Bronze → Silver → Features → Model',
    stages: [
      'Read silver data',
      'Engineer features',
      'Store in Feature Store',
      'Train model',
      'Register in MLflow',
      'Deploy endpoint',
    ],
    code: `from databricks.feature_engineering import FeatureEngineeringClient
import mlflow

# Feature engineering
features = spark.sql("""
SELECT customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS lifetime_value,
    AVG(amount) AS avg_order,
    DATEDIFF(current_date(), MAX(order_date)) AS recency
FROM catalog.silver.orders GROUP BY customer_id
""")

# Store features
fe = FeatureEngineeringClient()
fe.create_table(name="catalog.gold.customer_features",
    primary_keys=["customer_id"], df=features)

# Train model
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
training = fe.create_training_set(df=labels_df,
    feature_lookups=[FeatureLookup(table_name="catalog.gold.customer_features", lookup_key="customer_id")],
    label="churn")

model = Pipeline(stages=[assembler, rf]).fit(training.load_df())

# Register
with mlflow.start_run():
    mlflow.spark.log_model(model, "model", registered_model_name="catalog.models.churn_model")`,
  },
  {
    id: 10,
    category: 'ML',
    title: 'Batch Scoring Pipeline',
    desc: 'Score new data with production model',
    stages: [
      'Load production model',
      'Read new data',
      'Generate predictions',
      'Write results',
      'Monitor drift',
    ],
    code: `import mlflow

# Load production model
model = mlflow.pyfunc.load_model("models:/catalog.models.churn_model@production")

# Score new data
new_customers = spark.table("catalog.gold.customer_features")
predictions = model.predict(new_customers.toPandas())

# Save predictions
result = new_customers.toPandas()
result["churn_prediction"] = predictions
pred_df = spark.createDataFrame(result)
pred_df.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.churn_predictions")

# Monitor drift
from scipy import stats
baseline = spark.table("catalog.audit.baseline_features").toPandas()
for col_name in feature_cols:
    ks_stat, p_val = stats.ks_2samp(baseline[col_name], result[col_name])
    if p_val < 0.05:
        print(f"DRIFT DETECTED: {col_name} (KS={ks_stat:.3f}, p={p_val:.4f})")`,
  },
  {
    id: 11,
    category: 'ML',
    title: 'Real-time Scoring Pipeline',
    desc: 'Stream scoring with model serving',
    stages: [
      'Read streaming data',
      'Feature lookup',
      'Call model endpoint',
      'Write predictions',
      'Alert on anomalies',
    ],
    code: `import requests

def score_batch(batch_df, batch_id):
    features = batch_df.toPandas()
    endpoint = "https://workspace.cloud.databricks.com/serving-endpoints/churn-model/invocations"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.post(endpoint,
        json={"dataframe_records": features.to_dict("records")},
        headers=headers, timeout=30)

    predictions = response.json()["predictions"]
    features["prediction"] = predictions
    pred_df = spark.createDataFrame(features)
    pred_df.write.format("delta").mode("append").saveAsTable("catalog.gold.realtime_predictions")

spark.readStream.format("delta") \\
    .table("catalog.silver.new_customers") \\
    .writeStream.foreachBatch(score_batch) \\
    .option("checkpointLocation", "/mnt/cp/realtime_scoring") \\
    .start()`,
  },
  {
    id: 12,
    category: 'Governance',
    title: 'Data Quality Pipeline',
    desc: 'Automated data quality monitoring',
    stages: [
      'Profile data',
      'Run expectations',
      'Generate report',
      'Alert on failures',
      'Update dashboard',
    ],
    code: `from pyspark.sql.functions import count, when, isnan, isnull, col

tables = ["catalog.silver.customers", "catalog.silver.orders", "catalog.silver.products"]

for table_name in tables:
    df = spark.table(table_name)
    total = df.count()

    # Quality checks
    null_pcts = {c: df.filter(isnull(c)).count() / total * 100 for c in df.columns}
    dup_count = total - df.dropDuplicates().count()

    # Log results
    quality = spark.createDataFrame([{
        "table": table_name, "total_rows": total,
        "duplicate_count": dup_count,
        "max_null_pct": max(null_pcts.values()),
        "check_time": str(current_timestamp()),
        "status": "PASS" if max(null_pcts.values()) < 5 and dup_count == 0 else "FAIL"
    }])
    quality.write.format("delta").mode("append").saveAsTable("catalog.audit.data_quality_log")`,
  },
  {
    id: 13,
    category: 'Governance',
    title: 'Audit Trail Pipeline',
    desc: 'Track all data operations',
    stages: [
      'Capture operation',
      'Record metadata',
      'Write audit log',
      'Alert on anomalies',
      'Generate compliance report',
    ],
    code: `from pyspark.sql.functions import current_timestamp, lit

def audit_operation(operation, table_name, row_count, status, details=""):
    audit = spark.createDataFrame([{
        "operation": operation,
        "table_name": table_name,
        "row_count": row_count,
        "status": status,
        "details": details,
        "user": spark.sql("SELECT current_user()").collect()[0][0],
        "timestamp": str(current_timestamp())
    }])
    audit.write.format("delta").mode("append").saveAsTable("catalog.audit.operation_log")

# Usage in pipelines
df = spark.table("catalog.bronze.raw_data")
try:
    cleaned = df.dropDuplicates().filter("id IS NOT NULL")
    cleaned.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.data")
    audit_operation("TRANSFORM", "catalog.silver.data", cleaned.count(), "SUCCESS")
except Exception as e:
    audit_operation("TRANSFORM", "catalog.silver.data", 0, "FAILED", str(e))
    raise`,
  },
  {
    id: 14,
    category: 'Reporting',
    title: 'Daily Report Pipeline',
    desc: 'Automated daily report generation',
    stages: [
      'Aggregate data',
      'Generate charts',
      'Create HTML report',
      'Save to volume',
      'Send notification',
    ],
    code: `import plotly.express as px
from jinja2 import Template

# Aggregate
daily = spark.sql("""
SELECT order_date, SUM(amount) AS revenue, COUNT(*) AS orders
FROM catalog.gold.fact_sales
WHERE order_date >= current_date() - 30
GROUP BY order_date ORDER BY order_date
""").toPandas()

# Charts
fig1 = px.line(daily, x="order_date", y="revenue", title="30-Day Revenue")
fig2 = px.bar(daily, x="order_date", y="orders", title="Daily Orders")

# HTML report
template = Template("""<html><body>
<h1>Daily Report - {'{{'} date {'{}'}</h1>
<h2>Revenue: ${'{{'} total_revenue {'{}'}</h2>
{'{{'} chart1 {'{}'}{'{{'} chart2 {'{}'}
</body></html>""")
report = template.render(date=str(daily["order_date"].max()),
    total_revenue=f"{daily['revenue'].sum():,.2f}",
    chart1=fig1.to_html(full_html=False), chart2=fig2.to_html(full_html=False))

with open("/Volumes/catalog/reports/daily/report.html", "w") as f:
    f.write(report)`,
  },
  {
    id: 15,
    category: 'Reporting',
    title: 'Snowflake Sync Pipeline',
    desc: 'Sync data to/from Snowflake',
    stages: [
      'Read from Databricks',
      'Transform for Snowflake',
      'Write to Snowflake',
      'Verify sync',
      'Log audit',
    ],
    code: `# Write to Snowflake
sf_options = {
    "sfUrl": "account.snowflakecomputing.com",
    "sfUser": dbutils.secrets.get("scope", "sf_user"),
    "sfPassword": dbutils.secrets.get("scope", "sf_pass"),
    "sfDatabase": "ANALYTICS_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
}

gold_df = spark.table("catalog.gold.daily_revenue")
gold_df.write.format("snowflake").options(**sf_options) \\
    .option("dbtable", "DAILY_REVENUE") \\
    .mode("overwrite").save()

# Read from Snowflake
sf_df = spark.read.format("snowflake").options(**sf_options) \\
    .option("dbtable", "EXTERNAL_DATA").load()
sf_df.write.format("delta").saveAsTable("catalog.bronze.snowflake_data")`,
  },
  {
    id: 16,
    category: 'Ingestion',
    title: 'Image Data Pipeline',
    desc: 'Process images from landing zone',
    stages: [
      'Read binary files',
      'Extract metadata',
      'Generate embeddings',
      'Store in Delta',
      'Index in vector DB',
    ],
    code: `from pyspark.sql.functions import col, current_timestamp

# Read images as binary
images = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "binaryFile") \\
    .load("/mnt/landing/images/")

# Add metadata
enriched = images.select(
    col("path"), col("length").alias("file_size"),
    col("modificationTime").alias("file_modified"),
    col("content"),
    current_timestamp().alias("_ingest_ts")
)

enriched.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/images_bronze") \\
    .toTable("catalog.bronze.raw_images")

# Generate embeddings (batch)
from transformers import CLIPModel, CLIPProcessor
model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")`,
  },
  {
    id: 17,
    category: 'Ingestion',
    title: 'Log Data Pipeline',
    desc: 'Parse and analyze log files',
    stages: [
      'Read log files',
      'Parse with regex',
      'Extract fields',
      'Write to Bronze',
      'Analyze patterns',
    ],
    code: `from pyspark.sql.functions import regexp_extract, col

# Read log files
logs = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "text") \\
    .load("/mnt/landing/logs/")

# Parse Apache log format
parsed = logs.select(
    regexp_extract("value", r"(\\d+\\.\\d+\\.\\d+\\.\\d+)", 1).alias("ip"),
    regexp_extract("value", r"\\[(.*?)\\]", 1).alias("timestamp"),
    regexp_extract("value", r'"(GET|POST|PUT|DELETE) (.*?) HTTP', 1).alias("method"),
    regexp_extract("value", r'"(GET|POST|PUT|DELETE) (.*?) HTTP', 2).alias("path"),
    regexp_extract("value", r'" (\\d{3}) ', 1).cast("int").alias("status_code"),
    regexp_extract("value", r'" \\d{3} (\\d+)', 1).cast("long").alias("response_size")
)

parsed.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/logs_bronze") \\
    .toTable("catalog.bronze.access_logs")`,
  },
  {
    id: 18,
    category: 'Ingestion',
    title: 'Text Document Pipeline',
    desc: 'Process text documents for NLP',
    stages: [
      'Read text files',
      'Extract content',
      'Chunk text',
      'Generate embeddings',
      'Store in vector DB',
    ],
    code: `from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import ArrayType, StringType

# Read text documents
docs = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "text") \\
    .option("wholetext", "true") \\
    .load("/mnt/landing/text/")

# Chunk text
@udf(ArrayType(StringType()))
def chunk_text(text, chunk_size=500, overlap=50):
    if not text:
        return []
    chunks = []
    for i in range(0, len(text), chunk_size - overlap):
        chunks.append(text[i:i + chunk_size])
    return chunks

chunked = docs.withColumn("chunks", chunk_text(col("value")))

from pyspark.sql.functions import explode
exploded = chunked.select(
    col("path").alias("source_file"),
    explode("chunks").alias("chunk_text"),
    current_timestamp().alias("_ingest_ts")
)
exploded.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/text_bronze") \\
    .toTable("catalog.bronze.text_chunks")`,
  },
  {
    id: 19,
    category: 'ELT',
    title: 'SCD Type 1 Pipeline',
    desc: 'Overwrite with latest values',
    stages: [
      'Read source',
      'Compare with target',
      'Update changed records',
      'Insert new records',
      'Log changes',
    ],
    code: `from delta.tables import DeltaTable

source = spark.read.format("jdbc") \\
    .option("url", jdbc_url).option("dbtable", "products").load()

target = DeltaTable.forName(spark, "catalog.silver.products")

# SCD Type 1: Simply overwrite with latest
target.alias("t").merge(source.alias("s"), "t.product_id = s.product_id") \\
    .whenMatchedUpdate(set={
        "product_name": "s.product_name",
        "price": "s.price",
        "category": "s.category",
        "updated_at": "current_timestamp()"
    }) \\
    .whenNotMatchedInsert(values={
        "product_id": "s.product_id",
        "product_name": "s.product_name",
        "price": "s.price",
        "category": "s.category",
        "created_at": "current_timestamp()",
        "updated_at": "current_timestamp()"
    }).execute()`,
  },
  {
    id: 20,
    category: 'ELT',
    title: 'Data Reconciliation Pipeline',
    desc: 'Verify data between source and target',
    stages: [
      'Count source records',
      'Count target records',
      'Compare checksums',
      'Identify mismatches',
      'Generate report',
    ],
    code: `from pyspark.sql.functions import count, sum as spark_sum, md5, concat_ws

# Source count
source = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "orders").load()
target = spark.table("catalog.silver.orders")

source_count = source.count()
target_count = target.count()

# Checksum comparison
source_hash = source.withColumn("row_hash", md5(concat_ws("||", *source.columns)))
target_hash = target.withColumn("row_hash", md5(concat_ws("||", *[c for c in target.columns if not c.startswith("_")])))

# Find mismatches
mismatches = source_hash.join(target_hash, "order_id", "full_outer") \\
    .filter("source.row_hash != target.row_hash OR source.order_id IS NULL OR target.order_id IS NULL")

# Report
report = spark.createDataFrame([{
    "check_time": str(current_timestamp()),
    "source_count": source_count, "target_count": target_count,
    "mismatch_count": mismatches.count(),
    "status": "PASS" if mismatches.count() == 0 else "FAIL"
}])
report.write.format("delta").mode("append").saveAsTable("catalog.audit.reconciliation_log")`,
  },
];

const categories = [...new Set(pipelines.map((p) => p.category))];

function PipelineBuilder() {
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const filtered = pipelines.filter((p) => {
    const matchCat = selectedCategory === 'All' || p.category === selectedCategory;
    const matchSearch =
      p.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      p.desc.toLowerCase().includes(searchTerm.toLowerCase());
    return matchCat && matchSearch;
  });

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Pipeline Builder</h1>
          <p>{pipelines.length} end-to-end data pipelines with stages</p>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search pipelines..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '300px' }}
          />
          <select
            className="form-input"
            value={selectedCategory}
            onChange={(e) => setSelectedCategory(e.target.value)}
            style={{ maxWidth: '200px' }}
          >
            <option value="All">All Categories ({pipelines.length})</option>
            {categories.map((c) => (
              <option key={c} value={c}>
                {c} ({pipelines.filter((p) => p.category === c).length})
              </option>
            ))}
          </select>
        </div>
      </div>

      {filtered.map((p) => (
        <div key={p.id} className="card" style={{ marginBottom: '0.75rem' }}>
          <div
            onClick={() => setExpandedId(expandedId === p.id ? null : p.id)}
            style={{ cursor: 'pointer' }}
          >
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem',
                marginBottom: '0.5rem',
              }}
            >
              <span className="badge completed">{p.category}</span>
              <strong>
                #{p.id} — {p.title}
              </strong>
              <span style={{ marginLeft: 'auto', color: 'var(--text-secondary)' }}>
                {expandedId === p.id ? '▼' : '▶'}
              </span>
            </div>
            <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{p.desc}</p>
            <div style={{ display: 'flex', gap: '0.25rem', flexWrap: 'wrap', marginTop: '0.5rem' }}>
              {p.stages.map((s, i) => (
                <React.Fragment key={i}>
                  <span
                    style={{
                      padding: '0.2rem 0.5rem',
                      background: '#eff6ff',
                      borderRadius: '4px',
                      fontSize: '0.7rem',
                      color: '#1e40af',
                    }}
                  >
                    {s}
                  </span>
                  {i < p.stages.length - 1 && (
                    <span style={{ color: '#9ca3af', fontSize: '0.8rem' }}>→</span>
                  )}
                </React.Fragment>
              ))}
            </div>
          </div>
          {expandedId === p.id && (
            <div className="code-block" style={{ marginTop: '1rem' }}>
              {p.code}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

export default PipelineBuilder;
