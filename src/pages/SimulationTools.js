import React, { useState } from 'react';
import FileFormatRunner from '../components/common/FileFormatRunner';

// Generate simulation sample data by category/title
function generateSimulationData(sim, count = 20) {
  const cat = (sim.category || '').toLowerCase();
  const title = (sim.title || '').toLowerCase();

  if (cat.includes('ingestion') && title.includes('csv')) {
    return Array.from({ length: count }, (_, i) => ({
      batch: Math.floor(i / 10),
      order_id: 10000 + i,
      customer_id: Math.floor(Math.random() * 100) + 1,
      amount: +(Math.random() * 500).toFixed(2),
      created_at: `2024-01-15T${String(Math.floor(i / 60) % 24).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}:00Z`,
    }));
  }
  if (title.includes('stream') || title.includes('event')) {
    const types = ['click', 'view', 'purchase', 'search'];
    return Array.from({ length: count }, (_, i) => ({
      timestamp: `2024-01-15T12:${String(Math.floor(i / 60)).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}Z`,
      event_id: `evt-${1000 + i}`,
      event_type: types[i % 4],
      user_id: Math.floor(Math.random() * 10000) + 1,
      page_url: `/page/${Math.floor(Math.random() * 20)}`,
    }));
  }
  if (title.includes('cdc')) {
    const ops = ['I', 'U', 'U', 'D'];
    return Array.from({ length: count }, (_, i) => ({
      customer_id: 1000 + i,
      name: `Customer_${1000 + i}`,
      email: `user${1000 + i}@example.com`,
      operation: ops[i % 4],
      change_ts: `2024-01-15T12:${String(Math.floor(i / 60)).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}Z`,
    }));
  }
  // Default for other categories
  return Array.from({ length: count }, (_, i) => ({
    sim_id: i + 1,
    category: sim.category,
    metric_name: ['throughput', 'latency', 'error_rate', 'cpu'][i % 4],
    metric_value: +(Math.random() * 100).toFixed(2),
    timestamp: `2024-01-15T12:${String(i % 60).padStart(2, '0')}:00Z`,
    status: ['ok', 'warn', 'ok', 'ok'][i % 4],
  }));
}

const simulations = [
  {
    id: 1,
    category: 'Ingestion',
    title: 'Simulate CSV Ingestion',
    desc: 'Generate and ingest CSV files to test pipeline',
    code: `# Simulate CSV file arrival in landing zone
from pyspark.sql.functions import rand, expr, current_timestamp
import time

for batch in range(5):
    df = spark.range(10000).select(
        col("id").alias("order_id"),
        (rand() * 100).cast("int").alias("customer_id"),
        (rand() * 500).cast("decimal(10,2)").alias("amount"),
        current_timestamp().alias("created_at")
    )
    df.coalesce(1).write.csv(f"/mnt/landing/csv/batch_{batch}/", header=True, mode="overwrite")
    print(f"Batch {batch} written - {df.count()} records")
    time.sleep(2)  # Simulate file arrival delay

# Now test Auto Loader picks up files
stream = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "csv") \\
    .option("header", "true") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/sim_csv") \\
    .load("/mnt/landing/csv/batch_*/")

stream.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/sim_csv") \\
    .toTable("catalog.bronze.sim_csv_data")`,
  },
  {
    id: 2,
    category: 'Ingestion',
    title: 'Simulate Streaming Events',
    desc: 'Generate streaming events to test pipeline',
    code: `# Simulate streaming events using rate source
stream = spark.readStream.format("rate") \\
    .option("rowsPerSecond", 100) \\
    .load()

from pyspark.sql.functions import expr, struct, to_json
events = stream.select(
    col("timestamp"),
    expr("uuid()").alias("event_id"),
    expr("CASE floor(rand()*4) WHEN 0 THEN 'click' WHEN 1 THEN 'view' WHEN 2 THEN 'purchase' ELSE 'search' END").alias("event_type"),
    expr("floor(rand()*10000) + 1").alias("user_id"),
    expr("concat('/page/', floor(rand()*20))").alias("page_url")
)

query = events.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/sim_stream") \\
    .toTable("catalog.bronze.sim_streaming_events")

# Run for 60 seconds then stop
import time
time.sleep(60)
query.stop()
print(f"Records ingested: {spark.table('catalog.bronze.sim_streaming_events').count()}")`,
  },
  {
    id: 3,
    category: 'Ingestion',
    title: 'Simulate CDC Events',
    desc: 'Generate CDC (insert/update/delete) events',
    code: `from pyspark.sql.functions import expr, lit, current_timestamp

# Initial load
customers = spark.range(1000).select(
    col("id").alias("customer_id"),
    expr("concat('Customer_', id)").alias("name"),
    expr("concat('user', id, '@example.com')").alias("email"),
    lit("I").alias("operation"),
    current_timestamp().alias("change_ts")
)
customers.write.format("delta").saveAsTable("catalog.bronze.sim_cdc_events")

# Simulate updates
updates = spark.range(100).select(
    col("id").alias("customer_id"),
    expr("concat('Updated_Customer_', id)").alias("name"),
    expr("concat('updated', id, '@example.com')").alias("email"),
    lit("U").alias("operation"),
    current_timestamp().alias("change_ts")
)
updates.write.format("delta").mode("append").saveAsTable("catalog.bronze.sim_cdc_events")

# Simulate deletes
deletes = spark.range(50, 60).select(
    col("id").alias("customer_id"),
    lit(None).alias("name"), lit(None).alias("email"),
    lit("D").alias("operation"),
    current_timestamp().alias("change_ts")
)
deletes.write.format("delta").mode("append").saveAsTable("catalog.bronze.sim_cdc_events")`,
  },
  {
    id: 4,
    category: 'Processing',
    title: 'Simulate ETL Pipeline',
    desc: 'End-to-end ETL simulation with timing',
    code: `import time

def timed_step(name, func):
    start = time.time()
    result = func()
    duration = time.time() - start
    print(f"Step: {name} | Duration: {duration:.2f}s | Rows: {result.count() if hasattr(result, 'count') else 'N/A'}")
    return result

# Step 1: Extract
raw = timed_step("Extract", lambda: spark.table("catalog.bronze.sample_orders"))

# Step 2: Transform
cleaned = timed_step("Clean", lambda: raw.dropDuplicates(["order_id"]).filter("amount > 0"))

# Step 3: Enrich
enriched = timed_step("Enrich", lambda: cleaned.join(
    spark.table("catalog.bronze.sample_customers"), "customer_id", "left"))

# Step 4: Aggregate
gold = timed_step("Aggregate", lambda: enriched.groupBy("order_date").agg(
    {"amount": "sum", "order_id": "count"}))

# Step 5: Write
timed_step("Write", lambda: gold.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.sim_daily_metrics") or gold)

print("Pipeline complete!")`,
  },
  {
    id: 5,
    category: 'Processing',
    title: 'Simulate Data Quality Issues',
    desc: 'Generate data with known quality problems',
    code: `from pyspark.sql.functions import when, rand, lit

# Generate data with intentional quality issues
df = spark.range(10000).select(
    col("id"),
    when(rand() < 0.1, None).otherwise(expr("concat('Name_', id)")).alias("name"),  # 10% null
    when(rand() < 0.05, "invalid-email").otherwise(expr("concat('user', id, '@test.com')")).alias("email"),  # 5% invalid
    when(rand() < 0.03, -1).otherwise((rand() * 500).cast("decimal(10,2)")).alias("amount"),  # 3% negative
    when(rand() < 0.02, "2099-01-01").otherwise(expr("date_sub(current_date(), floor(rand()*365))")).alias("date"),  # 2% future dates
)

# Add duplicates (5%)
dups = df.limit(500)
dirty_data = df.union(dups)
dirty_data.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.sim_dirty_data")

# Now run quality checks
total = dirty_data.count()
null_names = dirty_data.filter("name IS NULL").count()
invalid_emails = dirty_data.filter("email = 'invalid-email'").count()
negative_amounts = dirty_data.filter("amount < 0").count()
dup_count = total - dirty_data.dropDuplicates(["id"]).count()

print(f"Total: {total}, Null names: {null_names}, Invalid emails: {invalid_emails}")
print(f"Negative amounts: {negative_amounts}, Duplicates: {dup_count}")`,
  },
  {
    id: 6,
    category: 'Processing',
    title: 'Simulate Schema Evolution',
    desc: 'Test schema changes in pipeline',
    code: `# Version 1: Original schema
v1 = spark.range(1000).select(
    col("id"), expr("concat('Name_', id)").alias("name"), (rand() * 100).alias("score"))
v1.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.sim_schema_evolution")

# Version 2: Add new column
v2 = spark.range(1000, 2000).select(
    col("id"), expr("concat('Name_', id)").alias("name"), (rand() * 100).alias("score"),
    expr("concat('user', id, '@test.com')").alias("email"))  # New column!
v2.write.format("delta").mode("append").option("mergeSchema", "true") \\
    .saveAsTable("catalog.bronze.sim_schema_evolution")

# Version 3: Add another column + type change
v3 = spark.range(2000, 3000).select(
    col("id"), expr("concat('Name_', id)").alias("name"),
    (rand() * 100).cast("decimal(10,2)").alias("score"),  # Type widened
    expr("concat('user', id, '@test.com')").alias("email"),
    expr("current_date()").alias("created_at"))  # New column
v3.write.format("delta").mode("append").option("mergeSchema", "true") \\
    .saveAsTable("catalog.bronze.sim_schema_evolution")

# Verify evolution
spark.sql("DESCRIBE HISTORY catalog.bronze.sim_schema_evolution").show()`,
  },
  {
    id: 7,
    category: 'ML',
    title: 'Simulate ML Training Pipeline',
    desc: 'End-to-end ML training simulation',
    code: `from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
import mlflow

# Generate training data
from pyspark.sql.functions import rand, when
data = spark.range(10000).select(
    (rand() * 10).alias("feature1"),
    (rand() * 20).alias("feature2"),
    (randn() * 5 + 10).alias("feature3"),
    when(rand() < 0.3, 1).otherwise(0).alias("label")
)
train, test = data.randomSplit([0.8, 0.2], seed=42)

assembler = VectorAssembler(inputCols=["feature1","feature2","feature3"], outputCol="features")
rf = RandomForestClassifier(numTrees=100, maxDepth=5, seed=42)
pipeline = Pipeline(stages=[assembler, rf])

with mlflow.start_run(run_name="sim_rf_model"):
    model = pipeline.fit(train)
    preds = model.transform(test)
    auc = BinaryClassificationEvaluator().evaluate(preds)
    mlflow.log_metric("auc", auc)
    mlflow.spark.log_model(model, "model")
    print(f"Model AUC: {auc:.4f}")`,
  },
  {
    id: 8,
    category: 'ML',
    title: 'Simulate Model Drift',
    desc: 'Generate drifted data to test monitoring',
    code: `from pyspark.sql.functions import rand, randn

# Baseline data (normal distribution)
baseline = spark.range(10000).select(
    (randn() * 1 + 5).alias("feature1"),   # mean=5, std=1
    (randn() * 2 + 10).alias("feature2"),  # mean=10, std=2
    (randn() * 0.5 + 3).alias("feature3")  # mean=3, std=0.5
)
baseline.write.format("delta").saveAsTable("catalog.audit.sim_baseline")

# Drifted data (shifted distributions)
drifted = spark.range(10000).select(
    (randn() * 1.5 + 7).alias("feature1"),   # mean shifted 5->7, std widened
    (randn() * 3 + 12).alias("feature2"),    # mean shifted 10->12
    (randn() * 0.5 + 3).alias("feature3")   # no drift on this feature
)
drifted.write.format("delta").saveAsTable("catalog.audit.sim_drifted")

# Detect drift
from scipy import stats
bp = baseline.toPandas()
dp = drifted.toPandas()
for col_name in ["feature1", "feature2", "feature3"]:
    ks_stat, p_val = stats.ks_2samp(bp[col_name], dp[col_name])
    drift = "DRIFT" if p_val < 0.05 else "OK"
    print(f"{col_name}: KS={ks_stat:.4f}, p={p_val:.6f} -> {drift}")`,
  },
  {
    id: 9,
    category: 'Performance',
    title: 'Simulate Data Skew',
    desc: 'Test partition skew scenarios',
    code: `from pyspark.sql.functions import when, rand, floor

# Create skewed data (90% goes to partition 0)
skewed = spark.range(1000000).select(
    col("id"),
    when(rand() < 0.9, 0).otherwise(floor(rand() * 100)).alias("partition_key"),
    (rand() * 100).alias("value")
)

# Show partition distribution
skewed.groupBy("partition_key").count().orderBy("count", ascending=False).show(10)

# Compare join performance: skewed vs balanced
import time
other = spark.range(100).select(col("id").alias("partition_key"), (rand()*10).alias("other_value"))

start = time.time()
skewed.join(other, "partition_key").count()
skew_time = time.time() - start

# Fix with salting
from pyspark.sql.functions import concat, lit
salted = skewed.withColumn("salt", floor(rand() * 10))
salted_other = other.crossJoin(spark.range(10).select(col("id").alias("salt")))

start = time.time()
salted.join(salted_other, ["partition_key", "salt"]).count()
salt_time = time.time() - start

print(f"Skewed join: {skew_time:.2f}s, Salted join: {salt_time:.2f}s")`,
  },
  {
    id: 10,
    category: 'Performance',
    title: 'Simulate Large Scale Processing',
    desc: 'Benchmark pipeline at different scales',
    code: `import time

scales = [100000, 500000, 1000000, 5000000]
results = []

for n in scales:
    df = spark.range(n).select(
        col("id"),
        (rand() * 100).alias("value"),
        expr("CASE floor(rand()*10) WHEN 0 THEN 'A' ELSE 'B' END").alias("category")
    )

    # Benchmark: filter + aggregate + write
    start = time.time()
    result = df.filter("value > 50") \\
        .groupBy("category").agg({"value": "avg", "id": "count"})
    result.write.format("delta").mode("overwrite") \\
        .saveAsTable(f"catalog.test.bench_{n}")
    duration = time.time() - start

    results.append({"scale": n, "duration": round(duration, 2)})
    print(f"Scale: {n:>10} rows | Duration: {duration:.2f}s")

bench_df = spark.createDataFrame(results)
bench_df.write.format("delta").saveAsTable("catalog.audit.benchmark_results")`,
  },
];

const categories = [...new Set(simulations.map((s) => s.category))];

function SimulationTools() {
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [expandedId, setExpandedId] = useState(null);

  const filtered = simulations.filter(
    (s) => selectedCategory === 'All' || s.category === selectedCategory
  );

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Simulation Tools</h1>
          <p>{simulations.length} simulation scenarios to test your pipelines</p>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
          <select
            className="form-input"
            value={selectedCategory}
            onChange={(e) => setSelectedCategory(e.target.value)}
            style={{ maxWidth: '200px' }}
          >
            <option value="All">All Categories</option>
            {categories.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </div>
      </div>

      {filtered.map((s) => (
        <div key={s.id} className="card" style={{ marginBottom: '0.75rem' }}>
          <div
            onClick={() => setExpandedId(expandedId === s.id ? null : s.id)}
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
                  gap: '0.5rem',
                  alignItems: 'center',
                  marginBottom: '0.25rem',
                }}
              >
                <span className="badge running">{s.category}</span>
                <strong>
                  #{s.id} — {s.title}
                </strong>
              </div>
              <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{s.desc}</p>
            </div>
            <span>{expandedId === s.id ? '▼' : '▶'}</span>
          </div>
          {expandedId === s.id && (
            <div style={{ marginTop: '1rem' }}>
              <div className="code-block">{s.code}</div>
              <div style={{ marginTop: '0.85rem' }}>
                <FileFormatRunner
                  data={generateSimulationData(s, 20)}
                  slug={`sim-${s.id}-${s.title
                    .toLowerCase()
                    .replace(/[^a-z0-9]+/g, '-')
                    .slice(0, 30)}`}
                  schemaName="SimulationRecord"
                  tableName={`catalog.bronze.sim_${s.category.toLowerCase().replace(/[^a-z0-9]+/g, '_')}_${s.id}`}
                />
              </div>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

export default SimulationTools;
