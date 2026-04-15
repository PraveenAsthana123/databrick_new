import React, { useState } from 'react';

const bronzeExamples = [
  {
    id: 1,
    title: 'Auto Loader to Bronze',
    desc: 'Incremental file ingestion',
    code: `df = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "json") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/bronze") \\
    .load("/mnt/landing/raw/")

from pyspark.sql.functions import current_timestamp, input_file_name
enriched = df.withColumn("_ingest_timestamp", current_timestamp()) \\
             .withColumn("_source_file", input_file_name())
enriched.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/bronze") \\
    .toTable("catalog.bronze.raw_events")`,
  },
  {
    id: 2,
    title: 'COPY INTO Bronze',
    desc: 'Idempotent batch ingestion',
    code: `spark.sql("""
COPY INTO catalog.bronze.transactions
FROM '/mnt/landing/transactions/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
""")`,
  },
  {
    id: 3,
    title: 'Kafka to Bronze',
    desc: 'Stream Kafka to Delta',
    code: `df = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "broker:9092") \\
    .option("subscribe", "events").load()
bronze = df.select(
    col("key").cast("string"), col("value").cast("string"),
    col("topic"), col("partition"), col("offset"),
    col("timestamp").alias("kafka_ts"), current_timestamp().alias("_ingest_ts")
)
bronze.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/kafka_bronze") \\
    .toTable("catalog.bronze.kafka_events")`,
  },
  {
    id: 4,
    title: 'JDBC to Bronze',
    desc: 'Database extraction',
    code: `df = spark.read.format("jdbc") \\
    .option("url", "jdbc:postgresql://host:5432/db") \\
    .option("dbtable", "public.customers") \\
    .option("user", dbutils.secrets.get("s","u")) \\
    .option("password", dbutils.secrets.get("s","p")).load()
df.withColumn("_ingest_ts", current_timestamp()) \\
  .withColumn("_source", lit("postgresql")) \\
  .write.format("delta").mode("append") \\
  .saveAsTable("catalog.bronze.pg_customers")`,
  },
  {
    id: 5,
    title: 'Quarantine Bad Records',
    desc: 'Separate good/bad data',
    code: `df = spark.read.format("csv") \\
    .option("mode", "PERMISSIVE") \\
    .option("columnNameOfCorruptRecord", "_corrupt_record") \\
    .schema(schema) \\
    .load("/mnt/landing/data/")
good = df.filter("_corrupt_record IS NULL").drop("_corrupt_record")
bad = df.filter("_corrupt_record IS NOT NULL")
good.write.format("delta").saveAsTable("catalog.bronze.clean_data")
bad.write.format("delta").saveAsTable("catalog.bronze.quarantine")`,
  },
];

const silverExamples = [
  {
    id: 1,
    title: 'Cleanse & Deduplicate',
    desc: 'Clean bronze data',
    code: `from pyspark.sql.functions import col, trim, lower, row_number
from pyspark.sql.window import Window

bronze = spark.table("catalog.bronze.raw_customers")
cleaned = bronze.withColumn("name", trim(col("name"))) \\
    .withColumn("email", lower(trim(col("email")))) \\
    .filter(col("customer_id").isNotNull())

w = Window.partitionBy("customer_id").orderBy(col("_ingest_ts").desc())
deduped = cleaned.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")
deduped.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.customers")`,
  },
  {
    id: 2,
    title: 'Join & Enrich',
    desc: 'Combine bronze tables',
    code: `orders = spark.table("catalog.bronze.orders")
customers = spark.table("catalog.silver.customers")
products = spark.table("catalog.bronze.products")

enriched = orders.join(customers, "customer_id", "left") \\
    .join(products, "product_id", "left") \\
    .select("order_id","order_date","customer_name","product_name","category",
            "quantity","unit_price",(col("quantity")*col("unit_price")).alias("total"))
enriched.write.format("delta").saveAsTable("catalog.silver.enriched_orders")`,
  },
  {
    id: 3,
    title: 'SCD Type 2',
    desc: 'Slowly Changing Dimension',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

target = DeltaTable.forName(spark, "catalog.silver.customers_scd2")
target.alias("t").merge(
    incoming.alias("s"), "t.id = s.id AND t.is_current = true"
).whenMatchedUpdate(
    condition="t.name != s.name OR t.email != s.email",
    set={"is_current": lit(False), "end_date": current_timestamp()}
).execute()

new = incoming.withColumn("is_current", lit(True)) \\
    .withColumn("start_date", current_timestamp()) \\
    .withColumn("end_date", lit(None).cast("timestamp"))
new.write.format("delta").mode("append").saveAsTable("catalog.silver.customers_scd2")`,
  },
  {
    id: 4,
    title: 'Delta MERGE Upsert',
    desc: 'Incremental upsert',
    code: `from delta.tables import DeltaTable

incoming = spark.table("catalog.bronze.orders_new")
target = DeltaTable.forName(spark, "catalog.silver.orders")
target.alias("t").merge(incoming.alias("s"), "t.order_id = s.order_id") \\
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`,
  },
  {
    id: 5,
    title: 'DLT Quality Gates',
    desc: 'Data quality expectations',
    code: `import dlt

@dlt.table(comment="Validated silver orders")
@dlt.expect("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_date", "order_date >= '2020-01-01'")
def silver_orders():
    return dlt.read("bronze_orders").dropDuplicates(["order_id"])`,
  },
];

const goldExamples = [
  {
    id: 1,
    title: 'Star Schema Fact Table',
    desc: 'Business fact table',
    code: `spark.sql("""
CREATE OR REPLACE TABLE catalog.gold.fact_sales AS
SELECT o.order_id, o.order_date, d.date_key, c.customer_key, p.product_key,
       o.quantity, o.total_amount, o.total_amount - (o.quantity * p.cost) AS profit
FROM catalog.silver.enriched_orders o
JOIN catalog.gold.dim_date d ON o.order_date = d.full_date
JOIN catalog.gold.dim_customer c ON o.customer_id = c.customer_id
JOIN catalog.gold.dim_product p ON o.product_id = p.product_id
""")`,
  },
  {
    id: 2,
    title: 'KPI Metrics Table',
    desc: 'Pre-computed KPIs',
    code: `spark.sql("""
CREATE OR REPLACE TABLE catalog.gold.daily_kpis AS
SELECT current_date() AS report_date,
    SUM(amount) AS total_revenue,
    COUNT(DISTINCT customer_id) AS active_customers,
    AVG(amount) AS avg_order_value,
    SUM(profit)/SUM(amount)*100 AS profit_margin_pct
FROM catalog.gold.fact_sales
WHERE order_date >= current_date() - INTERVAL 30 DAYS
""")`,
  },
  {
    id: 3,
    title: 'ML Feature Table',
    desc: 'Feature Store features',
    code: `from databricks.feature_engineering import FeatureEngineeringClient

features = spark.sql("""
SELECT customer_id, COUNT(*) AS total_orders,
    SUM(total_amount) AS lifetime_value,
    AVG(total_amount) AS avg_order_value,
    DATEDIFF(current_date(), MAX(order_date)) AS recency
FROM catalog.silver.enriched_orders GROUP BY customer_id
""")

fe = FeatureEngineeringClient()
fe.create_table(name="catalog.gold.customer_features",
    primary_keys=["customer_id"], df=features)`,
  },
  {
    id: 4,
    title: 'Gold Table Optimization',
    desc: 'Performance tuning',
    code: `OPTIMIZE catalog.gold.fact_sales ZORDER BY (order_date, customer_key);
ALTER TABLE catalog.gold.fact_sales CLUSTER BY (order_date, region);
ANALYZE TABLE catalog.gold.fact_sales COMPUTE STATISTICS FOR ALL COLUMNS;
ALTER TABLE catalog.gold.fact_sales SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);`,
  },
  {
    id: 5,
    title: 'BI Export View',
    desc: 'Serve to Power BI/Tableau',
    code: `spark.sql("""
CREATE OR REPLACE VIEW catalog.gold.bi_revenue AS
SELECT d.year, d.quarter, d.month_name, p.category,
       c.region, c.segment,
       SUM(f.total_amount) AS revenue,
       SUM(f.order_count) AS orders
FROM catalog.gold.fact_sales f
JOIN catalog.gold.dim_date d ON f.date_key = d.date_key
JOIN catalog.gold.dim_product p ON f.product_key = p.product_key
JOIN catalog.gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY 1,2,3,4,5,6
""")`,
  },
];

function Medallion() {
  const [activeTab, setActiveTab] = useState('overview');
  const [expandedId, setExpandedId] = useState(null);

  const renderExamples = (examples, color) => (
    <div>
      {examples.map((ex) => (
        <div key={ex.id} className="card" style={{ marginBottom: '0.75rem' }}>
          <div
            onClick={() =>
              setExpandedId(expandedId === `${color}-${ex.id}` ? null : `${color}-${ex.id}`)
            }
            style={{
              cursor: 'pointer',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            <div>
              <strong>
                #{ex.id} — {ex.title}
              </strong>
              <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{ex.desc}</p>
            </div>
            <span>{expandedId === `${color}-${ex.id}` ? '▼' : '▶'}</span>
          </div>
          {expandedId === `${color}-${ex.id}` && (
            <div className="code-block" style={{ marginTop: '1rem' }}>
              {ex.code}
            </div>
          )}
        </div>
      ))}
    </div>
  );

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Medallion Architecture</h1>
          <p>Bronze → Silver → Gold data lakehouse pattern</p>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h3 style={{ marginBottom: '1rem' }}>Data Flow</h3>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '0.5rem',
            flexWrap: 'wrap',
            padding: '1rem 0',
          }}
        >
          {[
            { label: 'Landing Zone', sub: 'Raw files', bg: '#e5e7eb', fg: '#374151' },
            { label: 'Bronze', sub: 'Raw, append-only', bg: '#fde68a', fg: '#92400e' },
            { label: 'Silver', sub: 'Cleansed, enriched', bg: '#d1d5db', fg: '#374151' },
            { label: 'Gold', sub: 'Business-ready', bg: '#fef3c7', fg: '#b45309' },
          ].map((s, i) => (
            <React.Fragment key={s.label}>
              {i > 0 && <span style={{ fontSize: '1.5rem', color: '#9ca3af' }}>→</span>}
              <div
                style={{
                  padding: '1rem 1.5rem',
                  borderRadius: '8px',
                  background: s.bg,
                  color: s.fg,
                  fontWeight: '700',
                  textAlign: 'center',
                  minWidth: '110px',
                }}
              >
                <div>{s.label}</div>
                <div style={{ fontSize: '0.7rem', fontWeight: '400', marginTop: '0.25rem' }}>
                  {s.sub}
                </div>
              </div>
            </React.Fragment>
          ))}
        </div>
      </div>

      <div className="tabs">
        {['overview', 'bronze', 'silver', 'gold'].map((tab) => (
          <button
            key={tab}
            className={`tab ${activeTab === tab ? 'active' : ''}`}
            onClick={() => {
              setActiveTab(tab);
              setExpandedId(null);
            }}
          >
            {tab.charAt(0).toUpperCase() + tab.slice(1)}
          </button>
        ))}
      </div>

      {activeTab === 'overview' && (
        <div className="grid-3">
          {[
            {
              title: 'Bronze',
              color: '#92400e',
              items: [
                'Raw data, no transforms',
                'Append-only ingestion',
                'Schema-on-read',
                'Metadata enrichment',
                'Quarantine bad records',
              ],
            },
            {
              title: 'Silver',
              color: '#374151',
              items: [
                'Cleansed & validated',
                'Deduplication',
                'Schema enforcement',
                'Joins & enrichment',
                'SCD Type 2 history',
              ],
            },
            {
              title: 'Gold',
              color: '#b45309',
              items: [
                'Business aggregates',
                'Star schema',
                'ML feature tables',
                'KPI/metrics tables',
                'Optimized for BI tools',
              ],
            },
          ].map((l) => (
            <div key={l.title} className="card">
              <h3 style={{ color: l.color, marginBottom: '0.5rem' }}>{l.title} Layer</h3>
              <ul style={{ paddingLeft: '1.2rem', fontSize: '0.85rem', lineHeight: '1.8' }}>
                {l.items.map((i) => (
                  <li key={i}>{i}</li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      )}
      {activeTab === 'bronze' && renderExamples(bronzeExamples, 'bronze')}
      {activeTab === 'silver' && renderExamples(silverExamples, 'silver')}
      {activeTab === 'gold' && renderExamples(goldExamples, 'gold')}
    </div>
  );
}

export default Medallion;
