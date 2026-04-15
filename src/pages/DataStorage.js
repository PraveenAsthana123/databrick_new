import React, { useState } from 'react';

const storageComparison = [
  {
    storage: 'DBFS',
    type: 'Managed',
    useCase: 'Quick prototyping, temp files',
    cost: 'Low',
    performance: 'Medium',
    governance: 'Limited',
    cloud: 'All',
  },
  {
    storage: 'UC Volumes',
    type: 'Managed/External',
    useCase: 'Production file storage with governance',
    cost: 'Medium',
    performance: 'High',
    governance: 'Full (Unity Catalog)',
    cloud: 'All',
  },
  {
    storage: 'ADLS Gen2',
    type: 'External',
    useCase: 'Azure production data lake',
    cost: 'Medium',
    performance: 'High',
    governance: 'Via UC External Locations',
    cloud: 'Azure',
  },
  {
    storage: 'S3',
    type: 'External',
    useCase: 'AWS production data lake',
    cost: 'Medium',
    performance: 'High',
    governance: 'Via UC External Locations',
    cloud: 'AWS',
  },
  {
    storage: 'GCS',
    type: 'External',
    useCase: 'GCP production data lake',
    cost: 'Medium',
    performance: 'High',
    governance: 'Via UC External Locations',
    cloud: 'GCP',
  },
  {
    storage: 'Delta Tables',
    type: 'Managed',
    useCase: 'Structured data with ACID, time travel',
    cost: 'Low',
    performance: 'Very High',
    governance: 'Full (Unity Catalog)',
    cloud: 'All',
  },
  {
    storage: 'External Tables',
    type: 'External',
    useCase: 'Shared data across workspaces/tools',
    cost: 'Varies',
    performance: 'High',
    governance: 'Via UC External Locations',
    cloud: 'All',
  },
  {
    storage: 'Snowflake Stages',
    type: 'Internal/External',
    useCase: 'File staging for bulk load/unload',
    cost: 'Low-Medium',
    performance: 'High',
    governance: 'Snowflake RBAC',
    cloud: 'All',
  },
  {
    storage: 'Snowflake Tables',
    type: 'Managed',
    useCase: 'Warehouse-native structured storage',
    cost: 'Medium-High',
    performance: 'Very High',
    governance: 'Full (Snowflake RBAC + Tags)',
    cloud: 'All',
  },
];

const storageByDataType = [
  {
    title: 'Structured Data',
    icon: 'table',
    recommendation: 'Delta Tables in Unity Catalog',
    desc: 'Best for tabular data with ACID transactions, time travel, and schema enforcement. Use Unity Catalog for governance and access control across teams.',
  },
  {
    title: 'Semi-structured Data',
    icon: 'code',
    recommendation: 'UC Volumes / Snowflake VARIANT',
    desc: 'JSON, XML, Avro files stored in managed or external volumes. Snowflake VARIANT columns handle semi-structured natively with automatic schema detection.',
  },
  {
    title: 'Unstructured (Images/Docs)',
    icon: 'image',
    recommendation: 'Object Storage (S3/ADLS/GCS)',
    desc: 'Binary files like images, PDFs, and videos. Use cloud object storage with Unity Catalog external locations or Snowflake external stages for governed access.',
  },
  {
    title: 'ML Models',
    icon: 'cpu',
    recommendation: 'MLflow Model Registry',
    desc: 'Store, version, and manage ML models with MLflow. Integrates with Unity Catalog for model governance and lineage tracking.',
  },
  {
    title: 'Feature Data',
    icon: 'layers',
    recommendation: 'Feature Store / Feature Engineering',
    desc: 'Centralized repository for ML features. Ensures consistency between training and serving, with point-in-time lookups for training data.',
  },
  {
    title: 'Streaming Data',
    icon: 'activity',
    recommendation: 'Delta Tables with Auto Loader',
    desc: 'Incrementally process streaming files as they land. Auto Loader handles schema inference, evolution, and exactly-once processing. Snowflake Snowpipe for Snowflake-native streaming.',
  },
  {
    title: 'Temporary / Scratch',
    icon: 'clock',
    recommendation: 'DBFS temp / Cluster-local Storage',
    desc: 'Short-lived data for intermediate processing steps. Cluster-local storage (/local_disk0) for fastest access, DBFS temp for cross-notebook sharing.',
  },
  {
    title: 'Snowflake Staging',
    icon: 'upload-cloud',
    recommendation: 'Internal / External Stages',
    desc: 'Use internal stages (@~, @%table, @named_stage) for Snowflake-managed storage, or external stages pointing to S3/ADLS/GCS for bulk data loading and unloading.',
  },
];

const codeExamples = [
  {
    id: 1,
    title: 'Configure ADLS Gen2 Access',
    desc: 'Set up Azure Data Lake Storage Gen2 access using service principal or account key',
    code: `# Option 1: Service Principal (recommended for production)
spark.conf.set("fs.azure.account.auth.type.mystorageaccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mystorageaccount.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mystorageaccount.dfs.core.windows.net",
    dbutils.secrets.get("scope", "client_id"))
spark.conf.set("fs.azure.account.oauth2.client.secret.mystorageaccount.dfs.core.windows.net",
    dbutils.secrets.get("scope", "client_secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mystorageaccount.dfs.core.windows.net",
    "https://login.microsoftonline.com/<tenant-id>/oauth2/token")

# Read data from ADLS Gen2
df = spark.read.parquet("abfss://container@mystorageaccount.dfs.core.windows.net/data/")

# Option 2: Account Key (simpler, less secure)
spark.conf.set(
    "fs.azure.account.key.mystorageaccount.dfs.core.windows.net",
    dbutils.secrets.get("scope", "adls_key")
)
df = spark.read.csv("abfss://container@mystorageaccount.dfs.core.windows.net/files/data.csv",
    header=True, inferSchema=True)`,
  },
  {
    id: 2,
    title: 'Configure S3 Access',
    desc: 'Set up AWS S3 access using instance profile or access keys',
    code: `# Option 1: Instance Profile (recommended - configured at cluster level)
# No code needed if instance profile is attached to cluster
df = spark.read.parquet("s3://my-bucket/data/")

# Option 2: Access Keys via Databricks Secrets
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get("aws_scope", "access_key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get("aws_scope", "secret_key"))

# Option 3: Assume IAM Role
spark.conf.set("fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
spark.conf.set("fs.s3a.assumed.role.arn", "arn:aws:iam::123456789:role/MyRole")

# Read data from S3
df = spark.read.format("delta").load("s3://my-bucket/delta-table/")
df = spark.read.json("s3://my-bucket/json-data/")

# Write back to S3
df.write.format("parquet").mode("overwrite").save("s3://my-bucket/output/")`,
  },
  {
    id: 3,
    title: 'Create Unity Catalog Volume',
    desc: 'Create managed and external volumes in Unity Catalog for file storage',
    code: `-- SQL: Create a managed volume (Databricks manages the storage)
CREATE VOLUME IF NOT EXISTS my_catalog.my_schema.managed_vol
COMMENT 'Managed volume for landing zone files';

-- SQL: Create an external volume (you manage the storage location)
CREATE EXTERNAL VOLUME IF NOT EXISTS my_catalog.my_schema.external_vol
LOCATION 's3://my-bucket/volumes/external_vol'
COMMENT 'External volume pointing to S3';

# PySpark: Upload files to volume using dbutils
dbutils.fs.cp(
    "file:/tmp/local_file.csv",
    "/Volumes/my_catalog/my_schema/managed_vol/local_file.csv"
)

# List files in volume
display(dbutils.fs.ls("/Volumes/my_catalog/my_schema/managed_vol/"))

# Read files from volume
df = spark.read.format("csv") \\
    .option("header", "true") \\
    .load("/Volumes/my_catalog/my_schema/managed_vol/local_file.csv")

# Write files to volume
df.toPandas().to_csv(
    "/Volumes/my_catalog/my_schema/managed_vol/output.csv", index=False
)`,
  },
  {
    id: 4,
    title: 'Create Managed Delta Table',
    desc: 'Create managed Delta tables within Unity Catalog with full governance',
    code: `-- SQL: Create managed Delta table with partitioning and properties
CREATE TABLE IF NOT EXISTS my_catalog.bronze.transactions (
    transaction_id STRING NOT NULL,
    customer_id STRING,
    amount DOUBLE,
    currency STRING DEFAULT 'USD',
    transaction_date TIMESTAMP,
    status STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Raw transaction data from payment gateway'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
)
PARTITIONED BY (status);

# PySpark: Create managed table from DataFrame
df = spark.read.csv(
    "/Volumes/my_catalog/landing/vol/transactions.csv",
    header=True, inferSchema=True
)
df.write.format("delta") \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .saveAsTable("my_catalog.bronze.transactions")

# Enable Change Data Feed for downstream consumers
ALTER TABLE my_catalog.bronze.transactions
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');`,
  },
  {
    id: 5,
    title: 'Snowflake Internal & External Stages',
    desc: 'Create Snowflake stages for bulk data loading and unloading',
    code: `-- Create a named internal stage (Snowflake-managed storage)
CREATE OR REPLACE STAGE my_db.my_schema.my_internal_stage
  FILE_FORMAT = (TYPE = 'PARQUET')
  COMMENT = 'Internal stage for ETL file staging';

-- Create an external stage pointing to S3
CREATE OR REPLACE STAGE my_db.my_schema.s3_external_stage
  URL = 's3://my-bucket/staging/'
  STORAGE_INTEGRATION = my_s3_integration
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
  COMMENT = 'External stage for S3 data ingestion';

-- Create an external stage pointing to Azure
CREATE OR REPLACE STAGE my_db.my_schema.azure_external_stage
  URL = 'azure://myaccount.blob.core.windows.net/container/path/'
  STORAGE_INTEGRATION = my_azure_integration
  FILE_FORMAT = (TYPE = 'JSON');

-- Upload file to internal stage
PUT file:///tmp/data.csv @my_db.my_schema.my_internal_stage AUTO_COMPRESS=TRUE;

-- List files in stage
LIST @my_db.my_schema.my_internal_stage;

-- Load data from stage into table
COPY INTO my_db.my_schema.transactions
  FROM @my_db.my_schema.s3_external_stage
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
  ON_ERROR = 'CONTINUE'
  PURGE = TRUE;`,
  },
  {
    id: 6,
    title: 'Snowflake Tables & Data Types',
    desc: 'Create Snowflake tables with clustering, VARIANT columns, and Snowpipe ingestion',
    code: `-- Create a clustered Snowflake table
CREATE OR REPLACE TABLE my_db.analytics.customer_events (
    event_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    event_type STRING,
    event_data VARIANT,           -- Semi-structured JSON column
    event_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    partition_date DATE DEFAULT CURRENT_DATE()
)
CLUSTER BY (partition_date, customer_id)
COMMENT = 'Customer event stream with semi-structured payload';

-- Insert with VARIANT data
INSERT INTO my_db.analytics.customer_events (event_id, customer_id, event_type, event_data)
SELECT
    UUID_STRING(),
    '12345',
    'purchase',
    PARSE_JSON('{"item":"laptop","price":999.99,"category":"electronics"}');

-- Query VARIANT column
SELECT
    event_id,
    event_data:item::STRING AS item_name,
    event_data:price::FLOAT AS price
FROM my_db.analytics.customer_events
WHERE event_type = 'purchase';

-- Set up Snowpipe for continuous ingestion
CREATE OR REPLACE PIPE my_db.my_schema.events_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO my_db.analytics.customer_events
  FROM @my_db.my_schema.s3_external_stage
  FILE_FORMAT = (TYPE = 'JSON');`,
  },
  {
    id: 7,
    title: 'MLflow Model Storage Setup',
    desc: 'Configure MLflow Model Registry with Unity Catalog for model versioning and governance',
    code: `import mlflow
from mlflow.models import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Set registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Train and log model
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
model = RandomForestClassifier(n_estimators=100)
model.fit(X_train, y_train)
signature = infer_signature(X_train, model.predict(X_train))

with mlflow.start_run():
    mlflow.log_params({"n_estimators": 100})
    mlflow.log_metric("accuracy", model.score(X_test, y_test))

    # Log model to Unity Catalog
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        signature=signature,
        registered_model_name="my_catalog.ml_models.fraud_detector"
    )

# Load model for inference
model_uri = "models:/my_catalog.ml_models.fraud_detector/1"
loaded_model = mlflow.sklearn.load_model(model_uri)

# Set model alias for deployment stages
from mlflow import MlflowClient
client = MlflowClient()
client.set_registered_model_alias(
    name="my_catalog.ml_models.fraud_detector",
    alias="production",
    version=1
)`,
  },
  {
    id: 8,
    title: 'Feature Store Configuration',
    desc: 'Create and manage feature tables using Databricks Feature Store with Unity Catalog',
    code: `from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Create feature table from DataFrame
customer_features_df = spark.sql("""
    SELECT
        customer_id,
        count(*) as total_orders,
        sum(amount) as total_spend,
        avg(amount) as avg_order_value,
        max(order_date) as last_order_date,
        datediff(current_date(), max(order_date)) as days_since_last_order
    FROM my_catalog.silver.orders
    GROUP BY customer_id
""")

# Register as feature table in Unity Catalog
fe.create_table(
    name="my_catalog.features.customer_features",
    primary_keys=["customer_id"],
    df=customer_features_df,
    description="Aggregated customer purchase behavior features"
)

# Update feature table with new data
fe.write_table(
    name="my_catalog.features.customer_features",
    df=updated_features_df,
    mode="merge"
)

# Use features for training with FeatureLookup
from databricks.feature_engineering import FeatureLookup

training_set = fe.create_training_set(
    df=labels_df,  # DataFrame with customer_id and label
    feature_lookups=[
        FeatureLookup(
            table_name="my_catalog.features.customer_features",
            lookup_key="customer_id"
        )
    ],
    label="is_churned"
)
training_df = training_set.load_df()`,
  },
];

const bestPractices = [
  'Use Unity Catalog as the default governance layer for all Databricks data assets -- tables, volumes, models, and features.',
  'In Snowflake, use RBAC with database roles and object tagging for governance; apply masking policies on sensitive columns.',
  'Prefer managed tables and volumes over external ones unless data must be shared outside Databricks or Snowflake.',
  'Store credentials in Databricks Secrets or Snowflake storage integrations -- never hardcode keys or connection strings.',
  'Use Delta format for all structured data in Databricks; use Snowflake native tables for warehouse-centric workloads.',
  'Enable auto-optimize (optimizeWrite + autoCompact) on Delta tables; use automatic clustering in Snowflake.',
  'Migrate away from DBFS mounts toward Unity Catalog external locations for better security and auditing.',
  'Partition Delta tables only when partitions are large (>1 GB each); in Snowflake, use micro-partitions with cluster keys instead.',
  'Set retention policies (delta.logRetentionDuration, Time Travel in Snowflake) to control storage costs and compliance.',
  'Use DBFS or cluster-local storage only for temporary/scratch data that does not need governance.',
  'Organize data in a medallion architecture: bronze (raw), silver (cleaned), gold (aggregated) in both Databricks and Snowflake.',
  'Tag tables and volumes with owners and descriptions to support data discovery and lineage.',
  'Enable Change Data Feed on Delta tables; use Snowflake Streams and Tasks for CDC in Snowflake.',
  'Use Auto Loader (cloudFiles) for incremental file ingestion in Databricks; use Snowpipe for continuous loading in Snowflake.',
  'Regularly run OPTIMIZE and VACUUM on Delta tables; Snowflake handles compaction automatically.',
  'For ML workflows, store features in Feature Store and models in MLflow to ensure reproducibility.',
  'Use Snowflake external stages with storage integrations rather than embedding credentials in stage definitions.',
  'Leverage Snowflake VARIANT columns for semi-structured data (JSON, Avro, Parquet) to avoid pre-flattening.',
];

function DataStorage() {
  const [expandedId, setExpandedId] = useState(null);

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Where to Keep Data</h1>
          <p>
            Storage options, configurations, and best practices across Databricks, PySpark, and
            Snowflake ecosystems
          </p>
        </div>
        <div className="stats-grid">
          <div className="card" style={{ padding: '1rem', textAlign: 'center' }}>
            <div
              style={{
                fontSize: '1.5rem',
                fontWeight: 700,
                color: 'var(--primary-color, #3b82f6)',
              }}
            >
              9
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
              Storage Options
            </div>
          </div>
          <div className="card" style={{ padding: '1rem', textAlign: 'center' }}>
            <div
              style={{
                fontSize: '1.5rem',
                fontWeight: 700,
                color: 'var(--primary-color, #3b82f6)',
              }}
            >
              8
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>Code Examples</div>
          </div>
          <div className="card" style={{ padding: '1rem', textAlign: 'center' }}>
            <div
              style={{
                fontSize: '1.5rem',
                fontWeight: 700,
                color: 'var(--primary-color, #3b82f6)',
              }}
            >
              3
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
              Cloud Providers
            </div>
          </div>
          <div className="card" style={{ padding: '1rem', textAlign: 'center' }}>
            <div
              style={{
                fontSize: '1.5rem',
                fontWeight: 700,
                color: 'var(--primary-color, #3b82f6)',
              }}
            >
              2
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>Platforms</div>
          </div>
        </div>
      </div>

      {/* Overview Section */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.75rem' }}>Overview</h2>
        <p style={{ color: 'var(--text-secondary)', lineHeight: '1.7' }}>
          Modern data platforms like <strong>Databricks</strong> and <strong>Snowflake</strong>{' '}
          provide multiple storage options depending on data type, governance needs, performance
          requirements, and cloud platform. In Databricks, the approach centers on{' '}
          <strong>Unity Catalog</strong> for governance, <strong>Delta Lake</strong> for structured
          data, and <strong>cloud object storage</strong> (S3, ADLS Gen2, GCS) as the underlying
          persistence layer. DBFS remains available for quick prototyping but production workloads
          should use Unity Catalog managed or external tables and volumes. In Snowflake, data is
          stored natively in <strong>micro-partitioned tables</strong> with automatic optimization,
          while <strong>stages</strong> (internal and external) handle file-based data loading and
          unloading. Choosing the right storage depends on data structure, access patterns, cost
          constraints, and whether data needs to be shared across workspaces, warehouses, or with
          external systems.
        </p>
      </div>

      {/* Storage Comparison Table */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.75rem' }}>Storage Comparison</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1rem', fontSize: '0.9rem' }}>
          Comparison of storage options across Databricks and Snowflake ecosystems
        </p>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }}>
            <thead>
              <tr style={{ borderBottom: '2px solid var(--border-color)' }}>
                <th
                  style={{
                    textAlign: 'left',
                    padding: '0.75rem 1rem',
                    color: 'var(--text-secondary)',
                  }}
                >
                  Storage
                </th>
                <th
                  style={{
                    textAlign: 'left',
                    padding: '0.75rem 1rem',
                    color: 'var(--text-secondary)',
                  }}
                >
                  Type
                </th>
                <th
                  style={{
                    textAlign: 'left',
                    padding: '0.75rem 1rem',
                    color: 'var(--text-secondary)',
                  }}
                >
                  Use Case
                </th>
                <th
                  style={{
                    textAlign: 'left',
                    padding: '0.75rem 1rem',
                    color: 'var(--text-secondary)',
                  }}
                >
                  Cost
                </th>
                <th
                  style={{
                    textAlign: 'left',
                    padding: '0.75rem 1rem',
                    color: 'var(--text-secondary)',
                  }}
                >
                  Performance
                </th>
                <th
                  style={{
                    textAlign: 'left',
                    padding: '0.75rem 1rem',
                    color: 'var(--text-secondary)',
                  }}
                >
                  Governance
                </th>
                <th
                  style={{
                    textAlign: 'left',
                    padding: '0.75rem 1rem',
                    color: 'var(--text-secondary)',
                  }}
                >
                  Cloud
                </th>
              </tr>
            </thead>
            <tbody>
              {storageComparison.map((row, idx) => (
                <tr key={idx} style={{ borderBottom: '1px solid var(--border-color)' }}>
                  <td style={{ padding: '0.75rem 1rem', fontWeight: 600 }}>{row.storage}</td>
                  <td style={{ padding: '0.75rem 1rem' }}>
                    <span
                      className={`badge ${
                        row.type === 'Managed'
                          ? 'running'
                          : row.type === 'External'
                            ? 'pending'
                            : 'warning'
                      }`}
                    >
                      {row.type}
                    </span>
                  </td>
                  <td style={{ padding: '0.75rem 1rem', color: 'var(--text-secondary)' }}>
                    {row.useCase}
                  </td>
                  <td style={{ padding: '0.75rem 1rem' }}>{row.cost}</td>
                  <td style={{ padding: '0.75rem 1rem' }}>
                    <strong
                      style={{
                        color:
                          row.performance === 'Very High'
                            ? 'var(--success-color, #10b981)'
                            : 'inherit',
                      }}
                    >
                      {row.performance}
                    </strong>
                  </td>
                  <td
                    style={{
                      padding: '0.75rem 1rem',
                      color: 'var(--text-secondary)',
                      fontSize: '0.85rem',
                    }}
                  >
                    {row.governance}
                  </td>
                  <td style={{ padding: '0.75rem 1rem' }}>
                    <span className="badge">{row.cloud}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Storage by Data Type Cards */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.75rem' }}>Storage by Data Type</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1rem', fontSize: '0.9rem' }}>
          Recommended storage for each category of data across both platforms
        </p>
        <div className="grid-2">
          {storageByDataType.map((item, idx) => (
            <div
              key={idx}
              className="card"
              style={{
                padding: '1.25rem',
                border: '1px solid var(--border-color)',
                borderRadius: '8px',
              }}
            >
              <h3 style={{ fontSize: '1rem', marginBottom: '0.5rem' }}>{item.title}</h3>
              <span
                className="badge running"
                style={{ marginBottom: '0.5rem', display: 'inline-block' }}
              >
                {item.recommendation}
              </span>
              <p
                style={{
                  color: 'var(--text-secondary)',
                  fontSize: '0.85rem',
                  lineHeight: '1.6',
                  marginTop: '0.5rem',
                }}
              >
                {item.desc}
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* Code Examples */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.75rem' }}>Storage Setup Examples</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1rem', fontSize: '0.9rem' }}>
          {codeExamples.length} PySpark and SQL code examples for configuring data storage in
          Databricks and Snowflake
        </p>
        <div className="grid-3" style={{ marginBottom: '1rem' }}>
          {codeExamples.map((example) => (
            <div
              key={example.id}
              className="card"
              onClick={() => setExpandedId(expandedId === example.id ? null : example.id)}
              style={{
                padding: '0.75rem 1rem',
                cursor: 'pointer',
                border:
                  expandedId === example.id
                    ? '2px solid var(--primary-color, #3b82f6)'
                    : '1px solid var(--border-color)',
                borderRadius: '8px',
                transition: 'border-color 0.2s',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <span className="badge running">#{example.id}</span>
                <strong style={{ fontSize: '0.85rem' }}>{example.title}</strong>
              </div>
            </div>
          ))}
        </div>

        {/* Expanded Code Block */}
        {expandedId && (
          <div style={{ marginTop: '0.5rem' }}>
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: '0.5rem',
              }}
            >
              <div>
                <strong style={{ fontSize: '1rem' }}>
                  {codeExamples.find((e) => e.id === expandedId)?.title}
                </strong>
                <p
                  style={{
                    color: 'var(--text-secondary)',
                    fontSize: '0.85rem',
                    marginTop: '0.25rem',
                  }}
                >
                  {codeExamples.find((e) => e.id === expandedId)?.desc}
                </p>
              </div>
              <span
                onClick={() => setExpandedId(null)}
                style={{
                  cursor: 'pointer',
                  fontSize: '1.2rem',
                  color: 'var(--text-secondary)',
                  padding: '0.25rem 0.5rem',
                }}
              >
                ✕
              </span>
            </div>
            <div className="code-block">{codeExamples.find((e) => e.id === expandedId)?.code}</div>
          </div>
        )}
      </div>

      {/* Best Practices */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.75rem' }}>Best Practices</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1rem', fontSize: '0.9rem' }}>
          {bestPractices.length} guidelines for data storage across Databricks and Snowflake
        </p>
        <ul style={{ margin: 0, paddingLeft: '1.25rem' }}>
          {bestPractices.map((practice, idx) => (
            <li
              key={idx}
              style={{
                color: 'var(--text-secondary)',
                fontSize: '0.9rem',
                lineHeight: '1.6',
                marginBottom: '0.5rem',
              }}
            >
              {practice}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default DataStorage;
