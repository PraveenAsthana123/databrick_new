import React, { useState } from 'react';

const landingZonePatterns = [
  {
    id: 1,
    title: 'Mount Cloud Storage',
    desc: 'Mount ADLS Gen2, S3, or GCS as a landing zone in Databricks',
    code: `# Mount Azure Data Lake Storage Gen2
dbutils.fs.mount(
    source="wasbs://landing@mystorageaccount.blob.core.windows.net/",
    mount_point="/mnt/landing",
    extra_configs={
        "fs.azure.account.key.mystorageaccount.blob.core.windows.net":
            dbutils.secrets.get(scope="storage", key="account_key")
    }
)

# Mount AWS S3
dbutils.fs.mount(
    source="s3a://my-landing-bucket",
    mount_point="/mnt/landing-s3",
    extra_configs={
        "fs.s3a.access.key": dbutils.secrets.get("aws", "access_key"),
        "fs.s3a.secret.key": dbutils.secrets.get("aws", "secret_key")
    }
)

# Mount Google Cloud Storage
dbutils.fs.mount(
    source="gs://my-landing-bucket",
    mount_point="/mnt/landing-gcs",
    extra_configs={
        "google.cloud.auth.service.account.enable": "true",
        "fs.gs.project.id": "my-project"
    }
)

# Verify mount
dbutils.fs.ls("/mnt/landing/")`,
  },
  {
    id: 2,
    title: 'Configure External Location',
    desc: 'Set up Unity Catalog external locations for landing zone access',
    code: `-- Create storage credential (admin operation)
CREATE STORAGE CREDENTIAL landing_zone_cred
WITH (
    AZURE_MANAGED_IDENTITY = 'managed-identity-id'
    -- For AWS: AWS_IAM_ROLE = 'arn:aws:iam::role/landing-zone-role'
);

-- Create external location pointing to landing zone
CREATE EXTERNAL LOCATION landing_zone
URL 'abfss://landing@storageaccount.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL landing_zone_cred);

-- Grant access to data engineers
GRANT READ FILES, WRITE FILES
ON EXTERNAL LOCATION landing_zone
TO \`data-engineers\`;

-- Verify access
LIST 'abfss://landing@storageaccount.dfs.core.windows.net/';

-- Use in PySpark
df = spark.read.format("csv") \\
    .option("header", "true") \\
    .load("abfss://landing@storageaccount.dfs.core.windows.net/sales/")`,
  },
  {
    id: 3,
    title: 'File Arrival Detection',
    desc: 'Detect and process new files as they arrive in the landing zone',
    code: `from datetime import datetime, timedelta
from pyspark.sql.functions import input_file_name, current_timestamp

# List recently arrived files
landing_path = "/mnt/landing/incoming/"
files = dbutils.fs.ls(landing_path)
recent_files = [
    f for f in files
    if f.modificationTime > (datetime.now() - timedelta(hours=1)).timestamp() * 1000
]
print(f"Found {len(recent_files)} new files in last hour")

# Process new arrivals with file metadata
df = spark.read.format("csv") \\
    .option("header", "true") \\
    .load([f.path for f in recent_files])

df_with_meta = df \\
    .withColumn("_source_file", input_file_name()) \\
    .withColumn("_ingested_at", current_timestamp())

df_with_meta.write.format("delta") \\
    .mode("append") \\
    .saveAsTable("bronze.incoming_data")

# Move processed files to archive
for f in recent_files:
    archive_path = f.path.replace("/incoming/", "/archive/")
    dbutils.fs.mv(f.path, archive_path)`,
  },
  {
    id: 4,
    title: 'Auto Loader Landing Zone',
    desc: 'Use Auto Loader (cloudFiles) for incremental file ingestion from landing zone',
    code: `# Auto Loader automatically detects and processes new files
# Supports: CSV, JSON, Parquet, Avro, ORC, text, binaryFile

df = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "csv") \\
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/landing_schema/") \\
    .option("cloudFiles.inferColumnTypes", "true") \\
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\
    .option("header", "true") \\
    .load("/mnt/landing/sales/")

# Add ingestion metadata
from pyspark.sql.functions import current_timestamp, input_file_name

enriched = df \\
    .withColumn("_ingestion_timestamp", current_timestamp()) \\
    .withColumn("_source_file", input_file_name())

# Write to bronze with checkpointing
enriched.writeStream \\
    .format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/landing_sales/") \\
    .option("mergeSchema", "true") \\
    .trigger(availableNow=True) \\
    .toTable("bronze.sales_raw")

# Monitor Auto Loader progress
# Check cloud_files_state for file tracking
display(spark.sql("SELECT * FROM cloud_files_state('/mnt/checkpoints/landing_sales/')"))`,
  },
  {
    id: 5,
    title: 'Landing Zone Folder Structure',
    desc: 'Organize landing zone with source, date, and batch partitioning',
    code: `# Recommended landing zone folder structure:
# /landing/
#   /{source_system}/
#     /{entity}/
#       /{year}/{month}/{day}/
#         /{batch_id}/
#           data_file.csv
#           _manifest.json
#           _SUCCESS

from datetime import datetime

def create_landing_structure(source, entity, batch_id=None):
    """Create organized landing zone path."""
    now = datetime.now()
    batch = batch_id or now.strftime("%Y%m%d_%H%M%S")
    path = f"/mnt/landing/{source}/{entity}/{now.year}/{now.month:02d}/{now.day:02d}/{batch}/"
    dbutils.fs.mkdirs(path)
    return path

# Example: Write incoming data to structured path
landing_path = create_landing_structure("salesforce", "contacts")
print(f"Landing path: {landing_path}")

# Write manifest file with metadata
import json
manifest = {
    "source": "salesforce",
    "entity": "contacts",
    "record_count": 15000,
    "extracted_at": datetime.now().isoformat(),
    "schema_version": "2.1",
    "checksum": "sha256:abc123..."
}
dbutils.fs.put(f"{landing_path}_manifest.json", json.dumps(manifest, indent=2))

# Read with partition discovery
df = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("recursiveFileLookup", "true") \\
    .load("/mnt/landing/salesforce/contacts/")`,
  },
  {
    id: 6,
    title: 'File Validation on Arrival',
    desc: 'Validate file integrity, format, and schema before promoting to bronze',
    code: `from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import hashlib

def validate_landing_file(file_path, expected_schema=None, min_rows=1, max_rows=10_000_000):
    """Validate a file in the landing zone before processing."""
    errors = []

    # 1. Check file exists and is not empty
    try:
        file_info = dbutils.fs.ls(file_path)
        if file_info[0].size == 0:
            errors.append("File is empty")
            return {"valid": False, "errors": errors}
    except Exception as e:
        errors.append(f"File not found: {e}")
        return {"valid": False, "errors": errors}

    # 2. Read and check row count
    df = spark.read.format("csv").option("header", "true").load(file_path)
    row_count = df.count()
    if row_count < min_rows:
        errors.append(f"Too few rows: {row_count} < {min_rows}")
    if row_count > max_rows:
        errors.append(f"Too many rows: {row_count} > {max_rows}")

    # 3. Schema validation
    if expected_schema:
        actual_cols = set(df.columns)
        expected_cols = set(f.name for f in expected_schema.fields)
        missing = expected_cols - actual_cols
        if missing:
            errors.append(f"Missing columns: {missing}")

    # 4. Null check on required fields
    from pyspark.sql.functions import col, count, when, isnull
    null_counts = df.select([
        count(when(isnull(c), 1)).alias(c) for c in df.columns
    ]).collect()[0]

    # 5. Duplicate check
    dup_count = row_count - df.dropDuplicates().count()
    if dup_count > 0:
        errors.append(f"Found {dup_count} duplicate rows")

    return {
        "valid": len(errors) == 0,
        "row_count": row_count,
        "duplicates": dup_count,
        "errors": errors
    }

# Usage
result = validate_landing_file(
    "/mnt/landing/sales/2024/01/15/batch_001/sales.csv",
    min_rows=100
)
print(f"Validation: {'PASSED' if result['valid'] else 'FAILED'}")
print(f"Rows: {result['row_count']}, Errors: {result['errors']}")`,
  },
  {
    id: 7,
    title: 'Landing Zone Cleanup and Archival',
    desc: 'Archive processed files and purge old data from the landing zone',
    code: `from datetime import datetime, timedelta

def archive_processed_files(landing_path, archive_path, retention_days=30):
    """Move processed files to archive, purge old archives."""
    now = datetime.now()
    archived_count = 0
    purged_count = 0

    # Move processed files to archive (preserve folder structure)
    for file_info in dbutils.fs.ls(landing_path):
        if file_info.name.startswith("_processed_"):
            dest = archive_path + file_info.name.replace("_processed_", "")
            dbutils.fs.mv(file_info.path, dest)
            archived_count += 1

    # Purge old archives beyond retention period
    cutoff = (now - timedelta(days=retention_days)).timestamp() * 1000
    try:
        for file_info in dbutils.fs.ls(archive_path):
            if file_info.modificationTime < cutoff:
                dbutils.fs.rm(file_info.path, recurse=True)
                purged_count += 1
    except Exception:
        pass  # Archive path may not exist yet

    return {"archived": archived_count, "purged": purged_count}

# Run cleanup across all sources
sources = ["salesforce", "erp", "web_analytics", "iot_devices"]
for source in sources:
    result = archive_processed_files(
        landing_path=f"/mnt/landing/{source}/",
        archive_path=f"/mnt/archive/{source}/",
        retention_days=90
    )
    print(f"{source}: archived={result['archived']}, purged={result['purged']}")

# Log cleanup metrics to audit table
cleanup_log = spark.createDataFrame([{
    "cleanup_time": datetime.now().isoformat(),
    "sources_processed": len(sources),
    "retention_days": 90
}])
cleanup_log.write.format("delta").mode("append").saveAsTable("audit.landing_zone_cleanup")`,
  },
  {
    id: 8,
    title: 'Multi-Format Landing Zone',
    desc: 'Handle mixed file formats (CSV, JSON, Parquet, Avro, XML) in a single landing zone',
    code: `from pyspark.sql.functions import input_file_name, current_timestamp, lit

def process_landing_zone(landing_path):
    """Process all supported file formats from landing zone."""
    format_handlers = {
        "csv": lambda p: spark.read.option("header", "true")
                   .option("inferSchema", "true").csv(p),
        "json": lambda p: spark.read.option("multiLine", "true").json(p),
        "parquet": lambda p: spark.read.parquet(p),
        "avro": lambda p: spark.read.format("avro").load(p),
    }

    results = {}
    for file_info in dbutils.fs.ls(landing_path):
        ext = file_info.name.rsplit(".", 1)[-1].lower() if "." in file_info.name else None
        if ext not in format_handlers:
            continue

        try:
            df = format_handlers[ext](file_info.path)
            df_enriched = df \\
                .withColumn("_source_file", input_file_name()) \\
                .withColumn("_file_format", lit(ext)) \\
                .withColumn("_ingested_at", current_timestamp())

            table_name = file_info.name.rsplit(".", 1)[0].lower().replace("-", "_")
            df_enriched.write.format("delta") \\
                .mode("append") \\
                .saveAsTable(f"bronze.landing_{table_name}")

            results[file_info.name] = {"status": "success", "rows": df.count()}
        except Exception as e:
            results[file_info.name] = {"status": "error", "error": str(e)}

    return results

# Process all files in landing zone
results = process_landing_zone("/mnt/landing/incoming/")
for filename, status in results.items():
    print(f"  {filename}: {status['status']}")`,
  },
  {
    id: 9,
    title: 'Encrypted Data Landing',
    desc: 'Decrypt and process encrypted files arriving in the landing zone',
    code: `from cryptography.fernet import Fernet
import base64
import json

def decrypt_landing_file(encrypted_path, output_path, key_scope, key_name):
    """Decrypt an encrypted file from landing zone."""
    # Retrieve encryption key from Databricks secrets
    encryption_key = dbutils.secrets.get(scope=key_scope, key=key_name)
    cipher = Fernet(encryption_key.encode())

    # Read encrypted content
    encrypted_data = dbutils.fs.head(encrypted_path, 100_000_000)  # 100MB max
    decrypted_data = cipher.decrypt(encrypted_data.encode()).decode("utf-8")

    # Write decrypted file
    dbutils.fs.put(output_path, decrypted_data, overwrite=True)
    return output_path

# Process encrypted CSV files from landing zone
encrypted_files = [
    f for f in dbutils.fs.ls("/mnt/landing/encrypted/")
    if f.name.endswith(".enc")
]

for enc_file in encrypted_files:
    decrypted_path = enc_file.path.replace("/encrypted/", "/decrypted/").replace(".enc", "")
    decrypt_landing_file(
        encrypted_path=enc_file.path,
        output_path=decrypted_path,
        key_scope="encryption",
        key_name="landing_zone_key"
    )

# Read decrypted files into bronze
df = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .load("/mnt/landing/decrypted/")

df.write.format("delta").mode("append").saveAsTable("bronze.decrypted_data")

# Clean up decrypted files (security best practice)
dbutils.fs.rm("/mnt/landing/decrypted/", recurse=True)
print("Decrypted files cleaned up after ingestion")`,
  },
  {
    id: 10,
    title: 'Landing Zone Monitoring',
    desc: 'Monitor landing zone health: file counts, sizes, staleness, and SLA compliance',
    code: `from datetime import datetime, timedelta
from pyspark.sql.functions import lit, current_timestamp

def monitor_landing_zone(landing_path, sla_hours=4):
    """Monitor landing zone health and SLA compliance."""
    now = datetime.now()
    sla_cutoff = (now - timedelta(hours=sla_hours)).timestamp() * 1000
    metrics = {
        "path": landing_path,
        "check_time": now.isoformat(),
        "total_files": 0,
        "total_size_mb": 0,
        "stale_files": 0,
        "oldest_file_hours": 0,
        "sla_breached": False,
        "formats": {}
    }

    try:
        files = dbutils.fs.ls(landing_path)
    except Exception:
        metrics["error"] = "Path not accessible"
        return metrics

    for f in files:
        if f.name.startswith("_"):
            continue
        metrics["total_files"] += 1
        metrics["total_size_mb"] += f.size / (1024 * 1024)

        ext = f.name.rsplit(".", 1)[-1] if "." in f.name else "unknown"
        metrics["formats"][ext] = metrics["formats"].get(ext, 0) + 1

        age_hours = (now.timestamp() * 1000 - f.modificationTime) / 3_600_000
        metrics["oldest_file_hours"] = max(metrics["oldest_file_hours"], age_hours)
        if f.modificationTime < sla_cutoff:
            metrics["stale_files"] += 1

    metrics["sla_breached"] = metrics["stale_files"] > 0
    metrics["total_size_mb"] = round(metrics["total_size_mb"], 2)
    metrics["oldest_file_hours"] = round(metrics["oldest_file_hours"], 1)
    return metrics

# Monitor all landing zone sources
sources = ["salesforce", "erp", "web_analytics", "iot_devices", "partner_feeds"]
all_metrics = []
for source in sources:
    m = monitor_landing_zone(f"/mnt/landing/{source}/", sla_hours=4)
    all_metrics.append(m)
    status = "BREACH" if m.get("sla_breached") else "OK"
    print(f"[{status}] {source}: {m['total_files']} files, "
          f"{m['total_size_mb']} MB, oldest={m['oldest_file_hours']}h")

# Write monitoring metrics to Delta table
metrics_df = spark.createDataFrame(all_metrics)
metrics_df.withColumn("_recorded_at", current_timestamp()) \\
    .write.format("delta").mode("append") \\
    .saveAsTable("audit.landing_zone_metrics")

# Alert on SLA breaches
breaches = [m for m in all_metrics if m.get("sla_breached")]
if breaches:
    print(f"\\nALERT: {len(breaches)} landing zones breached SLA!")
    for b in breaches:
        print(f"  - {b['path']}: {b['stale_files']} stale files")`,
  },
];

const storageOptions = [
  {
    name: 'ADLS Gen2',
    provider: 'Azure',
    color: '#0078D4',
    desc: 'Azure Data Lake Storage Gen2 with hierarchical namespace for high-throughput analytics.',
    path: 'abfss://landing@account.dfs.core.windows.net/',
  },
  {
    name: 'S3',
    provider: 'AWS',
    color: '#FF9900',
    desc: 'Amazon S3 buckets with lifecycle policies and cross-region replication support.',
    path: 's3://my-landing-bucket/raw/',
  },
  {
    name: 'GCS',
    provider: 'Google Cloud',
    color: '#4285F4',
    desc: 'Google Cloud Storage with unified object storage and strong consistency.',
    path: 'gs://my-landing-bucket/raw/',
  },
  {
    name: 'DBFS',
    provider: 'Databricks',
    color: '#FF3621',
    desc: 'Databricks File System - abstraction layer over cloud storage for workspace access.',
    path: 'dbfs:/mnt/landing/',
  },
  {
    name: 'Unity Catalog Volumes',
    provider: 'Databricks',
    color: '#00A972',
    desc: 'Governed storage volumes with fine-grained access control and lineage tracking.',
    path: '/Volumes/catalog/schema/landing/',
  },
];

const bestPractices = [
  {
    title: 'Keep raw files immutable',
    detail:
      'Never modify files in the landing zone. Treat them as append-only. If corrections are needed, land a new corrected file and handle deduplication downstream in the bronze layer.',
  },
  {
    title: 'Organize by source and date',
    detail:
      'Use a consistent folder hierarchy: /landing/{source}/{entity}/{year}/{month}/{day}/{batch_id}/ to enable partition pruning and easy troubleshooting.',
  },
  {
    title: 'Retain files for replay capability',
    detail:
      'Keep landing zone files for at least 30-90 days to allow re-ingestion if bronze layer processing needs to be rerun. Archive to cold storage after retention period.',
  },
  {
    title: 'Validate schema before bronze promotion',
    detail:
      'Check column names, data types, null counts, and row counts before moving data to the bronze layer. Quarantine files that fail validation for manual review.',
  },
  {
    title: 'Track file-level metadata',
    detail:
      'Record source file name, arrival time, file size, row count, and checksum for every file processed. Store metadata in an audit table for lineage and debugging.',
  },
];

const externalSources = ['APIs', 'Databases', 'Files', 'Streams', 'IoT'];

function LandingZone() {
  const [expandedId, setExpandedId] = useState(null);

  return (
    <div>
      {/* Page Header */}
      <div className="page-header">
        <div>
          <h1>Landing Zone</h1>
          <p>
            Temporary staging area where raw data lands before entering the medallion architecture
          </p>
        </div>
      </div>

      {/* Overview Card */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h3 style={{ marginBottom: '0.75rem' }}>What is a Landing Zone?</h3>
        <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7 }}>
          The <strong>Landing Zone</strong> is the first touchpoint for data entering your
          lakehouse. It is a temporary staging area in cloud storage where raw data from external
          systems is deposited in its original format before any processing occurs. Data in the
          landing zone is untouched and unvalidated -- it serves as a buffer between external
          sources and the bronze layer of the medallion architecture. Files here are typically
          short-lived, validated for integrity, and then promoted to the bronze layer or quarantined
          if they fail validation.
        </p>
      </div>

      {/* Architecture Diagram */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h3 style={{ marginBottom: '1rem' }}>Landing Zone Architecture</h3>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '1.5rem',
            flexWrap: 'wrap',
            padding: '1.5rem 0',
          }}
        >
          {/* External Sources */}
          <div
            style={{
              border: '2px solid var(--border-color)',
              borderRadius: '8px',
              padding: '1rem 1.5rem',
              textAlign: 'center',
              minWidth: '180px',
              background: 'var(--bg-primary)',
            }}
          >
            <div style={{ fontWeight: 600, marginBottom: '0.75rem', fontSize: '0.95rem' }}>
              External Sources
            </div>
            <div
              style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: '0.35rem',
                justifyContent: 'center',
              }}
            >
              {externalSources.map((src) => (
                <span
                  key={src}
                  className="badge"
                  style={{
                    background: 'var(--bg-secondary)',
                    color: 'var(--text-secondary)',
                    fontSize: '0.75rem',
                  }}
                >
                  {src}
                </span>
              ))}
            </div>
          </div>

          {/* Arrow */}
          <div style={{ fontSize: '1.5rem', color: 'var(--text-secondary)' }}>&#10132;</div>

          {/* Landing Zone */}
          <div
            style={{
              border: '2px solid #FF9900',
              borderRadius: '8px',
              padding: '1rem 1.5rem',
              textAlign: 'center',
              minWidth: '180px',
              background: 'rgba(255, 153, 0, 0.08)',
            }}
          >
            <div style={{ fontWeight: 700, fontSize: '1rem', color: '#FF9900' }}>Landing Zone</div>
            <div
              style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', marginTop: '0.35rem' }}
            >
              Raw / Untouched / Temporary
            </div>
          </div>

          {/* Arrow */}
          <div style={{ fontSize: '1.5rem', color: 'var(--text-secondary)' }}>&#10132;</div>

          {/* Bronze Layer */}
          <div
            style={{
              border: '2px solid #CD7F32',
              borderRadius: '8px',
              padding: '1rem 1.5rem',
              textAlign: 'center',
              minWidth: '180px',
              background: 'rgba(205, 127, 50, 0.08)',
            }}
          >
            <div style={{ fontWeight: 700, fontSize: '1rem', color: '#CD7F32' }}>Bronze Layer</div>
            <div
              style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', marginTop: '0.35rem' }}
            >
              Raw Delta Tables
            </div>
          </div>
        </div>
      </div>

      {/* Storage Options */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h3 style={{ marginBottom: '1rem' }}>Storage Options</h3>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
            gap: '1rem',
          }}
        >
          {storageOptions.map((opt) => (
            <div
              key={opt.name}
              style={{
                border: `1px solid var(--border-color)`,
                borderRadius: '8px',
                padding: '1rem',
                borderLeft: `4px solid ${opt.color}`,
              }}
            >
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  marginBottom: '0.5rem',
                }}
              >
                <strong>{opt.name}</strong>
                <span
                  className="badge"
                  style={{ background: opt.color, color: '#fff', fontSize: '0.7rem' }}
                >
                  {opt.provider}
                </span>
              </div>
              <p
                style={{
                  fontSize: '0.85rem',
                  color: 'var(--text-secondary)',
                  marginBottom: '0.5rem',
                }}
              >
                {opt.desc}
              </p>
              <code
                style={{
                  fontSize: '0.75rem',
                  background: 'var(--bg-secondary)',
                  padding: '0.25rem 0.5rem',
                  borderRadius: '4px',
                  wordBreak: 'break-all',
                }}
              >
                {opt.path}
              </code>
            </div>
          ))}
        </div>
      </div>

      {/* Landing Zone Patterns */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h3 style={{ marginBottom: '0.25rem' }}>Landing Zone Patterns</h3>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginBottom: '1rem' }}>
          {landingZonePatterns.length} PySpark patterns for managing your Databricks landing zone
        </p>
      </div>

      <div className="scenarios-list">
        {landingZonePatterns.map((pattern) => (
          <div key={pattern.id} className="card scenario-card" style={{ marginBottom: '0.75rem' }}>
            <div
              className="scenario-header"
              onClick={() => setExpandedId(expandedId === pattern.id ? null : pattern.id)}
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
                  <span className="badge running">Landing Zone</span>
                  <strong>
                    #{pattern.id} -- {pattern.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                  {pattern.desc}
                </p>
              </div>
              <span style={{ fontSize: '1.2rem', color: 'var(--text-secondary)' }}>
                {expandedId === pattern.id ? '\u25BC' : '\u25B6'}
              </span>
            </div>
            {expandedId === pattern.id && (
              <div className="code-block" style={{ marginTop: '1rem' }}>
                {pattern.code}
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Best Practices */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h3 style={{ marginBottom: '1rem' }}>Best Practices</h3>
        {bestPractices.map((bp, idx) => (
          <div
            key={idx}
            style={{
              padding: '0.75rem 0',
              borderBottom:
                idx < bestPractices.length - 1 ? '1px solid var(--border-color)' : 'none',
            }}
          >
            <strong style={{ fontSize: '0.95rem' }}>
              {idx + 1}. {bp.title}
            </strong>
            <p
              style={{
                color: 'var(--text-secondary)',
                fontSize: '0.85rem',
                marginTop: '0.35rem',
                lineHeight: 1.6,
              }}
            >
              {bp.detail}
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}

export default LandingZone;
