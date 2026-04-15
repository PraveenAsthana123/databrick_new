/**
 * Code Variants — Multiple ways to write each ingestion scenario
 *
 * Each scenario has up to 6 approaches:
 *   1. PySpark DataFrame API
 *   2. Spark SQL
 *   3. Delta Live Tables (DLT)
 *   4. Auto Loader (Structured Streaming)
 *   5. Pandas on Spark
 *   6. dbutils / Notebook
 */

const CODE_VARIANTS = {
  1: {
    title: 'CSV File Ingestion',
    variants: [
      {
        approach: 'Pseudo Code',
        icon: '\ud83d\udccb',
        difficulty: 'Concept',
        pros: 'Understand the logic before writing code',
        cons: 'Not executable — blueprint only',
        code: `ALGORITHM: CSV File Ingestion
=============================

INPUT:
  - Source: s3://bucket/data/*.csv
  - Format: CSV with headers
  - Schema: auto-infer or explicit

STEPS:
  Step 1: CONNECT to cloud storage
    - Authenticate via IAM role / service principal
    - Verify path exists and files are accessible

  Step 2: READ source files
    - Parse CSV with header row
    - Infer column types (string, int, double, date)
    - Handle: multi-line values, quoted fields, escape chars
    - Handle: encoding (UTF-8), null representations ("", "null", "NA")

  Step 3: VALIDATE raw data
    - Check row count > 0
    - Check expected columns exist
    - Log: file count, total rows, schema

  Step 4: ENRICH with metadata
    - Add _ingest_timestamp = current_timestamp()
    - Add _source_file = input_file_name()
    - Add _batch_id = unique batch identifier

  Step 5: WRITE to Bronze (Delta table)
    - Format: Delta Lake (Parquet + transaction log)
    - Mode: overwrite (full refresh) or append (incremental)
    - Enable schema evolution if columns change

  Step 6: VERIFY write
    - Count rows in target table
    - Compare with source count
    - Log success/failure to audit table

OUTPUT:
  - Target: catalog.bronze.csv_data
  - Format: Delta Lake
  - Partitions: none (or by date if large)

ERROR HANDLING:
  - File not found     -> log error, skip, continue
  - Schema mismatch    -> rescue data to quarantine table
  - Write failure      -> retry 3x with backoff, then alert
  - Corrupt file       -> move to /mnt/quarantine/, log

COMPLEXITY: O(n) where n = total rows across all CSV files
SPACE: O(n) in memory during read, then written to disk`,
      },
      {
        approach: 'PySpark DataFrame API',
        icon: '\u26a1',
        difficulty: 'Beginner',
        pros: 'Most common, full control, easy to debug',
        cons: 'Verbose for simple cases',
        code: `# Approach 1: PySpark DataFrame API
from pyspark.sql.functions import current_timestamp, input_file_name

df = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .option("multiLine", "true") \\
    .option("escape", '"') \\
    .load("s3://bucket/data/*.csv")

# Add metadata
df = df.withColumn("_ingest_ts", current_timestamp()) \\
       .withColumn("_source_file", input_file_name())

# Write to Delta
df.write.format("delta") \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .saveAsTable("catalog.bronze.csv_data")

print(f"Ingested {df.count()} rows")`,
      },
      {
        approach: 'Spark SQL (COPY INTO)',
        icon: '\ud83d\udcdd',
        difficulty: 'Beginner',
        pros: 'SQL-native, simple syntax, idempotent',
        cons: 'Less control over transformations',
        code: `-- Approach 2: Spark SQL — COPY INTO (idempotent)
CREATE TABLE IF NOT EXISTS catalog.bronze.csv_data (
  id INT, name STRING, email STRING, amount DOUBLE, date STRING
) USING DELTA;

COPY INTO catalog.bronze.csv_data
FROM 's3://bucket/data/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

SELECT COUNT(*) as total_rows FROM catalog.bronze.csv_data;`,
      },
      {
        approach: 'Delta Live Tables (DLT)',
        icon: '\ud83d\ude80',
        difficulty: 'Intermediate',
        pros: 'Declarative, auto-manages dependencies, built-in quality',
        cons: 'Requires DLT pipeline setup',
        code: `# Approach 3: Delta Live Tables (DLT)
import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="bronze_csv_data",
    comment="Raw CSV data ingested from landing zone",
    table_properties={"quality": "bronze"}
)
def ingest_csv():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3://bucket/data/*.csv")
        .withColumn("_ingest_ts", current_timestamp())
    )

@dlt.table(name="silver_csv_data")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def clean_csv():
    return dlt.read("bronze_csv_data").dropDuplicates(["id"])`,
      },
      {
        approach: 'Auto Loader (Streaming)',
        icon: '\ud83c\udf0a',
        difficulty: 'Intermediate',
        pros: 'Incremental, handles new files automatically, schema evolution',
        cons: 'Requires checkpoint location, streaming complexity',
        code: `# Approach 4: Auto Loader (cloudFiles) — Streaming ingestion
df = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "csv") \\
    .option("header", "true") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/csv_pipeline") \\
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\
    .load("s3://bucket/data/")

df.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/csv_bronze") \\
    .trigger(availableNow=True) \\
    .toTable("catalog.bronze.csv_data")`,
      },
      {
        approach: 'Pandas on Spark',
        icon: '\ud83d\udc3c',
        difficulty: 'Beginner',
        pros: 'Familiar pandas API, good for small-medium data',
        cons: 'Not ideal for very large datasets, limited Spark optimizations',
        code: `# Approach 5: Pandas on Spark (pandas API on Spark)
import pyspark.pandas as ps

# Read CSV using pandas API
pdf = ps.read_csv("s3://bucket/data/orders.csv")

# Familiar pandas operations
pdf["_ingest_ts"] = ps.Timestamp.now()
pdf = pdf.dropna(subset=["id"])
pdf = pdf.drop_duplicates(subset=["id"])

# Convert to Spark and save as Delta
pdf.to_delta("catalog.bronze.csv_data", mode="overwrite")

print(f"Ingested {len(pdf)} rows")`,
      },
      {
        approach: 'dbutils + Notebook',
        icon: '\ud83d\udcd3',
        difficulty: 'Beginner',
        pros: 'Simple, good for ad-hoc ingestion, widget parameters',
        cons: 'Not production-grade, hard to test, no scheduling',
        code: `# Approach 6: dbutils + Notebook (ad-hoc)

# Notebook widget for dynamic parameters
dbutils.widgets.text("source_path", "s3://bucket/data/", "Source Path")
dbutils.widgets.dropdown("mode", "overwrite", ["overwrite", "append"], "Write Mode")

source = dbutils.widgets.get("source_path")
mode = dbutils.widgets.get("mode")

# List files first
files = dbutils.fs.ls(source)
print(f"Found {len(files)} files in {source}")

# Read and write
df = spark.read.option("header", True).csv(source)
df.write.format("delta").mode(mode).saveAsTable("catalog.bronze.csv_data")

# Display results
display(df.limit(10))
print(f"Done! {df.count()} rows written in '{mode}' mode")`,
      },
    ],
  },

  2: {
    title: 'JSON File Ingestion',
    variants: [
      {
        approach: 'Pseudo Code',
        icon: '\ud83d\udccb',
        difficulty: 'Concept',
        pros: 'Understand nested JSON handling logic',
        cons: 'Not executable — blueprint only',
        code: `ALGORITHM: JSON File Ingestion
==============================

INPUT:
  - Source: abfss://container@storage/json/
  - Format: JSON (potentially nested)

STEPS:
  Step 1: DEFINE schema
    - Option A: explicit StructType (recommended for production)
    - Option B: inferSchema (ok for exploration)
    - Handle: nested objects -> StructType
    - Handle: arrays -> ArrayType

  Step 2: READ JSON files
    - Parse with schema enforcement
    - Handle: multiLine JSON (one object spans multiple lines)
    - Handle: corrupt records -> PERMISSIVE mode

  Step 3: FLATTEN nested structures
    - Extract nested fields: address.city -> city column
    - Explode arrays: tags[] -> one row per tag
    - Drop original nested columns

  Step 4: VALIDATE
    - Check: required fields not null
    - Check: JSON parse success rate > 99%
    - Route bad records to quarantine

  Step 5: WRITE to Bronze Delta table

FLOWCHART:
  JSON files -> Read with schema -> Flatten nested ->
  Validate -> [Pass] -> Write Delta
              [Fail] -> Quarantine table

COMPLEXITY: O(n * d) where n=rows, d=nesting depth`,
      },
      {
        approach: 'PySpark DataFrame API',
        icon: '\u26a1',
        difficulty: 'Beginner',
        pros: 'Full schema control, handles nested structures',
        cons: 'Schema definition can be verbose',
        code: `# PySpark — JSON with explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("zip", StringType())
    ]))
])
df = spark.read.schema(schema).json("abfss://container@storage/json/")
df.write.format("delta").saveAsTable("catalog.bronze.json_data")`,
      },
      {
        approach: 'Spark SQL',
        icon: '\ud83d\udcdd',
        difficulty: 'Beginner',
        pros: 'SQL-native, auto schema inference',
        cons: 'Less control over nested parsing',
        code: `-- SQL — Read JSON with auto schema
CREATE TABLE catalog.bronze.json_data
USING DELTA AS
SELECT * FROM json.\`abfss://container@storage/json/\`;

-- Or with COPY INTO
COPY INTO catalog.bronze.json_data
FROM 'abfss://container@storage/json/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true');`,
      },
      {
        approach: 'Delta Live Tables (DLT)',
        icon: '\ud83d\ude80',
        difficulty: 'Intermediate',
        pros: 'Declarative, auto-flatten nested JSON',
        cons: 'DLT pipeline required',
        code: `import dlt

@dlt.table(name="bronze_json_data")
def ingest_json():
    return spark.read.json("abfss://container@storage/json/")

@dlt.table(name="silver_json_flat")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
def flatten_json():
    return (
        dlt.read("bronze_json_data")
        .selectExpr("id", "name", "address.city", "address.zip")
    )`,
      },
      {
        approach: 'Auto Loader',
        icon: '\ud83c\udf0a',
        difficulty: 'Intermediate',
        pros: 'Handles schema evolution for changing JSON structures',
        cons: 'Checkpoint management needed',
        code: `# Auto Loader — JSON with schema evolution
df = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "json") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/json") \\
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\
    .option("multiLine", "true") \\
    .load("abfss://container@storage/json/")

df.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/json_bronze") \\
    .option("mergeSchema", "true") \\
    .trigger(availableNow=True) \\
    .toTable("catalog.bronze.json_data")`,
      },
    ],
  },

  3: {
    title: 'Parquet File Ingestion',
    variants: [
      {
        approach: 'Pseudo Code',
        icon: '\ud83d\udccb',
        difficulty: 'Concept',
        pros: 'Understand partition pruning and conversion',
        cons: 'Not executable — blueprint only',
        code: `ALGORITHM: Parquet File Ingestion
=================================

INPUT:
  - Source: gs://bucket/data/year=2024/month=*/
  - Format: Parquet (columnar, schema embedded)
  - Partitioned: by year and month (Hive-style)

STEPS:
  Step 1: READ Parquet files
    - Schema auto-detected from Parquet metadata
    - Partition columns auto-discovered from path
    - Partition pruning: only read needed partitions
    - Predicate pushdown: filter at file level

  Step 2: CONVERT types if needed
    - Parquet types -> Spark types (usually 1:1)
    - Timestamp handling: UTC normalization

  Step 3: WRITE to Delta with partitioning
    - Maintain same partition scheme (year/month)
    - Or re-partition for query patterns

  ALTERNATIVE: CONVERT TO DELTA (zero-copy!)
    - If Parquet already on cloud storage
    - Just adds Delta transaction log
    - No data movement, instant conversion

FLOWCHART:
  Parquet -> [Already on storage?]
    Yes -> CONVERT TO DELTA (instant, 0 bytes copied)
    No  -> Read -> Write Delta (full copy)

COMPLEXITY: O(n) for full copy, O(1) for CONVERT`,
      },
      {
        approach: 'PySpark DataFrame API',
        icon: '\u26a1',
        difficulty: 'Beginner',
        pros: 'Native format, fastest read, schema preserved',
        cons: 'Minimal — Parquet is ideal for Spark',
        code: `df = spark.read.format("parquet") \\
    .load("gs://bucket/data/year=2024/month=*/")
df.write.format("delta") \\
    .partitionBy("year", "month") \\
    .saveAsTable("catalog.bronze.parquet_data")`,
      },
      {
        approach: 'Spark SQL',
        icon: '\ud83d\udcdd',
        difficulty: 'Beginner',
        pros: 'One-liner table creation',
        cons: 'Less partition control',
        code: `-- SQL — Direct table creation from Parquet
CREATE TABLE catalog.bronze.parquet_data
USING DELTA
PARTITIONED BY (year, month)
AS SELECT * FROM parquet.\`gs://bucket/data/\`;`,
      },
      {
        approach: 'Delta CONVERT',
        icon: '\ud83d\udd04',
        difficulty: 'Beginner',
        pros: 'In-place conversion, no data copy, instant',
        cons: 'Only works for Parquet already in place',
        code: `-- Convert existing Parquet directory to Delta (no data movement!)
CONVERT TO DELTA parquet.\`gs://bucket/data/\`
PARTITIONED BY (year INT, month INT);

-- Register in catalog
CREATE TABLE catalog.bronze.parquet_data
USING DELTA LOCATION 'gs://bucket/data/';`,
      },
    ],
  },
};

// Generate generic variants for scenarios without specific ones
function getGenericVariants(scenario) {
  const format = scenario.title.split(' ')[0].toLowerCase();
  const cleanTitle = scenario.title.toUpperCase().replace(/[^A-Z0-9 ]/g, '');
  return [
    {
      approach: 'Pseudo Code',
      icon: '\ud83d\udccb',
      difficulty: 'Concept',
      pros: 'Understand logic before coding',
      cons: 'Not executable — blueprint only',
      code: `ALGORITHM: ${cleanTitle}
${'='.repeat(cleanTitle.length + 11)}

PURPOSE: ${scenario.desc}

INPUT:
  - Source: /mnt/landing/${format}/ (or external system)
  - Format: ${scenario.category}

STEPS:
  Step 1: CONNECT to source
    - Authenticate (IAM / service principal / credentials)
    - Verify source is accessible
    - Log connection attempt

  Step 2: READ data
    - Read from source in ${scenario.category} format
    - Apply schema (explicit or inferred)
    - Handle encoding, null values, bad records

  Step 3: VALIDATE
    - Check row count > 0
    - Check schema matches expectation
    - Check for nulls in required columns
    - Route bad records to quarantine

  Step 4: TRANSFORM (if needed)
    - Clean column names (lowercase, no spaces)
    - Cast data types
    - Add metadata: _ingest_ts, _source, _batch_id

  Step 5: WRITE to Bronze (Delta)
    - Format: Delta Lake
    - Mode: append or overwrite
    - Verify write count matches read count

  Step 6: LOG & AUDIT
    - Record: source, target, row count, status, timestamp
    - Alert on failure

ERROR HANDLING:
  - Source unavailable -> retry 3x, then alert
  - Schema mismatch   -> quarantine + alert
  - Write failure     -> retry, then manual intervention

FLOWCHART:
  Source -> Read -> Validate -> [Pass] -> Transform -> Write Delta -> Log
                             -> [Fail] -> Quarantine -> Alert`,
    },
    {
      approach: 'PySpark DataFrame API',
      icon: '\u26a1',
      difficulty: 'Beginner',
      pros: 'Most common, full control',
      cons: 'Verbose for simple cases',
      code: scenario.code,
    },
    {
      approach: 'Spark SQL',
      icon: '\ud83d\udcdd',
      difficulty: 'Beginner',
      pros: 'SQL-native, familiar syntax',
      cons: 'Less programmatic control',
      code: `-- Spark SQL approach for: ${scenario.title}
CREATE TABLE IF NOT EXISTS catalog.bronze.${format}_data
USING DELTA AS
SELECT *, current_timestamp() as _ingest_ts
FROM ${format}.\`/mnt/landing/${format}/\`;

SELECT COUNT(*) as row_count FROM catalog.bronze.${format}_data;`,
    },
    {
      approach: 'Delta Live Tables (DLT)',
      icon: '\ud83d\ude80',
      difficulty: 'Intermediate',
      pros: 'Declarative, auto quality checks',
      cons: 'Requires DLT pipeline config',
      code: `# DLT approach for: ${scenario.title}
import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="bronze_${format}_data",
    comment="${scenario.desc}"
)
def ingest_${format}():
    return (
        spark.read.format("${format}")
        .load("/mnt/landing/${format}/")
        .withColumn("_ingest_ts", current_timestamp())
    )`,
    },
  ];
}

export function getCodeVariants(scenarioId, scenario) {
  if (CODE_VARIANTS[scenarioId]) {
    return CODE_VARIANTS[scenarioId].variants;
  }
  return getGenericVariants(scenario);
}

export { CODE_VARIANTS };
export default CODE_VARIANTS;
