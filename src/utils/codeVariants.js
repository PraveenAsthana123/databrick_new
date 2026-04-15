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
  return [
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
