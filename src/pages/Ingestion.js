import React, { useState } from 'react';

const ingestionScenarios = [
  {
    id: 1,
    category: 'Batch - File',
    title: 'CSV File Ingestion',
    desc: 'Read CSV files from cloud storage into Delta table',
    code: `df = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .load("s3://bucket/data/*.csv")
df.write.format("delta").mode("overwrite").saveAsTable("bronze.csv_data")`,
  },
  {
    id: 2,
    category: 'Batch - File',
    title: 'JSON File Ingestion',
    desc: 'Read nested JSON files with schema enforcement',
    code: `from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("zip", StringType())
    ]))
])
df = spark.read.schema(schema).json("abfss://container@storage.dfs.core.windows.net/json/")
df.write.format("delta").saveAsTable("bronze.json_data")`,
  },
  {
    id: 3,
    category: 'Batch - File',
    title: 'Parquet File Ingestion',
    desc: 'Read Parquet files with partition pruning',
    code: `df = spark.read.format("parquet") \\
    .load("gs://bucket/data/year=2024/month=*/")
df.write.format("delta").partitionBy("year", "month").saveAsTable("bronze.parquet_data")`,
  },
  {
    id: 4,
    category: 'Batch - File',
    title: 'Avro File Ingestion',
    desc: 'Read Avro files with schema registry',
    code: `df = spark.read.format("avro") \\
    .load("/mnt/data/avro_files/")
df.write.format("delta").saveAsTable("bronze.avro_data")`,
  },
  {
    id: 5,
    category: 'Batch - File',
    title: 'ORC File Ingestion',
    desc: 'Read ORC format files from HDFS',
    code: `df = spark.read.format("orc") \\
    .load("hdfs:///data/orc_files/")
df.write.format("delta").saveAsTable("bronze.orc_data")`,
  },
  {
    id: 6,
    category: 'Batch - File',
    title: 'XML File Ingestion',
    desc: 'Parse XML files using spark-xml library',
    code: `df = spark.read.format("com.databricks.spark.xml") \\
    .option("rowTag", "record") \\
    .load("/mnt/data/xml_files/")
df.write.format("delta").saveAsTable("bronze.xml_data")`,
  },
  {
    id: 7,
    category: 'Batch - File',
    title: 'Excel File Ingestion',
    desc: 'Read Excel spreadsheets into Spark DataFrame',
    code: `# Using pandas on Spark
import pandas as pd
pdf = pd.read_excel("/dbfs/mnt/data/report.xlsx", sheet_name="Sheet1")
df = spark.createDataFrame(pdf)
df.write.format("delta").saveAsTable("bronze.excel_data")`,
  },
  {
    id: 8,
    category: 'Batch - File',
    title: 'Fixed-Width File Ingestion',
    desc: 'Parse fixed-width text files',
    code: `from pyspark.sql.functions import substring, trim

df = spark.read.text("/mnt/data/fixed_width.txt")
df = df.select(
    trim(substring("value", 1, 10)).alias("id"),
    trim(substring("value", 11, 30)).alias("name"),
    trim(substring("value", 41, 10)).alias("amount")
)
df.write.format("delta").saveAsTable("bronze.fixed_width_data")`,
  },
  {
    id: 9,
    category: 'Batch - File',
    title: 'Multi-line JSON Ingestion',
    desc: 'Handle multi-line JSON documents',
    code: `df = spark.read.option("multiLine", "true") \\
    .json("/mnt/data/multiline/*.json")
df.write.format("delta").saveAsTable("bronze.multiline_json")`,
  },
  {
    id: 10,
    category: 'Batch - File',
    title: 'Compressed File Ingestion',
    desc: 'Read gzip/bzip2 compressed files',
    code: `df = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("compression", "gzip") \\
    .load("s3://bucket/compressed/*.csv.gz")
df.write.format("delta").saveAsTable("bronze.compressed_data")`,
  },
  {
    id: 11,
    category: 'Batch - Database',
    title: 'JDBC - PostgreSQL Ingestion',
    desc: 'Read from PostgreSQL via JDBC connector',
    code: `df = spark.read.format("jdbc") \\
    .option("url", "jdbc:postgresql://host:5432/mydb") \\
    .option("dbtable", "public.customers") \\
    .option("user", dbutils.secrets.get("scope", "pg_user")) \\
    .option("password", dbutils.secrets.get("scope", "pg_pass")) \\
    .option("fetchsize", "10000") \\
    .load()
df.write.format("delta").saveAsTable("bronze.pg_customers")`,
  },
  {
    id: 12,
    category: 'Batch - Database',
    title: 'JDBC - MySQL Ingestion',
    desc: 'Read from MySQL with parallel partitions',
    code: `df = spark.read.format("jdbc") \\
    .option("url", "jdbc:mysql://host:3306/mydb") \\
    .option("dbtable", "orders") \\
    .option("partitionColumn", "order_id") \\
    .option("lowerBound", "1") \\
    .option("upperBound", "1000000") \\
    .option("numPartitions", "10") \\
    .load()
df.write.format("delta").saveAsTable("bronze.mysql_orders")`,
  },
  {
    id: 13,
    category: 'Batch - Database',
    title: 'JDBC - SQL Server Ingestion',
    desc: 'Read from SQL Server with query pushdown',
    code: `query = "(SELECT * FROM dbo.sales WHERE sale_date > '2024-01-01') AS subq"
df = spark.read.format("jdbc") \\
    .option("url", "jdbc:sqlserver://host:1433;databaseName=mydb") \\
    .option("dbtable", query) \\
    .option("user", dbutils.secrets.get("scope", "mssql_user")) \\
    .option("password", dbutils.secrets.get("scope", "mssql_pass")) \\
    .load()
df.write.format("delta").saveAsTable("bronze.mssql_sales")`,
  },
  {
    id: 14,
    category: 'Batch - Database',
    title: 'JDBC - Oracle Ingestion',
    desc: 'Read from Oracle database',
    code: `df = spark.read.format("jdbc") \\
    .option("url", "jdbc:oracle:thin:@host:1521:orcl") \\
    .option("dbtable", "HR.EMPLOYEES") \\
    .option("user", dbutils.secrets.get("scope", "ora_user")) \\
    .option("password", dbutils.secrets.get("scope", "ora_pass")) \\
    .option("fetchsize", "5000") \\
    .load()
df.write.format("delta").saveAsTable("bronze.oracle_employees")`,
  },
  {
    id: 15,
    category: 'Batch - Database',
    title: 'MongoDB Ingestion',
    desc: 'Read from MongoDB collections',
    code: `df = spark.read.format("mongodb") \\
    .option("uri", dbutils.secrets.get("scope", "mongo_uri")) \\
    .option("database", "mydb") \\
    .option("collection", "users") \\
    .load()
df.write.format("delta").saveAsTable("bronze.mongo_users")`,
  },
  {
    id: 16,
    category: 'Batch - Database',
    title: 'Cassandra Ingestion',
    desc: 'Read from Apache Cassandra',
    code: `df = spark.read.format("org.apache.spark.sql.cassandra") \\
    .option("keyspace", "my_keyspace") \\
    .option("table", "events") \\
    .option("spark.cassandra.connection.host", "cassandra-host") \\
    .load()
df.write.format("delta").saveAsTable("bronze.cassandra_events")`,
  },
  {
    id: 17,
    category: 'Batch - Database',
    title: 'DynamoDB Ingestion',
    desc: 'Read from AWS DynamoDB tables',
    code: `df = spark.read.format("dynamodb") \\
    .option("tableName", "user_sessions") \\
    .option("region", "us-east-1") \\
    .load()
df.write.format("delta").saveAsTable("bronze.dynamo_sessions")`,
  },
  {
    id: 18,
    category: 'Batch - Database',
    title: 'Snowflake Ingestion',
    desc: 'Read from Snowflake data warehouse',
    code: `options = {
    "sfUrl": "account.snowflakecomputing.com",
    "sfUser": dbutils.secrets.get("scope", "sf_user"),
    "sfPassword": dbutils.secrets.get("scope", "sf_pass"),
    "sfDatabase": "MY_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
}
df = spark.read.format("snowflake").options(**options).option("dbtable", "ORDERS").load()
df.write.format("delta").saveAsTable("bronze.snowflake_orders")`,
  },
  {
    id: 19,
    category: 'Batch - Database',
    title: 'Redshift Ingestion',
    desc: 'Read from AWS Redshift',
    code: `df = spark.read.format("io.github.spark_redshift_community.spark.redshift") \\
    .option("url", "jdbc:redshift://cluster:5439/mydb") \\
    .option("dbtable", "public.fact_sales") \\
    .option("tempdir", "s3://temp-bucket/redshift-temp/") \\
    .option("aws_iam_role", "arn:aws:iam::role/RedshiftRole") \\
    .load()
df.write.format("delta").saveAsTable("bronze.redshift_sales")`,
  },
  {
    id: 20,
    category: 'Batch - Database',
    title: 'BigQuery Ingestion',
    desc: 'Read from Google BigQuery',
    code: `df = spark.read.format("bigquery") \\
    .option("table", "project.dataset.table_name") \\
    .option("credentialsFile", "/dbfs/mnt/keys/bq_key.json") \\
    .load()
df.write.format("delta").saveAsTable("bronze.bigquery_data")`,
  },
  {
    id: 21,
    category: 'Streaming',
    title: 'Kafka Streaming Ingestion',
    desc: 'Real-time streaming from Apache Kafka',
    code: `df = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "broker:9092") \\
    .option("subscribe", "events_topic") \\
    .option("startingOffsets", "latest") \\
    .load()

from pyspark.sql.functions import from_json, col
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
parsed.writeStream.format("delta") \\
    .outputMode("append") \\
    .option("checkpointLocation", "/mnt/checkpoints/kafka_events") \\
    .toTable("bronze.kafka_events")`,
  },
  {
    id: 22,
    category: 'Streaming',
    title: 'Event Hubs Streaming',
    desc: 'Stream from Azure Event Hubs',
    code: `conf = {
    "eventhubs.connectionString": dbutils.secrets.get("scope", "eh_conn_str"),
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": '{"offset":"-1","isInclusive":true}'
}
df = spark.readStream.format("eventhubs").options(**conf).load()
df.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/eventhubs") \\
    .toTable("bronze.eventhub_events")`,
  },
  {
    id: 23,
    category: 'Streaming',
    title: 'Kinesis Streaming',
    desc: 'Stream from AWS Kinesis',
    code: `df = spark.readStream.format("kinesis") \\
    .option("streamName", "my-stream") \\
    .option("region", "us-east-1") \\
    .option("initialPosition", "TRIM_HORIZON") \\
    .load()
df.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/kinesis") \\
    .toTable("bronze.kinesis_stream")`,
  },
  {
    id: 24,
    category: 'Streaming',
    title: 'Auto Loader - cloudFiles',
    desc: 'Incremental file ingestion with Auto Loader',
    code: `df = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "json") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/autoloader") \\
    .option("cloudFiles.inferColumnTypes", "true") \\
    .load("s3://bucket/incoming/")
df.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/autoloader") \\
    .option("mergeSchema", "true") \\
    .toTable("bronze.auto_loaded_data")`,
  },
  {
    id: 25,
    category: 'Streaming',
    title: 'Auto Loader - CSV with Schema Evolution',
    desc: 'Handle schema changes automatically',
    code: `df = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "csv") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/csv_evolution") \\
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\
    .option("header", "true") \\
    .load("/mnt/landing/csv_data/")
df.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/csv_evolution") \\
    .option("mergeSchema", "true") \\
    .toTable("bronze.evolved_csv")`,
  },
  {
    id: 26,
    category: 'Streaming',
    title: 'Structured Streaming - Rate Source',
    desc: 'Generate test streaming data',
    code: `df = spark.readStream.format("rate") \\
    .option("rowsPerSecond", 100) \\
    .load()
df.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/rate_test") \\
    .toTable("bronze.rate_test_data")`,
  },
  {
    id: 27,
    category: 'Streaming',
    title: 'Kafka Avro Deserialization',
    desc: 'Deserialize Avro messages from Kafka with Schema Registry',
    code: `from confluent_kafka.schema_registry import SchemaRegistryClient

schema_registry_conf = {"url": "http://schema-registry:8081"}
df = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "broker:9092") \\
    .option("subscribe", "avro_topic") \\
    .load()

from pyspark.sql.avro.functions import from_avro
parsed = df.select(from_avro("value", schema_str).alias("data")).select("data.*")
parsed.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/kafka_avro") \\
    .toTable("bronze.kafka_avro_events")`,
  },
  {
    id: 28,
    category: 'Streaming',
    title: 'Delta Live Tables (DLT) Pipeline',
    desc: 'Declarative ingestion with DLT',
    code: `import dlt

@dlt.table(comment="Raw clickstream events from landing zone")
def bronze_clickstream():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/landing/clickstream/")
    )

@dlt.table(comment="Cleaned clickstream with valid sessions")
@dlt.expect_or_drop("valid_session", "session_id IS NOT NULL")
def silver_clickstream():
    return dlt.read_stream("bronze_clickstream").where("event_type IS NOT NULL")`,
  },
  {
    id: 29,
    category: 'API',
    title: 'REST API Ingestion',
    desc: 'Fetch data from REST APIs and load into Delta',
    code: `import requests
import json

response = requests.get(
    "https://api.example.com/v1/data",
    headers={"Authorization": f"Bearer {dbutils.secrets.get('scope', 'api_token')}"},
    timeout=30
)
data = response.json()["results"]

df = spark.createDataFrame(data)
df.write.format("delta").mode("append").saveAsTable("bronze.api_data")`,
  },
  {
    id: 30,
    category: 'API',
    title: 'Paginated API Ingestion',
    desc: 'Handle paginated REST API responses',
    code: `import requests

all_records = []
page = 1
while True:
    resp = requests.get(
        f"https://api.example.com/v1/records?page={page}&per_page=100",
        headers={"Authorization": f"Bearer {dbutils.secrets.get('scope','token')}"},
        timeout=30
    )
    data = resp.json()
    all_records.extend(data["results"])
    if page >= data["total_pages"]:
        break
    page += 1

df = spark.createDataFrame(all_records)
df.write.format("delta").mode("overwrite").saveAsTable("bronze.paginated_api")`,
  },
  {
    id: 31,
    category: 'API',
    title: 'GraphQL API Ingestion',
    desc: 'Query GraphQL APIs and ingest response',
    code: `import requests

query = """
{
  users(first: 1000) {
    edges {
      node { id name email createdAt }
    }
  }
}
"""
resp = requests.post(
    "https://api.example.com/graphql",
    json={"query": query},
    headers={"Authorization": f"Bearer {dbutils.secrets.get('scope','gql_token')}"},
    timeout=30
)
users = [edge["node"] for edge in resp.json()["data"]["users"]["edges"]]
df = spark.createDataFrame(users)
df.write.format("delta").saveAsTable("bronze.graphql_users")`,
  },
  {
    id: 32,
    category: 'API',
    title: 'Webhook Data Ingestion',
    desc: 'Process webhook payloads stored in cloud storage',
    code: `df = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "json") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/webhooks") \\
    .load("/mnt/landing/webhooks/")

from pyspark.sql.functions import current_timestamp
df = df.withColumn("ingested_at", current_timestamp())
df.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/webhooks") \\
    .toTable("bronze.webhook_events")`,
  },
  {
    id: 33,
    category: 'Cloud Storage',
    title: 'AWS S3 Direct Mount',
    desc: 'Mount S3 bucket and read data',
    code: `# Mount S3 bucket
dbutils.fs.mount(
    source="s3a://my-bucket",
    mount_point="/mnt/s3_data",
    extra_configs={
        "fs.s3a.access.key": dbutils.secrets.get("scope", "aws_key"),
        "fs.s3a.secret.key": dbutils.secrets.get("scope", "aws_secret")
    }
)
df = spark.read.parquet("/mnt/s3_data/datasets/")
df.write.format("delta").saveAsTable("bronze.s3_data")`,
  },
  {
    id: 34,
    category: 'Cloud Storage',
    title: 'Azure ADLS Gen2 Ingestion',
    desc: 'Read from Azure Data Lake Storage Gen2',
    code: `spark.conf.set(
    "fs.azure.account.key.mystorageaccount.dfs.core.windows.net",
    dbutils.secrets.get("scope", "adls_key")
)
df = spark.read.format("delta") \\
    .load("abfss://container@mystorageaccount.dfs.core.windows.net/data/")
df.write.format("delta").saveAsTable("bronze.adls_data")`,
  },
  {
    id: 35,
    category: 'Cloud Storage',
    title: 'GCS Ingestion',
    desc: 'Read from Google Cloud Storage',
    code: `df = spark.read.format("parquet") \\
    .load("gs://my-gcs-bucket/data/parquet_files/")
df.write.format("delta").saveAsTable("bronze.gcs_data")`,
  },
  {
    id: 36,
    category: 'Cloud Storage',
    title: 'Unity Catalog Volumes Ingestion',
    desc: 'Read files from UC managed volumes',
    code: `df = spark.read.format("csv") \\
    .option("header", "true") \\
    .load("/Volumes/my_catalog/my_schema/my_volume/data.csv")
df.write.format("delta").saveAsTable("my_catalog.bronze.volume_data")`,
  },
  {
    id: 37,
    category: 'CDC',
    title: 'CDC with Delta - MERGE',
    desc: 'Change Data Capture using Delta MERGE',
    code: `from delta.tables import DeltaTable

# Read CDC events
cdc_df = spark.read.format("delta").load("/mnt/landing/cdc_events/")

# Merge into target
target = DeltaTable.forName(spark, "silver.customers")
target.alias("t").merge(
    cdc_df.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdateAll() \\
 .whenNotMatchedInsertAll() \\
 .whenNotMatchedBySourceDelete() \\
 .execute()`,
  },
  {
    id: 38,
    category: 'CDC',
    title: 'Debezium CDC Ingestion',
    desc: 'Process Debezium CDC events from Kafka',
    code: `df = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "broker:9092") \\
    .option("subscribe", "dbserver1.public.customers") \\
    .load()

from pyspark.sql.functions import from_json, col
cdc_schema = "before STRUCT<id:INT,name:STRING>, after STRUCT<id:INT,name:STRING>, op STRING"
parsed = df.select(from_json(col("value").cast("string"), cdc_schema).alias("cdc"))
parsed.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/debezium") \\
    .toTable("bronze.debezium_cdc")`,
  },
  {
    id: 39,
    category: 'CDC',
    title: 'SCD Type 2 Implementation',
    desc: 'Slowly Changing Dimension Type 2',
    code: `from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

incoming = spark.read.format("delta").load("/mnt/staging/customers/")
target = DeltaTable.forName(spark, "silver.customers_scd2")

# Close old records
target.alias("t").merge(
    incoming.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(
    condition="t.name != s.name OR t.email != s.email",
    set={"is_current": lit(False), "end_date": current_timestamp()}
).execute()

# Insert new/changed records
new_records = incoming.withColumn("is_current", lit(True)) \\
    .withColumn("start_date", current_timestamp()) \\
    .withColumn("end_date", lit(None).cast("timestamp"))
new_records.write.format("delta").mode("append").saveAsTable("silver.customers_scd2")`,
  },
  {
    id: 40,
    category: 'CDC',
    title: 'DLT Apply Changes (CDC)',
    desc: 'Use DLT apply_changes for CDC processing',
    code: `import dlt

@dlt.table
def customers_raw():
    return spark.readStream.format("cloudFiles") \\
        .option("cloudFiles.format", "json") \\
        .load("/mnt/landing/cdc/customers/")

dlt.create_streaming_table("customers_clean")

dlt.apply_changes(
    target="customers_clean",
    source="customers_raw",
    keys=["customer_id"],
    sequence_by="updated_at",
    apply_as_deletes=col("operation") == "DELETE",
    stored_as_scd_type=2
)`,
  },
  {
    id: 41,
    category: 'Data Format',
    title: 'Delta Table CLONE',
    desc: 'Clone existing Delta tables',
    code: `# Deep clone (full copy)
spark.sql("CREATE TABLE bronze.customers_backup DEEP CLONE silver.customers")

# Shallow clone (metadata only, references same files)
spark.sql("CREATE TABLE bronze.customers_dev SHALLOW CLONE silver.customers")`,
  },
  {
    id: 42,
    category: 'Data Format',
    title: 'Delta COPY INTO',
    desc: 'Idempotent file ingestion with COPY INTO',
    code: `spark.sql("""
    COPY INTO bronze.sales_data
    FROM '/mnt/landing/sales/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")`,
  },
  {
    id: 43,
    category: 'Data Format',
    title: 'Delta Table - Time Travel Restore',
    desc: 'Restore data from previous version',
    code: `# Restore to specific version
spark.sql("RESTORE TABLE bronze.events TO VERSION AS OF 5")

# Restore to timestamp
spark.sql("RESTORE TABLE bronze.events TO TIMESTAMP AS OF '2024-01-15T10:00:00'")

# Read historical version
df_old = spark.read.format("delta").option("versionAsOf", 3).table("bronze.events")`,
  },
  {
    id: 44,
    category: 'External',
    title: 'FTP/SFTP File Ingestion',
    desc: 'Download files from FTP/SFTP servers',
    code: `import paramiko

transport = paramiko.Transport(("ftp.example.com", 22))
transport.connect(username=dbutils.secrets.get("scope","sftp_user"),
                  password=dbutils.secrets.get("scope","sftp_pass"))
sftp = paramiko.SFTPClient.from_transport(transport)
sftp.get("/remote/data.csv", "/dbfs/mnt/landing/sftp/data.csv")
sftp.close()
transport.close()

df = spark.read.csv("/mnt/landing/sftp/data.csv", header=True, inferSchema=True)
df.write.format("delta").saveAsTable("bronze.sftp_data")`,
  },
  {
    id: 45,
    category: 'External',
    title: 'Email Attachment Ingestion',
    desc: 'Extract attachments from emails via API',
    code: `import requests

resp = requests.get(
    "https://graph.microsoft.com/v1.0/me/messages?$filter=hasAttachments eq true",
    headers={"Authorization": f"Bearer {dbutils.secrets.get('scope','graph_token')}"},
    timeout=30
)
for msg in resp.json()["value"]:
    attachments = requests.get(
        f"https://graph.microsoft.com/v1.0/me/messages/{msg['id']}/attachments",
        headers={"Authorization": f"Bearer {dbutils.secrets.get('scope','graph_token')}"},
        timeout=30
    ).json()["value"]
    # Process and save attachments`,
  },
  {
    id: 46,
    category: 'External',
    title: 'Web Scraping Ingestion',
    desc: 'Scrape web pages and ingest structured data',
    code: `import requests
from bs4 import BeautifulSoup

html = requests.get("https://example.com/data-table", timeout=30).text
soup = BeautifulSoup(html, "html.parser")
rows = []
for tr in soup.select("table tbody tr"):
    cells = [td.text.strip() for td in tr.select("td")]
    rows.append({"col1": cells[0], "col2": cells[1], "col3": cells[2]})

df = spark.createDataFrame(rows)
df.write.format("delta").saveAsTable("bronze.scraped_data")`,
  },
  {
    id: 47,
    category: 'External',
    title: 'IoT Device Data Ingestion',
    desc: 'Ingest IoT sensor data via MQTT/Kafka',
    code: `# IoT data arrives via Kafka topic from MQTT bridge
df = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "broker:9092") \\
    .option("subscribe", "iot_sensors") \\
    .load()

from pyspark.sql.functions import from_json, col
iot_schema = "device_id STRING, temperature DOUBLE, humidity DOUBLE, timestamp LONG"
parsed = df.select(from_json(col("value").cast("string"), iot_schema).alias("sensor")).select("sensor.*")
parsed.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/iot") \\
    .toTable("bronze.iot_sensor_data")`,
  },
  {
    id: 48,
    category: 'External',
    title: 'Salesforce Data Ingestion',
    desc: 'Extract data from Salesforce CRM',
    code: `from simple_salesforce import Salesforce

sf = Salesforce(
    username=dbutils.secrets.get("scope", "sf_user"),
    password=dbutils.secrets.get("scope", "sf_pass"),
    security_token=dbutils.secrets.get("scope", "sf_token")
)
results = sf.query_all("SELECT Id, Name, Email FROM Contact")
df = spark.createDataFrame(results["records"])
df.write.format("delta").mode("overwrite").saveAsTable("bronze.salesforce_contacts")`,
  },
  {
    id: 49,
    category: 'Transform',
    title: 'Multi-Source Join Ingestion',
    desc: 'Join data from multiple sources during ingestion',
    code: `# Read from multiple sources
customers = spark.read.format("delta").table("bronze.customers")
orders = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "orders").load()
products = spark.read.csv("/mnt/landing/products.csv", header=True)

# Join and write to silver
enriched = orders.join(customers, "customer_id") \\
    .join(products, "product_id") \\
    .select("order_id", "customer_name", "product_name", "quantity", "total")
enriched.write.format("delta").saveAsTable("silver.enriched_orders")`,
  },
  {
    id: 50,
    category: 'Transform',
    title: 'Schema Enforcement Ingestion',
    desc: 'Enforce schema and handle bad records',
    code: `from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

expected_schema = StructType([
    StructField("id", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("category", StringType(), True)
])

df = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("mode", "PERMISSIVE") \\
    .option("columnNameOfCorruptRecord", "_corrupt_record") \\
    .schema(expected_schema.add("_corrupt_record", StringType())) \\
    .load("/mnt/landing/transactions/")

# Separate good and bad records
good = df.filter("_corrupt_record IS NULL").drop("_corrupt_record")
bad = df.filter("_corrupt_record IS NOT NULL")
good.write.format("delta").saveAsTable("bronze.transactions")
bad.write.format("delta").saveAsTable("bronze.transactions_quarantine")`,
  },
  {
    id: 51,
    category: 'Transform',
    title: 'Watermark-based Dedup Streaming',
    desc: 'Deduplicate streaming data using watermarks',
    code: `df = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "broker:9092") \\
    .option("subscribe", "events") \\
    .load()

from pyspark.sql.functions import from_json, col, to_timestamp
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

deduped = parsed \\
    .withWatermark("event_time", "1 hour") \\
    .dropDuplicatesWithinWatermark(["event_id"])

deduped.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/dedup_stream") \\
    .toTable("bronze.deduped_events")`,
  },
  {
    id: 52,
    category: 'Transform',
    title: 'Partition-based Incremental Load',
    desc: 'Load only new partitions',
    code: `from pyspark.sql.functions import max as spark_max

# Get last loaded partition
last_date = spark.sql("SELECT max(load_date) FROM bronze.daily_data").collect()[0][0]

# Load only new data
new_data = spark.read.format("csv") \\
    .option("header", "true") \\
    .load(f"/mnt/landing/daily/{last_date.strftime('%Y/%m/%d')}/*/")

new_data.write.format("delta") \\
    .mode("append") \\
    .partitionBy("load_date") \\
    .saveAsTable("bronze.daily_data")`,
  },
  {
    id: 53,
    category: 'Transform',
    title: 'Data Quality Checks on Ingestion',
    desc: 'Validate data quality during ingestion',
    code: `from pyspark.sql.functions import col, when, count, isnan, isnull

df = spark.read.parquet("/mnt/landing/raw_data/")

# Quality checks
total = df.count()
null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
dups = df.count() - df.dropDuplicates().count()

# Log metrics
quality_report = spark.createDataFrame([{
    "table": "raw_data", "total_rows": total,
    "duplicate_rows": dups, "check_time": str(current_timestamp())
}])
quality_report.write.format("delta").mode("append").saveAsTable("audit.data_quality_log")

# Write clean data
df.dropDuplicates().write.format("delta").saveAsTable("bronze.quality_checked_data")`,
  },
  {
    id: 54,
    category: 'Transform',
    title: 'Multi-Format Landing Zone Processing',
    desc: 'Process mixed file formats from landing zone',
    code: `import os

landing_path = "/mnt/landing/mixed/"
for file_info in dbutils.fs.ls(landing_path):
    path = file_info.path
    if path.endswith(".csv"):
        df = spark.read.csv(path, header=True, inferSchema=True)
    elif path.endswith(".json"):
        df = spark.read.json(path)
    elif path.endswith(".parquet"):
        df = spark.read.parquet(path)
    else:
        continue

    table_name = os.path.basename(path).split(".")[0]
    df.write.format("delta").mode("append").saveAsTable(f"bronze.{table_name}")`,
  },
  {
    id: 55,
    category: 'Transform',
    title: 'Flatten Nested JSON Ingestion',
    desc: 'Flatten deeply nested JSON structures',
    code: `from pyspark.sql.functions import explode, col

df = spark.read.json("/mnt/landing/nested_data/")

# Flatten nested arrays and structs
flattened = df \\
    .select("id", "name", explode("orders").alias("order")) \\
    .select("id", "name",
            col("order.order_id"),
            col("order.amount"),
            explode("order.items").alias("item")) \\
    .select("id", "name", "order_id", "amount",
            col("item.product_name"),
            col("item.quantity"),
            col("item.price"))

flattened.write.format("delta").saveAsTable("bronze.flattened_orders")`,
  },
];

const categories = [...new Set(ingestionScenarios.map((s) => s.category))];

function Ingestion() {
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const filtered = ingestionScenarios.filter((s) => {
    const matchCategory = selectedCategory === 'All' || s.category === selectedCategory;
    const matchSearch =
      s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.desc.toLowerCase().includes(searchTerm.toLowerCase());
    return matchCategory && matchSearch;
  });

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Ingestion Scenarios</h1>
          <p>{ingestionScenarios.length} PySpark data ingestion patterns for Databricks</p>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search scenarios..."
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
            <option value="All">All Categories ({ingestionScenarios.length})</option>
            {categories.map((cat) => (
              <option key={cat} value={cat}>
                {cat} ({ingestionScenarios.filter((s) => s.category === cat).length})
              </option>
            ))}
          </select>
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Showing {filtered.length} of {ingestionScenarios.length}
          </span>
        </div>
      </div>

      <div className="scenarios-list">
        {filtered.map((scenario) => (
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
                  <span className="badge running">{scenario.category}</span>
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
            {expandedId === scenario.id && (
              <div className="code-block" style={{ marginTop: '1rem' }}>
                {scenario.code}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export default Ingestion;
