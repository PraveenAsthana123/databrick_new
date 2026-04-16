import React, { useState } from 'react';
import ScenarioCard from '../components/common/ScenarioCard';

const unityCatalogScenarios = [
  // ===== Catalog Management (1-5) =====
  {
    id: 1,
    category: 'Catalog Management',
    title: 'Create Catalog',
    desc: 'Create a new Unity Catalog with optional managed location',
    code: `# Create a new catalog
spark.sql("CREATE CATALOG IF NOT EXISTS my_catalog")

# Create catalog with managed location
spark.sql("""
    CREATE CATALOG IF NOT EXISTS my_catalog
    MANAGED LOCATION 's3://my-bucket/unity-catalog/my_catalog'
""")

# Set current catalog
spark.sql("USE CATALOG my_catalog")
print("Catalog created and set as current catalog")`,
  },

  {
    id: 2,
    category: 'Catalog Management',
    title: 'Alter Catalog',
    desc: 'Modify catalog properties such as owner and comment',
    code: `# Alter catalog owner
spark.sql("ALTER CATALOG my_catalog SET OWNER TO \`data_engineers\`")

# Add or update catalog comment
spark.sql("ALTER CATALOG my_catalog SET COMMENT 'Production data catalog for analytics'")

# Set catalog properties
spark.sql("""
    ALTER CATALOG my_catalog
    SET DBPROPERTIES ('team' = 'data-engineering', 'env' = 'production')
""")
print("Catalog altered successfully")`,
  },

  {
    id: 3,
    category: 'Catalog Management',
    title: 'Drop Catalog',
    desc: 'Remove a catalog and optionally all its contents',
    code: `# Drop empty catalog
spark.sql("DROP CATALOG IF EXISTS test_catalog")

# Drop catalog with all schemas and tables (CASCADE)
spark.sql("DROP CATALOG IF EXISTS test_catalog CASCADE")

# Verify catalog is removed
catalogs = spark.sql("SHOW CATALOGS").collect()
catalog_names = [row.catalog for row in catalogs]
print(f"Remaining catalogs: {catalog_names}")`,
  },

  {
    id: 4,
    category: 'Catalog Management',
    title: 'List Catalogs',
    desc: 'Show all available catalogs in the Unity Catalog metastore',
    code: `# List all catalogs
catalogs_df = spark.sql("SHOW CATALOGS")
catalogs_df.show(truncate=False)

# Filter catalogs by pattern
filtered = spark.sql("SHOW CATALOGS LIKE 'prod_*'")
filtered.show(truncate=False)

# Get catalog count
count = catalogs_df.count()
print(f"Total catalogs available: {count}")`,
  },

  {
    id: 5,
    category: 'Catalog Management',
    title: 'Describe Catalog',
    desc: 'Get detailed metadata about a specific catalog',
    code: `# Describe catalog metadata
catalog_info = spark.sql("DESCRIBE CATALOG my_catalog")
catalog_info.show(truncate=False)

# Describe catalog extended info
catalog_ext = spark.sql("DESCRIBE CATALOG EXTENDED my_catalog")
catalog_ext.show(truncate=False)

# Programmatic access to catalog properties
for row in catalog_ext.collect():
    print(f"{row.info_name}: {row.info_value}")`,
  },

  // ===== Schema Management (6-10) =====
  {
    id: 6,
    category: 'Schema Management',
    title: 'Create Schema',
    desc: 'Create a new schema within a catalog with managed location',
    code: `# Create schema in current catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS my_catalog.bronze")

# Create schema with managed location
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS my_catalog.silver
    MANAGED LOCATION 's3://my-bucket/unity-catalog/my_catalog/silver'
    COMMENT 'Silver layer - cleaned and validated data'
""")

# Create schema with properties
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS my_catalog.gold
    WITH DBPROPERTIES ('layer' = 'gold', 'team' = 'analytics')
""")
spark.sql("USE SCHEMA my_catalog.bronze")
print("Schemas created successfully")`,
  },

  {
    id: 7,
    category: 'Schema Management',
    title: 'Alter Schema',
    desc: 'Modify schema properties, owner, and comment',
    code: `# Change schema owner
spark.sql("ALTER SCHEMA my_catalog.bronze SET OWNER TO \`data_engineers\`")

# Update schema comment
spark.sql("""
    ALTER SCHEMA my_catalog.bronze
    SET COMMENT 'Raw ingestion layer for all source systems'
""")

# Update schema properties
spark.sql("""
    ALTER SCHEMA my_catalog.bronze
    SET DBPROPERTIES ('retention_days' = '90', 'sla' = 'tier1')
""")
print("Schema altered successfully")`,
  },

  {
    id: 8,
    category: 'Schema Management',
    title: 'Drop Schema',
    desc: 'Remove a schema and optionally cascade to all objects',
    code: `# Drop empty schema
spark.sql("DROP SCHEMA IF EXISTS my_catalog.temp_schema")

# Drop schema with all tables, views, and functions
spark.sql("DROP SCHEMA IF EXISTS my_catalog.temp_schema CASCADE")

# Verify schema removal
schemas = spark.sql("SHOW SCHEMAS IN my_catalog").collect()
schema_names = [row.databaseName for row in schemas]
print(f"Remaining schemas: {schema_names}")`,
  },

  {
    id: 9,
    category: 'Schema Management',
    title: 'List Schemas',
    desc: 'Show all schemas in a catalog with optional filtering',
    code: `# List all schemas in a catalog
schemas_df = spark.sql("SHOW SCHEMAS IN my_catalog")
schemas_df.show(truncate=False)

# Filter schemas by pattern
filtered = spark.sql("SHOW SCHEMAS IN my_catalog LIKE 'silver*'")
filtered.show(truncate=False)

# List schemas with details
for row in schemas_df.collect():
    desc = spark.sql(f"DESCRIBE SCHEMA {row.databaseName}").collect()
    print(f"Schema: {row.databaseName}")
    for d in desc:
        print(f"  {d.database_description_item}: {d.database_description_value}")`,
  },

  {
    id: 10,
    category: 'Schema Management',
    title: 'Describe Schema',
    desc: 'Get detailed information about a schema',
    code: `# Describe schema
schema_info = spark.sql("DESCRIBE SCHEMA my_catalog.bronze")
schema_info.show(truncate=False)

# Describe schema with extended properties
schema_ext = spark.sql("DESCRIBE SCHEMA EXTENDED my_catalog.bronze")
schema_ext.show(truncate=False)

# Get schema location and properties
for row in schema_ext.collect():
    print(f"{row.database_description_item}: {row.database_description_value}")`,
  },

  // ===== Table Management (11-18) =====
  {
    id: 11,
    category: 'Table Management',
    title: 'Create Managed Table',
    desc: 'Create a Unity Catalog managed Delta table',
    code: `from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Create managed table using SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.bronze.customers (
        customer_id INT,
        name STRING,
        email STRING,
        created_at TIMESTAMP,
        region STRING
    )
    USING DELTA
    COMMENT 'Customer master data'
    TBLPROPERTIES ('quality' = 'bronze', 'delta.autoOptimize.optimizeWrite' = 'true')
""")

# Create managed table using DataFrame API
schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", IntegerType(), True),
    StructField("order_date", TimestampType(), True)
])
df = spark.createDataFrame([], schema)
df.write.format("delta").saveAsTable("my_catalog.bronze.orders")
print("Managed tables created")`,
  },

  {
    id: 12,
    category: 'Table Management',
    title: 'Create External Table',
    desc: 'Create a Unity Catalog external table pointing to cloud storage',
    code: `# Create external table with explicit location
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.bronze.external_events (
        event_id STRING,
        event_type STRING,
        payload STRING,
        event_time TIMESTAMP
    )
    USING DELTA
    LOCATION 's3://my-bucket/external/events/'
    COMMENT 'External events table from upstream system'
""")

# Create external table from existing data
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.bronze.legacy_logs
    USING PARQUET
    LOCATION 's3://my-bucket/legacy/logs/'
    OPTIONS (
        'mergeSchema' = 'true'
    )
""")
print("External tables created")`,
  },

  {
    id: 13,
    category: 'Table Management',
    title: 'Alter Table',
    desc: 'Modify table properties, add columns, rename, and set owner',
    code: `# Add new columns
spark.sql("""
    ALTER TABLE my_catalog.bronze.customers
    ADD COLUMNS (
        phone STRING COMMENT 'Customer phone number',
        loyalty_tier STRING COMMENT 'Bronze/Silver/Gold/Platinum'
    )
""")

# Rename column
spark.sql("""
    ALTER TABLE my_catalog.bronze.customers
    RENAME COLUMN name TO full_name
""")

# Set table properties
spark.sql("""
    ALTER TABLE my_catalog.bronze.customers
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# Change table owner
spark.sql("ALTER TABLE my_catalog.bronze.customers SET OWNER TO \`data_engineers\`")
print("Table altered successfully")`,
  },

  {
    id: 14,
    category: 'Table Management',
    title: 'Drop Table',
    desc: 'Drop managed or external tables from Unity Catalog',
    code: `# Drop managed table (data is also deleted)
spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.temp_data")

# Drop external table (only metadata removed, data persists in storage)
spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.external_temp")

# Verify table removal
tables = spark.sql("SHOW TABLES IN my_catalog.bronze").collect()
table_names = [row.tableName for row in tables]
print(f"Remaining tables in bronze: {table_names}")

# Purge table (remove from trash permanently)
spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.old_data PURGE")`,
  },

  {
    id: 15,
    category: 'Table Management',
    title: 'List and Describe Tables',
    desc: 'Show all tables and get detailed table metadata',
    code: `# List all tables in a schema
tables_df = spark.sql("SHOW TABLES IN my_catalog.bronze")
tables_df.show(truncate=False)

# Filter tables by pattern
filtered = spark.sql("SHOW TABLES IN my_catalog.bronze LIKE 'customer*'")
filtered.show(truncate=False)

# Describe table structure
spark.sql("DESCRIBE TABLE my_catalog.bronze.customers").show(truncate=False)

# Describe table extended (includes location, properties, stats)
spark.sql("DESCRIBE TABLE EXTENDED my_catalog.bronze.customers").show(truncate=False)

# Describe table history
spark.sql("DESCRIBE HISTORY my_catalog.bronze.customers").show(truncate=False)

# Show table detail
spark.sql("DESCRIBE DETAIL my_catalog.bronze.customers").show(truncate=False)`,
  },

  {
    id: 16,
    category: 'Table Management',
    title: 'Clone Table',
    desc: 'Create deep and shallow clones of Delta tables',
    code: `# Deep clone - full copy of data and metadata
spark.sql("""
    CREATE TABLE my_catalog.silver.customers_clone
    DEEP CLONE my_catalog.bronze.customers
""")

# Shallow clone - metadata only, references source data files
spark.sql("""
    CREATE TABLE my_catalog.silver.customers_shallow
    SHALLOW CLONE my_catalog.bronze.customers
""")

# Clone with version (time travel)
spark.sql("""
    CREATE TABLE my_catalog.silver.customers_v5
    DEEP CLONE my_catalog.bronze.customers VERSION AS OF 5
""")

# Clone with timestamp
spark.sql("""
    CREATE OR REPLACE TABLE my_catalog.silver.customers_snapshot
    DEEP CLONE my_catalog.bronze.customers
    TIMESTAMP AS OF '2024-01-15T00:00:00Z'
""")
print("Table clones created successfully")`,
  },

  {
    id: 17,
    category: 'Table Management',
    title: 'Convert to Delta',
    desc: 'Convert Parquet/Iceberg tables to Delta format in Unity Catalog',
    code: `# Convert Parquet table to Delta
spark.sql("""
    CONVERT TO DELTA parquet.\`s3://my-bucket/legacy/parquet_table/\`
""")

# Convert partitioned Parquet table
spark.sql("""
    CONVERT TO DELTA parquet.\`s3://my-bucket/legacy/partitioned_table/\`
    PARTITIONED BY (year INT, month INT)
""")

# Register converted table in Unity Catalog
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.bronze.converted_table
    USING DELTA
    LOCATION 's3://my-bucket/legacy/parquet_table/'
""")

# Verify conversion
df = spark.read.format("delta").table("my_catalog.bronze.converted_table")
print(f"Converted table row count: {df.count()}")
df.printSchema()`,
  },

  {
    id: 18,
    category: 'Table Management',
    title: 'Table Maintenance Operations',
    desc: 'Optimize, vacuum, and analyze Unity Catalog tables',
    code: `# Optimize table (bin-packing / Z-order)
spark.sql("OPTIMIZE my_catalog.bronze.customers")

# Optimize with Z-ORDER for query performance
spark.sql("""
    OPTIMIZE my_catalog.bronze.customers
    ZORDER BY (region, created_at)
""")

# Vacuum old files (default retention: 7 days)
spark.sql("VACUUM my_catalog.bronze.customers")

# Vacuum with custom retention (e.g., 24 hours)
spark.sql("VACUUM my_catalog.bronze.customers RETAIN 24 HOURS")

# Analyze table to compute statistics
spark.sql("ANALYZE TABLE my_catalog.bronze.customers COMPUTE STATISTICS")

# Analyze specific columns
spark.sql("""
    ANALYZE TABLE my_catalog.bronze.customers
    COMPUTE STATISTICS FOR COLUMNS customer_id, region
""")
print("Table maintenance completed")`,
  },

  // ===== Access Control (19-25) =====
  {
    id: 19,
    category: 'Access Control',
    title: 'Grant Privileges on Catalog',
    desc: 'Grant access permissions at the catalog level',
    code: `# Grant USE CATALOG to a group
spark.sql("GRANT USE CATALOG ON CATALOG my_catalog TO \`data_analysts\`")

# Grant CREATE SCHEMA privilege
spark.sql("GRANT CREATE SCHEMA ON CATALOG my_catalog TO \`data_engineers\`")

# Grant all privileges on catalog
spark.sql("GRANT ALL PRIVILEGES ON CATALOG my_catalog TO \`catalog_admins\`")

# Grant to individual user
spark.sql("GRANT USE CATALOG ON CATALOG my_catalog TO \`user@company.com\`")
print("Catalog-level grants applied")`,
  },

  {
    id: 20,
    category: 'Access Control',
    title: 'Grant Privileges on Schema',
    desc: 'Grant access permissions at the schema level',
    code: `# Grant USE SCHEMA
spark.sql("GRANT USE SCHEMA ON SCHEMA my_catalog.silver TO \`data_analysts\`")

# Grant CREATE TABLE within schema
spark.sql("GRANT CREATE TABLE ON SCHEMA my_catalog.bronze TO \`data_engineers\`")

# Grant SELECT on all tables in schema
spark.sql("GRANT SELECT ON SCHEMA my_catalog.gold TO \`bi_team\`")

# Grant multiple privileges
spark.sql("""
    GRANT USE SCHEMA, SELECT, MODIFY
    ON SCHEMA my_catalog.silver TO \`data_engineers\`
""")
print("Schema-level grants applied")`,
  },

  {
    id: 21,
    category: 'Access Control',
    title: 'Grant Privileges on Table',
    desc: 'Grant fine-grained access on specific tables',
    code: `# Grant SELECT on specific table
spark.sql("GRANT SELECT ON TABLE my_catalog.gold.revenue_report TO \`bi_team\`")

# Grant MODIFY (INSERT, UPDATE, DELETE)
spark.sql("GRANT MODIFY ON TABLE my_catalog.bronze.customers TO \`data_engineers\`")

# Grant ALL PRIVILEGES on table
spark.sql("GRANT ALL PRIVILEGES ON TABLE my_catalog.bronze.orders TO \`data_admins\`")

# Grant SELECT on a view
spark.sql("GRANT SELECT ON VIEW my_catalog.gold.customer_summary TO \`analysts\`")
print("Table-level grants applied")`,
  },

  {
    id: 22,
    category: 'Access Control',
    title: 'Revoke Privileges',
    desc: 'Revoke previously granted permissions from users and groups',
    code: `# Revoke SELECT on table
spark.sql("REVOKE SELECT ON TABLE my_catalog.gold.revenue_report FROM \`former_analysts\`")

# Revoke schema-level privileges
spark.sql("REVOKE ALL PRIVILEGES ON SCHEMA my_catalog.bronze FROM \`temp_contractors\`")

# Revoke catalog-level access
spark.sql("REVOKE USE CATALOG ON CATALOG my_catalog FROM \`external_users\`")

# Revoke MODIFY while keeping SELECT
spark.sql("REVOKE MODIFY ON TABLE my_catalog.bronze.customers FROM \`data_analysts\`")
print("Privileges revoked successfully")`,
  },

  {
    id: 23,
    category: 'Access Control',
    title: 'Show Grants',
    desc: 'View all grants on catalogs, schemas, and tables',
    code: `# Show grants on catalog
grants_catalog = spark.sql("SHOW GRANTS ON CATALOG my_catalog")
grants_catalog.show(truncate=False)

# Show grants on schema
grants_schema = spark.sql("SHOW GRANTS ON SCHEMA my_catalog.bronze")
grants_schema.show(truncate=False)

# Show grants on table
grants_table = spark.sql("SHOW GRANTS ON TABLE my_catalog.bronze.customers")
grants_table.show(truncate=False)

# Show grants for a specific principal
grants_user = spark.sql("SHOW GRANTS TO \`data_engineers\`")
grants_user.show(truncate=False)

# Show grants on a specific user for a table
grants_specific = spark.sql("""
    SHOW GRANTS \`data_analysts\`
    ON TABLE my_catalog.gold.revenue_report
""")
grants_specific.show(truncate=False)`,
  },

  {
    id: 24,
    category: 'Access Control',
    title: 'Row-Level Security',
    desc: 'Implement row filters to restrict data access by user or group',
    code: `# Create a row filter function
spark.sql("""
    CREATE OR REPLACE FUNCTION my_catalog.bronze.region_filter(region_val STRING)
    RETURNS BOOLEAN
    RETURN IF(
        IS_ACCOUNT_GROUP_MEMBER('global_admins'),
        TRUE,
        region_val = CASE
            WHEN IS_ACCOUNT_GROUP_MEMBER('us_team') THEN 'US'
            WHEN IS_ACCOUNT_GROUP_MEMBER('eu_team') THEN 'EU'
            ELSE 'NONE'
        END
    )
""")

# Apply row filter to a table
spark.sql("""
    ALTER TABLE my_catalog.bronze.customers
    SET ROW FILTER my_catalog.bronze.region_filter ON (region)
""")

# Verify row filter is applied
spark.sql("DESCRIBE TABLE EXTENDED my_catalog.bronze.customers").show(truncate=False)

# Remove row filter
# spark.sql("ALTER TABLE my_catalog.bronze.customers DROP ROW FILTER")
print("Row-level security configured")`,
  },

  {
    id: 25,
    category: 'Access Control',
    title: 'Column Masking',
    desc: 'Apply dynamic column masks to protect sensitive data',
    code: `# Create a column mask function for email
spark.sql("""
    CREATE OR REPLACE FUNCTION my_catalog.bronze.mask_email(email_val STRING)
    RETURNS STRING
    RETURN IF(
        IS_ACCOUNT_GROUP_MEMBER('data_admins'),
        email_val,
        CONCAT(LEFT(email_val, 2), '***@', SPLIT(email_val, '@')[1])
    )
""")

# Create mask for phone numbers
spark.sql("""
    CREATE OR REPLACE FUNCTION my_catalog.bronze.mask_phone(phone_val STRING)
    RETURNS STRING
    RETURN IF(
        IS_ACCOUNT_GROUP_MEMBER('data_admins'),
        phone_val,
        CONCAT('***-***-', RIGHT(phone_val, 4))
    )
""")

# Apply column masks to table
spark.sql("""
    ALTER TABLE my_catalog.bronze.customers
    ALTER COLUMN email SET MASK my_catalog.bronze.mask_email
""")
spark.sql("""
    ALTER TABLE my_catalog.bronze.customers
    ALTER COLUMN phone SET MASK my_catalog.bronze.mask_phone
""")
print("Column masking applied successfully")`,
  },

  // ===== Data Lineage (26-30) =====
  {
    id: 26,
    category: 'Data Lineage',
    title: 'Query Table Lineage',
    desc: 'Retrieve upstream and downstream lineage for tables using REST API',
    code: `import requests
import json

# Databricks workspace URL and token
workspace_url = dbutils.notebook.entry_point.getDbutils() \\
    .notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils() \\
    .notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {token}"}

# Get table lineage (downstream dependencies)
table_name = "my_catalog.bronze.customers"
response = requests.get(
    f"{workspace_url}/api/2.0/lineage-tracking/table-lineage",
    headers=headers,
    params={"table_name": table_name},
    timeout=30
)
lineage = response.json()
print(f"Lineage for {table_name}:")
print(json.dumps(lineage, indent=2))`,
  },

  {
    id: 27,
    category: 'Data Lineage',
    title: 'Column-Level Lineage',
    desc: 'Track data flow at column granularity through transformations',
    code: `import requests
import json

workspace_url = dbutils.notebook.entry_point.getDbutils() \\
    .notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils() \\
    .notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}"}

# Get column lineage for a specific table and column
table_name = "my_catalog.gold.revenue_report"
column_name = "total_revenue"

response = requests.get(
    f"{workspace_url}/api/2.0/lineage-tracking/column-lineage",
    headers=headers,
    params={"table_name": table_name, "column_name": column_name},
    timeout=30
)
col_lineage = response.json()
print(f"Column lineage for {table_name}.{column_name}:")
print(json.dumps(col_lineage, indent=2))

# Trace upstream sources for a column
if "upstream_cols" in col_lineage:
    for col in col_lineage["upstream_cols"]:
        print(f"  Source: {col['table_name']}.{col['name']}")`,
  },

  {
    id: 28,
    category: 'Data Lineage',
    title: 'Table Dependencies Mapping',
    desc: 'Build a dependency graph of tables using information schema',
    code: `# Query table dependencies using information_schema
deps_df = spark.sql("""
    SELECT
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.table_type,
        t.created,
        t.last_altered,
        t.data_source_format
    FROM my_catalog.information_schema.tables t
    WHERE t.table_schema NOT IN ('information_schema')
    ORDER BY t.table_schema, t.table_name
""")
deps_df.show(truncate=False)

# Find views and their dependencies
views_df = spark.sql("""
    SELECT
        table_catalog, table_schema, table_name,
        view_definition
    FROM my_catalog.information_schema.views
    ORDER BY table_schema, table_name
""")
views_df.show(truncate=False)
print(f"Total tables: {deps_df.count()}, Total views: {views_df.count()}")`,
  },

  {
    id: 29,
    category: 'Data Lineage',
    title: 'Lineage via Information Schema',
    desc: 'Use information_schema to discover table relationships and constraints',
    code: `# List all columns across tables
columns_df = spark.sql("""
    SELECT
        table_catalog, table_schema, table_name,
        column_name, data_type, is_nullable,
        ordinal_position, comment
    FROM my_catalog.information_schema.columns
    WHERE table_schema = 'bronze'
    ORDER BY table_name, ordinal_position
""")
columns_df.show(100, truncate=False)

# Find table constraints (primary keys, foreign keys)
constraints_df = spark.sql("""
    SELECT
        constraint_catalog, constraint_schema, constraint_name,
        table_catalog, table_schema, table_name,
        constraint_type, enforced
    FROM my_catalog.information_schema.table_constraints
    ORDER BY table_name
""")
constraints_df.show(truncate=False)

# Check constraint columns
spark.sql("""
    SELECT * FROM my_catalog.information_schema.constraint_column_usage
""").show(truncate=False)`,
  },

  {
    id: 30,
    category: 'Data Lineage',
    title: 'Audit Table Access Patterns',
    desc: 'Analyze table access and usage patterns for lineage insights',
    code: `# Query table properties for last access info
from pyspark.sql.functions import col, current_timestamp, datediff

tables_info = spark.sql("""
    SELECT
        table_catalog, table_schema, table_name,
        table_type, created, last_altered,
        data_source_format
    FROM my_catalog.information_schema.tables
    WHERE table_schema NOT IN ('information_schema')
""")

# Find stale tables (not altered in 90+ days)
from pyspark.sql.functions import current_timestamp, datediff
stale_tables = tables_info.filter(
    datediff(current_timestamp(), col("last_altered")) > 90
)
print("Stale tables (not modified in 90+ days):")
stale_tables.show(truncate=False)

# Summarize tables per schema
tables_info.groupBy("table_schema", "table_type") \\
    .count() \\
    .orderBy("table_schema") \\
    .show(truncate=False)`,
  },

  // ===== External Locations (31-35) =====
  {
    id: 31,
    category: 'External Locations',
    title: 'Create Storage Credential',
    desc: 'Create a storage credential for cloud storage access',
    code: `# Create storage credential for AWS (IAM Role)
spark.sql("""
    CREATE STORAGE CREDENTIAL IF NOT EXISTS aws_s3_credential
    WITH (
        AWS_IAM_ROLE = 'arn:aws:iam::123456789012:role/unity-catalog-role'
    )
    COMMENT 'Storage credential for S3 access'
""")

# Create storage credential for Azure (Managed Identity)
# spark.sql("""
#     CREATE STORAGE CREDENTIAL IF NOT EXISTS azure_credential
#     WITH (
#         AZURE_MANAGED_IDENTITY = '/subscriptions/.../resourceGroups/.../providers/Microsoft.ManagedIdentity/userAssignedIdentities/my-identity'
#     )
# """)

# Describe credential
spark.sql("DESCRIBE STORAGE CREDENTIAL aws_s3_credential").show(truncate=False)
print("Storage credential created")`,
  },

  {
    id: 32,
    category: 'External Locations',
    title: 'Create External Location',
    desc: 'Create an external location mapping to cloud storage path',
    code: `# Create external location
spark.sql("""
    CREATE EXTERNAL LOCATION IF NOT EXISTS my_s3_location
    URL 's3://my-bucket/external-data/'
    WITH (STORAGE CREDENTIAL aws_s3_credential)
    COMMENT 'External location for raw data files'
""")

# Create external location for Azure
# spark.sql("""
#     CREATE EXTERNAL LOCATION IF NOT EXISTS my_adls_location
#     URL 'abfss://container@storageaccount.dfs.core.windows.net/data/'
#     WITH (STORAGE CREDENTIAL azure_credential)
# """)

# List external locations
spark.sql("SHOW EXTERNAL LOCATIONS").show(truncate=False)
print("External location created")`,
  },

  {
    id: 33,
    category: 'External Locations',
    title: 'Alter External Location',
    desc: 'Modify external location properties and credentials',
    code: `# Update external location URL
spark.sql("""
    ALTER EXTERNAL LOCATION my_s3_location
    SET URL 's3://my-bucket/external-data-v2/'
""")

# Update storage credential
spark.sql("""
    ALTER EXTERNAL LOCATION my_s3_location
    SET STORAGE CREDENTIAL new_s3_credential
""")

# Change owner
spark.sql("ALTER EXTERNAL LOCATION my_s3_location SET OWNER TO \`storage_admins\`")

# Update comment
spark.sql("""
    ALTER EXTERNAL LOCATION my_s3_location
    SET COMMENT 'Updated external location for production data'
""")
print("External location altered successfully")`,
  },

  {
    id: 34,
    category: 'External Locations',
    title: 'Validate External Location',
    desc: 'Validate access permissions on external locations',
    code: `# Validate external location access
validation = spark.sql("""
    VALIDATE EXTERNAL LOCATION my_s3_location
""")
validation.show(truncate=False)

# Describe external location details
spark.sql("DESCRIBE EXTERNAL LOCATION my_s3_location").show(truncate=False)

# List files in external location to verify access
try:
    files = dbutils.fs.ls("s3://my-bucket/external-data/")
    for f in files[:10]:
        print(f"  {f.name} - {f.size} bytes")
    print(f"Total files: {len(files)}")
except Exception as e:
    print(f"Access validation failed: {e}")`,
  },

  {
    id: 35,
    category: 'External Locations',
    title: 'Drop External Location and Credential',
    desc: 'Remove external locations and storage credentials',
    code: `# Drop external location
spark.sql("DROP EXTERNAL LOCATION IF EXISTS temp_location")

# Drop storage credential (must not be in use by any external location)
spark.sql("DROP STORAGE CREDENTIAL IF EXISTS temp_credential")

# List remaining external locations
ext_locations = spark.sql("SHOW EXTERNAL LOCATIONS")
ext_locations.show(truncate=False)

# List remaining storage credentials
credentials = spark.sql("SHOW STORAGE CREDENTIALS")
credentials.show(truncate=False)
print("Cleanup completed")`,
  },

  // ===== Volumes (36-40) =====
  {
    id: 36,
    category: 'Volumes',
    title: 'Create Managed Volume',
    desc: 'Create a managed volume for storing files within Unity Catalog',
    code: `# Create managed volume (Unity Catalog manages the storage)
spark.sql("""
    CREATE VOLUME IF NOT EXISTS my_catalog.bronze.raw_files
    COMMENT 'Managed volume for raw data files'
""")

# Verify volume creation
spark.sql("DESCRIBE VOLUME my_catalog.bronze.raw_files").show(truncate=False)

# List volumes in schema
spark.sql("SHOW VOLUMES IN my_catalog.bronze").show(truncate=False)

# Get volume path for file operations
volume_path = "/Volumes/my_catalog/bronze/raw_files"
print(f"Volume path: {volume_path}")`,
  },

  {
    id: 37,
    category: 'Volumes',
    title: 'Create External Volume',
    desc: 'Create an external volume pointing to cloud storage',
    code: `# Create external volume
spark.sql("""
    CREATE EXTERNAL VOLUME IF NOT EXISTS my_catalog.bronze.external_files
    LOCATION 's3://my-bucket/volumes/external-files/'
    COMMENT 'External volume for shared data files'
""")

# Describe external volume
spark.sql("DESCRIBE VOLUME my_catalog.bronze.external_files").show(truncate=False)

# List all volumes in schema
volumes_df = spark.sql("SHOW VOLUMES IN my_catalog.bronze")
volumes_df.show(truncate=False)
print("External volume created successfully")`,
  },

  {
    id: 38,
    category: 'Volumes',
    title: 'Read Files from Volume',
    desc: 'Read various file formats from Unity Catalog volumes',
    code: `# Read CSV file from volume
csv_df = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .load("/Volumes/my_catalog/bronze/raw_files/data.csv")
csv_df.show(5)

# Read JSON files from volume
json_df = spark.read.json("/Volumes/my_catalog/bronze/raw_files/events/*.json")
json_df.show(5)

# Read binary files (images, PDFs)
binary_df = spark.read.format("binaryFile") \\
    .load("/Volumes/my_catalog/bronze/raw_files/images/")
binary_df.select("path", "length", "modificationTime").show(truncate=False)

# List files in volume using dbutils
files = dbutils.fs.ls("/Volumes/my_catalog/bronze/raw_files/")
for f in files:
    print(f"  {f.name} ({f.size} bytes)")`,
  },

  {
    id: 39,
    category: 'Volumes',
    title: 'Write Files to Volume',
    desc: 'Write data and files to Unity Catalog volumes',
    code: `# Write DataFrame as CSV to volume
df = spark.sql("SELECT * FROM my_catalog.bronze.customers LIMIT 100")
df.write.format("csv") \\
    .option("header", "true") \\
    .mode("overwrite") \\
    .save("/Volumes/my_catalog/bronze/raw_files/exports/customers.csv")

# Write as Parquet
df.write.format("parquet") \\
    .mode("overwrite") \\
    .save("/Volumes/my_catalog/bronze/raw_files/exports/customers.parquet")

# Copy files to volume using dbutils
dbutils.fs.cp(
    "dbfs:/tmp/report.pdf",
    "/Volumes/my_catalog/bronze/raw_files/reports/report.pdf"
)

# Write text file using Python
with open("/Volumes/my_catalog/bronze/raw_files/notes.txt", "w") as f:
    f.write("Processing completed at: " + str(spark.sql("SELECT current_timestamp()").collect()[0][0]))
print("Files written to volume successfully")`,
  },

  {
    id: 40,
    category: 'Volumes',
    title: 'List and Drop Volumes',
    desc: 'Manage volumes - list, describe, and remove',
    code: `# List all volumes in a schema
volumes = spark.sql("SHOW VOLUMES IN my_catalog.bronze")
volumes.show(truncate=False)

# Describe volume details
spark.sql("DESCRIBE VOLUME my_catalog.bronze.raw_files").show(truncate=False)

# Grant access to volume
spark.sql("GRANT READ VOLUME ON VOLUME my_catalog.bronze.raw_files TO \`data_analysts\`")
spark.sql("GRANT WRITE VOLUME ON VOLUME my_catalog.bronze.raw_files TO \`data_engineers\`")

# Drop volume (managed volume deletes data, external keeps data)
spark.sql("DROP VOLUME IF EXISTS my_catalog.bronze.temp_volume")

# Verify
remaining = spark.sql("SHOW VOLUMES IN my_catalog.bronze")
remaining.show(truncate=False)
print("Volume management completed")`,
  },

  // ===== Functions (41-45) =====
  {
    id: 41,
    category: 'Functions',
    title: 'Create SQL UDF',
    desc: 'Create SQL user-defined functions in Unity Catalog',
    code: `# Create a simple SQL UDF
spark.sql("""
    CREATE OR REPLACE FUNCTION my_catalog.bronze.clean_phone(phone STRING)
    RETURNS STRING
    COMMENT 'Standardize phone number format'
    RETURN REGEXP_REPLACE(phone, '[^0-9]', '')
""")

# Create a UDF with conditional logic
spark.sql("""
    CREATE OR REPLACE FUNCTION my_catalog.bronze.categorize_amount(amount DOUBLE)
    RETURNS STRING
    COMMENT 'Categorize transaction amount'
    RETURN CASE
        WHEN amount < 100 THEN 'Small'
        WHEN amount < 1000 THEN 'Medium'
        WHEN amount < 10000 THEN 'Large'
        ELSE 'Enterprise'
    END
""")

# Use the UDF
spark.sql("""
    SELECT
        customer_id,
        my_catalog.bronze.clean_phone(phone) AS clean_phone,
        my_catalog.bronze.categorize_amount(amount) AS category
    FROM my_catalog.bronze.customers
    LIMIT 10
""").show()`,
  },

  {
    id: 42,
    category: 'Functions',
    title: 'Create Python UDF',
    desc: 'Create Python user-defined functions registered in Unity Catalog',
    code: `# Create Python UDF using PySpark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import hashlib

@udf(returnType=StringType())
def hash_email(email):
    """Hash email for pseudonymization"""
    if email is None:
        return None
    return hashlib.sha256(email.lower().strip().encode()).hexdigest()[:16]

# Register UDF in catalog
spark.udf.register("hash_email_udf", hash_email)

# Create SQL-registered Python UDF
spark.sql("""
    CREATE OR REPLACE FUNCTION my_catalog.bronze.hash_value(input_val STRING)
    RETURNS STRING
    LANGUAGE PYTHON
    AS $$
    import hashlib
    if input_val is None:
        return None
    return hashlib.sha256(input_val.encode()).hexdigest()[:16]
    $$
""")

# Use the UDF
spark.sql("""
    SELECT email, my_catalog.bronze.hash_value(email) AS hashed_email
    FROM my_catalog.bronze.customers LIMIT 5
""").show(truncate=False)`,
  },

  {
    id: 43,
    category: 'Functions',
    title: 'Alter and Describe Functions',
    desc: 'Modify function properties and view function metadata',
    code: `# Describe function details
spark.sql("DESCRIBE FUNCTION my_catalog.bronze.clean_phone").show(truncate=False)

# Describe function extended (includes body)
spark.sql("DESCRIBE FUNCTION EXTENDED my_catalog.bronze.clean_phone").show(truncate=False)

# List all functions in schema
spark.sql("SHOW FUNCTIONS IN my_catalog.bronze").show(truncate=False)

# List user-defined functions only
spark.sql("SHOW USER FUNCTIONS IN my_catalog.bronze").show(truncate=False)

# Change function owner
spark.sql("ALTER FUNCTION my_catalog.bronze.clean_phone SET OWNER TO \`data_engineers\`")

# Grant EXECUTE on function
spark.sql("GRANT EXECUTE ON FUNCTION my_catalog.bronze.clean_phone TO \`data_analysts\`")
print("Function metadata retrieved and permissions updated")`,
  },

  {
    id: 44,
    category: 'Functions',
    title: 'Drop Functions',
    desc: 'Remove user-defined functions from Unity Catalog',
    code: `# Drop a specific function
spark.sql("DROP FUNCTION IF EXISTS my_catalog.bronze.temp_function")

# Drop function (will fail if function does not exist without IF EXISTS)
spark.sql("DROP FUNCTION IF EXISTS my_catalog.bronze.old_udf")

# Verify function removal
remaining = spark.sql("SHOW USER FUNCTIONS IN my_catalog.bronze")
remaining.show(truncate=False)

# List all functions with their details
functions = remaining.collect()
for func in functions:
    print(f"Function: {func.function}")
    try:
        desc = spark.sql(f"DESCRIBE FUNCTION EXTENDED {func.function}")
        desc.show(truncate=False)
    except Exception as e:
        print(f"  Could not describe: {e}")`,
  },

  {
    id: 45,
    category: 'Functions',
    title: 'Table-Valued Functions',
    desc: 'Create and use table-valued functions in Unity Catalog',
    code: `# Create a table-valued function
spark.sql("""
    CREATE OR REPLACE FUNCTION my_catalog.bronze.get_customer_orders(cust_id INT)
    RETURNS TABLE (
        order_id INT,
        amount DOUBLE,
        order_date TIMESTAMP,
        status STRING
    )
    RETURN
        SELECT order_id, amount, order_date, status
        FROM my_catalog.bronze.orders
        WHERE customer_id = cust_id
        ORDER BY order_date DESC
""")

# Use table-valued function
spark.sql("""
    SELECT * FROM my_catalog.bronze.get_customer_orders(12345)
""").show()

# Use in JOIN
spark.sql("""
    SELECT c.name, o.*
    FROM my_catalog.bronze.customers c,
    LATERAL my_catalog.bronze.get_customer_orders(c.customer_id) o
    WHERE c.region = 'US'
    LIMIT 20
""").show()
print("Table-valued function created and tested")`,
  },

  // ===== Sharing (46-50) =====
  {
    id: 46,
    category: 'Sharing',
    title: 'Create Delta Share',
    desc: 'Create a Delta Sharing share for cross-organization data sharing',
    code: `# Create a share
spark.sql("""
    CREATE SHARE IF NOT EXISTS customer_data_share
    COMMENT 'Share of customer data for partner organizations'
""")

# Describe the share
spark.sql("DESCRIBE SHARE customer_data_share").show(truncate=False)

# List all shares
spark.sql("SHOW SHARES").show(truncate=False)

# Alter share comment
spark.sql("""
    ALTER SHARE customer_data_share
    SET COMMENT 'Updated: Customer data share for Q1 2024 partners'
""")
print("Share created successfully")`,
  },

  {
    id: 47,
    category: 'Sharing',
    title: 'Add Tables to Share',
    desc: 'Add tables and partitions to a Delta Sharing share',
    code: `# Add a table to share
spark.sql("""
    ALTER SHARE customer_data_share
    ADD TABLE my_catalog.gold.customer_summary
""")

# Add table with alias (shared under different name)
spark.sql("""
    ALTER SHARE customer_data_share
    ADD TABLE my_catalog.gold.revenue_report
    AS shared_schema.revenue_data
""")

# Add table with partition filter
spark.sql("""
    ALTER SHARE customer_data_share
    ADD TABLE my_catalog.gold.orders
    PARTITION (region = 'US')
""")

# Add table with history (enable CDF for recipients)
spark.sql("""
    ALTER SHARE customer_data_share
    ADD TABLE my_catalog.gold.customer_events
    WITH HISTORY
""")

# Show share contents
spark.sql("SHOW ALL IN SHARE customer_data_share").show(truncate=False)`,
  },

  {
    id: 48,
    category: 'Sharing',
    title: 'Create Recipient',
    desc: 'Create a Delta Sharing recipient for external access',
    code: `# Create recipient (open sharing - token-based)
spark.sql("""
    CREATE RECIPIENT IF NOT EXISTS partner_org
    COMMENT 'Partner organization for data sharing'
""")

# Create recipient with specific sharing identifier (Databricks-to-Databricks)
# spark.sql("""
#     CREATE RECIPIENT IF NOT EXISTS partner_databricks
#     USING ID 'aws:us-west-2:partner-workspace-id'
# """)

# Describe recipient
spark.sql("DESCRIBE RECIPIENT partner_org").show(truncate=False)

# List all recipients
spark.sql("SHOW RECIPIENTS").show(truncate=False)

# Rotate recipient token
spark.sql("ALTER RECIPIENT partner_org ROTATE TOKEN")
print("Recipient created - share activation token with partner")`,
  },

  {
    id: 49,
    category: 'Sharing',
    title: 'Activate Sharing',
    desc: 'Grant share access to recipients and manage permissions',
    code: `# Grant share to recipient
spark.sql("GRANT SELECT ON SHARE customer_data_share TO RECIPIENT partner_org")

# Show grants on share
spark.sql("SHOW GRANTS ON SHARE customer_data_share").show(truncate=False)

# Show share grants for a recipient
spark.sql("SHOW GRANTS TO RECIPIENT partner_org").show(truncate=False)

# Revoke share from recipient
# spark.sql("REVOKE SELECT ON SHARE customer_data_share FROM RECIPIENT partner_org")

# Recipient-side: create catalog from share (done by recipient)
# spark.sql("""
#     CREATE CATALOG IF NOT EXISTS partner_shared_data
#     USING SHARE provider_org.customer_data_share
# """)
print("Share access granted to recipient")`,
  },

  {
    id: 50,
    category: 'Sharing',
    title: 'Remove Tables and Drop Share',
    desc: 'Clean up Delta Sharing resources',
    code: `# Remove table from share
spark.sql("""
    ALTER SHARE customer_data_share
    REMOVE TABLE my_catalog.gold.customer_summary
""")

# Remove all tables from share
tables_in_share = spark.sql("SHOW ALL IN SHARE customer_data_share").collect()
for row in tables_in_share:
    table_name = row.name
    spark.sql(f"ALTER SHARE customer_data_share REMOVE TABLE {table_name}")
    print(f"Removed {table_name} from share")

# Revoke all access
spark.sql("REVOKE SELECT ON SHARE customer_data_share FROM RECIPIENT partner_org")

# Drop recipient
spark.sql("DROP RECIPIENT IF EXISTS partner_org")

# Drop share
spark.sql("DROP SHARE IF EXISTS customer_data_share")
print("Share and recipient cleaned up successfully")`,
  },

  // ===== Audit (51-52) =====
  {
    id: 51,
    category: 'Audit',
    title: 'Audit Logging for Unity Catalog',
    desc: 'Query Unity Catalog audit logs for governance and compliance',
    code: `# Query system audit logs (available in system.access schema)
audit_df = spark.sql("""
    SELECT
        event_time,
        event_type,
        user_identity.email AS user_email,
        service_name,
        action_name,
        request_params,
        response.status_code,
        source_ip_address
    FROM system.access.audit
    WHERE service_name = 'unityCatalog'
      AND event_time > current_timestamp() - INTERVAL 7 DAYS
    ORDER BY event_time DESC
    LIMIT 100
""")
audit_df.show(truncate=False)

# Summarize actions by user
spark.sql("""
    SELECT
        user_identity.email AS user_email,
        action_name,
        COUNT(*) AS action_count
    FROM system.access.audit
    WHERE service_name = 'unityCatalog'
      AND event_time > current_timestamp() - INTERVAL 7 DAYS
    GROUP BY user_identity.email, action_name
    ORDER BY action_count DESC
""").show(truncate=False)`,
  },

  {
    id: 52,
    category: 'Audit',
    title: 'Track Data Access Patterns',
    desc: 'Monitor who accessed which tables and when for compliance',
    code: `# Track table read/write access
access_df = spark.sql("""
    SELECT
        event_time,
        user_identity.email AS user_email,
        action_name,
        request_params.full_name_arg AS table_name,
        response.status_code,
        source_ip_address
    FROM system.access.audit
    WHERE service_name = 'unityCatalog'
      AND action_name IN ('getTable', 'createTable', 'deleteTable', 'generateTemporaryTableCredential')
      AND event_time > current_timestamp() - INTERVAL 30 DAYS
    ORDER BY event_time DESC
""")
access_df.show(50, truncate=False)

# Find sensitive table access
spark.sql("""
    SELECT
        user_identity.email,
        request_params.full_name_arg AS table_accessed,
        COUNT(*) AS access_count,
        MIN(event_time) AS first_access,
        MAX(event_time) AS last_access
    FROM system.access.audit
    WHERE service_name = 'unityCatalog'
      AND action_name = 'generateTemporaryTableCredential'
      AND request_params.full_name_arg LIKE '%pii%'
      AND event_time > current_timestamp() - INTERVAL 30 DAYS
    GROUP BY user_identity.email, request_params.full_name_arg
    ORDER BY access_count DESC
""").show(truncate=False)`,
  },

  // ===== Migration (53-55) =====
  {
    id: 53,
    category: 'Migration',
    title: 'Migrate from Hive Metastore',
    desc: 'Migrate tables from legacy Hive metastore to Unity Catalog',
    code: `# List tables in Hive metastore (legacy)
hive_tables = spark.sql("SHOW TABLES IN hive_metastore.default")
hive_tables.show(truncate=False)

# Migrate a single table using CTAS
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.bronze.migrated_customers
    AS SELECT * FROM hive_metastore.default.customers
""")

# Migrate with full schema preservation
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.bronze.migrated_orders
    USING DELTA
    TBLPROPERTIES ('migrated_from' = 'hive_metastore.default.orders',
                   'migration_date' = current_date())
    AS SELECT * FROM hive_metastore.default.orders
""")

# Batch migrate all tables in a schema
hive_tables_list = spark.sql("SHOW TABLES IN hive_metastore.default").collect()
for tbl in hive_tables_list:
    table_name = tbl.tableName
    print(f"Migrating: hive_metastore.default.{table_name}")
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS my_catalog.bronze.{table_name}
            AS SELECT * FROM hive_metastore.default.{table_name}
        """)
        print(f"  Migrated successfully")
    except Exception as e:
        print(f"  Migration failed: {e}")`,
  },

  {
    id: 54,
    category: 'Migration',
    title: 'Upgrade Tables to Unity Catalog',
    desc: 'Upgrade existing managed and external tables to Unity Catalog',
    code: `# Upgrade managed table (moves metadata to Unity Catalog)
# This uses the UC upgrade wizard API approach
import requests

workspace_url = dbutils.notebook.entry_point.getDbutils() \\
    .notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils() \\
    .notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Upgrade a single table
upgrade_payload = {
    "src_hive_metastore": "hive_metastore",
    "src_schema": "default",
    "src_table": "customers",
    "dst_catalog": "my_catalog",
    "dst_schema": "bronze",
    "dst_table": "customers"
}

response = requests.post(
    f"{workspace_url}/api/2.0/unity-catalog/tables/upgrade",
    headers=headers,
    json=upgrade_payload,
    timeout=60
)
print(f"Upgrade status: {response.status_code}")
print(response.json())

# Verify upgraded table
spark.sql("DESCRIBE TABLE EXTENDED my_catalog.bronze.customers").show(truncate=False)`,
  },

  {
    id: 55,
    category: 'Migration',
    title: 'Sync Metastore and Validate Migration',
    desc: 'Validate migration completeness and sync metadata between metastores',
    code: `# Compare table counts between Hive metastore and Unity Catalog
hive_tables = spark.sql("SHOW TABLES IN hive_metastore.default")
uc_tables = spark.sql("SHOW TABLES IN my_catalog.bronze")

hive_count = hive_tables.count()
uc_count = uc_tables.count()
print(f"Hive metastore tables: {hive_count}")
print(f"Unity Catalog tables: {uc_count}")

# Validate row counts match for each migrated table
hive_list = [row.tableName for row in hive_tables.collect()]
uc_list = [row.tableName for row in uc_tables.collect()]

missing = set(hive_list) - set(uc_list)
if missing:
    print(f"Tables NOT yet migrated: {missing}")

# Validate data integrity for migrated tables
for table_name in set(hive_list) & set(uc_list):
    hive_count = spark.table(f"hive_metastore.default.{table_name}").count()
    uc_count = spark.table(f"my_catalog.bronze.{table_name}").count()
    status = "MATCH" if hive_count == uc_count else "MISMATCH"
    print(f"  {table_name}: Hive={hive_count}, UC={uc_count} [{status}]")

# Sync table properties
for table_name in set(hive_list) & set(uc_list):
    props = spark.sql(f"SHOW TBLPROPERTIES hive_metastore.default.{table_name}").collect()
    for prop in props:
        if not prop.key.startswith("delta."):
            spark.sql(f"""
                ALTER TABLE my_catalog.bronze.{table_name}
                SET TBLPROPERTIES ('{prop.key}' = '{prop.value}')
            """)
print("Migration validation and sync completed")`,
  },
];

const categories = [...new Set(unityCatalogScenarios.map((s) => s.category))];

function UnityCatalog() {
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const filtered = unityCatalogScenarios.filter((s) => {
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
          <h1>Unity Catalog Scenarios</h1>
          <p>{unityCatalogScenarios.length} PySpark Unity Catalog patterns for Databricks</p>
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
            <option value="All">All Categories ({unityCatalogScenarios.length})</option>
            {categories.map((cat) => (
              <option key={cat} value={cat}>
                {cat} ({unityCatalogScenarios.filter((s) => s.category === cat).length})
              </option>
            ))}
          </select>
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Showing {filtered.length} of {unityCatalogScenarios.length}
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
                  <span className="badge success">{scenario.category}</span>
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

export default UnityCatalog;
