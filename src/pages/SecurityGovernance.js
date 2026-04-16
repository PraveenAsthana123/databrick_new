import React, { useState } from 'react';
import ScenarioCard from '../components/common/ScenarioCard';

const securityScenarios = [
  {
    id: 1,
    category: 'Encryption',
    title: 'Column-Level Encryption with AES',
    desc: 'Encrypt sensitive columns using AES-256 encryption before writing to Delta tables',
    code: `from pyspark.sql.functions import aes_encrypt, aes_decrypt, col, lit

key = dbutils.secrets.get("security-scope", "aes-encryption-key")

# Encrypt sensitive columns before storing
encrypted_df = df.withColumn(
    "ssn_encrypted", aes_encrypt(col("ssn"), lit(key), lit("GCM"))
).withColumn(
    "credit_card_encrypted", aes_encrypt(col("credit_card"), lit(key), lit("GCM"))
).drop("ssn", "credit_card")

encrypted_df.write.format("delta").mode("overwrite") \\
    .saveAsTable("secure.encrypted_customer_data")

# Decrypt when reading (authorized users only)
decrypted_df = spark.table("secure.encrypted_customer_data") \\
    .withColumn("ssn", aes_decrypt(col("ssn_encrypted"), lit(key), lit("GCM")).cast("string")) \\
    .withColumn("credit_card", aes_decrypt(col("credit_card_encrypted"), lit(key), lit("GCM")).cast("string"))`,
  },
  {
    id: 2,
    category: 'Encryption',
    title: 'Row-Level Security with Dynamic Views',
    desc: 'Implement row-level security using dynamic views based on user groups',
    code: `-- Create a dynamic view that filters rows based on current user's group
CREATE OR REPLACE VIEW secure.customer_view AS
SELECT *
FROM gold.customers
WHERE
  region = CASE
    WHEN is_member('us_team') THEN 'US'
    WHEN is_member('eu_team') THEN 'EU'
    WHEN is_member('admin_group') THEN region
    ELSE NULL
  END;

-- Grant access to the view, not the underlying table
GRANT SELECT ON VIEW secure.customer_view TO \`data_analysts\`;
REVOKE SELECT ON TABLE gold.customers FROM \`data_analysts\`;`,
  },
  {
    id: 3,
    category: 'Encryption',
    title: 'Data Masking with Column Masks',
    desc: 'Apply dynamic data masking to sensitive columns using Unity Catalog column masks',
    code: `-- Create masking function
CREATE OR REPLACE FUNCTION secure.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_viewers') THEN ssn
  ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4))
END;

CREATE OR REPLACE FUNCTION secure.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_viewers') THEN email
  ELSE CONCAT(LEFT(email, 2), '***@', SPLIT(email, '@')[1])
END;

-- Apply column masks to table
ALTER TABLE gold.customers
ALTER COLUMN ssn SET MASK secure.mask_ssn;

ALTER TABLE gold.customers
ALTER COLUMN email SET MASK secure.mask_email;`,
  },
  {
    id: 4,
    category: 'Encryption',
    title: 'Transparent Data Encryption at Rest',
    desc: 'Configure customer-managed keys for Delta table encryption at rest',
    code: `# Configure workspace with customer-managed keys (CMK)
# Azure: Use Azure Key Vault for CMK
# AWS: Use AWS KMS for CMK

# Terraform configuration for CMK encryption
resource "databricks_workspace" "this" {
  managed_services_customer_managed_key_id = azurerm_key_vault_key.databricks.id
  managed_disk_customer_managed_key_id     = azurerm_key_vault_key.databricks_disk.id
}

# Verify encryption status via Spark
spark.conf.get("spark.databricks.io.encryption.enabled")
# Returns: "true"

# All Delta tables automatically encrypted at rest
df.write.format("delta").saveAsTable("secure.encrypted_table")`,
  },
  {
    id: 5,
    category: 'Access Control',
    title: 'RBAC with Unity Catalog',
    desc: 'Implement role-based access control using Unity Catalog privileges',
    code: `-- Create groups and assign privileges
CREATE GROUP data_engineers;
CREATE GROUP data_analysts;
CREATE GROUP data_scientists;

-- Catalog-level privileges
GRANT USE CATALOG ON CATALOG main TO \`data_engineers\`;
GRANT USE CATALOG ON CATALOG main TO \`data_analysts\`;

-- Schema-level privileges
GRANT USE SCHEMA ON SCHEMA main.gold TO \`data_analysts\`;
GRANT SELECT ON SCHEMA main.gold TO \`data_analysts\`;

-- Table-level privileges
GRANT ALL PRIVILEGES ON TABLE main.bronze.raw_events TO \`data_engineers\`;
GRANT SELECT ON TABLE main.gold.customer_360 TO \`data_analysts\`;
GRANT SELECT, MODIFY ON TABLE main.silver.features TO \`data_scientists\`;

-- Deny access to sensitive schemas
DENY ALL PRIVILEGES ON SCHEMA main.secure TO \`data_analysts\`;

-- View current grants
SHOW GRANTS ON TABLE main.gold.customer_360;`,
  },
  {
    id: 6,
    category: 'Access Control',
    title: 'Attribute-Based Access Control (ABAC)',
    desc: 'Implement fine-grained access control using tags and row filters',
    code: `-- Create row filter function based on user attributes
CREATE OR REPLACE FUNCTION secure.region_filter(region_col STRING)
RETURNS BOOLEAN
RETURN CASE
  WHEN is_member('global_admin') THEN TRUE
  WHEN is_member('us_team') AND region_col = 'US' THEN TRUE
  WHEN is_member('eu_team') AND region_col = 'EU' THEN TRUE
  WHEN is_member('apac_team') AND region_col = 'APAC' THEN TRUE
  ELSE FALSE
END;

-- Apply row filter to table
ALTER TABLE gold.sales_data
SET ROW FILTER secure.region_filter ON (region);

-- Tag sensitive columns for policy enforcement
ALTER TABLE gold.customers ALTER COLUMN ssn SET TAGS ('pii' = 'true', 'sensitivity' = 'high');
ALTER TABLE gold.customers ALTER COLUMN email SET TAGS ('pii' = 'true', 'sensitivity' = 'medium');

-- Query tags for auditing
SELECT * FROM system.information_schema.column_tags
WHERE tag_name = 'pii' AND tag_value = 'true';`,
  },
  {
    id: 7,
    category: 'Access Control',
    title: 'Service Principal Authentication',
    desc: 'Configure service principals for automated pipeline access',
    code: `# Create service principal via Databricks CLI
# databricks service-principals create --display-name "etl-pipeline-sp"

# Configure service principal with OAuth
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
    host="https://adb-1234567890.1.azuredatabricks.net",
    client_id=dbutils.secrets.get("auth-scope", "sp-client-id"),
    client_secret=dbutils.secrets.get("auth-scope", "sp-client-secret")
)

# Grant service principal access to resources
# SQL: GRANT USE CATALOG ON CATALOG main TO \`sp-etl-pipeline\`;
# SQL: GRANT ALL PRIVILEGES ON SCHEMA main.bronze TO \`sp-etl-pipeline\`;

# Use in automated jobs
spark.conf.set("spark.databricks.service.principal.id",
    dbutils.secrets.get("auth-scope", "sp-client-id"))`,
  },
  {
    id: 8,
    category: 'Access Control',
    title: 'Managed Identity Integration',
    desc: 'Use Azure/AWS managed identities for secure cross-service authentication',
    code: `# Azure: Configure managed identity for storage access
spark.conf.set(
    "fs.azure.account.auth.type.mystorage.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.mystorage.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.msi.tenant.mystorage.dfs.core.windows.net",
    dbutils.secrets.get("azure-scope", "tenant-id")
)

# AWS: Configure instance profile for S3 access
# Instance profile ARN attached to Databricks cluster
spark.conf.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.InstanceProfileCredentialsProvider"
)

# Read data using managed identity (no credentials in code)
df = spark.read.format("delta").load(
    "abfss://container@mystorage.dfs.core.windows.net/data/"
)`,
  },
  {
    id: 9,
    category: 'Network',
    title: 'VNet Injection and Private Endpoints',
    desc: 'Configure VNet-injected workspace with private endpoints for secure network access',
    code: `# Terraform: Azure VNet-injected Databricks workspace
resource "azurerm_databricks_workspace" "secure" {
  name                        = "secure-databricks-ws"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = "premium"
  managed_resource_group_name = "databricks-managed-rg"

  custom_parameters {
    virtual_network_id  = azurerm_virtual_network.this.id
    public_subnet_name  = azurerm_subnet.public.name
    private_subnet_name = azurerm_subnet.private.name
    no_public_ip        = true  # Secure connectivity cluster (NPIP)
  }
}

# Private endpoint for workspace
resource "azurerm_private_endpoint" "databricks" {
  name                = "databricks-pe"
  subnet_id           = azurerm_subnet.endpoints.id
  private_service_connection {
    name                           = "databricks-psc"
    private_connection_resource_id = azurerm_databricks_workspace.secure.id
    subresource_names              = ["databricks_ui_api"]
    is_manual_connection           = false
  }
}`,
  },
  {
    id: 10,
    category: 'Network',
    title: 'IP Access Lists',
    desc: 'Restrict workspace access using IP allowlists and denylists',
    code: `# Configure IP access lists via Databricks REST API
import requests

headers = {"Authorization": f"Bearer {token}"}
base_url = "https://adb-1234567890.1.azuredatabricks.net/api/2.0"

# Create IP allowlist
response = requests.post(
    f"{base_url}/ip-access-lists",
    headers=headers,
    json={
        "label": "Corporate VPN",
        "list_type": "ALLOW",
        "ip_addresses": [
            "10.0.0.0/8",
            "172.16.0.0/12",
            "203.0.113.0/24"
        ]
    },
    timeout=30
)

# Create IP denylist for known bad actors
response = requests.post(
    f"{base_url}/ip-access-lists",
    headers=headers,
    json={
        "label": "Blocked IPs",
        "list_type": "BLOCK",
        "ip_addresses": ["198.51.100.0/24"]
    },
    timeout=30
)

# Enable IP access list feature
requests.patch(
    f"{base_url}/workspace-conf",
    headers=headers,
    json={"enableIpAccessLists": "true"},
    timeout=30
)`,
  },
  {
    id: 11,
    category: 'Network',
    title: 'Private Link Connectivity',
    desc: 'Configure AWS PrivateLink for secure Databricks connectivity',
    code: `# Terraform: AWS PrivateLink for Databricks
resource "aws_vpc_endpoint" "databricks_workspace" {
  vpc_id             = aws_vpc.main.id
  service_name       = "com.amazonaws.vpce.us-east-1.vpce-svc-databricks-workspace"
  vpc_endpoint_type  = "Interface"
  subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids = [aws_security_group.databricks_pl.id]
  private_dns_enabled = true
}

resource "aws_vpc_endpoint" "databricks_relay" {
  vpc_id             = aws_vpc.main.id
  service_name       = "com.amazonaws.vpce.us-east-1.vpce-svc-databricks-relay"
  vpc_endpoint_type  = "Interface"
  subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids = [aws_security_group.databricks_pl.id]
  private_dns_enabled = true
}

# Security group for PrivateLink
resource "aws_security_group" "databricks_pl" {
  vpc_id = aws_vpc.main.id
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }
}`,
  },
  {
    id: 12,
    category: 'Secrets',
    title: 'Databricks Secrets Management',
    desc: 'Store and retrieve secrets using Databricks secret scopes',
    code: `# Create secret scope (Databricks-backed)
# CLI: databricks secrets create-scope --scope my-secrets

# Store secrets
# CLI: databricks secrets put --scope my-secrets --key db-password
# CLI: databricks secrets put --scope my-secrets --key api-key

# Retrieve secrets in notebooks/jobs
db_password = dbutils.secrets.get(scope="my-secrets", key="db-password")
api_key = dbutils.secrets.get(scope="my-secrets", key="api-key")

# Use in JDBC connections (secrets are redacted in logs)
df = spark.read.format("jdbc") \\
    .option("url", "jdbc:postgresql://host:5432/mydb") \\
    .option("user", dbutils.secrets.get("my-secrets", "db-user")) \\
    .option("password", dbutils.secrets.get("my-secrets", "db-password")) \\
    .option("dbtable", "public.customers") \\
    .load()

# List available scopes and keys
scopes = dbutils.secrets.listScopes()
keys = dbutils.secrets.list("my-secrets")`,
  },
  {
    id: 13,
    category: 'Secrets',
    title: 'Azure Key Vault Integration',
    desc: 'Configure Azure Key Vault-backed secret scope for centralized secret management',
    code: `# Create Azure Key Vault-backed secret scope
# Navigate to: https://<workspace-url>#secrets/createScope

# Configuration:
# Scope Name: azure-kv-scope
# Manage Principal: All Users (or Creator)
# DNS Name: https://my-keyvault.vault.azure.net/
# Resource ID: /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/my-keyvault

# Access secrets from Azure Key Vault
storage_key = dbutils.secrets.get("azure-kv-scope", "storage-account-key")
sql_password = dbutils.secrets.get("azure-kv-scope", "sql-server-password")
service_bus_conn = dbutils.secrets.get("azure-kv-scope", "servicebus-connection-string")

# Configure storage with Key Vault secret
spark.conf.set(
    "fs.azure.account.key.mystorage.blob.core.windows.net",
    dbutils.secrets.get("azure-kv-scope", "storage-account-key")
)

# Secrets are automatically rotated when updated in Key Vault
# No code changes needed after rotation`,
  },
  {
    id: 14,
    category: 'Secrets',
    title: 'AWS Secrets Manager Integration',
    desc: 'Retrieve secrets from AWS Secrets Manager in Databricks on AWS',
    code: `import boto3
import json

def get_aws_secret(secret_name, region="us-east-1"):
    """Retrieve secret from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region
    )
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# Retrieve database credentials
db_creds = get_aws_secret("prod/database/postgres")
db_host = db_creds["host"]
db_user = db_creds["username"]
db_pass = db_creds["password"]

# Use in Spark JDBC connection
df = spark.read.format("jdbc") \\
    .option("url", f"jdbc:postgresql://{db_host}:5432/analytics") \\
    .option("user", db_user) \\
    .option("password", db_pass) \\
    .option("dbtable", "public.transactions") \\
    .load()

# Rotate secrets automatically with Lambda + Secrets Manager rotation policy`,
  },
  {
    id: 15,
    category: 'Compliance',
    title: 'GDPR Data Deletion (Right to Erasure)',
    desc: 'Implement GDPR-compliant data deletion across Delta tables using Delta time travel',
    code: `from delta.tables import DeltaTable

def gdpr_delete_customer(customer_id):
    """Delete all customer data across tables for GDPR compliance."""
    tables_with_customer_data = [
        "gold.customers",
        "gold.orders",
        "gold.interactions",
        "silver.customer_events",
        "bronze.raw_customer_data"
    ]

    for table_name in tables_with_customer_data:
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.delete(f"customer_id = '{customer_id}'")

    # Vacuum to permanently remove data (override retention)
    for table_name in tables_with_customer_data:
        spark.sql(f"VACUUM {table_name} RETAIN 0 HOURS")

    # Log deletion for audit trail
    spark.sql(f"""
        INSERT INTO compliance.deletion_log
        VALUES ('{customer_id}', current_timestamp(), 'GDPR_ERASURE', current_user())
    """)

    return f"Customer {customer_id} data deleted from {len(tables_with_customer_data)} tables"

# Execute deletion
gdpr_delete_customer("CUST-12345")`,
  },
  {
    id: 16,
    category: 'Compliance',
    title: 'PII Detection and Classification',
    desc: 'Automatically detect and classify PII columns across tables',
    code: `import re
from pyspark.sql.functions import col, when, lit

def detect_pii_columns(table_name, sample_size=1000):
    """Scan table columns for potential PII data."""
    df = spark.table(table_name).limit(sample_size)
    pii_patterns = {
        "SSN": r"\\b\\d{3}-\\d{2}-\\d{4}\\b",
        "Email": r"\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b",
        "Phone": r"\\b\\d{3}[-.]?\\d{3}[-.]?\\d{4}\\b",
        "Credit Card": r"\\b\\d{4}[-\\s]?\\d{4}[-\\s]?\\d{4}[-\\s]?\\d{4}\\b",
        "IP Address": r"\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b",
    }
    pii_results = []
    for column in df.columns:
        if df.schema[column].dataType.simpleString() == "string":
            sample_values = [row[column] for row in df.select(column).collect() if row[column]]
            for pii_type, pattern in pii_patterns.items():
                matches = sum(1 for val in sample_values if re.search(pattern, str(val)))
                if matches > len(sample_values) * 0.1:
                    pii_results.append({
                        "table": table_name, "column": column,
                        "pii_type": pii_type, "confidence": matches / len(sample_values)
                    })
    return pii_results

# Scan all tables in a schema
tables = spark.catalog.listTables("gold")
all_pii = []
for table in tables:
    all_pii.extend(detect_pii_columns(f"gold.{table.name}"))

pii_df = spark.createDataFrame(all_pii)
pii_df.write.format("delta").mode("overwrite").saveAsTable("compliance.pii_inventory")`,
  },
  {
    id: 17,
    category: 'Compliance',
    title: 'Data Classification Tagging',
    desc: 'Apply classification tags to tables and columns for compliance tracking',
    code: `-- Classify tables by sensitivity level
ALTER TABLE gold.customers SET TAGS ('classification' = 'confidential');
ALTER TABLE gold.transactions SET TAGS ('classification' = 'restricted');
ALTER TABLE gold.product_catalog SET TAGS ('classification' = 'public');

-- Tag columns with PII indicators
ALTER TABLE gold.customers ALTER COLUMN ssn
    SET TAGS ('pii' = 'true', 'sensitivity' = 'high', 'gdpr_relevant' = 'true');
ALTER TABLE gold.customers ALTER COLUMN email
    SET TAGS ('pii' = 'true', 'sensitivity' = 'medium', 'gdpr_relevant' = 'true');
ALTER TABLE gold.customers ALTER COLUMN name
    SET TAGS ('pii' = 'true', 'sensitivity' = 'medium', 'gdpr_relevant' = 'true');
ALTER TABLE gold.customers ALTER COLUMN customer_id
    SET TAGS ('pii' = 'false', 'sensitivity' = 'low');

-- Query classified assets
SELECT table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE tag_name IN ('pii', 'sensitivity', 'gdpr_relevant')
ORDER BY tag_value DESC;

-- Generate compliance report
SELECT tag_value AS classification,
       COUNT(DISTINCT table_name) AS table_count
FROM system.information_schema.table_tags
WHERE tag_name = 'classification'
GROUP BY tag_value;`,
  },
  {
    id: 18,
    category: 'Compliance',
    title: 'Audit Trail and Access Logging',
    desc: 'Enable and query comprehensive audit logs for compliance reporting',
    code: `-- Query Unity Catalog audit logs (system tables)
SELECT
    event_time,
    user_identity.email AS user_email,
    action_name,
    request_params.full_name_arg AS resource,
    response.status_code
FROM system.access.audit
WHERE action_name IN ('getTable', 'createTable', 'deleteTable', 'grantPermission')
    AND event_date >= current_date() - INTERVAL 30 DAYS
ORDER BY event_time DESC;

-- Track data access patterns
SELECT
    user_identity.email,
    request_params.full_name_arg AS table_accessed,
    COUNT(*) AS access_count,
    MIN(event_time) AS first_access,
    MAX(event_time) AS last_access
FROM system.access.audit
WHERE action_name = 'getTable'
    AND event_date >= current_date() - INTERVAL 90 DAYS
GROUP BY 1, 2
ORDER BY access_count DESC;

-- Alert on suspicious activity
SELECT user_identity.email, COUNT(*) AS failed_attempts
FROM system.access.audit
WHERE response.status_code >= 400
    AND event_date >= current_date() - INTERVAL 1 DAY
GROUP BY 1
HAVING COUNT(*) > 50;`,
  },
  {
    id: 19,
    category: 'Encryption',
    title: 'Token-Based Data Tokenization',
    desc: 'Replace sensitive data with non-reversible tokens for analytics',
    code: `from pyspark.sql.functions import sha2, concat, col, lit

def tokenize_column(df, column_name, salt="my_secret_salt"):
    """Replace sensitive values with SHA-256 tokens."""
    return df.withColumn(
        f"{column_name}_token",
        sha2(concat(col(column_name), lit(salt)), 256)
    ).drop(column_name)

# Tokenize PII columns
tokenized_df = spark.table("gold.customers")
tokenized_df = tokenize_column(tokenized_df, "ssn")
tokenized_df = tokenize_column(tokenized_df, "email")
tokenized_df = tokenize_column(tokenized_df, "phone")

# Save tokenized version for analytics
tokenized_df.write.format("delta") \\
    .mode("overwrite") \\
    .saveAsTable("analytics.tokenized_customers")

# Create token mapping table (restricted access)
mapping_df = spark.table("gold.customers").select(
    sha2(concat(col("ssn"), lit("my_secret_salt")), 256).alias("ssn_token"),
    col("ssn")
)
mapping_df.write.format("delta") \\
    .mode("overwrite") \\
    .saveAsTable("secure.token_mapping")`,
  },
  {
    id: 20,
    category: 'Compliance',
    title: 'Data Residency and Sovereignty Controls',
    desc: 'Enforce data residency requirements using region-specific storage and policies',
    code: `# Configure region-specific storage locations
region_configs = {
    "EU": "abfss://eu-data@eustorage.dfs.core.windows.net/",
    "US": "s3://us-data-bucket/",
    "APAC": "abfss://apac-data@apacstorage.dfs.core.windows.net/"
}

def write_with_residency(df, table_name, region):
    """Write data to region-specific storage for data residency compliance."""
    location = region_configs.get(region)
    if not location:
        raise ValueError(f"Unknown region: {region}")
    df.write.format("delta") \\
        .mode("overwrite") \\
        .option("path", f"{location}{table_name}") \\
        .saveAsTable(f"{region.lower()}_catalog.{table_name}")

# Partition and route data by region
from pyspark.sql.functions import col

customers_df = spark.table("staging.global_customers")
for region in ["EU", "US", "APAC"]:
    region_df = customers_df.filter(col("data_residency_region") == region)
    write_with_residency(region_df, "customers", region)

# Validate no cross-region data leakage
for region, path in region_configs.items():
    count = spark.read.format("delta").load(f"{path}customers").count()
    print(f"{region}: {count} customer records")`,
  },
];

const governanceScenarios = [
  {
    id: 1,
    category: 'Cataloging',
    title: 'Unity Catalog Setup and Configuration',
    desc: 'Configure Unity Catalog metastore with three-level namespace for data governance',
    code: `-- Create catalog and schemas in Unity Catalog
CREATE CATALOG IF NOT EXISTS enterprise_data
COMMENT 'Enterprise-wide data catalog for all business domains';

CREATE SCHEMA IF NOT EXISTS enterprise_data.bronze
COMMENT 'Raw ingested data - append only';

CREATE SCHEMA IF NOT EXISTS enterprise_data.silver
COMMENT 'Cleansed and conformed data';

CREATE SCHEMA IF NOT EXISTS enterprise_data.gold
COMMENT 'Business-level aggregates and ML-ready features';

-- Set catalog and schema ownership
ALTER CATALOG enterprise_data SET OWNER TO \`data_platform_admins\`;
ALTER SCHEMA enterprise_data.gold SET OWNER TO \`data_engineers\`;

-- Configure managed storage locations
CREATE EXTERNAL LOCATION IF NOT EXISTS enterprise_storage
URL 'abfss://enterprise@storage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL enterprise_cred)
COMMENT 'Primary storage for enterprise data';

-- Set default managed location for catalog
ALTER CATALOG enterprise_data
SET (MANAGED LOCATION = 'abfss://enterprise@storage.dfs.core.windows.net/managed/');`,
  },
  {
    id: 2,
    category: 'Cataloging',
    title: 'Tag Management and Discovery',
    desc: 'Organize and discover data assets using Unity Catalog tags',
    code: `-- Apply business domain tags
ALTER TABLE gold.customers SET TAGS ('domain' = 'customer', 'team' = 'crm');
ALTER TABLE gold.orders SET TAGS ('domain' = 'sales', 'team' = 'revenue');
ALTER TABLE gold.products SET TAGS ('domain' = 'product', 'team' = 'catalog');

-- Apply data quality tags
ALTER TABLE gold.customers SET TAGS (
    'quality_tier' = 'certified',
    'refresh_frequency' = 'daily',
    'sla' = '99.9%'
);

-- Discover tables by tags
SELECT table_catalog, table_schema, table_name, tag_name, tag_value
FROM system.information_schema.table_tags
WHERE tag_name = 'domain' AND tag_value = 'customer';

-- Find all certified tables
SELECT t.table_catalog, t.table_schema, t.table_name, t.comment
FROM system.information_schema.tables t
JOIN system.information_schema.table_tags tt
  ON t.table_catalog = tt.catalog_name
  AND t.table_schema = tt.schema_name
  AND t.table_name = tt.table_name
WHERE tt.tag_name = 'quality_tier' AND tt.tag_value = 'certified';`,
  },
  {
    id: 3,
    category: 'Cataloging',
    title: 'Data Discovery with Information Schema',
    desc: 'Build a data discovery layer using Unity Catalog system tables',
    code: `-- Comprehensive data catalog query
SELECT
    t.table_catalog AS catalog,
    t.table_schema AS schema,
    t.table_name AS table,
    t.table_type,
    t.comment AS description,
    t.created AS created_date,
    t.last_altered AS last_modified,
    COUNT(c.column_name) AS column_count
FROM system.information_schema.tables t
LEFT JOIN system.information_schema.columns c
    ON t.table_catalog = c.table_catalog
    AND t.table_schema = c.table_schema
    AND t.table_name = c.table_name
WHERE t.table_catalog = 'enterprise_data'
GROUP BY ALL
ORDER BY t.last_altered DESC;

-- Search for tables containing specific columns
SELECT table_catalog, table_schema, table_name, column_name, data_type
FROM system.information_schema.columns
WHERE column_name LIKE '%customer%' OR column_name LIKE '%user_id%'
ORDER BY table_catalog, table_schema, table_name;

-- Table size and row counts
SELECT
    table_catalog, table_schema, table_name,
    number_of_rows, data_size_in_bytes,
    ROUND(data_size_in_bytes / 1024 / 1024, 2) AS size_mb
FROM system.information_schema.tables
WHERE data_size_in_bytes IS NOT NULL
ORDER BY data_size_in_bytes DESC
LIMIT 20;`,
  },
  {
    id: 4,
    category: 'Cataloging',
    title: 'External Table Registration',
    desc: 'Register external data sources in Unity Catalog for unified governance',
    code: `-- Create storage credential
CREATE STORAGE CREDENTIAL IF NOT EXISTS external_s3_cred
WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789012:role/databricks-external')
COMMENT 'Credential for external S3 data';

-- Create external location
CREATE EXTERNAL LOCATION IF NOT EXISTS partner_data
URL 's3://partner-data-bucket/'
WITH (STORAGE CREDENTIAL external_s3_cred)
COMMENT 'Partner data files in S3';

-- Register external tables
CREATE TABLE IF NOT EXISTS enterprise_data.external.partner_events
USING DELTA
LOCATION 's3://partner-data-bucket/events/';

CREATE TABLE IF NOT EXISTS enterprise_data.external.vendor_catalog
USING CSV
OPTIONS (header = 'true', inferSchema = 'true')
LOCATION 's3://partner-data-bucket/vendor_catalog/';

-- Apply governance to external tables (same as managed)
ALTER TABLE enterprise_data.external.partner_events
SET TAGS ('source' = 'partner', 'classification' = 'confidential');

GRANT SELECT ON TABLE enterprise_data.external.partner_events TO \`analysts\`;`,
  },
  {
    id: 5,
    category: 'Lineage',
    title: 'Table and Column Lineage Tracking',
    desc: 'Track data lineage across transformations using Unity Catalog system tables',
    code: `-- Query table-level lineage
SELECT
    source_table_full_name AS source_table,
    target_table_full_name AS target_table,
    event_time,
    entity_type
FROM system.access.table_lineage
WHERE target_table_full_name = 'enterprise_data.gold.customer_360'
ORDER BY event_time DESC;

-- Query column-level lineage
SELECT
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name,
    event_time
FROM system.access.column_lineage
WHERE target_table_full_name = 'enterprise_data.gold.customer_360'
ORDER BY target_column_name;

-- Build full lineage graph (upstream)
WITH RECURSIVE lineage_graph AS (
    SELECT source_table_full_name, target_table_full_name, 1 AS depth
    FROM system.access.table_lineage
    WHERE target_table_full_name = 'enterprise_data.gold.customer_360'
    UNION ALL
    SELECT l.source_table_full_name, l.target_table_full_name, g.depth + 1
    FROM system.access.table_lineage l
    JOIN lineage_graph g ON l.target_table_full_name = g.source_table_full_name
    WHERE g.depth < 5
)
SELECT DISTINCT source_table_full_name, target_table_full_name, depth
FROM lineage_graph ORDER BY depth;`,
  },
  {
    id: 6,
    category: 'Lineage',
    title: 'Impact Analysis for Schema Changes',
    desc: 'Analyze downstream impact before making schema changes using lineage data',
    code: `def analyze_impact(table_name, column_name=None):
    """Analyze downstream impact of changes to a table or column."""

    # Find all downstream tables
    downstream_tables = spark.sql(f"""
        SELECT DISTINCT target_table_full_name
        FROM system.access.table_lineage
        WHERE source_table_full_name = '{table_name}'
    """).collect()

    impact_report = {"table": table_name, "downstream_tables": []}

    for row in downstream_tables:
        target = row.target_table_full_name
        # Check column lineage if specific column
        if column_name:
            col_impact = spark.sql(f"""
                SELECT target_column_name
                FROM system.access.column_lineage
                WHERE source_table_full_name = '{table_name}'
                  AND source_column_name = '{column_name}'
                  AND target_table_full_name = '{target}'
            """).collect()
            affected_cols = [r.target_column_name for r in col_impact]
        else:
            affected_cols = ["*"]

        # Check grants on downstream table
        grants = spark.sql(f"SHOW GRANTS ON TABLE {target}").collect()

        impact_report["downstream_tables"].append({
            "table": target,
            "affected_columns": affected_cols,
            "granted_to": [g.Principal for g in grants]
        })

    return impact_report

# Analyze impact before dropping a column
impact = analyze_impact("enterprise_data.silver.customers", "phone_number")
print(f"Dropping 'phone_number' affects {len(impact['downstream_tables'])} downstream tables")`,
  },
  {
    id: 7,
    category: 'Quality',
    title: 'Data Quality Rules with Expectations',
    desc: 'Define and enforce data quality rules using Delta Live Tables expectations',
    code: `import dlt
from pyspark.sql.functions import col

# Bronze layer - ingest with basic quality checks
@dlt.table(comment="Raw customer data with quality expectations")
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email_format", "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Z|a-z]{2,}$'")
@dlt.expect_or_drop("non_empty_name", "name IS NOT NULL AND LENGTH(TRIM(name)) > 0")
def bronze_customers():
    return spark.readStream.format("cloudFiles") \\
        .option("cloudFiles.format", "json") \\
        .load("/mnt/raw/customers/")

# Silver layer - enforce stricter quality
@dlt.table(comment="Cleansed customer data")
@dlt.expect_all_or_fail({
    "valid_age": "age BETWEEN 0 AND 150",
    "valid_country": "country_code IS NOT NULL AND LENGTH(country_code) = 2",
    "valid_created_date": "created_date <= current_date()"
})
def silver_customers():
    return dlt.read_stream("bronze_customers").select(
        col("customer_id"),
        col("name").alias("full_name"),
        col("email"),
        col("age").cast("int"),
        col("country_code"),
        col("created_date").cast("date")
    )

# Quality metrics dashboard query
# SELECT * FROM event_log WHERE event_type = 'flow_progress'
# AND details:flow_progress:metrics:num_output_rows IS NOT NULL`,
  },
  {
    id: 8,
    category: 'Quality',
    title: 'Custom Data Quality Framework',
    desc: 'Build a reusable data quality framework with configurable rules and metrics',
    code: `from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, lit, current_timestamp

class DataQualityChecker:
    def __init__(self, spark, metrics_table="governance.dq_metrics"):
        self.spark = spark
        self.metrics_table = metrics_table
        self.results = []

    def check_nulls(self, df, column, threshold=0.0):
        total = df.count()
        null_count = df.filter(col(column).isNull()).count()
        null_pct = null_count / total if total > 0 else 0
        passed = null_pct <= threshold
        self.results.append({
            "rule": f"null_check_{column}",
            "column": column,
            "metric": null_pct,
            "threshold": threshold,
            "passed": passed,
            "details": f"{null_count}/{total} nulls ({null_pct:.2%})"
        })
        return passed

    def check_uniqueness(self, df, column):
        total = df.count()
        distinct = df.select(column).distinct().count()
        is_unique = total == distinct
        self.results.append({
            "rule": f"uniqueness_{column}",
            "column": column,
            "metric": distinct / total if total > 0 else 0,
            "threshold": 1.0,
            "passed": is_unique,
            "details": f"{distinct}/{total} unique values"
        })
        return is_unique

    def check_range(self, df, column, min_val, max_val):
        out_of_range = df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()
        passed = out_of_range == 0
        self.results.append({
            "rule": f"range_{column}",
            "column": column,
            "metric": out_of_range,
            "threshold": 0,
            "passed": passed,
            "details": f"{out_of_range} values outside [{min_val}, {max_val}]"
        })
        return passed

    def save_results(self, table_name):
        results_df = self.spark.createDataFrame(self.results)
        results_df.withColumn("table_name", lit(table_name)) \\
            .withColumn("check_time", current_timestamp()) \\
            .write.format("delta").mode("append") \\
            .saveAsTable(self.metrics_table)

# Usage
dq = DataQualityChecker(spark)
df = spark.table("gold.customers")
dq.check_nulls(df, "customer_id", threshold=0.0)
dq.check_uniqueness(df, "email")
dq.check_range(df, "age", 0, 150)
dq.save_results("gold.customers")`,
  },
  {
    id: 9,
    category: 'Quality',
    title: 'SLA Monitoring for Data Freshness',
    desc: 'Monitor data freshness SLAs and alert on stale data',
    code: `from pyspark.sql.functions import max as spark_max, current_timestamp, datediff, col
from datetime import datetime, timedelta

# Define SLA configurations
sla_configs = [
    {"table": "gold.customer_360", "max_stale_hours": 4, "owner": "crm-team"},
    {"table": "gold.daily_revenue", "max_stale_hours": 2, "owner": "finance-team"},
    {"table": "gold.inventory_snapshot", "max_stale_hours": 1, "owner": "ops-team"},
    {"table": "silver.clickstream", "max_stale_hours": 0.5, "owner": "analytics-team"},
]

def check_data_freshness(sla_configs):
    """Check data freshness against SLA thresholds."""
    results = []
    for config in sla_configs:
        table_name = config["table"]
        try:
            history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
            last_modified = history.timestamp
            hours_since_update = (datetime.now() - last_modified).total_seconds() / 3600
            is_stale = hours_since_update > config["max_stale_hours"]
            results.append({
                "table": table_name,
                "last_updated": str(last_modified),
                "hours_stale": round(hours_since_update, 2),
                "sla_hours": config["max_stale_hours"],
                "sla_breached": is_stale,
                "owner": config["owner"]
            })
        except Exception as e:
            results.append({
                "table": table_name,
                "last_updated": None,
                "hours_stale": None,
                "sla_hours": config["max_stale_hours"],
                "sla_breached": True,
                "owner": config["owner"]
            })
    return results

freshness = check_data_freshness(sla_configs)
freshness_df = spark.createDataFrame(freshness)
freshness_df.write.format("delta").mode("overwrite").saveAsTable("governance.sla_status")`,
  },
  {
    id: 10,
    category: 'Quality',
    title: 'Anomaly Detection in Data Quality',
    desc: 'Detect statistical anomalies in data quality metrics over time',
    code: `from pyspark.sql.functions import avg, stddev, col, abs as spark_abs, when

def detect_quality_anomalies(table_name, metric_column, lookback_days=30, z_threshold=3.0):
    """Detect anomalies using z-score method on quality metrics."""

    # Get historical metrics
    history_df = spark.sql(f"""
        SELECT check_time, metric
        FROM governance.dq_metrics
        WHERE table_name = '{table_name}'
          AND rule LIKE '%{metric_column}%'
          AND check_time >= current_date() - INTERVAL {lookback_days} DAYS
        ORDER BY check_time
    """)

    # Calculate statistics
    stats = history_df.agg(
        avg("metric").alias("mean"),
        stddev("metric").alias("std")
    ).collect()[0]

    mean_val = stats["mean"]
    std_val = stats["std"] or 0.001  # Avoid division by zero

    # Flag anomalies
    anomalies_df = history_df.withColumn(
        "z_score", (col("metric") - mean_val) / std_val
    ).withColumn(
        "is_anomaly", spark_abs(col("z_score")) > z_threshold
    ).withColumn(
        "severity", when(spark_abs(col("z_score")) > 5, "CRITICAL")
                    .when(spark_abs(col("z_score")) > z_threshold, "WARNING")
                    .otherwise("NORMAL")
    )

    # Save anomaly results
    anomalies_df.filter(col("is_anomaly")).write.format("delta") \\
        .mode("append").saveAsTable("governance.quality_anomalies")

    return anomalies_df.filter(col("is_anomaly"))

# Run anomaly detection
anomalies = detect_quality_anomalies("gold.customers", "null_check")
display(anomalies)`,
  },
  {
    id: 11,
    category: 'Policy',
    title: 'Data Retention and Archival Policy',
    desc: 'Implement automated data retention policies with archival and purging',
    code: `from datetime import datetime, timedelta
from delta.tables import DeltaTable

# Retention policy configuration
retention_policies = {
    "bronze.raw_events": {"retain_days": 90, "action": "archive_then_delete"},
    "silver.processed_events": {"retain_days": 365, "action": "archive_then_delete"},
    "gold.daily_metrics": {"retain_days": 730, "action": "archive_then_delete"},
    "staging.temp_uploads": {"retain_days": 7, "action": "delete"},
    "logs.audit_trail": {"retain_days": 2555, "action": "archive"},  # 7 years
}

def enforce_retention(table_name, policy):
    """Apply retention policy to a Delta table."""
    cutoff_date = (datetime.now() - timedelta(days=policy["retain_days"])).strftime("%Y-%m-%d")
    action = policy["action"]

    if "archive" in action:
        # Archive old data before deletion
        archive_df = spark.sql(f"""
            SELECT * FROM {table_name}
            WHERE event_date < '{cutoff_date}'
        """)
        archive_path = f"abfss://archive@storage.dfs.core.windows.net/{table_name}/{cutoff_date}/"
        archive_df.write.format("parquet").mode("overwrite").save(archive_path)
        print(f"Archived {archive_df.count()} rows from {table_name}")

    if "delete" in action:
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.delete(f"event_date < '{cutoff_date}'")
        spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
        print(f"Purged data older than {cutoff_date} from {table_name}")

    # Log retention action
    spark.sql(f"""
        INSERT INTO governance.retention_log
        VALUES ('{table_name}', '{action}', '{cutoff_date}', current_timestamp())
    """)

# Execute retention policies
for table, policy in retention_policies.items():
    enforce_retention(table, policy)`,
  },
  {
    id: 12,
    category: 'Policy',
    title: 'Data Lifecycle Management',
    desc: 'Manage data lifecycle stages from ingestion to archival with automated transitions',
    code: `from pyspark.sql.functions import col, current_timestamp, datediff

class DataLifecycleManager:
    STAGES = ["active", "warm", "cold", "archived", "deleted"]

    def __init__(self, spark):
        self.spark = spark

    def get_table_lifecycle_stage(self, table_name):
        """Determine current lifecycle stage based on access patterns."""
        access_stats = self.spark.sql(f"""
            SELECT
                COUNT(*) AS access_count,
                MAX(event_time) AS last_accessed,
                DATEDIFF(current_date(), MAX(event_time)) AS days_since_access
            FROM system.access.audit
            WHERE request_params.full_name_arg = '{table_name}'
              AND action_name = 'getTable'
              AND event_date >= current_date() - INTERVAL 365 DAYS
        """).collect()[0]

        days_idle = access_stats.days_since_access or 999
        if days_idle <= 7:
            return "active"
        elif days_idle <= 30:
            return "warm"
        elif days_idle <= 180:
            return "cold"
        elif days_idle <= 365:
            return "archived"
        else:
            return "deleted"

    def transition_table(self, table_name, target_stage):
        """Move table to target lifecycle stage."""
        if target_stage == "cold":
            # Move to cheaper storage tier
            self.spark.sql(f"""
                ALTER TABLE {table_name}
                SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
                                   'storage.tier' = 'cold')
            """)
        elif target_stage == "archived":
            # Convert to read-only parquet archive
            df = self.spark.table(table_name)
            archive_path = f"abfss://archive@storage.dfs.core.windows.net/{table_name}/"
            df.write.format("parquet").mode("overwrite").save(archive_path)

        # Update lifecycle metadata
        self.spark.sql(f"""
            ALTER TABLE {table_name} SET TAGS ('lifecycle_stage' = '{target_stage}')
        """)

lcm = DataLifecycleManager(spark)
stage = lcm.get_table_lifecycle_stage("gold.legacy_reports")
print(f"Current stage: {stage}")`,
  },
  {
    id: 13,
    category: 'Policy',
    title: 'Data Sharing Policies',
    desc: 'Configure secure data sharing using Delta Sharing with governance controls',
    code: `-- Create a Delta Sharing share
CREATE SHARE IF NOT EXISTS partner_analytics
COMMENT 'Aggregated analytics data shared with partners';

-- Add tables to the share with alias
ALTER SHARE partner_analytics ADD TABLE gold.daily_metrics AS partner.daily_metrics;
ALTER SHARE partner_analytics ADD TABLE gold.product_catalog AS partner.products;

-- Add tables with partition filtering (share only specific data)
ALTER SHARE partner_analytics ADD TABLE gold.regional_sales
PARTITION (region = 'US') AS partner.us_sales;

-- Create recipient
CREATE RECIPIENT IF NOT EXISTS partner_acme
COMMENT 'Acme Corp analytics team'
USING ID 'acme-databricks-sharing-id';

-- Grant share to recipient
GRANT SELECT ON SHARE partner_analytics TO RECIPIENT partner_acme;

-- Monitor sharing activity
SELECT
    event_time,
    recipient_name,
    table_name,
    operation,
    rows_shared
FROM system.access.audit
WHERE action_name LIKE '%sharing%'
ORDER BY event_time DESC;

-- Revoke access when partnership ends
REVOKE SELECT ON SHARE partner_analytics FROM RECIPIENT partner_acme;`,
  },
  {
    id: 14,
    category: 'Metadata',
    title: 'Business Glossary and Data Dictionary',
    desc: 'Maintain a business glossary with term definitions linked to physical tables',
    code: `-- Create business glossary tables
CREATE TABLE IF NOT EXISTS governance.business_glossary (
    term_id STRING,
    term_name STRING,
    definition STRING,
    domain STRING,
    owner STRING,
    synonyms ARRAY<STRING>,
    related_terms ARRAY<STRING>,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Populate business glossary
INSERT INTO governance.business_glossary VALUES
('BG001', 'Customer', 'An individual or organization that has purchased at least one product',
    'CRM', 'crm-team', ARRAY('Client', 'Buyer'), ARRAY('BG002', 'BG003'),
    current_timestamp(), current_timestamp()),
('BG002', 'Active Customer', 'Customer with a purchase in the last 12 months',
    'CRM', 'crm-team', ARRAY('Current Customer'), ARRAY('BG001'),
    current_timestamp(), current_timestamp()),
('BG003', 'Customer Lifetime Value', 'Total predicted revenue from a customer relationship',
    'Finance', 'finance-team', ARRAY('CLV', 'LTV'), ARRAY('BG001'),
    current_timestamp(), current_timestamp());

-- Link glossary terms to physical columns
CREATE TABLE IF NOT EXISTS governance.term_mappings (
    term_id STRING,
    catalog_name STRING,
    schema_name STRING,
    table_name STRING,
    column_name STRING
);

INSERT INTO governance.term_mappings VALUES
('BG001', 'enterprise_data', 'gold', 'customer_360', 'customer_id'),
('BG002', 'enterprise_data', 'gold', 'customer_360', 'is_active'),
('BG003', 'enterprise_data', 'gold', 'customer_360', 'lifetime_value');

-- Search glossary
SELECT g.term_name, g.definition, m.table_name, m.column_name
FROM governance.business_glossary g
JOIN governance.term_mappings m ON g.term_id = m.term_id
WHERE g.domain = 'CRM';`,
  },
  {
    id: 15,
    category: 'Metadata',
    title: 'Automated Metadata Enrichment',
    desc: 'Automatically enrich table metadata with statistics, profiling, and documentation',
    code: `from pyspark.sql.functions import count, countDistinct, min, max, avg, stddev, col
import json

def profile_table(table_name):
    """Generate comprehensive metadata profile for a table."""
    df = spark.table(table_name)
    total_rows = df.count()

    column_profiles = []
    for field in df.schema.fields:
        col_name = field.name
        col_type = str(field.dataType)
        profile = {"column": col_name, "type": col_type}

        # Null analysis
        null_count = df.filter(col(col_name).isNull()).count()
        profile["null_count"] = null_count
        profile["null_pct"] = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0
        profile["distinct_count"] = df.select(col_name).distinct().count()

        # Numeric statistics
        if col_type in ["IntegerType()", "LongType()", "DoubleType()", "FloatType()"]:
            stats = df.agg(
                min(col_name).alias("min"),
                max(col_name).alias("max"),
                avg(col_name).alias("mean"),
                stddev(col_name).alias("stddev")
            ).collect()[0]
            profile.update({
                "min": str(stats["min"]), "max": str(stats["max"]),
                "mean": round(float(stats["mean"] or 0), 4),
                "stddev": round(float(stats["stddev"] or 0), 4)
            })

        column_profiles.append(profile)

    # Store metadata
    metadata = {
        "table": table_name,
        "row_count": total_rows,
        "column_count": len(df.columns),
        "columns": column_profiles,
        "profiled_at": str(datetime.now())
    }

    spark.sql(f"""
        INSERT INTO governance.table_profiles
        VALUES ('{table_name}', '{json.dumps(metadata)}', current_timestamp())
    """)
    return metadata

# Profile all gold tables
for table in spark.catalog.listTables("gold"):
    profile = profile_table(f"gold.{table.name}")
    print(f"Profiled {table.name}: {profile['row_count']} rows, {profile['column_count']} columns")`,
  },
  {
    id: 16,
    category: 'Metadata',
    title: 'Schema Evolution Tracking',
    desc: 'Track and alert on schema changes across Delta tables',
    code: `from pyspark.sql.functions import col, from_json, schema_of_json

def track_schema_changes(table_name):
    """Compare current schema with last known schema and log changes."""

    # Get current schema
    current_schema = spark.table(table_name).schema.json()

    # Get last recorded schema
    last_schema_row = spark.sql(f"""
        SELECT schema_json FROM governance.schema_history
        WHERE table_name = '{table_name}'
        ORDER BY recorded_at DESC LIMIT 1
    """).collect()

    if last_schema_row:
        import json
        last_schema = json.loads(last_schema_row[0].schema_json)
        current = json.loads(current_schema)

        last_cols = {f["name"]: f for f in last_schema["fields"]}
        curr_cols = {f["name"]: f for f in current["fields"]}

        changes = []
        # Detect added columns
        for name in set(curr_cols) - set(last_cols):
            changes.append({"change": "ADDED", "column": name, "type": curr_cols[name]["type"]})
        # Detect removed columns
        for name in set(last_cols) - set(curr_cols):
            changes.append({"change": "REMOVED", "column": name, "type": last_cols[name]["type"]})
        # Detect type changes
        for name in set(curr_cols) & set(last_cols):
            if curr_cols[name]["type"] != last_cols[name]["type"]:
                changes.append({
                    "change": "TYPE_CHANGED", "column": name,
                    "old_type": str(last_cols[name]["type"]),
                    "new_type": str(curr_cols[name]["type"])
                })

        if changes:
            changes_json = json.dumps(changes)
            spark.sql(f"""
                INSERT INTO governance.schema_changes
                VALUES ('{table_name}', '{changes_json}', current_timestamp())
            """)
            return changes
    # Record current schema
    spark.sql(f"""
        INSERT INTO governance.schema_history
        VALUES ('{table_name}', '{current_schema}', current_timestamp())
    """)
    return []

# Monitor schema changes across all gold tables
for table in spark.catalog.listTables("gold"):
    changes = track_schema_changes(f"gold.{table.name}")
    if changes:
        print(f"Schema changes detected in gold.{table.name}: {changes}")`,
  },
  {
    id: 17,
    category: 'Lineage',
    title: 'Cross-Workspace Lineage',
    desc: 'Track data lineage across multiple Databricks workspaces',
    code: `# Cross-workspace lineage tracker
class CrossWorkspaceLineage:
    def __init__(self, spark, lineage_table="governance.cross_workspace_lineage"):
        self.spark = spark
        self.lineage_table = lineage_table

    def register_pipeline(self, pipeline_id, source_workspace, source_table,
                          target_workspace, target_table, transform_description):
        """Register a cross-workspace data pipeline for lineage tracking."""
        self.spark.sql(f"""
            INSERT INTO {self.lineage_table} VALUES (
                '{pipeline_id}',
                '{source_workspace}', '{source_table}',
                '{target_workspace}', '{target_table}',
                '{transform_description}',
                current_timestamp(),
                'ACTIVE'
            )
        """)

    def get_upstream(self, workspace, table_name, max_depth=5):
        """Find all upstream sources for a table."""
        upstream = []
        to_visit = [(workspace, table_name, 0)]
        visited = set()
        while to_visit:
            ws, tbl, depth = to_visit.pop(0)
            if (ws, tbl) in visited or depth > max_depth:
                continue
            visited.add((ws, tbl))
            sources = self.spark.sql(f"""
                SELECT source_workspace, source_table
                FROM {self.lineage_table}
                WHERE target_workspace = '{ws}' AND target_table = '{tbl}'
                  AND status = 'ACTIVE'
            """).collect()
            for row in sources:
                upstream.append({
                    "workspace": row.source_workspace,
                    "table": row.source_table,
                    "depth": depth + 1
                })
                to_visit.append((row.source_workspace, row.source_table, depth + 1))
        return upstream

lineage = CrossWorkspaceLineage(spark)
lineage.register_pipeline(
    "PIPE-001", "workspace-etl", "bronze.raw_events",
    "workspace-analytics", "gold.event_metrics",
    "Aggregate hourly event metrics"
)
print(lineage.get_upstream("workspace-analytics", "gold.event_metrics"))`,
  },
  {
    id: 18,
    category: 'Policy',
    title: 'Data Access Request Workflow',
    desc: 'Implement self-service data access request and approval workflow',
    code: `from pyspark.sql.functions import current_timestamp, lit

class DataAccessManager:
    def __init__(self, spark):
        self.spark = spark

    def request_access(self, requester, table_name, access_level, justification):
        """Submit a data access request."""
        request_id = f"DAR-{hash(requester + table_name) % 100000:05d}"
        self.spark.sql(f"""
            INSERT INTO governance.access_requests VALUES (
                '{request_id}', '{requester}', '{table_name}',
                '{access_level}', '{justification}',
                'PENDING', NULL, NULL, current_timestamp()
            )
        """)
        return request_id

    def approve_request(self, request_id, approver):
        """Approve and execute an access request."""
        request = self.spark.sql(f"""
            SELECT * FROM governance.access_requests
            WHERE request_id = '{request_id}' AND status = 'PENDING'
        """).collect()

        if not request:
            raise ValueError(f"Request {request_id} not found or not pending")

        req = request[0]
        # Grant access
        self.spark.sql(
            f"GRANT {req.access_level} ON TABLE {req.table_name} TO \`{req.requester}\`"
        )
        # Update request status
        self.spark.sql(f"""
            UPDATE governance.access_requests
            SET status = 'APPROVED', approved_by = '{approver}',
                approved_at = current_timestamp()
            WHERE request_id = '{request_id}'
        """)

    def revoke_expired_access(self, expiry_days=90):
        """Revoke access grants that have exceeded their expiry period."""
        expired = self.spark.sql(f"""
            SELECT * FROM governance.access_requests
            WHERE status = 'APPROVED'
              AND DATEDIFF(current_date(), approved_at) > {expiry_days}
        """).collect()
        for req in expired:
            self.spark.sql(
                f"REVOKE {req.access_level} ON TABLE {req.table_name} FROM \`{req.requester}\`"
            )

dam = DataAccessManager(spark)
rid = dam.request_access("analyst@company.com", "gold.revenue", "SELECT", "Q4 reporting")
print(f"Access request submitted: {rid}")`,
  },
  {
    id: 19,
    category: 'Lineage',
    title: 'Data Pipeline Dependency Graph',
    desc: 'Build and visualize pipeline dependency graphs for orchestration planning',
    code: `from collections import defaultdict

class PipelineDependencyGraph:
    def __init__(self):
        self.graph = defaultdict(set)
        self.reverse_graph = defaultdict(set)
        self.metadata = {}

    def add_pipeline(self, pipeline_id, inputs, outputs, schedule=None):
        """Register a pipeline with its input and output tables."""
        self.metadata[pipeline_id] = {
            "inputs": inputs, "outputs": outputs, "schedule": schedule
        }
        for inp in inputs:
            for out in outputs:
                self.graph[inp].add(out)
                self.reverse_graph[out].add(inp)

    def get_execution_order(self, target_table):
        """Determine correct execution order to produce target table."""
        order = []
        visited = set()

        def dfs(table):
            if table in visited:
                return
            visited.add(table)
            for dep in self.reverse_graph.get(table, []):
                dfs(dep)
            order.append(table)

        dfs(target_table)
        return order

    def detect_cycles(self):
        """Detect circular dependencies in the pipeline graph."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = defaultdict(int)
        cycles = []

        def dfs(node, path):
            color[node] = GRAY
            path.append(node)
            for neighbor in self.graph.get(node, []):
                if color[neighbor] == GRAY:
                    cycle_start = path.index(neighbor)
                    cycles.append(path[cycle_start:] + [neighbor])
                elif color[neighbor] == WHITE:
                    dfs(neighbor, path)
            path.pop()
            color[node] = BLACK

        for node in set(list(self.graph.keys()) + list(self.reverse_graph.keys())):
            if color[node] == WHITE:
                dfs(node, [])
        return cycles

# Build dependency graph
dag = PipelineDependencyGraph()
dag.add_pipeline("ingest_events", [], ["bronze.raw_events"], "*/5 * * * *")
dag.add_pipeline("clean_events", ["bronze.raw_events"], ["silver.events"], "*/10 * * * *")
dag.add_pipeline("aggregate", ["silver.events", "silver.users"], ["gold.metrics"], "0 * * * *")

order = dag.get_execution_order("gold.metrics")
print(f"Execution order: {order}")
print(f"Cycles detected: {dag.detect_cycles()}")`,
  },
  {
    id: 20,
    category: 'Metadata',
    title: 'Data Contract Enforcement',
    desc: 'Define and enforce data contracts between producer and consumer teams',
    code: `import json
from pyspark.sql.types import StructType

class DataContract:
    def __init__(self, spark, contract_table="governance.data_contracts"):
        self.spark = spark
        self.contract_table = contract_table

    def register_contract(self, contract_id, table_name, producer, consumers,
                          required_columns, sla_freshness_hours, quality_rules):
        """Register a data contract between producer and consumer teams."""
        contract = {
            "contract_id": contract_id,
            "table_name": table_name,
            "producer": producer,
            "consumers": consumers,
            "required_columns": required_columns,
            "sla_freshness_hours": sla_freshness_hours,
            "quality_rules": quality_rules
        }
        self.spark.sql(f"""
            INSERT INTO {self.contract_table}
            VALUES ('{contract_id}', '{table_name}', '{producer}',
                    '{json.dumps(contract)}', 'ACTIVE', current_timestamp())
        """)

    def validate_contract(self, contract_id):
        """Validate that a data contract is being met."""
        contract_row = self.spark.sql(f"""
            SELECT contract_json FROM {self.contract_table}
            WHERE contract_id = '{contract_id}' AND status = 'ACTIVE'
        """).collect()[0]

        contract = json.loads(contract_row.contract_json)
        table_name = contract["table_name"]
        violations = []

        # Check required columns exist
        actual_cols = set(self.spark.table(table_name).columns)
        for req_col in contract["required_columns"]:
            if req_col not in actual_cols:
                violations.append(f"Missing required column: {req_col}")

        # Check freshness SLA
        from datetime import datetime
        history = self.spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
        hours_stale = (datetime.now() - history.timestamp).total_seconds() / 3600
        if hours_stale > contract["sla_freshness_hours"]:
            violations.append(
                f"Freshness SLA breached: {hours_stale:.1f}h > {contract['sla_freshness_hours']}h"
            )

        return {"contract_id": contract_id, "valid": len(violations) == 0, "violations": violations}

dc = DataContract(spark)
dc.register_contract(
    "DC-001", "gold.customer_360", "data-eng-team", ["analytics", "ml-team"],
    ["customer_id", "email", "lifetime_value", "segment"], 4,
    {"null_threshold": 0.01, "min_rows": 10000}
)
result = dc.validate_contract("DC-001")
print(f"Contract valid: {result['valid']}, Violations: {result['violations']}")`,
  },
];

const deploymentScenarios = [
  {
    id: 1,
    category: 'Databricks Asset Bundles',
    title: 'DABs Project Configuration',
    desc: 'Set up a Databricks Asset Bundle project for structured deployments',
    code: `# databricks.yml - Root configuration for Databricks Asset Bundles
bundle:
  name: analytics-pipeline

include:
  - resources/*.yml
  - resources/**/*.yml

workspace:
  host: https://adb-1234567890.1.azuredatabricks.net

targets:
  dev:
    mode: development
    default: true
    workspace:
      root_path: /Users/\${workspace.current_user.userName}/.bundle/\${bundle.name}/dev

  staging:
    mode: development
    workspace:
      host: https://adb-staging.azuredatabricks.net
      root_path: /Shared/.bundle/\${bundle.name}/staging

  production:
    mode: production
    workspace:
      host: https://adb-production.azuredatabricks.net
      root_path: /Shared/.bundle/\${bundle.name}/prod
    run_as:
      service_principal_name: "sp-production-deployer"

# Deploy commands:
# databricks bundle validate -t dev
# databricks bundle deploy -t dev
# databricks bundle deploy -t production`,
  },
  {
    id: 2,
    category: 'Databricks Asset Bundles',
    title: 'DABs Job and Pipeline Resources',
    desc: 'Define jobs, pipelines, and clusters as bundle resources',
    code: `# resources/jobs.yml - Job definitions
resources:
  jobs:
    daily_etl_pipeline:
      name: "Daily ETL Pipeline"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "America/New_York"
      email_notifications:
        on_failure:
          - platform-alerts@company.com
      tasks:
        - task_key: ingest_raw_data
          notebook_task:
            notebook_path: ../src/01_ingest.py
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 2
            spark_conf:
              spark.sql.shuffle.partitions: "200"

        - task_key: transform_silver
          depends_on:
            - task_key: ingest_raw_data
          notebook_task:
            notebook_path: ../src/02_transform.py
            base_parameters:
              target_schema: "silver"

        - task_key: aggregate_gold
          depends_on:
            - task_key: transform_silver
          notebook_task:
            notebook_path: ../src/03_aggregate.py

    # Delta Live Tables pipeline
    dlt_customer_pipeline:
      name: "Customer DLT Pipeline"
      tasks:
        - task_key: run_dlt
          pipeline_task:
            pipeline_id: \${resources.pipelines.customer_pipeline.id}

  pipelines:
    customer_pipeline:
      name: "Customer Data Pipeline"
      target: "gold"
      libraries:
        - notebook:
            path: ../src/dlt/customer_pipeline.py`,
  },
  {
    id: 3,
    category: 'CI/CD',
    title: 'GitHub Actions for Databricks',
    desc: 'Configure GitHub Actions workflow for automated testing and deployment',
    code: `# .github/workflows/databricks-deploy.yml
name: Databricks CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  DATABRICKS_HOST: \${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_TOKEN: \${{ secrets.DATABRICKS_TOKEN }}

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

      - name: Validate Bundle
        run: databricks bundle validate -t dev

      - name: Run Unit Tests
        run: |
          pip install pytest pyspark delta-spark
          pytest tests/ -v --junitxml=test-results.xml

      - name: Lint PySpark Code
        run: |
          pip install ruff
          ruff check src/ --select E,F,I,N,W

  deploy-staging:
    needs: validate
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4
      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      - name: Deploy to Staging
        run: databricks bundle deploy -t staging
      - name: Run Integration Tests
        run: databricks bundle run integration_test_job -t staging

  deploy-production:
    needs: deploy-staging
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      - name: Deploy to Production
        run: databricks bundle deploy -t production`,
  },
  {
    id: 4,
    category: 'CI/CD',
    title: 'Terraform Databricks Provider',
    desc: 'Manage Databricks infrastructure with Terraform for reproducible environments',
    code: `# main.tf - Databricks workspace and resources via Terraform
terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "databricks" {
  host  = azurerm_databricks_workspace.this.workspace_url
  token = var.databricks_token
}

# Create Unity Catalog
resource "databricks_catalog" "analytics" {
  name    = "analytics"
  comment = "Analytics data catalog"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.analytics.name
  name         = "gold"
  comment      = "Business-level aggregated data"
}

# Create cluster policy
resource "databricks_cluster_policy" "data_engineering" {
  name = "Data Engineering Policy"
  definition = jsonencode({
    "spark_version" : { "type" : "fixed", "value" : "14.3.x-scala2.12" },
    "autotermination_minutes" : { "type" : "range", "maxValue" : 120 },
    "num_workers" : { "type" : "range", "minValue" : 1, "maxValue" : 10 }
  })
}

# Create job cluster
resource "databricks_job" "etl_pipeline" {
  name = "Daily ETL Pipeline"
  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "America/New_York"
  }
  task {
    task_key = "ingest"
    notebook_task {
      notebook_path = "/Repos/production/etl/01_ingest"
    }
    new_cluster {
      spark_version = "14.3.x-scala2.12"
      node_type_id  = "Standard_DS3_v2"
      num_workers   = 2
    }
  }
}

# Create SQL warehouse
resource "databricks_sql_endpoint" "analytics" {
  name             = "Analytics Warehouse"
  cluster_size     = "Small"
  max_num_clusters = 3
  auto_stop_mins   = 15
}`,
  },
  {
    id: 5,
    category: 'CI/CD',
    title: 'dbx Deployment Tool',
    desc: 'Use dbx for simplified Databricks job deployments and lifecycle management',
    code: `# .dbx/project.json - dbx project configuration
{
    "environments": {
        "default": {
            "profile": "DEFAULT",
            "workspace_dir": "/Shared/dbx/projects/analytics-pipeline",
            "artifact_location": "dbfs:/dbx/projects/analytics-pipeline/artifacts"
        }
    }
}

# conf/deployment.yml - Job deployment configuration
environments:
  default:
    workflows:
      - name: "analytics-etl-daily"
        tasks:
          - task_key: "ingest"
            spark_python_task:
              python_file: "file://src/ingest.py"
              parameters: ["--env", "production"]
            new_cluster:
              spark_version: "14.3.x-scala2.12"
              num_workers: 4
              node_type_id: "Standard_DS4_v2"
              spark_conf:
                spark.sql.adaptive.enabled: "true"
                spark.sql.shuffle.partitions: "200"

          - task_key: "transform"
            depends_on:
              - task_key: "ingest"
            spark_python_task:
              python_file: "file://src/transform.py"

# CLI commands:
# dbx deploy analytics-etl-daily --environment=default
# dbx launch analytics-etl-daily --environment=default --trace
# dbx destroy analytics-etl-daily`,
  },
  {
    id: 6,
    category: 'Automation',
    title: 'Notebook Deployment via REST API',
    desc: 'Automate notebook deployment using the Databricks REST API',
    code: `import requests
import base64
import os

class DatabricksDeployer:
    def __init__(self, host, token):
        self.host = host.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}

    def deploy_notebook(self, local_path, remote_path, overwrite=True):
        """Deploy a notebook to Databricks workspace."""
        with open(local_path, "r") as f:
            content = base64.b64encode(f.read().encode()).decode()

        language_map = {".py": "PYTHON", ".sql": "SQL", ".scala": "SCALA", ".r": "R"}
        ext = os.path.splitext(local_path)[1].lower()
        language = language_map.get(ext, "PYTHON")

        response = requests.post(
            f"{self.host}/api/2.0/workspace/import",
            headers=self.headers,
            json={
                "path": remote_path,
                "content": content,
                "language": language,
                "overwrite": overwrite,
                "format": "SOURCE"
            },
            timeout=30
        )
        response.raise_for_status()
        return {"status": "deployed", "path": remote_path}

    def deploy_directory(self, local_dir, remote_dir):
        """Recursively deploy all notebooks in a directory."""
        results = []
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                if file.endswith((".py", ".sql", ".scala")):
                    local_path = os.path.join(root, file)
                    relative = os.path.relpath(local_path, local_dir)
                    remote_path = f"{remote_dir}/{os.path.splitext(relative)[0]}"
                    result = self.deploy_notebook(local_path, remote_path)
                    results.append(result)
        return results

    def create_or_update_job(self, job_config):
        """Create or update a Databricks job."""
        # Check if job exists
        existing = requests.get(
            f"{self.host}/api/2.1/jobs/list",
            headers=self.headers,
            params={"name": job_config["name"]},
            timeout=30
        ).json()

        if existing.get("jobs"):
            job_id = existing["jobs"][0]["job_id"]
            job_config["job_id"] = job_id
            requests.post(
                f"{self.host}/api/2.1/jobs/reset",
                headers=self.headers,
                json={"job_id": job_id, "new_settings": job_config},
                timeout=30
            )
            return {"action": "updated", "job_id": job_id}
        else:
            resp = requests.post(
                f"{self.host}/api/2.1/jobs/create",
                headers=self.headers,
                json=job_config,
                timeout=30
            ).json()
            return {"action": "created", "job_id": resp["job_id"]}

deployer = DatabricksDeployer(
    host="https://adb-1234567890.1.azuredatabricks.net",
    token=dbutils.secrets.get("deploy-scope", "deploy-token")
)
deployer.deploy_directory("./src/notebooks", "/Repos/production/etl")`,
  },
  {
    id: 7,
    category: 'Automation',
    title: 'Job Scheduling with Dependencies',
    desc: 'Create complex job schedules with task dependencies and conditional execution',
    code: `# Complex multi-task job with dependencies and conditions
job_config = {
    "name": "Daily Analytics Pipeline",
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "UTC"
    },
    "max_concurrent_runs": 1,
    "timeout_seconds": 7200,
    "email_notifications": {
        "on_start": ["team@company.com"],
        "on_failure": ["oncall@company.com"],
        "on_success": ["team@company.com"]
    },
    "tasks": [
        {
            "task_key": "check_source_data",
            "notebook_task": {"notebook_path": "/pipelines/00_validate_sources"},
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "num_workers": 1,
                "node_type_id": "Standard_DS3_v2"
            },
            "timeout_seconds": 600
        },
        {
            "task_key": "ingest_batch",
            "depends_on": [{"task_key": "check_source_data"}],
            "notebook_task": {
                "notebook_path": "/pipelines/01_ingest",
                "base_parameters": {"mode": "incremental"}
            },
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "num_workers": 4,
                "node_type_id": "Standard_DS4_v2"
            }
        },
        {
            "task_key": "transform_silver",
            "depends_on": [{"task_key": "ingest_batch"}],
            "notebook_task": {"notebook_path": "/pipelines/02_transform"},
            "existing_cluster_id": "1234-567890-abcdef"
        },
        {
            "task_key": "build_gold",
            "depends_on": [{"task_key": "transform_silver"}],
            "notebook_task": {"notebook_path": "/pipelines/03_gold_aggregates"},
            "existing_cluster_id": "1234-567890-abcdef"
        },
        {
            "task_key": "run_quality_checks",
            "depends_on": [{"task_key": "build_gold"}],
            "notebook_task": {"notebook_path": "/pipelines/04_quality_checks"},
            "existing_cluster_id": "1234-567890-abcdef"
        },
        {
            "task_key": "notify_downstream",
            "depends_on": [{"task_key": "run_quality_checks"}],
            "condition_task": {"op": "EQUAL_TO", "left": "{{tasks.run_quality_checks.result_state}}", "right": "SUCCESS"},
            "notebook_task": {"notebook_path": "/pipelines/05_notify"}
        }
    ]
}

import requests
response = requests.post(
    f"{host}/api/2.1/jobs/create",
    headers=headers,
    json=job_config,
    timeout=30
)
print(f"Job created: {response.json()['job_id']}")`,
  },
  {
    id: 8,
    category: 'Automation',
    title: 'Blue-Green Deployment Strategy',
    desc: 'Implement blue-green deployment pattern for zero-downtime table updates',
    code: `from delta.tables import DeltaTable
from datetime import datetime

class BlueGreenDeployer:
    def __init__(self, spark, catalog="production"):
        self.spark = spark
        self.catalog = catalog

    def deploy_table(self, schema, table_name, new_data_df, validation_func=None):
        """Blue-green deployment for Delta tables."""
        blue_table = f"{self.catalog}.{schema}.{table_name}"
        green_table = f"{self.catalog}.{schema}.{table_name}_green"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Step 1: Write new data to green table
        new_data_df.write.format("delta") \\
            .mode("overwrite") \\
            .option("overwriteSchema", "true") \\
            .saveAsTable(green_table)

        green_count = self.spark.table(green_table).count()
        print(f"Green table populated: {green_count} rows")

        # Step 2: Validate green table
        if validation_func:
            is_valid = validation_func(self.spark.table(green_table))
            if not is_valid:
                self.spark.sql(f"DROP TABLE IF EXISTS {green_table}")
                raise ValueError("Green table validation failed. Deployment aborted.")

        # Step 3: Backup current blue table
        backup_table = f"{self.catalog}.{schema}.{table_name}_backup_{timestamp}"
        if self.spark.catalog.tableExists(blue_table):
            self.spark.sql(f"CREATE TABLE {backup_table} AS SELECT * FROM {blue_table}")
            print(f"Backup created: {backup_table}")

        # Step 4: Swap blue and green (atomic rename)
        self.spark.sql(f"DROP TABLE IF EXISTS {blue_table}")
        self.spark.sql(f"ALTER TABLE {green_table} RENAME TO {blue_table}")
        print(f"Deployment complete: {blue_table} updated")

        return {"status": "success", "rows": green_count, "backup": backup_table}

    def rollback(self, schema, table_name, backup_timestamp):
        """Rollback to a previous backup."""
        blue_table = f"{self.catalog}.{schema}.{table_name}"
        backup_table = f"{self.catalog}.{schema}.{table_name}_backup_{backup_timestamp}"
        self.spark.sql(f"DROP TABLE IF EXISTS {blue_table}")
        self.spark.sql(f"ALTER TABLE {backup_table} RENAME TO {blue_table}")
        print(f"Rollback complete: restored from {backup_table}")

# Usage
deployer = BlueGreenDeployer(spark)
new_df = spark.table("staging.customer_360_v2")
result = deployer.deploy_table(
    "gold", "customer_360", new_df,
    validation_func=lambda df: df.count() > 1000 and "customer_id" in df.columns
)`,
  },
  {
    id: 9,
    category: 'Automation',
    title: 'Canary Release for ML Models',
    desc: 'Implement canary deployment pattern for ML model rollouts with traffic splitting',
    code: `import mlflow
from pyspark.sql.functions import col, rand, when

class CanaryModelDeployer:
    def __init__(self, spark, tracking_table="ml.model_deployments"):
        self.spark = spark
        self.tracking_table = tracking_table

    def deploy_canary(self, model_name, new_version, canary_pct=10):
        """Deploy new model version as canary with traffic splitting."""
        # Get current production model
        client = mlflow.tracking.MlflowClient()
        prod_versions = client.get_latest_versions(model_name, stages=["Production"])
        prod_version = prod_versions[0].version if prod_versions else None

        # Register deployment
        self.spark.sql(f"""
            INSERT INTO {self.tracking_table} VALUES (
                '{model_name}', {new_version}, {prod_version or 'NULL'},
                {canary_pct}, 'CANARY', current_timestamp(), NULL
            )
        """)
        return {"canary_version": new_version, "prod_version": prod_version,
                "canary_pct": canary_pct}

    def score_with_canary(self, df, model_name, new_version, prod_version, canary_pct):
        """Score data using both production and canary models with traffic split."""
        # Assign traffic
        df_with_traffic = df.withColumn(
            "model_group",
            when(rand() * 100 < canary_pct, "canary").otherwise("production")
        )

        # Score with production model
        prod_model = mlflow.pyfunc.spark_udf(self.spark,
            f"models:/{model_name}/{prod_version}")
        prod_scored = df_with_traffic.filter(col("model_group") == "production") \\
            .withColumn("prediction", prod_model("features")) \\
            .withColumn("model_version", lit(prod_version))

        # Score with canary model
        canary_model = mlflow.pyfunc.spark_udf(self.spark,
            f"models:/{model_name}/{new_version}")
        canary_scored = df_with_traffic.filter(col("model_group") == "canary") \\
            .withColumn("prediction", canary_model("features")) \\
            .withColumn("model_version", lit(new_version))

        return prod_scored.union(canary_scored)

    def promote_canary(self, model_name, new_version):
        """Promote canary to production after validation."""
        client = mlflow.tracking.MlflowClient()
        client.transition_model_version_stage(model_name, new_version, "Production")
        self.spark.sql(f"""
            UPDATE {self.tracking_table}
            SET status = 'PROMOTED', promoted_at = current_timestamp()
            WHERE model_name = '{model_name}' AND canary_version = {new_version}
        """)

canary = CanaryModelDeployer(spark)
deployment = canary.deploy_canary("customer_churn_model", new_version=5, canary_pct=10)
print(f"Canary deployed: {deployment}")`,
  },
  {
    id: 10,
    category: 'Monitoring',
    title: 'Pipeline Monitoring Dashboard',
    desc: 'Build a comprehensive monitoring dashboard for pipeline health and performance',
    code: `from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min, window

def build_monitoring_dashboard(lookback_days=7):
    """Generate pipeline monitoring metrics for dashboard."""

    # Job run history and success rates
    job_metrics = spark.sql(f"""
        SELECT
            job_name,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
            ROUND(AVG(CASE WHEN status = 'SUCCESS'
                THEN duration_seconds ELSE NULL END), 0) AS avg_duration_secs,
            ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 /
                COUNT(*), 2) AS success_rate
        FROM monitoring.job_runs
        WHERE start_time >= current_date() - INTERVAL {lookback_days} DAYS
        GROUP BY job_name
        ORDER BY success_rate ASC
    """)

    # Data freshness overview
    freshness_metrics = spark.sql("""
        SELECT
            table_name,
            ROUND(hours_stale, 2) AS hours_since_update,
            sla_hours,
            CASE WHEN hours_stale > sla_hours THEN 'BREACHED' ELSE 'OK' END AS sla_status,
            owner
        FROM governance.sla_status
        ORDER BY hours_stale DESC
    """)

    # Cluster utilization
    cluster_metrics = spark.sql(f"""
        SELECT
            cluster_name,
            ROUND(AVG(cpu_utilization), 2) AS avg_cpu_pct,
            ROUND(AVG(memory_utilization), 2) AS avg_memory_pct,
            ROUND(SUM(dbu_usage), 2) AS total_dbus,
            ROUND(SUM(cost_usd), 2) AS total_cost_usd
        FROM monitoring.cluster_usage
        WHERE usage_date >= current_date() - INTERVAL {lookback_days} DAYS
        GROUP BY cluster_name
        ORDER BY total_cost_usd DESC
    """)

    # Data quality trend
    quality_trend = spark.sql(f"""
        SELECT
            DATE(check_time) AS check_date,
            table_name,
            ROUND(AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END) * 100, 2) AS pass_rate
        FROM governance.dq_metrics
        WHERE check_time >= current_date() - INTERVAL {lookback_days} DAYS
        GROUP BY DATE(check_time), table_name
        ORDER BY check_date DESC
    """)

    return {
        "job_metrics": job_metrics,
        "freshness_metrics": freshness_metrics,
        "cluster_metrics": cluster_metrics,
        "quality_trend": quality_trend
    }

dashboard = build_monitoring_dashboard(lookback_days=30)
display(dashboard["job_metrics"])`,
  },
  {
    id: 11,
    category: 'Monitoring',
    title: 'Alerting and Notification System',
    desc: 'Configure automated alerts for pipeline failures, SLA breaches, and anomalies',
    code: `import requests
from datetime import datetime

class AlertManager:
    def __init__(self, spark, webhook_url=None, email_config=None):
        self.spark = spark
        self.webhook_url = webhook_url
        self.email_config = email_config

    def check_and_alert(self):
        """Run all alert checks and send notifications."""
        alerts = []

        # Check for failed jobs in last hour
        failed_jobs = self.spark.sql("""
            SELECT job_name, error_message, start_time
            FROM monitoring.job_runs
            WHERE status = 'FAILED'
              AND start_time >= current_timestamp() - INTERVAL 1 HOUR
        """).collect()
        for job in failed_jobs:
            alerts.append({
                "severity": "CRITICAL",
                "type": "JOB_FAILURE",
                "message": f"Job '{job.job_name}' failed: {job.error_message}",
                "timestamp": str(job.start_time)
            })

        # Check SLA breaches
        sla_breaches = self.spark.sql("""
            SELECT table_name, hours_stale, sla_hours, owner
            FROM governance.sla_status
            WHERE hours_stale > sla_hours
        """).collect()
        for breach in sla_breaches:
            alerts.append({
                "severity": "WARNING",
                "type": "SLA_BREACH",
                "message": f"Table '{breach.table_name}' stale: "
                           f"{breach.hours_stale:.1f}h > {breach.sla_hours}h SLA",
                "owner": breach.owner
            })

        # Check data quality failures
        dq_failures = self.spark.sql("""
            SELECT table_name, rule, details
            FROM governance.dq_metrics
            WHERE NOT passed
              AND check_time >= current_timestamp() - INTERVAL 1 HOUR
        """).collect()
        for failure in dq_failures:
            alerts.append({
                "severity": "WARNING",
                "type": "QUALITY_FAILURE",
                "message": f"Quality check failed on '{failure.table_name}': "
                           f"{failure.rule} - {failure.details}"
            })

        # Send alerts
        for alert in alerts:
            self._send_slack(alert)
            self._log_alert(alert)
        return alerts

    def _send_slack(self, alert):
        """Send alert to Slack webhook."""
        if not self.webhook_url:
            return
        emoji = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🔵"}
        payload = {
            "text": f"{emoji.get(alert['severity'], '⚪')} "
                    f"*[{alert['severity']}]* {alert['type']}\\n{alert['message']}"
        }
        requests.post(self.webhook_url, json=payload, timeout=10)

    def _log_alert(self, alert):
        self.spark.sql(f"""
            INSERT INTO monitoring.alert_history
            VALUES ('{alert["severity"]}', '{alert["type"]}',
                    '{alert["message"]}', current_timestamp())
        """)

alerter = AlertManager(spark, webhook_url=dbutils.secrets.get("alerts", "slack-webhook"))
triggered = alerter.check_and_alert()
print(f"Triggered {len(triggered)} alerts")`,
  },
  {
    id: 12,
    category: 'Monitoring',
    title: 'Cost Monitoring and Optimization',
    desc: 'Track and optimize Databricks cluster costs with usage analytics',
    code: `from pyspark.sql.functions import col, sum as spark_sum, round as spark_round, window

def cost_analysis(lookback_days=30):
    """Analyze Databricks usage costs and identify optimization opportunities."""

    # Cost breakdown by workspace and team
    cost_by_team = spark.sql(f"""
        SELECT
            workspace_name,
            team_tag,
            cluster_type,
            ROUND(SUM(dbu_count), 2) AS total_dbus,
            ROUND(SUM(cost_usd), 2) AS total_cost,
            ROUND(AVG(utilization_pct), 2) AS avg_utilization
        FROM monitoring.usage_costs
        WHERE usage_date >= current_date() - INTERVAL {lookback_days} DAYS
        GROUP BY workspace_name, team_tag, cluster_type
        ORDER BY total_cost DESC
    """)

    # Identify idle clusters (low utilization)
    idle_clusters = spark.sql(f"""
        SELECT
            cluster_id, cluster_name,
            ROUND(AVG(cpu_utilization), 2) AS avg_cpu,
            ROUND(AVG(memory_utilization), 2) AS avg_memory,
            ROUND(SUM(cost_usd), 2) AS wasted_cost,
            SUM(uptime_hours) AS total_hours
        FROM monitoring.cluster_usage
        WHERE usage_date >= current_date() - INTERVAL {lookback_days} DAYS
        GROUP BY cluster_id, cluster_name
        HAVING AVG(cpu_utilization) < 20 AND SUM(uptime_hours) > 24
        ORDER BY wasted_cost DESC
    """)

    # Cost trend (daily)
    daily_trend = spark.sql(f"""
        SELECT
            usage_date,
            ROUND(SUM(cost_usd), 2) AS daily_cost,
            ROUND(SUM(dbu_count), 2) AS daily_dbus
        FROM monitoring.usage_costs
        WHERE usage_date >= current_date() - INTERVAL {lookback_days} DAYS
        GROUP BY usage_date
        ORDER BY usage_date
    """)

    # Optimization recommendations
    recommendations = []
    for row in idle_clusters.collect():
        if row.avg_cpu < 10:
            recommendations.append(
                f"Terminate idle cluster '{row.cluster_name}' "
                f"(avg CPU: {row.avg_cpu}%, wasted: $" + str(row.wasted_cost) + ")"
            )
        elif row.avg_cpu < 20:
            recommendations.append(
                f"Downsize cluster '{row.cluster_name}' "
                f"(avg CPU: {row.avg_cpu}%, cost: $" + str(row.wasted_cost) + ")"
            )

    return {
        "cost_by_team": cost_by_team,
        "idle_clusters": idle_clusters,
        "daily_trend": daily_trend,
        "recommendations": recommendations
    }

report = cost_analysis(30)
display(report["cost_by_team"])
for rec in report["recommendations"]:
    print(f"  - {rec}")`,
  },
  {
    id: 13,
    category: 'CI/CD',
    title: 'Automated Testing Framework',
    desc: 'Build a PySpark testing framework for validating data pipelines before deployment',
    code: `import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import date

class TestETLPipeline:
    """Integration tests for ETL pipeline."""

    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder \\
            .master("local[*]") \\
            .appName("ETL-Tests") \\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
            .getOrCreate()

    def create_test_data(self, schema, data):
        return self.spark.createDataFrame(data, schema)

    def test_customer_dedup(self):
        """Test that customer deduplication keeps latest record."""
        from src.transforms import deduplicate_customers

        test_data = [
            ("C001", "Alice", "alice@old.com", "2024-01-01"),
            ("C001", "Alice", "alice@new.com", "2024-06-01"),
            ("C002", "Bob", "bob@test.com", "2024-03-01"),
        ]
        schema = "customer_id STRING, name STRING, email STRING, updated_at STRING"
        input_df = self.create_test_data(schema, test_data)

        result = deduplicate_customers(input_df)

        assert result.count() == 2
        alice = result.filter("customer_id = 'C001'").collect()[0]
        assert alice.email == "alice@new.com"

    def test_revenue_aggregation(self):
        """Test daily revenue aggregation logic."""
        from src.transforms import aggregate_daily_revenue

        test_data = [
            ("2024-01-01", "P001", 100.0, 2),
            ("2024-01-01", "P002", 50.0, 3),
            ("2024-01-02", "P001", 200.0, 1),
        ]
        schema = "order_date STRING, product_id STRING, amount DOUBLE, quantity INT"
        input_df = self.create_test_data(schema, test_data)

        result = aggregate_daily_revenue(input_df)

        day1 = result.filter("order_date = '2024-01-01'").collect()[0]
        assert day1.total_revenue == 350.0
        assert day1.total_orders == 2

    def test_null_handling(self):
        """Test that null values are handled correctly."""
        from src.transforms import clean_customer_data

        test_data = [("C001", None, "alice@test.com"), ("C002", "Bob", None)]
        schema = "customer_id STRING, name STRING, email STRING"
        input_df = self.create_test_data(schema, test_data)

        result = clean_customer_data(input_df)

        assert result.filter("name IS NULL").count() == 0
        assert result.filter("email IS NULL").count() == 0

    def test_schema_validation(self):
        """Validate output schema matches expected contract."""
        expected_columns = {"customer_id", "name", "email", "segment", "lifetime_value"}
        from src.transforms import build_customer_360

        result = build_customer_360(self.spark)
        actual_columns = set(result.columns)
        missing = expected_columns - actual_columns
        assert not missing, f"Missing columns: {missing}"

# Run: pytest tests/test_etl.py -v --tb=short`,
  },
  {
    id: 14,
    category: 'Databricks Asset Bundles',
    title: 'DABs with Permissions and Cluster Config',
    desc: 'Configure fine-grained permissions and cluster policies in Databricks Asset Bundles',
    code: `# resources/clusters.yml - Cluster and permission configurations
resources:
  clusters:
    shared_analytics:
      cluster_name: "Shared Analytics Cluster"
      spark_version: "14.3.x-scala2.12"
      node_type_id: "Standard_DS4_v2"
      autoscale:
        min_workers: 1
        max_workers: 8
      spark_conf:
        spark.sql.adaptive.enabled: "true"
        spark.sql.shuffle.partitions: "auto"
        spark.databricks.delta.optimizeWrite.enabled: "true"
        spark.databricks.delta.autoCompact.enabled: "true"
      custom_tags:
        team: "analytics"
        cost_center: "CC-1234"
        environment: "\${bundle.target}"
      autotermination_minutes: 60

  permissions:
    job_permissions:
      - job_id: \${resources.jobs.daily_etl_pipeline.id}
        access_control_list:
          - group_name: "data-engineers"
            permission_level: "CAN_MANAGE"
          - group_name: "data-analysts"
            permission_level: "CAN_VIEW"
          - service_principal_name: "sp-production-deployer"
            permission_level: "IS_OWNER"

    cluster_permissions:
      - cluster_id: \${resources.clusters.shared_analytics.id}
        access_control_list:
          - group_name: "data-engineers"
            permission_level: "CAN_RESTART"
          - group_name: "data-analysts"
            permission_level: "CAN_ATTACH_TO"

# Variable overrides per target
targets:
  production:
    variables:
      cluster_size: "Standard_DS5_v2"
      max_workers: 16
      auto_terminate_mins: 30`,
  },
  {
    id: 15,
    category: 'Monitoring',
    title: 'SLA Tracking and Reporting',
    desc: 'Build comprehensive SLA tracking with historical reporting and trend analysis',
    code: `from pyspark.sql.functions import col, avg, count, when, round as spark_round
from datetime import datetime, timedelta

class SLATracker:
    def __init__(self, spark, sla_table="monitoring.sla_definitions",
                 history_table="monitoring.sla_history"):
        self.spark = spark
        self.sla_table = sla_table
        self.history_table = history_table

    def define_sla(self, sla_id, name, metric_type, target_value, owner):
        """Define an SLA target."""
        self.spark.sql(f"""
            INSERT INTO {self.sla_table} VALUES (
                '{sla_id}', '{name}', '{metric_type}', {target_value},
                '{owner}', 'ACTIVE', current_timestamp()
            )
        """)

    def record_metric(self, sla_id, actual_value):
        """Record an SLA metric measurement."""
        target = self.spark.sql(f"""
            SELECT target_value FROM {self.sla_table}
            WHERE sla_id = '{sla_id}' AND status = 'ACTIVE'
        """).collect()[0].target_value

        met = actual_value >= target if "uptime" in sla_id else actual_value <= target
        self.spark.sql(f"""
            INSERT INTO {self.history_table} VALUES (
                '{sla_id}', {actual_value}, {target}, {met}, current_timestamp()
            )
        """)
        return {"sla_id": sla_id, "actual": actual_value, "target": target, "met": met}

    def generate_report(self, lookback_days=30):
        """Generate SLA compliance report."""
        report = self.spark.sql(f"""
            SELECT
                d.sla_id, d.name, d.metric_type, d.target_value, d.owner,
                COUNT(h.met) AS total_checks,
                SUM(CASE WHEN h.met THEN 1 ELSE 0 END) AS met_count,
                ROUND(SUM(CASE WHEN h.met THEN 1 ELSE 0 END) * 100.0 /
                    COUNT(*), 2) AS compliance_pct,
                ROUND(AVG(h.actual_value), 4) AS avg_actual
            FROM {self.sla_table} d
            JOIN {self.history_table} h ON d.sla_id = h.sla_id
            WHERE h.recorded_at >= current_date() - INTERVAL {lookback_days} DAYS
            GROUP BY d.sla_id, d.name, d.metric_type, d.target_value, d.owner
            ORDER BY compliance_pct ASC
        """)
        return report

tracker = SLATracker(spark)
tracker.define_sla("SLA-FRESH-001", "Customer 360 Freshness", "freshness_hours", 4, "data-eng")
tracker.define_sla("SLA-QUALITY-001", "Gold Table Quality", "pass_rate", 99.5, "data-eng")
tracker.define_sla("SLA-UPTIME-001", "Pipeline Uptime", "uptime_pct", 99.9, "platform")

tracker.record_metric("SLA-FRESH-001", 2.5)
tracker.record_metric("SLA-QUALITY-001", 99.8)
tracker.record_metric("SLA-UPTIME-001", 99.95)

report = tracker.generate_report(30)
display(report)`,
  },
];

const securityCategories = [...new Set(securityScenarios.map((s) => s.category))];
const governanceCategories = [...new Set(governanceScenarios.map((s) => s.category))];
const deploymentCategories = [...new Set(deploymentScenarios.map((s) => s.category))];

function SecurityGovernance() {
  const [activeTab, setActiveTab] = useState('security');
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const getScenarios = () => {
    if (activeTab === 'security') return securityScenarios;
    if (activeTab === 'governance') return governanceScenarios;
    return deploymentScenarios;
  };

  const getCategories = () => {
    if (activeTab === 'security') return securityCategories;
    if (activeTab === 'governance') return governanceCategories;
    return deploymentCategories;
  };

  const getBadgeClass = () => {
    if (activeTab === 'security') return 'badge failed';
    if (activeTab === 'governance') return 'badge success';
    return 'badge completed';
  };

  const scenarios = getScenarios();
  const categories = getCategories();

  const filtered = scenarios.filter((s) => {
    const matchCategory = selectedCategory === 'All' || s.category === selectedCategory;
    const matchSearch =
      s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.desc.toLowerCase().includes(searchTerm.toLowerCase());
    return matchCategory && matchSearch;
  });

  const handleTabChange = (tab) => {
    setActiveTab(tab);
    setSelectedCategory('All');
    setExpandedId(null);
    setSearchTerm('');
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Data Security, Governance & Deployment</h1>
          <p>
            {securityScenarios.length + governanceScenarios.length + deploymentScenarios.length}{' '}
            PySpark scenarios for secure, governed, and automated Databricks environments
          </p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon" style={{ background: 'var(--danger)', color: 'white' }}>
            &#128274;
          </div>
          <div className="stat-info">
            <h4>{securityScenarios.length}</h4>
            <p>Security Scenarios</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon" style={{ background: 'var(--success)', color: 'white' }}>
            &#128220;
          </div>
          <div className="stat-info">
            <h4>{governanceScenarios.length}</h4>
            <p>Governance Scenarios</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon" style={{ background: 'var(--primary)', color: 'white' }}>
            &#128640;
          </div>
          <div className="stat-info">
            <h4>{deploymentScenarios.length}</h4>
            <p>Deployment Scenarios</p>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem' }}>
          <button
            className={`btn ${activeTab === 'security' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => handleTabChange('security')}
          >
            Data Security ({securityScenarios.length})
          </button>
          <button
            className={`btn ${activeTab === 'governance' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => handleTabChange('governance')}
          >
            Data Governance ({governanceScenarios.length})
          </button>
          <button
            className={`btn ${activeTab === 'deployment' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => handleTabChange('deployment')}
          >
            Data Deployment ({deploymentScenarios.length})
          </button>
        </div>

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
            style={{ maxWidth: '250px' }}
          >
            <option value="All">All Categories ({scenarios.length})</option>
            {categories.map((cat) => (
              <option key={cat} value={cat}>
                {cat} ({scenarios.filter((s) => s.category === cat).length})
              </option>
            ))}
          </select>
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Showing {filtered.length} of {scenarios.length}
          </span>
        </div>
      </div>

      <div className="scenarios-list">
        {filtered.map((scenario) => (
          <div
            key={`${activeTab}-${scenario.id}`}
            className="card scenario-card"
            style={{ marginBottom: '0.75rem' }}
          >
            <div
              className="scenario-header"
              onClick={() =>
                setExpandedId(
                  expandedId === `${activeTab}-${scenario.id}`
                    ? null
                    : `${activeTab}-${scenario.id}`
                )
              }
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
                  <span className={getBadgeClass()}>{scenario.category}</span>
                  <strong>
                    #{scenario.id} — {scenario.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                  {scenario.desc}
                </p>
              </div>
              <span style={{ fontSize: '1.2rem', color: 'var(--text-secondary)' }}>
                {expandedId === `${activeTab}-${scenario.id}` ? '\u25BC' : '\u25B6'}
              </span>
            </div>
            {expandedId === `${activeTab}-${scenario.id}` && <ScenarioCard scenario={scenario} />}
          </div>
        ))}
      </div>
    </div>
  );
}

export default SecurityGovernance;
