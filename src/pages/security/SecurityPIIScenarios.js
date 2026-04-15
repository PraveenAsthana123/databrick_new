import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

const securityScenarios = [
  // ─── 1–10: Basic Security ───
  {
    id: 1,
    group: 'Basic Security',
    title: 'Data at Rest Encryption',
    flow: 'Storage → encrypted AES-256',
    complexity: 2,
    scale: 5,
    sensitivity: 4,
    enforcement: 4,
    audit: 4,
    score: 3.8,
    code: `-- Enable Delta table encryption with managed keys
ALTER TABLE analytics.silver.customers
  SET TBLPROPERTIES ('delta.encryption.enabled' = 'true');

-- Python: write encrypted Delta table
df.write.format("delta") \\
  .option("delta.encryption.enabled", "true") \\
  .mode("overwrite") \\
  .saveAsTable("analytics.silver.customers")

-- Verify encryption status
DESCRIBE EXTENDED analytics.silver.customers;`,
  },
  {
    id: 2,
    group: 'Basic Security',
    title: 'Data in Transit Encryption',
    flow: 'TLS/HTTPS enforced',
    complexity: 2,
    scale: 5,
    sensitivity: 4,
    enforcement: 4,
    audit: 4,
    score: 3.8,
    code: `-- Enforce TLS for JDBC connections
spark.conf.set("spark.databricks.jdbc.ssl", "true")
spark.conf.set("spark.databricks.jdbc.ssl.tls.version", "TLSv1.2")

-- Cluster config: encrypt inter-node traffic
# In cluster policy:
# "spark.databricks.clusterPolicy.enableInterNodeEncryption": true

-- Verify TLS on external connections
SELECT * FROM jdbc_table
WHERE _metadata.ssl_enabled = true;`,
  },
  {
    id: 3,
    group: 'Basic Security',
    title: 'Secure File Ingestion',
    flow: 'SFTP/HTTPS → validated → ingested',
    complexity: 2,
    scale: 4,
    sensitivity: 3,
    enforcement: 4,
    audit: 3,
    score: 3.2,
    code: `-- Secure ingestion from SFTP via HTTPS
import paramiko, hashlib

def secure_ingest(remote_path, local_path):
    # Connect via SSH/SFTP with key auth
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.connect('sftp.partner.com', username='svc_acct',
                   key_filename='/dbfs/secrets/sftp_key.pem')
    sftp = client.open_sftp()
    sftp.get(remote_path, local_path)

    # Validate checksum before ingesting
    checksum = hashlib.sha256(open(local_path,'rb').read()).hexdigest()
    return checksum`,
  },
  {
    id: 4,
    group: 'Basic Security',
    title: 'API Security Enforcement',
    flow: 'Auth token → validated → API call',
    complexity: 3,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 4,
    score: 4.0,
    code: `-- Databricks REST API with token auth
import requests

token = dbutils.secrets.get(scope="api-secrets", key="databricks-token")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

response = requests.get(
    "https://adb-xxxx.azuredatabricks.net/api/2.0/clusters/list",
    headers=headers,
    timeout=30
)
response.raise_for_status()`,
  },
  {
    id: 5,
    group: 'Basic Security',
    title: 'Network Isolation',
    flow: 'VNet/VPC → private subnets → workspace',
    complexity: 3,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 4,
    score: 4.2,
    code: `-- Databricks workspace in VNet injection (Azure)
# terraform config
resource "azurerm_databricks_workspace" "main" {
  name                = "secure-databricks"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "premium"

  custom_parameters {
    virtual_network_id                                   = azurerm_virtual_network.main.id
    public_subnet_name                                   = "public-subnet"
    private_subnet_name                                  = "private-subnet"
    no_public_ip                                         = true
  }
}`,
  },
  {
    id: 6,
    group: 'Basic Security',
    title: 'Private Endpoint Access',
    flow: 'No public exposure → private link only',
    complexity: 3,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 4,
    score: 4.0,
    code: `-- Azure Private Link for Databricks
resource "azurerm_private_endpoint" "databricks" {
  name                = "databricks-private-ep"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = azurerm_subnet.private.id

  private_service_connection {
    name                           = "databricks-psc"
    private_connection_resource_id = azurerm_databricks_workspace.main.id
    subresource_names              = ["databricks_ui_api"]
    is_manual_connection           = false
  }
}

-- Disable public access
# workspace_conf: "enableIpAccessLists": "true"`,
  },
  {
    id: 7,
    group: 'Basic Security',
    title: 'IAM-Based Access Control',
    flow: 'RBAC/ABAC → Unity Catalog enforcement',
    complexity: 3,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 4,
    score: 4.2,
    code: `-- Unity Catalog RBAC: role assignment
GRANT USE CATALOG ON CATALOG analytics TO \`data_engineers\`;
GRANT USE SCHEMA ON SCHEMA analytics.silver TO \`data_engineers\`;
GRANT SELECT, MODIFY ON TABLE analytics.silver.orders TO \`data_engineers\`;

-- ABAC: attribute-based via row filter
CREATE OR REPLACE FUNCTION analytics.dept_filter(dept STRING)
RETURNS BOOLEAN
RETURN is_member(dept);

ALTER TABLE analytics.silver.employees
  SET ROW FILTER analytics.dept_filter ON (department);`,
  },
  {
    id: 8,
    group: 'Basic Security',
    title: 'Secrets Management',
    flow: 'Vault/Key Vault → Databricks Secret Scope',
    complexity: 3,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 4,
    score: 4.0,
    code: `-- Databricks Secret Scope backed by Azure Key Vault
# Create secret scope via CLI
databricks secrets create-scope --scope kv-scope \\
  --scope-backend-type AZURE_KEYVAULT \\
  --resource-id /subscriptions/.../vaults/mykeyvault \\
  --dns-name https://mykeyvault.vault.azure.net/

-- Use secrets in notebooks (never logs value)
db_password = dbutils.secrets.get(scope="kv-scope", key="db-password")
jdbc_url = f"jdbc:sqlserver://server:1433;password={db_password}"

-- List secrets (values never exposed)
dbutils.secrets.list("kv-scope")`,
  },
  {
    id: 9,
    group: 'Basic Security',
    title: 'Service Account Security',
    flow: 'Least privilege → service principal access',
    complexity: 3,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 4,
    score: 4.0,
    code: `-- Minimal service principal permissions
-- Step 1: Create service principal in Azure AD

-- Step 2: Grant minimal Unity Catalog permissions
GRANT USE CATALOG ON CATALOG analytics TO \`etl-sp-app-id\`;
GRANT USE SCHEMA ON SCHEMA analytics.bronze TO \`etl-sp-app-id\`;
GRANT MODIFY ON TABLE analytics.bronze.raw_events TO \`etl-sp-app-id\`;

-- Step 3: Verify no over-permissions
SHOW GRANTS TO \`etl-sp-app-id\`;

-- Step 4: Rotate credentials every 90 days (automated)`,
  },
  {
    id: 10,
    group: 'Basic Security',
    title: 'Backup Encryption',
    flow: 'Encrypted backups → verified restore',
    complexity: 2,
    scale: 4,
    sensitivity: 4,
    enforcement: 4,
    audit: 4,
    score: 3.6,
    code: `-- Encrypted Delta table backup
from pyspark.sql import SparkSession

def encrypted_backup(table_name, backup_path):
    df = spark.table(table_name)
    # Write to encrypted storage account
    df.write.format("delta") \\
        .option("fs.azure.account.key.backupstorage.dfs.core.windows.net",
                dbutils.secrets.get("kv-scope", "backup-storage-key")) \\
        .mode("overwrite") \\
        .save(backup_path)
    print(f"Encrypted backup written to {backup_path}")

encrypted_backup("analytics.gold.daily_revenue", "abfss://backup@storage/revenue/")`,
  },

  // ─── 11–20: PII Protection ───
  {
    id: 11,
    group: 'PII Protection',
    title: 'PII Discovery',
    flow: 'Scan tables → detect PII columns → report',
    complexity: 4,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Automated PII discovery using regex scanning
import re
from pyspark.sql import functions as F

PII_PATTERNS = {
    'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    'ssn': r'\d{3}-\d{2}-\d{4}',
    'phone': r'\d{3}[-.\s]\d{3}[-.\s]\d{4}',
    'credit_card': r'\d{4}[- ]\d{4}[- ]\d{4}[- ]\d{4}'
}

def scan_for_pii(df, table_name):
    findings = []
    for col in df.columns:
        sample = df.select(F.col(col).cast("string")).limit(1000).toPandas()
        for pii_type, pattern in PII_PATTERNS.items():
            if sample[col].str.contains(pattern, regex=True, na=False).any():
                findings.append({'table': table_name, 'column': col, 'pii_type': pii_type})
    return findings`,
  },
  {
    id: 12,
    group: 'PII Protection',
    title: 'PII Classification',
    flow: 'Detect → tag sensitive columns → catalog',
    complexity: 4,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Tag PII columns after discovery
ALTER TABLE analytics.silver.customers
  ALTER COLUMN email SET TAGS ('pii' = 'true', 'pii_type' = 'email', 'classification' = 'confidential');
ALTER TABLE analytics.silver.customers
  ALTER COLUMN ssn SET TAGS ('pii' = 'true', 'pii_type' = 'ssn', 'classification' = 'restricted');
ALTER TABLE analytics.silver.customers
  ALTER COLUMN phone SET TAGS ('pii' = 'true', 'pii_type' = 'phone', 'classification' = 'sensitive');

-- Query all classified PII columns
SELECT table_catalog, table_schema, table_name, column_name, tag_value AS pii_type
FROM system.information_schema.column_tags
WHERE tag_name = 'pii_type'
ORDER BY table_name, column_name;`,
  },
  {
    id: 13,
    group: 'PII Protection',
    title: 'Column Masking',
    flow: 'Define mask function → apply to columns',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Column masking functions for PII
CREATE OR REPLACE FUNCTION analytics.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_authorized') THEN email
  ELSE CONCAT(LEFT(email, 2), '***@***.com')
END;

CREATE OR REPLACE FUNCTION analytics.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_authorized') THEN ssn
  ELSE CONCAT('***-**-', RIGHT(ssn, 4))
END;

ALTER TABLE analytics.silver.customers ALTER COLUMN email SET MASK analytics.mask_email;
ALTER TABLE analytics.silver.customers ALTER COLUMN ssn SET MASK analytics.mask_ssn;`,
  },
  {
    id: 14,
    group: 'PII Protection',
    title: 'Dynamic Masking',
    flow: 'Role-based → different mask levels per user',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Dynamic masking: tiered access by role
CREATE OR REPLACE FUNCTION analytics.dynamic_pii_mask(value STRING, pii_type STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('data_admin') THEN value
  WHEN is_member('pii_partial') AND pii_type = 'email' THEN CONCAT(LEFT(value,2), '***@***.com')
  WHEN is_member('pii_partial') AND pii_type = 'phone' THEN CONCAT('***-***-', RIGHT(value,4))
  WHEN is_member('pii_partial') AND pii_type = 'ssn' THEN CONCAT('***-**-', RIGHT(value,4))
  ELSE '***MASKED***'
END;

ALTER TABLE analytics.silver.customers
  ALTER COLUMN email SET MASK analytics.dynamic_pii_mask USING COLUMNS (email, 'email');`,
  },
  {
    id: 15,
    group: 'PII Protection',
    title: 'Tokenization',
    flow: 'Replace PII → token → reversible by authorized',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Tokenization: replace PII with secure tokens
CREATE OR REPLACE FUNCTION analytics.tokenize_pii(value STRING)
RETURNS STRING
RETURN sha2(concat(value, 'SECURE_SALT_2024'), 256);

-- De-tokenize: authorized users only
CREATE OR REPLACE FUNCTION analytics.detokenize_pii(token STRING, original STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('detokenize_authorized')
    AND sha2(concat(original, 'SECURE_SALT_2024'), 256) = token THEN original
  ELSE '***TOKENIZED***'
END;

-- Build tokenized table for analytics use
CREATE OR REPLACE TABLE analytics.gold.customers_tokenized AS
SELECT analytics.tokenize_pii(customer_id) AS token_id,
       analytics.tokenize_pii(email) AS token_email,
       region, segment, lifetime_value
FROM analytics.silver.customers;`,
  },
  {
    id: 16,
    group: 'PII Protection',
    title: 'Data Anonymization',
    flow: 'Remove identity → anonymous analytics dataset',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Full anonymization: replace identity, generalize attributes
CREATE OR REPLACE TABLE analytics.anon.customers AS
SELECT
  abs(hash(customer_id)) AS anon_id,
  CASE
    WHEN age < 25 THEN '18-24'
    WHEN age < 35 THEN '25-34'
    WHEN age < 50 THEN '35-49'
    ELSE '50+'
  END AS age_group,
  LEFT(zip_code, 3) || 'XX' AS zip_prefix,
  region, segment, total_orders, lifetime_value
  -- Dropped: name, email, phone, ssn, dob, address
FROM analytics.silver.customers;

GRANT SELECT ON TABLE analytics.anon.customers TO \`data_scientists\`;`,
  },
  {
    id: 17,
    group: 'PII Protection',
    title: 'Pseudonymization',
    flow: 'Replace identifiers → reversible with key',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Pseudonymization with AES encryption (reversible)
from pyspark.sql import functions as F

PSEUDO_KEY = dbutils.secrets.get("kv-scope", "pseudonym-key")

def pseudonymize(df, pii_cols):
    for col in pii_cols:
        df = df.withColumn(
            col,
            F.base64(F.aes_encrypt(F.col(col).cast("binary"),
                                   F.lit(PSEUDO_KEY)))
        )
    return df

df = spark.table("analytics.silver.customers")
df_pseudo = pseudonymize(df, ['email', 'phone', 'customer_id'])
df_pseudo.write.format("delta").mode("overwrite") \\
    .saveAsTable("analytics.secure.customers_pseudo")`,
  },
  {
    id: 18,
    group: 'PII Protection',
    title: 'PII Field Encryption',
    flow: 'Encrypt PII columns at rest in Delta',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Column-level encryption using Databricks native encryption
from pyspark.sql import functions as F

encryption_key = dbutils.secrets.get("kv-scope", "column-encrypt-key")

df = spark.table("analytics.silver.raw_customers")

df_encrypted = df \\
    .withColumn("email_enc", F.base64(F.aes_encrypt(F.col("email").cast("binary"), F.lit(encryption_key)))) \\
    .withColumn("ssn_enc",   F.base64(F.aes_encrypt(F.col("ssn").cast("binary"),   F.lit(encryption_key)))) \\
    .drop("email", "ssn")

df_encrypted.write.format("delta").mode("overwrite") \\
    .saveAsTable("analytics.secure.customers_encrypted")`,
  },
  {
    id: 19,
    group: 'PII Protection',
    title: 'Geo-Restriction PII',
    flow: 'Country-based data visibility restriction',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- GDPR: restrict EU PII to EU team only
CREATE OR REPLACE FUNCTION analytics.geo_pii_filter(country STRING)
RETURNS BOOLEAN
RETURN CASE
  WHEN is_member('global_admin') THEN TRUE
  WHEN is_member('eu_data_team') AND country IN ('DE','FR','IT','ES','NL','BE','PL') THEN TRUE
  WHEN is_member('us_data_team') AND country = 'US' THEN TRUE
  WHEN is_member('apac_data_team') AND country IN ('AU','JP','SG','IN') THEN TRUE
  ELSE FALSE
END;

ALTER TABLE analytics.silver.customers
  SET ROW FILTER analytics.geo_pii_filter ON (country_code);

-- Verify: EU team sees only EU rows
SELECT COUNT(*), country_code FROM analytics.silver.customers GROUP BY country_code;`,
  },
  {
    id: 20,
    group: 'PII Protection',
    title: 'PII in Logs Protection',
    flow: 'Detect PII in logs → mask before storage',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Scrub PII from application logs before writing to Delta
import re
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(returnType=StringType())
def scrub_pii_from_log(log_line):
    if log_line is None:
        return None
    log_line = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '[EMAIL_REDACTED]', log_line)
    log_line = re.sub(r'\d{3}-\d{2}-\d{4}', '[SSN_REDACTED]', log_line)
    log_line = re.sub(r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b', '[CC_REDACTED]', log_line)
    return log_line

df_clean_logs = spark.table("raw.application_logs") \\
    .withColumn("log_line", scrub_pii_from_log(F.col("log_line")))
df_clean_logs.write.format("delta").mode("append").saveAsTable("secure.clean_logs")`,
  },

  // ─── 21–30: Access Control ───
  {
    id: 21,
    group: 'Access Control',
    title: 'RBAC',
    flow: 'Role → permission assignment → enforcement',
    complexity: 3,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 4,
    score: 4.2,
    code: `-- Role-Based Access Control in Unity Catalog
-- Step 1: Create role groups
CREATE GROUP IF NOT EXISTS data_readers;
CREATE GROUP IF NOT EXISTS data_writers;
CREATE GROUP IF NOT EXISTS data_admins;

-- Step 2: Assign permissions per role
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG analytics TO \`data_readers\`;
GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY ON CATALOG analytics TO \`data_writers\`;
GRANT ALL PRIVILEGES ON CATALOG analytics TO \`data_admins\`;

-- Step 3: Assign users to roles (via SCIM/AD sync)
-- Step 4: Verify
SHOW GRANTS TO \`data_readers\`;`,
  },
  {
    id: 22,
    group: 'Access Control',
    title: 'ABAC',
    flow: 'Attribute-driven → dynamic access decision',
    complexity: 4,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Attribute-Based Access Control using row filters + column masks
-- Users see only rows/columns matching their attributes

-- Row filter: match user's department attribute
CREATE OR REPLACE FUNCTION analytics.abac_dept_filter(owner_dept STRING)
RETURNS BOOLEAN
RETURN is_member(owner_dept) OR is_member('super_admin');

-- Column mask: classification-based visibility
CREATE OR REPLACE FUNCTION analytics.abac_classification_mask(value STRING, classification STRING)
RETURNS STRING
RETURN CASE
  WHEN classification = 'public' THEN value
  WHEN classification = 'internal' AND NOT is_member('external_users') THEN value
  WHEN classification = 'restricted' AND is_member('restricted_access') THEN value
  ELSE '***ACCESS_DENIED***'
END;`,
  },
  {
    id: 23,
    group: 'Access Control',
    title: 'Row-Level Security',
    flow: 'Filter rows by user role/region/tenant',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Multi-tenant row-level security
CREATE OR REPLACE FUNCTION analytics.tenant_rls(tenant_id STRING)
RETURNS BOOLEAN
RETURN CASE
  WHEN is_member('platform_admin') THEN TRUE
  WHEN current_user() IN (
    SELECT user_email FROM analytics.admin.tenant_user_map
    WHERE tenant_id = tenant_id
  ) THEN TRUE
  ELSE FALSE
END;

ALTER TABLE analytics.silver.orders
  SET ROW FILTER analytics.tenant_rls ON (tenant_id);

-- Test: user sees only their tenant's rows
SELECT COUNT(*), tenant_id FROM analytics.silver.orders GROUP BY tenant_id;`,
  },
  {
    id: 24,
    group: 'Access Control',
    title: 'Column-Level Security',
    flow: 'Restrict column visibility by role',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Column-level security via view-based access
-- Analysts: no PII columns
CREATE OR REPLACE VIEW analytics.gold.customers_analyst AS
SELECT
  customer_id, region, segment,
  total_orders, lifetime_value, account_status
  -- Excludes: name, email, phone, ssn, dob, address
FROM analytics.silver.customers;

GRANT SELECT ON VIEW analytics.gold.customers_analyst TO \`data_analysts\`;
REVOKE SELECT ON TABLE analytics.silver.customers FROM \`data_analysts\`;

-- Full access only for authorized team
GRANT SELECT ON TABLE analytics.silver.customers TO \`pii_authorized\`;`,
  },
  {
    id: 25,
    group: 'Access Control',
    title: 'Just-in-Time Access',
    flow: 'Temporary elevated access → auto-revoke',
    complexity: 5,
    scale: 3,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Just-in-Time (JIT) access with auto-revocation
import time
from datetime import datetime, timedelta

def grant_jit_access(user_email, table_name, duration_hours=8):
    # Grant temporary access
    spark.sql(f"GRANT SELECT ON TABLE {table_name} TO \`{user_email}\`")

    # Log the grant
    spark.sql(f"""
        INSERT INTO analytics.audit.jit_access_log
        VALUES ('{user_email}', '{table_name}', current_timestamp(),
                timestamp('{(datetime.now()+timedelta(hours=duration_hours)).isoformat()}'), 'ACTIVE')
    """)

    # Schedule auto-revocation
    time.sleep(duration_hours * 3600)
    spark.sql(f"REVOKE SELECT ON TABLE {table_name} FROM \`{user_email}\`")
    spark.sql(f"UPDATE analytics.audit.jit_access_log SET status='REVOKED' WHERE user='{user_email}'")`,
  },
  {
    id: 26,
    group: 'Access Control',
    title: 'MFA Enforcement',
    flow: 'Multi-factor auth → workspace access',
    complexity: 3,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- MFA enforcement via Azure AD Conditional Access
# Azure AD Conditional Access Policy (JSON)
{
  "displayName": "Databricks MFA Required",
  "conditions": {
    "applications": {"includeApplications": ["2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"]},
    "users": {"includeGroups": ["all-databricks-users"]}
  },
  "grantControls": {
    "operator": "AND",
    "builtInControls": ["mfa"]
  },
  "state": "enabled"
}

-- Audit MFA compliance
SELECT user_identity.email, authentication_details.mfa_authenticated
FROM system.access.audit
WHERE action_name = 'login'
  AND authentication_details.mfa_authenticated = false;`,
  },
  {
    id: 27,
    group: 'Access Control',
    title: 'Identity Federation',
    flow: 'SSO → IdP → Databricks workspace',
    complexity: 3,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 4,
    score: 4.2,
    code: `-- SAML 2.0 SSO federation with Azure AD
# Databricks workspace SCIM provisioning
curl -X POST https://adb-xxx.azuredatabricks.net/api/2.0/preview/scim/v2/ServicePrincipals \\
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \\
  -H "Content-Type: application/scim+json" \\
  -d '{
    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
    "applicationId": "<AAD_APP_ID>",
    "displayName": "ETL Service Principal",
    "groups": [{"value": "<group-id>"}]
  }'

-- Verify SSO users in Unity Catalog
SELECT user_name, display_name, active
FROM system.information_schema.users
WHERE user_name NOT LIKE '%@internal%';`,
  },
  {
    id: 28,
    group: 'Access Control',
    title: 'Least Privilege Enforcement',
    flow: 'Minimal permissions → continuous validation',
    complexity: 4,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Detect over-privileged users
SELECT
  grantee, privilege_type, object_type, object_name
FROM system.information_schema.object_privileges
WHERE grantee NOT IN ('data_admins', 'platform_admins')
  AND privilege_type IN ('ALL PRIVILEGES', 'CREATE')
  AND object_type = 'CATALOG'
ORDER BY grantee;

-- Remediate: revoke excess permissions
REVOKE ALL PRIVILEGES ON CATALOG analytics FROM \`over_privileged_user\`;
GRANT USE CATALOG ON CATALOG analytics TO \`over_privileged_user\`;
GRANT SELECT ON SCHEMA analytics.gold TO \`over_privileged_user\`;`,
  },
  {
    id: 29,
    group: 'Access Control',
    title: 'Data Access Segregation',
    flow: 'Domain isolation → no cross-domain leakage',
    complexity: 4,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Domain-isolated catalogs: finance vs HR vs marketing
CREATE CATALOG IF NOT EXISTS finance_domain;
CREATE CATALOG IF NOT EXISTS hr_domain;
CREATE CATALOG IF NOT EXISTS marketing_domain;

-- Strict isolation: each team only sees their catalog
GRANT ALL PRIVILEGES ON CATALOG finance_domain TO \`finance_team\`;
GRANT ALL PRIVILEGES ON CATALOG hr_domain TO \`hr_team\`;
GRANT ALL PRIVILEGES ON CATALOG marketing_domain TO \`marketing_team\`;

-- Cross-domain reads require explicit approval + audit
GRANT SELECT ON TABLE finance_domain.gold.budget_summary TO \`exec_team\`;
INSERT INTO system_audit.cross_domain_access VALUES ('exec_team', 'finance_domain', current_timestamp());`,
  },
  {
    id: 30,
    group: 'Access Control',
    title: 'External Access Control',
    flow: 'Vendor/partner → restricted view → audit',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Secure external vendor access
-- 1: Create sandboxed external catalog
CREATE CATALOG IF NOT EXISTS vendor_sandbox;

-- 2: Expose only approved, curated views
CREATE VIEW vendor_sandbox.shared.product_availability AS
SELECT product_id, category, stock_level, region
FROM analytics.gold.inventory
WHERE is_public = true;  -- No pricing, no costs

-- 3: Restrict vendor identity
GRANT USE CATALOG ON CATALOG vendor_sandbox TO \`vendor-sp-app-id\`;
GRANT SELECT ON VIEW vendor_sandbox.shared.product_availability TO \`vendor-sp-app-id\`;

-- 4: Audit all vendor access
SELECT * FROM system.access.audit
WHERE user_identity.email LIKE '%vendor%'
ORDER BY event_time DESC LIMIT 100;`,
  },

  // ─── 31–40: Compliance / Lifecycle ───
  {
    id: 31,
    group: 'Compliance/Lifecycle',
    title: 'GDPR Compliance',
    flow: 'Consent tracking → data rights → enforcement',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- GDPR: consent management + right to access + right to erasure
-- 1. Tag all PII columns
ALTER TABLE analytics.silver.customers
  ALTER COLUMN email SET TAGS ('gdpr' = 'true', 'lawful_basis' = 'consent');

-- 2. Right to Access: export user data
SELECT * FROM analytics.silver.customers WHERE email = 'user@example.com'
UNION ALL
SELECT * FROM analytics.silver.orders WHERE customer_email = 'user@example.com';

-- 3. Right to Erasure: delete all user data
DELETE FROM analytics.silver.customers WHERE email = 'user@example.com';
DELETE FROM analytics.silver.orders WHERE customer_email = 'user@example.com';
VACUUM analytics.silver.customers RETAIN 0 HOURS;  -- physically remove

-- 4. Log erasure event
INSERT INTO analytics.compliance.gdpr_log
VALUES ('user@example.com', 'ERASURE', current_timestamp(), current_user());`,
  },
  {
    id: 32,
    group: 'Compliance/Lifecycle',
    title: 'Right to Be Forgotten',
    flow: 'Request → identify → delete → verify → log',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Right to Erasure pipeline across all tables
def execute_rtbf(user_email: str):
    # Find all tables with PII tagged columns
    pii_tables = spark.sql("""
        SELECT DISTINCT table_catalog, table_schema, table_name
        FROM system.information_schema.column_tags
        WHERE tag_name = 'pii' AND tag_value = 'true'
    """).collect()

    deleted_counts = {}
    for row in pii_tables:
        table = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
        try:
            result = spark.sql(f"DELETE FROM {table} WHERE email = '{user_email}'")
            deleted_counts[table] = result.first()['num_affected_rows']
        except Exception as e:
            print(f"Warning: {table} - {e}")

    # Vacuum all affected tables
    for table in deleted_counts:
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS")

    return deleted_counts`,
  },
  {
    id: 33,
    group: 'Compliance/Lifecycle',
    title: 'HIPAA Compliance',
    flow: 'PHI isolation → encryption → access log',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- HIPAA: Protected Health Information controls
-- 1. PHI in isolated schema
CREATE SCHEMA IF NOT EXISTS healthcare.phi_secure;
REVOKE ALL ON SCHEMA healthcare.phi_secure FROM PUBLIC;

-- 2. Column masking for PHI fields
CREATE FUNCTION healthcare.mask_diagnosis(diag STRING)
RETURNS STRING
RETURN IF(is_member('treating_physician'), diag, '***PHI_RESTRICTED***');

ALTER TABLE healthcare.phi_secure.patient_records
  ALTER COLUMN diagnosis SET MASK healthcare.mask_diagnosis;
ALTER TABLE healthcare.phi_secure.patient_records
  ALTER COLUMN ssn SET MASK healthcare.mask_ssn;

-- 3. Access audit (HIPAA requires 6-year retention)
SELECT * FROM system.access.audit
WHERE request_params.full_name_arg LIKE '%phi_secure%'
ORDER BY event_time DESC;`,
  },
  {
    id: 34,
    group: 'Compliance/Lifecycle',
    title: 'PCI Compliance',
    flow: 'Cardholder data → tokenized → audit trail',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- PCI DSS: Payment card data protection
-- 1. Never store full PAN - tokenize immediately
CREATE OR REPLACE FUNCTION payments.tokenize_pan(pan STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('payment_processor') THEN pan
  ELSE CONCAT(LEFT(pan, 6), '******', RIGHT(pan, 4))
END;

ALTER TABLE payments.secure.transactions
  ALTER COLUMN card_number SET MASK payments.tokenize_pan;

-- 2. Isolate CDE (Cardholder Data Environment)
REVOKE ALL ON CATALOG payments FROM PUBLIC;
GRANT USE CATALOG ON CATALOG payments TO \`payment_team\`;

-- 3. PCI audit: who accessed card data
SELECT event_time, user_identity.email, action_name
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '%payments%'
  AND event_date >= current_date() - 90;`,
  },
  {
    id: 35,
    group: 'Compliance/Lifecycle',
    title: 'SOX Compliance',
    flow: 'Financial data → segregation of duties → immutable audit',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- SOX: financial controls and audit trail
-- 1. Segregation of duties
REVOKE MODIFY ON SCHEMA finance.gl FROM \`finance_analysts\`;
GRANT MODIFY ON SCHEMA finance.gl TO \`finance_controllers\`;

-- 2. Immutable audit log (append-only)
CREATE TABLE IF NOT EXISTS finance.audit.gl_changes (
  changed_by STRING, change_type STRING,
  table_name STRING, record_key STRING,
  old_value STRING, new_value STRING,
  changed_at TIMESTAMP
) TBLPROPERTIES ('delta.appendOnly' = 'true');

-- 3. SOX quarterly evidence export
CREATE OR REPLACE TABLE finance.compliance.sox_q1_2024 AS
SELECT event_time, user_identity.email, action_name, request_params
FROM system.access.audit
WHERE event_date BETWEEN '2024-01-01' AND '2024-03-31'
  AND request_params.full_name_arg LIKE '%finance%';`,
  },
  {
    id: 36,
    group: 'Compliance/Lifecycle',
    title: 'Data Retention Policy',
    flow: 'Classify data → apply retention rules → auto-purge',
    complexity: 4,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Tiered data retention policy enforcement
-- Bronze: 90 days, Silver: 1 year, Gold: 3 years, Compliance: 7 years

ALTER TABLE analytics.bronze.raw_events
  SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 90 days');

-- Automated purge job
def enforce_retention():
    retention_rules = [
        ("analytics.bronze.raw_events", 90),
        ("analytics.silver.orders", 365),
        ("analytics.gold.daily_revenue", 1095),
    ]
    for table, days in retention_rules:
        spark.sql(f"DELETE FROM {table} WHERE _ingest_ts < current_date() - {days}")
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
        spark.sql(f"INSERT INTO analytics.audit.retention_log VALUES ('{table}', {days}, current_timestamp())")`,
  },
  {
    id: 37,
    group: 'Compliance/Lifecycle',
    title: 'Data Archival',
    flow: 'Active data → cold storage → queryable archive',
    complexity: 3,
    scale: 5,
    sensitivity: 4,
    enforcement: 4,
    audit: 5,
    score: 4.2,
    code: `-- Archive old data to cold storage
-- Step 1: Clone old partitions to archive storage
CREATE OR REPLACE TABLE archive_catalog.gold.daily_revenue_2022
  DEEP CLONE analytics.gold.daily_revenue
  LOCATION 'abfss://cold-archive@storage/revenue/2022/'
  WHERE year = 2022;

-- Step 2: Vacuum source (delete old files)
DELETE FROM analytics.gold.daily_revenue WHERE year = 2022;
VACUUM analytics.gold.daily_revenue RETAIN 0 HOURS;

-- Step 3: Archive is still queryable
SELECT SUM(total_revenue) FROM archive_catalog.gold.daily_revenue_2022
WHERE quarter = 'Q4';`,
  },
  {
    id: 38,
    group: 'Compliance/Lifecycle',
    title: 'Data Purge Pipeline',
    flow: 'Identify expired data → purge → verify → log',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Automated data purge for expired PII
def run_purge_pipeline(dry_run=True):
    purge_rules = spark.table("analytics.compliance.purge_rules").collect()

    for rule in purge_rules:
        count_query = f"""
            SELECT COUNT(*) AS cnt FROM {rule.table_name}
            WHERE {rule.expiry_column} < current_date() - {rule.retention_days}
        """
        expired_count = spark.sql(count_query).first()['cnt']
        print(f"{rule.table_name}: {expired_count} rows eligible for purge")

        if not dry_run and expired_count > 0:
            spark.sql(f"""
                DELETE FROM {rule.table_name}
                WHERE {rule.expiry_column} < current_date() - {rule.retention_days}
            """)
            spark.sql(f"VACUUM {rule.table_name} RETAIN 168 HOURS")
            spark.sql(f"INSERT INTO analytics.audit.purge_log VALUES ('{rule.table_name}', {expired_count}, current_timestamp())")`,
  },
  {
    id: 39,
    group: 'Compliance/Lifecycle',
    title: 'Audit Log Retention',
    flow: 'Capture access logs → long-term storage → compliance query',
    complexity: 3,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Export system audit logs for long-term retention (7 years)
CREATE TABLE IF NOT EXISTS compliance.audit_archive.access_log_2024
USING DELTA
LOCATION 'abfss://compliance-archive@storage/audit/2024/'
TBLPROPERTIES ('delta.appendOnly' = 'true')
AS
SELECT
  event_time, event_date,
  user_identity.email AS user_email,
  action_name, source_ip_address,
  request_params.full_name_arg AS resource_accessed,
  response.status_code
FROM system.access.audit
WHERE event_date BETWEEN '2024-01-01' AND '2024-12-31';

-- Compliance query: who accessed sensitive data last quarter
SELECT user_email, COUNT(*) AS access_count, COUNT(DISTINCT resource_accessed) AS tables_accessed
FROM compliance.audit_archive.access_log_2024
WHERE resource_accessed LIKE '%secure%'
GROUP BY user_email ORDER BY access_count DESC;`,
  },
  {
    id: 40,
    group: 'Compliance/Lifecycle',
    title: 'Breach Response Tracking',
    flow: 'Detect incident → contain → notify → remediate → document',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Incident response: data breach tracking
def handle_breach_incident(incident_id, affected_table, suspected_user):
    # 1. Immediately revoke access
    spark.sql(f"REVOKE ALL PRIVILEGES ON TABLE {affected_table} FROM \`{suspected_user}\`")

    # 2. Capture forensic snapshot
    spark.sql(f"""
        CREATE TABLE security.incidents.forensic_{incident_id} AS
        SELECT * FROM system.access.audit
        WHERE user_identity.email = '{suspected_user}'
          AND event_date >= current_date() - 30
        ORDER BY event_time DESC
    """)

    # 3. Log incident
    spark.sql(f"""
        INSERT INTO security.incidents.breach_log
        VALUES ('{incident_id}', '{affected_table}', '{suspected_user}',
                'ACTIVE', current_timestamp(), current_user())
    """)

    # 4. Notify (via webhook/email)
    return f"Incident {incident_id} contained. Access revoked for {suspected_user}."`,
  },

  // ─── 41–50: Advanced / AI Security ───
  {
    id: 41,
    group: 'Advanced/AI Security',
    title: 'RAG Data Security',
    flow: 'Secure docs → controlled embedding → authorized retrieval',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Secure RAG pipeline with access-controlled retrieval
-- 1. Classify source documents
ALTER TABLE analytics.bronze.rag_documents
  SET TAGS ('classification' = 'confidential', 'rag_enabled' = 'true');

-- 2. Only authorized pipeline reads docs
GRANT SELECT ON TABLE analytics.bronze.rag_documents TO \`rag-pipeline-sp\`;

-- 3. Tag chunks with source classification
CREATE OR REPLACE TABLE analytics.ml.doc_chunks AS
SELECT chunk_id, document_id, chunk_text,
       source_classification, allowed_groups
FROM analytics.bronze.rag_documents
CROSS JOIN LATERAL chunk_document(content, 512);

-- 4. Vector Search endpoint: filter by user's groups
-- In retrieval code:
results = vector_search.similarity_search(
    query=user_query, k=5,
    filter={"allowed_groups": {"$in": user_groups}}
)`,
  },
  {
    id: 42,
    group: 'Advanced/AI Security',
    title: 'Embedding Security',
    flow: 'Protect vector embeddings → access-controlled index',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Secure embedding storage and retrieval
-- 1. Store embeddings in access-controlled Delta table
CREATE TABLE IF NOT EXISTS analytics.ml.document_embeddings (
  doc_id STRING, embedding ARRAY<FLOAT>,
  classification STRING, allowed_groups ARRAY<STRING>,
  created_at TIMESTAMP
);

GRANT SELECT ON TABLE analytics.ml.document_embeddings TO \`ml_engineers\`;
REVOKE SELECT ON TABLE analytics.ml.document_embeddings FROM PUBLIC;

-- 2. Apply row filter: users see only their clearance level
CREATE FUNCTION analytics.embedding_access_filter(classification STRING)
RETURNS BOOLEAN
RETURN CASE
  WHEN is_member('ml_admin') THEN TRUE
  WHEN classification = 'public' THEN TRUE
  WHEN classification = 'internal' AND NOT is_member('external') THEN TRUE
  ELSE FALSE
END;

ALTER TABLE analytics.ml.document_embeddings
  SET ROW FILTER analytics.embedding_access_filter ON (classification);`,
  },
  {
    id: 43,
    group: 'Advanced/AI Security',
    title: 'LLM Prompt Security',
    flow: 'Validate prompt → detect injection → safe execution',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- LLM prompt injection prevention
import re

INJECTION_PATTERNS = [
    r'ignore.*previous.*instructions',
    r'system.*prompt',
    r'jailbreak',
    r'act as.*admin',
    r'reveal.*secrets',
    r'DROP TABLE|DELETE FROM|INSERT INTO',  # SQL injection
]

def sanitize_prompt(user_input: str) -> str:
    for pattern in INJECTION_PATTERNS:
        if re.search(pattern, user_input, re.IGNORECASE):
            raise ValueError(f"Potential prompt injection detected: {pattern}")

    # Log all prompts for audit
    spark.sql(f"""
        INSERT INTO security.ai_audit.prompt_log
        VALUES (current_user(), '{user_input[:200]}', current_timestamp(), 'ALLOWED')
    """)
    return user_input[:4096]  # Enforce max length`,
  },
  {
    id: 44,
    group: 'Advanced/AI Security',
    title: 'AI Model Data Leakage Prevention',
    flow: 'Secure training data → differential privacy → safe model',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Prevent PII leakage from AI model training
from pyspark.sql import functions as F

def prepare_secure_training_data(raw_table, output_table):
    df = spark.table(raw_table)

    df_safe = df \\
        .drop("email", "phone", "ssn", "name", "address") \\
        .withColumn("customer_id", F.hash(F.col("customer_id"))) \\
        .withColumn("age", F.when(F.col("age") < 18, 18)
                            .when(F.col("age") > 90, 90)
                            .otherwise(F.col("age"))) \\
        .withColumn("zip_code", F.left(F.col("zip_code"), F.lit(3)))

    # Add Laplace noise for differential privacy
    epsilon = 1.0
    df_private = df_safe.withColumn(
        "amount",
        F.col("amount") + F.randn() * (1.0 / epsilon)
    )
    df_private.write.format("delta").mode("overwrite").saveAsTable(output_table)
    print(f"Secure training data written to {output_table}")`,
  },
  {
    id: 45,
    group: 'Advanced/AI Security',
    title: 'Feature Store Security',
    flow: 'Protect ML features → access-controlled serving',
    complexity: 4,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.4,
    code: `-- Secure Feature Store access
-- 1. Grant minimal permissions on feature tables
GRANT SELECT ON TABLE analytics.ml.customer_features TO \`ml_engineers\`;
GRANT MODIFY ON TABLE analytics.ml.customer_features TO \`feature-pipeline-sp\`;

-- 2. Tag feature columns with sensitivity
ALTER TABLE analytics.ml.customer_features
  ALTER COLUMN churn_score SET TAGS ('feature_type'='target', 'sensitivity'='low');
ALTER TABLE analytics.ml.customer_features
  ALTER COLUMN income_estimate SET TAGS ('feature_type'='inferred_pii', 'sensitivity'='high');

-- 3. Mask sensitive derived features
CREATE FUNCTION analytics.mask_inferred_pii(value FLOAT)
RETURNS FLOAT
RETURN IF(is_member('ml_admin'), value, NULL);

ALTER TABLE analytics.ml.customer_features
  ALTER COLUMN income_estimate SET MASK analytics.mask_inferred_pii;`,
  },
  {
    id: 46,
    group: 'Advanced/AI Security',
    title: 'Cross-Region Security',
    flow: 'Multi-region data → consistent policies → geo-fenced',
    complexity: 5,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 5.0,
    code: `-- Cross-region security with data residency enforcement
-- Region-specific catalogs with identical policy framework
CREATE CATALOG IF NOT EXISTS eu_west_data
  COMMENT 'EU West data - GDPR compliant, data stays in EU';
CREATE CATALOG IF NOT EXISTS us_east_data
  COMMENT 'US East data - CCPA compliant, data stays in US';

-- Apply geo-residency tag to all EU tables
ALTER TABLE eu_west_data.silver.customers
  SET TAGS ('data_residency' = 'EU', 'gdpr' = 'true');

-- Cross-region access: only anonymized, geo-validated data
CREATE VIEW global_analytics.gold.customer_summary AS
SELECT region, COUNT(*) AS customer_count, AVG(lifetime_value) AS avg_ltv
FROM eu_west_data.gold.customers_anon
UNION ALL
SELECT region, COUNT(*), AVG(lifetime_value)
FROM us_east_data.gold.customers_anon;`,
  },
  {
    id: 47,
    group: 'Advanced/AI Security',
    title: 'Multi-Cloud Security',
    flow: 'AWS + Azure + GCP → unified policy → consistent enforcement',
    complexity: 5,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 5.0,
    code: `-- Unity Catalog: unified security across AWS, Azure, GCP
-- Storage credentials per cloud
CREATE STORAGE CREDENTIAL aws_s3_cred
  WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789:role/databricks-role');

CREATE STORAGE CREDENTIAL azure_adls_cred
  WITH (AZURE_MANAGED_IDENTITY = '/subscriptions/.../userAssignedIdentities/databricks-id');

-- External locations per cloud
CREATE EXTERNAL LOCATION aws_data_lake
  URL 's3://company-data-lake/'
  WITH (STORAGE CREDENTIAL aws_s3_cred);

CREATE EXTERNAL LOCATION azure_data_lake
  URL 'abfss://data@storage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL azure_adls_cred);

-- Unified permissions apply regardless of underlying cloud
GRANT READ FILES ON EXTERNAL LOCATION aws_data_lake TO \`data_engineers\`;
GRANT READ FILES ON EXTERNAL LOCATION azure_data_lake TO \`data_engineers\`;`,
  },
  {
    id: 48,
    group: 'Advanced/AI Security',
    title: 'Data Sharing Security',
    flow: 'Delta Sharing → recipient auth → audit',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Secure Delta Sharing with access controls
-- 1. Create share with filtered views only
CREATE SHARE IF NOT EXISTS secure_partner_share;
ALTER SHARE secure_partner_share
  ADD TABLE analytics.gold.public_metrics;

-- 2. Add partitioned filter (share only non-PII columns)
ALTER SHARE secure_partner_share
  ADD TABLE analytics.gold.product_catalog
  PARTITION (is_public = true);

-- 3. Create recipient with IP allowlist
CREATE RECIPIENT IF NOT EXISTS partner_corp
  IP_ADDRESS_FILTER '203.0.113.0/24'
  COMMENT 'External partner - restricted IP range';

-- 4. Grant share to recipient
GRANT SELECT ON SHARE secure_partner_share TO RECIPIENT partner_corp;

-- 5. Audit sharing access
SELECT * FROM system.access.delta_sharing_audit
ORDER BY event_time DESC LIMIT 50;`,
  },
  {
    id: 49,
    group: 'Advanced/AI Security',
    title: 'Insider Threat Detection',
    flow: 'Baseline behavior → anomaly detection → alert',
    complexity: 5,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 5.0,
    code: `-- Insider threat: detect abnormal data access patterns
WITH user_baseline AS (
  SELECT user_identity.email AS user_email,
         AVG(daily_access_count) AS avg_daily_access,
         STDDEV(daily_access_count) AS stddev_access
  FROM (
    SELECT user_identity.email, event_date, COUNT(*) AS daily_access_count
    FROM system.access.audit
    WHERE event_date BETWEEN current_date() - 30 AND current_date() - 1
    GROUP BY user_identity.email, event_date
  )
  GROUP BY user_identity.email
),
today_access AS (
  SELECT user_identity.email, COUNT(*) AS today_count
  FROM system.access.audit
  WHERE event_date = current_date()
  GROUP BY user_identity.email
)
SELECT t.user_email, t.today_count,
       b.avg_daily_access, b.stddev_access,
       (t.today_count - b.avg_daily_access) / NULLIF(b.stddev_access, 0) AS z_score
FROM today_access t JOIN user_baseline b ON t.user_email = b.user_email
WHERE (t.today_count - b.avg_daily_access) / NULLIF(b.stddev_access, 0) > 3
ORDER BY z_score DESC;`,
  },
  {
    id: 50,
    group: 'Advanced/AI Security',
    title: 'Enterprise Security Framework',
    flow: 'End-to-end security across all layers',
    complexity: 5,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 5.0,
    code: `-- Enterprise Security Framework health check
-- Verify all security layers are active

SELECT 'encryption' AS check_name, COUNT(*) AS compliant_tables,
       'Delta encryption enabled' AS description
FROM system.information_schema.table_properties
WHERE property_name = 'delta.encryption.enabled' AND property_value = 'true'

UNION ALL
SELECT 'pii_tagged', COUNT(*), 'Columns tagged with PII classification'
FROM system.information_schema.column_tags WHERE tag_name = 'pii'

UNION ALL
SELECT 'column_masks', COUNT(*), 'Active column masks on PII columns'
FROM system.information_schema.column_masks

UNION ALL
SELECT 'row_filters', COUNT(*), 'Active row-level security filters'
FROM system.information_schema.row_filters

UNION ALL
SELECT 'audit_coverage', COUNT(DISTINCT request_params.full_name_arg), '30-day audit coverage'
FROM system.access.audit
WHERE event_date >= current_date() - 30

ORDER BY check_name;`,
  },
];

const groups = [...new Set(securityScenarios.map((s) => s.group))];

function ScoreBar({ label, value, max }) {
  const pct = (value / max) * 100;
  const color = value >= 4 ? '#22c55e' : value >= 3 ? '#f59e0b' : '#ef4444';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '0.7rem' }}>
      <span style={{ width: '72px', color: 'var(--text-secondary)' }}>{label}</span>
      <div style={{ flex: 1, height: '8px', background: '#f3f4f6', borderRadius: '4px' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: '4px' }} />
      </div>
      <span style={{ width: '18px', textAlign: 'right', fontWeight: 600 }}>{value}</span>
    </div>
  );
}

function SecurityPIIScenarios() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('id');

  const filtered = securityScenarios
    .filter((s) => {
      const matchGroup = selectedGroup === 'All' || s.group === selectedGroup;
      const matchSearch =
        s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
        s.flow.toLowerCase().includes(searchTerm.toLowerCase());
      return matchGroup && matchSearch;
    })
    .sort((a, b) => (sortBy === 'score' ? b.score - a.score : a.id - b.id));

  const downloadScorecard = () => {
    exportToCSV(
      securityScenarios.map((s) => ({
        id: s.id,
        group: s.group,
        title: s.title,
        flow: s.flow,
        complexity: s.complexity,
        scale: s.scale,
        sensitivity: s.sensitivity,
        enforcement: s.enforcement,
        audit: s.audit,
        score: s.score,
      })),
      'security-pii-scorecard.csv'
    );
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Data Security &amp; PII Scenarios</h1>
          <p>
            50 security patterns &mdash; Encryption, PII Protection, Access Control, Compliance, AI
            Security
          </p>
        </div>
      </div>

      <div
        className="card"
        style={{
          marginBottom: '1rem',
          padding: '0.75rem 1rem',
          background: '#fef2f2',
          border: '1px solid #fecaca',
        }}
      >
        <div style={{ fontSize: '0.8rem', color: '#991b1b' }}>
          <strong>5 Security Domains Covered:</strong> <strong>Basic Security</strong> (encryption,
          secrets, network isolation) &mdash; <strong>PII Protection</strong> (discovery, masking,
          tokenization, anonymization) &mdash; <strong>Access Control</strong> (RBAC, ABAC, RLS,
          JIT, MFA) &mdash; <strong>Compliance/Lifecycle</strong> (GDPR, HIPAA, PCI, SOX, retention,
          purge) &mdash; <strong>Advanced/AI Security</strong> (RAG, embeddings, prompt injection,
          insider threats). All patterns use Databricks Unity Catalog, Delta Lake, and PySpark.
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#128737;</div>
          <div className="stat-info">
            <h4>50</h4>
            <p>Scenarios</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#127937;</div>
          <div className="stat-info">
            <h4>{groups.length}</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#11088;</div>
          <div className="stat-info">
            <h4>{(securityScenarios.reduce((s, x) => s + x.score, 0) / 50).toFixed(1)}</h4>
            <p>Avg Score</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#127942;</div>
          <div className="stat-info">
            <h4>{securityScenarios.filter((s) => s.score >= 4.5).length}</h4>
            <p>Critical (4.5+)</p>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search security scenarios..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '280px' }}
          />
          <select
            className="form-input"
            value={selectedGroup}
            onChange={(e) => setSelectedGroup(e.target.value)}
            style={{ maxWidth: '230px' }}
          >
            <option value="All">All Groups (50)</option>
            {groups.map((g) => (
              <option key={g} value={g}>
                {g} ({securityScenarios.filter((s) => s.group === g).length})
              </option>
            ))}
          </select>
          <select
            className="form-input"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            style={{ maxWidth: '150px' }}
          >
            <option value="id">Sort by #</option>
            <option value="score">Sort by Score</option>
          </select>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadScorecard}
            style={{ marginLeft: 'auto' }}
          >
            Download Scorecard (CSV)
          </button>
        </div>
      </div>

      {filtered.map((s) => {
        const isExpanded = expandedId === s.id;
        return (
          <div key={s.id} className="card" style={{ marginBottom: '0.75rem' }}>
            <div
              onClick={() => setExpandedId(isExpanded ? null : s.id)}
              style={{
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <div style={{ flex: 1 }}>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    flexWrap: 'wrap',
                    marginBottom: '0.2rem',
                  }}
                >
                  <span className="badge running">{s.group}</span>
                  <strong>
                    #{s.id} &mdash; {s.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{s.flow}</p>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <span
                  style={{
                    fontSize: '1.1rem',
                    fontWeight: 700,
                    color:
                      s.score >= 4.5
                        ? 'var(--error)'
                        : s.score >= 3.5
                          ? 'var(--warning)'
                          : 'var(--success)',
                  }}
                >
                  {s.score}
                </span>
                <span style={{ color: 'var(--text-secondary)' }}>
                  {isExpanded ? '\u25BC' : '\u25B6'}
                </span>
              </div>
            </div>

            {isExpanded && (
              <div
                style={{
                  marginTop: '1rem',
                  display: 'grid',
                  gridTemplateColumns: '250px 1fr',
                  gap: '1rem',
                }}
              >
                <div style={{ padding: '0.6rem', background: '#f8f9fa', borderRadius: '6px' }}>
                  <div style={{ fontSize: '0.72rem', fontWeight: 700, marginBottom: '0.4rem' }}>
                    Security KPI
                  </div>
                  <ScoreBar label="Complexity" value={s.complexity} max={5} />
                  <ScoreBar label="Scale" value={s.scale} max={5} />
                  <ScoreBar label="Sensitivity" value={s.sensitivity} max={5} />
                  <ScoreBar label="Enforcement" value={s.enforcement} max={5} />
                  <ScoreBar label="Auditability" value={s.audit} max={5} />
                  <div style={{ marginTop: '0.4rem', fontSize: '0.8rem', fontWeight: 700 }}>
                    Overall:{' '}
                    <span
                      style={{
                        color: s.score >= 4.5 ? '#ef4444' : s.score >= 3.5 ? '#f59e0b' : '#22c55e',
                      }}
                    >
                      {s.score}/5.0
                    </span>
                  </div>
                </div>
                <div>
                  <div className="code-block" style={{ maxHeight: '350px', overflowY: 'auto' }}>
                    {s.code}
                  </div>
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default SecurityPIIScenarios;
