import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

const govScenarios = [
  // ─── 1–10: Basic Access Governance ───
  {
    id: 1,
    group: 'Basic Access',
    title: 'Catalog-Level Access',
    flow: 'Grant catalog \u2192 schema \u2192 table access',
    complexity: 2,
    scale: 3,
    sensitivity: 2,
    enforcement: 3,
    audit: 3,
    score: 2.6,
    code: `-- Grant catalog-level access\nGRANT USE CATALOG ON CATALOG analytics TO \`data_team\`;\nGRANT USE SCHEMA ON SCHEMA analytics.bronze TO \`data_team\`;\nGRANT SELECT ON TABLE analytics.bronze.orders TO \`data_team\`;\n\n-- Verify grants\nSHOW GRANTS ON CATALOG analytics;`,
  },
  {
    id: 2,
    group: 'Basic Access',
    title: 'Schema-Level Access',
    flow: 'Restrict schema visibility',
    complexity: 2,
    scale: 3,
    sensitivity: 2,
    enforcement: 3,
    audit: 3,
    score: 2.6,
    code: `-- Schema-level isolation\nGRANT USE SCHEMA ON SCHEMA analytics.silver TO \`analyst_group\`;\nREVOKE USE SCHEMA ON SCHEMA analytics.bronze FROM \`analyst_group\`;\n\n-- Analysts can only see silver, not bronze\nSHOW GRANTS ON SCHEMA analytics.silver;`,
  },
  {
    id: 3,
    group: 'Basic Access',
    title: 'Table-Level Access',
    flow: 'Grant select/insert on tables',
    complexity: 2,
    scale: 4,
    sensitivity: 3,
    enforcement: 3,
    audit: 3,
    score: 3.0,
    code: `-- Table-level permissions\nGRANT SELECT ON TABLE analytics.gold.daily_revenue TO \`bi_team\`;\nGRANT SELECT, MODIFY ON TABLE analytics.silver.orders TO \`etl_service\`;\nGRANT ALL PRIVILEGES ON TABLE analytics.bronze.raw_data TO \`data_engineers\`;\n\nSHOW GRANTS ON TABLE analytics.gold.daily_revenue;`,
  },
  {
    id: 4,
    group: 'Basic Access',
    title: 'Read-Only User Access',
    flow: 'Analyst read access only',
    complexity: 2,
    scale: 4,
    sensitivity: 2,
    enforcement: 3,
    audit: 3,
    score: 2.8,
    code: `-- Read-only access for analysts\nCREATE GROUP IF NOT EXISTS analysts;\nGRANT USE CATALOG ON CATALOG analytics TO \`analysts\`;\nGRANT USE SCHEMA ON SCHEMA analytics.gold TO \`analysts\`;\nGRANT SELECT ON SCHEMA analytics.gold TO \`analysts\`;\n\n-- Verify: no MODIFY or CREATE permissions\nSHOW GRANTS TO \`analysts\`;`,
  },
  {
    id: 5,
    group: 'Basic Access',
    title: 'Developer Write Access',
    flow: 'Dev access to dev schema only',
    complexity: 3,
    scale: 4,
    sensitivity: 3,
    enforcement: 4,
    audit: 3,
    score: 3.4,
    code: `-- Dev environment isolation\nGRANT USE CATALOG ON CATALOG dev TO \`developers\`;\nGRANT ALL PRIVILEGES ON SCHEMA dev.sandbox TO \`developers\`;\n\n-- Block prod access\nREVOKE ALL PRIVILEGES ON CATALOG prod FROM \`developers\`;\n\nSHOW GRANTS TO \`developers\`;`,
  },
  {
    id: 6,
    group: 'Basic Access',
    title: 'Environment Segregation',
    flow: 'Dev / QA / Prod isolation',
    complexity: 3,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 4,
    score: 4.0,
    code: `-- Create separate catalogs per environment\nCREATE CATALOG IF NOT EXISTS dev;\nCREATE CATALOG IF NOT EXISTS qa;\nCREATE CATALOG IF NOT EXISTS prod;\n\n-- Dev team: dev + qa only\nGRANT USE CATALOG ON CATALOG dev TO \`dev_team\`;\nGRANT USE CATALOG ON CATALOG qa TO \`dev_team\`;\nREVOKE ALL PRIVILEGES ON CATALOG prod FROM \`dev_team\`;\n\n-- Prod team: prod + qa read\nGRANT ALL PRIVILEGES ON CATALOG prod TO \`prod_ops\`;\nGRANT USE CATALOG, SELECT ON CATALOG qa TO \`prod_ops\`;`,
  },
  {
    id: 7,
    group: 'Basic Access',
    title: 'Role-Based Access (RBAC)',
    flow: 'Roles \u2192 assign permissions',
    complexity: 3,
    scale: 5,
    sensitivity: 3,
    enforcement: 4,
    audit: 4,
    score: 3.8,
    code: `-- RBAC pattern\n-- 1. Create roles\nCREATE GROUP IF NOT EXISTS data_reader;\nCREATE GROUP IF NOT EXISTS data_writer;\nCREATE GROUP IF NOT EXISTS data_admin;\n\n-- 2. Assign permissions to roles\nGRANT SELECT ON CATALOG analytics TO \`data_reader\`;\nGRANT SELECT, MODIFY ON CATALOG analytics TO \`data_writer\`;\nGRANT ALL PRIVILEGES ON CATALOG analytics TO \`data_admin\`;\n\n-- 3. Assign users to roles\n-- (Done via SCIM/AD integration in workspace settings)`,
  },
  {
    id: 8,
    group: 'Basic Access',
    title: 'Group-Based Access',
    flow: 'AD groups mapped to UC roles',
    complexity: 3,
    scale: 5,
    sensitivity: 3,
    enforcement: 4,
    audit: 4,
    score: 3.8,
    code: `-- Map AD/Entra ID groups to Unity Catalog\n-- Step 1: SCIM provisioning syncs AD groups automatically\n-- Step 2: Grant permissions to synced groups\n\nGRANT USE CATALOG ON CATALOG analytics TO \`AD-DataEngineers\`;\nGRANT SELECT ON SCHEMA analytics.gold TO \`AD-Analysts\`;\nGRANT ALL PRIVILEGES ON CATALOG analytics TO \`AD-DataAdmins\`;\n\n-- Verify group membership\nSHOW GROUPS;`,
  },
  {
    id: 9,
    group: 'Basic Access',
    title: 'Service Account Access',
    flow: 'Pipelines use controlled identity',
    complexity: 3,
    scale: 4,
    sensitivity: 3,
    enforcement: 5,
    audit: 4,
    score: 3.8,
    code: `-- Service principal for ETL pipelines\n-- Step 1: Register service principal in workspace\n-- Step 2: Grant minimal permissions\n\nGRANT USE CATALOG ON CATALOG analytics TO \`etl-service-principal\`;\nGRANT MODIFY ON SCHEMA analytics.bronze TO \`etl-service-principal\`;\nGRANT SELECT ON SCHEMA analytics.silver TO \`etl-service-principal\`;\n\n-- No access to gold (read by BI only)\nREVOKE ALL ON SCHEMA analytics.gold FROM \`etl-service-principal\`;`,
  },
  {
    id: 10,
    group: 'Basic Access',
    title: 'Temporary Access (Just-in-Time)',
    flow: 'Time-bound permissions',
    complexity: 4,
    scale: 3,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.2,
    code: `-- Just-in-time access pattern\n-- Step 1: Grant temporary access\nGRANT SELECT ON TABLE analytics.silver.customers TO \`contractor_user\`;\n\n-- Step 2: Schedule revocation (via Databricks Jobs)\n-- notebook: revoke_temp_access\nimport time\ntime.sleep(3600 * 8)  # 8 hours\nspark.sql("REVOKE SELECT ON TABLE analytics.silver.customers FROM \`contractor_user\`")\n\n-- Step 3: Log in audit\nspark.sql("INSERT INTO audit.access_grants VALUES ('contractor_user', 'customers', 'TEMP_8H', current_timestamp())")`,
  },

  // ─── 11–20: Fine-Grained Security ───
  {
    id: 11,
    group: 'Fine-Grained Security',
    title: 'Column-Level Masking',
    flow: 'Mask PII columns',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Column masking function\nCREATE OR REPLACE FUNCTION analytics.mask_email(email STRING)\nRETURNS STRING\nRETURN CASE\n  WHEN is_member('pii_authorized') THEN email\n  ELSE CONCAT(LEFT(email, 2), '***@***.com')\nEND;\n\n-- Apply mask to table\nALTER TABLE analytics.silver.customers\n  ALTER COLUMN email SET MASK analytics.mask_email;`,
  },
  {
    id: 12,
    group: 'Fine-Grained Security',
    title: 'Row-Level Security',
    flow: 'Filter rows by user role',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Row filter function\nCREATE OR REPLACE FUNCTION analytics.region_filter(region STRING)\nRETURNS BOOLEAN\nRETURN CASE\n  WHEN is_member('global_admin') THEN TRUE\n  WHEN is_member('us_team') AND region = 'US' THEN TRUE\n  WHEN is_member('eu_team') AND region = 'EU' THEN TRUE\n  ELSE FALSE\nEND;\n\n-- Apply row filter\nALTER TABLE analytics.silver.customers\n  SET ROW FILTER analytics.region_filter ON (region);`,
  },
  {
    id: 13,
    group: 'Fine-Grained Security',
    title: 'Dynamic Data Masking',
    flow: 'Mask based on role/context',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Dynamic masking: different views per role\nCREATE OR REPLACE FUNCTION analytics.dynamic_mask(value STRING, col_type STRING)\nRETURNS STRING\nRETURN CASE\n  WHEN is_member('data_admin') THEN value\n  WHEN col_type = 'SSN' THEN CONCAT('***-**-', RIGHT(value, 4))\n  WHEN col_type = 'PHONE' THEN CONCAT('***-***-', RIGHT(value, 4))\n  WHEN col_type = 'EMAIL' THEN CONCAT(LEFT(value, 2), '***@***.com')\n  ELSE '***MASKED***'\nEND;`,
  },
  {
    id: 14,
    group: 'Fine-Grained Security',
    title: 'PII Classification Enforcement',
    flow: 'Tag + enforce policies',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Tag columns with classification\nALTER TABLE analytics.silver.customers\n  ALTER COLUMN email SET TAGS ('pii' = 'email', 'classification' = 'confidential');\nALTER TABLE analytics.silver.customers\n  ALTER COLUMN ssn SET TAGS ('pii' = 'ssn', 'classification' = 'restricted');\n\n-- Query tags\nSELECT * FROM system.information_schema.column_tags\nWHERE tag_name = 'pii';`,
  },
  {
    id: 15,
    group: 'Fine-Grained Security',
    title: 'Sensitive Data Isolation',
    flow: 'Separate secure schemas',
    complexity: 3,
    scale: 3,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.2,
    code: `-- Isolate sensitive data in secure schema\nCREATE SCHEMA IF NOT EXISTS analytics.secure;\n\n-- Move PII tables to secure schema\nALTER TABLE analytics.silver.customers RENAME TO analytics.secure.customers;\n\n-- Restrict access\nREVOKE ALL ON SCHEMA analytics.secure FROM PUBLIC;\nGRANT SELECT ON SCHEMA analytics.secure TO \`pii_authorized\`;`,
  },
  {
    id: 16,
    group: 'Fine-Grained Security',
    title: 'Financial Data Restriction',
    flow: 'Restrict finance tables',
    complexity: 4,
    scale: 3,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.4,
    code: `-- Financial data governance\nCREATE SCHEMA IF NOT EXISTS analytics.finance_secure;\n\n-- Only finance team + auditors\nGRANT SELECT ON SCHEMA analytics.finance_secure TO \`finance_team\`;\nGRANT SELECT ON SCHEMA analytics.finance_secure TO \`external_auditors\`;\n\n-- Tag for SOX compliance\nALTER TABLE analytics.finance_secure.gl_entries\n  SET TAGS ('compliance' = 'SOX', 'classification' = 'restricted');`,
  },
  {
    id: 17,
    group: 'Fine-Grained Security',
    title: 'Customer Data Masking',
    flow: 'Hide personal identifiers',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Multi-column masking for customer data\nCREATE FUNCTION analytics.mask_name(name STRING)\nRETURNS STRING\nRETURN IF(is_member('pii_authorized'), name, CONCAT(LEFT(name,1), '***'));\n\nCREATE FUNCTION analytics.mask_phone(phone STRING)\nRETURNS STRING\nRETURN IF(is_member('pii_authorized'), phone, '***-***-' || RIGHT(phone,4));\n\nALTER TABLE analytics.silver.customers ALTER COLUMN name SET MASK analytics.mask_name;\nALTER TABLE analytics.silver.customers ALTER COLUMN phone SET MASK analytics.mask_phone;`,
  },
  {
    id: 18,
    group: 'Fine-Grained Security',
    title: 'Geo-Based Data Restriction',
    flow: 'Country-specific data rules',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- GDPR: EU data stays in EU, visible only to EU team\nCREATE FUNCTION analytics.geo_filter(country STRING)\nRETURNS BOOLEAN\nRETURN CASE\n  WHEN is_member('global_admin') THEN TRUE\n  WHEN is_member('eu_team') AND country IN ('DE','FR','IT','ES','NL') THEN TRUE\n  WHEN is_member('us_team') AND country = 'US' THEN TRUE\n  ELSE FALSE\nEND;\n\nALTER TABLE analytics.silver.customers SET ROW FILTER analytics.geo_filter ON (country);`,
  },
  {
    id: 19,
    group: 'Fine-Grained Security',
    title: 'Data Anonymization Enforcement',
    flow: 'Replace PII values',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Anonymization for analytics zone\nCREATE OR REPLACE TABLE analytics.anon.customers AS\nSELECT\n  abs(hash(customer_id)) AS anon_id,\n  CASE WHEN age < 25 THEN '18-24' WHEN age < 35 THEN '25-34' WHEN age < 50 THEN '35-49' ELSE '50+' END AS age_group,\n  region, segment,\n  -- Drop all PII: name, email, phone, ssn, address\n  total_orders, lifetime_value\nFROM analytics.silver.customers;`,
  },
  {
    id: 20,
    group: 'Fine-Grained Security',
    title: 'Tokenization Policy',
    flow: 'Secure sensitive fields',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Tokenize sensitive fields (reversible only by authorized)\nCREATE FUNCTION analytics.tokenize(value STRING)\nRETURNS STRING\nRETURN sha2(concat(value, 'SECRET_SALT'), 256);\n\nCREATE FUNCTION analytics.detokenize(token STRING, original STRING)\nRETURNS STRING\nRETURN IF(is_member('detokenize_authorized') AND sha2(concat(original, 'SECRET_SALT'), 256) = token, original, '***TOKENIZED***');`,
  },

  // ─── 21–30: Lineage & Metadata ───
  {
    id: 21,
    group: 'Lineage & Metadata',
    title: 'Table-Level Lineage Tracking',
    flow: 'Source \u2192 target visibility',
    complexity: 3,
    scale: 4,
    sensitivity: 3,
    enforcement: 4,
    audit: 5,
    score: 3.8,
    code: `-- Query table lineage from system tables\nSELECT * FROM system.access.table_lineage\nWHERE target_table_full_name = 'analytics.gold.daily_revenue'\nORDER BY event_time DESC LIMIT 20;`,
  },
  {
    id: 22,
    group: 'Lineage & Metadata',
    title: 'Column-Level Lineage',
    flow: 'Field-level tracking',
    complexity: 5,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Column lineage\nSELECT * FROM system.access.column_lineage\nWHERE target_table_full_name = 'analytics.gold.daily_revenue'\n  AND target_column_name = 'total_revenue';`,
  },
  {
    id: 23,
    group: 'Lineage & Metadata',
    title: 'End-to-End Pipeline Lineage',
    flow: 'Ingestion \u2192 Gold visibility',
    complexity: 4,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Trace full lineage: bronze → silver → gold\nWITH lineage AS (\n  SELECT source_table_full_name AS source, target_table_full_name AS target\n  FROM system.access.table_lineage\n)\nSELECT * FROM lineage\nWHERE target LIKE '%.gold.%'\nORDER BY source;`,
  },
  {
    id: 24,
    group: 'Lineage & Metadata',
    title: 'Metadata Cataloging',
    flow: 'Catalog/schema/table metadata',
    complexity: 3,
    scale: 5,
    sensitivity: 3,
    enforcement: 4,
    audit: 4,
    score: 3.8,
    code: `-- Explore metadata\nSHOW CATALOGS;\nSHOW SCHEMAS IN analytics;\nSHOW TABLES IN analytics.gold;\nDESCRIBE EXTENDED analytics.gold.daily_revenue;`,
  },
  {
    id: 25,
    group: 'Lineage & Metadata',
    title: 'Data Classification Tagging',
    flow: 'Tag PII, sensitive data',
    complexity: 4,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Classify all PII columns across catalog\nALTER TABLE analytics.silver.customers ALTER COLUMN email SET TAGS ('pii'='true', 'type'='email');\nALTER TABLE analytics.silver.customers ALTER COLUMN ssn SET TAGS ('pii'='true', 'type'='ssn', 'classification'='restricted');\nALTER TABLE analytics.silver.customers ALTER COLUMN phone SET TAGS ('pii'='true', 'type'='phone');\n\n-- Find all PII columns\nSELECT * FROM system.information_schema.column_tags WHERE tag_name = 'pii';`,
  },
  {
    id: 26,
    group: 'Lineage & Metadata',
    title: 'Ownership Governance',
    flow: 'Assign data owners/stewards',
    complexity: 3,
    scale: 4,
    sensitivity: 3,
    enforcement: 4,
    audit: 4,
    score: 3.6,
    code: `-- Assign ownership\nALTER TABLE analytics.gold.daily_revenue OWNER TO \`data_product_owner\`;\nALTER SCHEMA analytics.gold OWNER TO \`gold_steward\`;\nALTER CATALOG analytics OWNER TO \`data_governance_team\`;\n\n-- Query ownership\nSELECT table_name, table_owner FROM system.information_schema.tables WHERE table_schema = 'gold';`,
  },
  {
    id: 27,
    group: 'Lineage & Metadata',
    title: 'Schema Evolution Tracking',
    flow: 'Track schema changes',
    complexity: 4,
    scale: 4,
    sensitivity: 3,
    enforcement: 4,
    audit: 5,
    score: 4.0,
    code: `-- Track schema changes via Delta history\nDESCRIBE HISTORY analytics.silver.customers;\n\n-- Compare schemas across versions\nSELECT * FROM analytics.silver.customers VERSION AS OF 5;\nSELECT * FROM analytics.silver.customers VERSION AS OF 10;`,
  },
  {
    id: 28,
    group: 'Lineage & Metadata',
    title: 'Data Dictionary Enforcement',
    flow: 'Business metadata definitions',
    complexity: 4,
    scale: 4,
    sensitivity: 3,
    enforcement: 4,
    audit: 4,
    score: 3.8,
    code: `-- Add comments as data dictionary\nCOMMENT ON TABLE analytics.gold.daily_revenue IS 'Daily revenue aggregation from silver orders. Refreshed at 6 AM UTC.';\nCOMMENT ON COLUMN analytics.gold.daily_revenue.total_revenue IS 'Sum of order amounts for the day in USD';`,
  },
  {
    id: 29,
    group: 'Lineage & Metadata',
    title: 'Dataset Certification',
    flow: 'Trusted vs non-trusted data',
    complexity: 4,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.4,
    code: `-- Tag tables as certified/uncertified\nALTER TABLE analytics.gold.daily_revenue SET TAGS ('certified'='true', 'certification_date'='2024-04-15', 'certified_by'='data_quality_team');\nALTER TABLE analytics.bronze.raw_data SET TAGS ('certified'='false', 'reason'='raw_unvalidated');\n\n-- Query certified datasets only\nSELECT * FROM system.information_schema.table_tags WHERE tag_name = 'certified' AND tag_value = 'true';`,
  },
  {
    id: 30,
    group: 'Lineage & Metadata',
    title: 'Impact Analysis',
    flow: 'Downstream impact of change',
    complexity: 5,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Before changing silver.customers, check what depends on it\nSELECT DISTINCT target_table_full_name\nFROM system.access.table_lineage\nWHERE source_table_full_name = 'analytics.silver.customers';\n\n-- Recursive downstream impact\nWITH RECURSIVE downstream AS (\n  SELECT target_table_full_name AS tbl FROM system.access.table_lineage WHERE source_table_full_name = 'analytics.silver.customers'\n  UNION ALL\n  SELECT l.target_table_full_name FROM system.access.table_lineage l JOIN downstream d ON l.source_table_full_name = d.tbl\n)\nSELECT DISTINCT tbl FROM downstream;`,
  },

  // ─── 31–40: Audit, Compliance & Monitoring ───
  {
    id: 31,
    group: 'Audit & Compliance',
    title: 'Access Audit Logging',
    flow: 'Track who accessed data',
    complexity: 3,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Query audit logs\nSELECT event_time, user_identity.email, action_name, request_params.full_name_arg AS table_name\nFROM system.access.audit\nWHERE action_name IN ('getTable', 'commandSubmit')\n  AND event_date >= current_date() - 7\nORDER BY event_time DESC;`,
  },
  {
    id: 32,
    group: 'Audit & Compliance',
    title: 'Query Audit Tracking',
    flow: 'Track query usage',
    complexity: 3,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.4,
    code: `-- Track all queries against sensitive tables\nSELECT event_time, user_identity.email, request_params.commandText AS query\nFROM system.access.audit\nWHERE action_name = 'commandSubmit'\n  AND request_params.commandText LIKE '%customers%'\nORDER BY event_time DESC LIMIT 50;`,
  },
  {
    id: 33,
    group: 'Audit & Compliance',
    title: 'Data Access Monitoring',
    flow: 'Monitor usage patterns',
    complexity: 4,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Anomaly detection: unusual access patterns\nSELECT user_identity.email, COUNT(*) AS access_count, COUNT(DISTINCT request_params.full_name_arg) AS tables_accessed\nFROM system.access.audit\nWHERE event_date = current_date()\n  AND action_name = 'getTable'\nGROUP BY user_identity.email\nHAVING access_count > 100  -- Flag unusual volume\nORDER BY access_count DESC;`,
  },
  {
    id: 34,
    group: 'Audit & Compliance',
    title: 'SLA Monitoring',
    flow: 'Track pipeline/data SLA',
    complexity: 3,
    scale: 4,
    sensitivity: 3,
    enforcement: 4,
    audit: 4,
    score: 3.6,
    code: `-- SLA: data must be fresh within 6 hours\nSELECT table_name,\n  MAX(event_time) AS last_write,\n  TIMESTAMPDIFF(HOUR, MAX(event_time), current_timestamp()) AS hours_since_update,\n  CASE WHEN TIMESTAMPDIFF(HOUR, MAX(event_time), current_timestamp()) > 6 THEN 'SLA_BREACH' ELSE 'OK' END AS sla_status\nFROM system.access.audit\nWHERE action_name = 'commitInfo'\nGROUP BY table_name;`,
  },
  {
    id: 35,
    group: 'Audit & Compliance',
    title: 'Regulatory Audit Pipeline',
    flow: 'Track compliance evidence',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Generate compliance evidence report\nCREATE OR REPLACE TABLE analytics.compliance.audit_evidence AS\nSELECT\n  event_date, user_identity.email AS user_email,\n  action_name, request_params.full_name_arg AS resource,\n  response.status_code, source_ip_address\nFROM system.access.audit\nWHERE event_date BETWEEN '2024-01-01' AND '2024-03-31'\n  AND action_name IN ('getTable', 'commandSubmit', 'createTable', 'deleteTable')\nORDER BY event_time;`,
  },
  {
    id: 36,
    group: 'Audit & Compliance',
    title: 'GDPR Compliance',
    flow: 'Right to access/delete data',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- GDPR Right to Erasure\n-- Step 1: Find all tables containing user data\nSELECT table_catalog, table_schema, table_name, column_name\nFROM system.information_schema.column_tags\nWHERE tag_name = 'pii';\n\n-- Step 2: Delete user data across all tables\nDELETE FROM analytics.silver.customers WHERE email = 'user@request.com';\nDELETE FROM analytics.silver.orders WHERE customer_email = 'user@request.com';\n\n-- Step 3: Vacuum to remove from storage\nVACUUM analytics.silver.customers RETAIN 0 HOURS;  -- Requires override`,
  },
  {
    id: 37,
    group: 'Audit & Compliance',
    title: 'SOX Compliance',
    flow: 'Financial control enforcement',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- SOX controls for financial data\n-- 1. Segregation of duties\nREVOKE MODIFY ON SCHEMA analytics.finance FROM \`finance_analysts\`;  -- read only\nGRANT MODIFY ON SCHEMA analytics.finance TO \`finance_controllers\`;  -- write\n\n-- 2. Change tracking\nDESCRIBE HISTORY analytics.finance.gl_entries;\n\n-- 3. Audit trail\nSELECT * FROM system.access.audit\nWHERE request_params.full_name_arg LIKE '%finance%'\nAND event_date >= '2024-01-01';`,
  },
  {
    id: 38,
    group: 'Audit & Compliance',
    title: 'HIPAA Compliance',
    flow: 'Healthcare data protection',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- HIPAA: Protected Health Information (PHI)\n-- 1. Isolate PHI in secure schema\nCREATE SCHEMA IF NOT EXISTS healthcare.phi_secure;\n\n-- 2. Encrypt at rest (Delta default) + column masking\nALTER TABLE healthcare.phi_secure.patient_records ALTER COLUMN ssn SET MASK healthcare.mask_ssn;\nALTER TABLE healthcare.phi_secure.patient_records ALTER COLUMN diagnosis SET MASK healthcare.mask_diagnosis;\n\n-- 3. Row filter: only treating physician sees patient\nALTER TABLE healthcare.phi_secure.patient_records SET ROW FILTER healthcare.physician_filter ON (treating_physician);`,
  },
  {
    id: 39,
    group: 'Audit & Compliance',
    title: 'Data Retention Policy',
    flow: 'Lifecycle enforcement',
    complexity: 4,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Auto-purge data older than retention period\n-- Bronze: 90 days, Silver: 1 year, Gold: 3 years\n\n-- Bronze cleanup\nDELETE FROM analytics.bronze.raw_events WHERE _ingest_ts < current_date() - 90;\nVACUUM analytics.bronze.raw_events RETAIN 168 HOURS;\n\n-- Log retention actions\nINSERT INTO analytics.audit.retention_log\nVALUES ('bronze.raw_events', 'PURGE_90D', current_timestamp(), 'AUTO');`,
  },
  {
    id: 40,
    group: 'Audit & Compliance',
    title: 'Data Deletion Policy',
    flow: 'Purge sensitive data',
    complexity: 4,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Scheduled deletion of expired PII\nDELETE FROM analytics.silver.customers\nWHERE account_status = 'CLOSED'\n  AND account_closed_date < current_date() - 365;\n\n-- Vacuum to physically remove\nVACUUM analytics.silver.customers RETAIN 168 HOURS;\n\n-- Audit log\nINSERT INTO analytics.audit.deletion_log\nSELECT 'customers', COUNT(*), 'PII_PURGE', current_timestamp()\nFROM analytics.silver.customers WHERE account_status = 'CLOSED';`,
  },

  // ─── 41–50: Advanced / AI / Enterprise ───
  {
    id: 41,
    group: 'Advanced/AI/Enterprise',
    title: 'Multi-Catalog Governance',
    flow: 'Domain-based catalogs',
    complexity: 4,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Domain-driven catalog structure\nCREATE CATALOG IF NOT EXISTS finance;\nCREATE CATALOG IF NOT EXISTS marketing;\nCREATE CATALOG IF NOT EXISTS engineering;\n\n-- Domain owners\nALTER CATALOG finance OWNER TO \`finance_data_owner\`;\nALTER CATALOG marketing OWNER TO \`marketing_data_owner\`;\n\n-- Cross-domain read access\nGRANT USE CATALOG, SELECT ON CATALOG finance TO \`cross_domain_readers\`;`,
  },
  {
    id: 42,
    group: 'Advanced/AI/Enterprise',
    title: 'Cross-Workspace Governance',
    flow: 'Central UC across envs',
    complexity: 5,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Centralized Unity Catalog metastore shared across workspaces\n-- Workspace 1 (Dev): access to dev catalog\n-- Workspace 2 (Prod): access to prod catalog\n-- Both share same metastore for unified governance\n\n-- In each workspace:\nSHOW CATALOGS;  -- Shows only catalogs bound to this workspace\nSHOW GRANTS TO CURRENT_USER;`,
  },
  {
    id: 43,
    group: 'Advanced/AI/Enterprise',
    title: 'Multi-Cloud Governance',
    flow: 'AWS + Azure + GCP UC',
    complexity: 5,
    scale: 5,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Unity Catalog across clouds\n-- Storage credentials per cloud\nCREATE STORAGE CREDENTIAL aws_cred COMMENT 'AWS S3 access' ...\nCREATE STORAGE CREDENTIAL azure_cred COMMENT 'ADLS access' ...\nCREATE STORAGE CREDENTIAL gcp_cred COMMENT 'GCS access' ...\n\n-- External locations per cloud\nCREATE EXTERNAL LOCATION aws_data URL 's3://bucket/' WITH (STORAGE CREDENTIAL aws_cred);\nCREATE EXTERNAL LOCATION azure_data URL 'abfss://container@storage/' WITH (STORAGE CREDENTIAL azure_cred);`,
  },
  {
    id: 44,
    group: 'Advanced/AI/Enterprise',
    title: 'AI Model Governance',
    flow: 'Control model/data access',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Govern MLflow models via Unity Catalog\n-- Register model\nimport mlflow\nmlflow.set_registry_uri("databricks-uc")\nmlflow.register_model("runs:/abc123/model", "analytics.ml.churn_model")\n\n-- Control access\nGRANT USE SCHEMA ON SCHEMA analytics.ml TO \`ml_engineers\`;\nGRANT EXECUTE ON FUNCTION analytics.ml.churn_model TO \`ml_engineers\`;\n\n-- Audit model usage\nSELECT * FROM system.access.audit WHERE request_params.full_name_arg LIKE '%churn_model%';`,
  },
  {
    id: 45,
    group: 'Advanced/AI/Enterprise',
    title: 'Feature Store Governance',
    flow: 'Govern ML features',
    complexity: 4,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.4,
    code: `-- Feature table governance\nGRANT SELECT ON TABLE analytics.ml.customer_features TO \`ml_engineers\`;\nGRANT MODIFY ON TABLE analytics.ml.customer_features TO \`feature_pipeline_sp\`;\n\n-- Tag features\nALTER TABLE analytics.ml.customer_features ALTER COLUMN churn_score SET TAGS ('feature_type'='target', 'model'='churn_v2');`,
  },
  {
    id: 46,
    group: 'Advanced/AI/Enterprise',
    title: 'RAG Data Governance',
    flow: 'Control vector DB + docs',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Govern RAG pipeline data\n-- 1. Source documents: restricted access\nGRANT SELECT ON TABLE analytics.bronze.documents TO \`rag_pipeline_sp\`;\n\n-- 2. Chunks: ML team access\nGRANT SELECT ON TABLE analytics.ml.doc_chunks TO \`ml_team\`;\n\n-- 3. Embeddings: controlled via Vector Search endpoint\n-- Endpoint ACLs managed separately\n\n-- 4. Audit RAG queries\nSELECT * FROM system.access.audit WHERE action_name = 'vectorSearchQuery';`,
  },
  {
    id: 47,
    group: 'Advanced/AI/Enterprise',
    title: 'Data Sharing Governance',
    flow: 'Delta Sharing policies',
    complexity: 5,
    scale: 4,
    sensitivity: 4,
    enforcement: 5,
    audit: 5,
    score: 4.6,
    code: `-- Delta Sharing with governance\nCREATE SHARE IF NOT EXISTS partner_share;\n\n-- Add tables with filters\nALTER SHARE partner_share ADD TABLE analytics.gold.public_metrics;\nALTER SHARE partner_share ADD TABLE analytics.gold.product_catalog;\n\n-- Create recipient\nCREATE RECIPIENT partner_corp COMMENT 'External partner';\n\n-- Grant share to recipient\nGRANT SELECT ON SHARE partner_share TO RECIPIENT partner_corp;`,
  },
  {
    id: 48,
    group: 'Advanced/AI/Enterprise',
    title: 'External Partner Access',
    flow: 'Controlled data sharing',
    complexity: 5,
    scale: 4,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 4.8,
    code: `-- Secure external partner access\n-- 1. Create isolated catalog for partner\nCREATE CATALOG IF NOT EXISTS partner_data;\n\n-- 2. Create curated views (no raw data)\nCREATE VIEW partner_data.shared.orders_summary AS\nSELECT order_date, product_category, SUM(amount) AS total\nFROM analytics.gold.fact_sales\nGROUP BY order_date, product_category;\n\n-- 3. Grant read-only to partner identity\nGRANT USE CATALOG ON CATALOG partner_data TO \`partner_service_principal\`;\nGRANT SELECT ON SCHEMA partner_data.shared TO \`partner_service_principal\`;`,
  },
  {
    id: 49,
    group: 'Advanced/AI/Enterprise',
    title: 'DR Governance',
    flow: 'Governance across regions',
    complexity: 5,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 5.0,
    code: `-- Disaster Recovery: replicate governance\n-- 1. Clone tables to DR region\nCREATE OR REPLACE TABLE dr_catalog.gold.daily_revenue DEEP CLONE analytics.gold.daily_revenue;\n\n-- 2. Replicate permissions\n-- (Scripted: export grants from primary, apply to DR)\nSHOW GRANTS ON TABLE analytics.gold.daily_revenue;\n-- Apply same grants to DR\nGRANT SELECT ON TABLE dr_catalog.gold.daily_revenue TO \`bi_team\`;`,
  },
  {
    id: 50,
    group: 'Advanced/AI/Enterprise',
    title: 'Enterprise Governance Framework',
    flow: 'Full lifecycle governance',
    complexity: 5,
    scale: 5,
    sensitivity: 5,
    enforcement: 5,
    audit: 5,
    score: 5.0,
    code: `-- Enterprise governance: full framework\n-- 1. Catalog structure: domain-based\n-- 2. Access: RBAC via AD groups + SCIM\n-- 3. Security: column masks + row filters on PII\n-- 4. Classification: tags on all sensitive columns\n-- 5. Lineage: automatic via Unity Catalog\n-- 6. Audit: system.access.audit for all actions\n-- 7. Compliance: GDPR/SOX/HIPAA policies\n-- 8. Sharing: Delta Sharing for partners\n-- 9. AI: model + feature governance\n-- 10. DR: cross-region replication\n\n-- Health check\nSELECT 'catalogs' AS check, COUNT(*) AS count FROM system.information_schema.catalogs\nUNION ALL SELECT 'tables', COUNT(*) FROM system.information_schema.tables\nUNION ALL SELECT 'tagged_columns', COUNT(*) FROM system.information_schema.column_tags\nUNION ALL SELECT 'grants', COUNT(*) FROM system.information_schema.table_privileges;`,
  },
];

const groups = [...new Set(govScenarios.map((s) => s.group))];

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

function GovernanceScenarios() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('id');

  const filtered = govScenarios
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
      govScenarios.map((s) => ({
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
      'governance-scorecard.csv'
    );
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Unity Catalog Governance Scenarios</h1>
          <p>
            50 governance patterns — Access Control, Security, Lineage, Audit, Compliance, AI
            Governance
          </p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">{'\ud83d\udee1\ufe0f'}</div>
          <div className="stat-info">
            <h4>50</h4>
            <p>Scenarios</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">{'\ud83c\udfc2'}</div>
          <div className="stat-info">
            <h4>{groups.length}</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">{'\u2b50'}</div>
          <div className="stat-info">
            <h4>{(govScenarios.reduce((s, x) => s + x.score, 0) / 50).toFixed(1)}</h4>
            <p>Avg Score</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">{'\ud83c\udfc6'}</div>
          <div className="stat-info">
            <h4>{govScenarios.filter((s) => s.score >= 4.5).length}</h4>
            <p>Critical (4.5+)</p>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search governance scenarios..."
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
                {g} ({govScenarios.filter((s) => s.group === g).length})
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
                    #{s.id} \u2014 {s.title}
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
                    Governance KPI
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

export default GovernanceScenarios;
