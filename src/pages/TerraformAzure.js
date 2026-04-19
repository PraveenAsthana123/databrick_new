import React, { useState } from 'react';
import FileFormatRunner from '../components/common/FileFormatRunner';

const terraformScenarios = [
  {
    id: 1,
    title: 'Provider Setup & Authentication',
    desc: 'Configure the Databricks Terraform provider with Azure authentication using service principal or CLI credentials',
    code: `# providers.tf
terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90.0"
    }
  }
  backend "azurerm" {
    resource_group_name  = "terraform-state-rg"
    storage_account_name = "tfstatedatabricks"
    container_name       = "tfstate"
    key                  = "databricks.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
}

provider "databricks" {
  host                        = azurerm_databricks_workspace.this.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}

variable "subscription_id" { type = string }
variable "tenant_id"       { type = string }
variable "environment"     { type = string, default = "dev" }`,
  },
  {
    id: 2,
    title: 'Workspace Deployment',
    desc: 'Deploy a Databricks workspace with VNet injection, managed resource group, and custom tags',
    code: `# workspace.tf
resource "azurerm_resource_group" "databricks" {
  name     = "rg-databricks-\${var.environment}"
  location = var.location
  tags     = local.common_tags
}

resource "azurerm_databricks_workspace" "this" {
  name                        = "dbw-\${var.project}-\${var.environment}"
  resource_group_name         = azurerm_resource_group.databricks.name
  location                    = azurerm_resource_group.databricks.location
  sku                         = "premium"
  managed_resource_group_name = "rg-databricks-managed-\${var.environment}"

  custom_parameters {
    no_public_ip                                         = true
    virtual_network_id                                   = azurerm_virtual_network.this.id
    public_subnet_name                                   = azurerm_subnet.public.name
    private_subnet_name                                  = azurerm_subnet.private.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
  }

  tags = merge(local.common_tags, {
    "databricks-environment" = var.environment
  })
}

output "workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}`,
  },
  {
    id: 3,
    title: 'Cluster Configuration',
    desc: 'Create all-purpose and job clusters with autoscaling, init scripts, and Spark configuration',
    code: `# clusters.tf
data "databricks_node_type" "smallest" {
  local_disk = true
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_cluster" "shared_analytics" {
  cluster_name            = "shared-analytics-\${var.environment}"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 30
  data_security_mode      = "USER_ISOLATION"

  autoscale {
    min_workers = 1
    max_workers = 8
  }

  spark_conf = {
    "spark.databricks.delta.preview.enabled"        = "true"
    "spark.sql.shuffle.partitions"                   = "auto"
    "spark.databricks.io.cache.enabled"              = "true"
    "spark.speculation"                               = "false"
  }

  spark_env_vars = {
    "ENVIRONMENT" = var.environment
    "PYSPARK_PYTHON" = "/databricks/python3/bin/python3"
  }

  custom_tags = {
    "Team"        = "data-engineering"
    "CostCenter"  = var.cost_center
  }

  library {
    pypi {
      package = "delta-spark==3.1.0"
    }
  }

  depends_on = [azurerm_databricks_workspace.this]
}`,
  },
  {
    id: 4,
    title: 'Job Scheduling',
    desc: 'Define multi-task workflows with dependencies, retries, notifications, and schedules',
    code: `# jobs.tf
resource "databricks_job" "etl_pipeline" {
  name = "etl-pipeline-\${var.environment}"

  job_cluster {
    job_cluster_key = "etl_cluster"
    new_cluster {
      spark_version = data.databricks_spark_version.latest_lts.id
      node_type_id  = "Standard_DS3_v2"
      num_workers   = 4
      spark_conf = {
        "spark.sql.adaptive.enabled" = "true"
      }
    }
  }

  task {
    task_key = "extract"
    notebook_task {
      notebook_path = "/Repos/production/etl/extract"
      base_parameters = {
        "source_date" = "{{job.start_date}}"
        "environment" = var.environment
      }
    }
    job_cluster_key = "etl_cluster"
    max_retries     = 2
    min_retry_interval_millis = 60000
  }

  task {
    task_key = "transform"
    depends_on {
      task_key = "extract"
    }
    notebook_task {
      notebook_path = "/Repos/production/etl/transform"
    }
    job_cluster_key = "etl_cluster"
  }

  task {
    task_key = "load"
    depends_on {
      task_key = "transform"
    }
    notebook_task {
      notebook_path = "/Repos/production/etl/load"
    }
    job_cluster_key = "etl_cluster"
  }

  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "America/New_York"
  }

  email_notifications {
    on_failure = ["data-team@company.com"]
    on_success = ["data-team@company.com"]
  }

  tags = {
    "pipeline" = "etl"
    "owner"    = "data-engineering"
  }
}`,
  },
  {
    id: 5,
    title: 'Notebook Deployment',
    desc: 'Deploy notebooks from source, configure Repos integration, and manage workspace objects',
    code: `# notebooks.tf
resource "databricks_notebook" "etl_extract" {
  path     = "/Shared/etl/extract"
  language = "PYTHON"
  source   = "\${path.module}/notebooks/extract.py"
}

resource "databricks_notebook" "etl_transform" {
  path     = "/Shared/etl/transform"
  language = "PYTHON"
  source   = "\${path.module}/notebooks/transform.py"
}

# Git-based Repos integration
resource "databricks_repo" "production" {
  url          = "https://dev.azure.com/org/project/_git/databricks-notebooks"
  path         = "/Repos/production"
  branch       = var.environment == "prod" ? "main" : "develop"
  provider     = databricks
}

resource "databricks_repo" "staging" {
  url    = "https://dev.azure.com/org/project/_git/databricks-notebooks"
  path   = "/Repos/staging"
  branch = "develop"
}

# Workspace directory structure
resource "databricks_directory" "shared_etl" {
  path = "/Shared/etl"
}

resource "databricks_directory" "shared_utils" {
  path = "/Shared/utils"
}`,
  },
  {
    id: 6,
    title: 'Unity Catalog Setup',
    desc: 'Create metastore, catalogs, schemas, and manage external locations with storage credentials',
    code: `# unity_catalog.tf
resource "databricks_metastore" "this" {
  name          = "primary-metastore"
  storage_root  = "abfss://metastore@\${azurerm_storage_account.unity.name}.dfs.core.windows.net/"
  region        = var.location
  force_destroy = false
}

resource "databricks_metastore_assignment" "this" {
  metastore_id = databricks_metastore.this.id
  workspace_id = azurerm_databricks_workspace.this.workspace_id
}

resource "databricks_storage_credential" "external" {
  name = "azure-storage-cred"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }
}

resource "databricks_external_location" "data_lake" {
  name            = "data-lake-external"
  url             = "abfss://datalake@\${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external.name
}

resource "databricks_catalog" "analytics" {
  name    = "\${var.environment}_analytics"
  comment = "Analytics catalog for \${var.environment}"
  properties = {
    "purpose" = "analytics"
  }
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.analytics.name
  name         = "bronze"
  comment      = "Raw ingested data"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.analytics.name
  name         = "silver"
  comment      = "Cleaned and validated data"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.analytics.name
  name         = "gold"
  comment      = "Business-level aggregates"
}`,
  },
  {
    id: 7,
    title: 'Secret Scope Management',
    desc: 'Create Databricks-backed and Azure Key Vault-backed secret scopes with ACLs',
    code: `# secrets.tf

# Azure Key Vault-backed secret scope (recommended for production)
resource "databricks_secret_scope" "kv_backed" {
  name = "keyvault-secrets"

  keyvault_metadata {
    resource_id = azurerm_key_vault.databricks.id
    dns_name    = azurerm_key_vault.databricks.vault_uri
  }
}

# Databricks-backed secret scope (for non-Azure secrets)
resource "databricks_secret_scope" "app_secrets" {
  name = "app-secrets-\${var.environment}"
}

resource "databricks_secret" "db_password" {
  key          = "database-password"
  string_value = var.database_password
  scope        = databricks_secret_scope.app_secrets.name
}

resource "databricks_secret" "api_key" {
  key          = "external-api-key"
  string_value = var.external_api_key
  scope        = databricks_secret_scope.app_secrets.name
}

# ACLs for secret scope access
resource "databricks_secret_acl" "data_engineers" {
  principal  = "data-engineers"
  permission = "READ"
  scope      = databricks_secret_scope.app_secrets.name
}

resource "databricks_secret_acl" "admins" {
  principal  = "admins"
  permission = "MANAGE"
  scope      = databricks_secret_scope.app_secrets.name
}`,
  },
  {
    id: 8,
    title: 'Instance Pool Configuration',
    desc: 'Create instance pools to reduce cluster start times and optimize costs with preloaded Docker images',
    code: `# instance_pools.tf
resource "databricks_instance_pool" "general_purpose" {
  instance_pool_name = "general-purpose-\${var.environment}"
  node_type_id       = "Standard_DS3_v2"

  min_idle_instances                  = var.environment == "prod" ? 2 : 0
  max_capacity                        = 20
  idle_instance_autotermination_minutes = 15

  preloaded_spark_versions = [
    data.databricks_spark_version.latest_lts.id
  ]

  azure_attributes {
    availability       = "ON_DEMAND_AZURE"
    spot_bid_max_price = -1
  }

  custom_tags = {
    "Team"        = "data-engineering"
    "Environment" = var.environment
  }
}

resource "databricks_instance_pool" "high_memory" {
  instance_pool_name = "high-memory-\${var.environment}"
  node_type_id       = "Standard_E8ds_v4"

  min_idle_instances                  = 0
  max_capacity                        = 10
  idle_instance_autotermination_minutes = 10

  preloaded_spark_versions = [
    data.databricks_spark_version.latest_lts.id
  ]
}

# Use pool in cluster definition
resource "databricks_cluster" "pooled_cluster" {
  cluster_name            = "pooled-analytics-\${var.environment}"
  spark_version           = data.databricks_spark_version.latest_lts.id
  instance_pool_id        = databricks_instance_pool.general_purpose.id
  driver_instance_pool_id = databricks_instance_pool.general_purpose.id
  autotermination_minutes = 20
  num_workers             = 4
}`,
  },
  {
    id: 9,
    title: 'SQL Warehouse Provisioning',
    desc: 'Create serverless and classic SQL warehouses with channel, sizing, and permissions',
    code: `# sql_warehouses.tf
resource "databricks_sql_endpoint" "serverless" {
  name             = "serverless-wh-\${var.environment}"
  cluster_size     = "Small"
  max_num_clusters = 3
  min_num_clusters = 1
  auto_stop_mins   = 15

  enable_serverless_compute = true

  warehouse_type = "PRO"
  channel {
    name = "CHANNEL_NAME_CURRENT"
  }

  tags {
    custom_tags {
      key   = "Environment"
      value = var.environment
    }
    custom_tags {
      key   = "CostCenter"
      value = var.cost_center
    }
  }
}

resource "databricks_sql_endpoint" "classic" {
  name             = "classic-wh-\${var.environment}"
  cluster_size     = "Medium"
  max_num_clusters = 5
  min_num_clusters = 1
  auto_stop_mins   = 30

  enable_serverless_compute = false

  spot_instance_policy = "COST_OPTIMIZED"
}

# Grant access to SQL warehouse
resource "databricks_permissions" "sql_warehouse" {
  sql_endpoint_id = databricks_sql_endpoint.serverless.id

  access_control {
    group_name       = "data-analysts"
    permission_level = "CAN_USE"
  }

  access_control {
    group_name       = "data-engineers"
    permission_level = "CAN_MANAGE"
  }
}`,
  },
  {
    id: 10,
    title: 'Permissions & Access Control',
    desc: 'Manage workspace permissions, group assignments, cluster policies, and Unity Catalog grants',
    code: `# permissions.tf

# Groups
resource "databricks_group" "data_engineers" {
  display_name = "data-engineers"
}

resource "databricks_group" "data_analysts" {
  display_name = "data-analysts"
}

# SCIM user provisioning from Azure AD
resource "databricks_user" "engineer" {
  for_each  = toset(var.engineer_emails)
  user_name = each.value
}

resource "databricks_group_member" "engineers" {
  for_each  = databricks_user.engineer
  group_id  = databricks_group.data_engineers.id
  member_id = each.value.id
}

# Cluster policy (restrict instance types, max workers)
resource "databricks_cluster_policy" "fair_use" {
  name = "fair-use-policy"
  definition = jsonencode({
    "node_type_id" : {
      "type" : "allowlist",
      "values" : ["Standard_DS3_v2", "Standard_DS4_v2", "Standard_E4ds_v4"]
    },
    "autoscale.max_workers" : {
      "type" : "range",
      "maxValue" : 10
    },
    "autotermination_minutes" : {
      "type" : "range",
      "minValue" : 10,
      "maxValue" : 60,
      "defaultValue" : 20
    },
    "custom_tags.Team" : {
      "type" : "fixed",
      "value" : "restricted"
    }
  })
}

# Unity Catalog grants
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.analytics.name
  grant {
    principal  = "data-engineers"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  grant {
    principal  = "data-analysts"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "schema_gold" {
  schema = "\${databricks_catalog.analytics.name}.\${databricks_schema.gold.name}"
  grant {
    principal  = "data-analysts"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}`,
  },
];

const azureScenarios = [
  {
    id: 1,
    title: 'ADLS Gen2 Setup with Terraform',
    desc: 'Provision Azure Data Lake Storage Gen2 with hierarchical namespace, lifecycle policies, and Databricks access connector',
    code: `# adls_gen2.tf
resource "azurerm_storage_account" "datalake" {
  name                     = "stadatalake\${var.environment}"
  resource_group_name      = azurerm_resource_group.databricks.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Enables ADLS Gen2

  blob_properties {
    delete_retention_policy {
      days = 30
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  network_rules {
    default_action             = "Deny"
    bypass                     = ["AzureServices"]
    virtual_network_subnet_ids = [azurerm_subnet.private.id]
    ip_rules                   = var.allowed_ips
  }

  tags = local.common_tags
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# Access Connector for Unity Catalog
resource "azurerm_databricks_access_connector" "this" {
  name                = "dbac-\${var.environment}"
  resource_group_name = azurerm_resource_group.databricks.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "connector_storage" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}`,
  },
  {
    id: 2,
    title: 'Azure Key Vault Integration',
    desc: 'Create Key Vault with access policies for Databricks, store connection strings, and enable soft delete',
    code: `# key_vault.tf
data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "databricks" {
  name                        = "kv-dbricks-\${var.environment}"
  location                    = var.location
  resource_group_name         = azurerm_resource_group.databricks.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"
  soft_delete_retention_days  = 90
  purge_protection_enabled    = true
  enable_rbac_authorization   = false

  network_acls {
    default_action             = "Deny"
    bypass                     = "AzureServices"
    virtual_network_subnet_ids = [azurerm_subnet.private.id]
    ip_rules                   = var.allowed_ips
  }

  tags = local.common_tags
}

# Access policy for Databricks service principal
resource "azurerm_key_vault_access_policy" "databricks_sp" {
  key_vault_id = azurerm_key_vault.databricks.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azuread_service_principal.databricks.object_id

  secret_permissions = ["Get", "List"]
  key_permissions    = ["Get", "List", "WrapKey", "UnwrapKey"]
}

# Store secrets
resource "azurerm_key_vault_secret" "db_connection" {
  name         = "sql-connection-string"
  value        = "Server=tcp:\${azurerm_mssql_server.this.fqdn};Database=analytics;Authentication=Active Directory Integrated;"
  key_vault_id = azurerm_key_vault.databricks.id

  content_type    = "text/plain"
  expiration_date = "2026-12-31T00:00:00Z"
}

resource "azurerm_key_vault_secret" "storage_key" {
  name         = "adls-access-key"
  value        = azurerm_storage_account.datalake.primary_access_key
  key_vault_id = azurerm_key_vault.databricks.id
}`,
  },
  {
    id: 3,
    title: 'Service Principal & RBAC',
    desc: 'Create Azure AD service principal for Databricks automation with least-privilege role assignments',
    code: `# service_principal.tf
resource "azuread_application" "databricks_automation" {
  display_name = "sp-databricks-\${var.environment}"
  owners       = [data.azurerm_client_config.current.object_id]
}

resource "azuread_service_principal" "databricks" {
  client_id = azuread_application.databricks_automation.client_id
  owners    = [data.azurerm_client_config.current.object_id]
}

resource "azuread_service_principal_password" "databricks" {
  service_principal_id = azuread_service_principal.databricks.id
  end_date_relative    = "8760h" # 1 year
}

# Role Assignments - Least Privilege
resource "azurerm_role_assignment" "contributor_workspace" {
  scope                = azurerm_databricks_workspace.this.id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.databricks.object_id
}

resource "azurerm_role_assignment" "blob_contributor" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.databricks.object_id
}

resource "azurerm_role_assignment" "reader_rg" {
  scope                = azurerm_resource_group.databricks.id
  role_definition_name = "Reader"
  principal_id         = azuread_service_principal.databricks.object_id
}

# Register SP in Databricks workspace
resource "databricks_service_principal" "automation" {
  application_id = azuread_application.databricks_automation.client_id
  display_name   = "automation-sp-\${var.environment}"
}

# Store credentials in Key Vault
resource "azurerm_key_vault_secret" "sp_client_id" {
  name         = "databricks-sp-client-id"
  value        = azuread_application.databricks_automation.client_id
  key_vault_id = azurerm_key_vault.databricks.id
}

resource "azurerm_key_vault_secret" "sp_client_secret" {
  name         = "databricks-sp-client-secret"
  value        = azuread_service_principal_password.databricks.value
  key_vault_id = azurerm_key_vault.databricks.id
}`,
  },
  {
    id: 4,
    title: 'Azure Event Hubs Streaming',
    desc: 'Set up Event Hubs namespace with Kafka-enabled hub and consume events in Databricks Structured Streaming',
    code: `# event_hubs.tf
resource "azurerm_eventhub_namespace" "streaming" {
  name                = "ehns-databricks-\${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.databricks.name
  sku                 = "Standard"
  capacity            = 2

  auto_inflate_enabled     = true
  maximum_throughput_units = 10

  network_rulesets {
    default_action                 = "Deny"
    trusted_service_access_enabled = true
    virtual_network_rule {
      subnet_id = azurerm_subnet.private.id
    }
  }

  tags = local.common_tags
}

resource "azurerm_eventhub" "clickstream" {
  name                = "clickstream-events"
  namespace_name      = azurerm_eventhub_namespace.streaming.name
  resource_group_name = azurerm_resource_group.databricks.name
  partition_count     = 8
  message_retention   = 7
}

resource "azurerm_eventhub_consumer_group" "databricks" {
  name                = "databricks-consumer"
  namespace_name      = azurerm_eventhub_namespace.streaming.name
  eventhub_name       = azurerm_eventhub.clickstream.name
  resource_group_name = azurerm_resource_group.databricks.name
}

# --- Databricks notebook code to consume Event Hubs ---
# connection_string = dbutils.secrets.get("keyvault-secrets", "eventhub-conn-string")
# ehConf = {
#   "eventhubs.connectionString":
#       sc._jvm.org.apache.spark.eventhubs.EventHubsUtils
#         .encrypt(connection_string),
#   "eventhubs.consumerGroup": "databricks-consumer",
#   "maxEventsPerTrigger": 10000
# }
#
# df = (spark.readStream
#   .format("eventhubs")
#   .options(**ehConf)
#   .load()
#   .withColumn("body", col("body").cast("string"))
#   .withColumn("parsed", from_json(col("body"), event_schema))
# )
#
# (df.writeStream
#   .format("delta")
#   .outputMode("append")
#   .option("checkpointLocation", "/mnt/checkpoints/clickstream")
#   .toTable("bronze.clickstream_events")
# )`,
  },
  {
    id: 5,
    title: 'Azure Synapse Integration',
    desc: 'Connect Databricks to Synapse Analytics dedicated SQL pool using the Synapse connector for bi-directional data flow',
    code: `# PySpark - Read from Synapse dedicated SQL pool
synapse_url = (
    "jdbc:sqlserver://synapse-ws-dev.sql.azuresynapse.net:1433;"
    "database=dedicated_pool;"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.sql.azuresynapse.net;"
    "loginTimeout=30"
)

df_synapse = (spark.read
    .format("com.databricks.spark.sqldw")
    .option("url", synapse_url)
    .option("user", dbutils.secrets.get("keyvault-secrets", "synapse-user"))
    .option("password", dbutils.secrets.get("keyvault-secrets", "synapse-password"))
    .option("tempDir", "abfss://staging@stadatalake.dfs.core.windows.net/synapse-temp/")
    .option("forwardSparkAzureStorageCredentials", "true")
    .option("dbTable", "dbo.fact_sales")
    .load()
)

# Write enriched data back to Synapse
(enriched_df.write
    .format("com.databricks.spark.sqldw")
    .option("url", synapse_url)
    .option("user", dbutils.secrets.get("keyvault-secrets", "synapse-user"))
    .option("password", dbutils.secrets.get("keyvault-secrets", "synapse-password"))
    .option("tempDir", "abfss://staging@stadatalake.dfs.core.windows.net/synapse-temp/")
    .option("forwardSparkAzureStorageCredentials", "true")
    .option("dbTable", "dbo.fact_sales_enriched")
    .option("tableOptions", "DISTRIBUTION = HASH(customer_id), CLUSTERED COLUMNSTORE INDEX")
    .mode("overwrite")
    .save()
)

# Using Synapse Serverless SQL (via JDBC)
serverless_url = (
    "jdbc:sqlserver://synapse-ws-dev-ondemand.sql.azuresynapse.net:1433;"
    "database=analytics_db"
)

df_external = (spark.read
    .format("jdbc")
    .option("url", serverless_url)
    .option("query", "SELECT * FROM OPENROWSET(...) AS r")
    .option("authentication", "ActiveDirectoryServicePrincipal")
    .load()
)`,
  },
  {
    id: 6,
    title: 'Azure DevOps CI/CD Pipeline',
    desc: 'Full Azure DevOps YAML pipeline for Databricks: lint, test, deploy notebooks, run Terraform, and trigger jobs',
    code: `# azure-pipelines.yml
trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - databricks/**
      - terraform/**

variables:
  - group: databricks-secrets

stages:
  - stage: Validate
    jobs:
      - job: LintAndTest
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.11'
          - script: |
              pip install ruff pytest databricks-sdk
              ruff check databricks/
              pytest databricks/tests/ -v --tb=short
            displayName: 'Lint & Unit Test'

  - stage: DeployInfra
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - job: Terraform
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: TerraformInstaller@1
            inputs:
              terraformVersion: '1.7.0'
          - task: TerraformTaskV4@4
            inputs:
              provider: 'azurerm'
              command: 'init'
              workingDirectory: 'terraform/'
              backendServiceArm: 'azure-service-connection'
              backendAzureRmResourceGroupName: 'terraform-state-rg'
              backendAzureRmStorageAccountName: 'tfstatedatabricks'
              backendAzureRmContainerName: 'tfstate'
              backendAzureRmKey: 'databricks.tfstate'
          - task: TerraformTaskV4@4
            inputs:
              command: 'apply'
              workingDirectory: 'terraform/'
              environmentServiceNameAzureRM: 'azure-service-connection'

  - stage: DeployNotebooks
    dependsOn: DeployInfra
    jobs:
      - job: SyncRepos
        steps:
          - script: |
              pip install databricks-cli
              databricks repos update \\
                --path /Repos/production/etl \\
                --branch main
            env:
              DATABRICKS_HOST: $(DATABRICKS_HOST)
              DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
            displayName: 'Sync Repos to main'`,
  },
  {
    id: 7,
    title: 'AKS Integration for ML Serving',
    desc: 'Deploy Azure Kubernetes Service cluster for MLflow model serving with Databricks model registry',
    code: `# aks.tf
resource "azurerm_kubernetes_cluster" "ml_serving" {
  name                = "aks-mlserving-\${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.databricks.name
  dns_prefix          = "mlserving-\${var.environment}"

  default_node_pool {
    name                = "default"
    node_count          = 3
    vm_size             = "Standard_DS3_v2"
    enable_auto_scaling = true
    min_count           = 2
    max_count           = 5
    vnet_subnet_id      = azurerm_subnet.aks.id
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin    = "azure"
    load_balancer_sku = "standard"
  }

  tags = local.common_tags
}

# --- Deploy MLflow model to AKS from Databricks ---
# import mlflow.deployments
# from mlflow.tracking import MlflowClient
#
# client = MlflowClient()
# model_uri = "models:/fraud-detection-model/Production"
#
# # Build Docker container and push to ACR
# mlflow.models.build_docker(
#     model_uri=model_uri,
#     name="fraud-detection-serving",
# )
#
# # Kubernetes deployment manifest
# apiVersion: apps/v1
# kind: Deployment
# metadata:
#   name: fraud-detection-model
# spec:
#   replicas: 3
#   selector:
#     matchLabels:
#       app: fraud-detection
#   template:
#     spec:
#       containers:
#         - name: model
#           image: acr-registry.azurecr.io/fraud-detection-serving:latest
#           ports:
#             - containerPort: 8080
#           resources:
#             requests:
#               cpu: "500m"
#               memory: "1Gi"
#             limits:
#               cpu: "1000m"
#               memory: "2Gi"`,
  },
  {
    id: 8,
    title: 'Managed Identity Configuration',
    desc: 'Use Azure Managed Identity to eliminate credential management for Databricks access to Azure resources',
    code: `# managed_identity.tf
resource "azurerm_user_assigned_identity" "databricks" {
  name                = "id-databricks-\${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.databricks.name
}

# Assign roles to the managed identity
resource "azurerm_role_assignment" "mi_storage" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.databricks.principal_id
}

resource "azurerm_role_assignment" "mi_keyvault" {
  scope                = azurerm_key_vault.databricks.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.databricks.principal_id
}

# Access connector for Unity Catalog (system-assigned)
resource "azurerm_databricks_access_connector" "unity" {
  name                = "dbac-unity-\${var.environment}"
  resource_group_name = azurerm_resource_group.databricks.name
  location            = var.location

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.databricks.id]
  }
}

# --- Use in Databricks notebooks ---
# With Managed Identity, no secrets needed:
# spark.conf.set(
#     "fs.azure.account.auth.type.stadatalake.dfs.core.windows.net",
#     "OAuth"
# )
# spark.conf.set(
#     "fs.azure.account.oauth.provider.type.stadatalake.dfs.core.windows.net",
#     "org.apache.hadoop.fs.azurebfs.oauth2.ManagedIdentityCredentialProvider"
# )
# spark.conf.set(
#     "fs.azure.account.oauth2.msi.resource.stadatalake.dfs.core.windows.net",
#     azurerm_user_assigned_identity.databricks.client_id
# )
# df = spark.read.parquet("abfss://bronze@stadatalake.dfs.core.windows.net/events/")`,
  },
  {
    id: 9,
    title: 'Private Link & Network Isolation',
    desc: 'Set up Azure Private Link endpoints for Databricks workspace, storage, and Key Vault with full network isolation',
    code: `# private_link.tf
resource "azurerm_virtual_network" "databricks" {
  name                = "vnet-databricks-\${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.databricks.name
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_subnet" "public" {
  name                 = "snet-databricks-public"
  resource_group_name  = azurerm_resource_group.databricks.name
  virtual_network_name = azurerm_virtual_network.databricks.name
  address_prefixes     = ["10.0.1.0/24"]

  delegation {
    name = "databricks-del-pub"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
      ]
    }
  }
}

resource "azurerm_subnet" "private" {
  name                 = "snet-databricks-private"
  resource_group_name  = azurerm_resource_group.databricks.name
  virtual_network_name = azurerm_virtual_network.databricks.name
  address_prefixes     = ["10.0.2.0/24"]

  delegation {
    name = "databricks-del-priv"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
    }
  }
}

resource "azurerm_subnet" "private_endpoints" {
  name                 = "snet-private-endpoints"
  resource_group_name  = azurerm_resource_group.databricks.name
  virtual_network_name = azurerm_virtual_network.databricks.name
  address_prefixes     = ["10.0.3.0/24"]
}

# Private Endpoint for Storage
resource "azurerm_private_endpoint" "storage_blob" {
  name                = "pe-storage-blob-\${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.databricks.name
  subnet_id           = azurerm_subnet.private_endpoints.id

  private_service_connection {
    name                           = "psc-storage-blob"
    private_connection_resource_id = azurerm_storage_account.datalake.id
    subresource_names              = ["blob"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "storage-dns"
    private_dns_zone_ids = [azurerm_private_dns_zone.blob.id]
  }
}

resource "azurerm_private_dns_zone" "blob" {
  name                = "privatelink.blob.core.windows.net"
  resource_group_name = azurerm_resource_group.databricks.name
}

resource "azurerm_private_dns_zone_virtual_network_link" "blob" {
  name                  = "blob-dns-link"
  resource_group_name   = azurerm_resource_group.databricks.name
  private_dns_zone_name = azurerm_private_dns_zone.blob.name
  virtual_network_id    = azurerm_virtual_network.databricks.id
}`,
  },
  {
    id: 10,
    title: 'Cost Management & Tagging',
    desc: 'Implement Azure cost management with tag policies, budget alerts, and Databricks cluster cost tracking',
    code: `# cost_management.tf
resource "azurerm_consumption_budget_resource_group" "databricks" {
  name              = "budget-databricks-\${var.environment}"
  resource_group_id = azurerm_resource_group.databricks.id

  amount     = var.monthly_budget
  time_grain = "Monthly"

  time_period {
    start_date = "2026-01-01T00:00:00Z"
    end_date   = "2026-12-31T23:59:59Z"
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = var.budget_alert_emails
  }

  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThan"
    threshold_type = "Forecasted"
    contact_emails = var.budget_alert_emails
  }
}

# Azure Policy for mandatory tags
resource "azurerm_resource_group_policy_assignment" "require_tags" {
  name                 = "require-cost-tags"
  resource_group_id    = azurerm_resource_group.databricks.id
  policy_definition_id = "/providers/Microsoft.Authorization/policyDefinitions/96670d01-0a4d-4649-9c89-2d3abc0a5025"

  parameters = jsonencode({
    tagName = { value = "CostCenter" }
  })
}

locals {
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    CostCenter  = var.cost_center
    ManagedBy   = "terraform"
    Owner       = var.owner_email
    CreatedDate = timestamp()
  }
}

# --- Databricks cost tracking notebook ---
# from databricks.sdk import WorkspaceClient
# import pandas as pd
#
# w = WorkspaceClient()
# clusters = w.clusters.list()
#
# cost_data = []
# for c in clusters:
#     cost_data.append({
#         "cluster_name": c.cluster_name,
#         "cluster_id": c.cluster_id,
#         "state": c.state.value,
#         "node_type": c.node_type_id,
#         "num_workers": c.num_workers or 0,
#         "autotermination_mins": c.autotermination_minutes,
#         "tags": c.custom_tags
#     })
#
# df = spark.createDataFrame(pd.DataFrame(cost_data))
# df.write.mode("overwrite").saveAsTable("admin.cost_tracking.cluster_inventory")`,
  },
];

const snowflakeScenarios = [
  {
    id: 1,
    title: 'Snowflake Connector Setup',
    desc: 'Install and configure the Snowflake Spark connector with optimized session parameters for Databricks',
    code: `# Install the Snowflake connector on your Databricks cluster
# Cluster Libraries -> Install New -> Maven
# Coordinates: net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4
# Also install: net.snowflake:snowflake-jdbc:3.16.0

# Connection options dictionary
snowflake_options = {
    "sfUrl": "myaccount.snowflakecomputing.com",
    "sfUser": dbutils.secrets.get("snowflake-scope", "username"),
    "sfPassword": dbutils.secrets.get("snowflake-scope", "password"),
    "sfDatabase": "ANALYTICS_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "DATA_ENGINEER_ROLE",
    # Performance tuning
    "autopushdown": "on",           # Push predicates to Snowflake
    "sfCompress": "on",             # Compress data in transit
    "parallelism": "8",             # Parallel data fetch threads
    "keep_column_case": "on",       # Preserve column name casing
    "use_copy_unload": "true",      # Use COPY INTO for reads (faster)
    "column_mapping": "by_name",
}

# Verify connectivity
df_test = (spark.read
    .format("net.snowflake.spark.snowflake")
    .options(**snowflake_options)
    .option("query", "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
    .load()
)
df_test.show()
print("Snowflake connection verified successfully!")`,
  },
  {
    id: 2,
    title: 'Read/Write Snowflake from Spark',
    desc: 'Read Snowflake tables into Spark DataFrames and write Spark DataFrames back to Snowflake with various strategies',
    code: `# --- READ from Snowflake ---

# Read entire table
df_customers = (spark.read
    .format("net.snowflake.spark.snowflake")
    .options(**snowflake_options)
    .option("dbtable", "DIM_CUSTOMERS")
    .load()
)

# Read with SQL query (pushed down to Snowflake)
df_active = (spark.read
    .format("net.snowflake.spark.snowflake")
    .options(**snowflake_options)
    .option("query", """
        SELECT customer_id, name, segment, lifetime_value
        FROM DIM_CUSTOMERS
        WHERE is_active = TRUE
          AND created_date >= DATEADD(year, -1, CURRENT_DATE())
        ORDER BY lifetime_value DESC
        LIMIT 10000
    """)
    .load()
)

# --- WRITE to Snowflake ---

# Overwrite table
(enriched_df.write
    .format("net.snowflake.spark.snowflake")
    .options(**snowflake_options)
    .option("dbtable", "FACT_SALES_ENRICHED")
    .mode("overwrite")
    .save()
)

# Append to existing table
(new_events_df.write
    .format("net.snowflake.spark.snowflake")
    .options(**snowflake_options)
    .option("dbtable", "FACT_EVENTS")
    .mode("append")
    .save()
)

# Write with pre/post SQL actions
(df_report.write
    .format("net.snowflake.spark.snowflake")
    .options(**snowflake_options)
    .option("dbtable", "MONTHLY_REPORT")
    .option("preactions", "TRUNCATE TABLE IF EXISTS MONTHLY_REPORT_STAGING")
    .option("postactions", """
        INSERT INTO MONTHLY_REPORT SELECT * FROM MONTHLY_REPORT_STAGING;
        DROP TABLE MONTHLY_REPORT_STAGING;
    """)
    .mode("overwrite")
    .save()
)`,
  },
  {
    id: 3,
    title: 'Snowflake Stages & File Operations',
    desc: 'Use Snowflake internal/external stages with Databricks for bulk data loading and unloading via cloud storage',
    code: `-- Snowflake SQL: Create external stage pointing to ADLS Gen2
CREATE OR REPLACE STAGE analytics_db.public.adls_stage
    URL = 'azure://stadatalake.blob.core.windows.net/staging/'
    STORAGE_INTEGRATION = azure_integration
    FILE_FORMAT = (
        TYPE = 'PARQUET'
        COMPRESSION = 'SNAPPY'
    );

-- List files in stage
LIST @adls_stage/export/;

-- Load from stage into Snowflake table
COPY INTO FACT_SALES
FROM @adls_stage/export/sales/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE'
    PURGE = TRUE;

-- Unload from Snowflake to stage for Databricks consumption
COPY INTO @adls_stage/export/customers/
FROM (
    SELECT customer_id, name, segment, region
    FROM DIM_CUSTOMERS
    WHERE updated_at >= DATEADD(day, -1, CURRENT_DATE())
)
FILE_FORMAT = (TYPE = 'PARQUET')
OVERWRITE = TRUE
HEADER = TRUE
MAX_FILE_SIZE = 268435456;  -- 256 MB per file

# --- Databricks: Read the staged files ---
# df_from_stage = (spark.read
#     .parquet("abfss://staging@stadatalake.dfs.core.windows.net/export/customers/")
# )
# df_from_stage.createOrReplaceTempView("staged_customers")
# spark.sql("""
#     MERGE INTO gold.dim_customers t
#     USING staged_customers s ON t.customer_id = s.customer_id
#     WHEN MATCHED THEN UPDATE SET *
#     WHEN NOT MATCHED THEN INSERT *
# """)`,
  },
  {
    id: 4,
    title: 'Snowpark Integration',
    desc: 'Use Snowpark Python for server-side processing in Snowflake from Databricks, combining both engines',
    code: `# Install snowflake-snowpark-python on Databricks cluster
# Cluster Libraries -> PyPI -> snowflake-snowpark-python==1.14.0

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sf_sum, avg, when, lit
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType

# Create Snowpark session from Databricks
connection_params = {
    "account": "myaccount",
    "user": dbutils.secrets.get("snowflake-scope", "username"),
    "password": dbutils.secrets.get("snowflake-scope", "password"),
    "warehouse": "COMPUTE_WH",
    "database": "ANALYTICS_DB",
    "schema": "PUBLIC",
    "role": "DATA_ENGINEER_ROLE",
}

session = Session.builder.configs(connection_params).create()

# Run transformations in Snowflake (server-side)
orders_df = session.table("FACT_ORDERS")
customer_summary = (orders_df
    .filter(col("ORDER_DATE") >= "2025-01-01")
    .group_by("CUSTOMER_ID")
    .agg(
        sf_sum("ORDER_TOTAL").alias("TOTAL_SPEND"),
        avg("ORDER_TOTAL").alias("AVG_ORDER_VALUE"),
        sf_sum(when(col("STATUS") == "RETURNED", 1).otherwise(0)).alias("RETURN_COUNT")
    )
    .filter(col("TOTAL_SPEND") > 1000)
)

# Save result in Snowflake
customer_summary.write.mode("overwrite").save_as_table("CUSTOMER_SUMMARY_2025")

# Bring small result set to Databricks for ML
pandas_df = customer_summary.to_pandas()
spark_df = spark.createDataFrame(pandas_df)
spark_df.write.format("delta").mode("overwrite").saveAsTable("gold.customer_summary")

# Register Snowpark UDF
from snowflake.snowpark.functions import udf

@udf(name="calculate_risk_score", replace=True,
     input_types=[FloatType(), FloatType()], return_type=FloatType())
def calculate_risk_score(total_spend, return_count):
    if total_spend == 0:
        return 0.0
    return min(float(return_count) / float(total_spend) * 100, 100.0)

session.close()`,
  },
  {
    id: 5,
    title: 'External Tables & Data Sharing',
    desc: 'Create Snowflake external tables over cloud storage and set up secure data sharing between accounts',
    code: `-- External table over ADLS Gen2 (Databricks-managed Delta files)
CREATE OR REPLACE EXTERNAL TABLE analytics_db.public.ext_sales_delta (
    sale_id       VARCHAR   AS (value:sale_id::VARCHAR),
    customer_id   VARCHAR   AS (value:customer_id::VARCHAR),
    product_id    VARCHAR   AS (value:product_id::VARCHAR),
    amount        FLOAT     AS (value:amount::FLOAT),
    sale_date     DATE      AS (value:sale_date::DATE)
)
WITH LOCATION = @adls_stage/gold/sales/
AUTO_REFRESH = TRUE
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*\\.parquet';

-- Query external table
SELECT product_id, SUM(amount) as total_revenue
FROM ext_sales_delta
WHERE sale_date >= '2025-01-01'
GROUP BY product_id
ORDER BY total_revenue DESC;

-- ---- Secure Data Sharing ----

-- Provider account: Create a share
CREATE OR REPLACE SHARE customer_analytics_share;
GRANT USAGE ON DATABASE ANALYTICS_DB TO SHARE customer_analytics_share;
GRANT USAGE ON SCHEMA ANALYTICS_DB.PUBLIC TO SHARE customer_analytics_share;
GRANT SELECT ON TABLE ANALYTICS_DB.PUBLIC.CUSTOMER_SUMMARY_2025
    TO SHARE customer_analytics_share;

-- Add consumer account
ALTER SHARE customer_analytics_share ADD ACCOUNTS = consumer_account_id;

-- Consumer account: Create database from share
CREATE OR REPLACE DATABASE shared_analytics
    FROM SHARE provider_account.customer_analytics_share;

-- Query shared data
SELECT * FROM shared_analytics.public.customer_summary_2025
WHERE total_spend > 5000;

-- ---- Databricks: Read shared data via connector ----
# shared_options = {**snowflake_options, "sfDatabase": "SHARED_ANALYTICS"}
# df_shared = (spark.read
#     .format("net.snowflake.spark.snowflake")
#     .options(**shared_options)
#     .option("dbtable", "CUSTOMER_SUMMARY_2025")
#     .load()
# )`,
  },
  {
    id: 6,
    title: 'Snowpipe Automation',
    desc: 'Set up Snowpipe for continuous auto-ingestion from cloud storage with event notifications and error handling',
    code: `-- Create file format for incoming data
CREATE OR REPLACE FILE FORMAT json_ingest_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMPRESSION = 'AUTO'
    TRIM_SPACE = TRUE;

-- Create target table
CREATE OR REPLACE TABLE raw_events (
    event_id      VARCHAR,
    event_type    VARCHAR,
    user_id       VARCHAR,
    payload       VARIANT,
    received_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file   VARCHAR DEFAULT METADATA$FILENAME
);

-- Create Snowpipe for auto-ingestion
CREATE OR REPLACE PIPE analytics_db.public.events_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest events from ADLS Gen2'
AS
COPY INTO raw_events (event_id, event_type, user_id, payload)
FROM (
    SELECT
        $1:event_id::VARCHAR,
        $1:event_type::VARCHAR,
        $1:user_id::VARCHAR,
        $1
    FROM @adls_stage/events/incoming/
)
FILE_FORMAT = (FORMAT_NAME = json_ingest_format)
ON_ERROR = 'SKIP_FILE_5%';

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('analytics_db.public.events_pipe');

-- View copy history and errors
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_EVENTS',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- Monitor pipe load errors
SELECT *
FROM TABLE(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'analytics_db.public.events_pipe',
    START_TIME => DATEADD(hours, -2, CURRENT_TIMESTAMP())
));

-- ---- Databricks: Process Snowpipe-loaded data ----
# Process newly ingested data in Databricks for ML features
# df_recent = (spark.read
#     .format("net.snowflake.spark.snowflake")
#     .options(**snowflake_options)
#     .option("query", """
#         SELECT * FROM raw_events
#         WHERE received_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
#     """)
#     .load()
# )`,
  },
  {
    id: 7,
    title: 'Cross-Platform Query Federation',
    desc: 'Run federated queries across Databricks Delta Lake and Snowflake, combining data from both platforms',
    code: `# Cross-platform query pattern:
# 1. Large historical data stays in Delta Lake (Databricks)
# 2. Real-time/shared data stays in Snowflake
# 3. Join them in Spark for analytics

# Step 1: Read dimension table from Snowflake (small, shared)
df_products = (spark.read
    .format("net.snowflake.spark.snowflake")
    .options(**snowflake_options)
    .option("dbtable", "DIM_PRODUCTS")
    .load()
    .cache()  # Cache small dimension table
)

# Step 2: Read fact table from Delta Lake (large, historical)
df_transactions = spark.read.table("gold.fact_transactions")

# Step 3: Join across platforms
df_enriched = (df_transactions
    .join(df_products, "product_id", "left")
    .withColumn("revenue_category",
        when(col("unit_price") * col("quantity") > 1000, "high")
        .when(col("unit_price") * col("quantity") > 100, "medium")
        .otherwise("low")
    )
    .groupBy("category", "region", "revenue_category")
    .agg(
        sum("quantity").alias("total_units"),
        sum(col("unit_price") * col("quantity")).alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers")
    )
)

# Step 4: Write results back to both platforms
# To Delta Lake
df_enriched.write.format("delta").mode("overwrite").saveAsTable("gold.cross_platform_summary")

# To Snowflake
(df_enriched.write
    .format("net.snowflake.spark.snowflake")
    .options(**snowflake_options)
    .option("dbtable", "CROSS_PLATFORM_SUMMARY")
    .mode("overwrite")
    .save()
)

# Step 5: Create a view for BI tools
spark.sql("""
    CREATE OR REPLACE VIEW gold.vw_revenue_dashboard AS
    SELECT *, current_timestamp() as refreshed_at
    FROM gold.cross_platform_summary
""")`,
  },
  {
    id: 8,
    title: 'Snowflake Data Sharing with Databricks',
    desc: 'Use Snowflake Secure Data Sharing and Delta Sharing protocol for cross-platform data exchange',
    code: `# ---- Delta Sharing: Databricks -> Snowflake ----
# Databricks side: Create a Delta Share
# Using Unity Catalog

# Step 1: Create a share in Databricks
# CREATE SHARE IF NOT EXISTS sales_share;
# ALTER SHARE sales_share ADD TABLE gold.fact_sales;
# ALTER SHARE sales_share ADD TABLE gold.dim_customers;
#
# GRANT SELECT ON SHARE sales_share TO RECIPIENT snowflake_consumer;
#
# CREATE RECIPIENT IF NOT EXISTS snowflake_consumer
#     PROPERTIES (
#       'sharing_id' = 'snowflake-account-locator'
#     );

# Step 2: Snowflake reads Delta Sharing data
# -- Snowflake SQL: Create integration for Delta Sharing
# CREATE OR REPLACE EXTERNAL VOLUME delta_sharing_vol
#     STORAGE_LOCATIONS = (
#         (
#             NAME = 'adls_delta'
#             STORAGE_BASE_URL = 'azure://stadatalake.blob.core.windows.net/delta-shares/'
#             AZURE_TENANT_ID = '<tenant-id>'
#         )
#     );

# ---- Bi-directional Sync Pattern ----
# Databricks orchestrator notebook

from datetime import datetime, timedelta
import json

def sync_snowflake_to_delta(table_name, watermark_col="updated_at"):
    """Incremental sync from Snowflake to Delta Lake."""
    # Get last sync watermark
    try:
        last_sync = spark.sql(
            f"SELECT MAX({watermark_col}) FROM bronze.sf_{table_name}"
        ).collect()[0][0]
    except Exception:
        last_sync = "1900-01-01"

    # Read incremental data from Snowflake
    df_incremental = (spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**snowflake_options)
        .option("query", f"""
            SELECT * FROM {table_name}
            WHERE {watermark_col} > '{last_sync}'
            ORDER BY {watermark_col}
        """)
        .load()
    )

    count = df_incremental.count()
    if count > 0:
        (df_incremental.write
            .format("delta")
            .mode("append")
            .saveAsTable(f"bronze.sf_{table_name}")
        )
        print(f"Synced {count} rows from Snowflake {table_name}")
    else:
        print(f"No new data in Snowflake {table_name}")

    return count

# Run sync for multiple tables
tables_to_sync = ["DIM_PRODUCTS", "DIM_STORES", "FACT_INVENTORY"]
sync_results = {t: sync_snowflake_to_delta(t) for t in tables_to_sync}
print(json.dumps(sync_results, indent=2))`,
  },
];

const desktopScenarios = [
  {
    id: 1,
    title: 'Local PySpark Development Setup',
    desc: 'Install and configure PySpark locally with Delta Lake support for offline development and testing',
    code: `# --- Terminal: Install PySpark with Delta Lake ---
# pip install pyspark==3.5.0 delta-spark==3.1.0 pytest ipykernel

# --- local_spark.py: Reusable local Spark session ---
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_local_spark(app_name="local-dev", warehouse_dir="/tmp/spark-warehouse"):
    """Create a local Spark session with Delta Lake support."""
    builder = (SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.preview.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

# --- Usage in notebooks or scripts ---
if __name__ == "__main__":
    spark = get_local_spark()

    # Create sample Delta table
    data = [
        (1, "Alice", "Engineering", 95000),
        (2, "Bob", "Marketing", 82000),
        (3, "Carol", "Engineering", 110000),
    ]
    df = spark.createDataFrame(data, ["id", "name", "dept", "salary"])

    df.write.format("delta").mode("overwrite").save("/tmp/delta/employees")

    # Read and query
    df_read = spark.read.format("delta").load("/tmp/delta/employees")
    df_read.filter("dept = 'Engineering'").show()

    # Time travel
    df_read_v0 = (spark.read.format("delta")
        .option("versionAsOf", 0)
        .load("/tmp/delta/employees")
    )

    spark.stop()`,
  },
  {
    id: 2,
    title: 'Databricks Connect v2',
    desc: 'Set up Databricks Connect to run local code against a remote Databricks cluster seamlessly',
    code: `# --- Terminal: Install Databricks Connect ---
# pip install databricks-connect==14.3.1
# Must match your cluster DBR version!

# --- Configure authentication ---
# Option 1: Environment variables
# export DATABRICKS_HOST=https://adb-1234567890.1.azuredatabricks.net
# export DATABRICKS_TOKEN=dapi1234567890abcdef
# export DATABRICKS_CLUSTER_ID=0123-456789-abcde12

# Option 2: .databrickscfg profile
# [DEFAULT]
# host = https://adb-1234567890.1.azuredatabricks.net
# token = dapi1234567890abcdef
# cluster_id = 0123-456789-abcde12

# --- databricks_connect_session.py ---
from databricks.connect import DatabricksSession

def get_remote_spark(profile="DEFAULT"):
    """Create a Spark session connected to remote Databricks cluster."""
    spark = DatabricksSession.builder.profile(profile).getOrCreate()
    return spark

# --- Run local code on remote cluster ---
spark = get_remote_spark()

# This runs on the remote cluster, not locally
df = spark.sql("SELECT * FROM gold.fact_sales LIMIT 100")
df.show()

# Local pandas interop
pandas_df = df.toPandas()
print(f"Fetched {len(pandas_df)} rows")

# Write results back to Unity Catalog
(spark.createDataFrame(pandas_df)
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("dev_sandbox.analysis.local_test_output")
)

# --- Validate connection ---
# databricks-connect test
# Should print: "Spark Session available as 'spark'"

# --- Important notes ---
# 1. Cluster must be running (auto-start supported)
# 2. DBR version must match databricks-connect version
# 3. Unity Catalog tables accessible just like in notebooks
# 4. dbutils NOT available - use databricks-sdk instead`,
  },
  {
    id: 3,
    title: 'VS Code Extension & IDE Setup',
    desc: 'Configure VS Code with Databricks extension for notebook editing, job runs, and interactive development',
    code: `// --- .vscode/settings.json ---
{
  "python.defaultInterpreterPath": "./venv/bin/python",
  "python.analysis.extraPaths": [
    "src",
    "lib"
  ],
  "databricks.host": "https://adb-1234567890.1.azuredatabricks.net",
  "databricks.clusterId": "0123-456789-abcde12",
  "editor.formatOnSave": true,
  "python.formatting.provider": "black",
  "python.linting.ruffEnabled": true,
  "files.exclude": {
    "**/__pycache__": true,
    "**/.pytest_cache": true,
    "**/*.pyc": true
  }
}

// --- .vscode/launch.json (debug with Databricks Connect) ---
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Databricks Connect Debug",
      "type": "debugpy",
      "request": "launch",
      "program": "\${file}",
      "env": {
        "DATABRICKS_HOST": "https://adb-1234567890.1.azuredatabricks.net",
        "DATABRICKS_TOKEN": "\${env:DATABRICKS_TOKEN}",
        "DATABRICKS_CLUSTER_ID": "0123-456789-abcde12"
      },
      "console": "integratedTerminal"
    },
    {
      "name": "Local PySpark Debug",
      "type": "debugpy",
      "request": "launch",
      "program": "\${file}",
      "env": {
        "SPARK_HOME": "\${workspaceFolder}/venv/lib/python3.11/site-packages/pyspark",
        "PYSPARK_PYTHON": "\${workspaceFolder}/venv/bin/python"
      },
      "console": "integratedTerminal"
    }
  ]
}

# --- VS Code Extensions to install ---
# 1. Databricks (by Databricks) - notebook sync, job management
# 2. Pylance - Python IntelliSense
# 3. Jupyter - notebook rendering
# 4. Python - debugging, linting
# 5. Ruff - fast Python linting
# 6. GitLens - git history
# 7. Thunder Client - REST API testing (Databricks REST API)`,
  },
  {
    id: 4,
    title: 'Docker Local Environment',
    desc: 'Run a complete Databricks-like local development environment using Docker with Spark, Delta Lake, and Jupyter',
    code: `# --- Dockerfile.local-spark ---
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \\
    openjdk-17-jre-headless \\
    curl \\
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=\${SPARK_HOME}/bin:\${PATH}

# Install PySpark and Delta Lake
RUN pip install --no-cache-dir \\
    pyspark==3.5.0 \\
    delta-spark==3.1.0 \\
    databricks-connect==14.3.1 \\
    mlflow==2.12.0 \\
    jupyterlab==4.1.0 \\
    pytest==8.1.0 \\
    ruff==0.3.0 \\
    pandas numpy scikit-learn

WORKDIR /workspace
EXPOSE 8888 4040 5000

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

# --- docker-compose.local.yml ---
# version: "3.8"
# services:
#   spark-dev:
#     build:
#       context: .
#       dockerfile: Dockerfile.local-spark
#     ports:
#       - "8888:8888"   # JupyterLab
#       - "4040:4040"   # Spark UI
#       - "5000:5000"   # MLflow UI
#     volumes:
#       - ./notebooks:/workspace/notebooks
#       - ./src:/workspace/src
#       - ./data:/workspace/data
#       - ./tests:/workspace/tests
#       - spark-warehouse:/workspace/spark-warehouse
#     environment:
#       - DATABRICKS_HOST=\${DATABRICKS_HOST}
#       - DATABRICKS_TOKEN=\${DATABRICKS_TOKEN}
#       - MLFLOW_TRACKING_URI=http://localhost:5000
#     command: >
#       bash -c "
#         mlflow server --host 0.0.0.0 --port 5000 &
#         jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
#       "
#
#   mlflow-db:
#     image: postgres:16
#     environment:
#       POSTGRES_DB: mlflow
#       POSTGRES_USER: mlflow
#       POSTGRES_PASSWORD: mlflow
#     volumes:
#       - mlflow-data:/var/lib/postgresql/data
#
# volumes:
#   spark-warehouse:
#   mlflow-data:

# --- Usage ---
# docker compose -f docker-compose.local.yml up -d
# Open http://localhost:8888 for JupyterLab
# Open http://localhost:4040 for Spark UI (when Spark running)
# Open http://localhost:5000 for MLflow UI`,
  },
  {
    id: 5,
    title: 'Databricks CLI & Automation',
    desc: 'Install and use the Databricks CLI for workspace management, job control, secrets, and CI/CD scripting',
    code: `# --- Install Databricks CLI v2 ---
# pip install databricks-cli
# Or: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# --- Configure authentication ---
# databricks configure --token
# Or set environment variables:
# export DATABRICKS_HOST=https://adb-1234567890.1.azuredatabricks.net
# export DATABRICKS_TOKEN=dapi1234567890abcdef

# --- Workspace operations ---
# List workspace contents
databricks workspace ls /Repos/production/

# Export a notebook
databricks workspace export /Shared/etl/extract --format SOURCE -o extract.py

# Import a notebook
databricks workspace import ./local_notebook.py /Shared/etl/new_notebook --language PYTHON --overwrite

# --- Job management ---
# List all jobs
databricks jobs list --output JSON | jq '.jobs[] | {job_id, name: .settings.name}'

# Trigger a job run
databricks jobs run-now --job-id 12345 --notebook-params '{"date": "2026-04-14", "env": "prod"}'

# Get run status
databricks runs get --run-id 67890 | jq '{state: .state.life_cycle_state, result: .state.result_state}'

# --- Cluster management ---
databricks clusters list --output TABLE
databricks clusters start --cluster-id 0123-456789-abcde12
databricks clusters get --cluster-id 0123-456789-abcde12 | jq '.state'

# --- Secrets management ---
databricks secrets create-scope --scope my-scope
databricks secrets put --scope my-scope --key api-key --string-value "sk-abc123"
databricks secrets list --scope my-scope

# --- CI/CD script example (deploy.sh) ---
# #!/bin/bash
# set -euo pipefail
#
# echo "Syncing notebooks to production..."
# databricks workspace import_dir ./src/notebooks /Repos/production/etl --overwrite
#
# echo "Updating job definitions..."
# for job_file in jobs/*.json; do
#   JOB_NAME=$(jq -r '.name' "$job_file")
#   JOB_ID=$(databricks jobs list --output JSON | jq -r ".jobs[] | select(.settings.name==\\"$JOB_NAME\\") | .job_id")
#   if [ -n "$JOB_ID" ]; then
#     databricks jobs reset --job-id "$JOB_ID" --json-file "$job_file"
#     echo "Updated job: $JOB_NAME ($JOB_ID)"
#   else
#     databricks jobs create --json-file "$job_file"
#     echo "Created job: $JOB_NAME"
#   fi
# done`,
  },
  {
    id: 6,
    title: 'Local Testing & CI Integration',
    desc: 'Set up pytest-based testing framework for PySpark code with fixtures, mocks, and CI pipeline integration',
    code: `# --- conftest.py: Shared test fixtures ---
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    builder = (SparkSession.builder
        .master("local[2]")
        .appName("unit-tests")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()

@pytest.fixture
def sample_sales(spark):
    data = [
        (1, "P001", 10, 99.99, "2026-01-15"),
        (2, "P002", 5, 49.99, "2026-01-16"),
        (3, "P001", 3, 99.99, "2026-02-01"),
    ]
    return spark.createDataFrame(data,
        ["sale_id", "product_id", "quantity", "unit_price", "sale_date"])

# --- test_transformations.py ---
from pyspark.sql.functions import col, sum as spark_sum

def calculate_revenue(df):
    return df.withColumn("revenue", col("quantity") * col("unit_price"))

def aggregate_by_product(df):
    return (df
        .groupBy("product_id")
        .agg(spark_sum("revenue").alias("total_revenue"))
    )

def test_calculate_revenue(spark, sample_sales):
    result = calculate_revenue(sample_sales)
    row = result.filter("sale_id = 1").collect()[0]
    assert row["revenue"] == pytest.approx(999.9, rel=1e-2)

def test_aggregate_by_product(spark, sample_sales):
    df_with_rev = calculate_revenue(sample_sales)
    result = aggregate_by_product(df_with_rev)
    p001 = result.filter("product_id = 'P001'").collect()[0]
    assert p001["total_revenue"] == pytest.approx(1299.87, rel=1e-2)

def test_delta_write_read(spark, sample_sales, tmp_path):
    path = str(tmp_path / "test_delta")
    sample_sales.write.format("delta").save(path)
    df_read = spark.read.format("delta").load(path)
    assert df_read.count() == 3

# --- pytest.ini ---
# [pytest]
# testpaths = tests
# python_files = test_*.py
# python_functions = test_*
# addopts = -v --tb=short --strict-markers
# markers =
#     integration: marks tests as integration (deselect with '-m "not integration"')
#     slow: marks tests as slow

# --- Run tests ---
# pytest tests/ -v
# pytest tests/ -v -m "not integration"
# pytest tests/ --cov=src --cov-report=html --cov-fail-under=80`,
  },
];

const tabs = [
  {
    key: 'terraform',
    label: 'Terraform for Databricks',
    badge: '10',
    scenarios: terraformScenarios,
  },
  { key: 'azure', label: 'Azure Integration', badge: '10', scenarios: azureScenarios },
  { key: 'snowflake', label: 'Snowflake Integration', badge: '8', scenarios: snowflakeScenarios },
  { key: 'desktop', label: 'Desktop / Local Dev', badge: '6', scenarios: desktopScenarios },
];

// Generate multiple IaC approaches per scenario (Terraform HCL, Bicep, ARM, CLI, Pulumi)
function generateApproaches(scenario) {
  const hcl = scenario.code || '# Terraform HCL';

  const bicep = `// Bicep — ${scenario.title}
param environment string = 'dev'
param location string = 'eastus'

resource workspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: 'dbx-\${environment}'
  location: location
  sku: { name: 'premium' }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', 'managed-dbx-\${environment}')
  }
  tags: { env: environment, owner: 'data-platform' }
}`;

  const arm = `{
  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "environment": { "type": "string", "defaultValue": "dev" }
  },
  "resources": [
    {
      "type": "Microsoft.Databricks/workspaces",
      "apiVersion": "2023-02-01",
      "name": "[concat('dbx-', parameters('environment'))]",
      "location": "[resourceGroup().location]",
      "sku": { "name": "premium" },
      "properties": { "managedResourceGroupId": "[subscriptionResourceId('Microsoft.Resources/resourceGroups', concat('managed-dbx-', parameters('environment')))]" }
    }
  ]
}`;

  const cli = `# Azure CLI — ${scenario.title}
az databricks workspace create \\
  --resource-group rg-databricks-dev \\
  --name dbx-dev \\
  --location eastus \\
  --sku premium \\
  --tags env=dev owner=data-platform

# Databricks CLI — register workspace
databricks configure --token
databricks workspace list`;

  const pulumi = `// Pulumi TypeScript — ${scenario.title}
import * as azure from "@pulumi/azure-native";
import * as databricks from "@pulumi/databricks";

const rg = new azure.resources.ResourceGroup("rg-dbx", { location: "eastus" });

const workspace = new azure.databricks.Workspace("dbx-dev", {
  resourceGroupName: rg.name,
  location: rg.location,
  sku: { name: "premium" },
  managedResourceGroupId: \`/subscriptions/\${config.subscriptionId}/resourceGroups/managed-dbx-dev\`,
  tags: { env: "dev", owner: "data-platform" },
});

export const workspaceUrl = workspace.workspaceUrl;`;

  // Terragrunt wrapper
  const terragrunt = `# terragrunt.hcl — ${scenario.title}
include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "git::ssh://git@github.com/org/tf-modules.git//databricks?ref=v1.2.0"
}

inputs = {
  environment     = "dev"
  location        = "eastus"
  subscription_id = get_env("ARM_SUBSCRIPTION_ID")
  tenant_id       = get_env("ARM_TENANT_ID")
}

remote_state {
  backend = "azurerm"
  config = {
    resource_group_name  = "terraform-state-rg"
    storage_account_name = "tfstatedbx"
    container_name       = "tfstate"
    key                  = "\${path_relative_to_include()}/terraform.tfstate"
  }
}`;

  return [
    { id: 'hcl', label: 'Terraform HCL', icon: '🏗️', lang: 'hcl', code: hcl },
    { id: 'bicep', label: 'Azure Bicep', icon: '☁️', lang: 'bicep', code: bicep },
    { id: 'arm', label: 'ARM Template', icon: '📋', lang: 'json', code: arm },
    { id: 'cli', label: 'Azure / Databricks CLI', icon: '⌨️', lang: 'bash', code: cli },
    { id: 'pulumi', label: 'Pulumi (TypeScript)', icon: '🎯', lang: 'ts', code: pulumi },
    { id: 'terragrunt', label: 'Terragrunt', icon: '🧱', lang: 'hcl', code: terragrunt },
  ];
}

// Generate sample resource data for downloads/run/schedule
function generateResourceData(scenario) {
  const envs = ['dev', 'qa', 'prod'];
  return envs.map((env, i) => ({
    resource_id: `dbx-workspace-${env}-${1000 + scenario.id}`,
    resource_type: 'Microsoft.Databricks/workspaces',
    name: `dbx-${env}`,
    environment: env,
    location: 'eastus',
    sku: env === 'prod' ? 'premium' : 'standard',
    status: 'ACTIVE',
    managed_rg: `managed-dbx-${env}`,
    tags_owner: 'data-platform',
    created_at: `2024-01-${15 + i}T08:00:00Z`,
    drift_detected: env === 'prod' ? false : Math.random() > 0.7,
    monthly_cost_usd: env === 'prod' ? 12500 : env === 'qa' ? 3200 : 850,
  }));
}

function ScenarioCard({ scenario }) {
  const [expanded, setExpanded] = useState(false);
  const [activeApproach, setActiveApproach] = useState('hcl');
  const approaches = generateApproaches(scenario);
  const current = approaches.find((a) => a.id === activeApproach) || approaches[0];
  const resourceData = generateResourceData(scenario);
  const slug = `terraform-${scenario.id}-${(scenario.title || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .slice(0, 30)}`;

  return (
    <div className="card" style={{ marginBottom: '16px' }}>
      <div
        onClick={() => setExpanded(!expanded)}
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          cursor: 'pointer',
          padding: '16px',
        }}
      >
        <div>
          <h3 style={{ margin: 0, fontSize: '16px' }}>
            {scenario.id}. {scenario.title}
          </h3>
          <p style={{ margin: '4px 0 0', color: '#666', fontSize: '14px' }}>{scenario.desc}</p>
        </div>
        <span style={{ fontSize: '20px', color: '#888', flexShrink: 0, marginLeft: '12px' }}>
          {expanded ? '▲' : '▼'}
        </span>
      </div>
      {expanded && (
        <div style={{ padding: '0 16px 16px' }}>
          {/* Multi-approach tabs */}
          <div
            style={{
              display: 'flex',
              gap: '0.4rem',
              flexWrap: 'wrap',
              marginBottom: '0.6rem',
              padding: '0.5rem',
              background: '#f8fafc',
              border: '1px solid #e2e8f0',
              borderRadius: '8px',
            }}
          >
            <span
              style={{
                fontSize: '0.72rem',
                fontWeight: 700,
                textTransform: 'uppercase',
                color: '#64748b',
                padding: '0.3rem 0.5rem',
              }}
            >
              Approach:
            </span>
            {approaches.map((a) => {
              const isActive = a.id === activeApproach;
              return (
                <button
                  key={a.id}
                  onClick={() => setActiveApproach(a.id)}
                  style={{
                    padding: '0.35rem 0.75rem',
                    border: isActive ? '2px solid #3b82f6' : '1px solid #e2e8f0',
                    background: isActive ? '#eff6ff' : '#fff',
                    color: isActive ? '#1e40af' : '#334155',
                    fontWeight: isActive ? 700 : 500,
                    fontSize: '0.78rem',
                    borderRadius: '6px',
                    cursor: 'pointer',
                  }}
                >
                  {a.icon} {a.label}
                </button>
              );
            })}
          </div>

          {/* Selected approach code */}
          <pre className="code-block" style={{ marginBottom: '0.85rem' }}>
            <code>{current.code}</code>
          </pre>

          {/* Run / Schedule / Download */}
          <FileFormatRunner
            data={resourceData}
            slug={slug}
            schemaName="TerraformResource"
            tableName={`catalog.bronze.iac_resource_${scenario.id}`}
          />
        </div>
      )}
    </div>
  );
}

function TerraformAzure() {
  const [activeTab, setActiveTab] = useState('terraform');

  const currentTab = tabs.find((t) => t.key === activeTab);

  return (
    <div style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
      <div className="page-header">
        <h1>Terraform + Azure + Snowflake Integration</h1>
        <p>
          Infrastructure as Code, cloud integration, cross-platform data workflows, and local
          development patterns for Databricks.
        </p>
      </div>

      <div
        className="tabs"
        style={{ display: 'flex', gap: '8px', marginBottom: '24px', flexWrap: 'wrap' }}
      >
        {tabs.map((tab) => (
          <button
            key={tab.key}
            className={`tab ${activeTab === tab.key ? 'tab--active' : ''}`}
            onClick={() => setActiveTab(tab.key)}
            style={{
              padding: '10px 18px',
              border: activeTab === tab.key ? '2px solid #1a73e8' : '1px solid #ddd',
              borderRadius: '8px',
              background: activeTab === tab.key ? '#e8f0fe' : '#fff',
              color: activeTab === tab.key ? '#1a73e8' : '#333',
              cursor: 'pointer',
              fontWeight: activeTab === tab.key ? 600 : 400,
              fontSize: '14px',
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
            }}
          >
            {tab.label}
            <span
              className="badge"
              style={{
                background: activeTab === tab.key ? '#1a73e8' : '#888',
                color: '#fff',
                borderRadius: '12px',
                padding: '2px 8px',
                fontSize: '12px',
                fontWeight: 600,
              }}
            >
              {tab.badge}
            </span>
          </button>
        ))}
      </div>

      {currentTab && (
        <div>
          {currentTab.scenarios.map((scenario) => (
            <ScenarioCard key={scenario.id} scenario={scenario} />
          ))}
        </div>
      )}
    </div>
  );
}

export default TerraformAzure;
