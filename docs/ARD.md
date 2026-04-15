# Architecture Reference Document (ARD)

## Databricks PySpark Data Automation Platform

### 1. Architecture Principles

| # | Principle | Description |
|---|-----------|-------------|
| 1 | Medallion Architecture | All data flows through Landing -> Bronze -> Silver -> Gold |
| 2 | Delta Lake First | All tables use Delta format for ACID, time travel, schema evolution |
| 3 | Unity Catalog Governance | All objects managed under catalog.schema.table namespace |
| 4 | Automation Over Manual | Every manual process has a corresponding automation script |
| 5 | Schema Enforcement | Schema validated at Silver layer, schema-on-read at Bronze |
| 6 | Idempotent Operations | All pipelines can be safely re-run without duplicates |
| 7 | Security by Design | Encryption, RBAC, masking, row-level security from day 1 |
| 8 | Observable Pipelines | Every operation logged, monitored, and alertable |

### 2. Reference Architecture

#### 2.1 Data Ingestion Patterns

| Pattern | Use Case | Technology | Example |
|---------|----------|------------|---------|
| Batch File | Scheduled file drops | Auto Loader (cloudFiles) | CSV/JSON/Parquet -> Bronze |
| JDBC Extract | Database replication | spark.read.jdbc | PostgreSQL -> Bronze |
| Streaming | Real-time events | Structured Streaming | Kafka -> Bronze |
| CDC | Change tracking | Debezium + DLT | MySQL CDC -> Silver SCD2 |
| API Pull | External data | requests + Spark | REST API -> Bronze |
| COPY INTO | Idempotent batch | COPY INTO command | Landing -> Bronze |

#### 2.2 Data Processing Patterns

| Pattern | Layer | Technology | Description |
|---------|-------|------------|-------------|
| Append-Only | Bronze | Delta write(append) | Raw data, no transforms |
| Dedup + Clean | Silver | dropDuplicates + filter | Remove bad/duplicate records |
| MERGE Upsert | Silver | DeltaTable.merge | Update existing, insert new |
| SCD Type 2 | Silver | MERGE + history columns | Track changes over time |
| Star Schema | Gold | SQL aggregations | Fact + Dimension tables |
| Feature Engineering | Gold | Feature Store | ML-ready features |

#### 2.3 ML/AI Patterns

| Pattern | Technology | Use Case |
|---------|------------|----------|
| Batch Training | PySpark MLlib | Classification, Regression |
| Distributed Training | TorchDistributor | Deep Learning |
| AutoML | Databricks AutoML | Quick model selection |
| Hyperparameter Tuning | Hyperopt + SparkTrials | Parallel search |
| Model Serving | MLflow Serving | REST endpoint |
| Feature Store | Feature Engineering Client | Reusable features |
| RAG | Ollama + ChromaDB | Document Q&A |
| XAI | SHAP + LIME | Model explainability |

#### 2.4 Governance Patterns

| Pattern | Technology | Description |
|---------|------------|-------------|
| Access Control | Unity Catalog GRANT/REVOKE | Table/schema/catalog permissions |
| Row Security | Row Filter Functions | Per-group row access |
| Column Masking | Mask Functions | PII protection |
| Data Lineage | System Tables | Auto-tracked table/column lineage |
| Audit Logging | system.access.audit | Who accessed what, when |
| Data Sharing | Delta Sharing | Cross-org data access |
| Data Quality | DLT Expectations | Declarative quality rules |

### 3. Standard Components

#### 3.1 Cluster Configurations

| Type | Workers | Instance | Use Case |
|------|---------|----------|----------|
| Dev/Test | Single Node | DS3_v2 | Development, prototyping |
| ETL | 4-12 (auto) | D4s_v3 | Batch processing |
| ML Training | 4-16 (auto) | NC6s_v3 (GPU) | Model training |
| Streaming | 2-6 (auto) | D8s_v3 | Real-time processing |
| SQL Analytics | Serverless | - | BI queries, dashboards |

#### 3.2 Storage Layout

```
Unity Catalog:
  my_catalog/
    landing/
      raw_files (Volume)
      csv_files (Volume)
      json_files (Volume)
    bronze/
      raw_orders (Table)
      raw_customers (Table)
      quarantine (Table)
    silver/
      orders (Table)
      customers (Table)
      customers_scd2 (Table)
    gold/
      fact_sales (Table)
      dim_customer (Table)
      daily_kpis (Table)
      customer_features (Feature Table)
    ml_models/
      churn_predictor (Model)
    audit/
      data_quality_log (Table)
      operation_log (Table)
```

#### 3.3 Job Scheduling

| Job | Schedule | Type | Cluster |
|-----|----------|------|---------|
| Daily ETL | 06:00 AM daily | Notebook | ETL |
| CDC Sync | Every hour | Streaming | Streaming |
| ML Retrain | Weekly Sunday 2 AM | Notebook | ML Training |
| Quality Check | 08:00 AM daily | Notebook | Dev/Test |
| Gold Refresh | 07:00 AM daily | SQL | ETL |
| Table Optimization | 03:00 AM daily | SQL | ETL |

### 4. Integration Standards

#### 4.1 Secret Management
- All credentials stored in Databricks Secret Scopes
- Backed by Azure Key Vault or AWS Secrets Manager
- Never hardcode passwords, tokens, or keys
- Access via `dbutils.secrets.get("scope", "key")`

#### 4.2 Naming Conventions

| Object | Convention | Example |
|--------|-----------|---------|
| Catalog | project_name | analytics |
| Schema | layer_name | bronze, silver, gold |
| Table | entity_name | orders, customers |
| Column | snake_case | customer_id, order_date |
| Job | purpose_schedule | daily_etl_pipeline |
| Cluster | team_purpose | de_etl_cluster |
| Secret Scope | env_purpose | prod_database |

#### 4.3 Error Handling Standard
1. All external calls wrapped in retry with exponential backoff
2. Failed records quarantined, never dropped silently
3. Pipeline failures logged to audit table + alert triggered
4. Streaming checkpoints for exactly-once processing
5. Delta time travel for data recovery

### 5. Supported Data Types

| Type | Formats | Landing Path | Bronze Format |
|------|---------|--------------|---------------|
| Structured | CSV, Parquet, Avro, ORC | /landing/csv/ | Delta |
| Semi-structured | JSON, XML | /landing/json/ | Delta |
| Images | JPG, PNG, PDF | /landing/images/ | Delta (binary) |
| Text | TXT, DOC | /landing/text/ | Delta |
| Logs | App/access logs | /landing/logs/ | Delta |
| Streaming | Kafka/EventHub | N/A (direct) | Delta (streaming) |
