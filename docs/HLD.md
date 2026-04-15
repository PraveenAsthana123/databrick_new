# High Level Design (HLD)

## Databricks PySpark Data Automation Platform

### 1. Executive Summary

A centralized data automation platform that provides 500+ production-ready PySpark/SQL scenarios for building end-to-end data pipelines on Databricks, with integrations to Azure, AWS, GCP, Snowflake, and AI/ML services.

### 2. System Context

```
+------------------+     +------------------+     +------------------+
|  External Sources|     | Data Automation  |     |  Consumers       |
|  - APIs          | --> | Platform         | --> |  - BI Tools      |
|  - Databases     |     | (React Frontend) |     |  - ML Models     |
|  - Files/Streams |     |                  |     |  - Dashboards    |
|  - IoT Devices   |     | +- Databricks -+ |     |  - Reports       |
+------------------+     | | Spark Engine | |     |  - APIs          |
                          | | Delta Lake   | |     +------------------+
+------------------+     | | Unity Catalog| |
|  Cloud Services  |     | | MLflow       | |     +------------------+
|  - Azure ADLS    | <-> | +--------------+ |     |  Governance      |
|  - AWS S3        |     |                  | --> |  - Audit Logs    |
|  - GCP GCS       |     | +- AI/LLM ----+ |     |  - Lineage       |
|  - Snowflake     |     | | Ollama       | |     |  - Access Control|
+------------------+     | | RAG Pipeline | |     |  - Data Quality  |
                          | | Vector DB    | |     +------------------+
                          | +--------------+ |
                          +------------------+
```

### 3. High Level Architecture

#### 3.1 Medallion Architecture (Data Flow)

```
Layer 0: LANDING ZONE
  - Cloud Storage (S3/ADLS/GCS/DBFS/Volumes)
  - Raw files, API responses, database dumps
  - Organized by: source/date/format

Layer 1: BRONZE (Raw)
  - Delta tables, append-only
  - Schema-on-read, metadata enrichment
  - _ingest_timestamp, _source_file columns
  - Quarantine for bad records

Layer 2: SILVER (Cleansed)
  - Schema enforcement, type casting
  - Deduplication, null handling
  - Joins, enrichment from multiple bronze tables
  - SCD Type 2 history tracking
  - Data quality validation

Layer 3: GOLD (Business-Ready)
  - Star schema (fact + dimension tables)
  - Aggregation tables, KPI metrics
  - ML feature tables (Feature Store)
  - BI-optimized views
  - Z-ORDER / Liquid Clustering
```

#### 3.2 Component Architecture

```
+---------------------------------------------------------------+
|                    React Frontend (SPA)                        |
|  +----------+ +----------+ +----------+ +----------+          |
|  | Dashboard| | Scenarios| | Pipelines| | Settings |          |
|  +----------+ +----------+ +----------+ +----------+          |
+---------------------------------------------------------------+
                              |
+---------------------------------------------------------------+
|                    Scenario Engine                             |
|  +----------+ +----------+ +----------+ +----------+          |
|  | Ingestion| | Modeling | | Testing  | | Governance|          |
|  | (55)     | | (55)     | | (55)     | | (55)     |          |
|  +----------+ +----------+ +----------+ +----------+          |
|  +----------+ +----------+ +----------+ +----------+          |
|  | ELT/CDC  | | Viz      | | XAI      | | RAG/LLM |          |
|  | (48)     | | (55)     | | (46)     | | (36)     |          |
|  +----------+ +----------+ +----------+ +----------+          |
+---------------------------------------------------------------+
                              |
+---------------------------------------------------------------+
|                    Data Platform                               |
|  +-----------+ +-----------+ +-----------+ +-----------+      |
|  | Databricks| | Snowflake | | Azure     | | Terraform |      |
|  | Spark     | | Warehouse | | ADLS/KV   | | IaC       |      |
|  | Delta Lake| | Snowpark  | | Event Hubs| | CI/CD     |      |
|  | UC        | | Stages    | | DevOps    | | DABs      |      |
|  +-----------+ +-----------+ +-----------+ +-----------+      |
+---------------------------------------------------------------+
```

### 4. Data Integration Points

| Source Type | Protocol | Landing Format | Bronze Table |
|-------------|----------|----------------|--------------|
| REST APIs | HTTPS | JSON | Delta (append) |
| PostgreSQL | JDBC | DataFrame | Delta (overwrite/merge) |
| MySQL | JDBC | DataFrame | Delta (merge) |
| SQL Server | JDBC | DataFrame | Delta (merge) |
| MongoDB | Spark Connector | DataFrame | Delta (append) |
| Kafka | Structured Streaming | JSON/Avro | Delta (streaming) |
| Event Hubs | Spark Connector | JSON | Delta (streaming) |
| S3/ADLS/GCS | Cloud Files (Auto Loader) | CSV/JSON/Parquet | Delta (streaming) |
| Snowflake | Spark Connector | DataFrame | Delta (overwrite) |
| FTP/SFTP | Paramiko | File | Delta (append) |

### 5. Security Architecture

```
+---------------------------+
|     Network Layer         |
|  VNet Injection           |
|  Private Endpoints        |
|  IP Access Lists          |
+---------------------------+
           |
+---------------------------+
|     Identity Layer        |
|  Service Principals       |
|  Managed Identities       |
|  RBAC / ABAC              |
+---------------------------+
           |
+---------------------------+
|     Data Layer            |
|  Row-Level Security       |
|  Column Masking           |
|  Dynamic Views            |
|  Encryption at Rest       |
+---------------------------+
           |
+---------------------------+
|     Governance Layer      |
|  Unity Catalog            |
|  Data Lineage             |
|  Audit Logging            |
|  Data Classification      |
+---------------------------+
```

### 6. Non-Functional Requirements

| Requirement | Target |
|-------------|--------|
| Availability | 99.9% (Databricks SLA) |
| Data Freshness | < 5 min for streaming, < 1 hour for batch |
| Query Performance | < 10s for Gold table queries |
| Scalability | Auto-scale clusters (2-16 workers) |
| Security | Unity Catalog governance, encryption at rest |
| Compliance | GDPR, SOC2, data retention policies |
| Recovery | Delta Time Travel (30 days), VACUUM 7 days |
| Monitoring | Spark UI, job alerts, data quality SLAs |

### 7. Technology Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage Format | Delta Lake | ACID, time travel, schema evolution |
| Catalog | Unity Catalog | Unified governance across workspaces |
| ML Platform | MLflow | Native integration, model registry |
| Streaming | Structured Streaming | Exactly-once, Auto Loader |
| IaC | Terraform | Multi-cloud, Databricks provider |
| LLM | Ollama + RAG | Local inference, privacy-first |
| Vector DB | ChromaDB/FAISS | Lightweight, embedded |
| CI/CD | Databricks Asset Bundles | Native deployment |
