# Solution Architecture Document (SAD)

## Databricks PySpark Data Automation Platform

### 1. Solution Overview

This platform provides a unified data automation environment supporting:
- **Data Ingestion**: 55 patterns for batch, streaming, CDC, API data
- **Data Processing**: Medallion architecture (Bronze/Silver/Gold)
- **ML/AI**: Model training, serving, explainability, fairness
- **Governance**: Unity Catalog, security, lineage, compliance
- **Visualization**: 55 chart/dashboard patterns
- **Testing**: 55 data quality validation scenarios
- **LLM/RAG**: Ollama, vector databases, retrieval pipelines

### 2. Solution Architecture

```
                    +---------------------------------+
                    |    Data Automation Platform      |
                    |    (React SPA - localhost:3000)  |
                    +---------------------------------+
                                   |
          +------------------------+------------------------+
          |                        |                        |
  +-------v--------+    +---------v--------+    +---------v--------+
  | Scenario Engine |    | Pipeline Engine  |    | Simulation Engine|
  | - 500+ patterns |    | - 20 pipelines   |    | - 10 simulators  |
  | - Search/Filter |    | - Stage tracking |    | - Data generators|
  | - Code display  |    | - Orchestration  |    | - Drift/Skew     |
  +----------------+    +------------------+    +------------------+
          |                        |                        |
  +-------v--------------------------------------------------------+
  |                    Databricks Workspace                         |
  |  +----------+  +----------+  +----------+  +----------+       |
  |  | Clusters |  | Notebooks|  | Jobs     |  | SQL WH   |       |
  |  +----------+  +----------+  +----------+  +----------+       |
  |                                                                 |
  |  +-----------+  +-----------+  +-----------+  +----------+    |
  |  | Unity     |  | Delta     |  | MLflow    |  | Feature  |    |
  |  | Catalog   |  | Lake      |  |           |  | Store    |    |
  |  +-----------+  +-----------+  +-----------+  +----------+    |
  +-----------------------------------------------------------------+
          |                        |                        |
  +-------v--------+    +---------v--------+    +---------v--------+
  | Cloud Storage   |    | Databases        |    | AI/LLM Services  |
  | - ADLS Gen2     |    | - PostgreSQL     |    | - Ollama         |
  | - AWS S3        |    | - MySQL          |    | - ChromaDB       |
  | - GCS           |    | - SQL Server     |    | - FAISS          |
  | - Snowflake     |    | - MongoDB        |    | - Redis Cache    |
  +----------------+    +------------------+    +------------------+
```

### 3. Data Flow Architecture

#### 3.1 Batch Processing Flow
```
Source DB --JDBC--> Landing Zone --Auto Loader--> Bronze --Transform--> Silver --Aggregate--> Gold --JDBC--> BI Tool
                                                   |                     |                     |
                                              Quarantine            Quality Check          Feature Store
                                              (bad records)         (validation)           (ML features)
```

#### 3.2 Streaming Processing Flow
```
Kafka/Event Hub --Structured Streaming--> Bronze --foreachBatch MERGE--> Silver --Trigger--> Gold
                                            |                              |
                                       Checkpoint                    Watermark-based
                                       (exactly-once)               dedup
```

#### 3.3 CDC Processing Flow
```
Source DB --Debezium--> Kafka --Spark Streaming--> Bronze CDC Events --DLT apply_changes--> Silver SCD2
                                                                                                |
                                                                                          Change Data Feed
                                                                                          (downstream consumers)
```

#### 3.4 ML Pipeline Flow
```
Gold Tables --Feature Engineering--> Feature Store --Training Set--> ML Pipeline --MLflow--> Model Registry
                                                                         |                        |
                                                                    Cross-validation          Model Serving
                                                                    Hyperopt tuning           REST Endpoint
```

#### 3.5 RAG Pipeline Flow
```
Documents --Chunking--> Embeddings --Vector Store--> Index
                                                       |
User Query --Embedding--> Similarity Search --> Retrieved Chunks --LLM (Ollama)--> Response
                              |                                        |
                         Pre-retrieval                           Post-retrieval
                         (query expansion, HyDE)                (reranking, MMR)
```

### 4. Deployment Architecture

#### 4.1 Databricks Deployment
```
Git Repo --> GitHub Actions --> Databricks Asset Bundles --> Workspace
                                       |
                                  Terraform
                                  (infrastructure)
```

#### 4.2 Environment Strategy
```
Development:  dev-workspace   --> dev-catalog   --> dev cluster (single-node)
Staging:      stg-workspace   --> stg-catalog   --> stg cluster (auto-scale)
Production:   prod-workspace  --> prod-catalog  --> prod cluster (auto-scale + Photon)
```

### 5. Integration Matrix

| System | Direction | Protocol | Frequency | Data Volume |
|--------|-----------|----------|-----------|-------------|
| PostgreSQL | Inbound | JDBC | Daily | 1-10 GB |
| MySQL | Inbound | JDBC | Hourly | 100 MB-1 GB |
| Kafka | Inbound | Streaming | Real-time | 10K events/sec |
| Event Hubs | Inbound | Streaming | Real-time | 5K events/sec |
| S3/ADLS | Bidirectional | REST | On-demand | 1-100 GB |
| Snowflake | Bidirectional | Spark Connector | Daily | 1-50 GB |
| REST APIs | Inbound | HTTPS | Scheduled | 10-100 MB |
| Power BI | Outbound | JDBC/DirectQuery | On-demand | Query-based |
| Tableau | Outbound | JDBC | On-demand | Query-based |
| MLflow | Internal | REST | Per experiment | Model artifacts |
| Ollama | Internal | HTTP | On-demand | Token-based |

### 6. Monitoring & Alerting

| Component | Tool | Metrics |
|-----------|------|---------|
| Spark Jobs | Spark UI / Ganglia | Duration, shuffle, GC |
| Data Quality | Custom Delta table | Null %, duplicates, freshness |
| Pipeline SLA | Job alerts | Completion time, failure rate |
| ML Models | MLflow | AUC drift, PSI, feature drift |
| Costs | System tables | DBU usage, cluster idle time |
| Governance | Audit logs | Access patterns, policy violations |

### 7. Disaster Recovery

| Component | RPO | RTO | Strategy |
|-----------|-----|-----|----------|
| Delta Tables | 0 (ACID) | < 1 hour | Time Travel + cross-region replication |
| Configurations | 0 | < 30 min | Git + Terraform state |
| ML Models | 0 | < 1 hour | MLflow model registry |
| Metadata | 0 | < 30 min | Unity Catalog (managed) |
| Landing Zone | Minutes | < 1 hour | Cloud storage replication |
