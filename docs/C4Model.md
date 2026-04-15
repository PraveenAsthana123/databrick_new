# C4 Architecture Model

## Databricks PySpark Data Automation Platform

---

## Level 1: System Context Diagram

```
+-------------------+
|   Data Engineers   |
|   Data Scientists  |
|   Analysts         |
+---------+---------+
          |
          | Uses (Browser)
          v
+-------------------+          +-------------------+
|  Data Automation  |  JDBC/   |  Source Systems    |
|  Platform         | <------> |  - PostgreSQL      |
|  (React SPA)      |  API     |  - MySQL           |
|                   |          |  - MongoDB         |
|  500+ Scenarios   |          |  - REST APIs       |
|  22 Pages         |          |  - Kafka           |
+-------------------+          +-------------------+
          |
          | Deploys to / Runs on
          v
+-------------------+          +-------------------+
|  Databricks       |  Cloud   |  Cloud Services    |
|  Workspace        | <------> |  - Azure ADLS Gen2 |
|  - Spark Engine   |  Storage |  - AWS S3          |
|  - Delta Lake     |          |  - Google GCS      |
|  - Unity Catalog  |          |  - Snowflake       |
|  - MLflow         |          |  - Azure Key Vault |
+-------------------+          +-------------------+
          |
          | Serves
          v
+-------------------+
|  Consumers        |
|  - Power BI       |
|  - Tableau        |
|  - ML Endpoints   |
|  - REST APIs      |
+-------------------+
```

---

## Level 2: Container Diagram

```
+------------------------------------------------------------------+
|                    Data Automation Platform                        |
|                                                                    |
|  +---------------------+    +---------------------+              |
|  | React SPA           |    | Documentation       |              |
|  | (localhost:3000)     |    | (Markdown)          |              |
|  |                     |    |                     |              |
|  | - App.js (Router)   |    | - HLD.md            |              |
|  | - Sidebar (Nav)     |    | - SAD.md            |              |
|  | - 22 Page Components|    | - LLD.md            |              |
|  | - App.css (Design)  |    | - C4Model.md        |              |
|  +---------------------+    +---------------------+              |
|                                                                    |
+------------------------------------------------------------------+
          |                              |
          v                              v
+------------------+          +------------------+
| Databricks       |          | Cloud Storage    |
| Workspace        |          |                  |
| +- Clusters    -+|          | +- ADLS Gen2   -+|
| +- Notebooks   -+|          | +- AWS S3      -+|
| +- Jobs        -+|          | +- GCS         -+|
| +- SQL WH      -+|          | +- Snowflake   -+|
| +- Unity Cat   -+|          | +- UC Volumes  -+|
| +- MLflow      -+|          +------------------+
| +- Feature Str -+|
+------------------+          +------------------+
                              | AI/LLM Services  |
                              | +- Ollama      -+|
                              | +- ChromaDB    -+|
                              | +- FAISS       -+|
                              | +- Redis       -+|
                              +------------------+
```

---

## Level 3: Component Diagram

```
+------------------------------------------------------------------+
|                    React SPA Components                           |
|                                                                    |
|  App.js (Router + Lazy Loading)                                   |
|  +-------------------------------------------------------------+ |
|  |                                                               | |
|  |  +-- Overview --------+  +-- Scenarios -------+              | |
|  |  | Dashboard          |  | Ingestion (55)     |              | |
|  |  | Medallion          |  | Modeling (55)      |              | |
|  |  | Landing Zone       |  | Unity Catalog (55) |              | |
|  |  +--------------------+  | Visualization (55) |              | |
|  |                          | ELT/SCD/CDC (48)   |              | |
|  |  +-- Pipelines & AI --+  | Data Testing (55)  |              | |
|  |  | Pipeline Builder   |  +--------------------+              | |
|  |  | XAI/Fairness       |                                       | |
|  |  | RAG/Ollama/MCP     |  +-- Governance ------+              | |
|  |  +--------------------+  | Security/Gov (55)  |              | |
|  |                          | Terraform/Azure    |              | |
|  |  +-- Infrastructure --+  +--------------------+              | |
|  |  | Clusters           |                                       | |
|  |  | Notebooks          |  +-- Data & Tools ----+              | |
|  |  | Jobs               |  | Data Storage       |              | |
|  |  | Spark UI           |  | Download Data      |              | |
|  |  +--------------------+  | Simulation Tools   |              | |
|  |                          +--------------------+              | |
|  +-------------------------------------------------------------+ |
|                                                                    |
|  Sidebar.js (Navigation - 7 sections)                             |
|  App.css (Design System - CSS Variables)                          |
+------------------------------------------------------------------+
```

---

## Level 4: Code Diagram (Scenario Component Pattern)

```
Each scenario page follows this pattern:

+--------------------------------------------------+
| Page Component (e.g., Ingestion.js)              |
|                                                    |
|  State:                                           |
|  - selectedCategory: string                        |
|  - expandedId: number | null                       |
|  - searchTerm: string                              |
|  - viewMode: 'auto' | 'manual' (optional)         |
|                                                    |
|  Data:                                            |
|  - scenarios[]: { id, category, title, desc,      |
|                   code, manual? }                  |
|  - categories[]: derived from scenarios            |
|                                                    |
|  UI:                                              |
|  +----------------------------------------------+ |
|  | Search Bar + Category Filter + Count          | |
|  +----------------------------------------------+ |
|  | Scenario Card #1                              | |
|  |   [Badge: category] Title                     | |
|  |   Description                                  | |
|  |   [Expandable Code Block]                     | |
|  +----------------------------------------------+ |
|  | Scenario Card #2                              | |
|  |   ...                                          | |
|  +----------------------------------------------+ |
+--------------------------------------------------+
```

---

## Data Architecture (C4 - Data Flow)

```
Level 1: Sources --> Platform --> Consumers

Level 2: Landing Zone --> Bronze --> Silver --> Gold

Level 3:
  Bronze:
    - raw_csv, raw_json, raw_kafka_events
    - raw_api_data, raw_jdbc_data
    - quarantine (bad records)

  Silver:
    - customers (deduplicated, cleaned)
    - orders (enriched, validated)
    - products (standardized)
    - customers_scd2 (history tracking)

  Gold:
    - fact_sales (star schema)
    - dim_customer, dim_product, dim_date
    - daily_kpis, revenue_by_region
    - customer_features (Feature Store)
    - churn_predictions (ML output)
```
