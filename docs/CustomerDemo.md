# Customer Demo Guide

## Databricks PySpark Data Automation Platform

### Pre-Demo Checklist

1. Start the app: `npm start` (http://localhost:3000)
2. Ensure browser is full-screen
3. Have Databricks workspace open in another tab (optional)

---

## Demo Flow (30 minutes)

### Demo 1: Platform Overview (5 min)

**Page: Dashboard**
- Show the platform overview with 500+ scenarios
- Explain Medallion Architecture flow diagram
- Highlight platform components (Spark, Delta Lake, Unity Catalog, MLflow)
- Show the data flow ASCII architecture

**Talking Points:**
- "This platform covers the complete data lifecycle"
- "Every scenario has real, production-ready PySpark code"
- "Supports CSV, JSON, images, text, logs, and streaming data"

---

### Demo 2: Data Ingestion (5 min)

**Page: Ingestion**
- Show 55 ingestion scenarios
- Filter by category: "Batch - File" -> show CSV ingestion
- Filter by "Streaming" -> show Kafka scenario
- Filter by "CDC" -> show Debezium CDC
- Filter by "API" -> show REST API ingestion
- Expand a scenario to show the code

**Key Scenarios to Demo:**
- #1: CSV File Ingestion (simple, relatable)
- #24: Auto Loader (Databricks differentiation)
- #37: CDC with Delta MERGE (enterprise pattern)
- #21: Kafka Streaming (real-time)

---

### Demo 3: Medallion Architecture (3 min)

**Page: Medallion Architecture**
- Show the visual flow: Landing -> Bronze -> Silver -> Gold
- Click "Bronze" tab -> show Auto Loader code
- Click "Silver" tab -> show SCD Type 2 code
- Click "Gold" tab -> show Star Schema code

**Talking Points:**
- "Bronze is raw, Silver is clean, Gold is business-ready"
- "Each layer adds value and governance"

---

### Demo 4: ML Modeling (5 min)

**Page: Modeling**
- Show 55 ML scenarios
- Filter "Classification" -> show Random Forest
- Filter "MLflow" -> show experiment tracking
- Filter "Feature Engineering" -> show Feature Store
- Filter "Deep Learning" -> show HuggingFace Transformers

**Key Scenarios:**
- #2: Random Forest Classifier
- #36: MLflow Experiment Tracking
- #30: Feature Store
- #51: AutoML

---

### Demo 5: Data Quality & Testing (3 min)

**Page: Data Testing**
- Show 55 testing scenarios
- Filter "Schema" -> schema validation
- Filter "Great Expectations" -> integration
- Filter "Quality" -> null checks, outlier detection

---

### Demo 6: Governance & Security (3 min)

**Page: Unity Catalog**
- Show catalog/schema/table management
- Show access control (GRANT/REVOKE)
- Show row-level security
- Show column masking
- Toggle "Manual" vs "Auto" view

**Page: Security & Governance**
- Show encryption, RBAC, compliance tabs

---

### Demo 7: Pipeline Builder (3 min)

**Page: Pipeline Builder**
- Show 20 end-to-end pipelines
- Expand "CSV -> Bronze -> Silver" -> show stage flow
- Expand "Kafka Streaming" -> show real-time pipeline
- Show pipeline stages as visual flow (stage badges with arrows)

---

### Demo 8: AI/LLM & XAI (3 min)

**Page: RAG / Ollama / MCP**
- Show RAG pipeline setup
- Show Ollama local LLM integration
- Show vector database setup (ChromaDB)

**Page: XAI / Fairness AI**
- Show SHAP values explanation
- Show bias detection
- Show fairness metrics

---

## Quick Demo Scenarios (5-minute version)

If you only have 5 minutes:

1. **Dashboard** (30s) -> show overview
2. **Ingestion** (1m) -> search "Kafka" -> show streaming code
3. **Medallion** (30s) -> show Bronze/Silver/Gold flow
4. **Pipeline Builder** (1m) -> expand "API -> Bronze -> Silver -> Gold"
5. **Unity Catalog** (1m) -> show governance + row-level security
6. **Modeling** (1m) -> show MLflow tracking + Feature Store

---

## Demo Data

Use the **Download Data** page to generate sample datasets:
- E-Commerce Orders (500K rows)
- Customer Profiles (100K rows)
- IoT Sensor Data (2M rows)
- Financial Transactions (5M rows)
- Web Access Logs (1M rows)

Use the **Simulation Tools** page to:
- Simulate streaming events
- Simulate CDC events
- Simulate data quality issues
- Simulate schema evolution
- Simulate model drift

---

## Q&A Preparation

| Question | Answer |
|----------|--------|
| "Can this connect to our database?" | "Yes, 55 ingestion patterns cover PostgreSQL, MySQL, SQL Server, Oracle, MongoDB, Snowflake, and any JDBC source" |
| "How does it handle real-time?" | "Structured Streaming with Kafka/Event Hubs, exactly-once processing with checkpoints" |
| "What about data governance?" | "Unity Catalog with row-level security, column masking, data lineage, and audit logging" |
| "Does it support our cloud?" | "Azure ADLS, AWS S3, GCP GCS, plus Snowflake integration" |
| "What about ML/AI?" | "55 ML scenarios, MLflow tracking, Feature Store, AutoML, plus RAG with Ollama" |
| "How do we deploy?" | "Terraform + Databricks Asset Bundles, GitHub Actions CI/CD" |
