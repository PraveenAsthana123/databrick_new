# Databricks PySpark Data Automation Platform

A comprehensive React-based simulation and reference platform for **Databricks, PySpark, Snowflake, and Azure** data engineering workflows. Provides **500+ real-world scenarios** with executable PySpark/SQL code covering the complete data lifecycle.

## Quick Start

```bash
# Install dependencies
npm install

# Start development server
npm start

# Open http://localhost:3000
```

## What This Platform Does

This is a **data automation scenario simulator** that can integrate with **any database** and covers:
- 55 Ingestion scenarios (CSV, JSON, JDBC, Kafka, CDC, APIs)
- 55 Modeling scenarios (ML, DL, NLP, Recommendations)
- 55 Unity Catalog governance scenarios
- 55 Visualization scenarios (Matplotlib, Plotly, Seaborn)
- 55 Data Testing scenarios (Quality, Schema, Great Expectations)
- 55 Security & Governance scenarios
- 48 ELT / SCD / CDC / DataFrame / Delta operations
- 46 XAI (Explainable/Responsible/Fairness AI) scenarios
- 36 RAG / Ollama / MCP / Vector DB scenarios
- 34 Terraform / Azure / Snowflake scenarios
- 20 End-to-end pipelines with stages
- 10 Simulation tools for testing

**Each scenario includes:** Input, Process, Output, Manual Process, Automation Code

## Architecture

See [docs/HLD.md](docs/HLD.md) for High Level Design and [docs/SAD.md](docs/SAD.md) for Solution Architecture.

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, Native CSS |
| Data Platform | Databricks, Apache Spark, Delta Lake |
| Cloud | Azure (ADLS, Key Vault), AWS (S3), GCP (GCS) |
| Warehouse | Snowflake |
| ML/AI | MLflow, PyTorch, XGBoost, HuggingFace |
| LLM | Ollama, RAG, MCP, ChromaDB, FAISS |
| Governance | Unity Catalog, Delta Sharing |
| IaC | Terraform |
| XAI | SHAP, LIME, Fairlearn, AIF360 |

## Project Structure

```
src/
  components/Sidebar.js      # Navigation (7 sections, 22 pages)
  pages/
    Dashboard.js              # Overview + stats
    Medallion.js              # Bronze/Silver/Gold
    LandingZone.js            # Data staging
    Ingestion.js              # 55 ingestion patterns
    Modeling.js               # 55 ML patterns
    UnityCatalog.js           # 55 governance patterns
    Visualization.js          # 55 visualization patterns
    ELTOperations.js          # ELT, SCD, CDC, DataFrame, Delta
    PipelineBuilder.js        # 20 pipelines with stages
    DataTesting.js            # 55 testing scenarios
    SecurityGovernance.js     # Security + governance + deployment
    XAI.js                    # Explainable/Responsible/Fairness AI
    RAGIntegration.js         # RAG, Ollama, MCP, Vector DBs
    TerraformAzure.js         # Terraform, Azure, Snowflake
    Clusters.js               # Compute management
    Notebooks.js              # Dev workspace
    Jobs.js                   # Scheduling
    SparkUI.js                # Performance monitoring
    DataStorage.js            # Storage options guide
    DownloadData.js           # Sample data generators
    SimulationTools.js        # Pipeline simulations
    Settings.js               # Configuration
  App.js                      # Lazy-loaded routing
  App.css                     # Design system (CSS variables)
docs/
  HLD.md                      # High Level Design
  LLD.md                      # Low Level Design
  SAD.md                      # Solution Architecture Document
  ARD.md                      # Architecture Reference Document
  C4Model.md                  # C4 Architecture Model
  CustomerDemo.md             # Customer Demo Scenarios
```

## Available Scripts

```bash
# Development
npm start              # Start dev server (http://localhost:3000)
npm run build          # Production build
npx serve -s build     # Serve locally

# Code Quality (PEP8-equivalent for JS)
npm run lint           # Check lint errors
npm run lint:fix       # Auto-fix lint errors
npm run format         # Format all files (Prettier)
npm run format:check   # Verify formatting

# Testing
npm test               # Unit tests (Jest)
npm run test:e2e       # E2E tests (Playwright)
npm run test:e2e:ui    # E2E tests with UI
npm run test:e2e:headed # E2E tests in browser

# Pre-merge validation
npm run validate       # Lint + format + test
npm run pre-merge      # Full validation + build
```

## Code Quality Pipeline

| Layer | Tool | Trigger |
|-------|------|---------|
| Format | Prettier | Pre-commit hook (auto) |
| Lint | ESLint + Prettier | Pre-commit hook (auto) |
| Test | Jest + Playwright | CI pipeline |
| Review | CodeRabbit + SonarQube | On PR |
| Build | react-scripts | CI pipeline |

## Setup for New Contributors

```bash
git clone <repo-url>
cd databrick_new
npm install          # Installs deps + Husky hooks
npm start            # Start development
```

## Documentation

- [High Level Design (HLD)](docs/HLD.md)
- [Low Level Design (LLD)](docs/LLD.md)
- [Solution Architecture (SAD)](docs/SAD.md)
- [Architecture Reference (ARD)](docs/ARD.md)
- [C4 Model](docs/C4Model.md)
- [Customer Demo Guide](docs/CustomerDemo.md)

## License

Internal use - Data Engineering Team
