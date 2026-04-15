# Tech Stack — databrick_new

## Frontend

| Category | Technology | Version | Purpose |
|----------|-----------|---------|---------|
| Framework | React | 19.x | UI components |
| Routing | React Router | (planned) | Page navigation |
| Styling | Native CSS + CSS Variables | - | Design system |
| State | React Hooks (useState, useContext) | - | State management |
| Build | react-scripts (CRA) | 5.0.1 | Build toolchain |

## Code Quality

| Tool | Version | Purpose |
|------|---------|---------|
| ESLint | (via react-scripts) | Linting |
| Prettier | 3.x | Code formatting |
| Husky | 9.x | Git hooks |
| lint-staged | 16.x | Pre-commit checks |

## Testing

| Tool | Version | Purpose |
|------|---------|---------|
| Jest | (via react-scripts) | Unit tests |
| React Testing Library | 16.x | Component tests |
| Playwright | 1.59.x | E2E browser tests |
| CodeRabbit | (Claude plugin) | AI code review |
| SonarQube | (Claude plugin) | Static analysis |

## CI/CD

| Tool | Purpose |
|------|---------|
| GitHub Actions | Lint, test, build, E2E pipeline |
| Husky pre-commit | Local quality gate |

## Data Platform (Simulated)

| Technology | Purpose |
|-----------|---------|
| Databricks | Spark compute, notebooks |
| Apache Spark / PySpark | Data processing |
| Delta Lake | ACID storage |
| Snowflake | Data warehouse |
| Azure (ADLS, Key Vault) | Cloud storage, secrets |

## AI/ML (Reference Scenarios)

| Technology | Purpose |
|-----------|---------|
| MLflow | Model tracking |
| PyTorch / XGBoost | ML frameworks |
| SHAP / LIME | Explainable AI |
| Fairlearn / AIF360 | Responsible AI |
| Ollama / RAG | LLM integration |
| ChromaDB / FAISS | Vector databases |

## Infrastructure (Reference)

| Technology | Purpose |
|-----------|---------|
| Terraform | Infrastructure as Code |
| Docker | Containerization |
| Unity Catalog | Data governance |

## Browser Support

| Browser | Version |
|---------|---------|
| Chrome | Last 1 version |
| Firefox | Last 1 version |
| Safari | Last 1 version |
