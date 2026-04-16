# Data Architect — Complete Solution Guide

> End-to-end playbook covering Strategy → Architecture → Challenges → Solutions → Interview Positioning
> For Senior Architect / Principal / Director-level roles

---

## Table of Contents

1. [Data Strategy (20 Challenges)](#1-data-strategy)
2. [Data Maturity Model (5 Levels)](#2-data-maturity-model)
3. [Maturity Assessment Scorecard](#3-maturity-assessment)
4. [Enterprise Data Roadmap (4 Phases)](#4-data-roadmap)
5. [Architect Challenges (120)](#5-architect-challenges)
6. [Data FinOps Framework](#6-data-finops)
7. [Production Support Model (L1-L4)](#7-production-support)
8. [Interview Framework](#8-interview-framework)

---

## 1. Data Strategy

### The 10 Layers Every Strategy Must Cover

| Layer | Define |
|-------|--------|
| Vision | Why data matters to the business |
| Business Alignment | Use cases tied to measurable outcomes |
| Architecture | Target platform design (Lakehouse / Cloud) |
| Governance | Ownership, policies, RACI |
| Data Quality | Trust framework with KPIs |
| Security | PII protection, encryption, access |
| AI Readiness | ML / RAG capability |
| Operating Model | Teams, roles, stewardship |
| Roadmap | 12-24 month execution plan |
| Value Realization | ROI measurement framework |

### 20 Strategy Challenges — Quick Reference

| # | Challenge | Solution | Key Artifact |
|---|-----------|----------|--------------|
| 1 | No clear data strategy | Align to business KPIs | Strategy Document |
| 2 | Business-IT misalignment | Capability model | Use Case Catalog |
| 3 | No prioritization | Value vs complexity scoring | Prioritization Matrix |
| 4 | Poor enterprise DQ | DQ framework + ownership | DQ KPIs |
| 5 | No data ownership | Owner/Steward/Custodian roles | Governance RACI |
| 6 | Data silos | Domain-based architecture (Mesh) | Domain Architecture |
| 7 | Lack of standardization | Canonical model + glossary | Data Dictionary |
| 8 | No platform strategy | Unified lakehouse | Reference Architecture |
| 9 | High cost low ROI | Value realization framework | KPI Dashboard |
| 10 | No governance framework | Proactive operating model | Governance Model |
| 11 | Regulatory risk | Compliance in pipelines | Compliance Framework |
| 12 | PII exposure | Classification + masking + RBAC | Security Framework |
| 13 | Lack of data culture | Data literacy program | Training Plan |
| 14 | No self-service | Curated Gold layer + BI | Data Mart Strategy |
| 15 | Legacy constraints | Modernization roadmap | Migration Plan |
| 16 | No real-time | Streaming architecture | Event Architecture |
| 17 | AI not delivering | AI-ready data foundation | AI Strategy |
| 18 | No data product thinking | Data-as-product model | Product Catalog |
| 19 | No observability | Monitoring framework | Dashboards |
| 20 | No roadmap execution | Phased transformation | 12-24m Roadmap |

### Strategy Recommendations

| # | Recommendation | Why It Matters |
|---|----------------|----------------|
| 1 | **Tie every data initiative to a business KPI** | Prevents "tech for tech's sake"; guarantees ROI visibility |
| 2 | **Publish a 1-page data strategy** signed by CEO/CDO | Forces alignment across Business, IT, and Data teams |
| 3 | **Adopt Data-as-a-Product mindset** | Each domain owns quality, SLA, and consumer experience |
| 4 | **Build a use-case backlog** scored on Value x Complexity x Risk | Makes prioritization objective and defensible |
| 5 | **Invest 20% capacity in foundation** (platform, governance, DQ) | Feature-only strategies collapse within 18 months |
| 6 | **Define the operating model before tools** | Tools without ownership = shelfware |
| 7 | **Enforce canonical data model** at enterprise level | Eliminates 60%+ of integration friction |
| 8 | **Make governance enabling, not blocking** | Governance-as-code, not governance-by-committee |
| 9 | **Measure data maturity annually** | Creates compounding improvement discipline |
| 10 | **Treat AI readiness as a data problem, not a model problem** | 80% of AI failures are data failures |

---

## 2. Data Maturity Model

### 5 Maturity Levels

| Level | Name | Data | Pipelines | Governance | BI | AI |
|-------|------|------|-----------|-----------|----|----|
| 1 | **Ad Hoc** | Siloed | Manual | None | Excel | None |
| 2 | **Managed** | Centralized | Batch ETL | Minimal | Dashboards | Limited |
| 3 | **Defined** | Standardized | ELT + orchestration | Defined policies | Self-service | Early ML |
| 4 | **Advanced** | Trusted, governed | Scalable | Enforced (UC) | Enterprise-wide | Production ML |
| 5 | **Optimized** | Productized, real-time | Event-driven AI | Automated | Predictive | GenAI/RAG |

### Industry Distribution
```
L1-2:  50%  ████████████████████████████████░░░░░░░░░░░░░░░░
L3:    30%  ████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░
L4:    15%  █████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
L5:     5%  ███░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

### Capability Matrix

| Capability | L1 | L2 | L3 | L4 | L5 |
|------------|----|----|----|----|----|
| Ingestion | Manual | Batch | Standardized | Scalable | Real-time |
| Data Quality | None | Basic | Defined | Automated | Predictive |
| Governance | None | Minimal | Defined | Enforced | Automated |
| Architecture | None | Basic | Structured | Enterprise | Adaptive |
| BI | Manual | Reports | Self-service | Enterprise | Predictive |
| AI | None | Limited | Emerging | Production | Autonomous |

### Maturity Recommendations

| Target Level | Recommendation | Expected Outcome |
|--------------|----------------|------------------|
| **L1 -> L2** | Centralize data into one lakehouse; automate top-10 batch pipelines | Remove 50% manual effort, establish single source of truth |
| **L2 -> L3** | Introduce Silver layer + DQ framework + Unity Catalog | Trust score >80%, self-service BI unlocked |
| **L3 -> L4** | Enforce governance (masking/RLS/ABAC) + FinOps + ML platform | Cost -25%, SLA >95%, production ML running |
| **L4 -> L5** | Streaming-first + AI-assisted operations + data products | Real-time decisions, GenAI embedded in workflows |
| **All levels** | Do NOT skip levels — each level builds the foundation for the next | Skipping L2->L4 causes rework and cultural rejection |

**Architect's Rule:** Most enterprises try to leap from L2 to L4 because leadership wants AI. Resist this. Fix Silver/Gold + governance first, or your AI program will fail on bad data.

---

## 3. Maturity Assessment

### Scoring Model (1-5 per dimension)

| Score | Level |
|-------|-------|
| 1 | Ad Hoc (No structure) |
| 2 | Managed (Basic) |
| 3 | Defined (Standardized) |
| 4 | Advanced (Scalable) |
| 5 | Optimized (AI-driven) |

### 10 Assessment Dimensions (40 total criteria)

| Dimension | 4 Criteria |
|-----------|-----------|
| A. Ingestion | Source integration, Pipeline reliability, Schema handling, Data latency |
| B. Quality | Validation, Completeness, Reconciliation, Trust |
| C. Modeling | Data model, SCD handling, Reusability, Semantic layer |
| D. Governance | Ownership, Catalog, Lineage, Policy enforcement |
| E. Security | Access control, PII protection, Encryption, Audit logs |
| F. Platform | Platform, Scalability, Performance, Cost control |
| G. BI | Reporting, Access, KPI standardization, Usage |
| H. AI | ML usage, Data for AI, RAG/GenAI, Feedback loop |
| I. Observability | Monitoring, Alerting, SLA tracking, RCA |
| J. Culture | Literacy, Data-driven decisions, Collaboration, ROI |

### Interpretation

| Score | Interpretation |
|-------|---------------|
| 1.0 - 2.0 | Foundational / chaotic |
| 2.1 - 3.0 | Standardized |
| 3.1 - 4.0 | Scalable enterprise |
| 4.1 - 5.0 | AI-driven leader |

### Assessment Recommendations

| # | Recommendation | Detail |
|---|----------------|--------|
| 1 | **Run assessment every 6-12 months** | Track delta, not absolute score |
| 2 | **Include Business + IT + Data teams** | Each sees different gaps |
| 3 | **Score honestly, not aspirationally** | Inflated scores hide real risk |
| 4 | **Use external benchmark** | Gartner / internal peer group |
| 5 | **Publish dashboard to leadership** | Makes investment case concrete |
| 6 | **Focus on the 2 weakest dimensions first** | Highest leverage for overall uplift |
| 7 | **Pair each score with an evidence artifact** | No score without proof (pipeline count, DQ metric, catalog coverage) |
| 8 | **Target +1 level in 12 months per focus dimension** | Realistic pace; faster creates debt |

**Pro Tip:** Governance and Observability are usually the lowest-scoring dimensions — fix them first, they unlock progress in every other dimension.

---

## 4. Data Roadmap

### 4-Phase Enterprise Transformation

#### Phase 1 — Stabilize & Foundation (0-3 Months)
**Goal:** Centralize data, eliminate manual, stabilize pipelines

| Area | Actions |
|------|---------|
| Ingestion | Batch ETL pipelines |
| Storage | Bronze layer (raw data) |
| Orchestration | Airflow/Workflows |
| Quality | Basic validation rules |
| Monitoring | Logging + alerting |
| Security | Basic RBAC + encryption |

**Architecture:** `Sources → Ingestion → Bronze → Basic Reports`

**KPIs:**
- Pipeline success rate > 90%
- Data availability improved
- Manual effort reduced 30-40%

#### Phase 2 — Standardize & Scale (3-9 Months)
**Goal:** Improve quality, standardize models, enable self-service

| Area | Actions |
|------|---------|
| Modeling | Silver (clean layer) |
| Quality | DQ framework |
| Governance | Policies + ownership |
| BI | Dashboards + data marts |
| Integration | Canonical data model |

**Architecture:** `Bronze → Silver (clean) → Gold (basic marts)`

**KPIs:**
- Data quality score > 80%
- Report accuracy improved
- Self-service BI adoption

#### Phase 3 — Optimize & Govern (9-18 Months)
**Goal:** Scale platform, enforce governance, optimize cost

| Area | Actions |
|------|---------|
| Governance | Unity Catalog |
| Security | PII masking + RBAC/ABAC |
| Performance | Z-order, caching |
| Cost | FinOps model |
| Observability | Full monitoring + lineage |

**Architecture:** `Bronze → Silver → Gold → Governed + Secure + Optimized`

**KPIs:**
- SLA adherence > 95%
- Cost reduced 20-30%
- Data trust improved

#### Phase 4 — AI/GenAI Transformation (18-36 Months)
**Goal:** Enable AI/ML, build RAG, drive business value

| Area | Actions |
|------|---------|
| AI Platform | ML pipelines + feature store |
| RAG | Document ingestion + embeddings |
| Real-Time | Streaming pipelines |
| Data Products | Domain-based architecture |
| Automation | AI-assisted pipelines |

**Architecture:** `Gold → AI Models → RAG → Real-time → Business apps`

**KPIs:**
- AI adoption rate
- Prediction accuracy
- Business ROI

### Roadmap Recommendations

| # | Recommendation | Rationale |
|---|----------------|-----------|
| 1 | **Never skip Phase 1 foundation** | Accumulated tech debt surfaces at Phase 3 and kills the program |
| 2 | **Deliver one business win per phase** | Sustains executive sponsorship and funding |
| 3 | **Lock governance in Phase 2, not Phase 3** | Retrofitting UC/policies to existing tables costs 3x more |
| 4 | **Fund Phase 4 (AI) only after Phase 2 (quality) passes** | AI on bad data = public failure |
| 5 | **Assign a domain owner per phase deliverable** | Prevents platform becoming orphaned |
| 6 | **Publish quarterly roadmap review** | Keeps stakeholders aligned; catches drift early |
| 7 | **Include decommission items in every phase** | Legacy decommission funds future investment |
| 8 | **Build MVP + expand pattern** per use case | Faster ROI, less rework |
| 9 | **Reserve 15% for unplanned work** | Regulatory changes, incidents, priority shifts |
| 10 | **Review and re-baseline roadmap yearly** | Data strategies older than 18 months are stale |

---

## 5. Architect Challenges

### 120 Challenges Across 12 Categories

#### 5.1 Ingestion & Pipeline Reliability

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| Schema drift | No contract | Schema validation + evolution + alerts | Delta schema evolution | "We implemented schema contracts with validation at ingestion and allowed controlled evolution with alerts." |
| Late arriving data | Source latency | Watermark + reprocessing | Structured Streaming | "We handled late data using watermarking and built reconciliation jobs." |
| Duplicate ingestion | Re-runs | Idempotent design + merge | Delta MERGE | "We designed idempotent pipelines using merge and batch identifiers." |
| Partial pipeline failure | No checkpointing | Task-level recovery | Airflow retries, Spark checkpoints | "We enabled checkpointing so partial failures don't corrupt downstream." |
| Source system instability | Tight coupling | Source readiness + buffering | Airflow sensors, staging | "We decoupled ingestion using staging and readiness checks." |

**Recommendations — Ingestion**
- Adopt Auto Loader (`cloudFiles`) for all file-based sources — handles schema inference, evolution, exactly-once
- Design every pipeline to be **idempotent and restartable** from any point
- Enforce **source contracts** (schema, SLA, volume) with producers — no contract = no ingest
- Use **checkpointing** for streaming and **batch IDs** for batch — never timestamp-only logic
- Build a **reconciliation job** per critical pipeline (count, checksum, business-key parity)

#### 5.2 Data Quality & Trust

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| Data inconsistency | No reconciliation | Control totals + reconcile | DQ framework | "We implemented reconciliation pipelines with thresholds." |
| Duplicate records | No unique key | Business key + survivorship | Dedup logic | "We applied dedup using business keys in Silver." |
| Missing/invalid data | Weak validation | Centralized DQ | Great Expectations | "We enforced centralized validation and quarantined bad records." |
| Silent failures | No monitoring | Anomaly detection | Data observability | "We added anomaly detection on volume/schema/distribution." |
| Poor trust | No ownership | Stewardship model | Governance tagging | "We assigned data owners and built certification layers." |

**Recommendations — Data Quality**
- Apply the **DQ pyramid**: validate at Bronze (schema), enforce at Silver (business rules), certify at Gold (SLA)
- Use **DLT expectations** or **Great Expectations** — reject, quarantine, or warn based on severity
- Publish a **DQ dashboard** per domain (completeness, validity, freshness, uniqueness, consistency)
- Assign **data stewards per domain** — they own quality, not the platform team
- Create a **certification process**: Silver = clean, Gold = certified-for-business-use
- Introduce **"Bronze -> Quarantine"** branch for bad records — never silently drop

#### 5.3 Data Modeling

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| Wrong grain | Requirement unclear | Define grain upfront | Dimensional modeling | "We defined grain explicitly before modeling." |
| SCD complexity | No history strategy | Choose SCD type | Delta MERGE | "We used SCD Type 2 with effective dating." |
| Join explosion | Over-complex model | Pre-aggregation | Star schema | "We simplified joins using star schema." |
| Schema evolution | Tight coupling | Versioned schema | Schema registry | "We versioned schemas for backward compatibility." |
| No canonical model | Multiple interpretations | Enterprise data model | Modeling standards | "We created a canonical model aligned with business glossary." |

**Recommendations — Modeling**
- Start with **business grain**, not source schema — always
- Default to **Star Schema** for analytics; Data Vault only when auditability is paramount
- Standardize **SCD Type 2** for dimensions needing history; Type 1 for reference
- Publish **entity ownership** (who owns Customer, Product, Transaction)
- Maintain a **business glossary** tied to physical columns via Unity Catalog tags
- Enforce **naming conventions** (`dim_`, `fact_`, `agg_`, `stg_`) across all tables
- Build a **semantic layer** (dbt, LookML, or Databricks metric views) to prevent KPI drift

#### 5.4 Performance & Scalability

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| Slow queries | Poor partitioning | Partition by access pattern | Delta partitioning | "We optimized based on query access patterns." |
| Small file problem | Frequent small writes | Compaction | OPTIMIZE | "We resolved small files using compaction." |
| Data skew | Uneven keys | Repartition + salting | Spark optimization | "We handled skew using salting." |
| High compute cost | Inefficient queries | Tuning + caching | Spark SQL | "We reduced cost by optimizing queries." |
| Streaming lag | Backpressure | Autoscaling + tuning | Structured Streaming | "We tuned micro-batches and scaled clusters." |

**Recommendations — Performance**
- Enable **Liquid Clustering** on new tables — outperforms static partitioning for most workloads
- Run **OPTIMIZE + Z-ORDER** weekly on high-read tables; daily on hot tables
- Set **`delta.autoOptimize.optimizeWrite = true`** and **`autoCompact = true`** on write-heavy tables
- Pick partitions with **low cardinality + high query filter** frequency (usually date)
- Use **Photon** for SQL workloads — 2-3x faster, same cost envelope
- Profile before optimizing — **never guess** at performance bottlenecks
- Use **salting + AQE skew join** for known skewed keys; never rely on defaults alone

#### 5.5 Governance & Compliance

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| No lineage | Missing metadata | E2E lineage | Unity Catalog | "We enabled end-to-end lineage." |
| PII exposure | No classification | Tag + mask + restrict | UC masking | "We classified PII and enforced masking." |
| Over-permission | Broad roles | Least privilege | RBAC/ABAC | "We enforced least privilege." |
| No audit trail | No logging | Central audit logs | Audit tables | "We implemented centralized audit logging." |
| Regulatory | Manual processes | Automated enforcement | Governance framework | "We automated GDPR/SOX policy enforcement." |

**Recommendations — Governance**
- Adopt **Unity Catalog as the single governance plane** — do not split across tools
- Enforce **3-level namespace**: `catalog.schema.table` with environment separation (dev/staging/prod)
- Apply **tags for data classification** (PII, Confidential, Public) and drive policies from tags
- Use **Row-Level Security + Column Masking** as code (not manual views)
- Enable **system tables** for automatic audit, lineage, and access logs
- Require **every production table to have an owner** — no orphaned data
- Integrate **catalog -> BI -> ML feature store** so lineage is end-to-end

#### 5.6 Security

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| Data breach risk | Weak access | Least privilege + encryption | IAM + encryption | "We secured data using encryption and least privilege." |
| PII in logs | Raw logging | Log masking | Secure logging | "We sanitized logs to prevent PII exposure." |
| Key management | Scattered secrets | Central vault | Key Vault | "We centralized secrets using managed key vault." |
| External sharing | Uncontrolled | Secure sharing | Delta Sharing | "We implemented secure data sharing with audit." |
| Insider threat | No monitoring | Behavior tracking | Audit + anomaly | "We monitored access patterns for insider threats." |

**Recommendations — Security**
- Default to **deny-all**; grant explicit `USE CATALOG / SELECT` through groups, never users
- Store **all secrets in Key Vault / AWS Secrets Manager / Databricks Secrets** — never in code or notebooks
- **Mask PII at source** (Bronze -> Silver) — don't rely on BI-layer masking alone
- Use **Delta Sharing for external sharing** — never export CSVs to partners
- Log **every access to sensitive tables** and review monthly via anomaly detection
- Implement **customer-managed keys (CMK)** for regulated workloads
- Enforce **private link / no public IP** on all production workspaces

#### 5.7 Integration

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| Inconsistent IDs | No common key | MDM | Mapping tables | "We created golden ID using MDM." |
| Multi-system mismatch | Different schemas | Canonical model | Integration layer | "We standardized using canonical integration." |
| Batch vs real-time | Mixed architecture | Hybrid architecture | Lambda/Kappa | "We separated real-time and batch with unified serving." |
| Legacy limitation | No APIs | Wrapper ingestion | CDC / batch | "We wrapped legacy using CDC and batch extraction." |
| Data sync | Timing mismatch | Event-driven | Kafka/Event Hub | "We used event-driven architecture." |

**Recommendations — Integration**
- Build a **canonical integration layer** — do not let consumers pull from source systems directly
- Assign **golden IDs** through MDM for Customer, Product, Account — never let ID definitions differ
- Prefer **CDC over batch** for transactional sources (lower latency, smaller volumes)
- Use **Kafka / Event Hubs / Kinesis** for real-time; Autoloader for files; JDBC only as last resort
- Decouple systems via **event-driven architecture** — no point-to-point integrations beyond 5
- Version your **integration contracts** — break-changes go through change review
- Keep a **system-of-record registry** — one system is authoritative per entity

#### 5.8 AI / RAG / ML

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| Poor ML data quality | Weak pipeline | DQ before ML | Feature store | "We ensured feature quality via validation pipelines." |
| Feature inconsistency | Train/serve mismatch | Feature store | Databricks FS | "We used feature store for train/serve consistency." |
| RAG hallucination | Poor retrieval | Better retrieval + ranking | Vector DB | "We improved RAG via metadata filtering and ranking." |
| Embedding issues | Model inconsistency | Version embeddings | Versioned vector DB | "We versioned embeddings for consistency." |
| No feedback loop | Static model | Continuous learning | Monitoring + retraining | "We implemented feedback loops for improvement." |

**Recommendations — AI / ML / RAG**
- **Fix data quality before building models** — 80% of ML failure = data failure
- Use **Feature Store** (Databricks FS / Feast) to guarantee train-serve parity
- For **RAG**, invest in retrieval quality first: chunking, metadata, hybrid (BM25 + vector) search
- **Version embeddings** alongside models — re-embed on model upgrade
- Build **Evaluation Pipelines** (LLM-as-Judge, RAGAS) — deploy nothing without offline eval
- Keep **Human-in-the-Loop** for high-stakes decisions; log feedback into retraining
- Monitor **drift** (feature drift, prediction drift, concept drift) — retrain on trigger, not schedule
- Use **small fine-tuned models** before jumping to frontier LLMs — cheaper, often better

#### 5.9 Cost / FinOps

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| High compute cost | Always-on | Autoscaling | Job clusters | "We reduced cost using autoscaling job clusters." |
| Storage cost | Duplication | Lifecycle mgmt | Tiered storage | "We implemented retention + tiering policies." |
| Query cost | Inefficient | Optimization | Query tuning | "We optimized queries to reduce full scans." |
| No visibility | No tracking | Cost dashboards | FinOps tools | "We built cost dashboards per domain." |
| Over-engineering | Too many tools | Simplify | Reference architecture | "We standardized tools to avoid complexity." |

**Recommendations — Cost / FinOps**
- Use **Job Clusters** (not All-Purpose) for every scheduled workload — 50% cost reduction
- Enable **autoscaling + auto-termination (10-20 min)** on all clusters
- Enforce **cluster policies** — prevent oversized DBUs and GPU misuse
- Use **Spot/Preemptible** instances for batch, **on-demand** for streaming
- **Tag every resource** (team, project, env, cost-center) — untagged = rejected
- Publish a **cost chargeback dashboard** monthly per domain
- Delete data aggressively — **Vacuum + retention policies** on Delta tables
- Right-size **storage tiers** (hot/cool/archive) based on access patterns

#### 5.10 Observability & Operations

| Challenge | Root Cause | Solution | Tool | Interview Answer |
|-----------|-----------|----------|------|------------------|
| No monitoring | No metrics | Central monitoring | Observability stack | "We implemented centralized monitoring." |
| Hard debugging | Distributed | Traceability | Lineage + logs | "We used lineage and structured logs." |
| No alerting | No thresholds | Alert system | Slack/Email | "We added alerts for SLA and anomaly breaches." |
| No SLA tracking | Undefined SLAs | SLA framework | Airflow SLA | "We defined and tracked SLAs for all pipelines." |
| Repeated failures | No RCA | Root cause process | Incident mgmt | "We introduced RCA and preventive fixes." |

**Recommendations — Observability**
- Emit **structured logs + metrics + traces** from every pipeline (OpenTelemetry)
- Publish **5 Golden Signals per pipeline**: latency, volume, freshness, error rate, cost
- Define **SLOs per data product** (e.g., 99% of Gold tables refreshed by 7am)
- Build **data observability** (Monte Carlo / Lightup / custom) — detect anomaly before consumers do
- Make **lineage mandatory** — no Gold table without documented upstream lineage
- Tie **alerts to on-call rotation** (PagerDuty/Opsgenie) — route by domain ownership
- Run **blameless RCA** after every P1/P2; track preventive action closure rate

---

## 6. Data FinOps

### Framework — 5 Pillars

| Pillar | Focus |
|--------|-------|
| **Visibility** | Cost dashboards by team/pipeline/domain |
| **Allocation** | Tagging (project, domain, environment) |
| **Optimization** | Query tuning, autoscaling |
| **Governance** | Policies for compute/storage |
| **Culture** | Cost-aware engineering |

### Cost KPIs

| KPI | Target |
|-----|--------|
| Cost per pipeline run | ↓ trend |
| Cost per TB processed | ↓ |
| Idle compute % | < 10-15% |
| Storage growth rate | Controlled |
| Cost per ML prediction | ↓ |
| Data reuse % | ↑ |
| Cost vs business value | ROI positive |

### FinOps Maturity (1-5)

1. No visibility → 2. Basic tracking → 3. Cost dashboards → 4. Optimization + governance → 5. AI-driven optimization

### FinOps Recommendations

| Lever | Recommendation | Typical Savings |
|-------|----------------|-----------------|
| **Compute** | Job clusters + autoscaling + auto-termination | 30-50% |
| **Compute** | Use Photon for SQL, Serverless for ad-hoc | 20-30% |
| **Compute** | Spot/Preemptible for batch workloads | 60-70% on those jobs |
| **Storage** | Lifecycle tiering (hot -> cool -> archive) | 40-60% on cold data |
| **Storage** | Vacuum + retention on Delta tables | 20-40% |
| **Query** | OPTIMIZE + Z-ORDER + liquid clustering | 30-50% on read cost |
| **Query** | Materialized views for repeated aggregations | 50-80% on dashboards |
| **AI** | Cache embeddings + prompt caching | 30-60% on LLM cost |
| **AI** | Use smaller fine-tuned models when possible | 10x cheaper vs frontier |
| **Governance** | Cluster policies (max DBU, instance types) | Prevents runaway cost |

**Architect's Playbook for FinOps Program:**
1. **Month 1** — Instrument: tags on every resource, cost dashboard per domain
2. **Month 2** — Publish baseline: cost per pipeline, per TB, per query, per prediction
3. **Month 3** — Attack top-10 cost drivers (80/20 rule)
4. **Month 4-6** — Governance: cluster policies, query limits, chargeback
5. **Quarterly** — Optimization reviews with each domain owner
6. **Ongoing** — Cost-aware culture: show cost in PR reviews, notebook metadata, pipeline docs

---

## 7. Production Support

### 4-Level Support Model

| Level | Role | Focus | SLA |
|-------|------|-------|-----|
| **L1** | Operations / Support | Monitoring, triage | Minutes |
| **L2** | Data Engineer | Debug pipelines | Hours |
| **L3** | Senior Engineer / Architect | RCA + permanent fix | 1-2 days |
| **L4** | Platform / Vendor / SME | Deep system issues | Depends |

### End-to-End Flow

```
Alert → L1 (Triage) → L2 (Fix pipeline/data) → L3 (RCA & redesign) → L4 (Platform/vendor)
```

### What Strong Architects Add

| Area | Best Practice |
|------|--------------|
| Observability | Monitoring + alerts |
| Idempotency | Safe reruns |
| Data quality | Automated checks |
| Governance | Lineage + ownership |
| Automation | Self-healing pipelines |
| RCA | Prevent repeat issues |

### Production Support Recommendations

| # | Recommendation | Outcome |
|---|----------------|---------|
| 1 | **Runbook per pipeline** (symptoms, first-aid, escalation) | L1 resolves 60%+ without escalation |
| 2 | **Idempotent + restartable design** (batch IDs, checkpoints, MERGE) | Reruns are safe, no duplicates |
| 3 | **Alert on leading indicators**, not just failures (latency, volume drop, DQ score) | Catch issues before SLA miss |
| 4 | **Clear escalation matrix** (L1 -> L2 -> L3 -> L4 with SLA per tier) | No "lost in queue" incidents |
| 5 | **Post-incident RCA within 48 hours** with preventive actions tracked | Same issue doesn't recur |
| 6 | **Auto-retry with exponential backoff** on transient failures | 40%+ self-heal rate |
| 7 | **Circuit breakers on external dependencies** | Cascading failures avoided |
| 8 | **Weekly ops review** with top-5 incidents + fix status | Leadership visibility |
| 9 | **Error budget per data product** (SLO compliance) | Data product accountability |
| 10 | **Blameless culture + game days** (simulated outages quarterly) | Team readiness |
| 11 | **Separate prod from dev clusters and catalogs** | No accidental prod impact |
| 12 | **Enable Time Travel + table-level snapshots** for recovery | Restore in minutes, not hours |

**Gold Standard Ops Flow:**
```
Detect (observability) -> Triage (L1 runbook) -> Fix (L2/L3) 
  -> Verify (reconciliation) -> RCA (blameless) -> Prevent (automation)
  -> Measure (SLO, MTTR, recurrence %) -> Improve (next sprint)
```

---

## 8. Interview Framework

### Director-Level Answer Template

**For any architecture question, structure your answer as:**

1. **Context** (1 sentence) — What was the situation?
2. **Challenge** (1 sentence) — What was the problem?
3. **Approach** (2-3 sentences) — How did you solve it?
4. **Outcome** (1 sentence) — What was the measurable result?

### Master Interview Answers

**Q: "How do you assess data maturity?"**
> "I use a structured maturity model across ingestion, quality, governance, platform, and AI readiness. I score each dimension 1-5, identify gaps, and define a phased roadmap to move toward a scalable, AI-driven platform."

**Q: "How do you define a data roadmap?"**
> "I define a phased roadmap starting with data foundation, followed by standardization, governance, and finally AI enablement. Each phase delivers measurable business value."

**Q: "How do you manage data platform cost?"**
> "I implement a FinOps model with cost visibility, tagging, and chargeback. Then optimize compute using autoscaling and query tuning, reduce storage via lifecycle policies, and control AI costs through caching and model selection."

**Q: "How do you handle production incidents?"**
> "We structured production support into L1-L4. L1 handled monitoring and triage, L2 resolved pipeline and data issues, L3 focused on root cause and long-term fixes, and L4 handled platform issues. We also implemented observability, idempotency, and automated checks to reduce recurring incidents."

**Q: "What makes a strong data strategy?"**
> "A strong data strategy aligns to business outcomes, defines clear ownership, establishes governance, enables self-service, builds AI readiness, and tracks ROI. We executed this via a phased roadmap with measurable KPIs at each stage."

### What Separates Senior from Director Thinking

| Senior Architect | Director-Level Thinking |
|------------------|-------------------------|
| Solves technical problems | Aligns technology to business outcomes |
| Designs pipelines | Designs data operating model |
| Optimizes queries | Optimizes value per dollar spent |
| Builds governance | Embeds governance into culture |
| Monitors pipelines | Tracks business KPIs |
| Responds to incidents | Prevents incidents via maturity |
| Completes projects | Builds data products with SLAs |

---

## Summary — The Data Architect Mindset

> **Strategy → Architecture → Execution → Governance → Value**

Every decision should answer:
1. Does this align with business outcomes?
2. Is this governed and secure?
3. Does it scale?
4. Can we measure ROI?
5. Does it improve data maturity?

**Strong architects don't just build pipelines — they build platforms that deliver measurable business value at enterprise scale.**

---

## 9. Master Recommendations — The Architect's Playbook

### The Top 25 Commandments

| # | Recommendation | Category |
|---|----------------|----------|
| 1 | **Tie every data asset to a business KPI** | Strategy |
| 2 | **Measure maturity annually across 10 dimensions** | Strategy |
| 3 | **Never skip Phase 1 foundation for AI ambition** | Roadmap |
| 4 | **Adopt Medallion (Bronze/Silver/Gold) universally** | Architecture |
| 5 | **Make every pipeline idempotent and restartable** | Ingestion |
| 6 | **Enforce source contracts — no contract, no ingest** | Ingestion |
| 7 | **Quarantine bad data, never silently drop it** | Quality |
| 8 | **Use DLT expectations or Great Expectations for DQ** | Quality |
| 9 | **Model from business grain, not source schema** | Modeling |
| 10 | **SCD2 for dimensions needing history, SCD1 for reference** | Modeling |
| 11 | **Liquid Clustering + OPTIMIZE/Z-ORDER for read perf** | Performance |
| 12 | **Photon for SQL, autoscaling + auto-termination always** | Performance |
| 13 | **Unity Catalog as single governance plane** | Governance |
| 14 | **Tag-based policies: PII / Confidential / Public** | Governance |
| 15 | **Row-level security + column masking as code** | Security |
| 16 | **All secrets in Key Vault; never in code/notebooks** | Security |
| 17 | **Canonical integration layer; MDM for golden IDs** | Integration |
| 18 | **Event-driven > point-to-point beyond 5 systems** | Integration |
| 19 | **Feature Store for train/serve parity** | AI/ML |
| 20 | **Data quality BEFORE model quality** | AI/ML |
| 21 | **Job clusters + tags + chargeback for FinOps** | Cost |
| 22 | **Cluster policies prevent runaway cost** | Cost |
| 23 | **5 Golden Signals per pipeline: latency, volume, freshness, errors, cost** | Observability |
| 24 | **Runbooks + idempotency + blameless RCA** | Ops |
| 25 | **Build data products with owners, SLAs, and consumers** | Culture |

### Anti-Patterns to Avoid

| Anti-Pattern | Why It Hurts | Do This Instead |
|--------------|--------------|-----------------|
| All-Purpose clusters for scheduled jobs | 2x cost, no isolation | Job clusters per workflow |
| Allow-all access + manual revoke | Audit nightmare | Deny-default + group-based grants |
| Static partitioning without profiling | Small files + skew | Liquid clustering + profile first |
| Silent fallback on external failures | Hides real problems | Fail loud, circuit break, alert |
| Governance-by-committee | Months to approve anything | Governance-as-code, automated |
| Single big-bang migration | 18-month failure cycle | Phased migration, strangler fig |
| AI pilots on bad data | Public failure, lost trust | Fix data first, model second |
| Platform-first without domain ownership | Orphaned tables, no trust | Domain ownership before tooling |
| Centralized ingestion team as bottleneck | 6-week lead time per source | Self-service ingestion patterns |
| Copying data across systems | Sync issues, cost, governance blind spots | Delta Sharing, single source of truth |

### 90-Day Quick-Win Recommendations (For a New Architect)

| Day | Action |
|-----|--------|
| **Day 1-15** | Run maturity assessment across 10 dimensions; publish baseline + top-5 gaps |
| **Day 16-30** | Stand up Unity Catalog with 3-level namespace; migrate top-10 tables |
| **Day 31-45** | Enforce cluster policies, tags, auto-termination; publish cost dashboard |
| **Day 46-60** | Implement DQ framework on top-5 critical pipelines; add reconciliation |
| **Day 61-75** | Define SLOs for top-10 Gold tables; wire alerts + runbooks |
| **Day 76-90** | Publish 18-month roadmap with phased investments and measurable KPIs |

### Decision Framework — "Should I Build This?"

Ask every request these 5 questions:

1. **Business value** — What KPI improves? By how much?
2. **Ownership** — Who owns the data product long-term?
3. **Alignment** — Does it fit the target architecture / roadmap?
4. **Cost** — What is TCO over 3 years? ROI?
5. **Risk** — Security / compliance / operational blast radius?

If any answer is weak, **push back or reshape the request** — this is the architect's job.

### The 5 Architect Values

1. **Outcomes over outputs** — ship measurable business value, not dashboards
2. **Simplicity over novelty** — proven patterns beat shiny tools
3. **Ownership over coverage** — one accountable owner > five aware teams
4. **Automation over process** — governance-as-code, policy-as-code, ops-as-code
5. **Culture over controls** — shared standards beat enforcement

---

*End of Guide — From Strategy to Execution to Value*
