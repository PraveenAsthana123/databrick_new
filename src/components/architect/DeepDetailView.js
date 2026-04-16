import React from 'react';

/**
 * DeepDetailView — renders a deep, architect-level breakdown for any challenge / topic.
 *
 * Accepts a shallow input (title/rootCause/solution/artifacts/interviewAnswer)
 * and produces a rich, structured view with:
 *   1. Challenge Deep-Dive (elaborated)
 *   2. Step-by-Step Action Sequence (numbered)
 *   3. Architect Recommendations (bulleted)
 *   4. Tools & Patterns
 *   5. Real Impact / Metrics
 *   6. Interview Answer (highlighted quote)
 *
 * The elaboration is keyword-driven so every entry gets depth without
 * hand-authoring 200+ long-form writeups.
 */

// ── Keyword → context map (lightweight, extensible) ─────────────────────
const KEYWORD_MAP = [
  {
    k: ['schema', 'drift', 'contract'],
    domain: 'ingestion',
    elaborate:
      'Schema drift occurs when source systems add, remove, or change columns without coordinating with downstream consumers. In large enterprises with 50-500+ sources, this breaks pipelines silently and erodes trust in Silver/Gold layers. The architect must move the organization from reactive schema-fix mode to proactive contract governance.',
  },
  {
    k: ['late', 'watermark'],
    domain: 'ingestion',
    elaborate:
      'Late-arriving data is inevitable in distributed systems — clock skew, mobile network delays, third-party source lag, and cross-region replication mean events arrive after their event-time bucket has closed. Without watermarks and reprocessing strategies, results diverge from source-of-truth and regulatory reports become unreliable.',
  },
  {
    k: ['duplicate', 'idempot', 'rerun'],
    domain: 'ingestion',
    elaborate:
      'Duplicate ingestion is typically caused by pipeline re-runs, upstream retries, or at-least-once delivery semantics. Without idempotent design (unique business keys + MERGE), a single operational incident can inflate metrics by 2x, trigger compliance issues, and destroy trust in reporting.',
  },
  {
    k: ['partial', 'checkpoint', 'recovery'],
    domain: 'ingestion',
    elaborate:
      'Partial pipeline failures that cannot be resumed from the failure point force full re-runs, inflate cost, and often corrupt downstream state. The architect must design for atomic task boundaries, Delta transactional guarantees, and explicit checkpoint management from day one.',
  },
  {
    k: ['source', 'instab', 'sensor'],
    domain: 'ingestion',
    elaborate:
      'Tightly-coupled pipelines fail whenever the source system is down, slow, or returning partial data. Staging, readiness sensors, and buffer layers decouple production pipelines from source volatility and let source teams recover without breaking consumer SLAs.',
  },
  {
    k: ['inconsist', 'reconcil', 'control total'],
    domain: 'quality',
    elaborate:
      'Data inconsistency across systems is the #1 source of lost executive trust. Without reconciliation pipelines that compare source vs target counts, sums, and business-key parity, discrepancies compound over time and surface only during audits or board meetings.',
  },
  {
    k: ['duplicate record', 'dedup', 'survivorship'],
    domain: 'quality',
    elaborate:
      'Duplicate records arise from re-ingestion, merged source systems, and missing business keys. Survivorship rules (most-recent-wins, golden-record merging) must be explicit and governed by the data steward — not hidden in pipeline code.',
  },
  {
    k: ['missing', 'invalid', 'validation'],
    domain: 'quality',
    elaborate:
      'Weak validation lets bad data propagate to reports and ML models. A centralized DQ framework (DLT expectations, Great Expectations) enforces rules at ingestion and routes failures to a quarantine Delta table for steward review — never silent drop.',
  },
  {
    k: ['silent', 'anomaly', 'monitor'],
    domain: 'quality',
    elaborate:
      'Silent failures are the most dangerous category: pipelines run green but produce wrong or stale data. Anomaly detection on volume, schema, distribution, and freshness catches these before consumers file tickets.',
  },
  {
    k: ['trust', 'ownership', 'steward'],
    domain: 'quality',
    elaborate:
      'Poor trust stems from absence of ownership. Assigning data owners (business-side), stewards (quality accountability), and custodians (technical) creates a clear escalation path and makes quality a shared responsibility rather than an IT burden.',
  },
  {
    k: ['grain', 'dimensional'],
    domain: 'modeling',
    elaborate:
      'Wrong grain is the most expensive modeling mistake — it forces rework of every downstream query, join, and dashboard. Define grain explicitly with the business before writing a single CREATE TABLE.',
  },
  {
    k: ['scd', 'history'],
    domain: 'modeling',
    elaborate:
      'Slowly-changing dimensions without a deliberate history strategy produce reports that silently change as dimension records update. Pick SCD Type 1 (reference), Type 2 (history), or Type 6 (hybrid) per dimension based on business need.',
  },
  {
    k: ['join', 'explosion', 'star'],
    domain: 'modeling',
    elaborate:
      'Over-normalized models (snowflake/3NF) produce join explosions that make queries slow and BI dashboards fragile. A star schema with pre-joined facts and dimensions is almost always the right default for analytics.',
  },
  {
    k: ['canonical', 'enterprise model', 'glossary'],
    domain: 'modeling',
    elaborate:
      'Without a canonical data model, "Customer" means different things in CRM, Billing, and Support. A business glossary tied to physical columns via Unity Catalog tags eliminates 60%+ of integration ambiguity.',
  },
  {
    k: ['partition', 'slow query'],
    domain: 'performance',
    elaborate:
      'Poor partitioning (high cardinality, wrong column, too many files) is the #1 cause of slow Delta queries. Choose partition columns based on query access patterns, not source schema, and prefer Liquid Clustering for evolving workloads.',
  },
  {
    k: ['small file', 'compaction', 'optimize'],
    domain: 'performance',
    elaborate:
      'Small files hurt cloud read throughput dramatically — object listings dominate runtime. OPTIMIZE (with auto-compact on writes) consolidates small files into properly-sized data files and dramatically reduces query latency.',
  },
  {
    k: ['skew', 'salt', 'repartition'],
    domain: 'performance',
    elaborate:
      'Data skew (one key holds 30%+ of rows) causes straggler tasks that dominate end-to-end runtime. Salting, AQE skew joins, and broadcast joins are the proven mitigations — but first profile to confirm skew before applying.',
  },
  {
    k: ['streaming lag', 'backpressure', 'micro-batch'],
    domain: 'performance',
    elaborate:
      'Streaming lag builds up when micro-batch duration exceeds the input rate. Tune `maxFilesPerTrigger`, `maxBytesPerTrigger`, trigger interval, and cluster autoscaling together — tuning only one creates new bottlenecks.',
  },
  {
    k: ['lineage', 'metadata'],
    domain: 'governance',
    elaborate:
      'Without end-to-end lineage, impact analysis takes days and root-cause debugging becomes guesswork. Unity Catalog (or OpenLineage) should capture column-level lineage across ingest → transform → BI → ML.',
  },
  {
    k: ['pii', 'mask', 'classif'],
    domain: 'governance',
    elaborate:
      'PII exposure without explicit classification and masking is a regulatory ticking clock. Tag PII at the column level, enforce masking policies as code, and audit access quarterly — consumer convenience never justifies broad PII exposure.',
  },
  {
    k: ['permiss', 'rbac', 'least privilege'],
    domain: 'governance',
    elaborate:
      'Over-permissioned access is the most common finding in security audits. Least-privilege via group-based RBAC (never user-based) and periodic access reviews prevent data breaches and insider threats.',
  },
  {
    k: ['audit', 'audit log'],
    domain: 'governance',
    elaborate:
      'Missing audit trails block forensic investigation after breach or misuse incidents. Centralized audit logs (Unity Catalog system tables) with at least 90-day retention are non-negotiable for regulated workloads.',
  },
  {
    k: ['regulat', 'gdpr', 'sox', 'compliance'],
    domain: 'governance',
    elaborate:
      'Manual compliance processes cannot keep up with regulation evolution (GDPR, CCPA, DPDP, HIPAA). Embed compliance controls into pipelines (masking, retention, right-to-erasure) so compliance is automatic, not aspirational.',
  },
  {
    k: ['breach', 'encrypt', 'iam'],
    domain: 'security',
    elaborate:
      'Weak access + plaintext storage = breach by default. Enforce encryption-at-rest (AES-256) and in-transit (TLS 1.2+), combine with least-privilege IAM, and rotate credentials via managed key vaults.',
  },
  {
    k: ['pii log', 'secure log'],
    domain: 'security',
    elaborate:
      'Raw logs that leak PII (emails, account numbers, SSNs) create invisible compliance violations. Structured logging with field-level masking and log sanitization hooks prevents accidental exposure.',
  },
  {
    k: ['secret', 'vault', 'key'],
    domain: 'security',
    elaborate:
      'Secrets scattered across code, config files, and notebooks are the #1 cause of accidental credential leaks. Centralize in Key Vault / Secrets Manager / Databricks Secrets, and enforce via policy + scanning.',
  },
  {
    k: ['sharing', 'delta share', 'external'],
    domain: 'security',
    elaborate:
      'Uncontrolled external sharing (CSV emails, ad-hoc exports) creates data sprawl and audit blind spots. Delta Sharing provides governed, auditable, near-real-time sharing without data movement.',
  },
  {
    k: ['insider', 'behavior', 'anomaly access'],
    domain: 'security',
    elaborate:
      'Insider threats are invisible without behavior monitoring. Baseline access patterns per user/role and alert on deviations (unusual volume, off-hours, new tables) to catch threats early.',
  },
  {
    k: ['golden id', 'mdm'],
    domain: 'integration',
    elaborate:
      'Inconsistent customer/product/account IDs across systems force expensive joins and produce wrong KPIs. A golden-ID MDM layer resolves entities to one canonical identifier enterprise-wide.',
  },
  {
    k: ['multi-system', 'canonical', 'integration layer'],
    domain: 'integration',
    elaborate:
      'Point-to-point integrations scale as O(N²) and become unmaintainable past 5-10 systems. A canonical integration layer (hub-and-spoke) reduces complexity to O(N) and enables consistent transformations.',
  },
  {
    k: ['batch vs real-time', 'lambda', 'kappa'],
    domain: 'integration',
    elaborate:
      'Mixed batch and real-time architectures duplicate code and produce divergent results. Adopt a unified architecture (Kappa preferred — streaming as default, batch as replay) to eliminate duplicate logic.',
  },
  {
    k: ['legacy', 'cdc', 'wrapper'],
    domain: 'integration',
    elaborate:
      'Legacy systems (mainframe, old ERP) without APIs force nightly batch extracts. CDC (Debezium, native CDC) or staging-layer wrappers modernize access patterns without touching the legacy system.',
  },
  {
    k: ['event-driven', 'kafka', 'event hub'],
    domain: 'integration',
    elaborate:
      'Timing mismatches between producers and consumers cause race conditions and missed updates. Event-driven architecture (Kafka, Event Hubs, Kinesis) decouples producers/consumers and provides replay semantics.',
  },
  {
    k: ['feature', 'feature store', 'train/serve'],
    domain: 'ai',
    elaborate:
      'Train/serve feature inconsistency is the #1 cause of silent ML failure — offline accuracy looks good, production predictions are wrong. A feature store (Databricks FS, Feast) guarantees parity between training and serving.',
  },
  {
    k: ['ml data quality', 'ml quality'],
    domain: 'ai',
    elaborate:
      '80% of ML failures are data quality failures — not model architecture. Apply DQ rules to ML feature pipelines with the same rigor as BI pipelines, and reject training runs on bad data.',
  },
  {
    k: ['rag', 'hallucin', 'retrieval'],
    domain: 'ai',
    elaborate:
      'RAG hallucination usually stems from poor retrieval, not model capability. Invest in chunking strategy, metadata filtering, hybrid search (BM25 + vector), and re-ranking before blaming the LLM.',
  },
  {
    k: ['embedding', 'vector', 'embed'],
    domain: 'ai',
    elaborate:
      'Embedding inconsistencies (different models, unversioned regeneration) cause retrieval drift. Version embeddings alongside models and re-embed corpora on model upgrade to maintain retrieval quality.',
  },
  {
    k: ['feedback', 'retrain', 'continuous learning'],
    domain: 'ai',
    elaborate:
      'Static models drift as the world changes. Implement feedback capture (user thumbs-up, correction), drift monitoring, and triggered retraining to maintain model quality over time.',
  },
  {
    k: ['compute cost', 'always-on', 'autoscal'],
    domain: 'cost',
    elaborate:
      'Always-on All-Purpose clusters are the #1 source of Databricks cost overrun. Shift scheduled workloads to Job Clusters with autoscaling + aggressive auto-termination (10-20 min idle).',
  },
  {
    k: ['storage cost', 'lifecycle', 'duplicat'],
    domain: 'cost',
    elaborate:
      'Storage cost compounds invisibly — old partitions, unused tables, and redundant copies accumulate. Lifecycle policies (hot → cool → archive) and VACUUM reclaim 30-60% of storage cost.',
  },
  {
    k: ['query cost', 'inefficient', 'tuning'],
    domain: 'cost',
    elaborate:
      'Unoptimized queries (full scans, repeated aggregations) drive cost linearly with data growth. Query profiling, materialized views, and Photon enable large cost reductions without touching source data.',
  },
  {
    k: ['cost visibility', 'tracking', 'finops dashboard'],
    domain: 'cost',
    elaborate:
      'Without cost visibility, no team has incentive to optimize. Tag every resource (team, project, env, cost-center) and publish chargeback dashboards per domain monthly.',
  },
  {
    k: ['over-engineer', 'too many tools', 'simplif'],
    domain: 'cost',
    elaborate:
      'Tool sprawl (one-off platforms per team) multiplies licensing, integration, and support cost. A standardized reference architecture with 2-3 primary tools per layer drastically reduces total cost of ownership.',
  },
  {
    k: ['no monitoring', 'metrics', 'observab'],
    domain: 'observability',
    elaborate:
      'Without centralized monitoring, incidents are discovered by consumers — the worst possible source. Emit structured metrics, logs, traces per pipeline and wire them to a single observability platform.',
  },
  {
    k: ['debug', 'traceab', 'distributed'],
    domain: 'observability',
    elaborate:
      'Debugging distributed pipelines without lineage and correlation IDs is guesswork. End-to-end lineage + trace IDs let engineers jump from a bad report to its root cause in minutes, not hours.',
  },
  {
    k: ['alert', 'threshold', 'page'],
    domain: 'observability',
    elaborate:
      'Alert fatigue kills on-call effectiveness. Define alerts on leading indicators (latency, volume drop, DQ score) and route to the correct domain on-call — not a shared mailbox.',
  },
  {
    k: ['sla', 'slo', 'error budget'],
    domain: 'observability',
    elaborate:
      'Undefined SLAs mean every incident is "critical" and prioritization is impossible. Define SLOs per data product (e.g., 99% of Gold refreshed by 7am) with error budgets to guide investment.',
  },
  {
    k: ['repeated', 'rca', 'recurrence'],
    domain: 'observability',
    elaborate:
      'Repeated failures without RCA indicate missing process. Blameless RCA, preventive action tracking, and recurrence metrics turn ops into a learning organization.',
  },
];

const DEFAULT_ELABORATE =
  'This challenge reflects a common anti-pattern in enterprise data platforms. Resolving it requires a combination of architecture change, governance enforcement, and cultural adoption — addressing only the technical symptom usually lets the root cause resurface elsewhere.';

// ── Step templates per domain ─────────────────────────────────────────
const STEP_TEMPLATES = {
  ingestion: [
    'Inventory all upstream sources, formats, SLAs, and ownership',
    'Establish a source contract (schema, volume, latency, DQ) with each producer',
    'Implement Auto Loader (cloudFiles) or CDC depending on source type',
    'Design idempotent MERGE into Bronze using business keys + batch IDs',
    'Enable schema evolution with alerts for new/changed columns',
    'Add checkpointing and late-data handling via watermarks',
    'Build reconciliation job comparing source vs Bronze (count/checksum)',
    'Add observability: freshness, volume, error rate, schema drift alerts',
  ],
  quality: [
    'Classify data criticality per table (Critical / Important / Informational)',
    'Define DQ rules per table (completeness, validity, uniqueness, freshness)',
    'Implement rules as code (DLT expectations / Great Expectations)',
    'Route failures: WARN (log), QUARANTINE (side-table), FAIL (block pipeline)',
    'Assign a data steward per domain with quality accountability',
    'Publish a DQ dashboard (per domain + overall trust score)',
    'Introduce data certification: Silver = clean, Gold = business-certified',
    'Review DQ metrics weekly; set improvement targets per quarter',
  ],
  modeling: [
    'Engage business stakeholders to define grain and conformed dimensions',
    'Document a canonical data model + business glossary',
    'Choose SCD types per dimension (Type 1 / 2 / 6)',
    'Build Silver (clean, history) and Gold (aggregated, business-ready)',
    'Enforce naming conventions (dim_, fact_, agg_, stg_) and catalog tags',
    'Publish a semantic layer (dbt metrics, Databricks metric views)',
    'Validate models with business users via sample queries + known KPIs',
    'Version all model changes and follow a data change management process',
  ],
  performance: [
    'Profile current workload: query plans, shuffle, skew, file sizes',
    'Identify top-10 slow queries; categorize root cause (partition / skew / small files)',
    'Choose partition column by query access pattern OR enable Liquid Clustering',
    'Enable autoOptimize.optimizeWrite + autoCompact on write-heavy tables',
    'Schedule weekly OPTIMIZE + Z-ORDER on high-read tables',
    'Turn on Photon for SQL workloads; re-benchmark',
    'Address skew with salting + AQE skew-join',
    'Re-measure latency and cost; document baseline vs post-optimization gains',
  ],
  governance: [
    'Adopt Unity Catalog as the single governance plane',
    'Define 3-level namespace: catalog.schema.table (env-separated)',
    'Classify columns with tags (PII, Confidential, Public, Restricted)',
    'Write policy-as-code: row-level security, column masking via tags',
    'Assign owners (Owner / Steward / Custodian) per table; enforce via metadata',
    'Enable system tables for audit, access, lineage',
    'Integrate lineage end-to-end: ingest → transform → BI → ML',
    'Run quarterly access reviews and publish governance KPIs to leadership',
  ],
  security: [
    'Default-deny; grant USE CATALOG / SELECT via groups (never users)',
    'Move all secrets to Key Vault / Secrets Manager / Databricks Secrets',
    'Mask PII at ingestion (Bronze → Silver); never rely on BI-layer masking alone',
    'Enforce encryption at rest (CMK for regulated) + TLS 1.2+ in transit',
    'Restrict sharing to Delta Sharing; disable ad-hoc CSV export',
    'Log every access to sensitive tables; enable anomaly detection',
    'Configure private link / no-public-IP on all production workspaces',
    'Run quarterly access reviews and penetration testing',
  ],
  integration: [
    'Map the system landscape; identify systems-of-record per entity',
    'Build a canonical integration layer (hub-and-spoke, not point-to-point)',
    'Deploy MDM for golden IDs (Customer, Product, Account)',
    'Prefer CDC for transactional sources; Autoloader for files; JDBC as last resort',
    'Introduce event-driven (Kafka / Event Hubs) for decoupling beyond 5 systems',
    'Version integration contracts; breaking changes go through change review',
    'Publish an entity registry (authoritative source per entity)',
    'Add contract tests between systems to prevent silent breakage',
  ],
  ai: [
    'Establish AI-ready data foundation: clean Silver + governed Gold',
    'Deploy Feature Store (Databricks FS / Feast) for train/serve parity',
    'Define offline evaluation pipelines (accuracy, drift, bias, RAGAS for LLM)',
    'Version models + data + embeddings together for reproducibility',
    'For RAG: design chunking + metadata strategy; use hybrid search + re-rank',
    'Deploy with canary / shadow traffic; monitor drift + feedback',
    'Implement human-in-the-loop for high-stakes decisions',
    'Retrain on drift-trigger (not fixed schedule); track business outcome metrics',
  ],
  cost: [
    'Tag every resource (team, project, env, cost-center); reject untagged',
    'Publish cost dashboards per domain monthly (chargeback)',
    'Move scheduled workloads to Job Clusters with autoscaling + auto-termination',
    'Apply cluster policies (max DBU, instance family) to prevent runaway cost',
    'Enable Photon for SQL; Serverless SQL for ad-hoc BI',
    'Implement storage lifecycle: hot / cool / archive by access pattern',
    'Schedule OPTIMIZE + VACUUM on Delta tables; set retention',
    'Review top-10 cost drivers monthly with domain owners',
  ],
  observability: [
    'Instrument every pipeline: latency, volume, freshness, error rate, cost',
    'Emit structured logs + OpenTelemetry traces + metrics',
    'Publish end-to-end lineage (Unity Catalog / OpenLineage)',
    'Define SLOs per data product with error budgets',
    'Route alerts to domain on-call (PagerDuty / Opsgenie) — not shared inbox',
    'Detect anomalies on leading indicators (volume drop, DQ score decline)',
    'Run blameless RCA within 48h of P1/P2; track preventive actions',
    'Publish ops dashboard: MTTR, SLO compliance, recurrence rate',
  ],
  strategy: [
    'Articulate vision tied to business KPIs (not technology goals)',
    'Build a capability model + use-case catalog with value scoring',
    'Define target architecture (lakehouse / data mesh) + reference patterns',
    'Establish operating model: roles, RACI, domain ownership',
    'Set governance foundations: catalog, quality, security, compliance',
    'Build a phased 12-24 month roadmap with measurable KPIs per phase',
    'Invest in culture: data literacy, self-service enablement, change mgmt',
    'Measure value realization: adoption, ROI, maturity uplift — review quarterly',
  ],
};

const DEFAULT_STEPS = [
  'Assess current state: inventory, gaps, pain points, stakeholders',
  'Define target state with measurable success criteria',
  'Align with business stakeholders on value and priority',
  'Design the solution following enterprise architecture standards',
  'Build incrementally: proof-of-concept → pilot → scale',
  'Instrument observability and quality gates from day one',
  'Roll out with change management: training, documentation, communication',
  'Measure adoption, outcome, and iterate on feedback',
];

// ── Recommendation templates ──────────────────────────────────────────
const RECOMMENDATION_TEMPLATES = {
  ingestion: [
    'Use Auto Loader for file ingestion — handles schema inference, evolution, and exactly-once',
    'Design idempotent pipelines with business keys and MERGE — every pipeline must be restartable',
    'Enforce source contracts before onboarding new sources — no contract = no ingest',
    'Add reconciliation jobs for all critical pipelines (count, checksum, business-key parity)',
    'Decouple from source instability with staging + readiness sensors',
    'Emit freshness, volume, and schema drift metrics for every ingestion pipeline',
  ],
  quality: [
    'Implement DQ as code (DLT expectations / Great Expectations) — not runtime assertions',
    'Quarantine bad data; never silently drop — business stewards review weekly',
    'Publish a DQ dashboard per domain; tie trust score to data certification status',
    'Assign a data steward per domain — quality accountability lives with the business, not IT',
    'Certify Gold tables with explicit SLAs; only certified tables are exposed to self-service',
    'Run anomaly detection on volume, freshness, and distribution — catch silent failures',
  ],
  modeling: [
    'Start with business grain, not source schema — define grain explicitly before any DDL',
    'Default to Star Schema; use Data Vault only when auditability demands it',
    'Standardize SCD Type 2 for history, Type 1 for reference — document the choice per dimension',
    'Publish a business glossary tied to physical columns via Unity Catalog tags',
    'Enforce naming conventions (dim_, fact_, agg_, stg_) across all environments',
    'Build a semantic layer to prevent KPI drift across dashboards and teams',
  ],
  performance: [
    'Prefer Liquid Clustering over static partitioning for evolving workloads',
    'Enable autoOptimize.optimizeWrite + autoCompact on write-heavy tables',
    'Schedule weekly OPTIMIZE + Z-ORDER on high-read tables; daily on hot tables',
    'Turn on Photon for SQL workloads — 2-3x speed-up, same cost envelope',
    'Profile before optimizing — never guess at bottlenecks; use Spark UI + query profiles',
    'Address skew with salting + AQE — do not rely on defaults for known skewed keys',
  ],
  governance: [
    'Unity Catalog as single governance plane — do not split across tools',
    'Policy-as-code: tag-driven row-level security and column masking',
    'Every production table must have an owner — no orphaned data',
    'Quarterly access reviews with automatic revocation of stale grants',
    'Enable system tables for audit, access, and lineage — 90+ day retention',
    'Integrate catalog → BI → ML Feature Store for end-to-end lineage',
  ],
  security: [
    'Default-deny; grant access via groups (never users) through identity provider',
    'All secrets in Key Vault / Secrets Manager — never in code, notebooks, or configs',
    'Mask PII at ingestion (Bronze → Silver) — do not rely on BI-layer masking alone',
    'Delta Sharing for external data; disable ad-hoc CSV export',
    'Private link / no-public-IP on production workspaces; customer-managed keys for regulated data',
    'Monitor access patterns for anomalies; run quarterly penetration tests',
  ],
  integration: [
    'Build a canonical integration layer — do not let consumers pull directly from source systems',
    'MDM for golden IDs across Customer, Product, Account — assign a single authoritative source',
    'Prefer CDC over batch for transactional sources — lower latency, smaller volumes',
    'Use Kafka / Event Hubs for real-time decoupling beyond 5 systems',
    'Version integration contracts; breaking changes go through change review',
    'Maintain a system-of-record registry; one system is authoritative per entity',
  ],
  ai: [
    'Fix data quality BEFORE building models — 80% of ML failures are data failures',
    'Use Feature Store (Databricks FS / Feast) for train-serve parity',
    'For RAG: invest in retrieval quality first (chunking, metadata, hybrid + re-rank)',
    'Version embeddings alongside models; re-embed on model upgrade',
    'Deploy offline evaluation pipelines (accuracy, drift, RAGAS) — no deployment without eval',
    'Keep human-in-the-loop for high-stakes decisions; log feedback into retraining',
  ],
  cost: [
    'Use Job Clusters (not All-Purpose) for every scheduled workload — 50% cost reduction',
    'Autoscaling + auto-termination (10-20 min) on every cluster',
    'Cluster policies to prevent oversized DBUs and GPU misuse',
    'Spot/Preemptible for batch; on-demand for streaming',
    'Tag every resource (team, project, env, cost-center); untagged = rejected',
    'Storage lifecycle (hot → cool → archive) + VACUUM + retention on Delta tables',
  ],
  observability: [
    'Emit the 5 Golden Signals per pipeline: latency, volume, freshness, errors, cost',
    'Define SLOs per data product with error budgets',
    'Route alerts to domain on-call (PagerDuty/Opsgenie); never to shared inboxes',
    'End-to-end lineage is mandatory — no Gold table without documented upstream lineage',
    'Blameless RCA within 48h for P1/P2; track preventive action closure',
    'Publish ops dashboard: MTTR, SLO compliance, recurrence rate',
  ],
  strategy: [
    'Tie every data initiative to a business KPI — prevents "tech for tech\'s sake"',
    'Adopt Data-as-a-Product thinking — each domain owns quality, SLA, consumer experience',
    'Invest 20% capacity in foundation (platform, governance, DQ) — feature-only strategies collapse',
    'Define operating model before picking tools — tools without ownership = shelfware',
    'Make governance enabling, not blocking — governance-as-code, not governance-by-committee',
    'Measure data maturity annually — creates compounding improvement discipline',
  ],
};

const DEFAULT_RECOMMENDATIONS = [
  'Tie the initiative to a measurable business outcome — track it monthly',
  'Assign a single accountable owner; shared ownership = no ownership',
  'Build incrementally (pilot → scale) rather than big-bang migration',
  'Instrument metrics from day one — do not bolt on observability later',
  'Document decisions and trade-offs; publish architecture decision records (ADRs)',
  'Review quarterly and course-correct — no plan survives contact with reality unchanged',
];

// ── Tool templates per domain ────────────────────────────────────────
const TOOL_TEMPLATES = {
  ingestion: 'Auto Loader (cloudFiles), Delta MERGE, Structured Streaming, DLT, Airflow, Debezium',
  quality: 'DLT expectations, Great Expectations, Soda, Monte Carlo, Lightup',
  modeling: 'Delta Lake, dbt, Databricks metric views, Unity Catalog tags, Kimball toolkit',
  performance: 'Liquid Clustering, OPTIMIZE/Z-ORDER, Photon, AQE, Spark UI, Delta log',
  governance: 'Unity Catalog, Collibra, Alation, OpenLineage, Atlas',
  security: 'Key Vault, Secrets Manager, Delta Sharing, CMK, Private Link, Azure AD/Okta',
  integration: 'Kafka, Event Hubs, Kinesis, Debezium, Informatica, Airflow, Fivetran',
  ai: 'Databricks Feature Store, Feast, MLflow, Vector Search, Model Serving, RAGAS',
  cost: 'Cluster Policies, System Tables, CloudHealth, Finout, custom Delta cost tables',
  observability: 'Datadog, Grafana, Prometheus, OpenTelemetry, Monte Carlo, system tables',
  strategy: 'Gartner / TDWI maturity models, capability maps, value stream mapping, OKRs',
};

// ── Impact templates per domain ──────────────────────────────────────
const IMPACT_TEMPLATES = {
  ingestion: [
    'Pipeline failures reduced 70-90%',
    'Schema-break incidents cut to near-zero',
    'Reprocessing cost reduced 40-60% via idempotency',
    'Source onboarding time: weeks → days',
  ],
  quality: [
    'Trust score improved from 50-60% to 90%+',
    'Data-driven decision adoption up 2-3x',
    'Audit findings reduced 80%',
    'Steward-led remediation replacing reactive firefighting',
  ],
  modeling: [
    'KPI drift across dashboards eliminated',
    'Query complexity reduced; join explosion gone',
    'Self-service BI adoption up 3-5x after semantic layer',
    'New use-case time-to-value cut 50%',
  ],
  performance: [
    'Query latency improved 3-10x on hot tables',
    'Compute cost reduced 25-50%',
    'Streaming lag stable under load',
    'SLA adherence > 99%',
  ],
  governance: [
    'Audit readiness continuous (no scramble)',
    'PII incidents reduced to zero or near-zero',
    'Access provisioning time: days → minutes',
    'Lineage-driven impact analysis in minutes',
  ],
  security: [
    'Zero PII exposure incidents',
    'Penetration test findings: high → low/none',
    'Credential leaks eliminated via vault enforcement',
    'Regulatory fines avoided via proactive controls',
  ],
  integration: [
    'Integration lead time reduced 60-80%',
    'Point-to-point coupling replaced with hub-and-spoke',
    'Golden-ID adoption across CRM, Billing, Support',
    'Cross-system KPI reconciliation automated',
  ],
  ai: [
    'Train/serve skew eliminated via Feature Store',
    'RAG hallucination reduced via retrieval improvements',
    'Model deployment velocity up 3-5x',
    'Business outcome KPI tied to every production model',
  ],
  cost: [
    'Platform cost reduced 25-40%',
    'Idle compute < 10%',
    'Chargeback drives domain-level cost optimization',
    'ROI measurable per domain',
  ],
  observability: [
    'MTTR reduced 60-80%',
    'Incidents discovered internally before consumers notice',
    'SLO compliance > 99%',
    'Repeat-incident rate trending to zero via preventive actions',
  ],
  strategy: [
    'Data strategy approved by CEO/CDO',
    'Maturity uplift +1 level in 12 months',
    'Investment funding secured via business-case ROI',
    'Data culture scored higher in employee engagement surveys',
  ],
};

// ── Group → domain mapping ───────────────────────────────────────────
const GROUP_TO_DOMAIN = {
  'Ingestion & Pipeline': 'ingestion',
  'Data Quality': 'quality',
  'Data Quality & Trust': 'quality',
  Modeling: 'modeling',
  'Data Modeling': 'modeling',
  Performance: 'performance',
  'Performance & Scalability': 'performance',
  Governance: 'governance',
  'Governance & Compliance': 'governance',
  Security: 'security',
  'Data Security': 'security',
  Integration: 'integration',
  'Data Integration': 'integration',
  AI: 'ai',
  'AI / RAG / ML': 'ai',
  'AI/ML': 'ai',
  ML: 'ai',
  RAG: 'ai',
  Cost: 'cost',
  FinOps: 'cost',
  'Cost / FinOps': 'cost',
  Observability: 'observability',
  'Observability & Operations': 'observability',
  Operations: 'observability',
  Strategy: 'strategy',
  'Data Strategy': 'strategy',
};

// ── Helpers ──────────────────────────────────────────────────────────
function inferDomain(item) {
  if (item.domain) return item.domain; // explicit override
  if (item.group && GROUP_TO_DOMAIN[item.group]) return GROUP_TO_DOMAIN[item.group];

  const haystack = [
    item.title,
    item.rootCause,
    item.why,
    item.solution,
    item.artifacts,
    item.interviewAnswer,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  for (const entry of KEYWORD_MAP) {
    if (entry.k.some((kw) => haystack.includes(kw))) return entry.domain;
  }
  return null;
}

function elaborateChallenge(item) {
  if (item.challengeDetail) return item.challengeDetail;
  const haystack = [item.title, item.rootCause, item.why, item.solution]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
  for (const entry of KEYWORD_MAP) {
    if (entry.k.some((kw) => haystack.includes(kw))) return entry.elaborate;
  }
  return DEFAULT_ELABORATE;
}

function getSteps(item, domain) {
  if (Array.isArray(item.steps) && item.steps.length) return item.steps;
  return STEP_TEMPLATES[domain] || DEFAULT_STEPS;
}

function getRecommendations(item, domain) {
  if (Array.isArray(item.recommendations) && item.recommendations.length)
    return item.recommendations;
  return RECOMMENDATION_TEMPLATES[domain] || DEFAULT_RECOMMENDATIONS;
}

function getTools(item, domain) {
  return item.tools || TOOL_TEMPLATES[domain] || 'Databricks, Delta Lake, Unity Catalog, Airflow';
}

function getImpact(item, domain) {
  if (Array.isArray(item.impact) && item.impact.length) return item.impact;
  // If impact is a string sentence (from DataArchitectChallenges), keep it as sentence + add template metrics
  const fromTemplate = IMPACT_TEMPLATES[domain] || [
    'Measurable improvement in reliability',
    'Clearer ownership and accountability',
    'Reduced operational toil',
    'Faster time-to-value for new use cases',
  ];
  if (typeof item.impact === 'string' && item.impact.trim()) {
    return [item.impact, ...fromTemplate];
  }
  return fromTemplate;
}

// ── Component ────────────────────────────────────────────────────────
function DeepDetailView({ item }) {
  if (!item) return null;

  const domain = inferDomain(item) || 'strategy';
  const challengeDetail = elaborateChallenge(item);
  const steps = getSteps(item, domain);
  const recommendations = getRecommendations(item, domain);
  const tools = getTools(item, domain);
  const impact = getImpact(item, domain);

  return (
    <div style={{ marginTop: '1.25rem', display: 'grid', gap: '1rem' }}>
      {/* Challenge Deep-Dive */}
      <div
        style={{
          background: 'linear-gradient(135deg, #fff7ed 0%, #ffedd5 100%)',
          border: '1px solid #fed7aa',
          borderLeft: '4px solid #f97316',
          borderRadius: '10px',
          padding: '1rem 1.15rem',
        }}
      >
        <div
          style={{
            fontSize: '0.7rem',
            fontWeight: 700,
            textTransform: 'uppercase',
            letterSpacing: '0.05em',
            color: '#c2410c',
            marginBottom: '0.5rem',
          }}
        >
          Challenge — Deep Dive
        </div>
        <p style={{ margin: 0, fontSize: '0.9rem', lineHeight: 1.65, color: '#7c2d12' }}>
          <strong style={{ color: '#9a3412' }}>Root cause:</strong>{' '}
          {item.rootCause || item.why || '—'}
        </p>
        <p
          style={{ margin: '0.5rem 0 0 0', fontSize: '0.9rem', lineHeight: 1.7, color: '#7c2d12' }}
        >
          {challengeDetail}
        </p>
      </div>

      {/* Step-by-Step Action Sequence */}
      <div
        style={{
          background: 'linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%)',
          border: '1px solid #bfdbfe',
          borderLeft: '4px solid #2563eb',
          borderRadius: '10px',
          padding: '1rem 1.15rem',
        }}
      >
        <div
          style={{
            fontSize: '0.7rem',
            fontWeight: 700,
            textTransform: 'uppercase',
            letterSpacing: '0.05em',
            color: '#1d4ed8',
            marginBottom: '0.65rem',
          }}
        >
          Step-by-Step Action Sequence
        </div>
        <ol style={{ margin: 0, paddingLeft: '1.1rem', color: '#1e3a5f' }}>
          {steps.map((s, i) => (
            <li
              key={i}
              style={{
                fontSize: '0.88rem',
                lineHeight: 1.65,
                marginBottom: '0.35rem',
              }}
            >
              {s}
            </li>
          ))}
        </ol>
      </div>

      {/* Architect Recommendations */}
      <div
        style={{
          background: 'linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%)',
          border: '1px solid #bbf7d0',
          borderLeft: '4px solid #16a34a',
          borderRadius: '10px',
          padding: '1rem 1.15rem',
        }}
      >
        <div
          style={{
            fontSize: '0.7rem',
            fontWeight: 700,
            textTransform: 'uppercase',
            letterSpacing: '0.05em',
            color: '#15803d',
            marginBottom: '0.65rem',
          }}
        >
          Architect Recommendations
        </div>
        <ul style={{ margin: 0, paddingLeft: '1.1rem', color: '#14532d' }}>
          {recommendations.map((r, i) => (
            <li
              key={i}
              style={{
                fontSize: '0.88rem',
                lineHeight: 1.65,
                marginBottom: '0.35rem',
              }}
            >
              {r}
            </li>
          ))}
        </ul>
      </div>

      {/* Two-column: Tools & Real Impact */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: '1rem',
        }}
      >
        <div
          style={{
            background: 'linear-gradient(135deg, #fefce8 0%, #fef9c3 100%)',
            border: '1px solid #fde68a',
            borderLeft: '4px solid #ca8a04',
            borderRadius: '10px',
            padding: '1rem 1.15rem',
          }}
        >
          <div
            style={{
              fontSize: '0.7rem',
              fontWeight: 700,
              textTransform: 'uppercase',
              letterSpacing: '0.05em',
              color: '#a16207',
              marginBottom: '0.5rem',
            }}
          >
            Tools &amp; Patterns
          </div>
          <p
            style={{
              margin: 0,
              fontSize: '0.85rem',
              lineHeight: 1.65,
              color: '#713f12',
              fontFamily: 'monospace',
            }}
          >
            {tools}
          </p>
          {item.artifacts && (
            <p style={{ margin: '0.75rem 0 0 0', fontSize: '0.82rem', color: '#713f12' }}>
              <strong>Artifacts:</strong> {item.artifacts}
            </p>
          )}
        </div>

        <div
          style={{
            background: 'linear-gradient(135deg, #ecfeff 0%, #cffafe 100%)',
            border: '1px solid #a5f3fc',
            borderLeft: '4px solid #0891b2',
            borderRadius: '10px',
            padding: '1rem 1.15rem',
          }}
        >
          <div
            style={{
              fontSize: '0.7rem',
              fontWeight: 700,
              textTransform: 'uppercase',
              letterSpacing: '0.05em',
              color: '#0e7490',
              marginBottom: '0.5rem',
            }}
          >
            Real Impact
          </div>
          <ul style={{ margin: 0, paddingLeft: '1.1rem', color: '#155e75' }}>
            {impact.map((m, i) => (
              <li
                key={i}
                style={{
                  fontSize: '0.85rem',
                  lineHeight: 1.6,
                  marginBottom: '0.25rem',
                }}
              >
                {m}
              </li>
            ))}
          </ul>
        </div>
      </div>

      {/* Interview Answer */}
      {item.interviewAnswer && (
        <div
          style={{
            background: 'linear-gradient(135deg, #faf5ff 0%, #ede9fe 100%)',
            border: '1px solid #ddd6fe',
            borderLeft: '4px solid #7c3aed',
            borderRadius: '10px',
            padding: '1rem 1.15rem',
          }}
        >
          <div
            style={{
              fontSize: '0.7rem',
              fontWeight: 700,
              textTransform: 'uppercase',
              letterSpacing: '0.05em',
              color: '#6d28d9',
              marginBottom: '0.5rem',
            }}
          >
            Director-Level Interview Answer
          </div>
          <div
            style={{
              fontSize: '0.9rem',
              lineHeight: 1.7,
              color: '#3b0764',
              fontStyle: 'italic',
              position: 'relative',
              paddingLeft: '1.25rem',
            }}
          >
            <span
              style={{
                position: 'absolute',
                left: 0,
                top: '-4px',
                fontSize: '2rem',
                color: '#c4b5fd',
                lineHeight: 1,
              }}
            >
              &ldquo;
            </span>
            {item.interviewAnswer}
            <span
              style={{
                fontSize: '2rem',
                color: '#c4b5fd',
                lineHeight: 1,
                verticalAlign: 'bottom',
                marginLeft: '4px',
              }}
            >
              &rdquo;
            </span>
          </div>
        </div>
      )}
    </div>
  );
}

export default DeepDetailView;
