import React from 'react';

/**
 * DeepGuide — append a deep architect guide block to pages that already
 * have rich hand-authored content (MaturityModel, Roadmap, Assessment,
 * ProductionSupport). Does NOT replace their existing sections — it
 * ADDS a new "Architect Deep Guide" block with Steps + Recommendations
 * + Common Pitfalls + Success KPIs, keyed by (type, key).
 *
 * Usage:
 *   <DeepGuide type="maturity" level={3} />
 *   <DeepGuide type="roadmap" phase={2} />
 *   <DeepGuide type="assessment" dimension="B" />
 *   <DeepGuide type="support" level="L2" />
 */

// ── Maturity Model per level ─────────────────────────────────────────
const MATURITY = {
  1: {
    title: 'Level 1 — Ad Hoc: Stabilize the Chaos',
    challenges: [
      'No single source of truth; data scattered in Excel, emails, SharePoint',
      'Every report is a one-off build; no reusable pipelines',
      'Zero governance — nobody owns anything',
      'Trust is anecdotal; leadership questions every number',
    ],
    steps: [
      'Inventory every data source, owner, consumer, and pain point',
      'Pick one lakehouse platform (Databricks / Snowflake / Fabric) — stop tool proliferation',
      'Centralize top-10 critical sources into a raw Bronze layer',
      'Stand up basic orchestration (Airflow / Workflows) with retries + logging',
      'Introduce basic RBAC + encryption at rest',
      'Establish a core data team (3-7 people) with clear charter',
      'Publish a baseline maturity score to leadership',
    ],
    recommendations: [
      'Do NOT skip this level for AI ambitions — you will pay 3x later',
      'Pick ONE platform; resist multi-cloud complexity until L4+',
      'Hire for curiosity + fundamentals, not just résumés',
      'Over-communicate: every report should show source, date, owner',
      'Set a 6-month goal to reach L2 — not 12',
    ],
    pitfalls: [
      'Buying tools before defining use cases',
      'Trying to govern everything — focus on top-10 datasets first',
      'Building for "future scale" — stabilize the present first',
    ],
    kpis: [
      '10+ core sources centralized',
      'Top-20 critical reports migrated',
      'Manual effort reduced 30%+',
      'One data owner per critical report',
    ],
  },
  2: {
    title: 'Level 2 — Managed: Standardize & Automate',
    challenges: [
      'Silver layer exists but is inconsistent across domains',
      'DQ is manual; issues caught by consumers, not pipelines',
      'Governance is lightweight; catalog half-populated',
      'Self-service BI blocked by data quality concerns',
    ],
    steps: [
      'Build a governed Silver layer with enforced naming + column standards',
      'Introduce a DQ framework (DLT expectations / Great Expectations) on top-10 pipelines',
      'Deploy Unity Catalog and migrate top-20 tables with tags + ownership',
      'Build canonical dimensions (Customer, Product, Date) as single-source',
      'Start publishing data SLAs (freshness, completeness) per table',
      'Enable Delta Lake features: OPTIMIZE, Z-ORDER, retention policies',
      'Introduce reconciliation jobs for every critical pipeline',
    ],
    recommendations: [
      'Certify tables: Silver = clean, Gold = business-certified — publish the list',
      'Assign a data steward per domain (business-side, not IT-side)',
      'Kill redundant pipelines: if two teams calculate Revenue, merge them',
      'Stop all uncontrolled spreadsheet exports; route through Gold + BI',
      'Focus DQ investment on top-10 highest-impact tables first',
    ],
    pitfalls: [
      'Over-engineering Silver while Bronze is still broken',
      'Cataloguing for coverage instead of quality',
      'Letting DQ become an IT concern — stewards are business owners',
    ],
    kpis: [
      'DQ score > 80% on critical tables',
      'Unity Catalog coverage > 70%',
      'Self-service BI adoption growing',
      'Top-10 pipelines reconciled daily',
    ],
  },
  3: {
    title: 'Level 3 — Defined: Scale with Governance',
    challenges: [
      'Scale exposes edge cases: skew, small files, cost overruns',
      'Governance becomes a bottleneck if centralized',
      'First ML models in production — feature parity issues surface',
      'Cost visibility missing; leadership questions ROI',
    ],
    steps: [
      'Introduce FinOps: tags, cost dashboards, chargeback per domain',
      'Deploy cluster policies + job clusters + autoscaling org-wide',
      'Enable row-level security + column masking as code (Unity Catalog)',
      'Stand up Feature Store for ML (train/serve parity)',
      'Build CI/CD for data: Git + DLT + pipelines-as-code',
      'Introduce data observability (Monte Carlo / Lightup / custom)',
      'Publish domain-level data products with owners + SLAs',
    ],
    recommendations: [
      'Move from central governance to federated (domains own policies)',
      'Invest 20% of capacity in platform + reliability — feature-only burns out teams',
      'Productize data: owners, versioned contracts, documented SLAs',
      'Enforce cost via policy, not pleading — cluster policies are non-negotiable',
      'Start measuring data trust score + publish to leadership monthly',
    ],
    pitfalls: [
      'Adding tools instead of simplifying',
      'Governance-by-committee slowing velocity',
      'Skipping FinOps until cost is out of control',
    ],
    kpis: [
      'SLA adherence > 95%',
      'Cost reduced 20-30%',
      'ML models in production (> 5)',
      'Data products published (> 10)',
    ],
  },
  4: {
    title: 'Level 4 — Advanced: Enterprise Scale',
    challenges: [
      'Org-wide ML and streaming exposes operational gaps',
      'Regulatory complexity grows; manual compliance impossible',
      'Multi-cloud / multi-region latency & governance challenges',
      'AI governance (bias, explainability) becomes board-level topic',
    ],
    steps: [
      'Streaming-first patterns for new use cases (Structured Streaming / Flink)',
      'MLOps platform: experiment tracking, model registry, online serving',
      'Automated compliance: GDPR/CCPA/DPDP enforcement in pipelines',
      'Multi-region governance via Unity Catalog federation',
      'Drift monitoring + triggered retraining for production models',
      'Data contracts between producers + consumers with validation',
      'Domain-driven Data Mesh with central platform + federated ownership',
    ],
    recommendations: [
      'Shift from platform-as-service to platform-as-product (domain customers)',
      'Every data product must publish: SLA, schema, example, contact, cost',
      'Introduce error budgets per data product with SRE-style ops',
      'Bias + explainability for all customer-facing AI — before regulation mandates',
      'Measure business outcome per data product, not just technical KPIs',
    ],
    pitfalls: [
      'Platform team becoming bottleneck in federated model',
      'Over-engineering ML infrastructure before models deliver value',
      'Letting compliance slow innovation vs automating it',
    ],
    kpis: [
      'SLA > 99%',
      'Time-to-production for ML: weeks → days',
      'Cost / TB continues to trend down',
      'Audit findings near zero',
    ],
  },
  5: {
    title: 'Level 5 — Optimized: AI-Driven Enterprise',
    challenges: [
      'Innovation velocity vs reliability — both must improve together',
      'GenAI governance (hallucination, IP, data leakage) is a full discipline',
      'Talent model shifts from builders to product owners',
      'Measuring ROI on AI investments requires new accounting',
    ],
    steps: [
      'GenAI platform: RAG, fine-tuning, agents with governance + eval',
      'Real-time data products with event-driven architecture as default',
      'Self-healing pipelines with anomaly-triggered remediation',
      'Semantic layer + LLM-assisted analytics for all business users',
      'AI-assisted ops: incident triage, cost optimization, data discovery',
      'Federated learning / privacy-preserving AI where regulated',
      'Continuous maturity measurement + public data strategy transparency',
    ],
    recommendations: [
      'Invest in AI evaluation discipline (RAGAS, LLM-as-Judge, offline eval)',
      'Treat data products as first-class company products with P&L',
      'Hire data product managers — not just engineers',
      'Publish AI ethics + usage policies; appoint an AI review board',
      'Quarterly strategy reviews — you are now shaping the industry, not catching up',
    ],
    pitfalls: [
      'Complacency — maturity must be continuously earned',
      'Letting GenAI sprawl without governance',
      'Losing fundamentals while chasing cutting-edge',
    ],
    kpis: [
      'AI contribution to revenue measurable',
      'GenAI in production use cases (> 10)',
      'Maturity uplift in peer-group benchmarks',
      'Data as a competitive moat (public case studies)',
    ],
  },
};

// ── Roadmap per phase ────────────────────────────────────────────────
const ROADMAP = {
  1: {
    title: 'Phase 1 — Stabilize & Foundation (0-3 months)',
    challenges: [
      'Existing pipelines brittle and untrusted',
      'No centralized platform; data in silos',
      'Manual effort dominates; team burnt out',
      'Leadership needs quick credibility wins',
    ],
    steps: [
      'Executive sponsor + charter signed by CDO/CIO within week 1',
      'Platform decision: Databricks / Snowflake / Fabric — lock it in week 2',
      'Inventory top-30 data sources; prioritize top-10 by business value',
      'Build Bronze landing zone with standardized folder + naming',
      'Deploy orchestration (Airflow / Workflows) with retries + logging',
      'Migrate top-5 pipelines end-to-end as pilots by week 6',
      'Stand up core data team (ingestion, platform, governance leads)',
      'Baseline maturity assessment + publish quarterly scorecard',
    ],
    recommendations: [
      'Lock one platform — resist multi-tool architectures during foundation',
      'Prioritize quick wins: top-5 pipelines live + trusted is better than 50 half-built',
      "Don't skip ownership: every pipeline must have a named tech owner + business owner",
      'Over-communicate: weekly status to leadership; publish wins + risks',
      'Keep architecture simple: Bronze → Silver pattern, not data mesh on day one',
    ],
    pitfalls: [
      'Analysis paralysis on platform choice',
      'Trying to migrate everything instead of focusing',
      'Hiring ahead of charter instead of after',
    ],
    kpis: [
      'Pipeline success rate > 90%',
      'Top-10 pipelines on new platform',
      'Manual effort -30-40%',
      'Quarterly scorecard published',
    ],
  },
  2: {
    title: 'Phase 2 — Standardize & Scale (3-9 months)',
    challenges: [
      'Bronze data trusted, Silver inconsistent across domains',
      'Business wants self-service; DQ blocks it',
      'Governance council forming; slow to decide',
      'Cost growing without visibility',
    ],
    steps: [
      'Deploy Unity Catalog (or equivalent) with 3-level namespace',
      'Build Silver layer with enforced schemas + DQ expectations',
      'Publish canonical dimensions (Customer, Product, Date) enterprise-wide',
      'Launch top-3 domain data marts (Finance, Sales, Operations)',
      'Stand up data quality framework on top-20 critical pipelines',
      'Introduce tagging + first cost dashboards per domain',
      'Enable self-service BI on certified Gold tables',
      'Run maturity reassessment at month 9 — target L3',
    ],
    recommendations: [
      'Certify Gold tables publicly — unverified stays restricted',
      'Assign stewards per domain; they own quality, not IT',
      'Push decisions to domains; central team enables, does not block',
      'Start FinOps now — cost compounds invisibly once at scale',
      'Retire legacy pipelines as new ones go live — do not run in parallel indefinitely',
    ],
    pitfalls: [
      'Building Silver without DQ contracts',
      'Launching BI on uncertified data',
      'Central team becoming bottleneck',
    ],
    kpis: [
      'DQ score > 80%',
      'Unity Catalog coverage > 70%',
      'Self-service BI adoption up 2-3x',
      'Cost dashboards live per domain',
    ],
  },
  3: {
    title: 'Phase 3 — Optimize & Govern (9-18 months)',
    challenges: [
      'Scale exposes performance + cost issues',
      'Regulatory pressure intensifying (PII, audit, retention)',
      'First production ML models appearing',
      'Domains want autonomy; platform team worried about standards',
    ],
    steps: [
      'Enforce cluster policies + autoscaling + auto-termination org-wide',
      'Enable row-level security + column masking via Unity Catalog tags',
      'Deploy Feature Store + MLOps (MLflow registry, model serving)',
      'Introduce data observability (Monte Carlo / Lightup)',
      'Publish data products with SLAs + owners + discovery',
      'Automate compliance: GDPR right-to-erasure, SOX audit exports',
      'Adopt CI/CD for data (Git-first, pipelines-as-code, DLT)',
      'Shift to federated governance — platform enables, domains own',
    ],
    recommendations: [
      'FinOps program lead is now full-time — not a side project',
      'Publish a "What Good Looks Like" architecture guide for domains',
      'Every production ML model must have monitoring + rollback before launch',
      'Audit automation: if manual, it will fail under regulator pressure',
      'Maturity assessment every 6 months — tie to investment decisions',
    ],
    pitfalls: [
      'Platform team scope creep — stay platform-as-product',
      'Letting security become the new bottleneck',
      'Missing the ML governance shift until it is too late',
    ],
    kpis: [
      'SLA adherence > 95%',
      'Cost -20-30% from baseline',
      'ML models in production (> 5)',
      'Zero PII incidents',
    ],
  },
  4: {
    title: 'Phase 4 — AI/GenAI Transformation (18-36 months)',
    challenges: [
      'GenAI hype meets data reality — 80% of pilots fail',
      'Real-time data needs reshaping batch-first architecture',
      'AI governance (bias, hallucination, IP) becomes board topic',
      'Talent war for GenAI skills intensifies',
    ],
    steps: [
      'Stand up GenAI platform: LLM gateway, vector search, RAG patterns',
      'Build streaming-first architecture for new real-time use cases',
      'Deploy drift monitoring + auto-retrain triggers on production models',
      'Publish AI evaluation discipline (RAGAS, LLM-as-Judge, offline eval)',
      'Every production AI use case: eval + monitoring + human-in-loop',
      'Domain-driven data products as the default pattern (Data Mesh)',
      'Self-healing pipelines + AI-assisted ops (incident triage, cost)',
      'Publish AI ethics, usage policies, governance review board',
    ],
    recommendations: [
      'Resist GenAI pilots on bad data — fix data first, model second',
      'Evaluate before deploying — offline eval is non-negotiable',
      'Small fine-tuned models often beat frontier LLMs for cost + quality',
      'Treat AI as product: owner, P&L, customer feedback loop',
      'Measure business outcomes, not technical metrics (accuracy != value)',
    ],
    pitfalls: [
      'Launching GenAI without evaluation discipline',
      'Duplicating batch pipelines in streaming unnecessarily',
      'Ignoring governance until regulator arrives',
    ],
    kpis: [
      'AI contribution to revenue measurable',
      'GenAI production use cases (> 10)',
      'Streaming ingestion > 30% of pipelines',
      'Time-to-production for new model: days',
    ],
  },
};

// ── Assessment per dimension ─────────────────────────────────────────
const ASSESSMENT = {
  A: {
    title: 'A — Data Ingestion',
    challenges: [
      'Wide source variety (files, DBs, APIs, streams) with no standardization',
      'Schema drift breaks pipelines silently',
      'Late-arriving + duplicate data inflate metrics',
      'Source system instability propagates downstream',
    ],
    steps: [
      'Inventory all sources; classify by type (files / DB / stream / API)',
      'Auto Loader for files, CDC for transactional DBs, streaming for events',
      'Enforce source contracts (schema, SLA, volume, DQ) with producers',
      'Checkpointing + watermarking for late data',
      'Idempotent MERGE with business keys + batch IDs',
      'Reconciliation jobs for all critical pipelines',
      'Observability: freshness, volume, error rate, schema drift',
    ],
    recommendations: [
      'Auto Loader for all file ingestion — handles evolution, exactly-once',
      'No contract, no ingest — enforce source contracts as precondition',
      'Every pipeline idempotent + restartable; never timestamp-only logic',
      'Reconciliation is not optional — build one per critical pipeline',
      'Decouple from source instability with staging + readiness sensors',
    ],
    pitfalls: [
      'Building custom connectors instead of using Auto Loader / native CDC',
      'Trusting source teams to announce schema changes',
      'Running without checkpoints on streams',
    ],
    kpis: [
      'Pipeline success > 95% on critical sources',
      'Schema drift caught before failure',
      'Reprocessing cost low via idempotency',
      'Onboarding time: weeks → days',
    ],
  },
  B: {
    title: 'B — Data Quality',
    challenges: [
      'Validation is manual; issues surface via consumer complaints',
      'No unified DQ framework; each team builds its own',
      'Ownership unclear; platform team blamed for business data',
      'Trust score low; leadership questions every number',
    ],
    steps: [
      'Classify tables by criticality (Critical / Important / Informational)',
      'Define DQ rules per table (completeness, validity, uniqueness, freshness)',
      'Implement rules as code (DLT expectations / Great Expectations)',
      'Route failures: WARN / QUARANTINE / FAIL based on severity',
      'Assign stewards per domain with quality accountability',
      'Publish DQ dashboard + trust score per domain + overall',
      'Certify Silver = clean, Gold = business-certified',
    ],
    recommendations: [
      'DQ is code, not spreadsheets — DLT or Great Expectations',
      'Quarantine bad data; never silently drop',
      'Stewards are business-side; IT enforces, business owns',
      'Certification status is public — unverified stays restricted',
      'Weekly DQ review; quarterly improvement targets per domain',
    ],
    pitfalls: [
      'Letting DQ become IT problem instead of business accountability',
      'Trying to validate everything — start with top-20 critical tables',
      'Silent dropping bad data to "keep pipelines green"',
    ],
    kpis: [
      'DQ score > 90% on critical tables',
      'Trust score per domain published',
      '< 5% tables uncertified in Gold',
      'Steward-led remediation, not IT firefighting',
    ],
  },
  C: {
    title: 'C — Data Modeling',
    challenges: [
      'Source-driven schemas instead of business-grain modeling',
      'SCD history strategy inconsistent across dimensions',
      'Joins explode; dashboards slow and fragile',
      'KPI definitions drift across domains + tools',
    ],
    steps: [
      'Engage business to define grain + conformed dimensions',
      'Publish canonical data model + business glossary',
      'Choose SCD types per dimension (Type 1 / 2 / 6)',
      'Build Silver (clean, history) and Gold (aggregated, business-ready)',
      'Enforce naming conventions (dim_, fact_, agg_, stg_)',
      'Publish semantic layer (dbt metrics / metric views)',
      'Validate with business via sample queries + known KPIs',
    ],
    recommendations: [
      'Start with business grain — never source schema',
      'Star Schema by default; Data Vault only if audit demands it',
      'Semantic layer is mandatory once you have > 3 BI tools',
      'Version model changes; follow data change management process',
      'Business glossary lives in Unity Catalog, not a wiki',
    ],
    pitfalls: [
      'Building models without business validation',
      'Over-normalizing for "purity" instead of query performance',
      'Letting dashboards create their own metric definitions',
    ],
    kpis: [
      'Canonical dimensions published + adopted',
      'Semantic layer active for top-10 KPIs',
      'Dashboards using certified Gold only',
      'KPI drift eliminated across tools',
    ],
  },
  D: {
    title: 'D — Governance',
    challenges: [
      'Governance slow or non-existent; shadow data everywhere',
      'Ownership unclear; orphaned tables accumulate',
      'Catalog half-populated; metadata stale',
      'Lineage not end-to-end; impact analysis takes days',
    ],
    steps: [
      'Unity Catalog deployed with 3-level namespace (catalog.schema.table)',
      'Classify columns with tags (PII, Confidential, Public, Restricted)',
      'Policy-as-code: row-level security + column masking via tags',
      'Assign owners (Owner / Steward / Custodian) per table',
      'Enable system tables: audit, access, lineage',
      'Integrate lineage end-to-end: ingest → transform → BI → ML',
      'Quarterly access reviews + orphan cleanup',
    ],
    recommendations: [
      'Unity Catalog as SINGLE governance plane — do not split',
      'Every production table must have an owner — no orphans',
      'Policy-as-code via tags; manual views do not scale',
      'System tables for audit + 90+ day retention',
      'Catalog enables — not blocks — business self-service',
    ],
    pitfalls: [
      'Governance-by-committee slowing delivery',
      'Coverage-first cataloguing (low quality metadata)',
      'Letting lineage lag behind code',
    ],
    kpis: [
      'Unity Catalog coverage > 95% of production tables',
      'Owner assigned to 100% of Gold tables',
      'Access reviews on schedule',
      'End-to-end lineage for top-20 data products',
    ],
  },
  E: {
    title: 'E — Security',
    challenges: [
      'Over-permissioned users; audit findings every cycle',
      'Secrets scattered across code, configs, notebooks',
      'PII exposed in logs, exports, and dev environments',
      'External sharing uncontrolled (CSV emails, ad-hoc exports)',
    ],
    steps: [
      'Default-deny; group-based RBAC via identity provider (Entra ID / Okta)',
      'All secrets in Key Vault / Secrets Manager — no exceptions',
      'Mask PII at ingestion (Bronze → Silver) — do not rely on BI masking',
      'Encryption at rest (AES-256, CMK for regulated) + TLS 1.2+',
      'Delta Sharing for external data; disable ad-hoc exports',
      'Private link / no-public-IP on production workspaces',
      'Quarterly access reviews + penetration tests',
    ],
    recommendations: [
      'Default deny — whitelist access via groups, never users',
      'Mask PII at ingest — do not leak through to derived tables',
      'Customer-managed keys for regulated workloads',
      'Log every access to sensitive tables; monthly anomaly review',
      'Security is not bolt-on — bake into pipelines from day one',
    ],
    pitfalls: [
      'User-based grants instead of group-based',
      'Secrets in notebooks "just for testing"',
      'PII in dev + test environments via copy from prod',
    ],
    kpis: [
      'Zero PII exposure incidents',
      'Access reviews automated',
      'Secrets scanning: zero findings',
      'Pen-test findings: high → low or none',
    ],
  },
  F: {
    title: 'F — Platform & Performance',
    challenges: [
      'Slow queries blocking business; cost growing',
      'Small files dominate; I/O inefficient',
      'Data skew causing straggler tasks',
      'No FinOps visibility; teams cost-blind',
    ],
    steps: [
      'Profile current workload (Spark UI, query profiles, skew, files)',
      'Prefer Liquid Clustering on new tables; partition + Z-ORDER on old',
      'Enable autoOptimize.optimizeWrite + autoCompact on write-heavy',
      'Photon for SQL workloads; Serverless SQL for ad-hoc BI',
      'Cluster policies + autoscaling + aggressive auto-termination',
      'Tag every resource + publish cost dashboards per domain',
      'Storage lifecycle: hot / cool / archive by access pattern',
    ],
    recommendations: [
      'Profile before optimizing — never guess',
      'Liquid Clustering > static partitioning for evolving workloads',
      'Photon = 2-3x speed-up, same cost envelope — enable it',
      'Cluster policies prevent runaway cost — non-negotiable',
      'Cost dashboards + chargeback drives domain-level optimization',
    ],
    pitfalls: [
      'Partitioning on high-cardinality columns',
      'Running All-Purpose clusters for scheduled jobs',
      'Ignoring small-file problem until queries timeout',
    ],
    kpis: [
      'Query latency improved 3-10x',
      'Cost -25-50%',
      'Idle compute < 10%',
      'SLA compliance > 99%',
    ],
  },
  G: {
    title: 'G — BI & Self-Service',
    challenges: [
      'Every team builds its own metrics → KPI drift',
      'Dashboards proliferate; nobody trusts any single one',
      'BI tools disconnected from governance; stale copies everywhere',
      'Business asks "why are the numbers different?" weekly',
    ],
    steps: [
      'Deploy semantic layer (dbt metrics, Databricks metric views, LookML)',
      'Certify Gold tables with SLAs + owners',
      'Tie BI to certified Gold — block uncertified datasets',
      'Publish official KPI catalog + versioning',
      'Self-service enablement: training, templates, office hours',
      'Consolidate BI tools: 1-2 enterprise-wide, not 10',
      'Publish usage metrics + retire unused dashboards quarterly',
    ],
    recommendations: [
      'Semantic layer is mandatory with > 2 BI tools',
      'Certify Gold tables + publish the list; restrict uncertified',
      'Training matters — invest in data literacy',
      'Kill unused dashboards — every orphan erodes trust',
      'One metric, one definition, one owner',
    ],
    pitfalls: [
      'Letting every team pick BI tool → fragmentation',
      'KPI definitions in dashboards instead of semantic layer',
      'No retirement of old dashboards',
    ],
    kpis: [
      'KPI drift eliminated',
      'Self-service BI adoption > 50% of business users',
      'Certified Gold > 80% of BI usage',
      'BI tool consolidation achieved',
    ],
  },
  H: {
    title: 'H — AI / ML',
    challenges: [
      'Feature parity: offline looks great, production predictions wrong',
      'No MLOps: models deployed, never monitored, never retrained',
      'RAG hallucinations undermine user trust in GenAI',
      'Business outcomes untied to models — "AI theater"',
    ],
    steps: [
      'Clean Silver + governed Gold as ML foundation',
      'Deploy Feature Store (Databricks FS / Feast) for train/serve parity',
      'MLOps: experiment tracking + registry + serving + monitoring',
      'Offline evaluation pipelines (accuracy, drift, bias, RAGAS for LLM)',
      'Deploy with canary / shadow; monitor drift + feedback',
      'Retrain on drift-trigger (not fixed schedule)',
      'Tie every production model to business KPI',
    ],
    recommendations: [
      'Data quality before model quality — 80% of failure is data',
      'Feature Store is non-negotiable beyond 3 models',
      'RAG: retrieval quality first (chunking, metadata, hybrid + re-rank)',
      'Small fine-tuned often beats frontier — test before defaulting to GPT',
      'Human-in-loop for high-stakes decisions; capture feedback into retraining',
    ],
    pitfalls: [
      'Launching models without monitoring',
      'GenAI on bad data → public failure',
      'Treating AI as lab experiment vs production product',
    ],
    kpis: [
      'Train/serve skew eliminated',
      'Model deploy velocity up 3-5x',
      'Every model tied to business KPI',
      'Drift detection on all production models',
    ],
  },
  I: {
    title: 'I — Observability & Ops',
    challenges: [
      'Incidents discovered by consumers first',
      'Debugging takes hours; no lineage',
      'Alert fatigue; important signals missed',
      'Same incidents recur; no RCA discipline',
    ],
    steps: [
      'Instrument every pipeline: latency, volume, freshness, errors, cost',
      'Structured logs + OpenTelemetry traces + metrics',
      'End-to-end lineage (Unity Catalog / OpenLineage)',
      'Define SLOs per data product + error budgets',
      'Route alerts to domain on-call (PagerDuty / Opsgenie)',
      'Anomaly detection on leading indicators (volume, freshness, DQ score)',
      'Blameless RCA within 48h for P1/P2; track preventive actions',
    ],
    recommendations: [
      '5 Golden Signals per pipeline: latency, volume, freshness, errors, cost',
      'SLOs + error budgets per data product — not just global SLA',
      'Alerts only on leading indicators; alert fatigue kills on-call',
      'Lineage is mandatory — no Gold without documented upstream',
      'Recurrence rate is the real observability KPI — target < 5%',
    ],
    pitfalls: [
      'Alert proliferation without prioritization',
      'Logs without correlation IDs',
      'RCA theater — no preventive action closure',
    ],
    kpis: [
      'MTTR reduced 60-80%',
      'SLO compliance > 99%',
      'Recurrence rate < 5%',
      'Incidents discovered internally before consumers',
    ],
  },
  J: {
    title: 'J — Culture & Literacy',
    challenges: [
      'Data is an IT thing, not a business thing',
      'Business teams fear self-service; rely on spreadsheets',
      'No incentive to share or document',
      'Low literacy creates dependence on central team',
    ],
    steps: [
      'Publish data strategy + maturity scorecard to entire company',
      'Quarterly data literacy workshops + certifications',
      'Business-side data stewards as leadership role',
      'Celebrate data wins + post-mortems publicly (blameless)',
      'Incentivize data product publishing (internal marketplace)',
      'Executive KPIs include data maturity + data product adoption',
      'Hire data product managers, not just engineers',
    ],
    recommendations: [
      'Culture eats strategy — invest in literacy consistently',
      'Make data a board-level topic with regular reporting',
      'Reward documentation + sharing, not just delivery',
      'Data stewards are leadership roles, not side jobs',
      'Publish maturity score internally + externally — accountability',
    ],
    pitfalls: [
      'Treating culture as soft + deprioritized',
      'Training without follow-up coaching',
      'Letting central team become hero culture',
    ],
    kpis: [
      'Literacy certification % of workforce',
      'Data products published per domain',
      'Self-service BI adoption growing',
      'Maturity score in employee engagement surveys',
    ],
  },
};

// ── Production Support per level ─────────────────────────────────────
const SUPPORT = {
  L1: {
    title: 'L1 — Operations / Support',
    challenges: [
      'Alert volume high; signal-to-noise poor',
      'Runbooks missing or outdated',
      'Escalation criteria unclear',
      'Triage without context = unnecessary escalation',
    ],
    steps: [
      'Consolidate alerts into single pane (PagerDuty / Opsgenie)',
      'Runbook per pipeline with symptoms + first-aid + escalation',
      'Train L1 on platform (Airflow / Workflows / Databricks UI)',
      'SLA: acknowledge in 5 min, first-aid in 15 min',
      'Log every incident with standard template',
      'Daily handover between shifts with open incidents',
      'Monthly review: alert noise, false positives, runbook gaps',
    ],
    recommendations: [
      'Runbook completeness > 90% — measure this',
      'Alert only on leading indicators — noise kills effectiveness',
      'First-aid > escalation — L1 should resolve 60%+ without L2',
      'Standard incident template — enables trend analysis',
      'Shift handover is non-negotiable — document open incidents',
    ],
    pitfalls: [
      'Alert fatigue → ignored alerts → missed incidents',
      'Runbooks that are outdated the day they are written',
      'Escalating without first-aid attempt',
    ],
    kpis: [
      'Acknowledgment SLA > 99%',
      'First-aid resolution > 60%',
      'False positive rate < 10%',
      'Mean triage time decreasing',
    ],
  },
  L2: {
    title: 'L2 — Data Engineering',
    challenges: [
      'Root-cause debugging across distributed pipelines',
      'Data issues require reprocessing — cost + time',
      'Lineage gaps make impact analysis guesswork',
      'On-call burnout from repeated incidents',
    ],
    steps: [
      'Structured logs + traces with correlation IDs across pipeline stages',
      'End-to-end lineage (Unity Catalog / OpenLineage) for impact analysis',
      'Idempotent pipelines so rerun is safe (MERGE, batch IDs, checkpoints)',
      'Reconciliation jobs detect data issues before consumers',
      'SLA: L2 picks up from L1 within 30 min; resolve P1 within 4h',
      'Post-incident: write preventive action + track closure',
      'Share learnings cross-team via weekly incident review',
    ],
    recommendations: [
      'Invest in debuggability — logs, traces, lineage — always',
      'Idempotency is the foundation of safe rerun + recovery',
      'Every P1/P2 = blameless RCA within 48h + preventive action',
      'Weekly incident review across teams — knowledge compounds',
      'Rotate on-call fairly; burnout kills quality',
    ],
    pitfalls: [
      'Firefighting without RCA → same incident recurs',
      'Reprocessing entire history instead of targeted fix',
      'Fixing symptoms instead of root cause to close ticket',
    ],
    kpis: [
      'MTTR < 4h for P1',
      'Recurrence rate < 5%',
      'Preventive action closure > 90%',
      'Debug time decreasing',
    ],
  },
  L3: {
    title: 'L3 — Senior Engineer / Architect',
    challenges: [
      'Cross-team incidents blocked by ownership unclear',
      'Architectural fixes competing with feature work',
      'Learning loop: incidents → architecture evolution',
      'Translating ops signals to platform investment',
    ],
    steps: [
      'Lead blameless RCA for P1 incidents within 48h',
      'Propose architectural fixes: contracts, idempotency, observability',
      'Drive preventive action closure across teams (not just one)',
      'Quarterly: translate incident trends → platform roadmap items',
      'Mentor L2 on debugging, RCA, design',
      'Publish post-mortems + learnings enterprise-wide',
      'Tie reliability investment to business outcomes for leadership',
    ],
    recommendations: [
      'RCA quality > RCA speed — get to actual root cause',
      'Architect for reliability, not just features — 20% capacity',
      'Publish post-mortems publicly — culture of learning',
      'Mentor + grow L2s — bench strength is reliability insurance',
      'Translate ops pain into investment case for leadership',
    ],
    pitfalls: [
      'RCA theater — closed without real root cause',
      'Feature work crowding out reliability',
      'Keeping post-mortems private — limits learning',
    ],
    kpis: [
      'Recurrence rate trending to zero',
      'Platform roadmap driven by incident learnings',
      'L2 bench strength growing',
      'Reliability investment funded',
    ],
  },
  L4: {
    title: 'L4 — Platform / Vendor / SME',
    challenges: [
      'Deep system issues (Spark, Delta, cloud infra)',
      'Vendor SLA negotiation for critical incidents',
      'Cross-cloud / regional complexity',
      'Rare expertise concentration risk',
    ],
    steps: [
      'Vendor escalation relationships established + SLAs negotiated',
      'Platform SMEs on rotation for deep issues',
      'Access to vendor source code / roadmap for critical dependencies',
      'Knowledge sharing: platform SMEs teach L3/L2 via clinics',
      'Disaster recovery + region failover playbooks tested quarterly',
      'Upstream vendor bugs tracked + workarounds documented',
      'Strategic relationship with vendor — not just support tickets',
    ],
    recommendations: [
      'Build vendor relationship at executive level — not just tickets',
      'Negotiate SLA + escalation paths before you need them',
      'Platform SMEs must teach — concentration risk kills reliability',
      'DR tested quarterly — untested DR is no DR',
      'Track vendor bugs + known issues as architectural constraints',
    ],
    pitfalls: [
      'Relying on tickets without escalation relationship',
      'Untested DR plans',
      'SME concentration without knowledge sharing',
    ],
    kpis: [
      'Vendor SLA adherence tracked',
      'DR tests passing',
      'Knowledge spread across L3/L4',
      'Critical issues resolved within SLA',
    ],
  },
};

// ── Helpers ──────────────────────────────────────────────────────────
function getGuide(type, key) {
  const map = { maturity: MATURITY, roadmap: ROADMAP, assessment: ASSESSMENT, support: SUPPORT };
  const tree = map[type];
  if (!tree) return null;
  return tree[key] || null;
}

// ── Component ────────────────────────────────────────────────────────
function DeepGuide({ type, level, phase, dimension }) {
  const key = level || phase || dimension;
  const guide = getGuide(type, key);
  if (!guide) return null;

  return (
    <div
      style={{
        marginTop: '1rem',
        background: 'linear-gradient(135deg, #faf5ff 0%, #f5f3ff 100%)',
        border: '1px solid #ddd6fe',
        borderLeft: '4px solid #7c3aed',
        borderRadius: '12px',
        padding: '1.1rem 1.2rem',
      }}
    >
      <div
        style={{
          fontSize: '0.75rem',
          fontWeight: 800,
          textTransform: 'uppercase',
          letterSpacing: '0.08em',
          color: '#6d28d9',
          marginBottom: '0.75rem',
        }}
      >
        Architect Deep Guide — {guide.title}
      </div>

      <div style={{ display: 'grid', gap: '0.85rem' }}>
        {/* Challenges */}
        <div
          style={{
            background: '#fff',
            border: '1px solid #fecaca',
            borderLeft: '3px solid #ef4444',
            borderRadius: '8px',
            padding: '0.85rem 1rem',
          }}
        >
          <div
            style={{
              fontSize: '0.7rem',
              fontWeight: 700,
              textTransform: 'uppercase',
              color: '#991b1b',
              marginBottom: '0.4rem',
            }}
          >
            Challenges at This Stage
          </div>
          <ul style={{ margin: 0, paddingLeft: '1.1rem', color: '#1a1a1a' }}>
            {guide.challenges.map((c, i) => (
              <li key={i} style={{ fontSize: '0.85rem', lineHeight: 1.6, marginBottom: '0.25rem' }}>
                {c}
              </li>
            ))}
          </ul>
        </div>

        {/* Step Sequence */}
        <div
          style={{
            background: '#fff',
            border: '1px solid #bfdbfe',
            borderLeft: '3px solid #2563eb',
            borderRadius: '8px',
            padding: '0.85rem 1rem',
          }}
        >
          <div
            style={{
              fontSize: '0.7rem',
              fontWeight: 700,
              textTransform: 'uppercase',
              color: '#1d4ed8',
              marginBottom: '0.4rem',
            }}
          >
            Step-by-Step Action Sequence
          </div>
          <ol style={{ margin: 0, paddingLeft: '1.15rem', color: '#1a1a1a' }}>
            {guide.steps.map((s, i) => (
              <li key={i} style={{ fontSize: '0.85rem', lineHeight: 1.6, marginBottom: '0.3rem' }}>
                {s}
              </li>
            ))}
          </ol>
        </div>

        {/* Architect Recommendations */}
        <div
          style={{
            background: '#fff',
            border: '1px solid #bbf7d0',
            borderLeft: '3px solid #16a34a',
            borderRadius: '8px',
            padding: '0.85rem 1rem',
          }}
        >
          <div
            style={{
              fontSize: '0.7rem',
              fontWeight: 700,
              textTransform: 'uppercase',
              color: '#15803d',
              marginBottom: '0.4rem',
            }}
          >
            Architect Recommendations
          </div>
          <ul style={{ margin: 0, paddingLeft: '1.1rem', color: '#1a1a1a' }}>
            {guide.recommendations.map((r, i) => (
              <li key={i} style={{ fontSize: '0.85rem', lineHeight: 1.6, marginBottom: '0.3rem' }}>
                {r}
              </li>
            ))}
          </ul>
        </div>

        {/* Pitfalls + KPIs — two column */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.85rem' }}>
          <div
            style={{
              background: '#fff',
              border: '1px solid #fed7aa',
              borderLeft: '3px solid #f97316',
              borderRadius: '8px',
              padding: '0.85rem 1rem',
            }}
          >
            <div
              style={{
                fontSize: '0.7rem',
                fontWeight: 700,
                textTransform: 'uppercase',
                color: '#c2410c',
                marginBottom: '0.4rem',
              }}
            >
              Common Pitfalls
            </div>
            <ul style={{ margin: 0, paddingLeft: '1.05rem', color: '#1a1a1a' }}>
              {guide.pitfalls.map((p, i) => (
                <li
                  key={i}
                  style={{ fontSize: '0.82rem', lineHeight: 1.55, marginBottom: '0.25rem' }}
                >
                  {p}
                </li>
              ))}
            </ul>
          </div>

          <div
            style={{
              background: '#fff',
              border: '1px solid #a5f3fc',
              borderLeft: '3px solid #0891b2',
              borderRadius: '8px',
              padding: '0.85rem 1rem',
            }}
          >
            <div
              style={{
                fontSize: '0.7rem',
                fontWeight: 700,
                textTransform: 'uppercase',
                color: '#0e7490',
                marginBottom: '0.4rem',
              }}
            >
              Success KPIs
            </div>
            <ul style={{ margin: 0, paddingLeft: '1.05rem', color: '#1a1a1a' }}>
              {guide.kpis.map((k, i) => (
                <li
                  key={i}
                  style={{ fontSize: '0.82rem', lineHeight: 1.55, marginBottom: '0.25rem' }}
                >
                  {k}
                </li>
              ))}
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export default DeepGuide;
