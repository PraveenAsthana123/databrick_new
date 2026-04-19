import React from 'react';

/**
 * EnterpriseArchitectDetail — Renders 8 enterprise architect perspectives
 * on any topic/challenge/scenario:
 *   1. Selection  — why we picked this approach
 *   2. Rejection  — what we considered and rejected
 *   3. Edge Cases — boundary conditions + failure modes
 *   4. Challenges — what could go wrong
 *   5. Strategy   — north star
 *   6. Solution Sequence Plan — step-by-step rollout
 *   7. Test Plan  — validation steps
 *   8. Tech Stack — tools + platforms
 */

// ── Keyword-driven content generators ────────────────────────────────
const DOMAIN_TEMPLATES = {
  ingestion: {
    selection: [
      'Auto Loader (cloudFiles) selected over custom file pollers — native exactly-once, schema inference, notification mode',
      'Kafka / Event Hubs chosen for streaming over polling APIs — sub-second latency, replay semantics, decoupled producers',
      'Delta Lake as target format — ACID, time travel, schema evolution, efficient upserts',
      'Debezium selected for CDC over trigger-based approaches — log-based, no source-side impact',
    ],
    rejection: [
      'Custom Python file watchers — rejected due to exactly-once complexity and no native schema evolution',
      'JDBC full-table extracts for high-volume tables — rejected (source impact, long locks)',
      'Kafka Connect custom sinks — rejected in favor of Structured Streaming for native Delta integration',
      'Sqoop / legacy ETL — rejected as not cloud-native, no streaming support',
    ],
    edgeCases: [
      'Late-arriving data beyond watermark — route to reconciliation table with business-key match',
      'Schema drift (new column, type change) — auto-detect, land in Bronze, alert on Silver rejection',
      'Source reruns producing duplicate keys — idempotent MERGE with business key + batch ID',
      'Partial file writes mid-pipeline — use _SUCCESS markers, atomic rename, or checkpointing',
      'Source system outage mid-batch — resume from last committed offset, no gap no duplicate',
      'File larger than memory (multi-GB) — streaming read with trigger controls on batch size',
    ],
    challenges: [
      'Schema drift silently corrupting downstream consumers',
      'Late/duplicate data inflating metrics',
      'Source API rate limits blocking ingest',
      'Small files accumulating and tanking read performance',
      'Streaming checkpoint corruption after cluster crash',
    ],
    strategy:
      'Unified ingestion architecture: Auto Loader for files, CDC for transactional DBs, Kafka for events — all landing in Bronze Delta with idempotent MERGE, schema evolution, and reconciliation. Every source has a contract and a steward.',
    sequencePlan: [
      '1. Source inventory — catalog all systems, volumes, SLAs, owners',
      '2. Pattern selection — file / CDC / streaming / API per source',
      '3. Contract negotiation — schema, SLA, DQ expectations with producer teams',
      '4. Auto Loader / Kafka / Debezium deployment per source pattern',
      '5. Bronze write with idempotent MERGE + audit columns (_file, _ingested_ts, _batch_id)',
      '6. Reconciliation pipeline — source count vs Bronze count + checksum',
      '7. Observability — freshness, volume, error rate, schema drift alerts',
      '8. Runbook + on-call rotation — document every pipeline',
    ],
    testPlan: [
      'Unit tests — PySpark transformation functions (chispa / pytest)',
      'Contract tests — source schema matches registered schema (break build on mismatch)',
      'Volume tests — 10x production data in staging, measure latency',
      'Chaos tests — kill source mid-ingest, verify restart yields no dupes/gaps',
      'Schema evolution tests — add column, remove column, type change scenarios',
      'Reconciliation tests — source vs Bronze count diff < 0.01%',
      'E2E tests — full pipeline including downstream consumers',
      'Regression tests — re-run after every code change with same sample data',
    ],
    techStack:
      'Databricks Auto Loader, Structured Streaming, Delta Lake · Kafka / Event Hubs / Kinesis · Debezium (CDC) · Azure Data Factory / AWS Glue (orchestration) · Great Expectations / DLT expectations (DQ) · Unity Catalog (governance) · Datadog / OpenTelemetry (observability)',
  },
  modeling: {
    selection: [
      'Star schema over snowflake — denormalized facts/dims for BI performance',
      'SCD Type 2 for history — effective dating with is_current flag',
      'Surrogate keys over natural keys — decoupled from source volatility',
      'Medallion (Bronze/Silver/Gold) — clear ownership + quality gate per layer',
      'Delta Lake MERGE for upserts — idempotent, ACID, time travel',
    ],
    rejection: [
      'Wide denormalized tables — rejected (update anomalies, storage bloat)',
      'Snowflake schema — rejected for BI layer (too many joins)',
      'Data Vault for all dimensions — rejected as overkill, used only for auditable core',
      'SCD Type 1 (overwrite) — rejected for audit-sensitive attributes',
    ],
    edgeCases: [
      'Business key collisions across source systems — require golden-ID MDM',
      'Dimension attributes that change more than daily — switch to Type 6 hybrid',
      'Late-arriving facts referencing missing dim — handle with "Unknown" member',
      'Retroactive corrections to history — bi-temporal dim (valid_from + recorded_from)',
      'Hierarchy changes mid-year (reorg) — reserve both old and new hierarchies',
      'Very large dims (10M+ rows) — cluster by key + avoid Type 2 on non-PII changes',
    ],
    challenges: [
      'Wrong grain chosen upfront — forces full rebuild',
      'SCD complexity bleeding into fact tables',
      'KPI drift across teams with no semantic layer',
      'Join explosion from over-normalized design',
      'Dimension churn from source system updates',
    ],
    strategy:
      'Business-grain-first modeling: grain defined with stakeholders before DDL. Star schema for analytics, SCD Type 2 for history where audit requires, canonical dimensions (dim_customer, dim_product, dim_date) shared across marts. Semantic layer owns all KPIs.',
    sequencePlan: [
      '1. Engage business — define grain, conformed dims, key KPIs',
      '2. Canonical model — publish business glossary + ERD',
      '3. Build Silver layer — clean, deduped, conformed',
      '4. Build Gold layer — star schema with facts + dims',
      '5. Apply SCD Type 2 on dims needing history; Type 1 on reference',
      '6. Publish semantic layer — metrics owned centrally',
      '7. Validate with business users — sample queries match known truth',
      '8. Certify Gold tables + publish to BI consumers',
    ],
    testPlan: [
      'Grain tests — row count == expected grain count',
      'Referential integrity — no fact rows with missing dim keys',
      'SCD tests — is_current flag sums correctly, no overlap in effective dates',
      'KPI regression tests — daily snapshot vs yesterday must match for historical records',
      'Aggregation tests — sum of detail == sum of rollup (reconciliation)',
      'Join performance tests — EXPLAIN plan stays within target',
      'Business UAT — stakeholders validate sample reports',
    ],
    techStack:
      'Delta Lake · dbt (transformations) · Databricks SQL (semantic layer, metric views) · Unity Catalog (lineage, ownership, tags) · Spark SQL · Kimball / Data Vault patterns · Power BI / Tableau (consumers)',
  },
  governance: {
    selection: [
      'Unity Catalog selected as single governance plane — 3-level namespace + native Databricks integration',
      'Tag-driven policies over role-based views — scales better across thousands of tables',
      'System tables for audit — native, retained, queryable vs external SIEM sync',
      'Column-level masking at catalog layer — enforced regardless of consumer',
    ],
    rejection: [
      'Per-table ACL management — rejected (does not scale, drift-prone)',
      'External catalogs (Collibra-only) — rejected as sole source; used as complement to UC',
      'Manual access reviews — rejected in favor of quarterly automated attestation',
      'View-based row-level security — rejected for performance and management complexity',
    ],
    edgeCases: [
      'User changes role mid-day — SCIM sync must propagate within minutes, not hourly',
      'Shared dashboards accessed by departed employees — orphan detection in audit logs',
      'Break-glass access for incidents — auditable, time-bound, approved by CDO',
      'Cross-region data residency — region-specific metastores, federated catalogs',
      'PII in free-text fields (notes, comments) — NLP-based classification, not just column names',
      'Derived Gold containing masked columns — propagate masking via tag inheritance',
    ],
    challenges: [
      'Legacy Hive Metastore tables without ownership',
      'Shadow data created in workspace without catalog registration',
      'Policy conflicts between federated domains',
      'Governance seen as blocker by product teams',
      'Stale group memberships accumulating over years',
    ],
    strategy:
      'Governance-as-code via Unity Catalog: every table tagged with classification + owner, policies derived from tags, audit system tables retained 180 days, quarterly access reviews automated via SCIM. Federated domain ownership with central platform team.',
    sequencePlan: [
      '1. Deploy Unity Catalog metastore per region',
      '2. Migrate Hive Metastore tables with SYNC command',
      '3. Tag data — PII / Confidential / Public / Restricted at column level',
      '4. Write policy-as-code — row filters + column masks keyed on tags',
      '5. Assign owners — every production table has Owner / Steward / Custodian',
      '6. Enable system tables — audit, lineage, billing',
      '7. Wire SCIM from identity provider for group sync',
      '8. Quarterly access reviews — auto-revoke stale grants',
    ],
    testPlan: [
      'Policy tests — unauthorized users blocked, authorized users pass',
      'Tag propagation tests — derived tables inherit tags from upstream',
      'Audit coverage tests — every access appears in system.access.audit',
      'Penetration tests — quarterly red-team on critical catalogs',
      'Lineage accuracy tests — compare Unity Catalog lineage vs known DAG',
      'SCIM sync tests — user deprovision propagates within SLA',
      'Compliance audit tests — annual GDPR/SOX/HIPAA attestation',
    ],
    techStack:
      'Unity Catalog · Azure AD / Okta / AWS IAM (SCIM) · Collibra (business glossary) · OpenLineage · Monte Carlo (data observability) · Privacera / Immuta (PII scanning) · Key Vault (secrets) · SIEM integration',
  },
  visualization: {
    selection: [
      'Semantic layer (metric views) over per-dashboard KPI definitions — one metric, one truth',
      'DirectQuery for operational dashboards — near-real-time',
      'Import mode for executive dashboards — cached for predictable latency',
      'Row-level security via Unity Catalog — propagates to all BI tools',
    ],
    rejection: [
      'Every team picking their own BI tool — rejected, consolidated to 2 approved tools',
      'KPI logic in dashboard measures — rejected, moved to Gold / semantic layer',
      'Live Excel connections — rejected, security + performance risk',
      'Tableau extracts refreshed hourly — rejected in favor of scheduled DirectQuery',
    ],
    edgeCases: [
      'Executive asks for sliced view outside RLS scope — break-glass with audit log',
      'Report consumer in different timezone — server-side time zone conversion',
      'Historical comparison to pre-migration data — use certified historical snapshot',
      'High-cardinality filter (1M+ values) — server-side pagination in filter control',
      'Real-time dashboard lag during incident — fallback to cached view + banner',
      'Localization for multi-country reports — translation layer in semantic model',
    ],
    challenges: [
      'Slow dashboards driving business away to Excel',
      'KPI mismatch across tools eroding trust',
      'Adoption stalling despite investment',
      'BI cost ballooning with user growth',
      'Report sprawl with no retirement process',
    ],
    strategy:
      'Certified Gold + semantic layer + role-based dashboards. One metric definition, one owner, one calculation — everywhere. BI tools are presentation only; logic lives in Gold.',
    sequencePlan: [
      '1. Audit existing BI sprawl — count dashboards, users, owners',
      '2. Pick primary + secondary BI tools — consolidate',
      '3. Publish semantic layer / metric views on Gold',
      '4. Certify Gold tables — only certified exposed to BI',
      '5. Build role-based dashboards — exec, manager, analyst, partner',
      '6. Apply RLS + column masking — inherited from UC',
      '7. Enable alerts + subscriptions on KPI thresholds',
      '8. Retire uncertified / unused dashboards quarterly',
    ],
    testPlan: [
      'KPI consistency tests — same metric across dashboards = same value',
      'Performance tests — p95 dashboard load < 3s',
      'Security tests — RLS blocks unauthorized rows, CLS masks sensitive cols',
      'Reconciliation tests — dashboard totals match Gold table totals',
      'Drill-down tests — summary → detail returns matching rows',
      'Adoption tests — track MAU, session time, dashboards per user',
      'Accessibility tests — WCAG AA compliance',
    ],
    techStack:
      'Power BI / Tableau / Databricks SQL · dbt metrics / Databricks metric views (semantic layer) · Unity Catalog (RLS/CLS) · Databricks SQL Warehouse (compute) · Photon · SQL Analytics Dashboards · Alerting (PagerDuty / email)',
  },
  security: {
    selection: [
      'Zero Trust model — never trust, always verify — assume breach',
      'Unity Catalog for fine-grained access over per-table ACLs',
      'Customer-managed keys (CMK) for regulated workloads',
      'Private Link / no-public-IP for production workspaces',
      'Group-based grants via identity provider — never user-based',
    ],
    rejection: [
      'Shared service account credentials — rejected, use managed identity',
      'Password rotation via email — rejected in favor of vault + SSO',
      'IP allowlist as primary control — rejected (defense-in-depth complement only)',
      'Agent-based PII scanning — rejected in favor of UC tag-based policies',
    ],
    edgeCases: [
      'Credentials leaked in notebook — automated scanning in CI blocks merge',
      'Insider threat downloading bulk data — UEBA alerts on volume anomaly',
      'Third-party partner compromise — Delta Sharing recipient tokens rotated, audit logs reviewed',
      'Zero-day in DBR — vendor support escalation + quarantine workload',
      'Accidentally committed PII to Git — secrets scanning + force-push rewrite',
      'Audit finding in production catalog — break-glass remediation with CAB approval',
    ],
    challenges: [
      'Too many service principals with inherited access',
      'Secrets scattered in notebooks and configs',
      'Over-permissioned groups persisting from migrations',
      'Compliance team involved too late',
      'Encryption key rotation breaking jobs',
    ],
    strategy:
      'Defense-in-depth: IAM + network + encryption + monitoring + audit. Security embedded in CI/CD (DevSecOps), policies as code, every access logged, quarterly attestation.',
    sequencePlan: [
      '1. Threat model — identify crown jewels, attack surface, regulatory requirements',
      '2. Identity — SSO + MFA + SCIM sync; group-based grants',
      '3. Network — Private Link, no-public-IP, VNet injection',
      '4. Encryption — at-rest (CMK for regulated), in-transit (TLS 1.2+), in-use (confidential compute where possible)',
      '5. Secrets — migrate to vault, scan CI for leaks',
      '6. Data classification — tag PII/Confidential at column level',
      '7. Masking + RLS — policy-as-code on tags',
      '8. Audit + SIEM — system tables → SIEM → alerts',
    ],
    testPlan: [
      'Penetration tests — quarterly by external firm',
      'Secrets scan — every commit (pre-commit + CI)',
      'Access review tests — quarterly attestation, automated revocation',
      'Incident response drills — tabletop + red team',
      'Compliance audit tests — GDPR, SOX, HIPAA, PCI as applicable',
      'Encryption tests — verify at-rest + in-transit on every workspace',
      'Backup restore tests — quarterly DR drill',
    ],
    techStack:
      'Azure AD / Okta / AWS IAM · Unity Catalog · Key Vault / Secrets Manager · Private Link / VNet · TLS 1.2+, AES-256, CMK · Defender / GuardDuty · SIEM (Splunk / Sentinel) · Snyk / detect-secrets · Monte Carlo / UEBA',
  },
  ai: {
    selection: [
      'RAG over fine-tuning for knowledge questions — cheaper, auditable, easier to update',
      'Databricks Feature Store for train/serve parity',
      'MLflow for experiment tracking + model registry',
      'Hybrid retrieval (BM25 + vector) over pure vector — handles exact-match better',
      'Guardrails (input/output filters) on all customer-facing AI',
    ],
    rejection: [
      'Fine-tuning on every new knowledge update — rejected (cost, staleness, forgetting)',
      'Frontier LLM for all queries — rejected for cost, use smaller models where sufficient',
      'Notebook-based model serving — rejected for production, use Model Serving endpoints',
      'Manual prompt versioning — rejected, use Git + MLflow',
    ],
    edgeCases: [
      'LLM hallucinates plausible-but-wrong answer — require citations + guard on no-retrieval',
      'Prompt injection via user input — sanitize + guardrail + allowlist commands',
      'Model output contains PII — post-processing filter + classifier',
      'Retrieval returns stale doc — re-embed on doc update via CDC',
      'Training data contains poison — lineage to source + DQ checks + outlier detection',
      'Model latency spike under load — circuit breaker + cached fallback',
    ],
    challenges: [
      'Train/serve skew from inconsistent features',
      'RAG hallucinations undermining user trust',
      'Model drift undetected for weeks',
      'Cost of frontier LLM inference',
      'Regulatory ambiguity on AI governance',
    ],
    strategy:
      'Responsible AI-by-design: data quality before model quality, Feature Store for parity, offline eval before deploy, guardrails on all production AI, HITL for high-stakes, drift monitoring + retraining triggers.',
    sequencePlan: [
      '1. Data foundation — clean Silver, governed Gold, Feature Store',
      '2. Offline evaluation pipeline — accuracy, drift, bias, RAGAS metrics',
      '3. Model registry + versioning (MLflow)',
      '4. Serving — batch for periodic, online endpoint for real-time',
      '5. Guardrails — input sanitization, output filtering, refusal on no-retrieval',
      '6. Monitoring — drift (PSI/KS), prediction latency, cost per inference',
      '7. HITL integration — approval queue for high-stakes decisions',
      '8. Retraining triggers on drift — automated, with rollback',
    ],
    testPlan: [
      'Offline eval — accuracy, F1, RAGAS, BLEU per task',
      'Adversarial tests — prompt injection, jailbreak attempts',
      'Fairness tests — parity across demographic slices',
      'Load tests — p95 latency, cost per query',
      'Shadow deployment — compare new model predictions vs champion',
      'A/B tests — business outcome KPI over statistical significance window',
      'Compliance tests — explainability, auditability, consent verification',
    ],
    techStack:
      'Databricks Feature Store · MLflow · Model Serving · Mosaic AI / LangChain · Vector Search · RAGAS · Guardrails AI · NIST AI RMF / ISO 42001 · SHAP / LIME (XAI) · Evidently (drift) · Databricks Vector Search',
  },
  cost: {
    selection: [
      'Job Clusters for scheduled workloads — 40% cost reduction vs All-Purpose',
      'Photon for SQL — 2-3x speed, same cost envelope',
      'Cluster policies as enforcement — prevent runaway cost',
      'Spot/Preemptible for batch — 60% cost reduction on those jobs',
      'Tag-based chargeback — accountability drives optimization',
    ],
    rejection: [
      'All-Purpose clusters for scheduled jobs — rejected (2x cost, no isolation)',
      'Premium SKU for dev/test — rejected, use Standard',
      'Over-provisioned Reserved Instances — rejected, mix RI + Spot + On-Demand',
      'Manual cost reports — rejected, automated dashboards + alerts',
    ],
    edgeCases: [
      'Runaway query during peak — statement timeout + query size limit',
      'Forgotten GPU cluster over weekend — aggressive auto-termination + cost alert',
      'Storage growth outpacing revenue — lifecycle policies + retention review',
      'New team spinning up unmanaged clusters — cluster policies deny non-compliant',
      'Shared costs across domains — tags + chargeback dashboard per domain',
      'Free-tier features promoted to Premium unexpectedly — monthly SKU audit',
    ],
    challenges: [
      'Bill shock in month 2 — no budget alerts',
      'Teams lacking cost visibility',
      'Runaway queries with no safeguards',
      'Unused dev clusters accumulating',
      'Idle compute over weekends',
    ],
    strategy:
      'FinOps-as-culture: tags + chargeback + dashboards from day one. Cluster policies as enforcement. Quarterly cost reviews per domain. Right-size + auto-terminate + spot instances where safe.',
    sequencePlan: [
      '1. Tag every resource — team, project, env, cost-center',
      '2. Publish cost dashboards — per domain, per pipeline',
      '3. Cluster policies — max DBU, allowed instance types, auto-terminate',
      '4. Migrate scheduled jobs to Job Clusters',
      '5. Enable Photon + Serverless SQL where applicable',
      '6. Storage lifecycle — hot / cool / archive',
      '7. Monthly cost reviews with domain owners',
      '8. Quarterly FinOps optimization sprints',
    ],
    testPlan: [
      'Budget alert tests — trigger at 50/80/100% thresholds',
      'Policy enforcement tests — non-compliant cluster creation blocked',
      'Tag coverage tests — untagged resources flagged + auto-quarantined',
      'Cost regression tests — daily cost vs baseline; alert on > 20% spike',
      'Chargeback accuracy tests — sum of domains == total bill (± rounding)',
      'Optimization tests — before/after benchmark for OPTIMIZE, Photon, caching',
    ],
    techStack:
      'Databricks System Tables (billing.usage) · Cluster Policies · Photon · Serverless SQL · Azure Cost Management / AWS Cost Explorer · FinOps tools (CloudHealth, Finout, Vantage) · Tags + chargeback dashboards',
  },
  observability: {
    selection: [
      'OpenTelemetry for traces — vendor-neutral, standard',
      'Structured JSON logs — parseable, searchable, correlation IDs',
      'SLOs + error budgets per data product — SRE-style',
      'Data observability (Monte Carlo / Lightup) — catches silent failures',
      'PagerDuty for on-call — not shared inbox',
    ],
    rejection: [
      'Print-statement logging — rejected, structured only',
      'Per-team monitoring silos — rejected, unified observability',
      'Alert on every error — rejected, alert on SLO breach only',
      'Manual RCA without templates — rejected, structured postmortem process',
    ],
    edgeCases: [
      'Alert storm during cascading failure — dedup + rate limit',
      'Monitoring system itself down — meta-monitoring + redundant alerting',
      'False positive alerts eroding trust — review monthly, tune thresholds',
      'Lineage gap mid-pipeline — fallback to logs + manual trace',
      'Distributed trace spanning 100+ spans — sampling + top-N slow spans',
      'Metric cardinality explosion — limit labels + aggregate at collection',
    ],
    challenges: [
      'Silent data failures discovered by consumers',
      'MTTR high due to distributed debugging',
      'Alert fatigue from noisy alerts',
      'No SLOs — everything feels critical',
      'RCA recurrence not tracked',
    ],
    strategy:
      'Observability-as-foundation: 5 Golden Signals per pipeline, SLOs per data product, anomaly detection on leading indicators, blameless RCA with tracked preventive actions. Alerts route to domain on-call.',
    sequencePlan: [
      '1. Instrument every pipeline — latency, volume, freshness, errors, cost',
      '2. Centralize logs (Datadog / Splunk / Grafana Loki)',
      '3. End-to-end lineage (Unity Catalog / OpenLineage)',
      '4. Define SLOs per data product — freshness, completeness, accuracy',
      '5. Wire alerts to domain on-call (PagerDuty / Opsgenie)',
      '6. Data observability layer (Monte Carlo / Lightup)',
      '7. RCA process — blameless, 48h SLA, preventive action tracking',
      '8. Weekly observability review + monthly SLO review',
    ],
    testPlan: [
      'Alert tests — fire test alert, verify reaches on-call',
      'Runbook tests — on-call runs runbook in game day',
      'DR tests — simulate outage, measure MTTR',
      'Data observability tests — inject anomaly, verify detection',
      'SLO compliance tests — monthly review, error budget accounting',
      'Recurrence tracking tests — same RCA not repeating',
    ],
    techStack:
      'Datadog / Splunk / Grafana / CloudWatch · OpenTelemetry · PagerDuty / Opsgenie · Monte Carlo / Lightup / Soda · Unity Catalog system tables · Databricks lakehouse monitoring · SLO tools (Nobl9, OpenSLO)',
  },
  strategy: {
    selection: [
      'Lakehouse architecture (Bronze/Silver/Gold) — single platform for BI + ML',
      'Business-outcome KPIs — every investment tied to measurable value',
      'Phased roadmap (stabilize → standardize → optimize → AI) — avoid big bang',
      'Data-as-a-product mindset — owners, SLAs, consumers',
      'Federated governance with central platform — balance autonomy + consistency',
    ],
    rejection: [
      'Multi-lake / multi-warehouse sprawl — rejected, consolidate',
      'Top-down governance imposing brittle policies — rejected, enabling governance',
      'Centralized data team as bottleneck — rejected, federated domain ownership',
      'Technology-first strategy — rejected, business-outcome first',
    ],
    edgeCases: [
      'Regulatory change mid-roadmap — flex capacity, re-prioritize',
      'Merger / acquisition — integrate under canonical model, phased',
      'Key sponsor leaves — lock roadmap to measurable outcomes, not personalities',
      'Budget cut in Year 2 — focus on value-delivery phases, defer foundation',
      'Tech vendor lock-in risk — abstraction layers, open formats (Delta, Iceberg)',
      'Talent attrition — knowledge transfer + runbooks + no single-person dependencies',
    ],
    challenges: [
      'Business sponsors losing patience during foundation phase',
      'Tech debt from legacy accumulating faster than cleanup',
      'Governance perceived as slowing innovation',
      'AI hype driving skip-ahead to advanced phases',
      'Cost visibility prompting reactive cuts',
    ],
    strategy:
      'Phased transformation tied to business outcomes: stabilize (foundation) → standardize (quality + governance) → optimize (scale + cost) → AI (productize + automate). Each phase delivers measurable wins. Governance + FinOps embedded from day one.',
    sequencePlan: [
      '1. Vision — aligned with CEO/CDO, published 1-pager',
      '2. Maturity assessment — 10 dimensions, honest scoring',
      '3. Use-case catalog — scored on value × complexity × risk',
      '4. Target architecture — reference lakehouse + governance + FinOps',
      '5. Operating model — RACI, roles, domain ownership',
      '6. 12-24 month roadmap — phased, milestone-driven',
      '7. Quick wins in first 90 days — credibility + momentum',
      '8. Quarterly steering + annual re-baseline',
    ],
    testPlan: [
      'Maturity re-assessment quarterly — track delta',
      'KPI tracking — each initiative tied to metric',
      'Governance metrics — catalog coverage, policy compliance, audit findings',
      'Adoption metrics — self-service BI MAU, data product consumers',
      'Cost metrics — $/TB, $/query, $/prediction trend',
      'Business outcome attribution — revenue impact, cost savings, risk reduction',
    ],
    techStack:
      'Databricks Lakehouse · Unity Catalog · Delta Lake · dbt · MLflow · Feature Store · Azure / AWS / GCP · FinOps platform · Data observability · Collibra (glossary) · OKR platform (Ally / Jira aligns)',
  },
};

const DEFAULT = {
  selection: [
    'Chosen approach balances complexity, cost, and business value',
    'Preferred cloud-native managed services over self-hosted',
    'Prioritized reproducibility and automation over one-off solutions',
  ],
  rejection: [
    'Rejected ad-hoc / manual solutions — not maintainable at scale',
    'Rejected legacy on-prem equivalents — cost + scaling limits',
    'Rejected building from scratch where mature OSS/SaaS exists',
  ],
  edgeCases: [
    'Failure mid-operation — ensure idempotency and safe rerun',
    'Scale 10x beyond baseline — test + autoscale',
    'Missing / malformed input — validate and quarantine',
    'Concurrent access patterns — locking / isolation level',
  ],
  challenges: [
    'Ownership gaps once in production',
    'Cost scaling faster than value',
    'Governance added after the fact',
    'Integration with legacy systems',
  ],
  strategy:
    'Design for scale + governance + cost from day one. Business outcomes drive every decision. Federated ownership with central platform enablement.',
  sequencePlan: [
    '1. Assess current state + gaps',
    '2. Define target architecture',
    '3. Pilot on 1 use case',
    '4. Build platform primitives',
    '5. Scale to additional use cases',
    '6. Govern + certify outputs',
    '7. Observe + iterate',
    '8. Publish runbook + hand over to ops',
  ],
  testPlan: [
    'Unit tests for logic',
    'Integration tests for end-to-end',
    'Performance tests at 10x baseline',
    'Chaos tests for failure modes',
    'Security + governance tests',
    'UAT with business stakeholders',
  ],
  techStack: 'Databricks Lakehouse · Unity Catalog · Delta Lake · Spark · Python · SQL',
};

const KEYWORD_MAP = [
  ['ingestion', 'kafka', 'cdc', 'autoloader', 'auto loader', 'stream', 'file'],
  ['modeling', 'scd', 'star schema', 'dimension', 'fact', 'grain', 'silver', 'gold'],
  ['governance', 'unity', 'catalog', 'lineage', 'classification', 'audit', 'policy'],
  ['visualization', 'bi', 'dashboard', 'power bi', 'tableau', 'report', 'kpi'],
  ['security', 'iam', 'encryption', 'pii', 'rbac', 'zero trust', 'vault'],
  ['ai', 'ml', 'rag', 'llm', 'model', 'embedding', 'feature store'],
  ['cost', 'finops', 'chargeback', 'budget', 'tagging'],
  ['observability', 'monitor', 'alert', 'logs', 'metrics', 'sla', 'slo', 'rca'],
  ['strategy', 'roadmap', 'maturity', 'operating model'],
];
const DOMAINS = [
  'ingestion',
  'modeling',
  'governance',
  'visualization',
  'security',
  'ai',
  'cost',
  'observability',
  'strategy',
];

function inferDomain(text) {
  const t = String(text || '').toLowerCase();
  for (let i = 0; i < KEYWORD_MAP.length; i++) {
    if (KEYWORD_MAP[i].some((kw) => t.includes(kw))) return DOMAINS[i];
  }
  return null;
}

function getTemplate(domain) {
  return DOMAIN_TEMPLATES[domain] || DEFAULT;
}

// ── Component ────────────────────────────────────────────────────────
function EnterpriseArchitectDetail({ title, description, domain }) {
  const resolvedDomain = domain || inferDomain(`${title} ${description || ''}`) || 'strategy';
  const t = getTemplate(resolvedDomain);

  return (
    <div
      style={{
        marginTop: '1rem',
        borderRadius: '12px',
        overflow: 'hidden',
        border: '1px solid #c4b5fd',
      }}
    >
      <div
        style={{
          background: 'linear-gradient(135deg, #4c1d95 0%, #6d28d9 100%)',
          padding: '0.75rem 1rem',
          color: '#fff',
        }}
      >
        <div
          style={{
            fontSize: '0.7rem',
            fontWeight: 800,
            textTransform: 'uppercase',
            letterSpacing: '0.1em',
            color: '#ddd6fe',
            marginBottom: '0.25rem',
          }}
        >
          🏛️ Enterprise Architect — Complete Detail
        </div>
        <div style={{ fontSize: '0.95rem', fontWeight: 700 }}>{title}</div>
        {description && (
          <div style={{ fontSize: '0.82rem', color: '#e9d5ff', marginTop: '0.2rem' }}>
            {description}
          </div>
        )}
      </div>

      <div style={{ display: 'grid', gap: '0.75rem', padding: '0.85rem' }}>
        <Section
          title="1. Selection — Why We Chose This"
          color="#16a34a"
          bg="#f0fdf4"
          items={t.selection}
        />
        <Section
          title="2. Rejection — What We Considered + Rejected"
          color="#dc2626"
          bg="#fef2f2"
          items={t.rejection}
        />
        <Section
          title="3. Edge Cases — Boundary + Failure Modes"
          color="#ea580c"
          bg="#fff7ed"
          items={t.edgeCases}
        />
        <Section
          title="4. Challenges — What Could Go Wrong"
          color="#d97706"
          bg="#fffbeb"
          items={t.challenges}
        />
        <Paragraph
          title="5. Strategy — North Star"
          color="#7c3aed"
          bg="#faf5ff"
          text={t.strategy}
        />
        <Section
          title="6. Solution Sequence Plan — Step-by-Step Rollout"
          color="#2563eb"
          bg="#eff6ff"
          items={t.sequencePlan}
          ordered
        />
        <Section
          title="7. Test Plan — Validation"
          color="#0891b2"
          bg="#ecfeff"
          items={t.testPlan}
        />
        <Paragraph
          title="8. Tech Stack — Tools + Platforms"
          color="#0f172a"
          bg="#f8fafc"
          text={t.techStack}
          mono
        />
      </div>
    </div>
  );
}

function Section({ title, color, bg, items, ordered }) {
  const ListTag = ordered ? 'ol' : 'ul';
  return (
    <div
      style={{
        background: bg,
        borderLeft: `4px solid ${color}`,
        borderRadius: '8px',
        padding: '0.7rem 0.95rem',
      }}
    >
      <div
        style={{
          fontSize: '0.72rem',
          fontWeight: 800,
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          color,
          marginBottom: '0.4rem',
        }}
      >
        {title}
      </div>
      <ListTag style={{ margin: 0, paddingLeft: ordered ? '1.3rem' : '1.1rem' }}>
        {items.map((item, i) => (
          <li key={i} style={{ fontSize: '0.85rem', lineHeight: 1.6, color: '#1a1a1a' }}>
            {item}
          </li>
        ))}
      </ListTag>
    </div>
  );
}

function Paragraph({ title, color, bg, text, mono }) {
  return (
    <div
      style={{
        background: bg,
        borderLeft: `4px solid ${color}`,
        borderRadius: '8px',
        padding: '0.7rem 0.95rem',
      }}
    >
      <div
        style={{
          fontSize: '0.72rem',
          fontWeight: 800,
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          color,
          marginBottom: '0.4rem',
        }}
      >
        {title}
      </div>
      <p
        style={{
          margin: 0,
          fontSize: '0.85rem',
          lineHeight: 1.65,
          color: '#1a1a1a',
          fontFamily: mono ? 'Fira Code, Consolas, monospace' : 'inherit',
        }}
      >
        {text}
      </p>
    </div>
  );
}

export default EnterpriseArchitectDetail;
