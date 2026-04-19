import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';
import DeepDetailView from '../../components/architect/DeepDetailView';
import FileFormatRunner from '../../components/common/FileFormatRunner';

const challenges = [
  // ─── 1–10: Ingestion & Pipeline ───
  {
    id: 1,
    group: 'Ingestion & Pipeline',
    title: 'Schema Drift from Source Systems',
    why: 'Upstream teams add, rename, or remove columns without notice, breaking downstream pipelines silently.',
    impact:
      'Pipelines fail or produce null-filled columns; data consumers receive incomplete reports with no error.',
    solution:
      'Implement schema-on-read with schema evolution enabled in Delta Lake; add schema validation step that compares incoming schema against registered baseline and alerts on deviation.',
    tools:
      'Delta Lake schema evolution, Great Expectations, dbt schema tests, Kafka Schema Registry',
    interviewAnswer:
      'We implemented a schema registry with automated compatibility checks — any producer change triggers a contract validation step before data lands in the bronze layer, giving us advance warning before pipelines break.',
  },
  {
    id: 2,
    group: 'Ingestion & Pipeline',
    title: 'Late-Arriving Data',
    why: 'Events generated hours or days before their ingestion timestamp cause window aggregations and event-time joins to produce incorrect results.',
    impact:
      'Revenue metrics, funnel analytics, and SLA reports are understated for recent periods; corrections require expensive reprocessing.',
    solution:
      'Use watermarking in Spark Structured Streaming to tolerate late data up to a configurable threshold; design fact tables with separate event_time and load_time columns and partition on load_time for efficient late-data upserts.',
    tools:
      'Spark Structured Streaming watermarks, Delta Lake MERGE, Apache Flink event-time processing',
    interviewAnswer:
      'We designed our streaming pipeline with event-time watermarks set to 6 hours, and built a late-data reconciliation job that detects and upserts arrivals beyond the watermark into the corrected aggregation tables each morning.',
  },
  {
    id: 3,
    group: 'Ingestion & Pipeline',
    title: 'Duplicate Records at Ingestion',
    why: 'Network retries, at-least-once delivery guarantees, and manual re-runs produce duplicate events that inflate metrics if not removed.',
    impact:
      'Transaction counts, revenue totals, and user counts are overstated, leading to incorrect business decisions.',
    solution:
      'Assign a deterministic deduplication key (composite of source_id + event_timestamp); use Delta Lake MERGE with the dedup key as the join predicate to upsert, not append.',
    tools:
      'Delta Lake MERGE INTO, Kafka exactly-once semantics, dbt unique tests, Spark dropDuplicates',
    interviewAnswer:
      'We moved from append-only ingestion to a MERGE-based pattern using a composite natural key, which reduced our duplicate rate from 2.3% to near-zero and removed the need for expensive downstream deduplication jobs.',
  },
  {
    id: 4,
    group: 'Ingestion & Pipeline',
    title: 'Partial Pipeline Failure',
    why: 'A multi-step pipeline may succeed for some partitions and fail for others, leaving the target table in a mixed state.',
    impact:
      'Consumers see inconsistent data — some dates fully populated, others empty or partial — causing silent mis-reporting.',
    solution:
      'Wrap each pipeline stage in a transactional write using Delta Lake ACID guarantees; use checkpoint markers and idempotent writes so reruns safely overwrite only failed partitions.',
    tools:
      'Delta Lake transactions, Apache Airflow retry logic, Databricks Workflows with task retries, idempotent MERGE',
    interviewAnswer:
      'We restructured our pipelines to write to a staging location first, then atomically swap via Delta CLONE or MERGE only after all partitions pass validation, eliminating the partial-success state entirely.',
  },
  {
    id: 5,
    group: 'Ingestion & Pipeline',
    title: 'Source System Instability',
    why: 'Operational databases experience maintenance windows, rate limits, and unexpected outages that cause ingestion gaps without clear signals.',
    impact:
      'Missing data gaps are not discovered until consumers report stale dashboards, sometimes hours later.',
    solution:
      'Implement a freshness monitoring job that compares expected row counts against actuals per source; use circuit-breaker pattern to pause downstream processing and alert on-call when source availability drops below threshold.',
    tools:
      'Great Expectations freshness checks, Airflow sensors, PagerDuty alerting, Monte Carlo data observability',
    interviewAnswer:
      'We built a lightweight heartbeat table that each source system writes to every 15 minutes; our monitoring pipeline raises an alert and halts downstream processing within one polling cycle of a missed heartbeat.',
  },
  {
    id: 6,
    group: 'Ingestion & Pipeline',
    title: 'High-Volume Data Ingestion',
    why: 'Tens of billions of events per day cannot be processed sequentially; naive approaches create resource contention and multi-hour pipeline backlogs.',
    impact:
      'SLA breaches, stale gold-layer data, and cascading failures when partitions block each other.',
    solution:
      'Partition ingestion by natural shards (e.g., region or event_type); use auto-scaling clusters with dynamic allocation; enable Delta Lake auto-optimize and Z-order on high-cardinality query columns.',
    tools: 'Databricks Auto Loader, Delta Lake, Spark adaptive query execution, Kafka partitioning',
    interviewAnswer:
      'We redesigned our ingestion layer using Auto Loader with file notification mode and enabled Delta auto-compaction, which cut our peak cluster cost by 40% while maintaining a sub-30-minute bronze-to-gold SLA.',
  },
  {
    id: 7,
    group: 'Ingestion & Pipeline',
    title: 'API Rate Limits from SaaS Sources',
    why: 'Third-party SaaS APIs enforce per-minute or per-day call quotas; bulk extractions quickly exhaust limits and receive 429 responses.',
    impact:
      'Partial extracts with no clear indication of what data is missing, leading to gaps in CRM, billing, or marketing data.',
    solution:
      'Implement exponential backoff with jitter; use incremental pagination (cursor-based) rather than offset; cache API tokens; schedule extractions during off-peak windows; request elevated quotas from vendors.',
    tools:
      'Airbyte incremental sync, custom retry decorators, Redis token cache, Fivetran connector',
    interviewAnswer:
      'We switched from full-table polling to cursor-based incremental extraction with exponential backoff and jitter, reducing API calls by 85% while eliminating the quota exhaustion incidents we had been seeing nightly.',
  },
  {
    id: 8,
    group: 'Ingestion & Pipeline',
    title: 'File-Based Source Inconsistency',
    why: 'CSV/JSON files dropped by vendors have inconsistent delimiters, encodings, null representations, and header conventions across deliveries.',
    impact:
      'Parser failures or silent data corruption; different files parsed with different assumptions produce incompatible records in the same table.',
    solution:
      'Build a configurable file-format registry that maps each source to its parsing rules; validate parsed row counts and schema before committing to the target; quarantine malformed files with metadata for manual review.',
    tools:
      'Databricks Auto Loader with schema hints, Great Expectations, Delta Lake quarantine pattern, dbt staging models',
    interviewAnswer:
      'We created a vendor-specific parser configuration layer and a quarantine zone where files failing format checks are held with full diagnostic metadata, allowing analysts to remediate without blocking good files from the same batch.',
  },
  {
    id: 9,
    group: 'Ingestion & Pipeline',
    title: 'Change Data Capture Complexity',
    why: 'CDC streams from relational databases include inserts, updates, deletes, and schema changes that must be correctly sequenced and applied to maintain referential integrity.',
    impact:
      'Out-of-order application produces incorrect current state; missed deletes cause ghost records in downstream analytics.',
    solution:
      'Use Debezium or native CDC connectors to capture ordered log events; apply to Delta Lake via MERGE using the CDC operation type (insert/update/delete) and a log sequence number for ordering.',
    tools: 'Debezium, Kafka Connect, Delta Lake MERGE, Databricks DLT with SCD support, AWS DMS',
    interviewAnswer:
      'We built a CDC pipeline using Debezium into Kafka, then applied events to Delta Lake using ordered MERGE operations keyed on log sequence number, giving us a fully auditable, exactly-once current-state table.',
  },
  {
    id: 10,
    group: 'Ingestion & Pipeline',
    title: 'Streaming Backpressure',
    why: 'When a Kafka consumer or streaming job cannot keep up with producer throughput, the lag grows unboundedly, delaying all real-time metrics.',
    impact:
      'Real-time dashboards show minutes-to-hours-old data; operational decisions based on stale metrics cause business harm.',
    solution:
      'Set explicit maxOffsetsPerTrigger in Structured Streaming; scale consumers horizontally to match partition count; monitor consumer lag continuously and trigger auto-scaling when lag exceeds threshold.',
    tools:
      'Spark Structured Streaming, Kafka consumer group lag monitoring, Confluent Control Center, Databricks cluster auto-scaling',
    interviewAnswer:
      'We implemented consumer lag alerting with automatic scale-out triggers and capped batch sizes using maxOffsetsPerTrigger, which kept our streaming lag below 60 seconds even during a 10x traffic spike during a product launch.',
  },

  // ─── 11–20: Data Quality ───
  {
    id: 11,
    group: 'Data Quality',
    title: 'Missing / Null Data at Scale',
    why: 'Critical business columns are frequently null due to optional source fields, failed enrichment steps, or schema mismatches that pass silently.',
    impact:
      'Aggregations exclude rows silently; KPIs are understated; ML models trained on incomplete data produce biased predictions.',
    solution:
      'Define explicit nullability contracts per column in dbt; add Great Expectations completeness rules with thresholds; route high-null records to a quarantine layer for investigation rather than rejecting the entire batch.',
    tools: 'dbt not_null tests, Great Expectations, Delta Lake constraints, Soda Core',
    interviewAnswer:
      'We defined column-level completeness SLAs in dbt and enforced them as blocking pipeline gates, which reduced our critical-column null rate from 8% to under 0.1% within one quarter.',
  },
  {
    id: 12,
    group: 'Data Quality',
    title: 'Duplicate Records in Analytical Tables',
    why: 'Duplicates introduced during ingestion or join fanout accumulate in analytical layers where they are hard to detect without full table scans.',
    impact:
      'Revenue metrics, headcount counts, and transaction volumes are inflated, eroding trust in the data platform.',
    solution:
      'Run daily deduplication checks using COUNT vs COUNT(DISTINCT key); enforce unique constraints via Delta table constraints; add dbt unique tests on all primary and surrogate keys.',
    tools:
      'dbt unique tests, Delta Lake CHECK constraints, Apache Spark deduplication patterns, data quality dashboards',
    interviewAnswer:
      'We added dbt unique key tests to every gold-layer model and set up a daily reconciliation report comparing row counts against source system totals, catching duplicate-injection bugs within hours of their introduction.',
  },
  {
    id: 13,
    group: 'Data Quality',
    title: 'Inconsistent Data Formats',
    why: 'Date formats (MM/DD/YYYY vs YYYY-MM-DD), phone number representations, and currency codes vary across source systems and evolve over time.',
    impact:
      'Joins fail silently or produce cartesian products; date range queries miss records; international data is misclassified.',
    solution:
      'Standardize in the silver transformation layer using explicit parsing functions with strict error modes; maintain a format catalog per source; reject records with unparseable formats to a quarantine table.',
    tools:
      'dbt macros, Spark to_date with format string, Great Expectations regex rules, data dictionaries',
    interviewAnswer:
      'We centralized all format normalization into a silver-layer macro library and enforced parsing in strict mode, so format violations surface immediately as pipeline failures rather than silently producing null dates.',
  },
  {
    id: 14,
    group: 'Data Quality',
    title: 'Invalid Business Values',
    why: 'Source systems allow values outside business rules (negative ages, future birth dates, revenue > $1B for a single transaction) due to weak application-level validation.',
    impact:
      'Statistical summaries are distorted; outlier values bias ML models; regulatory reports contain invalid data.',
    solution:
      'Define domain value ranges and referential integrity rules as dbt tests or Great Expectations suites; route invalid records to a quality exceptions table with the violated rule captured for remediation.',
    tools:
      'dbt accepted_values and expression tests, Great Expectations, Monte Carlo anomaly detection, dbt-expectations package',
    interviewAnswer:
      'We mapped every business rule into a versioned Great Expectations suite that runs after each pipeline stage, and built a self-service exceptions dashboard so data stewards can triage and remediate invalid records without engineering involvement.',
  },
  {
    id: 15,
    group: 'Data Quality',
    title: 'Cross-System Data Mismatch',
    why: 'The same business entity (customer, order) is represented differently across CRM, ERP, and billing systems due to integration lag and reconciliation gaps.',
    impact:
      'Dashboards built from different systems show conflicting figures; finance cannot reconcile operational vs. accounting revenue.',
    solution:
      'Build a master reconciliation layer with source-system attribution; define a "system of record" per metric in the data dictionary; run automated cross-system balance checks daily and alert on deviations exceeding tolerance.',
    tools: 'dbt cross-model tests, reconciliation SQL patterns, Monte Carlo, Collibra data catalog',
    interviewAnswer:
      'We built a dedicated reconciliation layer that joins CRM and ERP order records daily, flags discrepancies above 0.1%, and routes unresolved mismatches to a data steward queue, which reduced our month-end close reconciliation effort by 60%.',
  },
  {
    id: 16,
    group: 'Data Quality',
    title: 'Completeness Monitoring at Partition Level',
    why: "A table may have full historical completeness but individual partitions (e.g., today's date) may be partially loaded, invisible to table-level checks.",
    impact:
      'Dashboards showing "today\'s data" are based on partial partitions, leading to understated daily metrics.',
    solution:
      'Implement partition-aware completeness checks that compare expected row counts per date partition against a rolling average of recent partitions; alert when any partition falls below 90% of expected volume.',
    tools:
      'Databricks system tables, Great Expectations partition checks, Airflow partition sensors, dbt freshness tests',
    interviewAnswer:
      'We built a partition completeness monitor that uses a 7-day rolling median as the baseline for each daily partition and alerts within 15 minutes of pipeline completion if any partition is more than 10% below baseline.',
  },
  {
    id: 17,
    group: 'Data Quality',
    title: 'Data Freshness Violations',
    why: 'Pipelines may silently stop updating tables; consumers assume data is current but are reading stale snapshots from hours or days ago.',
    impact:
      'Operational decisions (inventory replenishment, fraud alerts) based on stale data cause real business harm.',
    solution:
      'Tag every managed table with an expected_refresh_frequency metadata attribute; run a freshness watchdog that compares max(load_timestamp) against current_time and pages on-call when freshness SLA is breached.',
    tools:
      'dbt source freshness, Delta table properties, Databricks SQL alerts, Monte Carlo freshness monitors',
    interviewAnswer:
      'We deployed dbt source freshness checks on all critical tables with severity-tiered alerts — warn at 1.5x the expected interval, error at 2x — giving us automated SLA enforcement without manual monitoring.',
  },
  {
    id: 18,
    group: 'Data Quality',
    title: 'No Automated Validation in Pipelines',
    why: 'Pipelines without embedded quality checks pass bad data downstream silently; quality issues are discovered by consumers weeks later.',
    impact:
      'High remediation cost; stakeholder trust is damaged; historical corrections require expensive full reprocessing.',
    solution:
      'Embed quality gates as first-class pipeline steps using a quality framework; define pass/fail thresholds per check; block promotion from bronze to silver unless all critical checks pass.',
    tools:
      'Great Expectations, dbt tests, Soda Core, Delta Live Tables expectations, Databricks Workflows',
    interviewAnswer:
      'We made data quality checks a mandatory pipeline stage using Delta Live Tables expectations, so no record advances beyond the bronze layer without passing completeness, format, and referential integrity rules.',
  },
  {
    id: 19,
    group: 'Data Quality',
    title: 'Silent Failures — No Error Surfacing',
    why: 'Pipelines catch exceptions and log to files that nobody reads; data quality issues are buried and never acted on.',
    impact:
      'Problems accumulate for weeks before discovery; by then, significant downstream damage has occurred and fixes are expensive.',
    solution:
      'Centralize all pipeline errors and quality failures to a structured errors table; build an alerting layer that pages on critical failures immediately and surfaces warnings in a daily digest to data owners.',
    tools:
      'Databricks structured logging, PagerDuty, Slack webhook alerts, Airflow email operators, Grafana dashboards',
    interviewAnswer:
      'We built a centralized error catalog where every pipeline failure writes a structured record including error type, affected table, and row count, with automatic Slack and PagerDuty routing based on severity and domain ownership.',
  },
  {
    id: 20,
    group: 'Data Quality',
    title: 'Poor Data Lineage for Quality Debugging',
    why: 'When a quality issue is detected in a gold table, tracing it back to the root cause across 10+ pipeline hops is manual and time-consuming.',
    impact:
      'Mean time to resolution for data quality incidents is measured in days rather than hours.',
    solution:
      'Capture column-level lineage using Unity Catalog automatic lineage; annotate transformations with dbt lineage; build a lineage explorer UI that allows drilling from a failing metric back to the source system record.',
    tools: 'Databricks Unity Catalog lineage, dbt lineage graph, OpenLineage, Marquez, Collibra',
    interviewAnswer:
      'We integrated OpenLineage into our Airflow pipelines and surfaced it through a custom lineage UI, reducing our average data incident root-cause time from 4 hours to under 30 minutes.',
  },

  // ─── 21–30: Data Modeling ───
  {
    id: 21,
    group: 'Data Modeling',
    title: 'Wrong Grain in Fact Tables',
    why: 'Fact tables designed at the wrong granularity (e.g., daily instead of transaction-level) cannot answer all required questions without re-aggregation or re-ingestion.',
    impact:
      'Consumers must do ad-hoc workarounds; some queries become impossible; the table must be rebuilt when requirements change.',
    solution:
      'Define grain explicitly in the data dictionary before modeling; design fact tables at the lowest required grain; build aggregate tables as separate, clearly named objects rather than collapsing the base fact.',
    tools:
      'dbt model documentation, dimensional modeling (Kimball), data vault 2.0, data dictionary templates',
    interviewAnswer:
      "We established a mandatory grain declaration in every dbt model's description block and added a model review gate that requires explicit grain sign-off before merging new fact tables, which eliminated the grain mismatch incidents we had been experiencing.",
  },
  {
    id: 22,
    group: 'Data Modeling',
    title: 'Over-Normalization in Analytical Models',
    why: 'Applying OLTP normalization principles to analytical models forces consumers to join 15+ tables for basic queries, creating performance and usability problems.',
    impact:
      'Query complexity is too high for self-service analytics; poor performance on large datasets; high barrier to entry for business analysts.',
    solution:
      'Apply dimensional modeling principles: denormalize into wide fact tables and conformed dimensions; use dbt to build pre-joined gold models suited to specific business domains.',
    tools: 'Kimball dimensional modeling, dbt, Databricks Delta Lake, Star schema design',
    interviewAnswer:
      'We refactored our over-normalized silver models into domain-specific gold-layer star schemas using dbt, which reduced our most common analyst queries from 12-table joins to single-table scans and cut average query time by 70%.',
  },
  {
    id: 23,
    group: 'Data Modeling',
    title: 'Under-Normalization and Data Redundancy',
    why: 'Storing the same attribute in multiple tables without a single source of truth causes update anomalies and inconsistent reporting across domains.',
    impact:
      'The same metric returns different values from different dashboards; update operations must be replicated across tables manually.',
    solution:
      'Extract shared attributes into conformed dimensions with a single authoritative source; use dbt refs to ensure all models consume from the canonical dimension table.',
    tools:
      'dbt conformed dimensions, Unity Catalog table references, data catalog ownership tagging',
    interviewAnswer:
      'We identified 23 duplicated attribute definitions across our models and consolidated them into 8 conformed dimensions, after which all business dashboards reported consistent values from a single authoritative source.',
  },
  {
    id: 24,
    group: 'Data Modeling',
    title: 'Slowly Changing Dimension Complexity',
    why: 'Tracking historical changes to customer, product, or employee attributes (SCD Type 2) requires careful key management and increases model complexity significantly.',
    impact:
      'Historical reports show incorrect current-state attributes; merging SCD tables with facts at the wrong key causes fanout or dropped rows.',
    solution:
      'Implement SCD Type 2 with a surrogate key, effective_date, expiry_date, and is_current flag using Delta Lake MERGE; test all temporal join patterns with boundary-condition unit tests.',
    tools: 'dbt snapshots (SCD Type 2), Delta Lake MERGE, surrogate key generation, dbt_utils',
    interviewAnswer:
      'We standardized on dbt snapshots for all SCD Type 2 dimensions with automated boundary-condition tests, which reduced our SCD-related data errors to zero while cutting the implementation time for new history-tracking dimensions by 60%.',
  },
  {
    id: 25,
    group: 'Data Modeling',
    title: 'Schema Evolution Breaking Downstream Models',
    why: 'Adding or removing columns in upstream models breaks downstream dbt models and BI tool extracts without warning when schema changes are not managed.',
    impact:
      'Pipeline failures cascade across domains; BI dashboards go blank; analysts are blocked until engineers manually fix each dependency.',
    solution:
      'Use Delta Lake schema evolution with mergeSchema for additive changes; for breaking changes, version the model (v1 → v2) and maintain both until consumers migrate; use dbt select to identify all downstream dependents before a change.',
    tools:
      'Delta Lake schema evolution, dbt lineage graph, dbt deprecation warnings, API versioning patterns',
    interviewAnswer:
      'We introduced a schema change policy requiring impact analysis via the dbt lineage graph before any breaking change, and enforced a dual-model pattern for transitions, giving consumers a 2-sprint migration window before deprecation.',
  },
  {
    id: 26,
    group: 'Data Modeling',
    title: 'Data Skew in Partitioned Tables',
    why: 'Partitioning by a low-cardinality column (e.g., country) or a high-frequency value creates partitions orders of magnitude larger than others, causing resource imbalance.',
    impact:
      'Spark tasks on large partitions stall while small-partition tasks finish; overall job runtime is dominated by the slowest partition.',
    solution:
      "Choose partition columns with high cardinality and uniform distribution; add a sub-partitioning hash bucket for skewed keys; use Spark's AQE skew join optimization for runtime mitigation.",
    tools:
      'Delta Lake partitioning, Spark AQE (Adaptive Query Execution), Z-Order clustering, salting techniques',
    interviewAnswer:
      'We identified our top-3 skewed partition keys using Delta file size analysis and applied hash-based sub-partitioning with AQE skew join hints, reducing our worst-case query runtime from 4 hours to 35 minutes.',
  },
  {
    id: 27,
    group: 'Data Modeling',
    title: 'Join Explosion (Fanout)',
    why: 'Many-to-many relationships or non-unique join keys cause row count multiplication during joins, producing inflated metrics that are difficult to diagnose.',
    impact:
      'Revenue, event, and activity counts are inflated by factors of 2–100x; engineers spend days debugging before finding the join key issue.',
    solution:
      'Audit join keys for uniqueness before building models; add dbt relationship tests; document expected cardinality in the model description; use window functions rather than joins for ratio-style calculations.',
    tools: 'dbt relationship tests, dbt_utils.equality, Spark EXPLAIN plans, data profiling tools',
    interviewAnswer:
      'We added mandatory cardinality documentation to every dbt model and enforced join key uniqueness tests in CI, catching three fanout-causing model defects during code review before they ever reached production.',
  },
  {
    id: 28,
    group: 'Data Modeling',
    title: 'Surrogate vs. Natural Key Management',
    why: 'Inconsistent key strategy — mixing natural keys from source systems with generated surrogate keys — creates join ambiguity and makes master data integration fragile.',
    impact:
      'Different teams join on different keys and get different results; entity resolution across systems is unreliable.',
    solution:
      'Adopt a consistent key strategy: surrogate keys (UUID or hash) for all dimensions, natural keys stored as attributes for source traceability; build a master key table for cross-system entity resolution.',
    tools:
      'dbt_utils.generate_surrogate_key, entity resolution (Zingg, Splink), data vault satellite modeling',
    interviewAnswer:
      'We standardized on hash-based surrogate keys generated from the composite natural key in dbt and built a cross-system entity resolution service that resolved 1.2M customer duplicates across our 5 source systems.',
  },
  {
    id: 29,
    group: 'Data Modeling',
    title: 'Fact/Dimension Misalignment',
    why: 'Fact tables reference dimension keys that do not exist in the dimension table, causing outer join workarounds and silent data loss on inner joins.',
    impact:
      'Metrics lose rows silently when joined; analysts get different totals depending on join type; referential integrity violations are invisible.',
    solution:
      'Add dbt relationship tests that validate every foreign key in a fact table against the corresponding dimension; use a "not found" dimension row (key = -1) to capture orphan fact records rather than losing them.',
    tools:
      'dbt relationship tests, surrogate key patterns, Kimball "unknown dimension" member pattern',
    interviewAnswer:
      'We implemented dbt relationship tests on all fact-to-dimension foreign keys and introduced an "unknown" placeholder dimension row, eliminating silent data loss on joins and giving us a clear count of unresolvable foreign keys.',
  },
  {
    id: 30,
    group: 'Data Modeling',
    title: 'No Canonical Data Model',
    why: 'Each team builds its own model of the same business entity (customer, product) resulting in 15 different definitions of "active customer" across the company.',
    impact:
      'Executive dashboards show conflicting numbers; cross-team analysis is impossible; model proliferation creates maintenance burden.',
    solution:
      'Establish a canonical model working group with domain ownership; publish conformed entity definitions in the data catalog with approved metrics; enforce via dbt package that all models must import from the canonical layer.',
    tools:
      'Data catalog (Collibra, Atlan, Unity Catalog), dbt packages, data mesh domain ownership model',
    interviewAnswer:
      'We formed a canonical model guild with one domain owner per core entity, published the definitions in our data catalog, and made the canonical dbt package a mandatory import, reducing our "active customer" definition count from 15 to 1.',
  },

  // ─── 31–40: Performance ───
  {
    id: 31,
    group: 'Performance',
    title: 'Processing Terabytes of Daily Data',
    why: 'Full table scans on multi-terabyte tables are required by improperly designed queries or pipelines that do not leverage partition pruning.',
    impact:
      'Multi-hour query runtimes; excessive compute cost; SLA breaches for time-sensitive pipelines.',
    solution:
      'Enforce partition pruning by always filtering on the partition column; use Delta Lake Z-Order on secondary high-cardinality columns; enable AQE; use columnar pruning by selecting only required columns.',
    tools:
      'Delta Lake partitioning + Z-Order, Spark AQE, Photon engine, Databricks cluster right-sizing',
    interviewAnswer:
      'We added a query linting rule that blocks any query touching more than 10% of a partitioned table without a partition filter, and combined it with Z-Order on the most common non-partition filter columns, reducing scan costs by 65%.',
  },
  {
    id: 32,
    group: 'Performance',
    title: 'Small File Problem',
    why: 'Streaming or frequent-batch pipelines produce thousands of small Parquet files; each file requires a separate metadata operation during query planning.',
    impact:
      'Query planning overhead dominates runtime; simple queries take minutes due to thousands of file opens rather than seconds.',
    solution:
      'Enable Delta Lake auto-optimize (auto compaction + optimized writes) to coalesce small files automatically; run periodic OPTIMIZE commands during off-peak hours; use trigger(availableNow=True) for batch-mode streaming to produce fewer, larger files.',
    tools:
      'Delta Lake OPTIMIZE, auto-optimize, optimizedWrite, Databricks Delta table maintenance jobs',
    interviewAnswer:
      "We enabled Delta auto-optimize with optimizedWrite on all streaming tables and scheduled weekly OPTIMIZE + ZORDER runs, which reduced our worst-performing table's file count from 2.4M to 180K and cut query time from 8 minutes to 45 seconds.",
  },
  {
    id: 33,
    group: 'Performance',
    title: 'Poor Partition Strategy',
    why: 'Over-partitioning (too many small partitions) or under-partitioning (one massive partition) both cause performance degradation for different reasons.',
    impact:
      'Either query planning overhead or full-partition scans that defeat the purpose of partitioning entirely.',
    solution:
      'Target 1GB+ per partition file after compaction; partition only on columns used in WHERE clauses of most queries; use date-level partitioning for time-series data; use liquid clustering for tables with multiple query patterns.',
    tools:
      'Delta Lake liquid clustering (Databricks), partition analysis queries, ANALYZE TABLE, data profiling',
    interviewAnswer:
      'We migrated our 200+ table estate from ad-hoc partitioning to a standardized strategy using liquid clustering for multi-pattern tables, which reduced our cross-team query performance complaints by 80% within one month.',
  },
  {
    id: 34,
    group: 'Performance',
    title: 'Data Skew in Distributed Joins',
    why: 'A single partition key value (e.g., a popular customer_id) processes 10M rows while others process 100, causing one Spark task to stall the entire job.',
    impact:
      'Entire job runtime is determined by the single slowest task; cluster resources sit idle waiting for it.',
    solution:
      'Enable Spark AQE with skew join optimization; use salting technique to distribute skewed keys; broadcast small dimensions to avoid shuffle joins entirely; identify skewed keys via task metric analysis.',
    tools:
      'Spark AQE skewJoin, broadcast hints, salting pattern, Databricks Spark UI task analysis',
    interviewAnswer:
      'We used Spark UI task metrics to identify our top-10 skewed join keys and applied targeted broadcast hints for the dimension side and salting for the fact side, which eliminated our 6-hour skew-caused stalls.',
  },
  {
    id: 35,
    group: 'Performance',
    title: 'Inefficient Query Patterns',
    why: 'SELECT *, unreferenced subqueries, correlated subqueries, and missing predicate pushdown patterns generate far more data movement than necessary.',
    impact:
      'Cluster compute cost is 5–20x higher than optimal; query times are unnecessarily long; shared cluster contention increases.',
    solution:
      'Establish a query review process; add linting rules (no SELECT *, correlated subqueries require review); use EXPLAIN to verify predicate pushdown; push filters as early as possible in transformation chains.',
    tools:
      'Databricks SQL EXPLAIN, query profiler, dbt model optimization, Photon engine, ANALYZE TABLE statistics',
    interviewAnswer:
      'We introduced automated EXPLAIN-based query analysis in our CI pipeline that flags queries with full table scans or missing predicate pushdown, and mandated query profiling sign-off for any job consuming more than 100 DBU.',
  },
  {
    id: 36,
    group: 'Performance',
    title: 'High Query Concurrency Contention',
    why: 'Multiple teams running ad-hoc queries simultaneously on a shared cluster cause resource contention, queue buildup, and unpredictable query latencies.',
    impact:
      'Critical operational queries are delayed by analytical workloads; SLA-bound jobs miss their windows.',
    solution:
      'Implement workload isolation using Databricks SQL warehouses with separate pools per team type (batch, interactive, BI); use query queuing with priority tiers; enable serverless warehouses for elastic burst capacity.',
    tools:
      'Databricks SQL warehouses, serverless compute, cluster policies, workload isolation, Spark fair scheduler',
    interviewAnswer:
      'We separated our workloads into dedicated SQL warehouses by tier (critical SLA, interactive analytics, ad-hoc), which eliminated priority inversion and reduced our P99 interactive query latency from 4 minutes to under 20 seconds.',
  },
  {
    id: 37,
    group: 'Performance',
    title: 'Long-Running Pipeline Bottlenecks',
    why: 'Pipeline stages are executed sequentially when they could run in parallel; single-threaded Python transformations create artificial bottlenecks.',
    impact:
      'Pipeline completion time is the sum of all stage times rather than the maximum of parallelizable stages.',
    solution:
      'Model pipeline as a directed acyclic graph; execute independent branches in parallel using Airflow/Databricks Workflows fan-out; replace Python loops with Spark distributed transformations.',
    tools:
      'Apache Airflow dynamic task mapping, Databricks Workflows parallel tasks, Spark parallelism, dbt parallelism config',
    interviewAnswer:
      'We refactored our sequential 6-hour pipeline into a DAG with 3 parallel branches and replaced Python row iteration with Spark vectorized operations, cutting total runtime from 6 hours to 90 minutes.',
  },
  {
    id: 38,
    group: 'Performance',
    title: 'Storage Inefficiency and High Storage Cost',
    why: 'Tables accumulate stale Delta versions, duplicated data, and uncompressed files; storage costs grow faster than business data volume.',
    impact:
      'Monthly cloud storage bills grow 30–50% year-over-year without delivering proportional business value.',
    solution:
      'Enforce Delta VACUUM to remove old versions beyond retention period; enable Z-Standard compression; implement data lifecycle policies (hot → warm → cold → archive); audit and drop unused tables.',
    tools:
      'Delta Lake VACUUM, OPTIMIZE, lifecycle policies, AWS S3 Intelligent-Tiering, Azure Blob tiering',
    interviewAnswer:
      'We implemented automated VACUUM jobs on a 7-day retention policy and migrated historical data to cold storage tiers, reducing our storage cost by $40K/month with no impact on query performance for current-period data.',
  },
  {
    id: 39,
    group: 'Performance',
    title: 'No Result Caching for Repeated Queries',
    why: 'Dashboards refresh every 15 minutes and re-execute expensive aggregation queries against large tables even when underlying data has not changed.',
    impact:
      'Unnecessary compute spend; cluster load peaks at dashboard refresh intervals; users experience slow dashboard loads.',
    solution:
      'Enable Databricks SQL result cache for idempotent queries; implement materialized gold tables that are refreshed only when upstream data changes; use incremental materialization for large aggregations.',
    tools:
      'Databricks SQL result cache, Delta Lake incremental refresh, dbt incremental models, Looker PDT caching',
    interviewAnswer:
      'We redesigned our BI layer to read from pre-aggregated Delta gold tables refreshed on a 1-hour schedule, eliminating the repeated large aggregation queries and reducing our BI cluster DBU consumption by 55%.',
  },
  {
    id: 40,
    group: 'Performance',
    title: 'Real-Time Scaling Under Bursty Load',
    why: 'Streaming pipelines must handle traffic spikes (flash sales, viral events) that are 10–100x normal load without falling behind or dropping events.',
    impact:
      'Consumer-facing real-time features degrade; Kafka lag spikes; downstream metrics become stale during peak moments.',
    solution:
      'Design streaming clusters with auto-scaling enabled and pre-warmed capacity for known peak events; partition Kafka topics to match maximum expected parallelism; use Structured Streaming with trigger(processingTime) for stable throughput.',
    tools:
      'Databricks cluster auto-scaling, Kafka topic partitioning, Spark Structured Streaming, Confluent Cloud elastic compute',
    interviewAnswer:
      'We implemented pre-warming scripts that scale our streaming clusters to peak capacity 30 minutes before scheduled high-traffic events and enabled auto-scale for unplanned spikes, achieving zero lag breaches during our largest product launch.',
  },

  // ─── 41–50: Pipeline Orchestration ───
  {
    id: 41,
    group: 'Pipeline Orchestration',
    title: 'Complex Dependency Management',
    why: 'Hundreds of interdependent pipelines with implicit execution order assumptions are difficult to maintain and prone to race conditions.',
    impact:
      'Pipelines run out of order; downstream tables are built on incomplete upstream data; debugging dependency failures is extremely time-consuming.',
    solution:
      'Model all dependencies as an explicit DAG using a modern orchestrator; enforce data-driven triggers (table sensors) rather than time-based triggers; document inter-team dependencies in a central registry.',
    tools: 'Apache Airflow, Databricks Workflows, Prefect, Dagster, dbt DAG, table sensors',
    interviewAnswer:
      'We migrated from cron-based scheduling to an Airflow DAG with explicit data-driven sensors at each stage boundary, eliminating the race conditions that had been causing weekly pipeline failures.',
  },
  {
    id: 42,
    group: 'Pipeline Orchestration',
    title: 'Partial Failures Without Clear Recovery',
    why: 'When a multi-task pipeline fails midway, the recovery path is unclear — rerunning from the start is wasteful but resuming from the failure point requires careful state management.',
    impact:
      'Long pipelines either waste hours reprocessing completed work or require manual engineering effort to determine the safe restart point.',
    solution:
      'Design pipelines with idempotent tasks and explicit checkpointing; use Databricks Workflows task-level retry and resume-from-failure; persist task completion markers to a control table.',
    tools:
      'Databricks Workflows task retry, Airflow task instance state, checkpoint tables, idempotent MERGE patterns',
    interviewAnswer:
      'We redesigned each pipeline task to be idempotent using MERGE-based writes and added a task completion checkpoint table, so any failed run can safely resume from the last successful task without reprocessing completed work.',
  },
  {
    id: 43,
    group: 'Pipeline Orchestration',
    title: 'Non-Idempotent Pipeline Runs',
    why: 'Pipelines that append data on each run without checking for prior runs produce duplicates when retried; time-based logic (NOW()) makes outputs non-deterministic.',
    impact:
      'Failed-and-retried pipelines double-count data; manual deduplication is required after every incident.',
    solution:
      'Replace all append operations with MERGE using a deduplication key; parameterize pipelines with explicit date range arguments rather than using CURRENT_DATE; test idempotency by running twice and verifying identical output.',
    tools: 'Delta Lake MERGE INTO, parameterized Databricks jobs, idempotency testing framework',
    interviewAnswer:
      'We replaced all pipeline INSERT operations with MERGE operations using natural key deduplication and parameterized all date references, allowing safe reruns after any failure without producing duplicates.',
  },
  {
    id: 44,
    group: 'Pipeline Orchestration',
    title: 'Historical Backfill at Scale',
    why: 'Running a newly designed pipeline over 3 years of historical data requires processing 1000x the normal daily volume in a controlled, observable manner.',
    impact:
      'Naive backfills overwhelm clusters, blow budgets, and produce incorrect results if the pipeline is not designed for date-range parameterization.',
    solution:
      'Build backfill mode into pipeline design from day one with explicit date-range parameterization; use Airflow backfill with catchup=True and concurrency limits; monitor cost per partition during backfill.',
    tools:
      'Airflow backfill, Databricks multi-task workflows, date-range parameterization, cost monitoring',
    interviewAnswer:
      'We designed all pipelines with an explicit --start-date --end-date parameter from the start, and ran backfills in rolling 30-day windows with cost guardrails, completing a 3-year historical load in 4 days without exceeding our weekly compute budget.',
  },
  {
    id: 45,
    group: 'Pipeline Orchestration',
    title: 'Retry Strategy Causing Data Corruption',
    why: 'Aggressive automatic retries on non-idempotent tasks append duplicate data or overwrite partial writes in ways that corrupt the target table.',
    impact:
      'After a transient failure, the pipeline succeeds on retry but the output contains doubled records or partially overwritten data.',
    solution:
      'Gate retries on task idempotency — only automatically retry tasks that have been verified as idempotent; use exponential backoff; send non-idempotent failures to manual review queue rather than auto-retrying.',
    tools:
      'Databricks Workflows retry policy, Airflow retry_delay, idempotency classification per task type',
    interviewAnswer:
      'We classified every pipeline task as idempotent or non-idempotent, applied automatic retry only to the former category, and required human approval to replay non-idempotent tasks, eliminating the data corruption we had seen from blind retries.',
  },
  {
    id: 46,
    group: 'Pipeline Orchestration',
    title: 'Scheduling Conflicts and Resource Contention',
    why: 'Multiple heavy pipelines scheduled at the same clock time compete for shared cluster resources, causing queue buildup and SLA breaches.',
    impact:
      'The 6 AM critical reporting pipeline takes 4 hours instead of 1 because it is competing with 20 other jobs.',
    solution:
      'Stagger pipeline start times based on priority and resource requirements; use data-driven triggers rather than fixed schedules; implement resource pools with guaranteed capacity for critical workloads.',
    tools:
      'Airflow priority queues, Databricks cluster policies, workload resource pools, jitter-based scheduling',
    interviewAnswer:
      'We implemented priority-based scheduling with guaranteed cluster slots for critical pipelines and staggered start times for non-critical jobs, reducing our worst-case critical pipeline queue wait from 90 minutes to under 5 minutes.',
  },
  {
    id: 47,
    group: 'Pipeline Orchestration',
    title: 'Environment Drift Between Dev and Prod',
    why: 'Pipelines tested in dev use different cluster sizes, library versions, and data volumes than production, causing failures that only manifest in production.',
    impact:
      'Production incidents caused by environment differences; confidence in dev testing is low.',
    solution:
      'Use infrastructure-as-code for cluster definitions; pin library versions in requirements.txt; test on production-scale data samples in a dedicated staging environment; use Databricks Asset Bundles for consistent deployment.',
    tools:
      'Databricks Asset Bundles, Terraform, Docker for Python envs, pinned requirements.txt, staging environment',
    interviewAnswer:
      'We adopted Databricks Asset Bundles with Terraform for all cluster and job definitions, ensuring identical configuration across dev, staging, and prod, which reduced production-only failures from 3 per month to zero.',
  },
  {
    id: 48,
    group: 'Pipeline Orchestration',
    title: 'No Pipeline Observability',
    why: 'Pipeline success/failure is known but duration trends, throughput metrics, data volume, and SLA compliance are not tracked.',
    impact:
      'Gradual performance degradation goes unnoticed until it becomes a crisis; capacity planning is impossible.',
    solution:
      'Emit structured metrics (duration, row count, error count, bytes processed) to a monitoring table after each pipeline run; build dashboards tracking rolling P50/P95 runtime and SLA compliance rate.',
    tools:
      'Databricks system.lakeflow tables, Airflow StatsD metrics, Prometheus, Grafana, custom pipeline metrics table',
    interviewAnswer:
      'We built a pipeline metrics system that writes structured run summaries to a Delta table after each execution, then surfaced P50/P95 runtime trends and SLA compliance rates in a Grafana dashboard consumed by both engineering and data leadership.',
  },
  {
    id: 49,
    group: 'Pipeline Orchestration',
    title: 'Manual Intervention Requirements',
    why: 'Some pipelines require manual steps (approvals, file drops, configuration changes) that block automated execution and create unpredictable SLA variability.',
    impact:
      'Pipelines miss SLA windows when manual steps are delayed; engineers are on-call for non-technical gate operations.',
    solution:
      'Automate all inputs; replace manual file drops with event-driven triggers; use workflow approval steps with time-bounded automatic escalation; document and time-track all manual step requirements.',
    tools:
      'Airflow sensors, Databricks Workflows approval steps, event-driven triggers, ServiceNow integration',
    interviewAnswer:
      'We automated our three remaining manual pipeline gate steps using API-driven approvals with 4-hour auto-escalation to on-call management, reducing our SLA breach rate caused by manual delays from 15% to under 1%.',
  },
  {
    id: 50,
    group: 'Pipeline Orchestration',
    title: 'Orchestration Tool Limitations at Scale',
    why: 'Open-source orchestrators hit scheduler throughput limits when managing thousands of DAGs with complex dependencies at enterprise scale.',
    impact:
      'Scheduler lag causes all pipelines to start late; adding new pipelines degrades existing ones; operational overhead to maintain the orchestrator itself becomes unsustainable.',
    solution:
      'Right-size the orchestration tool to the workload; use managed orchestration (Astronomer, MWAA) to reduce operational burden; separate orchestration domains by team; offload scheduling of high-frequency tasks to native platform schedulers.',
    tools:
      'Astronomer, AWS MWAA, Databricks Workflows, Prefect Cloud, domain-isolated Airflow instances',
    interviewAnswer:
      'We migrated from a single monolithic Airflow instance to domain-isolated managed Astronomer deployments per business unit, eliminating scheduler lag and reducing our orchestration infrastructure maintenance effort by 70%.',
  },

  // ─── 51–60: Governance & Compliance ───
  {
    id: 51,
    group: 'Governance & Compliance',
    title: 'No Data Ownership Assignment',
    why: 'Tables and domains have no designated owner, so quality issues, access requests, and compliance questions have no one to escalate to.',
    impact:
      'Data quality incidents linger; access requests take weeks; regulatory auditors cannot identify accountable parties.',
    solution:
      'Mandate an owner for every table in the data catalog; implement ownership as a required metadata field enforced by CI/CD; tie ownership to on-call rotation and SLA accountability.',
    tools:
      'Unity Catalog ownership, Collibra stewardship, Atlan data catalog, dbt model owner field',
    interviewAnswer:
      'We made data ownership a mandatory field in Unity Catalog enforced at table creation time, assigned owners to all 800+ existing tables in a 2-week sprint, and tied ownership to automated incident routing and SLA accountability.',
  },
  {
    id: 52,
    group: 'Governance & Compliance',
    title: 'Missing Data Lineage',
    why: 'Without end-to-end lineage, it is impossible to determine the impact of upstream changes or trace a downstream metric back to its source records.',
    impact:
      'Regulatory audits cannot be completed; root cause analysis for data incidents takes days; change impact assessment is manual and error-prone.',
    solution:
      'Enable Unity Catalog automatic column-level lineage; instrument pipelines with OpenLineage; build a lineage explorer for self-service impact analysis.',
    tools:
      'Databricks Unity Catalog lineage, OpenLineage, Marquez, dbt lineage graph, Collibra lineage',
    interviewAnswer:
      'We enabled Unity Catalog automatic lineage across our entire Databricks platform and integrated OpenLineage into our Airflow DAGs, giving us end-to-end column-level lineage for 100% of our data assets within 6 weeks.',
  },
  {
    id: 53,
    group: 'Governance & Compliance',
    title: 'Inadequate Access Control',
    why: 'Broad group permissions granted for convenience allow analysts to access tables containing PII and financial data they have no business need for.',
    impact:
      'Regulatory violations (GDPR, CCPA); audit findings; risk of insider data misuse; inability to demonstrate principle of least privilege.',
    solution:
      'Implement attribute-based access control via Unity Catalog row/column-level security; conduct quarterly access reviews; enforce least-privilege by default with explicit grants for sensitive data.',
    tools:
      'Unity Catalog row filters, column masks, RBAC groups, access review workflows, SCIM provisioning',
    interviewAnswer:
      'We implemented Unity Catalog column masking on all PII columns and row-level security on regional data, then conducted a least-privilege audit that revoked 65% of existing permissions — passing our subsequent SOC 2 audit with zero findings.',
  },
  {
    id: 54,
    group: 'Governance & Compliance',
    title: 'PII Exposure in Non-Production Environments',
    why: 'Production data copied to dev/staging environments exposes real customer PII to engineers and analysts who do not need access to it.',
    impact:
      'GDPR/CCPA violations; insider risk; breach surface area increases; regulatory fines risk.',
    solution:
      'Implement data masking/anonymization pipelines that generate synthetic non-prod datasets; use format-preserving encryption for referential integrity; never copy raw PII to lower environments.',
    tools:
      'Databricks column masks, Faker library for synthetic data, format-preserving encryption (FPE), Tonic.ai',
    interviewAnswer:
      'We built an automated anonymization pipeline that generates PII-free synthetic development datasets refreshed nightly, eliminating real PII from all non-production environments and removing the GDPR risk from our dev/test infrastructure.',
  },
  {
    id: 55,
    group: 'Governance & Compliance',
    title: 'Inconsistent Data Retention Policies',
    why: 'Different teams apply different retention periods to the same data type; some data is never deleted, creating regulatory liability.',
    impact:
      'GDPR right-to-erasure requests cannot be fulfilled; storage costs grow unboundedly; regulatory audit findings.',
    solution:
      'Define and enforce retention policy per data classification in a central policy registry; implement automated deletion jobs that run on schedule; test deletion with erasure verification checks.',
    tools:
      'Unity Catalog data classification tags, Delta Lake time travel with VACUUM, automated retention jobs, audit logs',
    interviewAnswer:
      'We built a policy-driven retention enforcement system that reads classification tags and automatically purges data beyond its retention period using Delta VACUUM, enabling us to fulfill GDPR erasure requests within 24 hours.',
  },
  {
    id: 56,
    group: 'Governance & Compliance',
    title: 'No Enterprise Data Catalog',
    why: 'Without a catalog, data consumers cannot discover available datasets, understand their quality, or determine who to contact for access.',
    impact:
      'Duplicate dataset creation; shadow data; low trust in the data platform; high analyst onboarding time.',
    solution:
      'Deploy a data catalog integrated with the lakehouse; populate it with automated technical metadata from Unity Catalog and business metadata from domain owners; make it the first stop for data discovery.',
    tools: 'Databricks Unity Catalog, Collibra, Atlan, DataHub, Alation',
    interviewAnswer:
      'We deployed Atlan integrated with Unity Catalog as our enterprise data catalog, automated technical metadata ingestion for all 1,200 tables, and reduced analyst onboarding time from 2 weeks to 2 days through self-service discovery.',
  },
  {
    id: 57,
    group: 'Governance & Compliance',
    title: 'No Data Access Audit Trail',
    why: 'Without audit logging, it is impossible to prove who accessed sensitive data, when, and for what purpose — required by SOC 2, HIPAA, and GDPR.',
    impact:
      'Failed compliance audits; inability to investigate data breach incidents; regulatory fines.',
    solution:
      'Enable Unity Catalog audit logging to system.access.audit; set up automated audit report generation for compliance reviews; implement real-time alerting on anomalous access patterns.',
    tools:
      'Databricks system.access.audit, Unity Catalog audit logs, SIEM integration, Splunk, Datadog',
    interviewAnswer:
      'We enabled Unity Catalog audit logging and built automated compliance reports that summarize data access patterns by user and data classification, which we now deliver to our compliance team monthly and use during SOC 2 audits.',
  },
  {
    id: 58,
    group: 'Governance & Compliance',
    title: 'Multi-Regulation Compliance Complexity',
    why: 'Organizations subject to GDPR, CCPA, HIPAA, and SOX simultaneously must implement overlapping but non-identical compliance requirements without duplicating effort.',
    impact:
      'Compliance gaps in one regulation while over-engineering for another; high compliance maintenance cost.',
    solution:
      'Map all regulations to a common control framework; implement controls once and tag them with applicable regulations; use Unity Catalog as the enforcement layer shared across all regulatory contexts.',
    tools:
      'Unity Catalog policy engine, data classification taxonomy, compliance control mapping, OneTrust, BigID',
    interviewAnswer:
      'We built a unified control framework that maps GDPR, CCPA, and SOX requirements to a single set of Unity Catalog access controls and data classification tags, reducing our compliance maintenance overhead by 50% while passing all three audits.',
  },
  {
    id: 59,
    group: 'Governance & Compliance',
    title: 'Data Sprawl Across Multiple Platforms',
    why: 'Data exists in Snowflake, BigQuery, Databricks, S3, and legacy on-premises systems with no unified governance layer across them.',
    impact:
      'Inconsistent access controls; lineage gaps; impossible to fulfill data subject requests across all systems; high compliance risk.',
    solution:
      'Implement a federated governance layer that enforces consistent policies across platforms; consolidate to fewer platforms where possible; use a unified catalog as the governance hub.',
    tools:
      'Apache Atlas, Collibra, Unity Catalog external locations, Microsoft Purview, data mesh governance layer',
    interviewAnswer:
      'We implemented Microsoft Purview as our cross-platform governance hub to unify policy enforcement across Databricks, Azure SQL, and our legacy Oracle systems, giving us a single control plane for all data subject requests.',
  },
  {
    id: 60,
    group: 'Governance & Compliance',
    title: 'Weak Data Stewardship Model',
    why: 'Data stewardship responsibilities are informally assigned; stewards have accountability without authority; issues escalate to engineering rather than being resolved at the domain level.',
    impact:
      'Engineering becomes the bottleneck for all data quality and governance issues; domain knowledge is not captured; stewardship role burns out stewards.',
    solution:
      'Define stewardship authority including power to enforce quality standards, approve access requests, and mandate documentation; provide tooling that empowers stewards to act without engineering involvement.',
    tools:
      'Collibra stewardship workflows, self-service access provisioning, dbt owner field, data catalog stewardship module',
    interviewAnswer:
      'We redesigned our stewardship model to give domain stewards authority over access approvals and quality SLAs, backed by self-service tooling in our data catalog — reducing engineering governance involvement from 15 hours/week to under 2 hours.',
  },

  // ─── 61–70: Security ───
  {
    id: 61,
    group: 'Security',
    title: 'Over-Permissioned Data Access',
    why: 'Broad "ALL PRIVILEGES" grants made for convenience are never scoped down; the entire engineering team has read access to PII and financial data they do not need.',
    impact:
      'Insider threat surface; regulatory audit failures; inability to demonstrate least privilege to auditors.',
    solution:
      'Conduct a quarterly access review process; revoke all existing permissions and re-grant based on documented business need; enforce least-privilege via automated permission provisioning tied to role assignments in HR systems.',
    tools:
      'Unity Catalog RBAC, SCIM group sync, access review workflows, Okta integration, automated deprovisioning',
    interviewAnswer:
      'We implemented SCIM-based permission provisioning that maps HR role assignments directly to Unity Catalog groups, ensuring permissions are granted and revoked automatically as people change roles — passing our subsequent SOC 2 access review with zero exceptions.',
  },
  {
    id: 62,
    group: 'Security',
    title: 'Encryption Gaps in Data at Rest and in Transit',
    why: 'Legacy tables use default platform encryption without customer-managed keys; some internal data transfers use HTTP rather than HTTPS.',
    impact:
      'Compliance gaps for regulations requiring customer-managed encryption; data exposure risk if cloud infrastructure is compromised.',
    solution:
      'Enable customer-managed keys (CMK) for all sensitive Delta tables; enforce TLS for all data transfers; audit encryption configuration across all storage locations.',
    tools: 'Databricks CMK, AWS KMS, Azure Key Vault, TLS enforcement, encryption audit scripts',
    interviewAnswer:
      'We implemented customer-managed keys via AWS KMS for all sensitive data tiers and enforced TLS 1.2+ across all internal data transfer paths, satisfying the encryption requirements for our HIPAA and PCI-DSS audits.',
  },
  {
    id: 63,
    group: 'Security',
    title: 'PII in Logs and Diagnostic Output',
    why: 'Error messages, debug logs, and stack traces inadvertently include customer names, email addresses, or account numbers that are written to centralized logging systems.',
    impact:
      'Log aggregation systems become PII stores without appropriate controls; breach of logging system exposes sensitive customer data.',
    solution:
      'Implement log scrubbing middleware that redacts PII patterns (email, SSN, credit card regex) before writing to log systems; audit existing logs for PII exposure; classify logs with appropriate retention and access policies.',
    tools:
      'Log scrubbing libraries, regex-based PII detection, AWS Macie for log scanning, structured logging with field whitelisting',
    interviewAnswer:
      'We built a structured logging framework that allows only whitelisted fields to be written to our centralized log system and runs PII pattern detection at write time, eliminating the accidental PII leakage we found in 12% of our existing log streams.',
  },
  {
    id: 64,
    group: 'Security',
    title: 'Secrets and Credentials in Code Repositories',
    why: 'Developers commit database passwords, API keys, and connection strings directly to source control; repositories are later made public or cloned by contractors.',
    impact:
      'Immediate credentials compromise; unauthorized access to production systems; costly emergency rotation of all exposed credentials.',
    solution:
      'Implement pre-commit secret scanning hooks (detect-secrets, gitleaks); use a secrets manager (Databricks Secrets, AWS Secrets Manager) for all credentials; rotate any previously committed secrets immediately.',
    tools:
      'Databricks Secrets, AWS Secrets Manager, HashiCorp Vault, detect-secrets pre-commit hook, GitHub secret scanning',
    interviewAnswer:
      'We deployed detect-secrets as a mandatory pre-commit hook and migrated all credentials to Databricks Secrets Manager, reducing secret exposure incidents to zero — down from 3 in the previous quarter.',
  },
  {
    id: 65,
    group: 'Security',
    title: 'Insider Threat and Privilege Escalation',
    why: 'Disgruntled employees or compromised accounts with broad data access can exfiltrate large volumes of data without triggering any automated alerts.',
    impact:
      'Mass data exfiltration; regulatory breach notification requirements; reputational damage.',
    solution:
      'Implement behavioral anomaly detection on data access patterns; alert on unusual volume downloads (>10K rows), access outside business hours, or access from new IP addresses; enforce MFA for all data platform access.',
    tools:
      'Unity Catalog audit logs, UEBA tools, Splunk UBA, MFA enforcement, Databricks IP access lists',
    interviewAnswer:
      "We built a data access anomaly detection system that compares each user's hourly access patterns against their 30-day baseline and triggers immediate investigation alerts when deviation exceeds 3 standard deviations.",
  },
  {
    id: 66,
    group: 'Security',
    title: 'Unsecured External Data Access Points',
    why: 'APIs, JDBC connections, and Delta Sharing endpoints are exposed without IP allowlisting, authentication validation, or rate limiting.',
    impact:
      'Unauthorized access attempts go undetected; brute-force attacks on credentials; data exfiltration via API.',
    solution:
      'Enforce IP allowlisting for all external access points; require service principal authentication with short-lived tokens; implement rate limiting and anomaly detection on API endpoints.',
    tools:
      'Databricks IP access lists, service principals, OAuth 2.0, API gateway with rate limiting, WAF',
    interviewAnswer:
      'We implemented IP allowlisting and service principal authentication with 1-hour token TTL on all external data access endpoints, reducing unauthorized access attempts by 99% and eliminating the credential-stuffing attacks we had been experiencing.',
  },
  {
    id: 67,
    group: 'Security',
    title: 'Network Exposure of Data Infrastructure',
    why: 'Data clusters and storage accounts are deployed in public subnets or with overly broad network security group rules, exposing them to internet-facing attack vectors.',
    impact:
      'Network-level attack surface; potential for unauthorized cluster access; compliance failures for network isolation requirements.',
    solution:
      'Deploy all data infrastructure in private VPCs/VNets; use private endpoints for storage access; restrict security groups to known CIDR ranges only; implement VPC flow logging for network anomaly detection.',
    tools:
      'AWS VPC private subnets, Azure VNet, Databricks VPC injection, private endpoints, network security groups, VPC flow logs',
    interviewAnswer:
      'We migrated our entire Databricks deployment to a VPC-injected architecture with private storage endpoints and restricted NSG rules, achieving CIS benchmark compliance for network isolation and eliminating all public internet exposure.',
  },
  {
    id: 68,
    group: 'Security',
    title: 'No Dynamic Data Masking for Sensitive Columns',
    why: 'Sensitive columns (SSN, credit card, salary) are exposed in plain text to all users who have table access, even when they only need aggregates.',
    impact:
      'Principle of least privilege is violated; analysts see PII they do not need; compliance audit findings.',
    solution:
      'Implement Unity Catalog column masks that dynamically return masked values for non-privileged users; design masking functions that return full values only to users with the data_owner or pii_viewer role.',
    tools:
      'Unity Catalog column masks, row-level security, dynamic data masking in Snowflake/SQL Server',
    interviewAnswer:
      'We implemented Unity Catalog dynamic column masking on all 47 PII columns, applying full masking for standard users and unmasked access only for users with explicit data_owner role membership, satisfying our GDPR access minimization requirements.',
  },
  {
    id: 69,
    group: 'Security',
    title: 'Data Sharing Security Risks',
    why: 'Data shared with partners via Delta Sharing, S3 presigned URLs, or email exports lacks consistent access controls, expiry, and usage monitoring.',
    impact:
      'Partners retain access beyond the agreed term; shared data is re-shared without authorization; no visibility into how shared data is used.',
    solution:
      'Use Delta Sharing with recipient-level auditing and time-limited share grants; implement expiry policies on all shared assets; require partners to accept data use agreements before receiving access.',
    tools:
      'Delta Sharing, recipient audit logs, data use agreements, expiry-based access, Databricks marketplace controls',
    interviewAnswer:
      'We migrated all external data sharing to Delta Sharing with time-bounded grants and recipient-level usage auditing, giving us full visibility into partner data access and automated revocation at agreement expiry.',
  },
  {
    id: 70,
    group: 'Security',
    title: 'AI Model Training Data Leakage',
    why: 'LLM fine-tuning or RAG system construction uses data without checking whether it contains PII, confidential contracts, or trade secrets that could be surfaced in model outputs.',
    impact:
      'Model outputs leak sensitive information; regulatory violations; contractual breaches; reputational harm.',
    solution:
      'Scan all AI training data for PII and confidential content before use; implement data classification as a gate in the ML pipeline; apply differential privacy techniques for sensitive training data.',
    tools:
      'Presidio PII detection, AWS Macie, Unity Catalog classification tags, differential privacy libraries, MLflow data lineage',
    interviewAnswer:
      'We built a mandatory PII scanning step into our ML data preparation pipeline using Microsoft Presidio, which blocked 3 attempted training runs containing sensitive customer data before they reached the model training stage.',
  },

  // ─── 71–80: Data Integration ───
  {
    id: 71,
    group: 'Data Integration',
    title: 'Integrating Heterogeneous Source Systems',
    why: 'Each source system (ERP, CRM, IoT, legacy mainframe) has a different data model, API style, encoding, and availability pattern requiring custom integration logic.',
    impact:
      'High integration development cost; brittle custom connectors that break on source system upgrades; inconsistent data quality across sources.',
    solution:
      'Adopt a connector-based integration platform that abstracts source system differences; standardize landing zone schema through an ingestion contract; version all integration contracts.',
    tools:
      'Fivetran, Airbyte, Debezium, Apache Camel, MuleSoft, integration contract schema registry',
    interviewAnswer:
      'We standardized all source integrations through Airbyte with a connector-per-source model and a common landing zone schema contract, reducing integration development time from weeks to days and eliminating source-specific transformation logic from our pipelines.',
  },
  {
    id: 72,
    group: 'Data Integration',
    title: 'Data Duplication Across Integration Layers',
    why: 'The same source table is ingested by multiple teams independently using different tools, producing multiple copies with potentially different quality levels.',
    impact:
      'Storage waste; inconsistent values when teams use different copies; integration maintenance burden multiplied per copy.',
    solution:
      'Establish a single ingestion ownership model per source system; publish a source system catalog showing authoritative copies; deprecate and remove duplicate ingestions.',
    tools:
      'Source system ownership registry, data catalog, ingestion governance policy, Unity Catalog as single landing zone',
    interviewAnswer:
      'We audited our data platform and found 23 cases of the same source table being ingested independently by different teams; after centralizing ingestion ownership, we eliminated redundant copies and saved $180K/year in compute and storage.',
  },
  {
    id: 73,
    group: 'Data Integration',
    title: 'Inconsistent Entity Identifiers Across Systems',
    why: 'The same real-world entity (customer, product) has a different ID in each source system with no common key, making cross-system joins impossible without entity resolution.',
    impact:
      'Customer 360 views are incomplete; cross-channel attribution fails; customer support teams cannot see the full customer history.',
    solution:
      'Build or acquire an entity resolution system that generates a golden ID mapping across source system IDs; maintain the mapping as a managed table with lineage; use probabilistic matching for entities without deterministic links.',
    tools:
      'Zingg (open-source entity resolution), Splink, AWS Entity Resolution, Databricks MLflow for matching models',
    interviewAnswer:
      'We implemented Splink probabilistic entity resolution across our 6 source systems, achieving 94% customer ID link accuracy and enabling a unified customer 360 view that had previously been impossible due to incompatible identifiers.',
  },
  {
    id: 74,
    group: 'Data Integration',
    title: 'Batch and Real-Time Integration Conflict',
    why: 'Business processes require both batch historical data and real-time event data to be queryable together, but the two systems have different consistency guarantees and latency profiles.',
    impact:
      'Reports that mix batch and streaming data show inconsistencies; analysts cannot determine which time window each data source covers.',
    solution:
      'Implement the Lambda Architecture or Kappa Architecture; use Delta Lake as the unified serving layer that accepts both batch writes and streaming micro-batch updates; clearly document the freshness guarantees per data layer.',
    tools:
      'Delta Lake streaming + batch, Databricks Structured Streaming, unified serving layer, data freshness metadata',
    interviewAnswer:
      'We unified batch and streaming data into a single Delta Lake serving layer using a Kappa-style architecture, giving analysts a single query surface with clearly documented freshness metadata for each data domain.',
  },
  {
    id: 75,
    group: 'Data Integration',
    title: 'API Contract Inconsistencies',
    why: 'Internal and external APIs change field names, data types, and response structures without versioning or consumer notification.',
    impact:
      'Integration pipelines break silently or produce incorrect data; consumers have no warning of breaking changes.',
    solution:
      'Enforce API schema contracts using a schema registry; version all APIs; implement consumer-driven contract testing; require compatibility tests before any API change is deployed.',
    tools:
      'Confluent Schema Registry, OpenAPI specification, Pact consumer-driven contract testing, API gateway versioning',
    interviewAnswer:
      'We implemented consumer-driven contract testing using Pact for all our internal integration APIs, catching 8 breaking changes in development before they reached production and eliminating integration-related production incidents.',
  },
  {
    id: 76,
    group: 'Data Integration',
    title: 'Integration Latency Mismatch',
    why: 'A downstream system requires near-real-time data but the upstream integration pipeline runs only on a daily batch schedule.',
    impact:
      'Business process decisions are based on day-old data; operational efficiency gains from real-time data are unrealized.',
    solution:
      'Redesign high-priority integrations to use CDC or event streaming for near-real-time delivery; use micro-batch integration for moderate latency requirements; reserve full batch for historical and reporting workloads.',
    tools:
      'Debezium CDC, Kafka event streaming, Databricks Structured Streaming, micro-batch scheduling',
    interviewAnswer:
      'We redesigned our most latency-sensitive integrations from daily batch to CDC-based streaming using Debezium and Kafka, reducing integration latency from 24 hours to under 5 minutes for the business processes that needed it most.',
  },
  {
    id: 77,
    group: 'Data Integration',
    title: 'Vendor Data Feed Reliability',
    why: 'Third-party data vendors deliver feeds that are late, incomplete, or malformed without advance notice, breaking pipelines that depend on them.',
    impact:
      'Downstream analytics are delayed or incorrect; on-call engineers spend nights managing vendor feed failures.',
    solution:
      'Implement robust vendor feed monitoring with expected delivery windows; build graceful degradation that uses last-known-good data when the feed is late; establish SLAs with vendors including penalties for persistent failures.',
    tools:
      'Airflow sensors with timeout, last-known-good data pattern, vendor SLA management, alerting on missed delivery windows',
    interviewAnswer:
      "We implemented a graceful degradation pattern that detects missed vendor feed deliveries within 15 minutes and automatically uses the previous day's validated data, while simultaneously alerting the vendor and our team — eliminating the pipeline failures that had required nightly on-call intervention.",
  },
  {
    id: 78,
    group: 'Data Integration',
    title: 'Legacy System Integration Constraints',
    why: 'Legacy systems do not support modern APIs, CDC, or streaming protocols; data can only be extracted via scheduled batch exports or fragile database direct connections.',
    impact:
      'Integration is brittle; schema changes on the legacy system break exports silently; batch extracts lock legacy database tables during business hours.',
    solution:
      'Use change-data-capture on the database log level if available; implement staging tables on the legacy system as an anti-corruption layer; schedule extracts during off-peak windows with strict timeout enforcement.',
    tools:
      'Oracle GoldenGate, IBM InfoSphere CDC, database-level log tailing, staging table pattern, SFTP with schema contracts',
    interviewAnswer:
      'We implemented Oracle GoldenGate for our most critical legacy systems to capture change events at the database log level, eliminating table-locking batch exports and reducing our legacy integration latency from 6 hours to under 30 minutes.',
  },
  {
    id: 79,
    group: 'Data Integration',
    title: 'Event Ordering and Out-of-Order Processing',
    why: 'Events from distributed systems arrive at the integration layer out of chronological order; applying them in arrival order produces incorrect entity state.',
    impact:
      'Customer account state is incorrect; financial transactions are applied in wrong order; audit logs are misleading.',
    solution:
      'Use event sequence numbers or logical timestamps for ordering; implement event-time processing with watermarks; design idempotent event consumers that can safely re-process events and handle late arrivals.',
    tools:
      'Kafka with partition ordering, Spark Structured Streaming event-time, Flink event-time processing, sequence number ordering',
    interviewAnswer:
      'We redesigned our event processing pipeline to use logical sequence numbers from the source system rather than arrival timestamps, and implemented event-time watermarking with a 5-minute tolerance for late arrivals, eliminating the entity state corruption we had been experiencing.',
  },
  {
    id: 80,
    group: 'Data Integration',
    title: 'Real-Time and Batch Synchronization Complexity',
    why: 'Keeping batch-derived historical data and real-time streaming data synchronized requires complex merge logic; corrections to batch data must propagate to real-time state.',
    impact:
      'Historical corrections do not propagate to real-time views; consumers see different values depending on which data path they query.',
    solution:
      'Design a unified state store (Delta Lake) that serves both batch and streaming; implement a correction event pattern where batch reconciliations emit correction events processed by the streaming layer.',
    tools:
      'Delta Lake MERGE, correction event pattern, unified serving layer, Databricks Delta Live Tables',
    interviewAnswer:
      'We implemented a correction event pattern where our nightly batch reconciliation emits correction events that are processed by the streaming layer within 15 minutes, ensuring batch corrections are reflected in real-time views without requiring a full stream replay.',
  },

  // ─── 81–90: AI/ML/RAG ───
  {
    id: 81,
    group: 'AI/ML/RAG',
    title: 'Poor Data Quality for ML Training',
    why: 'ML models trained on noisy, incomplete, or biased training data produce unreliable predictions that are difficult to attribute to data quality rather than model design.',
    impact:
      'Models deployed to production perform significantly worse than offline evaluation metrics suggest; retraining is expensive and time-consuming.',
    solution:
      'Implement a data quality gate before any training dataset is approved; validate label quality, class balance, feature completeness, and temporal distribution; document data quality metrics alongside model performance metrics.',
    tools:
      'Great Expectations, dbt tests, Pandas Profiling / ydata-profiling, MLflow data versioning, Evidently AI',
    interviewAnswer:
      'We built a mandatory training data quality gate using Great Expectations that checks label noise, class imbalance, and feature completeness before any training run starts, which improved our model production performance from 71% to 89% accuracy by eliminating corrupted training sets.',
  },
  {
    id: 82,
    group: 'AI/ML/RAG',
    title: 'Training-Serving Feature Skew',
    why: 'Features computed differently at training time (batch, historical) versus serving time (real-time, current) cause the model to see different distributions during inference than it was trained on.',
    impact:
      'Model performance degrades in production relative to offline evaluation; the root cause is invisible without feature monitoring.',
    solution:
      'Use a unified feature store that serves identical feature computation logic at training and serving time; monitor feature distribution drift between training and production using statistical tests.',
    tools:
      'Databricks Feature Store, Feast, Tecton, Evidently AI feature monitoring, statistical drift tests (KS, PSI)',
    interviewAnswer:
      'We migrated to Databricks Feature Store for all production ML features, ensuring training and serving use identical computation logic, and added feature distribution monitoring that alerts on drift above 0.2 PSI — which identified two silent skew issues within the first month.',
  },
  {
    id: 83,
    group: 'AI/ML/RAG',
    title: 'Model and Data Drift in Production',
    why: 'Real-world data distributions shift over time due to seasonal patterns, business changes, and external events; models trained on historical data become progressively less accurate.',
    impact:
      'Model predictions degrade gradually without triggering obvious errors; business decisions based on stale models produce measurable harm before the issue is detected.',
    solution:
      'Implement continuous monitoring of prediction distributions, input feature distributions, and model accuracy metrics; set automated retraining triggers when drift exceeds defined thresholds.',
    tools:
      'Evidently AI, Arize AI, Databricks Lakehouse Monitoring, MLflow model registry, automated retraining pipelines',
    interviewAnswer:
      'We deployed Evidently AI monitoring on all production models with weekly distribution reports and automatic retraining triggers on PSI > 0.2, catching a significant drift event from a business rule change 3 weeks before it would have been noticed through business metrics alone.',
  },
  {
    id: 84,
    group: 'AI/ML/RAG',
    title: 'Insufficient Labeled Training Data',
    why: 'Supervised ML tasks require labeled examples; acquiring high-quality labels at scale is expensive and time-consuming, often becoming the primary bottleneck.',
    impact:
      'Models underperform due to small training sets; annotation costs are prohibitively high for edge case coverage.',
    solution:
      'Implement active learning to prioritize which examples need human labeling; use semi-supervised learning to leverage unlabeled data; explore weak supervision techniques (Snorkel) for programmatic label generation.',
    tools:
      'Label Studio, Scale AI, Snorkel (weak supervision), active learning frameworks, semi-supervised learning',
    interviewAnswer:
      'We implemented active learning using uncertainty sampling to prioritize the most informative examples for human labeling, reducing our annotation cost by 65% while achieving the same model performance as random sampling on 3x more examples.',
  },
  {
    id: 85,
    group: 'AI/ML/RAG',
    title: 'RAG Hallucination from Poor Knowledge Base',
    why: 'RAG systems built on stale, inconsistent, or poorly structured knowledge bases retrieve irrelevant context, causing LLMs to hallucinate plausible-sounding but incorrect answers.',
    impact:
      'Users receive confidently stated incorrect information; trust in the system erodes; incorrect decisions are made based on hallucinated responses.',
    solution:
      'Curate the knowledge base with regular freshness updates; implement retrieval quality evaluation using metrics like context relevance and faithfulness; add a validation layer that flags low-confidence responses.',
    tools:
      'LlamaIndex, LangChain, Databricks Vector Search, RAGAS evaluation framework, Guardrails AI',
    interviewAnswer:
      'We implemented RAGAS evaluation on our RAG pipeline with context relevance and faithfulness scoring, identified that 30% of our knowledge base documents were stale, and after curation and chunking improvements, reduced our hallucination rate from 18% to 4%.',
  },
  {
    id: 86,
    group: 'AI/ML/RAG',
    title: 'Retrieval Quality in Vector Search',
    why: 'Vector search retrieves semantically similar but not actually relevant documents due to poor chunking strategies, suboptimal embedding models, or missing metadata filtering.',
    impact:
      'LLM receives irrelevant context; answers are either generic or incorrect; retrieval performance is a silent bottleneck invisible to end users.',
    solution:
      'Evaluate retrieval precision and recall separately from generation quality; experiment with chunking strategies (size, overlap, semantic chunking); add metadata filters to constrain retrieval space; implement re-ranking to improve top-k relevance.',
    tools:
      'Databricks Vector Search, Pinecone, Weaviate, cross-encoder re-ranking (BGE, Cohere), BEIR benchmark evaluation',
    interviewAnswer:
      'We added cross-encoder re-ranking as a second-stage retrieval step after embedding-based search, and implemented domain-specific metadata filters, improving our retrieval MRR@5 from 0.42 to 0.78 — the single biggest quality improvement in our RAG pipeline.',
  },
  {
    id: 87,
    group: 'AI/ML/RAG',
    title: 'Embedding Model Mismatch and Staleness',
    why: 'Embeddings generated with one model version cannot be directly compared to embeddings generated with a different version; updating the embedding model requires reindexing all documents.',
    impact:
      'Retrieval quality degrades silently as the embedding model is updated; reindexing is a significant operational event that can take hours for large corpora.',
    solution:
      'Version embedding models alongside their vector indexes; plan reindexing as a coordinated operation with zero-downtime cutover using a blue/green index strategy; monitor embedding model version consistency.',
    tools:
      'Databricks Vector Search, Pinecone index management, embedding model versioning in MLflow, blue/green index cutover',
    interviewAnswer:
      'We implemented a blue/green index strategy for embedding model updates that builds the new index in parallel, validates retrieval quality against a held-out evaluation set, then atomically redirects traffic — achieving zero-downtime embedding model updates for our 50M document corpus.',
  },
  {
    id: 88,
    group: 'AI/ML/RAG',
    title: 'Model Explainability and Auditability',
    why: 'Black-box ML models cannot explain their predictions; regulators, auditors, and business stakeholders require understanding of why a model made a specific decision.',
    impact:
      'Regulatory compliance failures (GDPR Article 22, Fair Credit Reporting Act); inability to detect discriminatory model behavior; low trust from business users.',
    solution:
      'Implement SHAP or LIME explanations for all production models; log explanations alongside predictions in the serving layer; build a model card for each production model documenting known limitations and intended use.',
    tools:
      'SHAP, LIME, MLflow model cards, IBM AI Fairness 360, Databricks Model Serving explanation logging',
    interviewAnswer:
      'We integrated SHAP explanations into our model serving layer, logging feature contributions alongside every prediction to Delta Lake, enabling our compliance team to answer regulator inquiries about individual credit decisions within minutes rather than days.',
  },
  {
    id: 89,
    group: 'AI/ML/RAG',
    title: 'No Feedback Loop for Model Improvement',
    why: 'After deployment, model predictions are not connected back to ground truth outcomes, making it impossible to measure real-world performance or trigger retraining.',
    impact:
      'Models degrade over time without triggering any alerts; retraining is ad-hoc rather than outcome-driven.',
    solution:
      'Design a feedback capture system that joins predictions with eventual ground truth outcomes; compute real-world accuracy metrics on a rolling basis; use ground truth to trigger retraining when performance falls below threshold.',
    tools:
      'MLflow prediction logging, Delta Lake ground truth join, Evidently AI, automated retraining pipelines, A/B testing framework',
    interviewAnswer:
      "We built a prediction-to-outcome join pipeline that computes real accuracy metrics 30 days after prediction and writes them to MLflow experiments, enabling outcome-driven automatic retraining that improved our churn model's production accuracy by 12 percentage points over 6 months.",
  },
  {
    id: 90,
    group: 'AI/ML/RAG',
    title: 'AI Data Governance and Policy Compliance',
    why: 'AI systems consume sensitive data for training and inference without tracking which data was used, who approved it, or whether data subjects consented to AI use.',
    impact:
      'GDPR compliance failures; inability to fulfill data subject deletion requests that propagate to model deletion; lack of audit trail for AI decision systems.',
    solution:
      'Track training dataset provenance with full lineage to source tables; implement consent management that propagates to AI training eligibility; build a model card registry documenting data sources, consent basis, and intended use.',
    tools:
      'MLflow dataset tracking, Unity Catalog lineage, OneTrust consent management, AI model registry with governance metadata',
    interviewAnswer:
      'We extended our MLflow model registry with mandatory governance metadata including training data lineage, consent basis, and intended use documentation, and built an automated deletion propagation system that removes data subject records from both source tables and affected training datasets.',
  },

  // ─── 91–100: Cost & FinOps ───
  {
    id: 91,
    group: 'Cost & FinOps',
    title: 'Uncontrolled Compute Spend',
    why: 'Auto-scaling clusters and ad-hoc compute provisioning without cost guardrails result in month-end billing surprises with no accountability.',
    impact:
      'Cloud bills 3–5x over budget; no ability to attribute costs to specific teams or projects; leadership demands reactive cost cuts.',
    solution:
      'Implement cluster policies that enforce max DBU/hour limits per team; tag all resources with cost center and team; set up budget alerts at 80% and 100% of monthly allocation; review top-10 compute consumers weekly.',
    tools:
      'Databricks cluster policies, resource tagging, AWS Cost Explorer, Azure Cost Management, budget alerts',
    interviewAnswer:
      'We implemented Databricks cluster policies with hard DBU caps per team and daily cost attribution dashboards, reducing our uncontrolled compute spend by 45% in the first month and enabling chargebacks to individual business units for the first time.',
  },
  {
    id: 92,
    group: 'Cost & FinOps',
    title: 'Expensive and Inefficient Queries',
    why: 'Full table scans on large tables, repeated aggregation of raw data, and poorly written joins consume excessive compute without delivering proportional business value.',
    impact:
      'High per-query cost multiplied by query frequency results in significant unnecessary spend.',
    solution:
      'Identify top-10 most expensive queries by compute cost; optimize with materialized aggregations, partition pruning, and caching; implement query cost budgets that alert when a single query exceeds a DBU threshold.',
    tools:
      'Databricks query history + cost analysis, EXPLAIN plans, materialized views, result caching, query cost alerting',
    interviewAnswer:
      'We used Databricks query history to identify our 10 most expensive recurring queries, optimized them with materialized pre-aggregations and Z-Order, and set per-query DBU alerts — reducing our monthly query compute cost by $25K.',
  },
  {
    id: 93,
    group: 'Cost & FinOps',
    title: 'Data Duplication Driving Storage Costs',
    why: 'The same raw data is stored multiple times across bronze layers, team-specific staging tables, and legacy copies from migration projects.',
    impact:
      'Storage costs grow proportionally with duplication factor rather than actual data volume growth.',
    solution:
      'Audit the data catalog for duplicate datasets; establish a single authoritative copy per source; implement tiered storage (hot/warm/cold) based on access frequency; enforce data lifecycle policies.',
    tools:
      'Unity Catalog data catalog audit, duplicate detection queries, S3/ADLS lifecycle policies, Delta Lake VACUUM',
    interviewAnswer:
      'We audited our data catalog and identified 40% of our storage was duplicate copies of the same source data; after consolidation and lifecycle policy implementation, we reduced storage spend by $60K/month while improving data quality through single-source-of-truth.',
  },
  {
    id: 94,
    group: 'Cost & FinOps',
    title: 'Idle Cluster Costs',
    why: 'Clusters provisioned for interactive development or scheduled jobs remain running long after their workload completes, accumulating DBU charges for zero work.',
    impact: '20–40% of total compute spend is on idle clusters providing no business value.',
    solution:
      'Enforce aggressive auto-termination policies (15-minute idle timeout for interactive, immediate for job clusters); use serverless compute for SQL workloads; implement instance pools to reduce startup latency as a barrier to right-sizing.',
    tools:
      'Databricks auto-termination policies, serverless warehouses, instance pools, idle cost monitoring',
    interviewAnswer:
      'We enforced a 15-minute auto-termination policy on all interactive clusters and migrated SQL workloads to serverless warehouses, reducing our idle cluster spend from 35% to under 5% of total compute cost within the first billing cycle.',
  },
  {
    id: 95,
    group: 'Cost & FinOps',
    title: 'Streaming Pipeline Cost Optimization',
    why: 'Always-on streaming clusters run at full capacity 24/7 even when event volume is near-zero during off-peak hours, paying for unused capacity.',
    impact: 'Streaming compute costs are 3–5x what they would be with demand-based scaling.',
    solution:
      'Implement trigger(availableNow=True) for latency-tolerant streaming workloads to run micro-batch only when data is available; enable auto-scaling for streaming clusters; consolidate multiple low-volume streams onto shared clusters.',
    tools:
      'Structured Streaming trigger(availableNow), Delta Lake, cluster auto-scaling, stream consolidation patterns',
    interviewAnswer:
      'We migrated our 12 low-volume streaming pipelines from always-on triggers to availableNow micro-batch mode and consolidated them onto shared auto-scaling clusters, reducing our streaming compute cost by 60% with no measurable increase in data latency.',
  },
  {
    id: 96,
    group: 'Cost & FinOps',
    title: 'Poor Partitioning Causing Over-Scanning',
    why: 'Tables without efficient partition strategies require full table scans for common query patterns, consuming 10–100x the necessary compute per query.',
    impact:
      'Per-query costs are unnecessarily high; high-frequency dashboard queries drive disproportionate compute spend.',
    solution:
      'Analyze common WHERE clause patterns and align partitioning strategy; implement Z-Order on secondary filter columns; use liquid clustering for multi-access-pattern tables.',
    tools:
      'Delta Lake partitioning, Z-Order, liquid clustering, ANALYZE TABLE, query cost profiling',
    interviewAnswer:
      'We profiled our top-50 most expensive recurring queries against our actual table partition layouts and realigned partition strategies accordingly, reducing compute cost per query by an average of 70% for our highest-frequency workloads.',
  },
  {
    id: 97,
    group: 'Cost & FinOps',
    title: 'No Cost Visibility or Attribution',
    why: 'Cloud billing is a single aggregate charge with no breakdown by team, project, use case, or data product; cost accountability is impossible.',
    impact:
      'Teams have no incentive to optimize; leadership cannot make data-driven decisions about where to invest or cut; showback/chargeback programs are impossible.',
    solution:
      'Implement comprehensive resource tagging (team, project, environment, cost_center); build a cost attribution dashboard aggregating by tag; publish weekly per-team cost reports and establish optimization targets.',
    tools:
      'Databricks resource tagging, AWS Cost Explorer tag groups, Azure Cost Management, custom cost attribution dashboards',
    interviewAnswer:
      'We implemented a complete resource tagging strategy across all Databricks clusters and published weekly per-team cost attribution dashboards, which created the accountability needed for teams to self-optimize — driving a 30% aggregate cost reduction within one quarter.',
  },
  {
    id: 98,
    group: 'Cost & FinOps',
    title: 'Multi-Cloud Cost Complexity',
    why: 'Data workloads spanning AWS, Azure, and GCP accumulate egress charges for cross-cloud data transfers and require separate cost management tooling for each cloud.',
    impact:
      'Unexpected egress bills; difficulty comparing total cost of ownership across clouds; governance gaps across cloud boundaries.',
    solution:
      'Minimize cross-cloud data transfers by co-locating processing with data; use cloud-native cost management APIs aggregated into a unified FinOps platform; evaluate data gravity before designing cross-cloud architectures.',
    tools:
      'AWS Cost Explorer, Azure Cost Management, Google Cloud Billing, CloudHealth, Apptio Cloudability, unified FinOps platform',
    interviewAnswer:
      'We implemented a data gravity analysis before every cross-cloud architecture decision and consolidated our primary analytical workloads to a single cloud, reducing cross-cloud egress charges by 80% and simplifying our cost management to a single billing view.',
  },
  {
    id: 99,
    group: 'Cost & FinOps',
    title: 'Over-Engineering for Scale Not Yet Reached',
    why: 'Teams build Kafka + Flink streaming architectures for 1000 events/day workloads that would be served adequately by a simple hourly batch job.',
    impact:
      'Operational complexity and cost far exceeds the business value delivered; maintenance burden is disproportionate to the scale.',
    solution:
      'Right-size architecture to current and near-term requirements; design for scale when volume projections justify it; use the simplest architecture that meets the SLA; document scale assumptions and revisit quarterly.',
    tools:
      'Architecture review process, scale modeling templates, cost-per-query analysis, phased architecture design',
    interviewAnswer:
      'We introduced an architecture review gate that requires volume projections and cost-per-query analysis before approving streaming infrastructure, replacing 4 over-engineered Kafka pipelines with simple scheduled batch jobs and saving $120K/year in operational cost.',
  },
  {
    id: 100,
    group: 'Cost & FinOps',
    title: 'No Ongoing Cost Optimization Process',
    why: 'Cost optimization is treated as a one-time project rather than a continuous discipline; optimizations achieved in Q1 erode by Q3 without sustained effort.',
    impact:
      'Costs drift back up over time; the engineering team is repeatedly interrupted for reactive cost-cutting sprints instead of building new capabilities.',
    solution:
      'Establish a FinOps practice with a regular cadence (weekly cost review, monthly optimization sprint, quarterly architecture review); define unit economics targets (cost per TB processed, cost per query); track unit economics as a KPI.',
    tools:
      'FinOps Foundation framework, Databricks cost dashboards, unit economics tracking, optimization backlog, quarterly architecture reviews',
    interviewAnswer:
      'We established a weekly FinOps review meeting and defined unit economics KPIs including cost per TB processed and cost per active user, which transformed cost optimization from a reactive scramble to a proactive, measurable business discipline.',
  },

  // ─── 101–110: Organization & Process ───
  {
    id: 101,
    group: 'Organization & Process',
    title: 'No Coherent Data Strategy',
    why: 'Data initiatives are launched as tactical responses to immediate business requests without alignment to a long-term data vision, resulting in a fragmented, incoherent data estate.',
    impact:
      "Platform investments are duplicated; technical debt accumulates faster than it is retired; business leaders lose confidence in the data team's strategic value.",
    solution:
      'Develop a 3-year data strategy aligned to business objectives; define target state architecture and migration path; establish a data council with executive sponsorship; communicate strategy and progress quarterly.',
    tools:
      'Data strategy framework, OKRs for data, data council governance model, quarterly business review cadence',
    interviewAnswer:
      'We developed a 3-year data strategy with explicit architectural target states and business-aligned OKRs, presented it to the executive team for sponsorship, and established a quarterly data council to track progress — transforming our team from a reactive service desk to a strategic partner.',
  },
  {
    id: 102,
    group: 'Organization & Process',
    title: 'Siloed Data Teams by Department',
    why: 'Each business unit maintains its own data team, tools, and standards; cross-domain data products are impossible because teams cannot collaborate effectively.',
    impact:
      'Redundant infrastructure costs; inconsistent data definitions; inability to build enterprise-wide analytics products.',
    solution:
      'Adopt a data mesh or federated model with shared platform services, common standards, and cross-domain coordination; establish a data platform team providing shared infrastructure to domain teams.',
    tools:
      'Data mesh architecture, centralized data platform team model, shared data catalog, common dbt standards, cross-team working groups',
    interviewAnswer:
      'We transitioned from 8 siloed departmental data teams to a federated data mesh model with a shared platform team providing common infrastructure, which enabled our first cross-domain analytics product and reduced total data infrastructure cost by 35%.',
  },
  {
    id: 103,
    group: 'Organization & Process',
    title: 'No Data Modeling or Pipeline Standards',
    why: 'Each engineer designs pipelines and data models differently; there is no consistency in naming conventions, layer conventions, testing requirements, or documentation standards.',
    impact:
      'Onboarding new engineers takes months; code review is slow because each codebase is unique; cross-team collaboration requires complete context transfer.',
    solution:
      'Define and publish engineering standards covering naming conventions, layer architecture (bronze/silver/gold), testing requirements, documentation standards, and review checklists; enforce via templates and CI checks.',
    tools:
      'dbt style guide, company engineering standards wiki, CI lint checks, code review checklists, architecture decision records (ADRs)',
    interviewAnswer:
      'We published a comprehensive data engineering standards guide covering naming, testing, and documentation, and enforced it via CI lint checks and required code review checklists — reducing new engineer onboarding time from 3 months to 3 weeks.',
  },
  {
    id: 104,
    group: 'Organization & Process',
    title: 'Critical Skill Gaps in the Data Team',
    why: 'The team has strong SQL skills but lacks distributed systems knowledge, ML engineering capability, or cloud infrastructure expertise needed for modern data architectures.',
    impact:
      'Architecture decisions are made by people without relevant expertise; vendor selection and implementation quality suffer; the team cannot execute on the data strategy.',
    solution:
      'Conduct a skills gap analysis against the target architecture; create individual development plans; hire for missing critical skills; establish knowledge-sharing sessions and pair programming for skill transfer.',
    tools:
      'Skills assessment frameworks, learning platforms (Coursera, Databricks Academy), mentoring programs, strategic hiring plan',
    interviewAnswer:
      'We conducted a structured skills assessment across the data team, identified streaming and ML engineering as critical gaps, created development partnerships with Databricks Academy, and supplemented with two targeted hires — filling our capability gaps within two quarters.',
  },
  {
    id: 105,
    group: 'Organization & Process',
    title: 'Resistance to Data Governance Adoption',
    why: 'Engineers and analysts view governance processes (ownership, documentation, quality checks) as bureaucratic overhead that slows down delivery rather than enabling it.',
    impact:
      'Governance programs launch but are never consistently adopted; the data estate remains poorly governed despite the investment.',
    solution:
      'Design governance for developer experience — make it easier to follow standards than to ignore them; show concrete business value (faster debugging, higher trust); get leadership to reinforce governance expectations in performance reviews.',
    tools:
      'Self-service governance tooling, developer-friendly catalog UIs, governance as code (dbt), executive sponsorship',
    interviewAnswer:
      'We redesigned our governance program around developer experience by integrating all governance requirements into the dbt workflow engineers were already using, making compliance the path of least resistance and increasing governance adoption from 20% to 95% within one quarter.',
  },
  {
    id: 106,
    group: 'Organization & Process',
    title: 'Inadequate Data Documentation',
    why: 'Tables, columns, and pipelines exist without descriptions, business definitions, or usage examples; domain knowledge is locked in the heads of individual engineers.',
    impact:
      'High analyst onboarding cost; frequent misdirection of data consumers; documentation debt accumulates and grows with each new data product.',
    solution:
      'Make documentation a first-class deliverable; add dbt model descriptions and column comments as PR review requirements; use AI-assisted documentation generation for initial drafts; track documentation coverage as a KPI.',
    tools:
      'dbt docs, Unity Catalog column comments, AI documentation assistants, documentation coverage metrics, PR review checklists',
    interviewAnswer:
      'We made dbt model and column descriptions a mandatory PR merge requirement, used an AI assistant to generate initial documentation drafts from schema and code context, and tracked coverage as a team KPI — reaching 90% documentation coverage across our 600+ models.',
  },
  {
    id: 107,
    group: 'Organization & Process',
    title: 'No Enterprise Data Governance Operating Model',
    why: 'Data governance is discussed at the policy level but no one has defined who makes governance decisions, who enforces them, or how conflicts are resolved.',
    impact:
      'Governance policies are written but not enforced; exceptions proliferate; the governance program loses credibility.',
    solution:
      'Define a governance operating model with clear roles (data owner, steward, custodian, consumer), decision rights, escalation paths, and enforcement mechanisms; publish and train the organization on the model.',
    tools:
      'RACI matrix for data governance, data council charter, stewardship program, governance decision framework',
    interviewAnswer:
      'We implemented a formal data governance operating model with defined RACI for every governance decision type, established a data council for policy decisions, and empowered domain stewards with tooling to enforce standards — transitioning from a "governance theater" program to one with real operational accountability.',
  },
  {
    id: 108,
    group: 'Organization & Process',
    title: 'Stakeholder Misalignment on Data Priorities',
    why: 'The data team is pulled in conflicting directions by different business stakeholders with competing priorities, resulting in context-switching and poor delivery on all fronts.',
    impact:
      'Delivery velocity suffers; the team is perceived as slow; stakeholders lose trust and build shadow data capabilities.',
    solution:
      'Establish a demand management process with a prioritized backlog reviewed by a data council; make capacity and trade-offs transparent; communicate the business impact of data investments in business terms.',
    tools:
      'Data product roadmap, demand management process, data council prioritization forum, OKR-aligned backlog',
    interviewAnswer:
      'We established a monthly data council prioritization meeting where stakeholders align on the top-10 data investments given available capacity, eliminating the competing-demand conflicts that had been creating a 35% context-switching overhead on the data engineering team.',
  },
  {
    id: 109,
    group: 'Organization & Process',
    title: 'Agile Mismatch in Data Engineering',
    why: 'Data engineering work does not fit neatly into 2-week sprints; data pipeline and model development involves exploration, prototyping, and dependency on external systems that resist sprint-level estimation.',
    impact:
      'Sprint velocity is unpredictable; data engineers are pressured to commit to estimates they cannot fulfill; team morale suffers.',
    solution:
      'Adopt a data engineering delivery model that distinguishes between discovery work (timeboxed spikes), infrastructure work (Kanban-style), and feature delivery (sprint-based); communicate the distinction to stakeholders.',
    tools:
      'Kanban for infrastructure, sprint-based delivery for data products, spike estimation approach, dual-track agile',
    interviewAnswer:
      'We adopted a dual-track delivery model separating discovery and infrastructure work (managed on Kanban) from data product delivery (2-week sprints), which improved our sprint commitment accuracy from 45% to 85% and reduced stakeholder frustration with "missed" estimates.',
  },
  {
    id: 110,
    group: 'Organization & Process',
    title: 'No Demonstrated ROI for Data Investments',
    why: 'Data platform investments are justified with technical metrics (pipeline reliability, query performance) rather than business outcomes; leadership cannot evaluate whether the investment is worthwhile.',
    impact:
      'Data platform budget is vulnerable to cuts; the data team struggles to secure investment for strategic initiatives; credibility with business leadership is low.',
    solution:
      'Define business outcome metrics for each data investment (revenue attributed, decision speed, cost avoided); build a data value dashboard showing ROI across the portfolio; quantify value in business terms quarterly.',
    tools:
      'Data ROI framework, business outcome tracking, data value dashboards, executive data business reviews',
    interviewAnswer:
      'We built a data value dashboard that quantifies business outcomes for our top 20 data products including revenue attributed and cost avoided, and presented it to the CFO quarterly — which resulted in a 40% increase in our data platform budget by demonstrating $8M in annual value delivered.',
  },

  // ─── 111–120: Observability ───
  {
    id: 111,
    group: 'Observability',
    title: 'No Pipeline Monitoring or Alerting',
    why: 'Pipelines run without any runtime monitoring; failures are discovered when consumers report stale data hours or days later.',
    impact:
      'Mean time to detection for pipeline failures is measured in hours; business decisions based on stale data cause direct harm.',
    solution:
      'Implement monitoring for every pipeline covering success/failure status, runtime duration, row count, and data freshness; configure alerts that page on-call within 15 minutes of a critical failure.',
    tools:
      'Databricks Workflows alert notifications, Airflow email/Slack operators, Grafana dashboards, PagerDuty, Monte Carlo',
    interviewAnswer:
      'We deployed end-to-end pipeline monitoring using Databricks Workflows alerts and a custom metrics table that tracks row counts and runtimes, reducing our mean time to pipeline failure detection from 4 hours to under 15 minutes.',
  },
  {
    id: 112,
    group: 'Observability',
    title: 'Insufficient Structured Logging',
    why: 'Pipeline logs are unstructured free-text written to standard output; searching for specific error patterns or reconstructing pipeline execution history is difficult.',
    impact:
      'Debugging production issues requires manual log review that takes hours; patterns across multiple pipeline runs cannot be analyzed.',
    solution:
      'Implement structured JSON logging with consistent fields (pipeline_id, run_id, stage, status, row_count, duration_ms, error_code); centralize logs to a queryable log management system.',
    tools:
      'Python structlog, JSON logging, Databricks cluster logs, Splunk, Datadog, Elasticsearch + Kibana',
    interviewAnswer:
      'We standardized on structured JSON logging across all pipelines with a consistent schema including correlation IDs and business-level metrics, enabling us to build Grafana dashboards from log data and reducing average debugging time from 3 hours to 20 minutes.',
  },
  {
    id: 113,
    group: 'Observability',
    title: 'No Automated Alerting on Data Quality',
    why: 'Data quality checks are run but results are only viewable in reports that no one reads consistently; quality degradation is not escalated automatically.',
    impact:
      'Quality issues persist for days or weeks before being addressed; consumers discover issues instead of the data team.',
    solution:
      'Wire all quality check results to an alerting system; configure severity-based routing (critical failures → PagerDuty, warnings → Slack channel); set SLA targets for quality issue resolution.',
    tools:
      'Great Expectations alert integration, dbt test failure notifications, Monte Carlo data observability, PagerDuty, Slack',
    interviewAnswer:
      'We integrated our Great Expectations quality suite with PagerDuty for critical failures and Slack for warnings, and established a 2-hour SLA for critical quality issues — reducing our quality issue resolution time from an average of 3 days to under 4 hours.',
  },
  {
    id: 114,
    group: 'Observability',
    title: 'Data Drift Undetected Until Business Impact',
    why: 'Statistical drift in key metrics (distribution shift, mean change, volume change) is not monitored; changes in business conditions that affect data are discovered only when business reports diverge from expectations.',
    impact:
      'ML models silently degrade; dashboard metrics shift without explanation; root cause investigation is reactive and expensive.',
    solution:
      'Implement statistical drift monitoring on key metrics and ML features; run automated distribution tests (KS, chi-squared, PSI) on a daily or weekly schedule; alert when drift exceeds defined thresholds.',
    tools:
      'Monte Carlo data observability, Evidently AI, Databricks Lakehouse Monitoring, Great Expectations custom checks',
    interviewAnswer:
      'We deployed Monte Carlo drift monitoring on our 50 most critical business metrics with weekly PSI calculations, catching a significant input data distribution shift from a product change 2 weeks before it would have appeared as a degraded dashboard metric.',
  },
  {
    id: 115,
    group: 'Observability',
    title: 'Pipeline Execution Invisibility',
    why: 'Stakeholders cannot self-serve answers to "when will my data be ready?" or "why is my dashboard stale?"; every question requires engineering investigation.',
    impact:
      'High engineering support burden; stakeholder frustration; trust erosion in the data platform.',
    solution:
      'Build a self-service pipeline status dashboard showing last run time, status, and next scheduled run for all pipelines; expose data freshness metadata on each table via a catalog API consumed by BI tools.',
    tools:
      'Databricks Workflows run history API, custom pipeline status dashboard, dbt docs freshness, Airflow public status page',
    interviewAnswer:
      'We built a self-service pipeline and freshness status dashboard published to the company intranet, which answered 80% of the stakeholder "when will my data be ready?" questions without engineering involvement and reduced our support ticket volume by 60%.',
  },
  {
    id: 116,
    group: 'Observability',
    title: 'No Column-Level Data Lineage',
    why: "Table-level lineage shows which tables depend on which, but column-level lineage is required to trace where a specific field's value comes from across transformation chains.",
    impact:
      'Impact analysis for column changes is manual and error-prone; compliance audits requiring field-level traceability cannot be satisfied.',
    solution:
      'Enable column-level lineage capture in Unity Catalog; instrument dbt transformations with column lineage metadata; build a lineage explorer UI that allows analysts to trace any field back to its source.',
    tools:
      'Databricks Unity Catalog column lineage, dbt column lineage, OpenLineage, Atlan column lineage explorer',
    interviewAnswer:
      'We enabled Unity Catalog automatic column-level lineage and surfaced it in our data catalog, enabling us to satisfy a regulatory audit requirement for field-level data provenance across our entire analytical estate within 2 weeks — a request that would have taken months to answer manually.',
  },
  {
    id: 117,
    group: 'Observability',
    title: 'Distributed Debugging Across Pipeline Hops',
    why: 'When a data issue is reported, tracing it through 8 pipeline stages across different tools (Spark, dbt, SQL) requires context switching between systems with no shared correlation identifier.',
    impact:
      'Debugging data incidents takes days; engineers must reconstruct context manually for each system; knowledge is siloed.',
    solution:
      'Implement a correlation ID that propagates through all pipeline stages as a consistent run identifier; emit structured logs with the correlation ID to a centralized log aggregation system; build a unified incident investigation view.',
    tools:
      'Correlation ID pattern, structured logging, Splunk, Datadog distributed tracing, OpenTelemetry for data pipelines',
    interviewAnswer:
      'We implemented a correlation ID propagated across all pipeline stages and centralized logs in Datadog, enabling any data incident to be reconstructed in minutes by filtering on the correlation ID across the full execution chain.',
  },
  {
    id: 118,
    group: 'Observability',
    title: 'No SLA Tracking for Data Products',
    why: 'Data products have informal SLA commitments but no formal tracking, measurement, or consequence for violations — making SLAs meaningless in practice.',
    impact:
      'SLA violations go unnoticed by the data team; consumers have no recourse; the data platform has no accountability for reliability.',
    solution:
      'Define formal SLAs per data product (freshness, completeness, accuracy); instrument automated SLA measurement against each metric; publish SLA compliance rates to stakeholders monthly; trigger escalation on violation.',
    tools:
      'Custom SLA tracking tables, freshness monitoring, completeness checks, SLA compliance dashboards, escalation workflows',
    interviewAnswer:
      'We defined and codified SLAs for our 30 most critical data products and built automated SLA compliance tracking published to a leadership dashboard, which made our reliability commitments measurable and drove a 40-point improvement in SLA compliance within one quarter.',
  },
  {
    id: 119,
    group: 'Observability',
    title: 'Inconsistent Metric Definitions Across Tools',
    why: 'The same business metric (weekly active users, monthly revenue) is computed differently in Looker, Tableau, and ad-hoc SQL queries, producing conflicting numbers in different reports.',
    impact:
      'Leadership cannot trust any single number; time is wasted reconciling conflicting reports instead of making decisions.',
    solution:
      "Implement a semantic layer that defines metrics once and serves them consistently to all BI tools; adopt a metrics catalog that documents each metric's definition, owner, and approved calculation.",
    tools:
      'dbt Semantic Layer, LookML, Cube.js, Atlan metrics catalog, Databricks SQL dashboard standardization',
    interviewAnswer:
      'We implemented the dbt Semantic Layer as our single metric definition layer and certified all official metrics in our data catalog, which eliminated the "which number is right?" debates that had been consuming 3 hours of leadership meeting time per week.',
  },
  {
    id: 120,
    group: 'Observability',
    title: 'No Root Cause Analysis Process for Data Incidents',
    why: 'When data incidents occur, the team fixes the immediate issue and moves on without understanding the root cause, allowing the same class of failure to recur repeatedly.',
    impact:
      'The same types of incidents recur; the team is in permanent firefighting mode; underlying systemic issues are never addressed.',
    solution:
      'Implement a formal blameless post-mortem process for all data incidents with severity P1/P2; track root causes in an incident catalog; identify recurring failure patterns and address them with systemic fixes; review incident trends monthly.',
    tools:
      'Blameless post-mortem template, incident tracking database, trend analysis dashboard, systemic fix backlog, monthly review cadence',
    interviewAnswer:
      'We implemented blameless post-mortems for all critical data incidents and built an incident trend dashboard that surfaced the top 3 recurring failure patterns; addressing those root causes reduced our incident rate by 55% over 6 months.',
  },
];

const groups = [...new Set(challenges.map((c) => c.group))];

function DataArchitectChallenges() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('id');

  const filtered = challenges
    .filter((c) => {
      const matchGroup = selectedGroup === 'All' || c.group === selectedGroup;
      const q = searchTerm.toLowerCase();
      const matchSearch =
        c.title.toLowerCase().includes(q) ||
        c.why.toLowerCase().includes(q) ||
        c.solution.toLowerCase().includes(q) ||
        c.group.toLowerCase().includes(q);
      return matchGroup && matchSearch;
    })
    .sort((a, b) => {
      if (sortBy === 'title') return a.title.localeCompare(b.title);
      if (sortBy === 'group') return a.group.localeCompare(b.group);
      return a.id - b.id;
    });

  const downloadCSV = () => {
    exportToCSV(
      challenges.map((c) => ({
        id: c.id,
        group: c.group,
        title: c.title,
        why: c.why,
        impact: c.impact,
        solution: c.solution,
        tools: c.tools,
        interviewAnswer: c.interviewAnswer,
      })),
      'data-architect-challenges.csv'
    );
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Data Architect Challenges</h1>
          <p>120 challenges with root cause, impact, solution, tools, and interview answers</p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F9F1;</div>
          <div className="stat-info">
            <h4>120</h4>
            <p>Challenges</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#x1F4CA;</div>
          <div className="stat-info">
            <h4>12</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#x1F4CC;</div>
          <div className="stat-info">
            <h4>10</h4>
            <p>Per Category</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#x1F3AF;</div>
          <div className="stat-info">
            <h4>{filtered.length}</h4>
            <p>Showing</p>
          </div>
        </div>
      </div>

      <FileFormatRunner
        data={challenges.map((c) => ({
          id: c.id,
          group: c.group,
          title: c.title,
          why: c.why,
          impact: c.impact,
          solution: c.solution,
          tools: c.tools,
          interviewAnswer: c.interviewAnswer,
        }))}
        slug="architect-challenges"
        schemaName="ArchitectChallenge"
        tableName="catalog.architect.challenges"
      />

      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search challenges..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '280px' }}
          />
          <select
            className="form-input"
            value={selectedGroup}
            onChange={(e) => setSelectedGroup(e.target.value)}
            style={{ maxWidth: '260px' }}
          >
            <option value="All">All Categories (120)</option>
            {groups.map((g) => (
              <option key={g} value={g}>
                {g} ({challenges.filter((c) => c.group === g).length})
              </option>
            ))}
          </select>
          <select
            className="form-input"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            style={{ maxWidth: '160px' }}
          >
            <option value="id">Sort by #</option>
            <option value="title">Sort by Title</option>
            <option value="group">Sort by Category</option>
          </select>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadCSV}
            style={{ marginLeft: 'auto' }}
          >
            Download CSV
          </button>
        </div>
      </div>

      {filtered.length === 0 && (
        <div
          className="card"
          style={{ textAlign: 'center', color: 'var(--text-secondary)', padding: '2rem' }}
        >
          No challenges match your search. Try a different keyword or category.
        </div>
      )}

      {/* Light category color palette */}
      {(() => null)()}

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(420px, 1fr))',
          gap: '1rem',
        }}
      >
        {filtered.map((c) => {
          // Light color palette per category
          const categoryColors = {
            'Ingestion & Pipeline': { bg: '#fef3c7', accent: '#f59e0b', border: '#fde68a' },
            'Data Quality': { bg: '#dcfce7', accent: '#22c55e', border: '#bbf7d0' },
            'Data Modeling': { bg: '#dbeafe', accent: '#3b82f6', border: '#bfdbfe' },
            Performance: { bg: '#fce7f3', accent: '#ec4899', border: '#fbcfe8' },
            'Pipeline Orchestration': { bg: '#e0e7ff', accent: '#6366f1', border: '#c7d2fe' },
            'Governance & Compliance': { bg: '#fef2f2', accent: '#ef4444', border: '#fecaca' },
            Security: { bg: '#fee2e2', accent: '#dc2626', border: '#fecaca' },
            'Data Integration': { bg: '#ecfccb', accent: '#84cc16', border: '#d9f99d' },
            'AI/ML/RAG': { bg: '#f3e8ff', accent: '#a855f7', border: '#e9d5ff' },
            'Cost & FinOps': { bg: '#fff7ed', accent: '#ea580c', border: '#fed7aa' },
            'Organization & Process': { bg: '#ccfbf1', accent: '#14b8a6', border: '#99f6e4' },
            Observability: { bg: '#cffafe', accent: '#06b6d4', border: '#a5f3fc' },
          };
          const colors = categoryColors[c.group] || {
            bg: '#f3f4f6',
            accent: '#6b7280',
            border: '#e5e7eb',
          };
          const isExpanded = expandedId === c.id;

          return (
            <div
              key={c.id}
              style={{
                background: colors.bg,
                border: `2px solid ${colors.border}`,
                borderLeft: `6px solid ${colors.accent}`,
                borderRadius: '10px',
                padding: '1rem 1.25rem',
                marginBottom: '0.5rem',
                transition: 'all 0.2s',
                boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
              }}
            >
              <div
                onClick={() => setExpandedId(isExpanded ? null : c.id)}
                style={{
                  cursor: 'pointer',
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'flex-start',
                  gap: '0.75rem',
                }}
              >
                <div style={{ flex: 1 }}>
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.4rem',
                      flexWrap: 'wrap',
                      marginBottom: '0.3rem',
                    }}
                  >
                    <span
                      style={{
                        background: colors.accent,
                        color: '#fff',
                        padding: '0.15rem 0.5rem',
                        borderRadius: '4px',
                        fontSize: '0.65rem',
                        fontWeight: 700,
                        textTransform: 'uppercase',
                      }}
                    >
                      {c.group}
                    </span>
                    <strong style={{ fontSize: '0.95rem', color: '#1a1a1a' }}>
                      #{c.id} — {c.title}
                    </strong>
                  </div>
                </div>
                <span style={{ color: colors.accent, flexShrink: 0, fontSize: '1.1rem' }}>
                  {isExpanded ? '\u25BC' : '\u25B6'}
                </span>
              </div>

              {/* Always visible summary */}
              <div
                style={{
                  marginTop: '0.6rem',
                  display: 'grid',
                  gridTemplateColumns: '1fr 1fr',
                  gap: '0.5rem',
                }}
              >
                <div
                  style={{
                    padding: '0.4rem 0.6rem',
                    background: 'rgba(255,255,255,0.7)',
                    borderRadius: '6px',
                  }}
                >
                  <div
                    style={{
                      fontSize: '0.6rem',
                      fontWeight: 700,
                      color: '#991b1b',
                      textTransform: 'uppercase',
                      marginBottom: '0.15rem',
                    }}
                  >
                    {'\u26a0\ufe0f'} Why Hard
                  </div>
                  <div style={{ fontSize: '0.75rem', color: '#1a1a1a', lineHeight: 1.4 }}>
                    {c.why}
                  </div>
                </div>
                <div
                  style={{
                    padding: '0.4rem 0.6rem',
                    background: 'rgba(255,255,255,0.7)',
                    borderRadius: '6px',
                  }}
                >
                  <div
                    style={{
                      fontSize: '0.6rem',
                      fontWeight: 700,
                      color: '#166534',
                      textTransform: 'uppercase',
                      marginBottom: '0.15rem',
                    }}
                  >
                    {'\u2705'} Solution
                  </div>
                  <div style={{ fontSize: '0.75rem', color: '#1a1a1a', lineHeight: 1.4 }}>
                    {c.solution}
                  </div>
                </div>
              </div>

              {isExpanded && <DeepDetailView item={c} />}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default DataArchitectChallenges;
