import React, { useState, useRef } from 'react';
import { exportToCSV, exportToJSON } from '../utils/fileExport';
import { getSampleData } from '../utils/sampleData';
import DataPreview from '../components/common/DataPreview';

const pipelines = [
  {
    id: 1,
    category: 'Ingestion',
    title: 'CSV \u2192 Bronze \u2192 Silver',
    desc: 'File ingestion pipeline',
    stages: [
      'Read CSV from landing',
      'Add metadata columns',
      'Write to Bronze Delta',
      'Validate schema',
      'Clean & deduplicate',
      'Write to Silver Delta',
    ],
    code: `# Pipeline: CSV \u2192 Bronze \u2192 Silver
from pyspark.sql.functions import current_timestamp, input_file_name, col, trim, lower

# Stage 1: Ingest from landing
raw = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "csv") \\
    .option("header", "true") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/csv_pipeline") \\
    .load("/mnt/landing/csv/")

# Stage 2: Enrich & write Bronze
bronze = raw.withColumn("_ingest_ts", current_timestamp()) \\
    .withColumn("_source_file", input_file_name())
bronze.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/csv_bronze") \\
    .trigger(availableNow=True) \\
    .toTable("catalog.bronze.csv_data")

# Stage 3: Clean & write Silver
bronze_df = spark.table("catalog.bronze.csv_data")
silver = bronze_df.dropDuplicates(["id"]) \\
    .filter(col("id").isNotNull()) \\
    .withColumn("name", trim(col("name"))) \\
    .withColumn("email", lower(col("email")))
silver.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.clean_csv")`,
  },
  {
    id: 2,
    category: 'Ingestion',
    title: 'API \u2192 Bronze \u2192 Silver \u2192 Gold',
    desc: 'API data pipeline end-to-end',
    stages: [
      'Fetch API data',
      'Write to Bronze',
      'Validate & clean',
      'Write to Silver',
      'Aggregate',
      'Write to Gold',
    ],
    code: `import requests
from pyspark.sql.functions import current_timestamp, col

# Stage 1: Extract from API
resp = requests.get("https://api.example.com/v1/orders",
    headers={"Authorization": f"Bearer {dbutils.secrets.get('s','token')}"}, timeout=30)
data = resp.json()["results"]

# Stage 2: Bronze
df = spark.createDataFrame(data).withColumn("_ingest_ts", current_timestamp())
df.write.format("delta").mode("append").saveAsTable("catalog.bronze.api_orders")

# Stage 3: Silver
silver = spark.table("catalog.bronze.api_orders") \\
    .dropDuplicates(["order_id"]) \\
    .filter(col("amount") > 0)
silver.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.orders")

# Stage 4: Gold
spark.sql("""
CREATE OR REPLACE TABLE catalog.gold.daily_orders AS
SELECT order_date, COUNT(*) AS order_count, SUM(amount) AS revenue
FROM catalog.silver.orders GROUP BY order_date
""")`,
  },
  {
    id: 3,
    category: 'Ingestion',
    title: 'Kafka \u2192 Bronze \u2192 Silver',
    desc: 'Streaming Kafka pipeline',
    stages: ['Connect Kafka', 'Parse JSON', 'Write Bronze', 'Clean', 'Write Silver'],
    code: '# Kafka Streaming Pipeline\ndf = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "broker:9092") \\\n    .option("subscribe", "events") \\\n    .load()',
  },
  {
    id: 4,
    category: 'Ingestion',
    title: 'JDBC \u2192 Bronze \u2192 Silver',
    desc: 'Database extraction pipeline',
    stages: ['Connect JDBC', 'Read table', 'Write Bronze', 'Transform', 'Write Silver'],
    code: '# JDBC Pipeline\ndf = spark.read.format("jdbc") \\\n    .option("url", jdbc_url) \\\n    .option("dbtable", "orders") \\\n    .load()',
  },
  {
    id: 5,
    category: 'Ingestion',
    title: 'S3 \u2192 Bronze \u2192 Silver',
    desc: 'AWS S3 ingestion pipeline',
    stages: ['Read S3', 'Parse', 'Write Bronze', 'Clean', 'Write Silver'],
    code: '# S3 Pipeline\ndf = spark.read.format("parquet") \\\n    .load("s3a://bucket/path/")',
  },
  {
    id: 6,
    category: 'ELT',
    title: 'SCD Type 2 Pipeline',
    desc: 'Slowly Changing Dimensions Type 2',
    stages: ['Read source', 'Compare', 'Close old', 'Insert new', 'Verify'],
    code: '# SCD Type 2\nfrom delta.tables import DeltaTable\ntarget = DeltaTable.forName(spark, "catalog.silver.customers")',
  },
  {
    id: 7,
    category: 'ELT',
    title: 'CDC Pipeline',
    desc: 'Change Data Capture processing',
    stages: ['Read CDC log', 'Parse operations', 'Apply inserts', 'Apply updates', 'Apply deletes'],
    code: '# CDC Pipeline\ncdc = spark.readStream.format("delta") \\\n    .option("readChangeFeed", "true") \\\n    .table("catalog.bronze.changes")',
  },
  {
    id: 8,
    category: 'ML',
    title: 'Feature Engineering Pipeline',
    desc: 'Build ML features',
    stages: ['Read raw data', 'Generate features', 'Store feature table', 'Validate', 'Register'],
    code: '# Feature Pipeline\nfrom databricks.feature_engineering import FeatureEngineeringClient\nfe = FeatureEngineeringClient()',
  },
  {
    id: 9,
    category: 'ML',
    title: 'Model Training Pipeline',
    desc: 'Train and register ML model',
    stages: ['Load features', 'Split data', 'Train model', 'Evaluate', 'Register'],
    code: '# Model Training\nimport mlflow\nwith mlflow.start_run():\n    model = train_model(X_train, y_train)',
  },
  {
    id: 10,
    category: 'ML',
    title: 'Batch Scoring Pipeline',
    desc: 'Score data with deployed model',
    stages: ['Load model', 'Read data', 'Generate scores', 'Write results', 'Monitor'],
    code: '# Batch Scoring\nmodel = mlflow.pyfunc.load_model("models:/fraud_model/Production")\nscores = model.predict(df)',
  },
  {
    id: 11,
    category: 'ML',
    title: 'Real-time Scoring Pipeline',
    desc: 'Stream scoring pipeline',
    stages: ['Read stream', 'Load model', 'Score batch', 'Write results', 'Alert'],
    code: '# Real-time Scoring\ndef score_batch(batch_df, batch_id):\n    predictions = model.predict(batch_df)\n    return predictions',
  },
  {
    id: 12,
    category: 'Governance',
    title: 'Data Quality Pipeline',
    desc: 'Automated data quality monitoring',
    stages: [
      'Profile data',
      'Run expectations',
      'Generate report',
      'Alert on failures',
      'Update dashboard',
    ],
    code: '# Data Quality\nfrom pyspark.sql.functions import count, when, isnull\ntables = ["catalog.silver.customers", "catalog.silver.orders"]',
  },
  {
    id: 13,
    category: 'Governance',
    title: 'Audit Trail Pipeline',
    desc: 'Track all data operations',
    stages: [
      'Capture operation',
      'Record metadata',
      'Write audit log',
      'Alert on anomalies',
      'Compliance report',
    ],
    code: '# Audit Trail\ndef audit_operation(op, table, count, status):\n    audit = spark.createDataFrame([{"operation": op}])',
  },
  {
    id: 14,
    category: 'Reporting',
    title: 'Daily Report Pipeline',
    desc: 'Automated daily report generation',
    stages: [
      'Aggregate data',
      'Generate charts',
      'Create HTML',
      'Save to volume',
      'Send notification',
    ],
    code: '# Daily Report\nimport plotly.express as px\ndaily = spark.sql("SELECT * FROM catalog.gold.fact_sales")',
  },
  {
    id: 15,
    category: 'Reporting',
    title: 'Snowflake Sync Pipeline',
    desc: 'Sync data to/from Snowflake',
    stages: ['Read Databricks', 'Transform', 'Write Snowflake', 'Verify sync', 'Log audit'],
    code: '# Snowflake Sync\ngold_df = spark.table("catalog.gold.daily_revenue")\ngold_df.write.format("snowflake").save()',
  },
  {
    id: 16,
    category: 'Ingestion',
    title: 'Image Data Pipeline',
    desc: 'Process images from landing zone',
    stages: [
      'Read binary',
      'Extract metadata',
      'Generate embeddings',
      'Store Delta',
      'Index vector DB',
    ],
    code: '# Image Pipeline\nimages = spark.readStream.format("cloudFiles") \\\n    .option("cloudFiles.format", "binaryFile") \\\n    .load("/mnt/landing/images/")',
  },
  {
    id: 17,
    category: 'Ingestion',
    title: 'Log Data Pipeline',
    desc: 'Parse and analyze log files',
    stages: ['Read logs', 'Parse regex', 'Extract fields', 'Write Bronze', 'Analyze patterns'],
    code: '# Log Pipeline\nfrom pyspark.sql.functions import regexp_extract\nlogs = spark.readStream.format("cloudFiles").load("/mnt/logs/")',
  },
  {
    id: 18,
    category: 'Ingestion',
    title: 'Text Document Pipeline',
    desc: 'Process text documents for NLP',
    stages: [
      'Read text',
      'Extract content',
      'Chunk text',
      'Generate embeddings',
      'Store vector DB',
    ],
    code: '# Text Pipeline\ndocs = spark.readStream.format("cloudFiles") \\\n    .option("wholetext", "true") \\\n    .load("/mnt/landing/text/")',
  },
  {
    id: 19,
    category: 'ELT',
    title: 'SCD Type 1 Pipeline',
    desc: 'Overwrite with latest values',
    stages: ['Read source', 'Compare target', 'Update changed', 'Insert new', 'Log changes'],
    code: '# SCD Type 1\nfrom delta.tables import DeltaTable\ntarget = DeltaTable.forName(spark, "catalog.silver.products")',
  },
  {
    id: 20,
    category: 'ELT',
    title: 'Data Reconciliation Pipeline',
    desc: 'Verify data between source and target',
    stages: [
      'Count source',
      'Count target',
      'Compare checksums',
      'Find mismatches',
      'Generate report',
    ],
    code: '# Reconciliation\nsource = spark.read.format("jdbc").load()\ntarget = spark.table("catalog.silver.orders")',
  },
];

const categories = [...new Set(pipelines.map((p) => p.category))];

const SCHEDULE_OPTIONS = [
  { value: 'manual', label: 'Manual (Run Now)', cron: null },
  { value: 'every_5min', label: 'Every 5 Minutes', cron: '*/5 * * * *' },
  { value: 'every_15min', label: 'Every 15 Minutes', cron: '*/15 * * * *' },
  { value: 'every_hour', label: 'Every Hour', cron: '0 * * * *' },
  { value: 'every_6hours', label: 'Every 6 Hours', cron: '0 */6 * * *' },
  { value: 'daily_midnight', label: 'Daily at Midnight', cron: '0 0 * * *' },
  { value: 'daily_6am', label: 'Daily at 6 AM', cron: '0 6 * * *' },
  { value: 'weekly_monday', label: 'Weekly (Monday 6 AM)', cron: '0 6 * * 1' },
  { value: 'monthly', label: 'Monthly (1st at Midnight)', cron: '0 0 1 * *' },
];

// ─── Test Suite Definitions ───────────────────
const TEST_SUITES = [
  {
    id: 'row_count',
    name: 'Row Count Check',
    desc: 'Verify source vs target row counts match',
    severity: 'critical',
  },
  {
    id: 'null_check',
    name: 'Null Value Check',
    desc: 'Check for unexpected NULL values in key columns',
    severity: 'critical',
  },
  {
    id: 'duplicate_check',
    name: 'Duplicate Check',
    desc: 'Verify no duplicate records in primary key',
    severity: 'critical',
  },
  {
    id: 'schema_validation',
    name: 'Schema Validation',
    desc: 'Verify column names, types, and order match expected schema',
    severity: 'critical',
  },
  {
    id: 'data_type_check',
    name: 'Data Type Check',
    desc: 'Validate data types (numeric, date, string) are correct',
    severity: 'high',
  },
  {
    id: 'referential_integrity',
    name: 'Referential Integrity',
    desc: 'Check foreign key relationships are valid',
    severity: 'high',
  },
  {
    id: 'range_check',
    name: 'Range / Boundary Check',
    desc: 'Validate values are within expected min/max range',
    severity: 'high',
  },
  {
    id: 'freshness_check',
    name: 'Data Freshness',
    desc: 'Verify data timestamp is within acceptable lag',
    severity: 'medium',
  },
  {
    id: 'completeness',
    name: 'Completeness Check',
    desc: 'Verify all required fields are populated',
    severity: 'medium',
  },
  {
    id: 'format_check',
    name: 'Format Validation',
    desc: 'Check email, phone, date formats are valid',
    severity: 'medium',
  },
  {
    id: 'checksum',
    name: 'Checksum / Hash Match',
    desc: 'Compare source vs target data checksums',
    severity: 'high',
  },
  {
    id: 'business_rule',
    name: 'Business Rule Validation',
    desc: 'Apply domain-specific rules (e.g., amount > 0, status in allowed list)',
    severity: 'critical',
  },
  {
    id: 'reconciliation',
    name: 'Source-Target Reconciliation',
    desc: 'Full row-level comparison between source and target',
    severity: 'high',
  },
  {
    id: 'scd_validation',
    name: 'SCD History Validation',
    desc: 'Verify SCD Type 2 history chain is unbroken',
    severity: 'medium',
  },
  {
    id: 'performance',
    name: 'Performance Benchmark',
    desc: 'Check query execution time is within SLA',
    severity: 'low',
  },
];

function PipelineBuilder() {
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [activeTab, setActiveTab] = useState('pipelines');

  // Job scheduling state
  const [jobs, setJobs] = useState([]);
  const [jobRuns, setJobRuns] = useState([]);
  const [scheduleModal, setScheduleModal] = useState(null);
  const [scheduleConfig, setScheduleConfig] = useState({
    schedule: 'manual',
    notify: true,
    retries: 1,
    timeout: 30,
  });

  // Test reports state
  const [testReports, setTestReports] = useState([]);
  const [expandedReport, setExpandedReport] = useState(null);
  const [previewId, setPreviewId] = useState(null);

  const runIdCounter = useRef(1);
  const testIdCounter = useRef(1);

  const filtered = pipelines.filter((p) => {
    const matchCat = selectedCategory === 'All' || p.category === selectedCategory;
    const matchSearch =
      p.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      p.desc.toLowerCase().includes(searchTerm.toLowerCase());
    return matchCat && matchSearch;
  });

  // ─── Schedule a job ────────────────────────
  const scheduleJob = (pipeline) => {
    const scheduleInfo = SCHEDULE_OPTIONS.find((s) => s.value === scheduleConfig.schedule);
    const newJob = {
      id: `job_${Date.now()}`,
      pipelineId: pipeline.id,
      pipelineTitle: pipeline.title,
      category: pipeline.category,
      schedule: scheduleInfo.label,
      cron: scheduleInfo.cron,
      scheduleValue: scheduleConfig.schedule,
      retries: scheduleConfig.retries,
      timeout: scheduleConfig.timeout,
      notify: scheduleConfig.notify,
      status: scheduleConfig.schedule === 'manual' ? 'manual' : 'scheduled',
      createdAt: new Date().toISOString(),
      nextRun: scheduleConfig.schedule === 'manual' ? null : getNextRun(scheduleInfo.value),
      totalRuns: 0,
      lastRun: null,
    };
    setJobs((prev) => [...prev, newJob]);
    setScheduleModal(null);

    if (scheduleConfig.schedule === 'manual') {
      runJob(newJob, pipeline);
    }
  };

  // ─── Run a job ─────────────────────────────
  const runJob = (job, pipeline) => {
    const runId = `run_${runIdCounter.current++}`;
    const startTime = new Date().toISOString();
    const stages = pipeline
      ? pipeline.stages
      : pipelines.find((p) => p.id === job.pipelineId)?.stages || [];

    // Create run entry
    const newRun = {
      id: runId,
      jobId: job.id,
      pipelineTitle: job.pipelineTitle,
      startTime,
      endTime: null,
      status: 'running',
      currentStage: 0,
      totalStages: stages.length,
      stages: stages.map((s, i) => ({
        name: s,
        status: i === 0 ? 'running' : 'pending',
        duration: null,
      })),
      logs: [`[${formatTime(startTime)}] Job started: ${job.pipelineTitle}`],
    };

    setJobRuns((prev) => [newRun, ...prev]);
    setJobs((prev) =>
      prev.map((j) =>
        j.id === job.id
          ? { ...j, status: 'running', totalRuns: j.totalRuns + 1, lastRun: startTime }
          : j
      )
    );

    // Simulate stage execution
    simulateStages(runId, job.id, stages, 0);
  };

  // ─── Auto-trigger tests after job completes ──
  const runTestSuite = (runId, pipelineTitle, trigger) => {
    const reportId = `test_${testIdCounter.current++}`;
    const startTime = new Date().toISOString();

    const report = {
      id: reportId,
      runId,
      pipelineTitle,
      trigger, // 'auto' or 'manual'
      startTime,
      endTime: null,
      status: 'running',
      tests: TEST_SUITES.map((t) => ({
        ...t,
        status: 'pending',
        result: null,
        duration: null,
        details: null,
      })),
      summary: null,
    };

    setTestReports((prev) => [report, ...prev]);

    // Simulate running each test sequentially
    simulateTests(reportId, 0);
  };

  const simulateTests = (reportId, testIdx) => {
    if (testIdx >= TEST_SUITES.length) {
      // All tests done — generate summary
      setTestReports((prev) =>
        prev.map((r) => {
          if (r.id !== reportId) return r;
          const passed = r.tests.filter((t) => t.status === 'passed').length;
          const failed = r.tests.filter((t) => t.status === 'failed').length;
          const warnings = r.tests.filter((t) => t.status === 'warning').length;
          return {
            ...r,
            status: failed > 0 ? 'failed' : warnings > 0 ? 'warning' : 'passed',
            endTime: new Date().toISOString(),
            summary: {
              total: TEST_SUITES.length,
              passed,
              failed,
              warnings,
              score: Math.round((passed / TEST_SUITES.length) * 100),
            },
          };
        })
      );
      return;
    }

    const duration = Math.floor(Math.random() * 800) + 200;
    const rand = Math.random();
    // 75% pass, 15% fail, 10% warning
    const result = rand < 0.75 ? 'passed' : rand < 0.9 ? 'failed' : 'warning';

    const detailsMap = {
      passed: 'All checks passed. Data is consistent.',
      failed: 'Validation failed. Mismatch detected — review data.',
      warning: 'Minor issue detected. Within acceptable threshold.',
    };

    const metricsMap = {
      row_count: {
        source: 15243,
        target: result === 'failed' ? 15100 : 15243,
        delta: result === 'failed' ? -143 : 0,
      },
      null_check: {
        columns_checked: 12,
        nulls_found: result === 'failed' ? 45 : 0,
        threshold: '0%',
      },
      duplicate_check: { total_rows: 15243, duplicates: result === 'failed' ? 23 : 0 },
      schema_validation: {
        expected_cols: 15,
        actual_cols: result === 'failed' ? 14 : 15,
        missing: result === 'failed' ? ['updated_at'] : [],
      },
      data_type_check: { columns: 15, type_mismatches: result === 'failed' ? 2 : 0 },
      referential_integrity: { fk_checks: 5, violations: result === 'failed' ? 1 : 0 },
      range_check: { checks: 8, out_of_range: result === 'failed' ? 3 : 0 },
      freshness_check: {
        last_update: new Date().toISOString(),
        lag_minutes: result === 'warning' ? 35 : 2,
        sla_minutes: 30,
      },
      completeness: { required_fields: 10, populated: result === 'failed' ? 8 : 10 },
      format_check: { records_checked: 15243, format_errors: result === 'failed' ? 87 : 0 },
      checksum: { source_hash: 'a3f2c1', target_hash: result === 'failed' ? 'b4e3d2' : 'a3f2c1' },
      business_rule: { rules: 6, violations: result === 'failed' ? 2 : 0 },
      reconciliation: {
        matched: result === 'failed' ? 14800 : 15243,
        unmatched: result === 'failed' ? 443 : 0,
      },
      scd_validation: { records: 5000, broken_chains: result === 'failed' ? 12 : 0 },
      performance: { query_time_ms: result === 'warning' ? 4500 : 850, sla_ms: 3000 },
    };

    setTimeout(() => {
      setTestReports((prev) =>
        prev.map((r) => {
          if (r.id !== reportId) return r;
          const updatedTests = r.tests.map((t, i) => {
            if (i === testIdx) {
              return {
                ...t,
                status: result,
                result: detailsMap[result],
                duration,
                details: metricsMap[t.id] || null,
              };
            }
            if (i === testIdx + 1) return { ...t, status: 'running' };
            return t;
          });
          // Mark first test as running
          if (testIdx === 0) updatedTests[0].status = result;
          return { ...r, tests: updatedTests };
        })
      );
      simulateTests(reportId, testIdx + 1);
    }, duration);
  };

  const simulateStages = (runId, jobId, stages, stageIdx) => {
    if (stageIdx >= stages.length) {
      // Job completed — auto-trigger test suite
      const endTime = new Date().toISOString();
      const pipelineTitle = jobRuns.find((r) => r.id === runId)?.pipelineTitle || 'Unknown';

      setJobRuns((prev) =>
        prev.map((r) =>
          r.id === runId
            ? {
                ...r,
                status: 'completed',
                endTime,
                currentStage: stages.length,
                logs: [
                  ...r.logs,
                  `[${formatTime(endTime)}] Job completed successfully`,
                  `[${formatTime(endTime)}] Auto-triggering test suite...`,
                ],
              }
            : r
        )
      );
      setJobs((prev) =>
        prev.map((j) => (j.id === jobId ? { ...j, status: j.cron ? 'scheduled' : 'completed' } : j))
      );

      // AUTO-TRIGGER: Start test suite after 500ms delay
      setTimeout(() => {
        runTestSuite(runId, pipelineTitle, 'auto');
      }, 500);
      return;
    }

    // Random failure chance (10%)
    const willFail = Math.random() < 0.1 && stageIdx > 0;
    const duration = Math.floor(Math.random() * 3000) + 1000;

    setTimeout(() => {
      const now = new Date().toISOString();

      if (willFail) {
        setJobRuns((prev) =>
          prev.map((r) =>
            r.id === runId
              ? {
                  ...r,
                  status: 'failed',
                  endTime: now,
                  stages: r.stages.map((s, i) =>
                    i === stageIdx ? { ...s, status: 'failed', duration } : s
                  ),
                  logs: [
                    ...r.logs,
                    `[${formatTime(now)}] FAILED at stage: ${stages[stageIdx]}`,
                    `[${formatTime(now)}] Error: Simulated failure — retry or check logs`,
                  ],
                }
              : r
          )
        );
        setJobs((prev) => prev.map((j) => (j.id === jobId ? { ...j, status: 'failed' } : j)));
        return;
      }

      // Stage success
      setJobRuns((prev) =>
        prev.map((r) =>
          r.id === runId
            ? {
                ...r,
                currentStage: stageIdx + 1,
                stages: r.stages.map((s, i) => {
                  if (i === stageIdx) return { ...s, status: 'completed', duration };
                  if (i === stageIdx + 1) return { ...s, status: 'running' };
                  return s;
                }),
                logs: [
                  ...r.logs,
                  `[${formatTime(now)}] Completed: ${stages[stageIdx]} (${(duration / 1000).toFixed(1)}s)`,
                ],
              }
            : r
        )
      );

      simulateStages(runId, jobId, stages, stageIdx + 1);
    }, duration);
  };

  // ─── Cancel a running job ──────────────────
  const cancelJob = (jobId) => {
    setJobs((prev) => prev.map((j) => (j.id === jobId ? { ...j, status: 'cancelled' } : j)));
    setJobRuns((prev) =>
      prev.map((r) =>
        r.jobId === jobId && r.status === 'running'
          ? {
              ...r,
              status: 'cancelled',
              endTime: new Date().toISOString(),
              logs: [...r.logs, `[${formatTime(new Date().toISOString())}] Job cancelled by user`],
            }
          : r
      )
    );
  };

  // ─── Delete a job ──────────────────────────
  const deleteJob = (jobId) => {
    setJobs((prev) => prev.filter((j) => j.id !== jobId));
  };

  // ─── Download run history ──────────────────
  const downloadRunHistory = () => {
    const data = jobRuns.map((r) => ({
      run_id: r.id,
      pipeline: r.pipelineTitle,
      status: r.status,
      start_time: r.startTime,
      end_time: r.endTime || '',
      stages_completed: r.stages.filter((s) => s.status === 'completed').length,
      total_stages: r.totalStages,
    }));
    exportToCSV(data, `pipeline-runs-${new Date().toISOString().slice(0, 10)}.csv`);
  };

  const downloadJobsConfig = () => {
    exportToJSON(jobs, `pipeline-jobs-${new Date().toISOString().slice(0, 10)}.json`);
  };

  const downloadTestReport = (report) => {
    const data = report.tests.map((t) => ({
      test_name: t.name,
      severity: t.severity,
      status: t.status,
      result: t.result || '',
      duration_ms: t.duration || '',
      details: t.details ? JSON.stringify(t.details) : '',
    }));
    exportToCSV(
      data,
      `test-report-${report.pipelineTitle.replace(/[^a-zA-Z0-9]/g, '_')}-${new Date().toISOString().slice(0, 19).replace(/:/g, '')}.csv`
    );
  };

  const downloadAllTestReports = () => {
    const data = testReports.flatMap((r) =>
      r.tests.map((t) => ({
        report_id: r.id,
        pipeline: r.pipelineTitle,
        trigger: r.trigger,
        report_status: r.status,
        test_name: t.name,
        severity: t.severity,
        status: t.status,
        result: t.result || '',
        duration_ms: t.duration || '',
        timestamp: r.startTime,
      }))
    );
    if (data.length > 0)
      exportToCSV(data, `all-test-reports-${new Date().toISOString().slice(0, 10)}.csv`);
  };

  // ─── Helpers ───────────────────────────────
  function formatTime(iso) {
    return new Date(iso).toLocaleTimeString();
  }

  function getNextRun(scheduleValue) {
    const now = new Date();
    const map = {
      every_5min: 5 * 60000,
      every_15min: 15 * 60000,
      every_hour: 60 * 60000,
      every_6hours: 6 * 60 * 60000,
      daily_midnight: 24 * 60 * 60000,
      daily_6am: 24 * 60 * 60000,
      weekly_monday: 7 * 24 * 60 * 60000,
      monthly: 30 * 24 * 60 * 60000,
    };
    return new Date(now.getTime() + (map[scheduleValue] || 60000)).toISOString();
  }

  function getStatusBadge(status) {
    const map = {
      running: 'running',
      completed: 'success',
      failed: 'failed',
      scheduled: 'completed',
      manual: 'pending',
      cancelled: 'stopped',
    };
    return map[status] || 'pending';
  }

  // ─── Render ────────────────────────────────
  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Pipeline Builder</h1>
          <p>
            {pipelines.length} pipelines | {jobs.length} jobs | {jobRuns.length} runs
          </p>
        </div>
      </div>

      {/* Tabs */}
      <div className="tabs">
        <button
          className={`tab ${activeTab === 'pipelines' ? 'active' : ''}`}
          onClick={() => setActiveTab('pipelines')}
        >
          Pipelines ({pipelines.length})
        </button>
        <button
          className={`tab ${activeTab === 'jobs' ? 'active' : ''}`}
          onClick={() => setActiveTab('jobs')}
        >
          Scheduled Jobs ({jobs.length})
        </button>
        <button
          className={`tab ${activeTab === 'runs' ? 'active' : ''}`}
          onClick={() => setActiveTab('runs')}
        >
          Run History ({jobRuns.length})
        </button>
        <button
          className={`tab ${activeTab === 'tests' ? 'active' : ''}`}
          onClick={() => setActiveTab('tests')}
        >
          Test Reports ({testReports.length})
        </button>
      </div>

      {/* ═══ TAB: Pipelines ═══ */}
      {activeTab === 'pipelines' && (
        <>
          <div className="card" style={{ marginBottom: '1.5rem' }}>
            <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
              <input
                type="text"
                className="form-input"
                placeholder="Search pipelines..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                style={{ maxWidth: '300px' }}
              />
              <select
                className="form-input"
                value={selectedCategory}
                onChange={(e) => setSelectedCategory(e.target.value)}
                style={{ maxWidth: '200px' }}
              >
                <option value="All">All Categories ({pipelines.length})</option>
                {categories.map((c) => (
                  <option key={c} value={c}>
                    {c} ({pipelines.filter((p) => p.category === c).length})
                  </option>
                ))}
              </select>
            </div>
          </div>

          {filtered.map((p) => (
            <div key={p.id} className="card" style={{ marginBottom: '0.75rem' }}>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'flex-start',
                }}
              >
                <div
                  onClick={() => setExpandedId(expandedId === p.id ? null : p.id)}
                  style={{ cursor: 'pointer', flex: 1 }}
                >
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      marginBottom: '0.5rem',
                    }}
                  >
                    <span className="badge completed">{p.category}</span>
                    <strong>
                      #{p.id} \u2014 {p.title}
                    </strong>
                    <span style={{ marginLeft: 'auto', color: 'var(--text-secondary)' }}>
                      {expandedId === p.id ? '\u25BC' : '\u25B6'}
                    </span>
                  </div>
                  <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{p.desc}</p>
                  <div
                    style={{
                      display: 'flex',
                      gap: '0.25rem',
                      flexWrap: 'wrap',
                      marginTop: '0.5rem',
                    }}
                  >
                    {p.stages.map((s, i) => (
                      <React.Fragment key={i}>
                        <span
                          style={{
                            padding: '0.2rem 0.5rem',
                            background: '#eff6ff',
                            borderRadius: '4px',
                            fontSize: '0.7rem',
                            color: '#1e40af',
                          }}
                        >
                          {s}
                        </span>
                        {i < p.stages.length - 1 && (
                          <span style={{ color: '#9ca3af', fontSize: '0.8rem' }}>\u2192</span>
                        )}
                      </React.Fragment>
                    ))}
                  </div>
                </div>
                <div style={{ display: 'flex', gap: '0.5rem', marginLeft: '1rem', flexShrink: 0 }}>
                  <button
                    className="btn btn-primary btn-sm"
                    onClick={() => {
                      setScheduleModal(p);
                      setScheduleConfig({
                        schedule: 'manual',
                        notify: true,
                        retries: 1,
                        timeout: 30,
                      });
                    }}
                  >
                    Run Now
                  </button>
                  <button
                    className="btn btn-secondary btn-sm"
                    onClick={() => {
                      setScheduleModal(p);
                      setScheduleConfig({
                        schedule: 'daily_6am',
                        notify: true,
                        retries: 1,
                        timeout: 30,
                      });
                    }}
                  >
                    Schedule
                  </button>
                  <button
                    className="btn btn-sm"
                    style={{ background: '#eff6ff', color: '#1e40af' }}
                    onClick={() => setPreviewId(previewId === p.id ? null : p.id)}
                  >
                    {previewId === p.id ? 'Hide Data' : 'Data Preview'}
                  </button>
                </div>
              </div>
              {/* Data Preview: Before vs After */}
              {previewId === p.id && (
                <DataPreview data={getSampleData(p.id)} onClose={() => setPreviewId(null)} />
              )}
              {expandedId === p.id && (
                <div className="code-block" style={{ marginTop: '1rem' }}>
                  {p.code}
                </div>
              )}
            </div>
          ))}
        </>
      )}

      {/* ═══ TAB: Scheduled Jobs ═══ */}
      {activeTab === 'jobs' && (
        <div>
          {jobs.length === 0 ? (
            <div className="card" style={{ textAlign: 'center', padding: '3rem' }}>
              <p style={{ color: 'var(--text-secondary)', fontSize: '1.1rem' }}>
                No jobs scheduled yet
              </p>
              <p
                style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginTop: '0.5rem' }}
              >
                Go to Pipelines tab and click "Run Now" or "Schedule"
              </p>
            </div>
          ) : (
            <>
              <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem' }}>
                <button className="btn btn-secondary btn-sm" onClick={downloadJobsConfig}>
                  Download Jobs Config (JSON)
                </button>
              </div>
              <div className="table-wrapper">
                <table>
                  <thead>
                    <tr>
                      <th>Pipeline</th>
                      <th>Schedule</th>
                      <th>Status</th>
                      <th>Total Runs</th>
                      <th>Last Run</th>
                      <th>Next Run</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {jobs.map((job) => (
                      <tr key={job.id}>
                        <td>
                          <strong>{job.pipelineTitle}</strong>
                          <br />
                          <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                            {job.category}
                          </span>
                        </td>
                        <td>
                          {job.schedule}
                          <br />
                          {job.cron && (
                            <code style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>
                              {job.cron}
                            </code>
                          )}
                        </td>
                        <td>
                          <span className={`badge ${getStatusBadge(job.status)}`}>
                            {job.status}
                          </span>
                        </td>
                        <td>{job.totalRuns}</td>
                        <td>{job.lastRun ? new Date(job.lastRun).toLocaleString() : '\u2014'}</td>
                        <td>{job.nextRun ? new Date(job.nextRun).toLocaleString() : '\u2014'}</td>
                        <td>
                          <div style={{ display: 'flex', gap: '0.25rem' }}>
                            {job.status !== 'running' && (
                              <button
                                className="btn btn-primary btn-sm"
                                onClick={() => runJob(job)}
                              >
                                Run
                              </button>
                            )}
                            {job.status === 'running' && (
                              <button
                                className="btn btn-sm"
                                style={{ background: '#fee2e2', color: '#991b1b' }}
                                onClick={() => cancelJob(job.id)}
                              >
                                Cancel
                              </button>
                            )}
                            <button
                              className="btn btn-sm"
                              style={{ background: '#fee2e2', color: '#991b1b' }}
                              onClick={() => deleteJob(job.id)}
                            >
                              Delete
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      )}

      {/* ═══ TAB: Run History ═══ */}
      {activeTab === 'runs' && (
        <div>
          {jobRuns.length === 0 ? (
            <div className="card" style={{ textAlign: 'center', padding: '3rem' }}>
              <p style={{ color: 'var(--text-secondary)', fontSize: '1.1rem' }}>No runs yet</p>
              <p
                style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginTop: '0.5rem' }}
              >
                Run a pipeline to see execution history
              </p>
            </div>
          ) : (
            <>
              <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem' }}>
                <button className="btn btn-secondary btn-sm" onClick={downloadRunHistory}>
                  Download Run History (CSV)
                </button>
              </div>
              {jobRuns.map((run) => (
                <div key={run.id} className="card" style={{ marginBottom: '0.75rem' }}>
                  <div
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      marginBottom: '0.75rem',
                    }}
                  >
                    <div>
                      <strong>{run.pipelineTitle}</strong>
                      <span
                        className={`badge ${getStatusBadge(run.status)}`}
                        style={{ marginLeft: '0.5rem' }}
                      >
                        {run.status}
                      </span>
                    </div>
                    <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
                      {new Date(run.startTime).toLocaleString()}
                      {run.endTime && ` \u2192 ${new Date(run.endTime).toLocaleTimeString()}`}
                    </span>
                  </div>

                  {/* Stage progress */}
                  <div
                    style={{
                      display: 'flex',
                      gap: '0.25rem',
                      flexWrap: 'wrap',
                      marginBottom: '0.75rem',
                    }}
                  >
                    {run.stages.map((stage, i) => {
                      const colors = {
                        completed: '#dcfce7',
                        running: '#dbeafe',
                        failed: '#fee2e2',
                        pending: '#f3f4f6',
                      };
                      const textColors = {
                        completed: '#166534',
                        running: '#1e40af',
                        failed: '#991b1b',
                        pending: '#6b7280',
                      };
                      return (
                        <React.Fragment key={i}>
                          <span
                            style={{
                              padding: '0.3rem 0.6rem',
                              background: colors[stage.status],
                              color: textColors[stage.status],
                              borderRadius: '4px',
                              fontSize: '0.75rem',
                              fontWeight: stage.status === 'running' ? 600 : 400,
                            }}
                          >
                            {stage.status === 'running' && '\u25B6 '}
                            {stage.status === 'completed' && '\u2713 '}
                            {stage.status === 'failed' && '\u2717 '}
                            {stage.name}
                            {stage.duration && ` (${(stage.duration / 1000).toFixed(1)}s)`}
                          </span>
                          {i < run.stages.length - 1 && (
                            <span
                              style={{ color: '#9ca3af', fontSize: '0.8rem', alignSelf: 'center' }}
                            >
                              \u2192
                            </span>
                          )}
                        </React.Fragment>
                      );
                    })}
                  </div>

                  {/* Progress bar */}
                  <div className="progress-bar" style={{ marginBottom: '0.5rem' }}>
                    <div
                      className={`progress-fill ${run.status === 'failed' ? 'red' : run.status === 'completed' ? 'green' : 'blue'}`}
                      style={{
                        width: `${(run.stages.filter((s) => s.status === 'completed').length / run.totalStages) * 100}%`,
                      }}
                    />
                  </div>

                  {/* Logs */}
                  <details>
                    <summary
                      style={{
                        cursor: 'pointer',
                        fontSize: '0.8rem',
                        color: 'var(--text-secondary)',
                      }}
                    >
                      Execution Logs ({run.logs.length} entries)
                    </summary>
                    <div
                      style={{
                        background: '#1e1e2e',
                        color: '#cdd6f4',
                        padding: '0.75rem',
                        borderRadius: '6px',
                        marginTop: '0.5rem',
                        fontSize: '0.8rem',
                        fontFamily: 'monospace',
                        maxHeight: '200px',
                        overflowY: 'auto',
                      }}
                    >
                      {run.logs.map((log, i) => (
                        <div
                          key={i}
                          style={{
                            color: log.includes('FAILED')
                              ? '#f38ba8'
                              : log.includes('completed') || log.includes('Completed')
                                ? '#a6e3a1'
                                : '#cdd6f4',
                          }}
                        >
                          {log}
                        </div>
                      ))}
                    </div>
                  </details>
                </div>
              ))}
            </>
          )}
        </div>
      )}

      {/* ═══ TAB: Test Reports ═══ */}
      {activeTab === 'tests' && (
        <div>
          {testReports.length === 0 ? (
            <div className="card" style={{ textAlign: 'center', padding: '3rem' }}>
              <p style={{ color: 'var(--text-secondary)', fontSize: '1.1rem' }}>
                No test reports yet
              </p>
              <p
                style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginTop: '0.5rem' }}
              >
                Tests auto-run after each pipeline job completes, or run manually from Run History
              </p>
            </div>
          ) : (
            <>
              <div
                style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem', flexWrap: 'wrap' }}
              >
                <button className="btn btn-secondary btn-sm" onClick={downloadAllTestReports}>
                  Download All Reports (CSV)
                </button>
                <span
                  style={{
                    marginLeft: 'auto',
                    fontSize: '0.85rem',
                    color: 'var(--text-secondary)',
                  }}
                >
                  {testReports.filter((r) => r.status === 'passed').length} passed |{' '}
                  {testReports.filter((r) => r.status === 'failed').length} failed |{' '}
                  {testReports.filter((r) => r.status === 'running').length} running
                </span>
              </div>
              {testReports.map((report) => {
                const isExpanded = expandedReport === report.id;
                return (
                  <div key={report.id} className="card" style={{ marginBottom: '0.75rem' }}>
                    {/* Report Header */}
                    <div
                      style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        cursor: 'pointer',
                      }}
                      onClick={() => setExpandedReport(isExpanded ? null : report.id)}
                    >
                      <div>
                        <strong>{report.pipelineTitle}</strong>
                        <span
                          className={`badge ${report.status === 'passed' ? 'success' : report.status === 'failed' ? 'failed' : report.status === 'warning' ? 'pending' : 'running'}`}
                          style={{ marginLeft: '0.5rem' }}
                        >
                          {report.status}
                        </span>
                        <span
                          className={`badge ${report.trigger === 'auto' ? 'completed' : 'pending'}`}
                          style={{ marginLeft: '0.25rem' }}
                        >
                          {report.trigger === 'auto' ? 'Auto-triggered' : 'Manual'}
                        </span>
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                        {report.summary && (
                          <span style={{ fontSize: '0.85rem', fontWeight: 600 }}>
                            Score:{' '}
                            <span
                              style={{
                                color:
                                  report.summary.score >= 80
                                    ? 'var(--success)'
                                    : report.summary.score >= 60
                                      ? 'var(--warning)'
                                      : 'var(--error)',
                              }}
                            >
                              {report.summary.score}%
                            </span>{' '}
                            ({report.summary.passed}/{report.summary.total} passed)
                          </span>
                        )}
                        <span style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>
                          {new Date(report.startTime).toLocaleString()}
                        </span>
                        <span>{isExpanded ? '\u25BC' : '\u25B6'}</span>
                      </div>
                    </div>

                    {/* Summary Bar */}
                    {report.summary && (
                      <div className="progress-bar" style={{ marginTop: '0.75rem' }}>
                        <div
                          className={`progress-fill ${report.summary.score >= 80 ? 'green' : report.summary.score >= 60 ? 'orange' : 'red'}`}
                          style={{ width: `${report.summary.score}%` }}
                        />
                      </div>
                    )}

                    {/* Expanded: Individual Tests */}
                    {isExpanded && (
                      <div style={{ marginTop: '1rem' }}>
                        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.75rem' }}>
                          <button
                            className="btn btn-secondary btn-sm"
                            onClick={() => downloadTestReport(report)}
                          >
                            Download This Report (CSV)
                          </button>
                          <button
                            className="btn btn-primary btn-sm"
                            onClick={() =>
                              runTestSuite(report.runId, report.pipelineTitle, 'manual')
                            }
                          >
                            Re-run Tests
                          </button>
                        </div>
                        <div className="table-wrapper">
                          <table>
                            <thead>
                              <tr>
                                <th>Test</th>
                                <th>Severity</th>
                                <th>Status</th>
                                <th>Duration</th>
                                <th>Result</th>
                                <th>Details</th>
                              </tr>
                            </thead>
                            <tbody>
                              {report.tests.map((test) => (
                                <tr
                                  key={test.id}
                                  style={{
                                    background:
                                      test.status === 'failed'
                                        ? '#fef2f2'
                                        : test.status === 'warning'
                                          ? '#fffbeb'
                                          : undefined,
                                  }}
                                >
                                  <td>
                                    <strong style={{ fontSize: '0.85rem' }}>{test.name}</strong>
                                    <br />
                                    <span
                                      style={{
                                        fontSize: '0.75rem',
                                        color: 'var(--text-secondary)',
                                      }}
                                    >
                                      {test.desc}
                                    </span>
                                  </td>
                                  <td>
                                    <span
                                      className={`badge ${test.severity === 'critical' ? 'failed' : test.severity === 'high' ? 'pending' : test.severity === 'medium' ? 'completed' : 'success'}`}
                                    >
                                      {test.severity}
                                    </span>
                                  </td>
                                  <td>
                                    <span
                                      className={`badge ${test.status === 'passed' ? 'success' : test.status === 'failed' ? 'failed' : test.status === 'warning' ? 'pending' : test.status === 'running' ? 'running' : 'stopped'}`}
                                    >
                                      {test.status === 'running' ? '\u25B6 running' : test.status}
                                    </span>
                                  </td>
                                  <td>{test.duration ? `${test.duration}ms` : '\u2014'}</td>
                                  <td style={{ fontSize: '0.8rem', maxWidth: '200px' }}>
                                    {test.result || '\u2014'}
                                  </td>
                                  <td
                                    style={{
                                      fontSize: '0.75rem',
                                      fontFamily: 'monospace',
                                      maxWidth: '250px',
                                      overflow: 'hidden',
                                      textOverflow: 'ellipsis',
                                    }}
                                  >
                                    {test.details ? (
                                      <details>
                                        <summary style={{ cursor: 'pointer' }}>
                                          View metrics
                                        </summary>
                                        <pre
                                          style={{
                                            background: '#f5f5f5',
                                            padding: '0.5rem',
                                            borderRadius: '4px',
                                            marginTop: '0.25rem',
                                            fontSize: '0.7rem',
                                            whiteSpace: 'pre-wrap',
                                          }}
                                        >
                                          {JSON.stringify(test.details, null, 2)}
                                        </pre>
                                      </details>
                                    ) : (
                                      '\u2014'
                                    )}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </>
          )}
        </div>
      )}

      {/* ═══ Schedule Modal ═══ */}
      {scheduleModal && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            zIndex: 1000,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
          onClick={() => setScheduleModal(null)}
        >
          <div
            className="card"
            style={{ width: '500px', maxWidth: '90vw' }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ marginBottom: '1rem' }}>
              {scheduleConfig.schedule === 'manual' ? 'Run' : 'Schedule'}: {scheduleModal.title}
            </h3>

            <div className="form-group">
              <label>Schedule</label>
              <select
                className="form-input"
                value={scheduleConfig.schedule}
                onChange={(e) => setScheduleConfig({ ...scheduleConfig, schedule: e.target.value })}
              >
                {SCHEDULE_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                    {opt.cron ? ` (${opt.cron})` : ''}
                  </option>
                ))}
              </select>
            </div>

            <div className="form-group">
              <label>Timeout (minutes)</label>
              <input
                type="number"
                className="form-input"
                value={scheduleConfig.timeout}
                min={1}
                max={1440}
                onChange={(e) =>
                  setScheduleConfig({
                    ...scheduleConfig,
                    timeout: parseInt(e.target.value, 10) || 30,
                  })
                }
              />
            </div>

            <div className="form-group">
              <label>Retries on Failure</label>
              <select
                className="form-input"
                value={scheduleConfig.retries}
                onChange={(e) =>
                  setScheduleConfig({ ...scheduleConfig, retries: parseInt(e.target.value, 10) })
                }
              >
                <option value={0}>No retries</option>
                <option value={1}>1 retry</option>
                <option value={2}>2 retries</option>
                <option value={3}>3 retries</option>
              </select>
            </div>

            <div className="form-group">
              <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <input
                  type="checkbox"
                  checked={scheduleConfig.notify}
                  onChange={(e) =>
                    setScheduleConfig({ ...scheduleConfig, notify: e.target.checked })
                  }
                />
                Email notification on completion/failure
              </label>
            </div>

            <div
              style={{
                display: 'flex',
                gap: '0.5rem',
                justifyContent: 'flex-end',
                marginTop: '1.5rem',
              }}
            >
              <button className="btn btn-secondary" onClick={() => setScheduleModal(null)}>
                Cancel
              </button>
              <button className="btn btn-primary" onClick={() => scheduleJob(scheduleModal)}>
                {scheduleConfig.schedule === 'manual' ? 'Run Now' : 'Schedule Job'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default PipelineBuilder;
