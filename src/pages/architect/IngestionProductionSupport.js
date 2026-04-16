import React from 'react';
import ProductionSupportLibrary from '../../components/architect/ProductionSupportLibrary';

const LEVELS = {
  L1: [
    {
      role: 'Support Analyst',
      issueType: 'Job Failure',
      description: 'Scheduled ingestion job failed',
      scenario: 'Nightly batch job not triggered',
      impact: 'Data delay',
      rootCause: 'Scheduler issue',
      resolution: 'Restart job',
      tools: 'Databricks Jobs UI',
    },
    {
      role: 'Support Analyst',
      issueType: 'Data Delay',
      description: 'Data not arrived on time',
      scenario: 'API ingestion delayed',
      impact: 'SLA breach',
      rootCause: 'Source delay',
      resolution: 'Notify upstream',
      tools: 'Monitoring dashboard',
    },
    {
      role: 'Support Analyst',
      issueType: 'File Missing',
      description: 'Expected file not received',
      scenario: 'Daily CSV missing',
      impact: 'Pipeline blocked',
      rootCause: 'Source system issue',
      resolution: 'Raise incident',
      tools: 'File monitoring',
    },
    {
      role: 'Support Analyst',
      issueType: 'Alert Triggered',
      description: 'Alert on ingestion failure',
      scenario: 'Email alert from pipeline',
      impact: 'Requires investigation',
      rootCause: 'Unknown',
      resolution: 'Escalate to L2',
      tools: 'Alerting system',
    },
    {
      role: 'Support Analyst',
      issueType: 'Basic Validation Failure',
      description: 'Row count mismatch',
      scenario: 'Expected 1M rows, got 500K',
      impact: 'Data incomplete',
      rootCause: 'Partial load',
      resolution: 'Re-run ingestion',
      tools: 'Logs',
    },
  ],
  L2: [
    {
      role: 'Data Engineer',
      issueType: 'Schema Mismatch',
      description: 'Schema changed in source',
      scenario: 'New column added',
      impact: 'Job failure',
      rootCause: 'Rigid schema',
      resolution: 'Enable schema evolution',
      tools: 'Auto Loader',
    },
    {
      role: 'Data Engineer',
      issueType: 'Duplicate Data',
      description: 'Same data loaded twice',
      scenario: 'CDC overlap',
      impact: 'Wrong analytics',
      rootCause: 'No dedup logic',
      resolution: 'Deduplicate using keys',
      tools: 'Delta Lake',
    },
    {
      role: 'Data Engineer',
      issueType: 'API Failure',
      description: 'API timeout / error',
      scenario: 'CRM API fails',
      impact: 'Data gap',
      rootCause: 'Rate limit / timeout',
      resolution: 'Retry with backoff',
      tools: 'API logs',
    },
    {
      role: 'Data Engineer',
      issueType: 'Data Corruption',
      description: 'Invalid records ingested',
      scenario: 'Null/invalid values',
      impact: 'Poor data quality',
      rootCause: 'No validation',
      resolution: 'Apply validation rules',
      tools: 'Great Expectations',
    },
    {
      role: 'Data Engineer',
      issueType: 'Partition Issue',
      description: 'Data skew / bad partition',
      scenario: 'One partition overloaded',
      impact: 'Slow job',
      rootCause: 'Poor partitioning',
      resolution: 'Repartition data',
      tools: 'Spark UI',
    },
  ],
  L3: [
    {
      role: 'Senior Engineer',
      issueType: 'Streaming Failure',
      description: 'Streaming job crash',
      scenario: 'Kafka offset issue',
      impact: 'Data loss risk',
      rootCause: 'No checkpointing',
      resolution: 'Implement checkpointing',
      tools: 'Structured Streaming',
    },
    {
      role: 'Senior Engineer',
      issueType: 'CDC Inconsistency',
      description: 'Updates not captured',
      scenario: 'Missing DB updates',
      impact: 'Data mismatch',
      rootCause: 'Incorrect CDC config',
      resolution: 'Fix CDC logic',
      tools: 'Debezium',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Performance Issue',
      description: 'Job takes too long',
      scenario: '3hr ingestion job',
      impact: 'SLA breach',
      rootCause: 'Poor optimization',
      resolution: 'Optimize Spark job',
      tools: 'Spark UI',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Small File Problem',
      description: 'Millions of small files',
      scenario: 'Slow reads',
      impact: 'Performance issue',
      rootCause: 'No compaction',
      resolution: 'Optimize files',
      tools: 'Delta OPTIMIZE',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Retry Failure',
      description: 'Retry not working',
      scenario: 'Job fails permanently',
      impact: 'Pipeline stop',
      rootCause: 'No retry logic',
      resolution: 'Implement retry framework',
      tools: 'Workflows',
    },
  ],
  L4: [
    {
      role: 'Architect',
      issueType: 'Design Flaw',
      description: 'Wrong ingestion architecture',
      scenario: 'Batch used instead of streaming',
      impact: 'High latency',
      rootCause: 'Poor design choice',
      resolution: 'Redesign ingestion pattern',
      tools: 'Architecture review',
    },
    {
      role: 'Architect',
      issueType: 'Scalability Issue',
      description: 'Cannot handle high data volume',
      scenario: '10x data spike fails system',
      impact: 'Downtime',
      rootCause: 'No autoscaling',
      resolution: 'Implement autoscaling',
      tools: 'Databricks cluster',
    },
    {
      role: 'Architect',
      issueType: 'Multi-source Conflict',
      description: 'Data inconsistency across sources',
      scenario: 'ERP vs CRM mismatch',
      impact: 'Wrong reporting',
      rootCause: 'No canonical model',
      resolution: 'Define canonical ingestion',
      tools: 'Data model',
    },
    {
      role: 'Architect',
      issueType: 'Governance Gap',
      description: 'No control on ingestion',
      scenario: 'Sensitive data ingested raw',
      impact: 'Compliance risk',
      rootCause: 'No governance layer',
      resolution: 'Add governance controls',
      tools: 'Unity Catalog',
    },
    {
      role: 'Architect',
      issueType: 'Cost Explosion',
      description: 'High ingestion cost',
      scenario: 'Continuous jobs running',
      impact: 'Budget issue',
      rootCause: 'Inefficient design',
      resolution: 'Optimize ingestion strategy',
      tools: 'FinOps tools',
    },
  ],
};

const FLOW = `Issue Detected (L1)
  ↓
Basic Restart / Validation
  ↓
Escalate to L2 (Pipeline Debug)
  ↓
Escalate to L3 (Code / Logic Fix)
  ↓
Escalate to L4 (Architecture / Design Fix)`;

export default function IngestionProductionSupport() {
  return (
    <ProductionSupportLibrary
      pageTitle="Data Ingestion — Production Support (L1 → L4)"
      pageSubtitle="L1-L4 support playbook for batch, streaming, CDC, Auto Loader, Kafka, APIs, and Bronze layer failures."
      coverage="Batch / Streaming / CDC · Databricks Auto Loader / Kafka / APIs · Bronze layer failures"
      levels={LEVELS}
      csvName="ingestion-production-support.csv"
      flow={FLOW}
    />
  );
}
