import React from 'react';
import ProductionSupportLibrary from '../../components/architect/ProductionSupportLibrary';

const LEVELS = {
  L1: [
    {
      role: 'Support Analyst',
      issueType: 'Pipeline Failure',
      description: 'Silver/Gold job failed',
      scenario: 'Daily transformation job failed',
      impact: 'Data delay',
      rootCause: 'Upstream failure',
      resolution: 'Restart job',
      tools: 'Databricks Jobs',
    },
    {
      role: 'Support Analyst',
      issueType: 'Data Missing',
      description: 'Gold table empty',
      scenario: 'Dashboard shows no data',
      impact: 'Business disruption',
      rootCause: 'Pipeline not run',
      resolution: 'Trigger pipeline',
      tools: 'Monitoring dashboard',
    },
    {
      role: 'Support Analyst',
      issueType: 'Data Delay',
      description: 'KPI tables delayed',
      scenario: 'Morning reports not ready',
      impact: 'SLA breach',
      rootCause: 'Long-running job',
      resolution: 'Notify + escalate',
      tools: 'Alerts',
    },
    {
      role: 'Support Analyst',
      issueType: 'Basic Data Mismatch',
      description: 'Row count mismatch',
      scenario: 'Silver vs Bronze mismatch',
      impact: 'Trust issue',
      rootCause: 'Partial load',
      resolution: 'Re-run job',
      tools: 'Logs',
    },
    {
      role: 'Support Analyst',
      issueType: 'Alert Triggered',
      description: 'Data quality alert',
      scenario: 'Null spike detected',
      impact: 'Data issue',
      rootCause: 'Unknown',
      resolution: 'Escalate to L2',
      tools: 'Monitoring tools',
    },
  ],
  L2: [
    {
      role: 'Data Engineer',
      issueType: 'Join Failure',
      description: 'Join producing incorrect results',
      scenario: 'Missing customer records',
      impact: 'Wrong analytics',
      rootCause: 'Key mismatch',
      resolution: 'Fix join logic',
      tools: 'Spark SQL',
    },
    {
      role: 'Data Engineer',
      issueType: 'Duplicate Records',
      description: 'Duplicate rows in silver/gold',
      scenario: 'Same transaction repeated',
      impact: 'Incorrect KPIs',
      rootCause: 'No dedup logic',
      resolution: 'Apply deduplication',
      tools: 'Delta Lake',
    },
    {
      role: 'Data Engineer',
      issueType: 'Data Quality Issue',
      description: 'Null/invalid values',
      scenario: 'Missing price field',
      impact: 'Incorrect reports',
      rootCause: 'No validation',
      resolution: 'Add validation rules',
      tools: 'Great Expectations',
    },
    {
      role: 'Data Engineer',
      issueType: 'Schema Issue',
      description: 'Column mismatch',
      scenario: 'Field type changed',
      impact: 'Pipeline failure',
      rootCause: 'Schema drift',
      resolution: 'Update schema',
      tools: 'Delta',
    },
    {
      role: 'Data Engineer',
      issueType: 'Aggregation Error',
      description: 'Wrong totals',
      scenario: 'Revenue mismatch',
      impact: 'Business confusion',
      rootCause: 'Incorrect logic',
      resolution: 'Fix aggregation',
      tools: 'Spark SQL',
    },
  ],
  L3: [
    {
      role: 'Senior Engineer',
      issueType: 'SCD Failure',
      description: 'History not maintained',
      scenario: 'Customer changes overwritten',
      impact: 'Loss of history',
      rootCause: 'No SCD logic',
      resolution: 'Implement SCD Type 2',
      tools: 'Delta MERGE',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Performance Issue',
      description: 'Slow transformations',
      scenario: 'Gold build takes hours',
      impact: 'SLA breach',
      rootCause: 'Large joins',
      resolution: 'Optimize joins, caching',
      tools: 'Spark UI',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Data Skew',
      description: 'Uneven partition processing',
      scenario: 'One task takes too long',
      impact: 'Job delay',
      rootCause: 'Poor partitioning',
      resolution: 'Repartition data',
      tools: 'Spark',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Incremental Load Failure',
      description: 'Full load instead of incremental',
      scenario: 'Huge data reload',
      impact: 'Cost + delay',
      rootCause: 'Missing logic',
      resolution: 'Implement incremental load',
      tools: 'Delta',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Business Logic Bug',
      description: 'KPI calculated incorrectly',
      scenario: 'Profit margin wrong',
      impact: 'Wrong decisions',
      rootCause: 'Logic error',
      resolution: 'Fix business logic',
      tools: 'dbt/Spark',
    },
  ],
  L4: [
    {
      role: 'Architect',
      issueType: 'Wrong Data Model',
      description: 'Poor schema design',
      scenario: 'Too many joins in BI',
      impact: 'Slow dashboards',
      rootCause: 'No star schema',
      resolution: 'Redesign model',
      tools: 'Data modeling',
    },
    {
      role: 'Architect',
      issueType: 'No Semantic Layer',
      description: 'Business logic scattered',
      scenario: 'KPI mismatch across teams',
      impact: 'Confusion',
      rootCause: 'No central logic',
      resolution: 'Build semantic layer',
      tools: 'dbt',
    },
    {
      role: 'Architect',
      issueType: 'Data Granularity Issue',
      description: 'Wrong level of detail',
      scenario: 'Cannot drill down',
      impact: 'Limited analysis',
      rootCause: 'Poor requirement design',
      resolution: 'Define grain properly',
      tools: 'Modeling',
    },
    {
      role: 'Architect',
      issueType: 'Multi-source Conflict',
      description: 'ERP vs CRM mismatch',
      scenario: 'Inconsistent reporting',
      impact: 'Trust loss',
      rootCause: 'No canonical model',
      resolution: 'Standardize model',
      tools: 'ETL',
    },
    {
      role: 'Architect',
      issueType: 'Scalability Issue',
      description: 'Model fails at scale',
      scenario: 'Large joins crash system',
      impact: 'Downtime',
      rootCause: 'Poor architecture',
      resolution: 'Distributed design',
      tools: 'Spark',
    },
  ],
};

const FLOW = `Issue Detected (L1)
  ↓
Data Investigation (L2)
  ↓
Logic / Performance Fix (L3)
  ↓
Model Redesign (L4)`;

export default function ModelingProductionSupport() {
  return (
    <ProductionSupportLibrary
      pageTitle="Data Modeling — Production Support (L1 → L4)"
      pageSubtitle="L1-L4 support playbook for Silver + Gold layers, transformations, joins, SCD, KPIs — dbt / Spark / Delta Lake."
      coverage="Silver + Gold layers · Transformations, joins, SCD, KPIs · dbt / Spark / Delta Lake"
      levels={LEVELS}
      csvName="modeling-production-support.csv"
      flow={FLOW}
    />
  );
}
