import React from 'react';
import ProductionSupportLibrary from '../../components/architect/ProductionSupportLibrary';

const LEVELS = {
  L1: [
    {
      role: 'Support Analyst',
      issueType: 'Alert Triggered',
      description: 'Monitoring alert received',
      scenario: 'Job failure alert',
      impact: 'Needs action',
      rootCause: 'Unknown',
      resolution: 'Acknowledge + escalate',
      tools: 'Splunk / Datadog',
    },
    {
      role: 'Support Analyst',
      issueType: 'Job Failure Alert',
      description: 'Pipeline failure detected',
      scenario: 'ETL job failed overnight',
      impact: 'Data delay',
      rootCause: 'Unknown',
      resolution: 'Restart job',
      tools: 'Databricks UI',
    },
    {
      role: 'Support Analyst',
      issueType: 'High Latency Alert',
      description: 'Query running slow',
      scenario: 'Dashboard delay alert',
      impact: 'Poor UX',
      rootCause: 'Unknown',
      resolution: 'Escalate to L2',
      tools: 'Monitoring tools',
    },
    {
      role: 'Support Analyst',
      issueType: 'Resource Alert',
      description: 'High CPU/memory usage',
      scenario: 'Cluster overload',
      impact: 'Performance issue',
      rootCause: 'Unknown',
      resolution: 'Scale cluster',
      tools: 'Metrics dashboard',
    },
    {
      role: 'Support Analyst',
      issueType: 'Log Error',
      description: 'Error in logs',
      scenario: 'Exception in pipeline',
      impact: 'Failure risk',
      rootCause: 'Unknown',
      resolution: 'Escalate',
      tools: 'Log viewer',
    },
  ],
  L2: [
    {
      role: 'Data/Platform Engineer',
      issueType: 'Log Analysis',
      description: 'Analyze error logs',
      scenario: 'Null pointer in job',
      impact: 'Job failure',
      rootCause: 'Code/data issue',
      resolution: 'Fix logic',
      tools: 'Logs',
    },
    {
      role: 'Data/Platform Engineer',
      issueType: 'Metric Analysis',
      description: 'Analyze CPU/memory',
      scenario: 'High memory usage',
      impact: 'Slow jobs',
      rootCause: 'Data skew',
      resolution: 'Optimize job',
      tools: 'Metrics',
    },
    {
      role: 'Data/Platform Engineer',
      issueType: 'Trace Analysis',
      description: 'Track request flow',
      scenario: 'API → DB delay',
      impact: 'Latency',
      rootCause: 'Bottleneck',
      resolution: 'Optimize component',
      tools: 'OpenTelemetry',
    },
    {
      role: 'Data/Platform Engineer',
      issueType: 'Alert Noise',
      description: 'Too many alerts',
      scenario: 'Alert fatigue',
      impact: 'Ignored alerts',
      rootCause: 'Poor config',
      resolution: 'Tune alerts',
      tools: 'AIOps',
    },
    {
      role: 'Data/Platform Engineer',
      issueType: 'Dependency Issue',
      description: 'Downstream system failure',
      scenario: 'API not responding',
      impact: 'Pipeline failure',
      rootCause: 'External dependency',
      resolution: 'Retry / fallback',
      tools: 'Monitoring',
    },
  ],
  L3: [
    {
      role: 'Senior Engineer',
      issueType: 'No Traceability',
      description: 'Cannot trace pipeline',
      scenario: 'Hard to debug',
      impact: 'Delay',
      rootCause: 'No tracing',
      resolution: 'Implement tracing',
      tools: 'OpenTelemetry',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Poor Logging',
      description: 'Insufficient logs',
      scenario: 'Cannot identify issue',
      impact: 'Delay',
      rootCause: 'Weak logging',
      resolution: 'Improve logging',
      tools: 'Logging framework',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Metric Gap',
      description: 'Missing metrics',
      scenario: 'No performance visibility',
      impact: 'Blind spots',
      rootCause: 'No instrumentation',
      resolution: 'Add metrics',
      tools: 'Monitoring tools',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Alert Misconfiguration',
      description: 'Critical alerts missed',
      scenario: 'System failure unnoticed',
      impact: 'Risk',
      rootCause: 'Poor alert design',
      resolution: 'Redesign alerts',
      tools: 'AIOps',
    },
    {
      role: 'Senior Engineer',
      issueType: 'Incident RCA Gap',
      description: 'No proper RCA',
      scenario: 'Repeated issues',
      impact: 'Recurrence',
      rootCause: 'No RCA process',
      resolution: 'Implement RCA',
      tools: 'Incident mgmt',
    },
  ],
  L4: [
    {
      role: 'Architect',
      issueType: 'No Observability Strategy',
      description: 'No unified monitoring',
      scenario: 'Each team uses different tools',
      impact: 'Chaos',
      rootCause: 'No standard',
      resolution: 'Unified observability',
      tools: 'Splunk',
    },
    {
      role: 'Architect',
      issueType: 'No AIOps',
      description: 'Manual incident handling',
      scenario: 'Slow response',
      impact: 'Downtime',
      rootCause: 'No automation',
      resolution: 'Implement AIOps',
      tools: 'ML-based tools',
    },
    {
      role: 'Architect',
      issueType: 'No End-to-End Tracing',
      description: 'Cannot track full flow',
      scenario: 'Multi-system debugging failure',
      impact: 'Delay',
      rootCause: 'No tracing',
      resolution: 'Distributed tracing',
      tools: 'OpenTelemetry',
    },
    {
      role: 'Architect',
      issueType: 'No SLA Monitoring',
      description: 'SLA not tracked',
      scenario: 'Missed business targets',
      impact: 'Risk',
      rootCause: 'No SLA framework',
      resolution: 'SLA dashboards',
      tools: 'Monitoring',
    },
    {
      role: 'Architect',
      issueType: 'No Incident Management Model',
      description: 'No structured response',
      scenario: 'Slow resolution',
      impact: 'Downtime',
      rootCause: 'No process',
      resolution: 'Incident framework',
      tools: 'ITSM',
    },
  ],
};

const FLOW = `Alert / Log Trigger (L1)
  ↓
RCA Investigation (L2)
  ↓
Instrumentation / Monitoring Fix (L3)
  ↓
Observability Architecture & AIOps Strategy (L4)`;

export default function ObservabilityProductionSupport() {
  return (
    <ProductionSupportLibrary
      pageTitle="Observability / AIOps — Production Support (L1 → L4)"
      pageSubtitle="L1-L4 support playbook for logs, metrics, traces (OpenTelemetry), Databricks + Spark monitoring, AIOps (Splunk / Datadog / Azure Monitor), incident detection, RCA, automation."
      coverage="Logs, metrics, traces (OpenTelemetry) · Databricks + Spark monitoring · AIOps · Incident detection, RCA, automation"
      levels={LEVELS}
      csvName="observability-production-support.csv"
      flow={FLOW}
    />
  );
}
