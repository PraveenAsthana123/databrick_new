import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

const supportLevels = [
  {
    level: 'L1',
    name: 'Operations / Support',
    role: 'On-call Operator / Support Analyst',
    focus: 'Monitoring, triage, escalation',
    color: '#3b82f6',
    sla: 'Minutes (5–15 min response)',
    ownership: 'NOC / Operations Team',
    activities: [
      'Monitor dashboards and alert consoles',
      'Acknowledge and log all triggered alerts',
      'Perform basic job restart / retry actions',
      'Validate data freshness and file arrival',
      'Escalate unresolved issues to L2 with incident log',
    ],
    commonIssues: [
      {
        issue: 'Job failed',
        action: 'Retry job. If fails again, log and escalate to L2.',
      },
      {
        issue: 'Data delay detected',
        action: 'Check SLA dashboard. Log delay. Notify downstream teams. Escalate.',
      },
      {
        issue: 'Expected file not arrived',
        action: 'Validate source system status. Log missing file incident. Escalate.',
      },
      {
        issue: 'Alert triggered in monitoring system',
        action: 'Acknowledge alert. Log incident in JIRA/ServiceNow. Escalate if unresolved.',
      },
    ],
    tools: [
      'Airflow UI / Databricks Jobs UI',
      'Monitoring dashboards (Grafana / CloudWatch)',
      'JIRA / ServiceNow',
    ],
    interviewAnswer:
      'L1 handles monitoring, basic triage, and ensures incidents are logged and escalated properly. They are the first line of defense — their job is fast detection and clean escalation, not deep debugging.',
  },
  {
    level: 'L2',
    name: 'Data Engineer',
    role: 'Data Engineer / Pipeline Developer',
    focus: 'Pipeline debugging, data issue resolution, config fixes',
    color: '#f59e0b',
    sla: 'Hours (1–4 hr resolution target)',
    ownership: 'Data Engineering Team',
    activities: [
      'Investigate pipeline logs and Spark execution details',
      'Analyze input and output data for anomalies',
      'Fix pipeline configurations and parameter issues',
      'Apply minor code patches and deploy hotfixes',
      'Rerun affected pipeline stages after fix',
    ],
    commonIssues: [
      {
        issue: 'Schema mismatch causing pipeline failure',
        rootCause: 'Source schema changed unexpectedly',
        fix: 'Adjust schema mapping or add schema evolution handling. Rerun pipeline.',
      },
      {
        issue: 'Data quality rule failure',
        rootCause: 'Upstream data contains nulls / invalid values',
        fix: 'Apply filter or cleansing logic. Add DQ rule to catch at ingestion. Rerun.',
      },
      {
        issue: 'Join producing wrong results',
        rootCause: 'Incorrect join key or duplicates in source',
        fix: 'Fix join logic. Deduplicate source. Validate output row counts.',
      },
      {
        issue: 'API extraction failure',
        rootCause: 'Downstream API rate limit or timeout',
        fix: 'Add retry with exponential backoff. Implement batch window for extraction.',
      },
    ],
    tools: [
      'Spark logs / Databricks cluster logs',
      'SQL validation queries',
      'Data quality scripts',
      'Git / CI pipeline',
    ],
    interviewAnswer:
      'L2 handles debugging pipelines, fixing data issues, and ensuring successful reruns. They own the pipeline code and are responsible for resolving issues that cannot be handled by basic restart.',
  },
  {
    level: 'L3',
    name: 'Senior Engineer / Architect',
    role: 'Senior Data Engineer / Data Architect',
    focus: 'Root cause analysis, performance tuning, permanent fixes, automation',
    color: '#8b5cf6',
    sla: '1–2 business days',
    ownership: 'Architecture / Senior Engineering',
    activities: [
      'Perform deep root cause analysis (RCA) for recurring failures',
      'Redesign pipeline logic or data model to eliminate root causes',
      'Optimize Spark jobs for performance and cost',
      'Introduce automation to prevent manual intervention',
      'Define prevention patterns and update runbooks',
    ],
    commonIssues: [
      {
        issue: 'Data duplication in output tables',
        rootCause: 'Missing or incorrect merge/dedup logic in Delta writes',
        fix: 'Redesign MERGE logic. Add deduplication step. Validate idempotency.',
      },
      {
        issue: 'Severe query performance degradation',
        rootCause: 'Missing partitions, skewed data, or cartesian joins',
        fix: 'Optimize partition strategy. Add Z-ordering. Fix join logic. Run ANALYZE.',
      },
      {
        issue: 'Late-arriving data causing incorrect aggregations',
        rootCause: 'No watermark or event-time handling in streaming pipeline',
        fix: 'Add watermark configuration. Switch to event-time windowing.',
      },
      {
        issue: 'Pipeline instability — frequent random failures',
        rootCause: 'Fragile DAG design with no retry or dependency management',
        fix: 'Refactor DAG. Add proper retry policies. Introduce dependency checks.',
      },
    ],
    tools: [
      'Spark UI / Query plan analyzer',
      'Delta table history / audit logs',
      'Pipeline orchestration (Airflow / Databricks Workflows)',
      'Profiling and monitoring tools',
    ],
    interviewAnswer:
      'L3 performs root cause analysis and implements permanent fixes to prevent recurrence. They identify systemic weaknesses and redesign pipelines, data models, or orchestration logic to eliminate recurring failures.',
  },
  {
    level: 'L4',
    name: 'Platform / Vendor / SME',
    role: 'Platform Engineer / Cloud Architect / Vendor Support',
    focus: 'Platform-level failures, infrastructure issues, vendor escalations',
    color: '#ef4444',
    sla: 'Depends on vendor SLA (hours to days)',
    ownership: 'Platform / Infrastructure / Vendor',
    activities: [
      'Diagnose cluster-level or infrastructure failures',
      'Apply patches or version upgrades to platform components',
      'Engage cloud provider or vendor support channels',
      'Resolve storage system or networking failures',
      'Provide platform-level guidance and preventive hardening',
    ],
    commonIssues: [
      {
        issue: 'Cluster failure — all jobs failing across the platform',
        rootCause: 'Underlying cloud infrastructure or Databricks Runtime issue',
        fix: 'Engage Databricks/cloud provider support. Apply platform-level fix or migrate workload.',
      },
      {
        issue: 'Known Spark bug causing silent data corruption',
        rootCause: 'Regression in Spark or DBR version',
        fix: 'Patch or upgrade Databricks Runtime. Test in lower environments first.',
      },
      {
        issue: 'Storage system failure (S3 / ADLS / GCS)',
        rootCause: 'Cloud provider storage incident or misconfigured IAM/network',
        fix: 'Engage cloud provider infrastructure support. Review IAM and VPC config.',
      },
      {
        issue: 'External API or third-party service completely down',
        rootCause: 'Vendor outage or expired credentials',
        fix: 'Escalate to vendor. Implement circuit breaker. Notify stakeholders of delay.',
      },
    ],
    tools: [
      'Cloud provider support portal (AWS / Azure / GCP)',
      'Vendor support tickets (Databricks Support)',
      'Infrastructure logs / CloudTrail / Azure Monitor',
      'Network and IAM diagnostic tools',
    ],
    interviewAnswer:
      'L4 handles platform-level issues and works with vendors or cloud providers to resolve infrastructure failures. These are issues that cannot be fixed within the application layer — they require platform engineering or external vendor intervention.',
  },
];

const e2eFlow = [
  {
    step: 1,
    actor: 'Monitoring System',
    action: 'Alert fires — job failure, data delay, DQ breach',
    level: null,
  },
  {
    step: 2,
    actor: 'L1 Operations',
    action: 'Acknowledge alert. Log incident. Attempt basic restart.',
    level: 'L1',
  },
  {
    step: 3,
    actor: 'L1 → L2',
    action: 'Escalate with incident log if restart fails or issue is complex.',
    level: 'L1',
  },
  {
    step: 4,
    actor: 'L2 Data Engineer',
    action: 'Investigate logs. Fix config/code. Rerun pipeline.',
    level: 'L2',
  },
  {
    step: 5,
    actor: 'L2 → L3',
    action: 'Escalate if recurring pattern, performance issue, or RCA needed.',
    level: 'L2',
  },
  {
    step: 6,
    actor: 'L3 Senior/Architect',
    action: 'Root cause analysis. Permanent fix. Runbook update.',
    level: 'L3',
  },
  {
    step: 7,
    actor: 'L3 → L4',
    action: 'Escalate if platform, infrastructure, or vendor issue identified.',
    level: 'L3',
  },
  {
    step: 8,
    actor: 'L4 Platform/Vendor',
    action: 'Infrastructure fix, patch, or vendor ticket resolution.',
    level: 'L4',
  },
  {
    step: 9,
    actor: 'All Levels',
    action: 'Incident closed. Post-mortem written. Prevention action logged.',
    level: null,
  },
];

const exampleScenario = {
  title: 'Example: Data Missing in Dashboard',
  steps: [
    {
      level: 'L1',
      action:
        'Dashboard shows stale data. L1 checks pipeline status — job failed. Logs incident, retries. Fails again. Escalates to L2.',
    },
    {
      level: 'L2',
      action:
        'L2 checks Spark logs — schema mismatch in source file. Adjusts schema mapping. Reruns pipeline. Data refreshes. Escalates RCA request to L3.',
    },
    {
      level: 'L3',
      action:
        'L3 finds source team changed schema without notification. Adds schema evolution handling and alerting on schema drift. Updates runbook.',
    },
    { level: 'L4', action: 'Not required for this incident — resolved at L3.' },
  ],
};

const slaTable = [
  {
    level: 'L1',
    role: 'Ops/Support',
    responseTime: '5–15 minutes',
    resolutionTarget: 'Escalate within 30 min',
    owner: 'NOC / Operations',
  },
  {
    level: 'L2',
    role: 'Data Engineer',
    responseTime: '15–30 minutes',
    resolutionTarget: '1–4 hours',
    owner: 'Data Engineering',
  },
  {
    level: 'L3',
    role: 'Senior/Architect',
    responseTime: '1–2 hours',
    resolutionTarget: '1–2 business days',
    owner: 'Architecture Team',
  },
  {
    level: 'L4',
    role: 'Platform/Vendor',
    responseTime: 'Per vendor SLA',
    resolutionTarget: 'Per vendor SLA',
    owner: 'Platform / Vendor',
  },
];

const preventionBestPractices = [
  'Define alert thresholds and SLA breaches upfront — do not wait for failures to define them',
  'Every pipeline must have retry logic, timeout, and dead-letter handling',
  'Runbooks must be maintained for every known failure pattern',
  'Schema evolution must be handled explicitly — never assume source schema is stable',
  'Post-mortems are mandatory for P1/P2 incidents — document root cause and prevention',
  'L1 must have clear escalation criteria — do not leave judgment to individual operators',
  'Monitor data freshness, not just pipeline success/failure status',
  'Use circuit breakers for all external API dependencies',
];

const levelColors = { L1: '#3b82f6', L2: '#f59e0b', L3: '#8b5cf6', L4: '#ef4444' };

function ProductionSupport() {
  const [expandedLevel, setExpandedLevel] = useState(null);
  const [expandedIssue, setExpandedIssue] = useState({});

  const toggleIssue = (levelId, issueIdx) => {
    const key = `${levelId}-${issueIdx}`;
    setExpandedIssue((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const downloadCSV = () => {
    const rows = supportLevels.flatMap((l) =>
      l.commonIssues.map((issue) => ({
        level: l.level,
        name: l.name,
        role: l.role,
        sla: l.sla,
        ownership: l.ownership,
        issue: issue.issue,
        rootCauseOrAction: issue.rootCause || issue.action || '',
        fix: issue.fix || issue.action || '',
        interviewAnswer: l.interviewAnswer,
      }))
    );
    exportToCSV(rows, 'production-support-model.csv');
  };

  return (
    <div>
      {/* Page Header */}
      <div className="page-header">
        <div>
          <h1>Production Support Model (L1&ndash;L4)</h1>
          <p>
            4-level support model &mdash; Triage, Debug, RCA, Platform &mdash; with SLAs and
            interview answers
          </p>
        </div>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F6E1;</div>
          <div className="stat-info">
            <h4>4</h4>
            <p>Support Levels</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#x26A0;</div>
          <div className="stat-info">
            <h4>16</h4>
            <p>Common Issues</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#x1F4CB;</div>
          <div className="stat-info">
            <h4>9</h4>
            <p>E2E Flow Steps</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#x1F3AF;</div>
          <div className="stat-info">
            <h4>4</h4>
            <p>Interview Answers</p>
          </div>
        </div>
      </div>

      {/* Download CSV */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <p style={{ margin: 0, color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
            Click any level to expand details, activities, and common issues.
          </p>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadCSV}
            style={{ marginLeft: 'auto' }}
          >
            Download CSV
          </button>
        </div>
      </div>

      {/* Support Level Cards */}
      {supportLevels.map((lvl) => {
        const isExpanded = expandedLevel === lvl.level;
        const color = lvl.color;
        return (
          <div
            key={lvl.level}
            className="card"
            style={{ marginBottom: '1rem', borderLeft: `5px solid ${color}` }}
          >
            {/* Header */}
            <div
              onClick={() => setExpandedLevel(isExpanded ? null : lvl.level)}
              style={{
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'flex-start',
                gap: '1rem',
              }}
            >
              <div style={{ flex: 1 }}>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.75rem',
                    flexWrap: 'wrap',
                    marginBottom: '0.3rem',
                  }}
                >
                  <div
                    style={{
                      background: color,
                      color: '#fff',
                      fontWeight: 800,
                      fontSize: '0.85rem',
                      padding: '3px 10px',
                      borderRadius: '6px',
                      letterSpacing: '0.05em',
                    }}
                  >
                    {lvl.level}
                  </div>
                  <strong style={{ fontSize: '1rem', color: 'var(--text-primary)' }}>
                    {lvl.name}
                  </strong>
                  <span
                    style={{
                      fontSize: '0.78rem',
                      color: 'var(--text-secondary)',
                      fontStyle: 'italic',
                    }}
                  >
                    {lvl.role}
                  </span>
                </div>
                <div style={{ display: 'flex', gap: '1.5rem', flexWrap: 'wrap' }}>
                  <span style={{ fontSize: '0.82rem', color: 'var(--text-secondary)' }}>
                    <strong style={{ color: color }}>Focus:</strong> {lvl.focus}
                  </span>
                  <span style={{ fontSize: '0.82rem', color: 'var(--text-secondary)' }}>
                    <strong style={{ color: color }}>SLA:</strong> {lvl.sla}
                  </span>
                </div>
              </div>
              <span style={{ color: 'var(--text-secondary)', flexShrink: 0, marginTop: '4px' }}>
                {isExpanded ? '\u25BC' : '\u25B6'}
              </span>
            </div>

            {isExpanded && (
              <div style={{ marginTop: '1.5rem' }}>
                {/* Activities + Tools */}
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr 1fr',
                    gap: '1rem',
                    marginBottom: '1.25rem',
                  }}
                >
                  {/* Activities */}
                  <div
                    style={{
                      background: `${color}08`,
                      border: `1px solid ${color}30`,
                      borderRadius: '10px',
                      padding: '1rem',
                    }}
                  >
                    <div
                      style={{
                        fontSize: '0.7rem',
                        fontWeight: 700,
                        textTransform: 'uppercase',
                        letterSpacing: '0.05em',
                        color: color,
                        marginBottom: '0.6rem',
                      }}
                    >
                      Activities
                    </div>
                    <ul style={{ margin: 0, paddingLeft: '1.2rem', listStyle: 'disc' }}>
                      {lvl.activities.map((act, i) => (
                        <li
                          key={i}
                          style={{
                            fontSize: '0.85rem',
                            color: 'var(--text-primary)',
                            marginBottom: '0.3rem',
                            lineHeight: 1.45,
                          }}
                        >
                          {act}
                        </li>
                      ))}
                    </ul>
                  </div>

                  {/* Tools + Ownership */}
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                    <div
                      style={{
                        background: 'linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%)',
                        border: '1px solid #bfdbfe',
                        borderRadius: '10px',
                        padding: '1rem',
                      }}
                    >
                      <div
                        style={{
                          fontSize: '0.7rem',
                          fontWeight: 700,
                          textTransform: 'uppercase',
                          letterSpacing: '0.05em',
                          color: '#1d4ed8',
                          marginBottom: '0.5rem',
                        }}
                      >
                        Tools
                      </div>
                      <ul style={{ margin: 0, paddingLeft: '1.2rem', listStyle: 'disc' }}>
                        {lvl.tools.map((t, i) => (
                          <li
                            key={i}
                            style={{
                              fontSize: '0.82rem',
                              color: '#1e3a5f',
                              marginBottom: '0.2rem',
                              fontFamily: 'monospace',
                            }}
                          >
                            {t}
                          </li>
                        ))}
                      </ul>
                    </div>
                    <div
                      style={{
                        background: 'linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%)',
                        border: '1px solid #bbf7d0',
                        borderRadius: '10px',
                        padding: '0.85rem 1rem',
                      }}
                    >
                      <div
                        style={{
                          fontSize: '0.7rem',
                          fontWeight: 700,
                          textTransform: 'uppercase',
                          letterSpacing: '0.05em',
                          color: '#16a34a',
                          marginBottom: '0.3rem',
                        }}
                      >
                        Ownership
                      </div>
                      <p
                        style={{
                          margin: 0,
                          fontSize: '0.85rem',
                          color: '#14532d',
                          fontWeight: 600,
                        }}
                      >
                        {lvl.ownership}
                      </p>
                    </div>
                  </div>
                </div>

                {/* Common Issues */}
                <div style={{ marginBottom: '1.25rem' }}>
                  <div
                    style={{
                      fontSize: '0.75rem',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.05em',
                      color: 'var(--text-secondary)',
                      marginBottom: '0.6rem',
                    }}
                  >
                    Common Issues
                  </div>
                  {lvl.commonIssues.map((issue, idx) => {
                    const key = `${lvl.level}-${idx}`;
                    const open = expandedIssue[key];
                    return (
                      <div
                        key={idx}
                        style={{
                          background: 'var(--bg-secondary)',
                          border: '1px solid var(--border)',
                          borderRadius: '8px',
                          marginBottom: '0.5rem',
                          overflow: 'hidden',
                        }}
                      >
                        <div
                          onClick={() => toggleIssue(lvl.level, idx)}
                          style={{
                            cursor: 'pointer',
                            display: 'flex',
                            justifyContent: 'space-between',
                            alignItems: 'center',
                            padding: '0.6rem 0.85rem',
                            gap: '0.5rem',
                          }}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                            <span
                              style={{
                                width: '6px',
                                height: '6px',
                                borderRadius: '50%',
                                background: color,
                                flexShrink: 0,
                              }}
                            />
                            <span
                              style={{
                                fontSize: '0.875rem',
                                fontWeight: 600,
                                color: 'var(--text-primary)',
                              }}
                            >
                              {issue.issue}
                            </span>
                          </div>
                          <span style={{ color: 'var(--text-secondary)', fontSize: '0.75rem' }}>
                            {open ? '\u25BC' : '\u25B6'}
                          </span>
                        </div>
                        {open && (
                          <div
                            style={{
                              borderTop: '1px solid var(--border)',
                              padding: '0.75rem 0.85rem',
                              display: 'grid',
                              gridTemplateColumns: issue.rootCause ? '1fr 1fr' : '1fr',
                              gap: '0.75rem',
                            }}
                          >
                            {issue.rootCause && (
                              <div>
                                <div
                                  style={{
                                    fontSize: '0.68rem',
                                    fontWeight: 700,
                                    textTransform: 'uppercase',
                                    color: '#dc2626',
                                    marginBottom: '0.3rem',
                                  }}
                                >
                                  Root Cause
                                </div>
                                <p
                                  style={{
                                    margin: 0,
                                    fontSize: '0.82rem',
                                    color: '#7f1d1d',
                                    lineHeight: 1.45,
                                  }}
                                >
                                  {issue.rootCause}
                                </p>
                              </div>
                            )}
                            <div>
                              <div
                                style={{
                                  fontSize: '0.68rem',
                                  fontWeight: 700,
                                  textTransform: 'uppercase',
                                  color: '#16a34a',
                                  marginBottom: '0.3rem',
                                }}
                              >
                                {issue.fix ? 'Fix' : 'Action'}
                              </div>
                              <p
                                style={{
                                  margin: 0,
                                  fontSize: '0.82rem',
                                  color: '#14532d',
                                  lineHeight: 1.45,
                                }}
                              >
                                {issue.fix || issue.action}
                              </p>
                            </div>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>

                {/* Interview Answer */}
                <div
                  style={{
                    background: 'linear-gradient(135deg, #faf5ff 0%, #ede9fe 100%)',
                    border: '1px solid #ddd6fe',
                    borderRadius: '10px',
                    padding: '1rem',
                  }}
                >
                  <div
                    style={{
                      fontSize: '0.7rem',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.05em',
                      color: '#7c3aed',
                      marginBottom: '0.5rem',
                    }}
                  >
                    Interview Answer
                  </div>
                  <div
                    style={{
                      fontSize: '0.875rem',
                      lineHeight: 1.65,
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
                    {lvl.interviewAnswer}
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
              </div>
            )}
          </div>
        );
      })}

      {/* E2E Flow Diagram */}
      <div className="card" style={{ marginTop: '2rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>End-to-End Escalation Flow</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1.25rem', fontSize: '0.9rem' }}>
          How an incident travels from alert detection through all support levels to resolution.
        </p>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0' }}>
          {e2eFlow.map((step, idx) => {
            const isLast = idx === e2eFlow.length - 1;
            const stepColor = step.level ? levelColors[step.level] : '#6b7280';
            return (
              <div key={step.step} style={{ display: 'flex', gap: '0' }}>
                {/* Connector column */}
                <div
                  style={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    width: '36px',
                    flexShrink: 0,
                  }}
                >
                  <div
                    style={{
                      width: '28px',
                      height: '28px',
                      borderRadius: '50%',
                      background: stepColor,
                      color: '#fff',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      fontSize: '0.72rem',
                      fontWeight: 700,
                      flexShrink: 0,
                      zIndex: 1,
                    }}
                  >
                    {step.step}
                  </div>
                  {!isLast && (
                    <div
                      style={{
                        width: '2px',
                        flex: 1,
                        background: 'var(--border)',
                        minHeight: '20px',
                      }}
                    />
                  )}
                </div>

                {/* Step content */}
                <div
                  style={{
                    flex: 1,
                    paddingLeft: '0.75rem',
                    paddingBottom: isLast ? 0 : '0.75rem',
                    paddingTop: '2px',
                  }}
                >
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      marginBottom: '0.2rem',
                    }}
                  >
                    <span
                      style={{ fontWeight: 700, fontSize: '0.85rem', color: 'var(--text-primary)' }}
                    >
                      {step.actor}
                    </span>
                    {step.level && (
                      <span
                        style={{
                          fontSize: '0.68rem',
                          fontWeight: 700,
                          background: `${stepColor}20`,
                          color: stepColor,
                          border: `1px solid ${stepColor}50`,
                          borderRadius: '4px',
                          padding: '1px 6px',
                        }}
                      >
                        {step.level}
                      </span>
                    )}
                  </div>
                  <p
                    style={{
                      margin: 0,
                      fontSize: '0.82rem',
                      color: 'var(--text-secondary)',
                      lineHeight: 1.45,
                    }}
                  >
                    {step.action}
                  </p>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Example Scenario */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>{exampleScenario.title}</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1.25rem', fontSize: '0.9rem' }}>
          Trace a real production incident through all support levels.
        </p>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
          {exampleScenario.steps.map((s, idx) => {
            const color = levelColors[s.level] || '#6b7280';
            return (
              <div
                key={idx}
                style={{
                  display: 'flex',
                  gap: '0.85rem',
                  alignItems: 'flex-start',
                  background: `${color}08`,
                  border: `1px solid ${color}30`,
                  borderRadius: '8px',
                  padding: '0.75rem 1rem',
                }}
              >
                <div
                  style={{
                    background: color,
                    color: '#fff',
                    fontWeight: 800,
                    fontSize: '0.78rem',
                    padding: '2px 8px',
                    borderRadius: '5px',
                    flexShrink: 0,
                    alignSelf: 'center',
                  }}
                >
                  {s.level}
                </div>
                <p
                  style={{
                    margin: 0,
                    fontSize: '0.875rem',
                    color: 'var(--text-primary)',
                    lineHeight: 1.5,
                  }}
                >
                  {s.action}
                </p>
              </div>
            );
          })}
        </div>
      </div>

      {/* SLA Ownership Table */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>SLA Ownership Table</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1.25rem', fontSize: '0.9rem' }}>
          Response time targets and ownership per support level.
        </p>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.875rem' }}>
            <thead>
              <tr style={{ background: 'var(--bg-secondary)' }}>
                {['Level', 'Role', 'Response Time', 'Resolution Target', 'Owner'].map((h) => (
                  <th
                    key={h}
                    style={{
                      padding: '0.6rem 0.85rem',
                      textAlign: 'left',
                      fontWeight: 700,
                      color: 'var(--text-secondary)',
                      fontSize: '0.75rem',
                      textTransform: 'uppercase',
                      letterSpacing: '0.05em',
                      borderBottom: '1px solid var(--border)',
                    }}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {slaTable.map((row) => {
                const color = levelColors[row.level] || '#6b7280';
                return (
                  <tr key={row.level} style={{ borderBottom: '1px solid var(--border)' }}>
                    <td style={{ padding: '0.6rem 0.85rem' }}>
                      <span
                        style={{
                          background: color,
                          color: '#fff',
                          fontWeight: 700,
                          fontSize: '0.78rem',
                          padding: '2px 8px',
                          borderRadius: '5px',
                        }}
                      >
                        {row.level}
                      </span>
                    </td>
                    <td
                      style={{
                        padding: '0.6rem 0.85rem',
                        color: 'var(--text-primary)',
                        fontWeight: 600,
                      }}
                    >
                      {row.role}
                    </td>
                    <td
                      style={{
                        padding: '0.6rem 0.85rem',
                        color: '#d97706',
                        fontFamily: 'monospace',
                        fontSize: '0.82rem',
                      }}
                    >
                      {row.responseTime}
                    </td>
                    <td
                      style={{
                        padding: '0.6rem 0.85rem',
                        color: '#16a34a',
                        fontFamily: 'monospace',
                        fontSize: '0.82rem',
                      }}
                    >
                      {row.resolutionTarget}
                    </td>
                    <td style={{ padding: '0.6rem 0.85rem', color: 'var(--text-secondary)' }}>
                      {row.owner}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Prevention Best Practices */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>Prevention Best Practices</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1.25rem', fontSize: '0.9rem' }}>
          Design principles that reduce the need for L2+ escalations.
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
            gap: '0.6rem',
          }}
        >
          {preventionBestPractices.map((p, idx) => (
            <div
              key={idx}
              style={{
                display: 'flex',
                alignItems: 'flex-start',
                gap: '0.5rem',
                background: '#f0fdf4',
                border: '1px solid #bbf7d0',
                borderRadius: '8px',
                padding: '0.65rem 0.85rem',
              }}
            >
              <span
                style={{
                  color: '#16a34a',
                  fontWeight: 700,
                  flexShrink: 0,
                  fontSize: '0.85rem',
                  marginTop: '1px',
                }}
              >
                &#x2713;
              </span>
              <span style={{ fontSize: '0.85rem', color: '#14532d', lineHeight: 1.45 }}>{p}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Interview Summary */}
      <div
        className="card"
        style={{
          marginTop: '1.5rem',
          background: 'linear-gradient(135deg, #1e1b4b 0%, #312e81 100%)',
          border: '1px solid #4338ca',
          color: '#e0e7ff',
        }}
      >
        <div
          style={{
            fontSize: '0.7rem',
            fontWeight: 700,
            textTransform: 'uppercase',
            letterSpacing: '0.1em',
            color: '#a5b4fc',
            marginBottom: '0.75rem',
          }}
        >
          Interview Summary
        </div>
        <div
          style={{
            fontSize: '1rem',
            lineHeight: 1.75,
            fontStyle: 'italic',
            position: 'relative',
            paddingLeft: '1.5rem',
          }}
        >
          <span
            style={{
              position: 'absolute',
              left: 0,
              top: '-6px',
              fontSize: '2.5rem',
              color: '#6366f1',
              lineHeight: 1,
            }}
          >
            &ldquo;
          </span>
          A well-defined production support model is critical for data platform reliability. L1 owns
          monitoring and triage — their job is fast detection and clean escalation, not debugging.
          L2 data engineers investigate pipeline and data issues, apply config fixes, and rerun
          jobs. L3 senior engineers and architects perform root cause analysis, implement permanent
          fixes, and prevent recurrence through runbook updates and pipeline redesign. L4 handles
          platform-level and infrastructure failures, engaging cloud providers or vendors as needed.
          The model works when escalation criteria are explicit, SLAs are enforced at each level,
          and every major incident results in a post-mortem with a prevention action. The goal is to
          push issues left — catch more at L1, fix more permanently at L3, and never reach L4 for
          avoidable problems.
          <span
            style={{
              fontSize: '2.5rem',
              color: '#6366f1',
              lineHeight: 1,
              verticalAlign: 'bottom',
              marginLeft: '4px',
            }}
          >
            &rdquo;
          </span>
        </div>
      </div>
    </div>
  );
}

export default ProductionSupport;
