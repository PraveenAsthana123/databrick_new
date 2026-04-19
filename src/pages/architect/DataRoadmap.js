import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';
import DeepGuide from '../../components/architect/DeepGuide';
import FileFormatRunner from '../../components/common/FileFormatRunner';

const phases = [
  {
    id: 1,
    label: 'Phase 1',
    period: '0-3 Months',
    name: 'Stabilize & Foundation',
    color: '#3b82f6',
    badgeClass: 'running',
    objectives: [
      'Centralize data from disparate sources onto a single platform',
      'Eliminate manual data processes and reduce human error',
      'Stabilize existing pipelines and ensure reliable data delivery',
    ],
    initiatives: [
      'Batch ingestion pipelines for core source systems',
      'Bronze layer (raw data landing zone) on the lakehouse',
      'Orchestration framework (Airflow / Databricks Workflows)',
      'Basic data validation and schema enforcement',
      'Logging and alerting for pipeline failures',
      'Basic RBAC and access control setup',
    ],
    deliverables: [
      'Data platform setup and provisioned environment',
      'Ingestion framework with documented source inventory',
      'Initial operational dashboards for monitoring',
      'Data inventory and source catalogue',
    ],
    architecture: 'Sources → Ingestion Layer → Bronze (Raw) → Basic Reports',
    kpis: [
      'Pipeline success rate > 90%',
      'Data availability improved vs baseline',
      'Manual effort reduced by 30-40%',
    ],
    interviewAnswer:
      'We stabilized ingestion, centralized data onto a unified platform, and built foundational pipelines that eliminated manual processes and established reliable data delivery.',
  },
  {
    id: 2,
    label: 'Phase 2',
    period: '3-9 Months',
    name: 'Standardize & Scale',
    color: '#8b5cf6',
    badgeClass: 'pending',
    objectives: [
      'Improve data quality and establish measurable quality standards',
      'Standardize data models across business domains',
      'Enable self-service analytics for business users',
    ],
    initiatives: [
      'Silver layer (cleansed and conformed data) build-out',
      'Data Quality (DQ) framework with rules and scoring',
      'Governance policies, data ownership and stewardship model',
      'Business dashboards and data marts for analytics',
      'Canonical / enterprise data model definition',
      'Query optimization and performance tuning',
    ],
    deliverables: [
      'Silver datasets — cleansed, deduplicated, conformed',
      'DQ framework with automated rule execution',
      'Business glossary and data dictionary',
      'Gold layer (curated aggregates) initial build',
    ],
    architecture: 'Bronze → Silver (Cleansed) → Gold (Basic) → Self-Service BI',
    kpis: [
      'Data Quality score > 80% across key domains',
      'Report accuracy and consistency improved',
      'Self-service adoption rate growing month-over-month',
    ],
    interviewAnswer:
      'We standardized data models, improved quality through a formal DQ framework, and enabled self-service analytics so business teams could access trusted data independently.',
  },
  {
    id: 3,
    label: 'Phase 3',
    period: '9-18 Months',
    name: 'Optimize & Govern',
    color: '#10b981',
    badgeClass: 'success',
    objectives: [
      'Scale the platform to support enterprise-wide adoption',
      'Enforce governance, security, and compliance standards',
      'Optimize platform cost and operational efficiency',
    ],
    initiatives: [
      'Unity Catalog for centralized governance and lineage',
      'PII discovery, masking, and column-level RBAC',
      'Z-order clustering and caching for query performance',
      'FinOps dashboards and cost allocation by team / domain',
      'Full observability — pipeline health, DQ trends, usage metrics',
      'Delta Sharing for cross-team and external data distribution',
    ],
    deliverables: [
      'Governance framework with enforced policies',
      'Data catalog with full lineage and classification',
      'Cost dashboards with attribution and optimization recommendations',
      'Secure, auditable data access controls',
    ],
    architecture:
      'Bronze → Silver → Gold → Governed + Secured + Optimized (Unity Catalog + FinOps)',
    kpis: [
      'SLA adherence > 95% for critical datasets',
      'Platform cost reduced by 20-30%',
      'Data trust score improved (survey-based)',
    ],
    interviewAnswer:
      'We scaled the platform with enterprise governance, enforced security through Unity Catalog and PII controls, and achieved significant cost reduction through FinOps practices.',
  },
  {
    id: 4,
    label: 'Phase 4',
    period: '18-36 Months',
    name: 'AI / GenAI Transformation',
    color: '#f59e0b',
    badgeClass: 'warning',
    objectives: [
      'Enable AI and ML use cases on a trusted, governed data foundation',
      'Build RAG (Retrieval-Augmented Generation) architecture for GenAI',
      'Drive measurable business value through real-time and AI-powered insights',
    ],
    initiatives: [
      'ML pipelines and feature store on curated Gold data',
      'RAG pipeline for enterprise documents — embeddings and vector search',
      'Streaming ingestion for real-time event processing',
      'Data products with defined SLAs and consumer contracts',
      'AI-assisted data quality and anomaly detection',
      'Model feedback loop and continuous retraining framework',
    ],
    deliverables: [
      'AI-ready datasets with feature engineering pipelines',
      'RAG architecture with document ingestion and retrieval',
      'Real-time streaming pipelines for priority use cases',
      'Data product catalog with ownership and SLAs',
    ],
    architecture:
      'Gold → Feature Store → AI / ML Models → RAG → Real-Time Streaming → Business Applications',
    kpis: [
      'AI model adoption rate across business units',
      'Prediction accuracy and model performance metrics',
      'Measurable business ROI from AI-driven decisions',
    ],
    interviewAnswer:
      'We transformed the organization into an AI-driven ecosystem by building ML pipelines, a RAG architecture for GenAI, and real-time capabilities — all on a trusted, governed data foundation.',
  },
];

const capabilityMatrix = [
  {
    capability: 'Ingestion',
    ph1: 'Batch, scheduled',
    ph2: 'Validated, monitored',
    ph3: 'Governed, lineage-tracked',
    ph4: 'Real-time + streaming',
  },
  {
    capability: 'Storage',
    ph1: 'Bronze (raw landing)',
    ph2: 'Bronze + Silver',
    ph3: 'Bronze + Silver + Gold',
    ph4: 'Full medallion + vector store',
  },
  {
    capability: 'Data Quality',
    ph1: 'Schema checks only',
    ph2: 'DQ rules + scoring',
    ph3: 'Automated enforcement',
    ph4: 'AI-assisted anomaly detection',
  },
  {
    capability: 'Governance',
    ph1: 'Basic RBAC',
    ph2: 'Policies + ownership',
    ph3: 'Unity Catalog + lineage',
    ph4: 'Full data product contracts',
  },
  {
    capability: 'Analytics',
    ph1: 'Operational dashboards',
    ph2: 'Self-service BI + data marts',
    ph3: 'Optimized Gold layer',
    ph4: 'AI/ML + real-time insights',
  },
  {
    capability: 'Security',
    ph1: 'Basic access control',
    ph2: 'Defined ownership',
    ph3: 'PII masking + column RBAC',
    ph4: 'AI-monitored access patterns',
  },
  {
    capability: 'Observability',
    ph1: 'Logging + failure alerts',
    ph2: 'DQ trend monitoring',
    ph3: 'Full platform observability',
    ph4: 'Predictive health checks',
  },
  {
    capability: 'Cost Management',
    ph1: 'Baseline tracking',
    ph2: 'Team allocation starts',
    ph3: 'FinOps dashboards, -20-30%',
    ph4: 'Optimized AI workload costs',
  },
];

const commonFailures = [
  {
    failure: 'Skipping Bronze layer',
    impact: 'Raw data lost, no audit trail, cannot reprocess history',
    fix: 'Always land raw data first before any transformation',
  },
  {
    failure: 'No DQ framework in Phase 1',
    impact: 'Bad data propagates, trust erodes, rework costs rise',
    fix: 'Introduce basic schema + null checks from day one',
  },
  {
    failure: 'Governance as an afterthought',
    impact: 'Compliance risk, PII exposure, ungoverned data sprawl',
    fix: 'Define ownership and access policies by end of Phase 2',
  },
  {
    failure: 'Building AI before data quality is stable',
    impact: 'Models trained on bad data produce unreliable predictions',
    fix: 'Enforce DQ score threshold before enabling ML pipelines',
  },
  {
    failure: 'No FinOps discipline',
    impact: 'Cloud spend spirals, ROI difficult to demonstrate',
    fix: 'Tag resources, allocate costs by team, review monthly',
  },
  {
    failure: 'Self-service without curated Gold layer',
    impact: 'Users query raw data, inconsistent results, shadow IT',
    fix: 'Curate and certify Gold datasets before opening self-service',
  },
];

const artifactsByPhase = [
  {
    phase: 'Phase 1',
    color: '#3b82f6',
    artifacts: [
      'Source inventory document',
      'Ingestion pipeline templates',
      'Bronze layer DDL / schema definitions',
      'Orchestration DAG library',
      'Alert runbook',
      'RBAC access matrix (initial)',
    ],
  },
  {
    phase: 'Phase 2',
    color: '#8b5cf6',
    artifacts: [
      'Silver layer transformation specs',
      'DQ rules catalogue',
      'Business glossary (v1)',
      'Canonical data model',
      'Self-service BI guidelines',
      'Data ownership RACI',
    ],
  },
  {
    phase: 'Phase 3',
    color: '#10b981',
    artifacts: [
      'Unity Catalog namespace design',
      'PII classification register',
      'FinOps cost allocation model',
      'Observability dashboard suite',
      'Governance operating model',
      'SLA definitions for Gold datasets',
    ],
  },
  {
    phase: 'Phase 4',
    color: '#f59e0b',
    artifacts: [
      'Feature store schema and registry',
      'RAG architecture blueprint',
      'Streaming pipeline templates',
      'Data product catalog',
      'Model registry + lineage',
      'AI feedback loop runbook',
    ],
  },
];

function DataRoadmap() {
  const [expandedId, setExpandedId] = useState(null);
  const [activeTab, setActiveTab] = useState('phases');

  const downloadCSV = () => {
    exportToCSV(
      phases.map((p) => ({
        phase: p.label,
        period: p.period,
        name: p.name,
        objectives: p.objectives.join(' | '),
        initiatives: p.initiatives.join(' | '),
        deliverables: p.deliverables.join(' | '),
        architecture: p.architecture,
        kpis: p.kpis.join(' | '),
        interviewAnswer: p.interviewAnswer,
      })),
      'enterprise-data-roadmap.csv'
    );
  };

  const tabs = [
    { id: 'phases', label: 'Roadmap Phases' },
    { id: 'matrix', label: 'Capability Matrix' },
    { id: 'failures', label: 'Common Failures' },
    { id: 'artifacts', label: 'Artifacts by Phase' },
  ];

  return (
    <div>
      {/* Page Header */}
      <div className="page-header">
        <div>
          <h1>Enterprise Data Roadmap</h1>
          <p>
            4-phase transformation roadmap from stabilization to AI/GenAI — with objectives,
            initiatives, deliverables, KPIs, and interview answers
          </p>
        </div>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F5FA;</div>
          <div className="stat-info">
            <h4>4</h4>
            <p>Roadmap Phases</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#x1F3AF;</div>
          <div className="stat-info">
            <h4>36</h4>
            <p>Month Horizon</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#x2705;</div>
          <div className="stat-info">
            <h4>16</h4>
            <p>Key Deliverables</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#x1F4A1;</div>
          <div className="stat-info">
            <h4>4</h4>
            <p>Interview Answers</p>
          </div>
        </div>
      </div>

      <FileFormatRunner
        data={phases.map((p) => ({
          id: p.id,
          label: p.label,
          period: p.period,
          name: p.name,
          architecture: p.architecture,
          interviewAnswer: p.interviewAnswer,
        }))}
        slug="data-roadmap"
        schemaName="RoadmapPhase"
        tableName="catalog.architect.data_roadmap"
      />

      {/* Tabs + CSV */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div
          style={{
            display: 'flex',
            gap: '0.5rem',
            flexWrap: 'wrap',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={
                  activeTab === tab.id ? 'btn btn-primary btn-sm' : 'btn btn-secondary btn-sm'
                }
              >
                {tab.label}
              </button>
            ))}
          </div>
          <button className="btn btn-secondary btn-sm" onClick={downloadCSV}>
            Download CSV
          </button>
        </div>
      </div>

      {/* ─── TAB: Roadmap Phases ─── */}
      {activeTab === 'phases' && (
        <>
          {phases.map((phase) => {
            const isExpanded = expandedId === phase.id;
            return (
              <div key={phase.id} className="card" style={{ marginBottom: '0.75rem' }}>
                {/* Collapsed header */}
                <div
                  onClick={() => setExpandedId(isExpanded ? null : phase.id)}
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
                        gap: '0.6rem',
                        flexWrap: 'wrap',
                        marginBottom: '0.3rem',
                      }}
                    >
                      <span
                        className={`badge ${phase.badgeClass}`}
                        style={{ minWidth: '4.5rem', textAlign: 'center' }}
                      >
                        {phase.label}
                      </span>
                      <span
                        style={{
                          fontSize: '0.78rem',
                          fontWeight: 600,
                          color: phase.color,
                          background: `${phase.color}15`,
                          border: `1px solid ${phase.color}40`,
                          borderRadius: '4px',
                          padding: '2px 8px',
                        }}
                      >
                        {phase.period}
                      </span>
                      <strong style={{ fontSize: '1rem' }}>{phase.name}</strong>
                    </div>
                    <p
                      style={{
                        color: 'var(--text-secondary)',
                        fontSize: '0.85rem',
                        margin: 0,
                      }}
                    >
                      {phase.objectives[0]}
                    </p>
                  </div>
                  <span
                    style={{
                      color: 'var(--text-secondary)',
                      flexShrink: 0,
                      marginTop: '2px',
                    }}
                  >
                    {isExpanded ? '\u25BC' : '\u25B6'}
                  </span>
                </div>

                {/* Expanded detail */}
                {isExpanded && (
                  <div style={{ marginTop: '1.5rem' }}>
                    {/* Top row: Objectives + Initiatives */}
                    <div
                      style={{
                        display: 'grid',
                        gridTemplateColumns: '1fr 1fr',
                        gap: '1rem',
                        marginBottom: '1rem',
                      }}
                    >
                      {/* Objectives */}
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
                            marginBottom: '0.6rem',
                          }}
                        >
                          Objectives
                        </div>
                        <ul style={{ margin: 0, paddingLeft: '1.1rem' }}>
                          {phase.objectives.map((obj, i) => (
                            <li
                              key={i}
                              style={{
                                fontSize: '0.85rem',
                                lineHeight: 1.6,
                                color: '#1e3a5f',
                                marginBottom: '0.25rem',
                              }}
                            >
                              {obj}
                            </li>
                          ))}
                        </ul>
                      </div>

                      {/* Initiatives */}
                      <div
                        style={{
                          background: 'linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%)',
                          border: '1px solid #bbf7d0',
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
                            color: '#16a34a',
                            marginBottom: '0.6rem',
                          }}
                        >
                          Initiatives
                        </div>
                        <ul style={{ margin: 0, paddingLeft: '1.1rem' }}>
                          {phase.initiatives.map((init, i) => (
                            <li
                              key={i}
                              style={{
                                fontSize: '0.85rem',
                                lineHeight: 1.6,
                                color: '#14532d',
                                marginBottom: '0.25rem',
                              }}
                            >
                              {init}
                            </li>
                          ))}
                        </ul>
                      </div>
                    </div>

                    {/* Middle row: Deliverables + KPIs */}
                    <div
                      style={{
                        display: 'grid',
                        gridTemplateColumns: '1fr 1fr',
                        gap: '1rem',
                        marginBottom: '1rem',
                      }}
                    >
                      {/* Deliverables */}
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
                            marginBottom: '0.6rem',
                          }}
                        >
                          Deliverables
                        </div>
                        <ul style={{ margin: 0, paddingLeft: '1.1rem' }}>
                          {phase.deliverables.map((del, i) => (
                            <li
                              key={i}
                              style={{
                                fontSize: '0.85rem',
                                lineHeight: 1.6,
                                color: '#3b0764',
                                marginBottom: '0.25rem',
                              }}
                            >
                              {del}
                            </li>
                          ))}
                        </ul>
                      </div>

                      {/* KPIs */}
                      <div
                        style={{
                          background: 'linear-gradient(135deg, #fff7ed 0%, #ffedd5 100%)',
                          border: '1px solid #fed7aa',
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
                            color: '#c2410c',
                            marginBottom: '0.6rem',
                          }}
                        >
                          KPIs
                        </div>
                        <ul style={{ margin: 0, paddingLeft: '1.1rem' }}>
                          {phase.kpis.map((kpi, i) => (
                            <li
                              key={i}
                              style={{
                                fontSize: '0.85rem',
                                lineHeight: 1.6,
                                color: '#7c2d12',
                                marginBottom: '0.25rem',
                              }}
                            >
                              {kpi}
                            </li>
                          ))}
                        </ul>
                      </div>
                    </div>

                    {/* Architecture */}
                    <div
                      style={{
                        background: `${phase.color}10`,
                        border: `1px solid ${phase.color}35`,
                        borderLeft: `4px solid ${phase.color}`,
                        borderRadius: '10px',
                        padding: '0.875rem 1rem',
                        marginBottom: '1rem',
                      }}
                    >
                      <div
                        style={{
                          fontSize: '0.7rem',
                          fontWeight: 700,
                          textTransform: 'uppercase',
                          letterSpacing: '0.05em',
                          color: phase.color,
                          marginBottom: '0.4rem',
                        }}
                      >
                        Architecture Flow
                      </div>
                      <p
                        style={{
                          margin: 0,
                          fontSize: '0.875rem',
                          fontFamily: 'monospace',
                          color: 'var(--text-primary)',
                          fontWeight: 600,
                        }}
                      >
                        {phase.architecture}
                      </p>
                    </div>

                    {/* Interview Answer */}
                    <div
                      style={{
                        background: 'linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%)',
                        border: '1px solid #fecaca',
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
                          color: '#dc2626',
                          marginBottom: '0.5rem',
                        }}
                      >
                        Interview Answer
                      </div>
                      <div
                        style={{
                          fontSize: '0.875rem',
                          lineHeight: 1.65,
                          color: '#7f1d1d',
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
                            color: '#fca5a5',
                            lineHeight: 1,
                          }}
                        >
                          &ldquo;
                        </span>
                        {phase.interviewAnswer}
                        <span
                          style={{
                            fontSize: '2rem',
                            color: '#fca5a5',
                            lineHeight: 1,
                            verticalAlign: 'bottom',
                            marginLeft: '4px',
                          }}
                        >
                          &rdquo;
                        </span>
                      </div>
                    </div>

                    {/* Architect Deep Guide — steps, recommendations, pitfalls, KPIs */}
                    <DeepGuide type="roadmap" phase={phase.id} />
                  </div>
                )}
              </div>
            );
          })}
        </>
      )}

      {/* ─── TAB: Capability Matrix ─── */}
      {activeTab === 'matrix' && (
        <div className="card">
          <h2 style={{ marginBottom: '0.4rem' }}>Cross-Phase Capability Matrix</h2>
          <p
            style={{
              color: 'var(--text-secondary)',
              fontSize: '0.875rem',
              marginBottom: '1.25rem',
            }}
          >
            How each capability evolves across all four phases of the roadmap.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table
              style={{
                width: '100%',
                borderCollapse: 'collapse',
                fontSize: '0.85rem',
              }}
            >
              <thead>
                <tr>
                  {[
                    'Capability',
                    'Phase 1 (0-3m)',
                    'Phase 2 (3-9m)',
                    'Phase 3 (9-18m)',
                    'Phase 4 (18-36m)',
                  ].map((col, i) => (
                    <th
                      key={col}
                      style={{
                        textAlign: 'left',
                        padding: '0.6rem 0.75rem',
                        background:
                          i === 0
                            ? 'var(--bg-secondary)'
                            : ['#3b82f620', '#8b5cf620', '#10b98120', '#f59e0b20'][i - 1],
                        borderBottom: '2px solid var(--border)',
                        color:
                          i === 0
                            ? 'var(--text-secondary)'
                            : ['#3b82f6', '#8b5cf6', '#10b981', '#f59e0b'][i - 1],
                        fontWeight: 700,
                        fontSize: '0.78rem',
                        textTransform: 'uppercase',
                        letterSpacing: '0.04em',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {capabilityMatrix.map((row, idx) => (
                  <tr
                    key={row.capability}
                    style={{
                      background: idx % 2 === 0 ? 'transparent' : 'var(--bg-secondary)',
                    }}
                  >
                    <td
                      style={{
                        padding: '0.6rem 0.75rem',
                        fontWeight: 700,
                        color: 'var(--text-primary)',
                        borderBottom: '1px solid var(--border)',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {row.capability}
                    </td>
                    {[row.ph1, row.ph2, row.ph3, row.ph4].map((cell, ci) => (
                      <td
                        key={ci}
                        style={{
                          padding: '0.6rem 0.75rem',
                          color: 'var(--text-secondary)',
                          borderBottom: '1px solid var(--border)',
                          lineHeight: 1.5,
                        }}
                      >
                        {cell}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ─── TAB: Common Failures ─── */}
      {activeTab === 'failures' && (
        <div className="card">
          <h2 style={{ marginBottom: '0.4rem' }}>Common Roadmap Failures</h2>
          <p
            style={{
              color: 'var(--text-secondary)',
              fontSize: '0.875rem',
              marginBottom: '1.25rem',
            }}
          >
            Critical mistakes that derail data transformation programs — and how to avoid them.
          </p>
          <div
            style={{
              display: 'grid',
              gap: '0.75rem',
            }}
          >
            {commonFailures.map((item, idx) => (
              <div
                key={idx}
                style={{
                  display: 'grid',
                  gridTemplateColumns: '1fr 1fr 1fr',
                  gap: '0',
                  border: '1px solid var(--border)',
                  borderRadius: '10px',
                  overflow: 'hidden',
                }}
              >
                <div
                  style={{
                    background: 'linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%)',
                    padding: '0.875rem 1rem',
                    borderRight: '1px solid #fecaca',
                  }}
                >
                  <div
                    style={{
                      fontSize: '0.68rem',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.05em',
                      color: '#dc2626',
                      marginBottom: '0.35rem',
                    }}
                  >
                    Failure
                  </div>
                  <p
                    style={{
                      margin: 0,
                      fontSize: '0.85rem',
                      fontWeight: 600,
                      color: '#7f1d1d',
                      lineHeight: 1.4,
                    }}
                  >
                    {item.failure}
                  </p>
                </div>
                <div
                  style={{
                    background: 'linear-gradient(135deg, #fff7ed 0%, #ffedd5 100%)',
                    padding: '0.875rem 1rem',
                    borderRight: '1px solid #fed7aa',
                  }}
                >
                  <div
                    style={{
                      fontSize: '0.68rem',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.05em',
                      color: '#c2410c',
                      marginBottom: '0.35rem',
                    }}
                  >
                    Impact
                  </div>
                  <p
                    style={{
                      margin: 0,
                      fontSize: '0.85rem',
                      color: '#7c2d12',
                      lineHeight: 1.5,
                    }}
                  >
                    {item.impact}
                  </p>
                </div>
                <div
                  style={{
                    background: 'linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%)',
                    padding: '0.875rem 1rem',
                  }}
                >
                  <div
                    style={{
                      fontSize: '0.68rem',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.05em',
                      color: '#16a34a',
                      marginBottom: '0.35rem',
                    }}
                  >
                    Fix
                  </div>
                  <p
                    style={{
                      margin: 0,
                      fontSize: '0.85rem',
                      color: '#14532d',
                      lineHeight: 1.5,
                    }}
                  >
                    {item.fix}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ─── TAB: Artifacts by Phase ─── */}
      {activeTab === 'artifacts' && (
        <div>
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
              gap: '1rem',
            }}
          >
            {artifactsByPhase.map((section) => (
              <div
                key={section.phase}
                className="card"
                style={{
                  borderTop: `4px solid ${section.color}`,
                }}
              >
                <div
                  style={{
                    fontSize: '0.7rem',
                    fontWeight: 700,
                    textTransform: 'uppercase',
                    letterSpacing: '0.08em',
                    color: section.color,
                    marginBottom: '0.75rem',
                  }}
                >
                  {section.phase}
                </div>
                <ul style={{ margin: 0, paddingLeft: '1.1rem' }}>
                  {section.artifacts.map((artifact, i) => (
                    <li
                      key={i}
                      style={{
                        fontSize: '0.875rem',
                        lineHeight: 1.7,
                        color: 'var(--text-primary)',
                        marginBottom: '0.1rem',
                      }}
                    >
                      {artifact}
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Interview Summary Card */}
      <div
        className="card"
        style={{
          marginTop: '2rem',
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
          Interview Summary — Enterprise Data Roadmap
        </div>
        <div
          style={{
            fontSize: '1rem',
            lineHeight: 1.8,
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
          We executed a phased 36-month data transformation. In Phase 1, we stabilized ingestion,
          centralized data, and eliminated manual processes. In Phase 2, we standardized models,
          improved data quality, and enabled self-service analytics. In Phase 3, we scaled with
          enterprise governance through Unity Catalog, enforced PII and RBAC controls, and reduced
          cost by 20-30% through FinOps. In Phase 4, we transformed into an AI-driven ecosystem —
          building ML pipelines, a RAG architecture for GenAI, and real-time streaming capabilities
          — all on a trusted, governed data foundation with measurable business ROI.
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

export default DataRoadmap;
