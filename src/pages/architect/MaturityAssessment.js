import React, { useState, useMemo } from 'react';
import { exportToCSV } from '../../utils/fileExport';
import DeepGuide from '../../components/architect/DeepGuide';
import FileFormatRunner from '../../components/common/FileFormatRunner';

// ─── Assessment Data ───────────────────────────────────────────────────────────
const DIMENSIONS = [
  {
    id: 'A',
    name: 'Data Ingestion',
    color: '#6366f1',
    criteria: [
      {
        id: 'A1',
        name: 'Source Integration',
        levels: {
          1: 'Manual extracts from 1–2 sources, no automation',
          3: 'Automated connectors for major sources, some gaps',
          5: 'Unified ingestion framework, 100+ sources, real-time & batch',
        },
      },
      {
        id: 'A2',
        name: 'Pipeline Reliability',
        levels: {
          1: 'Ad-hoc scripts, frequent failures with no alerting',
          3: 'Orchestrated pipelines with basic retry and alerting',
          5: 'SLA-managed pipelines, self-healing, <0.1% failure rate',
        },
      },
      {
        id: 'A3',
        name: 'Schema Handling',
        levels: {
          1: 'Hardcoded schemas break on upstream changes',
          3: 'Schema registry in place, partial evolution handling',
          5: 'Full schema evolution, backward/forward compatible, auto-detection',
        },
      },
      {
        id: 'A4',
        name: 'Data Latency',
        levels: {
          1: 'Daily or weekly batch loads only',
          3: 'Near-real-time for select critical sources (hours)',
          5: 'Streaming architecture, sub-minute latency across all critical paths',
        },
      },
    ],
  },
  {
    id: 'B',
    name: 'Data Quality',
    color: '#3b82f6',
    criteria: [
      {
        id: 'B1',
        name: 'Validation',
        levels: {
          1: 'No validation; bad data silently flows downstream',
          3: 'Validation rules exist at ingestion; failures logged',
          5: 'Automated DQ checks at every layer; quarantine on failure; SLA-tracked',
        },
      },
      {
        id: 'B2',
        name: 'Completeness',
        levels: {
          1: 'High null rates, unknown extent of missing data',
          3: 'Completeness metrics measured, remediation in progress',
          5: '>99% completeness on critical fields, real-time dashboards',
        },
      },
      {
        id: 'B3',
        name: 'Reconciliation',
        levels: {
          1: 'No reconciliation; source vs. target variances unknown',
          3: 'Periodic reconciliation for key datasets',
          5: 'Automated reconciliation every cycle; zero-tolerance thresholds enforced',
        },
      },
      {
        id: 'B4',
        name: 'Data Trust',
        levels: {
          1: 'Teams do not trust the data; shadow spreadsheets everywhere',
          3: 'Certified datasets exist; business uses them for some decisions',
          5: 'Data trust score tracked; certified data is single source of truth',
        },
      },
    ],
  },
  {
    id: 'C',
    name: 'Data Modeling',
    color: '#0ea5e9',
    criteria: [
      {
        id: 'C1',
        name: 'Data Model Quality',
        levels: {
          1: 'No formal model; tables reflect source system structure',
          3: 'Dimensional model for key domains; partially documented',
          5: 'Enterprise canonical model, domain-aligned, versioned, fully documented',
        },
      },
      {
        id: 'C2',
        name: 'SCD Handling',
        levels: {
          1: 'No history tracked; overwrites destroy historical state',
          3: 'SCD Type 2 for some dimensions; inconsistent approach',
          5: 'Consistent SCD strategy across all entities; bi-temporal where needed',
        },
      },
      {
        id: 'C3',
        name: 'Reusability',
        levels: {
          1: 'Every team builds their own transformations; heavy duplication',
          3: 'Shared curated layer exists; partial adoption',
          5: 'Certified Gold layer consumed by all; no duplication; DRY by policy',
        },
      },
      {
        id: 'C4',
        name: 'Semantic Layer',
        levels: {
          1: 'Metrics defined differently in every report',
          3: 'Business glossary published; semantic layer for key KPIs',
          5: 'Unified semantic layer governs all metrics; single definition, everywhere',
        },
      },
    ],
  },
  {
    id: 'D',
    name: 'Governance',
    color: '#8b5cf6',
    criteria: [
      {
        id: 'D1',
        name: 'Data Ownership',
        levels: {
          1: 'No defined owners; IT owns everything by default',
          3: 'Domain owners identified for critical datasets',
          5: 'RACI model enforced; every dataset has owner, steward, custodian',
        },
      },
      {
        id: 'D2',
        name: 'Data Catalog',
        levels: {
          1: 'No catalog; discovery is word-of-mouth or tribal knowledge',
          3: 'Catalog populated for major datasets; search works partially',
          5: 'Auto-cataloging, 100% coverage, business context, usage stats, ratings',
        },
      },
      {
        id: 'D3',
        name: 'Data Lineage',
        levels: {
          1: 'No lineage; impact analysis is manual and error-prone',
          3: 'Column-level lineage for select pipelines',
          5: 'End-to-end automated lineage from source to BI; impact analysis self-serve',
        },
      },
      {
        id: 'D4',
        name: 'Policy Enforcement',
        levels: {
          1: 'Policies exist as documents; not enforced in systems',
          3: 'Some policies automated (access, retention); exceptions handled manually',
          5: 'All policies codified and enforced programmatically; audit trail complete',
        },
      },
    ],
  },
  {
    id: 'E',
    name: 'Security & PII',
    color: '#ef4444',
    criteria: [
      {
        id: 'E1',
        name: 'Access Control',
        levels: {
          1: 'Broad access; most people can read all data',
          3: 'RBAC in place for critical tables; exceptions common',
          5: 'Attribute-based access control, least-privilege enforced, reviewed quarterly',
        },
      },
      {
        id: 'E2',
        name: 'PII Protection',
        levels: {
          1: 'PII is not classified; stored in plain text everywhere',
          3: 'PII discovery done; masking applied in non-prod environments',
          5: 'Automated PII tagging, dynamic masking in prod, privacy by design',
        },
      },
      {
        id: 'E3',
        name: 'Encryption',
        levels: {
          1: 'Data at rest and in transit not consistently encrypted',
          3: 'Encryption at rest and TLS in transit for most systems',
          5: 'End-to-end encryption, key rotation automated, HSM for key management',
        },
      },
      {
        id: 'E4',
        name: 'Audit Logs',
        levels: {
          1: 'No audit logs; who accessed what is unknown',
          3: 'Audit logs exist for critical systems; reviewed manually',
          5: 'Tamper-proof audit logs, real-time anomaly detection, SIEM integrated',
        },
      },
    ],
  },
  {
    id: 'F',
    name: 'Platform & Architecture',
    color: '#f97316',
    criteria: [
      {
        id: 'F1',
        name: 'Platform Maturity',
        levels: {
          1: 'On-prem legacy systems, no unified platform',
          3: 'Cloud migration in progress; cloud and on-prem coexist',
          5: 'Unified cloud lakehouse; one platform, all workloads, fully managed',
        },
      },
      {
        id: 'F2',
        name: 'Scalability',
        levels: {
          1: 'Performance degrades under load; regular failures at peak',
          3: 'Platform scales for known workloads; surprises cause incidents',
          5: 'Auto-scaling, elastic compute, handles 10x spike without intervention',
        },
      },
      {
        id: 'F3',
        name: 'Performance',
        levels: {
          1: 'Queries take hours; no optimization culture',
          3: 'SLAs defined for key workloads; tuning done reactively',
          5: 'Proactive tuning, query optimization guardrails, SLAs met 99.9%',
        },
      },
      {
        id: 'F4',
        name: 'Cost Control',
        levels: {
          1: 'No visibility into data platform costs; budget overruns common',
          3: 'Cost dashboards exist; tagging partially done',
          5: 'FinOps culture, per-domain showback, automated cost guardrails',
        },
      },
    ],
  },
  {
    id: 'G',
    name: 'BI & Consumption',
    color: '#10b981',
    criteria: [
      {
        id: 'G1',
        name: 'Reporting Coverage',
        levels: {
          1: 'Siloed reports, inconsistent, built by IT per request',
          3: 'Centralized BI platform; key operational reports automated',
          5: 'Self-service BI, certified dashboards, real-time KPIs, embedded analytics',
        },
      },
      {
        id: 'G2',
        name: 'Data Access',
        levels: {
          1: 'Analysts must request IT for every data extract',
          3: 'Self-service for power users; basic users still dependent on IT',
          5: 'Fully self-service for all personas; no IT tickets for data access',
        },
      },
      {
        id: 'G3',
        name: 'KPI Standardization',
        levels: {
          1: 'Same KPI defined differently across 5+ reports',
          3: 'Enterprise KPI dictionary exists; adoption >50%',
          5: 'Single certified KPI repository, enforced at semantic layer, 100% adoption',
        },
      },
      {
        id: 'G4',
        name: 'Data Usage',
        levels: {
          1: 'Data usage not tracked; unknown who uses what',
          3: 'Usage metrics captured for major dashboards',
          5: 'Full usage analytics, inactive assets retired, high-value assets promoted',
        },
      },
    ],
  },
  {
    id: 'H',
    name: 'AI / Advanced Analytics',
    color: '#f59e0b',
    criteria: [
      {
        id: 'H1',
        name: 'ML Usage',
        levels: {
          1: 'No ML models in production; analytics is purely descriptive',
          3: 'A few ML models in production; no standardized lifecycle',
          5: 'ML platform with versioning, monitoring, retraining, and governance',
        },
      },
      {
        id: 'H2',
        name: 'Data Readiness for AI',
        levels: {
          1: 'Data not ready for AI; quality, completeness, and labeling missing',
          3: 'Feature store emerging; some datasets curated for ML',
          5: 'Enterprise feature store, data contracts, AI-ready Gold datasets',
        },
      },
      {
        id: 'H3',
        name: 'RAG / GenAI',
        levels: {
          1: 'No GenAI capability; LLMs not integrated with enterprise data',
          3: 'Pilot RAG system for 1–2 use cases; not in production',
          5: 'Production RAG pipelines, curated vector indexes, grounded on trusted data',
        },
      },
      {
        id: 'H4',
        name: 'Feedback Loop',
        levels: {
          1: 'Models deployed and forgotten; no monitoring or retraining',
          3: 'Manual model performance review quarterly',
          5: 'Automated drift detection, feedback ingestion, continuous retraining pipeline',
        },
      },
    ],
  },
  {
    id: 'I',
    name: 'Observability',
    color: '#ec4899',
    criteria: [
      {
        id: 'I1',
        name: 'Monitoring Coverage',
        levels: {
          1: 'No monitoring; issues discovered by end users',
          3: 'Monitoring for critical pipelines; gaps in coverage',
          5: 'Full observability stack: pipelines, DQ, platform, usage — all monitored',
        },
      },
      {
        id: 'I2',
        name: 'Alerting',
        levels: {
          1: 'No alerts; on-call team manually checks logs',
          3: 'Threshold-based alerts for key failures; noisy alert fatigue',
          5: 'ML-based anomaly detection, noise-reduced smart alerts, PagerDuty integrated',
        },
      },
      {
        id: 'I3',
        name: 'SLA Tracking',
        levels: {
          1: 'No SLAs defined for data delivery',
          3: 'SLAs defined for critical datasets; tracked manually',
          5: 'All datasets have SLAs; automated breach detection; exec dashboards',
        },
      },
      {
        id: 'I4',
        name: 'RCA Process',
        levels: {
          1: 'Root cause analysis is manual and takes days',
          3: 'Structured RCA process exists; lineage helps narrow scope',
          5: 'Automated impact analysis, lineage-driven RCA, MTTR <1 hour',
        },
      },
    ],
  },
  {
    id: 'J',
    name: 'Data Culture',
    color: '#14b8a6',
    criteria: [
      {
        id: 'J1',
        name: 'Data Literacy',
        levels: {
          1: 'Most employees cannot interpret data correctly',
          3: 'Literacy program launched; ~50% of analysts trained',
          5: 'Organization-wide data literacy; all roles have role-based training paths',
        },
      },
      {
        id: 'J2',
        name: 'Data-Driven Decisions',
        levels: {
          1: 'Decisions based on gut feel or HiPPO; data not consulted',
          3: 'Data consulted for major decisions; inconsistently applied',
          5: 'All strategic decisions backed by data; culture of evidence-based thinking',
        },
      },
      {
        id: 'J3',
        name: 'Collaboration',
        levels: {
          1: 'Business and data teams operate in silos',
          3: 'Embedded analysts in some business units',
          5: 'Domain data teams, product thinking, shared OKRs between business and data',
        },
      },
      {
        id: 'J4',
        name: 'ROI Tracking',
        levels: {
          1: 'Data investments not measured; ROI unknown',
          3: 'Select initiatives tracked for business value',
          5: 'Value realization framework; every data product tracked against business KPIs',
        },
      },
    ],
  },
];

// ─── Example Assessment ─────────────────────────────────────────────────────
const EXAMPLE_SCORES = {
  A1: 3,
  A2: 3,
  A3: 2,
  A4: 2,
  B1: 3,
  B2: 3,
  B3: 2,
  B4: 2,
  C1: 3,
  C2: 2,
  C3: 3,
  C4: 2,
  D1: 3,
  D2: 3,
  D3: 2,
  D4: 2,
  E1: 3,
  E2: 3,
  E3: 3,
  E4: 2,
  F1: 3,
  F2: 3,
  F3: 2,
  F4: 2,
  G1: 3,
  G2: 3,
  G3: 2,
  G4: 2,
  H1: 2,
  H2: 2,
  H3: 1,
  H4: 1,
  I1: 2,
  I2: 2,
  I3: 2,
  I4: 2,
  J1: 2,
  J2: 2,
  J3: 2,
  J4: 2,
};

// ─── Helpers ────────────────────────────────────────────────────────────────
function getScoreColor(score) {
  if (score <= 2) return '#ef4444';
  if (score <= 3) return '#f59e0b';
  if (score <= 4) return '#10b981';
  return '#3b82f6';
}

function getScoreLabel(score) {
  if (score <= 2) return 'Foundational';
  if (score <= 3) return 'Standardized';
  if (score <= 4) return 'Scalable';
  return 'AI-Driven';
}

function getScoreBadgeStyle(score) {
  const color = getScoreColor(score);
  return {
    background: `${color}20`,
    border: `1px solid ${color}50`,
    color,
    borderRadius: '6px',
    padding: '2px 10px',
    fontSize: '0.78rem',
    fontWeight: 700,
  };
}

// ─── Roadmap ─────────────────────────────────────────────────────────────────
const ROADMAP_PHASES = [
  {
    phase: 'Phase 1',
    name: 'Stabilize',
    horizon: '0–6 months',
    color: '#ef4444',
    focus: [
      'Establish data ownership and governance model',
      'Fix critical data quality issues in top-3 domains',
      'Implement schema registry and pipeline monitoring',
      'Classify and protect PII data',
    ],
  },
  {
    phase: 'Phase 2',
    name: 'Scale',
    horizon: '6–18 months',
    color: '#f59e0b',
    focus: [
      'Build unified lakehouse platform',
      'Deploy data catalog with full lineage',
      'Launch self-service BI and semantic layer',
      'Introduce feature store and first ML models in production',
    ],
  },
  {
    phase: 'Phase 3',
    name: 'Optimize',
    horizon: '18–36 months',
    color: '#10b981',
    focus: [
      'AI-ready data platform with RAG pipelines',
      'FinOps and cost optimization automation',
      'Real-time observability with ML-based alerting',
      'Data culture program driving ROI tracking organization-wide',
    ],
  },
];

// ─── Component ───────────────────────────────────────────────────────────────
function MaturityAssessment() {
  const initialScores = {};
  DIMENSIONS.forEach((dim) => {
    dim.criteria.forEach((c) => {
      initialScores[c.id] = 0;
    });
  });

  const [scores, setScores] = useState(initialScores);
  const [expandedDim, setExpandedDim] = useState('A');
  const [showExample, setShowExample] = useState(false);

  // ── Computed values ──
  const dimensionAverages = useMemo(() => {
    const avgs = {};
    DIMENSIONS.forEach((dim) => {
      const filled = dim.criteria.filter((c) => scores[c.id] > 0);
      if (filled.length === 0) {
        avgs[dim.id] = 0;
      } else {
        const sum = filled.reduce((acc, c) => acc + scores[c.id], 0);
        avgs[dim.id] = parseFloat((sum / filled.length).toFixed(2));
      }
    });
    return avgs;
  }, [scores]);

  const overallScore = useMemo(() => {
    const scored = DIMENSIONS.filter((d) => dimensionAverages[d.id] > 0);
    if (scored.length === 0) return 0;
    const sum = scored.reduce((acc, d) => acc + dimensionAverages[d.id], 0);
    return parseFloat((sum / scored.length).toFixed(2));
  }, [dimensionAverages]);

  const totalFilled = useMemo(() => Object.values(scores).filter((v) => v > 0).length, [scores]);

  const handleScore = (criterionId, value) => {
    setScores((prev) => ({ ...prev, [criterionId]: parseInt(value, 10) }));
  };

  const loadExample = () => {
    setScores(EXAMPLE_SCORES);
    setShowExample(true);
  };

  const resetAll = () => {
    setScores(initialScores);
    setShowExample(false);
  };

  const downloadCSV = () => {
    const rows = [];
    DIMENSIONS.forEach((dim) => {
      dim.criteria.forEach((c) => {
        rows.push({
          Dimension: dim.name,
          CriterionID: c.id,
          Criterion: c.name,
          Score: scores[c.id] || 0,
          Level1: c.levels[1],
          Level3: c.levels[3],
          Level5: c.levels[5],
        });
      });
      rows.push({
        Dimension: dim.name,
        CriterionID: `${dim.id}-AVG`,
        Criterion: 'DIMENSION AVERAGE',
        Score: dimensionAverages[dim.id] || 0,
        Level1: '',
        Level3: '',
        Level5: '',
      });
    });
    rows.push({
      Dimension: 'OVERALL',
      CriterionID: 'OVERALL',
      Criterion: 'OVERALL SCORE',
      Score: overallScore,
      Level1: '',
      Level3: '',
      Level5: '',
    });
    exportToCSV(rows, 'data-maturity-assessment.csv');
  };

  return (
    <div>
      {/* ── Page Header ── */}
      <div className="page-header">
        <div>
          <h1>Data Maturity Assessment Scorecard</h1>
          <p>Interactive assessment — score your organization across 10 dimensions, 40 criteria</p>
        </div>
      </div>

      {/* ── Stats ── */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F4CA;</div>
          <div className="stat-info">
            <h4>10</h4>
            <p>Dimensions</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#x1F4CB;</div>
          <div className="stat-info">
            <h4>40</h4>
            <p>Criteria</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#x2705;</div>
          <div className="stat-info">
            <h4>{totalFilled} / 40</h4>
            <p>Scored</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#x1F3AF;</div>
          <div className="stat-info">
            <h4>{overallScore > 0 ? overallScore.toFixed(2) : '—'}</h4>
            <p>Overall Score</p>
          </div>
        </div>
      </div>

      <FileFormatRunner
        data={DIMENSIONS.map((d) => ({
          id: d.id,
          name: d.name,
          criteria_count: d.criteria?.length || 0,
        }))}
        slug="maturity-assessment"
        schemaName="AssessmentDimension"
        tableName="catalog.architect.maturity_assessment"
      />

      {/* ── Action Bar ── */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap', alignItems: 'center' }}>
          {showExample && (
            <span
              style={{
                background: '#fef3c7',
                border: '1px solid #fcd34d',
                color: '#92400e',
                borderRadius: '6px',
                padding: '4px 12px',
                fontSize: '0.82rem',
                fontWeight: 600,
              }}
            >
              Example Assessment Loaded
            </span>
          )}
          <button className="btn btn-secondary btn-sm" onClick={loadExample}>
            Load Example Assessment
          </button>
          <button className="btn btn-secondary btn-sm" onClick={resetAll}>
            Reset All Scores
          </button>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadCSV}
            style={{ marginLeft: 'auto' }}
            disabled={totalFilled === 0}
          >
            Download CSV
          </button>
        </div>
      </div>

      {/* ── Score Legend ── */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ fontWeight: 700, marginBottom: '0.75rem', fontSize: '0.9rem' }}>
          Score Guide
        </div>
        <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
          {[
            { range: '1–2', label: 'Foundational', color: '#ef4444' },
            { range: '2.1–3', label: 'Standardized', color: '#f59e0b' },
            { range: '3.1–4', label: 'Scalable', color: '#10b981' },
            { range: '4.1–5', label: 'AI-Driven', color: '#3b82f6' },
          ].map((item) => (
            <div
              key={item.label}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem',
                background: `${item.color}15`,
                border: `1px solid ${item.color}40`,
                borderRadius: '8px',
                padding: '6px 14px',
              }}
            >
              <div
                style={{
                  width: 12,
                  height: 12,
                  borderRadius: '3px',
                  background: item.color,
                  flexShrink: 0,
                }}
              />
              <span style={{ fontWeight: 700, color: item.color, fontSize: '0.82rem' }}>
                {item.range}
              </span>
              <span style={{ color: 'var(--text-secondary)', fontSize: '0.82rem' }}>
                {item.label}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* ── Dimension Summary / Radar Bars ── */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '1rem' }}>Dimension Scores</h2>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
          {DIMENSIONS.map((dim) => {
            const avg = dimensionAverages[dim.id];
            const pct = avg > 0 ? (avg / 5) * 100 : 0;
            const barColor = avg > 0 ? getScoreColor(avg) : '#e5e7eb';
            return (
              <div key={dim.id} style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                {/* Dim label */}
                <div
                  style={{
                    minWidth: '220px',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                  }}
                >
                  <span
                    style={{
                      fontWeight: 700,
                      fontSize: '0.78rem',
                      color: dim.color,
                      minWidth: '1.5rem',
                    }}
                  >
                    {dim.id}.
                  </span>
                  <span style={{ fontSize: '0.85rem', fontWeight: 600 }}>{dim.name}</span>
                </div>
                {/* Bar */}
                <div
                  style={{
                    flex: 1,
                    background: '#f1f5f9',
                    borderRadius: '999px',
                    height: '10px',
                    overflow: 'hidden',
                  }}
                >
                  <div
                    style={{
                      width: `${pct}%`,
                      height: '100%',
                      background: barColor,
                      borderRadius: '999px',
                      transition: 'width 0.4s ease',
                    }}
                  />
                </div>
                {/* Score badge */}
                <div style={{ minWidth: '90px', textAlign: 'right' }}>
                  {avg > 0 ? (
                    <span style={getScoreBadgeStyle(avg)}>
                      {avg.toFixed(2)} — {getScoreLabel(avg)}
                    </span>
                  ) : (
                    <span style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>
                      Not scored
                    </span>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {/* Overall score bar */}
        {overallScore > 0 && (
          <div
            style={{
              marginTop: '1.25rem',
              padding: '1rem 1.25rem',
              background: `${getScoreColor(overallScore)}12`,
              border: `2px solid ${getScoreColor(overallScore)}50`,
              borderRadius: '10px',
              display: 'flex',
              alignItems: 'center',
              gap: '1rem',
              flexWrap: 'wrap',
            }}
          >
            <span style={{ fontWeight: 700, fontSize: '0.9rem' }}>Overall Maturity Score</span>
            <div
              style={{
                flex: 1,
                background: '#e5e7eb',
                borderRadius: '999px',
                height: '12px',
                minWidth: '120px',
              }}
            >
              <div
                style={{
                  width: `${(overallScore / 5) * 100}%`,
                  height: '100%',
                  background: getScoreColor(overallScore),
                  borderRadius: '999px',
                  transition: 'width 0.4s ease',
                }}
              />
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <span
                style={{
                  fontSize: '1.5rem',
                  fontWeight: 800,
                  color: getScoreColor(overallScore),
                }}
              >
                {overallScore.toFixed(2)}
              </span>
              <span style={{ ...getScoreBadgeStyle(overallScore), fontSize: '0.85rem' }}>
                {getScoreLabel(overallScore)}
              </span>
            </div>
          </div>
        )}
      </div>

      {/* ── Criteria Scoring ── */}
      <div style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.75rem' }}>Score Each Criterion</h2>
        {DIMENSIONS.map((dim) => {
          const isOpen = expandedDim === dim.id;
          const avg = dimensionAverages[dim.id];
          const filledCount = dim.criteria.filter((c) => scores[c.id] > 0).length;
          return (
            <div key={dim.id} className="card" style={{ marginBottom: '0.75rem' }}>
              {/* Dimension header */}
              <div
                onClick={() => setExpandedDim(isOpen ? null : dim.id)}
                style={{
                  cursor: 'pointer',
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  gap: '1rem',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flex: 1 }}>
                  <div
                    style={{
                      width: '36px',
                      height: '36px',
                      borderRadius: '8px',
                      background: `${dim.color}20`,
                      border: `1px solid ${dim.color}50`,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      fontWeight: 800,
                      color: dim.color,
                      fontSize: '0.9rem',
                      flexShrink: 0,
                    }}
                  >
                    {dim.id}
                  </div>
                  <div>
                    <div style={{ fontWeight: 700, fontSize: '0.95rem' }}>{dim.name}</div>
                    <div style={{ color: 'var(--text-secondary)', fontSize: '0.78rem' }}>
                      {filledCount} / 4 criteria scored
                    </div>
                  </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                  {avg > 0 && <span style={getScoreBadgeStyle(avg)}>{avg.toFixed(2)}</span>}
                  <span style={{ color: 'var(--text-secondary)' }}>
                    {isOpen ? '\u25BC' : '\u25B6'}
                  </span>
                </div>
              </div>

              {/* Expanded criteria */}
              {isOpen && (
                <div
                  style={{
                    marginTop: '1.25rem',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '1rem',
                  }}
                >
                  {dim.criteria.map((c, idx) => {
                    const score = scores[c.id];
                    const scored = score > 0;
                    return (
                      <div
                        key={c.id}
                        style={{
                          background: scored
                            ? `${getScoreColor(score)}08`
                            : 'var(--bg-secondary, #f8fafc)',
                          border: `1px solid ${scored ? getScoreColor(score) + '40' : '#e5e7eb'}`,
                          borderRadius: '10px',
                          padding: '1rem 1.25rem',
                        }}
                      >
                        {/* Criterion header row */}
                        <div
                          style={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            alignItems: 'flex-start',
                            gap: '1rem',
                            flexWrap: 'wrap',
                            marginBottom: '0.75rem',
                          }}
                        >
                          <div>
                            <span
                              style={{
                                fontWeight: 700,
                                color: 'var(--text-secondary)',
                                fontSize: '0.75rem',
                                marginRight: '0.5rem',
                              }}
                            >
                              {dim.id}
                              {idx + 1}.
                            </span>
                            <span style={{ fontWeight: 700, fontSize: '0.9rem' }}>{c.name}</span>
                          </div>
                          {/* Dropdown */}
                          <select
                            value={score}
                            onChange={(e) => handleScore(c.id, e.target.value)}
                            style={{
                              border: `2px solid ${scored ? getScoreColor(score) : '#d1d5db'}`,
                              borderRadius: '8px',
                              padding: '6px 12px',
                              fontWeight: 700,
                              fontSize: '0.9rem',
                              color: scored ? getScoreColor(score) : 'var(--text-secondary)',
                              background: 'var(--bg-card, #fff)',
                              cursor: 'pointer',
                              minWidth: '120px',
                            }}
                          >
                            <option value={0}>Select score</option>
                            {[1, 2, 3, 4, 5].map((v) => (
                              <option key={v} value={v}>
                                {v} — {getScoreLabel(v)}
                              </option>
                            ))}
                          </select>
                        </div>

                        {/* Score indicators: 1, 3, 5 */}
                        <div
                          style={{
                            display: 'grid',
                            gridTemplateColumns: 'repeat(3, 1fr)',
                            gap: '0.5rem',
                          }}
                        >
                          {[
                            {
                              level: 1,
                              label: 'Score 1',
                              color: '#ef4444',
                              bg: '#fef2f2',
                              border: '#fecaca',
                            },
                            {
                              level: 3,
                              label: 'Score 3',
                              color: '#f59e0b',
                              bg: '#fffbeb',
                              border: '#fde68a',
                            },
                            {
                              level: 5,
                              label: 'Score 5',
                              color: '#3b82f6',
                              bg: '#eff6ff',
                              border: '#bfdbfe',
                            },
                          ].map((lvl) => (
                            <div
                              key={lvl.level}
                              style={{
                                background: score === lvl.level ? lvl.bg : 'transparent',
                                border: `1px solid ${score === lvl.level ? lvl.border : '#e5e7eb'}`,
                                borderRadius: '8px',
                                padding: '0.5rem 0.65rem',
                                transition: 'all 0.2s',
                              }}
                            >
                              <div
                                style={{
                                  fontWeight: 700,
                                  fontSize: '0.7rem',
                                  color: lvl.color,
                                  marginBottom: '0.25rem',
                                  textTransform: 'uppercase',
                                  letterSpacing: '0.04em',
                                }}
                              >
                                {lvl.label}
                              </div>
                              <div
                                style={{
                                  fontSize: '0.78rem',
                                  color: 'var(--text-secondary)',
                                  lineHeight: 1.45,
                                }}
                              >
                                {c.levels[lvl.level]}
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    );
                  })}

                  {/* Architect Deep Guide — steps, recommendations, pitfalls, KPIs */}
                  <DeepGuide type="assessment" dimension={dim.id} />
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* ── Transformation Roadmap ── */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.25rem' }}>Transformation Roadmap</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', marginBottom: '1.25rem' }}>
          Use your assessment score to target the right phase. Start with Phase 1 regardless of
          current maturity.
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
            gap: '1rem',
          }}
        >
          {ROADMAP_PHASES.map((phase) => (
            <div
              key={phase.phase}
              style={{
                background: `${phase.color}10`,
                border: `1px solid ${phase.color}40`,
                borderTop: `4px solid ${phase.color}`,
                borderRadius: '10px',
                padding: '1.25rem',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  marginBottom: '0.5rem',
                }}
              >
                <span style={{ fontWeight: 800, color: phase.color, fontSize: '0.85rem' }}>
                  {phase.phase}
                </span>
                <span style={{ fontWeight: 700, fontSize: '1rem' }}>{phase.name}</span>
              </div>
              <div
                style={{
                  fontSize: '0.75rem',
                  color: 'var(--text-secondary)',
                  fontWeight: 600,
                  marginBottom: '0.75rem',
                  letterSpacing: '0.03em',
                }}
              >
                {phase.horizon}
              </div>
              <ul style={{ margin: 0, paddingLeft: '1.1rem' }}>
                {phase.focus.map((item, i) => (
                  <li
                    key={i}
                    style={{
                      fontSize: '0.82rem',
                      lineHeight: 1.6,
                      color: 'var(--text-primary)',
                      marginBottom: '0.25rem',
                    }}
                  >
                    {item}
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </div>

      {/* ── Example Assessment Card ── */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.25rem' }}>Example Assessment</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', marginBottom: '1rem' }}>
          A mid-size enterprise in the early stages of cloud migration — strong ingestion, weak AI
          and culture.
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))',
            gap: '0.5rem',
            marginBottom: '1rem',
          }}
        >
          {DIMENSIONS.map((dim) => {
            const avg =
              dim.criteria.reduce((acc, c) => acc + (EXAMPLE_SCORES[c.id] || 0), 0) /
              dim.criteria.length;
            return (
              <div
                key={dim.id}
                style={{
                  background: `${dim.color}10`,
                  border: `1px solid ${dim.color}30`,
                  borderLeft: `4px solid ${dim.color}`,
                  borderRadius: '8px',
                  padding: '0.6rem 0.85rem',
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  gap: '0.5rem',
                }}
              >
                <span style={{ fontSize: '0.82rem', fontWeight: 600 }}>{dim.name}</span>
                <span style={{ fontWeight: 800, color: getScoreColor(avg), fontSize: '0.88rem' }}>
                  {avg.toFixed(1)}
                </span>
              </div>
            );
          })}
        </div>
        <div
          style={{
            padding: '0.75rem 1rem',
            background: '#f0fdf4',
            border: '1px solid #bbf7d0',
            borderRadius: '8px',
            fontSize: '0.85rem',
            color: '#14532d',
          }}
        >
          <strong>Overall: 2.35 — Standardized.</strong> Strong infrastructure foundations but AI,
          observability, and data culture require significant investment to reach scalable maturity.
        </div>
        <button
          className="btn btn-secondary btn-sm"
          onClick={loadExample}
          style={{ marginTop: '0.75rem' }}
        >
          Load This Example Into Scorecard
        </button>
      </div>

      {/* ── Interview Answer ── */}
      <div
        className="card"
        style={{
          marginTop: '1rem',
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
          Interview Answer — How I Approach a Maturity Assessment
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
          When I join a new organization, I run a structured data maturity assessment across 10
          dimensions — ingestion, quality, modeling, governance, security, platform, BI, AI
          readiness, observability, and data culture. For each dimension I score 4 criteria from 1
          to 5 using a defined rubric, giving me 40 evidence-based data points. The aggregate score
          tells me whether the organization is Foundational (1–2), Standardized (2.1–3), Scalable
          (3.1–4), or AI-Driven (4.1–5). I use the heatmap to identify the lowest-scoring dimensions
          and prioritize them in a phased roadmap: Phase 1 stabilizes the foundation, Phase 2 scales
          the platform and self-service, and Phase 3 optimizes for AI and ROI. This approach ensures
          I can walk into any executive conversation with objective evidence, a clear gap analysis,
          and a credible transformation plan.
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

export default MaturityAssessment;
