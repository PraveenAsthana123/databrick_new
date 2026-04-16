import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

const maturityLevels = [
  {
    level: 1,
    name: 'Ad Hoc',
    focus: 'Chaos',
    color: '#ef4444',
    gradientFrom: '#fef2f2',
    gradientTo: '#fee2e2',
    borderColor: '#fecaca',
    textColor: '#7f1d1d',
    labelColor: '#dc2626',
    badgeBg: '#ef4444',
    areas: {
      data: 'Siloed inconsistent',
      pipelines: 'Manual fragile',
      governance: 'None',
      bi: 'Excel-driven',
      ai: 'Not possible',
    },
    challenges: [
      {
        challenge: 'Data silos',
        rootCause: 'No central platform',
        solution: 'Build centralized storage',
      },
      {
        challenge: 'Manual processing',
        rootCause: 'No automation',
        solution: 'Introduce ETL pipelines',
      },
      {
        challenge: 'No trust in data',
        rootCause: 'No validation',
        solution: 'Basic data quality checks',
      },
      {
        challenge: 'No ownership',
        rootCause: 'No governance',
        solution: 'Assign data owners',
      },
    ],
    artifacts: ['Basic data inventory', 'Initial ingestion pipelines', 'Data ownership list'],
    interviewAnswer:
      'At this stage, the focus is on centralizing data and eliminating manual processes.',
  },
  {
    level: 2,
    name: 'Managed',
    focus: 'Foundation',
    color: '#f97316',
    gradientFrom: '#fff7ed',
    gradientTo: '#ffedd5',
    borderColor: '#fed7aa',
    textColor: '#7c2d12',
    labelColor: '#ea580c',
    badgeBg: '#f97316',
    areas: {
      data: 'Centralized lake/warehouse',
      pipelines: 'Batch ETL',
      governance: 'Minimal',
      bi: 'Basic dashboards',
      ai: 'Limited',
    },
    challenges: [
      {
        challenge: 'Poor data quality',
        rootCause: 'No standards',
        solution: 'Introduce DQ framework',
      },
      {
        challenge: 'Pipeline failures',
        rootCause: 'Weak orchestration',
        solution: 'Use Airflow/Workflows',
      },
      {
        challenge: 'Duplicate data',
        rootCause: 'No modeling',
        solution: 'Define Bronze/Silver layers',
      },
      {
        challenge: 'Limited scalability',
        rootCause: 'Basic infra',
        solution: 'Move to cloud/lakehouse',
      },
    ],
    artifacts: ['Data platform architecture', 'ETL pipeline framework', 'Data quality rules'],
    interviewAnswer:
      'We established foundational pipelines, centralized data, and introduced basic quality controls.',
  },
  {
    level: 3,
    name: 'Defined',
    focus: 'Standardization',
    color: '#eab308',
    gradientFrom: '#fefce8',
    gradientTo: '#fef9c3',
    borderColor: '#fde68a',
    textColor: '#713f12',
    labelColor: '#ca8a04',
    badgeBg: '#eab308',
    areas: {
      data: 'Structured standardized',
      pipelines: 'ELT + orchestration',
      governance: 'Defined policies',
      bi: 'Self-service',
      ai: 'Early ML',
    },
    challenges: [
      {
        challenge: 'Inconsistent definitions',
        rootCause: 'No glossary',
        solution: 'Business glossary + canonical model',
      },
      {
        challenge: 'Governance gaps',
        rootCause: 'Not enforced',
        solution: 'Implement governance framework',
      },
      {
        challenge: 'Data duplication',
        rootCause: 'No reuse',
        solution: 'Data marts + conformed layers',
      },
      {
        challenge: 'Limited discoverability',
        rootCause: 'No catalog',
        solution: 'Implement data catalog',
      },
    ],
    artifacts: [
      'Data governance framework',
      'Business glossary',
      'Canonical data model',
      'Data catalog',
    ],
    interviewAnswer:
      'We standardized data definitions, implemented governance, and enabled self-service analytics.',
  },
  {
    level: 4,
    name: 'Advanced',
    focus: 'Enterprise Scale',
    color: '#22c55e',
    gradientFrom: '#f0fdf4',
    gradientTo: '#dcfce7',
    borderColor: '#bbf7d0',
    textColor: '#14532d',
    labelColor: '#16a34a',
    badgeBg: '#22c55e',
    areas: {
      data: 'Trusted governed',
      pipelines: 'Scalable automated',
      governance: 'Enforced Unity Catalog',
      bi: 'Enterprise-wide',
      ai: 'Production ML',
    },
    challenges: [
      {
        challenge: 'Scaling issues',
        rootCause: 'Growing volume',
        solution: 'Optimize pipelines partitioning',
      },
      {
        challenge: 'Governance complexity',
        rootCause: 'Multi-domain',
        solution: 'Domain-based governance Data Mesh',
      },
      {
        challenge: 'Security risks',
        rootCause: 'Sensitive data',
        solution: 'PII masking RBAC/ABAC',
      },
      {
        challenge: 'High cost',
        rootCause: 'Inefficient usage',
        solution: 'FinOps optimization',
      },
    ],
    artifacts: [
      'Enterprise data architecture',
      'Unity Catalog governance model',
      'Data security framework',
      'FinOps dashboard',
    ],
    interviewAnswer:
      'We scaled the platform, enforced governance, and optimized performance and cost across domains.',
  },
  {
    level: 5,
    name: 'Optimized',
    focus: 'AI-Driven Enterprise',
    color: '#3b82f6',
    gradientFrom: '#eff6ff',
    gradientTo: '#dbeafe',
    borderColor: '#bfdbfe',
    textColor: '#1e3a5f',
    labelColor: '#1d4ed8',
    badgeBg: '#3b82f6',
    areas: {
      data: 'Productized real-time',
      pipelines: 'Event-driven AI-assisted',
      governance: 'Automated',
      bi: 'Predictive',
      ai: 'GenAI/RAG/Agents',
    },
    challenges: [
      {
        challenge: 'AI not trusted',
        rootCause: 'Weak data foundation',
        solution: 'Build RAG + explainability',
      },
      {
        challenge: 'Data latency',
        rootCause: 'Batch-heavy',
        solution: 'Introduce streaming',
      },
      {
        challenge: 'No feedback loop',
        rootCause: 'Static systems',
        solution: 'Continuous learning',
      },
      {
        challenge: 'Complex ecosystem',
        rootCause: 'Multi-cloud AI',
        solution: 'Unified governance + orchestration',
      },
    ],
    artifacts: [
      'AI strategy & RAG architecture',
      'Data product catalog',
      'Real-time architecture',
      'AI governance framework',
    ],
    interviewAnswer:
      'We transformed data into products, enabled real-time AI, and implemented continuous learning systems.',
  },
];

const capabilityMatrix = [
  {
    capability: 'Ingestion',
    l1: 'Manual / ad hoc',
    l2: 'Batch ETL',
    l3: 'ELT + orchestration',
    l4: 'Scalable automated',
    l5: 'Event-driven streaming',
  },
  {
    capability: 'Quality',
    l1: 'None',
    l2: 'Basic rules',
    l3: 'DQ framework',
    l4: 'Automated enforcement',
    l5: 'AI-driven monitoring',
  },
  {
    capability: 'Governance',
    l1: 'None',
    l2: 'Minimal',
    l3: 'Defined policies',
    l4: 'Unity Catalog enforced',
    l5: 'Automated + mesh',
  },
  {
    capability: 'Architecture',
    l1: 'Siloed systems',
    l2: 'Lake / warehouse',
    l3: 'Lakehouse + layers',
    l4: 'Enterprise lakehouse',
    l5: 'Data mesh + fabric',
  },
  {
    capability: 'BI',
    l1: 'Excel / spreadsheets',
    l2: 'Basic dashboards',
    l3: 'Self-service BI',
    l4: 'Enterprise-wide',
    l5: 'Predictive analytics',
  },
  {
    capability: 'AI',
    l1: 'Not possible',
    l2: 'Limited experiments',
    l3: 'Early ML models',
    l4: 'Production ML',
    l5: 'GenAI / RAG / Agents',
  },
];

const assessmentQuestions = [
  'Where does your data currently live — in spreadsheets, siloed databases, or a centralized platform?',
  'Do you have automated data pipelines, or are processes largely manual?',
  'Is there a defined data governance framework with assigned data owners and stewards?',
  'Do business users have self-service access to trusted, curated data?',
  'Are there established data quality standards, rules, and monitoring in place?',
  'Is your organization running ML or AI models in production — or still experimenting?',
  'Do you treat data as a product with defined SLAs, ownership, and discoverability?',
];

const industryDistribution = [
  { label: 'Level 1–2 (Ad Hoc / Managed)', pct: 50, color: '#ef4444' },
  { label: 'Level 3 (Defined)', pct: 30, color: '#eab308' },
  { label: 'Level 4 (Advanced)', pct: 15, color: '#22c55e' },
  { label: 'Level 5 (Optimized)', pct: 5, color: '#3b82f6' },
];

const levelColors = {
  1: '#ef4444',
  2: '#f97316',
  3: '#eab308',
  4: '#22c55e',
  5: '#3b82f6',
};

function DataMaturityModel() {
  const [expandedLevel, setExpandedLevel] = useState(null);

  const allChallenges = maturityLevels.flatMap((lvl) =>
    lvl.challenges.map((c) => ({
      level: lvl.level,
      levelName: lvl.name,
      challenge: c.challenge,
      rootCause: c.rootCause,
      solution: c.solution,
    }))
  );

  const downloadCSV = () => {
    exportToCSV(allChallenges, 'data-maturity-challenges.csv');
  };

  return (
    <div>
      {/* Page Header */}
      <div className="page-header">
        <div>
          <h1>Data Maturity Model</h1>
          <p>
            5 maturity levels &mdash; Ad Hoc to Optimized &mdash; with challenges, solutions, and
            interview positioning
          </p>
        </div>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F4CA;</div>
          <div className="stat-info">
            <h4>5</h4>
            <p>Maturity Levels</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon red">&#x26A0;</div>
          <div className="stat-info">
            <h4>20</h4>
            <p>Challenges</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#x1F9E9;</div>
          <div className="stat-info">
            <h4>6</h4>
            <p>Capability Dimensions</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#x1F4AC;</div>
          <div className="stat-info">
            <h4>5</h4>
            <p>Interview Answers</p>
          </div>
        </div>
      </div>

      {/* Maturity Progression Flow */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>Maturity Progression</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', marginBottom: '1.25rem' }}>
          The journey from chaos to an AI-driven enterprise data platform.
        </p>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: '0.5rem',
          }}
        >
          {maturityLevels.map((lvl, idx) => (
            <React.Fragment key={lvl.level}>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  gap: '0.35rem',
                }}
              >
                <div
                  style={{
                    width: '2.5rem',
                    height: '2.5rem',
                    borderRadius: '50%',
                    background: lvl.badgeBg,
                    color: '#fff',
                    fontWeight: 700,
                    fontSize: '1rem',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    boxShadow: `0 2px 8px ${lvl.badgeBg}50`,
                  }}
                >
                  {lvl.level}
                </div>
                <div style={{ fontSize: '0.75rem', fontWeight: 600, color: lvl.color }}>
                  {lvl.name}
                </div>
                <div
                  style={{
                    fontSize: '0.65rem',
                    color: 'var(--text-secondary)',
                    textAlign: 'center',
                    maxWidth: '80px',
                  }}
                >
                  {lvl.focus}
                </div>
              </div>
              {idx < maturityLevels.length - 1 && (
                <div
                  style={{
                    fontSize: '1.4rem',
                    color: 'var(--text-secondary)',
                    flexShrink: 0,
                    marginBottom: '1.2rem',
                  }}
                >
                  &#x2192;
                </div>
              )}
            </React.Fragment>
          ))}
        </div>
      </div>

      {/* CSV Download */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <span style={{ fontSize: '0.9rem', color: 'var(--text-secondary)' }}>
            20 challenges across all maturity levels
          </span>
          <button className="btn btn-secondary btn-sm" onClick={downloadCSV}>
            Download CSV
          </button>
        </div>
      </div>

      {/* Maturity Level Cards */}
      {maturityLevels.map((lvl) => {
        const isExpanded = expandedLevel === lvl.level;
        return (
          <div
            key={lvl.level}
            className="card"
            style={{
              marginBottom: '0.75rem',
              borderLeft: `5px solid ${lvl.color}`,
            }}
          >
            {/* Collapsed Header */}
            <div
              onClick={() => setExpandedLevel(isExpanded ? null : lvl.level)}
              style={{
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                gap: '1rem',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <div
                  style={{
                    width: '2.75rem',
                    height: '2.75rem',
                    borderRadius: '50%',
                    background: lvl.badgeBg,
                    color: '#fff',
                    fontWeight: 800,
                    fontSize: '1.1rem',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    flexShrink: 0,
                    boxShadow: `0 3px 10px ${lvl.badgeBg}55`,
                  }}
                >
                  {lvl.level}
                </div>
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <strong style={{ fontSize: '1.05rem' }}>
                      Level {lvl.level} &mdash; {lvl.name}
                    </strong>
                    <span
                      style={{
                        background: `${lvl.color}20`,
                        color: lvl.color,
                        border: `1px solid ${lvl.color}40`,
                        borderRadius: '12px',
                        padding: '0.15rem 0.6rem',
                        fontSize: '0.7rem',
                        fontWeight: 600,
                      }}
                    >
                      {lvl.focus}
                    </span>
                  </div>
                  <div
                    style={{
                      display: 'flex',
                      gap: '1rem',
                      marginTop: '0.3rem',
                      flexWrap: 'wrap',
                    }}
                  >
                    {Object.entries(lvl.areas).map(([key, val]) => (
                      <span
                        key={key}
                        style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}
                      >
                        <strong style={{ color: 'var(--text-primary)' }}>
                          {key.charAt(0).toUpperCase() + key.slice(1)}:
                        </strong>{' '}
                        {val}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
              <span style={{ color: 'var(--text-secondary)', flexShrink: 0 }}>
                {isExpanded ? '\u25BC' : '\u25B6'}
              </span>
            </div>

            {/* Expanded Content */}
            {isExpanded && (
              <div style={{ marginTop: '1.5rem' }}>
                {/* Areas Table */}
                <div style={{ marginBottom: '1.25rem' }}>
                  <div
                    style={{
                      fontSize: '0.7rem',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.08em',
                      color: lvl.labelColor,
                      marginBottom: '0.6rem',
                    }}
                  >
                    Capability Areas
                  </div>
                  <div style={{ overflowX: 'auto' }}>
                    <table
                      style={{
                        width: '100%',
                        borderCollapse: 'collapse',
                        fontSize: '0.85rem',
                      }}
                    >
                      <thead>
                        <tr
                          style={{
                            background: `${lvl.color}12`,
                            borderBottom: `2px solid ${lvl.color}30`,
                          }}
                        >
                          {['Data', 'Pipelines', 'Governance', 'BI', 'AI'].map((h) => (
                            <th
                              key={h}
                              style={{
                                padding: '0.5rem 0.75rem',
                                textAlign: 'left',
                                fontWeight: 700,
                                color: lvl.labelColor,
                                fontSize: '0.75rem',
                              }}
                            >
                              {h}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        <tr>
                          {Object.values(lvl.areas).map((val, i) => (
                            <td
                              key={i}
                              style={{
                                padding: '0.5rem 0.75rem',
                                color: lvl.textColor,
                                borderBottom: `1px solid ${lvl.color}20`,
                              }}
                            >
                              {val}
                            </td>
                          ))}
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Challenges Table */}
                <div style={{ marginBottom: '1.25rem' }}>
                  <div
                    style={{
                      fontSize: '0.7rem',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.08em',
                      color: lvl.labelColor,
                      marginBottom: '0.6rem',
                    }}
                  >
                    Challenges &amp; Solutions
                  </div>
                  <div style={{ overflowX: 'auto' }}>
                    <table
                      style={{
                        width: '100%',
                        borderCollapse: 'collapse',
                        fontSize: '0.85rem',
                      }}
                    >
                      <thead>
                        <tr
                          style={{
                            background: `${lvl.color}12`,
                            borderBottom: `2px solid ${lvl.color}30`,
                          }}
                        >
                          {['#', 'Challenge', 'Root Cause', 'Solution'].map((h) => (
                            <th
                              key={h}
                              style={{
                                padding: '0.5rem 0.75rem',
                                textAlign: 'left',
                                fontWeight: 700,
                                color: lvl.labelColor,
                                fontSize: '0.75rem',
                              }}
                            >
                              {h}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {lvl.challenges.map((c, idx) => (
                          <tr
                            key={idx}
                            style={{
                              borderBottom: `1px solid ${lvl.color}15`,
                              background: idx % 2 === 0 ? 'transparent' : `${lvl.color}06`,
                            }}
                          >
                            <td
                              style={{
                                padding: '0.5rem 0.75rem',
                                color: lvl.color,
                                fontWeight: 700,
                                fontSize: '0.75rem',
                              }}
                            >
                              {idx + 1}
                            </td>
                            <td
                              style={{
                                padding: '0.5rem 0.75rem',
                                fontWeight: 600,
                                color: 'var(--text-primary)',
                              }}
                            >
                              {c.challenge}
                            </td>
                            <td
                              style={{
                                padding: '0.5rem 0.75rem',
                                color: '#dc2626',
                                fontSize: '0.82rem',
                              }}
                            >
                              {c.rootCause}
                            </td>
                            <td
                              style={{
                                padding: '0.5rem 0.75rem',
                                color: '#16a34a',
                                fontSize: '0.82rem',
                              }}
                            >
                              {c.solution}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Artifacts + Interview in 2-col grid */}
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr 1fr',
                    gap: '1rem',
                  }}
                >
                  {/* Artifacts */}
                  <div
                    style={{
                      background: `linear-gradient(135deg, ${lvl.gradientFrom} 0%, ${lvl.gradientTo} 100%)`,
                      border: `1px solid ${lvl.borderColor}`,
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
                        color: lvl.labelColor,
                        marginBottom: '0.6rem',
                      }}
                    >
                      Key Artifacts
                    </div>
                    <ul style={{ margin: 0, paddingLeft: '1.1rem' }}>
                      {lvl.artifacts.map((a, i) => (
                        <li
                          key={i}
                          style={{
                            fontSize: '0.85rem',
                            color: lvl.textColor,
                            marginBottom: '0.3rem',
                            fontFamily: 'monospace',
                          }}
                        >
                          {a}
                        </li>
                      ))}
                    </ul>
                  </div>

                  {/* Interview Answer */}
                  <div
                    style={{
                      background: 'linear-gradient(135deg, #faf5ff 0%, #ede9fe 100%)',
                      border: '1px solid #ddd6fe',
                      borderRadius: '10px',
                      padding: '1rem',
                      display: 'flex',
                      flexDirection: 'column',
                      justifyContent: 'center',
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
              </div>
            )}
          </div>
        );
      })}

      {/* Capability Matrix */}
      <div className="card" style={{ marginTop: '2rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>Capability Matrix</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', marginBottom: '1.25rem' }}>
          How each core capability evolves across the 5 maturity levels.
        </p>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.84rem' }}>
            <thead>
              <tr style={{ background: 'var(--bg-secondary, #f8fafc)' }}>
                <th
                  style={{
                    padding: '0.6rem 0.75rem',
                    textAlign: 'left',
                    fontWeight: 700,
                    color: 'var(--text-primary)',
                    fontSize: '0.8rem',
                    borderBottom: '2px solid var(--border-color, #e2e8f0)',
                    minWidth: '110px',
                  }}
                >
                  Capability
                </th>
                {maturityLevels.map((lvl) => (
                  <th
                    key={lvl.level}
                    style={{
                      padding: '0.6rem 0.75rem',
                      textAlign: 'left',
                      fontWeight: 700,
                      color: lvl.color,
                      fontSize: '0.8rem',
                      borderBottom: `2px solid ${lvl.color}50`,
                    }}
                  >
                    L{lvl.level} {lvl.name}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {capabilityMatrix.map((row, idx) => (
                <tr
                  key={row.capability}
                  style={{
                    borderBottom: '1px solid var(--border-color, #e2e8f0)',
                    background: idx % 2 === 0 ? 'transparent' : 'var(--bg-secondary, #f8fafc)',
                  }}
                >
                  <td
                    style={{
                      padding: '0.55rem 0.75rem',
                      fontWeight: 700,
                      color: 'var(--text-primary)',
                    }}
                  >
                    {row.capability}
                  </td>
                  {[row.l1, row.l2, row.l3, row.l4, row.l5].map((val, i) => (
                    <td
                      key={i}
                      style={{
                        padding: '0.55rem 0.75rem',
                        color: 'var(--text-secondary)',
                        fontSize: '0.82rem',
                        borderLeft: `3px solid ${levelColors[i + 1]}25`,
                      }}
                    >
                      {val}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Assessment Questions */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>Assessment Questions</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', marginBottom: '1.25rem' }}>
          Use these questions to assess your organization's current maturity level.
        </p>
        <ol style={{ margin: 0, paddingLeft: '1.4rem' }}>
          {assessmentQuestions.map((q, i) => (
            <li
              key={i}
              style={{
                fontSize: '0.9rem',
                color: 'var(--text-primary)',
                marginBottom: '0.75rem',
                lineHeight: 1.6,
              }}
            >
              {q}
            </li>
          ))}
        </ol>
      </div>

      {/* Industry Distribution */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>Industry Distribution</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', marginBottom: '1.25rem' }}>
          Approximate distribution of organizations across maturity levels.
        </p>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
          {industryDistribution.map((item) => (
            <div key={item.label}>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  marginBottom: '0.3rem',
                  fontSize: '0.85rem',
                }}
              >
                <span style={{ fontWeight: 600, color: 'var(--text-primary)' }}>{item.label}</span>
                <span style={{ fontWeight: 700, color: item.color }}>~{item.pct}%</span>
              </div>
              <div
                style={{
                  height: '10px',
                  background: 'var(--border-color, #e2e8f0)',
                  borderRadius: '6px',
                  overflow: 'hidden',
                }}
              >
                <div
                  style={{
                    height: '100%',
                    width: `${item.pct}%`,
                    background: item.color,
                    borderRadius: '6px',
                    transition: 'width 0.4s ease',
                  }}
                />
              </div>
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
          Interview Summary &mdash; Maturity Journey
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
          We started at Level 1 — data was siloed, manual, and untrusted. We built a centralized
          data platform, introduced ETL pipelines, and assigned data ownership. As we matured, we
          standardized definitions through a business glossary and canonical model, enforced
          governance via Unity Catalog, and enabled self-service analytics. At enterprise scale, we
          optimized for performance and cost using FinOps and domain-based governance. Today, we
          treat data as products, run GenAI and RAG pipelines, and implement continuous learning
          with real-time streaming — a full transformation from chaos to an AI-driven data
          enterprise.
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

export default DataMaturityModel;
