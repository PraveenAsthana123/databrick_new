import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

const challenges = [
  {
    id: 1,
    title: 'No Clear Data Strategy',
    rootCause: 'IT-driven, no business alignment',
    solution: 'Define enterprise data strategy aligned to business outcomes',
    artifacts: 'Data Strategy Document, Vision, Principles',
    interviewAnswer:
      'We defined an enterprise data strategy aligned to business KPIs, ensuring every data initiative had measurable outcomes.',
  },
  {
    id: 2,
    title: 'Business-IT Misalignment',
    rootCause: 'Lack of business involvement',
    solution: 'Build business capability model + use-case-driven roadmap',
    artifacts: 'Capability Model, Use Case Catalog',
    interviewAnswer:
      'We aligned data initiatives to business capabilities and prioritized high-value use cases.',
  },
  {
    id: 3,
    title: 'No Prioritization',
    rootCause: 'No value framework',
    solution: 'Introduce value vs complexity scoring',
    artifacts: 'Prioritization Matrix, Roadmap',
    interviewAnswer:
      'We implemented a value vs complexity model to prioritize initiatives with maximum ROI.',
  },
  {
    id: 4,
    title: 'Poor Enterprise Data Quality',
    rootCause: 'No ownership, no standards',
    solution: 'Define DQ framework + assign ownership',
    artifacts: 'Data Quality Framework, DQ KPIs',
    interviewAnswer:
      'We introduced enterprise DQ standards and ownership, improving trust in data.',
  },
  {
    id: 5,
    title: 'No Data Ownership/Governance',
    rootCause: 'No governance model',
    solution: 'Define roles: Owner, Steward, Custodian',
    artifacts: 'Governance Framework, RACI',
    interviewAnswer: 'We implemented a governance model with clear ownership across domains.',
  },
  {
    id: 6,
    title: 'Data Silos',
    rootCause: 'No shared architecture',
    solution: 'Introduce domain-based architecture (Data Mesh/Fabric)',
    artifacts: 'Domain Architecture, Integration Model',
    interviewAnswer: 'We broke silos using domain-driven data architecture and shared platforms.',
  },
  {
    id: 7,
    title: 'Lack of Standardization',
    rootCause: 'No business glossary',
    solution: 'Create canonical data model + glossary',
    artifacts: 'Data Dictionary, Business Glossary',
    interviewAnswer:
      'We standardized definitions through a business glossary and canonical models.',
  },
  {
    id: 8,
    title: 'No Platform Strategy',
    rootCause: 'Tool-driven decisions',
    solution: 'Define target architecture (Lakehouse/Cloud)',
    artifacts: 'Reference Architecture, Platform Strategy',
    interviewAnswer:
      'We consolidated tools into a unified lakehouse architecture to reduce complexity and cost.',
  },
  {
    id: 9,
    title: 'High Cost Low ROI',
    rootCause: 'No ROI tracking',
    solution: 'Introduce value realization framework',
    artifacts: 'KPI Dashboard, Value Model',
    interviewAnswer: 'We tracked ROI through measurable KPIs and optimized cost using FinOps.',
  },
  {
    id: 10,
    title: 'No Governance Framework',
    rootCause: 'Governance is reactive',
    solution: 'Build governance operating model',
    artifacts: 'Governance Model, Policies',
    interviewAnswer:
      'We created a proactive governance framework covering access, quality, and compliance.',
  },
  {
    id: 11,
    title: 'Regulatory/Compliance Risk',
    rootCause: 'No compliance integration',
    solution: 'Embed compliance into pipelines',
    artifacts: 'Compliance Framework, Audit Model',
    interviewAnswer: 'We integrated compliance into pipelines with auditability and controls.',
  },
  {
    id: 12,
    title: 'Data Security & PII',
    rootCause: 'No classification or masking',
    solution: 'PII discovery + masking + RBAC',
    artifacts: 'Security Framework, Data Classification',
    interviewAnswer:
      'We secured sensitive data using classification, masking, and access controls.',
  },
  {
    id: 13,
    title: 'Lack of Data Culture',
    rootCause: 'No awareness or training',
    solution: 'Build data literacy program',
    artifacts: 'Training Plan, Adoption Metrics',
    interviewAnswer: 'We drove adoption through data literacy programs and self-service analytics.',
  },
  {
    id: 14,
    title: 'No Self-Service Analytics',
    rootCause: 'Complex data access',
    solution: 'Enable curated Gold layer + BI',
    artifacts: 'Data Mart Strategy, Semantic Layer',
    interviewAnswer:
      'We enabled self-service analytics using curated datasets and semantic models.',
  },
  {
    id: 15,
    title: 'Legacy System Constraints',
    rootCause: 'Tight coupling',
    solution: 'Modernization roadmap',
    artifacts: 'Migration Plan, Target Architecture',
    interviewAnswer: 'We modernized legacy systems incrementally using a cloud-first approach.',
  },
  {
    id: 16,
    title: 'No Real-Time Capability',
    rootCause: 'Outdated architecture',
    solution: 'Introduce streaming architecture',
    artifacts: 'Streaming Strategy, Event Architecture',
    interviewAnswer:
      'We introduced real-time pipelines for critical use cases like fraud detection.',
  },
  {
    id: 17,
    title: 'AI/GenAI Not Delivering',
    rootCause: 'Poor data foundation',
    solution: 'Build AI-ready data platform',
    artifacts: 'AI Strategy, RAG Architecture',
    interviewAnswer:
      'We improved AI outcomes by building a strong data foundation and RAG pipelines.',
  },
  {
    id: 18,
    title: 'No Data Product Thinking',
    rootCause: 'No ownership or lifecycle',
    solution: 'Introduce data product model',
    artifacts: 'Data Product Catalog, SLAs',
    interviewAnswer: 'We shifted to data-as-a-product with ownership and SLAs.',
  },
  {
    id: 19,
    title: 'No Observability Strategy',
    rootCause: 'No monitoring framework',
    solution: 'Implement observability strategy',
    artifacts: 'Monitoring Framework, Dashboards',
    interviewAnswer: 'We implemented observability across pipelines, data quality, and usage.',
  },
  {
    id: 20,
    title: 'No Roadmap/Execution',
    rootCause: 'No phased plan',
    solution: 'Build transformation roadmap',
    artifacts: '12-24 Month Roadmap',
    interviewAnswer: 'We created a phased roadmap with quick wins and long-term transformation.',
  },
];

const strategyLayers = [
  { name: 'Vision', color: '#6366f1' },
  { name: 'Business Alignment', color: '#8b5cf6' },
  { name: 'Architecture', color: '#3b82f6' },
  { name: 'Governance', color: '#0ea5e9' },
  { name: 'Data Quality', color: '#10b981' },
  { name: 'Security', color: '#ef4444' },
  { name: 'AI Readiness', color: '#f59e0b' },
  { name: 'Operating Model', color: '#f97316' },
  { name: 'Roadmap', color: '#ec4899' },
  { name: 'Value Realization', color: '#14b8a6' },
];

function DataStrategy() {
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const filtered = challenges.filter((c) => {
    const q = searchTerm.toLowerCase();
    return (
      c.title.toLowerCase().includes(q) ||
      c.rootCause.toLowerCase().includes(q) ||
      c.solution.toLowerCase().includes(q) ||
      c.artifacts.toLowerCase().includes(q)
    );
  });

  const downloadCSV = () => {
    exportToCSV(
      challenges.map((c) => ({
        id: c.id,
        title: c.title,
        rootCause: c.rootCause,
        solution: c.solution,
        artifacts: c.artifacts,
        interviewAnswer: c.interviewAnswer,
      })),
      'data-strategy-challenges.csv'
    );
  };

  return (
    <div>
      {/* Page Header */}
      <div className="page-header">
        <div>
          <h1>Data Strategy &mdash; Challenges &amp; Solutions</h1>
          <p>
            20 enterprise data strategy challenges with root cause, solution, artifacts, and
            interview answers
          </p>
        </div>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F4CB;</div>
          <div className="stat-info">
            <h4>20</h4>
            <p>Challenges</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#x1F9E9;</div>
          <div className="stat-info">
            <h4>10</h4>
            <p>Strategy Layers</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#x1F3AF;</div>
          <div className="stat-info">
            <h4>{filtered.length}</h4>
            <p>Showing</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#x1F4A1;</div>
          <div className="stat-info">
            <h4>20</h4>
            <p>Interview Answers</p>
          </div>
        </div>
      </div>

      {/* Search + CSV */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search challenges..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '320px' }}
          />
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadCSV}
            style={{ marginLeft: 'auto' }}
          >
            Download CSV
          </button>
        </div>
      </div>

      {/* Empty state */}
      {filtered.length === 0 && (
        <div
          className="card"
          style={{ textAlign: 'center', color: 'var(--text-secondary)', padding: '2rem' }}
        >
          No challenges match your search. Try a different keyword.
        </div>
      )}

      {/* Challenge Cards */}
      {filtered.map((c) => {
        const isExpanded = expandedId === c.id;
        return (
          <div key={c.id} className="card" style={{ marginBottom: '0.75rem' }}>
            {/* Collapsed header row */}
            <div
              onClick={() => setExpandedId(isExpanded ? null : c.id)}
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
                    gap: '0.5rem',
                    flexWrap: 'wrap',
                    marginBottom: '0.25rem',
                  }}
                >
                  <span className="badge running" style={{ minWidth: '2rem', textAlign: 'center' }}>
                    #{c.id}
                  </span>
                  <strong>{c.title}</strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', margin: 0 }}>
                  {c.rootCause}
                </p>
              </div>
              <span style={{ color: 'var(--text-secondary)', flexShrink: 0, marginTop: '2px' }}>
                {isExpanded ? '\u25BC' : '\u25B6'}
              </span>
            </div>

            {/* Expanded detail grid */}
            {isExpanded && (
              <div
                style={{
                  marginTop: '1.25rem',
                  display: 'grid',
                  gridTemplateColumns: '1fr 1fr',
                  gap: '1rem',
                }}
              >
                {/* Root Cause */}
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
                    Root Cause
                  </div>
                  <p
                    style={{ margin: 0, fontSize: '0.875rem', lineHeight: 1.55, color: '#7f1d1d' }}
                  >
                    {c.rootCause}
                  </p>
                </div>

                {/* Solution */}
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
                      marginBottom: '0.5rem',
                    }}
                  >
                    Solution
                  </div>
                  <p
                    style={{ margin: 0, fontSize: '0.875rem', lineHeight: 1.55, color: '#14532d' }}
                  >
                    {c.solution}
                  </p>
                </div>

                {/* Key Artifacts */}
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
                    Key Artifacts
                  </div>
                  <p
                    style={{
                      margin: 0,
                      fontSize: '0.875rem',
                      lineHeight: 1.55,
                      color: '#1e3a5f',
                      fontFamily: 'monospace',
                    }}
                  >
                    {c.artifacts}
                  </p>
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
                    {c.interviewAnswer}
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

      {/* Strategy Framework Section */}
      <div className="card" style={{ marginTop: '2rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>Strategy Framework</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1.25rem', fontSize: '0.9rem' }}>
          The 10 layers of an enterprise data strategy, from vision to value realization.
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))',
            gap: '0.75rem',
          }}
        >
          {strategyLayers.map((layer, idx) => (
            <div
              key={layer.name}
              style={{
                background: `${layer.color}15`,
                border: `1px solid ${layer.color}40`,
                borderLeft: `4px solid ${layer.color}`,
                borderRadius: '8px',
                padding: '0.75rem 1rem',
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem',
              }}
            >
              <span
                style={{
                  fontWeight: 700,
                  fontSize: '0.8rem',
                  color: layer.color,
                  minWidth: '1.4rem',
                }}
              >
                {idx + 1}.
              </span>
              <span style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--text-primary)' }}>
                {layer.name}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* Interview Summary Card */}
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
          A successful enterprise data strategy starts with business alignment, not technology. We
          defined a vision tied to business KPIs, built a governance model with clear ownership,
          standardized our data through canonical models and a business glossary, and consolidated
          onto a unified lakehouse platform. We drove data quality through frameworks and
          accountability, embedded compliance and security from day one, and built a data literacy
          culture to enable self-service. We modernized legacy constraints, introduced real-time and
          AI-ready capabilities, adopted data-as-a-product thinking, and delivered everything
          through a phased roadmap with measurable ROI at every stage.
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

export default DataStrategy;
