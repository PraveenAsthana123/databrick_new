import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

const categories = [
  {
    id: 'A',
    name: 'Compute Cost',
    color: '#ef4444',
    challenges: [
      {
        id: 1,
        title: 'High Cluster Cost',
        rootCause: 'Always-on oversized clusters',
        solution: 'Autoscaling + job clusters',
        artifacts: 'Cluster Policy, Autoscaling Config, Job Cluster Templates',
        interviewAnswer:
          'We replaced always-on all-purpose clusters with autoscaling job clusters and implemented cluster policies to enforce size limits, reducing compute spend by over 40%.',
      },
      {
        id: 2,
        title: 'Inefficient Queries',
        rootCause: 'Full table scans and bad joins',
        solution: 'Query tuning + partition pruning',
        artifacts: 'Query Optimization Guide, Partition Strategy, Spark UI Analysis',
        interviewAnswer:
          'We analyzed Spark query plans, introduced partition pruning, rewrote broadcast joins, and eliminated unnecessary full scans — significantly improving query performance and reducing cost.',
      },
      {
        id: 3,
        title: 'Streaming Always-On',
        rootCause: '24/7 compute for non-critical streams',
        solution: 'Micro-batch for non-critical pipelines',
        artifacts: 'Streaming Classification Matrix, Micro-batch Config, Cost Comparison Report',
        interviewAnswer:
          'We audited all streaming pipelines and converted non-latency-sensitive ones to micro-batch mode, reducing continuous compute hours by 60% without impacting business SLAs.',
      },
      {
        id: 4,
        title: 'Concurrency Contention',
        rootCause: 'Shared clusters causing queue delays',
        solution: 'Workload isolation with dedicated pools',
        artifacts: 'Workload Isolation Design, Pool Configuration, SLA Tiering Model',
        interviewAnswer:
          'We introduced workload isolation by separating critical, standard, and ad-hoc workloads into dedicated pools, eliminating contention and improving throughput reliability.',
      },
    ],
  },
  {
    id: 'B',
    name: 'Storage Cost',
    color: '#f59e0b',
    challenges: [
      {
        id: 5,
        title: 'Data Duplication',
        rootCause: 'Multiple teams storing redundant copies',
        solution: 'Certified datasets + reuse model',
        artifacts: 'Data Catalog, Certified Dataset Registry, Reuse Policy',
        interviewAnswer:
          'We introduced a certified dataset layer in the Gold zone and mandated reuse over recreation, eliminating redundant copies and reducing storage costs by 30%.',
      },
      {
        id: 6,
        title: 'Unlimited Retention',
        rootCause: 'No lifecycle policy or data expiry rules',
        solution: 'Tiering + retention rules per data domain',
        artifacts: 'Retention Policy, Lifecycle Rules, Storage Tier Classification',
        interviewAnswer:
          'We defined retention policies per domain — hot, warm, cold, and archive tiers — and automated lifecycle transitions, cutting storage costs substantially.',
      },
      {
        id: 7,
        title: 'Small Files Problem',
        rootCause: 'Frequent tiny writes creating thousands of files',
        solution: 'Compaction jobs using Delta OPTIMIZE',
        artifacts: 'Compaction Schedule, OPTIMIZE Scripts, File Size Monitoring Dashboard',
        interviewAnswer:
          'We scheduled Delta OPTIMIZE jobs to compact small files, improving read performance and reducing metadata overhead that was inflating storage and query costs.',
      },
      {
        id: 8,
        title: 'Unused Datasets',
        rootCause: 'No ownership or access tracking',
        solution: 'Ownership assignment + usage-based cleanup',
        artifacts: 'Dataset Ownership Register, Usage Tracking Dashboard, Cleanup SOP',
        interviewAnswer:
          'We tracked dataset access patterns using Unity Catalog audit logs, assigned owners to every dataset, and implemented a cleanup process for unused data older than 90 days.',
      },
    ],
  },
  {
    id: 'C',
    name: 'Data Movement',
    color: '#3b82f6',
    challenges: [
      {
        id: 9,
        title: 'Excessive Ingestion',
        rootCause: 'Redundant full loads instead of incremental',
        solution: 'Incremental ingestion with watermarks',
        artifacts: 'Incremental Ingestion Design, Watermark Config, Ingestion Cost Report',
        interviewAnswer:
          'We replaced full-load ingestion patterns with incremental loads using watermarks and CDC, reducing ingestion volume and associated compute and storage costs dramatically.',
      },
      {
        id: 10,
        title: 'Cross-Region Traffic',
        rootCause: 'Poor architecture placing compute far from data',
        solution: 'Data locality — co-locate compute and storage',
        artifacts: 'Region Architecture Review, Data Locality Design, Network Cost Analysis',
        interviewAnswer:
          'We audited cross-region data flows and co-located compute with storage, eliminating unnecessary cross-region transfer charges that had been invisible in our cost breakdown.',
      },
      {
        id: 11,
        title: 'API Overuse',
        rootCause: 'Inefficient per-record API extraction',
        solution: 'Batch windows + response caching',
        artifacts: 'API Batch Design, Caching Strategy, API Cost Dashboard',
        interviewAnswer:
          'We restructured API-based ingestion to use bulk endpoints and batch windows, and introduced caching for frequently requested reference data, reducing API call volume by 70%.',
      },
      {
        id: 12,
        title: 'Egress Cost',
        rootCause: 'Uncontrolled external data sharing',
        solution: 'Optimize data sharing with Delta Sharing',
        artifacts: 'Data Sharing Policy, Delta Sharing Config, Egress Cost Report',
        interviewAnswer:
          'We replaced file-based external transfers with Delta Sharing, providing direct access to live data without egress, and introduced governance controls to prevent unauthorized exports.',
      },
    ],
  },
  {
    id: 'D',
    name: 'AI/ML Cost',
    color: '#8b5cf6',
    challenges: [
      {
        id: 13,
        title: 'High Embedding Cost',
        rootCause: 'Recomputing embeddings on every pipeline run',
        solution: 'Cache + incremental embedding updates',
        artifacts: 'Embedding Cache Design, Incremental Strategy, Cost Before/After Report',
        interviewAnswer:
          'We cached embeddings in a vector store and only recomputed them for changed documents, reducing embedding API cost by over 80% for our RAG pipelines.',
      },
      {
        id: 14,
        title: 'Expensive Inference',
        rootCause: 'Always routing to large models regardless of task complexity',
        solution: 'Model routing — use smallest sufficient model',
        artifacts: 'Model Routing Logic, Cost-Performance Matrix, Inference Cost Dashboard',
        interviewAnswer:
          'We implemented a model routing layer that classified request complexity and routed simple queries to smaller models, reserving large models for complex tasks — cutting inference spend by 60%.',
      },
      {
        id: 15,
        title: 'Over-Training',
        rootCause: 'No feature reuse across models',
        solution: 'Feature store + reuse across training jobs',
        artifacts: 'Feature Store Design, Reuse Catalog, Training Cost Report',
        interviewAnswer:
          'We introduced a centralized feature store so teams could reuse pre-computed features rather than recomputing them independently, eliminating duplicate training compute.',
      },
      {
        id: 16,
        title: 'RAG Inefficiency',
        rootCause: 'Poor retrieval leading to large context windows',
        solution: 'Optimize chunking, indexing, and retrieval precision',
        artifacts: 'Chunking Strategy, Retrieval Evaluation Report, Context Window Analysis',
        interviewAnswer:
          'We tuned chunk sizes, improved embedding model selection, and refined retrieval ranking — significantly reducing average context window size and therefore token cost per query.',
      },
    ],
  },
  {
    id: 'E',
    name: 'Governance & Visibility',
    color: '#10b981',
    challenges: [
      {
        id: 17,
        title: 'No Cost Visibility',
        rootCause: 'No tracking of who spends what',
        solution: 'Cost dashboards with team-level attribution',
        artifacts: 'Cost Dashboard, Tagging Strategy, Spend Attribution Report',
        interviewAnswer:
          'We implemented cost attribution by tagging all clusters and jobs with team and project labels, then built dashboards giving every team real-time visibility into their spend.',
      },
      {
        id: 18,
        title: 'No Accountability',
        rootCause: 'Shared ownership means no one is responsible',
        solution: 'Chargeback / showback model per team',
        artifacts: 'Chargeback Model, Budget Alerts, Monthly Cost Review Process',
        interviewAnswer:
          'We introduced a showback model where each team could see their cost share, then moved to chargeback with budget alerts — creating real financial accountability for data spend.',
      },
      {
        id: 19,
        title: 'Over-Engineering',
        rootCause: 'Too many tools solving the same problem',
        solution: 'Standardization on fewer, well-governed platforms',
        artifacts: 'Tool Rationalization Report, Approved Platform List, Migration Plan',
        interviewAnswer:
          'We audited our tooling landscape, identified redundancy, and standardized on a smaller set of approved platforms — reducing licensing, operational overhead, and training costs.',
      },
      {
        id: 20,
        title: 'No Optimization Culture',
        rootCause: 'Cost is never revisited after initial deployment',
        solution: 'Regular FinOps reviews and optimization sprints',
        artifacts: 'FinOps Review Calendar, Optimization Backlog, Savings Tracker',
        interviewAnswer:
          'We established monthly FinOps reviews where teams present their top cost drivers and optimization wins, creating a continuous improvement culture around data cost management.',
      },
    ],
  },
];

const allChallenges = categories.flatMap((cat) =>
  cat.challenges.map((c) => ({ ...c, category: cat.name }))
);

const finopsLayers = [
  {
    name: 'Visibility & Tagging',
    color: '#3b82f6',
    desc: 'Tag everything — clusters, jobs, teams',
  },
  { name: 'Attribution & Chargeback', color: '#8b5cf6', desc: 'Who owns what cost' },
  { name: 'Optimization', color: '#10b981', desc: 'Right-size, autoscale, prune' },
  {
    name: 'Governance & Policy',
    color: '#f59e0b',
    desc: 'Budget alerts, cluster policies, approval gates',
  },
  {
    name: 'Culture & Reviews',
    color: '#ef4444',
    desc: 'Monthly FinOps reviews, optimization sprints',
  },
];

const kpis = [
  { kpi: 'Cost per Pipeline Run', target: 'Trend down MoM', owner: 'Data Engineering' },
  { kpi: 'Cost per Query (SQL)', target: '< $0.01 average', owner: 'Analytics' },
  { kpi: 'Cluster Utilization %', target: '> 70%', owner: 'Platform' },
  { kpi: 'Storage Growth Rate', target: '< 10% MoM', owner: 'Data Engineering' },
  { kpi: 'Embedding Reuse Rate', target: '> 80%', owner: 'AI/ML Team' },
  { kpi: 'Egress Cost', target: 'Flat or declining', owner: 'Architecture' },
  { kpi: 'Budget Variance', target: '< 5% vs forecast', owner: 'FinOps Lead' },
  { kpi: 'Optimization Savings', target: 'Track monthly', owner: 'All Teams' },
];

const maturityLevels = [
  { level: 1, name: 'Unaware', desc: 'No cost tracking. Shared clusters. No tagging.' },
  { level: 2, name: 'Reactive', desc: 'Cost reviewed only after budget overrun. Manual checks.' },
  {
    level: 3,
    name: 'Proactive',
    desc: 'Dashboards in place. Teams see their costs. Some optimization.',
  },
  {
    level: 4,
    name: 'Optimized',
    desc: 'Chargeback active. Automated alerts. Regular FinOps reviews.',
  },
  {
    level: 5,
    name: 'Continuous',
    desc: 'Cost embedded in CI/CD. Optimization is a first-class metric.',
  },
];

const commonFailures = [
  'Running all-purpose clusters for automated jobs',
  'No partition strategy — full table scans on every query',
  'Ingesting the same data multiple times across teams',
  'Recomputing embeddings on unchanged documents',
  'No budget alerts until the bill arrives',
  'Small file accumulation degrading performance and inflating costs',
  'Cross-region data transfers not tracked in cost reports',
  'No cluster policies — users spin up any size at any time',
];

function DataFinOps() {
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [activeCategory, setActiveCategory] = useState('ALL');

  const filtered = allChallenges.filter((c) => {
    const q = searchTerm.toLowerCase();
    const matchesSearch =
      c.title.toLowerCase().includes(q) ||
      c.rootCause.toLowerCase().includes(q) ||
      c.solution.toLowerCase().includes(q) ||
      c.artifacts.toLowerCase().includes(q) ||
      c.category.toLowerCase().includes(q);
    const matchesCategory = activeCategory === 'ALL' || c.category === activeCategory;
    return matchesSearch && matchesCategory;
  });

  const downloadCSV = () => {
    exportToCSV(
      allChallenges.map((c) => ({
        id: c.id,
        category: c.category,
        title: c.title,
        rootCause: c.rootCause,
        solution: c.solution,
        artifacts: c.artifacts,
        interviewAnswer: c.interviewAnswer,
      })),
      'data-finops-challenges.csv'
    );
  };

  const getCategoryColor = (catName) => {
    const cat = categories.find((c) => c.name === catName);
    return cat ? cat.color : '#6366f1';
  };

  return (
    <div>
      {/* Page Header */}
      <div className="page-header">
        <div>
          <h1>Data FinOps Framework</h1>
          <p>20 cost challenges with solutions &mdash; Compute, Storage, AI, Governance</p>
        </div>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F4B0;</div>
          <div className="stat-info">
            <h4>20</h4>
            <p>Cost Challenges</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#x1F4CA;</div>
          <div className="stat-info">
            <h4>5</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#x1F4A1;</div>
          <div className="stat-info">
            <h4>{filtered.length}</h4>
            <p>Showing</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#x1F3AF;</div>
          <div className="stat-info">
            <h4>8</h4>
            <p>FinOps KPIs</p>
          </div>
        </div>
      </div>

      {/* Search + Filter + CSV */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search challenges..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '300px' }}
          />
          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
            <button
              className={`btn btn-sm ${activeCategory === 'ALL' ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => setActiveCategory('ALL')}
            >
              All
            </button>
            {categories.map((cat) => (
              <button
                key={cat.id}
                className="btn btn-sm"
                onClick={() => setActiveCategory(cat.name)}
                style={{
                  background: activeCategory === cat.name ? `${cat.color}20` : 'transparent',
                  border: `1px solid ${cat.color}60`,
                  color: cat.color,
                  fontWeight: activeCategory === cat.name ? 700 : 400,
                }}
              >
                {cat.id}. {cat.name}
              </button>
            ))}
          </div>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadCSV}
            style={{ marginLeft: 'auto' }}
          >
            Download CSV
          </button>
        </div>
      </div>

      {/* Empty State */}
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
        const catColor = getCategoryColor(c.category);
        return (
          <div
            key={c.id}
            className="card"
            style={{ marginBottom: '0.75rem', borderLeft: `4px solid ${catColor}` }}
          >
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
                  <span
                    style={{
                      fontSize: '0.7rem',
                      fontWeight: 700,
                      color: catColor,
                      background: `${catColor}15`,
                      border: `1px solid ${catColor}40`,
                      borderRadius: '4px',
                      padding: '1px 6px',
                    }}
                  >
                    {c.category}
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

      {/* FinOps Operating Model */}
      <div className="card" style={{ marginTop: '2rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>FinOps Operating Model</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1.25rem', fontSize: '0.9rem' }}>
          The 5-layer model for managing data platform costs end-to-end.
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
            gap: '0.75rem',
          }}
        >
          {finopsLayers.map((layer, idx) => (
            <div
              key={layer.name}
              style={{
                background: `${layer.color}12`,
                border: `1px solid ${layer.color}40`,
                borderLeft: `4px solid ${layer.color}`,
                borderRadius: '8px',
                padding: '0.85rem 1rem',
              }}
            >
              <div
                style={{
                  fontWeight: 700,
                  fontSize: '0.8rem',
                  color: layer.color,
                  marginBottom: '0.25rem',
                }}
              >
                Layer {idx + 1}: {layer.name}
              </div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>{layer.desc}</div>
            </div>
          ))}
        </div>
      </div>

      {/* KPIs Table */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>FinOps KPIs</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1.25rem', fontSize: '0.9rem' }}>
          8 key performance indicators for tracking cost efficiency.
        </p>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.875rem' }}>
            <thead>
              <tr style={{ background: 'var(--bg-secondary)' }}>
                {['#', 'KPI', 'Target', 'Owner'].map((h) => (
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
              {kpis.map((row, idx) => (
                <tr key={idx} style={{ borderBottom: '1px solid var(--border)' }}>
                  <td
                    style={{
                      padding: '0.6rem 0.85rem',
                      color: 'var(--text-secondary)',
                      fontWeight: 600,
                    }}
                  >
                    {idx + 1}
                  </td>
                  <td
                    style={{
                      padding: '0.6rem 0.85rem',
                      fontWeight: 600,
                      color: 'var(--text-primary)',
                    }}
                  >
                    {row.kpi}
                  </td>
                  <td
                    style={{
                      padding: '0.6rem 0.85rem',
                      color: '#16a34a',
                      fontFamily: 'monospace',
                      fontSize: '0.8rem',
                    }}
                  >
                    {row.target}
                  </td>
                  <td style={{ padding: '0.6rem 0.85rem', color: 'var(--text-secondary)' }}>
                    {row.owner}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Maturity Model */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>FinOps Maturity Model</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1.25rem', fontSize: '0.9rem' }}>
          5-level maturity progression from unaware to continuous optimization.
        </p>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
          {maturityLevels.map((m) => {
            const colors = ['#ef4444', '#f59e0b', '#3b82f6', '#8b5cf6', '#10b981'];
            const color = colors[m.level - 1];
            return (
              <div
                key={m.level}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '1rem',
                  background: `${color}10`,
                  border: `1px solid ${color}30`,
                  borderRadius: '8px',
                  padding: '0.75rem 1rem',
                }}
              >
                <div
                  style={{
                    width: '32px',
                    height: '32px',
                    borderRadius: '50%',
                    background: color,
                    color: '#fff',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontWeight: 700,
                    fontSize: '0.9rem',
                    flexShrink: 0,
                  }}
                >
                  {m.level}
                </div>
                <div>
                  <div style={{ fontWeight: 700, color: color, fontSize: '0.9rem' }}>{m.name}</div>
                  <div
                    style={{
                      fontSize: '0.82rem',
                      color: 'var(--text-secondary)',
                      marginTop: '1px',
                    }}
                  >
                    {m.desc}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Common Failures */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h2 style={{ marginBottom: '0.5rem' }}>Common FinOps Failures</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1.25rem', fontSize: '0.9rem' }}>
          Mistakes that silently inflate data platform costs.
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
            gap: '0.6rem',
          }}
        >
          {commonFailures.map((f, idx) => (
            <div
              key={idx}
              style={{
                display: 'flex',
                alignItems: 'flex-start',
                gap: '0.5rem',
                background: '#fef2f2',
                border: '1px solid #fecaca',
                borderRadius: '8px',
                padding: '0.65rem 0.85rem',
              }}
            >
              <span
                style={{ color: '#dc2626', fontWeight: 700, flexShrink: 0, fontSize: '0.85rem' }}
              >
                &#x26A0;
              </span>
              <span style={{ fontSize: '0.85rem', color: '#7f1d1d', lineHeight: 1.45 }}>{f}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Interview Summary */}
      <div
        className="card"
        style={{
          marginTop: '1.5rem',
          background: 'linear-gradient(135deg, #0c1a2e 0%, #1e3a5f 100%)',
          border: '1px solid #1d4ed8',
          color: '#e0f2fe',
        }}
      >
        <div
          style={{
            fontSize: '0.7rem',
            fontWeight: 700,
            textTransform: 'uppercase',
            letterSpacing: '0.1em',
            color: '#60a5fa',
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
              color: '#3b82f6',
              lineHeight: 1,
            }}
          >
            &ldquo;
          </span>
          A mature Data FinOps practice starts with visibility — you cannot optimize what you cannot
          see. We introduced cost attribution through tagging, built dashboards for every team, and
          moved from a shared-cost model to chargeback. On the compute side, we replaced always-on
          clusters with autoscaling job clusters, isolated workloads to prevent contention, and
          moved non-critical streams to micro-batch. For storage, we enforced retention policies,
          compacted small files with Delta OPTIMIZE, and eliminated duplication through certified
          datasets. On the AI side, we cached embeddings, implemented model routing to use the
          smallest sufficient model, and built a feature store for reuse. We standardized our
          tooling landscape and established monthly FinOps reviews to make optimization a continuous
          discipline rather than a one-time exercise.
          <span
            style={{
              fontSize: '2.5rem',
              color: '#3b82f6',
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

export default DataFinOps;
