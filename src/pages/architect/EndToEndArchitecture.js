import React from 'react';
import FileFormatRunner from '../../components/common/FileFormatRunner';
import EnterpriseArchitectDetail from '../../components/architect/EnterpriseArchitectDetail';

const LAYERS = [
  {
    id: 1,
    name: 'Source Systems',
    icon: '🏢',
    color: '#64748b',
    purpose: 'Generate business data',
    activities: 'Transactions, customer, product, pricing, contracts',
    challenges: 'Heterogeneous data, latency, schema changes',
    controls: 'Source profiling, contracts, connectivity standards',
    tools: 'SAP, CRM, APIs, files, Kafka',
  },
  {
    id: 2,
    name: 'Ingestion (Bronze)',
    icon: '🥉',
    color: '#b45309',
    purpose: 'Bring raw data into lakehouse',
    activities: 'Batch, streaming, CDC, file ingest',
    challenges: 'Schema drift, duplicates, late data, API limits',
    controls: 'Auto Loader, checkpointing, dedup, retries',
    tools: 'Databricks Auto Loader, Spark, Kafka',
  },
  {
    id: 3,
    name: 'Silver Layer',
    icon: '🥈',
    color: '#64748b',
    purpose: 'Clean and standardize data',
    activities: 'Validation, joins, cleansing, enrichment',
    challenges: 'Bad quality, inconsistent keys, timezone issues',
    controls: 'Quality rules, canonical models, standard keys',
    tools: 'Spark SQL, Delta Lake, dbt',
  },
  {
    id: 4,
    name: 'Gold Layer',
    icon: '🥇',
    color: '#ca8a04',
    purpose: 'Business-ready curated datasets',
    activities: 'Aggregation, KPI logic, marts',
    challenges: 'Wrong logic, poor performance, unclear grain',
    controls: 'Star schema, semantic layer, partitioning, documentation',
    tools: 'Delta, dbt, Databricks SQL',
  },
  {
    id: 5,
    name: 'Governance Layer',
    icon: '🛡️',
    color: '#10b981',
    purpose: 'Control access, trust, and compliance',
    activities: 'Lineage, RBAC, classification, audit',
    challenges: 'No ownership, no lineage, data leakage',
    controls: 'Unity Catalog, policies, audit logs, glossary',
    tools: 'Unity Catalog, Collibra',
  },
  {
    id: 6,
    name: 'Visualization Layer',
    icon: '📈',
    color: '#ec4899',
    purpose: 'Business consumption',
    activities: 'Dashboards, reports, drill-down, alerts',
    challenges: 'Slow dashboards, KPI mismatch, low adoption',
    controls: 'Semantic model, RLS, aggregated tables, UX standards',
    tools: 'Power BI, Tableau, Databricks SQL',
  },
  {
    id: 7,
    name: 'Monitoring / Ops',
    icon: '📊',
    color: '#3b82f6',
    purpose: 'Keep platform healthy',
    activities: 'Logs, metrics, alerts, cost tracking',
    challenges: 'Silent failures, alert fatigue, high cost',
    controls: 'Observability, runbooks, FinOps',
    tools: 'Splunk, Datadog, OpenTelemetry',
  },
  {
    id: 8,
    name: 'Security / Compliance',
    icon: '🔒',
    color: '#ef4444',
    purpose: 'Protect system',
    activities: 'IAM, masking, encryption, audit',
    challenges: 'Over-access, exposed PII, non-compliance',
    controls: 'Least privilege, masking, encryption, approval workflows',
    tools: 'IAM, Key Vault, SIEM',
  },
  {
    id: 9,
    name: 'AI / Advanced Analytics',
    icon: '🤖',
    color: '#8b5cf6',
    purpose: 'ML / GenAI / feature use',
    activities: 'Feature engineering, model training, RAG',
    challenges: 'Drift, poor quality features, governance gaps',
    controls: 'Feature store, model monitoring, AI governance',
    tools: 'MLflow, Feature Store',
  },
];

const FLOW_STEPS = [
  { label: 'Source Systems', note: 'SAP / CRM / APIs / Files / Kafka' },
  { label: 'Ingestion Layer', note: 'Batch / Streaming / CDC / Auto Loader' },
  { label: 'Bronze Layer', note: 'Raw immutable landing' },
  { label: 'Silver Layer', note: 'Cleaned, validated, standardized' },
  { label: 'Gold Layer', note: 'Business-ready KPIs, marts, semantic datasets' },
  { label: 'Governance + Security', note: 'Unity Catalog, lineage, RBAC, masking, audit' },
  { label: 'Visualization / Consumption', note: 'Power BI / Tableau / Databricks SQL / AI' },
];

export default function EndToEndArchitecture() {
  return (
    <div>
      <div className="page-header">
        <div>
          <h1>End-to-End Architecture — Databricks Lakehouse + Unity Catalog + BI</h1>
          <p>
            Complete flow: source systems → raw ingestion → cleansed/modelled data → governed access
            → dashboard/report consumption.
          </p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F3D7;</div>
          <div className="stat-info">
            <h4>9</h4>
            <p>Architecture Layers</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#x1F501;</div>
          <div className="stat-info">
            <h4>7</h4>
            <p>Flow Stages</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#x2705;</div>
          <div className="stat-info">
            <h4>5+</h4>
            <p>Purposes per Layer</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#x1F3AF;</div>
          <div className="stat-info">
            <h4>E2E</h4>
            <p>Reference Architecture</p>
          </div>
        </div>
      </div>

      {/* Layer Table */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ marginBottom: '1rem' }}>End-to-End Layer Table</h2>
        <div style={{ display: 'grid', gap: '0.85rem' }}>
          {LAYERS.map((l) => (
            <div
              key={l.id}
              style={{
                background: `${l.color}08`,
                border: `1px solid ${l.color}30`,
                borderLeft: `5px solid ${l.color}`,
                borderRadius: '10px',
                padding: '1rem 1.15rem',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.7rem',
                  marginBottom: '0.65rem',
                }}
              >
                <span style={{ fontSize: '1.6rem' }}>{l.icon}</span>
                <div>
                  <div style={{ fontWeight: 800, color: l.color, fontSize: '1.05rem' }}>
                    {l.id}. {l.name}
                  </div>
                  <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
                    {l.purpose}
                  </div>
                </div>
              </div>
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                  gap: '0.65rem',
                }}
              >
                <Field label="Activities" value={l.activities} color="#3b82f6" />
                <Field label="Key Challenges" value={l.challenges} color="#ef4444" />
                <Field label="Controls / Solutions" value={l.controls} color="#16a34a" />
                <Field label="Typical Tools" value={l.tools} color="#8b5cf6" />
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Flow */}
      <div
        className="card"
        style={{
          background: 'linear-gradient(135deg, #1e1b4b 0%, #312e81 100%)',
          color: '#e0e7ff',
        }}
      >
        <h2 style={{ color: '#c7d2fe', marginBottom: '1.25rem' }}>End-to-End Flow</h2>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
          {FLOW_STEPS.map((s, i) => (
            <React.Fragment key={s.label}>
              <div
                style={{
                  background: 'rgba(255,255,255,0.08)',
                  border: '1px solid rgba(199,210,254,0.3)',
                  borderRadius: '8px',
                  padding: '0.75rem 1rem',
                }}
              >
                <div style={{ fontWeight: 700, color: '#e0e7ff', fontSize: '0.95rem' }}>
                  {i + 1}. {s.label}
                </div>
                <div style={{ fontSize: '0.8rem', color: '#a5b4fc', marginTop: '0.2rem' }}>
                  ({s.note})
                </div>
              </div>
              {i < FLOW_STEPS.length - 1 && (
                <div
                  style={{
                    textAlign: 'center',
                    color: '#a5b4fc',
                    fontSize: '1.2rem',
                    lineHeight: 1,
                  }}
                >
                  ↓
                </div>
              )}
            </React.Fragment>
          ))}
        </div>
      </div>

      {/* Enterprise Architect Deep Detail for E2E */}
      <div style={{ marginTop: '1.5rem' }}>
        <EnterpriseArchitectDetail
          title="End-to-End Lakehouse Architecture"
          description="Databricks Lakehouse + Unity Catalog + BI — complete enterprise reference architecture"
          domain="strategy"
        />
      </div>

      {/* Download / Run / Schedule */}
      <div style={{ marginTop: '1.5rem' }}>
        <h2 style={{ fontSize: '1.1rem', marginBottom: '0.5rem' }}>
          Architecture Catalog — Download / Run / Schedule
        </h2>
        <FileFormatRunner
          data={LAYERS.map((l) => ({
            layer_id: l.id,
            name: l.name,
            purpose: l.purpose,
            activities: l.activities,
            challenges: l.challenges,
            controls: l.controls,
            tools: l.tools,
          }))}
          slug="e2e-architecture"
          schemaName="ArchitectureLayer"
          tableName="catalog.architect.e2e_architecture"
        />
      </div>
    </div>
  );
}

function Field({ label, value, color }) {
  return (
    <div
      style={{
        background: '#fff',
        border: `1px solid ${color}30`,
        borderLeft: `3px solid ${color}`,
        borderRadius: '7px',
        padding: '0.55rem 0.8rem',
      }}
    >
      <div
        style={{
          fontSize: '0.65rem',
          fontWeight: 700,
          textTransform: 'uppercase',
          color,
          marginBottom: '0.25rem',
          letterSpacing: '0.05em',
        }}
      >
        {label}
      </div>
      <div style={{ fontSize: '0.82rem', color: '#1a1a1a', lineHeight: 1.5 }}>{value}</div>
    </div>
  );
}
