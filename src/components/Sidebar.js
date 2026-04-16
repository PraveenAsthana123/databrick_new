import React from 'react';

const menuItems = [
  {
    section: 'Overview',
    items: [
      { id: 'dashboard', icon: '📊', label: 'Dashboard' },
      { id: 'medallion', icon: '🏅', label: 'Medallion Architecture' },
      { id: 'bronze-ops', icon: '🥉', label: 'Bronze Layer (100)' },
      { id: 'silver-ops', icon: '🥈', label: 'Silver Layer (100)' },
      { id: 'gold-ops', icon: '🥇', label: 'Gold Layer (100)' },
      { id: 'landing-zone', icon: '🛬', label: 'Landing Zone' },
    ],
  },
  {
    section: 'Ingestion',
    items: [
      { id: 'ingestion-batch', icon: '📦', label: 'Batch Ingestion (50)' },
      { id: 'batch-pipelines', icon: '🔗', label: 'Batch Pipelines (50)' },
      { id: 'stream-scenarios', icon: '🌊', label: 'Stream Pipelines (50)' },
      { id: 'ingestion-all', icon: '📥', label: 'All Ingestion (55)' },
    ],
  },
  {
    section: 'Transformation',
    items: [
      { id: 'elt-scenarios', icon: '🔄', label: 'ELT Pipelines (50)' },
      { id: 'etl-scenarios', icon: '⚙️', label: 'ETL Pipelines (50)' },
      { id: 'merge-patterns', icon: '🔀', label: 'Merge Patterns (30)' },
      { id: 'elt-operations', icon: '📋', label: 'ELT Operations' },
      { id: 'etl-operations', icon: '🔧', label: 'ETL Transformations' },
    ],
  },
  {
    section: 'Scenarios (50+)',
    items: [
      { id: 'modeling', icon: '🧠', label: 'Modeling (55)' },
      { id: 'unity-catalog', icon: '📚', label: 'Unity Catalog (55)' },
      { id: 'visualization', icon: '📈', label: 'Visualization (55)' },
      { id: 'data-testing', icon: '✅', label: 'Data Testing (55)' },
    ],
  },
  {
    section: 'Pipelines & AI',
    items: [
      { id: 'pipelines', icon: '⚡', label: 'Pipeline Builder (20)' },
      { id: 'xai', icon: '🔍', label: 'XAI / Fairness AI' },
      { id: 'rag', icon: '🤖', label: 'RAG / Ollama / MCP' },
    ],
  },
  {
    section: 'Governance',
    items: [
      { id: 'governance-scenarios', icon: '🛡️', label: 'UC Governance (50)' },
      { id: 'security-pii', icon: '🔐', label: 'Security & PII (50)' },
      { id: 'security', icon: '🔒', label: 'Security & Governance' },
      { id: 'terraform', icon: '☁️', label: 'Terraform / Azure / Snowflake' },
    ],
  },
  {
    section: 'Infrastructure',
    items: [
      { id: 'clusters', icon: '🖥️', label: 'Clusters' },
      { id: 'notebooks', icon: '📓', label: 'Notebooks' },
      { id: 'jobs', icon: '⏱️', label: 'Jobs' },
      { id: 'spark-ui', icon: '🔥', label: 'Spark UI' },
    ],
  },
  {
    section: 'Data & Tools',
    items: [
      { id: 'data-storage', icon: '🗄️', label: 'Data Storage' },
      { id: 'upload-docs', icon: '📂', label: 'Upload Documents' },
      { id: 'download-data', icon: '⬇️', label: 'Download Data' },
      { id: 'simulation', icon: '🧪', label: 'Simulation Tools' },
    ],
  },
  {
    section: 'Data Architect',
    items: [
      { id: 'architect-challenges', icon: '🏗️', label: 'Architect Challenges (120)' },
      { id: 'data-strategy', icon: '🎯', label: 'Data Strategy (20)' },
      { id: 'maturity-model', icon: '📊', label: 'Maturity Model (5 Levels)' },
      { id: 'maturity-assessment', icon: '📋', label: 'Maturity Assessment' },
      { id: 'data-roadmap', icon: '🗺️', label: 'Data Roadmap (4 Phases)' },
      { id: 'data-finops', icon: '💰', label: 'Data FinOps (20)' },
      { id: 'production-support', icon: '🚨', label: 'Production Support (L1-L4)' },
      { id: 'ingestion-challenges', icon: '📥', label: 'Ingestion Challenges (40)' },
      { id: 'modeling-challenges', icon: '🧩', label: 'Modeling Challenges (40)' },
      { id: 'governance-challenges', icon: '🛡️', label: 'Governance Challenges (40)' },
      { id: 'visualization-challenges', icon: '📈', label: 'Visualization Challenges (40)' },
    ],
  },
  {
    section: 'Implementation (Enterprise)',
    items: [
      { id: 'ingestion-impl', icon: '🔧', label: 'Ingestion Impl (20)' },
      { id: 'modeling-impl', icon: '🛠️', label: 'Modeling Impl (30)' },
      { id: 'governance-impl', icon: '⚖️', label: 'Governance Impl (30)' },
      { id: 'visualization-impl', icon: '📊', label: 'Visualization Impl (30)' },
      { id: 'ai-governance', icon: '🤖', label: 'AI Governance (30)' },
      { id: 'data-ai-security', icon: '🔐', label: 'Data + AI Security (30)' },
      { id: 'data-testing-impl', icon: '✅', label: 'Data Testing Impl (19)' },
    ],
  },
  {
    section: 'Production Support (L1-L4)',
    items: [
      { id: 'ingestion-prod-support', icon: '🟢', label: 'Ingestion Prod Support' },
      { id: 'modeling-prod-support', icon: '🟡', label: 'Modeling Prod Support' },
      { id: 'governance-prod-support', icon: '🔵', label: 'Governance Prod Support' },
      { id: 'devops-prod-support', icon: '🚀', label: 'DevOps Prod Support' },
      { id: 'security-prod-support', icon: '🛑', label: 'Security Prod Support' },
      { id: 'observability-prod-support', icon: '🔭', label: 'Observability / AIOps' },
      { id: 'ai-ml-prod-support', icon: '🧠', label: 'AI / ML / GenAI Support' },
    ],
  },
  {
    section: 'Reference Architecture',
    items: [{ id: 'e2e-architecture', icon: '🏛️', label: 'End-to-End Architecture' }],
  },
  { section: 'System', items: [{ id: 'settings', icon: '⚙️', label: 'Settings' }] },
];

function Sidebar({ activePage, onNavigate, collapsed }) {
  return (
    <aside className={`sidebar ${collapsed ? 'collapsed' : ''}`}>
      {menuItems.map((section) => (
        <div className="sidebar-section" key={section.section}>
          <div className="sidebar-label">{section.section}</div>
          <ul className="sidebar-nav">
            {section.items.map((item) => (
              <li key={item.id}>
                <button
                  className={`sidebar-item ${activePage === item.id ? 'active' : ''}`}
                  onClick={() => onNavigate(item.id)}
                  title={item.label}
                >
                  <span className="icon">{item.icon}</span>
                  <span className="label">{item.label}</span>
                </button>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </aside>
  );
}

export default Sidebar;
