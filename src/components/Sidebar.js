import React from 'react';

const menuItems = [
  {
    section: 'Overview',
    items: [
      { id: 'dashboard', icon: '📊', label: 'Dashboard' },
      { id: 'medallion', icon: '🏅', label: 'Medallion Architecture' },
      { id: 'landing-zone', icon: '🛬', label: 'Landing Zone' },
    ],
  },
  {
    section: 'Ingestion',
    items: [
      { id: 'ingestion-batch', icon: '📦', label: 'Batch Ingestion (50)' },
      { id: 'batch-pipelines', icon: '🔗', label: 'Batch Pipelines (50)' },
      { id: 'ingestion-stream', icon: '🌊', label: 'Stream Ingestion' },
      { id: 'ingestion-all', icon: '📥', label: 'All Ingestion (55)' },
    ],
  },
  {
    section: 'Transformation',
    items: [
      { id: 'elt-scenarios', icon: '🔄', label: 'ELT Pipelines (50)' },
      { id: 'etl-scenarios', icon: '⚙️', label: 'ETL Pipelines (50)' },
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
