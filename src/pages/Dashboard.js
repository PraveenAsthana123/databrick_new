import React from 'react';

function Dashboard() {
  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Databricks PySpark Environment</h1>
          <p>Overview of your data lakehouse platform</p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon orange">📥</div>
          <div className="stat-info">
            <h4>55</h4>
            <p>Ingestion Scenarios</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon blue">🧠</div>
          <div className="stat-info">
            <h4>55</h4>
            <p>Modeling Scenarios</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">📚</div>
          <div className="stat-info">
            <h4>55</h4>
            <p>Unity Catalog Scenarios</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">📈</div>
          <div className="stat-info">
            <h4>55</h4>
            <p>Visualization Scenarios</p>
          </div>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <h3>Medallion Architecture</h3>
          </div>
          <div className="medallion-flow">
            <div className="medallion-step">
              <div className="medallion-badge landing">Landing Zone</div>
              <p>Raw data ingestion from external sources</p>
            </div>
            <div className="medallion-arrow">→</div>
            <div className="medallion-step">
              <div className="medallion-badge bronze">Bronze</div>
              <p>Raw data, append-only, no transformations</p>
            </div>
            <div className="medallion-arrow">→</div>
            <div className="medallion-step">
              <div className="medallion-badge silver">Silver</div>
              <p>Cleansed, conformed, enriched data</p>
            </div>
            <div className="medallion-arrow">→</div>
            <div className="medallion-step">
              <div className="medallion-badge gold">Gold</div>
              <p>Business-level aggregates, ML-ready</p>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <h3>Platform Components</h3>
          </div>
          <div className="component-list">
            <div className="component-item">
              <span className="component-icon">🔥</span>
              <div>
                <strong>Apache Spark</strong>
                <p>Distributed compute engine</p>
              </div>
            </div>
            <div className="component-item">
              <span className="component-icon">🏠</span>
              <div>
                <strong>Delta Lake</strong>
                <p>ACID transactions on data lakes</p>
              </div>
            </div>
            <div className="component-item">
              <span className="component-icon">📚</span>
              <div>
                <strong>Unity Catalog</strong>
                <p>Unified governance & access control</p>
              </div>
            </div>
            <div className="component-item">
              <span className="component-icon">⚡</span>
              <div>
                <strong>Photon Engine</strong>
                <p>Native vectorized query engine</p>
              </div>
            </div>
            <div className="component-item">
              <span className="component-icon">🤖</span>
              <div>
                <strong>MLflow</strong>
                <p>ML lifecycle management</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginTop: '1.5rem' }}>
        <div className="card-header">
          <h3>Data Flow Architecture</h3>
        </div>
        <div className="code-block">
          {`# Databricks Lakehouse Architecture
#
# External Sources ──→ Landing Zone ──→ Bronze ──→ Silver ──→ Gold
#     │                    │              │          │          │
#     ├─ APIs              ├─ ADLS        ├─ Raw     ├─ Clean   ├─ Aggregated
#     ├─ Databases         ├─ S3          ├─ Delta   ├─ Delta   ├─ Delta
#     ├─ Files (CSV/JSON)  ├─ GCS         ├─ Append  ├─ Joined  ├─ Star Schema
#     ├─ Streaming         ├─ DBFS        ├─ Schema  ├─ Dedup   ├─ ML Features
#     └─ IoT Devices       └─ Volumes     └─ on Read └─ on Write└─ Served
#
# Governance: Unity Catalog (catalog.schema.table)
# Compute:    Spark Clusters / SQL Warehouses / Serverless
# ML:         MLflow + Feature Store + Model Serving`}
        </div>
      </div>
    </div>
  );
}

export default Dashboard;
