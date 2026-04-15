import React, { useState } from 'react';

const clusterConfigs = [
  {
    id: 1,
    name: 'General Purpose',
    type: 'All-Purpose',
    workers: '2-8',
    instance: 'Standard_DS3_v2',
    runtime: '14.3 LTS',
    autoscale: true,
    status: 'running',
  },
  {
    id: 2,
    name: 'ML Training',
    type: 'All-Purpose',
    workers: '4-16',
    instance: 'Standard_NC6s_v3 (GPU)',
    runtime: '14.3 ML LTS',
    autoscale: true,
    status: 'running',
  },
  {
    id: 3,
    name: 'ETL Pipeline',
    type: 'Job Cluster',
    workers: '4-12',
    instance: 'Standard_D4s_v3',
    runtime: '14.3 LTS',
    autoscale: true,
    status: 'stopped',
  },
  {
    id: 4,
    name: 'SQL Analytics',
    type: 'SQL Warehouse',
    workers: '1-4',
    instance: 'Serverless',
    runtime: 'Latest',
    autoscale: true,
    status: 'running',
  },
  {
    id: 5,
    name: 'Streaming',
    type: 'All-Purpose',
    workers: '2-6',
    instance: 'Standard_D8s_v3',
    runtime: '14.3 LTS',
    autoscale: true,
    status: 'pending',
  },
  {
    id: 6,
    name: 'Dev/Test',
    type: 'Single Node',
    workers: '0',
    instance: 'Standard_DS3_v2',
    runtime: '14.3 LTS',
    autoscale: false,
    status: 'stopped',
  },
];

const clusterExamples = [
  {
    id: 1,
    title: 'Create Cluster via API',
    code: `import requests
token = dbutils.secrets.get("scope", "databricks_token")
resp = requests.post(f"{workspace_url}/api/2.0/clusters/create", json={
    "cluster_name": "my-etl-cluster",
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "autoscale": {"min_workers": 2, "max_workers": 8},
    "spark_conf": {"spark.sql.shuffle.partitions": "auto", "spark.databricks.delta.optimizeWrite.enabled": "true"},
    "custom_tags": {"team": "data-engineering", "env": "production"}
}, headers={"Authorization": f"Bearer {token}"}, timeout=30)`,
  },
  {
    id: 2,
    title: 'Terraform Cluster',
    code: `resource "databricks_cluster" "etl" {
  cluster_name            = "etl-cluster"
  spark_version          = "14.3.x-scala2.12"
  node_type_id           = "Standard_DS3_v2"
  autotermination_minutes = 30
  autoscale {
    min_workers = 2
    max_workers = 8
  }
  spark_conf = {
    "spark.sql.shuffle.partitions" = "auto"
  }
}`,
  },
  {
    id: 3,
    title: 'Cluster Init Script',
    code: `# Init script: /dbfs/init_scripts/install_libs.sh
#!/bin/bash
pip install great-expectations==0.18.0
pip install delta-sharing==1.0.0
pip install mlflow==2.10.0

# Upload init script
dbutils.fs.put("/dbfs/init_scripts/install_libs.sh", script_content, overwrite=True)`,
  },
];

function Clusters() {
  const [expandedId, setExpandedId] = useState(null);

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Clusters</h1>
          <p>Compute resource management</p>
        </div>
        <button className="btn btn-primary">+ Create Cluster</button>
      </div>

      <div className="table-wrapper card" style={{ marginBottom: '1.5rem' }}>
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Workers</th>
              <th>Instance</th>
              <th>Runtime</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {clusterConfigs.map((c) => (
              <tr key={c.id}>
                <td>
                  <strong>{c.name}</strong>
                </td>
                <td>{c.type}</td>
                <td>
                  {c.workers} {c.autoscale && '(auto)'}
                </td>
                <td style={{ fontSize: '0.8rem' }}>{c.instance}</td>
                <td>{c.runtime}</td>
                <td>
                  <span className={`badge ${c.status}`}>{c.status}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h2 style={{ fontSize: '1.2rem', marginBottom: '1rem' }}>Configuration Examples</h2>
      {clusterExamples.map((ex) => (
        <div key={ex.id} className="card" style={{ marginBottom: '0.75rem' }}>
          <div
            onClick={() => setExpandedId(expandedId === ex.id ? null : ex.id)}
            style={{ cursor: 'pointer', display: 'flex', justifyContent: 'space-between' }}
          >
            <strong>{ex.title}</strong>
            <span>{expandedId === ex.id ? '▼' : '▶'}</span>
          </div>
          {expandedId === ex.id && (
            <div className="code-block" style={{ marginTop: '0.75rem' }}>
              {ex.code}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

export default Clusters;
