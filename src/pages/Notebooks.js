import React from 'react';

const notebooks = [
  {
    id: 1,
    name: '01_Ingestion_CSV',
    language: 'Python',
    path: '/Repos/team/pipelines/',
    lastRun: '2 hours ago',
    status: 'success',
  },
  {
    id: 2,
    name: '02_Bronze_to_Silver',
    language: 'Python',
    path: '/Repos/team/pipelines/',
    lastRun: '2 hours ago',
    status: 'success',
  },
  {
    id: 3,
    name: '03_Silver_to_Gold',
    language: 'SQL',
    path: '/Repos/team/pipelines/',
    lastRun: '3 hours ago',
    status: 'success',
  },
  {
    id: 4,
    name: '04_ML_Training',
    language: 'Python',
    path: '/Repos/team/ml/',
    lastRun: '1 day ago',
    status: 'success',
  },
  {
    id: 5,
    name: '05_Data_Quality',
    language: 'Python',
    path: '/Repos/team/quality/',
    lastRun: '6 hours ago',
    status: 'failed',
  },
  {
    id: 6,
    name: '06_Dashboard_Refresh',
    language: 'SQL',
    path: '/Repos/team/reporting/',
    lastRun: '1 hour ago',
    status: 'success',
  },
  {
    id: 7,
    name: '07_CDC_Pipeline',
    language: 'Python',
    path: '/Repos/team/cdc/',
    lastRun: '30 min ago',
    status: 'running',
  },
  {
    id: 8,
    name: '08_Snowflake_Sync',
    language: 'Python',
    path: '/Repos/team/integrations/',
    lastRun: '4 hours ago',
    status: 'success',
  },
];

function Notebooks() {
  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Notebooks</h1>
          <p>Development workspace</p>
        </div>
        <button className="btn btn-primary">+ New Notebook</button>
      </div>

      <div className="table-wrapper card">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Language</th>
              <th>Path</th>
              <th>Last Run</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {notebooks.map((n) => (
              <tr key={n.id}>
                <td>
                  <strong>{n.name}</strong>
                </td>
                <td>
                  <span className="badge completed">{n.language}</span>
                </td>
                <td style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>{n.path}</td>
                <td>{n.lastRun}</td>
                <td>
                  <span className={`badge ${n.status}`}>{n.status}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="card" style={{ marginTop: '1.5rem' }}>
        <h3 style={{ marginBottom: '0.75rem' }}>Notebook Best Practices</h3>
        <div className="grid-2">
          <ul style={{ paddingLeft: '1.2rem', fontSize: '0.85rem', lineHeight: '2' }}>
            <li>Use Repos for version control (Git integration)</li>
            <li>Follow naming convention: 01_Step_Name</li>
            <li>Use widgets for parameterized notebooks</li>
            <li>Keep notebooks focused (one task per notebook)</li>
          </ul>
          <ul style={{ paddingLeft: '1.2rem', fontSize: '0.85rem', lineHeight: '2' }}>
            <li>Use %run for notebook orchestration</li>
            <li>Add markdown documentation cells</li>
            <li>Use dbutils.notebook.exit() for return values</li>
            <li>Test with sample data before full runs</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

export default Notebooks;
