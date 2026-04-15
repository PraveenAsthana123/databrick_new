import React from 'react';

const stages = [
  {
    id: 0,
    name: 'Scan Parquet',
    tasks: 200,
    duration: '12.5s',
    input: '2.1 GB',
    output: '1.8 GB',
    shuffle: '0 B',
    status: 'completed',
  },
  {
    id: 1,
    name: 'Filter & Project',
    tasks: 200,
    duration: '3.2s',
    input: '1.8 GB',
    output: '450 MB',
    shuffle: '0 B',
    status: 'completed',
  },
  {
    id: 2,
    name: 'Hash Join',
    tasks: 200,
    duration: '45.8s',
    input: '450 MB',
    output: '380 MB',
    shuffle: '1.2 GB',
    status: 'completed',
  },
  {
    id: 3,
    name: 'Aggregate',
    tasks: 200,
    duration: '8.1s',
    input: '380 MB',
    output: '25 MB',
    shuffle: '500 MB',
    status: 'completed',
  },
  {
    id: 4,
    name: 'Write Delta',
    tasks: 50,
    duration: '15.3s',
    input: '25 MB',
    output: '25 MB',
    shuffle: '0 B',
    status: 'running',
  },
];

const executors = [
  {
    id: 'driver',
    host: '10.0.0.1',
    cores: 4,
    memory: '8 GB',
    activeTasks: 0,
    completedTasks: 450,
    failedTasks: 0,
  },
  {
    id: '1',
    host: '10.0.0.2',
    cores: 8,
    memory: '16 GB',
    activeTasks: 12,
    completedTasks: 180,
    failedTasks: 0,
  },
  {
    id: '2',
    host: '10.0.0.3',
    cores: 8,
    memory: '16 GB',
    activeTasks: 10,
    completedTasks: 175,
    failedTasks: 2,
  },
  {
    id: '3',
    host: '10.0.0.4',
    cores: 8,
    memory: '16 GB',
    activeTasks: 14,
    completedTasks: 190,
    failedTasks: 0,
  },
  {
    id: '4',
    host: '10.0.0.5',
    cores: 8,
    memory: '16 GB',
    activeTasks: 14,
    completedTasks: 185,
    failedTasks: 1,
  },
];

function SparkUI() {
  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Spark UI</h1>
          <p>Monitor Spark job execution and performance</p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon orange">⏱</div>
          <div className="stat-info">
            <h4>84.9s</h4>
            <p>Total Duration</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon blue">📊</div>
          <div className="stat-info">
            <h4>850</h4>
            <p>Total Tasks</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">📦</div>
          <div className="stat-info">
            <h4>2.1 GB</h4>
            <p>Data Processed</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">🔀</div>
          <div className="stat-info">
            <h4>1.7 GB</h4>
            <p>Shuffle Data</p>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div className="card-header">
          <h3>Stages</h3>
        </div>
        <div className="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Stage</th>
                <th>Name</th>
                <th>Tasks</th>
                <th>Duration</th>
                <th>Input</th>
                <th>Output</th>
                <th>Shuffle</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {stages.map((s) => (
                <tr key={s.id}>
                  <td>{s.id}</td>
                  <td>
                    <strong>{s.name}</strong>
                  </td>
                  <td>{s.tasks}</td>
                  <td>{s.duration}</td>
                  <td>{s.input}</td>
                  <td>{s.output}</td>
                  <td>{s.shuffle}</td>
                  <td>
                    <span className={`badge ${s.status}`}>{s.status}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <div className="card-header">
          <h3>Executors</h3>
        </div>
        <div className="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Host</th>
                <th>Cores</th>
                <th>Memory</th>
                <th>Active</th>
                <th>Completed</th>
                <th>Failed</th>
              </tr>
            </thead>
            <tbody>
              {executors.map((e) => (
                <tr key={e.id}>
                  <td>{e.id}</td>
                  <td>{e.host}</td>
                  <td>{e.cores}</td>
                  <td>{e.memory}</td>
                  <td>{e.activeTasks}</td>
                  <td>{e.completedTasks}</td>
                  <td style={{ color: e.failedTasks > 0 ? 'var(--error)' : 'inherit' }}>
                    {e.failedTasks}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

export default SparkUI;
