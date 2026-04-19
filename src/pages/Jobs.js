import React from 'react';
import FileFormatRunner from '../components/common/FileFormatRunner';

const jobs = [
  {
    id: 1,
    name: 'Daily ETL Pipeline',
    schedule: '0 6 * * *',
    nextRun: '06:00 AM',
    lastRun: '06:00 AM today',
    duration: '12m 34s',
    status: 'success',
    cluster: 'ETL Pipeline',
  },
  {
    id: 2,
    name: 'Hourly CDC Sync',
    schedule: '0 * * * *',
    nextRun: 'Next hour',
    lastRun: '45 min ago',
    duration: '3m 12s',
    status: 'success',
    cluster: 'Streaming',
  },
  {
    id: 3,
    name: 'ML Model Retrain',
    schedule: '0 2 * * 0',
    nextRun: 'Sunday 2 AM',
    lastRun: '3 days ago',
    duration: '45m 20s',
    status: 'success',
    cluster: 'ML Training',
  },
  {
    id: 4,
    name: 'Data Quality Check',
    schedule: '0 8 * * *',
    nextRun: '08:00 AM',
    lastRun: '08:00 AM today',
    duration: '5m 45s',
    status: 'failed',
    cluster: 'General Purpose',
  },
  {
    id: 5,
    name: 'Gold Layer Refresh',
    schedule: '0 7 * * *',
    nextRun: '07:00 AM',
    lastRun: '07:00 AM today',
    duration: '8m 10s',
    status: 'success',
    cluster: 'ETL Pipeline',
  },
  {
    id: 6,
    name: 'Snowflake Sync',
    schedule: '0 */4 * * *',
    nextRun: '4 hours',
    lastRun: '2 hours ago',
    duration: '6m 30s',
    status: 'success',
    cluster: 'General Purpose',
  },
  {
    id: 7,
    name: 'Report Generation',
    schedule: '0 9 * * 1-5',
    nextRun: '09:00 AM Mon',
    lastRun: 'Friday',
    duration: '2m 15s',
    status: 'success',
    cluster: 'Dev/Test',
  },
  {
    id: 8,
    name: 'Table Optimization',
    schedule: '0 3 * * *',
    nextRun: '03:00 AM',
    lastRun: '03:00 AM today',
    duration: '15m 45s',
    status: 'running',
    cluster: 'ETL Pipeline',
  },
];

function Jobs() {
  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Jobs</h1>
          <p>Scheduled workflow management</p>
        </div>
        <button className="btn btn-primary">+ Create Job</button>
      </div>

      <div className="stats-grid" style={{ marginBottom: '1.5rem' }}>
        <div className="stat-card">
          <div className="stat-icon green">✓</div>
          <div className="stat-info">
            <h4>{jobs.filter((j) => j.status === 'success').length}</h4>
            <p>Succeeded</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">⟳</div>
          <div className="stat-info">
            <h4>{jobs.filter((j) => j.status === 'running').length}</h4>
            <p>Running</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">✗</div>
          <div className="stat-info">
            <h4>{jobs.filter((j) => j.status === 'failed').length}</h4>
            <p>Failed</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon blue">⏱</div>
          <div className="stat-info">
            <h4>{jobs.length}</h4>
            <p>Total Jobs</p>
          </div>
        </div>
      </div>

      <div className="table-wrapper card">
        <table>
          <thead>
            <tr>
              <th>Job Name</th>
              <th>Schedule (Cron)</th>
              <th>Next Run</th>
              <th>Last Run</th>
              <th>Duration</th>
              <th>Cluster</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((j) => (
              <tr key={j.id}>
                <td>
                  <strong>{j.name}</strong>
                </td>
                <td style={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{j.schedule}</td>
                <td>{j.nextRun}</td>
                <td>{j.lastRun}</td>
                <td>{j.duration}</td>
                <td>{j.cluster}</td>
                <td>
                  <span className={`badge ${j.status}`}>{j.status}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: '1.5rem' }}>
        <h2 style={{ fontSize: '1.1rem', marginBottom: '0.5rem' }}>
          Schedule / Run / Download — Jobs
        </h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginBottom: '0.85rem' }}>
          Select a format to download the jobs catalog, run a command to simulate execution, or
          schedule the jobs catalog refresh as a Databricks Workflow.
        </p>
        <FileFormatRunner
          data={jobs.map((j) => ({
            job_id: `job_${String(j.id).padStart(6, '0')}`,
            name: j.name,
            schedule_cron: j.schedule,
            next_run: j.nextRun,
            last_run: j.lastRun,
            duration: j.duration,
            status: j.status,
            cluster: j.cluster,
          }))}
          slug="jobs-catalog"
          schemaName="JobsCatalog"
          tableName="catalog.platform.jobs_catalog"
        />
      </div>
    </div>
  );
}

export default Jobs;
