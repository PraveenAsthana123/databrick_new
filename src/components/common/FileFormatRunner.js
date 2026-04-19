import React, { useState } from 'react';
import {
  exportToCSV,
  exportToJSON,
  exportToXML,
  exportToAvro,
  exportToText,
  exportToParquet,
} from '../../utils/fileExport';

const FORMAT_OPTIONS = [
  {
    id: 'text',
    label: 'Text (.txt)',
    ext: '.txt',
    icon: '📝',
    readCmd: 'spark.read.text("dbfs:/data/{slug}.txt")',
    writeCmd: 'df.write.text("dbfs:/data/{slug}.txt")',
  },
  {
    id: 'csv',
    label: 'CSV (.csv)',
    ext: '.csv',
    icon: '📄',
    readCmd: 'spark.read.option("header","true").csv("dbfs:/data/{slug}.csv")',
    writeCmd: 'df.write.option("header","true").csv("dbfs:/data/{slug}.csv")',
  },
  {
    id: 'parquet',
    label: 'Parquet (.parquet)',
    ext: '.parquet',
    icon: '🧱',
    readCmd: 'spark.read.parquet("dbfs:/data/{slug}.parquet")',
    writeCmd: 'df.write.parquet("dbfs:/data/{slug}.parquet")',
  },
  {
    id: 'json',
    label: 'JSON (.json)',
    ext: '.json',
    icon: '{ }',
    readCmd: 'spark.read.json("dbfs:/data/{slug}.json")',
    writeCmd: 'df.write.json("dbfs:/data/{slug}.json")',
  },
  {
    id: 'avro',
    label: 'Avro (.avro)',
    ext: '.avro',
    icon: '🔷',
    readCmd: 'spark.read.format("avro").load("dbfs:/data/{slug}.avro")',
    writeCmd: 'df.write.format("avro").save("dbfs:/data/{slug}.avro")',
  },
  {
    id: 'xml',
    label: 'XML (.xml)',
    ext: '.xml',
    icon: '< >',
    readCmd: 'spark.read.format("xml").option("rowTag","record").load("dbfs:/data/{slug}.xml")',
    writeCmd:
      'df.write.format("xml").option("rootTag","records").option("rowTag","record").save("dbfs:/data/{slug}.xml")',
  },
];

function doExport(format, rows, slug, schemaName) {
  switch (format) {
    case 'text':
      exportToText(rows, `${slug}.txt`);
      break;
    case 'csv':
      exportToCSV(rows, `${slug}.csv`);
      break;
    case 'parquet':
      exportToParquet(rows, `${slug}.parquet.json`, schemaName);
      break;
    case 'json':
      exportToJSON(rows, `${slug}.json`);
      break;
    case 'avro':
      exportToAvro(rows, `${slug}.avro.json`, schemaName);
      break;
    case 'xml':
      exportToXML(rows, `${slug}.xml`, 'records', 'record');
      break;
    default:
      break;
  }
}

/**
 * FileFormatRunner — radio select file format + Download + Run Command
 *
 * Props:
 *   data: array of row objects
 *   slug: string for filename base
 *   schemaName: string for Avro/Parquet schema name (default 'Record')
 *   tableName: string for SQL command (default slug-based)
 */
function FileFormatRunner({ data, slug = 'data', schemaName = 'Record', tableName }) {
  const [selectedFormat, setSelectedFormat] = useState('csv');
  const [running, setRunning] = useState(false);
  const [runResult, setRunResult] = useState(null);

  // Scheduling state
  const [showScheduler, setShowScheduler] = useState(false);
  const [schedule, setSchedule] = useState({
    frequency: 'daily',
    time: '02:00',
    cluster: 'job-cluster-small',
    retries: 3,
    timeout: 3600,
    notify: true,
  });
  const [scheduled, setScheduled] = useState(null);

  const fmt = FORMAT_OPTIONS.find((f) => f.id === selectedFormat) || FORMAT_OPTIONS[1];
  const tbl = tableName || `catalog.bronze.${slug.replace(/-/g, '_')}`;

  const cronFor = (freq, time) => {
    const [hh, mm] = (time || '02:00').split(':');
    switch (freq) {
      case 'hourly':
        return '0 0 * * * ?';
      case 'daily':
        return `0 ${mm} ${hh} * * ?`;
      case 'weekly':
        return `0 ${mm} ${hh} ? * MON`;
      case 'monthly':
        return `0 ${mm} ${hh} 1 * ?`;
      case 'every-5-min':
        return '0 */5 * * * ?';
      case 'every-15-min':
        return '0 */15 * * * ?';
      default:
        return '0 0 2 * * ?';
    }
  };

  const handleSchedule = () => {
    const jobId = `job_${Math.floor(Math.random() * 1000000)
      .toString()
      .padStart(6, '0')}`;
    const nextRun = new Date(Date.now() + Math.floor(Math.random() * 3600 * 1000) + 60000);
    setScheduled({
      jobId,
      jobName: `${slug}_${selectedFormat}_pipeline`,
      status: 'SCHEDULED',
      cron: cronFor(schedule.frequency, schedule.time),
      frequency: schedule.frequency,
      time: schedule.time,
      cluster: schedule.cluster,
      retries: schedule.retries,
      timeout: schedule.timeout,
      notify: schedule.notify,
      nextRun: nextRun.toISOString().replace('T', ' ').slice(0, 19) + ' UTC',
      createdAt: new Date().toISOString().replace('T', ' ').slice(0, 19) + ' UTC',
    });
    setShowScheduler(false);
  };

  const handleRun = () => {
    setRunning(true);
    setRunResult(null);
    setTimeout(
      () => {
        setRunning(false);
        setRunResult({
          status: 'SUCCESS',
          rows: data.length,
          format: fmt.label,
          file: `${slug}${fmt.ext}`,
          duration: (Math.random() * 2 + 0.5).toFixed(2),
          readCommand: fmt.readCmd.replace(/\{slug\}/g, slug),
          writeCommand: fmt.writeCmd.replace(/\{slug\}/g, slug),
        });
      },
      Math.floor(Math.random() * 1500) + 800
    );
  };

  return (
    <div
      style={{
        marginBottom: '0.85rem',
        border: '1px solid #e2e8f0',
        borderRadius: '10px',
        overflow: 'hidden',
      }}
    >
      {/* Header */}
      <div
        style={{
          background: 'linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%)',
          padding: '0.65rem 1rem',
          borderBottom: '1px solid #e2e8f0',
          display: 'flex',
          alignItems: 'center',
          gap: '0.75rem',
        }}
      >
        <span style={{ fontWeight: 700, fontSize: '0.82rem', color: '#334155' }}>
          Select File Format:
        </span>
        <span style={{ fontSize: '0.75rem', color: '#94a3b8' }}>
          Choose format → Download or Run
        </span>
      </div>

      {/* Radio Options */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(170px, 1fr))',
          gap: '0.5rem',
          padding: '0.75rem 1rem',
        }}
      >
        {FORMAT_OPTIONS.map((f) => {
          const isSelected = selectedFormat === f.id;
          return (
            <label
              key={f.id}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem',
                padding: '0.5rem 0.65rem',
                borderRadius: '8px',
                cursor: 'pointer',
                border: isSelected ? '2px solid #3b82f6' : '1px solid #e2e8f0',
                background: isSelected ? '#eff6ff' : '#fff',
                transition: 'all 0.15s',
              }}
            >
              <input
                type="radio"
                name={`fmt-${slug}`}
                value={f.id}
                checked={isSelected}
                onChange={() => {
                  setSelectedFormat(f.id);
                  setRunResult(null);
                }}
                style={{ accentColor: '#3b82f6' }}
              />
              <span style={{ fontSize: '1rem' }}>{f.icon}</span>
              <span
                style={{
                  fontSize: '0.82rem',
                  fontWeight: isSelected ? 700 : 500,
                  color: isSelected ? '#1e40af' : '#334155',
                }}
              >
                {f.label}
              </span>
            </label>
          );
        })}
      </div>

      {/* Action Buttons */}
      <div
        style={{
          display: 'flex',
          gap: '0.65rem',
          padding: '0 1rem 0.75rem',
          flexWrap: 'wrap',
          alignItems: 'center',
        }}
      >
        <button
          className="btn btn-primary btn-sm"
          onClick={handleRun}
          disabled={running}
          style={{ minWidth: '120px' }}
        >
          {running ? '⏳ Running...' : '▶ Run Command'}
        </button>
        <button
          className="btn btn-secondary btn-sm"
          onClick={() => doExport(selectedFormat, data, slug, schemaName)}
        >
          ⬇ Download {fmt.label}
        </button>
        <button
          className="btn btn-sm"
          onClick={() => setShowScheduler(!showScheduler)}
          style={{
            background: showScheduler ? '#7c3aed' : '#f5f3ff',
            color: showScheduler ? '#fff' : '#6d28d9',
            border: '1px solid #ddd6fe',
            fontWeight: 600,
          }}
        >
          📅 Schedule Job
        </button>
        <span style={{ fontSize: '0.75rem', color: '#94a3b8', marginLeft: 'auto' }}>
          {data.length} rows · {fmt.label} format
        </span>
      </div>

      {/* Scheduler Panel */}
      {showScheduler && (
        <div
          style={{
            margin: '0 1rem 0.75rem',
            padding: '1rem 1.15rem',
            background: 'linear-gradient(135deg, #faf5ff 0%, #f5f3ff 100%)',
            border: '1px solid #ddd6fe',
            borderLeft: '4px solid #7c3aed',
            borderRadius: '10px',
          }}
        >
          <div
            style={{
              fontSize: '0.75rem',
              fontWeight: 800,
              textTransform: 'uppercase',
              letterSpacing: '0.08em',
              color: '#6d28d9',
              marginBottom: '0.85rem',
            }}
          >
            📅 Schedule as Background Job — Databricks Workflow
          </div>

          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
              gap: '0.75rem',
              marginBottom: '0.85rem',
            }}
          >
            <Field label="Frequency">
              <select
                value={schedule.frequency}
                onChange={(e) => setSchedule({ ...schedule, frequency: e.target.value })}
                style={selectStyle}
              >
                <option value="every-5-min">Every 5 minutes</option>
                <option value="every-15-min">Every 15 minutes</option>
                <option value="hourly">Hourly</option>
                <option value="daily">Daily</option>
                <option value="weekly">Weekly (Mon)</option>
                <option value="monthly">Monthly (1st)</option>
              </select>
            </Field>

            <Field label="Run Time (UTC)">
              <input
                type="time"
                value={schedule.time}
                onChange={(e) => setSchedule({ ...schedule, time: e.target.value })}
                style={selectStyle}
              />
            </Field>

            <Field label="Cluster">
              <select
                value={schedule.cluster}
                onChange={(e) => setSchedule({ ...schedule, cluster: e.target.value })}
                style={selectStyle}
              >
                <option value="job-cluster-small">Job Cluster — Small (2 nodes)</option>
                <option value="job-cluster-medium">Job Cluster — Medium (4 nodes)</option>
                <option value="job-cluster-large">Job Cluster — Large (8 nodes)</option>
                <option value="serverless">Serverless (auto-scale)</option>
                <option value="existing-shared">Existing — shared cluster</option>
              </select>
            </Field>

            <Field label="Max Retries">
              <input
                type="number"
                min="0"
                max="10"
                value={schedule.retries}
                onChange={(e) => setSchedule({ ...schedule, retries: +e.target.value })}
                style={selectStyle}
              />
            </Field>

            <Field label="Timeout (seconds)">
              <input
                type="number"
                min="60"
                step="60"
                value={schedule.timeout}
                onChange={(e) => setSchedule({ ...schedule, timeout: +e.target.value })}
                style={selectStyle}
              />
            </Field>

            <Field label="Email on Failure">
              <label
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.4rem',
                  padding: '0.4rem 0',
                }}
              >
                <input
                  type="checkbox"
                  checked={schedule.notify}
                  onChange={(e) => setSchedule({ ...schedule, notify: e.target.checked })}
                  style={{ accentColor: '#7c3aed' }}
                />
                <span style={{ fontSize: '0.82rem', color: '#3b0764' }}>Notify on-call</span>
              </label>
            </Field>
          </div>

          {/* Cron preview */}
          <div
            style={{
              padding: '0.55rem 0.8rem',
              background: '#1e1b4b',
              color: '#c7d2fe',
              borderRadius: '6px',
              fontFamily: 'Fira Code, Consolas, monospace',
              fontSize: '0.78rem',
              marginBottom: '0.85rem',
            }}
          >
            <span style={{ color: '#a5b4fc', fontWeight: 700 }}>cron:</span>{' '}
            {cronFor(schedule.frequency, schedule.time)}
          </div>

          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <button
              className="btn btn-sm"
              onClick={handleSchedule}
              style={{
                background: '#7c3aed',
                color: '#fff',
                fontWeight: 600,
                padding: '0.5rem 1rem',
              }}
            >
              ✓ Create Scheduled Job
            </button>
            <button className="btn btn-sm btn-secondary" onClick={() => setShowScheduler(false)}>
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Scheduled Job Confirmation */}
      {scheduled && (
        <div
          style={{
            margin: '0 1rem 0.75rem',
            borderRadius: '8px',
            overflow: 'hidden',
            border: '1px solid #c4b5fd',
          }}
        >
          <div
            style={{
              background: 'linear-gradient(135deg, #faf5ff 0%, #ede9fe 100%)',
              padding: '0.6rem 0.85rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.75rem',
              flexWrap: 'wrap',
              borderBottom: '1px solid #c4b5fd',
            }}
          >
            <span style={{ color: '#6d28d9', fontWeight: 700, fontSize: '0.82rem' }}>
              📅 {scheduled.status}
            </span>
            <span style={{ fontSize: '0.78rem', color: '#5b21b6' }}>
              {scheduled.jobId} · {scheduled.frequency} @ {scheduled.time} UTC
            </span>
            <span style={{ marginLeft: 'auto', fontSize: '0.75rem', color: '#5b21b6' }}>
              Next run: <strong>{scheduled.nextRun}</strong>
            </span>
          </div>

          <div style={{ padding: '0.75rem 0.9rem', background: '#fff' }}>
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
                gap: '0.5rem',
                marginBottom: '0.8rem',
              }}
            >
              <InfoTile label="Job Name" value={scheduled.jobName} mono />
              <InfoTile label="Cluster" value={scheduled.cluster} />
              <InfoTile label="Max Retries" value={String(scheduled.retries)} />
              <InfoTile label="Timeout" value={`${scheduled.timeout}s`} />
              <InfoTile label="Cron" value={scheduled.cron} mono />
              <InfoTile label="Alerts" value={scheduled.notify ? 'Email on failure ✓' : 'Off'} />
            </div>

            {/* Databricks Jobs API JSON */}
            <div style={{ marginBottom: '0.65rem' }}>
              <div
                style={{
                  fontSize: '0.68rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                  color: '#6d28d9',
                  letterSpacing: '0.04em',
                  marginBottom: '0.3rem',
                }}
              >
                Databricks Jobs API — POST /api/2.1/jobs/create
              </div>
              <pre
                style={{
                  margin: 0,
                  padding: '0.65rem 0.85rem',
                  background: '#1e293b',
                  color: '#e0e7ff',
                  borderRadius: '6px',
                  fontFamily: 'Fira Code, Consolas, monospace',
                  fontSize: '0.75rem',
                  lineHeight: 1.55,
                  whiteSpace: 'pre-wrap',
                  overflowX: 'auto',
                }}
              >
                {JSON.stringify(
                  {
                    name: scheduled.jobName,
                    schedule: {
                      quartz_cron_expression: scheduled.cron,
                      timezone_id: 'UTC',
                      pause_status: 'UNPAUSED',
                    },
                    job_clusters: [
                      {
                        job_cluster_key: scheduled.cluster,
                        new_cluster: {
                          node_type_id: 'Standard_DS3_v2',
                          num_workers: 2,
                          spark_version: '14.3.x-scala2.12',
                        },
                      },
                    ],
                    tasks: [
                      {
                        task_key: `${slug}_task`,
                        notebook_task: { notebook_path: `/Workflows/${slug}`, source: 'WORKSPACE' },
                        job_cluster_key: scheduled.cluster,
                        timeout_seconds: scheduled.timeout,
                        max_retries: scheduled.retries,
                      },
                    ],
                    email_notifications: scheduled.notify
                      ? { on_failure: ['oncall@company.com'] }
                      : {},
                    format: 'MULTI_TASK',
                  },
                  null,
                  2
                )}
              </pre>
            </div>

            {/* Databricks CLI command */}
            <div>
              <div
                style={{
                  fontSize: '0.68rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                  color: '#16a34a',
                  letterSpacing: '0.04em',
                  marginBottom: '0.3rem',
                }}
              >
                Databricks CLI Equivalent
              </div>
              <div className="code-block" style={{ fontSize: '0.8rem', padding: '0.5rem 0.75rem' }}>
                databricks jobs create --json-file {scheduled.jobName}.json
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Run Result */}
      {runResult && (
        <div
          style={{
            margin: '0 1rem 0.75rem',
            borderRadius: '8px',
            overflow: 'hidden',
            border: '1px solid #bbf7d0',
          }}
        >
          <div
            style={{
              background: '#f0fdf4',
              padding: '0.5rem 0.85rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.75rem',
              flexWrap: 'wrap',
              borderBottom: '1px solid #bbf7d0',
            }}
          >
            <span style={{ color: '#16a34a', fontWeight: 700, fontSize: '0.82rem' }}>
              ✅ {runResult.status}
            </span>
            <span style={{ fontSize: '0.78rem', color: '#166534' }}>
              {runResult.rows} rows · {runResult.format} · {runResult.duration}s
            </span>
            <span
              style={{
                marginLeft: 'auto',
                fontSize: '0.75rem',
                color: '#166534',
                fontFamily: 'monospace',
              }}
            >
              {runResult.file}
            </span>
          </div>
          <div style={{ padding: '0.65rem 0.85rem', background: '#fff' }}>
            <div style={{ marginBottom: '0.65rem' }}>
              <div
                style={{
                  fontSize: '0.68rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                  color: '#1d4ed8',
                  letterSpacing: '0.04em',
                  marginBottom: '0.3rem',
                }}
              >
                Read Command (PySpark)
              </div>
              <div className="code-block" style={{ fontSize: '0.8rem', padding: '0.5rem 0.75rem' }}>
                {runResult.readCommand}
              </div>
            </div>
            <div>
              <div
                style={{
                  fontSize: '0.68rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                  color: '#16a34a',
                  letterSpacing: '0.04em',
                  marginBottom: '0.3rem',
                }}
              >
                Write Command (PySpark)
              </div>
              <div className="code-block" style={{ fontSize: '0.8rem', padding: '0.5rem 0.75rem' }}>
                {runResult.writeCommand}
              </div>
            </div>
          </div>
          <div
            style={{
              padding: '0.65rem 0.85rem',
              background: 'linear-gradient(135deg, #1e293b 0%, #334155 100%)',
              color: '#e2e8f0',
            }}
          >
            <div
              style={{
                fontSize: '0.68rem',
                fontWeight: 700,
                textTransform: 'uppercase',
                color: '#93c5fd',
                letterSpacing: '0.04em',
                marginBottom: '0.35rem',
              }}
            >
              Spark SQL — Create Table
            </div>
            <pre
              style={{
                margin: 0,
                fontFamily: 'Fira Code, Consolas, monospace',
                fontSize: '0.78rem',
                lineHeight: 1.6,
                color: '#e0e7ff',
                whiteSpace: 'pre-wrap',
              }}
            >
              {`CREATE TABLE IF NOT EXISTS ${tbl}\nUSING ${selectedFormat === 'parquet' ? 'PARQUET' : selectedFormat === 'avro' ? 'AVRO' : selectedFormat === 'xml' ? 'XML' : selectedFormat === 'json' ? 'JSON' : selectedFormat === 'csv' ? 'CSV' : 'TEXT'}\nOPTIONS (path "dbfs:/data/${runResult.file}")${selectedFormat === 'csv' ? "\n  , (header 'true')" : ''}${selectedFormat === 'xml' ? "\n  , (rowTag 'record')" : ''};`}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Sub-components & styles ──────────────────────────────────────────
const selectStyle = {
  width: '100%',
  padding: '0.4rem 0.55rem',
  border: '1px solid #ddd6fe',
  borderRadius: '6px',
  fontSize: '0.82rem',
  color: '#3b0764',
  background: '#fff',
};

function Field({ label, children }) {
  return (
    <div>
      <div
        style={{
          fontSize: '0.68rem',
          fontWeight: 700,
          textTransform: 'uppercase',
          color: '#6d28d9',
          letterSpacing: '0.04em',
          marginBottom: '0.3rem',
        }}
      >
        {label}
      </div>
      {children}
    </div>
  );
}

function InfoTile({ label, value, mono }) {
  return (
    <div
      style={{
        padding: '0.45rem 0.65rem',
        background: '#faf5ff',
        border: '1px solid #e9d5ff',
        borderRadius: '6px',
      }}
    >
      <div
        style={{
          fontSize: '0.62rem',
          fontWeight: 700,
          textTransform: 'uppercase',
          color: '#6d28d9',
          letterSpacing: '0.04em',
          marginBottom: '0.15rem',
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: '0.8rem',
          color: '#3b0764',
          fontFamily: mono ? 'Fira Code, Consolas, monospace' : 'inherit',
        }}
      >
        {value}
      </div>
    </div>
  );
}

export default FileFormatRunner;
