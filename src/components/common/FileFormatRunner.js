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

  const fmt = FORMAT_OPTIONS.find((f) => f.id === selectedFormat) || FORMAT_OPTIONS[1];
  const tbl = tableName || `catalog.bronze.${slug.replace(/-/g, '_')}`;

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
        <span style={{ fontSize: '0.75rem', color: '#94a3b8', marginLeft: 'auto' }}>
          {data.length} rows · {fmt.label} format
        </span>
      </div>

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

export default FileFormatRunner;
