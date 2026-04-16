/**
 * ScenarioCard — Reusable Input/Process/Output + Code Approaches component
 *
 * Wraps ANY scenario with:
 *   1. Source → Target data path
 *   2. Before data table (pre-process)
 *   3. "Run Process" button
 *   4. After data table (post-process)
 *   5. Stats comparison (rows/nulls/dupes)
 *   6. Multiple code approach tabs (Pseudo, PySpark, SQL, DLT)
 *
 * Usage:
 *   <ScenarioCard scenario={scenario} isExpanded={true} />
 */

import React, { useState } from 'react';

// Generate sample before/after data based on scenario title/category
function generateSampleData(scenario) {
  const title = (scenario.title || '').toLowerCase();
  const cat = (scenario.category || scenario.group || '').toLowerCase();

  // Generate contextual before data
  const beforeRows = [
    { col_1: 'raw_val_1', col_2: 100, col_3: '2024-01-15', col_4: 'active', col_5: null },
    { col_1: 'raw_val_2', col_2: -5, col_3: 'bad_date', col_4: '', col_5: 'data' },
    { col_1: 'raw_val_1', col_2: 100, col_3: '2024-01-15', col_4: 'active', col_5: null },
    { col_1: null, col_2: 250, col_3: '2024-02-20', col_4: 'paused', col_5: 'ok' },
  ];

  const afterRows = [
    {
      col_1: 'raw_val_1',
      col_2: 100,
      col_3: '2024-01-15',
      col_4: 'active',
      col_5: 'N/A',
      _processed: '2024-04-15',
    },
    {
      col_1: 'raw_val_2',
      col_2: 0,
      col_3: '1970-01-01',
      col_4: 'unknown',
      col_5: 'data',
      _processed: '2024-04-15',
    },
    {
      col_1: 'UNKNOWN',
      col_2: 250,
      col_3: '2024-02-20',
      col_4: 'paused',
      col_5: 'ok',
      _processed: '2024-04-15',
    },
  ];

  // Customize based on scenario type
  if (title.includes('customer') || title.includes('crm') || cat.includes('customer')) {
    return {
      source: {
        path: 'source_system/customers',
        format: cat.includes('stream') ? 'Kafka' : 'JDBC/CSV',
      },
      target: { path: 'catalog.silver.customers', format: 'Delta' },
      before: [
        {
          customer_id: 'C001',
          name: ' Alice ',
          email: 'ALICE@TEST.COM',
          segment: null,
          created: '2024-01-15',
        },
        {
          customer_id: 'C002',
          name: 'Bob',
          email: 'bob@test.com',
          segment: 'Enterprise',
          created: '01/20/2024',
        },
        {
          customer_id: 'C001',
          name: ' Alice ',
          email: 'ALICE@TEST.COM',
          segment: null,
          created: '2024-01-15',
        },
        { customer_id: null, name: 'Charlie', email: 'invalid', segment: '', created: 'bad_date' },
      ],
      after: [
        {
          customer_id: 'C001',
          name: 'Alice',
          email: 'alice@test.com',
          segment: 'Unknown',
          created: '2024-01-15',
          _ts: '2024-04-15',
        },
        {
          customer_id: 'C002',
          name: 'Bob',
          email: 'bob@test.com',
          segment: 'Enterprise',
          created: '2024-01-20',
          _ts: '2024-04-15',
        },
      ],
      beforeStats: { rows: 50000, nulls: 1200, dupes: 800 },
      afterStats: { rows: 48000, nulls: 0, dupes: 0 },
    };
  }

  if (
    title.includes('order') ||
    title.includes('sales') ||
    title.includes('transaction') ||
    title.includes('pos')
  ) {
    return {
      source: {
        path: 'source_system/orders',
        format: cat.includes('stream') ? 'Kafka' : 'JDBC/CSV',
      },
      target: { path: 'catalog.silver.orders', format: 'Delta' },
      before: [
        {
          order_id: 1001,
          customer_id: 'C001',
          amount: 150.5,
          status: 'SHIPPED',
          order_date: '2024-01-15',
        },
        { order_id: 1002, customer_id: 'C002', amount: 0, status: null, order_date: '01/20/2024' },
        {
          order_id: 1001,
          customer_id: 'C001',
          amount: 150.5,
          status: 'SHIPPED',
          order_date: '2024-01-15',
        },
        {
          order_id: 1003,
          customer_id: null,
          amount: -100,
          status: 'PENDING',
          order_date: 'INVALID',
        },
      ],
      after: [
        {
          order_id: 1001,
          customer_id: 'C001',
          amount: 150.5,
          status: 'SHIPPED',
          order_date: '2024-01-15',
          _ts: '2024-04-15',
        },
        {
          order_id: 1002,
          customer_id: 'C002',
          amount: 0,
          status: 'UNKNOWN',
          order_date: '2024-01-20',
          _ts: '2024-04-15',
        },
      ],
      beforeStats: { rows: 100000, nulls: 3500, dupes: 1200 },
      afterStats: { rows: 97300, nulls: 0, dupes: 0 },
    };
  }

  if (
    title.includes('model') ||
    title.includes('ml') ||
    title.includes('train') ||
    title.includes('feature') ||
    cat.includes('ml')
  ) {
    return {
      source: { path: 'catalog.silver.features', format: 'Delta' },
      target: { path: 'catalog.gold.predictions', format: 'Delta' },
      before: [
        { id: 1, feature_1: 0.85, feature_2: 12, feature_3: 'A', label: 1 },
        { id: 2, feature_1: 0.23, feature_2: 45, feature_3: 'B', label: 0 },
        { id: 3, feature_1: null, feature_2: -1, feature_3: null, label: 1 },
      ],
      after: [
        { id: 1, prediction: 0.92, confidence: 'HIGH', model_version: 'v2.1' },
        { id: 2, prediction: 0.15, confidence: 'LOW', model_version: 'v2.1' },
      ],
      beforeStats: { rows: 50000, nulls: 500, dupes: 0 },
      afterStats: { rows: 50000, nulls: 0, dupes: 0 },
    };
  }

  // Default generic
  return {
    source: { path: '/mnt/landing/data', format: 'CSV/JSON' },
    target: { path: 'catalog.silver.processed', format: 'Delta' },
    before: beforeRows,
    after: afterRows,
    beforeStats: { rows: 10000, nulls: 250, dupes: 120 },
    afterStats: { rows: 9630, nulls: 0, dupes: 0 },
  };
}

// Generate explanation for the scenario (what/why/when/how)
function generateExplanation(scenario) {
  const title = scenario.title || 'Scenario';
  const desc = scenario.desc || scenario.description || '';
  const category = scenario.category || scenario.group || '';
  const flow = scenario.flow || '';

  const titleLower = title.toLowerCase();

  // What the scenario does
  const what =
    desc || `This scenario demonstrates "${title}" — a ${category} pattern for data processing.`;

  // Why it matters
  let why = 'Critical for reliable, production-grade data pipelines at enterprise scale.';
  if (titleLower.includes('cdc') || titleLower.includes('change')) {
    why =
      'Essential for keeping downstream systems in sync with source data changes without full reloads.';
  } else if (titleLower.includes('scd')) {
    why =
      'Enables historical tracking of dimension changes, critical for point-in-time analytics and audit.';
  } else if (titleLower.includes('pii') || titleLower.includes('mask')) {
    why =
      'Required for regulatory compliance (GDPR, HIPAA, PCI) and protecting sensitive customer data.';
  } else if (titleLower.includes('stream')) {
    why =
      'Delivers real-time insights for time-sensitive use cases like fraud detection and live dashboards.';
  } else if (titleLower.includes('merge') || titleLower.includes('upsert')) {
    why = 'Efficient way to apply inserts, updates, and deletes atomically in a single operation.';
  } else if (titleLower.includes('dedup')) {
    why = 'Prevents data duplication that skews metrics and wastes storage/compute.';
  } else if (titleLower.includes('quality') || titleLower.includes('validation')) {
    why = 'Catches bad data early to prevent garbage reaching business users and ML models.';
  } else if (titleLower.includes('encrypt') || titleLower.includes('security')) {
    why = 'Protects data at rest and in transit, required for compliance and breach prevention.';
  } else if (titleLower.includes('audit') || titleLower.includes('lineage')) {
    why = 'Provides traceability required for compliance, debugging, and trust in data.';
  } else if (titleLower.includes('kafka') || titleLower.includes('event')) {
    why =
      'Enables event-driven architecture with exactly-once processing and backpressure handling.';
  } else if (titleLower.includes('ingestion') || titleLower.includes('load')) {
    why = 'Foundation of the data pipeline — reliable ingestion determines downstream quality.';
  } else if (titleLower.includes('feature') || titleLower.includes('ml')) {
    why = 'Feature consistency between training and serving is critical for ML model accuracy.';
  } else if (titleLower.includes('rag') || titleLower.includes('embed')) {
    why = 'Core building block of GenAI applications — quality retrieval depends on good chunking.';
  } else if (titleLower.includes('partition') || titleLower.includes('optim')) {
    why = 'Performance directly impacts query cost and user experience at scale.';
  }

  // When to use
  let when =
    'Use when building production data pipelines with enterprise reliability requirements.';
  if (titleLower.includes('cdc') || titleLower.includes('incremental')) {
    when = 'Use when source tables change frequently and full reloads are too expensive.';
  } else if (titleLower.includes('scd type 2')) {
    when =
      'Use when business needs history of changes (customer lifecycle, pricing history, etc.).';
  } else if (titleLower.includes('scd type 1')) {
    when = 'Use when only current state matters and history is not needed.';
  } else if (titleLower.includes('batch')) {
    when = 'Use for periodic processing (hourly, daily) where real-time is not required.';
  } else if (titleLower.includes('stream') || titleLower.includes('real-time')) {
    when = 'Use when latency requirements are seconds/minutes, not hours.';
  } else if (titleLower.includes('pii') || titleLower.includes('mask')) {
    when = 'Use for any dataset containing personally identifiable or regulated information.';
  } else if (titleLower.includes('reconcil')) {
    when = 'Use to verify data consistency between source and target after critical loads.';
  } else if (titleLower.includes('dr') || titleLower.includes('disaster')) {
    when = 'Use for business-critical data requiring multi-region availability (RPO < 1hr).';
  }

  // Key steps
  const steps = [
    'Connect to source system and verify access',
    'Extract data with proper schema handling',
    'Validate data quality and schema',
    'Transform according to business rules',
    'Load to target (Delta Lake) with audit trail',
    'Verify row counts match expectations',
  ];

  // Business impact / use cases
  let impact = 'Improves data reliability, reduces operational cost, enables trusted analytics.';
  if (titleLower.includes('fraud') || titleLower.includes('risk')) {
    impact = 'Directly prevents financial loss by catching suspicious patterns in real-time.';
  } else if (titleLower.includes('customer') || titleLower.includes('crm')) {
    impact = 'Improves customer experience through accurate, unified customer view.';
  } else if (titleLower.includes('finance') || titleLower.includes('erp')) {
    impact = 'Ensures financial reporting accuracy and regulatory compliance.';
  } else if (titleLower.includes('cost') || titleLower.includes('finops')) {
    impact = 'Reduces cloud spend by identifying waste and optimizing resource usage.';
  }

  return { what, why, when, steps, impact, flow, category };
}

// Generate code approaches from original code
function generateApproaches(scenario) {
  const code = scenario.code || '# No code provided';
  const title = scenario.title || 'Scenario';

  return [
    {
      name: 'Pseudo Code',
      icon: '\ud83d\udccb',
      code: `ALGORITHM: ${title.toUpperCase()}\n${'='.repeat(Math.min(title.length + 11, 50))}\n\nSTEPS:\n  1. CONNECT to source system\n  2. EXTRACT data (read from source)\n  3. VALIDATE schema + data quality\n  4. TRANSFORM (clean, enrich, standardize)\n  5. LOAD to target (Delta table)\n  6. VERIFY row counts + quality\n  7. LOG audit trail\n\nERROR HANDLING:\n  - Retry on failure (3x with backoff)\n  - Quarantine bad records\n  - Alert on SLA breach`,
    },
    {
      name: 'PySpark',
      icon: '\u26a1',
      code: code,
    },
    {
      name: 'Spark SQL',
      icon: '\ud83d\udcdd',
      code: `-- SQL approach for: ${title}\nCREATE OR REPLACE TABLE catalog.silver.target_table AS\nSELECT *, current_timestamp() AS _processed_ts\nFROM catalog.bronze.source_table\nWHERE id IS NOT NULL\nQUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) = 1;`,
    },
    {
      name: 'DLT',
      icon: '\ud83d\ude80',
      code: `# Delta Live Tables approach\nimport dlt\n\n@dlt.table(name="target_table", comment="${title}")\n@dlt.expect_or_drop("valid_id", "id IS NOT NULL")\ndef process():\n    return dlt.read("source_table")`,
    },
  ];
}

// Mini data table
function MiniTable({ rows, badge, badgeColor }) {
  if (!rows || rows.length === 0) return null;
  const headers = Object.keys(rows[0]);
  return (
    <div
      style={{
        overflowX: 'auto',
        maxHeight: '200px',
        overflowY: 'auto',
        border: '1px solid var(--border)',
        borderRadius: '6px',
      }}
    >
      <table style={{ fontSize: '0.72rem', width: '100%' }}>
        <thead>
          <tr>
            {headers.map((h) => (
              <th
                key={h}
                style={{
                  padding: '0.3rem 0.5rem',
                  background: badgeColor || '#f5f5f5',
                  fontSize: '0.68rem',
                  whiteSpace: 'nowrap',
                }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri}>
              {headers.map((h) => {
                const v = row[h];
                const isNull = v === null || v === undefined || v === '';
                return (
                  <td
                    key={h}
                    style={{
                      padding: '0.25rem 0.5rem',
                      whiteSpace: 'nowrap',
                      background: isNull ? '#fef2f2' : undefined,
                      color: isNull
                        ? '#991b1b'
                        : typeof v === 'number' && v < 0
                          ? '#dc2626'
                          : undefined,
                    }}
                  >
                    {isNull ? 'NULL' : String(v)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ScenarioCard({ scenario }) {
  const [processed, setProcessed] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [activeTab, setActiveTab] = useState(0);
  const [duration, setDuration] = useState(null);

  const data = generateSampleData(scenario);
  const explanation = generateExplanation(scenario);
  const approaches = generateApproaches(scenario);

  const runProcess = () => {
    setProcessing(true);
    const time = (Math.random() * 3 + 1).toFixed(1);
    setTimeout(
      () => {
        setProcessed(true);
        setProcessing(false);
        setDuration(time);
      },
      Math.floor(Math.random() * 2000) + 1000
    );
  };

  return (
    <div style={{ marginTop: '1rem' }}>
      {/* What is this scenario? — Explanation */}
      <div
        style={{
          marginBottom: '1rem',
          padding: '0.75rem 1rem',
          background: 'linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%)',
          borderRadius: '8px',
          border: '1px solid #bfdbfe',
        }}
      >
        <div
          style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}
        >
          <span style={{ fontSize: '1rem' }}>{'\ud83d\udca1'}</span>
          <strong style={{ fontSize: '0.85rem', color: '#1e40af' }}>What is this scenario?</strong>
        </div>
        <p
          style={{
            fontSize: '0.8rem',
            color: '#1e3a8a',
            marginBottom: '0.6rem',
            lineHeight: '1.5',
          }}
        >
          {explanation.what}
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: '1fr 1fr',
            gap: '0.6rem',
            marginBottom: '0.6rem',
          }}
        >
          <div
            style={{
              padding: '0.5rem 0.7rem',
              background: 'rgba(255,255,255,0.7)',
              borderRadius: '6px',
              borderLeft: '3px solid #3b82f6',
            }}
          >
            <div
              style={{
                fontSize: '0.65rem',
                fontWeight: 700,
                color: '#1e40af',
                textTransform: 'uppercase',
                marginBottom: '0.2rem',
              }}
            >
              {'\ud83c\udfaf'} Why It Matters
            </div>
            <div style={{ fontSize: '0.75rem', color: '#1e3a8a' }}>{explanation.why}</div>
          </div>
          <div
            style={{
              padding: '0.5rem 0.7rem',
              background: 'rgba(255,255,255,0.7)',
              borderRadius: '6px',
              borderLeft: '3px solid #8b5cf6',
            }}
          >
            <div
              style={{
                fontSize: '0.65rem',
                fontWeight: 700,
                color: '#6d28d9',
                textTransform: 'uppercase',
                marginBottom: '0.2rem',
              }}
            >
              {'\ud83d\udd52'} When To Use
            </div>
            <div style={{ fontSize: '0.75rem', color: '#4c1d95' }}>{explanation.when}</div>
          </div>
        </div>
        <div
          style={{
            padding: '0.5rem 0.7rem',
            background: 'rgba(255,255,255,0.7)',
            borderRadius: '6px',
            borderLeft: '3px solid #10b981',
          }}
        >
          <div
            style={{
              fontSize: '0.65rem',
              fontWeight: 700,
              color: '#065f46',
              textTransform: 'uppercase',
              marginBottom: '0.3rem',
            }}
          >
            {'\ud83d\udccb'} Key Steps
          </div>
          <ol style={{ fontSize: '0.72rem', color: '#064e3b', margin: 0, paddingLeft: '1.2rem' }}>
            {explanation.steps.map((step, i) => (
              <li key={i} style={{ marginBottom: '0.15rem' }}>
                {step}
              </li>
            ))}
          </ol>
        </div>
        <div
          style={{
            marginTop: '0.5rem',
            padding: '0.4rem 0.7rem',
            background: 'rgba(253, 224, 71, 0.2)',
            borderRadius: '6px',
            fontSize: '0.72rem',
            color: '#713f12',
          }}
        >
          <strong>{'\ud83d\udcbc'} Business Impact:</strong> {explanation.impact}
        </div>
      </div>

      {/* Source → Target Path */}
      <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
        <div
          style={{
            flex: 1,
            minWidth: '180px',
            padding: '0.5rem 0.75rem',
            background: '#fef2f2',
            borderRadius: '6px',
            border: '1px solid #fecaca',
          }}
        >
          <div
            style={{
              fontSize: '0.6rem',
              color: '#991b1b',
              fontWeight: 700,
              textTransform: 'uppercase',
            }}
          >
            Source
          </div>
          <div style={{ fontSize: '0.8rem', fontFamily: 'monospace', marginTop: '0.15rem' }}>
            {data.source.path}
          </div>
          <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>
            {data.source.format} | {data.beforeStats.rows.toLocaleString()} rows
          </div>
        </div>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            fontSize: '1.5rem',
            color: 'var(--text-secondary)',
          }}
        >
          {'\u2192'}
        </div>
        <div
          style={{
            flex: 1,
            minWidth: '180px',
            padding: '0.5rem 0.75rem',
            background: '#dcfce7',
            borderRadius: '6px',
            border: '1px solid #bbf7d0',
          }}
        >
          <div
            style={{
              fontSize: '0.6rem',
              color: '#166534',
              fontWeight: 700,
              textTransform: 'uppercase',
            }}
          >
            Target
          </div>
          <div style={{ fontSize: '0.8rem', fontFamily: 'monospace', marginTop: '0.15rem' }}>
            {data.target.path}
          </div>
          <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>
            {data.target.format} | {data.afterStats.rows.toLocaleString()} rows
          </div>
        </div>
      </div>

      {/* Before / After Tables */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: '1rem',
          marginBottom: '1rem',
        }}
      >
        <div>
          <div
            style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.3rem' }}
          >
            <span
              style={{
                background: '#fee2e2',
                color: '#991b1b',
                padding: '0.1rem 0.4rem',
                borderRadius: '4px',
                fontSize: '0.6rem',
                fontWeight: 700,
              }}
            >
              BEFORE
            </span>
            <span style={{ fontSize: '0.72rem' }}>
              {data.beforeStats.rows.toLocaleString()} rows | {data.beforeStats.nulls} nulls |{' '}
              {data.beforeStats.dupes} dupes
            </span>
          </div>
          <MiniTable rows={data.before} badgeColor="#fef2f2" />
        </div>
        {processed ? (
          <div>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem',
                marginBottom: '0.3rem',
              }}
            >
              <span
                style={{
                  background: '#dcfce7',
                  color: '#166534',
                  padding: '0.1rem 0.4rem',
                  borderRadius: '4px',
                  fontSize: '0.6rem',
                  fontWeight: 700,
                }}
              >
                AFTER
              </span>
              <span style={{ fontSize: '0.72rem' }}>
                {data.afterStats.rows.toLocaleString()} rows | {data.afterStats.nulls} nulls |{' '}
                {data.afterStats.dupes} dupes
              </span>
            </div>
            <MiniTable rows={data.after} badgeColor="#dcfce7" />
          </div>
        ) : (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              border: '2px dashed var(--border)',
              borderRadius: '6px',
              color: 'var(--text-secondary)',
            }}
          >
            <div style={{ textAlign: 'center', padding: '1rem' }}>
              <div style={{ fontSize: '1.5rem' }}>{processing ? '\u23f3' : '\u25b6\ufe0f'}</div>
              <p style={{ fontSize: '0.8rem' }}>
                {processing ? 'Processing...' : 'Click "Run Process"'}
              </p>
            </div>
          </div>
        )}
      </div>

      {/* Process Button + Stats */}
      <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', marginBottom: '1rem' }}>
        <button className="btn btn-primary btn-sm" disabled={processing} onClick={runProcess}>
          {processing ? '\u23f3 Processing...' : processed ? '\u21bb Re-run' : '\u25b6 Run Process'}
        </button>
        {processed && (
          <div style={{ display: 'flex', gap: '1rem', fontSize: '0.8rem', flexWrap: 'wrap' }}>
            <span style={{ color: 'var(--success)' }}>Completed in {duration}s</span>
            <span>
              Rows: <b style={{ color: '#991b1b' }}>{data.beforeStats.rows.toLocaleString()}</b>{' '}
              {'\u2192'} <b style={{ color: '#166534' }}>{data.afterStats.rows.toLocaleString()}</b>
            </span>
            <span>
              Nulls: <b style={{ color: '#991b1b' }}>{data.beforeStats.nulls}</b> {'\u2192'}{' '}
              <b style={{ color: '#166534' }}>{data.afterStats.nulls}</b>
            </span>
          </div>
        )}
      </div>

      {/* Code Approaches */}
      <div style={{ border: '1px solid var(--border)', borderRadius: '8px', overflow: 'hidden' }}>
        <div
          style={{
            padding: '0.5rem 0.75rem',
            background: '#f8f9fa',
            borderBottom: '1px solid var(--border)',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}
        >
          <strong style={{ fontSize: '0.82rem' }}>{approaches.length} Code Approaches</strong>
          <span style={{ fontSize: '0.68rem', color: 'var(--text-secondary)' }}>
            Click each tab
          </span>
        </div>
        <div
          style={{ display: 'flex', borderBottom: '1px solid var(--border)', overflowX: 'auto' }}
        >
          {approaches.map((a, i) => (
            <button
              key={i}
              onClick={() => setActiveTab(i)}
              style={{
                padding: '0.4rem 0.7rem',
                border: 'none',
                cursor: 'pointer',
                fontSize: '0.73rem',
                whiteSpace: 'nowrap',
                background: activeTab === i ? '#fff' : '#f8f9fa',
                borderBottom:
                  activeTab === i ? '2px solid var(--primary)' : '2px solid transparent',
                color: activeTab === i ? 'var(--primary)' : 'var(--text-secondary)',
                fontWeight: activeTab === i ? 600 : 400,
              }}
            >
              {a.icon} {a.name}
            </button>
          ))}
        </div>
        <div style={{ padding: '0.75rem' }}>
          <div className="code-block" style={{ maxHeight: '300px', overflowY: 'auto' }}>
            {approaches[activeTab]?.code}
          </div>
        </div>
      </div>
    </div>
  );
}

export default ScenarioCard;
