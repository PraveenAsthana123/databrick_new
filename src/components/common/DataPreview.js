/**
 * DataPreview — Before/After data visualization component
 *
 * Shows:
 *   Left:  Before processing (raw/source data)
 *   Right: After processing (clean/target data)
 *   Bottom: Change log + stats comparison + bar chart
 *
 * Usage:
 *   <DataPreview data={getSampleData(pipelineId)} />
 */

import React, { useState } from 'react';

function MiniBarChart({ data, labelKey, valueKey, color }) {
  if (!data || data.length === 0) return null;
  const maxVal = Math.max(...data.map((d) => Number(d[valueKey]) || 0));

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
      {data.slice(0, 8).map((item, i) => {
        const val = Number(item[valueKey]) || 0;
        const pct = maxVal > 0 ? (val / maxVal) * 100 : 0;
        return (
          <div
            key={i}
            style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '0.75rem' }}
          >
            <span
              style={{
                width: '80px',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                color: 'var(--text-secondary)',
              }}
            >
              {String(item[labelKey] || '').slice(0, 12)}
            </span>
            <div
              style={{
                flex: 1,
                height: '16px',
                background: '#f3f4f6',
                borderRadius: '3px',
                overflow: 'hidden',
              }}
            >
              <div
                style={{
                  width: `${pct}%`,
                  height: '100%',
                  background: color || 'var(--info)',
                  borderRadius: '3px',
                  transition: 'width 0.5s ease',
                }}
              />
            </div>
            <span
              style={{ width: '50px', textAlign: 'right', fontWeight: 500, fontSize: '0.7rem' }}
            >
              {val}
            </span>
          </div>
        );
      })}
    </div>
  );
}

function StatBox({ label, value, color }) {
  return (
    <div
      style={{
        textAlign: 'center',
        padding: '0.5rem',
        background: '#f8f9fa',
        borderRadius: '6px',
        minWidth: '80px',
      }}
    >
      <div style={{ fontSize: '1.1rem', fontWeight: 700, color: color || 'var(--text-primary)' }}>
        {value}
      </div>
      <div style={{ fontSize: '0.65rem', color: 'var(--text-secondary)', marginTop: '2px' }}>
        {label}
      </div>
    </div>
  );
}

function DataTable({ rows, maxRows }) {
  if (!rows || rows.length === 0)
    return <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>No data</p>;
  const headers = Object.keys(rows[0]);
  const displayRows = rows.slice(0, maxRows || 6);

  return (
    <div style={{ overflowX: 'auto', maxHeight: '300px', overflowY: 'auto' }}>
      <table style={{ fontSize: '0.75rem', width: '100%' }}>
        <thead>
          <tr>
            {headers.map((h) => (
              <th
                key={h}
                style={{
                  padding: '0.4rem 0.5rem',
                  background: '#f5f5f5',
                  fontSize: '0.7rem',
                  whiteSpace: 'nowrap',
                }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayRows.map((row, ri) => (
            <tr key={ri}>
              {headers.map((h) => {
                const val = row[h];
                const isNull = val === null || val === undefined || val === '';
                const isNegative = typeof val === 'number' && val < 0;
                return (
                  <td
                    key={h}
                    style={{
                      padding: '0.35rem 0.5rem',
                      whiteSpace: 'nowrap',
                      maxWidth: '150px',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      background: isNull ? '#fef2f2' : undefined,
                      color: isNull ? '#991b1b' : isNegative ? '#dc2626' : undefined,
                    }}
                  >
                    {isNull ? 'NULL' : String(val)}
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

function DataPreview({ data, onClose }) {
  const [activeView, setActiveView] = useState('side-by-side');

  if (!data) return null;

  const { before, after, changes } = data;

  return (
    <div
      style={{
        marginTop: '1rem',
        border: '1px solid var(--border)',
        borderRadius: '8px',
        overflow: 'hidden',
      }}
    >
      {/* Header */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          padding: '0.75rem 1rem',
          background: '#f8f9fa',
          borderBottom: '1px solid var(--border)',
        }}
      >
        <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
          <strong style={{ fontSize: '0.9rem' }}>Data Preview: {data.label}</strong>
          <div style={{ display: 'flex', gap: '2px', marginLeft: '1rem' }}>
            {['side-by-side', 'before', 'after', 'changes', 'chart'].map((v) => (
              <button
                key={v}
                onClick={() => setActiveView(v)}
                style={{
                  padding: '0.2rem 0.6rem',
                  fontSize: '0.7rem',
                  border: '1px solid var(--border)',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  background: activeView === v ? 'var(--primary)' : '#fff',
                  color: activeView === v ? '#fff' : 'var(--text-secondary)',
                }}
              >
                {v === 'side-by-side' ? 'Before vs After' : v.charAt(0).toUpperCase() + v.slice(1)}
              </button>
            ))}
          </div>
        </div>
        {onClose && (
          <button className="btn btn-sm btn-secondary" onClick={onClose}>
            Close
          </button>
        )}
      </div>

      <div style={{ padding: '1rem' }}>
        {/* ═══ Side by Side ═══ */}
        {activeView === 'side-by-side' && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
            {/* Left: Before */}
            <div>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  marginBottom: '0.5rem',
                }}
              >
                <span
                  style={{
                    background: '#fee2e2',
                    color: '#991b1b',
                    padding: '0.15rem 0.5rem',
                    borderRadius: '4px',
                    fontSize: '0.7rem',
                    fontWeight: 600,
                  }}
                >
                  BEFORE
                </span>
                <span style={{ fontSize: '0.8rem', fontWeight: 600 }}>{before.title}</span>
              </div>
              <div
                style={{
                  fontSize: '0.7rem',
                  color: 'var(--text-secondary)',
                  marginBottom: '0.5rem',
                }}
              >
                Path:{' '}
                <code style={{ background: '#f3f4f6', padding: '1px 4px', borderRadius: '3px' }}>
                  {before.path}
                </code>{' '}
                | Format: {before.format}
              </div>
              <div
                style={{
                  display: 'flex',
                  gap: '0.5rem',
                  marginBottom: '0.75rem',
                  flexWrap: 'wrap',
                }}
              >
                <StatBox label="Rows" value={before.stats.rows?.toLocaleString()} />
                <StatBox label="Columns" value={before.stats.columns} />
                <StatBox
                  label="Nulls"
                  value={before.stats.nulls?.toLocaleString()}
                  color="var(--warning)"
                />
                <StatBox
                  label="Duplicates"
                  value={before.stats.duplicates?.toLocaleString()}
                  color="var(--error)"
                />
              </div>
              <DataTable rows={before.rows} />
            </div>

            {/* Right: After */}
            <div>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  marginBottom: '0.5rem',
                }}
              >
                <span
                  style={{
                    background: '#dcfce7',
                    color: '#166534',
                    padding: '0.15rem 0.5rem',
                    borderRadius: '4px',
                    fontSize: '0.7rem',
                    fontWeight: 600,
                  }}
                >
                  AFTER
                </span>
                <span style={{ fontSize: '0.8rem', fontWeight: 600 }}>{after.title}</span>
              </div>
              <div
                style={{
                  fontSize: '0.7rem',
                  color: 'var(--text-secondary)',
                  marginBottom: '0.5rem',
                }}
              >
                Path:{' '}
                <code style={{ background: '#f3f4f6', padding: '1px 4px', borderRadius: '3px' }}>
                  {after.path}
                </code>{' '}
                | Format: {after.format}
              </div>
              <div
                style={{
                  display: 'flex',
                  gap: '0.5rem',
                  marginBottom: '0.75rem',
                  flexWrap: 'wrap',
                }}
              >
                <StatBox
                  label="Rows"
                  value={
                    typeof after.stats.rows === 'number'
                      ? after.stats.rows.toLocaleString()
                      : after.stats.rows
                  }
                />
                <StatBox label="Columns" value={after.stats.columns || after.stats.total_checks} />
                <StatBox
                  label="Nulls"
                  value={after.stats.nulls?.toLocaleString() || '0'}
                  color="var(--success)"
                />
                <StatBox
                  label="Duplicates"
                  value={after.stats.duplicates?.toLocaleString() || '0'}
                  color="var(--success)"
                />
                {after.stats.accuracy && (
                  <StatBox label="Accuracy" value={after.stats.accuracy} color="var(--info)" />
                )}
                {after.stats.score && (
                  <StatBox label="Score" value={after.stats.score} color="var(--info)" />
                )}
              </div>
              <DataTable rows={after.rows} />
            </div>
          </div>
        )}

        {/* ═══ Before Only ═══ */}
        {activeView === 'before' && (
          <div>
            <h4 style={{ marginBottom: '0.5rem' }}>{before.title}</h4>
            <p
              style={{
                fontSize: '0.8rem',
                color: 'var(--text-secondary)',
                marginBottom: '0.75rem',
              }}
            >
              Path: {before.path} | Format: {before.format} | {before.stats.rows?.toLocaleString()}{' '}
              rows | Size: {before.stats.size}
            </p>
            <DataTable rows={before.rows} maxRows={20} />
          </div>
        )}

        {/* ═══ After Only ═══ */}
        {activeView === 'after' && (
          <div>
            <h4 style={{ marginBottom: '0.5rem' }}>{after.title}</h4>
            <p
              style={{
                fontSize: '0.8rem',
                color: 'var(--text-secondary)',
                marginBottom: '0.75rem',
              }}
            >
              Path: {after.path} | Format: {after.format} |{' '}
              {typeof after.stats.rows === 'number'
                ? after.stats.rows.toLocaleString()
                : after.stats.rows}{' '}
              rows | Size: {after.stats.size || 'N/A'}
            </p>
            <DataTable rows={after.rows} maxRows={20} />
          </div>
        )}

        {/* ═══ Changes ═══ */}
        {activeView === 'changes' && (
          <div>
            <h4 style={{ marginBottom: '0.75rem' }}>Change Log ({changes.length} operations)</h4>
            {changes.map((c, i) => {
              const colors = {
                removed: '#fee2e2',
                fixed: '#fef3c7',
                transformed: '#dbeafe',
                added: '#dcfce7',
                detected: '#fef3c7',
                unchanged: '#f3f4f6',
              };
              const textColors = {
                removed: '#991b1b',
                fixed: '#92400e',
                transformed: '#1e40af',
                added: '#166534',
                detected: '#92400e',
                unchanged: '#6b7280',
              };
              return (
                <div
                  key={i}
                  style={{
                    display: 'flex',
                    gap: '0.5rem',
                    alignItems: 'flex-start',
                    padding: '0.5rem 0',
                    borderBottom: '1px solid #f3f4f6',
                  }}
                >
                  <span
                    style={{
                      background: colors[c.type] || '#f3f4f6',
                      color: textColors[c.type] || '#333',
                      padding: '0.15rem 0.5rem',
                      borderRadius: '4px',
                      fontSize: '0.65rem',
                      fontWeight: 600,
                      textTransform: 'uppercase',
                      minWidth: '80px',
                      textAlign: 'center',
                    }}
                  >
                    {c.type}
                  </span>
                  <span style={{ fontSize: '0.8rem' }}>{c.desc}</span>
                </div>
              );
            })}
          </div>
        )}

        {/* ═══ Chart ═══ */}
        {activeView === 'chart' && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
            <div>
              <h4 style={{ fontSize: '0.85rem', marginBottom: '0.75rem', color: '#991b1b' }}>
                Before — Data Quality Issues
              </h4>
              <MiniBarChart
                data={[
                  { metric: 'Rows', value: before.stats.rows },
                  { metric: 'Nulls', value: before.stats.nulls },
                  { metric: 'Duplicates', value: before.stats.duplicates },
                  { metric: 'Columns', value: before.stats.columns },
                ]}
                labelKey="metric"
                valueKey="value"
                color="#ef4444"
              />
            </div>
            <div>
              <h4 style={{ fontSize: '0.85rem', marginBottom: '0.75rem', color: '#166534' }}>
                After — Clean Data
              </h4>
              <MiniBarChart
                data={[
                  {
                    metric: 'Rows',
                    value: typeof after.stats.rows === 'number' ? after.stats.rows : 0,
                  },
                  { metric: 'Nulls', value: after.stats.nulls || 0 },
                  { metric: 'Duplicates', value: after.stats.duplicates || 0 },
                  { metric: 'Columns', value: after.stats.columns || 0 },
                ]}
                labelKey="metric"
                valueKey="value"
                color="#22c55e"
              />
            </div>

            {/* Comparison Stats */}
            <div style={{ gridColumn: '1 / -1' }}>
              <h4 style={{ fontSize: '0.85rem', marginBottom: '0.75rem' }}>
                Before vs After Comparison
              </h4>
              <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
                {[
                  {
                    label: 'Rows',
                    before: before.stats.rows,
                    after: typeof after.stats.rows === 'number' ? after.stats.rows : 0,
                  },
                  { label: 'Nulls', before: before.stats.nulls, after: after.stats.nulls || 0 },
                  {
                    label: 'Duplicates',
                    before: before.stats.duplicates,
                    after: after.stats.duplicates || 0,
                  },
                ].map((item) => {
                  const reduced = item.before - item.after;
                  const pct = item.before > 0 ? Math.round((reduced / item.before) * 100) : 0;
                  return (
                    <div
                      key={item.label}
                      style={{
                        flex: 1,
                        minWidth: '150px',
                        padding: '0.75rem',
                        background: '#f8f9fa',
                        borderRadius: '6px',
                        textAlign: 'center',
                      }}
                    >
                      <div
                        style={{
                          fontSize: '0.7rem',
                          color: 'var(--text-secondary)',
                          marginBottom: '0.25rem',
                        }}
                      >
                        {item.label}
                      </div>
                      <div
                        style={{
                          display: 'flex',
                          justifyContent: 'center',
                          alignItems: 'center',
                          gap: '0.5rem',
                        }}
                      >
                        <span style={{ color: '#991b1b', fontWeight: 600 }}>
                          {item.before?.toLocaleString()}
                        </span>
                        <span style={{ color: 'var(--text-secondary)' }}>&rarr;</span>
                        <span style={{ color: '#166534', fontWeight: 600 }}>
                          {item.after?.toLocaleString()}
                        </span>
                      </div>
                      {reduced > 0 && (
                        <div
                          style={{
                            fontSize: '0.7rem',
                            color: 'var(--success)',
                            marginTop: '0.25rem',
                          }}
                        >
                          -{reduced.toLocaleString()} ({pct}% reduction)
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default DataPreview;
