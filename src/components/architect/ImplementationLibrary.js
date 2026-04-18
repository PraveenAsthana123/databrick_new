import React, { useState, useMemo } from 'react';
import FileFormatRunner from '../common/FileFormatRunner';

/**
 * ImplementationLibrary — renders enterprise implementation tables.
 * Each row has: topic, definition, implementation, task, service,
 * deliverable, validation, testing, comparison, deployment, flow.
 * Expanding a row shows all 11 dimensions as colored tiles.
 */

const TILE_COLORS = [
  { label: 'Definition', field: 'definition', bg: '#eff6ff', fg: '#1e3a8a', border: '#bfdbfe' },
  {
    label: 'Implementation',
    field: 'implementation',
    bg: '#f0fdf4',
    fg: '#14532d',
    border: '#bbf7d0',
  },
  { label: 'Task', field: 'task', bg: '#fefce8', fg: '#713f12', border: '#fde68a' },
  { label: 'Service', field: 'service', bg: '#faf5ff', fg: '#3b0764', border: '#ddd6fe' },
  { label: 'Deliverable', field: 'deliverable', bg: '#fdf4ff', fg: '#581c87', border: '#f5d0fe' },
  { label: 'Validation', field: 'validation', bg: '#ecfeff', fg: '#155e75', border: '#a5f3fc' },
  { label: 'Testing', field: 'testing', bg: '#fff7ed', fg: '#7c2d12', border: '#fed7aa' },
  { label: 'Comparison', field: 'comparison', bg: '#fef2f2', fg: '#7f1d1d', border: '#fecaca' },
  { label: 'Deployment', field: 'deployment', bg: '#f1f5f9', fg: '#0f172a', border: '#cbd5e1' },
  { label: 'Flow', field: 'flow', bg: '#e0f2fe', fg: '#0c4a6e', border: '#7dd3fc' },
];

function ImplementationLibrary({
  pageTitle,
  pageSubtitle,
  statsLabel,
  rows,
  csvName,
  accent = '#2563eb',
}) {
  const [expandedId, setExpandedId] = useState(null);
  const [search, setSearch] = useState('');

  const filtered = useMemo(() => {
    if (!search.trim()) return rows;
    const q = search.toLowerCase();
    return rows.filter((r) =>
      Object.values(r).some((v) =>
        String(v || '')
          .toLowerCase()
          .includes(q)
      )
    );
  }, [rows, search]);

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>{pageTitle}</h1>
          <p>{pageSubtitle}</p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F4D1;</div>
          <div className="stat-info">
            <h4>{rows.length}</h4>
            <p>{statsLabel}</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">&#x1F50D;</div>
          <div className="stat-info">
            <h4>{filtered.length}</h4>
            <p>Showing</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">&#x1F3AF;</div>
          <div className="stat-info">
            <h4>11</h4>
            <p>Dimensions per Topic</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#x1F680;</div>
          <div className="stat-info">
            <h4>{rows.length}</h4>
            <p>Enterprise Patterns</p>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search topics, tools, deliverables..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ maxWidth: '360px' }}
          />
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Click any row to expand all 11 dimensions
          </span>
        </div>
      </div>

      {/* File Format Selector + Run Command */}
      <FileFormatRunner data={rows} slug={csvName.replace(/\.\w+$/, '')} schemaName="Topic" />

      {filtered.length === 0 && (
        <div
          className="card"
          style={{ textAlign: 'center', color: 'var(--text-secondary)', padding: '2rem' }}
        >
          No rows match your search.
        </div>
      )}

      {filtered.map((r) => {
        const isOpen = expandedId === r.id;
        return (
          <div key={r.id} className="card" style={{ marginBottom: '0.75rem' }}>
            <div
              onClick={() => setExpandedId(isOpen ? null : r.id)}
              style={{
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'flex-start',
                gap: '1rem',
              }}
            >
              <div style={{ flex: 1 }}>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    flexWrap: 'wrap',
                    marginBottom: '0.35rem',
                  }}
                >
                  <span
                    style={{
                      background: accent,
                      color: '#fff',
                      padding: '0.15rem 0.55rem',
                      borderRadius: '12px',
                      fontSize: '0.7rem',
                      fontWeight: 700,
                      minWidth: '2.2rem',
                      textAlign: 'center',
                    }}
                  >
                    #{r.id}
                  </span>
                  <strong>{r.topic}</strong>
                </div>
                <p
                  style={{
                    color: 'var(--text-secondary)',
                    fontSize: '0.85rem',
                    margin: 0,
                    lineHeight: 1.55,
                  }}
                >
                  {r.definition}
                </p>
              </div>
              <span style={{ color: 'var(--text-secondary)', flexShrink: 0, marginTop: '2px' }}>
                {isOpen ? '\u25BC' : '\u25B6'}
              </span>
            </div>

            {isOpen && (
              <div
                style={{
                  marginTop: '1.15rem',
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
                  gap: '0.8rem',
                }}
              >
                {TILE_COLORS.map((tile) => (
                  <div
                    key={tile.field}
                    style={{
                      background: tile.bg,
                      border: `1px solid ${tile.border}`,
                      borderLeft: `4px solid ${tile.fg}`,
                      borderRadius: '10px',
                      padding: '0.85rem 1rem',
                    }}
                  >
                    <div
                      style={{
                        fontSize: '0.68rem',
                        fontWeight: 700,
                        textTransform: 'uppercase',
                        letterSpacing: '0.05em',
                        color: tile.fg,
                        marginBottom: '0.4rem',
                      }}
                    >
                      {tile.label}
                    </div>
                    <p
                      style={{
                        margin: 0,
                        fontSize: '0.85rem',
                        lineHeight: 1.6,
                        color: '#1a1a1a',
                      }}
                    >
                      {r[tile.field] || '—'}
                    </p>
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default ImplementationLibrary;
