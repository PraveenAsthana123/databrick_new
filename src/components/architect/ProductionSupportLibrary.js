import React, { useState, useMemo } from 'react';
import { exportToCSV } from '../../utils/fileExport';

/**
 * ProductionSupportLibrary — renders L1-L4 support playbooks.
 * Input: grouped L1-L4 data { L1: [...], L2: [...], L3: [...], L4: [...] }
 * Each row has: role, issueType, description, scenario, impact, rootCause, resolution, tools.
 */

const LEVEL_META = {
  L1: {
    title: 'L1 — Monitoring / Alert Handling',
    subtitle: 'Detect + Restart + Escalate',
    color: '#22c55e',
    gradFrom: '#f0fdf4',
    gradTo: '#dcfce7',
    border: '#bbf7d0',
    text: '#14532d',
  },
  L2: {
    title: 'L2 — Technical Investigation',
    subtitle: 'Analyze + Fix pipeline logic',
    color: '#eab308',
    gradFrom: '#fefce8',
    gradTo: '#fef9c3',
    border: '#fde68a',
    text: '#713f12',
  },
  L3: {
    title: 'L3 — Deep Engineering / Code Fix',
    subtitle: 'Code fix + Architecture correction',
    color: '#3b82f6',
    gradFrom: '#eff6ff',
    gradTo: '#dbeafe',
    border: '#bfdbfe',
    text: '#1e3a8a',
  },
  L4: {
    title: 'L4 — Architecture / Platform Level',
    subtitle: 'Redesign + Strategy + Platform Fix',
    color: '#ef4444',
    gradFrom: '#fef2f2',
    gradTo: '#fee2e2',
    border: '#fecaca',
    text: '#7f1d1d',
  },
};

function ProductionSupportLibrary({ pageTitle, pageSubtitle, coverage, levels, csvName, flow }) {
  const [expandedKey, setExpandedKey] = useState(null);
  const [search, setSearch] = useState('');

  const allRows = useMemo(() => {
    const out = [];
    Object.entries(levels).forEach(([lvl, arr]) => {
      arr.forEach((r, i) => out.push({ ...r, level: lvl, key: `${lvl}-${i}` }));
    });
    return out;
  }, [levels]);

  const totalByLevel = useMemo(() => {
    const t = { L1: 0, L2: 0, L3: 0, L4: 0 };
    Object.entries(levels).forEach(([lvl, arr]) => (t[lvl] = arr.length));
    return t;
  }, [levels]);

  const filtered = useMemo(() => {
    if (!search.trim()) return allRows;
    const q = search.toLowerCase();
    return allRows.filter((r) =>
      Object.values(r).some((v) =>
        String(v || '')
          .toLowerCase()
          .includes(q)
      )
    );
  }, [allRows, search]);

  const grouped = useMemo(() => {
    const g = { L1: [], L2: [], L3: [], L4: [] };
    filtered.forEach((r) => g[r.level].push(r));
    return g;
  }, [filtered]);

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>{pageTitle}</h1>
          <p>{pageSubtitle}</p>
        </div>
      </div>

      {coverage && (
        <div
          className="card"
          style={{
            marginBottom: '1rem',
            background: 'linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%)',
            borderLeft: '4px solid #2563eb',
          }}
        >
          <strong style={{ color: '#1e3a8a', fontSize: '0.9rem' }}>Coverage:</strong>
          <p style={{ margin: '0.35rem 0 0 0', color: '#334155', fontSize: '0.88rem' }}>
            {coverage}
          </p>
        </div>
      )}

      <div className="stats-grid">
        {['L1', 'L2', 'L3', 'L4'].map((lvl) => {
          const meta = LEVEL_META[lvl];
          return (
            <div
              key={lvl}
              className="stat-card"
              style={{
                borderLeft: `4px solid ${meta.color}`,
                background: `linear-gradient(135deg, ${meta.gradFrom} 0%, ${meta.gradTo} 100%)`,
              }}
            >
              <div
                style={{
                  width: '44px',
                  height: '44px',
                  borderRadius: '10px',
                  background: meta.color,
                  color: '#fff',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontWeight: 800,
                  fontSize: '0.95rem',
                }}
              >
                {lvl}
              </div>
              <div className="stat-info">
                <h4>{totalByLevel[lvl]}</h4>
                <p style={{ color: meta.text, fontWeight: 600 }}>{meta.subtitle}</p>
              </div>
            </div>
          );
        })}
      </div>

      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search issue types, scenarios, root causes..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ maxWidth: '360px' }}
          />
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Click any issue to see full triage + resolution
          </span>
          <button
            className="btn btn-secondary btn-sm"
            onClick={() => exportToCSV(allRows, csvName)}
            style={{ marginLeft: 'auto' }}
          >
            Download CSV
          </button>
        </div>
      </div>

      {['L1', 'L2', 'L3', 'L4'].map((lvl) => {
        const meta = LEVEL_META[lvl];
        const items = grouped[lvl];
        if (items.length === 0) return null;
        return (
          <div key={lvl} style={{ marginBottom: '1.5rem' }}>
            <div
              style={{
                padding: '0.85rem 1.15rem',
                borderRadius: '10px',
                background: `linear-gradient(135deg, ${meta.gradFrom} 0%, ${meta.gradTo} 100%)`,
                border: `1px solid ${meta.border}`,
                borderLeft: `5px solid ${meta.color}`,
                marginBottom: '0.7rem',
              }}
            >
              <div
                style={{
                  fontSize: '0.95rem',
                  fontWeight: 800,
                  color: meta.text,
                  marginBottom: '0.2rem',
                }}
              >
                {meta.title}
              </div>
              <div style={{ fontSize: '0.8rem', color: meta.text, opacity: 0.85 }}>
                {meta.subtitle} — {items.length} issue{items.length === 1 ? '' : 's'}
              </div>
            </div>

            {items.map((r) => {
              const isOpen = expandedKey === r.key;
              return (
                <div key={r.key} className="card" style={{ marginBottom: '0.5rem' }}>
                  <div
                    onClick={() => setExpandedKey(isOpen ? null : r.key)}
                    style={{
                      cursor: 'pointer',
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
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
                          marginBottom: '0.25rem',
                        }}
                      >
                        <span
                          style={{
                            background: meta.color,
                            color: '#fff',
                            padding: '0.15rem 0.55rem',
                            borderRadius: '4px',
                            fontSize: '0.7rem',
                            fontWeight: 700,
                          }}
                        >
                          {r.level}
                        </span>
                        <strong style={{ fontSize: '0.9rem' }}>{r.issueType}</strong>
                        <span
                          style={{
                            color: 'var(--text-secondary)',
                            fontSize: '0.75rem',
                            marginLeft: '0.25rem',
                          }}
                        >
                          · {r.role}
                        </span>
                      </div>
                      <p
                        style={{
                          color: 'var(--text-secondary)',
                          fontSize: '0.82rem',
                          margin: 0,
                        }}
                      >
                        {r.description}
                      </p>
                    </div>
                    <span style={{ color: 'var(--text-secondary)', flexShrink: 0 }}>
                      {isOpen ? '\u25BC' : '\u25B6'}
                    </span>
                  </div>

                  {isOpen && (
                    <div
                      style={{
                        marginTop: '1rem',
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
                        gap: '0.7rem',
                      }}
                    >
                      <Tile label="Real Scenario" color="#6366f1" bg="#eef2ff" value={r.scenario} />
                      <Tile label="Business Impact" color="#ef4444" bg="#fef2f2" value={r.impact} />
                      <Tile label="Root Cause" color="#f97316" bg="#fff7ed" value={r.rootCause} />
                      <Tile label="Resolution" color="#16a34a" bg="#f0fdf4" value={r.resolution} />
                      <Tile label="Tools / Logs" color="#0891b2" bg="#ecfeff" value={r.tools} />
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        );
      })}

      {flow && (
        <div
          className="card"
          style={{
            marginTop: '1.5rem',
            background: 'linear-gradient(135deg, #1e1b4b 0%, #312e81 100%)',
            color: '#e0e7ff',
          }}
        >
          <div
            style={{
              fontSize: '0.75rem',
              fontWeight: 800,
              textTransform: 'uppercase',
              letterSpacing: '0.08em',
              color: '#a5b4fc',
              marginBottom: '0.75rem',
            }}
          >
            Production Issue Flow
          </div>
          <pre
            style={{
              margin: 0,
              fontFamily: 'Fira Code, monospace',
              fontSize: '0.85rem',
              lineHeight: 1.7,
              color: '#e0e7ff',
              whiteSpace: 'pre-wrap',
            }}
          >
            {flow}
          </pre>
        </div>
      )}
    </div>
  );
}

function Tile({ label, color, bg, value }) {
  return (
    <div
      style={{
        background: bg,
        border: `1px solid ${color}30`,
        borderLeft: `3px solid ${color}`,
        borderRadius: '8px',
        padding: '0.7rem 0.9rem',
      }}
    >
      <div
        style={{
          fontSize: '0.68rem',
          fontWeight: 700,
          textTransform: 'uppercase',
          letterSpacing: '0.05em',
          color,
          marginBottom: '0.3rem',
        }}
      >
        {label}
      </div>
      <p style={{ margin: 0, fontSize: '0.83rem', lineHeight: 1.55, color: '#1a1a1a' }}>
        {value || '—'}
      </p>
    </div>
  );
}

export default ProductionSupportLibrary;
