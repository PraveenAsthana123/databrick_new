import React, { useState, useMemo } from 'react';
import { exportToCSV, exportToJSON, exportToXML, exportToAvro } from '../../utils/fileExport';
import DeepDetailView from './DeepDetailView';

/**
 * ChallengeLibrary — generic page for enterprise-challenge catalogs.
 * Used by IngestionChallenges, ModelingChallenges, GovernanceChallenges,
 * VisualizationChallenges. Each challenge expands into DeepDetailView.
 *
 * Required item shape:
 *   { id, title, description, scenario, impact, rootCause, solution, tools }
 */
function ChallengeLibrary({
  pageTitle,
  pageSubtitle,
  statsLabel,
  totalCount,
  challenges,
  domain, // ingestion | modeling | governance | visualization
  csvName,
}) {
  const [expandedId, setExpandedId] = useState(null);
  const [search, setSearch] = useState('');

  const filtered = useMemo(() => {
    if (!search.trim()) return challenges;
    const q = search.toLowerCase();
    return challenges.filter((c) =>
      [c.title, c.description, c.rootCause, c.solution, c.tools, c.scenario]
        .filter(Boolean)
        .some((f) => f.toLowerCase().includes(q))
    );
  }, [challenges, search]);

  const exportData = challenges.map((c) => ({
    id: c.id,
    title: c.title,
    description: c.description,
    scenario: c.scenario,
    impact: c.impact,
    rootCause: c.rootCause,
    solution: c.solution,
    tools: c.tools,
  }));

  const baseName = csvName.replace(/\.\w+$/, '');

  const downloadAs = (format) => {
    switch (format) {
      case 'csv':
        exportToCSV(exportData, `${baseName}.csv`);
        break;
      case 'json':
        exportToJSON(exportData, `${baseName}.json`);
        break;
      case 'xml':
        exportToXML(exportData, `${baseName}.xml`, 'challenges', 'challenge');
        break;
      case 'avro':
        exportToAvro(exportData, `${baseName}.avro.json`, 'Challenge');
        break;
      default:
        break;
    }
  };

  return (
    <div>
      {/* Page Header */}
      <div className="page-header">
        <div>
          <h1>{pageTitle}</h1>
          <p>{pageSubtitle}</p>
        </div>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">&#x1F4CB;</div>
          <div className="stat-info">
            <h4>{totalCount}</h4>
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
          <div className="stat-icon green">&#x1F4A1;</div>
          <div className="stat-info">
            <h4>{totalCount}</h4>
            <p>Step-by-Step Solutions</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">&#x1F3AF;</div>
          <div className="stat-info">
            <h4>{totalCount}</h4>
            <p>Architect Recommendations</p>
          </div>
        </div>
      </div>

      {/* Search + Multi-format Download */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search challenges, scenarios, tools..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ maxWidth: '360px' }}
          />
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Click any row to expand full detail
          </span>
          <div style={{ marginLeft: 'auto', display: 'flex', gap: '0.4rem', flexWrap: 'wrap' }}>
            {[
              { fmt: 'csv', label: 'CSV', icon: '📄' },
              { fmt: 'json', label: 'JSON', icon: '{ }' },
              { fmt: 'xml', label: 'XML', icon: '< >' },
              { fmt: 'avro', label: 'Avro', icon: '🔷' },
            ].map((b) => (
              <button
                key={b.fmt}
                className="btn btn-secondary btn-sm"
                onClick={() => downloadAs(b.fmt)}
                title={`Download as ${b.label}`}
              >
                {b.icon} {b.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Empty state */}
      {filtered.length === 0 && (
        <div
          className="card"
          style={{ textAlign: 'center', color: 'var(--text-secondary)', padding: '2rem' }}
        >
          No challenges match your search. Try a different keyword.
        </div>
      )}

      {/* Challenge cards */}
      {filtered.map((c) => {
        const isExpanded = expandedId === c.id;
        return (
          <div key={c.id} className="card" style={{ marginBottom: '0.75rem' }}>
            <div
              onClick={() => setExpandedId(isExpanded ? null : c.id)}
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
                    className="badge running"
                    style={{ minWidth: '2.2rem', textAlign: 'center' }}
                  >
                    #{c.id}
                  </span>
                  <strong>{c.title}</strong>
                </div>
                <p
                  style={{
                    color: 'var(--text-secondary)',
                    fontSize: '0.85rem',
                    margin: 0,
                    lineHeight: 1.55,
                  }}
                >
                  {c.description}
                </p>
              </div>
              <span style={{ color: 'var(--text-secondary)', flexShrink: 0, marginTop: '2px' }}>
                {isExpanded ? '\u25BC' : '\u25B6'}
              </span>
            </div>

            {isExpanded && <DeepDetailView item={{ ...c, domain }} />}
          </div>
        );
      })}
    </div>
  );
}

export default ChallengeLibrary;
