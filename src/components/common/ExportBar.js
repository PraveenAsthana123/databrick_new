/**
 * ExportBar — Reusable export toolbar for any page
 *
 * Usage:
 *   <ExportBar
 *     contentRef={pageRef}
 *     data={tableData}
 *     filename="my-report"
 *   />
 */

import { useState } from 'react';
import {
  exportToPDF,
  exportToPNG,
  exportToCSV,
  exportToWord,
  exportToJSON,
  exportToXML,
  exportToAvro,
} from '../../utils/fileExport';

const EXPORT_OPTIONS = [
  { id: 'pdf', label: 'PDF', icon: '📄' },
  { id: 'png', label: 'PNG', icon: '🖼️' },
  { id: 'csv', label: 'CSV', icon: '📊' },
  { id: 'json', label: 'JSON', icon: '{ }' },
  { id: 'xml', label: 'XML', icon: '< >' },
  { id: 'avro', label: 'Avro', icon: '🔷' },
  { id: 'word', label: 'Word', icon: '📝' },
];

function ExportBar({ contentRef, data, filename = 'export', formats }) {
  const [exporting, setExporting] = useState(null);
  const [exportError, setExportError] = useState(null);

  const availableFormats = formats || EXPORT_OPTIONS.map((o) => o.id);

  const handleExport = async (format) => {
    setExporting(format);
    try {
      const element = contentRef?.current;
      const ts = new Date().toISOString().slice(0, 10);
      const name = `${filename}_${ts}`;

      switch (format) {
        case 'pdf':
          if (element) await exportToPDF(element, `${name}.pdf`);
          break;
        case 'png':
          if (element) await exportToPNG(element, `${name}.png`);
          break;
        case 'csv':
          if (data) exportToCSV(data, `${name}.csv`);
          break;
        case 'word':
          if (element) exportToWord(element, `${name}.doc`);
          break;
        case 'json':
          if (data) exportToJSON(data, `${name}.json`);
          break;
        case 'xml':
          if (data) exportToXML(data, `${name}.xml`);
          break;
        case 'avro':
          if (data) exportToAvro(data, `${name}.avro.json`);
          break;
        default:
          break;
      }
    } catch (err) {
      setExportError(`Export failed: ${err.message || 'Unknown error'}`);
      setTimeout(() => setExportError(null), 5000);
    } finally {
      setExporting(null);
    }
  };

  return (
    <div className="export-bar" role="toolbar" aria-label="Export options">
      {exportError && (
        <span style={{ color: 'var(--error, #ef4444)', fontSize: '0.8rem', marginRight: '0.5rem' }}>
          {exportError}
        </span>
      )}
      <span className="export-label">Export:</span>
      {EXPORT_OPTIONS.filter((opt) => availableFormats.includes(opt.id)).map((opt) => (
        <button
          key={opt.id}
          className="btn btn-sm btn-secondary"
          onClick={() => handleExport(opt.id)}
          disabled={exporting !== null}
          title={`Export as ${opt.label}`}
        >
          {exporting === opt.id ? '...' : opt.icon} {opt.label}
        </button>
      ))}
    </div>
  );
}

export default ExportBar;
