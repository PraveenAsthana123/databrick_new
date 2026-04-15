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

import React, { useState } from 'react';
import { exportToPDF, exportToPNG, exportToCSV, exportToWord } from '../../utils/fileExport';

const EXPORT_OPTIONS = [
  { id: 'pdf', label: 'PDF', icon: '📄' },
  { id: 'png', label: 'PNG', icon: '🖼️' },
  { id: 'csv', label: 'CSV', icon: '📊' },
  { id: 'word', label: 'Word', icon: '📝' },
  { id: 'json', label: 'JSON', icon: '{ }' },
];

function ExportBar({ contentRef, data, filename = 'export', formats }) {
  const [exporting, setExporting] = useState(null);

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
          if (data) {
            const { exportToJSON } = await import('../../utils/fileExport');
            exportToJSON(data, `${name}.json`);
          }
          break;
        default:
          break;
      }
    } catch {
      // Export failed silently — could add error toast here
    } finally {
      setExporting(null);
    }
  };

  return (
    <div className="export-bar">
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
