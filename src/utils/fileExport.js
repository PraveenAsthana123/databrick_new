/**
 * File Export Utilities — PNG, SVG, PDF, Word, CSV
 *
 * Usage:
 *   import { exportToPDF, exportToPNG, exportToSVG, exportToCSV, exportToWord } from '../utils/fileExport';
 *   exportToPDF(elementRef, 'report.pdf');
 *   exportToPNG(elementRef, 'chart.png');
 */

import { jsPDF } from 'jspdf';
import html2canvas from 'html2canvas';
import { saveAs } from 'file-saver';

// ─── PDF Export ───────────────────────────────
export async function exportToPDF(element, filename = 'export.pdf', options = {}) {
  const canvas = await html2canvas(element, {
    scale: options.scale || 2,
    useCORS: true,
    logging: false,
  });

  const imgData = canvas.toDataURL('image/png');
  const pdf = new jsPDF({
    orientation: options.orientation || 'portrait',
    unit: 'mm',
    format: options.format || 'a4',
  });

  const pdfWidth = pdf.internal.pageSize.getWidth();
  const pdfHeight = pdf.internal.pageSize.getHeight();
  const imgWidth = pdfWidth - 20; // 10mm margin each side
  const imgHeight = (canvas.height * imgWidth) / canvas.width;

  let heightLeft = imgHeight;
  let position = 10; // top margin

  // First page
  pdf.addImage(imgData, 'PNG', 10, position, imgWidth, imgHeight);
  heightLeft -= pdfHeight - 20;

  // Additional pages if content overflows
  while (heightLeft > 0) {
    position = heightLeft - imgHeight + 10;
    pdf.addPage();
    pdf.addImage(imgData, 'PNG', 10, position, imgWidth, imgHeight);
    heightLeft -= pdfHeight - 20;
  }

  pdf.save(filename);
  return filename;
}

// ─── PNG Export ───────────────────────────────
export async function exportToPNG(element, filename = 'export.png', options = {}) {
  const canvas = await html2canvas(element, {
    scale: options.scale || 2,
    useCORS: true,
    backgroundColor: options.backgroundColor || '#ffffff',
    logging: false,
  });

  canvas.toBlob((blob) => {
    if (blob) {
      saveAs(blob, filename);
    }
  }, 'image/png');

  return filename;
}

// ─── SVG Export ───────────────────────────────
export function exportToSVG(svgElement, filename = 'export.svg') {
  let svgData;

  if (typeof svgElement === 'string') {
    // Raw SVG string
    svgData = svgElement;
  } else if (svgElement instanceof SVGElement) {
    // DOM SVG element
    const serializer = new XMLSerializer();
    svgData = serializer.serializeToString(svgElement);
  } else if (svgElement.current) {
    // React ref
    const svg = svgElement.current.querySelector('svg') || svgElement.current;
    const serializer = new XMLSerializer();
    svgData = serializer.serializeToString(svg);
  } else {
    throw new Error('Invalid SVG element provided');
  }

  const blob = new Blob([svgData], { type: 'image/svg+xml;charset=utf-8' });
  saveAs(blob, filename);
  return filename;
}

// ─── CSV Export ───────────────────────────────
export function exportToCSV(data, filename = 'export.csv', options = {}) {
  const separator = options.separator || ',';

  if (!data || data.length === 0) {
    throw new Error('No data to export');
  }

  // Get headers from first object
  const headers = Object.keys(data[0]);
  const headerRow = headers.join(separator);

  const rows = data.map((row) =>
    headers
      .map((header) => {
        const value = row[header];
        // Escape values containing separator, quotes, or newlines
        const stringVal = value === null || value === undefined ? '' : String(value);
        if (stringVal.includes(separator) || stringVal.includes('"') || stringVal.includes('\n')) {
          return `"${stringVal.replace(/"/g, '""')}"`;
        }
        return stringVal;
      })
      .join(separator)
  );

  const csvContent = [headerRow, ...rows].join('\n');
  const blob = new Blob(['\uFEFF' + csvContent], { type: 'text/csv;charset=utf-8' }); // BOM for Excel
  saveAs(blob, filename);
  return filename;
}

// ─── Word/DOCX Export (HTML-based) ────────────
export function exportToWord(element, filename = 'export.doc') {
  let htmlContent;

  if (typeof element === 'string') {
    htmlContent = element;
  } else {
    // Use XMLSerializer to safely read DOM content (avoids innerHTML)
    const serializer = new XMLSerializer();
    const target = element.current || element;
    if (!(target instanceof HTMLElement)) throw new Error('Invalid element provided');
    htmlContent = Array.from(target.childNodes)
      .map((node) => {
        if (node.nodeType === Node.TEXT_NODE) return node.textContent;
        return serializer.serializeToString(node);
      })
      .join('');
  }

  const docContent = `
    <html xmlns:o="urn:schemas-microsoft-com:office:office"
          xmlns:w="urn:schemas-microsoft-com:office:word"
          xmlns="http://www.w3.org/TR/REC-html40">
    <head>
      <meta charset="utf-8">
      <style>
        body { font-family: Calibri, Arial, sans-serif; font-size: 11pt; line-height: 1.6; }
        h1 { font-size: 18pt; color: #1a1a1a; border-bottom: 1px solid #ddd; padding-bottom: 8px; }
        h2 { font-size: 14pt; color: #333; }
        h3 { font-size: 12pt; color: #555; }
        table { border-collapse: collapse; width: 100%; margin: 12px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f5f5f5; font-weight: bold; }
        code { background-color: #f0f0f0; padding: 2px 6px; font-family: Consolas, monospace; font-size: 10pt; }
        pre { background-color: #f5f5f5; padding: 12px; border: 1px solid #ddd; overflow-x: auto; }
      </style>
    </head>
    <body>${htmlContent}</body>
    </html>
  `;

  const blob = new Blob([docContent], {
    type: 'application/msword;charset=utf-8',
  });
  saveAs(blob, filename);
  return filename;
}

// ─── JSON Export ──────────────────────────────
export function exportToJSON(data, filename = 'export.json') {
  const jsonContent = JSON.stringify(data, null, 2);
  const blob = new Blob([jsonContent], { type: 'application/json;charset=utf-8' });
  saveAs(blob, filename);
  return filename;
}
