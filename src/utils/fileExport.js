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

  // Characters that trigger formula injection in Excel (C-5 fix)
  const DANGEROUS_PREFIXES = ['=', '+', '-', '@', '\t', '\r'];

  const rows = data.map((row) =>
    headers
      .map((header) => {
        const value = row[header];
        let stringVal = value === null || value === undefined ? '' : String(value);
        // Prevent CSV injection — prefix dangerous chars with single quote
        if (stringVal.length > 0 && DANGEROUS_PREFIXES.includes(stringVal[0])) {
          stringVal = `'${stringVal}`;
        }
        // Escape values containing separator, quotes, or newlines
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

// ─── XML Export ──────────────────────────────
export function exportToXML(data, filename = 'export.xml', rootTag = 'records', rowTag = 'record') {
  if (!data || data.length === 0) throw new Error('No data to export');

  const escapeXml = (str) =>
    String(str === null || str === undefined ? '' : str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');

  const rows = data
    .map((row) => {
      const fields = Object.entries(row)
        .map(([key, val]) => `    <${key}>${escapeXml(val)}</${key}>`)
        .join('\n');
      return `  <${rowTag}>\n${fields}\n  </${rowTag}>`;
    })
    .join('\n');

  const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<${rootTag}>\n${rows}\n</${rootTag}>`;
  const blob = new Blob([xml], { type: 'application/xml;charset=utf-8' });
  saveAs(blob, filename);
  return filename;
}

// ─── Avro-JSON Export (Avro-compatible JSON with schema) ─────────
export function exportToAvro(data, filename = 'export.avro.json', schemaName = 'Record') {
  if (!data || data.length === 0) throw new Error('No data to export');

  // Infer Avro-style schema from first record
  const sample = data[0];
  const fields = Object.keys(sample).map((key) => {
    const val = sample[key];
    let type = 'string';
    if (typeof val === 'number') type = Number.isInteger(val) ? 'int' : 'double';
    else if (typeof val === 'boolean') type = 'boolean';
    return { name: key, type: ['null', type] };
  });

  const avroSchema = {
    type: 'record',
    name: schemaName,
    namespace: 'com.databricks.export',
    fields,
  };

  const avroPayload = {
    schema: avroSchema,
    records: data,
  };

  const jsonContent = JSON.stringify(avroPayload, null, 2);
  const blob = new Blob([jsonContent], { type: 'application/json;charset=utf-8' });
  saveAs(blob, filename);
  return filename;
}
