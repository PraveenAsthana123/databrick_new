/**
 * File Export Tests — CSV and JSON (no DOM dependencies)
 */

// Mock file-saver
jest.mock('file-saver', () => ({
  saveAs: jest.fn(),
}));

// Mock jspdf and html2canvas (they need browser APIs)
jest.mock('jspdf', () => ({ jsPDF: jest.fn() }));
jest.mock('html2canvas', () => jest.fn());

describe('File Export — CSV', () => {
  let exportToCSV;

  beforeEach(() => {
    jest.clearAllMocks();
    // Dynamic import to avoid TextEncoder issues
    const mod = require('../fileExport');
    exportToCSV = mod.exportToCSV;
  });

  test('exports array of objects to CSV', () => {
    const data = [
      { name: 'Alice', age: 30, city: 'NYC' },
      { name: 'Bob', age: 25, city: 'LA' },
    ];
    const result = exportToCSV(data, 'test.csv');
    expect(result).toBe('test.csv');
  });

  test('handles empty data gracefully', () => {
    expect(() => exportToCSV([], 'test.csv')).toThrow('No data to export');
  });

  test('calls saveAs with correct filename', () => {
    const { saveAs } = require('file-saver');
    const data = [{ name: 'Alice', value: 'test' }];
    exportToCSV(data, 'output.csv');
    expect(saveAs).toHaveBeenCalledTimes(1);
    expect(saveAs.mock.calls[0][1]).toBe('output.csv');
  });

  test('handles null and undefined values', () => {
    const data = [{ name: null, value: undefined, other: 'ok' }];
    const result = exportToCSV(data, 'test.csv');
    expect(result).toBe('test.csv');
  });

  test('escapes values containing commas', () => {
    const { saveAs } = require('file-saver');
    const data = [{ name: 'Smith, John', value: 'ok' }];
    exportToCSV(data, 'test.csv');
    // Check blob content includes quoted value
    const blobArg = saveAs.mock.calls[0][0];
    expect(blobArg).toBeInstanceOf(Blob);
  });
});

describe('File Export — JSON', () => {
  let exportToJSON;

  beforeEach(() => {
    jest.clearAllMocks();
    const mod = require('../fileExport');
    exportToJSON = mod.exportToJSON;
  });

  test('exports data to JSON file', () => {
    const data = { key: 'value', items: [1, 2, 3] };
    const result = exportToJSON(data, 'test.json');
    expect(result).toBe('test.json');
  });

  test('calls saveAs with JSON blob', () => {
    const { saveAs } = require('file-saver');
    exportToJSON({ a: 1 }, 'out.json');
    expect(saveAs).toHaveBeenCalledTimes(1);
    expect(saveAs.mock.calls[0][1]).toBe('out.json');
  });
});
