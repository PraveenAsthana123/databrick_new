import React, { useState, useRef } from 'react';
import { exportToCSV } from '../utils/fileExport';

const ACCEPTED_TYPES = {
  'text/csv': { ext: 'csv', icon: '\ud83d\udcca', label: 'CSV' },
  'application/vnd.ms-excel': { ext: 'csv', icon: '\ud83d\udcca', label: 'CSV' },
  'text/plain': { ext: 'txt', icon: '\ud83d\udcdd', label: 'Text' },
  'application/pdf': { ext: 'pdf', icon: '\ud83d\udcc4', label: 'PDF' },
  'application/json': { ext: 'json', icon: '{ }', label: 'JSON' },
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document': {
    ext: 'docx',
    icon: '\ud83d\udcc3',
    label: 'Word',
  },
  'application/msword': { ext: 'doc', icon: '\ud83d\udcc3', label: 'Word' },
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': {
    ext: 'xlsx',
    icon: '\ud83d\udcca',
    label: 'Excel',
  },
  'image/png': { ext: 'png', icon: '\ud83d\uddbc\ufe0f', label: 'PNG' },
  'image/jpeg': { ext: 'jpg', icon: '\ud83d\uddbc\ufe0f', label: 'JPG' },
  'image/svg+xml': { ext: 'svg', icon: '\ud83d\uddbc\ufe0f', label: 'SVG' },
};

const MAX_FILE_SIZE = 50 * 1024 * 1024; // 50MB

function UploadDocs() {
  const [files, setFiles] = useState([]);
  const [dragOver, setDragOver] = useState(false);
  const [previewFile, setPreviewFile] = useState(null);
  const [uploadHistory, setUploadHistory] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState('All');
  const fileInputRef = useRef(null);

  // ─── Handle file selection ──────────────────
  const processFiles = (fileList) => {
    const newFiles = Array.from(fileList).map((file) => {
      const typeInfo = ACCEPTED_TYPES[file.type] || {
        ext: file.name.split('.').pop(),
        icon: '\ud83d\udcc1',
        label: 'File',
      };

      if (file.size > MAX_FILE_SIZE) {
        return {
          id: `file_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
          name: file.name,
          size: file.size,
          type: file.type,
          ext: typeInfo.ext,
          icon: typeInfo.icon,
          label: typeInfo.label,
          status: 'error',
          error: 'File exceeds 50MB limit',
          uploadedAt: new Date().toISOString(),
          raw: null,
          preview: null,
        };
      }

      return {
        id: `file_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
        name: file.name,
        size: file.size,
        type: file.type,
        ext: typeInfo.ext,
        icon: typeInfo.icon,
        label: typeInfo.label,
        status: 'uploaded',
        error: null,
        uploadedAt: new Date().toISOString(),
        raw: file,
        preview: null,
        content: null,
      };
    });

    // Read file contents for preview
    newFiles.forEach((fileObj) => {
      if (!fileObj.raw) return;

      const reader = new FileReader();

      if (fileObj.type.startsWith('image/')) {
        reader.onload = (e) => {
          setFiles((prev) =>
            prev.map((f) => (f.id === fileObj.id ? { ...f, preview: e.target.result } : f))
          );
        };
        reader.readAsDataURL(fileObj.raw);
      } else if (
        fileObj.type === 'text/csv' ||
        fileObj.type === 'text/plain' ||
        fileObj.type === 'application/json'
      ) {
        reader.onload = (e) => {
          const content = e.target.result;
          let parsed = null;

          if (fileObj.type === 'text/csv' || fileObj.ext === 'csv') {
            const lines = content.split('\n').filter((l) => l.trim());
            const headers = lines[0].split(',').map((h) => h.trim().replace(/^"(.*)"$/, '$1'));
            const rows = lines.slice(1, 101).map((line) => {
              const values = line.split(',').map((v) => v.trim().replace(/^"(.*)"$/, '$1'));
              const row = {};
              headers.forEach((h, i) => {
                row[h] = values[i] || '';
              });
              return row;
            });
            parsed = { headers, rows, totalRows: lines.length - 1 };
          } else if (fileObj.type === 'application/json') {
            try {
              parsed = JSON.parse(content);
            } catch {
              parsed = null;
            }
          }

          setFiles((prev) =>
            prev.map((f) =>
              f.id === fileObj.id ? { ...f, content: content.slice(0, 10000), parsed } : f
            )
          );
        };
        reader.readAsText(fileObj.raw);
      } else if (fileObj.type === 'application/pdf') {
        setFiles((prev) =>
          prev.map((f) =>
            f.id === fileObj.id ? { ...f, preview: URL.createObjectURL(fileObj.raw) } : f
          )
        );
      }
    });

    setFiles((prev) => [...newFiles, ...prev]);
    setUploadHistory((prev) => [
      ...newFiles.map((f) => ({
        fileName: f.name,
        size: f.size,
        type: f.label,
        status: f.status,
        uploadedAt: f.uploadedAt,
      })),
      ...prev,
    ]);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    processFiles(e.dataTransfer.files);
  };

  const handleFileInput = (e) => {
    processFiles(e.target.files);
    e.target.value = '';
  };

  const deleteFile = (fileId) => {
    setFiles((prev) => prev.filter((f) => f.id !== fileId));
    if (previewFile?.id === fileId) setPreviewFile(null);
  };

  const formatSize = (bytes) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const downloadHistory = () => {
    if (uploadHistory.length === 0) return;
    exportToCSV(uploadHistory, `upload-history-${new Date().toISOString().slice(0, 10)}.csv`);
  };

  const filteredFiles = files.filter((f) => {
    const matchSearch = f.name.toLowerCase().includes(searchTerm.toLowerCase());
    const matchType = filterType === 'All' || f.label === filterType;
    return matchSearch && matchType;
  });

  const fileTypes = [...new Set(files.map((f) => f.label))];
  const totalSize = files.reduce((sum, f) => sum + f.size, 0);

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Upload Documents</h1>
          <p>
            {files.length} files uploaded ({formatSize(totalSize)})
          </p>
        </div>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">{'\ud83d\udcc1'}</div>
          <div className="stat-info">
            <h4>{files.length}</h4>
            <p>Total Files</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">{'\ud83d\udcca'}</div>
          <div className="stat-info">
            <h4>{files.filter((f) => f.ext === 'csv').length}</h4>
            <p>CSV Files</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">{'\ud83d\udcc4'}</div>
          <div className="stat-info">
            <h4>{files.filter((f) => f.ext === 'pdf').length}</h4>
            <p>PDF Files</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">{'\ud83d\uddbc\ufe0f'}</div>
          <div className="stat-info">
            <h4>{files.filter((f) => f.type?.startsWith('image/')).length}</h4>
            <p>Images</p>
          </div>
        </div>
      </div>

      {/* Upload Zone */}
      <div
        className="card"
        style={{
          marginBottom: '1.5rem',
          border: dragOver ? '2px dashed var(--primary)' : '2px dashed var(--border)',
          background: dragOver ? 'rgba(255, 54, 33, 0.03)' : 'var(--bg-card)',
          textAlign: 'center',
          padding: '2.5rem',
          cursor: 'pointer',
          transition: 'all 0.2s',
        }}
        onDragOver={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragLeave={() => setDragOver(false)}
        onDrop={handleDrop}
        onClick={() => fileInputRef.current?.click()}
      >
        <div style={{ fontSize: '2.5rem', marginBottom: '0.75rem' }}>{'\ud83d\udcc2'}</div>
        <h3 style={{ marginBottom: '0.5rem' }}>Drag & Drop Files Here</h3>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginBottom: '1rem' }}>
          or click to browse — CSV, TXT, PDF, JSON, Word, Excel, PNG, JPG, SVG (max 50MB)
        </p>
        <button
          className="btn btn-primary"
          onClick={(e) => {
            e.stopPropagation();
            fileInputRef.current?.click();
          }}
        >
          Browse Files
        </button>
        <input
          ref={fileInputRef}
          type="file"
          multiple
          accept=".csv,.txt,.pdf,.json,.doc,.docx,.xlsx,.png,.jpg,.jpeg,.svg"
          style={{ display: 'none' }}
          onChange={handleFileInput}
        />
      </div>

      {/* Search & Filter */}
      {files.length > 0 && (
        <div className="card" style={{ marginBottom: '1rem' }}>
          <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
            <input
              type="text"
              className="form-input"
              placeholder="Search files..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              style={{ maxWidth: '300px' }}
            />
            <select
              className="form-input"
              value={filterType}
              onChange={(e) => setFilterType(e.target.value)}
              style={{ maxWidth: '200px' }}
            >
              <option value="All">All Types ({files.length})</option>
              {fileTypes.map((t) => (
                <option key={t} value={t}>
                  {t} ({files.filter((f) => f.label === t).length})
                </option>
              ))}
            </select>
            <button
              className="btn btn-secondary btn-sm"
              onClick={downloadHistory}
              style={{ marginLeft: 'auto' }}
            >
              Download Upload History
            </button>
          </div>
        </div>
      )}

      {/* File List + Preview (left-right layout) */}
      {files.length > 0 && (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: previewFile ? '1fr 1fr' : '1fr',
            gap: '1rem',
          }}
        >
          {/* Left: File List */}
          <div>
            <div className="table-wrapper">
              <table>
                <thead>
                  <tr>
                    <th>File</th>
                    <th>Type</th>
                    <th>Size</th>
                    <th>Uploaded</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredFiles.map((f) => (
                    <tr
                      key={f.id}
                      style={{
                        background: previewFile?.id === f.id ? '#eff6ff' : undefined,
                        cursor: 'pointer',
                      }}
                      onClick={() => setPreviewFile(f)}
                    >
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                          <span style={{ fontSize: '1.2rem' }}>{f.icon}</span>
                          <div>
                            <strong style={{ fontSize: '0.85rem' }}>{f.name}</strong>
                            {f.status === 'error' && (
                              <div style={{ color: 'var(--error)', fontSize: '0.75rem' }}>
                                {f.error}
                              </div>
                            )}
                          </div>
                        </div>
                      </td>
                      <td>
                        <span className="badge completed">{f.label}</span>
                      </td>
                      <td>{formatSize(f.size)}</td>
                      <td style={{ fontSize: '0.8rem' }}>
                        {new Date(f.uploadedAt).toLocaleString()}
                      </td>
                      <td>
                        <div style={{ display: 'flex', gap: '0.25rem' }}>
                          <button
                            className="btn btn-sm btn-secondary"
                            onClick={(e) => {
                              e.stopPropagation();
                              setPreviewFile(f);
                            }}
                          >
                            Preview
                          </button>
                          {f.raw && (
                            <button
                              className="btn btn-sm btn-secondary"
                              onClick={(e) => {
                                e.stopPropagation();
                                const url = URL.createObjectURL(f.raw);
                                const a = document.createElement('a');
                                a.href = url;
                                a.download = f.name;
                                a.click();
                                URL.revokeObjectURL(url);
                              }}
                            >
                              Download
                            </button>
                          )}
                          <button
                            className="btn btn-sm"
                            style={{ background: '#fee2e2', color: '#991b1b' }}
                            onClick={(e) => {
                              e.stopPropagation();
                              deleteFile(f.id);
                            }}
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Right: Preview Panel */}
          {previewFile && (
            <div
              className="card"
              style={{
                position: 'sticky',
                top: '70px',
                maxHeight: 'calc(100vh - 100px)',
                overflowY: 'auto',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  marginBottom: '1rem',
                }}
              >
                <h3 style={{ fontSize: '0.95rem' }}>
                  {previewFile.icon} {previewFile.name}
                </h3>
                <button className="btn btn-sm btn-secondary" onClick={() => setPreviewFile(null)}>
                  Close
                </button>
              </div>

              <div
                style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', marginBottom: '1rem' }}
              >
                <span className="badge completed">{previewFile.label}</span>
                <span style={{ marginLeft: '0.5rem' }}>{formatSize(previewFile.size)}</span>
                <span style={{ marginLeft: '0.5rem' }}>
                  {new Date(previewFile.uploadedAt).toLocaleString()}
                </span>
              </div>

              {/* Image preview */}
              {previewFile.type?.startsWith('image/') && previewFile.preview && (
                <img
                  src={previewFile.preview}
                  alt={previewFile.name}
                  style={{
                    maxWidth: '100%',
                    borderRadius: '6px',
                    border: '1px solid var(--border)',
                  }}
                />
              )}

              {/* PDF preview */}
              {previewFile.ext === 'pdf' && previewFile.preview && (
                <iframe
                  src={previewFile.preview}
                  title={previewFile.name}
                  style={{
                    width: '100%',
                    height: '500px',
                    border: '1px solid var(--border)',
                    borderRadius: '6px',
                  }}
                />
              )}

              {/* CSV preview — table */}
              {previewFile.parsed?.headers && (
                <div>
                  <p
                    style={{
                      fontSize: '0.8rem',
                      color: 'var(--text-secondary)',
                      marginBottom: '0.5rem',
                    }}
                  >
                    Showing {Math.min(previewFile.parsed.rows.length, 100)} of{' '}
                    {previewFile.parsed.totalRows} rows | {previewFile.parsed.headers.length}{' '}
                    columns
                  </p>
                  <div className="table-wrapper" style={{ maxHeight: '400px', overflowY: 'auto' }}>
                    <table>
                      <thead>
                        <tr>
                          {previewFile.parsed.headers.map((h, i) => (
                            <th key={i}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {previewFile.parsed.rows.slice(0, 50).map((row, ri) => (
                          <tr key={ri}>
                            {previewFile.parsed.headers.map((h, ci) => (
                              <td key={ci}>{row[h]}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* JSON preview */}
              {previewFile.ext === 'json' && previewFile.parsed && (
                <div className="code-block" style={{ maxHeight: '400px', overflowY: 'auto' }}>
                  {JSON.stringify(previewFile.parsed, null, 2)}
                </div>
              )}

              {/* Text preview */}
              {(previewFile.ext === 'txt' || (previewFile.content && !previewFile.parsed)) &&
                previewFile.content && (
                  <div
                    className="code-block"
                    style={{ maxHeight: '400px', overflowY: 'auto', whiteSpace: 'pre-wrap' }}
                  >
                    {previewFile.content}
                  </div>
                )}

              {/* Unsupported preview */}
              {!previewFile.preview && !previewFile.content && !previewFile.parsed && (
                <div
                  style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-secondary)' }}
                >
                  <p style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>{previewFile.icon}</p>
                  <p>Preview not available for {previewFile.label} files</p>
                  <p style={{ fontSize: '0.8rem' }}>Click Download to view this file</p>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Empty state */}
      {files.length === 0 && (
        <div
          className="card"
          style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-secondary)' }}
        >
          <p style={{ fontSize: '1.1rem' }}>No files uploaded yet</p>
          <p style={{ fontSize: '0.85rem', marginTop: '0.5rem' }}>
            Drag and drop files above or click Browse
          </p>
        </div>
      )}
    </div>
  );
}

export default UploadDocs;
