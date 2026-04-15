import React, { useState } from 'react';

function Settings() {
  const [activeTab, setActiveTab] = useState('general');

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Settings</h1>
          <p>Platform configuration</p>
        </div>
      </div>

      <div className="tabs">
        {['general', 'security', 'compute', 'storage'].map((t) => (
          <button
            key={t}
            className={`tab ${activeTab === t ? 'active' : ''}`}
            onClick={() => setActiveTab(t)}
          >
            {t.charAt(0).toUpperCase() + t.slice(1)}
          </button>
        ))}
      </div>

      {activeTab === 'general' && (
        <div className="card">
          <h3 style={{ marginBottom: '1rem' }}>General Settings</h3>
          <div className="form-group">
            <label>Workspace Name</label>
            <input className="form-input" defaultValue="Data Engineering Workspace" />
          </div>
          <div className="form-group">
            <label>Default Catalog</label>
            <input className="form-input" defaultValue="my_catalog" />
          </div>
          <div className="form-group">
            <label>Default Schema</label>
            <input className="form-input" defaultValue="bronze" />
          </div>
          <div className="form-group">
            <label>Timezone</label>
            <select className="form-input">
              <option>UTC</option>
              <option>US/Eastern</option>
              <option>US/Pacific</option>
              <option>Asia/Kolkata</option>
            </select>
          </div>
          <button className="btn btn-primary" style={{ marginTop: '1rem' }}>
            Save Settings
          </button>
        </div>
      )}

      {activeTab === 'security' && (
        <div className="card">
          <h3 style={{ marginBottom: '1rem' }}>Security Settings</h3>
          <div className="form-group">
            <label>API Key Authentication</label>
            <select className="form-input">
              <option>Enabled</option>
              <option>Disabled</option>
            </select>
          </div>
          <div className="form-group">
            <label>IP Access List</label>
            <input className="form-input" defaultValue="0.0.0.0/0" />
          </div>
          <div className="form-group">
            <label>Encryption Key</label>
            <input className="form-input" type="password" defaultValue="*****" />
          </div>
          <div className="form-group">
            <label>Secret Scope</label>
            <input className="form-input" defaultValue="production-secrets" />
          </div>
          <button className="btn btn-primary" style={{ marginTop: '1rem' }}>
            Save Security
          </button>
        </div>
      )}

      {activeTab === 'compute' && (
        <div className="card">
          <h3 style={{ marginBottom: '1rem' }}>Compute Settings</h3>
          <div className="form-group">
            <label>Default Cluster Size</label>
            <select className="form-input">
              <option>Small (2 workers)</option>
              <option>Medium (4 workers)</option>
              <option>Large (8 workers)</option>
            </select>
          </div>
          <div className="form-group">
            <label>Auto-termination (minutes)</label>
            <input className="form-input" type="number" defaultValue="30" />
          </div>
          <div className="form-group">
            <label>Spark Version</label>
            <select className="form-input">
              <option>14.3 LTS</option>
              <option>14.3 ML LTS</option>
              <option>15.0</option>
            </select>
          </div>
          <div className="form-group">
            <label>Enable Photon</label>
            <select className="form-input">
              <option>Yes</option>
              <option>No</option>
            </select>
          </div>
          <button className="btn btn-primary" style={{ marginTop: '1rem' }}>
            Save Compute
          </button>
        </div>
      )}

      {activeTab === 'storage' && (
        <div className="card">
          <h3 style={{ marginBottom: '1rem' }}>Storage Settings</h3>
          <div className="form-group">
            <label>Landing Zone Path</label>
            <input className="form-input" defaultValue="/mnt/landing" />
          </div>
          <div className="form-group">
            <label>Bronze Path</label>
            <input className="form-input" defaultValue="catalog.bronze" />
          </div>
          <div className="form-group">
            <label>Silver Path</label>
            <input className="form-input" defaultValue="catalog.silver" />
          </div>
          <div className="form-group">
            <label>Gold Path</label>
            <input className="form-input" defaultValue="catalog.gold" />
          </div>
          <div className="form-group">
            <label>Checkpoint Base Path</label>
            <input className="form-input" defaultValue="/mnt/checkpoints" />
          </div>
          <div className="form-group">
            <label>Cloud Storage</label>
            <select className="form-input">
              <option>Azure ADLS Gen2</option>
              <option>AWS S3</option>
              <option>Google GCS</option>
              <option>Snowflake Stage</option>
            </select>
          </div>
          <button className="btn btn-primary" style={{ marginTop: '1rem' }}>
            Save Storage
          </button>
        </div>
      )}
    </div>
  );
}

export default Settings;
