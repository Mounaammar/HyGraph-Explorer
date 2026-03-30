import React, { useState, useRef } from 'react';
import './CreateEntityModal.css';

const API_BASE_URL = 'http://localhost:8000';

// TimeSeries templates
const TS_TEMPLATES = {
  spike: { name: 'Spike', description: 'Sudden increase then return to baseline', generate: (n) => {
    const data = [];
    const mid = Math.floor(n / 2);
    for (let i = 0; i < n; i++) {
      const dist = Math.abs(i - mid);
      data.push(10 + Math.max(0, 50 - dist * 5) + Math.random() * 2);
    }
    return data;
  }},
  valley: { name: 'Valley', description: 'Sudden decrease then return to baseline', generate: (n) => {
    const data = [];
    const mid = Math.floor(n / 2);
    for (let i = 0; i < n; i++) {
      const dist = Math.abs(i - mid);
      data.push(50 - Math.max(0, 40 - dist * 4) + Math.random() * 2);
    }
    return data;
  }},
  trend_up: { name: 'Trend Up', description: 'Gradual increase over time', generate: (n) => {
    const data = [];
    for (let i = 0; i < n; i++) {
      data.push(10 + (i / n) * 40 + Math.random() * 3);
    }
    return data;
  }},
  trend_down: { name: 'Trend Down', description: 'Gradual decrease over time', generate: (n) => {
    const data = [];
    for (let i = 0; i < n; i++) {
      data.push(50 - (i / n) * 40 + Math.random() * 3);
    }
    return data;
  }},
  sine: { name: 'Sine Wave', description: 'Periodic oscillation', generate: (n) => {
    const data = [];
    for (let i = 0; i < n; i++) {
      data.push(30 + 20 * Math.sin((i / n) * 4 * Math.PI) + Math.random() * 2);
    }
    return data;
  }},
  step: { name: 'Step', description: 'Sudden level change', generate: (n) => {
    const data = [];
    const mid = Math.floor(n / 2);
    for (let i = 0; i < n; i++) {
      data.push(i < mid ? 20 + Math.random() * 2 : 50 + Math.random() * 2);
    }
    return data;
  }},
  noise: { name: 'Random Noise', description: 'Random fluctuations', generate: (n) => {
    const data = [];
    for (let i = 0; i < n; i++) {
      data.push(30 + Math.random() * 20);
    }
    return data;
  }},
  seasonal: { name: 'Seasonal', description: 'Repeating pattern', generate: (n) => {
    const data = [];
    for (let i = 0; i < n; i++) {
      const hour = i % 24;
      const base = hour < 6 ? 10 : hour < 12 ? 40 : hour < 18 ? 35 : 15;
      data.push(base + Math.random() * 5);
    }
    return data;
  }}
};

const CreateEntityModal = ({ onClose, onEntityCreated }) => {
  const [entityType, setEntityType] = useState('node');
  
  const [nodeForm, setNodeForm] = useState({
    uid: '',
    label: '',
    startTime: '',
    endTime: '',
    staticProps: '{}',
    tsProps: '{}'
  });
  
  const [edgeForm, setEdgeForm] = useState({
    uid: '',
    sourceUid: '',
    targetUid: '',
    label: '',
    startTime: '',
    endTime: '',
    staticProps: '{}'
  });
  
  const [tsForm, setTsForm] = useState({
    entityUid: '',
    variable: '',
    timestamps: '',
    values: '',
    inputMode: 'manual', // manual, upload, template
    template: 'spike',
    templatePoints: 50,
    startDate: new Date().toISOString().slice(0, 16),
    interval: 3600 // seconds
  });
  
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState(null);
  const [error, setError] = useState(null);
  
  const fileInputRef = useRef(null);
  
  // Generate timestamps
  const generateTimestamps = (startDate, count, intervalSec) => {
    const timestamps = [];
    const start = new Date(startDate).getTime();
    for (let i = 0; i < count; i++) {
      timestamps.push(new Date(start + i * intervalSec * 1000).toISOString());
    }
    return timestamps;
  };
  
  // Apply template
  const applyTemplate = () => {
    const template = TS_TEMPLATES[tsForm.template];
    if (!template) return;
    
    const values = template.generate(tsForm.templatePoints);
    const timestamps = generateTimestamps(tsForm.startDate, tsForm.templatePoints, tsForm.interval);
    
    setTsForm({
      ...tsForm,
      timestamps: JSON.stringify(timestamps, null, 2),
      values: JSON.stringify(values.map(v => Math.round(v * 100) / 100), null, 2)
    });
  };
  
  // Handle file upload
  const handleFileUpload = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    
    const reader = new FileReader();
    reader.onload = (event) => {
      try {
        const text = event.target.result;
        const lines = text.split('\n').filter(l => l.trim());
        
        // Try to parse as CSV (timestamp,value)
        const timestamps = [];
        const values = [];
        
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i].trim();
          if (i === 0 && (line.toLowerCase().includes('time') || line.toLowerCase().includes('date'))) {
            continue; // Skip header
          }
          
          const parts = line.split(',');
          if (parts.length >= 2) {
            timestamps.push(parts[0].trim());
            values.push(parseFloat(parts[1].trim()));
          }
        }
        
        if (timestamps.length > 0) {
          setTsForm({
            ...tsForm,
            timestamps: JSON.stringify(timestamps, null, 2),
            values: JSON.stringify(values, null, 2)
          });
          setMessage(`Loaded ${timestamps.length} data points from file`);
        } else {
          setError('Could not parse file. Expected CSV format: timestamp,value');
        }
      } catch (err) {
        setError('Failed to read file: ' + err.message);
      }
    };
    reader.readAsText(file);
  };
  
  // Create node
  const createNode = async () => {
    setLoading(true);
    setError(null);
    setMessage(null);
    
    try {
      let staticProps = {};
      let tsProps = {};
      
      try {
        staticProps = JSON.parse(nodeForm.staticProps || '{}');
      } catch (e) {
        throw new Error('Invalid static properties JSON');
      }
      
      try {
        tsProps = JSON.parse(nodeForm.tsProps || '{}');
      } catch (e) {
        throw new Error('Invalid temporal properties JSON');
      }
      
      const res = await fetch(`${API_BASE_URL}/api/crud/node`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          uid: nodeForm.uid,
          label: nodeForm.label,
          start_time: nodeForm.startTime || new Date().toISOString(),
          end_time: nodeForm.endTime || '9999-12-31T23:59:59',
          static_properties: staticProps,
          ts_properties: tsProps
        })
      });
      
      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      
      const data = await res.json();
      setMessage(`Node "${nodeForm.label}" created with UID: ${data.uid || nodeForm.uid}`);
      setNodeForm({ uid: '', label: '', startTime: '', endTime: '', staticProps: '{}', tsProps: '{}' });
      
      if (onEntityCreated) onEntityCreated('node', data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };
  
  // Create edge
  const createEdge = async () => {
    setLoading(true);
    setError(null);
    setMessage(null);
    
    try {
      let staticProps = {};
      
      try {
        staticProps = JSON.parse(edgeForm.staticProps || '{}');
      } catch (e) {
        throw new Error('Invalid properties JSON');
      }
      
      const res = await fetch(`${API_BASE_URL}/api/crud/edge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          uid: edgeForm.uid,
          source_uid: edgeForm.sourceUid,
          target_uid: edgeForm.targetUid,
          label: edgeForm.label,
          start_time: edgeForm.startTime || new Date().toISOString(),
          end_time: edgeForm.endTime || '9999-12-31T23:59:59',
          properties: staticProps
        })
      });
      
      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      
      const data = await res.json();
      setMessage(`Edge "${edgeForm.label}" created: ${edgeForm.sourceUid} -> ${edgeForm.targetUid}`);
      setEdgeForm({ uid: '', sourceUid: '', targetUid: '', label: '', startTime: '', endTime: '', staticProps: '{}' });
      
      if (onEntityCreated) onEntityCreated('edge', data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };
  
  // Create timeseries
  const createTimeSeries = async () => {
    setLoading(true);
    setError(null);
    setMessage(null);
    
    try {
      let timestamps = [];
      let values = [];
      
      try {
        timestamps = JSON.parse(tsForm.timestamps || '[]');
        values = JSON.parse(tsForm.values || '[]');
      } catch (e) {
        throw new Error('Invalid timestamps or values JSON array');
      }
      
      if (timestamps.length !== values.length) {
        throw new Error('Timestamps and values arrays must have same length');
      }
      
      const res = await fetch(`${API_BASE_URL}/api/crud/timeseries`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          entity_uid: tsForm.entityUid,
          variable: tsForm.variable,
          timestamps,
          values
        })
      });
      
      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      
      const data = await res.json();
      setMessage(`TimeSeries "${tsForm.variable}" created with ${timestamps.length} data points`);
      setTsForm({ ...tsForm, entityUid: '', variable: '', timestamps: '', values: '' });
      
      if (onEntityCreated) onEntityCreated('timeseries', data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="cem-overlay" onClick={onClose}>
      <div className="cem-modal" onClick={e => e.stopPropagation()}>
        <div className="cem-header">
          <h2>Create Entity</h2>
          <button className="cem-close" onClick={onClose}>x</button>
        </div>
        
        <div className="cem-content">
          {/* Entity Type Tabs */}
          <div className="cem-tabs">
            <button className={entityType === 'node' ? 'active' : ''} onClick={() => setEntityType('node')}>
              Node
            </button>
            <button className={entityType === 'edge' ? 'active' : ''} onClick={() => setEntityType('edge')}>
              Edge
            </button>
            <button className={entityType === 'timeseries' ? 'active' : ''} onClick={() => setEntityType('timeseries')}>
              TimeSeries
            </button>
          </div>
          
          {/* Node Form */}
          {entityType === 'node' && (
            <div className="cem-form">
              <div className="cem-form-row">
                <div className="cem-form-group">
                  <label>UID *</label>
                  <input
                    type="text"
                    value={nodeForm.uid}
                    onChange={e => setNodeForm({...nodeForm, uid: e.target.value})}
                    placeholder="unique_id"
                  />
                </div>
                <div className="cem-form-group">
                  <label>Label *</label>
                  <input
                    type="text"
                    value={nodeForm.label}
                    onChange={e => setNodeForm({...nodeForm, label: e.target.value})}
                    placeholder="e.g., Station"
                  />
                </div>
              </div>
              
              <div className="cem-form-row">
                <div className="cem-form-group">
                  <label>Start Time</label>
                  <input
                    type="datetime-local"
                    value={nodeForm.startTime}
                    onChange={e => setNodeForm({...nodeForm, startTime: e.target.value})}
                  />
                </div>
                <div className="cem-form-group">
                  <label>End Time</label>
                  <input
                    type="datetime-local"
                    value={nodeForm.endTime}
                    onChange={e => setNodeForm({...nodeForm, endTime: e.target.value})}
                  />
                </div>
              </div>
              
              <div className="cem-form-group full">
                <label>Static Properties (JSON)</label>
                <textarea
                  value={nodeForm.staticProps}
                  onChange={e => setNodeForm({...nodeForm, staticProps: e.target.value})}
                  placeholder='{"capacity": 30, "name": "Station A"}'
                  rows={2}
                />
              </div>
              
              <div className="cem-form-group full">
                <label>Temporal Properties (JSON)</label>
                <textarea
                  value={nodeForm.tsProps}
                  onChange={e => setNodeForm({...nodeForm, tsProps: e.target.value})}
                  placeholder='{"num_bikes": "ts_id_123"}'
                  rows={2}
                />
              </div>
              
              <button 
                className="cem-submit-btn"
                onClick={createNode}
                disabled={loading || !nodeForm.uid || !nodeForm.label}
              >
                {loading ? 'Creating...' : 'Create Node'}
              </button>
            </div>
          )}
          
          {/* Edge Form */}
          {entityType === 'edge' && (
            <div className="cem-form">
              <div className="cem-form-row">
                <div className="cem-form-group">
                  <label>UID *</label>
                  <input
                    type="text"
                    value={edgeForm.uid}
                    onChange={e => setEdgeForm({...edgeForm, uid: e.target.value})}
                    placeholder="unique_edge_id"
                  />
                </div>
                <div className="cem-form-group">
                  <label>Label *</label>
                  <input
                    type="text"
                    value={edgeForm.label}
                    onChange={e => setEdgeForm({...edgeForm, label: e.target.value})}
                    placeholder="e.g., Trip"
                  />
                </div>
              </div>
              
              <div className="cem-form-row three-col">
                <div className="cem-form-group">
                  <label>Source Node UID *</label>
                  <input
                    type="text"
                    value={edgeForm.sourceUid}
                    onChange={e => setEdgeForm({...edgeForm, sourceUid: e.target.value})}
                    placeholder="source_node_uid"
                  />
                </div>
                <span className="cem-arrow">-&gt;</span>
                <div className="cem-form-group">
                  <label>Target Node UID *</label>
                  <input
                    type="text"
                    value={edgeForm.targetUid}
                    onChange={e => setEdgeForm({...edgeForm, targetUid: e.target.value})}
                    placeholder="target_node_uid"
                  />
                </div>
              </div>
              
              <div className="cem-form-row">
                <div className="cem-form-group">
                  <label>Start Time</label>
                  <input
                    type="datetime-local"
                    value={edgeForm.startTime}
                    onChange={e => setEdgeForm({...edgeForm, startTime: e.target.value})}
                  />
                </div>
                <div className="cem-form-group">
                  <label>End Time</label>
                  <input
                    type="datetime-local"
                    value={edgeForm.endTime}
                    onChange={e => setEdgeForm({...edgeForm, endTime: e.target.value})}
                  />
                </div>
              </div>
              
              <div className="cem-form-group full">
                <label>Properties (JSON)</label>
                <textarea
                  value={edgeForm.staticProps}
                  onChange={e => setEdgeForm({...edgeForm, staticProps: e.target.value})}
                  placeholder='{"distance": 1.5, "duration": 300}'
                  rows={2}
                />
              </div>
              
              <button 
                className="cem-submit-btn"
                onClick={createEdge}
                disabled={loading || !edgeForm.uid || !edgeForm.sourceUid || !edgeForm.targetUid || !edgeForm.label}
              >
                {loading ? 'Creating...' : 'Create Edge'}
              </button>
            </div>
          )}
          
          {/* TimeSeries Form */}
          {entityType === 'timeseries' && (
            <div className="cem-form">
              <div className="cem-form-row">
                <div className="cem-form-group">
                  <label>Entity UID *</label>
                  <input
                    type="text"
                    value={tsForm.entityUid}
                    onChange={e => setTsForm({...tsForm, entityUid: e.target.value})}
                    placeholder="node_or_edge_uid"
                  />
                </div>
                <div className="cem-form-group">
                  <label>Variable Name *</label>
                  <input
                    type="text"
                    value={tsForm.variable}
                    onChange={e => setTsForm({...tsForm, variable: e.target.value})}
                    placeholder="e.g., num_bikes"
                  />
                </div>
              </div>
              
              {/* Input Mode Selection */}
              <div className="cem-input-modes">
                <button 
                  className={tsForm.inputMode === 'manual' ? 'active' : ''} 
                  onClick={() => setTsForm({...tsForm, inputMode: 'manual'})}
                >
                  Manual
                </button>
                <button 
                  className={tsForm.inputMode === 'upload' ? 'active' : ''} 
                  onClick={() => setTsForm({...tsForm, inputMode: 'upload'})}
                >
                  Upload CSV
                </button>
                <button 
                  className={tsForm.inputMode === 'template' ? 'active' : ''} 
                  onClick={() => setTsForm({...tsForm, inputMode: 'template'})}
                >
                  Template
                </button>
              </div>
              
              {/* Template Mode */}
              {tsForm.inputMode === 'template' && (
                <div className="cem-template-section">
                  <div className="cem-form-row">
                    <div className="cem-form-group">
                      <label>Pattern</label>
                      <select 
                        value={tsForm.template} 
                        onChange={e => setTsForm({...tsForm, template: e.target.value})}
                      >
                        {Object.entries(TS_TEMPLATES).map(([key, tmpl]) => (
                          <option key={key} value={key}>{tmpl.name}</option>
                        ))}
                      </select>
                    </div>
                    <div className="cem-form-group">
                      <label>Points</label>
                      <input
                        type="number"
                        value={tsForm.templatePoints}
                        onChange={e => setTsForm({...tsForm, templatePoints: parseInt(e.target.value) || 50})}
                        min={10}
                        max={1000}
                      />
                    </div>
                  </div>
                  
                  <p className="cem-template-desc">
                    {TS_TEMPLATES[tsForm.template]?.description}
                  </p>
                  
                  <div className="cem-form-row">
                    <div className="cem-form-group">
                      <label>Start Date</label>
                      <input
                        type="datetime-local"
                        value={tsForm.startDate}
                        onChange={e => setTsForm({...tsForm, startDate: e.target.value})}
                      />
                    </div>
                    <div className="cem-form-group">
                      <label>Interval (sec)</label>
                      <select 
                        value={tsForm.interval}
                        onChange={e => setTsForm({...tsForm, interval: parseInt(e.target.value)})}
                      >
                        <option value={60}>1 minute</option>
                        <option value={300}>5 minutes</option>
                        <option value={900}>15 minutes</option>
                        <option value={3600}>1 hour</option>
                        <option value={86400}>1 day</option>
                      </select>
                    </div>
                  </div>
                  
                  <button className="cem-generate-btn" onClick={applyTemplate}>
                    Generate Data
                  </button>
                </div>
              )}
              
              {/* Upload Mode */}
              {tsForm.inputMode === 'upload' && (
                <div className="cem-upload-section">
                  <p className="cem-upload-desc">
                    Upload a CSV file with format: timestamp,value (one per line)
                  </p>
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept=".csv,.txt"
                    onChange={handleFileUpload}
                    style={{ display: 'none' }}
                  />
                  <button 
                    className="cem-upload-btn"
                    onClick={() => fileInputRef.current?.click()}
                  >
                    Select CSV File
                  </button>
                </div>
              )}
              
              {/* Data Fields (always shown) */}
              <div className="cem-form-group full">
                <label>Timestamps (JSON Array)</label>
                <textarea
                  value={tsForm.timestamps}
                  onChange={e => setTsForm({...tsForm, timestamps: e.target.value})}
                  placeholder='["2024-06-01T00:00:00", "2024-06-01T01:00:00"]'
                  rows={3}
                />
              </div>
              
              <div className="cem-form-group full">
                <label>Values (JSON Array)</label>
                <textarea
                  value={tsForm.values}
                  onChange={e => setTsForm({...tsForm, values: e.target.value})}
                  placeholder='[10, 15, 12]'
                  rows={3}
                />
              </div>
              
              <button 
                className="cem-submit-btn"
                onClick={createTimeSeries}
                disabled={loading || !tsForm.entityUid || !tsForm.variable || !tsForm.timestamps || !tsForm.values}
              >
                {loading ? 'Creating...' : 'Create TimeSeries'}
              </button>
            </div>
          )}
          
          {/* Messages */}
          {message && <div className="cem-message success">{message}</div>}
          {error && <div className="cem-message error">{error}</div>}
        </div>
      </div>
    </div>
  );
};

export default CreateEntityModal;
