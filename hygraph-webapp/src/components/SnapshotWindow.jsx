import React, { useState, useEffect, useRef } from 'react';
import * as echarts from 'echarts';
import GraphView from './GraphView';
import './SnapshotWindow.css';

const API_BASE_URL = 'http://localhost:8000';

/**
 * SnapshotWindow - Complete snapshot operators with:
 * - Point, Interval, Sequence, Diff, Metrics
 * - Diff shows colored graph: green=added, red=removed, yellow=changed
 */
const SnapshotWindow = ({ onBack }) => {
  const [activeOp, setActiveOp] = useState('point');
  const [vizMode, setVizMode] = useState('map');
  
  // Saved snapshots for Diff workflow
  const [savedSnapshots, setSavedSnapshots] = useState([]);
  
  // Configs
  const [pointConfig, setPointConfig] = useState({
    timestamp: '2024-06-01T12:00:00',
    mode: 'hybrid'  // Default to hybrid for property detection
  });
  
  const [intervalConfig, setIntervalConfig] = useState({
    start: '2024-06-01T00:00:00',
    end: '2024-06-01T23:59:59',
    mode: 'hybrid',
    ts_handling: 'aggregate',
    aggregation_fn: 'avg'
  });
  
  const [seqConfig, setSeqConfig] = useState({
    start: '2024-06-01T00:00:00',
    end: '2024-06-07T00:00:00',
    granularity: '1D',
    mode: 'hybrid'
  });
  
  const [diffConfig, setDiffConfig] = useState({
    timestamp1: '2024-06-01T12:00:00',
    timestamp2: '2024-06-08T12:00:00',
    mode: 'hybrid',  // Important: hybrid mode detects property changes!
    useSnapshots: false,
    snapshot1Id: null,
    snapshot2Id: null
  });
  
  const [metricsConfig, setMetricsConfig] = useState({
    timestamp: '2024-06-01T12:00:00',
    node_id: '',
    label: '',
    direction: 'both'
  });
  

  
  // State
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [seqIndex, setSeqIndex] = useState(0);
  const [selectedElement, setSelectedElement] = useState(null);
  
  // Metrics state
  const [metricsResult, setMetricsResult] = useState(null);
  const [degreeResult, setDegreeResult] = useState(null);
  

  
  // Slice chart refs
  const sliceChartRefs = useRef({});
  const sliceChartInstances = useRef({});

  // Transform API response
  const toGraphData = (apiData, diffStatus = null) => {
    if (!apiData) return null;
    
    const nodes = (apiData.nodes || []).map(n => ({
      ...n,
      oid: n.oid || n.uid,
      label: n.label,
      type: 'node',
      static_properties: n.static_properties || {},
      temporal_properties: n.temporal_properties || {},
      diffStatus: diffStatus || n.diffStatus || 'unchanged'
    }));
    
    const edges = (apiData.edges || []).map(e => ({
      ...e,
      oid: e.oid || e.uid,
      source: e.source || e.src_uid,
      target: e.target || e.dst_uid,
      label: e.label,
      type: 'edge',
      static_properties: e.static_properties || {},
      temporal_properties: e.temporal_properties || {},
      diffStatus: diffStatus || e.diffStatus || 'unchanged'
    }));
    
    let lats = [], lngs = [];
    for (const node of nodes) {
      const sp = node.static_properties || {};
      const lat = sp.latitude || sp.lat;
      const lng = sp.longitude || sp.lng || sp.lon;
      if (lat && lng) {
        lats.push(parseFloat(lat));
        lngs.push(parseFloat(lng));
      }
    }
    
    return {
      nodes,
      edges,
      timeseries: {},
      metadata: {
        hasCoordinates: lats.length > 0,
        center: {
          lat: lats.length > 0 ? lats.reduce((a,b) => a+b, 0) / lats.length : 40.7589,
          lng: lngs.length > 0 ? lngs.reduce((a,b) => a+b, 0) / lngs.length : -73.9851
        },
        zoom: 13
      }
    };
  };

  // Build diff graph with colored nodes/edges from API response
  // Handles both OLD format (diff.nodes.added) and NEW format (nodes_added)
  const buildDiffGraph = (diffData) => {
    const nodes = [];
    const edges = [];
    const nodeSet = new Set();
    const edgeSet = new Set();
    
    console.log('Building diff graph from:', diffData);
    
    // Helper to create node from diff data
    const addNode = (nodeData, status) => {
      const oid = nodeData.oid || nodeData.uid || nodeData.id;
      if (!oid || nodeSet.has(oid)) return;
      nodeSet.add(oid);
      
      nodes.push({
        ...nodeData,
        oid,
        type: 'node',
        diffStatus: status,
        static_properties: nodeData.static_properties || {},
        temporal_properties: nodeData.temporal_properties || {},
        changes: nodeData.changes || null
      });
    };
    
    // Helper to create edge from diff data
    const addEdge = (edgeData, status) => {
      const oid = edgeData.oid || edgeData.uid || edgeData.id;
      if (!oid || edgeSet.has(oid)) return;
      edgeSet.add(oid);
      
      edges.push({
        ...edgeData,
        oid,
        source: edgeData.source || edgeData.src_uid,
        target: edgeData.target || edgeData.dst_uid,
        type: 'edge',
        diffStatus: status,
        static_properties: edgeData.static_properties || {},
        temporal_properties: edgeData.temporal_properties || {}
      });
    };
    
    // Check if NEW format (nodes_added as array of full objects)
    const isNewFormat = Array.isArray(diffData.nodes_added) || 
                        Array.isArray(diffData.nodes_removed) ||
                        Array.isArray(diffData.nodes_unchanged);
    
    if (isNewFormat) {
      console.log('Using NEW API format');
      // Process nodes by status (API returns full node objects)
      (diffData.nodes_added || []).forEach(n => addNode(n, 'added'));
      (diffData.nodes_removed || []).forEach(n => addNode(n, 'removed'));
      (diffData.nodes_changed || []).forEach(n => addNode(n, 'changed'));
      (diffData.nodes_unchanged || []).forEach(n => addNode(n, 'unchanged'));
      
      // Process edges by status
      (diffData.edges_added || []).forEach(e => addEdge(e, 'added'));
      (diffData.edges_removed || []).forEach(e => addEdge(e, 'removed'));
      (diffData.edges_changed || []).forEach(e => addEdge(e, 'changed'));
      (diffData.edges_unchanged || []).forEach(e => addEdge(e, 'unchanged'));
    } else {
      console.log('Using OLD API format - backend needs restart for full visualization');
      // OLD format: diff.nodes.added is just OID list or count
      // We can't show a proper graph without full node data
      // Just extract what we can from the old format
      const oldDiff = diffData.diff || {};
      const nodesInfo = oldDiff.nodes || {};
      const edgesInfo = oldDiff.edges || {};
      
      // Old format only has OIDs, not full data - create minimal nodes
      (nodesInfo.added || []).forEach(oid => {
        if (typeof oid === 'string' || typeof oid === 'number') {
          addNode({ oid: String(oid), label: 'Node' }, 'added');
        }
      });
      (nodesInfo.removed || []).forEach(oid => {
        if (typeof oid === 'string' || typeof oid === 'number') {
          addNode({ oid: String(oid), label: 'Node' }, 'removed');
        }
      });
      (edgesInfo.added || []).forEach(oid => {
        if (typeof oid === 'string' || typeof oid === 'number') {
          addEdge({ oid: String(oid), label: 'Edge' }, 'added');
        }
      });
      (edgesInfo.removed || []).forEach(oid => {
        if (typeof oid === 'string' || typeof oid === 'number') {
          addEdge({ oid: String(oid), label: 'Edge' }, 'removed');
        }
      });
    }
    
    console.log(`Built diff graph: ${nodes.length} nodes, ${edges.length} edges`);
    
    // Calculate center from node coordinates
    let lats = [], lngs = [];
    for (const node of nodes) {
      const sp = node.static_properties || {};
      const lat = sp.latitude || sp.lat;
      const lng = sp.longitude || sp.lng || sp.lon;
      if (lat && lng) {
        lats.push(parseFloat(lat));
        lngs.push(parseFloat(lng));
      }
    }
    
    return {
      nodes,
      edges,
      timeseries: {},
      isDiff: true,
      metrics: diffData.metrics || {},
      // Include timeseries diff info
      timeseries_added: diffData.timeseries_added || [],
      metadata: {
        hasCoordinates: lats.length > 0,
        center: {
          lat: lats.length > 0 ? lats.reduce((a,b) => a+b, 0) / lats.length : 40.7589,
          lng: lngs.length > 0 ? lngs.reduce((a,b) => a+b, 0) / lngs.length : -73.9851
        },
        zoom: 13
      }
    };
  };

  const formatDateTime = (isoString) => {
    if (!isoString) return '';
    const d = new Date(isoString);
    return d.toLocaleString('en-US', { 
      month: 'short', 
      day: 'numeric', 
      hour: '2-digit', 
      minute: '2-digit'
    });
  };

  // API calls
  const execPoint = async () => {
    setLoading(true);
    setError(null);
    setSelectedElement(null);
    try {
      const res = await fetch(`${API_BASE_URL}/api/snapshot/at`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(pointConfig)
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setResult({ type: 'point', data, graphData: toGraphData(data), config: { ...pointConfig } });
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const execInterval = async () => {
    setLoading(true);
    setError(null);
    setSelectedElement(null);
    try {
      const res = await fetch(`${API_BASE_URL}/api/snapshot/interval`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(intervalConfig)
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setResult({ type: 'interval', data, graphData: toGraphData(data), config: { ...intervalConfig } });
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const execSequence = async () => {
    setLoading(true);
    setError(null);
    setSelectedElement(null);
    try {
      const res = await fetch(`${API_BASE_URL}/api/snapshot-sequence/summary`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(seqConfig)
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      
      const snapshots = data.snapshots || [];
      const seqMetrics = {
        total_snapshots: snapshots.length,
        avg_nodes: snapshots.length > 0 ? snapshots.reduce((s, snap) => s + (snap.node_count || 0), 0) / snapshots.length : 0,
        avg_edges: snapshots.length > 0 ? snapshots.reduce((s, snap) => s + (snap.edge_count || 0), 0) / snapshots.length : 0,
        avg_timeseries: snapshots.length > 0 ? snapshots.reduce((s, snap) => s + (snap.ts_count || 0), 0) / snapshots.length : 0,
        per_snapshot: snapshots.map((snap, i) => ({
          index: i,
          timestamp: snap.timestamp,
          nodes: snap.node_count || 0,
          edges: snap.edge_count || 0,
          timeseries: snap.ts_count || 0
        }))
      };
      
      setResult({ type: 'sequence', data, config: { ...seqConfig }, metrics: seqMetrics });
      setSeqIndex(0);
      if (snapshots.length > 0) loadSeqAt(0);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const loadSeqAt = async (idx) => {
    try {
      const configWithHybrid = { ...seqConfig, mode: 'hybrid' };
      const res = await fetch(`${API_BASE_URL}/api/snapshot-sequence/at/${idx}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(configWithHybrid)
      });
      if (!res.ok) return;
      const data = await res.json();
      setResult(prev => ({ 
        ...prev, 
        current: data.snapshot, 
        graphData: toGraphData(data.snapshot),
        currentTimestamp: data.snapshot?.timestamp || prev?.data?.snapshots?.[idx]?.timestamp
      }));
      setSeqIndex(idx);
    } catch (err) {
      console.error(err);
    }
  };

  const execDiff = async () => {
    setLoading(true);
    setError(null);
    setSelectedElement(null);
    try {
      const payload = {
        timestamp1: diffConfig.timestamp1,
        timestamp2: diffConfig.timestamp2,
        mode: diffConfig.mode  // Pass mode for property change detection
      };
      
      // If using saved snapshots, use their timestamps
      if (diffConfig.useSnapshots && diffConfig.snapshot1Id && diffConfig.snapshot2Id) {
        const snap1 = savedSnapshots.find(s => s.id === diffConfig.snapshot1Id);
        const snap2 = savedSnapshots.find(s => s.id === diffConfig.snapshot2Id);
        if (snap1 && snap2) {
          payload.timestamp1 = snap1.timestamp || snap1.config?.timestamp || snap1.config?.start;
          payload.timestamp2 = snap2.timestamp || snap2.config?.timestamp || snap2.config?.start;
          payload.mode = snap1.mode || snap1.config?.mode || 'hybrid';
        }
      }
      
      console.log('Diff payload:', payload);
      
      const res = await fetch(`${API_BASE_URL}/api/diff/compare`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      
      console.log('Diff response:', data);
      
      // Build graph with diff colors
      const diffGraphData = buildDiffGraph(data);
      
      setResult({ type: 'diff', data, graphData: diffGraphData, config: { ...diffConfig } });
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const execMetrics = async () => {
    setLoading(true);
    setError(null);
    setMetricsResult(null);
    try {
      const res = await fetch(`${API_BASE_URL}/api/snapshot/metrics`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ timestamp: metricsConfig.timestamp })
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setMetricsResult(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Save current snapshot for later use in Diff
  const saveSnapshot = (name) => {
    if (!result || !result.data) return;
    
    const snapshot = {
      id: Date.now().toString(),
      name: name || `Snapshot ${savedSnapshots.length + 1}`,
      type: result.type,  // 'point' or 'interval'
      timestamp: result.data.timestamp || result.config?.timestamp,
      config: result.config,
      mode: result.config?.mode || 'hybrid',
      nodeCount: result.graphData?.nodes?.length || 0,
      edgeCount: result.graphData?.edges?.length || 0,
      createdAt: new Date().toISOString()
    };
    
    setSavedSnapshots(prev => [...prev, snapshot]);
    return snapshot;
  };
  
  const deleteSnapshot = (id) => {
    setSavedSnapshots(prev => prev.filter(s => s.id !== id));
  };

  // Node degree execution


  const execNodeDegree = async () => {
    setLoading(true);
    setError(null);
    setDegreeResult(null);
    try {
      const payload = {
        timestamp: metricsConfig.timestamp,
        direction: metricsConfig.direction
      };
      
      const endpoint = metricsConfig.node_id 
        ? `${API_BASE_URL}/api/snapshot/node_degree`
        : `${API_BASE_URL}/api/snapshot/nodes_degree`;
      
      if (metricsConfig.node_id) payload.node_id = metricsConfig.node_id;
      if (metricsConfig.label) payload.label = metricsConfig.label;
      
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setDegreeResult(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Slice charts effect
  useEffect(() => {
    const isSlice = result?.config?.mode === 'hybrid' && result?.config?.ts_handling === 'slice';
    if (!selectedElement || !isSlice) return;
    
    const tempProps = selectedElement.temporal_properties || {};
    
    Object.values(sliceChartInstances.current).forEach(chart => {
      try { chart?.dispose(); } catch (e) {}
    });
    sliceChartInstances.current = {};
    
    const timer = setTimeout(() => {
      Object.entries(tempProps).forEach(([propName, values]) => {
        if (!Array.isArray(values) || values.length === 0) return;
        
        const container = sliceChartRefs.current[propName];
        if (!container) return;
        
        try {
          const chart = echarts.init(container);
          sliceChartInstances.current[propName] = chart;
          
          let chartData;
          if (typeof values[0] === 'object' && values[0].timestamp) {
            chartData = values.map(v => [new Date(v.timestamp), v.value]);
          } else {
            chartData = values.map((v, i) => [i, v]);
          }
          
          chart.setOption({
            backgroundColor: 'transparent',
            grid: { left: 45, right: 15, bottom: 35, top: 15 },
            tooltip: { trigger: 'axis' },
            xAxis: {
              type: typeof values[0] === 'object' ? 'time' : 'value',
              axisLine: { lineStyle: { color: '#e2e8f0' } },
              axisLabel: { color: '#64748b', fontSize: 9 }
            },
            yAxis: {
              type: 'value',
              axisLine: { show: false },
              axisLabel: { color: '#64748b', fontSize: 9 },
              splitLine: { lineStyle: { color: '#f1f5f9' } }
            },
            series: [{
              type: 'line',
              data: chartData,
              smooth: true,
              showSymbol: false,
              lineStyle: { width: 2, color: '#3b82f6' },
              areaStyle: {
                color: {
                  type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
                  colorStops: [
                    { offset: 0, color: 'rgba(59, 130, 246, 0.3)' },
                    { offset: 1, color: 'rgba(59, 130, 246, 0.02)' }
                  ]
                }
              }
            }]
          });
        } catch (e) {
          console.error(`Chart error for ${propName}:`, e);
        }
      });
    }, 150);
    
    return () => {
      clearTimeout(timer);
      Object.values(sliceChartInstances.current).forEach(chart => {
        try { chart?.dispose(); } catch (e) {}
      });
    };
  }, [selectedElement, result?.config?.mode, result?.config?.ts_handling]);

  useEffect(() => {
    return () => {
      Object.values(sliceChartInstances.current).forEach(chart => {
        try { chart?.dispose(); } catch (e) {}
      });
    };
  }, []);

  // ========== FORMS ==========
  const renderPointForm = () => (
    <div className="op-form">
      <div className="form-group">
        <label>Timestamp</label>
        <input
          type="datetime-local"
          value={pointConfig.timestamp.slice(0, 16)}
          onChange={e => setPointConfig({ ...pointConfig, timestamp: e.target.value + ':00' })}
        />
      </div>
      <div className="form-group">
        <label>Mode</label>
        <div className="mode-btns">
          <button className={pointConfig.mode === 'graph' ? 'active' : ''} onClick={() => setPointConfig({ ...pointConfig, mode: 'graph' })}>
            Graph 
          </button>
          <button className={pointConfig.mode === 'hybrid' ? 'active' : ''} onClick={() => setPointConfig({ ...pointConfig, mode: 'hybrid' })}>
            Hybrid 
          </button>
        </div>
      </div>
      <button className="exec-btn" onClick={execPoint} disabled={loading}>
        {loading ? 'Loading...' : 'Execute'}
      </button>
    </div>
  );

  const renderIntervalForm = () => {
    const startVal = intervalConfig.start?.slice(0,16) || "";
    const endVal = intervalConfig.end?.slice(0,16) || "";
    const startDate = startVal ? new Date(startVal): null;
    const endDate = endVal? new Date(endVal): null;
    const isInvalidRange= startDate && endDate && !Number.isNaN(startDate.getTime()) && !Number.isNaN(endDate.getTime()) ? endDate < startDate : false;
    
    return (
      <div className="op-form">
        <div className="form-row">
          <div className="form-group">
            <label>Start</label>
            <input
              type="datetime-local"
              value={intervalConfig.start.slice(0, 16)}
              onChange={e => setIntervalConfig({ ...intervalConfig, start: e.target.value + ':00' })}
            />
          </div>
          <div className="form-group">
            <label>End</label>
            <input
              type="datetime-local"
              value={intervalConfig.end.slice(0, 16)}
              onChange={e => setIntervalConfig({ ...intervalConfig, end: e.target.value + ':00' })}
            />
          </div>
        </div>
        
        <div className="form-group">
          <label>Mode</label>
          <div className="mode-btns">
            <button className={intervalConfig.mode === 'graph' ? 'active' : ''} onClick={() => setIntervalConfig({ ...intervalConfig, mode: 'graph' })}>
              Graph
            </button>
            <button className={intervalConfig.mode === 'hybrid' ? 'active' : ''} onClick={() => setIntervalConfig({ ...intervalConfig, mode: 'hybrid' })}>
              Hybrid
            </button>
          </div>
        </div>
        
        {intervalConfig.mode === 'hybrid' && (
          <>
            <div className="form-group">
              <label>TS Handling</label>
              <div className="mode-btns">
                <button 
                  className={intervalConfig.ts_handling === 'aggregate' ? 'active' : ''} 
                  onClick={() => setIntervalConfig({ ...intervalConfig, ts_handling: 'aggregate' })}
                >
                  Aggregate
                </button>
                <button 
                  className={intervalConfig.ts_handling === 'slice' ? 'active' : ''} 
                  onClick={() => setIntervalConfig({ ...intervalConfig, ts_handling: 'slice' })}
                >
                  Slice
                </button>
              </div>
            </div>
            
            {intervalConfig.ts_handling === 'aggregate' && (
              <div className="form-group">
                <label>Aggregation</label>
                <select
                  value={intervalConfig.aggregation_fn}
                  onChange={e => setIntervalConfig({ ...intervalConfig, aggregation_fn: e.target.value })}
                >
                  <option value="avg">Mean</option>
                  <option value="sum">Sum</option>
                  <option value="min">Min</option>
                  <option value="max">Max</option>
                </select>
              </div>
            )}
          </>
        )}
        
        <button className="exec-btn" onClick={execInterval} disabled={loading || isInvalidRange}>
          {loading ? 'Loading...' : 'Execute'}
        </button>
      </div>
    );
  };

  const renderSeqForm = () => (
    <div className="op-form">
      <div className="form-row">
        <div className="form-group">
          <label>Start</label>
          <input
            type="datetime-local"
            value={seqConfig.start.slice(0, 16)}
            onChange={e => setSeqConfig({ ...seqConfig, start: e.target.value + ':00' })}
          />
        </div>
        <div className="form-group">
          <label>End</label>
          <input
            type="datetime-local"
            value={seqConfig.end.slice(0, 16)}
            onChange={e => setSeqConfig({ ...seqConfig, end: e.target.value + ':00' })}
          />
        </div>
      </div>
      <div className="form-group">
        <label>Granularity</label>
        <select value={seqConfig.granularity} onChange={e => setSeqConfig({ ...seqConfig, granularity: e.target.value })}>
          <option value="1H">1 Hour</option>
          <option value="6H">6 Hours</option>
          <option value="12H">12 Hours</option>
          <option value="1D">1 Day</option>
          <option value="7D">1 Week</option>
          <option value="1M">1 Month</option>
        </select>
      </div>
      <div className="form-group">
        <label>Mode</label>
        <div className="mode-btns">
          <button className={seqConfig.mode === 'graph' ? 'active' : ''} onClick={() => setSeqConfig({ ...seqConfig, mode: 'graph' })}>Graph</button>
          <button className={seqConfig.mode === 'hybrid' ? 'active' : ''} onClick={() => setSeqConfig({ ...seqConfig, mode: 'hybrid' })}>Hybrid</button>
        </div>
      </div>
      <button className="exec-btn" onClick={execSequence} disabled={loading}>
        {loading ? 'Loading...' : 'Generate'}
      </button>
    </div>
  );

  const renderDiffForm = () => (
    <div className="op-form">
      {/* Saved Snapshots Section */}
      {savedSnapshots.length > 0 && (
        <div className="saved-snapshots-section">
          <div className="form-group">
            <label>
              <input
                type="checkbox"
                checked={diffConfig.useSnapshots}
                onChange={e => setDiffConfig({ ...diffConfig, useSnapshots: e.target.checked })}
              />
              {' '}Use Saved Snapshots
            </label>
          </div>
          
          {diffConfig.useSnapshots && (
            <div className="snapshot-selectors">
              <div className="form-group">
                <label>Snapshot 1</label>
                <select
                  value={diffConfig.snapshot1Id || ''}
                  onChange={e => setDiffConfig({ ...diffConfig, snapshot1Id: e.target.value })}
                >
                  <option value="">Select...</option>
                  {savedSnapshots.map(s => (
                    <option key={s.id} value={s.id}>
                      {s.name} ({formatDateTime(s.timestamp)}) - {s.mode}
                    </option>
                  ))}
                </select>
              </div>
              <div className="form-group">
                <label>Snapshot 2</label>
                <select
                  value={diffConfig.snapshot2Id || ''}
                  onChange={e => setDiffConfig({ ...diffConfig, snapshot2Id: e.target.value })}
                >
                  <option value="">Select...</option>
                  {savedSnapshots.map(s => (
                    <option key={s.id} value={s.id}>
                      {s.name} ({formatDateTime(s.timestamp)}) - {s.mode}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          )}
        </div>
      )}
      
      {/* Manual timestamp entry (when not using saved snapshots) */}
      {!diffConfig.useSnapshots && (
        <div className="form-row">
          <div className="form-group">
            <label>Timestamp 1</label>
            <input
              type="datetime-local"
              value={diffConfig.timestamp1.slice(0, 16)}
              onChange={e => setDiffConfig({ ...diffConfig, timestamp1: e.target.value + ':00' })}
            />
          </div>
          <div className="form-group">
            <label>Timestamp 2</label>
            <input
              type="datetime-local"
              value={diffConfig.timestamp2.slice(0, 16)}
              onChange={e => setDiffConfig({ ...diffConfig, timestamp2: e.target.value + ':00' })}
            />
          </div>
        </div>
      )}
      
      {/* Mode Selection - IMPORTANT for change detection */}
      <div className="form-group">
        <label>Mode</label>
        <div className="mode-btns">
          <button 
            className={diffConfig.mode === 'graph' ? 'active' : ''} 
            onClick={() => setDiffConfig({ ...diffConfig, mode: 'graph' })}
          >
            Graph
          </button>
          <button 
            className={diffConfig.mode === 'hybrid' ? 'active' : ''} 
            onClick={() => setDiffConfig({ ...diffConfig, mode: 'hybrid' })}
          >
            Hybrid
          </button>
        </div>
        <small className="mode-hint">
          {diffConfig.mode === 'graph' 
            ? 'Graph mode: Only structural changes (add/remove)' 
            : 'Hybrid mode: Detects property value changes too'}
        </small>
      </div>
      
      <button className="exec-btn" onClick={execDiff} disabled={loading}>
        {loading ? 'Loading...' : 'Compare'}
      </button>
      
      {/* Info about saving snapshots */}
      {savedSnapshots.length === 0 && (
        <div className="form-hint">
           Tip: Execute Point or Interval snapshots first, then save them to compare here.
        </div>
      )}
    </div>
  );

  const renderMetricsForm = () => (
    <div className="op-form">
      <div className="form-group">
        <label>Timestamp</label>
        <input
          type="datetime-local"
          value={metricsConfig.timestamp.slice(0, 16)}
          onChange={e => setMetricsConfig({ ...metricsConfig, timestamp: e.target.value + ':00' })}
        />
      </div>
      
      <button className="exec-btn" onClick={execMetrics} disabled={loading}>
        {loading ? 'Loading...' : 'Get Metrics'}
      </button>
      
      <hr className="form-divider" />
      
      <h4 className="form-subtitle">Node Degree</h4>
      
      <div className="form-group">
        <label>Node ID (optional)</label>
        <input
          type="text"
          value={metricsConfig.node_id}
          onChange={e => setMetricsConfig({ ...metricsConfig, node_id: e.target.value })}
          placeholder="e.g., station_123"
        />
      </div>
      
      <div className="form-group">
        <label>Label (optional)</label>
        <input
          type="text"
          value={metricsConfig.label}
          onChange={e => setMetricsConfig({ ...metricsConfig, label: e.target.value })}
          placeholder="e.g., Station"
        />
      </div>
      
      <div className="form-group">
        <label>Direction</label>
        <select
          value={metricsConfig.direction}
          onChange={e => setMetricsConfig({ ...metricsConfig, direction: e.target.value })}
        >
          <option value="both">Both</option>
          <option value="in">In</option>
          <option value="out">Out</option>
        </select>
      </div>
      
      <button className="exec-btn secondary" onClick={execNodeDegree} disabled={loading}>
        {loading ? 'Loading...' : metricsConfig.node_id ? 'Get Node Degree' : 'Get All Degrees'}
      </button>
    </div>
  );



  // ========== PROPERTIES PANEL ==========
  const renderPropertiesPanel = () => {
    if (!selectedElement) {
      return (
        <div className="props-panel">
          <div className="props-empty">Select a node or edge to view properties</div>
        </div>
      );
    }
    
    const staticProps = selectedElement.static_properties || {};
    const tempProps = selectedElement.temporal_properties || {};
    const isSlice = result?.config?.mode === 'hybrid' && result?.config?.ts_handling === 'slice';
    const diffStatus = selectedElement.diffStatus;
    const changes = selectedElement.changes;  // Property changes for diff mode
    
    return (
      <div className="props-panel">
        <div className="props-header">
          <span className="el-badge">{selectedElement.type?.toUpperCase()}</span>
          <span className="el-id">{selectedElement.oid || selectedElement.label}</span>
          {diffStatus && diffStatus !== 'unchanged' && (
            <span className={`diff-badge diff-${diffStatus}`}>{diffStatus}</span>
          )}
        </div>
        
        <div className="props-section">
          <h4>Static Properties</h4>
          {Object.keys(staticProps).length > 0 ? (
            <div className="props-list">
              {Object.entries(staticProps).map(([k, v]) => (
                <div key={k} className="prop-row">
                  <span className="prop-key">{k}</span>
                  <span className="prop-val">{String(v)}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="props-none">No static properties</div>
          )}
        </div>
        
        <div className="props-section temporal">
          <h4>Temporal Properties</h4>
          {Object.keys(tempProps).length > 0 ? (
            isSlice ? (
              <div className="props-none">See timeseries charts below</div>
            ) : (
              <div className="props-list">
                {Object.entries(tempProps).map(([k, v]) => (
                  <div key={k} className="prop-row temporal">
                    <span className="prop-key">{k}</span>
                    <span className="prop-val">
                      {typeof v === 'number' ? v.toFixed(2) : String(v)}
                    </span>
                  </div>
                ))}
              </div>
            )
          ) : (
            <div className="props-none">No temporal properties</div>
          )}
        </div>
        
        {/* Show property changes for changed nodes in diff mode */}
        {changes && (
          <div className="props-section changes">
            <h4>Property Changes</h4>
            {changes.modified && Object.keys(changes.modified).length > 0 && (
              <div className="changes-group">
                <span className="changes-label">Modified:</span>
                {Object.entries(changes.modified).map(([k, v]) => (
                  <div key={k} className="change-row modified">
                    <span className="prop-key">{k}</span>
                    <span className="change-detail">
                      {v.old} → {v.new}
                      {v.delta !== null && <span className="change-delta"> (Δ{v.delta > 0 ? '+' : ''}{typeof v.delta === 'number' ? v.delta.toFixed(2) : v.delta})</span>}
                    </span>
                  </div>
                ))}
              </div>
            )}
            {changes.added && Object.keys(changes.added).length > 0 && (
              <div className="changes-group">
                <span className="changes-label added">Added:</span>
                {Object.entries(changes.added).map(([k, v]) => (
                  <div key={k} className="change-row added">
                    <span className="prop-key">{k}</span>
                    <span className="prop-val">{String(v)}</span>
                  </div>
                ))}
              </div>
            )}
            {changes.removed && Object.keys(changes.removed).length > 0 && (
              <div className="changes-group">
                <span className="changes-label removed">Removed:</span>
                {Object.entries(changes.removed).map(([k, v]) => (
                  <div key={k} className="change-row removed">
                    <span className="prop-key">{k}</span>
                    <span className="prop-val">{String(v)}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    );
  };

  // ========== SLICE PANEL ==========
  const renderSlicePanel = () => {
    const isSlice = result?.config?.mode === 'hybrid' && result?.config?.ts_handling === 'slice';
    if (!isSlice) return null;
    
    if (!selectedElement) {
      return (
        <div className="slice-panel">
          <div className="slice-header"><h4>Time Series (Slice)</h4></div>
          <div className="slice-empty">Select a node or edge to view slice timeseries</div>
        </div>
      );
    }
    
    const tempProps = selectedElement.temporal_properties || {};
    const hasArrayData = Object.values(tempProps).some(v => Array.isArray(v) && v.length > 0);
    
    if (!hasArrayData) {
      return (
        <div className="slice-panel">
          <div className="slice-header">
            <h4>Time Series (Slice)</h4>
            <span className="el-label">{selectedElement.oid}</span>
          </div>
          <div className="slice-empty">No slice data for this element</div>
        </div>
      );
    }
    
    return (
      <div className="slice-panel">
        <div className="slice-header">
          <h4>Time Series (Slice)</h4>
          <span className="el-label">{selectedElement.oid}</span>
        </div>
        <div className="slice-charts">
          {Object.entries(tempProps).filter(([_, v]) => Array.isArray(v) && v.length > 0).map(([propName, values]) => (
            <div key={propName} className="slice-card">
              <div className="slice-card-header">
                <span>{propName}</span>
                <span className="slice-pts">{values.length} pts</span>
              </div>
              <div ref={el => { sliceChartRefs.current[propName] = el; }} className="slice-card-chart" />
            </div>
          ))}
        </div>
      </div>
    );
  };

  // ========== GRAPH RESULT ==========
  const renderGraphResult = () => {
    if (!result?.graphData) return null;
    const nodes = result.graphData.nodes?.length || 0;
    const edges = result.graphData.edges?.length || 0;
    const isSlice = result?.config?.mode === 'hybrid' && result?.config?.ts_handling === 'slice';
    
    return (
      <div className="result-container">
        <div className="result-header">
          <div className="result-stats">
            <span><strong>{nodes}</strong> nodes</span>
            <span><strong>{edges}</strong> edges</span>
            <span className="mode-badge">{result.config?.mode}</span>
            {result.config?.ts_handling && (
              <span className="mode-badge">{result.config.ts_handling}</span>
            )}
          </div>
          <div className="result-actions">
            <button 
              className="save-snapshot-btn" 
              onClick={() => {
                const name = prompt('Enter snapshot name:', `Snapshot ${savedSnapshots.length + 1}`);
                if (name) {
                  saveSnapshot(name);
                  alert(`Saved "${name}" - you can now use it in Diff tab!`);
                }
              }}
              title="Save for Diff comparison"
            >
               Save
            </button>
            <select value={vizMode} onChange={e => setVizMode(e.target.value)} className="view-select">
              <option value="map">Map View</option>
              <option value="force">HyGraph View</option>
            </select>
          </div>
        </div>
        
        <div className="result-body">
          <div className="result-graph">
            <GraphView
              data={result.graphData}
              mode={vizMode}
              onElementSelect={setSelectedElement}
              selectedElement={selectedElement}
            />
          </div>
          {renderPropertiesPanel()}
        </div>
        
        {isSlice && renderSlicePanel()}
      </div>
    );
  };

  // ========== SEQUENCE RESULT ==========
  const renderSeqResult = () => {
    if (!result || result.type !== 'sequence') return null;
    const snaps = result.data?.snapshots || [];
    const metrics = result.metrics;
    const currentTs = result.currentTimestamp || snaps[seqIndex]?.timestamp;
    
    return (
      <div className="result-container">
        {metrics && (
          <div className="seq-metrics">
            <div className="seq-metric-card">
              <span className="metric-value">{metrics.total_snapshots}</span>
              <span className="metric-label">Snapshots</span>
            </div>
            <div className="seq-metric-card">
              <span className="metric-value">{metrics.avg_nodes.toFixed(1)}</span>
              <span className="metric-label">Avg Nodes</span>
            </div>
            <div className="seq-metric-card">
              <span className="metric-value">{metrics.avg_edges.toFixed(1)}</span>
              <span className="metric-label">Avg Edges</span>
            </div>
            <div className="seq-metric-card">
              <span className="metric-value">{metrics.avg_timeseries.toFixed(1)}</span>
              <span className="metric-label">Avg TS</span>
            </div>
          </div>
        )}
        
        <div className="result-header">
          <div className="result-stats">
            <span>Snapshot {seqIndex + 1} of {snaps.length}</span>
            {snaps[seqIndex] && (
              <>
                <span><strong>{snaps[seqIndex].node_count || 0}</strong> nodes</span>
                <span><strong>{snaps[seqIndex].edge_count || 0}</strong> edges</span>
              </>
            )}
          </div>
          <select value={vizMode} onChange={e => setVizMode(e.target.value)} className="view-select">
            <option value="map">Map View</option>
            <option value="force">Force Graph</option>
          </select>
        </div>
        
        <div className="seq-timeline">
          <button className="seq-nav-btn" onClick={() => loadSeqAt(Math.max(0, seqIndex - 1))} disabled={seqIndex === 0}>
            Prev
          </button>
          
          <div className="seq-slider-container">
            <div className="seq-time-display">
              {currentTs ? formatDateTime(currentTs) : `Index ${seqIndex}`}
            </div>
            <input
              type="range"
              min="0"
              max={snaps.length - 1}
              value={seqIndex}
              onChange={e => loadSeqAt(parseInt(e.target.value))}
              className="seq-slider"
            />
            <div className="seq-time-range">
              <span>{formatDateTime(snaps[0]?.timestamp)}</span>
              <span>{formatDateTime(snaps[snaps.length - 1]?.timestamp)}</span>
            </div>
          </div>
          
          <button className="seq-nav-btn" onClick={() => loadSeqAt(Math.min(snaps.length - 1, seqIndex + 1))} disabled={seqIndex === snaps.length - 1}>
            Next
          </button>
        </div>
        
        {result.graphData && (
          <div className="result-body">
            <div className="result-graph">
              <GraphView data={result.graphData} mode={vizMode} onElementSelect={setSelectedElement} selectedElement={selectedElement} />
            </div>
            {renderPropertiesPanel()}
          </div>
        )}
      </div>
    );
  };

  // ========== DIFF RESULT ==========
  const renderDiffResult = () => {
    if (!result || result.type !== 'diff') return null;
    const s = result.data?.summary || {};
    const graphData = result.graphData;
    const metrics = result.data?.metrics || graphData?.metrics || {};
    const timeseriesAdded = result.data?.timeseries_added || graphData?.timeseries_added || [];
    
    // Check if using old API format (no full node data)
    const isOldFormat = result.data?.diff && !result.data?.nodes_unchanged;
    
    // Count by status
    const addedNodes = graphData?.nodes?.filter(n => n.diffStatus === 'added').length || 0;
    const removedNodes = graphData?.nodes?.filter(n => n.diffStatus === 'removed').length || 0;
    const changedNodes = graphData?.nodes?.filter(n => n.diffStatus === 'changed').length || 0;
    const unchangedNodes = graphData?.nodes?.filter(n => n.diffStatus === 'unchanged').length || 0;
    const addedEdges = graphData?.edges?.filter(e => e.diffStatus === 'added').length || 0;
    const removedEdges = graphData?.edges?.filter(e => e.diffStatus === 'removed').length || 0;
    const changedEdges = graphData?.edges?.filter(e => e.diffStatus === 'changed').length || 0;
    
    return (
      <div className="result-container">
        {/* Mode indicator */}
        <div className="diff-mode-indicator">
          Mode: <strong>{result.config?.mode || 'graph'}</strong>
          {result.config?.mode === 'graph' && (
            <span className="mode-warning"> (use Hybrid for property change detection)</span>
          )}
        </div>
        
        {/* Summary Cards */}
        <div className="diff-summary">
          <div className="diff-grid">
            <div className="diff-card added">
              <span className="diff-num">+{s.nodes_added ?? addedNodes}</span>
              <span>Nodes Added</span>
            </div>
            <div className="diff-card removed">
              <span className="diff-num">-{s.nodes_removed ?? removedNodes}</span>
              <span>Nodes Removed</span>
            </div>
            <div className="diff-card changed">
              <span className="diff-num">~{s.nodes_changed ?? changedNodes}</span>
              <span>Nodes Changed</span>
            </div>
            <div className="diff-card added">
              <span className="diff-num">+{s.edges_added ?? addedEdges}</span>
              <span>Edges Added</span>
            </div>
            <div className="diff-card removed">
              <span className="diff-num">-{s.edges_removed ?? removedEdges}</span>
              <span>Edges Removed</span>
            </div>
            <div className="diff-card added">
              <span className="diff-num">+{s.timeseries_added ?? timeseriesAdded.length}</span>
              <span>Time series Added</span>
            </div>
          </div>
          
          {/* Legend */}
          <div className="diff-legend">
            <span className="legend-item"><span className="legend-dot added"></span> Added</span>
            <span className="legend-item"><span className="legend-dot removed"></span> Removed</span>
            <span className="legend-item"><span className="legend-dot changed"></span> Changed</span>
            <span className="legend-item"><span className="legend-dot unchanged"></span> Unchanged ({s.nodes_unchanged ?? unchangedNodes})</span>
          </div>
        </div>
        
        {/* Warning for old API format */}
        {isOldFormat && (
          <div className="diff-warning">
             Backend needs restart to show full diff visualization. Run: <code>python web_api.py</code>
          </div>
        )}
        
        {/* Metrics from HyGraphDiff */}
        {Object.keys(metrics).length > 0 && (
          <div className="diff-metrics">
            <div className="diff-metrics-grid">
              <div className="diff-metric">
                <span className="metric-value">{(metrics.stability_score * 100).toFixed(1)}%</span>
                <span className="metric-label">Stability</span>
              </div>
              <div className="diff-metric">
                <span className="metric-value">{(metrics.jaccard_similarity_nodes * 100).toFixed(1)}%</span>
                <span className="metric-label">Node Similarity</span>
              </div>
              <div className="diff-metric">
                <span className="metric-value">{(metrics.jaccard_similarity_edges * 100).toFixed(1)}%</span>
                <span className="metric-label">Edge Similarity</span>
              </div>
              <div className="diff-metric">
                <span className="metric-value">{(metrics.graph_edit_distance * 100).toFixed(1)}%</span>
                <span className="metric-label">Edit Distance</span>
              </div>
            </div>
          </div>
        )}
        
        {/* Graph Visualization */}
        {graphData && graphData.nodes?.length > 0 && (
          <>
            <div className="result-header">
              <div className="result-stats">
                <span><strong>{graphData.nodes.length}</strong> nodes</span>
                <span><strong>{graphData.edges.length}</strong> edges</span>
              </div>
              <select value={vizMode} onChange={e => setVizMode(e.target.value)} className="view-select">
                <option value="map">Map View</option>
                <option value="force">HyGraph View</option>
              </select>
            </div>
            
            <div className="result-body">
              <div className="result-graph">
                <GraphView
                  data={graphData}
                  mode={vizMode}
                  onElementSelect={setSelectedElement}
                  selectedElement={selectedElement}
                />
              </div>
              {renderPropertiesPanel()}
            </div>
          </>
        )}
        
        {/* No graph data message */}
        {(!graphData || graphData.nodes?.length === 0) && !isOldFormat && (
          <div className="empty-result">No nodes to display in diff</div>
        )}
      </div>
    );
  };

  // ========== METRICS RESULT ==========
  const renderMetricsResult = () => {
    return (
      <div className="result-container">
        {metricsResult && (
          <div className="metrics-result">
            <h3>Snapshot Metrics at {formatDateTime(metricsResult.timestamp)}</h3>
            <div className="metrics-grid">
              <div className="metric-card">
                <span className="metric-value">{metricsResult.metrics?.node_count || 0}</span>
                <span className="metric-label">Nodes</span>
              </div>
              <div className="metric-card">
                <span className="metric-value">{metricsResult.metrics?.edge_count || 0}</span>
                <span className="metric-label">Edges</span>
              </div>
              <div className="metric-card">
                <span className="metric-value">{metricsResult.metrics?.ts_count || 0}</span>
                <span className="metric-label">Timeseries</span>
              </div>
              <div className="metric-card">
                <span className="metric-value">{(metricsResult.metrics?.density || 0).toFixed(4)}</span>
                <span className="metric-label">Density</span>
              </div>
              <div className="metric-card">
                <span className="metric-value">{metricsResult.metrics?.connected_components || 0}</span>
                <span className="metric-label">Components</span>
              </div>
            </div>
          </div>
        )}
        
        {degreeResult && (
          <div className="degree-result">
            <h3>Node Degree</h3>
            {typeof degreeResult === 'object' && !Array.isArray(degreeResult) && degreeResult.degree !== undefined ? (
              <div className="single-degree">
                <span className="degree-value">{degreeResult.degree}</span>
                <span className="degree-label">degree for {degreeResult.node_id || metricsConfig.node_id}</span>
              </div>
            ) : (
              <div className="degree-table-container">
                <table className="degree-table">
                  <thead><tr><th>Node ID</th><th>Timestamp</th><th>Degree</th></tr></thead>
                  <tbody>
                    {(Array.isArray(degreeResult) ? degreeResult : Object.entries(degreeResult)).map((item, idx) => {
                      const nodeId = Array.isArray(item) ? item[0] : item.node_id;
                      const degree = Array.isArray(item) ? item[1] : item.degree;
                      
                      return (<tr key={idx}><td>{nodeId}</td><td>{degree[0][0]}</td><td>{degree[0][1]}</td></tr>);
                    })}
                  </tbody>
                </table>
                {(Array.isArray(degreeResult) ? degreeResult.length : Object.keys(degreeResult).length) > 2000 && (
                  <div className="table-more">Showing 600 of {Array.isArray(degreeResult) ? degreeResult.length : Object.keys(degreeResult).length}</div>
                )}
              </div>
            )}
          </div>
        )}
        
        {!metricsResult && !degreeResult && (
          <div className="empty-result">Configure and execute metrics query</div>
        )}
      </div>
    );
  };

  
 

  // ========== MAIN RENDER ==========
  return (
    <div className="snapshot-window">
      <header className="snap-header">
        <button className="back-btn" onClick={onBack}>Back</button>
        <h1>Snapshot Operations</h1>
      </header>
      
      <div className="snap-tabs">
        <button className={activeOp === 'point' ? 'active' : ''} onClick={() => { setActiveOp('point'); setResult(null); setSelectedElement(null); }}>Point</button>
        <button className={activeOp === 'interval' ? 'active' : ''} onClick={() => { setActiveOp('interval'); setResult(null); setSelectedElement(null); }}>Interval</button>
        <button className={activeOp === 'sequence' ? 'active' : ''} onClick={() => { setActiveOp('sequence'); setResult(null); setSelectedElement(null); }}>Sequence</button>
        {/*<button className={activeOp === 'diff' ? 'active' : ''} onClick={() => { setActiveOp('diff'); setResult(null); setSelectedElement(null); }}>Diff</button>*/}
    
        <button className={activeOp === 'metrics' ? 'active' : ''} onClick={() => { setActiveOp('metrics'); setResult(null); setSelectedElement(null); setMetricsResult(null); setDegreeResult(null); }}>Metrics</button>
      </div>
      
      <div className="snap-main">
        <div className="snap-config">
          {activeOp === 'point' && renderPointForm()}
          {activeOp === 'interval' && renderIntervalForm()}
          {activeOp === 'sequence' && renderSeqForm()}
          {activeOp === 'diff' && renderDiffForm()}
          {activeOp === 'metrics' && renderMetricsForm()}
          {error && <div className="error-msg">{error}</div>}
          
          {/* Saved Snapshots List */}
          {savedSnapshots.length > 0 && (
            <div className="saved-snapshots-list">
              <h4>Saved Snapshots ({savedSnapshots.length})</h4>
              {savedSnapshots.map(snap => (
                <div key={snap.id} className="saved-snapshot-item">
                  <span className="snap-name">{snap.name}</span>
                  <span className="snap-info">{formatDateTime(snap.timestamp)} - {snap.mode}</span>
                  <button className="snap-delete" onClick={() => deleteSnapshot(snap.id)}>×</button>
                </div>
              ))}
            </div>
          )}
        </div>
        
        <div className="snap-result">
          {activeOp !== 'metrics' && activeOp !== 'tsgen' && !result && !loading && (
            <div className="empty-result">Configure and execute</div>
          )}
          {loading && <div className="loading-result"><div className="spinner" /></div>}
          {(result?.type === 'point' || result?.type === 'interval') && renderGraphResult()}
          {result?.type === 'sequence' && renderSeqResult()}
          {result?.type === 'diff' && renderDiffResult()}
          {activeOp === 'metrics' && !loading && renderMetricsResult()}
        </div>
      </div>
    </div>
  );
};

export default SnapshotWindow;
