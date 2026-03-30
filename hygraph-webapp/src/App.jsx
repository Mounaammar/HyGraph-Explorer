import React, { useState, useEffect, useCallback } from 'react';
import GraphView from './components/GraphView';
import TimeSeriesPanel from './components/TimeSeriesPanel';
import MetadataPanel from './components/MetadataPanel';
import SnapshotWindow from './components/SnapshotWindow';
import WorkflowBuilder from './components/WorkflowBuilder';
import PatternMatchingPanel from './components/PatternMatchingPanel';
import CreateEntityModal from './components/CreateEntityModal';
import './App.css';

const API_BASE_URL = 'http://localhost:8000';


let graphFetchPromise = null;

/**
 * HyGraph Visualizer - Main Application
 */
function App() {
  const [activeTab, setActiveTab] = useState('graph');
  const [activeOperator, setActiveOperator] = useState(null);
  const [vizMode, setVizMode] = useState('map');
  
  const [graphData, setGraphData] = useState(null);
  const [displayData, setDisplayData] = useState(null);
  const [selectedElement, setSelectedElement] = useState(null);
  const [loading, setLoading] = useState(true);
  const [backendAvailable, setBackendAvailable] = useState(false);
  const [error, setError] = useState(null);
  const [counts, setCounts] = useState({ nodes: 0, edges: 0 });
  
  // Pattern matching filter active
  const [isFiltered, setIsFiltered] = useState(false);
  
  // Track if we need to refresh when switching to graph tab
  const [needsRefresh, setNeedsRefresh] = useState(false);
  
  // Create entity modal
  const [showCreateModal, setShowCreateModal] = useState(false);
  
  // Diff result displayed as a HyGraph instance in main view
  const [diffGraphData, setDiffGraphData] = useState(null);
  const [diffGraphLabel, setDiffGraphLabel] = useState(null);
  
  // Instance selector
  const [instances, setInstances] = useState([]);
  const [selectedInstance, setSelectedInstance] = useState('__full__');
  const [instanceLoading, setInstanceLoading] = useState(false);

  // Fetch graph data with deduplication
  const fetchGraphData = useCallback(async (forceRefresh = false) => {
    try {
      // If already fetching, wait for existing request (which includes timeseries)
      if (graphFetchPromise && !forceRefresh) {
        console.log('[App] Reusing existing fetch request');
        const data = await graphFetchPromise;
        // Only update state if we got valid data
        if (data?.nodes?.length > 0) {
          setGraphData(data);
          setDisplayData(data);
          setCounts({ nodes: data.nodes?.length || 0, edges: data.edges?.length || 0 });
          setBackendAvailable(true);
          setLoading(false);
        }
        return;
      }
      
      setLoading(true);
      setError(null);
      
      // Create COMPLETE fetch promise that includes timeseries loading
      const url = forceRefresh ? `${API_BASE_URL}/api/graph?refresh=true` : `${API_BASE_URL}/api/graph`;
      
      graphFetchPromise = (async () => {
        // Step 1: Fetch graph data
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        
        // Check if backend is still loading
        if (data.loading) {
          return data; // Will be handled below
        }
        
        // Step 2: Collect all timeseries IDs
        const allTsIds = new Set();
        for (const node of data.nodes || []) {
          const tempProps = node.temporal_properties || {};
          for (const tsId of Object.values(tempProps)) {
            if (typeof tsId === 'string') allTsIds.add(tsId);
          }
        }
        for (const edge of data.edges || []) {
          const tempProps = edge.temporal_properties || {};
          for (const tsId of Object.values(tempProps)) {
            if (typeof tsId === 'string') allTsIds.add(tsId);
          }
        }
        
        console.log('[App] Collected TS IDs:', allTsIds.size);
        
        // Step 3: Batch fetch timeseries
        if (allTsIds.size > 0) {
          try {
            const tsResponse = await fetch(`${API_BASE_URL}/api/timeseries/batch`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ ts_ids: Array.from(allTsIds) })
            });
            
            if (tsResponse.ok) {
              const result = await tsResponse.json();
              const timeseries = {};
              
              console.log('[App Debug] Batch TS response:', {
                loadedCount: result.loaded,
                failedCount: result.failed?.length,
                sampleTs: Object.entries(result.timeseries || {}).slice(0, 2).map(([k, v]) => ({
                  key: k,
                  hasTimestamps: !!v?.timestamps,
                  timestampCount: v?.timestamps?.length,
                }))
              });
              
              Object.entries(result.timeseries || {}).forEach(([tsId, ts]) => {
                if (ts.timestamps && ts.data && ts.data.length > 0) {
                  // Transform to [{time, value}, ...] format
                  const transformedData = ts.timestamps.map((timestamp, idx) => ({
                    time: new Date(timestamp),
                    value: Array.isArray(ts.data[idx]) ? ts.data[idx][0] : ts.data[idx]
                  }));
                  
                  timeseries[tsId] = {
                    data: transformedData,
                    name: ts.variables?.[0] || tsId,
                    variables: ts.variables
                  };
                }
              });
              
              console.log('[App Debug] Transformed timeseries count:', Object.keys(timeseries).length);
              data.timeseries = timeseries;
            }
          } catch (err) {
            console.error('Failed to load timeseries:', err);
            data.timeseries = {};
          }
        } else {
          data.timeseries = {};
        }
        
        return data;
      })();
      
      // Clear the promise after completion
      graphFetchPromise.finally(() => {
        setTimeout(() => { graphFetchPromise = null; }, 500);
      });
      
      const data = await graphFetchPromise;
      
      // Check if we're still loading (backend returns loading indicator)
      if (data.loading) {
        console.log('Graph is still loading on server, will retry...');
        graphFetchPromise = null; // Clear so we can retry
        setTimeout(() => fetchGraphData(), 2000);
        return;
      }
      
      setGraphData(data);
      setDisplayData(data);
      
      console.log('[App Debug] Data set:', {
        nodeCount: data.nodes?.length,
        edgeCount: data.edges?.length,
        timeseriesCount: Object.keys(data.timeseries || {}).length,
        sampleTsKeys: Object.keys(data.timeseries || {}).slice(0, 5)
      });
      
      setCounts({
        nodes: data.nodes?.length || 0,
        edges: data.edges?.length || 0
      });
      setBackendAvailable(true);
      setLoading(false);
    } catch (err) {
      console.error('Failed to fetch graph:', err);
      graphFetchPromise = null; // Clear on error
      setError(err.message);
      setBackendAvailable(false);
      setLoading(false);
    }
  }, []);

  // Auto-refresh when switching to graph tab if needed
  useEffect(() => {
    if (activeTab === 'graph' && needsRefresh) {
      console.log('[App] Auto-refreshing graph data after workflow changes');
      setNeedsRefresh(false);
      fetchGraphData(true);
    }
  }, [activeTab, needsRefresh, fetchGraphData]);

  useEffect(() => { fetchGraphData(); }, [fetchGraphData]);

  // Fetch available instances
  const fetchInstances = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE_URL}/api/instances`);
      if (res.ok) {
        const data = await res.json();
        setInstances(data.instances || []);
      }
    } catch (e) {
      console.error('Failed to fetch instances:', e);
    }
  }, []);

  // Refresh instances when switching to graph tab
  useEffect(() => {
    if (activeTab === 'graph') fetchInstances();
  }, [activeTab, fetchInstances]);

  // Switch instance
  const handleInstanceChange = useCallback(async (instanceId) => {
    setSelectedInstance(instanceId);
    setSelectedElement(null);
    setIsFiltered(false);
    if (instanceId === '__full__') {
      setDisplayData(graphData);
      setCounts({ nodes: graphData?.nodes?.length || 0, edges: graphData?.edges?.length || 0 });
      return;
    }
    setInstanceLoading(true);
    try {
      const res = await fetch(`${API_BASE_URL}/api/instances/${instanceId}/data`);
      if (!res.ok) return;
      const data = await res.json();

      // Batch-fetch time series for all nodes
      const allTsIds = new Set();
      for (const node of data.nodes || []) {
        for (const tsId of Object.values(node.temporal_properties || {})) {
          if (typeof tsId === 'string') allTsIds.add(tsId);
        }
      }
      if (allTsIds.size > 0) {
        try {
          const tsRes = await fetch(`${API_BASE_URL}/api/timeseries/batch`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ts_ids: Array.from(allTsIds) }),
          });
          if (tsRes.ok) {
            const tsResult = await tsRes.json();
            const timeseries = {};
            Object.entries(tsResult.timeseries || {}).forEach(([tsId, ts]) => {
              if (ts.timestamps && ts.data?.length > 0) {
                timeseries[tsId] = {
                  data: ts.timestamps.map((t, i) => ({
                    time: new Date(t),
                    value: Array.isArray(ts.data[i]) ? ts.data[i][0] : ts.data[i],
                  })),
                  name: ts.variables?.[0] || tsId,
                  variables: ts.variables,
                };
              }
            });
            data.timeseries = timeseries;
          }
        } catch (e) { console.error('TS batch fetch failed:', e); }
      }

      setDisplayData(data);
      setCounts({ nodes: data.nodes?.length || 0, edges: data.edges?.length || 0 });
    } catch (e) {
      console.error('Failed to load instance:', e);
    } finally {
      setInstanceLoading(false);
    }
  }, [graphData]);

  // Handle element select
  const handleElementSelect = useCallback((element) => {
    setSelectedElement(element);
  }, []);
  
  // Handle pattern matching results
  const handlePatternResults = useCallback(async (results) => {
    if (!results) {
      setDisplayData(graphData);
      setIsFiltered(false);
      setCounts({
        nodes: graphData?.nodes?.length || 0,
        edges: graphData?.edges?.length || 0
      });
      return;
    }
    
    setIsFiltered(true);
    
    // Load timeseries for results
    const allTsIds = new Set();
    for (const node of results.nodes || []) {
      const tempProps = node.temporal_properties || {};
      for (const tsId of Object.values(tempProps)) {
        if (typeof tsId === 'string') allTsIds.add(tsId);
      }
    }
    for (const edge of results.edges || []) {
      const tempProps = edge.temporal_properties || {};
      for (const tsId of Object.values(tempProps)) {
        if (typeof tsId === 'string') allTsIds.add(tsId);
      }
    }
    
    let timeseries = {};
    if (allTsIds.size > 0) {
      try {
        const tsResponse = await fetch(`${API_BASE_URL}/api/timeseries/batch`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ts_ids: Array.from(allTsIds) })
        });
        
        if (tsResponse.ok) {
          const tsResult = await tsResponse.json();
          Object.entries(tsResult.timeseries || {}).forEach(([tsId, ts]) => {
            if (ts.timestamps && ts.data && ts.data.length > 0) {
              // Transform to [{time, value}, ...] format and remove timestamps
              // to avoid format confusion in TimeSeriesPanel
              const transformedData = ts.timestamps.map((timestamp, idx) => ({
                time: new Date(timestamp),
                value: Array.isArray(ts.data[idx]) ? ts.data[idx][0] : ts.data[idx]
              }));
              
              timeseries[tsId] = {
                data: transformedData,
                name: ts.variables?.[0] || tsId,
                variables: ts.variables
              };
            }
          });
        }
      } catch (err) {
        console.error('Failed to load timeseries for results:', err);
      }
    }
    
    // Calculate center for map
    const lats = [], lngs = [];
    for (const node of results.nodes || []) {
      const sp = node.static_properties || {};
      const lat = sp.latitude || sp.lat;
      const lng = sp.longitude || sp.lng || sp.lon;
      if (lat && lng) {
        lats.push(parseFloat(lat));
        lngs.push(parseFloat(lng));
      }
    }
    
    const displayResults = {
      nodes: results.nodes || [],
      edges: results.edges || [],
      timeseries: results.timeseries || timeseries,
      isDiff: results.isDiff || false,
      cpg_edges: results.cpg_edges || [],
      metadata: results.metadata || {
        hasCoordinates: lats.length > 0,
        center: {
          lat: lats.length > 0 ? lats.reduce((a,b) => a+b, 0) / lats.length : 40.7589,
          lng: lngs.length > 0 ? lngs.reduce((a,b) => a+b, 0) / lngs.length : -73.9851
        },
        zoom: 13
      }
    };
    
    setDisplayData(displayResults);
    setCounts({
      nodes: results.nodes?.length || 0,
      edges: results.edges?.length || 0
    });
  }, [graphData]);
  
  // Handle entity created - force refresh to get new data
  const handleEntityCreated = useCallback((type, data) => {
    console.log(`Entity created: ${type}`, data);
    setShowCreateModal(false);
    fetchGraphData(true); // Force refresh
  }, [fetchGraphData]);
  
  // Manual refresh handler
  const handleRefreshGraph = useCallback(() => {
    fetchGraphData(true);
  }, [fetchGraphData]);

  // Operator windows
  if (activeOperator === 'snapshot') {
    return <SnapshotWindow onBack={() => setActiveOperator(null)} />;
  }

  // Loading
  if (loading) {
    return (
      <div className="loading-screen">
        <h2>Loading HyGraph...</h2>
        <p>Connecting to {API_BASE_URL}</p>
      </div>
    );
  }

  // Error
  if (error) {
    return (
      <div className="error-screen">
        <h2>Connection Error</h2>
        <p>{error}</p>
        <button onClick={() => window.location.reload()}>Retry</button>
      </div>
    );
  }

  // No data - show limited UI with access to ingestion
  if (!graphData?.nodes?.length && activeTab !== 'ingestion') {
    return (
      <div className="app">
        <header className="app-header">
          <h1 className="app-title"> 
            <img src="/images/hygraph-logo.svg" alt="HyGraph logo" className="hygraph-logo" />
            HyGraph Visualizer
          </h1>
        </header>
        <div className="tab-bar">
          <div className="tab-buttons">
            <button onClick={() => setActiveTab('ingestion')}>Ingestion</button>
          </div>
        </div>
        <div className="error-screen">
          <h2>No Data</h2>
          <p>No graph data found. Use the Ingestion tab to load data.</p>
          <button onClick={() => setActiveTab('ingestion')}>Go to Ingestion</button>
        </div>
      </div>
    );
  }

 
  return (
    <div className="app">
      {/* CREATE ENTITY MODAL */}
      {showCreateModal && (
        <CreateEntityModal 
          onClose={() => setShowCreateModal(false)}
          onEntityCreated={handleEntityCreated}
        />
      )}

      {/* HEADER */}
      <header className="app-header">
        <h1 className="app-title"> 
          <img src="/images/hygraph-logo.svg" alt="HyGraph logo" className="hygraph-logo" />
          HyGraph Visualizer
        </h1>
        <div className="header-actions">
          <button className="create-entity-btn" onClick={() => setShowCreateModal(true)}>
            + Create Entity
          </button>
          <button 
            className="refresh-btn" 
            onClick={handleRefreshGraph}
            title="Refresh graph data from database"
          >
            ↻ Refresh
          </button>
          <select 
            value={vizMode} 
            onChange={e => setVizMode(e.target.value)}
            className="view-mode-select"
          >
            <option value="map">Map View</option>
            <option value="force">HyGraph View</option>
          </select>
        </div>
      </header>

      {/* TAB BAR */}
      <div className="tab-bar">
        <div className="tab-buttons">
          <button 
            className={activeTab === 'graph' ? 'active' : ''}
            onClick={() => setActiveTab('graph')}
          >
            HyGraph
          </button>
          <button 
            className={activeTab === 'workflow' ? 'active' : ''}
            onClick={() => setActiveTab('workflow')}
          >
            Workflow
          </button>
          <button 
            className={activeTab === 'operators' ? 'active' : ''}
            onClick={() => setActiveTab('operators')}
          >
            Operators
          </button>
          <button 
            className={activeTab === 'ingestion' ? 'active' : ''}
            onClick={() => setActiveTab('ingestion')}
          >
            Ingestion
          </button>

        </div>
        <div className="tab-info">
          {activeTab === 'graph' && instances.length > 1 && (
            <select
              className="instance-selector"
              value={selectedInstance}
              onChange={e => handleInstanceChange(e.target.value)}
            >
              {instances.map(inst => (
                <option key={inst.id} value={inst.id}>
                  {inst.type === 'hygraph' ? '[H]' : inst.type === 'subhygraph' ? '[S]' :
                   inst.type === 'induced' ? '[I]' : inst.type === 'diff' ? '[D]' : ''}
                  {' '}{inst.name} ({inst.detail})
                </option>
              ))}
            </select>
          )}
          {instanceLoading && <span className="filter-badge">Loading...</span>}
          {isFiltered && <span className="filter-badge">Filtered</span>}
          <span className="stats">{counts.nodes.toLocaleString()} nodes</span>
          <span className="stats">{counts.edges.toLocaleString()} edges</span>
        </div>
      </div>

      {/* GRAPH TAB */}
      {activeTab === 'graph' && (
        <div className="main-content-wrapper">
          {/* Pattern Matching + Instance Banner */}
          <div className="pattern-matching-wrapper">
            {selectedInstance !== '__full__' && displayData?.metadata?.isDiff && (
              <div style={{
                padding: '8px 16px', background: '#f0f4f8', borderRadius: 8,
                border: '1px solid #e8ecf0', margin: '0 0 8px 0',
                display: 'flex', gap: 16, alignItems: 'center', fontSize: '0.85rem', color: '#1a5276'
              }}>
                <strong>HyGraphDiff Result</strong>
                <span style={{color:'#563cf6'}}>● {displayData.metadata.period_1 || 'Period 1'}</span>
                <span>vs</span>
                <span style={{color:'#dc2626'}}>● {displayData.metadata.period_2 || 'Period 2'}</span>
                <button style={{
                  marginLeft: 'auto', padding: '4px 12px', border: '1px solid #bbdefb',
                  borderRadius: 6, background: 'white', cursor: 'pointer', fontSize: '0.8rem', color: '#1a5276'
                }} onClick={() => handleInstanceChange('__full__')}>Back to Full HyGraph</button>
              </div>
            )}
            <PatternMatchingPanel 
              onResultsChange={handlePatternResults}
              graphData={selectedInstance === '__full__' ? graphData : displayData}
            />
          </div>
          
          {/* Main content area */}
          <div className="main-content">
            <div className="graph-area">
              <GraphView 
                data={displayData}
                mode={vizMode}
                onElementSelect={handleElementSelect}
                selectedElement={selectedElement}
              />
            </div>
            <div className="sidebar">
              <MetadataPanel element={selectedElement} />
            </div>
          </div>
          
          {/* Time Series at Bottom */}
          <div className="timeseries-area">
            <TimeSeriesPanel 
              element={selectedElement}
              data={displayData}
            />
          </div>
        </div>
      )}

      {/* WORKFLOW TAB */}
      {activeTab === 'workflow' && (
        <WorkflowBuilder 
          onBack={() => setActiveTab('graph')} 
          onDataChanged={() => setNeedsRefresh(true)}
          onDiffAsHyGraph={(data, label) => {
            setDiffGraphData(data);
            setDiffGraphLabel(label);
            setDisplayData(data);
            setActiveTab('graph');
            fetchInstances(); // Refresh instance list
          }}
        />
      )}

      {/* OPERATORS TAB */}
      {activeTab === 'operators' && (
        <div className="operators-content">
          <h2>Operators</h2>
          <div className="operator-cards">
            <div className="operator-card" onClick={() => setActiveOperator('snapshot')}>
              <h3>Snapshot</h3>
              <p>View graph at point in time, interval, sequence, or compare</p>
            </div>
            <div className="operator-card disabled">
              <h3>TSGen</h3>
              <p>Generate timeseries from graph metrics</p>
            </div>
          </div>
        </div>
      )}



     
    </div>
  );
}

export default App;
