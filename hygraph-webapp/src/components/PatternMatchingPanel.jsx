import React, { useState, useEffect, useCallback } from 'react';
import './PatternMatchingPanel.css';

const API_BASE_URL = 'http://localhost:8000';


const PatternMatchingPanel = ({ 
  onResultsChange, 
  graphData, 
  selectedElement,      
  selectedSubHyGraph,  
  onTimeSeriesSelect 
}) => {
  // Detect diff view for client-side filtering
  const isDiffData = graphData?.isDiff || graphData?.metadata?.isDiff || false;

  // Collapsible sections
  const [simpleFilterOpen, setSimpleFilterOpen] = useState(true);
  const [patternMatchingOpen, setPatternMatchingOpen] = useState(false);
  
  // Simple filter sub-tabs
  const [simpleTab, setSimpleTab] = useState('nodes');
  
  // Simple filter state - Nodes
  const [nodeFilter, setNodeFilter] = useState({
    label: '',
    idContains: '',
    atTime: '',
    betweenStart: '',
    betweenEnd: '',
    staticConstraints: [],
    tsConstraints: []
  });
  
  // Simple filter state - Edges
  const [edgeFilter, setEdgeFilter] = useState({
    label: '',
    idContains: '',
    sourceId: '',
    targetId: '',
    staticConstraints: [],
    tsConstraints: []
  });
  
  // Pattern matching state
  const [patternNodes, setPatternNodes] = useState([
    { id: 'n1', variable: 'n1', label: '', uid: '', staticConstraints: [], tsConstraints: [] }
  ]);
  const [patternEdges, setPatternEdges] = useState([]);
  const [crossConstraints, setCrossConstraints] = useState([]);
  const [crossTSConstraints, setCrossTSConstraints] = useState([]);
  
  // Results state
  const [results, setResults] = useState(null);
  const [tableResult, setTableResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  // Save dialog state
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [saveName, setSaveName] = useState('');
  const [saveDescription, setSaveDescription] = useState('');
  
  // SubHyGraph list
  const [savedSubHyGraphs, setSavedSubHyGraphs] = useState([]);
  
  // Load saved subhygraphs on mount
  useEffect(() => {
    loadSavedSubHyGraphs();
  }, []);
  
  const loadSavedSubHyGraphs = async () => {
    try {
      const res = await fetch(`${API_BASE_URL}/api/subhygraph/list`);
      if (res.ok) {
        const data = await res.json();
        setSavedSubHyGraphs(data.subhygraphs || []);
      }
    } catch (err) {
      console.error('Failed to load subhygraphs:', err);
    }
  };
  
  // Load a saved SubHyGraph and display its data
  const loadSubHyGraph = async (shg) => {
    setLoading(true);
    setError(null);
    
    try {
      const res = await fetch(`${API_BASE_URL}/api/subhygraph/${shg.id}`);
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      
      const data = await res.json();
      
      // Set results for display
      setResults({
        nodes: data.nodes,
        edges: data.edges,
        node_ids: data.nodes.map(n => n.oid),
        count: data.nodes.length
      });
      
      // Pass to parent for map and timeseries display
      if (onResultsChange) {
        onResultsChange({
          nodes: data.nodes,
          edges: data.edges,
          node_ids: data.nodes.map(n => n.oid),
          filter: `SubHyGraph: ${shg.name}`
        });
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };
  

  
  const tsConstraintTypes = [
    { value: 'aggregation', label: 'Aggregation', desc: 'Compare aggregated value (mean, max, min, sum, etc.)' },
    { value: 'value_at', label: 'Value At Time', desc: 'Value at a specific timestamp' },
    { value: 'first', label: 'First Value', desc: 'First value in the time series' },
    { value: 'last', label: 'Last Value', desc: 'Last value in the time series' },
    { value: 'range', label: 'Range', desc: 'Range = max - min' },
    { value: 'change', label: 'Change', desc: 'Change = last - first' },
    { value: 'trend', label: 'Trend', desc: 'Is the series increasing, decreasing, or stable?' },
    { value: 'volatility', label: 'Volatility', desc: 'Coefficient of variation (std/mean)' },
    { value: 'count_above', label: 'Count Above', desc: 'Number of points above a threshold' },
    { value: 'count_below', label: 'Count Below', desc: 'Number of points below a threshold' },
    { value: 'percent_above', label: '% Above', desc: 'Percentage of points above a threshold' },
    { value: 'percent_below', label: '% Below', desc: 'Percentage of points below a threshold' },
    { value: 'pattern', label: 'Contains Pattern', desc: 'Contains a pattern template (spike, drop, etc.)' },
    { value: 'similar_reference', label: 'Similar to Reference TS', desc: 'Similar to a user-provided reference timeseries' },
    { value: 'similar_template', label: 'Similar to Template', desc: 'Similar to a predefined template' }
  ];
  
  const aggregationFunctions = [
    { value: 'mean', label: 'Mean' },
    { value: 'min', label: 'Min' },
    { value: 'max', label: 'Max' },
    { value: 'sum', label: 'Sum' },
    { value: 'std', label: 'Std Dev' },
    { value: 'count', label: 'Count' },
    { value: 'median', label: 'Median' }
  ];
  
  const patternTemplates = [
    { value: 'spike', label: 'Spike' },
    { value: 'drop', label: 'Drop' },
    { value: 'increasing', label: 'Increasing'},
    { value: 'decreasing', label: 'Decreasing' },
    { value: 'peak', label: 'Peak' },
    { value: 'valley', label: 'Valley' },
    { value: 'step_up', label: 'Step Up' },
    { value: 'step_down', label: 'Step Down' },
    { value: 'oscillation', label: 'Oscillation'}
  ];
  
  const similarityTemplates = [
    { value: 'increasing', label: 'Increasing Trend' },
    { value: 'decreasing', label: 'Decreasing Trend' },
    { value: 'stable', label: 'Stable (Low Variance)' },
    { value: 'high_variance', label: 'High Variance' }
  ];
  
  const trendValues = [
    { value: 1, label: 'Increasing' },
    { value: -1, label: 'Decreasing' },
    { value: 0, label: 'Stable' }
  ];
  
  const crossTSOperators = [
    { value: 'correlates', label: 'Correlates with', desc: 'Pearson correlation >= threshold' },
    { value: 'anti_correlates', label: 'Anti-correlates', desc: 'Negative correlation <= -threshold' },
    { value: 'similar_to', label: 'Similar to', desc: 'Statistical similarity (mean, std)' }
  ];
  
  const operators = ['<', '<=', '>', '>=', '==', '!='];
  

  
  const getAvailableLabels = useCallback(() => {
    if (!graphData?.nodes) return [];
    const labels = new Set();
    graphData.nodes.forEach(node => {
      if (node.label) labels.add(node.label);
    });
    return Array.from(labels);
  }, [graphData]);
  
  const getAvailableEdgeLabels = useCallback(() => {
    if (!graphData?.edges) return [];
    const labels = new Set();
    graphData.edges.forEach(edge => {
      if (edge.label) labels.add(edge.label);
    });
    return Array.from(labels);
  }, [graphData]);
  
  const getAvailableStaticProperties = useCallback(() => {
    if (!graphData?.nodes) return [];
    const props = new Set();
    graphData.nodes.forEach(node => {
      Object.keys(node.static_properties || {}).forEach(p => props.add(p));
    });
    return Array.from(props);
  }, [graphData]);
  
  const getAvailableEdgeStaticProperties = useCallback(() => {
    if (!graphData?.edges) return [];
    const props = new Set();
    graphData.edges.forEach(edge => {
      Object.keys(edge.static_properties || {}).forEach(p => props.add(p));
    });
    return Array.from(props);
  }, [graphData]);
  
  const getAvailableTSProperties = useCallback(() => {
    if (!graphData?.nodes) return [];
    const props = new Set();
    graphData.nodes.forEach(node => {
      const tempProps = node.temporal_properties || {};
      Object.keys(tempProps).forEach(propName => props.add(propName));
    });
    return Array.from(props);
  }, [graphData]);
  
  const getAvailableEdgeTSProperties = useCallback(() => {
    if (!graphData?.edges) return [];
    const props = new Set();
    graphData.edges.forEach(edge => {
      const tempProps = edge.temporal_properties || {};
      Object.keys(tempProps).forEach(propName => props.add(propName));
    });
    return Array.from(props);
  }, [graphData]);
  
  const getNodeIds = useCallback(() => {
    if (!graphData?.nodes) return [];
    return graphData.nodes.map(n => n.oid).filter(Boolean);
  }, [graphData]);
  

  
  const buildTSConstraint = (constraint) => {
    const base = {
      property: constraint.property,
      type: constraint.type
    };
    
    switch (constraint.type) {
      case 'aggregation':
        return {
          ...base,
          aggregation: constraint.aggregation || 'mean',
          operator: constraint.operator || '<',
          value: parseFloat(constraint.value) || 0
        };
      
      case 'value_at':
        return {
          ...base,
          timestamp: constraint.timestamp,
          operator: constraint.operator || '<',
          value: parseFloat(constraint.value) || 0
        };
      
      case 'first':
      case 'last':
      case 'range':
      case 'change':
      case 'volatility':
        return {
          ...base,
          operator: constraint.operator || '<',
          value: parseFloat(constraint.value) || 0
        };
      
      case 'trend':
        return {
          ...base,
          value: parseInt(constraint.trendValue) || 0
        };
      
      case 'count_above':
      case 'count_below':
        return {
          ...base,
          threshold: parseFloat(constraint.threshold) || 0,
          operator: constraint.operator || '>',
          value: parseFloat(constraint.countValue) || 0
        };
      
      case 'percent_above':
      case 'percent_below':
        return {
          ...base,
          threshold: parseFloat(constraint.threshold) || 0,
          operator: constraint.operator || '>',
          value: parseFloat(constraint.percentValue) || 0.5
        };
      
      case 'pattern':
        return {
          ...base,
          template: constraint.template || 'spike',
          pattern_length: parseInt(constraint.patternLength) || 10
        };
      
      case 'similar_reference':
        let refTimestamps = [];
        let refValues = [];
        try {
          if (constraint.refTimestamps) {
            refTimestamps = constraint.refTimestamps.split('\n').map(t => t.trim()).filter(Boolean);
          }
          if (constraint.refValues) {
            refValues = constraint.refValues.split('\n').map(v => parseFloat(v.trim())).filter(v => !isNaN(v));
          }
        } catch (e) {
          console.error('Error parsing reference TS:', e);
        }
        return {
          ...base,
          reference_ts: { timestamps: refTimestamps, values: refValues },
          tolerance: parseFloat(constraint.tolerance) || 0.1
        };
      
      case 'similar_template':
        return {
          ...base,
          similarity_template: constraint.similarityTemplate || 'stable',
          tolerance: parseFloat(constraint.tolerance) || 0.1
        };
      
      default:
        return base;
    }
  };
  
  const buildStaticConstraint = (constraint) => ({
    property: constraint.property,
    operator: constraint.operator || '>=',
    value: isNaN(parseFloat(constraint.value)) ? constraint.value : parseFloat(constraint.value)
  });
  

  
  // Client-side filtering for diff instances
  const executeNodeFilterClientSide = () => {
    if (!graphData?.nodes) return;
    let filtered = [...graphData.nodes];

    // Label filter
    if (nodeFilter.label) {
      filtered = filtered.filter(n => n.label === nodeFilter.label);
    }
    // ID contains
    if (nodeFilter.idContains) {
      filtered = filtered.filter(n => (n.oid || '').includes(nodeFilter.idContains));
    }
    // Static constraints
    for (const sc of nodeFilter.staticConstraints) {
      if (!sc.property || sc.value === '') continue;
      const val = isNaN(parseFloat(sc.value)) ? sc.value : parseFloat(sc.value);
      filtered = filtered.filter(n => {
        const prop = n.static_properties?.[sc.property];
        if (prop == null) return false;
        const numProp = typeof prop === 'string' ? prop : parseFloat(prop);
        const numVal = typeof val === 'string' ? val : parseFloat(val);
        if (typeof val === 'string' && typeof prop === 'string') {
          switch (sc.operator) {
            case '==': return prop.toLowerCase() === val.toLowerCase();
            case '!=': return prop.toLowerCase() !== val.toLowerCase();
            default: return prop === val;
          }
        }
        switch (sc.operator) {
          case '<': return numProp < numVal;
          case '<=': return numProp <= numVal;
          case '>': return numProp > numVal;
          case '>=': return numProp >= numVal;
          case '==': return numProp === numVal;
          case '!=': return numProp !== numVal;
          default: return true;
        }
      });
    }

    const nodeIds = new Set(filtered.map(n => n.oid));
    const filteredEdges = (graphData.edges || []).filter(e =>
      nodeIds.has(String(e.source)) && nodeIds.has(String(e.target))
    );

    setResults({ nodes: filtered, edges: filteredEdges, count: filtered.length });
    if (onResultsChange) {
      onResultsChange({
        nodes: filtered, edges: filteredEdges,
        timeseries: graphData.timeseries || {},
        metadata: graphData.metadata,
        isDiff: true,
        cpg_edges: (graphData.cpg_edges || []).filter(e =>
          nodeIds.has(String(e.source)) && nodeIds.has(String(e.target))
        ),
      });
    }
  };

  const executeNodeFilter = async () => {
    // Use client-side filtering for diff instances
    if (isDiffData) {
      setLoading(true);
      try { executeNodeFilterClientSide(); } finally { setLoading(false); }
      return;
    }

    setLoading(true);
    setError(null);
    setTableResult(null);
    
    try {
      const payload = {
        label: nodeFilter.label || null,
        id_contains: nodeFilter.idContains || null,
        at_time: nodeFilter.atTime || null,
        between: (nodeFilter.betweenStart && nodeFilter.betweenEnd) 
          ? [nodeFilter.betweenStart, nodeFilter.betweenEnd] 
          : null,
        static_constraints: nodeFilter.staticConstraints.length > 0 
          ? nodeFilter.staticConstraints.map(buildStaticConstraint)
          : null,
        ts_constraints: nodeFilter.tsConstraints.length > 0
          ? nodeFilter.tsConstraints.map(buildTSConstraint)
          : null
      };
      
      const res = await fetch(`${API_BASE_URL}/api/query/nodes`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      
      const data = await res.json();
      setResults(data);
      
      if (onResultsChange) {
        onResultsChange({
          nodes: data.nodes,
          edges: data.edges,
          node_ids: data.node_ids,
          filter: `Nodes: ${nodeFilter.label || 'All'}`
        });
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };
  
  const executeEdgeFilter = async () => {
    setLoading(true);
    setError(null);
    setTableResult(null);
    
    try {
      const payload = {
        label: edgeFilter.label || null,
        id_contains: edgeFilter.idContains || null,
        source_id: edgeFilter.sourceId || null,
        target_id: edgeFilter.targetId || null,
        static_constraints: edgeFilter.staticConstraints.length > 0
          ? edgeFilter.staticConstraints.map(buildStaticConstraint)
          : null,
        ts_constraints: edgeFilter.tsConstraints.length > 0
          ? edgeFilter.tsConstraints.map(buildTSConstraint)
          : null
      };
      
      const res = await fetch(`${API_BASE_URL}/api/query/edges`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      
      const data = await res.json();
      setResults(data);
      
      if (onResultsChange) {
        onResultsChange({
          nodes: data.nodes,
          edges: data.edges,
          node_ids: data.node_ids,
          filter: `Edges: ${edgeFilter.label || 'All'}`
        });
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };
  
  const executePatternMatch = async () => {
    setLoading(true);
    setError(null);
    setTableResult(null);
    
    try {
      const payload = {
        nodes: patternNodes.map(n => ({
          variable: n.variable,
          label: n.label || null,
          uid: n.uid || null,
          static_constraints: n.staticConstraints?.length > 0 
            ? n.staticConstraints.map(buildStaticConstraint)
            : null,
          ts_constraints: n.tsConstraints?.length > 0
            ? n.tsConstraints.map(buildTSConstraint)
            : null
        })),
        edges: patternEdges.length > 0 ? patternEdges.map(e => ({
          variable: e.variable,
          source_var: e.sourceVar,
          target_var: e.targetVar,
          label: e.label || null,
          static_constraints: e.staticConstraints?.length > 0
            ? e.staticConstraints.map(buildStaticConstraint)
            : null,
          ts_constraints: e.tsConstraints?.length > 0
            ? e.tsConstraints.map(buildTSConstraint)
            : null
        })) : null,
        cross_constraints: crossConstraints.length > 0 ? crossConstraints.map(c => ({
          left: c.left,
          operator: c.operator,
          right: c.right
        })) : null,
        cross_ts_constraints: crossTSConstraints.length > 0 ? crossTSConstraints.map(c => ({
          left: c.left,
          operator: c.operator,
          right: c.right,
          threshold: parseFloat(c.threshold) || 0.7
        })) : null
      };
      
      const res = await fetch(`${API_BASE_URL}/api/pattern/match`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      
      const data = await res.json();
      
      setResults({
        nodes: data.nodes,
        edges: data.edges,
        node_ids: data.node_ids,
        count: data.count,
        matches: data.matches
      });
      
      if (data.matches?.length > 0) {
        const columns = Object.keys(data.matches[0]);
        setTableResult({
          columns,
          rows: data.matches.map(m => columns.map(col => {
            const entity = m[col];
            return entity ? `${entity.label || ''}:${entity.oid}` : '';
          }))
        });
      }
      
      if (onResultsChange) {
        onResultsChange({
          nodes: data.nodes,
          edges: data.edges,
          node_ids: data.node_ids
        });
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };
  

  
  const saveAsSubHyGraph = async () => {
    if (!saveName.trim() || !results?.node_ids?.length) {
      setError('Please provide a name and have results to save');
      return;
    }
    
    try {
      const res = await fetch(`${API_BASE_URL}/api/subhygraph/save`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: saveName,
          description: saveDescription,
          node_ids: results.node_ids,
          filter_query: 'Query Builder Result'
        })
      });
      
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      
      setShowSaveDialog(false);
      setSaveName('');
      setSaveDescription('');
      loadSavedSubHyGraphs(); // Reload the list
      alert(`SubHyGraph "${saveName}" saved successfully!`);
    } catch (err) {
      setError(err.message);
    }
  };
  
  const clearResults = () => {
    setResults(null);
    setTableResult(null);
    if (onResultsChange) {
      if (isDiffData) {
        // Restore diff data instead of resetting to full HyGraph
        onResultsChange({...graphData});
      } else {
        onResultsChange(null);
      }
    }
  };
  

  
  const addPatternNode = () => {
    const id = `n${patternNodes.length + 1}`;
    setPatternNodes([...patternNodes, {
      id, variable: id, label: '', uid: '', staticConstraints: [], tsConstraints: []
    }]);
  };
  
  const removePatternNode = (nodeId) => {
    setPatternNodes(patternNodes.filter(n => n.id !== nodeId));
    setPatternEdges(patternEdges.filter(e => e.sourceVar !== nodeId && e.targetVar !== nodeId));
  };
  
  const updatePatternNode = (nodeId, updates) => {
    setPatternNodes(patternNodes.map(n => n.id === nodeId ? { ...n, ...updates } : n));
  };
  
  const addPatternEdge = () => {
    if (patternNodes.length < 2) return;
    const id = `e${patternEdges.length + 1}`;
    setPatternEdges([...patternEdges, {
      id, variable: id, sourceVar: patternNodes[0].variable, targetVar: patternNodes[1].variable, 
      label: '', staticConstraints: [], tsConstraints: []
    }]);
  };
  
  const removePatternEdge = (edgeId) => {
    setPatternEdges(patternEdges.filter(e => e.id !== edgeId));
  };
  
  const updatePatternEdge = (edgeId, updates) => {
    setPatternEdges(patternEdges.map(e => e.id === edgeId ? { ...e, ...updates } : e));
  };
  

  
  const renderSubHyGraphBlock = () => {
    return (
      <div className="pm-subhygraph-section">
        <div className="pm-subhygraph-section-header">
          <span>SubHyGraphs</span>
          <span className="pm-subhygraph-count">{savedSubHyGraphs.length} saved</span>
        </div>
        
        {/* List of saved SubHyGraphs */}
        <div className="pm-subhygraph-list">
          {savedSubHyGraphs.length === 0 ? (
            <div className="pm-empty-msg">No saved SubHyGraphs yet</div>
          ) : (
            savedSubHyGraphs.map(shg => (
              <div 
                key={shg.id} 
                className={`pm-subhygraph-item ${selectedSubHyGraph?.id === shg.id ? 'selected' : ''}`}
                onClick={() => loadSubHyGraph(shg)}
                title={shg.description || shg.name}
              >
                <span className="pm-shg-name">{shg.name}</span>
                <span className="pm-shg-stats">{shg.node_count || 0}n / {shg.edge_count || 0}e</span>
              </div>
            ))
          )}
        </div>
        
        {/* Selected SubHyGraph details */}
        {selectedSubHyGraph && (
          <div className="pm-selected-shg">
            <div className="pm-selected-shg-header">
              <span>{selectedSubHyGraph.name}</span>
            </div>
            {selectedSubHyGraph.description && (
              <p className="pm-selected-shg-desc">{selectedSubHyGraph.description}</p>
            )}
            <div className="pm-stats-row">
              <div className="pm-stat-box">
                <span className="pm-stat-value">{selectedSubHyGraph.node_count || 0}</span>
                <span className="pm-stat-label">Nodes</span>
              </div>
              <div className="pm-stat-box">
                <span className="pm-stat-value">{selectedSubHyGraph.edge_count || 0}</span>
                <span className="pm-stat-label">Edges</span>
              </div>
            </div>
          </div>
        )}
        
        {/* Selected Element properties (from graph click) */}
        {selectedElement?.data && (
          <div className="pm-selected-element">
            <div className={`pm-selected-element-header ${selectedElement.type}`}>
              <span>{selectedElement.type === 'node' ? '●' : '─'}</span>
              <span>{selectedElement.data.label || selectedElement.type}</span>
              <span className="pm-element-id">{selectedElement.data.oid}</span>
            </div>
            <div className="pm-selected-element-content">
              {Object.keys(selectedElement.data.static_properties || {}).length > 0 && (
                <div className="pm-props-section">
                  <div className="pm-props-section-title">Static</div>
                  <div className="pm-props-grid">
                    {Object.entries(selectedElement.data.static_properties || {}).map(([key, value]) => (
                      <div key={key} className="pm-prop-item">
                        <span className="pm-prop-key">{key}</span>
                        <span className="pm-prop-value">
                          {typeof value === 'number' ? value.toFixed(2) : String(value)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              
              {Object.keys(selectedElement.data.temporal_properties || {}).length > 0 && (
                <div className="pm-props-section">
                  <div className="pm-props-section-title">Temporal</div>
                  <div className="pm-props-grid">
                    {Object.entries(selectedElement.data.temporal_properties || {}).map(([propName, tsid]) => (
                      <div 
                        key={propName} 
                        className="pm-prop-item ts clickable"
                        onClick={() => onTimeSeriesSelect && onTimeSeriesSelect(tsid, propName)}
                      >
                        <span className="pm-prop-key">{propName}</span>
                        <span className="pm-prop-value ts-badge">TS</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    );
  };
  

  
  const TSConstraintEditor = ({ constraint, onChange, onRemove, availableProps }) => {
    return (
      <div className="pm-ts-constraint">
        <div className="pm-ts-constraint-row">
          <select
            value={constraint.property}
            onChange={e => onChange({ ...constraint, property: e.target.value })}
            className="pm-select"
          >
            <option value="">Select property...</option>
            {availableProps.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
          
          <select
            value={constraint.type}
            onChange={e => onChange({ ...constraint, type: e.target.value })}
            className="pm-select"
            title={tsConstraintTypes.find(t => t.value === constraint.type)?.desc}
          >
            {tsConstraintTypes.map(t => (
              <option key={t.value} value={t.value} title={t.desc}>{t.label}</option>
            ))}
          </select>
          
          <button className="pm-btn-remove" onClick={onRemove}>×</button>
        </div>
        
        <div className="pm-ts-constraint-details">
          {constraint.type === 'aggregation' && (
            <>
              <select
                value={constraint.aggregation || 'mean'}
                onChange={e => onChange({ ...constraint, aggregation: e.target.value })}
                className="pm-select-sm"
              >
                {aggregationFunctions.map(a => <option key={a.value} value={a.value}>{a.label}</option>)}
              </select>
              <select
                value={constraint.operator || '<'}
                onChange={e => onChange({ ...constraint, operator: e.target.value })}
                className="pm-select-sm"
              >
                {operators.map(op => <option key={op} value={op}>{op}</option>)}
              </select>
              <input
                type="number"
                value={constraint.value || ''}
                onChange={e => onChange({ ...constraint, value: e.target.value })}
                placeholder="Enter value..."
                className="pm-input"
              />
            </>
          )}
          
          {constraint.type === 'value_at' && (
            <>
              <input
                type="datetime-local"
                value={constraint.timestamp || ''}
                onChange={e => onChange({ ...constraint, timestamp: e.target.value })}
                className="pm-input"
              />
              <select
                value={constraint.operator || '<'}
                onChange={e => onChange({ ...constraint, operator: e.target.value })}
                className="pm-select-sm"
              >
                {operators.map(op => <option key={op} value={op}>{op}</option>)}
              </select>
              <input
                type="number"
                value={constraint.value || ''}
                onChange={e => onChange({ ...constraint, value: e.target.value })}
                placeholder="Enter value..."
                className="pm-input"
              />
            </>
          )}
          
          {['first', 'last', 'range', 'change', 'volatility'].includes(constraint.type) && (
            <>
              <span className="pm-label">{constraint.type === 'volatility' ? 'CV' : constraint.type}</span>
              <select
                value={constraint.operator || '<'}
                onChange={e => onChange({ ...constraint, operator: e.target.value })}
                className="pm-select-sm"
              >
                {operators.map(op => <option key={op} value={op}>{op}</option>)}
              </select>
              <input
                type="number"
                value={constraint.value || ''}
                onChange={e => onChange({ ...constraint, value: e.target.value })}
                placeholder="Enter value..."
                className="pm-input"
                step={constraint.type === 'volatility' ? '0.01' : '1'}
              />
            </>
          )}
          
          {constraint.type === 'trend' && (
            <select
              value={constraint.trendValue || '1'}
              onChange={e => onChange({ ...constraint, trendValue: e.target.value })}
              className="pm-select"
            >
              {trendValues.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
            </select>
          )}
          
          {['count_above', 'count_below'].includes(constraint.type) && (
            <>
              <span className="pm-label">Threshold:</span>
              <input
                type="number"
                value={constraint.threshold || ''}
                onChange={e => onChange({ ...constraint, threshold: e.target.value })}
                placeholder="Threshold..."
                className="pm-input-sm"
              />
              <span className="pm-label">Count</span>
              <select
                value={constraint.operator || '>'}
                onChange={e => onChange({ ...constraint, operator: e.target.value })}
                className="pm-select-sm"
              >
                {operators.map(op => <option key={op} value={op}>{op}</option>)}
              </select>
              <input
                type="number"
                value={constraint.countValue || ''}
                onChange={e => onChange({ ...constraint, countValue: e.target.value })}
                placeholder="Count..."
                className="pm-input-sm"
              />
            </>
          )}
          
          {['percent_above', 'percent_below'].includes(constraint.type) && (
            <>
              <span className="pm-label">Threshold:</span>
              <input
                type="number"
                value={constraint.threshold || ''}
                onChange={e => onChange({ ...constraint, threshold: e.target.value })}
                placeholder="Threshold..."
                className="pm-input-sm"
              />
              <span className="pm-label">%</span>
              <select
                value={constraint.operator || '>'}
                onChange={e => onChange({ ...constraint, operator: e.target.value })}
                className="pm-select-sm"
              >
                {operators.map(op => <option key={op} value={op}>{op}</option>)}
              </select>
              <input
                type="number"
                value={constraint.percentValue || '0.5'}
                onChange={e => onChange({ ...constraint, percentValue: e.target.value })}
                placeholder="0.5"
                className="pm-input-sm"
                step="0.1"
                min="0"
                max="1"
              />
            </>
          )}
          
          {constraint.type === 'pattern' && (
            <>
              <select
                value={constraint.template || 'spike'}
                onChange={e => onChange({ ...constraint, template: e.target.value })}
                className="pm-select"
              >
                {patternTemplates.map(t => (
                  <option key={t.value} value={t.value}>{t.label}</option>
                ))}
              </select>
              <span className="pm-label">Length:</span>
              <input
                type="number"
                value={constraint.patternLength || '10'}
                onChange={e => onChange({ ...constraint, patternLength: e.target.value })}
                className="pm-input-sm"
                min="3"
                max="100"
              />
            </>
          )}
          
          {constraint.type === 'similar_reference' && (
            <div className="pm-ref-ts-input">
              <div className="pm-ref-ts-row">
                <div className="pm-ref-ts-col">
                  <label>Timestamps (one per line)</label>
                  <textarea
                    value={constraint.refTimestamps || ''}
                    onChange={e => onChange({ ...constraint, refTimestamps: e.target.value })}
                    placeholder="2024-06-01T00:00:00&#10;2024-06-01T01:00:00"
                    rows={3}
                  />
                </div>
                <div className="pm-ref-ts-col">
                  <label>Values (one per line)</label>
                  <textarea
                    value={constraint.refValues || ''}
                    onChange={e => onChange({ ...constraint, refValues: e.target.value })}
                    placeholder="10&#10;15&#10;20"
                    rows={3}
                  />
                </div>
              </div>
              <div className="pm-ref-ts-tolerance">
                <span>Tolerance:</span>
                <input
                  type="number"
                  value={constraint.tolerance || '0.1'}
                  onChange={e => onChange({ ...constraint, tolerance: e.target.value })}
                  step="0.05"
                  min="0"
                  max="1"
                  className="pm-input-sm"
                />
              </div>
            </div>
          )}
          
          {constraint.type === 'similar_template' && (
            <>
              <select
                value={constraint.similarityTemplate || 'stable'}
                onChange={e => onChange({ ...constraint, similarityTemplate: e.target.value })}
                className="pm-select"
              >
                {similarityTemplates.map(t => (
                  <option key={t.value} value={t.value}>{t.label}</option>
                ))}
              </select>
              <span className="pm-label">Tolerance:</span>
              <input
                type="number"
                value={constraint.tolerance || '0.1'}
                onChange={e => onChange({ ...constraint, tolerance: e.target.value })}
                step="0.05"
                min="0"
                max="1"
                className="pm-input-sm"
              />
            </>
          )}
        </div>
      </div>
    );
  };
  

  
  const StaticConstraintEditor = ({ constraint, onChange, onRemove, availableProps }) => (
    <div className="pm-constraint-row">
      <select
        value={constraint.property}
        onChange={e => onChange({ ...constraint, property: e.target.value })}
        className="pm-select"
      >
        <option value="">Select property...</option>
        {availableProps.map(p => <option key={p} value={p}>{p}</option>)}
      </select>
      <select
        value={constraint.operator || '>='}
        onChange={e => onChange({ ...constraint, operator: e.target.value })}
        className="pm-select-sm"
      >
        {operators.map(op => <option key={op} value={op}>{op}</option>)}
      </select>
      <input
        type="text"
        value={constraint.value || ''}
        onChange={e => onChange({ ...constraint, value: e.target.value })}
        placeholder="Enter value..."
        className="pm-input"
      />
      <button className="pm-btn-remove" onClick={onRemove}>×</button>
    </div>
  );


  
  return (
    <div className="pattern-matching-panel">
      {/* Main container with two side-by-side sections */}
      <div className="pm-main-container">

        {/* LEFT: SIMPLE FILTER */}
        <div className={`pm-section ${simpleFilterOpen ? 'open' : 'collapsed'}`}>
          <div className="pm-section-header" onClick={() => setSimpleFilterOpen(!simpleFilterOpen)}>
            <span className="pm-section-title">Simple Filter</span>
            <span className="pm-collapse-icon">{simpleFilterOpen ? '▼' : '▶'}</span>
          </div>
          
          {simpleFilterOpen && <div className="pm-section-content">
            {/* Layout wrapper: Filter form + SubHyGraph sidebar */}
            <div className="pm-filter-layout">
              {/* LEFT: Filter form */}
              <div className="pm-filter-main">
                {/* Sub-tabs for Node/Edge filtering */}
                <div className="pm-subtabs">
              <button className={simpleTab === 'nodes' ? 'active' : ''} onClick={() => setSimpleTab('nodes')}>
                Nodes
              </button>
              <button className={simpleTab === 'edges' ? 'active' : ''} onClick={() => setSimpleTab('edges')}>
                Edges
              </button>
            </div>
            
            {/* NODE FILTER */}
            {simpleTab === 'nodes' && (
              <div className="pm-filter-form">
                <div className="pm-form-row">
                  <div className="pm-form-group">
                    <label>Label</label>
                    <select
                      value={nodeFilter.label}
                      onChange={e => setNodeFilter({...nodeFilter, label: e.target.value})}
                      className="pm-select"
                    >
                      <option value="">All labels</option>
                      {getAvailableLabels().map(l => <option key={l} value={l}>{l}</option>)}
                    </select>
                  </div>
                  <div className="pm-form-group">
                    <label>ID Contains</label>
                    <input
                      type="text"
                      value={nodeFilter.idContains}
                      onChange={e => setNodeFilter({...nodeFilter, idContains: e.target.value})}
                      placeholder="Filter by ID..."
                      className="pm-input"
                    />
                  </div>
                </div>
                
                <div className="pm-form-row">
                  <div className="pm-form-group">
                    <label>At Time</label>
                    <input
                      type="datetime-local"
                      value={nodeFilter.atTime}
                      onChange={e => setNodeFilter({...nodeFilter, atTime: e.target.value, betweenStart: '', betweenEnd: ''})}
                      className="pm-input"
                    />
                  </div>
                </div>
                
                <div className="pm-form-row">
                  <div className="pm-form-group">
                    <label>Between Start</label>
                    <input
                      type="datetime-local"
                      value={nodeFilter.betweenStart}
                      onChange={e => setNodeFilter({...nodeFilter, betweenStart: e.target.value, atTime: ''})}
                      className="pm-input"
                    />
                  </div>
                  <div className="pm-form-group">
                    <label>Between End</label>
                    <input
                      type="datetime-local"
                      value={nodeFilter.betweenEnd}
                      onChange={e => setNodeFilter({...nodeFilter, betweenEnd: e.target.value, atTime: ''})}
                      className="pm-input"
                    />
                  </div>
                </div>
                
                {/* Static Constraints */}
                <div className="pm-constraints-section">
                  <div className="pm-constraints-header">
                    <span>Static Constraints</span>
                    <button 
                      className="pm-btn-add"
                      onClick={() => setNodeFilter({
                        ...nodeFilter,
                        staticConstraints: [...nodeFilter.staticConstraints, { property: '', operator: '>=', value: '' }]
                      })}
                    >+ Add</button>
                  </div>
                  {nodeFilter.staticConstraints.map((sc, idx) => (
                    <StaticConstraintEditor
                      key={idx}
                      constraint={sc}
                      onChange={(updated) => {
                        const newConstraints = [...nodeFilter.staticConstraints];
                        newConstraints[idx] = updated;
                        setNodeFilter({...nodeFilter, staticConstraints: newConstraints});
                      }}
                      onRemove={() => {
                        setNodeFilter({
                          ...nodeFilter,
                          staticConstraints: nodeFilter.staticConstraints.filter((_, i) => i !== idx)
                        });
                      }}
                      availableProps={getAvailableStaticProperties()}
                    />
                  ))}
                </div>
                
                {/* Temporal Constraints */}
                <div className="pm-constraints-section">
                  <div className="pm-constraints-header">
                    <span>Temporal Constraints</span>
                    <button 
                      className="pm-btn-add"
                      onClick={() => setNodeFilter({
                        ...nodeFilter,
                        tsConstraints: [...nodeFilter.tsConstraints, { property: '', type: 'aggregation', aggregation: 'mean', operator: '<', value: '' }]
                      })}
                    >+ Add</button>
                  </div>
                  {nodeFilter.tsConstraints.map((tc, idx) => (
                    <TSConstraintEditor
                      key={idx}
                      constraint={tc}
                      onChange={(updated) => {
                        const newConstraints = [...nodeFilter.tsConstraints];
                        newConstraints[idx] = updated;
                        setNodeFilter({...nodeFilter, tsConstraints: newConstraints});
                      }}
                      onRemove={() => {
                        setNodeFilter({
                          ...nodeFilter,
                          tsConstraints: nodeFilter.tsConstraints.filter((_, i) => i !== idx)
                        });
                      }}
                      availableProps={getAvailableTSProperties()}
                    />
                  ))}
                </div>
                
                <button className="pm-btn-execute" onClick={executeNodeFilter} disabled={loading}>
                  {loading ? 'Searching...' : 'Filter Nodes'}
                </button>
              </div>
            )}
            
            {/* EDGE FILTER */}
            {simpleTab === 'edges' && (
              <div className="pm-filter-form">
                <div className="pm-form-row">
                  <div className="pm-form-group">
                    <label>Label</label>
                    <select
                      value={edgeFilter.label}
                      onChange={e => setEdgeFilter({...edgeFilter, label: e.target.value})}
                      className="pm-select"
                    >
                      <option value="">All labels</option>
                      {getAvailableEdgeLabels().map(l => <option key={l} value={l}>{l}</option>)}
                    </select>
                  </div>
                  <div className="pm-form-group">
                    <label>ID Contains</label>
                    <input
                      type="text"
                      value={edgeFilter.idContains}
                      onChange={e => setEdgeFilter({...edgeFilter, idContains: e.target.value})}
                      placeholder="Filter by ID..."
                      className="pm-input"
                    />
                  </div>
                </div>
                
                <div className="pm-form-row">
                  <div className="pm-form-group">
                    <label>Source Node</label>
                    <input
                      type="text"
                      value={edgeFilter.sourceId}
                      onChange={e => setEdgeFilter({...edgeFilter, sourceId: e.target.value})}
                      placeholder="Source node ID..."
                      className="pm-input"
                      list="source-nodes"
                    />
                    <datalist id="source-nodes">
                      {getNodeIds().slice(0, 50).map(id => <option key={id} value={id} />)}
                    </datalist>
                  </div>
                  <div className="pm-form-group">
                    <label>Target Node</label>
                    <input
                      type="text"
                      value={edgeFilter.targetId}
                      onChange={e => setEdgeFilter({...edgeFilter, targetId: e.target.value})}
                      placeholder="Target node ID..."
                      className="pm-input"
                      list="target-nodes"
                    />
                    <datalist id="target-nodes">
                      {getNodeIds().slice(0, 50).map(id => <option key={id} value={id} />)}
                    </datalist>
                  </div>
                </div>
                
                {/* Static Constraints */}
                <div className="pm-constraints-section">
                  <div className="pm-constraints-header">
                    <span>Static Constraints</span>
                    <button 
                      className="pm-btn-add"
                      onClick={() => setEdgeFilter({
                        ...edgeFilter,
                        staticConstraints: [...edgeFilter.staticConstraints, { property: '', operator: '>=', value: '' }]
                      })}
                    >+ Add</button>
                  </div>
                  {edgeFilter.staticConstraints.map((sc, idx) => (
                    <StaticConstraintEditor
                      key={idx}
                      constraint={sc}
                      onChange={(updated) => {
                        const newConstraints = [...edgeFilter.staticConstraints];
                        newConstraints[idx] = updated;
                        setEdgeFilter({...edgeFilter, staticConstraints: newConstraints});
                      }}
                      onRemove={() => {
                        setEdgeFilter({
                          ...edgeFilter,
                          staticConstraints: edgeFilter.staticConstraints.filter((_, i) => i !== idx)
                        });
                      }}
                      availableProps={getAvailableEdgeStaticProperties()}
                    />
                  ))}
                </div>
                
                {/* Temporal Constraints */}
                <div className="pm-constraints-section">
                  <div className="pm-constraints-header">
                    <span>Temporal Constraints</span>
                    <button 
                      className="pm-btn-add"
                      onClick={() => setEdgeFilter({
                        ...edgeFilter,
                        tsConstraints: [...edgeFilter.tsConstraints, { property: '', type: 'aggregation', aggregation: 'mean', operator: '<', value: '' }]
                      })}
                    >+ Add</button>
                  </div>
                  {edgeFilter.tsConstraints.map((tc, idx) => (
                    <TSConstraintEditor
                      key={idx}
                      constraint={tc}
                      onChange={(updated) => {
                        const newConstraints = [...edgeFilter.tsConstraints];
                        newConstraints[idx] = updated;
                        setEdgeFilter({...edgeFilter, tsConstraints: newConstraints});
                      }}
                      onRemove={() => {
                        setEdgeFilter({
                          ...edgeFilter,
                          tsConstraints: edgeFilter.tsConstraints.filter((_, i) => i !== idx)
                        });
                      }}
                      availableProps={getAvailableEdgeTSProperties()}
                    />
                  ))}
                </div>
                
                <button className="pm-btn-execute" onClick={executeEdgeFilter} disabled={loading}>
                  {loading ? 'Searching...' : 'Filter Edges'}
                </button>
              </div>
            )}
              </div>
              
              {/* RIGHT: SubHyGraph sidebar (on large screens) */}
              <div className="pm-subhygraph-sidebar">
                {renderSubHyGraphBlock()}
              </div>
            </div>
          </div>}
        </div>
        
        {/* RIGHT: PATTERN MATCHING */}
        <div className={`pm-section ${patternMatchingOpen ? 'open' : 'collapsed'}`}>
          <div className="pm-section-header pattern" onClick={() => setPatternMatchingOpen(!patternMatchingOpen)}>
            <span className="pm-section-title">Pattern Matching</span>
            <span className="pm-collapse-icon">{patternMatchingOpen ? '▼' : '▶'}</span>
          </div>
          
          {patternMatchingOpen && <div className="pm-section-content">
            {/* Node Patterns */}
            <div className="pm-pattern-block">
              <div className="pm-pattern-block-header">
                <span>Node Patterns</span>
                <button className="pm-btn-add" onClick={addPatternNode}>+ Node</button>
              </div>
              
              {patternNodes.map((node, idx) => (
                <div key={node.id} className="pm-pattern-item">
                  <div className="pm-pattern-item-header">
                    <span className="pm-var-badge">{node.variable}</span>
                    <select
                      value={node.label}
                      onChange={e => updatePatternNode(node.id, { label: e.target.value })}
                      className="pm-select"
                    >
                      <option value="">Any label</option>
                      {getAvailableLabels().map(l => <option key={l} value={l}>{l}</option>)}
                    </select>
                    <input
                      type="text"
                      value={node.uid}
                      onChange={e => updatePatternNode(node.id, { uid: e.target.value })}
                      placeholder="Specific UID (optional)..."
                      className="pm-input"
                    />
                    {idx > 0 && <button className="pm-btn-remove" onClick={() => removePatternNode(node.id)}>×</button>}
                  </div>
                  
                  {/* Node Static Constraints */}
                  <div className="pm-pattern-constraints">
                    <div className="pm-constraints-header-sm">
                      <small>Static:</small>
                      <button 
                        className="pm-btn-add-sm"
                        onClick={() => updatePatternNode(node.id, {
                          staticConstraints: [...(node.staticConstraints || []), { property: '', operator: '>=', value: '' }]
                        })}
                      >+</button>
                    </div>
                    {(node.staticConstraints || []).map((sc, scIdx) => (
                      <StaticConstraintEditor
                        key={scIdx}
                        constraint={sc}
                        onChange={(updated) => {
                          const newConstraints = [...node.staticConstraints];
                          newConstraints[scIdx] = updated;
                          updatePatternNode(node.id, { staticConstraints: newConstraints });
                        }}
                        onRemove={() => {
                          updatePatternNode(node.id, {
                            staticConstraints: node.staticConstraints.filter((_, i) => i !== scIdx)
                          });
                        }}
                        availableProps={getAvailableStaticProperties()}
                      />
                    ))}
                  </div>
                  
                  {/* Node TS Constraints */}
                  <div className="pm-pattern-constraints">
                    <div className="pm-constraints-header-sm">
                      <small>Temporal:</small>
                      <button 
                        className="pm-btn-add-sm"
                        onClick={() => updatePatternNode(node.id, {
                          tsConstraints: [...(node.tsConstraints || []), { property: '', type: 'aggregation', aggregation: 'mean', operator: '<', value: '' }]
                        })}
                      >+</button>
                    </div>
                    {(node.tsConstraints || []).map((tc, tcIdx) => (
                      <TSConstraintEditor
                        key={tcIdx}
                        constraint={tc}
                        onChange={(updated) => {
                          const newConstraints = [...node.tsConstraints];
                          newConstraints[tcIdx] = updated;
                          updatePatternNode(node.id, { tsConstraints: newConstraints });
                        }}
                        onRemove={() => {
                          updatePatternNode(node.id, {
                            tsConstraints: node.tsConstraints.filter((_, i) => i !== tcIdx)
                          });
                        }}
                        availableProps={getAvailableTSProperties()}
                      />
                    ))}
                  </div>
                </div>
              ))}
            </div>
            
            {/* Edge Patterns */}
            <div className="pm-pattern-block">
              <div className="pm-pattern-block-header">
                <span>Edge Patterns</span>
                <button className="pm-btn-add" onClick={addPatternEdge} disabled={patternNodes.length < 2}>+ Edge</button>
              </div>
              
              {patternEdges.map(edge => (
                <div key={edge.id} className="pm-pattern-item edge">
                  <div className="pm-pattern-item-header">
                    <span className="pm-var-badge edge">{edge.variable}</span>
                    <select
                      value={edge.sourceVar}
                      onChange={e => updatePatternEdge(edge.id, { sourceVar: e.target.value })}
                      className="pm-select-sm"
                    >
                      {patternNodes.map(n => <option key={n.variable} value={n.variable}>{n.variable}</option>)}
                    </select>
                    <span className="pm-arrow">→</span>
                    <select
                      value={edge.targetVar}
                      onChange={e => updatePatternEdge(edge.id, { targetVar: e.target.value })}
                      className="pm-select-sm"
                    >
                      {patternNodes.map(n => <option key={n.variable} value={n.variable}>{n.variable}</option>)}
                    </select>
                    <select
                      value={edge.label}
                      onChange={e => updatePatternEdge(edge.id, { label: e.target.value })}
                      className="pm-select"
                    >
                      <option value="">Any label</option>
                      {getAvailableEdgeLabels().map(l => <option key={l} value={l}>{l}</option>)}
                    </select>
                    <button className="pm-btn-remove" onClick={() => removePatternEdge(edge.id)}>×</button>
                  </div>
                  
                  {/* Edge Static Constraints */}
                  <div className="pm-pattern-constraints">
                    <div className="pm-constraints-header-sm">
                      <small>Static:</small>
                      <button 
                        className="pm-btn-add-sm"
                        onClick={() => updatePatternEdge(edge.id, {
                          staticConstraints: [...(edge.staticConstraints || []), { property: '', operator: '>=', value: '' }]
                        })}
                      >+</button>
                    </div>
                    {(edge.staticConstraints || []).map((sc, scIdx) => (
                      <StaticConstraintEditor
                        key={scIdx}
                        constraint={sc}
                        onChange={(updated) => {
                          const newConstraints = [...edge.staticConstraints];
                          newConstraints[scIdx] = updated;
                          updatePatternEdge(edge.id, { staticConstraints: newConstraints });
                        }}
                        onRemove={() => {
                          updatePatternEdge(edge.id, {
                            staticConstraints: edge.staticConstraints.filter((_, i) => i !== scIdx)
                          });
                        }}
                        availableProps={getAvailableEdgeStaticProperties()}
                      />
                    ))}
                  </div>
                  
                  {/* Edge TS Constraints */}
                  <div className="pm-pattern-constraints">
                    <div className="pm-constraints-header-sm">
                      <small>Temporal:</small>
                      <button 
                        className="pm-btn-add-sm"
                        onClick={() => updatePatternEdge(edge.id, {
                          tsConstraints: [...(edge.tsConstraints || []), { property: '', type: 'aggregation', aggregation: 'mean', operator: '<', value: '' }]
                        })}
                      >+</button>
                    </div>
                    {(edge.tsConstraints || []).map((tc, tcIdx) => (
                      <TSConstraintEditor
                        key={tcIdx}
                        constraint={tc}
                        onChange={(updated) => {
                          const newConstraints = [...edge.tsConstraints];
                          newConstraints[tcIdx] = updated;
                          updatePatternEdge(edge.id, { tsConstraints: newConstraints });
                        }}
                        onRemove={() => {
                          updatePatternEdge(edge.id, {
                            tsConstraints: edge.tsConstraints.filter((_, i) => i !== tcIdx)
                          });
                        }}
                        availableProps={getAvailableEdgeTSProperties()}
                      />
                    ))}
                  </div>
                </div>
              ))}
            </div>
            
            {/* Cross Constraints */}
            <div className="pm-pattern-block">
              <div className="pm-pattern-block-header">
                <span>Cross-Entity Constraints</span>
                <button className="pm-btn-add" onClick={() => setCrossConstraints([...crossConstraints, { left: '', operator: '<', right: '' }])}>+ Add</button>
              </div>
              <p className="pm-hint-text">Compare properties: n2.capacity &lt; n1.capacity</p>
              
              {crossConstraints.map((cc, idx) => (
                <div key={idx} className="pm-cross-constraint-row">
                  <input
                    type="text"
                    value={cc.left}
                    onChange={e => {
                      const updated = [...crossConstraints];
                      updated[idx] = {...cc, left: e.target.value};
                      setCrossConstraints(updated);
                    }}
                    placeholder="n1.property"
                    className="pm-input"
                  />
                  <select
                    value={cc.operator}
                    onChange={e => {
                      const updated = [...crossConstraints];
                      updated[idx] = {...cc, operator: e.target.value};
                      setCrossConstraints(updated);
                    }}
                    className="pm-select-sm"
                  >
                    {operators.map(op => <option key={op} value={op}>{op}</option>)}
                  </select>
                  <input
                    type="text"
                    value={cc.right}
                    onChange={e => {
                      const updated = [...crossConstraints];
                      updated[idx] = {...cc, right: e.target.value};
                      setCrossConstraints(updated);
                    }}
                    placeholder="n2.property or value"
                    className="pm-input"
                  />
                  <button 
                    className="pm-btn-remove"
                    onClick={() => setCrossConstraints(crossConstraints.filter((_, i) => i !== idx))}
                  >×</button>
                </div>
              ))}
            </div>
            
            {/* Cross-TS Constraints */}
            <div className="pm-pattern-block">
              <div className="pm-pattern-block-header">
                <span>Cross-TS Constraints</span>
                <button className="pm-btn-add" onClick={() => setCrossTSConstraints([...crossTSConstraints, { left: '', operator: 'correlates', right: '', threshold: '0.7' }])}>+ Add</button>
              </div>
              <p className="pm-hint-text">Compare time series: n1.num_bikes correlates n2.num_bikes</p>
              
              {crossTSConstraints.map((ctc, idx) => (
                <div key={idx} className="pm-cross-ts-constraint-row">
                  <input
                    type="text"
                    value={ctc.left}
                    onChange={e => {
                      const updated = [...crossTSConstraints];
                      updated[idx] = {...ctc, left: e.target.value};
                      setCrossTSConstraints(updated);
                    }}
                    placeholder="n1.ts_property"
                    className="pm-input"
                  />
                  <select
                    value={ctc.operator}
                    onChange={e => {
                      const updated = [...crossTSConstraints];
                      updated[idx] = {...ctc, operator: e.target.value};
                      setCrossTSConstraints(updated);
                    }}
                    className="pm-select"
                  >
                    {crossTSOperators.map(op => (
                      <option key={op.value} value={op.value}>{op.label}</option>
                    ))}
                  </select>
                  <input
                    type="text"
                    value={ctc.right}
                    onChange={e => {
                      const updated = [...crossTSConstraints];
                      updated[idx] = {...ctc, right: e.target.value};
                      setCrossTSConstraints(updated);
                    }}
                    placeholder="n2.ts_property"
                    className="pm-input"
                  />
                  <input
                    type="number"
                    value={ctc.threshold}
                    onChange={e => {
                      const updated = [...crossTSConstraints];
                      updated[idx] = {...ctc, threshold: e.target.value};
                      setCrossTSConstraints(updated);
                    }}
                    placeholder="0.7"
                    step="0.1"
                    min="0"
                    max="1"
                    className="pm-input-sm"
                    title="Threshold"
                  />
                  <button 
                    className="pm-btn-remove"
                    onClick={() => setCrossTSConstraints(crossTSConstraints.filter((_, i) => i !== idx))}
                  >×</button>
                </div>
              ))}
            </div>
            
            <button className="pm-btn-execute" onClick={executePatternMatch} disabled={loading}>
              {loading ? 'Searching...' : 'Execute Pattern Match'}
            </button>
          </div>}
        </div>
      </div>
      
      {/* RESULTS */}
      {error && <div className="pm-error">{error}</div>}
      
      {results && (
        <div className="pm-results">
          <div className="pm-results-header">
            <span className="pm-results-count">
              {results.nodes?.length || 0} nodes, {results.edges?.length || 0} edges
              {results.matches && ` (${results.matches.length} matches)`}
            </span>
            <div className="pm-results-actions">
              <button className="pm-btn-save" onClick={() => setShowSaveDialog(true)} disabled={!results.node_ids?.length}>
                Save
              </button>
              <button className="pm-btn-clear" onClick={clearResults}>Clear</button>
            </div>
          </div>
          
          {tableResult && (
            <div className="pm-table-result">
              <table>
                <thead>
                  <tr>
                    {tableResult.columns.map((col, i) => <th key={i}>{col}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {tableResult.rows.slice(0, 10).map((row, i) => (
                    <tr key={i}>
                      {row.map((cell, j) => <td key={j}>{cell}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
              {tableResult.rows.length > 10 && (
                <div className="pm-table-more">+{tableResult.rows.length - 10} more rows</div>
              )}
            </div>
          )}
        </div>
      )}
      
      {/* Save Dialog */}
      {showSaveDialog && (
        <div className="pm-modal-overlay" onClick={() => setShowSaveDialog(false)}>
          <div className="pm-save-dialog" onClick={e => e.stopPropagation()}>
            <h3>Save as SubHyGraph</h3>
            <div className="pm-form-group">
              <label>Name *</label>
              <input
                type="text"
                value={saveName}
                onChange={e => setSaveName(e.target.value)}
                placeholder="e.g., Low Usage Stations"
                className="pm-input"
                autoFocus
              />
            </div>
            <div className="pm-form-group">
              <label>Description</label>
              <textarea
                value={saveDescription}
                onChange={e => setSaveDescription(e.target.value)}
                placeholder="Optional description..."
                rows={2}
                className="pm-textarea"
              />
            </div>
            <div className="pm-save-info">
              Saving {results?.node_ids?.length || 0} nodes as a reusable SubHyGraph
            </div>
            <div className="pm-dialog-actions">
              <button className="pm-btn-cancel" onClick={() => setShowSaveDialog(false)}>Cancel</button>
              <button className="pm-btn-confirm" onClick={saveAsSubHyGraph}>Save</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PatternMatchingPanel;
