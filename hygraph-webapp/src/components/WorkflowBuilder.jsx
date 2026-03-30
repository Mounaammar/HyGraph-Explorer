import React, { useState, useEffect, useRef } from 'react';
import * as echarts from 'echarts';
import GraphView from './GraphView';
import TimeSeriesPanel from './TimeSeriesPanel';
import MetadataPanel from './MetadataPanel';
import ChangeExplorer from './ChangeExplorer';
import InducedExplorer from './InducedExplorer';
import './WorkflowBuilder.css';

const API_BASE_URL = 'http://localhost:8000';

const WorkflowBuilder = ({ onBack, onDataChanged, onDiffAsHyGraph }) => {
  // Pipeline state
  const [steps, setSteps] = useState([]);
  const [selectedStepId, setSelectedStepId] = useState(null);
  const [workflowName, setWorkflowName] = useState('New Workflow');
  
  // Execution state
  const [executing, setExecuting] = useState(false);
  const [executionLog, setExecutionLog] = useState([]);
  
  // Saved subhygraphs for selection
  const [savedSubHyGraphs, setSavedSubHyGraphs] = useState([]);
  
  // Available TS variables and static properties (fetched from DB)
  const [tsVariables, setTsVariables] = useState(['speed']);
  const [labelStaticProps, setLabelStaticProps] = useState([]);

  // SubHyGraph visualization state
  const [subhygraphData, setSubhygraphData] = useState(null);
  const [selectedElement, setSelectedElement] = useState(null);
  const [vizMode, setVizMode] = useState('force');
  
  // Get selected step
  const selectedStep = steps.find(s => s.id === selectedStepId);
  
  // Load saved subhygraphs and TS variables on mount
  useEffect(() => {
    loadSavedSubHyGraphs();
    loadTsVariables();
  }, []);
  
  const loadSavedSubHyGraphs = async () => {
    try {
      const res = await fetch(`${API_BASE_URL}/api/subhygraph/list`);
      if (res.ok) {
        const data = await res.json();
        setSavedSubHyGraphs(data.subhygraphs || []);
      }
    } catch (err) {
      console.error('Failed to load saved subhygraphs:', err);
    }
  };

  const loadTsVariables = async () => {
    try {
      const res = await fetch(`${API_BASE_URL}/api/ts_variables`);
      if (res.ok) {
        const data = await res.json();
        if (data.variables?.length > 0) setTsVariables(data.variables);
      }
    } catch (err) {
      console.error('Failed to load TS variables:', err);
    }
  };

  // Fetch numeric static properties for nodes with a given label
  const loadPropsForLabel = async (label) => {
    if (!label) { setLabelStaticProps([]); return; }
    try {
      const res = await fetch(`${API_BASE_URL}/api/query/nodes`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ label, limit: 5 }),
      });
      if (res.ok) {
        const data = await res.json();
        const propSet = new Set();
        (data.nodes || []).forEach(n => {
          Object.entries(n.static_properties || {}).forEach(([k, v]) => {
            if (k !== 'temporal_properties' && typeof v !== 'object') {
              const num = parseFloat(v);
              if (!isNaN(num)) propSet.add(k);
            }
          });
        });
        setLabelStaticProps(Array.from(propSet).sort());
      }
    } catch (err) {
      console.error('Failed to load label properties:', err);
    }
  };
  
  /**
   * Check if end time is after start time
   */
  const isValidTimeRange = (start, end) => {
    if (!start || !end) return false;
    const startDate = new Date(start);
    const endDate = new Date(end);
    return endDate > startDate;
  };
  
  /**
   * Get time validation error message for a step
   */
  const getTimeValidationError = (step) => {
    if (!step) return null;
    
    switch (step.type) {
      case 'snapshot_sequence':
        if (!isValidTimeRange(step.config.start, step.config.end)) {
          return 'End time must be after start time';
        }
        break;
      case 'tsgen':
        // TSGen uses input sequence's time range, check if we have valid input
        const inputStep = steps.find(s => s.id === step.inputRef);
        if (inputStep?.type === 'snapshot_sequence') {
          if (!isValidTimeRange(inputStep.config.start, inputStep.config.end)) {
            return 'Input sequence has invalid time range';
          }
        }
        break;
      case 'hygraph_diff':
        // No time range validation needed for diff (uses two snapshots)
        break;
    }
    return null;
  };
  
  /**
   * Check if step can be executed (valid config)
   */
  const canExecuteStep = (step) => {
    if (!step) return false;
    
    switch (step.type) {
      case 'subhygraph':
        return !!step.config.savedId;
      case 'snapshot':
        return !!step.config.timestamp;
      case 'snapshot_sequence':
        return isValidTimeRange(step.config.start, step.config.end);
      case 'tsgen': {
        const inputStep = steps.find(s => s.id === step.inputRef);
        if (!inputStep) return false;
        return !!(inputStep.result?.sequence_id || 
                  (inputStep.type === 'snapshot_sequence' && 
                   inputStep.config?.start && 
                   inputStep.config?.end));
      }
      case 'predicate_induced':
        return !!(step.config.predicate && step.config.start_time && step.config.end_time &&
                  isValidTimeRange(step.config.start_time, step.config.end_time));
      case 'tsgen_induced': {
        const inp = steps.find(s => s.id === step.inputRef);
        return !!(inp?.result?.induced_id);
      }
      case 'hygraph_diff': {
        const inp = steps.find(s => s.id === step.inputRef);
        return !!(inp?.result?.induced_id &&
                  step.config.period_1_start && step.config.period_1_end &&
                  step.config.period_2_start && step.config.period_2_end);
      }
      default:
        return true;
    }
  };
  
  // Available step types
  const stepTypes = [
    { type: 'subhygraph', label: 'SubHyGraph', icon: 'images/knowledge-graph.png', description: 'Filter nodes by label/property' },
    { type: 'snapshot', label: 'Snapshot', icon: 'images/snapshot.png', description: 'Point-in-time snapshot' },
    { type: 'snapshot_sequence', label: 'SnapshotSequence', icon: 'images/snapshots.png', description: 'Sequence of snapshots' },
    { type: 'tsgen', label: 'TSGen', icon: 'images/forecasting.png', description: 'Generate time series from sequence' },
    { type: 'predicate_induced', label: 'SubHyGraph (Predicate-Induced)', icon: 'images/knowledge-graph.png', description: 'Time-evolving subgraph via TS predicate' },
    { type: 'tsgen_induced', label: 'TSGen (Induced)', icon: 'images/forecasting.png', description: 'Graph-level signals from induced subgraph' },
    { type: 'hygraph_diff', label: 'HyGraphDiff', icon: 'images/chart.png', description: 'Compare two periods of a HyGraph instance' },
  ];

  // Get available inputs for a step (outputs from previous steps)
  const getAvailableInputs = (stepIndex, requiredType = null) => {
    const inputs = [{ id: 'hygraph', name: 'HyGraph (root)', type: 'hygraph' }];
    
    for (let i = 0; i < stepIndex; i++) {
      const step = steps[i];
      if (step.result) {
        const outputType = getStepOutputType(step.type);
        if (!requiredType || outputType === requiredType) {
          inputs.push({
            id: step.id,
            name: `$${step.name || step.type}_${i + 1}`,
            type: outputType
          });
        }
      }
    }
    return inputs;
  };

  const getStepOutputType = (stepType) => {
    switch (stepType) {
      case 'subhygraph': return 'hygraph';
      case 'snapshot': return 'snapshot';
      case 'snapshot_sequence': return 'snapshot_sequence';
      case 'tsgen': return 'timeseries';
      case 'predicate_induced': return 'induced';
      case 'tsgen_induced': return 'timeseries';
      case 'hygraph_diff': return 'diff_result';
      default: return 'unknown';
    }
  };

  // Add new step
  const addStep = (type) => {
    const stepInfo = stepTypes.find(s => s.type === type);
    const newStep = {
      id: `step_${Date.now()}`,
      type,
      name: stepInfo?.label || type,
      config: getDefaultConfig(type),
      inputRef: steps.length > 0 ? steps[steps.length - 1].id : 'hygraph',
      inputRef2: null, // For HyGraphDiff second input
      result: null,
      status: 'pending', // pending, running, success, error
      error: null
    };
    setSteps([...steps, newStep]);
    setSelectedStepId(newStep.id);
  };

  const getDefaultConfig = (type) => {
    switch (type) {
      case 'subhygraph':
        return { 
          savedId: '' // Only saved subhygraphs, no filter mode
        };
      case 'snapshot':
        return { timestamp: '2024-06-01T12:00:00', mode: 'hybrid' };
      case 'snapshot_sequence':
        return { start: '2024-06-01T00:00:00', end: '2024-06-30T00:00:00', granularity: '1D', mode: 'hybrid' };
      case 'tsgen':
        return { 
          scope: 'global', // global, entity
          entityType: 'nodes', // nodes, edges, graph
          metric: 'count', // count, degree, property, density, connected_components, biggest_component, between
          aggregation: 'avg', // avg, sum, min, max
          label: '',
          // For property metrics
          propertyName: '',
          // For entity-level (specific node/edge)
          entityId: '',
          // For degree
          direction: 'both', // in, out, both
          weight: '', // edge property for weighted degree
          // For edges.between()
          sourceNode: '',
          targetNode: '',
          directed: true,
          // Save config
          saveEnabled: false,
          saveTargetType: 'node', // node, edge, subhygraph
          saveTargetId: '',
          saveTargetLabel: '',
          savePropertyName: ''
        };
      case 'predicate_induced':
        return {
          predicate: 'speed / free_flow_speed < 0.6',
          pred_rows: [
            { ts: 'speed', math_op: '/', rhs_type: 'property', rhs_prop: 'free_flow_speed', rhs_num: '', comp_op: '<', threshold: '0.6' }
          ],
          pred_connectors: [],
          start_time: '2012-03-01T06:00:00',
          end_time: '2012-03-31T23:59:00',
          step_minutes: 60,
          node_label: 'Sensor',
          name: '',
        };
      case 'tsgen_induced':
        return {
          metrics: ['density', 'component_count', 'largest_component'],
          signal_metrics: [],
        };
      case 'hygraph_diff':
        return {
          mode: 'induced',            // 'induced' = two subgraph inputs (correct), 'predicate' = bundled shortcut
          predicate: 'speed / free_flow_speed < 0.6',
          node_label: 'Sensor',
          step_minutes: 60,

          min_frequency: 0.6,
          delta_max_days: 1.0,
          // induced mode fields (for backward compat)
          ts_property: 'speed',
        };
      default:
        return {};
    }
  };

  // Update step config
  const updateStepConfig = (stepId, newConfig) => {
    setSteps(steps.map(s => 
      s.id === stepId ? { ...s, config: { ...s.config, ...newConfig } } : s
    ));
  };

  // Update step input reference
  const updateStepInput = (stepId, inputRef, inputKey = 'inputRef') => {
    setSteps(steps.map(s => 
      s.id === stepId ? { ...s, [inputKey]: inputRef } : s
    ));
  };

  // Delete step
  const deleteStep = (stepId) => {
    setSteps(steps.filter(s => s.id !== stepId));
    if (selectedStepId === stepId) {
      setSelectedStepId(steps.length > 1 ? steps[0].id : null);
    }
  };

  // Move step up/down
  const moveStep = (stepId, direction) => {
    const index = steps.findIndex(s => s.id === stepId);
    if ((direction === 'up' && index === 0) || (direction === 'down' && index === steps.length - 1)) {
      return;
    }
    const newSteps = [...steps];
    const newIndex = direction === 'up' ? index - 1 : index + 1;
    [newSteps[index], newSteps[newIndex]] = [newSteps[newIndex], newSteps[index]];
    setSteps(newSteps);
  };

  // Execute single step
  const executeStep = async (stepId) => {
    const stepIndex = steps.findIndex(s => s.id === stepId);
    const step = steps[stepIndex];
    
    // Validate before executing
    if (!canExecuteStep(step)) {
      const error = getTimeValidationError(step) || 'Invalid configuration';
      setSteps(prev => prev.map(s => 
        s.id === stepId ? { ...s, status: 'error', error } : s
      ));
      addLog(`✗ ${step.name} failed: ${error}`);
      return;
    }
    
    setSteps(prev => prev.map(s => s.id === stepId ? { ...s, status: 'running', error: null } : s));
    
    try {
      let result;
      
      switch (step.type) {
        case 'subhygraph':
          result = await executeSubHyGraph(step);
          break;
        case 'snapshot':
          result = await executeSnapshot(step, stepIndex);
          break;
        case 'snapshot_sequence':
          result = await executeSnapshotSequence(step, stepIndex);
          break;
        case 'tsgen':
          result = await executeTSGen(step, stepIndex);
          break;
        case 'predicate_induced':
          result = await executePredicateInduced(step);
          break;
        case 'tsgen_induced':
          result = await executeTSGenInduced(step, stepIndex);
          break;
        case 'hygraph_diff':
          result = await executeHyGraphDiff(step, stepIndex);
          break;
        default:
          throw new Error(`Unknown step type: ${step.type}`);
      }
      
      setSteps(prev => prev.map(s => 
        s.id === stepId ? { ...s, result, status: 'success' } : s
      ));
      
      addLog(`✓ ${step.name} completed`);
    } catch (error) {
      setSteps(prev => prev.map(s => 
        s.id === stepId ? { ...s, status: 'error', error: error.message } : s
      ));
      addLog(`✗ ${step.name} failed: ${error.message}`);
    }
  };

  // Execute all steps
  const executeAll = async () => {
    setExecuting(true);
    setExecutionLog([]);
    addLog('Starting workflow execution...');
    
    for (let i = 0; i < steps.length; i++) {
      await executeStep(steps[i].id);
      // Small delay between steps
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    addLog('Workflow execution complete');
    setExecuting(false);
  };

  const addLog = (message) => {
    setExecutionLog(prev => [...prev, { time: new Date().toISOString(), message }]);
  };

  // API execution functions
  const executeSubHyGraph = async (step) => {
    // Only support saved subhygraphs
    if (!step.config.savedId) {
      throw new Error('Please select a saved SubHyGraph');
    }
    
    const res = await fetch(`${API_BASE_URL}/api/subhygraph/${step.config.savedId}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    
    // Load timeseries for visualization
    await loadTimeseriesForSubhygraph(data);
    
    // Store for visualization
    setSubhygraphData(data);
    
    return data;
  };
  
  // Load timeseries for subhygraph visualization
  const loadTimeseriesForSubhygraph = async (data) => {
    const allTsIds = new Set();
    
    // Collect TS IDs from nodes
    for (const node of data.nodes || []) {
      const tempProps = node.temporal_properties || {};
      for (const tsId of Object.values(tempProps)) {
        if (typeof tsId === 'string') allTsIds.add(tsId);
      }
    }
    
    // Collect TS IDs from edges
    for (const edge of data.edges || []) {
      const tempProps = edge.temporal_properties || {};
      for (const tsId of Object.values(tempProps)) {
        if (typeof tsId === 'string') allTsIds.add(tsId);
      }
    }
    
    // IMPORTANT: Also collect TS IDs from SubHyGraph's own temporal properties
    // These are stored directly on the SubHyGraph entity (from TSGen saves)
    const subhygraphTempProps = data.temporal_properties || {};
    for (const tsId of Object.values(subhygraphTempProps)) {
      if (typeof tsId === 'string') allTsIds.add(tsId);
    }
    

    
    if (allTsIds.size > 0) {
      try {
        const tsRes = await fetch(`${API_BASE_URL}/api/timeseries/batch`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ts_ids: Array.from(allTsIds) })
        });
        
        if (tsRes.ok) {
          const result = await tsRes.json();
          const timeseries = {};
          

          
          Object.entries(result.timeseries || {}).forEach(([tsId, ts]) => {
            if (ts.timestamps && ts.data && ts.data.length > 0) {
              // Transform to [{time, value}, ...] format
              // DON'T spread ...ts - it keeps timestamps which confuses normalizeTimeseriesData
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
          

          data.timeseries = timeseries;
        }
      } catch (err) {
        console.error('Failed to load timeseries:', err);
        data.timeseries = {};
      }
    } else {
      data.timeseries = {};
    }
  };

  const executeSnapshot = async (step, stepIndex) => {
    const res = await fetch(`${API_BASE_URL}/api/snapshot/at`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        timestamp: step.config.timestamp,
        mode: step.config.mode
      })
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  };

  const executeSnapshotSequence = async (step, stepIndex) => {
    const res = await fetch(`${API_BASE_URL}/api/snapshot-sequence/summary`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        start: step.config.start,
        end: step.config.end,
        granularity: step.config.granularity,
        mode: step.config.mode
      })
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  };

  const executeTSGen = async (step, stepIndex) => {
    // Get the input step - can be SnapshotSequence or direct from config
    const inputStep = steps.find(s => s.id === step.inputRef);
    
    let payload;
    let endpoint;
    
    // Check if input step has a sequence_id (already executed SnapshotSequence)
    if (inputStep?.result?.sequence_id) {
      // Use existing sequence
      endpoint = `${API_BASE_URL}/api/tsgen/generate`;
      payload = {
        sequence_id: inputStep.result.sequence_id,
        scope: step.config.scope,
        entity_type: step.config.entityType,
        metric: step.config.metric,
        aggregation: step.config.aggregation,
        label: step.config.label || null,
        property_name: step.config.propertyName || null,
        entity_id: step.config.entityId || null,
        direction: step.config.direction,
        weight: step.config.weight || null,
        source_node: step.config.sourceNode || null,
        target_node: step.config.targetNode || null,
        directed: step.config.directed
      };
    } else if (inputStep?.type === 'snapshot_sequence' && inputStep.config) {
      // Input is SnapshotSequence but not executed yet - use combined endpoint
      endpoint = `${API_BASE_URL}/api/tsgen/combined`;
      payload = {
        // SnapshotSequence params from input step
        start: inputStep.config.start,
        end: inputStep.config.end,
        granularity: inputStep.config.granularity,
        mode: inputStep.config.mode,
        // TSGen params
        scope: step.config.scope,
        entity_type: step.config.entityType,
        metric: step.config.metric,
        aggregation: step.config.aggregation,
        label: step.config.label || null,
        property_name: step.config.propertyName || null,
        entity_id: step.config.entityId || null,
        direction: step.config.direction,
        weight: step.config.weight || null,
        source_node: step.config.sourceNode || null,
        target_node: step.config.targetNode || null,
        directed: step.config.directed
      };
    } else {
      throw new Error('Please add a SnapshotSequence step before TSGen');
    }
    
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`HTTP ${res.status}: ${errText}`);
    }
    return await res.json();
  };

  // Save generated timeseries using TSGen Save endpoint
  const saveTSGenResult = async (step) => {
    if (!step.result?.timeseries) {
      throw new Error('No timeseries to save');
    }
    
    const ts = step.result.timeseries;
    const config = step.config;
    
    // Use the NEW TSGen save endpoint
    const payload = {
      timestamps: ts.timestamps,
      values: ts.data.map(d => Array.isArray(d) ? d[0] : d),
      variable_name: config.savePropertyName,
      source_metric: ts.name || config.metric || 'generated',
      target_type: config.saveTargetType,
      target_id: config.saveTargetId || null,
      target_label: config.saveTargetLabel || null,
      description: `Generated from TSGen (${config.scope} ${config.metric})`
    };
    
    const res = await fetch(`${API_BASE_URL}/api/tsgen/save`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`HTTP ${res.status}: ${errText}`);
    }
    
    return await res.json();
  };
  
  // Verify saved timeseries
  const verifyTSGenSave = async (entityType, entityId, propertyName) => {
    const res = await fetch(`${API_BASE_URL}/api/tsgen/verify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        entity_type: entityType,
        entity_id: entityId,
        property_name: propertyName
      })
    });
    
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`HTTP ${res.status}: ${errText}`);
    }
    
    return await res.json();
  };

  const executePredicateInduced = async (step) => {
    const cfg = step.config;
    const payload = {
      predicate: cfg.predicate,
      start_time: cfg.start_time,
      end_time: cfg.end_time,
      step_minutes: cfg.step_minutes,
      node_label: cfg.node_label,
      name: cfg.name || cfg.predicate,
    };
    const res = await fetch(`${API_BASE_URL}/api/induced/create`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) { const t = await res.text(); throw new Error(`HTTP ${res.status}: ${t}`); }
    return await res.json();
  };

  const executeTSGenInduced = async (step, stepIndex) => {
    const inputStep = steps.find(s => s.id === step.inputRef);
    if (!inputStep?.result?.induced_id) throw new Error('Run a Predicate-Induced step first');
    const payload = {
      induced_id: inputStep.result.induced_id,
      metrics: step.config.metrics,
    };
    // Add signal aggregation metrics if selected
    if (step.config.signal_metrics?.length > 0) {
      payload.signal_metrics = step.config.signal_metrics;
    }
    const res = await fetch(`${API_BASE_URL}/api/tsgen/induced`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) { const t = await res.text(); throw new Error(`HTTP ${res.status}: ${t}`); }
    return await res.json();
  };

  const executeHyGraphDiff = async (step, stepIndex) => {
    const cfg = step.config;
    const mode = cfg.mode || 'induced';

    if (mode === 'predicate') {
      // Single-step: specify predicate + two date ranges
      const payload = {
        predicate: cfg.predicate || 'ts / free_flow_speed < 0.6',
        period_1_start: cfg.period_1_start,
        period_1_end: cfg.period_1_end,
        period_2_start: cfg.period_2_start,
        period_2_end: cfg.period_2_end,
        period_1_label: cfg.period_1_label || 'Period 1',
        period_2_label: cfg.period_2_label || 'Period 2',
        step_minutes: cfg.step_minutes || 60,
        node_label: cfg.node_label || 'Sensor',
        min_frequency: cfg.min_frequency ?? 0,
        delta_max_days: cfg.delta_max_days ?? 1.0,
      };
      const res = await fetch(`${API_BASE_URL}/api/diff/predicate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) { const t = await res.text(); throw new Error(`HTTP ${res.status}: ${t}`); }
      return await res.json();

    } else if (mode === 'induced') {
      // Same input, two periods
      const inputStep = steps.find(s => s.id === step.inputRef);
      if (!inputStep?.result?.induced_id) {
        throw new Error('Run the input Predicate-Induced step first');
      }
      if (!cfg.period_1_start || !cfg.period_1_end || !cfg.period_2_start || !cfg.period_2_end) {
        throw new Error('Both Period 1 and Period 2 must be defined');
      }
      const payload = {
        induced_id_1: inputStep.result.induced_id,
        induced_id_2: inputStep.result.induced_id,
        ts_property: cfg.ts_property || null,
        delta_max_days: cfg.delta_max_days ?? 0.25,
        period_1_label: `${cfg.period_1_start.slice(0,10)} → ${cfg.period_1_end.slice(0,10)}`,
        period_2_label: `${cfg.period_2_start.slice(0,10)} → ${cfg.period_2_end.slice(0,10)}`,
        min_frequency: cfg.min_frequency ?? 0,
        period_1_start: cfg.period_1_start,
        period_1_end: cfg.period_1_end,
        period_2_start: cfg.period_2_start,
        period_2_end: cfg.period_2_end,
      };
      // Add TSGen graph-level metrics to compare
      if (cfg.tsgen_metrics?.length > 0) {
        payload.tsgen_metrics = cfg.tsgen_metrics;
      }
      if (cfg.tsgen_signal_metrics?.length > 0) {
        payload.tsgen_signal_metrics = cfg.tsgen_signal_metrics;
      }

      const res = await fetch(`${API_BASE_URL}/api/diff/induced`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) { const t = await res.text(); throw new Error(`HTTP ${res.status}: ${t}`); }
      return await res.json();

    } else {
      // Classic snapshot diff
      const snap1 = steps.find(s => s.id === step.inputRef);
      const snap2 = steps.find(s => s.id === step.inputRef2);
      const timestamp1 = snap1?.config?.timestamp || snap1?.result?.timestamp;
      const timestamp2 = snap2?.config?.timestamp || snap2?.result?.timestamp;
      if (!timestamp1 || !timestamp2) throw new Error('Both input snapshots must be executed first');
      const res = await fetch(`${API_BASE_URL}/api/diff/compare`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ timestamp1, timestamp2, mode: 'hybrid' }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    }
  };

  // Format helpers
  const formatDateTime = (isoString) => {
    if (!isoString) return '';
    const d = new Date(isoString);
    return d.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  };

  // Save/Load workflow
  const saveWorkflow = () => {
    const workflow = {
      name: workflowName,
      steps: steps.map(s => ({ ...s, result: null, status: 'pending' })),
      savedAt: new Date().toISOString()
    };
    localStorage.setItem(`workflow_${Date.now()}`, JSON.stringify(workflow));
    alert('Workflow saved!');
  };

  const loadWorkflow = () => {
    const keys = Object.keys(localStorage).filter(k => k.startsWith('workflow_'));
    if (keys.length === 0) {
      alert('No saved workflows found');
      return;
    }
    const key = keys[keys.length - 1]; // Load most recent
    const workflow = JSON.parse(localStorage.getItem(key));
    setWorkflowName(workflow.name);
    setSteps(workflow.steps);
    setSelectedStepId(workflow.steps[0]?.id || null);
  };


  
  const renderStepConfig = () => {
    if (!selectedStep) {
      return (
        <div className="step-config-empty">
          <p>Select a step to configure, or add a new step to begin.</p>
          <div className="add-step-buttons">
            {stepTypes.map(st => (
              <button key={st.type} onClick={() => addStep(st.type)} className="add-step-btn">
                <img className="step-icon" src={st.icon} alt={`${st.type} icon`} />
                <span className="step-label">{st.label}</span>
              </button>
            ))}
          </div>
        </div>
      );
    }

    switch (selectedStep.type) {
      case 'subhygraph':
        return renderSubHyGraphConfig();
      case 'snapshot':
        return renderSnapshotConfig();
      case 'snapshot_sequence':
        return renderSnapshotSequenceConfig();
      case 'tsgen':
        return renderTSGenConfig();
      case 'predicate_induced':
        return renderPredicateInducedConfig();
      case 'tsgen_induced':
        return renderTSGenInducedConfig();
      case 'hygraph_diff':
        return renderHyGraphDiffConfig();
      default:
        return <div>Unknown step type</div>;
    }
  };

  const renderSubHyGraphConfig = () => (
    <div className="step-config-panel">
      <h3> SubHyGraph Selection</h3>
      <p className="step-description">Select a saved subhygraph to use in this workflow. Create subhygraphs using Pattern Matching in the HyGraph tab.</p>
      
      <div className="config-form">
        {/* Saved SubHyGraph Selection */}
        <div className="saved-subhygraph-section">
          <div className="form-group">
            <label>Select Saved SubHyGraph</label>
            <select
              value={selectedStep.config.savedId}
              onChange={e => updateStepConfig(selectedStep.id, { savedId: e.target.value })}
            >
              <option value="">-- Select a SubHyGraph --</option>
              {savedSubHyGraphs.map(sg => (
                <option key={sg.id} value={sg.id}>
                  {sg.name} ({sg.node_count} nodes)
                </option>
              ))}
            </select>
          </div>
          
          {savedSubHyGraphs.length === 0 && (
            <div className="no-saved-message">
              <p>No saved subhygraphs yet.</p>
              <p>Go to <strong>HyGraph tab → Pattern Matching</strong> to create and save subhygraphs.</p>
            </div>
          )}
          
          {selectedStep.config.savedId && (
            <div className="selected-subhygraph-info">
              {(() => {
                const selected = savedSubHyGraphs.find(sg => sg.id === selectedStep.config.savedId);
                return selected ? (
                  <>
                    <div className="info-row">
                      <span className="info-label">Name:</span>
                      <span className="info-value">{selected.name}</span>
                    </div>
                    <div className="info-row">
                      <span className="info-label">Nodes:</span>
                      <span className="info-value">{selected.node_count}</span>
                    </div>
                    {selected.description && (
                      <div className="info-row">
                        <span className="info-label">Description:</span>
                        <span className="info-value">{selected.description}</span>
                      </div>
                    )}
                    {selected.filter_query && (
                      <div className="info-row">
                        <span className="info-label">Filter:</span>
                        <span className="info-value filter-tag">{selected.filter_query}</span>
                      </div>
                    )}
                  </>
                ) : null;
              })()}
            </div>
          )}
        </div>
        
        <button 
          className="run-step-btn" 
          onClick={() => executeStep(selectedStep.id)} 
          disabled={executing || !canExecuteStep(selectedStep)}
        >
          ▶ Load SubHyGraph
        </button>
      </div>
      
      {selectedStep.result && renderSubHyGraphResult()}
      
      {/* SubHyGraph Visualization */}
      {subhygraphData && selectedStep.status === 'success' && (
        <div className="subhygraph-visualization">
          <div className="viz-header">
            <h4>SubHyGraph Preview</h4>
            <select value={vizMode} onChange={e => setVizMode(e.target.value)} className="viz-select">
              <option value="force">Graph</option>
              <option value="map">Map</option>
            </select>
          </div>
          <div className="viz-graph">
            <GraphView
              data={subhygraphData}
              mode={vizMode}
              onElementSelect={setSelectedElement}
              selectedElement={selectedElement}
            />
          </div>
          
          {/* SubHyGraph's Own Properties - Always show if it has temporal properties */}
          {subhygraphData.temporal_properties && Object.keys(subhygraphData.temporal_properties).length > 0 && (
            <div className="subhygraph-properties-section">
              <div className="subhygraph-props-header">
                <h5>📊 SubHyGraph Temporal Properties</h5>
                <span className="props-hint">Properties generated by TSGen for this SubHyGraph</span>
              </div>
              <div className="subhygraph-props-content">
                <div className="props-list">
                  {Object.entries(subhygraphData.temporal_properties).map(([propName, tsId]) => {
                    const tsData = subhygraphData.timeseries?.[tsId];
                    return (
                      <div key={propName} className="subhygraph-prop-item">
                        <span className="prop-name">{propName}</span>
                        <span className="prop-tsid">TS: {tsId.substring(0, 20)}...</span>
                        {tsData && (
                          <span className="prop-points">{tsData.data?.length || 0} points</span>
                        )}
                      </div>
                    );
                  })}
                </div>
                {/* Show timeseries chart for SubHyGraph properties */}
                <div className="subhygraph-timeseries">
                  {Object.entries(subhygraphData.temporal_properties).map(([propName, tsId]) => {
                    const tsData = subhygraphData.timeseries?.[tsId];
                    if (!tsData?.data?.length) return null;
                    return (
                      <div key={propName} className="subhygraph-ts-chart">
                        <div className="ts-chart-header">{propName}</div>
                        <div className="ts-mini-chart" style={{ height: '120px' }}>
                          {/* Mini sparkline chart */}
                          <svg viewBox={`0 0 200 60`} preserveAspectRatio="none" style={{ width: '100%', height: '100%' }}>
                            {(() => {
                              const values = tsData.data.map(d => d.value);
                              const min = Math.min(...values);
                              const max = Math.max(...values);
                              const range = max - min || 1;
                              const points = values.map((v, i) => {
                                const x = (i / (values.length - 1)) * 200;
                                const y = 55 - ((v - min) / range) * 50;
                                return `${x},${y}`;
                              }).join(' ');
                              return (
                                <>
                                  <polyline
                                    fill="none"
                                    stroke="#4CAF50"
                                    strokeWidth="2"
                                    points={points}
                                  />
                                  <text x="5" y="12" fontSize="10" fill="#666">max: {max.toFixed(1)}</text>
                                  <text x="5" y="55" fontSize="10" fill="#666">min: {min.toFixed(1)}</text>
                                </>
                              );
                            })()}
                          </svg>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          )}
          
          {/* Selected Node/Edge Details */}
          {selectedElement && (
            <div className="viz-details">
              <div className="viz-metadata">
                <MetadataPanel element={selectedElement} />
              </div>
              <div className="viz-timeseries">
                <TimeSeriesPanel
                  element={selectedElement}
                  data={subhygraphData}
                />
              </div>
            </div>
          )}
          
          {/* Hint when nothing selected and no SubHyGraph properties */}
          {!selectedElement && (!subhygraphData.temporal_properties || Object.keys(subhygraphData.temporal_properties).length === 0) && (
            <div className="viz-hint">
              <p>Click on a node or edge to view its properties and timeseries.</p>
              <p className="hint-secondary">Use TSGen to generate temporal properties for this SubHyGraph.</p>
            </div>
          )}
        </div>
      )}
    </div>
  );

  const renderSnapshotConfig = () => {
    const stepIndex = steps.findIndex(s => s.id === selectedStep.id);
    const availableInputs = getAvailableInputs(stepIndex, 'hygraph');
    
    return (
      <div className="step-config-panel">
        <h3> Snapshot Configuration</h3>
        <p className="step-description">Create a point-in-time snapshot of the graph.</p>
        
        <div className="config-form">
          <div className="form-group">
            <label>Input</label>
            <select
              value={selectedStep.inputRef}
              onChange={e => updateStepInput(selectedStep.id, e.target.value)}
            >
              {availableInputs.map(inp => (
                <option key={inp.id} value={inp.id}>{inp.name}</option>
              ))}
            </select>
          </div>
          
          <div className="form-group">
            <label>Timestamp</label>
            <input
              type="datetime-local"
              value={selectedStep.config.timestamp.slice(0, 16)}
              onChange={e => updateStepConfig(selectedStep.id, { timestamp: e.target.value + ':00' })}
            />
          </div>
          
          <div className="form-group">
            <label>Mode</label>
            <div className="mode-buttons">
              <button
                className={selectedStep.config.mode === 'graph' ? 'active' : ''}
                onClick={() => updateStepConfig(selectedStep.id, { mode: 'graph' })}
              >
                Graph
              </button>
              <button
                className={selectedStep.config.mode === 'hybrid' ? 'active' : ''}
                onClick={() => updateStepConfig(selectedStep.id, { mode: 'hybrid' })}
              >
                Hybrid
              </button>
            </div>
          </div>
          
          <button 
            className="run-step-btn" 
            onClick={() => executeStep(selectedStep.id)} 
            disabled={executing || !canExecuteStep(selectedStep)}
          >
            ▶ Run Step
          </button>
        </div>
        
        {selectedStep.result && renderSnapshotResult()}
      </div>
    );
  };

  const renderSnapshotSequenceConfig = () => {
    const stepIndex = steps.findIndex(s => s.id === selectedStep.id);
    const availableInputs = getAvailableInputs(stepIndex, 'hygraph');
    const timeError = getTimeValidationError(selectedStep);
    const isValidTime = isValidTimeRange(selectedStep.config.start, selectedStep.config.end);
    
    return (
      <div className="step-config-panel">
        <h3> Snapshot Sequence Configuration</h3>
        <p className="step-description">Create a sequence of snapshots over a time range.</p>
        
        <div className="config-form">
          <div className="form-group">
            <label>Input</label>
            <select
              value={selectedStep.inputRef}
              onChange={e => updateStepInput(selectedStep.id, e.target.value)}
            >
              {availableInputs.map(inp => (
                <option key={inp.id} value={inp.id}>{inp.name}</option>
              ))}
            </select>
          </div>
          
          <div className="form-row">
            <div className="form-group">
              <label>Start Date</label>
              <input
                type="datetime-local"
                value={selectedStep.config.start.slice(0, 16)}
                onChange={e => updateStepConfig(selectedStep.id, { start: e.target.value + ':00' })}
                className={!isValidTime ? 'input-error' : ''}
              />
            </div>
            <div className="form-group">
              <label>End Date</label>
              <input
                type="datetime-local"
                value={selectedStep.config.end.slice(0, 16)}
                onChange={e => updateStepConfig(selectedStep.id, { end: e.target.value + ':00' })}
                className={!isValidTime ? 'input-error' : ''}
              />
            </div>
          </div>
          
          {/* Time validation error message */}
          {timeError && (
            <div className="time-error-message">
              {timeError}
            </div>
          )}
          
          <div className="form-row">
            <div className="form-group">
              <label>Granularity</label>
              <select
                value={selectedStep.config.granularity}
                onChange={e => updateStepConfig(selectedStep.id, { granularity: e.target.value })}
              >
                <option value="1H">Hourly (1H)</option>
                <option value="6H">6 Hours</option>
                <option value="12H">12 Hours</option>
                <option value="1D">Daily (1D)</option>
                <option value="7D">Weekly (7D)</option>
                <option value="1M">Monthly (1M)</option>
              </select>
            </div>
            <div className="form-group">
              <label>Mode</label>
              <select
                value={selectedStep.config.mode}
                onChange={e => updateStepConfig(selectedStep.id, { mode: e.target.value })}
              >
                <option value="graph">Graph</option>
                <option value="hybrid">Hybrid (Graph + Time Series)</option>
              </select>
            </div>
          </div>
          
          <button 
            className="run-step-btn" 
            onClick={() => executeStep(selectedStep.id)} 
            disabled={executing || !canExecuteStep(selectedStep)}
            title={timeError || ''}
          >
            ▶ Run Step
          </button>
        </div>
        
        {selectedStep.result && renderSnapshotSequenceResult()}
      </div>
    );
  };

  const renderTSGenConfig = () => {
    const stepIndex = steps.findIndex(s => s.id === selectedStep.id);
    const availableInputs = getAvailableInputs(stepIndex, 'snapshot_sequence');
    
    // Get input sequence config for query display
    const inputStep = steps.find(s => s.id === selectedStep.inputRef);
    const seqConfig = inputStep?.config || {};
    
    // Check if input sequence has valid time range
    const inputTimeValid = inputStep?.type === 'snapshot_sequence' 
      ? isValidTimeRange(inputStep.config.start, inputStep.config.end)
      : true;
    const timeError = !inputTimeValid ? 'Input sequence has invalid time range (end must be after start)' : null;
    
    const config = selectedStep.config;
    
    // Determine available metrics based on scope and entity type
    const getMetricOptions = () => {
      if (config.scope === 'entity') {
        if (config.entityType === 'nodes') {
          return [{ value: 'degree', label: 'Degree (specific node)' }];
        }
        return [];
      }
      // Global scope
      if (config.entityType === 'nodes') {
        return [
          { value: 'count', label: 'Count' },
          { value: 'degree', label: 'Degree (aggregated)' },
          { value: 'property', label: 'Property (aggregated)' }
        ];
      } else if (config.entityType === 'edges') {
        return [
          { value: 'count', label: 'Count' },
          { value: 'property', label: 'Property (aggregated)' },
          { value: 'between', label: 'Between two nodes' }
        ];
      } else if (config.entityType === 'graph') {
        return [
          { value: 'density', label: 'Density' },
          { value: 'connected_components', label: 'Connected Components' },
          { value: 'biggest_component', label: 'Biggest Component Size' }
        ];
      }
      return [];
    };
    
    // Build generated query string
    const buildQueryString = () => {
      let query = `ts = snapshots.tsgen("${seqConfig.start || '...'}", "${seqConfig.end || '...'}")`;
      
      if (config.scope === 'global') {
        query += `\nresult = ts.global_.${config.entityType}`;
        
        if (config.metric === 'count') {
          query += `.count(${config.label ? `label="${config.label}"` : ''})`;
        } else if (config.metric === 'degree') {
          query += `.degree(${config.label ? `label="${config.label}"` : ''}${config.weight ? `, weight="${config.weight}"` : ''}).${config.aggregation}(direction="${config.direction}")`;
        } else if (config.metric === 'property' && config.propertyName) {
          query += `.property("${config.propertyName}"${config.label ? `, label="${config.label}"` : ''}).${config.aggregation}()`;
        } else if (config.metric === 'between' && config.sourceNode && config.targetNode) {
          query += `.between("${config.sourceNode}", "${config.targetNode}", directed=${config.directed})`;
          if (config.propertyName) {
            query += `.property("${config.propertyName}").${config.aggregation}()`;
          } else {
            query += `.count()`;
          }
        } else if (config.metric === 'density') {
          query += `.density(${config.label ? `label="${config.label}"` : ''})`;
        } else if (config.metric === 'connected_components') {
          query += `.connected_components(${config.label ? `label="${config.label}"` : ''})`;
        } else if (config.metric === 'biggest_component') {
          query += `.biggest_component(${config.label ? `label="${config.label}"` : ''})`;
        }
      } else if (config.scope === 'entity' && config.entityId) {
        query += `\nresult = ts.entities.nodes.degree(node_id="${config.entityId}", direction="${config.direction}")`;
      }
      
      return query;
    };
    
    // Handle save action using TSGen Save endpoint
    const handleSave = async () => {
      if (!config.savePropertyName) {
        alert('Please enter a property name to save as');
        return;
      }
      if (config.saveTargetType !== 'new' && !config.saveTargetId) {
        alert('Please select a target');
        return;
      }
      
      try {
        const result = await saveTSGenResult(selectedStep);
        const savedCount = result.saved_count || 1;
        const errors = result.errors || [];
        
        let message = `✓ TimeSeries saved successfully!\n\n` +
          `Property: ${config.savePropertyName}\n` +
          `Entities: ${savedCount}\n` +
          `Data points: ${result.data_points}`;
        
        if (errors.length > 0) {
          message += `\n\n⚠ ${errors.length} error(s) occurred`;
        }
        
        // Optionally verify the first save
        if (result.saved && result.saved.length > 0 && result.saved[0].entity_id) {
          const firstSave = result.saved[0];
          try {
            const verifyResult = await verifyTSGenSave(
              firstSave.type, 
              firstSave.entity_id, 
              config.savePropertyName
            );
            if (verifyResult.found) {
              message += `\n\n✓ Verified: ${verifyResult.timeseries?.count || 0} data points stored`;
            }
          } catch (verifyErr) {
            console.warn('Verification failed:', verifyErr);
          }
        }
        
        // Notify parent that data changed - will trigger refresh when switching tabs
        if (onDataChanged) {
          onDataChanged();
        }
        
        // If we saved to a subhygraph that's currently loaded, refresh it
        if (config.saveTargetType === 'subhygraph' && config.saveTargetId) {
          // Find the subhygraph step and reload it
          const subhgStep = steps.find(s => s.type === 'subhygraph' && s.config.savedId === config.saveTargetId);
          if (subhgStep && subhgStep.result) {

            try {
              const res = await fetch(`${API_BASE_URL}/api/subhygraph/${config.saveTargetId}`);
              if (res.ok) {
                const data = await res.json();
                await loadTimeseriesForSubhygraph(data);
                setSubhygraphData(data);
                // Update the step result too
                setSteps(prev => prev.map(s => 
                  s.id === subhgStep.id ? { ...s, result: data } : s
                ));
              }
            } catch (refreshErr) {
              console.warn('Failed to refresh subhygraph:', refreshErr);
            }
          }
        }
        
        alert(message);
      } catch (err) {
        alert(`❌ Save failed: ${err.message}`);
      }
    };
    
    return (
      <div className="step-config-panel tsgen-panel">
        <div className="tsgen-header">
          <h3>TSGen - Time Series Generator</h3>
          <div className="tsgen-tabs">
            <button className="active">Visual Builder</button>
            <button disabled>Code View</button>
          </div>
        </div>
        
        <div className="tsgen-body">
          <div className="tsgen-builder">
            {/* Input Selection */}
            <div className="form-group">
              <label>Input (SnapshotSequence)</label>
              <select
                value={selectedStep.inputRef}
                onChange={e => updateStepInput(selectedStep.id, e.target.value)}
                className={timeError ? 'input-error' : ''}
              >
                <option value="">Select a SnapshotSequence...</option>
                {availableInputs.map(inp => (
                  <option key={inp.id} value={inp.id}>{inp.name}</option>
                ))}
              </select>
            </div>
            
            {/* Time validation error */}
            {timeError && (
              <div className="time-error-message">
                 {timeError}
              </div>
            )}
            
            {/* ① Scope */}
            <div className="builder-section">
              <label>① Scope</label>
              <div className="scope-buttons">
                <button
                  className={config.scope === 'global' ? 'active' : ''}
                  onClick={() => updateStepConfig(selectedStep.id, { scope: 'global', metric: 'count' })}
                >
                  <img src='images/global.png' className="btn-icon"/>
                  Global
                </button>
                <button
                  className={config.scope === 'entity' ? 'active' : ''}
                  onClick={() => updateStepConfig(selectedStep.id, { scope: 'entity', entityType: 'nodes', metric: 'degree' })}
                >
                  <img className="btn-icon" src='images/user.png'/>
                  Per-Entity
                </button>
              </div>
            </div>
            
            {/* ② Entity Type - only for global scope */}
            {config.scope === 'global' && (
              <div className="builder-section">
                <label>② Entity Type</label>
                <div className="entity-buttons">
                  <button
                    className={config.entityType === 'nodes' ? 'active' : ''}
                    onClick={() => updateStepConfig(selectedStep.id, { entityType: 'nodes', metric: 'count' })}
                  >
                    <span className="btn-icon">●</span>
                    Nodes
                  </button>
                  <button
                    className={config.entityType === 'edges' ? 'active' : ''}
                    onClick={() => updateStepConfig(selectedStep.id, { entityType: 'edges', metric: 'count' })}
                  >
                    <span className="btn-icon">↔</span>
                    Edges
                  </button>
                  <button
                    className={config.entityType === 'graph' ? 'active' : ''}
                    onClick={() => updateStepConfig(selectedStep.id, { entityType: 'graph', metric: 'density' })}
                  >
                    <img className="btn-icon" src='images/knowledge-graph.png'/>
                    Graph
                  </button>
                </div>
              </div>
            )}
            
            {/* ③ Metric */}
            <div className="builder-section">
              <label>③ Metric</label>
              <select
                value={config.metric}
                onChange={e => updateStepConfig(selectedStep.id, { metric: e.target.value })}
                className="metric-select"
              >
                {getMetricOptions().map(opt => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </select>
            </div>
            
            {/* Property Name - for property metric */}
            {(config.metric === 'property' || (config.metric === 'between' && config.propertyName !== undefined)) && (
              <div className="builder-section">
                <label>Property Name</label>
                <input
                  type="text"
                  value={config.propertyName}
                  onChange={e => updateStepConfig(selectedStep.id, { propertyName: e.target.value })}
                  placeholder="e.g., capacity, duration, num_bikes"
                />
              </div>
            )}
            
            {/* Source/Target Nodes - for between metric */}
            {config.metric === 'between' && (
              <>
                <div className="builder-section">
                  <label>Source Node ID</label>
                  <input
                    type="text"
                    value={config.sourceNode}
                    onChange={e => updateStepConfig(selectedStep.id, { sourceNode: e.target.value })}
                    placeholder="e.g., station_1"
                  />
                </div>
                <div className="builder-section">
                  <label>Target Node ID</label>
                  <input
                    type="text"
                    value={config.targetNode}
                    onChange={e => updateStepConfig(selectedStep.id, { targetNode: e.target.value })}
                    placeholder="e.g., station_2"
                  />
                </div>
                <div className="builder-section">
                  <label>
                    <input
                      type="checkbox"
                      checked={config.directed}
                      onChange={e => updateStepConfig(selectedStep.id, { directed: e.target.checked })}
                    />
                    {' '}Directed edges only
                  </label>
                </div>
                <div className="builder-section">
                  <label>Property to aggregate (optional)</label>
                  <input
                    type="text"
                    value={config.propertyName}
                    onChange={e => updateStepConfig(selectedStep.id, { propertyName: e.target.value })}
                    placeholder="Leave empty for count, or e.g., duration"
                  />
                </div>
              </>
            )}
            
            {/* Entity ID - for entity scope */}
            {config.scope === 'entity' && (
              <div className="builder-section">
                <label>Node ID</label>
                <input
                  type="text"
                  value={config.entityId}
                  onChange={e => updateStepConfig(selectedStep.id, { entityId: e.target.value })}
                  placeholder="e.g., station_123"
                />
              </div>
            )}
            
            {/* Direction - for degree metric */}
            {config.metric === 'degree' && (
              <div className="builder-section">
                <label>Direction</label>
                <div className="agg-buttons">
                  {['in', 'out', 'both'].map(dir => (
                    <button
                      key={dir}
                      className={config.direction === dir ? 'active' : ''}
                      onClick={() => updateStepConfig(selectedStep.id, { direction: dir })}
                    >
                      {dir.toUpperCase()}
                    </button>
                  ))}
                </div>
              </div>
            )}
            
            {/* Weight - for degree metric */}
            {config.metric === 'degree' && config.scope === 'global' && (
              <div className="builder-section">
                <label>Weight Property (optional)</label>
                <input
                  type="text"
                  value={config.weight}
                  onChange={e => updateStepConfig(selectedStep.id, { weight: e.target.value })}
                  placeholder="Leave empty for unweighted, or e.g., duration"
                />
              </div>
            )}
            
            {/* ④ Aggregation - for degree and property metrics */}
            {(config.metric === 'degree' || config.metric === 'property' || (config.metric === 'between' && config.propertyName)) && config.scope === 'global' && (
              <div className="builder-section">
                <label>④ Aggregation</label>
                <div className="agg-buttons">
                  {['avg', 'sum', 'min', 'max'].map(agg => (
                    <button
                      key={agg}
                      className={config.aggregation === agg ? 'active' : ''}
                      onClick={() => updateStepConfig(selectedStep.id, { aggregation: agg })}
                    >
                      {agg.toUpperCase()}
                    </button>
                  ))}
                </div>
              </div>
            )}
            
            {/* ⑤ Label Filter */}
            <div className="builder-section">
              <label> Label Filter (Optional)</label>
              <input
                type="text"
                value={config.label}
                onChange={e => updateStepConfig(selectedStep.id, { label: e.target.value })}
                placeholder="e.g., Station, Trip"
              />
            </div>
            
            {/* Generated Query Display */}
            <div className="generated-query">
              <div className="query-header">
                <span>Generated Query</span>
                <button className="copy-btn" onClick={() => navigator.clipboard.writeText(buildQueryString())}>📋 Copy</button>
              </div>
              <code>
                {buildQueryString().split('\n').map((line, i) => (
                  <React.Fragment key={i}>{line}<br/></React.Fragment>
                ))}
              </code>
            </div>
            
            <button 
              className="run-step-btn" 
              onClick={() => executeStep(selectedStep.id)} 
              disabled={executing || !canExecuteStep(selectedStep) || !!timeError}
              title={timeError || ''}
            >
               Generate Time Series
            </button>
          </div>
        </div>
        
        {/* Results */}
        {selectedStep.result && renderTSGenResult()}
        
        {/* Save Section - only show if we have results */}
        {selectedStep.result && (
          <div className="tsgen-save-section">
            <h4> Save as Property</h4>
            <p className="save-description">Save this generated timeseries as a temporal property</p>
            
            <div className="save-form">
              <div className="form-group">
                <label>Save To</label>
                <div className="mode-buttons">
                  <button
                    className={config.saveTargetType === 'node' ? 'active' : ''}
                    onClick={() => updateStepConfig(selectedStep.id, { saveTargetType: 'node' })}
                  >
                    Node
                  </button>
                  <button
                    className={config.saveTargetType === 'edge' ? 'active' : ''}
                    onClick={() => updateStepConfig(selectedStep.id, { saveTargetType: 'edge' })}
                  >
                    Edge
                  </button>
                  <button
                    className={config.saveTargetType === 'subhygraph' ? 'active' : ''}
                    onClick={() => updateStepConfig(selectedStep.id, { saveTargetType: 'subhygraph' })}
                  >
                    SubHyGraph
                  </button>
                  <button
                    className={config.saveTargetType === 'new' ? 'active' : ''}
                    onClick={() => updateStepConfig(selectedStep.id, { saveTargetType: 'new' })}
                  >
                    Standalone
                  </button>
                </div>
              </div>
              
              {/* SubHyGraph target fields */}
              {config.saveTargetType === 'subhygraph' && (
                <>
                  <div className="form-group">
                    <label>SubHyGraph</label>
                    <select
                      value={config.saveTargetId}
                      onChange={e => updateStepConfig(selectedStep.id, { saveTargetId: e.target.value })}
                    >
                      <option value="">-- Select SubHyGraph --</option>
                      {savedSubHyGraphs.map(sg => (
                        <option key={sg.id} value={sg.id}>
                          {sg.name} ({sg.node_count} nodes)
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="form-group">
                    <label>Filter by Label (optional)</label>
                    <input
                      type="text"
                      value={config.saveTargetLabel}
                      onChange={e => updateStepConfig(selectedStep.id, { saveTargetLabel: e.target.value })}
                      placeholder="e.g., Station (leave empty for all nodes)"
                    />
                  </div>
                </>
              )}
              
              {/* Node/Edge target field */}
              {(config.saveTargetType === 'node' || config.saveTargetType === 'edge') && (
                <div className="form-group">
                  <label>Entity UID</label>
                  <input
                    type="text"
                    value={config.saveTargetId}
                    onChange={e => updateStepConfig(selectedStep.id, { saveTargetId: e.target.value })}
                    placeholder="e.g., station_123"
                  />
                </div>
              )}
              
              <div className="form-group">
                <label>Variable Name (Property)</label>
                <input
                  type="text"
                  value={config.savePropertyName}
                  onChange={e => updateStepConfig(selectedStep.id, { savePropertyName: e.target.value })}
                  placeholder="e.g., degree_evolution, flow_ts"
                />
              </div>
              
              <button 
                className="save-btn" 
                onClick={handleSave}
                disabled={
                  !config.savePropertyName || 
                  (config.saveTargetType !== 'new' && !config.saveTargetId)
                }
              >
                Save TimeSeries
              </button>
            </div>
          </div>
        )}
      </div>
    );
  };

  // Load static properties when a predicate_induced step is selected
  useEffect(() => {
    if (selectedStep?.type === 'predicate_induced' && selectedStep.config?.node_label) {
      loadPropsForLabel(selectedStep.config.node_label);
    }
  }, [selectedStepId]);

  // Build compound predicate string from rows
  const buildPredicateStr = (rows, connectors) => {
    return rows.map((r, i) => {
      const expr = (r.math_op && r.math_op !== 'none')
        ? `${r.ts} ${r.math_op} ${r.rhs_type === 'property' ? r.rhs_prop : r.rhs_num}`
        : r.ts;
      const part = `${expr} ${r.comp_op || '<'} ${r.threshold || '0'}`;
      return i > 0 ? `${connectors[i-1] || 'AND'} ${part}` : part;
    }).join(' ');
  };

  const updatePredRow = (rowIdx, field, value) => {
    const rows = [...(selectedStep.config.pred_rows || [])];
    rows[rowIdx] = { ...rows[rowIdx], [field]: value };
    const connectors = selectedStep.config.pred_connectors || [];
    updateStepConfig(selectedStep.id, { pred_rows: rows, predicate: buildPredicateStr(rows, connectors) });
  };

  const renderPredicateInducedConfig = () => {
    const cfg = selectedStep.config;
    const rows = cfg.pred_rows || [{ ts: tsVariables[0], math_op: 'none', rhs_type: 'property', rhs_prop: '', rhs_num: '', comp_op: '<', threshold: '0.6' }];
    const connectors = cfg.pred_connectors || [];
    const timeOk = isValidTimeRange(cfg.start_time, cfg.end_time);
    return (
      <div className="step-config-panel">
        <h3>SubHyGraph — Predicate-Induced</h3>
       
        <div className="config-form">
          <div className="form-group">
            <label>Node Label</label>
            <input type="text" value={cfg.node_label}
              onChange={e => {
                updateStepConfig(selectedStep.id, { node_label: e.target.value });
                loadPropsForLabel(e.target.value);
              }}
              onBlur={() => loadPropsForLabel(cfg.node_label)}
              placeholder="Sensor" />
          </div>
          <div className="form-group">
            <label>Predicate {rows.length > 1 && <span style={{color:'#94a3b8',fontWeight:400}}>({rows.length} conditions)</span>}</label>
            {rows.map((r, idx) => (
              <div key={idx}>
                {idx > 0 && (
                  <div style={{ display: 'flex', justifyContent: 'center', margin: '3px 0' }}>
                    <select value={connectors[idx-1] || 'AND'} style={{ width: 65, textAlign: 'center', fontWeight: 700, fontSize: '0.72rem', border: '1px solid #d1d5db', borderRadius: 4, padding: '2px 0' }}
                      onChange={e => {
                        const c = [...connectors]; c[idx-1] = e.target.value;
                        updateStepConfig(selectedStep.id, { pred_connectors: c, predicate: buildPredicateStr(rows, c) });
                      }}>
                      <option value="AND">AND</option>
                      <option value="OR">OR</option>
                    </select>
                  </div>
                )}
                <div style={{ display: 'flex', gap: 3, alignItems: 'center', alignItems: 'center', justifyContent:'space-between', flexDirection:'row' }}>
                  <select value={r.ts || tsVariables[0]}
                    onChange={e => updatePredRow(idx, 'ts', e.target.value)}>
                    {tsVariables.map(v => <option key={v} value={v}>{v}</option>)}
                  </select>
                  <select value={r.math_op || 'none'} 
                    onChange={e => updatePredRow(idx, 'math_op', e.target.value)}>
                    <option value="none">—</option>
                    <option value="/">÷</option><option value="*">×</option>
                    <option value="-">−</option><option value="+">+</option>
                  </select>
                  {(r.math_op || 'none') !== 'none' && (
                    <>
                      <select value={r.rhs_type || 'property'} 
                        onChange={e => updatePredRow(idx, 'rhs_type', e.target.value)}>
                        <option value="property">prop</option>
                        <option value="number">num</option>
                      </select>
                      {(r.rhs_type || 'property') === 'property' ? (
                        <select value={r.rhs_prop || labelStaticProps[0] || ''}
                          onChange={e => updatePredRow(idx, 'rhs_prop', e.target.value)}>
                          {labelStaticProps.length === 0 && !r.rhs_prop && <option value="">set label...</option>}
                          {r.rhs_prop && !labelStaticProps.includes(r.rhs_prop) && <option value={r.rhs_prop}>{r.rhs_prop}</option>}
                          {labelStaticProps.map(p => <option key={p} value={p}>{p}</option>)}
                        </select>
                      ) : (
                        <input type="number" value={r.rhs_num || ''}  placeholder="1"
                          onChange={e => updatePredRow(idx, 'rhs_num', e.target.value)} />
                      )}
                    </>
                  )}
                  <select value={r.comp_op || '<'} 
                    onChange={e => updatePredRow(idx, 'comp_op', e.target.value)}>
                    <option value="<">&lt;</option><option value="<=">&le;</option>
                    <option value=">">&gt;</option><option value=">=">&ge;</option>
                    <option value="==">=</option><option value="!=">&ne;</option>
                  </select>
                  <input type="number" value={r.threshold || ''} step="any"  placeholder="0.6"
                    onChange={e => updatePredRow(idx, 'threshold', e.target.value)} />
                  {rows.length > 1 && (
                    <button style={{ padding: '1px 5px', border: '1px solid #fca5a5', borderRadius: 3, background: '#fff', color: '#dc2626', cursor: 'pointer', fontSize: '0.7rem', lineHeight: 1.4 }}
                      onClick={() => {
                        const newRows = rows.filter((_, i) => i !== idx);
                        const newConn = connectors.filter((_, i) => i !== (idx > 0 ? idx-1 : 0));
                        updateStepConfig(selectedStep.id, { pred_rows: newRows, pred_connectors: newConn, predicate: buildPredicateStr(newRows, newConn) });
                      }}>×</button>
                  )}
                </div>
              </div>
            ))}
            <button style={{ marginTop: 5, padding: '2px 9px', border: '1px dashed #93c5fd', borderRadius: 4, background: '#f0f7ff', color: '#2563eb', cursor: 'pointer', fontSize: '0.72rem' }}
              onClick={() => {
                const newRows = [...rows, { ts: tsVariables[0], math_op: 'none', rhs_type: 'property', rhs_prop: '', rhs_num: '', comp_op: '<', threshold: '' }];
                const newConn = [...connectors, 'AND'];
                updateStepConfig(selectedStep.id, { pred_rows: newRows, pred_connectors: newConn });
              }}>+ Add condition</button>
            <div style={{ fontSize: '0.7rem', color: '#94a3b8', marginTop: 3 }}>
              <code style={{ background: '#f0f4f8', padding: '1px 5px', borderRadius: 3, wordBreak: 'break-all' }}>{cfg.predicate}</code>
            </div>
          </div>
          <div className="form-row">
            <div className="form-group">
              <label>Start Time</label>
              <input type="datetime-local" value={cfg.start_time?.slice(0, 16)}
                onChange={e => updateStepConfig(selectedStep.id, { start_time: e.target.value + ':00' })}
                className={!timeOk ? 'input-error' : ''} />
            </div>
            <div className="form-group">
              <label>End Time</label>
              <input type="datetime-local" value={cfg.end_time?.slice(0, 16)}
                onChange={e => updateStepConfig(selectedStep.id, { end_time: e.target.value + ':00' })}
                className={!timeOk ? 'input-error' : ''} />
            </div>
          </div>
          {!timeOk && <div className="time-error-message">End time must be after start time</div>}
          <div className="form-group">
            <label>Step (minutes)</label>
            <select value={cfg.step_minutes}
              onChange={e => updateStepConfig(selectedStep.id, { step_minutes: parseInt(e.target.value) })}>
              <option value={5}>5 min</option>
              <option value={15}>15 min</option>
              <option value={30}>30 min</option>
              <option value={60}>1 hour</option>
              <option value={360}>6 hours</option>
            </select>
          </div>

          <div className="form-group">
            <label>Name (optional)</label>
            <input type="text" value={cfg.name}
              onChange={e => updateStepConfig(selectedStep.id, { name: e.target.value })}
              placeholder="e.g. March congestion" />
          </div>
          <button className="run-step-btn"
            onClick={() => executeStep(selectedStep.id)}
            disabled={executing || !canExecuteStep(selectedStep)}>
            Create SubHyGraph
          </button>
        </div>
        {selectedStep.result && (
          <div className="step-result">
            <h4>Summary</h4>
            <div className="result-summary">
              <div className="stat-card">
                <span className="stat-value">{selectedStep.result.summary?.unique_members ?? '—'}</span>
                <span className="stat-label">Unique Members</span>
              </div>
              <div className="stat-card">
                <span className="stat-value">{selectedStep.result.summary?.total_timesteps ?? '—'}</span>
                <span className="stat-label">Timesteps</span>
              </div>
              <div className="stat-card">
                <span className="stat-value">{selectedStep.result.summary?.max_simultaneous_members ?? '—'}</span>
                <span className="stat-label">Max Simultaneous</span>
              </div>
              <div className="stat-card">
                <span className="stat-value">{selectedStep.result.summary?.total_lifecycle_events ?? '—'}</span>
                <span className="stat-label">Lifecycle Events</span>
              </div>
            </div>
            <InducedExplorer
              inducedId={selectedStep.result.induced_id}
              stepConfig={selectedStep.config}
              summary={selectedStep.result.summary}
            />
          </div>
        )}
      </div>
    );
  };

  const renderTSGenInducedConfig = () => {
    const stepIndex = steps.findIndex(s => s.id === selectedStep.id);
    const inducedInputs = getAvailableInputs(stepIndex, 'induced');
    const cfg = selectedStep.config;
    const metricOptions = ['density', 'component_count', 'largest_component', 'avg_degree', 'member_count'];
    const toggleMetric = (m) => {
      const cur = cfg.metrics || [];
      const next = cur.includes(m) ? cur.filter(x => x !== m) : [...cur, m];
      updateStepConfig(selectedStep.id, { metrics: next });
    };
    // Signal aggregation — dynamic dropdowns
    const addSignalMetric = () => {
      const prop = cfg._signal_prop || 'speed';
      const agg = cfg._signal_agg || 'mean';
      const scope = cfg._signal_scope || 'all';
      const name = `${agg}_${prop}${scope === 'largest_component' ? '_largest' : ''}`;
      const cur = cfg.signal_metrics || [];
      if (cur.find(s => s.name === name)) return; // already added
      updateStepConfig(selectedStep.id, {
        signal_metrics: [...cur, { name, property_name: prop, aggregation: agg, scope }],
      });
    };
    const removeSignalMetric = (name) => {
      const cur = cfg.signal_metrics || [];
      updateStepConfig(selectedStep.id, { signal_metrics: cur.filter(s => s.name !== name) });
    };
    return (
      <div className="step-config-panel">
        <h3>TSGen — Graph-Level Signals (Induced)</h3>

        <div className="config-form">
          <div className="form-group">
            <label>Input (Predicate-Induced SubHyGraph)</label>
            <select value={selectedStep.inputRef}
              onChange={e => updateStepInput(selectedStep.id, e.target.value)}>
              <option value="">Select induced subgraph...</option>
              {inducedInputs.map(inp => (
                <option key={inp.id} value={inp.id}>{inp.name}</option>
              ))}
            </select>
          </div>
          <div className="form-group">
            <label>Structural Metrics</label>
            <div className="mode-buttons" style={{ flexWrap: 'wrap', gap: 6 }}>
              {metricOptions.map(m => (
                <button key={m}
                  className={(cfg.metrics || []).includes(m) ? 'active' : ''}
                  onClick={() => toggleMetric(m)}>
                  {m}
                </button>
              ))}
            </div>
          </div>
          <div className="form-group">
            <label>Signal Aggregations</label>
            <div className="form-row" style={{ gap: 6 }}>
              <select value={cfg._signal_prop || tsVariables[0]}
                onChange={e => updateStepConfig(selectedStep.id, { _signal_prop: e.target.value })}>
                {tsVariables.map(v => <option key={v} value={v}>{v}</option>)}
              </select>
              <select value={cfg._signal_agg || 'mean'}
                onChange={e => updateStepConfig(selectedStep.id, { _signal_agg: e.target.value })}>
                <option value="mean">mean</option>
                <option value="min">min</option>
                <option value="max">max</option>
                <option value="std">std</option>
                <option value="median">median</option>
              </select>
              <select value={cfg._signal_scope || 'all'}
                onChange={e => updateStepConfig(selectedStep.id, { _signal_scope: e.target.value })}>
                <option value="all">all members</option>
                <option value="largest_component">largest cluster</option>
              </select>
              <button className="run-step-btn" style={{ padding: '6px 14px', fontSize: '0.85rem' }}
                onClick={addSignalMetric}>+ Add</button>
            </div>
            {(cfg.signal_metrics || []).length > 0 && (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 6 }}>
                {(cfg.signal_metrics || []).map(sm => (
                  <span key={sm.name} className="mode-buttons">
                    <button className="active" onClick={() => removeSignalMetric(sm.name)}>
                      {sm.name} ✕
                    </button>
                  </span>
                ))}
              </div>
            )}

          </div>
          <button className="run-step-btn"
            onClick={() => executeStep(selectedStep.id)}
            disabled={executing || !canExecuteStep(selectedStep)}>
            ▶ Compute Graph Signals
          </button>
        </div>
        {selectedStep.result?.metrics && (
          <div className="step-result tsgen-result">
            {Object.entries(selectedStep.result.metrics).map(([name, ts]) => (
              <div key={name} className="tsgen-chart">
                <h4>{name} ({ts.length} points)</h4>
                <TSGenChart data={{ timestamps: ts.timestamps, data: ts.values, name }} />
              </div>
            ))}
          </div>
        )}
      </div>
    );
  };

  const renderHyGraphDiffConfig = () => {
    const stepIndex = steps.findIndex(s => s.id === selectedStep.id);
    const cfg = selectedStep.config;
    const inducedInputs = getAvailableInputs(stepIndex, 'induced');

    return (
      <div className="step-config-panel">
        <h3>HyGraphDiff</h3>

        <div className="config-form">

          {/* ── HyGraph Instances ── */}
          {(() => {
            // Collect ALL available HyGraph instances: subhygraphs, induced, and full hygraph
            const allInputs = [];
            // Add full HyGraph as an option
            allInputs.push({ id: '__full_hygraph__', name: 'Full HyGraph (all nodes)' });
            // Add saved SubHyGraphs
            savedSubHyGraphs.forEach(sg => {
              allInputs.push({ id: `subhygraph_${sg.name}`, name: `SubHyGraph: ${sg.name}`, subhygraphName: sg.name });
            });
            // Add predicate-induced steps from the pipeline
            inducedInputs.forEach(inp => {
              allInputs.push({ id: inp.id, name: `Induced: ${inp.name}` });
            });
            return (
            <>
              <div className="form-group">
                <label>Input (HyGraph instance)</label>
                <select value={selectedStep.inputRef || ''}
                  onChange={e => updateStepInput(selectedStep.id, e.target.value, 'inputRef')}>
                  <option value="">Select HyGraph instance...</option>
                  {allInputs.map(inp => <option key={inp.id} value={inp.id}>{inp.name}</option>)}
                </select>
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Period 1 Start</label>
                  <input type="datetime-local" value={(cfg.period_1_start || '').slice(0,16)}
                    onChange={e => updateStepConfig(selectedStep.id, { period_1_start: e.target.value })} />
                </div>
                <div className="form-group">
                  <label>Period 1 End</label>
                  <input type="datetime-local" value={(cfg.period_1_end || '').slice(0,16)}
                    onChange={e => updateStepConfig(selectedStep.id, { period_1_end: e.target.value })} />
                </div>
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Period 2 Start</label>
                  <input type="datetime-local" value={(cfg.period_2_start || '').slice(0,16)}
                    onChange={e => updateStepConfig(selectedStep.id, { period_2_start: e.target.value })} />
                </div>
                <div className="form-group">
                  <label>Period 2 End</label>
                  <input type="datetime-local" value={(cfg.period_2_end || '').slice(0,16)}
                    onChange={e => updateStepConfig(selectedStep.id, { period_2_end: e.target.value })} />
                </div>
              </div>

              <div className="form-group">
                <label>Graph-Level Metrics to Compare (via TSGen)</label>
                <div className="mode-buttons" style={{ flexWrap: 'wrap', gap: 6 }}>
                  {['density', 'component_count', 'largest_component', 'avg_degree'].map(m => (
                    <button key={m}
                      className={(cfg.tsgen_metrics || []).includes(m) ? 'active' : ''}
                      onClick={() => {
                        const cur = cfg.tsgen_metrics || [];
                        const next = cur.includes(m) ? cur.filter(x => x !== m) : [...cur, m];
                        updateStepConfig(selectedStep.id, { tsgen_metrics: next });
                      }}>{m}</button>
                  ))}
                </div>
                <div className="form-row" style={{ gap: 6, marginTop: 4 }}>
                  <select value={cfg._diff_sig_prop || tsVariables[0]}
                    onChange={e => updateStepConfig(selectedStep.id, { _diff_sig_prop: e.target.value })}>
                    {tsVariables.map(v => <option key={v} value={v}>{v}</option>)}
                  </select>
                  <select value={cfg._diff_sig_agg || 'mean'}
                    onChange={e => updateStepConfig(selectedStep.id, { _diff_sig_agg: e.target.value })}>
                    <option value="mean">mean</option>
                    <option value="min">min</option>
                    <option value="max">max</option>
                    <option value="std">std</option>
                  </select>
                  <button className="run-step-btn" style={{ padding: '6px 14px', fontSize: '0.85rem' }}
                    onClick={() => {
                      const prop = cfg._diff_sig_prop || 'speed';
                      const agg = cfg._diff_sig_agg || 'mean';
                      const name = `${agg}_${prop}`;
                      const cur = cfg.tsgen_signal_metrics || [];
                      if (cur.find(s => s.name === name)) return;
                      updateStepConfig(selectedStep.id, {
                        tsgen_signal_metrics: [...cur, { name, property_name: prop, aggregation: agg, scope: 'all' }],
                      });
                    }}>+ Add</button>
                </div>
                {(cfg.tsgen_signal_metrics || []).length > 0 && (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 4 }}>
                    {(cfg.tsgen_signal_metrics || []).map(sm => (
                      <span key={sm.name} className="mode-buttons">
                        <button className="active" onClick={() => {
                          const cur = cfg.tsgen_signal_metrics || [];
                          updateStepConfig(selectedStep.id, { tsgen_signal_metrics: cur.filter(s => s.name !== sm.name) });
                        }}>{sm.name} ✕</button>
                      </span>
                    ))}
                  </div>
                )}

              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Max Delay (days)</label>
                  <input type="number" value={cfg.delta_max_days ?? 0.25} min={0.04} max={90} step={0.04}
                    onChange={e => updateStepConfig(selectedStep.id, { delta_max_days: parseFloat(e.target.value) })} />
                  <small className="mode-hint">0.04=1h · 0.25=6h · 1=1day · 30=1month</small>
                </div>
                <div className="form-group">
                  <label>Min Frequency</label>
                  <input type="number" value={cfg.min_frequency ?? 0} min={0} max={1} step={0.05}
                    onChange={e => updateStepConfig(selectedStep.id, { min_frequency: parseFloat(e.target.value) })} />
                  <small className="mode-hint">0=any · 0.3=30% of steps · 0.6=majority</small>
                </div>
              </div>
            </>
            );
          })()}

          <button className="run-step-btn"
            onClick={() => executeStep(selectedStep.id)}
            disabled={executing || !canExecuteStep(selectedStep)}>
            ▶ Compute HyGraphDiff
          </button>
        </div>
        {selectedStep.result && renderHyGraphDiffResult()}
      </div>
    );
  };



  const renderSubHyGraphResult = () => {
    const result = selectedStep.result;
    const missingNodes = result.metadata?.missing_nodes || 0;
    
    return (
      <div className="step-result">
        <div className="result-summary">
          <div className="stat-card">
            <span className="stat-value">{result.nodes?.length || 0}</span>
            <span className="stat-label">Nodes</span>
          </div>
          <div className="stat-card">
            <span className="stat-value">{result.edges?.length || 0}</span>
            <span className="stat-label">Edges</span>
          </div>
        </div>
        {missingNodes > 0 && (
          <div className="warning-message">
            Note: {missingNodes} node(s) from the original saved SubHyGraph could not be found (may have been deleted).
          </div>
        )}
      </div>
    );
  };

  const renderSnapshotResult = () => {
    const result = selectedStep.result;
    return (
      <div className="step-result">
        <div className="result-summary">
          <div className="stat-card">
            <span className="stat-value">{result.nodes?.length || 0}</span>
            <span className="stat-label">Nodes</span>
          </div>
          <div className="stat-card">
            <span className="stat-value">{result.edges?.length || 0}</span>
            <span className="stat-label">Edges</span>
          </div>
          <div className="stat-card">
            <span className="stat-value">{selectedStep.config.mode}</span>
            <span className="stat-label">Mode</span>
          </div>
        </div>
      </div>
    );
  };

  const renderSnapshotSequenceResult = () => {
    const result = selectedStep.result;
    const summary = result.summary || {};
    const timestamps = summary.timestamps || [];
    const nodeCounts = summary.node_counts || [];
    const edgeCounts = summary.edge_counts || [];
    const densities = summary.densities || [];
    
    // Calculate averages
    const avgNodes = nodeCounts.length > 0 
      ? Math.round(nodeCounts.reduce((a, b) => a + b, 0) / nodeCounts.length) 
      : 0;
    const avgEdges = edgeCounts.length > 0 
      ? Math.round(edgeCounts.reduce((a, b) => a + b, 0) / edgeCounts.length) 
      : 0;
    
    return (
      <div className="step-result">
        <div className="result-summary">
          <div className="stat-card">
            <span className="stat-value">{result.snapshot_count || timestamps.length}</span>
            <span className="stat-label">Snapshots</span>
          </div>
          <div className="stat-card">
            <span className="stat-value">{avgNodes}</span>
            <span className="stat-label">Avg Nodes</span>
          </div>
          <div className="stat-card">
            <span className="stat-value">{avgEdges}</span>
            <span className="stat-label">Avg Edges</span>
          </div>
        </div>
        <div className="sequence-timeline">
          {timestamps.slice(0, 10).map((ts, i) => (
            <div key={i} className="timeline-item">
              <span className="timeline-index">{i + 1}</span>
              <span className="timeline-time">{formatDateTime(ts)}</span>
              <span className="timeline-count">{nodeCounts[i] || 0} nodes, {edgeCounts[i] || 0} edges</span>
            </div>
          ))}
          {timestamps.length > 10 && (
            <div className="timeline-more">... and {timestamps.length - 10} more</div>
          )}
        </div>
      </div>
    );
  };

  const renderTSGenResult = () => {
    const ts = selectedStep.result?.timeseries;
    if (!ts) return null;
    
    // Calculate stats
    const values = ts.data?.map(d => Array.isArray(d) ? d[0] : d) || [];
    const avg = values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : 0;
    const max = Math.max(...values);
    const min = Math.min(...values);
    const trend = values.length > 1 ? (values[values.length - 1] > values[0] ? 'Growing' : 'Declining') : 'Stable';
    
    return (
      <div className="step-result tsgen-result">
        {/* Stats Row */}
        <div className="tsgen-stats">
          <div className="stat-card">
            <span className="stat-value">{avg.toFixed(0)}</span>
            <span className="stat-label">AVG</span>
          </div>
          <div className="stat-card highlight-max">
            <span className="stat-value">{max}</span>
            <span className="stat-label">MAX</span>
          </div>
          <div className="stat-card highlight-min">
            <span className="stat-value">{min}</span>
            <span className="stat-label">MIN</span>
          </div>
          <div className="stat-card">
            <span className="stat-value trend">{trend === 'Growing' ? '' : ''}</span>
            <span className="stat-label">{trend}</span>
          </div>
        </div>
        
        {/* Main Chart */}
        <div className="tsgen-chart">
          <h4>{ts.name || 'Generated Time Series'}</h4>
          <TSGenChart data={ts} />
        </div>
        
        {/* Data Table */}
        <div className="tsgen-table">
          <table>
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {ts.timestamps?.slice(0, 10).map((t, i) => (
                <tr key={i}>
                  <td>{formatDateTime(t)}</td>
                  <td>{typeof (Array.isArray(ts.data[i]) ? ts.data[i][0] : ts.data[i]) === 'number' 
                    ? (Array.isArray(ts.data[i]) ? ts.data[i][0] : ts.data[i]).toFixed(2) 
                    : ts.data[i]}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {ts.timestamps?.length > 10 && (
            <div className="table-more">Showing 10 of {ts.timestamps.length} rows</div>
          )}
        </div>
      </div>
    );
  };

  const renderHyGraphDiffResult = () => {
    const result = selectedStep.result;
    const s = result.summary || {};
    const cpg = result.cpg || null;
    const ceResult = {
      nodes: result.nodes || {},
      summary: s,
      cpg: cpg,
      period_1: result.period_1 || 'Period 1',
      period_2: result.period_2 || 'Period 2',
    };

    // Build graph-view-compatible data from diff nodes for "Show in HyGraph"
    const buildDiffGraphData = () => {
      if (!result.nodes) return null;
      const nodes = Object.entries(result.nodes).map(([id, e]) => ({
        id, oid: id, label: e.label || 'Node',
        static_properties: { ...e.properties, diff_status: e.change?.delta_struct },
        temporal_properties: {},
        diff_status: e.change?.delta_struct,
        nrmse: e.change?.nrmse,
        delta_mu: e.change?.delta_mu,
        transition_timestamp: e.change?.transition_timestamp,
      }));
      // CPG edges as directed diff edges
      const edges = (cpg?.propagation_edges || []).map((pe, i) => ({
        id: `cpg_${i}`, label: 'propagates',
        source: pe.source, target: pe.target,
        static_properties: { delay_days: pe.delay_days },
        diff_status: 'CPG',
      }));
      return { nodes, edges, timeseries: {} };
    };

    return (
      <div className="step-result diff-result">
        {/* ── Summary Cards ─────────────────────────────────────────── */}
        <div className="diff-summary-grid">
          <div className="diff-card added">
            <span className="diff-num">+{s.added ?? s.nodes_added ?? 0}</span>
            <span>ADDED</span>
          </div>
          <div className="diff-card removed">
            <span className="diff-num">-{s.removed ?? s.nodes_removed ?? 0}</span>
            <span>REMOVED</span>
          </div>
          <div className="diff-card changed">
            <span className="diff-num">{s.persisted ?? s.nodes_unchanged ?? 0}</span>
            <span>PERSISTED</span>
          </div>
          <div className="diff-card unchanged">
            <span className="diff-num">{s.jaccard_nodes != null ? (s.jaccard_nodes * 100).toFixed(0) + '%' : '—'}</span>
            <span>Jaccard</span>
          </div>
        </div>

        {/* ── Signal Metrics ────────────────────────────────────────── */}
        {(
          <div className="diff-metrics">
            <div className="metric-item">
              <span className="metric-value">{s.avg_nrmse != null ? s.avg_nrmse.toFixed(3) : '—'}</span>
              <span className="metric-label">Avg Divergence</span>
            </div>
            <div className="metric-item">
              <span className="metric-value">{s.max_nrmse != null ? s.max_nrmse.toFixed(3) : '—'}</span>
              <span className="metric-label">Max Divergence</span>
            </div>
            {s.cpg_root_count != null && (
              <>
                <div className="metric-item">
                  <span className="metric-value">{s.cpg_root_count}</span>
                  <span className="metric-label">CPG Roots</span>
                </div>
                <div className="metric-item">
                  <span className="metric-value">{s.cpg_max_depth}</span>
                  <span className="metric-label">CPG Depth</span>
                </div>
                <div className="metric-item">
                  <span className="metric-value">{s.cpg_avg_delay_days != null ? (s.cpg_avg_delay_days < 1.0 ? (s.cpg_avg_delay_days * 24).toFixed(1) + 'h' : s.cpg_avg_delay_days.toFixed(1) + 'd') : '—'}</span>
                  <span className="metric-label">Avg Delay</span>
                </div>
              </>
            )}
          </div>
        )}

        {/* ── CPG Propagation Edges ─────────────────────────────────── */}
        {cpg && (cpg.node_count > 0 || cpg.edge_count > 0) && (
          <div className="cpg-section">
            <h4>Change Propagation Graph</h4>
            <div className="diff-metrics">
              <div className="metric-item"><span className="metric-value">{cpg.node_count}</span><span className="metric-label">Nodes</span></div>
              <div className="metric-item"><span className="metric-value">{cpg.edge_count}</span><span className="metric-label">Edges</span></div>
              <div className="metric-item"><span className="metric-value">{cpg.roots?.length ?? 0}</span><span className="metric-label">Roots</span></div>
              <div className="metric-item"><span className="metric-value">{cpg.max_depth}</span><span className="metric-label">Depth</span></div>
              <div className="metric-item"><span className="metric-value">{cpg.avg_delay_days != null ? (cpg.avg_delay_days < 1.0 ? (cpg.avg_delay_days * 24).toFixed(1) + 'h' : cpg.avg_delay_days.toFixed(1) + 'd') : '—'}</span><span className="metric-label">Avg Delay</span></div>
            </div>
            <div className="cpg-roots">
              <strong>Originators:</strong> {cpg.roots?.slice(0, 5).join(', ')}{cpg.roots?.length > 5 ? ` +${cpg.roots.length - 5} more` : ''}
            </div>
            {cpg.propagation_edges?.length > 0 && <div className="cpg-edges-list">
              {cpg.propagation_edges.slice(0, 8).map((e, i) => (
                <div key={i} className="cpg-edge-row">
                  <span className="cpg-node">{e.source}</span>
                  <span className="cpg-arrow">→ {e.delay_days < 1.0 ? (e.delay_days * 24).toFixed(1) + 'h' : e.delay_days.toFixed(1) + 'd'} →</span>
                  <span className="cpg-node">{e.target}</span>
                </div>
              ))}
              {cpg.propagation_edges.length > 8 && (
                <div className="cpg-more">… {cpg.propagation_edges.length - 8} more propagation edges</div>
              )}
            </div>}
            {(!cpg.propagation_edges || cpg.propagation_edges.length === 0) && cpg.node_count > 0 && (
              <div style={{fontSize:'0.8rem',color:'#94a3b8',marginTop:6}}>No propagation edges (ADDED nodes are not adjacent or delta_max_days too small)</div>
            )}
          </div>
        )}

        {/* ── Graph-Level Signal Comparison ─────────────────────────── */}
        {result.graph_level_comparisons && result.graph_level_comparisons.length > 0 && (
          <div className="graph-level-section">
            <h4>Graph-Level Signal Comparison (via TSGen)</h4>
            <table className="graph-level-table">
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Level Shift</th>
                  <th>Volatility Change</th>
                  <th>Trend Change</th>
                  <th>Divergence</th>
                </tr>
              </thead>
              <tbody>
                {result.graph_level_comparisons.map((c, i) => (
                  <tr key={i}>
                    <td style={{fontWeight: 600}}>{c.metric}</td>
                    <td>{c.delta_mu != null ? (c.delta_mu > 0 ? '+' : '') + c.delta_mu.toFixed(4) : '—'}</td>
                    <td>{c.sigma_ratio != null ? c.sigma_ratio.toFixed(3) : '—'}</td>
                    <td>{c.delta_slope != null ? (c.delta_slope > 0 ? '+' : '') + c.delta_slope.toFixed(4) : '—'}</td>
                    <td>{c.nrmse != null ? c.nrmse.toFixed(3) : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* ── Change Explorer (inline) ──────────────────────────────── */}
        {ceResult && (
          <div className="diff-change-explorer">
            <ChangeExplorer
              diffResult={ceResult}
              onNodeSelect={() => {}}
              onFilterChange={() => {}}
            />
          </div>
        )}

        {/* ── Show as HyGraph button ────────────────────────────────── */}
        {onDiffAsHyGraph && result.nodes && (
          <button className="run-step-btn" style={{ marginTop: 12, background: '#6366f1' }}
            onClick={() => {
              const data = buildDiffGraphData();
              if (data) {
                const label = `${result.period_1 || 'Period 1'} vs ${result.period_2 || 'Period 2'}`;
                onDiffAsHyGraph(data, label);
              }
            }}>
            Show Diff in HyGraph View
          </button>
        )}
      </div>
    );
  };

  // TSGen Chart Component
  const TSGenChart = ({ data }) => {
    const chartRef = useRef(null);
    
    useEffect(() => {
      if (!chartRef.current || !data?.timestamps?.length) return;
      
      const chart = echarts.init(chartRef.current);
      
      const chartData = data.timestamps.map((t, i) => [
        new Date(t),
        Array.isArray(data.data?.[i]) ? data.data[i][0] : (data.data?.[i] || 0)
      ]);
      
      chart.setOption({
        backgroundColor: 'transparent',
        grid: { left: 50, right: 20, bottom: 40, top: 20 },
        tooltip: { trigger: 'axis' },
        xAxis: {
          type: 'time',
          axisLine: { lineStyle: { color: '#e2e8f0' } },
          axisLabel: { color: '#64748b', fontSize: 10 }
        },
        yAxis: {
          type: 'value',
          axisLine: { show: false },
          axisLabel: { color: '#64748b', fontSize: 10 },
          splitLine: { lineStyle: { color: '#f1f5f9' } }
        },
        series: [{
          type: 'line',
          data: chartData,
          smooth: true,
          showSymbol: true,
          symbolSize: 6,
          lineStyle: { width: 2, color: '#3b82f6' },
          itemStyle: { color: '#3b82f6' },
          areaStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(59, 130, 246, 0.25)' },
                { offset: 1, color: 'rgba(59, 130, 246, 0.02)' }
              ]
            }
          }
        }]
      });
      
      const handleResize = () => chart.resize();
      window.addEventListener('resize', handleResize);
      
      return () => {
        window.removeEventListener('resize', handleResize);
        chart.dispose();
      };
    }, [data]);
    
    return <div ref={chartRef} style={{ width: '100%', height: '250px' }} />;
  };



  return (
    <div className="workflow-builder">
      {/* Header */}
      <header className="workflow-header">
        <button className="back-btn" onClick={onBack}>← Back</button>
        <input
          type="text"
          className="workflow-name-input"
          value={workflowName}
          onChange={e => setWorkflowName(e.target.value)}
        />
        <div className="header-actions">
          <button onClick={saveWorkflow}> Save</button>
          <button onClick={loadWorkflow}> Load</button>
          <button className="execute-all-btn" onClick={executeAll} disabled={executing || steps.length === 0}>
            {executing ? '◌ Running...' : '▶ Execute All'}
          </button>
        </div>
      </header>

      <div className="workflow-body">
        {/* Left Panel: Pipeline Steps */}
        <div className="pipeline-panel">
          <h3>Pipeline Steps</h3>
          
          <div className="steps-list">
            {steps.map((step, index) => (
              <div
                key={step.id}
                className={`step-card ${selectedStepId === step.id ? 'selected' : ''} ${step.status}`}
                onClick={() => setSelectedStepId(step.id)}
              >
                <div className="step-header">
                  <span className="step-index">{index + 1}</span>
                  <img className="step-icon" src={stepTypes.find(s => s.type === step.type)?.icon} />
                  <span className="step-name">{step.name}</span>
                  <span className={`step-status ${step.status}`}>
                    {step.status === 'success' ? '✓' : step.status === 'error' ? '✗' : step.status === 'running' ? '◌' : '○'}
                  </span>
                </div>
                {step.result && (
                  <div className="step-preview">
                    {step.type === 'subhygraph' && `${step.result.nodes?.length || 0} nodes`}
                    {step.type === 'snapshot' && `${step.result.nodes?.length || 0} nodes`}
                    {step.type === 'snapshot_sequence' && `${step.result.snapshot_count || step.result.summary?.timestamps?.length || 0} snapshots`}
                    {step.type === 'tsgen' && `${step.result.timeseries?.timestamps?.length || 0} points`}
                    {step.type === 'predicate_induced' && `${step.result.summary?.unique_members || '?'} members`}
                    {step.type === 'tsgen_induced' && `${Object.keys(step.result.metrics || {}).length} metrics`}
                    {step.type === 'hygraph_diff' && `A${step.result.summary?.added ?? 0} R${step.result.summary?.removed ?? 0} P${step.result.summary?.persisted ?? 0}`}
                  </div>
                )}
                <div className="step-actions">
                  <button onClick={(e) => { e.stopPropagation(); moveStep(step.id, 'up'); }} disabled={index === 0}>↑</button>
                  <button onClick={(e) => { e.stopPropagation(); moveStep(step.id, 'down'); }} disabled={index === steps.length - 1}>↓</button>
                  <button onClick={(e) => { e.stopPropagation(); deleteStep(step.id); }} className="delete-btn">×</button>
                </div>
              </div>
            ))}
          </div>
          
          {/* Add Step Dropdown */}
          <div className="add-step-section">
            <select
              onChange={e => { if (e.target.value) { addStep(e.target.value); e.target.value = ''; } }}
              defaultValue=""
            >
              <option value="" disabled>+ Add Step...</option>
              {stepTypes.map(st => (
                <option key={st.type} value={st.type}> {st.label}</option>
              ))}
            </select>
          </div>
          
          {/* Execution Log */}
          {executionLog.length > 0 && (
            <div className="execution-log">
              <h4>Execution Log</h4>
              {executionLog.map((log, i) => (
                <div key={i} className="log-entry">{log.message}</div>
              ))}
            </div>
          )}
        </div>

        {/* Right Panel: Step Configuration & Results */}
        <div className="config-panel">
          {renderStepConfig()}
        </div>
      </div>
    </div>
  );
};

export default WorkflowBuilder;
