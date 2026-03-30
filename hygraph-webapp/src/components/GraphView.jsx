import React, { useEffect, useRef, useState, useCallback, useMemo, memo } from 'react';
import cytoscape from 'cytoscape';
import { MapContainer, TileLayer, CircleMarker, Polyline, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import './GraphView.css';

// Diff status colors
const DIFF_COLORS = {
  added: { fill: '#22c55e', border: '#16a34a', label: '#15803d' },     
  removed: { fill: '#ef4444', border: '#dc2626', label: '#b91c1c' },    
  persisted: { fill: '#3b82f6', border: '#2563eb', label: '#1e3a8a' }, 
  changed: { fill: '#eab308', border: '#ca8a04', label: '#a16207' },    
  unchanged: { fill: '#e2e8f0', border: '#94a3b8', label: '#64748b' },  
};


const GraphView = memo(function GraphView({ data, mode, onElementSelect, selectedElement }) {
  const containerRef = useRef(null);
  const cyRef = useRef(null);
  const [graphReady, setGraphReady] = useState(false);
  const [mapKey, setMapKey] = useState(0);
  
  const callbackRef = useRef(onElementSelect);
  callbackRef.current = onElementSelect;
  
  const dataSignatureRef = useRef('');

  const isDiff = data?.isDiff || data?.metadata?.isDiff || false;

  // Compute map center from nodes if metadata doesn't provide it
  const mapCenter = useMemo(() => {
    if (data?.metadata?.center) return data.metadata.center;
    const lats = [], lngs = [];
    (data?.nodes || []).forEach(n => {
      const lat = parseFloat(n.static_properties?.lat || n.static_properties?.latitude);
      const lng = parseFloat(n.static_properties?.lon || n.static_properties?.lng || n.static_properties?.longitude);
      if (lat && lng && !isNaN(lat) && !isNaN(lng)) { lats.push(lat); lngs.push(lng); }
    });
    if (lats.length) return { lat: lats.reduce((a,b)=>a+b,0)/lats.length, lng: lngs.reduce((a,b)=>a+b,0)/lngs.length };
    return { lat: 34.05, lng: -118.25 };
  }, [data]);
  const mapZoom = data?.metadata?.zoom || 11;

  // Re-mount map when data changes (instance switch)
  useEffect(() => {
    if (mode === 'map') {
      setMapKey(prev => prev + 1);
    }
  }, [mode, data?.nodes?.length, isDiff]);

  useEffect(() => {
    return () => {
      if (cyRef.current) {
        cyRef.current.destroy();
        cyRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    if (mode !== 'force' && cyRef.current) {
      cyRef.current.destroy();
      cyRef.current = null;
      setGraphReady(false);
    }
  }, [mode]);

  // Create/update Cytoscape
  useEffect(() => {
    if (mode !== 'force' || !containerRef.current || !data?.nodes) return;
    
    const signature = `${data.nodes.length}-${data.edges?.length || 0}-${isDiff}`;
    
    if (cyRef.current && signature === dataSignatureRef.current) {
      return;
    }
    dataSignatureRef.current = signature;

    if (cyRef.current) {
      cyRef.current.destroy();
      cyRef.current = null;
    }

    const nodeIds = new Set(data.nodes.map(n => n.oid));
    const elements = [];

    // Add nodes
    data.nodes.forEach(node => {
      const diffStatus = (node.diffStatus || 'unchanged').toLowerCase();
      elements.push({
        data: {
          id: node.oid,
          label: node.label || node.oid,
          nodeType: 'node',
          diffStatus: diffStatus,
          _nodeColor: node._nodeColor || null,
          nrmse: node.nrmse,
          delta_mu: node.delta_mu,
          sigma_ratio: node.sigma_ratio,
          static_properties: node.static_properties || {},
          temporal_properties: node.temporal_properties || {}
        },
        classes: isDiff ? `diff-${diffStatus}` : (node._nodeColor ? 'custom-color' : '')
      });
    });

    // Add edges
    (data.edges || []).forEach((edge, idx) => {
      const sourceId = String(edge.source);
      const targetId = String(edge.target);
      if (!nodeIds.has(sourceId) || !nodeIds.has(targetId)) return;
      
      const diffStatus = (edge.diffStatus || 'unchanged').toLowerCase();
      elements.push({
        data: {
          id: edge.oid || `edge_${idx}`,
          source: sourceId,
          target: targetId,
          label: edge.label,
          nodeType: 'edge',
          diffStatus: diffStatus,
          static_properties: edge.static_properties || {},
          temporal_properties: edge.temporal_properties || {}
        },
        classes: isDiff ? `diff-${diffStatus}` : ''
      });
    });

    // Add CPG propagation edges (dashed orange)
    if (isDiff && data.cpg_edges) {
      data.cpg_edges.forEach((cpgEdge, idx) => {
        const src = String(cpgEdge.source);
        const tgt = String(cpgEdge.target);
        if (!nodeIds.has(src) || !nodeIds.has(tgt)) return;
        elements.push({
          data: {
            id: `cpg_${idx}`, source: src, target: tgt,
            label: 'propagation', nodeType: 'edge', diffStatus: 'cpg',
            static_properties: { delay_days: cpgEdge.delay_days },
          },
          classes: 'cpg-edge'
        });
      });
    }

    // Build styles
    const styles = [
      // Default node style
      {
        selector: 'node',
        style: {
          'background-color': '#dbeafe',
          'label': 'data(label)',
          'color': '#1e3a8a',
          'text-valign': 'center',
          'text-halign': 'center',
          'font-size': '12px',
          'font-weight': 'bold',
          'width': '50px',
          'height': '50px',
          'border-width': '2px',
          'border-color': '#3b82f6',
          'text-wrap': 'wrap',
          'text-max-width': '45px'
        }
      },
      {
        selector: 'node.selected',
        style: {
          'border-color': '#10b981',
          'border-width': '4px',
          'background-color': '#d1fae5'
        }
      },
      // Custom node color (cluster coloring from InducedExplorer)
      {
        selector: 'node.custom-color',
        style: {
          'background-color': 'data(_nodeColor)',
          'border-color': 'data(_nodeColor)',
          'border-width': '3px',
          'opacity': 0.9
        }
      },
      // Default edge style
      {
        selector: 'edge',
        style: {
          'width': 2,
          'line-color': '#9ca3af',
          'target-arrow-color': '#9ca3af',
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier'
        }
      },
      {
        selector: 'edge.selected',
        style: {
          'line-color': '#3b82f6',
          'target-arrow-color': '#3b82f6',
          'width': 3
        }
      }
    ];

    // Add diff status styles
    if (isDiff) {
      // Diff node styles
      styles.push(
        {
          selector: 'node.diff-added',
          style: {
            'background-color': DIFF_COLORS.added.fill,
            'border-color': DIFF_COLORS.added.border,
            'color': DIFF_COLORS.added.label,
            'border-width': '3px'
          }
        },
        {
          selector: 'node.diff-removed',
          style: {
            'background-color': DIFF_COLORS.removed.fill,
            'border-color': DIFF_COLORS.removed.border,
            'color': '#fff',
            'border-width': '3px',
            'border-style': 'dashed'
          }
        },
        {
          selector: 'node.diff-changed',
          style: {
            'background-color': DIFF_COLORS.changed.fill,
            'border-color': DIFF_COLORS.changed.border,
            'color': DIFF_COLORS.changed.label,
            'border-width': '3px'
          }
        },
        {
          selector: 'node.diff-unchanged',
          style: {
            'background-color': '#e2e8f0',
            'border-color': '#94a3b8',
            'color': '#64748b',
            'opacity': 0.7
          }
        },
        // Diff edge styles
        {
          selector: 'edge.diff-added',
          style: {
            'line-color': DIFF_COLORS.added.border,
            'target-arrow-color': DIFF_COLORS.added.border,
            'width': 3
          }
        },
        {
          selector: 'edge.diff-removed',
          style: {
            'line-color': DIFF_COLORS.removed.border,
            'target-arrow-color': DIFF_COLORS.removed.border,
            'width': 3,
            'line-style': 'dashed'
          }
        },
        {
          selector: 'edge.diff-changed',
          style: {
            'line-color': DIFF_COLORS.changed.border,
            'target-arrow-color': DIFF_COLORS.changed.border,
            'width': 3
          }
        },
        {
          selector: 'edge.diff-unchanged',
          style: {
            'line-color': '#cbd5e1',
            'target-arrow-color': '#cbd5e1',
            'opacity': 0.5
          }
        },
        {
          selector: 'edge.diff-persisted',
          style: {
            'line-color': DIFF_COLORS.persisted.border,
            'target-arrow-color': DIFF_COLORS.persisted.border,
            'width': 2
          }
        },
        {
          selector: '.cpg-edge',
          style: {
            'line-color': '#f59e0b',
            'target-arrow-color': '#f59e0b',
            'target-arrow-shape': 'triangle',
            'line-style': 'dashed',
            'line-dash-pattern': [8, 6],
            'width': 4,
            'opacity': 0.85,
            'curve-style': 'bezier',
            'arrow-scale': 1.5,
          }
        }
      );
    }

    const cy = cytoscape({
      container: containerRef.current,
      elements: elements,
      style: styles,
      layout: {
        name: 'cose',
        animate: false,
        fit: true,
        padding: 50,
        nodeRepulsion: 8000,
        idealEdgeLength: 100,
        randomize: false
      },
      wheelSensitivity: 0.2,
      boxSelectionEnabled: false,
      autounselectify: true
    });

    // Click handlers
    cy.on('tap', 'node', (evt) => {
      evt.stopPropagation();
      const d = evt.target.data();
      
      
      
      cy.elements().removeClass('selected');
      evt.target.addClass('selected');
      
      callbackRef.current?.({ 
        id: d.id, 
        oid: d.id,
        label: d.label, 
        type: 'node',
        diffStatus: d.diffStatus,
        nrmse: d.nrmse,
        delta_mu: d.delta_mu,
        sigma_ratio: d.sigma_ratio,
        static_properties: d.static_properties || {},
        temporal_properties: d.temporal_properties || {}
      });
    });

    cy.on('tap', 'edge', (evt) => {
      evt.stopPropagation();
      const d = evt.target.data();
      
      cy.elements().removeClass('selected');
      evt.target.addClass('selected');
      
      callbackRef.current?.({ 
        id: d.id,
        oid: d.id,
        label: d.label || `${d.source} -> ${d.target}`, 
        type: 'edge', 
        source: d.source, 
        target: d.target,
        diffStatus: d.diffStatus,
        static_properties: d.static_properties || {},
        temporal_properties: d.temporal_properties || {}
      });
    });

    cy.on('tap', (evt) => {
      if (evt.target === cy) {
        cy.elements().removeClass('selected');
        callbackRef.current?.(null);
      }
    });

    cyRef.current = cy;
    setGraphReady(true);

  }, [mode, data, isDiff]);

  // Sync selection state
  useEffect(() => {
    if (!cyRef.current || mode !== 'force' || !graphReady) return;
    
    cyRef.current.elements().removeClass('selected');
    
    if (selectedElement?.id) {
      const el = cyRef.current.getElementById(selectedElement.id);
      if (el.length > 0) {
        el.addClass('selected');
      }
    }
  }, [selectedElement?.id, mode, graphReady]);

  const handleFit = useCallback(() => cyRef.current?.fit(null, 50), []);
  const handleCenter = useCallback(() => cyRef.current?.center(), []);
  const handleReset = useCallback(() => {
    if (cyRef.current) {
      cyRef.current.reset();
      cyRef.current.fit(null, 50);
    }
  }, []);

  // Get node color based on diff status
  const getNodeColor = (diffStatus) => {
    if (!isDiff) return { fill: '#dbeafe', border: '#3b82f6' };
    return {
      fill: DIFF_COLORS[diffStatus]?.fill || '#e2e8f0',
      border: DIFF_COLORS[diffStatus]?.border || '#94a3b8'
    };
  };

  // Get edge color based on diff status
  const getEdgeColor = (diffStatus) => {
    if (!isDiff) return '#9ca3af';
    return DIFF_COLORS[diffStatus]?.border || '#cbd5e1';
  };


  if (mode === 'map') {
    if (!data?.metadata?.hasCoordinates) {
      return (
        <div className="graph-view-container">
          <div className="graph-header">
            <h3>Map View</h3>
            <span className="warning-badge">No coordinates</span>
          </div>
          <div className="no-map-message">
            <p>No lat/lng data. Switch to Graph View.</p>
          </div>
        </div>
      );
    }

    const nodeMap = {};
    data.nodes.forEach(node => {
      const lat = parseFloat(node.static_properties?.lat || node.static_properties?.latitude);
      const lng = parseFloat(node.static_properties?.lon || node.static_properties?.lng || node.static_properties?.longitude);
      if (lat && lng) nodeMap[node.oid] = { ...node, lat, lng };
    });

    return (
      <div className="graph-view-container">
        <div className="graph-header">
          <h3>Map View</h3>
          <span className="stat-badge">{Object.keys(nodeMap).length} stations</span>
        </div>
        <div key={mapKey} className="leaflet-map-wrapper">
          <MapContainer 
            center={[mapCenter.lat, mapCenter.lng]} 
            zoom={mapZoom} 
            style={{ height: '100%', width: '100%' }}
          >
            <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
            
            {data.edges?.map((edge, idx) => {
              const src = nodeMap[edge.source];
              const tgt = nodeMap[edge.target];
              if (!src || !tgt) return null;
              
              const isSelected = selectedElement?.id === edge.oid;
              const diffStatus = (edge.diffStatus || 'unchanged').toLowerCase();
              const edgeColor = getEdgeColor(diffStatus);
              
              return (
                <Polyline
                  key={edge.oid || `edge_${idx}`}
                  positions={[[src.lat, src.lng], [tgt.lat, tgt.lng]]}
                  color={isSelected ? '#3b82f6' : edgeColor}
                  weight={isSelected ? 4 : (isDiff && diffStatus !== 'unchanged' ? 3 : 2)}
                  opacity={isDiff && diffStatus === 'unchanged' ? 0.3 : 0.7}
                  dashArray={diffStatus === 'removed' ? '5,5' : undefined}
                  eventHandlers={{
                    click: () => callbackRef.current?.({
                      id: edge.oid,
                      oid: edge.oid,
                      label: edge.label || `${src.label} -> ${tgt.label}`,
                      type: 'edge',
                      source: edge.source,
                      target: edge.target,
                      diffStatus: diffStatus,
                      static_properties: edge.static_properties || {},
                      temporal_properties: edge.temporal_properties || {}
                    })
                  }}
                />
              );
            })}

            {/* Nodes — rendered first so CPG overlays are on top */}
            {Object.values(nodeMap).map(node => {
              const isSelected = selectedElement?.id === node.oid;
              const diffStatus = (node.diffStatus || 'unchanged').toLowerCase();
              const colors = getNodeColor(diffStatus);
              
              return (
                <CircleMarker
                  key={node.oid}
                  center={[node.lat, node.lng]}
                  radius={isSelected ? 12 : (isDiff && diffStatus !== 'unchanged' ? 9 : 7)}
                  fillColor={isSelected ? '#d1fae5' : colors.fill}
                  color={isSelected ? '#10b981' : colors.border}
                  weight={isDiff && diffStatus !== 'unchanged' ? 3 : 2}
                  fillOpacity={isDiff && diffStatus === 'unchanged' ? 0.5 : 0.9}
                  eventHandlers={{
                    click: () => {

                      callbackRef.current?.({
                        id: node.oid,
                        oid: node.oid,
                        label: node.label,
                        type: 'node',
                        diffStatus: diffStatus,
                        nrmse: node.nrmse,
                        delta_mu: node.delta_mu,
                        sigma_ratio: node.sigma_ratio,
                        static_properties: node.static_properties || {},
                        temporal_properties: node.temporal_properties || {}
                      });
                    }
                  }}
                >
                  <Popup>
                    <strong>{node.label || node.oid}</strong>
                    {isDiff && diffStatus !== 'unchanged' && (
                      <span className={`popup-diff-badge popup-diff-${diffStatus}`}> {diffStatus}</span>
                    )}

                  </Popup>
                </CircleMarker>
              );
            })}

            {/* CPG Propagation Edges — dashed orange, no offset, on top of nodes */}
            {isDiff && data.cpg_edges?.map((cpgEdge, idx) => {
              const src = nodeMap[cpgEdge.source];
              const tgt = nodeMap[cpgEdge.target];
              if (!src || !tgt) return null;
              const delayLabel = cpgEdge.delay_days < 1.0
                ? (cpgEdge.delay_days * 24).toFixed(1) + 'h'
                : cpgEdge.delay_days.toFixed(1) + 'd';
              return (
                <Polyline key={`cpg_${idx}`}
                  positions={[[src.lat, src.lng], [tgt.lat, tgt.lng]]}
                  color="#f59e0b" weight={5} opacity={0.9} dashArray="10,6"
                  eventHandlers={{
                    click: () => callbackRef.current?.({
                      id: `cpg_${idx}`, type: 'edge',
                      label: `Propagation: ${cpgEdge.source} → ${cpgEdge.target} (${delayLabel})`,
                      static_properties: { delay: delayLabel, source_time: cpgEdge.source_timestamp, target_time: cpgEdge.target_timestamp },
                      temporal_properties: {},
                    })
                  }}>
                  <Popup>
                    <strong>Propagation</strong><br/>
                    {cpgEdge.source} → {cpgEdge.target}<br/>
                    Delay: {delayLabel}
                  </Popup>
                </Polyline>
              );
            })}

            {/* Originator rings — on top of everything */}
            {isDiff && data.metadata?.cpg_roots?.map((rootId, idx) => {
              const node = nodeMap[rootId];
              if (!node) return null;
              return (
                <CircleMarker key={`root_${idx}`}
                  center={[node.lat, node.lng]} radius={18}
                  fillColor="transparent" color="#f59e0b" weight={3} fillOpacity={0}
                  dashArray="6,4"
                  eventHandlers={{
                    click: () => callbackRef.current?.({
                      id: rootId, oid: rootId, label: node.label || rootId,
                      type: 'node', diffStatus: 'added',
                      static_properties: node.static_properties || {},
                      temporal_properties: node.temporal_properties || {},
                    })
                  }}>
                  <Popup><strong>Originator:</strong> {node.label || rootId}</Popup>
                </CircleMarker>
              );
            })}

          </MapContainer>
          {isDiff && (
            <div style={{
              position: 'absolute', bottom: 10, left: 10, zIndex: 1000,
              background: 'white', borderRadius: 8, padding: '8px 12px',
              boxShadow: '0 2px 8px rgba(0,0,0,0.15)', fontSize: '0.75rem',
              display: 'flex', gap: 12, alignItems: 'center',
            }}>
              <span style={{display:'flex',alignItems:'center',gap:4}}><span style={{width:12,height:12,borderRadius:'50%',background:'#22c55e',display:'inline-block'}}></span> Added</span>
              <span style={{display:'flex',alignItems:'center',gap:4}}><span style={{width:12,height:12,borderRadius:'50%',background:'#ef4444',display:'inline-block'}}></span> Removed</span>
              <span style={{display:'flex',alignItems:'center',gap:4}}><span style={{width:12,height:12,borderRadius:'50%',background:'#3b82f6',display:'inline-block'}}></span> Persisted</span>
              <span style={{display:'flex',alignItems:'center',gap:4}}><span style={{width:16,height:3,background:'#f59e0b',display:'inline-block',borderTop:'2px dashed #f59e0b'}}></span> Propagation</span>
              <span style={{display:'flex',alignItems:'center',gap:4}}><span style={{width:12,height:12,borderRadius:'50%',border:'2px dashed #f59e0b',display:'inline-block'}}></span> Originator</span>
            </div>
          )}
        </div>
      </div>
    );
  }


  return (
    <div className="graph-view-container">
      <div className="graph-header">
        <h3>HyGraph View</h3>
        <div className="header-stats">
          <span className="stat-badge">{data?.nodes?.length || 0} nodes</span>
          <span className="stat-badge">{data?.edges?.length || 0} edges</span>
        </div>
      </div>
      <div ref={containerRef} className="cytoscape-container" />
      <div className="graph-controls">
        <button onClick={handleFit}>Fit</button>
        <button onClick={handleCenter}>Center</button>
        <button onClick={handleReset}>Reset</button>
      </div>
    </div>
  );
});

export default GraphView;
