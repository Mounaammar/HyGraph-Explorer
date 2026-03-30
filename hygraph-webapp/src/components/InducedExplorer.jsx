import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { MapContainer, TileLayer, CircleMarker, Polyline, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import * as echarts from 'echarts';
import GraphView from './GraphView';
import './InducedExplorer.css';

const API = 'http://localhost:8000';

const COMP_COLORS = [
  '#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6',
  '#1abc9c', '#e67e22', '#e84393', '#00b894', '#6c5ce7',
];

const COMP_LABELS = ['Cluster 1','Cluster 2','Cluster 3','Cluster 4','Cluster 5',
  'Cluster 6','Cluster 7','Cluster 8','Cluster 9','Cluster 10'];


const OPS = {
  '<':  (a, b) => a < b,
  '<=': (a, b) => a <= b,
  '>':  (a, b) => a > b,
  '>=': (a, b) => a >= b,
  '==': (a, b) => Math.abs(a - b) < 1e-9,
  '!=': (a, b) => Math.abs(a - b) >= 1e-9,
};

function computeComponents(members, adjacency) {
  const memberSet = new Set(members);
  const adj = {};
  for (const [s, t] of adjacency) {
    if (memberSet.has(s) && memberSet.has(t)) {
      if (!adj[s]) adj[s] = [];
      if (!adj[t]) adj[t] = [];
      adj[s].push(t);
      adj[t].push(s);
    }
  }
  const visited = new Set();
  const components = [];
  for (const node of members) {
    if (visited.has(node)) continue;
    const comp = [];
    const queue = [node];
    visited.add(node);
    while (queue.length) {
      const cur = queue.shift();
      comp.push(cur);
      for (const neighbor of (adj[cur] || [])) {
        if (!visited.has(neighbor)) {
          visited.add(neighbor);
          queue.push(neighbor);
        }
      }
    }
    components.push(comp);
  }
  // Sort by size descending so largest cluster = Cluster 1
  components.sort((a, b) => b.length - a.length);
  return components;
}

function findClosestTs(nodeValues, targetTs) {
  if (!nodeValues) return null;
  const targetPrefix = targetTs.substring(0, 19);
  for (const k of Object.keys(nodeValues)) {
    if (k.startsWith(targetPrefix)) return k;
  }
  const targetTime = new Date(targetTs).getTime();
  let best = null, bestDist = Infinity;
  for (const k of Object.keys(nodeValues)) {
    const d = Math.abs(new Date(k).getTime() - targetTime);
    if (d < bestDist) { bestDist = d; best = k; }
  }
  return bestDist < 3600000 ? best : null;
}

function evalExpression(pred, rawVal, staticProps) {
  if (pred.is_simple) return rawVal;
  const ctx = { [pred.ts_property]: rawVal, ...staticProps };
  let expr = pred.expression;
  for (const [k, v] of Object.entries(ctx)) {
    expr = expr.replace(new RegExp(`\\b${k}\\b`, 'g'), String(v));
  }
  try { return Function('"use strict"; return (' + expr + ')')(); } catch { return null; }
}

export default function InducedExplorer({ inducedId, stepConfig, summary }) {
  const [nodePositions, setNodePositions] = useState(null);
  const [mapCenter, setMapCenter] = useState([34.05, -118.25]);
  const [rawData, setRawData] = useState(null);
  const [rawLoading, setRawLoading] = useState(false);
  const [currentIdx, setCurrentIdx] = useState(0);
  const [threshold, setThreshold] = useState(null);
  const [originalThreshold, setOriginalThreshold] = useState(null);
  const [lifecycleEvents, setLifecycleEvents] = useState([]);
  const [selectedNode, setSelectedNode] = useState(null);
  const nodeChartRef = useRef(null);
  const nodeChartInstance = useRef(null);
  const [viewMode, setViewMode] = useState('map'); // 'map' or 'graph'
  const [playing, setPlaying] = useState(false);
  const playRef = useRef(false);
  const timerRef = useRef(null);

  // Fetch node positions
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API}/api/graph`);
        const data = await res.json();
        const positions = {};
        const lats = [], lngs = [];
        (data.nodes || []).forEach(n => {
          const sp = n.static_properties || {};
          const lat = parseFloat(sp.lat || sp.latitude);
          const lng = parseFloat(sp.lon || sp.lng || sp.longitude);
          if (lat && lng && !isNaN(lat) && !isNaN(lng)) {
            positions[n.oid] = { lat, lng, label: n.label || n.oid, props: sp };
            lats.push(lat);
            lngs.push(lng);
          }
        });
        setNodePositions(positions);
        if (lats.length) {
          setMapCenter([
            lats.reduce((a, b) => a + b, 0) / lats.length,
            lngs.reduce((a, b) => a + b, 0) / lngs.length,
          ]);
        }
      } catch (e) { console.error('Failed to load graph positions:', e); }
    })();
  }, []);

  // Fetch raw data
  useEffect(() => {
    if (!inducedId) return;
    setRawLoading(true);
    (async () => {
      try {
        const res = await fetch(`${API}/api/induced/${inducedId}/raw_data`);
        if (!res.ok) { setRawLoading(false); return; }
        const data = await res.json();
        if (data.status === 'ok') {
          setRawData(data);
          setOriginalThreshold(data.predicate?.threshold);
          setThreshold(data.predicate?.threshold);
          setCurrentIdx(0);
        }
      } catch (e) { console.error('Failed to load raw data:', e); }
      finally { setRawLoading(false); }
    })();
  }, [inducedId]);

  // Fetch lifecycle events
  useEffect(() => {
    if (!inducedId) return;
    (async () => {
      try {
        const res = await fetch(`${API}/api/induced/${inducedId}/lifecycle`);
        const data = await res.json();
        setLifecycleEvents(data.events || []);
      } catch (e) { console.error('Failed to load lifecycle events:', e); }
    })();
  }, [inducedId]);

  // Animation
  useEffect(() => {
    playRef.current = playing;
    if (playing && rawData?.timestamps) {
      timerRef.current = setInterval(() => {
        if (!playRef.current) return;
        setCurrentIdx(prev => {
          if (prev >= rawData.timestamps.length - 1) { setPlaying(false); return prev; }
          return prev + 1;
        });
      }, 400);
    }
    return () => clearInterval(timerRef.current);
  }, [playing, rawData?.timestamps?.length]);

  // Client-side predicate evaluation
  const { members, components, edges } = useMemo(() => {
    if (!rawData?.timestamps || !rawData.values || threshold == null)
      return { members: [], components: [], edges: [] };
    const ts = rawData.timestamps[currentIdx];
    if (!ts) return { members: [], components: [], edges: [] };
    const pred = rawData.predicate;
    const opFn = OPS[pred.operator] || (() => false);
    const qualifying = [];
    for (const [nodeUid, nodeValues] of Object.entries(rawData.values)) {
      const matchTs = findClosestTs(nodeValues, ts);
      if (!matchTs) continue;
      const rawVal = nodeValues[matchTs];
      if (rawVal == null || rawVal === 0) continue; // skip missing/faulty readings
      const lhs = evalExpression(pred, rawVal, rawData.static_props?.[nodeUid] || {});
      if (lhs != null && opFn(lhs, threshold)) qualifying.push(nodeUid);
    }
    const comps = computeComponents(qualifying, rawData.adjacency);
    const qualSet = new Set(qualifying);
    const edgesFiltered = rawData.adjacency.filter(([s, t]) => qualSet.has(s) && qualSet.has(t));
    return { members: qualifying, components: comps, edges: edgesFiltered };
  }, [rawData, currentIdx, threshold]);

  const timestamps = rawData?.timestamps || [];
  const currentTs = timestamps[currentIdx] || '';
  const timeLabel = currentTs ? new Date(currentTs).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }) : '--:--';
  const dateLabel = currentTs ? new Date(currentTs).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '';
  const memberSet = new Set(members);
  const nodeCompIdx = {};
  components.forEach((comp, idx) => {
    comp.forEach(nid => { nodeCompIdx[nid] = idx; });
  });
  // Compute actual min/max of expression values from the data
  const expressionRange = useMemo(() => {
    if (!rawData?.values || !rawData.predicate) return null;
    let min = Infinity, max = -Infinity;
    const pred = rawData.predicate;
    for (const [nodeUid, nodeValues] of Object.entries(rawData.values)) {
      const sp = rawData.static_props?.[nodeUid] || {};
      for (const rawVal of Object.values(nodeValues)) {
        if (rawVal == null || rawVal === 0) continue;
        const lhs = evalExpression(pred, rawVal, sp);
        if (lhs != null && isFinite(lhs)) {
          if (lhs < min) min = lhs;
          if (lhs > max) max = lhs;
        }
      }
    }
    return min < Infinity ? { min, max } : null;
  }, [rawData]);

  const thresholdMin = expressionRange ? +Math.max(0, expressionRange.min).toFixed(4) : 0;
  const thresholdMax = expressionRange ? +expressionRange.max.toFixed(4) : 1;
  const thresholdStep = +((thresholdMax - thresholdMin) / 200).toFixed(6) || 0.01;
  const thresholdChanged = threshold != null && originalThreshold != null && Math.abs(threshold - originalThreshold) > 1e-6;

  // Get node detail values
  const getNodeDetail = (oid) => {
    if (!rawData?.values?.[oid] || !currentTs) return null;
    const nodeVals = rawData.values[oid];
    const matchTs = findClosestTs(nodeVals, currentTs);
    if (!matchTs) return null;
    const rawVal = nodeVals[matchTs];
    if (rawVal == null) return null;
    const pred = rawData.predicate;
    const computed = evalExpression(pred, rawVal, rawData.static_props?.[oid] || {});
    const qualifies = computed != null && OPS[pred.operator]?.(computed, threshold);
    return { rawVal, computed, qualifies };
  };

  if (!nodePositions || rawLoading) {
    return <div className="induced-explorer"><div className="ie-loading">{rawLoading ? 'Loading sensor data...' : 'Loading sensor positions...'}</div></div>;
  }
  if (!rawData) {
    return <div className="induced-explorer"><div className="ie-loading">No data available. Restart backend and re-evaluate.</div></div>;
  }

  return (
    <div className="induced-explorer">
      {/* Header */}
      <div className="ie-header">
        <h4>Induced SubHyGraph Explorer</h4>
        <div className="ie-stats">
          <span className="ie-badge">{memberSet.size} active</span>
          <span className="ie-badge ie-badge-comp">{components.length} cluster{components.length !== 1 ? 's' : ''}</span>
          <span className="ie-badge ie-badge-time">{dateLabel} {timeLabel}</span>
          <div className="ie-view-toggle">
            <button className={viewMode === 'map' ? 'active' : ''} onClick={() => setViewMode('map')}>Map</button>
            <button className={viewMode === 'graph' ? 'active' : ''} onClick={() => setViewMode('graph')}>Graph</button>
          </div>
        </div>
      </div>

      {/* Timestep Slider */}
      <div className="ie-slider-row">
        <button className="ie-play-btn" onClick={() => setPlaying(!playing)}>
          {playing ? '⏸' : '▶'}
        </button>
        <input type="range" className="ie-slider" min={0} max={Math.max(0, timestamps.length - 1)}
          value={currentIdx} onChange={e => { setPlaying(false); setCurrentIdx(parseInt(e.target.value)); }} />
        <span className="ie-slider-label">{currentIdx + 1} / {timestamps.length}</span>
      </div>

      {/* Threshold Slider */}
      {originalThreshold != null && (
        <div className="ie-slider-row ie-threshold-row">
          <span className="ie-threshold-label">Threshold</span>
          <span style={{fontSize:'0.65rem',color:'#94a3b8',minWidth:32,textAlign:'right'}}>{thresholdMin}</span>
          <input type="range" className="ie-slider" min={thresholdMin} max={thresholdMax} step={thresholdStep}
            value={threshold ?? originalThreshold} onChange={e => setThreshold(parseFloat(e.target.value))} />
          <span style={{fontSize:'0.65rem',color:'#94a3b8',minWidth:32}}>{thresholdMax}</span>
          <span className={`ie-threshold-value ${thresholdChanged ? 'ie-threshold-changed' : ''}`}>
            {(threshold ?? originalThreshold).toFixed(originalThreshold < 1 ? 3 : originalThreshold < 10 ? 1 : 0)}
          </span>
          {thresholdChanged && <button className="ie-reset-btn" onClick={() => setThreshold(originalThreshold)}>↺</button>}
        </div>
      )}

      {/* Legend */}
      <div className="ie-legend">
        <div className="ie-legend-item">
          <span className="ie-legend-dot ie-legend-inactive"></span>
          <span>Inactive sensor</span>
        </div>
        {components.slice(0, 6).map((comp, i) => (
          <div key={i} className="ie-legend-item">
            <span className="ie-legend-dot" style={{ background: COMP_COLORS[i], border: `2px solid ${COMP_COLORS[i]}` }}></span>
            <span>Cluster {i + 1} ({comp.length} sensors)</span>
          </div>
        ))}
        {components.length > 6 && <div className="ie-legend-item"><span>+{components.length - 6} more clusters</span></div>}
      </div>

      {/* Map or Graph View */}
      <div className="ie-map-container">
        {viewMode === 'map' ? (
          <MapContainer center={mapCenter} zoom={11} style={{ height: '100%', width: '100%' }}>
            <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
            {/* Edges */}
            {edges.map(([s, t], i) => {
              const src = nodePositions[s];
              const tgt = nodePositions[t];
              if (!src || !tgt) return null;
              const ci = nodeCompIdx[s] ?? 0;
              return (
                <Polyline key={`e_${i}`} positions={[[src.lat, src.lng], [tgt.lat, tgt.lng]]}
                  color={COMP_COLORS[ci % COMP_COLORS.length]} weight={3.5} opacity={0.75}
                  eventHandlers={{ click: () => setSelectedNode(`edge_${s}_${t}`) }}>
                  <Popup>
                    <strong>Edge</strong><br />
                    {nodePositions[s]?.label} ({s}) → {nodePositions[t]?.label} ({t})<br />
                    Cluster {(ci ?? 0) + 1}
                  </Popup>
                </Polyline>
              );
            })}
            {/* Nodes */}
            {Object.entries(nodePositions).map(([oid, pos]) => {
              const isActive = memberSet.has(oid);
              const compIdx = nodeCompIdx[oid];
              const color = isActive ? COMP_COLORS[(compIdx ?? 0) % COMP_COLORS.length] : '#a8b4c0';
              const detail = isActive ? getNodeDetail(oid) : null;
              return (
                <CircleMarker key={oid} center={[pos.lat, pos.lng]}
                  radius={isActive ? 10 : 6}
                  fillColor={color}
                  color={isActive ? '#333' : '#8899aa'}
                  weight={isActive ? 2 : 1}
                  fillOpacity={isActive ? 0.9 : 0.45}
                  eventHandlers={{ click: () => setSelectedNode(oid) }}>
                  <Popup>
                    <strong>{pos.label} ({oid})</strong><br />
                    <span style={{color: isActive ? '#27ae60' : '#95a5a6', fontWeight: 600}}>
                      {isActive ? `● Cluster ${(compIdx ?? 0) + 1}` : '○ Inactive'}
                    </span>
                    {detail && (
                      <>
                        <br />{rawData.predicate.ts_property}: <strong>{detail.rawVal.toFixed(1)}</strong>
                        
                      </>
                    )}
                  </Popup>
                </CircleMarker>
              );
            })}
          </MapContainer>
        ) : (
          <GraphView
            data={{
              nodes: Object.entries(nodePositions)
                .filter(([oid]) => memberSet.has(oid))
                .map(([oid, pos]) => {
                  const ci = nodeCompIdx[oid] ?? 0;
                  return {
                    oid,
                    label: pos.label || 'Sensor',
                    _nodeColor: COMP_COLORS[ci % COMP_COLORS.length],
                    static_properties: { lat: pos.lat, lon: pos.lng, cluster: ci + 1, ...(pos.props || {}) },
                    temporal_properties: {},
                  };
                }),
              edges: edges.map(([s, t], i) => ({
                oid: `edge_${i}`,
                source: s,
                target: t,
                label: 'road_segment',
                static_properties: {},
                temporal_properties: {},
              })),
              metadata: { hasCoordinates: true, center: { lat: mapCenter[0], lng: mapCenter[1] }, zoom: 11 },
            }}
            mode="force"
            onElementSelect={(el) => el && setSelectedNode(el.oid)}
            selectedElement={selectedNode ? { id: selectedNode } : null}
          />
        )}
      </div>

      {/* Selected Node Detail */}
      {selectedNode && !selectedNode.startsWith('edge_') && rawData?.values?.[selectedNode] && (() => {
        const pred = rawData.predicate;
        const nodeVals = rawData.values[selectedNode];
        const staticP = rawData.static_props?.[selectedNode] || {};
        // Build time series for this node across all timestamps
        const series = timestamps.map((ts, idx) => {
          const matchTs = findClosestTs(nodeVals, ts);
          const rawVal = matchTs ? nodeVals[matchTs] : null;
          const computed = rawVal != null ? evalExpression(pred, rawVal, staticP) : null;
          const qualifies = computed != null && OPS[pred.operator]?.(computed, threshold);
          return { idx, rawVal, computed, qualifies, time: new Date(ts).toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit'}) };
        }).filter(p => p.computed != null);

        // Render chart after DOM update
        setTimeout(() => {
          if (!nodeChartRef.current) return;
          if (nodeChartInstance.current) try { nodeChartInstance.current.dispose(); } catch(e) {}
          const chart = echarts.init(nodeChartRef.current);
          nodeChartInstance.current = chart;
          const qualData = series.filter(p => p.qualifies).map(p => [p.time, p.computed]);
          const nonQualData = series.filter(p => !p.qualifies).map(p => [p.time, p.computed]);
          chart.setOption({
            backgroundColor: 'transparent',
            grid: { left: 50, right: 15, bottom: 25, top: 15 },
            tooltip: { trigger: 'axis', textStyle: { fontSize: 11 } },
            xAxis: { type: 'category', data: series.map(p => p.time), axisLabel: { fontSize: 8, rotate: 45 }, axisLine: { lineStyle: { color: '#e2e8f0' } } },
            yAxis: { type: 'value', axisLine: { show: false }, axisLabel: { fontSize: 9 }, splitLine: { lineStyle: { color: '#f1f5f9' } } },
            series: [
              { type: 'line', data: series.map(p => p.computed), smooth: true, showSymbol: series.length < 80, symbolSize: 4,
                lineStyle: { width: 2, color: '#3b82f6' },
                areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: 'rgba(59,130,246,0.2)' }, { offset: 1, color: 'rgba(59,130,246,0.02)' }] } },
                markLine: {
                  silent: true, symbol: 'none',
                  lineStyle: { color: '#ef4444', type: 'dashed', width: 2 },
                  label: { formatter: `threshold = ${threshold?.toFixed(3)}`, fontSize: 10, color: '#ef4444', position: 'insideEndTop' },
                  data: [{ yAxis: threshold }]
                }
              }
            ]
          });
        }, 50);

        return (
          <div className="ie-node-detail">
            <div className="ie-detail-header">
              <strong>{nodePositions[selectedNode]?.label || 'Sensor'} ({selectedNode})</strong>
              <span className={`ie-detail-status ${memberSet.has(selectedNode) ? 'ie-status-active' : 'ie-status-inactive'}`}>
                {memberSet.has(selectedNode) ? `Cluster ${(nodeCompIdx[selectedNode] ?? 0) + 1}` : 'Inactive'}
              </span>
              <button className="ie-detail-close" onClick={() => setSelectedNode(null)}>✕</button>
            </div>
            <div ref={nodeChartRef} style={{ width: '100%', height: 180, marginBottom: 8 }} />
            <div className="ie-detail-table-wrapper">
              <table className="ie-detail-table">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>{pred.ts_property}</th>
                    {!pred.is_simple && <th>{pred.expression}</th>}
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {series.map((p, i) => (
                    <tr key={i} className={p.idx === currentIdx ? 'ie-row-current' : ''}>
                      <td>{p.time}</td>
                      <td>{p.rawVal?.toFixed(1)}</td>
                      {!pred.is_simple && <td>{p.computed?.toFixed(3)}</td>}
                      <td>
                        <span className={`ie-qual-badge ${p.qualifies ? 'ie-qual-yes' : 'ie-qual-no'}`}>
                          {p.qualifies ? `✓ ${pred.operator} ${threshold?.toFixed(3)}` : `✗ ${pred.operator} ${threshold?.toFixed(3)}`}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );
      })()}

      {/* Lifecycle Timeline */}
      <div className="ie-lifecycle">
        <h4>Lifecycle Events ({lifecycleEvents.length})</h4>
        <div className="ie-events-list">
          {lifecycleEvents.length === 0 && <div className="ie-no-events">No lifecycle events recorded.</div>}
          {lifecycleEvents.map((ev, i) => {
            const evTime = new Date(ev.timestamp);
            const stepMs = (stepConfig?.step_minutes || 5) * 60 * 1000;
            const isCurrent = currentTs && Math.abs(evTime.getTime() - new Date(currentTs).getTime()) < stepMs;
            return (
              <div key={i} className={`ie-event ${isCurrent ? 'ie-event-active' : ''}`}
                onClick={() => {
                  const idx = timestamps.findIndex(t => Math.abs(new Date(t).getTime() - evTime.getTime()) < stepMs);
                  if (idx >= 0) { setPlaying(false); setCurrentIdx(idx); }
                }}>
                <span className="ie-event-type">{ev.event_type}</span>
                <span className="ie-event-time">{evTime.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}</span>
                <span className="ie-event-detail">C{ev.component_id} ({ev.size} node{ev.size !== 1 ? 's' : ''})</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
