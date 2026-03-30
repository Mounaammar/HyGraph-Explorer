import React, { useState, useMemo, useCallback } from 'react';
import './ChangeExplorer.css';

const ChangeExplorer = ({ diffResult, onNodeSelect, onFilterChange }) => {
  const [activeView, setActiveView] = useState('structural');
  const [structFilter, setStructFilter] = useState('ALL');
  const [nrmseThreshold, setNrmseThreshold] = useState(0.0);
  const [selectedNode, setSelectedNode] = useState(null);
  const [sortBy, setSortBy] = useState('nrmse');
  const [sortDesc, setSortDesc] = useState(true);

  const nodes = useMemo(() => {
    if (!diffResult?.nodes) return [];
    return Object.entries(diffResult.nodes).map(([id, data]) => ({
      id,
      ...data,
      nrmse: data.change?.nrmse ?? null,
      delta_mu: data.change?.delta_mu ?? null,
      sigma_ratio: data.change?.sigma_ratio ?? null,
      delta_struct: data.change?.delta_struct ?? 'UNKNOWN',
      transition_timestamp: data.change?.transition_timestamp ?? null,
    }));
  }, [diffResult]);

  const filteredNodes = useMemo(() => {
    let filtered = nodes;
    if (structFilter !== 'ALL') {
      filtered = filtered.filter(n => n.delta_struct === structFilter);
    }
    if (nrmseThreshold > 0) {
      filtered = filtered.filter(n =>
        n.delta_struct !== 'PERSISTED' || (n.nrmse !== null && n.nrmse >= nrmseThreshold)
      );
    }
    filtered.sort((a, b) => {
      let va = a[sortBy] ?? -Infinity;
      let vb = b[sortBy] ?? -Infinity;
      return sortDesc ? vb - va : va - vb;
    });
    return filtered;
  }, [nodes, structFilter, nrmseThreshold, sortBy, sortDesc]);

  const summary = diffResult?.summary ?? {};
  const cpg = diffResult?.cpg ?? null;

  const handleNodeClick = useCallback((nodeId) => {
    setSelectedNode(nodeId);
    onNodeSelect?.(nodeId);
  }, [onNodeSelect]);

  const handleFilterChange = useCallback((filter) => {
    setStructFilter(filter);
    onFilterChange?.({ structFilter: filter, nrmseThreshold });
  }, [onFilterChange, nrmseThreshold]);

  const counts = useMemo(() => {
    const c = { ADDED: 0, REMOVED: 0, PERSISTED: 0, total: 0 };
    nodes.forEach(n => {
      c[n.delta_struct] = (c[n.delta_struct] || 0) + 1;
      c.total++;
    });
    return c;
  }, [nodes]);

  return (
    <div className="change-explorer">
      <div className="ce-header">
        <h3>Change Explorer</h3>
        
      </div>

      <div className="ce-tabs">
        {['structural', 'signal', 'summary'].map(view => (
          <button key={view}
            className={`ce-tab ${activeView === view ? 'active' : ''}`}
            onClick={() => setActiveView(view)}>
            {view.charAt(0).toUpperCase() + view.slice(1)}
          </button>
        ))}
      </div>

      {activeView === 'structural' && (
        <div className="ce-structural">
          <div className="ce-filter-bar">
            {['ALL', 'ADDED', 'REMOVED', 'PERSISTED'].map(f => (
              <button key={f}
                className={`ce-filter-btn ${f.toLowerCase()} ${structFilter === f ? 'active' : ''}`}
                onClick={() => handleFilterChange(f)}>
                {f} <span className="ce-badge">{f === 'ALL' ? counts.total : counts[f] ?? 0}</span>
              </button>
            ))}
          </div>

       

          <div className="ce-entity-list">
            {filteredNodes.map(node => (
              <div key={node.id}
                className={`ce-entity-card ${node.delta_struct.toLowerCase()} ${selectedNode === node.id ? 'selected' : ''}`}
                onClick={() => handleNodeClick(node.id)}>
                <div className="ce-entity-header">
                  <span className={`ce-status-badge ${node.delta_struct.toLowerCase()}`}>
                    {node.delta_struct}
                  </span>
                  <span className="ce-entity-id">{node.id}</span>
                </div>
                {node.delta_struct === 'PERSISTED' && node.nrmse !== null && (
                  <div className="ce-entity-metrics">
                    <span>nRMSE: {node.nrmse.toFixed(3)}</span>
                    {node.delta_mu !== null && (
                      <span>&Delta;&mu;: {node.delta_mu >= 0 ? '+' : ''}{node.delta_mu.toFixed(1)}</span>
                    )}
                  </div>
                )}
                {node.delta_struct === 'ADDED' && node.transition_timestamp && (
                  <div className="ce-entity-metrics">
                    <span>Transition: {new Date(node.transition_timestamp).toLocaleDateString()}</span>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {activeView === 'signal' && (
        <div className="ce-signal">
          {selectedNode ? (
            <div className="ce-signal-detail">
              <h4>Sensor {selectedNode}</h4>
              <div className="ce-signal-chart-placeholder">
                <p>Time-series comparison chart renders here via ECharts.</p>
              </div>
              {(() => {
                const node = nodes.find(n => n.id === selectedNode);
                if (!node || node.delta_struct !== 'PERSISTED') return null;
                return (
                  <div className="ce-signal-metrics-detail">
                    <table><tbody>
                      <tr><td>nRMSE</td><td>{node.nrmse?.toFixed(4) ?? 'N/A'}</td></tr>
                      <tr><td>Mean shift</td><td>{node.delta_mu?.toFixed(2) ?? 'N/A'}</td></tr>
                      <tr><td>Volatility ratio</td><td>{node.sigma_ratio?.toFixed(3) ?? 'N/A'}</td></tr>
                    </tbody></table>
                  </div>
                );
              })()}
            </div>
          ) : (
            <div className="ce-signal-empty">
              <p>Click a sensor in the Structural view to compare time-series.</p>
            </div>
          )}
        </div>
      )}

      {activeView === 'summary' && (
        <div className="ce-summary">
          <div className="ce-summary-section">
            <h4>Structural Change</h4>
            <div className="ce-metric-row"><span className="ce-metric-label">Added</span><span className="ce-metric-value added">{summary.added ?? 0}</span></div>
            <div className="ce-metric-row"><span className="ce-metric-label">Removed</span><span className="ce-metric-value removed">{summary.removed ?? 0}</span></div>
            <div className="ce-metric-row"><span className="ce-metric-label">Persisted</span><span className="ce-metric-value persisted">{summary.persisted ?? 0}</span></div>
            <div className="ce-metric-row"><span className="ce-metric-label">Jaccard</span><span className="ce-metric-value">{(summary.jaccard_nodes ?? 0).toFixed(3)}</span></div>
          </div>

          <div className="ce-summary-section">
            <h4>Signal Change</h4>
            <div className="ce-metric-row"><span className="ce-metric-label">Avg nRMSE</span><span className="ce-metric-value">{(summary.avg_nrmse ?? 0).toFixed(3)}</span></div>
            <div className="ce-metric-row"><span className="ce-metric-label">Max nRMSE</span><span className="ce-metric-value">{(summary.max_nrmse ?? 0).toFixed(3)}</span></div>
          </div>

          {cpg && (
            <div className="ce-summary-section">
              <h4>Change Propagation</h4>
              <div className="ce-metric-row"><span className="ce-metric-label">Roots</span><span className="ce-metric-value">{summary.cpg_root_count ?? 0}</span></div>
              <div className="ce-metric-row"><span className="ce-metric-label">Max depth</span><span className="ce-metric-value">{summary.cpg_max_depth ?? 0} hops</span></div>
              <div className="ce-metric-row"><span className="ce-metric-label">Avg delay</span><span className="ce-metric-value">{(summary.cpg_avg_delay_days ?? 0).toFixed(1)} days</span></div>
              {cpg.roots && cpg.roots.length > 0 && (
                <div className="ce-cpg-roots">
                  <h5>Change Originators</h5>
                  {cpg.roots.map(rootId => (
                    <button key={rootId} className="ce-root-btn"
                      onClick={() => handleNodeClick(rootId)}>
                      {rootId}
                    </button>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default ChangeExplorer;
