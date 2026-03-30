import React from 'react';
import './MetadataPanel.css';

const MetadataPanel = ({ element }) => {
  if (!element) {
    return (
      <div className="metadata-panel">
        <div className="panel-title">Properties</div>
        <p className="no-selection">Select a node or edge to view properties</p>
      </div>
    );
  }

  // Get temporal properties from correct location
  const getTemporalProps = () => {
    if (element.temporal_properties && typeof element.temporal_properties === 'object') {
      return element.temporal_properties;
    }
    if (element.static_properties?.temporal_properties) {
      return element.static_properties.temporal_properties;
    }
    return {};
  };

  // Get static properties, excluding temporal_properties and diff descriptor fields
  const getStaticProps = () => {
    const source = element.static_properties || {};
    const excludeKeys = new Set(['temporal_properties', 'diffStatus', 'nrmse', 'delta_mu', 'sigma_ratio', 'delta_struct', 'nRMSE', 'delta_slope']);
    const result = {};
    
    Object.entries(source).forEach(([key, value]) => {
      if (excludeKeys.has(key)) return;
      if (typeof value === 'object' && value !== null) return;
      result[key] = value;
    });
    
    return result;
  };

  const temporalProps = getTemporalProps();
  const staticProps = getStaticProps();
  const hasTemporalProps = Object.keys(temporalProps).length > 0;
  const hasStaticProps = Object.keys(staticProps).length > 0;

  return (
    <div className="metadata-panel">
      <div className="panel-title">Properties</div>
      
      {/* Element Badge */}
      <div className="element-header">
        <span className={`type-badge ${element.type}`}>
          {element.type === 'node' ? '◉' : '→'} {element.type}
        </span>
        <span className="element-id">{element.label || element.oid || element.id}</span>
      </div>

      {/* Basic Info */}
      <div className="section">
        <div className="section-title">Identification</div>
        <div className="prop-row">
          <span className="prop-key">ID</span>
          <span className="prop-value">{element.oid || element.id}</span>
        </div>
        {element.label && (
          <div className="prop-row">
            <span className="prop-key">Label</span>
            <span className="prop-value">{element.label}</span>
          </div>
        )}
        {element.type === 'edge' && (
          <>
            <div className="prop-row">
              <span className="prop-key">Source</span>
              <span className="prop-value">{element.source}</span>
            </div>
            <div className="prop-row">
              <span className="prop-key">Target</span>
              <span className="prop-value">{element.target}</span>
            </div>
          </>
        )}
      </div>

      {/* Diff Status */}
      {element.diffStatus && (
        <div className="section">
          <div className="section-title">Change Descriptor</div>
          <div className="prop-row">
            <span className="prop-key">Struct Change</span>
            <span className="prop-value" style={{
              color: element.diffStatus.toLowerCase() === 'added' ? '#059669' :
                     element.diffStatus.toLowerCase() === 'removed' ? '#dc2626' :
                     element.diffStatus.toLowerCase() === 'persisted' ? '#2563eb' : '#5d6d7e',
              fontWeight: 700
            }}>{element.diffStatus}</span>
          </div>
          {element.nrmse != null && (
            <div className="prop-row">
              <span className="prop-key">Divergence</span>
              <span className="prop-value">{element.nrmse.toFixed(4)}</span>
            </div>
          )}
          {element.delta_mu != null && (
            <div className="prop-row">
              <span className="prop-key">Level Shift</span>
              <span className="prop-value">{element.delta_mu.toFixed(4)}</span>
            </div>
          )}
          {element.sigma_ratio != null && (
            <div className="prop-row">
              <span className="prop-key">Volatility Change</span>
              <span className="prop-value">{element.sigma_ratio.toFixed(4)}</span>
            </div>
          )}
        </div>
      )}

      {/* Temporal Validity */}
      {(element.start_time || element.end_time) && (
        <div className="section">
          <div className="section-title">Temporal Validity</div>
          {element.start_time && (
            <div className="prop-row">
              <span className="prop-key">Start</span>
              <span className="prop-value time">{element.start_time}</span>
            </div>
          )}
          {element.end_time && (
            <div className="prop-row">
              <span className="prop-key">End</span>
              <span className="prop-value time">{element.end_time}</span>
            </div>
          )}
        </div>
      )}

      {/* Static Properties */}
      {hasStaticProps && (
        <div className="section">
          <div className="section-title">Static Properties</div>
          {Object.entries(staticProps).map(([key, value]) => (
            <div key={key} className="prop-row">
              <span className="prop-key">{key}</span>
              <span className="prop-value">
                {typeof value === 'number' ? 
                  (Number.isInteger(value) ? value : value.toFixed(4)) : 
                  String(value)}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Temporal Properties (Time Series) */}
      {hasTemporalProps && (
        <div className="section">
          <div className="section-title">Temporal Properties (Time Series)</div>
          <div className="temporal-list">
            {Object.entries(temporalProps).map(([varName, tsId]) => (
              <div key={varName} className="temporal-item">
                <span className="ts-var">{varName}</span>
                <span className="ts-id">{tsId}</span>
              </div>
            ))}
          </div>
          <p className="ts-hint">↓ View time series below</p>
        </div>
      )}

      {/* No properties message */}
      {!hasStaticProps && !hasTemporalProps && (
        <div className="section">
          <p className="no-props">No properties available</p>
        </div>
      )}
    </div>
  );
};

export default MetadataPanel;
