import React, { useState, useEffect, useRef, useCallback, memo, useMemo } from 'react';
import * as echarts from 'echarts';
import './TimeSeriesPanel.css';


const DIFF_CHART_COLORS = {
  added: { line: '#16a34a', area: 'rgba(22, 163, 74, 0.25)', areaEnd: 'rgba(22, 163, 74, 0.02)' },
  removed: { line: '#dc2626', area: 'rgba(220, 38, 38, 0.25)', areaEnd: 'rgba(220, 38, 38, 0.02)' },
  persisted: { line: '#2563eb', area: 'rgba(37, 99, 235, 0.25)', areaEnd: 'rgba(37, 99, 235, 0.02)' },
};
const DEFAULT_CHART_COLOR = { line: '#3b82f6', area: 'rgba(59, 130, 246, 0.25)', areaEnd: 'rgba(59, 130, 246, 0.02)' };

const TimeSeriesPanel = memo(function TimeSeriesPanel({ element, data }) {
  const [selectedTsIds, setSelectedTsIds] = useState([]);
  const [viewMode, setViewMode] = useState('chart');
  const [fullscreenTs, setFullscreenTs] = useState(null);
  const [granularity, setGranularity] = useState(1);
  
  const chartRefs = useRef({});
  const chartInstances = useRef({});
  const fullscreenChartRef = useRef(null);
  const fullscreenChartInstance = useRef(null);
  const containerRef = useRef(null);
  const chartColor = DIFF_CHART_COLORS[(element?.diffStatus || '').toLowerCase()] || DEFAULT_CHART_COLOR;

  // Get temporal properties from element
  const getTemporalProperties = useCallback(() => {
    if (!element) return {};
    
    if (element.temporal_properties && typeof element.temporal_properties === 'object') {
      return element.temporal_properties;
    }
    if (element.static_properties?.temporal_properties) {
      return element.static_properties.temporal_properties;
    }
    return {};
  }, [element]);

  /**
   * Extract numeric value from any format - very permissive
   */
  const toNumber = (val) => {
    // Unwrap arrays recursively
    let v = val;
    while (Array.isArray(v) && v.length > 0) {
      v = v[0];
    }
    
    // Already a number
    if (typeof v === 'number' && isFinite(v)) return v;
    
    // Parse strings
    if (typeof v === 'string') {
      const n = parseFloat(v);
      return isFinite(n) ? n : null;
    }
    
    // Try Number conversion
    const n = Number(v);
    return isFinite(n) ? n : null;
  };

 
  const normalizeTimeseriesData = useCallback((ts) => {
    if (!ts) return { data: [], stats: null };
    
    const result = [];
    let min = Infinity, max = -Infinity, sum = 0, count = 0;
    
    const addPoint = (time, rawValue) => {
      const value = toNumber(rawValue);
      if (value !== null) {
        result.push({ time, value });
        if (value < min) min = value;
        if (value > max) max = value;
        sum += value;
        count++;
      }
    };
    
    // PRIORITY 1: API format {timestamps: [...], data: [...]}
    if (ts.timestamps && Array.isArray(ts.timestamps) && ts.timestamps.length > 0) {
      const dataArray = ts.data || [];
      for (let i = 0; i < ts.timestamps.length; i++) {
        addPoint(ts.timestamps[i], dataArray[i]);
      }
    }
    // PRIORITY 2: Already [{time, value}, ...] or [{timestamp, value}, ...]
    else if (ts.data && Array.isArray(ts.data) && ts.data.length > 0) {
      for (const item of ts.data) {
        if (item && typeof item === 'object') {
          const time = item.time || item.timestamp || item.ts;
          const value = item.value ?? item.val ?? item.v;
          if (time !== undefined) {
            addPoint(time, value);
          }
        } else {
          // Raw number array
          addPoint(new Date(Date.now() + result.length * 3600000).toISOString(), item);
        }
      }
    }
    
    const stats = count > 0 ? {
      min,
      max,
      avg: sum / count,
      points: count
    } : null;
    
    return { data: result, stats };
  }, []);

  // Get available timeseries that have actual data
  const getAvailableTimeseries = useCallback(() => {
    if (!element || !data?.timeseries) {
      return [];
    }
    
    const temporalProps = getTemporalProperties();
    const timeseries = data.timeseries;
    
   
    
    const available = [];
    
    for (const [propName, tsId] of Object.entries(temporalProps)) {
      if (typeof tsId !== 'string' || !tsId) continue;
      
      const ts = timeseries[tsId];
     
      
      
      
      // Normalize the data and get stats in one pass
      const { data: normalizedData, stats } = normalizeTimeseriesData(ts);
      
      
      if (normalizedData.length === 0) continue;
      

      
      available.push({
        id: tsId,
        name: propName,
        ts: ts,
        normalizedData: normalizedData,
        stats: stats  // Pre-calculated stats
      });
    }
    
    return available;
  }, [element, data?.timeseries, getTemporalProperties, normalizeTimeseriesData]);

  // Memoize available timeseries
  const availableTs = useMemo(() => getAvailableTimeseries(), [getAvailableTimeseries]);

  // Update selection when element changes
  useEffect(() => {
    setSelectedTsIds(availableTs.map(item => item.id));
    setGranularity(1);
  }, [availableTs]);

  // Apply granularity to data
  const applyGranularity = (tsData, gran) => {
    if (gran <= 1 || !tsData?.length) return tsData;
    return tsData.filter((_, idx) => idx % gran === 0);
  };

  // Check if this is a persisted node in a diff view with period ranges
  const isPersisted = (element?.diffStatus || '').toLowerCase() === 'persisted';
  const isDiffView = data?.metadata?.isDiff || data?.isDiff || false;
  const showSplitPeriods = isPersisted && isDiffView && data?.metadata?.period_1_start;
  const p1Start = showSplitPeriods ? new Date(data.metadata.period_1_start).getTime() : null;
  const p1End = showSplitPeriods ? new Date(data.metadata.period_1_end).getTime() : null;
  const p2Start = showSplitPeriods ? new Date(data.metadata.period_2_start).getTime() : null;
  const p2End = showSplitPeriods ? new Date(data.metadata.period_2_end).getTime() : null;


  const initChart = (container, displayData, color, title) => {
    const chart = echarts.init(container);
    chart.setOption({
      backgroundColor: 'transparent',
      grid: { left: 50, right: 15, bottom: 30, top: title ? 25 : 10 },
      title: title ? { text: title, left: 'center', textStyle: { fontSize: 11, color: '#5d6d7e', fontWeight: 500 } } : undefined,
      tooltip: {
        trigger: 'axis', backgroundColor: '#fff', borderColor: '#e2e8f0',
        textStyle: { color: '#1f2937', fontSize: 11 },
        formatter: (params) => {
          if (!params?.[0]) return '';
          const d = params[0];
          const val = d.data?.[1];
          return `<strong>${new Date(d.data[0]).toLocaleString()}</strong><br/>Value: ${typeof val === 'number' ? val.toFixed(2) : (val ?? '-')}`;
        }
      },
      xAxis: { type: 'time', boundaryGap: false, axisLine: { lineStyle: { color: '#e2e8f0' } }, axisLabel: { color: '#64748b', fontSize: 9, formatter: '{MM}-{dd} {HH}:{mm}' }, splitLine: { show: false } },
      yAxis: { type: 'value', axisLine: { show: false }, axisLabel: { color: '#64748b', fontSize: 9 }, splitLine: { lineStyle: { color: '#f1f5f9' } } },
      dataZoom: [{ type: 'inside', start: 0, end: 100 }],
      series: [{ type: 'line', data: displayData.map(d => [d.time, d.value]), smooth: true, showSymbol: displayData.length < 50, symbolSize: 4,
        lineStyle: { width: 2, color: color.line },
        areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: color.area }, { offset: 1, color: color.areaEnd }] } }
      }]
    });
    return chart;
  };

  // Render charts
  useEffect(() => {
    if (viewMode !== 'chart' || selectedTsIds.length === 0) return;

    const timer = setTimeout(() => {
      availableTs.filter(item => selectedTsIds.includes(item.id)).forEach(item => {
        if (!item.normalizedData?.length) return;

        if (showSplitPeriods) {
          // Split into two charts for persisted nodes
          const p1Container = chartRefs.current[`${item.id}_p1`];
          const p2Container = chartRefs.current[`${item.id}_p2`];
          const p1Data = applyGranularity(item.normalizedData.filter(d => d.time >= p1Start && d.time <= p1End), granularity);
          const p2Data = applyGranularity(item.normalizedData.filter(d => d.time >= p2Start && d.time <= p2End), granularity);

          if (chartInstances.current[`${item.id}_p1`]) try { chartInstances.current[`${item.id}_p1`].dispose(); } catch(e) {}
          if (chartInstances.current[`${item.id}_p2`]) try { chartInstances.current[`${item.id}_p2`].dispose(); } catch(e) {}

          const p1Color = { line: '#2563eb', area: 'rgba(37,99,235,0.25)', areaEnd: 'rgba(37,99,235,0.02)' };
          const p2Color = { line: '#dc2626', area: 'rgba(220,38,38,0.25)', areaEnd: 'rgba(220,38,38,0.02)' };

          if (p1Container && p1Data.length) {
            const r = p1Container.getBoundingClientRect();
            if (r.width > 10) chartInstances.current[`${item.id}_p1`] = initChart(p1Container, p1Data, p1Color, data.metadata.period_1);
          }
          if (p2Container && p2Data.length) {
            const r = p2Container.getBoundingClientRect();
            if (r.width > 10) chartInstances.current[`${item.id}_p2`] = initChart(p2Container, p2Data, p2Color, data.metadata.period_2);
          }
        } else {
          // Single chart
          const container = chartRefs.current[item.id];
          if (!container) return;
          const rect = container.getBoundingClientRect();
          if (rect.width < 10 || rect.height < 10) return;
          if (chartInstances.current[item.id]) try { chartInstances.current[item.id].dispose(); } catch(e) {}
          chartInstances.current[item.id] = initChart(container, applyGranularity(item.normalizedData, granularity), chartColor);
        }
      });
    }, 100);

    return () => clearTimeout(timer);
  }, [viewMode, selectedTsIds, availableTs, granularity, showSplitPeriods]);

  // Fullscreen chart
  useEffect(() => {
    if (!fullscreenTs || !fullscreenChartRef.current) return;
    
    const item = availableTs.find(t => t.id === fullscreenTs.id);
    if (!item?.normalizedData?.length) return;

    if (fullscreenChartInstance.current) {
      try { fullscreenChartInstance.current.dispose(); } catch (e) {}
    }

    const timer = setTimeout(() => {
      try {
        const chart = echarts.init(fullscreenChartRef.current);
        fullscreenChartInstance.current = chart;

        const displayData = applyGranularity(item.normalizedData, granularity);

        chart.setOption({
          backgroundColor: '#fff',
          grid: { left: 60, right: 30, bottom: 70, top: 50 },
          title: {
            text: fullscreenTs.name,
            left: 'center',
            textStyle: { fontSize: 16, color: '#1e293b' }
          },
          tooltip: {
            trigger: 'axis',
            backgroundColor: '#fff',
            borderColor: '#e2e8f0',
            textStyle: { color: '#1f2937', fontSize: 12 }
          },
          xAxis: {
            type: 'time',
            boundaryGap: false,
            axisLine: { lineStyle: { color: '#e2e8f0' } },
            axisLabel: { color: '#64748b', fontSize: 11 }
          },
          yAxis: {
            type: 'value',
            axisLine: { show: false },
            axisLabel: { color: '#64748b', fontSize: 11 },
            splitLine: { lineStyle: { color: '#f1f5f9' } }
          },
          dataZoom: [
            { type: 'slider', start: 0, end: 100, height: 25 },
            { type: 'inside', start: 0, end: 100 }
          ],
          series: [{
            type: 'line',
            data: displayData.map(d => [d.time, d.value]),
            smooth: true,
            showSymbol: displayData.length < 100,
            symbolSize: 5,
            lineStyle: { width: 2, color: chartColor.line },
            areaStyle: {
              color: {
                type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
                colorStops: [
                  { offset: 0, color: chartColor.area },
                  { offset: 1, color: chartColor.areaEnd }
                ]
              }
            }
          }]
        });
      } catch (e) {
        console.error('Fullscreen chart error:', e);
      }
    }, 100);

    return () => clearTimeout(timer);
  }, [fullscreenTs, availableTs, granularity]);

  // Resize handler
  useEffect(() => {
    const handleResize = () => {
      Object.values(chartInstances.current).forEach(chart => {
        try { chart?.resize(); } catch (e) {}
      });
      if (fullscreenChartInstance.current) {
        try { fullscreenChartInstance.current.resize(); } catch (e) {}
      }
    };
    
    window.addEventListener('resize', handleResize);
    
    let resizeObserver = null;
    if (containerRef.current) {
      resizeObserver = new ResizeObserver(handleResize);
      resizeObserver.observe(containerRef.current);
    }
    
    return () => {
      window.removeEventListener('resize', handleResize);
      resizeObserver?.disconnect();
    };
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      Object.values(chartInstances.current).forEach(chart => {
        try { chart?.dispose(); } catch (e) {}
      });
      if (fullscreenChartInstance.current) {
        try { fullscreenChartInstance.current.dispose(); } catch (e) {}
      }
    };
  }, []);

  // Download as CSV
  const downloadCSV = (tsItem) => {
    if (!tsItem?.normalizedData?.length) return;

    const csv = ['timestamp,value'];
    tsItem.normalizedData.forEach(d => {
      csv.push(`${new Date(d.time).toISOString()},${d.value}`);
    });

    const blob = new Blob([csv.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${element?.oid || 'timeseries'}_${tsItem.name}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // Download as PNG
  const downloadPNG = (tsId) => {
    const chart = chartInstances.current[tsId] || fullscreenChartInstance.current;
    if (!chart) return;

    const url = chart.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: '#fff' });
    const a = document.createElement('a');
    a.href = url;
    a.download = `${element?.oid || 'timeseries'}_chart.png`;
    a.click();
  };

  const handleTsToggle = useCallback((tsId) => {
    setSelectedTsIds(prev => 
      prev.includes(tsId) ? prev.filter(id => id !== tsId) : [...prev, tsId]
    );
  }, []);

  // Format stat value
  const formatStat = (value) => {
    if (value === undefined || value === null || typeof value !== 'number' || !isFinite(value)) {
      return '-';
    }
    if (Math.abs(value) >= 10000) return value.toFixed(0);
    if (Math.abs(value) >= 1000) return value.toFixed(1);
    if (Math.abs(value) >= 100) return value.toFixed(1);
    if (Math.abs(value) >= 10) return value.toFixed(2);
    return value.toFixed(2);
  };

  // No element selected
  if (!element) {
    return (
      <div className="ts-panel" ref={containerRef}>
        <div className="ts-header">
          <h3>Time Series</h3>
        </div>
        <div className="ts-empty">Select a node or edge to view time series</div>
      </div>
    );
  }

  // No data
  if (availableTs.length === 0) {
    return (
      <div className="ts-panel ts-panel-empty" ref={containerRef}>
        <div className="ts-header">
          <div className="ts-title">
            <h3>Time Series</h3>
            <span className="ts-element">
              <span className="el-type">{element.type?.toUpperCase()}</span>
              {element.label || element.oid}
            </span>
          </div>
        </div>
        <div className="ts-empty-minimal">No time series data</div>
      </div>
    );
  }

  // Count selected items for CSS class
  const selectedCount = availableTs.filter(item => selectedTsIds.includes(item.id)).length;

  // Has data
  return (
    <div className="ts-panel" ref={containerRef}>
      {/* Fullscreen Modal */}
      {fullscreenTs && (
        <div className="ts-fullscreen-overlay" onClick={() => setFullscreenTs(null)}>
          <div className="ts-fullscreen-modal" onClick={e => e.stopPropagation()}>
            <div className="ts-fullscreen-header">
              <span>{fullscreenTs.name}</span>
              <div className="ts-fullscreen-actions">
                <button onClick={() => downloadPNG(fullscreenTs.id)} title="Download PNG">PNG</button>
                <button onClick={() => downloadCSV(availableTs.find(t => t.id === fullscreenTs.id))} title="Download CSV">CSV</button>
                <button onClick={() => setFullscreenTs(null)}>Close</button>
              </div>
            </div>
            <div ref={fullscreenChartRef} className="ts-fullscreen-chart" />
            {/* Fullscreen Stats */}
            {(() => {
              const item = availableTs.find(t => t.id === fullscreenTs.id);
              const stats = item?.stats;
              if (!stats) return null;
              return (
                <div className="ts-fullscreen-stats">
                  <div className="stat">
                    <span className="stat-label">MIN</span>
                    <span className="stat-value">{formatStat(stats.min)}</span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">MAX</span>
                    <span className="stat-value">{formatStat(stats.max)}</span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">AVG</span>
                    <span className="stat-value">{formatStat(stats.avg)}</span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">POINTS</span>
                    <span className="stat-value">{stats.points}</span>
                  </div>
                </div>
              );
            })()}
          </div>
        </div>
      )}

      <div className="ts-header">
        <div className="ts-title">
          <h3>Time Series</h3>
          <span className="ts-element">
            <span className="el-type">{element.type?.toUpperCase()}</span>
            {element.label || element.oid}
          </span>
        </div>
        <div className="ts-controls">
          {/* Granularity Control */}
          <div className="ts-granularity">
            <button 
              onClick={() => setGranularity(g => Math.max(1, g - 1))}
              disabled={granularity <= 1}
              title="More detail"
            >
              +
            </button>
            <span title={`Showing every ${granularity === 1 ? '' : granularity + ' '}point${granularity > 1 ? 's' : ''}`}>
              {granularity === 1 ? 'All' : `1/${granularity}`}
            </span>
            <button 
              onClick={() => setGranularity(g => Math.min(10, g + 1))}
              disabled={granularity >= 10}
              title="Less detail"
            >
              -
            </button>
          </div>
          
          {/* View Toggle */}
          <div className="ts-toggle">
            <button className={viewMode === 'chart' ? 'active' : ''} onClick={() => setViewMode('chart')}>
              Chart
            </button>
            <button className={viewMode === 'table' ? 'active' : ''} onClick={() => setViewMode('table')}>
              Table
            </button>
          </div>
        </div>
      </div>

      <div className="ts-selectors">
        {availableTs.map(item => (
          <label key={item.id} className="ts-selector">
            <input
              type="checkbox"
              checked={selectedTsIds.includes(item.id)}
              onChange={() => handleTsToggle(item.id)}
            />
            <span>{item.name}</span>
            <span className="ts-count">({item.normalizedData?.length || 0})</span>
          </label>
        ))}
      </div>

      {selectedTsIds.length > 0 && viewMode === 'chart' && (
        <div className={`ts-grid ${selectedCount === 1 ? 'ts-grid-single' : ''}`}>
          {availableTs.filter(item => selectedTsIds.includes(item.id)).map(item => {
            const stats = item.stats;  // Use pre-calculated stats
            const displayCount = Math.ceil((item.normalizedData?.length || 0) / granularity);
            
            return (
              <div key={item.id} className={`ts-card ${selectedCount === 1 ? 'ts-card-single' : ''}`}>
                <div className="ts-card-header">
                  <span>{item.name}</span>
                  <div className="ts-card-actions">
                    <button onClick={() => setFullscreenTs(item)} title="Fullscreen">⛶</button>
                    <button onClick={() => downloadCSV(item)} title="Download CSV">CSV</button>
                  </div>
                </div>
                <div className="ts-card-chart">
                  {showSplitPeriods ? (
                    <div style={{ display: 'flex', width: '100%', height: '100%', gap: 4 }}>
                      <div ref={el => { chartRefs.current[`${item.id}_p1`] = el; }}
                        style={{ flex: 1, height: '100%' }} />
                      <div style={{ width: 1, background: '#e2e8f0' }} />
                      <div ref={el => { chartRefs.current[`${item.id}_p2`] = el; }}
                        style={{ flex: 1, height: '100%' }} />
                    </div>
                  ) : (
                    <div ref={el => { chartRefs.current[item.id] = el; }}
                      style={{ width: '100%', height: '100%' }} />
                  )}
                </div>
                {/* STATS ROW - Always visible */}
                <div className="ts-card-stats">
                  <div className="stat">
                    <span className="stat-label">MIN</span>
                    <span className="stat-value">{formatStat(stats?.min)}</span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">MAX</span>
                    <span className="stat-value">{formatStat(stats?.max)}</span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">AVG</span>
                    <span className="stat-value">{formatStat(stats?.avg)}</span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">PTS</span>
                    <span className="stat-value">{displayCount}</span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {selectedTsIds.length > 0 && viewMode === 'table' && (
        <div className="ts-tables">
          {availableTs.filter(item => selectedTsIds.includes(item.id)).map(item => {
            const stats = item.stats;
            return (
              <div key={item.id} className="ts-table-wrapper">
                <div className="ts-table-header">
                  <span>{item.name}</span>
                  <button onClick={() => downloadCSV(item)}>Download CSV</button>
                </div>
                {/* Stats in table view too */}
                {stats && (
                  <div className="ts-table-stats">
                    <span>Min: {formatStat(stats.min)}</span>
                    <span>Max: {formatStat(stats.max)}</span>
                    <span>Avg: {formatStat(stats.avg)}</span>
                    <span>Points: {stats.points}</span>
                  </div>
                )}
                <div className="ts-table-scroll">
                  <table className="ts-table">
                    <thead>
                      <tr><th>Time</th><th>Value</th></tr>
                    </thead>
                    <tbody>
                      {applyGranularity(item.normalizedData, granularity).slice(0, 100).map((d, idx) => (
                        <tr key={idx}>
                          <td>{new Date(d.time).toLocaleString()}</td>
                          <td>{typeof d.value === 'number' ? d.value.toFixed(2) : '-'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {item.normalizedData.length > 100 && (
                  <div className="ts-table-more">+ {item.normalizedData.length - 100} more</div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
});

export default TimeSeriesPanel;
