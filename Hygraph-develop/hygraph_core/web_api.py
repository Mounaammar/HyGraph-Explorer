"""
HyGraph FastAPI Backend v4.2.0 - With Full Fluent API Support
"""

import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Literal, Union

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).parent.parent))
from hygraph_core.hygraph.hygraph import HyGraph
from hygraph_core.operators import SnapshotSequence, HyGraphDiff, TSGen
from hygraph_core.operators.hygraphDiff import HyGraphIntervalDiff
from hygraph_core.model.timeseries import TimeSeries, TimeSeriesMetadata



app = FastAPI(title="HyGraph Web API", version="4.3.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)





HG_CONNECTION = "postgresql://postgres:postgres@localhost:5433/hygraph"

try:
    hg = HyGraph(HG_CONNECTION, graph_name="hygraph")
    print("HyGraph initialized")
except Exception as e:
    print(f"Failed: {e}")
    hg = None

TSID_METADATA: Dict[str, Dict[str, Any]] = {}
SEQUENCE_CACHE: Dict[str, SnapshotSequence] = {}
SAVED_SUBHYGRAPHS: Dict[str, Dict[str, Any]] = {}

# Graph data cache to avoid reloading on every request
GRAPH_CACHE: Dict[str, Any] = {
    'nodes': None,
    'edges': None,
    'metadata': None,
    'loaded_at': None,
    'loading': False  # Prevent concurrent loads
}



class BatchTimeSeriesRequest(BaseModel):
    ts_ids: List[str]

class SnapshotAtRequest(BaseModel):
    timestamp: str
    mode: str = "graph"
    label: Optional[str] = None

class SnapshotNodeDegreeRequest(BaseModel):
    timestamp: Optional[str] = None  # For point-in-time query
    start: Optional[str] = None  # For interval query
    end: Optional[str] = None    # For interval query
    mode: str = "graph"
    label: Optional[str] = None
    node_id: Optional[str] = None
    weight: Optional[str] = None
    direction: Literal["in", "out", "both"] = "both"

class SnapshotIntervalRequest(BaseModel):
    start: str
    end: str
    mode: str = "graph"
    ts_handling: Optional[str] = "aggregate"
    aggregation_fn: str = "avg"

class SnapshotSequenceRequest(BaseModel):
    start: str
    end: str
    granularity: str
    mode: str = "graph"

class TSGenRequest(BaseModel):
    start: str
    end: str
    granularity: str
    metric: str
    label: Optional[str] = None

class SubGraphRequest(BaseModel):
    label: str
    property: Optional[str] = None
    operator: str = ">="
    value: Optional[float] = None

class SaveSubHyGraphRequest(BaseModel):
    name: str
    description: Optional[str] = None
    node_ids: List[str]
    filter_query: Optional[str] = None
    metadata: Optional[dict] = None

class UpdateSubHyGraphRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    node_ids: Optional[List[str]] = None
    metadata: Optional[dict] = None

class DiffRequest(BaseModel):
    timestamp1: str
    timestamp2: str
    mode: str = "hybrid"


# Diff Query Models - For querying diff results
class DiffTSChangeFilter(BaseModel):
    """Time series change filter for diff queries"""
    ts_name: str
    aggregation: str = 'mean'  # mean, min, max, std, sum, median
    operator: str = '<'
    value: float


class DiffPropertyChangeFilter(BaseModel):
    """Property change filter for diff queries"""
    property: str
    operator: str = '>'
    value: float


class DiffDegreeChangeFilter(BaseModel):
    """Degree change filter for diff queries"""
    direction: str = 'both'  # in, out, both
    operator: str = '>'
    value: int


class DiffQueryRequest(BaseModel):
    """
    Query diff results using fluent API.
    
    Allows filtering changed, added, or removed entities based on:
    - Property changes
    - Time series aggregation changes
    - Degree changes (for nodes)
    
    Example:
    {
        "timestamp1": "2024-06-01T08:00:00",
        "timestamp2": "2024-06-01T10:00:00",
        "query_type": "changed_nodes",
        "label": "Station",
        "ts_change_filters": [
            {"ts_name": "num_bikes", "aggregation": "mean", "operator": "<", "value": -5}
        ],
        "degree_change_filters": [
            {"direction": "in", "operator": ">", "value": 10}
        ]
    }
    """
    timestamp1: str
    timestamp2: str
    mode: str = "hybrid"
    query_type: str  # changed_nodes, added_nodes, removed_nodes, persistent_nodes, changed_edges, added_edges, removed_edges
    label: Optional[str] = None
    ts_change_filters: Optional[List[DiffTSChangeFilter]] = None
    property_change_filters: Optional[List[DiffPropertyChangeFilter]] = None
    degree_change_filters: Optional[List[DiffDegreeChangeFilter]] = None
    limit: Optional[int] = None
    order_by: Optional[str] = None  # property name or 'degree_change'
    order_desc: bool = False




class IntervalDiffRequest(BaseModel):
    """Request for comparing two interval snapshots with TS analysis"""
    start1: str  # Interval 1 start
    end1: str    # Interval 1 end
    start2: str  # Interval 2 start
    end2: str    # Interval 2 end
    mode: str = "hybrid"
    significance_threshold: float = 0.1


class IntervalDiffTSFilter(BaseModel):
    """TS comparison filter for interval diff queries"""
    ts_name: str
    filter_type: str  # mean_diff, std_diff, trend_changed, correlation, significant
    operator: Optional[str] = '<'  # For mean_diff, std_diff, correlation
    value: Optional[float] = None


class IntervalDiffQueryRequest(BaseModel):
    """
    Query interval diff results with TS comparison filters.
    
    Supports filtering by:
    - Mean difference (ts2_mean - ts1_mean)
    - Std difference (volatility change)
    - Trend changed (direction reversed)
    - Correlation (shape similarity between intervals)
    - Significant change (above threshold)
    
    Example:
    {
        "start1": "2024-06-01T06:00:00",
        "end1": "2024-06-01T09:00:00",
        "start2": "2024-06-01T17:00:00",
        "end2": "2024-06-01T20:00:00",
        "query_type": "changed_nodes",
        "label": "Station",
        "ts_filters": [
            {"ts_name": "num_bikes", "filter_type": "mean_diff", "operator": "<", "value": -5},
            {"ts_name": "num_bikes", "filter_type": "trend_changed"}
        ]
    }
    """
    start1: str
    end1: str
    start2: str
    end2: str
    mode: str = "hybrid"
    query_type: str = "changed_nodes"  # changed_nodes, added_nodes, removed_nodes, persistent_nodes
    label: Optional[str] = None
    ts_filters: Optional[List[IntervalDiffTSFilter]] = None
    degree_change_filters: Optional[List[dict]] = None
    limit: Optional[int] = None

# CRUD Models
class CreateNodeRequest(BaseModel):
    uid: str
    label: str
    properties: Dict[str, Any] = {}
    ts_properties: Dict[str, Any] = {}
    start_time: str
    end_time: str

class UpdateNodeRequest(BaseModel):
    uid: str
    properties: Dict[str, Any]

class DeleteNodeRequest(BaseModel):
    uid: str
    hard: bool = False

class CreateEdgeRequest(BaseModel):
    uid: str
    source: str
    target: str
    label: str
    properties: Dict[str, Any] = {}
    start_time: str
    end_time: str

class UpdateEdgeRequest(BaseModel):
    uid: str
    properties: Dict[str, Any]

class DeleteEdgeRequest(BaseModel):
    uid: str
    hard: bool = False



class TSConstraint(BaseModel):
    """Time series constraint for filtering"""
    property: str
    type: str  # 'aggregation', 'value_at', 'first', 'last', 'range', 'change', 'pattern', 'similar'
    
    # For aggregation type
    aggregation: Optional[str] = 'mean'  # mean, min, max, sum, std, count, median, range
    operator: Optional[str] = '<'
    value: Optional[float] = None
    
    # For value_at type
    timestamp: Optional[str] = None
    
    # For pattern type (where_ts_pattern)
    template: Optional[str] = None  # spike, drop, increasing, decreasing, peak, valley, step_up, step_down, oscillation
    pattern_length: Optional[int] = 10
    threshold: Optional[float] = None
    
    # For similar type (where_ts_similar)
    reference_ts: Optional[dict] = None  # {timestamps: [], values: []}
    features: Optional[dict] = None  # {mean: X, std: Y, ...}
    similarity_template: Optional[str] = None  # increasing, decreasing, stable, spike, drop
    tolerance: Optional[dict] = None  # {mean: 0.1, std: 0.1}
    
    # For bucketed type (where_ts_bucketed)
    interval: Optional[str] = None  # '1 hour', '1 day', etc.
    condition: Optional[str] = 'all'  # all, any, majority, avg


class StaticConstraint(BaseModel):
    """Static property constraint"""
    property: str
    operator: str = '>='  # >=, <=, >, <, ==, !=
    value: Union[float, str, int]


class NodeQueryRequest(BaseModel):
    """Query nodes using fluent API - hg.query().nodes()"""
    label: Optional[str] = None
    id_contains: Optional[str] = None
    static_constraints: Optional[List[StaticConstraint]] = None
    ts_constraints: Optional[List[TSConstraint]] = None
    at_time: Optional[str] = None
    between: Optional[List[str]] = None  # [start, end]
    limit: Optional[int] = None  # No default limit


class EdgeQueryRequest(BaseModel):
    """Query edges using fluent API - hg.query().edges()"""
    label: Optional[str] = None
    id_contains: Optional[str] = None
    source_id: Optional[str] = None
    target_id: Optional[str] = None
    static_constraints: Optional[List[StaticConstraint]] = None
    ts_constraints: Optional[List[TSConstraint]] = None
    limit: Optional[int] = None  # No default limit


class TimeSeriesQueryRequest(BaseModel):
    """Query timeseries using fluent API - hg.query().timeseries()"""
    entity_id: str
    variable: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None


class PatternNodeDef(BaseModel):
    """Node definition in pattern matching"""
    variable: str  # n1, n2, etc.
    label: Optional[str] = None
    uid: Optional[str] = None  # Specific node ID
    static_constraints: Optional[List[StaticConstraint]] = None
    ts_constraints: Optional[List[TSConstraint]] = None


class PatternEdgeDef(BaseModel):
    """Edge definition in pattern matching"""
    variable: str  # e1, e2, etc.
    source_var: str  # n1
    target_var: str  # n2
    label: Optional[str] = None
    static_constraints: Optional[List[StaticConstraint]] = None
    ts_constraints: Optional[List[TSConstraint]] = None


class CrossConstraint(BaseModel):
    """Cross-entity constraint: n2.capacity < n1.capacity"""
    left: str  # "var.prop" or "var.prop:agg"
    operator: str  # <, >, <=, >=, ==, !=
    right: str  # "var.prop" or constant


class CrossTSConstraint(BaseModel):
    """Cross-entity TS constraint: n1.num_bikes correlates n2.num_bikes"""
    left: str  # "var.ts_prop"
    operator: str  # correlates, anti_correlates, similar_to, leads, lags, diverges_from, more_volatile, more_stable
    right: str  # "var.ts_prop"
    threshold: float = 0.7
    lag: int = 0


class PatternMatchRequest(BaseModel):
    """Full pattern matching: (n1)-[e]->(n2) with conditions"""
    nodes: List[PatternNodeDef]
    edges: Optional[List[PatternEdgeDef]] = None
    cross_constraints: Optional[List[CrossConstraint]] = None
    cross_ts_constraints: Optional[List[CrossTSConstraint]] = None
    limit: Optional[int] = None  # No default limit


class CreateTimeSeriesRequest(BaseModel):
    """Create a TimeSeries using hg.create_timeseries()"""
    tsid: Optional[str] = None
    owner_id: str
    owner_type: str = 'node'  # node or edge
    variable: str
    timestamps: List[str]
    values: List[float]
    description: Optional[str] = None
    units: Optional[str] = None


class CreateSequenceRequest(BaseModel):
    start: str
    end: str
    granularity: str = '1D'
    mode: str = 'graph'


class TSGenFromSequenceRequest(BaseModel):
    sequence_id: str
    scope: str = 'global'
    entity_type: str = 'nodes'
    metric: str = 'count'
    property_name: Optional[str] = None
    aggregation: str = 'avg'
    label: Optional[str] = None
    entity_id: Optional[str] = None
    direction: str = 'both'
    weight: Optional[str] = None
    source_node: Optional[str] = None
    target_node: Optional[str] = None
    directed: bool = True


class TSGenCombinedRequest(BaseModel):
    """Combined request that creates sequence and runs TSGen in one call"""
    # SnapshotSequence params
    start: str
    end: str
    granularity: str = '1D'
    mode: str = 'hybrid'
    # TSGen params
    scope: str = 'global'
    entity_type: str = 'nodes'
    metric: str = 'count'
    property_name: Optional[str] = None
    aggregation: str = 'avg'
    label: Optional[str] = None
    entity_id: Optional[str] = None
    direction: str = 'both'
    weight: Optional[str] = None
    source_node: Optional[str] = None
    target_node: Optional[str] = None
    directed: bool = True




def parse_granularity(granularity: str) -> timedelta:
    mapping = {
        '1H': timedelta(hours=1),
        '6H': timedelta(hours=6),
        '12H': timedelta(hours=12),
        '1D': timedelta(days=1),
        '7D': timedelta(days=7),
        '30D': timedelta(days=30),
        '1M': timedelta(days=30),
    }
    return mapping.get(granularity, timedelta(days=1))


def process_nodes_fast() -> List[Dict]:
    nodes = hg.query().nodes().execute()
    nodes_response = []

    for node in nodes:
        node_dict = node.to_dict()
        node_oid = str(node.oid)
        
        temporal_props = node_dict.get('temporal_properties', {})
        static_props = node_dict.get('static_properties', {})
        
        for var_name, tsid in temporal_props.items():
            TSID_METADATA[tsid] = {
                'variable': var_name,
                'owner_id': node_oid,
                'owner_type': 'node'
            }
        
        nodes_response.append({
            'oid': node_oid,
            'label': node_dict.get('label'),
            'start_time': str(node_dict.get('start_time', '')),
            'end_time': str(node_dict.get('end_time', '')),
            'static_properties': static_props,
            'temporal_properties': temporal_props,
        })

    return nodes_response


def process_edges_fast() -> List[Dict]:
    raw_edges = hg.age.query_edges()
    edges_response = []

    for edge_dict in raw_edges:
        edge_uid = edge_dict.get('uid', '')
        source_id = edge_dict.get('src_uid')
        target_id = edge_dict.get('dst_uid')
        if not source_id or not target_id:
            continue

        props = edge_dict.get('properties', {})
        temp_props = props.get('temporal_properties', {})
        static_props = {k: v for k, v in props.items() if k != 'temporal_properties'}
        
        for var_name, tsid in temp_props.items():
            TSID_METADATA[tsid] = {
                'variable': var_name,
                'owner_id': edge_uid,
                'owner_type': 'edge'
            }

        edges_response.append({
            'oid': edge_uid,
            'source': source_id,
            'target': target_id,
            'label': edge_dict.get('label', ''),
            'start_time': edge_dict.get('start_time', ''),
            'end_time': edge_dict.get('end_time', ''),
            'static_properties': static_props,
            'temporal_properties': temp_props,
        })

    return edges_response


def load_single_timeseries(tsid: str) -> Optional[Dict]:
    try:
        meta = TSID_METADATA.get(tsid, {})
        variable = meta.get('variable')
        
        if not variable:
            try:
                variables = hg.timescale.get_all_variables(tsid)
                variable = variables[0] if variables else None
            except:
                return None
        
        if not variable:
            return None
        
        measurements = hg.timescale.get_measurements(tsid, variable)
        if not measurements:
            return None

        return {
            'tsid': tsid,
            'timestamps': [m[0].isoformat() for m in measurements],
            'variables': [variable],
            'data': [[m[1]] for m in measurements],
            'metadata': {
                'owner_id': meta.get('owner_id', tsid),
                'owner_type': meta.get('owner_type', 'unknown'),
                'variable': variable
            }
        }
    except Exception as e:
        print(f"Error loading {tsid}: {e}")
        return None


def node_to_response(node) -> Dict:
    """Convert node to API response format"""
    node_dict = node.to_dict() if hasattr(node, 'to_dict') else {}
    return {
        'oid': str(node.oid),
        'label': node.label if hasattr(node, 'label') else '',
        'start_time': str(node_dict.get('start_time', '')),
        'end_time': str(node_dict.get('end_time', '')),
        'static_properties': node_dict.get('static_properties', {}),
        'temporal_properties': node_dict.get('temporal_properties', {})
    }


def edge_to_response(edge) -> Dict:
    """Convert edge to API response format"""
    edge_dict = edge.to_dict() if hasattr(edge, 'to_dict') else {}
    return {
        'oid': str(edge.oid),
        'source': str(edge.source) if hasattr(edge, 'source') else '',
        'target': str(edge.target) if hasattr(edge, 'target') else '',
        'label': edge.label if hasattr(edge, 'label') else '',
        'start_time': str(edge_dict.get('start_time', '')),
        'end_time': str(edge_dict.get('end_time', '')),
        'static_properties': edge_dict.get('static_properties', {}),
        'temporal_properties': edge_dict.get('temporal_properties', {})
    }


def apply_ts_constraint(query, constraint: TSConstraint):
    """Apply a time series constraint to a query builder"""
    
    if constraint.type == 'aggregation':
        # Use where_ts_agg: hg.query().nodes().where_ts_agg('num_bikes', 'mean', '<', 10)
        query = query.where_ts_agg(
            constraint.property,
            constraint.aggregation,
            constraint.operator,
            constraint.value
        )
    
    elif constraint.type == 'value_at':
        # Use where_ts with get_value_at_nearest
        if constraint.timestamp:
            ts_datetime = datetime.fromisoformat(constraint.timestamp.replace('Z', '+00:00'))
            def value_at_predicate(ts):
                if ts is None:
                    return False
                val = ts.get_value_at_nearest(ts_datetime)
                if val is None:
                    return False
                return eval(f"{val} {constraint.operator} {constraint.value}")
            query = query.where_ts(constraint.property, value_at_predicate)
    
    elif constraint.type == 'first':
        # Filter by first value
        def first_predicate(ts):
            if ts is None:
                return False
            val = ts.get_first_value()
            return eval(f"{val} {constraint.operator} {constraint.value}")
        query = query.where_ts(constraint.property, first_predicate)
    
    elif constraint.type == 'last':
        # Filter by last value
        def last_predicate(ts):
            if ts is None:
                return False
            val = ts.get_last_value()
            return eval(f"{val} {constraint.operator} {constraint.value}")
        query = query.where_ts(constraint.property, last_predicate)
    
    elif constraint.type == 'range':
        # Filter by range (max - min)
        def range_predicate(ts):
            if ts is None:
                return False
            val = ts.get_range()
            return eval(f"{val} {constraint.operator} {constraint.value}")
        query = query.where_ts(constraint.property, range_predicate)
    
    elif constraint.type == 'change':
        # Filter by change (last - first)
        def change_predicate(ts):
            if ts is None:
                return False
            val = ts.get_change()
            return eval(f"{val} {constraint.operator} {constraint.value}")
        query = query.where_ts(constraint.property, change_predicate)
    
    elif constraint.type == 'trend':
        # Filter by trend (increasing/decreasing)
        def trend_predicate(ts):
            if ts is None:
                return False
            if constraint.value == 1:  # increasing
                return ts.is_increasing()
            elif constraint.value == -1:  # decreasing
                return ts.is_decreasing()
            else:  # stable
                return ts.is_stable()
        query = query.where_ts(constraint.property, trend_predicate)
    
    elif constraint.type == 'pattern':
        # Use where_ts_pattern
        if constraint.template:
            query = query.where_ts_pattern(
                constraint.property,
                constraint.template,
                method='stumpy',
                threshold=constraint.threshold,
                normalize=True
            )
    
    elif constraint.type == 'similar_reference':
        # Use where_ts_similar with user-provided reference timeseries
        ref_ts = constraint.reference_ts
        if ref_ts and ref_ts.get('timestamps') and ref_ts.get('values'):
            try:
                from datetime import datetime as dt
                ref_timestamps = [dt.fromisoformat(t.replace('Z', '+00:00')) for t in ref_ts['timestamps']]
                ref_values = ref_ts['values']
                
                ref_ts_obj = TimeSeries(
                    tsid='reference_query',
                    timestamps=ref_timestamps,
                    variables=['value'],
                    data=[[v] for v in ref_values]
                )
                
                tol = constraint.tolerance if isinstance(constraint.tolerance, dict) else {'mean': 0.1, 'std': 0.1}
                query = query.where_ts_similar(
                    constraint.property,
                    reference_ts=ref_ts_obj,
                    tolerance=tol
                )
            except Exception as e:
                print(f"Error creating reference TS: {e}")
    
    elif constraint.type == 'similar_template':
        # Use where_ts_similar with predefined template
        template = constraint.similarity_template or 'stable'
        tol = constraint.tolerance if isinstance(constraint.tolerance, dict) else {'mean': 0.1, 'std': 0.1}
        query = query.where_ts_similar(
            constraint.property,
            template=template,
            tolerance=tol
        )
    
    elif constraint.type == 'similar':
        # Legacy: Use where_ts_similar
        query = query.where_ts_similar(
            constraint.property,
            reference_ts=None,
            features=constraint.features,
            template=constraint.similarity_template,
            tolerance=constraint.tolerance
        )
    
    elif constraint.type == 'bucketed':
        # Use where_ts_bucketed
        if constraint.interval:
            query = query.where_ts_bucketed(
                constraint.property,
                constraint.interval,
                constraint.aggregation or 'avg',
                constraint.condition or 'all',
                constraint.operator,
                constraint.value
            )
    
    elif constraint.type == 'count_above':
        def count_above_predicate(ts):
            if ts is None:
                return False
            count = ts.count_above(constraint.value)
            return count > 0
        query = query.where_ts(constraint.property, count_above_predicate)
    
    elif constraint.type == 'count_below':
        def count_below_predicate(ts):
            if ts is None:
                return False
            count = ts.count_below(constraint.value)
            return count > 0
        query = query.where_ts(constraint.property, count_below_predicate)
    
    elif constraint.type == 'percent_above':
        def percent_above_predicate(ts):
            if ts is None:
                return False
            pct = ts.percent_above(constraint.value)
            threshold = constraint.threshold or 0.5
            return pct >= threshold
        query = query.where_ts(constraint.property, percent_above_predicate)
    
    elif constraint.type == 'percent_below':
        def percent_below_predicate(ts):
            if ts is None:
                return False
            pct = ts.percent_below(constraint.value)
            threshold = constraint.threshold or 0.5
            return pct >= threshold
        query = query.where_ts(constraint.property, percent_below_predicate)
    
    elif constraint.type == 'volatility':
        # Coefficient of variation
        def volatility_predicate(ts):
            if ts is None:
                return False
            cv = ts.get_coefficient_of_variation()
            return eval(f"{cv} {constraint.operator} {constraint.value}")
        query = query.where_ts(constraint.property, volatility_predicate)
    
    return query




@app.get("/", tags=["Health"])
def root():
    return {
        "status": "online",
        "version": "4.3.1",
        "hygraph": "connected" if hg else "disconnected",
    }


@app.get("/health", tags=["Health"])
def health():
    if not hg:
        return {"status": "error", "hygraph": "disconnected"}
    try:
        hg.query().nodes().limit(1).execute()
        return {"status": "healthy", "hygraph": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}




@app.get("/api/graph", tags=["Graph"])
def get_graph(refresh: bool = False):
    """
    Get the full graph data with caching.
    
    Args:
        refresh: If True, force reload from database (default: False)
    
    The graph is cached in memory after first load. Subsequent requests
    return cached data instantly. Use ?refresh=true to force reload.
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        import time
        
        # Return cached data if available and not forcing refresh
        if not refresh and GRAPH_CACHE['nodes'] is not None:
            print(f"Returning cached graph: {len(GRAPH_CACHE['nodes'])} nodes, {len(GRAPH_CACHE['edges'])} edges")
            return {
                'nodes': GRAPH_CACHE['nodes'],
                'edges': GRAPH_CACHE['edges'],
                'timeseries': {},
                'metadata': GRAPH_CACHE['metadata']
            }
        
        # Prevent concurrent loads - return empty if already loading
        if GRAPH_CACHE['loading']:
            print("Graph already loading, returning cached or empty")
            if GRAPH_CACHE['nodes'] is not None:
                return {
                    'nodes': GRAPH_CACHE['nodes'],
                    'edges': GRAPH_CACHE['edges'],
                    'timeseries': {},
                    'metadata': GRAPH_CACHE['metadata']
                }
            # Return loading indicator
            return {
                'nodes': [],
                'edges': [],
                'timeseries': {},
                'metadata': {'loading': True},
                'loading': True
            }
        
        # Mark as loading
        GRAPH_CACHE['loading'] = True
        
        start_time = time.time()
        
        nodes_response = process_nodes_fast()
        edges_response = process_edges_fast()
        
        lats, lngs = [], []
        for node in nodes_response:
            sp = node.get('static_properties', {})
            lat = sp.get('latitude') or sp.get('lat')
            lng = sp.get('longitude') or sp.get('lng') or sp.get('lon')
            if lat and lng:
                lats.append(float(lat))
                lngs.append(float(lng))

        metadata = {
            'hasCoordinates': bool(lats),
            'center': {
                'lat': sum(lats) / len(lats) if lats else 40.7589,
                'lng': sum(lngs) / len(lngs) if lngs else -73.9851
            },
            'zoom': 13,
            'total_timeseries': len(TSID_METADATA)
        }
        
        # Cache the results
        GRAPH_CACHE['nodes'] = nodes_response
        GRAPH_CACHE['edges'] = edges_response
        GRAPH_CACHE['metadata'] = metadata
        GRAPH_CACHE['loaded_at'] = time.time()
        GRAPH_CACHE['loading'] = False
        
        elapsed = time.time() - start_time
        print(f"Graph loaded in {elapsed:.2f}s: {len(nodes_response)} nodes, {len(edges_response)} edges (CACHED)")

        return {
            'nodes': nodes_response,
            'edges': edges_response,
            'timeseries': {},
            'metadata': metadata
        }

    except Exception as e:
        GRAPH_CACHE['loading'] = False
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/graph/refresh", tags=["Graph"])
def refresh_graph():
    """Force refresh the graph cache from database."""
    # Clear cache
    GRAPH_CACHE['nodes'] = None
    GRAPH_CACHE['edges'] = None
    GRAPH_CACHE['metadata'] = None
    GRAPH_CACHE['loading'] = False
    
    # Reload
    return get_graph(refresh=True)


@app.get("/api/graph/cache-status", tags=["Graph"])
def get_cache_status():
    """Get the status of the graph cache."""
    return {
        'cached': GRAPH_CACHE['nodes'] is not None,
        'loading': GRAPH_CACHE['loading'],
        'loaded_at': GRAPH_CACHE['loaded_at'],
        'node_count': len(GRAPH_CACHE['nodes']) if GRAPH_CACHE['nodes'] else 0,
        'edge_count': len(GRAPH_CACHE['edges']) if GRAPH_CACHE['edges'] else 0,
    }


@app.get("/api/nodes", tags=["Graph"])
def get_nodes():
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    return process_nodes_fast()


@app.get("/api/edges", tags=["Graph"])
def get_edges():
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    return process_edges_fast()




@app.get("/api/timeseries/{ts_id}", tags=["TimeSeries"])
def get_timeseries(ts_id: str):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    ts_data = load_single_timeseries(ts_id)
    if not ts_data:
        raise HTTPException(404, f"TimeSeries not found: {ts_id}")
    return ts_data


@app.post("/api/timeseries/batch", tags=["TimeSeries"])
def get_timeseries_batch(request: BatchTimeSeriesRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    results = {}
    errors = []
    
    for ts_id in request.ts_ids:
        ts_data = load_single_timeseries(ts_id)
        if ts_data:
            results[ts_id] = ts_data
        else:
            errors.append(ts_id)
    
    return {
        'timeseries': results,
        'loaded': len(results),
        'failed': errors
    }


@app.get("/api/timeseries/metadata", tags=["TimeSeries"])
def get_timeseries_metadata():
    return {
        'count': len(TSID_METADATA),
        'timeseries': [
            {'tsid': tsid, **meta}
            for tsid, meta in TSID_METADATA.items()
        ]
    }




@app.post("/api/query/nodes", tags=["Query"])
def query_nodes(request: NodeQueryRequest):
    """
    Query nodes using fluent API with full temporal property support.
    
    Supports:
    - Label filtering
    - ID contains filtering  
    - Static property constraints
    - Temporal property constraints:
      - aggregation: mean, min, max, sum, std, count, median, range
      - value_at: value at specific timestamp
      - first/last: first or last value
      - range: max - min
      - change: last - first
      - trend: increasing/decreasing/stable
      - pattern: spike, drop, increasing, decreasing, etc.
      - similar: compare with reference or features
      - bucketed: time-bucketed aggregation
      - volatility: coefficient of variation
      - count_above/below, percent_above/below
    
    Example:
    {
        "label": "Station",
        "ts_constraints": [
            {"property": "num_bikes", "type": "aggregation", "aggregation": "mean", "operator": "<", "value": 10},
            {"property": "num_bikes", "type": "last", "operator": ">", "value": 5}
        ]
    }
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        # Build query using fluent API
        query = hg.query().nodes(label=request.label)
        
        # Apply time filters
        if request.at_time:
            query = query.at(request.at_time)
        elif request.between and len(request.between) == 2:
            query = query.between(request.between[0], request.between[1])
        
        # Apply static constraints
        if request.static_constraints:
            for sc in request.static_constraints:
                def make_static_filter(prop, op, val):
                    def filter_fn(n):
                        actual = n.get_static_property(prop)
                        if actual is None:
                            return False
                        try:
                            # Convert both to float for numeric comparison
                            actual_num = float(actual)
                            val_num = float(val)
                            if op == '>':
                                return actual_num > val_num
                            elif op == '>=':
                                return actual_num >= val_num
                            elif op == '<':
                                return actual_num < val_num
                            elif op == '<=':
                                return actual_num <= val_num
                            elif op == '==' or op == '=':
                                return abs(actual_num - val_num) < 1e-9
                            elif op == '!=' or op == '<>':
                                return abs(actual_num - val_num) >= 1e-9
                            else:
                                return False
                        except (ValueError, TypeError):
                            # Fall back to string comparison for non-numeric values
                            try:
                                if op == '==' or op == '=':
                                    return str(actual) == str(val)
                                elif op == '!=' or op == '<>':
                                    return str(actual) != str(val)
                                else:
                                    return False
                            except:
                                return False
                    return filter_fn
                query = query.where(make_static_filter(sc.property, sc.operator, sc.value))
        
        # Apply TS constraints
        if request.ts_constraints:
            for tc in request.ts_constraints:
                query = apply_ts_constraint(query, tc)
        
        # Apply limit
        if request.limit:
            query = query.limit(request.limit)
        
        # Execute
        nodes = query.execute()
        
        # Filter by ID contains (post-filter)
        if request.id_contains:
            nodes = [n for n in nodes if request.id_contains.lower() in str(n.oid).lower()]
        
        # Build response
        nodes_response = [node_to_response(n) for n in nodes]
        node_ids = [n['oid'] for n in nodes_response]
        
        # Get edges between matched nodes
        node_set = set(node_ids)
        all_edges = hg.age.query_edges()
        edges_response = []
        
        for edge_dict in all_edges:
            source_id = str(edge_dict.get('src_uid', ''))
            target_id = str(edge_dict.get('dst_uid', ''))
            if source_id in node_set and target_id in node_set:
                props = edge_dict.get('properties', {})
                edges_response.append({
                    'oid': edge_dict.get('uid'),
                    'source': source_id,
                    'target': target_id,
                    'label': edge_dict.get('label', ''),
                    'static_properties': {k: v for k, v in props.items() if k != 'temporal_properties'},
                    'temporal_properties': props.get('temporal_properties', {})
                })
        
        return {
            'nodes': nodes_response,
            'edges': edges_response,
            'node_ids': node_ids,
            'count': len(nodes_response),
            'query': {
                'label': request.label,
                'static_constraints': len(request.static_constraints) if request.static_constraints else 0,
                'ts_constraints': len(request.ts_constraints) if request.ts_constraints else 0
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/query/edges", tags=["Query"])
def query_edges(request: EdgeQueryRequest):
    """
    Query edges using fluent API with full temporal property support.
    
    Example:
    {
        "label": "Trip",
        "source_id": "station_1",
        "static_constraints": [
            {"property": "distance", "operator": "<", "value": 5}
        ]
    }
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        # Get all edges (fluent API edge query is simpler)
        all_edges = hg.age.query_edges(label=request.label)
        
        filtered_edges = []
        
        for edge_dict in all_edges:
            edge_uid = edge_dict.get('uid', '')
            source_id = str(edge_dict.get('src_uid', ''))
            target_id = str(edge_dict.get('dst_uid', ''))
            
            # ID contains filter
            if request.id_contains and request.id_contains.lower() not in edge_uid.lower():
                continue
            
            # Source/target filter
            if request.source_id and source_id != request.source_id:
                continue
            if request.target_id and target_id != request.target_id:
                continue
            
            props = edge_dict.get('properties', {})
            static_props = {k: v for k, v in props.items() if k != 'temporal_properties'}
            
            # Static constraint filter
            if request.static_constraints:
                skip = False
                for sc in request.static_constraints:
                    actual = static_props.get(sc.property)
                    if actual is None:
                        skip = True
                        break
                    try:
                        if not eval(f"{actual} {sc.operator} {sc.value}"):
                            skip = True
                            break
                    except:
                        skip = True
                        break
                if skip:
                    continue
            
            filtered_edges.append({
                'oid': edge_uid,
                'source': source_id,
                'target': target_id,
                'label': edge_dict.get('label', ''),
                'start_time': edge_dict.get('start_time', ''),
                'end_time': edge_dict.get('end_time', ''),
                'static_properties': static_props,
                'temporal_properties': props.get('temporal_properties', {})
            })
            
            if request.limit and len(filtered_edges) >= request.limit:
                break
        
        # Get connected nodes
        node_ids = set()
        for e in filtered_edges:
            node_ids.add(e['source'])
            node_ids.add(e['target'])
        
        nodes_response = []
        for node_id in node_ids:
            node = hg.crud.get_node_with_timeseries(node_id)
            if node:
                nodes_response.append(node_to_response(node))
        
        return {
            'nodes': nodes_response,
            'edges': filtered_edges,
            'node_ids': list(node_ids),
            'count': len(filtered_edges),
            'query': {
                'label': request.label,
                'source_id': request.source_id,
                'target_id': request.target_id
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/query/timeseries", tags=["Query"])
def query_timeseries(request: TimeSeriesQueryRequest):
    """
    Query timeseries using fluent API.
    hg.query().timeseries(entity_id, variable).between(start, end).execute()
    
    Example:
    {
        "entity_id": "station_1",
        "variable": "num_bikes",
        "start_time": "2024-06-01T00:00:00",
        "end_time": "2024-06-07T00:00:00"
    }
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        query = hg.query().timeseries(request.entity_id, request.variable)
        
        if request.start_time and request.end_time:
            query = query.between(request.start_time, request.end_time)
        
        result = query.execute()
        
        if result is None:
            raise HTTPException(404, f"TimeSeries not found for entity: {request.entity_id}")
        
        # Handle both single TS and dict of TS
        if isinstance(result, dict):
            # Multiple variables
            ts_responses = {}
            for var_name, ts_obj in result.items():
                ts_responses[var_name] = {
                    'tsid': ts_obj.tsid if hasattr(ts_obj, 'tsid') else f"{request.entity_id}_{var_name}",
                    'timestamps': [t.isoformat() for t in ts_obj.timestamps],
                    'variables': ts_obj.variables,
                    'data': ts_obj.data,
                    'stats': {
                        'mean': ts_obj.mean(),
                        'min': ts_obj.min(),
                        'max': ts_obj.max(),
                        'std': ts_obj.std(),
                        'count': ts_obj.length
                    }
                }
            return {
                'entity_id': request.entity_id,
                'timeseries': ts_responses,
                'count': len(ts_responses)
            }
        else:
            # Single TS object
            return {
                'entity_id': request.entity_id,
                'variable': request.variable,
                'timeseries': {
                    'tsid': result.tsid if hasattr(result, 'tsid') else f"{request.entity_id}_{request.variable}",
                    'timestamps': [t.isoformat() for t in result.timestamps],
                    'variables': result.variables,
                    'data': result.data,
                    'stats': {
                        'mean': result.mean(),
                        'min': result.min(),
                        'max': result.max(),
                        'std': result.std(),
                        'count': result.length,
                        'first': result.get_first_value(),
                        'last': result.get_last_value(),
                        'range': result.get_range(),
                        'change': result.get_change()
                    }
                }
            }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))




@app.post("/api/pattern/match", tags=["PatternMatch"])
def pattern_match(request: PatternMatchRequest):
    """
    Execute pattern matching using fluent API.
    
    Supports:
    - Multiple node patterns with constraints
    - Edge patterns connecting nodes
    - Cross-entity constraints (compare properties)
    - Cross-TS constraints (correlations, leads/lags, etc.)
    
    Example:
    {
        "nodes": [
            {"variable": "n1", "label": "Station", "ts_constraints": [
                {"property": "num_bikes", "type": "aggregation", "aggregation": "mean", "operator": "<", "value": 10}
            ]},
            {"variable": "n2", "label": "Station"}
        ],
        "edges": [
            {"variable": "e", "source_var": "n1", "target_var": "n2", "label": "Trip"}
        ],
        "cross_constraints": [
            {"left": "n2.capacity", "operator": "<", "right": "n1.capacity"}
        ],
        "cross_ts_constraints": [
            {"left": "n1.num_bikes", "operator": "correlates", "right": "n2.num_bikes", "threshold": 0.6}
        ]
    }
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        from hygraph_core.storage.fluent_api import PatternQuery
        
        pattern = PatternQuery(hg)
        
        # Add nodes
        for node_def in request.nodes:
            pattern.node(node_def.variable, label=node_def.label, uid=node_def.uid)
            
            # Build constraints
            static_cons = {}
            ts_cons = {}
            
            if node_def.static_constraints:
                for sc in node_def.static_constraints:
                    static_cons[sc.property] = (sc.operator, sc.value)
            
            if node_def.ts_constraints:
                for tc in node_def.ts_constraints:
                    if tc.type == 'aggregation':
                        ts_cons[tc.property] = (tc.aggregation, tc.operator, tc.value)
            
            if static_cons or ts_cons:
                pattern.where_node(
                    node_def.variable,
                    static=static_cons if static_cons else None,
                    ts=ts_cons if ts_cons else None
                )
        
        # Add edges
        if request.edges:
            for edge_def in request.edges:
                pattern.edge(
                    edge_def.variable,
                    label=edge_def.label,
                    source=edge_def.source_var,
                    target=edge_def.target_var
                )
                
                if edge_def.static_constraints:
                    static_cons = {sc.property: (sc.operator, sc.value) for sc in edge_def.static_constraints}
                    pattern.where_edge(edge_def.variable, static=static_cons)
        
        # Add cross constraints
        if request.cross_constraints:
            for cc in request.cross_constraints:
                pattern.where_cross(cc.left, cc.operator, cc.right)
        
        # Add cross TS constraints
        if request.cross_ts_constraints:
            for ctc in request.cross_ts_constraints:
                pattern.where_cross_ts(
                    ctc.left,
                    ctc.operator,
                    ctc.right,
                    threshold=ctc.threshold,
                    lag=ctc.lag
                )
        
        if request.limit:
            pattern.limit(request.limit)
        
        # Execute
        matches = pattern.execute()
        
        # Format results
        results = []
        all_node_ids = set()
        all_edge_ids = set()
        
        for match in matches:
            match_dict = {}
            for var, entity in match.items():
                if hasattr(entity, 'oid'):
                    entity_dict = entity.to_dict() if hasattr(entity, 'to_dict') else {}
                    is_edge = hasattr(entity, 'source')
                    
                    if is_edge:
                        all_edge_ids.add(str(entity.oid))
                    else:
                        all_node_ids.add(str(entity.oid))
                    
                    match_dict[var] = {
                        'oid': str(entity.oid),
                        'label': entity.label if hasattr(entity, 'label') else '',
                        'type': 'edge' if is_edge else 'node',
                        'static_properties': entity_dict.get('static_properties', {}),
                        'temporal_properties': entity_dict.get('temporal_properties', {})
                    }
                    
                    if is_edge:
                        match_dict[var]['source'] = str(entity.source)
                        match_dict[var]['target'] = str(entity.target)
            
            results.append(match_dict)
        
        # Get full node/edge data for visualization
        nodes_response = []
        for node_id in all_node_ids:
            node = hg.crud.get_node_with_timeseries(node_id)
            if node:
                nodes_response.append(node_to_response(node))
        
        edges_response = []
        all_edges = hg.age.query_edges()
        for edge_dict in all_edges:
            if edge_dict.get('uid') in all_edge_ids:
                props = edge_dict.get('properties', {})
                edges_response.append({
                    'oid': edge_dict.get('uid'),
                    'source': str(edge_dict.get('src_uid')),
                    'target': str(edge_dict.get('dst_uid')),
                    'label': edge_dict.get('label', ''),
                    'static_properties': {k: v for k, v in props.items() if k != 'temporal_properties'},
                    'temporal_properties': props.get('temporal_properties', {})
                })
        
        return {
            'matches': results,
            'nodes': nodes_response,
            'edges': edges_response,
            'node_ids': list(all_node_ids),
            'count': len(results),
            'query_info': {
                'nodes': len(request.nodes),
                'edges': len(request.edges) if request.edges else 0,
                'cross_constraints': len(request.cross_constraints) if request.cross_constraints else 0,
                'cross_ts_constraints': len(request.cross_ts_constraints) if request.cross_ts_constraints else 0
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))




@app.post("/api/timeseries/create", tags=["TimeSeries"])
def create_timeseries(request: CreateTimeSeriesRequest):
    """
    Create a TimeSeries using hg.create_timeseries().
    
    This uses the proper HyGraph method to create and store a timeseries.
    
    Example:
    {
        "owner_id": "station_1",
        "owner_type": "node",
        "variable": "num_bikes",
        "timestamps": ["2024-06-01T00:00:00", "2024-06-01T01:00:00"],
        "values": [10, 15],
        "description": "Bike availability",
        "units": "bikes"
    }
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        # Parse timestamps
        parsed_timestamps = [
            datetime.fromisoformat(ts.replace('Z', '+00:00'))
            for ts in request.timestamps
        ]
        
        # Create TimeSeries object
        ts_id = request.tsid or f"ts_{request.owner_id}_{request.variable}_{uuid.uuid4().hex[:8]}"
        
        ts_obj = TimeSeries(
            tsid=ts_id,
            timestamps=parsed_timestamps,
            variables=[request.variable],
            data=[[v] for v in request.values],
            metadata=TimeSeriesMetadata(
                owner_id=request.owner_id,
                element_type=request.owner_type,
                description=request.description,
                units=request.units
            )
        )
        
        # Use hg.create_timeseries()
        result_id = hg.create_timeseries(ts_obj)
        
        # Update metadata cache
        TSID_METADATA[result_id] = {
            'variable': request.variable,
            'owner_id': request.owner_id,
            'owner_type': request.owner_type
        }
        
        return {
            'success': True,
            'tsid': result_id,
            'owner_id': request.owner_id,
            'variable': request.variable,
            'data_points': len(request.values),
            'stats': {
                'mean': ts_obj.mean(),
                'min': ts_obj.min(),
                'max': ts_obj.max(),
                'first': ts_obj.get_first_value(),
                'last': ts_obj.get_last_value()
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.get("/api/timeseries/templates", tags=["TimeSeries"])
def get_timeseries_templates():
    """
    Get available time series pattern templates.
    
    These templates can be used with where_ts_pattern() or where_ts_similar().
    """
    templates = [
        {'name': 'spike', 'description': 'Sudden increase then decrease (Gaussian bump)', 'shape': '___/\\___'},
        {'name': 'drop', 'description': 'Sudden decrease then increase (inverted Gaussian)', 'shape': '___\\/___'},
        {'name': 'increasing', 'description': 'Steady upward trend', 'shape': '__/'},
        {'name': 'decreasing', 'description': 'Steady downward trend', 'shape': '\\__'},
        {'name': 'peak', 'description': 'Gradual peak pattern (inverted parabola)', 'shape': '/\\'},
        {'name': 'valley', 'description': 'Gradual valley pattern (parabola)', 'shape': '\\/'},
        {'name': 'step_up', 'description': 'Step increase', 'shape': '__|^^'},
        {'name': 'step_down', 'description': 'Step decrease', 'shape': '^^|__'},
        {'name': 'oscillation', 'description': 'Periodic wave pattern (sine)', 'shape': '/\\/\\/'},
        {'name': 'stable', 'description': 'Low variance (for similarity matching)', 'shape': '----'},
    ]
    
    aggregations = [
        {'name': 'mean', 'description': 'Average value'},
        {'name': 'min', 'description': 'Minimum value'},
        {'name': 'max', 'description': 'Maximum value'},
        {'name': 'sum', 'description': 'Sum of all values'},
        {'name': 'std', 'description': 'Standard deviation'},
        {'name': 'count', 'description': 'Number of data points'},
        {'name': 'median', 'description': 'Median value'},
        {'name': 'range', 'description': 'Max - Min'},
        {'name': 'first', 'description': 'First value'},
        {'name': 'last', 'description': 'Last value'},
    ]
    
    ts_constraint_types = [
        {'type': 'aggregation', 'description': 'Compare aggregated value (mean, max, etc.)'},
        {'type': 'value_at', 'description': 'Value at specific timestamp'},
        {'type': 'first', 'description': 'First value in series'},
        {'type': 'last', 'description': 'Last value in series'},
        {'type': 'range', 'description': 'Range (max - min)'},
        {'type': 'change', 'description': 'Change (last - first)'},
        {'type': 'trend', 'description': 'Trend direction (increasing/decreasing/stable)'},
        {'type': 'pattern', 'description': 'Contains pattern template (spike, drop, etc.)'},
        {'type': 'similar', 'description': 'Similar to reference or features'},
        {'type': 'bucketed', 'description': 'Time-bucketed aggregation'},
        {'type': 'count_above', 'description': 'Count of values above threshold'},
        {'type': 'count_below', 'description': 'Count of values below threshold'},
        {'type': 'percent_above', 'description': 'Percentage above threshold'},
        {'type': 'percent_below', 'description': 'Percentage below threshold'},
    ]
    
    cross_ts_operators = [
        {'operator': 'correlates', 'description': 'Pearson correlation >= threshold'},
        {'operator': 'anti_correlates', 'description': 'Negative correlation <= -threshold'},
        {'operator': 'similar_to', 'description': 'Statistical similarity (mean, std)'},
        {'operator': 'leads', 'description': 'Left TS leads right by lag steps'},
        {'operator': 'lags', 'description': 'Left TS lags behind right'},
        {'operator': 'diverges_from', 'description': 'Low correlation'},
        {'operator': 'more_volatile', 'description': 'Higher standard deviation'},
        {'operator': 'more_stable', 'description': 'Lower standard deviation'},
    ]
    
    return {
        'templates': templates,
        'aggregations': aggregations,
        'ts_constraint_types': ts_constraint_types,
        'cross_ts_operators': cross_ts_operators
    }




class SimplePatternRequest(BaseModel):
    label: Optional[str] = None
    static_property: Optional[str] = None
    static_operator: str = ">="
    static_value: Optional[float] = None
    ts_property: Optional[str] = None
    aggregation: str = "mean"
    ts_operator: str = "<"
    ts_value: Optional[float] = None
    limit: int = 100


@app.post("/api/pattern/simple", tags=["PatternMatch"])
def pattern_simple(request: SimplePatternRequest):
    """
    Simple pattern matching (legacy endpoint).
    Use /api/query/nodes for full functionality.
    """
    # Convert to new format
    node_request = NodeQueryRequest(
        label=request.label,
        limit=request.limit
    )
    
    if request.static_property and request.static_value is not None:
        node_request.static_constraints = [
            StaticConstraint(
                property=request.static_property,
                operator=request.static_operator,
                value=request.static_value
            )
        ]
    
    if request.ts_property and request.ts_value is not None:
        node_request.ts_constraints = [
            TSConstraint(
                property=request.ts_property,
                type='aggregation',
                aggregation=request.aggregation,
                operator=request.ts_operator,
                value=request.ts_value
            )
        ]
    
    result = query_nodes(node_request)
    result['filter'] = f"{request.label or 'All'}"
    if request.ts_property:
        result['filter'] += f" WHERE {request.ts_property}.{request.aggregation}() {request.ts_operator} {request.ts_value}"
    
    return result




@app.post("/api/nodes/create", tags=["CRUD"])
async def create_node(request: CreateNodeRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        ((((hg.create_node(request.uid)
        .valid_from(request.start_time)
        .valid_until(request.end_time)
                .with_label(request.label))
                .with_properties(**request.properties))
                .create()))
        return {"status": "created", "uid": request.uid}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/nodes/update", tags=["CRUD"])
async def update_node(request: UpdateNodeRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        hg.update_node(request.uid).set_properties(**request.properties).execute()
        return {"status": "updated", "uid": request.uid}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/nodes/delete", tags=["CRUD"])
async def delete_node(request: DeleteNodeRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        hg.age.delete_node(uid=request.uid, hard=request.hard)
        return {"status": "deleted", "uid": request.uid, "hard": request.hard}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/nodes/{uid}", tags=["CRUD"])
def get_node(uid: str):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        node = hg.query().node(uid).execute()
        if not node:
            raise HTTPException(404, f"Node not found: {uid}")
        return node_to_response(node)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))




@app.post("/api/edges/create", tags=["CRUD"])
async def create_edge(request: CreateEdgeRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        hg.age.create_edge(
            uid=request.uid,
            src_uid=request.source,
            dst_uid=request.target,
            label=request.label,
            properties=request.properties,
            start_time=request.start_time,
            end_time=request.end_time
        )
        return {"status": "created", "uid": request.uid}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/edges/update", tags=["CRUD"])
async def update_edge(request: UpdateEdgeRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        hg.age.update_edge(uid=request.uid, properties=request.properties)
        return {"status": "updated", "uid": request.uid}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/edges/delete", tags=["CRUD"])
async def delete_edge(request: DeleteEdgeRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        hg.age.delete_edge(uid=request.uid, hard=request.hard)
        return {"status": "deleted", "uid": request.uid, "hard": request.hard}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/edges/{uid}", tags=["CRUD"])
def get_edge(uid: str):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    try:
        edge = hg.query().edge(uid).execute()
        if not edge:
            raise HTTPException(404, f"Edge not found: {uid}")
        return edge_to_response(edge)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))




@app.post("/api/snapshot/at", tags=["Snapshot"])
def snapshot_at(request: SnapshotAtRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        snapshot = hg.snapshot(when=request.timestamp, mode=request.mode)
        return snapshot.to_api_response(include_graph=True)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/snapshot/interval", tags=["Snapshot"])
def snapshot_interval(request: SnapshotIntervalRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        snapshot = hg.snapshot(
            when=(request.start, request.end),
            mode=request.mode,
            ts_handling=request.ts_handling,
            aggregation_fn=request.aggregation_fn
        )
        return snapshot.to_api_response(include_graph=True)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/snapshot/metrics", tags=["Snapshot"])
def snapshot_metrics(request: SnapshotAtRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        snapshot = hg.snapshot(when=request.timestamp, mode="graph")
        
        return {
            'timestamp': request.timestamp,
            'metrics': {
                'node_count': snapshot.count_nodes(),
                'edge_count': snapshot.count_edges(),
                'ts_count': snapshot.count_timeseries(),
                'density': snapshot.density(),
                'connected_components': snapshot.connected_components()
            }
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/snapshot/nodes_degree", tags=["Snapshot"])
def snapshot_nodes_degree(request: SnapshotNodeDegreeRequest):
    """
    Get degree for all nodes at a specific timestamp or interval.
    
    For point-in-time (timestamp): Returns degrees based on edges active at that moment.
    For interval (start + end): Returns degrees based on all edges active during the interval.
    
    Note: For temporal graphs like CitiBike, intervals give more meaningful results
    since individual trips (edges) only last a few minutes.
    
    Returns a dict of {node_id: [(timestamp, degree)]}.
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        # Determine if this is a point or interval query
        if request.start and request.end:
            # Interval query - more useful for temporal edges
            snapshot = hg.snapshot(
                when=(request.start, request.end), 
                mode="graph",
                ts_handling="aggregate" if request.mode == "hybrid" else None
            )
            ts_label = f"{request.start} to {request.end}"
        elif request.timestamp:
            # Point-in-time query
            snapshot = hg.snapshot(when=request.timestamp, mode="graph")
            ts_label = request.timestamp
        else:
            raise HTTPException(400, "Either timestamp or (start + end) is required")
        
        # Use the degree method which returns Dict[str, list]
        # Each list contains tuples of (timestamp, degree)
        degree_results = snapshot.degree(
            node_id=None,  # All nodes
            label=request.label,
            weight=request.weight,
            direction=request.direction
        )
        
        # Frontend expects raw degrees object: {node_id: [(timestamp, degree)], ...}
        # Convert the dict to have string timestamps
        formatted_degrees = {}
        for node_id, ts_values in degree_results.items():
            formatted_degrees[node_id] = [
                [ts_label, val] for (ts, val) in ts_values
            ]
        
        return formatted_degrees
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/snapshot/node_degree", tags=["Snapshot"])
def snapshot_node_degree(request: SnapshotNodeDegreeRequest):
    """
    Get degree for a specific node at a timestamp or interval.
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    if not request.node_id:
        raise HTTPException(400, "node_id is required")

    try:
        # Determine if this is a point or interval query
        if request.start and request.end:
            snapshot = hg.snapshot(
                when=(request.start, request.end), 
                mode="graph",
                ts_handling="aggregate" if request.mode == "hybrid" else None
            )
            ts_label = f"{request.start} to {request.end}"
        elif request.timestamp:
            snapshot = hg.snapshot(when=request.timestamp, mode="graph")
            ts_label = request.timestamp
        else:
            raise HTTPException(400, "Either timestamp or (start + end) is required")
        
        # Use the degree method which returns Dict[str, list]
        degree_results = snapshot.degree(
            node_id=request.node_id,
            label=request.label,
            weight=request.weight,
            direction=request.direction
        )
        
        # Extract the degree value from the results
        # Format: {node_id: [(timestamp, degree)]}
        degree = 0
        if request.node_id in degree_results:
            ts_values = degree_results[request.node_id]
            if ts_values and len(ts_values) > 0:
                # Get the degree value (second element of tuple)
                degree = ts_values[0][1] if ts_values[0][1] is not None else 0
        
        return {
            'node_id': request.node_id,
            'timestamp': ts_label,
            'direction': request.direction,
            'degree': degree,
            'query_type': 'interval' if (request.start and request.end) else 'point',
            'edge_count': snapshot.count_edges()
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))




@app.post("/api/snapshot-sequence/create", tags=["SnapshotSequence"])
def create_and_cache_sequence(request: CreateSequenceRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        every = parse_granularity(request.granularity)
        sequence = hg.snapshot_sequence(
            start=request.start,
            end=request.end,
            every=every,
            mode=request.mode
        )

        seq_id = f"seq_{uuid.uuid4().hex[:12]}"
        SEQUENCE_CACHE[seq_id] = sequence

        return {
            'sequence_id': seq_id,
            'start': request.start,
            'end': request.end,
            'granularity': request.granularity,
            'mode': request.mode,
            'snapshot_count': len(sequence),
            'cached': True
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/snapshot-sequence/summary", tags=["SnapshotSequence"])
def snapshot_sequence_summary(request: CreateSequenceRequest):
    """
    Create a snapshot sequence and return summary metrics over time.
    Returns node count, edge count, density at each snapshot timestamp.
    
    Frontend expects: data.snapshots = [{timestamp, node_count, edge_count, ts_count}, ...]
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        every = parse_granularity(request.granularity)
        sequence = hg.snapshot_sequence(
            start=request.start,
            end=request.end,
            every=every,
            mode=request.mode
        )

        # Cache the sequence for later use with TSGen
        seq_id = f"seq_{uuid.uuid4().hex[:12]}"
        SEQUENCE_CACHE[seq_id] = sequence

        # Build snapshots array in format frontend expects
        snapshots = []
        for snap in sequence:
            # Get timestamp
            ts = None
            if hasattr(snap, 'when'):
                if isinstance(snap.when, tuple):
                    ts = snap.when[0]  # Use start of interval
                else:
                    ts = snap.when
            elif hasattr(snap, 'timestamp'):
                ts = snap.timestamp
            
            ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts) if ts else None
            
            # Get counts
            try:
                node_count = snap.count_nodes()
            except:
                node_count = 0
            
            try:
                edge_count = snap.count_edges()
            except:
                edge_count = 0
            
            try:
                ts_count = snap.count_timeseries()
            except:
                ts_count = 0
            
            snapshots.append({
                'timestamp': ts_str,
                'node_count': node_count,
                'edge_count': edge_count,
                'ts_count': ts_count
            })

        return {
            'sequence_id': seq_id,
            'start': request.start,
            'end': request.end,
            'granularity': request.granularity,
            'mode': request.mode,
            'snapshot_count': len(sequence),
            'cached': True,
            'snapshots': snapshots  # Frontend expects this format
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


# Model for snapshot-sequence/at endpoint
class SnapshotSequenceAtRequest(BaseModel):
    start: str
    end: str
    granularity: str = '1D'
    mode: str = 'hybrid'


@app.post("/api/snapshot-sequence/at/{index}", tags=["SnapshotSequence"])
def snapshot_sequence_at(index: int, request: SnapshotSequenceAtRequest):
    """
    Get snapshot at specific index from a sequence.
    Creates a new sequence if not cached, then returns the snapshot at the given index.
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        # Create the sequence (or use cached)
        every = parse_granularity(request.granularity)
        sequence = hg.snapshot_sequence(
            start=request.start,
            end=request.end,
            every=every,
            mode=request.mode
        )
        
        # Handle negative index (e.g., -1 for last)
        if index < 0:
            index = len(sequence) + index
        
        if index < 0 or index >= len(sequence):
            raise HTTPException(404, f"Snapshot index {index} out of range (0-{len(sequence)-1})")
        
        snapshot = sequence[index]
        
        return {
            'index': index,
            'total': len(sequence),
            'snapshot': snapshot.to_api_response(include_graph=True)
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.get("/api/snapshot-sequence/list", tags=["SnapshotSequence"])
def list_cached_sequences():
    return {
        'sequences': [
            {
                'sequence_id': seq_id,
                'snapshot_count': len(seq),
            }
            for seq_id, seq in SEQUENCE_CACHE.items()
        ],
        'count': len(SEQUENCE_CACHE)
    }


@app.delete("/api/snapshot-sequence/{sequence_id}", tags=["SnapshotSequence"])
def delete_cached_sequence(sequence_id: str):
    if sequence_id not in SEQUENCE_CACHE:
        raise HTTPException(404, f"Sequence not found: {sequence_id}")

    del SEQUENCE_CACHE[sequence_id]
    return {'status': 'deleted', 'sequence_id': sequence_id}


@app.post("/api/tsgen/generate", tags=["TSGen"])
def tsgen_generate(request: TSGenFromSequenceRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    sequence = SEQUENCE_CACHE.get(request.sequence_id)
    if not sequence:
        raise HTTPException(404, f"Sequence not found: {request.sequence_id}")

    try:
        tsgen = TSGen(sequence)
        ts = None
        metric_name = ''

        if request.scope == 'global':
            if request.entity_type == 'nodes':
                if request.metric == 'count':
                    ts = tsgen.global_.nodes.count(label=request.label)
                    metric_name = f"node_count{'_' + request.label if request.label else ''}"

                elif request.metric == 'degree':
                    degree_agg = tsgen.global_.nodes.degree(label=request.label, weight=request.weight)
                    if request.aggregation == 'avg':
                        ts = degree_agg.avg(direction=request.direction)
                    elif request.aggregation == 'max':
                        ts = degree_agg.max(direction=request.direction)
                    elif request.aggregation == 'min':
                        ts = degree_agg.min(direction=request.direction)
                    elif request.aggregation == 'sum':
                        ts = degree_agg.sum(direction=request.direction)
                    metric_name = f"{request.aggregation}_degree_{request.direction}"

                elif request.metric == 'property' and request.property_name:
                    prop_agg = tsgen.global_.nodes.property(request.property_name, label=request.label)
                    if request.aggregation == 'avg':
                        ts = prop_agg.avg()
                    elif request.aggregation == 'sum':
                        ts = prop_agg.sum()
                    elif request.aggregation == 'max':
                        ts = prop_agg.max()
                    elif request.aggregation == 'min':
                        ts = prop_agg.min()
                    metric_name = f"{request.aggregation}_{request.property_name}"

            elif request.entity_type == 'edges':
                if request.metric == 'count':
                    ts = tsgen.global_.edges.count(label=request.label)
                    metric_name = f"edge_count{'_' + request.label if request.label else ''}"

            elif request.entity_type == 'graph':
                if request.metric == 'density':
                    ts = tsgen.global_.graph.density(label=request.label, directed=request.directed)
                    metric_name = 'density'
                elif request.metric == 'connected_components':
                    ts = tsgen.global_.graph.connected_components(label=request.label, directed=request.directed)
                    metric_name = 'connected_components'

        elif request.scope == 'entity' and request.entity_id:
            if request.entity_type == 'nodes' and request.metric == 'degree':
                ts = tsgen.entities.nodes.degree(
                    node_id=request.entity_id,
                    direction=request.direction,
                    label=request.label,
                    weight=request.weight
                )
                metric_name = f"degree_{request.entity_id}_{request.direction}"

        if ts is None:
            raise HTTPException(400, "Invalid metric configuration")

        timestamps = []
        values = []

        if hasattr(ts, 'timestamps') and hasattr(ts, 'data'):
            timestamps = [t.isoformat() if hasattr(t, 'isoformat') else str(t) for t in ts.timestamps]
            values = [[v] if not isinstance(v, list) else v for v in ts.data]

        return {
            'timeseries': {
                'tsid': f'generated_{request.sequence_id}_{metric_name}',
                'name': metric_name,
                'timestamps': timestamps,
                'data': values,
                'variables': [metric_name],
            },
            'count': len(timestamps)
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/tsgen/combined", tags=["TSGen"])
def tsgen_combined(request: TSGenCombinedRequest):
    """
    Combined endpoint: Create snapshot sequence AND run TSGen in one call.
    This is useful when you don't need to reuse the sequence.
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        # Step 1: Create the snapshot sequence
        every = parse_granularity(request.granularity)
        sequence = hg.snapshot_sequence(
            start=request.start,
            end=request.end,
            every=every,
            mode=request.mode
        )

        # Cache it for potential reuse
        seq_id = f"seq_{uuid.uuid4().hex[:12]}"
        SEQUENCE_CACHE[seq_id] = sequence

        # Step 2: Run TSGen
        tsgen = TSGen(sequence)
        ts = None
        metric_name = ''

        if request.scope == 'global':
            if request.entity_type == 'nodes':
                if request.metric == 'count':
                    ts = tsgen.global_.nodes.count(label=request.label)
                    metric_name = f"node_count{'_' + request.label if request.label else ''}"

                elif request.metric == 'degree':
                    degree_agg = tsgen.global_.nodes.degree(label=request.label, weight=request.weight)
                    if request.aggregation == 'avg':
                        ts = degree_agg.avg(direction=request.direction)
                    elif request.aggregation == 'max':
                        ts = degree_agg.max(direction=request.direction)
                    elif request.aggregation == 'min':
                        ts = degree_agg.min(direction=request.direction)
                    elif request.aggregation == 'sum':
                        ts = degree_agg.sum(direction=request.direction)
                    metric_name = f"{request.aggregation}_degree_{request.direction}"

                elif request.metric == 'property' and request.property_name:
                    prop_agg = tsgen.global_.nodes.property(request.property_name, label=request.label)
                    if request.aggregation == 'avg':
                        ts = prop_agg.avg()
                    elif request.aggregation == 'sum':
                        ts = prop_agg.sum()
                    elif request.aggregation == 'max':
                        ts = prop_agg.max()
                    elif request.aggregation == 'min':
                        ts = prop_agg.min()
                    metric_name = f"{request.aggregation}_{request.property_name}"

            elif request.entity_type == 'edges':
                if request.metric == 'count':
                    ts = tsgen.global_.edges.count(label=request.label)
                    metric_name = f"edge_count{'_' + request.label if request.label else ''}"

            elif request.entity_type == 'graph':
                if request.metric == 'density':
                    ts = tsgen.global_.graph.density(label=request.label, directed=request.directed)
                    metric_name = 'density'
                elif request.metric == 'connected_components':
                    ts = tsgen.global_.graph.connected_components(label=request.label, directed=request.directed)
                    metric_name = 'connected_components'

        elif request.scope == 'entity' and request.entity_id:
            if request.entity_type == 'nodes' and request.metric == 'degree':
                ts = tsgen.entities.nodes.degree(
                    node_id=request.entity_id,
                    direction=request.direction,
                    label=request.label,
                    weight=request.weight
                )
                metric_name = f"degree_{request.entity_id}_{request.direction}"

        if ts is None:
            raise HTTPException(400, "Invalid metric configuration")

        timestamps = []
        values = []

        if hasattr(ts, 'timestamps') and hasattr(ts, 'data'):
            timestamps = [t.isoformat() if hasattr(t, 'isoformat') else str(t) for t in ts.timestamps]
            values = [[v] if not isinstance(v, list) else v for v in ts.data]

        return {
            'sequence_id': seq_id,
            'timeseries': {
                'tsid': f'generated_{seq_id}_{metric_name}',
                'name': metric_name,
                'timestamps': timestamps,
                'data': values,
                'variables': [metric_name],
            },
            'count': len(timestamps)
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))




@app.get("/api/subhygraph/list", tags=["SubHyGraph"])
def list_subhygraphs():
    return {
        'subhygraphs': [
            {
                'id': sid,
                'name': data['name'],
                'description': data.get('description'),
                'node_count': len(data.get('node_ids', [])),
                'filter_query': data.get('filter_query'),
                'created_at': data.get('created_at'),
            }
            for sid, data in SAVED_SUBHYGRAPHS.items()
        ],
        'count': len(SAVED_SUBHYGRAPHS)
    }


@app.post("/api/subhygraph/save", tags=["SubHyGraph"])
def save_subhygraph(request: SaveSubHyGraphRequest):
    try:
        subhygraph_id = f"subhg_{request.name.lower().replace(' ', '_')}_{int(datetime.now().timestamp())}"
        
        SAVED_SUBHYGRAPHS[subhygraph_id] = {
            'name': request.name,
            'description': request.description,
            'node_ids': request.node_ids,
            'filter_query': request.filter_query,
            'metadata': request.metadata or {},
            'created_at': datetime.now().isoformat()
        }
        hg.subHygraph(set(request.node_ids),request.name,request.filter_query)
        return {
            'status': 'saved',
            'id': subhygraph_id,
            'name': request.name,
            'node_count': len(request.node_ids)
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/subhygraph/{subhygraph_id}", tags=["SubHyGraph"])
def get_subhygraph(subhygraph_id: str):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    
    if subhygraph_id not in SAVED_SUBHYGRAPHS:
        raise HTTPException(404, f"SubHyGraph not found: {subhygraph_id}")
    
    try:
        saved = SAVED_SUBHYGRAPHS[subhygraph_id]
        node_ids = set(saved['node_ids'])
        original_count = len(node_ids)  # Count when saved
        
        nodes_response = []
        found_node_ids = set()
        for node_id in node_ids:
            node = hg.crud.get_node_with_timeseries(node_id)
            if node:
                nodes_response.append(node_to_response(node))
                found_node_ids.add(node_id)
        
        # Use found nodes for edge filtering
        all_edges = hg.age.query_edges()
        edges_response = []
        
        for edge_dict in all_edges:
            source_id = str(edge_dict.get('src_uid', ''))
            target_id = str(edge_dict.get('dst_uid', ''))
            if source_id in found_node_ids and target_id in found_node_ids:
                props = edge_dict.get('properties', {})
                edges_response.append({
                    'oid': edge_dict.get('uid'),
                    'source': source_id,
                    'target': target_id,
                    'label': edge_dict.get('label', ''),
                    'static_properties': {k: v for k, v in props.items() if k != 'temporal_properties'},
                    'temporal_properties': props.get('temporal_properties', {})
                })
        
        lats, lngs = [], []
        for node in nodes_response:
            sp = node.get('static_properties', {})
            lat = sp.get('latitude') or sp.get('lat')
            lng = sp.get('longitude') or sp.get('lng') or sp.get('lon')
            if lat and lng:
                lats.append(float(lat))
                lngs.append(float(lng))
        
        # DO NOT update saved node_ids - keep the original list intact
        # The frontend will show a warning if some nodes are missing
        
        return {
            'id': subhygraph_id,
            'name': saved['name'],
            'description': saved.get('description'),
            'filter_query': saved.get('filter_query'),
            'nodes': nodes_response,
            'edges': edges_response,
            'metadata': {
                'hasCoordinates': bool(lats),
                'center': {
                    'lat': sum(lats) / len(lats) if lats else 40.7589,
                    'lng': sum(lngs) / len(lngs) if lngs else -73.9851
                },
                'zoom': 13,
                'node_count': len(nodes_response),
                'edge_count': len(edges_response),
                'original_node_count': original_count,
                'missing_nodes': original_count - len(nodes_response)
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.delete("/api/subhygraph/{subhygraph_id}", tags=["SubHyGraph"])
def delete_subhygraph(subhygraph_id: str):
    if subhygraph_id not in SAVED_SUBHYGRAPHS:
        raise HTTPException(404, f"SubHyGraph not found: {subhygraph_id}")
    
    del SAVED_SUBHYGRAPHS[subhygraph_id]
    return {'status': 'deleted', 'id': subhygraph_id}




@app.post("/api/diff/compare", tags=["Diff"])
def diff_compare(request: DiffRequest):
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        snap1 = hg.snapshot(when=request.timestamp1, mode=request.mode)
        snap2 = hg.snapshot(when=request.timestamp2, mode=request.mode)
        
        diff = HyGraphDiff(snap1, snap2)
        
        added_node_oids = set(str(oid) for oid in diff.added_nodes())
        removed_node_oids = set(str(oid) for oid in diff.removed_nodes())
        persistent_node_oids = set(str(oid) for oid in diff.persistent_nodes())
        
        added_edge_oids = set(str(oid) for oid in diff.added_edges())
        removed_edge_oids = set(str(oid) for oid in diff.removed_edges())
        persistent_edge_oids = set(str(oid) for oid in diff.persistent_edges())
        
        nodes_added = []
        nodes_removed = []
        nodes_unchanged = []
        
        for node in snap2.get_all_nodes():
            node_data = node_to_response(node)
            if str(node.oid) in added_node_oids:
                nodes_added.append(node_data)
            elif str(node.oid) in persistent_node_oids:
                nodes_unchanged.append(node_data)
        
        for node in snap1.get_all_nodes():
            if str(node.oid) in removed_node_oids:
                nodes_removed.append(node_to_response(node))
        
        edges_added = []
        edges_removed = []
        edges_unchanged = []
        
        for edge in snap2.get_all_edges():
            edge_data = edge_to_response(edge)
            if str(edge.oid) in added_edge_oids:
                edges_added.append(edge_data)
            elif str(edge.oid) in persistent_edge_oids:
                edges_unchanged.append(edge_data)
        
        for edge in snap1.get_all_edges():
            if str(edge.oid) in removed_edge_oids:
                edges_removed.append(edge_to_response(edge))
        
        metrics = {
            'node_churn': diff.node_churn(),
            'edge_churn': diff.edge_churn(),
            'jaccard_similarity_nodes': diff.jaccard_similarity_nodes(),
            'jaccard_similarity_edges': diff.jaccard_similarity_edges(),
            'stability_score': diff.stability_score(),
        }
        
        return {
            'timestamp1': request.timestamp1,
            'timestamp2': request.timestamp2,
            'mode': request.mode,
            'nodes_added': nodes_added,
            'nodes_removed': nodes_removed,
            'nodes_unchanged': nodes_unchanged,
            'edges_added': edges_added,
            'edges_removed': edges_removed,
            'edges_unchanged': edges_unchanged,
            'summary': {
                'nodes_added': len(nodes_added),
                'nodes_removed': len(nodes_removed),
                'nodes_unchanged': len(nodes_unchanged),
                'edges_added': len(edges_added),
                'edges_removed': len(edges_removed),
                'edges_unchanged': len(edges_unchanged),
            },
            'metrics': metrics
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/diff/query", tags=["Diff"])
def diff_query(request: DiffQueryRequest):
    """
    Query diff results using the fluent DiffQueryBuilder API.
    
    This allows filtering changed, added, or removed entities based on:
    - Property change thresholds
    - Time series aggregation changes
    - Degree changes (for nodes)
    
    Query types:
    - changed_nodes: Nodes in both snapshots (may have property changes)
    - added_nodes: Nodes in snap2 but not snap1
    - removed_nodes: Nodes in snap1 but not snap2
    - persistent_nodes: Same as changed_nodes
    - changed_edges, added_edges, removed_edges: Same for edges
    
    Example use cases:
    - Find stations where bike availability dropped by more than 5: 
      query_type="changed_nodes", ts_change_filters=[{ts_name: "num_bikes", aggregation: "mean", operator: "<", value: -5}]
    - Find nodes that became major hubs (in-degree increased):
      query_type="changed_nodes", degree_change_filters=[{direction: "in", operator: ">", value: 10}]
    - Find newly added high-capacity stations:
      query_type="added_nodes", static constraint on capacity
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        # Create snapshots and diff
        snap1 = hg.snapshot(when=request.timestamp1, mode=request.mode)
        snap2 = hg.snapshot(when=request.timestamp2, mode=request.mode)
        diff = HyGraphDiff(snap1, snap2)
        
        # Build query using fluent API
        query = diff.query()
        
        # Set query type
        if request.query_type == 'changed_nodes':
            query = query.changed_nodes(label=request.label)
        elif request.query_type == 'added_nodes':
            query = query.added_nodes(label=request.label)
        elif request.query_type == 'removed_nodes':
            query = query.removed_nodes(label=request.label)
        elif request.query_type == 'persistent_nodes':
            query = query.persistent_nodes(label=request.label)
        elif request.query_type == 'changed_edges':
            query = query.changed_edges(label=request.label)
        elif request.query_type == 'added_edges':
            query = query.added_edges(label=request.label)
        elif request.query_type == 'removed_edges':
            query = query.removed_edges(label=request.label)
        else:
            raise HTTPException(400, f"Invalid query_type: {request.query_type}")
        
        # Apply TS change filters
        if request.ts_change_filters:
            for f in request.ts_change_filters:
                query = query.where_ts_change(
                    f.ts_name,
                    f.aggregation,
                    f.operator,
                    f.value
                )
        
        # Apply property change filters
        if request.property_change_filters:
            for f in request.property_change_filters:
                query = query.where_property_change(
                    f.property,
                    f.operator,
                    f.value
                )
        
        # Apply degree change filters (nodes only)
        if request.degree_change_filters and 'node' in request.query_type:
            for f in request.degree_change_filters:
                query = query.where_degree_change(
                    f.direction,
                    f.operator,
                    f.value
                )
        
        # Apply limit
        if request.limit:
            query = query.limit(request.limit)
        
        # Apply ordering
        if request.order_by:
            if request.order_by == 'degree_change':
                query = query.order_by(
                    lambda n: n.degree_change('both'),
                    desc=request.order_desc
                )
            else:
                query = query.order_by(request.order_by, desc=request.order_desc)
        
        # Execute
        results = query.execute()
        
        # Format response
        entities_response = []
        for entity in results:
            entity_data = {
                'oid': entity.oid,
                'label': entity.label,
                'change_type': entity.change_type,
            }
            
            # Add node-specific data
            if hasattr(entity, 'degree_change'):
                entity_data['degree_snap1'] = entity.degree_snap1('both')
                entity_data['degree_snap2'] = entity.degree_snap2('both')
                entity_data['degree_change'] = entity.degree_change('both')
                entity_data['degree_change_in'] = entity.degree_change('in')
                entity_data['degree_change_out'] = entity.degree_change('out')
            
            # Add edge-specific data
            if hasattr(entity, 'source'):
                entity_data['source'] = entity.source
                entity_data['target'] = entity.target
            
            # Add property access
            if entity.snap2_node or entity.snap2_edge if hasattr(entity, 'snap2_edge') else None:
                snap2_entity = entity.snap2_node if hasattr(entity, 'snap2_node') else entity.snap2_edge
                if snap2_entity and hasattr(snap2_entity, 'to_dict'):
                    entity_dict = snap2_entity.to_dict()
                    entity_data['static_properties'] = entity_dict.get('static_properties', {})
                    entity_data['temporal_properties'] = entity_dict.get('temporal_properties', {})
            elif entity.snap1_node or entity.snap1_edge if hasattr(entity, 'snap1_edge') else None:
                snap1_entity = entity.snap1_node if hasattr(entity, 'snap1_node') else entity.snap1_edge
                if snap1_entity and hasattr(snap1_entity, 'to_dict'):
                    entity_dict = snap1_entity.to_dict()
                    entity_data['static_properties'] = entity_dict.get('static_properties', {})
                    entity_data['temporal_properties'] = entity_dict.get('temporal_properties', {})
            
            entities_response.append(entity_data)
        
        return {
            'timestamp1': request.timestamp1,
            'timestamp2': request.timestamp2,
            'mode': request.mode,
            'query_type': request.query_type,
            'results': entities_response,
            'count': len(entities_response),
            'query_info': {
                'label': request.label,
                'ts_change_filters': len(request.ts_change_filters) if request.ts_change_filters else 0,
                'property_change_filters': len(request.property_change_filters) if request.property_change_filters else 0,
                'degree_change_filters': len(request.degree_change_filters) if request.degree_change_filters else 0,
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))






@app.post("/api/diff/interval/compare", tags=["IntervalDiff"])
def interval_diff_compare(request: IntervalDiffRequest):
    """
    Compare two interval snapshots with time series analysis.
    
    Unlike point-in-time diff which compares two moments, interval diff
    compares two time periods and analyzes how time series behaved
    in each interval.
    
    Example use case: Compare morning rush (6-9am) vs evening rush (5-8pm)
    to find stations where bike availability patterns changed.
    
    Returns:
    - Nodes/edges present in both intervals
    - TS comparison metrics (mean_diff, std_diff, trend_changed, correlation)
    - Significant changes based on threshold
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        # Create interval snapshots
        snap1 = hg.snapshot(when=(request.start1, request.end1), mode=request.mode)
        snap2 = hg.snapshot(when=(request.start2, request.end2), mode=request.mode)
        
        # Create interval diff
        diff = HyGraphIntervalDiff(
            snap1, snap2,
            significance_threshold=request.significance_threshold
        )
        
        # Get basic diff info
        persistent_node_oids = set(str(oid) for oid in diff.persistent_nodes())
        added_node_oids = set(str(oid) for oid in diff.added_nodes())
        removed_node_oids = set(str(oid) for oid in diff.removed_nodes())
        
        # Build response for persistent nodes with TS comparison
        nodes_with_ts_comparison = []
        for node in snap2.get_all_nodes():
            node_oid = str(node.oid)
            if node_oid not in persistent_node_oids:
                continue
            
            node_data = node_to_response(node)
            
            # Get TS comparison if available
            ts_comparisons = {}
            temporal_props = node_data.get('temporal_properties', {})
            for ts_name in temporal_props.keys():
                try:
                    comparison = diff.get_ts_comparison(node_oid, ts_name)
                    if comparison:
                        ts_comparisons[ts_name] = {
                            'mean_diff': comparison.mean_diff,
                            'std_diff': comparison.std_diff,
                            'trend_changed': comparison.trend_changed,
                            'correlation': comparison.correlation,
                            'snap1_mean': comparison.snap1_mean,
                            'snap1_std': comparison.snap1_std,
                            'snap2_mean': comparison.snap2_mean,
                            'snap2_std': comparison.snap2_std,
                            'is_significant': comparison.is_significant(
                                request.significance_threshold
                            )
                        }
                except Exception as e:
                    pass
            
            node_data['ts_comparisons'] = ts_comparisons
            nodes_with_ts_comparison.append(node_data)
        
        # Get added/removed nodes
        nodes_added = []
        for node in snap2.get_all_nodes():
            if str(node.oid) in added_node_oids:
                nodes_added.append(node_to_response(node))
        
        nodes_removed = []
        for node in snap1.get_all_nodes():
            if str(node.oid) in removed_node_oids:
                nodes_removed.append(node_to_response(node))
        
        return {
            'interval1': {'start': request.start1, 'end': request.end1},
            'interval2': {'start': request.start2, 'end': request.end2},
            'mode': request.mode,
            'significance_threshold': request.significance_threshold,
            'nodes_persistent': nodes_with_ts_comparison,
            'nodes_added': nodes_added,
            'nodes_removed': nodes_removed,
            'summary': {
                'persistent_count': len(nodes_with_ts_comparison),
                'added_count': len(nodes_added),
                'removed_count': len(nodes_removed),
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/diff/interval/query", tags=["IntervalDiff"])
def interval_diff_query(request: IntervalDiffQueryRequest):
    """
    Query interval diff results with TS comparison filters.
    
    Supports filtering by:
    - mean_diff: Difference in mean values between intervals
    - std_diff: Difference in standard deviation (volatility change)
    - trend_changed: Whether trend direction reversed
    - correlation: Shape similarity between intervals
    - significant: Change exceeds significance threshold
    
    Example use cases:
    - Find stations where morning→evening mean dropped by >5 bikes:
      ts_filters: [{ts_name: "num_bikes", filter_type: "mean_diff", operator: "<", value: -5}]
    - Find stations where trend reversed (morning increasing, evening decreasing):
      ts_filters: [{ts_name: "num_bikes", filter_type: "trend_changed"}]
    - Find anti-correlated patterns (morning peaks when evening dips):
      ts_filters: [{ts_name: "num_bikes", filter_type: "correlation", operator: "<", value: -0.5}]
    """
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")

    try:
        # Create interval snapshots
        snap1 = hg.snapshot(when=(request.start1, request.end1), mode=request.mode)
        snap2 = hg.snapshot(when=(request.start2, request.end2), mode=request.mode)
        
        # Create interval diff
        diff = HyGraphIntervalDiff(snap1, snap2)
        
        # Build query using fluent API
        query = diff.query()
        
        # Set query type
        if request.query_type == 'changed_nodes':
            query = query.changed_nodes(label=request.label)
        elif request.query_type == 'added_nodes':
            query = query.added_nodes(label=request.label)
        elif request.query_type == 'removed_nodes':
            query = query.removed_nodes(label=request.label)
        elif request.query_type == 'persistent_nodes':
            query = query.persistent_nodes(label=request.label)
        else:
            raise HTTPException(400, f"Invalid query_type: {request.query_type}")
        
        # Apply TS comparison filters
        if request.ts_filters:
            for f in request.ts_filters:
                if f.filter_type == 'mean_diff':
                    query = query.where_ts_mean_diff(f.ts_name, f.operator, f.value)
                elif f.filter_type == 'std_diff':
                    query = query.where_ts_std_diff(f.ts_name, f.operator, f.value)
                elif f.filter_type == 'trend_changed':
                    query = query.where_ts_trend_changed(f.ts_name)
                elif f.filter_type == 'correlation':
                    query = query.where_ts_correlation(f.ts_name, f.operator, f.value)
                elif f.filter_type == 'significant':
                    threshold = f.value if f.value else 0.1
                    query = query.where_ts_significant(f.ts_name, threshold)
        
        # Apply degree change filters if present
        if request.degree_change_filters:
            for f in request.degree_change_filters:
                query = query.where_degree_change(
                    f.get('direction', 'both'),
                    f.get('operator', '>'),
                    f.get('value', 0)
                )
        
        # Apply limit
        if request.limit:
            query = query.limit(request.limit)
        
        # Execute
        results = query.execute()
        
        # Format response
        entities_response = []
        for entity in results:
            entity_data = {
                'oid': entity.oid,
                'label': entity.label,
                'change_type': entity.change_type,
            }
            
            # Add TS comparisons
            if hasattr(entity, 'ts_comparisons'):
                ts_comps = {}
                for ts_name, comp in entity.ts_comparisons.items():
                    ts_comps[ts_name] = {
                        'mean_diff': comp.mean_diff,
                        'std_diff': comp.std_diff,
                        'trend_changed': comp.trend_changed,
                        'correlation': comp.correlation,
                    }
                entity_data['ts_comparisons'] = ts_comps
            
            # Add degree change if available
            if hasattr(entity, 'degree_change'):
                entity_data['degree_change'] = entity.degree_change('both')
            
            # Add properties
            snap2_entity = getattr(entity, 'snap2_node', None)
            if snap2_entity and hasattr(snap2_entity, 'to_dict'):
                entity_dict = snap2_entity.to_dict()
                entity_data['static_properties'] = entity_dict.get('static_properties', {})
                entity_data['temporal_properties'] = entity_dict.get('temporal_properties', {})
            
            entities_response.append(entity_data)
        
        return {
            'interval1': {'start': request.start1, 'end': request.end1},
            'interval2': {'start': request.start2, 'end': request.end2},
            'mode': request.mode,
            'query_type': request.query_type,
            'results': entities_response,
            'count': len(entities_response),
            'query_info': {
                'label': request.label,
                'ts_filters': len(request.ts_filters) if request.ts_filters else 0,
                'degree_change_filters': len(request.degree_change_filters) if request.degree_change_filters else 0,
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))

class TSGenSaveRequest(BaseModel):
    """Save a generated TimeSeries to target entity(ies)."""
    timestamps: List[str]
    values: List[float]
    variable_name: str
    source_metric: str
    source_sequence_id: Optional[str] = None
    target_type: str  # node, edge, subhygraph, new
    target_id: Optional[str] = None
    target_label: Optional[str] = None
    overwrite: bool = False
    description: Optional[str] = None


class TSGenVerifyRequest(BaseModel):
    """Verify that a temporal property was saved correctly"""
    entity_type: str  # node, edge
    entity_id: str
    property_name: str


class DiffPatternRequest(BaseModel):
    """Combined diff → pattern matching request"""
    timestamp1: str
    timestamp2: str
    diff_mode: str = "hybrid"
    diff_query_type: str = "changed_nodes"
    diff_label: Optional[str] = None
    diff_ts_filters: Optional[List[Dict[str, Any]]] = None
    diff_degree_filters: Optional[List[Dict[str, Any]]] = None
    diff_target_variable: str = "n1"
    pattern_nodes: List[Dict[str, Any]]
    pattern_edges: Optional[List[Dict[str, Any]]] = None
    cross_constraints: Optional[List[Dict[str, Any]]] = None
    cross_ts_constraints: Optional[List[Dict[str, Any]]] = None
    limit: Optional[int] = None


def _update_entity_temporal_property(hg, entity_type: str, entity_id: str, property_name: str, ts_id: str):
    """Helper to update temporal_properties on a node or edge."""
    if entity_type == 'node':
        node_data = hg.age.get_node(entity_id)
        if not node_data:
            raise ValueError(f"Node not found: {entity_id}")
        props = dict(node_data.get('properties', {}))
        temporal_props = props.get('temporal_properties', {})
        temporal_props[property_name] = ts_id
        props['temporal_properties'] = temporal_props
        props[f'{property_name}_ts_id'] = ts_id
        hg.age.update_node(uid=entity_id, properties=props)
    elif entity_type == 'edge':
        edge_data = hg.age.get_edge(entity_id)
        if not edge_data:
            raise ValueError(f"Edge not found: {entity_id}")
        props = dict(edge_data.get('properties', {}))
        temporal_props = props.get('temporal_properties', {})
        temporal_props[property_name] = ts_id
        props['temporal_properties'] = temporal_props
        props[f'{property_name}_ts_id'] = ts_id
        hg.age.update_edge(uid=entity_id, properties=props)
    else:
        raise ValueError(f"Invalid entity_type: {entity_type}")


@app.post("/api/tsgen/save", tags=["TSGen"])
def tsgen_save(request: TSGenSaveRequest):
    """Save a generated TimeSeries to target entity(ies)."""
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    try:
        parsed_timestamps = [
            datetime.fromisoformat(ts.replace('Z', '+00:00'))
            for ts in request.timestamps
        ]
        saved_ids = []
        errors = []

        if request.target_type == 'new':
            ts_id = f"tsgen_{request.source_metric}_{uuid.uuid4().hex[:8]}"
            ts_obj = TimeSeries(
                tsid=ts_id,
                timestamps=parsed_timestamps,
                variables=[request.variable_name],
                data=[[v] for v in request.values],
                metadata=TimeSeriesMetadata(
                    owner_id='generated',
                    element_type='generated',
                    description=request.description or f"Generated from {request.source_metric}"
                )
            )
            result_id = hg.create_timeseries(ts_obj)
            saved_ids.append({'type': 'new', 'tsid': result_id})
            TSID_METADATA[result_id] = {'variable': request.variable_name, 'owner_id': 'generated', 'owner_type': 'generated'}

        elif request.target_type == 'node':
            if not request.target_id:
                raise HTTPException(400, "target_id required for node target")
            ts_id = f"ts_{request.target_id}_{request.variable_name}_{uuid.uuid4().hex[:8]}"
            ts_obj = TimeSeries(
                tsid=ts_id,
                timestamps=parsed_timestamps,
                variables=[request.variable_name],
                data=[[v] for v in request.values],
                metadata=TimeSeriesMetadata(
                    owner_id=request.target_id,
                    element_type='node',
                    description=request.description or f"Generated from {request.source_metric}"
                )
            )
            result_id = hg.create_timeseries(ts_obj)
            _update_entity_temporal_property(hg, 'node', request.target_id, request.variable_name, result_id)
            saved_ids.append({'type': 'node', 'entity_id': request.target_id, 'property_name': request.variable_name, 'tsid': result_id})
            TSID_METADATA[result_id] = {'variable': request.variable_name, 'owner_id': request.target_id, 'owner_type': 'node'}

        elif request.target_type == 'edge':
            if not request.target_id:
                raise HTTPException(400, "target_id required for edge target")
            ts_id = f"ts_{request.target_id}_{request.variable_name}_{uuid.uuid4().hex[:8]}"
            ts_obj = TimeSeries(
                tsid=ts_id,
                timestamps=parsed_timestamps,
                variables=[request.variable_name],
                data=[[v] for v in request.values],
                metadata=TimeSeriesMetadata(
                    owner_id=request.target_id,
                    element_type='edge',
                    description=request.description or f"Generated from {request.source_metric}"
                )
            )
            result_id = hg.create_timeseries(ts_obj)
            _update_entity_temporal_property(hg, 'edge', request.target_id, request.variable_name, result_id)
            saved_ids.append({'type': 'edge', 'entity_id': request.target_id, 'property_name': request.variable_name, 'tsid': result_id})
            TSID_METADATA[result_id] = {'variable': request.variable_name, 'owner_id': request.target_id, 'owner_type': 'edge'}

        elif request.target_type == 'subhygraph':
            if not request.target_id:
                raise HTTPException(400, "target_id required for subhygraph target")
            if request.target_id not in SAVED_SUBHYGRAPHS:
                raise HTTPException(404, f"SubHyGraph not found: {request.target_id}")
            subhg = SAVED_SUBHYGRAPHS[request.target_id]
            node_ids = list(subhg['node_ids'])
            if request.target_label:
                filtered_ids = []
                for nid in node_ids:
                    try:
                        node = hg.crud.get_node_with_timeseries(nid)
                        if node and node.label == request.target_label:
                            filtered_ids.append(nid)
                    except:
                        pass
                node_ids = filtered_ids
            for node_id in node_ids:
                try:
                    ts_id = f"ts_{node_id}_{request.variable_name}_{uuid.uuid4().hex[:8]}"
                    ts_obj = TimeSeries(
                        tsid=ts_id,
                        timestamps=parsed_timestamps,
                        variables=[request.variable_name],
                        data=[[v] for v in request.values],
                        metadata=TimeSeriesMetadata(
                            owner_id=node_id,
                            element_type='node',
                            description=request.description or f"Generated from {request.source_metric} (SubHyGraph: {request.target_id})"
                        )
                    )
                    result_id = hg.create_timeseries(ts_obj)
                    _update_entity_temporal_property(hg, 'node', node_id, request.variable_name, result_id)
                    saved_ids.append({'type': 'node', 'entity_id': node_id, 'property_name': request.variable_name, 'tsid': result_id})
                    TSID_METADATA[result_id] = {'variable': request.variable_name, 'owner_id': node_id, 'owner_type': 'node'}
                except Exception as e:
                    errors.append({'node_id': node_id, 'error': str(e)})
        else:
            raise HTTPException(400, f"Invalid target_type: {request.target_type}")

        return {
            'success': True,
            'saved': saved_ids,
            'saved_count': len(saved_ids),
            'errors': errors,
            'variable_name': request.variable_name,
            'source_metric': request.source_metric,
            'data_points': len(request.values)
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/tsgen/verify", tags=["TSGen"])
def tsgen_verify(request: TSGenVerifyRequest):
    """Verify that a temporal property was saved correctly to an entity."""
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    try:
        if request.entity_type == 'node':
            entity = hg.crud.get_node_with_timeseries(request.entity_id)
            if not entity:
                raise HTTPException(404, f"Node not found: {request.entity_id}")
            entity_dict = entity.to_dict()
            temporal_props = entity_dict.get('temporal_properties', {})
            if request.property_name not in temporal_props:
                return {
                    'found': False,
                    'entity_type': request.entity_type,
                    'entity_id': request.entity_id,
                    'property_name': request.property_name,
                    'message': f"Property '{request.property_name}' not found",
                    'available_properties': list(temporal_props.keys())
                }
            ts_id = temporal_props[request.property_name]
            ts_data = None
            try:
                meta = TSID_METADATA.get(ts_id, {})
                variable = meta.get('variable') or request.property_name
                measurements = hg.timescale.get_measurements(ts_id, variable)
                if measurements:
                    ts_data = {
                        'tsid': ts_id,
                        'timestamps': [m[0].isoformat() for m in measurements],
                        'values': [m[1] for m in measurements],
                        'count': len(measurements)
                    }
            except Exception as e:
                ts_data = {'error': str(e)}
            return {
                'found': True,
                'entity_type': request.entity_type,
                'entity_id': request.entity_id,
                'property_name': request.property_name,
                'ts_id': ts_id,
                'timeseries': ts_data,
                'all_temporal_properties': temporal_props
            }
        elif request.entity_type == 'edge':
            edges = hg.age.query_edges()
            edge = None
            for e in edges:
                if e.get('uid') == request.entity_id:
                    edge = e
                    break
            if not edge:
                raise HTTPException(404, f"Edge not found: {request.entity_id}")
            props = edge.get('properties', {})
            temporal_props = props.get('temporal_properties', {})
            if request.property_name not in temporal_props:
                return {
                    'found': False,
                    'entity_type': request.entity_type,
                    'entity_id': request.entity_id,
                    'property_name': request.property_name,
                    'message': f"Property '{request.property_name}' not found",
                    'available_properties': list(temporal_props.keys())
                }
            ts_id = temporal_props[request.property_name]
            ts_data = None
            try:
                meta = TSID_METADATA.get(ts_id, {})
                variable = meta.get('variable') or request.property_name
                measurements = hg.timescale.get_measurements(ts_id, variable)
                if measurements:
                    ts_data = {
                        'tsid': ts_id,
                        'timestamps': [m[0].isoformat() for m in measurements],
                        'values': [m[1] for m in measurements],
                        'count': len(measurements)
                    }
            except Exception as e:
                ts_data = {'error': str(e)}
            return {
                'found': True,
                'entity_type': request.entity_type,
                'entity_id': request.entity_id,
                'property_name': request.property_name,
                'ts_id': ts_id,
                'timeseries': ts_data,
                'all_temporal_properties': temporal_props
            }
        else:
            raise HTTPException(400, f"Invalid entity_type: {request.entity_type}")
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))




@app.post("/api/diff/pattern", tags=["DiffPattern"])
def diff_pattern_match(request: DiffPatternRequest):
    """Combined diff → pattern matching."""
    if not hg:
        raise HTTPException(503, "HyGraph not initialized")
    try:
        from hygraph_core.storage.fluent_api import PatternQuery
        snap1 = hg.snapshot(when=request.timestamp1, mode=request.diff_mode)
        snap2 = hg.snapshot(when=request.timestamp2, mode=request.diff_mode)
        diff = HyGraphDiff(snap1, snap2)
        diff_query = diff.query()
        if request.diff_query_type == 'changed_nodes':
            diff_query = diff_query.changed_nodes(label=request.diff_label)
        elif request.diff_query_type == 'added_nodes':
            diff_query = diff_query.added_nodes(label=request.diff_label)
        elif request.diff_query_type == 'removed_nodes':
            diff_query = diff_query.removed_nodes(label=request.diff_label)
        if request.diff_ts_filters:
            for f in request.diff_ts_filters:
                diff_query = diff_query.where_ts_change(f['ts_name'], f.get('aggregation', 'mean'), f.get('operator', '<'), f['value'])
        if request.diff_degree_filters:
            for f in request.diff_degree_filters:
                diff_query = diff_query.where_degree_change(f.get('direction', 'both'), f.get('operator', '>'), f.get('value', 0))
        diff_node_ids = diff_query.as_node_ids()
        if not diff_node_ids:
            return {'matches': [], 'nodes': [], 'edges': [], 'count': 0, 'diff_node_count': 0, 'message': 'No nodes matched diff criteria'}
        pattern = PatternQuery(hg)
        for node_def in request.pattern_nodes:
            var = node_def['variable']
            label = node_def.get('label')
            if var == request.diff_target_variable:
                pattern.node(var, label=label, uid_in=diff_node_ids)
            else:
                pattern.node(var, label=label, uid=node_def.get('uid'))
            static_cons = {}
            ts_cons = {}
            if node_def.get('static_constraints'):
                for sc in node_def['static_constraints']:
                    static_cons[sc['property']] = (sc['operator'], sc['value'])
            if node_def.get('ts_constraints'):
                for tc in node_def['ts_constraints']:
                    if tc.get('type') == 'aggregation':
                        ts_cons[tc['property']] = (tc.get('aggregation', 'mean'), tc.get('operator', '<'), tc['value'])
            if static_cons or ts_cons:
                pattern.where_node(var, static=static_cons if static_cons else None, ts=ts_cons if ts_cons else None)
        if request.pattern_edges:
            for edge_def in request.pattern_edges:
                pattern.edge(edge_def['variable'], label=edge_def.get('label'), source=edge_def['source_var'], target=edge_def['target_var'])
        if request.cross_constraints:
            for cc in request.cross_constraints:
                pattern.where_cross(cc['left'], cc['operator'], cc['right'])
        if request.cross_ts_constraints:
            for ctc in request.cross_ts_constraints:
                pattern.where_cross_ts(ctc['left'], ctc['operator'], ctc['right'], threshold=ctc.get('threshold', 0.7), lag=ctc.get('lag', 0))
        if request.limit:
            pattern.limit(request.limit)
        matches = pattern.execute()
        results = []
        all_node_ids = set()
        all_edge_ids = set()
        for match in matches:
            match_dict = {}
            for var, entity in match.items():
                if hasattr(entity, 'oid'):
                    entity_dict = entity.to_dict() if hasattr(entity, 'to_dict') else {}
                    is_edge = hasattr(entity, 'source')
                    if is_edge:
                        all_edge_ids.add(str(entity.oid))
                    else:
                        all_node_ids.add(str(entity.oid))
                    match_dict[var] = {
                        'oid': str(entity.oid),
                        'label': entity.label if hasattr(entity, 'label') else '',
                        'type': 'edge' if is_edge else 'node',
                        'from_diff': str(entity.oid) in diff_node_ids,
                        'static_properties': entity_dict.get('static_properties', {}),
                        'temporal_properties': entity_dict.get('temporal_properties', {})
                    }
                    if is_edge:
                        match_dict[var]['source'] = str(entity.source)
                        match_dict[var]['target'] = str(entity.target)
            results.append(match_dict)
        return {
            'matches': results,
            'count': len(results),
            'diff_node_count': len(diff_node_ids),
            'diff_node_ids': diff_node_ids,
            'all_node_ids': list(all_node_ids),
            'query_info': {
                'diff_query_type': request.diff_query_type,
                'diff_label': request.diff_label,
                'diff_ts_filters': len(request.diff_ts_filters) if request.diff_ts_filters else 0,
                'pattern_nodes': len(request.pattern_nodes),
                'pattern_edges': len(request.pattern_edges) if request.pattern_edges else 0,
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))






from hygraph_core.operators.predicate_induced import PredicateInducedSubHyGraph
from hygraph_core.operators.tsgen_induced import TSGenFromInduced, SignalAggregatingMetric
from hygraph_core.operators.diff_result import HyGraphDiffResult

INDUCED_CACHE: Dict[str, PredicateInducedSubHyGraph] = {}
DIFF_RESULT_CACHE: Dict[str, HyGraphDiffResult] = {}
DIFF_INDUCED_MAP: Dict[str, str] = {}  # diff_id -> induced_id


class PredicateInducedRequest(BaseModel):
    predicate: str
    start_time: str
    end_time: str
    step_minutes: int = 5
    node_label: str = "Sensor"
    name: Optional[str] = None
    time_filter: Optional[List[int]] = None  # [start_hour, end_hour], e.g. [6, 10]


class SignalMetricRequest(BaseModel):
    name: str                       # display name, e.g. "avg_speed"
    property_name: str = "speed"    # TS variable to aggregate
    aggregation: str = "mean"       # mean, min, max, std, median
    scope: str = "all"              # "all" or "largest_component"

class TSGenInducedRequest(BaseModel):
    induced_id: str
    metrics: Optional[List[str]] = None
    signal_metrics: Optional[List[SignalMetricRequest]] = None


class DiffInducedRequest(BaseModel):
    induced_id_1: str
    induced_id_2: str
    ts_property: Optional[str] = None
    delta_max_days: float = 0.25    # default 6h — same morning only
    period_1_label: str = "period_1"
    period_2_label: str = "period_2"
    min_frequency: float = 0.0      # 0.0 = union; 0.3 = must qualify in 30% of steps
    period_1_start: Optional[str] = None   # temporal slice for induced_1
    period_1_end: Optional[str] = None
    period_2_start: Optional[str] = None   # temporal slice for induced_2
    period_2_end: Optional[str] = None
    tsgen_metrics: Optional[List[str]] = None     # graph-level metrics to compare
    tsgen_signal_metrics: Optional[List[SignalMetricRequest]] = None  # signal aggregations to compare


class DiffQueryFilterRequest(BaseModel):
    diff_id: str
    delta_struct: Optional[str] = None
    nrmse_min: Optional[float] = None
    nrmse_max: Optional[float] = None
    delta_mu_min: Optional[float] = None
    delta_mu_max: Optional[float] = None


@app.post("/api/induced/create", tags=["PredicateInduced"])
async def create_predicate_induced(req: PredicateInducedRequest):
    """Create and evaluate a predicate-induced SubHyGraph."""
    try:
        tf = tuple(req.time_filter) if req.time_filter and len(req.time_filter) == 2 else None
        induced = PredicateInducedSubHyGraph(
            hygraph=hg,
            predicate=req.predicate,
            time_range=(req.start_time, req.end_time),
            step_minutes=req.step_minutes,
            node_label=req.node_label,
            name=req.name,
            time_filter=tf,
        )
        induced.evaluate()
        induced_id = f"induced_{uuid.uuid4().hex[:8]}"
        INDUCED_CACHE[induced_id] = induced
        summary = induced.summary()
        summary["induced_id"] = induced_id
        return {"status": "ok", "induced_id": induced_id, "summary": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/induced/{induced_id}/lifecycle", tags=["PredicateInduced"])
async def get_lifecycle_events(induced_id: str):
    """Get component lifecycle events."""
    induced = INDUCED_CACHE.get(induced_id)
    if not induced:
        raise HTTPException(status_code=404, detail=f"'{induced_id}' not found")
    events = [
        {
            "timestamp": e.timestamp.isoformat(),
            "event_type": e.event_type.value,
            "component_id": e.component_id,
            "member_nodes": list(e.member_nodes),
            "parent_ids": list(e.parent_ids),
            "child_ids": list(e.child_ids),
            "size": e.size,
        }
        for e in induced.lifecycle_events
    ]
    return {"induced_id": induced_id, "events": events, "total": len(events)}


@app.get("/api/induced/{induced_id}/members", tags=["PredicateInduced"])
async def get_members_at_time(induced_id: str, timestamp: str):
    """Get qualifying members at a specific timestamp."""
    induced = INDUCED_CACHE.get(induced_id)
    if not induced:
        raise HTTPException(status_code=404, detail=f"'{induced_id}' not found")
    try:
        from datetime import datetime as dt
        ts = dt.fromisoformat(timestamp)
        members = induced.members_at(ts)
        edges = induced.edges_at(ts)
        components = induced.components_at(ts)
        return {
            "timestamp": timestamp,
            "members": list(members),
            "member_count": len(members),
            "edges": [{"source": s, "target": t} for s, t in edges],
            "edge_count": len(edges),
            "components": [list(c) for c in components],
            "component_count": len(components),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/induced/list", tags=["PredicateInduced"])
async def list_induced():
    """List all cached predicate-induced SubHyGraphs."""
    return {
        "induced_subgraphs": [
            {
                "id": iid,
                "predicate": ind.predicate_str,
                "time_range": [ind.start_time.isoformat(), ind.end_time.isoformat()],
                "timesteps": len(ind.timestamps),
                "evaluated": ind._evaluated,
            }
            for iid, ind in INDUCED_CACHE.items()
        ]
    }


@app.delete("/api/induced/{induced_id}", tags=["PredicateInduced"])
async def delete_induced(induced_id: str):
    """Remove a cached induced SubHyGraph."""
    if induced_id in INDUCED_CACHE:
        del INDUCED_CACHE[induced_id]
        return {"status": "ok", "deleted": induced_id}
    raise HTTPException(status_code=404, detail=f"'{induced_id}' not found")


@app.get("/api/induced/{induced_id}/raw_data", tags=["PredicateInduced"])
async def get_induced_raw_data(induced_id: str):
    """
    Return all raw sensor values, timestamps, adjacency, and predicate info.
    Used by the frontend for client-side threshold adjustment (Option B).
    """
    induced = INDUCED_CACHE.get(induced_id)
    if not induced:
        raise HTTPException(status_code=404, detail=f"'{induced_id}' not found")
    try:
        # 1. Actual evaluated timestamps
        timestamps = [t.isoformat() for t in induced.timestamps]

        # 2. Fetch ALL raw TS values for all sensors across all timestamps
        ts_ids = list(induced._ts_id_to_node_uid.keys())
        ts_property = induced.predicate.ts_property
        start_iso = induced.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_iso = induced.end_time.strftime("%Y-%m-%d %H:%M:%S")

        values_by_node = {}
        if ts_ids:
            ts_id_list = ", ".join(f"'{tid}'" for tid in ts_ids)
            sql = f"""
                SELECT entity_uid, ts, value FROM ts.measurements
                WHERE entity_uid IN ({ts_id_list})
                  AND variable = '{ts_property}'
                  AND ts >= '{start_iso}' AND ts <= '{end_iso}'
                  AND value != 0
                ORDER BY entity_uid, ts
            """
            with hg.db.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    for row in cur.fetchall():
                        ts_id = str(row["entity_uid"])
                        node_uid = induced._ts_id_to_node_uid.get(ts_id)
                        if not node_uid:
                            continue
                        ts_str = row["ts"].isoformat() if hasattr(row["ts"], 'isoformat') else str(row["ts"])
                        val = float(row["value"]) if row["value"] is not None else None
                        if node_uid not in values_by_node:
                            values_by_node[node_uid] = {}
                        values_by_node[node_uid][ts_str] = val

        # 3. Full adjacency between ALL sensors (not just qualifying)
        all_node_uids = list(induced._node_uid_to_ts_id.keys())
        adjacency = []
        if not all_node_uids:
            return {
                "status": "ok", "timestamps": timestamps, "values": {},
                "adjacency": [], "static_props": {},
                "predicate": {
                    "expression": induced.predicate.expression,
                    "operator": induced.predicate.operator,
                    "threshold": induced.predicate.threshold,
                    "ts_property": induced.predicate.ts_property,
                    "is_simple": induced.predicate.is_simple,
                },
                "node_count": 0,
            }
        uid_list = ", ".join(f"'{uid}'" for uid in all_node_uids)
        try:
            results = hg.age.cypher_multi(
                f"""
                MATCH (a:{induced.node_label})-[e]->(b:{induced.node_label})
                WHERE a.uid IN [{uid_list}] AND b.uid IN [{uid_list}]
                RETURN a.uid, b.uid
                """,
                columns=["src", "tgt"]
            )
            for row in results:
                src = str(row.get("src", "")).strip('"')
                tgt = str(row.get("tgt", "")).strip('"')
                if src and tgt:
                    adjacency.append([src, tgt])
        except Exception as e:
            print(f"  WARNING: adjacency fetch failed: {e}")

        # 4. Static props (for expression predicates like speed / free_flow_speed)
        static_props = {}
        if not induced.predicate.is_simple:
            for ts_id, props in induced._ts_id_to_static_props.items():
                node_uid = induced._ts_id_to_node_uid.get(ts_id)
                if node_uid:
                    static_props[node_uid] = props

        # 5. Predicate info
        predicate_info = {
            "expression": induced.predicate.expression,
            "operator": induced.predicate.operator,
            "threshold": induced.predicate.threshold,
            "ts_property": induced.predicate.ts_property,
            "is_simple": induced.predicate.is_simple,
        }

        return {
            "status": "ok",
            "timestamps": timestamps,
            "values": values_by_node,
            "adjacency": adjacency,
            "static_props": static_props,
            "predicate": predicate_info,
            "node_count": len(all_node_uids),
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/tsgen/induced", tags=["TSGenInduced"])
async def tsgen_from_induced_endpoint(req: TSGenInducedRequest):
    """Compute graph-level signals from a predicate-induced SubHyGraph."""
    induced = INDUCED_CACHE.get(req.induced_id)
    if not induced:
        raise HTTPException(status_code=404, detail=f"'{req.induced_id}' not found")
    try:
        signal_metrics_objs = []
        if req.signal_metrics:
            for sm in req.signal_metrics:
                signal_metrics_objs.append(SignalAggregatingMetric(
                    name=sm.name,
                    property_name=sm.property_name,
                    aggregation=sm.aggregation,
                    scope=sm.scope,
                ))
        tsgen = TSGenFromInduced(
            induced=induced,
            metrics=req.metrics,
            signal_metrics=signal_metrics_objs,
        )
        results = tsgen.compute()
        ts_data = {}
        for metric_name, ts in results.items():
            ts_data[metric_name] = {
                "timestamps": [
                    t.isoformat() if hasattr(t, 'isoformat') else str(t)
                    for t in ts.timestamps
                ],
                "values": [
                    float(v[0]) if isinstance(v, (list, tuple)) else float(v)
                    for v in ts.data
                ],
                "length": ts.length,
            }
        return {"status": "ok", "induced_id": req.induced_id, "metrics": ts_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/diff/induced", tags=["DiffInduced"])
async def diff_from_induced_endpoint(req: DiffInducedRequest):
    """Run HyGraphDiff on two predicate-induced SubHyGraphs."""
    induced_1 = INDUCED_CACHE.get(req.induced_id_1)
    induced_2 = INDUCED_CACHE.get(req.induced_id_2)
    if not induced_1:
        raise HTTPException(status_code=404, detail=f"'{req.induced_id_1}' not found")
    if not induced_2:
        raise HTTPException(status_code=404, detail=f"'{req.induced_id_2}' not found")
    try:
        # Build period tuples if provided
        p1 = (req.period_1_start, req.period_1_end) if req.period_1_start and req.period_1_end else None
        p2 = (req.period_2_start, req.period_2_end) if req.period_2_start and req.period_2_end else None
        diff_hg = hg.diff_from_induced(
            induced=induced_1, induced_2=induced_2,
            ts_property=req.ts_property, delta_max_days=req.delta_max_days,
            period_1_label=req.period_1_label, period_2_label=req.period_2_label,
            min_frequency=req.min_frequency,
            period_1=p1, period_2=p2,
        )

        # Run TSGen on both inputs and compare graph-level signals if requested
        if req.tsgen_metrics or req.tsgen_signal_metrics:
            from hygraph_core.operators.tsgen_induced import TSGenFromInduced, SignalAggregatingMetric as SAM
            sig_objs = []
            if req.tsgen_signal_metrics:
                for sm in req.tsgen_signal_metrics:
                    sig_objs.append(SAM(name=sm.name, property_name=sm.property_name,
                                        aggregation=sm.aggregation, scope=sm.scope))
            metrics_list = req.tsgen_metrics or ['density', 'component_count', 'largest_component']
            # Pass period time ranges so TSGen filters to each period
            from datetime import datetime as dt
            tr1 = (dt.fromisoformat(req.period_1_start), dt.fromisoformat(req.period_1_end)) if req.period_1_start and req.period_1_end else None
            tr2 = (dt.fromisoformat(req.period_2_start), dt.fromisoformat(req.period_2_end)) if req.period_2_start and req.period_2_end else None
            ts1 = TSGenFromInduced(induced_1, metrics=metrics_list, signal_metrics=sig_objs, time_range=tr1).compute()
            ts2 = TSGenFromInduced(induced_2, metrics=metrics_list, signal_metrics=sig_objs, time_range=tr2).compute()
            diff_hg.diff_result.add_graph_level_comparisons(ts1, ts2)

        diff_id = f"diff_{uuid.uuid4().hex[:8]}"
        DIFF_RESULT_CACHE[diff_id] = diff_hg.diff_result
        DIFF_INDUCED_MAP[diff_id] = req.induced_id_1  # track source
        response = diff_hg.diff_result.to_dict()
        response["diff_id"] = diff_id
        return {"status": "ok", **response}
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/diff/induced/query", tags=["DiffInduced"])
async def query_diff_result_endpoint(req: DiffQueryFilterRequest):
    """Query a diff result by hybrid change descriptor filters."""
    diff_result = DIFF_RESULT_CACHE.get(req.diff_id)
    if not diff_result:
        raise HTTPException(status_code=404, detail=f"'{req.diff_id}' not found")
    try:
        entities = diff_result.query_by_descriptor(
            delta_struct=req.delta_struct, nrmse_min=req.nrmse_min,
            nrmse_max=req.nrmse_max, delta_mu_min=req.delta_mu_min,
            delta_mu_max=req.delta_mu_max,
        )
        return {"status": "ok", "diff_id": req.diff_id, "count": len(entities),
                "entities": [e.to_dict() for e in entities]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diff/induced/{diff_id}/cpg", tags=["DiffInduced"])
async def get_cpg_endpoint(diff_id: str):
    """Get the Change Propagation Graph for a diff result."""
    diff_result = DIFF_RESULT_CACHE.get(diff_id)
    if not diff_result:
        raise HTTPException(status_code=404, detail=f"'{diff_id}' not found")
    if not diff_result.cpg:
        return {"status": "ok", "cpg": None, "message": "No CPG (no ADDED entities)"}
    return {"status": "ok", "diff_id": diff_id, "cpg": diff_result.cpg.to_dict()}


@app.get("/api/diff/induced/{diff_id}/cpg/paths/{root_id}", tags=["DiffInduced"])
async def get_cpg_paths_endpoint(diff_id: str, root_id: str):
    """Get propagation paths from a specific CPG root."""
    diff_result = DIFF_RESULT_CACHE.get(diff_id)
    if not diff_result:
        raise HTTPException(status_code=404, detail=f"'{diff_id}' not found")
    if not diff_result.cpg:
        return {"status": "ok", "paths": []}
    try:
        paths = diff_result.cpg.get_paths_from(root_id)
        return {
            "status": "ok", "root_id": root_id, "path_count": len(paths),
            "paths": [
                {
                    "nodes": p.nodes, "length": p.length,
                    "total_delay_days": round(p.total_delay_days, 2),
                    "edges": [{"source": e.source, "target": e.target,
                               "delay_days": round(e.delay_days, 2)} for e in p.edges],
                }
                for p in paths
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# PREDICATE DIFF — single-step endpoint: specify predicate + two date ranges
# ─────────────────────────────────────────────────────────────────────────────

class PredicateDiffRequest(BaseModel):
    predicate: str
    period_1_start: str
    period_1_end: str
    period_2_start: str
    period_2_end: str
    period_1_label: str = "Period 1"
    period_2_label: str = "Period 2"
    step_minutes: int = 60
    node_label: str = "Sensor"
    time_filter: Optional[List[int]] = None   # [start_hour, end_hour], shared by both periods
    min_frequency: float = 0.0
    delta_max_days: float = 0.25


@app.post("/api/diff/predicate", tags=["DiffInduced"])
async def diff_from_predicate_endpoint(req: PredicateDiffRequest):
    """
    Single-step HyGraphDiff: specify predicate + two date ranges.
    Evaluates both predicate-induced subgraphs internally and returns the diff.
    The time_filter (hour window) is applied to both periods equally.
    This is the preferred endpoint for the four demo scenarios.
    """
    try:
        tf = tuple(req.time_filter) if req.time_filter and len(req.time_filter) == 2 else None

        induced_1 = PredicateInducedSubHyGraph(
            hygraph=hg,
            predicate=req.predicate,
            time_range=(req.period_1_start, req.period_1_end),
            step_minutes=req.step_minutes,
            node_label=req.node_label,
            name=req.period_1_label,
            time_filter=tf,
        ).evaluate()

        induced_2 = PredicateInducedSubHyGraph(
            hygraph=hg,
            predicate=req.predicate,
            time_range=(req.period_2_start, req.period_2_end),
            step_minutes=req.step_minutes,
            node_label=req.node_label,
            name=req.period_2_label,
            time_filter=tf,
        ).evaluate()

        diff_hg = hg.diff_from_induced(
            induced=induced_1,
            induced_2=induced_2,
            delta_max_days=req.delta_max_days,
            period_1_label=req.period_1_label,
            period_2_label=req.period_2_label,
            min_frequency=req.min_frequency,
        )

        diff_id = f"diff_{uuid.uuid4().hex[:8]}"
        DIFF_RESULT_CACHE[diff_id] = diff_hg.diff_result
        # Also cache the induced subgraphs in case user wants TSGen later
        INDUCED_CACHE[f"{diff_id}_p1"] = induced_1
        INDUCED_CACHE[f"{diff_id}_p2"] = induced_2

        response = diff_hg.diff_result.to_dict()
        response["diff_id"] = diff_id
        response["induced_id_1"] = f"{diff_id}_p1"
        response["induced_id_2"] = f"{diff_id}_p2"
        return {"status": "ok", **response}
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diff/induced/list", tags=["DiffInduced"])
async def list_diff_results_endpoint():
    """List all cached diff results."""
    return {
        "diff_results": [
            {"id": did, "period_1": dr._period_1_label,
             "period_2": dr._period_2_label, "summary": dr.summary.to_dict()}
            for did, dr in DIFF_RESULT_CACHE.items()
        ]
    }


@app.get("/api/ts_variables", tags=["TimeSeries"])
async def list_ts_variables():
    """List all unique time series variable names from the database."""
    try:
        with hg.db.conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT variable FROM ts.measurements ORDER BY variable")
                variables = [row['variable'] for row in cur.fetchall()]
        return {"variables": variables}
    except Exception as e:
        return {"variables": ["speed"]}  # fallback


@app.get("/api/instances", tags=["Instances"])
async def list_instances():
    """List all available HyGraph instances for the main page selector."""
    instances = []
    seen_ids = set()
    # Full HyGraph
    if hg:
        n_count = len(hg.age.query_nodes()) if hasattr(hg, 'age') else 0
        instances.append({"id": "__full__", "type": "hygraph", "name": "Full HyGraph",
                          "detail": f"{n_count} nodes"})
    # Saved SubHyGraphs
    for sid, sg in SAVED_SUBHYGRAPHS.items():
        inst_id = f"sub_{sid}"
        if inst_id not in seen_ids:
            seen_ids.add(inst_id)
            instances.append({"id": inst_id, "type": "subhygraph", "name": sg['name'],
                              "detail": f"{len(sg['node_ids'])} nodes"})
    # Induced SubHyGraphs
    for iid, ind in INDUCED_CACHE.items():
        inst_id = f"ind_{iid}"
        if inst_id not in seen_ids:
            seen_ids.add(inst_id)
            ind_name = ind.name if hasattr(ind, 'name') else iid
            instances.append({"id": inst_id, "type": "induced", "name": ind_name,
                              "detail": f"{ind.summary()['unique_members']} members, {len(ind.timestamps)} steps"})
    # Diff results
    for did, dr in DIFF_RESULT_CACHE.items():
        inst_id = f"diff_{did}"
        if inst_id not in seen_ids:
            seen_ids.add(inst_id)
            instances.append({"id": inst_id, "type": "diff",
                              "name": f"{dr._period_1_label} vs {dr._period_2_label}",
                              "detail": f"+{dr.summary.added_count} -{dr.summary.removed_count} ~{dr.summary.persisted_count}"})
    return {"instances": instances}


@app.get("/api/instances/{instance_id}/data", tags=["Instances"])
async def get_instance_data(instance_id: str):
    """Get graph data for any instance, formatted for the main page GraphView."""
    try:
      return await _get_instance_data_impl(instance_id)
    except HTTPException:
      raise
    except Exception as e:
      import traceback; traceback.print_exc()
      raise HTTPException(status_code=500, detail=str(e))

async def _get_instance_data_impl(instance_id: str):
    if instance_id == "__full__":
        # Redirect to normal /api/graph
        return None  # Frontend will use normal fetchGraphData

    if instance_id.startswith("sub_"):
        sid = instance_id[4:]
        if sid not in SAVED_SUBHYGRAPHS:
            raise HTTPException(404, f"SubHyGraph '{sid}' not found")
        return get_subhygraph(sid)

    if instance_id.startswith("ind_"):
        iid = instance_id[4:]
        induced = INDUCED_CACHE.get(iid)
        if not induced:
            raise HTTPException(404, f"Induced '{iid}' not found")
        # Return all unique member nodes as graph data
        all_members = set()
        for change in induced._membership_history:
            all_members.update(change.current_members)
        nodes_response = []
        for nid in all_members:
            node = hg.crud.get_node_with_timeseries(nid)
            if node:
                nodes_response.append(node_to_response(node))
        # Get edges between members
        found_ids = {n['oid'] for n in nodes_response}
        all_edges = hg.age.query_edges()
        edges_response = []
        for e in all_edges:
            src = str(e.get('src_uid', ''))
            tgt = str(e.get('dst_uid', ''))
            if src in found_ids and tgt in found_ids:
                props = e.get('properties', {})
                edges_response.append({
                    'oid': e.get('uid'), 'source': src, 'target': tgt,
                    'label': e.get('label', ''),
                    'static_properties': {k: v for k, v in props.items() if k != 'temporal_properties'},
                    'temporal_properties': props.get('temporal_properties', {}),
                })
        return {'nodes': nodes_response, 'edges': edges_response, 'metadata': {'hasCoordinates': True}}

    if instance_id.startswith("diff_"):
        did = instance_id[5:]
        dr = DIFF_RESULT_CACHE.get(did)
        if not dr:
            raise HTTPException(404, f"Diff '{did}' not found")

        # Build nodes with diff status
        nodes = []
        node_ids_shown = set()
        for nid, entity in dr.nodes.items():
            node = hg.crud.get_node_with_timeseries(nid) if hg else None
            resp = node_to_response(node) if node else {
                'oid': nid, 'label': entity.label,
                'static_properties': entity.properties, 'temporal_properties': {},
            }
            resp['diffStatus'] = entity.change_descriptor.delta_struct
            resp['nrmse'] = entity.change_descriptor.nrmse
            resp['delta_mu'] = entity.change_descriptor.delta_mu
            resp['sigma_ratio'] = entity.change_descriptor.sigma_ratio
            # Also put in static_properties so Simple Filter can query them
            sp = resp.get('static_properties', {})
            sp['delta_struct'] = entity.change_descriptor.delta_struct
            if entity.change_descriptor.nrmse is not None:
                sp['nRMSE'] = round(entity.change_descriptor.nrmse, 4)
            if entity.change_descriptor.delta_mu is not None:
                sp['delta_mu'] = round(entity.change_descriptor.delta_mu, 4)
            if entity.change_descriptor.sigma_ratio is not None:
                sp['sigma_ratio'] = round(entity.change_descriptor.sigma_ratio, 4)
            if entity.change_descriptor.delta_slope is not None:
                sp['delta_slope'] = round(entity.change_descriptor.delta_slope, 4)
            resp['static_properties'] = sp
            nodes.append(resp)
            node_ids_shown.add(nid)

        # Real graph edges between classified nodes
        edges = []
        all_edges = hg.age.query_edges()
        for e in all_edges:
            src = str(e.get('src_uid', ''))
            tgt = str(e.get('dst_uid', ''))
            if src in node_ids_shown and tgt in node_ids_shown:
                props = e.get('properties', {})
                edges.append({
                    'oid': e.get('uid'), 'source': src, 'target': tgt,
                    'label': e.get('label', ''),
                    'static_properties': {k: v for k, v in props.items() if k != 'temporal_properties'},
                    'temporal_properties': props.get('temporal_properties', {}),
                })
        # CPG propagation edges
        cpg_edges = []
        if dr.cpg and dr.cpg.edges:
            for i, pe in enumerate(dr.cpg.edges):
                cpg_edges.append({
                    'source': pe.source, 'target': pe.target,
                    'delay_days': pe.delay_days,
                    'source_timestamp': pe.source_timestamp.isoformat() if pe.source_timestamp else None,
                    'target_timestamp': pe.target_timestamp.isoformat() if pe.target_timestamp else None,
                })
        # Compute center from node positions
        lats, lngs = [], []
        for n in nodes:
            sp = n.get('static_properties', {})
            lat = sp.get('lat') or sp.get('latitude')
            lng = sp.get('lon') or sp.get('lng') or sp.get('longitude')
            if lat and lng:
                lats.append(float(lat))
                lngs.append(float(lng))
        center = {
            'lat': sum(lats)/len(lats) if lats else 34.05,
            'lng': sum(lngs)/len(lngs) if lngs else -118.25,
        }
        return {
            'nodes': nodes, 'edges': edges, 'cpg_edges': cpg_edges,
            'isDiff': True,
            'metadata': {'hasCoordinates': bool(lats), 'isDiff': True,
                         'center': center, 'zoom': 11,
                         'period_1': dr._period_1_label, 'period_2': dr._period_2_label,
                         'period_1_start': dr._p1_start.isoformat() if dr._p1_start else None,
                         'period_1_end': dr._p1_end.isoformat() if dr._p1_end else None,
                         'period_2_start': dr._p2_start.isoformat() if dr._p2_start else None,
                         'period_2_end': dr._p2_end.isoformat() if dr._p2_end else None,
                         'cpg_roots': list(dr.cpg.roots) if dr.cpg else [],
                         'graph_level_comparisons': [c.to_dict() for c in dr.graph_level_comparisons] if dr.graph_level_comparisons else None},
        }

    raise HTTPException(404, f"Unknown instance '{instance_id}'")


if __name__ == "__main__":
    import uvicorn

    print("=" * 70)
    print("HYGRAPH WEB API v4.4.0 - VLDB Demo Operators")
    print("=" * 70)
    print()
    print("API Server: http://localhost:8000")
    print("API Docs:   http://localhost:8000/docs")
    print()
    print("=" * 70)

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
