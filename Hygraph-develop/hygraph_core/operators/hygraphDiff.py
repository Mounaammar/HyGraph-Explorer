from dataclasses import field, dataclass
from typing import List, Set, Optional, Any, Dict, Callable, Union
from hygraph_core.model import PGNode, PGEdge, TimeSeries
from hygraph_core.operators.temporal_snapshot import Snapshot
import numpy as np




#=================================================================
# Helper functions
#=================================================================
def display_property(d: Dict[str, any], limit: int = 5):
    if not d:
        return "Empty dictionary"
    items = list(d.items())
    head = items[:limit]
    s = ",".join(f"{k} = {v!r}" for k, v in head)
    if len(items) > limit:
        s += f",... (+{len(items) - limit})"
    return s

#=================================================================

class PropertyChange:
    """For a single property mdofication"""
    old:Any
    new:Any
    delta:Optional[float]
    def __init__(self, old: Any, new: Any, delta: Optional[float] = None):
        self.old = old
        self.new = new
        self.delta=delta
    def __repr__(self):
        if self.delta is not None and isinstance(self.delta, (float,int)) and not isinstance(self.delta, (str,bool)):
            return f"{self.old} -> {self.new}, (Δ={self.delta})"
        return f"{self.old} -> {self.new}"


# =============================================================================
# TIME SERIES COMPARISON - For Interval Diff
# =============================================================================

@dataclass
class TSComparison:
    """
    Comparison result between two time series from different intervals.
    
    Since time series values are immutable, we compare:
    - Statistical profile (mean, std, min, max)
    - Trend direction (increasing, decreasing, stable)
    - Shape similarity (correlation, DTW distance)
    - Temporal coverage
    
    Example:
        Morning vs Evening comparison for a station's bike availability:
        - ts1 (morning): mean=15, trend=decreasing (commuters taking bikes)
        - ts2 (evening): mean=8, trend=increasing (bikes returning)
        - correlation=-0.7 (anti-correlated patterns)
    """
    ts_name: str
    
    # The actual TimeSeries objects (sliced to their intervals)
    ts1: Optional['TimeSeries'] = None
    ts2: Optional['TimeSeries'] = None
    
    # Statistical comparisons
    ts1_mean: Optional[float] = None
    ts2_mean: Optional[float] = None
    mean_diff: Optional[float] = None  # ts2_mean - ts1_mean
    mean_diff_percent: Optional[float] = None
    
    ts1_std: Optional[float] = None
    ts2_std: Optional[float] = None
    std_diff: Optional[float] = None
    
    ts1_min: Optional[float] = None
    ts2_min: Optional[float] = None
    min_diff: Optional[float] = None
    
    ts1_max: Optional[float] = None
    ts2_max: Optional[float] = None
    max_diff: Optional[float] = None
    
    ts1_sum: Optional[float] = None
    ts2_sum: Optional[float] = None
    sum_diff: Optional[float] = None
    
    # Trend comparisons
    ts1_trend: Optional[str] = None  # 'increasing', 'decreasing', 'stable', 'volatile'
    ts2_trend: Optional[str] = None
    trend_changed: bool = False
    
    # Shape comparison
    correlation: Optional[float] = None  # Pearson correlation (if alignable)
    dtw_distance: Optional[float] = None  # Dynamic Time Warping distance
    shape_similarity: Optional[float] = None  # 0-1 score
    
    # Temporal coverage
    ts1_length: int = 0
    ts2_length: int = 0
    ts1_start: Optional[Any] = None
    ts1_end: Optional[Any] = None
    ts2_start: Optional[Any] = None
    ts2_end: Optional[Any] = None
    
    # Change detection
    has_significant_change: bool = False  # Based on configurable threshold
    change_magnitude: Optional[float] = None  # Normalized change score
    
    def __repr__(self):
        return (
            f"TSComparison({self.ts_name}): "
            f"mean {self.ts1_mean:.1f}->{self.ts2_mean:.1f} (Δ={self.mean_diff:+.1f}), "
            f"trend {self.ts1_trend}->{self.ts2_trend}, "
            f"corr={self.correlation:.2f}" if self.correlation else ""
        )


def compute_ts_comparison(
    ts_name: str,
    ts1: Optional['TimeSeries'],
    ts2: Optional['TimeSeries'],
    significance_threshold: float = 0.1
) -> TSComparison:
    """
    Compute comprehensive comparison between two time series.
    
    Args:
        ts_name: Name of the temporal property
        ts1: TimeSeries from first interval (can be None)
        ts2: TimeSeries from second interval (can be None)
        significance_threshold: Relative change threshold for "significant" flag
    
    Returns:
        TSComparison with all computed metrics
    """
    comp = TSComparison(ts_name=ts_name, ts1=ts1, ts2=ts2)
    
    # Handle missing time series
    if ts1 is None and ts2 is None:
        return comp
    
    # Compute ts1 statistics
    if ts1 is not None and ts1.length > 0:
        comp.ts1_length = ts1.length
        comp.ts1_start = ts1.timestamps[0] if ts1.timestamps else None
        comp.ts1_end = ts1.timestamps[-1] if ts1.timestamps else None
        try:
            comp.ts1_mean = ts1.mean()
            comp.ts1_std = ts1.std()
            comp.ts1_min = ts1.min()
            comp.ts1_max = ts1.max()
            comp.ts1_sum = ts1.sum()
            comp.ts1_trend = _detect_trend(ts1)
        except Exception:
            pass
    
    # Compute ts2 statistics
    if ts2 is not None and ts2.length > 0:
        comp.ts2_length = ts2.length
        comp.ts2_start = ts2.timestamps[0] if ts2.timestamps else None
        comp.ts2_end = ts2.timestamps[-1] if ts2.timestamps else None
        try:
            comp.ts2_mean = ts2.mean()
            comp.ts2_std = ts2.std()
            comp.ts2_min = ts2.min()
            comp.ts2_max = ts2.max()
            comp.ts2_sum = ts2.sum()
            comp.ts2_trend = _detect_trend(ts2)
        except Exception:
            pass
    
    # Compute differences
    if comp.ts1_mean is not None and comp.ts2_mean is not None:
        comp.mean_diff = comp.ts2_mean - comp.ts1_mean
        if abs(comp.ts1_mean) > 1e-9:
            comp.mean_diff_percent = (comp.mean_diff / abs(comp.ts1_mean)) * 100
    
    if comp.ts1_std is not None and comp.ts2_std is not None:
        comp.std_diff = comp.ts2_std - comp.ts1_std
    
    if comp.ts1_min is not None and comp.ts2_min is not None:
        comp.min_diff = comp.ts2_min - comp.ts1_min
    
    if comp.ts1_max is not None and comp.ts2_max is not None:
        comp.max_diff = comp.ts2_max - comp.ts1_max
    
    if comp.ts1_sum is not None and comp.ts2_sum is not None:
        comp.sum_diff = comp.ts2_sum - comp.ts1_sum
    
    # Trend change detection
    if comp.ts1_trend is not None and comp.ts2_trend is not None:
        comp.trend_changed = comp.ts1_trend != comp.ts2_trend
    
    # Shape comparison (correlation)
    if ts1 is not None and ts2 is not None and ts1.length >= 2 and ts2.length >= 2:
        try:
            comp.correlation = _compute_correlation(ts1, ts2)
            comp.shape_similarity = (comp.correlation + 1) / 2 if comp.correlation is not None else None
        except Exception:
            pass
        
        # DTW distance (if available)
        try:
            comp.dtw_distance = _compute_dtw_distance(ts1, ts2)
        except Exception:
            pass
    
    # Significance detection
    if comp.mean_diff_percent is not None:
        comp.has_significant_change = abs(comp.mean_diff_percent) > (significance_threshold * 100)
        comp.change_magnitude = abs(comp.mean_diff_percent) / 100
    
    return comp


def _detect_trend(ts: 'TimeSeries') -> str:
    """
    Detect the overall trend of a time series.
    
    Returns: 'increasing', 'decreasing', 'stable', or 'volatile'
    """
    if ts is None or ts.length < 2:
        return 'unknown'
    
    try:
        values = ts.to_numpy().flatten()
        if len(values) < 2:
            return 'unknown'
        
        # Simple linear regression slope
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        
        # Coefficient of variation for volatility
        mean_val = np.mean(values)
        std_val = np.std(values)
        cv = std_val / (abs(mean_val) + 1e-9)
        
        # Thresholds
        slope_threshold = std_val * 0.1  # Slope relative to variability
        volatility_threshold = 0.5  # CV threshold
        
        if cv > volatility_threshold:
            return 'volatile'
        elif slope > slope_threshold:
            return 'increasing'
        elif slope < -slope_threshold:
            return 'decreasing'
        else:
            return 'stable'
    except Exception:
        return 'unknown'


def _compute_correlation(ts1: 'TimeSeries', ts2: 'TimeSeries') -> Optional[float]:
    """
    Compute Pearson correlation between two time series.
    
    If lengths differ, uses interpolation or truncation.
    """
    try:
        v1 = ts1.to_numpy().flatten()
        v2 = ts2.to_numpy().flatten()
        
        if len(v1) == 0 or len(v2) == 0:
            return None
        
        # Align lengths by interpolation
        if len(v1) != len(v2):
            # Interpolate the shorter one to match the longer
            target_len = max(len(v1), len(v2))
            if len(v1) < target_len:
                v1 = np.interp(
                    np.linspace(0, 1, target_len),
                    np.linspace(0, 1, len(v1)),
                    v1
                )
            if len(v2) < target_len:
                v2 = np.interp(
                    np.linspace(0, 1, target_len),
                    np.linspace(0, 1, len(v2)),
                    v2
                )
        
        # Compute Pearson correlation
        corr = np.corrcoef(v1, v2)[0, 1]
        return float(corr) if not np.isnan(corr) else None
    except Exception:
        return None


def _compute_dtw_distance(ts1: 'TimeSeries', ts2: 'TimeSeries') -> Optional[float]:
    """
    Compute Dynamic Time Warping distance between two time series.
    
    Uses a simple implementation; for production, consider using dtw-python or tslearn.
    """
    try:
        v1 = ts1.to_numpy().flatten()
        v2 = ts2.to_numpy().flatten()
        
        if len(v1) == 0 or len(v2) == 0:
            return None
        
        # Simple DTW implementation
        n, m = len(v1), len(v2)
        dtw_matrix = np.full((n + 1, m + 1), np.inf)
        dtw_matrix[0, 0] = 0
        
        for i in range(1, n + 1):
            for j in range(1, m + 1):
                cost = abs(v1[i-1] - v2[j-1])
                dtw_matrix[i, j] = cost + min(
                    dtw_matrix[i-1, j],    # insertion
                    dtw_matrix[i, j-1],    # deletion
                    dtw_matrix[i-1, j-1]   # match
                )
        
        # Normalize by path length
        distance = dtw_matrix[n, m] / (n + m)
        return float(distance)
    except Exception:
        return None
@dataclass
class HyGraphDiffNode:
    """A Node difference object representing the difference between two nodes with annotations"""
    oid: Any
    label: str
    persistent_properties: Dict[str,any]=field(default_factory=dict)
    added_properties: Dict[str,any]=field(default_factory=dict)
    removed_properties: Dict[str,any]=field(default_factory=dict)
    changed_properties: Dict[str,PropertyChange]=field(default_factory=dict)
    def has_changes(self) -> bool:
        return bool(self.changed_properties or self.added_properties or self.removed_properties)
    def change_count(self):
        return len(self.changed_properties)+len(self.added_properties)+len(self.removed_properties)
    def get_delta(self,property_name:str) -> Optional[float]:
        if property_name in self.changed_properties:
            return self.changed_properties[property_name].delta
        return None


    def __repr__(self):
     return[   f"{self.label} Node with id={self.oid}\n"
        f"Perisstent properties: {display_property(self.persistent_properties)}\n "
        f"Added properties: {display_property(self.added_properties)}\n "
        f"Removed properties: {display_property(self.removed_properties)}\n "
        f"Changed properties: {display_property(self.changed_properties)}"]


@dataclass
class HyGraphDiffEdge:
    "An Edge difference object representing the difference between two edges with annotations"
    oid: Any
    label: str
    source: Any
    target: Any
    persistent_properties: Dict[str, any] = field(default_factory=dict)
    added_properties: Dict[str, any] = field(default_factory=dict)
    removed_properties: Dict[str, any] = field(default_factory=dict)
    changed_properties: Dict[str, PropertyChange] = field(default_factory=dict)

    def has_changes(self) -> bool:
        return bool(self.changed_properties or self.added_properties or self.removed_properties)

    def change_count(self):
        return len(self.changed_properties) + len(self.added_properties) + len(self.removed_properties)

    def get_delta(self, property_name: str) -> Optional[float]:
        if property_name in self.changed_properties:
            return self.changed_properties[property_name].delta
        return None

    def __repr__(self):
         return[ f"{self.label} Edge with id={self.oid} going from {self.source} to {self.target}\n"
                 f"Perisstent properties: {display_property(self.persistent_properties)}\n "
                 f"Added properties: {display_property(self.added_properties)}\n "
                 f"Removed properties: {display_property(self.removed_properties)}\n "
                 f"Changed properties: {display_property(self.changed_properties)}"]



class HyGraphDiff:
    """
    Compares two POINT Snapshot objects and finds differences in nodes, edges, and properties.

    Structural diff (added/removed entities):
    - Works for both "graph" and "hybrid" modes
    - Compares which entities exist in snap1 vs snap2

    Property diff (changed values):
    - Only works for "hybrid" mode (requires resolved scalar values)
    - Compares numeric property values between snapshots
    - Both snapshots must be point snapshots (not intervals)

    Usage:
        snap1 = hg.snapshot(when="2024-01-15T10:00:00", mode="hybrid")
        snap2 = hg.snapshot(when="2024-01-15T11:00:00", mode="hybrid")
        diff = HygraphDiff(snap1, snap2)

        diff.added_nodes()      # Nodes in snap2 but not snap1
        diff.removed_nodes()    # Nodes in snap1 but not snap2
        diff.changed_nodes()    # Nodes in both with property value differences
    """

    def __init__(self, sn1: Snapshot, sn2: Snapshot):
        # Validate both snapshots have same mode
        if sn1.mode != sn2.mode:
            raise ValueError(
                f"Cannot compare snapshots with different modes: {sn1.mode} vs {sn2.mode}"
            )

        # Validate both are point snapshots (not intervals)
        if sn1.is_interval() or sn2.is_interval():
            raise ValueError(
                "HygraphDiff only supports point snapshots. "
                "Comparing interval snapshots (temporal graphs) is not supported."
            )

        self.sn1 = sn1
        self.sn2 = sn2
        #Return all nodes and edges from each snapshot
        self._nodes_1 = {n.oid: n for n in sn1.get_all_nodes()}
        self._nodes_2 = {n.oid: n for n in sn2.get_all_nodes()}

        self._edges_1 = {e.oid: e for e in sn1.get_all_edges()}
        self._edges_2 = {e.oid: e for e in sn2.get_all_edges()}

        # Return OIDs of all edges and nodes from each snapshot
        self._node_oids_1 = {n.oid for n in sn1.get_all_nodes()}
        self._node_oids_2 = {n.oid for n in sn2.get_all_nodes()}

        self._edge_oids_1 = {e.oid for e in sn1.get_all_edges()}
        self._edge_oids_2 = {e.oid for e in sn2.get_all_edges()}

    @staticmethod
    def _entity_values(snapshot: Snapshot, entity: str):
        """
        Extract entity properties as dicts for comparison.

        For hybrid mode: get_all_nodes/edges resolves temporal properties
        and merges them into properties dict.

        Returns dict mapping oid -> {oid, prop1, prop2, ...}
        """
        if entity == "nodes":
            entities = snapshot.get_all_nodes()
        elif entity == "edges":
            entities = snapshot.get_all_edges()
        else:
            raise ValueError(f"Invalid entity type: {entity}. Must be 'nodes' or 'edges'")

        result = {}

        for e in entities:
            # In hybrid mode: properties contains static + resolved temporal
            # In graph mode: properties contains only static (temporal_properties separate)
            values = {"oid": e.oid}
            values.update(e.properties)  # Copy all properties

            result[e.oid] = values

        return result

    # ******************** Structural Diff methods (for both hybrid and graph mode) ************************
    def added_nodes(self) -> List[Any]:
        """Return OIDs of nodes present in snap2 but not in snap1"""
        return list(self._node_oids_2 - self._node_oids_1)

    def removed_nodes(self) -> List[Any]:
        """Return OIDs of nodes present in snap1 but not in snap2"""
        return list(self._node_oids_1 - self._node_oids_2)

    def added_edges(self) -> List[Any]:
        """Return OIDs of edges present in snap2 but not in snap1"""
        return list(self._edge_oids_2 - self._edge_oids_1)

    def removed_edges(self) -> List[Any]:
        """Return OIDs of edges present in snap1 but not in snap2"""
        return list(self._edge_oids_1 - self._edge_oids_2)

    def persistent_nodes(self) -> List[Any]:
        """Return OIDs of nodes present in both snapshots"""
        return [n for n in self._node_oids_2 if n in self._node_oids_1]

    def persistent_edges(self) -> List[Any]:
        """Return OIDs of edges present in both snapshots"""
        return [e for e in self._edge_oids_2 if e in self._edge_oids_1]

    def added_timeseries(self):
        return list(self.sn2.get_all_timeseries() - self.sn1.get_all_timeseries())


    # ***************************** Property Diff method ********************************


    def compute_property_diff(self, props1:Dict[str, Any], props2:Dict[str, Any]):
        """Compute property difference between the twi snapshots"""

        keys1= set(props1.keys())
        keys2= set(props2.keys())
        persistent={}
        added={}
        removed={}
        modified={}
        for key in keys2-keys1:
            added[key] = props2[key]
        for key in keys1-keys2:
            removed[key] = props1[key]
        for key in keys1^keys2:
            modified[key] = props2[key]
        for key in keys1&keys2:
            v_old=props1[key]
            v_new=props2[key]
            if v_old == v_new:
                persistent[key] = v_old
            else:
                delta=None
                if isinstance(v_old, (int,float)) & isinstance(v_new, (int,float)):
                    delta= v_new-v_old
                modified[key] = PropertyChange(v_old,v_new,delta)
        return persistent, added, removed, modified


    def changed_nodes(self,oids: Optional[List[str]] = None, element_type="node"):
        """Return nodes present in both snapshots with their property changes"""
        if self.sn1.mode == "graph" or self.sn2.mode == "graph":
            raise ValueError("Property diff requires hybrid mode. Graph mode has no resolved time series values to compare.")
        common_ids = self._node_oids_1 & self._node_oids_2
        if oids is not None:
            if isinstance(oids, (int,str)):
                oids=str(oids)
            else: oids={str(o) for o in oids}
            common_ids &= oids
        results=[]
        for oid in common_ids:
            element1= self._nodes_1[oid] if element_type == "node" else self._edges_1[oid]
            element2= self._nodes_2[oid] if element_type == "node" else self._edges_2[oid]
            persistent, added, removed, modified = self.compute_property_diff(element1.properties, element2.properties)
            if not added or removed or modified:
                continue
            if element_type == "node":
                diff_element= HyGraphDiffNode(
                    oid=oid,
                    label=element2.label,
                    persistent_properties=persistent,
                    added_properties=added,
                    removed_properties=removed,
                    changed_properties=modified
                )
            else:
                diff_element= HyGraphDiffEdge(
                oid=oid,
                label=element2.label,
                source=element2.source,
                    target=element2.target,
                persistent_properties=persistent,
                added_properties=added,
                removed_properties=removed,
                changed_properties=modified
            )
            results.append(diff_element)
        return results


    #==================================================================================
    # Node churn metrics
    #================================================================================
    def node_churn (self) -> float:
        """Calculate the rate of change of the nodes between snapshots
        churn rate in [0,1]:
        0=no changes
        1=complete turnover (no nodes in common)
        """
        union= self._node_oids_2 | self._node_oids_1
        if len(union)==0:
            return 0
        return len(self._node_oids_2 ^ self._node_oids_1)/len(union)


    def node_addition_rate(self)->float:
        """"Return the rate of addition of nodes between the two snapshots (Jaccard distance)"""
        union = self._node_oids_2 | self._node_oids_1
        if len(union)==0:
            return 0.0
        return  len(self._node_oids_2-self._node_oids_1)/len(union)
    def node_deletion_rate(self)->float:
        """"Return the rate of deletion of nodes between the two snapshots"""
        union = self._node_oids_2 | self._node_oids_1
        if len(union)==0:
            return 0.0
        return len(self._node_oids_1-self._node_oids_2)/len(union)


    #================================================================================
    # Edge churn metrics
    #================================================================================
    def edge_churn(self) -> float:
        """Calculate the rate of change of the edges between snapshots (Jaccard distance)"""
        union = self._edge_oids_2 | self._edge_oids_2
        if len(union) == 0:
            return 0
        return len(self._edge_oids_2 ^ self._edge_oids_2) / len(union)
    def edge_addition_rate(self)->float:
        """"Return the rate of addition of nodes between the two snapshots"""
        union = self._edge_oids_2 | self._edge_oids_1
        if len(union)==0:
            return 0.0
        return  len(self._edge_oids_2-self._edge_oids_1)/len(union)
    def edge_deletion_rate(self)->float:
        """"Return the rate of deletion of nodes between the two snapshots"""
        union = self._edge_oids_2 | self._edge_oids_1
        if len(union)==0:
            return 0.0
        return len(self._edge_oids_1-self._edge_oids_2)/len(union)


    def graph_edit_distance_normalized(self,node_weight: float=1.0, edge_weight:float=1.0) -> float:
        """Approximate the edit distance between the two snapshots (absolut count)"""
        nodes_union=len(self._node_oids_2 | self._node_oids_1)
        edges_union=len(self._edge_oids_2 | self._edge_oids_1)
        if nodes_union==0 and edges_union==0:
            return 0

        return (len (self.added_nodes() + self.removed_nodes())*node_weight + len (
            self.added_edges() + self.removed_edges())*edge_weight)/ (node_weight*nodes_union + edge_weight*edges_union)
        # =========================================================================
        # Similarity Metrics
        # =========================================================================

    def jaccard_similarity_nodes(self) -> float:
        """Calculate Jaccard similarity for nodes. Returns value in [0, 1]:  0 = no overlap 1 = identical node sets"""
        union = self._node_oids_1 | self._node_oids_2
        if len(union) == 0:
            return 1.0  # Both empty = identical
        intersection = self._node_oids_1 & self._node_oids_2
        return len(intersection) / len(union)

    def jaccard_similarity_edges(self) -> float:
        """Calculate Jaccard similarity for nodes. Returns value in [0, 1]:  0 = no overlap  1 = identical node sets"""
        union = self._edge_oids_1 | self._edge_oids_2
        if len(union) == 0:
            return 1.0  # Both empty = identical
        intersection = self._edge_oids_1 & self._edge_oids_2
        return len(intersection) / len(union)

    def stability_score(self, node_weight: float = 0.5, edge_weight: float = 0.5) -> float:
        """Calculate  Stability score in [0, 1]"""
        total_weight = node_weight + edge_weight
        if total_weight == 0:
            return 0.0

        node_sim = self.jaccard_similarity_nodes()
        edge_sim = self.jaccard_similarity_edges()

        return (node_weight * node_sim + edge_weight * edge_sim) / total_weight


    def __repr__(self):
        return (
            f"HygraphDiff:\n"
            f"  Added Nodes: +{len(self.added_nodes())}\n"
            f"  Removed Nodes: +{len(self.removed_nodes())}\n"
            f"  Persistent Nodes: +{len(self.persistent_nodes())}\n"
            f"  Added Edges: +{len(self.added_edges())}\n"
            f"  Removed Edges: +{len(self.removed_edges())}\n"
            f"  Persistent Edges: +{len(self.persistent_edges())}\n"
            f"  Added Time series: +{len(self.added_timeseries())}\n"
            f"  Graph Edit Distance: {self.graph_edit_distance_normalized()}\n"
            f"  Stablity score: {self.stability_score()}\n"
        )
    
    # =========================================================================
    # QUERYABLE DIFF API
    # =========================================================================
    
    def query(self) -> 'DiffQueryBuilder':
        """
        Start a fluent query on the diff results.
        
        Allows filtering and analyzing changed, added, or removed entities
        based on their change metrics.
        
        Returns:
            DiffQueryBuilder for fluent query construction
        
        Example:
            # Find nodes where degree increased significantly
            morning_hubs = diff.query() \\
                .changed_nodes() \\
                .where(lambda n: n.degree_change('in') > 10) \\
                .execute()
            
            # Find added nodes with high capacity
            new_large = diff.query() \\
                .added_nodes() \\
                .where(lambda n: n.get_property('capacity') > 50) \\
                .execute()
            
            # Find nodes where bike availability dropped
            depleted = diff.query() \\
                .changed_nodes() \\
                .where_ts_change('num_bikes', 'mean', '<', -5) \\
                .execute()
        """
        return DiffQueryBuilder(self)


# =============================================================================
# QUERYABLE DIFF NODE
# =============================================================================

@dataclass
class QueryableDiffNode:
    """
    Wrapper for a node in the diff context with change metrics.
    
    Provides access to:
    - snap1_node: Node state in first snapshot (None if added)
    - snap2_node: Node state in second snapshot (None if removed)
    - change_type: 'added', 'removed', or 'changed'
    - Property and degree change calculations
    """
    oid: str
    snap1_node: Optional[PGNode]
    snap2_node: Optional[PGNode]
    change_type: str  # 'added', 'removed', 'changed'
    diff_context: 'HyGraphDiff'
    
    @property
    def label(self) -> str:
        """Get node label."""
        if self.snap2_node:
            return self.snap2_node.label
        return self.snap1_node.label if self.snap1_node else ''
    
    def get_property(self, name: str, default: Any = None) -> Any:
        """
        Get property value from the most recent snapshot (snap2).
        Falls back to snap1 if snap2 doesn't have the node.
        """
        if self.snap2_node:
            return self.snap2_node.get_static_property(name, default)
        elif self.snap1_node:
            return self.snap1_node.get_static_property(name, default)
        return default
    
    def get_property_snap1(self, name: str, default: Any = None) -> Any:
        """Get property value from first snapshot."""
        if self.snap1_node:
            return self.snap1_node.get_static_property(name, default)
        return default
    
    def get_property_snap2(self, name: str, default: Any = None) -> Any:
        """Get property value from second snapshot."""
        if self.snap2_node:
            return self.snap2_node.get_static_property(name, default)
        return default
    
    def property_change(self, name: str) -> Optional[float]:
        """
        Calculate property value change between snapshots.
        
        Returns:
            Numeric change (snap2 - snap1), or None if not comparable
        """
        val1 = self.get_property_snap1(name)
        val2 = self.get_property_snap2(name)
        
        if val1 is None or val2 is None:
            return None
        
        try:
            return float(val2) - float(val1)
        except (TypeError, ValueError):
            return None
    
    def property_change_percent(self, name: str) -> Optional[float]:
        """
        Calculate percentage change in property value.
        
        Returns:
            Percentage change ((snap2 - snap1) / snap1 * 100), or None
        """
        val1 = self.get_property_snap1(name)
        val2 = self.get_property_snap2(name)
        
        if val1 is None or val2 is None:
            return None
        
        try:
            v1, v2 = float(val1), float(val2)
            if abs(v1) < 1e-9:
                return None
            return ((v2 - v1) / abs(v1)) * 100
        except (TypeError, ValueError):
            return None
    
    def degree_change(self, direction: str = 'both') -> int:
        """
        Calculate degree change between snapshots.
        
        Args:
            direction: 'in', 'out', or 'both'
        
        Returns:
            Degree change (snap2_degree - snap1_degree)
        """
        degree1 = self._calculate_degree(self.snap1_node, direction) if self.snap1_node else 0
        degree2 = self._calculate_degree(self.snap2_node, direction) if self.snap2_node else 0
        return degree2 - degree1
    
    def degree_snap1(self, direction: str = 'both') -> int:
        """Get degree in first snapshot."""
        if not self.snap1_node:
            return 0
        return self._calculate_degree(self.snap1_node, direction)
    
    def degree_snap2(self, direction: str = 'both') -> int:
        """Get degree in second snapshot."""
        if not self.snap2_node:
            return 0
        return self._calculate_degree(self.snap2_node, direction)
    
    def _calculate_degree(self, node: PGNode, direction: str) -> int:
        """Calculate node degree from diff context."""
        if node is None:
            return 0
        
        # Determine which snapshot's edges to use
        if node == self.snap1_node:
            edges = self.diff_context._edges_1
        else:
            edges = self.diff_context._edges_2
        
        count = 0
        for edge in edges.values():
            if direction in ('out', 'both') and edge.source == self.oid:
                count += 1
            if direction in ('in', 'both') and edge.target == self.oid:
                count += 1
        
        return count
    
    def ts_change(self, ts_name: str, aggregation: str = 'mean') -> Optional[float]:
        """
        Calculate time series aggregation change between snapshots.
        
        Args:
            ts_name: Temporal property name
            aggregation: 'mean', 'min', 'max', 'std', 'sum', 'median'
        
        Returns:
            Change in aggregated value (snap2_agg - snap1_agg)
        """
        agg1 = self._get_ts_aggregation(self.snap1_node, ts_name, aggregation)
        agg2 = self._get_ts_aggregation(self.snap2_node, ts_name, aggregation)
        
        if agg1 is None and agg2 is None:
            return None
        
        agg1 = agg1 or 0
        agg2 = agg2 or 0
        
        return agg2 - agg1
    
    def ts_snap1(self, ts_name: str) -> Optional['TimeSeries']:
        """Get time series from first snapshot."""
        if self.snap1_node:
            return self.snap1_node.get_temporal_property(ts_name)
        return None
    
    def ts_snap2(self, ts_name: str) -> Optional['TimeSeries']:
        """Get time series from second snapshot."""
        if self.snap2_node:
            return self.snap2_node.get_temporal_property(ts_name)
        return None
    
    def _get_ts_aggregation(self, node: Optional[PGNode], ts_name: str, agg: str) -> Optional[float]:
        """Get time series aggregation for a node."""
        if node is None:
            return None
        
        ts = node.get_temporal_property(ts_name)
        if ts is None:
            return None
        
        try:
            if agg == 'mean':
                return ts.mean()
            elif agg == 'min':
                return ts.min()
            elif agg == 'max':
                return ts.max()
            elif agg == 'std':
                return ts.std()
            elif agg == 'sum':
                return ts.sum()
            elif agg == 'median':
                return ts.median()
            elif agg == 'count':
                return float(ts.length)
        except Exception:
            pass
        
        return None
    
    def has_property_change(self, name: str) -> bool:
        """Check if property value changed between snapshots."""
        val1 = self.get_property_snap1(name)
        val2 = self.get_property_snap2(name)
        return val1 != val2
    
    def __repr__(self):
        return f"QueryableDiffNode({self.oid}, type={self.change_type}, label={self.label})"


@dataclass
class QueryableDiffEdge:
    """
    Wrapper for an edge in the diff context with change metrics.
    """
    oid: str
    snap1_edge: Optional[PGEdge]
    snap2_edge: Optional[PGEdge]
    change_type: str  # 'added', 'removed', 'changed'
    diff_context: 'HyGraphDiff'
    
    @property
    def label(self) -> str:
        """Get edge label."""
        if self.snap2_edge:
            return self.snap2_edge.label
        return self.snap1_edge.label if self.snap1_edge else ''
    
    @property
    def source(self) -> str:
        """Get source node ID."""
        if self.snap2_edge:
            return self.snap2_edge.source
        return self.snap1_edge.source if self.snap1_edge else ''
    
    @property
    def target(self) -> str:
        """Get target node ID."""
        if self.snap2_edge:
            return self.snap2_edge.target
        return self.snap1_edge.target if self.snap1_edge else ''
    
    def get_property(self, name: str, default: Any = None) -> Any:
        """Get property value from the most recent snapshot."""
        if self.snap2_edge:
            return self.snap2_edge.get_static_property(name, default)
        elif self.snap1_edge:
            return self.snap1_edge.get_static_property(name, default)
        return default
    
    def get_property_snap1(self, name: str, default: Any = None) -> Any:
        """Get property value from first snapshot."""
        if self.snap1_edge:
            return self.snap1_edge.get_static_property(name, default)
        return default
    
    def get_property_snap2(self, name: str, default: Any = None) -> Any:
        """Get property value from second snapshot."""
        if self.snap2_edge:
            return self.snap2_edge.get_static_property(name, default)
        return default
    
    def property_change(self, name: str) -> Optional[float]:
        """Calculate property value change between snapshots."""
        val1 = self.get_property_snap1(name)
        val2 = self.get_property_snap2(name)
        
        if val1 is None or val2 is None:
            return None
        
        try:
            return float(val2) - float(val1)
        except (TypeError, ValueError):
            return None
    
    def ts_change(self, ts_name: str, aggregation: str = 'mean') -> Optional[float]:
        """Calculate time series aggregation change between snapshots."""
        agg1 = self._get_ts_aggregation(self.snap1_edge, ts_name, aggregation)
        agg2 = self._get_ts_aggregation(self.snap2_edge, ts_name, aggregation)
        
        if agg1 is None and agg2 is None:
            return None
        
        agg1 = agg1 or 0
        agg2 = agg2 or 0
        
        return agg2 - agg1
    
    def _get_ts_aggregation(self, edge: Optional[PGEdge], ts_name: str, agg: str) -> Optional[float]:
        """Get time series aggregation for an edge."""
        if edge is None:
            return None
        
        ts = edge.get_temporal_property(ts_name)
        if ts is None:
            return None
        
        try:
            if agg == 'mean':
                return ts.mean()
            elif agg == 'min':
                return ts.min()
            elif agg == 'max':
                return ts.max()
            elif agg == 'std':
                return ts.std()
            elif agg == 'sum':
                return ts.sum()
            elif agg == 'median':
                return ts.median()
        except Exception:
            pass
        
        return None
    
    def __repr__(self):
        return f"QueryableDiffEdge({self.oid}, {self.source}->{self.target}, type={self.change_type})"


# =============================================================================
# DIFF QUERY BUILDER
# =============================================================================

class DiffQueryBuilder:
    """
    Fluent query builder for HyGraphDiff results.
    
    Allows filtering changed, added, or removed entities based on:
    - Lambda predicates with change metrics
    - Property change conditions
    - Time series change conditions
    - Degree change conditions
    
    Example:
        # Find nodes where in-degree increased by more than 10
        morning_hubs = diff.query() \\
            .changed_nodes() \\
            .where(lambda n: n.degree_change('in') > 10) \\
            .execute()
        
        # Find nodes where average bikes dropped by more than 5
        depleted = diff.query() \\
            .changed_nodes() \\
            .where_ts_change('num_bikes', 'mean', '<', -5) \\
            .execute()
        
        # Find added high-capacity nodes
        new_large = diff.query() \\
            .added_nodes() \\
            .where(lambda n: n.get_property('capacity', 0) > 50) \\
            .execute()
    """
    
    def __init__(self, diff: 'HyGraphDiff'):
        self.diff = diff
        self._query_type: Optional[str] = None  # 'changed_nodes', 'added_nodes', etc.
        self._filters: List[Callable] = []
        self._ts_change_filters: List[tuple] = []  # [(ts_name, agg, op, value)]
        self._property_change_filters: List[tuple] = []  # [(prop_name, op, value)]
        self._degree_change_filters: List[tuple] = []  # [(direction, op, value)]
        self._label_filter: Optional[str] = None
        self._limit: Optional[int] = None
        self._order_by: Optional[tuple] = None
    
    # -------------------------------------------------------------------------
    # Query Type Selection
    # -------------------------------------------------------------------------
    
    def changed_nodes(self, label: Optional[str] = None) -> 'DiffQueryBuilder':
        """
        Query nodes that exist in both snapshots but have property changes.
        
        Args:
            label: Optional label filter
        
        Returns:
            Self for chaining
        """
        self._query_type = 'changed_nodes'
        self._label_filter = label
        return self
    
    def added_nodes(self, label: Optional[str] = None) -> 'DiffQueryBuilder':
        """
        Query nodes that were added (exist in snap2 but not snap1).
        
        Args:
            label: Optional label filter
        """
        self._query_type = 'added_nodes'
        self._label_filter = label
        return self
    
    def removed_nodes(self, label: Optional[str] = None) -> 'DiffQueryBuilder':
        """
        Query nodes that were removed (exist in snap1 but not snap2).
        
        Args:
            label: Optional label filter
        """
        self._query_type = 'removed_nodes'
        self._label_filter = label
        return self
    
    def persistent_nodes(self, label: Optional[str] = None) -> 'DiffQueryBuilder':
        """
        Query nodes that exist in both snapshots (changed or unchanged).
        
        Args:
            label: Optional label filter
        """
        self._query_type = 'persistent_nodes'
        self._label_filter = label
        return self
    
    def changed_edges(self, label: Optional[str] = None) -> 'DiffQueryBuilder':
        """
        Query edges that exist in both snapshots but have property changes.
        """
        self._query_type = 'changed_edges'
        self._label_filter = label
        return self
    
    def added_edges(self, label: Optional[str] = None) -> 'DiffQueryBuilder':
        """
        Query edges that were added.
        """
        self._query_type = 'added_edges'
        self._label_filter = label
        return self
    
    def removed_edges(self, label: Optional[str] = None) -> 'DiffQueryBuilder':
        """
        Query edges that were removed.
        """
        self._query_type = 'removed_edges'
        self._label_filter = label
        return self
    
    # -------------------------------------------------------------------------
    # Filters
    # -------------------------------------------------------------------------
    
    def where(self, predicate: Callable[[Union[QueryableDiffNode, QueryableDiffEdge]], bool]) -> 'DiffQueryBuilder':
        """
        Filter with a lambda predicate.
        
        The predicate receives a QueryableDiffNode or QueryableDiffEdge
        with access to change metrics.
        
        Args:
            predicate: Function that takes diff entity and returns bool
        
        Example:
            # Filter by degree change
            .where(lambda n: n.degree_change('in') > 10)
            
            # Filter by property change
            .where(lambda n: n.property_change('capacity') is not None and n.property_change('capacity') > 5)
            
            # Filter by time series change
            .where(lambda n: n.ts_change('num_bikes', 'mean') < -5)
            
            # Complex conditions
            .where(lambda n: n.degree_change('both') > 5 and n.get_property('capacity', 0) > 30)
        """
        self._filters.append(predicate)
        return self
    
    def where_ts_change(
        self, 
        ts_name: str, 
        aggregation: str, 
        operator: str, 
        value: float
    ) -> 'DiffQueryBuilder':
        """
        Filter by time series aggregation change.
        
        Shorthand for comparing ts_change() values without lambda.
        
        Args:
            ts_name: Temporal property name
            aggregation: 'mean', 'min', 'max', 'std', 'sum', 'median'
            operator: '<', '>', '<=', '>=', '=', '!='
            value: Value to compare change against
        
        Example:
            # Find nodes where average bikes dropped by more than 5
            .where_ts_change('num_bikes', 'mean', '<', -5)
            
            # Find nodes where max bikes increased
            .where_ts_change('num_bikes', 'max', '>', 0)
        """
        self._ts_change_filters.append((ts_name, aggregation, operator, value))
        return self
    
    def where_property_change(
        self,
        prop_name: str,
        operator: str,
        value: float
    ) -> 'DiffQueryBuilder':
        """
        Filter by static property change.
        
        Args:
            prop_name: Property name
            operator: '<', '>', '<=', '>=', '=', '!='
            value: Value to compare change against
        
        Example:
            # Find nodes where capacity increased by at least 10
            .where_property_change('capacity', '>=', 10)
        """
        self._property_change_filters.append((prop_name, operator, value))
        return self
    
    def where_degree_change(
        self,
        direction: str,
        operator: str,
        value: int
    ) -> 'DiffQueryBuilder':
        """
        Filter by degree change.
        
        Args:
            direction: 'in', 'out', or 'both'
            operator: '<', '>', '<=', '>=', '=', '!='
            value: Value to compare degree change against
        
        Example:
            # Find nodes where in-degree increased by more than 10
            .where_degree_change('in', '>', 10)
            
            # Find nodes that lost connections
            .where_degree_change('both', '<', 0)
        """
        self._degree_change_filters.append((direction, operator, value))
        return self
    
    def limit(self, n: int) -> 'DiffQueryBuilder':
        """Limit number of results."""
        self._limit = n
        return self
    
    def order_by(
        self,
        key: Union[str, Callable],
        desc: bool = False
    ) -> 'DiffQueryBuilder':
        """
        Order results.
        
        Args:
            key: Property name or callable that extracts sort key
            desc: Sort descending if True
        
        Example:
            # Order by degree change
            .order_by(lambda n: n.degree_change('in'), desc=True)
            
            # Order by property
            .order_by('capacity', desc=True)
        """
        self._order_by = (key, desc)
        return self
    
    # -------------------------------------------------------------------------
    # Execution
    # -------------------------------------------------------------------------
    
    def execute(self) -> List[Union[QueryableDiffNode, QueryableDiffEdge]]:
        """
        Execute the query and return matching entities.
        
        Returns:
            List of QueryableDiffNode or QueryableDiffEdge objects
        """
        if self._query_type is None:
            raise ValueError("Must specify query type (e.g., .changed_nodes(), .added_nodes())")
        
        # Get candidates based on query type
        candidates = self._get_candidates()
        
        # Apply filters
        results = []
        for candidate in candidates:
            if self._matches_filters(candidate):
                results.append(candidate)
                
                if self._limit and len(results) >= self._limit:
                    break
        
        # Apply ordering
        if self._order_by:
            key_fn, desc = self._order_by
            if isinstance(key_fn, str):
                # Property name
                results.sort(key=lambda x: x.get_property(key_fn, 0), reverse=desc)
            else:
                # Callable
                results.sort(key=key_fn, reverse=desc)
        
        return results
    
    def _get_candidates(self) -> List[Union[QueryableDiffNode, QueryableDiffEdge]]:
        """Get candidate entities based on query type."""
        candidates = []
        
        if self._query_type == 'changed_nodes':
            # Nodes in both snapshots
            common_ids = self.diff._node_oids_1 & self.diff._node_oids_2
            for oid in common_ids:
                node = QueryableDiffNode(
                    oid=oid,
                    snap1_node=self.diff._nodes_1.get(oid),
                    snap2_node=self.diff._nodes_2.get(oid),
                    change_type='changed',
                    diff_context=self.diff
                )
                if self._label_filter is None or node.label == self._label_filter:
                    candidates.append(node)
        
        elif self._query_type == 'added_nodes':
            # Nodes in snap2 but not snap1
            added_ids = self.diff._node_oids_2 - self.diff._node_oids_1
            for oid in added_ids:
                node = QueryableDiffNode(
                    oid=oid,
                    snap1_node=None,
                    snap2_node=self.diff._nodes_2.get(oid),
                    change_type='added',
                    diff_context=self.diff
                )
                if self._label_filter is None or node.label == self._label_filter:
                    candidates.append(node)
        
        elif self._query_type == 'removed_nodes':
            # Nodes in snap1 but not snap2
            removed_ids = self.diff._node_oids_1 - self.diff._node_oids_2
            for oid in removed_ids:
                node = QueryableDiffNode(
                    oid=oid,
                    snap1_node=self.diff._nodes_1.get(oid),
                    snap2_node=None,
                    change_type='removed',
                    diff_context=self.diff
                )
                if self._label_filter is None or node.label == self._label_filter:
                    candidates.append(node)
        
        elif self._query_type == 'persistent_nodes':
            # Nodes in both snapshots (all persistent, changed or not)
            common_ids = self.diff._node_oids_1 & self.diff._node_oids_2
            for oid in common_ids:
                node = QueryableDiffNode(
                    oid=oid,
                    snap1_node=self.diff._nodes_1.get(oid),
                    snap2_node=self.diff._nodes_2.get(oid),
                    change_type='persistent',
                    diff_context=self.diff
                )
                if self._label_filter is None or node.label == self._label_filter:
                    candidates.append(node)
        
        elif self._query_type == 'changed_edges':
            common_ids = self.diff._edge_oids_1 & self.diff._edge_oids_2
            for oid in common_ids:
                edge = QueryableDiffEdge(
                    oid=oid,
                    snap1_edge=self.diff._edges_1.get(oid),
                    snap2_edge=self.diff._edges_2.get(oid),
                    change_type='changed',
                    diff_context=self.diff
                )
                if self._label_filter is None or edge.label == self._label_filter:
                    candidates.append(edge)
        
        elif self._query_type == 'added_edges':
            added_ids = self.diff._edge_oids_2 - self.diff._edge_oids_1
            for oid in added_ids:
                edge = QueryableDiffEdge(
                    oid=oid,
                    snap1_edge=None,
                    snap2_edge=self.diff._edges_2.get(oid),
                    change_type='added',
                    diff_context=self.diff
                )
                if self._label_filter is None or edge.label == self._label_filter:
                    candidates.append(edge)
        
        elif self._query_type == 'removed_edges':
            removed_ids = self.diff._edge_oids_1 - self.diff._edge_oids_2
            for oid in removed_ids:
                edge = QueryableDiffEdge(
                    oid=oid,
                    snap1_edge=self.diff._edges_1.get(oid),
                    snap2_edge=None,
                    change_type='removed',
                    diff_context=self.diff
                )
                if self._label_filter is None or edge.label == self._label_filter:
                    candidates.append(edge)
        
        return candidates
    
    def _matches_filters(self, entity: Union[QueryableDiffNode, QueryableDiffEdge]) -> bool:
        """Check if entity matches all filters."""
        # Check lambda predicates
        for predicate in self._filters:
            try:
                if not predicate(entity):
                    return False
            except Exception:
                return False
        
        # Check ts change filters
        for ts_name, agg, op, val in self._ts_change_filters:
            change = entity.ts_change(ts_name, agg)
            if change is None:
                return False
            if not self._compare(change, op, val):
                return False
        
        # Check property change filters
        for prop_name, op, val in self._property_change_filters:
            change = entity.property_change(prop_name)
            if change is None:
                return False
            if not self._compare(change, op, val):
                return False
        
        # Check degree change filters (only for nodes)
        if isinstance(entity, QueryableDiffNode):
            for direction, op, val in self._degree_change_filters:
                change = entity.degree_change(direction)
                if not self._compare(change, op, val):
                    return False
        
        return True
    
    @staticmethod
    def _compare(left: Any, op: str, right: Any) -> bool:
        """Compare two values with operator."""
        try:
            if op in ('=', '=='):
                return abs(float(left) - float(right)) < 1e-9
            elif op in ('!=', '<>'):
                return abs(float(left) - float(right)) >= 1e-9
            elif op == '<':
                return float(left) < float(right)
            elif op == '<=':
                return float(left) <= float(right)
            elif op == '>':
                return float(left) > float(right)
            elif op == '>=':
                return float(left) >= float(right)
        except (TypeError, ValueError):
            return False
        return False
    
    # -------------------------------------------------------------------------
    # Convenience Methods
    # -------------------------------------------------------------------------
    
    def count(self) -> int:
        """Return count of matching entities."""
        return len(self.execute())
    
    def first(self) -> Optional[Union[QueryableDiffNode, QueryableDiffEdge]]:
        """Get first matching entity."""
        results = self.limit(1).execute()
        return results[0] if results else None
    
    def exists(self) -> bool:
        """Check if any matching entities exist."""
        return self.count() > 0
    
    def as_node_ids(self) -> List[str]:
        """Execute and return just the node IDs."""
        return [e.oid for e in self.execute()]
    
    def as_subhygraph(self, hygraph, name: str):
        """
        Execute and create a SubHyGraph from matching nodes.
        
        Args:
            hygraph: HyGraph instance to create subgraph in
            name: Name for the subgraph
        
        Returns:
            SubHyGraph containing matching nodes
        """
        node_ids = self.as_node_ids()
        return hygraph.subHygraph(
            node_ids=node_ids,
            name=name,
            filter_query=f"DiffQuery: {self._query_type}"
        )


# =============================================================================
# INTERVAL-BASED HYGRAPH DIFF - With Time Series Comparison
# =============================================================================

class HyGraphIntervalDiff:

    
    def __init__(
        self, 
        sn1: Snapshot, 
        sn2: Snapshot,
        significance_threshold: float = 0.1
    ):
        """
        Initialize interval diff.
        
        Args:
            sn1: First interval snapshot
            sn2: Second interval snapshot
            significance_threshold: Relative change threshold for "significant" changes
        """
        # Validate both snapshots have same mode
        if sn1.mode != sn2.mode:
            raise ValueError(
                f"Cannot compare snapshots with different modes: {sn1.mode} vs {sn2.mode}"
            )
        
        # Validate both are interval snapshots
        if not sn1.is_interval() or not sn2.is_interval():
            raise ValueError(
                "HyGraphIntervalDiff requires interval snapshots. "
                "Use HyGraphDiff for point snapshots."
            )
        
        self.sn1 = sn1
        self.sn2 = sn2
        self.significance_threshold = significance_threshold
        
        # Store interval bounds
        self.interval1 = (sn1.start_time, sn1.end_time)
        self.interval2 = (sn2.start_time, sn2.end_time)
        
        # Build entity dictionaries
        self._nodes_1 = {n.oid: n for n in sn1.get_all_nodes()}
        self._nodes_2 = {n.oid: n for n in sn2.get_all_nodes()}
        
        self._edges_1 = {e.oid: e for e in sn1.get_all_edges()}
        self._edges_2 = {e.oid: e for e in sn2.get_all_edges()}
        
        # Build OID sets
        self._node_oids_1 = set(self._nodes_1.keys())
        self._node_oids_2 = set(self._nodes_2.keys())
        
        self._edge_oids_1 = set(self._edges_1.keys())
        self._edge_oids_2 = set(self._edges_2.keys())
        
        # Cache for TS comparisons (computed lazily)
        self._node_ts_comparisons: Dict[str, Dict[str, TSComparison]] = {}
        self._edge_ts_comparisons: Dict[str, Dict[str, TSComparison]] = {}
    
    # =========================================================================
    # Structural Diff (same as HyGraphDiff)
    # =========================================================================
    
    def added_nodes(self) -> List[Any]:
        """Return OIDs of nodes present in snap2 but not in snap1."""
        return list(self._node_oids_2 - self._node_oids_1)
    
    def removed_nodes(self) -> List[Any]:
        """Return OIDs of nodes present in snap1 but not in snap2."""
        return list(self._node_oids_1 - self._node_oids_2)
    
    def persistent_nodes(self) -> List[Any]:
        """Return OIDs of nodes present in both snapshots."""
        return list(self._node_oids_1 & self._node_oids_2)
    
    def added_edges(self) -> List[Any]:
        """Return OIDs of edges present in snap2 but not in snap1."""
        return list(self._edge_oids_2 - self._edge_oids_1)
    
    def removed_edges(self) -> List[Any]:
        """Return OIDs of edges present in snap1 but not in snap2."""
        return list(self._edge_oids_1 - self._edge_oids_2)
    
    def persistent_edges(self) -> List[Any]:
        """Return OIDs of edges present in both snapshots."""
        return list(self._edge_oids_1 & self._edge_oids_2)
    
    # =========================================================================
    # Time Series Comparison
    # =========================================================================
    
    def get_node_ts_comparison(
        self, 
        node_oid: str, 
        ts_name: str
    ) -> Optional[TSComparison]:
        """
        Get time series comparison for a node's temporal property.
        
        Args:
            node_oid: Node OID
            ts_name: Temporal property name
        
        Returns:
            TSComparison object or None if not available
        """
        # Check cache
        if node_oid in self._node_ts_comparisons:
            if ts_name in self._node_ts_comparisons[node_oid]:
                return self._node_ts_comparisons[node_oid][ts_name]
        
        # Compute comparison
        node1 = self._nodes_1.get(node_oid)
        node2 = self._nodes_2.get(node_oid)
        
        ts1 = node1.get_temporal_property(ts_name) if node1 else None
        ts2 = node2.get_temporal_property(ts_name) if node2 else None
        
        if ts1 is None and ts2 is None:
            return None
        
        comp = compute_ts_comparison(
            ts_name, ts1, ts2, 
            self.significance_threshold
        )
        
        # Cache result
        if node_oid not in self._node_ts_comparisons:
            self._node_ts_comparisons[node_oid] = {}
        self._node_ts_comparisons[node_oid][ts_name] = comp
        
        return comp
    
    def get_edge_ts_comparison(
        self, 
        edge_oid: str, 
        ts_name: str
    ) -> Optional[TSComparison]:
        """
        Get time series comparison for an edge's temporal property.
        """
        # Check cache
        if edge_oid in self._edge_ts_comparisons:
            if ts_name in self._edge_ts_comparisons[edge_oid]:
                return self._edge_ts_comparisons[edge_oid][ts_name]
        
        # Compute comparison
        edge1 = self._edges_1.get(edge_oid)
        edge2 = self._edges_2.get(edge_oid)
        
        ts1 = edge1.get_temporal_property(ts_name) if edge1 else None
        ts2 = edge2.get_temporal_property(ts_name) if edge2 else None
        
        if ts1 is None and ts2 is None:
            return None
        
        comp = compute_ts_comparison(
            ts_name, ts1, ts2,
            self.significance_threshold
        )
        
        # Cache result
        if edge_oid not in self._edge_ts_comparisons:
            self._edge_ts_comparisons[edge_oid] = {}
        self._edge_ts_comparisons[edge_oid][ts_name] = comp
        
        return comp
    
    def get_all_node_ts_comparisons(
        self, 
        ts_name: str,
        only_persistent: bool = True
    ) -> Dict[str, TSComparison]:
        """
        Get TS comparisons for all nodes for a given temporal property.
        
        Args:
            ts_name: Temporal property name
            only_persistent: If True, only include nodes in both snapshots
        
        Returns:
            Dict mapping node_oid -> TSComparison
        """
        if only_persistent:
            node_oids = self.persistent_nodes()
        else:
            node_oids = list(self._node_oids_1 | self._node_oids_2)
        
        results = {}
        for oid in node_oids:
            comp = self.get_node_ts_comparison(oid, ts_name)
            if comp is not None:
                results[oid] = comp
        
        return results
    
    # =========================================================================
    # Aggregated TS Diff Metrics
    # =========================================================================
    
    def ts_mean_changes(
        self, 
        ts_name: str,
        only_persistent: bool = True
    ) -> Dict[str, float]:
        """
        Get mean changes for all nodes/edges for a temporal property.
        
        Returns:
            Dict mapping entity_oid -> mean_diff
        """
        comparisons = self.get_all_node_ts_comparisons(ts_name, only_persistent)
        return {
            oid: comp.mean_diff 
            for oid, comp in comparisons.items() 
            if comp.mean_diff is not None
        }
    
    def nodes_with_significant_ts_change(
        self,
        ts_name: str
    ) -> List[str]:
        """
        Get nodes where the time series shows significant change.
        
        Significance is determined by the threshold set during init.
        """
        comparisons = self.get_all_node_ts_comparisons(ts_name, only_persistent=True)
        return [
            oid for oid, comp in comparisons.items()
            if comp.has_significant_change
        ]
    
    def nodes_with_trend_change(
        self,
        ts_name: str
    ) -> List[str]:
        """
        Get nodes where the time series trend changed direction.
        """
        comparisons = self.get_all_node_ts_comparisons(ts_name, only_persistent=True)
        return [
            oid for oid, comp in comparisons.items()
            if comp.trend_changed
        ]
    
    def nodes_with_anti_correlation(
        self,
        ts_name: str,
        threshold: float = -0.5
    ) -> List[str]:
        """
        Get nodes where interval1 and interval2 time series are anti-correlated.
        
        This indicates opposite behavior (e.g., increasing in morning, decreasing in evening).
        """
        comparisons = self.get_all_node_ts_comparisons(ts_name, only_persistent=True)
        return [
            oid for oid, comp in comparisons.items()
            if comp.correlation is not None and comp.correlation < threshold
        ]
    
    def ts_diff_summary(
        self,
        ts_name: str
    ) -> Dict[str, Any]:
        """
        Get summary statistics for time series changes across all nodes.
        
        Returns:
            Dict with aggregated statistics
        """
        comparisons = self.get_all_node_ts_comparisons(ts_name, only_persistent=True)
        
        mean_diffs = [c.mean_diff for c in comparisons.values() if c.mean_diff is not None]
        std_diffs = [c.std_diff for c in comparisons.values() if c.std_diff is not None]
        correlations = [c.correlation for c in comparisons.values() if c.correlation is not None]
        
        return {
            'ts_name': ts_name,
            'node_count': len(comparisons),
            'significant_changes': len(self.nodes_with_significant_ts_change(ts_name)),
            'trend_changes': len(self.nodes_with_trend_change(ts_name)),
            'anti_correlated': len(self.nodes_with_anti_correlation(ts_name)),
            'mean_diff': {
                'avg': np.mean(mean_diffs) if mean_diffs else None,
                'std': np.std(mean_diffs) if mean_diffs else None,
                'min': np.min(mean_diffs) if mean_diffs else None,
                'max': np.max(mean_diffs) if mean_diffs else None,
            },
            'std_diff': {
                'avg': np.mean(std_diffs) if std_diffs else None,
                'min': np.min(std_diffs) if std_diffs else None,
                'max': np.max(std_diffs) if std_diffs else None,
            },
            'correlation': {
                'avg': np.mean(correlations) if correlations else None,
                'min': np.min(correlations) if correlations else None,
                'max': np.max(correlations) if correlations else None,
            }
        }
    
    # =========================================================================
    # Churn Metrics (same as HyGraphDiff)
    # =========================================================================
    
    def node_churn(self) -> float:
        """Calculate node churn rate."""
        union = self._node_oids_1 | self._node_oids_2
        if len(union) == 0:
            return 0
        return len(self._node_oids_1 ^ self._node_oids_2) / len(union)
    
    def edge_churn(self) -> float:
        """Calculate edge churn rate."""
        union = self._edge_oids_1 | self._edge_oids_2
        if len(union) == 0:
            return 0
        return len(self._edge_oids_1 ^ self._edge_oids_2) / len(union)
    
    def jaccard_similarity_nodes(self) -> float:
        """Calculate Jaccard similarity for nodes."""
        union = self._node_oids_1 | self._node_oids_2
        if len(union) == 0:
            return 1.0
        intersection = self._node_oids_1 & self._node_oids_2
        return len(intersection) / len(union)
    
    def jaccard_similarity_edges(self) -> float:
        """Calculate Jaccard similarity for edges."""
        union = self._edge_oids_1 | self._edge_oids_2
        if len(union) == 0:
            return 1.0
        intersection = self._edge_oids_1 & self._edge_oids_2
        return len(intersection) / len(union)
    
    def stability_score(self, node_weight: float = 0.5, edge_weight: float = 0.5) -> float:
        """Calculate stability score."""
        total_weight = node_weight + edge_weight
        if total_weight == 0:
            return 0.0
        
        node_sim = self.jaccard_similarity_nodes()
        edge_sim = self.jaccard_similarity_edges()
        
        return (node_weight * node_sim + edge_weight * edge_sim) / total_weight
    
    # =========================================================================
    # Queryable API
    # =========================================================================
    
    def query(self) -> 'IntervalDiffQueryBuilder':
        """
        Start a fluent query on the interval diff results.
        
        This extends the basic DiffQueryBuilder with time series comparison filters.
        
        Example:
            # Find nodes where mean bikes dropped by more than 5
            depleted = diff.query() \\
                .changed_nodes() \\
                .where_ts_mean_diff('num_bikes', '<', -5) \\
                .execute()
            
            # Find nodes where trend reversed
            reversed = diff.query() \\
                .changed_nodes() \\
                .where_ts_trend_changed('num_bikes') \\
                .execute()
            
            # Find nodes with anti-correlated morning/evening patterns
            anti = diff.query() \\
                .changed_nodes() \\
                .where_ts_correlation('num_bikes', '<', -0.5) \\
                .execute()
        """
        return IntervalDiffQueryBuilder(self)
    
    def __repr__(self):
        return (
            f"HyGraphIntervalDiff:\n"
            f"  Interval 1: {self.interval1[0]} to {self.interval1[1]}\n"
            f"  Interval 2: {self.interval2[0]} to {self.interval2[1]}\n"
            f"  Added Nodes: +{len(self.added_nodes())}\n"
            f"  Removed Nodes: -{len(self.removed_nodes())}\n"
            f"  Persistent Nodes: {len(self.persistent_nodes())}\n"
            f"  Added Edges: +{len(self.added_edges())}\n"
            f"  Removed Edges: -{len(self.removed_edges())}\n"
            f"  Persistent Edges: {len(self.persistent_edges())}\n"
            f"  Node Churn: {self.node_churn():.2%}\n"
            f"  Stability Score: {self.stability_score():.2%}\n"
        )


# =============================================================================
# QUERYABLE INTERVAL DIFF NODE
# =============================================================================

@dataclass
class QueryableIntervalDiffNode:
    """
    Wrapper for a node in interval diff context with TS comparison access.
    
    Extends QueryableDiffNode with:
    - get_ts_comparison(ts_name) -> TSComparison
    - Direct access to statistical, trend, and shape differences
    """
    oid: str
    snap1_node: Optional[PGNode]
    snap2_node: Optional[PGNode]
    change_type: str
    diff_context: 'HyGraphIntervalDiff'
    
    @property
    def label(self) -> str:
        if self.snap2_node:
            return self.snap2_node.label
        return self.snap1_node.label if self.snap1_node else ''
    
    def get_property(self, name: str, default: Any = None) -> Any:
        if self.snap2_node:
            return self.snap2_node.get_static_property(name, default)
        elif self.snap1_node:
            return self.snap1_node.get_static_property(name, default)
        return default
    
    # -------------------------------------------------------------------------
    # Time Series Comparison Access
    # -------------------------------------------------------------------------
    
    def get_ts_comparison(self, ts_name: str) -> Optional[TSComparison]:
        """
        Get full time series comparison for a temporal property.
        
        Returns:
            TSComparison with all computed metrics
        """
        return self.diff_context.get_node_ts_comparison(self.oid, ts_name)
    
    def ts_mean_diff(self, ts_name: str) -> Optional[float]:
        """
        Get mean difference for a temporal property.
        
        Returns:
            ts2_mean - ts1_mean
        """
        comp = self.get_ts_comparison(ts_name)
        return comp.mean_diff if comp else None
    
    def ts_mean_diff_percent(self, ts_name: str) -> Optional[float]:
        """
        Get percentage mean change.
        """
        comp = self.get_ts_comparison(ts_name)
        return comp.mean_diff_percent if comp else None
    
    def ts_std_diff(self, ts_name: str) -> Optional[float]:
        """
        Get standard deviation difference (volatility change).
        
        Positive = became more volatile in interval2
        Negative = became more stable in interval2
        """
        comp = self.get_ts_comparison(ts_name)
        return comp.std_diff if comp else None
    
    def ts_trend_changed(self, ts_name: str) -> bool:
        """
        Check if trend direction changed between intervals.
        """
        comp = self.get_ts_comparison(ts_name)
        return comp.trend_changed if comp else False
    
    def ts_trend_snap1(self, ts_name: str) -> Optional[str]:
        """
        Get trend direction in interval1.
        """
        comp = self.get_ts_comparison(ts_name)
        return comp.ts1_trend if comp else None
    
    def ts_trend_snap2(self, ts_name: str) -> Optional[str]:
        """
        Get trend direction in interval2.
        """
        comp = self.get_ts_comparison(ts_name)
        return comp.ts2_trend if comp else None
    
    def ts_correlation(self, ts_name: str) -> Optional[float]:
        """
        Get correlation between interval1 and interval2 time series.
        
        Returns:
            Pearson correlation (-1 to 1)
            - Close to 1: Similar patterns
            - Close to 0: Unrelated patterns  
            - Close to -1: Opposite patterns (anti-correlated)
        """
        comp = self.get_ts_comparison(ts_name)
        return comp.correlation if comp else None
    
    def ts_shape_similarity(self, ts_name: str) -> Optional[float]:
        """
        Get shape similarity score (0 to 1).
        """
        comp = self.get_ts_comparison(ts_name)
        return comp.shape_similarity if comp else None
    
    def ts_has_significant_change(self, ts_name: str) -> bool:
        """
        Check if change exceeds significance threshold.
        """
        comp = self.get_ts_comparison(ts_name)
        return comp.has_significant_change if comp else False
    
    # -------------------------------------------------------------------------
    # Degree Change (from structural diff)
    # -------------------------------------------------------------------------
    
    def degree_change(self, direction: str = 'both') -> int:
        """Calculate degree change between snapshots."""
        degree1 = self._calculate_degree(self.snap1_node, direction) if self.snap1_node else 0
        degree2 = self._calculate_degree(self.snap2_node, direction) if self.snap2_node else 0
        return degree2 - degree1
    
    def _calculate_degree(self, node: PGNode, direction: str) -> int:
        if node is None:
            return 0
        
        if node == self.snap1_node:
            edges = self.diff_context._edges_1
        else:
            edges = self.diff_context._edges_2
        
        count = 0
        for edge in edges.values():
            if direction in ('out', 'both') and edge.source == self.oid:
                count += 1
            if direction in ('in', 'both') and edge.target == self.oid:
                count += 1
        return count
    
    def __repr__(self):
        return f"QueryableIntervalDiffNode({self.oid}, type={self.change_type}, label={self.label})"


# =============================================================================
# INTERVAL DIFF QUERY BUILDER
# =============================================================================

class IntervalDiffQueryBuilder:
    """
    Fluent query builder for HyGraphIntervalDiff with time series comparison filters.
    
    Extends DiffQueryBuilder with:
    - where_ts_mean_diff(ts_name, operator, value)
    - where_ts_std_diff(ts_name, operator, value)  
    - where_ts_trend_changed(ts_name)
    - where_ts_correlation(ts_name, operator, value)
    - where_ts_significant_change(ts_name)
    
    Example:
        # Find nodes where average bikes dropped by 5+ between intervals
        diff.query() \\
            .changed_nodes() \\
            .where_ts_mean_diff('num_bikes', '<', -5) \\
            .execute()
        
        # Find nodes where volatility increased
        diff.query() \\
            .changed_nodes() \\
            .where_ts_std_diff('num_bikes', '>', 2) \\
            .execute()
        
        # Find nodes with opposite morning/evening patterns
        diff.query() \\
            .changed_nodes() \\
            .where_ts_correlation('num_bikes', '<', -0.5) \\
            .execute()
    """
    
    def __init__(self, diff: 'HyGraphIntervalDiff'):
        self.diff = diff
        self._query_type: Optional[str] = None
        self._filters: List[Callable] = []
        self._label_filter: Optional[str] = None
        self._limit: Optional[int] = None
        self._order_by: Optional[tuple] = None
        
        # TS comparison filters
        self._ts_mean_diff_filters: List[tuple] = []  # [(ts_name, op, value)]
        self._ts_std_diff_filters: List[tuple] = []
        self._ts_trend_changed_filters: List[str] = []  # [ts_name]
        self._ts_correlation_filters: List[tuple] = []  # [(ts_name, op, value)]
        self._ts_significant_filters: List[str] = []  # [ts_name]
        self._degree_change_filters: List[tuple] = []
    
    # -------------------------------------------------------------------------
    # Query Type Selection
    # -------------------------------------------------------------------------
    
    def changed_nodes(self, label: Optional[str] = None) -> 'IntervalDiffQueryBuilder':
        """Query nodes that exist in both interval snapshots."""
        self._query_type = 'changed_nodes'
        self._label_filter = label
        return self
    
    def added_nodes(self, label: Optional[str] = None) -> 'IntervalDiffQueryBuilder':
        """Query nodes added in interval2."""
        self._query_type = 'added_nodes'
        self._label_filter = label
        return self
    
    def removed_nodes(self, label: Optional[str] = None) -> 'IntervalDiffQueryBuilder':
        """Query nodes removed (only in interval1)."""
        self._query_type = 'removed_nodes'
        self._label_filter = label
        return self
    
    def persistent_nodes(self, label: Optional[str] = None) -> 'IntervalDiffQueryBuilder':
        """Query nodes in both snapshots."""
        self._query_type = 'persistent_nodes'
        self._label_filter = label
        return self
    
    # -------------------------------------------------------------------------
    # Time Series Comparison Filters
    # -------------------------------------------------------------------------
    
    def where_ts_mean_diff(
        self,
        ts_name: str,
        operator: str,
        value: float
    ) -> 'IntervalDiffQueryBuilder':
        """
        Filter by mean value difference between intervals.
        
        Args:
            ts_name: Temporal property name
            operator: '<', '>', '<=', '>=', '=', '!='
            value: Threshold value
        
        Example:
            # Find nodes where mean bikes dropped by more than 5
            .where_ts_mean_diff('num_bikes', '<', -5)
        """
        self._ts_mean_diff_filters.append((ts_name, operator, value))
        return self
    
    def where_ts_std_diff(
        self,
        ts_name: str,
        operator: str,
        value: float
    ) -> 'IntervalDiffQueryBuilder':
        """
        Filter by standard deviation (volatility) change.
        
        Positive std_diff = became more volatile in interval2
        Negative std_diff = became more stable in interval2
        
        Example:
            # Find nodes that became more volatile
            .where_ts_std_diff('num_bikes', '>', 2)
        """
        self._ts_std_diff_filters.append((ts_name, operator, value))
        return self
    
    def where_ts_trend_changed(
        self,
        ts_name: str
    ) -> 'IntervalDiffQueryBuilder':
        """
        Filter for nodes where trend direction changed between intervals.
        
        Example:
            # Find nodes where increasing became decreasing (or vice versa)
            .where_ts_trend_changed('num_bikes')
        """
        self._ts_trend_changed_filters.append(ts_name)
        return self
    
    def where_ts_correlation(
        self,
        ts_name: str,
        operator: str,
        value: float
    ) -> 'IntervalDiffQueryBuilder':
        """
        Filter by correlation between interval1 and interval2 patterns.
        
        Args:
            ts_name: Temporal property name
            operator: '<', '>', '<=', '>='
            value: Correlation threshold (-1 to 1)
        
        Example:
            # Find nodes with anti-correlated patterns (opposite behavior)
            .where_ts_correlation('num_bikes', '<', -0.5)
            
            # Find nodes with similar patterns
            .where_ts_correlation('num_bikes', '>', 0.7)
        """
        self._ts_correlation_filters.append((ts_name, operator, value))
        return self
    
    def where_ts_significant_change(
        self,
        ts_name: str
    ) -> 'IntervalDiffQueryBuilder':
        """
        Filter for nodes with significant change (above threshold).
        
        Threshold is set when creating HyGraphIntervalDiff.
        
        Example:
            .where_ts_significant_change('num_bikes')
        """
        self._ts_significant_filters.append(ts_name)
        return self
    
    def where_degree_change(
        self,
        direction: str,
        operator: str,
        value: int
    ) -> 'IntervalDiffQueryBuilder':
        """
        Filter by degree change.
        """
        self._degree_change_filters.append((direction, operator, value))
        return self
    
    def where(
        self,
        predicate: Callable[[QueryableIntervalDiffNode], bool]
    ) -> 'IntervalDiffQueryBuilder':
        """
        Filter with a lambda predicate.
        
        Example:
            .where(lambda n: n.ts_mean_diff('num_bikes') < -5 and n.ts_trend_changed('num_bikes'))
        """
        self._filters.append(predicate)
        return self
    
    def limit(self, n: int) -> 'IntervalDiffQueryBuilder':
        """Limit results."""
        self._limit = n
        return self
    
    def order_by(
        self,
        key: Union[str, Callable],
        desc: bool = False
    ) -> 'IntervalDiffQueryBuilder':
        """
        Order results.
        
        Example:
            # Order by mean change (most decreased first)
            .order_by(lambda n: n.ts_mean_diff('num_bikes'), desc=False)
        """
        self._order_by = (key, desc)
        return self
    
    # -------------------------------------------------------------------------
    # Execution
    # -------------------------------------------------------------------------
    
    def execute(self) -> List[QueryableIntervalDiffNode]:
        """Execute the query and return matching nodes."""
        if self._query_type is None:
            raise ValueError("Must specify query type")
        
        candidates = self._get_candidates()
        
        results = []
        for candidate in candidates:
            if self._matches_filters(candidate):
                results.append(candidate)
                if self._limit and len(results) >= self._limit:
                    break
        
        if self._order_by:
            key_fn, desc = self._order_by
            if isinstance(key_fn, str):
                results.sort(key=lambda x: x.get_property(key_fn, 0), reverse=desc)
            else:
                results.sort(key=key_fn, reverse=desc)
        
        return results
    
    def _get_candidates(self) -> List[QueryableIntervalDiffNode]:
        """Get candidate nodes based on query type."""
        candidates = []
        
        if self._query_type == 'changed_nodes' or self._query_type == 'persistent_nodes':
            common_ids = self.diff._node_oids_1 & self.diff._node_oids_2
            for oid in common_ids:
                node = QueryableIntervalDiffNode(
                    oid=oid,
                    snap1_node=self.diff._nodes_1.get(oid),
                    snap2_node=self.diff._nodes_2.get(oid),
                    change_type='persistent',
                    diff_context=self.diff
                )
                if self._label_filter is None or node.label == self._label_filter:
                    candidates.append(node)
        
        elif self._query_type == 'added_nodes':
            added_ids = self.diff._node_oids_2 - self.diff._node_oids_1
            for oid in added_ids:
                node = QueryableIntervalDiffNode(
                    oid=oid,
                    snap1_node=None,
                    snap2_node=self.diff._nodes_2.get(oid),
                    change_type='added',
                    diff_context=self.diff
                )
                if self._label_filter is None or node.label == self._label_filter:
                    candidates.append(node)
        
        elif self._query_type == 'removed_nodes':
            removed_ids = self.diff._node_oids_1 - self.diff._node_oids_2
            for oid in removed_ids:
                node = QueryableIntervalDiffNode(
                    oid=oid,
                    snap1_node=self.diff._nodes_1.get(oid),
                    snap2_node=None,
                    change_type='removed',
                    diff_context=self.diff
                )
                if self._label_filter is None or node.label == self._label_filter:
                    candidates.append(node)
        
        return candidates
    
    def _matches_filters(self, entity: QueryableIntervalDiffNode) -> bool:
        """Check if entity matches all filters."""
        # Lambda predicates
        for predicate in self._filters:
            try:
                if not predicate(entity):
                    return False
            except Exception:
                return False
        
        # TS mean diff filters
        for ts_name, op, val in self._ts_mean_diff_filters:
            diff_val = entity.ts_mean_diff(ts_name)
            if diff_val is None:
                return False
            if not self._compare(diff_val, op, val):
                return False
        
        # TS std diff filters
        for ts_name, op, val in self._ts_std_diff_filters:
            diff_val = entity.ts_std_diff(ts_name)
            if diff_val is None:
                return False
            if not self._compare(diff_val, op, val):
                return False
        
        # TS trend changed filters
        for ts_name in self._ts_trend_changed_filters:
            if not entity.ts_trend_changed(ts_name):
                return False
        
        # TS correlation filters
        for ts_name, op, val in self._ts_correlation_filters:
            corr = entity.ts_correlation(ts_name)
            if corr is None:
                return False
            if not self._compare(corr, op, val):
                return False
        
        # TS significant change filters
        for ts_name in self._ts_significant_filters:
            if not entity.ts_has_significant_change(ts_name):
                return False
        
        # Degree change filters
        for direction, op, val in self._degree_change_filters:
            change = entity.degree_change(direction)
            if not self._compare(change, op, val):
                return False
        
        return True
    
    @staticmethod
    def _compare(left: Any, op: str, right: Any) -> bool:
        """Compare values with operator."""
        try:
            if op in ('=', '=='):
                return abs(float(left) - float(right)) < 1e-9
            elif op in ('!=', '<>'):
                return abs(float(left) - float(right)) >= 1e-9
            elif op == '<':
                return float(left) < float(right)
            elif op == '<=':
                return float(left) <= float(right)
            elif op == '>':
                return float(left) > float(right)
            elif op == '>=':
                return float(left) >= float(right)
        except (TypeError, ValueError):
            return False
        return False
    
    # -------------------------------------------------------------------------
    # Convenience Methods
    # -------------------------------------------------------------------------
    
    def count(self) -> int:
        """Return count of matching entities."""
        return len(self.execute())
    
    def first(self) -> Optional[QueryableIntervalDiffNode]:
        """Get first matching entity."""
        results = self.limit(1).execute()
        return results[0] if results else None
    
    def exists(self) -> bool:
        """Check if any matching entities exist."""
        return self.count() > 0
    
    def as_node_ids(self) -> List[str]:
        """Execute and return just the node IDs."""
        return [e.oid for e in self.execute()]

