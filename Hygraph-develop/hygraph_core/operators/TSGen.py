# File: hygraph_core/operators/TSGen.py
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Union, Any, TYPE_CHECKING, Literal
from hygraph_core.model.timeseries import TimeSeries
from hygraph_core.operators.hygraphDiff import HyGraphDiff as HyGraphDiff
from hygraph_core.operators.temporal_snapshot import SnapshotEdge

if TYPE_CHECKING:
    from hygraph_core.operators.snapshotSequence import SnapshotSequence



class TSGen:
    """
    Time Series Generator with hierarchical fluent API.
    
    Computes NEW time series from graph evolution.
    Structure:
        ts_gen.global_     → Aggregate metrics (one value per snapshot)
        ts_gen.entities    → Per-entity COMPUTED metrics (one time series per entity)

    Usage:
        from datetime import timedelta
        
        snapshots = hg.snapshot_sequence(
            start="2024-05-01",
            end="2024-05-31",
            every=timedelta(days=1)
        )
        ts = snapshots.tsgen()
        
        # Global metrics - aggregate across all entities
        node_count = ts.global_.nodes.count(label="Station")
        avg_degree = ts.global_.nodes.degree(label="Station").avg()
        avg_capacity = ts.global_.nodes.property("capacity").avg()
        
        # Edge aggregation between specific nodes
        trip_count = ts.global_.edges.between("station_1", "station_2").count()
        total_duration = ts.global_.edges.between("station_1", "station_2").property("duration").sum()

        # Per-entity COMPUTED metrics (degree is computed, not stored)
        degrees = ts.entities.nodes.degree(label="Station")
    """

    def __init__(self, snapshots: 'SnapshotSequence', context_name: str = "global"):
        self.snapshots = snapshots
        self.context_name = context_name

        self.global_ = GlobalMetrics(self)
        self.entities = EntityMetrics(self)

    def window(self, start: str, end: str) -> 'TSGen':
        """Filter snapshots to time window."""
        return TSGen(self.snapshots.window(start, end), self.context_name)
    
    def __repr__(self):
        return f"TSGen({len(self.snapshots)} snapshots, every={self.snapshots.granularity})"


# =============================================================================
# GLOBAL METRICS (one value per snapshot - aggregates across entities)
# =============================================================================

class GlobalMetrics:
    """
    Global aggregate metrics across entire graph.
    """

    def __init__(self, tsgen: TSGen):
        self.tsgen = tsgen
        self.nodes = GlobalNodeMetrics(tsgen)
        self.edges = GlobalEdgeMetrics(tsgen)
        self.graph = GlobalGraphMetrics(tsgen)


class GlobalNodeMetrics:
    """Global node metrics - aggregates across all nodes."""

    def __init__(self, tsgen: TSGen):
        self.tsgen = tsgen

    def count(self, label: Optional[str] = None) -> TimeSeries:
        """Count nodes per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            nodes = snapshot.get_all_nodes()
            if label:
                count = sum(1 for n in nodes if n.label == label)
            else:
                count = len(nodes)
            results.append((snapshot.when, count))

        return TimeSeries.from_results(results, ["node_count"])

    def degree(self, label: Optional[str] = None, weight: Optional[str] = None) -> 'DegreeAggregator':
        """
        Node degree statistics across all nodes.
        Returns aggregator with .avg(), .max(), .min(), .sum()
        Example:
            ts.global_.nodes.degree(label="Station").avg()
            ts.global_.nodes.degree(label="Station", weight="duration").avg()
        """
        return DegreeAggregator(self.tsgen, label, weight=weight)

    def property(self, property_name: str, label: Optional[str] = None) -> 'PropertyAggregator':
        """
        Property statistics AGGREGATED across all nodes.
        Returns aggregator with .avg(), .max(), .min(), .sum()
        Example:
            ts.global_.nodes.property("capacity").avg()  # Average capacity across all stations
            ts.global_.nodes.property("num_bikes").sum()  # Total bikes across all stations
        """
        return PropertyAggregator(self.tsgen, property_name, label, entity_type="node")


class GlobalEdgeMetrics:
    """Global edge metrics - aggregates across all edges."""

    def __init__(self, tsgen: TSGen):
        self.tsgen = tsgen

    def count(self, label: Optional[str] = None) -> TimeSeries:
        """Count edges per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            edges = snapshot.get_all_edges()
            if label:
                count = sum(1 for e in edges if e.label == label)
            else:
                count = len(edges)
            results.append((snapshot.when, count))

        return TimeSeries.from_results(results, ["edge_count"])

    def property(self, property_name: str, label: Optional[str] = None) -> 'PropertyAggregator':
        """
        Property statistics AGGREGATED across all edges.
        Returns aggregator with .avg(), .max(), .min(), .sum()
        """
        return PropertyAggregator(self.tsgen, property_name, label, entity_type="edge")

    def between(self, source: str, target: str, directed: bool = True) -> 'EdgeBetweenAggregator':
        """
        Aggregate edges between two specific nodes.
        Example:
            ts.global_.edges.between("station_1", "station_2").count()
            ts.global_.edges.between("station_1", "station_2").property("duration").sum()
        """
        return EdgeBetweenAggregator(self.tsgen, source, target, directed)


# =============================================================================
# EDGE BETWEEN AGGREGATORS
# =============================================================================

class EdgeBetweenAggregator:
    """Aggregator for edges between two specific nodes."""
    
    def __init__(self, tsgen: TSGen, source: str, target: str, directed: bool, label: Optional[str] = None):
        self.tsgen = tsgen
        self.source = source
        self.target = target
        self.directed = directed
        self.label = label
    
    def _get_edges_between(self, snapshot) -> List[SnapshotEdge]:
        """Get all edges between source and target in a snapshot."""
        edges = snapshot.get_all_edges()

        if self.directed:
            matched = [e for e in edges
                       if e.source == self.source and e.target == self.target]
        else:
            matched = [e for e in edges
                       if (e.source == self.source and e.target == self.target) or
                       (e.source == self.target and e.target == self.source)]

        if self.label:
            matched = [e for e in matched if e.label == self.label]

        return matched
    
    def count(self, label: Optional[str] = None) -> TimeSeries:
        """Count edges between the two nodes per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            edges = self._get_edges_between(snapshot)
            if label:
                edges = [e for e in edges if e.label == label]
            results.append((snapshot.when, len(edges)))
        
        direction = "directed" if self.directed else "undirected"
        metric = f"edge_count_between_{self.source}_{self.target}_{direction}"
        return TimeSeries.from_results(results, [metric])
    
    def property(self, property_name: str) -> 'EdgeBetweenPropertyAggregator':
        """Aggregate a property of edges between the two nodes."""
        return EdgeBetweenPropertyAggregator(
            self.tsgen, self.source, self.target, self.directed, property_name, self.label
        )


class EdgeBetweenPropertyAggregator:
    """Property aggregator for edges between two specific nodes."""
    
    def __init__(
        self, 
        tsgen: TSGen, 
        source: str, 
        target: str, 
        directed: bool,
        property_name: str, 
        label: Optional[str]
    ):
        self.tsgen = tsgen
        self.source = source
        self.target = target
        self.directed = directed
        self.property_name = property_name
        self.label = label

    def _get_edges_between(self, snapshot) -> List[SnapshotEdge]:
        """Get all edges between source and target in a snapshot."""
        edges = snapshot.get_all_edges()

        if self.directed:
            matched = [e for e in edges
                       if e.source == self.source and e.target == self.target]
        else:
            matched = [e for e in edges
                       if (e.source == self.source and e.target == self.target) or
                       (e.source == self.target and e.target == self.source)]

        if self.label:
            matched = [e for e in matched if e.label == self.label]

        return matched

    def _get_property_values(self, snapshot) -> List[Any]:
        """Get property values from all edges between nodes."""
        edges = self._get_edges_between(snapshot)
        values = []
        for e in edges:
            val = e.properties.get(self.property_name)
            if val is not None and isinstance(val, (int, float)):
                values.append(val)
        return values
    
    def _build_metric_name(self, agg: str) -> str:
        direction = "directed" if self.directed else "undirected"
        return f"{agg}_{self.property_name}_between_{self.source}_{self.target}_{direction}"
    
    def sum(self) -> TimeSeries:
        """Sum of property values for all edges between nodes, per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            values = self._get_property_values(snapshot)
            total = sum(values) if values else 0
            results.append((snapshot.when, total))
        
        return TimeSeries.from_results(results, [self._build_metric_name("sum")])
    
    def avg(self) -> TimeSeries:
        """Average property value across all edges between nodes, per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            values = self._get_property_values(snapshot)
            avg = sum(values) / len(values) if values else None
            results.append((snapshot.when, avg))
        
        return TimeSeries.from_results(results, [self._build_metric_name("avg")])
    
    def min(self) -> TimeSeries:
        """Minimum property value across all edges between nodes, per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            values = self._get_property_values(snapshot)
            min_val = min(values) if values else None
            results.append((snapshot.when, min_val))
        
        return TimeSeries.from_results(results, [self._build_metric_name("min")])
    
    def max(self) -> TimeSeries:
        """Maximum property value across all edges between nodes, per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            values = self._get_property_values(snapshot)
            max_val = max(values) if values else None
            results.append((snapshot.when, max_val))
        
        return TimeSeries.from_results(results, [self._build_metric_name("max")])


# =============================================================================
# AGGREGATORS
# =============================================================================

class DegreeAggregator:
    """
    Aggregator for degree statistics across all nodes.
    Supports both unweighted (edge count) and weighted (sum of edge property) degree.
    """

    def __init__(self, tsgen: TSGen, label: Optional[str], weight: Optional[str] = None):
        self.tsgen = tsgen
        self.label = label
        self.weight = weight

    def _compute_degree(self, node_id: str, edges: List, direction: Literal["in", "out", "both"] = "both") -> float:
        """Compute degree for a node. Supports weighted edges."""
        if direction == "out":
            connected = [e for e in edges if e.source == node_id]
        elif direction == "in":
            connected = [e for e in edges if e.target == node_id]
        else:
            connected = [e for e in edges if e.source == node_id or e.target == node_id]
        
        if self.weight is None:
            return len(connected)
        else:
            total = 0.0
            for e in connected:
                val = e.properties.get(self.weight)
                if val is not None and isinstance(val, (int, float)):
                    total += val
            return total

    def _compute_degrees(self, snapshot, direction: Literal["in", "out", "both"] = "both") -> List[float]:
        """Compute degrees for all nodes in a snapshot."""
        nodes = snapshot.get_all_nodes()
        edges = snapshot.get_all_edges()
        
        if self.label:
            nodes = [n for n in nodes if n.label == self.label]
        
        return [self._compute_degree(n.oid, edges, direction) for n in nodes]

    def _metric_name(self, agg: str, direction: Literal["in", "out", "both"] = "both") -> str:
        """Build metric name including weight if applicable."""
        if self.weight:
            return f"{agg}_weighted_degree_{self.weight}_{direction}"
        return f"{agg}_degree_{direction}"

    def avg(self, direction: Literal["in", "out", "both"] = "both") -> TimeSeries:
        """Average degree per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            degrees = self._compute_degrees(snapshot, direction)
            avg_degree = sum(degrees) / len(degrees) if degrees else None
            results.append((snapshot.when, avg_degree))
        return TimeSeries.from_results(results, [self._metric_name("avg", direction)])

    def max(self, direction: Literal["in", "out", "both"] = "both") -> TimeSeries:
        """Maximum degree per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            degrees = self._compute_degrees(snapshot, direction)
            max_degree = max(degrees) if degrees else None
            results.append((snapshot.when, max_degree))
        return TimeSeries.from_results(results, [self._metric_name("max", direction)])

    def min(self, direction: Literal["in", "out", "both"] = "both") -> TimeSeries:
        """Minimum degree per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            degrees = self._compute_degrees(snapshot, direction)
            min_degree = min(degrees) if degrees else None
            results.append((snapshot.when, min_degree))
        return TimeSeries.from_results(results, [self._metric_name("min", direction)])

    def sum(self, direction: Literal["in", "out", "both"] = "both") -> TimeSeries:
        """Sum of all degrees per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            degrees = self._compute_degrees(snapshot, direction)
            total = sum(degrees)
            results.append((snapshot.when, total))
        return TimeSeries.from_results(results, [self._metric_name("sum", direction)])


class PropertyAggregator:
    """
    Aggregator for property statistics ACROSS ALL entities.
    """

    def __init__(self, tsgen: TSGen, property_name: str, label: Optional[str], entity_type: str):
        self.tsgen = tsgen
        self.property_name = property_name
        self.label = label
        self.entity_type = entity_type

    def _get_entities(self, snapshot) -> List:
        """Get entities from snapshot based on entity_type."""
        if self.entity_type == "node":
            entities = snapshot.get_all_nodes()
        else:
            entities = snapshot.get_all_edges()
        
        if self.label:
            entities = [e for e in entities if e.label == self.label]
        
        return entities

    def _get_values(self, snapshot) -> List:
        """Get numeric property values from entities."""
        entities = self._get_entities(snapshot)
        values = []
        for e in entities:
            val = e.properties.get(self.property_name)
            if val is not None and isinstance(val, (int, float)):
                values.append(val)
        return values

    def avg(self) -> TimeSeries:
        """Average property value per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            values = self._get_values(snapshot)
            avg = sum(values) / len(values) if values else None
            results.append((snapshot.when, avg))
        return TimeSeries.from_results(results, [f"avg_{self.property_name}"])

    def sum(self) -> TimeSeries:
        """Sum of property values per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            values = self._get_values(snapshot)
            total = sum(values) if values else 0
            results.append((snapshot.when, total))
        return TimeSeries.from_results(results, [f"sum_{self.property_name}"])

    def max(self) -> TimeSeries:
        """Maximum property value per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            values = self._get_values(snapshot)
            max_val = max(values) if values else None
            results.append((snapshot.when, max_val))
        return TimeSeries.from_results(results, [f"max_{self.property_name}"])

    def min(self) -> TimeSeries:
        """Minimum property value per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            values = self._get_values(snapshot)
            min_val = min(values) if values else None
            results.append((snapshot.when, min_val))
        return TimeSeries.from_results(results, [f"min_{self.property_name}"])


# =============================================================================
# GRAPH-LEVEL METRICS
# =============================================================================

class GlobalGraphMetrics:
    """Global graph-level metrics over time."""

    def __init__(self, tsgen: TSGen):
        self.tsgen = tsgen

    def density(self, label: Optional[str] = None, directed: bool = True) -> TimeSeries:
        """Graph density per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            d = snapshot.density(label=label, directed=directed)
            results.append((snapshot.when, d))
        return TimeSeries.from_results(results, ["density"])

    def connected_components(self, label: Optional[str] = None, directed: bool = True) -> TimeSeries:
        """Number of connected components per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            cc = snapshot.connected_components(label=label, directed=directed)
            results.append((snapshot.when, cc.count))
        return TimeSeries.from_results(results, ["component_count"])

    def biggest_component(self, label: Optional[str] = None, directed: bool = True) -> TimeSeries:
        """Size of the largest connected component per snapshot."""
        results = []
        for snapshot in self.tsgen.snapshots:
            cc = snapshot.connected_components(label=label, directed=directed)
            results.append((snapshot.when, cc.biggest))
        return TimeSeries.from_results(results, ["biggest_component"])







# =============================================================================
# ENTITY METRICS (one time series per entity)
# =============================================================================

class EntityMetrics:
    """
    Per-entity node degree metrics (returns dict of TimeSeries).
    """

    def __init__(self, tsgen: TSGen):
        self.tsgen = tsgen
        self.nodes = EntityNodeMetrics(tsgen)


class EntityNodeMetrics:
    """
    Per-node COMPUTED metrics.
    """

    def __init__(self, tsgen: TSGen):
        self.tsgen = tsgen
        """
        Degree evolution per node

        @:param    direction: "in", "out", or "both"
        @:param    label: Filter nodes by label
        @:param    node_id: return single TimeSeries for this node
        @:param    weight: Edge property to use as weight.

        Returns:
            If node_id: Single TimeSeries
            Else: Dict[node_id -> TimeSeries]
        Example:
            ts.entities.nodes.degree(label="Station")
            ts.entities.nodes.degree(label="Station", weight="duration")
        """
    def degree(
        self,
        node_id: Any = None,
        direction: Literal["in", "out", "both"] = "both",
        label: Optional[str] = None,
        weight: Optional[str] = None
    ) -> Union[Dict[str, TimeSeries], TimeSeries]:
        """
        Compute degree evolution for node(s) across all snapshots.
        
        Args:
            node_id: If provided, return TimeSeries for this specific node.
                    If None, return dict of TimeSeries for all nodes.
            direction: "in", "out", or "both"
            label: Filter nodes by label
            weight: Edge property to use as weight (sum instead of count)
        
        Returns:
            If node_id: Single TimeSeries
            Else: Dict[node_id -> TimeSeries]
        
        Example:
            # Single node
            ts.entities.nodes.degree(node_id="2172", label="Station")
            
            # All nodes
            all_degrees = ts.entities.nodes.degree(label="Station")
        """
        if direction not in ("in", "out", "both"):
            raise ValueError("direction must be 'in', 'out', or 'both'")

        # ACCUMULATE results across all snapshots (BUG FIX: was overwriting)
        series: Dict[str, list] = defaultdict(list)

        for snapshot in self.tsgen.snapshots:
            # Get degree results for this snapshot
            snapshot_result = snapshot.degree(node_id, label, weight, direction)
            
            # MERGE results into accumulated series
            for nid, points in snapshot_result.items():
                series[nid].extend(points)

        metric_name = (
            f"weighted_degree_{weight}_{direction}" if weight else f"degree_{direction}"
        )

        if node_id is not None:
            if node_id in series:
                return TimeSeries.from_results(series[node_id], [metric_name])
            else:
                # Node not found in any snapshot
                return TimeSeries.from_results([], [metric_name])

        return {nid: TimeSeries.from_results(points, [metric_name])
                for nid, points in series.items()}
