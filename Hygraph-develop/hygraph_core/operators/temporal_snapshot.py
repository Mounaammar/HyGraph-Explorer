"""
Temporal Snapshot - Core operator for graph state at a point or interval in time.

Architecture:
- SnapshotNode/SnapshotEdge: Immutable dataclasses with SEPARATED properties
- Snapshot: Main class for querying graph state

Key Design:
- static_properties: ALWAYS static properties only
- temporal_properties:
    * Graph mode: Contains ts_ids (references to timeseries)
    * Hybrid mode: Contains resolved SCALAR VALUES at that timestamp
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Literal
from hygraph_core.operators.graph_metrics import connected_components, density, degree_metric


# =============================================================================
# DATA CLASSES - Immutable graph elements with separated properties
# =============================================================================

@dataclass(frozen=True)
class SnapshotNode:
    """
    Immutable node in a snapshot.

    Properties are ALWAYS separated:
    - properties: Static properties only
    - temporal_properties:
        * Graph mode: ts_id mappings (var_name -> ts_id)
        * Hybrid mode: resolved values (var_name -> scalar_value)
    """
    oid: str
    label: str
    properties: Dict[str, Any]           # ONLY static properties
    temporal_properties: Dict[str, Any]  # ts_ids (graph) OR values (hybrid)
    valid_from: Optional[str] = None     # For interval snapshots
    valid_to: Optional[str] = None       # For interval snapshots

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for internal serialization."""
        return {
            'oid': self.oid,
            'label': self.label,
            'static_properties': self.properties,
            'temporal_properties': self.temporal_properties,
            'valid_from': self.valid_from,
            'valid_to': self.valid_to,
        }

    def to_api_response(self) -> Dict[str, Any]:
        """Convert to API response format."""
        return {
            'oid': self.oid,
            'label': self.label,
            'start_time': self.valid_from,
            'end_time': self.valid_to,
            'static_properties': self.properties,
            'temporal_properties': self.temporal_properties,
        }


@dataclass(frozen=True)
class SnapshotEdge:
    """
    Immutable edge in a snapshot.

    Properties are ALWAYS separated:
    - properties: Static properties only
    - temporal_properties:
        * Graph mode: ts_id mappings (var_name -> ts_id)
        * Hybrid mode: resolved values (var_name -> scalar_value)
    """
    oid: str
    label: str
    source: str
    target: str
    properties: Dict[str, Any]           # ONLY static properties
    temporal_properties: Dict[str, Any]  # ts_ids (graph) OR values (hybrid)
    valid_from: Optional[str] = None     # For interval snapshots
    valid_to: Optional[str] = None       # For interval snapshots

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for internal serialization."""
        return {
            'oid': self.oid,
            'label': self.label,
            'source': self.source,
            'target': self.target,
            'static_properties': self.properties,
            'temporal_properties': self.temporal_properties,
            'valid_from': self.valid_from,
            'valid_to': self.valid_to,
        }

    def to_api_response(self) -> Dict[str, Any]:
        """Convert to API response format."""
        return {
            'oid': self.oid,
            'label': self.label,
            'source': self.source,
            'target': self.target,
            'start_time': self.valid_from,
            'end_time': self.valid_to,
            'static_properties': self.properties,
            'temporal_properties': self.temporal_properties,
        }


# =============================================================================
# SNAPSHOT CLASS
# =============================================================================

class Snapshot:
    """
    Snapshot of a HyGraph at a specific time or over an interval.

    Two modes:
    - "graph": temporal_properties contain ts_ids (references)
    - "hybrid": temporal_properties contain resolved scalar VALUES

    For point snapshots (when=timestamp):
    - "graph": Static graph at T, temporal_properties = ts_ids
    - "hybrid": Static graph at T, temporal_properties = scalar values at T

    For interval snapshots (when=(start, end)):
    - "graph": Temporal graph, temporal_properties = ts_ids
    - "hybrid" + ts_handling="aggregate": temporal_properties = aggregated values
    - "hybrid" + ts_handling="slice": temporal_properties = arrays of values
    """

    def __init__(self, hg, when, mode, ts_handling=None, aggregation_fn="mean"):
        """
        Args:
           hg: 
            when: Timestamp string OR tuple (start, end) for interval
            mode: "graph" or "hybrid"
            ts_handling: For intervals with hybrid mode - "aggregate" or "slice"
            aggregation_fn: Aggregation function for "aggregate" mode
        """
        self.age = hg.age
        self.timescale = hg.timescale
        self.when = when
        self.mode = mode
        self.ts_handling = ts_handling
        self.aggregation_fn = aggregation_fn

        self._nodes: Optional[List[SnapshotNode]] = None
        self._edges: Optional[List[SnapshotEdge]] = None
        self._measurements: Optional[Dict[str, Any]] = None

        self._validate()

    def _validate(self):
        """Validate constructor parameters"""
        if self.mode not in ("graph", "hybrid"):
            raise ValueError(f"Invalid mode: {self.mode}. Must be 'graph' or 'hybrid'")

        if self.is_interval():
            if self.mode == "hybrid" and self.ts_handling is None:
                raise ValueError("ts_handling is required for hybrid mode with intervals")
            if self.ts_handling is not None and self.ts_handling not in ("aggregate", "slice"):
                raise ValueError(f"Invalid ts_handling: {self.ts_handling}")
        else:
            if self.ts_handling is not None:
                raise ValueError("ts_handling only applies to interval snapshots")

    def is_interval(self) -> bool:
        """Check if this is an interval snapshot"""
        return isinstance(self.when, tuple)

    # =========================================================================
    # LOAD GRAPH STRUCTURE
    # =========================================================================

    def _load_graph(self):
        """
        Load graph structure from AGE.
        Separates static properties from temporal properties.
        
        Temporal filtering:
        - Point snapshot (when=timestamp): entities valid AT that exact time
        - Interval snapshot (when=(start, end)): entities overlapping with interval
        """
        if self._nodes is not None:
            return

        if self.is_interval():
            start, end = self.when
            nodes = self.age.query_nodes(start_time=start, end_time=end)
            edges = self.age.query_edges(start_time=start, end_time=end)
        else:
            nodes = self.age.query_nodes(at_time=self.when)
            edges = self.age.query_edges(at_time=self.when)

        # Process nodes - separate static from temporal
        self._nodes = []
        for node in nodes:
            props = dict(node.get("properties", {}))
            temporal_props = props.pop("temporal_properties", {})

            self._nodes.append(
                SnapshotNode(
                    oid=str(node["uid"]),  # Ensure string for consistent comparison
                    label=node["label"],
                    properties=props,
                    temporal_properties=temporal_props,
                    valid_from=node.get("start_time") if self.is_interval() else None,
                    valid_to=node.get("end_time") if self.is_interval() else None
                )
            )

        # Process edges - separate static from temporal
        self._edges = []
        for edge in edges:
            props = dict(edge.get("properties", {}))
            temporal_props = props.pop("temporal_properties", {})

            self._edges.append(
                SnapshotEdge(
                    oid=str(edge["uid"]),  # Ensure string
                    label=edge["label"],
                    source=str(edge["src_uid"]),  # Ensure string for consistent comparison
                    target=str(edge["dst_uid"]),  # Ensure string for consistent comparison
                    properties=props,
                    temporal_properties=temporal_props,
                    valid_from=edge.get("start_time") if self.is_interval() else None,
                    valid_to=edge.get("end_time") if self.is_interval() else None
                )
            )

    # =========================================================================
    # LOAD MEASUREMENTS (Hybrid Mode Only)
    # =========================================================================

    def _load_measurements(self):
        """Load scalar values from TimescaleDB (hybrid mode only)."""
        if self.mode == "graph":
            return

        if self._measurements is not None:
            return

        self._load_graph()

        # Collect all ts_ids
        ts_ids = set()
        for node in self._nodes:
            for ts_id in node.temporal_properties.values():
                ts_ids.add(str(ts_id))
        for edge in self._edges:
            for ts_id in edge.temporal_properties.values():
                ts_ids.add(str(ts_id))

        if not ts_ids:
            self._measurements = {}
            return

        ts_id_list = list(ts_ids)

        if self.is_interval():
            start, end = self.when

            if self.ts_handling == "aggregate":
                results = self.timescale.aggregate_measurements(
                    entity_uids=ts_id_list,
                    start_time=start,
                    end_time=end,
                    aggregation=self.aggregation_fn
                )
                self._measurements = {
                    row["entity_uid"]: row["value"]
                    for row in results
                }

            elif self.ts_handling == "slice":
                results = self.timescale.query_measurements(
                    entity_uids=ts_id_list,
                    start_time=start,
                    end_time=end
                )
                self._measurements = {}
                for row in results:
                    uid = row["entity_uid"]
                    if uid not in self._measurements:
                        self._measurements[uid] = []
                    self._measurements[uid].append({
                        'timestamp': row['ts'],
                        'value': row['value']
                    })
        else:
            # Point snapshot - get last value before timestamp
            results = self.timescale.get_last_values(
                entity_uids=ts_id_list,
                timestamp=self.when
            )
            self._measurements = {
                row["entity_uid"]: row["value"]
                for row in results
            }

    def _resolve_temporal_properties(self, temporal_props: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve ts_ids to scalar values (hybrid mode)."""
        resolved = {}
        for var_name, ts_id in temporal_props.items():
            resolved[var_name] = self._measurements.get(str(ts_id))
        return resolved

    # =========================================================================
    # PUBLIC API - Get Nodes/Edges
    # =========================================================================

    def get_all_nodes(self) -> List[SnapshotNode]:
        """
        Get all nodes.

        - Graph mode: temporal_properties = ts_ids
        - Hybrid mode: temporal_properties = scalar values
        """
        self._load_graph()

        if self.mode == "graph":
            return self._nodes

        # Hybrid mode: resolve to scalar values
        self._load_measurements()

        resolved_nodes = []
        for node in self._nodes:
            resolved_temporal = self._resolve_temporal_properties(node.temporal_properties)
            resolved_nodes.append(
                SnapshotNode(
                    oid=node.oid,
                    label=node.label,
                    properties=node.properties,
                    temporal_properties=resolved_temporal,
                    valid_from=node.valid_from,
                    valid_to=node.valid_to
                )
            )
        return resolved_nodes

    def get_all_edges(self) -> List[SnapshotEdge]:
        """
        Get all edges.

        - Graph mode: temporal_properties = ts_ids
        - Hybrid mode: temporal_properties = scalar values
        """
        self._load_graph()

        if self.mode == "graph":
            return self._edges

        # Hybrid mode: resolve to scalar values
        self._load_measurements()

        resolved_edges = []
        for edge in self._edges:
            resolved_temporal = self._resolve_temporal_properties(edge.temporal_properties)
            resolved_edges.append(
                SnapshotEdge(
                    oid=edge.oid,
                    label=edge.label,
                    source=edge.source,
                    target=edge.target,
                    properties=edge.properties,
                    temporal_properties=resolved_temporal,
                    valid_from=edge.valid_from,
                    valid_to=edge.valid_to
                )
            )
        return resolved_edges

    def get_all_timeseries(self):
        """get all timeseries of nodes and edges"""
        self._load_measurements()
        return self._measurements.copy() if self._measurements is not None else {}

    # =========================================================================
    # SERIALIZATION
    # =========================================================================

    def to_dict(self) -> Dict[str, Any]:
        """Convert entire snapshot to dictionary."""
        nodes = self.get_all_nodes()
        edges = self.get_all_edges()

        return {
            'when': self.when if not self.is_interval() else {'start': self.when[0], 'end': self.when[1]},
            'mode': self.mode,
            'is_interval': self.is_interval(),
            'nodes': [n.to_dict() for n in nodes],
            'edges': [e.to_dict() for e in edges],
            'node_count': len(nodes),
            'edge_count': len(edges),
        }

    def to_api_response(self, include_graph: bool = True) -> Dict[str, Any]:
        """
        Convert to API response format.

        - Graph mode: temporal_properties = ts_ids
        - Hybrid mode: temporal_properties = scalar values
        """
        nodes = self.get_all_nodes()
        edges = self.get_all_edges()

        response = {
            'timestamp': self.when if not self.is_interval() else None,
            'interval': {'start': self.when[0], 'end': self.when[1]} if self.is_interval() else None,
            'mode': self.mode,
            'metadata': {
                'node_count': len(nodes),
                'edge_count': len(edges),
            }
        }

        if include_graph:
            response['nodes'] = [n.to_api_response() for n in nodes]
            response['edges'] = [e.to_api_response() for e in edges]

        return response

    # =========================================================================
    # METRICS
    # =========================================================================

    def count_nodes(self) -> int:
        self._load_graph()
        return len(self._nodes)

    def count_edges(self) -> int:
        self._load_graph()
        return len(self._edges)

    def count_timeseries(self) -> int:
        self._load_graph()
        count = 0
        for node in self._nodes:
            count += len(node.temporal_properties)
        for edge in self._edges:
            count += len(edge.temporal_properties)
        return count

    def density(self, label: Optional[str] = None, directed: bool = True) -> float:
        """Compute graph density at this snapshot."""
        return density(self, directed)

    def connected_components(self, label: Optional[str] = None, directed: bool = True):
        """Find connected components at this snapshot."""
        return connected_components(self, label=label, directed=directed)

    def degree(self, node_id=None, label=None, weight=None, direction: Literal["in", "out", "both"] = "both"):
        """Compute degree metrics."""
        return degree_metric(self, node_id, label, weight, direction)

    def _nodes_edges_snapshot(self, label: Optional[str] = None):
        """Helper function for metrics computation."""
        nodes = self.get_all_nodes()
        edges = self.get_all_edges()

        if label:
            nodes = [n for n in nodes if n.label == label]
            node_ids = {n.oid for n in nodes}
            edges = [e for e in edges if e.source in node_ids and e.target in node_ids]

        return nodes, edges

    def __repr__(self):
        self._load_graph()
        when_str = f"{self.when[0]} to {self.when[1]}" if self.is_interval() else self.when
        return f"Snapshot(when={when_str}, mode={self.mode}, nodes={len(self._nodes)}, edges={len(self._edges)})"

    def debug_info(self) -> dict:
        """
        Get debug information about this snapshot.
        Useful for diagnosing empty snapshot issues.
        """
        self._load_graph()
        
        # Get sample node/edge
        sample_node = self._nodes[0] if self._nodes else None
        sample_edge = self._edges[0] if self._edges else None
        
        # Count by label
        node_labels = {}
        for n in self._nodes:
            node_labels[n.label] = node_labels.get(n.label, 0) + 1
        
        edge_labels = {}
        for e in self._edges:
            edge_labels[e.label] = edge_labels.get(e.label, 0) + 1
        
        return {
            'when': self.when,
            'is_interval': self.is_interval(),
            'mode': self.mode,
            'node_count': len(self._nodes),
            'edge_count': len(self._edges),
            'node_labels': node_labels,
            'edge_labels': edge_labels,
            'sample_node': {
                'oid': sample_node.oid,
                'label': sample_node.label,
                'properties_keys': list(sample_node.properties.keys()),
                'temporal_properties_keys': list(sample_node.temporal_properties.keys())
            } if sample_node else None,
            'sample_edge': {
                'oid': sample_edge.oid,
                'label': sample_edge.label,
                'source': sample_edge.source,
                'target': sample_edge.target,
            } if sample_edge else None
        }
