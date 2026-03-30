"""
HyGraph Storage - In-Memory Backend (NetworkX + Xarray)

Memory-based storage using:
- NetworkX MultiDiGraph for graph structure
- Xarray for time series data

Fast, no persistence, great for operators and analysis.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple, Callable, TYPE_CHECKING
import networkx as nx
import xarray as xr
import pandas as pd
import numpy as np
import copy

from .base import StorageBackend, Node, Edge, validate_node_type, validate_edge_type

# Import from model
try:
    from hygraph_core.model.nodes import PGNode, TSNode
    from hygraph_core.model.edges import PGEdge, TSEdge
    from hygraph_core.model.base import FAR_FUTURE
except ImportError:
    from typing import Any as PGNode, TSNode, PGEdge, TSEdge
    from datetime import datetime
    FAR_FUTURE = datetime(2100, 12, 31, 23, 59, 59)


class MemoryStorage(StorageBackend):
    """
    In-memory storage backend using NetworkX + Xarray.
    
    Data is stored in:
    - self.graph: NetworkX MultiDiGraph for nodes and edges
    - self.timeseries: Dict of Xarray DataArrays for time series
    
    Example:
        storage = MemoryStorage()
        storage.insert_node(node)
        storage.insert_timeseries('station_1', 'bikes', timestamps, values)
        result = storage.get_node('node_1')
    """

    def __init__(self):
        """Initialize memory storage"""
        # Graph storage (NetworkX)
        self.graph = nx.MultiDiGraph()
        
        # Time series storage (Xarray)
        # Structure: {entity_uid: {variable: xr.DataArray}}
        self.timeseries: Dict[str, Dict[str, xr.DataArray]] = {}
        
        # State
        self._connected = True
        self._in_transaction = False
        self._transaction_backup = None

    # =========================================================================
    # Connection Management
    # =========================================================================

    def connect(self) -> None:
        """Establish connection (no-op for memory storage)"""
        self._connected = True

    def disconnect(self) -> None:
        """Close connection (no-op for memory storage)"""
        self._connected = False

    def is_connected(self) -> bool:
        """Check if connected"""
        return self._connected

    # =========================================================================
    # Transaction Management
    # =========================================================================

    def begin_transaction(self) -> None:
        """Begin a transaction by creating backup"""
        if not self._in_transaction:
            self._transaction_backup = {
                'graph': copy.deepcopy(self.graph),
                'timeseries': copy.deepcopy(self.timeseries)
            }
            self._in_transaction = True

    def commit(self) -> None:
        """Commit transaction"""
        if self._in_transaction:
            self._transaction_backup = None
            self._in_transaction = False

    def rollback(self) -> None:
        """Rollback transaction"""
        if self._in_transaction and self._transaction_backup:
            self.graph = self._transaction_backup['graph']
            self.timeseries = self._transaction_backup['timeseries']
            self._transaction_backup = None
            self._in_transaction = False

    # =========================================================================
    # Node Operations
    # =========================================================================

    def insert_node(self, node: Node) -> None:
        """Insert a node"""
        validate_node_type(node)

        if self.node_exists(node.oid):
            raise ValueError(f"Node {node.oid} already exists")

        # Store node in graph with all its data
        self.graph.add_node(
            node.oid,
            data=node,
            label=node.label,
            start_time=node.start_time,
            end_time=node.end_time,
            type=type(node).__name__
        )

    def get_node(self, oid: str) -> Optional[Node]:
        """Get a node by ID"""
        if not self.node_exists(oid):
            return None
        return self.graph.nodes[oid]['data']

    def update_node(self, oid: str, updates: Dict[str, Any]) -> None:
        """Update node properties"""
        node = self.get_node(oid)
        if node is None:
            raise ValueError(f"Node {oid} doesn't exist")

        # Update node properties
        for key, value in updates.items():
            if key in ['start_time', 'end_time']:
                setattr(node, key, value)
                self.graph.nodes[oid][key] = value
            elif hasattr(node, '_static_properties'):
                node.add_static_property(key, value)

    def delete_node(self, oid: str, hard: bool = False) -> None:
        """Delete a node"""
        if not self.node_exists(oid):
            raise ValueError(f"Node {oid} doesn't exist")

        if hard:
            # Hard delete: remove from graph
            self.graph.remove_node(oid)
            # Also remove time series
            if oid in self.timeseries:
                del self.timeseries[oid]
        else:
            # Soft delete: set end_time to now
            node = self.get_node(oid)
            node.end_time = datetime.now()
            self.graph.nodes[oid]['end_time'] = node.end_time

    def node_exists(self, oid: str) -> bool:
        """Check if node exists"""
        return self.graph.has_node(oid)

    # =========================================================================
    # Edge Operations
    # =========================================================================

    def insert_edge(self, edge: Edge) -> None:
        """Insert an edges"""
        validate_edge_type(edge)

        if not self.node_exists(edge.source):
            raise ValueError(f"Source node {edge.source} doesn't exist")
        if not self.node_exists(edge.target):
            raise ValueError(f"Target node {edge.target} doesn't exist")

        if self.edge_exists(edge.oid):
            raise ValueError(f"Edge {edge.oid} already exists")

        self.graph.add_edge(
            edge.source,
            edge.target,
            key=edge.oid,
            data=edge,
            oid=edge.oid,
            label=edge.label,
            start_time=edge.start_time,
            end_time=edge.end_time,
            type=type(edge).__name__
        )

    def get_edge(self, oid: str) -> Optional[Edge]:
        """Get an edges by ID"""
        for u, v, key, data in self.graph.edges(keys=True, data=True):
            if data.get('oid') == oid:
                return data['data']
        return None

    def update_edge(self, oid: str, updates: Dict[str, Any]) -> None:
        """Update edges properties"""
        edge = self.get_edge(oid)
        if edge is None:
            raise ValueError(f"Edge {oid} doesn't exist")

        for key, value in updates.items():
            if key in ['start_time', 'end_time']:
                setattr(edge, key, value)
                for u, v, k, d in self.graph.edges(keys=True, data=True):
                    if d.get('oid') == oid:
                        d[key] = value
            elif hasattr(edge, '_static_properties'):
                edge.add_static_property(key, value)

    def delete_edge(self, oid: str, hard: bool = False) -> None:
        """Delete an edges"""
        edge = self.get_edge(oid)
        if edge is None:
            raise ValueError(f"Edge {oid} doesn't exist")

        if hard:
            for u, v, key, data in list(self.graph.edges(keys=True, data=True)):
                if data.get('oid') == oid:
                    self.graph.remove_edge(u, v, key)
                    # Remove time series if any
                    if oid in self.timeseries:
                        del self.timeseries[oid]
                    break
        else:
            edge.end_time = datetime.now()
            for u, v, key, data in self.graph.edges(keys=True, data=True):
                if data.get('oid') == oid:
                    data['end_time'] = edge.end_time
                    break

    def edge_exists(self, oid: str) -> bool:
        """Check if edges exists"""
        for u, v, key, data in self.graph.edges(keys=True, data=True):
            if data.get('oid') == oid:
                return True
        return False

    # =========================================================================
    # Query Operations
    # =========================================================================

    def query_nodes(
        self,
        label: Optional[str] = None,
        predicate: Optional[Callable[[Node], bool]] = None,
        at_time: Optional[datetime] = None,
        between: Optional[Tuple[datetime, datetime]] = None,
        limit: Optional[int] = None
    ) -> List[Node]:
        """Query nodes with filters"""
        results = []

        for node_id, node_data in self.graph.nodes(data=True):
            node = node_data['data']

            if label and node.label != label:
                continue

            if at_time:
                if not (node.start_time <= at_time <= node.end_time):
                    continue

            if between:
                start, end = between
                if node.end_time < start or node.start_time > end:
                    continue

            if predicate and not predicate(node):
                continue

            results.append(node)

            if limit and len(results) >= limit:
                break

        return results

    def query_edges(
        self,
        label: Optional[str] = None,
        source: Optional[str] = None,
        target: Optional[str] = None,
        predicate: Optional[Callable[[Edge], bool]] = None,
        at_time: Optional[datetime] = None,
        between: Optional[Tuple[datetime, datetime]] = None,
        limit: Optional[int] = None
    ) -> List[Edge]:
        """Query edges with filters"""
        results = []

        for u, v, key, edge_data in self.graph.edges(keys=True, data=True):
            edge = edge_data['data']

            if source and edge.source != source:
                continue
            if target and edge.target != target:
                continue
            if label and edge.label != label:
                continue

            if at_time:
                if not (edge.start_time <= at_time <= edge.end_time):
                    continue

            if between:
                start, end = between
                if edge.end_time < start or edge.start_time > end:
                    continue

            if predicate and not predicate(edge):
                continue

            results.append(edge)

            if limit and len(results) >= limit:
                break

        return results

    # =========================================================================
    # Traversal Operations
    # =========================================================================

    def get_neighbors(
        self,
        oid: str,
        direction: str = 'out',
        label: Optional[str] = None
    ) -> List[str]:
        """Get neighbor node IDs"""
        if not self.node_exists(oid):
            return []

        neighbors = []

        if direction in ['out', 'both']:
            for target in self.graph.successors(oid):
                if label:
                    for u, v, key, data in self.graph.edges(oid, target, keys=True, data=True):
                        if data.get('label') == label:
                            neighbors.append(target)
                            break
                else:
                    neighbors.append(target)

        if direction in ['in', 'both']:
            for source in self.graph.predecessors(oid):
                if label:
                    for u, v, key, data in self.graph.edges(source, oid, keys=True, data=True):
                        if data.get('label') == label:
                            neighbors.append(source)
                            break
                else:
                    neighbors.append(source)

        return list(set(neighbors))

    def get_edges_between(
        self,
        source: str,
        target: str,
        label: Optional[str] = None
    ) -> List[Edge]:
        """Get edges between two nodes"""
        edges = []

        if not self.graph.has_edge(source, target):
            return edges

        for key, data in self.graph[source][target].items():
            edge = data['data']
            if label and edge.label != label:
                continue
            edges.append(edge)

        return edges

    # =========================================================================
    # TIME SERIES OPERATIONS (Xarray)
    # =========================================================================

    def insert_timeseries(
        self,
        entity_uid: str,
        variable: str,
        timestamps: List[datetime],
        values: List[float]
    ) -> None:
        """
        Insert time series data into Xarray storage.
        
        Args:
            entity_uid: Node or edges ID
            variable: Variable name (e.g., 'num_bikes_available')
            timestamps: List of timestamps
            values: List of values
        """
        # Create Xarray DataArray
        da = xr.DataArray(
            values,
            coords={'time': pd.DatetimeIndex(timestamps)},
            dims=['time'],
            name=variable
        )
        
        # Store in timeseries dict
        if entity_uid not in self.timeseries:
            self.timeseries[entity_uid] = {}
        
        self.timeseries[entity_uid][variable] = da

    def get_timeseries(
        self,
        entity_uid: str,
        variable: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Optional[xr.DataArray]:
        """
        Get time series data from Xarray storage.
        
        Returns:
            Xarray DataArray or None if not found
        """
        if entity_uid not in self.timeseries:
            return None
        
        if variable not in self.timeseries[entity_uid]:
            return None
        
        da = self.timeseries[entity_uid][variable]
        
        # Apply time range filter if specified
        if start_time or end_time:
            if start_time:
                da = da.sel(time=da.time >= pd.Timestamp(start_time))
            if end_time:
                da = da.sel(time=da.time <= pd.Timestamp(end_time))
        
        return da

    def get_all_timeseries_variables(self, entity_uid: str) -> List[str]:
        """Get all variable names for an entity"""
        if entity_uid not in self.timeseries:
            return []
        
        return list(self.timeseries[entity_uid].keys())

    def delete_timeseries(
        self,
        entity_uid: str,
        variable: Optional[str] = None
    ) -> None:
        """
        Delete time series data.
        
        Args:
            entity_uid: Node or edges ID
            variable: Variable name (if None, delete all variables for entity)
        """
        if entity_uid not in self.timeseries:
            return
        
        if variable:
            # Delete specific variable
            if variable in self.timeseries[entity_uid]:
                del self.timeseries[entity_uid][variable]
        else:
            # Delete all variables for entity
            del self.timeseries[entity_uid]

    def timeseries_exists(self, entity_uid: str, variable: str) -> bool:
        """Check if time series exists"""
        return (entity_uid in self.timeseries and 
                variable in self.timeseries[entity_uid])

    # =========================================================================
    # Statistics
    # =========================================================================

    def count_nodes(self, label: Optional[str] = None) -> int:
        """Count nodes"""
        if label is None:
            return self.graph.number_of_nodes()
        else:
            return sum(1 for _, data in self.graph.nodes(data=True)
                       if data['label'] == label)

    def count_edges(self, label: Optional[str] = None) -> int:
        """Count edges"""
        if label is None:
            return self.graph.number_of_edges()
        else:
            return sum(1 for _, _, _, data in self.graph.edges(keys=True, data=True)
                       if data['label'] == label)

    def get_labels(self) -> Dict[str, int]:
        """Get all labels with counts"""
        labels = {}

        for _, data in self.graph.nodes(data=True):
            label = data['label']
            labels[label] = labels.get(label, 0) + 1

        for _, _, _, data in self.graph.edges(keys=True, data=True):
            label = data['label']
            key = f"edges:{label}"
            labels[key] = labels.get(key, 0) + 1

        return labels

    # =========================================================================
    # Bulk Operations
    # =========================================================================

    def bulk_insert_nodes(self, nodes: List[Node]) -> None:
        """Insert multiple nodes efficiently"""
        for node in nodes:
            self.insert_node(node)

    def bulk_insert_edges(self, edges: List[Edge]) -> None:
        """Insert multiple edges efficiently"""
        for edge in edges:
            self.insert_edge(edge)

    # =========================================================================
    # Utility
    # =========================================================================

    def clear(self) -> None:
        """Clear all data"""
        self.graph.clear()
        self.timeseries.clear()

    def copy(self) -> 'MemoryStorage':
        """Create a deep copy"""
        new_storage = MemoryStorage()
        new_storage.graph = copy.deepcopy(self.graph)
        new_storage.timeseries = copy.deepcopy(self.timeseries)
        return new_storage

    def stats(self) -> Dict[str, Any]:
        """Get backend statistics"""
        return {
            'backend': 'MemoryStorage',
            'nodes': self.count_nodes(),
            'edges': self.count_edges(),
            'labels': self.get_labels(),
            'timeseries_entities': len(self.timeseries),
            'connected': self._connected,
            'in_transaction': self._in_transaction
        }

    def __repr__(self) -> str:
        """String representation"""
        return (
            f"MemoryStorage(nodes={self.count_nodes()}, "
            f"edges={self.count_edges()}, "
            f"timeseries_entities={len(self.timeseries)})"
        )


def create_memory_storage() -> MemoryStorage:
    """Factory function to create MemoryStorage"""
    return MemoryStorage()
