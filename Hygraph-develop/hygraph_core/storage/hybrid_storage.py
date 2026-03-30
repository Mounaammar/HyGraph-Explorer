"""
HyGraph Storage - Hybrid Backend (Orchestrator)

ONLY orchestrates between:
- Persistence: AGEStore (graph) + TSStore (time series)
- Memory: MemoryStorage (NetworkX + Xarray)

Does NOT implement CRUD - delegates to AGEStore, TSStore, and MemoryStorage.

Workflow:
1. load_to_memory() - Load from AGE + TimescaleDB → NetworkX + Xarray
2. get_memory_backend() - Get in-memory backends for operators
3. flush_to_database() - Write changes from memory → AGE + TimescaleDB
4. clear_memory() - Free RAM
"""

from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple, Callable, Set, Union
import networkx as nx
import xarray as xr

from .base import StorageBackend
from .age import AGEStore
from .timescale import TSStore
from .memory import MemoryStorage
from .sql import DBPool

# Import from model
try:
    from hygraph_core.model.nodes import PGNode, TSNode
    from hygraph_core.model.edges import PGEdge, TSEdge
except ImportError:
    from typing import Any as PGNode, TSNode, PGEdge, TSEdge


class HybridStorage:
    """
    Hybrid storage orchestrator.
    
    Orchestrates between:
    - AGEStore: Graph persistence (static properties)
    - TSStore: Time series persistence (measurements)
    - MemoryStorage: In-memory (NetworkX + Xarray)
    
    Example:
        storage = HybridStorage(db_pool)
        
        # Load from persistence to memory
        storage.load_to_memory()
        
        # Get backends for computation
        nx_graph, ts_data = storage.get_memory_backend()
        
        # Compute...
        
        # Flush back to persistence
        storage.flush_to_database(modified_nodes={...})
        
        # Clear memory
        storage.clear_memory()
    """
    
    def __init__(self, db: Union[str, DBPool], graph_name: str = "hygraph"):
        """
        Initialize hybrid storage.
        
        Args:
            db: Either PostgreSQL connection string OR DBPool object
            graph_name: Name of AGE graph
        """
        # Create or use DBPool
        if isinstance(db, str):
            self.db = DBPool(db)
            self._owns_pool = True
        else:
            self.db = db
            self._owns_pool = False
        
        # Persistence backends
        self.age = AGEStore(self.db, graph=graph_name)
        self.timescale = TSStore(self.db)
        
        # Memory backend
        self.memory = MemoryStorage()
        
        # State
        self._memory_loaded = False
        self._connected = True
    

    
    def connect(self) -> None:
        """Connection managed by DBPool"""
        self._connected = True
    
    def disconnect(self) -> None:
        """Close database pool if we own it"""
        if self._owns_pool:
            self.db.close()
        self._connected = False
        self.clear_memory()
    
    def is_connected(self) -> bool:
        """Check if connected"""
        return self._connected
    

    
    def load_to_memory(
        self,
        node_filter: Optional[Callable] = None,
        edge_filter: Optional[Callable] = None,
        at_time: Optional[datetime] = None,
        between: Optional[Tuple[datetime, datetime]] = None,
        limit: Optional[int] = None
    ) -> None:
        """
        Load data from AGE + TimescaleDB into NetworkX + Xarray.
        
        Steps:
        1. Query nodes from AGE
        2. Query edges from AGE
        3. Build NetworkX graph
        4. Query measurements from TimescaleDB
        5. Build Xarray DataArrays
        
        Args:
            node_filter: Filter for nodes
            edge_filter: Filter for edges
            at_time: Load snapshot at time
            between: Load data in time range
            limit: Max entities to load
        """
        print(f"[HybridStorage] Loading data from persistence to memory...")
        
        # 1. Query nodes from AGE
        at_time_str = at_time.isoformat() if at_time else None
        nodes = self.age.query_nodes(
            at_time=at_time_str,
            limit=limit
        )
        print(f"[HybridStorage] Loaded {len(nodes)} nodes from AGE")
        
        # 2. Query edges from AGE
        edges = self.age.query_edges(
            at_time=at_time_str,
            limit=limit
        )
        print(f"[HybridStorage] Loaded {len(edges)} edges from AGE")
        
        # 3. Build NetworkX graph
        for node in nodes:
            # Parse AGE node data and add to memory
            self.memory.graph.add_node(
                node.get('uid'),
                **node
            )
        
        for edge in edges:
            # Parse AGE edges data and add to memory
            self.memory.graph.add_edge(
                edge.get('src_uid'),
                edge.get('dst_uid'),
                key=edge.get('uid'),
                **edge
            )
        
        print(f"[HybridStorage] Built NetworkX graph")
        
        # 4. Load time series from TimescaleDB
        self._load_timeseries_to_memory(nodes, edges, between)
        
        self._memory_loaded = True
        print(f"[HybridStorage] Memory loading complete")
    
    def _load_timeseries_to_memory(
        self,
        nodes: List[Dict],
        edges: List[Dict],
        time_range: Optional[Tuple[datetime, datetime]] = None
    ) -> None:
        """
        Load time series from TimescaleDB into Xarray.
        
        Args:
            nodes: Nodes from AGE
            edges: Edges from AGE
            time_range: Time range to load
        """
        start_time, end_time = time_range if time_range else (None, None)
        
        # Load time series for nodes
        for node in nodes:
            node_uid = node.get('uid')
            properties = node.get('properties', {})
            
            # Check if node has temporal properties
            temporal_props = properties.get('temporal_properties', {})
            if not temporal_props:
                continue
            
            # Query measurements for each temporal property
            for var_name in temporal_props.keys():
                measurements = self.timescale.get_measurements(
                    entity_uid=node_uid,
                    variable=var_name,
                    start_time=start_time,
                    end_time=end_time
                )
                
                if measurements:
                    timestamps = [m[0] for m in measurements]
                    values = [m[1] for m in measurements]
                    
                    # Store in Xarray
                    self.memory.insert_timeseries(
                        entity_uid=node_uid,
                        variable=var_name,
                        timestamps=timestamps,
                        values=values
                    )
        
        # Load time series for edges (same process)
        for edge in edges:
            edge_uid = edge.get('uid')
            properties = edge.get('properties', {})
            
            temporal_props = properties.get('temporal_properties', {})
            if not temporal_props:
                continue
            
            for var_name in temporal_props.keys():
                measurements = self.timescale.get_measurements(
                    entity_uid=edge_uid,
                    variable=var_name,
                    start_time=start_time,
                    end_time=end_time
                )
                
                if measurements:
                    timestamps = [m[0] for m in measurements]
                    values = [m[1] for m in measurements]
                    
                    self.memory.insert_timeseries(
                        entity_uid=edge_uid,
                        variable=var_name,
                        timestamps=timestamps,
                        values=values
                    )
        
        print(f"[HybridStorage] Loaded time series to Xarray")
    
    def get_memory_backend(self) -> Tuple[nx.MultiDiGraph, Dict[str, Dict[str, xr.DataArray]]]:
        """
        Get in-memory backends for operator execution.
        
        Returns:
            Tuple of (NetworkX graph, Xarray timeseries dict)
        
        Raises:
            RuntimeError: If memory not loaded
        
        Example:
            nx_graph, ts_data = storage.get_memory_backend()
            
            # Use NetworkX
            pagerank = nx.pagerank(nx_graph)
            
            # Use Xarray
            ts = ts_data['station_1']['bikes']
            mean = ts.mean()
        """
        if not self._memory_loaded:
            raise RuntimeError("Memory not loaded. Call load_to_memory() first.")
        
        return self.memory.graph, self.memory.timeseries
    

    
    def flush_to_database(
        self,
        modified_nodes: Optional[Set[str]] = None,
        modified_edges: Optional[Set[str]] = None,
        new_measurements: Optional[List[Tuple]] = None
    ) -> None:
        """
        Flush changes from memory back to AGE + TimescaleDB.
        
        Args:
            modified_nodes: Set of node IDs that were modified
            modified_edges: Set of edges IDs that were modified
            new_measurements: List of (entity_uid, variable, ts, value) tuples
        
        Example:
            # After modifying nodes in memory
            storage.flush_to_database(
                modified_nodes={'station_1', 'station_2'},
                modified_edges=set(),
                new_measurements=[
                    ('station_1', 'pagerank', datetime.now(), 0.85)
                ]
            )
        """
        if not self._memory_loaded:
            print("[HybridStorage] No memory loaded, nothing to flush")
            return
        
        print(f"[HybridStorage] Flushing changes to persistence...")
        
        # 1. Flush modified nodes to AGE
        if modified_nodes:
            for node_id in modified_nodes:
                if node_id in self.memory.graph.nodes:
                    node_data = self.memory.graph.nodes[node_id]
                    properties = node_data.get('properties', {})
                    
                    # Update in AGE
                    self.age.update_node(node_id, properties)
            
            print(f"[HybridStorage] Flushed {len(modified_nodes)} nodes to AGE")
        
        # 2. Flush modified edges to AGE
        if modified_edges:
            for edge_id in modified_edges:
                # Find edges in graph
                for u, v, key, data in self.memory.graph.edges(keys=True, data=True):
                    if data.get('uid') == edge_id or key == edge_id:
                        properties = data.get('properties', {})
                        
                        # Update in AGE
                        self.age.update_edge(edge_id, properties)
                        break
            
            print(f"[HybridStorage] Flushed {len(modified_edges)} edges to AGE")
        
        # 3. Flush new measurements to TimescaleDB
        if new_measurements:
            self.timescale.insert_measurements(new_measurements)
            print(f"[HybridStorage] Flushed {len(new_measurements)} measurements to TimescaleDB")
        
        print("[HybridStorage] Flush complete")
    
    def clear_memory(self) -> None:
        """Clear in-memory data to free RAM"""
        print("[HybridStorage] Clearing memory...")
        
        self.memory = MemoryStorage()
        self._memory_loaded = False
        
        print("[HybridStorage] Memory cleared")
    

    
    def stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        return {
            'backend': 'HybridStorage',
            'nodes': self.age.count_nodes(),
            'edges': self.age.count_edges(),
            'timeseries_entities': len(self.memory.timeseries) if self._memory_loaded else 0,
            'memory_loaded': self._memory_loaded,
            'connected': self._connected
        }
    
    def __repr__(self) -> str:
        """String representation"""
        stats = self.stats()
        return (
            f"HybridStorage("
            f"nodes={stats['nodes']}, "
            f"edges={stats['edges']}, "
            f"memory_loaded={stats['memory_loaded']})"
        )


def create_hybrid_storage(
    db: Union[str, DBPool],
    graph_name: str = "hygraph"
) -> HybridStorage:
    """
    Factory function to create HybridStorage.
    
    Args:
        db: Either PostgreSQL connection string OR DBPool object
        graph_name: Name of AGE graph
    
    Returns:
        HybridStorage instance
    """
    return HybridStorage(db, graph_name)
