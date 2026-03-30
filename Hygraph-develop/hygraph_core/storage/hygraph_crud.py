from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import json

from hygraph_core.utils.logging_config import setup_logging
from hygraph_core.model import PGNode, PGEdge, TimeSeries

from .age import AGEStore
from .timescale import TSStore

logger = setup_logging(log_file="logs/hygraph_crud.log")


class HybridCRUD:
    """
    Hybrid CRUD operations that combine AGE (graph) + TimescaleDB (time series).

    This class provides operations that need BOTH:
    - Node/edges structure from AGE
    - Time series data from TimescaleDB
    """

    def __init__(self, age: AGEStore, timescale: TSStore):
        """
        Initialize hybrid CRUD.

        Args:
            age: AGEStore instance
            timescale: TSStore instance
        """
        self.age = age
        self.timescale = timescale

    # =========================================================================
    # NODE CRUD (with time series)
    # =========================================================================

    def create_node_with_timeseries(
            self,
            uid: str,
            label: str,
            static_properties: Dict[str, Any],
            timeseries_properties: Dict[str, List[Tuple[datetime, float]]],
            start_time: str,
            end_time: str
    ) -> None:
        """
        Create a node with both static properties AND time series.

        Steps:
        1. Insert measurements to TimescaleDB
        2. Store node in AGE with time series IDs in properties

        Args:
            uid: Node unique ID
            label: Node label
            static_properties: Static properties (e.g., {'capacity': 50})
            timeseries_properties: Time series data (e.g., {'bikes': [(ts, val), ...]})
            start_time: Validity start time (ISO format)
            end_time: Validity end time (ISO format)

        Example:
            crud.create_node_with_timeseries(
                uid='station_1',
                label='Station',
                static_properties={'capacity': 50, 'region': 'Manhattan'},
                timeseries_properties={
                    'num_bikes_available': [(ts1, val1), (ts2, val2), ...],
                    'num_trips': [(ts1, val1), (ts2, val2), ...]
                },
                start_time='2024-01-01',
                end_time='2100-12-31'
            )
        """
        # 1. Insert time series measurements to TimescaleDB
        temporal_properties = {}

        for var_name, measurements in timeseries_properties.items():
            # Convert to measurement rows
            rows = [
                (uid, var_name, ts, val)
                for ts, val in measurements
            ]

            # Insert to TimescaleDB
            self.timescale.insert_measurements(rows)

            # Store time series ID (just the variable name for now)
            temporal_properties[var_name] = f"ts_{var_name}_{uid}"

        # 2. Create node in AGE with time series IDs
        all_properties = {**static_properties}
        if temporal_properties:
            all_properties['temporal_properties'] = temporal_properties

        self.age.create_node(
            uid=uid,
            label=label,
            properties=all_properties,
            start_time=start_time,
            end_time=end_time
        )

    def get_node_with_timeseries(
            self,
            uid: str,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None
    ) -> Optional[PGNode]:
        """
        Get node from AGE WITH its time series data from TimescaleDB.

        Returns:
            Dict with structure:
            {
                'uid': 'station_1',
                'label': 'Station',
                'start_time': '2024-01-01',
                'end_time': '2100-12-31',
                'properties': {
                    'capacity': 50,
                    'region': 'Manhattan',
                    'temporal_properties': {'bikes': 'ts_bikes_station_1'}
                },
                'timeseries': {
                    'bikes': [(ts1, val1), (ts2, val2), ...]
                }
            }
        """
        # 1. Get node from AGE (static properties only)
        node = self.age.get_node(uid)
        if not node:
            return None

        # 2. Parse properties (AGE stores as JSON string)
        properties = node['properties']

        # 3. Get time series IDs from properties
        temporal_properties = properties.get('temporal_properties', {})

        # 4. Query actual time series data from TimescaleDB using ts_id
        timeseries = {}
        for var_name, ts_id in temporal_properties.items():

            measurements = self.timescale.get_measurements(
                entity_uid=ts_id,
                variable=var_name,
                start_time=start_time,
                end_time=end_time
            )
            timeseries[var_name] = measurements

        # 5. Create PGNode object
        from datetime import datetime as dt

        # Parse dates
        start_dt = dt.fromisoformat(node['start_time']) if isinstance(node['start_time'], str) else node['start_time']
        end_dt = dt.fromisoformat(node['end_time']) if isinstance(node['end_time'], str) else node['end_time']

        # Create node
        pg_node = PGNode(
            oid=node['uid'],
            label=node['label'],
            start_time=start_dt,
            end_time=end_dt
        )

        # Add static properties (skip temporal_properties)
        for key, value in properties.items():
            if key != 'temporal_properties':
                pg_node.add_static_property(key, value)

        # Add temporal properties (time series IDs)
        for var_name, ts_id in temporal_properties.items():
            pg_node.add_temporal_property(var_name, ts_id)

        # Convert timeseries data to TimeSeries objects and cache them
        for var_name, ts_obj in timeseries.items():
            if ts_obj:  # ts_obj is already a TimeSeries from get_measurements
                pg_node.set_temporal_property_data(var_name, ts_obj)

        return pg_node

    def update_node_timeseries(
            self,
            uid: str,
            variable: str,
            new_measurements: List[Tuple[datetime, float]]
    ) -> None:
        """
        Update/add time series data for a node.

        Args:
            uid: Node ID
            variable: Variable name
            new_measurements: List of (timestamp, value) tuples

        Example:
            crud.update_node_timeseries(
                uid='station_1',
                variable='num_bikes_available',
                new_measurements=[
                    (datetime(2024, 1, 1, 10, 0), 15.0),
                    (datetime(2024, 1, 1, 11, 0), 12.0),
                ]
            )
        """
        # Insert new measurements to TimescaleDB
        rows = [(uid, variable, ts, val) for ts, val in new_measurements]
        self.timescale.insert_measurements(rows)

        # Update node properties in AGE to include this variable if not already present
        node = self.age.get_node(uid)
        if node:
            properties = node.get('properties', {})
            if isinstance(properties, str):
                properties = json.loads(properties)

            temporal_properties = properties.get('temporal_properties', {})
            if variable not in temporal_properties:
                temporal_properties[variable] = f"ts_{variable}_{uid}"
                properties['temporal_properties'] = temporal_properties

                self.age.update_node(uid, properties)

    def delete_node_with_timeseries(self, uid: str, hard: bool = False) -> None:
        """
        Delete node AND its time series data.

        Args:
            uid: Node ID
            hard: If True, hard delete. If False, soft delete (set end_time)

        Example:
            # Soft delete (set end_time to now)
            crud.delete_node_with_timeseries('station_1', hard=False)

            # Hard delete (remove completely)
            crud.delete_node_with_timeseries('station_1', hard=True)
        """
        # Delete node from AGE
        self.age.delete_node(uid, hard=hard)

        # If hard delete, also delete time series measurements
        if hard:
            self.timescale.delete_measurements(entity_uid=uid)

    # =========================================================================
    # EDGE CRUD (with time series)
    # =========================================================================

    def create_edge_with_timeseries(
            self,
            uid: str,
            src_uid: str,
            dst_uid: str,
            label: str,
            static_properties: Dict[str, Any],
            timeseries_properties: Dict[str, List[Tuple[datetime, float]]],
            start_time: str,
            end_time: str
    ) -> None:
        """
        Create an edges with both static properties AND time series.

        Example:
            crud.create_edge_with_timeseries(
                uid='trip_1',
                src_uid='station_1',
                dst_uid='station_2',
                label='trip',
                static_properties={'distance': 2.5},
                timeseries_properties={
                    'similarity': [(ts1, val1), (ts2, val2), ...]
                },
                start_time='2024-01-01',
                end_time='2100-12-31'
            )
        """
        # 1. Insert time series measurements to TimescaleDB
        temporal_properties = {}

        for var_name, measurements in timeseries_properties.items():
            rows = [(uid, var_name, ts, val) for ts, val in measurements]
            self.timescale.insert_measurements(rows)
            temporal_properties[var_name] = f"ts_{var_name}_{uid}"

        # 2. Create edges in AGE
        all_properties = {**static_properties}
        if temporal_properties:
            all_properties['temporal_properties'] = temporal_properties

        self.age.create_edge(
            uid=uid,
            src_uid=src_uid,
            dst_uid=dst_uid,
            label=label,
            properties=all_properties,
            start_time=start_time,
            end_time=end_time
        )

    def get_edge_with_timeseries(
            self,
            uid: str,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None
    ) -> Optional[PGEdge]:
        """
        Get edges from AGE WITH its time series data from TimescaleDB.

        Returns:
            PGEdge object with time series data loaded into cache
        """
        # 1. Get edges from AGE
        edge = self.age.get_edge(uid)
        if not edge:
            return None

        # 2. Parse properties
        properties = edge['properties']

        # 3. Get time series data using ts_id (NOT edge uid!)
        temporal_properties = properties.get('temporal_properties', {})
        timeseries = {}

        for var_name, ts_id in temporal_properties.items():
            measurements = self.timescale.get_measurements(
                entity_uid=ts_id,
                variable=var_name,
                start_time=start_time,
                end_time=end_time
            )
            timeseries[var_name] = measurements

        # 4. Create PGEdge object
        from datetime import datetime as dt

        # Parse dates
        start_dt = dt.fromisoformat(edge['start_time']) if isinstance(edge['start_time'], str) else edge['start_time']
        end_dt = dt.fromisoformat(edge['end_time']) if isinstance(edge['end_time'], str) else edge['end_time']

        # Create edge
        pg_edge = PGEdge(
            oid=edge['uid'],
            source=edge['src_uid'],
            target=edge['dst_uid'],
            label=edge['label'],
            start_time=start_dt,
            end_time=end_dt
        )

        # Add static properties
        for key, value in properties.items():
            if key != 'temporal_properties':
                pg_edge.add_static_property(key, value)

        # Add temporal properties (time series IDs)
        for var_name, ts_id in temporal_properties.items():
            pg_edge.add_temporal_property(var_name, ts_id)

        # Convert timeseries data to TimeSeries objects and cache them
        for var_name, ts_obj in timeseries.items():
            if ts_obj:  # ts_obj is already a TimeSeries from get_measurements
                pg_edge.set_temporal_property_data(var_name, ts_obj)

        return pg_edge

    def delete_edge_with_timeseries(self, uid: str, hard: bool = False) -> None:
        """
        Delete edges AND its time series data.
        """
        self.age.delete_edge(uid, hard=hard)

        if hard:
            self.timescale.delete_measurements(entity_uid=uid)

    # =========================================================================
    # QUERY OPERATIONS (combining graph + time series)
    # =========================================================================

    def query_nodes_with_timeseries(
            self,
            label: Optional[str] = None,
            at_time: Optional[str] = None,
            limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query nodes from AGE and include their time series data.

        Returns:
            List of nodes with timeseries (same structure as get_node_with_timeseries)

        Example:
            nodes = crud.query_nodes_with_timeseries(label='Station', limit=10)
            for node in nodes:
                print(node['uid'], node['properties'], node['timeseries'])
        """
        # 1. Query nodes from AGE
        nodes = self.age.query_nodes(label=label, at_time=at_time, limit=limit)

        # 2. For each node, get time series data
        results = []
        for node in nodes:
            # BUG FIX: uid is at top level, not in properties!
            node_with_ts = self.get_node_with_timeseries(node.get('uid'))
            if node_with_ts:
                results.append(node_with_ts)

        return results

    def query_nodes_by_timeseries_filter(
            self,
            label: Optional[str],
            variable: str,
            filter_func: Any,  # Function that takes TimeSeries and returns bool
            at_time: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Query nodes filtered by time series constraints.

        Args:
            label: Node label
            variable: Time series variable name
            filter_func: Function that takes TimeSeries object and returns True/False
            at_time: Temporal constraint

        Example:
            # Find stations where average bikes < 10
            def low_bikes(ts_obj):
                if not ts_obj:
                    return False
                return ts_obj.mean() < 10

            nodes = crud.query_nodes_by_timeseries_filter(
                label='Station',
                variable='num_bikes_available',
                filter_func=low_bikes
            )
        """
        # 1. Query all nodes
        nodes = self.age.query_nodes(label=label, at_time=at_time)

        # 2. Filter by time series
        filtered = []
        for node in nodes:
            uid = node.get('uid')
            
            # Get ts_id for this variable from node properties
            properties = node.get('properties', {})
            temporal_properties = properties.get('temporal_properties', {})
            ts_id = temporal_properties.get(variable)
            
            if not ts_id:
                continue  # Node doesn't have this temporal property
            
            # get_measurements now returns TimeSeries object
            ts_obj = self.timescale.get_measurements(
                entity_uid=ts_id,
                variable=variable
            )

            # Apply filter (now passes TimeSeries object)
            if filter_func(ts_obj):
                node_with_ts = self.get_node_with_timeseries(uid)
                if node_with_ts:
                    filtered.append(node_with_ts)

        return filtered

    # =========================================================================
    # STATISTICS (combining graph + time series)
    # =========================================================================

    def get_node_statistics(self, uid: str, variable: str) -> Dict[str, float]:
        """
        Get statistics for a node's time series.
        Returns  mean, min, max, std, count of the timeseries variable
        Example:
            stats = crud.get_node_statistics('station_1', 'num_bikes_available')
            print(stats['mean'])  # Average bikes available
        """
        # First get the ts_id for this variable
        node = self.age.get_node(uid)
        if not node:
            return {}
        
        properties = node.get('properties', {})
        temporal_properties = properties.get('temporal_properties', {})
        ts_id = temporal_properties.get(variable)
        
        if not ts_id:
            return {}
        
        # Query with ts_id - now returns TimeSeries object
        ts_obj = self.timescale.get_measurements(
            entity_uid=ts_id,
            variable=variable
        )

        if not ts_obj:
            return {}

        # Use TimeSeries methods for statistics
        import numpy as np
        return {
            'mean': ts_obj.mean(),
            'min': ts_obj.min(),
            'max': ts_obj.max(),
            'std': ts_obj.std(),
            'count': ts_obj.length
        }

    def get_timeseries_range(self, uid: str, variable: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Get time range for a time series.

        Returns Tuple of (min_timestamp, max_timestamp)
        """

        node = self.age.get_node(uid)
        if not node:
            return (None, None)
        
        properties = node.get('properties', {})
        temporal_properties = properties.get('temporal_properties', {})
        ts_id = temporal_properties.get(variable)
        
        if not ts_id:
            return (None, None)
        
        return self.timescale.get_time_range(ts_id, variable)


def create_hybrid_crud(age: AGEStore, timescale: TSStore) -> HybridCRUD:
    """
    Factory function to create HybridCRUD.
    Returns HybridCRUD instance
    """
    return HybridCRUD(age, timescale)