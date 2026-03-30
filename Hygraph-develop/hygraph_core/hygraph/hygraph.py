"""
HyGraph - Main Class

This is the class users interact with.
It's a thin coordination layer that provides:
1. Storage backends (AGE, TimescaleDB)
2. Fluent API for CRUD operations
3. Operators support via HybridStorage
4. Graph-level properties (static and temporal)
"""
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Set, Union, Tuple, Any

from hygraph_core.ingest.csv_converter import json_to_csv
from hygraph_core.ingest.csv_loader import load_csv
from hygraph_core.model.graph_properties import HyGraphProperties
from hygraph_core.operators.hygraphDiff import HyGraphDiff
from hygraph_core.operators.temporal_snapshot import Snapshot
from hygraph_core.operators.snapshotSequence import SnapshotSequence
from hygraph_core.storage.filtered_stored import FilteredAGEStore, FilteredTimescaleStore, DiffAnnotatedAGEStore
from hygraph_core.storage.sql import DBPool
from hygraph_core.storage.age import AGEStore
from hygraph_core.storage.timescale import TSStore
from hygraph_core.storage.hygraph_crud import HybridCRUD
from hygraph_core.storage.hybrid_storage import HybridStorage
from hygraph_core.model.timeseries import TimeSeries, TimeSeriesMetadata
from hygraph_core import SETTINGS
# Type alias for storage backends (original or filtered)
AGEStoreType = Union[AGEStore, FilteredAGEStore, DiffAnnotatedAGEStore]
TSStoreType = Union[TSStore, FilteredTimescaleStore]

# Import fluent API builders (users never import these directly)
from hygraph_core.storage.fluent_api import (
    NodeCreator,
    EdgeCreator,
    QueryBuilder,
    EntityUpdater,
    EntityDeleter
)


class HyGraph:
    """
    Main HyGraph class.
    
    Architecture:
        User → HyGraph → [AGEStore, TSStore, HybridCRUD, HybridStorage] → PostgreSQL
    
    Properties:
        HyGraph supports both static and temporal properties at the graph level.
        
        # Static properties
        hg.properties.add_static_property("domain", "transportation")
        hg.properties.add_static_property("region", "NYC")
        
        # Temporal properties (computed metrics as time series)
        hg.properties.add_temporal_property("node_count", "ts_node_count_001")
    """
    
    # Type hints for instance attributes
    db: DBPool
    age: AGEStoreType
    timescale: TSStoreType
    crud: HybridCRUD
    hybrid: Optional[HybridStorage]
    graph_name: str
    properties: HyGraphProperties

    # Set only on HyGraph instances returned by diff_from_induced()
    diff_summary: Optional[Any]   # DiffSummary dataclass
    cpg: Optional[Any]            # ChangePropagationGraph or None (on diff HyGraph)
    diff_result: Optional[Any]    # Full HyGraphDiffResult

    def __init__(self, connection_string: str, graph_name: str = "hygraph"):
        """
        Initialize HyGraph.

        Args:
            connection_string: PostgreSQL connection string
            graph_name: Apache AGE graph name
        """
        # Database pool
        self.db = DBPool(SETTINGS.dsn, SETTINGS.pool_min, SETTINGS.pool_max)

        # Storage backends
        self.age = AGEStore(self.db, graph=graph_name)
        self.timescale = TSStore(self.db)

        # Hybrid operations (combines age + timescale)
        self.crud = HybridCRUD(self.age, self.timescale)
        self.graph_name = graph_name
        
        # For operators (persistence ↔ memory)
        self.hybrid = HybridStorage(self.db, graph_name=graph_name)
        
        # Graph-level properties
        self.properties = HyGraphProperties(
            name=graph_name,
            is_subgraph=False
        )

        # Diff metadata — None on regular HyGraph, populated by diff_from_induced()
        self.diff_summary = None
        self.cpg = None
        self.diff_result = None

    # =========================================================================
    # INGESTION Operations
    # =========================================================================

    def ingest_from_json(
            self,
            json_dir: Path,
            node_field_map: Dict[str, str],
            edge_field_map: Dict[str, str],
            output_dir: Optional[Path] = None,
            skip_age: bool = False,
            batch_size: int = 5000
    ) -> Dict[str, int]:
        """
        Ingest data from JSON files using YOUR ingestion package.

        This method:
        1. Converts JSON to CSV (using csv_converter.py)
        2. Loads CSV into database (using csv_loader.py)

        Args:
            json_dir: Directory containing JSON files
            node_field_map: Field mapping for nodes
            edge_field_map: Field mapping for edges
            output_dir: Optional outputs directory for CSV (default: temp dir)
            skip_age: If True, skip AGE loading (only time series)
            batch_size: Batch size for loading

        """
        import tempfile

        # Use temp directory if no outputs directory specified
        if output_dir is None:
            output_dir = Path(tempfile.mkdtemp(prefix="hygraph_csv_"))

        # Step 1: Convert JSON to CSV (YOUR function)
        print(f"\n📝 Converting JSON to CSV...")
        json_to_csv(
            json_dir=json_dir,
            output_dir=output_dir,
            node_field_map=node_field_map,
            edge_field_map=edge_field_map
        )

        # Step 2: Load CSV into database (YOUR function)
        print(f"\n💾 Loading CSV into database...")
        stats = load_csv(
            csv_dir=output_dir,
            graph_name=self.graph_name,
            skip_age=skip_age,
            batch_size=batch_size
        )
        
        # Update properties
        self.properties.touch()

        return stats

    def ingest_from_csv(
            self,
            csv_dir: Path,
            skip_age: bool = False,
            batch_size: int = 5000
    ) -> Dict[str, int]:
        """
        Ingest data from CSV files.
        """
        print(f"\n  Loading CSV into database...")
        stats = load_csv(
            csv_dir=csv_dir,
            graph_name=self.graph_name,
            skip_age=skip_age,
            batch_size=batch_size
        )
        
        # Update properties
        self.properties.touch()

        return stats

    # =========================================================================
    # FLUENT API - CRUD Operations (Database)
    # =========================================================================

    def create_node(self, oid: Optional[str] = None) -> NodeCreator:
        """
        Create a new node (goes to DATABASE).

        Example:
            hg.create_node('s1')\\
                .with_label('Station')\\
                .with_property('capacity', 50)\\
                .with_ts_property('bikes', ts_data)\\
                .create()
        """
        return NodeCreator(self, oid)

    def create_edge(self, source: str, target: str, oid: Optional[str] = None) -> EdgeCreator:
        """
        Create a new edges (goes to DATABASE).

        Example:
            hg.create_edge('s1', 's2')\\
                .with_label('trip')\\
                .with_property('distance', 2.5)\\
                .create()
        """
        return EdgeCreator(self, source, target, oid)

    def create_timeseries(self, timeseires:TimeSeries):

        return self.timescale.add_timeseries(timeseires)


    def query(self) -> QueryBuilder:
        """
        Start a query (queries DATABASE).

        Example:
            results = hg.query()\\
                .nodes(label='Station')\\
                .where(lambda n: n['capacity'] > 50)\\
                .execute()
        """
        return QueryBuilder(self)

    def update_node(self, node_id: str) -> EntityUpdater:
        """
        Update a node (updates DATABASE).

        Example:
            hg.update_node('s1')\\
                .set_property('capacity', 60)\\
                .execute()
        """
        return EntityUpdater(self, 'node', node_id)

    def update_edge(self, edge_id: str) -> EntityUpdater:
        """
        Update an edges (updates DATABASE).
        """
        return EntityUpdater(self, 'edges', edge_id)

    def delete_node(self, node_id: str) -> EntityDeleter:
        """
        Delete a node (deletes from DATABASE).

        Example:
            # Soft delete
            hg.delete_node('s1').execute()

            # Hard delete
            hg.delete_node('s1').hard().execute()
        """
        return EntityDeleter(self, 'node', node_id)

    def delete_edge(self, edge_id: str) -> EntityDeleter:
        """Delete an edges (deletes from DATABASE)."""
        return EntityDeleter(self, 'edges', edge_id)

    # =========================================================================
    # ADVANCED QUERIES (Database) - THROUGH FLUENT API
    # =========================================================================
    def count_nodes(self, label: Optional[str] = None) -> int:
        """Count nodes (in DATABASE)."""
        return self.age.count_nodes(label)

    def count_edges(self, label: Optional[str] = None) -> int:
        """Count edges (in DATABASE)."""
        return self.age.count_edges(label)

    # =========================================================================
    # OPERATORS (System loads to memory automatically)
    # =========================================================================

    def shortest_path(self, source: str, target: str):
        """
        Find shortest path (system loads to memory automatically).

        Example:
            path = hg.shortest_path('s1', 's2')
        """
        # Load to memory if not already loaded
        if not self.hybrid._memory_loaded:
            self.hybrid.load_to_memory()

        # Use in-memory graph
        nx_graph, _ = self.hybrid.get_memory_backend()
        import networkx as nx
        return nx.shortest_path(nx_graph, source, target)

    def snapshot(
        self, 
        when, 
        mode: str = "hybrid",
        ts_handling: Optional[str] = None,
        aggregation_fn: str = "mean"
    ) -> Snapshot:
        """
        Create a snapshot of the graph at a specific time or interval.
        
        Args:
            when: Timestamp string OR tuple (start, end) for interval
            mode: "graph" or "hybrid"
            ts_handling: For intervals with hybrid mode - "aggregate" or "slice"
            aggregation_fn: Aggregation function for "aggregate" mode
        
        Returns:
            Snapshot object
        
        Example:
            # Point snapshot
            snap = hg.snapshot("2024-05-01T10:00:00", mode="hybrid")
            
            # Interval snapshot with aggregation
            snap = hg.snapshot(
                when=("2024-05-01", "2024-05-02"),
                mode="hybrid",
                ts_handling="aggregate",
                aggregation_fn="mean"
            )
        """
        return Snapshot(
            self,
            when, 
            mode,
            ts_handling=ts_handling,
            aggregation_fn=aggregation_fn
        )

    def snapshot_sequence(
        self,
        start: str,
        end: str,
        every: timedelta,
        mode: str = "hybrid"
    ) -> SnapshotSequence:
        """
        Create a sequence of snapshots for temporal analysis.
        
        This is the FACTORY METHOD that handles all the complexity of:
        - Generating timestamps based on interval
        - Creating snapshots at each timestamp
        - Assembling them into a SnapshotSequence
        
        Snapshots are discrete samples of graph state. TSGen computes
        metrics from these samples, producing discrete time series.
        
        Args:
            start: Start timestamp (ISO format)
            end: End timestamp (ISO format)
            every: Time interval between snapshots (timedelta)
            mode: "graph" or "hybrid" (default: "hybrid")
        
        Returns:
            SnapshotSequence ready for TSGen analysis
        
        Example:
            from datetime import timedelta
            
            snapshots = hg.snapshot_sequence(
                start="2024-05-01",
                end="2024-05-31",
                every=timedelta(days=1)
            )
            
            # Generate time series from snapshots
            ts = snapshots.tsgen()
            node_count = ts.global_.nodes.count(label="Station")
        """
        if not isinstance(every, timedelta):
            raise TypeError(f"every must be a timedelta, got: {type(every).__name__}")
        
        # Parse start/end times
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
        
        # Generate snapshots
        snapshots = []
        current = start_dt
        
        while current < end_dt:
            snap = Snapshot(
                self,
                when=current.isoformat(),
                mode=mode
            )
            snapshots.append(snap)
            current += every
        
        # Create and return SnapshotSequence
        return SnapshotSequence(
            snapshots=snapshots,
            every=every,
            start_time=start,
            end_time=end
        )

    @staticmethod
    def hygraph_diff(snap1: Snapshot, snap2: Snapshot):

        return HyGraphDiff(snap1, snap2)

    def subHygraph(
        self, 
        node_ids: Set[any],
        name: str,
        filter_query: Optional[str] = None
    ) -> 'HyGraph':
        """
        Create a subHygraph containing only specified nodes.
        Edges are automatically included if both endpoints are in node_ids.

        Args:
            node_ids: Set of node UIDs to include
            name: Name for the subHygraph
            filter_query: Optional human-readable filter description

        Returns:
            New HyGraph instance with filtered storage

        Example:
            # Get high-capacity stations
            nodes = hg.query().nodes("Station").where(
                lambda n: n.get_static_property("capacity") > 40
            ).execute()

            node_ids = {n.uid for n in nodes}

            # Create subHygraph
            high_cap_graph = hg.subHygraph(
                node_ids, 
                name="high_capacity",
                filter_query="capacity > 40"
            )
            
            # Access properties
            high_cap_graph.properties.is_subgraph  # True
            high_cap_graph.properties.filter_node_ids  # {...}
        """
        # Create filtered storage layers
        filtered_age = FilteredAGEStore(self.age, node_ids)
        filtered_timescale = FilteredTimescaleStore(self.timescale, filtered_age)

        # Create new HyGraph with filtered stores
        sub_hg = HyGraph.__new__(HyGraph)
        sub_hg.age = filtered_age
        sub_hg.timescale = filtered_timescale
        sub_hg.graph_name = name
        sub_hg.db = self.db
        sub_hg.crud = HybridCRUD(filtered_age, filtered_timescale)
        sub_hg.hybrid = None  # Not supported for subgraphs yet
        
        # Create properties for subgraph
        sub_hg.properties = HyGraphProperties(
            name=name,
            is_subgraph=True,
            filter_node_ids=node_ids,
            filter_query=filter_query
        )

        return sub_hg

    # =========================================================================
    # MEMORY MANAGEMENT (Advanced users)
    # =========================================================================

    def load_to_memory(self):
        """
        Explicitly load data to memory (for advanced users).

        Example:
            hg.load_to_memory()
            nx_graph, ts_data = hg.get_memory_backend()
            # Work with NetworkX directly
        """
        self.hybrid.load_to_memory()

    def get_memory_backend(self):
        """
        Get in-memory graph and time series (for advanced users).

        Returns:
            (nx_graph, ts_data)
        """
        return self.hybrid.get_memory_backend()

    def persist_to_db(self):
        """
        Persist in-memory changes back to database (for advanced users).
        """
        self.hybrid.flush_to_database()

    # =========================================================================
    # UTILITY
    # =========================================================================

    def close(self):
        """Close database connections"""
        self.db.close()

    def stats(self):
        """Get statistics"""
        return {
            'nodes': self.age.count_nodes(),
            'edges': self.age.count_edges(),
        }

    def debug_temporal_data(self) -> dict:
        """
        Get debug information about temporal data in the database.
        Useful for diagnosing snapshot issues.
        """
        # Query sample nodes without temporal filter
        sample_nodes = self.age.query_nodes(limit=5)
        
        # Query sample edges without temporal filter  
        sample_edges = self.age.query_edges(limit=5)
        
        # Get time range
        node_times = []
        for n in sample_nodes:
            if n.get('start_time'):
                node_times.append(('start', n['start_time']))
            if n.get('end_time'):
                node_times.append(('end', n['end_time']))
        
        return {
            'total_nodes': self.age.count_nodes(),
            'total_edges': self.age.count_edges(),
            'sample_nodes': [
                {
                    'uid': n.get('uid'),
                    'label': n.get('label'),
                    'start_time': n.get('start_time'),
                    'end_time': n.get('end_time'),
                    'properties_keys': list(n.get('properties', {}).keys()) if n.get('properties') else []
                }
                for n in sample_nodes
            ],
            'sample_edges': [
                {
                    'uid': e.get('uid'),
                    'label': e.get('label'),
                    'src_uid': e.get('src_uid'),
                    'dst_uid': e.get('dst_uid'),
                    'start_time': e.get('start_time'),
                    'end_time': e.get('end_time'),
                }
                for e in sample_edges
            ],
            'node_time_samples': node_times[:6] if node_times else 'No timestamps found'
        }

    # =========================================================================
    # VLDB DEMO OPERATORS
    # =========================================================================

    def predicate_induced(
        self,
        predicate: str,
        time_range: tuple,
        step_minutes: int = 5,
        node_label: str = "Sensor",
        name: str = None,
        time_filter: Optional[Tuple[int, int]] = None,
    ):
        """
        Create a predicate-induced SubHyGraph.
        Membership is determined by evaluating a time-series predicate
        at each timestep. Tracks component lifecycle events.

        The predicate can reference both time-series and static node
        properties. The first identifier is the TS property (looked up
        from TimescaleDB), others are static properties (from AGE).

        Args:
            predicate: e.g., "speed < 30" or "speed / free_flow_speed < 0.5"
            time_range: (start, end) as ISO strings
            step_minutes: Evaluation interval
            node_label: Label of nodes to evaluate
            name: Optional name for the SubHyGraph

        Returns:
            PredicateInducedSubHyGraph (call .evaluate() to run)

        Example:
            # Absolute threshold
            congestion = hg.predicate_induced(
                predicate="speed < 30",
                time_range=("2012-03-05 06:00", "2012-03-05 12:00"),
            ).evaluate()

            # Velocity ratio (Li et al., PNAS 2015)
            congestion = hg.predicate_induced(
                predicate="speed / free_flow_speed < 0.5",
                time_range=("2012-03-05 06:00", "2012-03-05 12:00"),
            ).evaluate()
        """
        from hygraph_core.operators.predicate_induced import PredicateInducedSubHyGraph
        return PredicateInducedSubHyGraph(
            hygraph=self,
            predicate=predicate,
            time_range=time_range,
            step_minutes=step_minutes,
            node_label=node_label,
            name=name,
            time_filter=time_filter,
        )

    def tsgen_from_induced(self, induced, metrics=None, signal_metrics=None):
        """
        Create a TSGen signal transformation on a predicate-induced SubHyGraph.
        Derives graph-level temporal signals by computing structural metrics
        at each moment where the subgraph membership changes.

        Args:
            induced: An evaluated PredicateInducedSubHyGraph
            metrics: List of metric names (default: density, component_count, largest_component)
            signal_metrics: Optional list of SignalAggregatingMetric objects

        Returns:
            TSGenFromInduced (call .compute() to run)

        Example:
            results = hg.tsgen_from_induced(congestion).compute()
            density_ts = results["density"]
        """
        from hygraph_core.operators.tsgen_induced import TSGenFromInduced
        return TSGenFromInduced(induced, metrics=metrics, signal_metrics=signal_metrics)

    def diff_from_induced(
        self,
        induced,
        induced_2=None,
        ts_property: str = None,
        delta_max_days: float = 30.0,
        period_1_label: str = "period_1",
        period_2_label: str = "period_2",
        min_frequency: float = 0.0,
        period_1: Optional[Tuple[str, str]] = None,
        period_2: Optional[Tuple[str, str]] = None,
    ) -> 'HyGraph':
        """
        Run HyGraphDiff on two predicate-induced SubHyGraphs.
        Returns a HyGraph (closure property) where each entity carries
        its change descriptor as regular queryable properties.

        The result supports the same query interface as any HyGraph:
            diff_hg.query().nodes().where(
                lambda n: n.get_static_property("delta_struct") == "ADDED"
            ).execute()

        Change annotations injected into node properties:
            delta_struct:    "ADDED" | "REMOVED" | "PERSISTED"
            nrmse:           float (signal divergence)
            delta_mu:        float (level shift)
            sigma_ratio:     float (variability change)
            transition_ts:   str (ISO timestamp, for ADDED nodes)

        Additional attributes on the returned HyGraph:
            .diff_summary:   DiffSummary dataclass
            .cpg:            ChangePropagationGraph (or None)

        Args:
            induced_1: First period's SubHyGraph (evaluated)
            induced_2: Second period's SubHyGraph (evaluated)
            ts_property: TS property to compare (auto-derived if None)
            delta_max_days: Max delay for CPG edges
            period_1_label: Label for first period
            period_2_label: Label for second period
            period_1: Optional (start, end) ISO strings — temporal slice
                      applied to induced_1. If None, uses induced_1's
                      own time range.
            period_2: Optional (start, end) ISO strings — temporal slice
                      applied to induced_2. If None, uses induced_2's
                      own time range.

        Returns:
            HyGraph instance with change annotations

        Example:
            # Same induced subgraph, different periods:
            diff_hg = hg.diff_from_induced(
                april_induced, april_induced,
                period_1=("2012-04-02 06:00", "2012-04-06 10:00"),
                period_2=("2012-04-07 06:00", "2012-04-08 10:00"),
                period_1_label="Weekday", period_2_label="Weekend",
            )

            # Or two separate induced subgraphs (original behavior):
            diff_hg = hg.diff_from_induced(march, june)

            # Access diff metadata
            print(diff_hg.diff_summary)
            print(diff_hg.cpg.roots)
        """
        from hygraph_core.operators.diff_result import HyGraphDiffResult

        # Step 1: Compute the diff
        if induced_2 is None:
            induced_2 = induced
        diff_result = HyGraphDiffResult.from_induced(
            induced_1=induced,
            induced_2=induced_2,
            hygraph=self,
            ts_property=ts_property,
            delta_max_days=delta_max_days,
            period_1_label=period_1_label,
            period_2_label=period_2_label,
            min_frequency=min_frequency,
            period_1=period_1,
            period_2=period_2,
        )

        # Step 2: Build per-node annotation dicts from the diff result
        annotations = {}
        for nid, entity in diff_result.nodes.items():
            ann = {"delta_struct": entity.change_descriptor.delta_struct}
            cd = entity.change_descriptor
            if cd.nrmse is not None:
                ann["nrmse"] = round(cd.nrmse, 4)
            if cd.delta_mu is not None:
                ann["delta_mu"] = round(cd.delta_mu, 4)
            if cd.sigma_ratio is not None:
                ann["sigma_ratio"] = round(cd.sigma_ratio, 4)
            if cd.delta_slope is not None:
                ann["delta_slope"] = round(cd.delta_slope, 4)
            if cd.transition_timestamp is not None:
                ann["transition_ts"] = cd.transition_timestamp.isoformat()
            annotations[str(nid)] = ann

        # Step 3: Create annotated HyGraph using the same pattern as subHygraph()
        all_node_ids = set(str(nid) for nid in diff_result.nodes.keys())
        diff_age = DiffAnnotatedAGEStore(self.age, all_node_ids, annotations)
        diff_timescale = FilteredTimescaleStore(self.timescale, diff_age)

        diff_hg = HyGraph.__new__(HyGraph)
        diff_hg.age = diff_age
        diff_hg.timescale = diff_timescale
        diff_hg.graph_name = f"diff_{period_1_label}_vs_{period_2_label}"
        diff_hg.db = self.db
        diff_hg.crud = HybridCRUD(diff_age, diff_timescale)
        diff_hg.hybrid = None

        diff_hg.properties = HyGraphProperties(
            name=diff_hg.graph_name,
            is_subgraph=True,
            filter_node_ids=all_node_ids,
            filter_query=(
                f"HyGraphDiff({period_1_label} vs {period_2_label}): "
                f"{diff_result.summary.added_count} added, "
                f"{diff_result.summary.removed_count} removed, "
                f"{diff_result.summary.persisted_count} persisted"
            ),
        )

        # Step 4: Attach diff metadata as extra attributes
        diff_hg.diff_summary = diff_result.summary
        diff_hg.cpg = diff_result.cpg
        diff_hg.diff_result = diff_result  # keep full result for advanced access

        return diff_hg

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __repr__(self):
        stats = self.stats()
        graph_type = "SubHyGraph" if self.properties.is_subgraph else "HyGraph"
        return f"{graph_type}({self.properties.name}, nodes={stats['nodes']}, edges={stats['edges']})"


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":

    print("=" * 80)
    print("HyGraph Quick Example")
    print("=" * 80)

    hg = HyGraph("postgresql://localhost:5432/hygraph")
    """
    schema = {
        "Station": {
            "capacity": "int",
            "lat": "float",
            "lon": "float",
            "region_id": "int"
        },

    }
    
    node_field_map = {
        "oid": "station_id",
        "start_time": "start",
        "end_time": "end",
    }
    edge_field_map = {
        "source_id": "from",
        "target_id": "to",
        "start_time": "start",
        "end_time": "end",
    }
    json_to_csv(
        json_dir=Path("../inputFiles/json"),
        output_dir=Path("../inputFiles/csv3"),
        node_field_map=node_field_map,
        edge_field_map=edge_field_map
    )

   
    stats = load_csv(
        csv_dir=Path("../inputFiles/csv3"),
        graph_name="hygraph",
        skip_age=False,
        schema=schema
    )
    print("Ingested:", stats) """

    # Create nodes
    #hg.create_node('s1').with_label('Station').with_property('capacity', 50).create()
    #hg.create_node('s2').with_label('Station').with_property('capacity', 70).create()
    
    # Create edge with time series
    """ now = datetime.now()
    timestamps = [now - timedelta(hours=i) for i in range(24)]
    data = [[float(i * 2)] for i in range(24)]

    ts_bikes = TimeSeries(
        tsid='ts_e1_available_bike',
        timestamps=timestamps,
        variables=['available_bike'],
        data=data,
        metadata=TimeSeriesMetadata(owner_id='E1', element_type='edge', units='bikes')
    )
    ts_id=hg.create_timeseries(ts_bikes)
    edge =hg.query().edge("Trip_2212_2212_2024-05-17T12:36:00").execute()
    edge.add_temporal_property(hg=hg,name="available_bike", ts_id=ts_id)
    print(ts_id)
    measurements = hg.timescale.get_measurements(
        entity_uid=ts_id,
        variable="available_bike"
    )
    print(measurements)
    
    hg.create_edge('s1', 's2', 'E1').with_label('Trip').with_ts_property("available_bike", ts_bikes).create()
    #result = hg.query().nodes("Station").where(lambda n: n.get_static_property('ca') >= 60).execute()
    #print(f"\nStations with capacity >= 50: {len(result), result}")
    edge =hg.query().edge("Trip_2212_2212_2024-05-17T12:36:00").execute()
    print("Available temporal properties:", edge.temporal_properties)
    print("Available static properties:", edge.static_properties)


    print(edge.get_temporal_property('num_rides'))
    print("Edge OID:", edge.oid)
    print("Temporal properties (IDs):", edge.temporal_properties)
    print("Data loaded status:", edge.list_temporal_properties_status())

    # Check TimescaleDB directly
    measurements = hg.timescale.get_measurements(
        entity_uid=edge.oid,
        variable="num_rides"
    )
    print(f"Direct TimescaleDB query with entity_uid='{edge.oid}': {len(measurements) if measurements else 0} rows")

    # Also try with the ts_id
    ts_id = edge.get_temporal_property_id("num_rides")
    print(f"ts_id: {ts_id}")
    # Query

    
    # Snapshot

    sn1 = hg.snapshot(when="2024-06-01 00:00:00", mode='hybrid')
    sn2 = hg.snapshot(when="2024-06-06 00:00:00", mode='hybrid')
    print(f"\nSnapshot: { sn1.count_nodes()} nodes, { sn1.count_edges()} edges")
    print(f"Density: { sn1.density()}\n")
    print(f"Connected components : { sn1.connected_components()} ")
    print(f"number of nodes of sn2: {sn2.count_nodes()}")

    # Snapshot sequence + TSGen
    """"""snapshots = hg.snapshot_sequence(
        start="2024-06-00 00:00:00",
        end="2024-06-06 00:00:00",
        every=timedelta(hours=1)
    )""""""

    edge = hg.query().edge("Trip_2212_2212_2024-05-17T12:36:00").execute()
    print("Available temporal properties:", edge.temporal_properties)
    print("Available static properties:", edge.static_properties)
   
    measurements = hg.timescale.get_measurements(
        entity_uid=edge.oid,
        variable="num_rides"
    )
    print(measurements)
    print(edge.get_temporal_property('num_rides'))
    print("Edge OID:", edge.oid)
    print("Temporal properties (IDs):", edge.temporal_properties)
    print("Data loaded status:", edge.list_temporal_properties_status())


    snapshots = hg.snapshot_sequence(
        start="2024-05-18 00:00:00",
        end="2024-05-25 00:00:00",
        every=timedelta(days=1)
    )
    print(f"\nSnapshotSequence: {len(snapshots)} snapshots")

    ts = snapshots.tsgen()
    node_count_ev=ts.global_.nodes.count(label="Station")
    
    print("node with id 2: ",hg.query().nodes(label="Station").execute()[1])
    avg_degree=ts.entities.nodes.degree(node_id="2172",label="Station", weight="num_rides").max()
    print(f"Node count TimeSeries: {node_count_ev}")
    print(f"Average degree TimeSeries: {avg_degree}")
    high_cap_stations=hg.query().nodes("Station").where(lambda n: n.get_static_property('capacity') >60).execute()
    print('result',len(high_cap_stations))
    nodes_id= {st.oid for st in high_cap_stations}
    high_cap=hg.subHygraph(nodes_id,"high_capacity_stations")
    #high_cap.properties.create_temporal_property(hg,'test',node_count_ev,)
    print("subgraph name:",high_cap.properties.get_temporal_property("test"),"\n")
    print("subgraph: ",high_cap,"\n")
    spike = TimeSeries.create_pattern('spike', length=10)
    ts=hg.query().node("2212").ts_property('num_bikes_available')
    print(spike)
    print(ts)
    matches=ts.find_pattern(spike,top_k=5)
    for m in matches:
        print(f"Found at {m.start_time} (distance: {m.distance:.3f})")
    
    low_availability = hg.query() \
        .nodes(label='Station') \
        .where_ts_agg('num_bikes_available', 'mean' ,'<', 19) \
        .execute()
    print("low_availability")
    print(low_availability)"""
    """
    results2=hg.query().nodes().between("2001-01-01", datetime.now().isoformat())\
    .where_ts_compare("num_bikes_available", "min", ">", "num_bikes_disabled","mean").execute()
    print("results2",results2)
    node1=hg.query().node('5').ts_property('num_bikes_available').min()
    print("min",node1)
    node2=hg.query().node('5').ts_property('num_bikes_disabled').mean()
    print("mean disabled",node2)
    results = (
        hg.query()
        .nodes()
        .between("2001-01-01", datetime.now().isoformat())
        .where_ts_agg("num_bikes_available", "min", ">", 2)
        .where_ts_agg("num_bikes_disabled", "max", "<", 10)
        .order_by("oid")
        .execute()
    )
    node=hg.query().node('4').ts_property('num_bikes_available')
    print("min",node.min())
    print("max",node.max())
    node2=hg.query().node('4').ts_property('num_bikes_disabled')
    print("min",node2.min())
    print("max",node2.max())
    print(results)
    snapshots = hg.snapshot_sequence(
        start="2024-05-20 00:00:00",
        end="2024-05-25 00:00:00",
        every=timedelta(days=1)
    )
    print(f"\nSnapshotSequence: {len(snapshots)} snapshots")
    ts_filter_start = time.perf_counter()
    result= hg.query().edges().between('2024-05-22', '2024-06-03').where_ts_agg('num_rides', ' max', ' > ',1000).where_ts_agg('active_trips','max','>',1000).execute()
    ts_filter_elapsed = time.perf_counter() - ts_filter_start
    print(f"[TIMER] where_ts_agg filter for edge: {ts_filter_elapsed * 1000:.2f}ms")
    print(len(result))
    edge_max=hg.query().edge('Trip_2207_2190_2024').  ts_property('num_rides').max
    edge_active_trips=hg.query().edge('Trip_2207_2190_2024').  ts_property('active_trips').max"""



    '''PGEdge(oid=Trip_2207_2190_2024 - 05 - 16
    T18: 21:40, 2207 - [Trip]->2190), PGEdge(oid=Trip_2208_2190_2024 - 05 - 17
    T16: 22:48, 2208 - [Trip]->2190), PGEdge(oid=Trip_2209_2190_2024 - 05 - 17
    T08: 54:33, 2209 - [Trip]->2190), PGEdge(oid=Trip_2211_2190_2024 - 05 - 17
    T16: 07:49, 2211 - [Trip]->2190), PGEdge(oid=Trip_2213_2190_2024 - 05 - 29
    T18: 24:24, 2213 - [Trip]->2190), PGEdge(oid=Trip_2137_2190_2024 - 05 - 16
    T00: 00:00, 2137 - [Trip]->2190),'''

    """demo scenario"""
    # Step 1: Convert METR-LA to CSV (generates ~7M rows, takes ~1 min)
    #result = convert_metrla()
    # This prints free-flow speed statistics — save these numbers for the paper

    # Step 2: Load CSV into database
    #load_csv(csv_dir= Path("../inputFiles/metrla_csv"), graph_name="hygraph")
    print(hg.stats())  # Should show 207 nodes, ~1500 edges
    nodes = hg.query().nodes("Sensor").limit(3).execute()
    for n in nodes:
        print(n.oid, n.get_static_property("free_flow_speed"))
    # ─────────────────────────────────────────────────────────────────────────────
    # S1 — Congestion cluster formation (Monday morning, 5-min steps = full granularity)
    # Predicate: velocity ratio r(v,t) = speed(v,t)/v95(v) < 0.6  [Li et al. 2015]
    # ─────────────────────────────────────────────────────────────────────────────
    week_april = hg.predicate_induced(
        "speed / free_flow_speed < 0.6",
        ("2012-03-05 06:00", "2012-03-05 09:00"),
        step_minutes=5  # METR-LA resolution: evaluate at every reading
    ).evaluate()

    print(week_april.summary())
    # Keys: total_timesteps, unique_members, max_simultaneous_members, lifecycle_events

    # Lifecycle events — this is what S1 shows: cluster birth, growth, merge
    for ev in week_april.lifecycle_events:
        print(f"  {ev.event_type.value:8s} @ {ev.timestamp.strftime('%H:%M')} "
              f"  component={ev.component_id}  size={ev.size}")


    t_peak = datetime(2012, 3, 5, 7, 30)
    members = week_april.members_at(t_peak)
    components = week_april.components_at(t_peak)
    print(f"\nAt 07:30: {len(members)} congested sensors in {len(components)} clusters")
    for i, comp in enumerate(components):
        print(f"  Cluster {i + 1}: {len(comp)} sensors")

    # ─────────────────────────────────────────────────────────────────────────────
    # S2 — Emergent network signals: Monday vs Friday
    # TSGen computes graph-level TS from the induced subgraph's structural evolution
    # ─────────────────────────────────────────────────────────────────────────────
    friday = hg.predicate_induced(
        "speed / free_flow_speed < 0.6",
        ("2012-03-09 06:00", "2012-03-09 09:00"),
        step_minutes=5
    ).evaluate()

    signals_mon = hg.tsgen_from_induced(monday).compute()
    signals_fri = hg.tsgen_from_induced(friday).compute()

    # signals_mon is a dict: {"density": TimeSeries, "component_count": TimeSeries, ...}
    for metric in ["density", "component_count", "largest_component"]:
        ts_mon = signals_mon[metric]
        ts_fri = signals_fri[metric]
        vals_mon = [v[0] if isinstance(v, list) else v for v in ts_mon.data]
        vals_fri = [v[0] if isinstance(v, list) else v for v in ts_fri.data]
        print(f"{metric:25s}  Monday peak={max(vals_mon):.3f}   Friday peak={max(vals_fri):.3f}")
    # Expected: Monday shows sharper peak (rapid merge → system-wide congestion)
    # Friday shows more gradual increase

    # ─────────────────────────────────────────────────────────────────────────────
    # S3 — Regime change analysis: March vs June
    # step_minutes=60: one evaluation per hour, sufficient for month-level analysis
    # ─────────────────────────────────────────────────────────────────────────────
    march = hg.predicate_induced(
        "speed / free_flow_speed < 0.3",
        ("2012-04-02 06:00", "2012-04-06 23:59"),
        step_minutes=60,
        time_filter=(6, 10),
        node_label="Sensor",
        name="March_mornings"
    ).evaluate()

    june = hg.predicate_induced(
        "speed / free_flow_speed < 0.3",
        ("2012-04-07 06:00", "2012-04-08 23:59"),
        step_minutes=60,
        time_filter=(6, 10),
        node_label="Sensor",
        name="June_mornings"
    ).evaluate()

    # min_frequency=0.3 means: a sensor must qualify in >=30% of morning evaluations
    # to be considered "chronically congested" in that period.
    # Sensors that only briefly dipped below the threshold are excluded from both sets.
    # This is what generates meaningful ADDED/REMOVED sets.
    diff = hg.diff_from_induced(
        march, june,
        period_1_label="March", period_2_label="June",
        min_frequency=0.6,# tune this: try 0.1, 0.2, 0.3, 0.5
        delta_max_days=0.25
    )

    print(diff)
    # → HyGraph(diff_March_vs_June, nodes=N, edges=M)

    # ── Diff summary (structural + signal statistics) ─────────────────────────
    print(diff.diff_summary)
    # DiffSummary: added=X, removed=Y, persisted=Z,
    #   jaccard=..., avg_nrmse=..., max_nrmse=...,
    #   cpg_root_count=..., cpg_max_depth=..., cpg_avg_delay_days=...

    # ── CPG: Change Propagation Graph ─────────────────────────────────────────
    # Built from structural_transition_times of ADDED nodes:
    # each ADDED node's transition time = when it first joined a cluster (≥2 members)
    # Directed edges: earlier-transitioning → later-transitioning adjacent sensors
    # within delta_max_days=30
    cpg = diff.cpg
    print(cpg)
    # → ChangePropagationGraph(nodes=N, edges=M, roots=K, max_depth=D, avg_delay=X.Xd)

    print("Change originators (roots):", cpg.roots)
    # These are the sensors where new congestion STARTED in June

    print("Leaves (most recently affected):", cpg.leaves)

    print("Propagation timeline:")
    for ts, node_id in cpg.get_propagation_timeline()[:10]:
        print(f"  {ts.strftime('%Y-%m-%d')}  sensor {node_id}")

    # Trace paths from each root — shows the exact propagation trajectory
    for root in cpg.roots[:3]:
        paths = cpg.get_paths_from(root)
        print(f"\nRoot {root}: {len(paths)} propagation paths")
        for path in paths[:2]:
            print(f"  {path}")
            # → PropagationPath(root → s2 → s5, 18.3d)

    # ── Hybrid pattern matching on the diff HyGraph ────────────────────────────
    # diff IS a HyGraph — use the SAME query interface as any HyGraph
    # This demonstrates the closure property described in the paper

    # Newly congested sensors in June
    added = diff.query().nodes().where(
        lambda n: n.get_static_property("delta_struct") == "ADDED"
    ).execute()
    print(f"\nNewly congested in June: {len(added)}")
    for n in added[:3]:
        print(f"  sensor {n.oid}"
              f"  transition={n.get_static_property('transition_ts')}")

    # Persistently congested sensors that got significantly worse (signal divergence)
    high_divergence = diff.query().nodes().where(
        lambda n: n.get_static_property("delta_struct") == "PERSISTED"
                  and (n.get_static_property("nrmse") or 0) > 0.5
    ).execute()
    print(f"Persistently congested + high nRMSE: {len(high_divergence)}")

    # Sensors that IMPROVED (less congested in June — negative delta_mu means
    # speed increased on average, i.e., less congestion)
    improved = diff.query().nodes().where(
        lambda n: n.get_static_property("delta_struct") == "PERSISTED"
                  and (n.get_static_property("delta_mu") or 0) > 5.0
    ).execute()
    print(f"Improved in June (avg speed +5mph): {len(improved)}")

    hg.close()