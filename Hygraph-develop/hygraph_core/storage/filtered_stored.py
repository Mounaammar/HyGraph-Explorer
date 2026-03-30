from typing import Set


class FilteredAGEStore:
    """
    Wrapper around AGEStore that filters results to only show
    nodes/edges in the subgraph.
    """

    def __init__(self, parent_age, node_ids: Set[str]):
        self.parent = parent_age
        self.node_ids = node_ids
        self.edge_ids = None  # Computed lazily

    def _get_edge_ids(self):
        """Compute edges that have both endpoints in subgraph"""
        if self.edge_ids is not None:
            return self.edge_ids

        # Query all edges
        all_edges = self.parent.query_edges()

        # Filter: keep only edges where BOTH src and dst are in node_ids
        self.edge_ids = {
            e['uid'] for e in all_edges
            if e['src_uid'] in self.node_ids and e['dst_uid'] in self.node_ids
        }

        return self.edge_ids

    # Override query methods to filter results

    def query_nodes(self, label=None, at_time=None, start_time=None, end_time=None, limit=None):
        """Query nodes, filtered to subgraph"""
        # Get results from parent
        nodes = self.parent.query_nodes(label, at_time, start_time, end_time, limit)

        # Filter to only nodes in subgraph
        return [n for n in nodes if n['uid'] in self.node_ids]

    def query_edges(self, label=None, source=None, target=None, at_time=None,
                    start_time=None, end_time=None, limit=None):
        """Query edges, filtered to subgraph"""
        # Get results from parent
        edges = self.parent.query_edges(label, source, target, at_time,
                                        start_time, end_time, limit)

        # Filter to only edges in subgraph
        edge_ids = self._get_edge_ids()
        return [e for e in edges if e['uid'] in edge_ids]

    def get_node(self, uid):
        """Get node if it's in subgraph"""
        if uid not in self.node_ids:
            return None
        return self.parent.get_node(uid)

    def get_edge(self, uid):
        """Get edge if it's in subgraph"""
        edge_ids = self._get_edge_ids()
        if uid not in edge_ids:
            return None
        return self.parent.get_edge(uid)

    def count_nodes(self, label=None):
        """Count nodes in subgraph"""
        return len(self.query_nodes(label))

    def count_edges(self, label=None):
        """Count edges in subgraph"""
        return len(self.query_edges(label))

    # Delegate other methods to parent (cypher, etc.)

    def cypher(self, query, params=None):
        """Cypher queries on full graph (not filtered)"""
        # WARNING: Cypher bypasses filtering!
        return self.parent.cypher(query, params)

    def __getattr__(self, name):
        """Delegate unknown methods to parent"""
        return getattr(self.parent, name)


class DiffAnnotatedAGEStore(FilteredAGEStore):
    """
    Wraps FilteredAGEStore to inject change annotations into node/edge properties.
    Used by HyGraphDiff to produce a HyGraph where each entity carries
    its change descriptor as regular queryable properties.

    Injected properties:
        delta_struct:  "ADDED" | "REMOVED" | "PERSISTED"
        nrmse:         float (signal divergence, PERSISTED only)
        delta_mu:      float (level shift, PERSISTED only)
        sigma_ratio:   float (variability change, PERSISTED only)
        delta_slope:   float (trend change, PERSISTED only)
        transition_ts: str   (ISO timestamp, ADDED only)
    """

    def __init__(self, parent_age, node_ids, annotations: dict):
        """
        Args:
            parent_age: The underlying AGEStore
            node_ids: Set of node UIDs in the diff
            annotations: Dict[uid -> dict of change descriptor fields]
        """
        super().__init__(parent_age, node_ids)
        self._annotations = annotations  # uid -> {"delta_struct": ..., "nrmse": ...}

    def _inject_annotations(self, node: dict) -> dict:
        """Inject change descriptor fields into a node's properties."""
        uid = node.get("uid", "")
        ann = self._annotations.get(str(uid))
        if ann:
            props = node.get("properties", {})
            props.update(ann)
            node["properties"] = props
        return node

    def query_nodes(self, label=None, at_time=None, start_time=None, end_time=None, limit=None):
        nodes = super().query_nodes(label, at_time, start_time, end_time, limit)
        return [self._inject_annotations(n) for n in nodes]

    def get_node(self, uid):
        node = super().get_node(uid)
        if node:
            node = self._inject_annotations(node)
        return node


class FilteredTimescaleStore:
    """
    Wrapper around TimescaleDB that filters to only show
    time series from nodes/edges in the subgraph.
    """

    def __init__(self, parent_timescale, age_store: FilteredAGEStore):
        self.parent = parent_timescale
        self.age_store = age_store
        self._ts_ids = None

    def _get_ts_ids(self):
        """Get all ts_ids from subgraph nodes/edges"""
        if self._ts_ids is not None:
            return self._ts_ids

        ts_ids = set()

        # Get ts_ids from nodes
        nodes = self.age_store.query_nodes()
        for node in nodes:
            temporal_props = node.get('properties', {}).get('temporal_properties', {})
            for ts_id in temporal_props.values():
                ts_ids.add(str(ts_id))

        # Get ts_ids from edges
        edges = self.age_store.query_edges()
        for edge in edges:
            temporal_props = edge.get('properties', {}).get('temporal_properties', {})
            for ts_id in temporal_props.values():
                ts_ids.add(str(ts_id))

        self._ts_ids = ts_ids
        return self._ts_ids

    def query_measurements(self, entity_uids=None, variable=None,
                           start_time=None, end_time=None):
        """Query measurements, filtered to subgraph"""
        # If entity_uids specified, filter to subgraph
        if entity_uids is not None:
            ts_ids = self._get_ts_ids()
            entity_uids = [uid for uid in entity_uids if uid in ts_ids]
        else:
            # No entity_uids specified, use all from subgraph
            entity_uids = list(self._get_ts_ids())

        return self.parent.query_measurements(
            entity_uids, variable, start_time, end_time
        )

    def get_last_values(self, entity_uids=None, timestamp=None):
        """Get last values, filtered to subgraph"""
        if entity_uids is not None:
            ts_ids = self._get_ts_ids()
            entity_uids = [uid for uid in entity_uids if uid in ts_ids]
        else:
            entity_uids = list(self._get_ts_ids())

        return self.parent.get_last_values(entity_uids, timestamp)

    def aggregate_measurements(self, entity_uids=None, start_time=None,
                               end_time=None, aggregation="mean"):
        """Aggregate measurements, filtered to subgraph"""
        if entity_uids is not None:
            ts_ids = self._get_ts_ids()
            entity_uids = [uid for uid in entity_uids if uid in ts_ids]
        else:
            entity_uids = list(self._get_ts_ids())

        return self.parent.aggregate_measurements(
            entity_uids, start_time, end_time, aggregation
        )

    def __getattr__(self, name):
        """Delegate unknown methods to parent"""
        return getattr(self.parent, name)