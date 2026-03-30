"""
TSGen Signal Transformation

Derives graph-level temporal signals from node-level time series by computing
structural metrics on evolving predicate-induced subgraphs.

"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

import numpy as np

from hygraph_core.model.timeseries import TimeSeries
from hygraph_core.operators.predicate_induced import (
    PredicateInducedSubHyGraph,
    compute_connected_components,
)

if TYPE_CHECKING:
    from hygraph_core.hygraph.hygraph import HyGraph




def metric_density(node_ids: Set[str], edges: List[Tuple[str, str]], **kw) -> float:
    n, m = len(node_ids), len(edges)
    if n <= 1:
        return 0.0
    return m / (n * (n - 1))


def metric_component_count(node_ids: Set[str], edges: List[Tuple[str, str]], **kw) -> float:
    return float(len(compute_connected_components(node_ids, edges)))


def metric_largest_component(node_ids: Set[str], edges: List[Tuple[str, str]], **kw) -> float:
    comps = compute_connected_components(node_ids, edges)
    return float(max(len(c) for c in comps)) if comps else 0.0


def metric_node_count(node_ids: Set[str], edges: List[Tuple[str, str]], **kw) -> float:
    return float(len(node_ids))


def metric_edge_count(node_ids: Set[str], edges: List[Tuple[str, str]], **kw) -> float:
    return float(len(edges))


def metric_avg_degree(node_ids: Set[str], edges: List[Tuple[str, str]], **kw) -> float:
    if not node_ids:
        return 0.0
    deg = 0
    for src, tgt in edges:
        if src in node_ids:
            deg += 1
        if tgt in node_ids:
            deg += 1
    return deg / len(node_ids)


METRIC_REGISTRY: Dict[str, Callable] = {
    "density": metric_density,
    "component_count": metric_component_count,
    "largest_component": metric_largest_component,
    "node_count": metric_node_count,
    "edge_count": metric_edge_count,
    "avg_degree": metric_avg_degree,
}




class SignalAggregatingMetric:
    """
    A metric that uses both graph structure AND signal values.
    Requires fetching actual TS values from ts.measurements.
    """

    def __init__(self, name: str, property_name: str,
                 aggregation: str = "mean", scope: str = "all"):
        self.name = name
        self.property_name = property_name
        self.aggregation = aggregation
        self.scope = scope  # "all" or "largest_component"

    def compute(self, node_ids: Set[str], edges: List[Tuple[str, str]],
                timestamp: datetime, node_uid_to_ts_id: Dict[str, str],
                hygraph: 'HyGraph') -> float:
        """
        Compute the metric by fetching actual signal values.
        
        Uses node_uid_to_ts_id mapping to translate node UIDs to ts_ids
        for querying ts.measurements.
        """
        target_nodes = node_ids

        if self.scope == "largest_component":
            comps = compute_connected_components(node_ids, edges)
            if comps:
                target_nodes = set(max(comps, key=len))
            else:
                return 0.0

        if not target_nodes:
            return 0.0

        # Map node UIDs to ts_ids
        ts_ids = [node_uid_to_ts_id.get(nid) for nid in target_nodes]
        ts_ids = [tid for tid in ts_ids if tid is not None]
        if not ts_ids:
            return 0.0

        ts_iso = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        ts_id_list = ", ".join(f"'{tid}'" for tid in ts_ids)

        sql = f"""
            SELECT value FROM ts.measurements
            WHERE entity_uid IN ({ts_id_list})
              AND variable = '{self.property_name}'
              AND ts = '{ts_iso}'
              AND value != 0
        """

        try:
            with hygraph.db.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    values = [float(row["value"]) for row in cur.fetchall() if row["value"] is not None]
        except Exception:
            return 0.0

        if not values:
            return 0.0

        arr = np.array(values)
        agg_map = {
            "mean": np.mean, "min": np.min, "max": np.max,
            "std": np.std, "sum": np.sum, "median": np.median,
        }
        return float(agg_map.get(self.aggregation, np.mean)(arr))




class TSGenFromInduced:
    """
    Signal transformation operator for predicate-induced SubHyGraphs.

    Derives graph-level temporal signals by computing structural metrics
    at each moment where the subgraph membership changes.
    """

    def __init__(
        self,
        induced: PredicateInducedSubHyGraph,
        metrics: Optional[List[str]] = None,
        signal_metrics: Optional[List[SignalAggregatingMetric]] = None,
        time_range: Optional[Tuple[datetime, datetime]] = None,
    ):
        if not induced._evaluated:
            raise RuntimeError("PredicateInducedSubHyGraph must be evaluated first")

        self.induced = induced
        self.metrics = metrics or ["density", "component_count", "largest_component"]
        self.signal_metrics = signal_metrics or []
        self.time_range = time_range  # (start, end) to filter membership changes

        for m in self.metrics:
            if m not in METRIC_REGISTRY:
                raise ValueError(f"Unknown metric '{m}'. Available: {list(METRIC_REGISTRY.keys())}")

    def compute(self) -> Dict[str, TimeSeries]:
        """Compute all metrics across membership changes."""
        results: Dict[str, List[Tuple[datetime, float]]] = {
            m: [] for m in self.metrics
        }
        for sm in self.signal_metrics:
            results[sm.name] = []

        change_count = 0
        for change in self.induced.membership_changes():
            # Filter to time_range if specified
            if self.time_range:
                if change.timestamp < self.time_range[0] or change.timestamp > self.time_range[1]:
                    continue
            members = change.current_members
            edges = change.current_edges
            ts = change.timestamp

            # Topological metrics (no signal values needed)
            for metric_name in self.metrics:
                func = METRIC_REGISTRY[metric_name]
                value = func(members, edges)
                results[metric_name].append((ts, value))

            # Signal-aggregating metrics (need actual TS values)
            for sm in self.signal_metrics:
                value = sm.compute(
                    members, edges, ts,
                    self.induced._node_uid_to_ts_id,
                    self.induced.hygraph
                )
                results[sm.name].append((ts, value))

            change_count += 1

        print(f"  TSGen computed {len(self.metrics) + len(self.signal_metrics)} "
              f"metrics across {change_count} change events")

        # Convert to TimeSeries objects
        ts_results: Dict[str, TimeSeries] = {}
        for name, data_points in results.items():
            if data_points:
                ts_results[name] = TimeSeries.from_results(data_points, [name])

        return ts_results

    def compute_and_persist(self, target=None) -> Dict[str, TimeSeries]:
        """Compute metrics and optionally persist as meta-properties."""
        results = self.compute()
        if target is not None and hasattr(target, 'add_temporal_property'):
            for name, ts in results.items():
                target.add_temporal_property(name, ts)
        return results

    def __repr__(self) -> str:
        all_metrics = self.metrics + [sm.name for sm in self.signal_metrics]
        return f"TSGenFromInduced(metrics={all_metrics})"
