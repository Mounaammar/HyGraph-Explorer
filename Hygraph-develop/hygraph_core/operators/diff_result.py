"""
HyGraphDiff Result — Hybrid Change Descriptor + Change Propagation Graph

Each entity carries delta(e) = (delta_struct, delta_signal).
The result satisfies the closure property: it IS a HyGraph instance.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

import numpy as np

from hygraph_core.operators.change_propagation import ChangePropagationGraph

if TYPE_CHECKING:
    from hygraph_core.hygraph.hygraph import HyGraph
    from hygraph_core.operators.predicate_induced import PredicateInducedSubHyGraph




@dataclass
class HybridChangeDescriptor:
    delta_struct: str  # "ADDED", "REMOVED", "PERSISTED"
    delta_mu: Optional[float] = None
    sigma_ratio: Optional[float] = None
    delta_slope: Optional[float] = None
    nrmse: Optional[float] = None
    transition_timestamp: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        d = {"delta_struct": self.delta_struct}
        if self.delta_mu is not None:
            d["delta_mu"] = round(self.delta_mu, 4)
        if self.sigma_ratio is not None:
            d["sigma_ratio"] = round(self.sigma_ratio, 4)
        if self.delta_slope is not None:
            d["delta_slope"] = round(self.delta_slope, 4)
        if self.nrmse is not None:
            d["nrmse"] = round(self.nrmse, 4)
        if self.transition_timestamp is not None:
            d["transition_timestamp"] = self.transition_timestamp.isoformat()
        return d

    def __repr__(self) -> str:
        parts = [f"delta_struct={self.delta_struct}"]
        if self.nrmse is not None:
            parts.append(f"nRMSE={self.nrmse:.3f}")
        if self.delta_mu is not None:
            parts.append(f"delta_mu={self.delta_mu:+.2f}")
        if self.transition_timestamp is not None:
            parts.append(f"transition={self.transition_timestamp.date()}")
        return f"delta({', '.join(parts)})"


@dataclass
class AnnotatedEntity:
    oid: str
    label: str
    properties: Dict[str, Any]
    change_descriptor: HybridChangeDescriptor

    def to_dict(self) -> Dict[str, Any]:
        return {
            "oid": self.oid,
            "label": self.label,
            "properties": self.properties,
            "change": self.change_descriptor.to_dict(),
        }




def compute_nrmse(v1: np.ndarray, v2: np.ndarray) -> float:
    if len(v1) == 0 or len(v2) == 0:
        return 0.0
    min_len = min(len(v1), len(v2))
    a, b = v1[:min_len], v2[:min_len]
    rmse = np.sqrt(np.mean((a - b) ** 2))
    std1 = np.std(a)
    return float(rmse / std1) if std1 > 1e-9 else 0.0


def compute_signal_descriptor(v1: np.ndarray, v2: np.ndarray):
    if len(v1) < 2 or len(v2) < 2:
        return (0.0, 1.0, 0.0, 0.0)
    delta_mu = float(np.mean(v2) - np.mean(v1))
    std1, std2 = np.std(v1), np.std(v2)
    sigma_ratio = float(std2 / std1) if std1 > 1e-9 else 1.0
    slope1 = float(np.polyfit(np.arange(len(v1)), v1, 1)[0])
    slope2 = float(np.polyfit(np.arange(len(v2)), v2, 1)[0])
    delta_slope = slope2 - slope1
    nrmse = compute_nrmse(v1, v2)
    return (delta_mu, sigma_ratio, delta_slope, nrmse)




@dataclass
class DiffSummary:
    added_count: int = 0
    removed_count: int = 0
    persisted_count: int = 0
    jaccard_nodes: float = 0.0
    avg_nrmse: float = 0.0
    max_nrmse: float = 0.0
    change_homogeneity: float = 0.0
    cpg_root_count: int = 0
    cpg_max_depth: int = 0
    cpg_avg_delay_days: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "added": self.added_count, "removed": self.removed_count,
            "persisted": self.persisted_count,
            "jaccard_nodes": round(self.jaccard_nodes, 4),
            "avg_nrmse": round(self.avg_nrmse, 4),
            "max_nrmse": round(self.max_nrmse, 4),
            "change_homogeneity": round(self.change_homogeneity, 4),
            "cpg_root_count": self.cpg_root_count,
            "cpg_max_depth": self.cpg_max_depth,
            "cpg_avg_delay_days": round(self.cpg_avg_delay_days, 2),
        }




def _build_ts_id_mapping(hygraph: 'HyGraph', variable: str,
                         node_label: str = "Node") -> Dict[str, str]:
    """
    Build node_uid -> ts_id mapping from AGE properties.
    Returns dict: {"1": "ts_0", "2": "ts_1", ...}
    """
    ts_id_key = f"{variable}_ts_id"
    mapping: Dict[str, str] = {}

    nodes = hygraph.age.query_nodes(label=node_label)
    for node in nodes:
        uid = node.get("uid", "")
        props = node.get("properties", {})
        if isinstance(props, str):
            try:
                props = json.loads(props)
            except Exception:
                props = {}

        ts_id = props.get(ts_id_key)
        temporal_props = props.get("temporal_properties", {})
        if not ts_id and variable in temporal_props:
            ts_id = temporal_props[variable]

        if ts_id and uid:
            mapping[str(uid)] = str(ts_id)

    return mapping


def _fetch_ts_values(hygraph: 'HyGraph', ts_id: str, variable: str,
                     start: datetime, end: datetime) -> np.ndarray:
    """Fetch time-series values from ts.measurements using the ts_id."""
    sql = """
        SELECT value FROM ts.measurements
        WHERE entity_uid = %s AND variable = %s
          AND ts >= %s AND ts < %s
          AND value != 0
        ORDER BY ts
    """
    params = [ts_id, variable,
              start.strftime("%Y-%m-%d %H:%M:%S"),
              end.strftime("%Y-%m-%d %H:%M:%S")]

    try:
        with hygraph.db.conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                values = [float(row["value"]) for row in cur.fetchall() if row["value"] is not None]
        return np.array(values) if values else np.array([])
    except Exception as e:
        print(f"  WARNING: TS fetch failed for {ts_id}: {e}")
        return np.array([])


def _fetch_adjacency(hygraph: 'HyGraph') -> List[Tuple[str, str]]:
    """Fetch all edges from AGE as (source_uid, target_uid) tuples."""
    try:
        results = hygraph.age.cypher_multi(
            "MATCH (a)-[e]->(b) RETURN a.uid, b.uid",
            columns=["src", "tgt"]
        )
        edges = []
        for row in results:
            src = str(row.get("src", "")).strip('"')
            tgt = str(row.get("tgt", "")).strip('"')
            if src and tgt:
                edges.append((src, tgt))
        return edges
    except Exception as e:
        print(f"  WARNING: Adjacency fetch failed: {e}")
        return []




@dataclass
class GraphLevelSignalComparison:
    """Comparison of a single graph-level metric between two periods.

    Uses the same signal descriptors as per-node comparison:
        delta_mu     — level shift (mean₂ - mean₁)
        sigma_ratio  — variability change (σ₂ / σ₁)
        delta_slope  — trend change (slope₂ - slope₁)
        nrmse        — overall divergence (RMSE / σ₁)
    """
    metric_name: str
    period_1_label: str
    period_2_label: str
    p1_length: int = 0
    p2_length: int = 0
    # Same descriptors as HybridChangeDescriptor
    delta_mu: Optional[float] = None
    sigma_ratio: Optional[float] = None
    delta_slope: Optional[float] = None
    nrmse: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "metric": self.metric_name,
            "period_1": self.period_1_label,
            "period_2": self.period_2_label,
            "p1_length": self.p1_length,
            "p2_length": self.p2_length,
        }
        if self.delta_mu is not None:
            d["delta_mu"] = round(self.delta_mu, 4)
        if self.sigma_ratio is not None:
            d["sigma_ratio"] = round(self.sigma_ratio, 4)
        if self.delta_slope is not None:
            d["delta_slope"] = round(self.delta_slope, 4)
        if self.nrmse is not None:
            d["nrmse"] = round(self.nrmse, 4)
        return d


def compare_graph_level_signals(
    ts_dict_1: Dict[str, Any],
    ts_dict_2: Dict[str, Any],
    period_1_label: str = "period_1",
    period_2_label: str = "period_2",
) -> List[GraphLevelSignalComparison]:
    """Compare graph-level time series from two TSGen runs.

    Uses the same compute_signal_descriptor as per-node comparison
    to produce consistent delta_mu, sigma_ratio, delta_slope, nRMSE.
    """
    comparisons = []
    all_metrics = set(list(ts_dict_1.keys()) + list(ts_dict_2.keys()))

    def _extract(ts):
        if ts is None:
            return np.array([])
        if hasattr(ts, 'data'):
            return np.array([v[0] if isinstance(v, (list, tuple)) else v
                             for v in ts.data], dtype=float)
        if isinstance(ts, dict) and 'values' in ts:
            return np.array(ts['values'], dtype=float)
        return np.array([])

    for metric in sorted(all_metrics):
        v1 = _extract(ts_dict_1.get(metric))
        v2 = _extract(ts_dict_2.get(metric))

        comp = GraphLevelSignalComparison(
            metric_name=metric,
            period_1_label=period_1_label,
            period_2_label=period_2_label,
            p1_length=len(v1),
            p2_length=len(v2),
        )

        if len(v1) >= 2 and len(v2) >= 2:
            delta_mu, sigma_ratio, delta_slope, nrmse = compute_signal_descriptor(v1, v2)
            comp.delta_mu = delta_mu
            comp.sigma_ratio = sigma_ratio
            comp.delta_slope = delta_slope
            comp.nrmse = nrmse

        comparisons.append(comp)

    return comparisons


class HyGraphDiffResult:
    """
    Result of HyGraphDiff — a HyGraph-compatible object with change annotations.
    """

    def __init__(self):
        self.nodes: Dict[str, AnnotatedEntity] = {}
        self.edges: Dict[str, AnnotatedEntity] = {}
        self.cpg: Optional[ChangePropagationGraph] = None
        self.summary: DiffSummary = DiffSummary()
        self.graph_level_comparisons: List[GraphLevelSignalComparison] = []
        self._period_1_label: str = "period_1"
        self._period_2_label: str = "period_2"
        self._p1_start: Optional[datetime] = None
        self._p1_end: Optional[datetime] = None
        self._p2_start: Optional[datetime] = None
        self._p2_end: Optional[datetime] = None

    @classmethod
    def from_induced(
        cls,
        induced_1: 'PredicateInducedSubHyGraph',
        induced_2: 'PredicateInducedSubHyGraph',
        hygraph: 'HyGraph',
        ts_property: Optional[str] = None,
        delta_max_days: float = 30.0,
        period_1_label: str = "period_1",
        period_2_label: str = "period_2",
        min_frequency: float = 0.0,
        period_1: Optional[Tuple[str, str]] = None,
        period_2: Optional[Tuple[str, str]] = None,
    ) -> 'HyGraphDiffResult':
        """Create diff from two predicate-induced SubHyGraphs.

        Args:
            min_frequency: Fraction of timesteps [0.0, 1.0] a sensor must qualify
                           in to be considered a member of that period.
                           0.0 (default) = union semantics (ever appeared = member).
                           0.1 = must qualify in >=10% of timesteps.
                           0.5 = must qualify in >=50% of timesteps (majority).
                           Use 0.1-0.3 for multi-week comparisons to filter out
                           sensors that only briefly dipped below threshold.
            period_1: Optional (start, end) ISO strings to slice induced_1's
                      time range. When provided, only timestamps within this
                      sub-period are considered for membership and TS fetching.
                      Falls back to induced_1's own time range if None.
            period_2: Optional (start, end) ISO strings to slice induced_2's
                      time range. Same semantics as period_1.
        """
        if ts_property is None:
            ts_property = induced_1.predicate.ts_property
        result = cls()
        result._period_1_label = period_1_label
        result._period_2_label = period_2_label

        # Resolve period bounds: use explicit periods if given, else induced's own range
        p1_start = datetime.fromisoformat(period_1[0]) if period_1 else induced_1.start_time
        p1_end   = datetime.fromisoformat(period_1[1]) if period_1 else induced_1.end_time
        p2_start = datetime.fromisoformat(period_2[0]) if period_2 else induced_2.start_time
        p2_end   = datetime.fromisoformat(period_2[1]) if period_2 else induced_2.end_time
        result._p1_start = p1_start
        result._p1_end = p1_end
        result._p2_start = p2_start
        result._p2_end = p2_end

        print(f"  Period 1: {p1_start} to {p1_end}")
        print(f"  Period 2: {p2_start} to {p2_end}")

        uid_to_ts_id = _build_ts_id_mapping(
            hygraph, ts_property, induced_1.node_label
        )

        # Collect member sets with frequency filtering
        def _members_by_frequency(induced, min_freq: float,
                                  period_start: datetime = None,
                                  period_end: datetime = None) -> Set[str]:
            # Filter timestamps to those within the period bounds
            if period_start and period_end:
                valid_timestamps = [t for t in induced.timestamps
                                    if period_start <= t < period_end]
            else:
                valid_timestamps = list(induced.timestamps)

            if min_freq <= 0.0:
                # Union semantics — backward compatible
                members: Set[str] = set()
                for ts in valid_timestamps:
                    members.update(induced.members_at(ts))
                return members

            # Count ACTUAL qualifying timesteps per sensor.
            # We cannot rely on membership_changes() records alone because
            # a record is only created when membership changes — a sensor
            # that stays active across multiple steps without any other
            # sensor changing would only appear in one record despite being
            # active for many steps. Instead we replay the membership state
            # across all evaluated timestamps using members_at().
            total_timesteps = len(valid_timestamps)
            if total_timesteps == 0:
                print(f"    WARNING: No timestamps within period bounds")
                return set()
            counts: Dict[str, int] = {}
            for ts in valid_timestamps:
                for nid in induced.members_at(ts):
                    counts[nid] = counts.get(nid, 0) + 1
            threshold_count = min_freq * total_timesteps
            qualified = {nid for nid, cnt in counts.items() if cnt >= threshold_count}
            print(f"    Frequency filter (>={min_freq:.0%} of {total_timesteps} steps): "
                  f"{len(qualified)}/{len(counts)} sensors qualify")
            return qualified

        members_1 = _members_by_frequency(induced_1, min_frequency, p1_start, p1_end)
        members_2 = _members_by_frequency(induced_2, min_frequency, p2_start, p2_end)

        added_ids = members_2 - members_1
        removed_ids = members_1 - members_2
        persisted_ids = members_1 & members_2

        print(f"  Diff: {len(added_ids)} ADDED, {len(removed_ids)} REMOVED, "
              f"{len(persisted_ids)} PERSISTED")

        # --- ADDED ---
        # (transition_timestamp will be updated below after CPG computation)
        for nid in added_ids:
            result.nodes[nid] = AnnotatedEntity(
                oid=nid, label=induced_1.node_label, properties={},
                change_descriptor=HybridChangeDescriptor(
                    delta_struct="ADDED",
                ),
            )

        # --- REMOVED ---
        for nid in removed_ids:
            result.nodes[nid] = AnnotatedEntity(
                oid=nid, label=induced_1.node_label, properties={},
                change_descriptor=HybridChangeDescriptor(delta_struct="REMOVED"),
            )

        # --- PERSISTED: compute signal comparison ---
        nrmse_values: List[float] = []
        for nid in persisted_ids:
            ts_id = uid_to_ts_id.get(nid)
            if not ts_id:
                continue

            v1 = _fetch_ts_values(hygraph, ts_id, ts_property,
                                  p1_start, p1_end)
            v2 = _fetch_ts_values(hygraph, ts_id, ts_property,
                                  p2_start, p2_end)

            delta_mu, sigma_ratio, delta_slope, nrmse = compute_signal_descriptor(v1, v2)
            nrmse_values.append(nrmse)

            result.nodes[nid] = AnnotatedEntity(
                oid=nid, label=induced_1.node_label, properties={},
                change_descriptor=HybridChangeDescriptor(
                    delta_struct="PERSISTED",
                    delta_mu=delta_mu, sigma_ratio=sigma_ratio,
                    delta_slope=delta_slope, nrmse=nrmse,
                ),
            )

        # --- Change Propagation Graph ---
        # For ADDED nodes, compute period-specific transition timestamps:
        # the first timestep within Period 2 where each ADDED sensor qualifies.
        # Using the full-evaluation transition times would be wrong because they
        # reflect when a sensor first appeared during the entire SubHyGraph
        # evaluation, not when it first appeared in the comparison period.
        if added_ids:
            adjacency = _fetch_adjacency(hygraph)

            # Compute period-2-specific first-appearance for ADDED nodes
            p2_transition_times: Dict[str, datetime] = {}
            p2_timestamps = sorted(t for t in induced_2.timestamps
                                   if p2_start <= t <= p2_end)
            remaining = set(added_ids)
            for ts in p2_timestamps:
                if not remaining:
                    break
                members_at_ts = induced_2.members_at(ts)
                for nid in list(remaining):
                    if nid in members_at_ts:
                        p2_transition_times[nid] = ts
                        remaining.discard(nid)

            print(f"  CPG: {len(p2_transition_times)}/{len(added_ids)} ADDED nodes "
                  f"have period-2 transition timestamps")

            cpg = ChangePropagationGraph(
                added_nodes=list(added_ids),
                transition_times=p2_transition_times,
                adjacency=adjacency,
                delta_max_days=delta_max_days,
            )
            cpg.build()
            result.cpg = cpg

            # Update ADDED nodes with period-specific transition timestamps
            for nid in added_ids:
                ts = p2_transition_times.get(nid)
                if ts and nid in result.nodes:
                    result.nodes[nid].change_descriptor.transition_timestamp = ts

        # --- Summary ---
        all_ids = members_1 | members_2
        result.summary = DiffSummary(
            added_count=len(added_ids),
            removed_count=len(removed_ids),
            persisted_count=len(persisted_ids),
            jaccard_nodes=len(persisted_ids) / len(all_ids) if all_ids else 1.0,
            avg_nrmse=float(np.mean(nrmse_values)) if nrmse_values else 0.0,
            max_nrmse=float(np.max(nrmse_values)) if nrmse_values else 0.0,
            change_homogeneity=float(np.var(nrmse_values)) if nrmse_values else 0.0,
            cpg_root_count=len(result.cpg.roots) if result.cpg else 0,
            cpg_max_depth=result.cpg.max_depth if result.cpg else 0,
            cpg_avg_delay_days=result.cpg.avg_delay_days if result.cpg else 0.0,
        )

        print(f"  Summary: Jaccard={result.summary.jaccard_nodes:.3f}, "
              f"avg_nRMSE={result.summary.avg_nrmse:.3f}, "
              f"CPG roots={result.summary.cpg_root_count}")

        return result



    def query_added(self) -> List[AnnotatedEntity]:
        return [e for e in self.nodes.values() if e.change_descriptor.delta_struct == "ADDED"]

    def query_removed(self) -> List[AnnotatedEntity]:
        return [e for e in self.nodes.values() if e.change_descriptor.delta_struct == "REMOVED"]

    def query_persisted(self, nrmse_threshold: Optional[float] = None) -> List[AnnotatedEntity]:
        results = [e for e in self.nodes.values() if e.change_descriptor.delta_struct == "PERSISTED"]
        if nrmse_threshold is not None:
            results = [e for e in results
                       if e.change_descriptor.nrmse is not None
                       and e.change_descriptor.nrmse > nrmse_threshold]
        return results

    def query_by_descriptor(self, delta_struct=None, nrmse_min=None, nrmse_max=None,
                            delta_mu_min=None, delta_mu_max=None) -> List[AnnotatedEntity]:
        results = list(self.nodes.values())
        if delta_struct:
            results = [e for e in results if e.change_descriptor.delta_struct == delta_struct]
        if nrmse_min is not None:
            results = [e for e in results if e.change_descriptor.nrmse is not None
                       and e.change_descriptor.nrmse >= nrmse_min]
        if nrmse_max is not None:
            results = [e for e in results if e.change_descriptor.nrmse is not None
                       and e.change_descriptor.nrmse <= nrmse_max]
        if delta_mu_min is not None:
            results = [e for e in results if e.change_descriptor.delta_mu is not None
                       and e.change_descriptor.delta_mu >= delta_mu_min]
        if delta_mu_max is not None:
            results = [e for e in results if e.change_descriptor.delta_mu is not None
                       and e.change_descriptor.delta_mu <= delta_mu_max]
        return results

    def query(self) -> 'DiffResultQueryBuilder':
        """
        Start a fluent query on the diff result — same interface as HyGraph.query().

        The diff result IS a HyGraph (closure property). Each entity carries
        its change descriptor as a queryable property.

        Example:
            # All added nodes
            diff.query().nodes().where(
                lambda e: e.change_descriptor.delta_struct == "ADDED"
            ).execute()

            # Persisted nodes with high divergence
            diff.query().nodes().where(
                lambda e: e.change_descriptor.nrmse is not None
                      and e.change_descriptor.nrmse > 1.0
            ).execute()

            # Shorthand: filter by structural annotation
            diff.query().nodes().added().execute()
            diff.query().nodes().persisted().where(
                lambda e: e.change_descriptor.delta_mu < -5
            ).execute()
        """
        return DiffResultQueryBuilder(self)

    def add_graph_level_comparisons(
        self,
        ts_dict_1: Dict[str, Any],
        ts_dict_2: Dict[str, Any],
    ) -> None:
        """Add graph-level signal comparisons from TSGen results.

        Args:
            ts_dict_1: {metric_name: TimeSeries} from TSGen on period 1
            ts_dict_2: {metric_name: TimeSeries} from TSGen on period 2
        """
        self.graph_level_comparisons = compare_graph_level_signals(
            ts_dict_1, ts_dict_2,
            self._period_1_label, self._period_2_label,
        )
        print(f"  Graph-level comparisons: {len(self.graph_level_comparisons)} metrics")
        for c in self.graph_level_comparisons:
            if c.nrmse is not None:
                print(f"    {c.metric_name}: "
                      f"delta_mu={c.delta_mu:+.3f}, sigma_ratio={c.sigma_ratio:.3f}, "
                      f"delta_slope={c.delta_slope:+.4f}, nRMSE={c.nrmse:.3f}")
            else:
                print(f"    {c.metric_name}: insufficient data")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "summary": self.summary.to_dict(),
            "nodes": {nid: e.to_dict() for nid, e in self.nodes.items()},
            "cpg": self.cpg.to_dict() if self.cpg else None,
            "period_1": self._period_1_label,
            "period_2": self._period_2_label,
            "graph_level_comparisons": [
                c.to_dict() for c in self.graph_level_comparisons
            ] if self.graph_level_comparisons else None,
        }

    def __repr__(self) -> str:
        return (
            f"HyGraphDiffResult(added={self.summary.added_count}, "
            f"removed={self.summary.removed_count}, "
            f"persisted={self.summary.persisted_count}, "
            f"cpg_roots={self.summary.cpg_root_count}, "
            f"graph_signals={len(self.graph_level_comparisons)})"
        )




class DiffResultQueryBuilder:
    """Fluent query builder for HyGraphDiffResult.

    Follows the same interface as HyGraph's QueryBuilder:
        .query().nodes().where(predicate).execute()

    Adds change-descriptor shortcuts:
        .added()      — filter to ADDED entities
        .removed()    — filter to REMOVED entities
        .persisted()  — filter to PERSISTED entities
        .where_nrmse(op, value) — filter by signal divergence
        .where_delta_mu(op, value) — filter by level shift
    """

    _OPS = {
        "<":  lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        ">":  lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
    }

    def __init__(self, diff: HyGraphDiffResult):
        self._diff = diff
        self._entity_type: Optional[str] = None  # "nodes" or "edges"
        self._label: Optional[str] = None
        self._struct_filter: Optional[str] = None  # "ADDED", "REMOVED", "PERSISTED"
        self._filters: List = []
        self._limit: Optional[int] = None
        self._order_by: Optional[tuple] = None

    # --- entity selection (mirrors QueryBuilder) ---

    def nodes(self, label: Optional[str] = None) -> 'DiffResultQueryBuilder':
        self._entity_type = "nodes"
        self._label = label
        return self

    def edges(self, label: Optional[str] = None) -> 'DiffResultQueryBuilder':
        self._entity_type = "edges"
        self._label = label
        return self

    # --- structural annotation shortcuts ---

    def added(self) -> 'DiffResultQueryBuilder':
        self._struct_filter = "ADDED"
        return self

    def removed(self) -> 'DiffResultQueryBuilder':
        self._struct_filter = "REMOVED"
        return self

    def persisted(self) -> 'DiffResultQueryBuilder':
        self._struct_filter = "PERSISTED"
        return self

    # --- generic predicate (same as QueryBuilder.where) ---

    def where(self, predicate) -> 'DiffResultQueryBuilder':
        """Filter with a lambda on AnnotatedEntity.

        Example:
            .where(lambda e: e.change_descriptor.nrmse > 1.0)
            .where(lambda e: e.properties.get("lat") > 34.1)
        """
        self._filters.append(predicate)
        return self

    # --- change-descriptor filters ---

    def where_nrmse(self, op: str, value: float) -> 'DiffResultQueryBuilder':
        cmp = self._OPS.get(op)
        if cmp is None:
            raise ValueError(f"Unknown operator: {op}")
        self._filters.append(
            lambda e, _cmp=cmp, _v=value: (
                e.change_descriptor.nrmse is not None and _cmp(e.change_descriptor.nrmse, _v)
            )
        )
        return self

    def where_delta_mu(self, op: str, value: float) -> 'DiffResultQueryBuilder':
        cmp = self._OPS.get(op)
        if cmp is None:
            raise ValueError(f"Unknown operator: {op}")
        self._filters.append(
            lambda e, _cmp=cmp, _v=value: (
                e.change_descriptor.delta_mu is not None and _cmp(e.change_descriptor.delta_mu, _v)
            )
        )
        return self

    def where_sigma_ratio(self, op: str, value: float) -> 'DiffResultQueryBuilder':
        cmp = self._OPS.get(op)
        if cmp is None:
            raise ValueError(f"Unknown operator: {op}")
        self._filters.append(
            lambda e, _cmp=cmp, _v=value: (
                e.change_descriptor.sigma_ratio is not None and _cmp(e.change_descriptor.sigma_ratio, _v)
            )
        )
        return self

    # --- ordering / limit ---

    def order_by(self, key, desc: bool = False) -> 'DiffResultQueryBuilder':
        self._order_by = (key, desc)
        return self

    def limit(self, n: int) -> 'DiffResultQueryBuilder':
        self._limit = n
        return self

    # --- execution ---

    def execute(self) -> List[AnnotatedEntity]:
        """Execute query and return matching annotated entities."""
        # Select pool
        if self._entity_type == "edges":
            pool = self._diff.edges.values()
        else:
            pool = self._diff.nodes.values()

        results: List[AnnotatedEntity] = []
        for entity in pool:
            # Label filter
            if self._label and entity.label != self._label:
                continue
            # Structural annotation filter
            if self._struct_filter and entity.change_descriptor.delta_struct != self._struct_filter:
                continue
            # Lambda filters
            if not all(f(entity) for f in self._filters):
                continue
            results.append(entity)

        # Ordering
        if self._order_by:
            key_fn, desc = self._order_by
            if isinstance(key_fn, str):
                results.sort(
                    key=lambda e: e.properties.get(key_fn, 0),
                    reverse=desc,
                )
            else:
                results.sort(key=key_fn, reverse=desc)

        # Limit
        if self._limit:
            results = results[:self._limit]

        return results

    def count(self) -> int:
        return len(self.execute())

    def first(self) -> Optional[AnnotatedEntity]:
        results = self.limit(1).execute()
        return results[0] if results else None

    def as_node_ids(self) -> List[str]:
        return [e.oid for e in self.execute()]
