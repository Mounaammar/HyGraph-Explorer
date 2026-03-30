"""
Predicate-Induced SubHyGraph Operator

Creates a time-varying SubHyGraph whose membership is determined by evaluating
a time-series predicate at each timestep. Tracks connected component lifecycle
events (birth, death, merge, split, grow, shrink).

TS -> Graph direction:
    node-level time series -> predicate evaluation -> induced subgraph

The hybrid query coordinator translates a single predicate evaluation into:
1. TimescaleDB range scan (find qualifying ts_ids by value)
2. AGE adjacency query (find connecting edges between qualifying nodes)
Both within the same PostgreSQL connection.

Usage:
    induced = PredicateInducedSubHyGraph(
        hygraph=hg, predicate="speed < 30",
        time_range=("2012-03-05 06:00", "2012-03-05 12:00"),
        step_minutes=5, node_label="Sensor"
    ).evaluate()
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any, Dict, FrozenSet, Iterator, List, Optional, Set, Tuple, TYPE_CHECKING
)

import json
import numpy as np

if TYPE_CHECKING:
    from hygraph_core.hygraph.hygraph import HyGraph




class LifecycleEventType(Enum):
    BIRTH = "birth"
    DEATH = "death"
    MERGE = "merge"
    SPLIT = "split"
    GROW = "grow"
    SHRINK = "shrink"
    CONTINUE = "continue"


@dataclass(frozen=True)
class LifecycleEvent:
    timestamp: datetime
    event_type: LifecycleEventType
    component_id: str
    member_nodes: FrozenSet[str]
    parent_ids: Tuple[str, ...] = ()
    child_ids: Tuple[str, ...] = ()
    size: int = 0

    def __repr__(self) -> str:
        return (
            f"LifecycleEvent({self.event_type.value} @ {self.timestamp}, "
            f"component={self.component_id}, size={self.size})"
        )


@dataclass
class MembershipChange:
    timestamp: datetime
    entering_nodes: Set[str]
    exiting_nodes: Set[str]
    current_members: Set[str]
    current_edges: List[Tuple[str, str]]




@dataclass
class ParsedPredicate:
    """A predicate that can reference both time-series and static node properties.

    Simple:     'speed < 30'
    Expression: 'speed / free_flow_speed < 0.5'

    The ts_property is the time-series variable (looked up from TimescaleDB).
    Any other identifiers in the expression are treated as static node properties
    (looked up from AGE node properties).
    """
    expression: str        # left-hand side, e.g. "speed / free_flow_speed"
    operator: str          # comparison operator
    threshold: float       # right-hand side numeric value
    ts_property: str       # the time-series variable name, e.g. "speed"
    referenced_props: List[str]  # all identifiers in expression

    @property
    def property_name(self) -> str:
        """Backward compat: the TS variable name."""
        return self.ts_property

    @property
    def is_simple(self) -> bool:
        """True if predicate is just 'property <op> value' with no expression."""
        return self.expression.strip() == self.ts_property

    @property
    def value(self) -> float:
        return self.threshold

    def to_sql_condition(self) -> str:
        """Only valid for simple predicates (no static property references)."""
        return f"value {self.operator} {self.threshold}"

    def evaluate(self, ts_value: float, static_props: Dict[str, float]) -> bool:
        """Evaluate the predicate given a TS value and static property values."""
        # Build variable context
        context = {self.ts_property: ts_value}
        context.update(static_props)
        try:
            lhs = eval(self.expression, {"__builtins__": {}}, context)
        except Exception:
            return False
        ops = {
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
        }
        return ops.get(self.operator, lambda a, b: False)(lhs, self.threshold)


def _extract_identifiers(expr: str) -> List[str]:
    """Extract Python identifiers from an expression string."""
    import re
    # Match word characters that aren't purely numeric
    tokens = re.findall(r'[A-Za-z_][A-Za-z0-9_]*', expr)
    return list(dict.fromkeys(tokens))  # unique, preserve order


def parse_predicate(predicate_str: str) -> ParsedPredicate:
    """Parse predicates like 'speed < 30' or 'speed / free_flow_speed < 0.5'."""
    predicate_str = predicate_str.strip()
    for op in ("<=", ">=", "==", "!=", "<", ">"):
        if op in predicate_str:
            parts = predicate_str.split(op, 1)
            expression = parts[0].strip()
            threshold = float(parts[1].strip())
            identifiers = _extract_identifiers(expression)
            if not identifiers:
                raise ValueError(f"No property found in expression: '{expression}'")
            # First identifier is assumed to be the TS property
            ts_property = identifiers[0]
            return ParsedPredicate(
                expression=expression,
                operator=op,
                threshold=threshold,
                ts_property=ts_property,
                referenced_props=identifiers,
            )
    raise ValueError(f"Cannot parse predicate: '{predicate_str}'")


@dataclass
class CompoundPredicate:
    """Multiple predicates joined by AND/OR.

    Supports compound predicate strings like:
        'speed < 30 AND occupancy > 0.8'
        'num_bikes < 5 OR num_docks < 3'
    """
    predicates: List[ParsedPredicate]
    connectors: List[str]  # 'AND' or 'OR', length = len(predicates) - 1

    @property
    def ts_properties(self) -> List[str]:
        """All unique TS variable names referenced."""
        return list(dict.fromkeys(p.ts_property for p in self.predicates))

    @property
    def all_referenced_props(self) -> List[str]:
        """All identifiers across all sub-predicates."""
        seen = {}
        for p in self.predicates:
            for r in p.referenced_props:
                seen[r] = True
        return list(seen.keys())

    @property
    def is_single(self) -> bool:
        return len(self.predicates) == 1

    @property
    def first(self) -> ParsedPredicate:
        return self.predicates[0]

    def evaluate(self, ts_values: Dict[str, float], static_props: Dict[str, float]) -> bool:
        """Evaluate compound predicate given TS values for each variable."""
        results = []
        for pred in self.predicates:
            ts_val = ts_values.get(pred.ts_property)
            if ts_val is None:
                results.append(False)
                continue
            results.append(pred.evaluate(ts_val, static_props))

        if not results:
            return False
        result = results[0]
        for i, connector in enumerate(self.connectors):
            if connector == 'OR':
                result = result or results[i + 1]
            else:  # AND
                result = result and results[i + 1]
        return result


def parse_compound_predicate(predicate_str: str) -> CompoundPredicate:
    """Parse single or compound predicates.

    Supports:
        'speed < 30'
        'speed / free_flow_speed < 0.5'
        'speed < 30 AND occupancy > 0.8'
        'num_bikes < 5 OR num_docks < 3'
    """
    import re
    # Split on AND/OR (case-insensitive, word-boundary)
    parts = re.split(r'\s+(AND|OR)\s+', predicate_str.strip(), flags=re.IGNORECASE)
    predicates = []
    connectors = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            predicates.append(parse_predicate(part.strip()))
        else:
            connectors.append(part.strip().upper())
    return CompoundPredicate(predicates=predicates, connectors=connectors)




class ComponentTracker:
    MATCH_THRESHOLD = 0.3

    def __init__(self):
        self._next_id = 0
        self._prev_components: Dict[str, FrozenSet[str]] = {}
        self.lifecycle_events: List[LifecycleEvent] = []

    def _new_id(self) -> str:
        self._next_id += 1
        return f"C{self._next_id}"

    def update(self, timestamp: datetime, current_components: List[FrozenSet[str]]) -> List[LifecycleEvent]:
        events: List[LifecycleEvent] = []
        if not self._prev_components and not current_components:
            return events

        prev_ids = list(self._prev_components.keys())
        prev_sets = [self._prev_components[pid] for pid in prev_ids]
        matched_prev: Set[str] = set()

        overlaps: Dict[Tuple[str, int], float] = {}
        for pi, pid in enumerate(prev_ids):
            for ci, curr in enumerate(current_components):
                j = self._jaccard(prev_sets[pi], curr)
                if j > self.MATCH_THRESHOLD:
                    overlaps[(pid, ci)] = j

        curr_to_prev: Dict[int, List[str]] = defaultdict(list)
        prev_to_curr: Dict[str, List[int]] = defaultdict(list)
        for (pid, ci), _ in overlaps.items():
            curr_to_prev[ci].append(pid)
            prev_to_curr[pid].append(ci)

        new_components: Dict[str, FrozenSet[str]] = {}

        for ci, curr_members in enumerate(current_components):
            prev_matches = curr_to_prev.get(ci, [])

            if len(prev_matches) == 0:
                cid = self._new_id()
                events.append(LifecycleEvent(timestamp=timestamp, event_type=LifecycleEventType.BIRTH,
                                             component_id=cid, member_nodes=curr_members, size=len(curr_members)))
                new_components[cid] = curr_members

            elif len(prev_matches) == 1:
                pid = prev_matches[0]
                prev_members = self._prev_components[pid]
                matched_prev.add(pid)
                curr_matches_for_prev = prev_to_curr.get(pid, [])

                if len(curr_matches_for_prev) > 1:
                    cid = self._new_id()
                    new_components[cid] = curr_members
                else:
                    cid = pid
                    if len(curr_members) > len(prev_members):
                        etype = LifecycleEventType.GROW
                    elif len(curr_members) < len(prev_members):
                        etype = LifecycleEventType.SHRINK
                    else:
                        etype = LifecycleEventType.CONTINUE
                    events.append(LifecycleEvent(timestamp=timestamp, event_type=etype,
                                                 component_id=cid, member_nodes=curr_members,
                                                 parent_ids=(pid,), size=len(curr_members)))
                    new_components[cid] = curr_members

            elif len(prev_matches) > 1:
                cid = self._new_id()
                for pid in prev_matches:
                    matched_prev.add(pid)
                events.append(LifecycleEvent(timestamp=timestamp, event_type=LifecycleEventType.MERGE,
                                             component_id=cid, member_nodes=curr_members,
                                             parent_ids=tuple(prev_matches), size=len(curr_members)))
                new_components[cid] = curr_members

        # SPLIT detection
        for pid in prev_ids:
            curr_matches = prev_to_curr.get(pid, [])
            if len(curr_matches) > 1:
                child_ids = []
                for ci in curr_matches:
                    for cid, members in new_components.items():
                        if members == current_components[ci]:
                            child_ids.append(cid)
                            break
                events.append(LifecycleEvent(timestamp=timestamp, event_type=LifecycleEventType.SPLIT,
                                             component_id=pid, member_nodes=self._prev_components[pid],
                                             child_ids=tuple(child_ids), size=len(self._prev_components[pid])))
                matched_prev.add(pid)

        # DEATH
        for pid in prev_ids:
            if pid not in matched_prev:
                events.append(LifecycleEvent(timestamp=timestamp, event_type=LifecycleEventType.DEATH,
                                             component_id=pid, member_nodes=self._prev_components[pid],
                                             size=len(self._prev_components[pid])))

        self._prev_components = new_components
        self.lifecycle_events.extend(events)
        return events

    @staticmethod
    def _jaccard(a: FrozenSet[str], b: FrozenSet[str]) -> float:
        if not a and not b:
            return 0.0
        return len(a & b) / len(a | b) if len(a | b) > 0 else 0.0




def compute_connected_components(node_ids: Set[str], edges: List[Tuple[str, str]]) -> List[FrozenSet[str]]:
    if not node_ids:
        return []
    adj: Dict[str, Set[str]] = defaultdict(set)
    for src, tgt in edges:
        if src in node_ids and tgt in node_ids:
            adj[src].add(tgt)
            adj[tgt].add(src)
    visited: Set[str] = set()
    components: List[FrozenSet[str]] = []
    for start in node_ids:
        if start in visited:
            continue
        component: Set[str] = set()
        queue = [start]
        while queue:
            node = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            component.add(node)
            for nb in adj.get(node, set()):
                if nb not in visited:
                    queue.append(nb)
        components.append(frozenset(component))
    return components




class PredicateInducedSubHyGraph:
    """
    A SubHyGraph whose membership is determined by evaluating a time-series
    predicate at each timestep.

    The hybrid query coordinator translates each evaluation step into:
    1. TimescaleDB scan: find ts_ids whose value satisfies the predicate
    2. ts_id -> node_uid mapping: resolve which graph nodes those ts_ids belong to
    3. AGE query: find edges connecting qualifying nodes
    """

    def __init__(
        self,
        hygraph: 'HyGraph',
        predicate: str,
        time_range: Tuple[str, str],
        step_minutes: int = 5,
        node_label: str = "Sensor",
        name: Optional[str] = None,
        time_filter: Optional[Tuple[int, int]] = None,
    ):
        """
        Args:
            time_filter: Optional (start_hour, end_hour) tuple (both inclusive).
                         If set, only timesteps whose hour falls within this range
                         are evaluated. All other timesteps are skipped.
                         Example: time_filter=(6, 9) evaluates only 06:xx-09:xx
                         across every day in the date range.
                         This lets you compare e.g. March 1-31 mornings only
                         vs June 1-30 mornings only, without evaluating nights.
        """
        self.hygraph = hygraph
        self.compound = parse_compound_predicate(predicate)
        self.predicate = self.compound.first  # backward compat
        self.predicate_str = predicate
        self.node_label = node_label
        self.name = name or f"induced_{self.predicate.property_name}"

        self.start_time = self._parse_time(time_range[0])
        self.end_time = self._parse_time(time_range[1])
        self.step = timedelta(minutes=step_minutes)
        self.time_filter = time_filter  # (start_hour, end_hour) or None

        # Mapping between ts_ids and node_uids (built at evaluate time)
        # Primary mapping (first TS variable) for backward compat
        self._ts_id_to_node_uid: Dict[str, str] = {}
        self._node_uid_to_ts_id: Dict[str, str] = {}
        # Per-variable mappings for compound predicates
        self._var_ts_to_node: Dict[str, Dict[str, str]] = {}  # var -> {ts_id: node_uid}
        self._var_node_to_ts: Dict[str, Dict[str, str]] = {}  # var -> {node_uid: ts_id}
        # Per-sensor static properties (for expression predicates)
        self._ts_id_to_static_props: Dict[str, Dict[str, float]] = {}

        # Results
        self.timestamps: List[datetime] = []
        self._membership_history: List[MembershipChange] = []
        self._lifecycle_events: List[LifecycleEvent] = []
        self._node_entry_times: Dict[str, datetime] = {}
        self._node_component_join_times: Dict[str, datetime] = {}
        self._evaluated = False

    @staticmethod
    def _parse_time(time_str: str) -> datetime:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                     "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(time_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Cannot parse time: {time_str}")



    def _build_ts_mapping(self):
        """
        Query AGE for all nodes of the target label and build the mapping
        between ts_ids (used in ts.measurements) and node_uids (used in AGE).

        Supports compound predicates referencing multiple TS variables.
        """
        # All TS variables referenced across all sub-predicates
        all_ts_vars = self.compound.ts_properties
        all_ref_props = self.compound.all_referenced_props

        # Query AGE for all nodes with this label
        nodes = self.hygraph.age.query_nodes(label=self.node_label)

        for node in nodes:
            uid = node.get("uid", "")
            props = node.get("properties", {})
            if isinstance(props, str):
                try:
                    props = json.loads(props)
                except Exception:
                    props = {}
            temporal_props = props.get("temporal_properties", {})

            # Build mapping for each TS variable
            for var_name in all_ts_vars:
                ts_id_key = f"{var_name}_ts_id"
                ts_id = props.get(ts_id_key)
                if not ts_id and var_name in temporal_props:
                    ts_id = temporal_props[var_name]
                if ts_id and uid:
                    ts_id_s, uid_s = str(ts_id), str(uid)
                    # Per-variable mapping
                    self._var_ts_to_node.setdefault(var_name, {})[ts_id_s] = uid_s
                    self._var_node_to_ts.setdefault(var_name, {})[uid_s] = ts_id_s
                    # Primary mapping (first variable, backward compat)
                    if var_name == all_ts_vars[0]:
                        self._ts_id_to_node_uid[ts_id_s] = uid_s
                        self._node_uid_to_ts_id[uid_s] = ts_id_s

            # Collect static properties for expression predicates
            static_props_needed = [p for p in all_ref_props if p not in all_ts_vars]
            if static_props_needed and uid:
                static_vals = {}
                for prop_name in static_props_needed:
                    val = props.get(prop_name)
                    if val is None:
                        val = temporal_props.get(prop_name)
                    if val is not None:
                        try:
                            static_vals[prop_name] = float(val)
                        except (ValueError, TypeError):
                            pass
                # Store by primary ts_id
                primary_ts_id = self._node_uid_to_ts_id.get(str(uid))
                if primary_ts_id:
                    self._ts_id_to_static_props[primary_ts_id] = static_vals

        print(f"  Compound predicate: '{self.predicate_str}'")
        print(f"    TS variables: {all_ts_vars}")
        for v in all_ts_vars:
            count = len(self._var_ts_to_node.get(v, {}))
            print(f"    {v}: {count} sensors mapped")

        if not self._ts_id_to_node_uid:
            print(f"  WARNING: No ts_id mapping found for variable '{all_ts_vars[0]}' "
                  f"on {self.node_label} nodes.")



    def evaluate(self) -> 'PredicateInducedSubHyGraph':
        """Evaluate predicate at each timestep, build membership history."""
        # Build mapping first
        self._build_ts_mapping()

        tracker = ComponentTracker()
        prev_members: Set[str] = set()
        in_window = True  # tracks whether we were inside the time_filter window
        all_lifecycle_events: List[LifecycleEvent] = []  # accumulate across all daily windows

        current_time = self.start_time
        total_steps = int((self.end_time - self.start_time) / self.step) + 1
        step_count = 0
        evaluated_steps = 0

        while current_time <= self.end_time:
            step_count += 1

            # ── Time filter: skip timesteps outside the allowed hours ──────
            if self.time_filter is not None:
                h_start, h_end = self.time_filter
                hour = current_time.hour
                inside = h_start <= hour <= h_end

                if not inside:
                    # We just left the window — save events and reset for next day
                    if in_window and prev_members:
                        all_lifecycle_events.extend(tracker.lifecycle_events)
                        tracker = ComponentTracker()
                        prev_members = set()
                        in_window = False
                    current_time += self.step
                    continue

                if not in_window:
                    # Re-entering the window for a new day — fresh tracker state
                    all_lifecycle_events.extend(tracker.lifecycle_events)
                    tracker = ComponentTracker()
                    prev_members = set()
                    in_window = True

            self.timestamps.append(current_time)
            evaluated_steps += 1

            # Hybrid query: TimescaleDB -> mapping -> AGE
            qualifying_nodes = self._query_qualifying_nodes(current_time)
            connecting_edges = self._query_connecting_edges(qualifying_nodes)

            entering = qualifying_nodes - prev_members
            exiting = prev_members - qualifying_nodes

            for node_id in entering:
                if node_id not in self._node_entry_times:
                    self._node_entry_times[node_id] = current_time

            if entering or exiting or not self._membership_history:
                change = MembershipChange(
                    timestamp=current_time,
                    entering_nodes=entering,
                    exiting_nodes=exiting,
                    current_members=qualifying_nodes.copy(),
                    current_edges=connecting_edges,
                )
                self._membership_history.append(change)

                components = compute_connected_components(qualifying_nodes, connecting_edges)
                events = tracker.update(current_time, components)

                for event in events:
                    if event.event_type in (LifecycleEventType.BIRTH, LifecycleEventType.GROW,
                                            LifecycleEventType.MERGE):
                        if len(event.member_nodes) >= 2:
                            for nid in event.member_nodes:
                                if nid not in self._node_component_join_times:
                                    self._node_component_join_times[nid] = current_time

            prev_members = qualifying_nodes
            current_time += self.step

            if step_count % 50 == 0:
                tf_note = f" [filter {self.time_filter[0]:02d}:00-{self.time_filter[1]:02d}:00]" \
                          if self.time_filter else ""
                print(f"    Step {step_count}/{total_steps}{tf_note}: "
                      f"{len(qualifying_nodes)} qualifying nodes, "
                      f"{len(self._lifecycle_events) + len(tracker.lifecycle_events)} events")

        # Collect any remaining events from the last window
        all_lifecycle_events.extend(tracker.lifecycle_events)
        self._lifecycle_events = all_lifecycle_events
        self._evaluated = True

        tf_desc = f" (time_filter {self.time_filter[0]:02d}:00-{self.time_filter[1]:02d}:00," \
                  f" {evaluated_steps} evaluated of {total_steps} total steps)" \
                  if self.time_filter else ""
        print(f"  Evaluation complete: {len(self.timestamps)} timesteps{tf_desc}, "
              f"{len(self._membership_history)} changes, "
              f"{len(self._lifecycle_events)} lifecycle events")
        return self

    def _query_qualifying_nodes(self, timestamp: datetime) -> Set[str]:
        """
        Hybrid query step 1+2: find nodes whose TS property satisfies the predicate.

        1. Query ts.measurements for ts_ids where value satisfies predicate
        2. Map ts_ids back to node_uids via the pre-built mapping

        When normalize_by is set, the predicate is evaluated as:
            value / normalize_value <op> threshold
        following the velocity-ratio formulation from Li et al. (PNAS 2015).
        """
        ts_iso = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # All ts_ids for this variable
        target_ts_ids = list(self._ts_id_to_node_uid.keys())
        if not target_ts_ids:
            return set()

        # Build IN clause
        ts_id_list = ", ".join(f"'{tid}'" for tid in target_ts_ids)

        qualifying_node_uids: Set[str] = set()

        if self.compound.is_single and self.predicate.is_simple:
            # Simple single predicate (e.g., 'speed < 30'): filter directly in SQL
            sql = f"""
                SELECT entity_uid
                FROM ts.measurements
                WHERE entity_uid IN ({ts_id_list})
                  AND variable = '{self.predicate.property_name}'
                  AND ts = '{ts_iso}'
                  AND value != 0
                  AND {self.predicate.to_sql_condition()}
            """
            try:
                with self.hygraph.db.conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        for row in cur.fetchall():
                            ts_id = str(row["entity_uid"])
                            node_uid = self._ts_id_to_node_uid.get(ts_id)
                            if node_uid:
                                qualifying_node_uids.add(node_uid)
            except Exception as e:
                print(f"  WARNING: Measurement query failed: {e}")
        else:
            # Compound or expression predicate: fetch all TS variables, evaluate in Python
            all_ts_vars = self.compound.ts_properties

            # Fetch values for all TS variables at this timestep
            # node_uid -> {var_name: value}
            node_values: Dict[str, Dict[str, float]] = {}

            for var_name in all_ts_vars:
                var_ts_ids = list(self._var_ts_to_node.get(var_name, {}).keys())
                if not var_ts_ids:
                    continue
                var_ts_id_list = ", ".join(f"'{tid}'" for tid in var_ts_ids)
                sql = f"""
                    SELECT entity_uid, value
                    FROM ts.measurements
                    WHERE entity_uid IN ({var_ts_id_list})
                      AND variable = '{var_name}'
                      AND ts = '{ts_iso}'
                      AND value != 0
                """
                try:
                    with self.hygraph.db.conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(sql)
                            for row in cur.fetchall():
                                ts_id = str(row["entity_uid"])
                                raw_value = row["value"]
                                if raw_value is None:
                                    continue
                                node_uid = self._var_ts_to_node.get(var_name, {}).get(ts_id)
                                if node_uid:
                                    node_values.setdefault(node_uid, {})[var_name] = float(raw_value)
                except Exception as e:
                    print(f"  WARNING: Measurement query for '{var_name}' failed: {e}")

            # Evaluate compound predicate per node
            for node_uid, ts_vals in node_values.items():
                primary_ts_id = self._node_uid_to_ts_id.get(node_uid, '')
                static_props = self._ts_id_to_static_props.get(primary_ts_id, {})
                if self.compound.evaluate(ts_vals, static_props):
                    qualifying_node_uids.add(node_uid)

        return qualifying_node_uids

    def _query_connecting_edges(self, node_uids: Set[str]) -> List[Tuple[str, str]]:
        """
        Hybrid query step 3: find edges in AGE connecting qualifying nodes.
        """
        if len(node_uids) < 2:
            return []

        # Build Cypher query to find edges between qualifying nodes
        uid_list = ", ".join(f"'{uid}'" for uid in node_uids)

        try:
            results = self.hygraph.age.cypher_multi(
                f"""
                MATCH (a:{self.node_label})-[e]->(b:{self.node_label})
                WHERE a.uid IN [{uid_list}] AND b.uid IN [{uid_list}]
                RETURN a.uid, b.uid
                """,
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
            print(f"  WARNING: Edge query failed: {e}")
            return []



    def membership_changes(self) -> Iterator[MembershipChange]:
        if not self._evaluated:
            raise RuntimeError("Call evaluate() first")
        yield from self._membership_history

    @property
    def lifecycle_events(self) -> List[LifecycleEvent]:
        if not self._evaluated:
            raise RuntimeError("Call evaluate() first")
        return self._lifecycle_events

    @property
    def node_entry_times(self) -> Dict[str, datetime]:
        return self._node_entry_times

    @property
    def structural_transition_times(self) -> Dict[str, datetime]:
        """
        Node_id -> timestamp when it first joined a connected component (>= 2 members).
        Used by HyGraphDiff for the Change Propagation Graph.
        """
        return self._node_component_join_times

    def members_at(self, timestamp: datetime) -> Set[str]:
        if not self._evaluated:
            raise RuntimeError("Call evaluate() first")
        result: Set[str] = set()
        for change in self._membership_history:
            if change.timestamp <= timestamp:
                result = change.current_members
            else:
                break
        return result

    def edges_at(self, timestamp: datetime) -> List[Tuple[str, str]]:
        if not self._evaluated:
            raise RuntimeError("Call evaluate() first")
        result: List[Tuple[str, str]] = []
        for change in self._membership_history:
            if change.timestamp <= timestamp:
                result = change.current_edges
            else:
                break
        return result

    def components_at(self, timestamp: datetime) -> List[FrozenSet[str]]:
        return compute_connected_components(self.members_at(timestamp), self.edges_at(timestamp))

    def summary(self) -> Dict[str, Any]:
        if not self._evaluated:
            raise RuntimeError("Call evaluate() first")
        all_members = set()
        max_size = 0
        for change in self._membership_history:
            all_members.update(change.current_members)
            max_size = max(max_size, len(change.current_members))
        event_counts = defaultdict(int)
        for event in self._lifecycle_events:
            event_counts[event.event_type.value] += 1
        return {
            "predicate": self.predicate_str,
            "time_range": (self.start_time.isoformat(), self.end_time.isoformat()),
            "total_timesteps": len(self.timestamps),
            "change_timesteps": len(self._membership_history),
            "unique_members": len(all_members),
            "max_simultaneous_members": max_size,
            "lifecycle_events": dict(event_counts),
            "total_lifecycle_events": len(self._lifecycle_events),
            "ts_mapping_size": len(self._ts_id_to_node_uid),
        }

    def __repr__(self) -> str:
        status = "evaluated" if self._evaluated else "not evaluated"
        return (
            f"PredicateInducedSubHyGraph("
            f"predicate='{self.predicate_str}', "
            f"{status}, "
            f"timesteps={len(self.timestamps)})"
        )
