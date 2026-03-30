

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class PropagationEdge:
    """A directed edge in the Change Propagation Graph."""
    source: str
    target: str
    delay_days: float
    source_timestamp: datetime
    target_timestamp: datetime
    
    def __repr__(self) -> str:
        # Show in hours if delay < 2 days, otherwise in days
        if self.delay_days < 2.0:
            delay_str = f"{self.delay_days * 24:.1f}h"
        else:
            delay_str = f"{self.delay_days:.1f}d"
        return f"PropagationEdge({self.source} -> {self.target}, delay={delay_str})"


@dataclass
class PropagationPath:
    """A path through the Change Propagation Graph."""
    nodes: List[str]
    edges: List[PropagationEdge]
    total_delay_days: float
    
    @property
    def length(self) -> int:
        return len(self.edges)
    
    def __repr__(self) -> str:
        if self.edges:
            # Show per-hop delays: A -3h-> B -21h-> C
            parts = [self.nodes[0]]
            for edge in self.edges:
                d = edge.delay_days
                hop = f"{d*24:.1f}h" if d < 2.0 else f"{d:.1f}d"
                parts.append(f"-{hop}->")
                parts.append(edge.target)
            path_str = " ".join(parts)
        else:
            path_str = " -> ".join(self.nodes)
        total = self.total_delay_days
        total_str = f"{total*24:.1f}h" if total < 2.0 else f"{total:.1f}d"
        return f"PropagationPath({path_str}) total={total_str}"


class ChangePropagationGraph:
    """
    Directed graph capturing how behavioral changes spread through the network.
    
    Nodes: ADDED entities (entities that entered the predicate set between periods)
    Edges: Directed from earlier-transitioning to later-transitioning adjacent entities
    """
    
    def __init__(
        self,
        added_nodes: List[str],
        transition_times: Dict[str, datetime],
        adjacency: List[Tuple[str, str]],
        delta_max_days: float = 30.0,
    ):
        self._added_set = set(added_nodes)
        self._transition_times = {
            nid: ts for nid, ts in transition_times.items()
            if nid in self._added_set
        }
        self._adjacency = adjacency
        self._delta_max = timedelta(days=delta_max_days)
        
        self.nodes: Set[str] = set()
        self.edges: List[PropagationEdge] = []
        self._adj_out: Dict[str, List[PropagationEdge]] = defaultdict(list)
        self._adj_in: Dict[str, List[PropagationEdge]] = defaultdict(list)
        self._built = False
    
    def build(self) -> 'ChangePropagationGraph':
        """Construct the Change Propagation Graph."""
        self.nodes = {
            nid for nid in self._added_set
            if nid in self._transition_times
        }
        
        if not self.nodes:
            self._built = True
            return self
        
        seen_edges: Set[Tuple[str, str]] = set()
        
        for src, tgt in self._adjacency:
            src_s, tgt_s = str(src), str(tgt)
            
            for u, v in [(src_s, tgt_s), (tgt_s, src_s)]:
                if u in self.nodes and v in self.nodes and (u, v) not in seen_edges:
                    tau_u = self._transition_times.get(u)
                    tau_v = self._transition_times.get(v)
                    
                    if tau_u is None or tau_v is None:
                        continue
                    
                    if tau_u < tau_v:
                        delay = tau_v - tau_u
                        if delay <= self._delta_max:
                            edge = PropagationEdge(
                                source=u, target=v,
                                delay_days=delay.total_seconds() / 86400,
                                source_timestamp=tau_u,
                                target_timestamp=tau_v,
                            )
                            self.edges.append(edge)
                            self._adj_out[u].append(edge)
                            self._adj_in[v].append(edge)
                            seen_edges.add((u, v))
        
        self._built = True
        return self
    
    @property
    def roots(self) -> List[str]:
        """Change originators — nodes with in-degree 0, sorted by transition time."""
        if not self._built:
            raise RuntimeError("Call build() first")
        root_nodes = [
            nid for nid in self.nodes
            if nid not in self._adj_in or len(self._adj_in[nid]) == 0
        ]
        return sorted(root_nodes, key=lambda n: self._transition_times.get(n, datetime.max))
    
    @property
    def leaves(self) -> List[str]:
        """Most recently affected nodes — out-degree 0, sorted by transition time desc."""
        if not self._built:
            raise RuntimeError("Call build() first")
        leaf_nodes = [
            nid for nid in self.nodes
            if nid not in self._adj_out or len(self._adj_out[nid]) == 0
        ]
        return sorted(leaf_nodes, key=lambda n: self._transition_times.get(n, datetime.min), reverse=True)
    
    @property
    def max_depth(self) -> int:
        """Length of the longest propagation path (in hops)."""
        if not self._built or not self.edges:
            return 0
        max_d = 0
        for root in self.roots:
            depths = {root: 0}
            queue = deque([root])
            while queue:
                node = queue.popleft()
                for edge in self._adj_out.get(node, []):
                    if edge.target not in depths:
                        depths[edge.target] = depths[node] + 1
                        max_d = max(max_d, depths[edge.target])
                        queue.append(edge.target)
        return max_d
    
    @property
    def avg_delay_days(self) -> float:
        """Average propagation delay across all edges (in days)."""
        if not self.edges:
            return 0.0
        return sum(e.delay_days for e in self.edges) / len(self.edges)
    
    def get_paths_from(self, root_id: str) -> List[PropagationPath]:
        """Get all propagation paths originating from a specific root."""
        if not self._built:
            raise RuntimeError("Call build() first")
        paths: List[PropagationPath] = []
        
        def dfs(node, path_nodes, path_edges):
            out_edges = self._adj_out.get(node, [])
            if not out_edges:
                if path_edges:
                    total_delay = sum(e.delay_days for e in path_edges)
                    paths.append(PropagationPath(
                        nodes=list(path_nodes), edges=list(path_edges),
                        total_delay_days=total_delay,
                    ))
                return
            for edge in out_edges:
                if edge.target not in path_nodes:
                    path_nodes.append(edge.target)
                    path_edges.append(edge)
                    dfs(edge.target, path_nodes, path_edges)
                    path_nodes.pop()
                    path_edges.pop()
        
        dfs(root_id, [root_id], [])
        paths.sort(key=lambda p: p.total_delay_days)
        return paths
    
    def get_propagation_timeline(self) -> List[Tuple[datetime, str]]:
        """Timeline of all transitions, sorted chronologically."""
        timeline = [
            (ts, nid) for nid, ts in self._transition_times.items()
            if nid in self.nodes
        ]
        timeline.sort(key=lambda x: x[0])
        return timeline
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize for API response."""
        return {
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "roots": self.roots,
            "leaves": self.leaves,
            "max_depth": self.max_depth,
            "avg_delay_days": round(self.avg_delay_days, 2),
            "propagation_edges": [
                {
                    "source": e.source, "target": e.target,
                    "delay_days": round(e.delay_days, 2),
                    "source_timestamp": e.source_timestamp.isoformat(),
                    "target_timestamp": e.target_timestamp.isoformat(),
                }
                for e in self.edges
            ],
            "timeline": [
                {"timestamp": ts.isoformat(), "node_id": nid}
                for ts, nid in self.get_propagation_timeline()
            ],
        }
    
    def summary(self) -> Dict[str, Any]:
        return {
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "root_count": len(self.roots),
            "max_depth": self.max_depth,
            "avg_delay_days": round(self.avg_delay_days, 2),
            "earliest_transition": min(self._transition_times.values()).isoformat() if self._transition_times else None,
            "latest_transition": max(self._transition_times.values()).isoformat() if self._transition_times else None,
        }
    
    def __repr__(self) -> str:
        if not self._built:
            return "ChangePropagationGraph(not built)"
        avg = self.avg_delay_days
        avg_str = f"{avg * 24:.1f}h" if avg < 2.0 else f"{avg:.1f}d"
        return (
            f"ChangePropagationGraph(nodes={len(self.nodes)}, edges={len(self.edges)}, "
            f"roots={len(self.roots)}, max_depth={self.max_depth}, "
            f"avg_delay={avg_str})"
        )
