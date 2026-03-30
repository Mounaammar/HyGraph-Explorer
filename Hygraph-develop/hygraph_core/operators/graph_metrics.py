# File: hygraph_core/operators/graph_metrics.py
"""
Graph metrics that can be computed on a single snapshot.

These can be used:
1. Directly: hg.snapshot(...).density(), hg.snapshot(...).connected_components()
2. Via TSGen: ts.global_.graph.density() (computes over sequence of snapshots)
"""
from collections import defaultdict
from typing import List, Set, Optional, Dict, Any, Literal
from dataclasses import dataclass


#========================================
# Helper function
#========================================
def _nodes_edges_snapshot(snapshot:Any, label: Optional[str] = None):
    """
    Get nodes and edges from a snapshot, optionally filtered by label.
    
    When label is specified:
    - Only nodes with that label are returned
    - Only edges where BOTH source and target are in the filtered node set
    
    Note: Converts all IDs to strings for consistent comparison.
    """
    nodes:List = snapshot.get_all_nodes()
    edges = list(snapshot.get_all_edges())
    
    if label:
        nodes = [n for n in nodes if n.label == label]
        # Use string conversion for consistent comparison
        node_ids = {str(n.oid) for n in nodes}
        edges = [e for e in edges if str(e.source) in node_ids and str(e.target) in node_ids]
    
    return nodes, edges


#================================
@dataclass
class ComponentResult:
    """Result of connected components computation."""
    components: List[Set[str]]  # List of node ID sets
    
    @property
    def count(self) -> int:
        """Number of components."""
        return len(self.components)
    
    @property
    def sizes(self) -> List[int]:
        """Sizes of each component, sorted descending."""
        return sorted([len(c) for c in self.components], reverse=True)
    
    @property
    def biggest(self) -> int:
        """Size of the largest component."""
        return max(len(c) for c in self.components) if self.components else 0
    
    @property
    def smallest(self) -> int:
        """Size of the smallest component."""
        return min(len(c) for c in self.components) if self.components else 0
    
    def get_component(self, node_id: str) -> Optional[Set[str]]:
        """Get the component containing a specific node."""
        for comp in self.components:
            if node_id in comp:
                return comp
        return None


def density(snapshot:Any, directed: bool = True) -> float:
    """
    Compute graph density.
    For directed graphs: density = m / (n * (n - 1))
    For undirected graphs: density = 2m / (n * (n - 1))
    Returns:
    Density value between 0 and 1
    """

    n = snapshot.count_nodes()
    m = snapshot.count_edges()

    if n <= 1:
        return 0.0

    max_edges = n * (n - 1)
    if not directed:
        max_edges = max_edges // 2
    
    return m / max_edges if max_edges > 0 else 0.0


def connected_components(
    snapshot:Any,
    directed: bool = True,
    label:Optional[str] = None,
) -> ComponentResult:
    """
    Find connected components.

    For directed graphs: finds strongly connected components (Kosaraju's algorithm)
    For undirected graphs: finds weakly connected components (BFS)
    """
    nodes, edges = _nodes_edges_snapshot(snapshot, label)
    node_ids = {n.oid for n in nodes}
    
    if not node_ids:
        return ComponentResult(components=[])
    
    if directed:
        return _strongly_connected_components(node_ids,edges)
    else:
        return _weakly_connected_components(node_ids,edges)


def _strongly_connected_components(node_ids: Set[Any], edges: List) -> ComponentResult:
    """
    Find strongly connected components using Kosaraju's algorithm.
    
    Reference: https://cp-algorithms.com/graph/strongly-connected-components.html
    """

    # Build adjacency lists
    adj = {nid: set() for nid in node_ids}
    rev = {nid: set() for nid in node_ids}
    
    for e in edges:
        if e.source in node_ids and e.target in node_ids:
            adj[e.source].add(e.target)
            rev[e.target].add(e.source)
    
    # First DFS - get finish order
    visited = set()
    order = []
    
    def dfs1(u):
        visited.add(u)
        for v in adj[u]:
            if v not in visited:
                dfs1(v)
        order.append(u)
    
    for u in node_ids:
        if u not in visited:
            dfs1(u)
    
    # Second DFS - find components on reversed graph
    visited.clear()
    components = []
    
    def dfs2(u, comp):
        visited.add(u)
        comp.add(u)
        for v in rev[u]:
            if v not in visited:
                dfs2(v, comp)
    
    for u in reversed(order):
        if u not in visited:
            comp = set()
            dfs2(u, comp)
            components.append(comp)
    
    return ComponentResult(components=components)


def _weakly_connected_components(node_ids: Set[Any], edges: List) -> ComponentResult:
    """
    Find weakly connected components using BFS.
    Treats graph as undirected.
    """
    # Build undirected adjacency list
    adj = {nid: set() for nid in node_ids}

    for e in edges:
        if e.source in node_ids and e.target in node_ids:
            adj[e.source].add(e.target)
            adj[e.target].add(e.source)

    visited = set()
    components = []

    for start in node_ids:
        if start in visited:
            continue

        # BFS from start
        component = set()
        queue = [start]

        while queue:
            node = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            component.add(node)

            for neighbor in adj[node]:
                if neighbor not in visited:
                    queue.append(neighbor)

        components.append(component)

    return ComponentResult(components=components)


def degree_distribution(
    snapshot:Any,
    label:Optional[str] = None,
    direction: Literal["in", "out", "both"] = "both",
) -> Dict[int, int]:
    """
    Compute degree distribution.
    @:param     nodes: List of node objects
    @:param     edges: List of edge objects
    @:param     direction: "in", "out", or "both"
    
    Returns: degree and count of nodes with that degree
    """
    distribution = {}
    nodes,edges=_nodes_edges_snapshot(snapshot,label)
    for node in nodes:
        if direction == "out":
            degree = sum(1 for e in edges if e.source == node.oid)
        elif direction == "in":
            degree = sum(1 for e in edges if e.target == node.oid)
        else:
            degree = sum(1 for e in edges if e.source == node.oid or e.target == node.oid)
        
        distribution[degree] = distribution.get(degree, 0) + 1
    
    return distribution

def degree_metric(snapshot:Any, node_id:Optional[Any]=None, label:Optional[str]=None, weight:Optional[Any]=None, direction: Literal["in", "out", "both"] = "both") -> Dict[str, list]:
    """
    Compute degree for nodes in a snapshot.
    
    Args:
        snapshot: Snapshot object
        node_id: If provided, compute for specific node only
        label: Filter nodes by label
        weight: Edge property to use as weight (sum instead of count)
        direction: "in", "out", or "both"
    
    Returns:
        Dict mapping node_id to list of (timestamp, degree) tuples.
        Each call returns ONE tuple per node (for this single snapshot).
    """
    ts = snapshot.when
    nodes, edges = _nodes_edges_snapshot(snapshot, label)
    series: Dict[str, list] = defaultdict(list)
    
    # Filter nodes by label if specified
    if label:
        nodes = [n for n in nodes if n.label == label]
    
    # Ensure all IDs are strings for consistent comparison
    # This fixes type mismatch between node.oid and edge.source/target
    present_ids = {str(n.oid) for n in nodes}
    
    # Create degree map with string keys
    degree_map: Dict[str, float] = {nid: 0.0 for nid in present_ids}
    
    if weight is None:
        # Unweighted: count edges
        for e in edges:
            src = str(e.source)
            tgt = str(e.target)
            if direction in ("both", "out") and src in degree_map:
                degree_map[src] += 1
            if direction in ("both", "in") and tgt in degree_map:
                degree_map[tgt] += 1
    else:
        # Weighted: sum edge property value
        for e in edges:
            w = e.properties.get(weight)
            if not isinstance(w, (int, float)):
                continue
            src = str(e.source)
            tgt = str(e.target)
            if direction in ("both", "out") and src in degree_map:
                degree_map[src] += float(w)
            if direction in ("both", "in") and tgt in degree_map:
                degree_map[tgt] += float(w)
    
    # Build results
    if node_id is not None:
        # Single node - convert to string for lookup
        node_id_str = str(node_id)
        if node_id_str in present_ids:
            series[node_id_str].append((ts, degree_map.get(node_id_str, 0.0)))
        else:
            # Node not present in this snapshot
            series[node_id_str].append((ts, None))
    else:
        # All nodes
        for nid in present_ids:
            series[nid].append((ts, degree_map.get(nid, 0.0)))
    
    return series
