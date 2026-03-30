# File: hygraph_core/operators/__init__.py
"""
HyGraph Operators Module

Exports:
- Snapshot, SnapshotNode, SnapshotEdge: Temporal snapshots of graph state
- SnapshotSequence: Ordered collection of snapshots
- TSGen: Time series generator from snapshots (original, SnapshotSequence-based)
- TSGenFromInduced: Signal transformation on predicate-induced SubHyGraphs
- PredicateInducedSubHyGraph: Time-varying SubHyGraph with component lifecycle
- HyGraphDiff: Diff between two snapshots (original)
- HyGraphDiffResult: Hybrid change descriptor + Change Propagation Graph
- ChangePropagationGraph: Directed graph of change spread
- Graph metrics: density, connected_components, etc.
"""

from hygraph_core.operators.temporal_snapshot import Snapshot, SnapshotNode, SnapshotEdge
from hygraph_core.operators.snapshotSequence import SnapshotSequence
from hygraph_core.operators.TSGen import TSGen
from hygraph_core.operators.hygraphDiff import (
    HyGraphDiff, 
    HyGraphDiffNode, 
    HyGraphDiffEdge,
    QueryableDiffNode,
    QueryableDiffEdge,
    DiffQueryBuilder,
    PropertyChange,
    HyGraphIntervalDiff,
    TSComparison,
    QueryableIntervalDiffNode,
    IntervalDiffQueryBuilder,
    compute_ts_comparison
)

# New VLDB demo operators
from hygraph_core.operators.predicate_induced import (
    PredicateInducedSubHyGraph,
    LifecycleEvent,
    LifecycleEventType,
    MembershipChange,
    ComponentTracker,
    compute_connected_components as compute_cc,
)

from hygraph_core.operators.tsgen_induced import (
    TSGenFromInduced,
    SignalAggregatingMetric,
    METRIC_REGISTRY,
)

from hygraph_core.operators.diff_result import (
    HyGraphDiffResult,
    HybridChangeDescriptor,
    AnnotatedEntity,
    DiffSummary,
)

from hygraph_core.operators.change_propagation import (
    ChangePropagationGraph,
    PropagationEdge,
    PropagationPath,
)

# Graph metrics
from hygraph_core.operators.graph_metrics import (
    density,
    connected_components,
    degree_distribution,
    ComponentResult
)

__all__ = [
    # Snapshot classes
    "Snapshot", "SnapshotNode", "SnapshotEdge", "SnapshotSequence",
    
    # Original operators
    "TSGen",
    "HyGraphDiff", "HyGraphDiffNode", "HyGraphDiffEdge",
    "QueryableDiffNode", "QueryableDiffEdge", "DiffQueryBuilder", "PropertyChange",
    "HyGraphIntervalDiff", "TSComparison",
    "QueryableIntervalDiffNode", "IntervalDiffQueryBuilder", "compute_ts_comparison",
    
    # New: Predicate-induced SubHyGraph
    "PredicateInducedSubHyGraph",
    "LifecycleEvent", "LifecycleEventType", "MembershipChange",
    "ComponentTracker",
    
    # New: TSGen signal transformation
    "TSGenFromInduced", "SignalAggregatingMetric", "METRIC_REGISTRY",
    
    # New: HyGraphDiff with hybrid change descriptors
    "HyGraphDiffResult", "HybridChangeDescriptor", "AnnotatedEntity", "DiffSummary",
    
    # New: Change Propagation Graph
    "ChangePropagationGraph", "PropagationEdge", "PropagationPath",
    
    # Graph metrics
    "density", "connected_components", "degree_distribution", "ComponentResult",
]
