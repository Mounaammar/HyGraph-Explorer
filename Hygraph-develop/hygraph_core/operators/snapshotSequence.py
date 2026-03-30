"""
Snapshot Sequence - Ordered collection of snapshots for temporal analysis.

Provides:
- Iteration over snapshots
- Filtering (window, slice, head, tail)
- TSGen factory for time series generation
- Serialization methods for API responses
"""

from __future__ import annotations
from datetime import timedelta
from typing import List, Iterator, Optional, Dict, Any, TYPE_CHECKING

from hygraph_core.operators.temporal_snapshot import Snapshot
from hygraph_core.model.base import format_timedelta

if TYPE_CHECKING:
    from hygraph_core.operators.TSGen import TSGen


class SnapshotSequence:
    """
    Container for ordered snapshots enabling temporal analysis -> Used by TSGEN to produce time series data
    Example
        sequence = hg.snapshot_sequence(
            start="2024-05-01",
            end="2024-05-31",
            every=timedelta(days=1)
        )
        # Generate time series from snapshots
        ts = sequence.tsgen()
        node_count = ts.global_.nodes.count(label="Station")
    """
    
    def __init__(
        self, 
        snapshots: List[Snapshot], 
        every: Optional[timedelta] = None, 
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> None:
        """
        Initialize snapshot sequence.

        :param     snapshots: Ordered list of Snapshot objects
        :param     every: Time interval between snapshots (timedelta)
        :param     start_time: Start timestamp of the sequence
        :param     end_time: End timestamp of the sequence
        """
        self.snapshots = snapshots
        self.every = every
        self.start_time = start_time
        self.end_time = end_time

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def granularity(self) -> Optional[str]:
        """Return granularity as human-readable string (e.g., '1D', '1H')."""
        if self.every is None:
            return None
        return format_timedelta(self.every)

    @property
    def timestamps(self) -> List[str]:
        """Return list of timestamps of all snapshots."""
        return [s.when for s in self.snapshots]

    @property
    def first_timestamp(self) -> Optional[str]:
        """Return timestamp of first snapshot."""
        return self.snapshots[0].when if self.snapshots else None

    @property
    def last_timestamp(self) -> Optional[str]:
        """Return timestamp of last snapshot."""
        return self.snapshots[-1].when if self.snapshots else None

    @property
    def avg_node_count(self) -> float:
        """Return average number of nodes over all snapshots."""
        if not self.snapshots:
            return 0.0
        total = sum(s.count_nodes() for s in self.snapshots)
        return total / len(self.snapshots)

    @property
    def avg_edge_count(self) -> float:
        """Return average number of edges over all snapshots."""
        if not self.snapshots:
            return 0.0
        total = sum(s.count_edges() for s in self.snapshots)
        return total / len(self.snapshots)

    @property
    def count_snapshots(self) -> int:
        return len(self.snapshots)
    # =========================================================================
    # Collection Methods
    # =========================================================================

    def __iter__(self) -> Iterator[Snapshot]:
        return iter(self.snapshots)

    def __len__(self) -> int:
        return len(self.snapshots)

    def __getitem__(self, item: int) -> Snapshot:
        return self.snapshots[item]

    def __setitem__(self, key: int, value: Snapshot) -> None:
        self.snapshots[key] = value

    def __delitem__(self, key: int) -> None:
        del self.snapshots[key]

    # =========================================================================
    # Serialization Methods
    # =========================================================================

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for internal serialization.
        Includes full snapshot data.
        """
        return {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'granularity': self.granularity,
            'count': len(self.snapshots),
            'snapshots': [s.to_dict() for s in self.snapshots],
        }

    def to_api_response_summary(self) -> Dict[str, Any]:
        """
        Convert to API response with summary only (FAST).
        
        Returns metadata and per-snapshot counts without full graph data.
        Good for overview/timeline visualization.
        """
        summaries = []
        for snap in self.snapshots:
            summaries.append({
                'timestamp': snap.when,
                'node_count': snap.count_nodes(),
                'edge_count': snap.count_edges(),
                'ts_count': snap.count_timeseries()
            })
        
        return {
            'metadata': {
                'start_time': self.start_time,
                'end_time': self.end_time,
                'granularity': self.granularity,
                'count': len(self.snapshots),
                'avg_node_count': self.avg_node_count,
                'avg_edge_count': self.avg_edge_count,
            },
            'snapshots': summaries,
        }

    def to_api_response_at(self, index: int) -> Dict[str, Any]:
        """
        Get API response for a single snapshot by index.
        
        Used for lazy loading: frontend fetches sequence summary first,
        then requests individual snapshots as needed.
        
        Args:
            index: 0-based index of snapshot to retrieve
        """
        if index < 0 or index >= len(self.snapshots):
            raise IndexError(f"Snapshot index {index} out of range (0-{len(self.snapshots)-1})")
        
        snap = self.snapshots[index]
        return {
            'index': index,
            'total': len(self.snapshots),
            'snapshot': snap.to_api_response(include_graph=True),
        }

    def to_api_response_full(self) -> Dict[str, Any]:
        """
        Convert to API response with all snapshot data (SLOW).
        
        Use only when full data is needed. Consider using summary + lazy loading instead.
        """
        return {
            'metadata': {
                'start_time': self.start_time,
                'end_time': self.end_time,
                'granularity': self.granularity,
                'count': self.count_snapshots,
            },
            'snapshots': [s.to_api_response(include_graph=True) for s in self.snapshots],
        }

    # =========================================================================
    # TSGen Factory Method
    # =========================================================================

    def tsgen(self) -> 'TSGen':
        """
        Create TSGen for analyzing this snapshot sequence.
        
        TSGen computes metrics from graph evolution, producing 
        discrete time series from structural and property changes.
        
        Returns:
            TSGen instance configured with this snapshot sequence
        """
        from hygraph_core.operators.TSGen import TSGen
        return TSGen(self)

    # =========================================================================
    # Filtering Methods
    # =========================================================================

    def window(self, start: str, end: str) -> 'SnapshotSequence':
        """Filter snapshots to a time window."""
        filtered = [s for s in self.snapshots if start <= s.when <= end]
        return SnapshotSequence(
            snapshots=filtered,
            every=self.every,
            start_time=start,
            end_time=end
        )

    def slice(self, start_idx: int, end_idx: int) -> 'SnapshotSequence':
        """Slice snapshots by index."""
        sliced = self.snapshots[start_idx:end_idx]
        return SnapshotSequence(
            snapshots=sliced,
            every=self.every,
            start_time=sliced[0].when if sliced else None,
            end_time=sliced[-1].when if sliced else None
        )

    def head(self, n: int = 5) -> 'SnapshotSequence':
        """Return first n snapshots."""
        return self.slice(0, n)

    def tail(self, n: int = 5) -> 'SnapshotSequence':
        """Return last n snapshots."""
        return self.slice(-n, len(self.snapshots))

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def __repr__(self):
        return (
            f"SnapshotSequence(\n"
            f"  count={len(self)},\n"
            f"  start={self.first_timestamp},\n"
            f"  end={self.last_timestamp},\n"
            f"  every={self.granularity},\n"
            f"  avg_nodes={self.avg_node_count:.1f},\n"
            f"  avg_edges={self.avg_edge_count:.1f}\n"
            f")"
        )
