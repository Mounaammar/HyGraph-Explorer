"""
HyGraph Storage - Abstract Backend Interface

Defines the contract that all storage backends must implement.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple, Callable, Literal

# Import from model
try:
    from hygraph_core.model.nodes import PGNode, TSNode
    from hygraph_core.model.edges import PGEdge, TSEdge
    from hygraph_core.model.timeseries import TimeSeries
except ImportError:
    # For standalone development
    from typing import Any as PGNode
    from typing import Any as TSNode
    from typing import Any as PGEdge
    from typing import Any as TSEdge
    from typing import Any as TimeSeries

# Type aliases
Node = PGNode | TSNode
Edge = PGEdge | TSEdge


class StorageBackend(ABC):
    """
    Abstract interface for HyGraph storage backends.

    All storage backends (Memory, AGE, TimescaleDB) must implement this interface.
    This allows HyGraph to be storage-agnostic.

    Design principles:
    - Simple, atomic operations
    - Type-safe (uses typed model classes)
    - Transaction support (begin, commit, rollback)
    - Efficient queries
    """

    # =========================================================================
    # Connection Management
    # =========================================================================

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to backend"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to backend"""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if connected"""
        pass

    # =========================================================================
    # Transaction Management
    # =========================================================================

    @abstractmethod
    def begin_transaction(self) -> None:
        """Begin a transaction"""
        pass

    @abstractmethod
    def commit(self) -> None:
        """Commit current transaction"""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Rollback current transaction"""
        pass

    # =========================================================================
    # Node Operations
    # =========================================================================

    @abstractmethod
    def insert_node(self, node: Node) -> None:
        """
        Insert a node.

        Args:
            node: PGNode or TSNode to insert

        Raises:
            ValueError: If node already exists
        """
        pass

    @abstractmethod
    def get_node(self, oid: str) -> Optional[Node]:
        """
        Get a node by ID.

        Args:
            oid: Node ID

        Returns:
            Node if found, None otherwise
        """
        pass

    @abstractmethod
    def update_node(self, oid: str, updates: Dict[str, Any]) -> None:
        """
        Update node properties.

        Args:
            oid: Node ID
            updates: Dict of property updates

        Raises:
            ValueError: If node doesn't exist
        """
        pass

    @abstractmethod
    def delete_node(self, oid: str, hard: bool = False) -> None:
        """
        Delete a node.

        Args:
            oid: Node ID
            hard: If True, remove completely. If False, soft delete (set end_time)

        Raises:
            ValueError: If node doesn't exist
        """
        pass

    @abstractmethod
    def node_exists(self, oid: str) -> bool:
        """
        Check if node exists.

        Args:
            oid: Node ID

        Returns:
            True if exists
        """
        pass

    # =========================================================================
    # Edge Operations
    # =========================================================================

    @abstractmethod
    def insert_edge(self, edge: Edge) -> None:
        """
        Insert an edges.

        Args:
            edge: PGEdge or TSEdge to insert

        Raises:
            ValueError: If edges already exists or nodes don't exist
        """
        pass

    @abstractmethod
    def get_edge(self, oid: str) -> Optional[Edge]:
        """
        Get an edges by ID.

        Args:
            oid: Edge ID

        Returns:
            Edge if found, None otherwise
        """
        pass

    @abstractmethod
    def update_edge(self, oid: str, updates: Dict[str, Any]) -> None:
        """
        Update edges properties.

        Args:
            oid: Edge ID
            updates: Dict of property updates

        Raises:
            ValueError: If edges doesn't exist
        """
        pass

    @abstractmethod
    def delete_edge(self, oid: str, hard: bool = False) -> None:
        """
        Delete an edges.

        Args:
            oid: Edge ID
            hard: If True, remove completely. If False, soft delete

        Raises:
            ValueError: If edges doesn't exist
        """
        pass

    @abstractmethod
    def edge_exists(self, oid: str) -> bool:
        """
        Check if edges exists.

        Args:
            oid: Edge ID

        Returns:
            True if exists
        """
        pass

    # =========================================================================
    # Query Operations
    # =========================================================================

    @abstractmethod
    def query_nodes(
            self,
            label: Optional[str] = None,
            predicate: Optional[Callable[[Node], bool]] = None,
            at_time: Optional[datetime] = None,
            between: Optional[Tuple[datetime, datetime]] = None,
            limit: Optional[int] = None
    ) -> List[Node]:
        """
        Query nodes with filters.

        Args:
            label: Filter by label
            predicate: Custom filter function
            at_time: Filter by temporal validity at specific time
            between: Filter by temporal validity in range (start, end)
            limit: Maximum number of results

        Returns:
            List of matching nodes
        """
        pass

    @abstractmethod
    def query_edges(
            self,
            label: Optional[str] = None,
            source: Optional[str] = None,
            target: Optional[str] = None,
            predicate: Optional[Callable[[Edge], bool]] = None,
            at_time: Optional[datetime] = None,
            between: Optional[Tuple[datetime, datetime]] = None,
            limit: Optional[int] = None
    ) -> List[Edge]:
        """
        Query edges with filters.

        Args:
            label: Filter by label
            source: Filter by source node ID
            target: Filter by target node ID
            predicate: Custom filter function
            at_time: Filter by temporal validity at specific time
            between: Filter by temporal validity in range
            limit: Maximum number of results

        Returns:
            List of matching edges
        """
        pass

    # =========================================================================
    # Traversal Operations
    # =========================================================================

    @abstractmethod
    def get_neighbors(
            self,
            oid: str,
            direction: Literal["in", "out", "both"] = "both",
            label: Optional[str] = None
    ) -> List[str]:
        """
        Get neighbor node IDs.

        Args:
            oid: Node ID
            direction: 'out', 'in', or 'both'
            label: Filter by edges label

        Returns:
            List of neighbor node IDs
        """
        pass

    @abstractmethod
    def get_edges_between(
            self,
            source: str,
            target: str,
            label: Optional[str] = None
    ) -> List[Edge]:
        """
        Get edges between two nodes.

        Args:
            source: Source node ID
            target: Target node ID
            label: Filter by label

        Returns:
            List of edges
        """
        pass

    # =========================================================================
    # Statistics
    # =========================================================================

    @abstractmethod
    def count_nodes(self, label: Optional[str] = None) -> int:
        """
        Count nodes.

        Args:
            label: Filter by label

        Returns:
            Number of nodes
        """
        pass

    @abstractmethod
    def count_edges(self, label: Optional[str] = None) -> int:
        """
        Count edges.

        Args:
            label: Filter by label

        Returns:
            Number of edges
        """
        pass

    @abstractmethod
    def get_labels(self) -> Dict[str, int]:
        """
        Get all labels with counts.

        Returns:
            Dict mapping label -> count
        """
        pass

    # =========================================================================
    # Bulk Operations
    # =========================================================================

    @abstractmethod
    def bulk_insert_nodes(self, nodes: List[Node]) -> None:
        """
        Insert multiple nodes efficiently.

        Args:
            nodes: List of nodes to insert
        """
        pass

    @abstractmethod
    def bulk_insert_edges(self, edges: List[Edge]) -> None:
        """
        Insert multiple edges efficiently.

        Args:
            edges: List of edges to insert
        """
        pass

    # =========================================================================
    # Utility
    # =========================================================================

    @abstractmethod
    def clear(self) -> None:
        """Clear all data"""
        pass

    @abstractmethod
    def copy(self) -> 'StorageBackend':
        """
        Create a deep copy of this backend.

        Returns:
            New StorageBackend instance with same data
        """
        pass

    @abstractmethod
    def stats(self) -> Dict[str, Any]:
        """
        Get backend statistics.

        Returns:
            Dict with statistics
        """
        pass

    # =========================================================================
    # Context Manager
    # =========================================================================

    def __enter__(self) -> 'StorageBackend':
        """Enter context (begin transaction)"""
        self.connect()
        self.begin_transaction()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context (commit or rollback)"""
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.disconnect()


# =============================================================================
# Helper Functions
# =============================================================================

def validate_node_type(node: Any) -> Node:
    """
    Validate that object is a valid node.

    Args:
        node: Object to validate

    Returns:
        Node if valid

    Raises:
        TypeError: If not a valid node
    """
    if not isinstance(node, (PGNode, TSNode)):
        raise TypeError(f"Expected PGNode or TSNode, got {type(node)}")
    return node


def validate_edge_type(edge: Any) -> Edge:
    """
    Validate that object is a valid edges.

    Args:
        edge: Object to validate

    Returns:
        Edge if valid

    Raises:
        TypeError: If not a valid edges
    """
    if not isinstance(edge, (PGEdge, TSEdge)):
        raise TypeError(f"Expected PGEdge or TSEdge, got {type(edge)}")
    return edge