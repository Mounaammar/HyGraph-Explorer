"""
HyGraph Core Storage

Storage backends for HyGraph.

Available backends:
- MemoryStorage: In-memory (NetworkX) - for development/testing
- AGEStorage: Apache AGE (PostgreSQL) - graph persistence
- TimescaleStorage: TimescaleDB (PostgreSQL) - time series persistence
- HybridStorage: Orchestrates AGE + TimescaleDB + in-memory - RECOMMENDED
"""

from .base import StorageBackend, Node, Edge, validate_node_type, validate_edge_type
from .memory import MemoryStorage, create_memory_storage

# Import database backends
try:
    from .age import AGEStorage
    from .timescale import TimescaleStorage
    from .hybrid_storage import HybridStorage, create_hybrid_storage
    _HAS_DB_BACKENDS = True
except ImportError:
    _HAS_DB_BACKENDS = False
    AGEStorage = None
    TimescaleStorage = None
    HybridStorage = None
    create_hybrid_storage = None

__all__ = [
    'StorageBackend',
    'Node',
    'Edge',
    'validate_node_type',
    'validate_edge_type',
    'MemoryStorage',
    'create_memory_storage',
]

# Add database backends if available
if _HAS_DB_BACKENDS:
    __all__.extend([
        'AGEStorage',
        'TimescaleStorage',
        'HybridStorage',
        'create_hybrid_storage',
    ])
