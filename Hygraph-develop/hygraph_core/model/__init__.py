"""
HyGraph Core Model

Typed domain objects for HyGraph.
"""

from .base import (
    Entity,
    EntityType,
    Property,
    StaticProperty,
    TemporalProperty,
    PropertyContainer,
    Metadata,
    PropertyType,
    FAR_FUTURE,
    validate_oid,
    validate_label,
    validate_temporal_validity,
    is_temporal_overlap,
    temporal_intersection,
    format_timedelta
)

from .timeseries import (
    TimeSeries,
    TimeSeriesMetadata,
    TimeSeriesType,
    create_empty_timeseries,
    merge_timeseries
)

from .nodes import (
    PGNode,
    TSNode,
    create_pgnode,
    create_tsnode,
    is_pgnode,
    is_tsnode
)

from .edges import (
    PGEdge,
    TSEdge,
    create_pgedge,
    create_tsedge,
    is_pgedge,
    is_tsedge
)

from .graph_properties import (
    HyGraphProperties,
)

__all__ = [
    # Base
    'Entity',
    'EntityType',
    'Property',
    'StaticProperty',
    'TemporalProperty',
    'PropertyContainer',
    'Metadata',
    'PropertyType',
    'FAR_FUTURE',
    'validate_oid',
    'validate_label',
    'validate_temporal_validity',
    'is_temporal_overlap',
    'temporal_intersection',
    'format_timedelta',

    # Time Series
    'TimeSeries',
    'TimeSeriesMetadata',
    'TimeSeriesType',
    'create_empty_timeseries',
    'merge_timeseries',

    # Nodes
    'PGNode',
    'TSNode',
    'create_pgnode',
    'create_tsnode',
    'is_pgnode',
    'is_tsnode',

    # Edges
    'PGEdge',
    'TSEdge',
    'create_pgedge',
    'create_tsedge',
    'is_pgedge',
    'is_tsedge',
    
    # Graph Properties
    'HyGraphProperties'
]
