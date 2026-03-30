"""
HyGraph Core Model - Edges

Fully typed edge classes: PGEdge and TSEdge.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .timeseries import TimeSeries

from .base import (
    Entity, EntityType, PropertyContainer, Metadata,
    validate_oid, validate_label, validate_temporal_validity,
    FAR_FUTURE
)


# =============================================================================
# PGEdge (Property Graph Edge)
# =============================================================================

@dataclass
class PGEdge(Entity, PropertyContainer):
    """
    Property Graph Edge with temporal validity.

    Represents a directed edge from source to target with:
    - Static properties (fixed values)
    - Temporal properties (time series references)
    - Temporal validity (start_time, end_time)
    - Metadata (tags, computed properties)

    Example:
        edge = PGEdge(
            oid="trip_1",
            source="station_1",
            target="station_2",
            label="trip",
            start_time=datetime(2024, 1, 1, 10, 30)
        )
        edge.add_static_property("distance", 2.5)
        edge.add_static_property("duration_minutes", 15)
    """


    
    # Edge-specific
    source: str = ""  # Source node ID
    target: str = ""  # Target node ID
    
    # Metadata
    metadata: Metadata = field(default_factory=Metadata)

    def __post_init__(self):
        """Validate edge and initialize property containers."""

        
        # Validate
        validate_oid(self.oid)
        validate_label(self.label)
        validate_temporal_validity(self.start_time, self.end_time)

        if not self.source:
            raise ValueError("Edge must have a source")
        if not self.target:
            raise ValueError("Edge must have a target")

    def entity_type(self) -> EntityType:
        """Return entity type"""
        return EntityType.PG_EDGE

    # -------------------------------------------------------------------------
    # Edge-specific Methods
    # -------------------------------------------------------------------------

    def is_self_loop(self) -> bool:
        """Check if edge is a self-loop"""
        return self.source == self.target

    # -------------------------------------------------------------------------
    # Metadata Helpers
    # -------------------------------------------------------------------------

    def set_metadata(self, **kwargs) -> None:
        """Set metadata computed fields."""
        for key, value in kwargs.items():
            self.metadata.set_computed(key, value)

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata computed value."""
        return self.metadata.get_computed(key, default)

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'oid': self.oid,
            'source': self.source,
            'target': self.target,
            'label': self.label,
            'type': self.entity_type().value,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'static_properties': self.static_properties,
            'temporal_properties': self.temporal_properties,
            'metadata': self.metadata.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PGEdge':
        """Create PGEdge from dictionary."""
        edge = cls(
            oid=data['oid'],
            source=data['source'],
            target=data['target'],
            label=data['label'],
            start_time=datetime.fromisoformat(data['start_time']),
            end_time=datetime.fromisoformat(data.get('end_time', FAR_FUTURE.isoformat()))
        )

        # Load properties using PropertyContainer methods
        edge.properties_from_dict(data)

        # Load metadata
        if 'metadata' in data:
            edge.metadata = Metadata.from_dict(data['metadata'])

        return edge

    # -------------------------------------------------------------------------
    # String Representation
    # -------------------------------------------------------------------------

    def __repr__(self) -> str:
        """String representation"""
        return (
            f"PGEdge(oid={self.oid}, "
            f"{self.source}-[{self.label}]->{self.target})"
        )

    def __str__(self) -> str:
        """User-friendly string"""
        return f"{self.source}-[{self.label}]->{self.target}"


# =============================================================================
# TSEdge (Time Series Edge)
# =============================================================================

@dataclass
class TSEdge(Entity):
    """
    Time Series Edge.

    An edge that IS a time series (not an edge with TS properties).
    Contains only:
    - oid, source, target, label
    - Reference to a time series (series_id)
    - Temporal validity
    - Metadata

    Example:
        edge = TSEdge(
            oid="similarity_1_2",
            source="station_1",
            target="station_2",
            label="similarity",
            series_id="ts_sim_001",
            start_time=datetime(2024, 1, 1)
        )
    """

    # From Entity
    oid: str
    label: str
    start_time: datetime
    end_time: datetime = FAR_FUTURE
    
    # Edge-specific
    source: str = ""
    target: str = ""
    
    # TSEdge-specific
    series_id: str = ""
    
    # Metadata
    metadata: Metadata = field(default_factory=Metadata)

    def __post_init__(self):
        """Validate edge after initialization"""
        validate_oid(self.oid)
        validate_label(self.label)
        validate_temporal_validity(self.start_time, self.end_time)

        if not self.source:
            raise ValueError("Edge must have a source")
        if not self.target:
            raise ValueError("Edge must have a target")
        if not self.series_id:
            raise ValueError("TSEdge must have a series_id")

    def entity_type(self) -> EntityType:
        """Return entity type"""
        return EntityType.TS_EDGE

    def is_self_loop(self) -> bool:
        """Check if edge is a self-loop"""
        return self.source == self.target

    @property
    def has_properties(self) -> bool:
        """TSEdge has no properties (it's pure time series)"""
        return False

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'oid': self.oid,
            'source': self.source,
            'target': self.target,
            'label': self.label,
            'type': self.entity_type().value,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'series_id': self.series_id,
            'metadata': self.metadata.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TSEdge':
        """Create TSEdge from dictionary."""
        edge = cls(
            oid=data['oid'],
            source=data['source'],
            target=data['target'],
            label=data['label'],
            series_id=data['series_id'],
            start_time=datetime.fromisoformat(data['start_time']),
            end_time=datetime.fromisoformat(data.get('end_time', FAR_FUTURE.isoformat()))
        )

        if 'metadata' in data:
            edge.metadata = Metadata.from_dict(data['metadata'])

        return edge

    # -------------------------------------------------------------------------
    # String Representation
    # -------------------------------------------------------------------------

    def __repr__(self) -> str:
        """String representation"""
        return (
            f"TSEdge(oid={self.oid}, "
            f"{self.source}-[{self.label}]->{self.target}, "
            f"series={self.series_id})"
        )

    def __str__(self) -> str:
        """User-friendly string"""
        return f"{self.source}-[{self.label}]->{self.target}"


# =============================================================================
# Factory Functions
# =============================================================================

def create_pgedge(
        oid: str,
        source: str,
        target: str,
        label: str,
        start_time: datetime,
        end_time: datetime = FAR_FUTURE,
        properties: Optional[Dict[str, Any]] = None,
        ts_properties: Optional[Dict[str, str]] = None
) -> PGEdge:
    """
    Factory function to create a PGEdge.
    """
    edge = PGEdge(
        oid=oid,
        source=source,
        target=target,
        label=label,
        start_time=start_time,
        end_time=end_time
    )

    if properties:
        for name, value in properties.items():
            edge.add_static_property(name, value)

    if ts_properties:
        for name, ts_id in ts_properties.items():
            edge.add_temporal_property(name, ts_id)

    return edge


def create_tsedge(
        oid: str,
        source: str,
        target: str,
        label: str,
        series_id: str,
        start_time: datetime,
        end_time: datetime = FAR_FUTURE
) -> TSEdge:
    """
    Factory function to create a TSEdge.
    """
    return TSEdge(
        oid=oid,
        source=source,
        target=target,
        label=label,
        series_id=series_id,
        start_time=start_time,
        end_time=end_time
    )


# =============================================================================
# Type Guards
# =============================================================================

def is_pgedge(entity: Entity) -> bool:
    """Check if entity is a PGEdge"""
    return isinstance(entity, PGEdge)


def is_tsedge(entity: Entity) -> bool:
    """Check if entity is a TSEdge"""
    return isinstance(entity, TSEdge)
