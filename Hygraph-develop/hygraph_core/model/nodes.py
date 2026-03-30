"""
HyGraph Core Model - Nodes

Fully typed node classes: PGNode and TSNode.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from .base import (
    Entity, EntityType, PropertyContainer, Metadata,
    validate_oid, validate_label, validate_temporal_validity,
    FAR_FUTURE
)


# =============================================================================
# PGNode (Property Graph Node)
# =============================================================================

@dataclass
class PGNode(Entity, PropertyContainer):
    """
    Property Graph Node with temporal validity.

    Supports:
    - Static properties (fixed values)
    - Temporal properties (time series references)
    - Temporal validity (start_time, end_time)
    - Metadata (tags, computed properties)

    Example:
        node = PGNode(
            oid="station_1",
            label="Station",
            start_time=datetime(2024, 1, 1)
        )
        node.add_static_property("capacity", 50)
        node.add_temporal_property("num_bikes", "ts_001")
    """

    # Metadata
    metadata: Metadata = field(default_factory=Metadata)

    def __post_init__(self):
        """Validate node and initialize property containers."""
        # Initialize PropertyContainer
        # Validate
        validate_oid(self.oid)
        validate_label(self.label)
        validate_temporal_validity(self.start_time, self.end_time)

    def entity_type(self) -> EntityType:
        """Return entity type"""
        return EntityType.PG_NODE

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
            'label': self.label,
            'type': self.entity_type().value,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'static_properties': self.static_properties,
            'temporal_properties': self.temporal_properties,
            'metadata': self.metadata.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PGNode':
        """Create PGNode from dictionary."""
        node = cls(
            oid=data['oid'],
            label=data['label'],
            start_time=datetime.fromisoformat(data['start_time']),
            end_time=datetime.fromisoformat(data.get('end_time', FAR_FUTURE.isoformat()))
        )

        # Load properties using PropertyContainer methods
        node.properties_from_dict(data)

        # Load metadata
        if 'metadata' in data:
            node.metadata = Metadata.from_dict(data['metadata'])

        return node

    # -------------------------------------------------------------------------
    # String Representation
    # -------------------------------------------------------------------------

    def __str__(self) -> str:
        """User-friendly string with pretty display"""
        lines = [
            f"Node: {self.oid}",
            f"  Label: {self.label}",
            f"  Valid: {self.start_time.strftime('%Y-%m-%d')} to {self.end_time.strftime('%Y-%m-%d')}"
        ]

        if self._static_properties:
            lines.append(f"  Static Properties:")
            for key, val in list(self._static_properties.items())[:5]:
                lines.append(f"    {key}: {val}")
            if len(self._static_properties) > 5:
                lines.append(f"    ... ({len(self._static_properties) - 5} more)")

        if self._temporal_properties:
            lines.append(f"  Temporal Properties:")
            for var, ts_id in list(self._temporal_properties.items())[:5]:
                lines.append(f"    {var} → {ts_id}")
            if len(self._temporal_properties) > 5:
                lines.append(f"    ... ({len(self._temporal_properties) - 5} more)")

        return '\n'.join(lines)

    def __repr__(self) -> str:
        """String representation"""
        props_str = ', '.join(
            f"{k}={v}" for k, v in list(self._static_properties.items())[:3]
        )
        if len(self._static_properties) > 3:
            props_str += ', ...'

        if props_str:
            return f"PGNode(oid={self.oid}, label={self.label}, {props_str})"
        return f"PGNode(oid={self.oid}, label={self.label})"


# =============================================================================
# TSNode (Time Series Node)
# =============================================================================

@dataclass
class TSNode(Entity):
    """
    Time Series Node.

    A node that IS a time series (not a node with TS properties).
    Contains only:
    - oid, label
    - Temporal validity
    - Metadata

    Example:
        node = TSNode(
            oid="sensor_1",
            label="TemperatureSensor",
            start_time=datetime(2024, 1, 1)
        )
    """

    # Metadata
    metadata: Metadata = field(default_factory=Metadata)

    def __post_init__(self):
        """Validate node after initialization"""
        validate_oid(self.oid)
        validate_label(self.label)
        validate_temporal_validity(self.start_time, self.end_time)

    def entity_type(self) -> EntityType:
        """Return entity type"""
        return EntityType.TS_NODE

    @property
    def has_properties(self) -> bool:
        """TSNode has no properties (it's pure time series)"""
        return False

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'oid': self.oid,
            'label': self.label,
            'type': self.entity_type().value,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'metadata': self.metadata.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TSNode':
        """Create TSNode from dictionary."""
        node = cls(
            oid=data['oid'],
            label=data['label'],
            start_time=datetime.fromisoformat(data['start_time']),
            end_time=datetime.fromisoformat(data.get('end_time', FAR_FUTURE.isoformat()))
        )

        if 'metadata' in data:
            node.metadata = Metadata.from_dict(data['metadata'])

        return node

    # -------------------------------------------------------------------------
    # String Representation
    # -------------------------------------------------------------------------

    def __repr__(self) -> str:
        """String representation"""
        return f"TSNode(oid={self.oid}, label={self.label})"

    def __str__(self) -> str:
        """User-friendly string"""
        return f"TSNode({self.oid}:{self.label})"


# =============================================================================
# Factory Functions
# =============================================================================

def create_pgnode(
        oid: str,
        label: str,
        start_time: datetime,
        end_time: datetime = FAR_FUTURE,
        properties: Optional[Dict[str, Any]] = None,
        ts_properties: Optional[Dict[str, str]] = None
) -> PGNode:
    """
    Factory function to create a PGNode.

    Args:
        oid: Node ID
        label: Node label
        start_time: Validity start
        end_time: Validity end
        properties: Static properties dict
        ts_properties: Temporal properties dict (name -> ts_id)

    Returns:
        PGNode instance
    """
    node = PGNode(
        oid=oid,
        label=label,
        start_time=start_time,
        end_time=end_time
    )

    if properties:
        for name, value in properties.items():
            node.add_static_property(name, value)

    if ts_properties:
        for name, ts_id in ts_properties.items():
            node.add_temporal_property(name, ts_id)

    return node


def create_tsnode(
        oid: str,
        label: str,
        start_time: datetime,
        end_time: datetime = FAR_FUTURE
) -> TSNode:
    """
    Factory function to create a TSNode.
    """
    return TSNode(
        oid=oid,
        label=label,
        start_time=start_time,
        end_time=end_time
    )


# =============================================================================
# Type Guards
# =============================================================================

def is_pgnode(entity: Entity) -> bool:
    """Check if entity is a PGNode"""
    return isinstance(entity, PGNode)


def is_tsnode(entity: Entity) -> bool:
    """Check if entity is a TSNode"""
    return isinstance(entity, TSNode)
