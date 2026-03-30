"""
HyGraph Core Model - Base Classes

Fully typed base classes for all HyGraph entities.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, Optional, Set

from .timeseries import TimeSeries

# =============================================================================
# Constants
# =============================================================================

FAR_FUTURE = datetime(2100, 12, 31, 23, 59, 59)
FAR_PAST = datetime(1970 , 1, 1, 23, 59, 59)

# =============================================================================
# Enums
# =============================================================================

class EntityType(Enum):
    """Type of entity in HyGraph"""
    PG_NODE = "PGNode"
    TS_NODE = "TSNode"
    PG_EDGE = "PGEdge"
    TS_EDGE = "TSEdge"
    HYGRAPH = "HyGraph"
    SUB_HYGRAPH = "SubHyGraph"


class PropertyType(Enum):
    """Type of property"""
    STATIC = "static"
    TEMPORAL = "temporal"


# =============================================================================
# Property Container (Base for Node, Edge, HyGraph)
# =============================================================================
@dataclass
class PropertyContainer:
    """
    Mixin class for entities that have properties.
    Used by: PGNode, PGEdge, HyGraphProperties
    Provides:
    - Static properties (fixed values)
    - Temporal properties (time series references)
    - Temporal property cache (loaded TimeSeries objects)
    Example:
        container.add_static_property("capacity", 50)
        container.add_temporal_property("num_bikes", "ts_001")
        
        # Get properties
        cap = container.get_static_property("capacity")
        ts_id = container.get_temporal_property_id("num_bikes")
    """
    

    _static_properties: Dict[str, Any] = field(default_factory=dict)
    _temporal_properties: Dict[str, str]  = field(default_factory=dict)
    _temporal_property_cache: Dict[str, 'TimeSeries']= field(default_factory=dict)
    


    # =========================================================================
    # Static Properties
    # =========================================================================

    @property
    def static_properties(self) -> Dict[str, Any]:
        """Get all static properties (copy)."""
        return self._static_properties.copy()

    def add_static_property(self, name: str, value: Any) -> None:
        """Add or update a static property."""
        self._static_properties[name] = value

    def get_static_property(self, name: str, default: Any = None, strict: bool = False) -> Any:
        """
        Get a static property by name.
        """
        if strict and name not in self._static_properties:
            available = list(self._static_properties.keys())
            raise KeyError(
                f"Static property '{name}' not found. "
                f"Available properties: {available}"
            )
        return self._static_properties.get(name, default)

    def has_static_property(self, name: str) -> bool:
        """Check if static property exists."""
        return name in self._static_properties

    def remove_static_property(self, name: str) -> None:
        """Remove a static property."""
        self._static_properties.pop(name, None)

    # =========================================================================
    # Temporal Properties
    # =========================================================================

    @property
    def temporal_properties(self) -> Dict[str, str]:
        """Get all temporal properties (name -> ts_id) (copy)."""
        return self._temporal_properties.copy()

    def add_temporal_property(self, name: str, ts_id: str) -> None:
        """
        Add or update a temporal property reference (ts_id only).
        
        This is the simple version that just stores the ts_id reference.
        Use link_orphan_timeseries() for attaching orphan timeseries with HyGraph.

        @:param    name: Property name (variable name)
        @:param     ts_id: Time series ID in TimescaleDB
        """
        self._temporal_properties[name] = ts_id

    def link_orphan_timeseries(self, hg, name: str, ts_id: str, owner_id: Optional[Any] = None) -> None:
        """
        Link an orphan timeseries to this entity.
        
        This method:
        1. Stores the ts_id reference in temporal_properties
        2. Optionally loads the timeseries data into cache

        @:param     hg: HyGraph instance (for database access)
        @:param     name: Property name (variable name)
        @:param     ts_id: Time series ID of the orphan timeseries
        @:param     owner_id: Optional owner ID to set in metadata
        """
        # Store the reference
        self._temporal_properties[name] = ts_id
        
        # Try to load the timeseries data
        try:
            ts_data = hg.timescale.get_measurements(entity_uid=ts_id, variable=name)
            if ts_data:
                if owner_id:
                    ts_data.metadata.owner_id = owner_id
                self._temporal_property_cache[name] = ts_data
        except Exception as e:
            # Don't fail if we can't load - just store the reference
            print(f"Warning: Could not load timeseries {ts_id}: {e}")

    def create_temporal_property(self, hg, name: str, timeseries: 'TimeSeries', owner_id: Optional[Any] = None) -> str:
        """
        Create a new timeseries.
        
        This method:
        1. Stores the timeseries in TimescaleDB
        2. Stores the ts_id reference in temporal_properties
        3. Caches the timeseries data
        Returns the ts_id of the created timeseries
        """
        # Store in TimescaleDB
        ts_id = hg.create_timeseries(timeseries)
        
        # Store reference
        self._temporal_properties[name] = ts_id
        
        # Cache the data
        self._temporal_property_cache[name] = timeseries
        
        return ts_id

    def get_temporal_property_id(self, name: str, strict: bool = False) -> Optional[str]:
        """
        Get temporal property time series ID by name.

        @:param     name: Property name
        @:param     strict: If True, raise KeyError when property doesn't exist
        
        Returns Time series ID or None
        """
        if strict and name not in self._temporal_properties:
            available = list(self._temporal_properties.keys())
            raise KeyError(
                f"Temporal property '{name}' not found. "
                f"Available temporal properties: {available}"
            )
        return self._temporal_properties.get(name)

    def has_temporal_property(self, name: str) -> bool:
        """Check if temporal property exists."""
        return name in self._temporal_properties

    def remove_temporal_property(self, name: str) -> None:
        """Remove a temporal property."""
        self._temporal_properties.pop(name, None)
        self._temporal_property_cache.pop(name, None)

    # =========================================================================
    # Temporal Property Cache (loaded TimeSeries objects)
    # =========================================================================

    def get_temporal_property(self, name: str) -> Optional['TimeSeries']:
        """
        Get temporal property as TimeSeries object.
        
        Returns the cached TimeSeries object if loaded, None otherwise.
        Use get_temporal_property_id() to get just the ID string.
        
        Note: If this returns None but has_temporal_property() returns True,
        the TimeSeries data wasn't loaded from the database. This can happen if:
        - No measurements exist in TimescaleDB for this entity/variable
        - The entity_uid in TimescaleDB doesn't match the entity's oid
        """
        return self._temporal_property_cache.get(name)

    def set_temporal_property_data(self, name: str, timeseries: 'TimeSeries') -> None:
        """Set the TimeSeries object for a temporal property."""
        self._temporal_property_cache[name] = timeseries

    def has_temporal_property_data(self, name: str) -> bool:
        """Check if TimeSeries data is loaded for a temporal property."""
        return name in self._temporal_property_cache

    def list_temporal_properties_status(self) -> Dict[str, bool]:
        """
        Debug helper: Show which temporal properties have data loaded.
        
        Returns:
            Dict mapping property name to whether data is loaded
            
        Example:
             node.list_temporal_properties_status()
            {'num_bikes': True, 'num_docks': False}  # num_docks has ID but no data
        """
        return {
            name: name in self._temporal_property_cache
            for name in self._temporal_properties.keys()
        }

    def clear_temporal_property_cache(self) -> None:
        """Clear all cached TimeSeries objects."""
        self._temporal_property_cache.clear()

    # =========================================================================
    # Combined Property Access
    # =========================================================================

    def get_property(self, name: str, default: Any = None) -> Any:
        """
        Get a property value (static or temporal).
        
        For temporal properties, returns the time series ID.
        """
        if name in self._static_properties:
            return self._static_properties[name]
        elif name in self._temporal_properties:
            return self._temporal_properties[name]
        return default

    def has_property(self, name: str) -> bool:
        """Check if property exists (static or temporal)."""
        return name in self._static_properties or name in self._temporal_properties

    def remove_property(self, name: str) -> None:
        """Remove a property (static or temporal)."""
        self._static_properties.pop(name, None)
        self._temporal_properties.pop(name, None)
        self._temporal_property_cache.pop(name, None)

    def all_properties(self) -> Dict[str, Any]:
        """Get all properties (static + temporal IDs)."""
        return {**self._static_properties, **self._temporal_properties}

    @property
    def property_count(self) -> int:
        """Total number of properties."""
        return len(self._static_properties) + len(self._temporal_properties)

    def require_static_property(self, name: str) -> Any:
        """
        Get a static property, raising KeyError if it doesn't exist.
        
        Shortcut for get_static_property(name, strict=True).
        
        Example:
            # These are equivalent:
            node.require_static_property('capacity')
            node.get_static_property('capacity', strict=True)
        """
        return self.get_static_property(name, strict=True)

    def require_temporal_property_id(self, name: str) -> str:
        """
        Get a temporal property ID, raising KeyError if it doesn't exist.
        
        Shortcut for get_temporal_property_id(name, strict=True).
        """
        return self.get_temporal_property_id(name, strict=True)

    # =========================================================================
    # Serialization Helpers
    # =========================================================================

    def properties_to_dict(self) -> Dict[str, Any]:
        """Convert properties to dict for serialization."""
        return {
            'static_properties': self.static_properties,
            'temporal_properties': self.temporal_properties
        }

    def properties_from_dict(self, data: Dict[str, Any]) -> None:
        """Load properties from dict."""
        for name, value in data.get('static_properties', {}).items():
            self.add_static_property(name, value)
        for name, ts_id in data.get('temporal_properties', {}).items():
            self.add_temporal_property(name, ts_id)


# =============================================================================
# Base Entity
# =============================================================================

@dataclass
class Entity(ABC):
    """
    Base class for all HyGraph entities.

    All entities have:
    - Unique identifier (oid)
    - Label
    - Temporal validity (start_time, end_time)
    """

    oid: str =""
    label: str=""
    start_time: datetime=FAR_PAST
    end_time: datetime = FAR_FUTURE

    @abstractmethod
    def entity_type(self) -> EntityType:
        """Return the type of this entity"""
        pass

    def is_valid_at(self, timestamp: datetime) -> bool:
        """Check if entity is valid at given timestamp"""
        return self.start_time <= timestamp <= self.end_time

    def overlaps_with(self, start: datetime, end: datetime) -> bool:
        """Check if entity's validity overlaps with given period"""
        return not (self.end_time < start or self.start_time > end)

    def __hash__(self) -> int:
        """Make entity hashable by oid"""
        return hash(self.oid)

    def __eq__(self, other: object) -> bool:
        """Compare entities by oid"""
        if not isinstance(other, Entity):
            return False
        return self.oid == other.oid


# =============================================================================
# Property Classes (for explicit property objects)
# =============================================================================

@dataclass
class Property:
    """
    Base property class.

    Properties can be static (fixed value) or temporal (time series reference).
    """

    name: str
    value: Any
    property_type: PropertyType

    def __repr__(self) -> str:
        return f"Property(name={self.name}, type={self.property_type.value})"


@dataclass
class StaticProperty(Property):
    """
    Static property with a fixed value.

    Example: capacity=50, name='Central Station'
    """

    def __init__(self, name: str, value: Any):
        super().__init__(name, value, PropertyType.STATIC)

    def __repr__(self) -> str:
        return f"StaticProperty({self.name}={self.value})"


@dataclass
class TemporalProperty(Property):
    """
    Temporal property referencing a time series.

    The value is a time series ID (string).
    """

    ts_id: str = field(init=False)

    def __init__(self, name: str, ts_id: str):
        super().__init__(name, ts_id, PropertyType.TEMPORAL)
        self.ts_id = ts_id

    def __repr__(self) -> str:
        return f"TemporalProperty({self.name}=ts:{self.ts_id})"


# =============================================================================
# Metadata
# =============================================================================

@dataclass
class Metadata:
    """
    Metadata attached to entities.

    Stores auxiliary information like tags, computed properties, etc.
    """

    tags: Set[str] = field(default_factory=set)
    computed: Dict[str, Any] = field(default_factory=dict)
    custom: Dict[str, Any] = field(default_factory=dict)

    def add_tag(self, tag: str) -> None:
        """Add a tag"""
        self.tags.add(tag)

    def remove_tag(self, tag: str) -> None:
        """Remove a tag"""
        self.tags.discard(tag)

    def has_tag(self, tag: str) -> bool:
        """Check if tag exists"""
        return tag in self.tags

    def set_computed(self, key: str, value: Any) -> None:
        """Set a computed property (e.g., degree, pagerank)"""
        self.computed[key] = value

    def get_computed(self, key: str, default: Any = None) -> Any:
        """Get a computed property"""
        return self.computed.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for serialization."""
        return {
            'tags': list(self.tags),
            'computed': self.computed,
            'custom': self.custom
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Metadata':
        """Create from dict."""
        metadata = cls()
        for tag in data.get('tags', []):
            metadata.add_tag(tag)
        metadata.computed = data.get('computed', {})
        metadata.custom = data.get('custom', {})
        return metadata


# =============================================================================
# Validation
# =============================================================================

class ValidationError(Exception):
    """Raised when entity validation fails"""
    pass


def validate_temporal_validity(start_time: datetime, end_time: datetime) -> None:
    """
    Validate temporal validity.

    Raises:
        ValidationError: If start_time >= end_time
    """
    if start_time >= end_time:
        raise ValidationError(
            f"start_time ({start_time}) must be before end_time ({end_time})"
        )


def validate_oid(oid: str) -> None:
    """
    Validate entity ID.

    Raises:
        ValidationError: If oid is empty or invalid
    """
    if not oid or not isinstance(oid, str):
        raise ValidationError(f"Invalid oid: {oid}")

    if len(oid) > 255:
        raise ValidationError(f"oid too long (max 255 chars): {oid}")


def validate_label(label: str) -> None:
    """
    Validate entity label.

    Raises:
        ValidationError: If label is empty or invalid
    """
    if not label or not isinstance(label, str):
        raise ValidationError(f"Invalid label: {label}")

    if len(label) > 100:
        raise ValidationError(f"label too long (max 100 chars): {label}")


# =============================================================================
# Utility Functions
# =============================================================================

def is_temporal_overlap(
        start1: datetime, end1: datetime,
        start2: datetime, end2: datetime
) -> bool:
    """
    Check if two temporal periods overlap.

    Args:
        start1, end1: First period
        start2, end2: Second period

    Returns:
        True if periods overlap
    """
    return not (end1 < start2 or start1 > end2)


def temporal_intersection(
        start1: datetime, end1: datetime,
        start2: datetime, end2: datetime
) -> Optional[tuple]:
    """
    Compute intersection of two temporal periods.

    Returns:
        (start, end) of intersection, or None if no overlap
    """
    if not is_temporal_overlap(start1, end1, start2, end2):
        return None

    return (max(start1, start2), min(end1, end2))


def format_timedelta(td: timedelta) -> str:
    """
    Format timedelta to human-readable string.
    
    Examples:
        timedelta(hours=1) -> "1H"
        timedelta(days=1) -> "1D"
        timedelta(weeks=1) -> "1W"
    """
    total_seconds = int(td.total_seconds())
    
    if total_seconds % (7 * 24 * 3600) == 0:
        weeks = total_seconds // (7 * 24 * 3600)
        return f"{weeks}W"
    elif total_seconds % (24 * 3600) == 0:
        days = total_seconds // (24 * 3600)
        return f"{days}D"
    elif total_seconds % 3600 == 0:
        hours = total_seconds // 3600
        return f"{hours}H"
    elif total_seconds % 60 == 0:
        minutes = total_seconds // 60
        return f"{minutes}M"
    else:
        return f"{total_seconds}S"


# =============================================================================
# Type Aliases
# =============================================================================

PropertyDict = Dict[str, Any]
TemporalPropertyDict = Dict[str, str]
