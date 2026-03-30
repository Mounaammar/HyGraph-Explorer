"""
HyGraph Core Model - Graph Properties

Properties for HyGraph and subHyGraph instances.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, Set

from .base import PropertyContainer, EntityType


# =============================================================================
# HyGraphProperties
# =============================================================================

@dataclass
class HyGraphProperties(PropertyContainer):
    """
    Properties for HyGraph and subHyGraph.
    
    Extends PropertyContainer to support both static and temporal properties
    at the graph level.
    
    Example:
        # Full HyGraph
        hg.properties.name = "nyc_bikes"
        hg.properties.add_static_property("domain", "transportation")
        hg.properties.add_static_property("region", "NYC")
        
        # Temporal property (e.g., computed metric as time series)
        hg.properties.add_temporal_property("node_count", "ts_node_count_001")
        
        # subHyGraph
        sub = hg.subHygraph(node_ids={...}, name="high_capacity")
        sub.properties.is_subgraph  # True
        sub.properties.filter_node_ids  # {...}
    """
    
    # =========================================================================
    # Identity
    # =========================================================================
    
    name: str = "default"
    created_at: datetime = field(default_factory=datetime.now)
    last_update: Optional[datetime] = None
    
    # =========================================================================
    # SubGraph Information
    # =========================================================================
    
    is_subgraph: bool = False
    filter_node_ids: Optional[Set[str]] = None
    filter_query: Optional[str] = None  # Human-readable filter description

    def entity_type(self) -> EntityType:
        """Return entity type"""
        return EntityType.SUB_HYGRAPH if self.is_subgraph else EntityType.HYGRAPH

    # =========================================================================
    # Temporal Property Methods (Override for HyGraph/SubHyGraph)
    # =========================================================================
    
    def create_temporal_property(self, hg, name: str, timeseries: 'TimeSeries', owner_id: Optional[Any] = None) -> str:
        """
        Create a new timeseries and link it to this HyGraph/SubHyGraph.
        
        Uses the graph's name as owner_id if not specified.
        
        Args:
            hg: HyGraph instance (for database access)
            name: Property name (variable name)
            timeseries: TimeSeries object to store
            owner_id: Optional owner ID (defaults to graph name)
            
        Returns:
            The ts_id of the created timeseries
            
        Example:
            # For main HyGraph
            hg.properties.create_temporal_property(hg, 'node_count', ts_obj)
            
            # For SubHyGraph
            sub.properties.create_temporal_property(hg, 'density', ts_obj)
        """
        # Use graph name as owner_id if not specified
        if owner_id is None:
            owner_id = self.name
        
        # Set owner_id in timeseries metadata
        timeseries.metadata.owner_id = owner_id
        
        # Store in TimescaleDB
        ts_id = hg.create_timeseries(timeseries)
        
        # Store reference
        self._temporal_properties[name] = ts_id
        
        # Cache the data
        self._temporal_property_cache[name] = timeseries
        
        # Update timestamp
        self.touch()
        
        return ts_id

    def link_orphan_timeseries(self, hg, name: str, ts_id: str, owner_id: Optional[Any] = None) -> None:
        """
        Link an existing (orphan) timeseries to this HyGraph/SubHyGraph.
        
        Args:
            hg: HyGraph instance (for database access)
            name: Property name (variable name)
            ts_id: Time series ID of the orphan timeseries
            owner_id: Optional owner ID (defaults to graph name)
            
        Example:
            # Link an orphan timeseries to a subgraph
            sub.properties.link_orphan_timeseries(hg, 'node_count', 'ts_12345')
        """
        # Use graph name as owner_id if not specified
        if owner_id is None:
            owner_id = self.name
        
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
        
        # Update timestamp
        self.touch()

    # =========================================================================
    # Update Tracking
    # =========================================================================

    def touch(self) -> None:
        """Update the last_update timestamp."""
        self.last_update = datetime.now()

    # =========================================================================
    # Serialization
    # =========================================================================

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            'name': self.name,
            'created_at': self.created_at.isoformat(),
            'is_subgraph': self.is_subgraph,
            'static_properties': self.static_properties,
            'temporal_properties': self.temporal_properties
        }
        
        if self.last_update:
            result['last_update'] = self.last_update.isoformat()
        
        if self.filter_node_ids:
            result['filter_node_ids'] = list(self.filter_node_ids)
        
        if self.filter_query:
            result['filter_query'] = self.filter_query
        
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HyGraphProperties':
        """Create from dictionary."""
        props = cls(
            name=data.get('name', 'default'),
            created_at=datetime.fromisoformat(data['created_at']) if 'created_at' in data else datetime.now(),
            is_subgraph=data.get('is_subgraph', False),
            filter_query=data.get('filter_query')
        )
        
        if 'last_update' in data and data['last_update']:
            props.last_update = datetime.fromisoformat(data['last_update'])
        
        if 'filter_node_ids' in data and data['filter_node_ids']:
            props.filter_node_ids = set(data['filter_node_ids'])
        
        # Load properties
        props.properties_from_dict(data)
        
        return props

    # =========================================================================
    # String Representation
    # =========================================================================

    def __repr__(self) -> str:
        graph_type = "SubHyGraph" if self.is_subgraph else "HyGraph"
        props_count = self.property_count
        return f"HyGraphProperties({graph_type}: {self.name}, {props_count} properties)"

    def __str__(self) -> str:
        lines = [f"{'SubHyGraph' if self.is_subgraph else 'HyGraph'}: {self.name}"]
        lines.append(f"  Created: {self.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.last_update:
            lines.append(f"  Last Update: {self.last_update.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.is_subgraph:
            if self.filter_node_ids:
                lines.append(f"  Filter: {len(self.filter_node_ids)} nodes")
            if self.filter_query:
                lines.append(f"  Query: {self.filter_query}")
        
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
