"""
HyGraph CRUD Operations - Fluent API

Provides clean, fluent interface for:
- CREATE: Nodes/edges with temporal + static properties
- READ: Query with time series constraints
- UPDATE: Modify properties and temporal validity
- DELETE: Soft/hard delete with temporal awareness

This fluent API delegates to HybridCRUD for actual storage operations.
"""
import math
import uuid
from datetime import datetime
from typing import List, Optional, Union, Callable, Any, Tuple

# Import model classes
from hygraph_core.model import PGNode, PGEdge, TimeSeries, TimeSeriesMetadata
from dataclasses import dataclass, field



def safe_datetime_convert(value: Union[str, datetime, None]) -> datetime:
    """Safely convert value to datetime"""
    if value is None:
        return datetime(2100, 12, 31, 23, 59, 59)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            # Try ISO format first
            return datetime.fromisoformat(value)
        except (ValueError, AttributeError):
            # Fallback to default
            return datetime(2100, 12, 31, 23, 59, 59)
    try:
        # Try converting to string then parsing
        return datetime.fromisoformat(str(value))
    except:
        return datetime(2100, 12, 31, 23, 59, 59)





class NodeCreator:
    """Fluent interface for creating nodes"""

    def __init__(self, hygraph, oid: Optional[str] = None):
        self.hygraph = hygraph
        self.oid = oid or str(uuid.uuid4())
        self.label = None
        self.start_time = datetime.now()
        self.end_time = datetime(2100, 12, 31, 23, 59, 59)
        self.static_props = {}
        self.temporal_props = {}  # {name: TimeSeries object}

    def with_label(self, label: str):
        """Set node label"""
        self.label = label
        return self

    def with_id(self, oid: str):
        """Set node ID"""
        self.oid = oid
        return self

    def valid_from(self, start: Union[str, datetime]):
        """Set start time"""
        if isinstance(start, str):
            start = safe_datetime_convert(start)
        self.start_time = start
        return self

    def valid_until(self, end: Union[str, datetime]):
        """Set end time"""
        if isinstance(end, str):
            end = safe_datetime_convert(end)
        self.end_time = end
        return self

    def valid_between(self, start: Union[str, datetime], end: Union[str, datetime]):
        """Set validity period"""
        return self.valid_from(start).valid_until(end)

    def with_property(self, name: str, value: Any):
        """Add static property"""
        self.static_props[name] = value
        return self

    def with_properties(self, **kwargs):
        """Add multiple static properties"""
        self.static_props.update(kwargs)
        return self

    def with_ts_property(self, name: str, time_series: Any):
        """
        Add temporal property (time series).

        
            name: Variable name (e.g., 'num_bikes_available')
            time_series: TimeSeries object with timestamps and data
        """
        self.temporal_props[name] = time_series
        return self

    def create(self):
        """
        Execute the creation - delegates to HybridCRUD.

        This method:
        1. Converts TimeSeries objects to measurement tuples
        2. Calls HybridCRUD.create_node_with_timeseries()
        3. HybridCRUD inserts measurements to TimescaleDB
        4. HybridCRUD creates node in AGE with time series IDs
        """
        if not self.label:
            raise ValueError("Node must have a label")

        # Convert TimeSeries objects to measurement tuples
        timeseries_properties = {}
        for name, ts in self.temporal_props.items():
            # TimeSeries object has: timestamps, data (2D array), variables
            # Convert to [(timestamp, value), ...]
            measurements = []
            for i in range(len(ts.timestamps)):
                # Assuming single variable or first variable
                value = ts.data[i][0] if len(ts.data[i]) > 0 else 0.0
                measurements.append((ts.timestamps[i], float(value)))

            timeseries_properties[name] = measurements

        # Delegate to HybridCRUD
        self.hygraph.crud.create_node_with_timeseries(
            uid=self.oid,
            label=self.label,
            static_properties=self.static_props,
            timeseries_properties=timeseries_properties,
            start_time=self.start_time.isoformat(),
            end_time=self.end_time.isoformat()
        )

        # Return PGNode for chaining
        # Create PGNode with properties
        node = PGNode(
            oid=self.oid,
            label=self.label,
            start_time=self.start_time,
            end_time=self.end_time
        )
        # Add static properties
        for name, value in self.static_props.items():
            node.add_static_property(name, value)
        # Note: temporal properties stored separately as measurements
        return node


class EdgeCreator:
    """Fluent interface for creating edges"""

    def __init__(self, hygraph, source: str, target: str, oid: Optional[str] = None):
        self.hygraph = hygraph
        self.oid = oid or str(uuid.uuid4())
        self.source = source
        self.target = target
        self.label = None
        self.start_time = datetime.now()
        self.end_time = datetime(2100, 12, 31, 23, 59, 59)
        self.static_props = {}
        self.temporal_props = {}

    def with_label(self, label: str):
        """Set edges label"""
        self.label = label
        return self

    def with_id(self, oid: str):
        """Set edges ID"""
        self.oid = oid
        return self

    def valid_from(self, start: Union[str, datetime]):
        """Set start time"""
        if isinstance(start, str):
            start = safe_datetime_convert(start)
        self.start_time = start
        return self

    def valid_until(self, end: Union[str, datetime]):
        """Set end time"""
        if isinstance(end, str):
            end = safe_datetime_convert(end)
        self.end_time = end
        return self

    def valid_between(self, start: Union[str, datetime], end: Union[str, datetime]):
        """Set validity period"""
        return self.valid_from(start).valid_until(end)

    def with_property(self, name: str, value: Any):
        """Add static property"""
        self.static_props[name] = value
        return self

    def with_properties(self, **kwargs):
        """Add multiple static properties"""
        self.static_props.update(kwargs)
        return self

    def with_ts_property(self, name: str, time_series: Any):
        """Add temporal property (time series)"""
        self.temporal_props[name] = time_series
        return self

    def create(self):
        """Execute the creation - delegates to HybridCRUD"""
        if not self.label:
            raise ValueError("Edge must have a label")

        # Convert TimeSeries objects to measurement tuples
        timeseries_properties = {}
        for name, ts in self.temporal_props.items():
            measurements = []
            for i in range(len(ts.timestamps)):
                value = ts.data[i][0] if len(ts.data[i]) > 0 else 0.0
                measurements.append((ts.timestamps[i], float(value)))

            timeseries_properties[name] = measurements

        # Delegate to HybridCRUD
        self.hygraph.crud.create_edge_with_timeseries(
            uid=self.oid,
            src_uid=self.source,
            dst_uid=self.target,
            label=self.label,
            static_properties=self.static_props,
            timeseries_properties=timeseries_properties,
            start_time=self.start_time.isoformat(),
            end_time=self.end_time.isoformat()
        )

        # Return PGEdge
        # Create PGEdge with properties
        edge = PGEdge(
            oid=self.oid,
            source=self.source,
            target=self.target,
            label=self.label,
            start_time=self.start_time,
            end_time=self.end_time
        )
        # Add static properties
        for name, value in self.static_props.items():
            edge.add_static_property(name, value)
        # Note: temporal properties stored separately as measurements
        return edge







class SingleEntityQuery:
    """
    Query for a single entity to access properties.

    Usage:
        hg.query().node('s1').property('capacity')
        hg.query().edge('E1').ts_property('flow')
    """

    def __init__(self, hygraph, entity_type: str, oid: str):
        self.hygraph = hygraph
        self.entity_type = entity_type  # 'node' or 'edge'
        self.oid = oid

    def property(self, name: str, default=None) -> Any:
        """
        Get a static property value.

        
            name: Property name
            default: Default value if not found

        Returns:
            Property value

        Example:
            capacity = hg.query().node('s1').property('capacity')
            distance = hg.query().edge('E1').property('distance', 0.0)
        """
        if self.entity_type == 'node':
            entity = self.hygraph.crud.get_node_with_timeseries(self.oid)
        else:
            entity = self.hygraph.crud.get_edge_with_timeseries(self.oid)

        if entity:
            return entity.get_static_property(name, default)
        return default

    def ts_property(self, variable: str,
                    start_time: Optional[datetime] = None,
                    end_time: Optional[datetime] = None):
        """
        Get a time series property as TimeSeries object.

        
            variable: Variable name
            start_time: Optional start time filter
            end_time: Optional end time filter

        Returns:
            TimeSeries object with beautiful table display, or None if not found

        Example:
            ts = hg.query().node('s1').ts_property('num_bikes')
            print(ts)  # Beautiful table!
            print(ts.head(5))
        """
        # First get the entity to find the ts_id
        if self.entity_type == 'node':
            entity = self.hygraph.crud.get_node_with_timeseries(self.oid)
        else:
            entity = self.hygraph.crud.get_edge_with_timeseries(self.oid)
        
        if not entity:
            return None
        
        # Get ts_id for this variable
        ts_id = entity.get_temporal_property_id(variable)
        if not ts_id:
            return None

        # get_measurements returns a TimeSeries object directly
        ts_obj = self.hygraph.timescale.get_measurements(
            entity_uid=ts_id,
            variable=variable,
            start_time=start_time,
            end_time=end_time
        )

        if not ts_obj:
            return None

        # Update metadata with owner info
        ts_obj.metadata.owner_id = self.oid
        ts_obj.metadata.element_type = self.entity_type

        return ts_obj

    def execute(self) -> Union[PGNode, PGEdge]:
        """
        Get the complete entity.

        Returns:
            Complete PGNode or PGEdge object

        Example:
            node = hg.query().node('s1').execute()
        """
        if self.entity_type == 'node':
            return self.hygraph.crud.get_node_with_timeseries(self.oid)
        else:
            return self.hygraph.crud.get_edge_with_timeseries(self.oid)


class QueryBuilder:
    """
    Fluent query builder for HyGraph.

    Queries use HybridCRUD to get nodes/edges WITH time series data.

    Example:
        # Simple query
        results = hg.query().nodes(label='Station').execute()

        # With time constraints
        results = hg.query()\\
            .nodes(label='Station')\\
            .at('2024-05-01')\\
            .where(lambda n: n['capacity'] > 50)\\
            .where_ts('num_bikes', lambda measurements: len(measurements) > 100)\\
            .execute()
    """

    def __init__(self, hygraph):
        self.hygraph = hygraph
        self._query_type = None  # 'nodes' or 'edges'
        self._label = None
        self._filters = []
        self._ts_filters = []
        self._at_time = None
        self._between_times = None
        self._limit = None
        self._order_by = None
        self._entity_uid = None
        self._variable = None

    # -------------------------------------------------------------------------
    # Query Type
    # -------------------------------------------------------------------------

    def nodes(self, label: Optional[str] = None):
        """Query nodes"""
        self._query_type = 'nodes'
        self._label = label
        return self

    def edges(self, label: Optional[str] = None):
        """Query edges"""
        self._query_type = 'edges'
        self._label = label
        return self

    # -------------------------------------------------------------------------
    # Single Entity Query (for property access)
    # -------------------------------------------------------------------------

    def node(self, oid: str) -> 'SingleEntityQuery':
        """
        Query a single node by OID.

        Returns SingleEntityQuery for property access.

        Example:
            capacity = hg.query().node('s1').property('capacity')
            bikes = hg.query().node('s1').ts_property('num_bikes')
        """
        return SingleEntityQuery(self.hygraph, entity_type='node', oid=oid)

    def edge(self, oid: str) -> 'SingleEntityQuery':
        """
        Query a single edge by OID.

        Returns SingleEntityQuery for property access.

        Example:
            distance = hg.query().edge('E1').property('distance')
            flow = hg.query().edge('E1').ts_property('flow')
        """
        return SingleEntityQuery(self.hygraph, entity_type='edge', oid=oid)

    def timeseries(self, entity_oid: Optional[str] = None, variable: Optional[str] = None):
        """
        Query time series data.

        
            entity_oid: Entity OID (node or edge)
            variable: Variable name

        Example:
            # Get all time series for entity
            all_ts = hg.query().timeseries('E1').execute()

            # Get specific time series
            ts = hg.query().timeseries('E1', 'flow').execute()
        """
        self._query_type = 'timeseries'
        self._entity_uid = entity_oid
        self._variable = variable
        return self

    # -------------------------------------------------------------------------
    # Temporal Constraints
    # -------------------------------------------------------------------------

    def at(self, timestamp: Union[str, datetime]):
        """Filter entities valid at specific timestamp"""
        if isinstance(timestamp, str):
            timestamp = safe_datetime_convert(timestamp)
        self._at_time = timestamp
        return self

    def between(self, start: Union[str, datetime], end: Union[str, datetime]):
        """Filter entities valid within time range"""
        if isinstance(start, str):
            start = safe_datetime_convert(start)
        if isinstance(end, str):
            end = safe_datetime_convert(end)
        self._between_times = (start, end)
        return self

    # -------------------------------------------------------------------------
    # Filters
    # -------------------------------------------------------------------------

    def where(self, predicate=None, **kwargs):
        """
        Filter by properties.

        Examples:
            .where(lambda n: n.get_static_property('capacity') > 50)
            .where(capacity=50)
        
        Note: Use strict=True to get better error messages:
            .where(lambda n: n.get_static_property('capacity', strict=True) > 50)
        """
        if predicate is not None:
            if callable(predicate):
                # Wrap predicate to provide better error messages
                def wrapped_predicate(entity):
                    try:
                        return predicate(entity)
                    except TypeError as e:
                        # Likely comparing None with a value
                        # Get available properties for error message
                        available = list(entity.static_properties.keys()) if hasattr(entity, 'static_properties') else []
                        raise TypeError(
                            f"Filter error: {e}. "
                            f"This usually means a property returned None. "
                            f"Available static properties: {available}. "
                            f"Tip: Use .get_static_property('name', strict=True) for better errors."
                        ) from e
                self._filters.append(wrapped_predicate)

        # Handle keyword arguments
        for key, value in kwargs.items():
            if callable(value):
                self._filters.append(lambda n, k=key, v=value: k in n and v(n[k]))
            else:
                self._filters.append(lambda n, k=key, v=value: n.get(k) == v)

        return self

    def where_ts_agg(
        self, 
        ts_name: str, 
        aggregation: str, 
        operator: str, 
        value: float
    ):
        """
        Filter by time series aggregation without lambda.

        @:param ts_name: Temporal property name (e.g., 'num_bikes_available')
        @:param    aggregation: Aggregation function ('mean', 'min', 'max', 'std', 'sum', 
                         'count', 'range', 'first', 'last', 'median')
        @:param    operator: Comparison operator ('=', '<', '>', '<=', '>=', '!=')
        @:param    value: Value to compare against

        Example:
            .where_ts_agg('num_bikes', 'mean', '<', 10)

        """
        def make_predicate(agg, op, val):
            def predicate(ts_or_measurements):
                # Handle TimeSeries object (preferred)
                agg_val= ts_aggregate(ts_or_measurements,agg)
                if agg_val is None:
                    return False
                # Compare
                if op in ('=', '=='):
                    return abs(agg_val - val) < 1e-9
                elif op in ('!=', '<>'):
                    return abs(agg_val - val) >= 1e-9
                elif op == '<':
                    return agg_val < val
                elif op == '<=':
                    return agg_val <= val
                elif op == '>':
                    return agg_val > val
                elif op == '>=':
                    return agg_val >= val
                return False
            return predicate
        
        self._ts_filters.append((ts_name, make_predicate(aggregation, operator, value)))
        return self

    def where_ts_bucketed(
        self,
        ts_name: str,
        interval: str,
        agg_func: str,
        condition: str,
        operator: str,
        value: float,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ):
        """
        Filter by time-bucketed aggregation using TimescaleDB's time_bucket.

        @:param    ts_name: Temporal property name
        @:param    interval: Time bucket interval (e.g., '1 hour', '15 minutes', '1 day')
        @:param    agg_func: Aggregation function ('avg', 'sum', 'min', 'max', 'count')
        @:param    condition: How to evaluate buckets:
                - 'all': ALL buckets must satisfy the condition
                - 'any': ANY bucket must satisfy the condition  
                - 'majority': >50% of buckets satisfy the condition
                - 'avg': Average of bucket values satisfies the condition
        @:param    operator: Comparison operator ('<', '>', '<=', '>=', '=', '!=')
        @:param    value: Value to compare against
        @:param    start_time: Optional start time filter
        @:param    end_time: Optional end time filter
        
        Example:
            # Find stations where ALL hourly averages are < 10
            .where_ts_bucketed('num_bikes', '1 hour', 'avg', 'all', '<', 10)
            
            # Find stations where ANY 15-minute max exceeds 50
            .where_ts_bucketed('num_bikes', '15 minutes', 'max', 'any', '>', 50)
            
            # Find stations where average of daily counts > 100
            .where_ts_bucketed('num_bikes', '1 day', 'count', 'avg', '>', 100)
        """
        # Store parameters for execution phase
        self._ts_bucketed_filters = getattr(self, '_ts_bucketed_filters', [])
        self._ts_bucketed_filters.append({
            'ts_name': ts_name,
            'interval': interval,
            'agg_func': agg_func,
            'condition': condition,
            'operator': operator,
            'value': value,
            'start_time': start_time,
            'end_time': end_time
        })
        return self

    def where_ts_pattern(
        self,
        ts_name: str,
        pattern: Any,
        method: str = 'euclidean',
        threshold: Optional[float] = None,
        normalize: bool = True
    ):
        """
        Filter by subsequence pattern matching.
        
        Find entities whose time series CONTAINS a given pattern as a subsequence.
        This is different from whole-series similarity - it searches for the pattern
        WITHIN the longer time series.

        
            ts_name: Temporal property name to search
            pattern: Pattern to search for (TimeSeries object or template name)
            method: Distance method:
                - 'euclidean': Euclidean distance (fast)
                - 'dtw': Dynamic Time Warping (handles time shifts)
                - 'correlation': Shape similarity
                - 'shape': Derivative-based (scale invariant)
            threshold: Max distance for match (auto if None)
            normalize: Z-normalize before comparison

        Example:
            # Create a spike pattern and find stations containing it
            spike = TimeSeries.create_pattern('spike', length=10)
            stations = hg.query().nodes('Station') \
                .where_ts_pattern('num_bikes', spike, method='shape') \
                .execute()
            
            # Find drop patterns in data
            drop = TimeSeries.create_pattern('drop', length=15)
            stations = hg.query().nodes('Station') \
                .where_ts_pattern('num_bikes', drop) \
                .execute()
                
            # Using template names directly
            stations = hg.query().nodes('Station') \
                .where_ts_pattern('num_bikes', 'spike', method='shape') \
                .execute()
        """
        
        # Convert template name to pattern if needed
        if isinstance(pattern, str):
            pattern = TimeSeries.create_pattern(pattern, length=10)
        
        def make_pattern_predicate():
            def predicate(ts_obj):
                if ts_obj is None or not hasattr(ts_obj, 'contains_pattern'):
                    return False
                try:
                    return ts_obj.contains_pattern(
                        pattern=pattern,
                        method=method,
                        threshold=threshold,
                        normalize=normalize
                    )
                except Exception:
                    return False
            return predicate
        
        # Store as TimeSeries predicate (not raw measurements)
        self._ts_pattern_filters = getattr(self, '_ts_pattern_filters', [])
        self._ts_pattern_filters.append((ts_name, make_pattern_predicate()))
        return self

    def where_ts_compare(
        self,
        ts_name_right: str,
        agg_right: str,
        operator: str,
        ts_name_left: str,
        agg_left: str

    ):
        """
        Compare aggregations of two time series properties on the same entity.
        @:param    ts_name_left: First temporal property name
        @:param     agg_left: Aggregation for left property ('mean', 'min', 'max', 'std', 'sum', 'count', 'range', 'median', 'first', 'last')
        @:param     operator: Comparison operator ('<', '>', '<=', '>=', '=', '!=')
        @:param     ts_name_right: Second temporal property name
       @:param      agg_right: Aggregation for right property
        
        Example
            .where_ts_compare('num_bikes_available', 'mean', '>', 'num_bikes_disabled', 'mean')
        """
        def make_pattern_predicate(op,agg_right,agg_left):
            def predicate(ts_right,ts_left):
                agg1=ts_aggregate(ts_right,agg_right)
                agg2=ts_aggregate(ts_left,agg_left)
                if agg1 is None or agg2 is None:
                    return False
                if op in("=","=="): return abs(agg1-agg2) < 1e-9
                if op in ("!=","<>"): return abs(agg1-agg2) > 1e-9
                if op == "<": return agg1 < agg2
                if op == ">": return agg1 > agg2
                if op=="<=": return agg1 <= agg2
                if op==">=": return agg1 >= agg2
                return False
            return predicate
        self._ts_filters.append(("_compare_",(ts_name_right,ts_name_left,make_pattern_predicate(operator,agg_right,agg_left))))
        return self
    def where_ts_similar(
        self,
        ts_name: str,
        reference_ts: Optional[Any] = None,
        features: Optional[dict] = None,
        template: Optional[str] = None,
        tolerance: Optional[dict] = None
    ):
        """
        Filter by time series similarity (whole-series comparison).
        
        Match entities whose time series is similar to:
        - A reference TimeSeries object
        - A pattern template ('increasing', 'decreasing', 'spike', etc.)
            @:param ts_name: Temporal property name
            @:param reference_ts: Reference TimeSeries to match against
            @:param features: Feature dict to match {mean: X, std: Y, ...}
            @:param template: Pattern template name:
                - 'increasing': Overall upward trend
                - 'decreasing': Overall downward trend  
                - 'stable': Low variance
                - 'spike': Has significant peak
                - 'drop': Has significant drop
                - 'drop_by_N': Drops by at least N (e.g., 'drop_by_5')
            @:param tolerance: Tolerance dict for matching {mean: 0.1, std: 0.1}

        Example:
            .where_ts_similar('num_bikes', template='decreasing')
        """
        tol = tolerance or {'mean': 0.1, 'std': 0.1}
        
        def make_similarity_predicate():
            def predicate(measurements):
                if not measurements:
                    return False
                values = [v for t, v in measurements]
                if len(values) < 2:
                    return False
                
                # Match against reference TimeSeries
                if reference_ts is not None:
                    ref_values = [d[0] for d in reference_ts.data] if hasattr(reference_ts, 'data') else []
                    if not ref_values:
                        return False
                    
                    # Statistical comparison
                    ts_mean = sum(values) / len(values)
                    ref_mean = sum(ref_values) / len(ref_values)
                    ts_std = (sum((x - ts_mean) ** 2 for x in values) / len(values)) ** 0.5
                    ref_std = (sum((x - ref_mean) ** 2 for x in ref_values) / len(ref_values)) ** 0.5
                    
                    mean_tol = tol.get('mean', 0.1)
                    std_tol = tol.get('std', 0.1)
                    
                    # Check mean tolerance (relative)
                    if ref_mean != 0:
                        if abs(ts_mean - ref_mean) / abs(ref_mean) > mean_tol:
                            return False
                    elif abs(ts_mean) > mean_tol:
                        return False
                    
                    # Check std tolerance
                    if ref_std != 0:
                        if abs(ts_std - ref_std) / abs(ref_std) > std_tol:
                            return False
                    elif abs(ts_std) > std_tol:
                        return False
                    
                    return True
                
                # Match against features
                elif features is not None:
                    ts_mean = sum(values) / len(values)
                    ts_std = (sum((x - ts_mean) ** 2 for x in values) / len(values)) ** 0.5
                    
                    for feat_name, expected in features.items():
                        if feat_name == 'mean':
                            actual = ts_mean
                        elif feat_name == 'std':
                            actual = ts_std
                        elif feat_name == 'min':
                            actual = min(values)
                        elif feat_name == 'max':
                            actual = max(values)
                        elif feat_name == 'range':
                            actual = max(values) - min(values)
                        else:
                            continue
                        
                        feat_tol = tol.get(feat_name, 0.1)
                        if expected != 0:
                            if abs(actual - expected) / abs(expected) > feat_tol:
                                return False
                        elif abs(actual) > feat_tol:
                            return False
                    return True
                
                # Match against template
                elif template is not None:
                    if template == 'increasing':
                        return values[-1] > values[0]
                    elif template == 'decreasing':
                        return values[-1] < values[0]
                    elif template == 'stable':
                        mean = sum(values) / len(values)
                        std = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
                        cv = std / (abs(mean) + 1e-9)
                        return cv < 0.1
                    elif template == 'spike':
                        mean = sum(values) / len(values)
                        std = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
                        return (max(values) - mean) > 2 * std
                    elif template == 'drop':
                        mean = sum(values) / len(values)
                        std = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
                        return (mean - min(values)) > 2 * std
                    elif template.startswith('drop_by_'):
                        drop_amount = float(template.split('_')[-1])
                        return (max(values) - min(values)) >= drop_amount
                    elif template.startswith('increase_by_'):
                        increase_amount = float(template.split('_')[-1])
                        return (values[-1] - values[0]) >= increase_amount
                
                return False
            return predicate
        
        self._ts_filters.append((ts_name, make_similarity_predicate()))
        return self

    def find_pattern_matches(
        self,
        ts_name: str,
        pattern: Any,
        method: str = 'euclidean',
        threshold: Optional[float] = None,
        normalize: bool = True,
        top_k: int = 5
    ) -> List[dict]:
        """
        Find pattern occurrences across all entities and return detailed matches.
        this method returns detailed information about WHERE patterns were found within each entity's
        time series.
        
        @:param    ts_name: Temporal property name to search
        @:param    pattern: Pattern to search for (TimeSeries or template name)
        @:param    method: Distance method ('euclidean', 'dtw', 'correlation', 'shape')
        @:param    threshold: Max distance for match
        @:param    normalize: Z-normalize before comparison
        @:param    top_k: Max matches per entity
        Example:
            spike = TimeSeries.create_pattern('spike', length=10)
            results = hg.query().nodes('Station').find_pattern_matches(
                'num_bikes', spike, method='shape'
            )
            for r in results:
                print(f"Station {r['entity'].oid}:")
                for m in r['matches']:
                    print(f"  - Found at {m['start_time']} (dist={m['distance']:.3f})")
        """
        
        # Convert template name to pattern if needed
        if isinstance(pattern, str):
            pattern = TimeSeries.create_pattern(pattern, length=10)
        
        # First get candidate entities
        entities = self.execute()
        
        results = []
        for entity in entities:
            ts_obj = entity.get_temporal_property(ts_name)
            if ts_obj is None or not hasattr(ts_obj, 'find_pattern'):
                continue
            
            try:
                matches = ts_obj.find_pattern(
                    pattern=pattern,
                    method=method,
                    threshold=threshold,
                    normalize=normalize,
                    top_k=top_k
                )
                
                if matches:
                    results.append({
                        'entity': entity,
                        'matches': matches
                    })
            except Exception:
                continue
        
        return results

    # -------------------------------------------------------------------------
    # PATTERN MATCHING - Multi-entity queries
    # -------------------------------------------------------------------------
    
    def match_pattern(self):
        """
        Switch to pattern matching mode for multi-entity queries.
        
        Pattern matching allows expressing queries like:
        (n1:Station)-[e:Trip]->(n2:Station) WHERE n2.capacity < n1.capacity
        
        Returns:
            PatternQuery builder for fluent pattern construction
        
        Example:
            matches = hg.query().match_pattern() \\
                .node("n1", label="Station") \\
                .edge("e", label="Trip") \\
                .node("n2", label="Station") \\
                .where_cross("n2.capacity", "<", "n1.capacity") \\
                .execute()
        """
        return PatternQuery(self.hygraph)

    # -------------------------------------------------------------------------
    # Ordering & Limiting
    # -------------------------------------------------------------------------

    def order_by(self, key: Union[str, Callable], desc: bool = False):
        """Order results"""
        self._order_by = (key, desc)
        return self

    def limit(self, n: int):
        """Limit number of results"""
        self._limit = n
        return self

    # -------------------------------------------------------------------------
    # Execution
    # -------------------------------------------------------------------------

    def execute(self) -> List[Union[PGNode, PGEdge,TimeSeries]]:
        """Execute the query and return results"""
        if self._query_type == 'nodes':
            return self._execute_node_query()
        elif self._query_type == 'edges':
            return self._execute_edge_query()
        elif self._query_type == 'timeseries':
            return self._execute_timeseries_query()
        else:
            raise ValueError("Must specify .nodes() or .edges()")

    def execute_as_subgraph(self, name:str):
        """Execute the query and return results as a subHygraph instance"""
        results= self.execute()
        node_ids=[node_id for node_id in results]
        return self.hygraph.subgraph(node_ids, name)

    def _execute_node_query(self) -> List[PGNode]:
        """Execute node query using HybridCRUD"""
        # Query nodes WITH time series from HybridCRUD
        at_time_str = self._at_time.isoformat() if self._at_time else None

        nodes = self.hygraph.crud.query_nodes_with_timeseries(
            label=self._label,
            at_time=at_time_str,
            limit=self._limit
        )

        # Apply filters
        results = []
        for node in nodes:
            # Apply temporal filter (between)
            if self._between_times:
                start, end = self._between_times
                node_start = safe_datetime_convert(node.start_time)
                node_end = safe_datetime_convert(node.end_time)
                if node_end < start or node_start > end:
                    continue

            # Apply property filters
            # Pass the PGNode object itself to the filter
            if not all(f(node) for f in self._filters):
                continue

            # Apply time series filters
            if self._ts_filters:
                skip = False
                for ts_name, predicate in self._ts_filters:
                    if ts_name=="_compare_":
                        ts_name_right, ts_name_left,compare_pred=predicate
                        ts_left=node.get_temporal_property(ts_name_left)
                        if ts_left is None and node.has_temporal_property(ts_name_left):
                            ts_id=node.get_temporal_property_id(ts_name_left)
                            if ts_id:
                                ts_left = self.hygraph.timescale.get_measurements(
                                    entity_uid=ts_id,
                                    variable=ts_name_left
                                )
                                if ts_left:
                                    node.set_temporal_property(ts_name_left, ts_left)

                        ts_right=node.get_temporal_property(ts_name_right)
                        if ts_right is None and node.has_temporal_property(ts_name_right):
                            ts_id=node.get_temporal_property_id(ts_name_right)
                            if ts_id:
                                ts_right = self.hygraph.timescale.get_measurements(
                                    entity_uid=ts_id,
                                    variable=ts_name_right
                                )
                                if ts_right:
                                    node.set_temporal_property(ts_name_right, ts_right)
                        if ts_left is None and ts_right is None:
                           skip = True
                           break
                        if not compare_pred(ts_right,ts_left):
                            skip = True
                            break
                        continue

                    # Get TimeSeries object from the node's cache
                    ts_obj = node.get_temporal_property(ts_name)
                    
                    # If not in cache, try to load from TimescaleDB
                    if ts_obj is None and node.has_temporal_property(ts_name):
                        ts_id = node.get_temporal_property_id(ts_name)
                        if ts_id:
                            ts_obj = self.hygraph.timescale.get_measurements(
                                entity_uid=ts_id,
                                variable=ts_name
                            )
                            # Cache it for future use
                            if ts_obj:
                                node.set_temporal_property_data(ts_name, ts_obj)
                    
                    # Skip node if no timeseries data available
                    if ts_obj is None:
                        skip = True
                        break
                    
                    # Pass TimeSeries object to predicate (for ts.mean(), ts.std(), etc.)
                    try:
                        result = predicate(ts_obj)
                    except (TypeError, AttributeError) as e:
                        # Predicate might expect raw measurements (backward compatibility)
                        # Convert TimeSeries to measurements list: [(timestamp, value), ...]
                        if hasattr(ts_obj, 'timestamps') and hasattr(ts_obj, 'data'):
                            measurements = list(zip(ts_obj.timestamps, [d[0] for d in ts_obj.data]))
                            try:
                                result = predicate(measurements)
                            except Exception:
                                # If even that fails, skip this node
                                skip = True
                                break
                        else:
                            skip = True
                            break
                    
                    if not result:
                        skip = True
                        break
                if skip:
                    continue

            # Apply time series pattern filters (subsequence matching)
            ts_pattern_filters = getattr(self, '_ts_pattern_filters', [])
            if ts_pattern_filters:
                skip = False
                for ts_name, predicate in ts_pattern_filters:
                    # Get full TimeSeries object for pattern matching
                    ts_obj = node.get_temporal_property(ts_name)
                    if not predicate(ts_obj):
                        skip = True
                        break
                if skip:
                    continue

            # Apply time-bucketed filters (using TimescaleDB time_bucket)
            ts_bucketed_filters = getattr(self, '_ts_bucketed_filters', [])
            if ts_bucketed_filters:
                skip = False
                for bf in ts_bucketed_filters:
                    ts_id = node.get_temporal_property_id(bf['ts_name'])
                    if not ts_id:
                        skip = True
                        break
                    
                    # Get time range if not specified
                    start_time = bf['start_time']
                    end_time = bf['end_time']
                    if not start_time or not end_time:
                        min_ts, max_ts = self.hygraph.timescale.get_time_range(
                            ts_id, bf['ts_name']
                        )
                        start_time = start_time or min_ts
                        end_time = end_time or max_ts
                    
                    if not start_time or not end_time:
                        skip = True
                        break
                    
                    # Get bucketed data from TimescaleDB
                    bucketed = self.hygraph.timescale.get_aggregated(
                        entity_uid=ts_id,
                        variable=bf['ts_name'],
                        start_time=start_time,
                        end_time=end_time,
                        interval=bf['interval'],
                        agg_func=bf['agg_func']
                    )
                    
                    if not bucketed:
                        skip = True
                        break
                    
                    bucket_values = [v for _, v in bucketed if v is not None]
                    if not bucket_values:
                        skip = True
                        break
                    
                    # Helper to compare using operator
                    def compare_op(a, op, b):
                        if op in ('=', '=='):
                            return abs(a - b) < 1e-9
                        elif op in ('!=', '<>'):
                            return abs(a - b) >= 1e-9
                        elif op == '<':
                            return a < b
                        elif op == '<=':
                            return a <= b
                        elif op == '>':
                            return a > b
                        elif op == '>=':
                            return a >= b
                        return False
                    
                    # Evaluate condition
                    condition = bf['condition']
                    op = bf['operator']
                    val = bf['value']
                    
                    if condition == 'all':
                        # All buckets must satisfy the condition
                        passed = all(compare_op(bv, op, val) for bv in bucket_values)
                    elif condition == 'any':
                        # Any bucket must satisfy the condition
                        passed = any(compare_op(bv, op, val) for bv in bucket_values)
                    elif condition == 'majority':
                        # >50% of buckets satisfy condition
                        count_satisfied = sum(1 for bv in bucket_values if compare_op(bv, op, val))
                        passed = count_satisfied > len(bucket_values) / 2
                    elif condition == 'avg':
                        # Average of bucket values satisfies condition
                        avg_val = sum(bucket_values) / len(bucket_values)
                        passed = compare_op(avg_val, op, val)
                    else:
                        passed = False
                    
                    if not passed:
                        skip = True
                        break
                        
                if skip:
                    continue

            # Node is already a PGNode from crud.query_nodes_with_timeseries
            results.append(node)

        # Apply ordering
        if self._order_by:
            key_fn, desc = self._order_by
            if isinstance(key_fn, str):
                results.sort(key=lambda r: r.get_static_property(key_fn, 0), reverse=desc)
            else:
                results.sort(key=key_fn, reverse=desc)

        return results


    def _execute_edge_query(self) -> List[PGEdge]:
        """Execute edges query using HybridCRUD"""
        edges = []

        # Query edges from AGE
        at_time_str = self._at_time.isoformat() if self._at_time else None
        age_edges = self.hygraph.age.query_edges(
            label=self._label,
            at_time=at_time_str,
            limit=self._limit
        )

        # Get full edges data with time series
        for edge in age_edges:
            edge_full = self.hygraph.crud.get_edge_with_timeseries(edge.get('uid'))
            if edge_full:
                edges.append(edge_full)

        # Apply filters
        results = []
        for edge in edges:
            # Apply temporal filter (between)
            if self._between_times:
                start, end = self._between_times
                edge_start = safe_datetime_convert(edge.start_time)
                edge_end = safe_datetime_convert(edge.end_time)
                if edge_end < start or edge_start > end:
                    continue

            # Apply property filters
            if not all(f(edge) for f in self._filters):
                continue

            # Apply time series filters (where_ts_agg, etc.)
            if self._ts_filters:
                skip = False
                for ts_name, predicate in self._ts_filters:
                    # Handle _compare_ special case
                    if ts_name == "_compare_":
                        ts_name_right, ts_name_left, compare_pred = predicate
                        ts_left = edge.get_temporal_property(ts_name_left)
                        if ts_left is None and edge.has_temporal_property(ts_name_left):
                            ts_id = edge.get_temporal_property_id(ts_name_left)
                            if ts_id:
                                ts_left = self.hygraph.timescale.get_measurements(
                                    entity_uid=ts_id,
                                    variable=ts_name_left
                                )
                                if ts_left:
                                    edge.set_temporal_property_data(ts_name_left, ts_left)

                        ts_right = edge.get_temporal_property(ts_name_right)
                        if ts_right is None and edge.has_temporal_property(ts_name_right):
                            ts_id = edge.get_temporal_property_id(ts_name_right)
                            if ts_id:
                                ts_right = self.hygraph.timescale.get_measurements(
                                    entity_uid=ts_id,
                                    variable=ts_name_right
                                )
                                if ts_right:
                                    edge.set_temporal_property_data(ts_name_right, ts_right)

                        if ts_left is None or ts_right is None:
                            skip = True
                            break
                        if not compare_pred(ts_right, ts_left):
                            skip = True
                            break
                        continue

                    # Regular ts filter (where_ts_agg, where_ts_similar, etc.)
                    # Check if edge has this temporal property at all
                    if not edge.has_temporal_property(ts_name):
                        # Edge doesn't have this property -> skip it
                        skip = True
                        break

                    # Get TimeSeries object from the edge's cache
                    ts_obj = edge.get_temporal_property(ts_name)

                    # If not in cache, try to load from TimescaleDB
                    if ts_obj is None:
                        ts_id = edge.get_temporal_property_id(ts_name)
                        if ts_id:
                            ts_obj = self.hygraph.timescale.get_measurements(
                                entity_uid=ts_id,
                                variable=ts_name
                            )
                            # Cache it for future use
                            if ts_obj:
                                edge.set_temporal_property_data(ts_name, ts_obj)

                    # Skip edge if no timeseries data available
                    if ts_obj is None:
                        skip = True
                        break

                    # Apply the predicate
                    try:
                        result = predicate(ts_obj)
                    except (TypeError, AttributeError):
                        # Predicate might expect raw measurements (backward compatibility)
                        if hasattr(ts_obj, 'timestamps') and hasattr(ts_obj, 'data'):
                            measurements = list(zip(ts_obj.timestamps, [d[0] for d in ts_obj.data]))
                            try:
                                result = predicate(measurements)
                            except Exception:
                                skip = True
                                break
                        else:
                            skip = True
                            break

                    if not result:
                        skip = True
                        break

                if skip:
                    continue

            # Apply time series pattern filters (subsequence matching)
            ts_pattern_filters = getattr(self, '_ts_pattern_filters', [])
            if ts_pattern_filters:
                skip = False
                for ts_name, predicate in ts_pattern_filters:
                    if not edge.has_temporal_property(ts_name):
                        skip = True
                        break
                    ts_obj = edge.get_temporal_property(ts_name)
                    if not predicate(ts_obj):
                        skip = True
                        break
                if skip:
                    continue

            # Apply time-bucketed filters
            ts_bucketed_filters = getattr(self, '_ts_bucketed_filters', [])
            if ts_bucketed_filters:
                skip = False
                for bf in ts_bucketed_filters:
                    ts_id = edge.get_temporal_property_id(bf['ts_name'])
                    if not ts_id:
                        skip = True
                        break

                    start_time = bf['start_time']
                    end_time = bf['end_time']
                    if not start_time or not end_time:
                        min_ts, max_ts = self.hygraph.timescale.get_time_range(
                            ts_id, bf['ts_name']
                        )
                        start_time = start_time or min_ts
                        end_time = end_time or max_ts

                    if not start_time or not end_time:
                        skip = True
                        break

                    bucketed = self.hygraph.timescale.get_aggregated(
                        entity_uid=ts_id,
                        variable=bf['ts_name'],
                        start_time=start_time,
                        end_time=end_time,
                        interval=bf['interval'],
                        agg_func=bf['agg_func']
                    )

                    if not bucketed:
                        skip = True
                        break

                    bucket_values = [v for _, v in bucketed if v is not None]
                    if not bucket_values:
                        skip = True
                        break

                    def compare_op(a, op, b):
                        if op in ('=', '=='):
                            return abs(a - b) < 1e-9
                        elif op in ('!=', '<>'):
                            return abs(a - b) >= 1e-9
                        elif op == '<':
                            return a < b
                        elif op == '<=':
                            return a <= b
                        elif op == '>':
                            return a > b
                        elif op == '>=':
                            return a >= b
                        return False

                    condition = bf['condition']
                    op = bf['operator']
                    val = bf['value']

                    if condition == 'all':
                        passed = all(compare_op(bv, op, val) for bv in bucket_values)
                    elif condition == 'any':
                        passed = any(compare_op(bv, op, val) for bv in bucket_values)
                    elif condition == 'majority':
                        count_satisfied = sum(1 for bv in bucket_values if compare_op(bv, op, val))
                        passed = count_satisfied > len(bucket_values) / 2
                    elif condition == 'avg':
                        avg_val = sum(bucket_values) / len(bucket_values)
                        passed = compare_op(avg_val, op, val)
                    else:
                        passed = False

                    if not passed:
                        skip = True
                        break

                if skip:
                    continue

            # Edge passed all filters
            results.append(edge)

        return results

    def _execute_timeseries_query(self):
        """
        Execute timeseries query.

        Supported:
          - hg.query().timeseries(ts_id, variable).between(...).execute() -> TimeSeries
          - hg.query().timeseries(ts_id).execute() -> dict[var -> TimeSeries]
        """
        if not self._entity_uid:
            raise ValueError("timeseries() requires an entity_oid/ts_id")

        # Interpret time constraints
        start_time = None
        end_time = None
        if self._between_times:
            start_time, end_time = self._between_times
        elif self._at_time:
            # up to timestamp; then keep last sample
            end_time = self._at_time

        def keep_last_point(ts_obj):
            if ts_obj and ts_obj.timestamps:
                ts_obj.timestamps = [ts_obj.timestamps[-1]]
                ts_obj.data = [ts_obj.data[-1]]
            return ts_obj

        # If variable is specified, return a single TimeSeries
        if self._variable:
            ts_obj = self.hygraph.timescale.get_measurements(
                entity_uid=self._entity_uid,
                variable=self._variable,
                start_time=start_time,
                end_time=end_time
            )
            if self._at_time:
                ts_obj = keep_last_point(ts_obj)
            return ts_obj

        # Otherwise return all variables for this ts_id
        vars_ = self.hygraph.timescale.get_all_variables(self._entity_uid)
        out = {}
        for v in vars_:
            ts_obj = self.hygraph.timescale.get_measurements(
                entity_uid=self._entity_uid,
                variable=v,
                start_time=start_time,
                end_time=end_time
            )
            if self._at_time:
                ts_obj = keep_last_point(ts_obj)
            if ts_obj:
                out[v] = ts_obj
        return out
    # -------------------------------------------------------------------------
    # Convenience Methods
    # -------------------------------------------------------------------------

    def count(self) -> int:
        """Count matching entities"""
        return len(self.execute())

    def first(self) -> Optional[Union[PGNode, PGEdge]]:
        """Get first result"""
        results = self.limit(1).execute()
        return results[0] if results else None

    def exists(self) -> bool:
        """Check if any matching entities exist"""
        return self.count() > 0




class EntityUpdater:
    """Fluent interface for updating entities"""

    def __init__(self, hygraph, entity_type: str, entity_id: str):
        self.hygraph = hygraph
        self.entity_type = entity_type  # 'node' or 'edges'
        self.entity_id = entity_id
        self.updates = {}
        self.new_start = None
        self.new_end = None
        self.new_ts_data = {}  # {variable: [(ts, val), ...]}

    def set_property(self, name: str, value: Any):
        """Update a property"""
        self.updates[name] = value
        return self

    def set_properties(self, **kwargs):
        """Update multiple properties"""
        self.updates.update(kwargs)
        return self

    def update_timeseries(self, variable: str, measurements: List[tuple]):
        """
        Update/add time series data.

        
            variable: Variable name
            measurements: List of (timestamp, value) tuples
        """
        self.new_ts_data[variable] = measurements
        return self

    def extend_validity(self, new_end: Union[str, datetime]):
        """Extend the end time"""
        if isinstance(new_end, str):
            new_end = safe_datetime_convert(new_end)
        self.new_end = new_end
        return self

    def change_validity(self, start: Union[str, datetime], end: Union[str, datetime]):
        """Change validity period"""
        if isinstance(start, str):
            start = safe_datetime_convert(start)
        if isinstance(end, str):
            end = safe_datetime_convert(end)
        self.new_start = start
        self.new_end = end
        return self

    def execute(self):
        """Apply the updates"""
        if self.entity_type == 'node':
            # Update static properties
            if self.updates:
                self.hygraph.age.update_node(self.entity_id, self.updates)

            # Update temporal validity
            if self.new_start or self.new_end:
                # Get current node data
                node = self.hygraph.age.get_node(self.entity_id)
                if node:
                    import json
                    props = node.get('properties', {})
                    if isinstance(props, str):
                        props = json.loads(props)

                    # Update times in properties if needed
                    # (This is a simplification - you may need more logic)

            # Update time series
            for variable, measurements in self.new_ts_data.items():
                self.hygraph.crud.update_node_timeseries(
                    uid=self.entity_id,
                    variable=variable,
                    new_measurements=measurements
                )

        elif self.entity_type == 'edges':
            # Update static properties
            if self.updates:
                self.hygraph.age.update_edge(self.entity_id, self.updates)

            # Update time series (similar to nodes)
            for variable, measurements in self.new_ts_data.items():
                # Convert to measurement rows
                rows = [(self.entity_id, variable, ts, val)
                        for ts, val in measurements]
                self.hygraph.timescale.insert_measurements(rows)

        return self




class EntityDeleter:
    """Fluent interface for deleting entities"""

    def __init__(self, hygraph, entity_type: str, entity_id: str):
        self.hygraph = hygraph
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.hard_delete = False

    def hard(self):
        """Perform hard delete (remove from graph AND time series)"""
        self.hard_delete = True
        return self

    def execute(self):
        """Execute the deletion"""
        if self.entity_type == 'node':
            self.hygraph.crud.delete_node_with_timeseries(
                uid=self.entity_id,
                hard=self.hard_delete
            )
        elif self.entity_type == 'edges':
            self.hygraph.crud.delete_edge_with_timeseries(
                uid=self.entity_id,
                hard=self.hard_delete
            )

        return self




class HyGraphCRUD:
    """
    Mixin to add fluent CRUD operations to HyGraph.

    This mixin requires HyGraph to have:
    - self.crud: HybridCRUD instance
    - self.age: AGEStore instance
    - self.timescale: TSStore instance

    Usage:
        class HyGraph(HyGraphCRUD):
            def __init__(self, db_pool):
                self.age = AGEStore(db_pool)
                self.timescale = TSStore(db_pool)
                self.crud = HybridCRUD(self.age, self.timescale)

    Then:
        hg = HyGraph(db_pool)

        # CREATE
        hg.create_node('station_1')\\
            .with_label('Station')\\
            .with_property('capacity', 50)\\
            .with_ts_property('bikes', ts_data)\\
            .create()

        # READ
        results = hg.query().nodes(label='Station')\\
            .where_ts('bikes', lambda m: len(m) > 100)\\
            .execute()

        # UPDATE
        hg.update_node('station_1')\\
            .set_property('capacity', 60)\\
            .update_timeseries('bikes', [(ts, val), ...])\\
            .execute()

        # DELETE
        hg.delete_node('station_1').hard().execute()
    """

    def query(self) -> QueryBuilder:
        """Start a query"""
        return QueryBuilder(self)

    def create_node(self, oid: Optional[str] = None) -> NodeCreator:
        """Create a new node"""
        return NodeCreator(self, oid)

    def create_edge(self, source: str, target: str, oid: Optional[str] = None) -> EdgeCreator:
        """Create a new edges"""
        return EdgeCreator(self, source, target, oid)

    def update_node(self, node_id: str) -> EntityUpdater:
        """Update a node"""
        return EntityUpdater(self, 'node', node_id)

    def update_edge(self, edge_id: str) -> EntityUpdater:
        """Update an edges"""
        return EntityUpdater(self, 'edges', edge_id)

    def delete_node(self, node_id: str) -> EntityDeleter:
        """Delete a node"""
        return EntityDeleter(self, 'node', node_id)

    def delete_edge(self, edge_id: str) -> EntityDeleter:
        """Delete an edges"""
        return EntityDeleter(self, 'edges', edge_id)




@dataclass
class NodePattern:
    """Definition of a node in a pattern."""
    variable: str
    label: Optional[str] = None
    uid: Optional[str] = None
    uid_candidates: Optional[set] = None  # Set of allowed node IDs (for Diff→Pattern integration)
    static_constraints: List[Tuple[str, str, Any]] = field(default_factory=list)  # [(prop, op, val)]
    ts_constraints: List[Tuple[str, str, str, float]] = field(default_factory=list)  # [(prop, agg, op, val)]
    ts_similarity: Optional[dict] = None  # {ts_name, reference_ts, features, template, tolerance}


@dataclass 
class EdgePattern:
    """Definition of an edge in a pattern."""
    variable: str
    source_var: str
    target_var: str
    label: Optional[str] = None
    direction: str = "out"  # "out", "in", "both"
    static_constraints: List[Tuple[str, str, Any]] = field(default_factory=list)
    ts_constraints: List[Tuple[str, str, str, float]] = field(default_factory=list)


@dataclass
class CrossConstraint:
    """Cross-entity constraint like n2.capacity < n1.capacity."""
    left_var: str
    left_prop: str
    left_agg: Optional[str]  # For temporal: 'mean', 'max', etc.
    operator: str
    right_var: Optional[str]  # None if comparing to constant
    right_prop: Optional[str]
    right_agg: Optional[str]
    right_value: Optional[Any]  # If comparing to constant


@dataclass
class CrossTSConstraint:
    """
    Cross-entity TIME SERIES constraint.
    
    Compares entire time series between two entities using:
    - correlation: Pearson correlation coefficient
    - similar: Shape/statistical similarity
    - dtw: Dynamic Time Warping distance
    - leads: One TS leads/predicts another
    - pattern_match: One TS contains pattern from another
    """
    left_var: str
    left_ts_prop: str
    operator: str  # 'correlates', 'similar_to', 'leads', 'lags', 'diverges_from'
    right_var: str
    right_ts_prop: str
    threshold: float = 0.7  # Threshold for the comparison
    lag: int = 0  # For leads/lags comparison


class PatternQuery:
    """
    Fluent API for pattern matching queries.
    
    Allows expressing complex graph patterns with:
    - Multiple nodes and edges
    - Static property constraints
    - Temporal property constraints (aggregated)
    - Time series similarity matching
    - Cross-entity comparisons
    
    Example:
        # Find trips where destination has lower capacity than source
        matches = hg.query().match_pattern() \\
            .node("n1", label="Station") \\
            .edge("e", label="Trip") \\
            .node("n2", label="Station") \\
            .where_node("n1", static={"capacity": (">", 40)}) \\
            .where_node("n2", ts={"num_bikes": ("mean", "<", 10)}) \\
            .where_cross("n2.capacity", "<", "n1.capacity") \\
            .execute()
    """
    
    def __init__(self, hygraph):
        self.hygraph = hygraph
        self._nodes: dict = {}  # var -> NodePattern
        self._edges: list = []  # List[EdgePattern]
        self._cross_constraints: list = []  # List[CrossConstraint]
        self._cross_ts_constraints: list = []  # List[CrossTSConstraint]
        self._limit: Optional[int] = None
    
    # -------------------------------------------------------------------------
    # Pattern Structure
    # -------------------------------------------------------------------------
    
    def node(
        self, 
        variable: str, 
        label: Optional[str] = None, 
        uid: Optional[str] = None,
        uid_in: Optional[List[str]] = None
    ):
        """
        Add a node to the pattern.
        
        
            variable: Variable name (e.g., "n1", "source", "station")
            label: Optional node label to filter by
            uid: Optional specific node ID to match
            uid_in: Optional list/set of node IDs to restrict candidates
                    (for Diff→Pattern integration)
        
        Example:
            .node("n1", label="Station")
            .node("specific", uid="station_123")
            
            # Diff → Pattern: restrict to nodes from diff query
            declining_ids = diff.query().changed_nodes().where_ts_change(...).as_node_ids()
            .node("n1", label="Station", uid_in=declining_ids)
        """
        uid_candidates = set(uid_in) if uid_in else None
        self._nodes[variable] = NodePattern(
            variable=variable, 
            label=label, 
            uid=uid,
            uid_candidates=uid_candidates
        )
        return self
    
    def edge(
        self, 
        variable: str, 
        label: Optional[str] = None,
        source: Optional[str] = None,
        target: Optional[str] = None,
        direction: str = "out"
    ):
        """
        Add an edge to the pattern.
        
        If source/target not specified, connects last two nodes.
        
        
            variable: Variable name (e.g., "e", "trip")
            label: Optional edge label
            source: Source node variable (default: second-to-last node)
            target: Target node variable (default: last node)
            direction: "out" (->), "in" (<-), or "both" (-)
        
        Example:
            .node("n1").edge("e", label="Trip").node("n2")
            # Creates: (n1)-[e:Trip]->(n2)
        """
        # Default: connect last two nodes
        if source is None or target is None:
            node_vars = list(self._nodes.keys())
            if len(node_vars) >= 2:
                if source is None:
                    source = node_vars[-2]
                if target is None:
                    target = node_vars[-1]
            else:
                raise ValueError("Edge requires at least 2 nodes. Define nodes first.")
        
        self._edges.append(EdgePattern(
            variable=variable,
            source_var=source,
            target_var=target,
            label=label,
            direction=direction
        ))
        return self
    
    # -------------------------------------------------------------------------
    # Constraints
    # -------------------------------------------------------------------------
    
    def where_node(
        self,
        variable: str,
        static: Optional[dict] = None,
        ts: Optional[dict] = None,
        ts_similar: Optional[dict] = None,
        uid_in: Optional[List[str]] = None
    ):
        """
        Add constraints to a node.
        
        
            variable: Node variable name
            static: Static property constraints {prop: (op, value)}
            ts: Temporal property constraints {prop: (agg, op, value)}
            ts_similar: Time series similarity {ts_name, reference_ts/features/template, tolerance}
            uid_in: Optional list/set of node IDs to restrict candidates
                    (for Diff→Pattern integration)
        
        Example:
            .where_node("n1", 
                static={"capacity": (">", 40), "name": ("=", "Central")},
                ts={"num_bikes": ("mean", "<", 10)}
            )
            
            .where_node("n1",
                ts_similar={"ts_name": "num_bikes", "template": "decreasing"}
            )
            
            # Diff → Pattern: restrict to nodes from diff query
            declining_ids = diff.query().changed_nodes().where_ts_change(...).as_node_ids()
            .where_node("n1", uid_in=declining_ids)
        """
        if variable not in self._nodes:
            raise ValueError(f"Node '{variable}' not defined. Use .node() first.")
        
        node_pattern = self._nodes[variable]
        
        if static:
            for prop, (op, val) in static.items():
                node_pattern.static_constraints.append((prop, op, val))
        
        if ts:
            for prop, (agg, op, val) in ts.items():
                node_pattern.ts_constraints.append((prop, agg, op, val))
        
        if ts_similar:
            node_pattern.ts_similarity = ts_similar
        
        if uid_in:
            # Merge with existing candidates if any
            new_candidates = set(uid_in)
            if node_pattern.uid_candidates:
                node_pattern.uid_candidates &= new_candidates
            else:
                node_pattern.uid_candidates = new_candidates
        
        return self
    
    def where_edge(
        self,
        variable: str,
        static: Optional[dict] = None,
        ts: Optional[dict] = None
    ):
        """
        Add constraints to an edge.
        
        
            variable: Edge variable name
            static: Static property constraints {prop: (op, value)}
            ts: Temporal property constraints {prop: (agg, op, value)}
        
        Example:
            .where_edge("e", static={"distance": ("<", 5)})
        """
        edge_pattern = None
        for ep in self._edges:
            if ep.variable == variable:
                edge_pattern = ep
                break
        
        if edge_pattern is None:
            raise ValueError(f"Edge '{variable}' not defined. Use .edge() first.")
        
        if static:
            for prop, (op, val) in static.items():
                edge_pattern.static_constraints.append((prop, op, val))
        
        if ts:
            for prop, (agg, op, val) in ts.items():
                edge_pattern.ts_constraints.append((prop, agg, op, val))
        
        return self
    
    def where_cross(self, left: str, operator: str, right: str):
        """
        Add cross-entity constraint.
        
        Compares properties between different entities in the pattern.
        
        
            left: "variable.property" or "variable.property:aggregation"
            operator: Comparison operator ("<", ">", "<=", ">=", "=", "!=")
            right: "variable.property" or constant value
        
        Example:
            # Compare static properties
            .where_cross("n2.capacity", "<", "n1.capacity")
            
            # Compare temporal properties with aggregation
            .where_cross("n2.num_bikes:mean", ">", "n1.num_bikes:mean")
            
            # Compare to constant
            .where_cross("n1.capacity", ">", "50")
        """
        # Parse left side: "var.prop" or "var.prop:agg"
        left_var, left_prop, left_agg = self._parse_prop_ref(left)
        
        # Parse right side (property reference or constant)
        try:
            # Try as number first
            right_value = float(right)
            right_var, right_prop, right_agg = None, None, None
        except (ValueError, TypeError):
            if '.' in str(right):
                # Parse as property reference
                right_var, right_prop, right_agg = self._parse_prop_ref(right)
                right_value = None
            else:
                # String constant
                right_value = right
                right_var, right_prop, right_agg = None, None, None
        
        self._cross_constraints.append(CrossConstraint(
            left_var=left_var,
            left_prop=left_prop,
            left_agg=left_agg,
            operator=operator,
            right_var=right_var,
            right_prop=right_prop,
            right_agg=right_agg,
            right_value=right_value
        ))
        return self
    
    def where_cross_ts(
        self,
        left: str,
        operator: str,
        right: str,
        threshold: float = 0.7,
        lag: int = 0
    ):
        """
        Add cross-entity TIME SERIES constraint.
        
        Compares entire time series between two entities, not just aggregated values.
        
        
            left: "variable.ts_property" (e.g., "n1.num_bikes")
            operator: Comparison type:
                - 'correlates': Pearson correlation >= threshold
                - 'anti_correlates': Pearson correlation <= -threshold  
                - 'similar_to': Statistical similarity (mean, std within tolerance)
                - 'leads': left TS leads right TS by `lag` time steps
                - 'lags': left TS lags behind right TS
                - 'diverges_from': Correlation < threshold (opposite of correlates)
                - 'more_volatile': left has higher std than right
                - 'more_stable': left has lower std than right
            right: "variable.ts_property" (e.g., "n2.num_bikes")
            threshold: Threshold for comparison (default 0.7)
                - For 'correlates': minimum correlation coefficient
                - For 'similar_to': maximum relative difference in stats
            lag: Time lag for leads/lags comparisons
        
        Example:
            # Find trips where origin and destination bike counts are correlated
            .where_cross_ts("n1.num_bikes", "correlates", "n2.num_bikes", threshold=0.6)
            
            # Find pairs where one station's pattern predicts another
            .where_cross_ts("n1.num_bikes", "leads", "n2.num_bikes", lag=2)
            
            # Find stations with similar behavior
            .where_cross_ts("n1.num_bikes", "similar_to", "n2.num_bikes", threshold=0.2)
            
            # Find stations that move opposite to each other
            .where_cross_ts("n1.num_bikes", "anti_correlates", "n2.num_bikes")
            
            # Find where source is more volatile than destination
            .where_cross_ts("n1.num_bikes", "more_volatile", "n2.num_bikes")
        """
        # Parse left: "var.ts_prop"
        left_parts = left.split('.')
        if len(left_parts) != 2:
            raise ValueError(f"Invalid left reference: {left}. Expected 'var.ts_property'")
        left_var, left_ts_prop = left_parts
        
        # Parse right: "var.ts_prop"
        right_parts = right.split('.')
        if len(right_parts) != 2:
            raise ValueError(f"Invalid right reference: {right}. Expected 'var.ts_property'")
        right_var, right_ts_prop = right_parts
        
        self._cross_ts_constraints.append(CrossTSConstraint(
            left_var=left_var,
            left_ts_prop=left_ts_prop,
            operator=operator,
            right_var=right_var,
            right_ts_prop=right_ts_prop,
            threshold=threshold,
            lag=lag
        ))
        return self
    
    def _parse_prop_ref(self, ref: str) -> Tuple[str, str, Optional[str]]:
        """Parse 'var.prop' or 'var.prop:agg' format."""
        parts = ref.split('.')
        if len(parts) != 2:
            raise ValueError(f"Invalid property reference: {ref}. Expected 'var.prop'")
        
        var = parts[0]
        prop_part = parts[1]
        
        if ':' in prop_part:
            prop, agg = prop_part.split(':')
        else:
            prop, agg = prop_part, None
        
        return var, prop, agg
    
    def limit(self, n: int):
        """Limit number of results."""
        self._limit = n
        return self
    
    def from_diff_query(self, variable: str, diff_query_result: List[str]):
        """
        Restrict a node variable to results from a diff query.
        
        Convenience method for Diff→Pattern integration.
        
        
            variable: Node variable name (must be already defined)
            diff_query_result: List of node IDs from diff.query()...as_node_ids()
        
        Example:
            # Step 1: Get declining stations from diff
            declining_ids = diff.query() \\
                .changed_nodes(label='Station') \\
                .where_ts_change('num_bikes_available', 'mean', '<', -10) \\
                .as_node_ids()
            
            # Step 2: Find patterns among declining stations
            problem_clusters = hg.query().match_pattern() \\
                .node("n1", label="Station") \\
                .edge("e", label="Trip") \\
                .node("n2", label="Station") \\
                .from_diff_query("n1", declining_ids) \\
                .from_diff_query("n2", declining_ids) \\
                .execute()
            
            # Result: Pairs of connected declining stations
        """
        if variable not in self._nodes:
            raise ValueError(f"Node '{variable}' not defined. Use .node() first.")
        
        node_pattern = self._nodes[variable]
        new_candidates = set(diff_query_result)
        
        if node_pattern.uid_candidates:
            node_pattern.uid_candidates &= new_candidates
        else:
            node_pattern.uid_candidates = new_candidates
        
        return self
    
    # -------------------------------------------------------------------------
    # Execution
    # -------------------------------------------------------------------------
    
    def execute(self) -> List[dict]:
        """
        Execute the pattern query.
        
        Returns:
            List of match dictionaries: [{var: entity, ...}, ...]
        
        Example:
            matches = pattern.execute()
            for match in matches:
                n1, e, n2 = match['n1'], match['e'], match['n2']
                print(f"{n1.oid} -> {n2.oid}")
        """
        if len(self._edges) == 0:
            return self._execute_node_only()
        elif len(self._edges) == 1:
            return self._execute_single_edge()
        else:
            return self._execute_multi_edge()
    
    def _execute_node_only(self) -> List[dict]:
        """Execute pattern with only nodes (no edges)."""
        matches = []
        
        for var, node_pattern in self._nodes.items():
            # Query candidates
            if node_pattern.uid:
                # Specific node requested
                node = self.hygraph.crud.get_node_with_timeseries(node_pattern.uid)
                candidates = [node] if node else []
            elif node_pattern.uid_candidates:
                # Diff→Pattern: load only the specified candidate nodes
                # This is the optimization for uid_in filtering
                candidates = []
                for uid in node_pattern.uid_candidates:
                    node = self.hygraph.crud.get_node_with_timeseries(uid)
                    if node:
                        # Still check label if specified
                        if node_pattern.label is None or node.label == node_pattern.label:
                            candidates.append(node)
                    if self._limit and len(candidates) >= self._limit * 2:
                        break  # Early termination for large candidate sets
            elif node_pattern.label:
                candidates = self.hygraph.crud.query_nodes_with_timeseries(
                    label=node_pattern.label,
                    limit=self._limit
                )
            else:
                candidates = self.hygraph.crud.query_nodes_with_timeseries(limit=self._limit)
            
            # Filter by constraints
            for node in candidates:
                if self._node_matches(node, node_pattern):
                    matches.append({var: node})
                
                if self._limit and len(matches) >= self._limit:
                    break
        
        return matches
    
    def _execute_single_edge(self) -> List[dict]:
        """Execute pattern with single edge: (n1)-[e]->(n2)."""
        matches = []
        edge_pattern = self._edges[0]
        
        source_pattern = self._nodes.get(edge_pattern.source_var)
        target_pattern = self._nodes.get(edge_pattern.target_var)
        
        # Query edges
        edges = self.hygraph.age.query_edges(
            label=edge_pattern.label,
            limit=(self._limit * 10) if self._limit else None
        )
        
        for edge_dict in edges:
            edge = self.hygraph.crud.get_edge_with_timeseries(edge_dict.get('uid'))
            if edge is None:
                continue
            
            # Check edge constraints
            if not self._edge_matches(edge, edge_pattern):
                continue
            
            # Get source and target nodes
            source_node = self.hygraph.crud.get_node_with_timeseries(edge.source)
            target_node = self.hygraph.crud.get_node_with_timeseries(edge.target)
            
            if source_node is None or target_node is None:
                continue
            
            # Check node constraints
            if source_pattern and not self._node_matches(source_node, source_pattern):
                continue
            if target_pattern and not self._node_matches(target_node, target_pattern):
                continue
            
            # Build bindings
            bindings = {
                edge_pattern.source_var: source_node,
                edge_pattern.variable: edge,
                edge_pattern.target_var: target_node
            }
            
            # Check cross-entity constraints
            if not self._check_cross_constraints(bindings):
                continue
            
            matches.append(bindings)
            
            if self._limit and len(matches) >= self._limit:
                break
        
        return matches
    
    def _execute_multi_edge(self) -> List[dict]:
        """Execute pattern with multiple edges."""
        # Start with first edge results
        first_results = self._execute_single_edge()
        
        # Extend with remaining edges
        matches = []
        for partial_match in first_results:
            extended = self._extend_match(partial_match, 1)
            matches.extend(extended)
            
            if self._limit and len(matches) >= self._limit:
                break
        
        return matches[:self._limit] if self._limit else matches
    
    def _extend_match(self, partial_match: dict, edge_idx: int) -> List[dict]:
        """Extend a partial match with additional edges."""
        if edge_idx >= len(self._edges):
            return [partial_match]
        
        edge_pattern = self._edges[edge_idx]
        results = []
        
        # Get bound entities
        source_entity = partial_match.get(edge_pattern.source_var)
        target_entity = partial_match.get(edge_pattern.target_var)
        
        # Find matching edges
        if source_entity:
            edges = self.hygraph.age.query_edges(
                source=source_entity.oid,
                label=edge_pattern.label
            )
        elif target_entity:
            edges = self.hygraph.age.query_edges(
                target=target_entity.oid,
                label=edge_pattern.label
            )
        else:
            return []
        
        for edge_dict in edges:
            edge = self.hygraph.crud.get_edge_with_timeseries(edge_dict.get('uid'))
            if edge is None or not self._edge_matches(edge, edge_pattern):
                continue
            
            new_bindings = dict(partial_match)
            new_bindings[edge_pattern.variable] = edge
            
            # Get unbound endpoint
            if source_entity is None:
                new_node = self.hygraph.crud.get_node_with_timeseries(edge.source)
                node_pattern = self._nodes.get(edge_pattern.source_var)
                if new_node and (node_pattern is None or self._node_matches(new_node, node_pattern)):
                    new_bindings[edge_pattern.source_var] = new_node
                else:
                    continue
            
            if target_entity is None:
                new_node = self.hygraph.crud.get_node_with_timeseries(edge.target)
                node_pattern = self._nodes.get(edge_pattern.target_var)
                if new_node and (node_pattern is None or self._node_matches(new_node, node_pattern)):
                    new_bindings[edge_pattern.target_var] = new_node
                else:
                    continue
            
            # Check cross constraints
            if self._check_cross_constraints(new_bindings):
                results.extend(self._extend_match(new_bindings, edge_idx + 1))
        
        return results
    
    # -------------------------------------------------------------------------
    # Constraint Checking
    # -------------------------------------------------------------------------
    
    def _node_matches(self, node, pattern: NodePattern) -> bool:
        """Check if node matches pattern constraints."""
        # Check uid_candidates first (Diff→Pattern filter) - early exit optimization
        if pattern.uid_candidates and node.oid not in pattern.uid_candidates:
            return False
        
        # Check label
        if pattern.label and node.label != pattern.label:
            return False
        
        # Check static constraints
        for prop, op, val in pattern.static_constraints:
            actual = node.get_static_property(prop)
            if not self._compare(actual, op, val):
                return False
        
        # Check temporal constraints
        for prop, agg, op, val in pattern.ts_constraints:
            ts = node.get_temporal_property(prop)
            if ts is None:
                return False
            agg_val = self._aggregate_ts(ts, agg)
            if agg_val is None or not self._compare(agg_val, op, val):
                return False
        
        # Check similarity
        if pattern.ts_similarity:
            ts_name = pattern.ts_similarity.get('ts_name')
            ts = node.get_temporal_property(ts_name)
            if ts is None:
                return False
            if not self._check_ts_similarity(ts, pattern.ts_similarity):
                return False
        
        return True
    
    def _edge_matches(self, edge, pattern: EdgePattern) -> bool:
        """Check if edge matches pattern constraints."""
        # Check label
        if pattern.label and edge.label != pattern.label:
            return False
        
        # Check static constraints
        for prop, op, val in pattern.static_constraints:
            actual = edge.get_static_property(prop)
            if not self._compare(actual, op, val):
                return False
        
        # Check temporal constraints
        for prop, agg, op, val in pattern.ts_constraints:
            ts = edge.get_temporal_property(prop)
            if ts is None:
                return False
            agg_val = self._aggregate_ts(ts, agg)
            if agg_val is None or not self._compare(agg_val, op, val):
                return False
        
        return True
    
    def _check_cross_constraints(self, bindings: dict) -> bool:
        """Check all cross-entity constraints (static and aggregated TS)."""
        for cc in self._cross_constraints:
            left_entity = bindings.get(cc.left_var)
            if left_entity is None:
                return False
            
            left_val = self._get_entity_value(left_entity, cc.left_prop, cc.left_agg)
            if left_val is None:
                return False
            
            if cc.right_value is not None:
                right_val = cc.right_value
            else:
                right_entity = bindings.get(cc.right_var)
                if right_entity is None:
                    return False
                right_val = self._get_entity_value(right_entity, cc.right_prop, cc.right_agg)
                if right_val is None:
                    return False
            
            if not self._compare(left_val, cc.operator, right_val):
                return False
        
        # Also check cross-TS constraints
        if not self._check_cross_ts_constraints(bindings):
            return False
        
        return True
    
    def _check_cross_ts_constraints(self, bindings: dict) -> bool:
        """
        Check all cross-entity TIME SERIES constraints.
        
        Compares entire time series between entities using correlation,
        similarity, or other TS-specific metrics.
        """
        import numpy as np
        
        for ctc in self._cross_ts_constraints:
            # Get left entity and its time series
            left_entity = bindings.get(ctc.left_var)
            if left_entity is None:
                return False
            
            left_ts = left_entity.get_temporal_property(ctc.left_ts_prop)
            if left_ts is None or left_ts.length < 2:
                return False
            
            # Get right entity and its time series
            right_entity = bindings.get(ctc.right_var)
            if right_entity is None:
                return False
            
            right_ts = right_entity.get_temporal_property(ctc.right_ts_prop)
            if right_ts is None or right_ts.length < 2:
                return False
            
            # Get values as numpy arrays
            left_values = left_ts.to_numpy().flatten()
            right_values = right_ts.to_numpy().flatten()
            
            # Align lengths (use minimum)
            min_len = min(len(left_values), len(right_values))
            if min_len < 2:
                return False
            
            left_values = left_values[:min_len]
            right_values = right_values[:min_len]
            
            # Apply comparison based on operator
            if ctc.operator == 'correlates':
                # Pearson correlation >= threshold
                corr = np.corrcoef(left_values, right_values)[0, 1]
                if np.isnan(corr) or corr < ctc.threshold:
                    return False
                    
            elif ctc.operator == 'anti_correlates':
                # Negative correlation <= -threshold
                corr = np.corrcoef(left_values, right_values)[0, 1]
                if np.isnan(corr) or corr > -ctc.threshold:
                    return False
                    
            elif ctc.operator == 'similar_to':
                # Statistical similarity: mean and std within threshold tolerance
                left_mean, left_std = np.mean(left_values), np.std(left_values)
                right_mean, right_std = np.mean(right_values), np.std(right_values)
                
                # Check mean similarity (relative difference)
                if right_mean != 0:
                    mean_diff = abs(left_mean - right_mean) / abs(right_mean)
                else:
                    mean_diff = abs(left_mean)
                
                # Check std similarity
                if right_std != 0:
                    std_diff = abs(left_std - right_std) / abs(right_std)
                else:
                    std_diff = abs(left_std)
                
                if mean_diff > ctc.threshold or std_diff > ctc.threshold:
                    return False
                    
            elif ctc.operator == 'leads':
                # Left time series leads right by `lag` steps
                # High correlation when left is shifted forward
                if ctc.lag <= 0 or ctc.lag >= min_len:
                    return False
                corr = np.corrcoef(left_values[:-ctc.lag], right_values[ctc.lag:])[0, 1]
                if np.isnan(corr) or corr < ctc.threshold:
                    return False
                    
            elif ctc.operator == 'lags':
                # Left time series lags behind right by `lag` steps
                if ctc.lag <= 0 or ctc.lag >= min_len:
                    return False
                corr = np.corrcoef(left_values[ctc.lag:], right_values[:-ctc.lag])[0, 1]
                if np.isnan(corr) or corr < ctc.threshold:
                    return False
                    
            elif ctc.operator == 'diverges_from':
                # Low correlation (opposite of correlates)
                corr = np.corrcoef(left_values, right_values)[0, 1]
                if np.isnan(corr):
                    corr = 0  # Treat NaN as uncorrelated
                if abs(corr) >= ctc.threshold:
                    return False
                    
            elif ctc.operator == 'more_volatile':
                # Left has higher std than right
                if np.std(left_values) <= np.std(right_values):
                    return False
                    
            elif ctc.operator == 'more_stable':
                # Left has lower std than right
                if np.std(left_values) >= np.std(right_values):
                    return False
                    
            elif ctc.operator == 'contains_pattern_from':
                # Left contains a pattern similar to right's overall shape
                # Uses the TimeSeries.contains_pattern method
                if hasattr(left_ts, 'contains_pattern'):
                    if not left_ts.contains_pattern(pattern=right_ts, threshold=ctc.threshold):
                        return False
                else:
                    return False
                    
            else:
                # Unknown operator
                return False
        
        return True
    
    def _get_entity_value(self, entity, prop: str, agg: Optional[str]) -> Any:
        """Get property value from entity, optionally aggregating if temporal."""
        # Try static first
        static_val = entity.get_static_property(prop)
        if static_val is not None:
            return static_val
        
        # Try temporal
        ts = entity.get_temporal_property(prop)
        if ts is not None:
            if agg:
                return self._aggregate_ts(ts, agg)
            else:
                # Default to mean
                return self._aggregate_ts(ts, 'mean')
        
        return None
    
    def _aggregate_ts(self, ts, agg: str) -> Optional[float]:
        """Compute aggregation on time series."""
        try:
            if agg == 'mean':
                return ts.mean()
            elif agg == 'min':
                return ts.min()
            elif agg == 'max':
                return ts.max()
            elif agg == 'std':
                return ts.std()
            elif agg == 'sum':
                return ts.sum()
            elif agg == 'count':
                return ts.length
            elif agg == 'range':
                return ts.max() - ts.min()
            elif agg == 'first':
                return ts.data[0][0] if ts.data else None
            elif agg == 'last':
                return ts.data[-1][0] if ts.data else None
        except Exception:
            pass
        return None
    
    def _compare(self, left: Any, op: str, right: Any) -> bool:
        """Compare two values with operator."""
        if left is None:
            return False
        try:
            if op in ('=', '=='):
                return left == right
            elif op in ('!=', '<>'):
                return left != right
            elif op == '<':
                return left < right
            elif op == '<=':
                return left <= right
            elif op == '>':
                return left > right
            elif op == '>=':
                return left >= right
            elif op == 'contains':
                return right in str(left)
            elif op == 'starts_with':
                return str(left).startswith(str(right))
        except Exception:
            return False
        return False
    
    def _check_ts_similarity(self, ts, similarity_config: dict) -> bool:
        """Check time series similarity."""
        try:
            values = [d[0] for d in ts.data]
            if len(values) < 2:
                return False
            
            ref_ts = similarity_config.get('reference_ts')
            features = similarity_config.get('features')
            template = similarity_config.get('template')
            tol = similarity_config.get('tolerance', {'mean': 0.1, 'std': 0.1})
            
            if ref_ts is not None:
                ref_values = [d[0] for d in ref_ts.data]
                if not ref_values:
                    return False
                
                ts_mean = sum(values) / len(values)
                ref_mean = sum(ref_values) / len(ref_values)
                ts_std = (sum((x - ts_mean) ** 2 for x in values) / len(values)) ** 0.5
                ref_std = (sum((x - ref_mean) ** 2 for x in ref_values) / len(ref_values)) ** 0.5
                
                mean_tol = tol.get('mean', 0.1)
                std_tol = tol.get('std', 0.1)
                
                if ref_mean != 0:
                    if abs(ts_mean - ref_mean) / abs(ref_mean) > mean_tol:
                        return False
                if ref_std != 0:
                    if abs(ts_std - ref_std) / abs(ref_std) > std_tol:
                        return False
                return True
            
            elif features is not None:
                ts_mean = sum(values) / len(values)
                ts_std = (sum((x - ts_mean) ** 2 for x in values) / len(values)) ** 0.5
                
                for feat, expected in features.items():
                    if feat == 'mean':
                        actual = ts_mean
                    elif feat == 'std':
                        actual = ts_std
                    elif feat == 'min':
                        actual = min(values)
                    elif feat == 'max':
                        actual = max(values)
                    else:
                        continue
                    
                    feat_tol = tol.get(feat, 0.1)
                    if expected != 0 and abs(actual - expected) / abs(expected) > feat_tol:
                        return False
                return True
            
            elif template is not None:
                if template == 'increasing':
                    return values[-1] > values[0]
                elif template == 'decreasing':
                    return values[-1] < values[0]
                elif template == 'stable':
                    mean = sum(values) / len(values)
                    std = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
                    return (std / (abs(mean) + 1e-9)) < 0.1
                elif template == 'spike':
                    mean = sum(values) / len(values)
                    std = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
                    return (max(values) - mean) > 2 * std
                elif template.startswith('drop_by_'):
                    amount = float(template.split('_')[-1])
                    return (max(values) - min(values)) >= amount
        except Exception:
            pass
        return False
    
    # -------------------------------------------------------------------------
    # Output Helpers
    # -------------------------------------------------------------------------
    
    def count(self) -> int:
        """Return count of matches."""
        return len(self.execute())
    
    def as_subhygraph(self, name: str):
        """
        Execute and return result as SubHyGraph.
        
        Collects all matched nodes and creates a subgraph.
        """
        matches = self.execute()
        
        node_ids = set()
        for match in matches:
            for var, entity in match.items():
                if var in self._nodes and hasattr(entity, 'oid'):
                    node_ids.add(entity.oid)
        
        return self.hygraph.subHygraph(
            node_ids=node_ids,
            name=name,
            filter_query=self._describe_pattern()
        )
    
    def _describe_pattern(self) -> str:
        """Generate human-readable pattern description."""
        parts = []
        
        for var, node in self._nodes.items():
            desc = f"({var}"
            if node.label:
                desc += f":{node.label}"
            desc += ")"
            parts.append(desc)
        
        for edge in self._edges:
            desc = f"-[{edge.variable}"
            if edge.label:
                desc += f":{edge.label}"
            desc += "]->"
            # Insert between source and target
            parts.insert(len(self._nodes) - 1, desc)
        
        return "".join(parts)

def ts_aggregate(ts_or_measurements: Any, agg: str) -> Optional[float]:
    """
    Compute an aggregation on either Timeseries object or raw measurements.
    """
    agg = (agg or "").lower()

    # TimeSeries object
    if isinstance(ts_or_measurements, TimeSeries):
        ts = ts_or_measurements
        try:
            if agg == "mean":
                return float(ts.mean())
            if agg == "min":
                return float(ts.min())
            if agg == "max":
                return float(ts.max())
            if agg == "std":
                return float(ts.std())
            if agg == "sum":
                return float(ts.sum())
            if agg == "count":
                return float(len(ts))
            if agg == "range":
                if hasattr(ts, "get_range"):
                    return float(ts.get_range())
                return float(ts.max() - ts.min())
            if agg == "median":
                return float(ts.median())
            if agg == "first":
                if hasattr(ts, "get_first_value"):
                    v = ts.get_first_value()
                    return None if v is None else float(v)
                return float(ts.data[0][0]) if getattr(ts, "data", None) else None
            if agg == "last":
                if hasattr(ts, "get_last_value"):
                    v = ts.get_last_value()
                    return None if v is None else float(v)
                return float(ts.data[-1][0]) if getattr(ts, "data", None) else None
            if agg == "change":
                v = ts.get_change() if hasattr(ts, "get_change") else None
                return None if v is None else float(v)
            if agg == "change_percent":
                v = ts.get_change_percent() if hasattr(ts, "get_change_percent") else None
                return None if v is None else float(v)
            if agg == "cv":
                v = ts.get_coefficient_of_variation() if hasattr(ts, "get_coefficient_of_variation") else None
                return None if v is None else float(v)

            return None
        except Exception:
            return None

    # raw measurements list
    measurements = ts_or_measurements
    if not measurements:
        return None
    try:
        values = [float(v) for _, v in measurements]
    except Exception:
        return None

    if agg == "mean":
        return sum(values) / len(values)
    if agg == "min":
        return min(values)
    if agg == "max":
        return max(values)
    if agg == "sum":
        return sum(values)
    if agg == "count":
        return float(len(values))
    if agg == "range":
        return max(values) - min(values)
    if agg == "std":
        mean = sum(values) / len(values)
        return math.sqrt(sum((x - mean) ** 2 for x in values) / len(values))
    if agg == "median":
        s = sorted(values)
        n = len(s)
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2
    if agg == "first":
        return values[0]
    if agg == "last":
        return values[-1]

    return None
