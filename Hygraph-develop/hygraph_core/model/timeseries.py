"""
HyGraph Core Model - Time Series

Fully typed TimeSeries class with metadata and operations.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Any, Dict, Tuple, Union
import numpy as np
from enum import Enum


# =============================================================================
# Enums
# =============================================================================

class TimeSeriesType(Enum):
    """Type of time series"""
    UNIVARIATE = "univariate"  # Single variable
    MULTIVARIATE = "multivariate"  # Multiple variables


# =============================================================================
# Pattern Matches Result
# =============================================================================

@dataclass
class PatternMatch:
    """
    Single pattern match result.
    
    Attributes:
        start_idx: Start index in the original time series
        end_idx: End index in the original time series
        start_time: Start timestamp
        end_time: End timestamp
        distance: Distance/dissimilarity score (lower = better match)
        similarity: Similarity score 0-100% (higher = better match)
        subsequence: The matched TimeSeries segment
        features: Optional dict of computed features {mean, std, min, max, range}
    """
    start_idx: int
    end_idx: int
    start_time: datetime
    end_time: datetime
    distance: float
    similarity: float  # 0-100%, computed from distance
    subsequence: 'TimeSeries'
    features: Optional[Dict[str, float]] = None
    
    def __str__(self) -> str:
        return (
            f"Match[{self.start_idx}:{self.end_idx}] "
            f"{self.start_time.strftime('%Y-%m-%d %H:%M')} → "
            f"{self.end_time.strftime('%Y-%m-%d %H:%M')} "
            f"(similarity: {self.similarity:.1f}%, distance: {self.distance:.3f})"
        )


@dataclass
class PatternMatches:
    """
    Collection of pattern match results with nice display.
    
    Provides:
    - Pretty table display with print()
    - Iteration over matches
    - Frequency statistics
    - Access by index
    
    Example:
        matches = ts.find_pattern(template='spike')
        print(matches)  # Pretty table
        
        for m in matches:
            print(m.start_time, m.similarity)
        
        print(f"Found {matches.frequency} occurrences")
        print(f"That's {matches.frequency_per_day:.2f} per day")
    """
    matches: List[PatternMatch]
    pattern_type: str  # 'template', 'features', 'timeseries'
    pattern_info: str  # Description of what was searched
    total_length: int  # Length of searched time series
    time_span_days: float  # Time span in days
    time_window: Optional[Tuple[datetime, datetime]] = None
    
    @property
    def frequency(self) -> int:
        """Number of pattern occurrences found."""
        return len(self.matches)
    
    @property
    def frequency_per_day(self) -> float:
        """Average occurrences per day."""
        if self.time_span_days > 0:
            return self.frequency / self.time_span_days
        return 0.0
    
    def __len__(self) -> int:
        return len(self.matches)
    
    def __iter__(self):
        return iter(self.matches)
    
    def __getitem__(self, idx) -> PatternMatch:
        return self.matches[idx]
    
    def __bool__(self) -> bool:
        return len(self.matches) > 0
    
    def __str__(self) -> str:
        lines = []
        lines.append("═" * 70)
        lines.append(f"Pattern Search Results: {self.pattern_info}")
        lines.append("═" * 70)
        lines.append(f"Found: {self.frequency} matches")
        lines.append(f"Frequency: {self.frequency_per_day:.2f} per day")
        lines.append(f"Searched: {self.total_length} points over {self.time_span_days:.1f} days")
        if self.time_window:
            lines.append(f"Time window: {self.time_window[0]} → {self.time_window[1]}")
        lines.append("─" * 70)
        
        if not self.matches:
            lines.append("No matches found.")
        else:
            # Header
            lines.append(f"{'#':<3} {'Start Time':<20} {'End Time':<20} {'Similarity':<12} {'Distance':<10}")
            lines.append("─" * 70)
            
            for i, m in enumerate(self.matches[:20]):  # Show max 20
                lines.append(
                    f"{i+1:<3} "
                    f"{m.start_time.strftime('%Y-%m-%d %H:%M'):<20} "
                    f"{m.end_time.strftime('%Y-%m-%d %H:%M'):<20} "
                    f"{m.similarity:>6.1f}%     "
                    f"{m.distance:<10.3f}"
                )
            
            if len(self.matches) > 20:
                lines.append(f"... and {len(self.matches) - 20} more matches")
        
        lines.append("═" * 70)
        lines.append("")
        lines.append("Distance: Lower = better match (0 = perfect match)")
        lines.append("Similarity: Higher = better match (100% = perfect match)")
        
        return "\n".join(lines)
    
    def __repr__(self) -> str:
        return f"PatternMatches({self.frequency} matches, {self.frequency_per_day:.2f}/day)"
    
    def to_list(self) -> List[Dict[str, Any]]:
        """Convert to list of dicts (for JSON serialization)."""
        return [
            {
                'start_idx': m.start_idx,
                'end_idx': m.end_idx,
                'start_time': m.start_time.isoformat(),
                'end_time': m.end_time.isoformat(),
                'distance': m.distance,
                'similarity': m.similarity,
                'features': m.features
            }
            for m in self.matches
        ]
    
    def best(self) -> Optional[PatternMatch]:
        """Get the best match (highest similarity)."""
        return self.matches[0] if self.matches else None
    
    def worst(self) -> Optional[PatternMatch]:
        """Get the worst match among found (lowest similarity)."""
        return self.matches[-1] if self.matches else None


# =============================================================================
# Pattern Builder (Fluent API)
# =============================================================================

class Pattern:
    """
    Fluent API for creating pattern templates.
    
    IMPORTANT: Pattern timestamps are IGNORED during matching!
    Only the SHAPE (values) matters. The algorithm uses z-normalization,
    so it finds similar shapes regardless of:
    - Absolute values (amplitude is normalized)
    - Time intervals (hourly data matches daily data if shape is similar)
    
    Example:
        # Create patterns with fluent API
        spike = Pattern.spike(length=10, amplitude=1.0)
        drop = Pattern.drop(length=15)
        trend = Pattern.increasing(length=20)
        
        # Use in search
        matches = ts.find_pattern(pattern=spike)
    """
    
    @staticmethod
    def spike(length: int = 10, amplitude: float = 1.0, position: float = 0.5) -> 'TimeSeries':
        """
        Create a spike/peak pattern (Gaussian bump).
        
        Shape: ___/\\___
        
        Args:
            length: Number of points
            amplitude: Height of spike (normalized anyway)
            position: Position of peak (0-1, default 0.5 = center)
        """
        return TimeSeries.create_pattern('spike', length=length, amplitude=amplitude, position=position)
    
    @staticmethod
    def drop(length: int = 10, amplitude: float = 1.0, position: float = 0.5) -> 'TimeSeries':
        """
        Create a drop/dip pattern (inverted Gaussian).
        
        Shape: ___\\/___
        
        Args:
            length: Number of points
            amplitude: Depth of drop
            position: Position of bottom (0-1)
        """
        return TimeSeries.create_pattern('drop', length=length, amplitude=amplitude, position=position)
    
    @staticmethod
    def increasing(length: int = 10, amplitude: float = 1.0) -> 'TimeSeries':
        """
        Create an increasing/upward trend pattern.
        
        Shape: __/
        """
        return TimeSeries.create_pattern('increasing', length=length, amplitude=amplitude)
    
    @staticmethod
    def decreasing(length: int = 10, amplitude: float = 1.0) -> 'TimeSeries':
        """
        Create a decreasing/downward trend pattern.
        
        Shape: \\__
        """
        return TimeSeries.create_pattern('decreasing', length=length, amplitude=amplitude)
    
    @staticmethod
    def peak(length: int = 10, amplitude: float = 1.0) -> 'TimeSeries':
        """
        Create a peak pattern (inverted parabola).
        
        Shape: /\\
        """
        return TimeSeries.create_pattern('peak', length=length, amplitude=amplitude)
    
    @staticmethod
    def valley(length: int = 10, amplitude: float = 1.0) -> 'TimeSeries':
        """
        Create a valley pattern (parabola).
        
        Shape: \\/
        """
        return TimeSeries.create_pattern('valley', length=length, amplitude=amplitude)
    
    @staticmethod
    def step_up(length: int = 10, amplitude: float = 1.0, position: float = 0.5) -> 'TimeSeries':
        """
        Create a step-up pattern.
        
        Shape: __|^^
        """
        return TimeSeries.create_pattern('step_up', length=length, amplitude=amplitude, position=position)
    
    @staticmethod
    def step_down(length: int = 10, amplitude: float = 1.0, position: float = 0.5) -> 'TimeSeries':
        """
        Create a step-down pattern.
        
        Shape: ^^|__
        """
        return TimeSeries.create_pattern('step_down', length=length, amplitude=amplitude, position=position)
    
    @staticmethod
    def oscillation(length: int = 20, amplitude: float = 1.0, periods: int = 2) -> 'TimeSeries':
        """
        Create an oscillation/wave pattern (sine wave).
        
        Shape: /\\/\\/
        """
        return TimeSeries.create_pattern('oscillation', length=length, amplitude=amplitude, periods=periods)
    
    @staticmethod
    def plateau(length: int = 10, amplitude: float = 1.0, position: float = 0.5) -> 'TimeSeries':
        """
        Create a plateau pattern (flat top).
        
        Shape: _/^^^\\_
        """
        return TimeSeries.create_pattern('plateau', length=length, amplitude=amplitude, position=position)
    
    @staticmethod
    def custom(values: List[float], name: str = 'custom') -> 'TimeSeries':
        """
        Create a custom pattern from values.
        
        Args:
            values: List of numeric values defining the pattern shape
            name: Name for the pattern
        
        Example:
            my_pattern = Pattern.custom([0, 1, 2, 3, 2, 1, 0], name='my_wave')
        """

        base_time = datetime(2024, 1, 1)
        timestamps = [base_time + timedelta(hours=i) for i in range(len(values))]
        
        return TimeSeries(
            tsid=f"pattern_{name}",
            timestamps=timestamps,
            variables=['value'],
            data=[[float(v)] for v in values],
            metadata=TimeSeriesMetadata(
                owner_id='pattern',
                element_type='pattern',
                description=f"Custom pattern: {name}"
            )
        )


# =============================================================================
# Time Series Metadata
# =============================================================================

@dataclass
class TimeSeriesMetadata:
    """
    Metadata for a time series.

    Tracks ownership, element type, and additional information.
    """

    owner_id: str  # Entity that owns this TS
    element_type: str  # 'node' or 'edges'
    description: Optional[str] = None  # Human-readable description
    units: Optional[str] = None  # Measurement units
    source: Optional[str] = None  # Data source
    tags: List[str] = field(default_factory=list)  # Tags for categorization
    custom: Dict[str, Any] = field(default_factory=dict)  # Custom metadata

    def add_tag(self, tag: str) -> None:
        """Add a tag"""
        if tag not in self.tags:
            self.tags.append(tag)

    def has_tag(self, tag: str) -> bool:
        """Check if tag exists"""
        return tag in self.tags


# =============================================================================
# Time Series
# =============================================================================

@dataclass
class TimeSeries:
    """
    Time series data structure.

    Stores:
    - tsid: Unique identifier
    - timestamps: List of datetime objects
    - variables: List of variable names
    - data: 2D list (timestamps × variables)
    - metadata: TimeSeriesMetadata

    Example:
        ts = TimeSeries(
            tsid="ts_001",
            timestamps=[datetime(2024,1,1), datetime(2024,1,2)],
            variables=["temperature"],
            data=[[20.5], [21.0]],
            metadata=TimeSeriesMetadata(owner_id="sensor_1", element_type="node")
        )
    """

    tsid: str
    timestamps: List[datetime]
    variables: List[str]
    data: List[List[float]]  # Shape: [n_timestamps, n_variables]
    metadata: TimeSeriesMetadata

    def __post_init__(self):
        """Validate time series data"""
        self._validate()

    def _validate(self) -> None:
        """Validate time series structure"""
        if not self.tsid:
            raise ValueError("tsid cannot be empty")

        if not self.timestamps:
            raise ValueError("timestamps cannot be empty")

        if not self.variables:
            raise ValueError("variables cannot be empty")

        if len(self.data) != len(self.timestamps):
            raise ValueError(
                f"data length ({len(self.data)}) must match "
                f"timestamps length ({len(self.timestamps)})"
            )

        # Check each row has correct number of variables
        for i, row in enumerate(self.data):
            if len(row) != len(self.variables):
                raise ValueError(
                    f"Row {i} has {len(row)} values, "
                    f"expected {len(self.variables)}"
                )

    # -------------------------------------------------------------------------
    # Basic Properties
    # -------------------------------------------------------------------------

    @property
    def length(self) -> int:
        """Number of time points"""
        return len(self.timestamps)

    @property
    def n_variables(self) -> int:
        """Number of variables"""
        return len(self.variables)

    @property
    def is_univariate(self) -> bool:
        """Check if univariate"""
        return len(self.variables) == 1

    @property
    def is_multivariate(self) -> bool:
        """Check if multivariate"""
        return len(self.variables) > 1

    @property
    def type(self) -> TimeSeriesType:
        """Get time series type"""
        return (TimeSeriesType.UNIVARIATE if self.is_univariate
                else TimeSeriesType.MULTIVARIATE)

    # -------------------------------------------------------------------------
    # Temporal Methods
    # -------------------------------------------------------------------------

    def first_timestamp(self) -> datetime:
        """Get first timestamp"""
        return self.timestamps[0]

    def last_timestamp(self) -> datetime:
        """Get last timestamp"""
        return self.timestamps[-1]

    def time_range(self) -> Tuple[datetime, datetime]:
        """Get (start, end) time range"""
        return (self.first_timestamp(), self.last_timestamp())

    def get_value_at(self, timestamp: datetime, variable: Optional[str] = None) -> Optional[float]:
        """
        Get value at specific timestamp.

        Args:
            timestamp: Time to query
            variable: Variable name (if multivariate)

        Returns:
            Value at timestamp, or None if not found
        """
        try:
            idx = self.timestamps.index(timestamp)
            if variable:
                var_idx = self.variables.index(variable)
                return self.data[idx][var_idx]
            else:
                return self.data[idx][0]  # Return first variable
        except (ValueError, IndexError):
            return None

    def get_first_value(self, variable: Optional[str] = None) -> float:
        """
        Get first value.

        Args:
            variable: Variable name (if multivariate)

        Returns:
            First value
        
        Example:
            first_bikes = ts.get_first_value()
            # Or in query:
            .where(lambda n: n.get_temporal_property('available_bike').get_first_value() > 10)
        """
        if variable:
            var_idx = self.variables.index(variable)
            return self.data[0][var_idx]
        else:
            return self.data[0][0]

    def get_last_value(self, variable: Optional[str] = None) -> float:
        """
        Get last value.

        Args:
            variable: Variable name (if multivariate)

        Returns:
            Last value
        
        Example:
            last_bikes = ts.get_last_value()
            # Or in query:
            .where(lambda n: n.get_temporal_property('available_bike').get_last_value() < 5)
        """
        if variable:
            var_idx = self.variables.index(variable)
            return self.data[-1][var_idx]
        else:
            return self.data[-1][0]

    def get_value_at_nearest(self, timestamp: datetime, variable: Optional[str] = None) -> Optional[float]:
        """
        Get value at nearest timestamp (finds closest match if exact not found).

        Args:
            timestamp: Time to query
            variable: Variable name (if multivariate)

        Returns:
            Value at nearest timestamp, or None if empty
        
        Example:
            # Get value closest to a specific time
            val = ts.get_value_at_nearest(datetime(2024, 6, 6, 12, 0, 0))
            
            # In query - find stations where value at noon was low
            .where(lambda n: n.get_temporal_property('available_bike')
                              .get_value_at_nearest(datetime(2024, 6, 6, 12, 0)) < 5)
        """
        if not self.timestamps:
            return None
        
        # Find nearest timestamp
        min_diff = None
        nearest_idx = 0
        
        for i, ts in enumerate(self.timestamps):
            diff = abs((ts - timestamp).total_seconds())
            if min_diff is None or diff < min_diff:
                min_diff = diff
                nearest_idx = i
        
        if variable:
            var_idx = self.variables.index(variable)
            return self.data[nearest_idx][var_idx]
        else:
            return self.data[nearest_idx][0]

    def get_value_at_index(self, index: int, variable: Optional[str] = None) -> Optional[float]:
        """
        Get value at specific index position.

        Args:
            index: Index position (supports negative indexing: -1 = last, -2 = second last)
            variable: Variable name (if multivariate)

        Returns:
            Value at index, or None if out of bounds
        
        Example:
            # Get 5th value
            val = ts.get_value_at_index(4)
            
            # Get second-to-last value
            val = ts.get_value_at_index(-2)
            
            # In query
            .where(lambda n: n.get_temporal_property('available_bike').get_value_at_index(-1) < 5)
        """
        try:
            if variable:
                var_idx = self.variables.index(variable)
                return self.data[index][var_idx]
            else:
                return self.data[index][0]
        except (IndexError, ValueError):
            return None

    def get_change(self, variable: Optional[str] = None) -> float:
        """
        Get change from first to last value (last - first).

        Args:
            variable: Variable name (if multivariate)

        Returns:
            Change value (positive = increased, negative = decreased)
        
        Example:
            # Check if bikes increased over time
            change = ts.get_change()
            
            # In query - find stations where bikes decreased
            .where_ts('available_bike', lambda ts: ts.get_change() < 0)
        """
        return self.get_last_value(variable) - self.get_first_value(variable)

    def get_change_percent(self, variable: Optional[str] = None) -> float:
        """
        Get percentage change from first to last value.

        Args:
            variable: Variable name (if multivariate)

        Returns:
            Percentage change (e.g., 0.25 = 25% increase, -0.5 = 50% decrease)
        
        Example:
            # Check if bikes dropped by more than 50%
            .where_ts('available_bike', lambda ts: ts.get_change_percent() < -0.5)
        """
        first = self.get_first_value(variable)
        if first == 0:
            return float('inf') if self.get_last_value(variable) > 0 else 0.0
        return (self.get_last_value(variable) - first) / abs(first)

    def get_range(self, variable: Optional[str] = None) -> float:
        """
        Get range (max - min).

        Args:
            variable: Variable name (if multivariate)

        Returns:
            Range value
        
        Example:
            # Find stations with high variability
            .where_ts('available_bike', lambda ts: ts.get_range() > 20)
        """
        return self.max(variable) - self.min(variable)

    def get_coefficient_of_variation(self, variable: Optional[str] = None) -> float:
        """
        Get coefficient of variation (std / mean).
        
        Useful for comparing variability across time series with different scales.

        Args:
            variable: Variable name (if multivariate)

        Returns:
            Coefficient of variation (0 = no variation, higher = more variation)
        
        Example:
            # Find stable stations (low CV)
            .where_ts('available_bike', lambda ts: ts.get_coefficient_of_variation() < 0.1)
        """
        mean_val = self.mean(variable)
        if mean_val == 0:
            return 0.0
        return self.std(variable) / abs(mean_val)

    def count_above(self, threshold: float, variable: Optional[str] = None) -> int:
        """
        Count values above a threshold.

        Args:
            threshold: Value threshold
            variable: Variable name (if multivariate)

        Returns:
            Count of values above threshold
        
        Example:
            # Find stations that were above 50 bikes more than 100 times
            .where_ts('available_bike', lambda ts: ts.count_above(50) > 100)
        """
        arr = self.to_numpy(variable).flatten()
        return int(np.sum(arr > threshold))

    def count_below(self, threshold: float, variable: Optional[str] = None) -> int:
        """
        Count values below a threshold.

        Args:
            threshold: Value threshold
            variable: Variable name (if multivariate)

        Returns:
            Count of values below threshold
        
        Example:
            # Find stations that were often empty
            .where_ts('available_bike', lambda ts: ts.count_below(5) > 50)
        """
        arr = self.to_numpy(variable).flatten()
        return int(np.sum(arr < threshold))

    def percent_above(self, threshold: float, variable: Optional[str] = None) -> float:
        """
        Get percentage of values above threshold.

        Args:
            threshold: Value threshold
            variable: Variable name (if multivariate)

        Returns:
            Percentage (0.0 to 1.0)
        
        Example:
            # Find stations above 50 bikes more than 80% of the time
            .where_ts('available_bike', lambda ts: ts.percent_above(50) > 0.8)
        """
        return self.count_above(threshold, variable) / self.length

    def percent_below(self, threshold: float, variable: Optional[str] = None) -> float:
        """
        Get percentage of values below threshold.

        Args:
            threshold: Value threshold
            variable: Variable name (if multivariate)

        Returns:
            Percentage (0.0 to 1.0)
        
        Example:
            # Find stations below 5 bikes more than 10% of the time
            .where_ts('available_bike', lambda ts: ts.percent_below(5) > 0.1)
        """
        return self.count_below(threshold, variable) / self.length

    def is_increasing(self, variable: Optional[str] = None) -> bool:
        """
        Check if time series has overall increasing trend (last > first).
        
        Example:
            .where_ts('available_bike', lambda ts: ts.is_increasing())
        """
        return self.get_last_value(variable) > self.get_first_value(variable)

    def is_decreasing(self, variable: Optional[str] = None) -> bool:
        """
        Check if time series has overall decreasing trend (last < first).
        
        Example:
            .where_ts('available_bike', lambda ts: ts.is_decreasing())
        """
        return self.get_last_value(variable) < self.get_first_value(variable)

    def is_stable(self, cv_threshold: float = 0.1, variable: Optional[str] = None) -> bool:
        """
        Check if time series is stable (low coefficient of variation).

        Args:
            cv_threshold: Max coefficient of variation for "stable" (default 0.1 = 10%)
            variable: Variable name (if multivariate)
        
        Example:
            .where_ts('available_bike', lambda ts: ts.is_stable(0.05))
        """
        return self.get_coefficient_of_variation(variable) < cv_threshold

    def slice_by_time(self, start: datetime, end: datetime) -> 'TimeSeries':
        """
        Slice time series by time range.

        Args:
            start: Start time (inclusive)
            end: End time (inclusive)

        Returns:
            New TimeSeries with filtered data
        """
        indices = [
            i for i, ts in enumerate(self.timestamps)
            if start <= ts <= end
        ]

        if not indices:
            raise ValueError(f"No data in range [{start}, {end}]")

        return TimeSeries(
            tsid=f"{self.tsid}_slice",
            timestamps=[self.timestamps[i] for i in indices],
            variables=self.variables.copy(),
            data=[self.data[i].copy() for i in indices],
            metadata=self.metadata
        )

    # -------------------------------------------------------------------------
    # Statistical Methods
    # -------------------------------------------------------------------------

    def to_numpy(self, variable: Optional[str] = None) -> np.ndarray:
        """
        Convert to numpy array.

        Args:
            variable: Specific variable, or None for all

        Returns:
            Numpy array of values
        """
        if variable:
            var_idx = self.variables.index(variable)
            return np.array([row[var_idx] for row in self.data])
        else:
            return np.array(self.data)

    def mean(self, variable: Optional[str] = None) -> float:
        """Calculate mean value"""
        arr = self.to_numpy(variable)
        return float(np.mean(arr))

    def std(self, variable: Optional[str] = None) -> float:
        """Calculate standard deviation"""
        arr = self.to_numpy(variable)
        return float(np.std(arr))

    def min(self, variable: Optional[str] = None) -> float:
        """Calculate minimum value"""
        arr = self.to_numpy(variable)
        return float(np.min(arr))

    def max(self, variable: Optional[str] = None) -> float:
        """Calculate maximum value"""
        arr = self.to_numpy(variable)
        return float(np.max(arr))

    def sum(self, variable: Optional[str] = None) -> float:
        """Calculate sum"""
        arr = self.to_numpy(variable)
        return float(np.sum(arr))

    def median(self, variable: Optional[str] = None) -> float:
        """Calculate median"""
        arr = self.to_numpy(variable)
        return float(np.median(arr))

    def percentile(self, q: float, variable: Optional[str] = None) -> float:
        """
        Calculate percentile.

        Args:
            q: Percentile (0-100)
            variable: Variable name

        Returns:
            Percentile value
        """
        arr = self.to_numpy(variable)
        return float(np.percentile(arr, q))

    # -------------------------------------------------------------------------
    # Comparison Methods
    # -------------------------------------------------------------------------

    def correlation(self, other: 'TimeSeries',
                    var1: Optional[str] = None,
                    var2: Optional[str] = None) -> float:
        """
        Calculate correlation with another time series.

        Args:
            other: Other time series
            var1: Variable in this TS
            var2: Variable in other TS

        Returns:
            Pearson correlation coefficient
        """
        arr1 = self.to_numpy(var1)
        arr2 = other.to_numpy(var2)

        # Must have same length
        min_len = min(len(arr1), len(arr2))
        arr1 = arr1[:min_len]
        arr2 = arr2[:min_len]

        return float(np.corrcoef(arr1, arr2)[0, 1])

    # -------------------------------------------------------------------------
    # Pattern Search Methods (Using External Libraries)
    # -------------------------------------------------------------------------

    def contains_pattern(
        self,
        pattern: Optional['TimeSeries'] = None,
        template: Optional[str] = None,
        method: str = 'stumpy',
        threshold: Optional[float] = None,
        normalize: bool = True,
        variable: Optional[str] = None,
        time_window: Optional[Tuple[datetime, datetime]] = None
    ) -> bool:
        """
        Check if this time series contains a given pattern as a subsequence.
        
        Uses STUMPY (Matrix Profile) for efficient pattern matching.
        Pattern can be specified as a TimeSeries object, template name, or features dict.
        
        Args:
            pattern: Short TimeSeries pattern to search for
            template: Pattern template name ('spike', 'drop', 'increasing', etc.)
            method: Library to use ('stumpy', 'mass', 'euclidean')
            threshold: Max distance threshold (auto-computed if None)
            normalize: Whether to z-normalize before comparison
            variable: Variable to search (for multivariate)
            time_window: Optional (start, end) datetime tuple to limit search range
        
        Returns:
            True if pattern is found within this time series
        
        Requires:
            pip install stumpy
        
        Example:
            # Using TimeSeries pattern
            if ts.contains_pattern(pattern=spike_pattern):
                print("Spike detected!")
            
            # Using template
            if ts.contains_pattern(template='spike'):
                print("Spike detected!")
            
            # Using features
            if ts.contains_pattern(features={'mean': 50, 'std': 5}):
                print("Found matching segment!")
            
            # With time window
            if ts.contains_pattern(template='drop', time_window=(start, end)):
                print("Drop detected in specified window!")
        """
        result = self.find_pattern(
            pattern=pattern,
            template=template,
            method=method,
            threshold=threshold,
            normalize=normalize,
            variable=variable,
            top_k=1,
            time_window=time_window
        )
        return bool(result)  # PatternMatches has __bool__ method

    def find_pattern(
        self,
        pattern: Optional['TimeSeries'] = None,
        template: Optional[str] = None,
        method: str = 'stumpy',
        threshold: Optional[float] = None,
        normalize: bool = True,
        variable: Optional[str] = None,
        top_k: int = 5,
        time_window: Optional[Tuple[datetime, datetime]] = None,
        match_mode: str = 'topk',
        exclusion_zone: Optional[int] = None
    ) -> 'PatternMatches':
        """
        Find occurrences of a pattern within this time series.
        
        Users can provide the pattern in THREE ways:
        1. **pattern**: A TimeSeries object to search for
        2. **template**: A template name ('spike', 'drop', 'increasing', etc.)
        3. **features**: A feature dict to match {mean: X, std: Y, range: Z}
        
        IMPORTANT: Pattern timestamps are IGNORED during matching!
        Only the SHAPE matters. Z-normalization makes matching scale-invariant,
        so a pattern from hourly data will match similar shapes in daily data.
        
        DISTANCE EXPLAINED:
        - Distance = Euclidean distance between z-normalized subsequences
        - Range: 0 to ~4 (for z-normalized data)
        - 0 = Perfect match (identical shape)
        - <1 = Very good match
        - 1-2 = Good match
        - 2-3 = Moderate match
        - >3 = Poor match
        - Similarity% = 100 * (1 - distance/4), clamped to [0, 100]
        
        Args:
            pattern: Short TimeSeries pattern to search for
            template: Pattern template name ('spike', 'drop', 'increasing', 
                      'decreasing', 'peak', 'valley', 'step_up', 'step_down', 'oscillation')
            method: 'stumpy' (recommended), 'mass', or 'euclidean' (fallback)
            threshold: Max distance for a match. Only used when match_mode='threshold'.
            normalize: Z-normalize subsequences before comparison (recommended True)
            variable: Variable to search (for multivariate)
            top_k: Return top-k best matches (default: 5)
            time_window: Optional (start, end) datetime tuple to limit search range
            match_mode: 'topk' (return best k matches) or 'threshold' (return all below threshold)
            exclusion_zone: Minimum separation between matches (default: pattern_length // 2)
        
        Returns:
            PatternMatches object with nice display. Use print(matches) to see results.
        
        Requires:
            pip install stumpy
        
        Example:
            # Method 1: Use a template name (simplest)
            matches = ts.find_pattern(template='spike')
            print(matches)  # Pretty table with all info
            
            # Method 2: Use Pattern fluent API
            spike = Pattern.spike(length=15)
            matches = ts.find_pattern(pattern=spike)
            
            # Method 3: Use features dict
            matches = ts.find_pattern(features={'mean': 15, 'std': 3})
            
            # With time window
            matches = ts.find_pattern(
                template='drop',
                time_window=(datetime(2024,5,1), datetime(2024,5,15))
            )
            
            # Get best match
            best = matches.best()
            print(f"Best match at {best.start_time} with {best.similarity:.1f}% similarity")
            
            # Iterate over matches
            for m in matches:
                print(m)  # Each match has nice __str__
        """
        # Apply time window if specified
        search_ts = self
        if time_window:
            start_time, end_time = time_window
            try:
                search_ts = self.slice_by_time(start_time, end_time)
            except ValueError:
                # No data in range - return empty PatternMatches
                return PatternMatches(
                    matches=[],
                    pattern_type='empty',
                    pattern_info='No data in time window',
                    total_length=0,
                    time_span_days=0.0,
                    time_window=time_window
                )
        
        # Determine pattern to search for and build pattern_info string
        if pattern is None and template is None :
            raise ValueError("Must provide one of: pattern (TimeSeries), template (str), or features (dict)")
        
        # Build pattern info for display
        if template is not None:
            pattern = TimeSeries.create_pattern(template, length=10)
            pattern_type = 'template'
            pattern_info = f"Template: '{template}'"
        else:
            pattern_type = 'timeseries'
            pattern_info = f"TimeSeries pattern (length={pattern.length})"
        
        # Calculate time span in days
        if search_ts.timestamps and len(search_ts.timestamps) >= 2:
            time_span = (search_ts.timestamps[-1] - search_ts.timestamps[0]).total_seconds()
            time_span_days = time_span / 86400
        else:
            time_span_days = 0.0

        # Pattern-based search using STUMPY
        raw_matches = search_ts._find_pattern_stumpy(
            pattern, method, threshold, normalize, variable, top_k,
            match_mode, exclusion_zone
        )

        # Convert raw matches to PatternMatch objects with similarity
        matches = []
        for m in raw_matches:
            distance = m['distance']
            # Similarity: 100% at distance=0, 0% at distance>=4
            # For z-normalized data, typical range is 0-4
            similarity = max(0.0, min(100.0, 100.0 * (1.0 - distance / 4.0)))
            
            matches.append(PatternMatch(
                start_idx=m['start_idx'],
                end_idx=m['end_idx'],
                start_time=m['start_time'],
                end_time=m['end_time'],
                distance=distance,
                similarity=similarity,
                subsequence=m['subsequence'],
                features=m.get('features')
            ))
        
        return PatternMatches(
            matches=matches,
            pattern_type=pattern_type,
            pattern_info=pattern_info,
            total_length=search_ts.length,
            time_span_days=time_span_days,
            time_window=time_window
        )
    
    def _find_pattern_stumpy(
        self,
        pattern: 'TimeSeries',
        method: str,
        threshold: Optional[float],
        normalize: bool,
        variable: Optional[str],
        top_k: int,
        match_mode: str = 'topk',
        exclusion_zone: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Internal method: Find pattern using STUMPY."""
        data = self.to_numpy(variable).flatten()
        pattern_data = pattern.to_numpy(variable).flatten()
        
        m = len(pattern_data)
        n = len(data)
        
        if m > n or m < 2:
            return []
        
        # Default exclusion zone: half the pattern length
        if exclusion_zone is None:
            exclusion_zone = m // 2
        
        # Try to use STUMPY (optimized)
        if method in ('stumpy', 'mass'):
            try:
                import stumpy
                # MASS: Mueen's Algorithm for Similarity Search
                # Returns distance profile - distance to pattern at each position
                distance_profile = stumpy.mass(pattern_data, data, normalize=normalize)
                
                # Find matches
                return self._extract_top_k_matches(
                    distance_profile, m, threshold, top_k, match_mode, exclusion_zone
                )
            except ImportError:
                # Fall back to basic implementation
                pass
        
        # Fallback: basic sliding window (slower)
        return self._find_pattern_basic(
            pattern_data, data, m, n, threshold, normalize, top_k, match_mode, exclusion_zone
        )
    
    def _find_by_features(
        self,
        features: Dict[str, float],
        top_k: int,
        variable: Optional[str] = None,
        window_size: int = 10,
        tolerance: float = 0.2,
        exclusion_zone: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Find subsequences matching statistical features.
        
        Args:
            features: Target features {mean, std, min, max, range}
            top_k: Number of matches to return
            variable: Variable to search
            window_size: Size of sliding window
            tolerance: Relative tolerance for feature matching (0.2 = 20%)
            exclusion_zone: Minimum separation between matches
        """
        data = self.to_numpy(variable).flatten()
        n = len(data)
        
        if window_size > n:
            return []
        
        # Default exclusion zone
        if exclusion_zone is None:
            exclusion_zone = window_size // 2
        
        matches = []
        
        for i in range(n - window_size + 1):
            window = data[i:i + window_size]
            
            # Compute window features
            w_mean = float(np.mean(window))
            w_std = float(np.std(window))
            w_min = float(np.min(window))
            w_max = float(np.max(window))
            w_range = w_max - w_min
            
            # Check if features match within tolerance
            score = 0.0
            match_count = 0
            
            for feat_name, expected in features.items():
                if feat_name == 'mean':
                    actual = w_mean
                elif feat_name == 'std':
                    actual = w_std
                elif feat_name == 'min':
                    actual = w_min
                elif feat_name == 'max':
                    actual = w_max
                elif feat_name == 'range':
                    actual = w_range
                else:
                    continue
                
                # Calculate relative difference
                if expected != 0:
                    diff = abs(actual - expected) / abs(expected)
                else:
                    diff = abs(actual)
                
                if diff <= tolerance:
                    match_count += 1
                score += diff
            
            # Only include if all features match
            if match_count == len(features):
                matches.append({
                    'start_idx': i,
                    'end_idx': i + window_size - 1,
                    'start_time': self.timestamps[i],
                    'end_time': self.timestamps[i + window_size - 1],
                    'distance': score,  # Lower is better
                    'features': {
                        'mean': w_mean,
                        'std': w_std,
                        'min': w_min,
                        'max': w_max,
                        'range': w_range
                    },
                    'subsequence': TimeSeries(
                        tsid=f"{self.tsid}_match_{i}",
                        timestamps=self.timestamps[i:i + window_size],
                        variables=self.variables.copy(),
                        data=[self.data[j].copy() for j in range(i, i + window_size)],
                        metadata=self.metadata
                    )
                })
        
        # Sort by score (lower is better) and return top_k
        matches.sort(key=lambda x: x['distance'])
        
        # Remove overlapping matches using exclusion zone
        non_overlapping = []
        used_intervals = []  # List of (start, end) tuples
        
        for m in matches:
            start_idx = m['start_idx']
            end_idx = m['end_idx']
            
            # Check if this overlaps with any used interval (within exclusion zone)
            overlaps = False
            for used_start, used_end in used_intervals:
                # Two intervals overlap if: start1 <= end2 + exclusion AND start2 <= end1 + exclusion
                if start_idx <= used_end + exclusion_zone and used_start <= end_idx + exclusion_zone:
                    overlaps = True
                    break
            
            if not overlaps:
                used_intervals.append((start_idx, end_idx))
                non_overlapping.append(m)
                if len(non_overlapping) >= top_k:
                    break
        
        return non_overlapping

    def _extract_top_k_matches(
        self,
        distance_profile: np.ndarray,
        pattern_length: int,
        threshold: Optional[float],
        top_k: int,
        match_mode: str = 'topk',
        exclusion_zone: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract top-k non-overlapping matches from distance profile.
        
        Args:
            distance_profile: Array of distances at each position
            pattern_length: Length of the pattern (m)
            threshold: Max distance threshold (only used if match_mode='threshold')
            top_k: Number of matches to return (only used if match_mode='topk')
            match_mode: 'topk' returns best k matches; 'threshold' returns all below threshold
            exclusion_zone: Minimum separation between match start indices
        
        DISTANCE VALUES (for z-normalized data):
        - 0 = Perfect match (identical shape)
        - <1 = Very good match
        - 1-2 = Good match  
        - 2-3 = Moderate match
        - >3 = Poor match
        - Max ~4 for z-normalized (since max euclidean dist between two z-norm sequences of length m is sqrt(2*m))
        """
        m = pattern_length
        
        # Default exclusion zone: half the pattern length (like Matrix Profile)
        if exclusion_zone is None:
            exclusion_zone = m // 2
        
        # Get indices sorted by distance (best first)
        sorted_indices = np.argsort(distance_profile)
        
        # Select non-overlapping matches
        results = []
        used_intervals = []  # List of (start, end) tuples
        
        for idx in sorted_indices:
            idx = int(idx)
            dist = float(distance_profile[idx])
            
            # In threshold mode, stop when we exceed threshold
            if match_mode == 'threshold' and threshold is not None:
                if dist > threshold:
                    break
            
            # Check overlap using exclusion zone
            # An interval [idx, idx+m-1] conflicts with [used_start, used_end]
            # if they are within exclusion_zone of each other
            overlaps = False
            for used_start, used_end in used_intervals:
                # Check if new match overlaps or is too close to existing match
                if idx <= used_end + exclusion_zone and used_start <= idx + m - 1 + exclusion_zone:
                    overlaps = True
                    break
            
            if overlaps:
                continue
            
            used_intervals.append((idx, idx + m - 1))
            results.append({
                'start_idx': idx,
                'end_idx': idx + m - 1,
                'start_time': self.timestamps[idx],
                'end_time': self.timestamps[idx + m - 1],
                'distance': dist,
                'subsequence': TimeSeries(
                    tsid=f"{self.tsid}_match_{idx}",
                    timestamps=self.timestamps[idx:idx + m],
                    variables=self.variables.copy(),
                    data=[self.data[j].copy() for j in range(idx, idx + m)],
                    metadata=self.metadata
                )
            })
            
            # In topk mode, stop after k matches
            if match_mode == 'topk' and len(results) >= top_k:
                break
        
        return results

    def _find_pattern_basic(
        self,
        pattern_data: np.ndarray,
        data: np.ndarray,
        m: int,
        n: int,
        threshold: Optional[float],
        normalize: bool,
        top_k: int,
        match_mode: str = 'topk',
        exclusion_zone: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Basic sliding window pattern search (fallback when STUMPY unavailable)."""
        distances = []
        pattern_norm = self._z_normalize(pattern_data) if normalize else pattern_data
        
        for i in range(n - m + 1):
            subseq = data[i:i + m]
            if normalize:
                subseq = self._z_normalize(subseq)
            dist = float(np.sqrt(np.sum((subseq - pattern_norm) ** 2)))
            distances.append(dist)
        
        distance_profile = np.array(distances)
        return self._extract_top_k_matches(
            distance_profile, m, threshold, top_k, match_mode, exclusion_zone
        )

    def find_motifs(
        self,
        motif_length: int,
        top_k: int = 5,
        variable: Optional[str] = None,
        min_separation: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Discover recurring patterns (motifs) using Matrix Profile.
        
        Uses STUMPY library for state-of-the-art motif discovery.
        Matrix Profile is the gold standard for time series motif discovery.
        
        Args:
            motif_length: Length of motif to discover
            top_k: Number of motif pairs to return
            variable: Variable to analyze
            min_separation: Minimum distance between motif occurrences
        
        Returns:
            List of motif pairs with indices, times, distances, and subsequences
        
        Requires:
            pip install stumpy
        
        Example:
            motifs = ts.find_motifs(motif_length=24, top_k=3)
            for m in motifs:
                print(f"Similar patterns at {m['motif1_time']} and {m['motif2_time']}")
        """
        data = self.to_numpy(variable).flatten().astype(np.float64)
        m = motif_length
        
        if m > len(data) // 2:
            return []
        
        if min_separation is None:
            min_separation = m
        
        try:
            import stumpy
            
            # Compute Matrix Profile
            # mp[:, 0] = distances to nearest neighbor
            # mp[:, 1] = index of nearest neighbor
            mp = stumpy.stump(data, m)
            
            # Extract motifs using STUMPY's built-in function
            # Returns motif indices and their distances
            motif_distances = mp[:, 0]  # Distance to nearest neighbor
            motif_indices = mp[:, 1].astype(int)  # Index of nearest neighbor
            
            # Find top-k motif pairs
            results = []
            used = set()
            
            # Sort by distance (smallest = best motif)
            sorted_idx = np.argsort(motif_distances)
            
            for i in sorted_idx:
                j = motif_indices[i]
                
                # Skip if already used or too close
                if i in used or j in used:
                    continue
                if abs(i - j) < min_separation:
                    continue
                
                used.add(i)
                used.add(j)
                
                # Ensure i < j for consistency
                if i > j:
                    i, j = j, i
                
                results.append({
                    'motif1_idx': int(i),
                    'motif2_idx': int(j),
                    'motif1_time': self.timestamps[i],
                    'motif2_time': self.timestamps[j],
                    'distance': float(motif_distances[i]),
                    'motif1': TimeSeries(
                        tsid=f"{self.tsid}_motif1_{i}",
                        timestamps=self.timestamps[i:i + m],
                        variables=self.variables.copy(),
                        data=[self.data[k].copy() for k in range(i, i + m)],
                        metadata=self.metadata
                    ),
                    'motif2': TimeSeries(
                        tsid=f"{self.tsid}_motif2_{j}",
                        timestamps=self.timestamps[j:j + m],
                        variables=self.variables.copy(),
                        data=[self.data[k].copy() for k in range(j, j + m)],
                        metadata=self.metadata
                    )
                })
                
                if len(results) >= top_k:
                    break
            
            return results
            
        except ImportError:
            raise ImportError(
                "STUMPY is required for motif discovery. "
                "Install with: pip install stumpy"
            )

    def find_anomalies(
        self,
        window_size: int,
        top_k: int = 5,
        variable: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Find anomalous subsequences (discords) using Matrix Profile.
        
        Uses STUMPY library for efficient discord discovery.
        Discords are subsequences with the LARGEST distance to their nearest neighbor.
        
        Args:
            window_size: Size of subsequences to analyze
            top_k: Number of anomalies to return
            variable: Variable to analyze
        
        Returns:
            List of anomalies with index, time, distance, and subsequence
        
        Requires:
            pip install stumpy
        
        Example:
            anomalies = ts.find_anomalies(window_size=24, top_k=3)
            for a in anomalies:
                print(f"Anomaly at {a['time']} with distance {a['distance']:.3f}")
        """
        data = self.to_numpy(variable).flatten().astype(np.float64)
        m = window_size
        
        if m > len(data) // 2:
            return []
        
        try:
            import stumpy
            
            # Compute Matrix Profile
            mp = stumpy.stump(data, m)
            
            # Discords = highest distances in matrix profile
            # mp[:, 0] contains distance to nearest neighbor
            distances = mp[:, 0]
            
            # Sort by distance (DESCENDING for discords)
            sorted_idx = np.argsort(-distances)
            
            results = []
            used = set()
            
            for idx in sorted_idx:
                # Skip overlapping
                if any(abs(idx - u) < m for u in used):
                    continue
                
                used.add(idx)
                
                results.append({
                    'idx': int(idx),
                    'time': self.timestamps[idx],
                    'distance': float(distances[idx]),
                    'subsequence': TimeSeries(
                        tsid=f"{self.tsid}_anomaly_{idx}",
                        timestamps=self.timestamps[idx:idx + m],
                        variables=self.variables.copy(),
                        data=[self.data[k].copy() for k in range(idx, idx + m)],
                        metadata=self.metadata
                    )
                })
                
                if len(results) >= top_k:
                    break
            
            return results
            
        except ImportError:
            raise ImportError(
                "STUMPY is required for anomaly detection. "
                "Install with: pip install stumpy"
            )

    def _z_normalize(self, arr: np.ndarray) -> np.ndarray:
        """Z-normalize an array."""
        std = np.std(arr)
        if std == 0:
            return arr - np.mean(arr)
        return (arr - np.mean(arr)) / std

    @staticmethod
    def create_pattern(
        pattern_type: str,
        length: int = 10,
        **kwargs
    ) -> 'TimeSeries':
        """
        Create a template pattern for searching.
        
        Args:
            pattern_type: 'spike', 'drop', 'increasing', 'decreasing', 
                         'peak', 'valley', 'step_up', 'step_down', 'oscillation'
            length: Number of points
            **kwargs: amplitude (default 1.0), position (default 0.5)
        
        Returns:
            TimeSeries pattern for use with find_pattern()
        
        Example:
            spike = TimeSeries.create_pattern('spike', length=20)
            matches = ts.find_pattern(spike)
        """
        amplitude = kwargs.get('amplitude', 1.0)
        position = kwargs.get('position', 0.5)
        
        x = np.linspace(0, 1, length)
        
        if pattern_type == 'spike':
            center, width = position, 0.1
            values = amplitude * np.exp(-((x - center) ** 2) / (2 * width ** 2))
        elif pattern_type == 'drop':
            center, width = position, 0.1
            values = -amplitude * np.exp(-((x - center) ** 2) / (2 * width ** 2))
        elif pattern_type == 'increasing':
            values = amplitude * x
        elif pattern_type == 'decreasing':
            values = amplitude * (1 - x)
        elif pattern_type == 'plateau':
            values = np.where(x < position, amplitude, 0.0)
        elif pattern_type == 'valley':
            values = amplitude * (4 * (x - 0.5) ** 2)
        elif pattern_type == 'peak':
            values = amplitude * (1 - 4 * (x - 0.5) ** 2)
        elif pattern_type == 'step_up':
            values = np.where(x < position, 0.0, amplitude)
        elif pattern_type == 'step_down':
            values = np.where(x < position, amplitude, 0.0)
        elif pattern_type == 'oscillation':
            periods = kwargs.get('periods', 2)
            values = amplitude * np.sin(2 * np.pi * periods * x)
        else:
            raise ValueError(f"Unknown pattern type: {pattern_type}")
        
        from datetime import timedelta
        base_time = datetime(2024, 1, 1)
        timestamps = [base_time + timedelta(hours=i) for i in range(length)]
        
        return TimeSeries(
            tsid=f"pattern_{pattern_type}",
            timestamps=timestamps,
            variables=['value'],
            data=[[float(v)] for v in values],
            metadata=TimeSeriesMetadata(
                owner_id='pattern',
                element_type='pattern',
                description=f"{pattern_type} pattern template"
            )
        )

    # -------------------------------------------------------------------------
    # Modification Methods
    # -------------------------------------------------------------------------

    def append_point(self, timestamp: datetime, values: List[float]) -> None:
        """
        Append a new point.

        Args:
            timestamp: Timestamp
            values: List of values (one per variable)
        """
        if len(values) != len(self.variables):
            raise ValueError(
                f"Expected {len(self.variables)} values, got {len(values)}"
            )

        # Check timestamp is after last
        if self.timestamps and timestamp <= self.timestamps[-1]:
            raise ValueError(
                f"Timestamp {timestamp} must be after last timestamp "
                f"{self.timestamps[-1]}"
            )

        self.timestamps.append(timestamp)
        self.data.append(values)

    def update_point(self, timestamp: datetime, values: List[float]) -> None:
        """
        Update values at existing timestamp.

        Args:
            timestamp: Timestamp to update
            values: New values
        """
        try:
            idx = self.timestamps.index(timestamp)
            self.data[idx] = values
        except ValueError:
            raise ValueError(f"Timestamp {timestamp} not found")

    # -------------------------------------------------------------------------
    # Export Methods
    # -------------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.

        Returns:
            Dict representation
        """
        return {
            'tsid': self.tsid,
            'timestamps': [ts.isoformat() for ts in self.timestamps],
            'variables': self.variables,
            'data': self.data,
            'metadata': {
                'owner_id': self.metadata.owner_id,
                'element_type': self.metadata.element_type,
                'description': self.metadata.description,
                'units': self.metadata.units,
                'source': self.metadata.source,
                'tags': self.metadata.tags,
                'custom': self.metadata.custom
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimeSeries':
        """
        Create from dictionary.

        Args:
            data: Dict with time series data

        Returns:
            TimeSeries object
        """
        metadata = TimeSeriesMetadata(
            owner_id=data['metadata']['owner_id'],
            element_type=data['metadata']['element_type'],
            description=data['metadata'].get('description'),
            units=data['metadata'].get('units'),
            source=data['metadata'].get('source'),
            tags=data['metadata'].get('tags', []),
            custom=data['metadata'].get('custom', {})
        )

        return cls(
            tsid=data['tsid'],
            timestamps=[datetime.fromisoformat(ts) for ts in data['timestamps']],
            variables=data['variables'],
            data=data['data'],
            metadata=metadata
        )

    # In TimeSeries class:
    @classmethod
    def from_results(
            cls,
            results: List[tuple],
            var_name: List[str],
            owner_id: str = "global",
            element_type: str = "global",
    ) -> 'TimeSeries':
        """
        Create TimeSeries from computation results.
        @:param    results: List of (timestamp_str, value) tuples
        @:param     owner_id: Owner identifier
        @:param     element_type: Element type
        @:param     label: Optional label to append to metric name
        """
        timestamps = [
            ts if isinstance(ts, datetime) else datetime.fromisoformat(ts)
            for ts, _ in results
        ]
        values = [[val if val is not None else 0] for _, val in results]

        return cls(
            tsid=f"{owner_id}",
            timestamps=timestamps,
            variables=var_name,
            data=values,
            metadata=TimeSeriesMetadata(
                owner_id=owner_id,
                element_type=element_type
            )
        )

    # -------------------------------------------------------------------------
    # Display Methods
    # -------------------------------------------------------------------------

    def __len__(self) -> int:
        """Length of time series"""
        return self.length

    def __repr__(self) -> str:
        """String representation"""
        return (
            f"TimeSeries(tsid={self.tsid}, "
            f"length={self.length}, "
            f"variables={self.variables}, "
            f"range={self.first_timestamp()} to {self.last_timestamp()})"
        )

    def to_table(self, max_rows: int = 20, precision: int = 2) -> str:
        """Display as formatted table"""
        if len(self.timestamps) == 0:
            return "Empty TimeSeries"

        lines = []
        header = ["Timestamp"] + self.variables
        col_widths = [20] + [max(len(v), 15) for v in self.variables]

        header_row = "  ".join(f"{h:<{w}}" for h, w in zip(header, col_widths))
        separator = "  ".join("-" * w for w in col_widths)
        lines.append(header_row)
        lines.append(separator)

        if self.length <= max_rows:
            rows_to_show = list(range(self.length))
            show_ellipsis = False
        else:
            half = max_rows // 2
            rows_to_show = list(range(half)) + list(range(self.length - half, self.length))
            show_ellipsis = True

        for i, idx in enumerate(rows_to_show):
            if show_ellipsis and i == half:
                lines.append("  ".join("..." + " " * (w - 3) for w in col_widths))
            ts_str = self.timestamps[idx].strftime('%Y-%m-%d %H:%M:%S')
            # Handle both single values and nested lists
            values = []
            for j in range(len(self.variables)):
                val = self.data[idx][j]
                # If val is a list, take first element
                if isinstance(val, list):
                    val = val[0] if val else 0.0
                # Format as float
                try:
                    values.append(f"{float(val):.{precision}f}")
                except (TypeError, ValueError):
                    values.append(str(val))
            cells = [ts_str] + values
            row = "  ".join(f"{cell:<{w}}" for cell, w in zip(cells, col_widths))
            lines.append(row)
        return "\n".join(lines)

    def head(self, n: int = 10) -> str:
        """Display first n rows"""
        if len(self.timestamps) == 0:
            return "Empty TimeSeries"
        return f"First {min(n, self.length)} rows:\n\n" + self.to_table(max_rows=n)

    def tail(self, n: int = 10) -> str:
        """Display last n rows"""
        if len(self.timestamps) == 0:
            return "Empty TimeSeries"
        lines = [f"Last {min(n, self.length)} rows:", ""]
        header = ["Timestamp"] + self.variables
        col_widths = [20] + [max(len(v), 15) for v in self.variables]
        lines.append("  ".join(f"{h:<{w}}" for h, w in zip(header, col_widths)))
        lines.append("  ".join("-" * w for w in col_widths))
        start_idx = max(0, self.length - n)
        for idx in range(start_idx, self.length):
            ts_str = self.timestamps[idx].strftime('%Y-%m-%d %H:%M:%S')
            values = [f"{self.data[idx][j]:.2f}" for j in range(len(self.variables))]
            cells = [ts_str] + values
            lines.append("  ".join(f"{cell:<{w}}" for cell, w in zip(cells, col_widths)))
        return "\n".join(lines)

    def __str__(self) -> str:
        """Pretty table representation"""
        if len(self.timestamps) == 0:
            return f"TimeSeries: {self.tsid} (no data)"

        lines = []
        lines.append(f"TimeSeries: {self.tsid}")
        lines.append(f"Variables: {', '.join(self.variables)}")
        lines.append(f"Length: {self.length} measurements")
        lines.append("")
        lines.append(self.to_table(max_rows=15))
        return "\n".join(lines)

    def __getitem__(self, index: int) -> Tuple[datetime, List[float]]:
        """Get (timestamp, values) at index"""
        return (self.timestamps[index], self.data[index])

# =============================================================================
# Utility Functions
# =============================================================================

def create_empty_timeseries(
        tsid: str,
        variables: List[str],
        owner_id: str,
        element_type: str = "node"
) -> TimeSeries:
    """
    Create an empty time series.
    @:param    tsid: Time series ID
    @:param     variables: Variable names
    @:param     owner_id: Owner entity ID
    @:param     element_type: 'node' or 'edges'

    Returns Empty TimeSeries
    """
    return TimeSeries(
        tsid=tsid,
        timestamps=[],
        variables=variables,
        data=[],
        metadata=TimeSeriesMetadata(
            owner_id=owner_id,
            element_type=element_type
        )
    )


def merge_timeseries(
        ts_list: List[TimeSeries],
        new_tsid: str
) -> TimeSeries:
    """
    Merge multiple time series with same variables.

    Args:
        ts_list: List of time series to merge
        new_tsid: ID for merged time series

    Returns:
        Merged time series
    """
    if not ts_list:
        raise ValueError("ts_list cannot be empty")

    # Check all have same variables
    variables = ts_list[0].variables
    for ts in ts_list[1:]:
        if ts.variables != variables:
            raise ValueError("All time series must have same variables")

    # Merge and sort by timestamp
    all_points = []
    for ts in ts_list:
        for i in range(len(ts)):
            all_points.append((ts.timestamps[i], ts.data[i]))

    all_points.sort(key=lambda x: x[0])

    # Remove duplicates (keep first occurrence)
    seen = set()
    unique_points = []
    for ts, values in all_points:
        if ts not in seen:
            seen.add(ts)
            unique_points.append((ts, values))

    timestamps = [ts for ts, _ in unique_points]
    data = [values for _, values in unique_points]

    return TimeSeries(
        tsid=new_tsid,
        timestamps=timestamps,
        variables=variables,
        data=data,
        metadata=ts_list[0].metadata
    )