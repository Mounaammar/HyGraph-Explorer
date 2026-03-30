"""
Test file for TimeSeries.find_pattern() functionality.

Tests the three ways to search for patterns:
1. Pass a TimeSeries pattern (using Pattern fluent API)
2. Use a template name  
3. Use a features dict

Also tests:
- Time window filtering
- Frequency/occurrence count
- Pretty display with print()
"""

import sys
sys.path.insert(0, 'C:/Data/phd/HYGRaph-file/Hygraph-develop')

from datetime import datetime, timedelta
from hygraph_core.model.timeseries import TimeSeries, TimeSeriesMetadata, Pattern, PatternMatches
import numpy as np


def create_test_timeseries():
    """Create a test time series with known patterns."""
    base_time = datetime(2024, 1, 1)
    
    # Create 100 data points (hourly for ~4 days)
    n_points = 100
    timestamps = [base_time + timedelta(hours=i) for i in range(n_points)]
    
    # Create data with some patterns:
    # - Spike at index 20-30
    # - Drop at index 50-60
    # - Stable period at index 70-80
    values = []
    for i in range(n_points):
        if 20 <= i <= 30:
            # Spike pattern
            center = 25
            values.append(10 + 20 * np.exp(-((i - center) ** 2) / 8))
        elif 50 <= i <= 60:
            # Drop pattern  
            center = 55
            values.append(10 - 15 * np.exp(-((i - center) ** 2) / 8))
        elif 70 <= i <= 80:
            # Stable period
            values.append(10 + np.random.normal(0, 0.5))
        else:
            # Normal fluctuation
            values.append(10 + np.random.normal(0, 2))
    
    data = [[v] for v in values]
    
    return TimeSeries(
        tsid="test_ts",
        timestamps=timestamps,
        variables=["value"],
        data=data,
        metadata=TimeSeriesMetadata(
            owner_id="test",
            element_type="node"
        )
    )


def test_pattern_fluent_api():
    """Test Pattern fluent API for creating templates."""
    print("\n" + "="*60)
    print("TEST 0: Pattern Fluent API")
    print("="*60)
    
    # Create patterns using fluent API
    spike = Pattern.spike(length=10)
    drop = Pattern.drop(length=15)
    increasing = Pattern.increasing(length=20)
    oscillation = Pattern.oscillation(length=30, periods=3)
    custom = Pattern.custom([0, 1, 2, 3, 2, 1, 0], name='triangle')
    
    print(f"Spike pattern: {spike.length} points")
    print(f"Drop pattern: {drop.length} points")
    print(f"Increasing pattern: {increasing.length} points")
    print(f"Oscillation pattern: {oscillation.length} points")
    print(f"Custom pattern: {custom.length} points")
    
    # Show that timestamps are just placeholders
    print("\nNote: Pattern timestamps are IGNORED during matching!")
    print("Only the SHAPE (values) matters for pattern matching.")


def test_find_pattern_with_template():
    """Test finding patterns using template names - just print the result!"""
    print("\n" + "="*60)
    print("TEST 1: Find pattern using TEMPLATE - Pretty Display!")
    print("="*60)
    
    ts = create_test_timeseries()
    print(f"Created test timeseries with {ts.length} points\n")
    
    # Find spike patterns - just print!
    print("--- Looking for 'spike' pattern ---")
    matches = ts.find_pattern(template='spike', top_k=3)
    print(matches)  # <-- This is all you need!
    
    # Find drop patterns
    print("\n--- Looking for 'drop' pattern ---")
    matches = ts.find_pattern(template='drop', top_k=3)
    print(matches)




def test_find_pattern_with_fluent_api():
    """Test finding patterns using Pattern fluent API."""
    print("\n" + "="*60)
    print("TEST 3: Find pattern using Pattern.spike() fluent API")
    print("="*60)
    
    ts = create_test_timeseries()
    
    # Create a custom pattern using fluent API
    spike = Pattern.spike(length=10, amplitude=20)
    print(f"Created spike pattern with {spike.length} points\n")
    
    # Search for this pattern
    matches = ts.find_pattern(pattern=spike, top_k=3)
    print(matches)


def test_find_pattern_with_timeseries_query():
    """Test finding patterns using an actual TimeSeries as query pattern."""
    print("\n" + "="*60)
    print("TEST 3b: Find pattern using TIMESERIES as query")
    print("="*60)
    
    # Create two time series
    ts1 = create_test_timeseries()  # Has spike at index 20-30
    
    # Create another time series with similar patterns
    base_time = datetime(2024, 2, 1)  # Different start time!
    n_points = 150
    timestamps = [base_time + timedelta(hours=i) for i in range(n_points)]
    
    values = []
    for i in range(n_points):
        if 30 <= i <= 40:  # Spike at different position
            center = 35
            values.append(10 + 18 * np.exp(-((i - center) ** 2) / 8))
        elif 80 <= i <= 90:  # Another spike
            center = 85
            values.append(10 + 22 * np.exp(-((i - center) ** 2) / 8))
        elif 120 <= i <= 130:  # Third spike
            center = 125
            values.append(10 + 15 * np.exp(-((i - center) ** 2) / 8))
        else:
            values.append(10 + np.random.normal(0, 2))
    
    ts2 = TimeSeries(
        tsid="ts2_with_spikes",
        timestamps=timestamps,
        variables=["value"],
        data=[[v] for v in values],
        metadata=TimeSeriesMetadata(owner_id="test2", element_type="node")
    )
    
    # EXTRACT a pattern from ts1 (the spike segment)
    # This is like saying "find me more patterns that look like this segment"
    query_pattern = ts1.slice_by_time(
        datetime(2024, 1, 1, 20),  # Index ~20
        datetime(2024, 1, 2, 6)    # Index ~30
    )
    
    print(f"Source TimeSeries (ts1): {ts1.length} points")
    print(f"Target TimeSeries (ts2): {ts2.length} points")
    print(f"Query pattern extracted from ts1: {query_pattern.length} points")
    print(f"Query pattern time range: {query_pattern.first_timestamp()} to {query_pattern.last_timestamp()}")
    print()
    
    # Now search for this pattern in ts2
    print("--- Searching ts2 for patterns similar to extracted segment from ts1 ---")
    matches = ts2.find_pattern(pattern=query_pattern, top_k=5)
    print(matches)
    
    # Also demonstrate: find similar patterns WITHIN THE SAME time series
    print("\n--- Finding similar patterns within ts2 itself ---")
    # Extract a segment from ts2 and search for similar segments in ts2
    segment = ts2.slice_by_time(
        datetime(2024, 2, 2, 6),   # Around index 30
        datetime(2024, 2, 2, 16)   # Around index 40
    )
    print(f"Extracted segment: {segment.length} points")
    
    matches_self = ts2.find_pattern(pattern=segment, top_k=5)
    print(matches_self)
    
    print("\nNote: The first match is usually the segment itself (distance ~0)")
    print("Other matches are similar patterns elsewhere in the time series.")


def test_find_pattern_with_time_window():
    """Test finding patterns within a specific time window."""
    print("\n" + "="*60)
    print("TEST 4: Find pattern with TIME WINDOW")
    print("="*60)
    
    ts = create_test_timeseries()
    
    # Search only in first half of data
    start_time = datetime(2024, 1, 1)
    end_time = datetime(2024, 1, 2, 12)  # First 36 hours
    
    print(f"Searching within time window: {start_time} to {end_time}")
    
    # Should find the spike at index 20-30 (within first 36 hours)
    matches = ts.find_pattern(
        template='spike',
        time_window=(start_time, end_time),
        top_k=3
    )
    print(matches)


def test_pattern_matches_iteration():
    """Test iterating over PatternMatches and accessing properties."""
    print("\n" + "="*60)
    print("TEST 5: PatternMatches iteration and properties")
    print("="*60)
    
    ts = create_test_timeseries()
    matches = ts.find_pattern(template='spike', top_k=5)
    
    # Access frequency info
    print(f"Frequency: {matches.frequency} occurrences")
    print(f"Frequency per day: {matches.frequency_per_day:.2f}")
    
    # Get best match
    best = matches.best()
    if best:
        print(f"\nBest match: {best}")
        print(f"  - Start time: {best.start_time}")
        print(f"  - Similarity: {best.similarity:.1f}%")
        print(f"  - Distance: {best.distance:.3f}")
    
    # Iterate over matches
    print("\nIterating over all matches:")
    for i, m in enumerate(matches):
        print(f"  {i+1}. {m}")  # Each PatternMatch has __str__
    
    # Convert to list for JSON
    print(f"\nAs list (for JSON): {len(matches.to_list())} items")


def test_contains_pattern():
    """Test contains_pattern() for quick boolean check."""
    print("\n" + "="*60)
    print("TEST 6: contains_pattern() boolean check")
    print("="*60)
    
    ts = create_test_timeseries()
    
    # Check using template
    has_spike = ts.contains_pattern(template='spike')
    print(f"Contains 'spike' pattern: {has_spike}")
    
    has_drop = ts.contains_pattern(template='drop')
    print(f"Contains 'drop' pattern: {has_drop}")

    
    # Check with time window
    early_window = (datetime(2024, 1, 1), datetime(2024, 1, 1, 12))
    has_spike_early = ts.contains_pattern(template='spike', time_window=early_window)
    print(f"Contains spike in first 12 hours: {has_spike_early}")
    
    spike_window = (datetime(2024, 1, 1, 18), datetime(2024, 1, 2, 6))
    has_spike_there = ts.contains_pattern(template='spike', time_window=spike_window)
    print(f"Contains spike between hour 18-30: {has_spike_there}")






if __name__ == "__main__":
    print("\n" + "#"*60)
    print("# TESTING TIMESERIES PATTERN FINDING")
    print("#"*60)
    
    # Run all tests
    test_pattern_fluent_api()
    test_find_pattern_with_template()
    test_find_pattern_with_fluent_api()
    test_find_pattern_with_timeseries_query()  # NEW: TimeSeries as query
    test_find_pattern_with_time_window()
    test_pattern_matches_iteration()
    test_contains_pattern()

    
    print("\n" + "#"*60)
    print("# ALL TESTS COMPLETED!")
    print("#"*60)
