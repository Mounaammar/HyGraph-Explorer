"""
Complete Example - EXACTLY as you described

This shows:
1. json_to_csv() with your field mappings
2. load_csv() to load into database
3. Add individual node
4. Add individual edges
5. Display node and edges
6. Query by label
7. Query by time series constraints
"""

from pathlib import Path
from datetime import datetime

from hygraph_core.hygraph.hygraph import HyGraph
from hygraph_core.model.timeseries import TimeSeries, TimeSeriesMetadata


def main():
    """Complete workflow exactly as you described"""

    print("=" * 80)
    print("HyGraph - Complete Workflow")
    print("=" * 80)

    # =========================================================================
    # STEP 1: INGEST DATA (Exactly as you described)
    # =========================================================================

    print("\n📥 STEP 1: INGEST DATA FROM JSON")
    print("-" * 80)

    # Initialize HyGraph
    hg = HyGraph("postgresql://localhost:5432/hygraph")

    # Define field mappings
    # These tell the converter which fields in your JSON correspond to node/edges properties
    node_field_map = {
        "oid": "station_id",  # YOUR JSON field for node ID
        "start_time": "installation_date",  # YOUR JSON field for start time
        "end_time": "removal_date"  # YOUR JSON field for end time
    }

    edge_field_map = {
        "oid": "trip_id",  # YOUR JSON field for edges ID
        "source_id": "start_station_id",  # YOUR JSON field for source node
        "target_id": "end_station_id",  # YOUR JSON field for target node
        "start_time": "start_time",
        "end_time": "end_time"
    }

    # Ingest data - EXACTLY as you described
    stats = hg.ingest_from_json(
        json_dir=Path("inputFiles"),
        node_field_map=node_field_map,
        edge_field_map=edge_field_map,
        output_dir=Path("inputFiles/csv3")
    )

    print(f"\n Data Ingested:")
    print(f"   Nodes: {stats.get('nodes', stats.get('nodes_total', 0)):,}")
    print(f"   Edges: {stats.get('edges', stats.get('edges_total', 0)):,}")
    print(f"   Measurements: {stats.get('measurements', 0):,}")

    # =========================================================================
    # STEP 2: ADD INDIVIDUAL NODE
    # =========================================================================

    print("\n➕ STEP 2: ADD INDIVIDUAL NODE")
    print("-" * 80)

    # Create time series data
    timestamps = [datetime(2024, 11, 10, i) for i in range(24)]
    bike_data = [[float(10 + 5 * (i % 6))] for i in range(24)]

    bikes_ts = TimeSeries(
        tsid="ts_bikes_new_station",
        timestamps=timestamps,
        variables=["num_bikes_available"],
        data=bike_data,
        metadata=TimeSeriesMetadata(owner_id="new_station_100", owner_type="node")
    )

    # Create node
    hg.create_node('new_station_100') \
        .with_label('Station') \
        .with_property('name', 'New Central Station') \
        .with_property('capacity', 50) \
        .with_property('region', 'Downtown') \
        .with_ts_property('num_bikes_available', bikes_ts) \
        .create()

    print("✅ Created node: new_station_100")

    # =========================================================================
    # STEP 3: ADD INDIVIDUAL EDGE
    # =========================================================================

    print("\n➕ STEP 3: ADD INDIVIDUAL EDGE")
    print("-" * 80)

    # Create time series for edges
    speed_timestamps = [datetime(2024, 11, 10, 10, i) for i in range(30)]
    speed_data = [[float(15 + 2 * (i % 5))] for i in range(30)]

    speed_ts = TimeSeries(
        tsid="ts_speed_new_trip",
        timestamps=speed_timestamps,
        variables=["bike_speed"],
        data=speed_data,
        metadata=TimeSeriesMetadata(owner_id="new_trip_100", owner_type="edges")
    )

    # Get first station ID from ingested data
    stations = hg.get_nodes_by_label('Station', limit=1, include_timeseries=False)
    if stations:
        first_station_id = stations[0]['uid']

        # Create edges
        hg.create_edge(first_station_id, 'new_station_100', 'new_trip_100') \
            .with_label('trip') \
            .with_property('duration_minutes', 25) \
            .with_property('user_id', 'user_123') \
            .with_ts_property('bike_speed', speed_ts) \
            .create()

        print(f"✅ Created edges: {first_station_id} → new_station_100")
    else:
        print("⚠️ No stations found to create edges")

    # =========================================================================
    # STEP 4: DISPLAY NODE
    # =========================================================================

    print("\n📊 STEP 4: DISPLAY NODE")
    print("-" * 80)

    # Display the node we just created
    node = hg.get_node('new_station_100')

    if node:
        print(f"\n🏢 Node Details:")
        print(f"   ID: {node['uid']}")
        print(f"   Label: {node['label']}")
        print(f"   Start Time: {node['start_time']}")
        print(f"   End Time: {node['end_time']}")
        print(f"\n   Properties:")
        for key, value in node['properties'].items():
            if key != 'temporal_properties':
                print(f"      {key}: {value}")
        print(f"\n   Time Series:")
        for var_name, measurements in node['timeseries'].items():
            print(f"      {var_name}: {len(measurements)} measurements")
            if measurements:
                first = measurements[0]
                last = measurements[-1]
                print(f"         From: {first[0]}")
                print(f"         To:   {last[0]}")

    # =========================================================================
    # STEP 5: DISPLAY EDGE
    # =========================================================================

    print("\n📊 STEP 5: DISPLAY EDGE")
    print("-" * 80)

    # Display the edges we just created
    edge = hg.get_edge('new_trip_100')

    if edge:
        print(f"\n🔗 Edge Details:")
        print(f"   ID: {edge['uid']}")
        print(f"   Label: {edge['label']}")
        print(f"   Route: {edge['src_uid']} → {edge['dst_uid']}")
        print(f"   Start Time: {edge['start_time']}")
        print(f"   End Time: {edge['end_time']}")
        print(f"\n   Properties:")
        for key, value in edge['properties'].items():
            if key != 'temporal_properties':
                print(f"      {key}: {value}")
        print(f"\n   Time Series:")
        for var_name, measurements in edge['timeseries'].items():
            print(f"      {var_name}: {len(measurements)} measurements")

    # =========================================================================
    # STEP 6: QUERY BY LABEL
    # =========================================================================

    print("\n🔍 STEP 6: QUERY BY LABEL")
    print("-" * 80)

    # Query all stations
    all_stations = hg.query().nodes(label='Station').execute()
    print(f"\n📍 Total Stations: {len(all_stations)}")

    # Show first 5 stations
    print(f"\n   First 5 stations:")
    for i, station in enumerate(all_stations[:5], 1):
        name = station.properties.get('name', 'N/A')
        capacity = station.properties.get('capacity', 'N/A')
        print(f"      {i}. {station.oid}: {name} (capacity: {capacity})")

    # Query all trips
    all_trips = hg.query().edges(label='trip').execute()
    print(f"\n🚴 Total Trips: {len(all_trips)}")

    # Count by label
    station_count = hg.count_nodes(label='Station')
    trip_count = hg.count_edges(label='trip')
    print(f"\n   Count Summary:")
    print(f"      Stations: {station_count}")
    print(f"      Trips: {trip_count}")

    # =========================================================================
    # STEP 7: QUERY BY TIME SERIES CONSTRAINTS
    # =========================================================================

    print("\n⏱️ STEP 7: QUERY BY TIME SERIES CONSTRAINTS")
    print("-" * 80)

    # Example 1: Find stations with low bike availability
    print("\n   Example 1: Stations with low average bike availability")

    def low_bikes(measurements):
        """Filter: average bikes < 10"""
        if not measurements or len(measurements) == 0:
            return False
        values = [val for ts, val in measurements]
        avg = sum(values) / len(values)
        return avg < 10

    # Check what time series variables exist
    if all_stations and all_stations[0].timeseries:
        ts_variables = list(all_stations[0].timeseries.keys())
        print(f"   Available time series: {ts_variables}")

        # Query by first available time series variable
        if ts_variables:
            var_name = ts_variables[0]
            print(f"   Querying by '{var_name}'...")

            low_availability = hg.get_nodes_by_timeseries_filter(
                variable=var_name,
                filter_func=low_bikes,
                label='Station'
            )
            print(f"   ✅ Found {len(low_availability)} stations with low availability")

    # Example 2: Find stations with high variance
    print("\n   Example 2: Stations with high bike availability variance")

    def high_variance(measurements):
        """Filter: standard deviation > 5"""
        if not measurements or len(measurements) < 2:
            return False
        values = [val for ts, val in measurements]
        import numpy as np
        return np.std(values) > 5.0

    if all_stations and all_stations[0].timeseries:
        ts_variables = list(all_stations[0].timeseries.keys())
        if ts_variables:
            var_name = ts_variables[0]

            high_var_stations = hg.get_nodes_by_timeseries_filter(
                variable=var_name,
                filter_func=high_variance,
                label='Station'
            )
            print(f"   ✅ Found {len(high_var_stations)} stations with high variance")

    # Example 3: Find busy stations (many measurements)
    print("\n   Example 3: Busy stations (many measurements)")

    def many_measurements(measurements):
        """Filter: more than 1000 measurements"""
        return len(measurements) > 1000

    if all_stations and all_stations[0].timeseries:
        ts_variables = list(all_stations[0].timeseries.keys())
        if ts_variables:
            var_name = ts_variables[0]

            busy_stations = hg.get_nodes_by_timeseries_filter(
                variable=var_name,
                filter_func=many_measurements,
                label='Station'
            )
            print(f"   ✅ Found {len(busy_stations)} busy stations")

    # =========================================================================
    # STEP 8: ADDITIONAL QUERIES
    # =========================================================================

    print("\n🔍 STEP 8: ADDITIONAL QUERIES")
    print("-" * 80)

    # Query by static property
    print("\n   Query by static property (capacity >= 50):")
    high_capacity = hg.query() \
        .nodes(label='Station') \
        .where(lambda n: n.properties.get('capacity', 0) >= 50) \
        .execute()
    print(f"   ✅ Found {len(high_capacity)} high-capacity stations")

    # Graph navigation
    print("\n   Graph navigation:")
    if all_stations:
        node_id = all_stations[0].oid
        neighbors = hg.get_neighbors(node_id, direction='out')
        degree = hg.get_node_degree(node_id)
        print(f"   Node {node_id}:")
        print(f"      Neighbors: {len(neighbors)}")
        print(f"      Degree: {degree}")

    # Time series statistics
    print("\n   Time series statistics:")
    if all_stations and all_stations[0].timeseries:
        node_id = all_stations[0].oid
        ts_variables = list(all_stations[0].timeseries.keys())
        if ts_variables:
            var_name = ts_variables[0]
            stats = hg.get_timeseries_statistics(node_id, var_name)
            if stats:
                print(f"   Statistics for {node_id}.{var_name}:")
                print(f"      Mean: {stats['mean']:.2f}")
                print(f"      Min: {stats['min']:.2f}")
                print(f"      Max: {stats['max']:.2f}")
                print(f"      Std Dev: {stats['std']:.2f}")
                print(f"      Count: {stats['count']}")

    # Close
    hg.close()

    print("\n" + "=" * 80)
    print("✅ COMPLETE! All steps executed successfully.")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n Error: {e}")
        import traceback

        traceback.print_exc()