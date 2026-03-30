"""
METR-LA Dataset Converter for HyGraph

Converts METR-LA raw files (HDF5 + pickle + locations CSV) into the CSV format
consumed by HyGraph's generic csv_loader.

Generates:
    csv/nodes/Sensor.csv       — 207 sensor nodes with speed_ts_id, lat, lon, free_flow_speed
    csv/edges/road_segment.csv — adjacency edges with proximity weight
    csv/measurements.csv       — ~7M speed measurements (ts_id, variable, timestamp, value)

Usage:
    python -m hygraph_core.ingest.metrla_loader

    Or from code:
        from hygraph_core.ingest.metrla_loader import convert_metrla
        convert_metrla(
            h5_path="inputFiles/metr-la/METR-LA.h5",
            pkl_path="inputFiles/metr-la/adj_METR-LA.pkl",
            output_dir="inputFiles/metrla_csv"
        )

    Then load into HyGraph via the standard pipeline:
        from hygraph_core.ingest.csv_loader import load_csv
        load_csv(csv_dir="inputFiles/metrla_csv", graph_name="hygraph")
"""

from __future__ import annotations

import csv
import os
import pickle
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np




METRLA_START = datetime(2012, 3, 1, 0, 0, 0)
METRLA_STEP_MINUTES = 5
METRLA_END_APPROX = datetime(2012, 6, 30, 23, 55, 0)




def read_h5(filepath: str) -> Tuple[np.ndarray, List[str]]:
    """
    Read METR-LA HDF5 file.

    Returns:
        (speed_matrix, sensor_ids)
        speed_matrix: numpy array [num_timesteps, num_sensors]
        sensor_ids: list of sensor ID strings
    """
    try:
        import h5py
    except ImportError:
        raise ImportError("h5py required: pip install h5py")

    with h5py.File(filepath, "r") as f:
        if "df" in f:
            speed_data = f["df"]["block0_values"][:]
            if "axis1" in f["df"]:
                raw_ids = f["df"]["axis1"][:]
                sensor_ids = [
                    str(sid) if isinstance(sid, (int, float))
                    else sid.decode("utf-8") if isinstance(sid, bytes)
                    else str(sid)
                    for sid in raw_ids
                ]
            else:
                sensor_ids = [str(i) for i in range(speed_data.shape[1])]
        elif "data" in f:
            speed_data = f["data"][:]
            sensor_ids = [str(i) for i in range(speed_data.shape[1])]
        else:
            key = list(f.keys())[0]
            speed_data = f[key][:]
            sensor_ids = [str(i) for i in range(speed_data.shape[1])]

    print(f"  Speed matrix: {speed_data.shape} "
          f"({speed_data.shape[0]} timesteps x {speed_data.shape[1]} sensors)")
    print(f"  Sensor IDs: {sensor_ids[:5]}... ({len(sensor_ids)} total)")

    return speed_data, sensor_ids


def read_adjacency_pkl(
    filepath: str,
) -> Tuple[List[str], List[Tuple[str, str, float]]]:
    """
    Read adjacency pickle file (DCRNN format).

    Returns:
        (sensor_ids, edges) where edges is list of (from_id, to_id, weight)
    """
    with open(filepath, "rb") as f:
        data = pickle.load(f, encoding="latin1")

    if isinstance(data, (list, tuple)) and len(data) == 3:
        sensor_ids_raw = data[0]
        adj_mx = data[2]
        sensor_ids = [str(sid) for sid in sensor_ids_raw]
    elif isinstance(data, dict):
        sensor_ids = [
            str(sid)
            for sid in data.get("sensor_ids", data.get("ids", []))
        ]
        adj_mx = data.get("adj_mx", data.get("adjacency", None))
        if adj_mx is None:
            raise ValueError("Cannot find adjacency matrix in pickle")
    else:
        raise ValueError(f"Unexpected pickle format: {type(data)}")

    adj_array = np.array(adj_mx)
    edges = []

    for i in range(adj_array.shape[0]):
        for j in range(adj_array.shape[1]):
            weight = adj_array[i, j]
            if weight > 0 and i != j:
                edges.append((sensor_ids[i], sensor_ids[j], float(weight)))

    print(f"  Adjacency: {adj_array.shape}, {len(edges)} non-zero edges")
    print(f"  Sensor IDs from pkl: {sensor_ids[:5]}... ({len(sensor_ids)} total)")

    return sensor_ids, edges


def read_sensor_locations(filepath: str) -> Dict[str, Tuple[float, float]]:
    """
    Read sensor_locations.csv → {sensor_id: (lat, lon)}.
    """
    locations: Dict[str, Tuple[float, float]] = {}
    if not os.path.exists(filepath):
        print(f"  WARNING: {filepath} not found — nodes will have no coordinates")
        return locations

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = str(row["sensor_id"])
            locations[sid] = (float(row["latitude"]), float(row["longitude"]))

    print(f"  Sensor locations: {len(locations)} entries")
    return locations




def compute_free_flow_speeds(
    speed_matrix: np.ndarray,
    percentile: float = 95.0,
) -> np.ndarray:
    """
    Compute per-sensor free-flow speed as the given percentile of speed
    during off-peak hours (10 PM – 5 AM), following Li et al. (PNAS 2015).

    Returns:
        numpy array of shape (num_sensors,) with free-flow speed per sensor.
    """
    n_per_day = 288  # 24h * 60min / 5min
    off_peak_hours = {22, 23, 0, 1, 2, 3, 4}

    off_peak_mask = np.array([
        ((t % n_per_day) * 5 // 60) in off_peak_hours
        for t in range(speed_matrix.shape[0])
    ])

    # Replace 0 with NaN (0 = missing / no reading)
    speed_clean = speed_matrix.copy().astype(np.float64)
    speed_clean[speed_clean == 0] = np.nan

    off_peak = speed_clean[off_peak_mask, :]
    with np.errstate(all="ignore"):
        ff_speeds = np.nanpercentile(off_peak, percentile, axis=0)

    # For sensors with no off-peak data, fall back to overall percentile
    missing = np.isnan(ff_speeds)
    if missing.any():
        ff_speeds[missing] = np.nanpercentile(
            speed_clean[:, missing], percentile, axis=0
        )

    print(f"  Free-flow speed ({percentile}th pct, off-peak):")
    print(f"    Range: {np.nanmin(ff_speeds):.1f} – {np.nanmax(ff_speeds):.1f} mph")
    print(f"    Mean:  {np.nanmean(ff_speeds):.1f} mph")
    print(f"    Median: {np.nanmedian(ff_speeds):.1f} mph")

    return ff_speeds




def generate_nodes_csv(
    output_dir: Path,
    sensor_ids: List[str],
    locations: Dict[str, Tuple[float, float]],
    free_flow_speeds: np.ndarray,
    ts_counter_start: int = 0,
) -> Tuple[int, Dict[str, int]]:
    """
    Generate nodes/Sensor.csv in HyGraph format.
    Includes lat, lon, and free_flow_speed as static properties.
    """
    nodes_dir = output_dir / "nodes"
    nodes_dir.mkdir(parents=True, exist_ok=True)

    csv_path = nodes_dir / "Sensor.csv"
    node_id_map: Dict[str, int] = {}
    ts_counter = ts_counter_start

    fieldnames = [
        "id", "node_id", "sensor_id",
        "lat", "lon", "free_flow_speed",
        "speed_ts_id",
        "start_time", "end_time",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for idx, sensor_id in enumerate(sensor_ids):
            seq_id = idx + 1
            node_id_map[sensor_id] = seq_id
            ts_id = f"ts_{ts_counter}"
            ts_counter += 1

            lat, lon = locations.get(sensor_id, (0.0, 0.0))
            ff_speed = round(float(free_flow_speeds[idx]), 2) if idx < len(free_flow_speeds) else 0.0

            writer.writerow({
                "id": seq_id,
                "node_id": sensor_id,
                "sensor_id": sensor_id,
                "lat": round(lat, 5),
                "lon": round(lon, 5),
                "free_flow_speed": ff_speed,
                "speed_ts_id": ts_id,
                "start_time": METRLA_START.isoformat(),
                "end_time": METRLA_END_APPROX.isoformat(),
            })

    print(f"  Wrote {csv_path}: {len(sensor_ids)} sensors "
          f"(with coords: {sum(1 for s in sensor_ids if s in locations)})")
    return ts_counter, node_id_map


def generate_edges_csv(
    output_dir: Path,
    edges: List[Tuple[str, str, float]],
    node_id_map: Dict[str, int],
) -> int:
    """Generate edges/road_segment.csv in HyGraph format."""
    edges_dir = output_dir / "edges"
    edges_dir.mkdir(parents=True, exist_ok=True)

    csv_path = edges_dir / "road_segment.csv"

    fieldnames = [
        "start_id", "start_vertex_type", "end_id", "end_vertex_type",
        "weight", "label", "start_time", "end_time",
    ]

    written = 0
    skipped = 0

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for from_id, to_id, weight in edges:
            src_seq = node_id_map.get(from_id)
            dst_seq = node_id_map.get(to_id)

            if src_seq is None or dst_seq is None:
                skipped += 1
                continue

            writer.writerow({
                "start_id": src_seq,
                "start_vertex_type": "Sensor",
                "end_id": dst_seq,
                "end_vertex_type": "Sensor",
                "weight": round(weight, 4),
                "label": "road_segment",
                "start_time": METRLA_START.isoformat(),
                "end_time": METRLA_END_APPROX.isoformat(),
            })
            written += 1

    print(f"  Wrote {csv_path}: {written} edges ({skipped} skipped)")
    return written


def generate_measurements_csv(
    output_dir: Path,
    speed_matrix: np.ndarray,
    sensor_ids: List[str],
    ts_counter_start: int = 0,
    chunk_size: int = 100000,
) -> int:
    """Generate measurements.csv in HyGraph format."""
    csv_path = output_dir / "measurements.csv"

    num_timesteps = speed_matrix.shape[0]
    num_sensors = speed_matrix.shape[1]
    step = timedelta(minutes=METRLA_STEP_MINUTES)

    total_written = 0
    start_time = time.time()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ts_id", "variable", "timestamp", "value"])

        for col_idx in range(num_sensors):
            ts_id = f"ts_{ts_counter_start + col_idx}"
            speeds = speed_matrix[:, col_idx]

            rows = []
            for t_idx in range(num_timesteps):
                val = speeds[t_idx]
                if np.isnan(val) or val == 0:
                    continue  # 0 = missing/faulty sensor reading in METR-LA

                ts_time = METRLA_START + t_idx * step
                rows.append([
                    ts_id,
                    "speed",
                    ts_time.strftime("%Y-%m-%dT%H:%M:%S"),
                    round(float(val), 2),
                ])

                if len(rows) >= chunk_size:
                    writer.writerows(rows)
                    total_written += len(rows)
                    rows = []

            if rows:
                writer.writerows(rows)
                total_written += len(rows)

            if (col_idx + 1) % 20 == 0:
                elapsed = time.time() - start_time
                rate = total_written / elapsed if elapsed > 0 else 0
                print(f"    Sensor {col_idx + 1}/{num_sensors}: "
                      f"{total_written:,} rows ({rate:,.0f} rows/s)")

    elapsed = time.time() - start_time
    print(f"  Wrote {csv_path}: {total_written:,} measurements in {elapsed:.1f}s")
    return total_written




def convert_metrla(
    h5_path: str = None,
    pkl_path: str = None,
    loc_path: str = None,
    output_dir: str = None,
) -> Dict[str, Any]:
    """
    Convert METR-LA dataset to HyGraph CSV format.

    Args:
        h5_path:    Path to METR-LA.h5
        pkl_path:   Path to adj_METR-LA.pkl
        loc_path:   Path to sensor_locations.csv
        output_dir: Output directory for CSV files
    """
    base_dir = Path(__file__).parent.parent / "inputFiles"

    if h5_path is None:
        h5_path = str(base_dir / "metr-la" / "METR-LA.h5")
    if pkl_path is None:
        pkl_path = str(base_dir / "metr-la" / "adj_METR-LA.pkl")
    if loc_path is None:
        loc_path = str(base_dir / "metr-la" / "sensor_locations.csv")
    if output_dir is None:
        output_dir = str(base_dir / "metrla_csv")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("METR-LA -> HyGraph CSV Conversion")
    print("=" * 70)
    start = time.time()

    # Step 1: Read HDF5
    print("\n[1/5] Reading speed data (HDF5)...")
    speed_matrix, h5_sensor_ids = read_h5(h5_path)

    # Step 2: Read adjacency
    print("\n[2/5] Reading adjacency (pickle)...")
    pkl_sensor_ids, edges = read_adjacency_pkl(pkl_path)

    # Use pkl sensor IDs as canonical
    if len(pkl_sensor_ids) == len(h5_sensor_ids):
        sensor_ids = pkl_sensor_ids
        print(f"  Using pickle sensor IDs ({len(sensor_ids)} sensors)")
    else:
        print(f"  WARNING: pkl has {len(pkl_sensor_ids)} sensors, "
              f"h5 has {len(h5_sensor_ids)} columns")
        sensor_ids = pkl_sensor_ids[:speed_matrix.shape[1]]

    # Step 3: Read sensor locations
    print("\n[3/5] Reading sensor locations...")
    locations = read_sensor_locations(loc_path)

    # Step 4: Compute free-flow speed
    print("\n[4/5] Computing free-flow speeds (95th pct, off-peak)...")
    free_flow_speeds = compute_free_flow_speeds(speed_matrix, percentile=95.0)

    # Step 5: Generate CSVs
    print("\n[5/5] Generating CSV files...")

    print("  --- Nodes ---")
    ts_counter, node_id_map = generate_nodes_csv(
        output_dir, sensor_ids, locations, free_flow_speeds, ts_counter_start=0
    )

    print("  --- Edges ---")
    edge_count = generate_edges_csv(output_dir, edges, node_id_map)

    print("  --- Measurements (this may take a minute) ---")
    meas_count = generate_measurements_csv(
        output_dir, speed_matrix, sensor_ids, ts_counter_start=0
    )

    elapsed = time.time() - start

    print("\n" + "=" * 70)
    print("Conversion Complete!")
    print(f"  Sensors:          {len(sensor_ids)}")
    print(f"  With coordinates: {sum(1 for s in sensor_ids if s in locations)}")
    print(f"  Edges:            {edge_count}")
    print(f"  Measurements:     {meas_count:,}")
    print(f"  Time:             {elapsed:.1f}s")
    print(f"  Output:           {output_dir}")
    print()
    print("Next step -- load into HyGraph:")
    print("  from hygraph_core.ingest.csv_loader import load_csv")
    print(f'  load_csv(csv_dir="{output_dir}", graph_name="hygraph")')
    print("=" * 70)

    return {
        "sensors": len(sensor_ids),
        "edges": edge_count,
        "measurements": meas_count,
        "output_dir": str(output_dir),
        "free_flow_speeds": {
            "min": round(float(np.nanmin(free_flow_speeds)), 1),
            "max": round(float(np.nanmax(free_flow_speeds)), 1),
            "mean": round(float(np.nanmean(free_flow_speeds)), 1),
            "median": round(float(np.nanmedian(free_flow_speeds)), 1),
        },
    }




if __name__ == "__main__":
    convert_metrla()