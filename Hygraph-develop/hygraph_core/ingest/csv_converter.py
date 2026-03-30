# hygraph_core/ingest/csv_converter.py
import csv
import json
from pathlib import Path
from typing import Dict
from collections import defaultdict

from hygraph_core.utils.logging_config import setup_logging

logger = setup_logging(log_file="logs/csv_conversion.log")


def json_to_csv(
        json_dir: Path,
        output_dir: Path,
        node_field_map: Dict[str, str],
        edge_field_map: Dict[str, str]
) -> dict:
    """
    Convert JSON to AGE CSV with SEQUENTIAL IDs (not original IDs).
    Original IDs are stored as 'orig_id' property.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    nodes_output = output_dir / "nodes"
    edges_output = output_dir / "edges"
    nodes_output.mkdir(exist_ok=True)
    edges_output.mkdir(exist_ok=True)

    logger.info("=" * 70)
    logger.info("JSON → CSV CONVERSION (Sequential IDs for AGE)")
    logger.info("=" * 70)

    # Sequential ID generator
    node_id_counter = 1
    node_id_map = {}  # Maps original_id → sequential_id

    ts_counter = 0
    nodes_by_label = defaultdict(list)
    all_measurements = []

    # ========================================
    # Process Nodes
    # ========================================
    nodes_dir = json_dir / "nodes"
    if nodes_dir.exists():
        logger.info("Processing nodes...")

        for json_file in sorted(nodes_dir.glob("*.json")):
            label = json_file.stem
            logger.info(f"  Reading {json_file.name} (label={label})")

            with open(json_file, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    data = [data]

            for record in data:
                original_id = str(record.get(node_field_map["oid"]))

                #  Assign sequential ID
                sequential_id = node_id_counter
                node_id_counter += 1
                node_id_map[original_id] = sequential_id

                # Build node row
                node_row = {
                    "id": sequential_id,  #  Sequential ID for AGE
                    "node_id": original_id,  #  Store original ID as property
                    "start_time": record.get(node_field_map.get("start_time", "")),
                    "end_time": record.get(node_field_map.get("end_time", "")),
                }

                # Add other properties
                for key, value in record.items():
                    if key not in [node_field_map["oid"],
                                   node_field_map.get("start_time"),
                                   node_field_map.get("end_time"),
                                   "ts", "labels", "node_id", "label"]:
                        if value is not None:
                            node_row[key] = str(value) if not isinstance(value, (int, float, bool)) else value

                # Process time series
                ts_block = record.get("ts", {})
                ts_ids = {}

                if ts_block:
                    for variable, measurements in ts_block.items():
                        ts_id = f"ts_{ts_counter}"
                        ts_counter += 1
                        ts_ids[f"{variable}_ts_id"] = ts_id

                        for entry in measurements:
                            timestamp = entry.get("Start") or entry.get("start") or entry.get("time")
                            value = entry.get("Value") or entry.get("value")
                            if timestamp and value is not None:
                                all_measurements.append({
                                    "ts_id": ts_id,
                                    "variable": variable,
                                    "timestamp": timestamp,
                                    "value": float(value)
                                })

                nodes_by_label[label].append((sequential_id, node_row, ts_ids))

    # Write nodes CSV
    logger.info("Writing node CSV files...")
    for label, nodes in nodes_by_label.items():
        csv_file = nodes_output / f"{label}.csv"
        if not nodes:
            continue

        all_keys = set()
        all_keys.add("id")
        for _, node_row, ts_ids in nodes:
            all_keys.update(node_row.keys())
            all_keys.update(ts_ids.keys())

        fieldnames = ["id"] + sorted([k for k in all_keys if k != "id"])

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for _, node_row, ts_ids in nodes:
                complete_row = {**node_row, **ts_ids}
                writer.writerow(complete_row)

        logger.info(f"  ✓ {csv_file.name}: {len(nodes):,} rows")

    # ========================================
    # Process Edges
    # ========================================
    edges_by_label = defaultdict(list)
    edges_dir = json_dir / "edges"

    if edges_dir.exists():
        logger.info("Processing edges...")

        for json_file in sorted(edges_dir.glob("*.json")):
            label = json_file.stem
            logger.info(f"  Reading {json_file.name} (label={label})")

            with open(json_file, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    data = [data]

            for record in data:
                original_src = str(record.get(edge_field_map["source_id"]))
                original_dst = str(record.get(edge_field_map["target_id"]))

                #  Map original IDs to sequential IDs
                src_id = node_id_map.get(original_src)
                dst_id = node_id_map.get(original_dst)

                if not src_id or not dst_id:
                    logger.warning(f"  Edge references unknown nodes: {original_src} → {original_dst}")
                    continue

                # Get labels
                src_label = None
                dst_label = None
                for lbl, nodes in nodes_by_label.items():
                    for nid, _, _ in nodes:
                        if nid == src_id:
                            src_label = lbl
                        if nid == dst_id:
                            dst_label = lbl

                if not src_label or not dst_label:
                    logger.warning(f"  Cannot determine vertex types for edges")
                    continue

                edge_row = {
                    "start_id": src_id,  #  Sequential ID
                    "start_vertex_type": src_label,
                    "end_id": dst_id,  #  Sequential ID
                    "end_vertex_type": dst_label,
                    "start_time": record.get(edge_field_map.get("start_time", "")),
                    "end_time": record.get(edge_field_map.get("end_time", "")),
                }

                # Add properties
                for key, value in record.items():
                    if key not in [edge_field_map.get("oid"),
                                   edge_field_map["source_id"],
                                   edge_field_map["target_id"],
                                   edge_field_map.get("start_time"),
                                   edge_field_map.get("end_time"),
                                   "ts"]:
                        if value is not None:
                            edge_row[key] = str(value) if not isinstance(value, (int, float, bool)) else value

                # Process time series
                ts_block = record.get("ts", {})
                ts_ids = {}
                if ts_block:
                    for variable, measurements in ts_block.items():
                        ts_id = f"ts_{ts_counter}"
                        ts_counter += 1
                        ts_ids[f"{variable}_ts_id"] = ts_id

                        for entry in measurements:
                            timestamp = entry.get("Start") or entry.get("start") or entry.get("time")
                            value = entry.get("Value") or entry.get("value")
                            if timestamp and value is not None:
                                all_measurements.append({
                                    "ts_id": ts_id,
                                    "variable": variable,
                                    "timestamp": timestamp,
                                    "value": float(value)
                                })

                edges_by_label[label].append((src_id, edge_row, ts_ids))

    # Write edges CSV
    logger.info("Writing edges CSV files...")
    for label, edges in edges_by_label.items():
        csv_file = edges_output / f"{label}.csv"
        if not edges:
            continue

        all_keys = set()
        all_keys.update(["start_id", "start_vertex_type", "end_id", "end_vertex_type"])
        for _, edge_row, ts_ids in edges:
            all_keys.update(edge_row.keys())
            all_keys.update(ts_ids.keys())

        fieldnames = ["start_id", "start_vertex_type", "end_id", "end_vertex_type"]
        fieldnames += sorted([k for k in all_keys if k not in fieldnames])

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for _, edge_row, ts_ids in edges:
                complete_row = {**edge_row, **ts_ids}
                writer.writerow(complete_row)

        logger.info(f"  ✓ {csv_file.name}: {len(edges):,} rows")

    # Deduplicate measurements
    logger.info("Processing measurements...")
    measurements_before = len(all_measurements)
    measurements_dict = {
        (m["ts_id"], m["variable"], m["timestamp"]): m
        for m in all_measurements
    }
    deduplicated_measurements = list(measurements_dict.values())
    duplicates_removed = measurements_before - len(deduplicated_measurements)

    if duplicates_removed > 0:
        logger.info(f"  Removed {duplicates_removed:,} duplicate timestamps")

    if deduplicated_measurements:
        csv_file = output_dir / "measurements.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["ts_id", "variable", "timestamp", "value"])
            writer.writeheader()
            deduplicated_measurements.sort(key=lambda x: (x["ts_id"], x["timestamp"]))
            writer.writerows(deduplicated_measurements)
        logger.info(f"  ✓ measurements.csv: {len(deduplicated_measurements):,} rows")

    # Summary
    logger.info("=" * 70)
    logger.info("✓ CONVERSION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"  Nodes: {len(node_id_map):,} (sequential IDs 1-{node_id_counter - 1})")
    logger.info(f"  Edges: {sum(len(edges) for edges in edges_by_label.values()):,}")
    logger.info(f"  Measurements: {len(deduplicated_measurements):,}")
    logger.info("=" * 70)

    return {
        "nodes": len(node_id_map),
        "edges": sum(len(edges) for edges in edges_by_label.values()),
        "measurements": len(deduplicated_measurements),
        "time_series": ts_counter
    }
