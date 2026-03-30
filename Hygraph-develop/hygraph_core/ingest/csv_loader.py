# hygraph_core/ingest/csv_loader.py
"""
Load CSV files into HyGraph with SSE progress updates.
"""
import csv
import json
import time
#import requests
from pathlib import Path
from typing import Optional, Dict, Any

from hygraph_core import AGEStore
from hygraph_core.utils.config import SETTINGS
from hygraph_core.storage.sql import DBPool
from hygraph_core.utils.logging_config import setup_logging

logger = setup_logging(log_file="logs/csv_ingestion.log")

# SSE Progress endpoint
SSE_ENDPOINT = "http://localhost:8000/api/ingestion/progress"


"""def #report_progress(nodes: int = 0, edges: int = 0, timeseries: int = 0, 
                    phase: str = "loading", message: str = None):
    """"""Report ingestion progress to SSE endpoint for real-time frontend updates.""""""
    try:
        requests.post(SSE_ENDPOINT, json={
            'nodes': nodes,
            'edges': edges,
            'timeseries': timeseries,
            'phase': phase,
            'message': message
        }, timeout=1)
    except Exception as e:
        # Don't fail ingestion if SSE reporting fails
        logger.debug(f"SSE progress report failed: {e}")"""


def reset_graph(graph_name: str, db: DBPool):
    """Drop and recreate the AGE graph to clear all data."""
    logger.info(f"Resetting graph '{graph_name}'...")
    ##report_progress(phase="reset", message=f"Resetting graph '{graph_name}'...")

    with db.conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path TO ag_catalog, '$user', public;")

            try:
                cursor.execute(f"SELECT drop_graph('{graph_name}', true);")
                conn.commit()
                logger.info(f"  ✓ Graph '{graph_name}' dropped")
            except Exception as e:
                conn.rollback()
                logger.warning(f"  ℹ Graph drop failed (might not exist): {e}")

            try:
                cursor.execute(f"SELECT create_graph('{graph_name}');")
                conn.commit()
                logger.info(f"  ✓ Graph '{graph_name}' recreated")
            except Exception as e:
                logger.error(f"  ✗ Failed to create graph: {e}")
                raise


def convert_value(value: str, target_type: str) -> Any:
    """Single optimized function to cast values"""
    if value is None or value == "":
        return None

    if target_type == "int":
        return int(value)
    elif target_type == "float":
        return float(value)
    elif target_type == "bool":
        return str(value).lower() in ('true', '1', 'yes')
    elif target_type == "json":
        return json.loads(value)
    else:
        return value


def load_csv(
        csv_dir: Path,
        graph_name: str = "hygraph",
        skip_age: bool = False,
        batch_size=5000,
        schema: Optional[Dict[str, Dict[str, str]]] = None
) -> dict:
    """
    Load CSV files using Apache AGE's native bulk loading with SSE progress updates.
    """
    csv_dir = Path(csv_dir)

    logger.info("=" * 70)
    logger.info("CSV → DATABASE LOADING (WITH SSE PROGRESS)")
    logger.info("=" * 70)
    
    #report_progress(phase="starting", message="Starting CSV ingestion...")

    overall_start = time.time()

    with DBPool(SETTINGS.dsn, SETTINGS.pool_min, SETTINGS.pool_max) as db:
        reset_graph(graph_name, db)
        
        stats = {
            "nodes_total": 0,
            "edges_total": 0,
            "measurements": 0,
        }

        with DBPool(SETTINGS.dsn, SETTINGS.pool_min, SETTINGS.pool_max) as db:
            reset_graph(graph_name, db)
            age = AGEStore(db, graph_name)

            # Load nodes
            logger.info("\nLoading Nodes...")
            nodes_dir = csv_dir / "nodes"

            if nodes_dir.exists():
                for csv_file in sorted(nodes_dir.glob("*.csv")):
                    label = csv_file.stem
                    node_start = time.time()
                    logger.info(f"  → {label}")

                    with open(csv_file, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        rows = list(reader)

                    total_rows = len(rows)
                    
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]
                        age_rows = []

                        for row in batch:
                            props = {k: v for k, v in row.items()
                                     if k not in ['id', 'start_time', 'end_time'] and v}
                            if schema and label in schema:
                                props = {
                                    k: convert_value(v, schema[label].get(k, "str"))
                                    for k, v in props.items()
                                }
                            age_rows.append({
                                "uid": str(row["id"]),
                                "start_time": row.get("start_time") or None,
                                "end_time": row.get("end_time") or None,
                                "props": props
                            })

                        age.upsert_nodes(label, age_rows, batch=batch_size)
                        
                        # Report progress after each batch
                        """current_count = stats["nodes_total"] + min(i + batch_size, total_rows)
                        report_progress(
                            nodes=current_count,
                            edges=stats["edges_total"],
                            timeseries=stats["measurements"],
                            phase="loading",
                            message=f"Loading {label} nodes: {min(i + batch_size, total_rows)}/{total_rows}"
                        )"""

                    node_time = time.time() - node_start
                    logger.info(f"      ✓ {len(rows):,} vertices in {node_time:.2f}s")
                    stats["nodes_total"] += len(rows)

            # Load edges
            logger.info("\nLoading Edges...")
            edges_dir = csv_dir / "edges"

            if edges_dir.exists():
                for csv_file in sorted(edges_dir.glob("*.csv")):
                    label = csv_file.stem
                    edge_start = time.time()
                    logger.info(f"  → {label}")

                    with open(csv_file, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        rows = list(reader)

                    total_rows = len(rows)
                    
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]
                        age_rows = []

                        for row in batch:
                            props = {k: v for k, v in row.items()
                                     if k not in ['start_id', 'start_vertex_type',
                                                  'end_id', 'end_vertex_type',
                                                  'start_time', 'end_time'] and v}

                            uid = f"{label}_{row['start_id']}_{row['end_id']}_{row['start_time']}"
                            if schema and label in schema:
                                props = {
                                    k: convert_value(v, schema[label].get(k, "str"))
                                    for k, v in props.items()
                                }
                            age_rows.append({
                                "uid": uid,
                                "src_uid": str(row["start_id"]),
                                "dst_uid": str(row["end_id"]),
                                "start_time": row.get("start_time") or None,
                                "end_time": row.get("end_time") or None,
                                "props": props
                            })

                        age.upsert_edges(label, age_rows, batch=batch_size)
                        
                        # Report progress after each batch
                        """current_count = stats["edges_total"] + min(i + batch_size, total_rows)
                        report_progress(
                            nodes=stats["nodes_total"],
                            edges=current_count,
                            timeseries=stats["measurements"],
                            phase="loading",
                            message=f"Loading {label} edges: {min(i + batch_size, total_rows)}/{total_rows}"
                        )
"""
                    edge_time = time.time() - edge_start
                    logger.info(f"      ✓ {len(rows):,} edges in {edge_time:.2f}s")
                    stats["edges_total"] += len(rows)

            # Load measurements
            logger.info("\nLoading Measurements...")
            measurements_file = csv_dir / "measurements.csv"

            if measurements_file.exists():
                meas_start = time.time()

                with open(measurements_file, 'r') as f:
                    meas_count = sum(1 for _ in f) - 1

                logger.info(f"  → {meas_count:,} measurements")
                
                """report_progress(
                    nodes=stats["nodes_total"],
                    edges=stats["edges_total"],
                    timeseries=0,
                    phase="loading",
                    message=f"Loading {meas_count:,} timeseries measurements..."
                )"""

                copy_sql = "COPY ts.measurements (entity_uid, variable, ts, value) FROM STDIN WITH CSV HEADER"

                with db.conn() as conn:
                    with conn.cursor() as cur:
                        with open(measurements_file, 'r') as f:
                            with cur.copy(copy_sql) as copy:
                                lines_processed = 0
                                while True:
                                    line = f.readline()
                                    if not line:
                                        break
                                    copy.write(line)
                                    lines_processed += 1
                                    
                                    # Report progress every 100k lines
                                    if lines_processed % 100000 == 0:
                                        """report_progress(
                                            nodes=stats["nodes_total"],
                                            edges=stats["edges_total"],
                                            timeseries=lines_processed,
                                            phase="loading",
                                            message=f"Loading measurements: {lines_processed:,}/{meas_count:,}"
                                        )"""

                    conn.commit()

                meas_time = time.time() - meas_start
                stats["measurements"] = meas_count
                logger.info(f"  ✓ {meas_count:,} measurements in {meas_time:.2f}s")


            overall_time = int(time.time() - overall_start)
            stats["elapsed_seconds"] = overall_time

            logger.info("\n" + "=" * 70)
            logger.info("COMPLETE")
            logger.info("=" * 70)
            logger.info(f" Vertices:     {stats['nodes_total']:,}")
            logger.info(f" Edges:        {stats['edges_total']:,}")
            logger.info(f" Time series:  {stats['measurements']:,}")
            logger.info(f" Total Time:   {overall_time:.2f}s ({overall_time / 60:.1f} min)")
            logger.info("=" * 70)

            # Final progress report
            """ #report_progress(
                nodes=stats["nodes_total"],
                edges=stats["edges_total"],
                timeseries=stats["measurements"],
                phase="complete",
                message=f"Ingestion complete in {overall_time}s"
            )
            
            # Refresh counts endpoint
            try:
                requests.post("http://localhost:8000/api/counts/refresh", timeout=2)
            except:
                pass"""

            return stats
