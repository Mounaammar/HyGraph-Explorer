#!/usr/bin/env python3
"""
Debugging script to test each component of the ingestion pipeline separately.
Run this BEFORE running the full ingestion to identify bottlenecks.
"""
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ingestion_debug.log')
    ]
)
logger = logging.getLogger(__name__)


def test_1_json_parsing(json_file: Path):
    """Test 1: JSON parsing speed"""
    logger.info("=" * 60)
    logger.info("TEST 1: JSON Parsing Speed")
    logger.info("=" * 60)

    start = time.time()
    with open(json_file) as f:
        data = json.load(f)
    elapsed = time.time() - start

    count = len(data) if isinstance(data, list) else 1
    logger.info(f"✓ Parsed {count:,} records in {elapsed:.3f}s ({count / elapsed:.0f} rec/s)")
    logger.info(f"  File size: {json_file.stat().st_size / 1024 / 1024:.2f} MB")

    return data


def test_2_uuid_generation(records: list, field_name: str):
    """Test 2: UUID generation/conversion speed"""
    logger.info("=" * 60)
    logger.info("TEST 2: UUID Generation Speed")
    logger.info("=" * 60)

    from hygraph_core.ingest.hybrid_json_loader import coerce_uuid  # Adjust import

    start = time.time()
    uuids = []
    for rec in records[:1000]:  # Test first 1000
        try:
            uid = coerce_uuid(rec[field_name])
            uuids.append(uid)
        except Exception as e:
            logger.error(f"UUID error for {rec.get(field_name)}: {e}")
            return False
    elapsed = time.time() - start

    logger.info(f"✓ Generated {len(uuids):,} UUIDs in {elapsed:.3f}s ({len(uuids) / elapsed:.0f} uuid/s)")
    return True


def test_3_database_connection():
    """Test 3: Database connection"""
    logger.info("=" * 60)
    logger.info("TEST 3: Database Connection")
    logger.info("=" * 60)

    try:
        from hygraph_core.utils.config import SETTINGS
        from hygraph_core.storage.sql import DBPool

        db = DBPool(SETTINGS.dsn, 1, 5)

        # Test query
        result = db.fetch_all("SELECT version()")
        logger.info(f"✓ Connected to PostgreSQL")
        logger.info(f"  Version: {result[0]['version'][:50]}...")

        # Test AGE
        result = db.fetch_all("SELECT * FROM ag_catalog.ag_graph LIMIT 1")
        logger.info(f"✓ Apache AGE is available")

        # Test tables
        result = db.fetch_all("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname IN ('hg', 'ts')
            ORDER BY schemaname, tablename
        """)
        logger.info(f"✓ Found {len(result)} tables:")
        for row in result:
            logger.info(f"    {row['schemaname']}.{row['tablename']}")

        db.close()
        return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False


def test_4_small_batch_insert(sample_data: list):
    """Test 4: Small batch insert (10 records)"""
    logger.info("=" * 60)
    logger.info("TEST 4: Small Batch Insert (10 records)")
    logger.info("=" * 60)

    try:
        from hygraph_core.utils.config import SETTINGS
        from hygraph_core.storage.sql import DBPool
        from hygraph_core.storage.timescale import TSStore

        db = DBPool(SETTINGS.dsn, 1, 5)
        ts = TSStore(db)

        # Create test nodes
        test_nodes = []
        for i, rec in enumerate(sample_data[:10]):
            test_nodes.append({
                "uid": f"test-{i}",
                "label": "test_node",
                "start_time": datetime.now().isoformat(),
                "end_time": None,
                "props": {"test": True}
            })

        start = time.time()
        ts.upsert_nodes(test_nodes)
        elapsed = time.time() - start

        logger.info(f"✓ Inserted {len(test_nodes)} nodes in {elapsed:.3f}s")

        # Verify
        result = db.fetch_all("SELECT COUNT(*) as n FROM hg.nodes WHERE label = 'test_node'")
        logger.info(f"✓ Verified: {result[0]['n']} test nodes in database")

        # Cleanup
        db.exec("DELETE FROM hg.nodes WHERE label = 'test_node'")
        logger.info(f"✓ Cleaned up test data")

        db.close()
        return True
    except Exception as e:
        logger.error(f"✗ Batch insert failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_5_copy_performance():
    """Test 5: COPY command performance"""
    logger.info("=" * 60)
    logger.info("TEST 5: COPY Command Performance (1000 measurements)")
    logger.info("=" * 60)

    try:
        from hygraph_core.utils.config import SETTINGS
        from hygraph_core.storage.sql import DBPool
        from hygraph_core.storage.timescale import TSStore

        db = DBPool(SETTINGS.dsn, 1, 5)
        ts = TSStore(db)

        # Generate test measurements
        test_measurements = []
        now = datetime.now()
        for i in range(1000):
            test_measurements.append((
                f"test-entity-{i % 10}",
                "test_variable",
                (now.replace(minute=i % 60)).isoformat(),
                float(i)
            ))

        start = time.time()
        ts.copy_measurements(test_measurements)
        elapsed = time.time() - start

        logger.info(f"✓ COPY inserted {len(test_measurements):,} measurements in {elapsed:.3f}s")
        logger.info(f"  Rate: {len(test_measurements) / elapsed:.0f} measurements/sec")

        # Cleanup
        db.exec("DELETE FROM ts.measurements WHERE variable = 'test_variable'")
        logger.info(f"✓ Cleaned up test data")

        db.close()
        return True
    except Exception as e:
        logger.error(f"✗ COPY failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_6_age_performance():
    """Test 6: AGE vertex creation performance"""
    logger.info("=" * 60)
    logger.info("TEST 6: AGE Vertex Creation (100 vertices)")
    logger.info("=" * 60)

    try:
        from hygraph_core.utils.config import SETTINGS
        from hygraph_core.storage.sql import DBPool
        from hygraph_core.storage.age import AGEStore

        db = DBPool(SETTINGS.dsn, 1, 5)
        age = AGEStore(db, "hygraph")

        # Create test vertices
        test_vertices = []
        for i in range(100):
            test_vertices.append({
                "uid": f"test-age-{i}",
                "start_time": datetime.now().isoformat(),
                "end_time": None,
                "props": {"test": True, "index": i}
            })

        start = time.time()
        age.upsert_vertices("test_label", test_vertices, batch=50)
        elapsed = time.time() - start

        logger.info(f"✓ Created {len(test_vertices)} AGE vertices in {elapsed:.3f}s")
        logger.info(f"  Rate: {len(test_vertices) / elapsed:.0f} vertices/sec")

        # Cleanup - Note: Cleanup in AGE requires Cypher
        logger.info(f"⚠️  Manual cleanup required: DELETE nodes with label 'test_label'")

        db.close()
        return True
    except Exception as e:
        logger.error(f"✗ AGE failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    logger.info("🔍 Starting Component Testing")
    logger.info("=" * 60)

    # Get test file from command line or use default
    if len(sys.argv) > 1:
        test_file = Path(sys.argv[1])
    else:
        logger.error("Usage: python test_ingestion.py <path_to_json_file>")
        logger.error("Example: python test_ingestion.py data/nodes/station.json")
        sys.exit(1)

    if not test_file.exists():
        logger.error(f"File not found: {test_file}")
        sys.exit(1)

    results = {}

    # Run tests
    data = test_1_json_parsing(test_file)
    if not data:
        logger.error("✗ JSON parsing failed, aborting")
        sys.exit(1)

    records = data if isinstance(data, list) else [data]

    # Determine field name for OID (adjust as needed)
    if "station_id" in records[0]:
        oid_field = "station_id"
    elif "nodeid" in records[0]:
        oid_field = "nodeid"
    else:
        oid_field = list(records[0].keys())[0]
        logger.warning(f"Guessing OID field: {oid_field}")

    results["uuid"] = test_2_uuid_generation(records, oid_field)
    results["db_conn"] = test_3_database_connection()

    if results["db_conn"]:
        results["batch_insert"] = test_4_small_batch_insert(records)
        results["copy"] = test_5_copy_performance()
        results["age"] = test_6_age_performance()

    # Summary
    logger.info("=" * 60)
    logger.info("📊 TEST SUMMARY")
    logger.info("=" * 60)
    for test, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"{status} - {test}")

    all_passed = all(results.values())
    if all_passed:
        logger.info("=" * 60)
        logger.info("✅ All tests passed! Ready for full ingestion.")
        logger.info("=" * 60)
        sys.exit(0)
    else:
        logger.info("=" * 60)
        logger.error("❌ Some tests failed. Fix issues before full ingestion.")
        logger.info("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
