import json
import sys

from hygraph_core.hygraph.hygraph import HyGraph

print("\n[STEP 3] Checking AGE database...")

hg = HyGraph('postgresql://postgres:postgres@localhost:5433/hygraph', 'hygraph')

# Query AGE directly with cypher
query = "MATCH (n) RETURN n LIMIT 1"
results = hg.age.cypher(query)

if not results:
    print("❌ ERROR: No nodes in AGE database!")
    print("The ingestion did not load data into AGE")
    hg.close()
    sys.exit(1)

print("✓ AGE has nodes")

# Check raw AGE data
raw_node = results[0]['r']
print(f"\n3.1) Raw AGE node structure:")
print(f"  Keys: {list(raw_node.keys())}")

if 'properties' in raw_node:
    props = raw_node['properties']

    # Parse if it's a JSON string
    if isinstance(props, str):
        print(f"  Properties is a JSON string, parsing...")
        props = json.loads(props)

    print(f"  Properties keys: {list(props.keys())}")

    # Check for _ts_id properties
    ts_id_props = [k for k in props.keys() if k.endswith('_ts_id')]
    if ts_id_props:
        print(f"  ✓ Found {len(ts_id_props)} temporal properties in AGE:")
        for prop in ts_id_props:
            print(f"    - {prop} = {props[prop]}")
    else:
        print(f"  ❌ NO properties ending with '_ts_id' in AGE!")
        print(f"  This means csv_loader.py didn't load temporal properties")
else:
    print(f"  ❌ NO 'properties' field in AGE node!")

# ============================================================================
# STEP 4: Check normalized query result
# ============================================================================
print("\n[STEP 4] Checking normalized query result...")

nodes = hg.age.query_nodes(limit=1)
if not nodes:
    print("❌ ERROR: query_nodes() returned no nodes!")
    hg.close()
    sys.exit(1)

node = nodes[0]
print(f"\n4.1) Normalized node structure:")
print(f"  UID: {node.get('uid')}")
print(f"  Label: {node.get('label')}")
print(f"  Static properties: {list(node.get('properties', {}).keys())}")
print(f"  Temporal properties: {list(node.get('temporal_properties', {}).keys())}")

if node.get('temporal_properties'):
    print(f"  ✓ Temporal properties exist after normalization!")
    for key, value in node['temporal_properties'].items():
        print(f"    - {key} → {value}")
else:
    print(f"  ❌ Temporal properties STILL empty after normalization!")

    # Check if _ts_id properties are in static properties
    static_props = node.get('properties', {})
    ts_id_in_static = [k for k in static_props.keys() if k.endswith('_ts_id')]
    if ts_id_in_static:
        print(f"  ⚠️  Found _ts_id in static properties (normalization failed):")
        for prop in ts_id_in_static:
            print(f"    - {prop}")
    else:
        print(f"  ❌ NO _ts_id properties anywhere!")

hg.close()

print("\n" + "=" * 70)
print("DIAGNOSTIC COMPLETE")
print("=" * 70)