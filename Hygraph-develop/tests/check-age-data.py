"""
Check what properties are actually stored in AGE database
"""
import sys

sys.path.insert(0, '/')

from hygraph_core.hygraph.hygraph import HyGraph

print("Connecting to database...")
hg = HyGraph('postgresql://postgres:postgres@localhost:5433/hygraph', 'hygraph')

print("\n" + "=" * 70)
print("CHECKING RAW AGE DATA")
print("=" * 70)

# Query AGE directly using cypher
query = "MATCH (n) RETURN n LIMIT 1"
results = hg.age.cypher(query)

if results:
    raw_node = results[0]['r']
    print("\n1. RAW NODE FROM AGE:")
    import json

    print(json.dumps(raw_node, indent=2))

    print("\n2. PROPERTIES FIELD:")
    if 'properties' in raw_node:
        props = raw_node['properties']
        print(f"Type: {type(props)}")
        if isinstance(props, str):
            print("Properties is a JSON string, parsing...")
            props = json.loads(props)
        print(json.dumps(props, indent=2))

        # Check for _ts_id properties
        ts_props = [k for k in props.keys() if k.endswith('_ts_id')]
        print(f"\n3. PROPERTIES ENDING IN '_ts_id': {ts_props}")
    else:
        print("No 'properties' field found!")
else:
    print("No nodes found in database!")

print("\n" + "=" * 70)
print("CHECKING NORMALIZED NODE")
print("=" * 70)

# Now check what the normalization returns
nodes = hg.age.query_nodes(limit=1)
if nodes:
    node = nodes[0]
    print("\n4. NORMALIZED NODE:")
    print(json.dumps(node, indent=2))

    print("\n5. PROPERTIES IN NORMALIZED NODE:")
    print(f"  properties: {list(node.get('properties', {}).keys())}")
    print(f"  temporal_properties: {list(node.get('temporal_properties', {}).keys())}")
else:
    print("No nodes returned from query_nodes()!")

hg.close()