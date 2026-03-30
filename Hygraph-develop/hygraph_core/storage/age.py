import json
from typing import Iterable, Mapping, Any, Optional, List, Dict, Literal
from .sql import DBPool
import json, re

_SUFFIX_RE = re.compile(r'::\w+$')  # strips ::node, ::edges, ::path
class AGEStore:
    """
    Apache AGE storage for graph structure (nodes and edges).
    """
    def __init__(self, db: DBPool, graph: str = "hygraph"):
        self.db = db
        self.graph = graph

    def cypher(self, query: str, params: Optional[Mapping[str, Any]] = None):
        """Execute Cypher query"""
        dq = chr(36) + chr(36)  # $ for PostgreSQL dollar quoting
        if params is None:
            # No parameters - simple query
            sql = f"SELECT * FROM cypher('{self.graph}', {dq}{query}{dq}) AS (r agtype);"
            rows = self.db.fetch_all(sql, None)
        else:
            # With parameters - pass as second argument
            ag = json.dumps(params)
            sql = f"SELECT * FROM cypher('{self.graph}', {dq}{query}{dq}, %s) AS (r agtype);"
            rows = self.db.fetch_all(sql, (ag,))

        return [{"r": self._ag_to_dict(row["r"])} for row in rows if row.get("r")]

    def cypher_multi(self, query: str, columns: list, params: Optional[Mapping[str, Any]] = None):
        """
        Execute Cypher query with multiple return columns. It returns a List of dicts with each column as a key
        @:param    query: Cypher query
        @:param      columns: List of column names expected in RETURN
       @:param       params: Query parameters

        """
        dq = chr(36) + chr(36)  # PostgreSQL dollar quoting
        # Build column definitions for SQL
        col_defs = ", ".join([f"{col} agtype" for col in columns])

        if params is None:
            sql = f"SELECT * FROM cypher('{self.graph}', {dq}{query}{dq}) AS ({col_defs});"
            rows = self.db.fetch_all(sql, None)
        else:
            ag = json.dumps(params)
            sql = f"SELECT * FROM cypher('{self.graph}', {dq}{query}{dq}, %s) AS ({col_defs});"
            rows = self.db.fetch_all(sql, (ag,))

        results = []
        for row in rows:
            result = {}
            for col in columns:
                val = row.get(col)
                if val:
                    result[col] = self._ag_to_dict(val)
                else:
                    result[col] = None
            results.append(result)

        return results


    def upsert_nodes(self, label: str, rows: Iterable[Mapping[str, Any]], batch: int = 1000):
        """Bulk upsert nodes - for data loading"""
        rows = list(rows)
        if not rows: return
        q = f"""
        UNWIND $rows AS r
        MERGE (n:{label} {{uid: r.uid}})
        SET n.properties = r.props
        SET n.start_time = r.start_time, n.end_time = r.end_time
        RETURN 1
        """
        for i in range(0, len(rows), batch):
            batch_rows = rows[i:i + batch]
            # Convert props dict to JSON string for AGE
            formatted = [
                {
                    "uid": str(r["uid"]),
                    "start_time": r.get("start_time"),
                    "end_time": r.get("end_time"),
                    "props": json.dumps(r.get("props", {}))  # JSON string
                }
                for r in batch_rows
            ]
            self.cypher(q, {"rows": formatted})

    def upsert_edges(self, label: str, rows: Iterable[Mapping[str, Any]], batch: int = 1000):
        """Bulk upsert edges - for data loading"""
        rows = list(rows)
        if not rows: return
        q = f"""
        UNWIND $rows AS r
        MATCH (s {{uid: r.src_uid}}), (t {{uid: r.dst_uid}})
        MERGE (s)-[e:{label} {{uid: r.uid}}]->(t)
        SET e.properties = r.props
        SET e.start_time = r.start_time, e.end_time = r.end_time
        RETURN 1
        """
        for i in range(0, len(rows), batch):
            batch_rows = rows[i:i + batch]
            formatted = [
                {
                    "uid": str(r["uid"]),
                    "src_uid": str(r["src_uid"]),
                    "dst_uid": str(r["dst_uid"]),
                    "start_time": r.get("start_time"),
                    "end_time": r.get("end_time"),
                    "props": json.dumps(r.get("props", {}))  # JSON string
                }
                for r in batch_rows
            ]
            self.cypher(q, {"rows": formatted})


    
    def create_node(self, uid: str, label: str, properties: Dict[str, Any], 
                    start_time: str, end_time: str) -> None:
        """Create a single node"""
        query = f"""
        CREATE (n:{label} {{uid: $uid, start_time: $start_time, end_time: $end_time}})
        SET n.properties = $props
        RETURN n
        """
        params = {
            "uid": uid,
            "label": label,
            "start_time": start_time,
            "end_time": end_time,
            "props": json.dumps(properties)
        }
        self.cypher(query, params)
    
    def get_node(self, uid: str) -> Optional[Dict[str, Any]]:
        """
        Get a node by uid - returns static properties only.
        Time series IDs are in properties, but not the actual time series data.
        """
        query = "MATCH (n {uid: $uid}) RETURN n"
        results = self.cypher(query, {"uid": uid})
        
        if not results or not results[0].get('r'):
            return None
        
        return self._normalize_node(results[0]['r'])
    
    def update_node(self, uid: str, properties: Dict[str, Any]) -> None:
        """Update node static properties"""
        query = """
        MATCH (n {uid: $uid})
        SET n.properties = $props
        RETURN n
        """
        params = {
            "uid": uid,
            "props": json.dumps(properties)
        }
        self.cypher(query, params)
    
    def delete_node(self, uid: str, hard: bool = False) -> None:
        """Delete a node"""
        if hard:
            query = "MATCH (n {uid: $uid}) DETACH DELETE n"
            self.cypher(query, {"uid": uid})
        else:
            # Soft delete - set end_time to now
            from datetime import datetime
            query = "MATCH (n {uid: $uid}) SET n.end_time = $end_time RETURN n"
            self.cypher(query, {"uid": uid, "end_time": datetime.now().isoformat()})
    
    def node_exists(self, uid: str) -> bool:
        """Check if node exists"""
        query = "MATCH (n {uid: $uid}) RETURN count(n) as cnt"
        results = self.cypher(query, {"uid": uid})
        return results and results[0].get('r', 0) > 0
    

    
    def create_edge(self, uid: str, src_uid: str, dst_uid: str, label: str,
                    properties: Dict[str, Any], start_time: str, end_time: str) -> None:
        """Create a single edges"""
        query = f"""
        MATCH (s {{uid: $src_uid}}), (t {{uid: $dst_uid}})
        CREATE (s)-[e:{label} {{uid: $uid, start_time: $start_time, end_time: $end_time}}]->(t)
        SET e.properties = $props
        RETURN e
        """
        params = {
            "uid": uid,
            "src_uid": src_uid,
            "dst_uid": dst_uid,
            "start_time": start_time,
            "end_time": end_time,
            "props": json.dumps(properties)
        }
        self.cypher(query, params)
    
    def get_edge(self, uid: str) -> Optional[Dict[str, Any]]:
        """
        Get an edge by uid - returns static properties only.
        Time series IDs are in properties, but not the actual time series data.
        """
        query = "MATCH (s)-[e {uid: $uid}]->(t) RETURN e, s.uid as src_uid, t.uid as dst_uid"
        results = self.cypher_multi(query, ['e', 'src_uid', 'dst_uid'], {"uid": uid})

        if not results or not results[0].get('e'):
            return None

        # Normalize with actual source/target UIDs
        item = results[0]
        return self._normalize_edge_with_endpoints(
            item['e'],
            item.get('src_uid'),
            item.get('dst_uid')
        )
    
    def update_edge(self, uid: str, properties: Dict[str, Any]) -> None:
        """Update edges static properties"""
        query = """
        MATCH ()-[e {uid: $uid}]->()
        SET e.properties = $props
        RETURN e
        """
        params = {
            "uid": uid,
            "props": json.dumps(properties)
        }
        self.cypher(query, params)
    
    def delete_edge(self, uid: str, hard: bool = False) -> None:
        """Delete an edges"""
        if hard:
            query = "MATCH ()-[e {uid: $uid}]->() DELETE e"
            self.cypher(query, {"uid": uid})
        else:
            from datetime import datetime
            query = "MATCH ()-[e {uid: $uid}]->() SET e.end_time = $end_time RETURN e"
            self.cypher(query, {"uid": uid, "end_time": datetime.now().isoformat()})
    
    def edge_exists(self, uid: str) -> bool:
        """Check if edges exists"""
        query = "MATCH ()-[e {uid: $uid}]->() RETURN count(e) as cnt"
        results = self.cypher(query, {"uid": uid})
        return results and results[0].get('r', 0) > 0
    


    def query_nodes(self, label: Optional[str] = None, at_time: Optional[str] = None, 
                     start_time: Optional[str] = None, end_time: Optional[str] = None,
                     limit: Optional[int] = None):
        """
        Query nodes with optional temporal filtering.
        
        Temporal filtering:
        - at_time: Return nodes valid at this exact point in time
        - start_time + end_time: Return nodes that overlap with this interval
        
        If no temporal filter specified, returns all nodes.
        
        Note: Handles null values in start_time/end_time gracefully:
        - NULL start_time is treated as negative infinity (always started)
        - NULL end_time is treated as positive infinity (still valid)
        """
        # Build cypher
        if label:
            cypher = f"MATCH (n:{label})"
        else:
            cypher = "MATCH (n)"

        where_clauses = []

        if at_time:
            # Point in time query:
            # Node is valid if: start_time <= at_time AND end_time >= at_time
            # Handle nulls: null start = always started, null end = still valid
            where_clauses.append(
                f"(n.start_time IS NULL OR n.start_time <= '{at_time}') AND "
                f"(n.end_time IS NULL OR n.end_time >= '{at_time}')"
            )

        if start_time and end_time:
            # Interval query - entities that overlap with [start_time, end_time]
            # Overlap condition: entity.start <= query.end AND entity.end >= query.start
            where_clauses.append(
                f"(n.start_time IS NULL OR n.start_time <= '{end_time}') AND "
                f"(n.end_time IS NULL OR n.end_time >= '{start_time}')"
            )

        if where_clauses:
            cypher += " WHERE " + " AND ".join(where_clauses)
        cypher += " RETURN n"

        if limit:
            cypher += f" LIMIT {limit}"

        # Execute
        results = self.cypher(cypher)

        # Normalize all nodes
        nodes = []
        for item in results:
            if 'r' in item:
                node_data = item['r']
                normalized = self._normalize_node(node_data)
                if normalized:
                    nodes.append(normalized)

        return nodes
    
    def query_edges(self, label: Optional[str] = None,
                    source: Optional[str] = None,
                    target: Optional[str] = None,
                    at_time: Optional[str] = None,
                    start_time: Optional[str] = None, 
                    end_time: Optional[str] = None,
                    limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Query edges, returns static properties only.
        Time series ids are in properties['temporal_properties'].
        
        Temporal filtering:
        - at_time: Point in time query (edge must be valid at this time)
        - start_time + end_time: Interval query (edge must overlap with interval)
        """
        # Build query that also returns source and target node UIDs
        query = "MATCH "

        if source:
            query += "(s {uid: $src_uid})"
        else:
            query += "(s)"

        query += f"-[e{':' + label if label else ''}]->"

        if target:
            query += "(t {uid: $dst_uid})"
        else:
            query += "(t)"

        params = {}
        if source:
            params['src_uid'] = source
        if target:
            params['dst_uid'] = target

        where_clauses = []
        
        if at_time:
            # Point in time: edge must be valid at this exact time
            # Handle nulls: null start = always started, null end = still valid
            where_clauses.append(
                "(e.start_time IS NULL OR e.start_time <= $at_time) AND "
                "(e.end_time IS NULL OR e.end_time >= $at_time)"
            )
            params['at_time'] = at_time
        elif start_time and end_time:
            # Interval: edge must overlap with the query interval
            # Overlap condition: entity.start <= query.end AND entity.end >= query.start
            where_clauses.append(
                "(e.start_time IS NULL OR e.start_time <= $query_end) AND "
                "(e.end_time IS NULL OR e.end_time >= $query_start)"
            )
            params['query_start'] = start_time
            params['query_end'] = end_time

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        # Return edge AND source/target UIDs
        query += " RETURN e, s.uid as src_uid, t.uid as dst_uid"

        if limit:
            query += f" LIMIT {limit}"

        # Use cypher_multi to get multiple columns
        results = self.cypher_multi(query, ['e', 'src_uid', 'dst_uid'], params if params else None)

        # Normalize all edges with their endpoints
        edges = []
        for item in results:
            edge_data = item.get('e')
            if edge_data:
                normalized = self._normalize_edge_with_endpoints(
                    edge_data,
                    item.get('src_uid'),
                    item.get('dst_uid')
                )
                if normalized:
                    edges.append(normalized)

        return edges
    

    
    def get_neighbors(self, uid: str, direction: Literal["in", "out", "both"] = "out",
                     label: Optional[str] = None) -> List[str]:
        """Get neighbor node UIDs"""
        if direction == 'out':
            query = f"MATCH ({{uid: $uid}})-[{':' + label if label else ''}]->(m) RETURN m.uid as neighbor"
        elif direction == 'in':
            query = f"MATCH ({{uid: $uid}})<-[{':' + label if label else ''}]-(m) RETURN m.uid as neighbor"
        else:  # both
            query = f"MATCH ({{uid: $uid}})-[{':' + label if label else ''}]-(m) RETURN m.uid as neighbor"
        
        results = self.cypher(query, {"uid": uid})
        return [r['neighbor'] for r in results if r.get('neighbor')]
    

    
    def count_nodes(self, label: Optional[str] = None) -> int:
        """Count nodes"""
        query = f"MATCH (n{':' + label if label else ''}) RETURN count(n) as cnt"
        results = self.cypher(query, None)
        return results[0]['r'] if results else 0
    
    def count_edges(self, label: Optional[str] = None) -> int:
        """Count edges"""
        query = f"MATCH ()-[e{':' + label if label else ''}]->() RETURN count(e) as cnt"
        results = self.cypher(query, None)
        return results[0]['r'] if results else 0

        

    def _ag_to_dict(self, s: str):
        s = _SUFFIX_RE.sub('', s.strip())
        return json.loads(s)

    def _extract_metadata_from_properties(self, properties: Dict) -> Dict:
        """
        Extract metadata (uid, start_time, end_time) from AGE properties.
        Returns: {metadata: {...}, clean_properties: {...}}
        """
        if not properties:
            return {'metadata': {}, 'clean_properties': {}}

        # Make a copy to avoid modifying original
        props = dict(properties)

        # Extract metadata fields
        metadata = {}

        # Extract uid (try multiple names)
        uid = props.pop('uid', None) or props.pop('node_id', None) or props.pop('id', None)
        if uid:
            metadata['uid'] = str(uid)

        # Extract temporal fields
        metadata['start_time'] = props.pop('start_time', None)
        metadata['end_time'] = props.pop('end_time', None)

        # Handle nested properties string
        if 'properties' in props:
            nested = props.pop('properties')
            if isinstance(nested, str):
                try:
                    nested_dict = json.loads(nested)
                    # Merge nested properties into main properties
                    props.update(nested_dict)
                except:
                    props['properties'] = nested

        return {
            'metadata': metadata,
            'clean_properties': props
        }

    def _normalize_node(self, node_data: Dict) -> Optional[Dict]:
        """
        Normalize node from AGE to clean structure.

        Input (from AGE):
            {
              "id": 123,
              "label": "Station",
              "properties": {
                "uid": "s1",
                "start_time": "...",
                "end_time": "...",
                "capacity": 50,
                "num_bikes_available_ts_id": "ts_3",  // temporal property
                "properties": "{...}"  // nested
              }
            }

        Output (clean):
            {
              "uid": "s1",
              "label": "Station",
              "start_time": "...",
              "end_time": "...",
              "properties": {
                "capacity": 50
              },
              "temporal_properties": {
                "num_bikes_available": "ts_3"  // extracted from _ts_id
              }
            }
        """
        if not node_data:
            return None

        # Get properties
        properties = node_data.get('properties', {})

        # Handle properties as string
        if isinstance(properties, str):
            try:
                properties = json.loads(properties)
            except:
                properties = {}

        # Extract metadata from properties
        extracted = self._extract_metadata_from_properties(properties)
        metadata = extracted['metadata']
        clean_props = extracted['clean_properties']


        static_props = {}
        temporal_props = {}
        
        for key, value in clean_props.items():
            if key.endswith('_ts_id'):
                # Extract variable name: "num_bikes_available_ts_id" → "num_bikes_available"
                var_name = key[:-6]  # Remove '_ts_id' suffix
                temporal_props[var_name] = value
            else:
                static_props[key] = value

        # Build result - put temporal_properties INSIDE properties for compatibility
        # with HybridCRUD which expects: properties['temporal_properties']
        static_props['temporal_properties'] = temporal_props
        
        result = {
            'uid': metadata.get('uid') or str(node_data.get('id')),
            'label': node_data.get('label'),
            'start_time': metadata.get('start_time'),
            'end_time': metadata.get('end_time'),
            'properties': static_props  #
        }

        return result


    def _normalize_edge_with_endpoints(self, edge_data: Dict, src_uid: Any, dst_uid: Any) -> Optional[Dict]:
        """
        Normalize edge from AGE with explicit source/target node UIDs.
        """
        if not edge_data:
            return None

        # Get properties
        properties = edge_data.get('properties', {})

        # Handle properties as string
        if isinstance(properties, str):
            try:
                properties = json.loads(properties)
            except:
                properties = {}

        # Extract metadata from properties
        extracted = self._extract_metadata_from_properties(properties)
        metadata = extracted['metadata']
        clean_props = extracted['clean_properties']

        # Remove any stale src_uid/dst_uid from properties
        clean_props.pop('src_uid', None)
        clean_props.pop('dst_uid', None)

        static_props = {}
        temporal_props = {}

        for key, value in clean_props.items():
            if key.endswith('_ts_id'):
                var_name = key[:-6]  # Remove '_ts_id' suffix
                temporal_props[var_name] = value
            else:
                static_props[key] = value

        # Build result with temporal_properties inside properties
        static_props['temporal_properties'] = temporal_props

        result = {
            'uid': metadata.get('uid') or str(edge_data.get('id')),
            'label': edge_data.get('label'),
            'src_uid': str(src_uid) if src_uid else None,
            'dst_uid': str(dst_uid) if dst_uid else None,
            'start_time': metadata.get('start_time'),
            'end_time': metadata.get('end_time'),
            'properties': static_props
        }

        return result