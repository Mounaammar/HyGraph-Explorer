import uuid
from typing import Iterable, Mapping, Any, Sequence, List, Tuple, Optional, Dict
from datetime import datetime

from hygraph_core.model import TimeSeries
from .sql import DBPool
from hygraph_core.utils.logging_config import setup_logging
from ..model import TimeSeries, TimeSeriesMetadata

logger = setup_logging(log_file="logs/ingestion.log")

MeasurementRow = Sequence[Any]  # (entity_uid, variable, ts, value)

class TSStore:
    """
    TimescaleDB storage for time series data.
    
    Stores only MEASUREMENTS in ts.measurements table (entity_uid, variable, ts, value).
    No separate nodes/edges tables - those are in AGE.
    """
    def __init__(self, db: DBPool):
        self.db = db
    

    
    def copy_measurements(self, rows: Iterable[MeasurementRow]):
        """
        Bulk insert measurements using COPY - fastest method for data loading.
        
        Args:
            rows: List of (entity_uid, variable, timestamp, value) tuples
        """
        if not isinstance(rows, list):
            rows = list(rows)

        if not rows:
            return

        total = len(rows)
        logger.info(f"Copying {total:,} measurements")

        # Deduplicate - keep last value for each (entity_uid, variable, timestamp)
        seen = {}
        for row in rows:
            key = (row[0], row[1], row[2])  # (entity_uid, variable, timestamp)
            seen[key] = row  # Keep last

        rows = list(seen.values())
        removed = total - len(rows)
        if removed > 0:
            logger.info(f"  ⚠  Removed {removed:,} duplicates (kept last values)")

        copy_sql = "COPY ts.measurements(entity_uid, variable, ts, value) FROM STDIN"

        try:
            self.db.copy_rows(copy_sql, rows)
            logger.info(f"✓ Copied {len(rows):,} measurements")
        except Exception as e:
            logger.error(f"✗ COPY failed: {e}")
            raise
    


    def add_timeseries(self, timeseries: TimeSeries, ts_id: Optional[str] = None) -> str:
        """
        Store a timeseries as an orphan (not yet attached to any entity).

        Args:
            timeseries: TimeSeries object with timestamps and data
            ts_id: Optional custom ID, otherwise auto-generated

        Returns:
            The ts_id (entity_uid) of the stored timeseries

        Example:
            ts = TimeSeries.from_results(results, ["node_count"])
            ts_id = ts_store.add_timeseries(ts)
            # Later: node.add_temporal_property("metric", ts_id)
        """
        # Generate ID if not provided
        if ts_id is None:
            ts_id = f"ts_{uuid.uuid4().hex[:12]}"

        # Prepare measurement rows: (entity_uid, variable, ts, value)
        rows = []
        for i, timestamp in enumerate(timeseries.timestamps):
            for j, variable in enumerate(timeseries.variables):
                value = timeseries.data[i][j] if isinstance(timeseries.data[i], list) else timeseries.data[i]
                rows.append((ts_id, variable, timestamp, float(value) if value is not None else 0.0))

        if not rows:
            logger.warning(f"No data to store for timeseries {ts_id}")
            return ts_id

        # Insert measurements
        self.insert_measurements(rows)
        logger.info(f"✓ Stored orphan timeseries {ts_id} with {len(rows)} measurements")

        return ts_id


    def insert_measurements(self, rows: List[Tuple]):
        """
        Insert measurements one by one (slower than copy, but handles conflicts).
        @:param    rows: List of (entity_uid, variable, timestamp, value) tuples
        """
        if not rows:
            return

        sql = """
        INSERT INTO ts.measurements (entity_uid, variable, ts, value)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (entity_uid, variable, ts) DO UPDATE
        SET value = EXCLUDED.value
        """

        self.db.executemany(sql, rows)
        logger.info(f"✓ Inserted {len(rows):,} measurements")
    
    def get_measurements(self, entity_uid: str, variable: str,
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None) -> TimeSeries | None:
        """
        Get measurements for a specific entity and variable.
        
        :return List of (timestamp, value) tuples
        """
        sql = """
        SELECT ts, value 
        FROM ts.measurements 
        WHERE entity_uid = %s AND variable = %s
        """
        
        params = [entity_uid, variable]
        
        if start_time:
            sql += " AND ts >= %s"
            params.append(start_time)
        if end_time:
            sql += " AND ts <= %s"
            params.append(end_time)
        
        sql += " ORDER BY ts"
        
        results = self.db.fetch_all(sql, params)
        if not results:
            return None
        timestamps= [r['ts'] for r in results]
        values=[[r['value']] for r in results]
        return TimeSeries(
            tsid=entity_uid,
            timestamps=timestamps,
            variables=[variable],
            data=values,
            metadata=TimeSeriesMetadata(
                owner_id=entity_uid,
                element_type='',
            )
        )
    
    def get_all_variables(self, entity_uid: str) -> List[str]:
        """Get all variable names for an entity"""
        sql = """
        SELECT DISTINCT variable 
        FROM ts.measurements 
        WHERE entity_uid = %s
        ORDER BY variable
        """
        
        results = self.db.fetch_all(sql, [entity_uid])
        return [r['variable'] for r in results]
    
    def delete_measurements(self, entity_uid: str, variable: Optional[str] = None,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None) -> int:
        """
        Delete measurements.
        
       :return Number of measurements deleted
        """
        sql = "DELETE FROM ts.measurements WHERE entity_uid = %s"
        params = [entity_uid]
        
        if variable:
            sql += " AND variable = %s"
            params.append(variable)
        
        if start_time:
            sql += " AND ts >= %s"
            params.append(start_time)
        if end_time:
            sql += " AND ts <= %s"
            params.append(end_time)
        
        # Execute and get row count
        with self.db.tx() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                count = cur.rowcount
        
        logger.info(f"✓ Deleted {count:,} measurements for {entity_uid}")
        return count
    
    def update_measurement(self, entity_uid: str, variable: str, 
                          timestamp: datetime, value: float) -> None:
        """Update a single measurement"""
        sql = """
        INSERT INTO ts.measurements (entity_uid, variable, ts, value)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (entity_uid, variable, ts) DO UPDATE
        SET value = EXCLUDED.value
        """
        
        self.db.exec(sql, [entity_uid, variable, timestamp, value])
    
    def measurement_exists(self, entity_uid: str, variable: str, 
                          timestamp: datetime) -> bool:
        """Check if a measurement exists"""
        sql = """
        SELECT EXISTS(
            SELECT 1 FROM ts.measurements 
            WHERE entity_uid = %s AND variable = %s AND ts = %s
        ) as exists
        """
        
        results = self.db.fetch_all(sql, [entity_uid, variable, timestamp])
        return results[0]['exists'] if results else False
    

    
    def query_measurements(self, entity_uids: Optional[List[str]] = None,
                          variables: Optional[List[str]] = None,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None,
                          limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Query measurements with filters.
        
        Returns:
            List of dicts with keys: entity_uid, variable, ts, value
        """
        # Batch size to stay under PostgreSQL's 65535 parameter limit
        BATCH_SIZE = 30000
        
        # If entity_uids is large, batch the queries
        if entity_uids and len(entity_uids) > BATCH_SIZE:
            all_results = []
            for i in range(0, len(entity_uids), BATCH_SIZE):
                batch = entity_uids[i:i + BATCH_SIZE]
                batch_results = self.query_measurements(
                    entity_uids=batch,
                    variables=variables,
                    start_time=start_time,
                    end_time=end_time,
                    limit=None  # Don't limit batches, limit final result
                )
                all_results.extend(batch_results)
            # Apply limit to combined results
            if limit:
                all_results = all_results[:limit]
            return all_results
        
        sql = "SELECT entity_uid, variable, ts, value FROM ts.measurements WHERE 1=1"
        params = []
        
        if entity_uids:
            placeholders = ','.join(['%s'] * len(entity_uids))
            sql += f" AND entity_uid IN ({placeholders})"
            params.extend(entity_uids)
        
        if variables:
            placeholders = ','.join(['%s'] * len(variables))
            sql += f" AND variable IN ({placeholders})"
            params.extend(variables)
        
        if start_time:
            sql += " AND ts >= %s"
            params.append(start_time)
        if end_time:
            sql += " AND ts <= %s"
            params.append(end_time)
        
        sql += " ORDER BY entity_uid, variable, ts"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        results = self.db.fetch_all(sql, params if params else None)
        return results
    
    def get_aggregated(self, entity_uid: str, variable: str,
                      start_time: datetime, end_time: datetime,
                      interval: str = '1 hour',
                      agg_func: str = 'avg') -> List[Tuple[datetime, float]]:
        """
        Get aggregated measurements using time_bucket.
        
        Args:
            entity_uid: Entity ID
            variable: Variable name
            start_time: Start time
            end_time: End time
            interval: Time bucket interval (e.g., '1 hour', '15 minutes')
            agg_func: Aggregation function ('avg', 'sum', 'min', 'max', 'count')
        
        Returns:
            List of (timestamp, aggregated_value) tuples
        """
        sql = f"""
        SELECT time_bucket(%s, ts) as bucket, {agg_func}(value) as agg_value
        FROM ts.measurements
        WHERE entity_uid = %s AND variable = %s
          AND ts >= %s AND ts <= %s
        GROUP BY bucket
        ORDER BY bucket
        """
        
        params = [interval, entity_uid, variable, start_time, end_time]
        results = self.db.fetch_all(sql, params)
        
        return [(r['bucket'], r['agg_value']) for r in results]
    

    
    def count_measurements(self, entity_uid: Optional[str] = None,
                          variable: Optional[str] = None) -> int:
        """Count measurements"""
        sql = "SELECT COUNT(*) as cnt FROM ts.measurements WHERE 1=1"
        params = []
        
        if entity_uid:
            sql += " AND entity_uid = %s"
            params.append(entity_uid)
        if variable:
            sql += " AND variable = %s"
            params.append(variable)
        
        results = self.db.fetch_all(sql, params if params else None)
        return results[0]['cnt'] if results else 0
    
    def get_time_range(self, entity_uid: str, variable: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get time range (min, max) for an entity's variable"""
        sql = """
        SELECT MIN(ts) as min_ts, MAX(ts) as max_ts
        FROM ts.measurements
        WHERE entity_uid = %s AND variable = %s
        """
        
        results = self.db.fetch_all(sql, [entity_uid, variable])
        if results:
            return (results[0]['min_ts'], results[0]['max_ts'])
        return (None, None)


    def aggregate_measurements(self, entity_uids: List[str],
                               start_time: datetime,
                               end_time: datetime,
                               aggregation: str = 'avg') -> List[Dict[str, Any]]:
        """
        Get a single aggregated value per entity_uid over a time range.

        Args:
            entity_uids: List of entity UIDs to aggregate
            start_time: Start of time range
            end_time: End of time range
            aggregation: Aggregation function ('avg', 'sum', 'min', 'max', 'count')

        Returns:
            List of dicts with keys: entity_uid, value
            Example: [
                {'entity_uid': 'ts_123', 'value': 12.5},
                {'entity_uid': 'ts_456', 'value': 8.3}
            ]
        """
        if not entity_uids:
            return []

        # Validate aggregation function
        valid_agg = ['avg', 'sum', 'min', 'max', 'count']
        if aggregation not in valid_agg:
            raise ValueError(f"Invalid aggregation: {aggregation}. Must be one of {valid_agg}")

        # Batch size to stay under PostgreSQL's 65535 parameter limit
        BATCH_SIZE = 30000

        # If entity_uids is large, batch the queries
        if len(entity_uids) > BATCH_SIZE:
            all_results = []
            for i in range(0, len(entity_uids), BATCH_SIZE):
                batch = entity_uids[i:i + BATCH_SIZE]
                batch_results = self.aggregate_measurements(
                    entity_uids=batch,
                    start_time=start_time,
                    end_time=end_time,
                    aggregation=aggregation
                )
                all_results.extend(batch_results)
            return all_results

        # Build query
        placeholders = ','.join(['%s'] * len(entity_uids))

        sql = f"""
        SELECT entity_uid, {aggregation}(value) as value
        FROM ts.measurements
        WHERE entity_uid IN ({placeholders})
          AND ts >= %s AND ts <= %s
        GROUP BY entity_uid
        """

        params = entity_uids + [start_time, end_time]
        results = self.db.fetch_all(sql, params)

        return results

    def get_last_values(self, entity_uids: List[str],
                        timestamp: datetime) -> List[Dict[str, Any]]:
        """
        Get the last measurement value before a timestamp for each entity.
        Uses DISTINCT ON for efficiency - returns only one row per entity_uid.

        Args:
            entity_uids: List of entity UIDs
            timestamp: Get last value before this time

        Returns:
            List of dicts with keys: entity_uid, value
        """
        if not entity_uids:
            return []

        # Batch size to stay under PostgreSQL's 65535 parameter limit
        # Each entity_uid is 1 param, plus 1 for timestamp = need to leave room
        BATCH_SIZE = 30000

        # If entity_uids is large, batch the queries
        if len(entity_uids) > BATCH_SIZE:
            all_results = []
            for i in range(0, len(entity_uids), BATCH_SIZE):
                batch = entity_uids[i:i + BATCH_SIZE]
                batch_results = self.get_last_values(
                    entity_uids=batch,
                    timestamp=timestamp
                )
                all_results.extend(batch_results)
            return all_results

        placeholders = ','.join(['%s'] * len(entity_uids))

        sql = f"""
        SELECT DISTINCT ON (entity_uid) 
            entity_uid, value
        FROM ts.measurements
        WHERE entity_uid IN ({placeholders})
          AND ts <= %s
        ORDER BY entity_uid, ts DESC
        """

        params = entity_uids + [timestamp]
        results = self.db.fetch_all(sql, params)

        return results