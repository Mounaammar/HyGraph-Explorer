from contextlib import contextmanager
from typing import Sequence, Any, Optional
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from hygraph_core.utils.logging_config import setup_logging

logger = setup_logging(log_file="logs/ingestion.log")
class DBPool:
    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 10):

        self.pool = ConnectionPool(conninfo=dsn, min_size=min_size, max_size=max_size,
                                   kwargs={"row_factory": dict_row}, configure=self._configure_connection)

    def _configure_connection(self, conn):
        """
        """
        try:
            # Load AGE extension
            conn.execute("LOAD 'age';")
            conn.execute("SET search_path = ag_catalog, '$user', public;")
            conn.execute("SET statement_timeout = '600s';")
            conn.commit()
            logger.debug("Connection configured with AGE loaded")
        except Exception as e:
            conn.rollback()
            logger.warning(f"Connection config warning: {e}")

    @contextmanager
    def conn(self):
        """ Get connection WITHOUT automatic transaction"""
        with self.pool.connection() as c:
            yield c

    @contextmanager
    def tx(self):
        """ Get connection WITH transaction"""
        with self.pool.connection() as c:
            with c.transaction():
                yield c

    """def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        with self.conn() as c:
            c.execute(sql, params or ())"""

    def fetch_all(self, sql: str, params: Optional[Sequence[Any]] = None):
        with self.conn() as c:
            cur = c.execute(sql, params or ())
            return cur.fetchall()

    def copy_rows(self, copy_sql: str, rows):

        with self.tx() as c:
            with c.cursor() as cur:
                with cur.copy(copy_sql) as cp:
                    for r in rows:

                        # r must be a 4-tuple/list: (entity_uid, variable, ts, value)
                        if not isinstance(r, (tuple, list)) or len(r) != 4:
                            raise TypeError(f"COPY expects a tuple/list of 4 columns, got: {r!r}")
                        cp.write_row(r)

    def close(self):
        self.pool.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        with self.conn() as c:
            with c.cursor() as cur:
                cur.execute(sql, params or ())

    def executemany(self, sql: str, params_list: Sequence[Sequence[Any]]) -> None:
        """Execute same SQL statement for multiple parameter sets"""
        with self.tx() as c:
            with c.cursor() as cur:
                cur.executemany(sql, params_list)