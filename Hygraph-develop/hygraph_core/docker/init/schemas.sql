-- Core schemas reflecting your HyGraph data model (PGNode/PGEdge + TS)
CREATE SCHEMA IF NOT EXISTS hg;
CREATE SCHEMA IF NOT EXISTS ts;

-- Nodes with validity + JSONB props (static props live here; temporal are in ts.measurements)
CREATE TABLE IF NOT EXISTS hg.nodes (
  uid        varchar(255) PRIMARY KEY,
  label      TEXT NOT NULL,
  start_time TIMESTAMPTZ,
  end_time   TIMESTAMPTZ,
  props      JSONB DEFAULT '{}'::jsonb
);

-- Edges with validity + JSONB props
CREATE TABLE IF NOT EXISTS hg.edges (
  uid        varchar(255) PRIMARY KEY,
  src_uid    varchar(255) NOT NULL REFERENCES hg.nodes(uid) ON DELETE CASCADE,
  dst_uid    varchar(255) NOT NULL REFERENCES hg.nodes(uid) ON DELETE CASCADE,
  label      TEXT NOT NULL,
  start_time TIMESTAMPTZ,
  end_time   TIMESTAMPTZ,
  props      JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_nodes_label ON hg.nodes(label);
CREATE INDEX IF NOT EXISTS idx_edges_label ON hg.edges(label);
CREATE INDEX IF NOT EXISTS idx_edges_src ON hg.edges(src_uid);
CREATE INDEX IF NOT EXISTS idx_edges_dst ON hg.edges(dst_uid);

-- Timescale hypertable for temporal properties of ANY graph element (node/edges)
CREATE TABLE IF NOT EXISTS ts.measurements (
  entity_uid varchar(255) NOT NULL,  -- uid of a node or edges
  variable   TEXT NOT NULL,  -- e.g., 'num_bikes_available'
  ts         TIMESTAMPTZ NOT NULL,
  value      DOUBLE PRECISION NOT NULL,
  PRIMARY KEY(entity_uid, variable, ts)
);

SELECT create_hypertable('ts.measurements', by_range('ts'), if_not_exists => TRUE);

--CREATE INDEX IF NOT EXISTS idx_ts_entity_time ON ts.measurements (entity_uid, ts DESC);
--CREATE INDEX IF NOT EXISTS idx_ts_variable ON ts.measurements (variable);
