-- Install both extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS age;
LOAD 'age';  -- enable AGE in this session
SET search_path = ag_catalog, "$user", public;

SELECT * FROM ag_catalog.create_graph('hygraph');