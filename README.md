# HyGraph

HyGraph combines graphs (Apache AGE) with time series data (TimescaleDB) in a single system.

## Prerequisites

- Docker Desktop
- Python 3.11+
- Node.js 18+

## Setup

### 1. Start the database

```bash
cd hygraph_core/docker
docker compose up -d
```


### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Ingest data

Two datasets are available: METR-LA (traffic) and NYC Citi Bike.

**METR-LA:** Download `METR-LA.h5`, `adj_METR-LA.pkl`, and `graph_sensor_locations.csv` from the [DCRNN repository](https://github.com/liyaguang/DCRNN). Place them in `hygraph_core/inputFiles/metr-la/`.

```python
# Convert raw files to CSV
from hygraph_core.ingest.metrla_loader import convert_metrla
convert_metrla()

# Load into HyGraph
from hygraph_core.ingest.csv_loader import load_csv
load_csv(csv_dir="inputFiles/metrla_csv", graph_name="hygraph")
```

**NYC Citi Bike:** Download it from : https://zenodo.org/records/13846868. Place json files in `hygraph_core/inputFiles/citibike/`, then load:

```python
from hygraph_core.ingest.csv_loader import load_csv
load_csv(csv_dir="inputFiles/citibike_csv", graph_name="hygraph")
```

### 4. Start the backend

```bash
cd hygraph_core
python web_api.py
```

API at `http://localhost:8000`, docs at `http://localhost:8000/docs`.

### 5. Start the frontend

```bash
cd hygraph-webapp
npm install
npm start
```

Opens at `http://localhost:3000`.

## Project Structure

```
hygraph_core/
  docker/           Database setup (PostgreSQL + AGE + TimescaleDB)
  hygraph/          Core HyGraph class
  model/            Data model (nodes, edges, time series)
  operators/        SubHyGraph, TSGen, HyGraphDiff, CPG
  storage/          AGE, TimescaleDB, hybrid storage
  ingest/           Data loaders (METR-LA, CSV)
  web_api.py        FastAPI backend

hygraph-webapp/src/
  App.jsx                    Main application
  components/
    GraphView.jsx            Map and HyGraph
    WorkflowBuilder.jsx      Operator pipeline
    InducedExplorer.jsx      Predicate-induced explorer
    PatternMatchingPanel.jsx Query builder
    TimeSeriesPanel.jsx      Time series charts
    MetadataPanel.jsx        Property panel
```

## Datasets

**METR-LA**: 207 freeway sensors in Los Angeles, speed readings every 5 minutes (March-June 2012). 1,515 road segment edges.

**NYC Citi Bike**: 2,213 stations, 5,626 trip edges. Multiple time series per node and edge.
