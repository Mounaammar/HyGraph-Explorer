# hygraph_core/ingest/__init__.py
"""
HyGraph Ingestion Module

Core functions:
- json_to_csv: Convert JSON files to CSV format
- load_csv: Load CSV files into HyGraph database

API:
- ingestion_app: FastAPI app with /api/convert and /api/ingest endpoints

Run API:
    python -m hygraph_core.ingest.api
    
Direct usage:
    from hygraph_core.ingest import json_to_csv, load_csv
    
    # With schema for type conversion
    schema = {
        "Station": {"capacity": "int", "lat": "float", "lon": "float"},
        "Trip": {"duration": "int", "distance": "float"}
    }
    
    stats = load_csv(
        csv_dir=Path("./csv"),
        graph_name="hygraph",
        schema=schema
    )
"""

from .csv_converter import json_to_csv
from .csv_loader import load_csv, reset_graph

__all__ = [
    'json_to_csv',
    'load_csv',
    'reset_graph',
]
