# hygraph_core/ingest/api.py
"""
FastAPI endpoints for HyGraph data ingestion.

Run with:
    python -m hygraph_core.ingest.api

Endpoints:
    POST /api/convert - Convert JSON to CSV
    POST /api/ingest  - Load CSV into HyGraph
"""

from pathlib import Path
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .csv_converter import json_to_csv
from .csv_loader import load_csv

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(
    title="HyGraph Ingestion API",
    version="1.1.0",
    description="Convert JSON to CSV and load into HyGraph database"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# REQUEST MODELS
# ============================================================================

class ConvertRequest(BaseModel):
    """Request to convert JSON files to CSV format."""
    input_dir: str
    output_dir: str
    node_field_map: Optional[Dict[str, str]] = None
    edge_field_map: Optional[Dict[str, str]] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "input_dir": "../inputFiles/json",
                "output_dir": "../inputFiles/csv3",
                "node_field_map": {
                    "oid": "station_id",
                    "start_time": "start",
                    "end_time": "end"
                },
                "edge_field_map": {
                    "source_id": "from",
                    "target_id": "to",
                    "start_time": "start",
                    "end_time": "end"
                }
            }
        }


class IngestRequest(BaseModel):
    """
    Request to ingest CSV files into HyGraph.
    
    Schema defines type conversions for properties:
    - "int": Integer
    - "float": Float/decimal
    - "bool": Boolean
    - "json": JSON object/array
    - "str": String (default)
    """
    csv_dir: str
    graph_name: str = "hygraph"
    batch_size: int = 5000
    skip_age: bool = False
    schema: Optional[Dict[str, Dict[str, str]]] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "csv_dir": "../inputFiles/csv3",
                "graph_name": "hygraph",
                "batch_size": 5000,
                "schema": {
                    "Station": {
                        "capacity": "int",
                        "lat": "float",
                        "lon": "float",
                        "region_id": "int"
                    },
                    "Trip": {
                        "duration": "int",
                        "distance": "float"
                    }
                }
            }
        }


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/", tags=["Health"])
def root():
    """Health check endpoint."""
    return {
        "status": "online",
        "service": "HyGraph Ingestion API",
        "version": "1.1.0",
        "endpoints": [
            "POST /api/convert - Convert JSON to CSV",
            "POST /api/ingest - Load CSV into HyGraph"
        ]
    }


@app.get("/health", tags=["Health"])
def health():
    """Health check."""
    return {"status": "healthy"}


@app.post("/api/convert", tags=["Ingestion"])
def convert_json_to_csv(request: ConvertRequest):
    """
    Convert JSON files to CSV format.
    
    Reads JSON files from input_dir/nodes/ and input_dir/edges/,
    converts them to CSV format in output_dir/.
    
    Field maps allow renaming fields during conversion:
    - node_field_map: Maps HyGraph fields (oid, start_time, end_time) to JSON fields
    - edge_field_map: Maps HyGraph fields (source_id, target_id, start_time, end_time) to JSON fields
    """
    try:
        input_path = Path(request.input_dir)
        output_path = Path(request.output_dir)
        
        if not input_path.exists():
            return {
                "success": False,
                "error": f"Input directory not found: {request.input_dir}"
            }
        
        # Run conversion
        stats = json_to_csv(
            json_dir=input_path,
            output_dir=output_path,
            node_field_map=request.node_field_map,
            edge_field_map=request.edge_field_map
        )
        
        return {
            "success": True,
            "input_dir": request.input_dir,
            "output_dir": request.output_dir,
            "stats": stats
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e)
        }


@app.post("/api/ingest", tags=["Ingestion"])
def ingest_csv_to_hygraph(request: IngestRequest):
    """
    Load CSV files into HyGraph database.
    
    Reads CSV files from csv_dir/nodes/, csv_dir/edges/, and csv_dir/measurements.csv,
    then loads them into the specified graph.
    
    Schema parameter defines type conversions for properties:
    ```json
    {
        "Station": {
            "capacity": "int",
            "lat": "float",
            "lon": "float"
        }
    }
    ```
    
    Supported types: int, float, bool, json, str (default)
    """
    try:
        csv_path = Path(request.csv_dir)
        
        if not csv_path.exists():
            return {
                "success": False,
                "error": f"CSV directory not found: {request.csv_dir}"
            }
        
        # Run ingestion with schema
        stats = load_csv(
            csv_dir=csv_path,
            graph_name=request.graph_name,
            skip_age=request.skip_age,
            batch_size=request.batch_size,
            schema=request.schema
        )
        
        return {
            "success": True,
            "graph_name": request.graph_name,
            "stats": stats,
            "schema_applied": request.schema is not None
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("HYGRAPH INGESTION API v1.1.0")
    print("=" * 60)
    print()
    print("Endpoints:")
    print("  POST /api/convert  - Convert JSON to CSV")
    print("  POST /api/ingest   - Load CSV into HyGraph (with schema)")
    print()
    print("API Server: http://localhost:8001")
    print("API Docs:   http://localhost:8001/docs")
    print()
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=False)
