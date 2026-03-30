from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    dsn: str = os.getenv("HYGRAPH_DSN", "postgresql://postgres:postgres@127.0.0.1:5433/hygraph?connect_timeout=5")
    pool_min: int = int(os.getenv("HYGRAPH_POOL_MIN", "1"))
    pool_max: int = int(os.getenv("HYGRAPH_POOL_MAX", "10"))
    age_graph: str = os.getenv("HYGRAPH_AGE_GRAPH", "hygraph")

SETTINGS = Settings()
