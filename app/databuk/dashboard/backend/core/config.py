import os
from pathlib import Path
from typing import Optional

CURRENT_DIR = Path(__file__).resolve().parent.parent  # points to backend/


class Settings:
    """Backend configuration settings."""
    
    # Base paths - Fix the path calculation
    # Backend is in: app/databuk/dashboard/backend/
    # Need to go up to: zarr_fuse/ (project root)
    PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent
    TEST_STORES_DIR = PROJECT_ROOT / "zarr_fuse" / "test" / "workdir"
    LOGS_DIR = PROJECT_ROOT / "log_store.zarr" / "logs"
    
    # API settings
    API_V1_STR: str = "/api"
    PROJECT_NAME: str = "ZARR FUSE Dashboard API"
    VERSION: str = "1.0.0"
    
    # CORS settings
    BACKEND_CORS_ORIGINS: list[str] = [
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative dev port
    ]

def has_test_endpoints() -> bool:
    """Check if test endpoints exist in config - derived from endpoints config file"""
    try:
        from .config_manager import load_endpoints
        endpoints = load_endpoints()
        return any('test' in name.lower() for name in endpoints.keys())
    except Exception:
        return False

settings = Settings()
