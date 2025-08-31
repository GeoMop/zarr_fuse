import os
from pathlib import Path
from typing import Optional

class Settings:
    """Backend configuration settings."""
    
    # Base paths - Fix the path calculation
    # Backend is in: app/databuk/dashboard/backend/
    # Need to go up to: zarr_fuse/ (project root)
    PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent
    TEST_STORES_DIR = PROJECT_ROOT / "zarr_fuse" / "test" / "workdir"
    
    # Zarr store paths
    STRUCTURE_TREE_STORE = TEST_STORES_DIR / "structure_tree.zarr"
    
    # API settings
    API_V1_STR: str = "/api"
    PROJECT_NAME: str = "ZARR FUSE Dashboard API"
    VERSION: str = "1.0.0"
    
    # CORS settings
    BACKEND_CORS_ORIGINS: list[str] = [
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative dev port
    ]
    
    # Feature flags
    ENABLE_TEST_STORES: bool = True
    ENABLE_WEATHER_DATA: bool = False  # Will be enabled later
    
    @classmethod
    def get_store_path(cls, store_name: str) -> Optional[Path]:
        """Get the full path to a Zarr store by name."""
        if store_name == "structure_tree":
            return cls.STRUCTURE_TREE_STORE
        # Add more stores here as needed
        return None

settings = Settings()
