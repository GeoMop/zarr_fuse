#!/usr/bin/env python3
"""
Simple script to run the ZARR FUSE Dashboard backend.
"""

import uvicorn
import sys
import os
from pathlib import Path



# Add the backend directory to Python path
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))


from dotenv import load_dotenv
load_dotenv("dev_env", override=False)

if __name__ == "__main__":
    print(f"Starting ZARR FUSE Dashboard Backend")
    print(f"Backend directory: {backend_dir}")
    print(f"Server will be available at: http://localhost:8000")
    print(f"API documentation: http://localhost:8000/docs")
    print(f"Health check: http://localhost:8000/health")
    print()
    
    # Run the server
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        reload_dirs=[str(backend_dir)]
    )
