from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables from .env file in root directory
env_path = Path("C:/Users/fatih/Documents/GitHub/zarr_fuse/.env")
load_dotenv(env_path)

# Debug: Check if environment variables are loaded
print(f"Environment variables loaded:")
print(f"   S3_ACCESS_KEY: {'Found' if os.getenv('S3_ACCESS_KEY') else 'Not found'}")
print(f"   S3_SECRET_KEY: {'Found' if os.getenv('S3_SECRET_KEY') else 'Not found'}")
print(f"   S3_BUCKET_NAME: {'Found' if os.getenv('S3_BUCKET_NAME') else 'Not found'}")
print(f"   .env path: {env_path}")

# Use absolute imports
from core.config import settings
from routers import config, s3

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    print(f"Starting {settings.PROJECT_NAME} v{settings.VERSION}")
    print(f"Test stores directory: {settings.TEST_STORES_DIR}")
    
    yield
    
    # Shutdown
    print(f"Shutting down {settings.PROJECT_NAME}")

# Create FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="API for exploring Zarr stores",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(config.router, prefix=settings.API_V1_STR)
app.include_router(s3.router, prefix=settings.API_V1_STR)

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": f"Welcome to {settings.PROJECT_NAME}",
        "version": settings.VERSION,
        "docs": "/docs",
        "redoc": "/redoc",
        "api_prefix": settings.API_V1_STR
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": settings.PROJECT_NAME}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
