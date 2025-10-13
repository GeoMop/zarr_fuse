from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pathlib import Path
import uvicorn

# Use absolute imports
from core.config import settings
from routers import config, s3
from routers import logs

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
app.include_router(logs.router, prefix=settings.API_V1_STR)

@app.get("/")
async def root():
    """
    Root endpoint with essential API information.
    
    Returns:
        message: Welcome message with service name
        docs: Link to API documentation
    """
    return {
        "message": f"Welcome to {settings.PROJECT_NAME}",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint with S3 connectivity check."""
    try:
        # Check S3 connection status
        from services.s3_service import s3_service
        if s3_service._fs is None:
            return {
                "status": "unhealthy", 
                "service": settings.PROJECT_NAME, 
                "s3": "disconnected"
            }
        
        # Test S3 connectivity with a simple operation
        # Check if we can access the configured endpoint
        from core.config_manager import get_first_endpoint
        endpoint = get_first_endpoint()
        if not endpoint:
            return {
                "status": "unhealthy", 
                "service": settings.PROJECT_NAME, 
                "s3": "no_endpoint_config"
            }
        
        return {
            "status": "healthy", 
            "service": settings.PROJECT_NAME, 
            "s3": "connected",
            "endpoint": endpoint.store_url
        }
    except Exception as e:
        return {
            "status": "unhealthy", 
            "service": settings.PROJECT_NAME, 
            "s3": "error", 
            "error": str(e)
        }

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
