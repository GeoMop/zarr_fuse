from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pathlib import Path
import uvicorn
import logging
# logging.basicConfig(
#         level=logging.DEBUG,  # most detailed normal level
#     )
print("DEBUG: Just to test the logging works.", Path.cwd())


# Use absolute imports
from core.config import settings
from core.config_manager import load_endpoints
from routers import config, s3, logs
from routers.factory import ServiceAPI

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    logger.info(f"Starting {settings.PROJECT_NAME} v{settings.VERSION}")
    logger.info(f"Test stores directory: {settings.TEST_STORES_DIR}")
    
    # Load and display endpoints
    try:
        endpoints = load_endpoints()
        logger.info(f"Loaded {len(endpoints)} endpoint(s): {list(endpoints.keys())}")
    except Exception as e:
        logger.error(f"Failed to load endpoints: {e}")
    
    yield
    
    # Shutdown
    logger.info(f"Shutting down {settings.PROJECT_NAME}")

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

# ============================================================================
# Dynamic Router Loading - Multiple Endpoints Support
# ============================================================================

# Global registry for ServiceAPI instances (for health checks)
service_api_instances = {}

# Load all endpoints from configuration
try:
    endpoints = load_endpoints()
    logger.info(f"Loading {len(endpoints)} endpoint(s) from configuration")
    
    # Create ServiceAPI instance for each endpoint and register router
    for endpoint_name, endpoint_config in endpoints.items():
        service_api = ServiceAPI(name=endpoint_name, cfg=endpoint_config)
        service_api_instances[endpoint_name] = service_api  # Store for health checks
        app.include_router(service_api.router, prefix=settings.API_V1_STR)
        logger.info(f"Registered endpoint: {endpoint_name} -> {settings.API_V1_STR}/{endpoint_name}")
    
except Exception as e:
    logger.error(f"Failed to load endpoints: {e}")
    logger.warning("No dynamic endpoints loaded - only static routes available")

# ============================================================================
# Static Routers (Config, Logs)
# ============================================================================

app.include_router(config.router, prefix=settings.API_V1_STR)
app.include_router(logs.router, prefix=settings.API_V1_STR)

# Legacy S3 router - kept for backward compatibility
# TODO: Remove after frontend migration to new endpoint-specific routes
app.include_router(s3.router, prefix=settings.API_V1_STR)

@app.get("/")
async def root():
    """
    Root endpoint with essential API information and available endpoints.
    
    Returns:
        message: Welcome message with service name
        docs: Link to API documentation
        endpoints: List of available endpoints
    """
    try:
        endpoints = load_endpoints()
        endpoint_list = [
            {
                "name": name,
                "url": f"{settings.API_V1_STR}/{name}",
                "description": cfg.description
            }
            for name, cfg in endpoints.items()
        ]
    except Exception as e:
        logger.error(f"Failed to load endpoints for root: {e}")
        endpoint_list = []
    
    return {
        "message": f"Welcome to {settings.PROJECT_NAME}",
        "version": settings.VERSION,
        "docs": "/docs",
        "endpoints": endpoint_list
    }

@app.get("/health")
async def health_check():
    """
    Health check endpoint with S3 connection status per endpoint.
    
    Returns detailed status for all configured endpoints including:
    - S3 connection status
    - Cache statistics
    - Configuration details
    """
    try:
        endpoints = load_endpoints()
        
        endpoint_status = []
        for name, cfg in endpoints.items():
            # Get ServiceAPI instance if available
            service_instance = service_api_instances.get(name)
            
            if service_instance:
                # Get S3 connection status
                s3_connected = service_instance.s3_service._fs is not None
                cache_size = len(service_instance.state["cache"])
                hits = service_instance.state["hits"]
            else:
                s3_connected = False
                cache_size = 0
                hits = 0
            
            endpoint_status.append({
                "name": name,
                "store_url": cfg.store_url,
                "description": cfg.description,
                "s3_connected": s3_connected,
                "cache_size": cache_size,
                "total_hits": hits
            })
        
        # Overall health: healthy if at least config loaded
        overall_status = "healthy" if len(endpoints) > 0 else "unhealthy"
        
        return {
            "status": overall_status,
            "service": settings.PROJECT_NAME,
            "version": settings.VERSION,
            "endpoints_count": len(endpoints),
            "endpoints": endpoint_status
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "service": settings.PROJECT_NAME,
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
