from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Use absolute imports
from core.config import settings
from routers import tree

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    print(f"🚀 Starting {settings.PROJECT_NAME} v{settings.VERSION}")
    print(f"📁 Test stores directory: {settings.TEST_STORES_DIR}")
    print(f"🌳 Structure tree store: {settings.STRUCTURE_TREE_STORE}")
    
    yield
    
    # Shutdown
    print(f"🛑 Shutting down {settings.PROJECT_NAME}")

# Create FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="API for exploring Zarr stores and building tree structures",
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
app.include_router(tree.router, prefix=settings.API_V1_STR)

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
