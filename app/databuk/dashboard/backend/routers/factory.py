"""
ServiceAPI Factory Pattern

This module provides a factory pattern for creating API routers per endpoint.
Each ServiceAPI instance encapsulates:
- Endpoint-specific configuration
- State management (hits, cache)
- S3Service instance (per-endpoint isolation)
- Cache management with TTL
- Standard endpoints: /status, /config, /touch, /structure, /cache/*

Usage:
    from routers.factory import ServiceAPI
    
    service = ServiceAPI(name="my_endpoint", cfg=endpoint_config)
    app.include_router(service.router, prefix="/api/v1")
"""

import time
import logging
from typing import Any, Dict
from fastapi import APIRouter, HTTPException
from services.s3_service import S3Service
from core.config_manager import EndpointConfig

logger = logging.getLogger(__name__)


class ServiceAPI:
    """
    Factory class for creating per-endpoint API routers.
    
    Each instance manages its own:
    - Configuration (endpoint-specific)
    - State (hits counter, cache)
    - S3 connection (isolated per endpoint)
    - Cache (in-memory with TTL)
    
    Attributes:
        name: Endpoint name (used as URL prefix)
        cfg: Endpoint configuration (from endpoints.yaml)
        state: Internal state (hits, cache, statistics)
        s3_service: S3Service instance for this endpoint
    """
    
    def __init__(self, name: str, cfg: EndpointConfig):
        """
        Initialize ServiceAPI instance.
        
        Args:
            name: Endpoint name (e.g., "test_s3_endpoint")
            cfg: Endpoint configuration object
        """
        self.name = name
        self.cfg = cfg
        self.state = {
            "hits": 0,
            "cache": {},
            "cache_timestamp": {},
            "cache_stats": {
                "hits": 0,
                "misses": 0
            }
        }
        
        # Each endpoint has its own S3Service instance
        self.s3_service = S3Service()
        
        # Create FastAPI router with endpoint-specific prefix
        self._router = APIRouter(prefix=f"/{name}", tags=[name])
        
        # Register routes (self is captured in closures)
        self._router.add_api_route("/status", self.status, methods=["GET"])
        self._router.add_api_route("/config", self.get_config, methods=["GET"])
        self._router.add_api_route("/touch", self.touch, methods=["POST"])
        self._router.add_api_route("/structure", self.get_structure, methods=["GET"])
        self._router.add_api_route("/cache/stats", self.get_cache_stats, methods=["GET"])
        self._router.add_api_route("/cache/clear", self.clear_cache, methods=["POST"])
        
        logger.info(f"ServiceAPI initialized for endpoint: {name}")
    
    @property
    def router(self) -> APIRouter:
        """Get the FastAPI router for this service."""
        return self._router
    
    # ========================================================================
    # Basic Endpoints
    # ========================================================================
    
    async def status(self) -> Dict[str, Any]:
        """
        Get service status.
        
        Returns health information including:
        - Service name
        - Total hits (request counter)
        - S3 connection status
        - Cache size
        
        Returns:
            Dict with status information
        """
        is_connected = self.s3_service._fs is not None
        
        return {
            "service": self.name,
            "hits": self.state["hits"],
            "s3_connected": is_connected,
            "cache_size": len(self.state["cache"]),
            "description": self.cfg.description
        }
    
    async def get_config(self) -> Dict[str, Any]:
        """
        Get endpoint configuration.
        
        Returns the full endpoint configuration including:
        - store_url
        - reload_interval
        - schema_file
        - description
        - etc.
        
        Returns:
            Dict with endpoint configuration
        """
        return self.cfg.dict()
    
    async def touch(self) -> Dict[str, Any]:
        """
        Touch endpoint (increment hit counter).
        
        Used for testing and monitoring. Each call increments
        the hit counter for this endpoint.
        
        Returns:
            Dict with success status and current hit count
        """
        self.state["hits"] += 1
        logger.info(f"[{self.name}] Touch endpoint hit #{self.state['hits']}")
        
        return {
            "ok": True,
            "hits": self.state["hits"],
            "service": self.name
        }
    
    # ========================================================================
    # S3 Operations with Cache
    # ========================================================================
    
    async def get_structure(self) -> Dict[str, Any]:
        """
        Get Zarr store structure with caching.
        
        This endpoint:
        1. Checks cache first (if valid)
        2. On cache miss, fetches from S3
        3. Updates cache with TTL
        4. Returns structure
        
        Cache TTL is based on cfg.reload_interval.
        
        Returns:
            Dict with Zarr store structure
            
        Raises:
            HTTPException: If S3 connection fails
        """
        cache_key = "structure"
        
        # Check cache validity
        if cache_key in self.state["cache"]:
            cache_age = time.time() - self.state["cache_timestamp"][cache_key]
            
            if cache_age < self.cfg.reload_interval:
                # Cache hit
                self.state["cache_stats"]["hits"] += 1
                logger.info(
                    f"[{self.name}] Cache HIT for structure "
                    f"(age: {cache_age:.1f}s / {self.cfg.reload_interval}s)"
                )
                return self.state["cache"][cache_key]
        
        # Cache miss - fetch from S3
        self.state["cache_stats"]["misses"] += 1
        logger.info(f"[{self.name}] Cache MISS for structure - fetching from S3")
        
        try:
            # Ensure S3 connection with correct config
            logger.info(f"[{self.name}] Connecting to S3: {self.cfg.store_url}")
            success = self.s3_service.connect(self.cfg)
            if not success:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to connect to S3 for endpoint '{self.name}'"
                )
            
            # Fetch structure from S3
            structure = self.s3_service.get_store_structure()
            
            # Update cache
            self.state["cache"][cache_key] = structure
            self.state["cache_timestamp"][cache_key] = time.time()
            
            logger.info(f"[{self.name}] Structure cached successfully")
            return structure
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"[{self.name}] Failed to get structure: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get structure for endpoint '{self.name}': {str(e)}"
            )
    
    # ========================================================================
    # Cache Management
    # ========================================================================
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns detailed cache performance metrics:
        - Total hits/misses
        - Hit rate percentage
        - Number of cached items
        - TTL configuration
        
        Returns:
            Dict with cache statistics
        """
        total_requests = (
            self.state["cache_stats"]["hits"] + 
            self.state["cache_stats"]["misses"]
        )
        
        hit_rate = (
            self.state["cache_stats"]["hits"] / total_requests * 100
            if total_requests > 0 
            else 0
        )
        
        return {
            "service": self.name,
            "cache_hits": self.state["cache_stats"]["hits"],
            "cache_misses": self.state["cache_stats"]["misses"],
            "total_requests": total_requests,
            "hit_rate": f"{hit_rate:.1f}%",
            "cached_items": len(self.state["cache"]),
            "reload_interval_seconds": self.cfg.reload_interval
        }
    
    async def clear_cache(self) -> Dict[str, Any]:
        """
        Clear cache for this endpoint.
        
        Useful for:
        - Forcing fresh data fetch
        - Debugging
        - Manual cache invalidation
        
        Returns:
            Dict with number of items cleared
        """
        items_cleared = len(self.state["cache"])
        
        self.state["cache"] = {}
        self.state["cache_timestamp"] = {}
        
        logger.info(f"[{self.name}] Cache cleared ({items_cleared} items)")
        
        return {
            "ok": True,
            "service": self.name,
            "items_cleared": items_cleared,
            "message": f"Cache cleared for endpoint '{self.name}'"
        }
