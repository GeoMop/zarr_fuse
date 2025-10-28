"""Ingress service - FastAPI entrypoint for ingestion into S3/Zarr."""

# optional convenience imports
from . import web, scrapper

__all__ = ["web", "scrapper"]
