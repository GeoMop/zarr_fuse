"""Ingress service - FastAPI entrypoint for ingestion into S3/Zarr."""

# optional convenience imports
__all__ = ["web", "scrapper"]

from . import web
from . import scrapper
