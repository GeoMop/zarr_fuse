"""
Extractor service package.

Obsahuje:
- Workflows: domain logic for Airflow DAGs (S3, tasks, etc.)
- Dags: Airflow DAG definitions.
"""

from . import dags
from . import workflows

__all__ = ["dags", "workflows"]
