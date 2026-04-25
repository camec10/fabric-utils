"""
fabric-utils: Data engineering utilities for Microsoft Fabric.

Provides table registry, watermarking, optimization scheduling, incremental loading, 
and Delta Lake write strategies.
"""

from fabric_utils.registry import TableRegistry, WatermarkManager
from fabric_utils.loader import (
    DeltaLoader,
    WriteStrategy,
    LoaderError,
    SchemaValidationError,
    LoadResult,
)
from fabric_utils.pipeline import Pipeline
from fabric_utils.setup import setup_control_tables

__version__ = "0.0.03"

__all__ = [
    "TableRegistry",
    "WatermarkManager",  # Backward compatibility alias for TableRegistry
    "DeltaLoader",
    "WriteStrategy",
    "LoaderError",
    "SchemaValidationError",
    "LoadResult",
    "Pipeline",
    "setup_control_tables",
]
