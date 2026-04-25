"""
fabric-utils: Data engineering utilities for Microsoft Fabric.

Provides watermarking, incremental loading, and Delta Lake write strategies.
"""

from fabric_utils.watermark import WatermarkManager
from fabric_utils.loader import (
    DeltaLoader,
    WriteStrategy,
    LoaderError,
    SchemaValidationError,
    LoadResult,
)
from fabric_utils.pipeline import Pipeline
from fabric_utils.setup import setup_control_tables

__version__ = "0.0.02"

__all__ = [
    "WatermarkManager",
    "DeltaLoader",
    "WriteStrategy",
    "LoaderError",
    "SchemaValidationError",
    "LoadResult",
    "Pipeline",
    "setup_control_tables",
]
