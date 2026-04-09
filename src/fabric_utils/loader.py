"""
Delta Lake loading utilities.

Provides write execution for FULL_REFRESH, DELETE+APPEND, and MERGE patterns.
DeltaLoader handles only writes — watermark tracking is the caller's
responsibility via WatermarkManager.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class WriteStrategy(Enum):
    """Available write strategies."""
    DELETE_APPEND = "delete_append"
    MERGE = "merge"
    FULL_REFRESH = "full_refresh"


@dataclass
class LoadResult:
    """Result of a load operation."""
    strategy: WriteStrategy
    rows_processed: int
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    duration_seconds: float
    is_initial_load: bool


class LoaderError(Exception):
    """Raised when DeltaLoader encounters an unrecoverable error."""
    pass


class SchemaValidationError(LoaderError):
    """Raised when required columns are missing from the source DataFrame."""
    pass


class DeltaLoader:
    """
    Writes DataFrames to Delta tables using a specified strategy.
    
    Supports FULL_REFRESH, DELETE+APPEND, and MERGE patterns with schema
    validation, error handling, and automatic rollback for DELETE+APPEND.
    
    DeltaLoader is intentionally watermark-agnostic — use WatermarkManager
    separately to track incremental progress. Choose your write strategy based
    on your data characteristics (see README for guidance).
    
    Example:
        >>> loader = DeltaLoader(
        ...     spark=spark,
        ...     target_table="bronze.orders",
        ...     unique_key_cols=["order_id"],
        ... )
        >>> result = loader.execute(
        ...     source_df,
        ...     strategy=WriteStrategy.DELETE_APPEND,
        ...     delete_predicate="modified_at >= '2024-01-01'",
        ... )
    """
    
    def __init__(
        self,
        spark,
        target_table: str,
        unique_key_cols: Optional[List[str]] = None,
    ):
        """
        Initialize the DeltaLoader.
        
        Args:
            spark: Active SparkSession
            target_table: Target Delta table (e.g., "bronze.orders")
            unique_key_cols: Columns that uniquely identify a row (required for MERGE)
        """
        self.spark = spark
        self.target_table = target_table
        self.unique_key_cols = unique_key_cols or []
    
    def _validate_schema(self, source_df, strategy: WriteStrategy) -> None:
        """Validate that required columns exist in the source DataFrame."""
        if strategy == WriteStrategy.MERGE and self.unique_key_cols:
            source_cols = set(source_df.columns)
            missing = [col for col in self.unique_key_cols if col not in source_cols]
            if missing:
                raise SchemaValidationError(
                    f"Source DataFrame is missing required merge key columns: {missing}. "
                    f"Available columns: {sorted(source_cols)}"
                )
    
    def _ensure_schema_exists(self) -> None:
        """Create the schema/namespace if it doesn't exist.
        
        Extracts the schema name (second-to-last part) from the table name.
        - For 3-part names (lakehouse.schema.table), creates schema
        - For 2-part names (schema.table), creates schema
        """
        parts = self.target_table.split(".")
        if len(parts) >= 2:
            # Schema is always the second-to-last part
            schema = parts[-2]
            try:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            except Exception as e:
                # Only ignore if schema already exists
                if "already exists" not in str(e).lower():
                    raise
    
    def _get_delta_version(self) -> Optional[int]:
        """Get the current Delta table version for rollback support."""
        try:
            history = self.spark.sql(
                f"DESCRIBE HISTORY {self.target_table} LIMIT 1"
            ).collect()
            if history:
                return int(history[0]["version"])
        except Exception:
            pass
        return None
    
    def _restore_version(self, version: int) -> None:
        """Restore the Delta table to a specific version."""
        self.spark.sql(
            f"RESTORE TABLE {self.target_table} TO VERSION AS OF {version}"
        )
    
    def execute(
        self,
        source_df,
        strategy: WriteStrategy,
        delete_predicate: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> LoadResult:
        """
        Execute a write with the specified strategy.
        
        Args:
            source_df: DataFrame to write (already transformed/filtered by caller)
            strategy: Write strategy to use
            delete_predicate: SQL predicate for DELETE+APPEND (e.g.,
                              "modified_at >= '2024-01-01'"). Required for
                              DELETE+APPEND on existing tables.
            run_id: Optional pipeline run ID stamped as _pipelineRunId column
            
        Returns:
            LoadResult with execution metrics
            
        Raises:
            SchemaValidationError: If MERGE key columns are missing from source_df
            LoaderError: If the write fails (DELETE+APPEND is auto-rolled back)
        """
        import time
        
        start_time = time.time()
        
        # Validate schema before doing any work
        self._validate_schema(source_df, strategy)
        
        from pyspark.sql import functions as F
        
        # Ensure the schema exists before checking/creating tables
        self._ensure_schema_exists()
        
        # Check for initial load
        is_initial = not self.spark.catalog.tableExists(self.target_table)
        
        # Snapshot version for DELETE+APPEND rollback
        restore_version = None
        if strategy == WriteStrategy.DELETE_APPEND and not is_initial:
            restore_version = self._get_delta_version()
        
        # Add pipeline metadata
        source_df = source_df.withColumn("_pipelineRunTs", F.current_timestamp())
        if run_id:
            source_df = source_df.withColumn("_pipelineRunId", F.lit(run_id))
        
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0
        
        try:
            if is_initial or strategy == WriteStrategy.FULL_REFRESH:
                rows_inserted = source_df.count()
                (
                    source_df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(self.target_table)
                )
                
            elif strategy == WriteStrategy.DELETE_APPEND:
                # Count source rows BEFORE deleting anything
                source_count = source_df.count()
                rows_inserted = source_count
                
                if delete_predicate:
                    rows_deleted = self.spark.sql(f"""
                        SELECT COUNT(*) AS cnt 
                        FROM {self.target_table} 
                        WHERE {delete_predicate}
                    """).collect()[0]["cnt"]
                    
                    # Safety check: Prevent accidental data loss
                    if rows_deleted > 0 and rows_inserted == 0:
                        raise LoaderError(
                            f"DELETE_APPEND safety check failed: Would delete {rows_deleted:,} "
                            f"rows but source DataFrame has 0 rows to insert. This would cause "
                            f"data loss.\n\n"
                            f"Possible causes:\n"
                            f"  • Source system(s) haven't been updated yet\n"
                            f"  • Source extraction returned no data\n"
                            f"  • One or more source tables is empty/down\n"
                            f"  • Multi-source union with mismatched update schedules\n\n"
                            f"Recommendation: Verify your source data is available before retrying, "
                            f"or use MERGE strategy instead of DELETE_APPEND for better safety."
                        )
                    
                    print(f"DELETE_APPEND: Will delete {rows_deleted:,} rows, insert {rows_inserted:,} rows")
                    
                    # Now safe to delete
                    self.spark.sql(f"""
                        DELETE FROM {self.target_table}
                        WHERE {delete_predicate}
                    """)
                
                if rows_inserted > 0:
                    (
                        source_df.write
                        .format("delta")
                        .mode("append")
                        .saveAsTable(self.target_table)
                    )
                    
            elif strategy == WriteStrategy.MERGE:
                from delta.tables import DeltaTable
                
                target_table = DeltaTable.forName(self.spark, self.target_table)
                
                merge_condition = " AND ".join([
                    f"target.{col} = source.{col}"
                    for col in self.unique_key_cols
                ])
                
                (
                    target_table.alias("target")
                    .merge(source_df.alias("source"), merge_condition)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                
                history = self.spark.sql(
                    f"DESCRIBE HISTORY {self.target_table} LIMIT 1"
                ).collect()[0]
                metrics = history["operationMetrics"]
                rows_inserted = int(metrics.get("numTargetRowsInserted", 0))
                rows_updated = int(metrics.get("numTargetRowsUpdated", 0))
        
        except Exception as e:
            if strategy == WriteStrategy.DELETE_APPEND and restore_version is not None:
                try:
                    self._restore_version(restore_version)
                except Exception:
                    raise LoaderError(
                        f"CRITICAL: DELETE+APPEND failed and rollback also failed. "
                        f"Attempted RESTORE to version {restore_version}. "
                        f"Manual intervention required. Original error: {e}"
                    ) from e
            raise LoaderError(
                f"Load failed for {self.target_table} with strategy "
                f"{strategy.value}: {e}"
            ) from e
        
        duration = time.time() - start_time
        
        return LoadResult(
            strategy=strategy,
            rows_processed=source_count if strategy == WriteStrategy.DELETE_APPEND else source_df.count(),
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
            duration_seconds=duration,
            is_initial_load=is_initial,
        )
