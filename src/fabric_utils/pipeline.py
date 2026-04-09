"""
Convenience orchestrator for incremental pipelines.

Composes WatermarkManager and DeltaLoader into a single workflow that
automates strategy selection, delete-predicate construction, and watermark
updates — while still exposing the raw components for power-user scenarios.
"""

import time
import uuid
from datetime import datetime
from typing import List, Optional, Union

from fabric_utils.loader import (
    DeltaLoader,
    LoaderError,
    LoadResult,
    WriteStrategy,
)
from fabric_utils.watermark import WatermarkManager

# Sentinel to distinguish "get_watermark() not yet called" from "called and got None"
_NOT_RETRIEVED = object()


class Pipeline:
    """
    High-level orchestrator for the standard incremental pattern:

        1. get_watermark()  → retrieve last position
        2. (caller extracts & transforms)
        3. execute(df)      → strategy selection + load + watermark update

    Internally composes WatermarkManager and DeltaLoader, both accessible
    as ``self.wm`` and ``self.loader`` for direct use when needed.

    Example — DELETE+APPEND::

        pipe = Pipeline(
            spark,
            target_table="bronze.time_entries",
            watermark_column="lastUpdateDate",
            strategy=WriteStrategy.DELETE_APPEND,
            lookback_days=90,
        )

        watermark = pipe.get_watermark()

        if watermark:
            source_df = spark.sql(f"SELECT * FROM raw.time_entries WHERE lastUpdateDate >= '{watermark}'")
        else:
            source_df = spark.table("raw.time_entries")

        result = pipe.execute(source_df)

    Example — MERGE::

        pipe = Pipeline(
            spark,
            target_table="silver.repair_orders",
            watermark_column="modifiedTimestamp",
            strategy=WriteStrategy.MERGE,
            unique_key_cols=["repairOrderId", "lineNumber"],
            lookback_days=30,
        )

        watermark = pipe.get_watermark()
        source_df = spark.sql(f"...WHERE modifiedTimestamp >= '{watermark}'") if watermark else spark.table("...")
        result = pipe.execute(source_df)
    """

    def __init__(
        self,
        spark,
        target_table: str,
        watermark_column: str,
        strategy: Union[WriteStrategy, str] = WriteStrategy.DELETE_APPEND,
        unique_key_cols: Optional[List[str]] = None,
        lookback_days: int = 90,
        lakehouse: str = None,
        control_schema: str = "control",
    ):
        """
        Args:
            spark: Active SparkSession
            target_table: Fully qualified Delta table (e.g., "bronze.orders")
            watermark_column: Column used for incremental tracking
            strategy: Write strategy for subsequent runs (first run always FULL_REFRESH).
                      Can be a WriteStrategy enum or string ("DELETE_APPEND", "MERGE", "FULL_REFRESH").
                      Strings are case-insensitive.
            unique_key_cols: Key columns for MERGE (required when strategy is MERGE)
            lookback_days: Days to subtract from watermark for late-arriving data
            lakehouse: Lakehouse name (e.g., "lkhRaw"). If None, uses default attached lakehouse.
            control_schema: Schema for control tables (default: "control")
        """
        # Convert string strategy to enum if needed
        if isinstance(strategy, str):
            try:
                self.strategy = WriteStrategy[strategy.upper()]
            except KeyError:
                valid = ", ".join([s.name for s in WriteStrategy])
                raise ValueError(
                    f"Invalid strategy '{strategy}'. Must be one of: {valid}"
                )
        else:
            self.strategy = strategy
        
        self.spark = spark
        self.target_table = target_table
        self.watermark_column = watermark_column
        self.lookback_days = lookback_days
        self.lakehouse = lakehouse
        self.control_schema = control_schema

        # Compose — not inherit
        self.wm = WatermarkManager(spark, lakehouse=lakehouse, schema=control_schema)
        self.loader = DeltaLoader(spark, target_table, unique_key_cols)

        self._watermark = _NOT_RETRIEVED

    def get_watermark(self):
        """
        Retrieve the current watermark (with lookback applied).

        Returns None on first run, signaling a full extract.
        Must be called before ``execute()``.
        """
        # If user explicitly requested FULL_REFRESH, reset watermark and return None
        # to signal full extraction
        if self.strategy == WriteStrategy.FULL_REFRESH:
            self.wm.reset_watermark(self.target_table)
            self._watermark = None
            return None
        
        # Check for orphaned watermark: watermark exists but table was dropped
        # Must check BEFORE user extracts data based on watermark
        try:
            table_exists = self.spark.catalog.tableExists(self.target_table)
        except Exception as e:
            # Schema might not exist yet - treat as table doesn't exist
            print(f"Note: Could not check if table exists ({e}), treating as new table")
            table_exists = False
        
        stored_watermark = self.wm.get_watermark(
            self.target_table, self.lookback_days
        )
        
        if stored_watermark is not None and not table_exists:
            print(
                f"⚠ WARNING: Watermark exists ({stored_watermark}) but table "
                f"{self.target_table} does not exist. This suggests the table was "
                f"dropped without resetting the watermark. Resetting watermark and "
                f"triggering full extraction."
            )
            self.wm.reset_watermark(self.target_table)
            self._watermark = None
            return None
        
        # Get watermark with lookback applied
        # Both extraction AND deletion use the same boundary to refresh the lookback window
        self._watermark = stored_watermark
        return self._watermark

    def execute(
        self,
        source_df,
        new_watermark=None,
        run_id: Optional[str] = None,
    ) -> LoadResult:
        """
        Execute load and update watermark on success.

        Handles:
        - Strategy selection (FULL_REFRESH on first run, configured strategy after)
        - Delete-predicate construction for DELETE+APPEND
        - Watermark update only on successful write

        Args:
            source_df: Already-filtered/transformed DataFrame to write
            new_watermark: Explicit watermark value to store on success.
                           If None, derived from ``max(watermark_column)``
                           on source_df.
            run_id: Optional pipeline run ID for audit

        Returns:
            LoadResult with execution metrics

        Raises:
            LoaderError: If get_watermark() was not called, or if the write fails
        """
        if self._watermark is _NOT_RETRIEVED:
            raise LoaderError(
                "Call get_watermark() before execute(). The pipeline needs to "
                "know the current position before it can pick a strategy and "
                "build the delete predicate."
            )

        # Generate run_id if not provided
        if run_id is None:
            run_id = str(uuid.uuid4())

        start_time = time.time()

        # First run → FULL_REFRESH regardless of configured strategy
        # Note: FULL_REFRESH strategy already handled in get_watermark()
        if self._watermark is None:
            effective_strategy = WriteStrategy.FULL_REFRESH
            print(f"Strategy: FULL_REFRESH (watermark is None - initial load or orphaned watermark)")
        else:
            effective_strategy = self.strategy
            print(f"Strategy: {effective_strategy.value} (watermark exists: {self._watermark})")

        # Build delete predicate for DELETE_APPEND
        delete_predicate = None
        if (
            effective_strategy == WriteStrategy.DELETE_APPEND
            and self._watermark is not None
        ):
            # Use the same lookback-adjusted watermark for both delete and extract
            # This refreshes the entire lookback window to catch late-arriving data
            if isinstance(self._watermark, datetime):
                # Omit microseconds if zero to avoid string comparison issues
                # when target column is stored as string without microseconds
                if self._watermark.microsecond == 0:
                    wm_str = self._watermark.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    wm_str = self._watermark.strftime("%Y-%m-%d %H:%M:%S.%f")
            else:
                wm_str = str(self._watermark)
            delete_predicate = f"{self.watermark_column} >= '{wm_str}'"

        # Log pipeline start
        self.wm.log_pipeline_run(
            run_id=run_id,
            pipeline_name=self.target_table,
            target_table=self.target_table,
            status="STARTED",
            strategy=effective_strategy.value,
        )

        try:
            # Delegate write to DeltaLoader
            result = self.loader.execute(
                source_df,
                strategy=effective_strategy,
                delete_predicate=delete_predicate,
                run_id=run_id,
            )

            # Detect orphaned watermark: table didn't exist but watermark did
            # This is a backup check - should have been caught in get_watermark()
            if result.is_initial_load and self._watermark is not None:
                print(
                    f"⚠ WARNING: Table {self.target_table} did not exist but watermark "
                    f"{self._watermark} was found. Watermark was reset during get_watermark(). "
                    f"If you see this, the table may have been dropped between get_watermark() "
                    f"and execute()."
                )

            # Derive new watermark if caller didn't supply one
            if new_watermark is None:
                from pyspark.sql import functions as F

                row = source_df.agg(F.max(self.watermark_column)).collect()[0][0]
                new_watermark = row

            # Update only on success (we only get here if loader didn't raise)
            if new_watermark is not None:
                self.wm.update_watermark(self.target_table, new_watermark, run_id)

            # Log successful completion
            duration = time.time() - start_time
            self.wm.log_pipeline_run(
                run_id=run_id,
                pipeline_name=self.target_table,
                target_table=self.target_table,
                status="COMPLETED",
                strategy=effective_strategy.value,
                rows_processed=result.rows_processed,
                rows_inserted=result.rows_inserted,
                rows_updated=result.rows_updated,
                rows_deleted=result.rows_deleted,
                duration_seconds=duration,
            )

            return result

        except Exception as e:
            # Log failure
            duration = time.time() - start_time
            self.wm.log_pipeline_run(
                run_id=run_id,
                pipeline_name=self.target_table,
                target_table=self.target_table,
                status="FAILED",
                strategy=effective_strategy.value if effective_strategy else None,
                duration_seconds=duration,
                error_message=str(e),
            )
            raise
