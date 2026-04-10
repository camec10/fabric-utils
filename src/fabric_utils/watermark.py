"""
Watermark management for incremental data loading.

Provides utilities to track and manage watermarks (high-water marks) for 
incremental data pipelines in Microsoft Fabric.
"""

from datetime import datetime, timedelta
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from fabric_utils.setup import setup_control_tables


class WatermarkManager:
    """
    Manages watermarks for incremental data loading pipelines.
    
    Watermarks track the last successfully processed value (typically a timestamp
    or incrementing ID) to enable efficient incremental loading.
    
    Example:
        >>> wm = WatermarkManager(spark, lakehouse="lkhRaw", schema="control")
        >>> watermark = wm.get_watermark("bronze.orders", lookback_days=90)
        >>> if watermark:
        ...     df = spark.sql(f"SELECT * FROM source WHERE modified_at >= '{watermark}'")
        >>> wm.update_watermark("bronze.orders", new_max_value)
    """
    
    def __init__(
        self, 
        spark: "SparkSession", 
        lakehouse: Optional[str] = None, 
        schema: str = "control", 
        table: str = "watermarks"
    ) -> None:
        """
        Initialize the WatermarkManager.
        
        Args:
            spark: Active SparkSession (provided by Fabric runtime)
            lakehouse: Lakehouse name (e.g., "lkhRaw"). If None, uses default attached lakehouse.
            schema: Schema name for control table (default: "control")
            table: Control table name (default: "watermarks")
        """
        self.spark = spark
        self.lakehouse = lakehouse
        self.schema = schema
        self.table_name = table
        
        # Build fully qualified table name for Fabric
        # Format: lakehouse.schema.table (3-part) or schema.table (2-part if no lakehouse)
        if lakehouse:
            self.control_table = f"{lakehouse}.{schema}.{table}"
            self.full_schema = f"{lakehouse}.{schema}"
        else:
            self.control_table = f"{schema}.{table}"
            self.full_schema = schema
        
        # Create control tables if they don't exist
        self._ensure_control_tables_exist()
    
    def _ensure_control_tables_exist(self) -> None:
        """
        Create control tables if they don't exist.
        
        This runs automatically on initialization to eliminate manual setup steps.
        Delegates to setup_control_tables() for the actual table creation.
        """
        try:
            setup_control_tables(self.spark, lakehouse=self.lakehouse, schema=self.schema, silent=True)
        except Exception as e:
            # Only ignore if tables already exist
            if "already exists" not in str(e).lower():
                raise
    
    def get_watermark(
        self, 
        table_name: str, 
        lookback_days: int = 0
    ) -> Optional[datetime]:
        """
        Get the last watermark value for a table.
        
        Args:
            table_name: Target table name (e.g., "bronze.orders")
            lookback_days: Days to subtract from watermark for late-arriving data
            
        Returns:
            Watermark datetime, or None if no watermark exists (initial load)
        """
        try:
            row = self.spark.sql(f"""
                SELECT watermarkValue 
                FROM {self.control_table}
                WHERE tableName = '{table_name}'
            """).collect()
            
            if not row or row[0]["watermarkValue"] is None:
                return None
            
            watermark = row[0]["watermarkValue"]
            
            if lookback_days > 0 and watermark:
                watermark = watermark - timedelta(days=lookback_days)
            
            return watermark
            
        except Exception:
            # Table doesn't exist or other error — treat as initial load
            return None
    
    def update_watermark(
        self, 
        table_name: str, 
        new_value: Any,
        run_id: Optional[str] = None
    ) -> None:
        """
        Update the watermark after successful processing.
        
        Uses MERGE to handle both insert (first run) and update (subsequent runs).
        
        Args:
            table_name: Target table name
            new_value: New watermark value (timestamp or comparable)
            run_id: Optional pipeline run ID for audit
        """
        # Format value for SQL
        # Omit microseconds if zero for consistency with source data formats
        if isinstance(new_value, datetime):
            if new_value.microsecond == 0:
                value_str = new_value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                value_str = new_value.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            value_str = str(new_value)
        
        run_id_str = f"'{run_id}'" if run_id else "NULL"
        
        self.spark.sql(f"""
            MERGE INTO {self.control_table} AS target
            USING (SELECT '{table_name}' AS tableName) AS source
            ON target.tableName = source.tableName
            WHEN MATCHED THEN
                UPDATE SET 
                    watermarkValue = '{value_str}',
                    lastRunTs = current_timestamp(),
                    lastRunId = {run_id_str}
            WHEN NOT MATCHED THEN
                INSERT (tableName, watermarkValue, lastRunTs, lastRunId, createdTs)
                VALUES ('{table_name}', '{value_str}', current_timestamp(), {run_id_str}, current_timestamp())
        """)
    
    def build_where_clause(
        self,
        table_name: str,
        column_name: str,
        lookback_days: int = 0
    ) -> str:
        """
        Build a WHERE clause for incremental queries.
        
        Args:
            table_name: Target table name for watermark lookup
            column_name: Source column to filter on
            lookback_days: Days to subtract for late-arriving data
            
        Returns:
            WHERE clause string, or empty string for initial load
            
        Example:
            >>> clause = wm.build_where_clause("bronze.orders", "modified_at", 90)
            >>> df = spark.sql(f"SELECT * FROM source {clause}")
        """
        watermark = self.get_watermark(table_name, lookback_days)
        
        if watermark is None:
            return ""  # Initial load — no filter
        
        if isinstance(watermark, datetime):
            # Omit microseconds if zero to avoid string comparison issues
            # when target column is stored as string without microseconds
            if watermark.microsecond == 0:
                watermark_str = watermark.strftime("%Y-%m-%d %H:%M:%S")
            else:
                watermark_str = watermark.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            watermark_str = str(watermark)
        
        return f"WHERE {column_name} >= '{watermark_str}'"
    
    def get_watermark_info(self, table_name: str) -> Optional[dict]:
        """
        Get full watermark metadata for a table.
        
        Returns:
            Dict with watermarkValue, lastRunTs, lastRunId, or None
        """
        try:
            row = self.spark.sql(f"""
                SELECT 
                    watermarkValue,
                    lastRunTs,
                    lastRunId
                FROM {self.control_table}
                WHERE tableName = '{table_name}'
            """).collect()
            
            if not row:
                return None
            
            return {
                "watermarkValue": row[0]["watermarkValue"],
                "lastRunTs": row[0]["lastRunTs"],
                "lastRunId": row[0]["lastRunId"],
            }
        except Exception:
            return None
    
    def reset_watermark(self, table_name: str) -> None:
        """
        Reset watermark to trigger a full reload on next run.
        
        Args:
            table_name: Target table name
        """
        self.spark.sql(f"""
            DELETE FROM {self.control_table}
            WHERE tableName = '{table_name}'
        """)
    
    def list_watermarks(self) -> list:
        """
        List all tracked watermarks.
        
        Returns:
            List of dicts with table_name, watermark_value, last_run_ts
        """
        try:
            rows = self.spark.sql(f"""
                SELECT tableName, watermarkValue, lastRunTs
                FROM {self.control_table}
                ORDER BY tableName
            """).collect()
            
            return [
                {
                    "tableName": r["tableName"],
                    "watermarkValue": r["watermarkValue"],
                    "lastRunTs": r["lastRunTs"],
                }
                for r in rows
            ]
        except Exception:
            return []
    
    def log_pipeline_run(
        self,
        run_id: str,
        pipeline_name: str,
        target_table: str = None,
        status: str = "STARTED",
        strategy: str = None,
        rows_processed: int = None,
        rows_inserted: int = None,
        rows_updated: int = None,
        rows_deleted: int = None,
        duration_seconds: float = None,
        error_message: str = None,
    ) -> None:
        """
        Log a pipeline run to the pipelineRuns audit table.
        
        Uses retry logic to handle concurrent writes from multiple pipelines.
        
        Args:
            run_id: Unique run identifier
            pipeline_name: Name of the pipeline
            target_table: Target table being loaded
            status: Run status (STARTED, COMPLETED, FAILED)
            strategy: Write strategy used
            rows_processed: Total rows processed
            rows_inserted: Rows inserted
            rows_updated: Rows updated
            rows_deleted: Rows deleted
            duration_seconds: Execution duration
            error_message: Error message if failed
        """
        import time
        
        # Build fully qualified table name for pipelineRuns
        if self.lakehouse:
            full_table = f"{self.lakehouse}.{self.schema}.pipelineRuns"
        else:
            full_table = f"{self.schema}.pipelineRuns"
        
        # Escape single quotes in error message for SQL
        escaped_error = error_message.replace("'", "''") if error_message else None
        
        # Build MERGE statement
        merge_sql = f"""
            MERGE INTO {full_table} AS target
            USING (
                SELECT 
                    '{run_id}' AS runId,
                    '{pipeline_name}' AS pipelineName
            ) AS source
            ON target.runId = source.runId
               AND target.pipelineName = source.pipelineName
            WHEN MATCHED THEN
                UPDATE SET 
                    status = '{status}',
                    strategy = {f"'{strategy}'" if strategy else 'NULL'},
                    rowsProcessed = {rows_processed if rows_processed else 'NULL'},
                    rowsInserted = {rows_inserted if rows_inserted else 'NULL'},
                    rowsUpdated = {rows_updated if rows_updated else 'NULL'},
                    rowsDeleted = {rows_deleted if rows_deleted else 'NULL'},
                    durationSeconds = {duration_seconds if duration_seconds else 'NULL'},
                    errorMessage = {f"'{escaped_error}'" if escaped_error else 'NULL'},
                    completedTs = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (
                    runId, pipelineName, targetTable, status, strategy,
                    rowsProcessed, rowsInserted, rowsUpdated, rowsDeleted,
                    durationSeconds, errorMessage, startedTs, completedTs
                )
                VALUES (
                    '{run_id}', '{pipeline_name}', 
                    {f"'{target_table}'" if target_table else 'NULL'},
                    '{status}',
                    {f"'{strategy}'" if strategy else 'NULL'},
                    {rows_processed if rows_processed else 'NULL'},
                    {rows_inserted if rows_inserted else 'NULL'},
                    {rows_updated if rows_updated else 'NULL'},
                    {rows_deleted if rows_deleted else 'NULL'},
                    {duration_seconds if duration_seconds else 'NULL'},
                    {f"'{escaped_error}'" if escaped_error else 'NULL'},
                    current_timestamp(),
                    NULL
                )
        """
        
        # Retry logic for concurrent writes
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                self.spark.sql(merge_sql)
                return  # Success!
            except Exception as e:
                error_str = str(e)
                if "ConcurrentAppendException" in error_str or "ConcurrentModificationException" in error_str:
                    if attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        import random
                        wait_time = retry_delay * (2 ** attempt) + random.uniform(0, 1)
                        time.sleep(wait_time)
                        continue
                    else:
                        # Final attempt failed - log warning but don't fail the pipeline
                        print(f"⚠ WARNING: Could not log pipeline run after {max_retries} attempts due to concurrent writes. Pipeline continues.")
                        return
                else:
                    # Non-concurrency error - could be schema issue, permissions, etc.
                    # Log warning but don't fail the pipeline over audit logging
                    print(f"⚠ WARNING: Could not log pipeline run: {error_str}")
                    return
