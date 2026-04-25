"""
Table registry for managing table metadata, watermarks, and optimization schedules.

Provides utilities to track and manage table metadata including watermarks (high-water marks),
optimization schedules, and write strategies for data pipelines in Microsoft Fabric.
"""

from datetime import datetime, timedelta
from typing import Any, Optional

from fabric_utils.setup import setup_control_tables


class TableRegistry:
    """
    Manages table metadata, watermarks, and optimization schedules for data pipelines.
    
    Replaces WatermarkManager with extended capabilities for table catalog management,
    optimization scheduling, and write strategy configuration.
    
    Example:
        >>> registry = TableRegistry(spark, control_lakehouse="lkhControl", schema="control")
        >>> watermark = registry.get_watermark("bronze.orders", lookback_days=90)
        >>> if watermark:
        ...     df = spark.sql(f"SELECT * FROM source WHERE modified_at >= '{watermark}'")
        >>> registry.update_watermark("bronze.orders", new_max_value)
    """
    
    def __init__(self, spark, control_lakehouse: str = None, schema: str = "control", table: str = "tableRegistry"):
        """
        Initialize the TableRegistry.
        
        Args:
            spark: Active SparkSession (provided by Fabric runtime)
            control_lakehouse: Lakehouse name for control tables (e.g., "lkhControl"). If None, uses default attached lakehouse.
            schema: Schema name for control table (default: "control")
            table: Control table name (default: "tableRegistry")
        """
        self.spark = spark
        self.control_lakehouse = control_lakehouse
        self.schema = schema
        self.table_name = table
        
        # Build fully qualified table name for Fabric
        # Format: control_lakehouse.schema.table (3-part) or schema.table (2-part if no control_lakehouse)
        if control_lakehouse:
            self.control_table = f"{control_lakehouse}.{schema}.{table}"
            self.full_schema = f"{control_lakehouse}.{schema}"
        else:
            self.control_table = f"{schema}.{table}"
            self.full_schema = schema
        
        # Create control tables if they don't exist (fresh installs)
        self._ensure_control_tables_exist()
        
        # Migrate from old watermarks table if needed (upgrades from v0.0.x)
        self._migrate_from_watermarks_if_needed()
    
    def _ensure_control_tables_exist(self) -> None:
        """
        Create control tables if they don't exist.
        
        This runs automatically on initialization to eliminate manual setup steps.
        Delegates to setup_control_tables() for the actual table creation.
        """
        try:
            setup_control_tables(self.spark, control_lakehouse=self.control_lakehouse, schema=self.schema, silent=True)
        except Exception as e:
            # Only ignore if tables already exist
            if "already exists" not in str(e).lower():
                raise
    
    def _migrate_from_watermarks_if_needed(self) -> None:
        """
        Migrate from control.watermarks (v0.0.x) to control.tableRegistry (v0.1.0+).
        
        Runs automatically on first use after upgrade. Idempotent and safe to run multiple times.
        Preserves all existing watermark data and adds new optimization columns.
        """
        old_table = f"{self.full_schema}.watermarks"
        new_table = self.control_table
        
        try:
            has_old = self.spark.catalog.tableExists(old_table)
            has_new = self.spark.catalog.tableExists(new_table)
            
            if has_old and not has_new:
                print(f"🔄 Migrating {old_table} → {new_table}...")
                
                # Create new table with expanded schema from old watermarks data
                self.spark.sql(f"""
                    CREATE TABLE {new_table}
                    USING DELTA
                    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
                    AS SELECT 
                        tableName,
                        COALESCE(createdTs, current_timestamp()) as createdTimestamp,
                        current_timestamp() as updatedTimestamp,
                        watermarkValue,
                        CAST(NULL AS STRING) as watermarkColumn,
                        0 as lookbackDays,
                        0 as optimizationScheduleDays,
                        CAST(NULL AS TIMESTAMP) as lastOptimizedTimestamp,
                        lastRunId,
                        lastRunTs as lastRunTimestamp
                    FROM {old_table}
                """)
                
                # Drop old table (data preserved in new table)
                self.spark.sql(f"DROP TABLE {old_table}")
                
                print(f"✅ Migration complete. {old_table} dropped, data preserved in {new_table}.")
            
            elif has_old and has_new:
                # Both exist - warn but don't break (rare edge case)
                print(f"⚠️  Both {old_table} and {new_table} exist. Using {new_table}.")
                print(f"    Consider manually dropping {old_table} if migration is complete.")
        
        except Exception as e:
            # Don't break initialization if migration fails - table might already exist
            if "already exists" not in str(e).lower():
                print(f"⚠️  Migration check failed: {e}")
    
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
                    lastRunTimestamp = current_timestamp(),
                    lastRunId = {run_id_str},
                    updatedTimestamp = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (tableName, watermarkValue, lastRunTimestamp, lastRunId, createdTimestamp, updatedTimestamp, lookbackDays, optimizationScheduleDays)
                VALUES ('{table_name}', '{value_str}', current_timestamp(), {run_id_str}, current_timestamp(), current_timestamp(), 0, 0)
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
            >>> clause = registry.build_where_clause("bronze.orders", "modified_at", 90)
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
    
    def get_table_metadata(self, table_name: str) -> Optional[dict]:
        """
        Get full metadata for a table.
        
        Returns:
            Dict with all table metadata, or None if table not registered
        """
        try:
            row = self.spark.sql(f"""
                SELECT 
                    tableName,
                    createdTimestamp,
                    updatedTimestamp,
                    watermarkValue,
                    watermarkColumn,
                    lookbackDays,
                    optimizationScheduleDays,
                    lastOptimizedTimestamp,
                    lastRunId,
                    lastRunTimestamp
                FROM {self.control_table}
                WHERE tableName = '{table_name}'
            """).collect()
            
            if not row:
                return None
            
            r = row[0]
            return {
                "tableName": r["tableName"],
                "createdTimestamp": r["createdTimestamp"],
                "updatedTimestamp": r["updatedTimestamp"],
                "watermarkValue": r["watermarkValue"],
                "watermarkColumn": r["watermarkColumn"],
                "lookbackDays": r["lookbackDays"],
                "optimizationScheduleDays": r["optimizationScheduleDays"],
                "lastOptimizedTimestamp": r["lastOptimizedTimestamp"],
                "lastRunId": r["lastRunId"],
                "lastRunTimestamp": r["lastRunTimestamp"],
            }
        except Exception:
            return None
    
    def register_table(
        self,
        table_name: str,
        watermark_column: Optional[str] = None,
        lookback_days: int = 0,
        optimization_schedule_days: int = 0,
    ) -> None:
        """
        Register a table with metadata and configuration.
        
        Args:
            table_name: Fully qualified table name (e.g., "bronze.orders")
            watermark_column: Column name for watermark tracking (e.g., "modified_at")
            lookback_days: Days to subtract from watermark for late-arriving data
            optimization_schedule_days: Run OPTIMIZE every N days (0 = disabled)
        """
        self.spark.sql(f"""
            MERGE INTO {self.control_table} AS target
            USING (SELECT '{table_name}' AS tableName) AS source
            ON target.tableName = source.tableName
            WHEN MATCHED THEN
                UPDATE SET
                    watermarkColumn = {f"'{watermark_column}'" if watermark_column else "NULL"},
                    lookbackDays = {lookback_days},
                    optimizationScheduleDays = {optimization_schedule_days},
                    updatedTimestamp = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (tableName, createdTimestamp, updatedTimestamp, watermarkColumn, lookbackDays, optimizationScheduleDays)
                VALUES ('{table_name}', current_timestamp(), current_timestamp(), 
                        {f"'{watermark_column}'" if watermark_column else "NULL"}, 
                        {lookback_days}, {optimization_schedule_days})
        """)
    
    def set_optimization_schedule(self, table_name: str, days: int) -> None:
        """
        Set optimization schedule for a table.
        
        Args:
            table_name: Target table name
            days: Run OPTIMIZE every N days (0 = disabled)
        """
        self.spark.sql(f"""
            UPDATE {self.control_table}
            SET optimizationScheduleDays = {days},
                updatedTimestamp = current_timestamp()
            WHERE tableName = '{table_name}'
        """)
    
    def update_last_optimized(self, table_name: str) -> None:
        """
        Update the lastOptimizedTimestamp to current time.
        
        Args:
            table_name: Target table name
        """
        self.spark.sql(f"""
            UPDATE {self.control_table}
            SET lastOptimizedTimestamp = current_timestamp(),
                updatedTimestamp = current_timestamp()
            WHERE tableName = '{table_name}'
        """)
    
    def get_tables_needing_optimization(self) -> list:
        """
        Get tables that are due for optimization based on their schedule.
        
        Returns:
            List of dicts with tableName and daysSinceOptimize
        """
        try:
            rows = self.spark.sql(f"""
                SELECT 
                    tableName,
                    optimizationScheduleDays,
                    lastOptimizedTimestamp,
                    COALESCE(DATEDIFF(current_timestamp(), lastOptimizedTimestamp), 999999) as daysSinceOptimize
                FROM {self.control_table}
                WHERE optimizationScheduleDays > 0
                  AND (lastOptimizedTimestamp IS NULL 
                       OR DATEDIFF(current_timestamp(), lastOptimizedTimestamp) >= optimizationScheduleDays)
                ORDER BY daysSinceOptimize DESC
            """).collect()
            
            return [
                {
                    "tableName": r["tableName"],
                    "optimizationScheduleDays": r["optimizationScheduleDays"],
                    "daysSinceOptimize": r["daysSinceOptimize"],
                }
                for r in rows
            ]
        except Exception:
            return []
    
    def reset_watermark(self, table_name: str) -> None:
        """
        Reset watermark to trigger a full reload on next run.
        
        Args:
            table_name: Target table name
        """
        self.spark.sql(f"""
            UPDATE {self.control_table}
            SET watermarkValue = NULL,
                updatedTimestamp = current_timestamp()
            WHERE tableName = '{table_name}'
        """)
    
    def list_tables(self, filter_active: bool = True) -> list:
        """
        List all registered tables.
        
        Args:
            filter_active: Only return tables with watermarks or optimization enabled
        
        Returns:
            List of dicts with table metadata
        """
        try:
            where_clause = ""
            if filter_active:
                where_clause = "WHERE watermarkValue IS NOT NULL OR optimizationScheduleDays > 0"
            
            rows = self.spark.sql(f"""
                SELECT 
                    tableName, 
                    watermarkValue, 
                    watermarkColumn,
                    optimizationScheduleDays,
                    lastOptimizedTimestamp,
                    lastRunTimestamp
                FROM {self.control_table}
                {where_clause}
                ORDER BY tableName
            """).collect()
            
            return [
                {
                    "tableName": r["tableName"],
                    "watermarkValue": r["watermarkValue"],
                    "watermarkColumn": r["watermarkColumn"],
                    "optimizationScheduleDays": r["optimizationScheduleDays"],
                    "lastOptimizedTimestamp": r["lastOptimizedTimestamp"],
                    "lastRunTimestamp": r["lastRunTimestamp"],
                }
                for r in rows
            ]
        except Exception:
            return []
    
    # Alias for backward compatibility
    def get_watermark_info(self, table_name: str) -> Optional[dict]:
        """
        Get watermark metadata for a table (backward compatibility alias).
        
        Returns:
            Dict with watermarkValue, lastRunTimestamp, lastRunId, or None
        """
        metadata = self.get_table_metadata(table_name)
        if not metadata:
            return None
        
        return {
            "watermarkValue": metadata["watermarkValue"],
            "lastRunTimestamp": metadata["lastRunTimestamp"],
            "lastRunId": metadata["lastRunId"],
        }
    
    def list_watermarks(self) -> list:
        """
        List all tracked watermarks (backward compatibility alias).
        
        Returns:
            List of dicts with table_name, watermark_value, lastRunTimestamp
        """
        return self.list_tables(filter_active=True)
    
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
        if self.control_lakehouse:
            full_table = f"{self.control_lakehouse}.{self.schema}.pipelineRuns"
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
                    completedTimestamp = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (
                    runId, pipelineName, targetTable, status, strategy,
                    rowsProcessed, rowsInserted, rowsUpdated, rowsDeleted,
                    durationSeconds, errorMessage, startedTimestamp, completedTimestamp
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


# Backward compatibility alias
WatermarkManager = TableRegistry
