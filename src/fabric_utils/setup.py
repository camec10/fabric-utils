"""
Setup utilities for control tables and schema initialization.
"""


def setup_control_tables(spark, lakehouse: str = None, schema: str = "control", silent: bool = False) -> None:
    """
    Create control tables for watermark tracking.
    
    Creates the following tables:
    - {lakehouse}.{schema}.watermarks — Timestamp-based watermark tracking
    - {lakehouse}.{schema}.pipelineRuns — Pipeline execution audit log
    
    Args:
        spark: Active SparkSession
        lakehouse: Lakehouse name (e.g., "lkhRaw"). If None, uses default attached lakehouse.
        schema: Schema name for control tables (default: "control")
        silent: If True, suppresses print output (used for auto-creation)
        
    Example:
        >>> from fabric_utils import setup_control_tables
        >>> setup_control_tables(spark, lakehouse="lkhRaw", schema="control")
    """
    # Build fully qualified schema name
    full_schema = f"{lakehouse}.{schema}" if lakehouse else schema
    
    # Create schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    
    # Watermark tracking table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_schema}.watermarks (
            tableName       STRING      NOT NULL,
            watermarkValue  TIMESTAMP,
            lastRunTs       TIMESTAMP,
            lastRunId       STRING,
            createdTs       TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    
    # Pipeline runs audit table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_schema}.pipelineRuns (
            runId               STRING      NOT NULL,
            pipelineName        STRING      NOT NULL,
            targetTable         STRING,
            status              STRING,
            strategy            STRING,
            rowsProcessed       BIGINT,
            rowsInserted        BIGINT,
            rowsUpdated         BIGINT,
            rowsDeleted         BIGINT,
            durationSeconds     DOUBLE,
            errorMessage        STRING,
            startedTs           TIMESTAMP,
            completedTs         TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    
    if not silent:
        print(f"✓ Control tables created in schema '{full_schema}':")
        print(f"  - {full_schema}.watermarks")
        print(f"  - {full_schema}.pipelineRuns")

