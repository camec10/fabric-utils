"""
Setup utilities for control tables and schema initialization.
"""


def setup_control_tables(spark, control_lakehouse: str = None, schema: str = "control", silent: bool = False) -> None:
    """
    Create control tables for table metadata, watermark tracking, and optimization.
    
    Creates the following tables:
    - {control_lakehouse}.{schema}.tableRegistry — Table metadata, watermarks, and optimization config
    - {control_lakehouse}.{schema}.pipelineRuns — Pipeline execution audit log
    
    Args:
        spark: Active SparkSession
        control_lakehouse: Lakehouse name for control tables (e.g., "lkhControl"). If None, uses default attached lakehouse.
        schema: Schema name for control tables (default: "control")
        silent: If True, suppresses print output (used for auto-creation)
        
    Example:
        >>> from fabric_utils import setup_control_tables
        >>> setup_control_tables(spark, control_lakehouse="lkhControl", schema="control")
    """
    # Build fully qualified schema name
    full_schema = f"{control_lakehouse}.{schema}" if control_lakehouse else schema
    
    # Create schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    
    # Table registry (replaces watermarks table)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_schema}.tableRegistry (
            tableName                   STRING      NOT NULL,
            createdTimestamp            TIMESTAMP   NOT NULL,
            updatedTimestamp            TIMESTAMP   NOT NULL,
            watermarkValue              TIMESTAMP,
            watermarkColumn             STRING,
            lookbackDays                INT,
            optimizationScheduleDays    INT,
            lastOptimizedTimestamp      TIMESTAMP,
            lastRunId                   STRING,
            lastRunTimestamp            TIMESTAMP
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
            startedTimestamp    TIMESTAMP,
            completedTimestamp  TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    
    if not silent:
        print(f"✓ Control tables created in schema '{full_schema}':")
        print(f"  - {full_schema}.tableRegistry")
        print(f"  - {full_schema}.pipelineRuns")

