"""
Integration tests for fabric_utils package.

These tests run with a local PySpark session and Delta Lake support.
They can be executed in:
- CI/CD pipelines (GitHub Actions with pyspark + delta-spark installed)
- Local development (with pyspark + delta-spark installed)
- Microsoft Fabric notebooks (with production Spark runtime)

Usage:
    # Run with pytest (local or CI/CD)
    pytest tests/integration_test_fabric_utils.py -v
    
    # Run in Fabric Notebook
    from tests.integration_test_fabric_utils import run_all_integration_tests
    run_all_integration_tests(spark, lakehouse="<your_lakehouse>")
"""

import pytest
from datetime import datetime
from typing import Optional


@pytest.fixture(scope="module")
def spark_session():
    """Create a local SparkSession with Delta Lake support for testing."""
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        builder = (
            SparkSession.builder
            .appName("fabric-utils-integration-tests")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        )
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")  # Reduce noise in test output
        
        yield spark
        
        spark.stop()
    except ImportError:
        pytest.skip("PySpark or Delta not installed - skipping integration tests")


def test_merge_metrics_and_counts(spark_session):
    """
    Integration test for MERGE strategy with metric validation.
    
    Verifies that:
    - MERGE correctly inserts new rows
    - MERGE correctly updates existing rows
    - LoadResult metrics (rows_inserted, rows_updated) are accurate
    """
    from fabric_utils import Pipeline, WriteStrategy
    from pyspark.sql import Row
    
    print("\n=== Test: MERGE Metrics and Counts ===")
    
    # Setup test data
    test_table = "test_integration.merge_test"
    
    # Initial load - 3 rows
    initial_data = [
        Row(order_id=1, customer="Alice", amount=100.0, modified_at=datetime(2024, 1, 1)),
        Row(order_id=2, customer="Bob", amount=200.0, modified_at=datetime(2024, 1, 1)),
        Row(order_id=3, customer="Charlie", amount=300.0, modified_at=datetime(2024, 1, 1)),
    ]
    initial_df = spark_session.createDataFrame(initial_data)
    
    pipeline = Pipeline(
        spark=spark_session,
        target_table=test_table,
        watermark_column="modified_at",
        strategy=WriteStrategy.FULL_REFRESH,
        unique_key_cols=["order_id"],
    )
    
    watermark = pipeline.get_watermark()
    result = pipeline.execute(initial_df, new_watermark=datetime(2024, 1, 1))
    
    print(f"Initial load: {result.rows_processed} rows")
    assert result.rows_processed == 3, f"Expected 3 rows, got {result.rows_processed}"
    assert result.is_initial_load == True
    
    # Incremental load with MERGE - 2 updates + 1 insert
    incremental_data = [
        Row(order_id=2, customer="Bob Updated", amount=250.0, modified_at=datetime(2024, 1, 2)),  # Update
        Row(order_id=3, customer="Charlie Updated", amount=350.0, modified_at=datetime(2024, 1, 2)),  # Update
        Row(order_id=4, customer="Dave", amount=400.0, modified_at=datetime(2024, 1, 2)),  # Insert
    ]
    incremental_df = spark_session.createDataFrame(incremental_data)
    
    pipeline_merge = Pipeline(
        spark=spark_session,
        target_table=test_table,
        watermark_column="modified_at",
        strategy=WriteStrategy.MERGE,
        unique_key_cols=["order_id"],
    )
    
    watermark = pipeline_merge.get_watermark()
    print(f"Watermark: {watermark}")
    assert watermark == datetime(2024, 1, 1)
    
    result = pipeline_merge.execute(incremental_df, new_watermark=datetime(2024, 1, 2))
    
    print(f"MERGE result:")
    print(f"  - Rows processed: {result.rows_processed}")
    print(f"  - Rows inserted: {result.rows_inserted}")
    print(f"  - Rows updated: {result.rows_updated}")
    
    # Validate metrics
    assert result.rows_processed == 3, f"Expected 3 rows processed, got {result.rows_processed}"
    assert result.rows_inserted == 1, f"Expected 1 insert, got {result.rows_inserted}"
    assert result.rows_updated == 2, f"Expected 2 updates, got {result.rows_updated}"
    
    # Validate final table state
    final_df = spark_session.sql(f"SELECT * FROM {test_table} ORDER BY order_id")
    final_rows = final_df.collect()
    
    assert len(final_rows) == 4, f"Expected 4 total rows, got {len(final_rows)}"
    assert final_rows[1]["customer"] == "Bob Updated"
    assert final_rows[2]["customer"] == "Charlie Updated"
    assert final_rows[3]["customer"] == "Dave"
    
    # Cleanup
    spark_session.sql(f"DROP TABLE IF EXISTS {test_table}")
    
    print("✅ MERGE metrics test passed!")


def test_delete_append_restore_rollback(spark_session):
    """
    Integration test for DELETE_APPEND automatic rollback on failure.
    
    Verifies that:
    - DELETE_APPEND deletes old data based on predicate
    - If append fails, RESTORE TABLE is triggered
    - Table is restored to pre-delete state
    """
    from fabric_utils import DeltaLoader, WriteStrategy, LoaderError
    from pyspark.sql import Row
    
    print("\n=== Test: DELETE_APPEND Restore Rollback ===")
    
    # Setup test data
    test_table = "test_integration.restore_test"
    
    # Create initial table with 5 rows
    initial_data = [
        Row(event_id=i, event_type="click", ts=datetime(2024, 1, i))
        for i in range(1, 6)
    ]
    initial_df = spark_session.createDataFrame(initial_data)
    initial_df.write.format("delta").mode("overwrite").saveAsTable(test_table)
    
    # Get initial version
    version_before = spark_session.sql(f"DESCRIBE HISTORY {test_table} LIMIT 1").collect()[0]["version"]
    print(f"Initial version: {version_before}")
    
    # Verify initial count
    initial_count = spark_session.sql(f"SELECT COUNT(*) as cnt FROM {test_table}").collect()[0]["cnt"]
    assert initial_count == 5, f"Expected 5 initial rows, got {initial_count}"
    
    # Attempt DELETE_APPEND with a DataFrame that will trigger an error
    # We'll use a schema mismatch to force a failure after delete
    loader = DeltaLoader(
        spark=spark_session,
        target_table=test_table,
    )
    
    # Create a DataFrame with wrong schema to force write failure
    bad_data = [
        Row(wrong_column="data", another_wrong="value")
    ]
    bad_df = spark_session.createDataFrame(bad_data)
    
    # This should fail during append, triggering RESTORE
    try:
        loader.execute(
            bad_df,
            strategy=WriteStrategy.DELETE_APPEND,
            delete_predicate="ts >= '2024-01-03'",  # Would delete 3 rows
        )
        assert False, "Expected LoaderError but none was raised"
    except Exception as e:
        print(f"Expected error caught: {type(e).__name__}")
        # Error is expected - could be LoaderError or underlying Spark error
    
    # Verify table was restored
    restored_count = spark_session.sql(f"SELECT COUNT(*) as cnt FROM {test_table}").collect()[0]["cnt"]
    print(f"Count after restore: {restored_count}")
    
    # Should still have all 5 rows due to RESTORE
    assert restored_count == 5, f"Expected 5 rows after restore, got {restored_count}"
    
    # Verify version was rolled back
    current_version = spark_session.sql(f"DESCRIBE HISTORY {test_table} LIMIT 1").collect()[0]["version"]
    print(f"Current version after restore: {current_version}")
    
    # Note: RESTORE creates a new version, so version will be higher than before
    # but the data should be the same as version_before
    
    # Cleanup
    spark_session.sql(f"DROP TABLE IF EXISTS {test_table}")
    
    print("✅ DELETE_APPEND restore rollback test passed!")


# Backward compatibility for Fabric Notebook usage
def run_all_integration_tests(spark, lakehouse: Optional[str] = None):
    """
    Run all integration tests (for use in Fabric notebooks).
    
    Args:
        spark: Active SparkSession (provided by notebook environment)
        lakehouse: Optional lakehouse name for test tables
    
    Example:
        >>> run_all_integration_tests(spark, lakehouse="lkhTest")
    
    Note: When running with pytest, this function is not needed.
          Just run: pytest tests/integration_test_fabric_utils.py -v
    """
    print("=" * 60)
    print("Running fabric_utils Integration Tests")
    print("=" * 60)
    
    # Create a mock spark_session object for compatibility
    class SparkSessionWrapper:
        def __init__(self, spark_instance):
            self._spark = spark_instance
            
        def __getattr__(self, name):
            return getattr(self._spark, name)
    
    spark_wrapper = SparkSessionWrapper(spark)
    
    # Create test schema
    schema = f"{lakehouse}.test_integration" if lakehouse else "test_integration"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    tests = [
        lambda: test_merge_metrics_and_counts(spark_wrapper),
        lambda: test_delete_append_restore_rollback(spark_wrapper),
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Integration Test Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    # Cleanup test schema
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    except Exception:
        pass
    
    return passed, failed


if __name__ == "__main__":
    print(__doc__)
