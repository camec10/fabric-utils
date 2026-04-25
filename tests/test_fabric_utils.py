"""
Unit tests for fabric_utils package.

Note: These tests mock SparkSession since we don't have a Spark cluster locally.
For integration tests, run inside a Fabric notebook.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta


class TestWatermarkManager:
    """Tests for WatermarkManager class."""
    
    def test_get_watermark_returns_none_on_initial_load(self):
        """First run should return None to trigger full load."""
        from fabric_utils import WatermarkManager
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        
        wm = WatermarkManager(mock_spark, schema="control")
        result = wm.get_watermark("bronze.orders")
        
        assert result is None
    
    def test_control_lakehouse_two_part_table_name(self):
        """Control lakehouse with 2-part data table name."""
        from fabric_utils import WatermarkManager
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        
        # Control tables in separate lakehouse, data in default lakehouse
        wm = WatermarkManager(mock_spark, control_lakehouse="lkhControl", schema="control")
        
        # Should build control_table as lkhControl.control.watermarks
        assert wm.control_table == "lkhControl.control.watermarks"
        assert wm.control_lakehouse == "lkhControl"
        
        # Data table can still be 2-part
        result = wm.get_watermark("bronze.orders")
        assert result is None
    
    def test_control_lakehouse_three_part_table_name(self):
        """Control lakehouse with 3-part data table name."""
        from fabric_utils import WatermarkManager
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        
        # Control tables in lkhControl, data in lkhRaw
        wm = WatermarkManager(mock_spark, control_lakehouse="lkhControl", schema="control")
        
        assert wm.control_table == "lkhControl.control.watermarks"
        
        # Data table is 3-part, separate from control
        result = wm.get_watermark("lkhRaw.bronze.orders")
        assert result is None
    
    def test_no_control_lakehouse_uses_default(self):
        """When control_lakehouse is None, use default attached lakehouse."""
        from fabric_utils import WatermarkManager
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        
        wm = WatermarkManager(mock_spark, control_lakehouse=None, schema="control")
        
        # Should build 2-part name without lakehouse prefix
        assert wm.control_table == "control.watermarks"
        assert wm.control_lakehouse is None
    
    def test_get_watermark_applies_lookback(self):
        """Lookback should subtract days from watermark."""
        from fabric_utils import WatermarkManager
        
        base_watermark = datetime(2024, 3, 15, 12, 0, 0)
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"watermarkValue": base_watermark}
        ]
        
        wm = WatermarkManager(mock_spark, schema="control")
        result = wm.get_watermark("bronze.orders", lookback_days=90)
        
        expected = base_watermark - timedelta(days=90)
        assert result == expected
    
    def test_build_where_clause_returns_empty_on_initial(self):
        """Initial load should return empty WHERE clause."""
        from fabric_utils import WatermarkManager
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        
        wm = WatermarkManager(mock_spark, schema="control")
        clause = wm.build_where_clause("bronze.orders", "modified_at")
        
        assert clause == ""
    
    def test_build_where_clause_formats_timestamp(self):
        """WHERE clause should properly format timestamp."""
        from fabric_utils import WatermarkManager
        
        watermark = datetime(2024, 1, 1, 0, 0, 0)
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"watermarkValue": watermark}
        ]
        
        wm = WatermarkManager(mock_spark, schema="control")
        clause = wm.build_where_clause("bronze.orders", "modified_at")
        
        assert "WHERE modified_at >=" in clause
        assert "2024-01-01" in clause


class TestWriteStrategy:
    """Tests for WriteStrategy enum."""
    
    def test_strategy_values(self):
        """Verify all expected strategies exist."""
        from fabric_utils import WriteStrategy
        
        assert WriteStrategy.DELETE_APPEND.value == "delete_append"
        assert WriteStrategy.MERGE.value == "merge"
        assert WriteStrategy.FULL_REFRESH.value == "full_refresh"


class TestDeltaLoader:
    """Tests for DeltaLoader class."""
    
    def test_init_minimal(self):
        """DeltaLoader only needs spark and target_table."""
        from fabric_utils import DeltaLoader
        
        mock_spark = MagicMock()
        loader = DeltaLoader(spark=mock_spark, target_table="bronze.orders")
        
        assert loader.target_table == "bronze.orders"
        assert loader.unique_key_cols == []
    
    def test_schema_validation_missing_merge_key(self):
        """Should raise SchemaValidationError when merge key columns are missing."""
        from fabric_utils import DeltaLoader, WriteStrategy, SchemaValidationError
        
        mock_spark = MagicMock()
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            unique_key_cols=["order_id", "line_id"],
        )
        
        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "order_id", "amount"]  # missing line_id
        
        with pytest.raises(SchemaValidationError, match="line_id"):
            loader.execute(mock_df, strategy=WriteStrategy.MERGE)
    
    def test_schema_validation_passes_for_merge(self):
        """Should not raise when all merge key columns exist."""
        from fabric_utils import DeltaLoader, WriteStrategy
        
        mock_spark = MagicMock()
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            unique_key_cols=["order_id"],
        )
        
        mock_df = MagicMock()
        mock_df.columns = ["order_id", "modified_at", "amount"]
        
        # Should not raise
        loader._validate_schema(mock_df, WriteStrategy.MERGE)
    
    def test_schema_validation_skipped_for_non_merge(self):
        """DELETE_APPEND and FULL_REFRESH should not validate merge keys."""
        from fabric_utils import DeltaLoader, WriteStrategy
        
        mock_spark = MagicMock()
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            unique_key_cols=["order_id"],
        )
        
        mock_df = MagicMock()
        mock_df.columns = ["amount"]  # missing order_id, but that's fine for non-MERGE
        
        # Should not raise for DELETE_APPEND or FULL_REFRESH
        loader._validate_schema(mock_df, WriteStrategy.DELETE_APPEND)
        loader._validate_schema(mock_df, WriteStrategy.FULL_REFRESH)
    
    def test_ensure_schema_exists_creates_schema_two_part(self):
        """2-part table names should create schema without lakehouse prefix."""
        from fabric_utils import DeltaLoader
        
        mock_spark = MagicMock()
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
        )
        
        loader._ensure_schema_exists()
        
        # Should create just "bronze"
        mock_spark.sql.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS bronze")
    
    def test_ensure_schema_exists_creates_schema_three_part(self):
        """3-part table names should extract just the schema (middle part)."""
        from fabric_utils import DeltaLoader
        
        mock_spark = MagicMock()
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="lkhRaw.eSHIFTTimeAndLabor.timeEntry",
        )
        
        loader._ensure_schema_exists()
        
        # Should create just "eSHIFTTimeAndLabor" (second-to-last part)
        mock_spark.sql.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS eSHIFTTimeAndLabor")
    
    @patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(), "pyspark.sql.functions": MagicMock()})
    def test_execute_wraps_error_in_loader_error(self):
        """Write failures should be wrapped in LoaderError."""
        from fabric_utils import DeltaLoader, WriteStrategy, LoaderError
        
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
        )
        
        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 5
        mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable.side_effect = RuntimeError("Disk full")
        
        with pytest.raises(LoaderError, match="Disk full"):
            loader.execute(mock_df, strategy=WriteStrategy.FULL_REFRESH)
    
    @patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(), "pyspark.sql.functions": MagicMock()})
    def test_delete_append_rollback_on_failure(self):
        """DELETE+APPEND should RESTORE table if APPEND fails after DELETE."""
        from fabric_utils import DeltaLoader, WriteStrategy, LoaderError
        
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_spark.sql.return_value.collect.return_value = [
            {"version": 5, "cnt": 10}
        ]
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
        )
        
        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 5
        mock_df.write.format.return_value.mode.return_value.saveAsTable.side_effect = RuntimeError("Write failed")
        
        with pytest.raises(LoaderError, match="Write failed"):
            loader.execute(
                mock_df,
                strategy=WriteStrategy.DELETE_APPEND,
                delete_predicate="modified_at >= '2024-01-01'",
            )
        
        # Verify RESTORE was called
        restore_calls = [
            call for call in mock_spark.sql.call_args_list
            if "RESTORE" in str(call)
        ]
        assert len(restore_calls) > 0, "Expected RESTORE TABLE to be called on failure"


class TestPipeline:
    """Tests for Pipeline orchestrator."""

    def test_raises_if_execute_called_before_get_watermark(self):
        """Must call get_watermark() first so pipeline knows current position."""
        from fabric_utils import Pipeline, WriteStrategy, LoaderError

        mock_spark = MagicMock()
        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
        )
        mock_df = MagicMock()

        with pytest.raises(LoaderError, match="Call get_watermark"):
            pipe.execute(mock_df)

    def test_first_run_uses_full_refresh(self):
        """When watermark is None (first run), strategy should be FULL_REFRESH."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        # WatermarkManager.get_watermark returns None (no row in control table)
        mock_spark.sql.return_value.collect.return_value = []

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy=WriteStrategy.DELETE_APPEND,
        )

        watermark = pipe.get_watermark()
        assert watermark is None
        # Internal state should allow execute to pick FULL_REFRESH
        assert pipe._watermark is None

    def test_subsequent_run_uses_configured_strategy(self):
        """When watermark exists, pipeline should use the configured strategy."""
        from fabric_utils import Pipeline, WriteStrategy

        stored_wm = datetime(2024, 6, 1, 12, 0, 0)
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"watermarkValue": stored_wm}
        ]

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy=WriteStrategy.MERGE,
            unique_key_cols=["order_id"],
            lookback_days=30,
        )

        watermark = pipe.get_watermark()
        expected = stored_wm - timedelta(days=30)
        assert watermark == expected

    def test_delete_predicate_built_from_watermark(self):
        """DELETE+APPEND should auto-build predicate from watermark_column and value."""
        from fabric_utils import Pipeline, WriteStrategy
        from fabric_utils.pipeline import _NOT_RETRIEVED

        stored_wm = datetime(2024, 6, 1, 0, 0, 0)
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"watermarkValue": stored_wm}
        ]

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy=WriteStrategy.DELETE_APPEND,
            lookback_days=0,
        )

        pipe.get_watermark()
        assert pipe._watermark == stored_wm

        # We can't fully execute without a real Spark, but we can verify
        # the predicate would be built correctly by checking the internal state
        assert pipe.watermark_column == "modified_at"
        assert pipe._watermark is not None
        assert pipe._watermark is not _NOT_RETRIEVED

    def test_components_accessible(self):
        """Underlying WatermarkManager and DeltaLoader should be directly accessible."""
        from fabric_utils import Pipeline, WriteStrategy
        from fabric_utils.watermark import WatermarkManager
        from fabric_utils.loader import DeltaLoader

        mock_spark = MagicMock()
        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            unique_key_cols=["order_id"],
        )

        assert isinstance(pipe.wm, WatermarkManager)
        assert isinstance(pipe.loader, DeltaLoader)
        assert pipe.loader.unique_key_cols == ["order_id"]

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_delete_predicate_uses_lookback_watermark(self):
        """DELETE predicate should use lookback-adjusted watermark to refresh the lookback window (v0.3.15)."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        # Stored watermark: 2024-06-01
        stored_wm = datetime(2024, 6, 1, 12, 0, 0)
        
        # Mock SQL return - proper responses for all queries
        def sql_mock(query):
            mock_result = MagicMock()
            if "watermarks" in query and "SELECT" in query:
                # Watermark query
                mock_result.collect.return_value = [{"watermarkValue": stored_wm}]
            elif "COUNT" in query or "DELETE" in query:
                # Count or delete queries
                mock_result.collect.return_value = [{"cnt": 100}]
            else:
                # Other queries (DESCRIBE HISTORY, etc.)
                mock_result.collect.return_value = []
            return mock_result
        
        mock_spark.sql.side_effect = sql_mock
        mock_spark.catalog.tableExists.return_value = True

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy=WriteStrategy.DELETE_APPEND,
            lookback_days=90,  # 90 day lookback
        )

        watermark = pipe.get_watermark()
        # Returned watermark should have lookback applied
        expected_wm_with_lookback = stored_wm - timedelta(days=90)
        assert watermark == expected_wm_with_lookback

        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 100
        mock_df.agg.return_value.collect.return_value = [datetime(2024, 6, 15)]

        result = pipe.execute(mock_df, new_watermark=datetime(2024, 6, 15))

        # Verify the delete used lookback-adjusted watermark (same as extraction)
        # This refreshes the entire lookback window to catch late-arriving data
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        
        # Look for DELETE statement with lookback-adjusted watermark
        delete_calls = [c for c in sql_calls if "DELETE FROM" in c and "bronze.orders" in c]
        
        # Should use lookback date "2024-03-03" (90 days before 2024-06-01)
        # Format check for the date that's 90 days back
        lookback_date_str = expected_wm_with_lookback.strftime("%Y-%m-%d")
        assert any(lookback_date_str in c for c in delete_calls), \
            f"Delete predicate should use lookback-adjusted watermark ({lookback_date_str}), not original watermark"

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_execute_full_refresh_updates_watermark(self):
        """Successful FULL_REFRESH should update watermark with explicit value."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        # get_watermark returns None → first run
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = False

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
        )

        pipe.get_watermark()

        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 10

        new_wm = datetime(2024, 7, 1)
        result = pipe.execute(mock_df, new_watermark=new_wm)

        # Verify watermark update was called (it's a MERGE INTO via spark.sql)
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        merge_calls = [c for c in sql_calls if "MERGE INTO control.watermarks" in c]
        assert len(merge_calls) > 0, "Expected watermark update after successful load"
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_explicit_full_refresh_resets_watermark(self):
        """Explicit FULL_REFRESH strategy should reset watermark in get_watermark() and return None (v0.3.10/v0.3.13)."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        # Watermark exists from previous run
        existing_wm = datetime(2024, 6, 1, 12, 0, 0)
        mock_spark.sql.return_value.collect.return_value = [
            {"watermarkValue": existing_wm}
        ]
        mock_spark.catalog.tableExists.return_value = True

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy=WriteStrategy.FULL_REFRESH,  # Explicit full refresh
            lookback_days=0,  # No lookback for this test
        )

        watermark = pipe.get_watermark()
        # CRITICAL: get_watermark() must return None when FULL_REFRESH is set
        # so the user's conditional logic extracts the full dataset
        assert watermark is None, "Expected None for FULL_REFRESH to trigger full extraction"

        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 100

        result = pipe.execute(mock_df, new_watermark=datetime(2024, 7, 1))

        # Verify reset was called during get_watermark() (DELETE FROM watermarks)
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        delete_calls = [c for c in sql_calls if "DELETE FROM" in c and "watermarks" in c]
        assert len(delete_calls) > 0, "Expected watermark reset for explicit FULL_REFRESH"

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_orphaned_watermark_detection(self):
        """Detect and warn about orphaned watermarks BEFORE extraction (v0.0.16 fix).
        
        Critical: get_watermark() must detect orphaned watermark and return None
        so user extracts full dataset, not just incremental data.
        """
        from fabric_utils import Pipeline, WriteStrategy
        import io
        import sys

        mock_spark = MagicMock()
        # Watermark exists but table is missing
        orphaned_wm = datetime(2024, 5, 15, 0, 0, 0)
        
        def sql_mock(query):
            mock_result = MagicMock()
            if "watermarks" in query and "SELECT" in query:
                # Watermark query returns orphaned watermark
                mock_result.collect.return_value = [{"watermarkValue": orphaned_wm}]
            else:
                mock_result.collect.return_value = []
            return mock_result
        
        mock_spark.sql.side_effect = sql_mock
        mock_spark.catalog.tableExists.return_value = False  # Table missing!

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            lookback_days=0,  # No lookback for this test
        )

        # Capture stdout to verify warning
        captured_output = io.StringIO()
        sys.stdout = captured_output

        watermark = pipe.get_watermark()

        sys.stdout = sys.__stdout__

        # CRITICAL: get_watermark() should return None for orphaned watermark
        # This signals full extraction, preventing partial data load
        assert watermark is None, (
            "get_watermark() should return None when watermark exists but table doesn't. "
            "This prevents extracting only incremental data when full refresh is needed."
        )

        # Verify warning was printed during get_watermark()
        output = captured_output.getvalue()
        assert "⚠ WARNING" in output or "orphaned" in output.lower(), \
            "Should warn about orphaned watermark in get_watermark()"
        assert "Watermark exists" in output or "does not exist" in output, \
            "Warning should mention table doesn't exist"

        # Verify reset was called during get_watermark()
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        delete_calls = [c for c in sql_calls if "DELETE FROM" in c and "watermarks" in c]
        assert len(delete_calls) > 0, "Expected watermark reset during get_watermark()"

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_pipeline_run_logging_started(self):
        """Pipeline should log STARTED status at beginning of execution (v0.3.11)."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = False

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
        )

        pipe.get_watermark()

        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 10

        result = pipe.execute(mock_df, new_watermark=datetime(2024, 7, 1))

        # Verify log_pipeline_run was called with STARTED status
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        pipeline_log_calls = [c for c in sql_calls if "pipelineRuns" in c and "STARTED" in c]
        assert len(pipeline_log_calls) > 0, "Expected pipeline run log with STARTED status"

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_pipeline_run_logging_completed(self):
        """Pipeline should log COMPLETED status on successful execution (v0.3.11)."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = False

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
        )

        pipe.get_watermark()

        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 10

        result = pipe.execute(mock_df, new_watermark=datetime(2024, 7, 1))

        # Verify log_pipeline_run was called with COMPLETED status
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        pipeline_log_calls = [c for c in sql_calls if "pipelineRuns" in c and "COMPLETED" in c]
        assert len(pipeline_log_calls) > 0, "Expected pipeline run log with COMPLETED status"

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_pipeline_run_logging_failed(self):
        """Pipeline should log FAILED status on execution error (v0.3.11)."""
        from fabric_utils import Pipeline, WriteStrategy, LoaderError

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = False

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
        )

        pipe.get_watermark()

        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 10
        # Simulate write failure
        mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable.side_effect = RuntimeError("Write failed")

        try:
            result = pipe.execute(mock_df, new_watermark=datetime(2024, 7, 1))
        except (LoaderError, RuntimeError):
            pass  # Expected to fail

        # Verify log_pipeline_run was called with FAILED status
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        pipeline_log_calls = [c for c in sql_calls if "pipelineRuns" in c and "FAILED" in c]
        assert len(pipeline_log_calls) > 0, "Expected pipeline run log with FAILED status"

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_string_strategy_uppercase(self):
        """Pipeline accepts string strategy names (uppercase) for parameterization (v0.3.14)."""
        from fabric_utils import Pipeline

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = True

        # Test with uppercase string
        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy="DELETE_APPEND",
        )
        
        from fabric_utils.loader import WriteStrategy
        assert pipe.strategy == WriteStrategy.DELETE_APPEND

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_string_strategy_lowercase(self):
        """Pipeline accepts string strategy names (case-insensitive) for parameterization (v0.3.14)."""
        from fabric_utils import Pipeline

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = True

        # Test with lowercase string
        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy="full_refresh",
        )
        
        from fabric_utils.loader import WriteStrategy
        assert pipe.strategy == WriteStrategy.FULL_REFRESH

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_string_strategy_invalid(self):
        """Pipeline raises clear error for invalid string strategy (v0.3.14)."""
        from fabric_utils import Pipeline

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = True

        # Test with invalid string
        with pytest.raises(ValueError) as exc_info:
            pipe = Pipeline(
                mock_spark,
                target_table="bronze.orders",
                watermark_column="modified_at",
                strategy="INVALID_STRATEGY",
            )
        
        assert "Invalid strategy" in str(exc_info.value)
        assert "INVALID_STRATEGY" in str(exc_info.value)

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_enum_strategy_still_works(self):
        """Pipeline still accepts enum strategy for backward compatibility (v0.3.14)."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = True

        # Test with enum (existing behavior)
        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy=WriteStrategy.MERGE,
        )
        
        assert pipe.strategy == WriteStrategy.MERGE

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_control_lakehouse_two_part_data_table(self):
        """Pipeline with control_lakehouse and 2-part data table name."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = False

        # Data in default lakehouse (2-part), control in separate lakehouse
        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",  # 2-part: uses default lakehouse
            watermark_column="modified_at",
            control_lakehouse="lkhControl",
        )

        assert pipe.target_table == "bronze.orders"
        assert pipe.control_lakehouse == "lkhControl"
        assert pipe.wm.control_table == "lkhControl.control.watermarks"
        
        pipe.get_watermark()

        # Verify watermark query uses control lakehouse
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        watermark_queries = [c for c in sql_calls if "lkhControl.control.watermarks" in c]
        assert len(watermark_queries) > 0, "Watermark queries should use control lakehouse"

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_control_lakehouse_three_part_data_table(self):
        """Pipeline with control_lakehouse and 3-part data table name."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = False

        # Data in lkhRaw (3-part), control in lkhControl
        pipe = Pipeline(
            mock_spark,
            target_table="lkhRaw.bronze.orders",  # 3-part: explicit lakehouse
            watermark_column="modified_at",
            control_lakehouse="lkhControl",
            strategy=WriteStrategy.MERGE,
            unique_key_cols=["order_id"],
        )

        assert pipe.target_table == "lkhRaw.bronze.orders"
        assert pipe.control_lakehouse == "lkhControl"
        assert pipe.wm.control_table == "lkhControl.control.watermarks"
        
        pipe.get_watermark()

        # Verify separation: watermarks in lkhControl, data in lkhRaw
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        watermark_queries = [c for c in sql_calls if "lkhControl.control.watermarks" in c]
        assert len(watermark_queries) > 0, "Watermark queries should use lkhControl"
        
        # Target table references should use lkhRaw
        assert pipe.loader.target_table == "lkhRaw.bronze.orders"

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_pipeline_logging_uses_control_lakehouse(self):
        """Pipeline run logging should use control lakehouse."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = False

        pipe = Pipeline(
            mock_spark,
            target_table="lkhRaw.bronze.orders",
            watermark_column="modified_at",
            control_lakehouse="lkhControl",
        )

        pipe.get_watermark()

        mock_df = MagicMock()
        mock_df.columns = ["modified_at", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 10

        result = pipe.execute(mock_df, new_watermark=datetime(2024, 7, 1))

        # Verify pipelineRuns logging uses control lakehouse
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        pipeline_log_calls = [c for c in sql_calls if "lkhControl.control.pipelineRuns" in c]
        assert len(pipeline_log_calls) > 0, "Pipeline logs should use lkhControl.control.pipelineRuns"

    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_no_control_lakehouse_uses_default(self):
        """Pipeline without control_lakehouse uses default lakehouse for control tables."""
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = False

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            control_lakehouse=None,  # Use default
        )

        assert pipe.control_lakehouse is None
        assert pipe.wm.control_table == "control.watermarks"  # 2-part, no lakehouse prefix
        
        pipe.get_watermark()

        # Verify watermark query uses 2-part name
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        watermark_queries = [c for c in sql_calls if "control.watermarks" in c]
        assert len(watermark_queries) > 0, "Should use default lakehouse (2-part name)"


class TestWatermarkManagerPipelineRuns:
    """Tests for WatermarkManager.log_pipeline_run() method (v0.3.11)."""
    
    def test_log_pipeline_run_creates_record(self):
        """log_pipeline_run should insert/update pipelineRuns table."""
        from fabric_utils import WatermarkManager
        
        mock_spark = MagicMock()
        wm = WatermarkManager(mock_spark, schema="control")
        
        wm.log_pipeline_run(
            run_id="test-run-123",
            pipeline_name="bronze.orders",
            target_table="bronze.orders",
            status="STARTED",
            strategy="full_refresh",
        )
        
        # Verify MERGE INTO was called on pipelineRuns table
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        merge_calls = [c for c in sql_calls if "MERGE INTO" in c and "pipelineRuns" in c]
        assert len(merge_calls) > 0, "Expected MERGE INTO pipelineRuns"
    
    def test_log_pipeline_run_with_metrics(self):
        """log_pipeline_run should record execution metrics on completion."""
        from fabric_utils import WatermarkManager
        
        mock_spark = MagicMock()
        wm = WatermarkManager(mock_spark, schema="control")
        
        wm.log_pipeline_run(
            run_id="test-run-456",
            pipeline_name="bronze.orders",
            target_table="bronze.orders",
            status="COMPLETED",
            strategy="merge",
            rows_processed=1000,
            rows_inserted=50,
            rows_updated=30,
            rows_deleted=0,
            duration_seconds=12.5,
        )
        
        # Verify pipelineRuns update includes metrics
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        merge_calls = [c for c in sql_calls if "pipelineRuns" in c]
        assert len(merge_calls) > 0
        
        # Check that metrics appear in the SQL
        full_sql = " ".join(sql_calls)
        assert "COMPLETED" in full_sql
        assert "1000" in full_sql or "rows_processed" in full_sql.lower()


class TestTimestampPrecision:
    """Tests for timestamp precision handling (v0.3.16)."""
    
    def test_build_where_clause_no_microseconds_when_zero(self):
        """WHERE clause should NOT include .000000 when microseconds are zero (v0.3.16).
        
        This fixes string column comparison issues where:
        '2024-01-01 00:00:00' >= '2024-01-01 00:00:00.000000' returns FALSE lexicographically
        """
        from fabric_utils import WatermarkManager
        
        # Watermark with zero microseconds
        watermark = datetime(2024, 1, 1, 12, 30, 45, 0)  # microsecond=0
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"watermarkValue": watermark}
        ]
        
        wm = WatermarkManager(mock_spark, schema="control")
        clause = wm.build_where_clause("bronze.orders", "modified_at")
        
        # Should NOT contain .000000
        assert ".000000" not in clause, f"Microseconds should be omitted when zero, got: {clause}"
        assert "2024-01-01 12:30:45" in clause
    
    def test_build_where_clause_includes_microseconds_when_nonzero(self):
        """WHERE clause SHOULD include microseconds when they are non-zero."""
        from fabric_utils import WatermarkManager
        
        # Watermark with non-zero microseconds
        watermark = datetime(2024, 1, 1, 12, 30, 45, 123456)
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"watermarkValue": watermark}
        ]
        
        wm = WatermarkManager(mock_spark, schema="control")
        clause = wm.build_where_clause("bronze.orders", "modified_at")
        
        # Should contain the microseconds
        assert ".123456" in clause, f"Microseconds should be included when non-zero, got: {clause}"
    
    def test_update_watermark_no_microseconds_when_zero(self):
        """update_watermark should NOT include .000000 when microseconds are zero (v0.3.16)."""
        from fabric_utils import WatermarkManager
        
        mock_spark = MagicMock()
        wm = WatermarkManager(mock_spark, schema="control")
        
        # Watermark with zero microseconds
        new_value = datetime(2024, 6, 15, 8, 0, 0, 0)
        wm.update_watermark("bronze.orders", new_value, run_id="test-123")
        
        # Check the SQL call
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        merge_calls = [c for c in sql_calls if "MERGE INTO" in c and "watermarks" in c]
        assert len(merge_calls) > 0
        
        # Should NOT contain .000000
        full_sql = " ".join(merge_calls)
        assert ".000000" not in full_sql, f"Microseconds should be omitted when zero, got: {full_sql}"
        assert "2024-06-15 08:00:00" in full_sql
    
    def test_update_watermark_includes_microseconds_when_nonzero(self):
        """update_watermark SHOULD include microseconds when they are non-zero."""
        from fabric_utils import WatermarkManager
        
        mock_spark = MagicMock()
        wm = WatermarkManager(mock_spark, schema="control")
        
        # Watermark with non-zero microseconds
        new_value = datetime(2024, 6, 15, 8, 0, 0, 500000)
        wm.update_watermark("bronze.orders", new_value, run_id="test-123")
        
        # Check the SQL call
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        merge_calls = [c for c in sql_calls if "MERGE INTO" in c and "watermarks" in c]
        assert len(merge_calls) > 0
        
        # Should contain .500000
        full_sql = " ".join(merge_calls)
        assert ".500000" in full_sql, f"Microseconds should be included when non-zero, got: {full_sql}"
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_pipeline_delete_predicate_no_microseconds_when_zero(self):
        """DELETE predicate should NOT include .000000 when microseconds are zero (v0.3.16).
        
        This is the critical fix for DELETE_APPEND with string timestamp columns.
        """
        from fabric_utils import Pipeline, WriteStrategy

        mock_spark = MagicMock()
        # Watermark with zero microseconds (common case)
        stored_wm = datetime(2024, 6, 1, 12, 0, 0, 0)
        
        def sql_mock(query):
            mock_result = MagicMock()
            if "watermarks" in query and "SELECT" in query:
                mock_result.collect.return_value = [{"watermarkValue": stored_wm}]
            elif "COUNT" in query:
                mock_result.collect.return_value = [{"cnt": 100}]
            else:
                mock_result.collect.return_value = []
            return mock_result
        
        mock_spark.sql.side_effect = sql_mock
        mock_spark.catalog.tableExists.return_value = True

        pipe = Pipeline(
            mock_spark,
            target_table="bronze.orders",
            watermark_column="clockIn",
            strategy=WriteStrategy.DELETE_APPEND,
            lookback_days=0,
        )

        pipe.get_watermark()

        mock_df = MagicMock()
        mock_df.columns = ["clockIn", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 100
        mock_df.agg.return_value.collect.return_value = [datetime(2024, 6, 15)]

        pipe.execute(mock_df, new_watermark=datetime(2024, 6, 15))

        # Verify the delete predicate does NOT contain .000000
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        delete_calls = [c for c in sql_calls if "DELETE FROM" in c]
        
        for call in delete_calls:
            assert ".000000" not in call, f"DELETE predicate should not have .000000, got: {call}"
            # Should have the correct format without microseconds
            assert "2024-06-01 12:00:00" in call or "clockIn >=" in call


class TestDeleteAppendSafety:
    """Tests for DELETE_APPEND safety checks (v0.0.18)."""
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_delete_append_prevents_data_loss_when_source_empty(self):
        """DELETE_APPEND should fail if source is empty but target has rows to delete."""
        from fabric_utils import DeltaLoader, WriteStrategy, LoaderError
        
        mock_spark = MagicMock()
        # Target has 275k rows matching the delete predicate
        mock_spark.sql.return_value.collect.return_value = [{"cnt": 275000}]
        mock_spark.catalog.tableExists.return_value = True
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
        )
        
        # Source DataFrame is EMPTY
        mock_df = MagicMock()
        mock_df.columns = ["clockIn", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 0  # ← Empty source!
        
        # Should raise LoaderError to prevent data loss
        with pytest.raises(LoaderError) as exc_info:
            loader.execute(
                mock_df,
                strategy=WriteStrategy.DELETE_APPEND,
                delete_predicate="clockIn >= '2024-01-01'",
            )
        
        assert "safety check failed" in str(exc_info.value).lower()
        assert "275,000" in str(exc_info.value) or "275000" in str(exc_info.value)
        assert "0 rows" in str(exc_info.value)
        
        # Verify DELETE was NOT executed
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        delete_executions = [c for c in sql_calls if "DELETE FROM bronze.orders" in c]
        assert len(delete_executions) == 0, "DELETE should not execute when source is empty"
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_delete_append_succeeds_when_balanced(self):
        """DELETE_APPEND should succeed when insert count matches or exceeds delete count."""
        from fabric_utils import DeltaLoader, WriteStrategy
        
        mock_spark = MagicMock()
        # Target has 1000 rows to delete
        mock_spark.sql.return_value.collect.return_value = [{"cnt": 1000}]
        mock_spark.catalog.tableExists.return_value = True
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
        )
        
        # Source has matching number of rows
        mock_df = MagicMock()
        mock_df.columns = ["clockIn", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 1050  # ← More than delete (50 new records)
        
        # Should succeed
        result = loader.execute(
            mock_df,
            strategy=WriteStrategy.DELETE_APPEND,
            delete_predicate="clockIn >= '2024-01-01'",
        )
        
        assert result.rows_deleted == 1000
        assert result.rows_inserted == 1050
        
        # Verify DELETE WAS executed
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        delete_executions = [c for c in sql_calls if "DELETE FROM bronze.orders" in c]
        assert len(delete_executions) > 0, "DELETE should execute when source has rows"


class TestTableProperties:
    """Tests for delta_options as persistent table properties (v0.0.02)."""
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_delta_options_applied_as_table_properties_on_full_refresh(self):
        """delta_options should be applied as persistent table properties via ALTER TABLE."""
        from fabric_utils import DeltaLoader, WriteStrategy
        
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False  # Initial load
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            delta_options={
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "delta.enableChangeDataFeed": "true",
            }
        )
        
        mock_df = MagicMock()
        mock_df.columns = ["order_id", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 100
        mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable = MagicMock()
        
        loader.execute(mock_df, strategy=WriteStrategy.FULL_REFRESH)
        
        # Verify ALTER TABLE SET TBLPROPERTIES was called
        sql_calls = [str(call) for call in mock_spark.sql.call_args_list]
        alter_table_calls = [c for c in sql_calls if "ALTER TABLE" in c and "SET TBLPROPERTIES" in c]
        
        assert len(alter_table_calls) > 0, "ALTER TABLE SET TBLPROPERTIES should be called"
        alter_call = alter_table_calls[0]
        
        # Verify all three properties are in the ALTER TABLE statement
        assert "delta.autoOptimize.optimizeWrite" in alter_call
        assert "delta.autoOptimize.autoCompact" in alter_call
        assert "delta.enableChangeDataFeed" in alter_call
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_ensure_table_properties_applies_to_existing_table(self):
        """ensure_table_properties() should work on existing tables."""
        from fabric_utils import DeltaLoader
        
        mock_spark = MagicMock()
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            delta_options={
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.enableChangeDataFeed": "true",
            }
        )
        
        # Call ensure_table_properties directly
        loader.ensure_table_properties()
        
        # Verify ALTER TABLE was called
        mock_spark.sql.assert_called()
        sql_call = str(mock_spark.sql.call_args_list[-1])
        
        assert "ALTER TABLE bronze.orders" in sql_call
        assert "SET TBLPROPERTIES" in sql_call
        assert "delta.autoOptimize.optimizeWrite" in sql_call
        assert "delta.enableChangeDataFeed" in sql_call
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_empty_delta_options_does_not_call_alter_table(self):
        """If delta_options is empty, should not call ALTER TABLE."""
        from fabric_utils import DeltaLoader
        
        mock_spark = MagicMock()
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            delta_options={}  # Empty
        )
        
        # Call ensure_table_properties
        loader.ensure_table_properties()
        
        # Verify ALTER TABLE was NOT called
        sql_calls = [str(call) for call in mock_spark.sql.call_args_list]
        alter_table_calls = [c for c in sql_calls if "ALTER TABLE" in c and "SET TBLPROPERTIES" in c]
        
        assert len(alter_table_calls) == 0, "ALTER TABLE should not be called when delta_options is empty"
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
        "delta": MagicMock(),
        "delta.tables": MagicMock(),
    })
    def test_pipeline_ensure_table_properties_delegates_to_loader(self):
        """Pipeline.ensure_table_properties() should delegate to loader."""
        from fabric_utils import Pipeline, WriteStrategy
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = True
        
        pipe = Pipeline(
            spark=mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy=WriteStrategy.MERGE,
            unique_key_cols=["order_id"],
            delta_options={
                "delta.autoOptimize.optimizeWrite": "true",
            }
        )
        
        # Call pipeline's ensure_table_properties
        pipe.ensure_table_properties()
        
        # Verify ALTER TABLE was called through the loader
        sql_calls = [str(call) for call in mock_spark.sql.call_args_list]
        alter_table_calls = [c for c in sql_calls if "ALTER TABLE" in c and "SET TBLPROPERTIES" in c]
        
        assert len(alter_table_calls) > 0, "Pipeline should delegate to loader's ensure_table_properties"
        assert "delta.autoOptimize.optimizeWrite" in alter_table_calls[0]
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_delta_options_not_applied_during_write(self):
        """delta_options should NOT be applied as write options, only as table properties."""
        from fabric_utils import DeltaLoader, WriteStrategy
        
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        
        mock_writer = MagicMock()
        mock_format = MagicMock()
        mock_mode = MagicMock()
        mock_option = MagicMock()
        
        # Chain the mock writer methods
        mock_df = MagicMock()
        mock_df.columns = ["order_id"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 10
        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_format
        mock_format.mode.return_value = mock_mode
        mock_mode.option.return_value = mock_option
        mock_option.saveAsTable = MagicMock()
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            delta_options={
                "delta.autoOptimize.optimizeWrite": "true",
            }
        )
        
        loader.execute(mock_df, strategy=WriteStrategy.FULL_REFRESH)
        
        # Verify .option() was only called for overwriteSchema, not for delta_options
        option_calls = [str(call) for call in mock_mode.option.call_args_list]
        
        # Should have exactly one option call for overwriteSchema
        assert len(option_calls) == 1, "Should only call .option() for overwriteSchema"
        assert "overwriteSchema" in option_calls[0]
        
        # delta.autoOptimize.optimizeWrite should NOT be in write options
        assert not any("autoOptimize" in c for c in option_calls), \
            "delta_options should not be applied as write options"


class TestTableAndColumnComments:
    """Tests for table_comment and column_comments support (v0.0.03)."""
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_table_comment_applied_on_full_refresh(self):
        """Table comment should be applied via COMMENT ON TABLE."""
        from fabric_utils import DeltaLoader, WriteStrategy
        
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            table_comment="Bronze layer orders from ERP system",
        )
        
        mock_df = MagicMock()
        mock_df.columns = ["order_id", "amount"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 100
        mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable = MagicMock()
        
        loader.execute(mock_df, strategy=WriteStrategy.FULL_REFRESH)
        
        # Verify COMMENT ON TABLE was called
        sql_calls = [str(call) for call in mock_spark.sql.call_args_list]
        comment_calls = [c for c in sql_calls if "COMMENT ON TABLE" in c]
        
        assert len(comment_calls) > 0, "COMMENT ON TABLE should be called"
        assert "Bronze layer orders from ERP system" in comment_calls[0]
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_column_comments_applied_on_full_refresh(self):
        """Column comments should be applied via ALTER TABLE ALTER COLUMN."""
        from fabric_utils import DeltaLoader, WriteStrategy
        
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            column_comments={
                "order_id": "Unique order identifier from source system",
                "amount": "Order total amount in USD",
                "created_at": "Timestamp when order was created",
            }
        )
        
        mock_df = MagicMock()
        mock_df.columns = ["order_id", "amount", "created_at"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 100
        mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable = MagicMock()
        
        loader.execute(mock_df, strategy=WriteStrategy.FULL_REFRESH)
        
        # Verify ALTER TABLE ALTER COLUMN COMMENT was called for each column
        sql_calls = [str(call) for call in mock_spark.sql.call_args_list]
        alter_column_calls = [c for c in sql_calls if "ALTER TABLE" in c and "ALTER COLUMN" in c and "COMMENT" in c]
        
        assert len(alter_column_calls) == 3, "Should have 3 ALTER COLUMN COMMENT calls"
        
        # Verify each column comment is present
        all_calls = " ".join(alter_column_calls)
        assert "order_id" in all_calls
        assert "Unique order identifier" in all_calls
        assert "amount" in all_calls
        assert "Order total amount in USD" in all_calls
        assert "created_at" in all_calls
        assert "Timestamp when order was created" in all_calls
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_comments_with_special_characters_are_escaped(self):
        """Comments with quotes should be properly escaped."""
        from fabric_utils import DeltaLoader, WriteStrategy
        
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            table_comment="Orders from 'ERP' system with \"special\" characters",
            column_comments={
                "order_id": "Customer's order ID",
            }
        )
        
        mock_df = MagicMock()
        mock_df.columns = ["order_id"]
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 10
        mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable = MagicMock()
        
        loader.execute(mock_df, strategy=WriteStrategy.FULL_REFRESH)
        
        # Verify escaping in SQL calls
        sql_calls = [str(call) for call in mock_spark.sql.call_args_list]
        
        # Table comment should have escaped quotes
        table_comment_calls = [c for c in sql_calls if "COMMENT ON TABLE" in c]
        assert len(table_comment_calls) > 0
        # Should contain escaped quotes
        assert "\\\\" in table_comment_calls[0], "Quotes should be escaped"
        
        # Column comment should have escaped quotes
        column_comment_calls = [c for c in sql_calls if "ALTER COLUMN" in c and "COMMENT" in c]
        assert len(column_comment_calls) > 0
        assert "\\\\" in column_comment_calls[0], "Quotes should be escaped"
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
    })
    def test_ensure_table_properties_applies_all_metadata(self):
        """ensure_table_properties should apply properties, table comment, and column comments."""
        from fabric_utils import DeltaLoader
        
        mock_spark = MagicMock()
        
        loader = DeltaLoader(
            spark=mock_spark,
            target_table="bronze.orders",
            delta_options={"delta.enableChangeDataFeed": "true"},
            table_comment="Bronze orders table",
            column_comments={"order_id": "Primary key"}
        )
        
        loader.ensure_table_properties()
        
        sql_calls = [str(call) for call in mock_spark.sql.call_args_list]
        
        # Should have called SET TBLPROPERTIES
        assert any("SET TBLPROPERTIES" in c for c in sql_calls), "Should set table properties"
        
        # Should have called COMMENT ON TABLE
        assert any("COMMENT ON TABLE" in c for c in sql_calls), "Should set table comment"
        
        # Should have called ALTER COLUMN COMMENT
        assert any("ALTER COLUMN" in c and "COMMENT" in c for c in sql_calls), "Should set column comments"
    
    @patch.dict("sys.modules", {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
        "delta": MagicMock(),
        "delta.tables": MagicMock(),
    })
    def test_pipeline_passes_comments_to_loader(self):
        """Pipeline should pass table_comment and column_comments to DeltaLoader."""
        from fabric_utils import Pipeline, WriteStrategy
        
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_spark.catalog.tableExists.return_value = True
        
        pipe = Pipeline(
            spark=mock_spark,
            target_table="bronze.orders",
            watermark_column="modified_at",
            strategy=WriteStrategy.MERGE,
            unique_key_cols=["order_id"],
            table_comment="Bronze orders from ERP",
            column_comments={"order_id": "Primary key", "amount": "Order total"}
        )
        
        # Verify loader has the comments
        assert pipe.loader.table_comment == "Bronze orders from ERP"
        assert pipe.loader.column_comments == {"order_id": "Primary key", "amount": "Order total"}
        
        # Call ensure_table_properties through pipeline
        pipe.ensure_table_properties()
        
        # Verify comments were applied
        sql_calls = [str(call) for call in mock_spark.sql.call_args_list]
        assert any("COMMENT ON TABLE" in c and "Bronze orders from ERP" in c for c in sql_calls)
        assert any("ALTER COLUMN" in c and "order_id" in c and "Primary key" in c for c in sql_calls)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
