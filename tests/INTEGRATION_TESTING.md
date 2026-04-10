# Integration Testing Guide

The `integration_test_fabric_utils.py` file contains tests that require a real Spark environment and should be run inside a Microsoft Fabric notebook.

## Prerequisites

1. Microsoft Fabric workspace with a lakehouse
2. fabric-utils package installed in your Fabric environment
3. Write permissions to create test schemas and tables

## Running Integration Tests

### In a Fabric Notebook

1. **Upload the integration test file** to your Fabric workspace

2. **Create a new notebook** in your Fabric workspace

3. **Run the tests** using one of these methods:

   **Method 1: Run all tests**
   ```python
   # Import the test module
   from tests.integration_test_fabric_utils import run_all_integration_tests
   
   # Run all integration tests
   run_all_integration_tests(spark, lakehouse="lkhTest")
   ```

   **Method 2: Run individual tests**
   ```python
   from tests.integration_test_fabric_utils import (
       test_merge_metrics_and_counts,
       test_delete_append_restore_rollback,
       test_watermark_with_integer_type,
   )
   
   # Run specific test
   test_merge_metrics_and_counts(spark, lakehouse="lkhTest")
   ```

## Test Coverage

The integration tests verify:

### 1. MERGE Metrics and Counts
- ✅ MERGE correctly inserts new rows
- ✅ MERGE correctly updates existing rows
- ✅ LoadResult metrics (rows_inserted, rows_updated) are accurate
- ✅ Final table state matches expectations

### 2. DELETE_APPEND Restore Rollback
- ✅ DELETE removes old data based on predicate
- ✅ If append fails, RESTORE TABLE is triggered automatically
- ✅ Table is restored to pre-delete state
- ✅ Data integrity is preserved on failure

### 3. Integer Watermark Support
- ✅ Integer watermarks are stored and retrieved correctly
- ✅ lookback_offset subtracts from integer watermarks
- ✅ build_where_clause works with integer comparisons
- ✅ Type safety is maintained (int vs datetime)

## Cleanup

All tests automatically clean up their test tables and schemas. However, if tests fail unexpectedly, you may need to manually drop:

```sql
DROP SCHEMA IF EXISTS test_integration CASCADE;
```

## CI/CD Integration

**Do NOT run these tests in GitHub Actions CI/CD** unless you have:
- A Spark cluster configured in your CI environment
- Proper authentication to Microsoft Fabric
- A dedicated test lakehouse

For CI/CD, use the unit tests in `test_fabric_utils.py` which use mocked SparkSession.

## Troubleshooting

### Common Issues

1. **"Table already exists" error**
   - The test cleanup may have failed on a previous run
   - Manually drop the test tables and schemas

2. **"Schema not found" error**
   - Ensure your lakehouse is attached to the notebook
   - Check that you have permissions to create schemas

3. **Type errors with watermarks**
   - Ensure you're using the latest version of fabric-utils
   - Check that py.typed marker file is present

## Contributing

When adding new integration tests:
1. Follow the existing test patterns
2. Include cleanup in finally blocks
3. Add docstrings explaining what's being tested
4. Update this guide with the new test coverage
