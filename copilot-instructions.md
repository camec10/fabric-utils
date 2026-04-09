# fabric-utils Agent Instructions

## Project Overview

This is a Python library for Microsoft Fabric data engineering. It provides utilities for:
- **Watermark tracking** — Incremental loading with lookback windows
- **Strategy assessment** — Recommending FULL_REFRESH, DELETE_APPEND, or MERGE
- **Delta Lake loading** — Safe writes with rollback support

**Runtime**: Microsoft Fabric Spark (PySpark provided by runtime, not bundled)

## Architecture

```
Pipeline (orchestrator)
    ├── WatermarkManager (tracks incremental progress)
    └── DeltaLoader (writes to Delta tables)

Standalone:
    └── setup_control_tables() — bootstraps control schema
```

**Entry point**: Use `Pipeline` for most use cases. Raw components available for advanced scenarios.

## Key Coding Conventions

### Timestamp Formatting
When formatting datetime for SQL predicates, **omit microseconds if zero** to avoid string comparison bugs:
```python
if value.microsecond == 0:
    formatted = value.strftime("%Y-%m-%d %H:%M:%S")
else:
    formatted = value.strftime("%Y-%m-%d %H:%M:%S.%f")
```
This is critical — string columns like `'2024-01-01 00:00:00'` fail lexicographic comparison against `'2024-01-01 00:00:00.000000'`.

### SQL Generation
- Use f-strings for SQL construction (Fabric notebooks, not production web apps)
- Table names are fully qualified: `lakehouse.schema.table` or `schema.table`
- Always check `spark.catalog.tableExists()` before assuming table exists

### Error Handling
- Wrap loader failures in `LoaderError`
- Schema validation failures raise `SchemaValidationError`
- DELETE_APPEND has automatic rollback via `RESTORE TABLE`

### Strategy Selection
| Scenario | Strategy |
|----------|----------|
| Table doesn't exist | FULL_REFRESH |
| No watermark (first run) | FULL_REFRESH |
| No unique key defined | DELETE_APPEND |
| Late arriving data | DELETE_APPEND |
| Unique keys defined and low churn | MERGE |

## Testing

### Running Tests
```powershell
$env:PYTHONPATH = "src"; python -m pytest tests/ -v
```

### Test Patterns
- Tests mock `SparkSession` — no real Spark cluster needed
- Use `MagicMock` for spark, verify SQL calls via `mock_spark.sql.call_args_list`
- Integration tests run inside Fabric notebooks (not in this repo)

### Adding Tests
1. Mock spark.sql() responses based on query content
2. Verify generated SQL strings contain expected predicates
3. Test both zero and non-zero microsecond cases for timestamps

## Build & Release

### Version Increment
Edit version in `pyproject.toml`:
```toml
version = "0.0.16"
```

**Versioning Strategy** (SemVer):
- `0.0.x` — Development: Fast iteration, API may change
- `0.1.0` — Alpha: Core API stable
- `0.2.0` — Beta: Feature-complete, hardening  
- `1.0.0` — Production: Breaking changes require MAJOR bump

### Build Wheel
```powershell
python -m build
```
Output: `dist/fabric_utils-{version}-py3-none-any.whl`

### Deployment
1. Upload wheel to Fabric Lakehouse Files
2. Attach to Fabric Environment → Public Libraries → Custom

## Common Tasks

### Adding a New Write Strategy
1. Add enum value to `WriteStrategy` in `loader.py`
2. Add handling in `DeltaLoader.execute()`
3. Add tests in `TestDeltaLoader`
4. Update README strategy guidance section

### Fixing Timestamp/Predicate Issues
Check these 3 locations for consistency:
- `pipeline.py` — delete predicate construction
- `watermark.py` — `build_where_clause()` and `update_watermark()`

### Updating README for New Version
Add release notes at top of "What's New" section, following existing format:
```markdown
### v0.X.Y — Brief Title (Month Year)
- **CATEGORY**: Description of change
```

## Known Gotchas

1. **String timestamp columns**: DELETE predicates fail silently when watermark has `.000000` but column values don't
2. **Lookback window**: Both delete AND extract use lookback-adjusted watermark (refreshes entire window)
3. **saveAsTable vs insertInto**: Using `saveAsTable` in append mode — works for Delta but creates table if missing
4. **Control table schema**: Auto-created on first use, but manual `setup_control_tables()` call still available
5. **Multi-source unions with DELETE_APPEND**: If you union multiple sources (e.g., `oracle_df.union(workforce_df)`) and run DELETE_APPEND when BOTH sources return 0 rows, the library will prevent data loss by failing with a safety check error. Ensure all source systems are updated before running the pipeline, or use MERGE strategy instead for better safety.

## Dependencies

- **Runtime**: PySpark (provided by Fabric)
- **Build**: hatchling
- **Dev**: pytest, pytest-cov, black, ruff
