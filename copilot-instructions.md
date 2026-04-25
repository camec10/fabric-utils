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

## Documentation Maintenance

### When to Update Documentation
**ALWAYS update documentation when:**
- Adding new parameters to public APIs (Pipeline, DeltaLoader, WatermarkManager)
- Adding new public methods or functions
- Changing behavior of existing features (even if backward compatible)
- Fixing bugs that users might have worked around
- Deprecating features or parameters

### Documentation Update Checklist
For every **major feature change**, update the following in order:

1. **CHANGELOG.md** (REQUIRED)
   - Add entry under `[Unreleased]` section
   - Use categories: Added, Changed, Deprecated, Removed, Fixed, Security
   - Include code examples for new features
   - Document breaking changes prominently
   - Follow [Keep a Changelog](https://keepachangelog.com/) format

2. **README.md** (REQUIRED)
   - Update "What's New" section with version and feature summary
   - Add/update code examples showing new parameters or methods
   - Update "Quick Start" if it demonstrates the new feature
   - Update "Component Reference" for API changes
   - Add new sections for major features (e.g., "Table Metadata & Documentation")
   - Update installation instructions if wheel name changes

3. **Version Bump** (REQUIRED for releases)
   - Update `pyproject.toml` version
   - Update `src/fabric_utils/__init__.py` `__version__`
   - Move `[Unreleased]` in CHANGELOG.md to versioned section on release

4. **Tests** (REQUIRED)
   - Add tests for new functionality
   - Update test count in README.md if changed
   - Update coverage percentage in README.md

### Documentation Style
- Use **code fences** with language hints: ```python
- Include **realistic examples** with business context
- Add **comments** explaining WHY, not just WHAT
- Show **both simple and advanced usage** patterns
- Document **failure scenarios** and error messages
- Use **bold** for emphasis, `backticks` for code symbols

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

### Adding New Features
When adding major features:
1. Implement the feature with tests
2. Follow the **Documentation Maintenance** checklist above
3. Update CHANGELOG.md and README.md before marking work complete
4. Rebuild the wheel with updated version

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
