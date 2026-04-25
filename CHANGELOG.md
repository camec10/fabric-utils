# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.03] - 2025-01-29

### Added
- **TableRegistry** class (replaces WatermarkManager) with optimization scheduling capabilities
- `register_table()` method to register tables with optimization schedules
- `set_optimization_schedule()` method to configure optimization frequency
- `get_tables_needing_optimization()` method to find tables due for optimization
- `update_last_optimized()` method to track optimization runs
- `get_table_metadata()` method to retrieve table registration details
- `list_tables()` method to list all registered tables
- Automatic migration from `control.watermarks` to `control.tableRegistry` on first use
- `uniqueColumns` field in tableRegistry for tracking unique identifiers
- `optimizationScheduleDays` field for scheduling periodic table optimization
- `lastOptimizedTimestamp` field for tracking last optimization run

### Changed
- **BREAKING**: Renamed `control.watermarks` table to `control.tableRegistry`
- **BREAKING**: Renamed `createdTs` → `createdTimestamp` in tableRegistry
- **BREAKING**: Renamed `updatedTs` → `updatedTimestamp` in tableRegistry (new field)
- **BREAKING**: Renamed `lastRunTs` → `lastRunTimestamp` in tableRegistry
- **BREAKING**: Renamed `startedTs` → `startedTimestamp` in pipelineRuns
- **BREAKING**: Renamed `completedTs` → `completedTimestamp` in pipelineRuns
- `reset_watermark()` now uses `UPDATE SET watermarkValue = NULL` instead of `DELETE FROM`
- All control table timestamp columns now use `Timestamp` suffix for consistency
- `WatermarkManager` is now an alias for `TableRegistry` (backward compatible)

### Migration Notes
- Automatic migration runs on first TableRegistry initialization
- Migration preserves existing watermark data with new schema
- Old `control.watermarks` table dropped after successful migration
- No user action required - migration is idempotent and automatic
- Both `TableRegistry` and `WatermarkManager` can be imported (same class)

## [0.0.02] - 2026-04-24

### Added
- `table_comment` parameter for DeltaLoader and Pipeline to document table purpose
- `column_comments` parameter (dict) for DeltaLoader and Pipeline to document column meanings
- `ensure_table_properties()` method on both DeltaLoader and Pipeline to apply metadata to existing tables
- Automatic application of table and column comments during FULL_REFRESH operations
- Quote escaping for comments containing special characters

### Changed
- **BREAKING**: `delta_options` now applied as persistent table properties via `ALTER TABLE SET TBLPROPERTIES` instead of write-time options
- Removed write-time option loop - properties are now only applied once as persistent metadata
- Table properties (like `delta.autoOptimize.*`) now persist across all future writes automatically

### Fixed
- Table properties not persisting after initial write (now use ALTER TABLE instead of .option())

## [0.0.01] - 2026-04-24

### Added
- Initial release with core incremental loading functionality
- Pipeline orchestrator composing WatermarkManager and DeltaLoader
- WatermarkManager for tracking high-water marks with configurable lookback windows
- DeltaLoader supporting FULL_REFRESH, DELETE_APPEND, and MERGE strategies
- Automatic control table setup on first use
- Pipeline execution auditing to `control.pipelineRuns` table
- Row-level lineage metadata (`pipelineRunId`, `pipelineRunTimestamp`)
- String strategy support for notebook parameterization (case-insensitive)
- Data loss prevention in DELETE_APPEND (blocks deletes when source is empty)
- Orphaned watermark detection and automatic reset
- DELETE_APPEND automatic rollback via `RESTORE TABLE` on failure
- Timestamp precision handling (omits microseconds when zero)
- Schema validation for MERGE operations
- Concurrency handling with retry logic
- Comprehensive test suite (38 tests) with 80% coverage
- Complete documentation with usage examples
- MIT License

### Design Decisions
- Unpartitioned `pipelineRuns` table to eliminate concurrency conflicts
- Independent, composable components (Pipeline, WatermarkManager, DeltaLoader)
- Watermark-agnostic DeltaLoader for maximum flexibility
- First run always uses FULL_REFRESH regardless of configured strategy
- Lookback applied to extraction and deletion for consistency

[Unreleased]: https://github.com/yourusername/fabric-utils/compare/v0.0.01...HEAD
[0.0.01]: https://github.com/yourusername/fabric-utils/releases/tag/v0.0.01
