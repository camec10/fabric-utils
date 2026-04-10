# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.01] - 2026-04-10

### Added
- **Pipeline Orchestrator**: Composable workflow for watermark tracking + Delta writes
- **Watermark Management**: Track incremental progress with configurable lookback windows
- **Delta Lake Strategies**: FULL_REFRESH, DELETE_APPEND, and MERGE with schema validation
- **Auto-Setup**: Control tables created automatically on first use
- **Pipeline Auditing**: Detailed execution metadata logged to `control.pipelineRuns`
- **String Strategy Support**: Pass strategy names as strings for notebook parameterization
- **Data Loss Prevention**: DELETE_APPEND verifies source has data before executing deletes
- **Orphaned Watermark Detection**: Auto-resets stale watermarks when tables are dropped
- **Concurrency Handling**: Retry logic with exponential backoff for concurrent pipeline runs
- **Automatic Rollback**: DELETE_APPEND failures trigger `RESTORE TABLE` recovery
- **Timestamp Precision**: Omits microseconds when zero to fix string column comparisons
- **Error Propagation**: Configuration errors (permissions, connectivity) surface immediately
- Unpartitioned `pipelineRuns` table eliminates concurrency conflicts
- Simplified control schema (watermarks + pipelineRuns only)
- Full test coverage (38 passing unit tests with mocked SparkSession)

### Changed
- N/A (Initial Release)

### Deprecated
- N/A (Initial Release)

### Removed
- N/A (Initial Release)

### Fixed
- N/A (Initial Release)

### Security
- SQL injection prevention in error message logging (escapes single quotes)

[0.0.01]: https://github.com/camec10/fabric-utils/releases/tag/v0.0.01
