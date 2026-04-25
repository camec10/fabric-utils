# fabric-utils

Data engineering utilities for Microsoft Fabric — table registry, watermarking, optimization scheduling, and Delta Lake strategies.

## What's New

### v0.0.03 — Table Registry & Optimization Scheduling (January 2025)

**Major Enhancement**: Expanded watermark tracking into a comprehensive table registry with optimization scheduling.

**New Features**
- **TableRegistry Class**: Replaces `WatermarkManager` with expanded metadata management
- **Optimization Scheduling**: Track and schedule periodic table optimization
  - `set_optimization_schedule()` — Configure optimization frequency in days
  - `get_tables_needing_optimization()` — Find tables due for optimization
  - `update_last_optimized()` — Record optimization completion
- **Table Registration**: Unified metadata management
  - `register_table()` — Register tables with unique columns and optimization schedules
  - `get_table_metadata()` — Retrieve full registration details
  - `list_tables()` — View all registered tables
- **Automatic Migration**: Zero-downtime upgrade from v0.0.x
  - Migrates `control.watermarks` → `control.tableRegistry` automatically
  - Preserves all existing watermark data
  - No user action required

**Breaking Changes**
- Table renamed: `control.watermarks` → `control.tableRegistry`
- Column naming standardized with `Timestamp` suffix:
  - `createdTs` → `createdTimestamp`
  - `lastRunTs` → `lastRunTimestamp`
  - `startedTs` → `startedTimestamp`
  - `completedTs` → `completedTimestamp`
- `reset_watermark()` now uses `UPDATE SET watermarkValue = NULL` instead of `DELETE`

**Backward Compatibility**
- `WatermarkManager` is maintained as an alias for `TableRegistry`
- Existing code continues to work without modification
- Both class names can be imported

See [CHANGELOG.md](CHANGELOG.md) for full details.

### v0.0.02 — Documentation & Metadata (April 2026)

Enhanced table metadata support for better data governance and documentation.

**New Features**
- **Table Comments**: Add descriptions to tables via `table_comment` parameter
- **Column Comments**: Document column meanings via `column_comments` dictionary
- **Persistent Table Properties**: `delta_options` now applied as permanent table metadata via `ALTER TABLE`
- **Metadata Management**: New `ensure_table_properties()` method to apply metadata to existing tables

**Changes**
- **Delta Options**: Now applied as persistent table properties (via `ALTER TABLE SET TBLPROPERTIES`) instead of write-time options
- Properties like `delta.autoOptimize.optimizeWrite` now persist automatically across all future writes

**Benefits**
- Self-documenting tables with clear business context
- Improved data governance and team collaboration
- Table properties prevent Spark job hangs from small file problems
- Retroactive metadata updates for existing tables

See [CHANGELOG.md](CHANGELOG.md) for full details.

### v0.0.01 — Initial Release (April 2026)

Production-ready utilities for incremental data loading in Microsoft Fabric with comprehensive safety features.

**Core Features**
- **Pipeline Orchestrator**: Composable workflow for watermark tracking + Delta writes
- **Watermark Management**: Track incremental progress with configurable lookback windows
- **Delta Lake Strategies**: FULL_REFRESH, DELETE_APPEND, and MERGE with schema validation
- **Auto-Setup**: Control tables created automatically on first use
- **Pipeline Auditing**: Detailed execution metadata logged to `control.pipelineRuns`
- **String Strategy Support**: Pass strategy names as strings for notebook parameterization

**Safety & Reliability**
- **Data Loss Prevention**: DELETE_APPEND verifies source has data before executing deletes
- **Orphaned Watermark Detection**: Auto-resets stale watermarks when tables are dropped
- **Concurrency Handling**: Retry logic with exponential backoff for concurrent pipeline runs
- **Automatic Rollback**: DELETE_APPEND failures trigger `RESTORE TABLE` recovery
- **Timestamp Precision**: Omits microseconds when zero to fix string column comparisons
- **Error Propagation**: Configuration errors (permissions, connectivity) surface immediately

**Architecture**
- Unpartitioned `pipelineRuns` table eliminates concurrency conflicts
- Simplified control schema (watermarks + pipelineRuns only)
- Independent, single-responsibility components
- Full test coverage (55 passing tests, 81% coverage)

## Installation

### In Fabric Environment

1. Build the wheel:
   ```bash
   pip install build
   python -m build
   ```

2. Upload `dist/fabric_utils-0.0.02-py3-none-any.whl` to your Fabric Lakehouse Files

3. Attach to your Fabric Environment:
   - Go to Environment settings → Public Libraries → Custom
   - Upload the wheel file

### Local Development

```bash
pip install -e ".[dev]"
```

## Quick Start

### Basic Pipeline with Documentation

```python
from fabric_utils import Pipeline, WriteStrategy
from pyspark.sql import functions as F

pipe = Pipeline(
    spark,
    target_table="bronze.orders",
    watermark_column="modified_at",
    strategy=WriteStrategy.DELETE_APPEND,
    lookback_days=90,
    # New in v0.0.02: Table documentation
    table_comment="Bronze layer orders from SAP ERP. Loaded daily at 2am UTC via incremental sync.",
    column_comments={
        "order_id": "Unique order identifier from SAP VBAK.VBELN",
        "customer_id": "Foreign key to customers table (SAP KNA1.KUNNR)",
        "amount": "Order total amount in USD",
        "modified_at": "Watermark column - last modification timestamp from source",
        "pipelineRunId": "Lineage metadata - links to control.pipelineRuns",
    },
    # Optimize for production workloads
    delta_options={
        "delta.autoOptimize.optimizeWrite": "true",  # Coalesce small files during writes
        "delta.autoOptimize.autoCompact": "true",    # Background compaction
        "delta.enableChangeDataFeed": "true",        # Enable CDC for downstream consumers
    },
)

# Get watermark (None on first run)
watermark = pipe.get_watermark()

# Extract from source
if watermark:
    source_df = spark.sql(f"""
        SELECT * FROM raw.sap_orders
        WHERE modified_at >= '{watermark}'
    """)
else:
    source_df = spark.table("raw.sap_orders")

# Transform
transformed = source_df.withColumn("loaded_at", F.current_timestamp())

# Load + update watermark atomically
result = pipe.execute(transformed)
print(f"✓ {result.rows_inserted:,} inserted, {result.rows_deleted:,} deleted")
```

**What happens automatically:**
- First run: FULL_REFRESH strategy, table comment and column comments applied
- Subsequent runs: DELETE_APPEND strategy as configured
- Table properties (autoOptimize, etc.) persist across all writes
- Execution logged to `control.pipelineRuns` for auditing
- Row-level lineage via `pipelineRunId` column

### Apply Metadata to Existing Tables

If you have existing tables and want to add documentation or properties:

```python
pipe = Pipeline(
    spark,
    target_table="bronze.orders",
    watermark_column="modified_at",
    strategy=WriteStrategy.DELETE_APPEND,
    table_comment="Updated description for existing table",
    column_comments={
        "order_id": "Primary key",
        "amount": "Order total in USD",
    },
    delta_options={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

# Apply metadata without running a pipeline
pipe.ensure_table_properties()
```

This runs:
- `ALTER TABLE bronze.orders SET TBLPROPERTIES (...)`
- `COMMENT ON TABLE bronze.orders IS '...'`
- `ALTER TABLE bronze.orders ALTER COLUMN ... COMMENT '...'`

### Verify Metadata

```python
# View table properties
spark.sql("SHOW TBLPROPERTIES bronze.orders").show(100, False)

# View table and column comments
spark.sql("DESCRIBE EXTENDED bronze.orders").show(100, False)
```

## Architecture

The library is composed of independent, single-responsibility components:

| Component | Responsibility |
|---|---|
| **Pipeline** | Orchestrator — composes table registry + loader into one workflow |
| **TableRegistry** | Track incremental progress, optimization schedules, and table metadata |
| **DeltaLoader** | Write DataFrames to Delta tables — overwrite, delete+append, merge |
| **setup_control_tables()** | Bootstrap control schema and audit tables |

**Pipeline** is the recommended entry point — it composes TableRegistry
and DeltaLoader so you can't forget to update the watermark or pick the wrong initial
strategy. The underlying components remain available for advanced use cases.

**Note**: `WatermarkManager` is maintained as an alias for `TableRegistry` for backward compatibility.

## Usage

### Quick Start with Pipeline (Recommended)

`Pipeline` handles strategy selection, delete-predicate construction,
and watermark updates automatically. Control tables are created automatically on first use.
You just extract, transform, and call `execute()`.

#### DELETE+APPEND

```python
from pyspark.sql import functions as F
from fabric_utils import Pipeline, WriteStrategy

pipe = Pipeline(
    spark,
    target_table="bronze.time_entries",
    watermark_column="lastUpdateDate",
    strategy=WriteStrategy.DELETE_APPEND,
    lookback_days=90,
)

# 1. Get watermark (None on first run)
watermark = pipe.get_watermark()

# 2. Extract — you know your source, the pipeline doesn't
if watermark:
    source_df = spark.sql(f"""
        SELECT * FROM raw.time_entries
        WHERE lastUpdateDate >= '{watermark}'
    """)
else:
    source_df = spark.table("raw.time_entries")

# 3. Transform
transformed = source_df.withColumn("processedDate", F.current_date())

# 4. Load + watermark update (one call — can't forget the watermark)
result = pipe.execute(transformed)
print(f"Done: {result.rows_inserted} inserted, {result.rows_deleted} deleted")
```

**How lookback works:**
- **Extraction**: Uses watermark with lookback applied (`stored_watermark - lookback_days`)
- **Deletion**: Uses original stored watermark (no lookback adjustment)
- **Why**: Prevents "watermark creep" where rows in the lookback window get orphaned

Example timeline:
```
Run 1: Stored watermark = 2024-06-01
       Extract with 90-day lookback: >= 2024-03-03
       Delete using original: >= 2024-06-01 (deletes previous run's data)
       Update watermark: 2024-06-15

Run 2: Stored watermark = 2024-06-15
       Extract with 90-day lookback: >= 2024-03-17
       Delete using original: >= 2024-06-15 (deletes previous run's data)
       No orphaned rows between 2024-03-03 and 2024-03-17 ✓
```

#### MERGE

```python
from fabric_utils import Pipeline, WriteStrategy

pipe = Pipeline(
    spark,
    target_table="silver.repair_orders",
    watermark_column="modifiedTimestamp",
    strategy=WriteStrategy.MERGE,
    unique_key_cols=["repairOrderId", "lineNumber"],
    lookback_days=30,
)

watermark = pipe.get_watermark()
source_df = (
    spark.sql(f"SELECT * FROM bronze.repair_orders WHERE modifiedTimestamp >= '{watermark}'")
    if watermark
    else spark.table("bronze.repair_orders")
)

result = pipe.execute(source_df)
print(f"Done: {result.rows_inserted} inserted, {result.rows_updated} updated")
```

#### Parameterized Notebooks

For scheduled pipelines with dynamic strategy selection, pass strategy as a string:

```python
# Parameters cell (configured in Fabric pipeline)
REFRESH_STRATEGY = "DELETE_APPEND"  # Or "FULL_REFRESH", "MERGE"
LOOKBACK_DAYS = 90

# Pipeline setup - no if/elif conversion needed!
pipe = Pipeline(
    spark,
    target_table="bronze.time_entries",
    watermark_column="lastUpdateDate",
    strategy=REFRESH_STRATEGY,  # String accepted directly (case-insensitive)
    lookback_days=LOOKBACK_DAYS,
)

# Rest of your code unchanged
watermark = pipe.get_watermark()
# ... extract, transform, execute
```

The string parameter is accepted directly (case-insensitive), no manual enum conversion needed!

#### Multi-Source with Explicit Watermark

When the new watermark can't be derived from a single column (e.g., joins),
pass it explicitly:

```python
result = pipe.execute(summary_df, new_watermark=max_timestamp_from_sources)
```

#### Dropping Down to Raw Components

The underlying components are always accessible:

```python
# Use the WatermarkManager directly
pipe.wm.reset_watermark("bronze.time_entries")
pipe.wm.list_watermarks()

# Use the DeltaLoader directly for a one-off full refresh
pipe.loader.execute(df, strategy=WriteStrategy.FULL_REFRESH)
```

---

### Manual Composition (Advanced)

For full control — multi-source pipelines, custom watermark logic, or when
you need to compose the components yourself.

#### Example 1: DELETE+APPEND Pipeline

Use when source data doesn't have a reliable unique key, or when a large
percentage of rows change between runs.

```python
from pyspark.sql import functions as F
from fabric_utils import WatermarkManager, DeltaLoader, WriteStrategy, LoaderError

TARGET_TABLE = "bronze.time_entries"
SOURCE_TABLE = "raw.time_entries"
WATERMARK_COL = "lastUpdateDate"
LOOKBACK_DAYS = 90

# --- 1. Get the watermark ---
wm = WatermarkManager(spark)
watermark = wm.get_watermark(TARGET_TABLE, lookback_days=LOOKBACK_DAYS)

# --- 2. Build source query ---
# watermark is None on the very first run → full extract
if watermark:
    source_df = spark.sql(f"""
        SELECT * FROM {SOURCE_TABLE}
        WHERE {WATERMARK_COL} >= '{watermark}'
    """)
    # The delete predicate must match what we extracted
    delete_predicate = f"{WATERMARK_COL} >= '{watermark}'"
else:
    source_df = spark.sql(f"SELECT * FROM {SOURCE_TABLE}")
    delete_predicate = None  # first run — nothing to delete

# --- 3. Transform (your business logic goes here) ---
transformed_df = (
    source_df
    .withColumn("processedDate", F.current_date())
    .filter(F.col("hours") > 0)
)

# --- 4. Load ---
loader = DeltaLoader(spark, target_table=TARGET_TABLE)

try:
    # First run (no watermark): FULL_REFRESH writes the whole table
    # Subsequent runs: DELETE+APPEND removes the lookback window then appends
    strategy = WriteStrategy.FULL_REFRESH if watermark is None else WriteStrategy.DELETE_APPEND

    result = loader.execute(
        transformed_df,
        strategy=strategy,
        delete_predicate=delete_predicate,
    )

    # --- 5. Update watermark only after a successful write ---
    new_wm = source_df.agg(F.max(WATERMARK_COL)).collect()[0][0]
    if new_wm:
        wm.update_watermark(TARGET_TABLE, new_wm)

    print(f"✓ {result.strategy.value}: "
          f"{result.rows_deleted} deleted, {result.rows_inserted} inserted "
          f"in {result.duration_seconds:.1f}s")

except LoaderError as e:
    # DELETE+APPEND is auto-rolled back via RESTORE if the append fails
    print(f"✗ Load failed: {e}")
    raise
```

---

### Example 2: MERGE Pipeline

Use when the source has a reliable unique key and only a small percentage of
rows change between runs (low churn).

```python
from pyspark.sql import functions as F
from fabric_utils import WatermarkManager, DeltaLoader, WriteStrategy, LoaderError

TARGET_TABLE = "silver.repair_orders"
SOURCE_TABLE = "bronze.repair_orders"
WATERMARK_COL = "modifiedTimestamp"
UNIQUE_KEYS   = ["repairOrderId", "lineNumber"]
LOOKBACK_DAYS = 30

# --- 1. Get the watermark ---
wm = WatermarkManager(spark)
watermark = wm.get_watermark(TARGET_TABLE, lookback_days=LOOKBACK_DAYS)

# --- 2. Build source query ---
if watermark:
    source_df = spark.sql(f"""
        SELECT * FROM {SOURCE_TABLE}
        WHERE {WATERMARK_COL} >= '{watermark}'
    """)
else:
    source_df = spark.sql(f"SELECT * FROM {SOURCE_TABLE}")

# --- 3. Transform ---
transformed_df = (
    source_df
    .withColumn("totalCost", F.col("laborCost") + F.col("materialCost"))
    .drop("_rawMetadata")
)

# --- 4. Load ---
loader = DeltaLoader(
    spark,
    target_table=TARGET_TABLE,
    unique_key_cols=UNIQUE_KEYS,
)

try:
    # First run: FULL_REFRESH. Subsequent: MERGE (matched → update, unmatched → insert)
    strategy = WriteStrategy.FULL_REFRESH if watermark is None else WriteStrategy.MERGE

    result = loader.execute(transformed_df, strategy=strategy)

    # --- 5. Update watermark only after a successful write ---
    new_wm = source_df.agg(F.max(WATERMARK_COL)).collect()[0][0]
    if new_wm:
        wm.update_watermark(TARGET_TABLE, new_wm)

    print(f"✓ {result.strategy.value}: "
          f"{result.rows_inserted} inserted, {result.rows_updated} updated "
          f"in {result.duration_seconds:.1f}s")

except LoaderError as e:
    print(f"✗ Load failed: {e}")
    raise
```

---

### Example 3: Multi-Source Pipeline with Explicit Watermarks

When a pipeline joins multiple source tables, each with its own watermark,
you retrieve watermarks upfront and manage them independently.

```python
from pyspark.sql import functions as F
from fabric_utils import WatermarkManager, DeltaLoader, WriteStrategy

TARGET_TABLE = "gold.fleet_cost_summary"

wm = WatermarkManager(spark)
repair_wm = wm.get_watermark("bronze.repair_orders", lookback_days=30)
mileage_wm = wm.get_watermark("bronze.mileage_events", lookback_days=7)

# Extract from both sources using their respective watermarks
repairs_df = spark.sql(f"""
    SELECT * FROM bronze.repair_orders
    WHERE modifiedTimestamp >= '{repair_wm}'
""") if repair_wm else spark.table("bronze.repair_orders")

mileage_df = spark.sql(f"""
    SELECT * FROM bronze.mileage_events
    WHERE eventDate >= '{mileage_wm}'
""") if mileage_wm else spark.table("bronze.mileage_events")

# Complex transformation joining both
summary_df = (
    repairs_df
    .join(mileage_df, "equipmentId")
    .groupBy("equipmentId")
    .agg(
        F.sum("totalCost").alias("totalRepairCost"),
        F.sum("miles").alias("totalMiles"),
    )
)

# Write
loader = DeltaLoader(spark, target_table=TARGET_TABLE, unique_key_cols=["equipmentId"])
result = loader.execute(summary_df, strategy=WriteStrategy.MERGE)

# Update both watermarks independently on success
new_repair_wm = repairs_df.agg(F.max("modifiedTimestamp")).collect()[0][0]
new_mileage_wm = mileage_df.agg(F.max("eventDate")).collect()[0][0]
if new_repair_wm:
    wm.update_watermark("bronze.repair_orders", new_repair_wm)
if new_mileage_wm:
    wm.update_watermark("bronze.mileage_events", new_mileage_wm)
```

---

### Component Reference

#### WatermarkManager

```python
from fabric_utils import WatermarkManager

wm = WatermarkManager(spark, lakehouse="lkhRaw", schema="control")

# Get watermark (returns None on first run → signals full extract)
watermark = wm.get_watermark("bronze.orders", lookback_days=90)

# Build a WHERE clause directly
where_clause = wm.build_where_clause("bronze.orders", "modified_at", lookback_days=90)

# Update after successful load
wm.update_watermark("bronze.orders", new_max_value)

# Admin: reset to force full reload
wm.reset_watermark("bronze.orders")

# Admin: list all tracked tables
wm.list_watermarks()

# Manual pipeline run logging (typically handled by Pipeline automatically)
wm.log_pipeline_run(
    run_id="run-123",
    pipeline_name="bronze.orders",
    target_table="bronze.orders",
    status="COMPLETED",
    strategy="merge",
    rows_processed=1000,
    rows_inserted=50,
    rows_updated=30,
    duration_seconds=12.5,
)
```

#### DeltaLoader

```python
from fabric_utils import DeltaLoader, WriteStrategy

# Basic initialization
loader = DeltaLoader(
    spark, 
    target_table="bronze.orders", 
    unique_key_cols=["order_id"],
)

# With metadata and optimization (new in v0.0.02)
loader = DeltaLoader(
    spark,
    target_table="bronze.orders",
    unique_key_cols=["order_id"],
    delta_options={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
    },
    table_comment="Bronze layer orders from ERP system",
    column_comments={
        "order_id": "Unique order identifier",
        "amount": "Order total in USD",
    },
)

# Full refresh (initial load or reset)
result = loader.execute(source_df, strategy=WriteStrategy.FULL_REFRESH)

# Delete + Append with explicit predicate
result = loader.execute(
    source_df,
    strategy=WriteStrategy.DELETE_APPEND,
    delete_predicate="modified_at >= '2024-01-01'",
)

# Merge (matched rows update, unmatched rows insert)
result = loader.execute(source_df, strategy=WriteStrategy.MERGE)

# Apply metadata to existing table
loader.ensure_table_properties()
```

**About `delta_options`** (Changed in v0.0.02):
- Applied as **persistent table properties** via `ALTER TABLE SET TBLPROPERTIES`
- Properties automatically apply to **all future writes** (not just the current write)
- Useful for:
  - `delta.autoOptimize.optimizeWrite`: Prevent small file problems
  - `delta.autoOptimize.autoCompact`: Background file compaction
  - `delta.enableChangeDataFeed`: Enable CDC for downstream consumers
  - `delta.logRetentionDuration`: Control metadata retention
  - `delta.deletedFileRetentionDuration`: Control time travel window
```

---

### Table Metadata & Documentation

**New in v0.0.02**: Add table and column comments for better data governance and team collaboration.

#### Why Document Your Tables?

- **Self-Service Analytics**: New team members understand data without hunting for docs
- **Data Governance**: Clear ownership, source systems, and update schedules
- **Compliance**: Document PII, data classification, and retention policies
- **Lineage**: Link tables to source systems and business processes

#### Adding Comments

**At Pipeline Creation:**
```python
pipe = Pipeline(
    spark,
    target_table="bronze.orders",
    watermark_column="modified_at",
    strategy=WriteStrategy.DELETE_APPEND,
    table_comment=(
        "Bronze layer orders from SAP ERP (VBAK table). "
        "Loaded daily at 2am UTC via incremental sync with 90-day lookback. "
        "Owner: data-engineering@company.com"
    ),
    column_comments={
        # Business keys
        "order_id": "Unique order identifier from SAP VBAK.VBELN (Primary Key)",
        "customer_id": "Foreign key to customers table (SAP KNA1.KUNNR)",
        
        # Business data
        "amount": "Order total amount in USD. Excludes tax and shipping.",
        "status": "Order status: PENDING, PROCESSING, COMPLETED, CANCELLED",
        "order_date": "Date order was placed by customer",
        
        # Lineage and audit
        "modified_at": "Watermark column - last modification timestamp from source system",
        "pipelineRunId": "Links to control.pipelineRuns for execution lineage",
        "pipelineRunTimestamp": "When this row was last loaded",
        
        # Data classification
        "customer_email": "[PII] Customer email address - handle per GDPR requirements",
    },
)
```

**For Existing Tables:**
```python
# Define metadata
loader = DeltaLoader(
    spark,
    target_table="bronze.existing_table",
    table_comment="Updated description with business context",
    column_comments={
        "id": "Primary key",
        "sensitive_field": "[PII] Handle per compliance policy",
    },
)

# Apply without running a full pipeline
loader.ensure_table_properties()
```

#### Special Characters in Comments

The library automatically escapes quotes and special characters:
```python
pipe = Pipeline(
    ...,
    table_comment="Orders from customer's ERP system (\"SAP\")",  # Quotes handled automatically
    column_comments={
        "notes": "Customer's order notes - may contain 'quotes' and \"symbols\"",
    }
)
```

#### Viewing Comments

```sql
-- Table comment and metadata
DESCRIBE EXTENDED bronze.orders;

-- All column names and comments
DESCRIBE TABLE bronze.orders;

-- Just table properties
SHOW TBLPROPERTIES bronze.orders;
```

---

### Choosing the Right Write Strategy

**When to use FULL_REFRESH:**
- Initial load (table doesn't exist yet)
- Force complete table rebuild
- Data source doesn't support incremental extraction
- Simplest approach for small tables

**When to use DELETE_APPEND:**
- No reliable unique key exists in your data
- High churn rate (>50% of rows change between runs)
- Lookback window covers all changed data
- Source supports incremental extraction
- DELETE predicate can be constructed from watermark

**When to use MERGE:**
- Reliable unique key exists (single column or composite)
- Low churn rate (<10% of rows change between runs)
- Need true upsert behavior (updates + inserts)
- Want to track row-level changes
- More complex but precise

**Rule of thumb:** Start with DELETE_APPEND unless you have a good unique key and low churn, then consider MERGE. Use FULL_REFRESH only when necessary.

## Control Table Setup

Control tables are **automatically created** when you first use `Pipeline` or `WatermarkManager`. No manual setup required!

If you prefer explicit setup (e.g., for permissions testing), you can still call:

```python
from fabric_utils import setup_control_tables

setup_control_tables(spark, schema="control")
```

This creates:
- `control.watermarks` — Timestamp-based watermark tracking
- `control.pipelineRuns` — Pipeline execution audit log

### Pipeline Run Auditing

The `pipelineRuns` table automatically captures execution metadata:

```python
from fabric_utils import Pipeline, WriteStrategy

pipe = Pipeline(
    spark,
    target_table="bronze.orders",
    watermark_column="modified_at",
    strategy=WriteStrategy.DELETE_APPEND,
)

# Pipeline automatically logs:
# - START: when execute() is called
# - COMPLETED: on success with row counts and duration
# - FAILED: on error with error message
result = pipe.execute(source_df)
```

Query the audit log:
```sql
SELECT 
    runId,
    pipelineName,
    status,
    strategy,
    rowsProcessed,
    rowsInserted,
    rowsUpdated,
    durationSeconds,
    startedTs,
    completedTs
FROM control.pipelineRuns
WHERE pipelineName = 'bronze.orders'
ORDER BY startedTs DESC
```

### Explicit Full Refresh

Force a complete reload by setting the strategy to `FULL_REFRESH`:

```python
pipe = Pipeline(
    spark,
    target_table="bronze.orders",
    watermark_column="modified_at",
    strategy=WriteStrategy.FULL_REFRESH,  # Resets watermark automatically
)

watermark = pipe.get_watermark()  # May return old watermark
source_df = spark.table("raw.orders")  # Full extract
result = pipe.execute(source_df)  # Watermark reset → full overwrite → new watermark set
```

The pipeline automatically:
1. Resets the watermark (deletes from control.watermarks)
2. Performs full overwrite of target table
3. Sets new watermark from the loaded data

### Orphaned Watermark Detection

If a table is dropped but its watermark remains, the Pipeline detects and fixes this:

```python
pipe = Pipeline(
    spark,
    target_table="bronze.orders",
    watermark_column="modified_at",
)

watermark = pipe.get_watermark()  # Returns old watermark
# But table 'bronze.orders' was dropped!

result = pipe.execute(source_df)  # Detects orphaned watermark
# Output: ⚠ WARNING: Table bronze.orders did not exist but watermark 2024-03-15 was found.
#         This suggests the table was dropped without resetting the watermark.
#         Resetting watermark to ensure clean state.
```

The pipeline automatically resets the watermark to prevent data corruption.

## Error Handling

```python
from fabric_utils import DeltaLoader, WriteStrategy, LoaderError, SchemaValidationError

loader = DeltaLoader(spark, "bronze.orders", unique_key_cols=["order_id"])

try:
    result = loader.execute(source_df, strategy=WriteStrategy.DELETE_APPEND,
                            delete_predicate="modified_at >= '2024-01-01'")
except SchemaValidationError as e:
    print(f"Schema issue: {e}")  # missing merge key columns
except LoaderError as e:
    print(f"Load failed: {e}")   # DELETE+APPEND auto-rolled back
```

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `pytest tests/ -v`
5. Update documentation and CHANGELOG.md
6. Submit a pull request

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a detailed history of changes, including breaking changes, new features, and bug fixes.

## License

MIT
