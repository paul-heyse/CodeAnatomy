# Part VII: Observability Layer

## Overview

The Observability Layer (`src/obs/`) provides diagnostic collection, metrics computation, quality tracking, and scan telemetry for CodeAnatomy's inference pipeline. This module integrates with both the Hamilton orchestration framework and the DataFusion execution engine to capture runtime events, compute dataset statistics, track data quality issues, and record scan performance metrics.

**Key Design Principles:**
1. **Non-Intrusive Collection**: Diagnostics are collected via callback hooks and sink patterns that don't affect pipeline execution
2. **Structured Event Storage**: All events use typed schemas for consistent analysis
3. **Quality-First Tracking**: Invalid entities are captured in quality tables rather than causing failures
4. **Scan Telemetry**: Fragment-level metrics enable performance analysis and optimization

---

## DiagnosticsCollector

**File:** `src/obs/diagnostics.py`

The `DiagnosticsCollector` class is the central sink for runtime events and artifacts during pipeline execution. It collects events, artifacts, and metrics in-memory for later persistence or analysis.

### Core Data Structure

```python
@dataclass
class DiagnosticsCollector:
    """Collect diagnostics events and artifacts in-memory."""

    events: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)
    artifacts: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)
    metrics: list[tuple[str, float, dict[str, str]]] = field(default_factory=list)
```

### Recording Methods

**Event Recording:**
```python
def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
    """Append event rows under a logical name."""

def record_event(self, name: str, properties: Mapping[str, object]) -> None:
    """Append a single event payload under a logical name."""
```

**Artifact Recording:**
```python
def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
    """Append an artifact payload under a logical name."""
```

**Metric Recording:**
```python
def record_metric(self, name: str, value: float, tags: Mapping[str, str]) -> None:
    """Record a metric value with dimensional tags."""
```

### Integration Points

The DiagnosticsCollector integrates with several pipeline components:

1. **Hamilton Pipeline** (`src/hamilton_pipeline/driver_factory.py`):
   - Collector is passed to driver construction
   - Records execution lifecycle events
   - Captures cache lineage artifacts

2. **DataFusion Engine** (`src/datafusion_engine/execution_facade.py`):
   - Records plan execution artifacts
   - Captures UDF snapshot diagnostics
   - Tracks view fingerprints

3. **Engine Session** (`src/engine/session_factory.py`):
   - Records feature state snapshots
   - Captures runtime profile metadata

### Specialized Recording Functions

The module provides type-safe recording functions for common artifact types:

```python
def record_view_fingerprints(
    sink: DiagnosticsCollector,
    *,
    view_nodes: Sequence[ViewNode],
) -> None:
    """Record policy-aware view fingerprints into diagnostics."""

def record_view_udf_parity(
    sink: DiagnosticsCollector,
    *,
    snapshot: Mapping[str, object],
    view_nodes: Sequence[ViewNode],
    ctx: SessionContext | None = None,
) -> None:
    """Record view/UDF parity diagnostics into the sink."""

def record_rust_udf_snapshot(
    sink: DiagnosticsCollector,
    *,
    snapshot: Mapping[str, object],
) -> None:
    """Record a Rust UDF snapshot summary payload."""

def record_view_contract_violations(
    sink: DiagnosticsCollector,
    *,
    table_name: str,
    violations: Sequence[SchemaViolation],
) -> None:
    """Record schema contract violations for a view."""

def record_view_artifact(
    sink: DiagnosticsCollector,
    *,
    artifact: DataFusionViewArtifact,
) -> None:
    """Record a deterministic view artifact payload."""

def record_cache_lineage(
    sink: DiagnosticsCollector,
    *,
    payload: Mapping[str, object],
    rows: Sequence[Mapping[str, object]] | None = None,
) -> None:
    """Record cache lineage artifacts and optional per-node rows."""
```

---

## Dataset and Column Statistics

**File:** `src/obs/metrics.py`

The metrics module computes dataset-level and column-level statistics for observability and manifest recording.

### Dataset Statistics Schema

```python
DATASET_STATS_SCHEMA = pa.schema(
    [
        ("dataset_name", pa.string()),
        ("rows", pa.int64()),
        ("columns", pa.int32()),
        ("schema_fingerprint", pa.string()),
    ]
)
```

### Column Statistics Schema

```python
COLUMN_STATS_SCHEMA = pa.schema(
    [
        ("dataset_name", pa.string()),
        ("column_name", pa.string()),
        ("type", pa.string()),
        ("null_count", pa.int64()),
    ]
)
```

### Statistics Computation

**Dataset-Level Statistics:**
```python
def dataset_stats_table(tables: Mapping[str, TableLike | None]) -> TableLike:
    """Build a dataset-level stats table.

    Table columns:
      dataset_name, rows, columns, schema_fingerprint
    """
```

**Column-Level Statistics:**
```python
def column_stats_table(tables: Mapping[str, TableLike | None]) -> TableLike:
    """Build a column-level stats table.

    Table columns:
      dataset_name, column_name, type, null_count
    """
```

**Table Summary:**
```python
def table_summary(table: TableLike) -> TableSummary:
    """Return a compact summary for a table suitable for manifest recording.

    Returns
    -------
    TableSummary
        Summary with rows, columns, schema_fingerprint, and schema fields.
    """
```

---

## Quality Tables

**File:** `src/obs/metrics.py`

Quality tables track invalid entities encountered during pipeline execution. Rather than failing on invalid data, the pipeline records issues in quality tables for downstream analysis.

### Quality Schema

```python
QUALITY_SCHEMA = pa.schema(
    [
        pa.field("entity_kind", pa.string(), nullable=False),
        pa.field("entity_id", pa.string()),
        pa.field("issue", pa.string(), nullable=False),
        pa.field("source_table", pa.string()),
    ]
)
```

### Quality Detection

**QualityPlanSpec:**
```python
@dataclass(frozen=True)
class QualityPlanSpec:
    """Specification for building quality plans from IDs."""

    id_col: str
    entity_kind: str
    issue: str
    source_table: str | None = None
```

**Quality Row Extraction:**
```python
def quality_from_ids(
    table: TableLike,
    *,
    id_col: str,
    entity_kind: str,
    issue: str,
    source_table: str | None = None,
) -> TableLike:
    """Return quality rows for invalid IDs in the specified column.

    Invalid IDs include:
    - NULL values
    - Zero values (integer 0 or string "0")

    Returns
    -------
    TableLike
        Quality rows for invalid identifiers.
    """
```

**Quality Table Aggregation:**
```python
def concat_quality_tables(tables: Sequence[TableLike]) -> TableLike:
    """Concatenate quality tables, returning an empty table when none exist."""

def empty_quality_table() -> TableLike:
    """Return an empty quality table with the canonical schema."""
```

### Integration with CPG Build

Quality tables are emitted as part of the CPG build process:
- `cpg_nodes_quality`: Invalid node identifiers
- `cpg_props_quality`: Invalid property values

These tables enable post-build analysis of data quality issues without interrupting the pipeline.

---

## Scan Telemetry

**File:** `src/obs/scan_telemetry.py`

Scan telemetry captures fragment-level metrics for Delta Lake and Parquet scans, enabling performance analysis and optimization.

### Scan Telemetry Schema

```python
SCAN_TELEMETRY_SCHEMA = pa.schema(
    [
        ("dataset", pa.string()),
        ("fragment_count", pa.int64()),
        ("row_group_count", pa.int64()),
        ("count_rows", pa.int64()),
        ("estimated_rows", pa.int64()),
        ("file_hints", pa.list_(pa.string())),
        ("fragment_paths", pa.list_(pa.string())),
        ("partition_expressions", pa.list_(pa.string())),
        ("required_columns", pa.list_(pa.string())),
        ("scan_columns", pa.list_(pa.string())),
        ("dataset_schema_msgpack", pa.binary()),
        ("projected_schema_msgpack", pa.binary()),
        ("discovery_policy_msgpack", pa.binary()),
        ("scan_profile_msgpack", pa.binary()),
    ]
)
```

### ScanTelemetry Data Structure

```python
class ScanTelemetry(
    StructBaseCompat,
    array_like=True,
    gc=False,
    cache_hash=True,
    frozen=True,
):
    """Scan telemetry for dataset fragments."""

    fragment_count: int
    row_group_count: int
    count_rows: int | None
    estimated_rows: int | None
    file_hints: tuple[str, ...] = ()
    fragment_paths: tuple[str, ...] = ()
    partition_expressions: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    scan_columns: tuple[str, ...] = ()
    dataset_schema: JsonDict | None = None
    projected_schema: JsonDict | None = None
    discovery_policy: JsonDict | None = None
    scan_profile: JsonDict | None = None
```

### Telemetry Collection

**Fragment Telemetry:**
```python
def fragment_telemetry(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
    scanner: ds.Scanner | None = None,
    options: ScanTelemetryOptions | None = None,
) -> ScanTelemetry:
    """Return telemetry for dataset fragments.

    Captures:
    - Fragment and row-group counts
    - Row count (actual and estimated)
    - File path hints for debugging
    - Partition expressions
    - Schema information (dataset and projected)
    - Discovery and scan profile configuration
    """
```

**Telemetry Options:**
```python
@dataclass(frozen=True)
class ScanTelemetryOptions:
    """Options for fragment scan telemetry."""

    hint_limit: int | None = 5
    discovery_policy: JsonDict | None = None
    scan_profile: JsonDict | None = None
    required_columns: Sequence[str] | None = None
    scan_columns: Sequence[str] | None = None
```

### Fragment Utilities

The metrics module provides utilities for working with dataset fragments:

```python
def list_fragments(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
) -> list[ds.Fragment]:
    """Return dataset fragments, optionally filtered by a predicate."""

def split_fragment_by_row_group(fragment: ds.Fragment) -> list[ds.Fragment]:
    """Split a fragment into row-group fragments when supported."""

def row_group_fragments(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
) -> list[ds.Fragment]:
    """Return row-group fragments for a dataset."""

def row_group_count(fragments: Sequence[ds.Fragment]) -> int:
    """Return the total row-group count for supported fragments."""

def fragment_file_hints(
    fragments: Sequence[ds.Fragment],
    *,
    limit: int | None = 5,
) -> tuple[str, ...]:
    """Return a small set of fragment path hints for debugging."""
```

### Row Group Statistics

```python
def row_group_stats(
    fragments: Sequence[ds.Fragment],
    *,
    schema: SchemaLike,
    columns: Sequence[str],
    max_row_groups: int | None = None,
) -> list[JsonDict]:
    """Return row-group statistics for selected columns.

    Each row-group entry includes:
    - file: Fragment file path
    - row_group: Index within file
    - num_rows: Row count
    - total_byte_size: Size in bytes
    - columns: Per-column stats (null_count, distinct_count, min, max)
    - sorting_columns: Sorting specification when available
    """
```

---

## DataFusion Run Tracking

**File:** `src/obs/datafusion_runs.py`

The DataFusion run tracking module provides correlation IDs and lifecycle management for pipeline executions.

### DataFusionRun Structure

```python
@dataclass
class DataFusionRun:
    """Run envelope for DataFusion execution with correlation ID.

    Tracks both run lifecycle (start/end times, status) and commit sequence
    for idempotent Delta writes.
    """

    run_id: str
    label: str
    start_time_unix_ms: int
    end_time_unix_ms: int | None = None
    status: str = "running"
    metadata: dict[str, object] = field(default_factory=dict)
    _commit_sequence: int = 0
```

### Run Lifecycle Management

**Starting a Run:**
```python
def start_run(
    *,
    label: str,
    sink: DiagnosticsSink | None = None,
    metadata: Mapping[str, object] | None = None,
) -> DataFusionRun:
    """Start a new DataFusion run with correlation ID."""
```

**Finishing a Run:**
```python
def finish_run(
    run: DataFusionRun,
    *,
    sink: DiagnosticsSink | None = None,
    status: str = "completed",
    metadata: Mapping[str, object] | None = None,
) -> DataFusionRun:
    """Finish a DataFusion run and record completion."""
```

**Context Manager:**
```python
@contextmanager
def tracked_run(
    *,
    label: str,
    sink: DiagnosticsSink | None = None,
    metadata: Mapping[str, object] | None = None,
) -> Iterator[DataFusionRun]:
    """Context manager for automatic run lifecycle tracking.

    Example
    -------
    >>> with tracked_run(label="query_execution", sink=diagnostics) as run:
    ...     df = ctx.sql("SELECT * FROM table")
    ...     result = df.to_arrow_table()
    """
```

### Idempotent Write Support

The run envelope tracks commit sequences for idempotent Delta writes:

```python
def next_commit_version(self) -> tuple[IdempotentWriteOptions, DataFusionRun]:
    """Return idempotent options and updated run for next commit.

    Returns tuple of (idempotent_options, updated_run) where:
    - idempotent_options: Contains app_id and version for Delta write
    - updated_run: Run with incremented commit sequence
    """
```

---

## Parquet Metadata Utilities

**File:** `src/obs/metrics.py`

The metrics module provides utilities for managing Parquet metadata sidecars.

### ParquetMetadataSpec

```python
@dataclass(frozen=True)
class ParquetMetadataSpec:
    """Parquet metadata sidecar configuration."""

    schema: SchemaLike
    file_metadata: tuple[pq.FileMetaData, ...] = ()

    def write_common_metadata(
        self,
        base_dir: PathLike,
        *,
        filename: str = "_common_metadata",
    ) -> str:
        """Write a _common_metadata sidecar file."""

    def write_metadata(
        self,
        base_dir: PathLike,
        *,
        filename: str = "_metadata",
    ) -> str | None:
        """Write a _metadata sidecar file with row-group stats when available."""
```

### Metadata Collection

```python
def parquet_metadata_collector(
    fragments: Sequence[ds.Fragment],
) -> tuple[pq.FileMetaData, ...]:
    """Collect Parquet file metadata from dataset fragments."""

def parquet_metadata_factory(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
) -> ParquetMetadataSpec:
    """Return a ParquetMetadataSpec for a dataset."""
```

---

## Critical Files Reference

| File | Lines | Purpose |
|------|-------|---------|
| `src/obs/diagnostics.py` | ~213 | DiagnosticsCollector and event recording |
| `src/obs/metrics.py` | ~732 | Dataset/column stats, quality tables, fragment utilities |
| `src/obs/scan_telemetry.py` | ~148 | ScanTelemetry capture and fragment analysis |
| `src/obs/datafusion_runs.py` | ~294 | Run envelope tracking with correlation IDs |

---

## Integration Patterns

### Hamilton Pipeline Integration

```python
from obs.diagnostics import DiagnosticsCollector
from hamilton_pipeline.driver_factory import build_driver

# Create diagnostics collector
diagnostics = DiagnosticsCollector()

# Pass to driver construction
driver = build_driver(
    config=config,
    diagnostics=diagnostics,
)

# After execution, access collected events
events = diagnostics.events_snapshot()
artifacts = diagnostics.artifacts_snapshot()
```

### DataFusion Session Integration

```python
from obs.diagnostics import DiagnosticsCollector, record_view_fingerprints
from datafusion_engine.view_graph_registry import build_view_graph

diagnostics = DiagnosticsCollector()
view_nodes = build_view_graph(ctx, profile)

# Record view fingerprints
record_view_fingerprints(diagnostics, view_nodes=view_nodes)
```

### Quality Table Generation

```python
from obs.metrics import quality_from_ids, concat_quality_tables

# Generate quality rows for each source table
quality_tables = [
    quality_from_ids(
        nodes_table,
        id_col="node_id",
        entity_kind="NODE",
        issue="INVALID_ID",
        source_table="cpg_nodes",
    ),
    quality_from_ids(
        props_table,
        id_col="entity_id",
        entity_kind="PROPERTY",
        issue="INVALID_ENTITY_ID",
        source_table="cpg_props",
    ),
]

# Combine into single quality table
combined_quality = concat_quality_tables(quality_tables)
```

### Scan Telemetry Capture

```python
from obs.scan_telemetry import fragment_telemetry, ScanTelemetryOptions

options = ScanTelemetryOptions(
    hint_limit=10,
    required_columns=["id", "name"],
)

telemetry = fragment_telemetry(
    dataset,
    predicate=filter_expr,
    options=options,
)

print(f"Fragments: {telemetry.fragment_count}")
print(f"Row groups: {telemetry.row_group_count}")
print(f"Estimated rows: {telemetry.estimated_rows}")
```

---

## Summary

The Observability Layer provides a comprehensive toolkit for monitoring and debugging CodeAnatomy's inference pipeline:

1. **DiagnosticsCollector**: Central sink for events, artifacts, and metrics
2. **Dataset/Column Statistics**: Schema-aware statistics computation
3. **Quality Tables**: Tracking of invalid entities without pipeline failures
4. **Scan Telemetry**: Fragment-level performance metrics
5. **Run Tracking**: Correlation IDs and lifecycle management

These components integrate with both Hamilton orchestration and DataFusion execution to provide end-to-end observability across the pipeline.
