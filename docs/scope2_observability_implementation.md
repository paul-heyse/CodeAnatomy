# Scope 2: Observability and Run Tracking Implementation

## Overview

This document describes the implementation of Scope 2 from the Combined Library Utilization Plan: **Observability and Plan Artifacts (Run Envelope)**.

## Objective

Add structured run tracking with correlation IDs and plan artifact capture to enable machine-readable execution metrics and diagnostics for every DataFusion run.

## Key Components

### 1. Run Envelope Tracking (`src/obs/datafusion_runs.py`)

#### DataFusionRun Dataclass
- **Purpose**: Encapsulates run metadata with correlation ID
- **Fields**:
  - `run_id`: Unique UUID for the run
  - `label`: Human-readable label (e.g., "incremental_rebuild")
  - `start_time_unix_ms`: Unix timestamp in milliseconds
  - `end_time_unix_ms`: Optional end timestamp
  - `status`: Run status ("running", "completed", "failed")
  - `metadata`: Flexible metadata dictionary

#### API Functions

##### `start_run(*, label, sink, metadata) -> DataFusionRun`
Creates a new run envelope with a unique correlation ID and records the start event.

```python
from obs.datafusion_runs import start_run
from obs.diagnostics import DiagnosticsCollector

sink = DiagnosticsCollector()
run = start_run(label="query_execution", sink=sink, metadata={"user": "admin"})
print(f"Run started: {run.run_id}")
```

##### `finish_run(run, *, sink, status, metadata) -> DataFusionRun`
Completes a run envelope, recording the end timestamp and final status.

```python
from obs.datafusion_runs import finish_run

finish_run(run, sink=sink, status="completed")
print(f"Run completed in {run.payload()['duration_ms']}ms")
```

##### `tracked_run(*, label, sink, metadata) -> Iterator[DataFusionRun]`
Context manager for automatic run lifecycle management with error handling.

```python
from obs.datafusion_runs import tracked_run

with tracked_run(label="data_pipeline", sink=sink) as run:
    # Automatically records start
    df = ctx.sql("SELECT * FROM large_table")
    result = df.to_arrow_table()
    # Automatically records completion
```

### 2. Schema Definitions (`src/datafusion_engine/schema_registry.py`)

#### DATAFUSION_RUNS_V1 Schema
```python
pa.schema([
    pa.field("run_id", pa.string(), nullable=False),
    pa.field("label", pa.string(), nullable=False),
    pa.field("start_time_unix_ms", pa.int64(), nullable=False),
    pa.field("end_time_unix_ms", pa.int64(), nullable=True),
    pa.field("status", pa.string(), nullable=False),
    pa.field("duration_ms", pa.int64(), nullable=True),
    pa.field("metadata", pa.string(), nullable=True),  # JSON string
])
```

#### DATAFUSION_PLAN_ARTIFACTS_V1 Schema
Extended schema capturing comprehensive plan diagnostics:
- Event timestamp and run correlation
- Plan hash and SQL text
- EXPLAIN/EXPLAIN ANALYZE artifact paths
- Substrait serialization and validation
- Unparsed SQL and error information
- Logical, optimized, and physical plans
- Graphviz visualization
- Partition count and join operators

### 3. Enhanced Plan Artifacts (`src/datafusion_engine/bridge.py`)

#### DataFusionPlanArtifacts Enhancements
- **New field**: `run_id: str | None` - Correlation ID for run tracking
- **New method**: `structured_explain_payload() -> dict[str, object]`
  - Returns a payload structured for diagnostics table builders
  - Includes timestamp, run_id, and all plan details
  - Compatible with `datafusion_plan_artifacts_table()`

#### Updated `collect_plan_artifacts()`
- Accepts optional `run_id` parameter
- Threads correlation ID through artifact collection
- Returns artifacts with run correlation

### 4. Compilation Options (`src/datafusion_engine/compile_options.py`)

#### DataFusionCompileOptions Extension
- **New field**: `run_id: str | None`
  - Enables automatic correlation of plan artifacts with run envelopes
  - Propagated through the compilation pipeline
  - Optional for backward compatibility

## Integration Points

### Diagnostics Sink Protocol
All run tracking integrates with the existing `DiagnosticsSink` protocol:

```python
class DiagnosticsSink(Protocol):
    def record_events(self, name: str, rows: list[Mapping[str, object]]) -> None: ...
    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None: ...
```

### Artifact Recording Flow
1. User calls `start_run()` → Records "datafusion_run_started_v1"
2. Query executes with `run_id` in compile options
3. Plan artifacts collected with correlation ID
4. `plan_artifacts_hook` receives `structured_explain_payload()`
5. User calls `finish_run()` → Records "datafusion_run_finished_v1"

### Example End-to-End Usage

```python
from datafusion import SessionContext
from obs.datafusion_runs import tracked_run
from obs.diagnostics import DiagnosticsCollector
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.bridge import ibis_to_datafusion

sink = DiagnosticsCollector()

with tracked_run(label="user_analytics", sink=sink) as run:
    ctx = SessionContext()

    options = DataFusionCompileOptions(
        run_id=run.run_id,
        capture_plan_artifacts=True,
        explain_analyze=True,
        plan_artifacts_hook=lambda payload: sink.record_artifact(
            "datafusion_plan_artifacts_v1", payload
        ),
    )

    df = ibis_to_datafusion(expr, backend=backend, ctx=ctx, options=options)
    result = df.to_arrow_table()

# Artifacts now correlated:
artifacts = sink.artifacts_snapshot()
print(artifacts["datafusion_run_started_v1"])
print(artifacts["datafusion_run_finished_v1"])
print(artifacts["datafusion_plan_artifacts_v1"])
```

## Benefits

### 1. Correlation and Traceability
- Every plan artifact linked to a specific run via `run_id`
- Easy to correlate diagnostics across multiple queries in a session
- Supports distributed tracing integration (future: OTLP export)

### 2. Machine-Readable Metrics
- Structured timestamps for latency analysis
- Status tracking for success/failure rates
- Duration computation for performance monitoring

### 3. Error Context
- Automatic error capture in failed runs
- Metadata attachment for debugging context
- Stack-compatible with existing error handling

### 4. Artifact Management
- Plan artifacts stored with correlation IDs
- Reproducible artifact payloads
- Schema-validated diagnostics tables

## Testing

Run the test suite:
```bash
uv run python test_scope2_implementation.py
```

Tests cover:
- Module imports and schema registration
- Run lifecycle (start/finish)
- Context manager (tracked_run)
- Error handling
- Table builders with coercion logic

## Future Enhancements

### Tracing Span Integration
- Wire DataFusion tracing spans to diagnostics sink
- Emit span events as structured diagnostics
- Correlate spans with run_id

### OTLP Export
- Export run envelopes as OpenTelemetry traces
- Send to observability backends (Jaeger, Tempo, etc.)
- Enable distributed tracing across services

### Cache Introspection
- Record cache hit/miss rates per run
- Capture cache statistics with correlation
- Enable cache performance analysis

### Metrics Aggregation
- Roll up run metrics into summary tables
- Compute percentiles for duration
- Track error rates by label

## Files Modified

### New Files
- `/home/paul/CodeAnatomy/src/obs/datafusion_runs.py`

### Modified Files
- `/home/paul/CodeAnatomy/src/datafusion_engine/schema_registry.py`
- `/home/paul/CodeAnatomy/src/datafusion_engine/bridge.py`
- `/home/paul/CodeAnatomy/src/datafusion_engine/compile_options.py`

## References

- Combined Library Utilization Plan: Scope 2 (lines 66-110)
- Existing diagnostics patterns: `src/obs/diagnostics.py`
- Plan artifact capture: `src/datafusion_engine/bridge.py:collect_plan_artifacts`
