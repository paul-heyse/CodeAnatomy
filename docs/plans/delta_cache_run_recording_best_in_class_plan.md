# Delta Cache Run Recording & Diagnostics — Best-in-Class Plan

Date: 2026-02-01  
Owner: Codex (design phase)  
Status: implemented (2026-02-01)

## Purpose
Deliver a best‑in‑class run‑recording and diagnostics layer for Delta‑backed caches that combines **real‑time OpenTelemetry** (traces/metrics/logs) with **durable Delta ledgers** (commit metadata + inventory + run summaries). The end state provides rapid operational visibility, deterministic auditability, and reproducible cache forensics without high‑cardinality blowups.

## Design Principles
- **Two‑plane observability**: OTel for live signals, Delta for durable ground truth.
- **Collector‑first**: emit OTLP to a Collector for tail sampling, redaction, routing.
- **Low‑cardinality by default**: paths/IDs hashed or stored only in Delta ledgers.
- **Commit metadata as truth**: every cache write is annotated at the Delta log level.
- **Trace‑log correlation**: all diagnostic logs carry trace/span IDs.

## Implementation Update (2026-02-01)
- All six scopes are implemented in the codebase.
- Checklists below are updated to reflect completed scope and the final implementation details.

---

## Scope 1 — Cache Telemetry Primitives (Traces + Metrics)

### Objective
Create a dedicated cache telemetry helper that emits **cache spans** and **cache metrics** for every Delta‑backed cache operation (view caches, dataset caches, runtime artifacts, metadata snapshots). Metrics are RED‑style and low‑cardinality.

### Representative code pattern
```python
# src/obs/otel/cache.py (NEW)
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from typing import Callable, Iterator

from obs.otel.metrics import record_cache_event
from obs.otel.scopes import SCOPE_DATAFUSION
from obs.otel.tracing import get_tracer, record_exception, span_attributes


@contextmanager
def cache_span(
    name: str,
    *,
    cache_policy: str,
    cache_scope: str,
    operation: str,
    attributes: Mapping[str, object] | None = None,
) -> Iterator[tuple[object, Callable[[str], None]]]:
    base = {
        "cache.policy": cache_policy,
        "cache.scope": cache_scope,
        "cache.operation": operation,
    }
    if attributes:
        base.update(attributes)
    tracer = get_tracer(SCOPE_DATAFUSION)
    result = "ok"
    with tracer.start_as_current_span(name, attributes=span_attributes(attrs=base)) as span:
        def set_result(value: str) -> None:
            nonlocal result
            result = value

        try:
            yield span, set_result
        except Exception as exc:
            result = "error"
            record_exception(span, exc)
            raise
        finally:
            record_cache_event(
                cache_policy=cache_policy,
                cache_scope=cache_scope,
                operation=operation,
                result=result,
            )
```

### Target files to modify
- `src/obs/otel/cache.py` (new helper)
- `src/obs/otel/metrics.py` (add cache counters/histograms + Views)
- `src/datafusion_engine/views/graph.py` (view cache spans/metrics)
- `src/datafusion_engine/dataset/registration.py` (dataset cache spans/metrics)
- `src/relspec/runtime_artifacts.py` (runtime artifact cache spans/metrics)
- `src/datafusion_engine/cache/metadata_snapshots.py` (snapshot spans/metrics)

### Deprecate/delete after completion
- Ad‑hoc `logger.info` cache timing logs (superseded by spans + metrics).

### Implementation checklist
- [x] Add `obs.otel.cache.cache_span` helper and `record_cache_event` metrics hooks.
- [x] Extend `obs.otel.metrics` with cache instruments + bounded Views.
- [x] Wrap cache write/read paths with `cache_span` in view/dataset/artifact/snapshot flows.
- [x] Emit cache result status (`write`/`hit`/`miss`/`error`) as metric attributes (low‑cardinality).

---

## Scope 2 — Standardized Delta Commit Metadata for Cache Writes

### Objective
Every cache write should include consistent **commitInfo metadata** in the Delta log so cache runs are auditable even without OTel. This metadata becomes the canonical, durable record of cache operations.

### Representative code pattern
```python
# src/datafusion_engine/cache/commit_metadata.py (NEW)
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from obs.otel.run_context import get_run_id


@dataclass(frozen=True)
class CacheCommitMetadataRequest:
    operation: str
    cache_policy: str
    cache_scope: str
    schema_hash: str | None = None
    plan_hash: str | None = None
    cache_key: str | None = None
    result: str | None = None
    extra: Mapping[str, object] | None = None


def cache_commit_metadata(request: CacheCommitMetadataRequest) -> dict[str, str]:
    payload: dict[str, object] = {
        "operation": request.operation,
        "cache_policy": request.cache_policy,
        "cache_scope": request.cache_scope,
    }
    run_id = get_run_id()
    if run_id:
        payload["run_id"] = run_id
    if request.schema_hash:
        payload["schema_hash"] = request.schema_hash
    if request.plan_hash:
        payload["plan_hash"] = request.plan_hash
    if request.cache_key:
        payload["cache_key"] = request.cache_key
    if request.result:
        payload["cache_result"] = request.result
    if request.extra:
        payload.update({str(k): v for k, v in request.extra.items() if v is not None})
    return {str(k): str(v) for k, v in payload.items() if v is not None}
```

```python
# Example usage in a cache write
WriteRequest(
    source=df,
    destination=cache_path,
    format=WriteFormat.DELTA,
    mode=WriteMode.OVERWRITE,
    format_options={
        "commit_metadata": cache_commit_metadata(
            CacheCommitMetadataRequest(
                operation="cache_write",
                cache_policy="dataset_delta_staging",
                cache_scope="dataset",
                schema_hash=schema_identity_hash,
                plan_hash=plan_identity_hash,
            )
        )
    },
)
```

### Target files to modify
- `src/datafusion_engine/cache/commit_metadata.py` (new)
- `src/datafusion_engine/dataset/registration.py`
- `src/datafusion_engine/views/graph.py`
- `src/relspec/runtime_artifacts.py`
- `src/datafusion_engine/cache/metadata_snapshots.py`
- `src/datafusion_engine/cache/inventory.py`
- `src/datafusion_engine/cache/ledger.py`

### Deprecate/delete after completion
- Per‑module ad‑hoc commit metadata payloads (replace with shared helper).

### Implementation checklist
- [x] Implement `cache_commit_metadata` helper.
- [x] Use it in all Delta cache writes (dataset/view/runtime/snapshot/inventory/ledgers).
- [x] Ensure `run_id`, `schema_hash`, `plan_hash` are populated where available.
- [x] Update inventory entries to include `run_id` (see Scope 3).

---

## Scope 3 — Cache Ledger Tables (Inventory + Run Summary + Snapshot Registry)

### Objective
Extend cache observability with **Delta ledger tables** that provide a run‑level summary and explicit snapshot registry. These are durable, queryable, and independent of OTel sampling.

### Representative code pattern
```python
# src/datafusion_engine/cache/ledger.py (NEW)
from dataclasses import dataclass
from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from datafusion_engine.io.ingest import datafusion_from_arrow
import pyarrow as pa

@dataclass(frozen=True)
class CacheRunSummary:
    run_id: str
    start_time_unix_ms: int
    end_time_unix_ms: int | None
    cache_root: str
    total_writes: int
    total_reads: int
    error_count: int

    def to_row(self) -> dict[str, object]:
        return self.__dict__.copy()


def record_cache_run_summary(profile, summary: CacheRunSummary) -> None:
    table = pa.Table.from_pylist([summary.to_row()])
    df = datafusion_from_arrow(profile.session_context(), "cache_run_summary_append", table)
    WritePipeline(profile.session_context(), runtime_profile=profile).write(
        WriteRequest(
            source=df,
            destination=f"{profile.cache_root()}/cache_ledgers/datafusion_cache_run_summary_v1",
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
        )
    )
```

### Target files to modify
- `src/datafusion_engine/cache/ledger.py` (new)
- `src/datafusion_engine/cache/inventory.py` (add `run_id`, `row_count`, `file_count`)
- `src/datafusion_engine/cache/registry.py` (record `run_id`, `result`, `row_count`, `file_count`)
- `src/hamilton_pipeline/execution.py` (hook run summary lifecycle)
- `src/obs/otel/cache.py` (collect run stats)
- `src/obs/otel/run_context.py` (ensure run_id always set during pipeline execution)

### Deprecate/delete after completion
- None (additive ledger layer).

### Implementation checklist
- [x] Add run summary table + registry schema.
- [x] Extend cache inventory entries with `run_id`, `result`, `row_count`, `file_count`.
- [x] Add snapshot registry table for metadata cache snapshots.
- [x] Emit run summary on pipeline completion (via pipeline execution hooks).

---

## Scope 4 — OTel Logs Integration for Diagnostics Artifacts

### Objective
Route diagnostics artifacts/events to OTel logs with trace/log correlation while keeping existing in‑memory diagnostics for tests.

### Representative code pattern
```python
# src/obs/otel/logs.py (NEW sink example)
from obs.otel.logs import emit_diagnostics_event

class OtelDiagnosticsSink:
    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        emit_diagnostics_event(name, payload=payload, event_kind="artifact")

    def record_event(self, name: str, properties: Mapping[str, object]) -> None:
        emit_diagnostics_event(name, payload=properties, event_kind="event")
```

### Target files to modify
- `src/obs/otel/logs.py` (add diagnostics logger + sink)
- `src/datafusion_engine/lineage/diagnostics.py` (wire optional OTel sink)
- `src/engine/runtime_profile.py` (enable sink via env override)

### Deprecate/delete after completion
- None (keep in‑memory sink for tests).

### Implementation checklist
- [x] Implement `OtelDiagnosticsSink` with trace/log correlation.
- [x] Add config to select in‑memory vs OTel sink.
- [x] Route cache diagnostics artifacts to OTel logs when enabled.

---

## Scope 5 — Collector Configuration + Redaction Policies

### Objective
Add a reference Collector pipeline that performs redaction, tail sampling, and span‑to‑metrics generation for cache telemetry.

### Representative code pattern (Collector YAML)
```yaml
processors:
  attributes:
    actions:
      - key: cache.path
        action: delete
  redaction:
    blocked_values: ["/home/", "s3://"]
  tail_sampling:
    policies:
      - name: cache-errors
        type: status_code
        status_code: {status_codes: [ERROR]}
      - name: cache-latency
        type: latency
        latency: {threshold_ms: 2000}
connectors:
  spanmetrics: {}

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [attributes, redaction, tail_sampling]
      exporters: [spanmetrics, otlp]
    metrics:
      receivers: [spanmetrics]
      exporters: [otlp]
```

### Target files to modify
- `docs/architecture/observability.md` (collector config + redaction guidance)
- `docs/architecture/configuration_reference.md` (OTel env var examples)

### Deprecate/delete after completion
- Ad‑hoc telemetry notes without collector guidance (replace with canonical config snippet).

### Implementation checklist
- [x] Add Collector reference config (tail sampling + redaction + spanmetrics).
- [x] Document required env vars for OTLP pipeline.
- [x] Document path/PII redaction policy for cache attributes.

---

## Scope 6 — Validation & Tests

### Objective
Provide deterministic tests for cache telemetry and Delta commit metadata.

### Representative code pattern
```python
# tests/integration/test_cache_commit_metadata.py
commit = read_delta_commit_metadata(cache_path)
assert commit["cache_policy"] == "dataset_delta_staging"
assert commit["run_id"] == expected_run_id
```

### Target files to modify
- `tests/integration/test_delta_cache_alignment.py` (extend for commit metadata)
- `tests/obs/test_otel_metrics_contract.py` (in‑memory exporters)
- `tests/obs/test_otel_traces_contract.py`
- `tests/obs/test_otel_logs_contract.py`
- `tests/test_helpers/delta_commit.py`

### Deprecate/delete after completion
- None.

### Implementation checklist
- [x] Add commit metadata assertions for dataset/runtime/metadata cache writes.
- [x] Add in‑memory OTel tests for cache spans + metrics.
- [x] Validate run_id propagation in cache telemetry.

---

## Implementation Order
1. Scope 2 (commit metadata helper)  
2. Scope 1 (cache spans + metrics)  
3. Scope 3 (cache ledger tables)  
4. Scope 4 (OTel diagnostics sink)  
5. Scope 5 (Collector config + docs)  
6. Scope 6 (tests)

---

## Success Criteria
1. Every cache write includes standardized Delta commit metadata.
2. OTel spans and metrics exist for all cache operations with bounded cardinality.
3. Cache inventory + run summary tables exist and are queryable in Delta.
4. Diagnostics artifacts/events are emitted as OTel logs with correlation IDs.
5. Collector config supports tail sampling + redaction + spanmetrics.
6. Tests confirm commit metadata + telemetry emission.
