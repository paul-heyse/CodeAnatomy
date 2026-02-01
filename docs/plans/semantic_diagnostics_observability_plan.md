# Semantic Diagnostics Observability Plan

> **Objective:** Capture semantic quality diagnostics without a dashboard by emitting structured artifacts/events/metrics in `src/obs`, while persisting full-fidelity data to Delta and correlating everything with run metadata.

---

## Executive Summary

This plan introduces a diagnostics-first observability layer for semantic quality outputs. It focuses on:

1. Standardized diagnostic payloads for semantic quality artifacts and event streams
2. Delta-backed snapshots for full-fidelity diagnostics with pointer artifacts
3. Low-cardinality metrics and normalized issue streams for lightweight inspection
4. Centralized emission wiring with explicit policy gating and run correlation
5. Tests to validate payload shapes and emission behavior

---

## Scope 1: Standardize Semantic Quality Diagnostics Payloads

### Goal
Create typed, versioned payloads for semantic quality artifacts and events, and emit them consistently via `DiagnosticsCollector` and OTel logs.

### Status
**Completed.** Implemented in `src/obs/diagnostics.py` with `SemanticQualityArtifact`, `record_semantic_quality_artifact`, and `record_semantic_quality_events`.

### Key Architectural Elements

```python
# src/obs/diagnostics.py

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from datafusion_engine.lineage.diagnostics import ensure_recorder_sink
from serde_msgspec import StructBaseCompat
from utils.uuid_factory import uuid7_str

_OBS_SESSION_ID: Final[str] = uuid7_str()


@dataclass(frozen=True)
class SemanticQualityArtifact(StructBaseCompat, frozen=True):
    """Summary payload for a semantic diagnostics artifact."""

    name: str
    row_count: int
    schema_hash: str | None
    artifact_uri: str | None
    run_id: str | None

    def payload(self) -> Mapping[str, object]:
        return {
            "name": self.name,
            "row_count": self.row_count,
            "schema_hash": self.schema_hash,
            "artifact_uri": self.artifact_uri,
            "run_id": self.run_id,
        }


def record_semantic_quality_artifact(
    sink: DiagnosticsSink,
    *,
    artifact: SemanticQualityArtifact,
) -> None:
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact("semantic_quality_artifact_v1", artifact.payload())


def record_semantic_quality_events(
    sink: DiagnosticsSink,
    *,
    name: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_events(name, rows)
```

### Target Files
- `src/obs/diagnostics.py`
- `src/obs/otel/logs.py`
- `src/datafusion_engine/lineage/diagnostics.py`

### Deprecate/Delete After Completion
- None (new diagnostics payloads)

### Implementation Checklist
- [x] Add typed artifact payloads for semantic quality summaries
- [x] Add recorder helpers for semantic diagnostics events
- [x] Emit `semantic_quality_artifact_v1` artifacts with `run_id` and schema hash

---

## Scope 2: Delta-Backed Diagnostic Snapshots + Pointer Artifacts

### Goal
Persist full diagnostic datasets (`file_quality_v1`, `relationship_quality_metrics_v1`, `relationship_ambiguity_report_v1`, `file_coverage_report_v1`) to Delta and emit pointer artifacts that describe where the data lives.

### Status
**Partially completed.** Pointer artifacts are emitted from the semantic pipeline. Diagnostic views are materialized to Delta when output locations are configured. A standalone snapshot writer exists but is not yet wired into an incremental pipeline path.

### Key Architectural Elements

```python
# src/semantics/incremental/metadata.py

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from datafusion_engine.arrow.build import empty_table
from datafusion_engine.arrow.interop import TableLike
from semantics.incremental.write_helpers import (
    IncrementalDeltaWriteRequest,
    write_delta_table_via_pipeline,
)


@dataclass(frozen=True)
class SemanticDiagnosticsSnapshot:
    name: str
    table: TableLike | pa.Table
    destination: Path


def write_semantic_diagnostics_snapshots(
    *,
    runtime: IncrementalRuntime,
    snapshots: Mapping[str, SemanticDiagnosticsSnapshot],
) -> dict[str, str]:
    updated: dict[str, str] = {}
    for name, snapshot in snapshots.items():
        table = snapshot.table if snapshot.table is not None else empty_table(pa.schema([]))
        write_delta_table_via_pipeline(
            runtime=runtime,
            table=table,
            request=IncrementalDeltaWriteRequest(
                destination=str(snapshot.destination),
                mode=WriteMode.OVERWRITE,
                schema_mode="overwrite",
                commit_metadata={"snapshot_kind": name},
                operation_id=f"semantic_diagnostics::{name}",
            ),
        )
        updated[name] = str(snapshot.destination)
    return updated
```

### Target Files
- `src/semantics/incremental/metadata.py`
- `src/semantics/catalog/analysis_builders.py`
- `src/semantics/diagnostics.py`

### Deprecate/Delete After Completion
- None (new Delta snapshot path)

### Implementation Checklist
- [x] Introduce a diagnostics snapshot writer for semantic quality tables
- [x] Emit pointer artifacts including `artifact_uri`, row counts, and schema hash
- [ ] Ensure snapshots use deterministic commit metadata for easy retrieval (diagnostic views currently use semantic output commit metadata; wire `write_semantic_diagnostics_snapshots` if `snapshot_kind` metadata is required)

---

## Scope 3: Issue Stream Normalization + Low-Cardinality Metrics

### Goal
Normalize row-level issues into a single lightweight stream and emit low-cardinality counters for quality thresholds.

### Status
**Completed.** Normalization helpers live in `src/obs/metrics.py`, and issue batches are emitted from the semantic pipeline.

### Key Architectural Elements

```python
# src/obs/metrics.py

from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion_engine.arrow.build import rows_to_table
from obs.otel.metrics import record_artifact_count


def quality_issue_rows(
    *,
    entity_kind: str,
    rows: Sequence[Mapping[str, object]],
    source_table: str,
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append(
            {
                "entity_kind": entity_kind,
                "entity_id": row.get("entity_id"),
                "issue": row.get("issue"),
                "source_table": source_table,
            }
        )
    return normalized


def record_quality_issue_counts(*, issue_kind: str, count: int) -> None:
    for _ in range(count):
        record_artifact_count(issue_kind, status="ok", attributes={"artifact.type": "event"})
```

### Target Files
- `src/obs/metrics.py`
- `src/obs/otel/metrics.py`
- `src/semantics/diagnostics.py`

### Deprecate/Delete After Completion
- None (new issue normalization stream)

### Implementation Checklist
- [x] Normalize issue rows into `QUALITY_SCHEMA`
- [x] Emit low-cardinality counters for thresholds (low confidence, ambiguity, missing coverage)
- [x] Keep metrics attribute sets minimal to avoid cardinality blowups

---

## Scope 4: Centralized Emission Wiring + Policy Gating

### Goal
Emit diagnostics at a single pipeline boundary with explicit policy switches and consistent run correlation.

### Status
**Completed.** Policy flag added to `DiagnosticsPolicy` and propagated to `DataFusionRuntimeProfile`; emission is centralized in `semantics.pipeline.build_cpg`.

### Key Architectural Elements

```python
# src/relspec/pipeline_policy.py

@dataclass(frozen=True)
class DiagnosticsPolicy(FingerprintableConfig):
    """Diagnostics capture policy for pipeline execution."""

    capture_datafusion_metrics: bool = True
    capture_datafusion_traces: bool = True
    capture_datafusion_explains: bool = True
    explain_analyze: bool = True
    explain_analyze_level: str | None = "summary"
    emit_kernel_lane_diagnostics: bool = True
    emit_semantic_quality_diagnostics: bool = True


# src/semantics/runtime.py (or semantic pipeline boundary)

def emit_semantic_quality_diagnostics(
    *,
    runtime: SemanticRuntime,
    diagnostics: DiagnosticsCollector | None,
) -> None:
    if diagnostics is None:
        return
    policy = runtime.profile.pipeline_policy.diagnostics
    if not policy.emit_semantic_quality_diagnostics:
        return
    # Build summary DataFrames and emit pointer artifacts + top-N issue events
```

### Target Files
- `src/relspec/pipeline_policy.py`
- `src/semantics/runtime.py`
- `src/engine/diagnostics.py`
- `src/obs/diagnostics.py`

### Deprecate/Delete After Completion
- None (policy gating and centralized emission)

### Implementation Checklist
- [x] Add `emit_semantic_quality_diagnostics` policy flag
- [x] Centralize emission at a single pipeline boundary
- [x] Ensure events/artifacts include run correlation (`run_id`) via `obs.otel.run_context.get_run_id` (may be `None` if not set upstream)

---

## Scope 5: Tests for Diagnostic Emission

### Goal
Guarantee diagnostic payload shape and emission behavior without requiring a live OTel backend.

### Status
**Completed.** New unit tests added under `tests/unit/obs`, `tests/unit/semantics`, and `tests/unit/engine`.

### Key Architectural Elements

```python
# tests/unit/obs/test_semantic_diagnostics.py

from obs.diagnostics import DiagnosticsCollector, record_semantic_quality_artifact


def test_semantic_quality_artifact_payload() -> None:
    collector = DiagnosticsCollector()
    artifact = SemanticQualityArtifact(
        name="relationship_quality_metrics_v1",
        row_count=10,
        schema_hash="abc123",
        artifact_uri="/tmp/metrics",
        run_id="run_1",
    )
    record_semantic_quality_artifact(collector, artifact=artifact)
    artifacts = collector.artifacts_snapshot()
    assert "semantic_quality_artifact_v1" in artifacts
```

### Target Files
- `tests/unit/obs/test_semantic_diagnostics.py`
- `tests/unit/semantics/test_diagnostics.py`

### Deprecate/Delete After Completion
- None

### Implementation Checklist
- [x] Add unit tests for artifact payloads
- [x] Add tests for policy gating (emission on/off)
- [x] Add tests for issue normalization and counts

---

## Notes

- This plan intentionally avoids a dashboard and uses diagnostics artifacts/events as the primary surface.
- Full-fidelity data should be stored in Delta and referenced by pointer artifacts.
- All new diagnostics payloads should remain versioned with `_v1` suffixes.

## Remaining Follow-Ups

1. **Wire `write_semantic_diagnostics_snapshots` into an incremental pipeline path** so diagnostic snapshots always include `snapshot_kind` commit metadata and can be persisted independent of semantic output materialization.
2. **Decide on the canonical storage location strategy** for diagnostic snapshots when output locations are not provided (e.g., a state-store path or dedicated diagnostics output root).
3. **Ensure run correlation context is set** (`obs.otel.run_context.set_run_id`) in the entrypoint that drives semantic builds if you want non-`None` `run_id` values in artifact payloads.
