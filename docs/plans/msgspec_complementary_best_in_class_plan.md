# msgspec Complementary Best-in-Class Implementation Plan (Design-Phase, Breaking Changes OK)

Date: 2026-01-29  
Owner: Codex (design phase)  
Status: completed (Scopes 1–10 implemented)  
Validated: 2026-01-29 (codebase cross-check)

## Purpose
Define a best-in-class plan to implement the complementary msgspec work that can proceed in parallel with the remaining DataFusion+Delta plan. This plan focuses on schema rigor, deterministic artifact envelopes, validation ergonomics, schema export richness, and performance safeguards.

## Design Principles
- **Schema-first artifacts**: all externally persisted or diagnostic artifacts are msgspec Structs with explicit constraints.
- **Typed boundaries**: decode/convert at ingress and normalize validation errors for diagnostics.
- **Tagged envelopes**: multi-artifact streams use tagged unions for deterministic decoding.
- **Exportable contracts**: schemas are fully documented for JSON Schema/OpenAPI.
- **Performance-aware**: reuse encoders/decoders and use `encode_into` on hot paths.
- **Breaking changes OK**: prioritize correctness and long-term stability.

---

## Scope 1 — Add explicit field constraints via Annotated + Meta

### Objective
Use `Annotated[..., msgspec.Meta(...)]` to enforce constraints and enrich schemas for new artifacts (stats decisions, cache artifacts, semantic metadata, run manifests, writer policies).

### Representative code pattern
```python
from typing import Annotated
import msgspec

NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]
NonNegInt = Annotated[int, msgspec.Meta(ge=0)]

class RunManifest(StructBaseCompat, frozen=True):
    run_id: NonEmptyStr
    plan_signature: str | None = None
    plan_fingerprints: dict[str, str]
    delta_inputs: tuple[dict[str, object], ...]
    outputs: tuple[dict[str, object], ...]
    artifact_ids: dict[str, str] | None = None
```

### Target files to modify
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `tests/msgspec_contract/_support/models.py`
- `tests/msgspec_contract/test_contract_*.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add constraint aliases (NonEmptyStr, NonNegInt, BoundedInt, etc.) where needed.
- [x] Apply constraints to new artifact structs and writer-policy snapshots.
- [x] Ensure schema exports include constraint metadata.
- [x] Add/refresh msgspec contract snapshots (JSON/MsgPack/schema/errors).

---

## Scope 2 — Tagged artifact envelope for multi-artifact streams

### Objective
Introduce a tagged-union envelope for all artifact streams that mix multiple payload types (stats decision, cache artifact, semantic validation, run manifest, writer policy validation) to provide deterministic decoding and clearer schema contracts.

### Representative code pattern
```python
class ArtifactEnvelope(StructBaseCompat, tag=True, tag_field="kind", frozen=True):
    kind: str

class StatsDecisionEnvelope(ArtifactEnvelope, tag="delta_stats_decision"):
    payload: DeltaStatsDecision

class CacheArtifactEnvelope(ArtifactEnvelope, tag="view_cache_artifact"):
    payload: ViewCacheArtifact
```

### Target files to modify
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/plan_bundle.py`
- `tests/msgspec_contract/test_contract_*.py`

### Modules to delete
- Any ad-hoc dict “kind” wrappers for artifact payloads (once envelope is canonical).

### Implementation checklist
- [x] Add envelope base + tagged variants for each new artifact type.
- [x] Update artifact persistence and diagnostic recording to use envelopes.
- [x] Export envelope schemas via registry and add contract snapshots.

---

## Scope 3 — Schema registry enforcement tests using msgspec.inspect

### Objective
Add tests that validate the schema registry set is complete and that each artifact uses the correct struct configuration (strict vs compat, forbid_unknown_fields, omit_defaults, array_like rules).

### Representative code pattern
```python
from msgspec.inspect import is_struct_type, type_info

info = type_info(RunManifest)
assert is_struct_type(RunManifest)
assert RunManifest.__struct_config__.forbid_unknown_fields is False
```

### Target files to modify
- `tests/msgspec_contract/test_contract_schema_registry.py` (new)
- `tests/msgspec_contract/_support/models.py`
- `src/serde_schema_registry.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add a test that iterates `SCHEMA_TYPES` and validates config + presence.
- [x] Validate strict/compat expectations by category.
- [x] Ensure registry tests fail on accidental dict payloads or missing types.

---

## Scope 4 — Schema enrichment via Meta + schema_hook

### Objective
Enrich JSON Schema/OpenAPI output with titles, descriptions, examples, and vendor metadata to make the published schemas usable for tooling and documentation.

### Representative code pattern
```python
from msgspec import Meta
from typing import Annotated

PlanFingerprint = Annotated[
    str,
    Meta(description="Deterministic plan fingerprint for reproducibility")
]
```

```python
def schema_hook(schema: dict[str, object], obj: object) -> dict[str, object]:
    if obj is RunManifest:
        schema["x-codeanatomy-domain"] = "run_manifest"
    return schema
```

### Target files to modify
- `src/serde_schema_registry.py`
- `src/serde_artifacts.py`
- `docs/architecture/msgspec_schema_registry.openapi.json`
- `docs/architecture/msgspec_schema_registry.json`

### Modules to delete
- None.

### Implementation checklist
- [x] Add descriptions/examples for new artifacts and key fields.
- [x] Add a `schema_hook` for vendor metadata where beneficial.
- [x] Regenerate schema registry exports.

---

## Scope 5 — Normalized ValidationError snapshots for new artifacts

### Objective
Ensure every new artifact type has consistent ValidationError normalization (using `validation_error_payload`) and corresponding golden snapshots.

### Representative code pattern
```python
try:
    payload = convert(raw, target_type=RunManifest, strict=False)
except msgspec.ValidationError as exc:
    msg = f"RunManifest validation failed: {validation_error_payload(exc)}"
    raise ValueError(msg) from exc
```

### Target files to modify
- `src/serde_msgspec.py`
- `src/serde_artifacts.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `tests/msgspec_contract/test_contract_errors.py`

### Modules to delete
- Any bespoke ValidationError formatting in artifact ingestion paths.

### Implementation checklist
- [x] Add validation hooks for new artifacts in ingestion/persistence paths.
- [x] Extend error-golden tests to cover each new artifact payload.

---

## Scope 6 — Performance-safe encoding for high-volume artifacts

### Objective
Use reusable encoders/decoders and `encode_into` in hot paths (run manifests, policy snapshots, cache artifacts) to reduce allocations while preserving deterministic ordering.

### Representative code pattern
```python
buffer = bytearray()
JSON_ENCODER.encode_into(payload, buffer)
```

### Target files to modify
- `src/serde_msgspec.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/plan_bundle.py`

### Modules to delete
- Per-call encoder instantiations in hot paths once centralized.

### Implementation checklist
- [x] Identify hot paths in artifact persistence and switch to `encode_into`.
- [x] Ensure deterministic ordering is preserved in all encoders.
- [x] Add micro-benchmark notes (optional) in docs.

---

## Scope 7 — Array-like payloads only for telemetry

### Objective
Restrict `array_like=True` usage to telemetry-like payloads and formalize append-only evolution rules for any array-like structs that remain or are added.

### Representative code pattern
```python
class ScanTelemetry(StructBaseCompat, array_like=True, frozen=True):
    fragment_count: int
    row_group_count: int
    count_rows: int | None = None
```

### Target files to modify
- `src/obs/scan_telemetry.py`
- `src/obs/metrics.py`
- `src/serde_schema_registry.py`
- `tests/msgspec_contract/test_contract_schema_registry.py`

### Modules to delete
- Any array-like struct for non-telemetry payloads (after migration).

### Implementation checklist
- [x] Audit array-like structs and ensure they are telemetry-only.
- [x] Enforce append-only evolution in tests (field order and defaults).

---

## Scope 8 — New artifact types aligned to Delta plan gaps

### Objective
Add the missing msgspec artifacts that are required to complete the Delta plan: stats decision, view cache artifact, semantic column validation, run manifest, and writer-policy validation artifacts.

### Representative code patterns
```python
class DeltaStatsDecision(StructBaseCompat, frozen=True):
    dataset_name: str
    stats_policy: str
    stats_columns: tuple[str, ...] | None
    lineage_columns: tuple[str, ...]
```

```python
class ViewCacheArtifact(StructBaseCompat, frozen=True):
    view_name: str
    cache_policy: str
    cache_path: str | None
    plan_fingerprint: str | None
    hit: bool | None
```

```python
class SemanticColumnSpec(StructBaseCompat, frozen=True):
    column_name: str
    semantic_type: str
    metadata_key: str
```

```python
class RunManifest(StructBaseCompat, frozen=True):
    run_id: str
    plan_signature: str | None
    plan_fingerprints: dict[str, str]
    delta_inputs: tuple[dict[str, object], ...]
    outputs: tuple[dict[str, object], ...]
    artifact_ids: dict[str, str] | None
```

### Target files to modify
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/schema_contracts.py`
- `tests/msgspec_contract/test_contract_*.py`

### Modules to delete
- None (new artifacts are additive at this stage).

### Implementation checklist
- [x] Implement the new artifact structs.
- [x] Wire persistence/recording of these artifacts into plan and write paths.
- [x] Add contract snapshots (JSON/MsgPack/schema/error) for each new artifact.

---

## Scope 9 — Contract tests for writer policy payloads

### Objective
Ensure writer policy payloads (bloom/dict, schema mode, stats policies) have contract tests and validation error snapshots.

### Representative code pattern
```python
payload = WriterPolicySnapshot(...)
json_bytes = dumps_json(payload)
```

### Target files to modify
- `tests/msgspec_contract/test_contract_writer_policy.py` (new)
- `src/storage/deltalake/config.py`
- `src/serde_schema_registry.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add writer policy contract snapshots (JSON/MsgPack/errors).
- [x] Ensure schema registry exports include writer policy schemas.

---

## Scope 10 — Deferred deletions (post-scope cleanup)

### Objective
Remove legacy or duplicate paths only after all above scopes are implemented and validated.

### Candidates for deletion
- Ad-hoc dict wrappers for artifacts (superseded by tagged envelopes).
- Any bespoke ValidationError formatting once normalized handling is in place.
- Any remaining local encoder/decoder instances outside `serde_msgspec.py`.
- Any array-like payloads used outside telemetry after migration.

### Target files to modify
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/serde_msgspec.py`
- `src/obs/*.py`

### Implementation checklist
- [x] Confirm all new artifacts are persisted and tested end-to-end.
- [x] Remove legacy wrappers and ad-hoc serialization paths.
- [x] Update contract tests and schema exports after deletions.

---

## Final Design Endpoint (Best-in-Class)
- All new Delta-plan artifacts are represented as constrained msgspec Structs.
- Artifact streams use tagged envelopes for deterministic, typed decoding.
- Schema registry exports are richly documented (titles/descriptions/examples).
- Contract tests cover JSON/MsgPack/schema/error for every canonical artifact.
- ValidationError shapes are consistent and normalized across all boundaries.
- Performance-sensitive artifact paths use `encode_into` and shared encoders.
- Legacy ad-hoc artifact payloads and wrappers are removed post-migration.
