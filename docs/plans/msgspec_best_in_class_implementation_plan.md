# msgspec Best-in-Class Implementation Plan (Design-Phase, Breaking Changes OK)

Date: 2026-01-28  
Owner: Codex (design phase)  
Status: completed (all scopes implemented)  
Validated: 2026-01-29 (codebase cross-check)

## Purpose
Create a comprehensive, best‑in‑class integration plan for **msgspec** across the codebase. This plan favors correctness, determinism, contract clarity, and high‑performance serialization even when it introduces breaking changes.

## Goals
- Treat msgspec‑typed models as the **canonical data contracts** for plan artifacts, Delta policies, runtime snapshots, and diagnostics payloads.
- Make **serialization deterministic** and **schema‑exportable** (JSON Schema / OpenAPI) for all artifacts.
- Enable **contract testing** (golden JSON/MsgPack/schema/error snapshots) as a CI regression gate.
- Standardize **typed decoding/validation** at all external boundaries.
- Remove inconsistent or ad‑hoc encoding paths and legacy artifact shapes.

## Design Principles
- **Schema‑first**: artifact shapes are defined by msgspec Structs and constraints.
- **Determinism by default**: stable encoding for hashing, snapshots, and caching.
- **Compatibility by design**: explicit forward/backward evolution rules and versioning.
- **Performance-aware**: leverage msgspec’s fast codecs and zero-copy options.
- **Breaking changes acceptable**: prioritize long-term correctness and clarity.

---

## Scope 1 — msgspec Contract Harness (goldens + schema + errors)

### Objective
Add a **contract test harness** that snapshots JSON, MessagePack, JSON Schema, and ValidationError shapes for all canonical msgspec contracts.

### Representative Code Pattern
```python
# tests/msgspec_contract/_support/codecs.py
JSON_ENCODER = msgspec.json.Encoder(
    order="deterministic",
    decimal_format="string",
    uuid_format="canonical",
)
MSGPACK_ENCODER = msgspec.msgpack.Encoder(
    order="deterministic",
    decimal_format="string",
    uuid_format="canonical",
)
```

```python
# tests/msgspec_contract/test_contract_schema.py
schema = msgspec.json.schema(ArtifactEnvelope)
assert_text_snapshot(path=GOLDEN, text=json.dumps(schema, indent=2), update=update)
```

### Target files to modify
- `tests/msgspec_contract/__init__.py`
- `tests/msgspec_contract/conftest.py`
- `tests/msgspec_contract/_support/*.py`
- `tests/msgspec_contract/test_contract_*.py`
- `tests/plan_golden/test_plan_artifacts.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Implement contract harness scaffolding (goldens, codecs, normalize, models).
- [x] Add JSON/MsgPack/schema/error snapshot tests for core artifacts.
- [x] Add `--update-goldens` flow and CI gate.
- [x] Wire plan-golden tests to canonical msgspec artifacts (fixtures updated for new schema/proto payloads).

---

## Scope 2 — Canonical msgspec Model Layer for Artifacts

### Objective
Define **canonical msgspec Structs** for plan artifacts, view artifacts, and runtime snapshots and make them the source of truth for serialization and schema export.

### Representative Code Pattern
```python
# src/serde_artifacts.py (new)
class PlanArtifacts(StructBaseStrict):
    explain_tree_rows: tuple[dict[str, object], ...] | None
    information_schema_snapshot: dict[str, object]
    information_schema_hash: str
    substrait_validation: dict[str, object] | None
    rulepack_hash: str | None
```

```python
# src/datafusion_engine/plan_bundle.py
artifacts = PlanArtifacts(
    explain_tree_rows=tuple(explain_tree.rows) if explain_tree else None,
    information_schema_snapshot=dict(info_schema),
    information_schema_hash=info_hash,
    substrait_validation=substrait_validation,
    rulepack_hash=rulepack_hash,
)
```

### Target files to modify
- `src/serde_artifacts.py` (new)
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/view_artifacts.py`
- `src/engine/runtime_profile.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/diagnostics.py`

### Modules to delete
- Dataclass-based artifact payload types replaced by Structs (after full migration).

### Implementation checklist
- [x] Create `serde_artifacts.py` with msgspec Structs for plan/view/runtime artifacts.
- [x] Replace dataclass payloads with Structs in bundle/store/snapshots.
- [x] Ensure all typed decode paths use these structs (boundary decoding added).
- [x] Export JSON Schema for the canonical artifacts.

---

## Scope 3 — Dual Base Types (Strict vs Compatible)

### Objective
Introduce **two base Struct types**: strict for configs/inputs, compatible for persisted artifacts (forward‑compatible decoding).

### Representative Code Pattern
```python
class StructBaseStrict(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True,
                       repr_omit_defaults=True, forbid_unknown_fields=True):
    """Strict contract base."""

class StructBaseCompat(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True,
                       repr_omit_defaults=True, forbid_unknown_fields=False):
    """Persisted artifact base."""
```

### Target files to modify
- `src/serde_msgspec.py`
- `src/incremental/cdf_cursors.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/view_artifacts.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add `StructBaseStrict` and `StructBaseCompat` in `serde_msgspec.py`.
- [x] Migrate persisted artifacts to `StructBaseCompat`.
- [x] Keep configuration/command inputs on `StructBaseStrict`.

---

## Scope 4 — Deterministic Encoding Policy Consolidation

### Objective
Standardize **all encoding/decoding** through `serde_msgspec` with explicit order/decimal/uuid policies to avoid drift.

### Representative Code Pattern
```python
# src/serde_msgspec.py
JSON_ENCODER = msgspec.json.Encoder(
    enc_hook=_json_enc_hook,
    order="deterministic",
    decimal_format="string",
    uuid_format="canonical",
)
```

### Target files to modify
- `src/serde_msgspec.py`
- `src/obs/metrics.py`
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`

### Modules to delete
- Local encoder singletons defined outside `serde_msgspec.py`.

### Implementation checklist
- [x] Add deterministic encoder settings (decimal/uuid/order).
- [x] Replace ad‑hoc encoders in telemetry/runtime with shared helpers.
- [x] Ensure all hash/fingerprint functions use canonical encoding.

---

## Scope 5 — MsgPack Extension Types for Binary Plan Payloads

### Objective
Use MsgPack **Ext codes** for binary plan payloads (Substrait/proto), avoiding base64 inflation and making payloads self‑describing.

### Representative Code Pattern
```python
# src/serde_msgspec_ext.py
SUBSTRAIT_EXT_CODE: int = 10
DF_PROTO_EXT_CODE: int = 11
```

```python
# src/serde_msgspec.py
if isinstance(obj, SubstraitBytes):
    return msgspec.msgpack.Ext(SUBSTRAIT_EXT_CODE, obj.data)
```

### Target files to modify
- `src/serde_msgspec_ext.py`
- `src/serde_msgspec.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/plan_bundle.py`

### Modules to delete
- Base64 encoding paths for Substrait/proto payloads after full migration.

### Implementation checklist
- [x] Define new Ext codes for Substrait/proto payloads.
- [x] Update enc/dec hooks to emit and decode these types.
- [x] Replace base64 storage fields with MsgPack ext payloads.
- [x] Update plan artifact schema version.

---

## Scope 6 — Schema Registry + JSON Schema / OpenAPI Export

### Objective
Create a registry of msgspec contract types and export **JSON Schema/OpenAPI** artifacts to docs and/or artifact storage.

### Representative Code Pattern
```python
# src/serde_schema_registry.py (new)
SCHEMA_TYPES = (PlanArtifacts, PlanArtifactRow, DataFusionViewArtifact, DeltaWritePolicy)

schemas, components = msgspec.json.schema_components(SCHEMA_TYPES)
```

### Target files to modify
- `src/serde_schema_registry.py` (new)
- `docs/architecture/` (new schema export docs)
- `src/datafusion_engine/plan_bundle.py` (optional embed schema hash)

### Modules to delete
- None.

### Implementation checklist
- [x] Create registry module listing canonical msgspec types.
- [x] Export JSON Schema/OpenAPI spec to `docs/`.
- [x] Add schema hash into plan artifacts and determinism audit bundle.

---

## Scope 7 — Delta Policy Modeling + Validation

### Objective
Define Delta policy/config objects as msgspec Structs with constraints and use typed validation at write/scan boundaries.

### Representative Code Pattern
```python
class DeltaWritePolicy(StructBaseStrict):
    target_file_size: Annotated[int, msgspec.Meta(ge=1)] | None = None
    partition_by: tuple[str, ...] = ()
    zorder_by: tuple[str, ...] = ()
    stats_policy: Literal["off", "explicit", "auto"] = "auto"
```

### Target files to modify
- `src/storage/deltalake/config.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/scan_planner.py`
- `src/schema_spec/system.py`

### Modules to delete
- Ad‑hoc dict-based Delta policy payloads after migration.

### Implementation checklist
- [x] Add msgspec models for write policy, schema policy, scan config, and protocol gates.
- [x] Validate policy inputs via typed decode/convert at boundaries.
- [x] Persist policy snapshots in plan/write artifacts.

---

## Scope 8 — Typed Ingestion & Validation at Boundaries

### Objective
Ensure inbound payloads are **typed decoded** (msgspec decode/convert) with consistent error handling and diagnostics.

### Representative Code Pattern
```python
payload = loads_json(raw_bytes, target_type=PlanArtifactRow, strict=True)
```

### Target files to modify
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/view_artifacts.py`
- `src/incremental/metadata.py`
- `src/extract/*.py`

### Modules to delete
- Manual dict-to-dataclass parsing paths.

### Implementation checklist
- [x] Identify all external payload ingestion paths.
- [x] Replace manual parsing with typed msgspec decode/convert.
- [x] Normalize ValidationError output for diagnostics.

---

## Scope 9 — Telemetry Struct Tuning (array_like + UNSET policy)

### Objective
Optimize telemetry structs and explicitly avoid `UNSET` in `array_like=True` payloads; use defaults and append‑only evolution rules.

### Representative Code Pattern
```python
class ScanTelemetry(StructBaseCompat, array_like=True, gc=False, cache_hash=True):
    fragment_count: int
    row_group_count: int
    count_rows: int | None = None
    estimated_rows: int | None = None
```

### Target files to modify
- `src/obs/scan_telemetry.py`
- `src/obs/metrics.py`
- `src/datafusion_engine/runtime.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Ensure array-like structs never use `msgspec.UNSET`.
- [x] Apply append‑only field evolution rules to array‑like structs.
- [x] Use `encode_into` where hot‑path buffers exist.

---

## Scope 10 — Artifact Schema Cutover

### Objective
Introduce a new artifact schema version for msgspec‑native payloads with a hard cutover.

### Representative Code Pattern
```python
PLAN_ARTIFACTS_TABLE_NAME = "datafusion_plan_artifacts_v9"
```

### Target files to modify
- `src/datafusion_engine/plan_artifact_store.py`
- `tests/plan_golden/test_plan_artifacts.py`

### Modules to delete
- Deprecated schema columns after migration.

### Implementation checklist
- [x] Bump plan artifacts schema version.
- [x] Add migration notes (backfill tool deferred).
- [x] Update golden tests to new schema.

---

## Scope 11 — Determinism Audit Enhancements

### Objective
Extend determinism audit bundles with msgspec schema hashes and contract versioning.

### Representative Code Pattern
```python
bundle = {
    "plan_fingerprint": plan_fingerprint,
    "planning_env_hash": planning_env_hash,
    "schema_contract_hash": schema_contract_hash,
}
```

### Target files to modify
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `tests/plan_golden/test_plan_artifacts.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add schema contract hash to determinism audit bundle.
- [x] Snapshot determinism audit payloads in golden tests.

---

## Scope 12 — Deferred Deletions (post‑implementation cleanup)

### Objective
Remove legacy payloads and duplicate paths once all scopes are validated end‑to‑end.

### Candidates for deletion
- Dataclass‑only artifact payload types superseded by msgspec Structs.
- Base64 storage of Substrait/proto payloads once MsgPack ext is adopted.
- Local encoder instances outside `serde_msgspec.py`.
- Manual dict parsing/serialization for artifacts and policies.

### Target files to modify
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/view_artifacts.py`
- `src/serde_msgspec.py`
- `src/obs/metrics.py`

### Implementation checklist
- [x] Confirm all msgspec models are canonical and used end‑to‑end.
- [x] Remove legacy serialization fields and conversion code.
- [x] Update tests and docs to reflect removals.

---

## Final Design Endpoint (Best‑in‑Class)
- All plan/runtime/view/policy artifacts are msgspec Structs with explicit constraints.
- Deterministic encoding policy enforced globally; hashes/snapshots are stable.
- JSON Schema/OpenAPI specs are generated from code and published in docs.
- Contract tests gate drift across JSON, MsgPack, schema, and error shapes.
- Delta policy inputs are strictly validated and fully captured in artifacts.
- Binary plan payloads use MsgPack Ext codes (no base64 inflation).
- Legacy artifact shapes and ad‑hoc encoders are removed after migration.
