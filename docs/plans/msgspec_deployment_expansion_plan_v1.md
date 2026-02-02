# msgspec Deployment Expansion Plan (Design-Phase, Breaking Changes OK)

Date: 2026-02-02  
Owner: Codex (design phase)  
Status: draft  
Validated: pending

## Purpose
Expand msgspec usage beyond the current artifact and policy contracts to become the **primary typed boundary** for configuration, metadata payloads, diagnostics, and schema introspection. This plan prioritizes a best-in-class target architecture aligned with the semantic IR approach, even if it requires breaking changes.

## Goals
- Treat msgspec Structs as **canonical IR boundary contracts** across config, metadata, diagnostics, and artifact pipelines.
- Eliminate ad-hoc mapping normalization and stringly-typed payloads in favor of **typed conversion + validation**.
- Make boundary serialization **deterministic and schema-exportable** for downstream tooling and semantic IR enforcement.
- Provide **structured, versioned, and evolvable payloads** for metadata and logs.

## Design Principles
- **Schema-first IR**: data flowing between subsystems is defined by msgspec Structs and constrained types.
- **Boundary validation**: convert/validate at IO boundaries; keep core logic strongly typed.
- **Deterministic encoding**: all serialized payloads use shared msgspec encoders and stable ordering.
- **Compatibility by design**: explicit versioning and forward-compatible decoding for persisted payloads.
- **Breaking changes acceptable**: prefer long-term correctness over short-term compatibility.

---

## Scope 1 — Typed Config Ingestion (TOML/JSON -> msgspec Structs)

### Objective
Replace raw `tomllib`/`json.loads` config ingestion with msgspec-typed config models. Config normalization and environment overrides become **typed conversions** with precise validation errors.

### Representative code pattern
```python
# src/cli/config_models.py (new)
class PlanConfig(StructBaseStrict, frozen=True):
    allow_partial: bool | None = None
    requested_tasks: tuple[str, ...] | None = None

class CacheConfig(StructBaseStrict, frozen=True):
    policy_profile: str | None = None
    path: str | None = None
    log_to_file: bool | None = None

class RootConfig(StructBaseStrict, frozen=True):
    plan: PlanConfig | None = None
    cache: CacheConfig | None = None
    # ...other sections...
```

```python
# src/cli/config_loader.py (new flow)
raw = msgspec.toml.decode(buf, type=RootConfig, strict=True)
normalized = normalize_config_contents(raw)
```

### Target files
- `src/cli/config_loader.py`
- `src/cli/config_source.py`
- `src/cli/config_models.py` (new)
- `src/utils/file_io.py`

### Deprecate/delete after this scope
- `src/cli/config_loader.py::_read_toml` (replace with msgspec.toml.decode)
- `src/utils/file_io.py::read_toml` (replace with typed decoding)

### Implementation checklist
- [ ] Introduce `src/cli/config_models.py` with msgspec Structs for all config sections.
- [ ] Replace `tomllib` parsing in `config_loader` with `msgspec.toml.decode` + typed validation.
- [ ] Normalize config with typed inputs; ensure `validation_error_payload` is used for friendly CLI errors.
- [ ] Add tests for typed config error surfaces (invalid types, unknown fields).

---

## Scope 2 — Versioned Metadata Payloads (Arrow metadata, msgpack payloads)

### Objective
Replace manual metadata payload validation with versioned msgspec payload structs. Ensure consistent schema evolution and typed decoding for metadata maps/lists/scalars.

### Representative code pattern
```python
# src/datafusion_engine/arrow/metadata_codec.py (new)
class MetadataMapPayload(StructBaseCompat, frozen=True):
    version: int
    entries: tuple[tuple[str, str], ...]

payload = MetadataMapPayload(version=1, entries=tuple(sorted(entries.items())))
raw = dumps_msgpack(payload)

decoded = loads_msgpack(raw, target_type=MetadataMapPayload, strict=True)
```

### Target files
- `src/datafusion_engine/arrow/metadata_codec.py`
- `src/serde_msgspec.py` (shared decode helpers)

### Deprecate/delete after this scope
- Manual map/list validation in `metadata_codec.py` (remove bespoke checks)

### Implementation checklist
- [ ] Define msgspec payload structs for metadata map/list/scalar forms.
- [ ] Encode/decode metadata payloads via `dumps_msgpack/loads_msgpack` with typed targets.
- [ ] Add version field and forward-compat decoding rules.

---

## Scope 3 — Structured Logs + Diagnostics via msgspec

### Objective
Move JSONL logging and diagnostic payload generation to msgspec Structs with deterministic ordering and stable schema.

### Representative code pattern
```python
# src/hamilton_pipeline/structured_logs.py (new)
class StructuredLogEvent(StructBaseCompat, tag=True, tag_field="event"):
    run_id: str
    plan_signature: str
    timestamp_ms: int

encoder = msgspec.json.Encoder(order="deterministic")

# emit JSONL
handle.write(encoder.encode_lines(events))
```

### Target files
- `src/hamilton_pipeline/structured_logs.py`
- `src/cli/commands/delta.py` (report payloads)
- `src/obs/otel/logs.py` (diagnostic payloads)

### Deprecate/delete after this scope
- `json.dumps` calls for structured JSONL in logging/diagnostics modules
- dataclass-only report payloads that are solely serialized (replace with Structs)

### Implementation checklist
- [ ] Introduce msgspec Structs for log/report payloads.
- [ ] Replace JSONL emission with `msgspec.json.Encoder.encode_lines`.
- [ ] Ensure deterministic ordering for all diagnostic JSON outputs.

---

## Scope 4 — Boundary Conversion: Replace Manual Mapping Normalization

### Objective
Replace manual mapping normalization and ad-hoc validation with `msgspec.convert` at boundary entry points.

### Representative code pattern
```python
# src/datafusion_engine/delta/protocol.py
snapshot = convert(raw, target_type=DeltaProtocolSnapshot, strict=False)

# src/datafusion_engine/views/artifacts.py
payload = convert(payload_raw, target_type=ViewArtifactPayload, strict=True)
```

### Target files
- `src/datafusion_engine/delta/protocol.py`
- `src/datafusion_engine/views/artifacts.py`
- `src/extract/coordination/context.py`
- `src/datafusion_engine/plan/artifact_store.py`

### Deprecate/delete after this scope
- Manual mapping normalization helpers (e.g., `_protocol_snapshot` custom coercions)
- Hand-rolled type checks for payload shape

### Implementation checklist
- [ ] Identify all mapping-normalization points at external boundaries.
- [ ] Replace manual logic with `convert` or typed `decode`.
- [ ] Standardize ValidationError reporting via `validation_error_payload`.

---

## Scope 5 — UNSET and Patch Semantics for Partial Updates

### Objective
Introduce `msgspec.UNSET` in patch-style update structs to distinguish **absent** vs **explicit null**, enabling forward-compatible updates in semantic IR contracts.

### Representative code pattern
```python
class CachePolicyPatch(StructBaseStrict, frozen=True):
    cache_path: str | msgspec.UnsetType = msgspec.UNSET
    cache_log_enabled: bool | msgspec.UnsetType = msgspec.UNSET

# apply patch
resolved = {
    "cache_path": coalesce_unset(patch.cache_path, base.cache_path),
    "cache_log_enabled": coalesce_unset(patch.cache_log_enabled, base.cache_log_enabled),
}
```

### Target files
- `src/serde_msgspec.py` (helpers already exist; extend usage)
- `src/cli/config_loader.py`
- `src/engine/runtime_profile.py` (patch-style overrides)

### Deprecate/delete after this scope
- Boolean/None sentinel hacks in config overrides

### Implementation checklist
- [ ] Identify fields where “missing vs null” matters (config, policy, runtime overrides).
- [ ] Add patch structs using `UNSET` and reuse `coalesce_unset` helpers.
- [ ] Ensure UNSET not used with `array_like=True` structs.

---

## Scope 6 — msgspec.inspect-driven Contract Index (Semantic IR Alignment)

### Objective
Use `msgspec.inspect` to build a **contract index** for semantic IR governance (schema checksums, docs, and IR surface inventory).

### Representative code pattern
```python
# src/serde_schema_registry.py (new helper)
from msgspec.inspect import multi_type_info

info = multi_type_info(SCHEMA_TYPES)
contract_index = [
    {"name": t.name, "kind": t.kind, "constraints": getattr(t, "constraints", None)}
    for t in info
]
```

### Target files
- `src/serde_schema_registry.py`
- `docs/` (generated IR contract index docs)

### Deprecate/delete after this scope
- None (new capability)

### Implementation checklist
- [ ] Create a contract index payload from `inspect.multi_type_info`.
- [ ] Export contract index alongside JSON Schema/OpenAPI artifacts.
- [ ] Add a small validation gate to ensure schema registry and inspect index are consistent.

---

## Scope 7 — Migrate Persisted Dataclasses to msgspec Structs

### Objective
Migrate dataclasses that are **persisted or serialized** to msgspec Structs for consistent validation and deterministic encoding.

### Representative code pattern
```python
# before
@dataclass(frozen=True)
class VacuumReport:
    retention_hours: int | None
    dry_run: bool

# after
class VacuumReport(StructBaseCompat, frozen=True):
    retention_hours: int | None
    dry_run: bool
```

### Target files
- `src/cli/commands/delta.py`
- `src/engine/runtime_profile.py`
- `src/hamilton_pipeline/structured_logs.py`
- `src/datafusion_engine/views/artifacts.py`

### Deprecate/delete after this scope
- Dataclass report payloads that are only used for serialization

### Implementation checklist
- [ ] Identify persisted/reporting dataclasses and group by ownership.
- [ ] Migrate to msgspec Structs with appropriate base type (Strict vs Compat).
- [ ] Replace `asdict` and manual JSON dumps with msgspec encoders.

---

## Dependencies and Ordering
1) Scope 1 (Typed config ingestion) should land before Scope 5 (UNSET patches).  
2) Scope 2 (metadata payloads) and Scope 4 (boundary conversion) can proceed in parallel.  
3) Scope 6 (inspect-driven index) depends on stable schema registry output.  
4) Scope 7 (dataclass migration) should follow scopes 3 and 4 to reuse shared patterns.

---

## Expected Architectural End State
- All boundary payloads are msgspec Structs with shared encoding policies.
- Schema registry + inspect index jointly define the **semantic IR contract surface**.
- Config ingestion is typed, validated, and emits uniform errors.
- Logs and diagnostics are deterministic and schema-controlled.
- Manual mapping normalization and ad-hoc payload coercion are eliminated.

