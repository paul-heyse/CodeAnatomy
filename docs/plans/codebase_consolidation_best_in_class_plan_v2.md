# Codebase Consolidation Plan v2 (Design-Phase, Breaking Changes OK)

## Executive Summary

This revision assumes **design-phase freedom**: no backward compatibility, no stable fingerprints, and a deliberate shift toward a **single-source-of-truth architecture**. All derived artifacts (fingerprints, caches, protocol payloads, metadata bytes) are allowed to change whenever source data or view definitions change. The goal is to **minimize duplication**, **tighten Rust/Python boundaries**, and **simplify runtime paths** with a best-possible design even if it is breaking.

**Key directional shifts (new):**
- **No compatibility constraints**: delete legacy APIs, payloads, and stable hashes.
- **Fingerprints are derived** from *source data + view definitions*; stability is not required.
- **Async driver path is deprecated/removed** to keep the runtime clean and deterministic.
- **Delta + DataFusion integration becomes schema-first**, with **Rust as the execution source-of-truth** and **generated types** across languages.
- **Metadata encoding moves to MessagePack**, eliminating Arrow IPC payloads for metadata.

**Estimated impact:**
- 30-50% reduction in duplicated Delta/Python protocol logic.
- 800-1,500 lines removed in schema/metadata/registry duplication.
- Simplified driver/session factory paths with fewer execution branches.
- Single, canonical hashing and metadata encoding strategy across the repo.

---

## Design-Phase Principles (Non-Negotiable)

1. **No backward compatibility**: remove legacy shapes, hashes, and payloads.
2. **Fingerprints and cache keys are not stable**: all derived artifacts can change.
3. **APIs can change freely**: prefer best design over migration safety.
4. **Async drivers are de-scoped**: only sync driver build paths remain.
5. **Single source of truth**:
   - Source data (extracted datasets)
   - View definitions (plan + schema intent)
   Everything else is derivative.

---

## Table of Contents

1. [Schema-First Delta Types (Rust/Python Codegen)](#1-schema-first-delta-types-rustpython-codegen)
2. [Delta Protocol + Gate Validation in Rust](#2-delta-protocol--gate-validation-in-rust)
3. [Arrow Schema Builders (Canonicalized)](#3-arrow-schema-builders-canonicalized)
4. [Metadata Codec Overhaul (MsgPack)](#4-metadata-codec-overhaul-msgpack)
5. [Field Specification Unification](#5-field-specification-unification)
6. [Fingerprinting & Identity Overhaul](#6-fingerprinting--identity-overhaul)
7. [Configuration Base + Fingerprinting Unification](#7-configuration-base--fingerprinting-unification)
8. [Driver Factory Simplification (Sync Only)](#8-driver-factory-simplification-sync-only)
9. [Registry Protocol Standardization](#9-registry-protocol-standardization)
10. [Session/Context Factory Consolidation](#10-sessioncontext-factory-consolidation)
11. [Cache Key & Hash Strategy Unification](#11-cache-key--hash-strategy-unification)
12. [Error Handling Alignment (Rust-Python)](#12-error-handling-alignment-rust-python)

---

## 1. Schema-First Delta Types (Rust/Python Codegen)

### Problem Statement
Delta protocol and mutation types are duplicated across Rust and Python with divergent semantics and serialization behavior.

### Target Implementation
Introduce a **schema-first contract** for Delta types, generate both Rust and Python types, and delete hand-written duplicates.

**Schema-first contract (JSON Schema 2020-12):**
```json
{
  "$id": "schemas/delta/feature_gate.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "DeltaFeatureGate",
  "type": "object",
  "properties": {
    "min_reader_version": {"type": ["integer", "null"], "minimum": 0},
    "min_writer_version": {"type": ["integer", "null"], "minimum": 0},
    "required_reader_features": {"type": "array", "items": {"type": "string"}},
    "required_writer_features": {"type": "array", "items": {"type": "string"}}
  },
  "additionalProperties": false
}
```

**Generated Python (example):**
```python
@dataclass(frozen=True)
class DeltaFeatureGate:
    min_reader_version: int | None = None
    min_writer_version: int | None = None
    required_reader_features: tuple[str, ...] = ()
    required_writer_features: tuple[str, ...] = ()
```

**Generated Rust (example):**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyo3::pyclass)]
pub struct DeltaFeatureGate {
    pub min_reader_version: Option<i32>,
    pub min_writer_version: Option<i32>,
    pub required_reader_features: Vec<String>,
    pub required_writer_features: Vec<String>,
}
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `schemas/delta/feature_gate.schema.json` | Canonical DeltaFeatureGate schema |
| Create | `schemas/delta/commit_options.schema.json` | Canonical DeltaCommitOptions schema |
| Create | `schemas/delta/app_transaction.schema.json` | Canonical DeltaAppTransaction schema |
| Create | `scripts/codegen_delta_types.py` | Generate Rust/Python types |
| Create | `src/datafusion_engine/generated/delta_types.py` | Generated Python types |
| Create | `rust/datafusion_ext/src/generated/delta_types.rs` | Generated Rust types |
| Modify | `rust/datafusion_ext/src/lib.rs` | Re-export generated Rust types |
| Modify | `rust/datafusion_python/src/codeanatomy_ext.rs` | Export types to Python |
| Modify | `src/datafusion_engine/delta_control_plane.py` | Replace local dataclasses |
| Modify | `src/datafusion_engine/delta_protocol.py` | Use generated DeltaFeatureGate |
| Modify | `src/serde_schema_registry.py` | Register generated Delta schemas |

### Implementation Checklist
- [ ] Add JSON schemas for all Delta protocol/mutation types.
- [ ] Implement codegen for Python + Rust (serde-compatible).
- [ ] Replace hand-written dataclasses in Python.
- [ ] Replace hand-written structs in Rust.
- [ ] Register generated types in schema registry.
- [ ] Remove msgspec-only DeltaFeatureGate if redundant.

### Decommissioning List
- Delete `DeltaAppTransaction` and `DeltaCommitOptions` in `src/datafusion_engine/delta_control_plane.py`.
- Delete `DeltaFeatureGate` in `src/datafusion_engine/delta_protocol.py` (replace with generated).
- Remove duplicated Rust structs in `rust/datafusion_ext/src/delta_mutations.rs` and `rust/datafusion_ext/src/delta_protocol.rs`.

---

## 2. Delta Protocol + Gate Validation in Rust

### Problem Statement
Protocol gate validation exists in both Rust and Python, with overlapping logic and inconsistent error surfaces.

### Target Implementation
Move all protocol gate validation into Rust and expose a single PyO3 entrypoint. Python should serialize the snapshot + gate payload and rely on Rust for validation.

```rust
#[pyfunction]
pub fn validate_protocol_gate(
    snapshot_ipc: &[u8],
    gate_msgpack: &[u8],
) -> PyResult<()> {
    let snapshot: DeltaSnapshotInfo = decode_snapshot(snapshot_ipc)?;
    let gate: DeltaFeatureGate = decode_gate(gate_msgpack)?;
    protocol_gate(&snapshot, &gate).map_err(to_pyerr)
}
```

```python
def validate_delta_gate(snapshot: DeltaProtocolSnapshot, gate: DeltaFeatureGate) -> None:
    payload = dumps_msgpack(gate)
    snapshot_ipc = _serialize_snapshot(snapshot)
    datafusion_ext.validate_protocol_gate(snapshot_ipc, payload)
```

### Target File List
| Action | File |
|--------|------|
| Modify | `rust/datafusion_ext/src/delta_protocol.rs` |
| Modify | `rust/datafusion_python/src/codeanatomy_ext.rs` |
| Modify | `src/datafusion_engine/delta_protocol.py` |
| Modify | `src/datafusion_engine/delta_control_plane.py` |
| Modify | `src/datafusion_engine/dataset_registry.py` |
| Modify | `src/datafusion_engine/catalog_provider.py` |
| Modify | `src/datafusion_engine/scan_planner.py` |
| Modify | `src/datafusion_engine/scan_overrides.py` |

### Implementation Checklist
- [ ] Add PyO3 `validate_protocol_gate` function.
- [ ] Serialize gate via msgpack (generated type).
- [ ] Remove Python-side validation logic.
- [ ] Centralize error mapping to Python exception types.
- [ ] Standardize Delta feature gate payload generation via `delta_feature_gate_payload()`.
- [ ] Centralize Delta scan config snapshot + hash in one helper.

### Decommissioning List
- Remove `_validate_reader_version`, `_validate_writer_version`, `_validate_reader_features`, `_validate_writer_features` in Python.
- Remove Python-side gate validation in `validate_delta_gate()`.
- Remove manual Delta feature gate dict construction in registry snapshots.
- Remove duplicate Delta scan config hashing in planner/registry helpers.

---

## 3. Arrow Schema Builders (Canonicalized)

### Problem Statement
Schema patterns are duplicated with inconsistent entry shapes across runtime and metadata systems.

### Target Implementation
Create **canonical schema builders** and standardize **map entry shapes** across the codebase. Design-phase allows full shape changes.

```python
def map_entry_type(*, with_kind: bool = True) -> pa.StructType:
    fields = [pa.field("key", pa.string(), nullable=False)]
    if with_kind:
        fields.extend([
            pa.field("value_kind", pa.string(), nullable=False),
            pa.field("value", pa.string(), nullable=True),
        ])
    else:
        fields.append(pa.field("value", pa.string(), nullable=True))
    return pa.struct(fields)
```

### Target File List
| Action | File |
|--------|------|
| Create | `src/datafusion_engine/arrow_schema/schema_builders.py` |
| Modify | `src/datafusion_engine/runtime.py` |
| Modify | `src/datafusion_engine/arrow_schema/metadata.py` |
| Modify | `src/datafusion_engine/schema_registry.py` |

### Implementation Checklist
- [ ] Add canonical schema builder functions.
- [ ] Replace all duplicated map/list schema definitions.
- [ ] Normalize on a single map entry shape (key/value_kind/value).
- [ ] Update payload builders and decoders accordingly.

### Decommissioning List
- Delete `_MAP_ENTRY_SCHEMA` and `_MAP_SCHEMA` duplicates.
- Delete `_LIST_SCHEMA`, `_SCALAR_MAP_SCHEMA`, `_ORDERING_KEYS_SCHEMA`, `_EXTRACTOR_DEFAULTS_SCHEMA` where redundant.

---

## 4. Metadata Codec Overhaul (MsgPack)

### Problem Statement
Metadata encoding uses Arrow IPC payloads, which are verbose, brittle, and duplicated.

### Target Implementation
Replace IPC encoding with **MessagePack** for all metadata lists/maps/scalar maps.

```python
_ENC = msgspec.msgpack.Encoder()
_DEC = msgspec.msgpack.Decoder()

def encode_metadata_map(mapping: Mapping[str, str]) -> bytes:
    return _ENC.encode(dict(mapping))

def decode_metadata_map(payload: bytes | None) -> dict[str, str]:
    if not payload:
        return {}
    return _DEC.decode(payload)
```

### Target File List
| Action | File |
|--------|------|
| Create | `src/datafusion_engine/arrow_schema/metadata_codec.py` |
| Modify | `src/datafusion_engine/arrow_schema/metadata.py` |
| Modify | `src/normalize/evidence_specs.py` |
| Modify | `src/schema_spec/specs.py` |
| Modify | `src/datafusion_engine/arrow_schema/encoding_metadata.py` |

### Implementation Checklist
- [ ] Replace IPC metadata encoding with msgpack.
- [ ] Update all metadata read/write call sites.
- [ ] Remove IPC schema constants tied to metadata encoding.
- [ ] Add unit tests for metadata codec with msgpack payloads.

### Decommissioning List
- Remove `metadata_list_bytes`, `metadata_map_bytes`, and IPC-based decode helpers.
- Remove metadata IPC schemas and `payload_ipc_bytes` usage for metadata only.

---

## 5. Field Specification Unification

### Problem Statement
`ArrowFieldSpec` and `ColumnContract` duplicate core field semantics.

### Target Implementation
Define **one canonical field spec** (`FieldSpec`) and replace all parallel types.

```python
@dataclass(frozen=True)
class FieldSpec:
    name: str
    dtype: pa.DataType
    nullable: bool = True
    metadata: dict[bytes, bytes] | None = None

    def to_arrow_field(self) -> pa.Field:
        return pa.field(self.name, self.dtype, nullable=self.nullable, metadata=self.metadata)
```

### Target File List
| Action | File |
|--------|------|
| Create | `src/schema_spec/field_spec.py` |
| Modify | `src/schema_spec/specs.py` |
| Modify | `src/datafusion_engine/schema_contracts.py` |

### Implementation Checklist
- [ ] Introduce `FieldSpec`.
- [ ] Replace `ColumnContract` and `ArrowFieldSpec` usage.
- [ ] Update schema contract logic to use `FieldSpec`.
- [ ] Add conversion tests (field -> schema -> field).

### Decommissioning List
- Remove `ColumnContract` from `schema_contracts.py`.
- Remove `ArrowFieldSpec` from `schema_spec/specs.py`.

---

## 6. Fingerprinting & Identity Overhaul

### Problem Statement
Fingerprints are scattered and assume stability. In the design phase, fingerprints should be **derived** and **unstable**.

### Target Implementation
Introduce a **single identity builder** that derives fingerprints from:
- Extracted source data identity
- View definition identity
- Runtime profile inputs (only as needed)

```python
def build_identity_payload(*, sources: Mapping[str, str], view: Mapping[str, object]) -> dict[str, object]:
    return {"sources": dict(sources), "view": dict(view)}

def identity_fingerprint(payload: Mapping[str, object]) -> str:
    return hash_msgpack_canonical(payload)
```

### Target File List
| Action | File |
|--------|------|
| Create | `src/datafusion_engine/identity.py` |
| Modify | `src/datafusion_engine/arrow_schema/abi.py` |
| Modify | `src/datafusion_engine/runtime.py` |
| Modify | `src/hamilton_pipeline/modules/task_execution.py` |
| Modify | `src/incremental/*` |

### Implementation Checklist
- [ ] Create `identity.py` with canonical payload builder.
- [ ] Replace `schema_fingerprint` and `dataset_fingerprint` with identity-based approach.
- [ ] Remove expectations of stability in caches and artifacts.
- [ ] Update all fingerprint call sites to use identity payloads.

### Decommissioning List
- Remove `schema_fingerprint()` from `arrow_schema/abi.py`.
- Remove `dataset_fingerprint()` logic that depends on IPC payloads.
- Remove IPC-based fingerprint schema usage for dataset fingerprints.

---

## 7. Configuration Base + Fingerprinting Unification

### Problem Statement
Config classes use inconsistent fingerprinting logic.

### Target Implementation
Introduce a base config interface with **single fingerprint implementation** and normalize config payload shapes.

```python
class FingerprintableConfig(Protocol):
    def fingerprint_payload(self) -> Mapping[str, object]: ...

def config_fingerprint(payload: Mapping[str, object]) -> str:
    return hash_msgpack_canonical(payload)
```

### Target File List
| Action | File |
|--------|------|
| Create | `src/core/config_base.py` |
| Modify | `src/cache/diskcache_factory.py` |
| Modify | `src/storage/deltalake/config.py` |
| Modify | `src/obs/otel/config.py` |

### Implementation Checklist
- [ ] Add `FingerprintableConfig` protocol.
- [ ] Replace custom fingerprint methods.
- [ ] Ensure all config fingerprints are derived via one function.

### Decommissioning List
- Remove custom fingerprint methods on DiskCache and Delta policy classes.

---

## 8. Driver Factory Simplification (Sync Only)

### Problem Statement
`driver_factory.py` contains duplicated sync/async builders and complex branching.

### Target Implementation
Remove async paths entirely and standardize on a sync-only `DriverBuilder`.

```python
class DriverBuilder:
    def __init__(self, plan_ctx: PlanContext) -> None:
        self._plan_ctx = plan_ctx

    def build(self) -> driver.Driver:
        builder = driver.Builder().allow_module_overrides()
        builder = builder.with_modules(*self._plan_ctx.modules).with_config(self._plan_ctx.config_payload)
        builder = _apply_dynamic_execution(builder, options=...)
        builder = _apply_graph_adapter(builder, ...)
        builder = _apply_cache(builder, ...)
        builder = _apply_materializers(builder, ...)
        builder = _apply_adapters(builder, ...)
        return builder.build()
```

### Target File List
| Action | File |
|--------|------|
| Create | `src/hamilton_pipeline/driver_builder.py` |
| Modify | `src/hamilton_pipeline/driver_factory.py` |

### Implementation Checklist
- [ ] Introduce `DriverBuilder` (sync only).
- [ ] Remove async builder context and async finalize logic.
- [ ] Update `build_driver()` to use builder.
- [ ] Delete async driver entrypoints.

### Decommissioning List
- Delete `build_async_driver_builder_context()`.
- Delete `build_async_driver()` and `finalize_async_driver()`.
- Remove `async_driver` imports from `driver_factory.py`.

---

## 9. Registry Protocol Standardization

### Problem Statement
Registries are inconsistent in interfaces and semantics.

### Target Implementation
Adopt `MutableRegistry` and `ImmutableRegistry` composition for all registries, eliminate ad-hoc dict logic.

### Target File List
| Action | File |
|--------|------|
| Modify | `src/datafusion_engine/provider_registry.py` |
| Modify | `src/datafusion_engine/view_registry.py` |
| Modify | `src/datafusion_engine/param_tables.py` |
| Modify | `src/datafusion_engine/schema_contracts.py` |
| Modify | `src/hamilton_pipeline/semantic_registry.py` |

### Implementation Checklist
- [ ] Replace dicts with `MutableRegistry` composition.
- [ ] Enforce `Registry` protocol for all registries.
- [ ] Add lightweight protocol compliance tests.

### Decommissioning List
- Remove ad-hoc registry dicts in the above modules.

---

## 10. Session/Context Factory Consolidation

### Problem Statement
Session creation is duplicated with fragmented config application.

### Target Implementation
Build a single `SessionFactory` that:
- Applies config settings in one pipeline.
- Integrates Delta defaults (`DeltaSessionConfig`, `DeltaRuntimeEnvBuilder`) when relevant.
- Owns UDF installation and schema validation toggles.

```python
class SessionFactory:
    def __init__(self, profile: DataFusionRuntimeProfile) -> None:
        self._profile = profile

    def build(self) -> SessionContext:
        ctx = SessionContext()
        config = DeltaSessionConfig()
        config = _apply_profile_config(config, self._profile)
        return ctx.with_session_config(config)
```

### Target File List
| Action | File |
|--------|------|
| Create | `src/datafusion_engine/session_factory.py` |
| Modify | `src/datafusion_engine/config_helpers.py` |
| Modify | `src/datafusion_engine/runtime.py` |
| Modify | `src/engine/session_factory.py` |

### Implementation Checklist
- [ ] Implement `SessionFactory`.
- [ ] Replace `apply_*` helper cluster with a single helper.
- [ ] Wire runtime/session builders through the factory.
- [ ] Integrate Delta defaults for SessionConfig and runtime env.

### Decommissioning List
- Remove `apply_config_value`, `apply_optional_config`, etc. from `config_helpers.py`.
- Remove `_apply_*` config helpers in `runtime.py`.

---

## 11. Cache Key & Hash Strategy Unification

### Problem Statement
Cache keys and hashes are inconsistent and use direct `hashlib` in multiple modules.

### Target Implementation
Introduce a `CacheKeyBuilder` and unify all hashing through `utils.hashing` with msgpack canonical payloads.

```python
class CacheKeyBuilder:
    def __init__(self, prefix: str = "") -> None:
        self._prefix = prefix
        self._components: dict[str, object] = {}

    def add(self, name: str, value: object) -> CacheKeyBuilder:
        self._components[name] = value
        return self

    def build(self) -> str:
        digest = hash_msgpack_canonical(self._components)
        return f"{self._prefix}:{digest}" if self._prefix else digest
```

### Target File List
| Action | File |
|--------|------|
| Modify | `src/utils/hashing.py` |
| Modify | `src/hamilton_pipeline/driver_factory.py` |
| Modify | `src/cache/diskcache_factory.py` |
| Modify | `src/datafusion_engine/plan_cache.py` |
| Modify | `src/schema_spec/system.py` |
| Modify | `src/datafusion_engine/schema_introspection.py` |

### Implementation Checklist
- [ ] Add `CacheKeyBuilder` to `utils/hashing.py`.
- [ ] Remove all direct `hashlib` usage outside `utils/hashing.py`.
- [ ] Replace DDL/schema map fingerprints with canonical hash helper.

### Decommissioning List
- Remove inline hashlib usage from `schema_spec/system.py` and `schema_introspection.py`.
- Remove manual JSON-based cache key construction.

---

## 12. Error Handling Alignment (Rust-Python)

### Problem Statement
Python errors do not align with Rust `ExtError` variants.

### Target Implementation
Introduce a unified Python error hierarchy aligned with Rust error kinds, and map Rust errors to Python `DataFusionEngineError`.

```python
class DataFusionEngineError(Exception):
    def __init__(self, message: str, kind: str = "generic") -> None:
        super().__init__(message)
        self.kind = kind
```

### Target File List
| Action | File |
|--------|------|
| Create | `src/datafusion_engine/errors.py` |
| Modify | `src/datafusion_engine/delta_control_plane.py` |
| Modify | `src/datafusion_engine/schema_validation.py` |

### Implementation Checklist
- [ ] Add `DataFusionEngineError` + kind enum.
- [ ] Map Rust errors to Python kinds (datafusion/arrow/delta/plugin).
- [ ] Replace ValueError/RuntimeError usage in Delta control plane.

### Decommissioning List
- Remove ad-hoc `ValueError`/`RuntimeError` usage where error type is known.

---

## Cross-Scope Dependencies

| Scope | Depends On | Notes |
|-------|------------|-------|
| #1 Schema-first Delta types | None | Foundation |
| #2 Delta protocol validation | #1 | Uses generated types |
| #3 Schema builders | None | Foundation |
| #4 Metadata codec | #3 | Uses canonical schema builders |
| #5 Field specs | #3, #4 | Uses canonical schema builders + metadata encoding |
| #6 Identity overhaul | #3, #4 | Needs canonical schema + metadata |
| #7 Config base | None | Foundation |
| #8 Driver factory | #7, #10 | Uses config/session pipeline |
| #9 Registry protocol | None | Foundation |
| #10 Session factory | #7 | Uses config base |
| #11 Cache keys | #6 | Cache keys derived from identity |
| #12 Errors | #2 | Error mapping for Rust-Python |

---

## Recommended Implementation Order

**Phase 1: Foundations**
1. Schema builders (#3)
2. Metadata codec (#4)
3. Schema-first Delta types (#1)
4. Config base (#7)
5. Registry protocol (#9)

**Phase 2: Core Runtime & Identity**
6. Delta protocol validation (#2)
7. Field specs (#5)
8. Identity overhaul (#6)
9. Cache key unification (#11)

**Phase 3: Runtime Simplification**
10. Session factory (#10)
11. Driver factory sync-only (#8)

**Phase 4: Error Alignment**
12. Error handling (#12)

---

## Verification (Design-Phase)

- Run tests: `uv run pytest tests/`
- Run type checks: `uv run pyright --warnings`
- Run linting: `uv run ruff check`
- Ensure all Delta types are generated from schemas.
- Ensure no direct `hashlib` usage outside `utils/hashing.py`.
- Ensure async driver references removed.

---

## Summary Statistics (Updated)

| Metric | Value |
|--------|-------|
| Total Scopes | 12 |
| Files to Create | ~15 |
| Files to Modify | ~55 |
| Files to Delete | 10-15 |
| Expected Code Reduction | 1,000-1,800 lines |
| Backward Compatibility | None (explicitly dropped) |
