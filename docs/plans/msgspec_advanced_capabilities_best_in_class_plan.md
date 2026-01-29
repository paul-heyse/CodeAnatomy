# msgspec Advanced Capabilities Best-in-Class Plan (Design-Phase, Breaking Changes OK)

Date: 2026-01-29  
Owner: Codex (design phase)  
Status: completed (Scopes 1–8 implemented)  
Validated: 2026-01-29 (codebase cross-check)

## Purpose
Define the next wave of best-in-class msgspec usage to harden schema evolution, improve streaming ingestion, tighten identifier constraints, and unlock advanced msgspec capabilities (Raw, decode_lines, schema metadata, and struct performance knobs). This plan assumes aggressive, design-phase changes are acceptable.

## Design Principles
- **Schema evolution is explicit**: structural changes must be intentional and gated.
- **Contracts are richly documented**: schemas include titles, descriptions, and examples.
- **Streaming-first ingestion**: decode NDJSON directly into typed payloads.
- **Performance-aware modeling**: raw buffers and GC tuning for hot paths.
- **Strict identifiers**: IDs and names are constrained with patterns and minimums.

---

## Scope 1 — Schema evolution guardrails (type_info + field order snapshots)

### Objective
Create deterministic schema snapshots from `msgspec.inspect` to detect accidental field-order changes, type shifts, or configuration drift across all schema registry types.

### Representative code pattern
```python
from pathlib import Path

import msgspec
from msgspec.inspect import multi_type_info

from serde_schema_registry import SCHEMA_TYPES

GOLDEN_PATH = Path("tests/msgspec_contract/goldens/schema_evolution.json")

info = multi_type_info(SCHEMA_TYPES)
field_order = {schema.__name__: schema.__struct_fields__ for schema in SCHEMA_TYPES}

snapshot = {
    "type_info": msgspec.to_builtins(info, order="sorted"),
    "field_order": field_order,
}
GOLDEN_PATH.write_bytes(msgspec.json.encode(snapshot, order="deterministic"))
```

### Target files to modify
- `tests/msgspec_contract/test_contract_schema_registry.py`
- `tests/msgspec_contract/test_schema_evolution.py` (new)
- `tests/msgspec_contract/goldens/schema_evolution.json` (new)
- `src/serde_schema_registry.py`
- `src/serde_msgspec.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add a new schema evolution test that snapshots `multi_type_info` + field order.
- [x] Add stable serialization helpers for schema snapshots (sorted/deterministic).
- [x] Fail the test on field-order drift or unexpected type changes.
- [x] Document the update flow for intentional schema evolution.
  - Update goldens via `uv run pytest -q --update-goldens tests/msgspec_contract`.

---

## Scope 2 — Schema metadata enrichment (titles, examples, extra_json_schema)

### Objective
Ensure every externally-consumed struct has `Meta` descriptions, examples, and schema annotations for clearer JSON Schema/OpenAPI output.

### Representative code pattern
```python
from typing import Annotated

import msgspec

RunId = Annotated[
    str,
    msgspec.Meta(
        min_length=1,
        pattern="^[A-Za-z0-9_-]{8,64}$",
        title="Run Id",
        description="Unique identifier for a pipeline run.",
        examples=["run_01HZX4J3C8F8M2KQ"],
        extra_json_schema={"x-codeanatomy-domain": "runtime"},
    ),
]
```

### Target files to modify
- `src/serde_artifacts.py`
- `src/storage/deltalake/config.py`
- `src/serde_schema_registry.py`
- `docs/architecture/msgspec_schema_registry.json`
- `docs/architecture/msgspec_schema_registry.openapi.json`

### Modules to delete
- None.

### Implementation checklist
- [x] Add title/description/examples for all public artifact and policy fields.
- [x] Introduce a schema hook for vendor metadata (`x-codeanatomy-*`).
- [x] Regenerate JSON Schema/OpenAPI exports.
- [x] Add a contract test that ensures required metadata is present.

---

## Scope 3 — Identifier pattern constraints (regex + min/max length)

### Objective
Standardize identifier types (run IDs, plan fingerprints, dataset/view names) with explicit regex and length constraints, and apply them across all structs.

### Representative code pattern
```python
from typing import Annotated

import msgspec

DatasetName = Annotated[
    str,
    msgspec.Meta(
        min_length=1,
        max_length=128,
        pattern="^[A-Za-z0-9_.-]+$",
        description="Dataset identifier used in artifacts and policies.",
    ),
]
```

### Target files to modify
- `src/serde_artifacts.py`
- `src/core_types.py`
- `src/storage/deltalake/config.py`
- `tests/msgspec_contract/test_contract_errors.py`
- `tests/msgspec_contract/_support/models.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Create a canonical identifier palette (RunId, PlanFingerprint, DatasetName, ViewName).
- [x] Replace ad-hoc `str` usages in schema structs with these aliases.
- [x] Extend error-golden tests for invalid identifiers.

---

## Scope 4 — NDJSON decoding pipeline (decode_lines)

### Objective
Add a typed NDJSON decode path to match existing `encode_lines` usage and enable streaming ingestion of artifact/event logs.

### Representative code pattern
```python
import msgspec

from serde_msgspec import JSON_DECODER


def decode_json_lines[T](buf: bytes, *, target_type: type[T], strict: bool = True) -> list[T]:
    decoder = msgspec.json.Decoder(type=target_type, strict=strict)
    return decoder.decode_lines(buf)
```

### Target files to modify
- `src/serde_msgspec.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `tests/msgspec_contract/test_contract_json.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add a `decode_json_lines` helper mirroring `encode_json_lines`.
- [x] Wire NDJSON decoding into artifact ingestion paths that read line-delimited payloads.
- [x] Add tests for round-trip encode/decode with typed artifacts.

---

## Scope 5 — Deferred decoding using msgspec.Raw

### Objective
Introduce `msgspec.Raw` for embedded policy payloads to avoid unnecessary decoding and allow zero-copy embedding in encoded messages while keeping JSON-facing artifacts byte-stable.

### Representative code pattern
```python
import msgspec

class WriteArtifactRow(StructBaseCompat, frozen=True):
    delta_write_policy_msgpack: msgspec.Raw
    delta_schema_policy_msgpack: msgspec.Raw


def decode_raw[T](raw: msgspec.Raw, *, target_type: type[T]) -> T:
    return msgspec.msgpack.decode(raw, type=target_type)
```

### Target files to modify
- `src/serde_artifacts.py`
- `src/serde_msgspec.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `tests/msgspec_contract/test_contract_msgpack.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Identify large payload fields suitable for `msgspec.Raw`.
- [x] Add safe `Raw.copy()` paths where buffers might outlive inputs.
- [x] Update encoding/decoding paths to accept `Raw` without copying.
- [x] Extend contract tests to cover `Raw` payloads in MessagePack contracts.

---

## Scope 6 — Struct performance configuration (gc/cache_hash)

### Objective
Define a performance-optimized base struct configuration for hot-path artifacts (frozen, gc disabled, hash cached) and apply it to high-volume types.

### Representative code pattern
```python
import msgspec

class StructBaseHotPath(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Base struct for high-volume, immutable artifacts."""


class RunManifest(StructBaseHotPath):
    run_id: RunId
    outputs: tuple[dict[str, object], ...]
```

### Target files to modify
- `src/serde_msgspec.py`
- `src/serde_artifacts.py`
- `tests/msgspec_contract/test_contract_schema_registry.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add a hot-path base struct and document its intended usage.
- [x] Apply to high-volume, immutable artifacts (run manifests, envelopes, cache artifacts).
- [x] Update schema registry tests to validate struct configs.

---

## Scope 7 — Attribute-based conversion boundaries (from_attributes)

### Objective
Standardize conversion of ORM-like or dataclass/attrs instances into msgspec structs at system boundaries using `from_attributes=True`.

### Representative code pattern
```python
from serde_msgspec import convert

payload = convert(obj, target_type=RunManifest, from_attributes=True)
```

### Target files to modify
- `src/serde_msgspec.py`
- `src/datafusion_engine/write_pipeline.py`
- `tests/msgspec_contract/test_contract_conversion.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Identify boundary points where non-dict inputs are converted to structs.
- [x] Enable `from_attributes=True` where appropriate to avoid manual dict mapping.
- [x] Add tests for attribute-based conversion and error normalization.

---

## Scope 8 — Deferred deletions and cleanup (post-scope)

### Objective
Identify any temporary scaffolding that can be deleted once all scopes are complete.

### Target files to delete or refactor
- None identified yet. Re-evaluate after scopes 1–7 land.

### Implementation checklist
- [x] Audit for superseded schema snapshot utilities.
- [x] Remove any ad-hoc line-delimited parsing logic replaced by `decode_json_lines`.
- [x] Confirm no unused schema metadata helpers remain.
