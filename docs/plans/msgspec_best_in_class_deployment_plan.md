# Msgspec Best-in-Class Deployment Plan

## Scope and Decisions

- Artifacts are internal-only: default to strict schemas with `forbid_unknown_fields=True`.
- Prefer structured payloads over JSON strings; encode only at storage or wire boundaries.
- Prefer MessagePack for internal caches and telemetry; keep JSON for human-readable artifacts.
- Missing vs null semantics are undecided; this plan includes a decision gate and shows an `UNSET` pattern.

---

## Status Summary

- Item 1: completed (shared serde module + hooks).
- Item 2: partially complete (UNSET used in diagnostics ingestion; decision gate still open).
- Item 3: completed and integrated into runtime/tests.
- Item 4: mostly complete (structured payloads done; Raw.copy deferred).
- Items 5–9: completed and integrated into runtime/tests.

---

## Item 1: Central msgspec policy + serde module

### Goal
Create a single, shared module that defines default struct behavior, encoders/decoders, ordering, hooks, and boundary helpers (JSON and MessagePack).

### Status
Completed in `src/serde_msgspec.py` and `src/serde_msgspec_ext.py`.

### Design / Architecture
```python
# src/serde_msgspec.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Protocol, cast

import msgspec

from serde_msgspec_ext import SCHEMA_EXT_CODE

_DEFAULT_ORDER: Literal["deterministic"] = "deterministic"


class StructBase(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    omit_defaults=True,
    repr_omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Base struct for internal artifacts."""


class _MsgpackBuffer(Protocol):
    def to_pybytes(self) -> bytes:
        """Return the buffer as raw bytes."""


class _ArrowSchema(Protocol):
    def serialize(self) -> _MsgpackBuffer:
        """Serialize schema into a bytes-like buffer."""


def _json_enc_hook(obj: object) -> object:
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError


def _msgpack_enc_hook(obj: object) -> object:
    if isinstance(obj, Path):
        return str(obj)
    try:
        import pyarrow as pa
    except ImportError:
        pa = None
    if pa is not None and isinstance(obj, pa.Schema):
        schema = cast(_ArrowSchema, obj)
        return msgspec.msgpack.Ext(
            SCHEMA_EXT_CODE,
            schema.serialize().to_pybytes(),
        )
    raise TypeError


def _dec_hook(type_hint: Any, obj: object) -> object:
    if type_hint is Path and isinstance(obj, str):
        return Path(obj)
    return obj


def _msgpack_ext_hook(code: int, data: memoryview) -> object:
    if code == SCHEMA_EXT_CODE:
        try:
            import pyarrow as pa
        except ImportError:
            return msgspec.msgpack.Ext(code, data.tobytes())
        return pa.ipc.read_schema(data)
    return msgspec.msgpack.Ext(code, data.tobytes())

JSON_ENCODER = msgspec.json.Encoder(enc_hook=_json_enc_hook, order=_DEFAULT_ORDER)
MSGPACK_ENCODER = msgspec.msgpack.Encoder(enc_hook=_msgpack_enc_hook, order=_DEFAULT_ORDER)


def dumps_json(obj: object, *, pretty: bool = False) -> bytes:
    raw = JSON_ENCODER.encode(obj)
    if not pretty:
        return raw
    return msgspec.json.format(raw, indent=2)


def loads_json[T](buf: bytes | str, *, target_type: type[T], strict: bool = True) -> T:
    decoder = msgspec.json.Decoder(type=target_type, dec_hook=_dec_hook, strict=strict)
    return decoder.decode(buf)


def dumps_msgpack(obj: object) -> bytes:
    return MSGPACK_ENCODER.encode(obj)


def loads_msgpack[T](buf: bytes, *, target_type: type[T], strict: bool = True) -> T:
    decoder = msgspec.msgpack.Decoder(
        type=target_type,
        dec_hook=_dec_hook,
        ext_hook=_msgpack_ext_hook,
        strict=strict,
    )
    return decoder.decode(buf)


def encode_json_into(obj: object, buf: bytearray) -> None:
    JSON_ENCODER.encode_into(obj, buf)


def encode_json_lines(items: list[object]) -> bytes:
    return JSON_ENCODER.encode_lines(items)


def convert[T](
    obj: object,
    *,
    target_type: type[T],
    strict: bool = True,
    from_attributes: bool = False,
) -> T:
    return msgspec.convert(
        obj,
        type=target_type,
        strict=strict,
        from_attributes=from_attributes,
        dec_hook=_dec_hook,
    )


def to_builtins(obj: object, *, str_keys: bool = True) -> object:
    return msgspec.to_builtins(
        obj,
        order=_DEFAULT_ORDER,
        str_keys=str_keys,
        enc_hook=_json_enc_hook,
    )
```

### Target Files
- `src/serde_msgspec.py` (new)
- `src/serde_msgspec_ext.py` (new)
- `src/obs/manifest.py`
- `src/obs/repro.py`
- `src/incremental/cdf_cursors.py`
- `src/sqlglot_tools/optimizer.py`
- `src/engine/runtime_profile.py`
- `src/datafusion_engine/bridge.py`

### Implementation Checklist
- [x] Add `StructBase` and JSON/MessagePack encoders/decoders in a shared module.
- [x] Centralize ordering policy (`order="deterministic"`) and hook logic.
- [x] Define helper functions for `encode_into` and `encode_lines` for hot paths.

---

## Item 2: Missing vs null decision gate (UNSET policy)

### Goal
Decide where "missing" must be distinguished from `None`, and implement `msgspec.UNSET` only where it matters.

### Status
Partially complete: `msgspec.UNSET` is used in diagnostics ingestion for optional timestamps, but the broader decision gate is still open.

### Design / Architecture
```python
from __future__ import annotations

from typing import Annotated, Literal

import msgspec

from serde_msgspec import StructBase, convert

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]


class ManifestV1(StructBase, tag="manifest.v1"):
    schema_version: Literal["v1"] = "v1"
    created_at_unix_s: NonNegInt
    # UNSET means "missing"; None means "explicit null"
    trace_id: str | msgspec.UnsetType = msgspec.UNSET


def coalesce_unset[T](value: T | msgspec.UnsetType, default: T) -> T:
    return default if value is msgspec.UNSET else value
```

### Target Files
- `src/obs/manifest.py`
- `src/obs/repro.py`
- `src/engine/runtime_profile.py`
- `src/obs/diagnostics_tables.py`

### Implementation Checklist
- [ ] Identify fields where "missing vs null" changes behavior (e.g., patch semantics).
- [x] Adopt `UNSET` for diagnostics ingestion timestamps where missing is meaningful.
- [ ] Add a small helper to normalize `UNSET` at read/write boundaries.

---

## Item 3: Migrate core artifacts to msgspec.Struct

### Goal
Replace dataclass + `asdict` + bespoke normalization with typed structs and reusable encoders.

### Status
Completed for incremental cursors, manifest, repro, and diagnostics specs.

### Design / Architecture
```python
# src/incremental/cdf_cursors.py
from __future__ import annotations

from pathlib import Path
from typing import Annotated

import msgspec

from serde_msgspec import StructBase, dumps_json, loads_json

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]


class CdfCursor(StructBase, frozen=True):
    dataset_name: str
    last_version: NonNegInt


class CdfCursorStore(StructBase, frozen=True):
    cursors_path: Path

    def save_cursor(self, cursor: CdfCursor) -> None:
        self.cursors_path.mkdir(parents=True, exist_ok=True)
        self._cursor_file(cursor.dataset_name).write_bytes(dumps_json(cursor, pretty=True))

    def load_cursor(self, dataset_name: str) -> CdfCursor | None:
        path = self._cursor_file(dataset_name)
        if not path.exists():
            return None
        try:
            return loads_json(path.read_bytes(), target_type=CdfCursor, strict=False)
        except msgspec.DecodeError:
            return None
```

```python
# src/obs/manifest.py (schema sketch)
from __future__ import annotations

from typing import Annotated, Literal

import msgspec

from serde_msgspec import StructBase
from core_types import JsonDict

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]


class DatasetRecord(StructBase, frozen=True):
    name: str
    kind: Literal["input", "intermediate", "relationship_output", "cpg_output"]
    path: str | None = None
    rows: NonNegInt | None = None
    schema: list[JsonDict] | None = None


class Manifest(StructBase, frozen=True):
    manifest_version: NonNegInt
    created_at_unix_s: NonNegInt
    datasets: list[DatasetRecord] = msgspec.field(default_factory=list)
    repro: JsonDict = msgspec.field(default_factory=dict)
```

### Target Files
- `src/incremental/cdf_cursors.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`
- `src/obs/diagnostics.py`

### Implementation Checklist
- [x] Replace `dataclass` definitions with `StructBase` for serialized artifacts.
- [x] Remove `asdict` and `_normalize_value` usage in favor of typed structs.
- [x] Use `msgspec.Meta` constraints for invariants (e.g., non-negative counts).

---

## Item 4: Structured AST/policy/runtime artifacts (no JSON strings)

### Goal
Store structured payloads in structs; stop embedding JSON strings in artifacts. Use `Raw` or MessagePack for compact storage.

### Status
Completed for SQLGlot AST artifacts, runtime profile payloads, and compiler diff payloads; `Raw.copy()` usage is deferred until long-lived caches are introduced.

### Design / Architecture
```python
# src/sqlglot_tools/optimizer.py (artifact rewrite)
from __future__ import annotations

import msgspec

from serde_msgspec import StructBase
from sqlglot import dump
from sqlglot.expressions import Expression

AST_ENCODER = msgspec.msgpack.Encoder(order="deterministic")
AST_DECODER = msgspec.msgpack.Decoder(type=list[dict[str, object]], strict=False)


class AstArtifact(StructBase, frozen=True):
    sql: str
    ast_raw: msgspec.Raw
    policy_hash: str


def ast_to_raw(expr: Expression) -> msgspec.Raw:
    return msgspec.Raw(AST_ENCODER.encode(dump(expr)))


def ast_from_raw(raw: msgspec.Raw) -> list[dict[str, object]]:
    return AST_DECODER.decode(raw)
```

```python
# src/engine/runtime_profile.py (snapshot rewrite)
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeProfileSnapshot:
    version: int
    name: str
    determinism_tier: str
    scan_profile: dict[str, object]
    ibis_options: dict[str, object]
    sqlglot_policy: dict[str, object] | None
    datafusion: dict[str, object] | None
    function_registry_hash: str
    profile_hash: str
    scan_profile_schema_msgpack: bytes | None = None
```

### Target Files
- `src/sqlglot_tools/optimizer.py`
- `src/engine/runtime_profile.py`
- `src/relspec/compiler.py`
- `src/datafusion_engine/bridge.py`

### Implementation Checklist
- [x] Replace JSON string fields with structured payload fields.
- [x] Store AST/policy snapshots as `Raw` (MessagePack) when size matters.
- [ ] Use `Raw.copy()` when retaining in long-lived caches (defer until caching is introduced).

---

## Item 5: Typed diagnostics and telemetry ingestion (MessagePack first)

### Goal
Replace coercion helpers with typed decode + `strict=False` for lenient ingestion, and prefer MessagePack for telemetry buffers.

### Status
Completed for diagnostics event ingestion, telemetry payloads, and MessagePack encoders.

### Design / Architecture
```python
# src/obs/diagnostics_tables.py (schema sketch)
from __future__ import annotations

from typing import Annotated

import msgspec

from serde_msgspec import StructBase

EventTimeMs = Annotated[int, msgspec.Meta(ge=0)]


class DatafusionExplainEvent(StructBase):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType = msgspec.UNSET
    sql: str
    explain_analyze: bool = False


def normalize_explain(raw: object, *, now_ms: int) -> DatafusionExplainEvent:
    event = convert(raw, target_type=DatafusionExplainEvent, strict=False, from_attributes=True)
    if event.event_time_unix_ms is msgspec.UNSET:
        return msgspec.structs.replace(event, event_time_unix_ms=now_ms)
    return event
```

### Target Files
- `src/obs/diagnostics_tables.py`
- `src/obs/diagnostics.py`
- `src/datafusion_engine/runtime.py`

### Implementation Checklist
- [x] Define message structs for each diagnostics table input.
- [x] Replace `_coerce_*` helpers with `msgspec.convert(..., strict=False)`.
- [x] Encode telemetry buffers as MessagePack and decode lazily as needed.

---

## Item 6: Options and metadata normalization via msgspec.convert

### Goal
Replace `asdict`-based normalization with `msgspec.convert` and struct-aware merging.

### Status
Completed for extract registry normalization and runtime profile options.

### Design / Architecture
```python
from __future__ import annotations

from serde_msgspec import StructBase, convert, to_builtins


class ExtractOptions(StructBase):
    repo_id: str | None = None
    max_rows: int | None = None
    max_bytes: int | None = None


DEFAULT_OPTIONS = ExtractOptions()


def normalize_options(options: object | None) -> ExtractOptions:
    if options is None:
        return DEFAULT_OPTIONS
    incoming = convert(options, target_type=ExtractOptions, strict=False, from_attributes=True)
    incoming_payload = to_builtins(incoming)
    if not isinstance(incoming_payload, dict):
        return DEFAULT_OPTIONS
    return ExtractOptions(**{**to_builtins(DEFAULT_OPTIONS), **incoming_payload})
```

### Target Files
- `src/datafusion_engine/extract_registry.py`
- `src/extract/helpers.py`
- `src/engine/runtime_profile.py`

### Implementation Checklist
- [x] Introduce typed option structs for normalization points.
- [x] Use `from_attributes=True` to handle dataclass-like inputs.
- [x] Replace `asdict` usage with `convert` + `to_builtins`.

---

## Item 7: Schema export, introspection, and contract tests

### Goal
Lock wire contracts, expose JSON Schema, and add drift detection for internal artifacts.

### Status
Completed with schema snapshots and MessagePack golden files under `tests/msgspec_contract/`.

### Design / Architecture
```python
# tests/msgspec_contract/test_schema.py
from __future__ import annotations

import msgspec

from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from obs.diagnostics import PreparedStatementSpec
from sqlglot_tools.optimizer import AstArtifact


def test_msgspec_schema_snapshots(update_goldens: bool) -> None:
    schema = msgspec.json.schema(CdfCursor)
    # snapshot as pretty JSON using central serde helpers
```

```python
# src/obs/schema_introspection.py
from __future__ import annotations

import msgspec

from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from obs.diagnostics import PreparedStatementSpec
from sqlglot_tools.optimizer import AstArtifact

INFO = {
    "cdf_cursor": msgspec.inspect.type_info(CdfCursor),
    "cdf_cursor_store": msgspec.inspect.type_info(CdfCursorStore),
    "prepared_statement_spec": msgspec.inspect.type_info(PreparedStatementSpec),
    "sqlglot_ast_artifact": msgspec.inspect.type_info(AstArtifact),
}
```

### Target Files
- `tests/msgspec_contract/` (new)
- `src/obs/schema_introspection.py` (new)
- `src/incremental/cdf_cursors.py`
- `src/obs/diagnostics.py`
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/runtime.py`

### Implementation Checklist
- [x] Snapshot JSON Schema for core artifacts.
- [x] Add MessagePack golden snapshots for telemetry payloads.
- [x] Use `msgspec.inspect.type_info` to register schema metadata.

---

## Item 8: Performance and memory tuning

### Goal
Apply advanced msgspec features where throughput and memory pressure are highest.

### Status
Completed for scan telemetry structs and MessagePack encoding hot paths.

### Design / Architecture
```python
from __future__ import annotations

import msgspec

from serde_msgspec import StructBase


class ScanTelemetry(
    StructBase,
    array_like=True,
    gc=False,
    cache_hash=True,
):
    fragment_count: int
    row_group_count: int
    count_rows: int | None
```

```python
buf = bytearray()
encoder = msgspec.msgpack.Encoder(order="deterministic")
encoder.encode_into(rows, buf)
```

### Target Files
- `src/obs/diagnostics_tables.py`
- `src/datafusion_engine/runtime.py`
- `src/arrowdsl/core/scan_telemetry.py`
- `src/arrowdsl/core/metrics.py`

### Implementation Checklist
- [x] Use `array_like=True` for hot-path telemetry payloads.
- [x] Apply `gc=False` only for acyclic, high-volume structs.
- [x] Use `encode_into` to reduce allocations in hot loops.

---

## Item 9: MessagePack extension types for internal binary payloads

### Goal
Use MessagePack extension types for binary-heavy artifacts (e.g., Arrow schema) where size and speed matter.

### Status
Completed with Arrow schema MessagePack extensions and shared registry constants.

### Design / Architecture
```python
from __future__ import annotations

import msgspec
import pyarrow as pa

SCHEMA_EXT_CODE = 3


def enc_hook(obj: object) -> object:
    if isinstance(obj, pa.Schema):
        return msgspec.msgpack.Ext(SCHEMA_EXT_CODE, obj.serialize().to_pybytes())
    raise TypeError


def ext_hook(code: int, data: memoryview) -> object:
    if code == SCHEMA_EXT_CODE:
        return pa.ipc.read_schema(data)
    return msgspec.msgpack.Ext(code, data.tobytes())
```

### Target Files
- `src/arrowdsl/schema/serialization.py`
- `src/obs/repro.py`
- `src/engine/runtime_profile.py`
- `src/serde_msgspec_ext.py`

### Implementation Checklist
- [x] Define extension codes for internal binary payloads.
- [x] Use `ext_hook` to decode without stringifying binary blobs.
- [x] Document extension codes in a small registry module.
