# Msgspec Best-in-Class Deployment Plan

## Scope and Decisions

- Artifacts are internal-only: default to strict schemas with `forbid_unknown_fields=True`.
- Prefer structured payloads over JSON strings; encode only at storage or wire boundaries.
- Prefer MessagePack for internal caches and telemetry; keep JSON for human-readable artifacts.
- Missing vs null semantics are undecided; this plan includes a decision gate and shows an `UNSET` pattern.

---

## Item 1: Central msgspec policy + serde module

### Goal
Create a single, shared module that defines default struct behavior, encoders/decoders, ordering, hooks, and boundary helpers (JSON and MessagePack).

### Design / Architecture
```python
# src/serde_msgspec.py
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

import msgspec
import msgspec.json
import msgspec.msgpack

if TYPE_CHECKING:
    import pyarrow as pa

T = TypeVar("T")


class StructBase(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    omit_defaults=True,
    repr_omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Base struct for internal artifacts."""


_DEFAULT_ORDER = "deterministic"


def _enc_hook(obj: object) -> object:
    if isinstance(obj, Path):
        return str(obj)
    # Keep pyarrow import local to avoid heavy module import at startup.
    try:
        import pyarrow as pa  # noqa: WPS433
    except ImportError:
        pa = None
    if pa is not None and isinstance(obj, pa.Schema):
        return {"__arrow_schema__": obj.to_string()}
    raise TypeError


def _dec_hook(type_hint: Any, obj: object) -> object:
    if type_hint is Path and isinstance(obj, str):
        return Path(obj)
    return obj


JSON_ENCODER = msgspec.json.Encoder(enc_hook=_enc_hook, order=_DEFAULT_ORDER)
JSON_DECODER = msgspec.json.Decoder(dec_hook=_dec_hook, strict=True)

MSGPACK_ENCODER = msgspec.msgpack.Encoder(enc_hook=_enc_hook, order=_DEFAULT_ORDER)
MSGPACK_DECODER = msgspec.msgpack.Decoder(dec_hook=_dec_hook, strict=True)


def dumps_json(obj: object, *, pretty: bool = False) -> bytes:
    raw = JSON_ENCODER.encode(obj)
    if not pretty:
        return raw
    return msgspec.json.format(raw, indent=2)


def loads_json(buf: bytes, *, type: type[T], strict: bool = True) -> T:
    decoder = msgspec.json.Decoder(type=type, dec_hook=_dec_hook, strict=strict)
    return decoder.decode(buf)


def dumps_msgpack(obj: object) -> bytes:
    return MSGPACK_ENCODER.encode(obj)


def loads_msgpack(buf: bytes, *, type: type[T], strict: bool = True) -> T:
    decoder = msgspec.msgpack.Decoder(type=type, dec_hook=_dec_hook, strict=strict)
    return decoder.decode(buf)


def encode_json_into(obj: object, buf: bytearray) -> None:
    JSON_ENCODER.encode_into(obj, buf)


def encode_json_lines(items: list[object]) -> bytes:
    return JSON_ENCODER.encode_lines(items)
```

### Target Files
- `src/serde_msgspec.py` (new)
- `src/obs/manifest.py`
- `src/obs/repro.py`
- `src/incremental/cdf_cursors.py`
- `src/sqlglot_tools/optimizer.py`
- `src/engine/runtime_profile.py`
- `src/datafusion_engine/bridge.py`

### Implementation Checklist
- [ ] Add `StructBase` and JSON/MessagePack encoders/decoders in a shared module.
- [ ] Centralize ordering policy (`order="deterministic"`) and hook logic.
- [ ] Define helper functions for `encode_into` and `encode_lines` for hot paths.

---

## Item 2: Missing vs null decision gate (UNSET policy)

### Goal
Decide where "missing" must be distinguished from `None`, and implement `msgspec.UNSET` only where it matters.

### Design / Architecture
```python
from __future__ import annotations

from typing import Annotated, Literal

import msgspec

from serde_msgspec import StructBase

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
- [ ] Adopt `UNSET` only for those fields and document the contract.
- [ ] Add a small helper to normalize `UNSET` at read/write boundaries.

---

## Item 3: Migrate core artifacts to msgspec.Struct

### Goal
Replace dataclass + `asdict` + bespoke normalization with typed structs and reusable encoders.

### Design / Architecture
```python
# src/incremental/cdf_cursors.py
from __future__ import annotations

from pathlib import Path
from typing import Annotated

import msgspec

from serde_msgspec import StructBase, dumps_json, loads_json

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]


class CdfCursor(StructBase):
    dataset_name: str
    last_version: NonNegInt


def _cursor_path(base: Path, dataset_name: str) -> Path:
    safe = dataset_name.replace("/", "_").replace("\\", "_")
    return base / f"{safe}.cursor.json"


def save_cursor(base: Path, cursor: CdfCursor) -> None:
    base.mkdir(parents=True, exist_ok=True)
    _cursor_path(base, cursor.dataset_name).write_bytes(dumps_json(cursor, pretty=True))


def load_cursor(base: Path, dataset_name: str) -> CdfCursor | None:
    path = _cursor_path(base, dataset_name)
    if not path.exists():
        return None
    try:
        return loads_json(path.read_bytes(), type=CdfCursor, strict=False)
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


class DatasetRecord(StructBase):
    name: str
    kind: Literal["input", "intermediate", "relationship_output", "cpg_output"]
    path: str | None = None
    rows: NonNegInt | None = None
    schema: list[JsonDict] | None = None


class ManifestV1(StructBase, tag="manifest.v1"):
    schema_version: Literal["v1"] = "v1"
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
- [ ] Replace `dataclass` definitions with `StructBase` for serialized artifacts.
- [ ] Remove `asdict` and `_normalize_value` usage in favor of typed structs.
- [ ] Use `msgspec.Meta` constraints for invariants (e.g., non-negative counts).

---

## Item 4: Structured AST/policy/runtime artifacts (no JSON strings)

### Goal
Store structured payloads in structs; stop embedding JSON strings in artifacts. Use `Raw` or MessagePack for compact storage.

### Design / Architecture
```python
# src/sqlglot_tools/optimizer.py (artifact rewrite)
from __future__ import annotations

import msgspec

from serde_msgspec import StructBase
from sqlglot import dump
from sqlglot.expressions import Expression

AST_ENCODER = msgspec.msgpack.Encoder(order="deterministic")
AST_DECODER = msgspec.msgpack.Decoder(type=dict[str, object], strict=False)


class SqlglotAstArtifact(StructBase):
    sql: str
    ast_raw: msgspec.Raw
    policy_hash: str


def ast_to_raw(expr: Expression) -> msgspec.Raw:
    return msgspec.Raw(AST_ENCODER.encode(dump(expr)))


def ast_from_raw(raw: msgspec.Raw) -> dict[str, object]:
    return AST_DECODER.decode(raw)
```

```python
# src/engine/runtime_profile.py (snapshot rewrite)
from __future__ import annotations

import msgspec

from serde_msgspec import StructBase


class RuntimeProfileArtifact(StructBase):
    runtime_profile: dict[str, object]
    sqlglot_policy: dict[str, object] | None
    function_registry: dict[str, object]
```

### Target Files
- `src/sqlglot_tools/optimizer.py`
- `src/engine/runtime_profile.py`
- `src/relspec/compiler.py`
- `src/datafusion_engine/bridge.py`

### Implementation Checklist
- [ ] Replace JSON string fields with structured payload fields.
- [ ] Store AST/policy snapshots as `Raw` (MessagePack) when size matters.
- [ ] Use `Raw.copy()` when retaining in long-lived caches.

---

## Item 5: Typed diagnostics and telemetry ingestion (MessagePack first)

### Goal
Replace coercion helpers with typed decode + `strict=False` for lenient ingestion, and prefer MessagePack for telemetry buffers.

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
    event = msgspec.convert(raw, type=DatafusionExplainEvent, strict=False)
    if event.event_time_unix_ms is msgspec.UNSET:
        return msgspec.structs.replace(event, event_time_unix_ms=now_ms)
    return event
```

### Target Files
- `src/obs/diagnostics_tables.py`
- `src/obs/diagnostics.py`
- `src/datafusion_engine/runtime.py`

### Implementation Checklist
- [ ] Define message structs for each diagnostics table input.
- [ ] Replace `_coerce_*` helpers with `msgspec.convert(..., strict=False)`.
- [ ] Encode telemetry buffers as MessagePack and decode lazily as needed.

---

## Item 6: Options and metadata normalization via msgspec.convert

### Goal
Replace `asdict`-based normalization with `msgspec.convert` and struct-aware merging.

### Design / Architecture
```python
from __future__ import annotations

import msgspec

from serde_msgspec import StructBase


class ExtractOptions(StructBase):
    repo_id: str | None = None
    max_rows: int | None = None
    max_bytes: int | None = None


DEFAULT_OPTIONS = ExtractOptions()


def normalize_options(options: object | None) -> ExtractOptions:
    if options is None:
        return DEFAULT_OPTIONS
    incoming = msgspec.convert(
        options,
        type=ExtractOptions,
        strict=False,
        from_attributes=True,
    )
    return msgspec.structs.replace(DEFAULT_OPTIONS, **msgspec.structs.asdict(incoming))
```

### Target Files
- `src/datafusion_engine/extract_registry.py`
- `src/extract/helpers.py`
- `src/engine/runtime_profile.py`

### Implementation Checklist
- [ ] Introduce typed option structs for normalization points.
- [ ] Use `from_attributes=True` to handle dataclass-like inputs.
- [ ] Replace `asdict` usage with `msgspec.structs.asdict`.

---

## Item 7: Schema export, introspection, and contract tests

### Goal
Lock wire contracts, expose JSON Schema, and add drift detection for internal artifacts.

### Design / Architecture
```python
# tests/msgspec_contract/test_schema.py
from __future__ import annotations

import msgspec

from obs.manifest import ManifestV1


def test_manifest_schema_snapshot(update_goldens: bool) -> None:
    schema = msgspec.json.schema(ManifestV1)
    # snapshot as pretty JSON using central serde helpers
```

```python
# src/obs/schema_introspection.py
from __future__ import annotations

import msgspec

from obs.manifest import ManifestV1

INFO = msgspec.inspect.type_info(ManifestV1)
```

### Target Files
- `tests/msgspec_contract/` (new)
- `src/obs/manifest.py`
- `src/obs/repro.py`
- `src/engine/runtime_profile.py`

### Implementation Checklist
- [ ] Snapshot JSON Schema for core artifacts.
- [ ] Add MessagePack golden snapshots for telemetry payloads.
- [ ] Use `msgspec.inspect.type_info` to register schema metadata.

---

## Item 8: Performance and memory tuning

### Goal
Apply advanced msgspec features where throughput and memory pressure are highest.

### Design / Architecture
```python
from __future__ import annotations

import msgspec

from serde_msgspec import StructBase


class TelemetrySpan(
    StructBase,
    array_like=True,
    gc=False,
    cache_hash=True,
):
    span_id: str
    start_ms: int
    end_ms: int
    tags: dict[str, str]
```

```python
buf = bytearray()
encoder = msgspec.msgpack.Encoder(order="deterministic")
encoder.encode_into(spans, buf)
```

### Target Files
- `src/obs/diagnostics_tables.py`
- `src/datafusion_engine/runtime.py`
- `src/arrowdsl/core/metrics.py`

### Implementation Checklist
- [ ] Use `array_like=True` for hot-path telemetry payloads.
- [ ] Apply `gc=False` only for acyclic, high-volume structs.
- [ ] Use `encode_into` to reduce allocations in hot loops.

---

## Item 9: MessagePack extension types for internal binary payloads

### Goal
Use MessagePack extension types for binary-heavy artifacts (e.g., Arrow schema) where size and speed matter.

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

### Implementation Checklist
- [ ] Define extension codes for internal binary payloads.
- [ ] Use `ext_hook` to decode without stringifying binary blobs.
- [ ] Document extension codes in a small registry module.
