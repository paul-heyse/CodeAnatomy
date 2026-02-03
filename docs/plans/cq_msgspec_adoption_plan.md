# CQ msgspec Adoption Plan

## Goal
Adopt msgspec across `tools/cq` for faster serialization, stronger schema validation, and deterministic outputs, leveraging the full msgspec surface (Structs, Meta constraints, UNSET, codecs, schema export, inspect).

---

## Scope 1 — Core CQ schema to msgspec.Struct

**Status:** ✅ Complete (schema migrated; mk_runmeta/mk_result preserved)

**Why**
`tools/cq/core/schema.py` currently uses dataclasses with manual `to_dict`/`from_dict`. msgspec.Struct removes that boilerplate, adds validation, and enables JSON Schema export.

**Representative snippets**
```python
# tools/cq/core/schema.py
import msgspec
from typing import Annotated, Literal

class Anchor(msgspec.Struct, frozen=True):
    file: str
    line: Annotated[int, msgspec.Meta(ge=1)]
    col: int | None = None
    end_line: int | None = None
    end_col: int | None = None

class Finding(msgspec.Struct):
    category: str
    message: str
    anchor: Anchor | None = None
    severity: Literal["info", "warning", "error"] = "info"
    details: dict[str, object] = msgspec.field(default_factory=dict)

class RunMeta(msgspec.Struct):
    macro: str
    argv: list[str]
    root: str
    started_ms: float
    elapsed_ms: float
    toolchain: dict[str, str | None] = msgspec.field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION
    run_id: str | None = None

class CqResult(msgspec.Struct):
    run: RunMeta
    summary: dict[str, object] = msgspec.field(default_factory=dict)
    key_findings: list[Finding] = msgspec.field(default_factory=list)
    evidence: list[Finding] = msgspec.field(default_factory=list)
    sections: list[Section] = msgspec.field(default_factory=list)
    artifacts: list[Artifact] = msgspec.field(default_factory=list)
```

**Target files**
- `tools/cq/core/schema.py`
- `tools/cq/core/artifacts.py` (uses schema types)
- `tools/cq/cli_app/result.py` (output formatting)

**Deprecate/remove after completion**
- `to_dict()` / `from_dict()` implementations on CQ schema types.

**Implementation checklist**
- [x] Convert all CQ schema dataclasses to `msgspec.Struct`.
- [x] Add `Meta` constraints for obvious invariants (line >= 1, severity Literal).
- [x] Replace `to_dict()`/`from_dict()` calls with msgspec encode/decode.
- [x] Keep `mk_runmeta()` / `mk_result()` API stable.

---

## Scope 2 — Unified msgspec codec utilities (JSON + Msgpack)

**Status:** ✅ Complete (central serialization utilities in place)

**Why**
Multiple modules use `json.dumps` or manual dict conversions. Use a single codec layer to standardize output and allow msgpack in caches.

**Representative snippets**
```python
# tools/cq/core/serialization.py
import msgspec

JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
JSON_DECODER = msgspec.json.Decoder(type=CqResult)

def dumps_json(result: CqResult) -> str:
    return JSON_ENCODER.encode(result).decode("utf-8")

def loads_json(data: bytes | str) -> CqResult:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return JSON_DECODER.decode(data)
```

**Target files**
- `tools/cq/core/serialization.py` (new)
- `tools/cq/cli_app/result.py`
- `tools/cq/core/artifacts.py`

**Deprecate/remove after completion**
- Direct `json.dumps(result.to_dict())` patterns in CQ output.

**Implementation checklist**
- [x] Add serialization module and wire into result/artifacts.
- [x] Ensure deterministic JSON ordering for stable outputs.

---

## Scope 3 — Cache payloads in msgpack with typed decode

**Status:** ✅ Complete (msgpack payloads, legacy JSON decode removed)

**Why**
Cache payloads in `diskcache_query_cache.py` and `diskcache_index_cache.py` currently store JSON strings and dicts. msgpack reduces size and parse costs; typed decoding improves correctness.

**Representative snippets**
```python
# tools/cq/index/diskcache_query_cache.py
import msgspec

_QUERY_ENCODER = msgspec.msgpack.Encoder()
_QUERY_DECODER = msgspec.msgpack.Decoder(type=CqResult)

entry = {
    "key": key,
    "value": _QUERY_ENCODER.encode(result),
    "file_hash": file_hash,
    "timestamp": time.time(),
    "file_paths": file_path_strs,
    "tags": tags,
}

cached = entry.get("value")
if isinstance(cached, (bytes, bytearray)):
    return _QUERY_DECODER.decode(cached)
```

**Target files**
- `tools/cq/index/diskcache_query_cache.py`
- `tools/cq/index/diskcache_index_cache.py` (store structured records in msgpack)

**Deprecate/remove after completion**
- JSON string payloads for cached records (`records_json`, `result.to_dict()` in cache).

**Implementation checklist**
- [x] Add msgpack encoder/decoder for CqResult and record lists.
- [x] Remove legacy JSON cache payload handling (breaking change).
- [x] Defer decode by storing msgpack bytes (no `msgspec.Raw` needed).

---

## Scope 4 — Query IR + Planner as msgspec.Struct (typed schema)

**Status:** ✅ Complete (IR + planner are msgspec.Struct)

**Why**
Query IR uses dataclasses and manual dict conversions. msgspec enables validation and schema export for CLI/API use.

**Representative snippets**
```python
# tools/cq/query/ir.py
class Expander(msgspec.Struct, frozen=True):
    kind: ExpanderKind
    depth: Annotated[int, msgspec.Meta(ge=1)] = 1

class Scope(msgspec.Struct, frozen=True):
    in_dir: str | None = None
    exclude: tuple[str, ...] = ()
    globs: tuple[str, ...] = ()
```

**Target files**
- `tools/cq/query/ir.py`
- `tools/cq/query/planner.py`

**Deprecate/remove after completion**
- Any manual `asdict()` serialization for IR and plan objects.

**Implementation checklist**
- [x] Convert IR dataclasses to `msgspec.Struct`.
- [x] Add constraints for depth/limits.
- [x] Update any serializer usage in plan explain output.

---

## Scope 5 — CLI config decoding via msgspec.toml / convert

**Status:** ✅ Complete (typed TOML + typed env coercion)

**Why**
Cyclopts config chain loads TOML/environment generically. msgspec can decode to typed config structs with validation.

**Representative snippets**
```python
# tools/cq/cli_app/config_types.py
class CqConfig(msgspec.Struct):
    output_format: str | None = None
    cache_dir: str | None = None
    no_save_artifact: bool = False

# tools/cq/cli_app/config.py
cfg = msgspec.toml.decode(open(path, "rb").read(), type=CqConfig)
```

**Target files**
- `tools/cq/cli_app/config.py`
- `tools/cq/cli_app/context.py`
- `tools/cq/cli_app/params.py`

**Deprecate/remove after completion**
- Manual config shape assumptions; prefer typed config.

**Implementation checklist**
- [x] Add config Structs.
- [x] Decode TOML with msgspec when config file is provided.
- [x] Use `msgspec.convert` for env value coercion where needed.

---

## Scope 6 — Schema export + inspection utilities

**Status:** ✅ Complete (schema export helper + CLI command)

**Why**
Provide explicit schema docs for CQ output and query IR, enabling downstream tooling and API contracts.

**Representative snippets**
```python
# tools/cq/core/schema_export.py
schema = msgspec.json.schema(CqResult)
components = msgspec.json.schema_components([CqResult, Query])
```

**Target files**
- `tools/cq/core/schema_export.py` (new)
- Optional CLI hook under `tools/cq/cli_app/commands/admin.py` or new command

**Deprecate/remove after completion**
- None.

**Implementation checklist**
- [x] Implement schema export helper.
- [x] Optionally add `cq schema` CLI output.

---

## Scope 7 — Macro outputs and report pipeline

**Status:** ⚠️ Partial (macro structs migrated; detail payloads still dicts)

**Why**
Many macros use dataclasses and emit structured details. msgspec can reduce overhead and standardize outputs.

**Representative snippets**
```python
# tools/cq/macros/impact.py
class ImpactNode(msgspec.Struct):
    name: str
    kind: str
    score: float
```

**Target files**
- `tools/cq/macros/*`
- `tools/cq/core/report.py`

**Deprecate/remove after completion**
- Manual dict assembly in macro detail payloads.

**Implementation checklist**
- [x] Migrate macro dataclasses to msgspec.Struct where stable.
- [x] Ensure report rendering handles Struct instances via msgspec encoder.
- [ ] Replace manual dict assembly in macro detail payloads with structured msgspec types (optional).

---

## Validation Notes
- Update golden files and tests impacted by output changes.
- **Breaking change is intended:** outputs may omit default/null fields (e.g., Anchor `col`, `end_line`, `end_col`) and caches no longer need to load legacy JSON payloads once migration is complete.

---

## Remaining Scope Summary (as of 2026-02-03)

1. **Validation updates (breaking change)**
   - Refresh goldens for the new JSON shape (default/null fields omitted).
   - Re-run CQ-specific tests once parallel work is stable and update snapshots as needed.

2. **Scope 7 follow-up (optional)**
   - Replace manual `details` dict assembly in macros with structured msgspec types if you want stricter schemas for downstream consumers.

---

## Summary of Expected Outcomes
- Faster JSON/msgpack serialization for CQ outputs and caches.
- Stronger type validation with schema constraints.
- Deterministic output formatting for tests and consumers.
- Exportable JSON Schema / OpenAPI for CQ results and query IR.
