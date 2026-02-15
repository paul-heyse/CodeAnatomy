# CQ Tools Dedup + Msgspec Best-in-Class Implementation Plan v1 (2026-02-15)

## 2. Scope Summary
This revised plan incorporates post-v1 tree-sitter and Rust-lane uplifts already landed in `tools/cq`, then expands scope to complete consolidation and msgspec-first boundary design. Design stance remains: CQ-only hard cutover, no `src/` integration, and no long-lived compatibility shims for duplicated CQ internals. The execution model is now stabilization-first (correctness and pathing), then consolidation, then decommission.

Excluded scope:
- Any coupling or shared runtime integration with `src/`.

## 3. Design Principles
1. `tools/cq` remains fully independent from `src/`.
2. Boundary contracts are typed-first: decode/convert at boundaries, not ad hoc dict coercion inside feature logic.
3. Consolidate before extending: one canonical helper per concern (serialization, target resolution, scoring, cache codecs, tree-sitter runtime plumbing).
4. Enforce strictness where data crosses module or persistence boundaries; keep internal runtime-only objects explicitly separated.
5. Prefer msgspec primitives (`Struct`, `Meta`, tagged unions, `UNSET`, reusable encoders/decoders) over manual dict shaping.
6. Stabilize correctness first: fix path/cycle regressions before additional feature layering.
7. Deletions are mandatory once replacement paths are fully landed.
8. Enforce one canonical contract key per payload concern (no long-lived alias keys).

## 4. Current Baseline
- Substantial tree-sitter uplift has landed, including `autotune`, `windowing`, typed query-hit export, typed diagnostic export, contract snapshots, grammar diff helpers, Rust tags/runtime, Rust module graph contracts, macro expansion bridge contracts, and expanded tree-sitter test coverage.
- `query.registry` local query directory resolution is currently inconsistent with the repository query location (`tools/cq/search/tree_sitter/query/registry.py` vs `tools/cq/search/queries/`), creating silent no-pack behavior risk.
- `diagnostics.collector` currently resolves diagnostics queries from a local non-existent `diagnostics/queries/` path instead of shared query packs in `tools/cq/search/queries/`.
- Tree-sitter payload keys are mixed (`tree_sitter_diagnostics` and `cst_diagnostics`), and consumers carry compatibility fallbacks instead of one canonical contract.
- Query-pack load/compile/plan loops remain duplicated across `tools/cq/search/tree_sitter/python_lane/runtime.py`, `tools/cq/search/tree_sitter/python_lane/facts.py`, and `tools/cq/search/tree_sitter/rust_lane/runtime.py`.
- Serialization and builtins conversion still overlap across `tools/cq/core/codec.py`, `tools/cq/core/serialization.py`, `tools/cq/core/public_serialization.py`, `tools/cq/core/contracts.py`, and `tools/cq/search/_shared/core.py`.
- CLI step modeling remains duplicated between `tools/cq/cli_app/step_types.py` and `tools/cq/run/spec.py`, with dataclass-to-msgspec conversion bridge logic in `tools/cq/run/loader.py`.
- Macro request base shape, file target resolution, and scoring payload shaping remain duplicated across macro modules.
- Cache decode/encode fallback logic is still duplicated in `tools/cq/core/cache/fragment_codecs.py`, `tools/cq/core/cache/tree_sitter_cache_store.py`, and `tools/cq/core/cache/search_artifact_store.py`.
- Msgspec `Meta`, `UNSET`, schema export coverage, and strict boundary policy are still uneven across output/public contract surfaces.

## 5. Per-Scope-Item Plan

## S0. Stabilization and Correctness Gate
### Goal
Fix high-risk correctness regressions introduced by recent CQ tree-sitter changes before expanding architecture scope. Specifically: canonical query resource pathing, diagnostics query pathing, and canonical diagnostics key contract.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/query/resource_paths.py
from __future__ import annotations

from pathlib import Path

_QUERY_ROOT = Path(__file__).resolve().parents[3] / "queries"


def query_pack_path(language: str, pack_name: str) -> Path:
    return _QUERY_ROOT / language / pack_name


def query_contracts_path(language: str) -> Path:
    return _QUERY_ROOT / language / "contracts.yaml"


def diagnostics_query_path(language: str) -> Path:
    return _QUERY_ROOT / language / "95_diagnostics.scm"
```

```python
# tools/cq/search/tree_sitter/python_lane/runtime.py
# Canonicalized payload contract key
payload["cst_diagnostics"] = [msgspec.to_builtins(row) for row in diagnostics]
```

### Files to Edit
- `tools/cq/search/tree_sitter/query/registry.py`
- `tools/cq/search/tree_sitter/diagnostics/collector.py`
- `tools/cq/search/tree_sitter/contracts/query_models.py`
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/semantic/models.py`
- `tools/cq/search/tree_sitter/query/lint.py`

### New Files to Create
- `tools/cq/search/tree_sitter/query/resource_paths.py`
- `tests/unit/cq/search/tree_sitter/test_query_resource_paths.py`

### Legacy Decommission/Delete Scope
- Delete direct local query path builders in `tools/cq/search/tree_sitter/query/registry.py` and `tools/cq/search/tree_sitter/diagnostics/collector.py` once shared resource path helpers are adopted.
- Delete Python diagnostics key alias fallback usage in consumers after `cst_diagnostics` is canonicalized at emit points.

---

## S1. Canonical Contract Codec and Serialization Boundary
### Goal
Create one canonical serialization boundary for CQ contracts (JSON/msgpack encode/decode + builtins conversion), then route all existing callers through it. Remove duplicated conversion policy and mapping checks.

### Representative Code Snippets
```python
# tools/cq/core/contract_codec.py
from __future__ import annotations

from typing import Any

import msgspec

JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
JSON_DECODER = msgspec.json.Decoder(strict=True)
MSGPACK_ENCODER = msgspec.msgpack.Encoder()


def encode_json(value: object, *, indent: int | None = None) -> str:
    payload = JSON_ENCODER.encode(value)
    if indent is None:
        return payload.decode("utf-8")
    return msgspec.json.format(payload, indent=indent).decode("utf-8")


def to_contract_builtins(value: object) -> object:
    return msgspec.to_builtins(value, order="deterministic", str_keys=True)


def require_mapping(value: object) -> dict[str, object]:
    payload = to_contract_builtins(value)
    if isinstance(payload, dict):
        return payload
    msg = f"Expected mapping payload, got {type(payload).__name__}"
    raise TypeError(msg)
```

```python
# tools/cq/core/contracts.py
from tools.cq.core.contract_codec import require_mapping, to_contract_builtins

# Existing contract helpers become thin wrappers or are inlined and deleted.
```

### Files to Edit
- `tools/cq/core/codec.py`
- `tools/cq/core/serialization.py`
- `tools/cq/core/public_serialization.py`
- `tools/cq/core/contracts.py`
- `tools/cq/search/_shared/core.py`
- `tools/cq/search/rg/contracts.py`
- `tools/cq/query/executor.py`

### New Files to Create
- `tools/cq/core/contract_codec.py`
- `tests/unit/cq/core/test_contract_codec.py`

### Legacy Decommission/Delete Scope
- Delete duplicated JSON helpers from `tools/cq/core/serialization.py`: `dumps_json`, `loads_json`, and `to_builtins` after callers migrate.
- Delete duplicated public conversion helpers from `tools/cq/core/public_serialization.py`: `to_public_dict`, `to_public_list` after canonical helpers are adopted.
- Delete `contract_to_builtins` duplication in `tools/cq/core/contracts.py` once `to_contract_builtins` is canonical.

---

## S2. Typed Boundary Adapter Layer for Coercion and Error Taxonomy
### Goal
Replace scattered `_coerce_*` and broad exception catches with shared typed adapters that standardize msgspec conversion behavior, strictness modes, and decode error handling.

### Representative Code Snippets
```python
# tools/cq/core/typed_boundary.py
from __future__ import annotations

from typing import TypeVar

import msgspec

T = TypeVar("T")


class BoundaryDecodeError(RuntimeError):
    pass


def convert_strict[T](payload: object, *, type_: type[T]) -> T:
    try:
        return msgspec.convert(payload, type=type_, strict=True)
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError) as exc:
        raise BoundaryDecodeError(str(exc)) from exc


def convert_lax[T](payload: object, *, type_: type[T]) -> T:
    try:
        return msgspec.convert(payload, type=type_, strict=False)
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError) as exc:
        raise BoundaryDecodeError(str(exc)) from exc
```

```python
# tools/cq/run/loader.py
from tools.cq.core.typed_boundary import convert_strict

# before: msgspec.convert(item, type=RunStep, strict=True)
step = convert_strict(item, type_=RunStep)
```

### Files to Edit
- `tools/cq/run/loader.py`
- `tools/cq/search/rg/codec.py`
- `tools/cq/core/diagnostics_contracts.py`
- `tools/cq/core/multilang_summary.py`
- `tools/cq/core/multilang_orchestrator.py`
- `tools/cq/search/_shared/search_contracts.py`
- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/core/front_door_insight.py`

### New Files to Create
- `tools/cq/core/typed_boundary.py`
- `tests/unit/cq/core/test_typed_boundary.py`

### Legacy Decommission/Delete Scope
- Delete local coercion helpers in `tools/cq/core/diagnostics_contracts.py`: `_coerce_dict`, `_coerce_list_of_dict`.
- Delete ad hoc conversion error patterns in `tools/cq/search/rg/codec.py`: repeated `except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError)` blocks.
- Delete redundant scalar coercers duplicated between `tools/cq/core/multilang_summary.py` and `tools/cq/query/executor.py` after shared adapters land.

---

## S3. CLI Contract Unification with Tagged Unions
### Goal
Unify Cyclopts-facing and run-plan-facing CLI models into one msgspec-first contract surface using tagged unions for steps, eliminating params/options/step-type duplication and `asdict` bridges.

### Representative Code Snippets
```python
# tools/cq/cli_app/contracts.py
from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqSettingsStruct


class CliRunStepBase(CqSettingsStruct, frozen=True, tag=True, tag_field="type"):
    id: str | None = None


class CliQStep(CliRunStepBase, tag="q", frozen=True):
    query: str


class CliSearchStep(CliRunStepBase, tag="search", frozen=True):
    query: str
    regex: bool = False
    literal: bool = False


type CliRunStep = CliQStep | CliSearchStep


class CliRunOptions(CqSettingsStruct, frozen=True):
    step: tuple[CliRunStep, ...] = ()
    steps: tuple[CliRunStep, ...] = ()
    stop_on_error: bool = False
```

```python
# tools/cq/cli_app/options.py
from tools.cq.core.typed_boundary import convert_strict


def options_from_params[T](params: object, *, type_: type[T]) -> T:
    return convert_strict(params, type_=type_)
```

### Files to Edit
- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/options.py`
- `tools/cq/cli_app/step_types.py`
- `tools/cq/cli_app/commands/run.py`
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/run/spec.py`
- `tools/cq/run/loader.py`

### New Files to Create
- `tools/cq/cli_app/contracts.py`
- `tests/unit/cq/cli/test_cli_contracts.py`
- `tests/unit/cq/cli/test_run_step_cli_decode.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/cli_app/step_types.py` once `CliRunStep` tagged-union contracts replace it.
- Delete `asdict`-based conversion in `tools/cq/cli_app/options.py`.
- Delete duplicate step shape definitions in `tools/cq/run/spec.py` that are superseded by shared CLI/run step contracts.

---

## S4. Macro Request Base Contracts and Entry Signature Normalization
### Goal
Standardize macro request shapes with shared msgspec base structs and normalize macro entrypoints to accept request objects consistently.

### Representative Code Snippets
```python
# tools/cq/macros/contracts.py
from __future__ import annotations

from pathlib import Path

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain


class MacroRequestBase(CqStruct, frozen=True):
    tc: Toolchain
    root: Path
    argv: tuple[str, ...]


class ScopedMacroRequestBase(MacroRequestBase, frozen=True):
    include: tuple[str, ...] = msgspec.field(default_factory=tuple)
    exclude: tuple[str, ...] = msgspec.field(default_factory=tuple)
```

```python
# tools/cq/macros/exceptions.py
class ExceptionsRequest(ScopedMacroRequestBase, frozen=True):
    function: str | None = None


def cmd_exceptions(request: ExceptionsRequest) -> CqResult:
    ...
```

### Files to Edit
- `tools/cq/macros/imports.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/macros/bytecode.py`
- `tools/cq/macros/calls.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/core/bundles.py`

### New Files to Create
- `tools/cq/macros/contracts.py`
- `tests/unit/cq/macros/test_macro_contracts.py`

### Legacy Decommission/Delete Scope
- Delete bespoke per-macro duplicates of `tc/root/argv` fields in request structs once they inherit shared bases.
- Delete positional `cmd_exceptions(tc, root, argv, ...)` call path in favor of request-object path.

---

## S5. Shared Macro Scan and Target Resolution Helpers
### Goal
Centralize Python file scanning and symbol/path target resolution for macro workflows; remove repeated implementations in bytecode/scopes/imports/exceptions/side-effects.

### Representative Code Snippets
```python
# tools/cq/macros/scan_utils.py
from __future__ import annotations

import ast
from collections.abc import Callable
from pathlib import Path

from tools.cq.macros.scope_filters import resolve_macro_files


def iter_python_asts(
    *,
    root: Path,
    include: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    max_files: int | None = None,
) -> list[tuple[Path, ast.AST, str]]:
    files = resolve_macro_files(root=root, include=include, exclude=exclude, extensions=(".py",))
    if max_files is not None:
        files = files[:max_files]
    out: list[tuple[Path, ast.AST, str]] = []
    for pyfile in files:
        try:
            src = pyfile.read_text(encoding="utf-8")
            out.append((pyfile, ast.parse(src, filename=pyfile.name), src))
        except (OSError, UnicodeDecodeError, SyntaxError):
            continue
    return out
```

```python
# tools/cq/macros/target_resolution.py
from tools.cq.macros.calls_target import resolve_target_definition


def resolve_target_files(root: Path, target: str, *, language: str | None, max_files: int) -> list[Path]:
    # Reuse calls-target resolver for symbol matching; preserve direct file path short-circuit.
    ...
```

### Files to Edit
- `tools/cq/macros/bytecode.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/macros/imports.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/macros/calls_target.py`
- `tools/cq/macros/scope_filters.py`

### New Files to Create
- `tools/cq/macros/scan_utils.py`
- `tools/cq/macros/target_resolution.py`
- `tests/unit/cq/macros/test_scan_utils.py`
- `tests/unit/cq/macros/test_target_resolution.py`

### Legacy Decommission/Delete Scope
- Delete `_iter_search_files` and `_resolve_target_files` from `tools/cq/macros/bytecode.py`.
- Delete `_iter_search_files` and `_resolve_target_files` from `tools/cq/macros/scopes.py`.
- Delete per-module `_iter_python_files` duplicates in `tools/cq/macros/imports.py`, `tools/cq/macros/exceptions.py`, and `tools/cq/macros/side_effects.py`.

---

## S6. Shared Macro Scoring Pipeline
### Goal
Eliminate repeated scoring dict construction by introducing a shared macro scoring helper that returns `ScoreDetails` and standardized detail payload builders.

### Representative Code Snippets
```python
# tools/cq/macros/scoring_utils.py
from __future__ import annotations

from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    build_detail_payload,
    build_score_details,
)
from tools.cq.core.schema import DetailPayload


def macro_detail_payload(
    *,
    sites: int,
    files: int,
    depth: int,
    breakages: int,
    ambiguities: int,
    evidence_kind: str,
    data: dict[str, object] | None = None,
) -> DetailPayload:
    score = build_score_details(
        impact=ImpactSignals(
            sites=sites,
            files=files,
            depth=depth,
            breakages=breakages,
            ambiguities=ambiguities,
        ),
        confidence=ConfidenceSignals(evidence_kind=evidence_kind),
    )
    return build_detail_payload(score=score, data=data)
```

```python
# tools/cq/macros/sig_impact.py
# before: scoring_details dict + build_detail_payload(scoring=...)
# after:
section.findings.append(
    Finding(
        ...,
        details=macro_detail_payload(
            sites=len(all_sites),
            files=unique_files,
            depth=0,
            breakages=len(buckets["would_break"]),
            ambiguities=len(buckets["ambiguous"]),
            evidence_kind="resolved_ast",
            data=details,
        ),
    )
)
```

### Files to Edit
- `tools/cq/macros/impact.py`
- `tools/cq/macros/imports.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/macros/bytecode.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/core/scoring.py`

### New Files to Create
- `tools/cq/macros/scoring_utils.py`
- `tests/unit/cq/macros/test_scoring_utils.py`

### Legacy Decommission/Delete Scope
- Delete `scoring_details: dict[str, object] = {...}` pattern blocks from macro modules listed above.
- Delete helper functions returning raw scoring dicts: `_build_imports_scoring` (`tools/cq/macros/imports.py`) and `_build_exception_scoring` (`tools/cq/macros/exceptions.py`) once replaced.

---

## S7. Tree-Sitter Runtime Utility Consolidation
### Goal
Complete consolidation on top of already-landed tree-sitter runtime uplifts (`autotune`, `windowing`, `locals_index`, `query_hits`, `diagnostic_export`, `tags`, snapshot/diff helpers). Primary goal is now adoption + deletion of duplicated loaders/path builders/text helpers across neighborhood and tree-sitter lanes.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/core/language_runtime.py
from __future__ import annotations

from functools import lru_cache

from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language

def load_language(language: str):
    resolved = load_tree_sitter_language(language)
    if resolved is None:
        msg = f"tree-sitter language unavailable: {language}"
        raise RuntimeError(msg)
    return resolved


def make_parser(language: str):
    from tree_sitter import Parser

    parser = Parser(load_language(language))
    return parser
```

```python
# tools/cq/search/tree_sitter/query/registry.py
# Adopt shared resource pathing instead of local __file__-relative query directories.
from __future__ import annotations

from tools.cq.search.tree_sitter.query.resource_paths import query_pack_path

def _local_query_dir(language: str):
    return query_pack_path(language, "").parent
```

```python
# tools/cq/search/tree_sitter/core/text_utils.py
from __future__ import annotations


def node_text(node, source_bytes: bytes, *, strip: bool = True, max_len: int | None = None) -> str:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    text = source_bytes[start:end].decode("utf-8", errors="replace")
    out = text.strip() if strip else text
    if max_len is not None and len(out) > max_len:
        return out[: max(1, max_len - 3)] + "..."
    return out
```

### Files to Edit
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py`
- `tools/cq/search/tree_sitter/query/registry.py`
- `tools/cq/search/tree_sitter/diagnostics/collector.py`
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/structural/export.py`
- `tools/cq/search/tree_sitter/structural/match_rows.py`

### New Files to Create
- `tools/cq/search/tree_sitter/core/language_runtime.py`
- `tools/cq/search/tree_sitter/core/text_utils.py`
- `tests/unit/cq/search/tree_sitter/test_language_runtime.py`
- `tests/unit/cq/search/tree_sitter/test_text_utils.py`

### Legacy Decommission/Delete Scope
- Delete duplicated `_python_language`/`_rust_language` loader functions in affected modules.
- Delete duplicated local query-path helper functions (for example `_query_path`) in neighborhood/query modules after shared path helpers are adopted.
- Delete duplicated `_node_text` helpers in affected modules once shared text utility is used.

---

## S8. Unified Typed Cache Codec Layer
### Goal
Consolidate cache decode/encode logic and error handling into one typed codec utility reused by fragment, tree-sitter, and artifact stores.

### Representative Code Snippets
```python
# tools/cq/core/cache/typed_codecs.py
from __future__ import annotations

from functools import lru_cache
from typing import TypeVar

import msgspec

T = TypeVar("T")

_ENCODER = msgspec.msgpack.Encoder()


@lru_cache(maxsize=64)
def decoder_for[T](*, type_: type[T]) -> msgspec.msgpack.Decoder[T]:
    return msgspec.msgpack.Decoder(type=type_)


def decode_payload[T](payload: object, *, type_: type[T]) -> T | None:
    try:
        if isinstance(payload, (bytes, bytearray, memoryview)):
            return decoder_for(type_=type_).decode(payload)
        if isinstance(payload, dict):
            return msgspec.convert(payload, type=type_, strict=True)
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError):
        return None
    return None


def encode_payload(value: object) -> bytes:
    return _ENCODER.encode(value)
```

```python
# tools/cq/core/cache/tree_sitter_cache_store.py
from tools.cq.core.cache.typed_codecs import decode_payload, encode_payload

encoded = encode_payload(envelope)
loaded = decode_payload(payload, type_=TreeSitterCacheEnvelopeV1)
```

### Files to Edit
- `tools/cq/core/cache/fragment_codecs.py`
- `tools/cq/core/cache/tree_sitter_cache_store.py`
- `tools/cq/core/cache/search_artifact_store.py`
- `tools/cq/core/cache/fragment_engine.py`

### New Files to Create
- `tools/cq/core/cache/typed_codecs.py`
- `tests/unit/cq/core/cache/test_typed_codecs.py`

### Legacy Decommission/Delete Scope
- Delete `_msgpack_decoder` and duplicate fallback decode branches from `tools/cq/core/cache/fragment_codecs.py`.
- Delete duplicated inline msgpack decode fallback branches in `tools/cq/core/cache/tree_sitter_cache_store.py` and `tools/cq/core/cache/search_artifact_store.py`.

---

## S9. Strict Output Contract Policy for External/Public Boundaries
### Goal
Introduce explicit strict-output contract tiers so public artifact/renderer boundaries reject unknown fields while internal mutable summary assembly remains controlled.

### Representative Code Snippets
```python
# tools/cq/core/structs.py
class CqStrictOutputStruct(
    msgspec.Struct,
    kw_only=True,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Strict output boundary contract for persisted/external payloads."""
```

```python
# tools/cq/search/_shared/search_contracts.py
from tools.cq.core.structs import CqStrictOutputStruct


class SearchSummaryContract(CqStrictOutputStruct, frozen=True):
    ...
```

### Files to Edit
- `tools/cq/core/structs.py`
- `tools/cq/search/_shared/search_contracts.py`
- `tools/cq/core/diagnostics_contracts.py`
- `tools/cq/core/cache/contracts.py`
- `tools/cq/core/schema.py`

### New Files to Create
- `tests/unit/cq/core/test_output_contract_strictness.py`

### Legacy Decommission/Delete Scope
- Delete permissive output contract inheritance where payloads are externally persisted or rendered without strict validation.
- Delete ad hoc unknown-field filtering that becomes unnecessary under strict boundary structs.

---

## S10. Expand Msgspec `Meta` Constraints and `UNSET` Usage
### Goal
Systematically enforce contract-level numeric/string invariants with `Annotated[..., msgspec.Meta(...)]` and adopt `UNSET` for omission semantics where fields are optional-but-not-null by default.

### Representative Code Snippets
```python
# tools/cq/core/contracts_constraints.py
from __future__ import annotations

from typing import Annotated

import msgspec

PositiveInt = Annotated[int, msgspec.Meta(ge=1)]
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]
NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]
BoundedRatio = Annotated[float, msgspec.Meta(ge=0.0, le=1.0)]
```

```python
# tools/cq/search/pipeline/profiles.py
from tools.cq.core.contracts_constraints import PositiveInt


class SearchLimits(CqSettingsStruct, frozen=True):
    max_files: PositiveInt = 5000
    max_matches_per_file: PositiveInt = 1000
    ...
```

```python
# tools/cq/search/_shared/search_contracts.py
class PythonSemanticOverview(CqOutputStruct, frozen=True):
    primary_symbol: str | msgspec.UnsetType | None = msgspec.UNSET
```

### Files to Edit
- `tools/cq/search/pipeline/profiles.py`
- `tools/cq/core/runtime/execution_policy.py`
- `tools/cq/core/cache/contracts.py`
- `tools/cq/search/tree_sitter/contracts/core_models.py`
- `tools/cq/search/_shared/search_contracts.py`
- `tools/cq/core/schema.py`

### New Files to Create
- `tools/cq/core/contracts_constraints.py`
- `tests/unit/cq/core/test_contract_constraints.py`

### Legacy Decommission/Delete Scope
- Delete unconstrained scalar contract fields in high-risk boundary structs (limits/policy/cache envelopes) once typed aliases are in place.
- Delete manual post-conversion range checks that become redundant under `Meta` constraints.

---

## S11. Expand CQ Schema Export + Contract Drift Guardrails
### Goal
Expand schema export/drift guardrails beyond currently landed tree-sitter grammar snapshot/diff helpers, covering CQ boundary contracts end-to-end (run steps, search summaries, tree-sitter payload contracts, cache contracts, and front-door outputs).

### Representative Code Snippets
```python
# tools/cq/core/schema_export.py
from tools.cq.run.spec import RunPlan, RunStep
from tools.cq.search.tree_sitter.contracts.core_models import (
    TreeSitterArtifactBundleV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search._shared.search_contracts import SearchSummaryContract


def cq_schema_components() -> tuple[tuple[dict[str, object], ...], dict[str, object]]:
    return msgspec.json.schema_components(
        [
            CqResult,
            Query,
            RunPlan,
            RunStep,
            SearchSummaryContract,
            TreeSitterArtifactBundleV1,
            TreeSitterDiagnosticV1,
            TreeSitterQueryHitV1,
        ],
        schema_hook=cast("Any", _schema_hook),
    )
```

```python
# tests/msgspec_contract/test_cq_schema_contract.py
def test_cq_schema_components_snapshot() -> None:
    schemas, components = cq_schema_components()
    assert schemas
    assert "RunPlan" in components or "RunStep" in components
    assert "TreeSitterArtifactBundleV1" in components
```

### Files to Edit
- `tools/cq/core/schema_export.py`
- `tools/cq/cli_app/commands/admin.py`
- `tests/unit/cq/core/test_schema_export.py`
- `tools/cq/search/tree_sitter/query/contract_snapshot.py`
- `tools/cq/search/tree_sitter/query/grammar_drift.py`

### New Files to Create
- `tests/msgspec_contract/test_cq_schema_contract.py`
- `tests/msgspec_contract/test_cq_contract_roundtrip.py`
- `tests/msgspec_contract/test_cq_tree_sitter_contract_schema.py`

### Legacy Decommission/Delete Scope
- Delete implicit “schema by convention” assumptions for run-step and summary contracts; replace with explicit schema export coverage.
- Delete partial schema tests that only validate `CqResult`/`Query` once expanded snapshots are authoritative.

---

## S12. Consolidate Summary/Insight/Diagnostics Typed Assembly
### Goal
Unify summary and insight payload assembly around typed contracts to eliminate parallel dict-shaping logic across multilang summary, diagnostics artifacts, and front-door insight serialization.

### Representative Code Snippets
```python
# tools/cq/core/summary_contracts.py
from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.structs import CqOutputStruct
from tools.cq.core.front_door_insight import FrontDoorInsightV1, coerce_front_door_insight


class SummaryEnvelopeV1(CqOutputStruct, frozen=True):
    summary: dict[str, object]
    front_door_insight: FrontDoorInsightV1 | None = None


def coerce_summary_envelope(summary: Mapping[str, object]) -> SummaryEnvelopeV1:
    insight = coerce_front_door_insight(summary.get("front_door_insight"))
    return SummaryEnvelopeV1(summary=dict(summary), front_door_insight=insight)
```

```python
# tools/cq/core/front_door_insight.py
# Replace manual _serialize_* helpers with canonical msgspec->builtins boundary path.
payload = msgspec.to_builtins(insight, order="deterministic", str_keys=True)
```

### Files to Edit
- `tools/cq/core/front_door_insight.py`
- `tools/cq/core/multilang_summary.py`
- `tools/cq/core/multilang_orchestrator.py`
- `tools/cq/core/diagnostics_contracts.py`
- `tools/cq/search/_shared/search_contracts.py`
- `tools/cq/cli_app/result.py`

### New Files to Create
- `tools/cq/core/summary_contracts.py`
- `tests/unit/cq/core/test_summary_contracts.py`

### Legacy Decommission/Delete Scope
- Delete manual front-door serialization helpers from `tools/cq/core/front_door_insight.py`: `_serialize_target`, `_serialize_slice`, `_serialize_risk_counters` once canonical conversion path is used.
- Delete duplicated dict coercion blocks in `tools/cq/core/diagnostics_contracts.py` and `tools/cq/core/multilang_summary.py` after summary envelope adoption.

---

## S13. Canonical Query Resource Path Resolver
### Goal
Introduce one canonical query resource resolver for query packs, query contracts, and diagnostics queries. This removes current path divergence between registry/lint/diagnostics modules and eliminates silent no-pack states.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/query/resource_paths.py
from __future__ import annotations

from pathlib import Path

_QUERY_ROOT = Path(__file__).resolve().parents[3] / "queries"


def query_pack_dir(language: str) -> Path:
    return _QUERY_ROOT / language


def query_pack_path(language: str, pack_name: str) -> Path:
    return query_pack_dir(language) / pack_name


def query_contracts_path(language: str) -> Path:
    return query_pack_dir(language) / "contracts.yaml"


def diagnostics_query_path(language: str) -> Path:
    return query_pack_dir(language) / "95_diagnostics.scm"
```

```python
# tools/cq/search/tree_sitter/diagnostics/collector.py
from tools.cq.search.tree_sitter.query.resource_paths import diagnostics_query_path

path = diagnostics_query_path(language)
```

### Files to Edit
- `tools/cq/search/tree_sitter/query/registry.py`
- `tools/cq/search/tree_sitter/diagnostics/collector.py`
- `tools/cq/search/tree_sitter/contracts/query_models.py`
- `tools/cq/search/tree_sitter/query/lint.py`

### New Files to Create
- `tools/cq/search/tree_sitter/query/resource_paths.py`
- `tests/unit/cq/search/tree_sitter/test_query_resource_paths.py`

### Legacy Decommission/Delete Scope
- Delete local `__file__`-relative query path builders in `tools/cq/search/tree_sitter/query/registry.py`.
- Delete local diagnostics query path assembly in `tools/cq/search/tree_sitter/diagnostics/collector.py`.

---

## S14. Decouple Query Registry Cache Plumbing from Import-Time Side Effects
### Goal
Remove import-time coupling between query registry and cache backend initialization to eliminate circular import fragility and make query registry independently importable/testable.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/query/cache_adapter.py
from __future__ import annotations

from pathlib import Path


def query_registry_cache():
    # Lazy import to avoid module import cycles at import time.
    from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend

    backend = get_cq_cache_backend(root=Path.cwd())
    return getattr(backend, "cache", None)
```

```python
# tools/cq/search/tree_sitter/query/registry.py
from tools.cq.search.tree_sitter.query.cache_adapter import query_registry_cache

cache = query_registry_cache()
```

### Files to Edit
- `tools/cq/search/tree_sitter/query/registry.py`
- `tools/cq/search/tree_sitter/rust_lane/bundle.py`
- `tools/cq/search/tree_sitter/query/lint.py`

### New Files to Create
- `tools/cq/search/tree_sitter/query/cache_adapter.py`
- `tests/unit/cq/search/tree_sitter/test_query_registry_cache_adapter.py`

### Legacy Decommission/Delete Scope
- Delete direct cache-backend import at module top level in `tools/cq/search/tree_sitter/query/registry.py`.
- Delete fallback branches that compensate for registry import failures after lazy adapter cutover.

---

## S15. Shared Query Pack Executor Across Python/Rust Lanes
### Goal
Extract duplicated query-pack load/compile/execute loops into one shared executor for tree-sitter lanes, including telemetry, query-hit export, and match-row projection.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/core/query_pack_executor.py
from __future__ import annotations

from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_captures, run_bounded_query_matches
from tools.cq.search.tree_sitter.structural.match_rows import build_match_rows_with_query_hits


def execute_pack_rows(*, query, root, source_bytes, windows, settings, callbacks):
    captures, capture_telemetry = run_bounded_query_captures(
        query, root, windows=windows, settings=settings, callbacks=callbacks
    )
    matches, match_telemetry = run_bounded_query_matches(
        query, root, windows=windows, settings=settings, callbacks=callbacks
    )
    rows, hits = build_match_rows_with_query_hits(
        query=query, matches=matches, source_bytes=source_bytes, query_name="<pack>"
    )
    return captures, rows, hits, capture_telemetry, match_telemetry
```

### Files to Edit
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/query/planner.py`

### New Files to Create
- `tools/cq/search/tree_sitter/core/query_pack_executor.py`
- `tests/unit/cq/search/tree_sitter/test_query_pack_executor.py`

### Legacy Decommission/Delete Scope
- Delete duplicated `_pack_source_rows` + `_pack_sources` execution loops in `tools/cq/search/tree_sitter/python_lane/runtime.py`, `tools/cq/search/tree_sitter/python_lane/facts.py`, and `tools/cq/search/tree_sitter/rust_lane/runtime.py`.
- Delete duplicated capture/match telemetry merge blocks once shared executor is adopted.

---

## S16. Canonical Tree-Sitter Enrichment Payload Contracts
### Goal
Define typed lane payload contracts for Python and Rust tree-sitter enrichment output to enforce canonical keys (`cst_diagnostics`, `cst_query_hits`, runtime telemetry) and remove compatibility aliasing.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/contracts/lane_payloads.py
from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct


class PythonTreeSitterPayloadV1(CqOutputStruct, frozen=True):
    language: str = "python"
    enrichment_status: str = "applied"
    cst_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    cst_query_hits: list[dict[str, object]] = msgspec.field(default_factory=list)
```

```python
# tools/cq/search/tree_sitter/python_lane/runtime.py
payload["cst_diagnostics"] = [msgspec.to_builtins(row) for row in diagnostics]
payload.pop("tree_sitter_diagnostics", None)
```

### Files to Edit
- `tools/cq/search/tree_sitter/contracts/core_models.py`
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/semantic/models.py`
- `tools/cq/search/rust/evidence.py`
- `tools/cq/neighborhood/tree_sitter_contracts.py`

### New Files to Create
- `tools/cq/search/tree_sitter/contracts/lane_payloads.py`
- `tests/unit/cq/search/tree_sitter/test_lane_payload_contracts.py`

### Legacy Decommission/Delete Scope
- Delete fallback reads of legacy `tree_sitter_diagnostics` keys in consumers after all emitters switch to `cst_diagnostics`.
- Delete ad hoc mixed payload key emission paths in Python and Rust lane runtime modules.

---

## S17. Msgspec YAML/TOML-First Contract Decoding
### Goal
Replace handwritten config parsing for query contracts and run plans with typed msgspec decoding (`msgspec.yaml`, `msgspec.toml`) to reduce parser drift and improve strict validation.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/contracts/query_models.py
import msgspec

from tools.cq.search.tree_sitter.query.resource_paths import query_contracts_path


class QueryPackContractsFileV1(msgspec.Struct, frozen=True):
    version: int = 1
    rules: QueryPackRulesV1 = QueryPackRulesV1()


def load_pack_rules(language: str) -> QueryPackRulesV1:
    path = query_contracts_path(language)
    payload = msgspec.yaml.decode(path.read_bytes(), type=QueryPackContractsFileV1, strict=True)
    return payload.rules
```

```python
# tools/cq/run/loader.py
plan_payload = msgspec.toml.decode(path.read_bytes(), type=RunPlan, strict=True)
```

### Files to Edit
- `tools/cq/search/tree_sitter/contracts/query_models.py`
- `tools/cq/run/loader.py`
- `tools/cq/search/tree_sitter/query/lint.py`

### New Files to Create
- `tests/unit/cq/search/tree_sitter/test_query_contracts_yaml_decode.py`
- `tests/unit/cq/run/test_run_loader_msgspec_toml.py`

### Legacy Decommission/Delete Scope
- Delete manual line-based `contracts.yaml` parser logic in `tools/cq/search/tree_sitter/contracts/query_models.py`.
- Delete `tomllib` + untyped-to-convert load path in `tools/cq/run/loader.py` after msgspec TOML decode adoption.

---

## S18. Boundary Conversion Policy and Error Taxonomy Hardening
### Goal
Complete migration to shared typed boundary adapters and normalized decode errors across CQ boundaries, including strict/lax conversion policy declaration at each boundary.

### Representative Code Snippets
```python
# tools/cq/core/typed_boundary.py
from __future__ import annotations

import msgspec


class BoundaryDecodeError(RuntimeError):
    pass


def convert_strict[T](payload: object, *, type_: type[T]) -> T:
    try:
        return msgspec.convert(payload, type=type_, strict=True)
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError) as exc:
        raise BoundaryDecodeError(str(exc)) from exc
```

```python
# tools/cq/search/rust/evidence.py
from tools.cq.core.typed_boundary import convert_lax

row = convert_lax(item, type_=RustMacroExpansionResultV1)
```

### Files to Edit
- `tools/cq/core/typed_boundary.py`
- `tools/cq/search/_shared/core.py`
- `tools/cq/run/loader.py`
- `tools/cq/search/rust/evidence.py`
- `tools/cq/search/rust/enrichment.py`
- `tools/cq/ldmd/writer.py`
- `tools/cq/core/report.py`
- `tools/cq/core/snb_registry.py`

### New Files to Create
- `tests/unit/cq/core/test_typed_boundary_policy_matrix.py`

### Legacy Decommission/Delete Scope
- Delete repeated per-module `msgspec.convert(...)/except` blocks that duplicate boundary behavior.
- Delete ad hoc mixed strictness conversion helpers in modules that migrate to shared boundary adapters.

---

## S19. Unified Typed Cache Codec Layer + Allocation Controls
### Goal
Finalize one typed cache codec layer for all cache stores, then apply allocation controls (`encode_into`) in high-volume paths with deterministic behavior.

### Representative Code Snippets
```python
# tools/cq/core/cache/typed_codecs.py
from __future__ import annotations

from functools import lru_cache

import msgspec

_MSGPACK_ENCODER = msgspec.msgpack.Encoder()


@lru_cache(maxsize=64)
def decoder_for[T](*, type_: type[T]) -> msgspec.msgpack.Decoder[T]:
    return msgspec.msgpack.Decoder(type=type_)


def encode_payload_into(value: object, *, buffer: bytearray) -> memoryview:
    _MSGPACK_ENCODER.encode_into(value, buffer)
    return memoryview(buffer)
```

```python
# tools/cq/core/cache/tree_sitter_cache_store.py
from tools.cq.core.cache.typed_codecs import decode_payload, encode_payload
```

### Files to Edit
- `tools/cq/core/cache/fragment_codecs.py`
- `tools/cq/core/cache/tree_sitter_cache_store.py`
- `tools/cq/core/cache/search_artifact_store.py`
- `tools/cq/core/cache/fragment_engine.py`

### New Files to Create
- `tools/cq/core/cache/typed_codecs.py`
- `tests/unit/cq/core/cache/test_typed_codecs.py`
- `tests/unit/cq/core/cache/test_typed_codecs_encode_into.py`

### Legacy Decommission/Delete Scope
- Delete module-local `_ENCODER` and duplicated `_DECODER` definitions in cache stores once shared typed codecs are canonical.
- Delete per-store dict/bytes fallback decode branches superseded by shared typed codec helpers.

---

## S20. Macro Request, Target Resolution, and Scoring Dedup Completion
### Goal
Complete macro-layer dedup by centralizing request base contracts, target file resolution, and score-details construction across macro modules.

### Representative Code Snippets
```python
# tools/cq/macros/common/contracts.py
from __future__ import annotations

from pathlib import Path

from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain


class MacroRequestBase(CqStruct, frozen=True):
    tc: Toolchain
    root: Path
    argv: list[str]
```

```python
# tools/cq/macros/common/scoring.py
from __future__ import annotations

from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    build_detail_payload,
    build_score_details,
)


def macro_score_payload(*, sites: int, files: int, depth: int, evidence_kind: str):
    score = build_score_details(
        impact=ImpactSignals(sites=sites, files=files, depth=depth),
        confidence=ConfidenceSignals(evidence_kind=evidence_kind),
    )
    return build_detail_payload(score=score)
```

### Files to Edit
- `tools/cq/macros/imports.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/macros/bytecode.py`
- `tools/cq/macros/scope_filters.py`

### New Files to Create
- `tools/cq/macros/common/contracts.py`
- `tools/cq/macros/common/targets.py`
- `tools/cq/macros/common/scoring.py`
- `tests/unit/cq/macros/test_common_contracts.py`
- `tests/unit/cq/macros/test_common_targets.py`
- `tests/unit/cq/macros/test_common_scoring.py`

### Legacy Decommission/Delete Scope
- Delete macro-local scoring dict builders (for example `_build_imports_scoring`, `_build_exception_scoring`) once shared scoring helpers are adopted.
- Delete duplicate `_iter_search_files`/`_resolve_target_files` logic in `tools/cq/macros/scopes.py` and `tools/cq/macros/bytecode.py` once shared target helpers are adopted.
- Delete repeated macro request base fields in per-macro request structs once common base contracts are introduced.

---

## 6. Cross-Scope Legacy Decommission and Deletion Plan
### Batch D0 (after S0, S13)
- Delete incorrect local query directory builders from `tools/cq/search/tree_sitter/query/registry.py` and diagnostics query path construction in `tools/cq/search/tree_sitter/diagnostics/collector.py`.
- Delete compatibility-only diagnostics key alias reads once canonical `cst_diagnostics` emission is complete.

### Batch D1 (after S1, S2, S18)
- Delete overlapping conversion entrypoints in `tools/cq/core/serialization.py` and `tools/cq/core/public_serialization.py` because canonical codec and boundary adapters are authoritative.
- Delete repeated module-local conversion/coercion catch blocks superseded by shared boundary adapters.

### Batch D2 (after S3, S4, S20)
- Delete `tools/cq/cli_app/step_types.py` and remove all references to `RunStepCli` from `tools/cq/cli_app/params.py`.
- Delete per-macro request-base duplication once common macro contract base is adopted.

### Batch D3 (after S5, S6, S20)
- Delete macro-local scan/target/scoring duplicates in:
  - `tools/cq/macros/bytecode.py`
  - `tools/cq/macros/scopes.py`
  - `tools/cq/macros/imports.py`
  - `tools/cq/macros/exceptions.py`
  - `tools/cq/macros/side_effects.py`
- Delete redundant scoring-dict helper functions once shared macro scoring helpers are authoritative.

### Batch D4 (after S7, S13, S15)
- Delete duplicated tree-sitter language/query/text helpers across neighborhood and lane modules once shared runtime/query executor utilities are adopted.
- Delete lane-local duplicated query-pack execution loops superseded by shared query pack executor.

### Batch D5 (after S8, S19)
- Delete module-specific cache decode fallback branches in `tools/cq/core/cache/tree_sitter_cache_store.py` and `tools/cq/core/cache/search_artifact_store.py` once typed cache codecs are canonical.
- Delete module-local msgpack encoder/decoder singletons replaced by shared typed cache codecs.

### Batch D6 (after S11, S16, S17)
- Delete legacy schema-by-convention assumptions and partial schema coverage after expanded msgspec schema snapshots become authoritative.
- Delete compatibility readers for deprecated tree-sitter payload keys once typed lane payload contracts are canonical.

### Batch D7 (after S9, S10, S12)
- Delete permissive output contract usage for public persisted payload paths in favor of strict output boundary structs.
- Delete manual summary/insight dict-shaping code paths made obsolete by typed summary envelope and schema-driven serialization.

## 7. Implementation Sequence
1. S0: stabilization gate first (path correctness + canonical diagnostics key) to eliminate silent regressions before further refactors.
2. S13: centralize query resource path resolution to remove path drift root cause for registry/lint/diagnostics.
3. S14: decouple query registry cache plumbing from import-time side effects to remove circular import fragility.
4. S17: migrate contracts/config decode to msgspec YAML/TOML early to stabilize strict parsing behavior for subsequent work.
5. S7: complete tree-sitter runtime helper adoption/deletion on top of already landed uplift modules.
6. S15: extract shared query-pack executor after path/runtime foundations are stable.
7. S16: enforce canonical typed lane payload contracts and remove key aliasing.
8. S1: converge contract codec/serialization boundaries once tree-sitter payload contract surface is stable.
9. S2: migrate remaining typed boundary adapters.
10. S18: finish boundary policy/error taxonomy hardening and remove duplicated catch/convert patterns.
11. S8: introduce unified typed cache codec layer.
12. S19: apply cache codec finalization and allocation controls (`encode_into`) after common layer exists.
13. S3: unify CLI contracts and step unions.
14. S4: standardize macro request entry signatures.
15. S5: centralize macro scan/target helpers.
16. S6: centralize macro scoring payload construction.
17. S20: complete macro dedup end-state (requests/targets/scoring).
18. S9: tighten strict output boundary policies.
19. S10: expand `Meta` and `UNSET` usage once strict policy boundaries are explicit.
20. S11: finalize schema export + drift guardrails with expanded contract inventory.
21. S12: complete summary/insight/diagnostics typed assembly.
22. D0-D7 decommission batches in order, each only after prerequisites are complete.

## 8. Implementation Checklist
- [ ] S0. Stabilization and Correctness Gate
- [ ] S1. Canonical Contract Codec and Serialization Boundary
- [ ] S2. Typed Boundary Adapter Layer for Coercion and Error Taxonomy
- [ ] S3. CLI Contract Unification with Tagged Unions
- [ ] S4. Macro Request Base Contracts and Entry Signature Normalization
- [ ] S5. Shared Macro Scan and Target Resolution Helpers
- [ ] S6. Shared Macro Scoring Pipeline
- [ ] S7. Tree-Sitter Runtime Utility Consolidation
- [ ] S8. Unified Typed Cache Codec Layer
- [ ] S9. Strict Output Contract Policy for External/Public Boundaries
- [ ] S10. Expand Msgspec `Meta` Constraints and `UNSET` Usage
- [ ] S11. Expand CQ Schema Export + Contract Drift Guardrails
- [ ] S12. Consolidate Summary/Insight/Diagnostics Typed Assembly
- [ ] S13. Canonical Query Resource Path Resolver
- [ ] S14. Decouple Query Registry Cache Plumbing from Import-Time Side Effects
- [ ] S15. Shared Query Pack Executor Across Python/Rust Lanes
- [ ] S16. Canonical Tree-Sitter Enrichment Payload Contracts
- [ ] S17. Msgspec YAML/TOML-First Contract Decoding
- [ ] S18. Boundary Conversion Policy and Error Taxonomy Hardening
- [ ] S19. Unified Typed Cache Codec Layer + Allocation Controls
- [ ] S20. Macro Request, Target Resolution, and Scoring Dedup Completion
- [ ] D0. Cross-scope deletion batch after S0+S13
- [ ] D1. Cross-scope deletion batch after S1+S2+S18
- [ ] D2. Cross-scope deletion batch after S3+S4+S20
- [ ] D3. Cross-scope deletion batch after S5+S6+S20
- [ ] D4. Cross-scope deletion batch after S7+S13+S15
- [ ] D5. Cross-scope deletion batch after S8+S19
- [ ] D6. Cross-scope deletion batch after S11+S16+S17
- [ ] D7. Cross-scope deletion batch after S9+S10+S12
