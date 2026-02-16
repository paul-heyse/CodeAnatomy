# tools/cq Design Improvements Implementation Plan v1 (2026-02-16)

## Scope Summary

This plan updates the original 12-scope draft to incorporate all agreed corrections and scope expansions from the 2026-02-16 design reviews. It now contains **20 scope items** that cover contract typing, DRY consolidation, dependency inversion fixes, cache policy unification, missing type-safety hardening, and decomposition tranches for high-risk mega-modules.

**Design stance:** hard cutover, design-phase migration, no compatibility shims.
- No re-export bridges for moved types.
- No deprecated dual APIs retained in parallel.
- No untyped overflow bags in typed contracts.
- Callers migrate in the same PR where the surface changes.

**Out of scope (explicitly deferred):**
- Full breakup of `tools/cq/query/executor.py` into separate module families beyond the scoped refactors in S8/S11/S16.
- One-shot replacement of all process-wide singletons (`_BACKEND_STATE`, `_SCHEDULER_STATE`, `_RENDER_ENRICHMENT_PORT_STATE`, `_RUNTIME_SERVICES`) with a unified lifecycle manager.
- New language-lane support beyond Python and Rust.

## Design Principles

1. **Hard cutover contracts**: when a contract changes, all callers are migrated and old paths are deleted in the same implementation sequence.
2. **Single source of truth**: constants, kind registries, error boundaries, and policy defaults have one canonical home.
3. **Typed boundaries first**: remove untyped `dict[str, object]` communication channels where fields are known.
4. **Dependency direction is strict**: core/library modules must not depend on CLI or search-adapter internals.
5. **Inject, do not resolve inline**: runtime services and cache/runtime dependencies are composed once and threaded through context.
6. **Library-native implementation**: prefer `msgspec`/`diskcache`/`tree-sitter` built-ins over bespoke patterns.
7. **Compositional decomposition**: split mega-modules by responsibility boundaries, not by arbitrary file size.
8. **Verified file manifests**: every file in `Files to Edit` exists now; every new module has a corresponding test file.

## Current Baseline

- `CqResult.summary` is `dict[str, object]` in `tools/cq/core/schema.py:356`; `result.summary[...]` / `result.summary.get(...)` is used in 22 files.
- `tools/cq/query/ir.py:631-744` has 4 manual `with_*` copy methods on a frozen `msgspec.Struct`.
- `_FRONT_DOOR_PREVIEW_PER_SLICE` is duplicated in 4 files and `_CALLS_TARGET_CALLEE_PREVIEW` in 2 files (`tools/cq/macros/calls/entry.py:69-70`, `tools/cq/macros/calls/insight.py:34`, `tools/cq/macros/calls/neighborhood.py:22`, `tools/cq/macros/calls/semantic.py:19`, `tools/cq/macros/calls_target.py:29`).
- `ENRICHMENT_ERRORS` variants are duplicated in 4 locations (`tools/cq/search/tree_sitter/core/lane_support.py:20`, `tools/cq/search/python/extractors.py:119`, `tools/cq/search/rust/enrichment.py:40`, `tools/cq/search/pipeline/smart_search.py:165`).
- Entity-kind sets are independently redefined in `tools/cq/query/enrichment.py:277-278`, `tools/cq/query/finding_builders.py:300-308`, and `tools/cq/query/executor_definitions.py:307-349`.
- `resolve_runtime_services(...)` is called inline in executors/step handlers across query/run/orchestration paths.
- `RunExecutionContext` is a **Protocol** at `tools/cq/core/run_context.py:74`, not a dataclass.
- Neighborhood orchestration is duplicated between `tools/cq/cli_app/commands/neighborhood.py:56-115` and `tools/cq/run/step_executors.py:353-438`.
- `tools/cq/ldmd/format.py:9` imports `LdmdSliceMode` from `tools/cq/cli_app/types.py` (dependency inversion).
- Rust extraction logic and scope constants are duplicated across ast-grep and tree-sitter lanes (`tools/cq/search/rust/enrichment.py`, `tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py`, `tools/cq/search/tree_sitter/rust_lane/runtime.py`, `tools/cq/neighborhood/tree_sitter_collector.py`).
- Fragment cache orchestration is duplicated in `tools/cq/query/executor.py:330-406` and `tools/cq/query/executor_ast_grep.py:158-274`.
- `tools/cq/search/pipeline/classifier_runtime.py:65-70` keeps 6 module-level mutable cache dicts with manual global clear.
- Cache defaults are duplicated between `tools/cq/core/runtime/execution_policy.py` and `tools/cq/core/cache/policy.py`.
- `tools/cq/core/contract_codec.py:133-157` exposes alias API trio (`dumps_json_value`, `loads_json_value`, `loads_json_result`) and `tools/cq/cli_app/contracts.py` is an empty placeholder module.
- `tools/cq/core/cache/contracts.py` imports search/astgrep-specific types (`RecordType`, `TreeSitterArtifactBundleV1`) causing core-to-search leakage.
- `tools/cq/query/executor_ast_grep.py` uses raw match payload `dict[str, object]` instead of a typed contract.
- `tools/cq/search/pipeline/smart_search.py:1215` uses `build_summary(*args, **kwargs)` legacy coercion and `tools/cq/search/pipeline/partition_pipeline.py` still has `Any`-typed enrichment payload plumbing.
- `tools/cq/search/tree_sitter/core/runtime.py` uses `getattr(node, ...)` defensive access despite `NodeLike` protocol availability; `QueryWindowV1` lacks `start_byte <= end_byte` invariant enforcement.
- `tools/cq/search/pipeline/smart_search.py` remains partially decomposed with wrapper modules importing back into the monolith.
- `tools/cq/core/front_door_builders.py` and `tools/cq/core/report.py` remain responsibility-heavy modules; front-door serialization is partially manual despite `msgspec.to_builtins(..., order="deterministic")` support.

---

## S1. Typed Summary Contract (Hard Cutover)

### Goal

Replace `CqResult.summary: dict[str, object]` with a typed `CqSummary` contract and migrate all current summary readers/writers in one cutover. No `extra` overflow field is retained.

### Representative Code Snippets

```python
# tools/cq/core/summary_contract.py

from __future__ import annotations

import msgspec


class SemanticTelemetryV1(msgspec.Struct, frozen=True, omit_defaults=True):
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    skipped: int = 0
    timed_out: int = 0


class CqSummary(msgspec.Struct, omit_defaults=True):
    matches: int = 0
    files_scanned: int = 0
    query_text: str | None = None
    lang: str | None = None
    mode: str | None = None
    plan_version: int | None = None
    file_filters: list[str] = msgspec.field(default_factory=list)
    python_semantic_telemetry: SemanticTelemetryV1 | None = None
    rust_semantic_telemetry: SemanticTelemetryV1 | None = None
    semantic_planes: dict[str, object] = msgspec.field(default_factory=dict)
    cache_backend: dict[str, object] = msgspec.field(default_factory=dict)
    front_door_insight: dict[str, object] | None = None
    # ...remaining known fields explicitly declared, no overflow bag...
```

```python
# tools/cq/core/schema.py
from tools.cq.core.summary_contract import CqSummary


class CqResult(msgspec.Struct):
    run: RunMeta
    summary: CqSummary = msgspec.field(default_factory=CqSummary)
    key_findings: list[Finding] = msgspec.field(default_factory=list)
    evidence: list[Finding] = msgspec.field(default_factory=list)
    sections: list[Section] = msgspec.field(default_factory=list)
    artifacts: list[Artifact] = msgspec.field(default_factory=list)
```

### Files to Edit

- `tools/cq/core/schema.py`
- `tools/cq/core/merge.py`
- `tools/cq/core/render_summary.py`
- `tools/cq/core/result_factory.py`
- `tools/cq/core/artifacts.py`
- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/cli_app/result.py`
- `tools/cq/ldmd/writer.py`
- `tools/cq/macros/result_builder.py`
- `tools/cq/macros/calls/entry.py`
- `tools/cq/macros/calls/semantic.py`
- `tools/cq/macros/calls_target.py`
- `tools/cq/neighborhood/snb_renderer.py`
- `tools/cq/orchestration/bundles.py`
- `tools/cq/orchestration/multilang_orchestrator.py`
- `tools/cq/orchestration/multilang_summary.py`
- `tools/cq/query/executor.py`
- `tools/cq/query/entity_front_door.py`
- `tools/cq/query/executor_definitions.py`
- `tools/cq/query/merge.py`
- `tools/cq/query/shared_utils.py`
- `tools/cq/run/q_execution.py`
- `tools/cq/run/q_step_collapsing.py`
- `tools/cq/run/run_summary.py`
- `tools/cq/run/runner.py`
- `tools/cq/run/step_executors.py`
- `tools/cq/search/_shared/search_contracts.py`
- `tools/cq/search/pipeline/assembly.py`
- `tools/cq/search/pipeline/search_semantic.py`
- `tools/cq/search/pipeline/smart_search.py`

### New Files to Create

- `tools/cq/core/summary_contract.py`
- `tests/unit/cq/core/test_summary_contract.py`
- `tests/unit/cq/run/test_summary_contract_integration.py`

### Legacy Decommission/Delete Scope

- Delete `summary: dict[str, object]` in `tools/cq/core/schema.py:356`.
- Delete dict-key summary mutation/read patterns (`result.summary[...]`, `result.summary.get(...)`) across all migrated files.
- Delete `MacroResultBuilder.set_summary_mapping(self, summary: dict[str, object])` dict-based replacement semantics in `tools/cq/macros/result_builder.py` and replace with typed assignment helpers.

---

## S2. Canonical Entity Kind Registry

### Goal

Define canonical entity-kind sets in one immutable registry and remove all local redefinitions.

### Representative Code Snippets

```python
# tools/cq/core/entity_kinds.py

from __future__ import annotations

import msgspec


class EntityKindRegistry(msgspec.Struct, frozen=True):
    function_kinds: frozenset[str] = frozenset({"function", "async_function", "function_typeparams"})
    class_kinds: frozenset[str] = frozenset(
        {"class", "class_bases", "class_typeparams", "class_typeparams_bases", "struct", "enum", "trait"}
    )
    import_kinds: frozenset[str] = frozenset(
        {"import", "import_as", "from_import", "from_import_as", "from_import_multi", "from_import_paren", "use_declaration"}
    )

    @property
    def decorator_kinds(self) -> frozenset[str]:
        return self.function_kinds | self.class_kinds


ENTITY_KINDS = EntityKindRegistry()
```

### Files to Edit

- `tools/cq/query/executor_definitions.py`
- `tools/cq/query/enrichment.py`
- `tools/cq/query/finding_builders.py`

### New Files to Create

- `tools/cq/core/entity_kinds.py`
- `tests/unit/cq/core/test_entity_kinds.py`

### Legacy Decommission/Delete Scope

- Delete local `function_kinds`/`class_kinds`/`import_kinds` sets from the three query modules above.

---

## S3. Consolidate Enrichment Error Boundaries

### Goal

Use one canonical enrichment error tuple and remove local tuple drift.

### Representative Code Snippets

```python
# tools/cq/search/_shared/error_boundaries.py

from __future__ import annotations

ENRICHMENT_ERRORS: tuple[type[Exception], ...] = (
    RuntimeError,
    TypeError,
    ValueError,
    AttributeError,
    UnicodeError,
    IndexError,
    KeyError,
    OSError,
)

__all__ = ["ENRICHMENT_ERRORS"]
```

### Files to Edit

- `tools/cq/search/tree_sitter/core/lane_support.py`
- `tools/cq/search/python/extractors.py`
- `tools/cq/search/rust/enrichment.py`
- `tools/cq/search/pipeline/smart_search.py`

### New Files to Create

- `tools/cq/search/_shared/error_boundaries.py`
- `tests/unit/cq/search/test_error_boundaries.py`

### Legacy Decommission/Delete Scope

- Delete `_ENRICHMENT_ERRORS` / `_RUST_ENRICHMENT_ERRORS` local tuple definitions from all four modules.

---

## S4. Consolidate Duplicated Macro Constants

### Goal

Move duplicated preview-limit constants to one macros constants module.

### Representative Code Snippets

```python
# tools/cq/macros/constants.py

from __future__ import annotations

FRONT_DOOR_PREVIEW_PER_SLICE: int = 5
CALLS_TARGET_CALLEE_PREVIEW: int = 10

__all__ = ["CALLS_TARGET_CALLEE_PREVIEW", "FRONT_DOOR_PREVIEW_PER_SLICE"]
```

### Files to Edit

- `tools/cq/macros/calls/entry.py`
- `tools/cq/macros/calls/neighborhood.py`
- `tools/cq/macros/calls/insight.py`
- `tools/cq/macros/calls/semantic.py`
- `tools/cq/macros/calls_target.py`

### New Files to Create

- `tools/cq/macros/constants.py`
- `tests/unit/cq/macros/test_constants.py`

### Legacy Decommission/Delete Scope

- Delete `_FRONT_DOOR_PREVIEW_PER_SLICE` and `_CALLS_TARGET_CALLEE_PREVIEW` duplicates from all listed modules.

---

## S5. Replace `Query.with_*` Boilerplate with `msgspec.structs.replace()`

### Goal

Use `msgspec.structs.replace()` for all `Query.with_*` methods to remove manual field-copy maintenance.

### Representative Code Snippets

```python
# tools/cq/query/ir.py

def with_scope(self, scope: Scope) -> Query:
    return msgspec.structs.replace(self, scope=scope)

def with_expand(self, *expanders: Expander) -> Query:
    return msgspec.structs.replace(self, expand=self.expand + expanders)
```

### Files to Edit

- `tools/cq/query/ir.py`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete manual field-copying method bodies in `tools/cq/query/ir.py:631-744`.

---

## S6. Consolidate Semantic Telemetry Construction

### Goal

Replace all inline telemetry dict assembly with shared typed builder usage.

### Representative Code Snippets

```python
# tools/cq/core/summary_contract.py

def build_semantic_telemetry(
    *,
    attempted: int,
    applied: int,
    failed: int,
    skipped: int = 0,
    timed_out: int = 0,
) -> SemanticTelemetryV1:
    return SemanticTelemetryV1(
        attempted=attempted,
        applied=applied,
        failed=max(failed, attempted - applied),
        skipped=skipped,
        timed_out=timed_out,
    )
```

### Files to Edit

- `tools/cq/query/entity_front_door.py`
- `tools/cq/macros/calls/entry.py`
- `tools/cq/search/pipeline/search_semantic.py`
- `tools/cq/run/run_summary.py`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete inline `{"attempted": ..., "applied": ..., "failed": ...}` dict construction patterns in all listed files.

---

## S7. Extract Shared Neighborhood Executor

### Goal

Remove duplicated neighborhood orchestration by delegating both CLI and run-step paths to one shared executor.

### Representative Code Snippets

```python
# tools/cq/neighborhood/executor.py

def execute_neighborhood(..., services: CqRuntimeServices, ...) -> CqResult:
    # parse target -> resolve -> build bundle -> render result -> finalize IDs/cache
    ...
```

### Files to Edit

- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/run/step_executors.py`

### New Files to Create

- `tools/cq/neighborhood/executor.py`
- `tests/unit/cq/neighborhood/test_executor.py`

### Legacy Decommission/Delete Scope

- Delete inline neighborhood orchestration bodies from `tools/cq/cli_app/commands/neighborhood.py` and `tools/cq/run/step_executors.py`.

---

## S8. Thread Runtime Services via Context Protocol

### Goal

Stop resolving runtime services inside executors. Resolve once at composition root and thread through context protocols/structs.

### Representative Code Snippets

```python
# tools/cq/core/run_context.py

class RunExecutionContext(Protocol):
    @property
    def services(self) -> CqRuntimeServices:
        ...
```

```python
# tools/cq/cli_app/context.py

class CliContext(CqStruct, frozen=True):
    ...
    services: CqRuntimeServices

@classmethod
def from_options(cls, ...) -> CliContext:
    services = resolve_runtime_services(root)
    return cls(..., services=services)
```

### Files to Edit

- `tools/cq/core/run_context.py`
- `tools/cq/cli_app/context.py`
- `tools/cq/run/helpers.py`
- `tools/cq/run/runner.py`
- `tools/cq/run/step_executors.py`
- `tools/cq/query/execution_context.py`
- `tools/cq/query/executor.py`
- `tools/cq/query/merge.py`
- `tools/cq/orchestration/bundles.py`
- `tools/cq/cli_app/commands/search.py`
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/analysis.py`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete inline `resolve_runtime_services(...)` usage from executor and step-executor bodies.

---

## S9. Move `LdmdSliceMode` to Core Types (No Re-export)

### Goal

Fix dependency inversion by relocating `LdmdSliceMode` to core shared types and updating all imports directly.

### Representative Code Snippets

```python
# tools/cq/core/types.py

from enum import StrEnum


class LdmdSliceMode(StrEnum):
    full = "full"
    preview = "preview"
    tldr = "tldr"
```

### Files to Edit

- `tools/cq/ldmd/format.py`
- `tools/cq/cli_app/commands/ldmd.py`
- `tools/cq/cli_app/types.py`

### New Files to Create

- `tools/cq/core/types.py`
- `tests/unit/cq/core/test_types.py`

### Legacy Decommission/Delete Scope

- Delete `LdmdSliceMode` definition from `tools/cq/cli_app/types.py` (no re-export bridge).

---

## S10. Unify Rust Extraction Layer (Full)

### Goal

Implement one shared Rust extraction implementation used by both ast-grep and tree-sitter lanes via node-access adapters.

### Representative Code Snippets

```python
# tools/cq/search/rust/node_access.py

from __future__ import annotations

from typing import Protocol


class RustNodeAccess(Protocol):
    def kind(self) -> str: ...
    def text(self) -> str: ...
    def child_by_field_name(self, name: str) -> RustNodeAccess | None: ...
    def children(self) -> list[RustNodeAccess]: ...
```

```python
# tools/cq/search/rust/extractors_shared.py

RUST_SCOPE_KINDS: frozenset[str] = frozenset(
    {"function_item", "impl_item", "trait_item", "mod_item", "block"}
)

def extract_function_signature(node: RustNodeAccess) -> dict[str, object]:
    ...
```

### Files to Edit

- `tools/cq/search/rust/enrichment.py`
- `tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/role_classification.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`

### New Files to Create

- `tools/cq/search/rust/node_access.py`
- `tools/cq/search/rust/extractors_shared.py`
- `tests/unit/cq/search/rust/test_node_access.py`
- `tests/unit/cq/search/rust/test_extractors_shared.py`

### Legacy Decommission/Delete Scope

- Delete duplicated extraction helpers from lane-specific modules once adapters delegate to shared extractors.
- Delete duplicated `_SCOPE_KINDS` / visibility keyword sets in all affected modules.

---

## S11. Extract Fragment Cache Orchestrator

### Goal

Replace duplicated fragment probe/scan/persist orchestration with one shared helper that wraps existing `fragment_engine` primitives.

### Representative Code Snippets

```python
# tools/cq/core/cache/fragment_orchestrator.py

from __future__ import annotations

from collections.abc import Callable

from tools.cq.core.cache.fragment_contracts import FragmentEntryV1, FragmentRequestV1
from tools.cq.core.cache.fragment_engine import FragmentPersistRuntimeV1, FragmentProbeRuntimeV1, partition_fragment_entries, persist_fragment_writes


def run_fragment_scan[T](
    *,
    request: FragmentRequestV1,
    entries: list[FragmentEntryV1],
    probe_runtime: FragmentProbeRuntimeV1,
    persist_runtime: FragmentPersistRuntimeV1,
    scan_misses: Callable[[list[object]], tuple[dict[str, T], list[object]]],
) -> tuple[dict[str, T], list[object]]:
    partition = partition_fragment_entries(request, entries, probe_runtime)
    ...
```

### Files to Edit

- `tools/cq/query/executor.py`
- `tools/cq/query/executor_ast_grep.py`

### New Files to Create

- `tools/cq/core/cache/fragment_orchestrator.py`
- `tests/unit/cq/core/cache/test_fragment_orchestrator.py`

### Legacy Decommission/Delete Scope

- Delete duplicate fragment orchestration blocks from both query executor implementations.

---

## S12. Classifier Runtime Cache Context

### Goal

Encapsulate classifier mutable module-level caches in explicit context and remove global clear semantics.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/classifier_runtime.py

class ClassifierCacheContext:
    __slots__ = (
        "sg_cache",
        "source_cache",
        "def_lines_cache",
        "symtable_cache",
        "record_context_cache",
        "node_index_cache",
    )

    def clear(self) -> None:
        ...
```

### Files to Edit

- `tools/cq/search/pipeline/classifier_runtime.py`
- `tools/cq/search/pipeline/classifier.py`
- `tools/cq/search/pipeline/smart_search.py`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `_sg_cache`, `_source_cache`, `_def_lines_cache`, `_symtable_cache`, `_record_context_cache`, `_node_index_cache` globals.
- Delete global-only clear API usage (`clear_classifier_caches`) from callers.

---

## S13. Consolidate Cache Policy Defaults and Env Resolution

### Goal

Create one cache-default authority consumed by both runtime execution policy and cache policy resolution paths.

### Representative Code Snippets

```python
# tools/cq/core/cache/defaults.py

from __future__ import annotations

from typing import Literal

CacheEvictionPolicy = Literal[
    "least-recently-stored",
    "least-recently-used",
    "least-frequently-used",
    "none",
]

DEFAULT_CACHE_TTL_SECONDS = 900
DEFAULT_CACHE_SHARDS = 8
DEFAULT_CACHE_TIMEOUT_SECONDS = 0.05
DEFAULT_CACHE_SIZE_LIMIT_BYTES = 2_147_483_648
DEFAULT_CACHE_CULL_LIMIT = 16
DEFAULT_CACHE_EVICTION_POLICY: CacheEvictionPolicy = "least-recently-stored"
```

### Files to Edit

- `tools/cq/core/cache/policy.py`
- `tools/cq/core/runtime/execution_policy.py`
- `tools/cq/core/cache/cache_runtime_tuning.py`

### New Files to Create

- `tools/cq/core/cache/defaults.py`
- `tests/unit/cq/core/cache/test_defaults.py`

### Legacy Decommission/Delete Scope

- Delete duplicated cache scalar defaults from `policy.py` and `execution_policy.py`.
- Delete divergent fallback strings for eviction policy defaults.

---

## S14. Contract Surface Cleanup (`contract_codec` + Empty CLI Contracts)

### Goal

Remove redundant alias APIs in `contract_codec` and delete empty placeholder module `cli_app/contracts.py`.

### Representative Code Snippets

```python
# tools/cq/core/contract_codec.py

__all__ = [
    "decode_json",
    "decode_json_result",
    "decode_msgpack",
    "decode_msgpack_result",
    "encode_json",
    "encode_msgpack",
]
```

### Files to Edit

- `tools/cq/core/contract_codec.py`
- `tools/cq/core/artifacts.py`
- `tools/cq/core/render_summary.py`
- `tools/cq/perf/smoke_report.py`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `dumps_json_value`, `loads_json_value`, `loads_json_result` from `tools/cq/core/contract_codec.py`.
- Delete `tools/cq/cli_app/contracts.py`.

---

## S15. Move Search-Dependent Cache Contracts out of `core/cache/contracts.py`

### Goal

Restore dependency direction by moving search/astgrep cache contracts out of core cache contracts.

### Representative Code Snippets

```python
# tools/cq/search/cache/contracts.py

from __future__ import annotations

import msgspec

from tools.cq.core.cache.base_contracts import CqCacheStruct


class SgRecordCacheV1(CqCacheStruct, frozen=True):
    ...


class QueryEntityScanCacheV1(CqCacheStruct, frozen=True):
    records: list[SgRecordCacheV1]
```

### Files to Edit

- `tools/cq/core/cache/contracts.py`
- `tools/cq/core/cache/__init__.py`
- `tools/cq/query/cache_converters.py`
- `tools/cq/query/executor.py`
- `tools/cq/query/executor_ast_grep.py`
- `tools/cq/search/pipeline/partition_pipeline.py`

### New Files to Create

- `tools/cq/search/cache/__init__.py`
- `tools/cq/search/cache/contracts.py`
- `tests/unit/cq/search/cache/test_contracts.py`

### Legacy Decommission/Delete Scope

- Delete search-specific contract definitions and imports from `tools/cq/core/cache/contracts.py`.

---

## S16. Replace Raw Match Dict Payload with Typed Match Contract

### Goal

Replace `dict[str, object]` raw-match payload usage in query ast-grep execution with typed match contract.

### Representative Code Snippets

```python
# tools/cq/query/match_contracts.py

from __future__ import annotations

import msgspec


class MatchRangePoint(msgspec.Struct, frozen=True):
    line: int
    column: int


class MatchRange(msgspec.Struct, frozen=True):
    start: MatchRangePoint
    end: MatchRangePoint


class MatchData(msgspec.Struct, frozen=True):
    file: str
    pattern: str
    range: MatchRange
    message: str
    metavars: dict[str, object] = msgspec.field(default_factory=dict)
```

### Files to Edit

- `tools/cq/query/executor_ast_grep.py`

### New Files to Create

- `tools/cq/query/match_contracts.py`
- `tests/unit/cq/query/test_match_contracts.py`

### Legacy Decommission/Delete Scope

- Delete raw `dict[str, object]` build/parse paths (`build_match_data`, `match_to_finding`, raw sort helpers) once all use `MatchData`.

---

## S17. Search Pipeline Type Hardening (`partition_pipeline` + Summary API)

### Goal

Remove `Any` and varargs/kwargs legacy summary API from search pipeline hot paths.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/smart_search.py

def build_summary(inputs: SearchSummaryInputs) -> dict[str, object]:
    return _build_summary(inputs)
```

```python
# tools/cq/search/pipeline/partition_pipeline.py
class _EnrichmentMissTask:
    items: list[tuple[int, RawMatch, str, str]]

class _EnrichmentMissResult:
    raw: RawMatch
    enriched_match: EnrichedMatch
```

### Files to Edit

- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/search/pipeline/smart_search_summary.py`
- `tools/cq/search/pipeline/partition_pipeline.py`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `_SUMMARY_ARGS_COUNT` and varargs coercion path from `tools/cq/search/pipeline/smart_search.py`.
- Delete `Any`-typed miss/result payload signatures in `tools/cq/search/pipeline/partition_pipeline.py`.

---

## S18. Tree-sitter Runtime Type Hardening and Window Invariants

### Goal

Use `NodeLike` protocol directly in runtime and enforce `QueryWindowV1` construction invariants.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/contracts/core_models.py

class QueryWindowV1(CqStruct, frozen=True):
    start_byte: int
    end_byte: int

    def __post_init__(self) -> None:
        if self.start_byte > self.end_byte:
            msg = "start_byte must be <= end_byte"
            raise ValueError(msg)
```

```python
# tools/cq/search/tree_sitter/core/runtime.py

def _node_bounds(node: NodeLike) -> tuple[int, int]:
    return int(node.start_byte), int(node.end_byte)
```

### Files to Edit

- `tools/cq/search/tree_sitter/contracts/core_models.py`
- `tools/cq/search/tree_sitter/core/runtime.py`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `getattr(node, "start_byte", ...)` / `getattr(node, "end_byte", ...)` patterns from runtime hot path.

---

## S19. Complete `smart_search.py` Decomposition and Introduce `SearchRuntimeContext`

### Goal

Finish partial extraction by moving implementations into dedicated phase modules and reduce `smart_search.py` to orchestration plus composition.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/runtime_context.py

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext


@dataclass(frozen=True)
class SearchRuntimeContext:
    cache_backend: CqCacheBackend
    classifier_cache: ClassifierCacheContext
    python_analysis_session: object
```

```python
# tools/cq/search/pipeline/smart_search.py

def smart_search(request: SearchRequestV1) -> CqResult:
    runtime = build_search_runtime_context(request)
    candidates = run_candidate_phase(request, runtime)
    classified = run_classify_phase(request, runtime, candidates)
    enriched = run_enrichment_phase(request, runtime, classified)
    return assemble_search_result(request, enriched)
```

### Files to Edit

- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/search/pipeline/smart_search_followups.py`
- `tools/cq/search/pipeline/smart_search_sections.py`
- `tools/cq/search/pipeline/smart_search_summary.py`
- `tools/cq/search/pipeline/smart_search_telemetry.py`
- `tools/cq/search/pipeline/classifier.py`

### New Files to Create

- `tools/cq/search/pipeline/runtime_context.py`
- `tools/cq/search/pipeline/candidate_phase.py`
- `tools/cq/search/pipeline/classify_phase.py`
- `tools/cq/search/pipeline/enrichment_phase.py`
- `tests/unit/cq/search/pipeline/test_runtime_context.py`
- `tests/unit/cq/search/pipeline/test_candidate_phase.py`
- `tests/unit/cq/search/pipeline/test_classify_phase.py`
- `tests/unit/cq/search/pipeline/test_enrichment_phase.py`

### Legacy Decommission/Delete Scope

- Delete wrapper modules that only re-export functions from `smart_search.py` once ownership is moved.
- Delete oversized `smart_search.py` export list entries that belong to phase modules.

---

## S20. Decompose Front-Door Builders and Report Orchestration

### Goal

Split `front_door_builders.py` and `report.py` by responsibility boundaries; use `msgspec.to_builtins(..., order="deterministic")` for public insight serialization.

### Representative Code Snippets

```python
# tools/cq/core/front_door_serialization.py

from __future__ import annotations

import msgspec


def to_public_front_door_insight_dict(insight: FrontDoorInsightV1) -> dict[str, object]:
    return msgspec.to_builtins(insight, order="deterministic", str_keys=True)
```

```python
# tools/cq/core/render_enrichment_orchestrator.py

def precompute_render_enrichment(...):
    # multiprocessing and IO orchestration only
    ...
```

### Files to Edit

- `tools/cq/core/front_door_builders.py`
- `tools/cq/core/front_door_render.py`
- `tools/cq/core/front_door_schema.py`
- `tools/cq/core/report.py`
- `tools/cq/core/render_enrichment.py`
- `tools/cq/core/render_utils.py`

### New Files to Create

- `tools/cq/core/front_door_contracts.py`
- `tools/cq/core/front_door_serialization.py`
- `tools/cq/core/render_enrichment_orchestrator.py`
- `tests/unit/cq/core/test_front_door_contracts.py`
- `tests/unit/cq/core/test_front_door_serialization.py`
- `tests/unit/cq/core/test_render_enrichment_orchestrator.py`

### Legacy Decommission/Delete Scope

- Delete manual `_serialize_target`, `_serialize_slice`, `_serialize_risk_counters` helpers from `tools/cq/core/front_door_builders.py`.
- Delete render-enrichment multiprocessing orchestration helpers from `tools/cq/core/report.py` after extraction.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S6)

- Delete all inline semantic telemetry dict constructions in `tools/cq/query/entity_front_door.py`, `tools/cq/macros/calls/entry.py`, `tools/cq/search/pipeline/search_semantic.py`, and `tools/cq/run/run_summary.py`.

### Batch D2 (after S1)

- Delete residual dict-style summary access helpers and adapters that only exist for `dict[str, object]` summary compatibility.

### Batch D3 (after S2, S3, S4)

- Delete all remaining inline definitions of entity-kind sets, enrichment error tuples, and macro preview constants via codebase-wide grep verification.

### Batch D4 (after S8)

- Delete inline runtime-service resolution from executor paths (`query`, `run`, `orchestration`), keeping resolution only at composition roots.

### Batch D5 (after S9)

- Delete `LdmdSliceMode` from `tools/cq/cli_app/types.py` entirely (no alias/re-export remains).

### Batch D6 (after S10, S11)

- Delete duplicate rust extractor helpers and duplicate fragment orchestration blocks once shared modules are adopted.

### Batch D7 (after S14, S15)

- Delete `tools/cq/cli_app/contracts.py`, delete contract-codec alias APIs, and delete search/astgrep contract leakage from `tools/cq/core/cache/contracts.py`.

### Batch D8 (after S17, S19, S20)

- Delete legacy summary varargs API path, smart-search wrapper-only re-export modules, and front-door/report legacy serialization/orchestration helpers.

## Implementation Sequence

1. **S5** — low-risk boilerplate removal, independent.
2. **S4** — constant consolidation, independent.
3. **S3** — error-boundary consolidation, independent.
4. **S2** — entity kind registry, independent.
5. **S13** — normalize cache default constants before deeper cache refactors.
6. **S14** — remove alias/placeholder low-risk surface debt.
7. **S9** — dependency inversion fix for LDMD enum.
8. **S15** — dependency direction cleanup for cache contracts.
9. **S16** — local typed-match contract refactor in query ast-grep path.
10. **S17** — search hot-path type tightening before decomposition.
11. **S18** — tree-sitter runtime type/invariant tightening before lane unification.
12. **S11** — shared fragment orchestrator extraction for query executors.
13. **S12** — classifier cache context explicitization.
14. **S8** — runtime services threading through context protocols and implementations.
15. **S1** — typed summary hard cutover across all readers/writers.
16. **S6** — telemetry builder unification on top of typed summary.
17. **S7** — neighborhood shared executor extraction.
18. **S10** — full rust extraction layer unification.
19. **S19** — complete smart-search decomposition with `SearchRuntimeContext`.
20. **S20** — front-door/report decomposition and serialization consolidation.

**Rationale:** Sequence front-loads low-risk DRY and dependency-direction fixes, then enforces type contracts and orchestration abstractions, then lands broad contract cutovers and decomposition tranches once foundational seams are in place.

## Implementation Checklist

- [ ] S1. Typed Summary Contract (hard cutover, no overflow bag)
- [ ] S2. Canonical Entity Kind Registry
- [ ] S3. Consolidate Enrichment Error Boundaries
- [ ] S4. Consolidate Duplicated Macro Constants
- [ ] S5. Replace `Query.with_*` boilerplate with `msgspec.structs.replace()`
- [ ] S6. Consolidate Semantic Telemetry Construction
- [ ] S7. Extract Shared Neighborhood Executor
- [ ] S8. Thread Runtime Services via Context Protocol
- [ ] S9. Move `LdmdSliceMode` to Core Types (no re-export)
- [ ] S10. Unify Rust Extraction Layer (full)
- [ ] S11. Extract Fragment Cache Orchestrator
- [ ] S12. Classifier Runtime Cache Context
- [ ] S13. Consolidate Cache Policy Defaults and Env Resolution
- [ ] S14. Contract Surface Cleanup (`contract_codec` + empty CLI contracts)
- [ ] S15. Move Search-Dependent Cache Contracts out of `core/cache/contracts.py`
- [ ] S16. Replace raw match dict payload with typed match contract
- [ ] S17. Search Pipeline Type Hardening (`partition_pipeline` + summary API)
- [ ] S18. Tree-sitter Runtime Type Hardening and Window Invariants
- [ ] S19. Complete `smart_search.py` decomposition + `SearchRuntimeContext`
- [ ] S20. Decompose front-door builders and report orchestration
- [ ] D1. Delete inline semantic telemetry dict construction
- [ ] D2. Delete dict-style summary compatibility patterns
- [ ] D3. Delete duplicate kind/error/constant definitions
- [ ] D4. Delete inline runtime-service resolution in executors
- [ ] D5. Delete CLI-owned `LdmdSliceMode`
- [ ] D6. Delete duplicate rust extractors and fragment orchestration blocks
- [ ] D7. Delete contract alias/placeholder/leakage legacy surfaces
- [ ] D8. Delete legacy smart-search/front-door/report compatibility paths
