# CQ Design Review Consolidated Implementation Plan v1 (2026-02-16)

## Scope Summary
This plan integrates all recommended design improvements from:
- `docs/reviews/design_review_synthesis_tools_cq_2026-02-15.md`
- `docs/reviews/design_review_tools_cq_search_pipeline_2026-02-15.md`
- `docs/reviews/design_review_tools_cq_search_tree_sitter_2026-02-15.md`
- `docs/reviews/design_review_tools_cq_search_lanes_2026-02-15.md`
- `docs/reviews/design_review_tools_cq_query_2026-02-15.md`
- `docs/reviews/design_review_tools_cq_core_2026-02-15.md`
- `docs/reviews/design_review_tools_cq_macros_2026-02-15.md`
- `docs/reviews/design_review_tools_cq_cli_execution_2026-02-15.md`

Design stance: hard cutover with breaking changes explicitly allowed. Long-lived compatibility shims are not allowed; short-lived migration seams are permitted only when explicitly scoped, time-bounded to one implementation phase, and tied to a decommission batch in this plan. No recommendation from the reviewed documents is intentionally excluded; recommendations are consolidated into cross-cutting scope items to avoid duplicate migrations.

## Design Principles
1. Hard cutover over transitional compatibility.
2. No upward imports from foundation/lane/core modules into orchestration modules.
3. Typed contracts at boundaries; avoid `dict[str, object]` and bare `str` categoricals for primary system state.
4. Deterministic behavior and idempotent read-only operations are non-negotiable.
5. Maximum module size target: ~800 LOC; larger modules must be decomposed.
6. Public API surfaces must be explicit (`__all__`) and private imports across module boundaries are forbidden.
7. Mutable process-global state must be encapsulated behind injectable abstractions.
8. Every new module ships with focused unit tests; no untested structural extraction.
9. Temporary migration seams must have explicit removal ownership and batch linkage.

## Current Baseline
- God modules remain in place: `tools/cq/search/pipeline/smart_search.py` (3914 LOC), `tools/cq/query/executor.py` (3456 LOC), `tools/cq/macros/calls.py` (2273 LOC), `tools/cq/search/python/extractors.py` (2250 LOC), `tools/cq/search/tree_sitter/rust_lane/runtime.py` (2138 LOC), `tools/cq/core/report.py` (1772 LOC), `tools/cq/run/runner.py` (1297 LOC).
- `QueryMode` and `SearchLimits` are currently defined in `tools/cq/search/pipeline/classifier.py` and `tools/cq/search/pipeline/profiles.py`, then imported upward/downward across unrelated layers.
- Core currently depends on search semantic contracts: `tools/cq/core/front_door_insight.py` imports `SemanticContractStateInputV1`, `SemanticStatus`, and `derive_semantic_contract_state` from `tools/cq/search/semantic/models.py`.
- `CqResult.summary` and cache result summaries are untyped mappings in `tools/cq/core/schema.py` and `tools/cq/core/cache/contracts.py`.
- Query batch modules import private executor internals (`tools/cq/query/batch.py`, `tools/cq/query/batch_spans.py` importing `_build_scan_context`, `_execute_rule_matches`, etc. from `tools/cq/query/executor.py`).
- Mutable module-level caches/tracking remain in multiple places: `tools/cq/search/pipeline/classifier_runtime.py`, `tools/cq/search/python/analysis_session.py`, `tools/cq/search/python/extractors.py`, `tools/cq/search/rust/enrichment.py`.
- `tools/cq/cli_app` command handlers currently use both `@require_ctx` and `require_context()`.

## S1. Foundation Type Lift and Dependency Direction Repair
### Goal
Move vocabulary and semantic-contract types into their correct foundational ownership layers, remove dependency inversions, and establish a stable import graph before larger decompositions. Scope is complete when no module imports `QueryMode`/`SearchLimits` from `search/pipeline/` and no `core/` module imports semantic state contracts from `search/semantic/`.

### Representative Code Snippets
```python
# tools/cq/search/_shared/types.py
from __future__ import annotations

from enum import Enum

from tools.cq.core.contracts_constraints import PositiveFloat, PositiveInt
from tools.cq.core.structs import CqSettingsStruct


class QueryMode(Enum):
    IDENTIFIER = "identifier"
    REGEX = "regex"
    LITERAL = "literal"


class SearchLimits(CqSettingsStruct, frozen=True):
    max_files: PositiveInt = 5000
    max_matches_per_file: PositiveInt = 1000
    max_total_matches: PositiveInt = 10000
    timeout_seconds: PositiveFloat = 30.0
    max_depth: PositiveInt = 25
    max_file_size_bytes: PositiveInt = 2 * 1024 * 1024
    context_before: int = 0
    context_after: int = 0
    multiline: bool = False
    sort_by_path: bool = False
```

```python
# tools/cq/core/semantic_contracts.py
from __future__ import annotations

from typing import Literal

from tools.cq.core.structs import CqStruct

SemanticProvider = Literal["python_static", "rust_static", "none"]
SemanticStatus = Literal["unavailable", "skipped", "failed", "partial", "ok"]


class SemanticContractStateInputV1(CqStruct, frozen=True):
    provider: SemanticProvider
    available: bool
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    reasons: tuple[str, ...] = ()
```

### Files to Edit
- `tools/cq/search/pipeline/classifier.py`
- `tools/cq/search/pipeline/profiles.py`
- `tools/cq/search/pipeline/contracts.py`
- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/search/_shared/core.py`
- `tools/cq/search/rg/adapter.py`
- `tools/cq/search/rg/runner.py`
- `tools/cq/search/rg/prefilter.py`
- `tools/cq/search/rg/collector.py`
- `tools/cq/core/request_factory.py`
- `tools/cq/core/services.py`
- `tools/cq/core/schema_export.py`
- `tools/cq/search/semantic/models.py`
- `tools/cq/search/semantic/__init__.py`
- `tools/cq/core/front_door_insight.py`
- `tools/cq/query/executor.py`
- `tools/cq/query/merge.py`
- `tools/cq/query/entity_front_door.py`
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/cli_app/commands/search.py`
- `tools/cq/run/runner.py`

### New Files to Create
- `tools/cq/search/_shared/types.py`
- `tools/cq/core/semantic_contracts.py`
- `tests/unit/tools/cq/search/_shared/test_types.py`
- `tests/unit/tools/cq/core/test_semantic_contracts.py`

### Legacy Decommission/Delete Scope
- Delete `QueryMode` definition from `tools/cq/search/pipeline/classifier.py` after import migration.
- Delete `SearchLimits` definition from `tools/cq/search/pipeline/profiles.py` after import migration.
- Delete `SemanticContractStateInputV1`, `SemanticStatus`, and `derive_semantic_contract_state` from `tools/cq/search/semantic/models.py` once imported from `tools/cq/core/semantic_contracts.py`.

---

## S2. Typed Contracts for Summaries, Enrichment Payloads, and Categorical Fields
### Goal
Replace untyped summary/payload/categorical contracts with msgspec/Literal-based structures so illegal states are unrepresentable and downstream logic stops re-validating ad hoc dicts. Execute this scope in three tracks: `S2a` summary typing, `S2b` enrichment payload typing, and `S2c` categorical literal/enums. Scope is complete only when summary producers and summary consumers both use typed adapters/contracts.

### Representative Code Snippets
```python
# tools/cq/core/summary_models.py
from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class RunSummaryV1(CqStruct, frozen=True):
    query: str | None = None
    mode: str | None = None
    lang_scope: str | None = None
    total_matches: int = 0
    matched_files: int = 0
    scanned_files: int = 0
    step_summaries: dict[str, dict[str, object]] = msgspec.field(default_factory=dict)
```

```python
# tools/cq/index/call_resolver.py
from typing import Literal

ResolutionConfidence = Literal["exact", "likely", "ambiguous", "unresolved"]


@dataclass
class ResolvedCall:
    call: CallInfo
    targets: list[FnDecl]
    confidence: ResolutionConfidence
    resolution_path: str
```

```python
# tools/cq/macros/shared.py
from tools.cq.macros.scoring_contracts import ScoringDetailsV1


def macro_scoring_details(...) -> ScoringDetailsV1:
    return ScoringDetailsV1(
        impact_score=impact,
        impact_bucket=bucket(impact),
        confidence_score=confidence,
        confidence_bucket=bucket(confidence),
        evidence_kind=evidence_kind,
    )
```

### Files to Edit
- `tools/cq/core/schema.py`
- `tools/cq/core/cache/contracts.py`
- `tools/cq/cli_app/context.py`
- `tools/cq/cli_app/result.py`
- `tools/cq/core/front_door_insight.py`
- `tools/cq/core/report.py`
- `tools/cq/core/merge.py`
- `tools/cq/macros/shared.py`
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/index/call_resolver.py`
- `tools/cq/introspection/cfg_builder.py`
- `tools/cq/search/enrichment/contracts.py`
- `tools/cq/search/tree_sitter/contracts/lane_payloads.py`
- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/macros/contracts.py`
- `tools/cq/query/merge.py`
- `tools/cq/query/entity_front_door.py`
- `tools/cq/ldmd/writer.py`

### New Files to Create
- `tools/cq/core/summary_models.py`
- `tools/cq/core/summary_adapters.py`
- `tools/cq/search/enrichment/models.py`
- `tools/cq/macros/scoring_contracts.py`
- `tests/unit/tools/cq/core/test_summary_models.py`
- `tests/unit/tools/cq/core/test_summary_adapters.py`
- `tests/unit/tools/cq/search/enrichment/test_models.py`
- `tests/unit/tools/cq/macros/test_scoring_contracts.py`

### Legacy Decommission/Delete Scope
- Delete `summary: dict[str, object]` contract field usage in `tools/cq/core/schema.py` and `tools/cq/core/cache/contracts.py`.
- Delete bare `str` category fields for `ResolvedCall.confidence`, `TaintedSite.kind`, `CallSite.binding`, and `CFGEdge.edge_type`.
- Delete dict-return shape for `macro_scoring_details` in `tools/cq/macros/shared.py`.

---

## S3. Global Mutable State Elimination and Cache Encapsulation
### Goal
Eliminate module-level mutable cache/state patterns by introducing bounded injectable cache abstractions and context-owned state containers. Scope is complete when targeted modules have no mutable process-global cache dictionaries/lists.

### Representative Code Snippets
```python
# tools/cq/search/_shared/bounded_cache.py
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


@dataclass(slots=True)
class BoundedCache(Generic[K, V]):
    max_entries: int
    _store: OrderedDict[K, V] = field(default_factory=OrderedDict)

    def get_or_set(self, key: K, factory: Callable[[], V]) -> V:
        if key in self._store:
            value = self._store.pop(key)
            self._store[key] = value
            return value
        value = factory()
        self._store[key] = value
        while len(self._store) > self.max_entries:
            self._store.popitem(last=False)
        return value
```

```python
# tools/cq/search/pipeline/classifier_cache.py
from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search._shared.bounded_cache import BoundedCache


@dataclass(slots=True)
class ClassifierCacheManager:
    sg_cache: BoundedCache[tuple[str, str], object]
    source_cache: BoundedCache[str, str]
    def_lines_cache: BoundedCache[tuple[str, str], list[tuple[int, int]]]
```

### Files to Edit
- `tools/cq/search/python/analysis_session.py`
- `tools/cq/search/python/extractors.py`
- `tools/cq/search/rust/enrichment.py`
- `tools/cq/search/pipeline/classifier_runtime.py`
- `tools/cq/search/pipeline/classifier.py`
- `tools/cq/search/tree_sitter/core/parse.py`

### New Files to Create
- `tools/cq/search/_shared/bounded_cache.py`
- `tools/cq/search/pipeline/classifier_cache.py`
- `tests/unit/tools/cq/search/_shared/test_bounded_cache.py`
- `tests/unit/tools/cq/search/pipeline/test_classifier_cache.py`

### Legacy Decommission/Delete Scope
- Delete `_SESSION_CACHE` from `tools/cq/search/python/analysis_session.py`.
- Delete `_AST_CACHE` and `_truncation_tracker` globals from `tools/cq/search/python/extractors.py`.
- Delete `_AST_CACHE` from `tools/cq/search/rust/enrichment.py`.
- Delete module-global dict caches from `tools/cq/search/pipeline/classifier_runtime.py`.

---

## S4. Search Pipeline Decomposition (`smart_search.py`) and CQS Cleanup
### Goal
Split pipeline orchestration from types, semantic processing, telemetry, and result assembly while fixing CQS and private API leakage. Scope is complete when `smart_search.py` is orchestration-focused and extracted modules own telemetry, semantic prefetch, and result assembly.

### Representative Code Snippets
```python
# tools/cq/search/pipeline/smart_search.py
from tools.cq.search.pipeline.assembly import assemble_smart_search_result
from tools.cq.search.pipeline.enrichment_telemetry import build_enrichment_telemetry
from tools.cq.search.pipeline.python_semantic import prefetch_python_semantic
from tools.cq.search.pipeline.types import EnrichedMatch, RawMatch, SearchStats


def smart_search(...) -> CqResult:
    partition_results = run_search_partitions(...)
    semantic_state = prefetch_python_semantic(...)
    summary_payload = build_enrichment_telemetry(...)
    return assemble_smart_search_result(...)
```

```python
# tools/cq/search/pipeline/classifier.py
_DEFINITION_NAME_PATTERNS: tuple[str, ...] = (
    r"(?:async\\s+)?(?:def|class)\\s+([A-Za-z_][A-Za-z0-9_]*)",
    r"fn\\s+([A-Za-z_][A-Za-z0-9_]*)",
    r"(?:struct|enum|trait|mod)\\s+([A-Za-z_][A-Za-z0-9_]*)",
)
```

### Files to Edit
- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/search/pipeline/partition_pipeline.py`
- `tools/cq/search/pipeline/orchestration.py`
- `tools/cq/search/pipeline/candidate_normalizer.py`
- `tools/cq/search/pipeline/classifier.py`
- `tools/cq/search/pipeline/classifier_runtime.py`
- `tools/cq/search/_shared/search_contracts.py`

### New Files to Create
- `tools/cq/search/pipeline/types.py`
- `tools/cq/search/pipeline/enrichment_telemetry.py`
- `tools/cq/search/pipeline/python_semantic.py`
- `tools/cq/search/pipeline/assembly.py`
- `tools/cq/search/pipeline/search_object_view_store.py`
- `tests/unit/tools/cq/search/pipeline/test_types.py`
- `tests/unit/tools/cq/search/pipeline/test_enrichment_telemetry.py`
- `tests/unit/tools/cq/search/pipeline/test_python_semantic.py`
- `tests/unit/tools/cq/search/pipeline/test_assembly.py`
- `tests/unit/tools/cq/search/pipeline/test_search_object_view_store.py`

### Legacy Decommission/Delete Scope
- Delete `_smart_search_module()` dynamic import/`Any` indirection usage from `tools/cq/search/pipeline/partition_pipeline.py`.
- Delete `_SEARCH_OBJECT_VIEW_REGISTRY` global dict in `tools/cq/search/pipeline/smart_search.py`.
- Delete in-file telemetry aggregation, semantic prefetch, and assembly helper blocks from `tools/cq/search/pipeline/smart_search.py` after extraction.
- Delete `clear_caches()` side-effect call from `_build_search_context` path in `tools/cq/search/pipeline/smart_search.py`.

---

## S5. Search Lanes and Tree-Sitter Consolidation + Rust Lane Split
### Goal
Remove Python/Rust lane duplication, make lane payload handling deterministic/non-mutating, and split Rust lane runtime into focused modules. Scope is complete when duplicate `NodeLike`, `_normalize_semantic_version`, and `_python_field_ids` definitions are fully centralized and deleted.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/core/lane_support.py
from tools.cq.search.tree_sitter.contracts.core_models import QueryWindowV1
from tools.cq.search.tree_sitter.core.change_windows import (
    contains_window,
    ensure_query_windows,
    windows_from_changed_ranges,
)

ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)


def build_query_windows(
    *,
    anchor_window: QueryWindowV1,
    source_byte_len: int,
    changed_ranges: tuple[object, ...],
) -> tuple[QueryWindowV1, ...]:
    windows = windows_from_changed_ranges(changed_ranges, source_byte_len=source_byte_len, pad_bytes=96)
    windows = ensure_query_windows(windows, fallback=anchor_window)
    if windows and not contains_window(
        windows,
        value=anchor_window.start_byte,
        width=anchor_window.end_byte - anchor_window.start_byte,
    ):
        return (*windows, anchor_window)
    return windows
```

```python
# tools/cq/search/tree_sitter/contracts/lane_payloads.py
def canonicalize_python_lane_payload(payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payload)
    legacy = payload.pop("tree_sitter_diagnostics", None)
    ...
    return payload
```

### Files to Edit
- `tools/cq/search/tree_sitter/contracts/core_models.py`
- `tools/cq/search/tree_sitter/contracts/lane_payloads.py`
- `tools/cq/search/tree_sitter/core/language_registry.py`
- `tools/cq/search/tree_sitter/core/infrastructure.py`
- `tools/cq/search/tree_sitter/core/node_utils.py`
- `tools/cq/search/tree_sitter/core/parse.py`
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/python_lane/locals_index.py`
- `tools/cq/search/tree_sitter/python_lane/fallback_support.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/injections.py`
- `tools/cq/search/tree_sitter/structural/exports.py`
- `tools/cq/search/tree_sitter/query/planner.py`
- `tools/cq/search/tree_sitter/schema/node_schema.py`
- `tools/cq/search/tree_sitter/tags.py`
- `tools/cq/search/python/analysis_session.py`
- `tools/cq/search/python/resolution_support.py`
- `tools/cq/search/python/evidence.py`
- `tools/cq/search/python/resolution_index.py`
- `tools/cq/search/objects/resolve.py`
- `tools/cq/search/semantic/models.py`
- `tools/cq/search/semantic/front_door.py`

### New Files to Create
- `tools/cq/search/tree_sitter/core/lane_support.py`
- `tools/cq/search/python/ast_helpers.py`
- `tools/cq/search/tree_sitter/python_lane/helpers.py`
- `tools/cq/search/tree_sitter/rust_lane/extraction.py`
- `tools/cq/search/tree_sitter/rust_lane/classification.py`
- `tools/cq/search/tree_sitter/rust_lane/facts.py`
- `tests/unit/tools/cq/search/tree_sitter/core/test_lane_support.py`
- `tests/unit/tools/cq/search/python/test_ast_helpers.py`
- `tests/unit/tools/cq/search/tree_sitter/python_lane/test_helpers.py`
- `tests/unit/tools/cq/search/tree_sitter/rust_lane/test_extraction.py`
- `tests/unit/tools/cq/search/tree_sitter/rust_lane/test_classification.py`
- `tests/unit/tools/cq/search/tree_sitter/rust_lane/test_facts.py`

### Legacy Decommission/Delete Scope
- Delete duplicate `_python_field_ids`, `_build_query_windows`, `_lift_anchor`, and duplicated `_ENRICHMENT_ERRORS` definitions across lane runtimes/facts.
- Delete duplicated `_normalize_semantic_version` helpers after centralization.
- Delete duplicated `NodeLike` protocols outside canonical contract file.
- Delete `_touch_tree_cache`-style direct cache mutation in Rust lane runtime after parse-session API adds `remove_entry`.
- Delete deprecated alias names in `tools/cq/search/semantic/models.py` (`fail_open`, `enrich_semantics`) once renamed/removed.

---

## S6. Query Subsystem Decomposition and Public Surface Hardening
### Goal
Decompose `executor.py`, remove private cross-module imports, deduplicate query utilities, enforce explicit public query module contracts (`__all__`), and add structured logging at execution boundaries. Scope is complete when batch modules no longer import private executor internals and query modules expose explicit public surfaces.

### Representative Code Snippets
```python
# tools/cq/query/ast_grep_match.py
from __future__ import annotations

from dataclasses import dataclass

from tools.cq.core.locations import SourceSpan


@dataclass(frozen=True)
class AstGrepMatchSpan:
    span: SourceSpan
    match: object


def execute_rule_matches(...): ...
def group_match_spans(...): ...
def match_passes_filters(...): ...
```

```python
# tools/cq/query/batch_spans.py
from tools.cq.query.ast_grep_match import (
    AstGrepMatchSpan,
    execute_rule_matches,
    group_match_spans,
    match_passes_filters,
    partition_query_metavar_filters,
    resolve_rule_metavar_names,
    resolve_rule_variadic_metavars,
)
```

```python
# tools/cq/query/executor.py
def execute_plan(request: ExecutePlanRequestV1) -> CqResult:
    if request.plan.is_pattern_query != request.query.is_pattern_query:
        msg = "Plan/query type mismatch"
        raise ValueError(msg)
    ...
```

```python
# tools/cq/query/executor.py
import logging

logger = logging.getLogger(__name__)

__all__ = ["execute_plan", "execute_entity_query_from_records", "execute_pattern_query_with_files"]
```

### Files to Edit
- `tools/cq/query/executor.py`
- `tools/cq/query/batch.py`
- `tools/cq/query/batch_spans.py`
- `tools/cq/query/__init__.py`
- `tools/cq/query/merge.py`
- `tools/cq/query/entity_front_door.py`
- `tools/cq/query/symbol_resolver.py`
- `tools/cq/query/enrichment.py`
- `tools/cq/query/planner.py`
- `tools/cq/query/sg_parser.py`
- `tools/cq/query/parser.py`

### New Files to Create
- `tools/cq/query/record_utils.py`
- `tools/cq/query/result_utils.py`
- `tools/cq/query/ast_grep_match.py`
- `tools/cq/query/scan.py`
- `tools/cq/query/finding_builders.py`
- `tools/cq/query/section_builders.py`
- `tools/cq/query/executor_cache.py`
- `tests/unit/tools/cq/query/test_record_utils.py`
- `tests/unit/tools/cq/query/test_result_utils.py`
- `tests/unit/tools/cq/query/test_ast_grep_match.py`
- `tests/unit/tools/cq/query/test_scan.py`
- `tests/unit/tools/cq/query/test_finding_builders.py`
- `tests/unit/tools/cq/query/test_section_builders.py`
- `tests/unit/tools/cq/query/test_executor_cache.py`

### Legacy Decommission/Delete Scope
- Delete private imports from `tools/cq/query/batch.py` and `tools/cq/query/batch_spans.py` into `_`-prefixed executor functions.
- Delete duplicate `_count_result_matches` and `_missing_languages_from_summary` implementations after centralization.
- Delete duplicate `_extract_def_name` implementations after centralization.
- Delete implicit/accidental query module exports once explicit `__all__` declarations are in place.
- Delete or explicitly deprecate non-integrated `tools/cq/query/symbol_resolver.py` flow if integration decision is to remove it.

---

## S7. Macros and Index Refactor (`calls.py` Package Split + SymbolIndex)
### Goal
Stabilize macro/index contracts with typed enums and a protocol-based symbol index, remove dead/duplicated code, and decompose `calls.py` into cohesive modules. Scope is complete when `tools/cq/macros/calls.py` is replaced by a package with explicit public re-exports.

### Representative Code Snippets
```python
# tools/cq/index/contracts.py
from __future__ import annotations

from typing import Protocol


class SymbolIndex(Protocol):
    def find_function_by_name(self, name: str) -> list[object]: ...
    def find_class_by_name(self, name: str) -> list[object]: ...
    def resolve_import_alias(self, file: str, symbol: str) -> tuple[str, str | None]: ...
```

```python
# tools/cq/macros/imports.py
import sys


def _is_stdlib_module(name: str) -> bool:
    top_level = name.split(".", 1)[0]
    return top_level in sys.stdlib_module_names
```

```python
# tools/cq/macros/calls/__init__.py
from tools.cq.macros.calls.entry import cmd_calls, collect_call_sites
from tools.cq.macros.calls.scanning import rg_find_candidates
from tools.cq.macros.calls.analysis import group_candidates

__all__ = ["cmd_calls", "collect_call_sites", "group_candidates", "rg_find_candidates"]
```

### Files to Edit
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/imports.py`
- `tools/cq/macros/contracts.py`
- `tools/cq/macros/shared.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/index/def_index.py`
- `tools/cq/index/call_resolver.py`
- `tools/cq/introspection/cfg_builder.py`
- `tools/cq/macros/__init__.py`

### New Files to Create
- `tools/cq/index/contracts.py`
- `tools/cq/macros/calls/__init__.py`
- `tools/cq/macros/calls/entry.py`
- `tools/cq/macros/calls/scanning.py`
- `tools/cq/macros/calls/analysis.py`
- `tools/cq/macros/calls/neighborhood.py`
- `tools/cq/macros/calls/semantic.py`
- `tools/cq/macros/calls/insight.py`
- `tools/cq/macros/calls/context_snippet.py`
- `tests/unit/tools/cq/index/test_contracts.py`
- `tests/unit/tools/cq/macros/calls/test_entry.py`
- `tests/unit/tools/cq/macros/calls/test_scanning.py`
- `tests/unit/tools/cq/macros/calls/test_analysis.py`
- `tests/unit/tools/cq/macros/calls/test_neighborhood.py`
- `tests/unit/tools/cq/macros/calls/test_semantic.py`
- `tests/unit/tools/cq/macros/calls/test_insight.py`
- `tests/unit/tools/cq/macros/calls/test_context_snippet.py`

### Legacy Decommission/Delete Scope
- Delete `DefIndex.load_or_build` from `tools/cq/index/def_index.py`.
- Delete duplicated `_SELF_CLS` constants from `tools/cq/index/def_index.py` and `tools/cq/index/call_resolver.py` after centralization.
- Delete `_STDLIB_PREFIXES` from `tools/cq/macros/imports.py`.
- Delete monolithic `tools/cq/macros/calls.py` once `tools/cq/macros/calls/` package is cut over.

---

## S8. Core Renderer Decomposition and Core/Search Decoupling
### Goal
Split rendering concerns in core, remove runtime coupling from core to search internals, and complete core contract cleanup (DetailPayload, helper properties, scheduler testability/telemetry). Scope is complete when `core/report.py` no longer imports search pipeline internals.

### Representative Code Snippets
```python
# tools/cq/core/render_enrichment.py
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from tools.cq.core.schema import Finding

EnrichmentCallback = Callable[[Finding, Path], dict[str, object]]


def maybe_attach_render_enrichment(
    finding: Finding,
    *,
    root: Path,
    enrich: EnrichmentCallback | None,
) -> None:
    if enrich is None:
        return
    payload = enrich(finding, root)
    for key, value in payload.items():
        if key not in finding.details:
            finding.details[key] = value
```

```python
# tools/cq/core/runtime/worker_scheduler.py
def get_worker_scheduler(policy: ParallelismPolicy | None = None) -> WorkerScheduler:
    with _SCHEDULER_LOCK:
        if _SCHEDULER_STATE.scheduler is None:
            resolved = policy or default_runtime_execution_policy().parallelism
            _SCHEDULER_STATE.scheduler = WorkerScheduler(resolved)
        return _SCHEDULER_STATE.scheduler


def set_worker_scheduler(scheduler: WorkerScheduler | None) -> None:
    with _SCHEDULER_LOCK:
        _SCHEDULER_STATE.scheduler = scheduler
```

### Files to Edit
- `tools/cq/core/report.py`
- `tools/cq/core/front_door_insight.py`
- `tools/cq/core/schema.py`
- `tools/cq/core/scoring.py`
- `tools/cq/core/runtime/worker_scheduler.py`
- `tools/cq/core/merge.py`
- `tools/cq/search/pipeline/smart_search.py`

### New Files to Create
- `tools/cq/core/render_enrichment.py`
- `tools/cq/core/render_summary.py`
- `tools/cq/core/type_coercion.py`
- `tests/unit/tools/cq/core/test_render_enrichment.py`
- `tests/unit/tools/cq/core/test_render_summary.py`
- `tests/unit/tools/cq/core/test_type_coercion.py`
- `tests/unit/tools/cq/core/runtime/test_worker_scheduler.py`

### Legacy Decommission/Delete Scope
- Delete direct import/call path from `tools/cq/core/report.py` into `tools/cq/search/pipeline/smart_search.py` internals (`RawMatch`, `classify_match`, `build_finding`) after callback injection.
- Delete duplicated coercion helpers from `tools/cq/core/schema.py` and `tools/cq/core/scoring.py`.
- Delete `DetailPayload.__setitem__` mapping-style mutation entry points once call sites are migrated.

---

## S9. CLI/Run/LDMD Cleanup and Runner Decomposition
### Goal
Unify context injection semantics, normalize naming/contracts, split `runner.py` into focused modules, split `handle_result` into pure/imperative paths, and make LDMD collapse/version behavior explicit and public. Scope is complete when no command uses `@require_ctx` and result preparation is independently testable.

### Representative Code Snippets
```python
# tools/cq/run/spec.py
class NeighborhoodStep(RunStepBase, tag="neighborhood", frozen=True):
    target: str
    lang: str = "python"
    top_k: int = 10
    enable_semantic_enrichment: bool = True
```

```python
# tools/cq/neighborhood/section_layout.py
def is_section_collapsed(section_id: str) -> bool:
    if section_id in _UNCOLLAPSED_SECTIONS:
        return False
    if section_id in _DYNAMIC_COLLAPSE_SECTIONS:
        return True
    return True
```

```python
# tools/cq/run/runner.py
from tools.cq.run.run_summary import populate_run_summary_metadata
from tools.cq.run.step_executors import execute_non_q_step


def execute_run_plan(plan: RunPlan, ctx: CliContext, *, stop_on_error: bool = False) -> CqResult:
    services = resolve_runtime_services(ctx.root)
    ...
    result = execute_non_q_step(step, plan=plan, ctx=ctx, services=services, run_id=run_id)
    ...
    populate_run_summary_metadata(merged, executed_results, total_steps=len(steps))
```

```python
# tools/cq/cli_app/result.py
def prepare_result(cli_result: CliResult, filters: FilterConfig | None = None) -> PreparedResult:
    ...


def emit_result(prepared: PreparedResult) -> int:
    ...
```

### Files to Edit
- `tools/cq/cli_app/infrastructure.py`
- `tools/cq/cli_app/commands/search.py`
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/report.py`
- `tools/cq/cli_app/commands/admin.py`
- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/cli_app/commands/ldmd.py`
- `tools/cq/cli_app/commands/repl.py`
- `tools/cq/cli_app/commands/chain.py`
- `tools/cq/cli_app/commands/artifact.py`
- `tools/cq/cli_app/commands/run.py`
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/cli_app/context.py`
- `tools/cq/cli_app/result.py`
- `tools/cq/run/runner.py`
- `tools/cq/run/spec.py`
- `tools/cq/ldmd/format.py`
- `tools/cq/neighborhood/section_layout.py`

### New Files to Create
- `tools/cq/run/step_executors.py`
- `tools/cq/run/run_summary.py`
- `tests/unit/tools/cq/cli_app/test_context_injection.py`
- `tests/unit/tools/cq/cli_app/test_result_pipeline.py`
- `tests/unit/tools/cq/run/test_step_executors.py`
- `tests/unit/tools/cq/run/test_run_summary.py`
- `tests/unit/tools/cq/ldmd/test_format_versioning.py`

### Legacy Decommission/Delete Scope
- Delete `require_ctx` decorator from `tools/cq/cli_app/infrastructure.py` and all decorator usage sites in `tools/cq/cli_app/commands/`.
- Delete `NeighborhoodStep.no_semantic_enrichment` from `tools/cq/run/spec.py`.
- Delete private `_is_collapsed` implementation in `tools/cq/ldmd/format.py` after switching to public `is_section_collapsed`.
- Delete monolithic `handle_result` path in `tools/cq/cli_app/result.py` after `prepare_result`/`emit_result` cutover.
- Delete monolithic step execution and run-summary helper blocks from `tools/cq/run/runner.py` once extracted.
- Delete or reclassify unused `plan_feasible_slices` from public CLI surface.

---

## S10. Search Python Extractors Decomposition
### Goal
Decompose `tools/cq/search/python/extractors.py` into stage-focused modules under `tools/cq/search/python/stages/` so each stage has one reason to change. Scope is complete when stage orchestration is thin and stage-specific logic/tests are isolated by module.

### Representative Code Snippets
```python
# tools/cq/search/python/stages/contracts.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PythonEnrichmentStageContext:
    root: Path
    rel_path: str
    source: str
    line: int
    col: int
```

```python
# tools/cq/search/python/extractors.py
from tools.cq.search.python.stages.astgrep_stage import run_astgrep_stage
from tools.cq.search.python.stages.ast_stage import run_ast_stage
from tools.cq.search.python.stages.tree_sitter_stage import run_tree_sitter_stage


def enrich_python_context(...):
    stage_state = run_astgrep_stage(...)
    stage_state = run_ast_stage(stage_state, ...)
    stage_state = run_tree_sitter_stage(stage_state, ...)
    return finalize_payload(stage_state)
```

### Files to Edit
- `tools/cq/search/python/extractors.py`
- `tools/cq/search/python/evidence.py`
- `tools/cq/search/python/resolution_index.py`
- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/search/enrichment/core.py`

### New Files to Create
- `tools/cq/search/python/stages/__init__.py`
- `tools/cq/search/python/stages/contracts.py`
- `tools/cq/search/python/stages/astgrep_stage.py`
- `tools/cq/search/python/stages/ast_stage.py`
- `tools/cq/search/python/stages/tree_sitter_stage.py`
- `tools/cq/search/python/stages/finalize_stage.py`
- `tests/unit/tools/cq/search/python/stages/test_astgrep_stage.py`
- `tests/unit/tools/cq/search/python/stages/test_ast_stage.py`
- `tests/unit/tools/cq/search/python/stages/test_contracts.py`
- `tests/unit/tools/cq/search/python/stages/test_exports.py`
- `tests/unit/tools/cq/search/python/stages/test_tree_sitter_stage.py`
- `tests/unit/tools/cq/search/python/stages/test_finalize_stage.py`

### Legacy Decommission/Delete Scope
- Delete stage-specific helper blocks from `tools/cq/search/python/extractors.py` after each stage has a canonical module owner.
- Delete transitional stage passthrough wrappers in `tools/cq/search/python/extractors.py` after all call sites import stage APIs.

---

## S11. Architecture Guardrails for Dependency Direction and Private Imports
### Goal
Add automated architecture guardrails that fail CI on forbidden dependency direction and private cross-module imports. Scope is complete when guards run in CI and all current exceptions are either removed or explicitly time-bounded.

### Representative Code Snippets
```python
# tools/cq/core/architecture_guards.py
from __future__ import annotations


def is_forbidden_private_import(importer: str, imported: str) -> bool:
    return imported.rsplit(".", 1)[-1].startswith("_") and importer.split(".")[:-1] != imported.split(".")[:-1]
```

```python
# tests/unit/tools/cq/architecture/test_dependency_direction.py
def test_core_does_not_import_search_pipeline_impl() -> None:
    assert not violates_dependency_direction(
        importer="tools.cq.core.report",
        imported="tools.cq.search.pipeline.smart_search",
    )
```

### Files to Edit
- `pyproject.toml`
- `tools/cq/core/report.py`
- `tools/cq/core/front_door_insight.py`
- `tools/cq/query/batch.py`
- `tools/cq/query/batch_spans.py`

### New Files to Create
- `tools/cq/core/architecture_guards.py`
- `tests/unit/tools/cq/architecture/test_dependency_direction.py`
- `tests/unit/tools/cq/architecture/test_private_import_boundaries.py`

### Legacy Decommission/Delete Scope
- Delete temporary allowlist entries from `tools/cq/core/architecture_guards.py` once violating imports are removed.
- Delete migration-only guard bypass toggles in CI config once guardrail pass-rate reaches 100%.

---

## S12. Determinism and Idempotency Non-Regression Harness
### Goal
Codify determinism/idempotency expectations for search/query/run outputs to prevent regressions during large refactors. Scope is complete when repeated invocations on fixed inputs produce byte-stable normalized outputs.

### Representative Code Snippets
```python
# tests/unit/tools/cq/query/test_executor_determinism.py
def test_execute_plan_is_deterministic_for_fixed_inputs(tmp_path: Path) -> None:
    first = run_query_fixture(tmp_path)
    second = run_query_fixture(tmp_path)
    assert normalize_result(first) == normalize_result(second)
```

```python
# tests/unit/tools/cq/run/test_runner_idempotency.py
def test_execute_run_plan_is_idempotent_for_same_plan(tmp_path: Path) -> None:
    plan = load_plan_fixture()
    first = run_plan_fixture(plan, tmp_path)
    second = run_plan_fixture(plan, tmp_path)
    assert normalize_result(first) == normalize_result(second)
```

### Files to Edit
- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/query/executor.py`
- `tools/cq/run/runner.py`
- `tools/cq/core/serialization.py`

### New Files to Create
- `tests/unit/tools/cq/search/pipeline/test_smart_search_determinism.py`
- `tests/unit/tools/cq/query/test_executor_determinism.py`
- `tests/unit/tools/cq/run/test_runner_idempotency.py`
- `tests/e2e/tools/cq/test_repeated_invocation_determinism.py`

### Legacy Decommission/Delete Scope
- Delete temporary deterministic-normalization helpers used only during baseline capture once canonical ordering is enforced in production paths.

---

## S13. Performance Regression Gate for Refactor Safety
### Goal
Introduce a repeatable performance gate for the highest-risk refactor paths (`search`, `q`, `calls`) so architectural changes do not silently degrade throughput/latency. Scope is complete when baseline thresholds are codified and enforced in CI.

### Representative Code Snippets
```python
# tools/cq/perf/regression_gate.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PerfThresholdV1:
    name: str
    max_p95_ms: float
    max_regression_ratio: float
```

```python
# tests/e2e/tools/cq/test_perf_regression_gate.py
def test_search_perf_regression_within_threshold() -> None:
    baseline, current = run_search_perf_probe()
    assert current.p95_ms <= baseline.p95_ms * 1.10
```

### Files to Edit
- `pyproject.toml`
- `tools/cq/core/runtime/worker_scheduler.py`
- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/query/executor.py`
- `tools/cq/macros/calls.py`

### New Files to Create
- `tools/cq/perf/regression_gate.py`
- `tests/unit/tools/cq/perf/test_regression_gate.py`
- `tests/e2e/tools/cq/test_perf_regression_gate.py`

### Legacy Decommission/Delete Scope
- Delete temporary perf-baseline capture toggles from `tools/cq/perf/regression_gate.py` after stable baseline publication.

---

## S14. Contract Versioning Matrix and Migration Seam Governance
### Goal
Formalize versioning and compatibility rules for CQ public contracts (summary payloads, LDMD markers, front-door insight payloads) and enforce bounded migration seams. Scope is complete when each public contract has an explicit version owner and compatibility rule.

### Representative Code Snippets
```python
# tools/cq/core/contract_versions.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ContractVersionPolicyV1:
    contract_name: str
    current_version: str
    backward_compatible_with: tuple[str, ...]
```

```python
# tools/cq/core/migration_seams.py
from __future__ import annotations


def assert_seam_allowed(seam_name: str, *, remove_by_batch: str) -> None:
    if not remove_by_batch.startswith("D"):
        msg = "Migration seam must reference a decommission batch."
        raise ValueError(msg)
```

### Files to Edit
- `tools/cq/core/schema.py`
- `tools/cq/core/cache/contracts.py`
- `tools/cq/core/contract_codec.py`
- `tools/cq/core/summary_models.py`
- `tools/cq/ldmd/format.py`
- `tools/cq/core/front_door_insight.py`

### New Files to Create
- `tools/cq/core/contract_versions.py`
- `tools/cq/core/migration_seams.py`
- `docs/contracts/cq_contract_version_matrix.md`
- `tests/unit/tools/cq/core/test_contract_versions.py`
- `tests/unit/tools/cq/core/test_migration_seams.py`

### Legacy Decommission/Delete Scope
- Delete unversioned LDMD marker parsing paths in `tools/cq/ldmd/format.py` after version-aware parsing is default.
- Delete seam compatibility adapters in `tools/cq/core/summary_adapters.py` after decommission batches complete.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S2)
- Delete legacy type ownership in `tools/cq/search/pipeline/classifier.py` and `tools/cq/search/pipeline/profiles.py` because `QueryMode` and `SearchLimits` are centralized in `tools/cq/search/_shared/types.py`.
- Delete search-semantic contract definitions from `tools/cq/search/semantic/models.py` because `tools/cq/core/semantic_contracts.py` is canonical.
- Delete dict-based scoring return shape from `tools/cq/macros/shared.py` because `ScoringDetailsV1` is canonical.

### Batch D2 (after S3, S4, S5, S10)
- Delete module-global cache variables in `tools/cq/search/pipeline/classifier_runtime.py`, `tools/cq/search/python/analysis_session.py`, `tools/cq/search/python/extractors.py`, and `tools/cq/search/rust/enrichment.py` because bounded/injected caches are canonical.
- Delete extracted in-file telemetry/semantic/assembly helper blocks from `tools/cq/search/pipeline/smart_search.py` because dedicated modules are canonical.
- Delete duplicated lane helpers (`_python_field_ids`, `_build_query_windows`, `_lift_anchor`, `_normalize_semantic_version`) from lane runtimes/facts because shared helpers are canonical.
- Delete in-file stage orchestration logic from `tools/cq/search/python/extractors.py` because stage modules under `tools/cq/search/python/stages/` are canonical.

### Batch D3 (after S6, S7)
- Delete private cross-module imports from `tools/cq/query/batch.py` and `tools/cq/query/batch_spans.py` because `scan.py` and `ast_grep_match.py` expose public APIs.
- Delete duplicated helper implementations `_count_result_matches`, `_missing_languages_from_summary`, `_extract_def_name` because `result_utils.py`/`record_utils.py` are canonical.
- Delete monolithic `tools/cq/macros/calls.py` after package cutover to `tools/cq/macros/calls/`.

### Batch D4 (after S8, S9)
- Delete core-to-search runtime enrichment coupling in `tools/cq/core/report.py` because callback-driven `render_enrichment.py` is canonical.
- Delete `require_ctx` decorator and decorator-based command injection because `require_context()` is canonical.
- Delete LDMD-local collapse policy logic from `tools/cq/ldmd/format.py` because `tools/cq/neighborhood/section_layout.py:is_section_collapsed` is canonical.

### Batch D5 (after S2, S8, S9, S14)
- Delete remaining untyped summary-path compatibility code in `tools/cq/core/schema.py`, `tools/cq/core/cache/contracts.py`, `tools/cq/cli_app/context.py`, and `tools/cq/run/runner.py` because typed summary models are canonical.
- Delete `DetailPayload` mutation API (`__setitem__`) from `tools/cq/core/schema.py` because immutable construction-time payloads are canonical.

### Batch D6 (after S11, S12, S13, S14)
- Delete architecture guardrail allowlists and advisory-only bypass settings once dependency/private-import violations are zero.
- Delete deterministic baseline capture helpers used only for migration verification after stable ordering is proven by canonical production paths.
- Delete temporary performance baseline bootstrap toggles after CI thresholds are locked.
- Delete seam-policy temporary adapters once all seam removals are completed by D1-D5.

## Implementation Sequence
1. Execute S11 first in advisory mode to expose dependency and private-import violations early with minimal risk.
2. Execute S12 second to lock deterministic/idempotent behavior baselines before structural refactors.
3. Execute S13 third to capture performance baselines before cache/orchestration decompositions.
4. Execute S1 to stabilize foundational imports and remove dependency inversions before deeper refactors.
5. Execute S2 (`S2a` summary typing, `S2b` enrichment payload typing, `S2c` categorical typing) so downstream modules consume typed contracts.
6. Execute S3 to eliminate global mutable state and make decomposition testable.
7. Execute S4 to split search pipeline monolith using S1-S3 contracts/cache abstractions.
8. Execute S5 to consolidate tree-sitter lane duplicates and split Rust lane runtime.
9. Execute S10 to decompose `tools/cq/search/python/extractors.py` after S3/S5 foundations are in place.
10. Execute S6 to split query executor and remove private cross-imports while architecture is fresh from S4/S5/S10.
11. Execute S7 after S2/S6 so macros adopt typed contracts and `SymbolIndex` without churn.
12. Execute S8 once typed summaries/semantic contracts are stable, then decouple core rendering from search internals.
13. Execute S9 to align CLI/run/LDMD around finalized contracts and extracted execution modules.
14. Execute S14 to finalize contract versioning, compatibility ownership, and seam governance across all changed contracts.
15. Promote S11 and S13 from advisory to blocking CI enforcement.
16. Execute decommission batches D1-D6 in order after their prerequisites to avoid deleting in-use migration seams.

## Implementation Checklist
- [ ] S1. Foundation Type Lift and Dependency Direction Repair
- [ ] S2. Typed Contracts for Summaries, Enrichment Payloads, and Categorical Fields
- [ ] S3. Global Mutable State Elimination and Cache Encapsulation
- [ ] S4. Search Pipeline Decomposition (`smart_search.py`) and CQS Cleanup
- [ ] S5. Search Lanes and Tree-Sitter Consolidation + Rust Lane Split
- [ ] S6. Query Subsystem Decomposition and Public Surface Hardening
- [ ] S7. Macros and Index Refactor (`calls.py` Package Split + SymbolIndex)
- [ ] S8. Core Renderer Decomposition and Core/Search Decoupling
- [ ] S9. CLI/Run/LDMD Cleanup and Runner Decomposition
- [ ] S10. Search Python Extractors Decomposition
- [ ] S11. Architecture Guardrails for Dependency Direction and Private Imports
- [ ] S12. Determinism and Idempotency Non-Regression Harness
- [ ] S13. Performance Regression Gate for Refactor Safety
- [ ] S14. Contract Versioning Matrix and Migration Seam Governance
- [ ] D1. Legacy Type Ownership and Dict-Contract Deletions
- [ ] D2. Global Cache and Lane-Duplication Deletions
- [ ] D3. Query Private-Import and Calls-Monolith Deletions
- [ ] D4. Core/Search Coupling and Decorator Injection Deletions
- [ ] D5. Final Typed-Summary and DetailPayload Mutation Deletions
- [ ] D6. Guardrail/Baseline/Seam Cleanup Deletions
