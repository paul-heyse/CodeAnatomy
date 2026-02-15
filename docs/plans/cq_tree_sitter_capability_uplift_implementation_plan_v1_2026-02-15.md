# CQ Tree-Sitter Capability Uplift Implementation Plan v1 (2026-02-15)

## Scope Summary

This plan implements the approved tree-sitter capability uplift for `tools/cq` across query execution, injections, query-pack usage, schema drift detection, CST artifact exports, Rust macro enrichment, module graph quality, and adaptive runtime controls. The scope explicitly covers the ten improvement areas identified in the prior review.

Design stance: **incremental hardening with strict decommission at the end**. We keep behavior-compatible surfaces during rollout, then delete superseded logic in explicit batches once replacement paths are live and tested.

## Design Principles

1. Keep query execution deterministic and bounded first, then optimize.
2. Prefer tree-sitter-native capabilities (`set_*_range`, `pattern_settings`, `included_ranges`) over Python-side reimplementation.
3. All new serialized outputs must be `msgspec` contracts; avoid raw ad hoc dict payloads for new planes.
4. Query-pack behavior changes must be contract-tested and grammar-drift-tested.
5. Keep Rust distribution-pack usage explicit and profile-driven; avoid hidden implicit loading.
6. Treat macro expansion as optional enrichment, never as a hard dependency for baseline search.
7. Every new module added under `tools/cq/search/**` gets a corresponding unit test file.
8. Legacy deletion is mandatory once replacement functionality is verified.

## Current Baseline

- `tools/cq/search/tree_sitter/core/runtime.py` currently bypasses `QueryCursor.set_byte_range(...)` and performs Python-side overlap filtering.
- Containment-window APIs are not wired in runtime settings or execution pathways.
- Injection planning in `tools/cq/search/tree_sitter/rust_lane/injections.py` is capture-bucket/index based, not `matches()` + `pattern_settings()` based.
- Rust runtime pack loading is local-only (`include_distribution_queries=False`) in `tools/cq/search/tree_sitter/rust_lane/runtime.py`.
- Python locals query pack `tools/cq/search/queries/python/40_locals.scm` lacks `@local.scope` and uses a broad identifier reference capture.
- Grammar drift checking in `tools/cq/search/tree_sitter/query/grammar_drift.py` is currently shallow (digest + basic source presence checks).
- Structural export (`tools/cq/search/tree_sitter/structural/export.py`) is available, but full `cst_query_hits`-style artifacting is not first-class in search runtime payloads.
- Rust macro evidence in `tools/cq/search/rust/evidence.py` is heuristic (`expansion_hint`) and not analyzer-backed.
- Rust module graph edges in `tools/cq/search/rust/evidence.py` are prefix-heuristic and do not model richer module/import semantics.
- Adaptive runtime telemetry exists in `tools/cq/search/tree_sitter/core/adaptive_runtime.py`, but lane-level tuning logic is still mostly static/ad hoc.

## S1. Containment-Aware Query Window Engine

### Goal

Upgrade bounded query execution to support intersection and containment windowing policies with runtime capability detection, so changed-range execution becomes both faster and semantically stricter when supported by the installed bindings.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/contracts/core_models.py
from typing import Literal

class QueryExecutionSettingsV1(CqStruct, frozen=True):
    match_limit: int = 4096
    max_start_depth: int | None = None
    budget_ms: int | None = None
    timeout_micros: int | None = None
    require_containment: bool = False
    window_mode: Literal[
        "intersection",
        "containment_preferred",
        "containment_required",
    ] = "intersection"
```

```python
# tools/cq/search/tree_sitter/core/windowing.py
from __future__ import annotations

def apply_byte_window(cursor: object, start_byte: int, end_byte: int, *, mode: str) -> bool:
    containing = getattr(cursor, "set_containing_byte_range", None)
    if mode in {"containment_preferred", "containment_required"} and callable(containing):
        containing(start_byte, end_byte)
        return True
    if mode == "containment_required":
        return False
    setter = getattr(cursor, "set_byte_range", None)
    if callable(setter):
        setter(start_byte, end_byte)
        return True
    return False
```

### Files to Edit

- `tools/cq/search/tree_sitter/contracts/core_models.py`
- `tools/cq/search/tree_sitter/core/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/diagnostics/collector.py`

### New Files to Create

- `tools/cq/search/tree_sitter/core/windowing.py`
- `tests/unit/cq/search/tree_sitter/test_core_windowing.py`

### Legacy Decommission/Delete Scope

- Delete the unconditional byte-range bypass branch in `_apply_window` from `tools/cq/search/tree_sitter/core/runtime.py` because cursor-native range scoping supersedes it.
- Delete `_filter_captures_by_window` in `tools/cq/search/tree_sitter/core/runtime.py` after cursor-native byte windowing is the default path.
- Delete `_capture_map_overlaps_window` in `tools/cq/search/tree_sitter/core/runtime.py` once overlap filtering is no longer required for primary execution.

---

## S2. Match-Centric Injection Planning and Full Injection Property Support

### Goal

Refactor injection planning to be `matches()`-driven and settings-aware (`pattern_settings`) so capture relationships remain correct and full injection directives are honored (`injection.combined`, `injection.include-children`, `injection.self`, `injection.parent`, optional filename/language selectors).

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/rust_lane/injection_settings.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct

class InjectionSettingsV1(CqStruct, frozen=True):
    language: str | None = None
    combined: bool = False
    include_children: bool = False
    use_self_language: bool = False
    use_parent_language: bool = False


def settings_for_pattern(query: object, pattern_idx: int) -> InjectionSettingsV1:
    raw = query.pattern_settings(pattern_idx)
    if not isinstance(raw, dict):
        return InjectionSettingsV1()
    return InjectionSettingsV1(
        language=str(raw.get("injection.language")) if raw.get("injection.language") else None,
        combined="injection.combined" in raw,
        include_children="injection.include-children" in raw,
        use_self_language="injection.self" in raw,
        use_parent_language="injection.parent" in raw,
    )
```

```python
# tools/cq/search/tree_sitter/rust_lane/injections.py
# New planner entrypoint: derive plans from match rows, not flattened capture buckets.
def build_injection_plan_from_matches(*, query: object, matches: list[tuple[int, dict[str, list[object]]]], source_bytes: bytes) -> tuple[InjectionPlanV1, ...]:
    plans: list[InjectionPlanV1] = []
    for pattern_idx, capture_map in matches:
        settings = settings_for_pattern(query, pattern_idx)
        content_nodes = capture_map.get("injection.content", [])
        language_nodes = capture_map.get("injection.language", [])
        for node in content_nodes:
            language_name = _resolve_language_name(settings, language_nodes, source_bytes)
            if language_name:
                plans.append(_plan_from_node(node, language_name, settings))
    return tuple(plans)
```

### Files to Edit

- `tools/cq/search/tree_sitter/rust_lane/injections.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/injection_runtime.py`
- `tools/cq/search/queries/rust/60_injections.scm`

### New Files to Create

- `tools/cq/search/tree_sitter/rust_lane/injection_settings.py`
- `tests/unit/cq/search/tree_sitter/test_rust_lane_injection_settings.py`
- `tests/unit/cq/search/tree_sitter/test_rust_lane_injection_match_planner.py`

### Legacy Decommission/Delete Scope

- Delete index-alignment logic in `_build_plan_key` from `tools/cq/search/tree_sitter/rust_lane/injections.py` because it is superseded by per-match planning.
- Delete pseudo-capture dependence on `@injection.combined` in `tools/cq/search/queries/rust/60_injections.scm`; use `#set! injection.combined` instead.
- Delete the current `build_injection_plan(...captures=...)` call path in `tools/cq/search/tree_sitter/rust_lane/runtime.py` once match-driven planning is wired.

---

## S3. Rust Query-Pack Profiles and Tags Plane Integration

### Goal

Introduce profile-driven query-pack loading (local-only vs local+distribution) and integrate a first-class Rust tags extraction plane so definition/reference indexing can leverage standard tags semantics.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/query/registry.py
from tools.cq.core.structs import CqStruct

class QueryPackProfileV1(CqStruct, frozen=True):
    profile_name: str
    include_distribution: bool = False
    required_pack_names: tuple[str, ...] = ()


def load_query_pack_sources_for_profile(language: str, profile: QueryPackProfileV1) -> tuple[QueryPackSourceV1, ...]:
    rows = load_query_pack_sources(language, include_distribution=profile.include_distribution)
    required = set(profile.required_pack_names)
    if required and not required.issubset({row.pack_name for row in rows}):
        return ()
    return rows
```

```python
# tools/cq/search/tree_sitter/tags/runtime.py
# Capture convention: @role.definition / @role.reference + @name.
def build_tag_events(matches: list[tuple[int, dict[str, list[object]]]], source_bytes: bytes) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for _pattern_idx, capture_map in matches:
        role = "definition" if "role.definition" in capture_map else "reference"
        names = capture_map.get("name", [])
        if not names:
            continue
        node = names[0]
        out.append({
            "role": role,
            "name": source_bytes[node.start_byte:node.end_byte].decode("utf-8", "replace"),
            "start_byte": int(node.start_byte),
            "end_byte": int(node.end_byte),
        })
    return out
```

### Files to Edit

- `tools/cq/search/tree_sitter/query/registry.py`
- `tools/cq/search/tree_sitter/rust_lane/bundle.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/pipeline/smart_search.py`

### New Files to Create

- `tools/cq/search/tree_sitter/tags/contracts.py`
- `tools/cq/search/tree_sitter/tags/runtime.py`
- `tools/cq/search/queries/rust/80_tags.scm`
- `tests/unit/cq/search/tree_sitter/test_query_registry_profiles.py`
- `tests/unit/cq/search/tree_sitter/test_tags_runtime.py`

### Legacy Decommission/Delete Scope

- Delete hard-coded local-only Rust pack loading in `_pack_source_rows` from `tools/cq/search/tree_sitter/rust_lane/runtime.py`.
- Delete direct `include_distribution_queries=False` usage for bundle reporting in `_attach_query_pack_payload` from `tools/cq/search/tree_sitter/rust_lane/runtime.py`.
- Delete the assumption that grammar bundle metadata is only `query_pack_names` in `tools/cq/search/tree_sitter/rust_lane/bundle.py` and replace with profile-aware pack metadata.

---

## S4. Locals Pack Completeness and Scope-Aware Locals Index

### Goal

Upgrade Python locals extraction to include explicit scope captures and a deterministic locals index so reference/definition relationships are more accurate and less noisy.

### Representative Code Snippets

```scheme
; tools/cq/search/queries/python/40_locals.scm
(function_definition
  name: (identifier) @local.definition
  body: (block) @local.scope)

(lambda
  body: (_) @local.scope)

(identifier) @local.reference
```

```python
# tools/cq/search/tree_sitter/python_lane/locals_index.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct

class LocalBindingV1(CqStruct, frozen=True):
    name: str
    scope_start: int
    scope_end: int
    definition_start: int


def build_locals_index(*, definitions: list[object], scopes: list[object], source_bytes: bytes) -> tuple[LocalBindingV1, ...]:
    bindings: list[LocalBindingV1] = []
    for node in definitions:
        name = source_bytes[node.start_byte:node.end_byte].decode("utf-8", "replace")
        scope = _nearest_scope(node, scopes)
        bindings.append(
            LocalBindingV1(
                name=name,
                scope_start=int(scope.start_byte) if scope else int(node.start_byte),
                scope_end=int(scope.end_byte) if scope else int(node.end_byte),
                definition_start=int(node.start_byte),
            )
        )
    return tuple(bindings)
```

### Files to Edit

- `tools/cq/search/queries/python/40_locals.scm`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/semantic/models.py`

### New Files to Create

- `tools/cq/search/tree_sitter/python_lane/locals_index.py`
- `tests/unit/cq/search/tree_sitter/test_python_locals_index.py`

### Legacy Decommission/Delete Scope

- Delete the broad catch-all identifier local-reference rule from `tools/cq/search/queries/python/40_locals.scm` once scoped locals captures are in place.
- Delete `_scope_chain` from `tools/cq/search/tree_sitter/python_lane/facts.py` after `locals_index.py` becomes the canonical scope source.

---

## S5. Deep Grammar Drift Diffing and Query Contract Snapshots

### Goal

Replace shallow drift checks with structural grammar/query diffing and persistable contract snapshots so grammar upgrades are assessed as schema migrations, not binary pass/fail events.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/query/drift_diff.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct

class GrammarDiffV1(CqStruct, frozen=True):
    added_node_kinds: tuple[str, ...] = ()
    removed_node_kinds: tuple[str, ...] = ()
    added_fields: tuple[str, ...] = ()
    removed_fields: tuple[str, ...] = ()


def diff_schema(old_index: object, new_index: object) -> GrammarDiffV1:
    old_nodes = set(old_index.all_node_kinds)
    new_nodes = set(new_index.all_node_kinds)
    old_fields = set(old_index.field_names)
    new_fields = set(new_index.field_names)
    return GrammarDiffV1(
        added_node_kinds=tuple(sorted(new_nodes - old_nodes)),
        removed_node_kinds=tuple(sorted(old_nodes - new_nodes)),
        added_fields=tuple(sorted(new_fields - old_fields)),
        removed_fields=tuple(sorted(old_fields - new_fields)),
    )
```

```python
# tools/cq/search/tree_sitter/query/grammar_drift.py
# Integrate structural diff payload into GrammarDriftReportV1.
report = GrammarDriftReportV1(
    language=language,
    grammar_digest=grammar_digest,
    query_digest=query_digest,
    compatible=not errors,
    errors=tuple(errors),
    schema_diff=msgspec.to_builtins(schema_diff),
)
```

### Files to Edit

- `tools/cq/search/tree_sitter/contracts/query_models.py`
- `tools/cq/search/tree_sitter/query/grammar_drift.py`
- `tools/cq/search/tree_sitter/query/lint.py`
- `tools/cq/search/tree_sitter/rust_lane/bundle.py`
- `tools/cq/search/pipeline/smart_search.py`

### New Files to Create

- `tools/cq/search/tree_sitter/query/drift_diff.py`
- `tools/cq/search/tree_sitter/query/contract_snapshot.py`
- `tests/unit/cq/search/tree_sitter/test_query_drift_diff.py`
- `tests/unit/cq/search/tree_sitter/test_query_contract_snapshot.py`

### Legacy Decommission/Delete Scope

- Delete digest-only compatibility logic from `build_grammar_drift_report` in `tools/cq/search/tree_sitter/query/grammar_drift.py`.
- Delete the current minimal `invalid_pack_name` drift-only failure mode in `tools/cq/search/tree_sitter/query/grammar_drift.py` after structured diff checks are active.

---

## S6. Full Agent-Ready CST Artifact Exports (`cst_nodes/edges/tokens/diagnostics/query_hits`)

### Goal

Promote CST artifacts to first-class runtime outputs with typed contracts for query hits and diagnostics so downstream LLM-facing features receive joinable, span-anchored structural data.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/contracts/core_models.py
class TreeSitterQueryHitV1(CqStruct, frozen=True):
    query_name: str
    pattern_index: int
    capture_name: str
    node_id: str
    start_byte: int
    end_byte: int


class TreeSitterArtifactBundleV1(CqCacheStruct, frozen=True):
    run_id: str
    query: str
    language: str
    files: list[str] = msgspec.field(default_factory=list)
    batches: list[TreeSitterEventBatchV1] = msgspec.field(default_factory=list)
    structural_exports: list[TreeSitterStructuralExportV1] = msgspec.field(default_factory=list)
    cst_tokens: list[TreeSitterCstTokenV1] = msgspec.field(default_factory=list)
    cst_diagnostics: list[TreeSitterDiagnosticV1] = msgspec.field(default_factory=list)
    cst_query_hits: list[TreeSitterQueryHitV1] = msgspec.field(default_factory=list)
```

```python
# tools/cq/search/tree_sitter/structural/query_hits.py
def export_query_hits(*, file_path: str, matches: list[tuple[int, dict[str, list[object]]]], source_bytes: bytes) -> tuple[TreeSitterQueryHitV1, ...]:
    rows: list[TreeSitterQueryHitV1] = []
    for pattern_index, capture_map in matches:
        for capture_name, nodes in capture_map.items():
            for node in nodes:
                rows.append(
                    TreeSitterQueryHitV1(
                        query_name=file_path,
                        pattern_index=pattern_index,
                        capture_name=capture_name,
                        node_id=f"{file_path}:{int(node.start_byte)}:{int(node.end_byte)}:{node.type}",
                        start_byte=int(node.start_byte),
                        end_byte=int(node.end_byte),
                    )
                )
    return tuple(rows)
```

### Files to Edit

- `tools/cq/search/tree_sitter/contracts/core_models.py`
- `tools/cq/search/tree_sitter/structural/export.py`
- `tools/cq/search/tree_sitter/structural/match_rows.py`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/neighborhood/tree_sitter_contracts.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`

### New Files to Create

- `tools/cq/search/tree_sitter/structural/query_hits.py`
- `tools/cq/search/tree_sitter/structural/diagnostic_export.py`
- `tests/unit/cq/search/tree_sitter/test_structural_query_hits.py`
- `tests/unit/cq/search/tree_sitter/test_structural_diagnostic_export.py`

### Legacy Decommission/Delete Scope

- Delete raw `query_match_rows` dict payload emission from `tools/cq/search/tree_sitter/python_lane/facts.py` after typed `cst_query_hits` is wired.
- Delete raw `query_match_rows` dict payload emission from `tools/cq/search/tree_sitter/rust_lane/runtime.py` after typed `cst_query_hits` is wired.

---

## S7. Rust Macro Expansion Bridge (Optional Semantic Enrichment)

### Goal

Add an optional rust-analyzer macro expansion bridge that can attach expansion-derived facts without making baseline tree-sitter enrichment dependent on analyzer availability.

### Representative Code Snippets

```python
# tools/cq/search/rust/macro_expansion_contracts.py
from tools.cq.core.structs import CqOutputStruct

class RustMacroExpansionRequestV1(CqOutputStruct, frozen=True):
    file_path: str
    line: int
    col: int
    macro_call_id: str


class RustMacroExpansionResultV1(CqOutputStruct, frozen=True):
    macro_call_id: str
    name: str | None = None
    expansion: str | None = None
    source: str = "rust_analyzer"
    applied: bool = False
```

```python
# tools/cq/search/rust/macro_expansion_bridge.py
# JSON-RPC call shape for rust-analyzer/expandMacro.
def expand_macro(client: object, request: RustMacroExpansionRequestV1) -> RustMacroExpansionResultV1:
    payload = {
        "textDocument": {"uri": request.file_path},
        "position": {"line": request.line, "character": request.col},
    }
    response = client.request("rust-analyzer/expandMacro", payload)
    result = response.get("result") if isinstance(response, dict) else None
    if not isinstance(result, dict):
        return RustMacroExpansionResultV1(macro_call_id=request.macro_call_id)
    return RustMacroExpansionResultV1(
        macro_call_id=request.macro_call_id,
        name=result.get("name") if isinstance(result.get("name"), str) else None,
        expansion=result.get("expansion") if isinstance(result.get("expansion"), str) else None,
        applied=True,
    )
```

### Files to Edit

- `tools/cq/search/rust/evidence.py`
- `tools/cq/search/rust/enrichment.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tests/unit/cq/search/test_rust_macro_expansion_bridge.py`

### New Files to Create

- `tools/cq/search/rust/macro_expansion_contracts.py`
- `tools/cq/search/rust/macro_expansion_bridge.py`
- `tests/unit/cq/search/rust/test_macro_expansion_bridge_runtime.py`

### Legacy Decommission/Delete Scope

- Delete `expansion_hint` heuristic-only semantics from `RustMacroEvidenceV1` in `tools/cq/search/rust/evidence.py` once expansion result contracts are integrated.
- Delete macro-evidence derivation that only strips trailing `!` from call names in `build_macro_evidence` in `tools/cq/search/rust/evidence.py` after bridge-backed enrichment is available.

---

## S8. Robust Rust Module and Import Graph Extraction

### Goal

Replace prefix-heuristic module edge construction with structured module/import fact extraction that supports module declarations, imports, extern crates, and re-export-aware graph semantics.

### Representative Code Snippets

```python
# tools/cq/search/rust/module_graph_contracts.py
from tools.cq.core.structs import CqOutputStruct

class RustModuleNodeV1(CqOutputStruct, frozen=True):
    module_id: str
    module_name: str
    file_path: str | None = None


class RustImportEdgeV1(CqOutputStruct, frozen=True):
    source_module_id: str
    target_path: str
    visibility: str = "private"
    is_reexport: bool = False
```

```python
# tools/cq/search/rust/module_graph_builder.py
# Build graph from normalized tree-sitter fact rows, not string-prefix heuristics.
def build_module_graph(*, module_rows: list[dict[str, object]], import_rows: list[dict[str, object]]) -> dict[str, object]:
    modules = _normalize_modules(module_rows)
    edges = _normalize_import_edges(import_rows, modules)
    return {
        "modules": [msgspec.to_builtins(row) for row in modules],
        "edges": [msgspec.to_builtins(row) for row in edges],
    }
```

### Files to Edit

- `tools/cq/search/rust/evidence.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/queries/rust/50_modules_imports.scm`
- `tests/unit/cq/search/test_rust_module_graph.py`

### New Files to Create

- `tools/cq/search/rust/module_graph_contracts.py`
- `tools/cq/search/rust/module_graph_builder.py`
- `tests/unit/cq/search/rust/test_module_graph_builder.py`

### Legacy Decommission/Delete Scope

- Delete prefix-only edge derivation in `build_rust_module_graph` from `tools/cq/search/rust/evidence.py` because structured import edges supersede it.
- Delete direct tuple passthrough of `modules`/`imports` in `RustModuleGraphV1` from `tools/cq/search/rust/evidence.py` once normalized node/edge contracts are adopted.

---

## S9. Telemetry-Driven Runtime Autotuning and Degrade Modes

### Goal

Use runtime telemetry to tune budget/match-limit/window-splitting decisions per lane and emit deterministic degrade reasons when throttling or cancellation occurs.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/autotune.py
from tools.cq.core.structs import CqStruct

class QueryAutotunePlanV1(CqStruct, frozen=True):
    budget_ms: int
    match_limit: int
    window_split_target: int


def build_autotune_plan(*, snapshot: object, default_budget_ms: int, default_match_limit: int) -> QueryAutotunePlanV1:
    avg = float(getattr(snapshot, "average_latency_ms", 0.0) or 0.0)
    budget = max(50, min(2_000, int(max(default_budget_ms, avg * 4.0))))
    match_limit = max(512, min(16_384, int(default_match_limit if avg < 120 else default_match_limit // 2)))
    split_target = 1 if avg < 100 else 4
    return QueryAutotunePlanV1(budget_ms=budget, match_limit=match_limit, window_split_target=split_target)
```

```python
# tools/cq/search/tree_sitter/core/runtime.py
# Apply autotune plan before cursor execution.
plan = build_autotune_plan(
    snapshot=runtime_snapshot("query_matches", fallback_budget_ms=200),
    default_budget_ms=effective_settings.budget_ms or 200,
    default_match_limit=effective_settings.match_limit,
)
settings = QueryExecutionSettingsV1(
    match_limit=plan.match_limit,
    budget_ms=plan.budget_ms,
    window_mode=effective_settings.window_mode,
)
```

### Files to Edit

- `tools/cq/search/tree_sitter/core/adaptive_runtime.py`
- `tools/cq/search/tree_sitter/core/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/pipeline/smart_search.py`

### New Files to Create

- `tools/cq/search/tree_sitter/core/autotune.py`
- `tests/unit/cq/search/tree_sitter/test_core_autotune.py`

### Legacy Decommission/Delete Scope

- Delete lane-specific hard-coded fallback budget branches in `tools/cq/search/tree_sitter/python_lane/runtime.py` after central autotune planning is adopted.
- Delete per-call ad hoc `match_limit` overrides in `tools/cq/search/tree_sitter/rust_lane/runtime.py` once autotune policy controls limits.

---

## S10. Query-Pack Contract Hygiene and CI Enforcement

### Goal

Strengthen query-pack contract hygiene across Python and Rust packs (captures, metadata, windowability, directives) and lock in these rules with CI-grade tests.

### Representative Code Snippets

```yaml
# tools/cq/search/queries/rust/contracts.yaml
require_rooted: true
forbid_non_local: true
required_metadata_keys:
  - cq.emit
  - cq.kind
  - cq.anchor
forbidden_capture_names:
  - injection.combined
```

```python
# tools/cq/search/tree_sitter/query/lint.py
# Enforce metadata keys per pattern and fail packs with missing contract fields.
def _missing_required_metadata(settings: dict[str, object], required: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(key for key in required if key not in settings)
```

### Files to Edit

- `tools/cq/search/tree_sitter/query/lint.py`
- `tools/cq/search/tree_sitter/contracts/query_models.py`
- `tools/cq/search/queries/python/contracts.yaml`
- `tools/cq/search/queries/rust/contracts.yaml`
- `tools/cq/search/queries/rust/60_injections.scm`
- `tests/unit/cq/search/test_query_pack_lint.py`

### New Files to Create

- `tools/cq/search/queries/rust/85_locals.scm`
- `tests/unit/cq/search/test_tree_sitter_query_contracts_schema.py`
- `tests/unit/cq/search/test_tree_sitter_tags_queries.py`

### Legacy Decommission/Delete Scope

- Delete `@injection.combined` capture usage from `tools/cq/search/queries/rust/60_injections.scm` once property-based injection directives are enforced.
- Delete redundant diagnostics overlap in `tools/cq/search/queries/python/90_quality.scm` after `95_diagnostics.scm` is the single diagnostics query source.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S9)

- Delete `_filter_captures_by_window` from `tools/cq/search/tree_sitter/core/runtime.py` because cursor-native windowing + autotune supersede Python post-filtering.
- Delete `_capture_map_overlaps_window` from `tools/cq/search/tree_sitter/core/runtime.py` for the same reason.

### Batch D2 (after S2, S10)

- Delete capture-index alignment path (`language_nodes[idx]`, `macro_nodes[idx]`) in `_build_plan_key` from `tools/cq/search/tree_sitter/rust_lane/injections.py` because match-centric planning guarantees capture coherence.
- Delete `@injection.combined` pseudo-capture from `tools/cq/search/queries/rust/60_injections.scm` because property directives become canonical.

### Batch D3 (after S3, S5, S6)

- Delete raw `query_match_rows` payload field emissions in `tools/cq/search/tree_sitter/python_lane/facts.py` and `tools/cq/search/tree_sitter/rust_lane/runtime.py` because typed `cst_query_hits` is the canonical artifact.
- Delete digest-only drift acceptance path in `tools/cq/search/tree_sitter/query/grammar_drift.py` because structural schema diffing supersedes it.

### Batch D4 (after S7, S8)

- Delete heuristic macro expansion hint generation in `build_macro_evidence` from `tools/cq/search/rust/evidence.py` because bridge-backed expansion artifacts supersede it.
- Delete prefix-based edge derivation in `build_rust_module_graph` from `tools/cq/search/rust/evidence.py` because normalized module/import graph builders supersede it.

## Implementation Sequence

1. Implement S1 first to establish the runtime windowing contract used by later scopes.
2. Implement S2 next so injection planning correctness is fixed before expanding injection/tags usage.
3. Implement S10 third to lock query-pack contract rules before adding more packs and metadata usage.
4. Implement S3 after S10 so tags/profile loading lands on top of enforced query-pack hygiene.
5. Implement S4 once query-pack hygiene is in place; this reduces locals pack churn.
6. Implement S5 to harden grammar drift/reporting before widening artifact outputs.
7. Implement S6 to standardize export contracts and replace raw row payloads.
8. Implement S8 before S7 so macro-expansion enrichment has a better module/import substrate to attach to.
9. Implement S7 to add optional rust-analyzer semantic depth without destabilizing baseline.
10. Implement S9 last to tune final runtime behavior based on all preceding capability planes.
11. Execute decommission batches D1-D4 in order once prerequisites and tests are complete.

## Implementation Checklist

- [ ] S1. Containment-Aware Query Window Engine
- [ ] S2. Match-Centric Injection Planning and Full Injection Property Support
- [ ] S3. Rust Query-Pack Profiles and Tags Plane Integration
- [ ] S4. Locals Pack Completeness and Scope-Aware Locals Index
- [ ] S5. Deep Grammar Drift Diffing and Query Contract Snapshots
- [ ] S6. Full Agent-Ready CST Artifact Exports (`cst_nodes/edges/tokens/diagnostics/query_hits`)
- [ ] S7. Rust Macro Expansion Bridge (Optional Semantic Enrichment)
- [ ] S8. Robust Rust Module and Import Graph Extraction
- [ ] S9. Telemetry-Driven Runtime Autotuning and Degrade Modes
- [ ] S10. Query-Pack Contract Hygiene and CI Enforcement
- [ ] D1. Remove Python-side window overlap filtering after S1+S9
- [ ] D2. Remove capture-index injection logic and pseudo-captures after S2+S10
- [ ] D3. Remove raw query row payloads and digest-only drift checks after S3+S5+S6
- [ ] D4. Remove heuristic macro/module graph code after S7+S8
