# Incremental Code Enrichment Implementation Plan v1 (2026-02-17)

## Scope Summary

Implement a hard-cutover incremental enrichment system for CQ search/neighborhood that fully integrates all approved `symtable`, `dis`, and `inspect` enrichments (including compound joins and preview/expand details kinds) on top of tree-sitter anchors.

**Design stance:**
- Hard cutover in design phase: migrate directly to target design
- No compatibility shims, no deprecation staging, no dual-path runtime behavior
- Replace legacy symtable-only and python-semantic-only search enrichment pathways with unified incremental enrichment payloads
- Keep fail-open behavior for per-anchor enrichment failures, but keep contracts and integration surfaces strict

## Implementation Audit Status (2026-02-17)

This plan has been audited against the current `tools/cq` codebase. Current status:

- `S1`–`S12`: complete.
- `D1`–`D4`: complete.
- Hard-cutover cleanup is complete for search-match compatibility surfaces:
  - Removed `tools/cq/search/pipeline/python_semantic.py`.
  - Removed semantic-wrapper contract surfaces superseded by incremental contracts.
  - Removed `python_semantic_prefetch` scaffolding from partition result contracts.
- Test matrix is aligned to incremental enrichment and hard-cutover expectations:
  - Added/updated coverage for incremental modes across classification, rendering/sections, CLI search/neighborhood, and run-step parity.
  - Removed obsolete legacy-focused pipeline tests tied to removed semantic-prefetch paths.

## Design Principles

1. **Information hiding:** plane internals (scope resolution, CFG/DFG, runtime reflection) remain private implementation details behind typed enrichment contracts.
2. **Separation of concerns:** extraction planes, orchestration/provider, rendering, and transport/request plumbing are split into distinct modules.
3. **SRP:** each plane module has one reason to change (symtable, dis, inspect, compound joins).
4. **High cohesion/low coupling:** reuse existing CQ introspection and pipeline primitives rather than duplicating algorithms.
5. **Dependency direction:** pipeline/CLI depend on contracts; contracts do not depend on CLI/runtime command surfaces.
6. **DRY (knowledge):** one canonical binding resolver and one canonical enrichment mode model.
7. **Parse, don’t validate:** parse once into typed contracts (`msgspec.Struct`) at boundaries; avoid downstream ad-hoc dict normalization.
8. **Illegal states unrepresentable:** enrichment mode is a closed enum; details kind names are registry-backed constants.
9. **CQS:** `enrich(...)` is a query; cache writes are isolated side effects.
10. **Functional core/imperative shell:** plane builders are deterministic transforms; I/O/import/runtime capture is isolated and capability-gated.
11. **Determinism/reproducibility:** `dis.get_instructions(..., adaptive=False)` for semantic facts, stable hashing for cache keys, explicit runtime gates.
12. **Declare/version contracts:** all cross-module payloads are versioned msgspec contracts; preview/expand payload protocol is explicit.

## Historical Baseline (At Plan Creation)

- `EnrichedMatch` currently carries `symtable`, `python_enrichment`, and `python_semantic_enrichment`; there is no incremental enrichment field (`tools/cq/search/pipeline/smart_search_types.py`).
- `classify_match(...)` currently runs legacy symtable/python/python-semantic stages and has no incremental stage (`tools/cq/search/pipeline/classification.py`).
- Classification callsites in `classify_phase` and `partition_pipeline` do not currently propagate any incremental-mode option object (`tools/cq/search/pipeline/classify_phase.py`, `tools/cq/search/pipeline/partition_pipeline.py`).
- Search request/config contracts currently do not carry incremental enrichment options (`tools/cq/core/services.py`, `tools/cq/orchestration/request_factory.py`, `tools/cq/search/pipeline/contracts.py`).
- CQ already has reusable introspection assets that should be leveraged instead of duplicated:
  - symtable scope extraction with meta-scope enum support (`tools/cq/introspection/symtable_extract.py`)
  - bytecode instruction facts + exception table parsing (`tools/cq/introspection/bytecode_index.py`)
  - CFG reconstruction (`tools/cq/introspection/cfg_builder.py`)
- Enrichment rendering/facts are currently integrated through structured enrichment payloads in sections/object facts (`tools/cq/search/pipeline/smart_search_sections.py`, `tools/cq/search/objects/resolve.py`, `tools/cq/core/enrichment_facts.py`, `tools/cq/core/render_enrichment.py`).
- CLI/run/neighborhood surfaces currently do not expose enrichment mode controls for this scope (`tools/cq/cli_app/params.py`, `tools/cq/cli_app/options.py`, `tools/cq/run/spec.py`, `tools/cq/neighborhood/executor.py`).

---

## S1. Contract and Mode Realignment

### Goal
Establish canonical, versioned incremental enrichment contracts and a single enrichment mode enum used consistently across pipeline, CLI, run steps, neighborhood execution, caching, and rendering.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/enrichment_contracts.py
from __future__ import annotations

import enum

import msgspec


class IncrementalEnrichmentModeV1(str, enum.Enum):
    TS_ONLY = "ts_only"
    TS_SYM = "ts_sym"
    TS_SYM_DIS = "ts_sym_dis"
    FULL = "full"


class IncrementalEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    schema_version: int = 1
    mode: IncrementalEnrichmentModeV1 = IncrementalEnrichmentModeV1.TS_SYM
    payload: dict[str, object] = msgspec.field(default_factory=dict)
```

```python
# tools/cq/search/pipeline/smart_search_types.py
@dataclass(frozen=True, slots=True)
class MatchClassifyOptions:
    incremental_enabled: bool = True
    incremental_mode: IncrementalEnrichmentModeV1 = IncrementalEnrichmentModeV1.TS_SYM
```

### Files to Edit
- `tools/cq/search/pipeline/enrichment_contracts.py`
- `tools/cq/search/pipeline/smart_search_types.py`
- `tools/cq/search/pipeline/contracts.py`

### New Files to Create
- `tests/unit/cq/search/pipeline/test_incremental_contracts.py`

### Legacy Decommission/Delete Scope
- Delete stringly-typed incremental mode handling paths in classification/CLI plumbing.
- Delete duplicated per-module mode enums once canonical contract enum is in place.

---

## S2. Symtable Plane Completion (A1-A5)

### Goal
Implement full symtable enrichment coverage: scope graph/meta-scopes, partitions, namespace edges, canonical binding resolution, and symtable↔bytecode correctness invariants.

### Representative Code Snippets

```python
# tools/cq/search/enrichment/incremental_symtable_plane.py
from __future__ import annotations

import symtable


def build_sym_scope_graph(table: symtable.SymbolTable) -> dict[str, object]:
    # Includes MODULE/FUNCTION/CLASS plus annotation/type meta-scopes in 3.13.
    rows: list[dict[str, object]] = []
    stack: list[symtable.SymbolTable] = [table]
    while stack:
        t = stack.pop()
        rows.append(
            {
                "table_id": t.get_id(),
                "type": str(t.get_type()),
                "name": t.get_name(),
                "lineno": t.get_lineno(),
                "is_nested": t.is_nested(),
                "is_optimized": t.is_optimized(),
            }
        )
        stack.extend(t.get_children())
    return {"tables": rows}


def resolve_binding_id(
    root: symtable.SymbolTable,
    scope: symtable.SymbolTable,
    name: str,
) -> str:
    # Canonical identity used across TS occurrences + bytecode def/use events.
    sym = scope.lookup(name)
    if sym.is_declared_global() or sym.is_global():
        return f"{root.get_id()}:{name}"
    if sym.is_parameter() or sym.is_local() or sym.is_imported() or sym.is_assigned():
        return f"{scope.get_id()}:{name}"
    return f"UNRESOLVED:{name}"
```

### Files to Edit
- `tools/cq/search/enrichment/__init__.py`
- `tools/cq/search/python/analysis_session.py`
- `tools/cq/introspection/symtable_extract.py`

### New Files to Create
- `tools/cq/search/enrichment/incremental_symtable_plane.py`
- `tests/unit/cq/search/enrichment/test_incremental_symtable_plane.py`

### Legacy Decommission/Delete Scope
- Delete legacy, low-fidelity symtable-only enrichment shaping in search classification once full symtable plane is wired.

---

## S3. dis Plane Completion (B1-B3)

### Goal
Implement full bytecode plane coverage: normalized instruction facts, exception-region semantics, CFG, def/use events, reaching-def DFG, and attribution coverage metrics.

### Representative Code Snippets

```python
# tools/cq/search/enrichment/incremental_dis_plane.py
from __future__ import annotations

import dis
from types import CodeType

from tools.cq.introspection.bytecode_index import extract_instruction_facts, parse_exception_table
from tools.cq.introspection.cfg_builder import build_cfg


def build_dis_bundle(code: CodeType) -> dict[str, object]:
    instr = extract_instruction_facts(code)
    cfg = build_cfg(code)
    exc = parse_exception_table(code)

    return {
        "instruction_facts": [
            {
                "offset": i.offset,
                "baseopname": i.baseopname,
                "opname": i.opname,
                "argval": i.argval,
                "positions": i.positions,
            }
            for i in instr
        ],
        "cfg": {
            "blocks": cfg.block_count,
            "edges": cfg.edge_count,
            "exit_blocks": cfg.exit_blocks,
        },
        "exception_regions": [
            {
                "start": e.start,
                "end": e.end,
                "target": e.target,
                "depth": e.depth,
                "lasti": e.lasti,
            }
            for e in exc
        ],
    }
```

### Files to Edit
- `tools/cq/introspection/bytecode_index.py`
- `tools/cq/introspection/cfg_builder.py`
- `tools/cq/search/enrichment/__init__.py`

### New Files to Create
- `tools/cq/search/enrichment/incremental_dis_plane.py`
- `tests/unit/cq/search/enrichment/test_incremental_dis_plane.py`

### Legacy Decommission/Delete Scope
- Delete duplicate CFG/exception parsing helpers in incremental search code once introspection-backed implementation is canonical.

---

## S4. inspect Plane Completion (C1-C4)

### Goal
Implement full inspect plane coverage: static-safe object inventory, member/descriptor analysis, unwrap/signature/bind simulation, annotation extraction, and runtime-state attribution (tracebacks/frames/generator-coro state).

### Representative Code Snippets

```python
# tools/cq/search/enrichment/incremental_inspect_plane.py
from __future__ import annotations

import importlib
import inspect


def inspect_object_inventory(module_name: str, dotted: str) -> dict[str, object]:
    mod = importlib.import_module(module_name)
    obj = mod
    for part in dotted.split("."):
        obj = inspect.getattr_static(obj, part)

    members = inspect.getmembers_static(obj)
    sig_wrapped = str(inspect.signature(obj, follow_wrapped=True, eval_str=False))
    sig_raw = str(inspect.signature(obj, follow_wrapped=False, eval_str=False))
    anns = inspect.get_annotations(obj, eval_str=False)

    unwrapped = inspect.unwrap(obj, stop=lambda x: hasattr(x, "__signature__"))
    return {
        "dotted": dotted,
        "object_kind": type(obj).__name__,
        "sig_follow_wrapped": sig_wrapped,
        "sig_raw": sig_raw,
        "unwrapped_qualname": getattr(unwrapped, "__qualname__", ""),
        "annotations_keys": sorted(k for k in anns.keys() if isinstance(k, str)),
        "members_count": len(members),
    }
```

### Files to Edit
- `tools/cq/search/enrichment/__init__.py`
- `tools/cq/search/python/analysis_session.py`

### New Files to Create
- `tools/cq/search/enrichment/incremental_inspect_plane.py`
- `tests/unit/cq/search/enrichment/test_incremental_inspect_plane.py`

### Legacy Decommission/Delete Scope
- Delete placeholder/no-op inspect enrichment stubs in incremental plan implementation once full inspect plane is implemented.

---

## S5. Compound Joins and Unified Binding Identity (D1-D3)

### Goal
Implement cross-plane joins that unify tree-sitter occurrences, symtable binding truth, and dis def/use events under a canonical `binding_id`, plus decorator-aware callsite correctness and compact correctness backstops.

### Representative Code Snippets

```python
# tools/cq/search/enrichment/incremental_compound_plane.py
from __future__ import annotations


def build_binding_join(
    ts_occurrences: list[dict[str, object]],
    dis_events: list[dict[str, object]],
) -> dict[str, object]:
    by_binding: dict[str, dict[str, object]] = {}
    for row in ts_occurrences:
        binding_id = str(row.get("binding_id", ""))
        if not binding_id:
            continue
        by_binding.setdefault(binding_id, {"ts": 0, "dis": 0})["ts"] += 1
    for row in dis_events:
        binding_id = str(row.get("binding_id", ""))
        if not binding_id:
            continue
        by_binding.setdefault(binding_id, {"ts": 0, "dis": 0})["dis"] += 1
    return {"binding_join": by_binding}
```

### Files to Edit
- `tools/cq/search/enrichment/__init__.py`
- `tools/cq/search/pipeline/classification.py`

### New Files to Create
- `tools/cq/search/enrichment/incremental_compound_plane.py`
- `tests/unit/cq/search/enrichment/test_incremental_compound_plane.py`

### Legacy Decommission/Delete Scope
- Delete separate per-plane identity keys where they duplicate canonical `binding_id` join semantics.

---

## S6. Provider Orchestration and Session Reuse

### Goal
Implement a single provider that orchestrates all planes using `PythonAnalysisSession` cached artifacts (tree-sitter, symtable, compiled module code object), with strict fail-open semantics per plane.

### Representative Code Snippets

```python
# tools/cq/search/python/analysis_session.py
class PythonAnalysisSession:
    # ... existing fields ...
    compiled_module: object | None = None

    def ensure_compiled_module(self) -> object | None:
        if self.compiled_module is not None:
            return self.compiled_module
        try:
            self.compiled_module = compile(self.source, str(self.file_path), "exec")
        except (SyntaxError, ValueError, TypeError):
            self.compiled_module = None
        return self.compiled_module
```

```python
# tools/cq/search/enrichment/incremental_provider.py
def enrich_anchor(...) -> IncrementalEnrichmentV1 | None:
    session = get_python_analysis_session(file_path, source)
    tree = session.ensure_tree_sitter_tree()
    st = session.ensure_symtable()
    co = session.ensure_compiled_module()
    # plane assembly + fail-open handling
```

### Files to Edit
- `tools/cq/search/python/analysis_session.py`
- `tools/cq/search/enrichment/__init__.py`

### New Files to Create
- `tools/cq/search/enrichment/incremental_provider.py`
- `tests/unit/cq/search/enrichment/test_incremental_provider.py`

### Legacy Decommission/Delete Scope
- Delete provider-local global code caches superseded by session-level compiled-module cache.

---

## S7. Pipeline and Cache Integration (Search Classification)

### Goal
Replace legacy symtable/python-semantic stage orchestration with canonical incremental enrichment stage integration, including complete option propagation across all classification callsites and cache payload boundaries.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/classification.py
def classify_match(..., options: MatchClassifyOptions | None = None, **legacy_flags: bool) -> EnrichedMatch:
    resolved = options or MatchClassifyOptions()
    incremental = _maybe_incremental_enrichment(
        file_path=file_path,
        raw=raw,
        lang=lang,
        mode=resolved.incremental_mode,
        enabled=resolved.incremental_enabled,
        cache_context=cache_context,
    )
    enrichment = MatchEnrichment(
        context_window=context_window,
        context_snippet=context_snippet,
        rust_tree_sitter=rust_tree_sitter,
        python_enrichment=python_enrichment,
        incremental_enrichment=incremental,
    )
```

```python
# tools/cq/search/pipeline/partition_pipeline.py
enriched_match = classify_match(
    raw,
    root,
    lang=task.lang,
    cache_context=cache_context,
    options=task.match_options,
)
```

### Files to Edit
- `tools/cq/search/pipeline/classification.py`
- `tools/cq/search/pipeline/smart_search_types.py`
- `tools/cq/search/pipeline/classify_phase.py`
- `tools/cq/search/pipeline/partition_pipeline.py`
- `tools/cq/search/pipeline/contracts.py`
- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/search/pipeline/smart_search_telemetry.py`
- `tools/cq/search/pipeline/smart_search_sections.py`

### New Files to Create
- `tests/unit/cq/search/pipeline/test_incremental_classification_integration.py`

### Legacy Decommission/Delete Scope
- Delete `_maybe_symtable_enrichment()` from `tools/cq/search/pipeline/classification.py`.
- Delete `_maybe_python_semantic_enrichment()` from `tools/cq/search/pipeline/classification.py`.
- Delete legacy `enable_symtable` and `enable_python_semantic` option handling in classification merging logic.

---

## S8. CLI, Request Factory, Run-Step, and Neighborhood Propagation

### Goal
Propagate incremental enrichment controls end-to-end through search CLI, request factory, service requests, run steps, and neighborhood execution contracts.

### Representative Code Snippets

```python
# tools/cq/cli_app/params.py
@dataclass(kw_only=True)
class SearchParams(FilterParams):
    # ... existing fields ...
    enrich: Annotated[bool, Parameter(name="--enrich", negative="--no-enrich")] = True
    enrich_mode: Annotated[str, Parameter(name="--enrich-mode")] = "ts_sym"
```

```python
# tools/cq/orchestration/request_factory.py
class SearchRequestOptionsV1(CqStruct, frozen=True):
    # ... existing fields ...
    incremental_enrichment_enabled: bool = True
    incremental_enrichment_mode: str = "ts_sym"
```

```python
# tools/cq/run/spec.py
class SearchStep(RunStepBase, tag="search", frozen=True):
    query: str
    mode: SearchMode | None = None
    include_strings: bool = False
    in_dir: str | None = None
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
    enrich: bool = True
    enrich_mode: str = "ts_sym"
```

### Files to Edit
- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/options.py`
- `tools/cq/cli_app/commands/search.py`
- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/core/services.py`
- `tools/cq/orchestration/request_factory.py`
- `tools/cq/search/pipeline/contracts.py`
- `tools/cq/search/pipeline/smart_search.py`
- `tools/cq/run/spec.py`
- `tools/cq/run/step_executors.py`
- `tools/cq/neighborhood/executor.py`

### New Files to Create
- `tests/unit/cq/cli_app/test_search_enrichment_flags.py`
- `tests/unit/cq/run/test_run_step_enrichment_flags.py`
- `tests/unit/cq/neighborhood/test_executor_enrichment_flags.py`

### Legacy Decommission/Delete Scope
- Delete legacy search option translation paths that do not include incremental enrichment fields.

---

## S9. Rendering and Code-Facts Integration

### Goal
Integrate incremental enrichment into existing structured enrichment/facts rendering pipeline (not raw markdown injection), preserving CQ section architecture and language adapter telemetry semantics.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/smart_search_sections.py
def _merge_enrichment_payloads(data: dict[str, object], match: EnrichedMatch) -> None:
    enrichment: dict[str, object] = {"language": match.language}
    if match.python_enrichment:
        python_payload = python_enrichment_payload(match.python_enrichment)
        if match.incremental_enrichment:
            python_payload["incremental"] = msgspec.to_builtins(match.incremental_enrichment)
        enrichment["python"] = python_payload
    if match.rust_tree_sitter:
        enrichment["rust"] = rust_enrichment_payload(match.rust_tree_sitter)
    if len(enrichment) > 1:
        data["enrichment"] = enrichment
```

```python
# tools/cq/core/enrichment_facts.py
# Add incremental clusters keyed by details.kind preview payloads:
# - sym.scope_graph / sym.partitions / sym.binding_resolve
# - dis.instruction_facts / dis.cfg / dis.defuse_dfg / dis.anchor_metrics
# - inspect.object_inventory / inspect.callsite_bind_check / runtime.trace_attribution
```

### Files to Edit
- `tools/cq/search/pipeline/smart_search_sections.py`
- `tools/cq/search/objects/resolve.py`
- `tools/cq/core/enrichment_facts.py`
- `tools/cq/core/render_enrichment.py`
- `tools/cq/core/render_enrichment_orchestrator.py`

### New Files to Create
- `tests/unit/cq/core/test_enrichment_facts_incremental.py`
- `tests/unit/cq/search/pipeline/test_incremental_rendering.py`

### Legacy Decommission/Delete Scope
- Delete any direct compact-string embedding path for incremental enrichment that bypasses `enrichment` structured payload.

---

## S10. Details-Kind Registry and Expand Dispatcher

### Goal
Implement registry-backed preview/expand protocol for heavy enrichment payloads to support token-efficient default rendering and deterministic on-demand expansion.

### Representative Code Snippets

```python
# tools/cq/core/details_kinds.py
class KindSpec(CqStruct, frozen=True):
    kind: str
    version: int = 1
    rank: int = 1000
    preview_keys: tuple[str, ...] = msgspec.field(default_factory=tuple)


DETAILS_KIND_REGISTRY: dict[str, KindSpec] = {
    "sym.scope_graph": KindSpec(kind="sym.scope_graph", rank=100, preview_keys=("tables_count",)),
    "dis.cfg": KindSpec(kind="dis.cfg", rank=230, preview_keys=("edges_n", "exc_edges_n")),
    "inspect.callsite_bind_check": KindSpec(
        kind="inspect.callsite_bind_check", rank=330, preview_keys=("callee", "bind_ok")
    ),
}
```

```python
# tools/cq/macros/expand.py
from tools.cq.core.details_kinds import DETAILS_KIND_REGISTRY


def cmd_expand(kind: str, handle: dict[str, object]) -> CqResult:
    spec = DETAILS_KIND_REGISTRY.get(kind)
    if spec is None:
        return error_result(...)
    payload = EXPANDERS[kind](handle)
    return build_expand_result(kind=kind, payload=payload)
```

### Files to Edit
- `tools/cq/core/schema.py`
- `tools/cq/core/scoring.py`
- `tools/cq/cli_app/app.py`

### New Files to Create
- `tools/cq/core/details_kinds.py`
- `tools/cq/macros/expand.py`
- `tools/cq/cli_app/commands/expand.py`
- `tests/unit/cq/macros/test_expand_incremental.py`
- `tests/unit/cq/core/test_details_kinds.py`

### Legacy Decommission/Delete Scope
- Delete ad-hoc, unregistered enrichment detail kind strings in search rendering once registry is authoritative.

---

## S11. Hard-Cutover Legacy Removal

### Goal
Complete the design-phase migration by removing superseded legacy enrichment contracts, flags, and staging paths in the same implementation window.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/smart_search_types.py
@dataclass(frozen=True, slots=True)
class MatchEnrichment:
    context_window: ContextWindow | None
    context_snippet: str | None
    rust_tree_sitter: RustTreeSitterEnrichmentV1 | None
    python_enrichment: PythonEnrichmentV1 | None
    incremental_enrichment: IncrementalEnrichmentV1 | None
```

### Files to Edit
- `tools/cq/search/pipeline/smart_search_types.py`
- `tools/cq/search/pipeline/classifier.py`
- `tools/cq/search/pipeline/classification.py`
- `tools/cq/search/pipeline/python_semantic.py`
- `tools/cq/search/pipeline/smart_search_telemetry.py`

### New Files to Create
- `tests/unit/cq/search/pipeline/test_hard_cutover_legacy_removal.py`

### Legacy Decommission/Delete Scope
- Delete `SymtableEnrichment` struct from `tools/cq/search/pipeline/classifier.py`.
- Delete legacy `symtable` field on `EnrichedMatch` and `MatchEnrichment`.
- Delete classification option flags superseded by incremental mode model (`enable_symtable`, `enable_python_semantic`, compatibility merge paths).
- Delete compatibility wiring that keeps old semantic-only enrichment path alive for search matching.

---

## S12. Test Matrix, Fixtures, and Validation Expansion

### Goal
Expand unit/e2e coverage to validate all sym/dis/inspect enrichments, compound joins, contract behavior, request propagation, and deterministic rendering/expand outputs.

### Representative Code Snippets

```python
# tests/unit/cq/search/enrichment/test_incremental_planes_end_to_end.py
@pytest.mark.parametrize("mode", ["ts_only", "ts_sym", "ts_sym_dis", "full"])
def test_incremental_provider_modes(mode: str, tmp_path: Path) -> None:
    p = tmp_path / "sample.py"
    p.write_text("""
def outer(x):
    y = x + 1
    def inner(z):
        return y + z
    return inner
""")
    provider = CQIncrementalEnrichmentProvider()
    bundle = provider.enrich(str(p), Point(row=4, col=15), IncrementalEnrichmentModeV1(mode))
    assert bundle is not None
```

```python
# tests/e2e/cq/test_incremental_enrichment_e2e.py
# Validate:
# - search --enrich --enrich-mode ts_sym_dis
# - neighborhood --enrich --enrich-mode full
# - details.kind preview + cq expand roundtrip
```

### Files to Edit
- `tests/unit/cq/search/pipeline/test_classification.py`
- `tests/unit/cq/search/pipeline/test_smart_search_sections.py`
- `tests/unit/cq/search/pipeline/test_enrichment_contracts.py`
- `tests/e2e/cq/test_search_command_e2e.py`
- `tests/e2e/cq/test_neighborhood_command_e2e.py`

### New Files to Create
- `tests/unit/cq/search/enrichment/test_incremental_planes_end_to_end.py`
- `tests/unit/cq/search/enrichment/test_incremental_binding_identity.py`
- `tests/unit/cq/search/enrichment/test_incremental_invariants.py`
- `tests/e2e/cq/test_incremental_enrichment_e2e.py`

### Legacy Decommission/Delete Scope
- Delete obsolete tests that only assert legacy symtable boolean enrichment shapes after cutover.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S7, S8)
- Delete legacy classification option flags (`enable_symtable`, `enable_python_semantic`) from `tools/cq/search/pipeline/smart_search_types.py` because canonical incremental mode/enable controls replace them.
- Delete compatibility merge logic in `tools/cq/search/pipeline/classification.py` that maps legacy flag kwargs into classify options.

### Batch D2 (after S2, S3, S5, S6)
- Delete legacy symtable-only enrichment shaping in `tools/cq/search/pipeline/classification.py` and `tools/cq/search/pipeline/classifier.py` because full sym/dis/compound incremental planes supersede it.
- Delete duplicate/disconnected binding identity fields that are superseded by canonical `binding_id` joins.

### Batch D3 (after S9, S10)
- Delete ad-hoc enrichment detail kind strings and unregistered payload keys in rendering paths because `tools/cq/core/details_kinds.py` becomes authoritative.
- Delete direct compact-enrichment blob rendering paths that bypass structured facts.

### Batch D4 (after S11, S12)
- Delete obsolete unit/e2e tests that target removed legacy symtable/python-semantic-only search enrichment surfaces.

## Implementation Sequence

1. **S1. Contract and mode realignment**: establish stable contract foundation before integration edits.
2. **S2. Symtable plane completion**: implement compiler-truth scope and binding substrate first.
3. **S3. dis plane completion**: add bytecode substrate and CFG/DFG facts on top of symtable grounding.
4. **S4. inspect plane completion**: add runtime reflection surfaces after static substrate is stable.
5. **S5. Compound joins**: unify cross-plane identities and correctness backstops.
6. **S6. Provider orchestration**: consolidate all plane assembly into one provider.
7. **S7. Pipeline/cache integration**: wire provider into classification and cache boundaries.
8. **S8. CLI/request/run/neighborhood propagation**: expose controls through all execution entrypoints.
9. **S9. Rendering/facts integration**: surface structured enrichment consistently in search/neighborhood outputs.
10. **S10. Details-kind + expand**: enable preview/expand protocol once payloads are in place.
11. **S11. Hard-cutover legacy removal**: remove superseded paths and fields in same milestone.
12. **S12. Test matrix expansion**: finalize with comprehensive unit/e2e validation.

## Implementation Checklist

- [x] S1. Contract and mode realignment
- [x] S2. Symtable plane completion (A1-A5)
- [x] S3. dis plane completion (B1-B3)
- [x] S4. inspect plane completion (C1-C4)
- [x] S5. Compound joins and unified binding identity (D1-D3)
- [x] S6. Provider orchestration and session reuse
- [x] S7. Pipeline and cache integration (search classification)
- [x] S8. CLI, request factory, run-step, and neighborhood propagation
- [x] S9. Rendering and code-facts integration
- [x] S10. Details-kind registry and expand dispatcher
- [x] S11. Hard-cutover legacy removal
- [x] S12. Test matrix, fixtures, and validation expansion
- [x] D1. Remove legacy classify options and compatibility merge paths
- [x] D2. Remove legacy symtable-only enrichment surfaces
- [x] D3. Remove ad-hoc details/rendering paths superseded by registry
- [x] D4. Remove obsolete legacy-focused tests
