# CQ Search File Consolidation Implementation Plan v1 (2026-02-15)

## Scope Summary

This plan restructures `tools/cq/search/` to achieve a practical target band of 250-750 LOC per implementation module, with clear semantic subfolders and reduced duplication. The plan also includes a serialization-boundary cleanup so transportable/settings/output contracts use `msgspec` (`CqSettingsStruct`, `CqOutputStruct`, `CqStruct`) while runtime-only handles stay in dataclasses/plain runtime objects.

Design stance: **staged migration with compatibility exports during execution, hard cleanup at the end**. This avoids breaking current imports from `tools/cq/*`, `tests/*`, and CLI/runtime entrypoints while the package is being decomposed.

## Design Principles

1. Every non-marker implementation module in `tools/cq/search/` must end in the 250-750 LOC band.
2. Package marker files (`__init__.py`) are exempt from LOC targets.
3. Runtime handles (`SgRoot`, tree-sitter `Node/Tree/Parser`, toolchain/session handles) must not live in serializable contracts.
4. All serialized config/settings contracts migrate to `CqSettingsStruct`; output/public contracts migrate to `CqOutputStruct`.
5. Shared helpers are centralized once and imported everywhere else (no copied helper functions).
6. Refactors are organized by semantic boundaries, not filename prefixes.
7. Library capability upgrades are intentional and tested (no speculative API usage).
8. Every new module gets a corresponding unit test module in `tests/unit/cq/search/`.
9. Compatibility re-exports stay only until all callsites move; then delete legacy files in explicit batches.

## Current Baseline

- `tools/cq/search/` currently contains 96 Python files and 22,101 LOC.
- LOC distribution is highly fragmented: 77 files are <250 LOC, 12 files are 250-750 LOC, and 7 files are >750 LOC.
- Oversized files requiring splits:
- `tools/cq/search/smart_search.py` (3720)
- `tools/cq/search/python_enrichment.py` (2250)
- `tools/cq/search/tree_sitter_rust.py` (1774)
- `tools/cq/search/classifier.py` (1047)
- `tools/cq/search/tree_sitter_python.py` (824)
- `tools/cq/search/python_native_resolution.py` (823)
- Undersized contract/helper fragmentation is concentrated in `tree_sitter_*_contracts.py`, `language_front_door_contracts.py`, `partition_contracts.py`, `rust_*_contracts.py`, `object_*_contracts.py`, and micro helper modules.
- Confirmed duplicated helpers (via `rg` + `ast-grep`) include:
- `_line_col_to_byte_offset` in `tools/cq/search/python_analysis_session.py`, `tools/cq/search/python_native_resolution.py`, `tools/cq/search/python_enrichment.py`, `tools/cq/search/language_front_door_pipeline.py`
- `_node_text` variants in `tools/cq/search/tree_sitter_python_facts.py`, `tools/cq/search/tree_sitter_python.py`, `tools/cq/search/tree_sitter_match_rows.py`, `tools/cq/search/tree_sitter_injections.py`, `tools/cq/search/tree_sitter_rust.py`, `tools/cq/search/rust_enrichment.py`
- `_source_hash` in `tools/cq/search/python_enrichment.py`, `tools/cq/search/python_analysis_session.py`, `tools/cq/search/rust_enrichment.py`
- `_truncate` in `tools/cq/search/python_enrichment.py`, `tools/cq/search/tree_sitter_rust.py`
- `_scope_chain` variants in `tools/cq/search/tree_sitter_python_facts.py`, `tools/cq/search/tree_sitter_rust.py`, `tools/cq/search/rust_enrichment.py`
- `_ENRICHMENT_ERRORS` tuple duplicated in `tools/cq/search/python_enrichment.py`, `tools/cq/search/tree_sitter_python.py`, `tools/cq/search/tree_sitter_rust.py`, `tools/cq/search/rust_enrichment.py`, `tools/cq/search/smart_search.py`
- Serialization boundary leaks exist in `tools/cq/search/requests.py` (`CqStruct` fields typed as `object` for runtime handles).
- External dependency surface is wide (CLI/query/macros/neighborhood/tests import many `tools.cq.search.*` modules), so staged compatibility is required.
- Additional library capabilities identified (from `cq-lib-ref`) and currently underused in search modules:
- `msgspec.json.Encoder` / `msgspec.msgpack.Encoder` reusable instances for hot paths
- `msgspec.convert(..., from_attributes=True)` for runtime-object to contract coercion
- `msgspec.Struct` perf knobs (`cache_hash=True`, selective `forbid_unknown_fields=True` policy)
- `pathspec.PathSpec.from_lines("gitwildmatch", ...)` for deterministic include/exclude normalization
- tree-sitter field/kind id lookups (`Language.field_id_for_name`, `Language.id_for_node_kind`) for cheaper repeated checks
- ast-grep config-mode matching (`constraints`, utility-rule style decomposition) for brittle manual predicate cleanup

## S1. Establish Target Topology and LOC Guardrails

### Goal
Define the final subfolder structure and introduce deterministic LOC guardrails so every module migration is measurable. Produce a file-action manifest (split/merge/move/delete/keep) for all current `tools/cq/search/*.py` files.

### Representative Code Snippets

```python
# tools/cq/search/_migration/file_manifest.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

Action = Literal["split", "merge", "move", "keep", "delete"]


@dataclass(frozen=True, slots=True)
class FileAction:
    source: Path
    action: Action
    targets: tuple[Path, ...] = ()


def enforce_loc_band(path: Path, *, min_loc: int = 250, max_loc: int = 750) -> bool:
    if path.name == "__init__.py":
        return True
    loc = sum(1 for _ in path.open("r", encoding="utf-8"))
    return min_loc <= loc <= max_loc
```

### Files to Edit

- `docs/plans/cq_search_file_consolidation_implementation_plan_v1_2026-02-15.md`
- `pyproject.toml`

### New Files to Create

- `tools/cq/search/_migration/file_manifest.py`
- `tools/cq/search/_migration/loc_guard.py`
- `tests/unit/cq/search/test_file_manifest.py`
- `tests/unit/cq/search/test_loc_guard.py`

### Legacy Decommission/Delete Scope

- Delete ad hoc one-off LOC counting scripts used outside repository policy.

---

## S2. Centralize Shared Helpers and Runtime-vs-Serializable Contracts

### Goal
Eliminate duplicated helpers and fix serialization boundaries by splitting request/settings/output objects from runtime-only objects.

### Representative Code Snippets

```python
# tools/cq/search/_shared/contracts.py
from __future__ import annotations

from dataclasses import dataclass

import msgspec

from tools.cq.core.structs import CqOutputStruct, CqSettingsStruct


class PythonByteRangeSettingsV1(CqSettingsStruct, frozen=True):
    cache_key: str
    byte_start: int
    byte_end: int
    query_budget_ms: int | None = None


class PythonEnrichmentPayloadV1(CqOutputStruct, frozen=True):
    enrichment_status: str = "applied"
    enrichment_sources: tuple[str, ...] = ()
    data: dict[str, object] = msgspec.field(default_factory=dict)


@dataclass(slots=True)
class PythonByteRangeRuntime:
    sg_root: object
    resolved_node: object | None = None
    session: object | None = None
```

```python
# tools/cq/search/_shared/helpers.py
from __future__ import annotations

from hashlib import blake2b


def source_hash(source_bytes: bytes) -> str:
    return blake2b(source_bytes, digest_size=16).hexdigest()


def truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max(1, max_len - 3)] + "..."
```

### Files to Edit

- `tools/cq/search/requests.py`
- `tools/cq/search/models.py`
- `tools/cq/search/contracts_runtime_boundary.py`
- `tools/cq/search/python_enrichment.py`
- `tools/cq/search/rust_enrichment.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/python_analysis_session.py`
- `tools/cq/search/python_native_resolution.py`
- `tools/cq/search/language_front_door_pipeline.py`

### New Files to Create

- `tools/cq/search/_shared/helpers.py`
- `tools/cq/search/_shared/byte_offsets.py`
- `tools/cq/search/_shared/contracts.py`
- `tools/cq/search/_shared/runtime.py`
- `tests/unit/cq/search/test_shared_helpers.py`
- `tests/unit/cq/search/test_shared_contract_boundaries.py`

### Legacy Decommission/Delete Scope

- Delete duplicated helper implementations listed in baseline once imports are redirected.
- Delete `object`-typed runtime fields from serializable request structs in `tools/cq/search/requests.py`.

---

## S3. Consolidate Ripgrep Lane (`rg/`) and Search Inputs

### Goal
Merge fragmented ripgrep execution/decoding/collection modules into a coherent `rg/` subpackage with three modules in the target LOC range.

### Representative Code Snippets

```python
# tools/cq/search/rg/contracts.py
from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct, CqSettingsStruct


class RgRunSettingsV1(CqSettingsStruct, frozen=True):
    pattern: str
    mode: str
    lang_types: tuple[str, ...]
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()


class RgProcessResultV1(CqOutputStruct, frozen=True):
    command: tuple[str, ...]
    timed_out: bool
    returncode: int
    stderr: str
    events: tuple[dict[str, object], ...] = msgspec.field(default_factory=tuple)
```

```python
# tools/cq/search/rg/codec.py
from __future__ import annotations

import msgspec

_TYPED_DECODER = msgspec.json.Decoder(type=dict[str, object])


def decode_event(line: bytes) -> dict[str, object] | None:
    try:
        return _TYPED_DECODER.decode(line)
    except (msgspec.DecodeError, msgspec.ValidationError):
        return None
```

### Files to Edit

- `tools/cq/search/adapter.py`
- `tools/cq/search/collector.py`
- `tools/cq/search/rg_events.py`
- `tools/cq/search/rg_native.py`
- `tools/cq/search/timeout.py`
- `tools/cq/search/requests.py`

### New Files to Create

- `tools/cq/search/rg/contracts.py`
- `tools/cq/search/rg/codec.py`
- `tools/cq/search/rg/runner.py`
- `tools/cq/search/rg/collector.py`
- `tests/unit/cq/search/rg/test_contracts.py`
- `tests/unit/cq/search/rg/test_codec.py`
- `tests/unit/cq/search/rg/test_runner.py`
- `tests/unit/cq/search/rg/test_collector.py`

### Legacy Decommission/Delete Scope

- Delete `tools/cq/search/rg_events.py` after migration.
- Delete `tools/cq/search/rg_native.py` after migration.
- Delete thin legacy wrappers in `tools/cq/search/adapter.py` once callsites import `rg/` directly.

---

## S4. Split Smart Search Orchestration into `pipeline/` Modules

### Goal
Decompose `smart_search.py` and `partition_pipeline.py` into domain modules (candidate phase, classification phase, enrichment phase, summary/sections, semantic post-processing) each within target LOC.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/orchestrator.py
from __future__ import annotations

from tools.cq.search.pipeline.types import SearchContext, SearchResultAssembly
from tools.cq.search.pipeline.candidates import run_candidate_phase
from tools.cq.search.pipeline.classification import run_classification_phase
from tools.cq.search.pipeline.assembly import assemble_result


def smart_search(ctx: SearchContext):
    candidates = run_candidate_phase(ctx)
    enriched = run_classification_phase(ctx, candidates)
    assembly = SearchResultAssembly(context=ctx, matches=enriched)
    return assemble_result(assembly)
```

### Files to Edit

- `tools/cq/search/smart_search.py`
- `tools/cq/search/partition_pipeline.py`
- `tools/cq/search/pipeline.py`
- `tools/cq/search/section_builder.py`
- `tools/cq/search/candidate_normalizer.py`
- `tools/cq/search/context.py`
- `tools/cq/search/context_window.py`

### New Files to Create

- `tools/cq/search/pipeline/types.py`
- `tools/cq/search/pipeline/candidates.py`
- `tools/cq/search/pipeline/classification.py`
- `tools/cq/search/pipeline/enrichment.py`
- `tools/cq/search/pipeline/summary.py`
- `tools/cq/search/pipeline/assembly.py`
- `tests/unit/cq/search/pipeline/test_candidates.py`
- `tests/unit/cq/search/pipeline/test_classification.py`
- `tests/unit/cq/search/pipeline/test_enrichment.py`
- `tests/unit/cq/search/pipeline/test_assembly.py`

### Legacy Decommission/Delete Scope

- Delete monolithic orchestration blocks from `tools/cq/search/smart_search.py` after extraction.
- Delete `_PartitionScopeContext`/`_EnrichmentMiss*` internals from `tools/cq/search/partition_pipeline.py` once moved.

---

## S5. Split Python Lane into Focused Modules

### Goal
Refactor Python enrichment and native resolution into semantically cohesive modules: extractors, staging/orchestration, native resolution indexes, and agreement builders.

### Representative Code Snippets

```python
# tools/cq/search/python/stages.py
from __future__ import annotations

from ast_grep_py import SgNode


def extract_signature_stage(node: SgNode) -> dict[str, object]:
    signature = node.text().split("{", maxsplit=1)[0].strip()
    return {"signature": signature[:200]} if signature else {}
```

```python
# tools/cq/search/python/ast_grep_rules.py
from __future__ import annotations

from ast_grep_py import SgNode


def find_import_aliases(node: SgNode) -> list[SgNode]:
    # Config-mode query enables constraints/utils style matching.
    return node.find_all({
        "rule": {"pattern": "import $X as $Y"},
        "constraints": {"Y": {"regex": "^[A-Za-z_][A-Za-z0-9_]*$"}},
    })
```

### Files to Edit

- `tools/cq/search/python_enrichment.py`
- `tools/cq/search/python_native_resolution.py`
- `tools/cq/search/python_analysis_session.py`
- `tools/cq/search/python_semantic_signal.py`
- `tools/cq/search/requests.py`

### New Files to Create

- `tools/cq/search/python/stages.py`
- `tools/cq/search/python/extractors.py`
- `tools/cq/search/python/orchestrator.py`
- `tools/cq/search/python/resolution_index.py`
- `tools/cq/search/python/resolution_payload.py`
- `tools/cq/search/python/agreement.py`
- `tools/cq/search/python/ast_grep_rules.py`
- `tests/unit/cq/search/python/test_stages.py`
- `tests/unit/cq/search/python/test_extractors.py`
- `tests/unit/cq/search/python/test_orchestrator.py`
- `tests/unit/cq/search/python/test_resolution_index.py`
- `tests/unit/cq/search/python/test_resolution_payload.py`
- `tests/unit/cq/search/python/test_ast_grep_rules.py`

### Legacy Decommission/Delete Scope

- Delete monolithic stage functions from `tools/cq/search/python_enrichment.py` once moved.
- Delete duplicated byte-offset and AST-index helpers from `tools/cq/search/python_native_resolution.py` after shared helper migration.

---

## S6. Consolidate Rust Lane and Evidence Modules

### Goal
Split `tree_sitter_rust.py` into runtime/extractor/query-pack modules and merge tiny Rust evidence modules/contracts into stable files inside `rust/`.

### Representative Code Snippets

```python
# tools/cq/search/rust/evidence.py
from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct


class RustMacroEvidenceV1(CqOutputStruct, frozen=True):
    macro_name: str
    confidence: str = "medium"


def macro_rows(calls: list[str]) -> tuple[RustMacroEvidenceV1, ...]:
    return tuple(
        RustMacroEvidenceV1(macro_name=name[:-1])
        for name in calls
        if isinstance(name, str) and name.endswith("!")
    )
```

### Files to Edit

- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/rust_enrichment.py`
- `tools/cq/search/rust_macro_expansion_bridge.py`
- `tools/cq/search/rust_macro_expansion_contracts.py`
- `tools/cq/search/rust_module_graph.py`
- `tools/cq/search/rust_module_graph_contracts.py`

### New Files to Create

- `tools/cq/search/rust/query_runtime.py`
- `tools/cq/search/rust/extractors.py`
- `tools/cq/search/rust/fact_payloads.py`
- `tools/cq/search/rust/evidence.py`
- `tests/unit/cq/search/rust/test_query_runtime.py`
- `tests/unit/cq/search/rust/test_extractors.py`
- `tests/unit/cq/search/rust/test_fact_payloads.py`
- `tests/unit/cq/search/rust/test_evidence.py`

### Legacy Decommission/Delete Scope

- Delete `tools/cq/search/rust_macro_expansion_bridge.py` after `rust/evidence.py` migration.
- Delete `tools/cq/search/rust_module_graph.py` after `rust/evidence.py` migration.
- Delete tiny `rust_*_contracts.py` modules once contracts are merged into `rust/evidence.py`.

---

## S7. Repackage Tree-Sitter Core and Contracts

### Goal
Create a `tree_sitter/` package with clear boundaries (`core`, `query`, `contracts`, `schema`, `diagnostics`, `structural`, `python_lane`, `rust_lane`) and consolidate contract fragmentation into target-sized modules.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/query/runtime.py
from __future__ import annotations

from tree_sitter import QueryCursor


def run_windowed_matches(query, root, *, start: int, end: int, match_limit: int):
    cursor = QueryCursor(query, match_limit=match_limit)
    if hasattr(cursor, "set_byte_range"):
        # Enable behind capability flag + tests because upstream instability was observed.
        cursor.set_byte_range(start, end)
    return cursor.matches(root)
```

```python
# tools/cq/search/tree_sitter/schema/runtime_ids.py
from __future__ import annotations


def build_runtime_ids(language) -> dict[str, int]:
    out: dict[str, int] = {}
    for kind in ("identifier", "call", "function_definition"):
        try:
            out[kind] = int(language.id_for_node_kind(kind, True))
        except (RuntimeError, TypeError, ValueError):
            continue
    return out
```

### Files to Edit

- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/tree_sitter_runtime_contracts.py`
- `tools/cq/search/tree_sitter_parse_session.py`
- `tools/cq/search/tree_sitter_parse_contracts.py`
- `tools/cq/search/tree_sitter_parser_controls.py`
- `tools/cq/search/tree_sitter_stream_source.py`
- `tools/cq/search/tree_sitter_change_windows.py`
- `tools/cq/search/tree_sitter_change_windows_contracts.py`
- `tools/cq/search/tree_sitter_query_registry.py`
- `tools/cq/search/tree_sitter_query_planner.py`
- `tools/cq/search/tree_sitter_query_planner_contracts.py`
- `tools/cq/search/tree_sitter_query_specialization.py`
- `tools/cq/search/tree_sitter_query_specialization_contracts.py`
- `tools/cq/search/tree_sitter_pack_contracts.py`
- `tools/cq/search/tree_sitter_pack_metadata.py`
- `tools/cq/search/tree_sitter_query_contracts.py`
- `tools/cq/search/tree_sitter_custom_predicates.py`
- `tools/cq/search/tree_sitter_custom_predicate_contracts.py`
- `tools/cq/search/tree_sitter_node_schema.py`
- `tools/cq/search/tree_sitter_node_codegen.py`
- `tools/cq/search/tree_sitter_language_registry.py`
- `tools/cq/search/tree_sitter_diagnostics.py`
- `tools/cq/search/tree_sitter_diagnostics_contracts.py`
- `tools/cq/search/tree_sitter_recovery_hints.py`
- `tools/cq/search/tree_sitter_structural_export.py`
- `tools/cq/search/tree_sitter_structural_contracts.py`
- `tools/cq/search/tree_sitter_token_export.py`
- `tools/cq/search/tree_sitter_artifact_contracts.py`
- `tools/cq/search/tree_sitter_event_contracts.py`
- `tools/cq/search/tree_sitter_work_queue.py`
- `tools/cq/search/tree_sitter_work_queue_contracts.py`
- `tools/cq/search/tree_sitter_adaptive_runtime.py`
- `tools/cq/search/tree_sitter_adaptive_runtime_contracts.py`
- `tools/cq/search/tree_sitter_grammar_drift.py`
- `tools/cq/search/tree_sitter_grammar_drift_contracts.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_python_facts.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_injections.py`
- `tools/cq/search/tree_sitter_injection_runtime.py`
- `tools/cq/search/tree_sitter_injection_contracts.py`
- `tools/cq/search/tree_sitter_rust_injection_profiles.py`
- `tools/cq/search/tree_sitter_rust_bundle.py`

### New Files to Create

- `tools/cq/search/tree_sitter/core/runtime.py`
- `tools/cq/search/tree_sitter/core/parse.py`
- `tools/cq/search/tree_sitter/core/windows.py`
- `tools/cq/search/tree_sitter/query/planner.py`
- `tools/cq/search/tree_sitter/query/registry.py`
- `tools/cq/search/tree_sitter/query/predicates.py`
- `tools/cq/search/tree_sitter/query/lint.py`
- `tools/cq/search/tree_sitter/contracts/runtime.py`
- `tools/cq/search/tree_sitter/contracts/query.py`
- `tools/cq/search/tree_sitter/contracts/diagnostics.py`
- `tools/cq/search/tree_sitter/contracts/structural.py`
- `tools/cq/search/tree_sitter/schema/runtime.py`
- `tools/cq/search/tree_sitter/schema/generated.py`
- `tools/cq/search/tree_sitter/diagnostics/runtime.py`
- `tools/cq/search/tree_sitter/structural/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/facts.py`
- `tools/cq/search/tree_sitter/rust_lane/injections.py`
- `tests/unit/cq/search/tree_sitter/test_core_runtime.py`
- `tests/unit/cq/search/tree_sitter/test_core_parse.py`
- `tests/unit/cq/search/tree_sitter/test_query_planner.py`
- `tests/unit/cq/search/tree_sitter/test_query_registry.py`
- `tests/unit/cq/search/tree_sitter/test_query_predicates.py`
- `tests/unit/cq/search/tree_sitter/test_contracts_runtime.py`
- `tests/unit/cq/search/tree_sitter/test_contracts_structural.py`
- `tests/unit/cq/search/tree_sitter/test_schema_runtime.py`
- `tests/unit/cq/search/tree_sitter/test_python_lane_runtime.py`
- `tests/unit/cq/search/tree_sitter/test_rust_lane_runtime.py`

### Legacy Decommission/Delete Scope

- Delete fragmented `tree_sitter_*_contracts.py` files once consolidated modules are in place.
- Delete old root-level `tree_sitter_*.py` modules after compatibility shim phase.

---

## S8. Consolidate Semantic Front Door, Diagnostics, and Object Views

### Goal
Move semantic front-door logic, capability diagnostics, and object view assembly into cohesive packages and integrate path/include normalization and consistent serialization patterns.

### Representative Code Snippets

```python
# tools/cq/search/semantic/path_filters.py
from __future__ import annotations

from pathspec import PathSpec


def compile_globs(globs: list[str]) -> PathSpec:
    return PathSpec.from_lines("gitwildmatch", globs)
```

```python
# tools/cq/search/semantic/front_door.py
from __future__ import annotations

from tools.cq.search.semantic.contracts import SemanticOutcomeV1


def fail_open(reason: str) -> SemanticOutcomeV1:
    return SemanticOutcomeV1(payload=None, timed_out=False, failure_reason=reason)
```

### Files to Edit

- `tools/cq/search/language_front_door_adapter.py`
- `tools/cq/search/language_front_door_contracts.py`
- `tools/cq/search/language_front_door_pipeline.py`
- `tools/cq/search/language_root_resolution.py`
- `tools/cq/search/semantic_contract_state.py`
- `tools/cq/search/semantic_planes_static.py`
- `tools/cq/search/semantic_request_budget.py`
- `tools/cq/search/multilang_diagnostics.py`
- `tools/cq/search/object_resolver.py`
- `tools/cq/search/object_sections.py`
- `tools/cq/search/object_fact_attachment.py`
- `tools/cq/search/object_resolution_contracts.py`
- `tools/cq/search/object_occurrence_contracts.py`

### New Files to Create

- `tools/cq/search/semantic/contracts.py`
- `tools/cq/search/semantic/front_door.py`
- `tools/cq/search/semantic/state.py`
- `tools/cq/search/semantic/diagnostics.py`
- `tools/cq/search/semantic/path_filters.py`
- `tools/cq/search/objects/contracts.py`
- `tools/cq/search/objects/resolve.py`
- `tools/cq/search/objects/render.py`
- `tests/unit/cq/search/semantic/test_contracts.py`
- `tests/unit/cq/search/semantic/test_front_door.py`
- `tests/unit/cq/search/semantic/test_diagnostics.py`
- `tests/unit/cq/search/semantic/test_path_filters.py`
- `tests/unit/cq/search/objects/test_resolve.py`
- `tests/unit/cq/search/objects/test_render.py`

### Legacy Decommission/Delete Scope

- Delete root-level `language_front_door_*` modules after callsite migration.
- Delete root-level `semantic_*` modules after callsite migration.
- Delete root-level `object_*` modules after contracts/render/resolve migration.

---

## S9. Msgspec Capability Uplift and Final API Cleanup

### Goal
Adopt additional `msgspec` capabilities in hot paths, finalize package exports, and remove temporary compatibility modules.

### Representative Code Snippets

```python
# tools/cq/search/_shared/codec.py
from __future__ import annotations

import msgspec

_JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
_JSON_DECODER = msgspec.json.Decoder(type=dict[str, object])


def encode_payload(payload: dict[str, object]) -> bytes:
    return _JSON_ENCODER.encode(payload)


def decode_payload(raw: bytes) -> dict[str, object]:
    return _JSON_DECODER.decode(raw)
```

```python
# tools/cq/search/_shared/conversion.py
from __future__ import annotations

import msgspec


def from_runtime(obj: object, *, tp: object):
    return msgspec.convert(obj, type=tp, from_attributes=True, strict=False)
```

### Files to Edit

- `tools/cq/search/__init__.py`
- `tools/cq/search/contracts.py`
- `tools/cq/search/enrichment/contracts.py`
- `tools/cq/search/enrichment/core.py`
- `tools/cq/search/partition_pipeline.py`
- `tools/cq/search/smart_search.py`

### New Files to Create

- `tools/cq/search/_shared/codec.py`
- `tools/cq/search/_shared/conversion.py`
- `tests/unit/cq/search/test_shared_codec.py`
- `tests/unit/cq/search/test_shared_conversion.py`
- `tests/unit/cq/search/test_public_api_exports.py`

### Legacy Decommission/Delete Scope

- Delete compatibility import shims from `tools/cq/search/__init__.py` once all internal/external imports are migrated.
- Delete temporary transition modules created during S3-S8.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S2, S3, S4)

- Delete legacy root-level ripgrep modules (`tools/cq/search/rg_native.py`, `tools/cq/search/rg_events.py`) because `rg/runner.py` and `rg/codec.py` supersede them.
- Delete duplicated helper implementations in `python_enrichment.py`, `rust_enrichment.py`, `tree_sitter_python.py`, `tree_sitter_rust.py`, `python_analysis_session.py`, `python_native_resolution.py`, and `language_front_door_pipeline.py` because `_shared/helpers.py` and `_shared/byte_offsets.py` become canonical.

### Batch D2 (after S5, S6, S7)

- Delete monolithic lane files `tools/cq/search/python_enrichment.py`, `tools/cq/search/python_native_resolution.py`, `tools/cq/search/tree_sitter_python.py`, and `tools/cq/search/tree_sitter_rust.py` after split modules are fully wired and tested.
- Delete fragmented tree-sitter contract files (`tools/cq/search/tree_sitter_*_contracts.py`) because consolidated `tree_sitter/contracts/*.py` replace them.

### Batch D3 (after S8, S9)

- Delete root-level semantic/object modules (`language_front_door_*`, `semantic_*`, `object_*`) after package cutover to `semantic/` and `objects/`.
- Remove all compatibility re-exports from `tools/cq/search/__init__.py` once no callsites import old paths.

## Implementation Sequence

1. S1 first to lock target topology and LOC guardrails before moving code.
2. S2 second so all later scopes use shared helpers and the corrected serialization boundary.
3. S3 third to stabilize ripgrep input/output contracts and reduce noise in smart-search split.
4. S4 fourth because smart-search orchestration is the central integration point.
5. S5 and S6 next (Python then Rust) to split oversized language lanes while S4 orchestration seams are still fresh.
6. S7 after lane splits to consolidate shared tree-sitter platform code/contracts with fewer cross-cutting conflicts.
7. S8 after tree-sitter/python/rust stabilization to consolidate semantic and object layers against final payload shapes.
8. S9 last for codec/perf uplift, API cleanup, and compatibility shim removal.
9. Execute decommission batches D1-D3 immediately after their prerequisite scopes complete.

## Implementation Checklist

- [x] S1. Establish Target Topology and LOC Guardrails
- [x] S2. Centralize Shared Helpers and Runtime-vs-Serializable Contracts
- [x] S3. Consolidate Ripgrep Lane (`rg/`) and Search Inputs
- [x] S4. Split Smart Search Orchestration into `pipeline/` Modules
- [x] S5. Split Python Lane into Focused Modules
- [x] S6. Consolidate Rust Lane and Evidence Modules
- [x] S7. Repackage Tree-Sitter Core and Contracts
- [x] S8. Consolidate Semantic Front Door, Diagnostics, and Object Views
- [x] S9. Msgspec Capability Uplift and Final API Cleanup
- [x] D1 Decommission Batch
- [x] D2 Decommission Batch
- [x] D3 Decommission Batch
