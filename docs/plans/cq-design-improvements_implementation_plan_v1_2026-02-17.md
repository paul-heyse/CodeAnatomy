# CQ Design Improvements: Comprehensive Implementation Plan v1

**Date:** 2026-02-17
**Author:** Design synthesis from 5 agent design reviews
**Synthesis Source:** `docs/reviews/design_review_synthesis_2026-02-17.md`
**Scope:** `tools/cq/` — ~80K LOC, 368 files, 14 subsystems
**Phase Count:** 6 phases, 34 scope items, 9 decommission batches

---

## 1. Scope Summary

This plan integrates all recommended improvements from five concurrent design reviews of the
`tools/cq` codebase. The reviews covered: Core Foundation, Search Infrastructure, Search Pipeline
and Enrichment, Query Engine, and Integration Shell. The synthesis heatmap identifies seven
systemic weaknesses across 24 design principles (1-3 scale):

| Rank | Principle | Avg Score | Primary Weakness |
|------|-----------|-----------|------------------|
| 1 | P7 DRY | 1.0 | Cache policy duplication, lane canonicalization, enrichment paths |
| 2 | P2 Separation of concerns | 1.4 | `assembly.py`, `executor_runtime.py`, and macros as God modules |
| 3 | P3 SRP | 1.6 | God modules with 5+ concerns per file |
| 4 | P21 Least astonishment | 1.6 | `SummaryEnvelopeV1` collision, `_evidence_to_bucket` naming mismatch |
| 5 | P10 Illegal states | 1.8 | Mutable core schema types; bare `str` where `Literal` fits |
| 6 | P11 CQS | 1.8 | `_apply_entity_handlers` mutates and queries simultaneously |
| 7 | P1 Information hiding | 1.8 | Reverse deps from `core/` into `search/` |

The 34 scope items address each weakness directly, organized into 6 phases by risk level and
dependency order. The 9 decommission batches schedule deletion of the artifacts that become
dead code after each phase completes.

This is a design-phase hard-cutover plan: no compatibility shims, no deprecation warnings, and
no transitional aliases that persist beyond the scope item where they are introduced.

---

## 2. Design Principles (Non-Negotiable Constraints)

These constraints govern every change in the plan. Any implementation that violates them must be
revised before merging.

1. **No compatibility shims** — When code moves, update all callers immediately. Re-export
   wrappers that persist beyond the scope item are forbidden.

2. **Every new module has a test** — All new files under `tools/cq/` must have corresponding
   tests under `tests/unit/cq/`. The test must exercise the new module's public API.

3. **Freeze one type at a time** — S16 freezes types incrementally: `DetailPayload` first
   (smallest blast radius), then `Finding`, then `SummaryEnvelopeV1`, then `CqResult`.

4. **Use `msgspec.structs.replace()`** — All mutations on frozen structs use
   `msgspec.structs.replace()`. Use of `object.__setattr__()` or similar workarounds is
   forbidden.

5. **Preserve determinism** — All changes must maintain the existing deterministic ordering and
   content-hashed caching invariants (P17 and P18 scored 2.8/2.6 in the heatmap — the highest-
   scoring principles — and must not regress).

6. **Preserve composition over inheritance** — No new class hierarchies. Continue using
   protocols, adapters, and function composition (P13 scored 2.8).

7. **msgspec contracts at boundaries** — All new cross-module structs use
   `CqStruct(frozen=True)` or `CqOutputStruct(frozen=True)` bases.

8. **No pydantic in CQ hot paths** — Per CLAUDE.md and AGENTS.md, use msgspec in
   `tools/cq/search`, `tools/cq/query`, and `tools/cq/run`.

9. **No stale migration baselines** — Mutation and file-count baselines must be regenerated
   immediately before S16 (using `rg` and `ast-grep`) and recorded in the PR description.

---

## 3. Current Baseline (Verified Observations)

The following are verified defects or technical debt items visible in the current codebase,
confirming the scope items are grounded in concrete code locations.

- `Finding(msgspec.Struct)` at `tools/cq/core/schema.py:240` lacks `frozen=True` — any
  consumer can mutate findings after construction.
- `CqResult(msgspec.Struct)` at `tools/cq/core/schema.py:350` has mutable `list[Finding]`
  fields — consumers can append without going through a builder.
- `DetailPayload.__setitem__` at `tools/cq/core/schema.py:130` enables dict-like mutation on
  a struct — violates P10 illegal states.
- `SummaryEnvelopeV1` at `tools/cq/core/summary_contract.py:23` lacks `frozen=True`;
  `apply_summary_mapping()` uses `setattr()` mutation.
- A second `SummaryEnvelopeV1` class exists in `tools/cq/core/summary_contracts.py` (the
  3-field output boundary envelope) with conflicting semantics — a direct naming collision.
- `CqStruct(frozen=True)` convention exists at `tools/cq/core/structs.py:8-43` but is not
  used by the 4 core schema types listed above.
- `assign_result_finding_ids()` at `tools/cq/core/schema.py:461-490` already demonstrates
  the correct `msgspec.structs.replace()` pattern — the precedent exists.
- `MacroResultBuilder` at `tools/cq/macros/result_builder.py` exists but internally mutates
  `result.key_findings`/`result.sections` via `.append()`/`.extend()`.
- Current mutation baseline (2026-02-17): 61 direct macro mutation sites across 12 macro files
  (`result.(key_findings|evidence|sections).(append|extend|insert)`), and 79 total mutation-like
  sites across 18 `tools/cq` files (including orchestration merges).
- `executor_runtime.py` is 1126 LOC with 28 imports mixing at least 5 concerns (scan
  orchestration, cache fragment management, entity execution, pattern execution, summary
  construction).
- `assembly.py` is 685 LOC mixing object resolution, neighborhood preview, cache maintenance,
  enrichment assembly, and section building.
- `_evidence_to_bucket` in `smart_search.py` returns `"medium"` while
  `smart_search_sections.py` uses `"med"` — an active naming inconsistency affecting
  categorization output.
- Two near-identical lane canonicalization functions in
  `tools/cq/search/tree_sitter/contracts/lane_payloads.py` (lines 39-68) differ only in the
  `msgspec.convert` target type.
- `_accumulate_runtime_flags` is duplicated verbatim in `python_adapter.py` (lines 116-126)
  and `rust_adapter.py` (lines 100-110).
- `CacheRuntimePolicy` in `tools/cq/core/runtime/execution_policy.py` (lines 58-72) is a
  structural duplicate of `CqCachePolicyV1` in `tools/cq/core/cache/policy.py`.
- `_env_namespace_ttls()` in `execution_policy.py` duplicates `_resolve_namespace_ttl_from_env()`
  in `policy.py`.
- `_PY_RESOLUTION_KEYS`, `_PY_BEHAVIOR_KEYS`, `_PY_STRUCTURAL_KEYS` frozensets in
  `tools/cq/search/enrichment/core.py` (lines 182-238) are hardcoded — will drift from fact
  structs.
- `enrichment_status: str` in `lane_payloads.py` accepts any string; valid values are only
  `"applied"`, `"degraded"`, `"skipped"`.
- `resolution_quality: str` in `tools/cq/search/objects/render.py` accepts any string; valid
  values are only `"strong"`, `"medium"`, `"weak"`.
- Python enrichment has 5-stage timing instrumentation; Rust has only 2-stage timing
  (`query_pack`, `payload_build`).
- `_SESSIONS` module-level dict in `tools/cq/search/tree_sitter/core/parse.py` has no DI seam
  — makes unit testing impossible without module-level monkeypatching.
- Parser `valid_entities` tuple in `tools/cq/query/parser.py` (lines 426-439) duplicates the
  IR `EntityType` `Literal` definition — a DRY violation that will cause silent drift.
- Dispatch chain `executor_entity` → `executor` → `executor_runtime` adds two indirection
  layers with no behavior — a KISS violation.
- `IncrementalEnrichmentModeV1` lives in `tools/cq/search/pipeline/enrichment_contracts.py`
  but is imported by `tools/cq/core/services.py` — a reverse dependency from `core/` into
  `search/`.

---

## 4. Per-Scope-Item Sections

---

### Phase 1: Safe Deletions and Renames (S1-S6)

These items carry zero risk of behavioral change. They delete dead code, eliminate naming
collisions, and tighten type constraints. Each can be done independently within the phase.

---

#### S1. Delete Duplicate Search Pipeline Helpers

**Goal:** Remove `_category_message` and `_evidence_to_bucket` from `smart_search.py`
(duplicates of `smart_search_sections.py`). Fix the active `"medium"` vs `"med"` bucket naming
mismatch. Delete dead `_apply_prefetched_search_semantic_outcome` from `search_semantic.py`.

**Files to Edit:**

- `tools/cq/search/pipeline/smart_search.py` — delete lines 270-323
- `tools/cq/search/pipeline/search_semantic.py` — delete lines 191-213
- `tools/cq/search/pipeline/smart_search_sections.py` — standardize `"med"` to `"medium"` at
  lines 30-35
- Any file importing `_evidence_to_bucket` or `_category_message` from `smart_search.py` must
  update its import to `smart_search_sections.py`

**Details:**

The two functions at `smart_search.py:270-323` are duplicates of functions already implemented
in `smart_search_sections.py`. The `_evidence_to_bucket` function in `smart_search.py` returns
`"medium"` while the canonical implementation in `smart_search_sections.py` returns `"med"`.
This inconsistency produces different bucket labels depending on which code path executes. The
fix standardizes on `"medium"` (the more descriptive string) in `smart_search_sections.py`.

The `_apply_prefetched_search_semantic_outcome` function at `search_semantic.py:191-213` is dead
code — no caller references it. It can be deleted without updating any caller.

**Decommission:** D1 batch follows immediately after this scope item completes.

---

#### S2. Rename SummaryEnvelopeV1 Collision

**Goal:** Rename `tools/cq/core/summary_contracts.py:SummaryEnvelopeV1` (the 3-field output
boundary envelope) to `SummaryOutputEnvelopeV1`. The primary `SummaryEnvelopeV1` in
`tools/cq/core/summary_contract.py` (the 60+ field summary struct) keeps its name.

**Files to Edit:**

- `tools/cq/core/summary_contracts.py` — rename class at line 14 from `SummaryEnvelopeV1` to
  `SummaryOutputEnvelopeV1`
- All files that import `SummaryEnvelopeV1` from `summary_contracts.py` must update to
  `SummaryOutputEnvelopeV1`

**Current Code (to change):**

```python
# tools/cq/core/summary_contracts.py
class SummaryEnvelopeV1(CqStrictOutputStruct, frozen=True):
    summary: dict[str, Any] = msgspec.field(default_factory=dict)
    diagnostics: list[dict[str, Any]] = msgspec.field(default_factory=list)
    telemetry: dict[str, Any] = msgspec.field(default_factory=dict)
```

**Target Code:**

```python
# tools/cq/core/summary_contracts.py
class SummaryOutputEnvelopeV1(CqStrictOutputStruct, frozen=True):
    summary: dict[str, Any] = msgspec.field(default_factory=dict)
    diagnostics: list[dict[str, Any]] = msgspec.field(default_factory=list)
    telemetry: dict[str, Any] = msgspec.field(default_factory=dict)
```

**Why This Is Safe:** The two classes are in different files with different import paths. There
is no runtime ambiguity — only a readability hazard where `from summary_contracts import
SummaryEnvelopeV1` and `from summary_contract import SummaryEnvelopeV1` could be confused by
readers and linters alike.

---

#### S3. Remove Query Dispatch Facades

**Goal:** Delete `tools/cq/query/executor.py` and `tools/cq/query/executor_dispatch.py`. Update
`executor_entity.py` and `executor_pattern.py` to import directly from `executor_runtime.py`.
Rename `_`-prefixed public functions to proper public names.

**Files to Delete (after updating all callers):**

- `tools/cq/query/executor.py`
- `tools/cq/query/executor_dispatch.py`

**Files to Edit:**

- `tools/cq/query/executor_entity.py` — change imports from `executor.py` to
  `executor_runtime.py`
- `tools/cq/query/executor_pattern.py` — change imports from `executor.py` to
  `executor_runtime.py`
- All files importing from `executor_dispatch.py` — update to import from
  `executor_entity.py` or `executor_pattern.py` directly

**Dispatch Chain to Eliminate:**

The current chain is:
```
executor_entity.py  ──imports──>  executor.py  ──imports──>  executor_runtime.py
executor_dispatch.py ──imports──> executor_entity.py / executor_pattern.py
```

The target is:
```
executor_entity.py  ──imports──>  executor_runtime.py
executor_pattern.py ──imports──>  executor_runtime.py
(caller code)       ──imports──>  executor_entity.py / executor_pattern.py
```

**Current `executor.py` re-exports (all must be wired directly to `executor_runtime.py`):**

- `_execute_entity_query`
- `_execute_pattern_query`
- `_collect_match_spans`
- `_execute_ast_grep_rules`
- `_filter_records_by_spans`
- `EntityExecutionState`
- `PatternExecutionState`
- `ExecutePlanRequestV1`
- `execute_entity_query_from_records`
- `execute_pattern_query_with_files`
- `execute_plan`
- `rg_files_with_matches`

**Current `executor_dispatch.py` functions (to be inlined into callers):**

`executor_dispatch.py` defines `execute_entity_query(ctx)` and `execute_pattern_query(ctx)` as
thin wrappers that lazy-import from `executor_entity.py` and `executor_pattern.py`. These lazy
imports exist to avoid circular dependencies that were present in an earlier design and are no
longer needed.

**Decommission:** D2 batch deletes both facade files after S3 completes.

---

#### S4. Add Contract Constraints

**Goal:** Add `NonEmptyStr` to `Finding.category`, `NonNegativeFloat` to
`RunMeta.started_ms`, `RunMeta.elapsed_ms`, and `RunMeta.run_created_ms`. Add executor
entry-point assertions to guard against incorrect dispatch.

**Files to Edit:**

- `tools/cq/core/schema.py` — `Finding` at line 263, `RunMeta` at lines 341-342
- `tools/cq/core/contracts_constraints.py` — add `NonNegativeFloat`
- `tools/cq/query/executor_runtime.py` — add assertions in `_execute_entity_query` (line 783)
  and `_execute_pattern_query` (line 853)

**Existing constraint types at `tools/cq/core/contracts_constraints.py:12-16`:**

```python
PositiveInt = Annotated[int, msgspec.Meta(gt=0)]
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]
PositiveFloat = Annotated[float, msgspec.Meta(gt=0.0)]
NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]
BoundedRatio = Annotated[float, msgspec.Meta(ge=0.0, le=1.0)]
```

**New constraint to add:**

```python
# tools/cq/core/contracts_constraints.py
NonNegativeFloat = Annotated[float, msgspec.Meta(ge=0.0)]
```

**Code changes for `Finding`:**

```python
# tools/cq/core/schema.py
from tools.cq.core.contracts_constraints import NonEmptyStr, NonNegativeFloat

class Finding(msgspec.Struct):
    category: NonEmptyStr  # was: str
    message: str
    # ... remaining fields unchanged
```

**Code changes for `RunMeta`:**

```python
# tools/cq/core/schema.py
class RunMeta(msgspec.Struct):
    macro: str
    argv: list[str]
    root: str
    started_ms: NonNegativeFloat  # was: float
    elapsed_ms: NonNegativeFloat  # was: float
    # ... other fields ...
    run_created_ms: NonNegativeFloat | None = None  # was: float | None
```

**Executor assertions:**

```python
# tools/cq/query/executor_runtime.py
def _execute_entity_query(ctx: ExecutionContext) -> CqResult:
    assert not ctx.plan.is_pattern_query, (
        "_execute_entity_query called with a pattern query plan; "
        "use _execute_pattern_query instead"
    )
    # ... existing body

def _execute_pattern_query(ctx: ExecutionContext) -> CqResult:
    assert ctx.plan.is_pattern_query, (
        "_execute_pattern_query called with an entity query plan; "
        "use _execute_entity_query instead"
    )
    # ... existing body
```

---

#### S5. Tighten Bare String Types to Literal/Enum

**Goal:** Replace `enrichment_status: str` with
`Literal["applied", "degraded", "skipped"]`, replace `resolution_quality: str` with
`Literal["strong", "medium", "weak"]`, and replace `coverage_level: str` with
`Literal["full_signal", "partial_signal", "structural_only"]`. Default `CliContext.output_format` to
`OutputFormat.md`.

**Files to Edit:**

- `tools/cq/search/tree_sitter/contracts/lane_payloads.py` — lines 17 and 27
- `tools/cq/search/objects/render.py` — lines 36 and 73
- `tools/cq/cli_app/context.py` — line 63

**Code changes for `lane_payloads.py`:**

```python
# tools/cq/search/tree_sitter/contracts/lane_payloads.py
from typing import Literal

EnrichmentStatus = Literal["applied", "degraded", "skipped"]

class PythonTreeSitterPayloadV1(CqOutputStruct, frozen=True):
    language: Literal["python"] = "python"
    enrichment_status: EnrichmentStatus = "applied"  # was: str
    # ... remaining fields unchanged

class RustTreeSitterPayloadV1(CqOutputStruct, frozen=True):
    language: Literal["rust"] = "rust"
    enrichment_status: EnrichmentStatus = "applied"  # was: str
    # ... remaining fields unchanged
```

**Code changes for `render.py`:**

```python
# tools/cq/search/objects/render.py
from typing import Literal

ResolutionQuality = Literal["strong", "medium", "weak"]
CoverageLevel = Literal["full_signal", "partial_signal", "structural_only"]

class ResolvedObjectRef(CqOutputStruct, frozen=True):
    # ... other fields ...
    resolution_quality: ResolutionQuality = "weak"  # was: str

class SearchObjectSummaryV1(CqOutputStruct, frozen=True):
    # ... other fields ...
    coverage_level: CoverageLevel = "structural_only"  # was: str
```

**Code changes for `context.py`:**

```python
# tools/cq/cli_app/context.py
class CliContext(CqStruct, frozen=True):
    # ... other fields ...
    output_format: OutputFormat = OutputFormat.md  # was: OutputFormat | None = None
```

The `output_format: OutputFormat | None = None` pattern requires every consumer to handle
`None` and select a default inline, scattering the default decision across all call sites.
Defaulting to `OutputFormat.md` moves the decision to the single correct location.

---

#### S6. Fix Minor Inversions and Add `__all__` Exports

**Goal:** Move `DEFAULT` and `INTERACTIVE` profile constants from
`tools/cq/search/pipeline/profiles.py` to `tools/cq/search/_shared/profiles.py`. Derive parser
validation from IR `Literal` types via `get_args()`. Add `__all__` to `impact.py` and
`options.py`.

**Files to Edit:**

- `tools/cq/search/pipeline/profiles.py` — remove constant definitions, add import from
  `_shared/profiles.py`
- Create `tools/cq/search/_shared/profiles.py` — new file with moved constants
- `tools/cq/search/rg/adapter.py` — update import path
- `tools/cq/query/parser.py` — lines 426-439, replace hardcoded validation tuples
- `tools/cq/macros/impact.py` — add `__all__`
- `tools/cq/cli_app/options.py` — add `__all__`

**Code changes for `parser.py` (derive validation sets from IR):**

```python
# tools/cq/query/parser.py
from typing import get_args
from tools.cq.query.ir import EntityType, StrictnessMode

# Derived at module load — will stay in sync with the IR Literal types automatically.
_VALID_ENTITIES: frozenset[str] = frozenset(get_args(EntityType))
_VALID_STRICTNESS_MODES: frozenset[str] = frozenset(get_args(StrictnessMode))

def _parse_entity_kind(entity_str: str) -> EntityType:
    if entity_str not in _VALID_ENTITIES:
        msg = _invalid_entity_message(entity_str, tuple(_VALID_ENTITIES))
        raise QueryParseError(msg)
    return cast("EntityType", entity_str)
```

The current pattern hardcodes a tuple `("function", "class", "import", ...)` in the parser that
must be manually kept in sync with the `EntityType = Literal["function", "class", "import", ...]`
definition in `ir.py`. The `get_args()` approach makes them a single source of truth.

---

### Phase 2: DRY Consolidation (S7-S10)

These items eliminate code duplication. Each refactor is internal — public APIs are unchanged.

---

#### S7. Unify Lane Canonicalization

**Goal:** Extract a generic `_canonicalize_lane_payload(payload, target_type)` in
`lane_payloads.py`. Both language-specific functions become one-line delegations.

**File to Edit:**

- `tools/cq/search/tree_sitter/contracts/lane_payloads.py` — lines 39-68

**Current state:** Two near-identical functions `canonicalize_python_lane_payload` and
`canonicalize_rust_lane_payload` differ only in the `msgspec.convert` target type
(`PythonTreeSitterPayloadV1` vs `RustTreeSitterPayloadV1`). The duplication means any fix to
the canonicalization logic must be applied twice.

**Target code:**

```python
# tools/cq/search/tree_sitter/contracts/lane_payloads.py
def _canonicalize_lane_payload(
    payload: dict[str, Any],
    target_type: type,
) -> dict[str, Any]:
    """Canonicalize a tree-sitter lane payload against a target contract.

    Parameters
    ----------
    payload : dict[str, Any]
        Raw lane payload dict, possibly containing legacy field names.
    target_type : type
        The target msgspec struct type to validate the payload shape against.

    Returns
    -------
    dict[str, Any]
        Canonicalized payload with normalized field names and coerced row types.
    """
    payload = dict(payload)
    legacy = payload.pop("tree_sitter_diagnostics", None)
    if "cst_diagnostics" not in payload and isinstance(legacy, list):
        payload["cst_diagnostics"] = legacy
    payload["cst_diagnostics"] = _coerce_mapping_rows(payload.get("cst_diagnostics"))
    payload["cst_query_hits"] = _coerce_mapping_rows(payload.get("cst_query_hits"))
    _ = msgspec.convert(payload, type=target_type, strict=False)
    return payload


def canonicalize_python_lane_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Canonicalize a Python tree-sitter lane payload."""
    return _canonicalize_lane_payload(payload, PythonTreeSitterPayloadV1)


def canonicalize_rust_lane_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Canonicalize a Rust tree-sitter lane payload."""
    return _canonicalize_lane_payload(payload, RustTreeSitterPayloadV1)
```

The public API (`canonicalize_python_lane_payload`, `canonicalize_rust_lane_payload`) is
unchanged. Only the shared internal body is extracted.

---

#### S8. Consolidate Enrichment Helpers

**Goal:** Move `_accumulate_runtime_flags` to `enrichment/core.py`. Create shared
`build_tree_sitter_diagnostic_rows` in `enrichment/core.py`. Update both adapters and
`semantic/models.py`.

**Files to Edit:**

- `tools/cq/search/enrichment/core.py` — add the two shared helpers
- `tools/cq/search/enrichment/python_adapter.py` — lines 116-141: remove duplicate, import
  from `core.py`
- `tools/cq/search/enrichment/rust_adapter.py` — lines 62-110: remove duplicate, import from
  `core.py`
- `tools/cq/search/semantic/models.py` — lines 130-180: remove duplicate diagnostic
  construction, import from `core.py`

**Code for `enrichment/core.py` additions:**

```python
# tools/cq/search/enrichment/core.py

def accumulate_runtime_flags(
    *,
    lang_bucket: dict[str, object],
    runtime_payload: object,
) -> None:
    """Accumulate runtime flags from enrichment into language bucket.

    Parameters
    ----------
    lang_bucket : dict[str, object]
        The language-specific bucket dict being built. Modified in place.
    runtime_payload : object
        The raw runtime payload from the enrichment response, may be any type.
    """
    runtime_bucket = lang_bucket.get("query_runtime")
    if not isinstance(runtime_payload, dict) or not isinstance(runtime_bucket, dict):
        return
    if bool(runtime_payload.get("did_exceed_match_limit")):
        runtime_bucket["did_exceed_match_limit"] = (
            int(runtime_bucket.get("did_exceed_match_limit", 0)) + 1
        )
    if bool(runtime_payload.get("cancelled")):
        runtime_bucket["cancelled"] = int(runtime_bucket.get("cancelled", 0)) + 1


def build_tree_sitter_diagnostic_rows(
    rows: object,
    *,
    max_rows: int = 8,
) -> list[dict[str, object]]:
    """Build normalized diagnostic rows from raw tree-sitter CST diagnostics.

    Parameters
    ----------
    rows : object
        Raw diagnostic rows, expected to be a list of mappings.
    max_rows : int, optional
        Maximum number of diagnostic rows to return. Defaults to 8.

    Returns
    -------
    list[dict[str, object]]
        Normalized diagnostic rows with guaranteed `kind`, `message`, `line`, and `col` keys.
    """
    if not isinstance(rows, list):
        return []
    from collections.abc import Mapping

    return [
        {
            "kind": _string_or_none(item.get("kind")) or "tree_sitter",
            "message": _string_or_none(item.get("message")) or "tree-sitter diagnostic",
            "line": item.get("start_line"),
            "col": item.get("start_col"),
        }
        for item in rows[:max_rows]
        if isinstance(item, Mapping)
    ]
```

**Decommission:** D3 batch deletes the duplicate definitions from adapter files after S8 and
S9 complete.

---

#### S9. Consolidate Cache Policy

**Goal:** Make `RuntimeExecutionPolicy.cache` use `CqCachePolicyV1` directly. Remove
`CacheRuntimePolicy`.

**Files to Edit:**

- `tools/cq/core/runtime/execution_policy.py` — delete `CacheRuntimePolicy` at lines 58-72
  and `_env_namespace_ttls()` (duplicate of `_resolve_namespace_ttl_from_env()` in `policy.py`)
- Update `RuntimeExecutionPolicy` to reference `CqCachePolicyV1` instead of
  `CacheRuntimePolicy`
- All files importing `CacheRuntimePolicy` from `execution_policy.py` must update to import
  `CqCachePolicyV1` from `tools/cq/core/cache/policy.py`

**Why:** `CacheRuntimePolicy` is a structural duplicate of `CqCachePolicyV1`. Two cache policy
types with different names but identical semantics create an implicit translation layer
everywhere the types cross a module boundary.

**Target:**

```python
# tools/cq/core/runtime/execution_policy.py
from tools.cq.core.cache.policy import CqCachePolicyV1

class RuntimeExecutionPolicy(CqStruct, frozen=True):
    cache: CqCachePolicyV1 = msgspec.field(default_factory=CqCachePolicyV1)
    # ... other fields unchanged
```

The `_env_namespace_ttls()` function in `execution_policy.py` duplicates the
`_resolve_namespace_ttl_from_env()` function in `policy.py`. Delete the duplicate from
`execution_policy.py` and update any internal callers in that file to use the `policy.py`
version.

---

#### S10. Derive Payload Field Sets from Struct Fields

**Goal:** Replace the static `_PY_RESOLUTION_KEYS`, `_PY_BEHAVIOR_KEYS`, and
`_PY_STRUCTURAL_KEYS` frozensets in `enrichment/core.py` with values derived from fact struct
`__struct_fields__` attributes.

**File to Edit:**

- `tools/cq/search/enrichment/core.py` — lines 182-238

**The problem:** The three frozensets are manually maintained lists of field names that mirror
the fields of fact structs in `python_facts.py`. If a field is added or removed from a fact
struct, the frozenset must be updated separately. The divergence is silent — no compile-time or
test-time check catches the drift.

**Target code:**

```python
# tools/cq/search/enrichment/core.py
from tools.cq.search.enrichment.python_facts import (
    PythonBehaviorFacts,
    PythonCallFacts,
    PythonClassShapeFacts,
    PythonImportFacts,
    PythonResolutionFacts,
    PythonSignatureFacts,
    PythonStructureFacts,
)

# Derived at module load — stays in sync with fact struct definitions automatically.
_PY_RESOLUTION_KEYS: frozenset[str] = frozenset(
    PythonResolutionFacts.__struct_fields__
    + PythonCallFacts.__struct_fields__
    + PythonImportFacts.__struct_fields__
)

_PY_BEHAVIOR_KEYS: frozenset[str] = frozenset(
    PythonBehaviorFacts.__struct_fields__
)

_PY_STRUCTURAL_KEYS: frozenset[str] = frozenset(
    PythonStructureFacts.__struct_fields__
    + PythonSignatureFacts.__struct_fields__
    + PythonClassShapeFacts.__struct_fields__
)
```

The behavior of the enrichment bucket assignment is unchanged — the key sets are identical. The
difference is that adding a field to `PythonResolutionFacts` now automatically adds it to
`_PY_RESOLUTION_KEYS` without a separate manual update.

---

### Phase 3: Structural Extraction (S11-S15)

These items address the God module problem. Each extraction creates new focused modules with
corresponding tests. The existing module is left as a thinner orchestrator.

---

#### S11. Extract from `executor_runtime.py` and `executor_ast_grep.py`

**Goal:** Create `query_scan.py` (~200 LOC), `query_summary.py` (~100 LOC), and
`fragment_cache.py` as shared cache/scan modules for both entity and pattern execution paths.
`executor_runtime.py` and `executor_ast_grep.py` become thin orchestrators.

**Current state:** `executor_runtime.py` is 1126 LOC and `executor_ast_grep.py` is 1000+ LOC,
both mixing scan/caching/assembly concerns with execution flow.

**New files to create:**

- `tools/cq/query/query_scan.py` — scan orchestration
- `tools/cq/query/query_summary.py` — summary construction
- `tools/cq/query/fragment_cache.py` — shared cache fragment management

**Corresponding test files to create:**

- `tests/unit/cq/query/test_query_scan.py`
- `tests/unit/cq/query/test_query_summary.py`
- `tests/unit/cq/query/test_fragment_cache.py`

**Extraction groups:**

| Group | Functions | Destination |
|-------|-----------|-------------|
| Scan | `_scan_entity_records`, `_scan_entity_fragment_misses` | `query_scan.py` |
| Cache (entity) | `_build_entity_fragment_context`, `_decode_entity_fragment_payload`, `_entity_records_from_hits`, `_assemble_entity_records` | `fragment_cache.py` |
| Cache (pattern) | fragment-cache helpers currently in `executor_ast_grep.py` | `fragment_cache.py` |
| Summary | `_build_runmeta`, `_query_mode`, `_query_text`, `_summary_common_for_query`, `_summary_common_for_context`, `_finalize_single_scope_summary` | `query_summary.py` |

After extraction, `executor_runtime.py` retains only:
- `execute_plan` (top-level entry point)
- `execute_entity_query_from_records`
- `execute_pattern_query_with_files`
- `_execute_entity_query`
- `_execute_pattern_query`
- `_apply_entity_handlers`
- `_process_decorator_query`
- `_process_call_query`

After extraction, `executor_ast_grep.py` retains only:

- AST-grep execution orchestration and rule execution
- result assembly wiring via imports from `fragment_cache.py`

**Design invariant:** No new public API. Imports in `executor_runtime.py` and
`executor_ast_grep.py` are updated to import from the new modules; external callers are unchanged.

---

#### S12. Extract from `assembly.py`

**Goal:** Create `target_resolution.py` and `neighborhood_preview.py` from `assembly.py`.
Extract cache maintenance to a post-assembly hook.

**Current state:** `assembly.py` is 685 LOC mixing 5 concerns.

**New files to create:**

- `tools/cq/search/pipeline/target_resolution.py` — object resolution
- `tools/cq/search/pipeline/neighborhood_preview.py` — neighborhood preview construction

**Corresponding test files to create:**

- `tests/unit/cq/search/pipeline/test_target_resolution.py`
- `tests/unit/cq/search/pipeline/test_neighborhood_preview.py`

**Extraction groups:**

| Group | Functions | Destination |
|-------|-----------|-------------|
| Object resolution | `_build_object_candidate_finding`, `_collect_definition_candidates`, `_resolve_primary_target_match`, `_candidate_scope_paths_for_neighborhood` | `target_resolution.py` |
| Neighborhood preview | `_build_structural_neighborhood_preview`, `_build_tree_sitter_neighborhood_preview` | `neighborhood_preview.py` |

After extraction, `assembly.py` retains the top-level `assemble_smart_search_result` function
and the enrichment section assembly code. The extracted functions are imported from their new
homes.

---

#### S13. Extract Pure Analysis from Macros

**Goal:** Create a `tools/cq/analysis/` package with pure analysis functions. Move
`TaintVisitor` (290 LOC) to `analysis/taint.py`, `_parse_signature` to
`analysis/signature.py`, and call-site classification logic to `analysis/calls.py`.

**Current state:**

- `TaintVisitor` at `tools/cq/macros/impact.py:135-427` — a 290-line pure AST visitor
  embedded in a macro file
- `_EXPR_TAINT_HANDLERS` at `impact.py:378-427` — the visitor dispatch table
- `_parse_signature` at `tools/cq/macros/sig_impact.py:69-144`

**New files to create:**

- `tools/cq/analysis/__init__.py`
- `tools/cq/analysis/taint.py`
- `tools/cq/analysis/signature.py`
- `tools/cq/analysis/calls.py`

**Corresponding test files to create:**

- `tests/unit/cq/analysis/test_taint.py`
- `tests/unit/cq/analysis/test_signature.py`
- `tests/unit/cq/analysis/test_calls.py`

**Design:** The macro entry points become thin orchestrators:

```
read file  →  parse AST  →  call pure analysis  →  build result
```

The pure analysis functions in `tools/cq/analysis/` accept AST nodes and return typed results.
They have no dependency on macro infrastructure, `CqResult`, or CLI context. This makes them
independently testable.

---

#### S14. Split `smart_search.py`

**Goal:** Move scoring functions to `relevance.py` and coercion helpers to
`request_parsing.py`. `smart_search.py` retains only the `smart_search()` entry point and
pipeline orchestration.

**Current state:** `smart_search.py` is 745 LOC with:
- `compute_relevance_score` and `KIND_WEIGHTS` at lines 93-248
- 10 `_coerce_*` functions at lines 401-474

**New files to create:**

- `tools/cq/search/pipeline/relevance.py`
- `tools/cq/search/pipeline/request_parsing.py`

**Corresponding test files to create:**

- `tests/unit/cq/search/pipeline/test_relevance.py`
- `tests/unit/cq/search/pipeline/test_request_parsing.py`

**Code skeleton for `relevance.py`:**

```python
# tools/cq/search/pipeline/relevance.py
from __future__ import annotations

from tools.cq.search.pipeline.smart_search_types import EnrichedMatch, MatchCategory

__all__ = ["KIND_WEIGHTS", "compute_relevance_score"]

KIND_WEIGHTS: dict[MatchCategory, float] = {
    "definition": 1.0,
    "callsite": 0.8,
    "import": 0.7,
    "from_import": 0.7,
    "reference": 0.6,
    "assignment": 0.5,
    "annotation": 0.5,
    "text_match": 0.3,
    "docstring_match": 0.2,
    "comment_match": 0.15,
    "string_match": 0.1,
}


def compute_relevance_score(match: EnrichedMatch) -> float:
    """Compute a relevance score for an enriched match.

    Parameters
    ----------
    match : EnrichedMatch
        The enriched match to score.

    Returns
    -------
    float
        Relevance score in [0.0, 1.0]. Higher is more relevant.
    """
    base = KIND_WEIGHTS.get(match.category, 0.3)
    # Boost/penalty factors carry over from smart_search.py implementation.
    return base
```

**Design invariant:** `smart_search.py` imports from `relevance.py` and `request_parsing.py`.
No external caller needs to update its imports since `smart_search.py` still re-exports the
`smart_search()` function as before.

---

#### S15. Fix Core→Search Reverse Dependencies

**Goal:** Move `IncrementalEnrichmentModeV1` to `tools/cq/core/` since it is a runtime policy
enum. Change `SearchServiceRequest.mode` to accept `str | None` and let search parse. Remove
`SettingsFactory` dependency on tree-sitter infrastructure.

**Files to Edit:**

- `tools/cq/core/services.py` — lines 11, 19, 95
- `tools/cq/core/settings_factory.py` — line 18
- Create `tools/cq/core/enrichment_mode.py` (new file) with `IncrementalEnrichmentModeV1`
- `tools/cq/search/pipeline/enrichment_contracts.py` — remove `IncrementalEnrichmentModeV1`
  definition after callers migrate

**The violation:** `tools/cq/core/services.py` imports `IncrementalEnrichmentModeV1` from
`tools/cq/search/pipeline/enrichment_contracts.py`. This creates a dependency from the
foundation layer (`core/`) into the search pipeline layer (`search/pipeline/`), reversing the
intended dependency direction.

**Target:** `IncrementalEnrichmentModeV1` lives in `tools/cq/core/enrichment_mode.py`. The
search pipeline imports it from `core/`. `core/` has no dependency on `search/`.

**Transition plan:**

1. Create `tools/cq/core/enrichment_mode.py` with `IncrementalEnrichmentModeV1`.
2. Update `tools/cq/core/services.py` to import from `core/enrichment_mode.py`.
3. Update all callers in `tools/cq/search/` to import from `core/enrichment_mode.py`.
4. Delete the definition from `enrichment_contracts.py`.

---

### Phase 4: Immutability Migration (S16-S18)

These items are the highest-leverage changes in the plan. They also carry the highest risk
because they break a large number of direct mutation callsites. Current baseline (2026-02-17):
61 macro callsites across 12 files, 79 callsites across 18 `tools/cq` files. The baseline must
be refreshed immediately before S16.

---

#### S16. Freeze Core Schema Types

**Goal:** Make `Finding`, `CqResult`, `SummaryEnvelopeV1`, and `DetailPayload` immutable in
practice, not only by attribute freezing. Replace external mutation with copy-on-write APIs and
builder-local mutable state.

**Files to Edit:**

- `tools/cq/core/schema.py` — `Finding` at 240, `CqResult` at 350, `DetailPayload` at 62
- `tools/cq/core/summary_contract.py` — `SummaryEnvelopeV1` at 23
- All macro/query/orchestration files containing direct result/summary/detail mutation sites
- All query executor files

**Test to create:**

- `tests/unit/cq/core/test_schema_frozen.py`

**Precedent:** `assign_result_finding_ids()` at `schema.py:461-490` already uses
`msgspec.structs.replace()` correctly. This is the pattern to follow everywhere.

**Incremental freeze order (per Design Principle 3):**

1. `DetailPayload` first — smallest blast radius, no field of type `DetailPayload` exists in
   `Finding` or `CqResult`
2. `Finding` second — after `DetailPayload` is frozen
3. `SummaryEnvelopeV1` third — after `Finding` is stable
4. `CqResult` last — after all contained types are frozen

**Code for `DetailPayload` (step 1):**

```python
# tools/cq/core/schema.py
class DetailPayload(msgspec.Struct, omit_defaults=True, frozen=True):
    kind: str | None = None
    score: ScoreDetails | None = None
    data_items: tuple[tuple[str, object], ...] = ()

    def with_entry(self, key: str, value: object) -> DetailPayload:
        """Return a new DetailPayload with the given key set."""
        if key == "kind":
            return msgspec.structs.replace(self, kind=None if value is None else str(value))
        if key in _SCORE_FIELDS:
            base = self.score or ScoreDetails()
            return msgspec.structs.replace(self, score=msgspec.structs.replace(base, **{key: value}))
        items = dict(self.data_items)
        items[key] = value
        return msgspec.structs.replace(self, data_items=tuple(sorted(items.items())))
```

**Deep-immutability note:** `frozen=True` is shallow for list/dict fields. S16 must also convert
public result/summary collection fields to immutable container types at the boundary (`tuple[...]`,
mapping snapshots) or ensure they are never exposed as mutable references.

**Code for `apply_summary_mapping` (step 3):**

```python
# tools/cq/core/summary_contract.py
def apply_summary_mapping(
    summary: SummaryEnvelopeV1,
    mapping: Mapping[str, object] | Iterable[tuple[str, object]],
) -> SummaryEnvelopeV1:
    """Return a new summary with the given mapping applied.

    Parameters
    ----------
    summary : SummaryEnvelopeV1
        The base summary to update.
    mapping : Mapping[str, object] | Iterable[tuple[str, object]]
        Key-value pairs to apply. Keys must be valid struct field names.

    Returns
    -------
    SummaryEnvelopeV1
        New SummaryEnvelopeV1 with the mapping applied.

    Raises
    ------
    TypeError
        If any key is not a string.
    KeyError
        If any key is not a valid SummaryEnvelopeV1 field.
    """
    items = mapping.items() if isinstance(mapping, Mapping) else mapping
    updates: dict[str, object] = {}
    for key_obj, value in items:
        if not isinstance(key_obj, str):
            raise TypeError("Summary update keys must be strings.")
        if key_obj not in summary.__struct_fields__:
            raise KeyError(f"Unknown summary key: {key_obj!r}")
        updates[key_obj] = value
    return msgspec.structs.replace(summary, **updates)
```

**Impact scale:** baseline refreshed before migration (currently 61 macro mutation sites, 79 total
across `tools/cq`). After boundary fields become immutable, direct `.append()`/`.extend()` patterns
on result payloads are disallowed by type contracts and runtime shape.

---

#### S17. Extend MacroResultBuilder

**Goal:** Add semantic methods for all finding/section/evidence additions. Migrate all 12+
macro files to use the builder exclusively.

**File to Edit:**

- `tools/cq/macros/result_builder.py`

**Current state:** `MacroResultBuilder` has `add_findings()`, `add_section()`, and `build()`
but internally uses `.append()` and `.extend()` on mutable lists. After S16 freezes `CqResult`,
the builder must use `msgspec.structs.replace()` internally.

**Target code:**

```python
# tools/cq/macros/result_builder.py
from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import msgspec

from tools.cq.core.schema import (
    CqResult,
    Finding,
    Section,
    assign_result_finding_ids,
)
from tools.cq.core.summary_contract import SummaryEnvelopeV1, apply_summary_mapping


class MacroResultBuilder:
    """Builder for CqResult instances.

    Accumulates findings, evidence, sections, and summary fields, then
    constructs a frozen CqResult via build().
    """

    def __init__(
        self,
        macro_name: str,
        *,
        root: Path,
        argv: list[str],
        run_ctx: object,
    ) -> None:
        self._run_meta = run_ctx.to_runmeta(macro_name)  # type: ignore[union-attr]
        self._findings: list[Finding] = []
        self._evidence: list[Finding] = []
        self._sections: list[Section] = []
        self._summary: SummaryEnvelopeV1 = SummaryEnvelopeV1()

    def add_finding(self, finding: Finding) -> MacroResultBuilder:
        """Add a single finding to the result."""
        self._findings.append(finding)
        return self

    def add_findings(self, findings: Iterable[Finding]) -> MacroResultBuilder:
        """Add multiple findings to the result."""
        self._findings.extend(findings)
        return self

    def add_evidence(self, finding: Finding) -> MacroResultBuilder:
        """Add a single evidence finding to the result."""
        self._evidence.append(finding)
        return self

    def add_section(self, section: Section) -> MacroResultBuilder:
        """Add a section to the result."""
        self._sections.append(section)
        return self

    def set_summary_field(self, key: str, value: object) -> MacroResultBuilder:
        """Set a single summary field by name."""
        self._summary = apply_summary_mapping(self._summary, [(key, value)])
        return self

    def with_summary(self, summary: SummaryEnvelopeV1) -> MacroResultBuilder:
        """Replace the entire summary with the given instance."""
        self._summary = summary
        return self

    def add_classified_findings(
        self,
        buckets: dict[str, list[Finding]],
    ) -> MacroResultBuilder:
        """Add findings from a classified bucket dict (category → findings)."""
        for findings in buckets.values():
            self._findings.extend(findings)
        return self

    def add_taint_findings(
        self,
        sites: list[Finding],
    ) -> MacroResultBuilder:
        """Add taint analysis site findings to evidence."""
        self._evidence.extend(sites)
        return self

    def build(self) -> CqResult:
        """Build and return the final frozen CqResult.

        Returns
        -------
        CqResult
            Frozen CqResult with all accumulated findings, evidence, sections,
            and summary, with finding IDs assigned.
        """
        result = CqResult(
            run=self._run_meta,
            summary=self._summary,
            key_findings=self._findings,
            evidence=self._evidence,
            sections=self._sections,
        )
        return assign_result_finding_ids(result)
```

**Migration:** All 12+ macro files that currently do `result.key_findings.append(finding)` or
`result.sections.append(section)` must be updated to use the builder methods. Since S16 will
make those patterns type errors, migration is enforced by the type checker.

---

#### S18. Refactor Executor CQS

**Goal:** `_apply_entity_handlers`, `_process_decorator_query`, and `_process_call_query`
return `list[Finding]` instead of mutating result. Centralize result mutation in top-level
execution functions.

**File to Edit:**

- `tools/cq/query/executor_runtime.py` — lines 584-1110+

**The CQS violation:** These three functions currently take a `result: CqResult` parameter and
do `result.key_findings.append(finding)` while also doing summary field updates
(`summary.total_defs = ...`). This conflates query (returning results) with command (mutating
state) in a single function — a CQS violation.

**Target signatures:**

```python
# tools/cq/query/executor_runtime.py

def _apply_entity_handlers(
    ctx: ExecutionContext,
    records: list[EntityRecord],
) -> tuple[list[Finding], dict[str, object]]:
    """Apply entity handler pipeline to records.

    Returns
    -------
    tuple[list[Finding], dict[str, object]]
        A tuple of (findings, summary_updates). The caller uses
        msgspec.structs.replace() to apply summary_updates to the result.
    """
    findings: list[Finding] = []
    summary_updates: dict[str, object] = {}
    # ... implementation using findings.append() instead of result.key_findings.append()
    return findings, summary_updates


def _process_decorator_query(
    ctx: ExecutionContext,
    records: list[EntityRecord],
) -> tuple[list[Finding], dict[str, object]]:
    """Process decorator-targeted entity query records.

    Returns
    -------
    tuple[list[Finding], dict[str, object]]
        Findings and summary updates. No mutation of result.
    """
    findings: list[Finding] = []
    summary_updates: dict[str, object] = {}
    # ... implementation
    return findings, summary_updates


def _process_call_query(
    ctx: ExecutionContext,
    records: list[EntityRecord],
) -> tuple[list[Finding], dict[str, object]]:
    """Process call-targeted entity query records.

    Returns
    -------
    tuple[list[Finding], dict[str, object]]
        Findings and summary updates. No mutation of result.
    """
    findings: list[Finding] = []
    summary_updates: dict[str, object] = {}
    # ... implementation
    return findings, summary_updates
```

**Top-level aggregation pattern:**

```python
# tools/cq/query/executor_runtime.py

def _execute_entity_query(ctx: ExecutionContext) -> CqResult:
    assert not ctx.plan.is_pattern_query
    records = _scan_entity_records(ctx)  # imported from query_scan.py
    findings, summary_updates = _apply_entity_handlers(ctx, records)
    base_summary = _build_summary_for_context(ctx)  # imported from query_summary.py
    summary = apply_summary_mapping(base_summary, summary_updates.items())
    result = CqResult(
        run=_build_runmeta(ctx),
        summary=summary,
        key_findings=findings,
        evidence=[],
        sections=[],
    )
    return assign_result_finding_ids(result)
```

---

### Phase 5: Typed Internal Flow (S19-S22)

These items complete the typed-boundary migration into the enrichment pipeline internals.
S19 is independent of Phase 4 and can proceed in parallel with it.

---

#### S19. Define Enrichment Payload View Structs

**Goal:** Create typed structs for the enrichment payload's major sub-sections consumed by
`tools/cq/search/objects/resolve.py`.

**New file to create:**

- `tools/cq/search/objects/payload_views.py`

**Test to create:**

- `tests/unit/cq/search/objects/test_payload_views.py`

**Motivation:** `tools/cq/search/objects/resolve.py` navigates the enrichment payload via
deeply-chained dict access: `payload.get("symbol_grounding", {}).get("definition_targets", [])`.
This pattern is invisible to the type checker and brittle. Typed view structs provide a
conversion layer at the entry point to `resolve.py`.

**Full code for `payload_views.py`:**

```python
# tools/cq/search/objects/payload_views.py
from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct

__all__ = [
    "AgreementView",
    "EnrichmentPayloadView",
    "ResolutionView",
    "StructuralView",
    "SymbolGroundingView",
]


class SymbolGroundingView(CqStruct, frozen=True):
    """Typed view over the symbol_grounding sub-section of an enrichment payload."""

    definition_targets: list[dict[str, object]] = msgspec.field(default_factory=list)
    reference_targets: list[dict[str, object]] = msgspec.field(default_factory=list)


class ResolutionView(CqStruct, frozen=True):
    """Typed view over the resolution sub-section of an enrichment payload."""

    qualified_name_candidates: list[object] = msgspec.field(default_factory=list)
    binding_candidates: list[object] = msgspec.field(default_factory=list)
    import_alias_chain: list[dict[str, object]] = msgspec.field(default_factory=list)
    import_alias_resolution: dict[str, object] = msgspec.field(default_factory=dict)


class StructuralView(CqStruct, frozen=True):
    """Typed view over structural metadata in an enrichment payload."""

    item_role: str | None = None
    node_kind: str | None = None
    scope_kind: str | None = None
    scope_chain: list[str] = msgspec.field(default_factory=list)


class AgreementView(CqStruct, frozen=True):
    """Typed view over cross-source agreement metadata in an enrichment payload."""

    status: str = "partial"
    conflicts: list[object] = msgspec.field(default_factory=list)


class EnrichmentPayloadView(CqStruct, frozen=True):
    """Typed view over an enrichment payload for object resolution.

    Use `EnrichmentPayloadView.from_raw()` to convert a raw dict at the
    entry boundary of resolve.py rather than navigating dicts inline.
    """

    symbol_grounding: SymbolGroundingView = msgspec.field(
        default_factory=SymbolGroundingView
    )
    resolution: ResolutionView = msgspec.field(default_factory=ResolutionView)
    structural: StructuralView = msgspec.field(default_factory=StructuralView)
    agreement: AgreementView = msgspec.field(default_factory=AgreementView)
    stage_errors: list[dict[str, object]] = msgspec.field(default_factory=list)

    @classmethod
    def from_raw(cls, payload: dict[str, object]) -> EnrichmentPayloadView:
        """Parse a raw enrichment payload dict into a typed view.

        Parameters
        ----------
        payload : dict[str, object]
            Raw enrichment payload from the search pipeline.

        Returns
        -------
        EnrichmentPayloadView
            Typed view with all sub-sections parsed and defaulted.
        """
        return msgspec.convert(payload, type=cls, strict=False)
```

---

#### S20. Build Typed Enrichment Facts Directly

**Goal:** Refactor `enrich_python_context` in `tools/cq/search/python/extractors.py` to build
`PythonEnrichmentFacts` directly from enrichment stages, converting to `dict[str, object]` only
at the serialization boundary.

**File to Edit:**

- `tools/cq/search/python/extractors.py` — lines 1040-1260

**Current state:** `_PythonEnrichmentState` (line 1040) uses `dict[str, object]` for
`ast_fields`, `python_resolution_fields`, and `tree_sitter_fields`. These dicts accumulate
values from multiple enrichment stages and are assembled into a final payload dict at
`_finalize_python_enrichment_payload`.

**Target:** The state holds typed fact structs (`PythonResolutionFacts`, `PythonBehaviorFacts`,
etc.). The dict conversion happens only at `_finalize_python_enrichment_payload`, where
`msgspec.to_builtins()` produces the output dict.

**Design invariant:** The serialized output format is unchanged. The typed-fact intermediate
representation is an implementation detail of the extractor, invisible to callers.

**Depends on:** S10 (field sets derived from struct fields), S19 (payload view structs stable).

---

#### S21. Add DI Seams to Enrichment Entry Points

**Goal:** Add optional `parse_session` and `cache_backend` parameters to
`enrich_python_context_by_byte_range` and `enrich_rust_context_by_byte_range`. Default to
module-level singletons.

**Files to Edit:**

- `tools/cq/search/tree_sitter/core/parse.py` — `_SESSIONS` at line ~270
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime_core.py` — line 1072

**Test to create:**

- `tests/unit/cq/search/tree_sitter/test_enrichment_di.py`

**Target signatures:**

```python
# tools/cq/search/tree_sitter/python_lane/runtime.py

def enrich_python_context_by_byte_range(
    source: bytes,
    bstart: int,
    bend: int,
    *,
    parse_session: ParseSession | None = None,
    cache_backend: CacheBackend | None = None,
) -> dict[str, object]:
    """Enrich a Python byte range context.

    Parameters
    ----------
    source : bytes
        Source file bytes.
    bstart : int
        Start byte offset of the target range.
    bend : int
        End byte offset of the target range.
    parse_session : ParseSession | None, optional
        Override the module-level parse session. If None, uses the module
        singleton. Pass a custom session in tests to avoid global state.
    cache_backend : CacheBackend | None, optional
        Override the module-level cache backend. If None, uses the module
        singleton.

    Returns
    -------
    dict[str, object]
        Enrichment payload dict.
    """
    session = parse_session or _get_default_session()
    cache = cache_backend or _get_default_cache()
    # ... existing body with session and cache injected
```

**Why:** The `_SESSIONS` module-level dict in `parse.py` creates implicit global state that
makes unit tests unreliable. Tests that call enrichment functions must either accept real tree-
sitter parse sessions (slow) or monkeypatch module-level state (brittle). DI parameters allow
tests to inject deterministic stubs.

---

#### S22. Add Rust Enrichment Stage Timings

**Goal:** Instrument `enrich_rust_context_by_byte_range` with per-stage timings matching the
5-stage Python pattern.

**File to Edit:**

- `tools/cq/search/tree_sitter/rust_lane/runtime_core.py` — lines 936-978, 981+

**Current state:** Rust enrichment has only `query_pack` and `payload_build` timings (line 958)
plus `attachment` and `total` (lines 974-977). Python enrichment has 5 named stages:
`ast_grep`, `python_ast`, `import_detail`, `libcst`, `tree_sitter`.

**Target pattern (matching Python):**

```python
# tools/cq/search/tree_sitter/rust_lane/runtime_core.py
from time import perf_counter

def enrich_rust_context_by_byte_range(...) -> dict[str, object]:
    total_started = perf_counter()
    stage_timings_ms: dict[str, float] = {}
    stage_status: dict[str, str] = {}

    # Stage 1: ast_grep
    stage_start = perf_counter()
    try:
        ast_grep_result = _run_ast_grep_stage(...)
        stage_status["ast_grep"] = "applied"
    except Exception:
        ast_grep_result = {}
        stage_status["ast_grep"] = "degraded"
    stage_timings_ms["ast_grep"] = (perf_counter() - stage_start) * 1000.0

    # Stage 2: tree_sitter
    stage_start = perf_counter()
    try:
        tree_sitter_result = _run_tree_sitter_stage(...)
        stage_status["tree_sitter"] = "applied"
    except Exception:
        tree_sitter_result = {}
        stage_status["tree_sitter"] = "degraded"
    stage_timings_ms["tree_sitter"] = (perf_counter() - stage_start) * 1000.0

    # ... additional stages following the same pattern ...

    stage_timings_ms["total"] = (perf_counter() - total_started) * 1000.0
    return {
        # ... existing payload fields ...
        "stage_timings_ms": stage_timings_ms,
        "stage_status": stage_status,
    }
```

**Stages to instrument (matching Python's 5-stage pattern):**

1. `ast_grep` — Rust ast-grep patterns
2. `tree_sitter` — tree-sitter CST parse
3. `query_pack` — tree-sitter query execution (was already timed; keep, rename if needed)
4. `payload_build` — payload assembly (was already timed; keep)
5. `attachment` — context attachment (was already timed; keep)

The telemetry output format must be identical to the Python format so that the enrichment
telemetry section in the search output renders Rust timing data the same way as Python timing
data.

---

### Phase 6: Post-Synthesis Corrections and Expanded Scope (S23-S34)

These scope items close gaps discovered during cross-review reconciliation. They are required to
fully align the plan with the design-review recommendations and the aggressive design-phase
cutover stance.

---

#### S23. Type `run_search_partition` Precisely and Delete `enrichment_phase.py`

**Goal:** Change `run_search_partition` return annotation from `object` to
`LanguageSearchResult`, remove the `cast(...)` wrapper module (`enrichment_phase.py`), and wire
callers directly to the partition pipeline.

**Files to Edit:**

- `tools/cq/search/pipeline/partition_pipeline.py`
- `tools/cq/search/pipeline/smart_search.py`

**Files to Delete:**

- `tools/cq/search/pipeline/enrichment_phase.py`

**Test to create:**

- `tests/unit/cq/search/pipeline/test_partition_pipeline_result_type.py`

**Representative snippet:**

```python
# tools/cq/search/pipeline/partition_pipeline.py
from tools.cq.search.pipeline.smart_search_types import LanguageSearchResult

def run_search_partition(
    plan: SearchPartitionPlanV1,
    *,
    ctx: SearchConfig,
    mode: QueryMode,
) -> LanguageSearchResult:
    ...
```

**Legacy decommission/delete scope:**

- Delete `run_enrichment_phase()` in `enrichment_phase.py` (pure cast wrapper).
- Delete `enrichment_phase.py` import usage from `smart_search.py`.

---

#### S24. Remove `_build_launch_context` Global Mutation

**Goal:** Make launch-context construction pure. Move `app.config` assignment into a dedicated
setup command invoked by the launcher before context construction.

**Files to Edit:**

- `tools/cq/cli_app/app.py`

**Test to create:**

- `tests/unit/cq/cli_app/test_launch_context_purity.py`

**Representative snippet:**

```python
# tools/cq/cli_app/app.py
def _configure_app(config_opts: ConfigOptionArgs) -> None:
    app.config = build_config_chain(
        config_file=config_opts.config,
        use_config=config_opts.use_config,
    )

def _build_launch_context(...) -> LaunchContext:
    return LaunchContext(...)
```

**Legacy decommission/delete scope:**

- Delete `app.config = ...` side effect from `_build_launch_context`.

---

#### S25. Promote `SearchStep.mode` to `QueryMode | None`

**Goal:** Replace `SearchStep.mode: Literal["regex", "literal"] | None` with
`QueryMode | None` so parsing/typing occurs at plan boundary, not inside step execution.

**Files to Edit:**

- `tools/cq/run/spec.py`
- `tools/cq/run/step_executors.py`
- Any run-plan parser/decoder paths that currently decode string modes into `SearchStep`

**Test to create:**

- `tests/unit/cq/run/test_search_step_mode_typed.py`

**Representative snippet:**

```python
# tools/cq/run/spec.py
from tools.cq.search._shared.types import QueryMode

class SearchStep(RunStepBase, tag="search", frozen=True):
    query: str
    mode: QueryMode | None = None
```

**Legacy decommission/delete scope:**

- Delete per-step string-to-enum conversion branches in `_execute_search_step`.

---

#### S26. Invert LDMD Collapse Dependency

**Goal:** Remove `ldmd/format.py` runtime import of neighborhood collapse constants. LDMD
parsing/indexing should not depend on neighborhood section-layout internals.

**Files to Edit:**

- `tools/cq/ldmd/format.py`
- `tools/cq/neighborhood/section_layout.py`
- `tools/cq/cli_app/commands/ldmd.py`

**New file to create:**

- `tools/cq/ldmd/collapse_policy.py`

**Test to create:**

- `tests/unit/cq/ldmd/test_collapse_policy.py`

**Representative snippet:**

```python
# tools/cq/ldmd/format.py
from tools.cq.ldmd.collapse_policy import LdmdCollapsePolicyV1

def build_index(content: bytes, *, collapse_policy: LdmdCollapsePolicyV1 | None = None) -> LdmdIndex:
    policy = collapse_policy or LdmdCollapsePolicyV1.default()
    ...
```

**Legacy decommission/delete scope:**

- Delete lazy import of `_DYNAMIC_COLLAPSE_SECTIONS` / `_UNCOLLAPSED_SECTIONS` in
  `ldmd/format.py`.

---

#### S27. Freeze Query Scan Context Contracts

**Goal:** Make `ScanContext` and `EntityCandidates` immutable contracts (frozen dataclasses or
frozen structs) with immutable collection fields where feasible.

**Files to Edit:**

- `tools/cq/query/scan.py`
- `tools/cq/query/executor_runtime.py`
- `tools/cq/query/executor_definitions.py`
- `tools/cq/query/section_builders.py`

**Test to create:**

- `tests/unit/cq/query/test_scan_context_immutability.py`

**Representative snippet:**

```python
# tools/cq/query/scan.py
@dataclass(frozen=True, slots=True)
class ScanContext:
    def_records: tuple[SgRecord, ...]
    call_records: tuple[SgRecord, ...]
    ...
```

**Legacy decommission/delete scope:**

- Delete mutable dataclass definitions for `ScanContext` and `EntityCandidates`.

---

#### S28. Remove AST-Grep/Query Dependency Inversion

**Goal:** Move AST-grep pattern metavariable helpers to AST-grep package boundaries and move
query-language constants/helpers to core typed vocabulary.

**Files to Edit:**

- `tools/cq/astgrep/sgpy_scanner.py`
- `tools/cq/query/metavar.py`
- `tools/cq/query/language.py`
- `tools/cq/core/types.py`

**New file to create:**

- `tools/cq/astgrep/metavar.py`

**Test to create:**

- `tests/unit/cq/astgrep/test_metavar_helpers.py`

**Representative snippet:**

```python
# tools/cq/astgrep/sgpy_scanner.py
from tools.cq.astgrep.metavar import extract_metavar_names, extract_variadic_metavar_names
from tools.cq.core.types import DEFAULT_QUERY_LANGUAGE, is_rust_language
```

**Legacy decommission/delete scope:**

- Delete AST-grep-specific helper definitions from `query/metavar.py`.

---

#### S29. Split `search/_shared/core.py` by Responsibility

**Goal:** Replace the 431-LOC mixed-responsibility module with focused modules: helpers,
contracts/requests, and timeout/runtime wrappers.

**Files to Edit:**

- `tools/cq/search/_shared/core.py`
- `tools/cq/search/_shared/__init__.py`
- All importers currently referencing `tools.cq.search._shared.core`

**New files to create:**

- `tools/cq/search/_shared/helpers.py`
- `tools/cq/search/_shared/requests.py`
- `tools/cq/search/_shared/timeouts.py`

**Tests to create:**

- `tests/unit/cq/search/shared/test_helpers.py`
- `tests/unit/cq/search/shared/test_requests.py`
- `tests/unit/cq/search/shared/test_timeouts.py`

**Representative snippet:**

```python
# tools/cq/search/_shared/helpers.py
def line_col_to_byte_offset(source_bytes: bytes, line: int, col: int) -> int | None:
    ...
```

**Legacy decommission/delete scope:**

- Delete monolithic definitions from `_shared/core.py` after imports are cut over.

---

#### S30. Remove Dead Null Check in `_combined_progress_callback`

**Goal:** Simplify callback budget wrapper by removing unreachable inner `callback is None`
branch after closure creation has already narrowed `callback`.

**Files to Edit:**

- `tools/cq/search/tree_sitter/core/runtime.py`

**Test to create:**

- `tests/unit/cq/search/tree_sitter/test_runtime_progress_callback.py`

**Representative snippet:**

```python
# tools/cq/search/tree_sitter/core/runtime.py
def _budget_callback(state: object) -> bool:
    if monotonic() >= deadline:
        return False
    return bool(callback(state))
```

**Legacy decommission/delete scope:**

- Delete dead `if callback is None: return True` branch in `_budget_callback`.

---

#### S31. Make `build_error_result` CQS-Compliant

**Goal:** Stop mutating `result.summary.error` after result construction. Build summary error via
copy-on-write summary update and return a fully-constructed result value.

**Files to Edit:**

- `tools/cq/core/result_factory.py`
- Any callers/tests assuming in-place mutation side effects

**Test to create:**

- `tests/unit/cq/core/test_result_factory_error_result.py`

**Representative snippet:**

```python
# tools/cq/core/result_factory.py
result = mk_result(run_ctx.to_runmeta(macro))
summary = apply_summary_mapping(result.summary, {"error": str(error)})
return msgspec.structs.replace(result, summary=summary)
```

**Legacy decommission/delete scope:**

- Delete direct `result.summary.error = ...` mutation in `build_error_result`.

---

#### S32. Move Render-Enrichment Session Assembly out of `report.py`

**Goal:** Move `_prepare_render_enrichment_session` orchestration out of renderer shell into
`render_enrichment_orchestrator.py` (or a dedicated `render_session.py`), leaving report render
as a view assembly function.

**Files to Edit:**

- `tools/cq/core/report.py`
- `tools/cq/core/render_enrichment_orchestrator.py`

**Test to create:**

- `tests/unit/cq/core/test_render_enrichment_session.py`

**Representative snippet:**

```python
# tools/cq/core/render_enrichment_orchestrator.py
def prepare_render_enrichment_session(...) -> RenderEnrichmentSessionV1:
    ...
```

**Legacy decommission/delete scope:**

- Delete `_prepare_render_enrichment_session` from `report.py`.

---

#### S33. Collapse CLI Params/Options Duplication into a Single Command Schema

**Goal:** Replace duplicated `*Params`/`*Options` field definitions with a single command-schema
source and generated projections for cyclopts binding and internal typed options.

**Files to Edit:**

- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/options.py`
- `tools/cq/cli_app/commands/*.py` (imports/construction)

**New files to create:**

- `tools/cq/cli_app/command_schema.py`
- `tools/cq/cli_app/schema_projection.py`

**Tests to create:**

- `tests/unit/cq/cli_app/test_command_schema_projection.py`
- `tests/unit/cq/cli_app/test_options_from_projected_params.py`

**Representative snippet:**

```python
# tools/cq/cli_app/command_schema.py
class SearchCommandSchema(CqStruct, frozen=True, kw_only=True):
    query: str
    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    ...
```

**Legacy decommission/delete scope:**

- Delete hand-maintained duplicated field declarations in `params.py` and `options.py` once
  projection layer is in place.

---

#### S34. Simplify Query Registry Caching with Diskcache Built-ins

**Goal:** Remove double-caching in query-pack registry loader by using one diskcache-backed
memoization strategy (`memoize_stampede`) for stamped loads and deleting redundant nested
`memoized_value(...)` wrapping where it does not add additional behavior.

**Files to Edit:**

- `tools/cq/search/tree_sitter/query/registry.py`
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py` (only if helper becomes unused)

**Test to create:**

- `tests/unit/cq/search/tree_sitter/query/test_registry_cache_path.py`

**Representative snippet:**

```python
# tools/cq/search/tree_sitter/query/registry.py
@memoize_stampede(cache, expire=_STAMP_TTL_SECONDS, tag=_STAMP_TAG)
def _load(*, include_distribution: bool, local_hash: str) -> tuple[QueryPackSourceV1, ...]:
    return _load_sources_uncached(
        language=language,
        include_distribution=include_distribution,
    )
```

**Legacy decommission/delete scope:**

- Delete redundant nested memoization wrapper path in `_stamped_loader`.
- Delete `memoized_value` usage in registry path if no longer needed after simplification.

---

## 5. Cross-Scope Legacy Decommission and Deletion Plan

Deletions are batched by dependency — each batch executes only after the named scope items
complete. Do not merge a deletion batch before its prerequisite scope items are verified passing.

---

### D1: Delete Duplicate Helpers (after S1)

Files and functions to delete:

- Delete `_category_message` function from `tools/cq/search/pipeline/smart_search.py`
  (lines 292-323)
- Delete `_evidence_to_bucket` function from `tools/cq/search/pipeline/smart_search.py`
  (lines 270-289)
- Delete `_apply_prefetched_search_semantic_outcome` from
  `tools/cq/search/pipeline/search_semantic.py` (lines 191-213)

**Verification:** Run `uv run ruff check` to confirm no remaining references to the deleted
functions.

---

### D2: Delete Dispatch Facade Files (after S3)

Files to delete entirely:

- `tools/cq/query/executor.py`
- `tools/cq/query/executor_dispatch.py`

**Verification:** Run `uv run ruff check` and `uv run pytest tests/unit/cq/query/` to confirm
no remaining imports and tests pass.

---

### D3: Delete Consolidated Duplicates (after S8, S9)

Functions and classes to delete:

- Delete `_accumulate_runtime_flags` from `tools/cq/search/enrichment/python_adapter.py`
  (lines 116-126) — now in `enrichment/core.py`
- Delete `_accumulate_runtime_flags` from `tools/cq/search/enrichment/rust_adapter.py`
  (lines 100-110) — now in `enrichment/core.py`
- Delete `_tree_sitter_diagnostics` from `tools/cq/search/enrichment/python_adapter.py`
  (lines 128-141) — now `build_tree_sitter_diagnostic_rows` in `enrichment/core.py`
- Delete `CacheRuntimePolicy` class from `tools/cq/core/runtime/execution_policy.py`
  (lines 58-72) — replaced by `CqCachePolicyV1`
- Delete `_env_namespace_ttls` from `tools/cq/core/runtime/execution_policy.py` — duplicates
  `_resolve_namespace_ttl_from_env()` in `policy.py`

**Verification:** Run `uv run pyrefly check` to catch any remaining type references to the
deleted types.

---

### D4: Delete Extracted Functions from Source Modules (after S11, S14)

After `query_scan.py`, `query_summary.py`, `fragment_cache.py`, `relevance.py`, and
`request_parsing.py` are created and wired:

- Delete scan functions from `executor_runtime.py` — moved to `query_scan.py`
  (`_scan_entity_records`, `_scan_entity_fragment_misses`)
- Delete summary functions from `executor_runtime.py` — moved to `query_summary.py`
  (`_build_runmeta`, `_query_mode`, `_query_text`, `_summary_common_for_query`,
  `_summary_common_for_context`, `_finalize_single_scope_summary`)
- Delete cache fragment functions from `executor_runtime.py` — moved to `fragment_cache.py`
  (`_build_entity_fragment_context`, `_decode_entity_fragment_payload`,
  `_entity_records_from_hits`, `_assemble_entity_records`)
- Delete pattern-fragment cache helpers from `executor_ast_grep.py` — moved to
  `fragment_cache.py`
- Delete scoring functions from `smart_search.py` — moved to `relevance.py`
  (`compute_relevance_score`, `KIND_WEIGHTS`)
- Delete coercion functions from `smart_search.py` — moved to `request_parsing.py`
  (10 `_coerce_*` functions)

**Verification:** `executor_runtime.py` should be under 500 LOC after D4. `smart_search.py`
should be under 300 LOC.

---

### D5: Delete Mutation-Supporting Methods (after S16, S17, S18, S27, S31)

After `CqResult`, `Finding`, `SummaryEnvelopeV1`, and `DetailPayload` are immutable at the
boundary, and mutation sites are migrated to builder-local state:

- Delete `DetailPayload.__setitem__` from `tools/cq/core/schema.py` (line 130)
- Delete `DetailPayload.__getitem__`, `__contains__`, and `get` dict-like methods if no
  longer needed after the frozen migration
- Delete all direct `result.key_findings.append()`, `result.evidence.append()`, and
  `result.sections.append()` patterns in macro/query/orchestration modules — replaced by
  `MacroResultBuilder` calls and copy-on-write updates
- Delete direct `result.summary.error = ...` assignment in `tools/cq/core/result_factory.py`

**Verification:** Run `uv run pyrefly check` — all mutation sites on frozen structs will be
type errors until they are migrated. Zero pyrefly errors confirms complete migration.

---

### D6: Delete Wrapper/Facade Modules Added by Historical Layering (after S23, S25)

Files/functions to delete:

- Delete `tools/cq/search/pipeline/enrichment_phase.py`
- Delete late mode-conversion branches in `tools/cq/run/step_executors.py` for
  string-mode parsing

**Verification:** Run `uv run pyrefly check` and `uv run pytest tests/unit/cq/run/ -q`.

---

### D7: Delete Reverse-Dependency Artifacts (after S26, S28, S29, S32)

Files/functions to delete:

- Delete LDMD lazy neighborhood import branch in `tools/cq/ldmd/format.py`
- Delete AST-grep helper imports from `tools/cq/query/metavar.py` that are moved to `astgrep/`
- Delete monolithic definitions from `tools/cq/search/_shared/core.py` once all imports move
  to `helpers.py`, `requests.py`, and `timeouts.py`
- Delete `_prepare_render_enrichment_session` from `tools/cq/core/report.py`

**Verification:** `rg "tools.cq.search._shared.core|tools.cq.query.metavar|_prepare_render_enrichment_session|section_layout" tools/cq`
should only show intended residual references.

---

### D8: Delete CLI Duplication Artifacts (after S33)

Files/functions to delete:

- Delete duplicated hand-maintained field declarations across `tools/cq/cli_app/params.py`
  and `tools/cq/cli_app/options.py` that are superseded by command-schema projections

**Verification:** Ensure schema-projection tests pass and no command defines fields in both
`params.py` and `options.py`.

---

### D9: Delete Redundant Query-Registry Memoization Layer (after S34)

Files/functions to delete:

- Delete redundant nested `memoized_value(...)` usage from
  `tools/cq/search/tree_sitter/query/registry.py` stamped loader path
- Delete helper import/path dependencies if no remaining callsites

**Verification:** Query-pack registry tests show unchanged functional behavior and expected cache
hit/miss telemetry under repeated calls.

---

## 6. Implementation Sequence

The sequence is dependency-ordered and front-loads low-risk architecture corrections before
high-risk immutability migration.

1. S1
2. S2
3. S4
4. S5
5. S6
6. S3
7. S23
8. S24
9. S25
10. S30
11. D1
12. D2
13. D6
14. S7
15. S8
16. S10
17. S9
18. D3
19. S14
20. S11
21. S12
22. S13
23. S15
24. S26
25. S28
26. S29
27. S32
28. D4
29. D7
30. S19
31. S20
32. S21
33. S22
34. S27
35. S31
36. S16
37. S17
38. S18
39. D5
40. S33
41. D8
42. S34
43. D9

**Ordering rationale:**

- Steps 1-13 eliminate obvious correctness and dependency-direction issues with low blast radius.
- Steps 14-29 perform structural extraction and reverse-dependency cleanup before immutability.
- Steps 30-33 complete typed internal flow and DI seams needed for stable migration.
- Steps 34-39 execute immutability and CQS changes after scaffolding is in place.
- Steps 40-43 finalize CLI/schema cache simplification and remove duplicated layers.

---

## 7. Implementation Checklist

**Phase 1: Safe Deletions and Renames**

- [ ] S1: Delete duplicate search pipeline helpers
- [ ] S2: Rename SummaryEnvelopeV1 collision
- [ ] S3: Remove query dispatch facades
- [ ] S4: Add contract constraints
- [ ] S5: Tighten bare string types to Literal/enum (`enrichment_status`, `resolution_quality`,
      `coverage_level`) and default `CliContext.output_format`
- [ ] S6: Fix minor inversions and add `__all__` exports
- [ ] S23: Type `run_search_partition` and delete `enrichment_phase.py`
- [ ] S24: Remove `_build_launch_context` global mutation
- [ ] S25: Promote `SearchStep.mode` to `QueryMode | None`
- [ ] S30: Remove dead null check in `_combined_progress_callback`
- [ ] D1: Delete duplicate helpers (after S1)
- [ ] D2: Delete dispatch facade files (after S3)
- [ ] D6: Delete wrapper/facade modules (after S23, S25)

**Phase 2: DRY Consolidation**

- [ ] S7: Unify lane canonicalization
- [ ] S8: Consolidate enrichment helpers
- [ ] S9: Consolidate cache policy
- [ ] S10: Derive payload field sets from struct fields
- [ ] D3: Delete consolidated duplicates (after S8, S9)

**Phase 3: Structural Extraction**

- [ ] S11: Extract scan/summary/fragment cache from `executor_runtime.py` and cache fragments
      from `executor_ast_grep.py`
- [ ] S12: Extract from `assembly.py`
- [ ] S13: Extract pure analysis from macros
- [ ] S14: Split `smart_search.py`
- [ ] S15: Fix core→search reverse dependencies with hard cutover (no re-export shim)
- [ ] S26: Invert LDMD collapse dependency
- [ ] S28: Remove AST-grep/query dependency inversion
- [ ] S29: Split `search/_shared/core.py`
- [ ] S32: Move render-enrichment session assembly out of `report.py`
- [ ] D4: Delete extracted functions from source modules (after S11, S14)
- [ ] D7: Delete reverse-dependency artifacts (after S26, S28, S29, S32)

**Phase 4: Immutability Migration**

- [ ] S27: Freeze query scan context contracts (`ScanContext`, `EntityCandidates`)
- [ ] S31: Make `build_error_result` CQS-compliant
- [ ] S16: Enforce deep immutability for core schema boundaries (`DetailPayload`, `Finding`,
      `SummaryEnvelopeV1`, `CqResult`) with refreshed mutation baseline
- [ ] S17: Extend `MacroResultBuilder` and migrate mutation callsites
- [ ] S18: Refactor executor CQS
- [ ] D5: Delete mutation-supporting methods and direct mutation patterns (after S16, S17, S18,
      S27, S31)

**Phase 5: Typed Internal Flow**

- [ ] S19: Define enrichment payload view structs
- [ ] S20: Build typed enrichment facts directly
- [ ] S21: Add DI seams to enrichment entry points
- [ ] S22: Add Rust enrichment stage timings

**Phase 6: Final Hard-Cutover Cleanup**

- [ ] S33: Collapse CLI Params/Options duplication into a single command schema
- [ ] D8: Delete CLI duplication artifacts (after S33)
- [ ] S34: Simplify query registry caching with diskcache built-ins
- [ ] D9: Delete redundant query-registry memoization layer (after S34)
