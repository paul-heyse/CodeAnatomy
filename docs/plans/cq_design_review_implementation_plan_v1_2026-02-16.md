# CQ Design Review Implementation Plan v1 (2026-02-16)

## Scope Summary

This plan integrates all recommendations from 7 design review documents covering the entire `tools/cq/` codebase (~76K LOC, 421 files). The reviews scored 24 design principles with specific file:line evidence and produced actionable recommendations.

**Design stance:** Hard-cutover migration to the target architecture. No compatibility shims, no deprecation period, and no transitional compatibility messaging. Callers are updated in the same change that moves or replaces behavior.

**Behavior stance:** Superior target-architecture behavior changes are accepted during this design phase and are defined now rather than deferred.

**Coverage:** Every recommendation from all 7 reviews is addressed. Scope items are grouped by theme rather than by review document to enable efficient batched work.

---

## Design Principles

1. **Hard cutover only** -- No compatibility aliases, no shims, no dual-path execution.
2. **One canonical location** -- Each piece of domain knowledge lives in exactly one place.
3. **Target behavior over legacy behavior** -- Better target behavior is preferred over preserving legacy quirks.
4. **Immediate legacy deletion** -- Superseded code is removed in the same scope item that replaces it.
5. **Test parity** -- Every new module has a corresponding test file. Existing tests are updated, not deleted, when code moves.
6. **msgspec for contracts** -- All new cross-module contracts use `msgspec.Struct` per the CQ model boundary policy.
7. **No `# noqa` or `# type: ignore`** -- Fix issues structurally.
8. **No CI layering checks in this plan** -- Structural/layering alignment is enforced through direct refactors, types, and tests in each scope item.

---

## Current Baseline

- `tools/cq/` currently contains 421 files (baseline refreshed from current tree)
- `tools/cq/core/` imports from `search/`, `macros/`, and `query/` in 15+ locations (reverse dependency)
- `tools/cq/core/report.py` (1,013 LOC), `render_overview.py` (293), `render_enrichment.py` (321), `render_summary.py` (551) share 6 duplicated private helper functions
- `tools/cq/core/contracts_constraints.py` defines canonical `PositiveInt`/`NonNegativeInt` but 4 other files redefine them locally
- `tools/cq/core/front_door_insight.py` (1,171 LOC) owns 5 responsibilities
- `tools/cq/search/pipeline/smart_search.py` (2,048 LOC) is the largest file, mixing orchestration with rendering
- `tools/cq/search/pipeline/assembly.py` currently imports summary/section builders from `smart_search.py` at runtime, creating reverse coupling during partial decomposition
- `tools/cq/query/executor.py` (1,216 LOC) has 34 private functions across 5 concerns
- `tools/cq/query/executor_bytecode.py` (11 LOC), `executor_cache.py` (11 LOC), `executor_metavars.py` (21 LOC) are empty placeholders
- `dict[str, object]` is the universal payload type in the search pipeline, despite typed fact structs existing in `enrichment/python_facts.py` and `enrichment/rust_facts.py`
- Language dispatch currently uses explicit string equality checks in at least 54 locations (`29` Python + `25` Rust comparisons)
- `tools/cq/run/runner.py` (516 LOC) depends on `cli_app.context.CliContext` directly
- `_has_query_tokens` in `runner.py:494-496` has already-drifted regex escaping vs the copy in `commands/query.py:27-34`
- `plan_feasible_slices` is duplicated with subtly different signatures in `neighborhood/contracts.py:65` and `neighborhood/bundle_builder.py:48`
- `coerce_float` semantics are split across modules (`TypeError`-raising strict behavior in `core/type_coercion.py` vs optional-return variants in `core/schema.py` and `core/scoring.py`)
- `tools/cq/ldmd/writer.py` uses 23 `getattr(...)` calls over untyped bundle objects even though `SemanticNeighborhoodBundleV1` is available
- `tools/cq/core/cache/__init__.py` currently exports 70 symbols, creating a broad and unstable import surface

---

## S1. Consolidate Rendering Helpers into `core/render_utils.py`

### Goal
Eliminate 6 duplicated private helper functions across 4 rendering files by extracting them into a single shared module.

### Representative Code Snippets

```python
# tools/cq/core/render_utils.py
"""Shared rendering utility functions for CQ output formatting."""
from __future__ import annotations

__all__ = [
    "na",
    "clean_scalar",
    "safe_int",
    "format_location",
    "extract_symbol_hint",
    "iter_result_findings",
]


def na(reason: str) -> str:
    """Render explicit N/A reason text."""
    return f"N/A - {reason.replace('_', ' ')}"


def clean_scalar(value: object) -> str | None:
    """Normalize scalar values while preserving current CQ rendering semantics."""
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        return str(value)
    return None


def safe_int(value: object) -> int | None:
    """Return int values only; reject bool and non-int types."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def format_location(
    file_value: str | None,
    line_value: int | None,
    col_value: int | None,
) -> str | None:
    """Format optional file/line/col into a compact location string."""
    if not file_value and line_value is None:
        return None
    base = file_value or "<unknown>"
    if line_value is not None and line_value > 0:
        base = f"{base}:{line_value}"
        if col_value is not None and col_value >= 0:
            base = f"{base}:{col_value}"
    return base


def extract_symbol_hint(finding: Finding) -> str | None:
    """Extract best symbol hint from structured finding details or message text."""
    for key in ("name", "symbol", "match_text", "callee", "text"):
        value = finding.details.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().split("\n", maxsplit=1)[0]
    message = finding.message.strip()
    if not message:
        return None
    if ":" in message:
        candidate = message.rsplit(":", maxsplit=1)[1].strip()
        if candidate:
            return candidate
    if "(" in message:
        return message.split("(", maxsplit=1)[0].strip() or None
    return message or None


def iter_result_findings(result: CqResult) -> list[Finding]:
    """Return all findings in stable order across key findings, sections, and evidence."""
    findings: list[Finding] = []
    findings.extend(result.key_findings)
    for section in result.sections:
        findings.extend(section.findings)
    findings.extend(result.evidence)
    return findings
```

### Files to Edit
- `tools/cq/core/report.py` -- remove `_na`, `_clean_scalar`, `_safe_int`, `_format_location`, `_extract_symbol_hint`, `_iter_result_findings`; import from `render_utils`
- `tools/cq/core/render_overview.py` -- remove `_na`, `_clean_scalar`, `_extract_symbol_hint`, `_iter_result_findings`; import from `render_utils`
- `tools/cq/core/render_enrichment.py` -- remove `_na`, `_clean_scalar`, `_safe_int`, `_format_location`; import from `render_utils`
- `tools/cq/core/render_summary.py` -- remove `_na`; import from `render_utils`

### New Files to Create
- `tools/cq/core/render_utils.py`
- `tests/unit/cq/core/test_render_utils.py`

### Legacy Decommission/Delete Scope
- Delete `_na` from `report.py:193`, `render_overview.py:17`, `render_enrichment.py:109`, `render_summary.py:58`
- Delete `_clean_scalar` from `report.py:197`, `render_overview.py:21`, `render_enrichment.py:120`
- Delete `_safe_int` from `report.py:208`, `render_enrichment.py:131`
- Delete `_format_location` from `report.py:214`, `render_enrichment.py:137`
- Delete `_extract_symbol_hint` from `report.py:227`, `render_overview.py:32`
- Delete `_iter_result_findings` from `report.py:417`, `render_overview.py:242`

---

## S2. Consolidate Constrained Type Aliases and Env Parsing

### Goal
Establish `contracts_constraints.py` as the single authority for constrained type aliases and `runtime/env_namespace.py` as the single authority for environment variable parsing. Replace the split `coerce_float` behavior with explicit strict/optional coercion APIs and migrate all call sites in one cutover.

### Representative Code Snippets

```python
# tools/cq/core/contracts_constraints.py (already exists, canonical source)
# PositiveInt and NonNegativeInt already defined here -- just import from here everywhere

# tools/cq/core/cache/policy.py -- BEFORE
PositiveInt = Annotated[int, msgspec.Meta(ge=1)]  # DELETE this
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]  # DELETE this

# tools/cq/core/cache/policy.py -- AFTER
from tools.cq.core.contracts_constraints import NonNegativeInt, PositiveInt
```

```python
# tools/cq/core/runtime/env_namespace.py (canonical location for env parsing)
def env_bool(raw: str | None, *, default: bool) -> bool:
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def env_int(raw: str | None, *, default: int, minimum: int = 1) -> int:
    if raw is None:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return parsed if parsed >= minimum else default

# tools/cq/core/cache/policy.py -- replace local _env_bool/_env_int
from tools.cq.core.runtime.env_namespace import env_bool, env_int

# tools/cq/core/runtime/execution_policy.py -- replace local _env_bool/_env_int
from tools.cq.core.runtime.env_namespace import env_bool, env_int
```

```python
# tools/cq/core/type_coercion.py
def coerce_float_strict(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError
    return float(value)


def coerce_float_optional(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)
```

### Files to Edit
- `tools/cq/core/cache/policy.py` -- remove local `PositiveInt`, `NonNegativeInt`, `_env_bool`, `_env_int`; import from canonical sources
- `tools/cq/core/cache/snapshot_fingerprint.py` -- remove local `NonNegativeInt`; import from `contracts_constraints`
- `tools/cq/core/cache/fragment_contracts.py` -- remove local `NonNegativeInt`; import from `contracts_constraints`
- `tools/cq/core/runtime/env_namespace.py` -- add public `env_bool`/`env_int` and include them in `__all__`
- `tools/cq/core/runtime/execution_policy.py` -- remove local `_env_bool`/`_env_int`; import from `env_namespace`
- `tools/cq/core/type_coercion.py` -- split coercion APIs into strict and optional functions
- `tools/cq/core/schema.py` -- remove local `coerce_float`; import optional coercion from `type_coercion`
- `tools/cq/core/scoring.py` -- remove local `_coerce_float`; import optional coercion from `type_coercion`
- `tools/cq/core/report.py` -- replace `try/except TypeError` score coercion with optional coercion API

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete local `PositiveInt`/`NonNegativeInt` from `cache/policy.py`, `cache/snapshot_fingerprint.py`, `cache/fragment_contracts.py`
- Delete local `_env_bool`/`_env_int` from `cache/policy.py` and `runtime/execution_policy.py`
- Delete local `coerce_float` from `schema.py` and `_coerce_float` from `scoring.py`
- Delete single-name `coerce_float` entry point in `type_coercion.py` and replace with explicit strict/optional APIs

---

## S3. Consolidate Tree-Sitter Python Lane Constants

### Goal
Extract shared constants and helpers from `python_lane/runtime.py`, `python_lane/facts.py`, and `python_lane/fallback_support.py` into a single `python_lane/constants.py`, preserving existing runtime semantics.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/python_lane/constants.py
"""Canonical constants and shared helpers for the Python tree-sitter lane."""
from __future__ import annotations

from functools import lru_cache

__all__ = [
    "PYTHON_LIFT_ANCHOR_TYPES",
    "MAX_CAPTURE_ITEMS",
    "DEFAULT_MATCH_LIMIT",
    "STOP_CONTEXT_KINDS",
    "get_python_field_ids",
]

PYTHON_LIFT_ANCHOR_TYPES: frozenset[str] = frozenset({
    "function_definition",
    "class_definition",
    "decorated_definition",
    "assignment",
    "augmented_assignment",
    "expression_statement",
    "import_statement",
    "import_from_statement",
    "if_statement",
    "for_statement",
    "while_statement",
    "with_statement",
    "try_statement",
})

MAX_CAPTURE_ITEMS: int = 8
DEFAULT_MATCH_LIMIT: int = 4_096
STOP_CONTEXT_KINDS: frozenset[str] = frozenset({
    "module",
    "source_file",
})


@lru_cache(maxsize=1)
def get_python_field_ids() -> dict[str, int]:
    """Return cached Python field IDs from tree-sitter."""
    from tools.cq.search.tree_sitter.core.infrastructure import cached_field_ids
    return cached_field_ids("python")
```

### Files to Edit
- `tools/cq/search/tree_sitter/python_lane/runtime.py` -- remove `_PYTHON_LIFT_ANCHOR_TYPES`, `_MAX_CAPTURE_ITEMS`, `_DEFAULT_MATCH_LIMIT`, `_STOP_CONTEXT_KINDS`, `get_python_field_ids`; import from `constants`
- `tools/cq/search/tree_sitter/python_lane/facts.py` -- remove duplicated constants; import from `constants`
- `tools/cq/search/tree_sitter/python_lane/fallback_support.py` -- remove `get_python_field_ids`; import from `constants`

### New Files to Create
- `tools/cq/search/tree_sitter/python_lane/constants.py`
- `tests/unit/cq/search/tree_sitter/python_lane/test_constants.py`

### Legacy Decommission/Delete Scope
- Delete `_PYTHON_LIFT_ANCHOR_TYPES` from `runtime.py:457-467` and `facts.py:53-63`
- Delete `_MAX_CAPTURE_ITEMS` from `runtime.py:55`
- Delete `_DEFAULT_MATCH_LIMIT` from `runtime.py:57` and `facts.py:51`
- Delete `_STOP_CONTEXT_KINDS` from `runtime.py:58` and `facts.py:52`
- Delete `get_python_field_ids` from `runtime.py:62-64` and `fallback_support.py:17-23`

---

## S4. Consolidate All Cross-Module Duplicated Functions

### Goal
Eliminate all remaining function-level duplication identified across search pipeline, query engine, CLI/run, macros, and tree-sitter while preserving intentional behavior differences (for example, sort-key detail level).

### Representative Code Snippets

```python
# tools/cq/search/pipeline/_semantic_helpers.py (NEW)
"""Shared helpers for Python semantic enrichment pipeline stages."""
from __future__ import annotations

__all__ = [
    "normalize_python_semantic_degradation_reason",
    "count_mapping_rows",
]


def normalize_python_semantic_degradation_reason(reason: str | None) -> str:
    """Normalize semantic degradation reason across semantic pipeline modules."""
    # Canonical implementation moved from python_semantic.py/search_semantic.py.
    ...


def count_mapping_rows(mapping: dict[str, object] | None) -> int:
    """Count rows in a mapping payload."""
    ...
```

```python
# tools/cq/run/helpers.py (NEW)
"""Shared helpers for the CQ run engine."""
from __future__ import annotations

__all__ = ["error_result", "merge_in_dir"]


def error_result(command_name: str, error: str, *, run_id: str = "") -> CqResult:
    """Construct a standard error result for a failed run step."""
    ...


def merge_in_dir(step_in_dir: str | None, plan_in_dir: str | None) -> str | None:
    """Merge step-level and plan-level --in directory scopes."""
    ...
```

```python
# tools/cq/query/cache_converters.py (NEW)
def finding_sort_key_detailed(finding: Finding) -> tuple[str, int, int, str]:
    """Deterministic, high-detail sort key used by query/executor.py."""
    ...


def finding_sort_key_lightweight(finding: Finding) -> tuple[str, int, int]:
    """Lightweight sort key used by query/executor_ast_grep.py."""
    ...
```

### Files to Edit

**Search Pipeline helpers:**
- `tools/cq/search/pipeline/python_semantic.py` -- remove `_normalize_python_semantic_degradation_reason`, `_count_mapping_rows`; import from `_semantic_helpers`
- `tools/cq/search/pipeline/search_semantic.py` -- remove same; import from `_semantic_helpers`
- `tools/cq/search/pipeline/smart_search.py` -- remove `_resolve_search_worker_count`; import from `worker_policy.py`
- `tools/cq/search/pipeline/smart_search_telemetry.py` -- remove duplicate `_resolve_search_worker_count`; import from `worker_policy.py`
- `tools/cq/search/semantic/models.py` and `tools/cq/search/rust/extensions.py` -- remove `_string`; import from `tools/cq/search/enrichment/core.py`

**Query Engine helpers:**
- `tools/cq/query/symbol_resolver.py` -- remove local `_extract_call_target`; import canonical extractor from `finding_builders`
- `tools/cq/query/executor.py` -- remove `_record_to_cache_record`, `_cache_record_to_record`, `_record_sort_key`, `_finding_sort_key`; import from `cache_converters.py`
- `tools/cq/query/executor_ast_grep.py` -- remove `record_to_cache_record`, `cache_record_to_record`, `finding_sort_key`, `record_sort_key`; import from `cache_converters.py`

**CLI/Run helpers:**
- `tools/cq/cli_app/commands/query.py` -- remove `_has_query_tokens`; import from `tools/cq/query/parser.py`
- `tools/cq/run/runner.py` -- remove `_has_query_tokens`, `_error_result`, `_merge_in_dir`; import from canonical locations (fix drifted regex)
- `tools/cq/run/step_executors.py` -- remove `_error_result`, `_merge_in_dir`, `_semantic_env_from_bundle`; import from canonical locations
- `tools/cq/cli_app/commands/neighborhood.py` -- remove `_semantic_env_from_bundle`; import from `neighborhood/semantic_env.py`

**Neighborhood:**
- `tools/cq/neighborhood/bundle_builder.py` -- remove duplicate `plan_feasible_slices`; import from `contracts.py`

**Tree-sitter:**
- `tools/cq/search/_shared/core.py` -- remove duplicate `node_text`; callers import from `tree_sitter/core/node_utils.py`

**Macros:**
- `tools/cq/macros/imports.py` -- replace `_STDLIB_PREFIXES` hardcoded set with `sys.stdlib_module_names`

### New Files to Create
- `tools/cq/search/pipeline/_semantic_helpers.py`
- `tools/cq/run/helpers.py`
- `tools/cq/query/cache_converters.py` (shared cache record converters and sort keys)
- `tools/cq/search/pipeline/worker_policy.py`
- `tools/cq/neighborhood/semantic_env.py`
- `tests/unit/cq/search/pipeline/test_semantic_helpers.py`
- `tests/unit/cq/run/test_helpers.py`
- `tests/unit/cq/query/test_cache_converters.py`
- `tests/unit/cq/search/pipeline/test_worker_policy.py`
- `tests/unit/cq/neighborhood/test_semantic_env.py`

### Legacy Decommission/Delete Scope
- Delete `_extract_call_target` from `symbol_resolver.py:357-380` (canonical implementation remains in `finding_builders.py`)
- Delete cache converters from `executor.py:503-528` (move to `cache_converters.py`)
- Delete cache converters from `executor_ast_grep.py:1020-1055` (import from `cache_converters.py`)
- Delete sort keys from `executor.py:531-564` and `executor_ast_grep.py:1058-1082` (move to strategy-specific keys in `cache_converters.py`)
- Delete `_has_query_tokens` from `runner.py:494-496` and `commands/query.py:27-34` (move to `query/parser.py`)
- Delete `_error_result` from `runner.py:499-511` and `step_executors.py:507-519`
- Delete `_merge_in_dir` from `runner.py:433-436` and `step_executors.py:501-504`
- Delete `_semantic_env_from_bundle` from `commands/neighborhood.py:118-136` and `step_executors.py:429-447` (move to `neighborhood/semantic_env.py`)
- Delete `plan_feasible_slices` from `bundle_builder.py:48-89` (keep in `contracts.py:65-92`)
- Delete `node_text` from `_shared/core.py:99-110` (canonical in `core/node_utils.py`)
- Delete `_STDLIB_PREFIXES` from `imports.py:31-77` (replace with `sys.stdlib_module_names`)
- Delete `_normalize_python_semantic_degradation_reason` and `_count_mapping_rows` from `search_semantic.py` (keep in `_semantic_helpers.py`)

---

## S5. Delete Empty Placeholder Modules and Clean Up Exports

### Goal
Remove empty placeholder files, fix underscore-prefixed exports in `__all__`, and clean up confusing type aliases.

### Representative Code Snippets

```python
# tools/cq/query/executor_metavars.py -- BEFORE (pure re-export, no value)
# DELETE THIS FILE -- consumers import from metavar.py directly

# tools/cq/macros/calls/__init__.py -- BEFORE
__all__ = [..., "_extract_context_snippet", "_find_function_signature", ...]
# AFTER: remove underscore-prefixed symbols from __all__
```

### Files to Edit
- `tools/cq/macros/calls/__init__.py` -- remove `_`-prefixed symbols from `__all__`
- `tools/cq/search/pipeline/smart_search_types.py` -- remove `_LanguageSearchResult` private type; use `LanguageSearchResult` directly
- `tools/cq/search/pipeline/contracts.py` -- remove `SmartSearchContext = SearchConfig` alias if redundant
- `tools/cq/core/serialization.py` -- consolidate serialization naming (remove redundant re-exports with different names)
- `tools/cq/core/report.py` -- rename `render_summary = render_summary_condensed` to avoid shadowing
- `tools/cq/index/def_index.py` -- rename `_SELF_CLS` to `SELF_CLS_NAMES`; add to `__all__`
- `tools/cq/index/call_resolver.py` and `tools/cq/index/arg_binder.py` -- update imports
- `tools/cq/search/tree_sitter/core/lane_support.py` -- rename `make_parser` to `make_parser_from_language`
- `tools/cq/introspection/__init__.py` -- trim re-exports to only externally consumed symbols
- `tests/unit/cq/macros/test_calls.py` and `tests/integration/cq/test_search_migration.py` -- update imports that currently rely on underscore exports from package `__init__`

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete `tools/cq/query/executor_bytecode.py` (11 LOC empty placeholder)
- Delete `tools/cq/query/executor_cache.py` (11 LOC empty placeholder)
- Delete `tools/cq/query/executor_metavars.py` (21 LOC pure re-export; update consumers to import from `metavar.py`)
- Delete `tools/cq/index/file_hash.py` (140 LOC, zero external imports)

---

## S6. Fix Type Safety: Literal Types, Enum Constraints, and Contract Guards

### Goal
Replace bare strings with `Literal` types or enums at 6 locations. Replace `assert` guards with proper `if` + `ValueError`. Replace `SearchStep.regex`/`literal` booleans with a `SearchMode` enum.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/contracts/lane_payloads.py
from typing import Literal

class PythonTreeSitterPayloadV1(CqStruct, frozen=True):
    language: Literal["python"] = "python"  # Was: str = "python"

class RustTreeSitterPayloadV1(CqStruct, frozen=True):
    language: Literal["rust"] = "rust"  # Was: str = "rust"
```

```python
# tools/cq/macros/contracts.py
BucketLevel = Literal["low", "medium", "high"]

class ScoringDetailsV1(CqStruct, frozen=True):
    impact_bucket: BucketLevel = "medium"  # Was: str
    confidence_bucket: BucketLevel = "medium"  # Was: str
```

```python
# tools/cq/neighborhood/target_resolution.py
ResolutionKind = Literal["anchor", "file_symbol", "symbol_fallback", "unresolved"]

@dataclass(frozen=True)
class ResolvedTarget:
    resolution_kind: ResolutionKind = "unresolved"  # Was: str
```

```python
# tools/cq/run/spec.py
from typing import Literal

SearchMode = Literal["regex", "literal"]

class SearchStep(RunStepBase, tag="search"):
    mode: SearchMode | None = None  # Replaces regex: bool + literal: bool
```

```python
# tools/cq/query/planner.py -- replace assert with guard
# BEFORE:
assert query.pattern_spec is not None

# AFTER:
if query.pattern_spec is None:
    raise ValueError("pattern_spec is required for pattern queries")
```

### Files to Edit
- `tools/cq/search/tree_sitter/contracts/lane_payloads.py` -- constrain `language` fields to `Literal`
- `tools/cq/macros/contracts.py` -- add `BucketLevel` type; use in scoring structs
- `tools/cq/neighborhood/target_resolution.py` -- add `ResolutionKind` type
- `tools/cq/run/spec.py` -- add `SearchMode`, replace `regex`/`literal` booleans with `mode`
- `tools/cq/run/step_executors.py` -- update dispatch for `SearchStep.mode`
- `tools/cq/run/chain.py` -- emit `SearchStep.mode` instead of `regex`/`literal` flags
- `tests/unit/cq/run/test_step_decode.py` -- update fixtures/assertions for `SearchStep.mode`
- `tools/cq/query/planner.py` -- replace `assert` at lines 334, 373, and 418 with `if` + `ValueError`
- `tools/cq/search/tree_sitter/query/predicates.py` -- replace runtime `assert` checks with explicit type guards + `ValueError`
- `tools/cq/ldmd/format.py` -- accept `LdmdSliceMode` enum instead of `str` for `mode` parameter

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete `regex: bool = False` and `literal: bool = False` from `SearchStep` in `run/spec.py`
- Delete runtime mutual-exclusion check from `step_executors.py:193-195`
- Delete runtime-only `assert` guards from production modules in this scope (`query/planner.py`, `tree_sitter/query/predicates.py`)

---

## S7. Fix CQS Violations

### Goal
Fix command-query separation violations in 5 specific functions across 3 modules.

### Representative Code Snippets

```python
# tools/cq/search/rust/evidence.py -- BEFORE
def attach_macro_expansion_evidence(payload: dict[str, object], ...) -> dict[str, object]:
    payload["macro_expansions"] = ...
    return payload  # CQS violation: mutate + return

# AFTER
def attach_macro_expansion_evidence(payload: dict[str, object], ...) -> None:
    payload["macro_expansions"] = ...
```

```python
# tools/cq/core/schema.py -- BEFORE
def assign_result_finding_ids(result: CqResult) -> CqResult:
    # mutates findings + returns result

# AFTER
def assign_result_finding_ids(result: CqResult) -> None:
    # mutates findings only (command)
```

### Files to Edit
- `tools/cq/search/rust/evidence.py` -- make `attach_macro_expansion_evidence`, `attach_rust_module_graph`, `attach_rust_evidence` return `None`
- `tools/cq/search/rust/evidence.py` -- update callers (chain calls become sequential)
- `tools/cq/core/schema.py` -- make `assign_result_finding_ids` return `None`
- `tools/cq/macros/multilang_fallback.py` -- make `apply_rust_macro_fallback` return `None`
- All callers of the above functions -- update to not use return values

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Remove return statements from the 5 functions listed above

---

## S8. Add Structured Observability Across CQ Subsystems

### Goal
Add `logging.getLogger(__name__)` to all modules identified as having zero observability. Log at WARNING for degradation/errors, DEBUG for normal operations.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/python_lane/runtime.py
import logging

logger = logging.getLogger(__name__)

# In degradation path:
def _mark_degraded(payload: dict[str, object], reason: str) -> None:
    logger.warning("Python enrichment degraded: %s", reason)
    payload["enrichment_status"] = "degraded"
    payload["degrade_reason"] = reason
```

```python
# tools/cq/query/executor.py
import logging

logger = logging.getLogger(__name__)

# At start of execute_plan:
logger.debug("Executing query plan: mode=%s, lang=%s", plan.mode, plan.lang)

# On file read failure:
logger.warning("Skipping unreadable file: %s", file_path)
```

```python
# tools/cq/run/runner.py
import logging

logger = logging.getLogger(__name__)

# At plan start:
logger.debug("Executing run plan: %d steps", len(plan.steps))

# At step completion:
logger.debug("Step %d/%d completed: type=%s, duration=%.1fms", i, total, step_type, elapsed)
```

### Files to Edit
- `tools/cq/search/tree_sitter/python_lane/runtime.py` -- add logger; log degradation
- `tools/cq/search/tree_sitter/rust_lane/runtime.py` -- add logger; log degradation
- `tools/cq/search/rg/runner.py` -- add logger; log timeout/errors
- `tools/cq/search/tree_sitter/core/parse.py` -- add logger; log cache stats on session close
- `tools/cq/search/python/extractors.py` -- add logger; log `_ENRICHMENT_ERRORS` catches
- `tools/cq/search/rust/enrichment.py` -- add logger; log `_ENRICHMENT_ERRORS` catches
- `tools/cq/query/executor.py` -- add logger; log query lifecycle and file read failures
- `tools/cq/query/executor_ast_grep.py` -- add logger; log file read failures
- `tools/cq/run/runner.py` -- add logger; log plan lifecycle
- `tools/cq/run/step_executors.py` -- add logger; log step errors
- `tools/cq/ldmd/format.py` -- add logger; log parse failures
- `tools/cq/macros/calls/entry.py` -- add logger; log macro execution lifecycle
- `tools/cq/index/def_index.py` -- add logger; log file skip reasons

### New Files to Create
None.

### Legacy Decommission/Delete Scope
None (additive change).

---

## S9. Move Orchestration Files Out of `core/` to Fix Dependency Direction

### Goal
Fix the only 0/3 principle score (P5 Dependency Direction) by moving orchestration modules that depend on `search/`, `macros/`, and `query/` out of `core/` into `tools/cq/orchestration/`, and remove the remaining direct outward imports from `core/` in the same cutover.

### Representative Code Snippets

```python
# tools/cq/orchestration/__init__.py
"""CQ orchestration layer -- integrates core, search, query, and macros."""
from __future__ import annotations

__all__ = [
    "bundles",
    "multilang_orchestrator",
    "multilang_summary",
    "schema_export",
    "request_factory",
]
```

```python
# tools/cq/core/ports.py
from typing import Protocol

class SearchObjectRenderPort(Protocol):
    def is_applicability_not_applicable(self, payload: object) -> bool: ...
```

### Files to Edit
- All files that import from the moved modules (grep for `tools.cq.core.bundles`, `tools.cq.core.multilang_orchestrator`, etc.)
- `tools/cq/core/report.py` -- extract render-time enrichment import from `search.pipeline.smart_search` behind a callback protocol
- `tools/cq/core/render_enrichment.py` -- remove module-scope import of `is_applicability_not_applicable` from `search.objects.render`; inline the check or move function to `core/`
- `tools/cq/core/cache/contracts.py` -- remove TYPE_CHECKING imports from `search/`; use generic types
- `tools/cq/core/services.py` -- wire service implementations through DI rather than deferred imports
- `tools/cq/core/contracts.py` -- remove remaining direct query-layer typing imports
- `tools/cq/core/schema_export.py` -- move query/run/search schema composition code to `orchestration/`

### New Files to Create
- `tools/cq/orchestration/__init__.py`
- `tools/cq/orchestration/bundles.py` (moved from `core/bundles.py`)
- `tools/cq/orchestration/multilang_orchestrator.py` (moved from `core/multilang_orchestrator.py`)
- `tools/cq/orchestration/multilang_summary.py` (moved from `core/multilang_summary.py`)
- `tools/cq/orchestration/schema_export.py` (moved from `core/schema_export.py`)
- `tools/cq/orchestration/request_factory.py` (moved from `core/request_factory.py`)
- `tools/cq/core/ports.py`
- `tests/unit/cq/orchestration/` (move existing tests)
- `tests/unit/cq/core/test_ports.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/core/bundles.py` (moved to `orchestration/`)
- Delete `tools/cq/core/multilang_orchestrator.py` (moved)
- Delete `tools/cq/core/multilang_summary.py` (moved)
- Delete `tools/cq/core/schema_export.py` (moved)
- Delete `tools/cq/core/request_factory.py` (moved)

---

## S10. Decompose God Module: `smart_search.py` (2,048 LOC)

### Goal
Complete second-stage decomposition of `smart_search.py` into focused modules plus a thin orchestrator, and remove reverse coupling where `assembly.py` imports summary/section builders back from `smart_search.py`.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/smart_search_summary.py
"""Summary building for Smart Search results."""
from __future__ import annotations

__all__ = ["build_search_summary", "build_language_summary"]

# Functions extracted from smart_search.py summary-building section
```

```python
# tools/cq/search/pipeline/smart_search_sections.py
"""Section building for Smart Search output."""
from __future__ import annotations

__all__ = ["build_definitions_section", "build_callsites_section", ...]
```

```python
# tools/cq/search/pipeline/smart_search_followups.py
"""Follow-up suggestion generation for Smart Search."""
from __future__ import annotations

__all__ = ["generate_followup_suggestions"]
```

### Files to Edit
- `tools/cq/search/pipeline/smart_search.py` -- extract summary, sections, followups; keep orchestration
- `tools/cq/search/pipeline/assembly.py` -- depend on extracted modules directly; remove runtime back-imports from `smart_search.py`

### New Files to Create
- `tools/cq/search/pipeline/smart_search_summary.py`
- `tools/cq/search/pipeline/smart_search_sections.py`
- `tools/cq/search/pipeline/smart_search_followups.py`
- `tests/unit/cq/search/pipeline/test_smart_search_summary.py`
- `tests/unit/cq/search/pipeline/test_smart_search_sections.py`
- `tests/unit/cq/search/pipeline/test_smart_search_followups.py`

### Legacy Decommission/Delete Scope
- Delete summary-building functions from `smart_search.py` (replaced by `smart_search_summary.py`)
- Delete section-building functions from `smart_search.py` (replaced by `smart_search_sections.py`)
- Delete follow-up generation from `smart_search.py` (replaced by `smart_search_followups.py`)

---

## S11. Decompose God Module: `executor.py` (1,216 LOC)

### Goal
Complete second-stage decomposition of `executor.py` by moving remaining mixed concerns into focused modules and leaving `executor.py` as a thin dispatch facade that composes already-extracted modules.

### Representative Code Snippets

```python
# tools/cq/query/executor_dispatch.py
"""Final dispatch facade for query execution entry points."""
from __future__ import annotations

from tools.cq.query.executor_definitions import build_definition_finding
from tools.cq.query.executor_ast_grep import execute_pattern_query_with_files
from tools.cq.query.finding_builders import build_entity_finding

__all__ = ["execute_entity_query", "execute_pattern_query"]
```

### Files to Edit
- `tools/cq/query/executor.py` -- reduce to facade and compatibility entry points only
- `tools/cq/query/executor_definitions.py` -- absorb remaining entity-definition code paths
- `tools/cq/query/executor_ast_grep.py` -- absorb remaining pattern execution pathways
- `tools/cq/query/finding_builders.py` -- absorb finding assembly logic moved from `executor.py`
- `tools/cq/query/cache_converters.py` (created in S4) -- own all cache record conversion and sort-key strategies

### New Files to Create
- `tools/cq/query/executor_dispatch.py`
- `tests/unit/cq/query/test_executor_dispatch.py`

### Legacy Decommission/Delete Scope
- Delete remaining entity execution helpers from `executor.py` (moved to `executor_definitions.py`/`finding_builders.py`)
- Delete remaining pattern execution helpers from `executor.py` (moved to `executor_ast_grep.py`)
- Delete remaining cache orchestration helpers from `executor.py` (moved to `cache_converters.py`)

---

## S12. Decompose God Module: `front_door_insight.py` (1,171 LOC)

### Goal
Split into 3 files: schema definitions, builder logic, and rendering/serialization.

### Files to Edit
- `tools/cq/core/front_door_insight.py` -- extract into 3 modules

### New Files to Create
- `tools/cq/core/front_door_schema.py` (structs + type aliases)
- `tools/cq/core/front_door_builders.py` (build_search/calls/entity_insight + augmentation)
- `tools/cq/core/front_door_render.py` (render_insight_card + serialization)
- `tests/unit/cq/core/test_front_door_schema.py`
- `tests/unit/cq/core/test_front_door_builders.py`
- `tests/unit/cq/core/test_front_door_render.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/core/front_door_insight.py` after all code has been moved to the 3 new files. Update `core/__init__.py` re-exports.

---

## S13. Decompose Mixed-Concern Modules (Secondary Splits)

### Goal
Split 4 additional modules identified as having multiple responsibilities: `_shared/core.py`, `runner.py`, `calls_target.py`, and `infrastructure.py`.

### Files to Edit
- `tools/cq/search/_shared/core.py` -- slim to enrichment contracts only
- `tools/cq/run/runner.py` -- slim to orchestration only
- `tools/cq/macros/calls_target.py` -- slim to resolution and coordination only
- `tools/cq/search/tree_sitter/core/infrastructure.py` -- slim to language runtime helpers only

### New Files to Create
- `tools/cq/search/_shared/encoding.py`
- `tests/unit/cq/search/_shared/test_encoding.py`
- `tools/cq/search/_shared/timeout.py`
- `tests/unit/cq/search/_shared/test_timeout.py`
- `tools/cq/search/_shared/rg_request.py`
- `tests/unit/cq/search/_shared/test_rg_request.py`
- `tools/cq/run/q_execution.py`
- `tests/unit/cq/run/test_q_execution.py`
- `tools/cq/run/scope.py`
- `tests/unit/cq/run/test_scope.py`
- `tools/cq/macros/calls_target_cache.py`
- `tests/unit/cq/macros/test_calls_target_cache.py`
- `tools/cq/search/tree_sitter/core/parallel.py`
- `tests/unit/cq/search/tree_sitter/core/test_parallel.py`
- `tools/cq/search/tree_sitter/core/streaming_parse.py`
- `tests/unit/cq/search/tree_sitter/core/test_streaming_parse.py`
- `tools/cq/search/tree_sitter/core/parser_controls.py`
- `tests/unit/cq/search/tree_sitter/core/test_parser_controls.py`

### Legacy Decommission/Delete Scope
- Functions move to new modules; original files become thin coordinators or are deleted.

---

## S14. Define `LanguageEnrichmentPort` Protocol

### Goal
Replace all identified string-comparison language dispatch sites (54 current equality checks) with a protocol-based adapter pattern and explicit language dispatch registry.

### Representative Code Snippets

```python
# tools/cq/search/enrichment/contracts.py
from typing import Protocol, Mapping

class LanguageEnrichmentPort(Protocol):
    """Port for language-specific enrichment operations."""

    def enrich_by_byte_range(
        self, source: str, *, byte_start: int, byte_end: int
    ) -> dict[str, object]: ...

    def accumulate_telemetry(
        self, lang_bucket: dict[str, object], payload: dict[str, object]
    ) -> None: ...

    def build_diagnostics(
        self, payload: Mapping[str, object]
    ) -> list[dict[str, object]]: ...


# tools/cq/search/enrichment/language_registry.py
from tools.cq.query.language import QueryLanguage

LANGUAGE_ADAPTERS: dict[QueryLanguage, LanguageEnrichmentPort] = {}

def register_language_adapter(lang: QueryLanguage, adapter: LanguageEnrichmentPort) -> None:
    LANGUAGE_ADAPTERS[lang] = adapter

def get_language_adapter(lang: QueryLanguage) -> LanguageEnrichmentPort | None:
    return LANGUAGE_ADAPTERS.get(lang)
```

### Files to Edit
- `tools/cq/search/enrichment/contracts.py` -- add `LanguageEnrichmentPort` protocol
- `tools/cq/search/pipeline/smart_search.py` -- replace 6 `lang ==` dispatch sites
- `tools/cq/search/pipeline/smart_search_telemetry.py` -- replace `match.language == "python"` dispatch
- `tools/cq/search/semantic/front_door.py` -- replace `request.language == "python"` dispatch
- `tools/cq/search/semantic/models.py` -- replace `language == "python"` dispatch
- `tools/cq/query/planner.py` -- remove direct primary-language string branching
- `tools/cq/query/sg_parser.py` -- replace language equality branching with dispatch helpers
- `tools/cq/query/executor_ast_grep.py` -- replace language equality branching with dispatch helpers
- `tools/cq/astgrep/sgpy_scanner.py` -- replace direct `"rust"`/`"python"` comparison branch

### New Files to Create
- `tools/cq/search/enrichment/language_registry.py`
- `tools/cq/search/enrichment/python_adapter.py`
- `tools/cq/search/enrichment/rust_adapter.py`
- `tests/unit/cq/search/enrichment/test_language_registry.py`
- `tests/unit/cq/search/enrichment/test_python_adapter.py`
- `tests/unit/cq/search/enrichment/test_rust_adapter.py`

### Legacy Decommission/Delete Scope
- Replace all discovered `if lang == "python"` / `if lang == "rust"` equality blocks with adapter/dispatch lookup

---

## S15. Introduce Typed Enrichment Payloads (Full Cutover)

### Goal
Replace `dict[str, object]` enrichment payload probing with typed facts at all primary boundaries (`search/enrichment`, `smart_search_telemetry`, and `objects/resolve`) in a single migration scope.

### Representative Code Snippets

```python
# tools/cq/search/enrichment/core.py -- add parse-at-boundary function
from tools.cq.search.enrichment.python_facts import PythonEnrichmentFacts

def parse_python_enrichment(raw: dict[str, object]) -> PythonEnrichmentFacts:
    """Parse raw enrichment dict into typed facts at the pipeline boundary."""
    return msgspec.convert(raw, type=PythonEnrichmentFacts, strict=False)
```

### Files to Edit
- `tools/cq/search/enrichment/core.py` -- add boundary parsing functions for Python and Rust typed facts
- `tools/cq/search/pipeline/smart_search_telemetry.py` -- use typed facts instead of dict probing
- `tools/cq/search/objects/resolve.py` -- use typed accessors instead of `isinstance` checks and direct dict probing
- `tools/cq/search/semantic/front_door.py` -- consume typed enrichment payloads directly

### New Files to Create
None (structs already exist in `python_facts.py` and `rust_facts.py`; this scope performs full consumer migration).

### Legacy Decommission/Delete Scope
- Delete dict-probing enrichment helpers and `isinstance`-based access branches in all files listed above

---

## S16. Improve Testability: DI and Context Decoupling

### Goal
Add dependency injection seams and decouple the run engine from the CLI layer.

### Representative Code Snippets

```python
# tools/cq/core/run_context.py
"""Execution context protocol for the run engine."""
from __future__ import annotations
from pathlib import Path
from typing import Protocol

class RunExecutionContext(Protocol):
    """Minimal context the run engine needs -- CLI-independent."""
    @property
    def root(self) -> Path: ...
    @property
    def argv(self) -> list[str]: ...
    @property
    def toolchain(self) -> object: ...
    @property
    def artifact_dir(self) -> Path | None: ...
```

```python
# tools/cq/core/cache/diskcache_backend.py -- add test injection
def set_cq_cache_backend(*, root: Path, backend: CqCacheBackend) -> None:
    """Inject a cache backend for a workspace root."""
    workspace = normalize_workspace_root(root)
    with _BACKEND_LOCK:
        _BACKEND_STATE.backends[workspace] = backend
```

### Files to Edit
- `tools/cq/core/run_context.py` -- add `RunExecutionContext` protocol alongside existing `RunContext` struct
- `tools/cq/run/runner.py`, `step_executors.py`, `q_step_collapsing.py` -- depend on `RunExecutionContext` protocol instead of concrete `CliContext`
- `tools/cq/run/chain.py` -- accept `App` as parameter instead of importing singleton
- `tools/cq/core/cache/diskcache_backend.py` -- add `set_cq_cache_backend`
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py` -- add optional `cache` parameter
- `tools/cq/query/execution_context.py` -- add optional `cache_backend` and `symtable_enricher` fields

### New Files to Create
- `tests/unit/cq/run/test_execution_context_protocol.py`

### Legacy Decommission/Delete Scope
- Remove `from tools.cq.cli_app.context import CliContext` from `runner.py`, `step_executors.py`, `q_step_collapsing.py` (replaced by protocol)
- Remove `from tools.cq.cli_app.app import app` from `chain.py` (passed as parameter)

---

## S17. Type LDMD Writer and Create `CallsRequest` Struct

### Goal
Replace untyped `getattr` access in LDMD writer with typed attribute access. Create `CallsRequest` struct to align `cmd_calls` with other macro signatures.

### Representative Code Snippets

```python
# tools/cq/ldmd/writer.py -- BEFORE
def _emit_snb_target(lines: list[str], bundle: object) -> None:
    subject = getattr(bundle, "subject", None)
    if subject is None:
        return
    name = getattr(subject, "name", "unknown")

# AFTER
from tools.cq.core.snb_schema import SemanticNeighborhoodBundleV1

def _emit_snb_target(lines: list[str], bundle: SemanticNeighborhoodBundleV1) -> None:
    subject = bundle.subject
    name = subject.name if subject else "unknown"
```

```python
# tools/cq/macros/contracts.py
class CallsRequest(ScopedMacroRequestBase, frozen=True):
    """Request struct for the calls macro, aligned with other macros."""
    function_name: str
```

### Files to Edit
- `tools/cq/ldmd/writer.py` -- type `bundle` parameter as `SemanticNeighborhoodBundleV1`; replace 23 `getattr` calls
- `tools/cq/macros/contracts.py` -- add `CallsRequest` struct
- `tools/cq/macros/calls/entry.py` -- refactor `cmd_calls` to accept `CallsRequest`
- `tools/cq/cli_app/commands/analysis.py` -- update `calls` command to construct `CallsRequest`
- `tools/cq/core/request_factory.py` -- produce `CallsRequest` through request factory
- `tools/cq/core/services.py` -- update calls service to use `CallsRequest`
- `tools/cq/run/step_executors.py` -- update calls step executor
- `tools/cq/perf/smoke_report.py` -- update direct `cmd_calls` invocation
- `tests/unit/cq/macros/test_calls.py` -- migrate direct invocations to request object
- `tests/integration/cq/test_calls_integration.py` -- migrate direct invocations to request object
- `tests/unit/cq/search/test_performance_smoke.py` -- migrate direct invocations to request object

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete 4-positional-arg `cmd_calls(tc, root, argv, function_name)` signature (replaced by `cmd_calls(request: CallsRequest)`)

---

## S18. Reduce Cache `__init__.py` Surface and Extract `MacroResultBuilder`

### Goal
Reduce the cache package's public surface from 70+ to ~10 symbols with immediate import-site migration. Extract a shared `MacroResultBuilder` to reduce boilerplate across 8 macro implementations.

### Representative Code Snippets

```python
# tools/cq/core/cache/__init__.py -- trim to essentials
__all__ = [
    "CqCacheBackend",
    "NoopCacheBackend",
    "CqCachePolicyV1",
    "default_cache_policy",
    "get_cq_cache_backend",
    "set_cq_cache_backend",
    "close_cq_cache_backend",
]
```

```python
# tools/cq/macros/result_builder.py
"""Shared result-building template for CQ macros."""
from __future__ import annotations

class MacroResultBuilder:
    """Encapsulate the common RunContext -> mk_result -> summary -> scoring flow."""

    def __init__(self, macro_name: str, root: Path, *, run_id: str = "") -> None: ...
    def set_summary(self, **kwargs: object) -> MacroResultBuilder: ...
    def set_scoring(self, details: ScoringDetailsV1) -> MacroResultBuilder: ...
    def add_findings(self, findings: list[Finding]) -> MacroResultBuilder: ...
    def add_section(self, section: Section) -> MacroResultBuilder: ...
    def apply_rust_fallback(self) -> MacroResultBuilder: ...
    def build(self) -> CqResult: ...
```

### Files to Edit
- `tools/cq/core/cache/__init__.py` -- trim exports
- Internal cache consumers -- update to import directly from submodules (hard cutover, no transition exports)
- `tools/cq/macros/impact.py`, `sig_impact.py`, `imports.py`, `scopes.py`, `exceptions.py`, `side_effects.py`, `bytecode.py`, `calls/entry.py` -- use `MacroResultBuilder`
- `tests/unit/cq/**` modules importing from `tools.cq.core.cache` -- update imports to explicit submodules

### New Files to Create
- `tools/cq/macros/result_builder.py`
- `tests/unit/cq/macros/test_result_builder.py`

### Legacy Decommission/Delete Scope
- Delete duplicated result-building boilerplate from each of the 8 macro files (replaced by `MacroResultBuilder`)

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S2)
- Delete all local `PositiveInt`/`NonNegativeInt` definitions outside `contracts_constraints.py`
- Delete all local `_env_bool`/`_env_int` definitions outside `runtime/env_namespace.py`
- Delete all local `coerce_float` definitions outside `type_coercion.py`
- Delete all duplicated rendering helper functions from `report.py`, `render_overview.py`, `render_enrichment.py`, `render_summary.py`

### Batch D2 (after S3, S4)
- Delete all duplicated constants from `python_lane/runtime.py` and `python_lane/facts.py`
- Delete `node_text` from `_shared/core.py`
- Delete `_STDLIB_PREFIXES` from `imports.py`
- Delete `plan_feasible_slices` from `bundle_builder.py`
- Delete all duplicated functions listed in S4

### Batch D3 (after S5)
- Delete `executor_bytecode.py`, `executor_cache.py` (empty placeholders)
- Delete `executor_metavars.py` (pure re-export)
- Delete `index/file_hash.py`

### Batch D4 (after S9, S10, S11, S12, S13)
- Delete `tools/cq/core/bundles.py`, `multilang_orchestrator.py`, `multilang_summary.py`, `schema_export.py`, `request_factory.py` (moved to `orchestration/`)
- Delete `tools/cq/core/front_door_insight.py` (split into 3 files)
- All God Module source files become thin facades or are fully replaced

### Batch D5 (after S14, S15)
- Delete `if lang == "python"` / `if lang == "rust"` dispatch blocks replaced by adapter/registry dispatch
- Delete `isinstance` payload checks replaced by typed enrichment facts

---

## Implementation Sequence

1. **Baseline revalidation** -- Validate file manifests, path references, and line-level anchors in this plan against the current tree before implementation starts.
2. **S1** -- Rendering helpers consolidation (no dependencies, immediate DRY win)
3. **S2** -- Type alias/env parsing/coercion consolidation (no dependencies, immediate DRY win)
4. **S3** -- Tree-sitter Python lane constants (no dependencies)
5. **S5** -- Placeholder cleanup and export fixes (no dependencies, reduces noise for later work)
6. **S6** -- Literal types and contract guards (no dependencies, catches bugs at type-check time)
7. **S7** -- CQS fixes (no dependencies, clarifies mutation semantics)
8. **S4** -- Cross-module function consolidation (depends on S3 for tree-sitter constants being in place)
9. **S8** -- Observability (after consolidation to avoid adding loggers to code that immediately moves)
10. **S9** -- Core dependency direction fix (foundational structural cutover)
11. **S12** -- front_door_insight.py decomposition (independent once core direction is fixed)
12. **S10** -- smart_search.py second-stage decomposition (after S4/S9)
13. **S11** -- executor.py second-stage decomposition (after S4)
14. **S13** -- Secondary module splits (depends on S4 helpers and S9 layering)
15. **S16** -- DI and context decoupling (depends on S9 for core boundaries)
16. **S17** -- LDMD typing and `CallsRequest` cutover (independent from god-module splits)
17. **S18** -- Cache surface reduction and `MacroResultBuilder` hard-cutover
18. **S14** -- `LanguageEnrichmentPort` protocol and dispatch registry
19. **S15** -- Typed enrichment payload full cutover (depends on S14 dispatch and S10/S11 extraction)

**Rationale:** Foundational deduplication and type-safety work lands first. Structural boundary corrections then remove core dependency violations. Protocol and payload hard-cutovers land after those seams exist so behavior changes are introduced once and not revisited.

---

## Implementation Checklist

- [ ] Baseline revalidation completed (file counts, paths, and anchors refreshed before implementation)
- [ ] S1. Create `core/render_utils.py` and consolidate 6 rendering helpers
- [ ] S2. Consolidate constrained type aliases and env parsing to canonical locations
- [ ] S3. Create `python_lane/constants.py` for shared tree-sitter constants
- [ ] S4. Consolidate all cross-module duplicated functions (pipeline, query, CLI, macros, tree-sitter, neighborhood)
- [ ] S5. Delete empty placeholders, fix exports, clean up aliases
- [ ] S6. Add Literal types, enum constraints, and replace assert guards
- [ ] S7. Fix CQS violations in 5 functions
- [ ] S8. Add structured logging to 13 modules across all subsystems
- [ ] S9. Move orchestration files out of `core/` and remove remaining outward `core/` imports
- [ ] S10. Decompose `smart_search.py` into summary/sections/followups
- [ ] S11. Complete second-stage `executor.py` decomposition with thin dispatch facade
- [ ] S12. Decompose `front_door_insight.py` into schema/builders/render
- [ ] S13. Decompose 4 secondary mixed-concern modules
- [ ] S14. Define `LanguageEnrichmentPort` protocol and language adapters
- [ ] S15. Introduce typed enrichment payloads (full cutover for Python and Rust paths)
- [ ] S16. Add DI seams and decouple run engine from CLI
- [ ] S17. Type LDMD writer and create `CallsRequest` struct
- [ ] S18. Reduce cache `__init__.py` surface and extract `MacroResultBuilder`
- [ ] D1. Delete legacy type aliases, env parsers, and rendering helpers (after S1, S2)
- [ ] D2. Delete legacy constants and duplicated functions (after S3, S4)
- [ ] D3. Delete empty placeholders and dead code (after S5)
- [ ] D4. Delete moved orchestration files and split source files (after S9-S13)
- [ ] D5. Delete dispatch blocks and `isinstance` payload checks replaced by S14/S15 (after S14, S15)
