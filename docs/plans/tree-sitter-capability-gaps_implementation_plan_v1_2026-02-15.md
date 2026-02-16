# Tree-Sitter Capability Gaps Implementation Plan v1 (2026-02-15)

## Scope Summary

This plan addresses 12 tree-sitter API capability gaps identified by cross-referencing py-tree-sitter 0.25+ documentation against actual usage in `tools/cq/search/tree_sitter/`. The scope spans safety (ABI gating + single construction gate), provenance (grammar version stamping), performance (field ID hot-path, byte-seek navigation, cursor pooling), enrichment (highlights and tags query packs), grammar intelligence (supertype taxonomy, node visibility), and runtime correctness (injection.combined handling, Node.has_changes filtering).

**Design stance:** No compatibility shims. All new APIs are py-tree-sitter 0.25+ only with fail-open guards. Each scope item is independently deployable. No existing behavior changes unless the scope item explicitly says so.

---

## Design Principles

1. **Fail-open always.** Any new API call that raises `AttributeError`, `TypeError`, or `RuntimeError` must degrade gracefully — never block parsing, query execution, or enrichment.
2. **Contract-first.** New metadata fields are added to existing `CqStruct` contracts before wiring into runtime code.
3. **No `# noqa` or `# type: ignore`.** Use `cast()` or `TYPE_CHECKING` guards for tree-sitter API types.
4. **Incremental deployment.** Each scope item can land independently; cross-scope deletions are in a separate batch section.
5. **msgspec boundary.** All new serialized structs use `CqStruct` (msgspec.Struct). No pydantic in hot paths.
6. **Single language construction gate.** `Language(...)` object creation happens only in `core/language_registry.py`; all other modules consume that loader.

---

## Current Baseline

- **Runtime versions (verified in environment):** `tree-sitter==0.25.2`, `tree-sitter-python==0.25.0`, `tree-sitter-rust==0.24.0`
- **Language loading:** `language_registry.py` has one cached loader, but direct `Language(...)` constructors also exist in `schema/node_schema.py`, `python_lane/runtime.py`, `python_lane/facts.py`, and `rust_lane/runtime.py`
- **No `Language.abi_version` usage** anywhere in codebase
- **No `Language.semantic_version` or `Language.name` usage** for provenance (and `semantic_version` in 0.25 is tuple-typed, not `str`)
- **`supertypes` field exists** on `TreeSitterLanguageRegistryV1` but is always `()` — never populated from runtime `Language.supertypes` ids
- **`build_runtime_ids()` and `build_runtime_field_ids()`** exist at `node_schema.py:188-208` but are never called from hot-path code
- **28 `child_by_field_name()` calls** across 5 files in `tools/cq/search/tree_sitter/` — all using string lookups, none using cached field IDs
- **Field enumeration bug:** runtime schema loading uses `range(field_count)`; tree-sitter field ids are 1..`field_count`, so the highest id is currently skipped
- **`InjectionSettingsV1.combined` field** is parsed at `injection_config.py:49` but `injection_runtime.py` treats all plans uniformly (no grouping for combined parsing)
- **`_advance_cursor()` in `structural/export.py`** uses only `goto_first_child()`, `goto_next_sibling()`, `goto_parent()` — no byte-seek or cursor copy
- **`TreeCursor.goto_first_child_for_byte()` return semantics are not handled:** API returns child index (`int | None`), where `0` is valid and must not be treated as falsey
- **Cursor reset semantics are not handled:** `TreeCursor.reset_to()` resets from another cursor; node-based reset is `TreeCursor.reset(node)`
- **No `.scm` files** exist under `tools/cq/search/tree_sitter/` — query packs are loaded from registry/distribution
- **Distribution query loading is Rust-only in `query/registry.py`**; Python wheel queries (`highlights.scm`, `tags.scm`) are not currently loaded via the same path
- **`build_tag_events()` exists** at `tags.py` but only recognizes `role.definition` / `role.reference`; standard captures like `definition.function` / `reference.call` are dropped
- **`QueryCursor.set_containing_*` APIs are not available in current runtime bindings** and require capability-gated fallback to byte/point range modes
- **`semantic_overlays.py`** has been removed/moved (only `.pyc` cache remains)
- **`Node.has_changes` is not used** anywhere in the codebase

---

## S1. ABI Version Gating

### Goal

Add startup ABI compatibility assertion using `tree_sitter.LANGUAGE_VERSION`, `tree_sitter.MIN_COMPATIBLE_LANGUAGE_VERSION`, and `Language.abi_version` in the language registry. This prevents silent corruption from mismatched grammar wheel versions.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/language_registry.py
import tree_sitter as _ts

def _assert_abi_compatible(language_obj: object, language_name: str) -> None:
    """Assert grammar ABI version is compatible with runtime."""
    ts_max = getattr(_ts, "LANGUAGE_VERSION", None)
    ts_min = getattr(_ts, "MIN_COMPATIBLE_LANGUAGE_VERSION", None)
    abi = getattr(language_obj, "abi_version", None)
    if ts_max is None or ts_min is None or abi is None:
        return  # fail-open: version introspection unavailable
    if not (ts_min <= abi <= ts_max):
        msg = (
            f"tree-sitter ABI mismatch for {language_name}: "
            f"grammar abi_version={abi}, runtime accepts [{ts_min}, {ts_max}]"
        )
        raise RuntimeError(msg)
```

```python
# Called in load_tree_sitter_language() after constructing Language object
lang_obj = _TreeSitterLanguage(_tree_sitter_python.language())
_assert_abi_compatible(lang_obj, "python")
return lang_obj
```

### Files to Edit

- `tools/cq/search/tree_sitter/core/language_registry.py` — add `_assert_abi_compatible()`, wire into `load_tree_sitter_language()`

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_abi_gating.py` — test ABI mismatch raises, ABI in-range passes, missing attributes degrade gracefully

### Legacy Decommission/Delete Scope

- None. This is a new safety guard.

---

## S2. Grammar Provenance Stamping

### Goal

Record `Language.semantic_version`, `Language.name`, and `Language.abi_version` in grammar metadata contracts for full reproducibility. Use tuple-typed semantic versioning to match py-tree-sitter 0.25 runtime behavior and avoid lossy string coercion.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/schema/node_schema.py
class GrammarSchemaV1(CqStruct, frozen=True):
    """Simplified grammar schema for lint-time checks."""
    language: str
    node_types: tuple[GrammarNodeTypeV1, ...] = ()
    grammar_name: str | None = None
    semantic_version: tuple[int, int, int] | None = None
    abi_version: int | None = None
```

```python
# tools/cq/search/tree_sitter/core/language_registry.py
class TreeSitterLanguageRegistryV1(CqStruct, frozen=True):
    language: str
    node_kinds: tuple[str, ...] = ()
    named_node_kinds: tuple[str, ...] = ()
    field_names: tuple[str, ...] = ()
    supertypes: tuple[str, ...] = ()
    grammar_name: str | None = None
    semantic_version: tuple[int, int, int] | None = None
    abi_version: int | None = None
```

```python
# Extraction helper at schema load time
def _extract_provenance(
    language_obj: object,
) -> tuple[str | None, tuple[int, int, int] | None, int | None]:
    """Extract provenance fields from a Language object."""
    name = getattr(language_obj, "name", None)
    sem_ver = getattr(language_obj, "semantic_version", None)
    abi = getattr(language_obj, "abi_version", None)

    sem_tuple: tuple[int, int, int] | None = None
    if (
        isinstance(sem_ver, tuple)
        and len(sem_ver) == 3
        and all(isinstance(part, int) and not isinstance(part, bool) for part in sem_ver)
    ):
        sem_tuple = (int(sem_ver[0]), int(sem_ver[1]), int(sem_ver[2]))

    return (
        str(name) if isinstance(name, str) else None,
        sem_tuple,
        int(abi) if isinstance(abi, int) and not isinstance(abi, bool) else None,
    )
```

```python
# tools/cq/search/tree_sitter/contracts/query_models.py
class QueryPackPlanV1(CqStruct, frozen=True):
    pack_name: str
    query_hash: str = ""
    plans: tuple[QueryPatternPlanV1, ...] = ()
    score: float = 0.0
    grammar_name: str | None = None
    semantic_version: tuple[int, int, int] | None = None
    abi_version: int | None = None
```

### Files to Edit

- `tools/cq/search/tree_sitter/schema/node_schema.py` — add provenance fields to `GrammarSchemaV1`, extract in `load_grammar_schema()`
- `tools/cq/search/tree_sitter/core/language_registry.py` — add provenance fields to `TreeSitterLanguageRegistryV1`, populate in `load_language_registry()`
- `tools/cq/search/tree_sitter/contracts/query_models.py` — add tuple-typed semantic version and ABI fields to `QueryPackPlanV1`
- `tools/cq/search/tree_sitter/query/planner.py` — stamp provenance into `build_pack_plan()` output from registry-loaded language metadata

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_grammar_provenance.py` — test provenance extraction from runtime Language objects, test stamping flows through to contracts

### Legacy Decommission/Delete Scope

- None. Additive fields only.

---

## S3. `Node.has_changes` Filter

### Goal

Use `node.has_changes` as a secondary filter during node-level processing within changed ranges. Apply this filter only when incremental parse reuse is confirmed (non-empty changed-range context from parse session); otherwise keep current full processing behavior.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/python_lane/facts.py — in enrichment pass
def _should_enrich_node(node: Node, *, within_changed_range: bool) -> bool:
    """Check if a node warrants full enrichment."""
    if not within_changed_range:
        return True
    has_changes = getattr(node, "has_changes", None)
    if has_changes is None:
        return True  # fail-open: attribute unavailable
    return bool(has_changes)
```

```python
# tools/cq/search/tree_sitter/core/runtime.py — windowed query execution
# Before processing a capture node in expensive enrichment:
for capture_name, nodes in captures.items():
    for node in nodes:
        if not _should_enrich_node(node, within_changed_range=has_change_context):
            continue
        # ... expensive enrichment ...
```

### Files to Edit

- `tools/cq/search/tree_sitter/python_lane/facts.py` — add `has_changes` check in `_collect_query_pack_captures()` loop
- `tools/cq/search/tree_sitter/core/runtime.py` — add optional `has_changes` filter in `run_bounded_query_captures()` only when changed-range context indicates incremental reuse

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_has_changes_filter.py` — test node filter with mock nodes having `has_changes=True/False/None`

### Legacy Decommission/Delete Scope

- None. Additive optimization only.

---

## S4. Byte-Seek Navigation

### Goal

Use `TreeCursor.goto_first_child_for_byte()` in traversal patterns that process changed ranges sequentially within wide container nodes. Treat its return value as `int | None` (child index), where `0` is a valid success result. Also use `Node.first_child_for_byte()` / `Node.first_named_child_for_byte()` for direct node access patterns.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/structural/export.py — byte-seek variant
def _advance_cursor_to_byte(cursor: Any, target_byte: int) -> bool:
    """Advance cursor to first child containing target byte offset."""
    goto = getattr(cursor, "goto_first_child_for_byte", None)
    if goto is None:
        return cursor.goto_first_child()  # fail-open: use standard traversal
    try:
        idx = goto(target_byte)
        return idx is not None
    except (TypeError, ValueError, RuntimeError):
        return cursor.goto_first_child()
```

```python
# tools/cq/search/tree_sitter/python_lane/facts.py — direct node access
def _find_child_at_byte(node: Node, byte_offset: int) -> Node | None:
    """Find first named child at byte offset using byte-seek API."""
    first_named = getattr(node, "first_named_child_for_byte", None)
    if first_named is not None:
        try:
            return first_named(byte_offset)
        except (TypeError, ValueError, RuntimeError):
            pass
    # fallback: linear scan
    for child in node.named_children:
        if int(getattr(child, "start_byte", 0)) <= byte_offset < int(getattr(child, "end_byte", 0)):
            return child
    return None
```

### Files to Edit

- `tools/cq/search/tree_sitter/structural/export.py` — add byte-seek variant of `_advance_cursor()`; use in windowed structural export
- `tools/cq/search/tree_sitter/python_lane/facts.py` — add `_find_child_at_byte()` helper; use in `_lift_anchor()` and descendant lookups

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_byte_seek_navigation.py` — test byte-seek cursor advancement, fallback when API unavailable, boundary conditions

### Legacy Decommission/Delete Scope

- None. Additive optimization with fallback.

---

## S5. TreeCursor Pooling

### Goal

Use `TreeCursor.copy()` for cursor pooling and support both reset APIs correctly: `reset_to(other_cursor)` for cursor-to-cursor rewind, and `reset(node)` for node-based repositioning.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/structural/export.py
def _fork_cursor(cursor: Any) -> Any | None:
    """Create a cursor copy for fork-and-explore traversal."""
    copy_fn = getattr(cursor, "copy", None)
    if copy_fn is None:
        return None
    try:
        return copy_fn()
    except (TypeError, RuntimeError, AttributeError):
        return None


def _reset_cursor_to(cursor: Any, *, from_cursor: Any | None = None, node: object | None = None) -> bool:
    """Reset a pooled cursor from another cursor or a node."""
    reset_to_fn = getattr(cursor, "reset_to", None)
    if from_cursor is not None and callable(reset_to_fn):
        try:
            reset_to_fn(from_cursor)
            return True
        except (TypeError, RuntimeError, AttributeError):
            pass
    reset_fn = getattr(cursor, "reset", None)
    if node is None or not callable(reset_fn):
        return False
    try:
        reset_fn(node)
        return True
    except (TypeError, RuntimeError, AttributeError):
        return False
```

```python
# Usage in structural export subtree exploration:
forked = _fork_cursor(cursor)
if forked is not None:
    # explore subtree with forked cursor
    _explore_subtree(forked, ...)
    # original cursor position unchanged
```

### Files to Edit

- `tools/cq/search/tree_sitter/structural/export.py` — add cursor pooling helpers, use in `export_structural_rows()`

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_cursor_pooling.py` — test copy/reset_to/reset behaviors, test fallback when methods unavailable

### Legacy Decommission/Delete Scope

- None. Additive optimization.

---

## S6. Supertype Taxonomy

### Goal

Use `Language.supertypes` and `Language.subtypes()` to derive taxonomy from grammar rather than maintaining hardcoded kind lists. In py-tree-sitter 0.25, these APIs are id-based (`int`), so taxonomy loading must resolve ids back to node-kind names explicitly. Populate `TreeSitterLanguageRegistryV1.supertypes` with resolved names and retain id mappings in schema indexes.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/language_registry.py
def _load_supertype_ids(language_obj: object) -> tuple[int, ...]:
    """Load supertype ids from Language.supertypes."""
    supertypes_attr = getattr(language_obj, "supertypes", None)
    if supertypes_attr is None:
        return ()
    try:
        return tuple(sorted(int(st) for st in supertypes_attr if isinstance(st, int)))
    except (TypeError, RuntimeError):
        return ()


def _kind_name(language_obj: object, kind_id: int) -> str | None:
    node_kind_for_id = getattr(language_obj, "node_kind_for_id", None)
    if not callable(node_kind_for_id):
        return None
    try:
        value = node_kind_for_id(kind_id)
    except (TypeError, RuntimeError, ValueError):
        return None
    return value if isinstance(value, str) and value else None


def _load_subtype_ids(language_obj: object, supertype_id: int) -> tuple[int, ...]:
    """Load subtype ids for one supertype via Language.subtypes()."""
    subtypes_fn = getattr(language_obj, "subtypes", None)
    if subtypes_fn is None or not callable(subtypes_fn):
        return ()
    try:
        return tuple(sorted(int(st) for st in subtypes_fn(supertype_id) if isinstance(st, int)))
    except (TypeError, RuntimeError):
        return ()
```

```python
# Populate supertypes in load_language_registry()
lang_obj = load_tree_sitter_language(language)
supertype_ids = _load_supertype_ids(lang_obj) if lang_obj is not None else ()
supertypes = tuple(
    name
    for supertype_id in supertype_ids
    if (name := _kind_name(lang_obj, supertype_id)) is not None
)
return TreeSitterLanguageRegistryV1(
    language=language,
    node_kinds=node_kinds,
    named_node_kinds=named_node_kinds,
    field_names=field_names,
    supertypes=supertypes,
)
```

```python
# tools/cq/search/tree_sitter/schema/node_schema.py — supertype index
class SupertypeIndexV1(CqStruct, frozen=True):
    """Supertype → subtypes mapping derived from grammar."""
    supertype_id: int
    supertype: str
    subtype_ids: tuple[int, ...] = ()
    subtypes: tuple[str, ...] = ()


def build_supertype_index(language_obj: object) -> tuple[SupertypeIndexV1, ...]:
    """Build full supertype taxonomy from Language.supertypes + subtypes()."""
    supertype_ids = _load_supertype_ids(language_obj)
    rows: list[SupertypeIndexV1] = []
    for supertype_id in supertype_ids:
        subtype_ids = _load_subtype_ids(language_obj, supertype_id)
        subtype_names = tuple(
            name
            for subtype_id in subtype_ids
            if (name := _kind_name(language_obj, subtype_id)) is not None
        )
        rows.append(
            SupertypeIndexV1(
                supertype_id=supertype_id,
                supertype=_kind_name(language_obj, supertype_id) or f"id:{supertype_id}",
                subtype_ids=subtype_ids,
                subtypes=subtype_names,
            )
        )
    return tuple(rows)
```

### Files to Edit

- `tools/cq/search/tree_sitter/core/language_registry.py` — populate `supertypes` field (currently always `()`) from `Language.supertypes`
- `tools/cq/search/tree_sitter/schema/node_schema.py` — add `SupertypeIndexV1` struct, add `build_supertype_index()`, integrate with `GrammarSchemaV1`

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_supertype_taxonomy.py` — test supertype loading, subtype resolution, index building, graceful degradation

### Legacy Decommission/Delete Scope

- `tools/cq/search/tree_sitter/core/language_registry.py:54` — remove hardcoded `supertypes=()` assignment; replace with dynamic loading.

---

## S7. Highlights Query Pack Execution

### Goal

Load and execute standard `highlights.scm` query packs shipped with Python and Rust grammar wheels to produce a complete semantic token overlay. The loader must be registry-based (not Rust-manifest-based) so both languages use one distribution-query ingestion path.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/highlights_runner.py (new file)
"""Execute highlights.scm query packs for semantic token overlay."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)

if TYPE_CHECKING:
    from tree_sitter import Node


class HighlightTokenV1(CqStruct, frozen=True):
    """One semantic token from highlights.scm execution."""
    capture_name: str       # e.g. "keyword", "function", "type", "string"
    start_byte: int
    end_byte: int
    start_line: int         # 0-based
    start_col: int          # 0-based


class HighlightsResultV1(CqStruct, frozen=True):
    """Result of highlights.scm execution over one file/region."""
    language: str
    token_count: int = 0
    tokens: tuple[HighlightTokenV1, ...] = ()
    grammar_name: str | None = None
    semantic_version: tuple[int, int, int] | None = None
    abi_version: int | None = None


def run_highlights_pack(
    *,
    language: str,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1 | None = None,
) -> HighlightsResultV1:
    """Execute highlights.scm and return byte-sorted token stream."""
    ...
```

```python
# tools/cq/search/tree_sitter/query/registry.py — unified distribution source loader
def load_distribution_query_source(language: str, pack_name: str) -> str | None:
    for row in _load_distribution_queries(language):
        if row.pack_name == pack_name:
            return row.source
    return None
```

### Files to Edit

- `tools/cq/search/tree_sitter/query/registry.py` — generalize distribution query loading beyond Rust and add `load_distribution_query_source(language, pack_name)`
- `tests/unit/cq/search/tree_sitter/test_query_registry.py` — add Python distribution query coverage and single-pack lookup assertions.
- `tests/unit/cq/search/tree_sitter/test_query_registry_profiles.py` — verify profile behavior remains deterministic after Python distribution support is introduced.

### New Files to Create

- `tools/cq/search/tree_sitter/core/highlights_runner.py` — resolve `highlights.scm` through query registry and execute over bounded windows.
- `tests/unit/cq/search/tree_sitter/test_highlights_runner.py` — test token extraction, byte-ordering, windowed execution, graceful degradation

### Legacy Decommission/Delete Scope

- None. New capability.

---

## S8. Tags Query Pack for Rust

### Goal

Normalize standard `tags.scm` capture taxonomies into `RustTagEventV1` for Rust, including both CQ-style (`role.definition` / `role.reference`) and grammar-style (`definition.*` / `reference.*`) captures. This preserves compatibility while enabling distribution tags to flow through the existing runtime path.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/tags.py — broaden role normalization
def _normalized_role(capture_names: Sequence[str]) -> str | None:
    for name in capture_names:
        if name.startswith("role.definition") or name.startswith("definition."):
            return "definition"
        if name.startswith("role.reference") or name.startswith("reference."):
            return "reference"
    return None


def build_tag_events(
    *,
    matches: Sequence[tuple[int, Mapping[str, Sequence[NodeLike]]]],
    source_bytes: bytes,
) -> tuple[RustTagEventV1, ...]:
    rows: list[RustTagEventV1] = []
    for pattern_idx, capture_map in matches:
        role = _normalized_role(tuple(capture_map.keys()))
        if role is None:
            continue
        names = capture_map.get("name", ())
        if not names:
            continue
        node = names[0]
        text = node_text(node, source_bytes)
        if not text:
            continue
        rows.append(
            RustTagEventV1(
                role=role,
                kind="symbol",
                name=text,
                start_byte=int(getattr(node, "start_byte", 0)),
                end_byte=int(getattr(node, "end_byte", 0)),
                metadata={"pattern_index": pattern_idx},
            )
        )
    return tuple(rows)
```

### Files to Edit

- `tools/cq/search/tree_sitter/tags.py` — normalize role capture taxonomies (`role.*` and `definition.*` / `reference.*`)
- `tools/cq/search/tree_sitter/rust_lane/runtime.py` — ensure `tags.scm` distribution pack remains enabled in runtime profiles and routed through `build_tag_events()`
- `tests/unit/cq/search/tree_sitter/test_tags.py` — add taxonomy fixtures for `definition.*` / `reference.*` capture names.
- `tests/unit/cq/search/tree_sitter/test_rust_lane_runtime.py` — validate distribution `tags.scm` rows flow through runtime event assembly.

### New Files to Create

- None.

### Legacy Decommission/Delete Scope

- None. Additive capability.

---

## S9. `injection.combined` Property Handling

### Goal

Handle the `injection.combined` flag in Rust injection processing. When set, all matching `@injection.content` ranges should be parsed as one logical embedded document using multiple disjoint `included_ranges` on a single parser run instead of parsing each range separately.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/rust_lane/injection_runtime.py
def parse_injected_ranges(
    *,
    source_bytes: bytes,
    language: Language,
    plans: tuple[InjectionPlanV1, ...],
) -> InjectionRuntimeResultV1:
    """Parse injected ranges, grouping combined plans together."""
    combined_plans = tuple(p for p in plans if bool(getattr(p, "combined", False)))
    separate_plans = tuple(p for p in plans if not bool(getattr(p, "combined", False)))

    # Combined: all ranges parsed as one logical document
    combined_result = _parse_combined_injection(
        source_bytes=source_bytes,
        language=language,
        plans=combined_plans,
    ) if combined_plans else None

    # Separate: each range parsed individually (current behavior)
    separate_result = _parse_separate_injections(
        source_bytes=source_bytes,
        language=language,
        plans=separate_plans,
    ) if separate_plans else None

    return _merge_injection_results(combined_result, separate_result, plans=plans)


def _parse_combined_injection(
    *,
    source_bytes: bytes,
    language: Language,
    plans: tuple[InjectionPlanV1, ...],
) -> InjectionRuntimeResultV1:
    """Parse all combined plans as one document with disjoint included_ranges."""
    parser = _TreeSitterParser(language)
    ranges = [
        _TreeSitterRange(
            _TreeSitterPoint(int(row.start_row), int(row.start_col)),
            _TreeSitterPoint(int(row.end_row), int(row.end_col)),
            int(row.start_byte),
            int(row.end_byte),
        )
        for row in plans
    ]
    parser.included_ranges = ranges
    tree = parser.parse(source_bytes)
    ...
```

### Files to Edit

- `tools/cq/search/tree_sitter/rust_lane/injection_runtime.py` — split parsing logic into combined vs separate paths; group combined plans by language

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_injection_combined.py` — test combined grouping, separate fallback, mixed plans

### Legacy Decommission/Delete Scope

- `tools/cq/search/tree_sitter/rust_lane/injection_runtime.py` — the current flat `parse_injected_ranges()` implementation that ignores the `combined` flag will be replaced by the split implementation.

---

## S10. Field ID Hot-Path Optimization

### Goal

Wire field IDs from `build_runtime_field_ids()` / `build_runtime_ids()` into hot-path node access code, and expand field-id precomputation beyond the current fixed trio (`name`, `body`, `parameters`). Fix runtime field enumeration so id `field_count` is not skipped.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/infrastructure.py — cached field IDs
from functools import lru_cache
from tools.cq.search.tree_sitter.schema.node_schema import (
    build_runtime_field_ids,
    build_runtime_ids,
    load_grammar_schema,
)

@lru_cache(maxsize=4)
def _cached_field_ids(language: str) -> dict[str, int]:
    """Load and cache field IDs for a language."""
    lang_obj = load_language(language)
    schema = load_grammar_schema(language)
    field_names = tuple(
        sorted({field for row in schema.node_types for field in row.fields})
    ) if schema is not None else ()
    return build_runtime_field_ids(lang_obj, field_names=field_names)


@lru_cache(maxsize=4)
def _cached_node_ids(language: str) -> dict[str, int]:
    """Load and cache node-kind IDs for a language."""
    lang_obj = load_language(language)
    return build_runtime_ids(lang_obj)
```

```python
# tools/cq/search/tree_sitter/schema/node_schema.py — field-id enumeration fix
field_names = tuple(
    field_name
    for field_id in range(1, field_count + 1)
    if isinstance((field_name := runtime_language.field_name_for_id(field_id)), str)
    and field_name
)
```

```python
# tools/cq/search/tree_sitter/core/node_utils.py or infrastructure.py
def child_by_field(node: Node, field_name: str, field_ids: dict[str, int]) -> Node | None:
    """Access child by cached field ID with string-name fallback."""
    field_id = field_ids.get(field_name)
    if field_id is not None:
        by_id = getattr(node, "child_by_field_id", None)
        if by_id is not None:
            try:
                return by_id(field_id)
            except (TypeError, ValueError, RuntimeError):
                pass
    return node.child_by_field_name(field_name)
```

```python
# Hot-path conversion example (rust_lane/runtime.py _optional_field_text):
# Before:
child = parent.child_by_field_name(field_name)
# After:
child = child_by_field(parent, field_name, _cached_field_ids("rust"))
```

### Files to Edit

- `tools/cq/search/tree_sitter/core/infrastructure.py` — add `_cached_field_ids()`, `_cached_node_ids()`, and `child_by_field()` helper; source candidate field names from runtime schema
- `tools/cq/search/tree_sitter/schema/node_schema.py` — fix field id enumeration (`1..field_count`) and extend `build_runtime_field_ids()` to accept caller-provided field-name candidates
- `tools/cq/search/tree_sitter/python_lane/facts.py` — convert 2 `child_by_field_name()` call sites
- `tools/cq/search/tree_sitter/python_lane/fallback_support.py` — convert 6 `child_by_field_name()` call sites
- `tools/cq/search/tree_sitter/python_lane/locals_index.py` — convert 2 `child_by_field_name()` call sites
- `tools/cq/search/tree_sitter/python_lane/runtime.py` — convert 1 `child_by_field_name()` call site
- `tools/cq/search/tree_sitter/rust_lane/runtime.py` — convert 17 `child_by_field_name()` call sites (heaviest usage)

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_field_id_optimization.py` — test `child_by_field()` with cached IDs, test fallback to string name, benchmark comparison

### Legacy Decommission/Delete Scope

- None. The `child_by_field()` helper wraps `child_by_field_name()` with a fast-path, so original API usage is preserved as fallback internally.

---

## S11. Supertype/Visible Classification

### Goal

Use `Language.node_kind_is_visible()` and `Language.node_kind_is_supertype()` in schema building and structural export to provide a more principled node classification (visible-named, visible-anonymous, hidden-supertype) beyond the current named/unnamed binary.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/schema/node_schema.py
class GrammarNodeTypeV1(CqStruct, frozen=True):
    """One node-type row from grammar introspection."""
    type: str
    named: bool
    fields: tuple[str, ...] = ()
    is_visible: bool | None = None       # NEW: node_kind_is_visible
    is_supertype: bool | None = None     # NEW: node_kind_is_supertype
```

```python
# In _load_runtime_node_types():
for kind_id in range(node_kind_count):
    node_kind = runtime_language.node_kind_for_id(kind_id)
    if not isinstance(node_kind, str) or not node_kind:
        continue
    is_visible = _safe_call(runtime_language, "node_kind_is_visible", kind_id)
    is_supertype = _safe_call(runtime_language, "node_kind_is_supertype", kind_id)
    rows.append(
        GrammarNodeTypeV1(
            type=node_kind,
            named=bool(runtime_language.node_kind_is_named(kind_id)),
            fields=(),
            is_visible=is_visible,
            is_supertype=is_supertype,
        )
    )


def _safe_call(obj: object, method: str, *args: object) -> bool | None:
    """Call a method with fail-open semantics."""
    fn = getattr(obj, method, None)
    if fn is None or not callable(fn):
        return None
    try:
        return bool(fn(*args))
    except (TypeError, RuntimeError, ValueError, AttributeError):
        return None
```

```python
# tools/cq/search/tree_sitter/structural/export.py — enriched node rows
# In _node_row(), add classification:
node_row = TreeSitterStructuralNodeV1(
    ...,
    is_visible=is_visible,      # from schema index lookup
    is_supertype=is_supertype,  # from schema index lookup
)
```

### Files to Edit

- `tools/cq/search/tree_sitter/schema/node_schema.py` — add `is_visible` and `is_supertype` fields to `GrammarNodeTypeV1`; populate in `_load_runtime_node_types()`
- `tools/cq/search/tree_sitter/structural/export.py` — use visibility/supertype classification in structural node rows

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_node_classification.py` — test visible/supertype classification, test GrammarNodeTypeV1 roundtrip with new fields

### Legacy Decommission/Delete Scope

- None. Additive fields only. The existing `named` boolean remains; new fields supplement it.

---

## S12. Centralized Language Construction + Runtime Capability Snapshot

### Goal

Route all `Language(...)` construction through `core/language_registry.py` so ABI/provenance gating is uniformly enforced, and expose a lightweight capability snapshot for runtime feature gating (`goto_first_child_for_byte`, cursor reset APIs, containing-range query cursor methods).

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/language_registry.py
class TreeSitterRuntimeCapabilitiesV1(CqStruct, frozen=True):
    language: str
    has_cursor_copy: bool = False
    has_cursor_reset: bool = False
    has_cursor_reset_to: bool = False
    has_goto_first_child_for_byte: bool = False
    has_query_cursor_containing_byte_range: bool = False
    has_query_cursor_containing_point_range: bool = False
```

```python
# tools/cq/search/tree_sitter/core/language_registry.py
@lru_cache(maxsize=8)
def load_tree_sitter_capabilities(language: str) -> TreeSitterRuntimeCapabilitiesV1:
    lang_obj = load_tree_sitter_language(language)
    if lang_obj is None:
        return TreeSitterRuntimeCapabilitiesV1(language=language)

    has_query_cursor_containing_byte_range = hasattr(
        _TreeSitterQueryCursor,
        "set_containing_byte_range",
    ) if _TreeSitterQueryCursor is not None else False
    has_query_cursor_containing_point_range = hasattr(
        _TreeSitterQueryCursor,
        "set_containing_point_range",
    ) if _TreeSitterQueryCursor is not None else False
    return TreeSitterRuntimeCapabilitiesV1(
        language=language,
        has_query_cursor_containing_byte_range=has_query_cursor_containing_byte_range,
        has_query_cursor_containing_point_range=has_query_cursor_containing_point_range,
    )
```

```python
# tools/cq/search/tree_sitter/python_lane/runtime.py
from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language

@lru_cache(maxsize=1)
def _python_language() -> Language:
    resolved = load_tree_sitter_language("python")
    if resolved is None:
        msg = "tree_sitter_python language bindings are unavailable"
        raise RuntimeError(msg)
    return cast("Language", resolved)
```

### Files to Edit

- `tools/cq/search/tree_sitter/core/language_registry.py` — add `TreeSitterRuntimeCapabilitiesV1` and `load_tree_sitter_capabilities()`
- `tools/cq/search/tree_sitter/schema/node_schema.py` — remove direct `Language(...)` construction and consume registry loader
- `tools/cq/search/tree_sitter/python_lane/runtime.py` — remove direct `Language(...)` construction and consume registry loader
- `tools/cq/search/tree_sitter/python_lane/facts.py` — remove direct `Language(...)` construction and consume registry loader
- `tools/cq/search/tree_sitter/rust_lane/runtime.py` — remove direct `Language(...)` construction and consume registry loader

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_language_registry_capabilities.py` — test capability snapshot defaults, binding-presence flags, and fail-open behavior.

### Legacy Decommission/Delete Scope

- Delete direct `_TreeSitterLanguage(...)` constructors from `tools/cq/search/tree_sitter/schema/node_schema.py`, `tools/cq/search/tree_sitter/python_lane/runtime.py`, `tools/cq/search/tree_sitter/python_lane/facts.py`, and `tools/cq/search/tree_sitter/rust_lane/runtime.py`.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S6, S11)

- Delete hardcoded `supertypes=()` assignment at `tools/cq/search/tree_sitter/core/language_registry.py` — superseded by runtime supertype loading.
- Keep `tools/cq/search/tree_sitter/rust_lane/runtime.py:_SCOPE_KINDS` as an explicit semantic policy list in this plan (not grammar-derived), because scope-grouping semantics are product-level and intentionally stricter than grammar supertypes.

### Batch D2 (after S7, S8)

- Delete any Rust-lane-only distribution query loading branches introduced during migration (`highlights.scm` / `tags.scm` lookup helpers in lane modules); keep `tools/cq/search/tree_sitter/query/registry.py` as the sole distribution query source loader for both Python and Rust.

### Batch D3 (after S9)

- Delete the flat `parse_injected_ranges()` implementation path in `tools/cq/search/tree_sitter/rust_lane/injection_runtime.py` once combined/separate split tests pass.

### Batch D4 (after S12)

- Delete duplicated direct language-construction code paths listed in S12 once all lanes are on `load_tree_sitter_language()`.

---

## Implementation Sequence

1. **S1 (ABI Version Gating)** — Safety gate first; blocks invalid runtime/grammar combinations.
2. **S12 (Centralized Language Construction + Capability Snapshot)** — Establishes one construction path and capability facts used by downstream scopes.
3. **S2 (Grammar Provenance)** — Provenance fields layer on top of S1/S12 language metadata.
4. **S6 (Supertype Taxonomy)** — Builds id-based supertype/subtype indexes from runtime language APIs.
5. **S11 (Supertype/Visible Classification)** — Extends schema/export rows using S6 taxonomy + visibility APIs.
6. **S10 (Field ID Hot-Path)** — Uses corrected field-id enumeration and cached ids for hot access paths.
7. **S4 (Byte-Seek Navigation)** — Introduces byte-seek traversal with correct `int | None` handling.
8. **S5 (TreeCursor Pooling)** — Adds copy/reset pooling with correct `reset_to` vs `reset` semantics.
9. **S9 (injection.combined Handling)** — Correctness fix for embedded parsing behavior.
10. **S3 (`Node.has_changes` Filter)** — Additive optimization gated on confirmed incremental reuse context.
11. **S7 (Highlights Query Pack Execution)** — Adds unified highlights execution via registry-backed distribution loading.
12. **S8 (Tags Query Pack for Rust)** — Finalizes distribution tags normalization and runtime integration.

---

## Implementation Checklist

- [ ] S1. ABI Version Gating
- [ ] S2. Grammar Provenance Stamping
- [ ] S3. `Node.has_changes` Filter
- [ ] S4. Byte-Seek Navigation
- [ ] S5. TreeCursor Pooling
- [ ] S6. Supertype Taxonomy
- [ ] S7. Highlights Query Pack Execution
- [ ] S8. Tags Query Pack for Rust
- [ ] S9. `injection.combined` Property Handling
- [ ] S10. Field ID Hot-Path Optimization
- [ ] S11. Supertype/Visible Classification
- [ ] S12. Centralized Language Construction + Runtime Capability Snapshot
- [ ] D1. Decommission hardcoded supertype placeholder (after S6 + S11)
- [ ] D2. Decommission lane-specific distribution loaders (after S7 + S8)
- [ ] D3. Decommission injection flat-path runtime logic (after S9)
- [ ] D4. Decommission duplicated direct language constructors (after S12)
