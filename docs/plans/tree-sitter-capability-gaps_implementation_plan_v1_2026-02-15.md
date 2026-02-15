# Tree-Sitter Capability Gaps Implementation Plan v1 (2026-02-15)

## Scope Summary

This plan addresses 11 tree-sitter API capability gaps identified by cross-referencing py-tree-sitter 0.25+ documentation against actual usage in `tools/cq/search/tree_sitter/`. The scope spans safety (ABI gating), provenance (grammar version stamping), performance (field ID hot-path, byte-seek navigation, cursor pooling), enrichment (highlights and tags query packs), grammar intelligence (supertype taxonomy, node visibility), and runtime correctness (injection.combined handling, Node.has_changes filtering).

**Design stance:** No compatibility shims. All new APIs are py-tree-sitter 0.25+ only with fail-open guards. Each scope item is independently deployable. No existing behavior changes unless the scope item explicitly says so.

---

## Design Principles

1. **Fail-open always.** Any new API call that raises `AttributeError`, `TypeError`, or `RuntimeError` must degrade gracefully — never block parsing, query execution, or enrichment.
2. **Contract-first.** New metadata fields are added to existing `CqStruct` contracts before wiring into runtime code.
3. **No `# noqa` or `# type: ignore`.** Use `cast()` or `TYPE_CHECKING` guards for tree-sitter API types.
4. **Incremental deployment.** Each scope item can land independently; cross-scope deletions are in a separate batch section.
5. **msgspec boundary.** All new serialized structs use `CqStruct` (msgspec.Struct). No pydantic in hot paths.

---

## Current Baseline

- **py-tree-sitter version:** 0.25+ (imports `Parser`, `Language`, `Query`, `QueryCursor`, `Point`, `Range` from `tree_sitter`)
- **Grammar wheels:** `tree_sitter_python`, `tree_sitter_rust` (imported directly)
- **Language loading:** `language_registry.py:59` — `@lru_cache(maxsize=8)` wrapping `_TreeSitterLanguage(_tree_sitter_python.language())`; no ABI version check
- **No `Language.abi_version` usage** anywhere in codebase
- **No `Language.semantic_version` or `Language.name` usage** for provenance
- **`supertypes` field exists** on `TreeSitterLanguageRegistryV1` (line 33) but is always `()` — never populated from `Language.supertypes`
- **`build_runtime_ids()` and `build_runtime_field_ids()`** exist at `node_schema.py:188-208` but are never called from hot-path code
- **28 `child_by_field_name()` calls** across 5 files in `tools/cq/search/tree_sitter/` — all using string lookups, none using cached field IDs
- **`InjectionSettingsV1.combined` field** is parsed at `injection_config.py:49` but `injection_runtime.py` treats all plans uniformly (no grouping for combined parsing)
- **`_advance_cursor()` in `structural/export.py:68`** uses only `goto_first_child()`, `goto_next_sibling()`, `goto_parent()` — no byte-seek or cursor copy
- **No `.scm` files** exist under `tools/cq/search/tree_sitter/` — query packs are loaded from registry/distribution
- **`RustManifestV1`** already has `highlights`, `injections`, `tags` fields parsed from grammar `tree-sitter.json` at `bundle.py:42`
- **`build_tag_events()` exists** at `tags.py:44` but only processes matches from CQ's custom query packs, not from standard `tags.scm`
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

Record `Language.semantic_version` and `Language.name` in grammar metadata contracts for full reproducibility. Stamp these into `GrammarSchemaV1`, `QueryPackPlanV1`, and `TreeSitterLanguageRegistryV1`.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/schema/node_schema.py
class GrammarSchemaV1(CqStruct, frozen=True):
    """Simplified grammar schema for lint-time checks."""
    language: str
    node_types: tuple[GrammarNodeTypeV1, ...] = ()
    grammar_name: str | None = None        # NEW: Language.name
    semantic_version: str | None = None     # NEW: Language.semantic_version
    abi_version: int | None = None          # NEW: Language.abi_version
```

```python
# tools/cq/search/tree_sitter/core/language_registry.py
class TreeSitterLanguageRegistryV1(CqStruct, frozen=True):
    language: str
    node_kinds: tuple[str, ...] = ()
    named_node_kinds: tuple[str, ...] = ()
    field_names: tuple[str, ...] = ()
    supertypes: tuple[str, ...] = ()
    grammar_name: str | None = None         # NEW
    semantic_version: str | None = None     # NEW
    abi_version: int | None = None          # NEW
```

```python
# Extraction helper at schema load time
def _extract_provenance(language_obj: object) -> tuple[str | None, str | None, int | None]:
    """Extract provenance fields from a Language object."""
    name = getattr(language_obj, "name", None)
    sem_ver = getattr(language_obj, "semantic_version", None)
    abi = getattr(language_obj, "abi_version", None)
    return (
        str(name) if isinstance(name, str) else None,
        str(sem_ver) if isinstance(sem_ver, str) else None,
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
    grammar_name: str | None = None         # NEW
    semantic_version: str | None = None     # NEW
```

### Files to Edit

- `tools/cq/search/tree_sitter/schema/node_schema.py` — add provenance fields to `GrammarSchemaV1`, extract in `load_grammar_schema()`
- `tools/cq/search/tree_sitter/core/language_registry.py` — add provenance fields to `TreeSitterLanguageRegistryV1`, populate in `load_language_registry()`
- `tools/cq/search/tree_sitter/contracts/query_models.py` — add provenance fields to `QueryPackPlanV1`
- `tools/cq/search/tree_sitter/query/planner.py` — stamp provenance into `build_pack_plan()` output

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_grammar_provenance.py` — test provenance extraction from runtime Language objects, test stamping flows through to contracts

### Legacy Decommission/Delete Scope

- None. Additive fields only.

---

## S3. `Node.has_changes` Filter

### Goal

Use `node.has_changes` as a secondary filter during node-level processing within changed ranges. When walking a changed range's descendant tree in enrichment passes, nodes without `has_changes` can be skipped for certain expensive enrichment operations.

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
- `tools/cq/search/tree_sitter/core/runtime.py` — add optional `has_changes` filter in `run_bounded_query_captures()` when changed-range context is available

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_has_changes_filter.py` — test node filter with mock nodes having `has_changes=True/False/None`

### Legacy Decommission/Delete Scope

- None. Additive optimization only.

---

## S4. Byte-Seek Navigation

### Goal

Use `TreeCursor.goto_first_child_for_byte()` in traversal patterns that process changed ranges sequentially within wide container nodes. Also use `Node.first_child_for_byte()` / `Node.first_named_child_for_byte()` for direct node access patterns.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/structural/export.py — byte-seek variant
def _advance_cursor_to_byte(cursor: Any, target_byte: int) -> bool:
    """Advance cursor to first child containing target byte offset."""
    goto = getattr(cursor, "goto_first_child_for_byte", None)
    if goto is None:
        return cursor.goto_first_child()  # fail-open: use standard traversal
    try:
        return bool(goto(target_byte))
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

Use `TreeCursor.copy()` and `TreeCursor.reset_to()` for cursor pooling in structural export where fork-and-explore patterns are needed.

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


def _reset_cursor_to(cursor: Any, node: object) -> bool:
    """Reset a pooled cursor to a new root node."""
    reset_fn = getattr(cursor, "reset_to", None)
    if reset_fn is None:
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

- `tests/unit/cq/search/tree_sitter/test_cursor_pooling.py` — test copy/reset_to behaviors, test fallback when methods unavailable

### Legacy Decommission/Delete Scope

- None. Additive optimization.

---

## S6. Supertype Taxonomy

### Goal

Use `Language.supertypes` and `Language.subtypes()` to derive taxonomy from grammar rather than maintaining hardcoded kind lists. Build supertype/subtype indices for Python and Rust grammars. Populate the existing `supertypes` field on `TreeSitterLanguageRegistryV1`.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/language_registry.py
def _load_supertypes(language_obj: object) -> tuple[str, ...]:
    """Load supertype kinds from Language.supertypes."""
    supertypes_attr = getattr(language_obj, "supertypes", None)
    if supertypes_attr is None:
        return ()
    try:
        return tuple(sorted(str(st) for st in supertypes_attr if isinstance(st, str)))
    except (TypeError, RuntimeError):
        return ()


def _load_subtypes(language_obj: object, supertype: str) -> tuple[str, ...]:
    """Load subtype kinds for one supertype via Language.subtypes()."""
    subtypes_fn = getattr(language_obj, "subtypes", None)
    if subtypes_fn is None or not callable(subtypes_fn):
        return ()
    try:
        return tuple(sorted(str(st) for st in subtypes_fn(supertype) if isinstance(st, str)))
    except (TypeError, RuntimeError):
        return ()
```

```python
# Populate supertypes in load_language_registry()
lang_obj = load_tree_sitter_language(language)
supertypes = _load_supertypes(lang_obj) if lang_obj is not None else ()
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
    supertype: str
    subtypes: tuple[str, ...] = ()


def build_supertype_index(language_obj: object) -> tuple[SupertypeIndexV1, ...]:
    """Build full supertype taxonomy from Language.supertypes + subtypes()."""
    supertypes = _load_supertypes(language_obj)
    return tuple(
        SupertypeIndexV1(
            supertype=st,
            subtypes=_load_subtypes(language_obj, st),
        )
        for st in supertypes
    )
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

Load and execute the standard `highlights.scm` query packs shipped with Python and Rust grammars to produce a complete semantic token overlay. This provides a byte-sorted token stream with semantic classes (@keyword, @function, @type, @property, @string, etc.) that complements CQ's existing captures (defs/calls/imports).

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
    semantic_version: str | None = None


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
# Loading highlights.scm from distribution
# tools/cq/search/tree_sitter/rust_lane/bundle.py already has:
#   RustManifestV1.highlights field with the .scm filename
# Use _distribution_file_path() to resolve the actual file content
```

### Files to Edit

- `tools/cq/search/tree_sitter/rust_lane/bundle.py` — add `load_highlights_source()` helper that reads `highlights.scm` from distribution
- `tools/cq/search/tree_sitter/core/language_registry.py` — add `load_highlights_source(language)` dispatch for Python/Rust

### New Files to Create

- `tools/cq/search/tree_sitter/core/highlights_runner.py` — `HighlightTokenV1`, `HighlightsResultV1` contracts; `run_highlights_pack()` entry point
- `tests/unit/cq/search/tree_sitter/test_highlights_runner.py` — test token extraction, byte-ordering, windowed execution, graceful degradation

### Legacy Decommission/Delete Scope

- None. New capability.

---

## S8. Tags Query Pack for Rust

### Goal

Load and execute the standard `tags.scm` query pack for the Rust grammar to provide a grammar-author-maintained definition/reference event stream. Normalize into the existing `RustTagEventV1` contract. Enable cross-reference with CQ's custom captures for drift detection.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/tags.py — extend build_tag_events
# The existing build_tag_events() already normalizes matches into
# RustTagEventV1 rows. The change is to also feed matches from
# the standard tags.scm (loaded from distribution) through the
# same normalization.

def run_distribution_tags_pack(
    *,
    language: str,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1 | None = None,
) -> tuple[RustTagEventV1, ...]:
    """Execute distribution tags.scm and normalize into tag events."""
    from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_matches
    from tools.cq.search.tree_sitter.query.compiler import compile_query
    from tools.cq.search.tree_sitter.rust_lane.bundle import load_tags_source

    tags_source = load_tags_source(language)
    if tags_source is None:
        return ()
    query = compile_query(
        language=language,
        pack_name="tags.scm",
        source=tags_source,
        request_surface="distribution",
    )
    matches, _telemetry = run_bounded_query_matches(
        query, root, windows=windows, settings=settings,
    )
    return build_tag_events(matches=matches, source_bytes=source_bytes)
```

### Files to Edit

- `tools/cq/search/tree_sitter/rust_lane/bundle.py` — add `load_tags_source()` helper
- `tools/cq/search/tree_sitter/tags.py` — add `run_distribution_tags_pack()` entry point
- `tools/cq/search/tree_sitter/rust_lane/runtime.py` — integrate distribution tags into `_collect_query_pack_captures()` output

### New Files to Create

- `tests/unit/cq/search/tree_sitter/test_distribution_tags.py` — test tags.scm loading, match normalization, cross-reference with custom captures

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

Wire the already-computed field IDs from `build_runtime_field_ids()` and `build_runtime_ids()` in `node_schema.py` into the hot-path node access code. Replace `child_by_field_name("name")` with `child_by_field_id(cached_name_id)` in frequently-called functions. Infrastructure already exists — this is pure plumbing.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/infrastructure.py — cached field IDs
from functools import lru_cache
from tools.cq.search.tree_sitter.schema.node_schema import (
    build_runtime_field_ids,
    build_runtime_ids,
)

@lru_cache(maxsize=4)
def _cached_field_ids(language: str) -> dict[str, int]:
    """Load and cache field IDs for a language."""
    lang_obj = load_language(language)
    return build_runtime_field_ids(lang_obj)


@lru_cache(maxsize=4)
def _cached_node_ids(language: str) -> dict[str, int]:
    """Load and cache node-kind IDs for a language."""
    lang_obj = load_language(language)
    return build_runtime_ids(lang_obj)
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

- `tools/cq/search/tree_sitter/core/infrastructure.py` — add `_cached_field_ids()`, `_cached_node_ids()`, and `child_by_field()` helper
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

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S6, S11)

- Delete hardcoded `supertypes=()` assignment at `language_registry.py:54` — superseded by dynamic `_load_supertypes()`.
- If `_SCOPE_KINDS` tuples in `rust_lane/runtime.py:82-90` can be derived from supertype taxonomy, replace hardcoded tuple with grammar-derived set. (Requires validation that Rust grammar supertype `_declaration_statement` covers the same kinds.)

### Batch D2 (after S7, S8)

- If distribution highlights.scm and tags.scm cover all captures currently provided by CQ's custom `.scm` packs for the same semantic roles, document overlap percentage and recommend deduplication of overlapping custom captures in a follow-up plan. (No deletion in this plan — requires validation against actual grammar .scm content.)

### Batch D3 (after S9)

- Delete the flat `parse_injected_ranges()` implementation from `injection_runtime.py` once the combined/separate split is validated — the new implementation fully replaces it.

---

## Implementation Sequence

1. **S1 (ABI Version Gating)** — Highest priority, trivial effort, prevents silent corruption. No dependencies.
2. **S2 (Grammar Provenance)** — Builds on S1 infrastructure (same `language_registry.py`). Additive contract fields.
3. **S10 (Field ID Hot-Path)** — Low effort, existing infrastructure. Immediate perf benefit. No dependencies.
4. **S6 (Supertype Taxonomy)** — Populates existing empty field. Required foundation for S11.
5. **S11 (Supertype/Visible Classification)** — Depends on S6 for supertype data. Additive schema fields.
6. **S7 (Highlights Query Pack)** — Medium effort, new capability. Benefits from S2 provenance for stamping.
7. **S8 (Tags Query Pack for Rust)** — Parallel with S7. Builds on existing `tags.py` contract.
8. **S4 (Byte-Seek Navigation)** — Independent perf optimization. Can land anytime.
9. **S3 (Node.has_changes Filter)** — Independent optimization. Requires changed-range context plumbing.
10. **S9 (injection.combined Handling)** — Independent correctness fix. Refactors injection_runtime.py.
11. **S5 (TreeCursor Pooling)** — Lowest priority, marginal benefit. Can land anytime.

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
- [ ] D1. Cross-scope decommission (after S6 + S11)
- [ ] D2. Highlights/Tags overlap assessment (after S7 + S8)
- [ ] D3. Injection runtime flat-path removal (after S9)
