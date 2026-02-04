# cq Smart Search Implementation Plan v1

This plan captures all identified discrepancies between the current Smart Search implementation and the design in `docs/plans/cq_smart_search_design.md`. Each scope item includes representative snippets, target files, deprecations/removals, and an implementation checklist.

---

## Scope 1: Per‑File Symtable Cache (Performance)

**Problem**  
`enrich_with_symtable()` is invoked per match and re-parses symtable each time. This is O(matches × parse) and dominates runtime on dense hits.

**Approach**  
Introduce a per-file symtable cache and/or reuse `SymtableEnricher` from `tools/cq/query/enrichment.py`. Compute symtable once per file and pass it into `classify_match()` or a shared enrichment path.

**Representative snippet**

```python
# tools/cq/search/classifier.py
_symtable_cache: dict[str, symtable.SymbolTable] = {}

def get_symtable(file_path: Path, source: str) -> symtable.SymbolTable | None:
    key = str(file_path)
    if key in _symtable_cache:
        return _symtable_cache[key]
    try:
        table = symtable.symtable(source, str(file_path), "exec")
    except SyntaxError:
        return None
    _symtable_cache[key] = table
    return table
```

```python
# tools/cq/search/smart_search.py
source = get_cached_source(file_path)
table = get_symtable(file_path, source) if source else None
if table is not None:
    symtable_enrichment = enrich_with_symtable_from_table(table, match_text, raw.line)
```

**Target files**
- `tools/cq/search/classifier.py`
- `tools/cq/search/smart_search.py`
- Optional reuse: `tools/cq/query/enrichment.py`

**Deprecate/Delete**
- If the new cache path is adopted, consider deprecating `enrich_with_symtable()` in favor of `enrich_with_symtable_from_table()` or `SymtableEnricher` reuse.

**Implementation checklist**
1. Add `_symtable_cache` and `get_symtable()` with file-level caching.
2. Add `enrich_with_symtable_from_table()` helper for reusing a cached table.
3. Wire `classify_match()` to use the cached table.
4. Update `clear_caches()` to clear symtable cache too.
5. Add a unit test validating cache hits for multiple matches in one file.

---

## Scope 2: Cache Def‑Lines and Avoid Re‑Reads (Performance)

**Problem**  
`find_def_lines()` reads the file on every match and is called in `classify_match()` per match.

**Approach**  
Cache `def_lines` per file alongside cached source. Compute once per file and reuse.

**Representative snippet**

```python
# tools/cq/search/smart_search.py
_def_lines_cache: dict[str, list[tuple[int, int]]] = {}

def get_def_lines_cached(file_path: Path, source: str) -> list[tuple[int, int]]:
    key = str(file_path)
    if key in _def_lines_cache:
        return _def_lines_cache[key]
    lines = source.splitlines()
    result: list[tuple[int, int]] = []
    for i, line in enumerate(lines, 1):
        stripped = line.lstrip()
        if stripped.startswith(("def ", "async def ")):
            indent = len(line) - len(stripped)
            result.append((i, indent))
    _def_lines_cache[key] = result
    return result
```

**Target files**
- `tools/cq/search/smart_search.py`
- `tools/cq/search/adapter.py` (optionally keep as legacy, but stop calling from Smart Search)

**Deprecate/Delete**
- Stop calling `find_def_lines()` inside `classify_match()`.

**Implementation checklist**
1. Add `_def_lines_cache` and cached accessor.
2. Use cached `source` and `def_lines` in context window computation.
3. Clear def-line cache in `clear_caches()`.
4. Add test: multiple matches in same file should reuse cached def-lines.

---

## Scope 3: Interval Index for AST Lookup (Performance)

**Problem**  
AST node lookup is O(n) per match (`_find_node_at_position` walks the tree).

**Approach**  
Build a per-file interval index for matchable spans (e.g., via `SgRecord` spans or AST node ranges) and do O(log n) lookups for match positions.

**Representative snippet**

```python
# tools/cq/search/classifier.py
@dataclass(frozen=True)
class Span:
    start_line: int
    end_line: int
    node: SgNode

_span_index_cache: dict[str, IntervalIndex] = {}

def get_span_index(file_path: Path, sg_root: SgRoot) -> IntervalIndex:
    key = str(file_path)
    if key in _span_index_cache:
        return _span_index_cache[key]
    spans: list[Span] = []
    for node in sg_root.root().children():
        rng = node.range()
        spans.append(Span(rng.start.line + 1, rng.end.line + 1, node))
    index = IntervalIndex.from_spans(spans)
    _span_index_cache[key] = index
    return index
```

**Target files**
- `tools/cq/search/classifier.py`
- Optional reuse from: `tools/cq/query/executor.py` (IntervalIndex)

**Deprecate/Delete**
- Deprecate `_find_node_at_position()` once the index path is stable.

**Implementation checklist**
1. Add span/index cache for SgRoot files.
2. Replace `_find_node_at_position()` with an interval-based lookup.
3. Clear index cache in `clear_caches()`.
4. Add a perf test on a large file with many hits.

---

## Scope 4: Accurate `scanned_files` and `caps_hit` (Functionality)

**Problem**  
`scanned_files` currently equals `matched_files`, and `caps_hit` doesn’t distinguish `max_files`.

**Approach**  
Track `scanned_files` separately if possible (via rg summary or by counting files traversed). If not available, set an explicit `scanned_files="unknown"` field or add `scanned_files_approx` to avoid misreporting. Add `max_files` to `caps_hit` logic.

**Representative snippet**

```python
# tools/cq/search/smart_search.py
caps_hit = "none"
if stats.timed_out:
    caps_hit = "timeout"
elif stats.truncated:
    caps_hit = "max_files" if stats.matched_files >= limits.max_files else "max_total_matches"
```

**Target files**
- `tools/cq/search/smart_search.py`

**Deprecate/Delete**
- Remove misleading `scanned_files` value if no accurate value can be computed.

**Implementation checklist**
1. Update `SearchStats` to add `scanned_files_approx` if needed.
2. Adjust `caps_hit` to surface `max_files`.
3. Update tests and summary schema expectations.

---

## Scope 5: Progressive Disclosure for Evidence (Performance + UX)

**Problem**  
Evidence currently includes all enriched matches (with context), inflating output size.

**Approach**  
Emit evidence as raw match lines or cap evidence to a small number. Keep full details in artifacts.

**Representative snippet**

```python
# tools/cq/search/smart_search.py
evidence = [build_finding(m, root) for m in enriched_matches[:100]]
result = CqResult(..., evidence=evidence)
```

**Target files**
- `tools/cq/search/smart_search.py`
- `tools/cq/core/report.py` (if new evidence rendering behavior is needed)

**Deprecate/Delete**
- N/A

**Implementation checklist**
1. Add evidence limit constant (e.g., `MAX_EVIDENCE = 100`).
2. Use raw match-only evidence or cap evidence list.
3. Ensure full JSON artifact remains complete.

---

## Scope 6: Use ast-grep Rules Instead of Node‑Kind Map (Correctness)

**Problem**  
`NODE_KIND_MAP` is ad‑hoc and may diverge from tree-sitter/ast-grep semantics.

**Approach**  
Scan matched files using existing rules in `tools/cq/astgrep/rules_py.py` and map `SgRecord` spans to matches.

**Representative snippet**

```python
# tools/cq/search/classifier.py
from tools.cq.astgrep.rules_py import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import scan_files

records = scan_files([file_path], get_rules_for_types(None), root)
record_index = IntervalIndex.from_records(records)
match_record = record_index.find_containing(raw.line)
```

**Target files**
- `tools/cq/search/classifier.py`
- `tools/cq/astgrep/sgpy_scanner.py`
- `tools/cq/astgrep/rules_py.py`

**Deprecate/Delete**
- Deprecate `NODE_KIND_MAP` when rules-based classification is stable.

**Implementation checklist**
1. Build a record index per matched file.
2. Prefer record-based classification for category and containing scope.
3. Keep node-kind fallback for edge cases (optional).
4. Add tests mapping `SgRecord` kinds to categories.

---

## Scope 7: Reuse Context Helpers From `calls.py` (Consistency)

**Problem**  
Smart Search duplicates `_compute_context_window` and `_extract_context_snippet`.

**Approach**  
Reuse helpers from `tools/cq/macros/calls.py` to keep consistent behavior and reduce divergence.

**Representative snippet**

```python
# tools/cq/search/smart_search.py
from tools.cq.macros.calls import _compute_context_window, _extract_context_snippet
```

**Target files**
- `tools/cq/search/smart_search.py`
- `tools/cq/macros/calls.py`

**Deprecate/Delete**
- Remove duplicated helper implementations in `smart_search.py`.

**Implementation checklist**
1. Replace local helper implementations with imports.
2. Update tests to ensure identical snippet behavior to `calls`.

---

## Scope 8: Remove Unused rpygrep Context Map (Performance)

**Problem**  
`collect_candidates()` builds `context_map` but never uses it downstream.

**Approach**  
Either drop it or wire it into evidence/context rendering. Prefer removal unless explicitly needed.

**Representative snippet**

```python
# tools/cq/search/smart_search.py
# Remove context_map construction and store only RawMatch core fields.
```

**Target files**
- `tools/cq/search/smart_search.py`

**Deprecate/Delete**
- Delete `context_map` building and `MatchedFile` usage if unused.

**Implementation checklist**
1. Remove context map creation.
2. Verify no other code depends on `context_before/context_after`.
3. Update RawMatch if those fields are no longer used.

---

## Scope 9: Parse‑First `q` Fallback (Functionality)

**Problem**  
`_is_plain_search()` uses regex token checks and can route malformed `q` queries into Smart Search instead of returning a parse error.

**Approach**  
Attempt `parse_query()` first; fallback to Smart Search only on “missing entity” or “no tokens” errors.

**Representative snippet**

```python
# tools/cq/cli_app/commands/query.py
try:
    parsed_query = parse_query(query_string)
except QueryParseError as exc:
    if is_plain_search_error(exc):
        return smart_search(...)
    return error_result(...)
```

**Target files**
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/query/parser.py` (optional: richer errors)

**Deprecate/Delete**
- Remove `_is_plain_search()` if parse-first approach covers all cases.

**Implementation checklist**
1. Add helper to classify parse errors for fallback vs error.
2. Update `q` command to use parse-first flow.
3. Add tests for malformed queries and plain identifiers.

---

## Scope 10: Tests and Performance Validation

**Problem**  
Performance-sensitive changes need coverage and regression checks.

**Approach**  
Add unit tests and small performance fixtures to validate caches and grouping.

**Target files**
- `tests/cq/search/test_smart_search.py`
- `tests/cq/search/test_classifier.py`
- `tests/cli_golden/test_search_golden.py`

**Deprecate/Delete**
- N/A

**Implementation checklist**
1. Add tests for cache hits (symtable, def_lines, AST spans).
2. Add tests for grouping by containing function.
3. Add tests for `--include-strings`.
4. Add golden test for summary fields (`caps_hit`, `pattern`, `include/exclude`).

---

## Quality Gate

Run after implementation:

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q
```

---

## Notes

- Always use `uv run` for Python commands.  
- Keep repo root anchors stable; `--in` should always be a scan filter.  
- Prefer small, scoped diffs per item; land caches before interval index to isolate perf regressions.  

