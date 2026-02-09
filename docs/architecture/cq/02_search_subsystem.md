# CQ Search Subsystem Architecture

## Executive Summary

The CQ search subsystem (`tools/cq/search/`) provides semantically-enriched code search with multi-stage classification, parallel execution, and cross-source agreement validation. It transforms raw text matches from ripgrep into high-confidence, context-rich findings suitable for agent consumption.

**Key characteristics:**

- Three-tier classification pipeline (heuristic → AST node → record-based)
- Five-stage Python enrichment pipeline (ast-grep → Python AST → import → LibCST → tree-sitter)
- Parallel ProcessPool classification with spawn context and fail-open semantics
- Per-file caching with thread-unsafe cache architecture
- Cross-source agreement validation across ast-grep, LibCST, and tree-sitter
- Multi-language orchestration with per-language partition statistics
- Pyrefly LSP integration for semantic hover data and diagnostics

**Target audience:** Advanced LLM programmers proposing architectural improvements.

---

## 1. Module Map

### Core Modules

| Module | Size (LOC) | Responsibility |
|--------|------------|----------------|
| `smart_search.py` | ~2514 | Main entry point, orchestration, parallel pools, result assembly |
| `classifier.py` | ~1025 | Three-tier classification pipeline (heuristic → AST → record) |
| `python_enrichment.py` | ~2174 | Five-stage Python enrichment pipeline with agreement validation |
| `rust_enrichment.py` | ~468 | Two-stage Rust enrichment (tree-sitter → syntax analysis) |
| `adapter.py` | ~289 | Ripgrep adapter functions and pattern construction |
| `collector.py` | ~193 | RgCollector for streaming JSON event parsing |
| `context_window.py` | ~112 | Context snippet extraction with window computation |

### Support Modules

| Module | Size (LOC) | Responsibility |
|--------|------------|----------------|
| `models.py` | ~61 | SearchConfig, SearchRequest, CandidateSearchRequest |
| `requests.py` | ~83 | Request structs: RgRunRequest, enrichment requests |
| `profiles.py` | ~66 | SearchLimits presets (DEFAULT, INTERACTIVE, AUDIT, LITERAL) |
| `contracts.py` | ~488 | Multi-language summary contracts and telemetry |
| `timeout.py` | ~108 | Async/sync timeout wrappers with fallback semantics |
| `pyrefly_lsp.py` | ~341 | Pyrefly LSP integration for hover/diagnostic enrichment |

### Enrichment Sub-Modules

Located in `tools/cq/search/enrichment/`:

- `core.py` - Shared enrichment utilities (payload normalization, budget enforcement)
- `libcst_python.py` - LibCST resolution enrichment
- `tree_sitter_python.py` - Tree-sitter gap-fill enrichment
- `tree_sitter_rust.py` - Rust tree-sitter enrichment

---

## 2. Entry Point: smart_search()

**Location:** `smart_search.py:2470-2514`

**Signature:**
```python
def smart_search(
    root: Path,
    query: str,
    **kwargs: object,
) -> CqResult:
    """Execute Smart Search pipeline.

    Parameters
    ----------
    root
        Repository root path.
    query
        Search query string.
    kwargs
        Optional overrides: mode, include_globs, exclude_globs, include_strings,
        limits, tc, argv.

    Returns
    -------
    CqResult
        Complete search results.
    """
```

### Pipeline Flow

```python
# 1. Coerce kwargs into typed request
request = _coerce_search_request(root=root, query=query, kwargs=kwargs)

# 2. Build search context from request
ctx = _build_search_context(request)

# 3. Execute per-language partitions
partition_results = _run_language_partitions(ctx)

# 4. Fallback to literal mode if identifier mode yields no results
mode_chain = [ctx.mode]
if _should_fallback_to_literal(request, ctx.mode, partition_results):
    fallback_ctx = msgspec.structs.replace(ctx, mode=QueryMode.LITERAL, fallback_applied=True)
    fallback_partitions = _run_language_partitions(fallback_ctx)
    mode_chain.append(QueryMode.LITERAL)
    if _partition_total_matches(fallback_partitions) > 0:
        partition_results = fallback_partitions

# 5. Assemble final result with merged partitions + telemetry
return _assemble_smart_search_result(ctx, partition_results)
```

**Key functions:**

- `_coerce_search_request()` (line 1650-1668) - Converts kwargs → SearchRequest
- `_build_search_context()` (line 1621-1647) - Converts SearchRequest → SearchConfig
- `_run_language_partitions()` (line 1803-1808) - Executes per-language search
- `_assemble_smart_search_result()` (line 2371-2467) - Merges partitions into CqResult

### Search Context Construction

**Function:** `_build_search_context()` (line 1621-1647)

Resolves:
- Query mode (auto-detection via `detect_query_mode()`)
- Search limits (defaults to INTERACTIVE profile)
- Language scope (default: "auto" → Python + Rust)
- Include/exclude globs (constrained by language scope)
- Toolchain reference
- Start timestamp

Returns `SearchConfig` struct with resolved configuration.

---

## 3. Candidate Phase: collect_candidates()

**Location:** `smart_search.py:459-510`

**Purpose:** Execute native `rg` (ripgrep) search and collect raw matches.

**Signature:**
```python
def collect_candidates(
    request: CandidateCollectionRequest,
) -> tuple[list[RawMatch], SearchStats]:
    """Execute native ``rg`` search and collect raw matches.

    Parameters
    ----------
    request
        CandidateCollectionRequest with root, query, mode, limits, lang, globs.

    Returns
    -------
    tuple[list[RawMatch], SearchStats]
        Raw matches and collection statistics.
    """
```

### Implementation Details

```python
# 1. Build ripgrep command and execute
proc = run_rg_json(
    RgRunRequest(
        root=request.root,
        pattern=request.pattern,
        mode=request.mode,
        lang_types=(ripgrep_type_for_language(request.lang),),
        include_globs=request.include_globs or [],
        exclude_globs=request.exclude_globs or [],
        limits=request.limits,
    )
)

# 2. Parse streaming JSON events into RawMatch objects
collector = RgCollector(limits=request.limits, match_factory=RawMatch)
for event in proc.events:
    collector.handle_event(event)
collector.finalize()

# 3. Filter matches by language scope (extension-authoritative)
scope_filtered = [
    match for match in collector.matches
    if is_path_in_lang_scope(match.file, request.lang)
]
dropped_by_scope = len(collector.matches) - len(scope_filtered)

# 4. Build search statistics
stats = _build_search_stats(collector, timed_out=proc.timed_out)
stats = msgspec.structs.replace(
    stats,
    matched_files=len({match.file for match in scope_filtered}),
    total_matches=len(scope_filtered),
    dropped_by_scope=dropped_by_scope,
)

return scope_filtered, stats
```

### RgCollector Event Handling

**Location:** `collector.py:44-193`

The `RgCollector` parses ripgrep JSON output events:

- `begin` - Start of file scan
- `match` - Match event with line/column/text
- `end` - End of file scan with stats
- `summary` - Final summary stats

**Key features:**
- Streaming event parser (no memory accumulation)
- Respects `SearchLimits` caps (max_files, max_matches_per_file, max_total_matches)
- Tracks scanned/matched file counts
- Timeout detection via process exit code

### Search Statistics

**Function:** `_build_search_stats()` (line 429-456)

Returns `SearchStats` with:
- `scanned_files` - Total files scanned by ripgrep
- `matched_files` - Files with at least one match
- `total_matches` - Total match count
- `truncated` - Whether result set was truncated
- `timed_out` - Whether search timed out
- `caps_hit` - Which limit was hit ("none", "files", "matches_per_file", "total_matches")
- `dropped_by_scope` - Matches excluded by language scope filtering

---

## 4. Three-Tier Classification Pipeline

**Location:** `classifier.py`

The classification pipeline progressively refines match categorization with three tiers of increasing cost and accuracy.

### Overview

| Tier | Complexity | Strategy | Confidence | Skip Deeper |
|------|------------|----------|------------|-------------|
| 1. Heuristic | O(1) | Line pattern matching | 0.60-0.95 | True for comments/imports |
| 2. AST Node | O(log n) | ast-grep node lookup | 0.60-0.95 | - |
| 3. Record-Based | O(log n) | ast-grep record filtering | 0.85-0.95 | - |

### Tier 1: Heuristic Classification

**Function:** `classify_heuristic()` (line 369-447)

**Purpose:** Fast O(1) line-based pattern matching for common cases.

**Algorithm:**
```python
def classify_heuristic(line: str, col: int, match_text: str) -> HeuristicResult:
    stripped = line.lstrip()

    # 1. Check comment (match position is after #)
    hash_pos = line.find("#")
    if hash_pos >= 0 and col > hash_pos:
        return HeuristicResult(
            category="comment_match",
            confidence=0.95,
            skip_deeper=True,  # No AST node for comments
        )

    # 2. Check definition patterns (high confidence)
    if stripped.startswith(("def ", "async def ")):
        return HeuristicResult(
            category="definition",
            confidence=0.90,
            skip_deeper=False,  # AST can confirm
        )

    elif stripped.startswith("class "):
        return HeuristicResult(
            category="definition",
            confidence=0.90,
            skip_deeper=False,
        )

    # 3. Check import patterns (very high confidence)
    elif stripped.startswith("import "):
        return HeuristicResult(
            category="import",
            confidence=0.95,
            skip_deeper=True,
        )

    elif stripped.startswith("from ") and " import " in stripped:
        return HeuristicResult(
            category="from_import",
            confidence=0.95,
            skip_deeper=True,
        )

    # 4. Check for call pattern (name followed by parenthesis)
    else:
        rest = line[col + len(match_text):]
        if rest.lstrip().startswith("("):
            return HeuristicResult(
                category="callsite",
                confidence=0.70,  # Needs AST confirmation
                skip_deeper=False,
            )

        # 5. Check for docstring context (triple quotes)
        elif '"""' in line or "'''" in line:
            return HeuristicResult(
                category="docstring_match",
                confidence=0.60,  # Uncertain without AST
                skip_deeper=False,
            )

    return HeuristicResult(category=None, confidence=0.0, skip_deeper=False)
```

**Detected categories:**
- `definition` - Function/class definitions (0.90 confidence)
- `import` / `from_import` - Import statements (0.95 confidence)
- `callsite` - Function calls (0.70 confidence, needs AST)
- `comment_match` - In comment (0.95 confidence, skip AST)
- `docstring_match` - In docstring (0.60 confidence)

**Skip-deeper flag:** Set to `True` for comments and imports where AST cannot provide additional information.

### Tier 2: AST Node Classification

**Function:** `classify_from_node()` (line 661-689)

**Purpose:** Use cached ast-grep node lookup for higher accuracy.

**Signature:**
```python
def classify_from_node(
    sg_root: SgRoot,
    line: int,
    col: int,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> NodeClassification | None:
    """Classify using ast-grep node lookup.

    Parameters
    ----------
    sg_root
        Parsed AST root (cached).
    line
        1-indexed line number.
    col
        0-indexed column offset.
    lang
        Query language used for parser/node-kind semantics.

    Returns
    -------
    NodeClassification | None
        Classification result, or None if no classifiable node found.
    """
```

**Implementation:**
```python
# 1. Find node at position
node = _find_node_at_position(sg_root, line, col, lang=lang)
if node is None:
    return None

# 2. Classify from resolved node
return classify_from_resolved_node(node)
```

**Node kind mapping:** `NODE_KIND_MAP` (line 177-213) maps tree-sitter node kinds to (category, confidence):

```python
NODE_KIND_MAP: dict[str, tuple[MatchCategory, float]] = {
    # High-confidence definitions
    "function_definition": ("definition", 0.95),
    "class_definition": ("definition", 0.95),

    # High-confidence callsites
    "call": ("callsite", 0.95),
    "call_expression": ("callsite", 0.95),

    # Import statements
    "import_statement": ("import", 0.95),
    "import_from_statement": ("from_import", 0.95),

    # Assignments
    "assignment": ("assignment", 0.85),
    "assignment_statement": ("assignment", 0.85),

    # References
    "identifier": ("reference", 0.60),
    "attribute": ("reference", 0.70),

    # Annotations
    "type": ("annotation", 0.80),
    "type_annotation": ("annotation", 0.80),
}
```

**Parent chain walk:** If node kind is not directly mappable, walks up to 5 parent levels with 0.9x confidence degradation per level.

**Containing scope extraction:** Walks parent chain to find enclosing function/class definition and extracts scope name.

### Tier 3: Record-Based Classification

**Function:** `classify_from_records()` (line 732-793)

**Purpose:** Use pre-scanned ast-grep records for fallback classification.

**Strategy:**
1. Load cached `RecordContext` for file (def/call/import/assign records)
2. Build interval indexes for fast line/column filtering
3. Query indexes for candidates at match position
4. Filter by node span containment
5. Return best match with containing scope

**Record types:**
- `def` - Function/class definitions
- `call` - Function/method calls
- `import` - Import statements
- `assign_ctor` - Assignment constructors

**Containing scope:** Extracted from def record's scope chain.

### Symtable Enrichment (Optional)

**Function:** `enrich_with_symtable_from_table()` (line 828-888)

**Purpose:** Enrich high-confidence Python matches with Python symtable binding information.

**Conditional:** Only runs for Python files with confidence >= 0.70.

**Algorithm:**
1. Load cached Python symtable
2. Find innermost scope containing match line
3. Lookup symbol binding in scope
4. Extract binding flags: imported, assigned, referenced, parameter, global, local, free, nonlocal

**Returns:** `SymtableEnrichment` struct with binding flags.

**Cost:** O(parse) for first file access, O(log n) for cached accesses.

### Classification Result

**Struct:** `EnrichedMatch`

```python
class EnrichedMatch:
    file: str
    line: int
    col: int
    text: str
    category: MatchCategory
    confidence: float
    node_kind: str | None
    containing_scope: str | None
    context: str  # Context snippet with window
    symtable: SymtableEnrichment | None
```

---

## 5. Five-Stage Python Enrichment Pipeline

**Location:** `python_enrichment.py`

The Python enrichment pipeline runs sequentially per-match, accumulating state across five stages. Each stage has independent error handling (fail-open).

### Pipeline State

**Struct:** `_PythonEnrichmentState` (line 1860-1871)

```python
@dataclass(slots=True)
class _PythonEnrichmentState:
    payload: dict[str, object]                     # Accumulated fields
    stage_status: dict[str, str]                   # Stage execution status
    stage_timings_ms: dict[str, float]             # Stage timing metrics
    degrade_reasons: list[str]                     # Degradation reasons
    ast_fields: dict[str, object]                  # Stage 1 fields (for agreement)
    libcst_fields: dict[str, object]               # Stage 4 fields (for agreement)
    tree_sitter_fields: dict[str, object]          # Stage 5 fields (for agreement)
```

### Stage 1: ast-grep (lines 1898-1911)

**Function:** `_run_ast_grep_stage()`

**Purpose:** Extract structured context from ast-grep node (zero incremental cost via cached SgRoot).

**Extracted fields:**
- `signature` - Function/class signature
- `decorators` - Decorator list
- `item_role` - "source" / "test" / "doc"
- `class_context` - Containing class name
- `call_target` - Call target name
- `scope_chain` - Nested scope chain
- `structural_context` - Parent node kinds

**Cost:** O(1) - reuses cached SgRoot from classification.

**Degradation:** Append reasons to `degrade_reasons` on extraction failures.

### Stage 2: Python AST (lines 1914-1932)

**Function:** `_run_python_ast_stage()`

**Purpose:** Extract behavior summary using Python `ast` module.

**Conditional:** Only runs for function nodes (`_is_function_node(node)`).

**Extracted fields:**
- `is_generator` - Generator detection (yield/yield from)
- `is_async` - Async function flag
- `awaits` - Async await count
- `yields` - Yield count
- `returns_value` - Has non-None return

**Cost:** O(parse) for first file access, O(1) for cached AST trees.

**Caching:** Uses `_py_tree_cache` (LRU, max 64 entries).

### Stage 3: Import Detail (lines 1935-1955)

**Function:** `_run_import_stage()`

**Purpose:** Extract import normalization details.

**Conditional:** Only runs for import/import_from nodes.

**Extracted fields:**
- `import_module` - Module name
- `import_alias` - Alias name
- `import_names` - Imported names list
- `import_level` - Relative import level
- `is_type_import` - TYPE_CHECKING context flag

**Cost:** O(1) - node text parsing only.

### Stage 4: LibCST (lines 1966-2002)

**Function:** `_run_libcst_stage()`

**Purpose:** Extract resolution information using LibCST metadata.

**Conditional:** Only runs when byte range is available (byte_start and byte_end).

**Extracted fields:**
- `qualified_names` - Fully-qualified name candidates
- `binding_candidates` - Binding scope candidates
- `import_alias_chain` - Import alias resolution chain
- `call_target` - Call target resolution
- `call_receiver` - Call receiver resolution
- `call_method` - Call method name

**Strategy:** Gap-fill merge - only adds fields not already populated by ast-grep.

**Cost:** O(parse) for first file access, O(1) for cached LibCST wrappers.

**Session integration:** Reuses `PythonAnalysisSession.libcst_wrapper` if available.

### Stage 5: Tree-Sitter (lines 2021-2066)

**Function:** `_run_tree_sitter_stage()`

**Purpose:** Final gap-fill fallback using tree-sitter.

**Conditional:** Only runs when byte range is available.

**Extracted fields:**
- All gaps NOT filled by prior stages
- `parse_quality` - Tree-sitter parse quality metrics

**Strategy:** `_merge_gap_fill_fields()` - only adds missing fields.

**Cost:** O(parse) for first file access, O(1) for cached tree-sitter trees.

**Degradation:** Appends `degrade_reason` from tree-sitter payload.

### Pipeline Finalization (lines 2069-2100)

**Function:** `_finalize_python_enrichment_payload()`

**Operations:**

1. **Build agreement section** - Compare ast-grep, LibCST, tree-sitter results
2. **Crosscheck validation** - If `CQ_PY_ENRICHMENT_CROSSCHECK=1` and conflicts detected, mark as degraded
3. **Status marking** - Set `enrichment_status` to "degraded" if `degrade_reasons` non-empty
4. **Stage metadata** - Add `stage_status` and `stage_timings_ms`
5. **Truncation tracking** - Add `truncated_fields` if any fields were truncated
6. **Payload budget enforcement** - Enforce 4096-byte limit via `_enforce_payload_budget()`
7. **Size hint** - Add `payload_size_hint` for monitoring

---

## 6. Cross-Source Agreement

**Purpose:** Validate consistency across ast-grep, LibCST, and tree-sitter extractions.

**Function:** `_build_agreement_section()` (line 1817-1863)

**Algorithm:**

```python
def _build_agreement_section(
    ast_fields: dict[str, object],
    libcst_fields: dict[str, object],
    tree_sitter_fields: dict[str, object],
) -> dict[str, object]:
    # 1. Track which sources provided each field
    present_sources: list[str] = []
    if ast_fields:
        present_sources.append("ast_grep")
    if libcst_fields:
        present_sources.append("libcst")
    if tree_sitter_fields:
        present_sources.append("tree_sitter")

    # 2. Compare overlapping fields
    conflicts: list[str] = []
    for field in ast_fields:
        if field in libcst_fields and ast_fields[field] != libcst_fields[field]:
            conflicts.append(f"ast_grep vs libcst: {field}")
        if field in tree_sitter_fields and ast_fields[field] != tree_sitter_fields[field]:
            conflicts.append(f"ast_grep vs tree_sitter: {field}")

    for field in libcst_fields:
        if field in tree_sitter_fields and libcst_fields[field] != tree_sitter_fields[field]:
            conflicts.append(f"libcst vs tree_sitter: {field}")

    # 3. Determine agreement status
    if len(present_sources) >= _FULL_AGREEMENT_SOURCE_COUNT and not conflicts:
        status = "full"
    elif conflicts:
        status = "conflict"
    else:
        status = "partial"

    return {
        "status": status,
        "sources": present_sources,
        "conflicts": conflicts,
    }
```

**Agreement status values:**
- `"full"` - 3+ sources agree, no conflicts
- `"partial"` - <3 sources or some fields missing
- `"conflict"` - Disagreement between sources

**Crosscheck enforcement:** When `CQ_PY_ENRICHMENT_CROSSCHECK=1` environment variable is set, conflicts trigger degradation and are added to `crosscheck_mismatches` field.

---

## 7. Parallel Worker Pool Architecture

### ProcessPool Classification

**Function:** `_run_classification_phase()` (line 1727-1782)

**Purpose:** Parallelize classification across multiple files using ProcessPoolExecutor.

**Strategy:**

```python
def _run_classification_phase(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
) -> list[EnrichedMatch]:
    # 1. Filter by language scope
    filtered_raw_matches = [m for m in raw_matches if is_path_in_lang_scope(m.file, lang)]
    if not filtered_raw_matches:
        return []

    # 2. Partition by file
    indexed: list[tuple[int, RawMatch]] = list(enumerate(filtered_raw_matches))
    partitioned: dict[str, list[tuple[int, RawMatch]]] = {}
    for idx, raw in indexed:
        partitioned.setdefault(raw.file, []).append((idx, raw))
    batches = list(partitioned.values())

    # 3. Determine worker count
    workers = min(len(batches), MAX_SEARCH_CLASSIFY_WORKERS)  # MAX = 4

    # 4. Fallback to sequential if <2 workers or <2 batches
    if workers <= 1 or len(batches) <= 1:
        return [classify_match(raw, ctx.root, lang=lang) for raw in filtered_raw_matches]

    # 5. Build classification tasks
    tasks = [
        ClassificationBatchTask(root=str(ctx.root), lang=lang, batch=batch)
        for batch in batches
    ]

    # 6. Execute in ProcessPool with spawn context
    try:
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),  # spawn, not fork
        ) as pool:
            indexed_results: list[tuple[int, EnrichedMatch]] = []
            for batch_results in pool.map(_classify_partition_batch, tasks):
                indexed_results.extend((item.index, item.match) for item in batch_results)
    except Exception:  # Fail-open to sequential classification
        return [classify_match(raw, ctx.root, lang=lang) for raw in filtered_raw_matches]

    # 7. Sort by original index to preserve order
    indexed_results.sort(key=lambda pair: pair[0])
    return [match for _idx, match in indexed_results]
```

**Worker count:** `min(partition_count, MAX_SEARCH_CLASSIFY_WORKERS)` where `MAX_SEARCH_CLASSIFY_WORKERS = 4`.

**Spawn context:** Uses `multiprocessing.get_context("spawn")` (not `fork`) for clean process state and no GIL inheritance.

**Fail-open semantics:** Catches all exceptions and falls back to sequential classification to prevent pipeline failures.

**Task envelope:** `ClassificationBatchTask` (line 320-325)

```python
class ClassificationBatchTask(CqStruct, frozen=True):
    root: str
    lang: QueryLanguage
    batch: list[tuple[int, RawMatch]]
```

**Result envelope:** `ClassificationBatchResult` (line 328-332)

```python
class ClassificationBatchResult(CqStruct, frozen=True):
    index: int
    match: EnrichedMatch
```

**Index preservation:** Original match indices are preserved through the pool to maintain result ordering.

### ThreadPool Pyrefly Prefetch

**Function:** `_prefetch_pyrefly_in_background()` (line 1818-1839)

**Purpose:** Prefetch Pyrefly LSP data in background thread while classification runs.

**Strategy:**

```python
def _prefetch_pyrefly_in_background(
    ctx: SmartSearchContext,
    *,
    lang: QueryLanguage,
    raw_matches: list[RawMatch],
) -> Future[dict[str, object]] | None:
    # Only for Python matches
    if lang != "python":
        return None

    # Single-worker ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            _prefetch_pyrefly_for_raw_matches,
            ctx,
            raw_matches=raw_matches,
        )

    return future
```

**Concurrency model:** Runs in parallel with classification phase. Main thread continues classification while background thread prefetches LSP data.

**Polling:** Result is polled after classification completes via `future.result(timeout=0.1)`.

**Python-only:** Only executed for Python language partitions.

---

## 8. Caching Architecture

### Cache Invariants

All caches are:
- **Thread-unsafe** - No locking primitives
- **Per-file keyed** - Key format varies by cache type
- **Cleared before search** - `clear_caches()` called at start of `smart_search()`
- **Session-scoped** - Lifetime tied to single search execution

### Cache Registry

**Module-level caches in `classifier.py`:**

```python
_sg_cache: dict[str, SgRoot] = {}                  # ast-grep parsed roots
_source_cache: dict[str, str] = {}                 # File source text
_def_lines_cache: dict[str, set[int]] = {}         # Definition line ranges
_symtable_cache: dict[str, symtable.SymbolTable] = {}  # Python symtables
_record_context_cache: dict[str, RecordContext] = {}   # ast-grep records + indexes
_node_index_cache: dict[str, IntervalIndex] = {}       # Node span interval indexes
```

### Cache Clearing

**Function:** `clear_caches()` (classifier.py)

Called at the start of every `smart_search()` invocation to prevent cross-search contamination.

```python
def clear_caches() -> None:
    _sg_cache.clear()
    _source_cache.clear()
    _def_lines_cache.clear()
    _symtable_cache.clear()
    _record_context_cache.clear()
    _node_index_cache.clear()
```

### Cache Access Patterns

**ast-grep SgRoot cache:**
```python
def get_sg_root(file: Path, *, lang: QueryLanguage) -> SgRoot | None:
    key = str(file)
    if key in _sg_cache:
        return _sg_cache[key]

    # Parse and cache
    sg_root = SgRoot(file.read_text(), language_for_lang(lang))
    _sg_cache[key] = sg_root
    return sg_root
```

**Source text cache:**
```python
def get_cached_source(file: Path) -> str:
    key = str(file)
    if key not in _source_cache:
        _source_cache[key] = file.read_text()
    return _source_cache[key]
```

**Record context cache:**
```python
def _get_record_context(file: Path, *, lang: QueryLanguage) -> RecordContext:
    key = str(file)
    if key not in _record_context_cache:
        # Scan file for def/call/import/assign records
        records = scan_files([file], get_rules_for_types(["def", "call", "import", "assign_ctor"]), lang=lang)
        _record_context_cache[key] = RecordContext(
            records=records,
            def_index=_build_def_index(records),
            call_index=_build_call_index(records),
            import_index=_build_import_index(records),
        )
    return _record_context_cache[key]
```

### Python Enrichment Caches

**Module-level caches in `python_enrichment.py`:**

```python
_py_tree_cache: dict[str, ast.Module] = {}         # Python AST trees
_MAX_TREE_CACHE_ENTRIES = 64                       # LRU eviction threshold
```

**Cache key format:** `blake2b(source_bytes).hexdigest()` for content-addressable caching.

**LRU eviction:** When cache exceeds `_MAX_TREE_CACHE_ENTRIES`, oldest entries are evicted.

---

## 9. Relevance Scoring

**Function:** `compute_relevance_score()` (line 992-1025)

**Purpose:** Rank matches by relevance for display prioritization.

**Algorithm:**

```python
def compute_relevance_score(match: EnrichedMatch, *, depth: int = 0) -> float:
    # 1. Base weight by category
    base = KIND_WEIGHTS[match.category]

    # 2. Role multiplier
    role_mult = {
        "src": 1.0,
        "lib": 0.9,
        "test": 0.5,
        "doc": 0.3,
    }.get(match.item_role, 0.8)

    # 3. Confidence factor
    conf_factor = match.confidence

    # 4. Depth penalty (for nested scopes)
    depth_penalty = depth * 0.05

    return (base * role_mult * conf_factor) - depth_penalty
```

**Category base weights (`KIND_WEIGHTS`):**

```python
KIND_WEIGHTS: dict[MatchCategory, float] = {
    "definition": 1.0,           # Highest priority
    "callsite": 0.8,
    "import": 0.7,
    "from_import": 0.7,
    "assignment": 0.6,
    "reference": 0.5,
    "annotation": 0.4,
    "docstring_match": 0.2,
    "comment_match": 0.1,
    "string_match": 0.1,
    "text_match": 0.3,           # Fallback
}
```

**Item role detection:** Extracted from file path (src/ → "src", tests/ → "test", docs/ → "doc", site-packages/ → "lib").

---

## 10. Scope Grouping

**Purpose:** Group matches by containing function/class for hierarchical display.

### Mechanism 1: Classification Output

**Function:** `_find_containing_scope()` (classifier.py)

Walks parent chain from match node to find enclosing function/class definition.

**Returns:** Scope name string (e.g., "MyClass.my_method").

### Mechanism 2: Record-Based Containing Scope

**Strategy:** Query def_index interval index to find innermost containing definition.

**Returns:** Scope name from def record.

### Mechanism 3: Result Grouping

**Function:** `_group_matches_by_context()` (smart_search.py)

Groups enriched matches by containing scope for section assembly.

**Key format:** `"{scope} ({file})"` for uniqueness across files.

**Ungrouped handling:** Matches without containing scope are placed in a top-level "ungrouped" section.

---

## 11. Multi-Language Orchestration

**Location:** `tools/cq/core/multilang_orchestrator.py`

The search subsystem supports multi-language queries via language scope partitioning.

### Language Scope Values

```python
QueryLanguageScope = Literal["auto", "python", "rust"]
```

- `"auto"` - Search Python + Rust (union)
- `"python"` - Python-only search (`.py`, `.pyi`)
- `"rust"` - Rust-only search (`.rs`)

### Partition Execution

**Function:** `execute_by_language_scope()` (multilang_orchestrator.py)

**Strategy:**

```python
def execute_by_language_scope(
    ctx: SmartSearchContext,
    *,
    executor: Callable[[SmartSearchContext, QueryLanguage], T],
) -> list[tuple[QueryLanguage, T]]:
    # 1. Expand scope to concrete languages
    languages = expand_language_scope(ctx.lang_scope)

    # 2. Execute for each language
    results: list[tuple[QueryLanguage, T]] = []
    for lang in languages:
        result = executor(ctx, lang)
        results.append((lang, result))

    return results
```

**Executor signature:** `(SmartSearchContext, QueryLanguage) -> T`

### Partition Merging

**Function:** `merge_partitioned_items()` (multilang_orchestrator.py)

Merges per-language results into unified output with:
- Language-specific sections
- Cross-language diagnostics
- Partition statistics

### Scope Filtering

**Function:** `is_path_in_lang_scope()` (query/language.py)

**Extension-authoritative filtering:**

```python
def is_path_in_lang_scope(path: Path, lang: QueryLanguage) -> bool:
    ext = path.suffix.lower()
    if lang == "python":
        return ext in {".py", ".pyi"}
    elif lang == "rust":
        return ext == ".rs"
    else:
        return True  # Auto scope includes all
```

**Dropped by scope tracking:** Matches excluded by scope filtering are counted in `dropped_by_scope` diagnostic.

---

## 12. Search Limits and Profiles

**Location:** `profiles.py`

### SearchLimits Struct

```python
class SearchLimits(CqStruct, frozen=True):
    max_files: int = 5000
    max_matches_per_file: int = 1000
    max_total_matches: int = 10000
    timeout_seconds: float = 30.0
    max_depth: int = 25
    max_file_size_bytes: int = 2 * 1024 * 1024  # 2 MiB
```

### Preset Profiles

| Profile | max_files | max_total_matches | timeout_seconds | Use Case |
|---------|-----------|-------------------|-----------------|----------|
| DEFAULT | 5000 | 10000 | 30.0 | General-purpose searches |
| INTERACTIVE | 1000 | 10000 | 10.0 | Fast user-facing queries |
| AUDIT | 50000 | 100000 | 300.0 | Comprehensive codebase scans |
| LITERAL | 2000 | 10000 | 30.0 | Simple string searches |

**Default:** `INTERACTIVE` profile is used when no limits are explicitly provided.

---

## 13. Pyrefly LSP Integration

**Location:** `pyrefly_lsp.py`

**Purpose:** Enrich Python matches with Pyrefly LSP hover data and diagnostics.

### PyreflyLspRequest

```python
class PyreflyLspRequest(CqStruct, frozen=True):
    root: Path
    file: Path
    line: int
    col: int
    timeout_seconds: float = 2.0
```

### Enrichment Function

```python
def enrich_with_pyrefly_lsp(
    request: PyreflyLspRequest,
) -> dict[str, object] | None:
    """Query Pyrefly LSP for hover data at position."""
```

**Returns:**
- `hover` - Hover text (signature, docstring)
- `diagnostics` - LSP diagnostics at position
- `references` - Reference count
- `implementations` - Implementation count
- `pyrefly_status` - "applied" / "degraded" / "failed"

**Timeout:** 2 seconds per request (configurable).

**Degradation:** Falls back to empty payload on timeout or error.

### Integration Point

**Prefetch phase:** `_prefetch_pyrefly_for_raw_matches()` runs in background ThreadPool during classification.

**Merge phase:** Pyrefly data is merged into enriched matches after classification completes.

---

## 14. Result Assembly

**Function:** `_assemble_smart_search_result()` (line 2371-2467)

**Purpose:** Merge per-language partitions into final `CqResult`.

### Assembly Steps

```python
def _assemble_smart_search_result(
    ctx: SearchConfig,
    partition_results: list[_LanguageSearchResult],
) -> CqResult:
    # 1. Extract matches and telemetry from partitions
    all_matches: list[EnrichedMatch] = []
    partition_stats: dict[QueryLanguage, dict[str, object]] = {}
    for partition in partition_results:
        all_matches.extend(partition.matches)
        partition_stats[partition.lang] = partition.stats

    # 2. Sort by relevance
    all_matches.sort(key=lambda m: compute_relevance_score(m), reverse=True)

    # 3. Group by containing scope
    grouped = _group_matches_by_context(all_matches)

    # 4. Build sections
    sections = [
        _build_top_contexts_section(grouped),
        _build_definitions_section(all_matches),
        _build_imports_section(all_matches),
        _build_callsites_section(all_matches),
        _build_uses_by_kind_section(all_matches),
        _build_non_code_section(all_matches),
        _build_hot_files_section(all_matches),
        _build_suggested_followups_section(ctx, all_matches),
    ]

    # 5. Build summary payload
    summary = _build_summary_payload(
        ctx=ctx,
        partition_stats=partition_stats,
        total_matches=len(all_matches),
    )

    # 6. Build findings
    findings = [
        _enriched_match_to_finding(match) for match in all_matches
    ]

    # 7. Return CqResult
    return CqResult(
        command=ctx.argv,
        summary=summary,
        sections=sections,
        findings=findings,
    )
```

### Section Construction

**Top Contexts:** Highest-relevance matches grouped by containing scope.

**Definitions:** All definition matches (functions, classes).

**Imports:** All import/from_import matches.

**Callsites:** All callsite matches.

**Uses by Kind:** Breakdown by match category (reference, assignment, etc.).

**Non-Code Matches:** Docstring/comment/string matches (collapsed by default).

**Hot Files:** Files with most matches.

**Suggested Follow-ups:** Next CQ commands to explore (e.g., `/cq calls <function>`).

### Summary Payload

**Structure:**

```python
{
    "query": ctx.query,
    "mode": ctx.mode.value,
    "lang_scope": ctx.lang_scope,
    "language_order": ["python", "rust"],
    "languages": {
        "python": {
            "matches": 42,
            "files_scanned": 150,
            "matched_files": 12,
            "total_matches": 42,
            "truncated": False,
            "timed_out": False,
            "caps_hit": "none",
        },
        "rust": { ... },
    },
    "cross_language_diagnostics": [ ... ],
    "language_capabilities": { ... },
    "enrichment_telemetry": {
        "python": {
            "applied": 40,
            "degraded": 2,
            "skipped": 0,
        },
        "rust": { ... },
    },
    "pyrefly_overview": {
        "primary_symbol": "build_graph",
        "total_incoming_callers": 5,
        "total_outgoing_callees": 12,
        "targeted_diagnostics": 0,
        "matches_enriched": 3,
    },
    "pyrefly_telemetry": {
        "attempted": 3,
        "applied": 3,
        "failed": 0,
        "skipped": 0,
        "timed_out": 0,
    },
}
```

---

## 15. Data Flow Summary Diagram

```
Input: (root, query, **kwargs)
  ↓
[1. Request Coercion] _coerce_search_request()
  SearchRequest(root, query, mode, lang_scope, include_globs, exclude_globs, limits, ...)
  ↓
[2. Context Building] _build_search_context()
  SearchConfig(mode detection, limit resolution, glob constraint, toolchain, timestamp)
  ↓
[3. Language Partition Execution] _run_language_partitions()
  For each language in expand_language_scope(ctx.lang_scope):
    ↓
    [3a. Candidate Generation] collect_candidates()
      run_rg_json() → RgCollector → RawMatch[]
      Filters: language scope (extension-authoritative)
      Statistics: scanned_files, matched_files, total_matches, dropped_by_scope
    ↓
    [3b. Classification Phase] _run_classification_phase()
      ProcessPool (max 4 workers, spawn context, fail-open):
        For each file batch:
          ↓
          [Tier 1: Heuristic] classify_heuristic()
            O(1) line pattern matching → (category, confidence, skip_deeper)
          ↓
          [Tier 2: AST Node] classify_from_node()
            O(log n) ast-grep node lookup → NodeClassification
            Cached SgRoot, parent walk, containing scope extraction
          ↓
          [Tier 3: Record-Based] classify_from_records()
            O(log n) record interval index query → NodeClassification
            Cached RecordContext (def/call/import/assign records)
          ↓
          [Symtable Enrichment] enrich_with_symtable_from_table()
            Python-only, high-confidence matches → SymtableEnrichment
        → EnrichedMatch[]
    ↓
    [3c. Enrichment Phase] _enrich_matches()
      For Python matches (sequential):
        ↓
        [Stage 1: ast-grep] _run_ast_grep_stage()
          signature, decorators, scope_chain, call_target, class_context
        ↓
        [Stage 2: Python AST] _run_python_ast_stage()
          is_generator, is_async, awaits, yields, returns_value
        ↓
        [Stage 3: Import Detail] _run_import_stage()
          import_module, import_alias, import_names, import_level
        ↓
        [Stage 4: LibCST] _run_libcst_stage()
          qualified_names, binding_candidates, import_alias_chain, call_resolution
          Gap-fill merge (only adds missing fields)
        ↓
        [Stage 5: Tree-Sitter] _run_tree_sitter_stage()
          Final gap-fill fallback, parse_quality
        ↓
        [Finalization] _finalize_python_enrichment_payload()
          Agreement section (ast_grep vs libcst vs tree_sitter)
          Crosscheck validation (CQ_PY_ENRICHMENT_CROSSCHECK=1)
          Payload budget enforcement (4096 bytes)
        → EnrichedMatch with DetailPayload

      For Rust matches (sequential):
        ↓
        [Stage 1: Tree-Sitter] tree_sitter_rust enrichment
        ↓
        [Stage 2: Syntax Analysis] Rust-specific extraction
        → EnrichedMatch with DetailPayload
    ↓
    [3d. Pyrefly LSP Enrichment] _prefetch_pyrefly_in_background()
      ThreadPool (1 worker, Python-only):
        For high-relevance matches:
          enrich_with_pyrefly_lsp() → hover, diagnostics, references
        → Pyrefly data merged into EnrichedMatch
    → _LanguageSearchResult(lang, matches, stats)

  → partition_results: list[_LanguageSearchResult]
  ↓
[4. Fallback Logic] _should_fallback_to_literal()
  If identifier mode yielded 0 results:
    Retry with QueryMode.LITERAL
    If literal mode yields >0 results:
      Replace partition_results with fallback results
  ↓
[5. Result Assembly] _assemble_smart_search_result()
  Merge partitions → sort by relevance → group by scope → build sections
  ├─ Top Contexts (highest-relevance, grouped by scope)
  ├─ Definitions (all function/class definitions)
  ├─ Imports (all import statements)
  ├─ Callsites (all function calls)
  ├─ Uses by Kind (breakdown by category)
  ├─ Non-Code Matches (docstring/comment/string, collapsed)
  ├─ Hot Files (files with most matches)
  └─ Suggested Follow-ups (next CQ commands)

  Build summary payload:
  ├─ Query metadata (query, mode, lang_scope)
  ├─ Per-language partition stats (matches, files_scanned, timed_out, dropped_by_scope)
  ├─ Cross-language diagnostics
  ├─ Language capabilities matrix
  ├─ Enrichment telemetry (applied, degraded, skipped)
  ├─ Pyrefly overview (primary_symbol, callers, callees, diagnostics)
  └─ Pyrefly telemetry (attempted, applied, failed, timed_out)

  → CqResult(command, summary, sections, findings)
```

---

## 16. Configuration and Environment Variables

### Environment Variables

| Variable | Type | Default | Purpose |
|----------|------|---------|---------|
| `CQ_PY_ENRICHMENT_CROSSCHECK` | bool | `0` | Enable conflict degradation on agreement mismatches |
| `CQ_FORMAT` | str | `"md"` | Output format (md, json, mermaid) |
| `CQ_ROOT` | Path | `.` | Repository root path |
| `CQ_VERBOSE` | int | `0` | Verbosity level (0-3) |
| `MAX_SEARCH_CLASSIFY_WORKERS` | int | `4` | Maximum classification workers |

### Payload Budget Constants

**Location:** `python_enrichment.py` (lines 59-74)

```python
_MAX_SIGNATURE_LEN = 200
_MAX_PARAMS = 12
_MAX_RETURN_TYPE_LEN = 100
_MAX_DECORATORS = 8
_MAX_DECORATOR_LEN = 60
_MAX_BASE_CLASSES = 6
_MAX_BASE_CLASS_LEN = 60
_MAX_CALL_TARGET_LEN = 120
_MAX_CALL_RECEIVER_LEN = 80
_MAX_SCOPE_CHAIN = 8
_MAX_IMPORT_NAMES = 12
_MAX_METHODS_SHOWN = 8
_MAX_PROPERTIES_SHOWN = 8
_MAX_PAYLOAD_BYTES = 4096  # Total payload size limit
```

### Tree-Sitter Parser Configuration

**Location:** Distributed across enrichment modules

- Python: `tree_sitter_python.py` - Uses `tree-sitter-python` grammar
- Rust: `tree_sitter_rust.py` - Uses `tree-sitter-rust` grammar

---

## 17. Error Handling and Degradation

### Fail-Open Policy

All enrichment stages follow a fail-open policy:
- Exceptions are caught and logged to `degrade_reasons`
- Degraded matches are marked with `enrichment_status: "degraded"`
- Pipeline continues with partial data

### Degradation Reasons

**Tracked in:** `_PythonEnrichmentState.degrade_reasons: list[str]`

**Common reasons:**
- `"ast_grep: <exception>"` - ast-grep extraction failed
- `"libcst: <exception>"` - LibCST parsing failed
- `"tree_sitter: <exception>"` - Tree-sitter parsing failed
- `"crosscheck mismatch"` - Agreement conflict detected
- `"payload_budget"` - Payload exceeded 4096 bytes

### Stage Status Values

**Tracked in:** `_PythonEnrichmentState.stage_status: dict[str, str]`

**Status values:**
- `"applied"` - Stage ran successfully
- `"degraded"` - Stage ran but encountered errors
- `"skipped"` - Stage was not applicable (e.g., import stage for non-import nodes)

### Timeout Handling

**Ripgrep timeout:** Detected via process exit code, tracked in `SearchStats.timed_out`.

**Pyrefly timeout:** 2-second timeout per request, tracked in `PyreflyTelemetry.timed_out`.

**Classification timeout:** None - ProcessPool relies on OS-level process limits.

---

## 18. Architectural Observations for Improvement Proposals

### Design Tensions

#### 1. Cache Consistency vs Performance

**Current state:** Thread-unsafe per-file caches with manual clearing.

**Tension:** Parallel classification requires cache isolation but clearing before each search wastes parse work across queries.

**Potential improvements:**
- Thread-safe cache with RWLock for shared reads
- Per-worker cache isolation in ProcessPool (avoid serialization overhead)
- Persistent cross-query cache with content-addressable keys
- LRU eviction policy for bounded memory usage

**Trade-offs:**
- Thread-safe cache adds locking overhead
- Persistent cache requires invalidation on file changes
- Per-worker cache duplicates parse work

#### 2. Sequential Enrichment vs Parallel Enrichment

**Current state:** Five-stage Python enrichment runs sequentially per-match.

**Tension:** ast-grep and tree-sitter stages are independent but run sequentially. LibCST stage depends on tree-sitter only for gap-fill.

**Potential improvements:**
- Parallelize ast-grep and tree-sitter stages
- Batch enrichment requests per file (amortize parse cost)
- Async enrichment with stage-level futures

**Trade-offs:**
- Parallelization adds coordination overhead
- Batching delays first-match results (bad for interactive queries)
- Async adds complexity without clear latency benefit

#### 3. Classification Granularity vs Accuracy

**Current state:** Three-tier classification with heuristic → AST → record fallback.

**Tension:** Heuristic tier is fast but low-confidence. AST tier is accurate but requires node lookup. Record tier is fallback but requires pre-scanning.

**Potential improvements:**
- Skip heuristic tier entirely (always use AST)
- Pre-compute classification during candidate collection (single pass)
- Machine learning classifier trained on labeled matches

**Trade-offs:**
- Skipping heuristic increases average latency (more AST lookups)
- Pre-computation requires stateful candidate collector
- ML classifier requires training data and model maintenance

#### 4. Cross-Source Agreement Validation

**Current state:** Agreement checked by comparing ast-grep, LibCST, tree-sitter outputs. Conflicts trigger degradation only if `CQ_PY_ENRICHMENT_CROSSCHECK=1`.

**Tension:** Agreement validation catches bugs but adds payload overhead and complexity.

**Potential improvements:**
- Remove agreement section entirely (trust single source)
- Make agreement validation always-on (remove env var gate)
- Use agreement status for confidence scoring

**Trade-offs:**
- Removing agreement loses debugging signal
- Always-on validation adds payload size
- Using agreement for scoring requires confidence calibration

#### 5. Payload Budget Enforcement

**Current state:** Hard 4096-byte limit with field dropping.

**Tension:** Limit prevents payload bloat but drops high-value fields.

**Potential improvements:**
- Priority-based dropping (keep high-value fields)
- Compressed payload encoding (msgpack, zstd)
- Separate "full" and "summary" payloads

**Trade-offs:**
- Priority dropping requires field ranking policy
- Compression adds CPU cost
- Dual payloads increase complexity

### Coupling Concerns

#### 1. Enrichment Stages Tightly Coupled to ast-grep

**Current state:** ast-grep stage extracts fields that later stages gap-fill.

**Coupling:** LibCST and tree-sitter stages depend on ast-grep field names.

**Risk:** Changes to ast-grep extraction break gap-fill logic.

**Mitigation:** Shared field schema defined in `enrichment/core.py`.

#### 2. Classification Depends on File-Level Caching

**Current state:** Classification requires cached SgRoot from prior candidate phase.

**Coupling:** Candidate phase must populate caches before classification.

**Risk:** Cache miss in classification triggers redundant parse.

**Mitigation:** Explicit cache population contract via `get_sg_root()`.

#### 3. Multi-Language Orchestration Spreads Across Modules

**Current state:** Language scope logic in `query/language.py`, partition execution in `smart_search.py`, merging in `multilang_orchestrator.py`.

**Coupling:** Three modules must agree on language scope semantics.

**Risk:** Inconsistent scope filtering across modules.

**Mitigation:** Centralize language scope resolution in single module.

### Potential Improvement Vectors

#### 1. Incremental Classification

**Goal:** Avoid re-classifying unchanged matches across queries.

**Approach:**
- Content-addressable match fingerprints
- Persistent classification cache keyed by (file_hash, line, col, query_pattern)
- Incremental cache invalidation on file changes

**Benefits:**
- Reduce latency for repeated queries
- Amortize parse cost across queries

**Risks:**
- Cache invalidation complexity
- Stale classifications on file changes

#### 2. Streaming Result Assembly

**Goal:** Return top results before full search completes.

**Approach:**
- Yield results incrementally as matches are classified
- Assemble sections progressively (top contexts first)
- Terminate search early when sufficient high-confidence matches found

**Benefits:**
- Improve perceived latency for interactive queries
- Early termination reduces wasted work

**Risks:**
- Streaming complicates result assembly
- Early termination may miss high-relevance matches

#### 3. Adaptive Worker Pool Sizing

**Goal:** Scale worker count based on match distribution.

**Approach:**
- Heuristic: `workers = min(len(files_with_matches), cpu_count, MAX_WORKERS)`
- Dynamic spawning: start with 1 worker, spawn more if queue depth exceeds threshold
- Adaptive shutdown: terminate idle workers after timeout

**Benefits:**
- Reduce overhead for small match sets
- Scale up for large match sets

**Risks:**
- Dynamic spawning adds startup latency
- Adaptive shutdown wastes spawn cost

#### 4. Enrichment Stage Parallelization

**Goal:** Run independent enrichment stages in parallel.

**Approach:**
- Split pipeline into DAG: `ast-grep || tree-sitter`, then `LibCST` (depends on tree-sitter gap-fill)
- Use `asyncio` or ThreadPool for parallel stage execution
- Merge results via structured concurrency

**Benefits:**
- Reduce enrichment latency for high-parse-cost files
- Better utilize multi-core machines

**Risks:**
- Coordination overhead exceeds parallelism benefit
- Complexity increase for marginal latency gain

#### 5. Machine Learning Classification

**Goal:** Replace heuristic tier with ML classifier.

**Approach:**
- Train multi-class classifier on labeled matches (definition, callsite, import, etc.)
- Features: line text, surrounding lines, token patterns, indentation
- Use classifier as Tier 1, fall back to AST tier for low-confidence predictions

**Benefits:**
- Higher accuracy than heuristic patterns
- Adaptive to codebase idioms

**Risks:**
- Requires labeled training data
- Model maintenance and versioning
- Inference latency may exceed heuristic tier

#### 6. Unified Cache Architecture

**Goal:** Replace per-module caches with centralized cache manager.

**Approach:**
- Define `CacheManager` protocol with get/put/clear operations
- Inject cache manager into classification and enrichment functions
- Support multiple cache backends (in-memory, Redis, filesystem)

**Benefits:**
- Consistent cache semantics across modules
- Easier to add cache observability (hit rate, eviction count)
- Pluggable cache backends for different deployment scenarios

**Risks:**
- Increased abstraction adds complexity
- Cache manager becomes performance bottleneck

---

## Appendix A: Key Data Structures

### RawMatch

```python
class RawMatch(CqStruct, frozen=True):
    file: str          # File path
    line: int          # 1-indexed line number
    col: int           # 0-indexed column offset
    text: str          # Matched text
    context: str       # Full line content
```

### EnrichedMatch

```python
class EnrichedMatch(CqStruct, frozen=True):
    file: str
    line: int
    col: int
    text: str
    category: MatchCategory
    confidence: float
    node_kind: str | None
    containing_scope: str | None
    context: str
    symtable: SymtableEnrichment | None
    detail_payload: dict[str, object] | None  # Python/Rust enrichment
    item_role: str | None  # "src", "test", "doc", "lib"
```

### SearchConfig

```python
class SearchConfig(CqStruct, frozen=True):
    root: Path
    query: str
    mode: QueryMode
    limits: SearchLimits
    lang_scope: QueryLanguageScope = "auto"
    mode_requested: QueryMode | None = None
    mode_chain: tuple[QueryMode, ...] = ()
    fallback_applied: bool = False
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    argv: list[str] = []
    tc: Toolchain | None = None
    started_ms: float = 0.0
```

### SearchStats

```python
class SearchStats(CqStruct, frozen=True):
    scanned_files: int
    matched_files: int
    total_matches: int
    truncated: bool
    timed_out: bool
    caps_hit: str  # "none", "files", "matches_per_file", "total_matches"
    dropped_by_scope: int = 0
```

---

## Appendix B: Function Call Graph

### smart_search() Call Hierarchy

```
smart_search()
├─ _coerce_search_request()
├─ _build_search_context()
│  ├─ detect_query_mode()
│  └─ constrain_include_globs_for_language()
├─ _run_language_partitions()
│  └─ execute_by_language_scope()
│     └─ _run_single_language_partition()
│        ├─ collect_candidates()
│        │  ├─ run_rg_json()
│        │  ├─ RgCollector.handle_event()
│        │  ├─ is_path_in_lang_scope()
│        │  └─ _build_search_stats()
│        ├─ _prefetch_pyrefly_in_background()
│        │  └─ _prefetch_pyrefly_for_raw_matches()
│        │     └─ enrich_with_pyrefly_lsp()
│        ├─ _run_classification_phase()
│        │  └─ ProcessPoolExecutor.map(_classify_partition_batch)
│        │     └─ classify_match()
│        │        ├─ classify_heuristic()
│        │        ├─ classify_from_node()
│        │        │  ├─ get_sg_root()
│        │        │  ├─ _find_node_at_position()
│        │        │  └─ classify_from_resolved_node()
│        │        ├─ classify_from_records()
│        │        │  └─ _get_record_context()
│        │        ├─ enrich_with_symtable_from_table()
│        │        └─ extract_search_context_snippet()
│        └─ _enrich_matches()
│           ├─ enrich_python_context()  [Python]
│           │  ├─ _run_ast_grep_stage()
│           │  ├─ _run_python_ast_stage()
│           │  ├─ _run_import_stage()
│           │  ├─ _run_libcst_stage()
│           │  ├─ _run_tree_sitter_stage()
│           │  └─ _finalize_python_enrichment_payload()
│           └─ enrich_rust_context()  [Rust]
├─ _should_fallback_to_literal()
└─ _assemble_smart_search_result()
   ├─ compute_relevance_score()
   ├─ _group_matches_by_context()
   ├─ _build_top_contexts_section()
   ├─ _build_definitions_section()
   ├─ _build_imports_section()
   ├─ _build_callsites_section()
   ├─ _build_uses_by_kind_section()
   ├─ _build_non_code_section()
   ├─ _build_hot_files_section()
   ├─ _build_suggested_followups_section()
   └─ _build_summary_payload()
```

---

## Appendix C: Module Dependencies

### Core Dependencies

```
smart_search.py
├─ classifier.py (three-tier classification)
├─ python_enrichment.py (five-stage Python enrichment)
├─ rust_enrichment.py (Rust enrichment)
├─ collector.py (RgCollector)
├─ context_window.py (context snippet extraction)
├─ models.py (SearchConfig, SearchRequest)
├─ requests.py (RgRunRequest, enrichment requests)
├─ profiles.py (SearchLimits)
├─ contracts.py (summary contracts, telemetry)
├─ pyrefly_lsp.py (LSP enrichment)
├─ adapter.py (ripgrep adapter)
└─ multilang_orchestrator.py (multi-language execution)
```

### Enrichment Sub-Module Dependencies

```
python_enrichment.py
├─ enrichment/core.py (payload normalization, budget enforcement)
├─ enrichment/libcst_python.py (LibCST resolution)
└─ enrichment/tree_sitter_python.py (tree-sitter gap-fill)

rust_enrichment.py
└─ enrichment/tree_sitter_rust.py (Rust tree-sitter)
```

### External Dependencies

- `ast_grep_py` - AST parsing and node lookup
- `tree-sitter` - Alternative parser for Python/Rust
- `libcst` - Python CST with metadata resolution
- `ripgrep` (rg) - Fast text search engine
- `msgspec` - Fast serialization

---

## Document History

- **Version 1.0** (2026-02-08) - Initial comprehensive architecture document

---

**End of Search Subsystem Architecture Document**
